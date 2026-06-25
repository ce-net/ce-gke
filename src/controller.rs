//! Controller — one reconcile tick wiring placement + rollout + driver together.
//!
//! A *tick* is the orchestrator's unit of work: refresh each tracked replica's phase, plan the next
//! rolling-update/reconcile step, place new replicas on atlas-ranked hosts, and kill the ones the
//! plan retired. Drivers (real or fake) supply the side effects; the planners stay pure. Running
//! `tick` to a fixed point converges actual state to the [`Deployment`].
//!
//! State the controller carries between ticks is just the list of [`ReplicaState`] handles it
//! placed — it is the source of truth for "what did I launch", refreshed against the driver each
//! tick so a host-side failure shows up as a `Failed` phase and triggers a reschedule.

use anyhow::Result;

use crate::deps::{DepReadiness, Gate};
use crate::driver::MeshDriver;
use crate::placement::rank;
use crate::reconcile::{Phase, ReplicaState};
use crate::rollout::{plan_step, RolloutStep};
use crate::spec::Deployment;

/// How many consecutive failed health refreshes a replica tolerates before it is treated as
/// `Failed`. A single transient local-API blip must not mark the whole fleet failed and trigger a
/// reschedule storm; only a *sustained* inability to confirm a replica condemns it.
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

/// The controller's tracked state for one deployment: the replicas it has placed.
#[derive(Debug, Clone, Default)]
pub struct Controller {
    /// Replicas the controller launched and is responsible for.
    pub replicas: Vec<ReplicaState>,
    /// Capability grant token forwarded on every deploy/kill (authorizes the host to run our work).
    pub grant: Option<String>,
    /// Freshness window for atlas candidates (seconds); 0 disables.
    pub max_stale_secs: u64,
    /// Consecutive refresh failures, per job id. A replica is only marked `Failed` from a refresh
    /// error after this count reaches [`failure_threshold`](Self::failure_threshold) — bounded
    /// retry, so a transient API hiccup never mass-fails the fleet.
    refresh_failures: std::collections::HashMap<String, u32>,
    /// Threshold for the above. Defaults to [`DEFAULT_FAILURE_THRESHOLD`].
    pub failure_threshold: u32,
}

/// What happened in one tick — for logging and for the convergence loop's stop condition.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Replicas placed this tick (job ids).
    pub placed: Vec<String>,
    /// Replicas killed this tick (job ids).
    pub killed: Vec<String>,
    /// Placements that failed (no candidate, or every candidate rejected) — surfaced, not panicked.
    pub place_failures: u32,
    /// Kills that failed (dropped peer) — will be retried next tick.
    pub kill_failures: u32,
    /// True if the deployment's named service was (re-)advertised this tick.
    pub service_advertised: bool,
    /// True when the deployment is fully reconciled on its target revision.
    pub done: bool,
    /// When the deployment is held by an unmet [`depends_on`](crate::spec::Deployment::depends_on),
    /// the bare service names it is waiting on (sorted). Empty when not waiting. While waiting, no
    /// new replicas are placed and the named service is not advertised.
    pub waiting_on: Vec<String>,
}

impl TickReport {
    /// Did this tick change anything?
    pub fn made_progress(&self) -> bool {
        !self.placed.is_empty() || !self.killed.is_empty()
    }

    /// Is the deployment held waiting on an unmet dependency this tick?
    pub fn is_waiting(&self) -> bool {
        !self.waiting_on.is_empty()
    }
}

impl Controller {
    /// New empty controller (no replicas yet).
    pub fn new(grant: Option<String>) -> Self {
        Controller {
            replicas: Vec::new(),
            grant,
            max_stale_secs: 120,
            refresh_failures: std::collections::HashMap::new(),
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
        }
    }

    /// Refresh every tracked replica's phase from the driver, running the deployment's liveness
    /// probe (when set) inside the cell. A replica whose refresh *errors* is not condemned on the
    /// first miss: its consecutive-failure counter is bumped, and only after
    /// [`failure_threshold`](Self::failure_threshold) sustained failures is it marked `Failed` for
    /// rescheduling. A successful refresh resets the counter. This prevents a transient local-API
    /// blip from mass-failing the fleet and triggering a reschedule storm.
    async fn refresh<D: MeshDriver>(&mut self, driver: &D, d: &Deployment) {
        let probe = d.liveness.as_ref();
        let grant = self.grant.clone();
        // Collect updates first to avoid borrowing self mutably across the await of a method that
        // also reads self fields.
        let mut updates: Vec<(String, Option<Phase>)> = Vec::with_capacity(self.replicas.len());
        for r in &self.replicas {
            let res = driver.phase(&r.node_id, &r.job_id, probe, grant.as_deref()).await;
            updates.push((r.job_id.clone(), res.ok()));
        }
        for (job_id, maybe_phase) in updates {
            match maybe_phase {
                Some(p) => {
                    self.refresh_failures.remove(&job_id);
                    if let Some(r) = self.replicas.iter_mut().find(|r| r.job_id == job_id) {
                        r.phase = p;
                    }
                }
                None => {
                    let n = self.refresh_failures.entry(job_id.clone()).or_insert(0);
                    *n += 1;
                    if *n >= self.failure_threshold.max(1)
                        && let Some(r) = self.replicas.iter_mut().find(|r| r.job_id == job_id)
                    {
                        r.phase = Phase::Failed;
                    }
                }
            }
        }
        // Drop counters for replicas we no longer track.
        let live: std::collections::HashSet<_> =
            self.replicas.iter().map(|r| r.job_id.clone()).collect();
        self.refresh_failures.retain(|k, _| live.contains(k));
    }

    /// Resolve every `depends_on` of `d` over the mesh (via the driver's `service_ready`, backed by
    /// `ce_rs::locate`) and fold the observations into the pure [`Gate`] decision. The mesh lookups
    /// are gathered first into a readiness map (keyed by bare service name), then [`Gate::evaluate`]
    /// — which is pure — makes the ready/waiting call. A lookup that errors is treated as `Absent`
    /// (fail-safe: an unresolvable dependency holds the dependent rather than racing it up).
    async fn evaluate_deps<D: MeshDriver>(&self, driver: &D, d: &Deployment) -> Gate {
        if d.depends_on.is_empty() {
            return Gate::Ready;
        }
        let mut readiness: std::collections::HashMap<String, DepReadiness> =
            std::collections::HashMap::new();
        for dep in &d.depends_on {
            let resolved = dep.resolved_service_name(&d.namespace);
            let observed = driver
                .service_ready(&resolved)
                .await
                .unwrap_or(DepReadiness::Absent);
            readiness.insert(dep.service.clone(), observed);
        }
        Gate::evaluate(d, |svc| readiness.get(svc).copied().unwrap_or(DepReadiness::Absent))
    }

    /// Run one reconcile tick for `d`. Steps:
    /// 1. refresh phases (health),
    /// 2. plan the next rolling-update step against `d.revision()`,
    /// 3. kill what the plan retired (drop successfully-killed from tracking; keep failures to retry),
    /// 4. place what the plan wants on atlas-ranked candidates (skip+count hosts that reject).
    ///
    /// Returns a [`TickReport`]. Never panics on driver errors — they become failure counts.
    pub async fn tick<D: MeshDriver>(&mut self, driver: &D, d: &Deployment) -> Result<TickReport> {
        let mut report = TickReport::default();
        let target = d.revision();

        // 1. Health refresh (with bounded retry + liveness probe).
        self.refresh(driver, d).await;

        // 1b. Dependency gate: resolve each `depends_on` over the mesh and decide ready vs waiting.
        // This is the "docker-compose for the mesh" layer — a deployment whose deps are unmet places
        // nothing this tick and reports `waiting_on`; the next tick retries. When a dep disappears,
        // the per-tick resolution flips it back to waiting (no special teardown path needed). The
        // *decision* is the pure `Gate`; only the readiness observation touches the mesh.
        let gate = self.evaluate_deps(driver, d).await;
        if let Gate::Waiting { unmet } = &gate {
            report.waiting_on = unmet.clone();
        }

        // 2. Plan.
        let step: RolloutStep = plan_step(d, &self.replicas, &target);
        report.done = step.done && gate.is_ready();

        // 3. Kills first (frees capacity for surge placements; matches Recreate's drain-then-fill).
        for job_id in &step.to_kill {
            // Find the replica to learn its host.
            let node_id = self
                .replicas
                .iter()
                .find(|r| &r.job_id == job_id)
                .map(|r| r.node_id.clone());
            let Some(node_id) = node_id else {
                // Already untracked — nothing to do.
                continue;
            };
            match driver.kill(&node_id, job_id, self.grant.as_deref()).await {
                Ok(()) => {
                    self.replicas.retain(|r| &r.job_id != job_id);
                    report.killed.push(job_id.clone());
                }
                Err(_) => {
                    // Dropped peer / host error — leave it tracked and retry next tick.
                    report.kill_failures += 1;
                }
            }
        }

        // 3b. Resilience: if a *count-based* scale-down was wanted but a specific victim's kill
        // failed, we are still over the desired count on the current revision. Any excess on-target
        // replica is interchangeable for a pure scale-down, so retire an alternate (the planner only
        // names one of several equivalent victims). This keeps `scale` converging despite a stuck
        // host, without ever substituting a *rolling-update* old-revision kill (those replicas are
        // not on the target revision, so they are excluded here).
        if report.kill_failures > 0 {
            let on_target_live: Vec<String> = self
                .replicas
                .iter()
                .filter(|r| r.revision == target && r.phase.is_alive())
                .map(|r| r.job_id.clone())
                .collect();
            let mut excess = on_target_live.len() as i64 - d.replicas as i64;
            // Try killing alternates we have not already tried-and-failed this tick.
            for job_id in on_target_live {
                if excess <= 0 {
                    break;
                }
                if step.to_kill.contains(&job_id) {
                    continue; // already attempted (and failed) above
                }
                let node_id = self
                    .replicas
                    .iter()
                    .find(|r| r.job_id == job_id)
                    .map(|r| r.node_id.clone());
                let Some(node_id) = node_id else { continue };
                if driver.kill(&node_id, &job_id, self.grant.as_deref()).await.is_ok() {
                    self.replicas.retain(|r| r.job_id != job_id);
                    report.killed.push(job_id);
                    report.kill_failures = report.kill_failures.saturating_sub(1);
                    excess -= 1;
                }
            }
        }

        // 4. Placements on ranked candidates. Suppressed while a dependency is unmet: we never bring
        // a consumer up before its producer is live on the mesh. Kills above (scale-down / rolled-out
        // old revisions / reaped failures) still ran — holding a deployment must not strand excess or
        // dead replicas — but no *new* replica is placed until the gate opens.
        if step.to_place > 0 && gate.is_ready() {
            let atlas = driver.atlas().await.unwrap_or_default();
            let mut candidates = rank(&atlas, d, now_secs(), self.max_stale_secs);
            let mut to_place = step.to_place;
            let mut attempts = 0u32;
            // Cap attempts so a cluster that rejects everything terminates instead of looping.
            let max_attempts = step.to_place.saturating_mul(4).max(candidates.len() as u32 + 1);
            while to_place > 0 && attempts < max_attempts {
                attempts += 1;
                if candidates.is_empty() {
                    report.place_failures += to_place;
                    break;
                }
                // Always pick the best-scored candidate (candidates stay sorted best-first).
                let node_id = candidates[0].node_id.clone();
                match driver.deploy(&node_id, d, self.grant.as_deref()).await {
                    Ok(job_id) => {
                        self.replicas.push(ReplicaState {
                            job_id: job_id.clone(),
                            node_id: node_id.clone(),
                            revision: target.clone(),
                            phase: Phase::Pending,
                        });
                        report.placed.push(job_id);
                        to_place -= 1;
                        // Penalize the chosen host so the next replica prefers a different one
                        // (spread), then re-sort to keep best-first ordering.
                        if let Some(c) = candidates.iter_mut().find(|c| c.node_id == node_id) {
                            c.score -= 150;
                        }
                        candidates.sort_by(|a, b| {
                            b.score.cmp(&a.score).then_with(|| a.node_id.cmp(&b.node_id))
                        });
                    }
                    Err(_) => {
                        // This host rejected — drop it and try the next.
                        candidates.retain(|c| c.node_id != node_id);
                    }
                }
            }
            if to_place > 0 && !candidates.is_empty() {
                // Ran out of attempts with candidates still present (all transiently failing).
                report.place_failures += to_place;
            }
        }

        // 5. Service discovery: if the deployment names a service and we have at least one healthy
        // replica, advertise it on the DHT so clients can find the live set. Re-advertised each tick
        // because provider records expire. Best-effort: a failure here never fails the tick.
        if let Some(service) = d.service_name() {
            let has_ready = self.replicas.iter().any(|r| r.phase == Phase::Running);
            // Do not advertise a service whose own dependencies are unmet: a dependent of *this*
            // service must not see it as live while it is still waiting on its producers.
            if has_ready && gate.is_ready() {
                match driver.advertise_service(&service).await {
                    Ok(()) => report.service_advertised = true,
                    Err(_) => report.service_advertised = false,
                }
            }
        }

        Ok(report)
    }

    /// Drive `tick` until the deployment is reconciled or `max_ticks` is reached. Between ticks the
    /// caller's driver is expected to advance replica readiness (the real loop sleeps; tests mark
    /// replicas ready). Returns the final [`TickReport`].
    pub async fn converge<D: MeshDriver>(
        &mut self,
        driver: &D,
        d: &Deployment,
        max_ticks: u32,
        between: impl Fn(&D),
    ) -> Result<TickReport> {
        let mut last = TickReport::default();
        for _ in 0..max_ticks {
            last = self.tick(driver, d).await?;
            if last.done {
                break;
            }
            between(driver);
        }
        Ok(last)
    }

    /// Replicas currently tracked, for status reporting.
    pub fn current(&self) -> &[ReplicaState] {
        &self.replicas
    }
}

/// Wall-clock seconds (for atlas freshness). Isolated so tests of pure planners never call it.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::FakeDriver;
    use crate::spec::{Resources, Strategy};
    use ce_rs::{Amount, AtlasEntry};

    fn host(id: &str, cpu: u32, mem: u32) -> AtlasEntry {
        AtlasEntry {
            node_id: id.into(),
            cpu_cores: cpu,
            mem_mb: mem,
            running_jobs: 0,
            last_seen_secs: now_secs(),
            tags: vec!["docker".into()],
        }
    }

    fn deploy(replicas: u32, strategy: Strategy) -> Deployment {
        Deployment {
            name: "web".into(),
            image: "nginx".into(),
            command: vec![],
            replicas,
            resources: Resources { cpu_cores: 1, mem_mb: 256 },
            select: vec![],
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn tick_scales_up_to_desired() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192), host("b", 8, 8192)]);
        let d = deploy(3, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        let r = ctrl.tick(&fake, &d).await.unwrap();
        // RollingUpdate from zero places desired (surge ceiling >= desired).
        assert_eq!(r.placed.len(), 3);
        assert_eq!(ctrl.current().len(), 3);
    }

    #[tokio::test]
    async fn tick_spreads_across_hosts() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192), host("b", 8, 8192)]);
        let d = deploy(2, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.tick(&fake, &d).await.unwrap();
        // two replicas, two hosts → one each (spread).
        let nodes: std::collections::HashSet<_> =
            ctrl.current().iter().map(|r| r.node_id.clone()).collect();
        assert_eq!(nodes.len(), 2, "replicas should spread across both hosts");
    }

    #[tokio::test]
    async fn converges_then_idle() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let d = deploy(3, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        let report = ctrl
            .converge(&fake, &d, 10, |f| f.mark_all_ready())
            .await
            .unwrap();
        assert!(report.done, "should converge");
        assert_eq!(ctrl.current().len(), 3);
        // a follow-up tick is a no-op
        let again = ctrl.tick(&fake, &d).await.unwrap();
        assert!(again.done);
        assert!(!again.made_progress());
    }

    #[tokio::test]
    async fn failed_replica_is_rescheduled() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let d = deploy(2, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        // bring it up
        ctrl.converge(&fake, &d, 5, |f| f.mark_all_ready()).await.unwrap();
        assert_eq!(ctrl.current().len(), 2);
        // kill one replica's health out from under us
        let victim = ctrl.current()[0].job_id.clone();
        fake.set_phase(&victim, Phase::Failed);
        // next tick: reap the failed one and place a replacement
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r.killed.contains(&victim), "failed replica reaped");
        assert_eq!(r.placed.len(), 1, "one replacement placed");
        // converge to be sure
        fake.mark_all_ready();
        let done = ctrl.converge(&fake, &d, 5, |f| f.mark_all_ready()).await.unwrap();
        assert!(done.done);
        assert_eq!(ctrl.current().len(), 2);
        assert!(ctrl.current().iter().all(|r| r.phase == Phase::Running));
    }

    #[tokio::test]
    async fn host_rejecting_deploy_falls_back_to_another() {
        let fake = FakeDriver::new(vec![host("bad", 16, 16384), host("good", 16, 16384)]);
        fake.fail_deploy_on("bad");
        let d = deploy(2, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        let r = ctrl.tick(&fake, &d).await.unwrap();
        // both replicas land on "good" since "bad" rejects.
        assert_eq!(r.placed.len(), 2);
        assert!(ctrl.current().iter().all(|rep| rep.node_id == "good"));
    }

    #[tokio::test]
    async fn no_candidates_reports_failure_not_panic() {
        // Atlas has only a non-docker host → no candidates.
        let mut h = host("a", 8, 8192);
        h.tags = vec!["linux".into()]; // no docker
        let fake = FakeDriver::new(vec![h]);
        let d = deploy(2, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert_eq!(r.placed.len(), 0);
        assert_eq!(r.place_failures, 2);
    }

    #[tokio::test]
    async fn kill_failure_is_retried_next_tick() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let d2 = deploy(2, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.converge(&fake, &d2, 5, |f| f.mark_all_ready()).await.unwrap();
        // scale down to 1; the chosen victim's kill is made to fail once.
        let d1 = deploy(1, Strategy::default());
        // make the kill of one specific replica fail
        let victim = ctrl.current()[0].job_id.clone();
        fake.fail_kill_on(&victim);
        let r = ctrl.tick(&fake, &d1).await.unwrap();
        // Either the victim was the kill target (failure recorded) or another replica was killed.
        // Drive convergence; the failing-kill replica must eventually be retried/handled.
        let _ = r;
        // Remove the kill failure and converge.
        // (FakeDriver has no "clear", so the victim's kill keeps failing; controller must pick a
        // different excess replica if available. With desired=1 and 2 live, one kill suffices.)
        let done = ctrl.converge(&fake, &d1, 10, |f| f.mark_all_ready()).await.unwrap();
        assert!(done.done, "scale-down converges despite one failing kill target");
        assert_eq!(ctrl.current().len(), 1);
    }

    #[tokio::test]
    async fn scale_down_removes_excess() {
        let fake = FakeDriver::new(vec![host("a", 32, 32768)]);
        let up = deploy(4, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.converge(&fake, &up, 10, |f| f.mark_all_ready()).await.unwrap();
        assert_eq!(ctrl.current().len(), 4);
        let down = deploy(1, Strategy::default());
        let done = ctrl.converge(&fake, &down, 10, |f| f.mark_all_ready()).await.unwrap();
        assert!(done.done);
        assert_eq!(ctrl.current().len(), 1);
    }

    #[tokio::test]
    async fn rolling_update_to_new_image_converges() {
        let fake = FakeDriver::new(vec![host("a", 32, 32768), host("b", 32, 32768)]);
        let v1 = deploy(3, Strategy::RollingUpdate { max_unavailable: 1, max_surge: 1 });
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.converge(&fake, &v1, 10, |f| f.mark_all_ready()).await.unwrap();
        assert_eq!(ctrl.current().len(), 3);
        let v1_rev = v1.revision();

        // New image → new revision.
        let mut v2 = v1.clone();
        v2.image = "nginx:1.26".into();
        assert_ne!(v2.revision(), v1_rev);

        let done = ctrl.converge(&fake, &v2, 30, |f| f.mark_all_ready()).await.unwrap();
        assert!(done.done, "rolling update converged");
        assert_eq!(ctrl.current().len(), 3);
        let target = v2.revision();
        assert!(
            ctrl.current().iter().all(|r| r.revision == target),
            "all replicas on the new revision"
        );
    }

    #[tokio::test]
    async fn liveness_probe_failure_reschedules_running_replica() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let mut d = deploy(2, Strategy::default());
        d.liveness = Some(crate::spec::Probe {
            kind: crate::spec::ProbeKind::Exec { command: vec!["true".into()] },
            success_threshold: 1,
            failure_threshold: 1,
            period_secs: 1,
        });
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.converge(&fake, &d, 5, |f| f.mark_all_ready()).await.unwrap();
        assert_eq!(ctrl.current().len(), 2);
        // A running replica whose probe fails (deadlocked container) must be replaced.
        let victim = ctrl.current()[0].job_id.clone();
        fake.fail_probe_on(&victim);
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r.killed.contains(&victim), "probe-failed replica reaped");
        assert_eq!(r.placed.len(), 1, "replacement placed");
    }

    #[tokio::test]
    async fn transient_refresh_error_does_not_immediately_fail() {
        // A replica whose phase() errors is only condemned after failure_threshold misses.
        let fake = FakeDriver::new(vec![host("a", 16, 16384)]);
        let d = deploy(1, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.failure_threshold = 3;
        ctrl.converge(&fake, &d, 5, |f| f.mark_all_ready()).await.unwrap();
        let job = ctrl.current()[0].job_id.clone();
        // Remove the job from the fake so phase() errors ("unknown job").
        fake.kill("a", &job, None).await.unwrap();
        // First two ticks: errors counted but not yet Failed (no reschedule storm).
        let r1 = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r1.placed.is_empty(), "not yet condemned after 1 miss");
        let r2 = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r2.placed.is_empty(), "not yet condemned after 2 misses");
        // Third miss crosses the threshold → replica marked Failed → replacement placed.
        let r3 = ctrl.tick(&fake, &d).await.unwrap();
        assert_eq!(r3.placed.len(), 1, "condemned and replaced after threshold");
    }

    #[tokio::test]
    async fn service_is_advertised_when_ready() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let mut d = deploy(2, Strategy::default());
        d.service = "web".into();
        d.namespace = "prod".into();
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        // First tick: replicas Pending → not advertised yet.
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(!r.service_advertised, "no ready replicas yet");
        // After they become ready, the next tick advertises.
        fake.mark_all_ready();
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r.service_advertised, "service advertised once ready");
        assert!(fake.advertised_services().contains(&"ce-gke/prod/web".to_string()));
    }

    #[tokio::test]
    async fn no_service_means_no_advertise() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384)]);
        let d = deploy(1, Strategy::default()); // no service
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.converge(&fake, &d, 5, |f| f.mark_all_ready()).await.unwrap();
        assert!(fake.advertised_services().is_empty());
    }

    fn deploy_with_deps(replicas: u32, deps: &[(&str, bool)]) -> Deployment {
        let mut d = deploy(replicas, Strategy::default());
        d.namespace = "default".into();
        d.depends_on = deps
            .iter()
            .map(|(s, h)| crate::spec::DependencyRef { service: (*s).into(), healthy: *h })
            .collect();
        d
    }

    #[tokio::test]
    async fn waiting_on_unmet_dependency_places_nothing() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let d = deploy_with_deps(2, &[("db", false)]);
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        // db is not advertised -> the deployment is held waiting, nothing placed.
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r.is_waiting());
        assert_eq!(r.waiting_on, vec!["db".to_string()]);
        assert!(r.placed.is_empty());
        assert!(!r.done);
        assert_eq!(ctrl.current().len(), 0);
    }

    #[tokio::test]
    async fn dependency_becoming_ready_unblocks_placement() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let d = deploy_with_deps(2, &[("db", false)]);
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        // tick 1: waiting.
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r.is_waiting());
        // db comes up (its orchestrator advertises the namespaced service name).
        fake.set_service_ready("ce-gke/default/db", crate::deps::DepReadiness::LivePresent);
        // tick 2: gate open -> places.
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(!r.is_waiting());
        assert_eq!(r.placed.len(), 2);
        assert_eq!(ctrl.current().len(), 2);
    }

    #[tokio::test]
    async fn healthy_required_dep_waits_until_healthy() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384)]);
        let d = deploy_with_deps(1, &[("db", true)]); // requires healthy
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        // db merely present -> still waiting (healthy required).
        fake.set_service_ready("ce-gke/default/db", crate::deps::DepReadiness::LivePresent);
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r.is_waiting());
        assert!(r.placed.is_empty());
        // db healthy -> unblocked.
        fake.set_service_ready("ce-gke/default/db", crate::deps::DepReadiness::Healthy);
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(!r.is_waiting());
        assert_eq!(r.placed.len(), 1);
    }

    #[tokio::test]
    async fn dependency_disappearing_sends_dependent_back_to_waiting() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384)]);
        let d = deploy_with_deps(1, &[("db", false)]);
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        fake.set_service_ready("ce-gke/default/db", crate::deps::DepReadiness::LivePresent);
        ctrl.tick(&fake, &d).await.unwrap();
        fake.mark_all_ready();
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(!r.is_waiting());
        assert_eq!(ctrl.current().len(), 1);
        // db disappears -> next tick reports waiting again (placement gated). Existing replica is not
        // torn down by the gate (steady-state self-healing is unchanged).
        fake.clear_service_ready("ce-gke/default/db");
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(r.is_waiting());
        assert_eq!(r.waiting_on, vec!["db".to_string()]);
        assert!(r.placed.is_empty());
        assert_eq!(ctrl.current().len(), 1, "gate does not tear down running replicas");
    }

    #[tokio::test]
    async fn no_deps_behaves_exactly_as_before() {
        // A deployment without depends_on must take the unchanged fast path (always ready).
        let fake = FakeDriver::new(vec![host("a", 16, 16384), host("b", 16, 16384)]);
        let d = deploy(2, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        let r = ctrl.tick(&fake, &d).await.unwrap();
        assert!(!r.is_waiting());
        assert_eq!(r.placed.len(), 2);
    }

    #[tokio::test]
    async fn tick_to_zero_kills_all() {
        let fake = FakeDriver::new(vec![host("a", 16, 16384)]);
        let up = deploy(3, Strategy::default());
        let mut ctrl = Controller::new(None);
        ctrl.max_stale_secs = 0;
        ctrl.converge(&fake, &up, 10, |f| f.mark_all_ready()).await.unwrap();
        let zero = deploy(0, Strategy::default());
        let done = ctrl.converge(&fake, &zero, 10, |f| f.mark_all_ready()).await.unwrap();
        assert!(done.done);
        assert_eq!(ctrl.current().len(), 0);
    }
}
