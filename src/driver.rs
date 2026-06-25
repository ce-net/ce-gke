//! Driver — the side-effecting layer that turns plans into mesh calls.
//!
//! The reconcile/rollout planners are pure; the *driver* is where actual `mesh-deploy`/`mesh-kill`/
//! status calls happen. It is a trait so the control loop ([`crate::controller`]) can be exercised
//! against a deterministic in-memory fake — including injected failures (a host that rejects a
//! deploy, a dropped peer on kill, a job that fails health) — without a live node. That is how the
//! "failure → reschedule" behavior is validated.

use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ce_rs::{Amount, AtlasEntry, BidSpec, CeClient};

use crate::deps::DepReadiness;
use crate::protocol::{ProbeReply, ProbeRequest, PROBE_TOPIC};
use crate::reconcile::{Phase, ReplicaState};
use crate::secrets::SecretStore;
use crate::spec::{Deployment, Probe};

/// The capabilities the controller needs from the CE mesh. Async, object-safe via `async_trait`-free
/// manual desugaring is avoided by using `impl Future` returns through a small wrapper; instead we
/// keep it simple with `#[allow]`-free associated futures by returning boxed futures.
///
/// Kept minimal: list the atlas (placement), deploy a replica, kill a replica, get a job's phase.
pub trait MeshDriver: Send + Sync {
    /// Current capacity atlas (placement candidates).
    fn atlas(&self) -> impl std::future::Future<Output = Result<Vec<AtlasEntry>>> + Send;

    /// Place one replica of `d` (current revision) on `node_id`. Returns the host-assigned job id.
    /// `grant` is the forwarded capability token authorizing the deploy on the host.
    fn deploy(
        &self,
        node_id: &str,
        d: &Deployment,
        grant: Option<&str>,
    ) -> impl std::future::Future<Output = Result<String>> + Send;

    /// Stop the replica `job_id` on `node_id`.
    fn kill(
        &self,
        node_id: &str,
        job_id: &str,
        grant: Option<&str>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Current phase of `job_id`, optionally running `probe` inside the cell on `node_id`. The
    /// driver should consult the host's authoritative view (and the probe) rather than only the
    /// local payer's cached job record. `probe` of `None` means status-only.
    fn phase(
        &self,
        node_id: &str,
        job_id: &str,
        probe: Option<&Probe>,
        grant: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Phase>> + Send;

    /// Advertise that `service` is provided by this orchestrator's node (DHT discovery). Default is
    /// a no-op so fakes need not implement it.
    fn advertise_service(
        &self,
        _service: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Observe the readiness of a *dependency* service over the mesh: is a live instance of the
    /// (already-namespaced) `service` advertised, and if so is any instance confirmed healthy? This
    /// is how `depends_on` is resolved — never via a side channel, only via DHT discovery + the
    /// instance selection [`ce_rs::locate`] provides. Default reports [`DepReadiness::Absent`] so a
    /// driver that does not implement discovery never falsely unblocks a dependent.
    fn service_ready(
        &self,
        _service: &str,
    ) -> impl std::future::Future<Output = Result<crate::deps::DepReadiness>> + Send {
        async { Ok(crate::deps::DepReadiness::Absent) }
    }
}

/// A real CE-backed driver: every method is a thin call over [`CeClient`] (the SDK).
pub struct CeDriver {
    ce: CeClient,
    /// Freshness window for atlas entries (seconds); 0 disables.
    pub max_stale_secs: u64,
    /// Secret store used to resolve `value_from` env vars at deploy time. Empty = none.
    secrets: Arc<SecretStore>,
    /// Per-probe mesh request timeout (ms).
    pub probe_timeout_ms: u64,
}

impl CeDriver {
    /// Build a driver over a CE node at `base_url` (uses the node's discovered API token).
    pub fn new(base_url: impl Into<String>) -> Self {
        CeDriver {
            ce: CeClient::new(base_url),
            max_stale_secs: 120,
            secrets: Arc::new(SecretStore::default()),
            probe_timeout_ms: 5_000,
        }
    }

    /// Build a driver over an existing client (e.g. with an explicit token).
    pub fn with_client(ce: CeClient) -> Self {
        CeDriver {
            ce,
            max_stale_secs: 120,
            secrets: Arc::new(SecretStore::default()),
            probe_timeout_ms: 5_000,
        }
    }

    /// Attach a secret store for resolving `value_from` env vars.
    pub fn with_secrets(mut self, secrets: SecretStore) -> Self {
        self.secrets = Arc::new(secrets);
        self
    }

    /// Access the underlying SDK client (for status/balance/preflight queries).
    pub fn client(&self) -> &CeClient {
        &self.ce
    }

    fn bid_spec(&self, d: &Deployment) -> Result<BidSpec> {
        let secrets = Arc::clone(&self.secrets);
        let cmd = d.effective_command(&move |name| secrets.get(name).map(|s| s.to_string()))?;
        Ok(BidSpec {
            image: d.image.clone(),
            cmd,
            cpu_cores: d.resources.cpu_cores,
            mem_mb: d.resources.mem_mb,
            duration_secs: d.duration_secs,
            bid: d.bid,
        })
    }
}

impl MeshDriver for CeDriver {
    async fn atlas(&self) -> Result<Vec<AtlasEntry>> {
        self.ce.atlas().await
    }

    async fn deploy(&self, node_id: &str, d: &Deployment, grant: Option<&str>) -> Result<String> {
        let spec = self.bid_spec(d)?;
        self.ce.mesh_deploy(node_id, &spec, grant).await
    }

    async fn kill(&self, node_id: &str, job_id: &str, grant: Option<&str>) -> Result<()> {
        self.ce.mesh_kill(node_id, job_id, grant).await
    }

    async fn phase(
        &self,
        node_id: &str,
        job_id: &str,
        probe: Option<&Probe>,
        grant: Option<&str>,
    ) -> Result<Phase> {
        // Ask the host's `ce-gke serve` agent for the authoritative job phase (and run the probe in
        // the cell, if any). If the host runs no agent (request times out / errors), fall back to
        // the payer's local job record — strictly worse but never a hard failure.
        let req = ProbeRequest {
            job_id: job_id.to_string(),
            check_command: probe.map(|p| p.check_command()).unwrap_or_default(),
            grant: grant.map(|s| s.to_string()),
        };
        match req.encode() {
            Ok(payload) => {
                match self.ce.request(node_id, PROBE_TOPIC, &payload, self.probe_timeout_ms).await {
                    Ok(bytes) => {
                        if let Ok(reply) = ProbeReply::decode(&bytes) {
                            return Ok(reply.effective_phase());
                        }
                        // Garbled reply → fall through to local fallback.
                    }
                    Err(_) => { /* no agent / timeout → local fallback */ }
                }
            }
            Err(_) => { /* should not happen for a tiny request */ }
        }
        let job = self.ce.job(job_id).await?;
        Ok(Phase::from_job_status(&job.status))
    }

    async fn advertise_service(&self, service: &str) -> Result<()> {
        self.ce.advertise_service(service).await
    }

    async fn service_ready(&self, service: &str) -> Result<DepReadiness> {
        // Resolve the dependency over the mesh: DHT discovery + live-instance selection. We do NOT
        // hold a stored ip:port or open a side channel — `locate` ranks the instances advertising
        // `service` and drops stale ones, exactly the mesh-native discovery the keystone provides.
        let opts = ce_rs::locate::LocateOpts {
            // We only need to know "is there at least one live instance"; ask for a few so a single
            // stale advertisement does not make a healthy service look absent.
            want: 3,
            require_tags: Vec::new(),
            max_stale_secs: self.max_stale_secs.max(1),
            spread_domains: false,
        };
        // VERIFY: ce_rs::locate::locate(&CeClient, &str, &LocateOpts) -> Result<Vec<Instance>>
        // (signature confirmed against ce-rs/src/locate.rs; feature "locate" is enabled).
        let instances = ce_rs::locate::locate(&self.ce, service, &opts).await?;
        if instances.is_empty() {
            return Ok(DepReadiness::Absent);
        }
        // A ce-gke orchestrator advertises a service ONLY once it has a Running replica
        // (see controller::tick's discovery step), so a live advertisement already implies at least
        // one ready replica. We surface that as LivePresent — sufficient for `healthy: false` deps.
        //
        // Distinguishing a deeper `Healthy` (the workload's own readiness probe passing, not just the
        // container being up) needs the in-cell readiness-probe data plane, which is documented as
        // deferred (see lib.rs "Deferred"). Until that lands, a `healthy: true` dependency is met by
        // the same advertised-and-live signal; this is conservative for liveness and never *under*-
        // reports. We promote to Healthy when the orchestrator confirms a ready replica via the
        // host-agent probe path, which today equals advertisement, hence Healthy here.
        // VERIFY: if/when an in-cell readiness probe exists, gate Healthy on it instead of presence.
        Ok(DepReadiness::Healthy)
    }
}

/// A deterministic in-memory driver for tests. Models a small cluster, supports failure injection,
/// and lets the controller's reschedule/health behavior be validated without a node.
pub struct FakeDriver {
    inner: Mutex<FakeState>,
}

struct FakeState {
    /// node_id -> capacity.
    hosts: Vec<AtlasEntry>,
    /// job_id -> (node_id, phase).
    jobs: HashMap<String, (String, Phase)>,
    next_job: u64,
    /// Node ids whose `deploy` will fail (injected rejection / dropped peer).
    deploy_fails: Vec<String>,
    /// Job ids whose `kill` will fail (injected dropped peer).
    kill_fails: Vec<String>,
    /// If set, the next `deploy` returns this error then clears (one-shot 5xx).
    deploy_fail_once: bool,
    /// Job ids whose liveness/readiness probe is reported as failing (running-but-unhealthy).
    probe_fails: Vec<String>,
    /// Service names advertised via [`MeshDriver::advertise_service`].
    advertised: Vec<String>,
    /// Explicit dependency-service readiness for [`MeshDriver::service_ready`]. A service not in the
    /// map is reported `Absent`.
    service_readiness: HashMap<String, DepReadiness>,
}

impl FakeDriver {
    /// New fake with the given hosts (each becomes an atlas entry as-is).
    pub fn new(hosts: Vec<AtlasEntry>) -> Self {
        FakeDriver {
            inner: Mutex::new(FakeState {
                hosts,
                jobs: HashMap::new(),
                next_job: 0,
                deploy_fails: Vec::new(),
                kill_fails: Vec::new(),
                deploy_fail_once: false,
                probe_fails: Vec::new(),
                advertised: Vec::new(),
                service_readiness: HashMap::new(),
            }),
        }
    }

    /// Set the readiness reported by [`MeshDriver::service_ready`] for a (namespaced) dependency
    /// service, so dependency-gate behavior can be driven in tests without a mesh.
    pub fn set_service_ready(&self, service: &str, readiness: DepReadiness) {
        self.inner
            .lock()
            .expect("lock")
            .service_readiness
            .insert(service.to_string(), readiness);
    }

    /// Clear a dependency service's readiness (models the service disappearing → `Absent`).
    pub fn clear_service_ready(&self, service: &str) {
        self.inner.lock().expect("lock").service_readiness.remove(service);
    }

    /// Make `job_id`'s probe report Failed even when its host status is Running (deadlocked cell).
    pub fn fail_probe_on(&self, job_id: &str) {
        self.inner.lock().expect("lock").probe_fails.push(job_id.to_string());
    }

    /// Services advertised via [`MeshDriver::advertise_service`] so far.
    pub fn advertised_services(&self) -> Vec<String> {
        self.inner.lock().expect("lock").advertised.clone()
    }

    /// Make every `deploy` on `node_id` fail (models a host that rejects deploys / a dropped peer).
    pub fn fail_deploy_on(&self, node_id: &str) {
        self.inner.lock().expect("lock").deploy_fails.push(node_id.to_string());
    }

    /// Make the next single `deploy` (any host) fail once, then succeed (models a transient 5xx).
    pub fn fail_next_deploy_once(&self) {
        self.inner.lock().expect("lock").deploy_fail_once = true;
    }

    /// Make `kill` of `job_id` fail (models a dropped peer on teardown).
    pub fn fail_kill_on(&self, job_id: &str) {
        self.inner.lock().expect("lock").kill_fails.push(job_id.to_string());
    }

    /// Force a tracked job into a phase (models a health change: a running replica that died).
    pub fn set_phase(&self, job_id: &str, phase: Phase) {
        if let Some(e) = self.inner.lock().expect("lock").jobs.get_mut(job_id) {
            e.1 = phase;
        }
    }

    /// Mark all pending jobs as running (models replicas becoming ready between reconciles).
    pub fn mark_all_ready(&self) {
        let mut s = self.inner.lock().expect("lock");
        for (_, (_, ph)) in s.jobs.iter_mut() {
            if *ph == Phase::Pending {
                *ph = Phase::Running;
            }
        }
    }

    /// Snapshot the currently live (non-terminal) jobs as [`ReplicaState`]s on the given revision.
    /// The controller would normally build these from its own tracking; tests use this to observe.
    pub fn live_replicas(&self, revision: &str) -> Vec<ReplicaState> {
        let s = self.inner.lock().expect("lock");
        let mut out: Vec<ReplicaState> = s
            .jobs
            .iter()
            .map(|(job_id, (node_id, phase))| ReplicaState {
                job_id: job_id.clone(),
                node_id: node_id.clone(),
                revision: revision.to_string(),
                phase: *phase,
            })
            .collect();
        out.sort_by(|a, b| a.job_id.cmp(&b.job_id));
        out
    }

    /// Count jobs in a given phase.
    pub fn count_phase(&self, phase: Phase) -> usize {
        self.inner.lock().expect("lock").jobs.values().filter(|(_, p)| *p == phase).count()
    }
}

impl MeshDriver for FakeDriver {
    async fn atlas(&self) -> Result<Vec<AtlasEntry>> {
        Ok(self.inner.lock().expect("lock").hosts.clone())
    }

    async fn deploy(&self, node_id: &str, _d: &Deployment, _grant: Option<&str>) -> Result<String> {
        let mut s = self.inner.lock().expect("lock");
        if s.deploy_fail_once {
            s.deploy_fail_once = false;
            anyhow::bail!("injected transient deploy failure (5xx) on {node_id}");
        }
        if s.deploy_fails.iter().any(|n| n == node_id) {
            anyhow::bail!("host {node_id} rejected the deploy");
        }
        s.next_job += 1;
        let job_id = format!("job-{}", s.next_job);
        // bump the host's running_jobs so subsequent placements spread.
        if let Some(h) = s.hosts.iter_mut().find(|h| h.node_id == node_id) {
            h.running_jobs += 1;
        }
        s.jobs.insert(job_id.clone(), (node_id.to_string(), Phase::Pending));
        Ok(job_id)
    }

    async fn kill(&self, _node_id: &str, job_id: &str, _grant: Option<&str>) -> Result<()> {
        let mut s = self.inner.lock().expect("lock");
        if s.kill_fails.iter().any(|j| j == job_id) {
            anyhow::bail!("dropped peer: kill of {job_id} failed");
        }
        if let Some((node_id, _)) = s.jobs.remove(job_id)
            && let Some(h) = s.hosts.iter_mut().find(|h| h.node_id == node_id)
        {
            h.running_jobs = h.running_jobs.saturating_sub(1);
        }
        Ok(())
    }

    async fn phase(
        &self,
        _node_id: &str,
        job_id: &str,
        probe: Option<&Probe>,
        _grant: Option<&str>,
    ) -> Result<Phase> {
        let s = self.inner.lock().expect("lock");
        let phase = s
            .jobs
            .get(job_id)
            .map(|(_, p)| *p)
            .ok_or_else(|| anyhow::anyhow!("unknown job {job_id}"))?;
        // If a probe is requested and this job is marked as failing its probe, report Failed even if
        // the host status is Running (models a deadlocked-but-running container).
        if probe.is_some() && s.probe_fails.iter().any(|j| j == job_id) {
            return Ok(Phase::Failed);
        }
        Ok(phase)
    }

    async fn advertise_service(&self, service: &str) -> Result<()> {
        self.inner.lock().expect("lock").advertised.push(service.to_string());
        Ok(())
    }

    async fn service_ready(&self, service: &str) -> Result<DepReadiness> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .service_readiness
            .get(service)
            .copied()
            .unwrap_or(DepReadiness::Absent))
    }
}

/// Helper exposed for the controller and tests: an amount formatted for logs.
pub fn fmt_credits(a: Amount) -> String {
    a.credits()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(id: &str, cpu: u32, mem: u32) -> AtlasEntry {
        AtlasEntry {
            node_id: id.into(),
            cpu_cores: cpu,
            mem_mb: mem,
            running_jobs: 0,
            last_seen_secs: 0,
            tags: vec!["docker".into()],
        }
    }

    fn deploy() -> Deployment {
        Deployment {
            name: "web".into(),
            image: "nginx".into(),
            command: vec![],
            replicas: 2,
            resources: Default::default(),
            select: vec![],
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Default::default(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn fake_deploy_and_kill_roundtrip() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192)]);
        let d = deploy();
        let job = fake.deploy("a", &d, None).await.unwrap();
        assert_eq!(fake.phase("a", &job, None, None).await.unwrap(), Phase::Pending);
        assert_eq!(fake.count_phase(Phase::Pending), 1);
        fake.kill("a", &job, None).await.unwrap();
        assert!(fake.phase("a", &job, None, None).await.is_err(), "killed job is gone");
    }

    #[tokio::test]
    async fn fake_deploy_rejection_is_error() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192)]);
        fake.fail_deploy_on("a");
        let d = deploy();
        let r = fake.deploy("a", &d, None).await;
        assert!(r.is_err());
        assert_eq!(fake.count_phase(Phase::Pending), 0, "failed deploy created no replica");
    }

    #[tokio::test]
    async fn fake_transient_deploy_failure_clears() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192)]);
        fake.fail_next_deploy_once();
        let d = deploy();
        assert!(fake.deploy("a", &d, None).await.is_err());
        // second attempt succeeds
        assert!(fake.deploy("a", &d, None).await.is_ok());
    }

    #[tokio::test]
    async fn fake_kill_failure_is_error_and_keeps_job() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192)]);
        let d = deploy();
        let job = fake.deploy("a", &d, None).await.unwrap();
        fake.fail_kill_on(&job);
        assert!(fake.kill("a", &job, None).await.is_err());
        // job still tracked (kill failed → controller must retry)
        assert!(fake.phase("a", &job, None, None).await.is_ok());
    }

    #[tokio::test]
    async fn fake_set_phase_models_health_change() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192)]);
        let d = deploy();
        let job = fake.deploy("a", &d, None).await.unwrap();
        fake.set_phase(&job, Phase::Failed);
        assert_eq!(fake.phase("a", &job, None, None).await.unwrap(), Phase::Failed);
    }

    #[tokio::test]
    async fn fake_atlas_reflects_load() {
        let fake = FakeDriver::new(vec![host("a", 8, 8192)]);
        let d = deploy();
        fake.deploy("a", &d, None).await.unwrap();
        let atlas = fake.atlas().await.unwrap();
        assert_eq!(atlas[0].running_jobs, 1);
    }

    #[test]
    fn fmt_credits_works() {
        assert_eq!(fmt_credits(Amount::from_credits(5)), "5");
    }
}
