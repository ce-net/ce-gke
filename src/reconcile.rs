//! Reconcile — the control loop's core: compute the diff between desired and actual state.
//!
//! This is the heart of any orchestrator. Given the desired [`Deployment`] and the set of replicas
//! we believe are running ([`ReplicaState`]s), produce the [`Action`]s that move actual → desired:
//! scale up (place new replicas), scale down (kill excess), restart (replace failed ones), and
//! migrate (replace replicas on the wrong revision — the rolling-update driver feeds these).
//!
//! The planner is pure — no mesh, no clock side effects beyond the `now` argument — so every branch
//! (scale up/down, failure → reschedule, revision drift) is unit-testable, and the actual mesh
//! calls live in the driver.

use serde::{Deserialize, Serialize};

use crate::spec::Deployment;

/// What a replica is doing, from the orchestrator's point of view. Derived from the host job status
/// (`pending`/`running`/`awaiting_settlement`/`settled`/`failed:*`) plus liveness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Placed on a host, container starting.
    Pending,
    /// Container is up and (as far as we know) healthy.
    Running,
    /// The host reported the cell finished or failed, or our health probe gave up. Needs replacing.
    Failed,
    /// The cell exited cleanly / settled — for a long-running service this is also "needs replacing"
    /// (the service should be perpetual), but we distinguish it for reporting.
    Succeeded,
}

impl Phase {
    /// Is this replica counted toward the desired replica total? Only live (pending/running) ones.
    pub fn is_alive(self) -> bool {
        matches!(self, Phase::Pending | Phase::Running)
    }

    /// Should this replica be replaced (it is dead or terminal)?
    pub fn is_terminal(self) -> bool {
        matches!(self, Phase::Failed | Phase::Succeeded)
    }

    /// Map a CE job status string to a phase. Unknown strings are treated as `Failed` (fail-safe:
    /// we'd rather replace a replica we cannot account for than leave the service short).
    pub fn from_job_status(status: &str) -> Phase {
        match status {
            "pending" => Phase::Pending,
            "running" => Phase::Running,
            "settled" => Phase::Succeeded,
            s if s.starts_with("failed") => Phase::Failed,
            // awaiting_settlement means it ran and is wrapping up — treat as succeeded for a service.
            "awaiting_settlement" => Phase::Succeeded,
            _ => Phase::Failed,
        }
    }
}

/// One replica the orchestrator is tracking. `revision` is the pod-template hash it was launched
/// with, so a rolling update can spot replicas on the old template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaState {
    /// Host-assigned job id (the handle for kill/status).
    pub job_id: String,
    /// The host running it.
    pub node_id: String,
    /// The pod-template revision this replica was launched with.
    pub revision: String,
    /// Current lifecycle phase.
    pub phase: Phase,
}

/// An action the driver should take to move actual state toward desired. Pure data — the driver
/// turns these into mesh-deploy / mesh-kill calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Place one new replica of the current revision (host is chosen by the driver via placement).
    Scale,
    /// Stop this running replica (excess, drained, or being rolled out).
    Kill(String),
}

/// The reconciliation outcome for one deployment: how many replicas to add, and which to remove.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Plan {
    /// New replicas to place (of the deployment's current revision).
    pub to_place: u32,
    /// Job ids to kill.
    pub to_kill: Vec<String>,
}

impl Plan {
    /// Is the world already in the desired state (nothing to do)?
    pub fn is_noop(&self) -> bool {
        self.to_place == 0 && self.to_kill.is_empty()
    }

    /// Flatten into a list of [`Action`]s (place actions first, then kills) for drivers that want a
    /// single ordered stream.
    pub fn actions(&self) -> Vec<Action> {
        let mut a: Vec<Action> = (0..self.to_place).map(|_| Action::Scale).collect();
        a.extend(self.to_kill.iter().cloned().map(Action::Kill));
        a
    }
}

/// Reconcile actual replicas against the desired count, replacing dead ones. This is the
/// *steady-state* loop (no rolling update): it does not care about revision, only liveness and
/// count. Rolling updates are layered on top (see [`crate::rollout`]).
///
/// Algorithm:
/// 1. Terminal replicas (failed/succeeded) are always killed and removed from the live count.
/// 2. If live < desired → place the shortfall.
/// 3. If live > desired → kill the excess, choosing the least-settled first (pending before
///    running, so we cancel not-yet-started work before tearing down healthy replicas).
pub fn reconcile(d: &Deployment, replicas: &[ReplicaState]) -> Plan {
    let mut plan = Plan::default();

    // 1. Reap terminal replicas.
    let mut live: Vec<&ReplicaState> = Vec::new();
    for r in replicas {
        if r.phase.is_terminal() {
            plan.to_kill.push(r.job_id.clone());
        } else {
            live.push(r);
        }
    }

    let alive = live.len() as u32;
    let desired = d.replicas;

    if alive < desired {
        // 2. Scale up by the shortfall (terminal ones already counted out, so reaping a failed
        //    replica naturally triggers its replacement here).
        plan.to_place = desired - alive;
    } else if alive > desired {
        // 3. Scale down: kill excess, pending first (cheaper to cancel), then running.
        let excess = (alive - desired) as usize;
        let mut order: Vec<&ReplicaState> = live.clone();
        // Pending sorts before Running; within a phase, deterministic by job_id.
        order.sort_by(|a, b| {
            phase_kill_rank(a.phase).cmp(&phase_kill_rank(b.phase)).then_with(|| a.job_id.cmp(&b.job_id))
        });
        for r in order.into_iter().take(excess) {
            plan.to_kill.push(r.job_id.clone());
        }
    }

    plan
}

/// Kill-preference rank: lower = killed first when scaling down. Pending work is cancelled before
/// healthy running replicas.
fn phase_kill_rank(p: Phase) -> u8 {
    match p {
        Phase::Pending => 0,
        Phase::Running => 1,
        // terminal phases never reach here (reaped separately), but rank them last for safety.
        Phase::Succeeded | Phase::Failed => 2,
    }
}

/// Count replicas by liveness for status reporting: `(pending, running, terminal)`.
pub fn tally_phases(replicas: &[ReplicaState]) -> (u32, u32, u32) {
    let mut pending = 0;
    let mut running = 0;
    let mut terminal = 0;
    for r in replicas {
        match r.phase {
            Phase::Pending => pending += 1,
            Phase::Running => running += 1,
            Phase::Failed | Phase::Succeeded => terminal += 1,
        }
    }
    (pending, running, terminal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_rs::Amount;

    fn deploy(replicas: u32) -> Deployment {
        Deployment {
            name: "web".into(),
            image: "nginx".into(),
            command: vec![],
            replicas,
            resources: Default::default(),
            select: vec![],
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Default::default(),
            ..Default::default()
        }
    }

    fn replica(job: &str, node: &str, phase: Phase) -> ReplicaState {
        ReplicaState { job_id: job.into(), node_id: node.into(), revision: "rev0".into(), phase }
    }

    #[test]
    fn phase_from_job_status() {
        assert_eq!(Phase::from_job_status("pending"), Phase::Pending);
        assert_eq!(Phase::from_job_status("running"), Phase::Running);
        assert_eq!(Phase::from_job_status("settled"), Phase::Succeeded);
        assert_eq!(Phase::from_job_status("awaiting_settlement"), Phase::Succeeded);
        assert_eq!(Phase::from_job_status("failed: oom"), Phase::Failed);
        assert_eq!(Phase::from_job_status("failed"), Phase::Failed);
        // unknown → fail-safe to Failed
        assert_eq!(Phase::from_job_status("wat"), Phase::Failed);
        assert_eq!(Phase::from_job_status(""), Phase::Failed);
    }

    #[test]
    fn phase_predicates() {
        assert!(Phase::Pending.is_alive());
        assert!(Phase::Running.is_alive());
        assert!(!Phase::Failed.is_alive());
        assert!(!Phase::Succeeded.is_alive());
        assert!(Phase::Failed.is_terminal());
        assert!(Phase::Succeeded.is_terminal());
        assert!(!Phase::Running.is_terminal());
    }

    #[test]
    fn reconcile_noop_when_at_desired() {
        let d = deploy(2);
        let replicas =
            vec![replica("j1", "a", Phase::Running), replica("j2", "b", Phase::Running)];
        let plan = reconcile(&d, &replicas);
        assert!(plan.is_noop());
    }

    #[test]
    fn reconcile_scales_up_from_zero() {
        let d = deploy(3);
        let plan = reconcile(&d, &[]);
        assert_eq!(plan.to_place, 3);
        assert!(plan.to_kill.is_empty());
    }

    #[test]
    fn reconcile_scales_up_partial() {
        let d = deploy(3);
        let replicas = vec![replica("j1", "a", Phase::Running)];
        let plan = reconcile(&d, &replicas);
        assert_eq!(plan.to_place, 2);
    }

    #[test]
    fn reconcile_scales_down_excess() {
        let d = deploy(1);
        let replicas = vec![
            replica("j1", "a", Phase::Running),
            replica("j2", "b", Phase::Running),
            replica("j3", "c", Phase::Running),
        ];
        let plan = reconcile(&d, &replicas);
        assert_eq!(plan.to_place, 0);
        assert_eq!(plan.to_kill.len(), 2);
    }

    #[test]
    fn reconcile_scale_down_kills_pending_first() {
        let d = deploy(1);
        let replicas = vec![
            replica("run", "a", Phase::Running),
            replica("pend", "b", Phase::Pending),
        ];
        let plan = reconcile(&d, &replicas);
        // one excess, pending should be the victim
        assert_eq!(plan.to_kill, vec!["pend".to_string()]);
    }

    #[test]
    fn reconcile_replaces_failed_replica() {
        // 2 desired, one running + one failed → reap the failed, place one replacement.
        let d = deploy(2);
        let replicas =
            vec![replica("ok", "a", Phase::Running), replica("dead", "b", Phase::Failed)];
        let plan = reconcile(&d, &replicas);
        assert_eq!(plan.to_kill, vec!["dead".to_string()]);
        assert_eq!(plan.to_place, 1);
    }

    #[test]
    fn reconcile_reaps_all_terminal_and_refills() {
        let d = deploy(2);
        let replicas = vec![
            replica("d1", "a", Phase::Failed),
            replica("d2", "b", Phase::Succeeded),
        ];
        let plan = reconcile(&d, &replicas);
        assert_eq!(plan.to_kill.len(), 2);
        assert_eq!(plan.to_place, 2);
    }

    #[test]
    fn reconcile_to_zero_kills_everything() {
        let d = deploy(0);
        let replicas =
            vec![replica("j1", "a", Phase::Running), replica("j2", "b", Phase::Pending)];
        let plan = reconcile(&d, &replicas);
        assert_eq!(plan.to_place, 0);
        assert_eq!(plan.to_kill.len(), 2);
    }

    #[test]
    fn plan_actions_places_then_kills() {
        let plan = Plan { to_place: 2, to_kill: vec!["x".into()] };
        let actions = plan.actions();
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0], Action::Scale);
        assert_eq!(actions[1], Action::Scale);
        assert_eq!(actions[2], Action::Kill("x".into()));
        assert!(!plan.is_noop());
    }

    #[test]
    fn tally_phases_counts() {
        let replicas = vec![
            replica("a", "h", Phase::Pending),
            replica("b", "h", Phase::Running),
            replica("c", "h", Phase::Running),
            replica("d", "h", Phase::Failed),
            replica("e", "h", Phase::Succeeded),
        ];
        assert_eq!(tally_phases(&replicas), (1, 2, 2));
    }

    #[test]
    fn reconcile_is_deterministic() {
        // Same inputs always yield the same kills (sorted), so the loop never thrashes.
        let d = deploy(1);
        let replicas = vec![
            replica("zzz", "a", Phase::Running),
            replica("aaa", "b", Phase::Running),
        ];
        let p1 = reconcile(&d, &replicas);
        let p2 = reconcile(&d, &replicas);
        assert_eq!(p1, p2);
        // deterministic tie-break: "aaa" sorts first among equal-phase running replicas.
        assert_eq!(p1.to_kill, vec!["aaa".to_string()]);
    }
}
