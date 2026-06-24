//! Rolling update — step the fleet from an old revision to a new one without breaching the
//! availability budget.
//!
//! When a [`Deployment`]'s pod template changes (new image, resources, ...), its [`revision`] hash
//! changes, and every running replica on the old revision must be replaced. A naive replace-all
//! causes downtime; a rolling update does it `max_surge` ahead / `max_unavailable` behind so the
//! service stays up.
//!
//! [`plan_step`] is a pure function: given the current set of replicas, the target revision, and
//! the strategy, it returns the *next batch* of place/kill actions. The driver applies the batch,
//! observes the new world, and calls `plan_step` again until [`RolloutStep::done`]. This makes the
//! step logic — the trickiest part of any orchestrator — fully unit-testable.
//!
//! [`revision`]: crate::spec::Deployment::revision

use crate::reconcile::{Phase, ReplicaState};
use crate::spec::{Deployment, Strategy};

/// One step of a rolling update: what to do *right now* to make progress, plus whether the rollout
/// is complete.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RolloutStep {
    /// How many new-revision replicas to place this step.
    pub to_place: u32,
    /// Old-revision (or dead) job ids to kill this step.
    pub to_kill: Vec<String>,
    /// True when every desired replica is on the target revision and healthy — nothing left to do.
    pub done: bool,
}

impl RolloutStep {
    /// Nothing to place or kill this step (but not necessarily done — may be waiting on
    /// in-flight replicas to become ready).
    pub fn is_idle(&self) -> bool {
        self.to_place == 0 && self.to_kill.is_empty()
    }
}

/// A snapshot of replica counts relevant to a rolling update, derived from `replicas` against the
/// `target` revision. Pure helper; exposed for testing/inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutCensus {
    /// Live (pending/running) replicas already on the target revision.
    pub new_live: u32,
    /// Running-and-healthy replicas on the target revision (count toward availability).
    pub new_ready: u32,
    /// Live replicas still on an old revision (must eventually be replaced).
    pub old_live: u32,
    /// Healthy (running) old-revision replicas (count toward availability until killed).
    pub old_ready: u32,
    /// Terminal replicas (any revision) that should be reaped regardless.
    pub terminal: Vec<String>,
}

/// Census the replica set against the target revision.
pub fn census(replicas: &[ReplicaState], target_rev: &str) -> RolloutCensus {
    let mut c = RolloutCensus {
        new_live: 0,
        new_ready: 0,
        old_live: 0,
        old_ready: 0,
        terminal: Vec::new(),
    };
    for r in replicas {
        if r.phase.is_terminal() {
            c.terminal.push(r.job_id.clone());
            continue;
        }
        let on_target = r.revision == target_rev;
        if on_target {
            c.new_live += 1;
            if r.phase == Phase::Running {
                c.new_ready += 1;
            }
        } else {
            c.old_live += 1;
            if r.phase == Phase::Running {
                c.old_ready += 1;
            }
        }
    }
    c
}

/// Plan the next rolling-update step.
///
/// `replicas` is everything currently tracked for the deployment; `target_rev` is
/// `d.revision()` (the desired pod template). The returned [`RolloutStep`] respects the strategy:
///
/// **RollingUpdate { max_unavailable, max_surge }** — at any moment:
/// - total live replicas must not exceed `desired + max_surge` (surge ceiling), so we only place
///   new replicas while there is surge room and we still need more new ones;
/// - available (ready) replicas must not drop below `desired - max_unavailable` (availability
///   floor), so we only kill old replicas while doing so keeps enough ready capacity.
///
/// **Recreate** — kill every old/terminal replica first; once none remain, place up to `desired`
/// new ones.
///
/// Terminal replicas are always scheduled for reaping. When `desired == 0`, the step kills all live
/// replicas and reports `done` once none remain.
pub fn plan_step(d: &Deployment, replicas: &[ReplicaState], target_rev: &str) -> RolloutStep {
    let desired = d.replicas;
    let c = census(replicas, target_rev);
    let mut step = RolloutStep::default();

    // Always reap terminal replicas (they consume a slot but contribute nothing).
    step.to_kill.extend(c.terminal.iter().cloned());

    match &d.strategy {
        Strategy::Recreate => plan_recreate(replicas, desired, target_rev, &c, &mut step),
        Strategy::RollingUpdate { max_unavailable, max_surge } => {
            plan_rolling(replicas, desired, target_rev, &c, *max_unavailable, *max_surge, &mut step)
        }
    }

    step
}

/// Recreate: drain everything not on target, then fill to desired with the new revision.
fn plan_recreate(
    replicas: &[ReplicaState],
    desired: u32,
    target_rev: &str,
    c: &RolloutCensus,
    step: &mut RolloutStep,
) {
    if c.old_live > 0 {
        // Drain all old-revision live replicas first (downtime is accepted for Recreate).
        for r in replicas {
            if !r.phase.is_terminal() && r.revision != target_rev {
                step.to_kill.push(r.job_id.clone());
            }
        }
        return;
    }
    // No old replicas left: bring the new revision up to desired.
    if c.new_live < desired {
        step.to_place = desired - c.new_live;
    } else if c.new_live > desired {
        // Over-provisioned new replicas (e.g. desired was scaled down mid-roll): trim.
        kill_excess_new(replicas, desired, target_rev, c.new_live, step);
    }
    step.done = c.new_live == desired && c.new_ready == desired;
}

/// Rolling update: surge new replicas ahead, retire old ones behind, honoring the budgets.
#[allow(clippy::too_many_arguments)]
fn plan_rolling(
    replicas: &[ReplicaState],
    desired: u32,
    target_rev: &str,
    c: &RolloutCensus,
    max_unavailable: u32,
    max_surge: u32,
    step: &mut RolloutStep,
) {
    let total_live = c.new_live + c.old_live;

    // Done: exactly `desired` replicas, all on target and ready, no old ones left.
    if c.old_live == 0 && c.new_live == desired && c.new_ready == desired {
        step.done = true;
        return;
    }

    // --- Trim excess on-target replicas (desired shrank below what we already run on target). ---
    // This is independent of old replicas: even mid-roll, if we somehow hold more new replicas than
    // desired (e.g. desired was scaled down), the surplus must go. Old replicas are retired below.
    if c.new_live > desired {
        kill_excess_new(replicas, desired, target_rev, c.new_live, step);
        // If there are no old replicas either, the trim is the whole job for this step.
        if c.old_live == 0 {
            return;
        }
    }

    // --- Surge: place new replicas while we still need more and have surge headroom ---
    // We need `desired` on the new revision eventually. We may run up to `desired + max_surge` total.
    let surge_ceiling = desired.saturating_add(max_surge);
    if c.new_live < desired {
        let want_more = desired - c.new_live;
        let surge_room = surge_ceiling.saturating_sub(total_live);
        step.to_place = want_more.min(surge_room);
    }

    // --- Retire: kill old replicas while availability stays above the floor ---
    // Availability floor: at least `desired - max_unavailable` replicas must remain ready.
    let avail_floor = desired.saturating_sub(max_unavailable);
    if c.old_live > 0 {
        // Ready capacity that will exist after this step's placements become ready is uncertain, so
        // be conservative: only count *currently ready* replicas. We may retire an old replica only
        // if the ready replicas that remain (new_ready + old_ready - 1) stay >= floor, OR if there
        // are already enough new_ready to cover the floor without this old one.
        let ready_now = c.new_ready + c.old_ready;
        // How many old-ready replicas can we drop while staying >= floor?
        let droppable_ready = ready_now.saturating_sub(avail_floor);
        // Also: any old replica that is not yet ready (pending old — rare) can be killed freely,
        // it contributes nothing to availability.
        let mut kills = 0u32;
        // First kill non-ready old replicas (they only consume surge budget).
        for r in replicas {
            if r.phase.is_terminal() {
                continue;
            }
            if r.revision != target_rev && r.phase != Phase::Running {
                step.to_kill.push(r.job_id.clone());
                kills += 1;
            }
        }
        // Then retire ready old replicas up to the droppable budget.
        let mut dropped = 0u32;
        for r in replicas {
            if dropped >= droppable_ready {
                break;
            }
            if r.phase.is_terminal() {
                continue;
            }
            if r.revision != target_rev && r.phase == Phase::Running {
                step.to_kill.push(r.job_id.clone());
                dropped += 1;
            }
        }
        let _ = kills; // (kills counted for clarity; not otherwise used)
    }
}

/// Kill `new_live - desired` excess on-target replicas (used when desired shrinks). Pending first.
fn kill_excess_new(
    replicas: &[ReplicaState],
    desired: u32,
    target_rev: &str,
    new_live: u32,
    step: &mut RolloutStep,
) {
    let excess = new_live.saturating_sub(desired) as usize;
    let mut on_target: Vec<&ReplicaState> = replicas
        .iter()
        .filter(|r| !r.phase.is_terminal() && r.revision == target_rev)
        .collect();
    // Pending before running; deterministic by job_id.
    on_target.sort_by(|a, b| {
        kill_rank(a.phase).cmp(&kill_rank(b.phase)).then_with(|| a.job_id.cmp(&b.job_id))
    });
    for r in on_target.into_iter().take(excess) {
        step.to_kill.push(r.job_id.clone());
    }
}

fn kill_rank(p: Phase) -> u8 {
    match p {
        Phase::Pending => 0,
        Phase::Running => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_rs::Amount;

    fn deploy(replicas: u32, strategy: Strategy) -> Deployment {
        Deployment {
            name: "web".into(),
            image: "nginx".into(),
            command: vec![],
            replicas,
            resources: Default::default(),
            select: vec![],
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy,
            ..Default::default()
        }
    }

    fn rep(job: &str, rev: &str, phase: Phase) -> ReplicaState {
        ReplicaState { job_id: job.into(), node_id: "h".into(), revision: rev.into(), phase }
    }

    fn rolling(mu: u32, ms: u32) -> Strategy {
        Strategy::RollingUpdate { max_unavailable: mu, max_surge: ms }
    }

    // ---- census ----

    #[test]
    fn census_classifies_replicas() {
        let reps = vec![
            rep("a", "new", Phase::Running),
            rep("b", "new", Phase::Pending),
            rep("c", "old", Phase::Running),
            rep("d", "old", Phase::Pending),
            rep("e", "old", Phase::Failed),
        ];
        let c = census(&reps, "new");
        assert_eq!(c.new_live, 2);
        assert_eq!(c.new_ready, 1);
        assert_eq!(c.old_live, 2);
        assert_eq!(c.old_ready, 1);
        assert_eq!(c.terminal, vec!["e".to_string()]);
    }

    // ---- fresh deploy (no replicas yet) ----

    #[test]
    fn rolling_fresh_deploy_surges_up_to_ceiling() {
        let d = deploy(3, rolling(1, 2));
        let step = plan_step(&d, &[], &d.revision());
        // surge ceiling = 3 + 2 = 5, but we only need 3 new → place 3.
        assert_eq!(step.to_place, 3);
        assert!(step.to_kill.is_empty());
        assert!(!step.done);
    }

    #[test]
    fn rolling_fresh_deploy_limited_by_surge_when_smaller() {
        // max_surge 0 would normally stall a fresh deploy; we model it as "place up to ceiling".
        // desired 2, surge 1 → ceiling 3, place min(2, 3-0)=2.
        let d = deploy(2, rolling(1, 1));
        let step = plan_step(&d, &[], &d.revision());
        assert_eq!(step.to_place, 2);
    }

    // ---- steady state (all on target, ready) ----

    #[test]
    fn rolling_done_when_all_on_target_and_ready() {
        let d = deploy(2, rolling(1, 1));
        let rev = d.revision();
        let reps = vec![rep("a", &rev, Phase::Running), rep("b", &rev, Phase::Running)];
        let step = plan_step(&d, &reps, &rev);
        assert!(step.done);
        assert!(step.is_idle());
    }

    #[test]
    fn rolling_not_done_while_new_pending() {
        let d = deploy(2, rolling(1, 1));
        let rev = d.revision();
        let reps = vec![rep("a", &rev, Phase::Running), rep("b", &rev, Phase::Pending)];
        let step = plan_step(&d, &reps, &rev);
        assert!(!step.done);
        // already at desired count, nothing to place; waiting on "b" to become ready.
        assert_eq!(step.to_place, 0);
        assert!(step.to_kill.is_empty());
    }

    // ---- the actual roll: old → new ----

    #[test]
    fn rolling_surges_new_before_killing_old() {
        // 2 desired, both on old revision, max_surge 1, max_unavailable 0.
        let d = deploy(2, rolling(0, 1));
        let target = "new";
        let reps = vec![rep("o1", "old", Phase::Running), rep("o2", "old", Phase::Running)];
        let step = plan_step(&d, &reps, target);
        // surge ceiling 3, total_live 2 → may place 1 new. Availability floor = 2-0 = 2; both old
        // are ready (=2), dropping one would leave 1 < floor → cannot kill any old yet.
        assert_eq!(step.to_place, 1);
        assert!(step.to_kill.is_empty(), "must not kill old while it would breach availability");
    }

    #[test]
    fn rolling_retires_old_once_new_is_ready() {
        // 2 desired, max_unavailable 1, max_surge 1. One new is up+ready, two old ready.
        let d = deploy(2, rolling(1, 1));
        let target = "new";
        let reps = vec![
            rep("n1", "new", Phase::Running),
            rep("o1", "old", Phase::Running),
            rep("o2", "old", Phase::Running),
        ];
        let step = plan_step(&d, &reps, target);
        // new_live 1 < desired 2; surge ceiling 3, total_live 3 → no surge room → place 0.
        assert_eq!(step.to_place, 0);
        // ready_now = 1 new + 2 old = 3; floor = 2-1 = 1; droppable = 3-1 = 2 → may retire old.
        assert!(!step.to_kill.is_empty(), "should retire at least one old replica");
        assert!(step.to_kill.iter().all(|j| j == "o1" || j == "o2"));
    }

    #[test]
    fn rolling_with_unavailable_budget_kills_old_immediately() {
        // max_unavailable 1: we may drop below desired by 1, so we can kill an old replica on step 1
        // even before a new one is ready.
        let d = deploy(2, rolling(1, 0)); // no surge, but 1 unavailable allowed
        let target = "new";
        let reps = vec![rep("o1", "old", Phase::Running), rep("o2", "old", Phase::Running)];
        let step = plan_step(&d, &reps, target);
        // surge ceiling = 2, total_live 2 → no surge room → place 0.
        assert_eq!(step.to_place, 0);
        // floor = 1; ready_now 2 → droppable 1 → kill exactly one old.
        assert_eq!(step.to_kill.len(), 1);
    }

    #[test]
    fn rolling_kills_non_ready_old_freely() {
        // A pending old replica contributes nothing to availability and can be killed regardless.
        let d = deploy(2, rolling(0, 0));
        let target = "new";
        let reps = vec![
            rep("n1", "new", Phase::Running),
            rep("n2", "new", Phase::Running),
            rep("opending", "old", Phase::Pending),
        ];
        let step = plan_step(&d, &reps, target);
        // new is already at desired+ready; the stray pending-old is reaped.
        assert!(step.to_kill.contains(&"opending".to_string()));
    }

    #[test]
    fn rolling_reaps_terminal_replicas() {
        let d = deploy(2, rolling(1, 1));
        let rev = d.revision();
        let reps = vec![
            rep("a", &rev, Phase::Running),
            rep("dead", &rev, Phase::Failed),
        ];
        let step = plan_step(&d, &reps, &rev);
        assert!(step.to_kill.contains(&"dead".to_string()));
        // we lost a replica → need to place one more (new_live counts only live = 1).
        assert_eq!(step.to_place, 1);
    }

    // ---- a full roll converges (drive the loop to completion) ----

    #[test]
    fn rolling_update_converges_to_target() {
        let d = deploy(3, rolling(1, 1));
        let target = "v2";
        // Start: 3 old replicas, all running.
        let mut reps = vec![
            rep("o1", "v1", Phase::Running),
            rep("o2", "v1", Phase::Running),
            rep("o3", "v1", Phase::Running),
        ];
        let mut seq = 0;
        // Simulate the driver: apply step, mark placed replicas Running, remove killed, repeat.
        for _ in 0..50 {
            let step = plan_step(&d, &reps, target);
            if step.done {
                break;
            }
            // apply kills
            reps.retain(|r| !step.to_kill.contains(&r.job_id));
            // apply placements (immediately ready, to model an eventually-healthy roll)
            for _ in 0..step.to_place {
                seq += 1;
                reps.push(rep(&format!("n{seq}"), target, Phase::Running));
            }
            // safety: never breach surge ceiling
            assert!(reps.len() as u32 <= d.replicas + 1, "surge ceiling breached: {}", reps.len());
        }
        let final_step = plan_step(&d, &reps, target);
        assert!(final_step.done, "roll did not converge");
        assert_eq!(reps.len(), 3);
        assert!(reps.iter().all(|r| r.revision == target && r.phase == Phase::Running));
    }

    #[test]
    fn rolling_never_breaches_availability_floor_during_roll() {
        // max_unavailable 0 → availability must never dip below desired among *ready* replicas at
        // the moment we issue kills (we only count currently-ready).
        let d = deploy(4, rolling(0, 2));
        let target = "v2";
        let mut reps: Vec<ReplicaState> =
            (0..4).map(|i| rep(&format!("o{i}"), "v1", Phase::Running)).collect();
        let mut seq = 0;
        for _ in 0..50 {
            let step = plan_step(&d, &reps, target);
            if step.done {
                break;
            }
            // count ready that would survive this step's kills
            let surviving_ready = reps
                .iter()
                .filter(|r| r.phase == Phase::Running && !step.to_kill.contains(&r.job_id))
                .count() as u32;
            // max_unavailable is 0 here, so the floor is exactly `desired`.
            assert!(
                surviving_ready >= d.replicas,
                "availability floor breached: {surviving_ready} ready after kills"
            );
            reps.retain(|r| !step.to_kill.contains(&r.job_id));
            for _ in 0..step.to_place {
                seq += 1;
                reps.push(rep(&format!("n{seq}"), target, Phase::Running));
            }
        }
        assert!(plan_step(&d, &reps, target).done);
    }

    // ---- Recreate strategy ----

    #[test]
    fn recreate_drains_all_then_fills() {
        let d = deploy(2, Strategy::Recreate);
        let target = "v2";
        let reps = vec![rep("o1", "v1", Phase::Running), rep("o2", "v1", Phase::Running)];
        // Step 1: drain all old.
        let s1 = plan_step(&d, &reps, target);
        assert_eq!(s1.to_kill.len(), 2);
        assert_eq!(s1.to_place, 0);
        assert!(!s1.done);
        // Step 2: nothing old left → place 2 new.
        let s2 = plan_step(&d, &[], target);
        assert_eq!(s2.to_place, 2);
        assert!(s2.to_kill.is_empty());
    }

    #[test]
    fn recreate_done_when_new_ready() {
        let d = deploy(2, Strategy::Recreate);
        let target = "v2";
        let reps = vec![rep("n1", target, Phase::Running), rep("n2", target, Phase::Running)];
        assert!(plan_step(&d, &reps, target).done);
    }

    // ---- scale-down mid-state ----

    #[test]
    fn scale_down_on_target_trims_excess() {
        // desired shrank to 1; we have 3 on target.
        let d = deploy(1, rolling(1, 1));
        let target = "v2";
        let reps = vec![
            rep("n1", target, Phase::Running),
            rep("n2", target, Phase::Running),
            rep("n3", target, Phase::Pending),
        ];
        let step = plan_step(&d, &reps, target);
        // 2 excess; pending killed first.
        assert_eq!(step.to_kill.len(), 2);
        assert!(step.to_kill.contains(&"n3".to_string()), "pending should be trimmed first");
    }

    // ---- desired == 0 ----

    #[test]
    fn rolling_to_zero_kills_all_then_done() {
        let d = deploy(0, rolling(1, 1));
        let target = "v2";
        let reps = vec![rep("a", "v1", Phase::Running), rep("b", target, Phase::Running)];
        let step = plan_step(&d, &reps, target);
        assert_eq!(step.to_place, 0);
        assert_eq!(step.to_kill.len(), 2);
        // after all gone → done
        assert!(plan_step(&d, &[], target).done);
    }

    #[test]
    fn step_is_idle_helper() {
        let s = RolloutStep { to_place: 0, to_kill: vec![], done: false };
        assert!(s.is_idle());
        let s = RolloutStep { to_place: 1, to_kill: vec![], done: false };
        assert!(!s.is_idle());
    }
}
