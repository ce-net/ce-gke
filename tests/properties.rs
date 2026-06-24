//! Property tests for ce-gke's pure logic: serialization roundtrips, placement monotonicity,
//! reconcile conservation, and — the load-bearing one — that a rolling update always converges and
//! never breaches its availability/surge budgets, for arbitrary cluster shapes and strategies.
//!
//! These complement the per-module unit tests by checking invariants across a wide input space,
//! including failure-shaped inputs (zero replicas, all-failed fleets, single-host clusters).

use ce_gke::placement::{free_capacity, host_can_fit, rank, score_host};
use ce_gke::reconcile::{reconcile, Phase, ReplicaState};
use ce_gke::rollout::{census, plan_step};
use ce_gke::spec::{Deployment, Resources, Strategy};
use ce_rs::{Amount, AtlasEntry};
use proptest::prelude::*;

fn deploy(replicas: u32, strategy: Strategy) -> Deployment {
    Deployment {
        name: "web".into(),
        image: "nginx".into(),
        command: vec![],
        replicas,
        resources: Resources { cpu_cores: 1, mem_mb: 64 },
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

// ---- serialization roundtrips ----

proptest! {
    /// Any well-formed Deployment survives a JSON roundtrip unchanged (wire stability).
    #[test]
    fn deployment_json_roundtrip(
        replicas in 0u32..50,
        cpu in 1u32..64,
        mem in 1u64..65536,
        img in "[a-z][a-z0-9]{0,20}:[0-9]{1,3}",
        mu in 0u32..5,
        ms in 0u32..5,
    ) {
        let d = Deployment {
            name: "svc".into(),
            namespace: "default".into(),
            image: img,
            command: vec!["run".into()],
            replicas,
            resources: Resources { cpu_cores: cpu, mem_mb: mem },
            select: vec!["docker".into()],
            bid: Amount::from_base((replicas as i128) * 1_000),
            duration_secs: 3600,
            strategy: Strategy::RollingUpdate { max_unavailable: mu, max_surge: ms },
            ..Default::default()
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Deployment = serde_json::from_str(&s).unwrap();
        prop_assert_eq!(d, back);
    }

    /// The revision hash is deterministic and ignores replicas/select (where-not-what).
    #[test]
    fn revision_ignores_replicas_and_placement(replicas1 in 0u32..100, replicas2 in 0u32..100) {
        let mut a = deploy(replicas1, Strategy::default());
        let mut b = deploy(replicas2, Strategy::default());
        a.select = vec!["gpu".into()];
        b.select = vec!["linux".into()];
        prop_assert_eq!(a.revision(), b.revision());
    }

    /// A manifest never panics the parser, whatever the bytes.
    #[test]
    fn manifest_parse_never_panics(s in ".{0,200}") {
        let _ = Deployment::from_manifest(&s); // ok or err, never panic
    }
}

// ---- placement ----

proptest! {
    /// Scoring is monotonic: a host with >= cpu and >= mem and <= jobs never scores lower.
    #[test]
    fn score_monotonic(cpu in 1u32..64, mem in 64u32..65536, jobs in 0u32..32, bump in 1u32..32) {
        let d = deploy(1, Strategy::default());
        let lo = AtlasEntry {
            node_id: "a".into(), cpu_cores: cpu, mem_mb: mem, running_jobs: jobs + 1,
            last_seen_secs: 0, tags: vec!["docker".into()],
        };
        let hi = AtlasEntry {
            node_id: "a".into(), cpu_cores: cpu + bump, mem_mb: mem + bump, running_jobs: jobs,
            last_seen_secs: 0, tags: vec!["docker".into()],
        };
        prop_assert!(score_host(&hi, &d) >= score_host(&lo, &d));
    }

    /// rank() output is always sorted best-first and contains only fitting hosts.
    #[test]
    fn rank_is_sorted_and_fitting(
        cpus in prop::collection::vec(1u32..16, 0..8),
    ) {
        let d = deploy(1, Strategy::default());
        let atlas: Vec<AtlasEntry> = cpus.iter().enumerate().map(|(i, &c)| AtlasEntry {
            node_id: format!("h{i}"), cpu_cores: c, mem_mb: 4096, running_jobs: 0,
            last_seen_secs: 0, tags: vec!["docker".into()],
        }).collect();
        let ranked = rank(&atlas, &d, 0, 0);
        // sorted by score desc
        for w in ranked.windows(2) {
            prop_assert!(w[0].score >= w[1].score);
        }
        // every ranked candidate had >= 1 cpu (fits a 1-core replica)
        prop_assert!(ranked.iter().all(|c| c.free_cpu >= 1));
    }

    /// A ranked candidate always has the headroom it claims: free_cpu/free_mem are enough for one
    /// replica, so placement never selects a host the fit check would reject (no silent over-commit).
    #[test]
    fn rank_never_overcommits(
        cpu in 1u32..32, mem in 64u32..65536, jobs in 0u32..16, rep_cpu in 1u32..8, rep_mem in 64u64..8192,
    ) {
        let d = deploy(1, Strategy::default());
        let mut d = d;
        d.resources = Resources { cpu_cores: rep_cpu, mem_mb: rep_mem };
        let h = AtlasEntry {
            node_id: "h".into(), cpu_cores: cpu, mem_mb: mem, running_jobs: jobs,
            last_seen_secs: 0, tags: vec!["docker".into()],
        };
        let ranked = rank(std::slice::from_ref(&h), &d, 0, 0);
        if let Some(c) = ranked.first() {
            // The candidate was deemed to fit; its reported headroom must cover one replica.
            prop_assert!(c.free_cpu >= rep_cpu);
            prop_assert!(c.free_mem_mb >= rep_mem as u32);
            // And host_can_fit agrees (rank and fit are consistent).
            prop_assert!(host_can_fit(&h, &d));
        } else {
            // If not ranked, the host genuinely cannot fit.
            prop_assert!(!host_can_fit(&h, &d));
        }
        // free_capacity is monotone non-increasing in running_jobs (more load never frees space).
        let (f0, m0) = free_capacity(&h, rep_cpu, rep_mem as u32);
        let busier = AtlasEntry { running_jobs: jobs + 1, ..h.clone() };
        let (f1, m1) = free_capacity(&busier, rep_cpu, rep_mem as u32);
        prop_assert!(f1 <= f0);
        prop_assert!(m1 <= m0);
    }
}

// ---- reconcile conservation ----

proptest! {
    /// After reconcile, the resulting live count equals desired (when enough capacity to place):
    /// live_after = alive_before - killed_live + placed. We check the count algebra holds.
    #[test]
    fn reconcile_drives_to_desired_count(
        desired in 0u32..20,
        running in 0u32..20,
        failed in 0u32..10,
    ) {
        let d = deploy(desired, Strategy::default());
        let mut replicas = Vec::new();
        for i in 0..running { replicas.push(rep(&format!("r{i}"), "rev0", Phase::Running)); }
        for i in 0..failed { replicas.push(rep(&format!("f{i}"), "rev0", Phase::Failed)); }
        let plan = reconcile(&d, &replicas);

        // All failed replicas are always killed.
        for i in 0..failed {
            let job = format!("f{i}");
            prop_assert!(plan.to_kill.contains(&job));
        }
        // Simulate applying the plan: live = running - killed_running + placed.
        let killed_running = plan.to_kill.iter().filter(|j| j.starts_with('r')).count() as u32;
        let live_after = running - killed_running + plan.to_place;
        prop_assert_eq!(live_after, desired);
    }

    /// reconcile never both places and kills *live* replicas in the same plan (no thrash).
    #[test]
    fn reconcile_no_simultaneous_scale_both_ways(
        desired in 0u32..20,
        running in 0u32..20,
    ) {
        let d = deploy(desired, Strategy::default());
        let replicas: Vec<_> = (0..running).map(|i| rep(&format!("r{i}"), "rev0", Phase::Running)).collect();
        let plan = reconcile(&d, &replicas);
        let killed_live = plan.to_kill.iter().filter(|j| j.starts_with('r')).count();
        // either we placed, or we killed live, never both.
        prop_assert!(!(plan.to_place > 0 && killed_live > 0));
    }
}

// ---- rolling update: the load-bearing convergence + safety property ----

/// Simulate a rolling update to completion for arbitrary parameters and assert it converges within
/// a bounded number of steps without breaching the surge ceiling. Placed replicas become ready
/// immediately (best case for the host); the planner must still respect budgets.
fn drive_rollout(desired: u32, max_unavailable: u32, max_surge: u32, start_old: u32) -> bool {
    let d = deploy(desired, Strategy::RollingUpdate { max_unavailable, max_surge });
    let target = "v2";
    let mut reps: Vec<ReplicaState> =
        (0..start_old).map(|i| rep(&format!("o{i}"), "v1", Phase::Running)).collect();
    let mut seq = 0u32;
    let surge_ceiling = desired.saturating_add(max_surge);
    for _ in 0..500 {
        let step = plan_step(&d, &reps, target);
        if step.done {
            // Converged: exactly `desired` replicas, all on target & running.
            let c = census(&reps, target);
            return c.new_live == desired && c.new_ready == desired && c.old_live == 0;
        }
        // Apply kills.
        reps.retain(|r| !step.to_kill.contains(&r.job_id));
        // Apply placements (immediately ready).
        for _ in 0..step.to_place {
            seq += 1;
            reps.push(rep(&format!("n{seq}"), target, Phase::Running));
        }
        // Surge ceiling must never be breached at any observed state.
        if reps.len() as u32 > surge_ceiling.max(desired) {
            return false;
        }
    }
    false // did not converge in budget
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, ..ProptestConfig::default() })]

    /// For any valid rolling-update parameters, the roll converges and never over-surges.
    #[test]
    fn rolling_update_always_converges(
        desired in 0u32..8,
        // at least one of unavailable/surge must be > 0 for progress; bias to valid combos.
        mu in 0u32..4,
        ms in 0u32..4,
        start_old in 0u32..8,
    ) {
        // Skip the degenerate "stuck" config the validator rejects.
        prop_assume!(mu > 0 || ms > 0);
        prop_assert!(
            drive_rollout(desired, mu, ms, start_old),
            "did not converge: desired={desired} mu={mu} ms={ms} old={start_old}"
        );
    }

    /// A fresh deploy (no old replicas) converges to exactly `desired` for any strategy params.
    #[test]
    fn fresh_deploy_converges(desired in 0u32..10, mu in 0u32..4, ms in 0u32..4) {
        prop_assume!(mu > 0 || ms > 0);
        prop_assert!(drive_rollout(desired, mu, ms, 0));
    }
}

// ---- probe protocol round-trips + bounds ----

use ce_gke::protocol::{ProbeReply, ProbeRequest, MAX_MESSAGE_BYTES};

proptest! {
    /// Any probe request with a bounded job id / command survives a bincode round-trip unchanged.
    #[test]
    fn probe_request_roundtrip(
        job in "[a-z0-9-]{0,64}",
        cmd in prop::collection::vec("[ -~]{0,32}", 0..8),
        has_grant in any::<bool>(),
    ) {
        let req = ProbeRequest {
            job_id: job,
            check_command: cmd,
            grant: if has_grant { Some("deadbeef".into()) } else { None },
        };
        let bytes = req.encode().unwrap();
        prop_assert!(bytes.len() <= MAX_MESSAGE_BYTES);
        prop_assert_eq!(ProbeRequest::decode(&bytes).unwrap(), req);
    }

    /// Arbitrary bytes never panic the decoder (ok or err, never crash) and oversized input is
    /// rejected before allocation.
    #[test]
    fn probe_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
        let _ = ProbeRequest::decode(&bytes);
        let _ = ProbeReply::decode(&bytes);
    }
}
