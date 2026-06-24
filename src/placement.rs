//! Placement — rank atlas hosts for a deployment's replicas.
//!
//! This is the GKE scheduler's "node fit + scoring" stage, but over CE's atlas instead of a
//! cluster API. A host is a *candidate* if it advertises every required tag and has enough free
//! CPU/memory headroom for one replica. Candidates are then *scored* so the orchestrator spreads
//! replicas across the least-loaded, most-capable hosts rather than stacking them on one box.
//!
//! All functions here are pure (atlas snapshot in, ranking out) so the scoring policy is fully
//! unit- and property-testable without a live mesh.

use ce_rs::AtlasEntry;

use crate::spec::Deployment;

/// A scored placement candidate. Higher `score` = better fit. `score` is deterministic given the
/// atlas entry + deployment, so placement is reproducible (auditable, like seeding from the beacon).
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub node_id: String,
    /// Fit score (higher is better). See [`score_host`].
    pub score: i64,
    /// Free CPU cores at scoring time (capacity minus running jobs' rough footprint).
    pub free_cpu: u32,
    /// Free memory (MiB) at scoring time.
    pub free_mem_mb: u32,
}

/// Does this host advertise every tag the deployment requires?
pub fn host_matches_tags(host: &AtlasEntry, required: &[String]) -> bool {
    required.iter().all(|t| host.has_tag(t))
}

/// Rough free capacity of a host: total capacity minus an estimate of what its running jobs already
/// consume. We do not know exact per-job sizes from the atlas, so we charge each running job one
/// core and a memory slice; this is a conservative bin-packing heuristic, never an over-commit.
///
/// Returns `(free_cpu, free_mem_mb)`, both saturating at zero.
pub fn free_capacity(host: &AtlasEntry, per_replica_cpu: u32, per_replica_mem_mb: u32) -> (u32, u32) {
    // Charge each already-running job the size of one replica of *this* deployment as the best
    // available estimate of contention. This keeps a busy host from looking artificially empty.
    let cpu_used = host.running_jobs.saturating_mul(per_replica_cpu.max(1));
    let mem_used = host.running_jobs.saturating_mul(per_replica_mem_mb.max(1));
    let free_cpu = host.cpu_cores.saturating_sub(cpu_used);
    let free_mem = host.mem_mb.saturating_sub(mem_used);
    (free_cpu, free_mem)
}

/// Can this host fit one more replica of the deployment, given its current load?
pub fn host_can_fit(host: &AtlasEntry, d: &Deployment) -> bool {
    if !host_matches_tags(host, &d.required_tags()) {
        return false;
    }
    let (free_cpu, free_mem) = free_capacity(host, d.resources.cpu_cores, d.resources.mem_mb as u32);
    free_cpu >= d.resources.cpu_cores && free_mem >= d.resources.mem_mb as u32
}

/// Score a host for a deployment. Higher is better. Policy:
/// - prefer hosts with more free headroom (so replicas spread to the emptiest hosts),
/// - lightly penalize already-running jobs (spread, not stack),
/// - stale hosts (not seen recently) are filtered out earlier; staleness is not scored here.
///
/// The exact weights are a heuristic; the property that matters (and is tested) is *monotonicity*:
/// more free capacity and fewer running jobs never lowers the score.
pub fn score_host(host: &AtlasEntry, d: &Deployment) -> i64 {
    let (free_cpu, free_mem) = free_capacity(host, d.resources.cpu_cores, d.resources.mem_mb as u32);
    // Memory is the usually-scarcer resource; weight it but keep cpu meaningful.
    let headroom = (free_cpu as i64) * 100 + (free_mem as i64);
    let load_penalty = (host.running_jobs as i64) * 50;
    headroom - load_penalty
}

/// Rank all hosts in the atlas that can fit a replica of `d`, best-first. Ties broken by node_id
/// for deterministic, reproducible output. `now_secs` and `max_stale_secs` drop hosts whose last
/// capacity signal is older than the freshness window (a silent/dead host is not a candidate); pass
/// `max_stale_secs == 0` to disable the freshness filter (useful in tests with synthetic timestamps).
pub fn rank(atlas: &[AtlasEntry], d: &Deployment, now_secs: u64, max_stale_secs: u64) -> Vec<Candidate> {
    let mut cands: Vec<Candidate> = atlas
        .iter()
        .filter(|h| max_stale_secs == 0 || now_secs.saturating_sub(h.last_seen_secs) <= max_stale_secs)
        .filter(|h| host_can_fit(h, d))
        .map(|h| {
            let (free_cpu, free_mem) =
                free_capacity(h, d.resources.cpu_cores, d.resources.mem_mb as u32);
            Candidate {
                node_id: h.node_id.clone(),
                score: score_host(h, d),
                free_cpu,
                free_mem_mb: free_mem,
            }
        })
        .collect();
    // Best score first; deterministic tie-break by node_id.
    cands.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.node_id.cmp(&b.node_id)));
    cands
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_rs::Amount;

    fn host(id: &str, cpu: u32, mem: u32, jobs: u32, seen: u64, tags: &[&str]) -> AtlasEntry {
        AtlasEntry {
            node_id: id.to_string(),
            cpu_cores: cpu,
            mem_mb: mem,
            running_jobs: jobs,
            last_seen_secs: seen,
            tags: tags.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn deploy(cpu: u32, mem: u64, select: &[&str]) -> Deployment {
        Deployment {
            name: "web".into(),
            image: "nginx".into(),
            command: vec![],
            replicas: 3,
            resources: crate::spec::Resources { cpu_cores: cpu, mem_mb: mem },
            select: select.iter().map(|s| s.to_string()).collect(),
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Default::default(),
            ..Default::default()
        }
    }

    #[test]
    fn tag_matching() {
        let h = host("a", 4, 4096, 0, 0, &["docker", "gpu", "linux"]);
        assert!(host_matches_tags(&h, &["docker".into()]));
        assert!(host_matches_tags(&h, &["docker".into(), "gpu".into()]));
        assert!(!host_matches_tags(&h, &["docker".into(), "windows".into()]));
    }

    #[test]
    fn free_capacity_saturates_at_zero() {
        // A host with more running jobs than cores cannot go negative.
        let h = host("a", 2, 1024, 10, 0, &["docker"]);
        let (cpu, mem) = free_capacity(&h, 1, 256);
        assert_eq!(cpu, 0);
        assert_eq!(mem, 0);
    }

    #[test]
    fn free_capacity_charges_running_jobs() {
        let h = host("a", 8, 8192, 2, 0, &["docker"]);
        let (cpu, mem) = free_capacity(&h, 1, 1024);
        // 2 running jobs each charged 1 cpu / 1024 MiB.
        assert_eq!(cpu, 8 - 2);
        assert_eq!(mem, 8192 - 2 * 1024);
    }

    #[test]
    fn host_can_fit_requires_tags_and_headroom() {
        let d = deploy(2, 1024, &["gpu"]);
        // right tags, plenty of room
        assert!(host_can_fit(&host("a", 8, 8192, 0, 0, &["docker", "gpu"]), &d));
        // missing gpu tag
        assert!(!host_can_fit(&host("b", 8, 8192, 0, 0, &["docker"]), &d));
        // missing docker tag
        assert!(!host_can_fit(&host("c", 8, 8192, 0, 0, &["gpu"]), &d));
        // not enough cpu
        assert!(!host_can_fit(&host("d", 1, 8192, 0, 0, &["docker", "gpu"]), &d));
        // not enough mem
        assert!(!host_can_fit(&host("e", 8, 512, 0, 0, &["docker", "gpu"]), &d));
    }

    #[test]
    fn rank_orders_emptiest_first() {
        let d = deploy(1, 256, &[]);
        let atlas = vec![
            host("busy", 8, 8192, 6, 100, &["docker"]),
            host("empty", 8, 8192, 0, 100, &["docker"]),
            host("mid", 8, 8192, 3, 100, &["docker"]),
        ];
        let r = rank(&atlas, &d, 100, 0);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].node_id, "empty");
        assert_eq!(r[1].node_id, "mid");
        assert_eq!(r[2].node_id, "busy");
    }

    #[test]
    fn rank_excludes_non_matching_and_full_hosts() {
        let d = deploy(2, 1024, &["gpu"]);
        let atlas = vec![
            host("ok", 8, 8192, 0, 100, &["docker", "gpu"]),
            host("nogpu", 8, 8192, 0, 100, &["docker"]),
            host("full", 2, 1024, 1, 100, &["docker", "gpu"]),
        ];
        let r = rank(&atlas, &d, 100, 0);
        assert_eq!(r.iter().map(|c| c.node_id.as_str()).collect::<Vec<_>>(), vec!["ok"]);
    }

    #[test]
    fn rank_drops_stale_hosts() {
        let d = deploy(1, 256, &[]);
        let atlas = vec![
            host("fresh", 8, 8192, 0, 1000, &["docker"]),
            host("stale", 8, 8192, 0, 100, &["docker"]),
        ];
        // now=1100, window=200 → stale (last seen 100, age 1000) dropped; fresh (age 100) kept.
        let r = rank(&atlas, &d, 1100, 200);
        assert_eq!(r.iter().map(|c| c.node_id.as_str()).collect::<Vec<_>>(), vec!["fresh"]);
    }

    #[test]
    fn rank_tie_break_is_deterministic() {
        let d = deploy(1, 256, &[]);
        // identical hosts → ordered by node_id
        let atlas = vec![
            host("zzz", 4, 4096, 0, 100, &["docker"]),
            host("aaa", 4, 4096, 0, 100, &["docker"]),
            host("mmm", 4, 4096, 0, 100, &["docker"]),
        ];
        let r = rank(&atlas, &d, 100, 0);
        assert_eq!(r.iter().map(|c| c.node_id.as_str()).collect::<Vec<_>>(), vec!["aaa", "mmm", "zzz"]);
    }

    #[test]
    fn rank_empty_atlas_is_empty() {
        assert!(rank(&[], &deploy(1, 256, &[]), 0, 0).is_empty());
    }

    #[test]
    fn score_is_monotonic_in_free_capacity() {
        let d = deploy(1, 256, &[]);
        let small = host("a", 2, 1024, 0, 0, &["docker"]);
        let big = host("b", 16, 16384, 0, 0, &["docker"]);
        assert!(score_host(&big, &d) > score_host(&small, &d));
    }

    #[test]
    fn score_penalizes_load() {
        let d = deploy(1, 256, &[]);
        let idle = host("a", 8, 8192, 0, 0, &["docker"]);
        let loaded = host("b", 8, 8192, 4, 0, &["docker"]);
        assert!(score_host(&idle, &d) > score_host(&loaded, &d));
    }
}
