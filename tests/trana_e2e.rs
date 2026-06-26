//! ce-gke <-> trana orchestration e2e (deterministic, in-process).
//!
//! Verifies that ce-gke can declaratively deploy the **trana** distributed social/content backend as
//! a Deployment, keep the desired number of replicas running, advertise it for mesh discovery, and —
//! the crux — **self-heal when nodes/replicas fail at random**. It drives the real Controller +
//! daemon `reconcile_pass` + Store wiring against the deterministic `FakeDriver`, injecting the same
//! failures a chaotic fleet produces (replica crashes, whole-node loss, hosts that reject deploys)
//! and asserting convergence back to the desired state every time.
//!
//! This is the runnable-anywhere half of the trana e2e story (no live mesh, no containers, fully
//! reproducible). The shell harness in `~/ce-net/e2e/` exercises the same flow against real `ce`
//! nodes / VMs; this test pins the orchestration contract that harness depends on.

use ce_gke::daemon::reconcile_pass;
use ce_gke::driver::{FakeDriver, MeshDriver};
use ce_gke::reconcile::Phase;
use ce_gke::spec::{Deployment, Resources, Strategy};
use ce_gke::state::Store;
use ce_rs::{Amount, AtlasEntry};

const NS: &str = "social";
const KEY: &str = "social/trana-node";
const SERVICE: &str = "ce-gke/social/trana-api";

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A docker-capable host in the atlas (the only tag trana requires).
fn host(id: &str) -> AtlasEntry {
    AtlasEntry {
        node_id: id.into(),
        cpu_cores: 16,
        mem_mb: 16384,
        running_jobs: 0,
        last_seen_secs: now(),
        tags: vec!["docker".into()],
    }
}

/// The trana Deployment ce-gke manages — mirrors `trana/deploy/trana.gke.yaml`: a mesh service with
/// a stable replica count and a discoverable service name.
fn trana(replicas: u32) -> Deployment {
    Deployment {
        name: "trana-node".into(),
        namespace: NS.into(),
        image: "ghcr.io/ce-net/trana:0.1.0".into(),
        command: vec!["trana-node".into()],
        replicas,
        resources: Resources { cpu_cores: 2, mem_mb: 512 },
        bid: Amount::from_credits(10),
        duration_secs: 7200,
        select: vec!["docker".into()],
        service: "trana-api".into(),
        strategy: Strategy::default(),
        ..Default::default()
    }
}

/// Run daemon reconcile passes (the same loop `ce-gke run` uses) until the deployment reports the
/// desired replica count Running, or `max_passes` is exhausted. Marks replicas ready between passes
/// the way a healthy fleet's hosts would.
async fn settle(fake: &FakeDriver, store: &mut Store, want: usize, max_passes: u32) -> bool {
    for _ in 0..max_passes {
        reconcile_pass(fake, store, None, None, None).await;
        fake.mark_all_ready();
        if running(store) == want {
            return true;
        }
    }
    running(store) == want
}

/// Count Running replicas of the trana deployment in the store.
fn running(store: &Store) -> usize {
    store
        .get(KEY)
        .map(|m| m.replicas.iter().filter(|r| r.phase == Phase::Running).count())
        .unwrap_or(0)
}

/// The node ids currently hosting trana replicas.
fn replica_nodes(store: &Store) -> Vec<String> {
    store.get(KEY).map(|m| m.replicas.iter().map(|r| r.node_id.clone()).collect()).unwrap_or_default()
}

/// A tiny deterministic PRNG (LCG) so "random" failure patterns are reproducible in CI — no `rand`
/// dependency, no wall-clock seeding.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 { 0 } else { (self.next() as usize) % n }
    }
}

#[tokio::test]
async fn trana_deploys_and_is_discoverable() {
    let fake = FakeDriver::new(vec![host("a"), host("b"), host("c"), host("d"), host("e")]);
    let mut store = Store::default();
    store.upsert(trana(4), Some("grant".into()));

    assert!(settle(&fake, &mut store, 4, 12).await, "trana converged to 4 replicas");
    assert_eq!(running(&store), 4);

    // Replicas are spread across distinct hosts (distributed, not stacked on one node).
    let nodes = replica_nodes(&store);
    let distinct: std::collections::HashSet<_> = nodes.iter().collect();
    assert_eq!(distinct.len(), 4, "4 replicas land on 4 distinct hosts: {nodes:?}");

    // The healthy replica set is advertised for mesh discovery (ce_rs::locate finds it).
    assert!(
        fake.advertised_services().contains(&SERVICE.to_string()),
        "trana advertised as {SERVICE}; got {:?}",
        fake.advertised_services()
    );
}

#[tokio::test]
async fn trana_survives_random_replica_failures() {
    // 5 hosts, 4 desired replicas: headroom for one host to be gone at any time.
    let fake = FakeDriver::new(vec![host("a"), host("b"), host("c"), host("d"), host("e")]);
    let mut store = Store::default();
    store.upsert(trana(4), Some("grant".into()));
    assert!(settle(&fake, &mut store, 4, 12).await, "baseline 4 replicas up");

    // Chaos: many rounds of killing a random 1-2 replicas, asserting ce-gke heals back to 4 each
    // time and never leaves a dead replica tracked.
    let mut rng = Lcg(0x7242_1a4a_b00b_5eed);
    for round in 0..10 {
        let victims: Vec<String> = {
            let reps = &store.get(KEY).unwrap().replicas;
            let kills = 1 + rng.below(2); // 1 or 2
            let mut chosen = Vec::new();
            for _ in 0..kills {
                if reps.is_empty() {
                    break;
                }
                let v = reps[rng.below(reps.len())].job_id.clone();
                if !chosen.contains(&v) {
                    chosen.push(v);
                }
            }
            chosen
        };
        for v in &victims {
            fake.set_phase(v, Phase::Failed);
        }

        assert!(
            settle(&fake, &mut store, 4, 8).await,
            "round {round}: healed back to 4 after killing {victims:?}"
        );
        let reps = &store.get(KEY).unwrap().replicas;
        for v in &victims {
            assert!(!reps.iter().any(|r| &r.job_id == v), "round {round}: dead replica {v} reaped");
        }
    }
    assert_eq!(running(&store), 4, "stable at desired count after sustained chaos");
}

#[tokio::test]
async fn trana_survives_whole_node_loss() {
    let fake = FakeDriver::new(vec![host("a"), host("b"), host("c"), host("d"), host("e")]);
    let mut store = Store::default();
    store.upsert(trana(4), Some("grant".into()));
    assert!(settle(&fake, &mut store, 4, 12).await, "baseline up");

    // Pick a node that is actually hosting a replica and take the whole machine down: every replica
    // on it fails AND the host stops accepting new deploys (it's gone, not just flaky).
    let dead_node = replica_nodes(&store).into_iter().next().expect("a hosting node");
    fake.fail_deploy_on(&dead_node);
    let on_dead: Vec<String> = store
        .get(KEY)
        .unwrap()
        .replicas
        .iter()
        .filter(|r| r.node_id == dead_node)
        .map(|r| r.job_id.clone())
        .collect();
    for j in &on_dead {
        fake.set_phase(j, Phase::Failed);
    }

    assert!(settle(&fake, &mut store, 4, 12).await, "ce-gke re-placed replicas off the dead node");
    // No replica is on the dead node anymore, and we're back to full strength on the survivors.
    let nodes = replica_nodes(&store);
    assert!(!nodes.contains(&dead_node), "no replica left on dead node {dead_node}: {nodes:?}");
    assert_eq!(running(&store), 4);
    assert!(fake.advertised_services().contains(&SERVICE.to_string()), "still discoverable after node loss");
}

#[tokio::test]
async fn trana_scale_and_rolling_update_under_load() {
    let fake = FakeDriver::new(vec![host("a"), host("b"), host("c"), host("d"), host("e"), host("f")]);
    let mut store = Store::default();
    store.upsert(trana(3), Some("grant".into()));
    assert!(settle(&fake, &mut store, 3, 12).await, "start at 3");

    // Scale up to 6 (demand spike).
    store.get_mut(KEY).unwrap().spec.replicas = 6;
    assert!(settle(&fake, &mut store, 6, 16).await, "scaled up to 6");

    // Rolling image update while a replica simultaneously dies — ce-gke must converge all replicas
    // onto the new revision without dropping below the surge/unavailable budget.
    let v1 = store.get(KEY).unwrap().spec.revision();
    store.upsert(
        Deployment { image: "ghcr.io/ce-net/trana:0.2.0".into(), ..trana(6) },
        Some("grant".into()),
    );
    // Kill one mid-rollout.
    let victim = store.get(KEY).unwrap().replicas[0].job_id.clone();
    fake.set_phase(&victim, Phase::Failed);

    assert!(settle(&fake, &mut store, 6, 40).await, "rolling update + heal converged");
    let m = store.get(KEY).unwrap();
    assert_ne!(m.spec.revision(), v1, "advanced to a new revision");
    assert!(
        m.replicas.iter().all(|r| r.revision == m.spec.revision()),
        "every replica is on the new revision"
    );

    // Scale back down to 2 (demand drops); excess replicas are reaped cleanly.
    store.get_mut(KEY).unwrap().spec.replicas = 2;
    assert!(settle(&fake, &mut store, 2, 16).await, "scaled down to 2");
    assert_eq!(store.get(KEY).unwrap().replicas.len(), 2);
}
