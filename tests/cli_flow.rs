//! End-to-end integration tests for the orchestrator's library surface, driving the same
//! apply -> scale -> rollout -> undo -> delete lifecycle the CLI does, against the deterministic
//! `FakeDriver`. These exercise the wiring (Store + Controller + daemon reconcile-pass + history)
//! that the per-module unit tests do not cover together.

use ce_gke::controller::Controller;
use ce_gke::daemon::reconcile_pass;
use ce_gke::driver::{FakeDriver, MeshDriver};
use ce_gke::reconcile::Phase;
use ce_gke::spec::{Deployment, Resources, Strategy};
use ce_gke::state::Store;
use ce_rs::{Amount, AtlasEntry};

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn host(id: &str) -> AtlasEntry {
    AtlasEntry {
        node_id: id.into(),
        cpu_cores: 32,
        mem_mb: 32768,
        running_jobs: 0,
        last_seen_secs: now(),
        tags: vec!["docker".into()],
    }
}

fn deploy(name: &str, ns: &str, image: &str, replicas: u32) -> Deployment {
    Deployment {
        name: name.into(),
        namespace: ns.into(),
        image: image.into(),
        replicas,
        resources: Resources { cpu_cores: 1, mem_mb: 64 },
        bid: Amount::from_credits(1),
        duration_secs: 60,
        strategy: Strategy::default(),
        ..Default::default()
    }
}

/// Drive a single managed deployment to convergence the way the CLI does (fresh controller per
/// store snapshot, write replicas back), advancing readiness between ticks.
async fn converge_key(fake: &FakeDriver, store: &mut Store, key: &str, ticks: u32) {
    let managed = store.get(key).unwrap().clone();
    let mut ctrl = Controller::new(managed.grant.clone());
    ctrl.max_stale_secs = 0;
    ctrl.replicas = managed.replicas.clone();
    let spec = managed.spec.clone();
    for _ in 0..ticks {
        let r = ctrl.tick(fake, &spec).await.unwrap();
        if let Some(m) = store.get_mut(key) {
            m.replicas = ctrl.replicas.clone();
        }
        if r.done {
            break;
        }
        fake.mark_all_ready();
    }
    fake.mark_all_ready();
}

#[tokio::test]
async fn apply_scale_delete_lifecycle() {
    let fake = FakeDriver::new(vec![host("a"), host("b"), host("c")]);
    let mut store = Store::default();

    // apply web @ 3
    store.upsert(deploy("web", "default", "nginx:1.25", 3), Some("grant-tok".into()));
    converge_key(&fake, &mut store, "default/web", 10).await;
    assert_eq!(store.get("default/web").unwrap().replicas.len(), 3);

    // scale to 5
    store.get_mut("default/web").unwrap().spec.replicas = 5;
    converge_key(&fake, &mut store, "default/web", 10).await;
    assert_eq!(store.get("default/web").unwrap().replicas.len(), 5);

    // scale down to 2
    store.get_mut("default/web").unwrap().spec.replicas = 2;
    converge_key(&fake, &mut store, "default/web", 10).await;
    assert_eq!(store.get("default/web").unwrap().replicas.len(), 2);

    // delete: kill all and forget
    let managed = store.remove("default/web").unwrap();
    for r in &managed.replicas {
        fake.kill(&r.node_id, &r.job_id, managed.grant.as_deref()).await.unwrap();
    }
    assert!(store.get("default/web").is_none());
    assert_eq!(fake.count_phase(Phase::Running), 0);
}

#[tokio::test]
async fn rolling_update_then_undo() {
    let fake = FakeDriver::new(vec![host("a"), host("b"), host("c")]);
    let mut store = Store::default();

    store.upsert(deploy("web", "default", "nginx:1.25", 3), None);
    converge_key(&fake, &mut store, "default/web", 10).await;
    let v1_rev = store.get("default/web").unwrap().spec.revision();

    // apply a new image → rolling update, prior revision recorded in history.
    store.upsert(deploy("web", "default", "nginx:1.26", 3), None);
    assert_eq!(store.get("default/web").unwrap().history.len(), 1);
    converge_key(&fake, &mut store, "default/web", 30).await;
    let m = store.get("default/web").unwrap();
    assert!(m.replicas.iter().all(|r| r.revision == m.spec.revision()));
    assert_ne!(m.spec.revision(), v1_rev);

    // rollout undo → roll back to v1.
    let prior = store.get_mut("default/web").unwrap().history.pop().unwrap();
    let mut rolled = prior;
    rolled.replicas = 3;
    rolled.namespace = "default".into();
    rolled.name = "web".into();
    store.get_mut("default/web").unwrap().spec = rolled;
    converge_key(&fake, &mut store, "default/web", 30).await;
    assert_eq!(store.get("default/web").unwrap().spec.revision(), v1_rev);
}

#[tokio::test]
async fn daemon_pass_heals_across_namespaces() {
    let fake = FakeDriver::new(vec![host("a"), host("b")]);
    let mut store = Store::default();
    store.upsert(deploy("web", "team-a", "nginx", 2), None);
    store.upsert(deploy("api", "team-b", "redis", 1), None);

    // Bring everything up via repeated daemon passes.
    for _ in 0..8 {
        reconcile_pass(&fake, &mut store, None, None).await;
        fake.mark_all_ready();
    }
    assert_eq!(store.get("team-a/web").unwrap().replicas.len(), 2);
    assert_eq!(store.get("team-b/api").unwrap().replicas.len(), 1);

    // Kill a replica's health; a daemon pass must heal it.
    let victim = store.get("team-a/web").unwrap().replicas[0].job_id.clone();
    fake.set_phase(&victim, Phase::Failed);
    for _ in 0..4 {
        reconcile_pass(&fake, &mut store, None, None).await;
        fake.mark_all_ready();
    }
    let reps = &store.get("team-a/web").unwrap().replicas;
    assert_eq!(reps.len(), 2);
    assert!(!reps.iter().any(|r| r.job_id == victim));
}

#[tokio::test]
async fn state_persists_across_simulated_invocations() {
    // Each "invocation" loads from disk, mutates, saves — proving statefulness without a server.
    let dir = std::env::temp_dir().join(format!("ce-gke-cliflow-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.json");
    let fake = FakeDriver::new(vec![host("a"), host("b")]);

    // invocation 1: apply
    {
        let mut store = Store::load(&path).unwrap();
        store.upsert(deploy("web", "default", "nginx", 2), None);
        converge_key(&fake, &mut store, "default/web", 10).await;
        store.save(&path).unwrap();
    }
    // invocation 2: load, observe handles survived, scale
    {
        let mut store = Store::load(&path).unwrap();
        assert_eq!(store.get("default/web").unwrap().replicas.len(), 2, "handles persisted");
        store.get_mut("default/web").unwrap().spec.replicas = 3;
        converge_key(&fake, &mut store, "default/web", 10).await;
        store.save(&path).unwrap();
    }
    // invocation 3: load, verify
    {
        let store = Store::load(&path).unwrap();
        assert_eq!(store.get("default/web").unwrap().replicas.len(), 3);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
