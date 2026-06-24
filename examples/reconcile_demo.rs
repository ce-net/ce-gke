//! A runnable, node-free demonstration of the ce-gke reconcile loop.
//!
//!   cargo run --example reconcile_demo
//!
//! It builds a deployment, converges it on a deterministic `FakeDriver` cluster, injects a replica
//! failure, heals it, then rolls out a new image — printing what each tick does. This is exactly the
//! logic the CLI runs against a real node, with the mesh swapped for an in-memory fake.

use ce_gke::controller::Controller;
use ce_gke::driver::FakeDriver;
use ce_gke::reconcile::Phase;
use ce_gke::spec::Deployment;
use ce_rs::AtlasEntry;

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let fake = FakeDriver::new(vec![host("alpha"), host("beta"), host("gamma")]);

    let d = Deployment::from_manifest(
        r#"
name: web
namespace: demo
image: nginx:1.25
replicas: 3
service: web
strategy:
  type: rolling_update
  max_unavailable: 1
  max_surge: 1
"#,
    )?;

    let mut ctrl = Controller::new(None);
    ctrl.max_stale_secs = 0; // synthetic timestamps

    println!("== converge to 3 replicas ==");
    for tick in 0..6 {
        let r = ctrl.tick(&fake, &d).await?;
        println!(
            "tick {tick}: +{} placed, -{} killed, service_advertised={}, done={}",
            r.placed.len(),
            r.killed.len(),
            r.service_advertised,
            r.done
        );
        if r.done {
            break;
        }
        fake.mark_all_ready();
    }
    println!("placed on: {:?}", ctrl.current().iter().map(|r| &r.node_id).collect::<Vec<_>>());

    println!("\n== inject a failure and self-heal ==");
    let victim = ctrl.current()[0].job_id.clone();
    fake.set_phase(&victim, Phase::Failed);
    let r = ctrl.tick(&fake, &d).await?;
    println!("reaped {} failed, placed {} replacement", r.killed.len(), r.placed.len());
    fake.mark_all_ready();
    ctrl.converge(&fake, &d, 5, |f| f.mark_all_ready()).await?;
    println!("healthy replicas: {}", ctrl.current().iter().filter(|r| r.phase == Phase::Running).count());

    println!("\n== rolling update to nginx:1.26 ==");
    let mut v2 = d.clone();
    v2.image = "nginx:1.26".into();
    let report = ctrl.converge(&fake, &v2, 30, |f| f.mark_all_ready()).await?;
    println!(
        "rollout done={}; all on new revision {}: {}",
        report.done,
        v2.revision(),
        ctrl.current().iter().all(|r| r.revision == v2.revision())
    );

    Ok(())
}
