//! The long-running controller daemon — what makes "self-healing" real.
//!
//! GKE's controllers reconcile forever. Without a daemon, ce-gke only reconciles inside a CLI
//! command's bounded tick budget: the moment `ce-gke apply` returns, nothing watches the fleet, so a
//! replica that fails afterward is never replaced until an operator re-runs a command. [`run_daemon`]
//! closes that gap: it loops over every managed deployment on a fixed interval, runs one reconcile
//! tick each, persists handles, and re-advertises services — indefinitely, until a graceful
//! shutdown signal (SIGINT/SIGTERM) is received.
//!
//! It is still a pure client over CE primitives (no server, no node changes). State is the same
//! `gke-state.json`; the daemon holds the [`crate::state::StateLock`] only for the brief
//! read-modify-write around each pass, so CLI commands can still run between passes.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;

use crate::controller::Controller;
use crate::driver::MeshDriver;
use crate::reconcile::tally_phases;
use crate::state::{StateLock, Store};

/// Daemon configuration.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Path to the state file.
    pub state_path: PathBuf,
    /// Seconds between reconcile passes.
    pub interval_secs: u64,
    /// Optional namespace filter: only reconcile deployments in this namespace. `None` = all.
    pub namespace: Option<String>,
    /// Fallback grant for deployments that carry none.
    pub grant: Option<String>,
}

/// Run one reconcile pass over every managed deployment (optionally filtered by namespace). Returns
/// the number of deployments that are not yet converged. Pure with respect to the driver/store
/// seams, so it is unit-testable against [`crate::driver::FakeDriver`] and an in-memory store path.
pub async fn reconcile_pass<D: MeshDriver>(
    driver: &D,
    store: &mut Store,
    namespace: Option<&str>,
    fallback_grant: Option<&str>,
) -> usize {
    let keys: Vec<String> = match namespace {
        Some(ns) => store.names_in(ns),
        None => store.names(),
    };
    let mut unconverged = 0usize;
    for key in keys {
        let Some(managed) = store.get(&key) else { continue };
        if managed.paused {
            // A paused rollout still reconciles count/health but the spec is held; we simply run a
            // normal tick (scale/heal), which never advances a revision the operator did not change.
        }
        let spec = managed.spec.clone();
        let grant = managed.grant.clone().or_else(|| fallback_grant.map(str::to_string));
        let mut ctrl = Controller::new(grant);
        ctrl.replicas = managed.replicas.clone();
        match ctrl.tick(driver, &spec).await {
            Ok(report) => {
                if let Some(m) = store.get_mut(&key) {
                    m.replicas = ctrl.replicas.clone();
                }
                if !report.done {
                    unconverged += 1;
                }
                let (pending, running, terminal) = tally_phases(&ctrl.replicas);
                if report.placed.len() + report.killed.len() > 0
                    || report.place_failures + report.kill_failures > 0
                {
                    tracing::info!(
                        deployment = %key,
                        placed = report.placed.len(),
                        killed = report.killed.len(),
                        place_failures = report.place_failures,
                        kill_failures = report.kill_failures,
                        running,
                        pending,
                        terminal,
                        "reconcile"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(deployment = %key, error = %e, "reconcile tick failed");
                unconverged += 1;
            }
        }
    }
    unconverged
}

/// Loop [`reconcile_pass`] forever (until `shutdown` fires), persisting state each pass under the
/// state lock. The lock is acquired and released per pass so interactive CLI commands are not
/// starved. `shutdown` lets the caller wire SIGINT/SIGTERM (or a test) to a clean stop.
pub async fn run_daemon<D: MeshDriver>(
    driver: &D,
    cfg: &DaemonConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!(
        interval = cfg.interval_secs,
        namespace = cfg.namespace.as_deref().unwrap_or("<all>"),
        "ce-gke daemon started"
    );
    let interval = Duration::from_secs(cfg.interval_secs.max(1));
    loop {
        if *shutdown.borrow() {
            break;
        }
        // One pass under the lock (brief), then release so CLI commands can interleave.
        {
            let _lock = StateLock::acquire(&cfg.state_path)
                .context("daemon could not acquire the state lock")?;
            let mut store = Store::load(&cfg.state_path)?;
            let unconverged =
                reconcile_pass(driver, &mut store, cfg.namespace.as_deref(), cfg.grant.as_deref())
                    .await;
            store.save(&cfg.state_path)?;
            if unconverged > 0 {
                tracing::debug!(unconverged, "deployments not yet converged");
            }
        }
        // Sleep the interval, but wake immediately on shutdown.
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
        }
    }
    tracing::info!("ce-gke daemon stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::FakeDriver;
    use crate::spec::Strategy;
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

    fn deploy(name: &str, ns: &str, replicas: u32) -> crate::spec::Deployment {
        crate::spec::Deployment {
            name: name.into(),
            namespace: ns.into(),
            image: "nginx".into(),
            replicas,
            resources: crate::spec::Resources { cpu_cores: 1, mem_mb: 64 },
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Strategy::default(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn pass_reconciles_all_namespaces() {
        let fake = FakeDriver::new(vec![host("a"), host("b")]);
        let mut store = Store::default();
        store.upsert(deploy("web", "team-a", 2), None);
        store.upsert(deploy("api", "team-b", 1), None);
        // First pass places replicas (Pending), not yet converged.
        let unconverged = reconcile_pass(&fake, &mut store, None, None).await;
        assert!(unconverged >= 1);
        fake.mark_all_ready();
        // Subsequent passes drive to convergence.
        for _ in 0..5 {
            reconcile_pass(&fake, &mut store, None, None).await;
            fake.mark_all_ready();
        }
        assert_eq!(store.get("team-a/web").unwrap().replicas.len(), 2);
        assert_eq!(store.get("team-b/api").unwrap().replicas.len(), 1);
    }

    #[tokio::test]
    async fn pass_respects_namespace_filter() {
        let fake = FakeDriver::new(vec![host("a"), host("b")]);
        let mut store = Store::default();
        store.upsert(deploy("web", "team-a", 2), None);
        store.upsert(deploy("api", "team-b", 2), None);
        // Only reconcile team-a.
        for _ in 0..5 {
            reconcile_pass(&fake, &mut store, Some("team-a"), None).await;
            fake.mark_all_ready();
        }
        assert_eq!(store.get("team-a/web").unwrap().replicas.len(), 2);
        assert_eq!(store.get("team-b/api").unwrap().replicas.len(), 0, "other ns untouched");
    }

    #[tokio::test]
    async fn pass_heals_a_failed_replica() {
        let fake = FakeDriver::new(vec![host("a"), host("b")]);
        let mut store = Store::default();
        store.upsert(deploy("web", "default", 2), None);
        for _ in 0..5 {
            reconcile_pass(&fake, &mut store, None, None).await;
            fake.mark_all_ready();
        }
        let victim = store.get("default/web").unwrap().replicas[0].job_id.clone();
        fake.set_phase(&victim, crate::reconcile::Phase::Failed);
        // A pass reaps + replaces it.
        reconcile_pass(&fake, &mut store, None, None).await;
        fake.mark_all_ready();
        reconcile_pass(&fake, &mut store, None, None).await;
        let reps = &store.get("default/web").unwrap().replicas;
        assert_eq!(reps.len(), 2);
        assert!(!reps.iter().any(|r| r.job_id == victim), "victim replaced");
    }

    #[tokio::test]
    async fn daemon_stops_on_shutdown_signal() {
        let dir = std::env::temp_dir().join(format!("ce-gke-daemon-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");
        let mut store = Store::default();
        store.upsert(deploy("web", "default", 1), None);
        store.save(&path).unwrap();

        let fake = FakeDriver::new(vec![host("a")]);
        let (tx, rx) = watch::channel(false);
        let cfg = DaemonConfig {
            state_path: path.clone(),
            interval_secs: 1,
            namespace: None,
            grant: None,
        };
        // Signal shutdown almost immediately; the daemon must return promptly.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(true);
        });
        let res =
            tokio::time::timeout(Duration::from_secs(5), run_daemon(&fake, &cfg, rx)).await;
        assert!(res.is_ok(), "daemon did not stop on shutdown");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
