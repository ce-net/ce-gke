//! ce-gke — the orchestrator CLI.
//!
//! `kubectl`-shaped commands over a CE node: `apply` a Deployment manifest, `get` status, `scale`
//! the replica count, `rollout` (reconcile to convergence, e.g. after a new image), and `delete`
//! (kill all replicas). State (specs + replica handles) persists to `<data_dir>/ce/gke-state.json`
//! so the orchestrator is stateful across invocations with no server to run.
//!
//! Each mutating command runs reconcile ticks against the live node until the deployment converges
//! (or a tick budget is hit), then saves state. This is the swarm scatter pattern hardened into a
//! self-healing control loop.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use ce_gke::controller::Controller;
use ce_gke::driver::{CeDriver, MeshDriver};
use ce_gke::reconcile::{tally_phases, Phase};
use ce_gke::spec::Deployment;
use ce_gke::state::Store;

#[derive(Parser)]
#[command(name = "ce-gke", about = "Container orchestrator on CE — declarative Deployments over the mesh", version)]
struct Cli {
    /// CE node HTTP API URL.
    #[arg(long, default_value = "http://127.0.0.1:8844", global = true)]
    node: String,
    /// Capability grant token authorizing deploys/kills on the target hosts (hex chain from
    /// `ce-iam`/`ce grant`). For a fleet, one token rooted at a key all hosts honor covers them all.
    #[arg(long, global = true)]
    grant: Option<String>,
    /// Override the state file path (default: `<data_dir>/ce/gke-state.json`).
    #[arg(long, global = true)]
    state: Option<PathBuf>,
    /// Max reconcile ticks per command before giving up convergence (still saves progress).
    #[arg(long, default_value = "60", global = true)]
    max_ticks: u32,
    /// Seconds to wait between reconcile ticks (lets placed replicas come up).
    #[arg(long, default_value = "2", global = true)]
    interval: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Apply a Deployment manifest (YAML or JSON) and reconcile to it. Creates or updates.
    Apply {
        /// Path to the manifest file (`-` reads stdin).
        #[arg(short = 'f', long = "file")]
        file: String,
    },
    /// Show managed deployments and their replica status. With a name, show just that one.
    Get {
        /// Deployment name (omit to list all).
        name: Option<String>,
    },
    /// Scale a deployment to N replicas and reconcile.
    Scale {
        /// Deployment name.
        name: String,
        /// Desired replica count.
        replicas: u32,
    },
    /// Reconcile a deployment to convergence now (drive the rolling update / heal failures).
    Rollout {
        /// Deployment name.
        name: String,
    },
    /// Delete a deployment: kill all its replicas and forget it.
    Delete {
        /// Deployment name.
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let state_path = match &cli.state {
        Some(p) => p.clone(),
        None => Store::default_path()?,
    };
    let mut store = Store::load(&state_path)?;
    let driver = CeDriver::new(cli.node.clone());

    // Borrow the subcommand so the handlers can still borrow `&cli` for its global flags.
    match &cli.cmd {
        Cmd::Apply { file } => apply(&cli, &driver, &mut store, file).await?,
        Cmd::Get { name } => get(&driver, &store, name.as_deref()).await?,
        Cmd::Scale { name, replicas } => scale(&cli, &driver, &mut store, name, *replicas).await?,
        Cmd::Rollout { name } => rollout(&cli, &driver, &mut store, name).await?,
        Cmd::Delete { name } => delete(&cli, &driver, &mut store, name).await?,
    }

    store.save(&state_path)?;
    Ok(())
}

/// Read a manifest from a file path or stdin (`-`).
fn read_manifest(file: &str) -> Result<String> {
    if file == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf).context("reading manifest from stdin")?;
        Ok(buf)
    } else {
        std::fs::read_to_string(file).with_context(|| format!("reading manifest {file}"))
    }
}

async fn apply<D: MeshDriver>(cli: &Cli, driver: &D, store: &mut Store, file: &str) -> Result<()> {
    let manifest = read_manifest(file)?;
    let spec = Deployment::from_manifest(&manifest)?;
    let name = spec.name.clone();
    println!("Applying deployment '{name}' (image {}, {} replica(s))", spec.image, spec.replicas);
    store.upsert(spec, cli.grant.clone());
    reconcile_to_convergence(cli, driver, store, &name).await
}

async fn scale<D: MeshDriver>(
    cli: &Cli,
    driver: &D,
    store: &mut Store,
    name: &str,
    replicas: u32,
) -> Result<()> {
    let Some(managed) = store.get_mut(name) else {
        bail!("no deployment named '{name}' (apply one first)");
    };
    let old = managed.spec.replicas;
    managed.spec.replicas = replicas;
    println!("Scaling '{name}': {old} -> {replicas} replica(s)");
    reconcile_to_convergence(cli, driver, store, name).await
}

async fn rollout<D: MeshDriver>(cli: &Cli, driver: &D, store: &mut Store, name: &str) -> Result<()> {
    if store.get(name).is_none() {
        bail!("no deployment named '{name}'");
    }
    println!("Reconciling '{name}'...");
    reconcile_to_convergence(cli, driver, store, name).await
}

async fn delete<D: MeshDriver>(cli: &Cli, driver: &D, store: &mut Store, name: &str) -> Result<()> {
    let Some(managed) = store.remove(name) else {
        bail!("no deployment named '{name}'");
    };
    println!("Deleting '{name}': killing {} replica(s)", managed.replicas.len());
    let grant = managed.grant.or_else(|| cli.grant.clone());
    let mut failures = 0u32;
    for r in &managed.replicas {
        match driver.kill(&r.node_id, &r.job_id, grant.as_deref()).await {
            Ok(()) => println!("  killed {} on {}", short(&r.job_id), short(&r.node_id)),
            Err(e) => {
                failures += 1;
                eprintln!("  warning: kill of {} failed: {e}", short(&r.job_id));
            }
        }
    }
    if failures > 0 {
        eprintln!("{failures} replica(s) could not be killed (host may already have reaped them).");
    }
    println!("Deleted '{name}'.");
    Ok(())
}

/// Run reconcile ticks for `name` until it converges or the tick budget is exhausted, sleeping
/// `interval` between ticks so placed replicas have time to come up. Persists handles back into the
/// store as we go.
async fn reconcile_to_convergence<D: MeshDriver>(
    cli: &Cli,
    driver: &D,
    store: &mut Store,
    name: &str,
) -> Result<()> {
    // Pull the managed deployment out to drive its controller, then write back.
    let managed = store.get(name).context("deployment vanished")?.clone();
    let mut ctrl = Controller::new(managed.grant.clone().or_else(|| cli.grant.clone()));
    ctrl.replicas = managed.replicas.clone();
    let spec = managed.spec.clone();

    let mut converged = false;
    for tick in 0..cli.max_ticks {
        let report = ctrl.tick(driver, &spec).await?;
        // Persist handles every tick so a crash mid-rollout is recoverable.
        if let Some(m) = store.get_mut(name) {
            m.replicas = ctrl.replicas.clone();
        }
        if !report.placed.is_empty() || !report.killed.is_empty() || report.place_failures > 0
            || report.kill_failures > 0
        {
            println!(
                "  tick {tick}: +{} placed, -{} killed{}{}",
                report.placed.len(),
                report.killed.len(),
                fail_note("place", report.place_failures),
                fail_note("kill", report.kill_failures),
            );
        }
        if report.done {
            converged = true;
            break;
        }
        // Wait for replicas to become ready before the next tick.
        tokio::time::sleep(Duration::from_secs(cli.interval.max(1))).await;
    }

    // Final status line.
    let (pending, running, terminal) = tally_phases(&ctrl.replicas);
    if converged {
        println!("'{name}' converged: {running} running.");
    } else {
        println!(
            "'{name}' not fully converged within {} ticks: {running} running, {pending} pending, {terminal} terminal.",
            cli.max_ticks
        );
        println!("Re-run `ce-gke rollout {name}` to continue.");
    }
    Ok(())
}

fn fail_note(kind: &str, n: u32) -> String {
    if n == 0 {
        String::new()
    } else {
        format!(", {n} {kind} failure(s)")
    }
}

async fn get<D: MeshDriver>(driver: &D, store: &Store, name: Option<&str>) -> Result<()> {
    let names: Vec<String> = match name {
        Some(n) => {
            if store.get(n).is_none() {
                bail!("no deployment named '{n}'");
            }
            vec![n.to_string()]
        }
        None => store.names(),
    };
    if names.is_empty() {
        println!("No deployments. Apply one with `ce-gke apply -f deploy.yaml`.");
        return Ok(());
    }

    println!(
        "{:<20}  {:<24}  {:>8}  {:>9}  {:<16}",
        "NAME", "IMAGE", "DESIRED", "READY", "REVISION"
    );
    for n in &names {
        let m = store.get(n).expect("present");
        // Refresh live phases best-effort (a node that is down just shows the cached counts).
        let mut running = 0u32;
        for r in &m.replicas {
            let phase = driver.phase(&r.node_id, &r.job_id).await.unwrap_or(r.phase);
            if phase == Phase::Running {
                running += 1;
            }
        }
        println!(
            "{:<20}  {:<24}  {:>8}  {:>9}  {:<16}",
            truncate(n, 20),
            truncate(&m.spec.image, 24),
            m.spec.replicas,
            format!("{running}/{}", m.spec.replicas),
            m.spec.revision(),
        );
    }
    Ok(())
}

fn short(s: &str) -> String {
    s.chars().take(12).collect()
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}...", &s[..n.saturating_sub(3)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_truncates() {
        assert_eq!(short("0123456789abcdef"), "0123456789ab");
        assert_eq!(short("abc"), "abc");
    }

    #[test]
    fn truncate_adds_ellipsis() {
        assert_eq!(truncate("short", 20), "short");
        assert_eq!(truncate("a-very-long-image-name:latest", 10), "a-very-...");
        assert_eq!(truncate("exactly-ten", 11), "exactly-ten");
    }

    #[test]
    fn fail_note_formats() {
        assert_eq!(fail_note("place", 0), "");
        assert_eq!(fail_note("place", 2), ", 2 place failure(s)");
    }

    #[test]
    fn read_manifest_from_file() {
        let dir = std::env::temp_dir().join(format!("ce-gke-main-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("d.yaml");
        std::fs::write(&path, "name: web\nimage: nginx\nreplicas: 1\n").unwrap();
        let got = read_manifest(path.to_str().unwrap()).unwrap();
        assert!(got.contains("nginx"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_manifest_missing_file_errors() {
        assert!(read_manifest("/nonexistent/path/deploy.yaml").is_err());
    }
}
