//! ce-gke — the orchestrator CLI.
//!
//! `kubectl`-shaped commands over a CE node: `apply` a Deployment manifest, `get` status, `scale`
//! the replica count, `rollout` (reconcile / undo / pause / resume), and `delete` (kill all
//! replicas). A long-running `run` daemon reconciles the whole fleet forever, and `serve` is the
//! host-side health agent. State (specs + replica handles + revision history) persists to
//! `<data_dir>/ce/gke-state.json` so the orchestrator is stateful across invocations with no server.
//!
//! Every mutating command takes the state lock (so two `ce-gke` processes never lose each other's
//! updates), runs reconcile ticks against the live node until the deployment converges (or a tick
//! budget is hit), then saves state.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use tokio::sync::watch;

use ce_gke::auth::{preflight, ACTION_DEPLOY};
use ce_gke::controller::Controller;
use ce_gke::daemon::{run_daemon, DaemonConfig};
use ce_gke::driver::{CeDriver, MeshDriver};
use ce_gke::reconcile::{tally_phases, Phase};
use ce_gke::secrets::SecretStore;
use ce_gke::spec::Deployment;
use ce_gke::state::{StateLock, Store};

use ce_rs::CeClient;

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
    /// Namespace scoping the deployment(s). Defaults to `default`.
    #[arg(long, short = 'n', default_value = "default", global = true)]
    namespace: String,
    /// Override the state file path (default: `<data_dir>/ce/gke-state.json`).
    #[arg(long, global = true)]
    state: Option<PathBuf>,
    /// Override the secret file path (default: `<data_dir>/ce/gke-secrets.json`).
    #[arg(long, global = true)]
    secrets: Option<PathBuf>,
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
        /// Deployment name (omit to list all in the namespace).
        name: Option<String>,
    },
    /// Scale a deployment to N replicas and reconcile.
    Scale {
        /// Deployment name.
        name: String,
        /// Desired replica count.
        replicas: u32,
    },
    /// Rollout operations: reconcile now, or undo/pause/resume.
    Rollout {
        #[command(subcommand)]
        op: RolloutOp,
    },
    /// Delete a deployment: kill all its replicas and forget it.
    Delete {
        /// Deployment name.
        name: String,
        /// Forget the deployment even if some replicas could not be killed (host unreachable).
        #[arg(long)]
        force: bool,
    },
    /// Run the long-running controller daemon: reconcile the fleet forever until interrupted.
    Run {
        /// Reconcile every N seconds.
        #[arg(long, default_value = "10")]
        every: u64,
        /// Only reconcile this namespace (default: all namespaces).
        #[arg(long)]
        all_namespaces: bool,
    },
    /// Run the host-side health agent: answer probe requests for jobs this node runs.
    Serve {
        /// Require a capability grant on every probe (default: open).
        #[arg(long)]
        require_grant: bool,
        /// Poll interval (ms) for incoming probe requests.
        #[arg(long, default_value = "250")]
        poll_ms: u64,
    },
    /// Manage secrets resolved into `value_from` env vars (local store, never in manifests/state).
    Secret {
        #[command(subcommand)]
        op: SecretOp,
    },
}

#[derive(Subcommand)]
enum RolloutOp {
    /// Reconcile a deployment to convergence now (drive the rolling update / heal failures).
    Status {
        /// Deployment name.
        name: String,
    },
    /// Roll back to the previous revision (or `--to-revision <hash>`).
    Undo {
        /// Deployment name.
        name: String,
        /// Roll back to a specific revision hash from history (default: the most recent prior one).
        #[arg(long)]
        to_revision: Option<String>,
    },
    /// Pause a deployment's rollout (count/health reconcile continues; no new revision progresses).
    Pause {
        /// Deployment name.
        name: String,
    },
    /// Resume a paused deployment.
    Resume {
        /// Deployment name.
        name: String,
    },
    /// Show the revision history kept for `rollout undo`.
    History {
        /// Deployment name.
        name: String,
    },
}

#[derive(Subcommand)]
enum SecretOp {
    /// Set (or replace) a secret value.
    Set {
        /// Secret name.
        name: String,
        /// Secret value (`-` reads from stdin).
        value: String,
    },
    /// Remove a secret.
    Rm {
        /// Secret name.
        name: String,
    },
    /// List secret names (values are never printed).
    Ls,
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
    let secret_path = match &cli.secrets {
        Some(p) => p.clone(),
        None => SecretStore::default_path()?,
    };
    let secrets = SecretStore::load(&secret_path)?;
    let driver = CeDriver::new(cli.node.clone()).with_secrets(secrets.clone());

    match &cli.cmd {
        // The daemon and serve agent run their own loops and manage the lock themselves.
        Cmd::Run { every, all_namespaces } => {
            return run_cmd(&cli, &driver, &state_path, *every, *all_namespaces).await;
        }
        Cmd::Serve { require_grant, poll_ms } => {
            return serve_cmd(&cli, *require_grant, *poll_ms).await;
        }
        Cmd::Secret { op } => return secret_cmd(op, &secret_path),
        _ => {}
    }

    // All other commands take the lock for the read-modify-write.
    let _lock = StateLock::acquire(&state_path)?;
    let mut store = Store::load(&state_path)?;

    match &cli.cmd {
        Cmd::Apply { file } => apply(&cli, &driver, &mut store, file).await?,
        Cmd::Get { name } => get(&driver, &store, &cli.namespace, name.as_deref()).await?,
        Cmd::Scale { name, replicas } => scale(&cli, &driver, &mut store, name, *replicas).await?,
        Cmd::Rollout { op } => rollout_cmd(&cli, &driver, &mut store, op).await?,
        Cmd::Delete { name, force } => delete(&cli, &driver, &mut store, name, *force).await?,
        Cmd::Run { .. } | Cmd::Serve { .. } | Cmd::Secret { .. } => unreachable!(),
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

/// Best-effort capability + balance preflight before touching the mesh. A bad/expired grant or
/// insufficient balance fails fast with a clear message instead of N opaque place-failures.
async fn preflight_checks(cli: &Cli, driver: &CeDriver, spec: &Deployment) -> Result<()> {
    // One status fetch covers both checks. If the node is unreachable here, we do not block the
    // command — the deploy itself will surface the real error; preflight is a fail-fast convenience.
    let status = match driver.client().status().await {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };

    // 1. Capability preflight (only if a grant is supplied; an open mesh needs none).
    if let Some(token) = cli.grant.as_deref()
        && let Some(self_id) = parse_node_id(&status.node_id)
    {
        // We do not know the target host's id until placement, so verify the grant's own
        // self-consistency: the requester is us, action is deploy, rooted at the grant's own issuer.
        // This catches malformed/expired/wrong-requester tokens before any deploy.
        if let Ok(roots) = grant_roots(token) {
            let now = now_secs();
            let never = |_: &[u8; 32], _: u64| false;
            let ok = roots.iter().any(|root| {
                preflight(token, &self_id, &[*root], &[], now, &self_id, ACTION_DEPLOY, &never)
                    .is_ok()
                    || preflight(token, root, &[], &[], now, &self_id, ACTION_DEPLOY, &never)
                        .is_ok()
            });
            if !ok {
                bail!(
                    "grant preflight failed: the token does not authorize this node to deploy (check requester/expiry/action)"
                );
            }
        }
    }

    // 2. Balance preflight: replicas * bid must not exceed spendable balance.
    if !spec.bid.is_zero() && spec.replicas > 0 {
        let need = spec.bid.base().saturating_mul(spec.replicas as i128);
        let free = status.free.unwrap_or(status.balance).base().max(0);
        if need > free {
            bail!(
                "insufficient balance: {} replica(s) x {} credits = {} needed, but only {} spendable",
                spec.replicas,
                spec.bid.credits(),
                ce_rs::Amount::from_base(need).credits(),
                ce_rs::Amount::from_base(free).credits()
            );
        }
    }
    Ok(())
}

async fn apply(cli: &Cli, driver: &CeDriver, store: &mut Store, file: &str) -> Result<()> {
    let manifest = read_manifest(file)?;
    // A manifest declaring `apps:` is a multi-deployment stack ("docker-compose for the mesh");
    // anything else is a single Deployment. The stack path topo-sorts by depends_on (rejecting
    // cycles) and applies producers before consumers.
    if is_stack_manifest(&manifest) {
        return apply_stack(cli, driver, store, &manifest).await;
    }
    let mut spec = Deployment::from_manifest(&manifest)?;
    // The CLI --namespace flag overrides only if the manifest left it at the default.
    if spec.namespace == "default" && cli.namespace != "default" {
        spec.namespace = cli.namespace.clone();
    }
    spec.validate()?;
    preflight_checks(cli, driver, &spec).await?;
    let key = spec.key();
    println!(
        "Applying deployment '{}' (image {}, {} replica(s))",
        key, spec.image, spec.replicas
    );
    store.upsert(spec, cli.grant.clone());
    reconcile_to_convergence(cli, driver, store, &key).await
}

/// Does this manifest declare a multi-deployment stack (an `apps:` list) rather than a single
/// Deployment? Parses leniently as a generic YAML map and checks for the key — never fails the apply
/// on a malformed manifest (the real parser surfaces the precise error downstream).
fn is_stack_manifest(manifest: &str) -> bool {
    match serde_yaml::from_str::<serde_yaml::Value>(manifest) {
        Ok(serde_yaml::Value::Mapping(m)) => {
            m.contains_key(serde_yaml::Value::String("apps".to_string()))
        }
        _ => false,
    }
}

/// Apply a multi-deployment stack: validate the graph (rejecting cycles + dangling deps), then apply
/// each app in dependency order. Each app is upserted and reconciled; a consumer whose producer is
/// not yet live is held `waiting` by the per-tick dependency gate and converges on a later pass
/// (or under `ce-gke run`). The static topo-order gives a sensible first-apply sequence; runtime
/// correctness comes from the gate, not the order.
async fn apply_stack(cli: &Cli, driver: &CeDriver, store: &mut Store, manifest: &str) -> Result<()> {
    let mut stack = ce_gke::stack::Stack::from_manifest(manifest)?;
    // CLI --namespace applies to apps still at the default namespace (same rule as single apply).
    if cli.namespace != "default" {
        for app in &mut stack.apps {
            if app.namespace == "default" {
                app.namespace = cli.namespace.clone();
            }
        }
        // Re-validate after the namespace shift (keeps depends_on/service co-namespacing consistent).
        stack.validate()?;
    }
    let order = stack.apply_order()?; // errors here name a dependency cycle
    println!(
        "Applying stack: {} app(s) in dependency order: {}",
        stack.apps.len(),
        order.iter().map(|k| strip_ns(k)).collect::<Vec<_>>().join(" -> ")
    );
    // Index apps by key so we can apply in topo order.
    let mut by_key: std::collections::HashMap<String, Deployment> =
        stack.apps.into_iter().map(|a| (a.key(), a)).collect();
    for key in &order {
        let Some(spec) = by_key.remove(key) else { continue };
        spec.validate()?;
        preflight_checks(cli, driver, &spec).await?;
        println!(
            "Applying '{}' (image {}, {} replica(s){})",
            key,
            spec.image,
            spec.replicas,
            if spec.depends_on.is_empty() {
                String::new()
            } else {
                format!(
                    ", depends_on: {}",
                    spec.depends_on.iter().map(|d| d.service.clone()).collect::<Vec<_>>().join(",")
                )
            }
        );
        store.upsert(spec, cli.grant.clone());
        reconcile_to_convergence(cli, driver, store, key).await?;
    }
    println!(
        "Stack applied. Run `ce-gke run` to keep dependents reconciling as their dependencies come up."
    );
    Ok(())
}

async fn scale<D: MeshDriver>(
    cli: &Cli,
    driver: &D,
    store: &mut Store,
    name: &str,
    replicas: u32,
) -> Result<()> {
    let key = format!("{}/{}", cli.namespace, name);
    let Some(managed) = store.get_mut(&key) else {
        bail!("no deployment '{key}' (apply one first)");
    };
    if replicas > ce_gke::spec::MAX_REPLICAS {
        bail!("replicas {replicas} exceeds the {} ceiling", ce_gke::spec::MAX_REPLICAS);
    }
    let old = managed.spec.replicas;
    managed.spec.replicas = replicas;
    println!("Scaling '{key}': {old} -> {replicas} replica(s)");
    reconcile_to_convergence(cli, driver, store, &key).await
}

async fn rollout_cmd<D: MeshDriver>(
    cli: &Cli,
    driver: &D,
    store: &mut Store,
    op: &RolloutOp,
) -> Result<()> {
    match op {
        RolloutOp::Status { name } => {
            let key = format!("{}/{}", cli.namespace, name);
            if store.get(&key).is_none() {
                bail!("no deployment '{key}'");
            }
            println!("Reconciling '{key}'...");
            reconcile_to_convergence(cli, driver, store, &key).await
        }
        RolloutOp::Undo { name, to_revision } => {
            let key = format!("{}/{}", cli.namespace, name);
            let Some(managed) = store.get_mut(&key) else { bail!("no deployment '{key}'") };
            let prior = match to_revision {
                Some(rev) => {
                    let idx = managed.history.iter().rposition(|h| &h.revision() == rev);
                    match idx {
                        Some(i) => managed.history.remove(i),
                        None => bail!("no revision '{rev}' in history for '{key}'"),
                    }
                }
                None => match managed.history.pop() {
                    Some(p) => p,
                    None => bail!("no prior revision to roll back to for '{key}'"),
                },
            };
            // Preserve the desired replica count; only the pod template rolls back.
            let mut rolled = prior;
            rolled.replicas = managed.spec.replicas;
            rolled.namespace = managed.spec.namespace.clone();
            rolled.name = managed.spec.name.clone();
            println!("Rolling '{key}' back to revision {}", rolled.revision());
            managed.spec = rolled;
            reconcile_to_convergence(cli, driver, store, &key).await
        }
        RolloutOp::Pause { name } => {
            let key = format!("{}/{}", cli.namespace, name);
            let Some(managed) = store.get_mut(&key) else { bail!("no deployment '{key}'") };
            managed.paused = true;
            println!("Paused rollout for '{key}'.");
            Ok(())
        }
        RolloutOp::Resume { name } => {
            let key = format!("{}/{}", cli.namespace, name);
            let Some(managed) = store.get_mut(&key) else { bail!("no deployment '{key}'") };
            managed.paused = false;
            println!("Resumed rollout for '{key}'.");
            reconcile_to_convergence(cli, driver, store, &key).await
        }
        RolloutOp::History { name } => {
            let key = format!("{}/{}", cli.namespace, name);
            let Some(managed) = store.get(&key) else { bail!("no deployment '{key}'") };
            println!("Revision history for '{key}' (newest last):");
            if managed.history.is_empty() {
                println!("  (none)");
            }
            for (i, h) in managed.history.iter().enumerate() {
                println!("  {}. {}  image={}", i + 1, h.revision(), h.image);
            }
            println!("  current: {}  image={}", managed.spec.revision(), managed.spec.image);
            Ok(())
        }
    }
}

async fn delete<D: MeshDriver>(
    cli: &Cli,
    driver: &D,
    store: &mut Store,
    name: &str,
    force: bool,
) -> Result<()> {
    let key = format!("{}/{}", cli.namespace, name);
    let Some(managed) = store.get(&key).cloned() else {
        bail!("no deployment '{key}'");
    };
    println!("Deleting '{key}': killing {} replica(s)", managed.replicas.len());
    let grant = managed.grant.clone().or_else(|| cli.grant.clone());
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
    if failures > 0 && !force {
        eprintln!(
            "{failures} replica(s) could not be killed; the deployment is kept. Re-run with --force to forget it anyway."
        );
        return Ok(());
    }
    store.remove(&key);
    if failures > 0 {
        eprintln!("{failures} replica(s) could not be killed (forgotten anyway via --force).");
    }
    println!("Deleted '{key}'.");
    Ok(())
}

/// Run reconcile ticks for `key` until it converges or the tick budget is exhausted, sleeping
/// `interval` between ticks so placed replicas have time to come up. Persists handles into the store
/// as we go. A paused deployment still reconciles count/health (the controller never advances a
/// revision the operator did not change).
async fn reconcile_to_convergence<D: MeshDriver>(
    cli: &Cli,
    driver: &D,
    store: &mut Store,
    key: &str,
) -> Result<()> {
    let managed = store.get(key).context("deployment vanished")?.clone();
    let mut ctrl = Controller::new(managed.grant.clone().or_else(|| cli.grant.clone()));
    ctrl.replicas = managed.replicas.clone();
    let spec = managed.spec.clone();

    let mut converged = false;
    let mut last_waiting: Vec<String> = Vec::new();
    for tick in 0..cli.max_ticks {
        let report = ctrl.tick(driver, &spec).await?;
        if let Some(m) = store.get_mut(key) {
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
        // Surface a dependency hold only when it changes, so the loop does not spam the same line.
        if report.waiting_on != last_waiting {
            if report.is_waiting() {
                println!(
                    "  tick {tick}: waiting on dependency service(s): {}",
                    report.waiting_on.join(", ")
                );
            }
            last_waiting = report.waiting_on.clone();
        }
        if report.done {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(cli.interval.max(1))).await;
    }

    let (pending, running, terminal) = tally_phases(&ctrl.replicas);
    if converged {
        println!("'{key}' converged: {running} running.");
    } else {
        println!(
            "'{key}' not fully converged within {} ticks: {running} running, {pending} pending, {terminal} terminal.",
            cli.max_ticks
        );
        println!("Re-run `ce-gke rollout status {}` to continue.", strip_ns(key));
    }
    Ok(())
}

/// The daemon command: loop reconcile over the fleet forever, with SIGINT/SIGTERM graceful stop.
async fn run_cmd(
    cli: &Cli,
    driver: &CeDriver,
    state_path: &std::path::Path,
    every: u64,
    all_namespaces: bool,
) -> Result<()> {
    let cfg = DaemonConfig {
        state_path: state_path.to_path_buf(),
        interval_secs: every,
        namespace: if all_namespaces { None } else { Some(cli.namespace.clone()) },
        grant: cli.grant.clone(),
    };
    let (tx, rx) = watch::channel(false);
    // Wire OS signals to the shutdown channel.
    let tx_signal = tx.clone();
    tokio::spawn(async move {
        wait_for_shutdown().await;
        let _ = tx_signal.send(true);
    });
    run_daemon(driver, &cfg, rx).await
}

/// Await SIGINT (and SIGTERM on Unix).
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// The host-side health agent loop: answer probe requests on the probe topic.
async fn serve_cmd(cli: &Cli, require_grant: bool, poll_ms: u64) -> Result<()> {
    use ce_gke::serve::{handle_probe_bytes, JobSource, ProbeAuthority};

    let ce = CeClient::new(cli.node.clone());
    let status = ce.status().await.context("querying node status")?;
    let self_id = parse_node_id(&status.node_id)
        .context("node returned an invalid node id")?;
    // Snapshot the revocation set once; a long-running agent could refresh periodically (documented).
    let revoked_set = ce.revoked().await.unwrap_or_default();
    let is_revoked = move |issuer: &[u8; 32], nonce: u64| {
        revoked_set.iter().any(|(iss, n)| iss == &hex::encode(issuer) && *n == nonce)
    };

    ce.subscribe(ce_gke::protocol::PROBE_TOPIC).await.ok();
    println!(
        "ce-gke serve: answering probes on '{}' as {} (require_grant={require_grant})",
        ce_gke::protocol::PROBE_TOPIC,
        short(&status.node_id)
    );

    /// Job lookup backed by the local node's job table.
    struct NodeJobs {
        jobs: std::collections::HashMap<String, Phase>,
    }
    impl JobSource for NodeJobs {
        fn job_phase(&self, job_id: &str) -> Option<Phase> {
            self.jobs.get(job_id).copied()
        }
    }

    loop {
        let msgs = match ce.messages().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "probe agent: messages() failed");
                tokio::time::sleep(Duration::from_millis(poll_ms.max(50) * 4)).await;
                continue;
            }
        };
        for msg in msgs {
            if msg.topic != ce_gke::protocol::PROBE_TOPIC {
                continue;
            }
            let Some(token) = msg.reply_token else { continue };
            let payload = match msg.payload() {
                Ok(p) => p,
                Err(_) => continue,
            };
            let requester = match parse_node_id(&msg.from) {
                Some(id) => id,
                None => continue,
            };
            // Refresh the job table per request batch (cheap; bounded by the node).
            let jobs = ce.jobs().await.unwrap_or_default();
            let map: std::collections::HashMap<String, Phase> = jobs
                .into_iter()
                .map(|j| (j.job_id, Phase::from_job_status(&j.status)))
                .collect();
            let source = NodeJobs { jobs: map };
            let authority = ProbeAuthority {
                self_id,
                accepted_roots: &[],
                self_tags: &status_tags(&ce, &status.node_id).await,
                now: now_secs(),
                is_revoked: &is_revoked,
                require_grant,
            };
            match handle_probe_bytes(&requester, &payload, &authority, &source) {
                Ok(reply) => {
                    let _ = ce.reply(token, &reply).await;
                }
                Err(e) => tracing::warn!(error = %e, "probe handling failed"),
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_ms.max(50))).await;
    }
}

/// Look up this host's advertised self-tags from the atlas (for resource-scoped grant checks).
async fn status_tags(ce: &CeClient, self_id: &str) -> Vec<String> {
    ce.atlas()
        .await
        .ok()
        .and_then(|a| a.into_iter().find(|e| e.node_id == self_id).map(|e| e.tags))
        .unwrap_or_default()
}

fn secret_cmd(op: &SecretOp, path: &std::path::Path) -> Result<()> {
    let mut store = SecretStore::load(path)?;
    match op {
        SecretOp::Set { name, value } => {
            let value = if value == "-" {
                use std::io::Read;
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf).context("reading secret from stdin")?;
                buf.trim_end_matches('\n').to_string()
            } else {
                value.clone()
            };
            store.set(name, value);
            store.save(path)?;
            println!("Set secret '{name}'.");
        }
        SecretOp::Rm { name } => {
            if store.remove(name) {
                store.save(path)?;
                println!("Removed secret '{name}'.");
            } else {
                println!("No secret '{name}'.");
            }
        }
        SecretOp::Ls => {
            let names = store.names();
            if names.is_empty() {
                println!("No secrets.");
            }
            for n in names {
                println!("{n}");
            }
        }
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

async fn get<D: MeshDriver>(
    driver: &D,
    store: &Store,
    namespace: &str,
    name: Option<&str>,
) -> Result<()> {
    let keys: Vec<String> = match name {
        Some(n) => {
            let key = format!("{namespace}/{n}");
            if store.get(&key).is_none() {
                bail!("no deployment '{key}'");
            }
            vec![key]
        }
        None => store.names_in(namespace),
    };
    if keys.is_empty() {
        println!("No deployments in namespace '{namespace}'. Apply one with `ce-gke apply -f deploy.yaml`.");
        return Ok(());
    }

    println!(
        "{:<24}  {:<24}  {:>8}  {:>9}  {:<16}",
        "NAME", "IMAGE", "DESIRED", "READY", "REVISION"
    );
    for key in &keys {
        let m = store.get(key).context("deployment present")?;
        let mut running = 0u32;
        for r in &m.replicas {
            let phase = driver
                .phase(&r.node_id, &r.job_id, m.spec.liveness.as_ref(), m.grant.as_deref())
                .await
                .unwrap_or(r.phase);
            if phase == Phase::Running {
                running += 1;
            }
        }
        let mut name_col = truncate(key, 24);
        if m.paused {
            name_col.push_str(" (paused)");
        }
        println!(
            "{:<24}  {:<24}  {:>8}  {:>9}  {:<16}",
            truncate(&name_col, 24),
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

/// Truncate a string to at most `n` characters with an ellipsis, never splitting a multibyte
/// UTF-8 scalar (the historical panic was slicing on a byte index inside a char).
fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    let keep = n.saturating_sub(3);
    let prefix: String = s.chars().take(keep).collect();
    format!("{prefix}...")
}

/// Strip the `namespace/` prefix from a key for user-facing re-run hints.
fn strip_ns(key: &str) -> &str {
    key.split_once('/').map(|(_, n)| n).unwrap_or(key)
}

/// Parse a 64-hex NodeId into `[u8; 32]`.
fn parse_node_id(hex_id: &str) -> Option<[u8; 32]> {
    let bytes = hex::decode(hex_id.trim()).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Some(out)
}

/// Extract the root issuer(s) of a grant token's chain so preflight can root at it.
fn grant_roots(token: &str) -> Result<Vec<[u8; 32]>> {
    let chain = ce_cap::decode_chain(token)?;
    Ok(chain.first().map(|c| vec![c.cap.issuer]).unwrap_or_default())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
    fn truncate_multibyte_does_not_panic() {
        // The historical panic: byte-slicing inside a multibyte scalar.
        let s = "café-very-long-name-with-accents-éééééééééé";
        let out = truncate(s, 10);
        assert!(out.chars().count() <= 10);
        assert!(out.ends_with("..."));
        // pure emoji string (4-byte scalars) must also be safe
        let emoji = "🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀";
        let out = truncate(emoji, 5);
        assert!(out.chars().count() <= 5);
    }

    #[test]
    fn strip_ns_works() {
        assert_eq!(strip_ns("default/web"), "web");
        assert_eq!(strip_ns("web"), "web");
    }

    #[test]
    fn parse_node_id_validates() {
        assert!(parse_node_id("zz").is_none());
        assert!(parse_node_id(&"ab".repeat(31)).is_none()); // 62 hex = 31 bytes
        assert!(parse_node_id(&"ab".repeat(32)).is_some()); // 64 hex = 32 bytes
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
