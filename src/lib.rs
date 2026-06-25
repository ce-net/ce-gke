//! # ce-gke — a container orchestrator (GKE-like) on CE
//!
//! ce-gke is to CE what GKE is to Google: a declarative orchestrator that takes a [`Deployment`]
//! (image, replicas, resources) and reconciles the world to match it — placing replicas across
//! atlas-ranked hosts over CE's `mesh-deploy`, replacing failed ones, rolling out new revisions,
//! and scaling. **There is no control plane to run or pay for**: the orchestrator is a client over
//! CE primitives (jobs/atlas for compute + placement, ce-cap for authorization, the mesh for
//! transport), exactly the swarm scatter pattern hardened into a stateful, self-healing loop.
//!
//! ## Architecture (honor the CE boundary)
//!
//! Everything here is an **app over the SDK** (`ce-rs` + `ce-cap`). No node changes, no new
//! endpoints, no stored ip:port. A deploy on a remote host is authorized by a signed, attenuating
//! [capability chain](crate::auth) the host honors; the orchestrator forwards the grant token on
//! every `mesh-deploy`/`mesh-kill`. Money is integer base units, decimal strings on the wire.
//!
//! ## The pieces
//!
//! - [`spec`] — the [`Deployment`] desired-state type (+ YAML/JSON manifests, revision hashing).
//! - [`placement`] — rank atlas hosts for a replica (tag-fit + capacity + spread scoring). Pure.
//! - [`reconcile`] — the steady-state diff: desired vs actual replica count, reap failures. Pure.
//! - [`rollout`] — rolling-update / recreate step planner honoring surge/unavailable budgets. Pure.
//! - [`driver`] — the side-effecting mesh layer ([`MeshDriver`] trait) + a real [`CeDriver`] and a
//!   deterministic [`FakeDriver`] for failure-injection tests.
//! - [`controller`] — one reconcile [`tick`](controller::Controller::tick) wiring the pure planners
//!   to a driver; [`converge`](controller::Controller::converge) drives it to a fixed point.
//! - [`daemon`] — the long-running [`run_daemon`](daemon::run_daemon) loop: reconcile the whole fleet
//!   forever (real self-healing), with graceful shutdown.
//! - [`auth`] — build/verify the capability chain authorizing deploys/kills/probes on hosts.
//! - [`protocol`] / [`serve`] — the host-side health-probe protocol and agent.
//! - [`secrets`] — local secret store for `value_from` env vars (kept out of manifests/state).
//!
//! ## What this is and is not (vs GKE)
//!
//! Implemented: declarative Deployments, atlas-ranked placement with spread + host fallback, rolling
//! update / recreate, steady-state self-healing, a continuous reconcile **daemon**, namespaces,
//! revision history + `rollout undo`/`pause`/`resume`, liveness/readiness probes (host-agent +
//! status fallback), env vars + secret refs, named-service advertisement on the DHT, capability
//! preflight + balance pre-check, atomic + locked state. Deferred (documented, not faked): a
//! container-exec data plane for in-cell probe commands (the protocol carries them; the agent fills
//! `probe_passed` only where a sidecar exists), HPA autoscaling, Service load-balancing beyond
//! discovery, and StatefulSet/DaemonSet/CronJob workload kinds. See the README for the full matrix.
//!
//! ## Example
//!
//! ```no_run
//! use ce_gke::{spec::Deployment, driver::CeDriver, controller::Controller};
//! # async fn demo() -> anyhow::Result<()> {
//! let d = Deployment::from_manifest(r#"
//! name: web
//! image: nginx:1.25
//! replicas: 3
//! "#)?;
//! let driver = CeDriver::new("http://127.0.0.1:8844");
//! let mut ctrl = Controller::new(None /* grant token */);
//! // One reconcile tick: place/kill to move the world toward `d`.
//! let report = ctrl.tick(&driver, &d).await?;
//! println!("placed {} replica(s)", report.placed.len());
//! # Ok(()) }
//! ```

pub mod auth;
pub mod controller;
pub mod daemon;
pub mod deps;
pub mod driver;
pub mod placement;
pub mod protocol;
pub mod reconcile;
pub mod rollout;
pub mod secrets;
pub mod serve;
pub mod spec;
pub mod stack;
pub mod state;

pub use controller::{Controller, TickReport};
pub use daemon::{run_daemon, DaemonConfig};
pub use deps::{DepReadiness, Gate};
pub use driver::{CeDriver, FakeDriver, MeshDriver};
pub use placement::{rank, Candidate};
pub use protocol::{ProbeReply, ProbeRequest, PROBE_TOPIC};
pub use reconcile::{reconcile, Phase, Plan, ReplicaState};
pub use rollout::{plan_step, RolloutStep};
pub use secrets::SecretStore;
pub use spec::{DependencyRef, Deployment, EnvVar, Probe, ProbeKind, Resources, Strategy};
pub use stack::{topo_sort, Stack};
pub use state::{ManagedDeployment, StateLock, Store};
