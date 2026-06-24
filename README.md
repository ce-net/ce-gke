# ce-gke — a container orchestrator on CE

`ce-gke` is to CE what **GKE** is to Google Cloud: a declarative orchestrator that takes a
*Deployment* (image, replicas, resources) and keeps the world matching it — placing replicas across
atlas-ranked hosts over the mesh, replacing failed ones, rolling out new revisions, and scaling.

The twist: **there is no control plane to run or pay for.** ce-gke is a thin *client* over CE
primitives. It is the [swarm](https://github.com/ce-net/swarm) scatter pattern hardened into a
stateful, self-healing reconcile loop. It changes **nothing** in the node — it composes:

| CE primitive | Role in ce-gke |
|---|---|
| **jobs / `mesh-deploy` / `mesh-kill`** (via `ce-rs`) | run + stop a replica (a container cell) on a host |
| **atlas + history** (via `ce-rs`) | rank candidate hosts by free capacity, spread, freshness |
| **`ce-cap`** | a deploy is authorized by a signed, attenuating capability chain the host honors; the orchestrator forwards the grant token on every deploy/kill |
| **payment channels / credits** | each replica is funded with a `bid` (integer base units, decimal-string on the wire) |

It is an **app over the SDK** (`ce-rs` + `ce-cap`). No new node endpoints. No allowlists. No stored
ip:port. Authorization between nodes is always a `ce-cap` chain. This is the GKE entry from
[`12-google-infra-portfolio.md`](../PLAN/12-google-infra-portfolio.md), Wave 3.

---

## Features

- **Declarative Deployments** (YAML or JSON manifests), content-addressed pod-template `revision()`.
- **Atlas-ranked placement** — tag-fit + free-capacity + spread scoring + freshness, with host
  fallback when a chosen host rejects a deploy.
- **Self-healing** — failed/lost replicas are reaped and replaced.
- **Continuous reconcile daemon** (`ce-gke run`) — reconciles the whole fleet *forever* with graceful
  SIGINT/SIGTERM shutdown. This is what makes self-healing real between commands.
- **Rolling update / recreate** honoring `max_surge` / `max_unavailable` budgets.
- **Rollout history** + `rollout undo` / `pause` / `resume` (last 10 revisions kept).
- **Namespaces** — `--namespace`; two teams never collide on a name.
- **Health probes** — liveness (trips a replica to `Failed`) and readiness, exec/tcp/http, answered by
  the host-side `ce-gke serve` agent over the mesh (authoritative host view, not the payer's cache),
  with a status-only fallback when no agent runs.
- **Env vars & secrets** — inline `env`, or `value_from` a local secret store (`ce-gke secret set`)
  that keeps credentials out of manifests and persisted state.
- **Service discovery** — advertise the healthy replica set on the DHT (`ce-gke/<ns>/<service>`).
- **Capability + balance preflight** — a bad/expired grant or insufficient balance fails fast.
- **Atomic, locked, versioned state** — temp-file + fsync + rename, an advisory inter-process lock,
  and a schema version with forward migration from older files.

---

## Install / build

```bash
cargo build --release    # produces target/release/ce-gke
```

It talks to a local CE node's HTTP API (default `http://127.0.0.1:8844`); the node's API token is
auto-discovered (see `ce-rs`).

---

## CLI

```
ce-gke apply   -f deploy.yaml          # create/update a Deployment and reconcile to it
ce-gke get     [name]                  # list deployments (or one) with READY counts + revision
ce-gke scale   <name> <replicas>       # change the replica count and reconcile
ce-gke rollout status  <name>          # reconcile to convergence now (drive a roll / heal failures)
ce-gke rollout undo    <name> [--to-revision <hash>]   # roll back to a prior revision
ce-gke rollout pause   <name>          # hold the rollout (count/health reconcile still runs)
ce-gke rollout resume  <name>          # resume a paused rollout
ce-gke rollout history <name>          # show kept revisions
ce-gke delete  <name> [--force]        # kill all replicas and forget the deployment
ce-gke run     [--every N] [--all-namespaces]   # long-running daemon: reconcile the fleet forever
ce-gke serve   [--require-grant]       # host-side health agent (answer probe requests)
ce-gke secret  set|rm|ls               # manage secrets for value_from env vars
```

Global flags: `--node <url>`, `--grant <hex-cap-chain>`, `--namespace/-n <ns>`, `--state <path>`,
`--secrets <path>`, `--max-ticks`, `--interval`.

### The daemon — real self-healing

`ce-gke apply` reconciles only within a bounded tick budget, then returns. To keep the fleet healthy
*between* commands (a replica that dies after `apply` returns), run the daemon:

```bash
ce-gke run --every 10            # reconcile every namespace's deployments every 10s, forever
ce-gke -n prod run --every 10    # just the `prod` namespace
```

It loops `reconcile` over every managed deployment, replacing failures and re-advertising services,
until you Ctrl-C (it shuts down gracefully). It holds the state lock only briefly per pass, so
interactive commands still work alongside it.

State (desired specs + the replica handles the controller launched) is persisted to
`<data_dir>/ce/gke-state.json`, so the orchestrator is **stateful across invocations** with no
server. Each mutating command runs reconcile ticks against the live node until the deployment
converges (or the tick budget is hit), then saves state.

### A manifest (YAML or JSON — `from_manifest` reads both)

```yaml
name: web
image: nginx:1.25
replicas: 3
resources:
  cpu_cores: 1
  mem_mb: 256
select:            # only place on hosts advertising all of these atlas self-tags ("docker" implied)
  - docker
bid: "5000000000000000000"   # 5 credits per replica, in base units (10^18 = 1 credit)
duration_secs: 3600
strategy:
  type: rolling_update       # or: { type: recreate }
  max_unavailable: 1         # never fewer than (replicas - 1) ready during a roll
  max_surge: 1               # never more than (replicas + 1) total during a roll
```

```bash
ce-gke apply -f web.yaml
ce-gke get web
ce-gke scale web 5
# edit web.yaml: image: nginx:1.26
ce-gke apply -f web.yaml        # rolling update to the new revision
ce-gke delete web
```

---

## Library

The binary is a thin shell over a fully-tested library (`ce_gke`). The orchestration logic is split
into **pure planners** (no I/O, exhaustively unit- and property-tested) and a **driver** (the
side-effecting mesh layer, mockable for failure injection):

| Module | What it does | Purity |
|---|---|---|
| `spec` | the `Deployment` desired-state type, YAML/JSON manifests, content-addressed `revision()` | pure |
| `placement` | rank atlas hosts for a replica (tag-fit + capacity + spread + freshness) | pure |
| `reconcile` | the steady-state diff: desired vs actual replica count, reap failures | pure |
| `rollout` | rolling-update / recreate **step planner** honoring surge & unavailable budgets | pure |
| `driver` | `MeshDriver` trait + real `CeDriver` (over `ce-rs`) + deterministic `FakeDriver` | I/O |
| `controller` | one reconcile **tick** wiring the planners to a driver; `converge()` to a fixed point | I/O |
| `auth` | build / pre-flight-verify the `ce-cap` chain authorizing deploys on hosts | pure |
| `state` | persist managed specs + replica handles (`gke-state.json`) | I/O |

```rust
use ce_gke::{spec::Deployment, driver::CeDriver, controller::Controller};

# async fn demo() -> anyhow::Result<()> {
let d = Deployment::from_manifest("name: web\nimage: nginx:1.25\nreplicas: 3\n")?;
let driver = CeDriver::new("http://127.0.0.1:8844");
let mut ctrl = Controller::new(None /* optional ce-cap grant token */);
let report = ctrl.tick(&driver, &d).await?;   // place/kill to move toward desired
println!("placed {} replica(s)", report.placed.len());
# Ok(()) }
```

### How a reconcile tick works

1. **Health refresh** — re-read each tracked replica's phase from the node (`pending` / `running` /
   `failed:*` / `settled`). A replica the node no longer knows about is treated as `Failed`
   (fail-safe — it gets rescheduled, never a panic).
2. **Plan** — `rollout::plan_step` computes the next batch against the desired `revision()`,
   respecting the strategy budgets (surge ahead, unavailable behind). For an unchanged revision this
   reduces to plain reconcile (scale up/down, replace failures).
3. **Kill** what the plan retired (drop killed replicas from tracking; a failed kill is retried next
   tick, and a stuck count-based scale-down victim is substituted by an interchangeable replica).
4. **Place** what the plan wants, on `placement::rank`-ordered candidates — a host that rejects a
   deploy is dropped and the next-best host is tried; if none can take it, the shortfall is reported,
   not panicked.

`Controller::converge` drives ticks to a fixed point. Running it repeatedly is idempotent: at the
desired state every tick is a no-op.

---

## Authorization (ce-cap)

A host runs *your* replica only if you present a capability chain it honors. A host operator
onboards an orchestrator exactly like `ce grant`:

```rust,ignore
use ce_gke::auth::{issue_host_grant, token};
use ce_cap::{Resource, Caveats};
use ce_identity::Identity;

// On the HOST (the resource owner), self-issue a deploy/kill/probe grant to the orchestrator's id:
let host: Identity = Identity::load_or_generate("/var/lib/ce/identity".as_ref())?;
let orchestrator_node_id = /* the orchestrator's [u8;32] NodeId */ [0u8; 32];
let cap = issue_host_grant(&host, orchestrator_node_id, Resource::Any, Caveats::default(), 1);
let grant = token(&[cap]);   // hex chain — pass as `--grant` (or per-deployment in state)
```

The orchestrator forwards `grant` on every `mesh-deploy` / `mesh-kill` / probe; the **host** verifies
it (offline, in microseconds) — abilities `deploy` / `kill` / `probe`, scoped resource, expiry,
revocation. For a fleet, one grant rooted at a key all hosts honor covers them all. `auth::preflight`
mirrors the host's check locally so the CLI can reject a bad/expired/over-broad token before hitting
the mesh, and `apply` also runs a **balance pre-check** so a too-large `replicas * bid` fails fast.

---

## Tests

Built with tests from the start — the foundation is validated, not assumed:

- **Unit tests on every public fn** (happy + error paths): manifest parsing, revision hashing,
  placement fit/score/rank, reconcile scale up/down + failure reaping, rolling-update step logic for
  every branch (surge, retire, recreate, scale-down-mid-roll, to-zero), capability preflight,
  state persistence.
- **Property tests** (`tests/properties.rs`): manifest JSON roundtrips, placement-score
  monotonicity, reconcile count-conservation (no thrash), and — the load-bearing one — that a
  rolling update **always converges and never breaches its surge ceiling** for arbitrary cluster
  shapes and strategy parameters.
- **Failure injection** via `FakeDriver`: a host that rejects a deploy → fall back to another; a
  dropped peer on kill → retried / substituted; a `running` replica that dies → rescheduled; an
  empty/non-docker atlas → reported, never panics; malformed manifests/tokens → graceful errors.

- **Integration tests** (`tests/cli_flow.rs`): the full apply -> scale -> rolling-update -> undo ->
  delete lifecycle and the daemon reconcile-pass driving multiple namespaces, plus statefulness
  across simulated CLI invocations (load -> mutate -> save).
- **Probe protocol + agent** (`serve` / `protocol`): authorization (grant required / open / wrong
  requester / malformed / revoked), unknown-job-is-failed, size bounds, and round-trips.

```bash
# Do NOT run cargo locally in this workspace (shared full disk). Use the Hetzner box:
tools/remote-test.sh ce-gke --clippy   # 162 tests + 2 doctests, all green; clippy clean
```

---

## Architecture & limitations (vs GKE)

ce-gke is a deliberate **CLI + daemon client**, not an always-on control plane with its own etcd.
Desired state and replica handles live in `gke-state.json`; the `run` daemon supplies the continuous
reconcile loop GKE's controllers provide. See [`docs/architecture.md`](docs/architecture.md) for the
deploy -> escrow -> heartbeat -> settle lifecycle and the CLI-vs-daemon ADR.

| GKE capability | ce-gke | Notes |
|---|---|---|
| Declarative Deployments | yes | YAML/JSON, content-addressed revision |
| Scheduler / node fit + scoring | yes | atlas-ranked, spread, freshness, fallback |
| Self-healing controllers | yes | `ce-gke run` daemon (continuous), CLI ticks (bounded) |
| Rolling update / Recreate | yes | surge / unavailable budgets, property-tested |
| Rollout history / undo / pause | yes | last 10 revisions |
| Namespaces | yes | scoped keys + per-namespace listing |
| Liveness / readiness probes | partial | host-agent reports authoritative phase; in-cell probe **command** execution is deferred to a container-exec sidecar (CE exposes no exec RPC to apps) |
| Env vars / Secrets | yes | inline + `value_from` local store; injected via `env` command wrap (requires explicit `command`) |
| Services / discovery | partial | DHT advertise of the healthy set; client-side LB, virtual IP/DNS deferred |
| Horizontal autoscaling (HPA) | **deferred** | atlas exposes load; the autoscaler loop is future work |
| StatefulSet / DaemonSet / CronJob | **deferred** | only the Deployment kind today |
| ConfigMaps / volumes | **deferred** | env + secrets cover config; persistent volumes are future work |

Nothing above is faked: deferred items are absent and labelled, not stubbed to look present.

---

## License

MIT. Author: Leif Rydenfalk.
