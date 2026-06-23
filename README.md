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

## Install / build

```bash
cargo build --release    # produces target/release/ce-gke
```

It talks to a local CE node's HTTP API (default `http://127.0.0.1:8844`); the node's API token is
auto-discovered (see `ce-rs`).

---

## CLI

```
ce-gke apply   -f deploy.yaml      # create/update a Deployment and reconcile to it
ce-gke get     [name]              # list deployments (or one) with READY counts + revision
ce-gke scale   <name> <replicas>   # change the replica count and reconcile
ce-gke rollout <name>              # reconcile to convergence now (drive a roll / heal failures)
ce-gke delete  <name>              # kill all replicas and forget the deployment
```

Global flags: `--node <url>`, `--grant <hex-cap-chain>`, `--state <path>`, `--max-ticks`, `--interval`.

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

```rust
use ce_gke::auth::{issue_host_grant, token};
use ce_cap::{Resource, Caveats};

// On the HOST (the resource owner), self-issue a deploy/kill grant to the orchestrator's NodeId:
let cap = issue_host_grant(&host_identity, orchestrator_node_id, Resource::Any, Caveats::default(), 1);
let grant = token(&[cap]);   // hex chain — pass as `--grant` (or per-deployment in state)
```

The orchestrator forwards `grant` on every `mesh-deploy` / `mesh-kill`; the **host** verifies it
(offline, in microseconds) — abilities `deploy` / `kill`, scoped resource, expiry, revocation. For a
fleet, one grant rooted at a key all hosts honor covers them all. `auth::preflight` mirrors the
host's check locally so the CLI can reject a bad/expired/over-broad token before hitting the mesh.

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

```bash
cargo test            # ~105 tests, all green
cargo clippy --all-targets   # clean
```

---

## License

MIT. Author: Leif Rydenfalk.
