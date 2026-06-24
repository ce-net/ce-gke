# ce-gke architecture

This document records the deliberate design decisions behind ce-gke and the runtime lifecycle of a
deployment. It is the ADR the audit asked for: ce-gke is **not** a GKE-style always-on control plane
with its own etcd; it is a thin client + an optional daemon over CE primitives.

## ADR-1: CLI + daemon, not a hosted control plane

**Context.** GKE runs a managed control plane (API server, scheduler, controller-manager, etcd) that
reconciles forever. CE's thesis is "no control plane to run or pay for": apps are clients over node
primitives.

**Decision.** ce-gke keeps desired state and replica handles in a local JSON file
(`<data_dir>/ce/gke-state.json`). Mutating CLI commands (`apply`/`scale`/`rollout`) run a bounded
number of reconcile ticks and return. For continuous self-healing — the property that makes "GKE"
real — `ce-gke run` is a long-running daemon that loops the same reconcile over every managed
deployment on an interval, indefinitely, until a graceful shutdown signal.

**Consequences.**
- No server to operate; the operator's node + state file are the whole system.
- Between CLI commands the fleet is only as healthy as the last command *unless* the daemon runs.
  This is documented prominently; the daemon closes the gap.
- State is a cross-invocation contract: it is versioned (`STORE_VERSION`) with forward migration, and
  guarded by an advisory inter-process lock so two `ce-gke` processes never lose each other's writes.

## ADR-2: capability authorization, never an allowlist

A deploy/kill/probe on a remote host is authorized by a signed, attenuating `ce-cap` chain the host
honors (rooted at the host's own key or a configured org root). The orchestrator forwards the grant
token on every mesh call; the **host** is the enforcement point. ce-gke additionally runs
`auth::preflight` locally to fail fast on an obviously-bad token, and the host-side `ce-gke serve`
agent re-verifies probe grants. There is no device list and no stored ip:port — purely mesh-routed.

## ADR-3: health beyond CE job status

CE reports a coarse job status (`pending`/`running`/`failed:*`). A container that is "running" per
the host may still be unhealthy. ce-gke layers probes:

- The orchestrator asks the host (over the `ce-gke/probe/1` mesh request topic) for the
  **authoritative** phase of a job — the host's own view, not the payer's cache. This alone fixes the
  case where the payer loses track of a remote job.
- The probe protocol carries the in-cell check **command** (exec/tcp/http mapped to a shell check).
  Running it requires container-exec, which CE intentionally does not expose to apps (exec is the
  `rdev` app's domain). So today the agent fills `probe_passed` only where a container-exec sidecar
  exists; otherwise it returns the host phase and `probe_passed = None`. This is a real slice with a
  documented seam, not a stub: a sidecar can fill the field with **no protocol change**.

## Deployment lifecycle (deploy -> escrow -> heartbeat -> settle)

1. **Placement.** `placement::rank` scores atlas hosts (tag-fit, free capacity, spread, freshness)
   and the controller deploys one replica per the rolling/recreate plan, falling back to the next
   candidate if a host rejects.
2. **Escrow.** Each replica is funded with the spec `bid` (locked on `mesh-deploy`). `apply` runs a
   balance pre-check (`replicas * bid <= spendable`) so a fat-fingered count fails before any deploy.
3. **Heartbeat.** The cell runs on the host; the CE node bills it in 30s heartbeat intervals (a CE
   primitive, not ce-gke logic). ce-gke's reconcile refreshes each replica's phase each tick.
4. **Settle / replace.** When a replica finishes, fails, or its probe trips, the controller reaps it
   and places a replacement to hold the desired count. `delete` kills all replicas and settles.

Billing is **Cloud-Run-min-instance shaped** (you pay for the held cell via heartbeats), not
per-request FaaS. There is no per-invocation meter; a Deployment is a perpetual service.

## State file schema (versioned)

```jsonc
{
  "version": 1,
  "deployments": {
    "<namespace>/<name>": {
      "spec":   { /* Deployment */ },
      "replicas": [ { "job_id", "node_id", "revision", "phase" } ],
      "grant":  "<hex ce-cap chain>",
      "history": [ /* prior Deployment specs, newest last, capped at 10 */ ],
      "paused": false
    }
  }
}
```

A v0 file (no `version`, bare-name keys, no namespace) is migrated forward on load: keys are re-scoped
to `default/<name>` and `namespace` is backfilled. Secrets live in a separate `gke-secrets.json`
(`0600`) so credentials never enter the shareable desired state.

## Module map

| Module | Purity | Role |
|---|---|---|
| `spec` | pure | Deployment + manifests + revision + env/probe/service + effective-command |
| `placement` | pure | atlas fit + scoring + rank |
| `reconcile` | pure | steady-state diff (count, reap failures) |
| `rollout` | pure | rolling-update / recreate step planner |
| `controller` | I/O | one reconcile tick wiring planners to a driver (probe, advertise, bounded-retry) |
| `daemon` | I/O | continuous reconcile-pass over the fleet + graceful shutdown |
| `driver` | I/O | `MeshDriver` trait + real `CeDriver` + `FakeDriver` |
| `protocol` / `serve` | mixed | host-side probe protocol + agent |
| `secrets` | I/O | local secret store |
| `state` | I/O | versioned, locked, atomic persistence |
| `auth` | pure | ce-cap grant issue + preflight |
