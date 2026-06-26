# ce-gke + ce-appmgr: how they layer

ce-gke is the **orchestrator** (declarative Deployments, atlas placement, replicas,
rolling update, scale, reconcile). ce-appmgr is the **packager + per-node agent**
one level below the image string (resolve `ceapp.toml` -> deps -> fetch+verify
artifact -> mint scoped cap -> materialize -> single-instance supervise + register).

They compose; **ce-gke uses ce-appmgr**, not the other way round. Full design in
`~/ce-net/PLAN/ce-app-package-runtime.md` ("Relationship to ce-gke").

## What ce-gke does NOT have to build

Artifact fetch/verify, dependency resolution, versioning/registry, packaging,
per-app sandbox profiles, scoped capabilities — those live in ce-appmgr. ce-gke
keeps owning desired-state, placement, replicas, rollout, and the reconcile loop.

## The seam: `app://` image references

`Deployment.image` keeps accepting a raw image string (e.g. `nginx:1.25`) — that
path is unchanged and needs no ce-appmgr. It additionally accepts:

```
image: app://<name>[@<version-req>]      # e.g. app://postgres@16
```

When the image is an `app://` ref, ce-gke resolves it through ce-appmgr **before
placing replicas**:

1. ce-gke calls ce-appmgr resolve(app, version_req).
2. ce-appmgr returns a resolved runnable: a pinned oci image digest (or native
   artifact handle), the sandbox profile, required capabilities, and any dependency
   services the app declared.
3. ce-gke schedules N replicas of that resolved runnable across atlas-ranked hosts
   via the same `mesh-deploy` it uses today.
4. On each host the ce-appmgr agent materializes + runs + locally supervises that
   one instance and registers it (with health) to ce-hub.
5. ce-gke reconciles the replica count from ce-hub instance health.

## Simplification: instance health from ce-hub

Once ce-appmgr's instance registry (PLAN M4) lands, ce-gke can read per-replica
health from ce-hub's global instance registry instead of running its own
`ce-gke serve` host agent. Same data, one source, less bespoke code.

## Division of supervision (no collision)

- ce-appmgr supervises **one instance's liveness on a host**.
- ce-gke decides **the fleet shape across hosts** (how many, where, rollout).

Single instance (a CLI, one daemon) → ce-appmgr alone. Replicated/scaled workload
→ ce-gke delegating resolution + materialization to ce-appmgr.
