# Changelog

All notable changes to ce-gke are documented here. This project adheres to semantic versioning.

## [Unreleased]

### Added
- **Long-running controller daemon** (`ce-gke run`): reconciles every managed deployment forever on
  a configurable interval, with graceful SIGINT/SIGTERM shutdown. Real self-healing between commands.
- **Health probes**: liveness (trips a replica to `Failed`) and readiness, with `exec`/`tcp`/`http`
  kinds. Answered by the new host-side **`ce-gke serve`** agent over the `ce-gke/probe/1` mesh topic,
  giving the orchestrator the host's authoritative job phase instead of the payer's cache; a
  status-only fallback applies where no agent runs.
- **Namespaces** (`--namespace/-n`): store keys are now `namespace/name`; two teams never collide.
- **Rollout history + operations**: `rollout undo` (to previous or `--to-revision`), `pause`,
  `resume`, `history`. Up to 10 prior revisions kept.
- **Environment variables & secrets**: `env` (inline) and `value_from` a local secret store
  (`ce-gke secret set|rm|ls`, `0600`), injected by wrapping the command with `env NAME=VALUE`.
- **Service discovery**: `service:` advertises the healthy replica set on the DHT as
  `ce-gke/<namespace>/<service>`.
- **Capability + balance preflight** in `apply`: a bad/expired grant or insufficient
  `replicas * bid` fails fast with a clear error instead of opaque place-failures.
- **`--force` on `delete`** to forget a deployment whose host is unreachable.
- `protocol`, `serve`, `daemon`, `secrets` modules; `examples/web-full.yaml`, `examples/batch.json`,
  `examples/reconcile_demo.rs`; `docs/architecture.md` (ADR + lifecycle); this changelog.

### Changed
- **State file is versioned** (`STORE_VERSION`) with forward migration from v0 (bare-name, no
  namespace) files; saved atomically (temp + **fsync** + rename) under an **advisory inter-process
  lock** so concurrent `ce-gke` processes cannot lose each other's updates.
- `refresh` uses **bounded retry**: a replica is only marked `Failed` after `failure_threshold`
  consecutive refresh errors, preventing a transient API blip from triggering a fleet-wide reschedule
  storm. The controller now consults the host's authoritative phase rather than only the local payer.
- `rollout` is now a subcommand group (`status`/`undo`/`pause`/`resume`/`history`).

### Fixed
- **UTF-8 panic** in `get`'s `truncate`: it sliced on a byte index that could split a multibyte
  scalar. Truncation is now char-aware, and image references are validated to the registry ASCII
  subset (and bounded length), so a multibyte image can never reach the truncation path.
- **DoS bounds**: replica count capped at `MAX_REPLICAS` (4096); image, env-var count/size, and probe
  message size are all bounded and validated on both encode and decode.

### Tests
- ~105 -> 162 tests + 2 doctests, all green, clippy clean. New: integration lifecycle
  (`tests/cli_flow.rs`), daemon reconcile-pass, probe protocol/agent authorization + bounds,
  liveness-probe reschedule, bounded-retry-no-storm, service advertisement, multibyte truncation,
  namespace isolation, state migration + lock, and placement no-over-commit property.
