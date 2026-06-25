# ce-gke-rs — deferred decisions (documented, not yet resolved)

Two design decisions are intentionally **deferred** (Leif: "none of those two decisions are
priority — document them and work on the actual important stuff"). They do not block the SDK; the
SDK is built so either resolution drops in behind a trait.

## Issue 1 — Hard singleton claims

**Context.** A "singleton" dependency (e.g. one encoder per stream) needs one owner. `ce-coord` is
eventually-consistent and cannot prevent two nodes from claiming the same service name at the exact
same instant.

**Current approach (good enough, shipped as the default).** A claim is a `ce-coord` record with a
deterministic tiebreak: on a simultaneous double-claim, the **lowest node id wins**; the loser
observes the converged registry and **releases** (self-heal). Worst case is a brief transient
duplicate that resolves on the next convergence — acceptable for app orchestration.

**Open question.** Do we ever need *hard* global uniqueness for an app singleton (no transient
duplicate ever)? If so, that needs a stronger primitive (a coordination lock or an on-chain claim).
Money/identity/global-uniqueness already use the chain; this is only about whether any *app*
singleton needs that strength. **Deferred** — the tiebreak+self-heal default stands until a concrete
case demands more. The SDK isolates this in `claim.rs` (the `winner()` tiebreak) + the `Registry`
trait, so a stronger backend swaps in without touching callers.

## Issue 2 — Donation counted by vault-operator (not by node key)

**Context.** A local native node and the same operator's browser node are **distinct keys** (a
browser can't hold the native node's secret) but must count as **one donor / one device**, not two
(Sybil + fairness). The binding is the **ce-secrets vault**: both are devices of one vault =
one operator.

**Requirement this creates.** The economy / donation accounting must attribute compute to the
**vault-linked operator**, not the raw node key — otherwise "counted as one" doesn't hold and an
operator's surfaces look like separate donors.

**Open question.** Does the existing compute-donation / Sybil accounting already attribute by an
operator identity that a vault can map to, or does it count raw node keys? This must be checked
against the Sybil-security / compute-fabric work before "counted as one" is real. **Deferred** —
documented here so it isn't silently assumed. It is an *economy* property, independent of this SDK.

---

These live with `ce-gke-rs` because they're properties of the dependency/donation system. Revisit
when a concrete need (a hard singleton, or live double-counting) forces the decision.
