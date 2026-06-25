# Org-root-attested "stable" host designation

A deployment can require that its replicas land only on **vetted, long-lived** hosts via the
placement affinity `require_stable: true`. This document explains what a stable attestation is, where
it lives, the mint flow, and the exact verification path — so the affinity cannot be satisfied by a
Sybil that merely self-claims a `stable` tag.

## The problem with a self-claimed tag

The atlas carries each node's **self-advertised** capability tags (`docker`, `gpu`, `linux`, ...).
A node controls its own `CE_TAGS`, so anyone can start a node that advertises `stable`. Filtering on
a self-claimed atlas tag would therefore designate any Sybil as "stable". The designation must come
from an authority the orchestrator trusts, not from the host itself.

## What a stable attestation is

A **stable attestation** is one `ce-cap` `SignedCapability` (a single-link chain) issued by the
offline organization root key — the same `ce-root` whose **public** key is pinned in every node's
`<data_dir>/roots/` per `ce-fleet`'s trust spine:

| Field | Value |
|---|---|
| `issuer` | the org root (`ce-root`) |
| `audience` | the host's own NodeId (the host holds and serves its own attestation) |
| `abilities` | `["stable"]` (`ce_gke::stable::ABILITY_STABLE`) |
| `resource` | `Resource::Node(host_id)` — applies to exactly this node |
| `caveats` | optional expiry (`not_after`) so a decommissioned host loses the designation offline |
| `parent` | `None` — a root delegation; its issuer must be the pinned org root |

It is verified **offline** (no network, no chain lookup) against the pinned org-root pubkey. Because
`ce-cap` enforces the root issuer, the Ed25519 signature, the audience, the resource match, and
expiry, a host that presents a valid attestation provably had `ce-root` vouch for *that NodeId*.

## Where it lives, and how placement sees it

The host **serves** its own attestation; nothing is stored as ip:port and the node binary is
unchanged. The flow:

1. The operator installs the minted token on the host at `<data_dir>/ce-gke/stable.token` (or sets
   `CE_GKE_STABLE_TOKEN` to the token or a path).
2. `ce-gke serve` (the existing host-side agent) loads it and answers `AttestQuery` requests on the
   new mesh topic `ce-gke/stable/1` with the token (an `AttestReply`). This is a *public* query — a
   stable attestation is org-root vouching meant to be advertised, so no grant is required.
3. Before placing a `require_stable` deployment, the controller queries each fresh, tag-fitting atlas
   candidate's attestation (`MeshDriver::stable_attestation`, over `ce-gke/stable/1`), verifies it
   offline, and passes only the proven-stable NodeIds into `placement::rank` as `stable_ids`. A host
   not in that set is not a candidate.

Carrying the attestation on the host (served on demand) — rather than in the atlas — is the cleanest
fit: the atlas is node-controlled (untrustworthy for this purpose) and cannot be changed without a
node change, whereas `ce-gke serve` is already the orchestrator's authenticated per-host channel.

## The exact verify path (`ce_gke::stable::verify_stable`)

```
decode_chain(token)                                  // hex -> Vec<SignedCapability>
require chain[0].issuer == org_root                  // close the implicit-self-root hole (see below)
ce_cap::authorize(
    self_id        = host_id,                         // Resource::Node(host_id) matches against this
    accepted_roots = [org_root],                      // the ONLY accepted authority
    self_tags      = [],                              // a NodeId binding uses no tags
    now            = <unix secs>,                     // enforces not_after expiry
    requester      = host_id,                         // audience-bound to the host
    action         = "stable",                        // ABILITY_STABLE
    chain,
    is_revoked,                                        // on-chain RevokeCapability, or |_,_| false offline
)
```

**Why the explicit `issuer == org_root` guard.** `ce_cap::authorize` treats `self_id` as an *implicit
accepted root* (so a node can always delegate its own resources). We must pass `self_id = host_id` for
the `Resource::Node(host_id)` match — but that would also make the host's own key an implicit root,
letting a host self-mint its own `stable` cap. Requiring `chain[0].issuer == org_root` before
authorizing closes that hole: the attestation must come from the org root, never the host.

This is a thin wrapper over `ce_cap::authorize` — verification is **not** re-implemented here.

## Minting (the operator runs this once per stable host)

```bash
ce-gke stable mint <host-node-id> --root-key /secure/ce-root --expires-days 365 --nonce 7
```

- `--root-key` is the offline org-root identity directory; the signing key never leaves that machine.
- Prints the attestation token, the org-root pubkey to pin as `--org-root`, and the install path.
- Install the token on the host as `<data_dir>/ce-gke/stable.token`.

Verify a minted token before installing:

```bash
ce-gke stable verify <token> --node-id <host-node-id> --org-root <root-pubkey-hex>
```

## Using the affinity

```yaml
name: ledger
image: postgres:16
replicas: 3
require_stable: true        # only org-root-attested hosts
```

Run the orchestrator with the pinned root:

```bash
ce-gke --org-root <ce-root-pubkey-hex> apply -f ledger.yaml
ce-gke --org-root <ce-root-pubkey-hex> run            # daemon: same flag
```

If no `--org-root` is pinned, a `require_stable` deployment **fails closed** — it finds no candidate
rather than trusting any host (an orchestrator with no pinned root cannot vouch for stability).

## Security properties (tested)

- A host presenting a valid org-root attestation **is** selected by `stable` affinity.
- A host with **no attestation** is **not** selected.
- A host that **self-claims a `stable` atlas tag** (no attestation) is **not** selected.
- An attestation signed by a **non-root key** is **not** selected (root-pin mismatch).
- An attestation for a **different NodeId** does not vet this host (resource/audience mismatch).
- An **expired** or **revoked** attestation is rejected.
- With **no pinned org root**, `require_stable` finds no candidate (fail closed).

See `src/stable.rs` (unit) and `src/controller.rs` (end-to-end through a reconcile tick).
