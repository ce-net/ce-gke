//! Org-root-attested "stable" host designation.
//!
//! A placement affinity (`stable: true`) must be able to require that a candidate host is a
//! *vetted, long-lived* node — not merely a host that self-claims a `stable` atlas tag. Self-claimed
//! tags are advertised by the node itself and are therefore trivially forgeable by a Sybil: anyone
//! can start a node that lists `stable` in its `CE_TAGS`. That is exactly the trust gap this module
//! closes.
//!
//! ## What a stable attestation is
//!
//! A **stable attestation** is one [`ce_cap::SignedCapability`] issued by the offline organization
//! root key (`ce-root`, the same key pinned in every node's `<data_dir>/roots/` per `ce-fleet`'s
//! trust spine) that binds a *specific* [`NodeId`] as a stable host:
//!
//! - `issuer`   = the org root (`ce-root`),
//! - `audience` = the host's own NodeId (so the host holds and serves its own attestation),
//! - `abilities`= `["stable"]` (the [`ABILITY_STABLE`] action string),
//! - `resource` = `Resource::Node(host_id)` (the attestation applies to exactly this node),
//! - `parent`   = `None` (a root delegation — its issuer must be the pinned org root).
//!
//! It is verified **offline** against the pinned org-root public key with `ce_cap::authorize`: no
//! network, no chain lookup. Because `ce-cap` enforces the root issuer, the signature, the audience,
//! the resource match, and (for a single link) attenuation trivially, a host that presents a valid
//! attestation provably had `ce-root` vouch for *that NodeId*. A node cannot mint one for itself, and
//! an attestation signed by any non-root key is rejected at the root check.
//!
//! ## Where it lives
//!
//! The host **serves** its own attestation over the existing `ce-gke serve` mesh channel (carried in
//! the probe reply, and answerable on its own to a candidate query — see [`crate::protocol`] and
//! [`crate::driver`]). The orchestrator collects the candidate hosts' attestations before placement
//! and verifies each here. Nothing is stored as ip:port; the attestation is a self-describing,
//! offline-verifiable token, so a poisoned atlas advertisement grants no stable status.
//!
//! ## Minting
//!
//! The operator runs the `ce-gke stable mint` CLI once per stable host with the offline root key; it
//! calls [`mint_stable_attestation`] and prints the hex token, which is installed on the host
//! (e.g. `<data_dir>/ce-gke/stable.token`) for the serve agent to hand out.

use anyhow::Result;
use ce_cap::{authorize, decode_chain, encode_chain, Caveats, Resource, SignedCapability};
use ce_identity::{Identity, NodeId};

/// The opaque ce-cap ability string an org root grants to designate a host as stable. Opaque to
/// `ce-cap` (it assigns abilities no meaning); ce-gke owns this vocabulary, exactly like
/// [`crate::auth::ACTION_DEPLOY`].
pub const ABILITY_STABLE: &str = "stable";

/// Mint a stable attestation for `host_id`, signed by the offline org root `root`.
///
/// The attestation is a single-link ce-cap chain rooted at `root`, audience-bound to `host_id`, with
/// resource `Node(host_id)` and ability `stable`. `caveats` bound it (e.g. an expiry so a
/// decommissioned host loses its designation without a revocation round-trip); pass
/// [`Caveats::default`] for a non-expiring attestation. `nonce` names it for on-chain revocation.
///
/// Returns the single-link chain; encode it with [`encode_attestation`] for the host to serve.
pub fn mint_stable_attestation(
    root: &Identity,
    host_id: NodeId,
    caveats: Caveats,
    nonce: u64,
) -> SignedCapability {
    SignedCapability::issue(
        root,
        host_id,
        vec![ABILITY_STABLE.to_string()],
        Resource::Node(host_id),
        caveats,
        nonce,
        None,
    )
}

/// Encode a stable attestation to the portable hex token the host serves and stores.
pub fn encode_attestation(attestation: &SignedCapability) -> String {
    encode_chain(std::slice::from_ref(attestation))
}

/// Verify, **offline**, that `token` is a valid stable attestation for `host_id`, rooted at the
/// pinned org root `org_root`.
///
/// This is a thin, exact wrapper over `ce_cap::authorize` — it does NOT re-implement verification.
/// The attestation qualifies iff `authorize` accepts the decoded chain for:
/// - `self_id`        = `host_id` (the node being vetted; the attestation's `Resource::Node` must match it),
/// - `accepted_roots` = `[org_root]` (the ONLY accepted authority — the host's own id is not enough),
/// - `requester`      = `host_id` (the attestation is audience-bound to the host that holds it),
/// - `action`         = [`ABILITY_STABLE`].
///
/// `now` is unix seconds (enforces the attestation's expiry); `is_revoked` consults the on-chain
/// revocation set (pass a closure returning `false` for a pure offline check).
///
/// Returns `Ok(())` if the host is attested-stable, or `Err(reason)` otherwise. A self-claimed
/// `stable` atlas tag never reaches this function; an attestation signed by a non-root key fails the
/// root check; an attestation for a different NodeId fails the resource/audience check.
pub fn verify_stable(
    token: &str,
    host_id: &NodeId,
    org_root: &NodeId,
    now: u64,
    is_revoked: &dyn Fn(&NodeId, u64) -> bool,
) -> Result<()> {
    let chain = decode_chain(token)?;
    // SECURITY: `ce_cap::authorize` treats `self_id` (which we set to `host_id` so the attestation's
    // `Resource::Node(host_id)` matches) as an *implicit accepted root*. That implicit-self rule is
    // correct for "a node delegates its OWN resources", but here it would let a host self-mint its own
    // `stable` attestation (issuer == host == implicit root) and pass. A stable attestation must come
    // from the ORG ROOT, never the host itself — so we require the chain's root issuer to be exactly
    // the pinned org root *before* authorizing. This closes the implicit-self-root hole.
    let root_issuer = chain
        .first()
        .map(|l| l.cap.issuer)
        .ok_or_else(|| anyhow::anyhow!("empty attestation chain"))?;
    if &root_issuer != org_root {
        return Err(anyhow::anyhow!(
            "stable attestation does not root at the pinned org root"
        ));
    }
    authorize(
        host_id,
        std::slice::from_ref(org_root),
        // The attestation binds a NodeId, not a tag-set; no self-tags participate in verification.
        &[],
        now,
        host_id,
        ABILITY_STABLE,
        &chain,
        is_revoked,
    )
    .map_err(|e| anyhow::anyhow!("not a valid org-root stable attestation: {e}"))
}

/// Convenience: is `token` a valid stable attestation for `host_id` rooted at `org_root` at `now`?
/// Offline (never-revoked) boolean form used by placement to build the verified-stable set.
pub fn is_stable(token: &str, host_id: &NodeId, org_root: &NodeId, now: u64) -> bool {
    verify_stable(token, host_id, org_root, now, &|_, _| false).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn id(tag: &str) -> Identity {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ce-gke-stable-{}-{n}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Identity::load_or_generate(&dir).unwrap()
    }

    #[test]
    fn valid_org_root_attestation_verifies() {
        let root = id("root");
        let host = id("host");
        let att = mint_stable_attestation(&root, host.node_id(), Caveats::default(), 1);
        let tok = encode_attestation(&att);
        assert!(verify_stable(&tok, &host.node_id(), &root.node_id(), 1000, &|_, _| false).is_ok());
        assert!(is_stable(&tok, &host.node_id(), &root.node_id(), 1000));
    }

    #[test]
    fn attestation_signed_by_non_root_is_rejected() {
        // A different key signs an otherwise-well-formed "stable" attestation for the host.
        let real_root = id("real-root");
        let impostor = id("impostor");
        let host = id("host");
        let att = mint_stable_attestation(&impostor, host.node_id(), Caveats::default(), 1);
        let tok = encode_attestation(&att);
        // Verified against the REAL pinned root → rejected because the chain's root issuer is not it.
        let err = verify_stable(&tok, &host.node_id(), &real_root.node_id(), 1000, &|_, _| false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("root at the pinned org root"), "got: {err}");
        assert!(!is_stable(&tok, &host.node_id(), &real_root.node_id(), 1000));
    }

    #[test]
    fn attestation_for_a_different_node_does_not_vet_this_host() {
        let root = id("root");
        let other = id("other");
        let host = id("host");
        // Root attests `other`, not `host`.
        let att = mint_stable_attestation(&root, other.node_id(), Caveats::default(), 1);
        let tok = encode_attestation(&att);
        // Presented as `host`'s attestation → the audience/resource bind to `other`, so it fails.
        assert!(verify_stable(&tok, &host.node_id(), &root.node_id(), 1000, &|_, _| false).is_err());
        assert!(!is_stable(&tok, &host.node_id(), &root.node_id(), 1000));
    }

    #[test]
    fn self_minted_attestation_is_not_root_signed() {
        // The host mints its OWN "stable" cap (the self-claimed-tag attack, in cap form). Verified
        // against the org root, it does not root at an accepted authority.
        let root = id("root");
        let host = id("host");
        let self_att = mint_stable_attestation(&host, host.node_id(), Caveats::default(), 1);
        let tok = encode_attestation(&self_att);
        assert!(!is_stable(&tok, &host.node_id(), &root.node_id(), 1000));
    }

    #[test]
    fn expired_attestation_is_rejected() {
        let root = id("root");
        let host = id("host");
        let caveats = Caveats { not_after: 500, ..Default::default() };
        let att = mint_stable_attestation(&root, host.node_id(), caveats, 1);
        let tok = encode_attestation(&att);
        // now=1000 > not_after=500 → expired.
        assert!(!is_stable(&tok, &host.node_id(), &root.node_id(), 1000));
        // before expiry → valid.
        assert!(is_stable(&tok, &host.node_id(), &root.node_id(), 400));
    }

    #[test]
    fn revoked_attestation_is_rejected() {
        let root = id("root");
        let host = id("host");
        let att = mint_stable_attestation(&root, host.node_id(), Caveats::default(), 42);
        let tok = encode_attestation(&att);
        let revoke_42 = |_: &NodeId, nonce: u64| nonce == 42;
        assert!(verify_stable(&tok, &host.node_id(), &root.node_id(), 1000, &revoke_42).is_err());
    }

    #[test]
    fn malformed_token_does_not_panic() {
        let root = id("root");
        let host = id("host");
        assert!(!is_stable("not-hex-!!!", &host.node_id(), &root.node_id(), 0));
        assert!(!is_stable("", &host.node_id(), &root.node_id(), 0));
        assert!(!is_stable("deadbeef", &host.node_id(), &root.node_id(), 0));
    }
}
