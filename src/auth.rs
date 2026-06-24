//! Authorization — the capability chain that lets the orchestrator deploy on a host.
//!
//! A deploy on a remote host is gated by `ce-cap`: the host honors a signed, attenuating chain
//! rooted at a key it accepts (its own, or a configured org root). The orchestrator holds the leaf
//! capability and forwards the chain's hex token as the `grant` on every `mesh-deploy`/`mesh-kill`.
//!
//! This module is thin glue over the [`ce_cap`] primitive: it does **not** re-implement
//! verification (that lives in the node / `ce-cap::authorize`). It provides the action vocabulary
//! ce-gke uses, a builder for the operator-facing "let this orchestrator deploy on my host" grant,
//! and a local pre-flight check so the CLI can reject an obviously-wrong token before hitting the
//! mesh. The host remains the real enforcement point.

use anyhow::Result;
use ce_cap::{authorize, decode_chain, Caveats, Resource, SignedCapability};
use ce_identity::{Identity, NodeId};

/// The action a deploy requires on the host. Opaque string in ce-cap; ce-gke owns this vocabulary.
pub const ACTION_DEPLOY: &str = "deploy";
/// The action a kill requires.
pub const ACTION_KILL: &str = "kill";
/// The action a health probe requires (the host-side `ce-gke serve` agent verifies it).
pub const ACTION_PROBE: &str = "probe";

/// Issue a root grant from `host` (the resource owner) to `orchestrator`, authorizing it to deploy
/// and kill on this host. This is what a host operator runs to onboard an orchestrator (the
/// `ce grant <orchestrator> --can deploy,kill` equivalent). Returns the single-link chain.
///
/// `caveats` bound the grant (expiry, max cpu/mem/credits); pass [`Caveats::default`] for an
/// unbounded self-grant. `nonce` names it for revocation.
pub fn issue_host_grant(
    host: &Identity,
    orchestrator: NodeId,
    resource: Resource,
    caveats: Caveats,
    nonce: u64,
) -> SignedCapability {
    SignedCapability::issue(
        host,
        orchestrator,
        vec![ACTION_DEPLOY.to_string(), ACTION_KILL.to_string(), ACTION_PROBE.to_string()],
        resource,
        caveats,
        nonce,
        None,
    )
}

/// Decode a grant token (hex chain) and locally pre-verify it authorizes `requester` to `action`
/// on a host with the given `(self_id, self_tags)`, rooted at one of `accepted_roots` (the host's
/// own id is always an implicit root). Mirrors what the host will check — used by the CLI to fail
/// fast on a bad/expired/over-broad token. Returns `Ok(())` if it would be accepted.
///
/// `now` is unix seconds; `is_revoked` consults the on-chain revocation set (pass a closure that
/// returns `false` for an offline check).
#[allow(clippy::too_many_arguments)]
pub fn preflight(
    token: &str,
    self_id: &NodeId,
    accepted_roots: &[NodeId],
    self_tags: &[String],
    now: u64,
    requester: &NodeId,
    action: &str,
    is_revoked: &dyn Fn(&NodeId, u64) -> bool,
) -> Result<()> {
    let chain = decode_chain(token)?;
    authorize(self_id, accepted_roots, self_tags, now, requester, action, &chain, is_revoked)
        .map_err(|e| anyhow::anyhow!("capability would be rejected by the host: {e}"))
}

/// Encode a chain back to the portable hex token carried as the `grant`.
pub fn token(chain: &[SignedCapability]) -> String {
    ce_cap::encode_chain(chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_cap::encode_chain;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn id(tag: &str) -> Identity {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ce-gke-auth-{}-{n}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Identity::load_or_generate(&dir).unwrap()
    }

    fn never(_: &NodeId, _: u64) -> bool {
        false
    }

    #[test]
    fn host_grant_authorizes_deploy_and_kill() {
        let host = id("host");
        let orch = id("orch");
        let cap = issue_host_grant(&host, orch.node_id(), Resource::Any, Caveats::default(), 1);
        let tok = encode_chain(&[cap]);
        // deploy ok
        assert!(preflight(&tok, &host.node_id(), &[], &[], 1000, &orch.node_id(), ACTION_DEPLOY, &never).is_ok());
        // kill ok
        assert!(preflight(&tok, &host.node_id(), &[], &[], 1000, &orch.node_id(), ACTION_KILL, &never).is_ok());
    }

    #[test]
    fn preflight_rejects_unrelated_action() {
        let host = id("host");
        let orch = id("orch");
        let cap = issue_host_grant(&host, orch.node_id(), Resource::Any, Caveats::default(), 1);
        let tok = encode_chain(&[cap]);
        assert!(preflight(&tok, &host.node_id(), &[], &[], 1000, &orch.node_id(), "tunnel", &never).is_err());
    }

    #[test]
    fn preflight_rejects_wrong_requester() {
        let host = id("host");
        let orch = id("orch");
        let stranger = id("stranger");
        let cap = issue_host_grant(&host, orch.node_id(), Resource::Any, Caveats::default(), 1);
        let tok = encode_chain(&[cap]);
        assert!(preflight(&tok, &host.node_id(), &[], &[], 1000, &stranger.node_id(), ACTION_DEPLOY, &never).is_err());
    }

    #[test]
    fn preflight_rejects_expired() {
        let host = id("host");
        let orch = id("orch");
        let caveats = Caveats { not_after: 500, ..Default::default() };
        let cap = issue_host_grant(&host, orch.node_id(), Resource::Any, caveats, 1);
        let tok = encode_chain(&[cap]);
        // now=1000 > not_after=500
        assert!(preflight(&tok, &host.node_id(), &[], &[], 1000, &orch.node_id(), ACTION_DEPLOY, &never).is_err());
        // before expiry ok
        assert!(preflight(&tok, &host.node_id(), &[], &[], 100, &orch.node_id(), ACTION_DEPLOY, &never).is_ok());
    }

    #[test]
    fn preflight_rejects_revoked() {
        let host = id("host");
        let orch = id("orch");
        let cap = issue_host_grant(&host, orch.node_id(), Resource::Any, Caveats::default(), 42);
        let tok = encode_chain(&[cap]);
        let revoke_42 = |_: &NodeId, nonce: u64| nonce == 42;
        assert!(preflight(&tok, &host.node_id(), &[], &[], 1000, &orch.node_id(), ACTION_DEPLOY, &revoke_42).is_err());
    }

    #[test]
    fn preflight_rejects_resource_mismatch() {
        let host = id("host");
        let orch = id("orch");
        // grant scoped to gpu hosts only
        let cap = issue_host_grant(&host, orch.node_id(), Resource::Tag("gpu".into()), Caveats::default(), 1);
        let tok = encode_chain(&[cap]);
        // host without gpu tag → rejected
        assert!(preflight(&tok, &host.node_id(), &[], &["linux".into()], 1000, &orch.node_id(), ACTION_DEPLOY, &never).is_err());
        // host with gpu tag → ok
        assert!(preflight(&tok, &host.node_id(), &[], &["gpu".into()], 1000, &orch.node_id(), ACTION_DEPLOY, &never).is_ok());
    }

    #[test]
    fn malformed_token_does_not_panic() {
        let host = id("host");
        let orch = id("orch");
        assert!(preflight("not-hex-!!!", &host.node_id(), &[], &[], 0, &orch.node_id(), ACTION_DEPLOY, &never).is_err());
        assert!(preflight("", &host.node_id(), &[], &[], 0, &orch.node_id(), ACTION_DEPLOY, &never).is_err());
        // valid hex but not a chain
        assert!(preflight("deadbeef", &host.node_id(), &[], &[], 0, &orch.node_id(), ACTION_DEPLOY, &never).is_err());
    }

    #[test]
    fn token_roundtrips() {
        let host = id("host");
        let orch = id("orch");
        let cap = issue_host_grant(&host, orch.node_id(), Resource::Any, Caveats::default(), 1);
        let tok = token(std::slice::from_ref(&cap));
        let back = decode_chain(&tok).unwrap();
        assert_eq!(back.len(), 1);
        assert!(back[0].verify_sig().is_ok());
    }
}
