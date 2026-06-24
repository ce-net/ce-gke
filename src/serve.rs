//! The host-side health agent (`ce-gke serve`).
//!
//! A host that runs replicas for an orchestrator runs this agent so the orchestrator can ask for the
//! *authoritative* phase of a job (the host's own view, not the payer's cache) over the mesh. The
//! agent answers [`ProbeRequest`]s on [`PROBE_TOPIC`], after verifying the forwarded ce-cap grant
//! authorizes the caller to [`ACTION_PROBE`] on this host.
//!
//! The agent is intentionally a thin, well-bounded request handler: it validates the request,
//! enforces the capability, looks up the job, and replies. The job lookup is abstracted behind
//! [`JobSource`] so the request-handling logic — including the failure paths — is unit-testable
//! without a live node.
//!
//! ## What a probe actually checks
//!
//! CE exposes no command-exec endpoint to apps (exec is the `rdev` app's domain, not a node RPC), so
//! this agent reports the job's phase from the host job table and sets `probe_passed = None` for
//! exec/http/tcp probe commands it cannot run in-process. That is still strictly more accurate than
//! the payer's cached view, and the wire protocol already carries the probe command so a host that
//! *does* run a container-exec sidecar can fill in `probe_passed` without any protocol change. This
//! is the documented "real slice now, deeper exec deferred" boundary.

use anyhow::Result;
use ce_cap::{authorize, decode_chain};
use ce_identity::NodeId;

use crate::auth::ACTION_PROBE;
use crate::protocol::{ProbeReply, ProbeRequest};
use crate::reconcile::Phase;

/// A source of job phase information (the host's own job table). Abstracted for testing.
pub trait JobSource {
    /// The phase the host believes `job_id` is in, or `None` if it does not know the job.
    fn job_phase(&self, job_id: &str) -> Option<Phase>;
}

/// Everything the agent needs to authorize a probe request, independent of transport.
pub struct ProbeAuthority<'a> {
    /// This host's NodeId (always an implicit accepted root).
    pub self_id: NodeId,
    /// Additional accepted root keys (e.g. an org root).
    pub accepted_roots: &'a [NodeId],
    /// This host's capability self-tags (for resource-scoped grants).
    pub self_tags: &'a [String],
    /// Current unix time (for expiry checks).
    pub now: u64,
    /// On-chain revocation check.
    pub is_revoked: &'a dyn Fn(&NodeId, u64) -> bool,
    /// When true, a request with no grant is rejected (closed by default for an open mesh).
    pub require_grant: bool,
}

/// Handle one decoded probe request: authorize, look up the job, and produce a reply. Pure given the
/// `requester`, the authority, and a [`JobSource`]; the transport (decode bytes / send reply) is the
/// caller's job. Never panics; every error path yields a [`ProbeReply::failed`].
pub fn handle_probe<S: JobSource>(
    requester: &NodeId,
    req: &ProbeRequest,
    authority: &ProbeAuthority<'_>,
    jobs: &S,
) -> ProbeReply {
    // 1. Authorization.
    match &req.grant {
        Some(token) => match decode_chain(token) {
            Ok(chain) => {
                if let Err(e) = authorize(
                    &authority.self_id,
                    authority.accepted_roots,
                    authority.self_tags,
                    authority.now,
                    requester,
                    ACTION_PROBE,
                    &chain,
                    authority.is_revoked,
                ) {
                    return ProbeReply::failed(format!("unauthorized: {e}"));
                }
            }
            Err(e) => return ProbeReply::failed(format!("malformed grant: {e}")),
        },
        None => {
            if authority.require_grant {
                return ProbeReply::failed("probe requires a capability grant");
            }
        }
    }

    // 2. Job lookup.
    if req.job_id.is_empty() || req.job_id.len() > 256 {
        return ProbeReply::failed("invalid job id");
    }
    match jobs.job_phase(&req.job_id) {
        Some(phase) => ProbeReply {
            phase,
            // We cannot exec a probe command in-process; report status only. A host with a
            // container-exec sidecar fills this in.
            probe_passed: None,
            detail: String::new(),
        },
        // The host does not know this job → it is gone from this host's perspective → Failed, so the
        // orchestrator reschedules it. This is the precise robustness win over the payer-local view.
        None => ProbeReply {
            phase: Phase::Failed,
            probe_passed: None,
            detail: "host does not know this job".into(),
        },
    }
}

/// Decode raw request bytes from a mesh peer, handle them, and return encoded reply bytes. This is
/// the one-call seam a transport loop uses: it bounds and validates the payload, so a malformed or
/// oversized request can never crash the agent.
pub fn handle_probe_bytes<S: JobSource>(
    requester: &NodeId,
    payload: &[u8],
    authority: &ProbeAuthority<'_>,
    jobs: &S,
) -> Result<Vec<u8>> {
    let reply = match ProbeRequest::decode(payload) {
        Ok(req) => handle_probe(requester, &req, authority, jobs),
        Err(e) => ProbeReply::failed(format!("bad request: {e}")),
    };
    reply.encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_cap::{encode_chain, Caveats, Resource};
    use ce_identity::Identity;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct MapJobs(HashMap<String, Phase>);
    impl JobSource for MapJobs {
        fn job_phase(&self, job_id: &str) -> Option<Phase> {
            self.0.get(job_id).copied()
        }
    }

    fn id(tag: &str) -> Identity {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ce-gke-serve-{}-{n}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Identity::load_or_generate(&dir).unwrap()
    }

    fn never(_: &NodeId, _: u64) -> bool {
        false
    }

    fn authority<'a>(
        host: &Identity,
        now: u64,
        require_grant: bool,
        revoked: &'a dyn Fn(&NodeId, u64) -> bool,
    ) -> ProbeAuthority<'a> {
        ProbeAuthority {
            self_id: host.node_id(),
            accepted_roots: &[],
            self_tags: &[],
            now,
            is_revoked: revoked,
            require_grant,
        }
    }

    fn grant_token(host: &Identity, orch: &Identity) -> String {
        let cap = crate::auth::issue_host_grant(
            host,
            orch.node_id(),
            Resource::Any,
            Caveats::default(),
            1,
        );
        encode_chain(&[cap])
    }

    #[test]
    fn authorized_probe_reports_phase() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::from([("job-1".to_string(), Phase::Running)]));
        let req = ProbeRequest {
            job_id: "job-1".into(),
            check_command: vec![],
            grant: Some(grant_token(&host, &orch)),
        };
        let auth = authority(&host, 1000, true, &never);
        let reply = handle_probe(&orch.node_id(), &req, &auth, &jobs);
        assert_eq!(reply.phase, Phase::Running);
    }

    #[test]
    fn unknown_job_is_failed() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::new());
        let req = ProbeRequest {
            job_id: "missing".into(),
            check_command: vec![],
            grant: Some(grant_token(&host, &orch)),
        };
        let auth = authority(&host, 1000, true, &never);
        let reply = handle_probe(&orch.node_id(), &req, &auth, &jobs);
        assert_eq!(reply.phase, Phase::Failed);
    }

    #[test]
    fn missing_grant_rejected_when_required() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::from([("job-1".to_string(), Phase::Running)]));
        let req = ProbeRequest { job_id: "job-1".into(), check_command: vec![], grant: None };
        let auth = authority(&host, 1000, true, &never);
        let reply = handle_probe(&orch.node_id(), &req, &auth, &jobs);
        assert_eq!(reply.phase, Phase::Failed);
        assert!(reply.detail.contains("requires a capability"));
    }

    #[test]
    fn missing_grant_allowed_when_open() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::from([("job-1".to_string(), Phase::Running)]));
        let req = ProbeRequest { job_id: "job-1".into(), check_command: vec![], grant: None };
        let auth = authority(&host, 1000, false, &never);
        let reply = handle_probe(&orch.node_id(), &req, &auth, &jobs);
        assert_eq!(reply.phase, Phase::Running);
    }

    #[test]
    fn wrong_requester_rejected() {
        let host = id("host");
        let orch = id("orch");
        let stranger = id("stranger");
        let jobs = MapJobs(HashMap::from([("job-1".to_string(), Phase::Running)]));
        let req = ProbeRequest {
            job_id: "job-1".into(),
            check_command: vec![],
            grant: Some(grant_token(&host, &orch)),
        };
        let auth = authority(&host, 1000, true, &never);
        // stranger presents orch's grant → unauthorized
        let reply = handle_probe(&stranger.node_id(), &req, &auth, &jobs);
        assert_eq!(reply.phase, Phase::Failed);
        assert!(reply.detail.contains("unauthorized"));
    }

    #[test]
    fn malformed_grant_rejected() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::new());
        let req = ProbeRequest {
            job_id: "job-1".into(),
            check_command: vec![],
            grant: Some("not-a-chain".into()),
        };
        let auth = authority(&host, 1000, true, &never);
        let reply = handle_probe(&orch.node_id(), &req, &auth, &jobs);
        assert!(reply.detail.contains("malformed grant"));
    }

    #[test]
    fn revoked_grant_rejected() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::from([("job-1".to_string(), Phase::Running)]));
        let cap = crate::auth::issue_host_grant(
            &host,
            orch.node_id(),
            Resource::Any,
            Caveats::default(),
            42,
        );
        let token = encode_chain(&[cap]);
        let req = ProbeRequest { job_id: "job-1".into(), check_command: vec![], grant: Some(token) };
        let revoked = |_: &NodeId, nonce: u64| nonce == 42;
        let auth = authority(&host, 1000, true, &revoked);
        let reply = handle_probe(&orch.node_id(), &req, &auth, &jobs);
        assert_eq!(reply.phase, Phase::Failed);
    }

    #[test]
    fn handle_bytes_roundtrips_and_bounds() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::from([("job-1".to_string(), Phase::Pending)]));
        let req = ProbeRequest {
            job_id: "job-1".into(),
            check_command: vec![],
            grant: Some(grant_token(&host, &orch)),
        };
        let bytes = req.encode().unwrap();
        let auth = authority(&host, 1000, true, &never);
        let reply_bytes = handle_probe_bytes(&orch.node_id(), &bytes, &auth, &jobs).unwrap();
        let reply = ProbeReply::decode(&reply_bytes).unwrap();
        assert_eq!(reply.phase, Phase::Pending);

        // Malformed bytes → a failed reply, not a panic/error.
        let reply_bytes = handle_probe_bytes(&orch.node_id(), &[0xff, 0x00], &auth, &jobs).unwrap();
        let reply = ProbeReply::decode(&reply_bytes).unwrap();
        assert_eq!(reply.phase, Phase::Failed);
    }

    #[test]
    fn empty_job_id_rejected() {
        let host = id("host");
        let orch = id("orch");
        let jobs = MapJobs(HashMap::new());
        let req = ProbeRequest {
            job_id: String::new(),
            check_command: vec![],
            grant: Some(grant_token(&host, &orch)),
        };
        let auth = authority(&host, 1000, true, &never);
        let reply = handle_probe(&orch.node_id(), &req, &auth, &jobs);
        assert!(reply.detail.contains("invalid job id"));
    }
}
