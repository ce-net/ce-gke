//! Wire protocol between the orchestrator and the host-side `ce-gke serve` health agent.
//!
//! CE itself only reports a coarse job status (`pending`/`running`/`failed:*`). To know whether a
//! replica is *actually* healthy — serving requests, not deadlocked — the orchestrator asks the host
//! that runs the cell. That host runs `ce-gke serve`, which answers [`ProbeRequest`]s over the mesh
//! `request`/`reply` primitive on the [`PROBE_TOPIC`] topic, authenticated by the caller's NodeId and
//! authorized by the forwarded ce-cap grant (ability [`crate::auth::ACTION_PROBE`]).
//!
//! This is an **app protocol over CE primitives** — no node endpoint, no stored ip:port. The bytes
//! are bincode (deterministic, per project standards); requests are bounded in size on both encode
//! and decode so a malformed or hostile payload cannot exhaust memory.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::reconcile::Phase;

/// The mesh request/reply topic the health agent answers on.
pub const PROBE_TOPIC: &str = "ce-gke/probe/1";

/// Hard cap on an encoded probe request or reply (defense against a hostile/huge payload). Probe
/// messages are tiny; 64 KiB is generous and bounds memory on both ends.
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024;

/// The orchestrator asks the host whether a job it placed is healthy, optionally running a probe
/// command inside the cell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeRequest {
    /// Job id (the cell handle on the host).
    pub job_id: String,
    /// Probe command to run inside the cell (exit 0 = healthy). Empty = report the host job status
    /// only (no in-cell exec), which is still strictly better than the payer's local view.
    #[serde(default)]
    pub check_command: Vec<String>,
    /// Forwarded ce-cap grant token (hex chain) authorizing the probe on this host. The agent
    /// verifies it before answering.
    #[serde(default)]
    pub grant: Option<String>,
}

impl ProbeRequest {
    /// Encode to bounded bincode bytes.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        if bytes.len() > MAX_MESSAGE_BYTES {
            bail!("probe request too large ({} bytes)", bytes.len());
        }
        Ok(bytes)
    }

    /// Decode bounded bincode bytes (rejects oversized input before allocating).
    pub fn decode(bytes: &[u8]) -> Result<ProbeRequest> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            bail!("probe request exceeds {MAX_MESSAGE_BYTES} bytes");
        }
        Ok(bincode::deserialize(bytes)?)
    }
}

/// The host's answer: the job's phase as the host itself sees it, plus the probe outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeReply {
    /// The job's phase from the host's own job table (authoritative, unlike the payer's cache).
    pub phase: Phase,
    /// `Some(true)` if the probe command exited 0, `Some(false)` if it failed, `None` if no probe
    /// was requested or the host could not run one.
    #[serde(default)]
    pub probe_passed: Option<bool>,
    /// Optional human-readable detail (probe stderr tail, host note) for diagnostics.
    #[serde(default)]
    pub detail: String,
}

impl ProbeReply {
    /// An error reply carrying a Failed phase and a reason (so the caller never deserializes garbage).
    pub fn failed(reason: impl Into<String>) -> ProbeReply {
        ProbeReply { phase: Phase::Failed, probe_passed: Some(false), detail: reason.into() }
    }

    /// Encode to bounded bincode bytes.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        if bytes.len() > MAX_MESSAGE_BYTES {
            bail!("probe reply too large ({} bytes)", bytes.len());
        }
        Ok(bytes)
    }

    /// Decode bounded bincode bytes.
    pub fn decode(bytes: &[u8]) -> Result<ProbeReply> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            bail!("probe reply exceeds {MAX_MESSAGE_BYTES} bytes");
        }
        Ok(bincode::deserialize(bytes)?)
    }

    /// Resolve the effective phase: if a probe ran and failed, the replica is Failed regardless of
    /// the host's coarse status (the workload is up but unhealthy — the readiness/liveness point).
    pub fn effective_phase(&self) -> Phase {
        match self.probe_passed {
            Some(false) => Phase::Failed,
            _ => self.phase,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let req = ProbeRequest {
            job_id: "job-1".into(),
            check_command: vec!["true".into()],
            grant: Some("deadbeef".into()),
        };
        let bytes = req.encode().unwrap();
        assert_eq!(ProbeRequest::decode(&bytes).unwrap(), req);
    }

    #[test]
    fn reply_roundtrip() {
        let r = ProbeReply { phase: Phase::Running, probe_passed: Some(true), detail: "ok".into() };
        let bytes = r.encode().unwrap();
        assert_eq!(ProbeReply::decode(&bytes).unwrap(), r);
    }

    #[test]
    fn oversized_decode_rejected() {
        let big = vec![0u8; MAX_MESSAGE_BYTES + 1];
        assert!(ProbeRequest::decode(&big).is_err());
        assert!(ProbeReply::decode(&big).is_err());
    }

    #[test]
    fn effective_phase_failed_probe_overrides_running() {
        let r = ProbeReply { phase: Phase::Running, probe_passed: Some(false), detail: String::new() };
        assert_eq!(r.effective_phase(), Phase::Failed);
        let r = ProbeReply { phase: Phase::Running, probe_passed: Some(true), detail: String::new() };
        assert_eq!(r.effective_phase(), Phase::Running);
        let r = ProbeReply { phase: Phase::Pending, probe_passed: None, detail: String::new() };
        assert_eq!(r.effective_phase(), Phase::Pending);
    }

    #[test]
    fn failed_reply_helper() {
        let r = ProbeReply::failed("boom");
        assert_eq!(r.phase, Phase::Failed);
        assert_eq!(r.probe_passed, Some(false));
        assert_eq!(r.detail, "boom");
    }

    #[test]
    fn malformed_decode_does_not_panic() {
        assert!(ProbeRequest::decode(&[0xff, 0xff, 0xff]).is_err());
        assert!(ProbeReply::decode(&[0xff, 0xff, 0xff]).is_err());
    }
}
