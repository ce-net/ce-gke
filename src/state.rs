//! Local state — the orchestrator's record of what it manages, persisted between CLI invocations.
//!
//! GKE keeps desired state + replica handles in etcd. ce-gke is a thin client with no control
//! plane, so it persists the same two things to a small JSON file under the CE data dir: the
//! [`Deployment`] specs the operator has applied, and the [`ReplicaState`] handles the controller
//! launched. Each `ce-gke` command loads this, runs reconcile ticks, and saves it back. This keeps
//! the orchestrator stateful (handles survive across `apply`/`scale`/`rollout`) without a server.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::reconcile::ReplicaState;
use crate::spec::Deployment;

/// One managed deployment: its desired spec plus the replica handles the controller is tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManagedDeployment {
    pub spec: Deployment,
    #[serde(default)]
    pub replicas: Vec<ReplicaState>,
    /// Capability grant token forwarded to hosts for this deployment's deploys/kills.
    #[serde(default)]
    pub grant: Option<String>,
}

/// The whole persisted store: name -> managed deployment.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Store {
    #[serde(default)]
    pub deployments: BTreeMap<String, ManagedDeployment>,
}

impl Store {
    /// The default state file path: `<data_dir>/ce/gke-state.json`.
    pub fn default_path() -> Result<PathBuf> {
        let dir = directories::ProjectDirs::from("", "", "ce")
            .context("could not determine the CE data directory")?
            .data_dir()
            .to_path_buf();
        Ok(dir.join("gke-state.json"))
    }

    /// Load the store from `path`, or an empty store if it does not exist. A corrupt file is an
    /// error (we never silently discard managed state).
    pub fn load(path: &Path) -> Result<Store> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("state file {} is corrupt", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Store::default()),
            Err(e) => Err(e).with_context(|| format!("reading state file {}", path.display())),
        }
    }

    /// Persist the store to `path`, creating parent dirs. Writes atomically (temp + rename) so a
    /// crash mid-write never corrupts the existing state.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        std::fs::write(&tmp, &bytes).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
        Ok(())
    }

    /// Insert or replace a deployment's spec, preserving existing replica handles + grant if the
    /// deployment already exists (an `apply` updates the spec but keeps tracking live replicas).
    pub fn upsert(&mut self, spec: Deployment, grant: Option<String>) {
        let entry = self.deployments.entry(spec.name.clone()).or_default();
        entry.spec = spec;
        if grant.is_some() {
            entry.grant = grant;
        }
    }

    /// Look up a managed deployment by name.
    pub fn get(&self, name: &str) -> Option<&ManagedDeployment> {
        self.deployments.get(name)
    }

    /// Mutable lookup.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut ManagedDeployment> {
        self.deployments.get_mut(name)
    }

    /// Remove a deployment, returning it (so the caller can kill its replicas).
    pub fn remove(&mut self, name: &str) -> Option<ManagedDeployment> {
        self.deployments.remove(name)
    }

    /// Names of all managed deployments, sorted.
    pub fn names(&self) -> Vec<String> {
        self.deployments.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_rs::Amount;

    fn deploy(name: &str, replicas: u32) -> Deployment {
        Deployment {
            name: name.into(),
            image: "nginx".into(),
            command: vec![],
            replicas,
            resources: Default::default(),
            select: vec![],
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Default::default(),
        }
    }

    #[test]
    fn upsert_and_get() {
        let mut s = Store::default();
        s.upsert(deploy("web", 3), Some("tok".into()));
        assert_eq!(s.get("web").unwrap().spec.replicas, 3);
        assert_eq!(s.get("web").unwrap().grant.as_deref(), Some("tok"));
    }

    #[test]
    fn upsert_preserves_replicas_and_grant() {
        let mut s = Store::default();
        s.upsert(deploy("web", 3), Some("tok".into()));
        // pretend the controller launched a replica
        s.get_mut("web").unwrap().replicas.push(ReplicaState {
            job_id: "j1".into(),
            node_id: "a".into(),
            revision: "r".into(),
            phase: crate::reconcile::Phase::Running,
        });
        // apply a new spec without a grant → replicas + grant preserved
        s.upsert(deploy("web", 5), None);
        assert_eq!(s.get("web").unwrap().spec.replicas, 5);
        assert_eq!(s.get("web").unwrap().replicas.len(), 1);
        assert_eq!(s.get("web").unwrap().grant.as_deref(), Some("tok"));
    }

    #[test]
    fn remove_returns_managed() {
        let mut s = Store::default();
        s.upsert(deploy("web", 1), None);
        let removed = s.remove("web").unwrap();
        assert_eq!(removed.spec.name, "web");
        assert!(s.get("web").is_none());
    }

    #[test]
    fn names_are_sorted() {
        let mut s = Store::default();
        s.upsert(deploy("zebra", 1), None);
        s.upsert(deploy("apple", 1), None);
        assert_eq!(s.names(), vec!["apple", "zebra"]);
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("ce-gke-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");
        let mut s = Store::default();
        s.upsert(deploy("web", 2), Some("g".into()));
        s.save(&path).unwrap();
        let back = Store::load(&path).unwrap();
        assert_eq!(back.get("web").unwrap().spec.replicas, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_is_empty() {
        let path = std::env::temp_dir().join("ce-gke-definitely-missing-xyz.json");
        let _ = std::fs::remove_file(&path);
        let s = Store::load(&path).unwrap();
        assert!(s.names().is_empty());
    }

    #[test]
    fn load_corrupt_is_error() {
        let dir = std::env::temp_dir().join(format!("ce-gke-corrupt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.json");
        std::fs::write(&path, b"{ not json").unwrap();
        assert!(Store::load(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
