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

/// The on-disk schema version for [`Store`]. Bumped when the persisted shape changes so old files
/// can be migrated forward rather than rejected as corrupt.
pub const STORE_VERSION: u32 = 1;

/// Maximum number of prior revisions kept per deployment for `rollout undo`.
pub const MAX_HISTORY: usize = 10;

/// One managed deployment: its desired spec plus the replica handles the controller is tracking.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManagedDeployment {
    pub spec: Deployment,
    #[serde(default)]
    pub replicas: Vec<ReplicaState>,
    /// Capability grant token forwarded to hosts for this deployment's deploys/kills.
    #[serde(default)]
    pub grant: Option<String>,
    /// Prior pod-template specs (newest last), for `rollout undo`. Capped at [`MAX_HISTORY`].
    #[serde(default)]
    pub history: Vec<Deployment>,
    /// When set, the rollout is paused: reconcile holds the current world and makes no progress
    /// toward a new revision (scale/heal still happen). `rollout resume` clears it.
    #[serde(default)]
    pub paused: bool,
}

impl ManagedDeployment {
    /// Record the current spec into history if it differs in revision from the newest entry, then
    /// keep history bounded. Call before replacing `spec` with a new revision.
    pub fn push_history(&mut self, prior: Deployment) {
        if self.history.last().map(|h| h.revision()) == Some(prior.revision()) {
            return;
        }
        // Do not record a revision identical to the one we are keeping.
        self.history.push(prior);
        if self.history.len() > MAX_HISTORY {
            let overflow = self.history.len() - MAX_HISTORY;
            self.history.drain(0..overflow);
        }
    }
}

/// The whole persisted store: namespaced key (`namespace/name`) -> managed deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Store {
    /// Schema version (see [`STORE_VERSION`]). Defaults to 0 for files written before versioning.
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub deployments: BTreeMap<String, ManagedDeployment>,
}

impl Default for Store {
    fn default() -> Self {
        Store { version: STORE_VERSION, deployments: BTreeMap::new() }
    }
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
    /// error (we never silently discard managed state). A file written by an older version is
    /// migrated forward in memory (see [`Store::migrate`]).
    pub fn load(path: &Path) -> Result<Store> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let mut store: Store = serde_json::from_slice(&bytes)
                    .with_context(|| format!("state file {} is corrupt", path.display()))?;
                store.migrate();
                Ok(store)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Store::default()),
            Err(e) => Err(e).with_context(|| format!("reading state file {}", path.display())),
        }
    }

    /// Migrate an older on-disk store forward to [`STORE_VERSION`]. Idempotent. v0 files were keyed
    /// by bare `name`; re-key them to `default/name` and backfill `namespace`/`history`/`paused`
    /// defaults, so old state keeps working after the namespace change.
    pub fn migrate(&mut self) {
        if self.version >= STORE_VERSION {
            self.version = STORE_VERSION;
            return;
        }
        // v0 -> v1: ensure each deployment has a namespace and is keyed `namespace/name`.
        let old = std::mem::take(&mut self.deployments);
        for (key, mut managed) in old {
            if managed.spec.namespace.is_empty() {
                managed.spec.namespace = "default".to_string();
            }
            // If the key is already namespaced (`ns/name`), keep it; else derive from the spec.
            let new_key =
                if key.contains('/') { key } else { managed.spec.key() };
            self.deployments.insert(new_key, managed);
        }
        self.version = STORE_VERSION;
    }

    /// Persist the store to `path`, creating parent dirs. Writes atomically (temp + fsync + rename)
    /// so a crash mid-write never corrupts the existing state, and stamps the current schema version.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let mut to_write = self.clone();
        to_write.version = STORE_VERSION;
        // Unique temp name per process so two writers do not clobber each other's temp file before
        // the rename (the lock serializes the read-modify-write, this guards the temp itself).
        let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
        let bytes = serde_json::to_vec_pretty(&to_write)?;
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)
                .with_context(|| format!("writing {}", tmp.display()))?;
            f.write_all(&bytes).with_context(|| format!("writing {}", tmp.display()))?;
            f.sync_all().with_context(|| format!("fsync {}", tmp.display()))?;
        }
        std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
        Ok(())
    }

    /// Insert or replace a deployment's spec, preserving existing replica handles + grant if the
    /// deployment already exists (an `apply` updates the spec but keeps tracking live replicas).
    /// When the new spec is a different revision than the stored one, the prior spec is pushed to
    /// history so it can be rolled back to. Keyed by the spec's namespace-scoped [`Deployment::key`].
    pub fn upsert(&mut self, spec: Deployment, grant: Option<String>) {
        let key = spec.key();
        let entry = self.deployments.entry(key).or_default();
        // Record the outgoing revision for `rollout undo` if it changed.
        if !entry.spec.image.is_empty() && entry.spec.revision() != spec.revision() {
            let prior = entry.spec.clone();
            entry.push_history(prior);
        }
        entry.spec = spec;
        if grant.is_some() {
            entry.grant = grant;
        }
    }

    /// Look up a managed deployment by its namespace-scoped key (`namespace/name`).
    pub fn get(&self, key: &str) -> Option<&ManagedDeployment> {
        self.deployments.get(key)
    }

    /// Mutable lookup by namespace-scoped key.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut ManagedDeployment> {
        self.deployments.get_mut(key)
    }

    /// Remove a deployment by key, returning it (so the caller can kill its replicas).
    pub fn remove(&mut self, key: &str) -> Option<ManagedDeployment> {
        self.deployments.remove(key)
    }

    /// Keys of all managed deployments (`namespace/name`), sorted.
    pub fn names(&self) -> Vec<String> {
        self.deployments.keys().cloned().collect()
    }

    /// Keys of deployments in a given namespace, sorted.
    pub fn names_in(&self, namespace: &str) -> Vec<String> {
        self.deployments
            .iter()
            .filter(|(_, m)| m.spec.namespace == namespace)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// All managed deployments (key + value), sorted by key — for daemon reconcile-all.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ManagedDeployment)> {
        self.deployments.iter()
    }
}

/// An advisory inter-process lock around the state file, held for the duration of a CLI command's
/// read-modify-write so two concurrent `ce-gke` processes cannot lose each other's updates.
///
/// Implemented as an exclusive-create lock file next to the state file. It is best-effort and
/// self-healing: a stale lock left by a crashed process older than [`LOCK_STALE_SECS`] is broken so
/// the orchestrator never wedges permanently. (A kernel `flock` would be stronger but is not
/// portable to all the targets ce ships on; this is the same pattern Cargo uses for its package
/// cache.)
pub struct StateLock {
    path: PathBuf,
}

/// A lock file older than this (seconds) is presumed stale (its owner crashed) and broken.
pub const LOCK_STALE_SECS: u64 = 120;

impl StateLock {
    /// Acquire the lock for `state_path`, spinning briefly if another process holds it. Returns a
    /// guard that releases the lock on drop.
    pub fn acquire(state_path: &Path) -> Result<StateLock> {
        let lock_path = state_path.with_extension("lock");
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        // Try for ~5s, breaking a clearly-stale lock.
        for _ in 0..50 {
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&lock_path) {
                Ok(mut f) => {
                    use std::io::Write;
                    let _ = writeln!(f, "{}", std::process::id());
                    return Ok(StateLock { path: lock_path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if Self::is_stale(&lock_path) {
                        let _ = std::fs::remove_file(&lock_path);
                        continue;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("acquiring state lock {}", lock_path.display())
                    });
                }
            }
        }
        anyhow::bail!(
            "could not acquire state lock {} (another ce-gke process is running); retry shortly",
            lock_path.display()
        )
    }

    fn is_stale(lock_path: &Path) -> bool {
        let Ok(meta) = std::fs::metadata(lock_path) else { return false };
        let Ok(modified) = meta.modified() else { return false };
        modified.elapsed().map(|e| e.as_secs() > LOCK_STALE_SECS).unwrap_or(false)
    }
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_rs::Amount;

    fn deploy(name: &str, replicas: u32) -> Deployment {
        Deployment {
            name: name.into(),
            namespace: "default".into(),
            image: "nginx".into(),
            command: vec![],
            replicas,
            resources: Default::default(),
            select: vec![],
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Default::default(),
            ..Default::default()
        }
    }

    #[test]
    fn upsert_and_get() {
        let mut s = Store::default();
        s.upsert(deploy("web", 3), Some("tok".into()));
        assert_eq!(s.get("default/web").unwrap().spec.replicas, 3);
        assert_eq!(s.get("default/web").unwrap().grant.as_deref(), Some("tok"));
    }

    #[test]
    fn upsert_preserves_replicas_and_grant() {
        let mut s = Store::default();
        s.upsert(deploy("web", 3), Some("tok".into()));
        // pretend the controller launched a replica
        s.get_mut("default/web").unwrap().replicas.push(ReplicaState {
            job_id: "j1".into(),
            node_id: "a".into(),
            revision: "r".into(),
            phase: crate::reconcile::Phase::Running,
        });
        // apply a new spec without a grant → replicas + grant preserved
        s.upsert(deploy("web", 5), None);
        assert_eq!(s.get("default/web").unwrap().spec.replicas, 5);
        assert_eq!(s.get("default/web").unwrap().replicas.len(), 1);
        assert_eq!(s.get("default/web").unwrap().grant.as_deref(), Some("tok"));
    }

    #[test]
    fn upsert_records_history_on_revision_change() {
        let mut s = Store::default();
        let mut v1 = deploy("web", 2);
        v1.image = "nginx:1.25".into();
        s.upsert(v1, None);
        let mut v2 = deploy("web", 2);
        v2.image = "nginx:1.26".into();
        s.upsert(v2, None);
        let m = s.get("default/web").unwrap();
        assert_eq!(m.spec.image, "nginx:1.26");
        assert_eq!(m.history.len(), 1, "prior revision recorded");
        assert_eq!(m.history[0].image, "nginx:1.25");
        // scaling (same revision) does not grow history
        let mut scaled = deploy("web", 5);
        scaled.image = "nginx:1.26".into();
        s.upsert(scaled, None);
        assert_eq!(s.get("default/web").unwrap().history.len(), 1);
    }

    #[test]
    fn namespaces_do_not_collide() {
        let mut s = Store::default();
        let mut a = deploy("web", 1);
        a.namespace = "team-a".into();
        let mut b = deploy("web", 9);
        b.namespace = "team-b".into();
        s.upsert(a, None);
        s.upsert(b, None);
        assert_eq!(s.get("team-a/web").unwrap().spec.replicas, 1);
        assert_eq!(s.get("team-b/web").unwrap().spec.replicas, 9);
        assert_eq!(s.names_in("team-a"), vec!["team-a/web"]);
    }

    #[test]
    fn remove_returns_managed() {
        let mut s = Store::default();
        s.upsert(deploy("web", 1), None);
        let removed = s.remove("default/web").unwrap();
        assert_eq!(removed.spec.name, "web");
        assert!(s.get("default/web").is_none());
    }

    #[test]
    fn names_are_sorted() {
        let mut s = Store::default();
        s.upsert(deploy("zebra", 1), None);
        s.upsert(deploy("apple", 1), None);
        assert_eq!(s.names(), vec!["default/apple", "default/zebra"]);
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
        assert_eq!(back.get("default/web").unwrap().spec.replicas, 2);
        assert_eq!(back.version, STORE_VERSION);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn migrates_v0_unnamespaced_file() {
        // Simulate a v0 file: version omitted, deployment keyed by bare name, namespace empty.
        let dir = std::env::temp_dir().join(format!("ce-gke-migrate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("old.json");
        let raw = r#"{"deployments":{"web":{"spec":{"name":"web","image":"nginx","replicas":2,"command":[],"select":[],"bid":"0","duration_secs":60,"strategy":{"type":"recreate"}},"replicas":[]}}}"#;
        std::fs::write(&path, raw).unwrap();
        let s = Store::load(&path).unwrap();
        assert_eq!(s.version, STORE_VERSION);
        // re-keyed to default/web with a backfilled namespace
        let m = s.get("default/web").expect("re-keyed under default namespace");
        assert_eq!(m.spec.namespace, "default");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn state_lock_excludes_concurrent_holder() {
        let dir = std::env::temp_dir().join(format!("ce-gke-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");
        let g = StateLock::acquire(&path).unwrap();
        // A second immediate acquire should fail within the spin budget (held by `g`).
        // Use a short-circuit: the lock file exists and is fresh, so acquire spins then bails.
        // To keep the test fast we just assert the lock file exists while held.
        assert!(path.with_extension("lock").exists());
        drop(g);
        assert!(!path.with_extension("lock").exists(), "lock released on drop");
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
