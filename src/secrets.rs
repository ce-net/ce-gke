//! Local secret store — resolves [`crate::spec::SecretRef`] env vars without putting credentials in
//! manifests or the persisted desired state.
//!
//! GKE mounts Secret Manager values into pods; ce-gke is a thin client, so secrets live in a small
//! local JSON file (`<data_dir>/ce/gke-secrets.json`, `0600` on Unix) that the operator populates
//! with `ce-gke secret set NAME VALUE`. At deploy time the orchestrator resolves each `value_from`
//! reference to its literal and injects it into the replica's environment. The literal is never
//! written back into `gke-state.json` (only the *reference* is), so the desired state remains safe
//! to share.
//!
//! This is deliberately simple (no envelope encryption, no remote KMS) — a real coherent slice that
//! keeps secrets out of manifests and state. Stronger at-rest protection is documented as deferred.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// The on-disk secret store: name -> value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretStore {
    #[serde(default)]
    secrets: BTreeMap<String, String>,
}

impl SecretStore {
    /// Default path: `<data_dir>/ce/gke-secrets.json`.
    pub fn default_path() -> Result<PathBuf> {
        let dir = directories::ProjectDirs::from("", "", "ce")
            .context("could not determine the CE data directory")?
            .data_dir()
            .to_path_buf();
        Ok(dir.join("gke-secrets.json"))
    }

    /// Load the store, or an empty one if absent. A corrupt file is an error.
    pub fn load(path: &Path) -> Result<SecretStore> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("secret file {} is corrupt", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(SecretStore::default()),
            Err(e) => Err(e).with_context(|| format!("reading secret file {}", path.display())),
        }
    }

    /// Persist atomically (temp-file + rename) with `0600` perms on Unix.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating secret dir {}", parent.display()))?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        std::fs::write(&tmp, &bytes).with_context(|| format!("writing {}", tmp.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
        }
        std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
        Ok(())
    }

    /// Set (or replace) a secret value.
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.secrets.insert(name.into(), value.into());
    }

    /// Remove a secret, returning whether it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.secrets.remove(name).is_some()
    }

    /// Resolve a secret by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.secrets.get(name).map(|s| s.as_str())
    }

    /// Names of all secrets (values never exposed), sorted.
    pub fn names(&self) -> Vec<String> {
        self.secrets.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_remove() {
        let mut s = SecretStore::default();
        s.set("db", "hunter2");
        assert_eq!(s.get("db"), Some("hunter2"));
        assert_eq!(s.names(), vec!["db"]);
        assert!(s.remove("db"));
        assert!(!s.remove("db"));
        assert_eq!(s.get("db"), None);
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("ce-gke-secrets-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.json");
        let mut s = SecretStore::default();
        s.set("token", "abc");
        s.save(&path).unwrap();
        let back = SecretStore::load(&path).unwrap();
        assert_eq!(back.get("token"), Some("abc"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_is_empty() {
        let path = std::env::temp_dir().join("ce-gke-secrets-missing-xyz.json");
        let _ = std::fs::remove_file(&path);
        assert!(SecretStore::load(&path).unwrap().names().is_empty());
    }

    #[test]
    fn load_corrupt_is_error() {
        let dir = std::env::temp_dir().join(format!("ce-gke-secrets-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.json");
        std::fs::write(&path, b"not json").unwrap();
        assert!(SecretStore::load(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
