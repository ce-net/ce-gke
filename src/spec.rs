//! Deployment spec — the declarative desired state, GKE/`kubectl apply`-style.
//!
//! A [`Deployment`] is what the user *wants*: an image, a replica count, per-replica resources,
//! optional host-selection tags, a funding bid, and a rollout strategy. The orchestrator's job is
//! to make the world match this. It is content-addressed by [`Deployment::revision`] so a rolling
//! update is "the running pods are on the wrong revision".
//!
//! These types (de)serialize from both JSON and YAML so `ce-gke apply -f deploy.yaml` works with
//! the same manifests Kubernetes users already write.

use ce_rs::Amount;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Per-replica resource request. Mirrors the `BidSpec` resource fields the host enforces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
    /// CPU cores requested per replica.
    pub cpu_cores: u32,
    /// Memory (MiB) requested per replica.
    pub mem_mb: u64,
}

impl Default for Resources {
    fn default() -> Self {
        Resources { cpu_cores: 1, mem_mb: 256 }
    }
}

/// How replicas are rolled when the spec (image/resources/command) changes.
///
/// Internally tagged by a `type` field so the manifest representation is a plain map
/// (`{type: rolling_update, max_unavailable: 1, max_surge: 1}`) that both serde_json and serde_yaml
/// handle identically — externally-tagged enums serialize awkwardly under serde_yaml.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Strategy {
    /// Replace pods incrementally, never dropping below `desired - max_unavailable` available and
    /// never exceeding `desired + max_surge` total. The default (and the only safe one for a
    /// service that must stay up).
    RollingUpdate {
        /// How many replicas may be unavailable during the roll (absolute count).
        #[serde(default = "one")]
        max_unavailable: u32,
        /// How many extra replicas may be created above desired during the roll (absolute count).
        #[serde(default = "one")]
        max_surge: u32,
    },
    /// Tear every old replica down, then bring the new revision up. Causes downtime; only for
    /// workloads that cannot run two revisions at once.
    Recreate,
}

fn one() -> u32 {
    1
}

impl Default for Strategy {
    fn default() -> Self {
        Strategy::RollingUpdate { max_unavailable: 1, max_surge: 1 }
    }
}

/// The declarative desired state for one workload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    /// Unique workload name (DNS-label-ish: lowercase `a-z`/`0-9`/hyphen, 1-63 chars).
    pub name: String,
    /// Container image to run for every replica.
    pub image: String,
    /// Command override (empty = image entrypoint).
    #[serde(default)]
    pub command: Vec<String>,
    /// Desired number of running replicas.
    pub replicas: u32,
    /// Per-replica resource request.
    #[serde(default)]
    pub resources: Resources,
    /// Only place replicas on hosts advertising *all* of these atlas self-tags (e.g. `["docker"]`,
    /// `["docker","gpu"]`). `docker` is implied — a host must run containers — but listing it is
    /// fine. Empty means "any docker host".
    #[serde(default)]
    pub select: Vec<String>,
    /// Max credits to fund each replica (locked at deploy time). Base units, decimal-string on wire.
    #[serde(default)]
    pub bid: Amount,
    /// Max expected runtime per replica, seconds (the host expires the cell after this).
    #[serde(default = "default_duration")]
    pub duration_secs: u64,
    /// Rollout strategy when the spec changes.
    #[serde(default)]
    pub strategy: Strategy,
}

fn default_duration() -> u64 {
    3600
}

impl Deployment {
    /// The content-address of the *pod template* — image, command, resources, bid, duration. Two
    /// deployments with the same template (but different `replicas`) share a revision, so scaling
    /// is not a rollout but changing the image is. Hex sha256, first 16 chars (short, collision-safe
    /// enough for a revision label).
    pub fn revision(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.image.as_bytes());
        h.update([0]);
        for c in &self.command {
            h.update(c.as_bytes());
            h.update([0]);
        }
        h.update(self.resources.cpu_cores.to_le_bytes());
        h.update(self.resources.mem_mb.to_le_bytes());
        h.update(self.bid.base().to_le_bytes());
        h.update(self.duration_secs.to_le_bytes());
        // select affects where, not what, so it is intentionally excluded from the pod-template hash.
        let digest = h.finalize();
        hex::encode(&digest[..8])
    }

    /// Validate the spec; returns the first problem found. Pure — used by `apply` before touching
    /// the mesh, so a bad manifest never deploys anything.
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_name(&self.name)?;
        if self.image.trim().is_empty() {
            anyhow::bail!("deployment '{}': image must not be empty", self.name);
        }
        if self.replicas == 0 {
            // Zero replicas is legal (a scaled-to-zero service); we only reject negative-ish via the
            // u32 type. But resources must still be sane.
        }
        if self.resources.cpu_cores == 0 {
            anyhow::bail!("deployment '{}': cpu_cores must be >= 1", self.name);
        }
        if self.resources.mem_mb == 0 {
            anyhow::bail!("deployment '{}': mem_mb must be >= 1", self.name);
        }
        if self.bid.base() < 0 {
            anyhow::bail!("deployment '{}': bid must not be negative", self.name);
        }
        if let Strategy::RollingUpdate { max_unavailable: 0, max_surge: 0 } = &self.strategy {
            anyhow::bail!(
                "deployment '{}': rolling update needs max_unavailable>0 or max_surge>0 (else it can never progress)",
                self.name
            );
        }
        Ok(())
    }

    /// The atlas self-tags a host must advertise to be a placement candidate: `docker` plus any
    /// extra `select` tags, de-duplicated.
    pub fn required_tags(&self) -> Vec<String> {
        let mut tags = vec!["docker".to_string()];
        for t in &self.select {
            if !tags.contains(t) {
                tags.push(t.clone());
            }
        }
        tags
    }

    /// Parse a manifest from YAML or JSON (YAML is a superset of JSON, so one parser covers both).
    pub fn from_manifest(s: &str) -> anyhow::Result<Deployment> {
        let d: Deployment = serde_yaml::from_str(s)
            .map_err(|e| anyhow::anyhow!("manifest is not valid YAML/JSON: {e}"))?;
        d.validate()?;
        Ok(d)
    }
}

/// Validate a deployment name: 1-63 chars, lowercase `a-z`/`0-9`/hyphen, not leading/trailing hyphen.
pub fn validate_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() || name.len() > 63 {
        anyhow::bail!("name '{name}' must be 1-63 characters");
    }
    if name.starts_with('-') || name.ends_with('-') {
        anyhow::bail!("name '{name}' must not start or end with a hyphen");
    }
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
        anyhow::bail!("name '{name}' may only contain lowercase letters, digits, and hyphens");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Deployment {
        Deployment {
            name: "web".into(),
            image: "nginx:1.25".into(),
            command: vec![],
            replicas: 3,
            resources: Resources { cpu_cores: 1, mem_mb: 256 },
            select: vec![],
            bid: Amount::from_credits(5),
            duration_secs: 3600,
            strategy: Strategy::default(),
        }
    }

    #[test]
    fn revision_is_stable_for_same_template() {
        let a = base();
        let mut b = base();
        b.replicas = 99; // replicas do not change the pod template
        b.select = vec!["gpu".into()]; // nor does placement
        assert_eq!(a.revision(), b.revision());
    }

    #[test]
    fn revision_changes_with_image() {
        let a = base();
        let mut b = base();
        b.image = "nginx:1.26".into();
        assert_ne!(a.revision(), b.revision());
    }

    #[test]
    fn revision_changes_with_command_and_resources() {
        let a = base();
        let mut img = base();
        img.command = vec!["sleep".into(), "1".into()];
        assert_ne!(a.revision(), img.revision());
        let mut res = base();
        res.resources.cpu_cores = 4;
        assert_ne!(a.revision(), res.revision());
        let mut mem = base();
        mem.resources.mem_mb = 1024;
        assert_ne!(a.revision(), mem.revision());
    }

    #[test]
    fn revision_is_short_hex() {
        let r = base().revision();
        assert_eq!(r.len(), 16);
        assert!(r.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn validate_accepts_good_spec() {
        assert!(base().validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_image() {
        let mut d = base();
        d.image = "  ".into();
        assert!(d.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_resources() {
        let mut d = base();
        d.resources.cpu_cores = 0;
        assert!(d.validate().is_err());
        let mut d = base();
        d.resources.mem_mb = 0;
        assert!(d.validate().is_err());
    }

    #[test]
    fn validate_rejects_negative_bid() {
        let mut d = base();
        d.bid = Amount::from_base(-1);
        assert!(d.validate().is_err());
    }

    #[test]
    fn validate_rejects_stuck_rolling_update() {
        let mut d = base();
        d.strategy = Strategy::RollingUpdate { max_unavailable: 0, max_surge: 0 };
        assert!(d.validate().is_err());
    }

    #[test]
    fn validate_allows_zero_replicas() {
        let mut d = base();
        d.replicas = 0;
        assert!(d.validate().is_ok());
    }

    #[test]
    fn name_validation_rules() {
        assert!(validate_name("web").is_ok());
        assert!(validate_name("web-1").is_ok());
        assert!(validate_name("a").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("-web").is_err());
        assert!(validate_name("web-").is_err());
        assert!(validate_name("Web").is_err());
        assert!(validate_name("web_1").is_err());
        assert!(validate_name(&"a".repeat(64)).is_err());
        assert!(validate_name(&"a".repeat(63)).is_ok());
    }

    #[test]
    fn required_tags_always_include_docker() {
        let mut d = base();
        d.select = vec!["gpu".into(), "linux".into()];
        assert_eq!(d.required_tags(), vec!["docker", "gpu", "linux"]);
        // docker listed explicitly is not duplicated
        d.select = vec!["docker".into(), "gpu".into()];
        assert_eq!(d.required_tags(), vec!["docker", "gpu"]);
        // no select → just docker
        d.select = vec![];
        assert_eq!(d.required_tags(), vec!["docker"]);
    }

    #[test]
    fn parses_yaml_manifest() {
        let yaml = r#"
name: web
image: nginx:1.25
replicas: 4
resources:
  cpu_cores: 2
  mem_mb: 512
select:
  - gpu
bid: "5000000000000000000"
strategy:
  type: rolling_update
  max_unavailable: 1
  max_surge: 2
"#;
        let d = Deployment::from_manifest(yaml).unwrap();
        assert_eq!(d.name, "web");
        assert_eq!(d.replicas, 4);
        assert_eq!(d.resources.cpu_cores, 2);
        assert_eq!(d.select, vec!["gpu"]);
        assert_eq!(d.bid, Amount::from_credits(5));
        assert_eq!(d.strategy, Strategy::RollingUpdate { max_unavailable: 1, max_surge: 2 });
    }

    #[test]
    fn parses_json_manifest() {
        // YAML is a superset of JSON, so the same parser reads a JSON manifest.
        let json = r#"{"name":"api","image":"redis","replicas":2,"bid":"0"}"#;
        let d = Deployment::from_manifest(json).unwrap();
        assert_eq!(d.name, "api");
        assert_eq!(d.replicas, 2);
        // defaults applied
        assert_eq!(d.resources, Resources::default());
        assert_eq!(d.duration_secs, 3600);
        assert_eq!(d.strategy, Strategy::default());
    }

    #[test]
    fn manifest_with_bad_spec_is_rejected() {
        let yaml = "name: BAD\nimage: x\nreplicas: 1\n";
        assert!(Deployment::from_manifest(yaml).is_err());
    }

    #[test]
    fn malformed_manifest_does_not_panic() {
        assert!(Deployment::from_manifest("::: not yaml :::").is_err());
        assert!(Deployment::from_manifest("").is_err());
        assert!(Deployment::from_manifest("name: web").is_err()); // missing required fields
    }

    #[test]
    fn deployment_json_roundtrip() {
        let d = base();
        let s = serde_json::to_string(&d).unwrap();
        let back: Deployment = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }
}
