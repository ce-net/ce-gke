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

/// Hard upper bound on a deployment's replica count. GKE's per-cluster pod ceilings exist for the
/// same reason: an unbounded `replicas` (the `u32` type alone allows ~4.3 billion) would let a
/// fat-fingered manifest fan out billions of deploys, each locking `bid` credits — a self-inflicted
/// denial-of-service and spend bomb. 4096 is generous for a mesh app yet keeps placement, billing,
/// and the state file bounded.
pub const MAX_REPLICAS: u32 = 4096;

/// Maximum length of an image reference (registry/name:tag@digest). Real registries cap names well
/// under this; the bound exists so an adversarial or buggy manifest cannot blow up the state file or
/// the wire BidSpec.
pub const MAX_IMAGE_LEN: usize = 512;

/// Maximum number of environment variables per replica.
pub const MAX_ENV_VARS: usize = 256;

/// Maximum length of a single env var name or value.
pub const MAX_ENV_LEN: usize = 32 * 1024;

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

/// A single environment variable injected into every replica's container.
///
/// `value` is inline plaintext. For sensitive material prefer [`EnvVar::value_from`] referencing a
/// [`SecretRef`] so the literal never lands in the manifest or the state file; the orchestrator
/// resolves it from the local secret store at deploy time. (This is the app-side analogue of a
/// Kubernetes `envFrom`/`valueFrom.secretKeyRef`.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvVar {
    /// Variable name (`A-Z`, `a-z`, `0-9`, `_`; must not start with a digit — POSIX env rules).
    pub name: String,
    /// Inline literal value. Mutually exclusive with `value_from`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    /// Resolve the value from a named secret at deploy time instead of inlining it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_from: Option<SecretRef>,
}

/// Reference to a secret resolved from the local secret store (`<data_dir>/ce/gke-secrets.json`),
/// keyed `name`. Keeps credentials out of manifests and the persisted desired state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    /// Secret name in the local store.
    pub secret: String,
}

impl EnvVar {
    /// An inline `NAME=value` variable.
    pub fn inline(name: impl Into<String>, value: impl Into<String>) -> EnvVar {
        EnvVar { name: name.into(), value: value.into(), value_from: None }
    }
    /// A variable whose value is resolved from the secret store at deploy time.
    pub fn value_from(name: impl Into<String>, secret: impl Into<String>) -> EnvVar {
        EnvVar {
            name: name.into(),
            value: String::new(),
            value_from: Some(SecretRef { secret: secret.into() }),
        }
    }
    /// `NAME=value` form for passing to the container runtime (resolved value).
    pub fn as_kv(&self, resolved: &str) -> String {
        format!("{}={}", self.name, resolved)
    }
    fn validate(&self) -> anyhow::Result<()> {
        if self.name.is_empty() || self.name.len() > MAX_ENV_LEN {
            anyhow::bail!("env var name must be 1-{MAX_ENV_LEN} chars");
        }
        let first_ok = self
            .name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false);
        if !first_ok {
            anyhow::bail!("env var '{}': name must start with a letter or underscore", self.name);
        }
        if !self.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            anyhow::bail!(
                "env var '{}': name may only contain letters, digits, and underscores",
                self.name
            );
        }
        if self.value.len() > MAX_ENV_LEN {
            anyhow::bail!("env var '{}': value exceeds {MAX_ENV_LEN} bytes", self.name);
        }
        if !self.value.is_empty() && self.value_from.is_some() {
            anyhow::bail!(
                "env var '{}': set either an inline value or value_from, not both",
                self.name
            );
        }
        Ok(())
    }
}

/// A health probe used to decide replica liveness/readiness beyond raw CE job status.
///
/// CE only reports a coarse job status (`pending`/`running`/`failed:*`); a container that is
/// "running" per the host may still be serving 500s or be deadlocked. A probe lets the orchestrator
/// check the actual workload. Probes run over CE primitives the orchestrator already has: an
/// [`ProbeKind::Exec`] runs a command in the cell over the mesh, an [`ProbeKind::Tcp`]/[`ProbeKind::Http`]
/// checks a port the workload exposes (today modeled as an exec of a tiny check inside the cell, so
/// no inbound networking to the replica is required — the same trick GKE's exec probes use).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Probe {
    /// What kind of check to run.
    #[serde(flatten)]
    pub kind: ProbeKind,
    /// Consecutive successes before the probe is considered passing (readiness gate). Min 1.
    #[serde(default = "one")]
    pub success_threshold: u32,
    /// Consecutive failures before the probe is considered failing (liveness trip). Min 1.
    #[serde(default = "three")]
    pub failure_threshold: u32,
    /// Seconds between probe attempts (advisory; the daemon's reconcile interval bounds it).
    #[serde(default = "ten")]
    pub period_secs: u64,
}

fn three() -> u32 {
    3
}
fn ten() -> u64 {
    10
}

/// The check a [`Probe`] performs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProbeKind {
    /// Run a command inside the cell; exit code 0 = healthy.
    Exec {
        /// Command and args (non-empty).
        command: Vec<String>,
    },
    /// Check that a TCP port inside the cell accepts a connection (modeled as an in-cell check).
    Tcp {
        /// Port to check.
        port: u16,
    },
    /// HTTP GET a path on a port inside the cell; 2xx/3xx = healthy.
    Http {
        /// Port to GET.
        port: u16,
        /// Path to GET (default `/`).
        #[serde(default = "root_path")]
        path: String,
    },
}

fn root_path() -> String {
    "/".to_string()
}

impl Probe {
    fn validate(&self) -> anyhow::Result<()> {
        if self.success_threshold == 0 || self.failure_threshold == 0 {
            anyhow::bail!("probe thresholds must be >= 1");
        }
        match &self.kind {
            ProbeKind::Exec { command } => {
                if command.is_empty() {
                    anyhow::bail!("exec probe needs a non-empty command");
                }
            }
            ProbeKind::Tcp { port } | ProbeKind::Http { port, .. } => {
                if *port == 0 {
                    anyhow::bail!("probe port must be non-zero");
                }
            }
        }
        Ok(())
    }

    /// The command to run inside the cell to perform this probe (exit 0 = pass). Tcp/Http are mapped
    /// to a small shell check so no inbound networking to the replica is required.
    pub fn check_command(&self) -> Vec<String> {
        match &self.kind {
            ProbeKind::Exec { command } => command.clone(),
            ProbeKind::Tcp { port } => vec![
                "sh".into(),
                "-c".into(),
                // Try /dev/tcp (bash) then nc; succeed if either connects.
                format!(
                    "(exec 3<>/dev/tcp/127.0.0.1/{port}) 2>/dev/null || nc -z 127.0.0.1 {port}"
                ),
            ],
            ProbeKind::Http { port, path } => vec![
                "sh".into(),
                "-c".into(),
                format!(
                    "wget -q -O /dev/null http://127.0.0.1:{port}{path} || curl -fsS http://127.0.0.1:{port}{path} -o /dev/null"
                ),
            ],
        }
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Strategy::RollingUpdate { max_unavailable: 1, max_surge: 1 }
    }
}

/// The declarative desired state for one workload.
///
/// Parse one from a YAML or JSON manifest and inspect its content-addressed revision:
///
/// ```
/// use ce_gke::spec::Deployment;
/// let d = Deployment::from_manifest("name: web\nimage: nginx:1.25\nreplicas: 3\n").unwrap();
/// assert_eq!(d.namespace, "default");        // implied
/// assert_eq!(d.key(), "default/web");        // namespace-scoped identity
/// assert_eq!(d.revision().len(), 16);        // short content hash of the pod template
/// // Scaling does not change the revision (it is not a rollout); changing the image does.
/// let mut scaled = d.clone();
/// scaled.replicas = 10;
/// assert_eq!(scaled.revision(), d.revision());
/// let mut bumped = d.clone();
/// bumped.image = "nginx:1.26".into();
/// assert_ne!(bumped.revision(), d.revision());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    /// Unique workload name (DNS-label-ish: lowercase `a-z`/`0-9`/hyphen, 1-63 chars).
    pub name: String,
    /// Namespace scoping the workload (DNS-label-ish). Two operators/teams using different
    /// namespaces never collide on `name`. Defaults to `default`.
    #[serde(default = "default_namespace")]
    pub namespace: String,
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
    /// Environment variables injected into every replica.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
    /// Liveness probe: a failing probe trips the replica to `Failed` so it is replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liveness: Option<Probe>,
    /// Readiness probe: a not-yet-ready replica is held `Pending` (does not count toward
    /// availability, gates the rollout) until it passes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<Probe>,
    /// Publish the healthy replica set as a named CE service (DHT discovery). Empty = no service.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service: String,
    /// Services this deployment depends on. Each is resolved over the mesh (DHT/`ce_rs::locate`)
    /// before any replica is placed; an unmet dependency holds the deployment in a `waiting` state
    /// (no replicas placed) until the next reconcile tick finds it satisfied. This is the
    /// docker-compose `depends_on` analogue, but mesh-native: a dependency is "a live instance of a
    /// named CE service exists", not "a sibling container is up on this host".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<DependencyRef>,
}

/// A dependency on another CE service by name. The dependency is considered met when
/// [`ce_rs::locate`] finds at least one live instance of the service; when `healthy` is set, the
/// instance must additionally pass the orchestrator's readiness check (the host-agent probe), not
/// merely be advertised.
///
/// `service` is a bare service name (the same `service` field a sibling [`Deployment`] publishes).
/// Within a [`crate::stack::Stack`] it is resolved namespace-locally to the sibling's
/// [`Deployment::service_name`]; standalone it is resolved against the default namespace. The
/// resolution rule lives in [`DependencyRef::resolved_service_name`] so it stays pure and testable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyRef {
    /// The bare CE service name this deployment waits for.
    pub service: String,
    /// Require a *healthy* (readiness-passing) instance, not merely an advertised one. Defaults to
    /// false (advertised-and-live is enough), matching docker-compose's `service_started` vs
    /// `service_healthy` conditions.
    #[serde(default)]
    pub healthy: bool,
}

impl DependencyRef {
    /// The namespaced CE service name to resolve over the mesh, given the dependent's namespace.
    /// Mirrors [`Deployment::service_name`] so a dependency on a sibling's `service` resolves to the
    /// exact name that sibling advertises: `ce-gke/<namespace>/<service>`.
    pub fn resolved_service_name(&self, namespace: &str) -> String {
        format!("ce-gke/{}/{}", namespace, self.service)
    }
}

fn default_duration() -> u64 {
    3600
}

fn default_namespace() -> String {
    "default".to_string()
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
        // env is part of the pod template (changing config restarts pods, like GKE).
        for e in &self.env {
            h.update(e.name.as_bytes());
            h.update([0]);
            h.update(e.value.as_bytes());
            h.update([0]);
            if let Some(s) = &e.value_from {
                h.update(s.secret.as_bytes());
            }
            h.update([1]);
        }
        // select affects where, not what, so it is intentionally excluded from the pod-template hash.
        let digest = h.finalize();
        hex::encode(&digest[..8])
    }

    /// The namespace-scoped store key: `namespace/name`. Stable identity across CLI invocations.
    pub fn key(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    /// The CE service name the healthy replica set advertises, if any. Namespaced so two teams'
    /// `web` services do not collide: `ce-gke/<namespace>/<service>`.
    pub fn service_name(&self) -> Option<String> {
        if self.service.is_empty() {
            None
        } else {
            Some(format!("ce-gke/{}/{}", self.namespace, self.service))
        }
    }

    /// Validate the spec; returns the first problem found. Pure — used by `apply` before touching
    /// the mesh, so a bad manifest never deploys anything.
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_name(&self.name)?;
        validate_name(&self.namespace)
            .map_err(|e| anyhow::anyhow!("namespace invalid: {e}"))?;
        validate_image(&self.image)?;
        if self.replicas > MAX_REPLICAS {
            anyhow::bail!(
                "deployment '{}': replicas {} exceeds the {MAX_REPLICAS} ceiling",
                self.name,
                self.replicas
            );
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
        if self.env.len() > MAX_ENV_VARS {
            anyhow::bail!(
                "deployment '{}': {} env vars exceeds the {MAX_ENV_VARS} cap",
                self.name,
                self.env.len()
            );
        }
        let mut seen = std::collections::HashSet::new();
        for e in &self.env {
            e.validate()?;
            if !seen.insert(&e.name) {
                anyhow::bail!("deployment '{}': duplicate env var '{}'", self.name, e.name);
            }
        }
        if let Some(p) = &self.liveness {
            p.validate().map_err(|e| anyhow::anyhow!("liveness probe: {e}"))?;
        }
        if let Some(p) = &self.readiness {
            p.validate().map_err(|e| anyhow::anyhow!("readiness probe: {e}"))?;
        }
        if !self.service.is_empty() {
            validate_name(&self.service)
                .map_err(|e| anyhow::anyhow!("service name invalid: {e}"))?;
        }
        let mut dep_seen = std::collections::HashSet::new();
        for dep in &self.depends_on {
            validate_name(&dep.service).map_err(|e| {
                anyhow::anyhow!("deployment '{}': depends_on service invalid: {e}", self.name)
            })?;
            if dep.service == self.service {
                anyhow::bail!(
                    "deployment '{}': cannot depend on its own service '{}'",
                    self.name,
                    dep.service
                );
            }
            if !dep_seen.insert(&dep.service) {
                anyhow::bail!(
                    "deployment '{}': duplicate dependency on '{}'",
                    self.name,
                    dep.service
                );
            }
        }
        Ok(())
    }

    /// Build the effective container command with environment injected.
    ///
    /// The SDK `BidSpec` carries no env field, so ce-gke injects variables by prefixing the command
    /// with `env NAME=VALUE ...`. This requires an explicit [`command`](Self::command): an
    /// image-entrypoint deployment (empty command) with env vars is rejected at validate-resolve
    /// time, because we cannot wrap an entrypoint we do not know. `resolve` maps each env var to its
    /// literal value, pulling `value_from` references through the provided resolver (the secret
    /// store). Returns the command vector to pass to the host.
    pub fn effective_command(
        &self,
        resolve_secret: &dyn Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Vec<String>> {
        if self.env.is_empty() {
            return Ok(self.command.clone());
        }
        if self.command.is_empty() {
            anyhow::bail!(
                "deployment '{}': env vars require an explicit `command` (cannot wrap an image entrypoint with env injection)",
                self.name
            );
        }
        let mut out: Vec<String> = vec!["env".to_string()];
        for e in &self.env {
            let value = match &e.value_from {
                Some(secret_ref) => resolve_secret(&secret_ref.secret).ok_or_else(|| {
                    anyhow::anyhow!(
                        "deployment '{}': env '{}' references unknown secret '{}'",
                        self.name,
                        e.name,
                        secret_ref.secret
                    )
                })?,
                None => e.value.clone(),
            };
            out.push(e.as_kv(&value));
        }
        out.extend(self.command.iter().cloned());
        Ok(out)
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

/// Validate a container image reference. Restricts to the printable ASCII subset registries actually
/// use (`a-z A-Z 0-9` plus `. _ - / : @`), bounded by [`MAX_IMAGE_LEN`]. Crucially this rejects
/// multibyte/control characters, which both keeps the wire `BidSpec` clean and guarantees the CLI's
/// byte-index truncation in `get` can never split a multibyte UTF-8 scalar (the historical panic).
pub fn validate_image(image: &str) -> anyhow::Result<()> {
    let img = image.trim();
    if img.is_empty() {
        anyhow::bail!("image must not be empty");
    }
    if img.len() > MAX_IMAGE_LEN {
        anyhow::bail!("image reference exceeds {MAX_IMAGE_LEN} bytes");
    }
    let ok = |c: char| {
        c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/' | ':' | '@')
    };
    if let Some(bad) = img.chars().find(|c| !ok(*c)) {
        anyhow::bail!(
            "image '{img}' contains an invalid character {bad:?} (allowed: a-z A-Z 0-9 . _ - / : @)"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Deployment {
        Deployment {
            name: "web".into(),
            namespace: "default".into(),
            image: "nginx:1.25".into(),
            command: vec![],
            replicas: 3,
            resources: Resources { cpu_cores: 1, mem_mb: 256 },
            select: vec![],
            bid: Amount::from_credits(5),
            duration_secs: 3600,
            strategy: Strategy::default(),
            env: vec![],
            liveness: None,
            readiness: None,
            service: String::new(),
            depends_on: vec![],
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

    #[test]
    fn validate_rejects_multibyte_image() {
        let mut d = base();
        d.image = "café/app:latest".into(); // multibyte é
        assert!(d.validate().is_err(), "multibyte image must be rejected");
    }

    #[test]
    fn validate_rejects_oversized_image() {
        let mut d = base();
        d.image = "a".repeat(MAX_IMAGE_LEN + 1);
        assert!(d.validate().is_err());
    }

    #[test]
    fn validate_rejects_too_many_replicas() {
        let mut d = base();
        d.replicas = MAX_REPLICAS + 1;
        assert!(d.validate().is_err());
        d.replicas = MAX_REPLICAS;
        assert!(d.validate().is_ok());
    }

    #[test]
    fn validate_image_accepts_registry_refs() {
        assert!(validate_image("nginx").is_ok());
        assert!(validate_image("nginx:1.25").is_ok());
        assert!(validate_image("gcr.io/proj/app:v1").is_ok());
        assert!(validate_image("repo@sha256:abcdef").is_ok());
        assert!(validate_image("").is_err());
        assert!(validate_image("a b").is_err()); // space
        assert!(validate_image("a\nb").is_err()); // control
    }

    #[test]
    fn default_namespace_applied() {
        let json = r#"{"name":"api","image":"redis","replicas":1,"bid":"0"}"#;
        let d = Deployment::from_manifest(json).unwrap();
        assert_eq!(d.namespace, "default");
        assert_eq!(d.key(), "default/api");
    }

    #[test]
    fn env_validation() {
        let mut d = base();
        d.env = vec![EnvVar::inline("FOO", "bar"), EnvVar::value_from("TOKEN", "api-key")];
        assert!(d.validate().is_ok());
        // bad name
        d.env = vec![EnvVar::inline("1BAD", "x")];
        assert!(d.validate().is_err());
        // duplicate
        d.env = vec![EnvVar::inline("A", "1"), EnvVar::inline("A", "2")];
        assert!(d.validate().is_err());
        // both inline and value_from
        d.env = vec![EnvVar {
            name: "A".into(),
            value: "x".into(),
            value_from: Some(SecretRef { secret: "s".into() }),
        }];
        assert!(d.validate().is_err());
    }

    #[test]
    fn env_changes_revision() {
        let a = base();
        let mut b = base();
        b.env = vec![EnvVar::inline("FOO", "bar")];
        assert_ne!(a.revision(), b.revision());
    }

    #[test]
    fn probe_check_commands() {
        let exec = Probe {
            kind: ProbeKind::Exec { command: vec!["true".into()] },
            success_threshold: 1,
            failure_threshold: 3,
            period_secs: 10,
        };
        assert_eq!(exec.check_command(), vec!["true".to_string()]);
        let tcp = Probe {
            kind: ProbeKind::Tcp { port: 8080 },
            success_threshold: 1,
            failure_threshold: 3,
            period_secs: 10,
        };
        let cmd = tcp.check_command();
        assert_eq!(cmd[0], "sh");
        assert!(cmd[2].contains("8080"));
        let http = Probe {
            kind: ProbeKind::Http { port: 80, path: "/healthz".into() },
            success_threshold: 1,
            failure_threshold: 3,
            period_secs: 10,
        };
        assert!(http.check_command()[2].contains("/healthz"));
    }

    #[test]
    fn probe_validation() {
        let mut p = Probe {
            kind: ProbeKind::Exec { command: vec![] },
            success_threshold: 1,
            failure_threshold: 1,
            period_secs: 10,
        };
        assert!(p.validate().is_err(), "empty exec command rejected");
        p.kind = ProbeKind::Tcp { port: 0 };
        assert!(p.validate().is_err(), "zero port rejected");
        p.kind = ProbeKind::Tcp { port: 8080 };
        p.success_threshold = 0;
        assert!(p.validate().is_err(), "zero threshold rejected");
    }

    #[test]
    fn effective_command_injects_env() {
        let mut d = base();
        d.command = vec!["server".into(), "--port=80".into()];
        d.env = vec![EnvVar::inline("LOG", "debug"), EnvVar::value_from("TOKEN", "tok")];
        let resolver = |name: &str| {
            if name == "tok" { Some("secret-value".to_string()) } else { None }
        };
        let cmd = d.effective_command(&resolver).unwrap();
        assert_eq!(
            cmd,
            vec!["env", "LOG=debug", "TOKEN=secret-value", "server", "--port=80"]
        );
    }

    #[test]
    fn effective_command_no_env_is_passthrough() {
        let mut d = base();
        d.command = vec!["server".into()];
        let cmd = d.effective_command(&|_| None).unwrap();
        assert_eq!(cmd, vec!["server"]);
    }

    #[test]
    fn effective_command_env_without_command_errors() {
        let mut d = base();
        d.command = vec![];
        d.env = vec![EnvVar::inline("LOG", "debug")];
        assert!(d.effective_command(&|_| None).is_err());
    }

    #[test]
    fn effective_command_missing_secret_errors() {
        let mut d = base();
        d.command = vec!["x".into()];
        d.env = vec![EnvVar::value_from("T", "absent")];
        assert!(d.effective_command(&|_| None).is_err());
    }

    #[test]
    fn service_name_is_namespaced() {
        let mut d = base();
        assert_eq!(d.service_name(), None);
        d.service = "web".into();
        d.namespace = "team-a".into();
        assert_eq!(d.service_name(), Some("ce-gke/team-a/web".to_string()));
    }

    #[test]
    fn depends_on_defaults_empty_and_parses() {
        // Existing manifests with no depends_on still parse (serde default).
        let d = Deployment::from_manifest("name: web\nimage: nginx\nreplicas: 1\n").unwrap();
        assert!(d.depends_on.is_empty());
        // And a manifest can declare deps, including the `healthy` flag.
        let yaml = r#"
name: web
image: nginx
replicas: 1
depends_on:
  - service: db
  - service: cache
    healthy: true
"#;
        let d = Deployment::from_manifest(yaml).unwrap();
        assert_eq!(d.depends_on.len(), 2);
        assert_eq!(d.depends_on[0], DependencyRef { service: "db".into(), healthy: false });
        assert_eq!(d.depends_on[1], DependencyRef { service: "cache".into(), healthy: true });
    }

    #[test]
    fn depends_on_does_not_change_revision() {
        // depends_on affects *when* a deployment is placed, not *what* runs, so it must not change
        // the pod-template revision (changing a dep must not trigger a rollout).
        let a = base();
        let mut b = base();
        b.depends_on = vec![DependencyRef { service: "db".into(), healthy: true }];
        assert_eq!(a.revision(), b.revision());
    }

    #[test]
    fn depends_on_validation() {
        let mut d = base();
        d.service = "web".into();
        // self-dependency rejected
        d.depends_on = vec![DependencyRef { service: "web".into(), healthy: false }];
        assert!(d.validate().is_err());
        // duplicate rejected
        d.depends_on = vec![
            DependencyRef { service: "db".into(), healthy: false },
            DependencyRef { service: "db".into(), healthy: true },
        ];
        assert!(d.validate().is_err());
        // bad name rejected
        d.depends_on = vec![DependencyRef { service: "BAD".into(), healthy: false }];
        assert!(d.validate().is_err());
        // good deps accepted
        d.depends_on = vec![
            DependencyRef { service: "db".into(), healthy: false },
            DependencyRef { service: "cache".into(), healthy: true },
        ];
        assert!(d.validate().is_ok());
    }

    #[test]
    fn dependency_resolved_service_name_matches_sibling() {
        let dep = DependencyRef { service: "db".into(), healthy: false };
        assert_eq!(dep.resolved_service_name("prod"), "ce-gke/prod/db");
        // matches what a sibling publishing service "db" in ns "prod" would advertise
        let mut sib = base();
        sib.service = "db".into();
        sib.namespace = "prod".into();
        assert_eq!(sib.service_name(), Some(dep.resolved_service_name("prod")));
    }

    #[test]
    fn probe_manifest_parses() {
        let yaml = r#"
name: web
image: nginx
replicas: 1
liveness:
  type: http
  port: 8080
  path: /healthz
  failure_threshold: 2
readiness:
  type: tcp
  port: 8080
env:
  - name: LOG_LEVEL
    value: debug
  - name: SECRET
    value_from:
      secret: db-password
service: web-svc
namespace: prod
"#;
        let d = Deployment::from_manifest(yaml).unwrap();
        assert_eq!(d.namespace, "prod");
        assert_eq!(d.env.len(), 2);
        assert_eq!(d.service, "web-svc");
        match d.liveness.as_ref().unwrap().kind {
            ProbeKind::Http { port, .. } => assert_eq!(port, 8080),
            _ => panic!("wrong probe kind"),
        }
    }
}
