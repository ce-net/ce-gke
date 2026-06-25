//! [`Dep`] — a fluent, in-code description of a dependency an app wants. This is the "no stack.yaml"
//! point: an app declares its *direct* dependencies in code and the SDK ensures them. A `Dep` lowers
//! to a [`crate::Deployment`] for the actual placement (reusing ce-gke's spec + planners).

use crate::Deployment;
use anyhow::Result;
use std::time::Duration;

/// Where a dependency should run.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Place {
    /// Let placement pick (atlas-ranked). The default.
    #[default]
    Auto,
    /// Pin to a specific node id (64-hex) — e.g. co-locate with an encoder.
    Node(String),
    /// Any node advertising this atlas self-tag (e.g. `gpu`, `wasm`).
    Tag(String),
}

/// A dependency declaration. Build it fluently, then `gke.ensure(dep)`.
#[derive(Clone, Debug)]
pub struct Dep {
    pub name: String,
    pub namespace: String,
    pub image: String,
    pub cmd: Vec<String>,
    pub replicas: u32,
    pub cpu_cores: u32,
    pub mem_mb: u32,
    pub duration_secs: u64,
    pub place: Place,
    /// A singleton has one owner (claim-based); non-singletons place `replicas` copies.
    pub singleton: bool,
    /// TCP port to readiness-probe (None = no probe; ready as soon as placed).
    pub health_port: Option<u16>,
    /// How long each lease this app holds on the dependency is valid before it must renew.
    pub lease_ttl_secs: u64,
}

impl Dep {
    /// A new dependency named `name` (in the default namespace).
    pub fn new(name: impl Into<String>) -> Self {
        Dep {
            name: name.into(),
            namespace: "default".into(),
            image: String::new(),
            cmd: vec![],
            replicas: 1,
            cpu_cores: 1,
            mem_mb: 256,
            duration_secs: 24 * 60 * 60,
            place: Place::Auto,
            singleton: false,
            health_port: None,
            lease_ttl_secs: 60,
        }
    }
    pub fn namespace(mut self, ns: impl Into<String>) -> Self { self.namespace = ns.into(); self }
    pub fn image(mut self, image: impl Into<String>) -> Self { self.image = image.into(); self }
    pub fn cmd<I: IntoIterator<Item = S>, S: Into<String>>(mut self, c: I) -> Self { self.cmd = c.into_iter().map(Into::into).collect(); self }
    pub fn replicas(mut self, n: u32) -> Self { self.replicas = n.max(1); self }
    pub fn resources(mut self, cpu_cores: u32, mem_mb: u32) -> Self { self.cpu_cores = cpu_cores.max(1); self.mem_mb = mem_mb.max(1); self }
    pub fn duration(mut self, d: Duration) -> Self { self.duration_secs = d.as_secs().max(1); self }
    /// Mark this dependency a singleton (one claim-based owner; `replicas` forced to 1).
    pub fn singleton(mut self) -> Self { self.singleton = true; self.replicas = 1; self }
    pub fn place(mut self, p: Place) -> Self { self.place = p; self }
    pub fn place_node(mut self, node_id: impl Into<String>) -> Self { self.place = Place::Node(node_id.into()); self }
    pub fn place_tag(mut self, tag: impl Into<String>) -> Self { self.place = Place::Tag(tag.into()); self }
    pub fn health_tcp(mut self, port: u16) -> Self { self.health_port = Some(port); self }
    pub fn lease_ttl(mut self, d: Duration) -> Self { self.lease_ttl_secs = d.as_secs().max(1); self }

    /// The mesh service name this dependency is discovered under. The SDK uses this consistently for
    /// advertise + locate; a dependency app must advertise the same name.
    pub fn service(&self) -> String {
        format!("ce-gke/{}/{}", self.namespace, self.name)
    }

    /// The atlas self-tag required for placement, if `Place::Tag`.
    pub fn select_tag(&self) -> Option<&str> {
        match &self.place {
            Place::Tag(t) => Some(t.as_str()),
            _ => None,
        }
    }

    /// Lower to a [`crate::Deployment`] (via its manifest parser, so we never hand-build the struct).
    pub fn to_deployment(&self) -> Result<Deployment> {
        let mut y = String::new();
        y.push_str(&format!("name: {}\n", self.name));
        y.push_str(&format!("namespace: {}\n", self.namespace));
        y.push_str(&format!("image: {}\n", self.image));
        if !self.cmd.is_empty() {
            let items = self.cmd.iter().map(|c| format!("{:?}", c)).collect::<Vec<_>>().join(", ");
            y.push_str(&format!("command: [{}]\n", items));
        }
        y.push_str(&format!("replicas: {}\n", self.replicas));
        y.push_str(&format!("resources:\n  cpu_cores: {}\n  mem_mb: {}\n", self.cpu_cores, self.mem_mb));
        y.push_str(&format!("duration_secs: {}\n", self.duration_secs));
        y.push_str("bid: \"1000000000000000000\"\n");
        if let Some(tag) = self.select_tag() {
            y.push_str(&format!("select: [{}]\n", tag));
        }
        if let Some(port) = self.health_port {
            y.push_str(&format!("readiness:\n  type: tcp\n  port: {}\n", port));
        }
        Deployment::from_manifest(&y)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_name_is_namespaced() {
        let d = Dep::new("cast-encoder").namespace("cast");
        assert_eq!(d.service(), "ce-gke/cast/cast-encoder");
    }

    #[test]
    fn singleton_forces_one_replica() {
        let d = Dep::new("enc").replicas(5).singleton();
        assert_eq!(d.replicas, 1);
        assert!(d.singleton);
    }

    #[test]
    fn lowers_to_a_valid_deployment() {
        let d = Dep::new("enc")
            .namespace("cast")
            .image("ce-net/cast-encoder:latest")
            .resources(2, 1024)
            .place_tag("gpu")
            .health_tcp(8080)
            .singleton();
        let dep = d.to_deployment().expect("manifest should parse");
        assert_eq!(dep.name, "enc");
        assert_eq!(dep.image, "ce-net/cast-encoder:latest");
    }
}
