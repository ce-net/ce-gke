//! Stack — a multi-deployment "compose" manifest: several [`Deployment`]s plus their inter-service
//! dependencies, applied as one unit.
//!
//! This is the docker-compose / Kubernetes-`List` analogue for the mesh. A single YAML/JSON file
//! declares `apps: [ ... ]` (one entry per [`Deployment`]); each app may declare
//! [`depends_on`](crate::spec::Deployment::depends_on) referencing another app's published
//! `service`. The stack is **topologically sorted** by those dependencies so an `apply` brings up
//! producers before consumers, and a **cycle is rejected** with a clear error before anything
//! touches the mesh.
//!
//! Crucially, dependency *satisfaction* at runtime is not done by ordering alone — services come and
//! go independently on a live mesh — it is checked every reconcile tick via [`ce_rs::locate`]
//! (a dependent whose dep has disappeared goes back to `waiting`). The topo-sort here only gives a
//! sensible *initial* apply order and a static guarantee the graph is acyclic; the running system
//! stays correct purely through the per-tick readiness gate (see [`crate::deps`]). Both layers are
//! pure and unit-tested.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::spec::Deployment;

/// A multi-deployment manifest. Parses from the same YAML/JSON the single-[`Deployment`] manifests
/// use, wrapped in an `apps:` list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stack {
    /// Optional namespace applied to every app that left its namespace at the default. Lets a whole
    /// stack be dropped into one namespace from the top of the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// The deployments in this stack. Order in the file is irrelevant; apply order is computed by
    /// [`Stack::apply_order`] (topo-sort over `depends_on`).
    pub apps: Vec<Deployment>,
}

impl Stack {
    /// Parse a stack manifest from YAML or JSON (YAML is a JSON superset, so one parser covers both),
    /// applying the stack-level namespace and validating every app and the dependency graph.
    pub fn from_manifest(s: &str) -> Result<Stack> {
        let mut stack: Stack = serde_yaml::from_str(s)
            .map_err(|e| anyhow::anyhow!("stack manifest is not valid YAML/JSON: {e}"))?;
        stack.apply_namespace();
        stack.validate()?;
        Ok(stack)
    }

    /// Push the stack-level namespace down into any app still at the default namespace. An app that
    /// set its own namespace explicitly keeps it.
    fn apply_namespace(&mut self) {
        let Some(ns) = self.namespace.clone() else { return };
        for app in &mut self.apps {
            if app.namespace == "default" {
                app.namespace = ns.clone();
            }
        }
    }

    /// Validate the whole stack: each app individually, plus stack-wide invariants — unique app
    /// names within a namespace, every `depends_on` resolves to an app in the *same namespace* that
    /// publishes a matching service, and the dependency graph is acyclic.
    pub fn validate(&self) -> Result<()> {
        if self.apps.is_empty() {
            bail!("stack has no apps (declare at least one under `apps:`)");
        }
        for app in &self.apps {
            app.validate()?;
        }
        // Unique deployment identity (namespace/name).
        let mut keys = HashSet::new();
        for app in &self.apps {
            if !keys.insert(app.key()) {
                bail!("stack declares two apps with the same key '{}'", app.key());
            }
        }
        // Every dependency must name a service published by a sibling in the same namespace. A stack
        // is self-contained: a cross-stack/external dependency would never order-resolve here, and
        // silently treating an unknown name as "external" would mask typos. (To depend on a service
        // outside the stack, apply that deployment separately and reference it from a standalone
        // manifest, where no sibling-existence check applies.)
        let published: HashSet<(String, String)> = self
            .apps
            .iter()
            .filter(|a| !a.service.is_empty())
            .map(|a| (a.namespace.clone(), a.service.clone()))
            .collect();
        for app in &self.apps {
            for dep in &app.depends_on {
                if !published.contains(&(app.namespace.clone(), dep.service.clone())) {
                    bail!(
                        "app '{}' depends on service '{}', but no app in namespace '{}' publishes it",
                        app.name,
                        dep.service,
                        app.namespace
                    );
                }
            }
        }
        // Acyclicity.
        self.apply_order()?;
        Ok(())
    }

    /// The order in which to apply the apps so producers come up before consumers: a topological sort
    /// over `depends_on`. Returns the namespace-scoped keys (`namespace/name`) in apply order, or an
    /// error naming a cycle if the graph has one.
    ///
    /// Pure: depends only on the declared graph, never the mesh.
    pub fn apply_order(&self) -> Result<Vec<String>> {
        // Build the dependency edges keyed by deployment key. A dep names a *service*; map it to the
        // owning app's key within the same namespace.
        let mut service_owner: HashMap<(String, String), String> = HashMap::new();
        for app in &self.apps {
            if !app.service.is_empty() {
                service_owner.insert((app.namespace.clone(), app.service.clone()), app.key());
            }
        }
        // edges: key -> set of keys it depends on (must come first).
        let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for app in &self.apps {
            let entry = deps.entry(app.key()).or_default();
            for d in &app.depends_on {
                if let Some(owner) = service_owner.get(&(app.namespace.clone(), d.service.clone())) {
                    // Ignore a self-edge defensively (validate() already rejects self-deps).
                    if owner != &app.key() {
                        entry.insert(owner.clone());
                    }
                }
            }
        }
        topo_sort(&deps)
    }
}

/// Kahn's-algorithm topological sort over a `key -> {dependencies}` map. Returns keys such that
/// every key appears after all the keys it depends on. Deterministic (BTree ordering breaks ties).
/// Errors with the set of keys forming a cycle if one exists.
///
/// Pure and standalone so the controller's dependency logic and the stack ordering share one tested
/// implementation.
pub fn topo_sort(deps: &BTreeMap<String, BTreeSet<String>>) -> Result<Vec<String>> {
    // In-degree = number of unresolved dependencies for each node.
    let mut indegree: BTreeMap<String, usize> = BTreeMap::new();
    for (node, ds) in deps {
        indegree.entry(node.clone()).or_insert(0);
        for d in ds {
            // A dependency referenced but not itself a node still counts as a satisfied prerequisite
            // (it has no deps of its own); register it so it is emitted too.
            indegree.entry(d.clone()).or_insert(0);
        }
    }
    // Count how many nodes depend on each node, i.e. compute indegree = number of deps.
    for (node, ds) in deps {
        *indegree.get_mut(node).expect("node registered") = ds.len();
    }

    // Reverse edges: dependency -> dependents, so finishing a dep can decrement its dependents.
    let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (node, ds) in deps {
        for d in ds {
            dependents.entry(d.clone()).or_default().push(node.clone());
        }
    }

    // Queue of nodes with no remaining dependencies (indegree 0), processed in deterministic order.
    let mut ready: BTreeSet<String> =
        indegree.iter().filter(|(_, n)| **n == 0).map(|(k, _)| k.clone()).collect();
    let mut order = Vec::with_capacity(indegree.len());
    while let Some(node) = ready.iter().next().cloned() {
        ready.remove(&node);
        order.push(node.clone());
        if let Some(deps_of) = dependents.get(&node) {
            for dep in deps_of {
                if let Some(n) = indegree.get_mut(dep) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        ready.insert(dep.clone());
                    }
                }
            }
        }
    }

    if order.len() != indegree.len() {
        // The unprocessed nodes are exactly those on/behind a cycle.
        let stuck: Vec<String> =
            indegree.keys().filter(|k| !order.contains(k)).cloned().collect();
        bail!(
            "dependency cycle detected among: {} (depends_on must form a DAG)",
            stuck.join(", ")
        );
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DependencyRef, Resources, Strategy};
    use ce_rs::Amount;

    fn app(name: &str, service: &str, deps: &[&str]) -> Deployment {
        Deployment {
            name: name.into(),
            namespace: "default".into(),
            image: "nginx".into(),
            replicas: 1,
            resources: Resources { cpu_cores: 1, mem_mb: 64 },
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Strategy::default(),
            service: service.into(),
            depends_on: deps
                .iter()
                .map(|s| DependencyRef { service: (*s).into(), healthy: false })
                .collect(),
            ..Default::default()
        }
    }

    fn map(edges: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
        let mut m = BTreeMap::new();
        for (node, ds) in edges {
            m.insert((*node).to_string(), ds.iter().map(|s| (*s).to_string()).collect());
        }
        m
    }

    #[test]
    fn topo_orders_dependencies_first() {
        // web -> api -> db: db must precede api must precede web.
        let m = map(&[("web", &["api"]), ("api", &["db"]), ("db", &[])]);
        let order = topo_sort(&m).unwrap();
        let pos = |k: &str| order.iter().position(|x| x == k).unwrap();
        assert!(pos("db") < pos("api"));
        assert!(pos("api") < pos("web"));
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn topo_is_deterministic() {
        let m = map(&[("web", &["db"]), ("api", &["db"]), ("db", &[])]);
        assert_eq!(topo_sort(&m).unwrap(), topo_sort(&m).unwrap());
    }

    #[test]
    fn topo_handles_independent_nodes() {
        let m = map(&[("a", &[]), ("b", &[]), ("c", &[])]);
        let order = topo_sort(&m).unwrap();
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn topo_registers_referenced_only_dependency() {
        // "base" is referenced as a dependency but is not itself a keyed node; it must still appear,
        // and before its dependent.
        let m = map(&[("app", &["base"])]);
        let order = topo_sort(&m).unwrap();
        assert_eq!(order, vec!["base".to_string(), "app".to_string()]);
    }

    #[test]
    fn topo_detects_direct_cycle() {
        let m = map(&[("a", &["b"]), ("b", &["a"])]);
        let err = topo_sort(&m).unwrap_err().to_string();
        assert!(err.contains("cycle"), "got: {err}");
        assert!(err.contains('a') && err.contains('b'));
    }

    #[test]
    fn topo_detects_self_cycle() {
        let m = map(&[("a", &["a"])]);
        assert!(topo_sort(&m).is_err());
    }

    #[test]
    fn topo_detects_longer_cycle() {
        let m = map(&[("a", &["b"]), ("b", &["c"]), ("c", &["a"]), ("d", &[])]);
        let err = topo_sort(&m).unwrap_err();
        let s = err.to_string();
        assert!(s.contains("cycle"));
        // d is not in the cycle and must not be reported as stuck.
        assert!(!s.contains(", d") && !s.contains("d,"), "d wrongly reported: {s}");
    }

    #[test]
    fn stack_apply_order_resolves_services() {
        // web depends_on service "api-svc" (published by app "api"), api depends_on "db-svc" (db).
        let stack = Stack {
            namespace: None,
            apps: vec![
                app("web", "web-svc", &["api-svc"]),
                app("api", "api-svc", &["db-svc"]),
                app("db", "db-svc", &[]),
            ],
        };
        let order = stack.apply_order().unwrap();
        let pos = |k: &str| order.iter().position(|x| x == k).unwrap();
        assert!(pos("default/db") < pos("default/api"));
        assert!(pos("default/api") < pos("default/web"));
    }

    #[test]
    fn stack_rejects_cycle() {
        let stack = Stack {
            namespace: None,
            apps: vec![
                app("a", "a-svc", &["b-svc"]),
                app("b", "b-svc", &["a-svc"]),
            ],
        };
        assert!(stack.apply_order().is_err());
        assert!(stack.validate().is_err());
    }

    #[test]
    fn stack_rejects_dependency_on_unknown_service() {
        let stack = Stack {
            namespace: None,
            apps: vec![app("web", "web-svc", &["ghost-svc"])],
        };
        let err = stack.validate().unwrap_err().to_string();
        assert!(err.contains("ghost-svc"), "got: {err}");
    }

    #[test]
    fn stack_rejects_duplicate_keys() {
        let stack = Stack {
            namespace: None,
            apps: vec![app("web", "w1", &[]), app("web", "w2", &[])],
        };
        assert!(stack.validate().is_err());
    }

    #[test]
    fn stack_namespace_pushes_down() {
        let yaml = r#"
namespace: prod
apps:
  - name: db
    image: postgres
    replicas: 1
    service: db
  - name: web
    image: nginx
    replicas: 1
    service: web
    namespace: special
    depends_on:
      - service: db
"#;
        // db inherits prod; web kept its explicit namespace, so its depends_on points at a service
        // not published in `special` -> validation should fail (a clear, early error).
        let res = Stack::from_manifest(yaml);
        assert!(res.is_err(), "cross-namespace dep should be rejected");
    }

    #[test]
    fn stack_parses_and_orders_from_manifest() {
        let yaml = r#"
namespace: prod
apps:
  - name: web
    image: nginx
    replicas: 2
    service: web
    depends_on:
      - service: db
        healthy: true
  - name: db
    image: postgres
    replicas: 1
    service: db
"#;
        let stack = Stack::from_manifest(yaml).unwrap();
        assert_eq!(stack.apps.len(), 2);
        assert!(stack.apps.iter().all(|a| a.namespace == "prod"));
        let order = stack.apply_order().unwrap();
        let pos = |k: &str| order.iter().position(|x| x == k).unwrap();
        assert!(pos("prod/db") < pos("prod/web"));
    }

    #[test]
    fn stack_rejects_empty() {
        assert!(Stack::from_manifest("apps: []").is_err());
    }
}
