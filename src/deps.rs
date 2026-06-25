//! Dependency gate — the pure decision of whether a deployment may be placed yet, given the
//! observed readiness of the services it `depends_on`.
//!
//! The mesh lookups (does service X have a live/healthy instance?) live in the driver
//! ([`crate::driver::MeshDriver::service_ready`], backed by [`ce_rs::locate`]); the *decision* made
//! from their results is pure and lives here so the `ready` vs `waiting` transition is unit-tested
//! offline. The controller calls [`Gate::evaluate`] once per tick with the resolved readiness of
//! each dependency and either proceeds with reconcile (deps met) or marks the deployment `waiting`
//! and places nothing (a dep is unmet). When a previously-met dependency disappears, the next tick's
//! readiness map flips it to unmet and the dependent goes back to `waiting` — convergence handles
//! the rest with no special teardown path.

use crate::spec::Deployment;

/// Observed readiness of a single dependency service this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepReadiness {
    /// No live instance advertised at all.
    Absent,
    /// At least one live instance advertised, but none confirmed healthy (readiness-probing).
    LivePresent,
    /// At least one live instance confirmed healthy.
    Healthy,
}

/// The outcome of the dependency gate for one deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Gate {
    /// All dependencies are satisfied; the deployment may reconcile/place this tick.
    Ready,
    /// One or more dependencies are unmet; hold the deployment, placing nothing. Carries the
    /// human-readable list of unmet dependency service names for status/logging.
    Waiting {
        /// Bare service names of the unmet dependencies (sorted, deduped) — for the `waiting` reason.
        unmet: Vec<String>,
    },
}

impl Gate {
    /// Is the gate open (deployment may place replicas)?
    pub fn is_ready(&self) -> bool {
        matches!(self, Gate::Ready)
    }

    /// Decide the gate for `d` given a resolver that returns each dependency's observed readiness.
    ///
    /// `readiness(dep_service)` is the per-tick mesh observation (the driver fills it via
    /// `ce_rs::locate`). A deployment with no `depends_on` is always [`Gate::Ready`]. A dependency
    /// with `healthy: false` is met by [`DepReadiness::LivePresent`] or [`DepReadiness::Healthy`];
    /// one with `healthy: true` requires [`DepReadiness::Healthy`].
    ///
    /// Pure: the resolver is the only window onto the mesh, and the same inputs always yield the same
    /// gate, so the `ready`/`waiting` transitions are fully testable with a stub resolver.
    pub fn evaluate(d: &Deployment, mut readiness: impl FnMut(&str) -> DepReadiness) -> Gate {
        let mut unmet: Vec<String> = Vec::new();
        for dep in &d.depends_on {
            let observed = readiness(&dep.service);
            let met = match (dep.healthy, observed) {
                (_, DepReadiness::Absent) => false,
                (false, DepReadiness::LivePresent | DepReadiness::Healthy) => true,
                (true, DepReadiness::Healthy) => true,
                (true, DepReadiness::LivePresent) => false,
            };
            if !met {
                unmet.push(dep.service.clone());
            }
        }
        if unmet.is_empty() {
            Gate::Ready
        } else {
            unmet.sort();
            unmet.dedup();
            Gate::Waiting { unmet }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DependencyRef, Resources, Strategy};
    use ce_rs::Amount;

    fn deploy(deps: &[(&str, bool)]) -> Deployment {
        Deployment {
            name: "web".into(),
            namespace: "default".into(),
            image: "nginx".into(),
            replicas: 1,
            resources: Resources { cpu_cores: 1, mem_mb: 64 },
            bid: Amount::from_credits(1),
            duration_secs: 60,
            strategy: Strategy::default(),
            depends_on: deps
                .iter()
                .map(|(s, h)| DependencyRef { service: (*s).into(), healthy: *h })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn no_deps_is_always_ready() {
        let d = deploy(&[]);
        assert!(Gate::evaluate(&d, |_| DepReadiness::Absent).is_ready());
    }

    #[test]
    fn present_satisfies_unhealthy_dep() {
        let d = deploy(&[("db", false)]);
        assert!(Gate::evaluate(&d, |_| DepReadiness::LivePresent).is_ready());
        assert!(Gate::evaluate(&d, |_| DepReadiness::Healthy).is_ready());
    }

    #[test]
    fn absent_dep_waits() {
        let d = deploy(&[("db", false)]);
        let g = Gate::evaluate(&d, |_| DepReadiness::Absent);
        assert_eq!(g, Gate::Waiting { unmet: vec!["db".to_string()] });
        assert!(!g.is_ready());
    }

    #[test]
    fn healthy_required_not_satisfied_by_merely_present() {
        let d = deploy(&[("db", true)]);
        assert_eq!(
            Gate::evaluate(&d, |_| DepReadiness::LivePresent),
            Gate::Waiting { unmet: vec!["db".to_string()] }
        );
        assert!(Gate::evaluate(&d, |_| DepReadiness::Healthy).is_ready());
    }

    #[test]
    fn reports_all_unmet_sorted() {
        let d = deploy(&[("cache", false), ("db", true)]);
        // cache present (ok), db only present but requires healthy (unmet).
        let g = Gate::evaluate(&d, |s| match s {
            "cache" => DepReadiness::LivePresent,
            "db" => DepReadiness::LivePresent,
            _ => DepReadiness::Absent,
        });
        assert_eq!(g, Gate::Waiting { unmet: vec!["db".to_string()] });

        // both unmet -> sorted list.
        let g = Gate::evaluate(&d, |_| DepReadiness::Absent);
        assert_eq!(g, Gate::Waiting { unmet: vec!["cache".to_string(), "db".to_string()] });
    }

    #[test]
    fn dependency_disappearing_flips_back_to_waiting() {
        let d = deploy(&[("db", false)]);
        // tick 1: db present -> ready
        assert!(Gate::evaluate(&d, |_| DepReadiness::LivePresent).is_ready());
        // tick 2: db gone -> waiting (the per-tick resolver is the only state; no special teardown)
        assert!(!Gate::evaluate(&d, |_| DepReadiness::Absent).is_ready());
    }
}
