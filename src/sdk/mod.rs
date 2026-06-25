//! # The per-app dependency SDK
//!
//! An app orchestrates its **own** dependencies from code — there is no global controller and no
//! `stack.yaml`. Each app declares its *direct* dependencies and the SDK **ensures** them:
//! locate-or-claim-or-spawn, then hold a renewing **lease** (the distributed ref-count). A
//! dependency's owner tears it down only when no live lease remains. The dependency graph spans the
//! mesh emergently — each app only ever knows its direct neighbors, so it scales to millions of nodes.
//!
//! This is the in-code counterpart to the (now-legacy) `stack.yaml` + central reconciler: it reuses
//! the same planners ([`crate::placement`], [`crate::rollout`], [`crate::deps`]) and mesh driver
//! ([`crate::MeshDriver`]), but stores leases/claims in a distributed [`Registry`] (production:
//! ce-coord; here: [`MemRegistry`]) and lets each app drive its own neighborhood.
//!
//! ```no_run
//! use ce_gke::{Gke, Dep, Place};
//! use std::time::Duration;
//! # async fn demo() -> anyhow::Result<()> {
//! let gke = Gke::local("<my-node-id>");                  // CeDriver + MemRegistry over the local node
//! let enc = gke.ensure(
//!     Dep::new("cast-encoder").namespace("cast")
//!         .image("ce-net/cast-encoder:latest")
//!         .place(Place::Tag("gpu".into()))
//!         .health_tcp(8080)
//!         .singleton()
//!         .lease_ttl(Duration::from_secs(60)),
//! ).await?;
//! gke.advertise("ce-gke/cast/cast-control").await?;       // so my dependents can find + lease me
//! # let _ = enc; Ok(()) }
//! ```

pub mod coord;
pub mod dep;
pub mod registry;

pub use coord::{Claim, Lease};
pub use dep::{Dep, Place};
pub use registry::{MemRegistry, Registry};

use crate::{CeDriver, DepReadiness, MeshDriver};
use anyhow::{Result, anyhow};
use std::time::Duration;

/// The per-app dependency SDK. One per app; it ensures that app's own dependencies.
pub struct Gke<D: MeshDriver, R: Registry> {
    driver: D,
    registry: R,
    me: String,
}

impl Gke<CeDriver, MemRegistry> {
    /// Convenience: drive the local CE node (`http://127.0.0.1:8844`) with an in-process registry —
    /// good for a single node / development. The distributed registry (ce-coord) is the production
    /// backend; pass it via [`Gke::new`] instead.
    pub fn local(me: impl Into<String>) -> Self {
        Gke::new(CeDriver::new("http://127.0.0.1:8844"), MemRegistry::new(), me)
    }
}

impl<D: MeshDriver, R: Registry> Gke<D, R> {
    /// Build the SDK over a mesh `driver` and a distributed `registry`, identifying as node `me`.
    pub fn new(driver: D, registry: R, me: impl Into<String>) -> Self {
        Gke { driver, registry, me: me.into() }
    }

    /// Is `dep` currently running + advertised on the mesh?
    pub async fn locate(&self, dep: &Dep) -> Result<DepReadiness> {
        self.driver.service_ready(&dep.service()).await
    }

    /// Advertise that THIS node provides `service`, so dependents can locate + lease it.
    pub async fn advertise(&self, service: &str) -> Result<()> {
        self.driver.advertise_service(service).await
    }

    /// Ensure `dep` is running and lease it. Already up → just lease. Absent → for a singleton,
    /// claim ownership and let the deterministic [`coord::winner`] spawn it (losers lease the
    /// winner's instance); for a replicated dep, spawn it. Returns a [`Handle`] that renews the
    /// lease while held and releases it on drop.
    pub async fn ensure(&self, dep: Dep) -> Result<Handle<R>> {
        let service = dep.service();
        let ttl = dep.lease_ttl_secs;
        let now = coord::now_secs();

        // I depend on it → record my lease regardless of who ends up spawning it.
        self.registry.put_lease(Lease::new(&service, &self.me, ttl, now)).await?;

        let readiness = self.driver.service_ready(&service).await.unwrap_or(DepReadiness::Absent);
        if matches!(readiness, DepReadiness::Absent) {
            if dep.singleton {
                self.registry.put_claim(Claim::new(&service, &self.me, ttl, now)).await?;
                let claims = self.registry.claims(&service).await?;
                let owner = coord::winner(&claims, now).map(|c| c.owner.clone());
                if owner.as_deref() == Some(self.me.as_str()) {
                    self.spawn(&dep).await?;
                }
                // a loser does not spawn — the converged owner does; locate finds it shortly.
            } else {
                self.spawn(&dep).await?;
            }
        }

        Ok(Handle::start(self.registry.clone(), service, self.me.clone(), ttl))
    }

    /// Place `dep`'s cell(s) and advertise its service.
    async fn spawn(&self, dep: &Dep) -> Result<()> {
        let deployment = dep.to_deployment()?;
        let host = self.pick_host(dep).await?;
        for _ in 0..dep.replicas.max(1) {
            self.driver.deploy(&host, &deployment, None).await?;
        }
        let _ = self.driver.advertise_service(&dep.service()).await;
        Ok(())
    }

    /// Where to place `dep`: an explicit node wins; otherwise the first atlas host (ranking by
    /// tag/capacity via [`crate::placement::rank`] is the documented refinement).
    async fn pick_host(&self, dep: &Dep) -> Result<String> {
        if let Place::Node(n) = &dep.place {
            return Ok(n.clone());
        }
        let atlas = self.driver.atlas().await?;
        atlas
            .first()
            .map(|e| e.node_id.clone())
            .ok_or_else(|| anyhow!("no atlas host available to place '{}'", dep.name))
    }

    /// Wait until `dep`'s service is healthy, or `timeout` elapses. Returns whether it became ready.
    pub async fn wait_ready(&self, dep: &Dep, timeout: Duration) -> Result<bool> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if matches!(self.driver.service_ready(&dep.service()).await?, DepReadiness::Healthy) {
                return Ok(true);
            }
            if std::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// A held dependency lease. While alive it renews the lease in the background; on drop it releases
/// the lease so the dependency's owner can reclaim it when no holder remains. (If the release task
/// doesn't run, the lease lapses by TTL anyway — the lease is the correctness boundary.)
pub struct Handle<R: Registry> {
    service: String,
    holder: String,
    registry: R,
    renew: tokio::task::JoinHandle<()>,
}

impl<R: Registry> Handle<R> {
    fn start(registry: R, service: String, holder: String, ttl: u64) -> Self {
        let r = registry.clone();
        let s = service.clone();
        let h = holder.clone();
        let renew = tokio::spawn(async move {
            let period = Duration::from_secs((ttl / 2).max(1));
            loop {
                tokio::time::sleep(period).await;
                let now = coord::now_secs();
                let _ = r.put_lease(Lease::new(&s, &h, ttl, now)).await;
            }
        });
        Handle { service, holder, registry, renew }
    }

    /// The mesh service name of the dependency this handle holds.
    pub fn service(&self) -> &str {
        &self.service
    }
}

impl<R: Registry> Drop for Handle<R> {
    fn drop(&mut self) {
        self.renew.abort();
        let r = self.registry.clone();
        let s = self.service.clone();
        let h = self.holder.clone();
        tokio::spawn(async move {
            let _ = r.drop_lease(&s, &h).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Deployment, Phase, Probe};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct MockDriver {
        deploys: Arc<Mutex<Vec<(String, String)>>>, // (deployment name, node)
        ready: Arc<Mutex<DepReadiness>>,
    }
    impl MockDriver {
        fn with(ready: DepReadiness) -> Self {
            MockDriver { deploys: Arc::new(Mutex::new(vec![])), ready: Arc::new(Mutex::new(ready)) }
        }
    }
    impl MeshDriver for MockDriver {
        async fn atlas(&self) -> Result<Vec<ce_rs::AtlasEntry>> {
            Ok(vec![])
        }
        async fn deploy(&self, node_id: &str, d: &Deployment, _g: Option<&str>) -> Result<String> {
            self.deploys.lock().unwrap().push((d.name.clone(), node_id.to_string()));
            Ok("job-1".into())
        }
        async fn kill(&self, _n: &str, _j: &str, _g: Option<&str>) -> Result<()> {
            Ok(())
        }
        async fn phase(&self, _n: &str, _j: &str, _p: Option<&Probe>, _g: Option<&str>) -> Result<Phase> {
            Ok(Phase::Running)
        }
        async fn service_ready(&self, _s: &str) -> Result<DepReadiness> {
            Ok(self.ready.lock().unwrap().clone())
        }
    }

    #[tokio::test]
    async fn ensure_singleton_spawns_when_absent_and_i_win() {
        let driver = MockDriver::with(DepReadiness::Absent);
        let gke = Gke::new(driver.clone(), MemRegistry::new(), "aaaa");
        let dep = Dep::new("enc").namespace("cast").image("img").place_node("bbbbnode").singleton();
        let _h = gke.ensure(dep).await.unwrap();
        let d = driver.deploys.lock().unwrap();
        assert_eq!(d.len(), 1, "sole claimant must spawn the singleton");
        assert_eq!(d[0].1, "bbbbnode");
    }

    #[tokio::test]
    async fn ensure_already_running_does_not_spawn() {
        let driver = MockDriver::with(DepReadiness::Healthy);
        let gke = Gke::new(driver.clone(), MemRegistry::new(), "aaaa");
        let dep = Dep::new("enc").image("img").place_node("b");
        let _h = gke.ensure(dep).await.unwrap();
        assert!(driver.deploys.lock().unwrap().is_empty(), "running dep must not be re-spawned");
    }

    #[tokio::test]
    async fn ensure_records_a_lease() {
        let driver = MockDriver::with(DepReadiness::Healthy);
        let reg = MemRegistry::new();
        let gke = Gke::new(driver, reg.clone(), "aaaa");
        let dep = Dep::new("enc").namespace("cast").image("img").place_node("b");
        let svc = dep.service();
        let _h = gke.ensure(dep).await.unwrap();
        let leases = reg.leases(&svc).await.unwrap();
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].holder, "aaaa");
    }
}
