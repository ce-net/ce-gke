//! The distributed-state **seam**: where leases + claims live so every node converges on them.
//!
//! [`Registry`] is a trait so the storage backend is swappable. The production backend is
//! **ce-coord** (the off-chain coordination layer — a `Merged` registry per service, gossiped over
//! the mesh, snapshotted to blobs); it is NOT the chain — app orchestration never touches consensus.
//! [`MemRegistry`] is the in-process implementation used for a single node and for tests; it proves
//! the SDK's lease/claim flow without a live mesh. (`CoordRegistry` is the next concrete step.)
//!
//! The trait returns `impl Future + Send` (matching [`crate::MeshDriver`]) so handles can
//! renew/release leases from background tasks.

use anyhow::Result;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use super::coord::{Claim, Lease};

/// Where leases + claims are stored and converged. Implementations must be cheaply cloneable and
/// `Send + Sync + 'static` so a [`crate::sdk::Handle`] can renew its lease from a spawned task.
pub trait Registry: Clone + Send + Sync + 'static {
    /// Record (or refresh) a lease — upsert by `(service, holder)`.
    fn put_lease(&self, lease: Lease) -> impl Future<Output = Result<()>> + Send;
    /// Drop this holder's lease on `service` (fast release; otherwise it lapses by TTL).
    fn drop_lease(&self, service: &str, holder: &str) -> impl Future<Output = Result<()>> + Send;
    /// All leases currently recorded for `service` (the caller filters expired via the clock).
    fn leases(&self, service: &str) -> impl Future<Output = Result<Vec<Lease>>> + Send;
    /// Record (or refresh) an ownership claim — upsert by `(service, owner)`.
    fn put_claim(&self, claim: Claim) -> impl Future<Output = Result<()>> + Send;
    /// All claims currently recorded for `service`.
    fn claims(&self, service: &str) -> impl Future<Output = Result<Vec<Claim>>> + Send;
}

/// In-process registry (single node / tests). Distributed convergence is the ce-coord backend's job.
#[derive(Clone, Default)]
pub struct MemRegistry {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    leases: HashMap<String, Vec<Lease>>,
    claims: HashMap<String, Vec<Claim>>,
}

impl MemRegistry {
    pub fn new() -> Self {
        MemRegistry::default()
    }
}

impl Registry for MemRegistry {
    fn put_lease(&self, lease: Lease) -> impl Future<Output = Result<()>> + Send {
        let inner = self.inner.clone();
        async move {
            let mut g = inner.lock().unwrap();
            let v = g.leases.entry(lease.service.clone()).or_default();
            v.retain(|l| l.holder != lease.holder);
            v.push(lease);
            Ok(())
        }
    }
    fn drop_lease(&self, service: &str, holder: &str) -> impl Future<Output = Result<()>> + Send {
        let inner = self.inner.clone();
        let service = service.to_string();
        let holder = holder.to_string();
        async move {
            let mut g = inner.lock().unwrap();
            if let Some(v) = g.leases.get_mut(&service) {
                v.retain(|l| l.holder != holder);
            }
            Ok(())
        }
    }
    fn leases(&self, service: &str) -> impl Future<Output = Result<Vec<Lease>>> + Send {
        let inner = self.inner.clone();
        let service = service.to_string();
        async move { Ok(inner.lock().unwrap().leases.get(&service).cloned().unwrap_or_default()) }
    }
    fn put_claim(&self, claim: Claim) -> impl Future<Output = Result<()>> + Send {
        let inner = self.inner.clone();
        async move {
            let mut g = inner.lock().unwrap();
            let v = g.claims.entry(claim.service.clone()).or_default();
            v.retain(|c| c.owner != claim.owner);
            v.push(claim);
            Ok(())
        }
    }
    fn claims(&self, service: &str) -> impl Future<Output = Result<Vec<Claim>>> + Send {
        let inner = self.inner.clone();
        let service = service.to_string();
        async move { Ok(inner.lock().unwrap().claims.get(&service).cloned().unwrap_or_default()) }
    }
}

#[cfg(test)]
mod tests {
    use super::super::coord::now_secs;
    use super::*;

    #[tokio::test]
    async fn mem_registry_lease_upsert_and_drop() {
        let r = MemRegistry::new();
        let now = now_secs();
        r.put_lease(Lease::new("svc", "a", 60, now)).await.unwrap();
        r.put_lease(Lease::new("svc", "b", 60, now)).await.unwrap();
        r.put_lease(Lease::new("svc", "a", 120, now)).await.unwrap(); // upsert, no dup
        let ls = r.leases("svc").await.unwrap();
        assert_eq!(ls.len(), 2);
        r.drop_lease("svc", "a").await.unwrap();
        let ls = r.leases("svc").await.unwrap();
        assert_eq!(ls.len(), 1);
        assert_eq!(ls[0].holder, "b");
    }

    #[tokio::test]
    async fn mem_registry_claims_upsert() {
        let r = MemRegistry::new();
        let now = now_secs();
        r.put_claim(Claim::new("svc", "z", 60, now)).await.unwrap();
        r.put_claim(Claim::new("svc", "a", 60, now)).await.unwrap();
        r.put_claim(Claim::new("svc", "a", 90, now)).await.unwrap(); // upsert
        let cs = r.claims("svc").await.unwrap();
        assert_eq!(cs.len(), 2);
        assert_eq!(super::super::coord::winner(&cs, now).unwrap().owner, "a");
    }
}
