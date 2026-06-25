//! Pure distributed-coordination state: **leases** (ref-counting a dependency) and **claims**
//! (singleton ownership). No I/O — these are the rules the registry stores and every node agrees on.
//!
//! - **Lease**: a dependent renews a TTL lease while it needs a dependency. The dependency stays up
//!   while any live lease exists and self-terminates after the last one lapses + grace. The lease IS
//!   the distributed ref-count — no central registry of "who depends on what".
//! - **Claim**: who owns (is responsible for spawning/healing) a singleton service. On a simultaneous
//!   double-claim the [`winner`] is deterministic (lowest node id), so the loser observes the
//!   converged set and releases — at worst a brief transient duplicate that self-heals. Hard global
//!   uniqueness is intentionally NOT promised here (see docs/ISSUES.md).

use serde::{Deserialize, Serialize};

/// A 64-hex node id (the holder/owner identity).
pub type NodeId = String;

/// Unix seconds now.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A renewable TTL lease: `holder` depends on `service` until `expires_at`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub service: String,
    pub holder: NodeId,
    pub expires_at: u64,
}

impl Lease {
    /// A lease on `service` by `holder`, valid for `ttl_secs` from `now`.
    pub fn new(service: &str, holder: &str, ttl_secs: u64, now: u64) -> Self {
        Lease { service: service.to_string(), holder: holder.to_string(), expires_at: now.saturating_add(ttl_secs.max(1)) }
    }
    /// Has this lease lapsed at `now`?
    pub fn expired(&self, now: u64) -> bool {
        now >= self.expires_at
    }
}

/// Number of *live* (non-expired) holders of a service at `now` — the distributed ref-count. A
/// dependency owner tears the service down only when this reaches zero (plus a grace window).
pub fn live_holders(leases: &[Lease], now: u64) -> usize {
    leases.iter().filter(|l| !l.expired(now)).count()
}

/// A singleton-ownership claim: `owner` is responsible for `service` until `expires_at` (renewed
/// while alive; lapses if the owner dies, letting a peer take over).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub service: String,
    pub owner: NodeId,
    pub expires_at: u64,
}

impl Claim {
    pub fn new(service: &str, owner: &str, ttl_secs: u64, now: u64) -> Self {
        Claim { service: service.to_string(), owner: owner.to_string(), expires_at: now.saturating_add(ttl_secs.max(1)) }
    }
    pub fn expired(&self, now: u64) -> bool {
        now >= self.expires_at
    }
}

/// The deterministic owner among `claims` at `now`: the live claim with the **lowest node id**.
/// Every node folds the same converged set the same way, so they agree on one owner without a lock.
pub fn winner(claims: &[Claim], now: u64) -> Option<&Claim> {
    claims.iter().filter(|c| !c.expired(now)).min_by(|a, b| a.owner.cmp(&b.owner))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_expiry() {
        let l = Lease::new("svc", "node-a", 60, 1000);
        assert_eq!(l.expires_at, 1060);
        assert!(!l.expired(1059));
        assert!(l.expired(1060));
        assert!(l.expired(2000));
    }

    #[test]
    fn live_holders_counts_only_unexpired() {
        let now = 1000;
        let leases = vec![
            Lease::new("svc", "a", 100, now), // expires 1100
            Lease::new("svc", "b", 5, now),   // expires 1005
            Lease::new("svc", "c", 100, now),
        ];
        assert_eq!(live_holders(&leases, 1004), 3);
        assert_eq!(live_holders(&leases, 1006), 2); // b lapsed
        assert_eq!(live_holders(&leases, 2000), 0); // all lapsed -> tear down
    }

    #[test]
    fn winner_is_lowest_id_and_ignores_expired() {
        let now = 1000;
        let claims = vec![
            Claim::new("svc", "cccc", 100, now),
            Claim::new("svc", "aaaa", 100, now),
            Claim::new("svc", "bbbb", 100, now),
        ];
        assert_eq!(winner(&claims, now).unwrap().owner, "aaaa");

        // aaaa's claim lapses -> bbbb wins (takeover on owner death)
        let claims2 = vec![
            Claim::new("svc", "aaaa", 5, now), // expires 1005
            Claim::new("svc", "bbbb", 100, now),
        ];
        assert_eq!(winner(&claims2, 1006).unwrap().owner, "bbbb");
        assert!(winner(&[], now).is_none());
    }

    #[test]
    fn winner_is_deterministic_regardless_of_order() {
        let now = 0;
        let a = vec![Claim::new("s", "z", 9, now), Claim::new("s", "a", 9, now)];
        let b = vec![Claim::new("s", "a", 9, now), Claim::new("s", "z", 9, now)];
        assert_eq!(winner(&a, now).unwrap().owner, winner(&b, now).unwrap().owner);
    }
}
