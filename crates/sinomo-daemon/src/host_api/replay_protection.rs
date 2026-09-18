use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;

/// Host-agnostic replay protection for authenticated request middleware.
///
/// A scope normally identifies one daemon data root. Keeping scope selection
/// in the host prevents nonce traffic for independent daemon instances from
/// interfering while this owner retains the expiry-index and replay policy.
#[derive(Debug)]
pub struct ScopedReplayNonceCache<Scope> {
    scopes: Mutex<HashMap<Scope, ExpiringNonceSet>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("replay nonce cache lock is poisoned")]
pub struct ReplayNonceCachePoisoned;

#[derive(Debug, Default)]
struct ExpiringNonceSet {
    // Membership and expiry ordering are separate so pruning never scans the
    // complete live set for every authenticated request.
    nonces: HashSet<Arc<[u8]>>,
    expirations: BinaryHeap<Reverse<(OffsetDateTime, Arc<[u8]>)>>,
}

impl<Scope> Default for ScopedReplayNonceCache<Scope> {
    fn default() -> Self {
        Self {
            scopes: Mutex::new(HashMap::new()),
        }
    }
}

impl<Scope> ScopedReplayNonceCache<Scope>
where
    Scope: Eq + Hash,
{
    /// Remembers a nonce until `expires_at`.
    ///
    /// Returns `false` when the same nonce is already live in this scope. An
    /// expired nonce may be accepted again once its expiry is strictly before
    /// `now`, matching the signed-command freshness boundary.
    pub fn remember(
        &self,
        scope: Scope,
        nonce: Vec<u8>,
        expires_at: OffsetDateTime,
        now: OffsetDateTime,
    ) -> Result<bool, ReplayNonceCachePoisoned> {
        let mut scopes = self.scopes.lock().map_err(|_| ReplayNonceCachePoisoned)?;
        Ok(scopes
            .entry(scope)
            .or_default()
            .remember(nonce, expires_at, now))
    }
}

impl ExpiringNonceSet {
    fn remember(
        &mut self,
        nonce: Vec<u8>,
        expires_at: OffsetDateTime,
        now: OffsetDateTime,
    ) -> bool {
        self.prune_expired(now);
        let nonce = Arc::<[u8]>::from(nonce);
        if !self.nonces.insert(Arc::clone(&nonce)) {
            return false;
        }
        self.expirations.push(Reverse((expires_at, nonce)));
        true
    }

    fn prune_expired(&mut self, now: OffsetDateTime) {
        while self
            .expirations
            .peek()
            .is_some_and(|Reverse((expires_at, _))| now > *expires_at)
        {
            let Reverse((_expires_at, nonce)) =
                self.expirations.pop().expect("checked nonce expiration");
            self.nonces.remove(&nonce);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_nonce_is_rejected_and_expired_nonce_can_be_reused() {
        let cache = ScopedReplayNonceCache::default();
        let now = OffsetDateTime::now_utc();
        let expiry = now + time::Duration::minutes(10);

        assert!(
            cache
                .remember("daemon-a", b"nonce".to_vec(), expiry, now)
                .expect("remember nonce")
        );
        assert!(
            !cache
                .remember("daemon-a", b"nonce".to_vec(), expiry, now)
                .expect("reject replay")
        );
        assert!(
            cache
                .remember(
                    "daemon-a",
                    b"nonce".to_vec(),
                    expiry + time::Duration::minutes(10),
                    expiry + time::Duration::nanoseconds(1),
                )
                .expect("reuse expired nonce")
        );
    }

    #[test]
    fn nonce_membership_is_isolated_by_scope() {
        let cache = ScopedReplayNonceCache::default();
        let now = OffsetDateTime::now_utc();
        let expiry = now + time::Duration::minutes(10);

        assert!(
            cache
                .remember("daemon-a", b"nonce".to_vec(), expiry, now)
                .expect("first scope")
        );
        assert!(
            cache
                .remember("daemon-b", b"nonce".to_vec(), expiry, now)
                .expect("second scope")
        );
    }

    #[test]
    fn nonce_bookkeeping_stays_incremental_at_high_cardinality() {
        const LEGACY_PRUNE_TRIGGER: usize = 8_192;
        const EXTRA_NONCES: usize = 1_024;

        let now = OffsetDateTime::now_utc();
        let expires_at = now + time::Duration::minutes(10);
        let mut nonces = ExpiringNonceSet::default();
        for index in 0..LEGACY_PRUNE_TRIGGER + EXTRA_NONCES {
            assert!(nonces.remember(format!("nonce-{index}").into_bytes(), expires_at, now));
        }

        assert_eq!(nonces.nonces.len(), LEGACY_PRUNE_TRIGGER + EXTRA_NONCES);
        assert_eq!(
            nonces.expirations.len(),
            LEGACY_PRUNE_TRIGGER + EXTRA_NONCES
        );
        assert!(!nonces.remember(b"nonce-0".to_vec(), expires_at, now));

        let after_expiry = expires_at + time::Duration::nanoseconds(1);
        assert!(nonces.remember(
            b"post-expiry".to_vec(),
            after_expiry + time::Duration::minutes(10),
            after_expiry,
        ));
        assert_eq!(nonces.nonces.len(), 1);
        assert_eq!(nonces.expirations.len(), 1);
    }
}
