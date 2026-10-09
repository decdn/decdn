//! The hand-off from the browser to the CLI: `POST /v1/fund` stores the
//! capability it got for a client, and `GET /v1/capability` hands it to the
//! polling CLI. In memory: a restart loses only hand-offs in flight, and the
//! user passes the gate again.

use std::collections::HashMap;
use std::sync::Mutex;

use alloy_primitives::Address;
use sponsord_api::IssuedCapability;

/// How long a capability stays here after it is issued. The CLI polls for
/// at most 10 minutes; past this, `POST /v1/fund` asks the daemon again,
/// which hands a registered signer its same capability back.
pub const HANDOFF_SECS: u64 = 30 * 60;

/// Most capabilities held at once; at the bound, the oldest is dropped.
pub const MAX_GRANTS: usize = 100_000;

struct Entry {
    capability: IssuedCapability,
    /// Unix time past which the entry is dropped: the hand-off window or the
    /// capability's own expiry, whichever is first.
    evict_at: u64,
}

/// Capabilities issued by `POST /v1/fund`, keyed by client signer, until
/// the CLI polls them or they expire.
pub struct GrantCache {
    entries: Mutex<HashMap<Address, Entry>>,
    handoff_secs: u64,
    max_entries: usize,
}

// By hand: the entries are live capability tokens.
impl std::fmt::Debug for GrantCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrantCache")
            .field("handoff_secs", &self.handoff_secs)
            .field("max_entries", &self.max_entries)
            .finish_non_exhaustive()
    }
}

impl Default for GrantCache {
    fn default() -> Self {
        Self::new(HANDOFF_SECS, MAX_GRANTS)
    }
}

impl GrantCache {
    /// An empty cache holding each capability for at most `handoff_secs` and
    /// at most `max_entries` (at least 1) at once.
    #[must_use]
    pub fn new(handoff_secs: u64, max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            handoff_secs,
            max_entries: max_entries.max(1),
        }
    }

    /// The capability held for `client`, unless it has been dropped or has
    /// expired by `now`.
    pub fn get(&self, client: Address, now: u64) -> Option<IssuedCapability> {
        let entries = self.lock();
        entries
            .get(&client)
            .filter(|e| now < e.evict_at)
            .map(|e| e.capability.clone())
    }

    /// Hold `capability` for `client`, replacing any earlier one.
    pub fn put(&self, client: Address, capability: IssuedCapability, now: u64) {
        let evict_at = capability.expiry.min(now.saturating_add(self.handoff_secs));
        let mut entries = self.lock();
        if entries.len() >= self.max_entries && !entries.contains_key(&client) {
            entries.retain(|_, e| now < e.evict_at);
            if entries.len() >= self.max_entries
                && let Some(oldest) = entries
                    .iter()
                    .min_by_key(|(_, e)| e.evict_at)
                    .map(|(k, _)| *k)
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            client,
            Entry {
                capability,
                evict_at,
            },
        );
    }

    /// Drop every entry past its time. `put` also sweeps when it is full.
    pub fn sweep(&self, now: u64) {
        self.lock().retain(|_, e| now < e.evict_at);
    }

    /// Entries held, including any past their time not yet swept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether no entry is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Address, Entry>> {
        // A panic while holding the lock leaves a map that is still valid.
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
