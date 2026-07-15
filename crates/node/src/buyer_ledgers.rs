//! The live voucher ledger for each provider's current buyer channel.
//!
//! One [`ChannelLedger`] per `(provider, channel)` is a CORRECTNESS requirement, not a
//! cache. `stream_fetch_shared` states the contract it exists to satisfy:
//!
//! > each `stream_fetch`/`stream_fetch_tracked` call seeds its own voucher state from
//! > `ctx.prior_*`, so N concurrent pulls on the same channel all sign the next voucher at
//! > `prior_nonce + 1` and collide — the node accepts exactly one and rejects the rest as
//! > `StaleNonce`.
//!
//! Both node pull paths built a FRESH ledger per pull and so broke that contract (#1145
//! review). It is reachable at the defaults, on an ordinary node doing nothing unusual: the
//! cache engine coalesces in-flight pulls **by hash** (`cache::engine::inflight`), so two
//! misses for *different* blobs run concurrent `NodeOrigin::fetch` calls, and on a
//! tens-of-nodes network both routinely rank the same provider first. Both then take the
//! channel-reuse fast path, read the same `prior_nonce`, and collide.
//!
//! What made that fatal rather than merely wasteful is that `StaleNonce` is now a terminal
//! verdict: the losing pull's collision wedges the channel — `wedged_channel` keeps the row
//! for the reclaim sweep but suppresses the provider — and the desync persists until the
//! deposit is reclaimed at expiry (see [`crate::buyer_channel`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use alloy::primitives::Address;
use decdn_client_pull::{ChannelLedger, Cumulative};
use decdn_incentive::ChannelId;

/// The voucher ledger of each provider's current channel, shared by every concurrent pull
/// on it.
///
/// Keyed by `(provider, channel)` so the ledger a pull issues through is pinned to the exact
/// channel it read: a stale or rotated `channel_id` can neither collide with nor evict a
/// different channel's live ledger. Cardinality stays bounded without that hazard — a rotated
/// channel's ledger is dropped by [`Self::get_or_seed`] the moment no pull still holds it
/// (`Arc::strong_count == 1`), so at most one live channel per provider lingers, plus any
/// whose pulls are still in flight. That is the buyer store's bound, made safe against a
/// stale key (#1145 review): the earlier provider-only key evicted the live ledger on ANY
/// channel-id mismatch, which is exactly the collision this type exists to prevent.
#[derive(Debug, Default)]
pub struct BuyerLedgers {
    live: Mutex<HashMap<(Address, ChannelId), Arc<ChannelLedger>>>,
}

impl BuyerLedgers {
    /// The ledger to issue this pull's vouchers through: the live one for
    /// `(provider, channel_id)`, or a fresh one seeded from `seed` if there is none.
    ///
    /// An existing entry for the SAME channel wins over `seed`, and that precedence is the
    /// whole point. `seed` comes from the persisted row that `open_or_reuse_channel` read;
    /// a concurrent pull holding the live ledger may already have issued vouchers the row
    /// does not carry yet (it is written on settle, not on issue). Re-seeding from the row
    /// would rewind the nonce and re-create the collision this type exists to prevent.
    ///
    /// A DIFFERENT `channel_id` addresses a DIFFERENT entry — a rotated or stale channel id
    /// can neither read nor evict this channel's ledger. The provider's other channels are
    /// pruned here, but only those no pull still holds (`Arc::strong_count == 1`), so a
    /// rotation cannot throw away a ledger a concurrent pull is issuing through.
    pub fn get_or_seed(
        &self,
        provider: Address,
        channel_id: ChannelId,
        seed: Cumulative,
    ) -> Arc<ChannelLedger> {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        // Bound cardinality by dropping this provider's OTHER channels — but only those no
        // pull still holds, so a stale/rotated key cannot evict a live ledger.
        live.retain(|(p, c), l| *p != provider || *c == channel_id || Arc::strong_count(l) > 1);
        Arc::clone(
            live.entry((provider, channel_id))
                .or_insert_with(|| Arc::new(ChannelLedger::new(seed))),
        )
    }

    /// Drop `(provider, channel_id)`'s ledger — the channel was retired, so no further voucher
    /// can be signed against it.
    ///
    /// Keyed by the exact channel, so a concurrent open that already rotated the provider onto
    /// a NEW channel is untouched: its ledger lives under a different key and is not thrown
    /// away.
    pub fn forget(&self, provider: Address, channel_id: ChannelId) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        live.remove(&(provider, channel_id));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloy::primitives::{B256, U256};

    fn addr(b: u8) -> Address {
        Address::from([b; 20])
    }

    fn chan(b: u8) -> ChannelId {
        B256::from([b; 32])
    }

    fn seed_at(nonce: u64) -> Cumulative {
        Cumulative {
            nonce: U256::from(nonce),
            bytes: U256::ZERO,
            amount: U256::ZERO,
        }
    }

    /// The property the whole type exists for: two concurrent pulls to one provider issue
    /// through ONE ledger, so their vouchers are serialized instead of both signing
    /// `prior_nonce + 1` and colliding.
    #[test]
    fn concurrent_pulls_on_one_channel_share_a_ledger() {
        let ledgers = BuyerLedgers::default();
        let a = ledgers.get_or_seed(addr(1), chan(9), seed_at(7));
        let b = ledgers.get_or_seed(addr(1), chan(9), seed_at(7));
        assert!(
            Arc::ptr_eq(&a, &b),
            "both pulls must issue through the same ledger, or they sign the same nonce"
        );
    }

    /// The live ledger outranks the persisted seed. The second pull's `seed` is the store
    /// row, which lags the in-flight pull's issuance — honouring it would rewind the nonce
    /// and re-create the collision.
    #[test]
    fn a_stale_seed_cannot_rewind_a_live_ledger() {
        let ledgers = BuyerLedgers::default();
        let live = ledgers.get_or_seed(addr(1), chan(9), seed_at(7));
        // The in-flight pull advances the ledger past what the store row knows.
        let stale_row = seed_at(7);
        let rejoined = ledgers.get_or_seed(addr(1), chan(9), stale_row);
        assert!(Arc::ptr_eq(&live, &rejoined));
        assert_eq!(
            rejoined.committed().nonce,
            U256::from(7u64),
            "the live ledger's own watermark stands; the seed is ignored on a hit"
        );
    }

    /// A rotated channel gets its own ledger — the old one can sign nothing.
    #[test]
    fn a_rotated_channel_gets_a_fresh_ledger() {
        let ledgers = BuyerLedgers::default();
        let old = ledgers.get_or_seed(addr(1), chan(9), seed_at(7));
        let new = ledgers.get_or_seed(addr(1), chan(10), seed_at(0));
        assert!(!Arc::ptr_eq(&old, &new));
        assert_eq!(new.committed().nonce, U256::ZERO, "seeded from the new row");
    }

    /// Retiring a channel drops its ledger, so a later reuse re-seeds from the store.
    #[test]
    fn forget_drops_only_the_named_channel() {
        let ledgers = BuyerLedgers::default();
        let first = ledgers.get_or_seed(addr(1), chan(9), seed_at(7));
        // A concurrent open already rotated us onto chan(10); a late retire of chan(9)
        // must not throw away the live ledger.
        let live = ledgers.get_or_seed(addr(1), chan(10), seed_at(0));
        ledgers.forget(addr(1), chan(9));
        let rejoined = ledgers.get_or_seed(addr(1), chan(10), seed_at(0));
        assert!(Arc::ptr_eq(&live, &rejoined), "chan(10)'s ledger survives");
        drop(first);

        ledgers.forget(addr(1), chan(10));
        let fresh = ledgers.get_or_seed(addr(1), chan(10), seed_at(3));
        assert!(!Arc::ptr_eq(&live, &fresh), "its own retire does drop it");
    }

    /// The bug this `(provider, channel)` keying closes (#1145 review): a call carrying a
    /// STALE `channel_id` — an older row a concurrent path read — must not evict the provider's
    /// LIVE ledger. The earlier provider-only key evicted on ANY mismatch, so the next pull on
    /// the live channel got a fresh ledger, re-signed a spent nonce, and wedged the channel.
    #[test]
    fn a_stale_channel_id_does_not_evict_the_live_ledger() {
        let ledgers = BuyerLedgers::default();
        // The live channel, held by a concurrent pull (so its `Arc` outlives the map entry).
        let live = ledgers.get_or_seed(addr(1), chan(10), seed_at(5));
        // A late call with a STALE id: under the old provider-key this evicted chan(10)'s
        // ledger; keyed by `(provider, channel)` it just addresses chan(9)'s own entry.
        let _stale = ledgers.get_or_seed(addr(1), chan(9), seed_at(0));
        // chan(10)'s live ledger survives — a subsequent pull on it rejoins the SAME Arc.
        let rejoined = ledgers.get_or_seed(addr(1), chan(10), seed_at(0));
        assert!(
            Arc::ptr_eq(&live, &rejoined),
            "a stale channel_id must not evict the live channel's ledger"
        );
        assert_eq!(
            rejoined.committed().nonce,
            U256::from(5u64),
            "the live ledger's watermark stands; the seed is ignored on a hit"
        );
    }

    /// Providers do not share a ledger with each other.
    #[test]
    fn distinct_providers_get_distinct_ledgers() {
        let ledgers = BuyerLedgers::default();
        let a = ledgers.get_or_seed(addr(1), chan(9), seed_at(0));
        let b = ledgers.get_or_seed(addr(2), chan(9), seed_at(0));
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
