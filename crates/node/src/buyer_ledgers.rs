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
//! What made that fatal rather than merely wasteful is `retire_dead_channel`: `StaleNonce`
//! is now a terminal verdict, so the losing pull DELETES the channel row out from under the
//! winner — which is still streaming on it. The winner's watermark write then fails
//! `UnknownProvider`, and the deposit is stranded (see [`crate::buyer_channel`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use alloy::primitives::Address;
use decdn_client_pull::{ChannelLedger, Cumulative};
use decdn_incentive::ChannelId;

/// The voucher ledger of each provider's CURRENT channel, shared by every concurrent pull
/// on it.
///
/// Keyed by provider rather than by `(provider, channel)` because a provider has exactly one
/// live buyer channel at a time — the same shape as the buyer store's own row — so a rotated
/// channel's ledger is evicted by the act of seeding its replacement rather than lingering.
/// Cardinality is therefore bounded by the number of providers this node has ever paid,
/// which is the buyer store's bound too.
#[derive(Debug, Default)]
pub struct BuyerLedgers {
    live: Mutex<HashMap<Address, (ChannelId, Arc<ChannelLedger>)>>,
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
    /// A DIFFERENT `channel_id` means the provider's channel rotated. The old ledger belongs
    /// to a channel no future voucher can be signed against, so it is replaced.
    pub fn get_or_seed(
        &self,
        provider: Address,
        channel_id: ChannelId,
        seed: Cumulative,
    ) -> Arc<ChannelLedger> {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        match live.get(&provider) {
            Some((current, ledger)) if *current == channel_id => Arc::clone(ledger),
            _ => {
                let ledger = Arc::new(ChannelLedger::new(seed));
                live.insert(provider, (channel_id, Arc::clone(&ledger)));
                ledger
            }
        }
    }

    /// Drop `provider`'s ledger if it is still `channel_id`'s — the channel was retired, so
    /// no further voucher can be signed against it.
    ///
    /// Compare-and-remove, for the same reason `forget_if_channel` is: a concurrent open may
    /// already have rotated the provider onto a NEW channel, whose ledger is live and must
    /// not be thrown away.
    pub fn forget(&self, provider: Address, channel_id: ChannelId) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((current, _)) = live.get(&provider)
            && *current == channel_id
        {
            live.remove(&provider);
        }
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

    /// Providers do not share a ledger with each other.
    #[test]
    fn distinct_providers_get_distinct_ledgers() {
        let ledgers = BuyerLedgers::default();
        let a = ledgers.get_or_seed(addr(1), chan(9), seed_at(0));
        let b = ledgers.get_or_seed(addr(2), chan(9), seed_at(0));
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
