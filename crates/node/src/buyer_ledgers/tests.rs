use super::*;
use alloy::primitives::{Address, B256, U256};

fn lane(pool: u8, provider: u8) -> LaneKey {
    LaneKey {
        pool_id: B256::from([pool; 32]),
        signer: Address::from([0x5a; 20]),
        provider: Address::from([provider; 20]),
    }
}

fn seed_at(amount: u64) -> Cumulative {
    Cumulative {
        bytes: U256::ZERO,
        amount: U256::from(amount),
    }
}

/// The property the whole type exists for: two concurrent pulls to one lane issue through
/// ONE ledger, so their vouchers are serialized instead of both signing from the same
/// watermark and colliding.
#[test]
fn concurrent_pulls_on_one_lane_share_a_ledger() {
    let ledgers = BuyerLedgers::default();
    let a = ledgers.get_or_seed(lane(9, 1), seed_at(7));
    let b = ledgers.get_or_seed(lane(9, 1), seed_at(7));
    assert!(
        Arc::ptr_eq(&a, &b),
        "both pulls must issue through the same ledger, or they sign the same watermark"
    );
}

/// The live ledger outranks the persisted seed. The second pull's `seed` is the store
/// row, which lags the in-flight pull's issuance — honouring it would rewind the watermark
/// and re-create the collision.
#[test]
fn a_stale_seed_cannot_rewind_a_live_ledger() {
    let ledgers = BuyerLedgers::default();
    let live = ledgers.get_or_seed(lane(9, 1), seed_at(7));
    let rejoined = ledgers.get_or_seed(lane(9, 1), seed_at(7));
    assert!(Arc::ptr_eq(&live, &rejoined));
    assert_eq!(
        rejoined.committed().amount,
        U256::from(7u64),
        "the live ledger's own watermark stands; the seed is ignored on a hit"
    );
}

/// Each pool id keys its own ledger, so a different pool addresses a distinct
/// entry rather than sharing the incumbent's.
#[test]
fn a_rotated_pool_gets_a_fresh_ledger() {
    let ledgers = BuyerLedgers::default();
    let old = ledgers.get_or_seed(lane(9, 1), seed_at(7));
    let new = ledgers.get_or_seed(lane(10, 1), seed_at(0));
    assert!(!Arc::ptr_eq(&old, &new));
    assert_eq!(
        new.committed().amount,
        U256::ZERO,
        "seeded from the new row"
    );
}

/// Retiring a lane drops its ledger, so a later reuse re-seeds from the store.
#[test]
fn forget_drops_only_the_named_lane() {
    let ledgers = BuyerLedgers::default();
    let first = ledgers.get_or_seed(lane(9, 1), seed_at(7));
    // A concurrent open already rotated us onto pool 10; a late retire of pool 9 must not
    // throw away the live ledger.
    let live = ledgers.get_or_seed(lane(10, 1), seed_at(0));
    ledgers.forget(lane(9, 1));
    let rejoined = ledgers.get_or_seed(lane(10, 1), seed_at(0));
    assert!(Arc::ptr_eq(&live, &rejoined), "pool 10's ledger survives");
    drop(first);

    ledgers.forget(lane(10, 1));
    let fresh = ledgers.get_or_seed(lane(10, 1), seed_at(3));
    assert!(!Arc::ptr_eq(&live, &fresh), "its own retire does drop it");
}

/// The bug this [`LaneKey`] keying closes (#1145 review): a call carrying a STALE pool —
/// an older row a concurrent path read — must not evict the provider's LIVE ledger. The
/// earlier provider-only key evicted on ANY mismatch, so the next pull on the live lane
/// got a fresh ledger, re-signed a spent watermark, and wedged the lane.
#[test]
fn a_stale_pool_does_not_evict_the_live_ledger() {
    let ledgers = BuyerLedgers::default();
    // The live lane, held by a concurrent pull (so its `Arc` outlives the map entry).
    let live = ledgers.get_or_seed(lane(10, 1), seed_at(5));
    // A late call with a STALE pool: keyed by `LaneKey`, it addresses pool 9's
    // own entry and never touches pool 10's live ledger.
    let _stale = ledgers.get_or_seed(lane(9, 1), seed_at(0));
    // pool 10's live ledger survives — a subsequent pull on it rejoins the SAME Arc.
    let rejoined = ledgers.get_or_seed(lane(10, 1), seed_at(0));
    assert!(
        Arc::ptr_eq(&live, &rejoined),
        "a stale pool must not evict the live lane's ledger"
    );
    assert_eq!(
        rejoined.committed().amount,
        U256::from(5u64),
        "the live ledger's watermark stands; the seed is ignored on a hit"
    );
}

/// Providers do not share a ledger with each other.
#[test]
fn distinct_providers_get_distinct_ledgers() {
    let ledgers = BuyerLedgers::default();
    let a = ledgers.get_or_seed(lane(9, 1), seed_at(0));
    let b = ledgers.get_or_seed(lane(9, 2), seed_at(0));
    assert!(!Arc::ptr_eq(&a, &b));
}

/// The pool-wide accounting the node's ranged-drive loop gates on (#1506): the
/// spend a later run subtracts from the shared deposit is the WHOLE pool's
/// committed amount, summed across every provider's lane — not this one lane's.
///
/// Run 1 pays provider A (its lane committed 700). Run 2 opens a FRESH lane to
/// provider B, whose own committed is 0. If run 2 gated on B's ledger alone it
/// would read `committed == 0`, believe the whole deposit unspent, and sign a
/// voucher the pool cannot back. `pool_committed` returns 700 for that same
/// pool, so run 2 subtracts run 1's spend from the deposit exactly as it must.
#[test]
fn pool_committed_sums_every_providers_lane_in_the_pool() {
    let ledgers = BuyerLedgers::default();
    let pool_a = lane(1, 1); // pool 1, provider A — run 1
    let pool_b = lane(1, 2); // pool 1, provider B — run 2 (fresh lane)
    let other = lane(2, 1); // a different pool entirely

    let _run1 = ledgers.get_or_seed(pool_a, seed_at(700));
    let _run2 = ledgers.get_or_seed(pool_b, seed_at(0));
    let _elsewhere = ledgers.get_or_seed(other, seed_at(999));

    assert_eq!(
        ledgers.pool_committed(pool_a.pool_id),
        U256::from(700u64),
        "run 2's gate must see run 1's 700 across the pool, not provider B's own 0",
    );
    assert_eq!(
        ledgers.pool_committed(other.pool_id),
        U256::from(999u64),
        "a different pool's spend is isolated",
    );
    assert_eq!(
        ledgers.pool_committed(B256::from([7u8; 32])),
        U256::ZERO,
        "a pool with no live lanes has spent nothing",
    );
}
