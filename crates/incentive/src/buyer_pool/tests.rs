use super::*;
use alloy::primitives::{address, b256};

const DEPLOYMENT: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(0x9c),
};

/// Every `owner_byte` gets a **distinct** `pool_id` too (derived from the
/// same byte): `pool_id` is the store's primary key, so two samples
/// sharing one `pool_id` would collide in the primary table instead of
/// coexisting as two independent pools.
fn sample(owner_byte: u8) -> BuyerPoolState {
    let mut obytes = [0u8; 20];
    obytes[19] = owner_byte;
    let mut idbytes = [0u8; 32];
    idbytes[31] = owner_byte;
    let owner = Address::from(obytes);
    let mut state = BuyerPoolState::new(
        PoolId::from(idbytes),
        DEPLOYMENT,
        owner,
        address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        U256::from(10_000_000u64),
    );
    let lane = LaneKey {
        pool_id: state.pool_id,
        signer: owner,
        provider: address!("00000000000000000000000000000000000000b2"),
    };
    // Ignore: fresh state, cannot regress.
    let _ = state.advance_lane(lane, U256::from(4_096u64), U256::from(1_234u64));
    state
}

fn only_lane(state: &BuyerPoolState) -> LaneKey {
    state.lanes().next().map_or(
        LaneKey {
            pool_id: state.pool_id,
            signer: state.owner,
            provider: Address::ZERO,
        },
        |(k, _)| k,
    )
}

/// The committed amount is the sum over every lane, not any one lane: the
/// lanes share one deposit.
#[test]
fn committed_amount_sums_every_lane() {
    let mut state = sample(1);
    let lane2 = LaneKey {
        provider: address!("00000000000000000000000000000000000000b3"),
        ..only_lane(&state)
    };
    assert!(
        state
            .advance_lane(lane2, U256::from(5u64), U256::from(66u64))
            .is_ok()
    );
    assert_eq!(state.committed_amount(), U256::from(1_300u64));
    assert_eq!(
        BuyerPoolState::new(
            state.pool_id,
            DEPLOYMENT,
            state.owner,
            state.token,
            U256::from(1u64),
        )
        .committed_amount(),
        U256::ZERO,
        "a pool with no lanes has committed nothing"
    );
}

/// A row is on a deployment only when both the chain and the contract
/// match. The same `PaymentPool` address on another chain is another
/// deployment, and pool ids repeat there too.
#[test]
fn is_on_compares_the_chain_and_the_contract() {
    let row = sample(1);
    assert!(row.is_on(DEPLOYMENT));
    assert!(!row.is_on(Deployment {
        chain_id: 1,
        ..DEPLOYMENT
    }));
    assert!(!row.is_on(Deployment {
        payment_pool: Address::repeat_byte(0xDE),
        ..DEPLOYMENT
    }));
}

#[test]
fn memory_store_round_trip() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let a = sample(1);
    let b = sample(2);
    store.record(&a)?;
    store.record(&b)?;
    anyhow::ensure!(store.len() == 2);
    let got = store
        .get_by_owner(a.owner)?
        .ok_or_else(|| anyhow::anyhow!("missing a"))?;
    anyhow::ensure!(got == a);
    Ok(())
}

#[test]
fn memory_store_record_overwrites_by_owner() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let mut s = sample(1);
    store.record(&s)?;
    s.deposit = U256::from(99u64);
    store.record(&s)?;
    anyhow::ensure!(store.len() == 1, "same owner overwrites");
    let only = store
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("missing entry"))?;
    anyhow::ensure!(only.deposit == U256::from(99u64));
    Ok(())
}

#[test]
fn memory_store_forget_removes_entry() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let s = sample(1);
    store.record(&s)?;
    store.forget(s.owner)?;
    anyhow::ensure!(store.is_empty());
    // Forgetting an unknown owner is a no-op.
    store.forget(address!("00000000000000000000000000000000000000ff"))?;
    Ok(())
}

#[test]
fn advance_lane_accepts_monotonic_and_equal() -> anyhow::Result<()> {
    let mut s = BuyerPoolState::new(
        b256!("11111111111111111111111111111111111111111111111111111111111111ab"),
        DEPLOYMENT,
        address!("00000000000000000000000000000000000000a1"),
        address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        U256::ZERO,
    );
    let lane = LaneKey {
        pool_id: s.pool_id,
        signer: s.owner,
        provider: address!("00000000000000000000000000000000000000b1"),
    };
    // First advance from an untouched lane.
    s.advance_lane(lane, U256::from(1_000u64), U256::from(10u64))?;
    anyhow::ensure!(
        s.lane_progress(lane)
            == Some(BuyerLaneProgress {
                last_amount: U256::from(10u64),
                last_bytes: U256::from(1_000u64),
            })
    );
    // Equal totals are allowed (idempotent re-record).
    s.advance_lane(lane, U256::from(1_000u64), U256::from(10u64))?;
    // Strictly higher advances.
    s.advance_lane(lane, U256::from(2_000u64), U256::from(20u64))?;
    anyhow::ensure!(
        s.lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("missing lane"))?
            .last_bytes
            == U256::from(2_000u64)
    );
    Ok(())
}

#[test]
fn advance_lane_rejects_regression_and_leaves_state_unchanged() -> anyhow::Result<()> {
    let mut base = sample(1);
    let lane = only_lane(&base);
    // `sample` already left this lane at (bytes 4_096, amount 1_234);
    // advance past that before probing the regression cases below.
    base.advance_lane(lane, U256::from(5_000u64), U256::from(5_000u64))?;

    // (reported (bytes, amount), expected regressed field).
    let cases = [
        ((U256::from(4_000u64), U256::from(6_000u64)), "bytes"),
        ((U256::from(6_000u64), U256::from(4_000u64)), "amount"),
    ];
    for ((bytes, amount), expected_field) in cases {
        let mut s = base.clone();
        let err = s
            .advance_lane(lane, bytes, amount)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected regression error for {expected_field}"))?;
        anyhow::ensure!(
            matches!(err, BuyerProgressError::Regressed { field, .. } if field == expected_field),
            "wrong error variant/field: {err:?}"
        );
        anyhow::ensure!(s == base, "rejected advance must not mutate state");
    }
    Ok(())
}

#[test]
fn forget_if_pool_only_deletes_matching_pool() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let s = sample(1);
    store.record(&s)?;

    // Wrong pool id → no delete, row preserved.
    let deleted = store.forget_if_pool(
        s.owner,
        b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
    )?;
    anyhow::ensure!(!deleted, "mismatched pool must not delete");
    anyhow::ensure!(store.len() == 1, "row must survive a mismatched CAS");

    // Matching pool id → deletes.
    let deleted = store.forget_if_pool(s.owner, s.pool_id)?;
    anyhow::ensure!(deleted, "matching pool must delete");
    anyhow::ensure!(store.is_empty());

    // Unknown owner → false, no-op.
    anyhow::ensure!(!store.forget_if_pool(
        address!("00000000000000000000000000000000000000ff"),
        s.pool_id
    )?);
    Ok(())
}

#[test]
fn advance_progress_advances_committed_watermark() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let s = BuyerPoolState::new(
        b256!("22222222222222222222222222222222222222222222222222222222222222ab"),
        DEPLOYMENT,
        address!("00000000000000000000000000000000000000a3"),
        address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        U256::from(10_000_000u64),
    );
    store.record(&s)?;
    let lane = LaneKey {
        pool_id: s.pool_id,
        signer: s.owner,
        provider: address!("00000000000000000000000000000000000000b3"),
    };

    let outcome = store.advance_progress(
        s.owner,
        s.pool_id,
        lane,
        U256::from(3_000u64),
        U256::from(30u64),
    )?;
    anyhow::ensure!(outcome == AdvanceOutcome::Advanced, "got {outcome:?}");
    let stored = store
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("missing row"))?;
    let progress = stored
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("missing lane"))?;
    anyhow::ensure!(progress.last_bytes == U256::from(3_000u64));
    anyhow::ensure!(progress.last_amount == U256::from(30u64));
    Ok(())
}

#[test]
fn advance_progress_rejects_regression_without_writing() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let mut s = sample(1);
    let lane = only_lane(&s);
    // `sample` already left this lane at (bytes 4_096, amount 1_234);
    // advance past that before probing the regression cases below.
    s.advance_lane(lane, U256::from(9_000u64), U256::from(9_000u64))?;
    store.record(&s)?;

    // Each cumulative field's regression must surface through the
    // wrapper as `AdvanceOutcome::Regressed` with the right field — and
    // must NOT touch the committed watermark.
    let cases = [
        ((U256::from(8_999u64), U256::from(9_000u64)), "bytes"),
        ((U256::from(9_000u64), U256::from(8_999u64)), "amount"),
    ];
    for ((bytes, amount), expected_field) in cases {
        let outcome = store.advance_progress(s.owner, s.pool_id, lane, bytes, amount)?;
        anyhow::ensure!(
            matches!(outcome, AdvanceOutcome::Regressed(BuyerProgressError::Regressed { field, .. }) if field == expected_field),
            "expected {expected_field} regression, got {outcome:?}"
        );
        let stored = store
            .get_by_owner(s.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing row"))?;
        let progress = stored
            .lane_progress(lane)
            .ok_or_else(|| anyhow::anyhow!("missing lane"))?;
        anyhow::ensure!(
            progress.last_bytes == U256::from(9_000u64)
                && progress.last_amount == U256::from(9_000u64),
            "watermark regressed after rejecting {expected_field}"
        );
    }
    Ok(())
}

#[test]
fn advance_and_deposit_guard_on_pool_and_owner() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let s = sample(1);
    let lane = only_lane(&s);
    store.record(&s)?;
    let other_pool = b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    let unknown = address!("00000000000000000000000000000000000000ff");

    // Pool-id mismatch → stale, no write.
    anyhow::ensure!(
        store.advance_progress(
            s.owner,
            other_pool,
            lane,
            U256::from(99u64),
            U256::from(99u64),
        )? == AdvanceOutcome::PoolMismatch
    );
    anyhow::ensure!(
        store.add_deposit(s.owner, other_pool, U256::from(1u64))? == DepositOutcome::PoolMismatch
    );
    let stored = store
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("missing row"))?;
    anyhow::ensure!(stored == s, "mismatched calls must not mutate the row");

    // Unknown owner → UnknownPool, no write.
    anyhow::ensure!(
        store.advance_progress(unknown, s.pool_id, lane, U256::from(1u64), U256::from(1u64),)?
            == AdvanceOutcome::UnknownPool
    );
    anyhow::ensure!(
        store.add_deposit(unknown, s.pool_id, U256::from(1u64))? == DepositOutcome::UnknownPool
    );
    Ok(())
}

#[test]
fn add_deposit_accumulates_committed_deposit() -> anyhow::Result<()> {
    let store = MemoryBuyerPoolStore::new();
    let mut s = sample(1);
    s.deposit = U256::from(100u64);
    store.record(&s)?;

    let outcome = store.add_deposit(s.owner, s.pool_id, U256::from(40u64))?;
    anyhow::ensure!(
        outcome == DepositOutcome::Added(U256::from(140u64)),
        "got {outcome:?}"
    );
    let stored = store
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("missing row"))?;
    anyhow::ensure!(stored.deposit == U256::from(140u64));
    Ok(())
}
