use super::*;
use alloy::primitives::{U256, address, b256};

fn sample(pool_id_byte: u8, signer_byte: u8) -> LaneState {
    let mut pool = [0u8; 32];
    pool[31] = pool_id_byte;
    let mut signer = [0u8; 20];
    signer[19] = signer_byte;
    LaneState::hydrate(
        pool.into(),
        signer.into(),
        address!("00000000000000000000000000000000000000b2"),
        U256::from(10_000_000u64),
        1_900_000_000,
        U256::from(1_234u64),
        U256::from(4_096u64),
        Some([0xABu8; 65]),
        crate::lane::LaneChain::NONE,
    )
}

#[test]
fn record_does_not_regress_registered_until() -> Result<(), StoreError> {
    let store = MemoryPoolStateStore::new();
    let mut st = sample(1, 2);
    st.registered_until = 0;
    store.record(&st)?; // voucher-path shape: unknown
    store.set_registered_until(st.key(), 1_800_000_000)?; // redeemer learns expiry
    // A later voucher record carries registered_until 0 again.
    store.record(&st)?;
    let got = store.get(st.key())?.ok_or(StoreError::Corrupt {
        pool_id: None,
        detail: "missing".into(),
    })?;
    assert_eq!(
        got.registered_until, 1_800_000_000,
        "record must not clobber to 0"
    );
    Ok(())
}

#[test]
fn set_registered_until_is_monotone() -> Result<(), StoreError> {
    let store = MemoryPoolStateStore::new();
    let st = sample(3, 4);
    store.record(&st)?;
    store.set_registered_until(st.key(), 100)?;
    store.set_registered_until(st.key(), 50)?; // lower: no-op
    let got = store.get(st.key())?.ok_or(StoreError::Corrupt {
        pool_id: None,
        detail: "x".into(),
    })?;
    assert_eq!(got.registered_until, 100);
    Ok(())
}

#[test]
fn memory_store_round_trip() -> anyhow::Result<()> {
    let store = MemoryPoolStateStore::new();
    let a = sample(1, 1);
    let b = sample(2, 2);
    store.record(&a)?;
    store.record(&b)?;
    let mut all = store.load_all()?;
    all.sort_by_key(|s| s.pool_id);
    anyhow::ensure!(all.len() == 2);
    let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
    let second = all.get(1).ok_or_else(|| anyhow::anyhow!("missing [1]"))?;
    anyhow::ensure!(*first == a);
    anyhow::ensure!(*second == b);
    Ok(())
}

#[test]
fn memory_store_record_overwrites() -> anyhow::Result<()> {
    let store = MemoryPoolStateStore::new();
    let s = sample(1, 1);
    store.record(&s)?;
    // Re-record the same lane with an advanced amount (overwrite). The
    // `last_*` fields are private, so rebuild via `hydrate` rather than
    // mutating in place.
    let advanced = LaneState::hydrate(
        s.pool_id,
        s.signer,
        s.provider,
        s.cap,
        s.expiry,
        U256::from(9_999u64),
        s.last_bytes_delivered(),
        s.last_signature().copied(),
        crate::lane::LaneChain::NONE,
    );
    store.record(&advanced)?;
    let all = store.load_all()?;
    anyhow::ensure!(all.len() == 1);
    let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
    anyhow::ensure!(only.last_amount() == U256::from(9_999u64));
    Ok(())
}

#[test]
fn memory_store_forget_removes_entry() -> anyhow::Result<()> {
    let store = MemoryPoolStateStore::new();
    let s = sample(1, 1);
    let key = s.key();
    store.record(&s)?;
    store.forget(key)?;
    anyhow::ensure!(store.is_empty());
    // Forgetting a never-recorded lane is a no-op.
    store.forget(LaneKey {
        pool_id: b256!("1111111111111111111111111111111111111111111111111111111111111111"),
        signer: address!("00000000000000000000000000000000000000a1"),
        provider: address!("00000000000000000000000000000000000000b2"),
    })?;
    Ok(())
}

#[test]
fn memory_store_flush_is_noop_ok() -> anyhow::Result<()> {
    let store = MemoryPoolStateStore::new();
    store.record(&sample(1, 1))?;
    // Volatile store: flush has nothing to do and must succeed.
    store.flush()?;
    anyhow::ensure!(store.len() == 1);
    Ok(())
}
