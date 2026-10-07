use super::*;
use alloy::primitives::address;

/// Only what is specific to *this* store: `open()`'s guards, and that
/// the trait delegations are wired to the right shared operation. The
/// record codec and the table semantics are covered once, next to the
/// shared implementation in `crate::buyer_pool_table`.
fn store(dir: &tempfile::TempDir) -> anyhow::Result<RedbBuyerPoolStore> {
    // `open` hardens the data dir to 0o700 and rejects anything looser,
    // so point it at a subdir it creates rather than the tempdir root.
    Ok(RedbBuyerPoolStore::open(&dir.path().join("d"))?)
}

fn state(byte: u8, bytes: u64, amount: u64) -> anyhow::Result<(BuyerPoolState, LaneKey)> {
    let owner = Address::repeat_byte(byte);
    let pool_id = PoolId::repeat_byte(byte);
    let lane = LaneKey {
        pool_id,
        signer: owner,
        provider: address!("00000000000000000000000000000000000000aa"),
    };
    let mut s = BuyerPoolState::new(
        pool_id,
        crate::Deployment {
            chain_id: 421_614,
            payment_pool: Address::repeat_byte(0x9c),
        },
        owner,
        Address::repeat_byte(0xaa),
        U256::from(1_000_000u64),
    );
    s.advance_lane(lane, U256::from(bytes), U256::from(amount))?;
    Ok((s, lane))
}

#[test]
fn second_concurrent_open_is_already_open_not_raw_backend() -> anyhow::Result<()> {
    // #942: a second opener on the same data dir (a concurrent `decdn
    // fetch` / `bundle pull`) trips redb's process-exclusive write
    // lock. It must fail with the dedicated, user-facing `AlreadyOpen`
    // — never a raw redb backend string — and recover once the first
    // handle drops.
    let dir = tempfile::tempdir()?;
    let data = dir.path().join("d");
    let first = RedbBuyerPoolStore::open(&data)?;

    let err = RedbBuyerPoolStore::open(&data)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a second concurrent open must fail"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::AlreadyOpen { path } if path == &data.join(BUYER_POOLS_DB_FILE)),
        "wrong error or path: {err:?}"
    );
    let msg = err.to_string();
    anyhow::ensure!(
        msg.contains("another decdn process is using") && msg.contains("--data-dir"),
        "message must be user-facing, got: {msg}"
    );

    // Releasing the first handle frees the lock; a fresh open then succeeds.
    drop(first);
    RedbBuyerPoolStore::open(&data)?;
    Ok(())
}

/// The #2084 read: a store written by one process, closed, and then read
/// by another. A dropped `Database` releases redb's lock, so the rows come
/// back byte-for-byte through the read-only path — which is what makes a
/// stopped daemon's `buyer.redb` inspectable at all.
#[test]
fn a_closed_store_reads_back_read_only() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let data = dir.path().join("d");
    let (recorded, _) = state(7, 4096, 9)?;
    {
        let store = RedbBuyerPoolStore::open(&data)?;
        store.record(&recorded)?;
    }

    let path = data.join(BUYER_POOLS_DB_FILE);
    let reader = ReadOnlyBuyerPoolStore::open_file(&path)?;
    anyhow::ensure!(reader.path() == path, "the reader must name its file");
    let load = reader.load_all()?;
    anyhow::ensure!(load.skipped.is_empty(), "no row should be skipped");
    anyhow::ensure!(
        load.pools == vec![recorded],
        "read-only load must match what was written: {:?}",
        load.pools
    );
    Ok(())
}

/// The lock is the liveness signal. While a writer holds the file, the
/// read-only open fails with `AlreadyOpen` — so a caller can tell "no
/// daemon is running" from "a daemon has this file" without guessing.
#[test]
fn read_only_open_reports_a_live_writer_as_already_open() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let data = dir.path().join("d");
    let writer = RedbBuyerPoolStore::open(&data)?;
    let path = data.join(BUYER_POOLS_DB_FILE);

    let err = ReadOnlyBuyerPoolStore::open_file(&path)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a read-only open must not race a live writer"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::AlreadyOpen { path: p } if p == &path),
        "wrong error or path: {err:?}"
    );

    drop(writer);
    ReadOnlyBuyerPoolStore::open_file(&path)?;
    Ok(())
}

/// The property that keeps #2078 closed: the read-only path never creates.
/// Pointed at a dir with no store, it reports that there is none rather
/// than manufacturing an empty one and reporting its emptiness as state.
#[test]
fn read_only_open_creates_nothing() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("buyer.redb");

    anyhow::ensure!(
        ReadOnlyBuyerPoolStore::open_file(&path).is_err(),
        "an absent store must not open"
    );
    anyhow::ensure!(!path.exists(), "a read-only open must not create the file");
    Ok(())
}

#[test]
fn rejects_zero_length_db_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let data = dir.path().join("d");
    drop(store(&dir)?); // create, then release the lock
    // Truncate to zero: redb would treat this as "create fresh" and
    // silently wipe the watermark, so `open` must refuse.
    std::fs::write(data.join(BUYER_POOLS_DB_FILE), b"")?;
    let err = RedbBuyerPoolStore::open(&data)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a zero-length db file must be rejected"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("empty (length 0)")),
        "expected Corrupt(empty), got {err:?}"
    );
    Ok(())
}

#[test]
fn record_then_reuse_resumes_watermark_across_reopen() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let (s, _lane) = state(1, 5_000, 50)?;
    {
        let store = store(&dir)?;
        store.record(&s)?;
    }
    // A later `decdn fetch` on the same data dir resumes the watermark
    // instead of re-signing stale totals (#940).
    let reopened = store(&dir)?;
    let got = reopened
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("row vanished across reopen"))?;
    anyhow::ensure!(got == s, "watermark must resume across invocations");
    Ok(())
}

/// Each of the seven trait methods must reach its matching shared
/// operation with its arguments in the right order — the shared suite
/// proves the operations are correct but cannot see this store's
/// wiring.
///
/// Method-level transpositions are mostly impossible (the types reject
/// `forget` wired to `forget_if_pool`). The reachable mistake is
/// *argument* order among `advance_progress`'s `U256`s, which the
/// compiler cannot catch and which silently corrupts a payment
/// watermark — so assert the resulting **row**, not just the outcome
/// enum.
#[test]
fn store_delegates_every_op() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = store(&dir)?;
    let (s, lane) = state(2, 1_000, 10)?;

    // record + get_by_owner + load_all
    store.record(&s)?;
    anyhow::ensure!(
        store.get_by_owner(s.owner)?.as_ref() == Some(&s),
        "record/get"
    );
    anyhow::ensure!(store.load_all()?.pools.len() == 1, "load_all");

    // advance_progress
    anyhow::ensure!(
        store.advance_progress(
            s.owner,
            s.pool_id,
            lane,
            U256::from(2_000u64),
            U256::from(20u64),
        )? == AdvanceOutcome::Advanced,
        "advance_progress"
    );

    // add_deposit
    anyhow::ensure!(
        store.add_deposit(s.owner, s.pool_id, U256::from(7u64))?
            == DepositOutcome::Added(s.deposit + U256::from(7u64)),
        "add_deposit"
    );

    // The outcomes above are all reachable with the U256 arguments
    // permuted; only the row proves each landed in its own field.
    let row = store
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
    let progress = row
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("lane vanished"))?;
    anyhow::ensure!(
        progress.last_bytes == U256::from(2_000u64),
        "last_bytes = {}",
        progress.last_bytes
    );
    anyhow::ensure!(
        progress.last_amount == U256::from(20u64),
        "last_amount = {}",
        progress.last_amount
    );
    anyhow::ensure!(
        row.deposit == s.deposit + U256::from(7u64),
        "deposit = {}",
        row.deposit
    );

    // forget_if_pool: wrong id leaves the row, right id deletes it.
    anyhow::ensure!(
        !store.forget_if_pool(s.owner, PoolId::repeat_byte(0xee))?,
        "forget_if_pool must not delete on a mismatched pool"
    );
    anyhow::ensure!(store.get_by_owner(s.owner)?.is_some(), "row must survive");
    anyhow::ensure!(
        store.forget_if_pool(s.owner, s.pool_id)?,
        "forget_if_pool must delete on a match"
    );
    anyhow::ensure!(store.get_by_owner(s.owner)?.is_none(), "row must be gone");

    // forget
    store.record(&s)?;
    store.forget(s.owner)?;
    anyhow::ensure!(store.load_all()?.pools.is_empty(), "forget");
    Ok(())
}
