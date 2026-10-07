use alloy::primitives::{address, b256};
use tempfile::TempDir;

use super::*;

/// A bare `redb` database in a temp dir. These tests drive the shared
/// table ops directly, so they need no store, no data-dir hardening,
/// and no file layout — which is the point: the ops are agnostic about
/// who owns the file.
fn db() -> anyhow::Result<(TempDir, Database)> {
    let dir = tempfile::tempdir()?;
    let db = Database::create(dir.path().join("t.redb"))?;
    Ok((dir, db))
}

/// The deployment every fixture row lives on, unless a test names another.
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: Address::repeat_byte(0x9c),
};

fn tbl(db: &Database) -> BuyerPoolTable<'_> {
    BuyerPoolTable::new(db)
}

/// `true` if the buyer table was never created. Distinguishes "no row"
/// from "no table" so the no-row ops can be held to not creating one.
fn table_absent(db: &Database) -> anyhow::Result<bool> {
    let read_txn = db.begin_read()?;
    Ok(matches!(
        read_txn.open_table(BUYER_POOL_TABLE),
        Err(redb::TableError::TableDoesNotExist(_))
    ))
}

/// Every `byte` gets a **distinct** `pool_id` too (derived from the same
/// byte): `pool_id` is the store's primary key, so two fixtures sharing
/// one `pool_id` would collide in the primary table instead of
/// coexisting as two independent pools. One lane, keyed off `owner` as
/// both signer and provider seed byte, tracks the sample's cumulative
/// progress.
fn state(byte: u8) -> BuyerPoolState {
    let mut id = [0u8; 32];
    id[31] = byte;
    let mut own = [0u8; 20];
    own[19] = byte;
    let owner = Address::from(own);
    let pool_id = PoolId::from(id);
    let mut s = BuyerPoolState::new(
        pool_id,
        DEPLOYMENT,
        owner,
        address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        U256::from(10_000_000u64),
    );
    let lane = LaneKey {
        pool_id,
        signer: owner,
        provider: address!("00000000000000000000000000000000000000b2"),
    };
    let _ = s.advance_lane(
        lane,
        U256::from(byte) * U256::from(1_024u64),
        U256::from(byte) * U256::from(1_000u64),
    );
    s
}

/// Mirrors `state(1)`, named for the round-trip test below.
fn sample_state() -> BuyerPoolState {
    state(1)
}

const OTHER_POOL: PoolId =
    b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

/// Golden fixture: two distinct lanes with distinct byte patterns in
/// every field. Postcard writes fields (and vec elements) positionally,
/// so a golden can only catch a reordering if the swapped fields encode
/// differently — every field here holds a unique byte pattern.
fn golden_state() -> anyhow::Result<BuyerPoolState> {
    let pool_id = B256::repeat_byte(0x11);
    let owner = Address::repeat_byte(0x22);
    let lane_a = LaneKey {
        pool_id,
        signer: Address::repeat_byte(0x66),
        provider: Address::repeat_byte(0x77),
    };
    let lane_b = LaneKey {
        pool_id,
        signer: Address::repeat_byte(0x88),
        provider: Address::repeat_byte(0x99),
    };
    let mut s = BuyerPoolState::adopt(
        pool_id,
        Deployment {
            chain_id: 0x5555_5555_5555_5555,
            payment_pool: Address::repeat_byte(0x9c),
        },
        owner,
        Address::repeat_byte(0x33),
        U256::from(0xAAAA_AAAA_AAAA_AAAAu64),
        U256::from(0xF0F0_F0F0_F0F0_F0F0u64),
    );
    s.advance_lane(
        lane_a,
        U256::from(0xDDDD_DDDD_DDDD_DDDDu64),
        U256::from(0xBBBB_BBBB_BBBB_BBBBu64),
    )?;
    s.advance_lane(
        lane_b,
        U256::from(0xEEEE_EEEE_EEEE_EEEEu64),
        U256::from(0xCCCC_CCCC_CCCC_CCCCu64),
    )?;
    Ok(s)
}

use alloy::primitives::B256;

/// Postcard encoding of [`golden_state`] (schema 1). Two lanes, sorted
/// by `(signer, provider)` for a deterministic encoding regardless of
/// `HashMap` iteration order.
///
/// The lane row is the two cumulatives and nothing else. A chain hangs off
/// the anchor in its own opening voucher (ADR 003 §One chain per lane), so
/// `last_amount` already says which chain the lane resumes on — there is no
/// counter here to keep, and none to get wrong.
const GOLDEN_RECORD_HEX: &str = concat!(
    "01",                                                               // schema_version (varint)
    "1111111111111111111111111111111111111111111111111111111111111111", // pool_id
    "5555555555555555",                                                 // chain_id (big-endian)
    "9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c",                         // payment_pool
    "2222222222222222222222222222222222222222",                         // owner
    "3333333333333333333333333333333333333333",                         // token
    "000000000000000000000000000000000000000000000000aaaaaaaaaaaaaaaa", // deposit
    "02",                                       // lanes: length prefix (2 entries)
    "6666666666666666666666666666666666666666", // lane a: signer
    "7777777777777777777777777777777777777777", // lane a: provider
    "000000000000000000000000000000000000000000000000bbbbbbbbbbbbbbbb", // lane a: last_amount
    "000000000000000000000000000000000000000000000000dddddddddddddddd", // lane a: last_bytes
    "8888888888888888888888888888888888888888", // lane b: signer
    "9999999999999999999999999999999999999999", // lane b: provider
    "000000000000000000000000000000000000000000000000cccccccccccccccc", // lane b: last_amount
    "000000000000000000000000000000000000000000000000eeeeeeeeeeeeeeee", // lane b: last_bytes
    "000000000000000000000000000000000000000000000000f0f0f0f0f0f0f0f0", // redeemed_elsewhere
);

/// Lowercase hex of `bytes`. `fold` + `write!` rather than the obvious
/// `map(format!).collect()`, which trips `clippy::format_collect`.
fn hex_of(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            // Infallible: writing to a String never errors.
            let _ = write!(acc, "{b:02x}");
            acc
        })
}

/// The buyer record is a **frozen on-disk format**, shared by the
/// node's `buyer.redb` and the client's `buyer-pools.redb`. Postcard
/// encodes struct fields positionally and unnamed, so reordering,
/// retyping, or inserting a field rewrites the bytes with no compile
/// error and no other test failure — every store the suites build is a
/// fresh tempdir, so they would all still pass while the bytes on disk
/// silently changed meaning.
///
/// This golden is the tripwire for that, and the only test here that
/// would fail on such a change.
#[test]
fn encode_is_byte_stable() -> anyhow::Result<()> {
    let hex = hex_of(&encode_record(&golden_state()?)?);
    anyhow::ensure!(
        hex == GOLDEN_RECORD_HEX,
        "the buyer record's on-disk encoding changed — this orphans every existing record in \
         both `buyer.redb` and `buyer-pools.redb`.\n  got:  {hex}\n  want: {GOLDEN_RECORD_HEX}",
    );
    Ok(())
}

/// The frozen bytes must also *decode* back to the fixture — the
/// direction that actually matters (a record already on disk still
/// loads).
#[test]
fn golden_bytes_still_decode() -> anyhow::Result<()> {
    let bytes: Vec<u8> = (0..GOLDEN_RECORD_HEX.len() / 2)
        .map(|i| {
            GOLDEN_RECORD_HEX
                .get(i * 2..i * 2 + 2)
                .ok_or_else(|| anyhow::anyhow!("odd-length golden hex"))
                .and_then(|b| u8::from_str_radix(b, 16).map_err(Into::into))
        })
        .collect::<anyhow::Result<_>>()?;
    let want = golden_state()?;
    let got = decode_record(want.pool_id.into(), &bytes)?;
    anyhow::ensure!(
        got == want,
        "frozen bytes no longer decode:\n {got:?}\n {want:?}"
    );
    Ok(())
}

#[test]
fn open_empty_store_returns_no_entries() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    anyhow::ensure!(tbl(&db).load_all()?.pools.is_empty());
    anyhow::ensure!(tbl(&db).get_by_owner(state(1).owner)?.is_none());
    anyhow::ensure!(tbl(&db).get_by_pool_id(state(1).pool_id)?.is_none());
    Ok(())
}

/// A lane seeded from chain on an adopted row takes its watermark out of
/// the redeemed spend no lane accounts for, once: a second seed of the same
/// lane only advances it.
#[test]
fn seed_progress_moves_a_new_lanes_watermark_out_of_the_redeemed_spend() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let base = state(1);
    let s = BuyerPoolState::hydrate(
        base.pool_id,
        base.deployment,
        base.owner,
        base.token,
        base.deposit,
        base.lanes().collect(),
        U256::from(100u64),
    );
    tbl(&db).record(&s)?;
    let lane = LaneKey {
        pool_id: s.pool_id,
        signer: Address::repeat_byte(0x61),
        provider: Address::repeat_byte(0x71),
    };
    let seed = |amount: u64| {
        tbl(&db).seed_progress(
            s.owner,
            s.pool_id,
            lane,
            U256::from(amount * 1000),
            U256::from(amount),
        )
    };
    anyhow::ensure!(matches!(seed(60)?, AdvanceOutcome::Advanced));
    let row = tbl(&db)
        .get_by_pool_id(s.pool_id)?
        .ok_or_else(|| anyhow::anyhow!("row"))?;
    anyhow::ensure!(row.redeemed_elsewhere() == U256::from(40u64));
    let base = s.committed_amount();
    anyhow::ensure!(row.pool_spend() == base + U256::from(100u64));
    anyhow::ensure!(matches!(seed(70)?, AdvanceOutcome::Advanced));
    let row = tbl(&db)
        .get_by_pool_id(s.pool_id)?
        .ok_or_else(|| anyhow::anyhow!("row"))?;
    anyhow::ensure!(row.redeemed_elsewhere() == U256::from(40u64));
    anyhow::ensure!(row.pool_spend() == base + U256::from(110u64));
    Ok(())
}

#[test]
fn record_round_trips_and_keys_by_pool_id() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = sample_state();
    tbl(&db).record(&s)?;

    let by_id = tbl(&db)
        .get_by_pool_id(s.pool_id)?
        .ok_or_else(|| anyhow::anyhow!("missing by pool_id"))?;
    anyhow::ensure!(by_id == s);

    let by_owner = tbl(&db)
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("missing by owner"))?;
    anyhow::ensure!(by_owner.pool_id == s.pool_id);
    anyhow::ensure!(by_owner == s);
    Ok(())
}

#[test]
fn record_get_and_persist_across_reopen() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("t.redb");
    let a = state(1);
    let b = state(2);
    {
        let db = Database::create(&path)?;
        tbl(&db).record(&a)?;
        tbl(&db).record(&b)?;
        let got = tbl(&db)
            .get_by_owner(a.owner)?
            .ok_or_else(|| anyhow::anyhow!("missing a"))?;
        anyhow::ensure!(got == a, "get_by_owner must round-trip");
    }
    // Reopen the same file: records survive.
    let db = Database::create(&path)?;
    let mut load = tbl(&db).load_all()?;
    load.pools.sort_by_key(|s| s.owner);
    anyhow::ensure!(load.pools.len() == 2);
    anyhow::ensure!(*load.pools.first().ok_or_else(|| anyhow::anyhow!("[0]"))? == a);
    anyhow::ensure!(*load.pools.get(1).ok_or_else(|| anyhow::anyhow!("[1]"))? == b);
    Ok(())
}

#[test]
fn record_overwrites_by_owner() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let mut s = state(5);
    tbl(&db).record(&s)?;
    s.deposit = U256::from(20_000_000u64);
    tbl(&db).record(&s)?;
    anyhow::ensure!(
        tbl(&db).load_all()?.pools.len() == 1,
        "same owner overwrites"
    );
    let only = tbl(&db)
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    anyhow::ensure!(only.deposit == U256::from(20_000_000u64));
    Ok(())
}

#[test]
fn load_all_returns_every_recorded_pool() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    for byte in 1..=4u8 {
        tbl(&db).record(&state(byte))?;
    }
    let mut got: Vec<_> = tbl(&db).load_all()?.pools.iter().map(|s| s.owner).collect();
    got.sort_unstable();
    let want: Vec<_> = (1..=4u8).map(|b| state(b).owner).collect();
    anyhow::ensure!(got == want, "load_all must return every owner, got {got:?}");
    Ok(())
}

#[test]
fn forget_removes_entry_and_is_noop_when_absent() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(3);
    tbl(&db).record(&s)?;
    tbl(&db).forget(s.owner)?;
    anyhow::ensure!(tbl(&db).load_all()?.pools.is_empty());
    // forget on an unknown owner is a no-op.
    tbl(&db).forget(address!("00000000000000000000000000000000000000ff"))?;
    Ok(())
}

/// A buyer record stamped with a future schema version must NOT fail
/// An OLDER record is rejected, not decoded.
///
/// This is why the version is matched exactly rather than as a ceiling. The
/// fields are positional, so a record in a different layout — one without
/// `chain_id`, say — would decode the first 8 bytes of its `payment_pool`
/// into `chain_id`, and every field after it would shift, with no error. A
/// silently wrong deployment tag is the one outcome the tag exists to
/// prevent, so the reader refuses it instead.
#[test]
fn older_schema_version_is_rejected_not_misdecoded() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(1);
    let mut stored = StoredBuyerPoolState::from(&s);
    stored.schema_version = 0;
    let encoded = postcard::to_allocvec(&stored)?;
    tbl(&db).insert_raw(s.pool_id, &encoded)?;

    let load = tbl(&db).load_all()?;
    anyhow::ensure!(
        load.pools.is_empty(),
        "an older-schema record must be skipped by load_all, never hydrated",
    );
    anyhow::ensure!(load.skipped == vec![s.pool_id]);
    let err = tbl(&db)
        .get_by_pool_id(s.pool_id)
        .err()
        .ok_or_else(|| anyhow::anyhow!("an older schema must reject on get_by_pool_id"))?;
    anyhow::ensure!(
        matches!(
            err,
            StoreError::UnsupportedSchema { found, supported }
                if found == 0 && supported == BUYER_SUPPORTED_SCHEMA_VERSION
        ),
        "expected UnsupportedSchema for the older record, got {err:?}"
    );
    Ok(())
}

/// A buyer record stamped with a future schema version must NOT fail
/// hydration — that would disable the whole buyer path and the reclaim
/// sweep. `load_all` skips it; the point lookup still surfaces the precise
/// error.
#[test]
fn future_schema_version_skipped_on_hydration() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(1);
    let mut stored = StoredBuyerPoolState::from(&s);
    stored.schema_version = BUYER_SUPPORTED_SCHEMA_VERSION + 1;
    let encoded = postcard::to_allocvec(&stored)?;
    tbl(&db).insert_raw(s.pool_id, &encoded)?;

    let load = tbl(&db).load_all()?;
    anyhow::ensure!(
        load.pools.is_empty(),
        "future-schema record must be skipped, not propagated, by load_all",
    );
    anyhow::ensure!(load.skipped == vec![s.pool_id]);
    let err = tbl(&db)
        .get_by_pool_id(s.pool_id)
        .err()
        .ok_or_else(|| anyhow::anyhow!("future schema must reject on get_by_pool_id"))?;
    anyhow::ensure!(
        matches!(
            err,
            StoreError::UnsupportedSchema { found, supported }
                if found == BUYER_SUPPORTED_SCHEMA_VERSION + 1
                    && supported == BUYER_SUPPORTED_SCHEMA_VERSION,
        ),
        "expected UnsupportedSchema, got {err:?}",
    );
    Ok(())
}

/// Garbage value bytes under a real buyer key are skipped by `load_all`
/// (one bad row must not strand every other pool's deposit — PR #753
/// review), while a healthy record alongside it survives.
#[test]
fn corrupt_value_bytes_skipped_keeps_healthy() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let healthy = state(2);
    tbl(&db).record(&healthy)?;
    tbl(&db).insert_raw(state(3).pool_id, &[0u8; 8])?; // far too short

    let load = tbl(&db).load_all()?;
    anyhow::ensure!(
        load.pools.len() == 1 && load.pools.first() == Some(&healthy),
        "corrupt row must be skipped while the healthy row survives, got {load:?}",
    );
    anyhow::ensure!(
        load.skipped == vec![state(3).pool_id],
        "the skipped pool_id is the only repair handle, got {load:?}",
    );
    let err = tbl(&db)
        .get_by_pool_id(state(3).pool_id)
        .err()
        .ok_or_else(|| anyhow::anyhow!("garbage value must reject on get_by_pool_id"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("postcard decode")),
        "expected Corrupt(postcard decode), got {err:?}",
    );
    Ok(())
}

/// A record whose embedded `pool_id` doesn't match its table key is
/// skipped by `load_all`; the point lookup still surfaces `Corrupt`.
#[test]
fn pool_id_key_mismatch_skipped_on_hydration() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s_for_a = state(0xAA);
    let encoded = postcard::to_allocvec(&StoredBuyerPoolState::from(&s_for_a))?;
    // Filed under a *different* pool_id's key.
    tbl(&db).insert_raw(state(0xBB).pool_id, &encoded)?;

    let load = tbl(&db).load_all()?;
    anyhow::ensure!(
        load.pools.is_empty(),
        "pool_id/key-mismatch record must be skipped by load_all",
    );
    anyhow::ensure!(load.skipped == vec![state(0xBB).pool_id]);
    let err = tbl(&db)
        .get_by_pool_id(state(0xBB).pool_id)
        .err()
        .ok_or_else(|| anyhow::anyhow!("pool_id/key mismatch must reject on get_by_pool_id"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("does not match table key")),
        "expected Corrupt(does not match table key), got {err:?}",
    );
    Ok(())
}

/// Forward-compat: a future writer's additive trailing bytes after the
/// buyer prefix decode cleanly (`take_from_bytes`).
#[test]
fn extra_trailing_bytes_are_tolerated() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(0x42);
    let mut encoded = postcard::to_allocvec(&StoredBuyerPoolState::from(&s))?;
    encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01]);
    tbl(&db).insert_raw(s.pool_id, &encoded)?;

    let load = tbl(&db).load_all()?;
    anyhow::ensure!(load.pools.len() == 1);
    let only = load
        .pools
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing"))?;
    anyhow::ensure!(*only == s, "prefix must decode despite trailing bytes");
    Ok(())
}

/// `forget_if_pool` (compare-and-delete) deletes only the matching
/// pool — the lost-update guard for the reclaim sweep.
#[test]
fn forget_if_pool_is_compare_and_delete() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    // CAS on a never-written store is a no-op (false).
    anyhow::ensure!(!tbl(&db).forget_if_pool(state(1).owner, state(1).pool_id)?);

    let s = state(4);
    tbl(&db).record(&s)?;
    // Wrong pool id → not deleted, row survives.
    anyhow::ensure!(
        !tbl(&db).forget_if_pool(s.owner, OTHER_POOL)?,
        "mismatched pool must not delete"
    );
    anyhow::ensure!(
        tbl(&db).get_by_owner(s.owner)?.is_some(),
        "row must survive"
    );
    // Matching pool id → deleted.
    anyhow::ensure!(tbl(&db).forget_if_pool(s.owner, s.pool_id)?);
    anyhow::ensure!(tbl(&db).get_by_owner(s.owner)?.is_none());
    Ok(())
}

/// #838: a stale `advance_progress` reporting totals below the
/// committed watermark is rejected and leaves the committed watermark
/// intact.
#[test]
fn advance_progress_cannot_regress_committed_watermark() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(8);
    tbl(&db).record(&s)?;
    let lane = s
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
    let progress = s
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("missing progress"))?;

    let outcome = tbl(&db).advance_progress(
        s.owner,
        s.pool_id,
        lane,
        progress.last_bytes - U256::from(1u64),
        progress.last_amount,
    )?;
    anyhow::ensure!(
        matches!(outcome, AdvanceOutcome::Regressed(_)),
        "stale progress must be rejected, got {outcome:?}"
    );
    let stored = tbl(&db)
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
    anyhow::ensure!(
        stored.lane_progress(lane) == Some(progress),
        "committed watermark regressed"
    );
    Ok(())
}

/// A `BytesRegression` rebase anchor is behind the record on amount and
/// ahead of it on bytes. The rebase overwrites the record with it all the
/// same and advances to the totals.
#[test]
fn rebase_progress_takes_an_anchor_ahead_on_bytes() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(8);
    tbl(&db).record(&s)?;
    let lane = s
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
    let progress = s
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("missing progress"))?;
    let anchor = BuyerLaneProgress {
        last_amount: progress.last_amount - U256::from(10u64),
        last_bytes: progress.last_bytes + U256::from(4_000u64),
    };
    let totals = BuyerLaneProgress {
        last_amount: progress.last_amount + U256::from(3u64),
        last_bytes: anchor.last_bytes + U256::from(300u64),
    };
    let outcome = tbl(&db).rebase_progress(s.owner, s.pool_id, lane, anchor, totals)?;
    anyhow::ensure!(
        matches!(outcome, AdvanceOutcome::Advanced),
        "a rebase must write, got {outcome:?}"
    );
    let stored = tbl(&db)
        .get_by_owner(s.owner)?
        .and_then(|row| row.lane_progress(lane))
        .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
    anyhow::ensure!(stored == totals, "got {stored:?}");
    Ok(())
}

/// A rebase overwrites the committed watermark DOWN (the ledger moved to the
/// node's watermark), and keeps the same pool-id guard as an advance: it
/// never clobbers a row replaced by a newer open.
#[test]
fn rebase_progress_overwrites_down_and_guards_the_pool() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(8);
    tbl(&db).record(&s)?;
    let lane = s
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
    let progress = s
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("missing progress"))?;
    let anchor = BuyerLaneProgress {
        last_amount: progress.last_amount - U256::from(10u64),
        last_bytes: progress.last_bytes - U256::from(10u64),
    };
    let totals = BuyerLaneProgress {
        last_amount: anchor.last_amount + U256::from(3u64),
        last_bytes: anchor.last_bytes + U256::from(3u64),
    };

    let wrong_pool = PoolId::repeat_byte(0xEE);
    let outcome = tbl(&db).rebase_progress(s.owner, wrong_pool, lane, anchor, totals)?;
    anyhow::ensure!(
        matches!(outcome, AdvanceOutcome::PoolMismatch),
        "a rebase against a replaced pool must not write, got {outcome:?}"
    );

    let outcome = tbl(&db).rebase_progress(s.owner, s.pool_id, lane, anchor, totals)?;
    anyhow::ensure!(
        matches!(outcome, AdvanceOutcome::Advanced),
        "a rebase must write, got {outcome:?}"
    );
    let stored = tbl(&db)
        .get_by_owner(s.owner)?
        .and_then(|row| row.lane_progress(lane))
        .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
    anyhow::ensure!(
        stored == totals,
        "the rebase must overwrite down to the anchor and advance to the totals, got \
         {stored:?}"
    );

    // Totals below the anchor record the anchor alone.
    let low = BuyerLaneProgress {
        last_amount: anchor.last_amount - U256::from(1u64),
        last_bytes: anchor.last_bytes,
    };
    let outcome = tbl(&db).rebase_progress(s.owner, s.pool_id, lane, anchor, low)?;
    anyhow::ensure!(
        matches!(outcome, AdvanceOutcome::Advanced),
        "got {outcome:?}"
    );
    let stored = tbl(&db)
        .get_by_owner(s.owner)?
        .and_then(|row| row.lane_progress(lane))
        .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
    anyhow::ensure!(stored == anchor, "got {stored:?}");
    Ok(())
}

/// #838: the atomic mutators are pool-id guarded (a row replaced by a
/// newer open for the same owner is not clobbered) and report unknown
/// owners.
#[test]
fn advance_and_deposit_guard_on_pool_and_owner() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let ghost = state(1);
    let ghost_lane = ghost
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
    anyhow::ensure!(
        tbl(&db).advance_progress(
            ghost.owner,
            ghost.pool_id,
            ghost_lane,
            U256::from(1u64),
            U256::from(1u64),
        )? == AdvanceOutcome::UnknownPool
    );
    anyhow::ensure!(
        tbl(&db).add_deposit(ghost.owner, ghost.pool_id, U256::from(1u64))?
            == DepositOutcome::UnknownPool
    );

    let s = state(9);
    tbl(&db).record(&s)?;
    let lane = s
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;

    // With the table now present, an unrecorded owner must still report
    // UnknownPool. Both these calls and the never-written-store calls
    // above return from inside the write transaction.
    let absent = state(0x5A);
    let absent_lane = absent
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
    anyhow::ensure!(
        tbl(&db).advance_progress(
            absent.owner,
            absent.pool_id,
            absent_lane,
            U256::from(1u64),
            U256::from(1u64),
        )? == AdvanceOutcome::UnknownPool,
        "advance_progress on an absent row of an existing table"
    );
    anyhow::ensure!(
        tbl(&db).add_deposit(absent.owner, absent.pool_id, U256::from(1u64))?
            == DepositOutcome::UnknownPool,
        "add_deposit on an absent row of an existing table"
    );

    anyhow::ensure!(
        tbl(&db).advance_progress(
            s.owner,
            OTHER_POOL,
            lane,
            U256::from(99u64),
            U256::from(99u64),
        )? == AdvanceOutcome::PoolMismatch
    );
    anyhow::ensure!(
        tbl(&db).add_deposit(s.owner, OTHER_POOL, U256::from(1u64))?
            == DepositOutcome::PoolMismatch
    );
    let stored = tbl(&db)
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
    anyhow::ensure!(stored == s, "mismatched calls must not mutate the row");
    Ok(())
}

/// The no-row operations must not implicitly create the table on a
/// never-written store. Each opens it inside a write transaction, then
/// returns without committing; abort-on-drop must roll the creation
/// back. Committing any of these no-write paths leaves the table behind
/// and makes the corresponding assertion fail.
#[test]
fn no_row_ops_do_not_create_the_table() -> anyhow::Result<()> {
    let (_d, db) = db()?;
    let s = state(1);
    let lane = s
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;

    tbl(&db).forget(s.owner)?;
    anyhow::ensure!(table_absent(&db)?, "forget created the table");

    anyhow::ensure!(!tbl(&db).forget_if_pool(s.owner, s.pool_id)?);
    anyhow::ensure!(table_absent(&db)?, "forget_if_pool created the table");

    anyhow::ensure!(
        tbl(&db).advance_progress(s.owner, s.pool_id, lane, U256::from(1u64), U256::from(1u64),)?
            == AdvanceOutcome::UnknownPool
    );
    anyhow::ensure!(table_absent(&db)?, "advance_progress created the table");

    anyhow::ensure!(
        tbl(&db).add_deposit(s.owner, s.pool_id, U256::from(1u64))? == DepositOutcome::UnknownPool
    );
    anyhow::ensure!(table_absent(&db)?, "add_deposit created the table");

    // `record`, by contrast, is supposed to create it.
    tbl(&db).record(&s)?;
    anyhow::ensure!(!table_absent(&db)?, "record must create the table");
    Ok(())
}

/// Real deletions still honour the `Durability::Immediate` contract:
/// both unconditional and compare-and-delete removals survive a
/// database reopen.
#[test]
fn forget_deletions_survive_reopen() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("t.redb");
    let forgotten = state(0x31);
    let compare_deleted = state(0x32);
    let retained = state(0x33);
    {
        let db = Database::create(&path)?;
        tbl(&db).record(&forgotten)?;
        tbl(&db).record(&compare_deleted)?;
        tbl(&db).record(&retained)?;

        tbl(&db).forget(forgotten.owner)?;
        anyhow::ensure!(tbl(&db).forget_if_pool(compare_deleted.owner, compare_deleted.pool_id)?);
    }

    let db = Database::create(&path)?;
    anyhow::ensure!(tbl(&db).get_by_owner(forgotten.owner)?.is_none());
    anyhow::ensure!(tbl(&db).get_by_owner(compare_deleted.owner)?.is_none());
    anyhow::ensure!(
        tbl(&db).get_by_owner(retained.owner)? == Some(retained),
        "unrelated row must survive both durable deletions"
    );
    Ok(())
}

/// #838: the atomic mutators honour the `Durability::Immediate` "MUST
/// commit durably" contract — an `advance_progress` + `add_deposit`
/// survive a close/reopen, while a no-write `PoolMismatch` persists
/// nothing.
#[test]
fn advance_and_deposit_survive_reopen() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("t.redb");
    let s = state(2);
    let lane = s
        .lanes()
        .next()
        .map(|(k, _)| k)
        .ok_or_else(|| anyhow::anyhow!("fixture has no lane"))?;
    let progress = s
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("missing progress"))?;
    {
        let db = Database::create(&path)?;
        tbl(&db).record(&s)?;
        anyhow::ensure!(
            tbl(&db).advance_progress(
                s.owner,
                s.pool_id,
                lane,
                progress.last_bytes + U256::from(3_000u64),
                progress.last_amount + U256::from(30u64),
            )? == AdvanceOutcome::Advanced
        );
        anyhow::ensure!(
            tbl(&db).add_deposit(s.owner, s.pool_id, U256::from(40u64))?
                == DepositOutcome::Added(s.deposit + U256::from(40u64))
        );
        // A mismatched (no-write) call must leave nothing extra to
        // persist.
        anyhow::ensure!(
            tbl(&db).add_deposit(s.owner, OTHER_POOL, U256::from(1u64))?
                == DepositOutcome::PoolMismatch
        );
    }
    let db = Database::create(&path)?;
    let reopened = tbl(&db)
        .get_by_owner(s.owner)?
        .ok_or_else(|| anyhow::anyhow!("row vanished across reopen"))?;
    let reopened_progress = reopened
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("lane vanished across reopen"))?;
    anyhow::ensure!(reopened_progress.last_bytes == progress.last_bytes + U256::from(3_000u64));
    anyhow::ensure!(reopened_progress.last_amount == progress.last_amount + U256::from(30u64));
    anyhow::ensure!(
        reopened.deposit == s.deposit + U256::from(40u64),
        "mismatched call must not have altered the deposit"
    );
    Ok(())
}

/// #838: interleaving `add_deposit` with `advance_progress` on the same
/// owner row must lose neither the deposit accrual nor the watermark
/// advance. The pre-fix `get → mutate → record` (read outside the write
/// txn) would clobber one writer with the other's stale snapshot; the
/// atomic in-txn mutators serialise correctly.
#[test]
fn concurrent_top_up_and_progress_preserve_both() -> anyhow::Result<()> {
    const N: u64 = 300;
    let (_d, db) = db()?;
    let db = std::sync::Arc::new(db);
    let mut base = BuyerPoolState::new(
        PoolId::repeat_byte(6),
        DEPLOYMENT,
        Address::repeat_byte(6),
        address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        U256::from(1_000u64),
    );
    let lane = LaneKey {
        pool_id: base.pool_id,
        signer: base.owner,
        provider: address!("00000000000000000000000000000000000000b6"),
    };
    base.advance_lane(lane, U256::ZERO, U256::ZERO)?;
    tbl(&db).record(&base)?;
    let (owner, pool_id) = (base.owner, base.pool_id);

    let depositor = std::sync::Arc::clone(&db);
    let deposit_thread = std::thread::spawn(move || -> anyhow::Result<()> {
        for _ in 0..N {
            let outcome = tbl(&depositor).add_deposit(owner, pool_id, U256::from(1u64))?;
            anyhow::ensure!(
                matches!(outcome, DepositOutcome::Added(_)),
                "deposit outcome {outcome:?}"
            );
        }
        Ok(())
    });
    let advancer = std::sync::Arc::clone(&db);
    let progress_thread = std::thread::spawn(move || -> anyhow::Result<()> {
        for i in 1..=N {
            let outcome = tbl(&advancer).advance_progress(
                owner,
                pool_id,
                lane,
                U256::from(i) * U256::from(1_024u64),
                U256::from(i) * U256::from(10u64),
            )?;
            anyhow::ensure!(
                outcome == AdvanceOutcome::Advanced,
                "advance outcome {outcome:?}"
            );
        }
        Ok(())
    });
    deposit_thread
        .join()
        .map_err(|_| anyhow::anyhow!("deposit thread panicked"))??;
    progress_thread
        .join()
        .map_err(|_| anyhow::anyhow!("progress thread panicked"))??;

    let final_row = tbl(&db)
        .get_by_owner(owner)?
        .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
    anyhow::ensure!(
        final_row.deposit == U256::from(1_000u64) + U256::from(N),
        "lost a top-up: deposit = {}",
        final_row.deposit
    );
    let final_progress = final_row
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("lane vanished"))?;
    anyhow::ensure!(
        final_progress.last_amount == U256::from(N) * U256::from(10u64),
        "watermark not fully advanced: last_amount = {}",
        final_progress.last_amount
    );
    Ok(())
}
