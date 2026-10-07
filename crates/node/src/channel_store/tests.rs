use super::*;
use alloy::primitives::{address, b256};
use std::sync::Arc;
use tempfile::TempDir;

/// The deployment the tests' stores bind to.
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 421_614,
    payment_pool: address!("00000000000000000000000000000000000000ce"),
};

fn data_dir() -> anyhow::Result<TempDir> {
    let dir = TempDir::new()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}

/// A lane with a `signer` distinct from `provider`, so a decoder that
/// transposed the two — or dropped a key segment — fails the assertion
/// instead of round-tripping by coincidence.
fn sample(byte: u8) -> LaneState {
    let mut pool = [0u8; 32];
    pool[31] = byte;
    let mut signer = [0u8; 20];
    signer[19] = byte;
    LaneState::hydrate(
        B256::from(pool),
        Address::from(signer),
        address!("00000000000000000000000000000000000000de"),
        U256::from(10_000_000u64),
        1_900_000_000 + u64::from(byte),
        U256::from(byte) * U256::from(1_000u64),
        U256::from(byte) * U256::from(1_024u64),
        Some([byte; 65]),
        LaneChain::NONE,
    )
}

/// Build a hydrated [`LaneState`] with the given identity + spending cap.
/// `pool_byte` seeds the pool id, `signer_byte` the signer; the provider is
/// a fixed non-signer address so a signer/provider transposition would be
/// caught.
fn mk_lane(pool_byte: u8, signer_byte: u8, cap: u64) -> LaneState {
    let mut pool = [0u8; 32];
    pool[31] = pool_byte;
    let mut signer = [0u8; 20];
    signer[19] = signer_byte;
    LaneState::hydrate(
        B256::from(pool),
        Address::from(signer),
        address!("00000000000000000000000000000000000000de"),
        U256::from(cap),
        1_900_000_000,
        U256::from(1_234u64),
        U256::from(4_096u64),
        Some([0xABu8; 65]),
        LaneChain::NONE,
    )
}

/// The owner-signed capability material rides the lane record: a lane
/// recorded with an `owner_sig` and flushed reads back with the same
/// `owner_sig` after a reopen. The material and the voucher frontier are one
/// row committed in one transaction, so the redeemer's registration material
/// can never be durable-out-of-step with the frontier it backs (the #1906
/// stranding gap).
#[test]
fn owner_sig_rides_the_lane_record_through_flush_and_reopen() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    let mut lane = sample(0x5a);
    lane.owner_sig = Some([0x42u8; 65]);
    PoolStateStore::record(store.as_ref(), &lane)?;
    store.flush()?;
    drop(store);

    // Reopen from disk: an in-memory read would prove nothing about the
    // postcard round-trip.
    let reopened = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let got = PoolStateStore::get(&reopened, lane.key())?
        .ok_or_else(|| anyhow::anyhow!("lane gone after reopen"))?;
    anyhow::ensure!(
        got.owner_sig == Some([0x42u8; 65]),
        "owner_sig survives the reopen with the frontier it backs"
    );
    Ok(())
}

#[test]
fn open_empty_store_returns_no_entries() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    anyhow::ensure!(store.load_all()?.is_empty());
    Ok(())
}

#[test]
fn record_then_load_round_trip() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let a = sample(1);
    let b = sample(2);
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
fn record_persists_across_reopen() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let s = sample(7);
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        store.record(&s)?;
        store.flush()?;
    }
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let all = store.load_all()?;
    anyhow::ensure!(all.len() == 1);
    let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
    anyhow::ensure!(*only == s);
    Ok(())
}

#[test]
fn forget_removes_entry() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let s = sample(3);
    store.record(&s)?;
    store.forget(s.key())?;
    anyhow::ensure!(store.load_all()?.is_empty());
    // forget on a never-recorded lane is a no-op
    store.forget(LaneKey {
        pool_id: b256!("1111111111111111111111111111111111111111111111111111111111111111"),
        signer: address!("00000000000000000000000000000000000000a1"),
        provider: address!("00000000000000000000000000000000000000b2"),
    })?;
    Ok(())
}

#[test]
fn get_round_trips_and_reports_unknown() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let s = sample(9);
    // get on a never-written store (no table yet) is None, not an error.
    anyhow::ensure!(store.get(s.key())?.is_none());

    store.record(&s)?;
    let got = store
        .get(s.key())?
        .ok_or_else(|| anyhow::anyhow!("expected Some"))?;
    anyhow::ensure!(got == s, "get must round-trip incl. signature");
    anyhow::ensure!(
        store
            .get(LaneKey {
                pool_id: b256!("2222222222222222222222222222222222222222222222222222222222222222"),
                signer: s.signer,
                provider: s.provider,
            })?
            .is_none(),
        "unknown lane -> None"
    );
    Ok(())
}

/// Two lanes that share a `pool_id` but differ in `signer` are distinct rows
/// — the lane key is the full `(pool_id, signer, provider)` triple, not the
/// pool id alone. Also proves the decoder recovers `signer`/`provider` from
/// the key rather than transposing them.
#[test]
fn lanes_sharing_a_pool_are_distinct_rows() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let base = sample(1);
    let sibling = LaneState::hydrate(
        base.pool_id,
        address!("00000000000000000000000000000000000000c1"),
        base.provider,
        base.cap,
        base.expiry,
        base.last_amount(),
        base.last_bytes_delivered(),
        base.last_signature().copied(),
        LaneChain::NONE,
    );
    store.record(&base)?;
    store.record(&sibling)?;
    anyhow::ensure!(
        store.load_all()?.len() == 2,
        "distinct signers, distinct rows"
    );
    let got = store
        .get(sibling.key())?
        .ok_or_else(|| anyhow::anyhow!("sibling missing"))?;
    anyhow::ensure!(
        got == sibling,
        "sibling lane must round-trip its own signer"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn file_mode_is_owner_only() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = data_dir()?;
    let _store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    // Every family file is hardened to 0o600, not just the lane store.
    for file in [
        LANES_DB_FILE,
        SETTLE_DB_FILE,
        CHECKPOINT_DB_FILE,
        BUYER_DB_FILE,
    ] {
        let path = dir.path().join(file);
        anyhow::ensure!(path.exists(), "{file} must be created at open()");
        let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
        anyhow::ensure!(
            mode == DB_FILE_MODE,
            "{file} mode {mode:o} != expected {DB_FILE_MODE:o}"
        );
    }
    Ok(())
}

/// `decdn_common::data_dir::DAEMON_STORE_FILES` claims that a daemon's
/// bring-up creates every file in the set and nothing outside it. That
/// claim is what `daemon_marker` — and therefore the CLI's node-vs-client
/// classification (#2078) — rests on, so it is checked here rather than
/// asserted in prose. A fifth store added to `open_with` without a matching
/// entry in the set fails this test.
#[test]
fn open_creates_exactly_the_daemon_store_files() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let _store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;

    for file in decdn_common::data_dir::DAEMON_STORE_FILES {
        anyhow::ensure!(
            dir.path().join(file).exists(),
            "{file} is in DAEMON_STORE_FILES but open() did not create it"
        );
    }

    for entry in std::fs::read_dir(dir.path())? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".redb") {
            continue;
        }
        anyhow::ensure!(
            decdn_common::data_dir::DAEMON_STORE_FILES.contains(&name.as_ref()),
            "open() created {name}, which DAEMON_STORE_FILES does not name"
        );
    }
    Ok(())
}

/// **Cleanup-asymmetry regression (#527 follow-up).** On `tighten_permissions`
/// failure for a FRESHLY-CREATED file, the cleanup branch MUST remove the
/// partial file so the next start sees a clean state.
#[test]
fn fresh_file_chmod_failure_removes_file() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let path_buf = dir.path().join(LANES_DB_FILE);
    anyhow::ensure!(!path_buf.exists(), "precondition: file does not exist");

    // `open_with` now hardens one file per family, so `chmod_fn` is `Fn`: it
    // builds a fresh error each call rather than moving a captured one out.
    // Failing on the first file (lanes.redb) aborts the open there.
    let err = PersistentPoolStateStore::open_with(dir.path(), DEPLOYMENT, |path| {
        Err(StoreError::PermissionTighten {
            path: path.to_path_buf(),
            source: std::io::Error::other("simulated chmod failure"),
        })
    })
    .err()
    .ok_or_else(|| anyhow::anyhow!("open_with should propagate the chmod failure"))?;

    anyhow::ensure!(
        matches!(err, StoreError::PermissionTighten { .. }),
        "expected PermissionTighten, got {err:?}",
    );
    anyhow::ensure!(
        !path_buf.exists(),
        "freshly-created file MUST be removed on chmod failure",
    );
    Ok(())
}

/// **Cleanup-asymmetry regression (#527 follow-up).** On `tighten_permissions`
/// failure for a PRE-EXISTING file, the cleanup branch MUST NOT remove the
/// file — otherwise a transient chmod error on a subsequent boot destroys
/// live voucher state and reopens the replay window.
#[test]
fn pre_existing_file_chmod_failure_preserves_file() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let recorded = sample(7);
    let recorded_key = recorded.key();

    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        store.record(&recorded)?;
        store.flush()?;
    }

    let path_buf = dir.path().join(LANES_DB_FILE);
    anyhow::ensure!(
        path_buf.exists(),
        "precondition: file exists with voucher state"
    );
    let size_before = std::fs::metadata(&path_buf)?.len();

    // `chmod_fn` is `Fn` (see `fresh_file_chmod_failure_removes_file`): fail on
    // whichever family file is offered, building a fresh error each call.
    let err = PersistentPoolStateStore::open_with(dir.path(), DEPLOYMENT, |path| {
        Err(StoreError::PermissionTighten {
            path: path.to_path_buf(),
            source: std::io::Error::other("simulated EROFS"),
        })
    })
    .err()
    .ok_or_else(|| anyhow::anyhow!("open_with should propagate the chmod failure"))?;
    anyhow::ensure!(
        matches!(err, StoreError::PermissionTighten { .. }),
        "expected PermissionTighten, got {err:?}",
    );

    anyhow::ensure!(
        path_buf.exists(),
        "pre-existing file MUST NOT be removed on chmod failure (issue #527 replay window)",
    );
    let size_after = std::fs::metadata(&path_buf)?.len();
    anyhow::ensure!(
        size_after == size_before,
        "file size MUST be unchanged ({size_before} → {size_after})",
    );

    let recovered = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let all = recovered.load_all()?;
    anyhow::ensure!(all.len() == 1);
    let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
    anyhow::ensure!(only.key() == recorded_key);
    anyhow::ensure!(*only == recorded);
    Ok(())
}

/// Fill every seller-side family a deployment change must clear — a lane,
/// a seller and a buyer pending settle, a watcher checkpoint — and flush.
fn seed_seller_state(dir: &Path, deployment: Deployment) -> anyhow::Result<LaneState> {
    let store = Arc::new(PersistentPoolStateStore::open(dir, deployment)?);
    let lane = sample(0x31);
    store.record(&lane)?;
    store.flush()?;
    let pending = PendingSettle {
        pool_id: lane.pool_id,
        settle_after: 1_700_000_000,
    };
    store.record_pending(&pending)?;
    BuyerPendingSettleStoreHandle::new(Arc::clone(&store)).record_pending(&pending)?;
    store.record_checkpoint(CheckpointKey::PoolOpened, 4_242)?;
    Ok(lane)
}

/// Whether every seller-side family `seed_seller_state` fills is empty.
fn seller_state_is_empty(store: &Arc<PersistentPoolStateStore>) -> anyhow::Result<bool> {
    let buyer = BuyerPendingSettleStoreHandle::new(Arc::clone(store));
    Ok(store.load_all()?.is_empty()
        && store.load_pending()?.is_empty()
        && buyer.load_pending()?.is_empty()
        && store.load_checkpoint(CheckpointKey::PoolOpened)?.is_none())
}

/// The stored deployment stamp, read straight from `lanes.redb`.
fn stored_stamp(dir: &Path) -> anyhow::Result<Option<Deployment>> {
    let db = Database::create(dir.join(LANES_DB_FILE))?;
    let txn = db.begin_read()?;
    let table = txn.open_table(DEPLOYMENT_TABLE)?;
    Ok(table
        .get(DEPLOYMENT_KEY)?
        .map(|guard| Deployment::from_bytes(guard.value()))
        .transpose()?)
}

/// A reopen against the same deployment keeps every seller-side row.
#[test]
fn reopen_on_the_same_deployment_keeps_seller_state() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let lane = seed_seller_state(dir.path(), DEPLOYMENT)?;

    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    anyhow::ensure!(store.load_all()? == vec![lane], "lane kept");
    anyhow::ensure!(store.load_pending()?.len() == 1, "seller pending kept");
    anyhow::ensure!(
        BuyerPendingSettleStoreHandle::new(Arc::clone(&store))
            .load_pending()?
            .len()
            == 1,
        "buyer pending kept"
    );
    anyhow::ensure!(
        store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(4_242),
        "checkpoint kept"
    );
    Ok(())
}

/// A reopen against another `PaymentPool` drops every seller-side row the
/// old deployment wrote — its pool ids repeat on the new one — and restamps
/// the store. A second reopen on the new deployment keeps the (now empty)
/// state and the new stamp.
#[test]
fn reopen_on_another_payment_pool_drops_seller_state() -> anyhow::Result<()> {
    let dir = data_dir()?;
    seed_seller_state(dir.path(), DEPLOYMENT)?;
    let redeployed = Deployment {
        payment_pool: address!("00000000000000000000000000000000000000cf"),
        ..DEPLOYMENT
    };

    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), redeployed)?);
    anyhow::ensure!(
        seller_state_is_empty(&store)?,
        "old deployment's rows dropped"
    );
    drop(store);
    anyhow::ensure!(
        stored_stamp(dir.path())? == Some(redeployed),
        "store restamped"
    );

    // New state on the new deployment survives its own reopen.
    let lane = sample(0x44);
    {
        let store = PersistentPoolStateStore::open(dir.path(), redeployed)?;
        store.record(&lane)?;
        store.flush()?;
    }
    let store = PersistentPoolStateStore::open(dir.path(), redeployed)?;
    anyhow::ensure!(
        store.load_all()? == vec![lane],
        "new deployment's lane kept"
    );
    drop(store);
    // The stamp survives the record/flush cycles above: a lost stamp would
    // silently degrade the NEXT repoint to the claim-and-keep path.
    anyhow::ensure!(
        stored_stamp(dir.path())? == Some(redeployed),
        "stamp survives record/flush cycles"
    );
    Ok(())
}

/// The chain id is part of the stamp: the same `PaymentPool` address on
/// another chain is another deployment.
#[test]
fn reopen_on_another_chain_drops_seller_state() -> anyhow::Result<()> {
    let dir = data_dir()?;
    seed_seller_state(dir.path(), DEPLOYMENT)?;
    let other_chain = Deployment {
        chain_id: 31_337,
        ..DEPLOYMENT
    };

    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), other_chain)?);
    anyhow::ensure!(seller_state_is_empty(&store)?, "other chain's rows dropped");
    Ok(())
}

/// A store with rows but no stamp is claimed by the first deployment that
/// opens it: the rows stay and the stamp is written.
#[test]
fn an_unstamped_store_is_claimed_without_dropping_rows() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let lane = seed_seller_state(dir.path(), DEPLOYMENT)?;
    {
        let db = Database::create(dir.path().join(LANES_DB_FILE))?;
        let txn = db.begin_write()?;
        anyhow::ensure!(txn.delete_table(DEPLOYMENT_TABLE)?, "stamp was present");
        txn.commit()?;
    }

    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    anyhow::ensure!(store.load_all()? == vec![lane], "lanes kept");
    // The claim keeps every family, not just the lanes.
    anyhow::ensure!(store.load_pending()?.len() == 1, "seller pending kept");
    anyhow::ensure!(
        BuyerPendingSettleStoreHandle::new(Arc::clone(&store))
            .load_pending()?
            .len()
            == 1,
        "buyer pending kept"
    );
    anyhow::ensure!(
        store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(4_242),
        "checkpoint kept"
    );
    drop(store);
    anyhow::ensure!(
        stored_stamp(dir.path())? == Some(DEPLOYMENT),
        "store stamped"
    );
    Ok(())
}

/// A reopen on the stamped deployment does not grow `lanes.redb`, which the
/// #527 file-stability guarantee rests on.
#[test]
fn reopen_on_the_same_deployment_does_not_write() -> anyhow::Result<()> {
    let dir = data_dir()?;
    seed_seller_state(dir.path(), DEPLOYMENT)?;
    let path = dir.path().join(LANES_DB_FILE);
    // Size, not bytes: redb rewrites its header on every open.
    let before = std::fs::metadata(&path)?.len();
    drop(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    let after = std::fs::metadata(&path)?.len();
    anyhow::ensure!(after == before, "lanes.redb grew ({before} → {after})");
    Ok(())
}

/// A stamp of the wrong width aborts the open instead of guessing which
/// rows to keep.
#[test]
fn a_malformed_stamp_is_corrupt() -> anyhow::Result<()> {
    let dir = data_dir()?;
    drop(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    {
        let db = Database::create(dir.path().join(LANES_DB_FILE))?;
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(DEPLOYMENT_TABLE)?;
            table.insert(DEPLOYMENT_KEY, [0u8; 3].as_slice())?;
        }
        txn.commit()?;
    }

    let err = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a malformed stamp must fail the open"))?;
    anyhow::ensure!(
        matches!(err, StoreError::Corrupt { .. }),
        "expected Corrupt, got {err:?}"
    );
    Ok(())
}

/// A boot that crashed between the settle/checkpoint clears and the
/// lanes+stamp commit leaves the OLD stamp with those tables already gone.
/// The next boot against the new deployment repeats the whole drop: clearing
/// the already-missing tables is a no-op, and the lanes and stamp land.
#[test]
fn a_crashed_drop_is_repeated_by_the_next_boot() -> anyhow::Result<()> {
    let dir = data_dir()?;
    seed_seller_state(dir.path(), DEPLOYMENT)?;
    // The crash state: settle and checkpoint tables deleted, foreign stamp
    // and lane rows intact.
    for (file, tables) in [
        (
            SETTLE_DB_FILE,
            vec!["pending_settle_v1", "buyer_pending_settle_v1"],
        ),
        (CHECKPOINT_DB_FILE, vec!["watcher_checkpoint_v1"]),
    ] {
        let db = Database::create(dir.path().join(file))?;
        let txn = db.begin_write()?;
        for name in tables {
            let table: TableDefinition<'_, &[u8; 32], u64> = TableDefinition::new(name);
            // The checkpoint table's real key type is &str, but
            // delete_table drops by NAME; the definition's types are not
            // checked on delete. Require the delete to have found the
            // table: a drifted name literal would silently turn this test
            // into a plain foreign-stamp drop.
            anyhow::ensure!(
                txn.delete_table(table)?,
                "crash-state setup: table {name} was not present"
            );
        }
        txn.commit()?;
    }

    let redeployed = Deployment {
        payment_pool: address!("00000000000000000000000000000000000000cf"),
        ..DEPLOYMENT
    };
    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), redeployed)?);
    anyhow::ensure!(seller_state_is_empty(&store)?, "the repeated drop lands");
    drop(store);
    anyhow::ensure!(
        stored_stamp(dir.path())? == Some(redeployed),
        "the repeated drop restamps"
    );
    Ok(())
}

/// A foreign stamp over a store that never flushed a lane (the lane table
/// was never created) drops cleanly: the walk tolerates the absent table
/// and the unconditional lane `delete_table` is a no-op.
#[test]
fn a_foreign_stamp_without_a_lane_table_drops_cleanly() -> anyhow::Result<()> {
    let dir = data_dir()?;
    // Open + close without recording a lane: the stamp exists, the lane
    // table does not (only a flush creates it).
    drop(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    // Precondition: the lane table really is absent — if open() ever
    // creates it eagerly, this test silently stops covering the
    // TableDoesNotExist arms.
    {
        let db = Database::create(dir.path().join(LANES_DB_FILE))?;
        let txn = db.begin_read()?;
        anyhow::ensure!(
            matches!(
                txn.open_table(LANE_TABLE),
                Err(redb::TableError::TableDoesNotExist(_))
            ),
            "precondition: the lane table must not exist before a flush"
        );
    }

    let redeployed = Deployment {
        payment_pool: address!("00000000000000000000000000000000000000cf"),
        ..DEPLOYMENT
    };
    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), redeployed)?);
    anyhow::ensure!(seller_state_is_empty(&store)?, "nothing to drop, no error");
    drop(store);
    anyhow::ensure!(
        stored_stamp(dir.path())? == Some(redeployed),
        "store restamped"
    );
    Ok(())
}

/// The forfeit audit reports the lane's FULL claim: the signed cumulative
/// plus the chain-verified frontier, minus the paid watermark — the same
/// figure the redeemer collects. `last_amount` alone would understate a
/// lane with chain progress, and an over-paid lane saturates to zero.
#[test]
fn forfeited_value_counts_the_chain_frontier() {
    let mut lane = LaneState::hydrate(
        B256::repeat_byte(0x21),
        Address::repeat_byte(0x22),
        Address::repeat_byte(0x23),
        U256::from(10_000_000u64),
        1_900_000_000,
        U256::from(1_000u64), // signed cumulative
        U256::from(4_096u64),
        Some([0x11u8; 65]),
        LaneChain {
            chain_root: B256::repeat_byte(0x31),
            chunk_price: U256::from(5u64),
            verified_index: 3, // frontier worth 15 on top of the signature
            tip: B256::repeat_byte(0x32),
        },
    );
    lane.paid_cumulative = U256::from(400u64);
    assert_eq!(
        forfeited_value(&lane),
        U256::from(1_000u64 + 15 - 400),
        "owed (signed + chain frontier) minus paid"
    );

    // A chain-extended redemption can leave paid above the signed amount:
    // the difference saturates instead of underflowing.
    lane.paid_cumulative = U256::from(2_000u64);
    assert_eq!(forfeited_value(&lane), U256::ZERO);
}

/// A corrupt row in a FOREIGN store must not abort this deployment's
/// bring-up: the rebind logs it as undecodable and drops it with the rest.
/// (A corrupt row under a MATCHING stamp still aborts — `hydrate_lanes`
/// would use it; here it is about to be deleted.)
#[test]
fn a_corrupt_foreign_lane_row_does_not_abort_the_rebind() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let lane = seed_seller_state(dir.path(), DEPLOYMENT)?;
    {
        // A garbage value under a fresh, valid 72-byte key.
        let mut key = lane_key_bytes(&lane.key());
        key[71] ^= 0xFF;
        let db = Database::create(dir.path().join(LANES_DB_FILE))?;
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(LANE_TABLE)?;
            table.insert(&key, [0xDEu8, 0xAD].as_slice())?;
        }
        txn.commit()?;
    }

    let redeployed = Deployment {
        payment_pool: address!("00000000000000000000000000000000000000cf"),
        ..DEPLOYMENT
    };
    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), redeployed)?);
    anyhow::ensure!(
        seller_state_is_empty(&store)?,
        "the corrupt row and the healthy row are both dropped"
    );
    drop(store);
    anyhow::ensure!(
        stored_stamp(dir.path())? == Some(redeployed),
        "store restamped despite the corrupt row"
    );
    Ok(())
}

/// The deployment rebind never touches `buyer.redb`: a buyer pool row
/// survives a foreign reopen. The buyer table carries its own per-row
/// deployment tag and its own drop path (`drop_foreign_row`, #2087).
#[test]
fn a_buyer_pool_row_survives_a_seller_side_drop() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let pool_id = sample(0x31).pool_id;
    {
        let store = Arc::new(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
        let buyer = BuyerPoolStoreHandle::new(Arc::clone(&store));
        buyer.record(&BuyerPoolState::new(
            pool_id,
            DEPLOYMENT,
            Address::repeat_byte(0x0a),
            Address::repeat_byte(0x0b),
            U256::from(1_000_000u64),
        ))?;
    }

    let redeployed = Deployment {
        payment_pool: address!("00000000000000000000000000000000000000cf"),
        ..DEPLOYMENT
    };
    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), redeployed)?);
    let row = BuyerPoolStoreHandle::new(Arc::clone(&store))
        .get_by_pool_id(pool_id)?
        .ok_or_else(|| anyhow::anyhow!("buyer row gone after the seller-side drop"))?;
    anyhow::ensure!(
        row.deployment == DEPLOYMENT,
        "the buyer row keeps its own deployment tag"
    );
    Ok(())
}

/// A store carrying either superseded capability table drops both at open:
/// the owner-signed material now rides the lane record itself, so the side
/// tables are dead. The migration deletes them without reading a row. The
/// lane frontier in the same file is untouched.
#[test]
fn superseded_capability_tables_are_dropped_at_open() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let recorded = sample(9);
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        store.record(&recorded)?;
        store.flush()?;
    }

    // Inject legacy `capability_v1` AND `capability_v2` rows directly, with
    // arbitrary bytes — the migration never decodes them, it deletes both
    // whole tables.
    let path = dir.path().join(LANES_DB_FILE);
    let key = pool_signer_key_bytes(B256::repeat_byte(0x51), Address::repeat_byte(0x52));
    {
        let db =
            Database::create(&path).map_err(|e| anyhow::anyhow!("open lanes.redb raw: {e}"))?;
        let txn = db.begin_write()?;
        {
            let mut v1 = txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V1)?;
            v1.insert(&key, [0xAAu8; 40].as_slice())?;
            let mut v2 = txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V2)?;
            v2.insert(&key, [0xBBu8; 17].as_slice())?;
        }
        txn.commit()?;
    }

    // Reopen through the store — the drop runs — then close it so the raw
    // read below can take the file.
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        let lanes = store.load_all()?;
        anyhow::ensure!(lanes.len() == 1, "the lane frontier survives the migration");
    }

    let db = Database::create(&path).map_err(|e| anyhow::anyhow!("reopen raw: {e}"))?;
    let read_txn = db.begin_read()?;
    anyhow::ensure!(
        matches!(
            read_txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V1),
            Err(redb::TableError::TableDoesNotExist(_))
        ),
        "the superseded capability_v1 table must be gone after open"
    );
    anyhow::ensure!(
        matches!(
            read_txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V2),
            Err(redb::TableError::TableDoesNotExist(_))
        ),
        "the superseded capability_v2 table must be gone after open"
    );
    Ok(())
}

/// A record carrying a `schema_version` higher than this binary supports —
/// `load_all` MUST refuse to decode it (per ADR 003: silently dropping
/// unknown fields is unsafe because we cannot honour the persistence
/// invariant for fields we don't understand).
#[test]
fn future_schema_version_refuses_to_load() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let s = sample(4);
    let key_bytes = lane_key_bytes(&s.key());
    let forward = StoredLaneState {
        schema_version: SUPPORTED_SCHEMA_VERSION + 1,
        ..StoredLaneState::from(&s)
    };
    let encoded = postcard::to_allocvec(&forward)?;
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        let mut wtx = store.lanes_db.begin_write()?;
        wtx.set_durability(Durability::Immediate)?;
        {
            let mut table = wtx.open_table(LANE_TABLE)?;
            table.insert(&key_bytes, encoded.as_slice())?;
        }
        wtx.commit()?;
    }
    // Hydration at open() eagerly decodes the whole table, so the future
    // schema version is caught on reopen rather than on the first `load_all`.
    let err = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected UnsupportedSchema"))?;
    anyhow::ensure!(
        matches!(err, StoreError::UnsupportedSchema { .. }),
        "{err:?}"
    );
    Ok(())
}

/// A newer writer adding an additive field appears to this reader as
/// trailing bytes; `take_from_bytes` ignores them and the record decodes
/// cleanly (the regression guard against re-introducing strict decoding).
#[test]
fn extra_trailing_bytes_are_tolerated() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let s = sample(0x42);
    let key_bytes = lane_key_bytes(&s.key());
    let mut encoded = postcard::to_allocvec(&StoredLaneState::from(&s))?;
    encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03]);
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        let mut tx = store.lanes_db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(LANE_TABLE)?;
            t.insert(&key_bytes, encoded.as_slice())?;
        }
        tx.commit()?;
    }
    // Hydration at open() reads the whole table, so reopen to pick up the
    // record this test wrote directly (bypassing the working set).
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let all = store.load_all()?;
    anyhow::ensure!(all.len() == 1);
    let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
    anyhow::ensure!(*only == s, "decoded record must equal the original");
    Ok(())
}

/// A stored signature that is neither empty nor exactly 65 bytes MUST be
/// rejected as `Corrupt` rather than silently truncated/padded into a value
/// the seller path would submit to `PaymentPool.redeem` (#751).
#[test]
fn wrong_length_signature_is_corrupt() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let s = sample(0x77);
    let key_bytes = lane_key_bytes(&s.key());
    let mut stored = StoredLaneState::from(&s);
    stored.signature = vec![0xCD; 64];
    let encoded = postcard::to_allocvec(&stored)?;
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        let mut tx = store.lanes_db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(LANE_TABLE)?;
            t.insert(&key_bytes, encoded.as_slice())?;
        }
        tx.commit()?;
    }
    // Hydration at open() eagerly decodes the whole table, so the corrupt
    // signature is caught on reopen rather than on the first `load_all`.
    let err = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)
        .err()
        .ok_or_else(|| anyhow::anyhow!("wrong-length signature must reject"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Corrupt { pool_id: Some(id), detail }
            if *id == s.pool_id && detail.contains("expected 0 or 65")),
        "expected Corrupt(...expected 0 or 65...), got {err:?}",
    );
    Ok(())
}

/// A lane record with a bad-length `owner_sig` refuses the open: the redeemer
/// must not submit malformed registration material to `PaymentPool.redeemMany`.
/// The diagnostic names the pool the operator has to repair.
#[test]
fn corrupt_owner_sig_on_a_lane_refuses_the_open() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let mut lane = sample(0xc1);
    // A well-formed lane, then corrupt only its stored owner_sig to a length
    // that is neither empty (None) nor 65 (Some).
    lane.owner_sig = Some([0x11u8; 65]);
    let key_bytes = lane_key_bytes(&lane.key());
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        let mut stored = StoredLaneState::from(&lane);
        stored.owner_sig = vec![0xFFu8; 3];
        let encoded = postcard::to_allocvec(&stored)?;
        let mut tx = store.lanes_db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(LANE_TABLE)?;
            table.insert(&key_bytes, encoded.as_slice())?;
        }
        tx.commit()?;
    }
    let err = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a corrupt owner_sig must refuse the open"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Corrupt { pool_id: Some(id), detail }
            if *id == lane.pool_id
                && detail.contains("owner capability signature is 3 bytes")),
        "expected a Corrupt naming the pool and the owner-signature length, got {err:?}",
    );
    Ok(())
}

/// The watcher scan checkpoint round-trips, overwrites in place, and
/// survives a reopen — so the next boot resumes the `PoolOpened` backfill
/// from the persisted block. An empty table reads as `None` (first boot).
#[test]
fn watcher_checkpoint_round_trip_and_persist() -> anyhow::Result<()> {
    let dir = data_dir()?;
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)?.is_none());
        store.record_checkpoint(CheckpointKey::PoolOpened, 1_000)?;
        anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(1_000));
        store.record_checkpoint(CheckpointKey::PoolOpened, 2_500)?;
        anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(2_500));
    }
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    anyhow::ensure!(
        store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(2_500),
        "checkpoint must survive a restart"
    );
    Ok(())
}

/// Pending-settle entries round-trip, re-stamp on a re-close, survive a
/// reopen, and forget cleanly (#327). Keyed by `pool_id`.
#[test]
fn pending_settle_round_trip_and_persist() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let a = PendingSettle {
        pool_id: sample(1).pool_id,
        settle_after: 1_700_000_000,
    };
    let b = PendingSettle {
        pool_id: sample(2).pool_id,
        settle_after: 1_700_000_500,
    };
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        store.record_pending(&a)?;
        store.record_pending(&b)?;
        store.record_pending(&PendingSettle {
            settle_after: 1_700_009_999,
            ..a
        })?;
    }
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let mut all = store.load_pending()?;
    all.sort_by_key(|p| p.pool_id);
    anyhow::ensure!(all.len() == 2, "overwrite must not add a row");
    let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
    anyhow::ensure!(first.settle_after == 1_700_009_999, "deadline re-stamped");
    // forget clears it.
    store.forget_pending(a.pool_id)?;
    store.forget_pending(b.pool_id)?;
    anyhow::ensure!(store.load_pending()?.is_empty());
    Ok(())
}

/// Seller and buyer pending-settle sets are isolated: a pool recorded in one
/// never appears in the other (#988).
#[test]
fn buyer_and_seller_pending_settle_sets_are_isolated() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = std::sync::Arc::new(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    let buyer = BuyerPendingSettleStoreHandle::new(std::sync::Arc::clone(&store));
    let seller_pool = b256!("aa00000000000000000000000000000000000000000000000000000000000000");
    let buyer_pool = b256!("bb00000000000000000000000000000000000000000000000000000000000000");
    PendingSettleStore::record_pending(
        store.as_ref(),
        &PendingSettle {
            pool_id: seller_pool,
            settle_after: 1_000,
        },
    )?;
    buyer.record_pending(&PendingSettle {
        pool_id: buyer_pool,
        settle_after: 2_000,
    })?;
    let seller_set = PendingSettleStore::load_pending(store.as_ref())?;
    let buyer_set = buyer.load_pending()?;
    anyhow::ensure!(
        seller_set.len() == 1 && seller_set.first().map(|p| p.pool_id) == Some(seller_pool)
    );
    anyhow::ensure!(
        buyer_set.len() == 1 && buyer_set.first().map(|p| p.pool_id) == Some(buyer_pool)
    );
    Ok(())
}

#[test]
fn record_is_buffered_until_flush_then_reload_sees_it() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let s = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    s.record(&mk_lane(1, 0xAA, 2_000_000))?;
    // Buffered in this handle immediately.
    anyhow::ensure!(s.load_all()?.len() == 1, "record visible in-memory");
    // A fresh open BEFORE flush must NOT see it (nothing fsynced yet).
    drop(s);
    let s2 = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    anyhow::ensure!(s2.load_all()?.is_empty(), "unflushed record is not durable");
    s2.record(&mk_lane(1, 0xAA, 2_000_000))?;
    s2.flush()?;
    drop(s2);
    let s3 = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    anyhow::ensure!(s3.load_all()?.len() == 1, "flushed record survives reopen");
    Ok(())
}

#[test]
fn forget_tombstone_applies_on_flush() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let s = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let lane = mk_lane(2, 0xBB, 100_000);
    s.record(&lane)?;
    s.flush()?;
    s.forget(lane.key())?;
    s.flush()?;
    drop(s);
    let s2 = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    anyhow::ensure!(s2.load_all()?.is_empty(), "flushed forget survives reopen");
    Ok(())
}

/// The last byte of a lane's `pool_id`, which [`mk_lane`] varies — a payload
/// tag that ties an encoded value to the key it belongs with.
fn pool_tag(lane: &LaneKey) -> u8 {
    lane.pool_id.as_slice().last().copied().unwrap_or_default()
}

/// [`FlushSnapshot::sort_by_table_key`] puts both batches in redb key order,
/// keeps every entry, and keeps each encoded value with its own key. `flush`
/// builds the snapshot from a work-list drained through a `HashSet`, so this
/// is the only place the ordering is fixed — redb reads a table back sorted
/// whatever order it was written in.
#[test]
fn flush_snapshot_sorts_into_table_key_order() -> anyhow::Result<()> {
    // Disjoint key sets, as `writes` and `tombstones` are in a snapshot.
    let written: Vec<LaneKey> = (0u8..8).map(|i| mk_lane(i, 0xC0, 10).key()).collect();
    let forgotten: Vec<LaneKey> = (8u8..14).map(|i| mk_lane(i, 0xC0, 10).key()).collect();
    let mut snapshot = FlushSnapshot {
        // Reversed, so the input is worst-case descending.
        writes: written
            .iter()
            .rev()
            .map(|lane| (*lane, vec![pool_tag(lane)]))
            .collect(),
        tombstones: forgotten.iter().rev().copied().collect(),
    };
    snapshot.sort_by_table_key();

    anyhow::ensure!(
        snapshot
            .writes
            .iter()
            .map(|(lane, _)| lane_key_bytes(lane))
            .is_sorted(),
        "writes ascend by table key"
    );
    anyhow::ensure!(
        snapshot.tombstones.iter().map(lane_key_bytes).is_sorted(),
        "tombstones ascend by table key"
    );
    anyhow::ensure!(
        snapshot.writes.len() == written.len() && snapshot.tombstones.len() == forgotten.len(),
        "sorting drops and duplicates nothing"
    );
    anyhow::ensure!(
        snapshot
            .writes
            .iter()
            .all(|(lane, encoded)| *encoded == vec![pool_tag(lane)]),
        "every value still travels with its own key"
    );
    Ok(())
}

/// A batch large enough to span many leaf pages flushes in one transaction and
/// hydrates whole at the next open, and it lands in redb packed rather than
/// sprawling.
///
/// `record` pushes lanes onto a work-list drained through a `HashSet`, so a
/// test cannot choose the order the snapshot is built in — only
/// [`FlushSnapshot::sort_by_table_key`] decides what redb sees. The
/// `leaf_pages` bound is therefore the one assertion that ties `flush` to that
/// sort: unsorted order measures 65-68 pages here and sorted measures 47, so
/// dropping the call from `flush` trips the bound.
#[test]
fn flush_of_a_large_batch_lands_packed_and_round_trips() -> anyhow::Result<()> {
    use redb::ReadableTableMetadata as _;

    let dir = data_dir()?;
    let mut lanes: Vec<LaneState> = Vec::with_capacity(512);
    for pool in 0u8..=255 {
        for signer in 0u8..2 {
            lanes.push(mk_lane(pool, signer, 1_000 + u64::from(pool)));
        }
    }
    {
        let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        for lane in &lanes {
            store.record(lane)?;
        }
        store.flush()?;
    }
    let store = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    anyhow::ensure!(
        store.load_all()?.len() == lanes.len(),
        "every lane hydrates after the reopen"
    );
    for lane in &lanes {
        let got = store
            .get(lane.key())?
            .ok_or_else(|| anyhow::anyhow!("lane missing after reopen"))?;
        anyhow::ensure!(got == *lane, "each lane round-trips to its own record");
    }

    let read_txn = store.lanes_db.begin_read()?;
    let leaf_pages = read_txn.open_table(LANE_TABLE)?.stats()?.leaf_pages();
    anyhow::ensure!(
        leaf_pages <= 55,
        "a sorted flush packs {} lanes into <=55 leaf pages; got {leaf_pages} \
         (unsorted measures 65-68, so this is `flush` skipping the sort)",
        lanes.len()
    );
    Ok(())
}

#[test]
fn flush_when_clean_is_noop_ok() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let s = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    s.flush()?; // nothing dirty
    Ok(())
}

/// A lane whose `last_bytes_delivered` watermark is `bytes`, so a test can
/// advance one lane across rounds and check which value reached disk.
fn lane_at(pool_byte: u8, signer_byte: u8, bytes: u64) -> LaneState {
    let mut pool = [0u8; 32];
    pool[31] = pool_byte;
    let mut signer = [0u8; 20];
    signer[19] = signer_byte;
    LaneState::hydrate(
        B256::from(pool),
        Address::from(signer),
        address!("00000000000000000000000000000000000000de"),
        U256::from(1_000_000u64),
        1_900_000_000,
        U256::from(bytes),
        U256::from(bytes),
        Some([signer_byte; 65]),
        LaneChain::NONE,
    )
}

/// One flush persists a mix of dirty writes and tombstones in a single
/// commit: the surviving lanes reload, the forgotten one does not.
#[test]
fn flush_persists_mixed_writes_and_tombstones() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let keep_a = mk_lane(1, 0xA1, 100_000);
    let keep_b = mk_lane(2, 0xB2, 200_000);
    let drop_c = mk_lane(3, 0xC3, 300_000);
    {
        let s = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        // Persist drop_c first so the later forget produces a real tombstone
        // against an on-disk row, not a no-op against an absent key.
        s.record(&drop_c)?;
        s.flush()?;
        s.record(&keep_a)?;
        s.record(&keep_b)?;
        s.forget(drop_c.key())?;
        s.flush()?;
    }
    let s = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let mut all = s.load_all()?;
    all.sort_by_key(|l| l.pool_id);
    anyhow::ensure!(all.len() == 2, "one write forgotten, two survive");
    anyhow::ensure!(all.first() == Some(&keep_a));
    anyhow::ensure!(all.get(1) == Some(&keep_b));
    Ok(())
}

/// The store only needs a monotonic floor: a record that advances a lane
/// after a prior flush pushes its key afresh and the newer value reaches disk
/// on the next flush. Models the "record lands after the work-list is
/// drained" case — the key pushed after one flush is honored by the next.
#[test]
fn monotonic_remark_after_flush_reaches_disk() -> anyhow::Result<()> {
    let dir = data_dir()?;
    {
        let s = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
        s.record(&lane_at(9, 0x9A, 1_000))?;
        s.flush()?;
        // Advance the same lane, then flush again — the re-mark must win.
        s.record(&lane_at(9, 0x9A, 5_000))?;
        s.flush()?;
    }
    let s = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let all = s.load_all()?;
    anyhow::ensure!(all.len() == 1);
    let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
    anyhow::ensure!(
        only.last_bytes_delivered() == U256::from(5_000u64),
        "the newer watermark, marked after the first flush, must reach disk"
    );
    Ok(())
}

/// Records made concurrently with a continuous stream of flushes are never
/// blocked into a wedge and never lost: the fsync holds no lane shard lock,
/// so a `record` that lands mid-commit pushes its lane onto the work-list and
/// a later flush captures it. After the writers finish and one final flush
/// runs, every lane's last (highest) watermark is on disk.
#[test]
fn concurrent_records_during_flush_are_not_lost() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};

    const LANES: u8 = 16;
    const ROUNDS: u64 = 60;

    let dir = data_dir()?;
    let store = Arc::new(PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?);
    let stop = Arc::new(AtomicBool::new(false));

    // A flusher committing repeatedly while the writers advance lanes. It
    // yields between commits rather than busy-spinning (a tight loop would
    // burn a core, especially when a flush is a no-op), and surfaces any
    // flush error instead of swallowing it.
    let flusher = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || -> anyhow::Result<()> {
            while !stop.load(Ordering::Relaxed) {
                store.flush()?;
                std::thread::yield_now();
            }
            Ok(())
        })
    };

    let mut writers = Vec::new();
    for lane in 0..LANES {
        let store = Arc::clone(&store);
        writers.push(std::thread::spawn(move || -> anyhow::Result<()> {
            for round in 1..=ROUNDS {
                // Watermark strictly increases with the round, so the final
                // record for each lane carries the highest value.
                store.record(&lane_at(lane, lane, round * 4_096))?;
            }
            Ok(())
        }));
    }
    for w in writers {
        w.join()
            .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    }
    stop.store(true, Ordering::Relaxed);
    flusher
        .join()
        .map_err(|_| anyhow::anyhow!("flusher thread panicked"))??;

    // A final flush drains whatever the last records re-marked, then reopen
    // and confirm every lane reached its highest watermark on disk.
    store.flush()?;
    let store = Arc::try_unwrap(store).map_err(|_| anyhow::anyhow!("outstanding store handles"))?;
    drop(store);

    let reopened = PersistentPoolStateStore::open(dir.path(), DEPLOYMENT)?;
    let all = reopened.load_all()?;
    anyhow::ensure!(all.len() == usize::from(LANES), "every lane persisted");
    for lane in all {
        anyhow::ensure!(
            lane.last_bytes_delivered() == U256::from(ROUNDS * 4_096),
            "each lane must hold its final (highest) watermark, none lost"
        );
    }
    Ok(())
}
