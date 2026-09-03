//! End-to-end regression tests for issue #527: voucher replay across node
//! restarts. The `replay_after_restart_is_rejected` test is the canonical
//! reproduction — it drives the full
//! `PersistentPoolStateStore` → `apply_voucher` → drop → reopen →
//! `apply_voucher(replay)` path and asserts that the second `apply_voucher`
//! rejects with `PoolError::AmountRegression`, the same revert reason the
//! on-chain `PaymentPool.redeem` would surface.
//!
//! See [ADR 003 §Off-chain voucher state persistence] for the protocol rule
//! these tests guard.
//!
//! [ADR 003 §Off-chain voucher state persistence]: ../../../adr/003-payments.md

use std::path::Path;
use std::sync::Arc;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256, address, b256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::{
    LaneKey, LaneState, PoolError, PoolStateStore, SignedVoucher, StoreError, Voucher,
    voucher_domain,
};
use decdn_node::channel_store::PersistentPoolStateStore;
use tempfile::TempDir;

const CHAIN_ID: u64 = 421_614; // Arbitrum Sepolia
const VERIFYING: Address = address!("0000000000000000000000000000000000001234");
const PROVIDER: Address = address!("00000000000000000000000000000000000000b2");
const POOL_ID: B256 = b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff");

fn data_dir() -> anyhow::Result<TempDir> {
    let dir = TempDir::new()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}

/// A fresh empty lane, capability signer bound to `signer`, cap 10 USDC.
fn make_state(pool_id: B256, signer: &PrivateKeySigner) -> LaneState {
    LaneState::hydrate(
        pool_id,
        signer.address(),
        PROVIDER,
        U256::from(10_000_000u64), // 10 USDC cap
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    )
}

fn signed_voucher(
    signer: &PrivateKeySigner,
    domain: &Eip712Domain,
    pool_id: B256,
    amount: u64,
    bytes_delivered: u64,
) -> anyhow::Result<SignedVoucher> {
    let v = Voucher {
        pool_id,
        signer: signer.address(),
        provider: PROVIDER,
        amount: U256::from(amount),
        bytes_delivered: U256::from(bytes_delivered),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    };
    Ok(v.sign(signer, domain)?)
}

fn hydrate(
    store: &PersistentPoolStateStore,
    pool_id: B256,
    signer: &PrivateKeySigner,
) -> anyhow::Result<LaneState> {
    // Find the persisted lane, falling back to a fresh zero-state if absent —
    // same semantics the runtime applies during bring-up (an absent entry means
    // "lane never seen").
    let key = LaneKey {
        pool_id,
        signer: signer.address(),
        provider: PROVIDER,
    };
    match store.get(key)? {
        Some(found) => Ok(found),
        None => Ok(make_state(pool_id, signer)),
    }
}

/// **Canonical issue #527 regression.** Apply amount=5000 to a fresh store,
/// close it, reopen it, then attempt to replay a lower-amount voucher
/// (simulating a malicious client that captured an earlier voucher and
/// resubmits after node restart). The store-hydrated `last_amount=5000` MUST
/// reject the amount=3000 replay with `PoolError::AmountRegression`.
#[test]
fn replay_after_restart_is_rejected() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(CHAIN_ID, VERIFYING);

    // Phase 1: open the store, apply amount=5000, drop. Mirrors a node that
    // accepted a few vouchers and then was shut down.
    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mut state = make_state(POOL_ID, &signer);
        let v5 = signed_voucher(&signer, &domain, POOL_ID, 5_000, 5_000_000)?;
        state.apply_voucher(&v5, &domain, &store)?;
        anyhow::ensure!(state.last_amount() == U256::from(5_000u64));
        // The lane store buffers `record` in memory; flush explicitly so the
        // restart this test simulates below observes a durable write.
        store.flush()?;
    }

    // Phase 2: simulate the restart. Reopen the store, rebuild `state` from
    // the persisted lane (which is what the runtime does at bring-up), then
    // attempt a replay of amount=3000. Pre-#527 this would be accepted because
    // the fresh in-memory state had `last_amount = 0`.
    let store = PersistentPoolStateStore::open(dir.path())?;
    let mut state = hydrate(&store, POOL_ID, &signer)?;
    anyhow::ensure!(
        state.last_amount() == U256::from(5_000u64),
        "post-restart state must reflect the persisted last_amount, got {:?}",
        state.last_amount(),
    );

    let v3 = signed_voucher(&signer, &domain, POOL_ID, 3_000, 3_000_000)?;
    let err = state
        .apply_voucher(&v3, &domain, &store)
        .err()
        .ok_or_else(|| anyhow::anyhow!("replay was accepted post-restart (issue #527)"))?;
    anyhow::ensure!(
        matches!(err, PoolError::AmountRegression { .. }),
        "expected AmountRegression, got {err:?}",
    );

    // And: the in-memory state must not have advanced.
    anyhow::ensure!(state.last_amount() == U256::from(5_000u64));
    Ok(())
}

/// Tighter variant of the canonical regression: replay the *same* amount (5000)
/// after restart, not a lower one. The fence must reject equal amounts too —
/// matches the `PaymentPool.redeem` "strictly higher amount" contract invariant.
#[test]
fn replay_same_nonce_after_restart_is_rejected() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(CHAIN_ID, VERIFYING);

    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mut state = make_state(POOL_ID, &signer);
        let v5 = signed_voucher(&signer, &domain, POOL_ID, 5_000, 5_000_000)?;
        state.apply_voucher(&v5, &domain, &store)?;
        // The lane store buffers `record` in memory; flush explicitly so the
        // restart this test simulates below observes a durable write.
        store.flush()?;
    }

    let store = PersistentPoolStateStore::open(dir.path())?;
    let mut state = hydrate(&store, POOL_ID, &signer)?;
    let v5_replay = signed_voucher(&signer, &domain, POOL_ID, 5_000, 5_000_000)?;
    let err = state
        .apply_voucher(&v5_replay, &domain, &store)
        .err()
        .ok_or_else(|| anyhow::anyhow!("equal-amount replay was accepted post-restart"))?;
    anyhow::ensure!(
        matches!(err, PoolError::AmountRegression { .. }),
        "expected AmountRegression, got {err:?}",
    );
    Ok(())
}

/// After applying amount=5000 and restarting, a fresh forward-progress voucher
/// (amount=6000) must still be accepted — the persistence guard must not break
/// the legitimate path.
#[test]
fn forward_progress_after_restart_is_accepted() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(CHAIN_ID, VERIFYING);

    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mut state = make_state(POOL_ID, &signer);
        let v5 = signed_voucher(&signer, &domain, POOL_ID, 5_000, 5_000_000)?;
        state.apply_voucher(&v5, &domain, &store)?;
        // The lane store buffers `record` in memory; flush explicitly so the
        // restart this test simulates below observes a durable write.
        store.flush()?;
    }

    let store = PersistentPoolStateStore::open(dir.path())?;
    let mut state = hydrate(&store, POOL_ID, &signer)?;
    let v6 = signed_voucher(&signer, &domain, POOL_ID, 6_000, 6_000_000)?;
    state.apply_voucher(&v6, &domain, &store)?;
    anyhow::ensure!(state.last_amount() == U256::from(6_000u64));
    anyhow::ensure!(state.last_bytes_delivered() == U256::from(6_000_000u64));
    Ok(())
}

/// `registered_until` (the observed on-chain capability expiry watermark)
/// must survive a `record` → `flush` → reopen → `get` round trip, the same
/// durability guarantee the `last_*` voucher fields already have.
#[test]
fn registered_until_round_trips_across_restart() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();

    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mut state = make_state(POOL_ID, &signer);
        state.registered_until = 1_800_000_000;
        store.record(&state)?;
        store.flush()?;
    }

    let store = PersistentPoolStateStore::open(dir.path())?;
    let key = LaneKey {
        pool_id: POOL_ID,
        signer: signer.address(),
        provider: PROVIDER,
    };
    let found = store
        .get(key)?
        .ok_or_else(|| anyhow::anyhow!("lane not found after reopen"))?;
    anyhow::ensure!(
        found.registered_until == 1_800_000_000,
        "registered_until did not survive persistence round trip, got {}",
        found.registered_until,
    );
    Ok(())
}

/// `set_registered_until` writes must survive a `flush` → reopen round trip
/// like `record` does, and once persisted, a later voucher-path `record`
/// carrying `registered_until: 0` (the shape `apply_voucher` produces for a
/// lane it has not yet observed a registration for) must not regress the
/// persisted value after reopen — the clobber-safety guarantee this store
/// exists to provide.
#[test]
fn set_registered_until_survives_restart_and_resists_regression() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();

    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        let state = make_state(POOL_ID, &signer);
        store.record(&state)?;
        store.set_registered_until(state.key(), 1_800_000_000)?;
        store.flush()?;
    }

    let key = LaneKey {
        pool_id: POOL_ID,
        signer: signer.address(),
        provider: PROVIDER,
    };

    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        let found = store
            .get(key)?
            .ok_or_else(|| anyhow::anyhow!("lane not found after reopen"))?;
        anyhow::ensure!(
            found.registered_until == 1_800_000_000,
            "set_registered_until did not survive persistence round trip, got {}",
            found.registered_until,
        );

        // Simulate a subsequent voucher acceptance, whose `record` carries the
        // voucher path's unknown-registration shape (registered_until: 0).
        let mut regressed = found.clone();
        regressed.registered_until = 0;
        store.record(&regressed)?;
        store.flush()?;
    }

    let store = PersistentPoolStateStore::open(dir.path())?;
    let found = store
        .get(key)?
        .ok_or_else(|| anyhow::anyhow!("lane not found after second reopen"))?;
    anyhow::ensure!(
        found.registered_until == 1_800_000_000,
        "a later record with registered_until=0 regressed the persisted value after restart, got {}",
        found.registered_until,
    );
    Ok(())
}

/// Two distinct lanes with interleaved monotonic voucher progressions must both
/// reach their expected terminal state — verifies the store does not
/// cross-contaminate entries across lane keys. Same-lane concurrent acceptance
/// is intentionally out of scope: ADR 003 requires callers to serialise per-lane
/// writes, and the `cdn/client/v1` handler does so by holding a per-lane mutex
/// around voucher application (see the `lanes` map in `handlers/client/mod.rs`);
/// this test only exercises the cross-lane independence the store itself must
/// provide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_vouchers_across_distinct_channels() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
    let domain = voucher_domain(CHAIN_ID, VERIFYING);

    let signer_a = PrivateKeySigner::random();
    let signer_b = PrivateKeySigner::random();
    let pool_a = b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let pool_b = b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    let cap = U256::from(1_000_000_000u64);
    let state_a = LaneState::hydrate(
        pool_a,
        signer_a.address(),
        PROVIDER,
        cap,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );
    let state_b = LaneState::hydrate(
        pool_b,
        signer_b.address(),
        PROVIDER,
        cap,
        0,
        U256::ZERO,
        U256::ZERO,
        None,
        decdn_incentive::LaneChain::NONE,
    );

    // Drive 50 monotonic vouchers per lane concurrently. Each task runs its
    // `apply_voucher` calls inside `spawn_blocking` because the store
    // implementation is sync (matches the runtime's required call pattern).
    let store_a = Arc::clone(&store);
    let store_b = Arc::clone(&store);
    let domain_a = domain.clone();
    let domain_b = domain.clone();
    let addr_a = signer_a.address();
    let addr_b = signer_b.address();

    let task_a = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || -> anyhow::Result<U256> {
            let mut state = state_a;
            for i in 1u64..=50 {
                let v = Voucher {
                    pool_id: pool_a,
                    signer: addr_a,
                    provider: PROVIDER,
                    amount: U256::from(i) * U256::from(100u64),
                    bytes_delivered: U256::from(i) * U256::from(1_024u64),
                    chain_root: B256::ZERO,
                    chunk_price: U256::ZERO,
                }
                .sign(&signer_a, &domain_a)?;
                state.apply_voucher(&v, &domain_a, &*store_a)?;
            }
            Ok(state.last_amount())
        })
        .await?
    });

    let task_b = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || -> anyhow::Result<U256> {
            let mut state = state_b;
            for i in 1u64..=50 {
                let v = Voucher {
                    pool_id: pool_b,
                    signer: addr_b,
                    provider: PROVIDER,
                    amount: U256::from(i) * U256::from(200u64),
                    bytes_delivered: U256::from(i) * U256::from(2_048u64),
                    chain_root: B256::ZERO,
                    chunk_price: U256::ZERO,
                }
                .sign(&signer_b, &domain_b)?;
                state.apply_voucher(&v, &domain_b, &*store_b)?;
            }
            Ok(state.last_amount())
        })
        .await?
    });

    let final_a = task_a.await??;
    let final_b = task_b.await??;
    anyhow::ensure!(final_a == U256::from(5_000u64));
    anyhow::ensure!(final_b == U256::from(10_000u64));

    // Both lanes' final state is in the store's working set (`load_all` reads the
    // buffer; this test does not exercise the flush).
    let all = store.load_all()?;
    anyhow::ensure!(all.len() == 2, "expected two lanes, got {}", all.len());
    let a_final = all
        .iter()
        .find(|s| s.pool_id == pool_a)
        .ok_or_else(|| anyhow::anyhow!("lane A missing"))?;
    let b_final = all
        .iter()
        .find(|s| s.pool_id == pool_b)
        .ok_or_else(|| anyhow::anyhow!("lane B missing"))?;
    anyhow::ensure!(a_final.last_amount() == U256::from(5_000u64));
    anyhow::ensure!(b_final.last_amount() == U256::from(10_000u64));
    Ok(())
}

/// A non-redb (garbage) file at the expected path must cause `open` to
/// fail with a backend error that names the store path. Silently treating
/// a corrupt store as empty would silently forfeit the issue #527
/// guarantee. The strict `matches!` check (rather than `is_err`) ensures a
/// future refactor cannot make this test vacuously pass by short-circuiting
/// in some unrelated path.
#[test]
fn corrupt_file_refuses_to_start() -> anyhow::Result<()> {
    let dir = data_dir()?;
    // Write 4 KiB of zeros where redb's header should be — guaranteed to
    // miss the redb magic and non-zero length (so the empty-file guard
    // doesn't catch it first).
    let path = dir.path().join("lanes.redb");
    std::fs::write(&path, vec![0u8; 4096])?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    let err = PersistentPoolStateStore::open(dir.path())
        .err()
        .ok_or_else(|| anyhow::anyhow!("open() should reject corrupt file"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Backend(msg) if msg.contains("lanes.redb")),
        "expected StoreError::Backend(...lanes.redb...), got {err:?}",
    );
    Ok(())
}

/// Truncating an otherwise-valid redb file (to a non-zero size below the
/// header) must likewise refuse to open with a backend error.
#[test]
fn truncated_file_refuses_to_start() -> anyhow::Result<()> {
    let dir = data_dir()?;
    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        // Force one commit so the file has real redb structure.
        store.record(&LaneState::hydrate(
            b256!("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
            address!("00000000000000000000000000000000000000aa"),
            PROVIDER,
            U256::from(1_000_000u64),
            0,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        ))?;
    }
    let path = dir.path().join("lanes.redb");
    let f = std::fs::OpenOptions::new().write(true).open(&path)?;
    // Truncate to a tiny size — well below any valid redb header, but
    // non-zero so the empty-file guard isn't the rejecter.
    f.set_len(32)?;
    drop(f);
    let err = PersistentPoolStateStore::open(dir.path())
        .err()
        .ok_or_else(|| anyhow::anyhow!("open() should reject truncated file"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Backend(msg) if msg.contains("lanes.redb")),
        "expected StoreError::Backend(...lanes.redb...), got {err:?}",
    );
    Ok(())
}

/// Zero-length file at the expected path must be rejected with a `Corrupt`
/// error mentioning issue #527. Regression for the silent-bypass class:
/// `redb::Database::create` happily treats a length-0 file as "create a
/// fresh database," which would re-open the replay window if not caught
/// upstream.
#[test]
fn zero_length_file_refuses_to_start() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let path = dir.path().join("lanes.redb");
    // Touch an empty file.
    std::fs::write(&path, b"")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    let err = PersistentPoolStateStore::open(dir.path())
        .err()
        .ok_or_else(|| anyhow::anyhow!("open() should reject zero-length file"))?;
    anyhow::ensure!(
        matches!(
            &err,
            StoreError::Corrupt { pool_id: None, detail } if detail.contains("#527"),
        ),
        "expected Corrupt {{ pool_id: None, detail: ...#527... }}, got {err:?}",
    );
    Ok(())
}

/// A `data_dir` with group/world permission bits set must be rejected by the
/// `ensure_data_dir` guard before the store touches it — same security
/// posture as the node key path in `decdn_common::identity`.
#[cfg(unix)]
#[test]
fn bad_permissions_rejected() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new()?;
    // Group-readable — explicitly insecure.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))?;
    let res = PersistentPoolStateStore::open(dir.path());
    anyhow::ensure!(
        res.is_err(),
        "open() must reject a data_dir that is not 0700"
    );
    Ok(())
}

/// Sanity check: the parent of the redb file is `data_dir`, not a sibling.
#[test]
fn channels_db_lives_in_data_dir() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = PersistentPoolStateStore::open(dir.path())?;
    let parent = store
        .path()
        .parent()
        .ok_or_else(|| anyhow::anyhow!("store path has no parent"))?;
    anyhow::ensure!(
        parent == Path::new(dir.path()),
        "store file must live directly in data_dir; parent was {}",
        parent.display(),
    );
    Ok(())
}

/// #988: the buyer pending-settle set must be ISOLATED from the seller's, even
/// though both live in one redb file. A buyer unilateral close recorded via the
/// `BuyerPendingSettleStoreHandle` must never surface in the seller's
/// `load_pending` (which the seller settle sweep drains) and vice versa — that
/// isolation is what keeps the two settle sweeps and their metric families
/// (#989) from finalizing or mis-attributing each other's closes. Survives a
/// reopen, since both tables are durable in the same file.
#[test]
fn buyer_and_seller_pending_settle_sets_are_isolated() -> anyhow::Result<()> {
    use decdn_incentive::{PendingSettle, PendingSettleStore};
    use decdn_node::channel_store::BuyerPendingSettleStoreHandle;

    let dir = data_dir()?;
    let seller_pool = b256!("aa00000000000000000000000000000000000000000000000000000000000000");
    let buyer_pool = b256!("bb00000000000000000000000000000000000000000000000000000000000000");

    {
        let concrete = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let seller: Arc<dyn PendingSettleStore> = concrete.clone();
        let buyer: Arc<dyn PendingSettleStore> =
            Arc::new(BuyerPendingSettleStoreHandle::new(Arc::clone(&concrete)));

        seller.record_pending(&PendingSettle {
            pool_id: seller_pool,
            settle_after: 1_000,
        })?;
        buyer.record_pending(&PendingSettle {
            pool_id: buyer_pool,
            settle_after: 2_000,
        })?;

        let seller_pending = seller.load_pending()?;
        let buyer_pending = buyer.load_pending()?;
        anyhow::ensure!(
            seller_pending.len() == 1
                && seller_pending.first().map(|e| e.pool_id) == Some(seller_pool),
            "seller set must hold only the seller close, got {seller_pending:?}"
        );
        anyhow::ensure!(
            buyer_pending.len() == 1
                && buyer_pending.first().map(|e| e.pool_id) == Some(buyer_pool),
            "buyer set must hold only the buyer close, got {buyer_pending:?}"
        );

        // Forgetting one side never touches the other.
        buyer.forget_pending(buyer_pool)?;
        anyhow::ensure!(
            buyer.load_pending()?.is_empty(),
            "buyer forget drops the buyer entry"
        );
        anyhow::ensure!(
            seller.load_pending()?.len() == 1,
            "seller entry is untouched by a buyer forget"
        );
    }

    // Reopen: durability + isolation both survive a restart.
    let concrete = Arc::new(PersistentPoolStateStore::open(dir.path())?);
    let seller: Arc<dyn PendingSettleStore> = concrete.clone();
    let buyer: Arc<dyn PendingSettleStore> =
        Arc::new(BuyerPendingSettleStoreHandle::new(Arc::clone(&concrete)));
    anyhow::ensure!(
        seller.load_pending()?.len() == 1 && buyer.load_pending()?.is_empty(),
        "the seller entry persists and the buyer set is still empty after reopen"
    );
    Ok(())
}

/// `record_bucket` running concurrently with the periodic lane flush (#1783).
/// Floor-loss writes and lane writes now land in separate redb files
/// (`floor-loss.redb` and `lanes.redb`), so they no longer share a writer slot;
/// `record_bucket` still aborts (rather than fsyncs) its no-op write, which saves
/// the fsync and keeps it off `floor-loss.redb`'s own writer slot (#1780). Three
/// tasks run concurrently: a lane driving 50 monotonic vouchers through
/// `apply_voucher` (each a buffered lane write), a pool walking its bucket snapshot
/// forward through advancing and non-advancing (abort-path, older-timestamp)
/// `record_bucket` calls, and a pool cycling record/forget. Lane state, the freshest
/// bucket snapshot, and the forget tombstone must each land intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_bucket_races_the_lane_flush() -> anyhow::Result<()> {
    use decdn_incentive::PoolFloorLossStore;

    let dir = data_dir()?;
    let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
    let domain = voucher_domain(CHAIN_ID, VERIFYING);

    let lane_signer = PrivateKeySigner::random();
    let lane_pool = b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let loss_pool = b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let churn_pool = b256!("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    let loss_signer = address!("00000000000000000000000000000000000000a1");
    let churn_signer = address!("00000000000000000000000000000000000000b2");

    let lane_task = {
        let store = Arc::clone(&store);
        let domain = domain.clone();
        let signer = lane_signer.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<U256> {
            let mut state = make_state(lane_pool, &signer);
            for i in 1u64..=50 {
                let v = signed_voucher(&signer, &domain, lane_pool, i * 100, i * 1_024)?;
                state.apply_voucher(&v, &domain, &*store)?;
            }
            Ok(state.last_amount())
        })
    };

    let loss_task = {
        let store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || -> Result<(), StoreError> {
            for i in 1u64..=200 {
                store.record_bucket(loss_pool, loss_signer, u128::from(i) * 10, i)?;
                // Non-advancing snapshot (older timestamp): the no-op abort path,
                // interleaved with the lane flush's committing writes.
                store.record_bucket(loss_pool, loss_signer, 5, 0)?;
            }
            Ok(())
        })
    };

    let churn_task = {
        let store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || -> Result<(), StoreError> {
            for i in 1u64..=50 {
                store.record_bucket(churn_pool, churn_signer, u128::from(i), i)?;
                store.forget_loss(churn_pool)?;
            }
            Ok(())
        })
    };

    let lane_final = lane_task.await??;
    loss_task.await??;
    churn_task.await??;

    anyhow::ensure!(lane_final == U256::from(5_000u64));
    let lanes = store.load_all()?;
    anyhow::ensure!(
        lanes.len() == 1 && lanes.first().map(LaneState::last_amount) == Some(U256::from(5_000u64)),
        "lane state must survive the concurrent floor-loss writers intact"
    );
    let buckets = store.load_buckets()?;
    anyhow::ensure!(
        buckets == vec![(loss_pool, loss_signer, 2_000u128, 200u64)],
        "the bucket must settle on its greatest-timestamp snapshot and the churned pool \
         must stay forgotten, got {buckets:?}"
    );
    anyhow::ensure!(
        store.sweep_forgotten()? == 1,
        "the churned pool's forget leaves exactly one tombstone for the boot sweep"
    );
    Ok(())
}

/// Bucket survival across a restart, distinguishable from unconditional-overwrite
/// semantics (#1783): the OLDER-timestamp snapshot lands LAST before the restart, so
/// a store that overwrote rather than kept the freshest would hydrate the stale value
/// — a single-write restart test cannot tell the two apart. A second pool plays the
/// #1781 race (its `record_bucket` lands after its `forget_loss`) and must stay gone
/// across the same restart, with the bring-up sweep reclaiming its tombstone.
#[test]
fn floor_bucket_restart_hydrates_the_freshest_snapshot() -> anyhow::Result<()> {
    use decdn_incentive::PoolFloorLossStore;

    let dir = data_dir()?;
    let survivor = b256!("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
    let reclaimed = b256!("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    let signer_a = address!("00000000000000000000000000000000000000a1");
    let signer_b = address!("00000000000000000000000000000000000000b2");
    {
        let store = PersistentPoolStateStore::open(dir.path())?;
        store.record_bucket(survivor, signer_a, 5_000, 200)?;
        store.record_bucket(survivor, signer_a, 3_000, 100)?; // older ts: no-op
        // A co-tenant on the SAME pool holds its own row: per-signer isolation is
        // what has to survive the restart, not just a pool-wide total.
        store.record_bucket(survivor, signer_b, 900, 100)?;
        store.record_bucket(reclaimed, signer_a, 700, 100)?;
        store.record_bucket(reclaimed, signer_b, 800, 100)?;
        store.forget_loss(reclaimed)?;
        store.record_bucket(reclaimed, signer_a, 700, 200)?; // late persist after the forget
    }
    // "Restart": reopen the same file, sweep tombstones as bring-up does, hydrate.
    let store = PersistentPoolStateStore::open(dir.path())?;
    anyhow::ensure!(
        store.sweep_forgotten()? == 1,
        "the reclaimed pool's tombstone survives the restart for the boot sweep"
    );
    let mut hydrated = store.load_buckets()?;
    hydrated.sort_by_key(|&(_, signer, _, _)| signer);
    anyhow::ensure!(
        hydrated
            == vec![
                (survivor, signer_a, 5_000u128, 200u64),
                (survivor, signer_b, 900u128, 100u64)
            ],
        "hydration must see each signer's freshest snapshot, and nothing for the \
         reclaimed pool whose every signer row went with it, got {hydrated:?}"
    );
    Ok(())
}
