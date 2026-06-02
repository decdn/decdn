//! End-to-end regression tests for issue #527: voucher replay across node
//! restarts. The `replay_after_restart_is_rejected` test is the canonical
//! reproduction — it drives the full
//! `PersistentChannelStateStore` → `apply_voucher` → drop → reopen →
//! `apply_voucher(replay)` path and asserts that the second `apply_voucher`
//! rejects with `ChannelError::NonceNotIncreasing`, the same revert reason
//! the on-chain `disputeChannel` would surface.
//!
//! See [ADR 003 §Off-chain voucher state persistence] for the protocol rule
//! these tests guard.
//!
//! [ADR 003 §Off-chain voucher state persistence]: ../../../adr/003-payments.md

use std::path::Path;
use std::sync::Arc;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256, address, b256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::{
    ChannelError, ChannelState, ChannelStateStore, SignedVoucher, StoreError, Voucher,
    voucher_domain,
};
use decdn_node::channel_store::PersistentChannelStateStore;
use tempfile::TempDir;

const CHAIN_ID: u64 = 421_614; // Arbitrum Sepolia
const VERIFYING: Address = address!("0000000000000000000000000000000000001234");
const TOKEN: Address = address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

fn data_dir() -> anyhow::Result<TempDir> {
    let dir = TempDir::new()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}

fn make_state(signer: &PrivateKeySigner) -> ChannelState {
    ChannelState::new(
        b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
        signer.address(),
        TOKEN,
        U256::from(10_000_000u64), // 10 USDC deposit
    )
}

fn signed_voucher(
    signer: &PrivateKeySigner,
    domain: &Eip712Domain,
    channel_id: alloy::primitives::B256,
    amount: u64,
    nonce: u64,
    bytes_delivered: u64,
) -> anyhow::Result<SignedVoucher> {
    let v = Voucher {
        channel_id,
        amount: U256::from(amount),
        nonce: U256::from(nonce),
        bytes_delivered: U256::from(bytes_delivered),
        token: TOKEN,
    };
    Ok(v.sign(signer, domain)?)
}

fn hydrate(
    store: &PersistentChannelStateStore,
    channel_id: alloy::primitives::B256,
    signer: &PrivateKeySigner,
) -> anyhow::Result<ChannelState> {
    // Find the persisted entry for `channel_id`, falling back to a fresh
    // zero-state if absent — same semantics the runtime applies during
    // bring-up (an absent entry means "channel never seen").
    let persisted = store.load_all()?;
    if let Some(found) = persisted.into_iter().find(|s| s.channel_id == channel_id) {
        Ok(found)
    } else {
        Ok(make_state(signer))
    }
}

/// **Canonical issue #527 regression.** Apply nonce=5 to a fresh store,
/// close it, reopen it, then attempt to replay a lower-nonce voucher
/// (simulating a malicious client that captured an earlier voucher and
/// resubmits after node restart). The store-hydrated `last_nonce=5` MUST
/// reject the nonce=3 replay with `ChannelError::NonceNotIncreasing`.
#[test]
fn replay_after_restart_is_rejected() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(CHAIN_ID, VERIFYING);
    let state0 = make_state(&signer);
    let channel_id = state0.channel_id;

    // Phase 1: open the store, apply nonce=5, drop. Mirrors a node that
    // accepted a few vouchers and then was shut down.
    {
        let store = PersistentChannelStateStore::open(dir.path())?;
        let mut state = state0.clone();
        let v5 = signed_voucher(&signer, &domain, channel_id, 5_000, 5, 5_000_000)?;
        state.apply_voucher(&v5, &domain, &store)?;
        anyhow::ensure!(state.last_nonce() == U256::from(5u64));
    }

    // Phase 2: simulate the restart. Reopen the store, rebuild `state` from
    // persisted records (which is what the runtime does at bring-up), then
    // attempt a replay of nonce=3. Pre-#527 this would be accepted because
    // the fresh in-memory state had `last_nonce = 0`.
    let store = PersistentChannelStateStore::open(dir.path())?;
    let mut state = hydrate(&store, channel_id, &signer)?;
    anyhow::ensure!(
        state.last_nonce() == U256::from(5u64),
        "post-restart state must reflect the persisted last_nonce, got {:?}",
        state.last_nonce(),
    );

    let v3 = signed_voucher(&signer, &domain, channel_id, 3_000, 3, 3_000_000)?;
    let err = state
        .apply_voucher(&v3, &domain, &store)
        .err()
        .ok_or_else(|| anyhow::anyhow!("replay was accepted post-restart (issue #527)"))?;
    anyhow::ensure!(
        matches!(err, ChannelError::NonceNotIncreasing { .. }),
        "expected NonceNotIncreasing, got {err:?}",
    );

    // And: the in-memory state must not have advanced.
    anyhow::ensure!(state.last_nonce() == U256::from(5u64));
    Ok(())
}

/// Tighter variant of the canonical regression: replay the *same* nonce (5)
/// after restart, not a lower one. The fence must reject equal nonces too —
/// matches the `disputeChannel` "strictly higher nonce" contract invariant.
#[test]
fn replay_same_nonce_after_restart_is_rejected() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(CHAIN_ID, VERIFYING);
    let state0 = make_state(&signer);
    let channel_id = state0.channel_id;

    {
        let store = PersistentChannelStateStore::open(dir.path())?;
        let mut state = state0.clone();
        let v5 = signed_voucher(&signer, &domain, channel_id, 5_000, 5, 5_000_000)?;
        state.apply_voucher(&v5, &domain, &store)?;
    }

    let store = PersistentChannelStateStore::open(dir.path())?;
    let mut state = hydrate(&store, channel_id, &signer)?;
    let v5_replay = signed_voucher(&signer, &domain, channel_id, 5_000, 5, 5_000_000)?;
    let err = state
        .apply_voucher(&v5_replay, &domain, &store)
        .err()
        .ok_or_else(|| anyhow::anyhow!("equal-nonce replay was accepted post-restart"))?;
    anyhow::ensure!(
        matches!(err, ChannelError::NonceNotIncreasing { .. }),
        "expected NonceNotIncreasing, got {err:?}",
    );
    Ok(())
}

/// After applying nonce=5 and restarting, a fresh forward-progress voucher
/// (nonce=6) must still be accepted — the persistence guard must not break
/// the legitimate path.
#[test]
fn forward_progress_after_restart_is_accepted() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let signer = PrivateKeySigner::random();
    let domain = voucher_domain(CHAIN_ID, VERIFYING);
    let state0 = make_state(&signer);
    let channel_id = state0.channel_id;

    {
        let store = PersistentChannelStateStore::open(dir.path())?;
        let mut state = state0.clone();
        let v5 = signed_voucher(&signer, &domain, channel_id, 5_000, 5, 5_000_000)?;
        state.apply_voucher(&v5, &domain, &store)?;
    }

    let store = PersistentChannelStateStore::open(dir.path())?;
    let mut state = hydrate(&store, channel_id, &signer)?;
    let v6 = signed_voucher(&signer, &domain, channel_id, 6_000, 6, 6_000_000)?;
    state.apply_voucher(&v6, &domain, &store)?;
    anyhow::ensure!(state.last_nonce() == U256::from(6u64));
    anyhow::ensure!(state.last_amount() == U256::from(6_000u64));
    Ok(())
}

/// Two distinct channels with interleaved monotonic voucher progressions
/// must both reach their expected terminal state — verifies the store does
/// not cross-contaminate entries across channel ids. Same-channel
/// concurrent acceptance is intentionally out of scope: ADR 003 requires
/// callers to serialise per-channel writes (a future `cdn/client/v1`
/// handler will hold a per-channel mutex, see `TODO(#317)` in
/// `runtime/mod.rs`); this test only exercises the cross-channel
/// independence the store itself must provide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_vouchers_across_distinct_channels() -> anyhow::Result<()> {
    let dir = data_dir()?;
    let store = Arc::new(PersistentChannelStateStore::open(dir.path())?);
    let domain = voucher_domain(CHAIN_ID, VERIFYING);

    let signer_a = PrivateKeySigner::random();
    let signer_b = PrivateKeySigner::random();
    let chan_a = b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let chan_b = b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    let deposit = U256::from(1_000_000_000u64);
    let state_a = ChannelState::new(chan_a, signer_a.address(), TOKEN, deposit);
    let state_b = ChannelState::new(chan_b, signer_b.address(), TOKEN, deposit);

    // Drive 50 monotonic vouchers per channel concurrently. Each task runs
    // its `apply_voucher` calls inside `spawn_blocking` because the store
    // implementation is sync (matches the runtime's required call pattern).
    let store_a = Arc::clone(&store);
    let store_b = Arc::clone(&store);
    let domain_a = domain.clone();
    let domain_b = domain.clone();

    let task_a = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || -> anyhow::Result<U256> {
            let mut state = state_a;
            for i in 1u64..=50 {
                let v = Voucher {
                    channel_id: chan_a,
                    amount: U256::from(i) * U256::from(100u64),
                    nonce: U256::from(i),
                    bytes_delivered: U256::from(i) * U256::from(1_024u64),
                    token: TOKEN,
                }
                .sign(&signer_a, &domain_a)?;
                state.apply_voucher(&v, &domain_a, &*store_a)?;
            }
            Ok(state.last_nonce())
        })
        .await?
    });

    let task_b = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || -> anyhow::Result<U256> {
            let mut state = state_b;
            for i in 1u64..=50 {
                let v = Voucher {
                    channel_id: chan_b,
                    amount: U256::from(i) * U256::from(200u64),
                    nonce: U256::from(i),
                    bytes_delivered: U256::from(i) * U256::from(2_048u64),
                    token: TOKEN,
                }
                .sign(&signer_b, &domain_b)?;
                state.apply_voucher(&v, &domain_b, &*store_b)?;
            }
            Ok(state.last_nonce())
        })
        .await?
    });

    let final_a = task_a.await??;
    let final_b = task_b.await??;
    anyhow::ensure!(final_a == U256::from(50u64));
    anyhow::ensure!(final_b == U256::from(50u64));

    // Both channels' final state is durable.
    let all = store.load_all()?;
    anyhow::ensure!(all.len() == 2, "expected two channels, got {}", all.len());
    let a_final = all
        .iter()
        .find(|s| s.channel_id == chan_a)
        .ok_or_else(|| anyhow::anyhow!("channel A missing"))?;
    let b_final = all
        .iter()
        .find(|s| s.channel_id == chan_b)
        .ok_or_else(|| anyhow::anyhow!("channel B missing"))?;
    anyhow::ensure!(a_final.last_nonce() == U256::from(50u64));
    anyhow::ensure!(b_final.last_nonce() == U256::from(50u64));
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
    let path = dir.path().join("channels.redb");
    std::fs::write(&path, vec![0u8; 4096])?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    let err = PersistentChannelStateStore::open(dir.path())
        .err()
        .ok_or_else(|| anyhow::anyhow!("open() should reject corrupt file"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Backend(msg) if msg.contains("channels.redb")),
        "expected StoreError::Backend(...channels.redb...), got {err:?}",
    );
    Ok(())
}

/// Truncating an otherwise-valid redb file (to a non-zero size below the
/// header) must likewise refuse to open with a backend error.
#[test]
fn truncated_file_refuses_to_start() -> anyhow::Result<()> {
    let dir = data_dir()?;
    {
        let store = PersistentChannelStateStore::open(dir.path())?;
        // Force one commit so the file has real redb structure.
        store.record(&ChannelState::new(
            b256!("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
            address!("00000000000000000000000000000000000000aa"),
            TOKEN,
            U256::from(1_000_000u64),
        ))?;
    }
    let path = dir.path().join("channels.redb");
    let f = std::fs::OpenOptions::new().write(true).open(&path)?;
    // Truncate to a tiny size — well below any valid redb header, but
    // non-zero so the empty-file guard isn't the rejecter.
    f.set_len(32)?;
    drop(f);
    let err = PersistentChannelStateStore::open(dir.path())
        .err()
        .ok_or_else(|| anyhow::anyhow!("open() should reject truncated file"))?;
    anyhow::ensure!(
        matches!(&err, StoreError::Backend(msg) if msg.contains("channels.redb")),
        "expected StoreError::Backend(...channels.redb...), got {err:?}",
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
    let path = dir.path().join("channels.redb");
    // Touch an empty file.
    std::fs::write(&path, b"")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    let err = PersistentChannelStateStore::open(dir.path())
        .err()
        .ok_or_else(|| anyhow::anyhow!("open() should reject zero-length file"))?;
    anyhow::ensure!(
        matches!(
            &err,
            StoreError::Corrupt { channel_id: None, detail } if detail.contains("#527"),
        ),
        "expected Corrupt {{ channel_id: None, detail: ...#527... }}, got {err:?}",
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
    let res = PersistentChannelStateStore::open(dir.path());
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
    let store = PersistentChannelStateStore::open(dir.path())?;
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
