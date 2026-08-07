//! Behavior tests for [`decdn_cache::ServeStore`] over [`NodeRangedStore`]
//! (#1621 Task 5, B1 of the node serve-miss driver).
//!
//! Three cases, matching the Task 4 primitives:
//! - `encode_range` round-trips to the ADR-038 header-less wire bao.
//! - `observe` is a LIVE watch: it reflects a range admitted after the watch
//!   opened, not just the snapshot at open time.
//! - `observe` on a never-admitted hash is `Err(Backend(_))`, deliberately
//!   distinct from `present_ranges`'s `Ok(absent)` for the same hash.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests

use std::time::Duration;

use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, RangedStore, RangedStoreError};
use decdn_cache::{NodeRangedStore, ServeStore};
use futures_util::StreamExt;
use iroh_blobs::Hash;

mod util;

/// Deterministic pseudo-random blob, same generator as
/// `decdn_bao_range::conformance::synth_blob` / `tests/util::make_blob`.
fn synth_blob(len: usize) -> ([u8; 32], Bytes, Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let outboard = Bytes::from(ob.data);
    (root, Bytes::from(plaintext), outboard)
}

/// Interleaved bao for `aligned`, ready to hand to `RangedStore::admit`.
fn bao_for_range(
    root: [u8; 32],
    plaintext: &[u8],
    outboard: Bytes,
    aligned: &decdn_bao_range::AlignedRange,
) -> Bytes {
    let s = usize::try_from(aligned.fetch_start()).expect("fetch_start fits usize");
    let e = usize::try_from(aligned.fetch_end()).expect("fetch_end fits usize");
    let data = plaintext.get(s..e).expect("aligned range within plaintext");
    decdn_bao_range::encode_verified_range(root, aligned, data, outboard).expect("range verifies")
}

/// `encode_range` re-encodes whatever is currently held into the ADR-038
/// header-less wire bao — the same bytes `encode_whole_blob_headerless`
/// computes independently from the plaintext + outboard. This proves the
/// serve encoding is byte-exact, not merely non-empty.
#[tokio::test]
async fn encode_range_round_trips_to_wire_bao() -> anyhow::Result<()> {
    let len = usize::try_from(CHUNK_GROUP_BYTES).expect("fits usize") + 123;
    let (root, plaintext, outboard) = synth_blob(len);
    let total = plaintext.len() as u64;

    let (engine, tmp) = util::empty_engine().await?;
    let hash = Hash::from(root);
    let store = NodeRangedStore::new(engine, hash, total);

    // Admit the whole blob.
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let bao = bao_for_range(root, &plaintext, outboard.clone(), &aligned);
    RangedStore::admit(&store, aligned, bao).await?;

    // Drain encode_range(0, 0) — "whole blob" per the trait's byte_len == 0
    // convention.
    let mut stream = ServeStore::encode_range(&store, 0, 0).await?;
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        got.extend_from_slice(&item?);
    }

    // Independently compute the expected header-less wire bao from the
    // plaintext + outboard directly (not via the store), matching the
    // reference-encoding pattern at engine.rs's
    // `local_outboard_pull_streams_full_wire` test.
    let mut want = Vec::new();
    decdn_bao_range::streaming::encode_whole_blob_headerless(
        root,
        total,
        outboard,
        &plaintext[..],
        &mut want,
    )?;

    assert_eq!(
        got, want,
        "encode_range(0, 0) must equal the independently-computed header-less wire bao"
    );

    drop(tmp);
    Ok(())
}

/// `observe` is a live watch: its first snapshot reflects what is present at
/// open time, and a LATER snapshot (after more bytes land) reflects the new
/// presence — proving it is not a one-shot snapshot frozen at open.
#[tokio::test]
async fn observe_reflects_progressive_admit() -> anyhow::Result<()> {
    let group = CHUNK_GROUP_BYTES;
    let total = 2 * group + 123;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);

    let (engine, tmp) = util::empty_engine().await?;
    let hash = Hash::from(root);
    let store = NodeRangedStore::new(engine, hash, total);

    // Admit group 0 = [0, group) before opening the watch.
    let group0 = decdn_bao_range::align_range(0, group, total)?;
    let bao0 = bao_for_range(root, &plaintext, outboard.clone(), &group0);
    RangedStore::admit(&store, group0.clone(), bao0).await?;

    let mut watch = ServeStore::observe(&store).await?;

    // First snapshot must already reflect group 0. Timeout-bounded like the
    // later ones so a regressed first-item delivery fails fast, never hangs.
    let first = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .map_err(|_| anyhow::anyhow!("observe stream yielded no first snapshot within 5s"))?
        .ok_or_else(|| anyhow::anyhow!("observe stream ended before first snapshot"))?;
    assert!(
        group0.chunk_ranges().is_subset(&first),
        "first observe snapshot must be a superset of group 0: got {first:?}"
    );

    // Admit group 1 = [group, total) AFTER the watch opened.
    let group1 = decdn_bao_range::align_range(group, total - group, total)?;
    let bao1 = bao_for_range(root, &plaintext, outboard, &group1);
    RangedStore::admit(&store, group1.clone(), bao1).await?;

    // A later snapshot must reflect BOTH groups — bounded so a missed live
    // update fails as a timeout, never an infinite hang.
    let union = group0.chunk_ranges().clone() | group1.chunk_ranges().clone();
    let mut saw_union = false;
    for _ in 0..8 {
        let snapshot = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .map_err(|_| anyhow::anyhow!("observe: timed out waiting for the group-1 update"))?
            .ok_or_else(|| anyhow::anyhow!("observe stream ended before reflecting group 1"))?;
        if union.is_subset(&snapshot) {
            saw_union = true;
            break;
        }
    }
    assert!(
        saw_union,
        "observe must eventually yield a snapshot superset of groups 0 ∪ 1"
    );

    drop(tmp);
    Ok(())
}

/// `observe` on a hash that was never admitted is `Err(Backend(_))` —
/// deliberately distinct from `present_ranges`, which returns `Ok(absent)`
/// for the same never-seen hash. iroh-blobs `observe` has no defined current
/// state for a hash it has never seen, so watching one is an error, not an
/// empty watch; pinning this here keeps it from silently regressing to a
/// hang.
#[tokio::test]
async fn observe_on_never_admitted_hash_is_backend_error() -> anyhow::Result<()> {
    let len = usize::try_from(CHUNK_GROUP_BYTES).expect("fits usize");
    let (root, _plaintext, _outboard) = synth_blob(len);
    let total = u64::try_from(len)?;

    let (engine, tmp) = util::empty_engine().await?;
    let hash = Hash::from(root);
    let store = NodeRangedStore::new(engine, hash, total);

    // Sanity: present_ranges on the same never-admitted hash is Ok(absent),
    // the asymmetry this test pins.
    let present = RangedStore::present_ranges(&store).await?;
    assert!(
        present.is_empty(),
        "present_ranges on a never-admitted hash must be Ok(absent), got {present:?}"
    );

    match ServeStore::observe(&store).await {
        Ok(_) => anyhow::bail!("observe on a never-admitted hash must error, got Ok"),
        Err(err) => assert!(
            matches!(err, RangedStoreError::Backend(_)),
            "observe on a never-admitted hash must be Backend(_), got {err:?}"
        ),
    }

    drop(tmp);
    Ok(())
}
