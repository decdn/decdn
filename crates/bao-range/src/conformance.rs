//! Shared `RangedStore` conformance suite, run against every backend.
//!
//! Both the node (iroh-blobs cache) and client (`.partial` sidecar) backends
//! implement [`crate::RangedStore`] against a shared contract: [`run_all`]
//! drives one implementation through present/missing/admit/read/finalize over
//! prefix, interior, and disjoint holds so the two backends cannot silently
//! diverge in behavior. This module has no backend of its own — it is pure
//! test support, gated behind the `test-util` feature so no production build
//! ever links it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // test-support harness, feature-gated (never in production builds)

use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;

use crate::AlignedRange;
use crate::ranged_store::RangedStore;

/// Produces a FRESH, EMPTY `RangedStore` for one blob `(root, total_bytes)`.
/// Async because backends open files / stores. Each call must return a new
/// empty store (no bytes admitted yet) so conformance cases stay isolated
/// from one another.
pub trait ConformanceFactory {
    type Store: RangedStore;

    fn make(
        &self,
        root: [u8; 32],
        total_bytes: u64,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = Self::Store> + Send + '_>>;
}

/// `u64 -> usize`, infallible on the 64-bit hosts this test-support harness
/// runs on; a truncating value would only ever come from a broken test fixture.
fn to_usize(x: u64) -> usize {
    usize::try_from(x).expect("test fixture offsets fit usize")
}

/// Deterministic blob of `len` bytes plus its bao root and full pre-order
/// outboard. Mirrors `crates/cache/tests/range_pull.rs::make_blob` — an
/// xorshift fill, not random, so runs are reproducible.
fn synth_blob(len: usize) -> ([u8; 32], Bytes, Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        // Low byte of the state; `to_le_bytes().first()` avoids a truncating cast.
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let ob = PreOrderMemOutboard::create(&plaintext, crate::IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let outboard = Bytes::from(ob.data);
    (root, Bytes::from(plaintext), outboard)
}

/// Interleaved bao for `aligned`, ready to hand to `RangedStore::admit`.
fn bao_for_range(
    root: [u8; 32],
    plaintext: &[u8],
    outboard: Bytes,
    aligned: &AlignedRange,
) -> Bytes {
    let data = plaintext
        .get(to_usize(aligned.fetch_start())..to_usize(aligned.fetch_end()))
        .expect("aligned range within synth_blob plaintext");
    crate::encode_verified_range(root, aligned, data, outboard).expect("range verifies")
}

/// Shared fixture handed to every case: one synthetic blob plus the three
/// group-aligned cut points `k < l < m` cases admit and probe around.
struct Fixture {
    root: [u8; 32],
    total: u64,
    plaintext: Bytes,
    outboard: Bytes,
    k: u64,
    l: u64,
    m: u64,
}

impl Fixture {
    fn new() -> Self {
        let group = crate::CHUNK_GROUP_BYTES;
        let total = 3 * group + group / 2;
        let (root, plaintext, outboard) = synth_blob(to_usize(total));
        Self {
            root,
            total,
            plaintext,
            outboard,
            k: group,
            l: 2 * group,
            m: 3 * group,
        }
    }

    async fn fresh_store<F: ConformanceFactory>(&self, factory: &F) -> F::Store {
        factory.make(self.root, self.total).await
    }

    fn bao_for(&self, aligned: &AlignedRange) -> Bytes {
        bao_for_range(self.root, &self.plaintext, self.outboard.clone(), aligned)
    }

    fn align(&self, offset: u64, len: u64) -> AlignedRange {
        crate::align_range(offset, len, self.total).expect("in-bounds test fixture range")
    }
}

/// Fresh store, nothing admitted: everything is missing and nothing is
/// present or complete.
async fn case_empty<F: ConformanceFactory>(fx: &Fixture, factory: &F) {
    let store = fx.fresh_store(factory).await;
    let present = store.present_ranges().await.expect("present_ranges");
    assert!(
        present.is_empty(),
        "fresh store must have no present ranges"
    );

    let full = fx.align(0, 0);
    let missing = store.missing_ranges(0, 0).await.expect("missing_ranges");
    assert_eq!(
        &missing,
        full.chunk_ranges(),
        "empty store: everything missing"
    );

    assert!(
        !store.is_complete().await.expect("is_complete"),
        "empty store is incomplete"
    );
}

/// Admit `[0, k)`: presence is exactly the prefix, the suffix is still
/// missing, and the admitted bytes read back correctly.
async fn case_admit_prefix<F: ConformanceFactory>(fx: &Fixture, factory: &F) {
    let store = fx.fresh_store(factory).await;
    let prefix = fx.align(0, fx.k);
    store
        .admit(prefix.clone(), fx.bao_for(&prefix))
        .await
        .expect("admit prefix");

    let present = store.present_ranges().await.expect("present_ranges");
    assert_eq!(&present, prefix.chunk_ranges(), "prefix presence");

    let suffix = fx.align(fx.k, fx.total - fx.k);
    let missing = store.missing_ranges(0, 0).await.expect("missing_ranges");
    assert_eq!(
        &missing,
        suffix.chunk_ranges(),
        "prefix: suffix still missing"
    );

    let got = store.read(0, fx.k).await.expect("read prefix");
    let want = fx
        .plaintext
        .get(0..to_usize(fx.k))
        .expect("plaintext prefix");
    assert_eq!(got.as_ref(), want, "prefix round-trip");

    // Non-group-aligned sub-span strictly inside the first present group:
    // read(7, k - 14) starts and ends mid-group but stays within [0, k).
    // `read` must trim to EXACTLY this span, not return an aligned superset
    // (e.g. the whole group), so a backend cannot pass by over-fetching.
    let trim_offset = 7u64;
    let trim_len = fx.k - 2 * trim_offset;
    let got_trim = store
        .read(trim_offset, trim_len)
        .await
        .expect("read non-aligned trim");
    let want_trim = fx
        .plaintext
        .get(to_usize(trim_offset)..to_usize(trim_offset + trim_len))
        .expect("plaintext trim span");
    assert_eq!(
        got_trim.as_ref(),
        want_trim,
        "byte-exact trim: read must return exactly [offset, offset+len), no group padding"
    );
    assert_eq!(
        got_trim.len(),
        to_usize(trim_len),
        "byte-exact trim: returned length must match requested len exactly"
    );
}

/// Admit the interior group `[k, l)`: both the prefix and suffix remain
/// missing, and the interior reads back correctly.
async fn case_admit_interior<F: ConformanceFactory>(fx: &Fixture, factory: &F) {
    let store = fx.fresh_store(factory).await;
    let interior = fx.align(fx.k, fx.l - fx.k);
    store
        .admit(interior.clone(), fx.bao_for(&interior))
        .await
        .expect("admit interior");

    let present = store.present_ranges().await.expect("present_ranges");
    assert_eq!(&present, interior.chunk_ranges(), "interior presence");

    let prefix_gap = fx.align(0, fx.k);
    let suffix_gap = fx.align(fx.l, fx.total - fx.l);
    let expected_missing = prefix_gap.chunk_ranges().clone() | suffix_gap.chunk_ranges().clone();
    let missing = store.missing_ranges(0, 0).await.expect("missing_ranges");
    assert_eq!(
        missing, expected_missing,
        "interior: prefix + suffix gaps missing"
    );

    let got = store.read(fx.k, fx.l - fx.k).await.expect("read interior");
    let want = fx
        .plaintext
        .get(to_usize(fx.k)..to_usize(fx.l))
        .expect("plaintext interior");
    assert_eq!(got.as_ref(), want, "interior round-trip");
}

/// Admit `[0, k)` then `[l, m)`: presence is the two-span set, and the hole
/// `[k, l)` plus the untouched tail `[m, total)` are both missing.
async fn case_admit_disjoint<F: ConformanceFactory>(fx: &Fixture, factory: &F) {
    let store = fx.fresh_store(factory).await;
    let first = fx.align(0, fx.k);
    store
        .admit(first.clone(), fx.bao_for(&first))
        .await
        .expect("admit first");

    let second = fx.align(fx.l, fx.m - fx.l);
    store
        .admit(second.clone(), fx.bao_for(&second))
        .await
        .expect("admit second");

    let present = store.present_ranges().await.expect("present_ranges");
    let expected_present = first.chunk_ranges().clone() | second.chunk_ranges().clone();
    assert_eq!(present, expected_present, "disjoint: two-span presence");

    let hole = fx.align(fx.k, fx.l - fx.k);
    let tail = fx.align(fx.m, fx.total - fx.m);
    let expected_missing = hole.chunk_ranges().clone() | tail.chunk_ranges().clone();
    let missing = store.missing_ranges(0, 0).await.expect("missing_ranges");
    assert_eq!(missing, expected_missing, "disjoint: hole + tail missing");
}

/// Re-admitting an already-present range is a no-op: no error, presence
/// unchanged.
async fn case_idempotent<F: ConformanceFactory>(fx: &Fixture, factory: &F) {
    let store = fx.fresh_store(factory).await;
    let prefix = fx.align(0, fx.k);
    let bao = fx.bao_for(&prefix);
    store
        .admit(prefix.clone(), bao.clone())
        .await
        .expect("admit prefix");
    store
        .admit(prefix.clone(), bao)
        .await
        .expect("re-admit prefix");

    let present = store.present_ranges().await.expect("present_ranges");
    assert_eq!(
        &present,
        prefix.chunk_ranges(),
        "idempotent presence unchanged"
    );
}

/// Admitting every group makes the store complete and `finalize` succeeds.
async fn case_complete_and_finalize<F: ConformanceFactory>(fx: &Fixture, factory: &F) {
    let store = fx.fresh_store(factory).await;
    for aligned in [
        fx.align(0, fx.k),
        fx.align(fx.k, fx.l - fx.k),
        fx.align(fx.l, fx.m - fx.l),
        fx.align(fx.m, fx.total - fx.m),
    ] {
        let bao = fx.bao_for(&aligned);
        store
            .admit(aligned, bao)
            .await
            .expect("admit full coverage");
    }

    assert!(
        store.is_complete().await.expect("is_complete"),
        "fully admitted store is complete"
    );
    store.finalize().await.expect("finalize complete store");
}

/// `finalize` on a partial store errors `Incomplete` rather than succeeding.
async fn case_incomplete_finalize<F: ConformanceFactory>(fx: &Fixture, factory: &F) {
    let store = fx.fresh_store(factory).await;
    let prefix = fx.align(0, fx.k);
    store
        .admit(prefix.clone(), fx.bao_for(&prefix))
        .await
        .expect("admit prefix");

    assert!(
        !store.is_complete().await.expect("is_complete"),
        "partial store is incomplete"
    );
    let err = store
        .finalize()
        .await
        .expect_err("finalize on partial store must error");
    assert!(
        matches!(err, crate::RangedStoreError::Incomplete),
        "finalize on partial store must be Incomplete, got {err:?}"
    );
}

/// Runs every conformance case against `factory`. Test-only; asserts on
/// failure.
pub async fn run_all<F: ConformanceFactory>(factory: F) {
    let fx = Fixture::new();
    case_empty(&fx, &factory).await;
    case_admit_prefix(&fx, &factory).await;
    case_admit_interior(&fx, &factory).await;
    case_admit_disjoint(&fx, &factory).await;
    case_idempotent(&fx, &factory).await;
    case_complete_and_finalize(&fx, &factory).await;
    case_incomplete_finalize(&fx, &factory).await;
}
