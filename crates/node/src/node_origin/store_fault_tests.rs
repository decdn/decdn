//! How the node scores an ingest into its [`CacheStore`] that fails: the
//! store's own failure is this node's fault, a wire cut short is the peer's.

use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{
    IROH_BLOCK_SIZE, IngestStore, StashedFault, align_range, encode_verified_range,
};
use decdn_cache::{CacheEngine, CacheStore};
use iroh_io::AsyncStreamReader;

use super::{PullVerdict, pull_verdict};

/// Deterministic pseudo-random blob, matching the generator the cache
/// crate's own `admit_bao_stream` tests use (`crates/cache/src/engine.rs`).
fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    (*ob.root.as_bytes(), plaintext, Bytes::from(ob.data))
}

/// A leg's wire in memory, with no fault to park.
struct MemReader {
    wire: Bytes,
}

impl AsyncStreamReader for MemReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        let take = self.wire.len().min(len);
        Ok(self.wire.split_to(take))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        if self.wire.len() < L {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "MemReader exhausted before a fixed-size bao read",
            ));
        }
        let got = self.wire.split_to(L);
        let mut out = [0u8; L];
        out.copy_from_slice(&got);
        Ok(out)
    }
}

impl StashedFault for MemReader {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
    }
}

/// The header-less bao wire for the second chunk group of a synthetic
/// 4-group blob, with its root, size and aligned range.
fn interior_range_wire() -> ([u8; 32], u64, decdn_bao_range::AlignedRange, Bytes) {
    let group = decdn_cache::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let aligned = align_range(group, group, total).expect("align interior group");
    let s = aligned.fetch_start() as usize;
    let e = aligned.fetch_end() as usize;
    let combined = encode_verified_range(root, &aligned, &plaintext[s..e], outboard)
        .expect("encode verified range");
    (root, total, aligned, combined.slice(8..))
}

/// Set every directory under `root` to `mode`, so a store opened there can
/// (or cannot) create files.
#[cfg(unix)]
fn chmod_dirs(root: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                dirs.push(entry.path());
            }
        }
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode)).unwrap();
    }
}

/// The store failing under an ingest is this node's own fault (#2286). The
/// failure comes from the real store, not a hand-built error: iroh-blobs
/// reports a failed data-file write without its `io::ErrorKind`, so the
/// verdict cannot rest on the kind.
#[cfg(unix)]
#[tokio::test]
async fn a_store_that_cannot_write_is_a_local_fault_not_the_peers() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let (root, total, aligned, wire) = interior_range_wire();
    let store = CacheStore::new(engine, decdn_cache::Hash::from(root), total);

    chmod_dirs(tmp.path(), 0o555);
    if std::fs::File::create(tmp.path().join("probe")).is_ok() {
        // Running as root: directory modes do not stop it, so there is no
        // store fault to observe.
        chmod_dirs(tmp.path(), 0o755);
        return;
    }
    let err = IngestStore::ingest_stream(
        &store,
        &aligned,
        MemReader { wire },
        None,
        aligned.blob_size(),
        None,
    )
    .await
    .err();
    chmod_dirs(tmp.path(), 0o755);
    let err = err.expect("a store that cannot create its data file fails the ingest");
    assert_eq!(pull_verdict(&err), PullVerdict::OurLocalFault, "{err:#}");
}

/// A wire the peer cut short fails the same ingest, and that is the peer's
/// short delivery: it still scores `Unreachable`.
#[tokio::test]
async fn a_truncated_wire_is_the_peers_fault() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let (root, total, aligned, wire) = interior_range_wire();
    let store = CacheStore::new(engine, decdn_cache::Hash::from(root), total);

    let cut = wire.slice(..wire.len() / 2);
    let err = IngestStore::ingest_stream(
        &store,
        &aligned,
        MemReader { wire: cut },
        None,
        aligned.blob_size(),
        None,
    )
    .await
    .err()
    .expect("a truncated wire fails the ingest");
    assert_eq!(pull_verdict(&err), PullVerdict::Unreachable, "{err:#}");
}
