//! `DECDNMAN` **file manifests** — the client-side consumer for chunked
//! single-file publishing (ADR 012 § File Manifests and Reconstruction, #1183).
//!
//! A large file is split into 256 MiB chunk blobs at ingest; a postcard-encoded
//! *manifest blob* lists the ordered chunk hashes, and the manifest's own BLAKE3
//! hash is the canonical file identifier shared out-of-band. This module is the
//! download half: sniff the magic, deserialise, fetch and verify each chunk in
//! order, concatenate.
//!
//! # Not the bundle manifest
//!
//! This is a different layer from the *bundle* manifest in
//! [`super::bundle_pull`] (`appendix-bundles.md` § Relationship to file
//! manifests). That one is JSON, publisher-side, and **inter-file**: it maps
//! relative paths to blob hashes across a directory. This one is postcard,
//! ingest-side, and **intra-file**: it maps chunk indices to blob hashes within
//! one file. They compose — a bundle entry's `hash` may itself name a
//! `DECDNMAN` manifest — but they are separate types with separate magic, hence
//! the deliberately distinct `FileManifest` / `Manifest` naming.
//!
//! # Backward compatibility
//!
//! Every blob a client fetches is sniffed for the 8-byte magic. Wrong or
//! missing magic ⇒ the bytes are a raw blob and are written through unchanged,
//! so raw single-blob downloads are untouched by this path.

use std::future::Future;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};
use serde::{Deserialize, Serialize};

/// Magic header of a file-manifest blob. Postcard encodes a `[u8; 8]` as eight
/// bare bytes with no length prefix, so this is literally the blob's prefix and
/// can be sniffed before any deserialization is attempted.
pub(crate) const MAGIC: [u8; 8] = *b"DECDNMAN";

/// The only manifest version this build understands.
pub(crate) const VERSION: u8 = 1;

/// A chunked file's manifest (ADR 012 § Manifest format).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileManifest {
    /// Always [`MAGIC`]; checked before *and* after deserialization.
    pub magic: [u8; 8],
    /// Format version; [`VERSION`] today.
    pub version: u8,
    /// Reconstructed file size — the sum of the chunk sizes.
    pub total_bytes: u64,
    /// Content type, empty if unknown.
    pub mime_type: String,
    /// Basename hint, empty if not provided.
    pub filename: String,
    /// Chunks in file order; the last one is partial.
    pub chunks: Vec<ChunkEntry>,
}

/// One chunk blob of a [`FileManifest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChunkEntry {
    /// BLAKE3 hash of the chunk blob — what a `StreamRequest` asks for.
    pub hash: [u8; 32],
    /// Chunk length in bytes.
    pub size: u64,
}

/// Sniff `bytes` for a file manifest.
///
/// Returns `None` when the magic is wrong or the blob is shorter than the magic
/// — the ADR's backward-compatibility rule: those bytes are a raw blob. Returns
/// `Some(Err(_))` when the magic *does* match but the body is unusable, because
/// silently writing a corrupt manifest blob out as if it were file content would
/// be worse than failing.
pub(crate) fn sniff(bytes: &[u8]) -> Option<anyhow::Result<FileManifest>> {
    if bytes.get(..MAGIC.len()) != Some(&MAGIC[..]) {
        return None;
    }
    Some(decode(bytes))
}

/// Deserialise + validate manifest bytes whose magic already matched.
fn decode(bytes: &[u8]) -> anyhow::Result<FileManifest> {
    let manifest: FileManifest =
        postcard::from_bytes(bytes).context("decode DECDNMAN file manifest")?;
    if manifest.magic != MAGIC {
        bail!("file manifest magic mismatch after decode (truncated or corrupt blob)");
    }
    if manifest.version != VERSION {
        bail!(
            "unsupported file manifest version {} (this build supports v{VERSION})",
            manifest.version
        );
    }
    // The chunk sizes are what reconstruction actually concatenates, so a
    // `total_bytes` that disagrees with them means the manifest cannot be
    // trusted to describe the file it claims to.
    let summed = manifest
        .chunks
        .iter()
        .try_fold(0u64, |acc, c| acc.checked_add(c.size))
        .ok_or_else(|| anyhow::anyhow!("file manifest chunk sizes overflow u64"))?;
    if summed != manifest.total_bytes {
        bail!(
            "file manifest total_bytes {} disagrees with the sum of its {} chunk sizes ({summed})",
            manifest.total_bytes,
            manifest.chunks.len()
        );
    }
    Ok(manifest)
}

/// Root of the per-manifest chunk part directories: `~/.decdn/downloads`
/// (ADR 012 § Download flow).
pub(crate) fn downloads_root() -> PathBuf {
    decdn_common::cli::common::expand_tilde(Path::new("~/.decdn/downloads"))
}

/// Lowercase hex of a BLAKE3 digest — the per-manifest part-directory name.
fn hex(hash: &[u8; 32]) -> String {
    blake3::Hash::from_bytes(*hash).to_hex().to_string()
}

/// Fetch every chunk of `manifest` in order, verify each against its own BLAKE3
/// hash, and concatenate them into `output`.
///
/// Chunks land in `<downloads_root>/<manifest_hash>/chunk-<index>.part`. A part
/// file already present and hash-verified is reused rather than re-fetched, so
/// an interrupted reconstruction resumes and pays only for what is missing.
/// Fetches are strictly sequential, as the ADR's ordered download flow requires
/// (and as voucher-nonce safety on a shared channel requires anyway).
///
/// `keep_blobs` retains the verified part files after reconstruction (the
/// default) so the client can re-serve them; `false` — `--no-keep-blobs` —
/// deletes them, and the part directory, immediately.
///
/// Returns the number of bytes written to `output`.
pub(crate) async fn reconstruct<F, Fut>(
    manifest: &FileManifest,
    manifest_hash: [u8; 32],
    downloads_root: &Path,
    output: &Path,
    keep_blobs: bool,
    fetch_chunk: F,
) -> anyhow::Result<u64>
where
    F: Fn([u8; 32]) -> Fut,
    Fut: Future<Output = anyhow::Result<Vec<u8>>>,
{
    let part_dir = downloads_root.join(hex(&manifest_hash));
    std::fs::create_dir_all(&part_dir)
        .with_context(|| format!("create part dir {}", part_dir.display()))?;

    let total = manifest.chunks.len();
    let mut parts = Vec::with_capacity(total);
    for (index, chunk) in manifest.chunks.iter().enumerate() {
        let part = part_dir.join(format!("chunk-{index}.part"));
        if !part_is_verified(&part, chunk) {
            let bytes = fetch_chunk(chunk.hash)
                .await
                .with_context(|| format!("fetch chunk {}/{total}", index + 1))?;
            verify_chunk(&bytes, chunk, index)?;
            super::fetch::write_blob_atomic(&part, &bytes)
                .with_context(|| format!("write {}", part.display()))?;
        }
        parts.push(part);
    }

    let written = concat_parts(&parts, output)
        .with_context(|| format!("reconstruct into {}", output.display()))?;
    if written != manifest.total_bytes {
        bail!(
            "reconstructed {written} bytes but the manifest declares {}",
            manifest.total_bytes
        );
    }

    if !keep_blobs {
        // Best-effort: the file is already reconstructed and verified, so a
        // failure to clean up is a warning, not a failed download.
        for part in &parts {
            if let Err(e) = std::fs::remove_file(part) {
                eprintln!("warning: could not remove {}: {e}", part.display());
            }
        }
        let _ = std::fs::remove_dir(&part_dir);
    }
    Ok(written)
}

/// Whether an existing part file already holds this chunk's verified bytes.
/// Any read/IO problem simply means "not verified" — the chunk is re-fetched.
///
/// The declared size is checked from the directory entry *before* hashing: a
/// truncated part from an interrupted run is the common case here, and chunks
/// are 256 MiB, so reading one only to reject it on length is a wasted pass.
fn part_is_verified(part: &Path, chunk: &ChunkEntry) -> bool {
    let Ok(mut file) = std::fs::File::open(part) else {
        return false;
    };
    if !file.metadata().is_ok_and(|m| m.len() == chunk.size) {
        return false;
    }
    let mut hasher = blake3::Hasher::new();
    // `update_reader` over `io::copy`: it owns its buffering and retries
    // interrupted reads rather than surfacing them as a verification failure.
    if hasher.update_reader(&mut file).is_err() {
        return false;
    }
    *hasher.finalize().as_bytes() == chunk.hash
}

/// Check freshly-fetched chunk bytes against the manifest's hash and size.
fn verify_chunk(bytes: &[u8], chunk: &ChunkEntry, index: usize) -> anyhow::Result<()> {
    if bytes.len() as u64 != chunk.size {
        bail!(
            "chunk {index} is {} bytes but the manifest declares {}",
            bytes.len(),
            chunk.size
        );
    }
    let got = *blake3::hash(bytes).as_bytes();
    if got != chunk.hash {
        bail!(
            "chunk {index} failed BLAKE3 verification (expected {}, got {})",
            hex(&chunk.hash),
            hex(&got)
        );
    }
    Ok(())
}

/// Stream `parts` in order into `output`, written atomically (temp-in-dir then
/// rename). Streamed rather than buffered: a manifest's chunks are 256 MiB each
/// and the whole point is that the file need not fit in memory.
fn concat_parts(parts: &[PathBuf], output: &Path) -> std::io::Result<u64> {
    let parent = output.parent().filter(|p| !p.as_os_str().is_empty());
    let mut tmp = match parent {
        Some(p) => tempfile::NamedTempFile::new_in(p)?,
        None => tempfile::NamedTempFile::new_in(".")?,
    };
    let mut written = 0u64;
    for part in parts {
        let mut src = std::io::BufReader::new(std::fs::File::open(part)?);
        written = written.saturating_add(std::io::copy(&mut src, tmp.as_file_mut())?);
    }
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    tmp.persist(output).map_err(|e| e.error)?;
    Ok(written)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Build a manifest over `chunks`, returning it plus the encoded blob.
    fn manifest_for(chunks: &[&[u8]]) -> (FileManifest, Vec<u8>) {
        let entries: Vec<ChunkEntry> = chunks
            .iter()
            .map(|c| ChunkEntry {
                hash: *blake3::hash(c).as_bytes(),
                size: c.len() as u64,
            })
            .collect();
        let manifest = FileManifest {
            magic: MAGIC,
            version: VERSION,
            total_bytes: entries.iter().map(|e| e.size).sum(),
            mime_type: "application/octet-stream".into(),
            filename: "big.bin".into(),
            chunks: entries,
        };
        let blob = postcard::to_allocvec(&manifest).unwrap();
        (manifest, blob)
    }

    /// A chunk source backed by an in-memory map, recording every fetch so the
    /// tests can assert on re-fetch/resume behavior.
    struct Source {
        blobs: Vec<([u8; 32], Vec<u8>)>,
        fetched: RefCell<Vec<[u8; 32]>>,
    }

    impl Source {
        fn new(chunks: &[&[u8]]) -> Self {
            Self {
                blobs: chunks
                    .iter()
                    .map(|c| (*blake3::hash(c).as_bytes(), c.to_vec()))
                    .collect(),
                fetched: RefCell::new(Vec::new()),
            }
        }

        async fn get(&self, hash: [u8; 32]) -> anyhow::Result<Vec<u8>> {
            // A real chunk pull suspends on network I/O; yielding here makes
            // these futures do so too, so the in-order assertions below hold
            // across genuine suspension points rather than a straight-line poll.
            tokio::task::yield_now().await;
            self.fetched.borrow_mut().push(hash);
            self.blobs
                .iter()
                .find(|(h, _)| *h == hash)
                .map(|(_, b)| b.clone())
                .ok_or_else(|| anyhow::anyhow!("no such blob"))
        }
    }

    /// The headline of #1183: a manifest blob is a *file*, and reconstructing it
    /// yields exactly the concatenation of its chunks — the bytes the publisher
    /// split up, back in order.
    #[tokio::test]
    async fn reconstruct_concatenates_chunks_in_order() {
        let chunks: [&[u8]; 3] = [b"alpha", b"bravo", b"charlie!"];
        let (manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.bin");
        let src = Source::new(&chunks);

        let n = reconstruct(
            &manifest,
            hash,
            &dir.path().join("downloads"),
            &out,
            true,
            |h| src.get(h),
        )
        .await
        .unwrap();

        assert_eq!(n, manifest.total_bytes);
        assert_eq!(std::fs::read(&out).unwrap(), b"alphabravocharlie!");
        // In order, one fetch per chunk.
        assert_eq!(
            *src.fetched.borrow(),
            manifest.chunks.iter().map(|c| c.hash).collect::<Vec<_>>()
        );
    }

    /// Retention is the default so the client can re-serve the chunks; a second
    /// reconstruction then costs no fetches at all.
    #[tokio::test]
    async fn parts_are_retained_by_default_and_reused() {
        let chunks: [&[u8]; 2] = [b"one", b"two"];
        let (manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        let dir = tempfile::tempdir().unwrap();
        let downloads = dir.path().join("downloads");
        let out = dir.path().join("out.bin");
        let src = Source::new(&chunks);

        reconstruct(&manifest, hash, &downloads, &out, true, |h| src.get(h))
            .await
            .unwrap();
        let part_dir = downloads.join(hex(&hash));
        assert!(part_dir.join("chunk-0.part").exists());
        assert!(part_dir.join("chunk-1.part").exists());

        // Re-run: every part verifies, so nothing is fetched (and paid for) again.
        src.fetched.borrow_mut().clear();
        reconstruct(&manifest, hash, &downloads, &out, true, |h| src.get(h))
            .await
            .unwrap();
        assert!(src.fetched.borrow().is_empty());
        assert_eq!(std::fs::read(&out).unwrap(), b"onetwo");
    }

    /// A part left truncated by an interrupted run is re-fetched, not reused —
    /// the size check rejects it before the (256 MiB-scale) hash pass runs.
    #[tokio::test]
    async fn a_truncated_part_is_refetched() {
        let chunks: [&[u8]; 2] = [b"one", b"two"];
        let (manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        let dir = tempfile::tempdir().unwrap();
        let downloads = dir.path().join("downloads");
        let out = dir.path().join("out.bin");
        let src = Source::new(&chunks);

        reconstruct(&manifest, hash, &downloads, &out, true, |h| src.get(h))
            .await
            .unwrap();

        // Simulate a run cut short mid-write: chunk 1's part is short.
        let part = downloads.join(hex(&hash)).join("chunk-1.part");
        std::fs::write(&part, b"tw").unwrap();
        src.fetched.borrow_mut().clear();

        reconstruct(&manifest, hash, &downloads, &out, true, |h| src.get(h))
            .await
            .unwrap();
        assert_eq!(*src.fetched.borrow(), vec![manifest.chunks[1].hash]);
        assert_eq!(std::fs::read(&out).unwrap(), b"onetwo");
    }

    /// `--no-keep-blobs` deletes the parts immediately after reconstruction.
    #[tokio::test]
    async fn no_keep_blobs_deletes_parts() {
        let chunks: [&[u8]; 2] = [b"one", b"two"];
        let (manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        let dir = tempfile::tempdir().unwrap();
        let downloads = dir.path().join("downloads");
        let out = dir.path().join("out.bin");
        let src = Source::new(&chunks);

        reconstruct(&manifest, hash, &downloads, &out, false, |h| src.get(h))
            .await
            .unwrap();

        assert_eq!(std::fs::read(&out).unwrap(), b"onetwo");
        assert!(!downloads.join(hex(&hash)).exists());
    }

    /// A chunk whose bytes don't hash to the manifest's entry is rejected — the
    /// client verifies every chunk, not just the manifest.
    #[tokio::test]
    async fn a_chunk_that_fails_blake3_aborts_reconstruction() {
        let chunks: [&[u8]; 1] = [b"good"];
        let (manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.bin");

        let err = reconstruct(
            &manifest,
            hash,
            &dir.path().join("downloads"),
            &out,
            true,
            |_| async { Ok(b"evil".to_vec()) },
        )
        .await
        .unwrap_err();

        assert!(
            format!("{err:#}").contains("failed BLAKE3 verification"),
            "{err:#}"
        );
        assert!(!out.exists(), "no output on a failed verification");
    }

    #[test]
    fn sniff_round_trips_a_manifest() {
        let chunks: [&[u8]; 2] = [b"a", b"bb"];
        let (manifest, blob) = manifest_for(&chunks);
        let decoded = sniff(&blob).expect("magic matches").unwrap();
        assert_eq!(decoded, manifest);
    }

    /// Backward compatibility: bytes without the magic are a raw blob, and
    /// `sniff` says so rather than erroring — the raw download path is untouched.
    #[test]
    fn a_blob_without_the_magic_is_not_a_manifest() {
        assert!(sniff(b"just some file contents").is_none());
        assert!(sniff(b"DECDNMA").is_none(), "shorter than the magic");
        assert!(sniff(b"").is_none());
        assert!(sniff(b"DECDNMAX and more").is_none(), "wrong 8th byte");
    }

    /// The magic is a *prefix* check, so a raw blob that happens to start with
    /// it must not be silently mangled — it is a decode error, loudly.
    #[test]
    fn magic_with_an_undecodable_body_errors_rather_than_falling_back() {
        let err = sniff(b"DECDNMAN\xff\xff\xff")
            .expect("magic matches")
            .unwrap_err();
        assert!(format!("{err:#}").contains("file manifest"), "{err:#}");
    }

    #[test]
    fn an_unsupported_version_is_rejected() {
        let (mut manifest, _) = manifest_for(&[b"x"]);
        manifest.version = 2;
        let blob = postcard::to_allocvec(&manifest).unwrap();
        let err = sniff(&blob).expect("magic matches").unwrap_err();
        assert!(
            format!("{err:#}").contains("unsupported file manifest version 2"),
            "{err:#}"
        );
    }

    #[test]
    fn a_total_bytes_that_disagrees_with_the_chunks_is_rejected() {
        let (mut manifest, _) = manifest_for(&[b"abc"]);
        manifest.total_bytes = 99;
        let blob = postcard::to_allocvec(&manifest).unwrap();
        let err = sniff(&blob).expect("magic matches").unwrap_err();
        assert!(format!("{err:#}").contains("disagrees"), "{err:#}");
    }
}
