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
//! [`super::bundle_pull`] (`appendix-bundles.md` § Non-relationship to ADR 012's
//! `DECDNMAN` chunk manifest). That one is JSON, publisher-side, and
//! **inter-file**: it maps relative paths to blob hashes across a directory.
//! This one is postcard, ingest-side, and **intra-file**: it maps chunk indices
//! to blob hashes within one file. They compose — a bundle entry's `hash` may
//! itself name a `DECDNMAN` manifest — but they are separate types with separate
//! framing (the bundle manifest is JSON with a `version` field and no magic),
//! hence the deliberately distinct `FileManifest` / `Manifest` naming.
//!
//! # Backward compatibility
//!
//! Every blob a client fetches is sniffed for the 8-byte magic. Wrong or
//! missing magic ⇒ the bytes are a raw blob and are written through unchanged,
//! so raw single-blob downloads are untouched by this path.

use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};
use serde::{Deserialize, Serialize};

/// Magic header of a file-manifest blob. Postcard encodes a `[u8; 8]` as eight
/// bare bytes with no length prefix, so this is literally the blob's prefix and
/// can be sniffed before any deserialization is attempted.
pub(crate) const MAGIC: [u8; 8] = *b"DECDNMAN";

/// The only manifest version this build understands.
pub(crate) const VERSION: u8 = 1;

/// Upper bound on the chunk count a manifest may declare.
///
/// This is a bound on *spending* and on part-file count, not on allocation:
/// [`decode`] materializes the whole chunk list before this is checked. That is
/// acceptable because the list cannot be conjured from a small input — serde
/// grows the vector as bytes are actually consumed, so an over-large list means
/// an over-large manifest blob, which `--max-blob-mb` already caps and which the
/// client has already paid to receive.
///
/// What the cap does buy: every chunk is a separate *paid* sequential pull and a
/// separate part file, so a manifest declaring tens of millions of them is a way
/// to spend someone else's channel balance. At ~33-37 bytes per encoded
/// [`ChunkEntry`], the default 1024 MiB blob ceiling alone would permit ~29M.
/// ADR 012 sizes the format for "10,000-chunk files", so this leaves two orders
/// of magnitude of headroom over the documented scale.
pub(crate) const MAX_CHUNKS: usize = 1_000_000;

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
    /// Chunks in file order; the last one is typically partial (a file whose
    /// size is an exact multiple of the chunk size ends on a full one).
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
    // `take_from_bytes` rather than `from_bytes`: the latter ignores trailing
    // input, so a manifest with garbage appended would decode happily. The blob
    // was fetched by hash so integrity holds either way, but this module's whole
    // posture is that a blob claiming to be a manifest and failing to be one is
    // an error rather than something to paper over.
    let (manifest, rest): (FileManifest, &[u8]) =
        postcard::take_from_bytes(bytes).context("decode DECDNMAN file manifest")?;
    if !rest.is_empty() {
        bail!(
            "file manifest has {} trailing byte(s) after a complete record",
            rest.len()
        );
    }
    if manifest.magic != MAGIC {
        bail!("file manifest magic mismatch after decode (truncated or corrupt blob)");
    }
    if manifest.version != VERSION {
        bail!(
            "unsupported file manifest version {} (this build supports v{VERSION})",
            manifest.version
        );
    }
    if manifest.chunks.len() > MAX_CHUNKS {
        bail!(
            "file manifest declares {} chunks, over the {MAX_CHUNKS} limit",
            manifest.chunks.len()
        );
    }
    // A zero-size chunk costs a part file and a pull but contributes nothing.
    // Paired with the empty blob's hash — the natural way to write one, and
    // trivially present in iroh-blobs — the pull always "succeeds", so millions
    // of them would be a file-creation storm that every other check here would
    // wave through.
    if let Some(index) = manifest.chunks.iter().position(|c| c.size == 0) {
        bail!("file manifest chunk {index} declares zero bytes");
    }
    // `filename` is a basename *hint* and is not used as a path today. Reject a
    // path-bearing one at the door so it cannot become a traversal vector the
    // day someone wires it up to a default output name.
    if manifest.filename.contains('/')
        || manifest.filename.contains('\\')
        || matches!(manifest.filename.as_str(), "." | "..")
    {
        bail!(
            "file manifest filename {:?} is not a bare basename",
            manifest.filename
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

/// Root of the per-manifest chunk part directories: `<data_dir>/downloads`
/// (ADR 012 § Download flow).
///
/// Derived from the resolved client data dir rather than a hardcoded
/// `~/.decdn/downloads` so that `--data-dir`/`identity.data_dir` governs where
/// parts land. They share a root with the buyer-channel store, which matters
/// because a chunked download is the largest thing the client writes — hundreds
/// of GB — and silently ignoring an explicit `--data-dir` for it would be a
/// surprise.
pub(crate) fn downloads_root(data_dir: &Path) -> PathBuf {
    data_dir.join("downloads")
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

    // Ensure the never-GC'd downloads root exists first: the lockfile is opened
    // there (O_CREAT) before the part dir is touched.
    blocking(
        {
            let root = downloads_root.to_path_buf();
            move || std::fs::create_dir_all(&root)
        },
        || format!("create downloads root {}", downloads_root.display()),
    )
    .await?;

    // Cross-process advisory lock (#1303). `bundle pull`'s per-hash dedup already
    // guarantees a single reconstruction of this manifest *within* one process;
    // this lock extends that guarantee across separate `decdn` invocations sharing
    // `downloads_root`, which would otherwise share this part directory — one
    // process's `--no-keep-blobs` cleanup deleting parts mid-concatenate in the
    // other, or both re-fetching and re-paying for every chunk. Held for the whole
    // reconstruction (fetch → concat → optional cleanup) and released when `_lock`
    // drops. The OS frees the lock on handle close, so a crashed holder needs no
    // staleness policy.
    let _lock = {
        let root = downloads_root.to_path_buf();
        let name = hex(&manifest_hash);
        blocking(
            move || acquire_part_lock(&root, &name),
            || format!("lock reconstruction of {}", hex(&manifest_hash)),
        )
        .await?
    };

    // Create the part dir *under* the lock. A prior holder running
    // `--no-keep-blobs` removes this dir during its (locked) cleanup, so a waiter
    // that created it pre-lock could find it gone the moment it acquires the lock;
    // the first `write_blob_atomic` would then fail `ENOENT` after already paying
    // for a chunk. Creating it here, inside the critical section, closes that.
    blocking(
        {
            let dir = part_dir.clone();
            move || std::fs::create_dir_all(&dir)
        },
        || format!("create part dir {}", part_dir.display()),
    )
    .await?;

    let total = manifest.chunks.len();
    let mut parts = Vec::with_capacity(total);
    for (index, chunk) in manifest.chunks.iter().enumerate() {
        let part = part_dir.join(format!("chunk-{index}.part"));
        // Hashing a whole chunk is far too long to sit on the executor: `bundle
        // pull` polls every entry from ONE task (`buffer_unordered`, no
        // `tokio::spawn` because `probe_once` is not `Send`), so blocking here
        // stalls every sibling entry — and their stall deadlines are wall-clock
        // timers that elapse unpolled and fire the instant we yield.
        let verified = {
            let (owned_part, chunk) = (part.clone(), chunk.clone());
            tokio::task::spawn_blocking(move || part_is_verified(&owned_part, &chunk))
                .await
                .with_context(|| {
                    format!(
                        "verify cached chunk {}/{total} at {}",
                        index + 1,
                        part.display()
                    )
                })?
        };
        if !verified {
            let bytes = fetch_chunk(chunk.hash)
                .await
                .with_context(|| format!("fetch chunk {}/{total}", index + 1))?;
            // Verify AND write on the blocking pool. `verify_chunk` is a
            // whole-chunk BLAKE3 hash — the same executor-stalling work
            // `part_is_verified` is offloaded for just above — and the cache-miss
            // path is the one place it is guaranteed to run, so it must not sit
            // on the executor either. (The transport already verifies the bytes
            // against `chunk.hash` via the bao decoder in `client-pull`; this is
            // defense-in-depth, but a whole-chunk hash all the same.) Verifying
            // before the write means a bad chunk never reaches disk.
            let (part_w, chunk_w) = (part.clone(), chunk.clone());
            blocking(
                move || -> anyhow::Result<()> {
                    verify_chunk(&bytes, &chunk_w, index)?;
                    super::fetch::write_blob_atomic(&part_w, &bytes)?;
                    Ok(())
                },
                || {
                    format!(
                        "verify and write chunk {}/{total} to {}",
                        index + 1,
                        part.display()
                    )
                },
            )
            .await?;
        }
        parts.push(part);
    }

    // The length check lives inside `concat_parts`, before it renames anything
    // into place: a mismatch must leave `output` untouched rather than publish
    // wrong bytes under the final name and merely *report* failure. `bundle
    // pull`'s skip-existing would treat such a file as verified-good forever,
    // and this module already pins "no output on a failed verification" for the
    // BLAKE3 path.
    let expected = manifest.total_bytes;
    let written = blocking(
        {
            let (parts, output) = (parts.clone(), output.to_path_buf());
            move || concat_parts(&parts, &output, expected)
        },
        || format!("reconstruct into {}", output.display()),
    )
    .await?;

    if !keep_blobs {
        // On the blocking pool for the same reason the rest of this function is:
        // a manifest may hold up to `MAX_CHUNKS` parts, and that many `unlink`
        // syscalls inline would stall every sibling `bundle pull` entry.
        let (parts, part_dir) = (parts.clone(), part_dir.clone());
        // Cleanup is best-effort: the file is already reconstructed and
        // verified, so failing to tidy up is a warning, not a failed download.
        let _ = tokio::task::spawn_blocking(move || remove_parts(&parts, &part_dir)).await;
    }
    Ok(written)
}

/// Delete `parts` and then their directory, reporting failures as one summary
/// line rather than one per file: a permissions or IO fault hits every part
/// alike, and thousands of near-identical lines is how a warning gets trained
/// out of a user's attention.
fn remove_parts(parts: &[PathBuf], part_dir: &Path) {
    let mut failed = 0usize;
    let mut first: Option<String> = None;
    for part in parts {
        if let Err(e) = std::fs::remove_file(part) {
            failed += 1;
            first.get_or_insert_with(|| format!("{}: {e}", part.display()));
        }
    }
    if let Some(first) = first {
        eprintln!(
            "warning: could not remove {failed} of {} part file(s) under {} (first: {first})",
            parts.len(),
            part_dir.display()
        );
    }
    // Not `let _ =`: `write_blob_atomic` stages its temp file *inside* this
    // directory, so a run killed mid-write leaves an orphan `.tmp` that the loop
    // above never sees. `remove_dir` then fails `ENOTEMPTY` — and since nothing
    // ever GCs the downloads root, a silent failure here is a leak that the user
    // asked us specifically to avoid.
    if let Err(e) = std::fs::remove_dir(part_dir) {
        eprintln!(
            "warning: could not remove part dir {} ({e}); leftover files may remain",
            part_dir.display()
        );
    }
}

/// Run a blocking filesystem call on the blocking pool, flattening the join
/// error and attaching `context` to the inner failure.
async fn blocking<T, E, F, C>(f: F, context: impl FnOnce() -> C) -> anyhow::Result<T>
where
    F: FnOnce() -> Result<T, E> + Send + 'static,
    T: Send + 'static,
    E: Into<anyhow::Error> + Send + 'static,
    C: std::fmt::Display + Send + Sync + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(inner) => inner.map_err(Into::into).with_context(context),
        Err(join) => Err(anyhow::Error::new(join)).with_context(context),
    }
}

/// Acquire an exclusive, cross-process advisory lock for reconstructing the
/// manifest named by `hex_name` (#1303).
///
/// The lock is a `flock` on a persistent `<downloads_root>/<hex>.lock` file — it
/// is deliberately **not** the part directory nor a file inside it. Under
/// `--no-keep-blobs` the part directory is removed at the end, and locking a
/// target that is removed-then-recreated lets two processes hold locks on
/// *different* inodes and both proceed. The lockfile lives in the never-GC'd
/// downloads root and is never removed: it is empty, and the downloads root
/// already accumulates by design (see [`remove_parts`]). `flock` releases on
/// `close`, so a crashed holder frees it with no staleness bookkeeping.
///
/// Blocking until the holder finishes is intended: the waiter then finds the
/// holder's verified parts (default retention — free) or re-fetches them
/// (`--no-keep-blobs`, a separate cross-process payment).
///
/// [`std::fs::File::lock`] (stable since Rust 1.89; MSRV here is 1.95) is a
/// cross-platform OS advisory lock — `flock` on Unix, `LockFileEx` on Windows —
/// so the CLI builds and locks on every release target including
/// `x86_64-pc-windows-msvc`, with no third-party dependency. The returned
/// [`File`](std::fs::File) *is* the lock guard: the OS releases the lock when it
/// is dropped (or the process exits), so no unlock/staleness bookkeeping is
/// needed.
fn acquire_part_lock(downloads_root: &Path, hex_name: &str) -> anyhow::Result<std::fs::File> {
    let lock_path = downloads_root.join(format!("{hex_name}.lock"));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // The lockfile is a 0-byte anchor; never truncate a concurrent holder's.
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("open lock file {}", lock_path.display()))?;
    // Exclusive, blocking; released when `file` drops.
    file.lock()
        .with_context(|| format!("lock {}", lock_path.display()))?;
    Ok(file)
}

/// Whether an existing part file already holds this chunk's verified bytes.
/// Anything else means "not verified" — the chunk is re-fetched.
///
/// The declared size is checked from the file's metadata *before* hashing, so a
/// part that cannot possibly match is rejected without reading a whole chunk.
///
/// A *missing* part is silent — that is the ordinary first-run and resume state.
/// Everything else warns, because re-fetching is not free: it is a **paid**
/// transfer of up to a whole chunk, and retention is sold on the promise of not
/// re-paying for bytes already held. A bad sector that silently costs the user
/// money on every run is exactly what these warnings exist to surface.
///
/// Note a wrong-size part is an anomaly here, not an interrupted write: parts
/// are written by [`super::fetch::write_blob_atomic`] (`O_EXCL` temp →
/// `sync_all` → rename), so this program only ever leaves a part absent or
/// complete. A short one means a foreign writer or filesystem damage, which
/// deserves the same warning as a hash mismatch.
fn part_is_verified(part: &Path, chunk: &ChunkEntry) -> bool {
    let warn = |what: &str, e: &dyn std::fmt::Display| {
        eprintln!(
            "warning: {what} for cached chunk {} ({e}); re-fetching it (a paid transfer)",
            part.display()
        );
    };

    let mut file = match std::fs::File::open(part) {
        Ok(f) => f,
        // The part simply isn't there yet: the ordinary first-run path.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
        Err(e) => {
            warn("cannot open", &e);
            return false;
        }
    };
    match file.metadata() {
        Ok(m) if m.len() == chunk.size => {}
        Ok(m) => {
            warn(
                "size mismatch",
                &format!("{} bytes on disk, {} declared", m.len(), chunk.size),
            );
            return false;
        }
        Err(e) => {
            warn("cannot stat", &e);
            return false;
        }
    }
    let mut hasher = blake3::Hasher::new();
    // `update_reader` over `io::copy`: it owns a buffer large enough for the
    // crate's SIMD implementations, where `io::copy`'s is not, rather than
    // round-tripping through `Hasher`'s `Write` impl. (Both retry
    // `ErrorKind::Interrupted`, so that is not the distinction. The exact buffer
    // size is documented as unstable, hence no figure here.)
    if let Err(e) = hasher.update_reader(&mut file) {
        warn("cannot read", &e);
        return false;
    }
    if *hasher.finalize().as_bytes() != chunk.hash {
        // Right size, wrong bytes: corruption or tampering.
        warn("BLAKE3 mismatch", &"cached bytes do not match the manifest");
        return false;
    }
    true
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
/// rename). Streamed rather than buffered: a manifest's chunks are large and the
/// whole point is that the file need not fit in memory.
///
/// `expected` is checked against the concatenated length *before* the rename, so
/// a mismatch destroys the temp file and leaves `output` as it was.
fn concat_parts(parts: &[PathBuf], output: &Path, expected: u64) -> anyhow::Result<u64> {
    let mut tmp = super::fetch::temp_in_parent(output)?;
    let mut written = 0u64;
    for part in parts {
        let mut src = std::io::BufReader::new(
            std::fs::File::open(part).with_context(|| format!("open {}", part.display()))?,
        );
        // Context on the copy too, not just the open: a bad sector surfaces here
        // rather than at open, and "which of N parts" is the whole diagnosis —
        // deleting that one part is enough to recover.
        written = written.saturating_add(
            std::io::copy(&mut src, tmp.as_file_mut())
                .with_context(|| format!("read {}", part.display()))?,
        );
    }
    if written != expected {
        bail!("reconstructed {written} bytes but the manifest declares {expected}");
    }
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
    /// the size check rejects it before the (whole-chunk) hash pass runs.
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

    /// A part that is the *right size* but holds the wrong bytes (bit-rot, or a
    /// stale part left by a size-colliding chunk) is re-fetched, not reused. This
    /// exercises the hash branch of `part_is_verified` — distinct from
    /// `a_truncated_part_is_refetched`, which is rejected earlier on length: if
    /// the hash check regressed, corrupt bytes would be silently concatenated and
    /// the size-only `total_bytes` guard would not catch it (the sizes still sum).
    #[tokio::test]
    async fn a_same_size_corrupt_part_is_refetched() {
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

        // Corrupt chunk 1's part in place, keeping its length (3 bytes → 3 bytes)
        // so only the hash check can reject it.
        let part = downloads.join(hex(&hash)).join("chunk-1.part");
        assert_eq!(std::fs::metadata(&part).unwrap().len(), 3);
        std::fs::write(&part, b"XXX").unwrap();
        src.fetched.borrow_mut().clear();

        reconstruct(&manifest, hash, &downloads, &out, true, |h| src.get(h))
            .await
            .unwrap();
        // Only the corrupt chunk is re-fetched; the good chunk-0 part is reused.
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

    /// #1303: the cross-process lock is a persistent lockfile in the *downloads
    /// root* — a sibling of the part dir, never inside it — so `--no-keep-blobs`
    /// sweeping the part dir can't take the lock target with it. If it could, two
    /// `decdn` processes would lock different inodes and both proceed, re-opening
    /// the race the lock exists to close. Assert the lockfile is created and
    /// outlives the part-dir cleanup.
    #[tokio::test]
    async fn reconstruct_leaves_a_persistent_cross_process_lockfile() {
        let chunks: [&[u8]; 2] = [b"one", b"two"];
        let (manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        let dir = tempfile::tempdir().unwrap();
        let downloads = dir.path().join("downloads");
        let out = dir.path().join("out.bin");
        let src = Source::new(&chunks);

        // --no-keep-blobs: the part dir is swept, but the lockfile must remain.
        reconstruct(&manifest, hash, &downloads, &out, false, |h| src.get(h))
            .await
            .unwrap();

        assert!(
            downloads.join(format!("{}.lock", hex(&hash))).exists(),
            "lockfile must persist in the downloads root for cross-process serialization"
        );
        assert!(
            !downloads.join(hex(&hash)).exists(),
            "part dir is swept under --no-keep-blobs, but the sibling lockfile is not"
        );
    }

    /// `--no-keep-blobs` cleanup must *surface* a part dir it cannot empty, not
    /// silently leave it. `write_blob_atomic` stages its temp *inside* the part
    /// dir, so a run killed mid-write leaves an orphan the delete loop never
    /// enumerates; `remove_dir` then fails `ENOTEMPTY`. Since nothing GCs the
    /// downloads root, the dir must remain (the leak is visible) and the run must
    /// still succeed — cleanup is best-effort. A regression reverting the
    /// `remove_dir` warning back to `let _ =` would make this leak invisible.
    #[tokio::test]
    async fn no_keep_blobs_leaves_a_dir_holding_an_orphan_tmp() {
        let chunks: [&[u8]; 2] = [b"one", b"two"];
        let (manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        let dir = tempfile::tempdir().unwrap();
        let downloads = dir.path().join("downloads");
        let out = dir.path().join("out.bin");
        let src = Source::new(&chunks);

        // Pre-seed the part dir with a stray temp file, as an interrupted
        // `write_blob_atomic` would leave behind.
        let part_dir = downloads.join(hex(&hash));
        std::fs::create_dir_all(&part_dir).unwrap();
        let orphan = part_dir.join("chunk-0.part.tmp-orphan");
        std::fs::write(&orphan, b"half-written").unwrap();

        reconstruct(&manifest, hash, &downloads, &out, false, |h| src.get(h))
            .await
            .unwrap();

        // The file still reconstructed, and the chunk parts were deleted...
        assert_eq!(std::fs::read(&out).unwrap(), b"onetwo");
        assert!(!part_dir.join("chunk-0.part").exists());
        // ...but the orphan kept the dir non-empty, so `remove_dir` failed and
        // the dir survives rather than being silently gone.
        assert!(part_dir.exists(), "orphan-holding part dir should remain");
        assert!(orphan.exists());
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

    /// Every chunk is a separate *paid* pull, so the chunk count is a spending
    /// bound. Both sides of the boundary are pinned: a cap that rejected exactly
    /// `MAX_CHUNKS` would be an off-by-one nobody would notice.
    ///
    /// This is the slowest test in the file (~1s), because tripping the guard
    /// requires materializing a list that large — the guard runs after decode by
    /// design, since moving it earlier would mean hand-parsing postcard's
    /// sequence-length varint.
    #[test]
    fn a_chunk_list_over_the_cap_is_rejected() {
        let at_cap = |n: usize| {
            let (mut manifest, _) = manifest_for(&[b"x"]);
            manifest.chunks = vec![
                ChunkEntry {
                    hash: [0u8; 32],
                    size: 1,
                };
                n
            ];
            manifest.total_bytes = n as u64;
            postcard::to_allocvec(&manifest).unwrap()
        };

        let err = sniff(&at_cap(MAX_CHUNKS + 1))
            .expect("magic matches")
            .unwrap_err();
        assert!(format!("{err:#}").contains("over the"), "{err:#}");

        // Exactly at the cap is legal.
        let ok = sniff(&at_cap(MAX_CHUNKS)).expect("magic matches");
        assert!(ok.is_ok(), "{:#}", ok.unwrap_err());
    }

    /// A zero-size chunk costs a part file and a pull but contributes nothing,
    /// and the empty blob is trivially present, so such pulls always "succeed".
    #[test]
    fn a_zero_size_chunk_is_rejected() {
        let (mut manifest, _) = manifest_for(&[b"abc"]);
        manifest.chunks.push(ChunkEntry {
            hash: *blake3::hash(b"").as_bytes(),
            size: 0,
        });
        let blob = postcard::to_allocvec(&manifest).unwrap();
        let err = sniff(&blob).expect("magic matches").unwrap_err();
        assert!(format!("{err:#}").contains("zero bytes"), "{err:#}");
    }

    /// `from_bytes` would ignore trailing input; a blob claiming to be a
    /// manifest and only partly being one is an error, not something to accept.
    #[test]
    fn trailing_bytes_after_the_record_are_rejected() {
        let (_, mut blob) = manifest_for(&[b"abc"]);
        blob.extend_from_slice(b"junk");
        let err = sniff(&blob).expect("magic matches").unwrap_err();
        assert!(format!("{err:#}").contains("trailing"), "{err:#}");
    }

    /// `filename` is a basename hint today, but it is exactly the field someone
    /// will later wire to a default output name. Reject traversal at the door.
    #[test]
    fn a_path_bearing_filename_is_rejected() {
        for evil in ["../../etc/passwd", "sub/dir.bin", "..", ".", r"back\slash"] {
            let (mut manifest, _) = manifest_for(&[b"abc"]);
            manifest.filename = evil.to_string();
            let blob = postcard::to_allocvec(&manifest).unwrap();
            let err = sniff(&blob).expect("magic matches").unwrap_err();
            assert!(
                format!("{err:#}").contains("not a bare basename"),
                "{evil}: {err:#}"
            );
        }
    }

    /// The basename check must not reject an *absent* hint. Producers write an
    /// empty `filename` when they have none (the e2e mirror does), so tightening
    /// the guard to reject empty would break real manifests — pinned here rather
    /// than only in the anvil-gated e2e.
    #[test]
    fn an_empty_filename_is_accepted() {
        let (mut manifest, _) = manifest_for(&[b"abc"]);
        manifest.filename = String::new();
        let blob = postcard::to_allocvec(&manifest).unwrap();
        let decoded = sniff(&blob).expect("magic matches").unwrap();
        assert_eq!(decoded.filename, "");
    }

    /// A length mismatch must not publish bytes under the final name. `bundle
    /// pull`'s skip-existing treats a present file as verified-good, so a
    /// corrupt file left at `output` would be skipped on every later run.
    #[tokio::test]
    async fn a_length_mismatch_leaves_no_output_file() {
        let chunks: [&[u8]; 2] = [b"one", b"two"];
        let (mut manifest, blob) = manifest_for(&chunks);
        let hash = *blake3::hash(&blob).as_bytes();
        // Only reachable by constructing the struct directly: `decode` proves
        // `total_bytes == sum(chunk.size)`, so this models a part mutating
        // between verification and concatenation.
        manifest.total_bytes = 99;
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.bin");
        std::fs::write(&out, b"pre-existing").unwrap();
        let src = Source::new(&chunks);

        let err = reconstruct(
            &manifest,
            hash,
            &dir.path().join("downloads"),
            &out,
            true,
            |h| src.get(h),
        )
        .await
        .unwrap_err();

        assert!(
            format!("{err:#}").contains("but the manifest declares"),
            "{err:#}"
        );
        // The prior contents survive: the temp file was dropped, never renamed.
        assert_eq!(std::fs::read(&out).unwrap(), b"pre-existing");
    }

    /// Part files follow the resolved data dir so `--data-dir` governs where a
    /// multi-hundred-GB reconstruction lands.
    #[test]
    fn downloads_root_is_data_dir_relative() {
        assert_eq!(
            downloads_root(Path::new("/custom/data")),
            Path::new("/custom/data/downloads")
        );
    }
}
