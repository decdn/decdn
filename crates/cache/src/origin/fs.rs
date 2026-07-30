//! Filesystem origin backend.
//!
//! Reads blobs from a sharded local directory at
//! `{base}/{hex[0..2]}/{hex}`. Useful for local dev, pre-seeded caches,
//! and operators who prefer to hand-curate the origin surface without
//! standing up an HTTP server.
//!
//! The sharded layout mirrors git's object store — it keeps directory
//! cardinality manageable as the content set grows, and a pre-seed script
//! is a trivial `cp` loop.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::Context;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use iroh_blobs::Hash;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use super::{Origin, OriginFetch, OriginKind, OriginRangeFetch, OriginRangeRequest, OutboardFetch};
use crate::error::OriginPullError;

/// Sibling-key suffix for the published pre-order bao outboard
/// (`{H}.obao4`), per [ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md).
/// Shared spelling across the filesystem / HTTP / S3 adapters so an operator
/// `aws s3 sync`-ing between backends keeps the same object names.
pub(super) const OBAO4_SUFFIX: &str = ".obao4";

/// Origin backed by a local filesystem directory. Blobs live at
/// `{base}/{hex[0..2]}/{hex}`; the engine is responsible for BLAKE3
/// verification after the bytes come back (same contract as every other
/// [`Origin`] impl).
#[derive(Debug, Clone)]
pub struct FilesystemOrigin {
    /// Canonicalized at construction so the per-fetch containment check
    /// compares two resolved paths — see [`Self::new`] and the
    /// canonicalize step in [`Origin::fetch`].
    base: PathBuf,
}

impl FilesystemOrigin {
    /// Construct an origin rooted at `base`. Fails fast if `base` doesn't
    /// exist or isn't a directory — a typo in the config shouldn't surface
    /// as a per-request miss.
    ///
    /// `base` is canonicalized so the per-request containment check in
    /// [`Origin::fetch`] can compare a resolved blob path against a
    /// resolved root. Without this, an operator who configured the origin
    /// via a symlinked path would have every fetch look "outside" itself.
    pub async fn new(base: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let base = base.into();
        let meta = tokio::fs::metadata(&base)
            .await
            .with_context(|| format!("cache.origin.path {} is not accessible", base.display()))?;
        if !meta.is_dir() {
            anyhow::bail!("cache.origin.path {} is not a directory", base.display());
        }
        let base = tokio::fs::canonicalize(&base).await.with_context(|| {
            format!(
                "cache.origin.path {} could not be canonicalized",
                base.display()
            )
        })?;
        Ok(Self { base })
    }

    /// Expose the configured base for diagnostics / logging.
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Build the per-hash path: `{base}/{hex[0..2]}/{hex}`. The first
    /// two hex chars shard the directory so a content set with millions
    /// of entries doesn't land in a single unix dirent list.
    fn path_for(&self, hash: Hash) -> PathBuf {
        let hex = hash.to_hex();
        // BLAKE3 hex is always 64 lowercase chars; `get(..2)` is defensive
        // against an unexpected iroh_blobs::Hash format change, and the
        // workspace's anti-indexing lint forbids `&hex[..2]` here.
        let shard = hex.get(..2).unwrap_or("");
        self.base.join(shard).join(hex.as_str())
    }

    /// Build the sibling outboard path: `{base}/{hex[0..2]}/{hex}.obao4`
    /// ([ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md)).
    /// Sits next to the data object so a pre-seed `cp`/`sync` carries both.
    fn obao4_path_for(&self, hash: Hash) -> PathBuf {
        let hex = hash.to_hex();
        let shard = hex.get(..2).unwrap_or("");
        self.base
            .join(shard)
            .join(format!("{}{OBAO4_SUFFIX}", hex.as_str()))
    }
}

/// Classify a filesystem `io::Error` as transient or permanent. Most FS
/// failures (`PermissionDenied`, "not a directory", read errors on a
/// closed file handle) are deterministic — retry won't change the
/// answer. The handful of error kinds the kernel uses for "the syscall
/// got interrupted, try again" are retriable.
///
/// `NotFound` is intentionally not handled here — the call sites
/// convert it to [`OriginFetch::NotFound`] before reaching this helper.
/// Symlink-escape is *also* not an `io::Error` and never reaches this
/// helper — it's detected by path comparison after `canonicalize` and
/// emits `OriginPullError::Permanent` directly at the call site.
fn classify_io_error(err: std::io::Error) -> OriginPullError {
    use std::io::ErrorKind;
    match err.kind() {
        ErrorKind::Interrupted
        | ErrorKind::TimedOut
        | ErrorKind::ResourceBusy
        | ErrorKind::WouldBlock => OriginPullError::Transient(err.into()),
        _ => OriginPullError::Permanent(err.into()),
    }
}

/// Parse a sharded leaf file name as a BLAKE3 [`struct@Hash`], panic-free.
///
/// `iroh_blobs::Hash`'s own `FromStr` panics (via `data-encoding`) on a
/// wrong-length input, so it must never see an untrusted directory entry
/// name (see the same warning in `decdn-common`'s admin hash parser). We
/// accept only the canonical 64-char lowercase-hex form that `to_hex()`
/// produces; anything else (stray files, `.obao4` siblings, uppercase) is
/// `None` and silently skipped by the enumeration.
fn hash_from_hex_name(name: &str) -> Option<Hash> {
    if name.len() != 64 {
        return None;
    }
    let bytes = name.as_bytes();
    let hex_val = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    };
    let mut out = [0u8; 32];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = hex_val(*bytes.get(i * 2)?)?;
        let lo = hex_val(*bytes.get(i * 2 + 1)?)?;
        *slot = (hi << 4) | lo;
    }
    Some(Hash::from_bytes(out))
}

impl Origin for FilesystemOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Filesystem
    }

    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let path = self.path_for(hash);

            // Resolve symlinks before touching the file. A symlink dropped
            // into the shard tree by an operator mistake or compromised
            // tooling could otherwise turn a content-addressed read into
            // an arbitrary-file read of anything the process can see —
            // and the engine's BLAKE3 check happens *after* the bytes
            // already left the disk, so it is not a defense for what got
            // read in the first place. We canonicalize and require the
            // result to sit under the (already-canonical) base.
            let canonical = match tokio::fs::canonicalize(&path).await {
                Ok(p) => p,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginFetch::NotFound);
                }
                Err(err) => {
                    let path_msg = format!(
                        "cache.origin.path canonicalize failed for {}",
                        path.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(path_msg)));
                }
            };
            if !canonical.starts_with(&self.base) {
                // Symlink escape: deterministic permanent failure.
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "cache.origin.path entry {} resolves to {} which is outside base {}",
                    path.display(),
                    canonical.display(),
                    self.base.display()
                )));
            }

            // Open once and stat via the file handle (fstat), so the
            // metadata we check and the bytes we read come from the
            // same inode. A pair of `tokio::fs::metadata` + `tokio::fs::read`
            // on the same path leaves a TOCTOU window where the path
            // could be swapped for a much larger file or a different
            // symlink between the two syscalls — the size cap would
            // run on stale metadata. Holding the fd avoids that, and
            // also saves a redundant path traversal.
            //
            // The file can also disappear between `canonicalize` above
            // and this open (eviction, gc, operator cleanup); treat
            // that the same as a missing leaf and surface `NotFound`
            // rather than a hard error, matching the canonicalize arm.
            let file = match tokio::fs::File::open(&canonical).await {
                Ok(f) => f,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginFetch::NotFound);
                }
                Err(err) => {
                    let path_msg =
                        format!("cache.origin.path open failed for {}", canonical.display());
                    return Err(classify_io_error(err).map_inner(|e| e.context(path_msg)));
                }
            };
            let meta = file.metadata().await.map_err(|err| {
                let path_msg = format!("cache.origin.path stat failed for {}", canonical.display());
                classify_io_error(err).map_inner(|e| e.context(path_msg))
            })?;

            if !meta.is_file() {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "cache.origin.path entry {} is not a regular file",
                    canonical.display()
                )));
            }

            let len = meta.len();
            if len > max_bytes {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "cache.origin.path entry {} is {len} bytes, exceeds max {max_bytes}",
                    canonical.display()
                )));
            }

            // Stream the file through `ReaderStream` rather than reading
            // the entire payload into a `Vec` (issue #271). The owned
            // `File` moves into `Take`, then into `ReaderStream`, so the
            // fd outlives the stream rather than being borrowed; iroh
            // -blobs' `add_stream` can drive this directly without any
            // intermediate buffer.
            //
            // `take(max_bytes + 1)` provides an I/O-layer cap: a
            // file that grew between `fstat` and the read (append,
            // pwrite past EOF, truncate-then-extend — all happen on
            // the same inode our fd is pinning) is bounded by the
            // kernel's read syscall. The wrapper in
            // [`cap_at_max_bytes`] catches the one-byte overrun and
            // converts it to a typed `io::Error` ("grew past max
            // during read") that the engine surfaces as
            // `CacheError::OriginError`.
            //
            // `saturating_add(1)` preserves correctness for
            // `max_bytes == u64::MAX`: saturating rather than
            // wrapping to `0`.
            let path_for_log = canonical.clone();
            let reader = file.take(max_bytes.saturating_add(1));
            let raw = ReaderStream::new(reader);
            let stream = cap_at_max_bytes(raw, max_bytes, path_for_log);
            Ok(OriginFetch::Found {
                stream: Box::pin(stream),
                size_hint: Some(len),
            })
        })
    }

    fn fetch_range(
        &self,
        hash: Hash,
        req: OriginRangeRequest,
        outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            // The sibling outboard is the gate: an origin that doesn't publish
            // `{H}.obao4` can't be range-pulled, so degrade to whole-blob
            // before issuing the (more expensive) ranged data read. A missing
            // outboard is the *expected* path for backends that pre-date the
            // optimization — `Unsupported`, not an error.
            let obao4_path = self.obao4_path_for(hash);
            // Check the on-disk length via `metadata()` BEFORE reading the
            // file: a wildly oversized outboard is a malformed/foreign
            // `{H}.obao4`, and reading it first would buffer the whole thing
            // into memory only to reject it (an OOM lever for a hostile
            // sibling). Degrade to whole-blob (`Unsupported`) — never a
            // failure. The engine's `encode_verified_range` length check is
            // the load-bearing reject; this just bounds the allocation.
            match tokio::fs::metadata(&obao4_path).await {
                Ok(meta) if meta.len() > outboard_max_bytes => {
                    return Ok(OriginRangeFetch::Unsupported);
                }
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginRangeFetch::Unsupported);
                }
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path outboard metadata failed for {}",
                        obao4_path.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            }
            let outboard = match tokio::fs::read(&obao4_path).await {
                Ok(b) => b,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginRangeFetch::Unsupported);
                }
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path outboard read failed for {}",
                        obao4_path.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };
            // Defense-in-depth: a file that grew between `metadata` and `read`
            // (TOCTOU) is still rejected on the buffered length. Degrade rather
            // than feed an oversized outboard to verification.
            if u64::try_from(outboard.len()).unwrap_or(u64::MAX) > outboard_max_bytes {
                return Ok(OriginRangeFetch::Unsupported);
            }

            // Resolve + contain the data path exactly as `fetch` does: a
            // symlink-escape is a permanent failure, a missing data object is
            // `Unsupported` (caller will whole-blob pull, which then surfaces
            // the real `NotFound`).
            let path = self.path_for(hash);
            let canonical = match tokio::fs::canonicalize(&path).await {
                Ok(p) => p,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginRangeFetch::Unsupported);
                }
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path canonicalize failed for {}",
                        path.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };
            if !canonical.starts_with(&self.base) {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "cache.origin.path entry {} resolves to {} which is outside base {}",
                    path.display(),
                    canonical.display(),
                    self.base.display()
                )));
            }

            let mut file = match tokio::fs::File::open(&canonical).await {
                Ok(f) => f,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginRangeFetch::Unsupported);
                }
                Err(err) => {
                    let msg = format!("cache.origin.path open failed for {}", canonical.display());
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };

            // Empty span only arises for a zero-length blob; nothing to read.
            if req.is_empty() {
                return Ok(OriginRangeFetch::Ranged {
                    data: Bytes::new(),
                    outboard: Bytes::from(outboard),
                });
            }

            // Seek to the aligned start and read exactly `req.len()` bytes. A
            // short read (the on-disk object is smaller than the aligned span
            // the engine derived from the signed blob size) means the origin
            // copy is stale/truncated — degrade to whole-blob rather than feed
            // a partial span to verification (which would reject it anyway).
            if let Err(err) = file.seek(std::io::SeekFrom::Start(req.fetch_start)).await {
                let msg = format!("cache.origin.path seek failed for {}", canonical.display());
                return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
            }
            let want = usize::try_from(req.len()).unwrap_or(usize::MAX);
            let mut data = vec![0u8; want];
            if let Err(err) = file.read_exact(&mut data).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(OriginRangeFetch::Unsupported);
                }
                let msg = format!(
                    "cache.origin.path range read failed for {}",
                    canonical.display()
                );
                return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
            }

            Ok(OriginRangeFetch::Ranged {
                data: Bytes::from(data),
                outboard: Bytes::from(outboard),
            })
        })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let obao4_path = self.obao4_path_for(hash);
            // Check the on-disk length via `metadata()` before reading, same
            // rationale as `fetch_range`'s outboard sub-fetch: a wildly
            // oversized `.obao4` is malformed/foreign, and reading it first
            // would buffer the whole thing into memory only to reject it.
            match tokio::fs::metadata(&obao4_path).await {
                Ok(meta) if meta.len() > outboard_max_bytes => {
                    return Ok(OutboardFetch::Unsupported);
                }
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OutboardFetch::NotFound);
                }
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path outboard metadata failed for {}",
                        obao4_path.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            }
            let outboard = match tokio::fs::read(&obao4_path).await {
                Ok(b) => b,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OutboardFetch::NotFound);
                }
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path outboard read failed for {}",
                        obao4_path.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };
            // Defense-in-depth: a file that grew between `metadata` and `read`
            // (TOCTOU) is still rejected on the buffered length.
            if u64::try_from(outboard.len()).unwrap_or(u64::MAX) > outboard_max_bytes {
                return Ok(OutboardFetch::Unsupported);
            }
            Ok(OutboardFetch::Found(Bytes::from(outboard)))
        })
    }

    fn size(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            // The local data object's on-disk length IS the canonical blob size
            // (filesystem origins never compress), so a `metadata()` stat is an
            // exact, body-free answer. Resolve + contain the path exactly as
            // `fetch` / `fetch_range` do so a symlink escape is a permanent
            // failure, not a silent read outside `base`.
            let path = self.path_for(hash);
            let canonical = match tokio::fs::canonicalize(&path).await {
                Ok(p) => p,
                // Missing data object → unknown size, degrade to whole-blob
                // (which then surfaces the real `NotFound`).
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path canonicalize failed for {}",
                        path.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };
            if !canonical.starts_with(&self.base) {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "cache.origin.path entry {} resolves to {} which is outside base {}",
                    path.display(),
                    canonical.display(),
                    self.base.display()
                )));
            }
            match tokio::fs::metadata(&canonical).await {
                Ok(meta) => Ok(Some(meta.len())),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path metadata failed for {}",
                        canonical.display()
                    );
                    Err(classify_io_error(err).map_inner(|e| e.context(msg)))
                }
            }
        })
    }

    fn enumerate(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Hash>, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            // Presence-only walk of `{base}/{hex[0..2]}/{hex}` — the file
            // name IS the hash, so this never reads or hashes payloads
            // (trust model, #1130). Skip sibling `{H}.obao4` outboards,
            // non-directory shard entries, and any leaf whose name isn't a
            // valid hash sharded under its own first two hex chars (a stray
            // file dropped into the tree is silently ignored, not an error).
            let mut out = Vec::new();
            let mut shards = match tokio::fs::read_dir(&self.base).await {
                Ok(rd) => rd,
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path read_dir failed for {}",
                        self.base.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };
            while let Some(shard) = shards.next_entry().await.map_err(|err| {
                classify_io_error(err)
                    .map_inner(|e| e.context("cache.origin.path shard read failed".to_string()))
            })? {
                let shard_name = shard.file_name();
                let shard_str = match shard_name.to_str() {
                    // Shard dirs are exactly the two-char hex prefix.
                    Some(s) if s.len() == 2 => s,
                    _ => continue,
                };
                match shard.file_type().await {
                    Ok(ft) if ft.is_dir() => {}
                    _ => continue,
                }
                let shard_path = shard.path();
                // A shard that vanished mid-walk (gc/operator cleanup) is not
                // fatal to enumerating the rest of the tree.
                let Ok(mut entries) = tokio::fs::read_dir(&shard_path).await else {
                    continue;
                };
                while let Some(entry) = entries.next_entry().await.map_err(|err| {
                    classify_io_error(err)
                        .map_inner(|e| e.context("cache.origin.path entry read failed".to_string()))
                })? {
                    let name = entry.file_name();
                    let name = match name.to_str() {
                        Some(n) if !n.ends_with(OBAO4_SUFFIX) => n,
                        _ => continue,
                    };
                    let Some(hash) = hash_from_hex_name(name) else {
                        continue;
                    };
                    // The leaf must live under its own shard, else it's a
                    // mis-placed file we won't be able to serve by path.
                    if hash.to_hex().get(..2) == Some(shard_str) {
                        out.push(hash);
                    }
                }
            }
            Ok(out)
        })
    }
}

/// Defense-in-depth running-cap wrapper around a chunk stream. The
/// inner `take(max_bytes + 1)` bounds the I/O-layer read at one byte
/// past the cap, so a file that grew during read produces a stream
/// whose chunks sum to at most `max_bytes + 1`. This wrapper trips on
/// the cumulative byte count and yields the overrun as an
/// `io::Error`. The engine's outer wrapper (`count_and_cap_stream`)
/// captures the error into its side channel and yields `None` to
/// `iroh_blobs::Blobs::add_stream`, so this error never reaches
/// iroh-blobs directly — it surfaces back to the caller as
/// `CacheError::OriginError` once `temp_tag().await` completes and
/// the engine inspects the side channel.
///
/// `path_for_log` is captured for the error message so the operator
/// log surfaces the offending file path. The path is the canonicalized
/// form (already resolved, so no symlink trickery in logs).
fn cap_at_max_bytes<S>(
    stream: S,
    max_bytes: u64,
    path_for_log: PathBuf,
) -> impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + Unpin + 'static,
{
    // **Termination after error:** the inner stream is wrapped in
    // `Option` so that after yielding an `Err`, the next poll returns
    // `None`. Polling a stream after a terminal error is undefined
    // (some impls error again, some hang); the sentinel makes the
    // wrapper deterministic and prevents iroh-blobs' `add_stream` from
    // hanging when the upstream errors.
    futures_util::stream::unfold(
        (Some(stream), 0u64, path_for_log),
        move |(maybe_s, total, path)| async move {
            let mut s = maybe_s?;
            let next = s.next().await?;
            match next {
                Err(e) => Some((Err(e), (None, total, path))),
                Ok(chunk) => {
                    let new_total = total.saturating_add(chunk.len() as u64);
                    if new_total > max_bytes {
                        // Pack a typed `BlobTooLargeMarker` so the engine
                        // surfaces `CacheError::BlobTooLarge` rather than
                        // routing through `classify_io_error`, which
                        // defaults `ErrorKind::Other` to Transient and
                        // would burn the retry budget on a cap breach.
                        // Matches `http.rs::response_chunk_stream`'s
                        // typed-marker symmetry. The file path is
                        // intentionally dropped from the error — the
                        // typed marker is the operator-visible signal;
                        // adding path context would require a wider
                        // typed variant that `classify_io_error`
                        // doesn't recognise.
                        let err = std::io::Error::other(super::BlobTooLargeMarker { max_bytes });
                        Some((Err(err), (None, total, path)))
                    } else {
                        Some((Ok(chunk), (Some(s), new_total, path)))
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_rejects_missing_path() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let missing = tmp.path().join("does-not-exist");
        let err = FilesystemOrigin::new(&missing)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("missing path should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("not accessible"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_rejects_non_directory() -> anyhow::Result<()> {
        let tmp = tempfile::NamedTempFile::new()?;
        let err = FilesystemOrigin::new(tmp.path())
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("file path should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("not a directory"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn path_for_uses_two_char_shard() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let hash = Hash::new(b"marker");
        let hex = hash.to_hex();
        let path = origin.path_for(hash);
        let expected_shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        // Compare against the canonicalized tmp dir — on macOS the
        // tempdir lives under /var, which is itself a symlink to
        // /private/var, so origin.base differs from tmp.path().
        let canonical_tmp = tokio::fs::canonicalize(tmp.path()).await?;
        let expected = canonical_tmp.join(expected_shard).join(hex.as_str());
        anyhow::ensure!(path == expected, "got: {}", path.display());
        Ok(())
    }

    /// A symlink in the shard directory pointing outside the base must
    /// be rejected — that's the whole point of the per-fetch
    /// containment check (see issue #374).
    #[cfg(unix)]
    #[tokio::test]
    async fn fetch_rejects_symlink_pointing_outside_base() -> anyhow::Result<()> {
        let outside = tempfile::tempdir()?;
        let secret = outside.path().join("secret");
        tokio::fs::write(&secret, b"top secret").await?;

        let inside = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(inside.path()).await?;
        let hash = Hash::new(b"marker");
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = inside.path().join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        let link = shard_dir.join(hex.as_str());
        tokio::fs::symlink(&secret, &link).await?;

        let err = origin
            .fetch(hash, 1024)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("symlink outside base should have been rejected"))?
            .to_string();
        anyhow::ensure!(err.contains("outside base"), "error lacked context: {err}");
        Ok(())
    }

    /// A symlink that still resolves to a regular file inside the base
    /// is fine — we're guarding against escape, not symlinks per se.
    #[cfg(unix)]
    #[tokio::test]
    async fn fetch_follows_symlink_inside_base() -> anyhow::Result<()> {
        let inside = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(inside.path()).await?;

        // Drop the real file in a sibling directory under base, then
        // place a symlink at the expected sharded location that points
        // at it. canonicalize() resolves to the real file, which is
        // still under base, so the fetch should succeed.
        let real_dir = inside.path().join("real");
        tokio::fs::create_dir_all(&real_dir).await?;
        let real_file = real_dir.join("blob");
        tokio::fs::write(&real_file, b"hello").await?;

        let hash = Hash::new(b"marker");
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = inside.path().join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        let link = shard_dir.join(hex.as_str());
        tokio::fs::symlink(&real_file, &link).await?;

        let fetched = origin.fetch(hash, 1024).await?;
        let bytes = fetched
            .collect_to_bytes()
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected Found, got NotFound"))?;
        anyhow::ensure!(bytes.as_ref() == b"hello", "got: {bytes:?}");
        Ok(())
    }

    /// A missing file should still surface as `NotFound`, even though
    /// `canonicalize` is the first syscall and errors when the leaf
    /// doesn't exist.
    #[tokio::test]
    async fn fetch_missing_file_is_not_found() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let hash = Hash::new(b"marker");
        match origin.fetch(hash, 1024).await? {
            OriginFetch::NotFound => Ok(()),
            OriginFetch::Found { .. } => anyhow::bail!("expected NotFound"),
        }
    }

    /// `fstat`-time cap rejection: a file whose `metadata().len()`
    /// already exceeds `max_bytes` is rejected upfront in the prologue,
    /// before any stream is constructed. This is the cheap path —
    /// catches the legitimate "operator pre-seeded an oversized
    /// blob" case without paying for `take(max_bytes + 1)` /
    /// `ReaderStream` setup.
    #[tokio::test]
    async fn fetch_rejects_oversize_file_at_fstat_prologue() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let hash = Hash::new(b"oversize-marker");
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = tmp.path().join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        let payload = vec![0xAAu8; 8 * 1024];
        tokio::fs::write(shard_dir.join(hex.as_str()), &payload).await?;

        // 8 KiB on disk, 1 KiB cap → prologue rejects (no stream
        // is built; this is the expected fast path).
        let err = origin
            .fetch(hash, 1024)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("oversize fstat must be rejected"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("exceeds max"),
            "fstat-time cap message lost actionable wording: {msg}"
        );
        Ok(())
    }

    /// Mid-stream cap (`cap_at_max_bytes`) is the defense for the
    /// TOCTOU window: between `fstat` and the actual read, the file
    /// could grow on the same inode (append, pwrite past EOF,
    /// truncate-then-extend). The kernel bound is
    /// `take(max_bytes + 1)`; this test exercises the wrapper that
    /// catches the one-byte overrun.
    ///
    /// We can't deterministically stage a TOCTOU race in a unit
    /// test without OS-level coordination, so we drive
    /// `cap_at_max_bytes` directly with a synthetic upstream that
    /// produces `max_bytes + 1` bytes — same chunk-shape that
    /// `ReaderStream::new(file.take(max_bytes + 1))` would emit on
    /// a grew-during-read file.
    #[tokio::test]
    async fn cap_at_max_bytes_rejects_one_byte_overrun() -> anyhow::Result<()> {
        use futures_util::StreamExt;
        let chunks: Vec<std::io::Result<Bytes>> = vec![
            Ok(Bytes::from(vec![0xAAu8; 1024])),
            // 1025th byte — should trip the running-total cap.
            Ok(Bytes::from(vec![0xBBu8; 1])),
        ];
        let upstream = futures_util::stream::iter(chunks);
        let mut stream = Box::pin(cap_at_max_bytes(
            upstream,
            1024,
            std::path::PathBuf::from("/tmp/test-fixture"),
        ));

        // First chunk: 1024 bytes, exactly at cap, must pass through.
        let first = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("expected first chunk"))?;
        let first = first.map_err(|e| anyhow::anyhow!("first chunk errored: {e}"))?;
        anyhow::ensure!(
            first.len() == 1024,
            "first chunk truncated: {}",
            first.len()
        );

        // Second chunk: 1 byte, would push total to 1025 > 1024,
        // wrapper must error.
        let second = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("expected second chunk"))?;
        let err = second
            .err()
            .ok_or_else(|| anyhow::anyhow!("second chunk should have errored"))?;
        // Cap breach is signalled via a typed `BlobTooLargeMarker`
        // packed into the `io::Error` inner so the engine can
        // surface `CacheError::BlobTooLarge` (not a generic
        // `OriginError`). Operator-visible Display still mentions
        // the cap; assert on the typed shape because that's what
        // the engine's downcast walks.
        let has_marker = err.get_ref().is_some_and(
            <dyn std::error::Error + Send + Sync>::is::<crate::origin::BlobTooLargeMarker>,
        );
        anyhow::ensure!(
            has_marker,
            "wrapper error lost typed BlobTooLargeMarker inner: {err}"
        );
        anyhow::ensure!(
            err.to_string().contains("max_blob_bytes=1024"),
            "wrapper Display lost cap-value wording: {err}"
        );
        Ok(())
    }

    /// The streaming path keeps the file descriptor alive across many
    /// `poll_next` calls — `ReaderStream` owns the `Take<File>` and
    /// drives one read per poll. A regression that borrowed `&mut
    /// file` instead of moving ownership would close the fd between
    /// chunks and either truncate the read or blow up; this test
    /// catches that by asking for a payload large enough to require
    /// multiple reads (default `ReaderStream` chunk = 4 KiB) and
    /// asserting the full bytes come back.
    #[tokio::test]
    async fn fetch_streams_multi_chunk_payload() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        // 32 KiB → 8 chunks at the default 4 KiB ReaderStream size.
        let payload = (0..32u8)
            .flat_map(|i| std::iter::repeat_n(i, 1024))
            .collect::<Vec<_>>();
        let hash = Hash::new(&payload);
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = tmp.path().join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        tokio::fs::write(shard_dir.join(hex.as_str()), &payload).await?;

        let fetched = origin.fetch(hash, 1 << 20).await?;
        let bytes = fetched
            .collect_to_bytes()
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected Found"))?;
        anyhow::ensure!(
            bytes.len() == payload.len(),
            "streamed length mismatch: {} vs {}",
            bytes.len(),
            payload.len()
        );
        anyhow::ensure!(bytes.as_ref() == payload.as_slice(), "byte mismatch");
        Ok(())
    }

    /// Seed a sharded data object plus its sibling `{hex}.obao4` outboard
    /// under `base`, returning the content hash. Mirrors `path_for` /
    /// `obao4_path_for`.
    async fn seed_blob_with_outboard(base: &Path, payload: &[u8]) -> anyhow::Result<Hash> {
        use bao_tree::io::outboard::PreOrderMemOutboard;
        let ob = PreOrderMemOutboard::create(payload, crate::range_pull::IROH_BLOCK_SIZE);
        let hash = Hash::from_bytes(*ob.root.as_bytes());
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = base.join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        tokio::fs::write(shard_dir.join(hex.as_str()), payload).await?;
        tokio::fs::write(
            shard_dir.join(format!("{}{OBAO4_SUFFIX}", hex.as_str())),
            ob.data,
        )
        .await?;
        Ok(hash)
    }

    /// `enumerate` returns exactly the data-object hashes present under the
    /// sharded tree, excluding `.obao4` siblings and stray non-hash files,
    /// and without reading payloads (#1130 discovery, trust model).
    #[tokio::test]
    async fn enumerate_lists_data_hashes_only() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let canonical = tokio::fs::canonicalize(tmp.path()).await?;

        // Two real blobs, one WITH a sibling outboard (so the `.obao4`
        // exclusion is exercised) and one without.
        let h1 = seed_blob_with_outboard(&canonical, &vec![1u8; 40 * 1024]).await?;
        let payload2 = vec![2u8; 8 * 1024];
        let h2 = Hash::new(&payload2);
        let hex2 = h2.to_hex();
        let shard2 = hex2
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard2_dir = canonical.join(shard2);
        tokio::fs::create_dir_all(&shard2_dir).await?;
        tokio::fs::write(shard2_dir.join(hex2.as_str()), &payload2).await?;

        // A stray non-hash file inside a valid shard dir — must be ignored.
        tokio::fs::write(shard2_dir.join("not-a-hash.txt"), b"junk").await?;

        let mut got = origin.enumerate().await?;
        got.sort_by_key(|h| *h.as_bytes());
        let mut want = vec![h1, h2];
        want.sort_by_key(|h| *h.as_bytes());
        anyhow::ensure!(
            got == want,
            "enumerate mismatch: got {got:?}, want {want:?}"
        );
        Ok(())
    }

    /// An empty origin enumerates to nothing (not an error).
    #[tokio::test]
    async fn enumerate_empty_origin_is_empty() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        anyhow::ensure!(origin.enumerate().await?.is_empty(), "expected empty");
        Ok(())
    }

    /// A blob with a published `{hex}.obao4` range-fetches: the aligned span
    /// comes back exactly, plus the full outboard.
    #[tokio::test]
    async fn fetch_range_returns_span_and_outboard() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let canonical = tokio::fs::canonicalize(tmp.path()).await?;
        let payload = (0..200u32)
            .flat_map(|i| std::iter::repeat_n((i & 0xff) as u8, 1024))
            .collect::<Vec<_>>();
        let hash = seed_blob_with_outboard(&canonical, &payload).await?;

        let req = OriginRangeRequest {
            fetch_start: 16 * 1024,
            fetch_end: 48 * 1024,
        };
        match origin.fetch_range(hash, req, 1 << 20).await? {
            OriginRangeFetch::Ranged { data, outboard } => {
                anyhow::ensure!(
                    data.as_ref() == payload.get(16 * 1024..48 * 1024).unwrap_or_default(),
                    "span mismatch",
                );
                anyhow::ensure!(!outboard.is_empty(), "outboard must be served");
            }
            other => anyhow::bail!("expected Ranged, got {other:?}"),
        }
        Ok(())
    }

    /// No sibling outboard → degrade to `Unsupported` (the expected path for a
    /// pre-existing filesystem origin), never an error.
    #[tokio::test]
    async fn fetch_range_without_outboard_is_unsupported() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let canonical = tokio::fs::canonicalize(tmp.path()).await?;
        // Seed only the data object — no `.obao4`.
        let payload = vec![7u8; 32 * 1024];
        let hash = Hash::new(&payload);
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = canonical.join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        tokio::fs::write(shard_dir.join(hex.as_str()), &payload).await?;

        let req = OriginRangeRequest {
            fetch_start: 0,
            fetch_end: 16 * 1024,
        };
        anyhow::ensure!(
            matches!(
                origin.fetch_range(hash, req, 1 << 20).await?,
                OriginRangeFetch::Unsupported
            ),
            "missing outboard must degrade",
        );
        Ok(())
    }

    /// A blob with a published sibling `{hex}.obao4` returns the outboard
    /// bytes verbatim via `fetch_outboard` alone (no data read). This is the
    /// failing-first TDD test for #1130's stream-while-store seam — written
    /// before `fetch_outboard` existed on the trait.
    #[tokio::test]
    async fn fetch_outboard_returns_sibling_obao4() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let canonical = tokio::fs::canonicalize(tmp.path()).await?;
        let payload = vec![9u8; 64 * 1024];
        let hash = seed_blob_with_outboard(&canonical, &payload).await?;

        let obao4_path = origin.obao4_path_for(hash);
        let expected = tokio::fs::read(&obao4_path).await?;

        match origin.fetch_outboard(hash, 1 << 20).await? {
            OutboardFetch::Found(bytes) => {
                anyhow::ensure!(
                    bytes.as_ref() == expected.as_slice(),
                    "outboard bytes mismatch"
                );
            }
            other => anyhow::bail!("expected Found, got {other:?}"),
        }
        Ok(())
    }

    /// A hash with no sibling `.obao4` returns `NotFound`, not an error.
    #[tokio::test]
    async fn fetch_outboard_missing_sibling_is_not_found() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let hash = Hash::new(b"no-outboard-marker");
        anyhow::ensure!(
            matches!(
                origin.fetch_outboard(hash, 1 << 20).await?,
                OutboardFetch::NotFound
            ),
            "missing sibling must be NotFound",
        );
        Ok(())
    }

    /// An oversize sibling outboard (beyond `outboard_max_bytes`) degrades
    /// rather than buffering — a hostile/foreign `{H}.obao4` can't force a huge
    /// read.
    #[tokio::test]
    async fn fetch_range_oversize_outboard_is_unsupported() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let canonical = tokio::fs::canonicalize(tmp.path()).await?;
        let payload = vec![3u8; 64 * 1024];
        let hash = seed_blob_with_outboard(&canonical, &payload).await?;
        // Cap the outboard read at 1 byte — the real outboard is larger.
        let req = OriginRangeRequest {
            fetch_start: 0,
            fetch_end: 16 * 1024,
        };
        anyhow::ensure!(
            matches!(
                origin.fetch_range(hash, req, 1).await?,
                OriginRangeFetch::Unsupported
            ),
            "oversize outboard must degrade",
        );
        Ok(())
    }

    /// OOM guard: a multi-megabyte `{H}.obao4` against a tiny cap must degrade
    /// via the `metadata()` length pre-check, BEFORE `tokio::fs::read` buffers
    /// the whole file into memory. Pre-fix the file was read in full and only
    /// then compared to the cap.
    #[tokio::test]
    async fn fetch_range_oversize_outboard_rejected_before_read() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let origin = FilesystemOrigin::new(tmp.path()).await?;
        let canonical = tokio::fs::canonicalize(tmp.path()).await?;
        // Seed a real data object so only the outboard size is the gate.
        let payload = vec![3u8; 64 * 1024];
        let hash = Hash::new(&payload);
        let hex = hash.to_hex();
        let shard = hex
            .get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
        let shard_dir = canonical.join(shard);
        tokio::fs::create_dir_all(&shard_dir).await?;
        tokio::fs::write(shard_dir.join(hex.as_str()), &payload).await?;
        // 8 MiB foreign/hostile outboard.
        tokio::fs::write(
            shard_dir.join(format!("{}{OBAO4_SUFFIX}", hex.as_str())),
            vec![0x5Au8; 8 * 1024 * 1024],
        )
        .await?;

        let req = OriginRangeRequest {
            fetch_start: 0,
            fetch_end: 16 * 1024,
        };
        // 4 KiB cap — the 8 MiB outboard is rejected on its metadata length.
        anyhow::ensure!(
            matches!(
                origin.fetch_range(hash, req, 4 * 1024).await?,
                OriginRangeFetch::Unsupported
            ),
            "multi-MiB outboard must degrade without buffering",
        );
        Ok(())
    }
}
