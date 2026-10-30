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

/// Sibling-key suffix for the published pre-order bao outboard (`{H}.obao4`),
/// per [ADR 037 §Origin-tier pull-through](https://github.com/decdn/decdn/blob/main/adr/037-regional-proxy-warming.md).
/// Re-exported from [`decdn_bao_range`] — the shared layout contract every
/// origin reader (filesystem / S3) and the `decdn origin import` writer derive
/// the sibling key from, so an operator `aws s3 sync`-ing between backends keeps
/// the same object names.
pub(super) use decdn_bao_range::OBAO4_SUFFIX;

/// Origin backed by a local filesystem directory. Blobs live at
/// `{base}/{hex[0..2]}/{hex}`; the engine is responsible for BLAKE3
/// verification after the bytes come back (same contract as every other
/// [`Origin`] impl).
#[derive(Debug, Clone)]
pub struct FilesystemOrigin {
    /// Canonicalized at construction so the per-read containment check
    /// compares two resolved paths — see [`Self::new`] and
    /// [`Self::resolve_regular`].
    base: PathBuf,
}

impl FilesystemOrigin {
    /// Construct an origin rooted at `base`. Fails fast if `base` doesn't
    /// exist or isn't a directory — a typo in the config shouldn't surface
    /// as a per-request miss.
    ///
    /// `base` is canonicalized so the per-request containment check can
    /// compare a resolved blob path against a resolved root. Without this,
    /// an operator who configured the origin via a symlinked path would
    /// have every fetch look "outside" itself.
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
    /// ([ADR 037 §Origin-tier pull-through](https://github.com/decdn/decdn/blob/main/adr/037-regional-proxy-warming.md)).
    /// Sits next to the data object so a pre-seed `cp`/`sync` carries both.
    fn obao4_path_for(&self, hash: Hash) -> PathBuf {
        let hex = hash.to_hex();
        let shard = hex.get(..2).unwrap_or("");
        self.base
            .join(shard)
            .join(format!("{}{OBAO4_SUFFIX}", hex.as_str()))
    }

    /// Resolve `path` to a regular file inside `base`, before anything
    /// opens it. Every read goes through here.
    ///
    /// A symlink dropped into the shard tree by an operator mistake or
    /// compromised tooling could otherwise turn a content-addressed read
    /// into an arbitrary-file read of anything the process can see — and
    /// the engine's BLAKE3 check happens *after* the bytes already left the
    /// disk, so it is not a defense for what got read in the first place.
    /// The path is canonicalized and the result must sit under the
    /// (already-canonical) base; an escape is a permanent failure.
    ///
    /// The resolved path is then stat'ed and must be a regular file. A
    /// FIFO blocks `open` until a writer appears, and a device such as
    /// `/dev/zero` reports a zero length but never ends when read; this
    /// check keeps both out, unless the path is swapped between the stat
    /// and the caller's open. The stat never opens the file, so it cannot
    /// block.
    ///
    /// `Ok(None)` means the path (or its symlink target) does not exist.
    /// Each caller maps that to its own "absent" answer.
    async fn resolve_regular(
        &self,
        path: &Path,
    ) -> Result<Option<(PathBuf, std::fs::Metadata)>, OriginPullError> {
        let canonical = match tokio::fs::canonicalize(path).await {
            Ok(p) => p,
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
        let meta = match tokio::fs::metadata(&canonical).await {
            Ok(m) => m,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                let msg = format!("cache.origin.path stat failed for {}", canonical.display());
                return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
            }
        };
        if !meta.is_file() {
            return Err(OriginPullError::Permanent(anyhow::anyhow!(
                "cache.origin.path entry {} is not a regular file",
                canonical.display()
            )));
        }
        Ok(Some((canonical, meta)))
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
/// helper — [`FilesystemOrigin::resolve_regular`] detects it by path
/// comparison after `canonicalize` and emits `OriginPullError::Permanent`
/// directly.
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
            let Some((canonical, _)) = self.resolve_regular(&self.path_for(hash)).await? else {
                return Ok(OriginFetch::NotFound);
            };

            // Open once and stat via the file handle (fstat), so the
            // metadata we check and the bytes we read come from the
            // same inode. The path stat in `resolve_regular` leaves a
            // TOCTOU window where the path could be swapped for a much
            // larger file or a different symlink before the open — the
            // size cap would run on stale metadata. The fstat on the
            // held fd closes that window for the size cap and the
            // regular-file check; it does not re-check containment.
            //
            // The file can also disappear between the resolve above
            // and this open (eviction, gc, operator cleanup); treat
            // that the same as a missing leaf and surface `NotFound`
            // rather than a hard error, matching the resolve.
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

    fn fetch_range_data(
        &self,
        hash: Hash,
        req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            // A missing data object is `Unsupported`: the caller falls back
            // to a whole-blob pull, which then surfaces the real `NotFound`.
            let Some((canonical, _)) = self.resolve_regular(&self.path_for(hash)).await? else {
                return Ok(OriginRangeFetch::Unsupported);
            };

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
                return Ok(OriginRangeFetch::Ranged { data: Bytes::new() });
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
            })
        })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let Some((canonical, meta)) = self.resolve_regular(&self.obao4_path_for(hash)).await?
            else {
                return Ok(OutboardFetch::NotFound);
            };
            // Check the on-disk length before reading: a wildly oversized
            // `.obao4` is malformed/foreign, and reading it first would buffer
            // the whole thing into memory only to reject it (an OOM lever for
            // a hostile sibling). Degrade — never a failure; the engine's
            // outboard length check is the load-bearing reject.
            if meta.len() > outboard_max_bytes {
                return Ok(OutboardFetch::Unsupported);
            }
            let file = match tokio::fs::File::open(&canonical).await {
                Ok(f) => f,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OutboardFetch::NotFound);
                }
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path outboard open failed for {}",
                        canonical.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };
            // fstat the held fd: the path may have been swapped between the
            // resolve and the open. The fstat re-checks the file type and the
            // length of what was opened; it does not re-check containment.
            let meta = file.metadata().await.map_err(|err| {
                let msg = format!(
                    "cache.origin.path outboard stat failed for {}",
                    canonical.display()
                );
                classify_io_error(err).map_inner(|e| e.context(msg))
            })?;
            if !meta.is_file() {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "cache.origin.path entry {} is not a regular file",
                    canonical.display()
                )));
            }
            if meta.len() > outboard_max_bytes {
                return Ok(OutboardFetch::Unsupported);
            }
            // `take(outboard_max_bytes + 1)` bounds the read at one byte past
            // the cap, so a file that grew on the same inode after the stat
            // cannot grow the buffer without limit. `saturating_add(1)` keeps
            // `u64::MAX` from wrapping to `0`. The buffer starts at the stat'ed
            // length, so the `Bytes` the engine caches holds no spare capacity.
            let mut outboard = Vec::with_capacity(usize::try_from(meta.len()).unwrap_or(0));
            let mut reader = file.take(outboard_max_bytes.saturating_add(1));
            if let Err(err) = reader.read_to_end(&mut outboard).await {
                let msg = format!(
                    "cache.origin.path outboard read failed for {}",
                    canonical.display()
                );
                return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
            }
            // The one-byte overrun `take` lets through marks a file that grew
            // past the cap after the stat.
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
            // (filesystem origins never compress), so the resolve's stat is an
            // exact, body-free answer. A missing data object is an unknown
            // size: degrade to whole-blob, which then surfaces the real
            // `NotFound`.
            Ok(self
                .resolve_regular(&self.path_for(hash))
                .await?
                .map(|(_, meta)| meta.len()))
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
mod tests;
