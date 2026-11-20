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
use iroh_blobs::Hash;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use super::{Origin, OriginKind, OriginRangeFetch, OriginRangeRequest, OutboardFetch};
use crate::error::OriginPullError;

/// Sibling-key suffix for the published pre-order bao outboard (`{H}.obao4`),
/// per [ADR 037 §Origin-tier pull-through](https://github.com/decdn/decdn/blob/main/adr/037-regional-proxy-warming.md).
/// Re-exported from [`decdn_bao_range`] — the shared layout contract every
/// origin reader (filesystem / S3) and the `decdn origin import` writer derive
/// the sibling key from, so an operator `aws s3 sync`-ing between backends keeps
/// the same object names.
pub(super) use decdn_bao_range::OBAO4_SUFFIX;
use decdn_bao_range::{MAX_SIZE_PROOF_OBJECT_BYTES, SIZE_PROOF_SUFFIX};

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
    /// `base` is canonicalized so the per-read containment check can
    /// compare a resolved blob path against a resolved root. Without this,
    /// an operator who configured the origin via a symlinked path would
    /// have every read look "outside" itself.
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

    /// Build the sibling size proof path: `{base}/{hex[0..2]}/{hex}.sizeproof`.
    fn size_proof_path_for(&self, hash: Hash) -> PathBuf {
        let hex = hash.to_hex();
        let shard = hex.get(..2).unwrap_or("");
        self.base
            .join(shard)
            .join(format!("{}{SIZE_PROOF_SUFFIX}", hex.as_str()))
    }

    /// Resolve `path` to a regular file inside `base`, before anything
    /// opens it. `fetch_range_data`, `fetch_outboard` and `fetch_size_proof`
    /// all go through here.
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
    /// FIFO blocks `open` until a writer appears, and a device reports a
    /// zero length but may never end when read. This check keeps out a
    /// FIFO or device inside the base; a symlink out to `/dev` already
    /// fails containment. The stat never opens the file, so it cannot
    /// block.
    ///
    /// A path swapped between this stat and the caller's open is not
    /// covered here. `fetch_outboard` re-checks the type on the opened file,
    /// which catches a device but not a FIFO: the open blocks on a FIFO
    /// first. `fetch_range_data` and `fetch_size_proof` do not re-check.
    ///
    /// `Ok(None)` means nothing exists at the path: no leaf, or no shard
    /// directory. Each caller maps that to its own "absent" answer. A
    /// dangling symlink — a leaf or a shard directory whose target does not
    /// exist — is not an absence: it is an operator mistake, for example an
    /// origin tree bind-mounted into a container with absolute links, so it
    /// is a permanent failure. As an absence, `fetch_size_proof` would report
    /// it as an authoritative `Ok(None)` with no log at any level.
    async fn resolve_regular(
        &self,
        path: &Path,
    ) -> Result<Option<(PathBuf, std::fs::Metadata)>, OriginPullError> {
        let canonical = match tokio::fs::canonicalize(path).await {
            Ok(p) => p,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return match dangling_link(path).await {
                    Some(link) => Err(OriginPullError::Permanent(anyhow::anyhow!(
                        "cache.origin.path entry {} is a symlink whose target does not exist",
                        link.display()
                    ))),
                    None => self.absent_unless_base_gone().await,
                };
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

    /// The answer for a path that does not resolve and is no dangling link:
    /// an absence while the base still exists, else a permanent failure. The
    /// base is checked once at construction, so a base directory that is
    /// deleted later, or that sits below a mount point and goes with the
    /// unmount, would otherwise read as every hash being absent. A base that
    /// is itself the mount point still exists as an empty directory after an
    /// unmount and keeps the absence, as does a stat error other than
    /// `NotFound`.
    async fn absent_unless_base_gone<T>(&self) -> Result<Option<T>, OriginPullError> {
        match tokio::fs::metadata(&self.base).await {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "cache.origin.path base {} no longer exists",
                    self.base.display()
                )))
            }
            _ => Ok(None),
        }
    }
}

/// The dangling symlink that makes `path` fail to resolve, if one does: the
/// leaf `path` itself, or its shard directory. `None` when neither is a
/// symlink whose target is missing, so the path is genuinely absent.
///
/// The shard directory is the only other component that can be a link: the
/// base is canonical. A component counts only when `symlink_metadata` sees a
/// link and `metadata` cannot follow it. Thus a valid symlinked shard
/// directory with no leaf under it stays an absence. An error from either
/// call reads as "not dangling" — the caller then keeps the absent answer it
/// would have given before this probe ran — and logs at debug, so a probe
/// failure (an ESTALE or a permission flip mid-check) leaves a trace.
async fn dangling_link(path: &Path) -> Option<&Path> {
    for candidate in [Some(path), path.parent()].into_iter().flatten() {
        let is_link = match tokio::fs::symlink_metadata(candidate).await {
            Ok(m) => m.file_type().is_symlink(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(err) => {
                tracing::debug!(
                    path = %candidate.display(),
                    error = %err,
                    "dangling-link probe lstat failed; keeping the absent answer",
                );
                false
            }
        };
        if !is_link {
            continue;
        }
        match tokio::fs::metadata(candidate).await {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Some(candidate),
            Ok(_) => {}
            Err(err) => {
                tracing::debug!(
                    path = %candidate.display(),
                    error = %err,
                    "dangling-link probe stat failed; keeping the absent answer",
                );
            }
        }
    }
    None
}

/// Classify a filesystem `io::Error` as transient or permanent. Most FS
/// failures (`PermissionDenied`, "not a directory", read errors on a
/// closed file handle) are deterministic — retry won't change the
/// answer. The handful of error kinds the kernel uses for "the syscall
/// got interrupted, try again" are retriable, and so is `ESTALE`
/// (`StaleNetworkFileHandle`): on an NFS-backed base it usually clears on
/// the next path lookup, which `canonicalize` does on every read.
///
/// `NotFound` is intentionally not handled here: a `NotFound` path lookup
/// is `Ok(None)` from [`FilesystemOrigin::resolve_regular`], and each read
/// maps that, and a `NotFound` open, to its own absent answer. A symlink
/// escape, a dangling symlink and a non-regular file are not `io::Error`s
/// either: `resolve_regular`, and the re-check on the opened file in
/// `fetch_outboard`, emit `OriginPullError::Permanent` directly.
fn classify_io_error(err: std::io::Error) -> OriginPullError {
    use std::io::ErrorKind;
    match err.kind() {
        ErrorKind::Interrupted
        | ErrorKind::TimedOut
        | ErrorKind::ResourceBusy
        | ErrorKind::WouldBlock
        | ErrorKind::StaleNetworkFileHandle => OriginPullError::Transient(err.into()),
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

    fn fetch_range_data(
        &self,
        hash: Hash,
        req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let Some((canonical, _)) = self.resolve_regular(&self.path_for(hash)).await? else {
                return Ok(OriginRangeFetch::NotFound);
            };

            let mut file = match tokio::fs::File::open(&canonical).await {
                Ok(f) => f,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(OriginRangeFetch::NotFound);
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
            // the engine derived from the proven blob size) means the origin
            // copy is stale or truncated: a permanent fault.
            if let Err(err) = file.seek(std::io::SeekFrom::Start(req.fetch_start)).await {
                let msg = format!("cache.origin.path seek failed for {}", canonical.display());
                return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
            }
            let want = usize::try_from(req.len()).unwrap_or(usize::MAX);
            let mut data = vec![0u8; want];
            if let Err(err) = file.read_exact(&mut data).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(OriginPullError::Permanent(anyhow::anyhow!(
                        "cache.origin.path entry {} is shorter than the range [{}, {}); \
                         re-run `decdn origin import` for the object",
                        canonical.display(),
                        req.fetch_start,
                        req.fetch_end
                    )));
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
            // a hostile sibling). An outboard over the cap is a permanent
            // fault; the engine's exact-length check rejects a wrong-length
            // outboard that fits under the cap.
            if meta.len() > outboard_max_bytes {
                return Err(oversize_outboard(
                    &canonical,
                    meta.len(),
                    outboard_max_bytes,
                ));
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
            // length of what was opened. It does not re-check containment, and
            // it cannot catch a FIFO swapped in: the open blocks on it first.
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
                return Err(oversize_outboard(
                    &canonical,
                    meta.len(),
                    outboard_max_bytes,
                ));
            }
            match read_capped(file, meta.len(), outboard_max_bytes).await {
                Ok(Some(outboard)) => Ok(OutboardFetch::Found(Bytes::from(outboard))),
                Ok(None) => Err(oversize_outboard(
                    &canonical,
                    outboard_max_bytes.saturating_add(1),
                    outboard_max_bytes,
                )),
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path outboard read failed for {}",
                        canonical.display()
                    );
                    Err(classify_io_error(err).map_inner(|e| e.context(msg)))
                }
            }
        })
    }

    fn fetch_size_proof(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let Some((canonical, meta)) = self
                .resolve_regular(&self.size_proof_path_for(hash))
                .await?
            else {
                return Ok(None);
            };
            let max = MAX_SIZE_PROOF_OBJECT_BYTES as u64;
            if meta.len() > max {
                return Err(oversize_size_proof(&canonical, max));
            }
            let file = match tokio::fs::File::open(&canonical).await {
                Ok(f) => f,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path size proof open failed for {}",
                        canonical.display()
                    );
                    return Err(classify_io_error(err).map_inner(|e| e.context(msg)));
                }
            };
            match read_capped(file, meta.len(), max).await {
                Ok(Some(proof)) => Ok(Some(Bytes::from(proof))),
                Ok(None) => Err(oversize_size_proof(&canonical, max)),
                Err(err) => {
                    let msg = format!(
                        "cache.origin.path size proof read failed for {}",
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

/// Read `reader` into a buffer presized to `len_hint`, stopping one byte
/// past `max`. `Ok(None)` means the source held more than `max` bytes.
///
/// The presize is fallible: a reservation the allocator refuses is an
/// `OutOfMemory` error for this one read, as in `std::fs::read`, not a
/// process abort.
///
/// `take(max + 1)` bounds the read at one byte past the cap, so a source
/// longer than its hint (a file that grew after its stat) cannot grow
/// the buffer without limit; that extra byte marks the overrun.
/// `saturating_add(1)` keeps `u64::MAX` from wrapping to `0`.
///
/// The returned buffer holds no spare capacity, because the engine's
/// outboard cache budgets each entry by its length. The exact presize
/// gives that when the hint is right; a hint gone stale between the stat
/// and the read leaves a grown or unfilled buffer, which is shrunk.
async fn read_capped<R: AsyncRead + Unpin>(
    reader: R,
    len_hint: u64,
    max: u64,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    buf.try_reserve_exact(usize::try_from(len_hint).unwrap_or(usize::MAX))
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::OutOfMemory, err))?;
    reader
        .take(max.saturating_add(1))
        .read_to_end(&mut buf)
        .await?;
    if u64::try_from(buf.len()).unwrap_or(u64::MAX) > max {
        return Ok(None);
    }
    buf.shrink_to_fit();
    Ok(Some(buf))
}

/// The permanent fault for an outboard at `path` of `len` bytes, over its
/// `cap`. An outboard that large is malformed or foreign, for example a
/// chunk-level outboard from another tool.
fn oversize_outboard(path: &Path, len: u64, cap: u64) -> OriginPullError {
    OriginPullError::Permanent(anyhow::anyhow!(
        "cache.origin.path outboard {} is {len} bytes, over its {cap}-byte cap; re-run \
         `decdn origin import` for the object",
        path.display()
    ))
}

/// The permanent fault for a size proof object at `path` larger than `cap`.
fn oversize_size_proof(path: &Path, cap: u64) -> OriginPullError {
    OriginPullError::Permanent(anyhow::anyhow!(
        "cache.origin.path size proof {} is over its {cap}-byte cap; re-run \
         `decdn origin import` for the object",
        path.display()
    ))
}

#[cfg(test)]
mod tests;
