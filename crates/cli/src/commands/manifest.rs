//! Shared bundle-manifest machinery: the directory walk, path-safety rules,
//! canonical JSON serialization, and atomic manifest write used by
//! `decdn origin import` (issues #391, #1904).
//!
//! `origin import` walks a directory and emits canonical manifest bytes; with
//! `--dry-run` it prints the exact same bytes to stdout instead of importing,
//! so operators can inspect or capture the manifest the import would produce.
//! Keeping the walk and the serializer in one place is what makes that
//! byte-identity hold by construction rather than by two copies staying in
//! sync.
//!
//! The on-disk schema, hash format (`b3:<hex>`), path-safety rules, and the
//! determinism contract that makes single-hash bundle distribution viable are
//! specified in [`appendix-bundles`](../../../../adr/appendix-bundles.md).

use std::fs::File;
use std::io::Write;
use std::path::{Component, Path};

use anyhow::{Context as _, anyhow, bail};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::Serialize;
use walkdir::WalkDir;

// Do not reorder: JSON key order is part of the bundle hash. Determinism
// requires the produced bytes to be stable across runs, so the only way to add
// a field in v1 is to add it at the end (and prefer a v2 bump instead — see
// `appendix-bundles.md` § Determinism).
#[derive(Serialize)]
struct Bundle<'a> {
    version: u32,
    entries: &'a [BundleEntry],
}

/// One manifest entry: a file's relative POSIX path, its `b3:<hex>` content
/// address, and its byte length.
///
/// `hash`/`size` always describe the **whole file**, stored and served as one
/// blob. When `chunks` is present it lists advisory `{hash, size}`
/// range-dedup hints over that same file's byte layout, in content order; a
/// hint's hash is never independently stored or fetched, only matched
/// against bytes a client already holds. `hash` is the authoritative
/// end-to-end validator regardless of whether hints are present. A plain
/// (unhinted) entry omits `chunks` entirely and serializes byte-identically
/// to a bundle with no chunk hints.
#[derive(Serialize)]
pub(crate) struct BundleEntry {
    /// Relative POSIX path within the bundle root (`a/b.txt`, never absolute).
    pub(crate) path: String,
    /// The whole file's BLAKE3 content address, `b3:<64 lowercase hex>`.
    pub(crate) hash: String,
    /// The whole file's length in bytes.
    pub(crate) size: u64,
    /// Optional ordered range-dedup hints over the file's bytes. The file is
    /// always stored and fetched as the one whole-file blob named by `hash`;
    /// a hint only lets a client recognize a byte range it already holds (a
    /// sibling entry with a matching chunk hash) and skip re-downloading it.
    /// Skipped from the serialized form when absent so plain entries keep
    /// their pre-hints byte layout.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chunks: Option<Vec<Chunk>>,
}

/// One range-dedup hint over a hinted [`BundleEntry`]'s bytes: a BLAKE3 over
/// that byte range and its length. Hints are listed in content order — the
/// range they cover, in sequence, spans the whole file — and a hint `hash`
/// shared across entries tells a client the two files share that byte range,
/// so it fetches and pays for it once and splices the other copy in locally.
/// The hash is never independently stored, served, or fetched as a blob.
#[derive(Serialize)]
pub(crate) struct Chunk {
    /// The BLAKE3 content address of this byte range, `b3:<64 lowercase hex>`.
    pub(crate) hash: String,
    /// The chunk's length in bytes. The chunk sizes sum to the entry's `size`.
    pub(crate) size: u64,
}

/// The result of walking a directory: the sorted entries plus the two
/// counters the operator-facing status report (`origin import`) surfaces.
pub(crate) struct WalkOutput {
    /// Manifest entries, sorted by path bytes (deterministic).
    pub(crate) entries: Vec<BundleEntry>,
    /// Count of symlinks skipped (only non-zero without `--follow-symlinks`).
    pub(crate) skipped_symlinks: u64,
    /// Sum of every recorded entry's size.
    pub(crate) total_size: u64,
}

/// Format a BLAKE3 hash as the `b3:<hex>` content-address string.
pub(crate) fn b3_hex_str(h: blake3::Hash) -> String {
    format!("b3:{}", h.to_hex())
}

/// Compile the repeatable `--exclude` globs into a single matcher.
pub(crate) fn build_excluder(patterns: &[String]) -> anyhow::Result<GlobSet> {
    build_glob_set(patterns, "--exclude")
}

/// Compile the repeatable glob `patterns` given for `flag` into a single
/// matcher. `flag` names the source flag so a bad pattern reports the flag the
/// operator actually typed. Shared by `origin import`'s `--exclude` and
/// bundle pull's `--include`/`--exclude` entry filter.
pub(crate) fn build_glob_set(patterns: &[String], flag: &str) -> anyhow::Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for raw in patterns {
        // `literal_separator(true)` aligns with gitignore semantics: `*` does
        // not cross `/`. Operators expect `tmp/*` to match files directly under
        // `tmp/` and `tmp/**` to recurse, not for `*` to greedily span
        // separators.
        let glob = GlobBuilder::new(raw)
            .literal_separator(true)
            .build()
            .with_context(|| format!("invalid {flag} pattern {raw:?}"))?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|e| anyhow!("failed to build glob set: {e}"))
}

/// `filter_entry` predicate: whether the walker keeps (descends into / yields) a
/// non-root entry at relative path `rel`. `is_dir` is the walker's lexical file
/// type (does NOT follow symlinks). The directory branch only prunes when the
/// exclude pattern is recursive at this directory (sentinel-child probe) so a
/// one-level `tmp/*` can't strand a deeper `tmp/sub/y`.
fn keep_entry(rel: &Path, is_dir: bool, excluder: &GlobSet) -> bool {
    if !excluder.is_match(rel) {
        return true;
    }
    if !is_dir {
        // Matched file/symlink: skip it before canonicalize.
        return false;
    }
    // Matched directory: prune only if its whole subtree is excluded.
    // `__decdn_subtree_probe__` is a synthetic leaf name — its only role is to
    // distinguish a recursive pattern (`tmp/**`, which also matches the child)
    // from a one-level one (`tmp/*`, which does not).
    !excluder.is_match(rel.join("__decdn_subtree_probe__"))
}

/// Walk `root` (already canonicalized by the caller), applying the exclude set
/// and symlink-containment rules, and invoke `per_file` for every regular file
/// kept. `per_file` receives the file's canonicalized path and its validated
/// relative POSIX string, and returns the file's whole-file `b3:<hex>` address,
/// size, and an optional chunk decomposition — a dry-run caller just hashes;
/// `origin import` (without `--dry-run`) hashes **and** writes the blob; a
/// plain (non-chunking) caller always returns `None` for the third element.
///
/// The entries are sorted by path bytes so the emitted manifest — and therefore
/// its own BLAKE3 — is byte-stable across runs.
pub(crate) fn walk_and_collect<F>(
    root: &Path,
    follow_symlinks: bool,
    excluder: &GlobSet,
    mut per_file: F,
) -> anyhow::Result<WalkOutput>
where
    F: FnMut(&Path, &str) -> anyhow::Result<(String, u64, Option<Vec<Chunk>>)>,
{
    let mut entries: Vec<BundleEntry> = Vec::new();
    let mut skipped_symlinks: u64 = 0;
    let mut total_size: u64 = 0;

    // Skip / prune excluded entries *lexically*, before any canonicalize.
    // `filter_entry` runs on every yielded entry including the root: when the
    // entry is the root (empty relative path) or `strip_prefix` can't produce a
    // relative path, keep it — never prune the root. globset's `Candidate`
    // normalizes a `&Path` to its forward-slash form internally, so matching the
    // relative `&Path` here is equivalent to matching the validated `rel_str`
    // below.
    //
    // A matched FILE/symlink is skipped here, before the canonicalize step — so
    // an escaping symlink that the operator excluded (e.g. via `tmp/**`) is
    // never resolved and cannot trip the escape `bail!` below.
    //
    // A matched DIRECTORY is pruned only when its WHOLE subtree is excluded; the
    // sentinel-probe in `keep_entry` distinguishes a recursive pattern from a
    // one-level one so pruning never strands non-excluded descendants.
    let walker = WalkDir::new(root)
        .follow_links(follow_symlinks)
        .into_iter()
        .filter_entry(|entry| match entry.path().strip_prefix(root) {
            Ok(rel) if rel.as_os_str().is_empty() => true,
            Ok(rel) => keep_entry(rel, entry.file_type().is_dir(), excluder),
            Err(_) => true,
        });

    for step in walker {
        // walkdir surfaces opendir/readdir errors and (with follow_links)
        // symlink loops as Err here — propagate; never silently skip.
        let entry = step.with_context(|| format!("walking {}", root.display()))?;
        let ftype = entry.file_type();
        if ftype.is_dir() {
            continue;
        }
        // With follow_links=false, symlinks come through as symlink entries we
        // never read — skip silently and surface the count in the report. With
        // follow_links=true, walkdir resolves the link transparently and the
        // entry presents as a regular file; only then does the path-safety check
        // below run and catch escapes via in-tree symlinks.
        if ftype.is_symlink() {
            skipped_symlinks = skipped_symlinks.saturating_add(1);
            continue;
        }
        if !ftype.is_file() {
            continue;
        }

        // Canonicalize-and-contain — same shape as `FilesystemOrigin::fetch` in
        // the cache crate. With follow_links=true a walked path can resolve
        // outside the root via a symlink in any ancestor; the manifest's `path`
        // field is a relative POSIX string and cannot truthfully describe an
        // external target. Hard error.
        let canonical = std::fs::canonicalize(entry.path())
            .with_context(|| format!("canonicalize {}", entry.path().display()))?;
        if !canonical.starts_with(root) {
            bail!(
                "{} resolves to {} which is outside root {}",
                entry.path().display(),
                canonical.display(),
                root.display()
            );
        }

        // Use the lexical walked path (rooted at the canonical root) for the
        // manifest's `path` field, not the canonical resolved target — a
        // followed symlink is recorded under the name a user sees, and an
        // in-root symlink + its real target each get a distinct entry.
        let rel = entry
            .path()
            .strip_prefix(root)
            .map_err(|_| anyhow!("strip_prefix failed for {}", entry.path().display()))?;
        let rel_str = validate_relpath(rel)?;

        // Cheap backstop: `filter_entry` above already pruned excluded subtrees
        // lexically. Keep it so the exclusion contract holds even if the
        // pre-walk filter and this validated key ever diverge.
        if excluder.is_match(&rel_str) {
            continue;
        }

        let (hash, size, chunks) = per_file(&canonical, &rel_str)
            .with_context(|| format!("processing {}", canonical.display()))?;

        total_size = total_size.checked_add(size).ok_or_else(|| {
            anyhow!("total_size overflow at {rel_str}: running={total_size} adding={size}")
        })?;

        entries.push(BundleEntry {
            path: rel_str,
            hash,
            size,
            chunks,
        });
    }

    // Sort by path bytes — deterministic and matches the byte order any verifier
    // would use when independently rebuilding the manifest from the same tree.
    entries.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));

    Ok(WalkOutput {
        entries,
        skipped_symlinks,
        total_size,
    })
}

/// Build the POSIX-`/`-joined relative path string from a `Path`, rejecting any
/// non-`Normal` component. Defensive — `strip_prefix` on canonical paths
/// shouldn't produce these segments — but the manifest's `path` field is the
/// contract verifiers re-validate against, so we assert here rather than
/// trusting the upstream walker.
pub(crate) fn validate_relpath(rel: &Path) -> anyhow::Result<String> {
    let mut out = String::new();
    let mut empty = true;
    for comp in rel.components() {
        match comp {
            Component::Normal(os) => {
                let s = os
                    .to_str()
                    .ok_or_else(|| anyhow!("non-UTF-8 path component in {}", rel.display()))?;
                if !empty {
                    out.push('/');
                }
                out.push_str(s);
                empty = false;
            }
            Component::CurDir => {
                // ADR `appendix-bundles.md` § Path-safety rules requires every
                // component to be `Component::Normal`. Rust's Path::components
                // normalizes most `.` segments away, but a leading `./` still
                // surfaces here — bail to match the spec rather than silently
                // collapse to `Normal` order.
                bail!("rejected current-dir component '.' in {}", rel.display());
            }
            Component::ParentDir => {
                bail!("rejected parent-dir component '..' in {}", rel.display());
            }
            Component::RootDir => {
                bail!("rejected root-dir component '/' in {}", rel.display());
            }
            Component::Prefix(_) => {
                bail!("rejected path prefix component in {}", rel.display());
            }
        }
    }
    if out.is_empty() {
        bail!("empty relative path");
    }
    Ok(out)
}

/// Open a file once and pull both the BLAKE3 hash and the size from the open
/// handle. The fd-derived `metadata()` (`fstat` on Unix) closes the TOCTOU
/// window between a separate metadata call and the read.
/// `blake3::Hasher::update_reader` is the upstream-recommended streaming
/// primitive; it manages its own buffer and retries on `Interrupted` internally.
pub(crate) fn hash_file_at(path: &Path) -> std::io::Result<(blake3::Hash, u64)> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut hasher = blake3::Hasher::new();
    hasher.update_reader(&mut file)?;
    Ok((hasher.finalize(), size))
}

/// Render the manifest to its canonical byte form: single-line compact JSON, no
/// trailing newline, UTF-8, struct-order keys.
pub(crate) fn serialize_canonical(entries: &[BundleEntry]) -> anyhow::Result<Vec<u8>> {
    let bundle = Bundle {
        version: 1,
        entries,
    };
    serde_json::to_vec(&bundle).map_err(|e| anyhow!("serialize bundle: {e}"))
}

/// Write manifest bytes to `target` atomically. `NamedTempFile` creates a
/// uniquely-named file in the destination directory via `O_CREAT|O_EXCL`, so
/// there is no predictable `<output>.partial` path an adversary or concurrent
/// writer can plant a symlink at. `persist` does the cross-platform atomic
/// rename-replace — when `target` already exists as a symlink, `rename(2)`
/// replaces the symlink itself with the new file, it does not follow it.
pub(crate) fn write_bundle(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
    let mut tmp = match parent {
        Some(p) => tempfile::NamedTempFile::new_in(p)?,
        None => tempfile::NamedTempFile::new_in(".")?,
    };
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(target).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
