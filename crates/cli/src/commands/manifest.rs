//! Shared bundle-manifest machinery: the directory walk, path-safety rules,
//! canonical JSON serialization, and atomic manifest write used by both
//! `decdn bundle create` and `decdn origin import` (issues #391, #1904).
//!
//! Both commands walk a directory the same way and emit the **same** canonical
//! manifest bytes, so a directory seeded with `origin import` is retrievable by
//! the exact bundle hash `bundle create` would report for it. Keeping the walk
//! and the serializer in one place is what makes that byte-identity hold by
//! construction rather than by two copies staying in sync.
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
#[derive(Serialize)]
pub(crate) struct BundleEntry {
    /// Relative POSIX path within the bundle root (`a/b.txt`, never absolute).
    pub(crate) path: String,
    /// The file's BLAKE3 content address, `b3:<64 lowercase hex>`.
    pub(crate) hash: String,
    /// The file's length in bytes.
    pub(crate) size: u64,
}

/// The result of walking a directory: the sorted entries plus the two counters
/// the operator-facing status reports (`bundle create`, `origin import`) surface.
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
/// operator actually typed. Shared by `--exclude` (bundle create / origin
/// import) and bundle pull's `--include`/`--exclude` entry filter.
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
/// relative POSIX string, and returns the file's `b3:<hex>` address and size —
/// `bundle create` just hashes; `origin import` hashes **and** writes the blob.
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
    F: FnMut(&Path, &str) -> anyhow::Result<(String, u64)>,
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
        // never read — skip silently and surface the count in --json. With
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

        let (hash, size) = per_file(&canonical, &rel_str)
            .with_context(|| format!("processing {}", canonical.display()))?;

        total_size = total_size.checked_add(size).ok_or_else(|| {
            anyhow!("total_size overflow at {rel_str}: running={total_size} adding={size}")
        })?;

        entries.push(BundleEntry {
            path: rel_str,
            hash,
            size,
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
fn validate_relpath(rel: &Path) -> anyhow::Result<String> {
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
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn validate_relpath_rejects_parent_dir() {
        let rel = PathBuf::from("foo/../bar");
        let err = validate_relpath(&rel).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("parent-dir"), "msg was: {msg}");
        assert!(msg.contains(".."), "msg was: {msg}");
    }

    #[cfg(unix)]
    #[test]
    fn validate_relpath_rejects_absolute_unix() {
        let rel = PathBuf::from("/etc/passwd");
        let err = validate_relpath(&rel).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("root-dir"), "msg was: {msg}");
    }

    #[test]
    fn validate_relpath_rejects_current_dir() {
        let rel = PathBuf::from("./a/b");
        let err = validate_relpath(&rel).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("current-dir"), "msg was: {msg}");
    }

    #[test]
    fn validate_relpath_rejects_empty() {
        let err = validate_relpath(Path::new("")).unwrap_err();
        assert!(format!("{err:#}").contains("empty"));
    }

    #[test]
    fn validate_relpath_joins_with_posix_slash() {
        let p = PathBuf::from("a").join("b").join("c.txt");
        let s = validate_relpath(&p).unwrap();
        assert_eq!(s, "a/b/c.txt");
    }

    #[test]
    fn build_excluder_compiles_pattern() {
        let g = build_excluder(&["*.log".to_string()]).unwrap();
        assert!(g.is_match("file.log"));
        assert!(!g.is_match("file.txt"));
    }

    #[test]
    fn build_excluder_combines_multiple_patterns() {
        let g = build_excluder(&["*.log".to_string(), "tmp/*".to_string()]).unwrap();
        assert!(g.is_match("a.log"));
        assert!(g.is_match("tmp/x"));
        assert!(!g.is_match("src/main.rs"));
    }

    #[test]
    fn build_excluder_rejects_invalid_pattern() {
        let err = build_excluder(&["[".to_string()]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid --exclude pattern"), "msg was: {msg}");
    }

    #[test]
    fn b3_hex_str_format_matches_documented_shape() {
        let h = blake3::hash(b"");
        let s = b3_hex_str(h);
        assert!(s.starts_with("b3:"));
        assert_eq!(s.len(), 3 + 64);
    }

    #[test]
    fn serialize_canonical_emits_struct_field_order_no_newline() {
        let entries = vec![BundleEntry {
            path: "a.txt".into(),
            hash: "b3:abc".into(),
            size: 12,
        }];
        let bytes = serialize_canonical(&entries).unwrap();
        let s = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(
            s,
            "{\"version\":1,\"entries\":[{\"path\":\"a.txt\",\"hash\":\"b3:abc\",\"size\":12}]}"
        );
        assert!(!s.ends_with('\n'));
    }

    #[test]
    fn serialize_canonical_empty_entries() {
        let bytes = serialize_canonical(&[]).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            "{\"version\":1,\"entries\":[]}"
        );
    }

    #[test]
    fn hash_file_at_matches_in_memory_blake3_and_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("hello.txt");
        std::fs::write(&path, b"hello world\n").unwrap();
        let (hash, size) = hash_file_at(&path).unwrap();
        assert_eq!(hash, blake3::hash(b"hello world\n"));
        assert_eq!(size, 12);
    }

    #[test]
    fn write_bundle_persists_target_no_temp_leftover() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("bundle.json");
        write_bundle(&target, b"{\"version\":1,\"entries\":[]}").unwrap();
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"{\"version\":1,\"entries\":[]}"
        );
        let mut entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|r| r.unwrap().file_name())
            .collect();
        entries.sort();
        assert_eq!(entries, vec![std::ffi::OsString::from("bundle.json")]);
    }

    #[test]
    fn write_bundle_overwrites_existing_target() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("bundle.json");
        std::fs::write(&target, b"old").unwrap();
        write_bundle(&target, b"{\"version\":1,\"entries\":[]}").unwrap();
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"{\"version\":1,\"entries\":[]}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn write_bundle_replaces_symlink_target_does_not_follow() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().unwrap();
        let decoy = dir.path().join("decoy.txt");
        std::fs::write(&decoy, b"do not touch").unwrap();
        let target = dir.path().join("bundle.json");
        symlink(&decoy, &target).unwrap();

        write_bundle(&target, b"{\"version\":1,\"entries\":[]}").unwrap();

        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"{\"version\":1,\"entries\":[]}"
        );
        let meta = std::fs::symlink_metadata(&target).unwrap();
        assert!(
            meta.file_type().is_file(),
            "target should be a regular file"
        );
        assert_eq!(std::fs::read(&decoy).unwrap(), b"do not touch");
    }

    // Collect a canonical-rooted tree and return the set of manifest paths,
    // using the same per-file hasher `bundle create` uses.
    fn collect_paths(
        root: &Path,
        follow_symlinks: bool,
        exclude: &[&str],
    ) -> anyhow::Result<Vec<String>> {
        let canonical = std::fs::canonicalize(root)?;
        let excluder =
            build_excluder(&exclude.iter().map(|s| (*s).to_string()).collect::<Vec<_>>())?;
        let out = walk_and_collect(&canonical, follow_symlinks, &excluder, |canon, _rel| {
            let (h, s) = hash_file_at(canon)?;
            Ok((b3_hex_str(h), s))
        })?;
        Ok(out.entries.into_iter().map(|e| e.path).collect())
    }

    #[test]
    fn collect_excludes_matched_files() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("keep.txt"), b"k").unwrap();
        std::fs::write(dir.path().join("drop.log"), b"d").unwrap();
        let paths = collect_paths(dir.path(), false, &["*.log"]).unwrap();
        assert_eq!(paths, vec!["keep.txt".to_string()]);
    }

    #[test]
    fn collect_prunes_recursively_excluded_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("tmp/sub")).unwrap();
        std::fs::write(dir.path().join("tmp/a.txt"), b"a").unwrap();
        std::fs::write(dir.path().join("tmp/sub/b.txt"), b"b").unwrap();
        std::fs::write(dir.path().join("keep.txt"), b"k").unwrap();
        let paths = collect_paths(dir.path(), false, &["tmp/**"]).unwrap();
        assert_eq!(paths, vec!["keep.txt".to_string()]);
    }

    #[test]
    fn collect_one_level_glob_keeps_deeper_files() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("tmp/sub")).unwrap();
        std::fs::write(dir.path().join("tmp/x.txt"), b"x").unwrap();
        std::fs::write(dir.path().join("tmp/sub/y.txt"), b"y").unwrap();
        std::fs::write(dir.path().join("keep.txt"), b"k").unwrap();
        let mut paths = collect_paths(dir.path(), false, &["tmp/*"]).unwrap();
        paths.sort();
        assert_eq!(
            paths,
            vec!["keep.txt".to_string(), "tmp/sub/y.txt".to_string()]
        );
    }

    #[cfg(unix)]
    #[test]
    fn collect_excluded_dir_with_escaping_symlink_does_not_error() {
        use std::os::unix::fs::symlink;

        let outside = tempfile::TempDir::new().unwrap();
        std::fs::write(outside.path().join("secret.txt"), b"s").unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("tmp")).unwrap();
        std::fs::write(dir.path().join("tmp/inner.txt"), b"i").unwrap();
        symlink(
            outside.path().join("secret.txt"),
            dir.path().join("tmp/escape"),
        )
        .unwrap();
        std::fs::write(dir.path().join("keep.txt"), b"k").unwrap();

        let paths = collect_paths(dir.path(), true, &["tmp/**"]).unwrap();
        assert_eq!(paths, vec!["keep.txt".to_string()]);
    }

    #[cfg(unix)]
    #[test]
    fn collect_non_excluded_escaping_symlink_errors() {
        use std::os::unix::fs::symlink;

        let outside = tempfile::TempDir::new().unwrap();
        std::fs::write(outside.path().join("secret.txt"), b"s").unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        symlink(outside.path().join("secret.txt"), dir.path().join("escape")).unwrap();

        let err = collect_paths(dir.path(), true, &[]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("outside root"), "msg was: {msg}");
    }

    #[cfg(unix)]
    #[test]
    fn collect_excluded_symlinked_dir_does_not_error() {
        use std::os::unix::fs::symlink;

        let outside = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(outside.path().join("payload")).unwrap();
        std::fs::write(outside.path().join("payload/secret.txt"), b"s").unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        symlink(outside.path().join("payload"), dir.path().join("tmp")).unwrap();
        std::fs::write(dir.path().join("keep.txt"), b"k").unwrap();

        let paths = collect_paths(dir.path(), true, &["tmp/**"]).unwrap();
        assert_eq!(paths, vec!["keep.txt".to_string()]);
    }
}
