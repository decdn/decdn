//! `decdn bundle create` — walk a directory, BLAKE3-hash each regular
//! file, and emit a canonical JSON manifest. Issue #391.
//!
//! The on-disk schema, hash format (`b3:<hex>`), path-safety rules, and
//! the determinism contract that makes single-hash bundle distribution
//! viable are specified in
//! [`appendix-bundles`](../../../../adr/appendix-bundles.md). The walk
//! is synchronous (walkdir + blake3 are sync, no payoff in interleaving
//! for trees that fit on a publisher's local disk), wrapped once in
//! `tokio::task::spawn_blocking` from the async entry point so the
//! publisher CLI's runtime isn't held up.

use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::{Component, Path};

use anyhow::{Context as _, anyhow, bail};
use decdn_common::cli::{BundleArgs, BundleCommand, BundleCreateArgs};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::Serialize;
use walkdir::WalkDir;

const HASH_BUF_SIZE: usize = 64 * 1024;

/// Top-level dispatcher for `decdn bundle ...`. Single-variant today —
/// `Pull` is deferred (see `appendix-bundles.md` § Future work).
pub async fn bundle_dispatch(args: &BundleArgs) -> anyhow::Result<()> {
    match &args.cmd {
        BundleCommand::Create(create_args) => bundle_create(create_args).await,
    }
}

// Do not reorder: JSON key order is part of the bundle hash. Determinism
// requires the produced bytes to be stable across runs, so the only
// way to add a field in v1 is to add it at the end (and prefer a v2 bump
// instead — see `appendix-bundles.md` § Determinism).
#[derive(Serialize)]
struct Bundle<'a> {
    version: u32,
    entries: &'a [BundleEntry],
}

#[derive(Serialize)]
struct BundleEntry {
    path: String,
    hash: String,
    // Writer-side: always emitted. The future `bundle pull` reader will
    // model this as `Option<u64>` because the wire format permits absence,
    // but the create path always has the metadata in hand.
    size: u64,
}

/// Status report emitted to stdout after a successful `bundle create`.
/// Operator-facing — the bundle file itself is always JSON, and `--json`
/// only swaps the human summary for this single-line shape.
#[derive(Serialize)]
struct CreateReport {
    bundle: String,
    entries: u64,
    total_size: u64,
    bundle_hash: String,
    skipped_symlinks: u64,
}

struct CollectOutput {
    entries: Vec<BundleEntry>,
    skipped_symlinks: u64,
    total_size: u64,
}

/// Public entry. Validates inputs, performs the (sync) walk inside
/// `spawn_blocking`, writes the bundle, prints the status report.
pub async fn bundle_create(args: &BundleCreateArgs) -> anyhow::Result<()> {
    let input = args.input.clone();
    let output = args.output.clone();
    let follow = args.follow_symlinks;
    let exclude = args.exclude.clone();

    let report = tokio::task::spawn_blocking(move || -> anyhow::Result<CreateReport> {
        let excluder = build_excluder(&exclude)?;

        if !input.is_dir() {
            bail!("--input {} is not an existing directory", input.display());
        }
        let root = std::fs::canonicalize(&input)
            .with_context(|| format!("canonicalize --input {}", input.display()))?;

        // The walker is rooted at the already-canonical path on purpose —
        // every produced `entry.path()` is then a descendant of `root`,
        // and the `starts_with(root)` invariant below holds without an
        // extra canonicalize step on each entry's lexical prefix.
        let collected = collect_entries(&root, follow, &excluder)?;
        let bundle_bytes = serialize_canonical(&collected.entries)?;
        let bundle_hash = blake3::hash(&bundle_bytes);

        write_bundle(&output, &bundle_bytes)
            .with_context(|| format!("write --output {}", output.display()))?;

        let entries_count = u64::try_from(collected.entries.len())
            .map_err(|_| anyhow!("entry count {} exceeds u64", collected.entries.len()))?;
        Ok(CreateReport {
            bundle: output.display().to_string(),
            entries: entries_count,
            total_size: collected.total_size,
            bundle_hash: b3_hex_str(bundle_hash),
            skipped_symlinks: collected.skipped_symlinks,
        })
    })
    .await
    .map_err(|e| {
        // JoinError fires on panic or cancellation — don't lie about
        // which one happened. Pattern mirrors `crates/cache/src/engine.rs`
        // (the workspace's canonical handler-side template).
        let note = if e.is_panic() {
            "bundle create task panicked"
        } else if e.is_cancelled() {
            "bundle create task cancelled"
        } else {
            "bundle create task failed to join"
        };
        anyhow::Error::from(e).context(note)
    })??;

    let mut stdout = std::io::stdout().lock();
    write_create_report(&mut stdout, &report, args.json)
        .map_err(|e| anyhow!("failed to write status report: {e}"))?;
    Ok(())
}

fn b3_hex_str(h: blake3::Hash) -> String {
    format!("b3:{}", h.to_hex())
}

fn build_excluder(patterns: &[String]) -> anyhow::Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for raw in patterns {
        // `literal_separator(true)` aligns with gitignore semantics: `*`
        // does not cross `/`. Operators expect `tmp/*` to match files
        // directly under `tmp/` and `tmp/**` to recurse, not for `*` to
        // greedily span path separators.
        let glob = GlobBuilder::new(raw)
            .literal_separator(true)
            .build()
            .with_context(|| format!("invalid --exclude pattern {raw:?}"))?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|e| anyhow!("failed to build glob set: {e}"))
}

fn collect_entries(
    root: &Path,
    follow_symlinks: bool,
    excluder: &GlobSet,
) -> anyhow::Result<CollectOutput> {
    let mut entries: Vec<BundleEntry> = Vec::new();
    let mut skipped_symlinks: u64 = 0;
    let mut total_size: u64 = 0;

    let walker = WalkDir::new(root).follow_links(follow_symlinks).into_iter();

    for step in walker {
        // walkdir surfaces opendir/readdir errors and (with follow_links)
        // symlink loops as Err here — propagate; never silently skip.
        let entry = step.with_context(|| format!("walking {}", root.display()))?;
        let ftype = entry.file_type();
        if ftype.is_dir() {
            continue;
        }
        // With follow_links=false, symlinks come through as symlink entries
        // we never read — skip silently and surface the count in --json.
        // With follow_links=true, walkdir resolves the link transparently
        // and the entry presents as a regular file; the path-safety check
        // below catches escapes regardless of the follow flag.
        if ftype.is_symlink() {
            skipped_symlinks = skipped_symlinks.saturating_add(1);
            continue;
        }
        if !ftype.is_file() {
            continue;
        }

        // Canonicalize-and-contain — same shape as `FilesystemOrigin::fetch`
        // in the cache crate. With follow_links=true a walked path can
        // resolve outside the root via a symlink in any ancestor; the
        // bundle's `path` field is a relative POSIX string and cannot
        // truthfully describe an external target. Hard error.
        let canonical = std::fs::canonicalize(entry.path())
            .with_context(|| format!("canonicalize {}", entry.path().display()))?;
        if !canonical.starts_with(root) {
            bail!(
                "{} resolves to {} which is outside --input root {}",
                entry.path().display(),
                canonical.display(),
                root.display()
            );
        }

        // Use the lexical walked path (rooted at the canonical root) for
        // the bundle's `path` field, not the canonical resolved target.
        // That way a followed symlink is recorded under the name a user
        // sees in the directory, and an in-root symlink + its real target
        // each get a distinct entry instead of colliding.
        let rel = entry
            .path()
            .strip_prefix(root)
            .map_err(|_| anyhow!("strip_prefix failed for {}", entry.path().display()))?;
        let rel_str = validate_relpath(rel)?;

        if excluder.is_match(&rel_str) {
            continue;
        }

        let metadata = entry
            .metadata()
            .with_context(|| format!("metadata for {}", entry.path().display()))?;
        let size = metadata.len();
        let hash = hash_file_streaming(&canonical)
            .with_context(|| format!("hash {}", canonical.display()))?;

        total_size = total_size.checked_add(size).ok_or_else(|| {
            anyhow!("total_size overflow at {rel_str}: running={total_size} adding={size}")
        })?;

        entries.push(BundleEntry {
            path: rel_str,
            hash: b3_hex_str(hash),
            size,
        });
    }

    // Sort by path bytes — deterministic and matches the byte order any
    // verifier would use when independently rebuilding the bundle from
    // the same source tree.
    entries.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));

    Ok(CollectOutput {
        entries,
        skipped_symlinks,
        total_size,
    })
}

/// Build the POSIX-`/`-joined relative path string from a `Path`,
/// rejecting any non-`Normal` component. Defensive — `strip_prefix` on
/// canonical paths shouldn't produce these segments — but the bundle's
/// `path` field is the contract verifiers re-validate against, so we
/// assert here rather than trusting the upstream walker.
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
            Component::CurDir => {}
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

/// Stream a file through blake3 with a 64 KiB buffer. Avoids loading
/// the whole file into memory for large blobs. Retries on `Interrupted`
/// (EINTR) so a stray signal during a multi-GB hash doesn't surface as
/// a permanent error — same idiom as `std::io::copy`.
fn hash_file_streaming(path: &Path) -> std::io::Result<blake3::Hash> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; HASH_BUF_SIZE];
    loop {
        let n = match file.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if n == 0 {
            break;
        }
        let chunk = buf
            .get(..n)
            .ok_or_else(|| std::io::Error::other("read returned out-of-range len"))?;
        hasher.update(chunk);
    }
    Ok(hasher.finalize())
}

/// Render the bundle to its canonical byte form: single-line compact JSON,
/// no trailing newline, UTF-8, struct-order keys.
fn serialize_canonical(entries: &[BundleEntry]) -> anyhow::Result<Vec<u8>> {
    let bundle = Bundle {
        version: 1,
        entries,
    };
    serde_json::to_vec(&bundle).map_err(|e| anyhow!("serialize bundle: {e}"))
}

/// Write the bundle bytes atomically: full content lands in a sibling
/// `<output>.partial`, gets fsync'd, then rename-replaces the target.
/// On a crash mid-write the half-written file is `.partial`, never the
/// operator-expected name — downstream tooling that reads `--output`
/// without re-running create can't be tricked by a torn write.
fn write_bundle(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let file_name = target
        .file_name()
        .ok_or_else(|| std::io::Error::other("--output has no file-name component"))?;
    let mut tmp_name = file_name.to_owned();
    tmp_name.push(".partial");
    let tmp_path = target.with_file_name(tmp_name);
    {
        let mut tmp = File::create(&tmp_path)?;
        if let Err(e) = tmp.write_all(bytes).and_then(|()| tmp.sync_all()) {
            // Don't leave the partial behind on a write failure — operator
            // re-runs should not see two artifacts. Best-effort cleanup;
            // the original error wins.
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
    }
    if let Err(e) = std::fs::rename(&tmp_path, target) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }
    Ok(())
}

fn write_create_report(
    w: &mut impl Write,
    report: &CreateReport,
    json: bool,
) -> std::io::Result<()> {
    if json {
        let line = serde_json::to_string(report)
            .map_err(|e| std::io::Error::other(format!("serialize status report: {e}")))?;
        writeln!(w, "{line}")
    } else {
        writeln!(
            w,
            "wrote {} ({} entries, {} bytes, hash {})",
            report.bundle, report.entries, report.total_size, report.bundle_hash
        )?;
        if report.skipped_symlinks > 0 {
            writeln!(
                w,
                "skipped {} symlink(s); pass --follow-symlinks to include",
                report.skipped_symlinks
            )?;
        }
        Ok(())
    }
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
    fn hash_file_streaming_matches_in_memory_blake3() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("hello.txt");
        std::fs::write(&path, b"hello world\n").unwrap();
        let got = hash_file_streaming(&path).unwrap();
        assert_eq!(got, blake3::hash(b"hello world\n"));
    }

    #[test]
    fn write_bundle_is_atomic_no_partial_on_success() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("bundle.json");
        write_bundle(&target, b"{\"version\":1,\"entries\":[]}").unwrap();
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"{\"version\":1,\"entries\":[]}"
        );
        // No `.partial` artifact left behind.
        let partial = dir.path().join("bundle.json.partial");
        assert!(!partial.exists(), "partial should not survive success");
    }

    #[test]
    fn create_report_json_shape() {
        let report = CreateReport {
            bundle: "/tmp/x.json".into(),
            entries: 3,
            total_size: 42,
            bundle_hash: "b3:cafef00d".into(),
            skipped_symlinks: 1,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_create_report(&mut buf, &report, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(buf.trim_ascii_end()).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 5);
        assert_eq!(obj["bundle"].as_str(), Some("/tmp/x.json"));
        assert_eq!(obj["entries"].as_u64(), Some(3));
        assert_eq!(obj["total_size"].as_u64(), Some(42));
        assert_eq!(obj["bundle_hash"].as_str(), Some("b3:cafef00d"));
        assert_eq!(obj["skipped_symlinks"].as_u64(), Some(1));
    }
}
