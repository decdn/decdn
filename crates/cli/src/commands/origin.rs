//! `decdn origin import` — seed a cache origin store with local content
//! (issue #1904).
//!
//! For every blob it writes the sharded data object `{base}/{hex[0..2]}/{hex}`
//! and its sibling pre-order bao outboard `{hex}.obao4` — the exact layout the
//! daemon reads back (ADR 037 §Origin-tier pull-through). The hash + outboard
//! computation is backend-independent (an origin store is content-addressed with
//! an identical object layout across the fs and s3 backends); only the final
//! write differs, so the `--to` target selects a writer. v1 implements the
//! `fs:<dir>` target; `s3://` is an additive follow-up.
//!
//! The work is synchronous (bao encoding + filesystem writes are sync), wrapped
//! once in `tokio::task::spawn_blocking` from the async entry point so the CLI's
//! runtime isn't held up — the same shape as `bundle create`.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail};
use decdn_bao_range::{EncodedOutboard, encode_outboard};
use decdn_common::cli::{OriginArgs, OriginCommand, OriginImportArgs};
use serde::Serialize;

use super::manifest::{
    build_excluder, hash_file_at, serialize_canonical, walk_and_collect, write_bundle,
};

/// Dispatcher for `decdn origin ...`. Every subcommand is offline, config-free
/// local work, so no `config_path` is threaded through.
pub async fn origin_dispatch(args: &OriginArgs) -> anyhow::Result<()> {
    match &args.cmd {
        OriginCommand::Import(import_args) => origin_import(import_args).await,
    }
}

/// The parsed `--to` target. Only the filesystem writer is implemented; the
/// enum exists so an `s3://` writer is an additive variant, not a rewrite.
#[derive(Debug)]
enum ImportTarget {
    /// A local filesystem origin store rooted at this directory.
    Fs(PathBuf),
}

/// Parse a `--to <backend>:<location>` target string. `fs:<dir>` is the only
/// implemented backend; `s3://` and `http(s)://` produce actionable errors.
fn parse_target(raw: &str) -> anyhow::Result<ImportTarget> {
    if let Some(dir) = raw.strip_prefix("fs:") {
        if dir.is_empty() {
            bail!("--to fs: needs a directory, e.g. --to fs:/var/lib/decdn/origin");
        }
        return Ok(ImportTarget::Fs(PathBuf::from(dir)));
    }
    if raw.starts_with("s3://") {
        bail!(
            "--to {raw} is not yet implemented; v1 imports to a fs: target only \
             (the object layout is identical, so s3 is a fast follow)"
        );
    }
    if raw.starts_with("http://") || raw.starts_with("https://") {
        bail!(
            "--to {raw} is not a write target: an HTTP origin is a read-only \
             static server — seed the fs: layout onto the disk it serves instead"
        );
    }
    bail!("--to {raw} is not a recognized target; use fs:<dir>");
}

/// One-line JSON / human status report emitted after a successful import.
#[derive(Serialize)]
struct ImportReport {
    /// Number of source files imported (excludes the derived bundle manifest).
    imported: u64,
    /// Sum of the imported source files' sizes in bytes.
    bytes: u64,
    /// The `--to` target string the blobs were written to.
    origin: String,
    /// Every imported source file's path, relative to `--input`, mapped to its
    /// `b3:<hex>` content address. For a single-file import the one key is the
    /// file's own name; for a directory it is the POSIX relative path within the
    /// tree. Excludes the derived bundle manifest — that is `bundle_hash`.
    files: BTreeMap<String, String>,
    /// The bundle hash publishers distribute — `Some` for a directory import,
    /// `None` for a single file (which produces no manifest).
    bundle_hash: Option<String>,
    /// Whether sources were moved (`--move`) rather than copied.
    moved: bool,
}

/// Entry point. Parses the target, dispatches file vs directory import inside
/// `spawn_blocking`, and prints the status report.
pub async fn origin_import(args: &OriginImportArgs) -> anyhow::Result<()> {
    let target = parse_target(&args.to)?;
    let origin_label = args.to.clone();

    let input = args.input.clone();
    let follow = args.follow_symlinks;
    let exclude = args.exclude.clone();
    let bundle_out = args.bundle.clone();
    let move_source = args.move_source;
    let force = args.force;

    let report = tokio::task::spawn_blocking(move || -> anyhow::Result<ImportReport> {
        let ImportTarget::Fs(base) = target;
        // Create the origin root up front; a single file and a directory both
        // need it to exist before the first shard `create_dir_all`.
        std::fs::create_dir_all(&base)
            .map_err(|e| anyhow!("create origin dir {}: {e}", base.display()))?;
        let base = std::fs::canonicalize(&base)
            .map_err(|e| anyhow!("canonicalize origin dir {}: {e}", base.display()))?;

        let meta = std::fs::symlink_metadata(&input)
            .map_err(|e| anyhow!("--input {}: {e}", input.display()))?;

        if meta.is_dir() {
            import_directory(
                &base,
                &input,
                follow,
                &exclude,
                bundle_out.as_deref(),
                move_source,
                force,
                origin_label,
            )
        } else if meta.is_file() {
            let blob = import_one_file(&base, &input, move_source, force)?;
            // Key the single entry by the file's own name — the path it has
            // relative to its parent (the "--input directory"), never the
            // absolute or working-directory-relative path the operator typed.
            let name = input.file_name().map_or_else(
                || input.display().to_string(),
                |n| n.to_string_lossy().into_owned(),
            );
            let files = BTreeMap::from([(name, b3_hex_str_from_hex(&blob.hash_hex))]);
            Ok(ImportReport {
                imported: 1,
                bytes: blob.size,
                origin: origin_label,
                files,
                bundle_hash: None,
                moved: move_source,
            })
        } else {
            bail!(
                "--input {} is neither a regular file nor a directory",
                input.display()
            );
        }
    })
    .await
    .map_err(|e| {
        let note = if e.is_panic() {
            "origin import task panicked"
        } else if e.is_cancelled() {
            "origin import task cancelled"
        } else {
            "origin import task failed to join"
        };
        anyhow::Error::from(e).context(note)
    })??;

    let mut stdout = std::io::stdout().lock();
    write_import_report(&mut stdout, &report, args.json)
        .map_err(|e| anyhow!("failed to write status report: {e}"))?;
    Ok(())
}

/// Import a whole directory tree: import every regular file, emit the canonical
/// bundle manifest, import the manifest blob itself, and (optionally) write the
/// manifest file. The manifest bytes are byte-identical to `bundle create`, so
/// the tree is retrievable by the reported bundle hash.
#[allow(clippy::too_many_arguments)] // one offline command's flags; a params struct would only indirect
fn import_directory(
    base: &Path,
    input: &Path,
    follow: bool,
    exclude: &[String],
    bundle_out: Option<&Path>,
    move_source: bool,
    force: bool,
    origin_label: String,
) -> anyhow::Result<ImportReport> {
    let excluder = build_excluder(exclude)?;
    let root = std::fs::canonicalize(input)
        .map_err(|e| anyhow!("canonicalize --input {}: {e}", input.display()))?;

    let collected = walk_and_collect(&root, follow, &excluder, |canonical, _rel| {
        let blob = import_one_file(base, canonical, move_source, force)?;
        Ok((b3_hex_str_from_hex(&blob.hash_hex), blob.size))
    })?;

    let bundle_bytes = serialize_canonical(&collected.entries)?;
    // Import the manifest blob itself so the whole tree is retrievable by one
    // hash. The manifest is in-memory bytes, always copied (never moved).
    let manifest_blob = import_bytes(base, &bundle_bytes, force)?;

    if let Some(out) = bundle_out {
        write_bundle(out, &bundle_bytes)
            .map_err(|e| anyhow!("write --bundle {}: {e}", out.display()))?;
    }

    let imported = u64::try_from(collected.entries.len())
        .map_err(|_| anyhow!("entry count {} exceeds u64", collected.entries.len()))?;
    // Each manifest entry's `path` is already the validated POSIX path relative
    // to the bundle root, and its `hash` is the `b3:<hex>` address — exactly the
    // file→hash map the report surfaces.
    let files = collected
        .entries
        .into_iter()
        .map(|e| (e.path, e.hash))
        .collect();
    Ok(ImportReport {
        imported,
        bytes: collected.total_size,
        origin: origin_label,
        files,
        bundle_hash: Some(format!("b3:{}", manifest_blob.hash_hex)),
        moved: move_source,
    })
}

/// A blob written to the origin store: its content address and byte length.
struct ImportedBlob {
    hash_hex: String,
    size: u64,
}

/// Prefix a bare 64-hex address with the manifest's `b3:` scheme.
fn b3_hex_str_from_hex(hex: &str) -> String {
    format!("b3:{hex}")
}

/// Import a single source file into the filesystem origin `base`. Streams the
/// file once to compute its hash + outboard; copies (default) or moves
/// (`--move`) the data object into `{base}/{hex[0..2]}/{hex}` and writes the
/// `{hex}.obao4` sibling, both atomically. Idempotent: a data object already
/// present at its content address is left as-is (a matching hash is matching
/// bytes); a size mismatch is refused unless `--force`.
fn import_one_file(
    base: &Path,
    source: &Path,
    move_source: bool,
    force: bool,
) -> anyhow::Result<ImportedBlob> {
    let file = File::open(source).map_err(|e| anyhow!("open {}: {e}", source.display()))?;
    let size = file
        .metadata()
        .map_err(|e| anyhow!("stat {}: {e}", source.display()))?
        .len();

    // Prep (hash + outboard) is backend-independent. In copy mode we tee the
    // read into a staged temp data object so the single read pass produces both
    // the outboard and the on-disk copy; in move mode we rename the source into
    // place afterward, so no copy is staged.
    let (eo, staged): (EncodedOutboard, Option<tempfile::NamedTempFile>) = if move_source {
        let eo = encode_outboard(file, size)
            .map_err(|e| anyhow!("encode outboard for {}: {e}", source.display()))?;
        (eo, None)
    } else {
        let tmp = tempfile::NamedTempFile::new_in(base)
            .map_err(|e| anyhow!("stage temp data object in {}: {e}", base.display()))?;
        let eo = {
            let sink = tmp.as_file();
            let tee = TeeReader::new(file, sink);
            encode_outboard(tee, size)
                .map_err(|e| anyhow!("encode outboard for {}: {e}", source.display()))?
        };
        tmp.as_file()
            .sync_all()
            .map_err(|e| anyhow!("sync staged data object: {e}"))?;
        (eo, Some(tmp))
    };

    let (shard_dir, data_path, obao4_path) = object_paths(base, &eo);
    std::fs::create_dir_all(&shard_dir)
        .map_err(|e| anyhow!("create shard dir {}: {e}", shard_dir.display()))?;

    if data_slot_needs_write(&data_path, size, force, &eo.hash_hex)? {
        if move_source {
            move_into_place(source, base, &data_path)?;
        } else if let Some(tmp) = staged {
            tmp.persist(&data_path)
                .map_err(|e| anyhow!("persist data object {}: {}", data_path.display(), e.error))?;
        }
    } else if move_source {
        // The object is already present (its size matched) and `--move` is about
        // to delete the only other copy. A length match does NOT prove the store
        // holds the right bytes — a same-size corrupt/foreign object at this hash
        // would let us silently discard the source. Re-hash the present object
        // and refuse to delete the source unless it verifies.
        let (existing_hash, _) = hash_file_at(&data_path)
            .map_err(|e| anyhow!("verify existing object {}: {e}", data_path.display()))?;
        if existing_hash.to_hex().as_str() != eo.hash_hex.as_str() {
            bail!(
                "origin object {} already exists at {} but its bytes do not match \
                 their hash; refusing to delete the moved source (pass --force to overwrite)",
                eo.hash_hex,
                data_path.display()
            );
        }
        std::fs::remove_file(source)
            .map_err(|e| anyhow!("remove moved source {}: {e}", source.display()))?;
    }

    ensure_obao4(&shard_dir, &obao4_path, &eo.outboard)?;

    Ok(ImportedBlob {
        hash_hex: eo.hash_hex,
        size,
    })
}

/// Import an in-memory blob (the bundle manifest) into the origin `base`. Always
/// a copy — there is no source file to move.
fn import_bytes(base: &Path, bytes: &[u8], force: bool) -> anyhow::Result<ImportedBlob> {
    let size = u64::try_from(bytes.len()).map_err(|_| anyhow!("blob length exceeds u64"))?;
    let eo = encode_outboard(bytes, size).map_err(|e| anyhow!("encode outboard for blob: {e}"))?;

    let (shard_dir, data_path, obao4_path) = object_paths(base, &eo);
    std::fs::create_dir_all(&shard_dir)
        .map_err(|e| anyhow!("create shard dir {}: {e}", shard_dir.display()))?;

    if data_slot_needs_write(&data_path, size, force, &eo.hash_hex)? {
        let mut tmp = tempfile::NamedTempFile::new_in(&shard_dir)
            .map_err(|e| anyhow!("stage temp data object in {}: {e}", shard_dir.display()))?;
        tmp.write_all(bytes)
            .map_err(|e| anyhow!("write data object: {e}"))?;
        tmp.as_file()
            .sync_all()
            .map_err(|e| anyhow!("sync data object: {e}"))?;
        tmp.persist(&data_path)
            .map_err(|e| anyhow!("persist data object {}: {}", data_path.display(), e.error))?;
    }

    ensure_obao4(&shard_dir, &obao4_path, &eo.outboard)?;
    Ok(ImportedBlob {
        hash_hex: eo.hash_hex,
        size,
    })
}

/// Build the `(shard_dir, data_path, obao4_path)` triple for a blob under `base`.
fn object_paths(base: &Path, eo: &EncodedOutboard) -> (PathBuf, PathBuf, PathBuf) {
    let shard_dir = base.join(eo.shard());
    let data_path = shard_dir.join(&eo.hash_hex);
    let obao4_path = shard_dir.join(eo.obao4_name());
    (shard_dir, data_path, obao4_path)
}

/// Decide whether the data object at `data_path` must be written. Returns
/// `false` (a no-op) when a correct object is already present; `true` when it is
/// absent or `--force` was passed. A present object whose size differs from the
/// incoming blob is refused unless `--force` — a content-addressed path should
/// never hold different bytes, so a size mismatch signals a corrupt/foreign
/// store the operator must resolve deliberately.
fn data_slot_needs_write(
    data_path: &Path,
    size: u64,
    force: bool,
    hex: &str,
) -> anyhow::Result<bool> {
    match std::fs::metadata(data_path) {
        Ok(_) if force => Ok(true),
        Ok(meta) => {
            if meta.len() != size {
                bail!(
                    "origin object {hex} already exists at {} with a different size \
                     ({} vs {size}); pass --force to overwrite",
                    data_path.display(),
                    meta.len()
                );
            }
            Ok(false)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(anyhow!("stat {}: {e}", data_path.display())),
    }
}

/// Ensure the `{hex}.obao4` outboard sibling holds exactly `outboard`. Writes it
/// when missing, and replaces it when a present sibling's bytes differ — a stale
/// or foreign outboard from a partial prior import must not be left in place,
/// because the node rejects a mismatching `.obao4` and drops to whole-blob range
/// serving. A byte-identical present sibling is left untouched, so a clean
/// re-import stays a no-op. The outboard is small (~1/256 of the blob), so the
/// read-and-compare is cheap.
fn ensure_obao4(shard_dir: &Path, obao4_path: &Path, outboard: &[u8]) -> anyhow::Result<()> {
    match std::fs::read(obao4_path) {
        // Present and already correct — nothing to do.
        Ok(existing) if existing == outboard => return Ok(()),
        // Present but stale/foreign — fall through and replace atomically.
        Ok(_) => {}
        // Missing — fall through and write.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(anyhow!("read outboard {}: {e}", obao4_path.display())),
    }
    let mut tmp = tempfile::NamedTempFile::new_in(shard_dir)
        .map_err(|e| anyhow!("stage temp outboard in {}: {e}", shard_dir.display()))?;
    tmp.write_all(outboard)
        .map_err(|e| anyhow!("write outboard: {e}"))?;
    tmp.as_file()
        .sync_all()
        .map_err(|e| anyhow!("sync outboard: {e}"))?;
    tmp.persist(obao4_path)
        .map_err(|e| anyhow!("persist outboard {}: {}", obao4_path.display(), e.error))?;
    Ok(())
}

/// Move `source` to `data_path`, falling back to copy-then-unlink when the two
/// live on different filesystems (`rename(2)` returns `EXDEV`). The copy lands
/// in a staged temp under `base` first so a partial copy never appears at the
/// content-addressed path.
fn move_into_place(source: &Path, base: &Path, data_path: &Path) -> anyhow::Result<()> {
    match std::fs::rename(source, data_path) {
        Ok(()) => Ok(()),
        Err(e) if is_cross_device(&e) => {
            let tmp = tempfile::NamedTempFile::new_in(base)
                .map_err(|e| anyhow!("stage cross-device copy in {}: {e}", base.display()))?;
            std::fs::copy(source, tmp.path())
                .map_err(|e| anyhow!("copy {} across filesystems: {e}", source.display()))?;
            tmp.as_file()
                .sync_all()
                .map_err(|e| anyhow!("sync cross-device copy: {e}"))?;
            tmp.persist(data_path)
                .map_err(|e| anyhow!("persist data object {}: {}", data_path.display(), e.error))?;
            std::fs::remove_file(source)
                .map_err(|e| anyhow!("remove moved source {}: {e}", source.display()))?;
            Ok(())
        }
        Err(e) => Err(anyhow!(
            "move {} into {}: {e}",
            source.display(),
            data_path.display()
        )),
    }
}

/// Whether an `io::Error` is `EXDEV` (cross-device link), the one `rename`
/// failure that a copy-then-unlink can recover. `raw_os_error` is checked
/// directly because `ErrorKind` has no stable `CrossesDevices` on the MSRV.
fn is_cross_device(err: &std::io::Error) -> bool {
    // 18 == EXDEV on Linux and macOS.
    err.raw_os_error() == Some(18)
}

/// A `Read` wrapper that copies every byte it yields into `sink` — used to
/// compute the outboard and write the staged data object in one read pass.
struct TeeReader<R, W> {
    inner: R,
    sink: W,
}

impl<R: Read, W: Write> TeeReader<R, W> {
    const fn new(inner: R, sink: W) -> Self {
        Self { inner, sink }
    }
}

impl<R: Read, W: Write> Read for TeeReader<R, W> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if let Some(chunk) = buf.get(..n) {
            self.sink.write_all(chunk)?;
        }
        Ok(n)
    }
}

/// The sole blob's content hash when the report describes a single-file import
/// (no manifest, exactly one entry), else `None` — used for the concise human
/// line, where a directory's full file map would be noise.
fn single_file_hash(report: &ImportReport) -> Option<&str> {
    if report.bundle_hash.is_some() {
        return None;
    }
    let mut it = report.files.values();
    match (it.next(), it.next()) {
        (Some(h), None) => Some(h.as_str()),
        _ => None,
    }
}

fn write_import_report(
    w: &mut impl Write,
    report: &ImportReport,
    json: bool,
) -> std::io::Result<()> {
    if json {
        let line = serde_json::to_string(report)
            .map_err(|e| std::io::Error::other(format!("serialize status report: {e}")))?;
        writeln!(w, "{line}")
    } else {
        let verb = if report.moved { "moved" } else { "imported" };
        write!(
            w,
            "{verb} {} file(s), {} bytes, into {}",
            report.imported, report.bytes, report.origin
        )?;
        // A directory import is addressed by its bundle hash; a single-file
        // import has no manifest, so surface the one blob's content hash. The
        // full file→hash map is reserved for `--json`.
        match (&report.bundle_hash, single_file_hash(report)) {
            (Some(b), _) => writeln!(w, " (bundle {b})"),
            (None, Some(h)) => writeln!(w, " ({h})"),
            (None, None) => writeln!(w),
        }
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

    #[test]
    fn parse_target_accepts_fs() {
        match parse_target("fs:/var/lib/decdn/origin").unwrap() {
            ImportTarget::Fs(p) => assert_eq!(p, PathBuf::from("/var/lib/decdn/origin")),
        }
    }

    #[test]
    fn parse_target_rejects_empty_fs_dir() {
        let err = parse_target("fs:").unwrap_err();
        assert!(format!("{err:#}").contains("needs a directory"));
    }

    #[test]
    fn parse_target_s3_is_not_yet_implemented() {
        let err = parse_target("s3://bucket/prefix").unwrap_err();
        assert!(format!("{err:#}").contains("not yet implemented"));
    }

    #[test]
    fn parse_target_http_is_not_a_write_target() {
        let err = parse_target("https://example.com").unwrap_err();
        assert!(format!("{err:#}").contains("not a write target"));
    }

    #[test]
    fn parse_target_rejects_unknown_scheme() {
        let err = parse_target("/plain/path").unwrap_err();
        assert!(format!("{err:#}").contains("not a recognized target"));
    }

    #[test]
    fn import_report_json_shape_directory() {
        let report = ImportReport {
            imported: 3,
            bytes: 42,
            origin: "fs:/tmp/origin".into(),
            files: BTreeMap::from([
                ("a.txt".into(), "b3:aaaa".into()),
                ("dir/b.txt".into(), "b3:bbbb".into()),
            ]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(buf.trim_ascii_end()).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 6);
        assert_eq!(obj["imported"].as_u64(), Some(3));
        assert_eq!(obj["bytes"].as_u64(), Some(42));
        assert_eq!(obj["origin"].as_str(), Some("fs:/tmp/origin"));
        assert_eq!(obj["files"]["a.txt"].as_str(), Some("b3:aaaa"));
        assert_eq!(obj["files"]["dir/b.txt"].as_str(), Some("b3:bbbb"));
        assert_eq!(obj["bundle_hash"].as_str(), Some("b3:cafef00d"));
        assert_eq!(obj["moved"].as_bool(), Some(false));
    }

    #[test]
    fn import_report_json_single_file_maps_name_to_hash_null_bundle_hash() {
        let report = ImportReport {
            imported: 1,
            bytes: 10,
            origin: "fs:/tmp/origin".into(),
            files: BTreeMap::from([("blob.bin".into(), "b3:deadbeef".into())]),
            bundle_hash: None,
            moved: true,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(buf.trim_ascii_end()).unwrap();
        assert_eq!(parsed["files"]["blob.bin"].as_str(), Some("b3:deadbeef"));
        assert!(parsed["bundle_hash"].is_null());
        assert_eq!(parsed["moved"].as_bool(), Some(true));
    }

    #[test]
    fn import_report_human_single_file_shows_hash() {
        let report = ImportReport {
            imported: 1,
            bytes: 10,
            origin: "fs:/tmp/origin".into(),
            files: BTreeMap::from([("blob.bin".into(), "b3:deadbeef".into())]),
            bundle_hash: None,
            moved: false,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, false).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("(b3:deadbeef)"), "got: {line}");
    }

    #[test]
    fn import_report_human_directory_shows_bundle_not_file_map() {
        let report = ImportReport {
            imported: 2,
            bytes: 20,
            origin: "fs:/tmp/origin".into(),
            files: BTreeMap::from([
                ("a.txt".into(), "b3:aaaa".into()),
                ("b.txt".into(), "b3:bbbb".into()),
            ]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, false).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("(bundle b3:cafef00d)"), "got: {line}");
        assert!(
            !line.contains("b3:aaaa"),
            "file map must stay out of human line: {line}"
        );
    }
}
