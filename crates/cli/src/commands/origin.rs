//! `decdn origin import` — seed a cache origin store with local content
//! (issue #1904).
//!
//! For every blob it writes the sharded data object `{base}/{hex[0..2]}/{hex}`
//! and its sibling pre-order bao outboard `{hex}.obao4` — the exact layout the
//! daemon reads back (ADR 037 §Origin-tier pull-through).
//!
//! `--to` is a local filesystem directory: the store is written to disk and
//! nowhere else. Populating an S3 origin needs no separate writer, because the
//! fs and S3 object layouts are byte-identical (the S3 reader keys objects
//! `{prefix}{hex[0..2]}/{hex}` to match this on-disk shard) — import to a local
//! directory and `aws s3 sync` it up. `parse_target` turns an `s3://` (or
//! `http(s)://`) target into an error that spells out that recipe rather than
//! writing to a directory named after the URL.
//!
//! The work is synchronous (bao encoding + filesystem writes are sync), wrapped
//! once in `tokio::task::spawn_blocking` from the async entry point so the CLI's
//! runtime isn't held up.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail};
use decdn_bao_range::{EncodedOutboard, encode_outboard, shard_prefix};
use decdn_common::cli::{OriginArgs, OriginCommand, OriginImportArgs};
use serde::Serialize;

use super::chunker::{ChunkSizes, chunk_file};
use super::manifest::{
    BundleEntry, Chunk, b3_hex_str, build_excluder, hash_file_at, serialize_canonical,
    validate_relpath, walk_and_collect, write_bundle,
};

/// Dispatcher for `decdn origin ...`. Every subcommand is offline, config-free
/// local work, so no `config_path` is threaded through.
pub async fn origin_dispatch(args: &OriginArgs) -> anyhow::Result<()> {
    match &args.cmd {
        OriginCommand::Import(import_args) => origin_import(import_args).await,
    }
}

/// The parsed `--to` target. The filesystem is the only write backend, so this
/// is a one-variant enum — it stays an enum so the write dispatch reads as a
/// `match` and a future backend is an added arm, not a rewrite.
#[derive(Debug)]
enum ImportTarget {
    /// A local filesystem origin store rooted at this directory.
    Fs(PathBuf),
}

/// Parse a `--to <dir>` target. The target is a local filesystem directory —
/// the default and only write backend. An `s3://` or `http(s)://` target is not
/// a write target and produces an error that spells out the supported path; a
/// legacy `fs:` prefix is rejected with a hint to drop it, so an operator's
/// muscle memory does not silently create a directory literally named `fs:…`.
///
/// The scheme checks run on the path's UTF-8 view. A non-UTF-8 path (valid on
/// Unix) can be none of those ASCII schemes, so it falls straight through to
/// the filesystem arm rather than being rejected for not being UTF-8.
fn parse_target(raw: &Path) -> anyhow::Result<ImportTarget> {
    if let Some(s) = raw.to_str() {
        if s.is_empty() {
            bail!("--to needs a directory, e.g. --to /var/lib/decdn/origin");
        }
        if s.starts_with("s3://") {
            bail!(
                "--to {s} is not a write target: `origin import` writes a local \
                 filesystem store only. The fs and S3 object layouts are identical, so \
                 import to a directory and sync it up:\n  \
                 decdn origin import -i <input> --to <dir>\n  \
                 aws s3 sync <dir> {s}"
            );
        }
        if s.starts_with("http://") || s.starts_with("https://") {
            bail!(
                "--to {s} is not a write target: an HTTP origin is a read-only static \
                 server — seed a local directory and serve it from that disk instead"
            );
        }
        if s.strip_prefix("fs:").is_some() {
            bail!(
                "--to no longer takes an `fs:` prefix — pass the directory path \
                 directly, e.g. --to /var/lib/decdn/origin"
            );
        }
    }
    Ok(ImportTarget::Fs(raw.to_path_buf()))
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
    /// Whether sources were moved (`--move`) rather than copied. Always
    /// `false` under `--dry-run`, since nothing is written; `--move` is
    /// rejected together with `--optimize`, so this is never `true` for an
    /// optimized import.
    moved: bool,
    /// Whether content-defined chunking (`--optimize`) was applied — each file
    /// is still stored as one whole-file blob, but the manifest also carries
    /// its chunk decomposition as dedup hints.
    optimized: bool,
    /// Chunk hint count across every file (0 unless `--optimize`). Each file is
    /// stored as one whole-file blob; the chunks are manifest-only dedup hints,
    /// not separately stored blobs. Equals the sum of every entry's chunk count.
    chunks_total: u64,
    /// Count of symlinks skipped during a directory walk (0 for a single-file
    /// import, and only non-zero without `--follow-symlinks`).
    skipped_symlinks: u64,
}

/// The resolved import context, threaded through the sync import paths. It
/// captures the two axes that steer every write decision — `write` (whether
/// blobs land on disk at all, false under `--dry-run`) and `optimize` (whether
/// files are content-defined-chunked) — plus the shared flags and the resolved
/// filesystem `base` (`Some` exactly when `write`).
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent per-invocation toggles resolved from CLI flags, not modelled state"
)]
struct ImportCtx {
    /// The origin store root the blobs are written under, `Some` iff `write`.
    base: Option<PathBuf>,
    /// Whether blobs are written. False under `--dry-run` (hash + manifest only).
    write: bool,
    /// Whether the status report and manifest bytes route for a dry run:
    /// manifest to stdout, status to stderr.
    dry_run: bool,
    /// Move sources into the store instead of copying (`--move`).
    move_source: bool,
    /// Overwrite an object already present at its hash (`--force`).
    force: bool,
    /// Also write the canonical manifest to this file (`--bundle`).
    bundle_out: Option<PathBuf>,
    /// The `--to` target string surfaced in the report (a placeholder in a
    /// no-target dry run).
    origin_label: String,
    /// Content-define-chunk each file and record its chunk decomposition as
    /// manifest dedup hints, on top of the usual whole-file blob (`--optimize`).
    optimize: bool,
    /// The validated chunk-size triple, `Some` iff `optimize`.
    sizes: Option<ChunkSizes>,
    /// The validated `--subfolder` prefix (a relative POSIX path, no trailing
    /// slash), prepended to every manifest entry's path so a pull lands the
    /// whole bundle under one directory. `None` when the flag is absent.
    subfolder: Option<String>,
}

/// Validate `--subfolder` into the normalized prefix stored in [`ImportCtx`].
/// Reuses the manifest path-safety rules ([`validate_relpath`]): a relative
/// POSIX path with only `Normal` components — `..`, absolute paths, and root
/// prefixes are rejected — and normalizes a trailing slash away, so `assets/`
/// and `assets` both yield the `assets` prefix.
///
/// A backslash is rejected before that check. `\` is not a POSIX separator, so
/// on Unix `a\..\evil` collapses to one `Normal` component and slips past the
/// `..` check — yet a Windows `bundle pull` parses `\` as a separator in
/// `safe_join`, turning that same manifest path into a parent-dir escape.
/// Requiring `/` keeps the published manifest identical and safe on every
/// platform.
fn resolve_subfolder(raw: Option<&str>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    if raw.contains('\\') {
        bail!(
            "invalid --subfolder {raw:?}: use '/' as the path separator; \
             '\\' is not allowed (a POSIX-relative subfolder keeps the \
             manifest identical and safe across platforms)"
        );
    }
    let normalized = validate_relpath(Path::new(raw))
        .map_err(|e| anyhow!("invalid --subfolder {raw:?}: {e}"))?;
    Ok(Some(normalized))
}

/// Place a bundle-relative path under the `--subfolder` prefix when one is set,
/// so a pull writes the file to `<out>/<subfolder>/<rel>`. `prefix` is already
/// validated to a relative POSIX path, and `rel` is a validated relative POSIX
/// path, so the join stays all-`Normal`-component and re-validatable.
fn place_under(prefix: Option<&str>, rel: &str) -> String {
    match prefix {
        Some(sub) => format!("{sub}/{rel}"),
        None => rel.to_string(),
    }
}

/// Entry point. Resolves the target and chunk options, dispatches file vs
/// directory import inside `spawn_blocking`, and routes the status report —
/// stdout normally, stderr under `--dry-run` (where stdout carries the manifest).
pub async fn origin_import(args: &OriginImportArgs) -> anyhow::Result<()> {
    // `--to` is required unless `--dry-run`; a dry run needs no target because it
    // writes no blobs.
    let target = match (&args.to, args.dry_run) {
        (Some(t), _) => Some(parse_target(t)?),
        (None, true) => None,
        (None, false) => bail!("--to <target> is required unless --dry-run"),
    };
    // The chunk-size flags only mean something with `--optimize`; reject them
    // otherwise so a typo like `--chunk-avg` without `--optimize` is not silently
    // ignored.
    if !args.optimize
        && (args.chunk_avg.is_some() || args.chunk_min.is_some() || args.chunk_max.is_some())
    {
        bail!("--chunk-avg/--chunk-min/--chunk-max require --optimize");
    }
    // The 4 MiB avg default lives here, not in clap, so an explicit `--chunk-avg`
    // without `--optimize` stays detectable as `Some` above.
    let avg = args.chunk_avg.unwrap_or(4 * 1024 * 1024);
    let sizes = if args.optimize {
        Some(ChunkSizes::resolve(avg, args.chunk_min, args.chunk_max)?)
    } else {
        None
    };

    let subfolder = resolve_subfolder(args.subfolder.as_deref())?;

    let write = !args.dry_run;
    // `--to` is a path; the report's `origin` field is a display string, so a
    // non-UTF-8 target degrades lossily here — only for the human/JSON label,
    // never for the actual write path, which keeps the raw `PathBuf`.
    let origin_label = args
        .to
        .as_ref()
        .map_or_else(|| "(dry-run)".to_string(), |p| p.display().to_string());

    let input = args.input.clone();
    let follow = args.follow_symlinks;
    let exclude = args.exclude.clone();
    let json = args.json;
    let dry_run = args.dry_run;

    let ctx = ImportCtx {
        base: None,
        write,
        dry_run,
        move_source: args.move_source,
        force: args.force,
        bundle_out: args.bundle.clone(),
        origin_label,
        optimize: args.optimize,
        sizes,
        subfolder,
    };

    let report = tokio::task::spawn_blocking(move || -> anyhow::Result<ImportReport> {
        let mut ctx = ctx;
        // Create + canonicalize the origin root only when writing; a dry run
        // must never touch the filesystem target.
        if ctx.write {
            let ImportTarget::Fs(base) =
                target.ok_or_else(|| anyhow!("internal: write without a --to target"))?;
            std::fs::create_dir_all(&base)
                .map_err(|e| anyhow!("create origin dir {}: {e}", base.display()))?;
            let base = std::fs::canonicalize(&base)
                .map_err(|e| anyhow!("canonicalize origin dir {}: {e}", base.display()))?;
            ctx.base = Some(base);
        }

        let meta = std::fs::symlink_metadata(&input)
            .map_err(|e| anyhow!("--input {}: {e}", input.display()))?;

        if meta.is_dir() {
            import_directory(&ctx, &input, follow, &exclude)
        } else if meta.is_file() {
            if ctx.optimize {
                import_single_optimized(&ctx, &input)
            } else {
                import_single_plain(&ctx, &input)
            }
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

    // The manifest bytes already went to stdout inside `emit_manifest` during a
    // dry run; the status report follows on stderr so stdout is exactly the
    // manifest. A normal import prints the status to stdout.
    if dry_run {
        let mut stderr = std::io::stderr().lock();
        write_import_report(&mut stderr, &report, json)
            .map_err(|e| anyhow!("failed to write status report: {e}"))?;
    } else {
        let mut stdout = std::io::stdout().lock();
        write_import_report(&mut stdout, &report, json)
            .map_err(|e| anyhow!("failed to write status report: {e}"))?;
    }
    Ok(())
}

/// Import a whole directory tree and emit its canonical bundle manifest. Every
/// regular file is stored as one whole-file blob, plain or `--optimize` alike;
/// `--optimize` additionally content-defined-chunks each file and records the
/// chunk list in the manifest as dedup hints — hints only, never separately
/// stored blobs. Either way the manifest bytes are the same canonical bytes
/// `--dry-run` would print, so the tree is retrievable by the reported bundle
/// hash.
fn import_directory(
    ctx: &ImportCtx,
    input: &Path,
    follow: bool,
    exclude: &[String],
) -> anyhow::Result<ImportReport> {
    let excluder = build_excluder(exclude)?;
    let root = std::fs::canonicalize(input)
        .map_err(|e| anyhow!("canonicalize --input {}: {e}", input.display()))?;

    if ctx.optimize {
        let sizes = ctx
            .sizes
            .as_ref()
            .ok_or_else(|| anyhow!("internal: optimize without chunk sizes"))?;
        let mut chunks_total: u64 = 0;
        let collected = walk_and_collect(&root, follow, &excluder, |canonical, _rel| {
            let (hash_hex, size, chunks) = import_optimized_file(ctx, canonical, sizes)?;
            let n = u64::try_from(chunks.len())
                .map_err(|_| anyhow!("chunk count {} exceeds u64", chunks.len()))?;
            chunks_total = chunks_total
                .checked_add(n)
                .ok_or_else(|| anyhow!("chunk count overflow"))?;
            Ok((b3_hex_str_from_hex(&hash_hex), size, Some(chunks)))
        })?;
        emit_manifest(
            ctx,
            collected.entries,
            collected.total_size,
            true,
            chunks_total,
            collected.skipped_symlinks,
        )
    } else {
        let collected = walk_and_collect(&root, follow, &excluder, |canonical, _rel| {
            if ctx.write {
                let base = ctx
                    .base
                    .as_deref()
                    .ok_or_else(|| anyhow!("internal: write without a base"))?;
                let blob = import_one_file(base, canonical, ctx.move_source, ctx.force)?;
                Ok((b3_hex_str_from_hex(&blob.hash_hex), blob.size, None))
            } else {
                let (h, s) = hash_file_at(canonical)
                    .map_err(|e| anyhow!("hash {}: {e}", canonical.display()))?;
                Ok((b3_hex_str(h), s, None))
            }
        })?;
        emit_manifest(
            ctx,
            collected.entries,
            collected.total_size,
            false,
            0,
            collected.skipped_symlinks,
        )
    }
}

/// Import one `--optimize` file: store its whole-file blob + `.obao4` outboard
/// exactly like a plain import (`ctx.write` gates the write, same as
/// `import_one_file`/`hash_file_at` elsewhere), then compute its chunk hints
/// with a second, no-op-sink streaming pass over the same bytes. Returns the
/// whole-file `(hex hash, size, chunk hints)`.
///
/// The chunk pass reads the just-written **stored object** at its
/// content-addressed path, not the source. The store copy always exists after
/// `import_one_file` — a `--move` renames the source into the store, a copy
/// stages it there — so `--optimize` composes with `--move` for free, and
/// chunking the immutable stored bytes rules out any read race between the two
/// passes. Re-chunking those bytes must reproduce the whole-file hash and size;
/// a mismatch would mean the stored object was corrupted between the write and
/// the chunk pass, and is reported rather than emitting hints that don't
/// describe the stored blob.
fn import_optimized_file(
    ctx: &ImportCtx,
    path: &Path,
    sizes: &ChunkSizes,
) -> anyhow::Result<(String, u64, Vec<Chunk>)> {
    if ctx.write {
        let base = ctx
            .base
            .as_deref()
            .ok_or_else(|| anyhow!("internal: write without a base"))?;
        let blob = import_one_file(base, path, ctx.move_source, ctx.force)?;

        let stored = base.join(shard_prefix(&blob.hash_hex)).join(&blob.hash_hex);
        let file = File::open(&stored)
            .map_err(|e| anyhow!("open stored object {}: {e}", stored.display()))?;
        let cf = chunk_file(file, sizes, |_chash, _data| Ok(()))?;

        if cf.whole_hash.to_hex().as_str() != blob.hash_hex || cf.total_size != blob.size {
            bail!(
                "stored object {} is corrupt: whole-file pass wrote {} bytes at {}, \
                 chunk pass read {} bytes at {}",
                stored.display(),
                blob.size,
                blob.hash_hex,
                cf.total_size,
                cf.whole_hash.to_hex()
            );
        }
        Ok((blob.hash_hex, blob.size, cf.chunks))
    } else {
        // A dry run writes nothing, so one streaming pass suffices for both the
        // whole-file hash and the chunk hints.
        let file = File::open(path).map_err(|e| anyhow!("open {}: {e}", path.display()))?;
        let cf = chunk_file(file, sizes, |_chash, _data| Ok(()))?;
        Ok((cf.whole_hash.to_hex().to_string(), cf.total_size, cf.chunks))
    }
}

/// Import a single file with `--optimize`: store its whole-file blob (exactly
/// like a plain import) plus a manifest chunk-hint list, and emit a one-entry
/// manifest keyed by the file's own name.
fn import_single_optimized(ctx: &ImportCtx, input: &Path) -> anyhow::Result<ImportReport> {
    let sizes = ctx
        .sizes
        .as_ref()
        .ok_or_else(|| anyhow!("internal: optimize without chunk sizes"))?;
    let (hash_hex, total_size, chunks) = import_optimized_file(ctx, input, sizes)?;

    // Key the one entry by the file's own name, validated to the same POSIX
    // path rules a directory entry obeys.
    let name = input
        .file_name()
        .ok_or_else(|| anyhow!("--input {} has no file name", input.display()))?;
    let path = validate_relpath(Path::new(name))?;
    let chunks_total =
        u64::try_from(chunks.len()).map_err(|_| anyhow!("chunk count exceeds u64"))?;
    let entry = BundleEntry {
        path,
        hash: b3_hex_str_from_hex(&hash_hex),
        size: total_size,
        chunks: Some(chunks),
    };
    emit_manifest(ctx, vec![entry], total_size, true, chunks_total, 0)
}

/// Import a single file without optimization: one whole-file blob, no manifest.
/// Honors `--dry-run` (hash only, no write); the report keys the file by its own
/// name and carries the one blob's content hash.
fn import_single_plain(ctx: &ImportCtx, input: &Path) -> anyhow::Result<ImportReport> {
    let (hash_hex, size) = if ctx.write {
        let base = ctx
            .base
            .as_deref()
            .ok_or_else(|| anyhow!("internal: write without a base"))?;
        let blob = import_one_file(base, input, ctx.move_source, ctx.force)?;
        (blob.hash_hex, blob.size)
    } else {
        let (h, s) = hash_file_at(input).map_err(|e| anyhow!("hash {}: {e}", input.display()))?;
        (h.to_hex().to_string(), s)
    };
    // Key the single entry by the file's own name — never the absolute or
    // working-directory-relative path the operator typed.
    let name = input.file_name().map_or_else(
        || input.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let files = BTreeMap::from([(
        place_under(ctx.subfolder.as_deref(), &name),
        b3_hex_str_from_hex(&hash_hex),
    )]);
    Ok(ImportReport {
        imported: 1,
        bytes: size,
        origin: ctx.origin_label.clone(),
        files,
        bundle_hash: None,
        // A dry run writes nothing, so it never actually moved the source —
        // even though `--move` was passed, don't claim it happened.
        moved: ctx.move_source && ctx.write,
        optimized: false,
        chunks_total: 0,
        skipped_symlinks: 0,
    })
}

/// Shared manifest tail for every path that emits a bundle (a directory import
/// and a single-file `--optimize`). Serializes the canonical manifest, then —
/// gated on the context — imports the manifest blob (`write`), writes the
/// `--bundle` file, and prints the exact canonical bytes to stdout (`--dry-run`,
/// no trailing newline, so stdout equals the `--bundle` file equals the bundle
/// hash preimage). Returns the assembled report; its `bundle_hash` is the
/// manifest's own content address whether or not the blob was written.
fn emit_manifest(
    ctx: &ImportCtx,
    mut entries: Vec<BundleEntry>,
    total_size: u64,
    optimized: bool,
    chunks_total: u64,
    skipped_symlinks: u64,
) -> anyhow::Result<ImportReport> {
    // Prefix every entry's path with `--subfolder` before serializing, so the
    // bundle hash and every downstream pull place the tree under that folder.
    // The prefix is uniform, so it preserves the by-path-bytes order the walk
    // already imposed — no re-sort needed.
    if let Some(sub) = ctx.subfolder.as_deref() {
        for entry in &mut entries {
            entry.path = place_under(Some(sub), &entry.path);
        }
    }
    let bundle_bytes = serialize_canonical(&entries)?;

    if ctx.write
        && let Some(base) = ctx.base.as_deref()
    {
        // Import the manifest blob itself so the whole tree is retrievable by
        // one hash. The manifest is in-memory bytes, always copied.
        import_bytes(base, &bundle_bytes, ctx.force)?;
    }
    if let Some(out) = ctx.bundle_out.as_deref() {
        write_bundle(out, &bundle_bytes)
            .map_err(|e| anyhow!("write --bundle {}: {e}", out.display()))?;
    }
    if ctx.dry_run {
        // The canonical bytes are the sole stdout payload of a dry run.
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(&bundle_bytes)
            .map_err(|e| anyhow!("write manifest to stdout: {e}"))?;
    }

    let bundle_hash = Some(b3_hex_str(blake3::hash(&bundle_bytes)));
    let imported = u64::try_from(entries.len())
        .map_err(|_| anyhow!("entry count {} exceeds u64", entries.len()))?;
    // Each entry's `path` is the validated POSIX path and its `hash` the
    // `b3:<hex>` address — exactly the file→hash map the report surfaces.
    let files = entries.into_iter().map(|e| (e.path, e.hash)).collect();
    Ok(ImportReport {
        imported,
        bytes: total_size,
        origin: ctx.origin_label.clone(),
        files,
        bundle_hash,
        // A dry run writes nothing, so it never actually moved a source;
        // gating on `ctx.write` keeps this line self-evidently truthful.
        moved: ctx.move_source && ctx.write,
        optimized,
        chunks_total,
        skipped_symlinks,
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
        if report.optimized {
            write!(w, ", {} chunk hint(s)", report.chunks_total)?;
        }
        // A directory import is addressed by its bundle hash; a single-file
        // import has no manifest, so surface the one blob's content hash. The
        // full file→hash map is reserved for `--json`.
        match (&report.bundle_hash, single_file_hash(report)) {
            (Some(b), _) => writeln!(w, " (bundle {b})")?,
            (None, Some(h)) => writeln!(w, " ({h})")?,
            (None, None) => writeln!(w)?,
        }
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

    #[test]
    fn resolve_subfolder_none_is_none() {
        assert_eq!(resolve_subfolder(None).unwrap(), None);
    }

    #[test]
    fn resolve_subfolder_normalizes_trailing_slash() {
        assert_eq!(
            resolve_subfolder(Some("a/b/")).unwrap(),
            Some("a/b".to_string())
        );
    }

    #[test]
    fn resolve_subfolder_rejects_parent_dir() {
        let err = resolve_subfolder(Some("../evil")).unwrap_err();
        assert!(format!("{err:#}").contains("parent-dir"));
    }

    #[test]
    fn resolve_subfolder_rejects_backslash() {
        let err = resolve_subfolder(Some("a\\..\\evil")).unwrap_err();
        assert!(format!("{err:#}").contains("separator"));
    }

    #[test]
    fn place_under_prefixes_when_set() {
        assert_eq!(place_under(Some("assets"), "a/b.txt"), "assets/a/b.txt");
        assert_eq!(place_under(Some("a/b"), "c.txt"), "a/b/c.txt");
    }

    #[test]
    fn place_under_is_identity_when_absent() {
        assert_eq!(place_under(None, "a/b.txt"), "a/b.txt");
    }

    #[test]
    fn parse_target_accepts_bare_path() {
        match parse_target(Path::new("/var/lib/decdn/origin")).unwrap() {
            ImportTarget::Fs(p) => assert_eq!(p, PathBuf::from("/var/lib/decdn/origin")),
        }
    }

    #[test]
    fn parse_target_accepts_relative_bare_path() {
        // A relative directory is a filesystem path like any other.
        match parse_target(Path::new("origin")).unwrap() {
            ImportTarget::Fs(p) => assert_eq!(p, PathBuf::from("origin")),
        }
    }

    #[test]
    fn parse_target_rejects_empty() {
        let err = parse_target(Path::new("")).unwrap_err();
        assert!(format!("{err:#}").contains("needs a directory"));
    }

    #[test]
    fn parse_target_s3_points_at_aws_sync() {
        // The S3 target is not a writer; the error must hand the operator the
        // exact import-then-sync recipe, echoing the requested s3:// URL.
        let err = parse_target(Path::new("s3://bucket/prefix")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("aws s3 sync"), "got: {msg}");
        assert!(msg.contains("s3://bucket/prefix"), "got: {msg}");
    }

    #[test]
    fn parse_target_http_is_not_a_write_target() {
        let err = parse_target(Path::new("https://example.com")).unwrap_err();
        assert!(format!("{err:#}").contains("not a write target"));
    }

    #[test]
    fn parse_target_rejects_legacy_fs_prefix() {
        // `fs:` is gone; rejecting it (rather than treating it as a path) keeps
        // an operator from silently seeding a directory named `fs:...`.
        let err = parse_target(Path::new("fs:/var/lib/decdn/origin")).unwrap_err();
        assert!(format!("{err:#}").contains("no longer takes an `fs:` prefix"));
    }

    #[test]
    fn parse_target_non_utf8_path_is_filesystem() {
        // A non-UTF-8 Unix path can be none of the ASCII schemes, so it must
        // fall through to the filesystem arm rather than erroring.
        #[cfg(unix)]
        {
            use std::ffi::OsStr;
            use std::os::unix::ffi::OsStrExt;
            let raw = OsStr::from_bytes(b"/var/lib/decdn/\xff\xfeorigin");
            match parse_target(Path::new(raw)).unwrap() {
                ImportTarget::Fs(p) => assert_eq!(p.as_os_str(), raw),
            }
        }
    }

    #[test]
    fn import_report_json_shape_directory() {
        let report = ImportReport {
            imported: 3,
            bytes: 42,
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([
                ("a.txt".into(), "b3:aaaa".into()),
                ("dir/b.txt".into(), "b3:bbbb".into()),
            ]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
            optimized: false,
            chunks_total: 0,
            skipped_symlinks: 0,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(buf.trim_ascii_end()).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 9);
        assert_eq!(obj["imported"].as_u64(), Some(3));
        assert_eq!(obj["bytes"].as_u64(), Some(42));
        assert_eq!(obj["origin"].as_str(), Some("/tmp/origin"));
        assert_eq!(obj["files"]["a.txt"].as_str(), Some("b3:aaaa"));
        assert_eq!(obj["files"]["dir/b.txt"].as_str(), Some("b3:bbbb"));
        assert_eq!(obj["bundle_hash"].as_str(), Some("b3:cafef00d"));
        assert_eq!(obj["moved"].as_bool(), Some(false));
        assert_eq!(obj["optimized"].as_bool(), Some(false));
        assert_eq!(obj["chunks_total"].as_u64(), Some(0));
        assert_eq!(obj["skipped_symlinks"].as_u64(), Some(0));
    }

    #[test]
    fn import_report_human_shows_skipped_symlinks_warning() {
        let report = ImportReport {
            imported: 1,
            bytes: 10,
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([("a.txt".into(), "b3:aaaa".into())]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
            optimized: false,
            chunks_total: 0,
            skipped_symlinks: 2,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, false).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(
            line.contains("skipped 2 symlink(s); pass --follow-symlinks to include"),
            "got: {line}"
        );
    }

    #[test]
    fn import_report_json_carries_skipped_symlinks() {
        let report = ImportReport {
            imported: 1,
            bytes: 10,
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([("a.txt".into(), "b3:aaaa".into())]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
            optimized: false,
            chunks_total: 0,
            skipped_symlinks: 2,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(buf.trim_ascii_end()).unwrap();
        assert_eq!(parsed["skipped_symlinks"].as_u64(), Some(2));
    }

    #[test]
    fn import_report_json_optimized_carries_chunk_counter() {
        let report = ImportReport {
            imported: 2,
            bytes: 100,
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([("a.bin".into(), "b3:aaaa".into())]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
            optimized: true,
            chunks_total: 7,
            skipped_symlinks: 0,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(buf.trim_ascii_end()).unwrap();
        assert_eq!(parsed["optimized"].as_bool(), Some(true));
        assert_eq!(parsed["chunks_total"].as_u64(), Some(7));
    }

    #[test]
    fn import_report_human_optimized_shows_chunk_hint_count() {
        let report = ImportReport {
            imported: 2,
            bytes: 100,
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([("a.bin".into(), "b3:aaaa".into())]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
            optimized: true,
            chunks_total: 7,
            skipped_symlinks: 0,
        };
        let mut buf: Vec<u8> = Vec::new();
        write_import_report(&mut buf, &report, false).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert!(line.contains("7 chunk hint(s)"), "got: {line}");
    }

    #[test]
    fn import_report_json_single_file_maps_name_to_hash_null_bundle_hash() {
        let report = ImportReport {
            imported: 1,
            bytes: 10,
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([("blob.bin".into(), "b3:deadbeef".into())]),
            bundle_hash: None,
            moved: true,
            optimized: false,
            chunks_total: 0,
            skipped_symlinks: 0,
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
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([("blob.bin".into(), "b3:deadbeef".into())]),
            bundle_hash: None,
            moved: false,
            optimized: false,
            chunks_total: 0,
            skipped_symlinks: 0,
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
            origin: "/tmp/origin".into(),
            files: BTreeMap::from([
                ("a.txt".into(), "b3:aaaa".into()),
                ("b.txt".into(), "b3:bbbb".into()),
            ]),
            bundle_hash: Some("b3:cafef00d".into()),
            moved: false,
            optimized: false,
            chunks_total: 0,
            skipped_symlinks: 0,
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
