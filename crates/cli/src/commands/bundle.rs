//! `decdn bundle create` — walk a directory, BLAKE3-hash each regular
//! file, and emit a canonical JSON manifest. Issue #391.
//!
//! The on-disk schema, hash format (`b3:<hex>`), path-safety rules, and
//! the determinism contract that makes single-hash bundle distribution
//! viable are specified in
//! [`appendix-bundles`](../../../../adr/appendix-bundles.md). The walk,
//! path-safety rules, and canonical serializer live in [`super::manifest`],
//! shared with `decdn origin import` so both emit byte-identical manifests.
//! The walk is synchronous (walkdir + blake3 are sync), wrapped once in
//! `tokio::task::spawn_blocking` from the async entry point so the publisher
//! CLI's runtime isn't held up.

use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, bail};
use decdn_common::cli::{BundleArgs, BundleCommand, BundleCreateArgs};
use serde::Serialize;

use super::manifest::{
    b3_hex_str, build_excluder, hash_file_at, serialize_canonical, walk_and_collect, write_bundle,
};

/// Top-level dispatcher for `decdn bundle ...`. `config_path` (the global
/// `--config`) is only consumed by `pull` (relays/discovery/chain coordinates);
/// `create` is purely local.
pub async fn bundle_dispatch(args: &BundleArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    match &args.cmd {
        BundleCommand::Create(create_args) => bundle_create(create_args).await,
        BundleCommand::Pull(pull_args) => {
            super::bundle_pull::bundle_pull(pull_args, config_path).await
        }
    }
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
            .map_err(|e| anyhow!("canonicalize --input {}: {e}", input.display()))?;

        // The walker is rooted at the already-canonical path on purpose — every
        // produced `entry.path()` is then a descendant of `root`, and the
        // `starts_with(root)` invariant holds without an extra canonicalize step
        // on each entry's lexical prefix.
        let collected = walk_and_collect(&root, follow, &excluder, |canonical, _rel| {
            let (hash, size) = hash_file_at(canonical)?;
            Ok((b3_hex_str(hash), size, None))
        })?;
        let bundle_bytes = serialize_canonical(&collected.entries)?;
        let bundle_hash = blake3::hash(&bundle_bytes);

        write_bundle(&output, &bundle_bytes)
            .map_err(|e| anyhow!("write --output {}: {e}", output.display()))?;

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
        // JoinError fires on panic or cancellation — don't lie about which one
        // happened. Pattern mirrors `crates/cache/src/engine.rs`.
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
