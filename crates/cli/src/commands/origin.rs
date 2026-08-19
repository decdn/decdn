//! `decdn origin index` — generates `{hex}.obao4` pre-order outboard siblings
//! for every blob in each configured filesystem origin, enabling the node's
//! zero-copy fs-origin serve path (ADR 038 / #1511).

use std::path::Path;

use decdn_common::{cli, config, config::ResolvedOrigin};

/// Per-root indexing tally.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct IndexStats {
    pub written: u64,
    pub skipped: u64,
    pub mismatched: u64,
}

pub fn origin_dispatch(args: &cli::OriginArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    match &args.cmd {
        cli::OriginCommand::Index(index) => {
            let resolved = config::resolve_config(config_path, &index.run)?;
            let fs_roots: Vec<&std::path::PathBuf> = resolved
                .cache
                .origins
                .iter()
                .filter_map(|o| match o {
                    ResolvedOrigin::Fs { path } => Some(path),
                    _ => None,
                })
                .collect();
            anyhow::ensure!(
                !fs_roots.is_empty(),
                "no filesystem origins configured (cache.origins with kind = \"fs\")"
            );
            let mut total = IndexStats::default();
            for root in fs_roots {
                let s = index_fs_origin(root, index.force)?;
                println!(
                    "{}: {} written, {} skipped, {} mismatched",
                    root.display(),
                    s.written,
                    s.skipped,
                    s.mismatched
                );
                total.written += s.written;
                total.skipped += s.skipped;
                total.mismatched += s.mismatched;
            }
            println!(
                "done: {} written, {} skipped, {} mismatched",
                total.written, total.skipped, total.mismatched
            );
            Ok(())
        }
    }
}

/// Walk one fs-origin root, generating a `{hex}.obao4` for every blob file that
/// lacks one (or all, when `force`). A file whose contents do not hash to its
/// filename is left untouched and counted as mismatched.
pub fn index_fs_origin(base: &Path, force: bool) -> anyhow::Result<IndexStats> {
    let mut stats = IndexStats::default();
    for entry in walkdir::WalkDir::new(base).min_depth(2).max_depth(2) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let is_outboard = Path::new(&name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("obao4"));
        if is_outboard || name.len() != 64 {
            continue; // outboards and non-hash names are skipped
        }
        let Ok(expected_hash) = blake3::Hash::from_hex(&name) else {
            continue;
        };
        let data_path = entry.path();
        let obao4_path = data_path.with_file_name(format!("{name}.obao4"));
        if obao4_path.exists() && !force {
            stats.skipped += 1;
            continue;
        }
        let size = std::fs::metadata(data_path)?.len();
        let file = std::io::BufReader::new(std::fs::File::open(data_path)?);
        let (root, outboard) = decdn_bao_range::compute_pre_order_outboard(file, size)?;
        if root != *expected_hash.as_bytes() {
            eprintln!(
                "warning: {} contents hash to a different value; skipping (not writing an outboard)",
                data_path.display()
            );
            stats.mismatched += 1;
            continue;
        }
        // Write to a same-directory NamedTempFile then persist (atomic rename), so
        // a crash never leaves a partial .obao4 and concurrent `index` runs over
        // the same shard dir never collide on a shared temp name.
        let Some(shard_dir) = obao4_path.parent() else {
            anyhow::bail!(
                "obao4 path {} has no parent directory",
                obao4_path.display()
            );
        };
        let mut tmp = tempfile::NamedTempFile::new_in(shard_dir)?;
        std::io::Write::write_all(&mut tmp, &outboard)?;
        // Flush the bytes to disk before the rename: a `.obao4` is required for
        // zero-copy serve, so a power loss right after an unsynced rename must not
        // leave a present-but-empty outboard. Matches the CLI's other atomic
        // writers (`bundle create`, `bundle pull`).
        tmp.as_file().sync_all()?;
        tmp.persist(&obao4_path)
            .map_err(|e| anyhow::anyhow!("failed to persist {}: {e}", obao4_path.display()))?;
        stats.written += 1;
    }
    Ok(stats)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn index_generates_and_verifies_outboards_idempotently() {
        let dir = tempfile::tempdir().expect("tmp");
        let base = dir.path();
        // Seed one blob at {hex[0..2]}/{hex} with no .obao4.
        let payload = vec![5u8; 3 * 16 * 1024 + 11];
        let hash = blake3::hash(&payload);
        let hex = hash.to_hex();
        let shard = base.join(&hex.as_str()[..2]);
        std::fs::create_dir_all(&shard).expect("mkdir");
        std::fs::write(shard.join(hex.as_str()), &payload).expect("write blob");

        let stats = index_fs_origin(base, false).expect("index");
        assert_eq!(stats.written, 1);
        assert_eq!(stats.skipped, 0);

        // The .obao4 exists and matches the streaming compute over the payload.
        let obao4 = shard.join(format!("{}.obao4", hex.as_str()));
        let written = std::fs::read(&obao4).expect("obao4 present");
        let (root, expected) = decdn_bao_range::compute_pre_order_outboard(
            std::io::Cursor::new(payload),
            (3 * 16 * 1024 + 11) as u64,
        )
        .expect("compute");
        assert_eq!(root, *hash.as_bytes());
        assert_eq!(written, expected);

        // Idempotent second pass without --force skips it.
        let again = index_fs_origin(base, false).expect("index");
        assert_eq!(again.written, 0);
        assert_eq!(again.skipped, 1);
    }

    #[test]
    fn index_skips_hash_mismatched_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let base = dir.path();
        // A file whose name is a hash that does NOT match its contents.
        let wrong = blake3::hash(b"not the payload");
        let hex = wrong.to_hex();
        let shard = base.join(&hex.as_str()[..2]);
        std::fs::create_dir_all(&shard).expect("mkdir");
        std::fs::write(shard.join(hex.as_str()), b"actual contents").expect("write");

        let stats = index_fs_origin(base, false).expect("index");
        assert_eq!(stats.written, 0);
        assert_eq!(stats.mismatched, 1);
        assert!(
            !shard.join(format!("{}.obao4", hex.as_str())).exists(),
            "no bad outboard"
        );
    }
}
