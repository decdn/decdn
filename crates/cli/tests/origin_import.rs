//! Integration tests for `decdn origin import` (issue #1904).
//!
//! The strongest check runs the real cache reader against what the command
//! wrote: a `decdn-cache` `FilesystemOrigin` must fetch the blob and, using the
//! sibling `{hex}.obao4`, serve a verified byte range. That proves the outboard
//! is the daemon-compatible `IROH_BLOCK_SIZE` encoding — the whole point of the
//! command — not just that a file with the right name exists.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::fs;
use std::path::{Path, PathBuf};

use decdn_cache::range_pull::{align_range, encode_verified_range};
use decdn_cache::{
    CHUNK_GROUP_BYTES, FilesystemOrigin, Hash, Origin, OriginFetch, OriginRangeFetch,
    OriginRangeRequest, OutboardFetch,
};
use decdn_cli::commands::origin::origin_import;
use decdn_common::cli::OriginImportArgs;
use tempfile::TempDir;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn import_args(input: &Path, origin: &Path) -> OriginImportArgs {
    OriginImportArgs {
        input: input.to_path_buf(),
        to: Some(origin.to_path_buf()),
        move_source: false,
        bundle: None,
        force: false,
        follow_symlinks: false,
        exclude: Vec::new(),
        json: false,
        optimize: false,
        chunk_avg: None,
        chunk_min: None,
        chunk_max: None,
        subfolder: None,
        dry_run: false,
    }
}

// A payload spanning several 16 KiB chunk groups so a non-trivial aligned range
// can be served from the written outboard.
fn multi_group_payload() -> Vec<u8> {
    (0..(5 * CHUNK_GROUP_BYTES + 123))
        .map(|i| (i % 251) as u8)
        .collect()
}

fn data_object_path(origin: &Path, hex: &str) -> PathBuf {
    origin.join(&hex[..2]).join(hex)
}

fn hash_of(payload: &[u8]) -> (Hash, String) {
    let h = blake3::hash(payload);
    (Hash::from_bytes(*h.as_bytes()), h.to_hex().to_string())
}

#[test]
fn single_file_import_is_readable_and_range_serves() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let payload = multi_group_payload();
    let file = src.path().join("blob.bin");
    fs::write(&file, &payload).unwrap();

    rt().block_on(origin_import(&import_args(&file, origin.path())))
        .unwrap();

    let (hash, hex) = hash_of(&payload);
    let data = data_object_path(origin.path(), &hex);
    let obao4 = origin.path().join(&hex[..2]).join(format!("{hex}.obao4"));
    assert!(data.is_file(), "data object missing at {}", data.display());
    assert!(obao4.is_file(), "outboard missing at {}", obao4.display());
    // The data object is the source bytes verbatim.
    assert_eq!(fs::read(&data).unwrap(), payload);

    // Now read it back through the real cache reader.
    rt().block_on(async {
        let reader = FilesystemOrigin::new(origin.path()).await.unwrap();

        // Whole-blob fetch round-trips the exact bytes.
        let fetched = reader.fetch(hash, 1 << 30).await.unwrap();
        let bytes = match fetched {
            OriginFetch::Found { .. } => fetched.collect_to_bytes().await.unwrap().unwrap(),
            other => panic!("expected Found, got {other:?}"),
        };
        assert_eq!(bytes.as_ref(), payload.as_slice());

        // A chunk-group-aligned range verifies against the written outboard —
        // this fails if the `.obao4` is a foreign / wrong-block-size encoding.
        let req = OriginRangeRequest {
            fetch_start: CHUNK_GROUP_BYTES,
            fetch_end: 3 * CHUNK_GROUP_BYTES,
        };
        let start = usize::try_from(CHUNK_GROUP_BYTES).unwrap();
        let end = usize::try_from(3 * CHUNK_GROUP_BYTES).unwrap();
        let outboard = match reader.fetch_outboard(hash, 1 << 30).await.unwrap() {
            OutboardFetch::Found(outboard) => outboard,
            other => panic!("expected the outboard, got {other:?}"),
        };
        match reader.fetch_range_data(hash, req).await.unwrap() {
            OriginRangeFetch::Ranged { data } => {
                assert_eq!(data.as_ref(), &payload[start..end]);
                let total = u64::try_from(payload.len()).unwrap();
                let aligned = align_range(CHUNK_GROUP_BYTES, 2 * CHUNK_GROUP_BYTES, total).unwrap();
                encode_verified_range(*hash.as_bytes(), &aligned, &data, outboard)
                    .expect("the range must verify against the written outboard");
            }
            other => panic!("expected Ranged, got {other:?}"),
        }
        match reader.fetch_outboard(hash, 1 << 30).await.unwrap() {
            OutboardFetch::Found(outboard) => {
                assert!(!outboard.is_empty(), "outboard must be served for a range");
            }
            other => panic!("expected Found, got {other:?}"),
        }
    });
}

#[test]
fn reimport_is_idempotent_noop() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let payload = vec![9u8; 40 * 1024];
    let file = src.path().join("blob.bin");
    fs::write(&file, &payload).unwrap();

    let args = import_args(&file, origin.path());
    rt().block_on(origin_import(&args)).unwrap();
    let (_, hex) = hash_of(&payload);
    let data = data_object_path(origin.path(), &hex);
    let obao4 = origin.path().join(&hex[..2]).join(format!("{hex}.obao4"));
    let data_bytes = fs::read(&data).unwrap();
    let obao4_bytes = fs::read(&obao4).unwrap();

    // Second import of identical content is a clean no-op — same bytes, no
    // stray temp files left in the shard dir.
    rt().block_on(origin_import(&args)).unwrap();
    assert_eq!(fs::read(&data).unwrap(), data_bytes);
    assert_eq!(fs::read(&obao4).unwrap(), obao4_bytes);
    let shard_entries: Vec<_> = fs::read_dir(origin.path().join(&hex[..2]))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(shard_entries.len(), 2, "expected only data + .obao4");
}

#[test]
fn move_consumes_the_source_file() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let payload = vec![1u8; 20 * 1024];
    let file = src.path().join("blob.bin");
    fs::write(&file, &payload).unwrap();

    let mut args = import_args(&file, origin.path());
    args.move_source = true;
    rt().block_on(origin_import(&args)).unwrap();

    assert!(!file.exists(), "source should have been moved");
    let (_, hex) = hash_of(&payload);
    assert!(data_object_path(origin.path(), &hex).is_file());
}

// Data-safety: with --move, a same-size but corrupt/foreign object already at
// the target hash must NOT cause the source (the only correct copy) to be
// deleted. The command re-verifies the present object's content and refuses.
#[test]
fn move_refuses_to_delete_source_when_present_object_is_corrupt() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let payload = vec![1u8; 20 * 1024];
    let file = src.path().join("blob.bin");
    fs::write(&file, &payload).unwrap();

    // Plant a same-length but wrong-byte object at the content-addressed path.
    let (_, hex) = hash_of(&payload);
    let shard = origin.path().join(&hex[..2]);
    fs::create_dir_all(&shard).unwrap();
    let mut corrupt = payload.clone();
    corrupt[0] ^= 0xFF;
    assert_eq!(corrupt.len(), payload.len());
    fs::write(shard.join(&hex), &corrupt).unwrap();

    let mut args = import_args(&file, origin.path());
    args.move_source = true;
    let err = rt().block_on(origin_import(&args)).unwrap_err();
    assert!(
        format!("{err:#}").contains("do not match"),
        "err was: {err:#}"
    );
    // The source is preserved and the corrupt object is untouched.
    assert!(file.exists(), "source must not be deleted on a mismatch");
    assert_eq!(fs::read(shard.join(&hex)).unwrap(), corrupt);
}

// With --move, a source whose content is already present AND verifies is safely
// consumed (the store holds the correct bytes).
#[test]
fn move_consumes_source_when_present_object_verifies() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let payload = vec![2u8; 24 * 1024];

    // Seed the origin by copy-importing one copy.
    let first = src.path().join("first.bin");
    fs::write(&first, &payload).unwrap();
    rt().block_on(origin_import(&import_args(&first, origin.path())))
        .unwrap();

    // A second identical file, move-imported: the object is already present and
    // verifies, so the redundant source is removed.
    let second = src.path().join("second.bin");
    fs::write(&second, &payload).unwrap();
    let mut args = import_args(&second, origin.path());
    args.move_source = true;
    rt().block_on(origin_import(&args)).unwrap();
    assert!(
        !second.exists(),
        "verified-present source should be consumed"
    );
}

// A stale/foreign `.obao4` sibling left by a partial prior import is replaced on
// re-import, not trusted — otherwise the node would reject range serving.
#[test]
fn stale_obao4_is_replaced() {
    let origin = TempDir::new().unwrap();
    let payload = vec![5u8; 48 * 1024];
    let (_, hex) = hash_of(&payload);
    let shard = origin.path().join(&hex[..2]);
    fs::create_dir_all(&shard).unwrap();
    // Correct data object, but a garbage sibling outboard.
    fs::write(shard.join(&hex), &payload).unwrap();
    fs::write(shard.join(format!("{hex}.obao4")), b"stale-garbage").unwrap();

    let src = TempDir::new().unwrap();
    let file = src.path().join("blob.bin");
    fs::write(&file, &payload).unwrap();
    rt().block_on(origin_import(&import_args(&file, origin.path())))
        .unwrap();

    let size = u64::try_from(payload.len()).unwrap();
    let expected = decdn_bao_range::encode_outboard(payload.as_slice(), size)
        .unwrap()
        .outboard;
    assert_eq!(
        fs::read(shard.join(format!("{hex}.obao4"))).unwrap(),
        expected,
        "stale outboard must be replaced with the correct encoding"
    );
}

#[test]
fn directory_import_matches_dry_run_and_imports_manifest() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    // Manifest outputs live OUTSIDE the walked tree so neither walk sees them.
    let out = TempDir::new().unwrap();

    // A small tree with nested files.
    fs::create_dir_all(src.path().join("sub")).unwrap();
    fs::write(src.path().join("a.txt"), b"alpha").unwrap();
    fs::write(src.path().join("sub/b.bin"), vec![7u8; 33 * 1024]).unwrap();

    // Reference manifest bytes: what `origin import --dry-run` prints on the
    // same fileset, via the real binary (a subprocess is the only way to
    // observe the dry-run stdout contract).
    let dry_run_out = Command::new(env!("CARGO_BIN_EXE_decdn"))
        .args([
            "origin",
            "import",
            "-i",
            src.path().to_str().unwrap(),
            "--dry-run",
        ])
        .output()
        .expect("run decdn binary");
    assert!(
        dry_run_out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&dry_run_out.stderr)
    );
    let ref_bytes = dry_run_out.stdout;

    // Import the directory, asking for the manifest to be written out too.
    let out_bundle = out.path().join("out.json");
    let mut args = import_args(src.path(), origin.path());
    args.bundle = Some(out_bundle.clone());
    rt().block_on(origin_import(&args)).unwrap();

    // The `--bundle` output is byte-identical to the `--dry-run` stdout — that
    // is what makes the reported bundle hash the one publishers distribute.
    let out_bytes = fs::read(&out_bundle).unwrap();
    assert_eq!(
        out_bytes, ref_bytes,
        "manifest must match origin import --dry-run bytes"
    );

    // The manifest blob itself must be present in the origin under its own hash,
    // so the whole tree is retrievable by that one hash.
    let manifest_hash = blake3::hash(&out_bytes).to_hex().to_string();
    assert!(
        data_object_path(origin.path(), &manifest_hash).is_file(),
        "manifest blob must be imported into the origin"
    );

    // Every referenced file blob is present and readable through the reader.
    let out_json: serde_json::Value = serde_json::from_slice(&out_bytes).unwrap();
    let entries = out_json["entries"].as_array().unwrap();
    rt().block_on(async {
        let reader = FilesystemOrigin::new(origin.path()).await.unwrap();
        for e in entries {
            let hex = e["hash"].as_str().unwrap().strip_prefix("b3:").unwrap();
            let (hash, _) = hash_from_hex(hex);
            match reader.fetch(hash, 1 << 30).await.unwrap() {
                OriginFetch::Found { .. } => {}
                other => panic!("blob {hex} not readable: {other:?}"),
            }
        }
    });
}

// A directory import with `--subfolder` writes a manifest whose every entry
// path is prefixed with that folder, and imports the manifest blob under the
// hash it reports — so a pull materializes the whole tree under one directory.
#[test]
fn directory_import_subfolder_prefixes_manifest_entries() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();

    fs::create_dir_all(src.path().join("sub")).unwrap();
    fs::write(src.path().join("a.txt"), b"alpha").unwrap();
    fs::write(src.path().join("sub/b.txt"), b"beta").unwrap();

    let out_bundle = out.path().join("out.json");
    let mut args = import_args(src.path(), origin.path());
    args.bundle = Some(out_bundle.clone());
    args.subfolder = Some("release/v2".to_string());
    rt().block_on(origin_import(&args)).unwrap();

    // The written manifest carries prefixed, sorted paths.
    let out_bytes = fs::read(&out_bundle).unwrap();
    let out_json: serde_json::Value = serde_json::from_slice(&out_bytes).unwrap();
    let paths: Vec<&str> = out_json["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["release/v2/a.txt", "release/v2/sub/b.txt"]);

    // The manifest blob is imported under its own hash (of the prefixed bytes).
    let manifest_hash = blake3::hash(&out_bytes).to_hex().to_string();
    assert!(
        data_object_path(origin.path(), &manifest_hash).is_file(),
        "prefixed manifest blob must be imported into the origin"
    );
}

fn hash_from_hex(hex: &str) -> (Hash, String) {
    let mut raw = [0u8; 32];
    for (i, slot) in raw.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    (Hash::from_bytes(raw), hex.to_string())
}

// --- `--optimize` / `--dry-run` wiring (Task 6) ------------------------------
//
// These drive the real `decdn` binary so the actual process stdout/stderr and
// exit code are observed — the dry-run contract is "the canonical manifest
// bytes, and nothing else, on stdout", which only a subprocess can prove.

use std::process::{Command, Output};

fn decdn(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_decdn"))
        .args(args)
        .output()
        .expect("run decdn binary")
}

// Deterministic pseudo-random bytes so content-defined chunk boundaries form.
fn pseudo(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

// The exact canonical manifest bytes a whole-file (non-optimized) import of
// `tree` produces, computed independently of the command under test.
fn expected_whole_file_manifest(tree: &Path) -> Vec<u8> {
    // Two files placed by `two_file_tree`, sorted by path bytes.
    let mut entries: Vec<(String, String, u64)> = ["a.txt", "b.txt"]
        .into_iter()
        .map(|name| {
            let bytes = fs::read(tree.join(name)).unwrap();
            let h = blake3::hash(&bytes);
            (
                name.to_string(),
                format!("b3:{}", h.to_hex()),
                bytes.len() as u64,
            )
        })
        .collect();
    entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let frags: Vec<String> = entries
        .iter()
        .map(|(p, h, sz)| format!("{{\"path\":\"{p}\",\"hash\":\"{h}\",\"size\":{sz}}}"))
        .collect();
    format!("{{\"version\":1,\"entries\":[{}]}}", frags.join(",")).into_bytes()
}

fn two_file_tree() -> TempDir {
    let tree = TempDir::new().unwrap();
    fs::write(tree.path().join("a.txt"), b"alpha contents").unwrap();
    fs::write(tree.path().join("b.txt"), b"bravo contents here").unwrap();
    tree
}

#[test]
fn dry_run_prints_canonical_manifest_to_stdout_no_blobs() {
    let tree = two_file_tree();
    let origin = TempDir::new().unwrap();
    // A --to path that does NOT exist yet: dry-run must never create it.
    let ghost = origin.path().join("never-created");

    let out = decdn(&[
        "origin",
        "import",
        "-i",
        tree.path().to_str().unwrap(),
        "--dry-run",
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.stdout, expected_whole_file_manifest(tree.path()));
    assert!(!ghost.exists(), "dry-run must not create any origin dir");
}

#[test]
fn dry_run_stdout_matches_bundle_written_file() {
    let tree = two_file_tree();
    let out_dir = TempDir::new().unwrap();
    let bundle = out_dir.path().join("m.json");

    let out = decdn(&[
        "origin",
        "import",
        "-i",
        tree.path().to_str().unwrap(),
        "--dry-run",
        "--bundle",
        bundle.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let file_bytes = fs::read(&bundle).unwrap();
    assert_eq!(out.stdout, file_bytes);
    assert_eq!(out.stdout, expected_whole_file_manifest(tree.path()));
}

#[test]
fn to_required_unless_dry_run() {
    let tree = two_file_tree();
    let out = decdn(&["origin", "import", "-i", tree.path().to_str().unwrap()]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--to"), "stderr was: {err}");
}

#[test]
fn chunk_flags_require_optimize() {
    let tree = two_file_tree();
    let origin = TempDir::new().unwrap();
    let out = decdn(&[
        "origin",
        "import",
        "-i",
        tree.path().to_str().unwrap(),
        "--to",
        &origin.path().display().to_string(),
        "--chunk-avg",
        "2MiB",
    ]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--optimize"), "stderr was: {err}");
}

// A tree of two files that share a >4 MiB identical middle region so CDC
// produces at least one shared chunk that dedups across the two files.
fn shared_region_tree() -> TempDir {
    let tree = TempDir::new().unwrap();
    let shared = pseudo(3, 8 * 1024 * 1024);
    let mut f1 = pseudo(10, 4 * 1024 * 1024);
    f1.extend_from_slice(&shared);
    f1.extend_from_slice(&pseudo(11, 4 * 1024 * 1024));
    let mut f2 = pseudo(20, 4 * 1024 * 1024);
    f2.extend_from_slice(&shared);
    f2.extend_from_slice(&pseudo(21, 4 * 1024 * 1024));
    fs::write(tree.path().join("f1.bin"), &f1).unwrap();
    fs::write(tree.path().join("f2.bin"), &f2).unwrap();
    tree
}

#[test]
fn optimize_stores_whole_file_blobs_and_chunk_hints() {
    let tree = shared_region_tree();
    let origin = TempDir::new().unwrap();

    let out = decdn(&[
        "origin",
        "import",
        "-i",
        tree.path().to_str().unwrap(),
        "--to",
        &origin.path().display().to_string(),
        "--optimize",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Status JSON is on stdout for a non-dry-run import.
    let report: serde_json::Value = serde_json::from_slice(out.stdout.trim_ascii_end()).unwrap();
    assert_eq!(report["optimized"].as_bool(), Some(true));
    let total = report["chunks_total"].as_u64().unwrap();
    assert!(total > 0, "expected at least one chunk hint, got {total}");
    assert!(
        report.get("chunks_written").is_none(),
        "chunks_written must be dropped now that optimize stores no per-chunk blobs"
    );

    // The written manifest blob is retrievable; every entry's WHOLE-FILE hash
    // (+ its .obao4 outboard) is stored, exactly like a plain import — the
    // chunk list is manifest-only dedup hints, never separately stored blobs.
    let bundle_hex = report["bundle_hash"]
        .as_str()
        .unwrap()
        .strip_prefix("b3:")
        .unwrap();
    assert!(
        data_object_path(origin.path(), bundle_hex).is_file(),
        "manifest blob must be stored"
    );
    let manifest_bytes = fs::read(data_object_path(origin.path(), bundle_hex)).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    let entries = manifest["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);

    let mut whole_hashes: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut all_hint_hashes: Vec<String> = Vec::new();
    for e in entries {
        let whole_hex = e["hash"].as_str().unwrap().strip_prefix("b3:").unwrap();
        whole_hashes.insert(whole_hex.to_string());
        let data = data_object_path(origin.path(), whole_hex);
        let obao4 = origin
            .path()
            .join(&whole_hex[..2])
            .join(format!("{whole_hex}.obao4"));
        assert!(data.is_file(), "whole-file blob {whole_hex} must be stored");
        assert!(
            obao4.is_file(),
            "whole-file outboard missing: {}",
            obao4.display()
        );

        let size = e["size"].as_u64().unwrap();
        let chunks = e["chunks"].as_array().expect("entry must carry hints");
        assert!(!chunks.is_empty());
        let sum: u64 = chunks.iter().map(|c| c["size"].as_u64().unwrap()).sum();
        assert_eq!(sum, size, "chunk hint sizes must sum to the entry size");
        for c in chunks {
            let chex = c["hash"].as_str().unwrap().strip_prefix("b3:").unwrap();
            all_hint_hashes.push(chex.to_string());
        }
    }

    // No stored object exists for a chunk-hash-only hint (one that isn't also
    // some entry's whole-file hash) — optimize stores whole files, not chunks.
    for chex in &all_hint_hashes {
        if whole_hashes.contains(chex) {
            continue;
        }
        assert!(
            !data_object_path(origin.path(), chex).is_file(),
            "chunk hint {chex} must NOT be stored as a data object"
        );
    }

    // The two files share an identical middle region, so their hint lists
    // must share at least one identical chunk hash.
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut shared = false;
    for h in &all_hint_hashes {
        if !seen.insert(h.as_str()) {
            shared = true;
        }
    }
    assert!(shared, "expected the two entries to share >=1 chunk hint");
}

// `--dry-run --optimize` must preview the chunk-hint count the same way a real
// run would compute it, even though no blob is written.
#[test]
fn optimize_dry_run_previews_chunk_hints_without_writing() {
    let tree = shared_region_tree();
    let origin = TempDir::new().unwrap();
    // A --to path that must NEVER be created by a dry run.
    let ghost = origin.path().join("never-created");

    let out = decdn(&[
        "origin",
        "import",
        "-i",
        tree.path().to_str().unwrap(),
        "--optimize",
        "--dry-run",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Status JSON is on stderr under --dry-run; stdout carries only the
    // canonical manifest.
    let report: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&out.stderr).trim()).unwrap();
    assert_eq!(report["optimized"].as_bool(), Some(true));
    let total = report["chunks_total"].as_u64().unwrap();
    assert!(total > 0, "expected at least one chunk hint, got {total}");
    assert!(
        report.get("chunks_written").is_none(),
        "chunks_written must be dropped"
    );

    // Stdout is still the clean manifest, carrying chunk hints.
    let manifest: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let entries = manifest["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    for e in entries {
        assert!(e["chunks"].as_array().is_some(), "entry must carry hints");
    }

    // No blobs written, and the never-requested origin dir was never created.
    assert!(!ghost.exists(), "dry-run must not create any origin dir");
}

#[test]
fn optimize_single_file_emits_one_entry_manifest() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let out_dir = TempDir::new().unwrap();
    let file = src.path().join("model.bin");
    fs::write(&file, pseudo(42, 10 * 1024 * 1024)).unwrap();
    let bundle = out_dir.path().join("m.json");

    let out = decdn(&[
        "origin",
        "import",
        "-i",
        file.to_str().unwrap(),
        "--to",
        &origin.path().display().to_string(),
        "--optimize",
        "--bundle",
        bundle.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_slice(&fs::read(&bundle).unwrap()).unwrap();
    let entries = manifest["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "single file → exactly one entry");
    let e = &entries[0];
    assert_eq!(e["path"].as_str(), Some("model.bin"));
    let whole_hex = e["hash"].as_str().unwrap().strip_prefix("b3:").unwrap();
    assert!(
        data_object_path(origin.path(), whole_hex).is_file(),
        "whole-file blob {whole_hex} must be written"
    );
    let obao4 = origin
        .path()
        .join(&whole_hex[..2])
        .join(format!("{whole_hex}.obao4"));
    assert!(obao4.is_file(), "whole-file outboard must be written");

    let chunks = e["chunks"].as_array().expect("entry must carry hints");
    assert!(!chunks.is_empty());
    let size = e["size"].as_u64().unwrap();
    let sum: u64 = chunks.iter().map(|c| c["size"].as_u64().unwrap()).sum();
    assert_eq!(sum, size, "chunk hint sizes must sum to the entry size");
    for c in chunks {
        let chex = c["hash"].as_str().unwrap().strip_prefix("b3:").unwrap();
        if chex == whole_hex {
            continue;
        }
        assert!(
            !data_object_path(origin.path(), chex).is_file(),
            "chunk hint {chex} must NOT be stored as its own data object"
        );
    }
}

// `--optimize` streams the source file twice: once to write the whole-file
// blob, once to compute chunk hints. `--move` renames the source away after
// the first pass, leaving nothing for the second pass to re-open, so the
// combo is rejected up front, before any filesystem work happens.
#[test]
fn optimize_and_move_are_rejected_together() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let file = src.path().join("blob.bin");
    fs::write(&file, pseudo(1, 8 * 1024 * 1024)).unwrap();

    let out = decdn(&[
        "origin",
        "import",
        "-i",
        file.to_str().unwrap(),
        "--to",
        &origin.path().display().to_string(),
        "--optimize",
        "--move",
    ]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--move is incompatible with --optimize"),
        "stderr was: {stderr}"
    );
    // Nothing should have been written or moved.
    assert!(file.exists(), "source must be left untouched");
    assert!(
        fs::read_dir(origin.path()).unwrap().next().is_none(),
        "origin dir must stay empty"
    );
}

// The stored whole-file blob is byte-identical to the source, and its content
// hash is exactly the entry's `hash` and the BLAKE3 the chunker independently
// computed over the same bytes — the two streaming passes (`import_one_file`
// and `chunk_file`) agree on both hash and size.
#[test]
fn optimize_whole_file_blob_matches_entry_hash() {
    let tree = shared_region_tree();
    let origin = TempDir::new().unwrap();

    let out = decdn(&[
        "origin",
        "import",
        "-i",
        tree.path().to_str().unwrap(),
        "--to",
        &origin.path().display().to_string(),
        "--optimize",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(out.stdout.trim_ascii_end()).unwrap();
    let bundle_hex = report["bundle_hash"]
        .as_str()
        .unwrap()
        .strip_prefix("b3:")
        .unwrap();
    let manifest_bytes = fs::read(data_object_path(origin.path(), bundle_hex)).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();

    for e in manifest["entries"].as_array().unwrap() {
        let whole_hex = e["hash"].as_str().unwrap().strip_prefix("b3:").unwrap();
        let stored = fs::read(data_object_path(origin.path(), whole_hex)).unwrap();
        assert_eq!(blake3::hash(&stored).to_hex().as_str(), whole_hex);
        let source = fs::read(tree.path().join(e["path"].as_str().unwrap())).unwrap();
        assert_eq!(stored, source, "stored blob must equal the source bytes");
    }
}
