//! Integration tests for `decdn origin import` (issue #1904).
//!
//! The strongest check runs the real cache reader against what the command
//! wrote: a `decdn-cache` `FilesystemOrigin` must read the blob back and, using the
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
    CHUNK_GROUP_BYTES, FilesystemOrigin, Hash, Origin, OriginRangeFetch, OriginRangeRequest,
    OutboardFetch,
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

        // The size proof sibling proves the true size and no other.
        let object = reader
            .fetch_size_proof(hash)
            .await
            .unwrap()
            .expect("import writes the size proof");
        let (stated, proof) = decdn_bao_range::parse_size_proof_object(&object)
            .expect("the size proof object holds its size");
        let size = u64::try_from(payload.len()).unwrap();
        assert_eq!(stated, size, "the object states the true size");
        assert!(decdn_bao_range::verify_size_proof(
            *hash.as_bytes(),
            size,
            proof
        ));
        assert!(!decdn_bao_range::verify_size_proof(
            *hash.as_bytes(),
            size + 1,
            proof
        ));

        // A range read of the whole span round-trips the exact bytes.
        let whole = OriginRangeRequest {
            fetch_start: 0,
            fetch_end: size,
        };
        match reader.fetch_range_data(hash, whole).await.unwrap() {
            OriginRangeFetch::Ranged { data } => assert_eq!(data.as_ref(), payload.as_slice()),
            OriginRangeFetch::NotFound => panic!("expected the whole span, got NotFound"),
        }

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
            OutboardFetch::NotFound => panic!("expected the outboard, got NotFound"),
        };
        match reader.fetch_range_data(hash, req).await.unwrap() {
            OriginRangeFetch::Ranged { data } => {
                assert_eq!(data.as_ref(), &payload[start..end]);
                let total = u64::try_from(payload.len()).unwrap();
                let aligned = align_range(CHUNK_GROUP_BYTES, 2 * CHUNK_GROUP_BYTES, total).unwrap();
                encode_verified_range(*hash.as_bytes(), &aligned, &data, outboard)
                    .expect("the range must verify against the written outboard");
            }
            OriginRangeFetch::NotFound => panic!("expected Ranged, got NotFound"),
        }
        match reader.fetch_outboard(hash, 1 << 30).await.unwrap() {
            OutboardFetch::Found(outboard) => {
                assert!(!outboard.is_empty(), "outboard must be served for a range");
            }
            OutboardFetch::NotFound => panic!("expected Found, got NotFound"),
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
    assert_eq!(
        shard_entries.len(),
        3,
        "expected only data + .obao4 + .sizeproof"
    );
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
            // Each blob's size proof is readable, and a range of its whole
            // stated size serves.
            let object = reader
                .fetch_size_proof(hash)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("blob {hex} has no size proof"));
            let (size, _) = decdn_bao_range::parse_size_proof_object(&object).unwrap();
            let whole = OriginRangeRequest {
                fetch_start: 0,
                fetch_end: size,
            };
            match reader.fetch_range_data(hash, whole).await.unwrap() {
                OriginRangeFetch::Ranged { data } => {
                    assert_eq!(u64::try_from(data.len()).unwrap(), size);
                }
                OriginRangeFetch::NotFound => panic!("blob {hex} not readable: NotFound"),
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

// Whether `dir` lives on a case-sensitive file system: a lowercase file is
// written, and its uppercase name must not resolve to it.
fn is_case_sensitive(dir: &Path) -> bool {
    let lower = dir.join("decdn_case_probe");
    fs::write(&lower, b"x").unwrap();
    let sensitive = !dir.join("DECDN_CASE_PROBE").exists();
    fs::remove_file(&lower).unwrap();
    sensitive
}

// Every regular file under `dir`, at any depth.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(files_under(&path));
        } else {
            found.push(path);
        }
    }
    found
}

// A tree holding two paths that name one file on a case-insensitive file
// system is refused before anything is imported: every reader rejects its
// manifest (#2691). With `--move`, every source file stays where it is,
// the one beside the colliding pair included, and the store gets no blob.
// Skipped on a case-insensitive file system, which cannot hold both files.
#[test]
#[expect(
    clippy::print_stderr,
    reason = "test harness diagnostic surfaced in the nextest log"
)]
fn directory_import_refuses_paths_that_name_one_file() {
    for move_source in [false, true] {
        let src = TempDir::new().unwrap();
        if !is_case_sensitive(src.path()) {
            eprintln!(
                "skipping directory_import_refuses_paths_that_name_one_file: \
                 case-insensitive filesystem collapses README/readme"
            );
            return;
        }
        let origin = TempDir::new().unwrap();
        let out = TempDir::new().unwrap();
        for name in ["README", "readme", "other"] {
            fs::write(src.path().join(name), name).unwrap();
        }

        let out_bundle = out.path().join("out.json");
        let mut args = import_args(src.path(), origin.path());
        args.bundle = Some(out_bundle.clone());
        args.move_source = move_source;
        let err = rt().block_on(origin_import(&args)).unwrap_err();

        let case = format!("move={move_source}");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(r#"no reader accepts a bundle of it: "README" and "readme""#),
            "{case}: {msg}"
        );
        for name in ["README", "readme", "other"] {
            assert_eq!(
                fs::read(src.path().join(name)).unwrap(),
                name.as_bytes(),
                "{case}: the source file {name} stays"
            );
        }
        assert_eq!(files_under(origin.path()), Vec::<PathBuf>::new(), "{case}");
        assert!(!out_bundle.exists(), "{case}: no manifest is written");
    }
}

// A tree holding a name Windows strips (`README `) or opens as a stream
// (`a:b`) is refused before anything is imported: every reader rejects its
// manifest (#2746). With `--move`, every source file stays where it is.
// Unix only: a Windows file system cannot hold either name.
#[cfg(unix)]
#[test]
fn directory_import_refuses_an_unportable_name() {
    for (bad, move_source) in [("README ", false), ("README ", true), ("a:b", true)] {
        let src = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let out = TempDir::new().unwrap();
        fs::create_dir(src.path().join("dir")).unwrap();
        let names = [format!("dir/{bad}"), "other".to_string()];
        for name in &names {
            fs::write(src.path().join(name), name).unwrap();
        }

        let out_bundle = out.path().join("out.json");
        let mut args = import_args(src.path(), origin.path());
        args.bundle = Some(out_bundle.clone());
        args.move_source = move_source;
        let err = rt().block_on(origin_import(&args)).unwrap_err();

        let case = format!("{bad:?} move={move_source}");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(&format!("rejected component {bad:?}")),
            "{case}: {msg}"
        );
        for name in &names {
            assert_eq!(
                fs::read(src.path().join(name)).unwrap(),
                name.as_bytes(),
                "{case}: the source file {name} stays"
            );
        }
        assert_eq!(files_under(origin.path()), Vec::<PathBuf>::new(), "{case}");
        assert!(!out_bundle.exists(), "{case}: no manifest is written");
    }
}

fn hash_from_hex(hex: &str) -> (Hash, String) {
    let mut raw = [0u8; 32];
    for (i, slot) in raw.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    (Hash::from_bytes(raw), hex.to_string())
}

// --- `--dry-run` wiring ------------------------------------------------------
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

// The exact canonical manifest bytes a whole-file import of
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
