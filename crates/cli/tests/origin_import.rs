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

use decdn_cache::{
    CHUNK_GROUP_BYTES, FilesystemOrigin, Hash, Origin, OriginFetch, OriginRangeFetch,
    OriginRangeRequest,
};
use decdn_cli::commands::bundle::bundle_create;
use decdn_cli::commands::origin::origin_import;
use decdn_common::cli::{BundleCreateArgs, OriginImportArgs};
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
        to: format!("fs:{}", origin.display()),
        move_source: false,
        bundle: None,
        force: false,
        follow_symlinks: false,
        exclude: Vec::new(),
        json: false,
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
        match reader.fetch_range(hash, req, 1 << 30).await.unwrap() {
            OriginRangeFetch::Ranged { data, outboard } => {
                assert_eq!(data.as_ref(), &payload[start..end]);
                assert!(!outboard.is_empty(), "outboard must be served for a range");
            }
            other => panic!("expected Ranged, got {other:?}"),
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
fn directory_import_matches_bundle_create_and_imports_manifest() {
    let src = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    // Manifest outputs live OUTSIDE the walked tree so neither walk sees them.
    let out = TempDir::new().unwrap();

    // A small tree with nested files.
    fs::create_dir_all(src.path().join("sub")).unwrap();
    fs::write(src.path().join("a.txt"), b"alpha").unwrap();
    fs::write(src.path().join("sub/b.bin"), vec![7u8; 33 * 1024]).unwrap();

    // Reference manifest bytes from `bundle create` on the same fileset.
    let ref_bundle = out.path().join("ref.json");
    rt().block_on(bundle_create(&BundleCreateArgs {
        input: src.path().to_path_buf(),
        output: ref_bundle.clone(),
        follow_symlinks: false,
        exclude: Vec::new(),
        json: false,
    }))
    .unwrap();
    let ref_bytes = fs::read(&ref_bundle).unwrap();

    // Import the directory, asking for the manifest to be written out too.
    let out_bundle = out.path().join("out.json");
    let mut args = import_args(src.path(), origin.path());
    args.bundle = Some(out_bundle.clone());
    rt().block_on(origin_import(&args)).unwrap();

    // The `--bundle` output is byte-identical to `bundle create` — that is what
    // makes the reported bundle hash the one publishers distribute.
    let out_bytes = fs::read(&out_bundle).unwrap();
    assert_eq!(
        out_bytes, ref_bytes,
        "manifest must match bundle create bytes"
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

fn hash_from_hex(hex: &str) -> (Hash, String) {
    let mut raw = [0u8; 32];
    for (i, slot) in raw.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    (Hash::from_bytes(raw), hex.to_string())
}
