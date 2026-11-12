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
fn parse_target_rejects_fs_prefix() {
    // `fs` is the config `kind` name; as a `--to` prefix it would otherwise
    // seed a relative directory named `fs:...` that the node never reads.
    let err = parse_target(Path::new("fs:/var/lib/decdn/origin")).unwrap_err();
    assert!(format!("{err:#}").contains("not an `fs:` URI"));
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

#[test]
fn is_cross_device_matches_only_the_cross_device_kind() {
    assert!(is_cross_device(&std::io::Error::from(
        std::io::ErrorKind::CrossesDevices
    )));
    assert!(!is_cross_device(&std::io::Error::from(
        std::io::ErrorKind::NotFound
    )));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn is_cross_device_matches_a_raw_exdev() {
    // 18 == EXDEV on Linux and macOS: the error `rename` returns across mounts.
    assert!(is_cross_device(&std::io::Error::from_raw_os_error(18)));
}

/// A `--move` import of `bytes` from a fresh source file: the source, its
/// encoding, and the store's object directory.
fn move_fixture(bytes: &[u8]) -> (tempfile::TempDir, PathBuf, EncodedOutboard, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("store");
    std::fs::create_dir_all(&base).unwrap();
    let source = dir.path().join("model.bin");
    std::fs::write(&source, bytes).unwrap();
    let eo = encode_outboard(bytes, bytes.len() as u64).unwrap();
    let shard = base.join(eo.shard());
    (dir, source, eo, shard)
}

/// A `--move` import leaves the object with both siblings and no source.
#[test]
fn a_move_import_lands_the_object_with_both_siblings() {
    let bytes = vec![0x5a; 40_000];
    let (dir, source, eo, shard) = move_fixture(&bytes);
    import_one_file(&dir.path().join("store"), &source, true, false).unwrap();
    assert!(!source.exists(), "the source moved");
    assert_eq!(std::fs::read(shard.join(&eo.hash_hex)).unwrap(), bytes);
    assert_eq!(
        std::fs::read(shard.join(eo.obao4_name())).unwrap(),
        eo.outboard
    );
    assert_eq!(
        std::fs::read(shard.join(eo.size_proof_name())).unwrap(),
        eo.size_proof.as_ref()
    );
}

/// A `--move` cut short after the siblings landed still has its source, and
/// the next import of it moves the object into place.
#[test]
fn a_move_cut_after_the_siblings_finishes_on_the_next_import() {
    let bytes = vec![0xa5; 40_000];
    let (dir, source, eo, shard) = move_fixture(&bytes);
    std::fs::create_dir_all(&shard).unwrap();
    ensure_siblings(&shard, &eo).unwrap();
    assert!(source.exists() && !shard.join(&eo.hash_hex).exists());
    import_one_file(&dir.path().join("store"), &source, true, false).unwrap();
    assert!(!source.exists());
    assert_eq!(std::fs::read(shard.join(&eo.hash_hex)).unwrap(), bytes);
}
