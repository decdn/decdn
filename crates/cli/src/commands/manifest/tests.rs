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
        chunks: None,
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
fn serialize_canonical_emits_chunks_after_size() {
    let entries = vec![BundleEntry {
        path: "model.safetensors".into(),
        hash: "b3:whole".into(),
        size: 100,
        chunks: Some(vec![
            Chunk {
                hash: "b3:c0".into(),
                size: 60,
            },
            Chunk {
                hash: "b3:c1".into(),
                size: 40,
            },
        ]),
    }];
    let bytes = serialize_canonical(&entries).unwrap();
    let s = std::str::from_utf8(&bytes).unwrap();
    assert_eq!(
        s,
        "{\"version\":1,\"entries\":[{\"path\":\"model.safetensors\",\"hash\":\"b3:whole\",\"size\":100,\"chunks\":[{\"hash\":\"b3:c0\",\"size\":60},{\"hash\":\"b3:c1\",\"size\":40}]}]}"
    );
}

#[test]
fn serialize_canonical_omits_chunks_key_when_absent() {
    // A plain (unchunked) entry must serialize byte-identically to the
    // pre-chunks format: no `chunks` key at all.
    let entries = vec![BundleEntry {
        path: "a.txt".into(),
        hash: "b3:abc".into(),
        size: 12,
        chunks: None,
    }];
    let bytes = serialize_canonical(&entries).unwrap();
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap(),
        "{\"version\":1,\"entries\":[{\"path\":\"a.txt\",\"hash\":\"b3:abc\",\"size\":12}]}"
    );
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
// using the same per-file hasher `origin import` uses.
fn collect_paths(
    root: &Path,
    follow_symlinks: bool,
    exclude: &[&str],
) -> anyhow::Result<Vec<String>> {
    let canonical = std::fs::canonicalize(root)?;
    let excluder = build_excluder(&exclude.iter().map(|s| (*s).to_string()).collect::<Vec<_>>())?;
    let out = walk_and_collect(&canonical, follow_symlinks, &excluder, |canon, _rel| {
        let (h, s) = hash_file_at(canon)?;
        Ok((b3_hex_str(h), s, None))
    })?;
    Ok(out.entries.into_iter().map(|e| e.path).collect())
}

#[test]
fn walk_threads_chunks_from_closure_into_entry() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(dir.path().join("a.bin"), b"abc").unwrap();
    let canonical = std::fs::canonicalize(dir.path()).unwrap();
    let excluder = build_excluder(&[]).unwrap();
    let out = walk_and_collect(&canonical, false, &excluder, |_c, _r| {
        Ok((
            "b3:whole".to_string(),
            3,
            Some(vec![Chunk {
                hash: "b3:c0".into(),
                size: 3,
            }]),
        ))
    })
    .unwrap();
    assert_eq!(out.entries.len(), 1);
    let e = out.entries.first().unwrap();
    assert_eq!(e.chunks.as_ref().unwrap().len(), 1);
    assert_eq!(e.chunks.as_ref().unwrap().first().unwrap().hash, "b3:c0");
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
