use super::*;

#[test]
fn store_then_load_returns_the_cached_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"{\"version\":1,\"entries\":[]}".to_vec();
    let hash = *blake3::hash(&bytes).as_bytes();

    store(dir.path(), hash, &bytes);

    assert_eq!(load(dir.path(), hash).unwrap(), Some(bytes));
}

#[test]
fn load_is_none_when_no_cache_file_exists() {
    let dir = tempfile::tempdir().unwrap();
    let hash = *blake3::hash(b"absent").as_bytes();

    assert_eq!(load(dir.path(), hash).unwrap(), None);
}

/// A cached file whose bytes do not hash to the key is an error that names the
/// path (#2361).
#[test]
fn load_errors_naming_the_path_when_bytes_do_not_hash_to_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"original".to_vec();
    let hash = *blake3::hash(&bytes).as_bytes();
    store(dir.path(), hash, &bytes);

    // Corrupt the cached file in place; its bytes no longer match `hash`.
    let path = cache_path(dir.path(), hash);
    std::fs::write(&path, b"tampered").unwrap();

    let err = load(dir.path(), hash).expect_err("a mismatched cache file is an error");
    let msg = format!("{err:#}");
    assert!(msg.contains("does not hash to the bundle"), "{msg}");
    assert!(msg.contains(&path.display().to_string()), "{msg}");
}

/// A cache file that exists but cannot be read is an error that names the path
/// (#2361). A directory at the cache path stands in for an unreadable file, so
/// the test also holds when it runs as root.
#[test]
fn load_errors_naming_the_path_when_the_cache_file_is_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    let hash = *blake3::hash(b"unreadable").as_bytes();
    let path = cache_path(dir.path(), hash);
    std::fs::create_dir_all(&path).unwrap();

    let err = load(dir.path(), hash).expect_err("an unreadable cache file is an error");

    let msg = format!("{err:#}");
    assert!(msg.contains(&path.display().to_string()), "{msg}");
}

/// When the cache directory cannot be created, the write error names it.
#[test]
fn try_store_errors_naming_the_cache_dir_when_it_is_a_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(CACHE_DIR), b"not a directory").unwrap();
    let bytes = b"manifest".to_vec();
    let hash = *blake3::hash(&bytes).as_bytes();

    let err = try_store(dir.path(), hash, &bytes).expect_err("the cache dir is a file");

    assert!(format!("{err:#}").contains(CACHE_DIR), "{err:#}");
}

/// When the final rename fails, the write error names the destination file. A
/// directory at the cache path makes the rename fail.
#[test]
fn try_store_errors_naming_the_file_when_the_rename_fails() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"manifest".to_vec();
    let hash = *blake3::hash(&bytes).as_bytes();
    let path = cache_path(dir.path(), hash);
    std::fs::create_dir_all(&path).unwrap();

    let err = try_store(dir.path(), hash, &bytes).expect_err("a directory blocks the rename");

    let msg = format!("{err:#}");
    assert!(
        msg.contains(&format!("persist {}", path.display())),
        "{msg}"
    );
}

#[test]
fn cache_path_is_hash_keyed_under_the_cache_dir() {
    let hash = *blake3::hash(b"x").as_bytes();
    let p = cache_path(Path::new("/out"), hash);
    let hex = blake3::Hash::from_bytes(hash).to_hex();
    assert_eq!(
        p,
        Path::new("/out")
            .join(CACHE_DIR)
            .join(format!("{hex}.json"))
    );
}
