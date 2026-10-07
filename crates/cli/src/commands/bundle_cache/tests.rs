use super::*;

#[test]
fn store_then_load_returns_the_cached_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"{\"version\":1,\"entries\":[]}".to_vec();
    let hash = *blake3::hash(&bytes).as_bytes();

    store(dir.path(), hash, &bytes);

    assert_eq!(load(dir.path(), hash), Some(bytes));
}

#[test]
fn load_is_none_when_no_cache_file_exists() {
    let dir = tempfile::tempdir().unwrap();
    let hash = *blake3::hash(b"absent").as_bytes();

    assert_eq!(load(dir.path(), hash), None);
}

#[test]
fn load_rejects_bytes_that_do_not_hash_to_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"original".to_vec();
    let hash = *blake3::hash(&bytes).as_bytes();
    store(dir.path(), hash, &bytes);

    // Corrupt the cached file in place; its bytes no longer match `hash`.
    std::fs::write(cache_path(dir.path(), hash), b"tampered").unwrap();

    assert_eq!(load(dir.path(), hash), None);
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
