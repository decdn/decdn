use super::*;

/// The suites read the content by these keys, so a renamed key breaks every
/// suite at once. Assert them here, with no chain and no built binary, so a
/// plain `cargo nextest run -p decdn-e2e` catches the rename.
#[test]
fn content_fixture_names_its_keys() {
    let fixture = Content::new().fixture();
    let keys: Vec<&str> = fixture.keys().map(String::as_str).collect();
    assert_eq!(keys.len(), 4, "{keys:?}");
    for key in ["bundle", "files", "blob", "blobBytes"] {
        assert!(keys.contains(&key), "no {key} in {keys:?}");
    }
    let files = fixture
        .get("files")
        .and_then(serde_json::Value::as_object)
        .expect("files is a JSON object");
    assert_eq!(files.len(), 3, "{files:?}");
    for path in ["a.txt", "index.html", "data.bin"] {
        assert!(files.contains_key(path), "no {path} in files");
    }
}
