use super::*;

#[test]
fn debug_is_redacted() {
    let s = Secret::from("hunter2");
    assert_eq!(format!("{s:?}"), "<redacted>");
    assert_eq!(s.expose(), "hunter2");
}

#[test]
fn file_source_trims_one_trailing_newline() {
    let dir = std::env::temp_dir().join(format!("sponsord-secret-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("token");
    std::fs::write(&path, "abc\n").unwrap();
    assert_eq!(
        Secret::resolve("X", None, Some(&path)).unwrap().expose(),
        "abc"
    );
    std::fs::write(&path, "abc \r\n").unwrap();
    assert_eq!(
        Secret::resolve("X", None, Some(&path)).unwrap().expose(),
        "abc "
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn missing_or_empty_is_an_error_naming_the_setting() {
    let err = Secret::resolve("SPONSORD_API_TOKEN", None, None).unwrap_err();
    assert!(err.to_string().contains("SPONSORD_API_TOKEN_FILE"), "{err}");
    assert!(Secret::resolve("X", Some(String::new()), None).is_err());
}
