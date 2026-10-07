use super::*;

#[test]
fn debug_redacts_value() {
    let s = SecretString::new("hunter2");
    let dbg = format!("{s:?}");
    assert_eq!(dbg, "\"***\"");
    assert!(
        !dbg.contains("hunter2"),
        "raw value must never appear in Debug output"
    );
}

#[test]
fn debug_redacts_inside_struct() {
    // Realistic case: a containing struct derives Debug and prints
    // every field. The wrapper must redact in that context too.
    #[derive(Debug)]
    struct Wrap {
        #[allow(dead_code)] // Read by Debug only.
        secret: SecretString,
    }
    let w = Wrap {
        secret: SecretString::new("super-secret-token-abc123"),
    };
    let dbg = format!("{w:?}");
    assert!(!dbg.contains("super-secret"), "got: {dbg}");
    assert!(dbg.contains("***"), "redaction marker missing: {dbg}");
}

#[test]
fn expose_returns_raw_value() {
    let s = SecretString::new("hunter2");
    assert_eq!(s.expose(), "hunter2");
}

#[test]
fn is_empty_does_not_leak_value() {
    assert!(SecretString::new("").is_empty());
    assert!(!SecretString::new("x").is_empty());
}

#[test]
fn deserializes_from_toml_string() {
    // Realistic shape: secrets land via TOML -> serde into the
    // wrapper without ceremony at the call site.
    #[derive(Deserialize)]
    struct Wrap {
        secret: SecretString,
    }
    let w: Wrap = toml::from_str("secret = \"hunter2\"").unwrap();
    assert_eq!(w.secret.expose(), "hunter2");
}

#[test]
fn serialize_does_not_leak_value() {
    // The serialized form must never contain the cleartext secret.
    // This is the defense-in-depth contract: any serialization of a
    // config section (the structs derive `Serialize`) must redact
    // the credential rather than surface it.
    let s = SecretString::new("hunter2-this-is-the-secret");
    let json = serde_json::to_string(&s).unwrap();
    assert!(
        !json.contains("hunter2"),
        "raw secret leaked through Serialize: {json}"
    );
    assert!(
        json.starts_with("\"hash:") && json.ends_with('"'),
        "expected hashed form, got: {json}"
    );
}

#[test]
fn serialize_distinguishes_distinct_secrets() {
    // Distinct secrets must produce distinct serialized forms so the
    // redacted output stays useful for equality/change checks rather
    // than collapsing every credential to one indistinguishable
    // token.
    let a = SecretString::new("old-key");
    let b = SecretString::new("new-key");
    let ja = serde_json::to_string(&a).unwrap();
    let jb = serde_json::to_string(&b).unwrap();
    assert_ne!(ja, jb, "distinct secrets must serialize differently");
}

#[test]
fn serialize_is_deterministic_within_process() {
    // The same secret, serialized twice, must produce the same
    // bytes — a stable redacted form, so the hash is usable for
    // equality checks rather than varying run-to-run.
    let s = SecretString::new("a-stable-key");
    let first = serde_json::to_string(&s).unwrap();
    let second = serde_json::to_string(&s).unwrap();
    assert_eq!(first, second);
}
