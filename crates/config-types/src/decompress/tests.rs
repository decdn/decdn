use super::*;

/// The TOML/JSON tag form is operator-visible config surface — pin
/// the lowercase `rename_all` contract so a future refactor that
/// drops or changes the attribute fails here rather than silently
/// breaking every operator's `decompress = "auto"`.
#[test]
fn serialises_to_lowercase_tags() {
    assert_eq!(
        serde_json::to_string(&DecompressMode::Auto).expect("serialise"),
        "\"auto\""
    );
    assert_eq!(
        serde_json::to_string(&DecompressMode::Strict).expect("serialise"),
        "\"strict\""
    );
    let back: DecompressMode = serde_json::from_str("\"strict\"").expect("deserialise");
    assert_eq!(back, DecompressMode::Strict);
    assert_eq!(DecompressMode::default(), DecompressMode::Auto);
}
