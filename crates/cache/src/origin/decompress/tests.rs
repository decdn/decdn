use super::*;

#[test]
fn auto_resolves_known_and_identity_encodings() {
    assert!(matches!(
        resolve_encoding("", DecompressMode::Auto),
        Ok(None)
    ));
    assert!(matches!(
        resolve_encoding("identity", DecompressMode::Auto),
        Ok(None)
    ));
    assert!(matches!(
        resolve_encoding("gzip", DecompressMode::Auto),
        Ok(Some(SupportedEncoding::Gzip))
    ));
    assert!(matches!(
        resolve_encoding("X-GZIP", DecompressMode::Auto),
        Ok(Some(SupportedEncoding::Gzip))
    ));
    assert!(matches!(
        resolve_encoding("ZSTD", DecompressMode::Auto),
        Ok(Some(SupportedEncoding::Zstd))
    ));
}

#[test]
fn unknown_encoding_is_error_in_both_modes() {
    for mode in [DecompressMode::Auto, DecompressMode::Strict] {
        assert!(matches!(
            resolve_encoding("br", mode),
            Err(OriginError::UnsupportedEncoding { .. })
        ));
    }
}

#[test]
fn strict_rejects_known_encodings_but_allows_identity() {
    assert!(matches!(
        resolve_encoding("gzip", DecompressMode::Strict),
        Err(OriginError::UnsupportedEncoding { .. })
    ));
    assert!(matches!(
        resolve_encoding("zstd", DecompressMode::Strict),
        Err(OriginError::UnsupportedEncoding { .. })
    ));
    assert!(matches!(
        resolve_encoding("identity", DecompressMode::Strict),
        Ok(None)
    ));
}
