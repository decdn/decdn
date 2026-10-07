use super::*;

const ROOT: &str = "cause-root-7f3a";
const OUTER: &str = "ctx-outer-7f3a";

fn chained() -> anyhow::Error {
    anyhow::anyhow!(ROOT).context(OUTER)
}

fn faults() -> [(CacheError, String); 4] {
    let hash = Hash::from_bytes([0; 32]);
    [
        (CacheError::Store(chained()), "store error".to_owned()),
        (CacheError::Feed(chained()), "admit feed failed".to_owned()),
        (CacheError::Internal(chained()), "internal fault".to_owned()),
        (
            CacheError::OriginError {
                hash,
                source: chained(),
            },
            format!("origin error fetching {hash}"),
        ),
    ]
}

#[test]
fn display_names_only_the_variant() {
    for (fault, shown) in faults() {
        assert_eq!(fault.to_string(), shown);
    }
}

#[test]
fn source_exposes_the_cause() {
    for (fault, _) in faults() {
        let source = std::error::Error::source(&fault).map(ToString::to_string);
        assert_eq!(source.as_deref(), Some(OUTER), "{fault:?}");
    }
}

#[test]
fn display_chain_renders_every_cause_once() {
    for (fault, shown) in faults() {
        assert_eq!(
            fault.display_chain().to_string(),
            format!("{shown}: {OUTER}: {ROOT}")
        );
    }
}

/// Format `e` the way the node logs a cache fault it carries in an `anyhow`
/// chain (`error = %format_args!("{err:#}")`), plus the `Debug` form. The
/// `Debug` form lists `source()` causes under "Caused by:", so a cause that
/// also sat in `Display` would appear there twice.
fn logged(e: CacheError) -> [String; 2] {
    let err = anyhow::Error::from(e);
    [format!("{err:#}"), format!("{err:?}")]
}

#[test]
fn logged_chain_names_each_cause_once() {
    for (fault, _) in faults() {
        for shown in logged(fault) {
            assert_eq!(shown.matches(ROOT).count(), 1, "{shown}");
            assert_eq!(shown.matches(OUTER).count(), 1, "{shown}");
        }
    }
}

fn decompression_fault() -> CacheError {
    let typed = OriginError::DecompressionFailed {
        encoding: SupportedEncoding::Gzip,
        source: std::io::Error::other(ROOT),
    };
    CacheError::OriginError {
        hash: Hash::from_bytes([0; 32]),
        source: anyhow::Error::from(typed).context(OUTER),
    }
}

#[test]
fn logged_decompression_failure_names_the_decoder_error_once() {
    for shown in logged(decompression_fault()) {
        assert_eq!(shown.matches(ROOT).count(), 1, "{shown}");
        assert!(shown.contains("failed to decompress gzip"), "{shown}");
    }
}

#[test]
fn origin_error_kind_finds_the_typed_cause_under_context() {
    let e = decompression_fault();
    assert!(
        matches!(
            e.origin_error_kind(),
            Some(OriginError::DecompressionFailed {
                encoding: SupportedEncoding::Gzip,
                ..
            })
        ),
        "{e:?}"
    );
}
