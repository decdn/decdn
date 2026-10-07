use super::*;

#[test]
fn error_from_range_verify() {
    // Any RangeVerifyError becomes RangedStoreError::Alignment via `?`.
    fn coerce(e: crate::RangeVerifyError) -> RangedStoreError {
        e.into()
    }
    // The trait is dyn-compatible: this type-checks only if object-safe.
    fn _assert_dyn(_: &dyn RangedStore) {}
    // Construct a representative RangeVerifyError to exercise the From.
    let err = coerce(crate::RangeVerifyError::RangeOutOfBounds {
        offset: 0,
        len: 0,
        blob_size: 0,
    });
    assert!(matches!(err, RangedStoreError::Alignment(_)));
}

/// A wrapping variant names only itself and hands its cause to `source()`,
/// so a chain walk prints each cause once.
#[test]
fn a_wrapping_variant_renders_its_cause_once_in_a_chain() {
    const ROOT: &str = "store-root-7f3a";
    let wrapping = [
        (RangedStoreError::Backend(ROOT.into()), "backend"),
        (
            RangedStoreError::Alignment(crate::RangeVerifyError::RangeOutOfBounds {
                offset: 9,
                len: 1,
                blob_size: 4,
            }),
            "range alignment",
        ),
    ];
    for (err, shown) in wrapping {
        assert_eq!(err.to_string(), shown);
        let mut chain = vec![err.to_string()];
        let mut cause = std::error::Error::source(&err);
        while let Some(next) = cause {
            chain.push(next.to_string());
            cause = next.source();
        }
        let rendered = chain.join(": ");
        assert_eq!(chain.len(), 2, "{rendered}");
        let Some(inner) = chain.get(1) else {
            continue;
        };
        assert_eq!(rendered.matches(inner.as_str()).count(), 1, "{rendered}");
    }
}
