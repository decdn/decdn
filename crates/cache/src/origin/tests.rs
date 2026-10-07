use super::OriginFetch;

/// The bytes-less variants collect to `None`. `AlreadyAdmitted` carries no
/// stream (the origin already streamed the blob into the store), so a
/// caller draining it gets `None` just as it does for `NotFound` — never a
/// panic or an empty buffer masquerading as content. The pull-through path
/// never drains it (it short-circuits on the variant), so this only pins the
/// contract for the `collect_to_bytes` helper's other callers.
#[tokio::test]
async fn bytesless_variants_collect_to_none() {
    assert!(
        OriginFetch::AlreadyAdmitted
            .collect_to_bytes()
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        OriginFetch::NotFound
            .collect_to_bytes()
            .await
            .unwrap()
            .is_none()
    );
}
