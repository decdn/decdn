use super::*;

/// `wait_admin` is required: `{}` is refused rather than silently read as
/// the SIGTERM-equivalent drain.
#[test]
fn drain_request_requires_wait_admin() {
    assert!(
        serde_json::from_str::<DrainRequest>("{}").is_err(),
        "a DrainRequest without wait_admin must not deserialize"
    );
    let req: DrainRequest = serde_json::from_str(r#"{"wait_admin":true}"#)
        .expect("an explicit wait_admin must deserialize");
    assert!(req.wait_admin);
}

/// `dry_run` is required: `{"hash": …}` alone is refused rather than read
/// as a real evict.
#[test]
fn evict_request_requires_dry_run() {
    assert!(
        serde_json::from_str::<EvictRequest>(r#"{"hash":"00"}"#).is_err(),
        "an EvictRequest without dry_run must not deserialize"
    );
}
