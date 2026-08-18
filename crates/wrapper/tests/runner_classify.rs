use onramp::runner::{FetchOutcome, classify_exit};

fn exit(code: i32) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    std::process::ExitStatus::from_raw((code & 0xff) << 8)
}

#[test]
fn exhausted_marker_classifies_as_exhausted() {
    let out = classify_exit(
        exit(1),
        "error: capability exhausted: ask the sponsor for a higher-cap capability or to refill the pool",
    );
    assert!(matches!(out, FetchOutcome::Exhausted));
}

#[test]
fn success_is_complete() {
    assert!(matches!(classify_exit(exit(0), ""), FetchOutcome::Complete));
}

#[test]
fn other_error_is_failed() {
    let out = classify_exit(exit(1), "some other error");
    assert!(matches!(out, FetchOutcome::Failed(_)));
}
