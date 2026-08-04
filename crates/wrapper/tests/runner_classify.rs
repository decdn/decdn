use onramp::runner::{classify_exit, FetchOutcome};

fn exit(code: i32) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    std::process::ExitStatus::from_raw((code & 0xff) << 8)
}

#[test]
fn drain_marker_classifies_as_drained() {
    let out = classify_exit(
        exit(1),
        "error: channel exhausted mid-fetch: this key 0x.. is not the channel's funder ..",
    );
    assert!(matches!(out, FetchOutcome::Drained));
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
