use super::ClientFetchArgs;
use clap::Parser;
use std::time::Duration;

/// Parse `ClientFetchArgs` the way clap will at runtime, so the tests below
/// assert on the real defaults rather than a hand-built struct that could
/// drift from them.
#[derive(Debug, Parser)]
struct TestCli {
    #[command(flatten)]
    common: ClientFetchArgs,
}

fn parse(args: &[&str]) -> ClientFetchArgs {
    let mut with_bin = vec!["test"];
    with_bin.extend_from_slice(args);
    TestCli::parse_from(with_bin).common
}

/// `--keystore-password-file` reaches the field clap stores it on. Only the
/// explicit-flag direction is asserted: the arg also carries
/// `env = "DECDN_KEYSTORE_PASSWORD_FILE"`, so a developer who exports that
/// variable would see an "absent" assertion fail for the right reason.
#[test]
fn parse_keystore_password_file() {
    let parsed = parse(&["--keystore-password-file", "/abs/pw.txt"]);
    assert_eq!(
        parsed.keystore_password_file.as_deref(),
        Some(std::path::Path::new("/abs/pw.txt"))
    );
}

/// `--namespace` accepts a non-zero id, rejects the reserved `0` (the
/// `NO_NAMESPACE` sentinel — users omit the flag instead), and rejects
/// non-numeric input. Mirrors the publish-side `assign` parser's coverage.
#[test]
fn parse_fetch_namespace_id_validates() {
    assert_eq!(super::parse_fetch_namespace_id("7"), Ok(7));
    assert_eq!(super::parse_fetch_namespace_id("1"), Ok(1));

    let zero = super::parse_fetch_namespace_id("0").expect_err("0 must be rejected");
    assert!(
        zero.contains(">= 1"),
        "0 error should point to the floor: {zero}"
    );

    let nan = super::parse_fetch_namespace_id("abc").expect_err("non-numeric must be rejected");
    assert!(
        nan.contains("invalid namespace id"),
        "non-numeric error should name the field: {nan}"
    );
}

#[test]
fn give_up_after_is_unset_by_default_and_parses_whole_seconds() {
    let args = parse(&[]);
    assert_eq!(args.give_up_after(), None);
    let args = parse(&["--give-up-after-secs", "90"]);
    assert_eq!(args.give_up_after(), Some(Duration::from_secs(90)));
    assert!(
        TestCli::try_parse_from(["test", "--give-up-after-secs", "0"]).is_err(),
        "a zero limit would give up before the first byte"
    );
}

#[test]
fn removed_recovery_flags_are_rejected() {
    for flag in [
        "--stall-timeout-ms",
        "--unit-deadline-ms",
        "--min-throughput-bps",
        "--multi-source-min-bytes",
    ] {
        let parsed = TestCli::try_parse_from(["test", flag, "1"]);
        assert!(parsed.is_err(), "{flag} must be gone");
    }
    // The multi-source switches took no value, so a lone flag is what a
    // parse of the old command line would have seen.
    for flag in ["--multi-source", "--no-multi-source"] {
        let parsed = TestCli::try_parse_from(["test", flag]);
        assert!(parsed.is_err(), "{flag} must be gone");
    }
}

/// `--timeout-ms` bounds discovery and defaults to an hour, far above any
/// honest registry read.
#[test]
fn the_default_discovery_bound_is_an_hour() {
    assert_eq!(parse(&[]).discovery_cap(), Duration::from_hours(1));
}

/// `--region` is validated at parse time, not silently discarded. Before
/// this, `--region usa` parsed fine and then failed `Region::parse` deep in
/// discovery, so the user got round-robin selection with no hint that the
/// flag they passed had been thrown away.
#[test]
fn an_unrecognized_region_flag_is_rejected_not_ignored() {
    let err = TestCli::try_parse_from(["test", "--region", "usa"])
        .expect_err("`usa` is not an ISO 3166-1 alpha-2 code");
    let msg = err.to_string();
    assert!(
        msg.contains("ISO 3166-1"),
        "the error names the format: {msg}"
    );

    // Any spelling of a valid code is accepted, and normalized on the way in
    // so discovery compares canonical values.
    assert_eq!(parse(&["--region", " us "]).region.as_deref(), Some("US"));
    assert_eq!(parse(&["--region", "De"]).region.as_deref(), Some("DE"));
    assert_eq!(parse(&[]).region, None, "the flag stays optional");
}

/// Discovery must ALWAYS have a non-zero bound. `--timeout-ms` supplies it
/// (`discovery_cap`) and is a non-optional `Duration` — never absent — so the only thing
/// left for a test to pin is that it cannot be zero, which is clap's job. (The delivery
/// pull itself carries no overall wall-clock cap; a progressing pull is bounded only by
/// its stall window, #1134.)
#[test]
fn discovery_always_has_a_nonzero_bound() {
    assert!(
        !parse(&[]).discovery_cap().is_zero(),
        "an unbounded discovery could sit through the registry read's full retry schedule"
    );
    assert!(
        TestCli::try_parse_from(["test", "--timeout-ms", "0"]).is_err(),
        "a zero --timeout-ms would abort discovery on the first poll"
    );
}

/// `--timeout-ms` bounds discovery (#1349), so a small `--timeout-ms` does not sit
/// through the registry read's full retry schedule.
#[test]
fn discovery_is_bounded_by_the_timeout_flag() {
    let c = parse(&["--timeout-ms", "5000"]);
    assert_eq!(c.discovery_cap(), Duration::from_secs(5));
    assert!(parse(&[]).validate().is_ok(), "the defaults are legal");
}

/// `--provider-address` alone (no `--node-id`) names no node to dial and is
/// rejected by `validate()`, even though clap itself admits it (the `requires`
/// pairing runs only from `--node-id`'s side, so this direction needs its own
/// runtime check).
#[test]
fn provider_address_alone_is_rejected_by_validate() {
    let addr = "0x0000000000000000000000000000000000000001";
    let c = parse(&["--provider-address", addr]);
    let err = c
        .validate()
        .expect_err("provider-address alone names no node");
    let msg = err.to_string();
    assert!(msg.contains("--node-id"), "{msg}");
}

#[test]
fn provider_address_with_node_id_is_the_unchanged_auto_open_path() {
    let addr = "0x0000000000000000000000000000000000000001";
    let c = parse(&["--node-id", "n", "--provider-address", addr]);
    assert!(c.validate().is_ok(), "explicit node + provider still valid");
}

#[test]
fn node_id_alone_is_still_a_clap_error() {
    // `--node-id` keeps `requires = "provider_address"`, enforced at the clap layer.
    assert!(
        TestCli::try_parse_from(["test", "--node-id", "n"]).is_err(),
        "--node-id without --provider-address is a parse error"
    );
}

/// No capability flag => the self-owned path (`None`), unchanged.
#[test]
fn capability_token_absent_is_none() {
    assert_eq!(parse(&[]).resolve_capability_token().unwrap(), None);
}

/// `--capability` returns the inline token verbatim.
#[test]
fn capability_token_inline_is_returned() {
    let token = parse(&["--capability", "dcap1:abc"])
        .resolve_capability_token()
        .unwrap();
    assert_eq!(token.as_deref(), Some("dcap1:abc"));
}

/// `--rediscover` bypasses the store fast path: absent it defaults
/// to `false` (the fast path is eligible), and the flag itself carries no
/// value — presence alone flips it to `true`.
#[test]
fn rediscover_defaults_false_and_flag_sets_true() {
    assert!(!parse(&[]).rediscover, "defaults to false");
    assert!(parse(&["--rediscover"]).rediscover, "flag sets it true");
}

/// `--capability-file` returns the file's trimmed contents; the two forms are
/// mutually exclusive at the clap layer.
#[test]
fn capability_token_from_file_is_trimmed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cap.txt");
    std::fs::write(&path, "  dcap1:fromfile\n").unwrap();
    let token = parse(&["--capability-file", path.to_str().unwrap()])
        .resolve_capability_token()
        .unwrap();
    assert_eq!(token.as_deref(), Some("dcap1:fromfile"));

    assert!(
        TestCli::try_parse_from(["test", "--capability", "dcap1:a", "--capability-file", "x",])
            .is_err(),
        "--capability and --capability-file are mutually exclusive"
    );
}
