use clap::Parser;

use crate::cli::Cli;

#[test]
fn assign_needs_at_least_one_operator() {
    let err = Cli::try_parse_from(["decdn", "publish", "assign", "7"]);
    assert!(err.is_err());
}

#[test]
fn assign_rejects_reserved_namespace_zero() {
    let err = Cli::try_parse_from(["decdn", "publish", "assign", "0", "0xabc"])
        .unwrap_err()
        .to_string();
    assert!(err.contains("0 is reserved"), "{err}");
}

#[test]
fn revoke_takes_exactly_one_operator_and_rejects_namespace_zero() {
    assert!(Cli::try_parse_from(["decdn", "publish", "revoke", "7"]).is_err());
    assert!(Cli::try_parse_from(["decdn", "publish", "revoke", "7", "0xa", "0xb"]).is_err());
    let err = Cli::try_parse_from(["decdn", "publish", "revoke", "0", "0xabc"])
        .unwrap_err()
        .to_string();
    assert!(err.contains("0 is reserved"), "{err}");
}
