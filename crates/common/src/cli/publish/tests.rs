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

fn point(args: &[&str]) -> Result<crate::cli::PointArgs, clap::Error> {
    let mut with_bin = vec!["decdn", "publish", "point"];
    with_bin.extend_from_slice(args);
    match Cli::try_parse_from(with_bin)?.command {
        crate::cli::Command::Publish(publish) => match publish.command {
            crate::cli::PublishCommand::Point(point) => Ok(point),
            other => panic!("not a point command: {other:?}"),
        },
        other => panic!("not a publish command: {other:?}"),
    }
}

#[test]
fn point_takes_a_namespace_a_uri_and_the_gateway() {
    let uri = format!("decdn:{}/", "ab".repeat(32));
    let args = point(&["7", &uri, "--gateway", "https://decdn.link"]).unwrap();
    assert_eq!(args.namespace, 7);
    assert_eq!(args.uri, uri);
    assert_eq!(args.gateway.as_str(), "https://decdn.link/");
    assert_eq!(args.seq, None);
    let args = point(&["7", &uri, "--gateway", "https://decdn.link", "--seq", "42"]).unwrap();
    assert_eq!(args.seq, Some(42));
}

#[test]
fn point_needs_the_gateway_and_rejects_namespace_zero() {
    let uri = format!("decdn:{}/", "ab".repeat(32));
    assert!(point(&["7", &uri]).is_err());
    assert!(point(&["7", "--gateway", "https://decdn.link"]).is_err());
    let err = point(&["0", &uri, "--gateway", "https://decdn.link"])
        .unwrap_err()
        .to_string();
    assert!(err.contains("0 is reserved"), "{err}");
    assert!(point(&["7", &uri, "--gateway", "not a url"]).is_err());
}
