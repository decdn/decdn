use super::GatewayArgs;
use crate::cli::{Cli, Command};
use clap::Parser as _;

fn parse(args: &[&str]) -> Result<GatewayArgs, clap::Error> {
    let mut with_bin = vec!["decdn", "gateway"];
    with_bin.extend_from_slice(args);
    match Cli::try_parse_from(with_bin)?.command {
        Command::Gateway(args) => Ok(*args),
        other => panic!("not a gateway command: {other:?}"),
    }
}

#[test]
fn a_public_gateway_parses_its_url_and_allowance() {
    let args = parse(&[
        "--public-url",
        "https://decdn.link",
        "--allowance-bytes-per-day",
        "512MiB",
    ])
    .unwrap();
    assert_eq!(
        args.public.public_url.as_ref().map(ToString::to_string),
        Some("https://decdn.link/".to_owned())
    );
    assert_eq!(args.public.allowance_bytes_per_day, 512 << 20);
}

#[test]
fn the_allowance_defaults_to_one_gib_and_needs_the_public_url() {
    let args = parse(&[]).unwrap();
    assert!(args.public.public_url.is_none());
    assert_eq!(args.public.allowance_bytes_per_day, 1 << 30);
    assert!(parse(&["--allowance-bytes-per-day", "1"]).is_err());
}

#[test]
fn a_malformed_public_url_or_allowance_is_refused() {
    assert!(parse(&["--public-url", "not a url"]).is_err());
    for bad in ["", "x", "1XiB"] {
        assert!(
            parse(&[
                "--public-url",
                "https://decdn.link",
                "--allowance-bytes-per-day",
                bad
            ])
            .is_err(),
            "{bad:?}"
        );
    }
}
