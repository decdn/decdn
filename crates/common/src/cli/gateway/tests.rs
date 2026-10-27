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

#[test]
fn metrics_listen_parses_with_a_public_url() {
    let args = parse(&[
        "--public-url",
        "https://decdn.link",
        "--metrics-listen",
        "127.0.0.1:9100",
    ])
    .unwrap();
    assert_eq!(
        args.metrics_listen.map(|a| a.to_string()),
        Some("127.0.0.1:9100".to_owned())
    );
    assert!(parse(&[]).unwrap().metrics_listen.is_none());
}

/// A loopback gateway takes `--metrics-listen` too: every gateway counts.
#[test]
fn metrics_listen_parses_without_public_url() {
    let args = parse(&["--metrics-listen", "127.0.0.1:9100"]).unwrap();
    assert_eq!(
        args.metrics_listen.map(|a| a.to_string()),
        Some("127.0.0.1:9100".to_owned())
    );
}

#[test]
fn a_namespace_allowance_parses_as_ns_equals_bytes_and_repeats() {
    let args = parse(&[
        "--public-url",
        "https://decdn.link",
        "--allowance",
        "7=20GiB",
        "--allowance",
        "9=1024",
    ])
    .unwrap();
    assert_eq!(args.public.allowance, vec![(7, 20 << 30), (9, 1024)]);
    assert!(
        parse(&["--public-url", "https://decdn.link"])
            .unwrap()
            .public
            .allowance
            .is_empty()
    );
    for bad in ["7", "=1GiB", "0=1GiB", "07=1GiB", "x=1GiB", "7=", "7=1XiB"] {
        assert!(
            parse(&["--public-url", "https://decdn.link", "--allowance", bad]).is_err(),
            "{bad:?}"
        );
    }
    assert!(parse(&["--allowance", "7=1GiB"]).is_err());
}

#[test]
fn the_publisher_registry_address_needs_the_public_url() {
    let args = parse(&[
        "--public-url",
        "https://decdn.link",
        "--publisher-registry-address",
        "0x00000000000000000000000000000000000000aa",
    ])
    .unwrap();
    assert_eq!(
        args.public.publisher_registry_address.as_deref(),
        Some("0x00000000000000000000000000000000000000aa")
    );
    assert!(
        parse(&[])
            .unwrap()
            .public
            .publisher_registry_address
            .is_none()
    );
    assert!(
        parse(&[
            "--publisher-registry-address",
            "0x00000000000000000000000000000000000000aa"
        ])
        .is_err()
    );
}

/// The listener serves the operator with no token on a public gateway, so it
/// takes a loopback address only, refused at parse time.
#[test]
fn metrics_listen_takes_a_loopback_address_only() {
    for given in ["127.0.0.1:9100", "[::1]:9100", "127.0.0.2:9100"] {
        let args = parse(&["--metrics-listen", given]).unwrap();
        assert_eq!(
            args.metrics_listen.map(|a| a.to_string()),
            Some(given.to_owned())
        );
    }
    for given in [
        "0.0.0.0:9100",
        "192.168.1.5:9100",
        "[::]:9100",
        "10.0.0.1:9100",
    ] {
        let Err(err) = parse(&["--metrics-listen", given]) else {
            panic!("{given} parsed");
        };
        assert!(err.to_string().contains("loopback"), "{given}: {err}");
    }
}
