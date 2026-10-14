use clap::Parser as _;
use decdn_client::uri::UriTarget;
use decdn_common::cli::{Cli, Command, GetArgs};

use super::parse_and_check;

const HEX: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

fn get_args(argv: &[&str]) -> GetArgs {
    let cli = Cli::try_parse_from(["decdn", "get"].iter().chain(argv)).unwrap();
    match cli.command {
        Command::Get(get) => *get,
        other => panic!("parsed {other:?}"),
    }
}

fn err_of(argv: &[&str]) -> String {
    format!("{:#}", parse_and_check(&get_args(argv)).unwrap_err())
}

#[test]
fn blob_uri_takes_a_file_or_stdout() {
    let uri = format!("decdn:{}?ns=7", HEX.to_uppercase());
    for out in ["out.bin", "-"] {
        let parsed = parse_and_check(&get_args(&[&uri, "-o", out])).unwrap();
        assert_eq!(parsed.target, UriTarget::Blob);
        assert_eq!(parsed.namespace, Some(7));
        assert_eq!(parsed.to_string(), format!("decdn:{HEX}?ns=7"));
    }
}

#[test]
fn bundle_uri_takes_the_bundle_flags() {
    let uri = format!("decdn:{HEX}/");
    let argv = [
        &uri,
        "-o",
        "dir",
        "--include",
        "*.bin",
        "--jobs",
        "2",
        "--json",
    ];
    let parsed = parse_and_check(&get_args(&argv)).unwrap();
    assert_eq!(parsed.target, UriTarget::Bundle);
}

#[test]
fn entry_uri_takes_the_run_flags() {
    let uri = format!("decdn:{HEX}/models/a%20b%5B1%5D.bin");
    let argv = [&uri, "-o", "dir", "--dry-run", "--overwrite", "--jobs", "1"];
    let parsed = parse_and_check(&get_args(&argv)).unwrap();
    assert_eq!(
        parsed.target,
        UriTarget::BundleEntry("models/a b[1].bin".to_owned())
    );
}

#[test]
fn shared_flags_reach_the_args() {
    let uri = format!("decdn:{HEX}");
    assert_eq!(
        get_args(&[&uri, "-o", "x", "--max-sources", "9"])
            .common
            .max_sources,
        9
    );
}

#[test]
fn bundle_flags_rejected_on_blob_uri() {
    let uri = format!("decdn:{HEX}");
    let err = err_of(&[&uri, "-o", "x", "--jobs", "2", "--json"]);
    assert!(err.contains("--jobs, --json"), "{err}");
    assert!(err.contains("bundle URI"), "{err}");
}

#[test]
fn entry_filters_rejected_on_entry_uri() {
    let uri = format!("decdn:{HEX}/a.txt");
    let err = err_of(&[&uri, "-o", "dir", "--exclude", "*", "--select"]);
    assert!(err.contains("--exclude, --select"), "{err}");
}

#[test]
fn stdout_rejected_on_bundle_uri() {
    for uri in [format!("decdn:{HEX}/"), format!("decdn:{HEX}/a.txt")] {
        let err = err_of(&[&uri, "-o", "-"]);
        assert!(err.contains("stdout"), "{err}");
    }
}

#[test]
fn bad_uri_is_an_error() {
    let err = err_of(&[&format!("b3:{HEX}"), "-o", "x"]);
    assert!(err.contains("not a decdn URI"), "{err}");
}

#[test]
fn the_old_commands_are_gone() {
    let argvs: [&[&str]; 2] = [
        &["decdn", "fetch", "--hash", HEX, "-o", "x"],
        &["decdn", "bundle", "pull", "--hash", HEX, "-o", "x"],
    ];
    for argv in argvs {
        assert!(Cli::try_parse_from(argv).is_err(), "{argv:?}");
    }
}
