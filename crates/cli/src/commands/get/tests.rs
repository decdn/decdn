use std::path::Path;

use clap::Parser as _;
use decdn_common::cli::{Cli, Command, GetArgs};

use super::{Route, literal_glob, route};
use crate::commands::manifest::build_glob_set;

const HEX: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

fn get_args(argv: &[&str]) -> GetArgs {
    let cli = Cli::try_parse_from(["decdn", "get"].iter().chain(argv)).unwrap();
    match cli.command {
        Command::Get(get) => get,
        other => panic!("parsed {other:?}"),
    }
}

fn route_of(argv: &[&str]) -> anyhow::Result<Route> {
    route(&get_args(argv))
}

fn err_of(argv: &[&str]) -> String {
    route_of(argv).unwrap_err().to_string()
}

#[test]
fn blob_uri_routes_to_fetch() {
    let uri = format!("decdn:{}?ns=7", HEX.to_uppercase());
    let Route::Fetch(f) = route_of(&[&uri, "-o", "out.bin"]).unwrap() else {
        panic!("not a fetch");
    };
    assert_eq!(f.hash, HEX);
    assert_eq!(f.output, Path::new("out.bin"));
    assert_eq!(f.namespace, Some(7));
}

#[test]
fn blob_uri_streams_to_stdout() {
    let uri = format!("decdn:{HEX}");
    let Route::Fetch(f) = route_of(&[&uri, "-o", "-"]).unwrap() else {
        panic!("not a fetch");
    };
    assert_eq!(f.output, Path::new("-"));
    assert_eq!(f.namespace, None);
}

#[test]
fn shared_flags_carry_over() {
    let uri = format!("decdn:{HEX}");
    let Route::Fetch(f) = route_of(&[&uri, "-o", "x", "--max-sources", "9"]).unwrap() else {
        panic!("not a fetch");
    };
    assert_eq!(f.common.max_sources, 9);
}

#[test]
fn bundle_uri_routes_to_bundle_pull() {
    let uri = format!("decdn:{HEX}/?ns=3");
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
    let Route::Bundle(b) = route_of(&argv).unwrap() else {
        panic!("not a bundle pull");
    };
    assert_eq!(b.hash.as_deref(), Some(HEX));
    assert!(b.input.is_none());
    assert_eq!(b.output, Path::new("dir"));
    assert_eq!(b.include, ["*.bin"]);
    assert_eq!(b.jobs, 2);
    assert_eq!(b.max_lane_streams, 4);
    assert!(b.json);
    assert_eq!(b.namespace, Some(3));
}

#[test]
fn entry_uri_includes_exactly_that_path() {
    let uri = format!("decdn:{HEX}/models/a%20b%5B1%5D.bin");
    let Route::Bundle(b) = route_of(&[&uri, "-o", "dir", "--dry-run"]).unwrap() else {
        panic!("not a bundle pull");
    };
    assert_eq!(b.include, [literal_glob("models/a b[1].bin")]);
    assert!(b.exclude.is_empty());
    assert!(b.dry_run);
}

#[test]
fn literal_glob_matches_only_its_path() {
    for path in [
        "a*.bin",
        "dir/f?le",
        "x[1].txt",
        "{a,b}.txt",
        "back\\slash",
        "plain/path.txt",
    ] {
        let set = build_glob_set(&[literal_glob(path)], "--include").unwrap();
        assert!(set.is_match(Path::new(path)), "{path} must match itself");
    }
    let star = build_glob_set(&[literal_glob("a*.bin")], "--include").unwrap();
    assert!(!star.is_match(Path::new("abc.bin")));
    let class = build_glob_set(&[literal_glob("x[1].txt")], "--include").unwrap();
    assert!(!class.is_match(Path::new("x1.txt")));
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
    let err = format!(
        "{:#}",
        route_of(&[&format!("b3:{HEX}"), "-o", "x"]).unwrap_err()
    );
    assert!(err.contains("not a decdn URI"), "{err}");
}
