use clap::Parser as _;
use decdn_client::uri::UriTarget;
use decdn_common::cli::{Cli, Command, GetArgs};

use std::path::PathBuf;

use crate::commands::interrupt::{Interrupted, stop_message};

use super::{Destination, parse_and_check, streams_one_blob, with_resume_hint};

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
    let err = err_of(&[&format!("decdn:{HEX}/"), "-o", "-"]);
    assert!(
        err.contains("a whole bundle writes to a directory"),
        "{err}"
    );
}

/// Whether `flags` run the blob fetch.
fn routes_to_blob_fetch(flags: &[&str]) -> bool {
    let args = get_args(flags);
    streams_one_blob(&args, &parse_and_check(&args).unwrap())
}

#[test]
fn entry_uri_streams_to_stdout_as_one_blob() {
    let entry = format!("decdn:{HEX}/sintel/Sintel.mkv?ns=1");
    assert!(routes_to_blob_fetch(&[&entry, "-o", "-"]));
    assert!(!routes_to_blob_fetch(&[&entry, "-o", "dir"]));
    assert!(!routes_to_blob_fetch(&[&entry]));
    let blob = format!("decdn:{HEX}");
    assert!(routes_to_blob_fetch(&[&blob, "-o", "-"]));
    assert!(routes_to_blob_fetch(&[&blob]));
    assert!(!routes_to_blob_fetch(&[
        &format!("decdn:{HEX}/"),
        "-o",
        "dir"
    ]));
}

#[test]
fn bundle_flags_rejected_on_entry_uri_to_stdout() {
    let uri = format!("decdn:{HEX}/a.txt");
    let err = err_of(&[&uri, "-o", "-", "--jobs", "2", "--overwrite"]);
    assert!(err.contains("--jobs, --overwrite"), "{err}");
    assert!(err.contains("-o -"), "{err}");
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

#[test]
fn output_defaults_follow_the_uri_target() {
    let cases = [
        (
            format!("decdn:{HEX}"),
            format!("./{HEX}"),
            None,
            format!("./{HEX}"),
        ),
        (
            format!("decdn:{HEX}/"),
            ".".to_owned(),
            None,
            ".".to_owned(),
        ),
        (
            format!("decdn:{HEX}/models/a.bin"),
            ".".to_owned(),
            Some("a.bin"),
            "./a.bin".to_owned(),
        ),
        (
            format!("decdn:{HEX}/top.bin"),
            ".".to_owned(),
            Some("top.bin"),
            "./top.bin".to_owned(),
        ),
    ];
    for (uri, path, entry_name, shown) in cases {
        let args = get_args(&[&uri]);
        let parsed = parse_and_check(&args).unwrap();
        let dest = Destination::of(&args, &parsed);
        assert_eq!(dest.path, PathBuf::from(&path), "{uri}");
        assert_eq!(dest.entry_name.as_deref(), entry_name, "{uri}");
        assert_eq!(dest.shown(), PathBuf::from(&shown), "{uri}");
    }
}

#[test]
fn an_explicit_output_is_kept_as_given() {
    let uri = format!("decdn:{HEX}/models/a.bin");
    let args = get_args(&[&uri, "-o", "dir"]);
    let parsed = parse_and_check(&args).unwrap();
    let dest = Destination::of(&args, &parsed);
    assert_eq!(
        dest,
        Destination {
            path: PathBuf::from("dir"),
            entry_name: None,
        }
    );
}

/// The line `main` prints for an interrupt that `get` returns for `argv`.
fn interrupt_line(argv: &[&str]) -> String {
    let err = with_resume_hint::<()>(&get_args(argv), Err(Interrupted.into())).unwrap_err();
    stop_message(&err).unwrap()
}

/// A default destination and a relative `-o` resolve against the current
/// directory, so an interrupt names it on the blob and the bundle route alike.
#[test]
fn an_interrupt_names_the_current_directory_for_a_relative_destination() {
    let named = format!(
        "interrupted; run the same command again from {} to resume",
        std::env::current_dir().unwrap().display()
    );
    let blob = format!("decdn:{HEX}");
    let bundle = format!("decdn:{HEX}/");
    assert_eq!(interrupt_line(&[&blob]), named);
    assert_eq!(interrupt_line(&[&bundle]), named);
    assert_eq!(interrupt_line(&[&blob, "-o", "out.bin"]), named);
    assert_eq!(interrupt_line(&[&bundle, "-o", "sub/dir"]), named);
}

/// An absolute `-o` holds its resume state wherever a re-run starts, and `-o -`
/// keeps none: neither names a directory.
#[test]
fn an_interrupt_names_no_directory_for_an_absolute_or_stdout_destination() {
    let plain = "interrupted; run the same command again to resume";
    let absolute = std::env::temp_dir().join("out.bin");
    let absolute = absolute.to_str().unwrap();
    let blob = format!("decdn:{HEX}");
    assert_eq!(interrupt_line(&[&blob, "-o", absolute]), plain);
    assert_eq!(interrupt_line(&[&blob, "-o", "-"]), plain);
    assert_eq!(
        interrupt_line(&[&format!("decdn:{HEX}/model.bin"), "-o", "-"]),
        plain
    );
}
