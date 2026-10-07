use super::*;
use clap::Parser;

#[derive(Parser)]
struct Wrap {
    #[command(flatten)]
    args: OriginImportArgs,
}

#[test]
fn chunk_avg_is_none_by_default() {
    // No clap default: `None` = unset, so `decdn origin import` can reject
    // `--chunk-avg` given without `--optimize`. The 4 MiB default is applied
    // downstream, not by clap.
    let w = Wrap::try_parse_from(["x", "-i", "d", "--to", "/o"]).unwrap();
    assert_eq!(w.args.chunk_avg, None);
    assert!(!w.args.optimize);
    assert!(!w.args.dry_run);
}

#[test]
fn to_is_optional() {
    let w = Wrap::try_parse_from(["x", "-i", "d", "--dry-run"]).unwrap();
    assert!(w.args.to.is_none());
    assert!(w.args.dry_run);
}

#[test]
fn subfolder_is_none_by_default() {
    let w = Wrap::try_parse_from(["x", "-i", "d", "--to", "/o"]).unwrap();
    assert_eq!(w.args.subfolder, None);
}

#[test]
fn parses_subfolder() {
    let w =
        Wrap::try_parse_from(["x", "-i", "d", "--to", "/o", "--subfolder", "assets/v1"]).unwrap();
    assert_eq!(w.args.subfolder.as_deref(), Some("assets/v1"));
}

#[test]
fn parses_optimize_and_sizes() {
    let w = Wrap::try_parse_from([
        "x",
        "-i",
        "d",
        "--to",
        "/o",
        "--optimize",
        "--chunk-avg",
        "2MiB",
        "--chunk-min",
        "1MiB",
        "--chunk-max",
        "4MiB",
    ])
    .unwrap();
    assert!(w.args.optimize);
    assert_eq!(w.args.chunk_avg, Some(2 * 1024 * 1024));
    assert_eq!(w.args.chunk_min, Some(1024 * 1024));
    assert_eq!(w.args.chunk_max, Some(4 * 1024 * 1024));
}
