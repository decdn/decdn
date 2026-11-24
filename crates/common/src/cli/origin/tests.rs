use super::*;
use clap::Parser;

#[derive(Parser)]
struct Wrap {
    #[command(flatten)]
    args: OriginImportArgs,
}

#[test]
fn dry_run_is_off_by_default() {
    let w = Wrap::try_parse_from(["x", "-i", "d", "--to", "/o"]).unwrap();
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
