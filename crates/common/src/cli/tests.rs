use super::*;

/// Clap builds subcommands lazily along the parsed path, so a malformed arg
/// definition (duplicate long flag, duplicate arg id) panics at *runtime* on
/// the first invocation of that subcommand rather than failing the build.
/// `debug_assert` walks the whole command tree eagerly, so it catches this
/// for every subcommand — including the ones no test parses.
///
/// This matters most for the flattened [`CommonChainArgs`]: it is embedded in
/// both `ChainArgs` and `PublishChainArgs`, so a future struct that flattens
/// two of them (or re-declares a shared flag by hand) would collide.
#[test]
fn cli_command_tree_is_clap_valid() {
    use clap::CommandFactory;
    Cli::command().debug_assert();
}

/// `--max-lane-streams` takes 1 to 64: one connection per node carries
/// every stream of the command, so the bound stays below the node's
/// per-connection stream limit.
#[test]
fn max_lane_streams_is_bounded_to_1_through_64() {
    let parse = |n: &str| {
        Cli::try_parse_from([
            "decdn",
            "bundle",
            "pull",
            "-i",
            "m.json",
            "-o",
            "out",
            "--max-lane-streams",
            n,
        ])
    };
    let lanes = |cli: Cli| match cli.command {
        Command::Bundle(BundleArgs {
            cmd: BundleCommand::Pull(args),
        }) => Some(args.max_lane_streams),
        _ => None,
    };
    assert_eq!(parse("64").ok().and_then(lanes), Some(64));
    assert_eq!(parse("1").ok().and_then(lanes), Some(1));
    assert!(parse("65").is_err(), "65 is above the bound");
    assert!(parse("0").is_err(), "0 is below the bound");
}
