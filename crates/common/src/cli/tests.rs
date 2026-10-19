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
            "get",
            "decdn:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262/",
            "-o",
            "out",
            "--max-lane-streams",
            n,
        ])
    };
    let lanes = |cli: Cli| match cli.command {
        Command::Get(args) => args.max_lane_streams,
        _ => None,
    };
    assert_eq!(parse("64").ok().and_then(lanes), Some(64));
    assert_eq!(parse("1").ok().and_then(lanes), Some(1));
    assert!(parse("65").is_err(), "65 is above the bound");
    assert!(parse("0").is_err(), "0 is below the bound");
}

/// `--json` is a per-subcommand flag, declared on each args struct that renders
/// JSON. [`json_requested`] finds it at whatever depth the parsed command
/// nests it, so the post-run update notice can stay out of machine-read output
/// without a hand-maintained list of JSON-capable commands.
#[test]
fn json_requested_sees_the_flag_at_every_depth() {
    use clap::CommandFactory;
    for argv in [
        &["decdn", "probe", "--json", "--node-id", "x", "--hash", "h"][..],
        &["decdn", "node", "status", "--json"][..],
        &["decdn", "pool", "list", "--json"][..],
        &["decdn", "get", "decdn:abc", "--json"][..],
    ] {
        let matches = Cli::command().get_matches_from(argv);
        assert!(json_requested(&matches), "{argv:?} asked for JSON");
    }
    for argv in [
        &["decdn", "probe", "--node-id", "x", "--hash", "h"][..],
        &["decdn", "whoami"][..],
        &["decdn", "node", "status"][..],
    ] {
        let matches = Cli::command().get_matches_from(argv);
        assert!(!json_requested(&matches), "{argv:?} did not ask for JSON");
    }
}

/// `self-update --check` reports without downloading; the bare command applies.
#[test]
fn self_update_parses_check() {
    #[derive(clap::Parser)]
    struct Wrapper {
        #[command(flatten)]
        args: SelfUpdateArgs,
    }
    let bare = Wrapper::parse_from(["x"]);
    assert!(!bare.args.check);
    let check = Wrapper::parse_from(["x", "--check"]);
    assert!(check.args.check);
}
