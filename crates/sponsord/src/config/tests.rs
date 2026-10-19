use super::*;

const POOL: &str = "0x0000000000000000000000000000000000000001";

fn args(extra: &[&str]) -> Result<Args, clap::Error> {
    let pool_id = format!("0x{}", "11".repeat(32));
    let token = "a".repeat(32);
    let mut argv = vec![
        "sponsord",
        "--rpc-url",
        "http://localhost:8545",
        "--payment-pool",
        POOL,
        "--pool-id",
        &pool_id,
        "--treasury-keystore",
        "/tmp/ks.json",
        "--treasury-password",
        "pw",
    ];
    if !extra.iter().any(|a| a.starts_with("--api-token")) {
        argv.extend(["--api-token", &token]);
    }
    argv.extend(extra);
    Args::try_parse_from(argv)
}

#[test]
fn reads_required_and_defaults() {
    let cfg = DaemonConfig::from_args(args(&[]).unwrap()).unwrap();
    assert_eq!(cfg.bind, "127.0.0.1:8090".parse().unwrap());
    assert_eq!(cfg.chain.chain_id, 421_614);
    assert_eq!(cfg.chain.payment_pool, POOL.parse::<Address>().unwrap());
    assert_eq!(cfg.limits.max_spending_cap, MicroUsdc(5_000_000));
    assert_eq!(cfg.limits.max_ttl_secs, 172_800);
    assert_eq!(cfg.keeper.low_water, MicroUsdc(20_000_000));
    assert_eq!(cfg.keeper.interval, Duration::from_hours(1));
}

#[test]
fn parse_errors_name_the_setting() {
    let err = args(&["--max-ttl-secs", "abc"]).unwrap_err().to_string();
    assert!(err.contains("--max-ttl-secs"), "{err}");
}

#[test]
fn refuses_a_short_api_token_and_requires_one() {
    let short = "a".repeat(31);
    let err = DaemonConfig::from_args(args(&["--api-token", &short]).unwrap()).unwrap_err();
    assert!(err.to_string().contains("at least 32"), "{err}");
}

#[test]
fn secrets_can_come_from_files_but_not_from_both() {
    let dir = std::env::temp_dir().join(format!("sponsord-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let token_file = dir.join("token");
    std::fs::write(&token_file, format!("{}\n", "b".repeat(40))).unwrap();
    let path = token_file.to_str().unwrap();

    let cfg = DaemonConfig::from_args(args(&["--api-token-file", path]).unwrap()).unwrap();
    assert_eq!(cfg.api_token.expose(), "b".repeat(40));
    assert!(args(&["--api-token-file", path, "--api-token", &"c".repeat(32)]).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn debug_redacts_secrets() {
    let cfg = DaemonConfig::from_args(args(&[]).unwrap()).unwrap();
    let shown = format!("{cfg:?}");
    assert!(!shown.contains(&"a".repeat(32)));
    assert!(!shown.contains("\"pw\""));
    assert!(!shown.contains("localhost:8545"), "the RPC URL: {shown}");
    assert!(shown.contains("<redacted>"));
}

/// `--help` prints `[env: NAME=value]` for a set variable unless its value
/// is hidden; the RPC URL often carries an API key (#38).
#[test]
fn help_hides_secret_env_values() {
    let cmd = <Args as clap::CommandFactory>::command();
    for id in ["api_token", "treasury_password", "rpc_url"] {
        let arg = cmd.get_arguments().find(|a| a.get_id() == id).unwrap();
        assert!(arg.is_hide_env_values_set(), "{id}");
    }
}

#[test]
fn zero_limits_are_refused() {
    let err = DaemonConfig::from_args(args(&["--max-ttl-secs", "0"]).unwrap()).unwrap_err();
    assert!(err.to_string().contains("above zero"), "{err}");
}
