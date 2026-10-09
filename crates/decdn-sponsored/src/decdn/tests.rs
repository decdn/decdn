use super::*;

fn sample() -> PullArgs {
    PullArgs {
        hash: "ab".repeat(32),
        output: PathBuf::from("out"),
        namespace: NonZeroU64::new(1),
        capability_file: PathBuf::from("/s/capability"),
        keystore: PathBuf::from("/s/keystore.json"),
        password_file: PathBuf::from("/s/password"),
        data_dir: PathBuf::from("/root/decdn"),
        rpc_url: "http://rpc".into(),
        payment_pool: Address::repeat_byte(0x01),
        capacity_bond: Some(Address::repeat_byte(0x02)),
        slash_judge: None,
        chain_id: 421_614,
    }
}

fn value_after(args: &[OsString], flag: &str) -> Option<String> {
    let i = args.iter().position(|a| a == flag)?;
    args.get(i + 1).map(|v| v.to_string_lossy().into_owned())
}

#[test]
fn builds_bundle_pull_with_capability_file() {
    let args = sample().to_args();
    assert_eq!(args[0], "bundle");
    assert_eq!(args[1], "pull");
    assert_eq!(value_after(&args, "--hash").unwrap(), "ab".repeat(32));
    assert_eq!(
        value_after(&args, "--capability-file").unwrap(),
        "/s/capability"
    );
    assert_eq!(
        value_after(&args, "--keystore-password-file").unwrap(),
        "/s/password"
    );
    assert_eq!(
        value_after(&args, "--capacity-bond-address").unwrap(),
        Address::repeat_byte(0x02).to_string()
    );
    assert!(!args.iter().any(|a| a == "--capability"));
    assert_eq!(value_after(&args, "--namespace").unwrap(), "1");
    assert_eq!(value_after(&args, "--data-dir").unwrap(), "/root/decdn");
    assert_eq!(
        value_after(&args, "--keystore").unwrap(),
        "/s/keystore.json"
    );
    assert!(!args.iter().any(|a| a == "--slash-judge-address"));
}
