use alloy::primitives::address;

use super::*;

// Distinct valid 20-byte addresses standing in for the placeholder
// strings ("0xFLAG"/"0xCONFIG"/…). `resolve*` parse their address fields,
// so the fixtures must be real addresses; the mnemonic last byte maps back to
// the old placeholder. Inputs use `ADDR.to_string()` (round-trips through
// `parse`), expectations compare the `Address` directly.
const FLAG_ADDR: Address = address!("0x00000000000000000000000000000000000000F1");
const CONFIG_ADDR: Address = address!("0x00000000000000000000000000000000000000C0");
const OA_ADDR: Address = address!("0x000000000000000000000000000000000000000A");

/// The wrapper's own contract: it tilde-expands `password_file`, and hands
/// back what `eth_identity::standard_sources` builds from it. Expansion is
/// checked against a path assembled independently of `expand_tilde`, so
/// dropping the `map(expand_tilde)` fails here rather than comparing the
/// raw path with itself. `HOME` gates that half — `expand_tilde` returns
/// `~/pw.txt` unchanged where no home directory is available — and CI
/// always has one.
///
/// Matching the whole `[Env, File, Prompt]` shape is deliberate
/// redundancy. `standard_sources` owns the ordering and pins it in its own
/// crate; re-checking it at the boundary catches a wrapper that stops
/// delegating and rebuilds the list by hand.
#[test]
fn password_sources_expands_tilde_and_delegates() {
    let sources = password_sources(Some(Path::new("~/pw.txt")), PasswordUse::Unlock);
    let want = std::env::var_os("HOME").map(|home| PathBuf::from(home).join("pw.txt"));
    assert!(
        matches!(
            sources.as_slice(),
            [
                PasswordSource::Env(name),
                PasswordSource::File(p),
                PasswordSource::Prompt {
                    usage: PasswordUse::Unlock
                },
            ] if *name == eth_identity::KEYSTORE_PASSWORD_ENV
                && want.as_ref().is_none_or(|want| p == want)
        ),
        "got: {sources:?}, wanted file {want:?}"
    );

    // An already-expanded path arrives untouched: `fetch` and `pool` hand
    // the wrapper a path their own `resolve_chain` expanded, and a second
    // pass must not rewrite it.
    let absolute = password_sources(Some(Path::new("/abs/pw.txt")), PasswordUse::Unlock);
    assert!(
        matches!(
            absolute.as_slice(),
            [_, PasswordSource::File(p), _] if p == Path::new("/abs/pw.txt")
        ),
        "got: {absolute:?}"
    );

    let without = password_sources(None, PasswordUse::Create);
    assert!(
        matches!(
            without.as_slice(),
            [
                PasswordSource::Env(_),
                PasswordSource::Prompt {
                    usage: PasswordUse::Create
                },
            ]
        ),
        "got: {without:?}"
    );
}

fn empty_chain() -> cli::ChainArgs {
    cli::ChainArgs {
        common: cli::CommonChainArgs {
            config: None,
            rpc_url: None,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/decdn-test")),
            keystore_password_file: None,
            dry_run: true,
            json: false,
        },
        capacity_bond_address: None,
    }
}

fn file_with(bc: FileBlockchain) -> FileConfig {
    FileConfig {
        blockchain: Some(bc),
        identity: None,
        network: None,
    }
}

#[test]
fn bind_port_reads_the_network_section() {
    let file: FileConfig = toml::from_str("[network]\nbind_port = 5000\n").unwrap();
    assert_eq!(file.network.and_then(|n| n.bind_port), Some(5000));
}

#[test]
fn flags_override_config() {
    let mut chain = empty_chain();
    chain.common.rpc_url = Some("http://flag:8545".to_string());
    chain.capacity_bond_address = Some(FLAG_ADDR.to_string());
    chain.common.chain_id = Some(99);
    let file = file_with(FileBlockchain {
        rpc_url: Some("http://config:8545".to_string()),
        chain_id: Some(1),
        capacity_bond_address: Some(CONFIG_ADDR.to_string()),
        eth_keystore: None,
        publisher_registry_address: None,
        origin_assignment_address: None,
        slash_appeal_address: None,
    });
    let r = resolve(&chain, &file).unwrap();
    assert_eq!(r.rpc_url, "http://flag:8545");
    assert_eq!(r.capacity_bond_address, FLAG_ADDR);
    assert_eq!(r.chain_id, 99);
}

#[test]
fn config_fills_unset_flags() {
    let chain = empty_chain();
    let file = file_with(FileBlockchain {
        rpc_url: Some("http://config:8545".to_string()),
        chain_id: None,
        capacity_bond_address: Some(CONFIG_ADDR.to_string()),
        eth_keystore: Some(PathBuf::from("/keys/ks.json")),
        publisher_registry_address: None,
        origin_assignment_address: None,
        slash_appeal_address: None,
    });
    let r = resolve(&chain, &file).unwrap();
    assert_eq!(r.rpc_url, "http://config:8545");
    assert_eq!(r.capacity_bond_address, CONFIG_ADDR);
    // chain_id absent everywhere → default.
    assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
    assert_eq!(r.keystore, PathBuf::from("/keys/ks.json"));
}

#[test]
fn keystore_defaults_under_data_dir() {
    let chain = empty_chain();
    let file = file_with(FileBlockchain {
        rpc_url: Some("http://x".to_string()),
        chain_id: None,
        capacity_bond_address: Some(CONFIG_ADDR.to_string()),
        eth_keystore: None,
        publisher_registry_address: None,
        origin_assignment_address: None,
        slash_appeal_address: None,
    });
    let r = resolve(&chain, &file).unwrap();
    assert_eq!(r.keystore, PathBuf::from("/tmp/decdn-test/keystore.json"));
}

#[test]
fn missing_required_fields_error() {
    let chain = empty_chain();
    let err = resolve(&chain, &FileConfig::default()).unwrap_err();
    assert!(err.to_string().contains("rpc_url not set"), "{err}");
}

#[test]
fn resolve_appeal_parses_flag_address() {
    let mut chain = empty_chain();
    chain.common.rpc_url = Some("http://x".to_string());
    let flag = FLAG_ADDR.to_string();
    let r = resolve_appeal(&chain.common, Some(flag.as_str()), &FileConfig::default()).unwrap();
    assert_eq!(r.slash_appeal_address, FLAG_ADDR);
}

#[test]
fn resolve_appeal_rejects_zero_address() {
    let mut chain = empty_chain();
    chain.common.rpc_url = Some("http://x".to_string());
    let zero = Address::ZERO.to_string();
    let err =
        resolve_appeal(&chain.common, Some(zero.as_str()), &FileConfig::default()).unwrap_err();
    assert!(
        err.to_string().contains("must not be the zero address"),
        "{err}"
    );
}

#[test]
fn resolve_rejects_zero_capacity_bond_address() {
    // The zero address parses cleanly but is never a real deployment; the
    // shared guard rejects it at resolve time rather than as an opaque
    // `CapacityBond` revert later (#1153).
    let mut chain = empty_chain();
    chain.common.rpc_url = Some("http://x".to_string());
    chain.capacity_bond_address = Some(Address::ZERO.to_string());
    let err = resolve(&chain, &FileConfig::default()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("capacity_bond_address"), "{err}");
    assert!(msg.contains("must not be the zero address"), "{err}");
}

/// Publish-side counterpart of [`empty_chain`], so the publish resolver's
/// precedence can be exercised as thoroughly as the node resolver's — both
/// now route through `resolve_common`, so a regression there hits both.
fn empty_publish_chain() -> cli::PublishChainArgs {
    cli::PublishChainArgs {
        common: cli::CommonChainArgs {
            config: None,
            rpc_url: None,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/decdn-test")),
            keystore_password_file: None,
            dry_run: true,
            json: false,
        },
        publisher_registry_address: None,
        origin_assignment_address: None,
    }
}

/// `resolve_common` returns `(String, u64, PathBuf, PathBuf)` — `data_dir`
/// and `keystore` are the same type, so transposing them at *any* of the
/// three destructure sites compiles silently. The node path is guarded by
/// `keystore_defaults_under_data_dir`; this is the publish path's guard.
/// Without it, a swap confined to `resolve_publish` would ship green and
/// hand the data dir to the keystore loader on every `decdn publish` submit.
#[test]
fn resolve_publish_keystore_defaults_under_data_dir() {
    let mut args = empty_publish_chain();
    args.common.rpc_url = Some("http://x".to_string());
    let r = resolve_publish(&args, &FileConfig::default()).unwrap();
    assert_eq!(r.data_dir, PathBuf::from("/tmp/decdn-test"));
    assert_eq!(r.keystore, PathBuf::from("/tmp/decdn-test/keystore.json"));
}

#[test]
fn resolve_publish_config_fills_unset_flags() {
    let args = empty_publish_chain();
    let file = file_with(FileBlockchain {
        rpc_url: Some("http://config:8545".to_string()),
        chain_id: None,
        eth_keystore: Some(PathBuf::from("/keys/ks.json")),
        publisher_registry_address: Some(CONFIG_ADDR.to_string()),
        origin_assignment_address: Some(OA_ADDR.to_string()),
        ..Default::default()
    });
    let r = resolve_publish(&args, &file).unwrap();
    assert_eq!(r.rpc_url, "http://config:8545");
    // `chain_id` absent from both flag and config → the shared default.
    assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
    assert_eq!(r.keystore, PathBuf::from("/keys/ks.json"));
    assert_eq!(r.publisher_registry_address, Some(CONFIG_ADDR));
    assert_eq!(r.origin_assignment_address, Some(OA_ADDR));
}

#[test]
fn resolve_publish_missing_rpc_url_errors() {
    let args = empty_publish_chain();
    let err = resolve_publish(&args, &FileConfig::default()).unwrap_err();
    assert!(err.to_string().contains("rpc_url not set"), "{err}");
}

#[test]
fn resolve_publish_flag_beats_config() {
    let args = cli::PublishChainArgs {
        common: cli::CommonChainArgs {
            config: None,
            rpc_url: Some("http://flag:8545".to_string()),
            chain_id: Some(42),
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/decdn-test")),
            keystore_password_file: None,
            dry_run: true,
            json: false,
        },
        publisher_registry_address: Some(FLAG_ADDR.to_string()),
        origin_assignment_address: None,
    };
    let file = file_with(FileBlockchain {
        rpc_url: Some("http://config:8545".to_string()),
        chain_id: Some(1),
        capacity_bond_address: None,
        eth_keystore: None,
        publisher_registry_address: Some(CONFIG_ADDR.to_string()),
        origin_assignment_address: Some(OA_ADDR.to_string()),
        slash_appeal_address: None,
    });
    let r = resolve_publish(&args, &file).unwrap();
    assert_eq!(r.rpc_url, "http://flag:8545");
    assert_eq!(r.chain_id, 42);
    assert_eq!(r.publisher_registry_address, Some(FLAG_ADDR));
    // unset flag falls through to config
    assert_eq!(r.origin_assignment_address, Some(OA_ADDR));
}

#[test]
fn resolve_publish_rejects_unparseable_address() {
    // A present-but-garbage address must surface as an Err, not be silently
    // dropped to `None` by the `.map(parse).transpose()?` — guards against a
    // future `.ok()` / `filter_map` rewrite that would swallow it and let
    // publish proceed as if the contract were unconfigured. Each field is
    // checked independently.
    let mut args = empty_publish_chain();
    args.common.rpc_url = Some("http://x".to_string());
    args.publisher_registry_address = Some("not-an-address".to_string());
    let err = resolve_publish(&args, &FileConfig::default()).unwrap_err();
    assert!(
        err.to_string().contains("publisher_registry_address"),
        "{err}"
    );

    let mut args = empty_publish_chain();
    args.common.rpc_url = Some("http://x".to_string());
    args.origin_assignment_address = Some("nope".to_string());
    let err = resolve_publish(&args, &FileConfig::default()).unwrap_err();
    assert!(
        err.to_string().contains("origin_assignment_address"),
        "{err}"
    );
}

#[test]
fn resolve_publish_rejects_zero_address() {
    // A present `0x0…0` for either publish address must fail at resolve time
    // via the shared guard, not surface as an opaque `PublisherRegistry` /
    // `OriginAssignment` revert later. Unset stays `None` (checked elsewhere);
    // only a *present* zero errors. Each field is checked independently
    // (#1153).
    let zero = Address::ZERO.to_string();

    let mut args = empty_publish_chain();
    args.common.rpc_url = Some("http://x".to_string());
    args.publisher_registry_address = Some(zero.clone());
    let err = resolve_publish(&args, &FileConfig::default()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("publisher_registry_address"), "{err}");
    assert!(msg.contains("must not be the zero address"), "{err}");

    let mut args = empty_publish_chain();
    args.common.rpc_url = Some("http://x".to_string());
    args.origin_assignment_address = Some(zero);
    let err = resolve_publish(&args, &FileConfig::default()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("origin_assignment_address"), "{err}");
    assert!(msg.contains("must not be the zero address"), "{err}");
}
