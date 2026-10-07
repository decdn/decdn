use super::*;
use crate::cli::run::BlockchainArgs;
use tempfile::TempDir;

/// The capability-expiry margin is one redeem interval plus the landing
/// slack, and saturates instead of overflowing.
#[test]
fn capability_expiry_margin_is_one_interval_plus_the_slack() {
    assert_eq!(
        capability_expiry_margin_secs(300),
        300 + REDEEM_LANDING_SLACK_SECS
    );
    assert_eq!(capability_expiry_margin_secs(u64::MAX), u64::MAX);
}

#[test]
fn normalize_region_accepts_and_uppercases() -> anyhow::Result<()> {
    assert_eq!(normalize_region("US")?, "US");
    assert_eq!(normalize_region("us")?, "US");
    assert_eq!(normalize_region("Us")?, "US");
    Ok(())
}

#[test]
fn normalize_region_rejects_wrong_length_or_charset() {
    // Length / charset failures and unassigned codes. Adversarial
    // inputs that the wire layer also rejects are pinned here too so
    // a config-resolver regression can't shift the only effective
    // check onto the receive path.
    let bad_inputs = [
        "usa", "u1", "", "U", "U S", "Ü1", "12", "U-", "OO", "JJ", "BX", "U/", "U\0",
    ];
    for bad in bad_inputs {
        assert!(
            normalize_region(bad).is_err(),
            "expected {bad:?} to be rejected"
        );
    }
}

#[test]
fn normalize_region_accepts_reserved_codes() {
    // Reserved-for-user-assignment codes (ISO 3166-1 §8.1.3) must
    // round-trip unchanged so air-gapped / testnet operators can use
    // them. Spot-check each range.
    for code in ["AA", "QM", "QZ", "XA", "XK", "XZ", "ZZ"] {
        assert_eq!(
            normalize_region(code).expect("accepted"),
            code,
            "{code} should round-trip"
        );
    }
}

// vitalik.eth, known-good EIP-55 checksum.
const GOOD_ADDR: &str = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
const ALT_ADDR_1: &str = "0x0000000000000000000000000000000000000001";
const ALT_ADDR_2: &str = "0x0000000000000000000000000000000000000002";
const ALT_ADDR_3: &str = "0x0000000000000000000000000000000000000003";

#[test]
fn parse_contract_address_accepts_checksummed() -> anyhow::Result<()> {
    let out = parse_contract_address("x", GOOD_ADDR)?;
    assert_eq!(out, GOOD_ADDR);
    Ok(())
}

#[test]
fn parse_contract_address_trims_whitespace() -> anyhow::Result<()> {
    let padded = format!("  {GOOD_ADDR}\n");
    let out = parse_contract_address("x", &padded)?;
    assert_eq!(out, GOOD_ADDR);
    Ok(())
}

#[test]
fn parse_contract_address_rejects_missing_0x_prefix() -> anyhow::Result<()> {
    let s = GOOD_ADDR
        .get(2..)
        .ok_or_else(|| anyhow::anyhow!("GOOD_ADDR shorter than expected"))?;
    assert!(parse_contract_address("x", s).is_err());
    Ok(())
}

#[test]
fn parse_contract_address_rejects_wrong_length() {
    assert!(parse_contract_address("x", "0xabc").is_err());
    assert!(parse_contract_address("x", &format!("{GOOD_ADDR}00")).is_err());
}

#[test]
fn parse_contract_address_rejects_empty_and_bare_prefix() {
    assert!(parse_contract_address("x", "").is_err());
    assert!(parse_contract_address("x", "0x").is_err());
}

#[test]
fn parse_contract_address_rejects_all_lowercase() {
    let lower = GOOD_ADDR.to_lowercase();
    assert!(parse_contract_address("x", &lower).is_err());
}

#[test]
fn parse_contract_address_rejects_bad_checksum() {
    let mut bad = String::from(GOOD_ADDR);
    // Flip the case of the first hex digit so the EIP-55 checksum no longer matches.
    bad.replace_range(2..3, "D");
    assert!(parse_contract_address("x", &bad).is_err());
}

#[test]
fn parse_contract_address_rejects_non_hex() {
    let bad = "0xZZZZ6BF26964aF9D7eEd9e03E53415D37aA96045";
    assert!(parse_contract_address("x", bad).is_err());
}

#[test]
fn parse_contract_address_error_names_field_and_format() -> anyhow::Result<()> {
    let lower = GOOD_ADDR.to_lowercase();
    let Err(err) = parse_contract_address("payment_pool_address", &lower) else {
        anyhow::bail!("expected parse_contract_address to fail on lowercase input");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("invalid payment_pool_address"),
        "missing flag name: {msg}"
    );
    assert!(msg.contains("EIP-55"), "missing format hint: {msg}");
    Ok(())
}

#[test]
fn resolve_receipts_applies_defaults_when_absent() -> anyhow::Result<()> {
    let r = resolve_receipts(None)?;
    assert_eq!(r.max_file_bytes, DEFAULT_RECEIPT_MAX_FILE_BYTES);
    assert_eq!(r.retained_files, DEFAULT_RECEIPT_RETAINED_FILES);
    Ok(())
}

#[test]
fn resolve_receipts_applies_explicit_values() -> anyhow::Result<()> {
    let cfg = types::ReceiptsConfig {
        max_file_bytes: Some(8 << 20),
        retained_files: Some(0),
    };
    let r = resolve_receipts(Some(&cfg))?;
    assert_eq!(r.max_file_bytes, 8 << 20);
    assert_eq!(r.retained_files, 0);
    Ok(())
}

#[test]
fn resolve_receipts_accepts_boundary_values() -> anyhow::Result<()> {
    for bytes in [MIN_RECEIPT_MAX_FILE_BYTES, MAX_RECEIPT_MAX_FILE_BYTES] {
        let cfg = types::ReceiptsConfig {
            max_file_bytes: Some(bytes),
            retained_files: Some(MAX_RECEIPT_RETAINED_FILES),
        };
        let r = resolve_receipts(Some(&cfg))?;
        assert_eq!(r.max_file_bytes, bytes);
        assert_eq!(r.retained_files, MAX_RECEIPT_RETAINED_FILES);
    }
    Ok(())
}

#[test]
fn resolve_receipts_rejects_max_file_bytes_below_floor() {
    let cfg = types::ReceiptsConfig {
        max_file_bytes: Some(MIN_RECEIPT_MAX_FILE_BYTES - 1),
        retained_files: None,
    };
    let err = resolve_receipts(Some(&cfg))
        .expect_err("expected error")
        .to_string();
    assert!(
        err.contains("receipts.max_file_bytes"),
        "error missing field context: {err}"
    );
}

#[test]
fn resolve_receipts_rejects_max_file_bytes_above_ceiling() {
    let cfg = types::ReceiptsConfig {
        max_file_bytes: Some(MAX_RECEIPT_MAX_FILE_BYTES + 1),
        retained_files: None,
    };
    let err = resolve_receipts(Some(&cfg))
        .expect_err("expected error")
        .to_string();
    assert!(
        err.contains("receipts.max_file_bytes"),
        "error missing field context: {err}"
    );
}

#[test]
fn resolve_receipts_rejects_retained_files_above_cap() {
    let cfg = types::ReceiptsConfig {
        max_file_bytes: None,
        retained_files: Some(MAX_RECEIPT_RETAINED_FILES + 1),
    };
    let err = resolve_receipts(Some(&cfg))
        .expect_err("expected error")
        .to_string();
    assert!(
        err.contains("receipts.retained_files"),
        "error missing field context: {err}"
    );
}

// HOME is guaranteed set in Rust test harness on Linux/macOS and used here
// to exercise ${VAR} expansion without mutating the process environment
// (std::env::set_var is `unsafe` in edition 2024, and workspace lints
// forbid `unsafe_code`).
fn home_str() -> anyhow::Result<String> {
    Ok(dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("test requires home dir"))?
        .to_string_lossy()
        .into_owned())
}

fn cfg_with_rpc(raw: &str) -> FileConfig {
    FileConfig {
        blockchain: Some(types::BlockchainConfig {
            origin_assignment_address: None,
            publisher_registry_address: None,
            rpc_url: Some(raw.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn expand_env_substitutes_string_field() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = cfg_with_rpc("${HOME}/rpc");
    expand_env(&mut cfg)?;
    let url = cfg
        .blockchain
        .as_ref()
        .and_then(|b| b.rpc_url.as_deref())
        .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
    anyhow::ensure!(url == format!("{home}/rpc"), "got: {url}");
    Ok(())
}

#[test]
fn expand_env_substitutes_slash_judge_address() -> anyhow::Result<()> {
    // Regression for the wiring line in `expand_env`: a `${VAR}` in
    // blockchain.slash_judge_address must be expanded before
    // `parse_contract_address`, like the other contract-address fields.
    let home = home_str()?;
    let mut cfg = FileConfig {
        blockchain: Some(types::BlockchainConfig {
            origin_assignment_address: None,
            publisher_registry_address: None,
            slash_judge_address: Some("${HOME}/judge".to_string()),
            slash_appeal_address: None,
            content_blacklist_address: None,
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let got = cfg
        .blockchain
        .as_ref()
        .and_then(|b| b.slash_judge_address.as_deref())
        .ok_or_else(|| anyhow::anyhow!("slash_judge_address missing"))?;
    anyhow::ensure!(got == format!("{home}/judge"), "got: {got}");
    Ok(())
}

#[test]
fn expand_env_substitutes_each_relay_url() -> anyhow::Result<()> {
    // Regression for the per-element loop in `expand_env`: every entry of
    // network.relay_urls must get the same `${VAR}` treatment, not just the
    // first.
    let home = home_str()?;
    let mut cfg = FileConfig {
        network: Some(types::NetworkConfig {
            bind_port: None,
            relay_urls: Some(vec![
                "${HOME}/relay-a".to_string(),
                "${HOME}/relay-b".to_string(),
            ]),
            discovery: None,
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let urls = cfg
        .network
        .as_ref()
        .and_then(|n| n.relay_urls.as_ref())
        .ok_or_else(|| anyhow::anyhow!("relay_urls missing"))?;
    anyhow::ensure!(
        urls == &[format!("{home}/relay-a"), format!("{home}/relay-b")],
        "got: {urls:?}"
    );
    Ok(())
}

#[test]
fn expand_env_substitutes_discovery_fields() -> anyhow::Result<()> {
    // #863: `[network.discovery]` fields (pkarr_url, dns_origin, and each
    // peer's relay_url + addrs) must get the same `${VAR}` expansion as
    // their `relay_urls` sibling, not pass through as literal strings.
    let home = home_str()?;
    let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        id.to_string(),
        types::DiscoveryPeer {
            relay_url: Some("${HOME}/peer-relay".to_string()),
            addrs: vec!["${HOME}/addr-a".to_string(), "${HOME}/addr-b".to_string()],
        },
    );
    let mut cfg = FileConfig {
        network: Some(types::NetworkConfig {
            bind_port: None,
            relay_urls: None,
            discovery: Some(types::DiscoveryConfig {
                pkarr_url: Some("${HOME}/pkarr".to_string()),
                dns_origin: Some("${HOME}/dns".to_string()),
                peers: Some(peers),
            }),
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let d = cfg
        .network
        .as_ref()
        .and_then(|n| n.discovery.as_ref())
        .ok_or_else(|| anyhow::anyhow!("discovery missing"))?;
    anyhow::ensure!(d.pkarr_url.as_deref() == Some(format!("{home}/pkarr").as_str()));
    anyhow::ensure!(d.dns_origin.as_deref() == Some(format!("{home}/dns").as_str()));
    let peer = d
        .peers
        .as_ref()
        .and_then(|p| p.get(id))
        .ok_or_else(|| anyhow::anyhow!("peer missing"))?;
    anyhow::ensure!(peer.relay_url.as_deref() == Some(format!("{home}/peer-relay").as_str()));
    anyhow::ensure!(
        peer.addrs == [format!("{home}/addr-a"), format!("{home}/addr-b")],
        "got: {:?}",
        peer.addrs
    );
    Ok(())
}

#[test]
fn expand_env_discovery_peer_error_order_is_deterministic() -> anyhow::Result<()> {
    // #863 review: peers are stored in a `HashMap`, so without sorting the
    // first env-expansion error surfaced when several peers are malformed
    // would vary across runs. With two peers both carrying an unset var, the
    // error must always name the lexicographically-smallest peer id.
    let missing = "DECDN_UNSET_PEER_ORDER_VAR_ZZZ";
    anyhow::ensure!(
        std::env::var_os(missing).is_none(),
        "test precondition violated: {missing} is set in the environment"
    );
    let placeholder = format!("${{{missing}}}");
    let lo = "0000000000000000000000000000000000000000000000000000000000000000";
    let hi = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let mut peers = std::collections::HashMap::new();
    for id in [lo, hi] {
        peers.insert(
            id.to_string(),
            types::DiscoveryPeer {
                relay_url: Some(placeholder.clone()),
                addrs: Vec::new(),
            },
        );
    }
    let mut cfg = FileConfig {
        network: Some(types::NetworkConfig {
            bind_port: None,
            relay_urls: None,
            discovery: Some(types::DiscoveryConfig {
                pkarr_url: None,
                dns_origin: None,
                peers: Some(peers),
            }),
        }),
        ..Default::default()
    };
    let err = expand_env(&mut cfg)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected expansion to fail"))?
        .to_string();
    anyhow::ensure!(
        err.contains(&format!("network.discovery.peers[{lo}]")),
        "error should name the lexicographically-smallest peer: {err}"
    );
    Ok(())
}

#[test]
fn expand_env_substitutes_path_field() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = FileConfig {
        identity: Some(types::IdentityConfig {
            data_dir: Some(PathBuf::from("${HOME}/node")),
            region: None,
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let dd = cfg
        .identity
        .as_ref()
        .and_then(|i| i.data_dir.as_deref())
        .ok_or_else(|| anyhow::anyhow!("data_dir missing"))?
        .to_path_buf();
    let expected = format!("{home}/node");
    anyhow::ensure!(dd == Path::new(&expected), "got: {}", dd.display());
    Ok(())
}

#[test]
fn expand_env_errors_on_missing_var_naming_field() -> anyhow::Result<()> {
    // Var name unlikely to exist; if it does, the test is meaningless —
    // skip loudly rather than producing a false pass.
    let missing = "DECDN_DEFINITELY_UNSET_VAR_QZX_223";
    anyhow::ensure!(
        std::env::var_os(missing).is_none(),
        "test precondition violated: {missing} is set in the environment"
    );
    let mut cfg = cfg_with_rpc(&format!("${{{missing}}}"));
    let err = expand_env(&mut cfg)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected expansion error"))?
        .to_string();
    anyhow::ensure!(
        err.contains("blockchain.rpc_url") && err.contains(missing),
        "error missing context, got: {err}"
    );
    Ok(())
}

#[test]
fn expand_env_leaves_plain_values_untouched() -> anyhow::Result<()> {
    let mut cfg = cfg_with_rpc("https://plain.example");
    expand_env(&mut cfg)?;
    let url = cfg
        .blockchain
        .as_ref()
        .and_then(|b| b.rpc_url.as_deref())
        .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
    anyhow::ensure!(url == "https://plain.example", "got: {url}");
    Ok(())
}

// A literal `$` (e.g. in basic-auth passwords or query strings) must
// pass through untouched; only the explicit `${VAR}` form triggers
// expansion. Otherwise operators lose access to values containing `$`.
#[test]
fn expand_env_preserves_literal_dollar_without_braces() -> anyhow::Result<()> {
    let raw = "https://user:p$w0rd@host/path?token=abc$def";
    let mut cfg = cfg_with_rpc(raw);
    expand_env(&mut cfg)?;
    let url = cfg
        .blockchain
        .as_ref()
        .and_then(|b| b.rpc_url.as_deref())
        .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
    anyhow::ensure!(url == raw, "got: {url}");
    Ok(())
}

// Windows-style paths with backslashes must keep their backslashes and
// still expand `${VAR}` — shell-style escape interpreters would swallow
// `\` before `$` and disable the expansion on real Windows paths.
#[test]
fn expand_env_handles_backslash_before_brace() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            cache_dir: Some(PathBuf::from(r"C:\data\${HOME}\cache")),
            cache_size_mb: None,
            max_blob_size_mb: None,
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let dir = cfg
        .cache
        .as_ref()
        .and_then(|c| c.cache_dir.as_deref())
        .ok_or_else(|| anyhow::anyhow!("cache_dir missing"))?
        .to_path_buf();
    let expected = PathBuf::from(format!(r"C:\data\{home}\cache"));
    anyhow::ensure!(dir == expected, "got: {}", dir.display());
    Ok(())
}

// Unterminated `${` should surface a clear error rather than silently
// consume the rest of the string.
#[test]
fn expand_env_errors_on_unterminated_brace() -> anyhow::Result<()> {
    let mut cfg = cfg_with_rpc("https://${HOST/api");
    let err = expand_env(&mut cfg)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error"))?
        .to_string();
    anyhow::ensure!(
        err.contains("blockchain.rpc_url") && err.contains("unterminated"),
        "got: {err}"
    );
    Ok(())
}

#[test]
fn expand_env_expands_tilde_in_path_field() -> anyhow::Result<()> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("test requires home dir"))?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            cache_dir: Some(PathBuf::from("~/decdn-cache")),
            cache_size_mb: None,
            max_blob_size_mb: None,
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let dir = cfg
        .cache
        .as_ref()
        .and_then(|c| c.cache_dir.as_deref())
        .ok_or_else(|| anyhow::anyhow!("cache_dir missing"))?
        .to_path_buf();
    anyhow::ensure!(dir == home.join("decdn-cache"), "got: {}", dir.display());
    Ok(())
}

#[test]
fn expand_env_substitutes_multiple_vars_in_one_value() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = cfg_with_rpc("${HOME}/a/${HOME}/b");
    expand_env(&mut cfg)?;
    let url = cfg
        .blockchain
        .as_ref()
        .and_then(|b| b.rpc_url.as_deref())
        .ok_or_else(|| anyhow::anyhow!("rpc_url missing"))?;
    anyhow::ensure!(url == format!("{home}/a/{home}/b"), "got: {url}");
    Ok(())
}

#[test]
fn expand_env_expands_bare_tilde_path() -> anyhow::Result<()> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("test requires home dir"))?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            cache_dir: Some(PathBuf::from("~")),
            cache_size_mb: None,
            max_blob_size_mb: None,
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let dir = cfg
        .cache
        .as_ref()
        .and_then(|c| c.cache_dir.as_deref())
        .ok_or_else(|| anyhow::anyhow!("cache_dir missing"))?
        .to_path_buf();
    anyhow::ensure!(dir == home, "got: {}", dir.display());
    Ok(())
}

type FieldSetter = fn(&mut FileConfig, &str);

// Guards against copy-paste mislabeling in the 8-arm wiring of
// `expand_env` — every expandable field must surface its own dotted
// path in the error message.
#[test]
// Length crept past 100 lines after `..Default::default()` was
// added to every CacheConfig literal in the field-setter table
// (#312/#276 PR). The body is a flat list of cases — splitting
// wouldn't compress information density.
#[allow(clippy::too_many_lines)]
fn expand_env_per_field_error_context() -> anyhow::Result<()> {
    let missing = "DECDN_UNSET_PER_FIELD_VAR_ZZZ";
    anyhow::ensure!(
        std::env::var_os(missing).is_none(),
        "test precondition violated: {missing} is set in the environment"
    );
    let placeholder = format!("${{{missing}}}");

    let cases: &[(&str, FieldSetter)] = &[
        ("identity.data_dir", |c, v| {
            c.identity = Some(types::IdentityConfig {
                data_dir: Some(PathBuf::from(v)),
                region: None,
            });
        }),
        ("identity.region", |c, v| {
            c.identity = Some(types::IdentityConfig {
                data_dir: None,
                region: Some(v.to_string()),
            });
        }),
        ("network.relay_urls", |c, v| {
            c.network = Some(types::NetworkConfig {
                bind_port: None,
                relay_urls: Some(vec![v.to_string()]),
                discovery: None,
            });
        }),
        ("blockchain.rpc_url", |c, v| {
            c.blockchain = Some(types::BlockchainConfig {
                origin_assignment_address: None,
                publisher_registry_address: None,
                rpc_url: Some(v.to_string()),
                ..Default::default()
            });
        }),
        ("blockchain.eth_keystore", |c, v| {
            c.blockchain = Some(types::BlockchainConfig {
                origin_assignment_address: None,
                publisher_registry_address: None,
                eth_keystore: Some(PathBuf::from(v)),
                ..Default::default()
            });
        }),
        ("blockchain.payment_pool_address", |c, v| {
            c.blockchain = Some(types::BlockchainConfig {
                origin_assignment_address: None,
                publisher_registry_address: None,
                payment_pool_address: Some(v.to_string()),
                ..Default::default()
            });
        }),
        ("blockchain.capacity_bond_address", |c, v| {
            c.blockchain = Some(types::BlockchainConfig {
                origin_assignment_address: None,
                publisher_registry_address: None,
                capacity_bond_address: Some(v.to_string()),
                ..Default::default()
            });
        }),
        ("cache.cache_dir", |c, v| {
            c.cache = Some(types::CacheConfig {
                cache_dir: Some(PathBuf::from(v)),
                cache_size_mb: None,
                max_blob_size_mb: None,
                ..Default::default()
            });
        }),
        ("cache.origin.url", |c, v| {
            c.cache = Some(types::CacheConfig {
                origin: Some(types::OriginConfig::Http {
                    url: v.to_string(),
                    decompress: None,
                }),
                ..Default::default()
            });
        }),
        ("cache.origin.path", |c, v| {
            c.cache = Some(types::CacheConfig {
                origin: Some(types::OriginConfig::Fs {
                    path: PathBuf::from(v),
                }),
                ..Default::default()
            });
        }),
        ("cache.user_agent", |c, v| {
            c.cache = Some(types::CacheConfig {
                user_agent: Some(v.to_string()),
                ..Default::default()
            });
        }),
        ("observability.otlp_endpoint", |c, v| {
            c.observability = Some(types::ObservabilityConfig {
                log_level: None,
                log_format: None,
                metrics_port: None,
                metrics_bind: None,
                admin_port: None,
                otlp_endpoint: Some(v.to_string()),
            });
        }),
    ];

    for (expected_ctx, setter) in cases {
        let mut cfg = FileConfig::default();
        setter(&mut cfg, &placeholder);
        let err = expand_env(&mut cfg)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected error for {expected_ctx}"))?
            .to_string();
        anyhow::ensure!(
            err.contains(expected_ctx),
            "field `{expected_ctx}` missing from error: {err}"
        );
    }
    Ok(())
}

// Same guard as `expand_env_substitutes_cache_origin_url`, for the
// user_agent field. Operators commonly want to embed `${HOSTNAME}` or
// a build-tag env var into the UA, and silently shipping the literal
// `${...}` would be a confusing wire-level surprise.
#[test]
fn expand_env_substitutes_cache_user_agent() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            user_agent: Some("decdn-${HOME}/test".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let ua = cfg
        .cache
        .as_ref()
        .and_then(|c| c.user_agent.as_deref())
        .ok_or_else(|| anyhow::anyhow!("user_agent missing"))?;
    let expected = format!("decdn-{home}/test");
    anyhow::ensure!(ua == expected, "got: {ua}");
    Ok(())
}

// Guards against the classic "added a field, forgot to wire expansion"
// regression — the HTTP-origin URL is URL-shaped and must get the
// same `${VAR}` treatment as sibling URL fields (rpc_url, relay_urls,
// etc).
#[test]
fn expand_env_substitutes_cache_origin_url() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            origin: Some(types::OriginConfig::Http {
                url: "https://origin.example/${HOME}/bucket".to_string(),
                decompress: None,
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let url = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
        Some(types::OriginConfig::Http { url, .. }) => url.clone(),
        other => anyhow::bail!("expected Http origin, got: {other:?}"),
    };
    let expected = format!("https://origin.example/{home}/bucket");
    anyhow::ensure!(url == expected, "got: {url}");
    Ok(())
}

// Sibling of `expand_env_substitutes_cache_origin_url` — the FS
// origin path is a path-shaped field and must get the same
// `${VAR}` treatment so an operator can write
// `path = "${HOME}/cache-origin"` in their TOML and have it
// resolve correctly.
#[test]
fn expand_env_substitutes_cache_origin_fs_path() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            origin: Some(types::OriginConfig::Fs {
                path: PathBuf::from("${HOME}/cache-origin"),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let path = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
        Some(types::OriginConfig::Fs { path }) => path.clone(),
        other => anyhow::bail!("expected Fs origin, got: {other:?}"),
    };
    let expected = PathBuf::from(format!("{home}/cache-origin"));
    anyhow::ensure!(path == expected, "got: {}", path.display());
    Ok(())
}

/// A zero throughput-floor window wedges this node's pull path (#1797): the floor demands
/// progress over no time at all, so it trips on the first poll of every streaming read and
/// abandons every upstream before a byte can arrive. The abort is non-attributable, so it
/// does not defame peers — but a node that can never complete a pull is still a broken
/// node, so the window must be rejected at load.
#[test]
fn resolve_cache_rejects_zero_stall_window() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        node_pull_stall_window_sec: Some(0),
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir")).is_err(),
        "a 0 window must be rejected: the throughput floor would abandon every upstream"
    );
}

/// The eviction hysteresis gap is structural (#1173): `target_pct` must sit
/// at least 5 points below `high_water_pct`, else the driver thrashes on
/// writes hovering near the trigger.
#[test]
fn resolve_cache_rejects_eviction_target_above_hysteresis_gap() {
    let cli = empty_cache_args();
    // 88 is only 2 points below 90 — inside the 5-point gap.
    let toml = types::CacheConfig {
        eviction_high_water_pct: Some(90),
        eviction_target_pct: Some(88),
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir")).is_err(),
        "target within 5 points of high-water must be rejected (hysteresis gap)"
    );
}

/// Eviction percentages have hard bounds; an out-of-range high-water is a
/// governance error the resolver must catch, not clamp.
#[test]
fn resolve_cache_rejects_out_of_range_eviction_high_water() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        eviction_high_water_pct: Some(99), // > 95 upper bound
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir")).is_err(),
        "high-water above the [60,95] bound must be rejected"
    );
}

/// The default eviction knobs resolve and satisfy the hysteresis invariant.
#[test]
fn resolve_cache_eviction_defaults_are_valid() {
    let cli = empty_cache_args();
    let resolved = resolve_cache(&cli, None, Path::new("/data-dir"))
        .expect("default eviction knobs must resolve");
    assert_eq!(resolved.eviction_high_water_pct, 90);
    assert_eq!(resolved.eviction_target_pct, 80);
    assert_eq!(resolved.eviction_per_sweep_budget, 16);
    assert_eq!(resolved.eviction_tick_secs, 1);
}

/// The three origin-probe TTLs are one graded policy: a fault must be
/// re-probed no less eagerly than a positive answer, or a recovered origin
/// stays hidden and the origin-only serve gate keeps refusing paying
/// clients; and no more eagerly than an absence, or memoising it buys
/// nothing. Both ends are rejected rather than clamped — the ordering is
/// stated in every knob's docs, so it has to hold for every accepted
/// config, not just the defaults.
#[test]
fn resolve_cache_rejects_unordered_origin_probe_ttls() {
    let cli = empty_cache_args();
    let too_long = types::CacheConfig {
        origin_probe_fault_ttl_sec: Some(3600),
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&too_long), Path::new("/data-dir")).is_err(),
        "a fault TTL above the positive TTL hides a recovered origin"
    );
    let too_short = types::CacheConfig {
        origin_probe_negative_ttl_sec: Some(30),
        origin_probe_fault_ttl_sec: Some(5),
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&too_short), Path::new("/data-dir")).is_err(),
        "a fault TTL below the negative TTL is not a graded policy"
    );
    let ordered = types::CacheConfig {
        origin_probe_ttl_sec: Some(30),
        origin_probe_negative_ttl_sec: Some(2),
        origin_probe_fault_ttl_sec: Some(10),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&ordered), Path::new("/data-dir"))
        .expect("an ordered override must resolve");
    assert_eq!(resolved.origin_probe_fault_ttl_sec, 10);
}

/// The default probe TTLs satisfy the ordering the resolver enforces, so a
/// node that touches none of the knobs still starts.
#[test]
fn resolve_cache_origin_probe_defaults_are_ordered() {
    let cli = empty_cache_args();
    let resolved = resolve_cache(&cli, None, Path::new("/data-dir"))
        .expect("default probe knobs must resolve");
    assert_eq!(resolved.origin_probe_ttl_sec, 15);
    assert_eq!(resolved.origin_probe_negative_ttl_sec, 2);
    assert_eq!(resolved.origin_probe_fault_ttl_sec, 5);
    assert!(
        resolved.origin_probe_negative_ttl_sec <= resolved.origin_probe_fault_ttl_sec
            && resolved.origin_probe_fault_ttl_sec <= resolved.origin_probe_ttl_sec
    );
}

/// Defaults keep the pre-ADR-040 behavior: LRU eviction, unconditional
/// admission. Operators who never touch the new knobs see no change.
#[test]
fn defaults_are_always_lru() {
    let cli = empty_cache_args();
    let resolved =
        resolve_cache(&cli, None, Path::new("/data-dir")).expect("defaults must resolve");
    assert_eq!(resolved.admission_policy, "always");
    assert_eq!(resolved.eviction_policy, "lru");
}

/// `[cache.tinylfu]` defaults apply once `eviction_policy = "tinylfu"` is
/// selected, even with no `[cache.tinylfu]` table present.
#[test]
fn tinylfu_defaults_resolve() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        eviction_policy: Some("tinylfu".to_string()),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&toml), Path::new("/data-dir"))
        .expect("tinylfu eviction policy must resolve");
    assert_eq!(resolved.eviction_policy, "tinylfu");
    assert_eq!(resolved.tinylfu.promotion_threshold, 2);
    assert_eq!(resolved.tinylfu.probation_target_pct, 10);
    assert_eq!(resolved.tinylfu.sketch_bytes, 262_144);
    assert_eq!(resolved.tinylfu.aging_halflife_sec, 600);
}

/// An unknown `cache.eviction_policy` name is a config error at load time —
/// never a silent fallback to `lru`.
#[test]
fn unknown_eviction_policy_is_rejected() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        eviction_policy: Some("nonsense".to_string()),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/data-dir"))
        .expect_err("unknown eviction policy name must be rejected");
    assert!(
        err.to_string().contains("cache.eviction_policy"),
        "error must name cache.eviction_policy: {err}"
    );
}

/// An unknown `cache.admission_policy` name is a config error at load time —
/// never a silent fallback to `always`.
#[test]
fn unknown_admission_policy_is_rejected() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        admission_policy: Some("nonsense".to_string()),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/data-dir"))
        .expect_err("unknown admission policy name must be rejected");
    assert!(
        err.to_string().contains("cache.admission_policy"),
        "error must name cache.admission_policy: {err}"
    );
}

/// `probation_target_pct` has a hard bound `[1, 50]` — a value outside it is
/// a governance error the resolver must catch, not clamp.
#[test]
fn resolve_cache_rejects_out_of_range_probation_target_pct() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        eviction_policy: Some("tinylfu".to_string()),
        tinylfu: Some(types::TinyLfuConfig {
            probation_target_pct: Some(51),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir")).is_err(),
        "probation_target_pct above the [1, 50] bound must be rejected"
    );
}

/// `promotion_threshold` must be `>= 1`: zero would admit everything straight
/// to `Main` and defeat probationary admission, so the resolver rejects it
/// rather than clamping (ADR 040 §Probationary admission).
#[test]
fn resolve_cache_rejects_zero_promotion_threshold() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        eviction_policy: Some("tinylfu".to_string()),
        tinylfu: Some(types::TinyLfuConfig {
            promotion_threshold: Some(0),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir")).is_err(),
        "promotion_threshold = 0 must be rejected"
    );
}

/// `sketch_bytes` below the floor buys too few columns per shard for the
/// estimates to separate distinct blobs, so the resolver rejects it rather
/// than clamping (ADR 040 §Configuration surface).
#[test]
fn resolve_cache_rejects_an_undersized_sketch() {
    let sized = |bytes: usize, admission: &str| {
        let cli = empty_cache_args();
        let toml = types::CacheConfig {
            admission_policy: Some(admission.to_string()),
            tinylfu: Some(types::TinyLfuConfig {
                sketch_bytes: Some(bytes),
                ..Default::default()
            }),
            ..Default::default()
        };
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir"))
            .err()
            .map(|e| e.to_string())
    };

    // Name the knob in the assertion: the resolver bags every check into one
    // error, so a bare `is_err()` would pass on any unrelated failure.
    let err = sized(MIN_TINYLFU_SKETCH_BYTES - 1, "tinylfu")
        .unwrap_or_else(|| "resolved without error".to_string());
    assert!(
        err.contains("cache.tinylfu.sketch_bytes"),
        "the floor rejection must name the knob, got: {err}"
    );

    // The floor itself resolves — an off-by-one to `>` would reject the
    // exact value the docs tell an operator is allowed.
    assert!(
        sized(MIN_TINYLFU_SKETCH_BYTES, "tinylfu").is_none(),
        "the documented minimum must be accepted"
    );

    // Validation does not consult the policy selectors (ADR 040
    // §Configuration surface). For `sketch_bytes` the selectors are not even
    // the only gate: the default `serve_economics.policy = "margin"` builds
    // the same estimator, so an `always` node runs this sketch for real.
    let inert = sized(MIN_TINYLFU_SKETCH_BYTES - 1, "always")
        .unwrap_or_else(|| "resolved without error".to_string());
    assert!(
        inert.contains("cache.tinylfu.sketch_bytes"),
        "the floor applies whether or not a selector names tinylfu, got: {inert}"
    );
}

/// A zero open budget abandons every upstream before its handshake can finish,
/// so no pull can ever succeed. Local-only blast radius (a `PullTimeout` is
/// exonerating), but still a config that cannot work.
#[test]
fn resolve_cache_rejects_zero_pull_timeout() {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        node_pull_timeout_sec: Some(0),
        ..Default::default()
    };
    assert!(
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir")).is_err(),
        "a 0 open budget must be rejected: no pull could ever complete its handshake"
    );
}

/// Parse a `[cache...]` TOML snippet and resolve it into a [`ResolvedCache`],
/// the same round trip an operator's config file goes through.
fn resolve_from_toml(toml_str: &str) -> anyhow::Result<ResolvedCache> {
    let file: FileConfig = toml::from_str(toml_str)?;
    let cli = empty_cache_args();
    resolve_cache(&cli, file.cache.as_ref(), Path::new("/data-dir"))
}

/// `[cache.serve_economics]` defaults to the `"margin"` policy (ADR 041) with
/// a 50% discount and an `n_max` of 64, even with no table present.
#[test]
fn serve_economics_defaults_to_margin() {
    let resolved = resolve_from_toml("").expect("empty config resolves");
    assert_eq!(resolved.serve_economics.policy, "margin");
    assert_eq!(resolved.serve_economics.discount_bps, 5000);
    assert_eq!(resolved.serve_economics.n_max, 64);
    assert_eq!(resolved.serve_economics.warming_budget, 5_000_000);
    assert_eq!(resolved.serve_economics.warming_refill, 58);
}

/// An unknown `cache.serve_economics.policy` name is a config error at load
/// time — never a silent fallback to `"margin"`.
#[test]
fn unknown_serve_economics_policy_is_rejected() {
    let toml = "[cache.serve_economics]\npolicy = \"bogus\"\n";
    let err = resolve_from_toml(toml).expect_err("unknown policy rejected");
    assert!(err.to_string().contains("cache.serve_economics.policy"));
}

/// `discount` must fall within `(0.0, 1.0]`: `0.0` would give away
/// everything for free (defeating the margin gate) and anything above `1.0`
/// is not a discount.
#[test]
fn serve_economics_discount_out_of_range_is_rejected() {
    for bad in ["0.0", "1.5"] {
        let toml = format!("[cache.serve_economics]\ndiscount = {bad}\n");
        assert!(
            resolve_from_toml(&toml).is_err(),
            "discount {bad} must be rejected"
        );
    }
}

/// `n_max = 0` is rejected rather than clamped: it would leave no room for
/// any speculative warming source.
#[test]
fn serve_economics_n_max_zero_is_rejected() {
    let toml = "[cache.serve_economics]\nn_max = 0\n";
    assert!(
        resolve_from_toml(toml).is_err(),
        "n_max = 0 must be rejected"
    );
}

/// `warming_budget = 0` is rejected rather than silently disabling warming:
/// the operator must set `policy = \"off\"` to opt out explicitly.
#[test]
fn serve_economics_warming_budget_zero_is_rejected() {
    let toml = "[cache.serve_economics]\nwarming_budget = 0\n";
    assert!(
        resolve_from_toml(toml).is_err(),
        "warming_budget = 0 must be rejected"
    );
}

// The FS arm of `resolve_origin` calls `expand_tilde` so a TOML like
// `path = "~/origin"` resolves to `<home>/origin`. The expansion happens inside
// resolution (not in `expand_env`), because `~` is filesystem-shaped and the
// `expand_env` contract only handles `${VAR}` substitution.
#[test]
fn resolve_cache_origin_fs_expands_tilde_in_path() -> anyhow::Result<()> {
    let home_dir = TempDir::new()?;
    let home = home_dir.path().to_path_buf();
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Fs {
            path: PathBuf::from("~/origin"),
        }),
        ..Default::default()
    };
    let resolved = common::test_support::with_home_override(Some(&home), || {
        resolve_cache(&cli, Some(&toml), Path::new("/data-dir"))
    })?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::Fs { path }) => {
            anyhow::ensure!(
                path == home.join("origin"),
                "tilde should have expanded; got: {}",
                path.display()
            );
        }
        other => anyhow::bail!("expected Fs origin, got: {other:?}"),
    }
    Ok(())
}

// resolve_cache must reject an HTTP origin variant whose `url`
// is the empty string. The error is raised by the explicit
// `anyhow::ensure!(!url.is_empty(), ...)` in `resolve_origin`,
// separate from `parse_origin_url`'s scheme/format checks. The
// existing `resolve_cache_rejects_non_http_origin_url` test
// covers the parser path; this one covers the
// empty-string-fails-fast path so a future refactor (e.g.
// pushing the empty check inside the parser) can't silently
// drop the contract.
#[test]
fn resolve_cache_rejects_empty_http_origin_url() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: String::new(),
            decompress: None,
        }),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected empty-url rejection"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("cache.origin.url") && msg.contains("must not be empty"),
        "error lacked context: {msg}"
    );
    Ok(())
}

// Sibling of the empty-URL test for the FS variant. The
// emptiness check runs *after* tilde expansion (the operator
// wrote `path = ""`), so an empty PathBuf reaches the
// `as_os_str().is_empty()` guard.
#[test]
fn resolve_cache_rejects_empty_fs_origin_path() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Fs {
            path: PathBuf::new(),
        }),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected empty-path rejection"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("cache.origin.path") && msg.contains("must not be empty"),
        "error lacked context: {msg}"
    );
    Ok(())
}

#[test]
fn blockchain_usdc_address_parses_under_deny_unknown_fields() -> anyhow::Result<()> {
    // `usdc_address` is the settlement token a client config names, and
    // `BlockchainConfig` has `deny_unknown_fields`, so a client config must
    // parse here (and via `decdn config validate`) — otherwise the schema
    // forks. Assert it deserializes without an "unknown field" error.
    let toml = "\
[blockchain]
usdc_address = \"0xUsdc\"
";
    let file: crate::config::FileConfig = ::toml::from_str(toml)?;
    let bc = file
        .blockchain
        .ok_or_else(|| anyhow::anyhow!("missing [blockchain] section"))?;
    anyhow::ensure!(bc.usdc_address.as_deref() == Some("0xUsdc"));
    Ok(())
}

// -------------------------------------------------------------------
// Multi-origin fallback config validation (#284)
//
// The resolver collapses both wire forms — singular `[cache.origin]`
// and plural `[[cache.origins]]` — into a single canonical
// `Vec<ResolvedOrigin>` in `ResolvedCache`. These tests pin the
// invariants the chain-walk engine relies on: declared order is
// preserved, both forms cannot be set simultaneously, and an
// empty plural array fails fast rather than silently degrading
// to "no pull-through".
// -------------------------------------------------------------------

#[test]
fn resolve_cache_accepts_array_of_origins_preserving_order() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origins: Some(vec![
            types::OriginConfig::Http {
                url: "https://primary.example/".to_string(),
                decompress: None,
            },
            types::OriginConfig::Fs {
                path: PathBuf::from("/var/lib/decdn/mirror"),
            },
        ]),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.origins.len() == 2,
        "expected 2 origins, got {}",
        resolved.origins.len()
    );
    // Order is operator-controlled and load-bearing — assert the
    // first slot is the HTTP entry and the second is the Fs one,
    // matching the TOML declaration order.
    anyhow::ensure!(
        matches!(
            resolved.origins[0],
            crate::config::ResolvedOrigin::Http { .. }
        ),
        "expected origins[0] to be Http, got {:?}",
        resolved.origins[0]
    );
    anyhow::ensure!(
        matches!(
            resolved.origins[1],
            crate::config::ResolvedOrigin::Fs { .. }
        ),
        "expected origins[1] to be Fs, got {:?}",
        resolved.origins[1]
    );
    Ok(())
}

#[test]
fn resolve_cache_rejects_both_origin_and_origins() -> anyhow::Result<()> {
    // Each wire form names a different fallback policy. Setting
    // both is an operator mistake the resolver must surface, not
    // silently pick a winner.
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "https://one.example/".to_string(),
            decompress: None,
        }),
        origins: Some(vec![types::OriginConfig::Http {
            url: "https://two.example/".to_string(),
            decompress: None,
        }]),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected mutual-exclusion rejection"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("cache.origin")
            && msg.contains("cache.origins")
            && msg.contains("mutually exclusive"),
        "error lacked both keys and exclusivity marker: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_cache_rejects_empty_origins_array() -> anyhow::Result<()> {
    // `origins = []` is almost certainly a half-finished config
    // edit (operator meant to populate it later). Reject at load
    // so the surprise lands at startup, not at the first cache
    // miss hours later.
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origins: Some(Vec::new()),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected empty-array rejection"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("cache.origins") && msg.contains("at least one entry"),
        "error lacked the at-least-one-entry guidance: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_cache_origin_singular_resolves_to_one_element_vec() -> anyhow::Result<()> {
    // The singular `[cache.origin]` table resolves to the same
    // one-element vec as a single `[[cache.origins]]` entry: the
    // resolver collapses both wire forms into a vec, and a length-1
    // vec is the canonical representation of the singular form.
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "https://only.example/".to_string(),
            decompress: None,
        }),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.origins.len() == 1,
        "singular [cache.origin] must produce a 1-element vec, got len {}",
        resolved.origins.len(),
    );
    anyhow::ensure!(
        matches!(
            resolved.origins[0],
            crate::config::ResolvedOrigin::Http { .. }
        ),
        "expected the single origin to be Http"
    );
    Ok(())
}

#[test]
fn resolve_cache_no_origin_resolves_to_empty_vec() -> anyhow::Result<()> {
    // Absent both `[cache.origin]` and `[[cache.origins]]` means "no
    // pull-through configured". The resolver returns an empty vec;
    // engine's `pull_through` short-circuits to NoOrigin.
    let cli = empty_cache_args();
    let toml = types::CacheConfig::default();
    let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.origins.is_empty(),
        "absent origin section must yield empty vec, got len {}",
        resolved.origins.len(),
    );
    Ok(())
}

#[test]
fn resolve_cache_relay_foreign_namespaces_defaults_false_with_origin() -> anyhow::Result<()> {
    // Role-derived default (#1759): a node with an origin backend
    // configured is an origin, not a general proxy — absent an explicit
    // override it defaults to origin-only (`false`).
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "https://origin.example/".to_string(),
            decompress: None,
        }),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
    anyhow::ensure!(
        !resolved.relay_foreign_namespaces,
        "a node with an origin configured must default to origin-only (relay_foreign_namespaces = false)"
    );
    Ok(())
}

#[test]
fn resolve_cache_relay_foreign_namespaces_defaults_true_without_origin() -> anyhow::Result<()> {
    // Role-derived default (#1759): a node with no origin backend is a
    // pure relay edge — its only function is relaying, so absent an
    // explicit override it defaults to relay (`true`).
    let cli = empty_cache_args();
    let toml = types::CacheConfig::default();
    let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.relay_foreign_namespaces,
        "a node with no origin configured must default to relay (relay_foreign_namespaces = true)"
    );
    Ok(())
}

#[test]
fn resolve_cache_relay_foreign_namespaces_explicit_true_overrides_origin_default()
-> anyhow::Result<()> {
    // Explicit config always wins: an origin node may opt back into
    // relay to also earn relay revenue.
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "https://origin.example/".to_string(),
            decompress: None,
        }),
        relay_foreign_namespaces: Some(true),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.relay_foreign_namespaces,
        "an explicit relay_foreign_namespaces = true must override the origin-only default"
    );
    Ok(())
}

#[test]
fn resolve_cache_accepts_duplicate_origins_without_erroring() -> anyhow::Result<()> {
    // A duplicate records a notice but must not fail config
    // resolution — operators legitimately use duplicates for
    // connection-pool sharding. This test pins the non-erroring
    // contract so the notice cannot become a hard error unnoticed.
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origins: Some(vec![
            types::OriginConfig::Http {
                url: "https://shared.example/".to_string(),
                decompress: None,
            },
            types::OriginConfig::Http {
                url: "https://shared.example/".to_string(),
                decompress: None,
            },
        ]),
        ..Default::default()
    };
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_cache_into(&cli, Some(&toml), Path::new("/tmp"), &mut bag);
    let notices = bag.take_notices();
    bag.into_result()?;
    anyhow::ensure!(
        resolved.origins.len() == 2,
        "duplicate origins must both survive into the resolved vec"
    );
    let notice = notices.first().ok_or_else(|| {
        anyhow::anyhow!("the duplicate must reach the operator, not just the resolved vec")
    })?;
    anyhow::ensure!(notices.len() == 1, "one notice per duplicate: {notices:?}");
    // Indexed at the *second* entry: the operator needs to know which
    // line to delete, and the first occurrence is the one to keep.
    anyhow::ensure!(notice.field == "cache.origins[1]", "{notice:?}");
    anyhow::ensure!(notice.level == ConfigNoticeLevel::Warn, "{notice:?}");
    anyhow::ensure!(
        notice.message.contains("duplicates an earlier entry"),
        "{notice:?}"
    );
    Ok(())
}

#[test]
fn resolve_cache_origins_propagates_per_entry_error_with_index() -> anyhow::Result<()> {
    // The second entry has an empty URL — the existing
    // `resolve_origin` validator should reject it, and the wrapper
    // must thread the `cache.origins[1]` index into the error
    // context so an operator with three entries can identify which
    // one is broken.
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origins: Some(vec![
            types::OriginConfig::Http {
                url: "https://good.example/".to_string(),
                decompress: None,
            },
            types::OriginConfig::Http {
                url: String::new(),
                decompress: None,
            },
        ]),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected per-entry validation rejection"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("cache.origins[1]") && msg.contains("must not be empty"),
        "error lacked the indexed context: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_cache_origins_accumulates_every_bad_entry() -> anyhow::Result<()> {
    // Intra-section accumulation: two malformed origins at indices 0
    // and 2 (valid at 1) must both be reported by index. A regression
    // to fail-fast on the first bad entry would only report `[0]`.
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origins: Some(vec![
            types::OriginConfig::Http {
                url: String::new(), // idx 0: empty URL
                decompress: None,
            },
            types::OriginConfig::Http {
                url: "https://good.example/".to_string(), // idx 1: valid
                decompress: None,
            },
            types::OriginConfig::Fs {
                path: std::path::PathBuf::new(), // idx 2: empty path
            },
        ]),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected per-entry validation rejection"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("cache.origins[0]") && msg.contains("cache.origins[2]"),
        "both bad entries must be named by index: {msg}"
    );
    anyhow::ensure!(
        !msg.contains("cache.origins[1]"),
        "the valid entry must not be reported: {msg}"
    );
    Ok(())
}

// resolve_cache must reject a non-http(s) URL at config resolution
// instead of deferring the check to engine wiring. This locks in the
// "single parser" invariant introduced by `parse_origin_url`.
#[test]
fn resolve_cache_rejects_non_http_origin_url() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "file:///etc/passwd".to_string(),
            decompress: None,
        }),
        ..Default::default()
    };
    let err = resolve_cache(&cli, Some(&toml), std::path::Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected scheme rejection"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("invalid cache.origin") || msg.contains("unsupported origin URL scheme"),
        "error lacked context: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_payment_rejects_zero_from_cli() -> anyhow::Result<()> {
    let cli = crate::cli::run::PaymentArgs {
        rate_per_mb: Some(0),
    };
    let err = resolve_payment(&cli, None)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection for rate_per_mb=0"))?
        .to_string();
    anyhow::ensure!(
        err.contains("rate_per_mb") && err.contains("> 0"),
        "error lacked context: {err}"
    );
    Ok(())
}

// Origin variant from TOML resolves into a typed `ResolvedOrigin::Http`
// that round-trips the parsed URL (#437). The origin has no CLI flag, so
// there is no CLI-vs-TOML precedence to test; this is a smoke test that
// the TOML form makes it through resolution without dropping anything.
#[test]
fn resolve_cache_origin_http_from_toml_round_trips() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "https://origin.example/".to_string(),
            decompress: None,
        }),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&toml), std::path::Path::new("/tmp"))?;
    let url = match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::Http { url, .. }) => url,
        other => anyhow::bail!("expected Http origin, got: {other:?}"),
    };
    anyhow::ensure!(
        url.as_url().as_str() == "https://origin.example/",
        "got: {url}"
    );
    Ok(())
}

#[test]
fn resolve_payment_rejects_zero_from_file() -> anyhow::Result<()> {
    let cli = crate::cli::run::PaymentArgs { rate_per_mb: None };
    let file = types::PaymentConfig {
        rate_per_mb: Some(0),
        credit_max: None,
        credit_ramp_divisor: None,
        frame_target_bytes: None,
        voucher_commit_interval_ms: None,
    };
    let err = resolve_payment(&cli, Some(&file))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection for rate_per_mb=0"))?
        .to_string();
    anyhow::ensure!(
        err.contains("rate_per_mb") && err.contains("> 0"),
        "error lacked context: {err}"
    );
    Ok(())
}

// Filesystem origin variant resolves into the typed
// `ResolvedOrigin::Fs` that the runtime's `build_cache` enum match
// dispatches on (#437).
#[test]
fn resolve_cache_origin_fs_round_trips() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let toml = types::CacheConfig {
        origin: Some(types::OriginConfig::Fs {
            path: PathBuf::from("/tmp/origin"),
        }),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&toml), std::path::Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::Fs { path }) => {
            anyhow::ensure!(path == Path::new("/tmp/origin"), "path: {}", path.display());
        }
        other => anyhow::bail!("expected Fs origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_payment_cli_overrides_file_and_passes_nonzero() -> anyhow::Result<()> {
    // Regression guard for the merge order: a zero file value must not
    // short-circuit the CLI override that would otherwise be valid.
    let cli = crate::cli::run::PaymentArgs {
        rate_per_mb: Some(42),
    };
    let file = types::PaymentConfig {
        rate_per_mb: Some(0),
        credit_max: None,
        credit_ramp_divisor: None,
        frame_target_bytes: None,
        voucher_commit_interval_ms: None,
    };
    let resolved = resolve_payment(&cli, Some(&file))?;
    anyhow::ensure!(resolved.rate_per_mb == 42, "got: {}", resolved.rate_per_mb);
    Ok(())
}

#[test]
fn resolve_payment_defaults_when_unset() -> anyhow::Result<()> {
    let cli = crate::cli::run::PaymentArgs { rate_per_mb: None };
    let resolved = resolve_payment(&cli, None)?;
    anyhow::ensure!(
        resolved.rate_per_mb == DEFAULT_RATE_PER_MB,
        "got: {}",
        resolved.rate_per_mb
    );
    anyhow::ensure!(
        resolved.credit_max == DEFAULT_CREDIT_MAX,
        "credit_max default, got: {}",
        resolved.credit_max
    );
    anyhow::ensure!(
        resolved.credit_ramp_divisor == DEFAULT_CREDIT_RAMP_DIVISOR,
        "credit_ramp_divisor default, got: {}",
        resolved.credit_ramp_divisor
    );
    anyhow::ensure!(
        resolved.voucher_commit_interval_ms == DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
        "voucher_commit_interval_ms default, got: {}",
        resolved.voucher_commit_interval_ms
    );
    Ok(())
}

#[test]
fn resolve_payment_threads_explicit_commit_interval() -> anyhow::Result<()> {
    // A non-zero explicit value threads through rather than falling back to
    // the default.
    for set in [Some(20u64), Some(1_000)] {
        let file = types::PaymentConfig {
            rate_per_mb: Some(10),
            credit_max: None,
            credit_ramp_divisor: None,
            frame_target_bytes: None,
            voucher_commit_interval_ms: set,
        };
        let resolved = resolve_payment(&empty_payment_args(), Some(&file))?;
        anyhow::ensure!(
            resolved.voucher_commit_interval_ms == set.unwrap_or_default(),
            "explicit commit interval threaded, got: {}",
            resolved.voucher_commit_interval_ms
        );
    }
    Ok(())
}

#[test]
fn resolve_payment_rejects_zero_commit_interval() -> anyhow::Result<()> {
    let file = types::PaymentConfig {
        voucher_commit_interval_ms: Some(0),
        ..Default::default()
    };
    let err = resolve_payment(&empty_payment_args(), Some(&file))
        .expect_err("zero flush interval must be rejected");
    anyhow::ensure!(err.to_string().contains("voucher_commit_interval_ms"));
    Ok(())
}

#[test]
fn resolve_payment_defaults_credit_max_and_ramp_divisor() -> anyhow::Result<()> {
    let resolved = resolve_payment(&empty_payment_args(), None)?;
    anyhow::ensure!(
        resolved.credit_max == DEFAULT_CREDIT_MAX,
        "got: {}",
        resolved.credit_max
    );
    anyhow::ensure!(
        resolved.credit_ramp_divisor == DEFAULT_CREDIT_RAMP_DIVISOR,
        "got: {}",
        resolved.credit_ramp_divisor
    );
    Ok(())
}

#[test]
fn resolve_payment_threads_explicit_credit_max_and_ramp_divisor() -> anyhow::Result<()> {
    let file = types::PaymentConfig {
        rate_per_mb: Some(10),
        credit_max: Some(decdn_config_types::Bytes::new(32 * 1024 * 1024)),
        credit_ramp_divisor: Some(5),
        frame_target_bytes: None,
        voucher_commit_interval_ms: None,
    };
    let resolved = resolve_payment(&empty_payment_args(), Some(&file))?;
    anyhow::ensure!(
        resolved.credit_max == 32 * 1024 * 1024,
        "credit_max threaded, got: {}",
        resolved.credit_max
    );
    anyhow::ensure!(
        resolved.credit_ramp_divisor == 5,
        "credit_ramp_divisor threaded, got: {}",
        resolved.credit_ramp_divisor
    );
    Ok(())
}

// Issue #378: configuring a rate above the protocol-level wire ceiling
// is a startup error — otherwise the node would publish ProbeResponses
// that every honest client decoder rejects, silently dropping itself
// out of the candidate pool.
#[test]
fn resolve_payment_rejects_rate_above_protocol_max() -> anyhow::Result<()> {
    let cli = crate::cli::run::PaymentArgs {
        rate_per_mb: Some(decdn_protocol::MAX_RATE_PER_MB + 1),
    };
    let err = resolve_payment(&cli, None)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection above MAX_RATE_PER_MB"))?
        .to_string();
    anyhow::ensure!(
        err.contains("MAX_RATE_PER_MB") && err.contains("#378"),
        "error lacked MAX_RATE_PER_MB / issue #378 context: {err}"
    );
    Ok(())
}

#[test]
fn resolve_payment_accepts_rate_at_protocol_max() -> anyhow::Result<()> {
    let cli = crate::cli::run::PaymentArgs {
        rate_per_mb: Some(decdn_protocol::MAX_RATE_PER_MB),
    };
    let resolved = resolve_payment(&cli, None)?;
    anyhow::ensure!(
        resolved.rate_per_mb == decdn_protocol::MAX_RATE_PER_MB,
        "expected MAX_RATE_PER_MB; got {}",
        resolved.rate_per_mb,
    );
    Ok(())
}

fn cache_cli(
    cache_size_mb: Option<u64>,
    max_blob_size_mb: Option<u64>,
) -> crate::cli::run::CacheArgs {
    crate::cli::run::CacheArgs {
        cache_dir: None,
        cache_size_mb,
        max_blob_size_mb,
        max_rate_per_mb: None,
        max_probe_holds: None,
        stake_lane_reserved_holds: None,
    }
}

#[test]
fn resolve_cache_accepts_max_blob_equal_to_cache_size() -> anyhow::Result<()> {
    // Equality is the default (unset => cache_size_mb) and is allowed:
    // the disk budget is the admission ceiling.
    let cli = cache_cli(Some(100), Some(100));
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.max_blob_size_mb == 100,
        "max_blob: {}",
        resolved.max_blob_size_mb
    );
    Ok(())
}

#[test]
fn resolve_cache_rejects_max_blob_greater_than_cache_size() -> anyhow::Result<()> {
    let cli = cache_cli(Some(100), Some(200));
    let err = resolve_cache(&cli, None, Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection for max > cache"))?
        .to_string();
    anyhow::ensure!(
        err.contains("must not exceed"),
        "error lacked context: {err}"
    );
    Ok(())
}

#[test]
fn resolve_cache_accepts_max_blob_below_cache_size() -> anyhow::Result<()> {
    let cli = cache_cli(Some(1024), Some(512));
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.cache_size_mb == 1024,
        "cache_size: {}",
        resolved.cache_size_mb
    );
    anyhow::ensure!(
        resolved.max_blob_size_mb == 512,
        "max_blob: {}",
        resolved.max_blob_size_mb
    );
    Ok(())
}

// ----- pinned_hashes / decompress (#276, #312) -----

fn make_hex_hash(seed: u8) -> String {
    use std::fmt::Write as _;

    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        // 32-element array, so usize→u8 always fits.
        let i_u8 = u8::try_from(i).unwrap_or(0);
        *b = i_u8.wrapping_add(seed);
    }
    // 64 lowercase hex chars — matches the BLAKE3 wire form.
    let mut s = String::with_capacity(64);
    for b in bytes {
        // write! to a String is infallible; the `_` swallows the
        // formal Result without invoking the workspace's expect_used
        // lint.
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[test]
fn parse_pinned_hashes_accepts_valid_lowercase_hex() -> anyhow::Result<()> {
    let h1 = make_hex_hash(1);
    let h2 = make_hex_hash(2);
    let raw = vec![h1.clone(), h2.clone()];
    let parsed = parse_pinned_hashes(Some(&raw))?;
    anyhow::ensure!(parsed.len() == 2, "expected 2 hashes, got {}", parsed.len());
    Ok(())
}

#[test]
fn parse_pinned_hashes_deduplicates() -> anyhow::Result<()> {
    let h = make_hex_hash(7);
    let raw = vec![h.clone(), h.clone(), h];
    let parsed = parse_pinned_hashes(Some(&raw))?;
    anyhow::ensure!(parsed.len() == 1, "duplicates should collapse");
    Ok(())
}

#[test]
fn parse_pinned_hashes_rejects_wrong_length() -> anyhow::Result<()> {
    let raw = vec!["abcd".to_string()]; // 4 chars, not 64
    let err = parse_pinned_hashes(Some(&raw))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("64 hex chars"),
        "error should mention length: {msg}"
    );
    Ok(())
}

#[test]
fn parse_pinned_hashes_rejects_non_hex_chars() -> anyhow::Result<()> {
    // 64 chars but contains 'z' which is not hex.
    let bad: String = std::iter::repeat_n('z', 64).collect();
    let raw = vec![bad];
    let err = parse_pinned_hashes(Some(&raw))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("lowercase hex"),
        "error should mention hex: {msg}"
    );
    Ok(())
}

#[test]
fn parse_pinned_hashes_rejects_uppercase_hex() -> anyhow::Result<()> {
    // A copy-paste from a UI that upper-cased the digest is the most
    // likely operator mistake. Reject explicitly so they get a clear
    // error rather than a half-pinned set.
    let bad: String = std::iter::repeat_n('A', 64).collect();
    let raw = vec![bad];
    let err = parse_pinned_hashes(Some(&raw))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(msg.contains("lowercase"), "got: {msg}");
    Ok(())
}

#[test]
fn parse_pinned_hashes_empty_or_none_yields_empty_set() -> anyhow::Result<()> {
    anyhow::ensure!(parse_pinned_hashes(None)?.is_empty());
    let raw: Vec<String> = vec![];
    anyhow::ensure!(parse_pinned_hashes(Some(&raw))?.is_empty());
    Ok(())
}

// --- ADR 011 local denylist (`[content]`, #1168) ------------------------

#[test]
fn parse_denied_hashes_accepts_bare_lowercase_hex() -> anyhow::Result<()> {
    let raw = vec!["ab".repeat(32)];
    let parsed = parse_denied_hashes(Some(&raw))?;
    anyhow::ensure!(parsed.len() == 1);
    anyhow::ensure!(parsed.contains(&decdn_config_types::Hash::from_bytes([0xab; 32])));
    Ok(())
}

/// ADR 011 §Local Denylist writes the TOML with a `blake3:` prefix; the
/// implementation follows `cache.pinned_hashes`' bare-hex spelling instead
/// so an operator has ONE hash format across the config file, and the ADR
/// example was amended to match. Pin the rejection so the two cannot drift
/// back apart silently.
#[test]
fn parse_denied_hashes_rejects_the_blake3_prefix() {
    let raw = vec![format!("blake3:{}", "ab".repeat(32))];
    let err = parse_denied_hashes(Some(&raw)).expect_err("prefixed form must be rejected");
    assert!(format!("{err:#}").contains("64 hex chars"), "{err:#}");
}

#[test]
fn parse_denied_hashes_rejects_uppercase_and_wrong_length() {
    assert!(parse_denied_hashes(Some(&["AB".repeat(32)])).is_err());
    assert!(parse_denied_hashes(Some(&["ab".repeat(31)])).is_err());
}

/// Errors must name `content.denied_hashes`, not `cache.pinned_hashes` —
/// the two share a parser, and a mislabelled error would send an operator
/// discharging a takedown to the wrong config key.
#[test]
fn parse_denied_hashes_errors_name_their_own_field() {
    let err = parse_denied_hashes(Some(&["nope".to_string()])).expect_err("must reject");
    let msg = format!("{err:#}");
    assert!(msg.contains("content.denied_hashes"), "{msg}");
    assert!(!msg.contains("pinned"), "{msg}");
}

#[test]
fn parse_denied_hashes_none_and_empty_are_both_empty() -> anyhow::Result<()> {
    anyhow::ensure!(parse_denied_hashes(None)?.is_empty());
    let raw: Vec<String> = vec![];
    anyhow::ensure!(parse_denied_hashes(Some(&raw))?.is_empty());
    Ok(())
}

#[test]
fn parse_denied_origins_accepts_addresses_and_rejects_zero() -> anyhow::Result<()> {
    let raw = vec!["0x000000000000000000000000000000000000dEaD".to_string()];
    anyhow::ensure!(parse_denied_origins(Some(&raw))?.len() == 1);
    let zero = vec!["0x0000000000000000000000000000000000000000".to_string()];
    assert!(parse_denied_origins(Some(&zero)).is_err(), "zero rejected");
    assert!(parse_denied_origins(Some(&["nope".to_string()])).is_err());
    Ok(())
}

#[test]
fn parse_denied_origins_zero_error_advises_an_operator_not_a_contract() {
    let zero = vec!["0x0000000000000000000000000000000000000000".to_string()];
    let err = parse_denied_origins(Some(&zero))
        .expect_err("zero rejected")
        .to_string();
    assert!(
        err.contains("operator address"),
        "denylist zero-address advice should be operator-oriented: {err}"
    );
    assert!(
        !err.contains("deployed contract address"),
        "denylist must not reuse the contract-address hint: {err}"
    );
}

#[test]
fn pinned_and_denied_hash_collision_is_rejected() -> anyhow::Result<()> {
    let shared = "cd".repeat(32);
    let cli = cache_cli(None, None);
    let cache_file = types::CacheConfig {
        pinned_hashes: Some(vec![shared.clone(), "ab".repeat(32)]),
        ..types::CacheConfig::default()
    };
    let content_file = types::ContentConfig {
        denied_hashes: Some(vec![shared.clone()]),
        ..types::ContentConfig::default()
    };
    let mut bag = ConfigDiagnostics::new();
    let cache = resolve_cache_into(&cli, Some(&cache_file), Path::new("/tmp"), &mut bag);
    let content = resolve_content_into(Some(&content_file), &mut bag);
    bag.into_result()?; // each section parses fine on its own

    let err = ensure_no_hash_pinned_and_denied(&cache, &content)
        .expect_err("a hash in both lists must be rejected")
        .to_string();
    assert!(
        err.contains(&shared),
        "error should name the colliding hash: {err}"
    );
    assert!(
        err.contains("content.denied_hashes"),
        "error should name the field: {err}"
    );
    Ok(())
}

#[test]
fn disjoint_pinned_and_denied_hashes_are_accepted() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let cache_file = types::CacheConfig {
        pinned_hashes: Some(vec!["ab".repeat(32)]),
        ..types::CacheConfig::default()
    };
    let content_file = types::ContentConfig {
        denied_hashes: Some(vec!["cd".repeat(32)]),
        ..types::ContentConfig::default()
    };
    let mut bag = ConfigDiagnostics::new();
    let cache = resolve_cache_into(&cli, Some(&cache_file), Path::new("/tmp"), &mut bag);
    let content = resolve_content_into(Some(&content_file), &mut bag);
    bag.into_result()?;
    ensure_no_hash_pinned_and_denied(&cache, &content)?;
    Ok(())
}

#[test]
fn resolve_content_from_file_config() -> anyhow::Result<()> {
    let file: FileConfig = toml::from_str(&format!(
        "[content]\ndenied_hashes = [\"{}\"]\ndenied_origins = [\"0x000000000000000000000000000000000000dEaD\"]\n",
        "cd".repeat(32)
    ))?;
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_content_into(file.content.as_ref(), &mut bag);
    bag.into_result()?;
    anyhow::ensure!(resolved.denied_hashes.len() == 1);
    anyhow::ensure!(resolved.denied_origins.len() == 1);
    Ok(())
}

#[test]
fn resolve_content_absent_section_denies_nothing() {
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_content_into(None, &mut bag);
    assert!(bag.into_result().is_ok());
    assert!(resolved.denied_hashes.is_empty());
    assert!(resolved.denied_origins.is_empty());
}

#[test]
fn resolve_cache_no_origin_and_pinned_empty_by_default() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.origins.is_empty(),
        "no [cache.origin]/[[cache.origins]] => no pull-through"
    );
    anyhow::ensure!(resolved.pinned_hashes.is_empty());
    Ok(())
}

#[test]
fn resolve_cache_user_agent_defaults_to_workspace_constant() -> anyhow::Result<()> {
    // Absent => DEFAULT_USER_AGENT (#435).
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.user_agent == decdn_config_types::DEFAULT_USER_AGENT,
        "expected default UA, got: {}",
        resolved.user_agent
    );
    Ok(())
}

#[test]
fn resolve_cache_user_agent_from_file_overrides_default() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        user_agent: Some("MyCdn/1.0 (+ops@example.com)".to_string()),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.user_agent == "MyCdn/1.0 (+ops@example.com)",
        "got: {}",
        resolved.user_agent
    );
    Ok(())
}

#[test]
fn resolve_cache_user_agent_invalid_falls_back_to_default() {
    // Present-but-invalid UA must both record the `cache.user_agent`
    // problem AND leave the resolved struct carrying
    // `DEFAULT_USER_AGENT`. The fallback matters because
    // `resolve_config` keeps accumulating across sections after this
    // call; a `ResolvedCache` carrying the invalid bytes would smuggle
    // them past the resolver into the eventual `reqwest::Client::builder`.
    // The shim `resolve_cache` returns `Err` and drops the partial
    // value, so the test drives `resolve_cache_into` directly to
    // observe the field.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        user_agent: Some("evil\r\nX-Inject: 1".to_string()),
        ..types::CacheConfig::default()
    };
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_cache_into(&cli, Some(&file), Path::new("/tmp"), &mut bag);
    assert!(
        bag.has_field("cache.user_agent"),
        "expected cache.user_agent problem to be recorded"
    );
    assert_eq!(
        resolved.user_agent,
        decdn_config_types::DEFAULT_USER_AGENT,
        "invalid UA must fall back to DEFAULT_USER_AGENT, got: {}",
        resolved.user_agent
    );
}

#[test]
fn resolve_cache_gc_interval_defaults_when_absent() -> anyhow::Result<()> {
    // Absent => DEFAULT_GC_INTERVAL_SEC (#518).
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.gc_interval_sec == DEFAULT_GC_INTERVAL_SEC,
        "expected default {}, got: {}",
        DEFAULT_GC_INTERVAL_SEC,
        resolved.gc_interval_sec
    );
    Ok(())
}

#[test]
fn resolve_cache_gc_interval_from_file_overrides_default() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        gc_interval_sec: Some(42),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.gc_interval_sec == 42,
        "expected 42, got: {}",
        resolved.gc_interval_sec
    );
    Ok(())
}

#[test]
fn resolve_cache_fs_rescan_interval_defaults_when_absent() -> anyhow::Result<()> {
    // Absent => DEFAULT_FS_RESCAN_INTERVAL_SEC (#1130).
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.fs_rescan_interval_sec == DEFAULT_FS_RESCAN_INTERVAL_SEC,
        "expected default {}, got: {}",
        DEFAULT_FS_RESCAN_INTERVAL_SEC,
        resolved.fs_rescan_interval_sec
    );
    Ok(())
}

#[test]
fn resolve_cache_fs_rescan_interval_from_file_overrides_default() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        fs_rescan_interval_sec: Some(30),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.fs_rescan_interval_sec == 30,
        "expected 30, got: {}",
        resolved.fs_rescan_interval_sec
    );
    Ok(())
}

#[test]
fn resolve_cache_gc_interval_zero_disables() -> anyhow::Result<()> {
    // `0` is the documented "disable periodic GC" sentinel — no
    // clamp/floor should turn it back on. A regression that
    // saturated to a minimum would silently re-enable GC for
    // operators who explicitly opted out.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        gc_interval_sec: Some(0),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.gc_interval_sec == 0,
        "0 must round-trip as 0 (disabled); got: {}",
        resolved.gc_interval_sec
    );
    Ok(())
}

#[test]
fn resolve_cache_stake_lane_reserved_holds_defaults_to_zero() -> anyhow::Result<()> {
    // Absent everywhere => reservation off (#757). The default MUST be 0
    // so a node that never opted in reserves no stake-lane holds.
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.stake_lane_reserved_holds == DEFAULT_STAKE_LANE_RESERVED_HOLDS,
        "expected default {DEFAULT_STAKE_LANE_RESERVED_HOLDS}, got: {}",
        resolved.stake_lane_reserved_holds
    );
    anyhow::ensure!(
        resolved.stake_lane_reserved_holds == 0,
        "the #757 default must be 0 (reservation off)"
    );
    Ok(())
}

#[test]
fn resolve_cache_stake_lane_reserved_holds_from_file() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        stake_lane_reserved_holds: Some(8),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.stake_lane_reserved_holds == 8,
        "expected 8 from file, got: {}",
        resolved.stake_lane_reserved_holds
    );
    Ok(())
}

#[test]
fn resolve_cache_stake_lane_reserved_holds_cli_overrides_file() -> anyhow::Result<()> {
    // CLI wins over file, mirroring the `max_probe_holds` precedence.
    let cli = crate::cli::run::CacheArgs {
        cache_dir: None,
        cache_size_mb: None,
        max_blob_size_mb: None,
        max_rate_per_mb: None,
        max_probe_holds: None,
        stake_lane_reserved_holds: Some(3),
    };
    let file = types::CacheConfig {
        stake_lane_reserved_holds: Some(8),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.stake_lane_reserved_holds == 3,
        "CLI flag must override the file value; expected 3, got: {}",
        resolved.stake_lane_reserved_holds
    );
    Ok(())
}

#[test]
fn resolve_cache_rejects_empty_user_agent() {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        user_agent: Some(String::new()),
        ..types::CacheConfig::default()
    };
    let err =
        resolve_cache(&cli, Some(&file), Path::new("/tmp")).expect_err("empty UA must reject");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("cache.user_agent"),
        "missing field name: {msg}"
    );
}

/// Reject control-byte bytes at config load — most importantly CR/LF, which
/// would let an operator-supplied (or env-expanded) value smuggle a second
/// header onto every origin request. Also covers NUL, other C0 controls,
/// DEL, and non-ASCII obs-text. Without this, the value reaches
/// `reqwest::Client::builder` and surfaces as a generic build error from
/// inside `HttpOrigin::new_with_user_agent` at startup, which doesn't tell
/// the operator which config field is to blame.
#[test]
fn resolve_cache_rejects_user_agent_with_control_bytes() {
    let cli = cache_cli(None, None);
    for bad in [
        "evil\r\nX-Inject: 1",
        "has\nlf",
        "has\rcr",
        "nul\0byte",
        "del\x7fbyte",
        "non-ascii-\u{00e9}",
    ] {
        let file = types::CacheConfig {
            user_agent: Some(bad.to_string()),
            ..types::CacheConfig::default()
        };
        let result = resolve_cache(&cli, Some(&file), Path::new("/tmp"));
        assert!(result.is_err(), "UA `{bad:?}` must reject but resolved OK");
    }
    // Pin the message shape on one representative case so a regression in
    // error context is caught (e.g. losing the field name or the byte
    // position).
    let file = types::CacheConfig {
        user_agent: Some("crlf\r\ninjection".to_string()),
        ..types::CacheConfig::default()
    };
    let err = resolve_cache(&cli, Some(&file), Path::new("/tmp")).expect_err("CRLF UA must reject");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("cache.user_agent") && msg.contains("invalid byte"),
        "error should name the field and describe the byte, got: {msg}"
    );
}

/// Tab is part of the legal HTTP header-value byte set and shows up in
/// real-world UAs occasionally; make sure the validator doesn't over-reject.
#[test]
fn resolve_cache_accepts_user_agent_with_tab() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        user_agent: Some("MyCdn/1.0\t(ops@example.com)".to_string()),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.user_agent == "MyCdn/1.0\t(ops@example.com)",
        "got: {}",
        resolved.user_agent
    );
    Ok(())
}

#[test]
fn resolve_cache_http_decompress_strict_via_file() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "https://origin.example/".to_string(),
            decompress: Some(decdn_config_types::DecompressMode::Strict),
        }),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::Http { decompress, .. }) => {
            anyhow::ensure!(matches!(
                decompress,
                decdn_config_types::DecompressMode::Strict
            ));
        }
        other => anyhow::bail!("expected Http origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_cache_s3_decompress_strict_via_file() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = cache_with_s3(types::S3OriginConfig {
        decompress: Some(decdn_config_types::DecompressMode::Strict),
        ..s3_cfg("decdn-blobs")
    });
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(matches!(
                s3.decompress,
                decdn_config_types::DecompressMode::Strict
            ));
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_cache_s3_decompress_defaults_to_auto() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = cache_with_s3(s3_cfg("decdn-blobs"));
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(matches!(
                s3.decompress,
                decdn_config_types::DecompressMode::Auto
            ));
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_cache_http_decompress_defaults_to_auto() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin: Some(types::OriginConfig::Http {
            url: "https://origin.example/".to_string(),
            decompress: None,
        }),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::Http { decompress, .. }) => {
            anyhow::ensure!(matches!(
                decompress,
                decdn_config_types::DecompressMode::Auto
            ));
        }
        other => anyhow::bail!("expected Http origin, got: {other:?}"),
    }
    Ok(())
}

// ----- #437: S3 origin schema -----
//
// The tests below pin the shape of the schema validators and the
// resolved-form normalization. The runtime backend (`S3Origin`)
// lives in `decdn-cache` and is exercised separately by
// `crates/cache/tests/s3_origin.rs`; these tests stay focused on
// the resolver contract that feeds it.

/// Build a TOML S3 origin with sane defaults; tests override
/// individual fields. Avoids 6-line struct literals at every call
/// site.
fn s3_cfg(bucket: &str) -> types::S3OriginConfig {
    types::S3OriginConfig {
        bucket: bucket.to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: None,
        prefix: None,
        credentials: None,
        decompress: None,
    }
}

fn cache_with_s3(s3: types::S3OriginConfig) -> types::CacheConfig {
    types::CacheConfig {
        origin: Some(types::OriginConfig::S3(s3)),
        ..types::CacheConfig::default()
    }
}

#[test]
fn resolve_cache_origin_s3_happy_path() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = cache_with_s3(types::S3OriginConfig {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: Some("https://r2.cloudflarestorage.com".to_string()),
        path_style: Some(true),
        prefix: Some("blobs".to_string()),
        credentials: None,
        decompress: None,
    });
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(s3.bucket == "decdn-blobs", "bucket: {}", s3.bucket);
            anyhow::ensure!(s3.region == "us-east-1", "region: {}", s3.region);
            anyhow::ensure!(s3.path_style, "path_style should round-trip true");
            // Trailing-slash auto-append.
            anyhow::ensure!(s3.prefix == "blobs/", "prefix: {}", s3.prefix);
            let endpoint = s3
                .endpoint_url
                .ok_or_else(|| anyhow::anyhow!("endpoint missing"))?;
            anyhow::ensure!(
                endpoint.as_url().as_str() == "https://r2.cloudflarestorage.com/",
                "endpoint: {endpoint}"
            );
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_s3_origin_prefix_trailing_slash_already_present_unchanged() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let mut s3 = s3_cfg("decdn-blobs");
    s3.prefix = Some("foo/bar/".to_string());
    let resolved = resolve_cache(&cli, Some(&cache_with_s3(s3)), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(s3.prefix == "foo/bar/", "prefix: {}", s3.prefix);
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_s3_origin_empty_prefix_stays_empty() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let mut s3 = s3_cfg("decdn-blobs");
    s3.prefix = Some(String::new());
    let resolved = resolve_cache(&cli, Some(&cache_with_s3(s3)), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(s3.prefix.is_empty(), "prefix should remain empty");
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_s3_origin_path_style_none_collapses_to_false() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(
        &cli,
        Some(&cache_with_s3(s3_cfg("decdn-blobs"))),
        Path::new("/tmp"),
    )?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(
                !s3.path_style,
                "path_style absent => false (SDK default = virtual-hosted)"
            );
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

/// Helper: assert that a TOML-form S3 config rejects with an
/// error whose chained message contains every required fragment.
fn assert_s3_rejects(s3: types::S3OriginConfig, fragments: &[&str]) -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let err = resolve_cache(&cli, Some(&cache_with_s3(s3)), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection"))?;
    let msg = format!("{err:#}");
    for fragment in fragments {
        anyhow::ensure!(
            msg.contains(fragment),
            "error did not contain `{fragment}`: {msg}"
        );
    }
    Ok(())
}

#[test]
fn validate_bucket_rejects_too_short() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg("ab"), &["cache.origin.bucket", "3..=63"])
}

#[test]
fn validate_bucket_accepts_min_length_three() -> anyhow::Result<()> {
    // Boundary: exactly 3 chars must pass.
    let cli = cache_cli(None, None);
    let _ = resolve_cache(&cli, Some(&cache_with_s3(s3_cfg("abc"))), Path::new("/tmp"))?;
    Ok(())
}

#[test]
fn validate_bucket_rejects_too_long() -> anyhow::Result<()> {
    let name: String = std::iter::repeat_n('a', 64).collect();
    assert_s3_rejects(s3_cfg(&name), &["cache.origin.bucket", "3..=63"])
}

#[test]
fn validate_bucket_accepts_max_length_sixty_three() -> anyhow::Result<()> {
    let name: String = std::iter::repeat_n('a', 63).collect();
    let cli = cache_cli(None, None);
    let _ = resolve_cache(&cli, Some(&cache_with_s3(s3_cfg(&name))), Path::new("/tmp"))?;
    Ok(())
}

#[test]
fn validate_bucket_rejects_uppercase() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg("MyBucket"), &["lowercase"])
}

#[test]
fn validate_bucket_rejects_underscore() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg("my_bucket"), &["lowercase"])
}

#[test]
fn validate_bucket_rejects_leading_dot() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg(".mybucket"), &["begin"])
}

#[test]
fn validate_bucket_rejects_trailing_dot() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg("mybucket."), &["end"])
}

#[test]
fn validate_bucket_rejects_leading_hyphen() -> anyhow::Result<()> {
    // Documented AWS rule: "Bucket names must begin and end with a
    // letter or number." The validator checks the hyphen edge case,
    // not only dots.
    assert_s3_rejects(s3_cfg("-mybucket"), &["begin"])
}

#[test]
fn validate_bucket_rejects_trailing_hyphen() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg("mybucket-"), &["end"])
}

#[test]
fn validate_bucket_rejects_consecutive_dots() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg("my..bucket"), &["consecutive dots"])
}

#[test]
fn validate_bucket_rejects_ipv4_literal() -> anyhow::Result<()> {
    assert_s3_rejects(s3_cfg("192.168.1.1"), &["IPv4"])
}

#[test]
fn validate_region_rejects_empty_string() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.region = String::new();
    assert_s3_rejects(s3, &["cache.origin.region", "must not be empty"])
}

#[test]
fn validate_region_rejects_whitespace_only() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.region = "   ".to_string();
    assert_s3_rejects(s3, &["cache.origin.region", "must not be empty"])
}

#[test]
fn validate_endpoint_rejects_empty_string() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.endpoint_url = Some(String::new());
    assert_s3_rejects(s3, &["cache.origin.endpoint_url", "omit the key instead"])
}

#[test]
fn validate_endpoint_rejects_non_http_scheme() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.endpoint_url = Some("ftp://endpoint.example/".to_string());
    assert_s3_rejects(s3, &["cache.origin.endpoint_url"])
}

#[test]
fn validate_prefix_rejects_leading_slash() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.prefix = Some("/foo/".to_string());
    assert_s3_rejects(s3, &["cache.origin.prefix", "must not start with `/`"])
}

#[test]
fn validate_prefix_rejects_dotdot() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.prefix = Some("foo/../bar/".to_string());
    assert_s3_rejects(s3, &["cache.origin.prefix", "must not contain `..`"])
}

#[test]
fn validate_prefix_rejects_backslash() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.prefix = Some("foo\\bar/".to_string());
    assert_s3_rejects(s3, &["cache.origin.prefix", "must not contain `\\`"])
}

#[test]
fn validate_prefix_rejects_control_chars() -> anyhow::Result<()> {
    // \n / \t / \0 inside the prefix would corrupt the eventual
    // S3 key; reject loud at config load instead of letting the
    // SDK URL-encode them into a "key not found".
    let mut s3 = s3_cfg("decdn-blobs");
    s3.prefix = Some("foo\nbar/".to_string());
    assert_s3_rejects(s3, &["cache.origin.prefix", "control characters"])
}

#[test]
fn validate_prefix_rejects_whitespace() -> anyhow::Result<()> {
    let mut s3 = s3_cfg("decdn-blobs");
    s3.prefix = Some("foo bar/".to_string());
    assert_s3_rejects(s3, &["cache.origin.prefix", "whitespace"])
}

// ----- #437: deny_unknown_fields catches typos on Http variant -----
//
// The first-pass review caught that the OriginConfig enum lacked
// deny_unknown_fields, allowing typos like `decompres = "auto"`
// to silently no-op. These two tests lock the contract.

#[test]
fn http_origin_rejects_unknown_field_typo() -> anyhow::Result<()> {
    let toml = r#"
            kind = "http"
            url = "https://origin.example/"
            decompres = "auto"
        "#;
    let err = toml::from_str::<types::OriginConfig>(toml)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected unknown-field error"))?;
    let msg = format!("{err}");
    anyhow::ensure!(
        msg.contains("unknown field") && msg.contains("decompres"),
        "got: {msg}"
    );
    Ok(())
}

#[test]
fn fs_origin_rejects_unknown_field_typo() -> anyhow::Result<()> {
    let toml = r#"
            kind = "fs"
            paht = "/var/decdn"
        "#;
    let err = toml::from_str::<types::OriginConfig>(toml)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected unknown-field error"))?;
    let msg = format!("{err}");
    anyhow::ensure!(
        msg.contains("unknown field") && msg.contains("paht"),
        "got: {msg}"
    );
    Ok(())
}

/// Helper: assert that a TOML deserialization fails with a
/// message containing every required fragment. Used by the
/// missing-required-field and unknown-tag-value tests.
fn assert_origin_toml_rejects(toml: &str, fragments: &[&str]) -> anyhow::Result<()> {
    let err = toml::from_str::<types::OriginConfig>(toml)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection for: {toml}"))?;
    let msg = format!("{err}");
    for fragment in fragments {
        anyhow::ensure!(
            msg.contains(fragment),
            "error did not contain `{fragment}`: {msg}"
        );
    }
    Ok(())
}

// serde-tagged-enum invariant: the `kind` field is required and
// names which variant is being deserialized. Without it,
// serde can't disambiguate. Operators who copy-paste the inner
// table without the `kind = "..."` line need a clear error.
#[test]
fn origin_rejects_missing_kind_tag() -> anyhow::Result<()> {
    assert_origin_toml_rejects(
        r#"url = "https://origin.example/""#,
        &["missing field", "kind"],
    )
}

// An unknown `kind` value (e.g. operator typo `httpx` or a
// forward-looking `gcs` someone speculatively wrote) must
// surface a clear error instead of being silently dropped.
#[test]
fn origin_rejects_unknown_kind_value() -> anyhow::Result<()> {
    assert_origin_toml_rejects(
        r#"
                kind = "httpx"
                url = "https://origin.example/"
            "#,
        &["unknown variant", "httpx"],
    )
}

// HTTP variant requires a `url` field. serde reports a
// missing-field error for the inner struct payload of the
// tagged enum.
#[test]
fn http_origin_rejects_missing_url_field() -> anyhow::Result<()> {
    assert_origin_toml_rejects(
        r#"
                kind = "http"
                decompress = "auto"
            "#,
        &["missing field", "url"],
    )
}

// FS variant requires a `path` field.
#[test]
fn fs_origin_rejects_missing_path_field() -> anyhow::Result<()> {
    assert_origin_toml_rejects(r#"kind = "fs""#, &["missing field", "path"])
}

// S3 variant requires `bucket` and `region`. Two siblings —
// missing each in turn — so future tag-renames or shape changes
// can't silently drop either requirement.
#[test]
fn s3_origin_rejects_missing_bucket_field() -> anyhow::Result<()> {
    assert_origin_toml_rejects(
        r#"
                kind = "s3"
                region = "us-east-1"
            "#,
        &["missing field", "bucket"],
    )
}

#[test]
fn s3_origin_rejects_missing_region_field() -> anyhow::Result<()> {
    assert_origin_toml_rejects(
        r#"
                kind = "s3"
                bucket = "decdn-blobs"
            "#,
        &["missing field", "region"],
    )
}

// ----- #437: S3 credentials end-to-end resolution -----

// Static credentials round-trip through `resolve_cache` into
// `ResolvedS3Credentials::Static` with the secret values
// preserved (as `SecretString`s, exposing only via `expose()`).
// Also exercises the validator path that converts wire-form
// `S3Credentials::Static` into resolved form.
#[test]
fn resolve_cache_origin_s3_static_credentials_round_trip() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = cache_with_s3(types::S3OriginConfig {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: None,
        prefix: None,
        credentials: Some(types::S3Credentials::Static {
            access_key_id: secret::SecretString::new("AKIA-test-id"),
            secret_access_key: secret::SecretString::new("test-secret-value"),
            session_token: Some(secret::SecretString::new("STS-token")),
        }),
        decompress: None,
    });
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    let creds = match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => s3
            .credentials
            .ok_or_else(|| anyhow::anyhow!("credentials missing"))?,
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    };
    match creds {
        crate::config::ResolvedS3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            anyhow::ensure!(access_key_id.expose() == "AKIA-test-id");
            anyhow::ensure!(secret_access_key.expose() == "test-secret-value");
            anyhow::ensure!(
                session_token.as_ref().map(secret::SecretString::expose) == Some("STS-token")
            );
        }
        crate::config::ResolvedS3Credentials::DefaultChain { .. } => {
            anyhow::bail!("expected Static credentials, got DefaultChain")
        }
    }
    Ok(())
}

// DefaultChain with no profile — the most common shape for AWS
// operators using IAM roles or AWS_PROFILE. Resolves to
// `ResolvedS3Credentials::DefaultChain { profile: None }`.
#[test]
fn resolve_cache_origin_s3_default_chain_no_profile() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = cache_with_s3(types::S3OriginConfig {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: None,
        prefix: None,
        credentials: Some(types::S3Credentials::DefaultChain { profile: None }),
        decompress: None,
    });
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    let creds = match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => s3
            .credentials
            .ok_or_else(|| anyhow::anyhow!("credentials missing"))?,
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    };
    match creds {
        crate::config::ResolvedS3Credentials::DefaultChain { profile } => {
            anyhow::ensure!(profile.is_none(), "profile should be None");
        }
        crate::config::ResolvedS3Credentials::Static { .. } => {
            anyhow::bail!("expected DefaultChain, got Static")
        }
    }
    Ok(())
}

// `DefaultChain { profile = "" }` (e.g. from a `${PROFILE}` env-var
// that resolved to empty, or a literal empty string in TOML) is
// collapsed to `profile = None` at the resolver. Without this
// normalization the runtime would call `loader.profile_name("")`
// and the SDK would surface a confusing "profile '' not found" at
// first credential need. Pinned so a future refactor of
// `resolve_s3_origin` can't drop the filter.
#[test]
fn resolve_cache_origin_s3_default_chain_empty_profile_collapses_to_none() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = cache_with_s3(types::S3OriginConfig {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: None,
        prefix: None,
        credentials: Some(types::S3Credentials::DefaultChain {
            profile: Some(String::new()),
        }),
        decompress: None,
    });
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    let creds = match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => s3
            .credentials
            .ok_or_else(|| anyhow::anyhow!("credentials missing"))?,
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    };
    match creds {
        crate::config::ResolvedS3Credentials::DefaultChain { profile } => {
            anyhow::ensure!(profile.is_none(), "empty profile should collapse to None");
        }
        crate::config::ResolvedS3Credentials::Static { .. } => {
            anyhow::bail!("expected DefaultChain, got Static")
        }
    }
    Ok(())
}

// Absent `[cache.origin.credentials]` => resolved credentials
// are `None` (the runtime then falls back to the AWS default
// credential chain). Pin this default so a future refactor of
// `resolve_s3_origin` can't accidentally synthesize a
// `DefaultChain` placeholder where one wasn't requested.
#[test]
fn resolve_cache_origin_s3_no_credentials_resolves_to_none() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = cache_with_s3(s3_cfg("decdn-blobs"));
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    match resolved.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(
                s3.credentials.is_none(),
                "absent credentials must resolve to None, not synthesized DefaultChain"
            );
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

// ----- #437: expand_origin walks every URL/path/secret field -----

#[test]
fn expand_env_substitutes_s3_bucket_and_region() -> anyhow::Result<()> {
    // Operators routinely template region by env var across
    // multi-region deployments.
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                bucket: "blobs-${HOME}".to_string(),
                region: "${HOME}-east-1".to_string(),
                endpoint_url: None,
                path_style: None,
                prefix: None,
                credentials: None,
                decompress: None,
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let s3 = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
        Some(types::OriginConfig::S3(s3)) => s3,
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    };
    anyhow::ensure!(s3.bucket == format!("blobs-{home}"));
    anyhow::ensure!(s3.region == format!("{home}-east-1"));
    Ok(())
}

#[test]
fn expand_env_substitutes_s3_endpoint_and_prefix() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                bucket: "decdn-blobs".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: Some("https://${HOME}.example/".to_string()),
                path_style: None,
                prefix: Some("blobs-${HOME}/".to_string()),
                credentials: None,
                decompress: None,
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let s3 = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
        Some(types::OriginConfig::S3(s3)) => s3,
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    };
    anyhow::ensure!(s3.endpoint_url.as_deref() == Some(&format!("https://{home}.example/")[..]));
    anyhow::ensure!(s3.prefix.as_deref() == Some(&format!("blobs-{home}/")[..]));
    Ok(())
}

#[test]
fn expand_env_substitutes_s3_static_credentials() -> anyhow::Result<()> {
    // The whole point of supporting `${VAR}` in TOML is to keep
    // secrets out of the file: operators write
    // `access_key_id = "${AWS_ACCESS_KEY_ID}"` and the env var
    // supplies the value. This test locks the wiring so a future
    // refactor of expand_origin can't silently regress it.
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                bucket: "decdn-blobs".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                path_style: None,
                prefix: None,
                credentials: Some(types::S3Credentials::Static {
                    access_key_id: secret::SecretString::new("${HOME}-access"),
                    secret_access_key: secret::SecretString::new("${HOME}-secret"),
                    session_token: Some(secret::SecretString::new("${HOME}-token")),
                }),
                decompress: None,
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let creds = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
        Some(types::OriginConfig::S3(s3)) => s3.credentials.as_ref(),
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    .ok_or_else(|| anyhow::anyhow!("credentials missing"))?;
    match creds {
        types::S3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            anyhow::ensure!(access_key_id.expose() == format!("{home}-access"));
            anyhow::ensure!(secret_access_key.expose() == format!("{home}-secret"));
            anyhow::ensure!(
                session_token
                    .as_ref()
                    .map(secret::SecretString::expose)
                    .map(str::to_string)
                    == Some(format!("{home}-token"))
            );
        }
        types::S3Credentials::DefaultChain { .. } => {
            anyhow::bail!("expected Static, got DefaultChain")
        }
    }
    Ok(())
}

#[test]
fn expand_env_substitutes_s3_default_chain_profile() -> anyhow::Result<()> {
    let home = home_str()?;
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                bucket: "decdn-blobs".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                path_style: None,
                prefix: None,
                credentials: Some(types::S3Credentials::DefaultChain {
                    profile: Some("${HOME}-prof".to_string()),
                }),
                decompress: None,
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    expand_env(&mut cfg)?;
    let profile = match cfg.cache.as_ref().and_then(|c| c.origin.as_ref()) {
        Some(types::OriginConfig::S3(s3)) => match s3.credentials.as_ref() {
            Some(types::S3Credentials::DefaultChain { profile }) => profile.clone(),
            other => anyhow::bail!("expected DefaultChain, got: {other:?}"),
        },
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    };
    anyhow::ensure!(profile == Some(format!("{home}-prof")));
    Ok(())
}

#[test]
fn expand_secret_error_on_missing_env_var_does_not_leak_partial_value() -> anyhow::Result<()> {
    // The safety claim is: an undefined `${VAR}` in a secret
    // field surfaces only the field's dotted-path context and
    // the env-var name — never any cleartext that might be
    // adjacent to the marker in the TOML. This test pins that
    // contract for the credential path so a refactor of
    // expand_value cannot silently regress it.
    let missing = "DECDN_UNSET_SECRET_VAR_XYZ";
    anyhow::ensure!(
        std::env::var_os(missing).is_none(),
        "test prereq: unset env var"
    );
    let mut cfg = FileConfig {
        cache: Some(types::CacheConfig {
            origin: Some(types::OriginConfig::S3(types::S3OriginConfig {
                bucket: "decdn-blobs".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                path_style: None,
                prefix: None,
                credentials: Some(types::S3Credentials::Static {
                    access_key_id: secret::SecretString::new(format!(
                        "AKIA-prefix-${{{missing}}}-suffix"
                    )),
                    secret_access_key: secret::SecretString::new("does-not-matter"),
                    session_token: None,
                }),
                decompress: None,
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    let err = expand_env(&mut cfg)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected env-var error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("cache.origin.credentials.access_key_id"),
        "error must name the field: {msg}"
    );
    anyhow::ensure!(
        !msg.contains("AKIA") && !msg.contains("suffix"),
        "error must not echo any cleartext from the secret value: {msg}"
    );
    Ok(())
}

// ----- flat decompress field rejection -----

#[test]
fn http_origin_flat_top_level_decompress_field_rejected() -> anyhow::Result<()> {
    // A flat `decompress` field directly under `[cache]` is rejected
    // by `deny_unknown_fields`. Sibling of the flat
    // `origin_url`/`origin_path` rejection tests below.
    let toml_body = format!(
        "{}\n\n[cache]\ndecompress = \"strict\"\n",
        complete_toml_body()
    );
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, &toml_body)?;
    let args = run_args_with_data_dir(dir.path());
    let err = resolve_config(Some(&path), &args)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected unknown-field error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("decompress") && msg.contains("unknown field"),
        "error must call out the flat `decompress` key: {msg}"
    );
    Ok(())
}

// ----- #437: regression lock — `decdn config validate` accepts S3 today -----

#[test]
fn resolve_config_accepts_s3_origin_today() -> anyhow::Result<()> {
    // `decdn config validate` (which goes through `resolve_config`)
    // must succeed for a valid S3 TOML — operator-side validation
    // is the schema + resolver layer, not the runtime backend.
    // Pinned so a future refactor that moved the S3 backend's
    // construction-time validation upstream into `resolve_origin`
    // can't silently start rejecting configs that `build_cache`
    // would otherwise accept.
    let toml_body = format!(
        "{}\n\n[cache.origin]\nkind = \"s3\"\n\
         bucket = \"decdn-blobs\"\nregion = \"us-east-1\"\n",
        complete_toml_body()
    );
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, &toml_body)?;
    let args = run_args_with_data_dir(dir.path());
    let (resolved, _notices) = resolve_config(Some(&path), &args)?;
    match resolved.cache.origins.into_iter().next() {
        Some(ResolvedOrigin::S3(s3)) => {
            anyhow::ensure!(s3.bucket == "decdn-blobs");
        }
        other => anyhow::bail!("expected S3 origin, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn resolve_cache_pinned_hashes_propagate_through_file() -> anyhow::Result<()> {
    let h = make_hex_hash(5);
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        pinned_hashes: Some(vec![h.clone()]),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.pinned_hashes.len() == 1,
        "expected one pinned hash, got {}",
        resolved.pinned_hashes.len()
    );
    Ok(())
}

#[test]
fn resolve_cache_invalid_pinned_hash_fails_resolution() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        pinned_hashes: Some(vec!["not-a-hash".to_string()]),
        ..types::CacheConfig::default()
    };
    let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("invalid cache.pinned_hashes"),
        "error should be contextualized: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_origin_retry_defaults_when_absent() -> anyhow::Result<()> {
    // Absent `cache.origin_retry` section => defaults from
    // RetryPolicy::default(). Pin the contract here so a future
    // default change has to update this test deliberately.
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    let p = resolved.origin_retry;
    anyhow::ensure!(p.max_retries == 3, "default max_retries");
    anyhow::ensure!(p.initial_backoff_ms == 100, "default initial_backoff_ms");
    anyhow::ensure!(p.max_backoff_ms == 10_000, "default max_backoff_ms");
    anyhow::ensure!(
        (p.jitter_ratio - 0.1).abs() < f64::EPSILON,
        "default jitter_ratio"
    );
    Ok(())
}

#[test]
fn resolve_origin_retry_parses_full_section() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin_retry: Some(decdn_config_types::RetryPolicy {
            max_retries: 7,
            initial_backoff_ms: 50,
            max_backoff_ms: 2_000,
            jitter_ratio: 0.25,
            buffered_max_bytes: 8 << 20, // 8 MiB
        }),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    let p = resolved.origin_retry;
    anyhow::ensure!(p.max_retries == 7);
    anyhow::ensure!(p.initial_backoff_ms == 50);
    anyhow::ensure!(p.max_backoff_ms == 2_000);
    anyhow::ensure!((p.jitter_ratio - 0.25).abs() < f64::EPSILON);
    anyhow::ensure!(p.buffered_max_bytes == 8 << 20);
    Ok(())
}

#[test]
fn resolve_origin_retry_partial_section_inherits_defaults() -> anyhow::Result<()> {
    // `#[serde(default)]` on RetryPolicy fills missing fields from
    // Default. Pinning the contract here so a future struct-level
    // attribute change doesn't silently break partial TOML.
    let toml = "[cache.origin_retry]\nmax_retries = 5\n";
    let file: crate::config::FileConfig = ::toml::from_str(toml)?;
    let p = file
        .cache
        .as_ref()
        .and_then(|c| c.origin_retry.as_ref())
        .ok_or_else(|| anyhow::anyhow!("origin_retry missing"))?;
    anyhow::ensure!(p.max_retries == 5);
    anyhow::ensure!(p.initial_backoff_ms == 100, "default carried through");
    anyhow::ensure!(p.max_backoff_ms == 10_000, "default carried through");
    Ok(())
}

#[test]
fn resolve_origin_retry_max_retries_zero_is_valid() -> anyhow::Result<()> {
    // `0` opts out and is the documented disable knob; resolution
    // must not reject it.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin_retry: Some(decdn_config_types::RetryPolicy {
            max_retries: 0,
            ..decdn_config_types::RetryPolicy::default()
        }),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(resolved.origin_retry.max_retries == 0);
    Ok(())
}

#[test]
fn resolve_origin_retry_rejects_initial_above_max() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin_retry: Some(decdn_config_types::RetryPolicy {
            initial_backoff_ms: 2_000,
            max_backoff_ms: 1_000,
            ..decdn_config_types::RetryPolicy::default()
        }),
        ..types::CacheConfig::default()
    };
    let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("schedule never grows"),
        "error message should explain why initial>max is rejected, got: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_origin_retry_rejects_jitter_out_of_range() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    for bad in [-0.1, 1.5, f64::NAN, f64::INFINITY] {
        let file = types::CacheConfig {
            origin_retry: Some(decdn_config_types::RetryPolicy {
                jitter_ratio: bad,
                ..decdn_config_types::RetryPolicy::default()
            }),
            ..types::CacheConfig::default()
        };
        let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection for jitter={bad}"))?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            msg.contains("jitter_ratio"),
            "error message should reference jitter_ratio, got: {msg}"
        );
    }
    Ok(())
}

#[test]
fn resolve_origin_retry_rejects_buffered_max_bytes_above_ceiling() -> anyhow::Result<()> {
    // #519 hard ceiling: buffered_max_bytes > 64 MiB is almost
    // certainly an operator typo. The streaming abort+restart
    // path covers any blob size without raising this knob.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin_retry: Some(decdn_config_types::RetryPolicy {
            buffered_max_bytes: MAX_BUFFERED_MAX_BYTES + 1,
            ..decdn_config_types::RetryPolicy::default()
        }),
        ..types::CacheConfig::default()
    };
    let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection for too-large buffered_max_bytes"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("buffered_max_bytes"),
        "error message should reference buffered_max_bytes, got: {msg}"
    );
    anyhow::ensure!(
        msg.contains("hard ceiling"),
        "error message should explain the ceiling rationale, got: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_origin_retry_accepts_buffered_max_bytes_at_ceiling() -> anyhow::Result<()> {
    // Boundary: exactly the ceiling is allowed; only > ceiling is
    // rejected. Pins the inclusive-bound semantics so a future
    // edit can't accidentally flip the inequality.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin_retry: Some(decdn_config_types::RetryPolicy {
            buffered_max_bytes: MAX_BUFFERED_MAX_BYTES,
            ..decdn_config_types::RetryPolicy::default()
        }),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(resolved.origin_retry.buffered_max_bytes == MAX_BUFFERED_MAX_BYTES);
    Ok(())
}

#[test]
fn resolve_origin_retry_accepts_buffered_max_bytes_zero() -> anyhow::Result<()> {
    // `0` disables the buffer path entirely (operator opt-out;
    // documented in `RetryPolicy::buffered_max_bytes`). Must
    // resolve cleanly.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        origin_retry: Some(decdn_config_types::RetryPolicy {
            buffered_max_bytes: 0,
            ..decdn_config_types::RetryPolicy::default()
        }),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(resolved.origin_retry.buffered_max_bytes == 0);
    Ok(())
}

#[test]
fn resolve_circuit_breaker_defaults_when_absent() -> anyhow::Result<()> {
    // Absent `cache.circuit_breaker` => defaults from
    // CircuitBreakerPolicy::default() (#963). Pin the contract so a
    // default change is a deliberate, test-visible edit.
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    let p = resolved.circuit_breaker;
    anyhow::ensure!(p.enabled, "breaker on by default");
    anyhow::ensure!(p.failure_threshold == 5, "default failure_threshold");
    anyhow::ensure!(p.cooldown_ms == 30_000, "default cooldown_ms");
    anyhow::ensure!(p.half_open_max_calls == 1, "default half_open_max_calls");
    Ok(())
}

#[test]
fn resolve_circuit_breaker_parses_full_section() -> anyhow::Result<()> {
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        circuit_breaker: Some(decdn_config_types::CircuitBreakerPolicy {
            enabled: true,
            failure_threshold: 10,
            cooldown_ms: 60_000,
            half_open_max_calls: 3,
        }),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    let p = resolved.circuit_breaker;
    anyhow::ensure!(p.failure_threshold == 10);
    anyhow::ensure!(p.cooldown_ms == 60_000);
    anyhow::ensure!(p.half_open_max_calls == 3);
    Ok(())
}

#[test]
fn resolve_circuit_breaker_partial_section_inherits_defaults() -> anyhow::Result<()> {
    // `#[serde(default)]` on CircuitBreakerPolicy fills missing
    // fields. A partial `[cache.circuit_breaker]` with only
    // cooldown_ms set must carry the other defaults through.
    let toml = "[cache.circuit_breaker]\ncooldown_ms = 12345\n";
    let file: crate::config::FileConfig = ::toml::from_str(toml)?;
    let p = file
        .cache
        .as_ref()
        .and_then(|c| c.circuit_breaker.as_ref())
        .ok_or_else(|| anyhow::anyhow!("circuit_breaker missing"))?;
    anyhow::ensure!(p.cooldown_ms == 12_345);
    anyhow::ensure!(p.enabled, "default carried through");
    anyhow::ensure!(p.failure_threshold == 5, "default carried through");
    anyhow::ensure!(p.half_open_max_calls == 1, "default carried through");
    Ok(())
}

#[test]
fn resolve_circuit_breaker_disabled_skips_half_open_invariant() -> anyhow::Result<()> {
    // A disabled breaker (enabled = false) never reaches HALF-OPEN,
    // so half_open_max_calls = 0 must resolve cleanly — an operator
    // opting out shouldn't have to supply a half-open value.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        circuit_breaker: Some(decdn_config_types::CircuitBreakerPolicy::disabled()),
        ..types::CacheConfig::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    anyhow::ensure!(!resolved.circuit_breaker.enabled);
    anyhow::ensure!(!resolved.circuit_breaker.is_active());
    Ok(())
}

#[test]
fn resolve_circuit_breaker_rejects_zero_half_open_when_active() -> anyhow::Result<()> {
    // An ACTIVE breaker with half_open_max_calls = 0 could never
    // probe for recovery and would stay open forever — reject it at
    // config-load time with a contextualized error.
    let cli = cache_cli(None, None);
    let file = types::CacheConfig {
        circuit_breaker: Some(decdn_config_types::CircuitBreakerPolicy {
            enabled: true,
            failure_threshold: 5,
            cooldown_ms: 30_000,
            half_open_max_calls: 0,
        }),
        ..types::CacheConfig::default()
    };
    let err = resolve_cache(&cli, Some(&file), Path::new("/tmp"))
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("invalid cache.circuit_breaker"),
        "error should be contextualized: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_cache_defaults_satisfy_invariant() -> anyhow::Result<()> {
    // Regression guard: the resolved defaults must satisfy the load-time
    // `max_blob_size_mb <= cache_size_mb` invariant. The default blob ceiling
    // (`DEFAULT_MAX_BLOB_SIZE_MB`, clamped to the cache size) sits below the
    // default cache budget, so this holds.
    let cli = cache_cli(None, None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    anyhow::ensure!(
        resolved.max_blob_size_mb <= resolved.cache_size_mb,
        "defaults violate invariant: max_blob={} cache_size={}",
        resolved.max_blob_size_mb,
        resolved.cache_size_mb
    );
    Ok(())
}

fn obs_cli(
    metrics_port: Option<u16>,
    admin_port: Option<u16>,
) -> crate::cli::run::ObservabilityArgs {
    crate::cli::run::ObservabilityArgs {
        log_level: None,
        log_format: None,
        metrics_port,
        metrics_bind: None,
        admin_port,
        otlp_endpoint: None,
    }
}

fn net(port: u16) -> ResolvedNetwork {
    ResolvedNetwork {
        bind_port: port,
        relay_urls: Vec::new(),
        discovery: ResolvedDiscovery::default(),
    }
}

fn obs(port: u16) -> ResolvedObservability {
    ResolvedObservability {
        log_level: crate::cli::common::LogLevel::default(),
        log_format: crate::cli::common::LogFormat::default(),
        metrics_port: port,
        metrics_bind: DEFAULT_METRICS_BIND,
        admin_port: None,
        otlp_endpoint: None,
    }
}

fn obs_with_admin(metrics: u16, admin: u16) -> ResolvedObservability {
    ResolvedObservability {
        log_level: crate::cli::common::LogLevel::default(),
        log_format: crate::cli::common::LogFormat::default(),
        metrics_port: metrics,
        metrics_bind: DEFAULT_METRICS_BIND,
        admin_port: Some(admin),
        otlp_endpoint: None,
    }
}

#[test]
fn resolve_observability_defaults_admin_port_to_9191() -> anyhow::Result<()> {
    let obs = resolve_observability(&obs_cli(None, None), None)?;
    anyhow::ensure!(
        obs.admin_port == Some(DEFAULT_ADMIN_PORT),
        "got: {:?}",
        obs.admin_port
    );
    anyhow::ensure!(obs.metrics_port == DEFAULT_METRICS_PORT);
    Ok(())
}

#[test]
fn validate_port_layout_rejects_bind_equal_metrics() {
    let err = validate_port_layout(&net(9090), &obs(9090))
        .expect_err("equal bind and metrics ports should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("network.bind_port") && msg.contains("metrics_port"),
        "error should name both fields: {msg}"
    );
}

#[test]
fn validate_port_layout_accepts_distinct_ports() -> anyhow::Result<()> {
    validate_port_layout(&net(4433), &obs(9090))?;
    Ok(())
}

#[test]
fn resolve_observability_admin_port_zero_disables() -> anyhow::Result<()> {
    let obs = resolve_observability(&obs_cli(None, Some(0)), None)?;
    anyhow::ensure!(obs.admin_port.is_none(), "got: {:?}", obs.admin_port);
    Ok(())
}

#[test]
fn resolve_observability_file_admin_port_zero_disables() -> anyhow::Result<()> {
    // Closes #300. `Option<u16>` distinguishes absent (None) from
    // explicit zero (Some(0)) under serde+toml, so the file leg of the
    // merge can carry the operator's "disable" intent through to the
    // resolved config without ambiguity.
    let file = types::ObservabilityConfig {
        admin_port: Some(0),
        ..Default::default()
    };
    let obs = resolve_observability(&obs_cli(None, None), Some(&file))?;
    anyhow::ensure!(obs.admin_port.is_none(), "got: {:?}", obs.admin_port);
    Ok(())
}

#[test]
fn file_admin_port_zero_deserializes_as_some_zero() -> anyhow::Result<()> {
    // Locks the deserializer invariant #300 was filed against: an
    // explicit `admin_port = 0` must round-trip to Some(0), distinct
    // from a missing key which round-trips to None.
    let absent: types::ObservabilityConfig = toml::from_str("")?;
    anyhow::ensure!(absent.admin_port.is_none(), "got: {:?}", absent.admin_port);
    let zero: types::ObservabilityConfig = toml::from_str("admin_port = 0")?;
    anyhow::ensure!(zero.admin_port == Some(0), "got: {:?}", zero.admin_port);
    Ok(())
}

#[test]
fn validate_port_layout_allows_well_known_port() -> anyhow::Result<()> {
    // Well-known range only records a notice, never hard-fails —
    // operators have legitimate reasons to bind there (QUIC on 443,
    // privileged setup scripts, CAP_NET_BIND_SERVICE).
    validate_port_layout(&net(443), &obs(9090))?;
    Ok(())
}

#[test]
fn validate_port_layout_rejects_admin_eq_metrics() {
    let err = validate_port_layout(&net(4433), &obs_with_admin(9090, 9090))
        .expect_err("equal admin and metrics ports should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("admin_port") && msg.contains("metrics_port"),
        "error should name both fields: {msg}"
    );
}

#[test]
fn validate_port_layout_rejects_bind_equal_admin() {
    let err = validate_port_layout(&net(9191), &obs_with_admin(9090, 9191))
        .expect_err("equal bind and admin ports should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("bind_port") && msg.contains("admin_port"),
        "error should name both fields: {msg}"
    );
}

#[test]
fn validate_port_layout_skips_admin_when_disabled() -> anyhow::Result<()> {
    // admin_port = None means admin surface is disabled; collision
    // rules that involve admin must be skipped even when other
    // numbers happen to coincide.
    validate_port_layout(&net(4433), &obs(9090))?;
    Ok(())
}

#[test]
fn resolve_observability_cli_admin_port_overrides_file() -> anyhow::Result<()> {
    let file = types::ObservabilityConfig {
        admin_port: Some(1111),
        ..Default::default()
    };
    let obs = resolve_observability(&obs_cli(None, Some(2222)), Some(&file))?;
    anyhow::ensure!(obs.admin_port == Some(2222), "got: {:?}", obs.admin_port);
    Ok(())
}

#[test]
fn validate_port_layout_allows_both_zero() -> anyhow::Result<()> {
    // Port 0 requests an OS-assigned ephemeral port, so two zeros
    // resolve to two distinct ports at bind time and cannot collide.
    // The equality check must not fire here.
    validate_port_layout(&net(0), &obs(0))?;
    Ok(())
}

#[test]
fn validate_port_layout_allows_zero_with_nonzero() -> anyhow::Result<()> {
    // One ephemeral + one fixed is unambiguously collision-free.
    validate_port_layout(&net(0), &obs(9090))?;
    validate_port_layout(&net(4433), &obs(0))?;
    Ok(())
}

#[test]
fn validate_port_layout_aggregates_three_way_collision() {
    // All three ports set to the same number: each pair surfaces its
    // own problem (bind/metrics, bind/admin, admin/metrics) instead of
    // the first collision masking the other two. Locks the comment in
    // `validate_port_layout_into` that promises accumulation across
    // pairs, and verifies each pair carries a distinct field label so
    // the bag can hold all three without one overwriting another.
    let mut bag = ConfigDiagnostics::new();
    validate_port_layout_into(&net(7000), &obs_with_admin(7000, 7000), &mut bag);
    let err = bag
        .into_result()
        .expect_err("three colliding ports must report problems");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("3 problem(s):"),
        "expected three pair problems: {msg}"
    );
    for needle in [
        "network.bind_port vs observability.metrics_port",
        "network.bind_port vs observability.admin_port",
        "observability.admin_port vs observability.metrics_port",
    ] {
        assert!(msg.contains(needle), "missing pair label {needle:?}: {msg}");
    }
}

#[test]
fn resolve_observability_metrics_bind_defaults_to_localhost() -> anyhow::Result<()> {
    let obs = resolve_observability(&obs_cli(None, None), None)?;
    assert_eq!(
        obs.metrics_bind,
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    );
    Ok(())
}

#[test]
fn resolve_observability_metrics_bind_from_cli() -> anyhow::Result<()> {
    let mut cli = obs_cli(None, None);
    cli.metrics_bind = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    let obs = resolve_observability(&cli, None)?;
    assert_eq!(
        obs.metrics_bind,
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    );
    Ok(())
}

#[test]
fn resolve_observability_metrics_bind_from_file() -> anyhow::Result<()> {
    let file = types::ObservabilityConfig {
        metrics_bind: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
        ..Default::default()
    };
    let obs = resolve_observability(&obs_cli(None, None), Some(&file))?;
    assert_eq!(
        obs.metrics_bind,
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    );
    Ok(())
}

#[test]
fn resolve_observability_metrics_bind_cli_overrides_file() -> anyhow::Result<()> {
    let mut cli = obs_cli(None, None);
    cli.metrics_bind = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    let file = types::ObservabilityConfig {
        metrics_bind: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
        ..Default::default()
    };
    let obs = resolve_observability(&cli, Some(&file))?;
    assert_eq!(
        obs.metrics_bind,
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    );
    Ok(())
}

#[test]
fn resolve_observability_rejects_otlp_endpoint_bad_scheme() {
    let mut cli = obs_cli(None, None);
    cli.otlp_endpoint = Some("grpc://collector:4317".to_string());
    let err = resolve_observability(&cli, None).expect_err("non-http scheme should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("http://"),
        "error should mention the valid scheme: {msg}"
    );
}

#[test]
fn resolve_observability_rejects_https_otlp_endpoint() {
    let mut cli = obs_cli(None, None);
    cli.otlp_endpoint = Some("HTTPS://collector:4317".to_string());
    let err = resolve_observability(&cli, None)
        .expect_err("https endpoint should be rejected: the exporter has no TLS");
    let msg = err.to_string();
    assert!(
        msg.contains("no TLS"),
        "error should explain the TLS gap: {msg}"
    );
}

#[test]
fn resolve_observability_accepts_valid_otlp_endpoint() -> anyhow::Result<()> {
    let mut cli = obs_cli(None, None);
    cli.otlp_endpoint = Some("http://collector:4317".to_string());
    let obs = resolve_observability(&cli, None)?;
    assert_eq!(obs.otlp_endpoint.as_deref(), Some("http://collector:4317"));
    Ok(())
}

#[test]
fn otlp_endpoint_accepts_ipv6_default_port_and_trailing_slash() -> anyhow::Result<()> {
    for ep in [
        "http://[::1]:4317",
        "http://collector:80",
        "http://10.0.0.5:4317/",
        "HTTP://collector:4317",
    ] {
        validate_otlp_endpoint(ep).with_context(|| format!("{ep} should be accepted"))?;
    }
    Ok(())
}

#[test]
fn otlp_endpoint_rejects_unusable_shapes() {
    for (ep, expect) in [
        ("http://collector", "port"),
        ("http://[::1]", "port"),
        ("http://collector:4318/v1/traces", "no path"),
        ("http://collector:4317?x=1", "no path"),
        ("http://collector:4317#frag", "no path"),
        ("http://user:pw@collector:4317", "userinfo"),
        ("http://@collector:4317", "userinfo"),
        ("http://:@collector:4317", "userinfo"),
        ("http://coll ector:4317", "not a valid URL"),
        ("not a url", "must start with http://"),
        ("http:collector:4317", "must start with http://"),
        (" http://collector:4317", "whitespace"),
        ("http://coll\tector:4317", "whitespace"),
    ] {
        let err = validate_otlp_endpoint(ep).expect_err(ep);
        assert!(
            err.to_string().contains(expect),
            "{ep}: expected {expect:?} in {err}"
        );
    }
}

/// The endpoint can carry credentials, so no rejection may echo it.
#[test]
fn otlp_endpoint_errors_never_echo_the_value() {
    let secret = "s3cr3t-token";
    for ep in [
        format!("http://collector:4317/?api_key={secret}"),
        format!("http://{secret}:pw@collector:4317"),
        format!("https://collector:4317/{secret}"),
        format!("http://collector/{secret}"),
        format!("{secret}:pw@collector:4317"),
        format!("{secret}.example:4317"),
    ] {
        let mut cli = obs_cli(None, None);
        cli.otlp_endpoint = Some(ep.clone());
        let msg = format!(
            "{:#}",
            resolve_observability(&cli, None).expect_err("endpoint should be rejected")
        );
        assert!(!msg.contains(secret), "{ep} leaked into: {msg}");
    }
}

// Closes #268. The three-layer merge is CLI/env > TOML file > default;
// `resolve_*_cli_overrides_file*` tests cover the "Option::Some on
// RunArgs beats file" leg. The remaining leg — that clap populates
// RunArgs from `DECDN_*` env vars so those Option::Some values are
// there to win — lives in this test.
//
// Done declaratively (via clap's `Command` introspection) rather than
// by setting process env vars: `std::env::set_var` is `unsafe` under
// edition 2024 and the workspace lints forbid `unsafe_code`. A dev-
// dependency like `temp-env` would work but costs more than the
// regression risk we're pinning here — a dropped `env = "DECDN_*"`
// attribute or a field rename fails this test immediately.
#[test]
fn run_subcommand_args_are_wired_to_decdn_env_vars() {
    use clap::{Args, CommandFactory, Parser};

    // The user CLI has no `run` subcommand (#421 — the
    // daemon binary `decdn-node` owns it). `RunArgs` itself
    // remains in `decdn-common` because `decdn config validate`
    // flattens it for env-var parity with the daemon. Wrap
    // `RunArgs` in a local `Parser` and walk *its* args — this
    // is the same set of env mappings the daemon's
    // `decdn-node run` exposes and that `decdn config validate`
    // honours.
    #[derive(Parser, Debug)]
    struct RunWrap {
        #[command(flatten)]
        run: crate::cli::RunArgs,
    }

    let _ = RunWrap::command(); // surface a parse error if RunArgs is broken
    let run = <crate::cli::RunArgs as Args>::augment_args(clap::Command::new("run"));

    // One line per DECDN_* env var operators may set. Adding a new
    // `#[arg(env = "DECDN_*")]` field without adding it here is a test
    // failure — which is the point. Arg IDs are the Rust field name
    // (underscored), not the `--long` form, because that's what clap
    // stores on the `Arg` struct.
    let expected: &[(&str, &str)] = &[
        ("data_dir", "DECDN_DATA_DIR"),
        ("region", "DECDN_REGION"),
        ("bind_port", "DECDN_BIND_PORT"),
        ("relay_url", "DECDN_RELAY_URL"),
        ("rpc_url", "DECDN_RPC_URL"),
        ("eth_keystore", "DECDN_ETH_KEYSTORE"),
        ("keystore_password_file", "DECDN_KEYSTORE_PASSWORD_FILE"),
        ("payment_pool_address", "DECDN_PAYMENT_POOL_ADDRESS"),
        ("capacity_bond_address", "DECDN_CAPACITY_BOND_ADDRESS"),
        (
            "origin_assignment_address",
            "DECDN_ORIGIN_ASSIGNMENT_ADDRESS",
        ),
        (
            "publisher_registry_address",
            "DECDN_PUBLISHER_REGISTRY_ADDRESS",
        ),
        ("slash_judge_address", "DECDN_SLASH_JUDGE_ADDRESS"),
        (
            "content_blacklist_address",
            "DECDN_CONTENT_BLACKLIST_ADDRESS",
        ),
        ("chain_id", "DECDN_CHAIN_ID"),
        ("cache_dir", "DECDN_CACHE_DIR"),
        ("cache_size_mb", "DECDN_CACHE_SIZE_MB"),
        ("max_blob_size_mb", "DECDN_MAX_BLOB_SIZE_MB"),
        ("max_rate_per_mb", "DECDN_MAX_RATE_PER_MB"),
        ("max_probe_holds", "DECDN_MAX_PROBE_HOLDS"),
        (
            "stake_lane_reserved_holds",
            "DECDN_STAKE_LANE_RESERVED_HOLDS",
        ),
        ("rate_per_mb", "DECDN_RATE_PER_MB"),
        ("log_level", "DECDN_LOG_LEVEL"),
        ("log_format", "DECDN_LOG_FORMAT"),
        ("metrics_port", "DECDN_METRICS_PORT"),
        ("metrics_bind", "DECDN_METRICS_BIND"),
        ("admin_port", "DECDN_ADMIN_PORT"),
        ("otlp_endpoint", "DECDN_OTLP_ENDPOINT"),
    ];

    for (arg_id, env_name) in expected {
        let arg = run
            .get_arguments()
            .find(|a| a.get_id() == arg_id)
            .unwrap_or_else(|| panic!("run subcommand missing arg {arg_id:?}"));
        let env = arg
            .get_env()
            .unwrap_or_else(|| panic!("arg {arg_id:?} has no env mapping (expected {env_name:?})"));
        assert_eq!(
            env.to_str(),
            Some(*env_name),
            "arg {arg_id:?} env mapping drifted"
        );
    }

    // Reverse direction: catch a newly-added `#[arg(env = "DECDN_*")]`
    // that wasn't added to `expected`. Otherwise this test only
    // enforces "don't remove env mappings", not "don't silently add
    // undocumented ones".
    let all_env_args: Vec<String> = run
        .get_arguments()
        .filter(|a| a.get_env().is_some())
        .map(|a| a.get_id().to_string())
        .collect();
    assert_eq!(
        all_env_args.len(),
        expected.len(),
        "env-bearing args drifted; declared: {all_env_args:?}, expected {}",
        expected.len()
    );
}

fn data_dir_with_keystore() -> anyhow::Result<TempDir> {
    let dir = TempDir::new()?;
    std::fs::write(dir.path().join("keystore.json"), "")?;
    Ok(dir)
}

#[test]
fn resolve_blockchain_names_correct_field_for_bad_address() -> anyhow::Result<()> {
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some("0xNOTHEX".to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let dir = data_dir_with_keystore()?;
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on bad staking address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("capacity_bond_address"),
        "error should name capacity_bond_address: {msg}"
    );
    assert!(
        !msg.contains("payment_pool_address"),
        "error must not name the valid field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_names_correct_field_for_bad_payment_address() -> anyhow::Result<()> {
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some("0xNOTHEX".to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let dir = data_dir_with_keystore()?;
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on bad payment address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("payment_pool_address"),
        "error should name payment_pool_address: {msg}"
    );
    assert!(
        !msg.contains("capacity_bond_address"),
        "error must not name the valid field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_origin_directory_unset_resolves_to_none() -> anyhow::Result<()> {
    // The chain-backed origin directory is opt-in: with neither address set
    // both resolve to `None` (the runtime then uses an empty deny-all origin
    // directory) and resolution succeeds.
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let dir = data_dir_with_keystore()?;
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert!(resolved.origin_assignment_address.is_none());
    assert!(resolved.publisher_registry_address.is_none());
    Ok(())
}

#[test]
fn resolve_blockchain_origin_directory_both_set_resolves_to_some() -> anyhow::Result<()> {
    let cli = BlockchainArgs {
        origin_assignment_address: Some(GOOD_ADDR.to_string()),
        publisher_registry_address: Some(GOOD_ADDR.to_string()),
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let dir = data_dir_with_keystore()?;
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert!(resolved.origin_assignment_address.is_some());
    assert!(resolved.publisher_registry_address.is_some());
    Ok(())
}

#[test]
fn resolve_blockchain_lone_origin_assignment_resolves() -> anyhow::Result<()> {
    // The origin directory keys solely on OriginAssignment (a namespace
    // resolves directly via getOrigins), so a lone origin_assignment_address
    // is valid and enables the directory; publisher_registry is independent.
    let cli = BlockchainArgs {
        origin_assignment_address: Some(GOOD_ADDR.to_string()),
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let dir = data_dir_with_keystore()?;
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert!(resolved.origin_assignment_address.is_some());
    assert!(resolved.publisher_registry_address.is_none());
    Ok(())
}

#[test]
fn resolve_blockchain_lone_publisher_registry_resolves() -> anyhow::Result<()> {
    // publisher_registry_address is independent (the publish CLI's `namespace
    // create` target); setting it alone is valid and does not enable the
    // origin directory (origin_assignment unset => empty deny-all directory).
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: Some(GOOD_ADDR.to_string()),
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let dir = data_dir_with_keystore()?;
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert!(resolved.origin_assignment_address.is_none());
    assert!(resolved.publisher_registry_address.is_some());
    Ok(())
}

#[test]
fn resolve_blockchain_origin_directory_ttls_default_when_unset() -> anyhow::Result<()> {
    // Absent origin-directory knobs resolve to the DEFAULT_ORIGIN_DIRECTORY_*
    // consts.
    let dir = data_dir_with_keystore()?;
    let cli = empty_blockchain_args();
    let file = types::BlockchainConfig {
        rpc_url: Some("https://example/rpc".to_string()),
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        origin_directory_positive_ttl_sec: None,
        origin_directory_negative_ttl_sec: None,
        origin_directory_cache_capacity: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(
        resolved.origin_directory_positive_ttl_sec,
        DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC
    );
    assert_eq!(
        resolved.origin_directory_negative_ttl_sec,
        DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC
    );
    assert_eq!(
        resolved.origin_directory_cache_capacity,
        DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY
    );
    Ok(())
}

#[test]
fn resolve_blockchain_origin_directory_ttls_pass_through() -> anyhow::Result<()> {
    // Explicit values survive resolution unchanged.
    let dir = data_dir_with_keystore()?;
    let cli = empty_blockchain_args();
    let file = types::BlockchainConfig {
        rpc_url: Some("https://example/rpc".to_string()),
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        origin_directory_positive_ttl_sec: Some(600),
        origin_directory_negative_ttl_sec: Some(5),
        origin_directory_cache_capacity: Some(128),
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(resolved.origin_directory_positive_ttl_sec, 600);
    assert_eq!(resolved.origin_directory_negative_ttl_sec, 5);
    assert_eq!(resolved.origin_directory_cache_capacity, 128);
    Ok(())
}

#[test]
fn resolve_blockchain_chain_staleness_grace_defaults_when_unset() -> anyhow::Result<()> {
    // An absent grace resolves to the 30-minute default.
    let dir = data_dir_with_keystore()?;
    let cli = empty_blockchain_args();
    let file = types::BlockchainConfig {
        rpc_url: Some("https://example/rpc".to_string()),
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        chain_staleness_grace_sec: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(
        resolved.chain_staleness_grace_sec,
        DEFAULT_CHAIN_STALENESS_GRACE_SEC
    );
    Ok(())
}

#[test]
fn resolve_blockchain_chain_staleness_grace_passes_through() -> anyhow::Result<()> {
    // A large explicit value survives — this is how an operator opts out of
    // the stop-serving-while-stale behavior.
    let dir = data_dir_with_keystore()?;
    let cli = empty_blockchain_args();
    let file = types::BlockchainConfig {
        rpc_url: Some("https://example/rpc".to_string()),
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        chain_staleness_grace_sec: Some(31_536_000),
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(resolved.chain_staleness_grace_sec, 31_536_000);
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_chain_staleness_grace() -> anyhow::Result<()> {
    // `0` is not a disable sentinel — a zero grace refuses every serve — so
    // it is rejected, and the error names the field.
    let dir = data_dir_with_keystore()?;
    let cli = empty_blockchain_args();
    let file = types::BlockchainConfig {
        rpc_url: Some("https://example/rpc".to_string()),
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        chain_staleness_grace_sec: Some(0),
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to reject a zero grace");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("blockchain.chain_staleness_grace_sec"),
        "error should name the field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_fails_when_keystore_missing() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on missing keystore");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("invalid eth_keystore"),
        "error should name eth_keystore: {msg}"
    );
    assert!(
        msg.contains("cannot access"),
        "error should describe access failure: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_fails_when_cli_keystore_override_missing() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let bogus = dir.path().join("does-not-exist.json");
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: Some(bogus),
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on bogus --eth-keystore");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("invalid eth_keystore"), "{msg}");
    assert!(
        msg.contains("does-not-exist.json"),
        "error should cite the overridden path: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_directory_as_keystore() -> anyhow::Result<()> {
    // `File::open` accepts a directory on Linux, so the explicit `is_file`
    // check is the only thing standing between the node and a later panic.
    let dir = TempDir::new()?;
    std::fs::create_dir(dir.path().join("keystore.json"))?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected resolve_blockchain to fail on directory keystore");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("invalid eth_keystore"), "{msg}");
    assert!(
        msg.contains("not a regular file"),
        "error should say 'not a regular file': {msg}"
    );
    Ok(())
}

// -- helpers for full RunArgs construction (issue #217) ---------------

fn empty_identity_args() -> crate::cli::run::IdentityArgs {
    crate::cli::run::IdentityArgs {
        data_dir: None,
        region: None,
    }
}

fn empty_network_args() -> crate::cli::run::NetworkArgs {
    crate::cli::run::NetworkArgs {
        bind_port: None,
        relay_url: None,
    }
}

fn empty_blockchain_args() -> crate::cli::run::BlockchainArgs {
    crate::cli::run::BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: None,
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: None,
        capacity_bond_address: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    }
}

fn empty_cache_args() -> crate::cli::run::CacheArgs {
    crate::cli::run::CacheArgs {
        cache_dir: None,
        cache_size_mb: None,
        max_blob_size_mb: None,
        max_rate_per_mb: None,
        max_probe_holds: None,
        stake_lane_reserved_holds: None,
    }
}

fn empty_payment_args() -> crate::cli::run::PaymentArgs {
    crate::cli::run::PaymentArgs { rate_per_mb: None }
}

fn empty_observability_args() -> crate::cli::run::ObservabilityArgs {
    crate::cli::run::ObservabilityArgs {
        log_level: None,
        log_format: None,
        metrics_port: None,
        metrics_bind: None,
        admin_port: None,
        otlp_endpoint: None,
    }
}

fn empty_run_args() -> RunArgs {
    RunArgs {
        identity: empty_identity_args(),
        network: empty_network_args(),
        blockchain: empty_blockchain_args(),
        cache: empty_cache_args(),
        payment: empty_payment_args(),
        observability: empty_observability_args(),
    }
}

// ---- resolve_identity: CLI > file, file used when CLI omits ----------

#[test]
fn resolve_identity_cli_region_overrides_file() -> anyhow::Result<()> {
    let mut cli = empty_identity_args();
    cli.region = Some("de".to_string());
    cli.data_dir = Some(PathBuf::from("/tmp/cli-data"));
    let file = types::IdentityConfig {
        data_dir: Some(PathBuf::from("/tmp/file-data")),
        region: Some("us".to_string()),
    };
    let resolved = resolve_identity(&cli, Some(&file))?;
    assert_eq!(resolved.region.as_deref(), Some("DE"));
    assert_eq!(resolved.data_dir, PathBuf::from("/tmp/cli-data"));
    Ok(())
}

#[test]
fn resolve_identity_falls_back_to_file_when_cli_absent() -> anyhow::Result<()> {
    let cli = empty_identity_args();
    let file = types::IdentityConfig {
        data_dir: Some(PathBuf::from("/tmp/file-data")),
        region: Some("sg".to_string()),
    };
    let resolved = resolve_identity(&cli, Some(&file))?;
    // Region is normalized to upper case.
    assert_eq!(resolved.region.as_deref(), Some("SG"));
    assert_eq!(resolved.data_dir, PathBuf::from("/tmp/file-data"));
    Ok(())
}

// ---- resolve_network: CLI > file, default when both omit -------------

#[test]
fn resolve_network_cli_bind_port_overrides_file() {
    let mut cli = empty_network_args();
    cli.bind_port = Some(5555);
    let file = types::NetworkConfig {
        bind_port: Some(6666),
        relay_urls: None,
        discovery: None,
    };
    let resolved = resolve_network(&cli, Some(&file));
    assert_eq!(resolved.bind_port, 5555);
}

#[test]
fn resolve_network_uses_file_when_cli_absent() {
    let cli = empty_network_args();
    let file = types::NetworkConfig {
        bind_port: Some(6666),
        relay_urls: Some(vec!["https://relay.example".to_string()]),
        discovery: None,
    };
    let resolved = resolve_network(&cli, Some(&file));
    assert_eq!(resolved.bind_port, 6666);
    assert_eq!(
        resolved.relay_urls,
        vec!["https://relay.example".to_string()]
    );
}

#[test]
fn resolve_network_uses_relay_urls_list() {
    let cli = empty_network_args();
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec![
            "https://relay-a.example".to_string(),
            "https://relay-b.example".to_string(),
        ]),
        discovery: None,
    };
    let resolved = resolve_network(&cli, Some(&file));
    assert_eq!(
        resolved.relay_urls,
        vec![
            "https://relay-a.example".to_string(),
            "https://relay-b.example".to_string()
        ]
    );
}

#[test]
fn resolve_network_cli_relay_url_overrides_file_list() {
    // The singular `--relay-url` CLI flag takes precedence over the file
    // list, preserving the existing single-relay override semantics. This
    // is also the #843 notice trigger (CLI/env relay set while a non-empty
    // `relay_urls` list exists); the notice itself is asserted by
    // `network_cli_relay_override_of_a_file_list_warns`.
    let mut cli = empty_network_args();
    cli.relay_url = Some("https://cli.example".to_string());
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec!["https://list.example".to_string()]),
        discovery: None,
    };
    let resolved = resolve_network(&cli, Some(&file));
    assert_eq!(resolved.relay_urls, vec!["https://cli.example".to_string()]);
}

/// `[network]` carries no 0-RTT knob, and the section denies unknown
/// fields, so a config still setting one fails to load rather than
/// being silently ignored — the operator has to delete the line.
#[test]
fn network_rejects_removed_0rtt_key() -> anyhow::Result<()> {
    let toml = "
            bind_port = 4433
            enable_0rtt = true
        ";
    let err = toml::from_str::<types::NetworkConfig>(toml)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected unknown-field error"))?;
    let msg = format!("{err}");
    anyhow::ensure!(
        msg.contains("unknown field") && msg.contains("enable_0rtt"),
        "got: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_network_default_bind_port_when_unset() {
    let cli = empty_network_args();
    let resolved = resolve_network(&cli, None);
    assert_eq!(resolved.bind_port, DEFAULT_BIND_PORT);
    assert!(resolved.relay_urls.is_empty());
}

// ---- resolve_network: validate-time relay URL parse check (#818) ------

#[test]
fn resolve_network_accepts_well_formed_relay_urls() {
    // The happy path records no problems. Includes a non-`http(s)` scheme
    // (`relay://`) that iroh's `RelayUrl` accepts at bring-up, so the
    // validate-time check must accept it too — it is a parse-only subset
    // of `RelayUrl`, never stricter (see the worker doc).
    let cli = empty_network_args();
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec![
            "https://relay-a.example".to_string(),
            "relay://no-port-host".to_string(),
        ]),
        discovery: None,
    };
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_network_into(&cli, Some(&file), &mut bag);
    assert_eq!(resolved.relay_urls.len(), 2);
    assert!(
        bag.into_result().is_ok(),
        "well-formed relay URLs must not record a problem"
    );
}

#[test]
fn resolve_network_records_problem_for_malformed_relay_url() {
    let cli = empty_network_args();
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec!["not a url".to_string()]),
        discovery: None,
    };
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    // Names the indexed field and echoes the offending entry so the
    // operator can find it.
    assert!(msg.contains("network.relay_urls[0]"), "{msg}");
    assert!(msg.contains("not a url"), "{msg}");
}

#[test]
fn resolve_network_reports_each_malformed_relay_entry_by_index() {
    // A clean entry between two malformed ones: only the bad indices are
    // reported, each under its own label, in order.
    let cli = empty_network_args();
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec![
            "::: bad".to_string(),
            "https://good.example".to_string(),
            "also bad".to_string(),
        ]),
        discovery: None,
    };
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(msg.contains("configuration has 2 problem(s):"), "{msg}");
    assert!(msg.contains("network.relay_urls[0]"), "{msg}");
    assert!(msg.contains("network.relay_urls[2]"), "{msg}");
    assert!(!msg.contains("network.relay_urls[1]"), "{msg}");
}

#[test]
fn resolve_network_redacts_credentials_in_malformed_relay_error() {
    // A malformed entry can still carry `user:pass@`; the validate-time
    // error must not leak it, matching bring-up's `parse_relay_urls`
    // invariant. `host:notaport` fails `url::Url::parse` (bad port), so it
    // reaches the error arm with credentials attached.
    let cli = empty_network_args();
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec!["https://user:s3cret@host:notaport".to_string()]),
        discovery: None,
    };
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(msg.contains("network.relay_urls[0]"), "{msg}");
    assert!(!msg.contains("s3cret"), "credentials leaked: {msg}");
    assert!(!msg.contains("user:"), "userinfo leaked: {msg}");
    assert!(
        msg.contains("***@host"),
        "redacted host should appear: {msg}"
    );
}

#[test]
fn resolve_network_validates_cli_relay_url_flag() {
    // The CLI `--relay-url` singular flag is also validated and, like the
    // alias, reported under the singular `network.relay_url` label.
    let mut cli = empty_network_args();
    cli.relay_url = Some("not a url".to_string());
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, None, &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(
        msg.contains("network.relay_url:") && !msg.contains("network.relay_urls["),
        "CLI singular source must use the singular label: {msg}"
    );
}

// ---- resolve_discovery: operator-configurable discovery (#818 scope 1) ----

/// A valid 64-hex `NodeId` for peer-map tests (`iroh::PublicKey::FromStr`
/// accepts the hex form). Distinct nibbles so a wrong byte order would show.
const DISCOVERY_PEER_ID: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn network_with_discovery(discovery: types::DiscoveryConfig) -> types::NetworkConfig {
    types::NetworkConfig {
        bind_port: None,
        relay_urls: None,
        discovery: Some(discovery),
    }
}

#[test]
fn resolve_discovery_empty_when_absent() {
    let cli = empty_network_args();
    let resolved = resolve_network(&cli, None);
    assert!(resolved.discovery.is_empty());
}

#[test]
fn resolve_discovery_accepts_pkarr_and_dns() {
    let cli = empty_network_args();
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: Some("https://pkarr.example/".to_string()),
        dns_origin: Some("discovery.example.".to_string()),
        peers: None,
    });
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_network_into(&cli, Some(&file), &mut bag);
    assert!(bag.into_result().is_ok());
    assert_eq!(
        resolved.discovery.pkarr_url.as_deref(),
        Some("https://pkarr.example/")
    );
    assert_eq!(
        resolved.discovery.dns_origin.as_deref(),
        Some("discovery.example.")
    );
}

#[test]
fn resolve_discovery_accepts_dns_only() {
    // A resolve-only node (resolves peers via DNS, publishes nothing) is
    // valid: only the reverse — publish without a resolver — is rejected.
    let cli = empty_network_args();
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: Some("discovery.example.".to_string()),
        peers: None,
    });
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_network_into(&cli, Some(&file), &mut bag);
    assert!(bag.into_result().is_ok());
    assert!(resolved.discovery.pkarr_url.is_none());
    assert_eq!(
        resolved.discovery.dns_origin.as_deref(),
        Some("discovery.example.")
    );
}

#[test]
fn resolve_discovery_rejects_pkarr_without_dns() {
    // Publishing to a pkarr relay with no resolver to read it back is a
    // misconfiguration; the error points at the missing dns_origin.
    let cli = empty_network_args();
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: Some("https://pkarr.example/".to_string()),
        dns_origin: None,
        peers: None,
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(msg.contains("network.discovery.dns_origin"), "{msg}");
}

#[test]
fn resolve_discovery_rejects_malformed_pkarr_url() {
    // A malformed pkarr_url can carry credentials; the error must name the
    // field, echo the entry, and never leak userinfo.
    let cli = empty_network_args();
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: Some("https://user:s3cret@host:notaport".to_string()),
        dns_origin: Some("discovery.example.".to_string()),
        peers: None,
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(msg.contains("network.discovery.pkarr_url"), "{msg}");
    assert!(!msg.contains("s3cret"), "credentials leaked: {msg}");
    assert!(
        msg.contains("***@host"),
        "redacted host should appear: {msg}"
    );
}

#[test]
fn resolve_discovery_rejects_empty_dns_origin() {
    let cli = empty_network_args();
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: Some("   ".to_string()),
        peers: None,
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(msg.contains("network.discovery.dns_origin"), "{msg}");
}

#[test]
fn resolve_discovery_accepts_valid_peers() {
    // A peer with a relay URL and both a v4 and a bracketed-v6 direct addr.
    let cli = empty_network_args();
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        DISCOVERY_PEER_ID.to_string(),
        types::DiscoveryPeer {
            relay_url: Some("https://relay.example/".to_string()),
            addrs: vec![
                "203.0.113.4:4433".to_string(),
                "[2001:db8::1]:4433".to_string(),
            ],
        },
    );
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: None,
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_network_into(&cli, Some(&file), &mut bag);
    assert!(bag.into_result().is_ok());
    assert_eq!(resolved.discovery.peers.len(), 1);
    let peer = resolved.discovery.peers.first().expect("one peer");
    assert_eq!(peer.node_id, DISCOVERY_PEER_ID);
    assert_eq!(peer.addrs.len(), 2);
}

#[test]
fn resolve_discovery_rejects_bad_node_id() {
    let cli = empty_network_args();
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        "not-a-node-id".to_string(),
        types::DiscoveryPeer {
            relay_url: None,
            addrs: vec!["203.0.113.4:4433".to_string()],
        },
    );
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: None,
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(
        msg.contains("network.discovery.peers[not-a-node-id]"),
        "{msg}"
    );
}

#[test]
fn resolve_discovery_rejects_bad_socket_addr() {
    let cli = empty_network_args();
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        DISCOVERY_PEER_ID.to_string(),
        types::DiscoveryPeer {
            relay_url: None,
            addrs: vec!["not-a-socket-addr".to_string()],
        },
    );
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: None,
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(
        msg.contains(&format!(
            "network.discovery.peers[{DISCOVERY_PEER_ID}].addrs[0]"
        )),
        "{msg}"
    );
}

#[test]
fn resolve_discovery_coexists_peers_and_pkarr_dns() {
    // All three providers set together is valid — they compose.
    let cli = empty_network_args();
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        DISCOVERY_PEER_ID.to_string(),
        types::DiscoveryPeer {
            relay_url: Some("https://relay.example/".to_string()),
            addrs: Vec::new(),
        },
    );
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: Some("https://pkarr.example/".to_string()),
        dns_origin: Some("discovery.example.".to_string()),
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_network_into(&cli, Some(&file), &mut bag);
    assert!(bag.into_result().is_ok());
    assert!(resolved.discovery.pkarr_url.is_some());
    assert!(resolved.discovery.dns_origin.is_some());
    assert_eq!(resolved.discovery.peers.len(), 1);
}

#[test]
fn resolve_discovery_rejects_uppercase_node_id() {
    // An uppercase 64-hex id parses under a bare hex check but iroh's
    // `PublicKey::from_str` decodes lowercase-hex only — so validate must
    // reject it, matching what the node would do at bring-up. Guards the
    // validate==parse contract against a regression to a looser hex check.
    let cli = empty_network_args();
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        DISCOVERY_PEER_ID.to_uppercase(),
        types::DiscoveryPeer {
            relay_url: None,
            addrs: vec!["203.0.113.4:4433".to_string()],
        },
    );
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: None,
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(
        msg.contains("network.discovery.peers[") && msg.contains("NodeId"),
        "uppercase id must be rejected at validate, not at bring-up: {msg}"
    );
}

#[test]
fn resolve_discovery_rejects_malformed_peer_relay_url_and_redacts() {
    // A peer relay_url is validated and, like the pkarr URL, can carry
    // credentials — the error must name the field and not leak userinfo.
    let cli = empty_network_args();
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        DISCOVERY_PEER_ID.to_string(),
        types::DiscoveryPeer {
            relay_url: Some("https://user:s3cret@host:notaport".to_string()),
            addrs: Vec::new(),
        },
    );
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: None,
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(
        msg.contains(&format!(
            "network.discovery.peers[{DISCOVERY_PEER_ID}].relay_url"
        )),
        "{msg}"
    );
    assert!(!msg.contains("s3cret"), "credentials leaked: {msg}");
    assert!(
        msg.contains("***@host"),
        "redacted host should appear: {msg}"
    );
}

#[test]
fn resolve_discovery_accumulates_every_bad_field_in_a_peer() {
    // A single peer with both a bad relay_url and a bad addr records BOTH
    // problems (not fail-fast), so an operator sees every fix at once.
    let cli = empty_network_args();
    let mut peers = std::collections::HashMap::new();
    peers.insert(
        DISCOVERY_PEER_ID.to_string(),
        types::DiscoveryPeer {
            relay_url: Some("not a url".to_string()),
            addrs: vec!["not-a-socket-addr".to_string()],
        },
    );
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: None,
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let _ = resolve_network_into(&cli, Some(&file), &mut bag);
    assert_eq!(bag.problem_count(), 2, "both fields should be reported");
    let msg = format!("{:#}", bag.into_result().unwrap_err());
    assert!(
        msg.contains(&format!(
            "network.discovery.peers[{DISCOVERY_PEER_ID}].relay_url"
        )),
        "{msg}"
    );
    assert!(
        msg.contains(&format!(
            "network.discovery.peers[{DISCOVERY_PEER_ID}].addrs[0]"
        )),
        "{msg}"
    );
}

#[test]
fn resolve_discovery_sorts_peers_by_node_id() {
    // `peers` is sorted by node_id so the node build and tests are stable
    // despite nondeterministic `HashMap` order. Two distinct valid ids.
    let cli = empty_network_args();
    let id_a = iroh::SecretKey::generate().public().to_string();
    let id_b = iroh::SecretKey::generate().public().to_string();
    let mut peers = std::collections::HashMap::new();
    for id in [&id_a, &id_b] {
        peers.insert(
            id.clone(),
            types::DiscoveryPeer {
                relay_url: Some("https://relay.example/".to_string()),
                addrs: Vec::new(),
            },
        );
    }
    let file = network_with_discovery(types::DiscoveryConfig {
        pkarr_url: None,
        dns_origin: None,
        peers: Some(peers),
    });
    let mut bag = ConfigDiagnostics::new();
    let resolved = resolve_network_into(&cli, Some(&file), &mut bag);
    assert!(bag.into_result().is_ok());
    assert_eq!(resolved.discovery.peers.len(), 2);
    let ids: Vec<String> = resolved
        .discovery
        .peers
        .iter()
        .map(|p| p.node_id.clone())
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "peers must be sorted by node_id");
}

// ---- resolve_blockchain: CLI > file, missing-required errors ---------

#[test]
fn resolve_blockchain_cli_rpc_url_overrides_file() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://cli-wins.example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: None,
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://file-loses.example/rpc".to_string()),
        eth_keystore: None,
        payment_pool_address: None,
        capacity_bond_address: None,
        rpc_watchdog_interval_sec: None,
        event_poll_interval_ms: None,
        redeem_threshold_micro_usdc: None,
        redeem_interval_secs: None,
        buyer_working_deposit_micro_usdc: None,
        buyer_max_approve: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        chain_id: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    // url::Url normalisation appends a trailing path on bare-host URLs;
    // both inputs already include `/rpc`, so the prefix match suffices
    // and is robust against future normalisation tweaks.
    assert!(
        resolved.rpc_url.starts_with("https://cli-wins.example/rpc"),
        "expected CLI rpc_url to win, got {}",
        resolved.rpc_url,
    );
    Ok(())
}

#[test]
fn resolve_blockchain_uses_file_rpc_url_when_cli_absent() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = empty_blockchain_args();
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: None,
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://file-only.example/rpc".to_string()),
        eth_keystore: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        rpc_watchdog_interval_sec: None,
        event_poll_interval_ms: None,
        redeem_threshold_micro_usdc: None,
        redeem_interval_secs: None,
        buyer_working_deposit_micro_usdc: None,
        buyer_max_approve: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        chain_id: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert!(
        resolved
            .rpc_url
            .starts_with("https://file-only.example/rpc"),
        "expected file rpc_url to be used, got {}",
        resolved.rpc_url,
    );
    Ok(())
}

#[test]
fn resolve_blockchain_errors_when_rpc_url_missing() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: None,
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error when rpc_url missing");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("--rpc-url") && msg.contains("rpc_url"),
        "error should mention rpc_url and the flag form: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_errors_when_payment_pool_address_missing() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: None,
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error when payment_pool_address missing");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("payment_pool_address"),
        "error should mention payment_pool_address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_errors_when_capacity_bond_address_missing() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error when capacity_bond_address missing");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("capacity_bond_address"),
        "error should mention capacity_bond_address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_errors_when_slash_judge_address_missing() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: None,
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error when slash_judge_address missing");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("slash_judge_address"),
        "error should mention slash_judge_address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_errors_when_content_blacklist_address_missing() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error when content_blacklist_address is missing");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("content_blacklist_address") && msg.contains("--content-blacklist-address"),
        "error should name the required config key and CLI flag: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_slash_judge_address() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some("0x0000000000000000000000000000000000000000".to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error for zero slash_judge_address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("slash_judge_address") && msg.contains("zero address"),
        "error should reject the zero address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_content_blacklist_address() -> anyhow::Result<()> {
    // The zero address is a fail-open compliance trap: every scope check
    // reverts, nothing is evicted, and the operator believes the watcher
    // is active. Must be rejected like `slash_judge_address`.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some("0x0000000000000000000000000000000000000000".to_string()),
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error for zero content_blacklist_address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("content_blacklist_address") && msg.contains("zero address"),
        "error should reject the zero address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_payment_pool_address() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some("0x0000000000000000000000000000000000000000".to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error for zero payment_pool_address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("payment_pool_address") && msg.contains("zero address"),
        "error should reject the zero address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_capacity_bond_address() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some("0x0000000000000000000000000000000000000000".to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error for zero capacity_bond_address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("capacity_bond_address") && msg.contains("zero address"),
        "error should reject the zero address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_origin_assignment_address() -> anyhow::Result<()> {
    // Only origin_assignment is zeroed, to exercise its zero-address guard in
    // isolation; publisher_registry carries a valid address (the two are
    // validated independently, with no both-or-neither pairing).
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: Some("0x0000000000000000000000000000000000000000".to_string()),
        publisher_registry_address: Some(GOOD_ADDR.to_string()),
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error for zero origin_assignment_address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("origin_assignment_address") && msg.contains("zero address"),
        "error should reject the zero address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_publisher_registry_address() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: Some(GOOD_ADDR.to_string()),
        publisher_registry_address: Some("0x0000000000000000000000000000000000000000".to_string()),
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error for zero publisher_registry_address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("publisher_registry_address") && msg.contains("zero address"),
        "error should reject the zero address: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_content_blacklist_poll_interval() -> anyhow::Result<()> {
    // A zero interval panics `tokio::time::interval_at`, killing the watcher.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: Some(0),
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error for zero content_blacklist_poll_interval_sec");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("content_blacklist_poll_interval_sec") && msg.contains("must not be 0"),
        "error should reject the zero poll interval: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_chain_id() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: Some(0),
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error for chain_id=0");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("chain_id") && msg.contains("must not be 0"),
        "error should reject chain_id=0: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_treats_empty_string_rpc_url_as_missing() -> anyhow::Result<()> {
    // Mirror the `.filter(|s| !s.is_empty())` guard: a blank value (e.g.
    // `DECDN_RPC_URL=""`) must surface the same "missing" diagnostic as
    // an absent value rather than silently passing `""` to url::Url.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some(String::new()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let Err(err) = resolve_blockchain(&cli, None, dir.path()) else {
        anyhow::bail!("expected error when rpc_url is empty string");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("--rpc-url"),
        "empty rpc_url should surface as missing: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_small_nonzero_watchdog_interval() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: None,
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: None,
        eth_keystore: None,
        payment_pool_address: None,
        capacity_bond_address: None,
        rpc_watchdog_interval_sec: Some(1),
        event_poll_interval_ms: None,
        redeem_threshold_micro_usdc: None,
        redeem_interval_secs: None,
        buyer_working_deposit_micro_usdc: None,
        buyer_max_approve: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        chain_id: None,
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error when watchdog interval is below the minimum");
    };
    let msg = format!("{err:#}");
    let expected_min = format!("minimum {MIN_RPC_WATCHDOG_INTERVAL_SEC}s");
    assert!(
        msg.contains("rpc_watchdog_interval_sec") && msg.contains(&expected_min),
        "error should mention the field and the {expected_min} floor: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_redeem_threshold() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: None,
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: None,
        eth_keystore: None,
        payment_pool_address: None,
        capacity_bond_address: None,
        rpc_watchdog_interval_sec: None,
        event_poll_interval_ms: None,
        redeem_threshold_micro_usdc: Some(0),
        redeem_interval_secs: None,
        buyer_working_deposit_micro_usdc: None,
        buyer_max_approve: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        chain_id: None,
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error when redeem threshold is 0");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("redeem_threshold_micro_usdc"),
        "error should name the field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_redeem_interval() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        redeem_interval_secs: Some(0),
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error when redeem interval is 0");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("redeem_interval_secs"),
        "error should name the field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_applies_default_redeem_interval_when_absent() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert_eq!(resolved.redeem_interval_secs, DEFAULT_REDEEM_INTERVAL_SECS);
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_redeem_interval_above_grace_margin() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        redeem_interval_secs: Some(MAX_REDEEM_INTERVAL_SECS + 1),
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error when redeem interval exceeds the 6h cap");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("redeem_interval_secs"),
        "error should name the field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_accepts_redeem_interval_at_grace_margin() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        redeem_interval_secs: Some(MAX_REDEEM_INTERVAL_SECS),
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(resolved.redeem_interval_secs, MAX_REDEEM_INTERVAL_SECS);
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_zero_redeem_max_vouchers_per_tx() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        redeem_max_vouchers_per_tx: Some(0),
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error when redeem_max_vouchers_per_tx is 0");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("redeem_max_vouchers_per_tx"),
        "error should name the field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_applies_default_redeem_max_vouchers_per_tx_when_absent() -> anyhow::Result<()>
{
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert_eq!(
        resolved.redeem_max_vouchers_per_tx,
        DEFAULT_REDEEM_MAX_VOUCHERS_PER_TX
    );
    Ok(())
}

#[test]
fn resolve_blockchain_applies_default_pool_floor_when_absent() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert_eq!(
        resolved.pool_min_remaining_deposit_micro_usdc,
        DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC
    );
    Ok(())
}

#[test]
fn resolve_blockchain_threads_explicit_pool_floor() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        pool_min_remaining_deposit_micro_usdc: Some(2_500_000),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(resolved.pool_min_remaining_deposit_micro_usdc, 2_500_000);
    Ok(())
}

#[test]
fn resolve_blockchain_accepts_zero_watchdog_interval() -> anyhow::Result<()> {
    // `0` is the documented disable sentinel and must bypass the floor.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: None,
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: None,
        eth_keystore: None,
        payment_pool_address: None,
        capacity_bond_address: None,
        rpc_watchdog_interval_sec: Some(0),
        event_poll_interval_ms: None,
        redeem_threshold_micro_usdc: None,
        redeem_interval_secs: None,
        buyer_working_deposit_micro_usdc: None,
        buyer_max_approve: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        chain_id: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(resolved.rpc_watchdog_interval_sec, 0);
    Ok(())
}

#[test]
fn resolve_blockchain_accepts_min_watchdog_interval() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: None,
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: None,
        eth_keystore: None,
        payment_pool_address: None,
        capacity_bond_address: None,
        rpc_watchdog_interval_sec: Some(MIN_RPC_WATCHDOG_INTERVAL_SEC),
        event_poll_interval_ms: None,
        redeem_threshold_micro_usdc: None,
        redeem_interval_secs: None,
        buyer_working_deposit_micro_usdc: None,
        buyer_max_approve: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        chain_id: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&file), dir.path())?;
    assert_eq!(
        resolved.rpc_watchdog_interval_sec,
        MIN_RPC_WATCHDOG_INTERVAL_SEC
    );
    Ok(())
}

#[test]
fn resolve_blockchain_applies_default_watchdog_interval_when_absent() -> anyhow::Result<()> {
    // Pins the no-config bootstrap path: if a future change moved
    // DEFAULT below MIN (or to 0), every operator without an explicit
    // setting would silently lose the watchdog.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert_eq!(
        resolved.rpc_watchdog_interval_sec,
        DEFAULT_RPC_WATCHDOG_INTERVAL_SEC
    );
    Ok(())
}

#[test]
fn buyer_working_deposit_zero_is_rejected() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        content_blacklist_poll_interval_sec: None,
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: None,
        eth_keystore: None,
        payment_pool_address: None,
        capacity_bond_address: None,
        rpc_watchdog_interval_sec: None,
        event_poll_interval_ms: None,
        redeem_threshold_micro_usdc: None,
        redeem_interval_secs: None,
        buyer_working_deposit_micro_usdc: Some(0),
        buyer_max_approve: None,
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        chain_id: None,
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error when buyer working deposit is 0");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("blockchain.buyer_working_deposit_micro_usdc"),
        "error should name the field: {msg}"
    );
    Ok(())
}

/// The per-signer floor knob (ADR 003 § Pool solvency) defaults to `8` live
/// windows and threads an explicit value through. No value is rejected: it is
/// lower-clamped at use, so there is nothing for the resolver to bound.
#[test]
fn pool_floor_signer_knobs_default_and_thread() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    // Literals, not the constants: asserting a value against the constant it
    // came from passes whatever the constant says, and the shipped defaults are
    // documented in several places that have to agree with it.
    assert_eq!(resolved.pool_floor_signer_live_windows, 8);
    assert_eq!(DEFAULT_POOL_FLOOR_SIGNER_LIVE_WINDOWS, 8);

    let explicit = types::BlockchainConfig {
        pool_floor_signer_live_windows: Some(16),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&explicit), dir.path())?;
    assert_eq!(resolved.pool_floor_signer_live_windows, 16);
    Ok(())
}

#[test]
fn buyer_working_deposit_defaults_when_absent() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert_eq!(resolved.buyer_working_deposit_micro_usdc, 10_000_000);
    Ok(())
}

#[test]
fn resolve_blockchain_rejects_event_poll_interval_below_minimum() -> anyhow::Result<()> {
    // #1011/#1106: a sub-minimum interval would drive every eth_getLogs
    // watcher tick — and the pending-tx receipt heartbeat — too frequently,
    // so resolution must reject it.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: None,
        chain_id: None,
    };
    let file = types::BlockchainConfig {
        event_poll_interval_ms: Some(MIN_EVENT_POLL_INTERVAL_MS - 1),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&cli, Some(&file), dir.path()) else {
        anyhow::bail!("expected error when event poll interval is below the minimum");
    };
    let msg = format!("{err:#}");
    let expected_min = format!("minimum {MIN_EVENT_POLL_INTERVAL_MS}ms");
    assert!(
        msg.contains("event_poll_interval_ms") && msg.contains(&expected_min),
        "error should mention the field and the {expected_min} floor: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_applies_default_event_poll_interval_when_absent() -> anyhow::Result<()> {
    // Pins the no-config bootstrap path: the default must stay at or above
    // MIN so an operator without an explicit setting never floods the RPC.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let resolved = resolve_blockchain(&cli, None, dir.path())?;
    assert_eq!(
        resolved.event_poll_interval_ms,
        DEFAULT_EVENT_POLL_INTERVAL_MS
    );
    assert!(resolved.event_poll_interval_ms >= MIN_EVENT_POLL_INTERVAL_MS);
    Ok(())
}

#[test]
fn resolve_blockchain_accepts_min_and_in_range_event_poll_interval() -> anyhow::Result<()> {
    // Pins the inclusive floor (exactly MIN is accepted, guarding an
    // off-by-one regression to `>`) and that an in-range value is preserved
    // through resolution rather than clamped. Mirrors the
    // `accepts_min_watchdog_interval` boundary test.
    let dir = data_dir_with_keystore()?;
    let cli = BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    };
    let at_min = types::BlockchainConfig {
        event_poll_interval_ms: Some(MIN_EVENT_POLL_INTERVAL_MS),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&at_min), dir.path())?;
    assert_eq!(resolved.event_poll_interval_ms, MIN_EVENT_POLL_INTERVAL_MS);

    let in_range = types::BlockchainConfig {
        event_poll_interval_ms: Some(1000),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        slash_appeal_address: None,
        content_blacklist_address: None,
        ..Default::default()
    };
    let resolved = resolve_blockchain(&cli, Some(&in_range), dir.path())?;
    assert_eq!(resolved.event_poll_interval_ms, 1000);
    Ok(())
}

fn get_logs_span_cli() -> BlockchainArgs {
    BlockchainArgs {
        origin_assignment_address: None,
        publisher_registry_address: None,
        rpc_url: Some("https://example/rpc".to_string()),
        eth_keystore: None,
        keystore_password_file: None,
        payment_pool_address: Some(GOOD_ADDR.to_string()),
        capacity_bond_address: Some(GOOD_ADDR.to_string()),
        slash_judge_address: Some(GOOD_ADDR.to_string()),
        content_blacklist_address: Some(GOOD_ADDR.to_string()),
        chain_id: None,
    }
}

#[test]
fn resolve_blockchain_rejects_zero_get_logs_max_block_span() -> anyhow::Result<()> {
    // A zero span would scan no blocks: every chain watcher would go blind.
    let dir = data_dir_with_keystore()?;
    let file = types::BlockchainConfig {
        get_logs_max_block_span: Some(0),
        ..Default::default()
    };
    let Err(err) = resolve_blockchain(&get_logs_span_cli(), Some(&file), dir.path()) else {
        anyhow::bail!("expected an error for get_logs_max_block_span = 0");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("get_logs_max_block_span"),
        "error should name the field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_blockchain_applies_default_get_logs_max_block_span_when_absent() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let resolved = resolve_blockchain(&get_logs_span_cli(), None, dir.path())?;
    assert_eq!(
        resolved.get_logs_max_block_span,
        DEFAULT_GET_LOGS_MAX_BLOCK_SPAN
    );
    Ok(())
}

#[test]
fn resolve_blockchain_accepts_a_provider_sized_get_logs_max_block_span() -> anyhow::Result<()> {
    // One block is the smallest span that still scans; 10 is Alchemy's
    // free-tier cap.
    let dir = data_dir_with_keystore()?;
    for span in [1, 10] {
        let file = types::BlockchainConfig {
            get_logs_max_block_span: Some(span),
            ..Default::default()
        };
        let resolved = resolve_blockchain(&get_logs_span_cli(), Some(&file), dir.path())?;
        assert_eq!(resolved.get_logs_max_block_span, span);
    }
    Ok(())
}

// ---- resolve_cache: CLI > file, defaults, tilde expansion ------------

#[test]
fn resolve_cache_cli_cache_dir_overrides_file_and_expands_tilde() -> anyhow::Result<()> {
    // Hermetic: inject a stub home so the assertion holds whether or
    // not the host's `dirs::home_dir()` returns Some, and so the
    // assertion exercises the documented behaviour (tilde-expand
    // against $HOME).
    let home_dir = TempDir::new()?;
    let home = home_dir.path().to_path_buf();
    let mut cli = empty_cache_args();
    cli.cache_dir = Some(PathBuf::from("~/from-cli"));
    let file = types::CacheConfig {
        cache_dir: Some(PathBuf::from("/from/file")),
        cache_size_mb: None,
        max_blob_size_mb: None,
        ..Default::default()
    };
    let resolved = common::test_support::with_home_override(Some(&home), || {
        resolve_cache(&cli, Some(&file), Path::new("/data-dir"))
    })?;
    assert_eq!(resolved.cache_dir, home.join("from-cli"));
    Ok(())
}

#[test]
fn resolve_cache_cli_cache_dir_passes_through_when_home_unavailable() -> anyhow::Result<()> {
    // Sibling of the above: the documented contract is "log + leave
    // path unchanged" when `dirs::home_dir()` is None (see
    // `cli::common::expand_tilde`). Verify resolve_cache honours it.
    let mut cli = empty_cache_args();
    cli.cache_dir = Some(PathBuf::from("~/from-cli"));
    let resolved = common::test_support::with_home_override(None, || {
        resolve_cache(&cli, None, Path::new("/data-dir"))
    })?;
    assert_eq!(resolved.cache_dir, PathBuf::from("~/from-cli"));
    Ok(())
}

#[test]
fn resolve_cache_uses_file_cache_dir_when_cli_absent() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let file = types::CacheConfig {
        cache_dir: Some(PathBuf::from("/from/file")),
        cache_size_mb: None,
        max_blob_size_mb: None,
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/data-dir"))?;
    assert_eq!(resolved.cache_dir, PathBuf::from("/from/file"));
    Ok(())
}

#[test]
fn resolve_cache_falls_back_to_data_dir_subdirectory() -> anyhow::Result<()> {
    // No CLI, no file: the documented fallback is `<data-dir>/cache`.
    let cli = empty_cache_args();
    let resolved = resolve_cache(&cli, None, Path::new("/data-dir"))?;
    assert_eq!(resolved.cache_dir, PathBuf::from("/data-dir/cache"));
    Ok(())
}

#[test]
fn resolve_cache_default_size_constants_apply_when_unset() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    assert_eq!(resolved.cache_size_mb, DEFAULT_CACHE_SIZE_MB);
    // Unset `max_blob_size_mb` defaults to `DEFAULT_MAX_BLOB_SIZE_MB` (below the
    // default cache budget, so the clamp is a no-op here).
    assert_eq!(resolved.max_blob_size_mb, DEFAULT_MAX_BLOB_SIZE_MB);
    Ok(())
}

#[test]
fn resolve_cache_default_blob_size_clamps_to_a_small_cache() -> anyhow::Result<()> {
    // A cache smaller than `DEFAULT_MAX_BLOB_SIZE_MB` with `max_blob_size_mb`
    // unset must resolve the blob ceiling DOWN to the cache size, not the 50 GB
    // default — otherwise the `max_blob <= cache_size` invariant would trip on a
    // perfectly valid small-cache config.
    let small = DEFAULT_MAX_BLOB_SIZE_MB / 2;
    let cli = cache_cli(Some(small), None);
    let resolved = resolve_cache(&cli, None, Path::new("/tmp"))?;
    assert_eq!(resolved.cache_size_mb, small);
    assert_eq!(resolved.max_blob_size_mb, small);
    anyhow::ensure!(resolved.max_blob_size_mb <= resolved.cache_size_mb);
    Ok(())
}

#[test]
fn resolve_cache_cli_size_overrides_file_size() -> anyhow::Result<()> {
    let mut cli = empty_cache_args();
    cli.cache_size_mb = Some(2_048);
    cli.max_blob_size_mb = Some(256);
    let file = types::CacheConfig {
        cache_dir: None,
        cache_size_mb: Some(99_999),
        max_blob_size_mb: Some(50_000),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    assert_eq!(resolved.cache_size_mb, 2_048);
    assert_eq!(resolved.max_blob_size_mb, 256);
    Ok(())
}

#[test]
fn resolve_cache_uses_file_size_when_cli_absent() -> anyhow::Result<()> {
    let cli = empty_cache_args();
    let file = types::CacheConfig {
        cache_dir: None,
        cache_size_mb: Some(2_048),
        max_blob_size_mb: Some(256),
        ..Default::default()
    };
    let resolved = resolve_cache(&cli, Some(&file), Path::new("/tmp"))?;
    assert_eq!(resolved.cache_size_mb, 2_048);
    assert_eq!(resolved.max_blob_size_mb, 256);
    Ok(())
}

// ---- resolve_payment: CLI > file (positive value path) ---------------

#[test]
fn resolve_payment_cli_rate_overrides_file_rate() -> anyhow::Result<()> {
    let cli = crate::cli::run::PaymentArgs {
        rate_per_mb: Some(99),
    };
    let file = types::PaymentConfig {
        rate_per_mb: Some(1),
        credit_max: None,
        credit_ramp_divisor: None,
        frame_target_bytes: None,
        voucher_commit_interval_ms: None,
    };
    let resolved = resolve_payment(&cli, Some(&file))?;
    assert_eq!(resolved.rate_per_mb, 99);
    Ok(())
}

#[test]
fn resolve_payment_uses_file_when_cli_absent() -> anyhow::Result<()> {
    let cli = empty_payment_args();
    let file = types::PaymentConfig {
        rate_per_mb: Some(50),
        credit_max: None,
        credit_ramp_divisor: None,
        frame_target_bytes: None,
        voucher_commit_interval_ms: None,
    };
    let resolved = resolve_payment(&cli, Some(&file))?;
    assert_eq!(resolved.rate_per_mb, 50);
    Ok(())
}

// ---- resolve_observability: CLI > file, defaults ---------------------

#[test]
fn resolve_observability_cli_log_level_overrides_file() -> anyhow::Result<()> {
    let mut cli = empty_observability_args();
    cli.log_level = Some(crate::cli::common::LogLevel::Trace);
    let file = types::ObservabilityConfig {
        log_level: Some(crate::cli::common::LogLevel::Error),
        ..Default::default()
    };
    let resolved = resolve_observability(&cli, Some(&file))?;
    assert_eq!(resolved.log_level, crate::cli::common::LogLevel::Trace);
    Ok(())
}

#[test]
fn resolve_observability_uses_file_log_level_when_cli_absent() -> anyhow::Result<()> {
    let cli = empty_observability_args();
    let file = types::ObservabilityConfig {
        log_level: Some(crate::cli::common::LogLevel::Debug),
        ..Default::default()
    };
    let resolved = resolve_observability(&cli, Some(&file))?;
    assert_eq!(resolved.log_level, crate::cli::common::LogLevel::Debug);
    Ok(())
}

#[test]
fn resolve_observability_default_metrics_port_when_unset() -> anyhow::Result<()> {
    let cli = empty_observability_args();
    let resolved = resolve_observability(&cli, None)?;
    assert_eq!(resolved.metrics_port, DEFAULT_METRICS_PORT);
    Ok(())
}

#[test]
fn resolve_observability_cli_metrics_port_overrides_file() -> anyhow::Result<()> {
    let mut cli = empty_observability_args();
    cli.metrics_port = Some(8888);
    let file = types::ObservabilityConfig {
        metrics_port: Some(7777),
        ..Default::default()
    };
    let resolved = resolve_observability(&cli, Some(&file))?;
    assert_eq!(resolved.metrics_port, 8888);
    Ok(())
}

// ---- end-to-end resolve_config: file path, default lookup, e2e -------

fn write_minimal_toml(dir: &TempDir, body: &str) -> anyhow::Result<PathBuf> {
    let path = dir.path().join("node.toml");
    std::fs::write(&path, body)?;
    Ok(path)
}

fn complete_toml_body() -> &'static str {
    r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
"#
}

fn blockchain_toml_body(
    rpc_url: &str,
    payment_pool_address: &str,
    capacity_bond_address: &str,
    rpc_watchdog_interval_sec: Option<u64>,
) -> String {
    let watchdog = rpc_watchdog_interval_sec
        .map(|value| format!("rpc_watchdog_interval_sec = {value}\n"))
        .unwrap_or_default();
    format!(
        r#"
[blockchain]
rpc_url = "{rpc_url}"
payment_pool_address = "{payment_pool_address}"
capacity_bond_address = "{capacity_bond_address}"
{watchdog}"#
    )
}

/// Build a minimal `RunArgs` that, combined with `complete_toml_body`,
/// produces a successfully-resolving config.  `data_dir` points at a
/// tempdir holding a fake `keystore.json`, which lets `resolve_blockchain`
/// validate the keystore without touching `$HOME`.
fn run_args_with_data_dir(data_dir: &Path) -> RunArgs {
    let mut args = empty_run_args();
    args.identity.data_dir = Some(data_dir.to_path_buf());
    args
}

#[test]
fn resolve_config_end_to_end_three_layer_merge() -> anyhow::Result<()> {
    // CLI > file > default exercised together: TOML supplies blockchain
    // required fields; CLI overrides the bind port; defaults fill
    // metrics_port + admin_port.
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, complete_toml_body())?;
    let mut args = run_args_with_data_dir(dir.path());
    args.network.bind_port = Some(31_337);

    let (resolved, _notices) = resolve_config(Some(&path), &args)?;

    // CLI value wins for bind_port.
    assert_eq!(resolved.network.bind_port, 31_337);
    // File supplied required blockchain values.
    assert!(
        resolved
            .blockchain
            .rpc_url
            .starts_with("https://example/rpc")
    );
    // Defaults fill in.
    assert_eq!(resolved.observability.metrics_port, DEFAULT_METRICS_PORT);
    assert_eq!(resolved.observability.admin_port, Some(DEFAULT_ADMIN_PORT));
    assert_eq!(resolved.cache.cache_size_mb, DEFAULT_CACHE_SIZE_MB);
    assert_eq!(resolved.payment.rate_per_mb, DEFAULT_RATE_PER_MB);
    Ok(())
}

#[test]
fn resolve_config_blockchain_override_layer_beats_file_for_required_fields() -> anyhow::Result<()> {
    // Env vars and CLI flags both populate the same top `RunArgs` layer;
    // `run_subcommand_args_are_wired_to_decdn_env_vars` pins the env
    // mapping, while this test pins that the populated override layer wins
    // for every required blockchain field in one end-to-end resolve.
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(
        &dir,
        &blockchain_toml_body(
            "https://file.example/rpc",
            ALT_ADDR_1,
            ALT_ADDR_2,
            Some(DEFAULT_RPC_WATCHDOG_INTERVAL_SEC),
        ),
    )?;
    let mut args = run_args_with_data_dir(dir.path());
    args.blockchain.rpc_url = Some("https://override.example/rpc".to_string());
    args.blockchain.payment_pool_address = Some(GOOD_ADDR.to_string());
    args.blockchain.capacity_bond_address = Some(ALT_ADDR_3.to_string());

    let (resolved, _notices) = resolve_config(Some(&path), &args)?;

    assert!(
        resolved
            .blockchain
            .rpc_url
            .starts_with("https://override.example/rpc"),
        "override-layer rpc_url should win, got {}",
        resolved.blockchain.rpc_url,
    );
    assert_eq!(resolved.blockchain.payment_pool_address, GOOD_ADDR);
    assert_eq!(resolved.blockchain.capacity_bond_address, ALT_ADDR_3);
    Ok(())
}

#[test]
fn resolve_config_accepts_file_only_blockchain_fields_at_watchdog_min() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(
        &dir,
        &blockchain_toml_body(
            "https://file-only.example/rpc",
            ALT_ADDR_1,
            ALT_ADDR_2,
            Some(MIN_RPC_WATCHDOG_INTERVAL_SEC),
        ),
    )?;
    let args = run_args_with_data_dir(dir.path());

    let (resolved, _notices) = resolve_config(Some(&path), &args)?;

    assert!(
        resolved
            .blockchain
            .rpc_url
            .starts_with("https://file-only.example/rpc"),
        "file-only rpc_url should be used, got {}",
        resolved.blockchain.rpc_url,
    );
    assert_eq!(resolved.blockchain.payment_pool_address, ALT_ADDR_1);
    assert_eq!(resolved.blockchain.capacity_bond_address, ALT_ADDR_2);
    assert_eq!(
        resolved.blockchain.rpc_watchdog_interval_sec,
        MIN_RPC_WATCHDOG_INTERVAL_SEC
    );
    Ok(())
}

#[test]
fn resolve_config_rejects_file_only_blockchain_watchdog_below_minimum() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(
        &dir,
        &blockchain_toml_body(
            "https://file-only.example/rpc",
            ALT_ADDR_1,
            ALT_ADDR_2,
            Some(MIN_RPC_WATCHDOG_INTERVAL_SEC - 1),
        ),
    )?;
    let args = run_args_with_data_dir(dir.path());

    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected resolve_config to reject a too-small watchdog interval");
    };
    let msg = format!("{err:#}");
    let expected_min = format!("minimum {MIN_RPC_WATCHDOG_INTERVAL_SEC}s");
    assert!(
        msg.contains("blockchain.rpc_watchdog_interval_sec") && msg.contains(&expected_min),
        "error should mention the watchdog field and floor: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_errors_when_bind_port_equals_metrics_port() -> anyhow::Result<()> {
    // Drive validate_port_layout through resolve_config end-to-end —
    // separate from the helper-level coverage above.
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, complete_toml_body())?;
    let mut args = run_args_with_data_dir(dir.path());
    args.network.bind_port = Some(9090);
    args.observability.metrics_port = Some(9090);
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected port-collision error");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("network.bind_port") && msg.contains("metrics_port"),
        "error should name both colliding ports: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_errors_when_a_hash_is_pinned_and_denied() -> anyhow::Result<()> {
    // Drive the pinned∩denied cross-section check through resolve_config
    // end-to-end — the helper-level tests above call the `#[cfg(test)]` shim,
    // so this is what guards the `ensure_no_hash_pinned_and_denied_into` wiring
    // line (deleting it would leave those shim tests green).
    let shared = "cd".repeat(32);
    let body = format!(
        r#"
[identity]
region = "US"

[blockchain]
rpc_url = "https://example/rpc"
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[cache]
pinned_hashes = ["{shared}"]

[content]
denied_hashes = ["{shared}"]
"#
    );
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, &body)?;
    let args = run_args_with_data_dir(dir.path());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected resolve_config to reject a pinned+denied hash");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains(&shared) && msg.contains("content.denied_hashes"),
        "error should name the colliding hash and field: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_aggregates_all_problems_in_one_pass() -> anyhow::Result<()> {
    // Four independent problems across three sections (bad region,
    // missing rpc_url, max_blob >= cache, rate_per_mb = 0) must all
    // surface in a single error so the operator fixes them in one
    // edit cycle.
    let body = r#"
[identity]
region = "USA"

[blockchain]
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[cache]
cache_size_mb = 100
max_blob_size_mb = 500

[payment]
rate_per_mb = 0
"#;
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, body)?;
    let args = run_args_with_data_dir(dir.path());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected resolve_config to fail with multiple problems");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("configuration has 4 problem(s):"),
        "expected aggregated header with count: {msg}"
    );
    for needle in [
        "identity.region",
        "rpc_url",
        "max_blob_size_mb",
        "rate_per_mb",
    ] {
        assert!(
            msg.contains(needle),
            "aggregated error missing {needle:?}: {msg}"
        );
    }
    Ok(())
}

#[test]
fn resolve_config_errors_on_malformed_relay_url() -> anyhow::Result<()> {
    // Drive the #818 relay-URL parse check through resolve_config
    // end-to-end: a malformed `network.relay_urls` entry now fails
    // `decdn config validate` (which calls resolve_config) up front,
    // instead of only at node bring-up.
    let body = r#"
[network]
relay_urls = ["https://ok.example", "not a url"]

[blockchain]
rpc_url = "https://example/rpc"
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

"#;
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, body)?;
    let args = run_args_with_data_dir(dir.path());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected resolve_config to reject a malformed relay URL");
    };
    let msg = format!("{err:#}");
    // Only the bad entry (index 1) is named, not the well-formed one.
    assert!(
        msg.contains("network.relay_urls[1]") && msg.contains("not a url"),
        "error should name the malformed relay entry: {msg}"
    );
    assert!(
        !msg.contains("network.relay_urls[0]"),
        "the well-formed relay entry must not be reported: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_missing_rpc_url_emits_no_parse_cascade() -> anyhow::Result<()> {
    // Cascade guard: a missing `rpc_url` records exactly the
    // "missing required option" problem and skips the URL-parse +
    // scheme checks (a synthesized placeholder must not also emit
    // "is not a valid URL").
    let body = r#"
[blockchain]
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
"#;
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, body)?;
    let args = run_args_with_data_dir(dir.path());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected resolve_config to fail on missing rpc_url");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("configuration has 1 problem(s):"),
        "missing rpc_url must be the only problem (no parse cascade): {msg}"
    );
    assert!(
        msg.contains("missing required option: --rpc-url"),
        "expected the missing-option message: {msg}"
    );
    assert!(
        !msg.contains("is not a valid URL"),
        "URL-parse cascade must be suppressed: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_malformed_slash_judge_emits_no_zero_address_cascade() -> anyhow::Result<()> {
    // Cascade guard: a slash_judge_address that fails to parse
    // records the parse/checksum problem and skips the zero-address
    // check (the empty placeholder must not also emit "must not be
    // the zero address"). The address is set via CLI args because
    // `empty_blockchain_args` defaults `slash_judge_address` to a
    // valid one (CLI > file), so a TOML value would be ignored.
    let body = r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

"#;
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, body)?;
    let mut args = run_args_with_data_dir(dir.path());
    args.blockchain.slash_judge_address = Some("0xnothex".to_string());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected resolve_config to fail on bad slash_judge_address");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("configuration has 1 problem(s):"),
        "bad slash_judge must be the only problem (no zero-addr cascade): {msg}"
    );
    assert!(
        msg.contains("slash_judge_address"),
        "error should name slash_judge_address: {msg}"
    );
    assert!(
        !msg.contains("must not be the zero address"),
        "zero-address cascade must be suppressed: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_missing_home_dir_emits_no_keystore_cascade() -> anyhow::Result<()> {
    // Cascade guard for `IDENTITY_DATA_DIR`: when `data_dir` cannot be
    // resolved (no CLI/file path and `dirs::home_dir()` returns None),
    // `resolve_identity_into` records the `identity.data_dir` problem
    // and stamps a `/nonexistent` placeholder. The downstream
    // `eth_keystore` existence check (which would otherwise fail on
    // `/nonexistent/keystore.json`) must be suppressed via
    // `data_dir_valid` so the operator sees the real problem rather
    // than a stack of cascading filesystem errors.
    let body = r#"
[blockchain]
rpc_url = "https://example/rpc"
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
"#;
    let dir = TempDir::new()?;
    let path = write_minimal_toml(&dir, body)?;
    // `empty_run_args` leaves `identity.data_dir = None`, so resolution
    // falls through to `default_data_dir()`; the override forces
    // `dirs::home_dir()` to None, triggering the placeholder path.
    let args = empty_run_args();
    let err = common::test_support::with_home_override(None, || resolve_config(Some(&path), &args))
        .expect_err("expected resolve_config to fail with data_dir problem");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("configuration has 1 problem(s):"),
        "data_dir failure must be the only problem (no keystore cascade): {msg}"
    );
    assert!(
        msg.contains("identity.data_dir"),
        "expected identity.data_dir problem: {msg}"
    );
    assert!(
        !msg.contains("eth_keystore"),
        "keystore cascade must be suppressed when data_dir is invalid: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_aggregates_cross_and_intra_section_problems() -> anyhow::Result<()> {
    // Cross-section + intra-section accumulation compose with a
    // correct count: bad region (1) + two malformed cache.origins
    // (2) + rate_per_mb=0 (1) = 4.
    let body = r#"
[identity]
region = "USA"

[blockchain]
rpc_url = "https://example/rpc"
payment_pool_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
capacity_bond_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
slash_judge_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

[[cache.origins]]
kind = "http"
url = ""

[[cache.origins]]
kind = "http"
url = "https://good.example/"

[[cache.origins]]
kind = "fs"
path = ""

[payment]
rate_per_mb = 0
"#;
    let dir = data_dir_with_keystore()?;
    let path = write_minimal_toml(&dir, body)?;
    let args = run_args_with_data_dir(dir.path());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected resolve_config to fail with 4 problems");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("configuration has 4 problem(s):"),
        "expected exactly 4 problems: {msg}"
    );
    for needle in [
        "identity.region",
        "cache.origins[0]",
        "cache.origins[2]",
        "rate_per_mb",
    ] {
        assert!(
            msg.contains(needle),
            "aggregated error missing {needle:?}: {msg}"
        );
    }
    assert!(
        !msg.contains("cache.origins[1]"),
        "the valid origin must not be reported: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_errors_on_flat_origin_url_field() -> anyhow::Result<()> {
    // A flat `origin_url` field directly under `[cache]` is rejected
    // by `deny_unknown_fields`: origins live under the tagged
    // `[cache.origin]` table, so a flat field must get a clear
    // "unknown field" error at config load instead of silently
    // dropping the origin and missing every cache pull.
    let dir = data_dir_with_keystore()?;
    let toml_body = format!(
        "{}\n\n[cache]\norigin_url = \"https://origin.example/\"\n",
        complete_toml_body()
    );
    let path = write_minimal_toml(&dir, &toml_body)?;
    let args = run_args_with_data_dir(dir.path());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected unknown-field error for a flat origin_url");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("origin_url") && msg.contains("unknown field"),
        "error should call out the flat `origin_url` key: {msg}"
    );
    Ok(())
}

#[test]
fn resolve_config_errors_on_flat_origin_path_field() -> anyhow::Result<()> {
    // Sibling of the above: a flat `origin_path` field directly under
    // `[cache]` is rejected by `deny_unknown_fields`.
    let dir = data_dir_with_keystore()?;
    let toml_body = format!(
        "{}\n\n[cache]\norigin_path = \"/var/cache/decdn/origin\"\n",
        complete_toml_body()
    );
    let path = write_minimal_toml(&dir, &toml_body)?;
    let args = run_args_with_data_dir(dir.path());
    let Err(err) = resolve_config(Some(&path), &args) else {
        anyhow::bail!("expected unknown-field error for a flat origin_path");
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("origin_path") && msg.contains("unknown field"),
        "error should call out the flat `origin_path` key: {msg}"
    );
    Ok(())
}

#[test]
fn load_file_config_returns_default_when_path_is_none_and_default_absent() -> anyhow::Result<()> {
    // Implicit-path branch of load_file_config: when no explicit
    // path is given and the documented default location does not
    // exist, resolution must fall back to FileConfig::default() —
    // operators running `decdn` without a config file rely on this.
    //
    // Hermetic: pin the home directory to a fresh TempDir so the
    // default config path (`<home>/.decdn/node.toml`) is guaranteed
    // absent regardless of the test host's real `~/.decdn/`.
    let home_dir = TempDir::new()?;
    let cfg =
        common::test_support::with_home_override(Some(home_dir.path()), || load_file_config(None))?;
    // FileConfig::default() leaves every section as None.
    assert!(cfg.identity.is_none());
    assert!(cfg.network.is_none());
    assert!(cfg.blockchain.is_none());
    assert!(cfg.cache.is_none());
    assert!(cfg.payment.is_none());
    assert!(cfg.observability.is_none());
    Ok(())
}

#[test]
fn load_file_config_returns_default_when_home_unavailable() -> anyhow::Result<()> {
    // Sibling of the above: when `dirs::home_dir()` is None (e.g.
    // minimal containers) `default_config_path()` is None, and the
    // implicit-path branch must still fall back to defaults rather
    // than erroring.
    let cfg = common::test_support::with_home_override(None, || load_file_config(None))?;
    assert!(cfg.identity.is_none());
    assert!(cfg.network.is_none());
    assert!(cfg.blockchain.is_none());
    assert!(cfg.cache.is_none());
    assert!(cfg.payment.is_none());
    assert!(cfg.observability.is_none());
    Ok(())
}

#[test]
fn load_file_config_errors_when_explicit_path_missing() {
    // Sibling of the implicit-path test above: an *explicit* missing
    // path must error rather than silently fall through to defaults.
    let dir = TempDir::new().expect("tempdir");
    let bogus = dir.path().join("does-not-exist.toml");
    let err = load_file_config(Some(&bogus)).expect_err("explicit missing path should error");
    let msg = err.to_string();
    assert!(
        msg.contains("does-not-exist.toml"),
        "error should name the missing file: {msg}"
    );
}

#[test]
fn load_file_config_reads_and_parses_explicit_path() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("node.toml");
    std::fs::write(
        &path,
        r"
[network]
bind_port = 12345
",
    )?;
    let cfg = load_file_config(Some(&path))?;
    let bind = cfg
        .network
        .as_ref()
        .and_then(|n| n.bind_port)
        .ok_or_else(|| anyhow::anyhow!("expected network.bind_port to deserialise"))?;
    assert_eq!(bind, 12345);
    Ok(())
}

#[test]
fn load_file_config_errors_on_bad_toml() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "this is not = valid = toml = at = all").expect("write");
    let err = load_file_config(Some(&path)).expect_err("invalid TOML should fail to parse");
    let msg = err.to_string();
    assert!(
        msg.contains("failed to parse config file"),
        "error should describe the parse failure: {msg}"
    );
}

// --- resolve_security ----------------------------------------------------

fn sec_with(mutate: impl FnOnce(&mut types::SecurityConfig)) -> types::SecurityConfig {
    let mut s = types::SecurityConfig::default();
    mutate(&mut s);
    s
}

#[test]
fn resolve_security_populates_defaults_when_absent() {
    let resolved = resolve_security(None).expect("defaults must be valid");
    assert_eq!(
        resolved.max_concurrent_handlers,
        DEFAULT_MAX_CONCURRENT_HANDLERS
    );
    assert!(
        (resolved.per_source_rate_per_sec - DEFAULT_PER_SOURCE_RATE_PER_SEC).abs() < f64::EPSILON
    );
    assert_eq!(resolved.per_source_burst, DEFAULT_PER_SOURCE_BURST);
    assert_eq!(resolved.max_tracked_sources, DEFAULT_MAX_TRACKED_SOURCES);
}

#[test]
fn resolve_security_accepts_zero_max_concurrent_handlers_as_disabled() {
    let s = sec_with(|s| s.max_concurrent_handlers = Some(0));
    let resolved = resolve_security(Some(&s)).expect("0 disables the global cap");
    assert_eq!(resolved.max_concurrent_handlers, 0);
}

#[test]
fn resolve_security_rejects_non_finite_or_negative_per_source_rate() {
    // 0.0 is now valid (disabled); only NaN, ±inf, and strictly-negative are rejected.
    for bad in [-1.0_f64, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let s = sec_with(|s| s.per_source_rate_per_sec = Some(bad));
        assert!(
            resolve_security(Some(&s)).is_err(),
            "per_source_rate_per_sec={bad} should be rejected"
        );
    }
}

#[test]
fn resolve_security_accepts_zero_per_source_pair_as_disabled() {
    // rate=0 + burst=0 disables the per-source layer.
    let s = sec_with(|s| {
        s.per_source_rate_per_sec = Some(0.0);
        s.per_source_burst = Some(0);
    });
    let resolved = resolve_security(Some(&s)).expect("0/0 disables per-source");
    assert_eq!(resolved.per_source_burst, 0);
}

#[test]
fn resolve_security_rejects_zero_per_source_burst_with_positive_rate() {
    // rate>0 with burst=0 is the deny-all corner; reject it.
    let s = sec_with(|s| {
        s.per_source_rate_per_sec = Some(10.0);
        s.per_source_burst = Some(0);
    });
    let err = resolve_security(Some(&s)).expect_err("rate>0+burst=0 must reject");
    assert!(format!("{err:#}").contains("per_source_burst"));
}

#[test]
fn resolve_security_accepts_zero_max_tracked_sources_as_unbounded() {
    let s = sec_with(|s| s.max_tracked_sources = Some(0));
    let resolved = resolve_security(Some(&s)).expect("0 makes the map unbounded");
    assert_eq!(resolved.max_tracked_sources, 0);
}

#[test]
fn resolve_security_partial_override_keeps_other_defaults() {
    let s = sec_with(|s| s.max_concurrent_handlers = Some(512));
    let resolved = resolve_security(Some(&s)).expect("valid override");
    assert_eq!(resolved.max_concurrent_handlers, 512);
    assert_eq!(resolved.per_source_burst, DEFAULT_PER_SOURCE_BURST);
}

// Per-field merge coverage (#438). The four `[security]` fields all
// resolve through the same file-vs-default path; one happy-path test
// per field plus an "absent => default" companion documents the
// contract for each field individually so a regression that
// accidentally applies the wrong default doesn't slip past the
// existing aggregate `_populates_defaults_when_absent` assertion.
//
// There is no `[security]` CLI surface — `resolve_security` takes
// only the file argument. Operators tune these fields by editing
// the config file (with hot reload via SIGHUP). Per-field CLI
// overrides could land later without changing the test surface
// here.

#[test]
fn resolve_security_max_concurrent_handlers_file_override() {
    let s = sec_with(|s| s.max_concurrent_handlers = Some(1024));
    let resolved = resolve_security(Some(&s)).expect("valid override");
    assert_eq!(resolved.max_concurrent_handlers, 1024);
}

#[test]
fn resolve_security_max_concurrent_handlers_default_when_field_absent() {
    // Other fields populated, this one absent — companion to the
    // aggregate-defaults test, isolating this single field.
    let s = sec_with(|s| {
        s.per_source_rate_per_sec = Some(50.0);
        s.per_source_burst = Some(100);
        s.max_tracked_sources = Some(2048);
    });
    let resolved = resolve_security(Some(&s)).expect("valid partial config");
    assert_eq!(
        resolved.max_concurrent_handlers,
        DEFAULT_MAX_CONCURRENT_HANDLERS
    );
}

#[test]
fn resolve_security_per_source_rate_file_override() {
    let s = sec_with(|s| s.per_source_rate_per_sec = Some(42.5));
    let resolved = resolve_security(Some(&s)).expect("valid override");
    assert!((resolved.per_source_rate_per_sec - 42.5).abs() < f64::EPSILON);
}

#[test]
fn resolve_security_per_source_rate_default_when_field_absent() {
    let s = sec_with(|s| s.max_concurrent_handlers = Some(128));
    let resolved = resolve_security(Some(&s)).expect("valid partial config");
    assert!(
        (resolved.per_source_rate_per_sec - DEFAULT_PER_SOURCE_RATE_PER_SEC).abs() < f64::EPSILON
    );
}

#[test]
fn resolve_security_per_source_burst_file_override() {
    let s = sec_with(|s| {
        s.per_source_rate_per_sec = Some(50.0);
        s.per_source_burst = Some(500);
    });
    let resolved = resolve_security(Some(&s)).expect("valid override");
    assert_eq!(resolved.per_source_burst, 500);
}

#[test]
fn resolve_security_per_source_burst_default_when_field_absent() {
    let s = sec_with(|s| s.per_source_rate_per_sec = Some(50.0));
    let resolved = resolve_security(Some(&s)).expect("valid partial config");
    assert_eq!(resolved.per_source_burst, DEFAULT_PER_SOURCE_BURST);
}

#[test]
fn resolve_security_max_tracked_sources_file_override() {
    let s = sec_with(|s| s.max_tracked_sources = Some(8192));
    let resolved = resolve_security(Some(&s)).expect("valid override");
    assert_eq!(resolved.max_tracked_sources, 8192);
}

#[test]
fn resolve_security_max_tracked_sources_default_when_field_absent() {
    let s = sec_with(|s| s.max_concurrent_handlers = Some(128));
    let resolved = resolve_security(Some(&s)).expect("valid partial config");
    assert_eq!(resolved.max_tracked_sources, DEFAULT_MAX_TRACKED_SOURCES);
}

/// Independent fields don't bleed: setting one to a non-default
/// value must leave the others at their defaults. Catches a
/// regression that copy-pasted `s.max_concurrent_handlers` into
/// the wrong field's `unwrap_or(DEFAULT_*)` arm.
#[test]
fn resolve_security_field_overrides_are_independent() {
    let s = sec_with(|s| s.per_source_burst = Some(777));
    let resolved = resolve_security(Some(&s)).expect("valid partial config");
    assert_eq!(resolved.per_source_burst, 777);
    assert_eq!(
        resolved.max_concurrent_handlers,
        DEFAULT_MAX_CONCURRENT_HANDLERS
    );
    assert!(
        (resolved.per_source_rate_per_sec - DEFAULT_PER_SOURCE_RATE_PER_SEC).abs() < f64::EPSILON
    );
    assert_eq!(resolved.max_tracked_sources, DEFAULT_MAX_TRACKED_SOURCES);
}

// --- resolve_load_shed ----------------------------------------------------

#[test]
fn load_shed_defaults_are_resource_pressure() {
    let r = resolve_load_shed(None).expect("defaults resolve");
    assert_eq!(r.policy, LoadShedPolicyKind::ResourcePressure);
    assert_eq!(r.egress_budget_mbps, DEFAULT_LOAD_SHED_EGRESS_BUDGET_MBPS);
    assert!(r.max_concurrent_serves_high >= r.max_concurrent_serves_low);
}

#[test]
fn load_shed_rejects_high_below_low() {
    let raw = types::LoadShedConfig {
        max_concurrent_serves_high: Some(10),
        max_concurrent_serves_low: Some(20),
        ..Default::default()
    };
    assert!(
        resolve_load_shed(Some(&raw)).is_err(),
        "high < low must be a config error"
    );
}

#[test]
fn load_shed_parses_always_admit() {
    let raw = types::LoadShedConfig {
        policy: Some("always-admit".to_string()),
        ..Default::default()
    };
    assert_eq!(
        resolve_load_shed(Some(&raw)).expect("parse").policy,
        LoadShedPolicyKind::AlwaysAdmit
    );
}

// --- resolve_dht: keyspace caps (#645) -----------------------------------
//
// Mirrors the `resolve_security_max_tracked_sources_*` triple: each of
// the two `[dht.rate_limit]` knobs gets a file-override, a
// default-when-absent, and a zero-as-unbounded test. A typo of the
// shape `unwrap_or(0)` instead of `unwrap_or(DEFAULT_DHT_MAX_TRACKED_PER_*)`
// would silently re-introduce the unbounded-keyspace DoS these caps close —
// these tests are the resolver-layer regression guard.

fn dht_rl_with(mutate: impl FnOnce(&mut types::DhtRateLimitConfig)) -> types::DhtConfig {
    let mut r = types::DhtRateLimitConfig::default();
    mutate(&mut r);
    types::DhtConfig {
        rate_limit: Some(r),
    }
}

#[test]
fn resolve_dht_absent_yields_adr022_defaults() {
    // No `[dht.rate_limit]` section at all => every ADR 022 default.
    // Counterpart to `resolve_probe_absent_yields_adr005_defaults`: the six
    // rate/burst rows of `resolve_dht_into` are otherwise unpinned, so a
    // wrong `DEFAULT_DHT_*` on the right-hand side of any `unwrap_or` ships
    // silently. Rates and bursts are spelled as literals on purpose —
    // asserting against the same constant the resolver reads would pin
    // nothing. `max_tracked_*` has no ADR literal (it is a #645
    // implementation cap), so those two go through the constants.
    let resolved = resolve_dht(None).expect("absent section is valid");
    assert!((resolved.per_peer_rate_per_sec - 20.0).abs() < f64::EPSILON);
    assert_eq!(resolved.per_peer_burst, 40);
    assert!((resolved.per_ip_rate_per_sec - 100.0).abs() < f64::EPSILON);
    assert_eq!(resolved.per_ip_burst, 200);
    assert!((resolved.global_rate_per_sec - 1000.0).abs() < f64::EPSILON);
    assert_eq!(resolved.global_burst, 2000);
    assert_eq!(resolved.max_tracked_per_ip, DEFAULT_DHT_MAX_TRACKED_PER_IP);
    assert_eq!(
        resolved.max_tracked_per_peer,
        DEFAULT_DHT_MAX_TRACKED_PER_PEER
    );
}

/// `ResolvedDht::default()` must agree with what the resolver produces for
/// an absent section.
///
/// The two are independent copies of the same eight values —
/// `DEFAULT_DHT_*` is what production resolves through, while
/// `ResolvedDht::default()` is what hand-built `ResolvedConfig` fixtures
/// across `node`, `cli`, and the e2e suite use. Nothing in the type system
/// ties them together, so drift would leave every one of those fixtures
/// exercising a configuration the resolver never emits.
#[test]
fn resolved_dht_default_matches_resolver() {
    let resolved = resolve_dht(None).expect("absent section is valid");
    let hand = ResolvedDht::default();
    assert!((resolved.per_peer_rate_per_sec - hand.per_peer_rate_per_sec).abs() < f64::EPSILON);
    assert_eq!(resolved.per_peer_burst, hand.per_peer_burst);
    assert!((resolved.per_ip_rate_per_sec - hand.per_ip_rate_per_sec).abs() < f64::EPSILON);
    assert_eq!(resolved.per_ip_burst, hand.per_ip_burst);
    assert!((resolved.global_rate_per_sec - hand.global_rate_per_sec).abs() < f64::EPSILON);
    assert_eq!(resolved.global_burst, hand.global_burst);
    assert_eq!(resolved.max_tracked_per_ip, hand.max_tracked_per_ip);
    assert_eq!(resolved.max_tracked_per_peer, hand.max_tracked_per_peer);
}

/// Probe counterpart of `resolved_dht_default_matches_resolver`.
/// `resolve_probe_absent_yields_adr005_defaults` pins `DEFAULT_PROBE_*`
/// against the ADR but says nothing about `ResolvedProbe::default()`, which
/// is the copy the fixtures use.
#[test]
fn resolved_probe_default_matches_resolver() {
    let resolved = resolve_probe(None).expect("absent section is valid");
    let hand = ResolvedProbe::default();
    assert!((resolved.per_peer_rate_per_sec - hand.per_peer_rate_per_sec).abs() < f64::EPSILON);
    assert_eq!(resolved.per_peer_burst, hand.per_peer_burst);
    assert!((resolved.per_ip_rate_per_sec - hand.per_ip_rate_per_sec).abs() < f64::EPSILON);
    assert_eq!(resolved.per_ip_burst, hand.per_ip_burst);
    assert!((resolved.global_rate_per_sec - hand.global_rate_per_sec).abs() < f64::EPSILON);
    assert_eq!(resolved.global_burst, hand.global_burst);
    assert_eq!(resolved.max_tracked_per_ip, hand.max_tracked_per_ip);
    assert_eq!(resolved.max_tracked_per_peer, hand.max_tracked_per_peer);
}

#[test]
fn resolve_dht_max_tracked_per_ip_file_override() {
    let d = dht_rl_with(|r| r.max_tracked_per_ip = Some(8192));
    let resolved = resolve_dht(Some(&d)).expect("valid override");
    assert_eq!(resolved.max_tracked_per_ip, 8192);
}

#[test]
fn resolve_dht_max_tracked_per_ip_default_when_field_absent() {
    let d = dht_rl_with(|r| r.per_peer_burst = Some(50));
    let resolved = resolve_dht(Some(&d)).expect("valid partial config");
    assert_eq!(resolved.max_tracked_per_ip, DEFAULT_DHT_MAX_TRACKED_PER_IP);
}

#[test]
fn resolve_dht_accepts_zero_max_tracked_per_ip_as_unbounded() {
    let d = dht_rl_with(|r| r.max_tracked_per_ip = Some(0));
    let resolved = resolve_dht(Some(&d)).expect("0 makes the map unbounded");
    assert_eq!(resolved.max_tracked_per_ip, 0);
}

#[test]
fn resolve_dht_max_tracked_per_peer_file_override() {
    let d = dht_rl_with(|r| r.max_tracked_per_peer = Some(8192));
    let resolved = resolve_dht(Some(&d)).expect("valid override");
    assert_eq!(resolved.max_tracked_per_peer, 8192);
}

#[test]
fn resolve_dht_max_tracked_per_peer_default_when_field_absent() {
    let d = dht_rl_with(|r| r.per_ip_burst = Some(150));
    let resolved = resolve_dht(Some(&d)).expect("valid partial config");
    assert_eq!(
        resolved.max_tracked_per_peer,
        DEFAULT_DHT_MAX_TRACKED_PER_PEER
    );
}

#[test]
fn resolve_dht_accepts_zero_max_tracked_per_peer_as_unbounded() {
    let d = dht_rl_with(|r| r.max_tracked_per_peer = Some(0));
    let resolved = resolve_dht(Some(&d)).expect("0 makes the map unbounded");
    assert_eq!(resolved.max_tracked_per_peer, 0);
}

// ---- #982: `[probe.rate_limit]` resolver (ADR 005 §Probe rate limiting).
// Mirrors the `[dht.rate_limit]` resolver tests above; the key regression
// guards are the ADR-005 defaults (a tighter per-peer cap than the DHT
// layer) and that the field keys in validation errors say `probe.*`. ----

fn probe_rl_with(mutate: impl FnOnce(&mut types::ProbeRateLimitConfig)) -> types::ProbeConfig {
    let mut r = types::ProbeRateLimitConfig::default();
    mutate(&mut r);
    types::ProbeConfig {
        rate_limit: Some(r),
    }
}

#[test]
fn resolve_probe_absent_yields_adr005_defaults() {
    // No `[probe.rate_limit]` section at all => every ADR 005 default.
    let resolved = resolve_probe(None).expect("absent section is valid");
    assert!((resolved.per_peer_rate_per_sec - 5.0).abs() < f64::EPSILON);
    assert_eq!(resolved.per_peer_burst, 5);
    assert!((resolved.per_ip_rate_per_sec - 50.0).abs() < f64::EPSILON);
    assert_eq!(resolved.per_ip_burst, 200);
    assert!((resolved.global_rate_per_sec - 1000.0).abs() < f64::EPSILON);
    assert_eq!(resolved.global_burst, 2000);
    assert_eq!(
        resolved.max_tracked_per_ip,
        DEFAULT_PROBE_MAX_TRACKED_PER_IP
    );
    assert_eq!(
        resolved.max_tracked_per_peer,
        DEFAULT_PROBE_MAX_TRACKED_PER_PEER
    );
}

#[test]
fn resolve_probe_file_override_applies() {
    let p = probe_rl_with(|r| {
        r.per_peer_rate_per_sec = Some(7.0);
        r.per_peer_burst = Some(9);
    });
    let resolved = resolve_probe(Some(&p)).expect("valid override");
    assert!((resolved.per_peer_rate_per_sec - 7.0).abs() < f64::EPSILON);
    assert_eq!(resolved.per_peer_burst, 9);
    // Untouched fields keep their ADR 005 defaults.
    assert_eq!(resolved.per_ip_burst, 200);
}

#[test]
fn resolve_probe_zero_rate_and_burst_disables_layer() {
    let p = probe_rl_with(|r| {
        r.per_peer_rate_per_sec = Some(0.0);
        r.per_peer_burst = Some(0);
    });
    let resolved = resolve_probe(Some(&p)).expect("0/0 disables the per-peer layer");
    assert!(resolved.per_peer_rate_per_sec.abs() < f64::EPSILON);
    assert_eq!(resolved.per_peer_burst, 0);
}

#[test]
fn resolve_probe_rate_positive_with_zero_burst_rejects() {
    let p = probe_rl_with(|r| {
        r.per_peer_rate_per_sec = Some(5.0);
        r.per_peer_burst = Some(0);
    });
    let err = resolve_probe(Some(&p)).expect_err("rate>0+burst=0 must reject");
    assert!(format!("{err:#}").contains("probe.rate_limit.per_peer_burst"));
}

#[test]
fn resolve_probe_accepts_zero_max_tracked_as_unbounded() {
    let p = probe_rl_with(|r| {
        r.max_tracked_per_ip = Some(0);
        r.max_tracked_per_peer = Some(0);
    });
    let resolved = resolve_probe(Some(&p)).expect("0 makes the maps unbounded");
    assert_eq!(resolved.max_tracked_per_ip, 0);
    assert_eq!(resolved.max_tracked_per_peer, 0);
}

// -----------------------------------------------------------------
// Resolve-time notices
//
// A resolver runs before any subscriber exists, so the bag is the only
// place a notice is observable — which is what makes "the operator was
// actually told" assertable at all rather than leaving the value itself
// as the only thing a test can check. These are the guards that keep a
// refactor from dropping one.
// -----------------------------------------------------------------

/// Run a bag-threading resolver and hand back only what it recorded.
/// Also asserts the resolver recorded no *problem*. Every value below is a
/// documented escape hatch, so a resolver that started rejecting one would
/// otherwise leave these tests green while the notice channel became
/// unreachable behind a hard error.
fn notices_from<T>(f: impl FnOnce(&mut ConfigDiagnostics) -> T) -> Vec<ConfigNotice> {
    let mut bag = ConfigDiagnostics::new();
    f(&mut bag);
    let notices = bag.take_notices();
    assert_eq!(
        bag.problem_count(),
        0,
        "a notice-triggering value must stay non-fatal: {:#}",
        bag.into_result().unwrap_err()
    );
    notices
}

/// Assert exactly one notice, and return it.
fn only_notice(notices: Vec<ConfigNotice>) -> ConfigNotice {
    assert_eq!(notices.len(), 1, "expected exactly one notice: {notices:?}");
    notices.into_iter().next().expect("one notice")
}

#[test]
fn security_zero_max_tracked_sources_warns_about_the_unbounded_map() {
    let s = sec_with(|s| s.max_tracked_sources = Some(0));
    let notice = only_notice(notices_from(|bag| resolve_security_into(Some(&s), bag)));
    assert_eq!(notice.field, "security.max_tracked_sources");
    assert_eq!(
        notice.level,
        ConfigNoticeLevel::Warn,
        "an unbounded bookkeeping map weakens a safety property"
    );
    assert!(
        notice.message.contains("unbounded"),
        "the operator greps for this word: {}",
        notice.message
    );
}

/// The two disabled-layer notices are `Info`, not `Warn`: switching a rate
/// limit off is a documented opt-out working exactly as configured, and a
/// consumer that gates its exit status on severity must not trip on them.
#[test]
fn security_disabled_layers_are_informational_not_warnings() {
    let s = sec_with(|s| {
        s.max_concurrent_handlers = Some(0);
        s.per_source_rate_per_sec = Some(0.0);
        s.per_source_burst = Some(0);
    });
    let notices = notices_from(|bag| resolve_security_into(Some(&s), bag));
    // The exact slice also pins that `per_source_burst = 0` adds nothing:
    // `rate == 0` short-circuits the rate/burst pairing check, so the
    // rate notice already covers the disabled layer.
    let fields: Vec<&str> = notices.iter().map(|n| n.field.as_str()).collect();
    assert_eq!(
        fields,
        [
            "security.max_concurrent_handlers",
            "security.per_source_rate_per_sec"
        ]
    );
    assert!(notices.iter().all(|n| n.level == ConfigNoticeLevel::Info));
    // Discriminating substrings, so swapping the two bodies fails here.
    let messages: Vec<&str> = notices.iter().map(|n| n.message.as_str()).collect();
    assert!(messages[0].contains("concurrency cap"), "{messages:?}");
    assert!(
        messages[1].contains("per-source rate-limit"),
        "{messages:?}"
    );
}

#[test]
fn security_at_defaults_records_no_notice() {
    assert!(notices_from(|bag| resolve_security_into(None, bag)).is_empty());
}

#[test]
fn dht_zero_max_tracked_warns_per_ip_and_per_peer() {
    let d = dht_rl_with(|r| {
        r.max_tracked_per_ip = Some(0);
        r.max_tracked_per_peer = Some(0);
    });
    let notices = notices_from(|bag| resolve_dht_into(Some(&d), bag));
    let fields: Vec<&str> = notices.iter().map(|n| n.field.as_str()).collect();
    assert_eq!(
        fields,
        [
            "dht.rate_limit.max_tracked_per_ip",
            "dht.rate_limit.max_tracked_per_peer"
        ]
    );
    assert!(notices.iter().all(|n| n.level == ConfigNoticeLevel::Warn));
    assert!(notices.iter().all(|n| n.message.contains("unbounded")));
    // Per-IP and per-peer are different attacks; a swapped pair must fail.
    let messages: Vec<&str> = notices.iter().map(|n| n.message.as_str()).collect();
    assert!(messages[0].contains("per-IP"), "{messages:?}");
    assert!(messages[1].contains("per-peer"), "{messages:?}");
}

/// The counterpart guard: a default `[dht.rate_limit]` must stay silent.
/// An inverted condition would fire two `Warn`s on every clean config and
/// move `doctor --strict` off zero for every node on the network.
#[test]
fn dht_at_defaults_records_no_notice() {
    assert!(notices_from(|bag| resolve_dht_into(None, bag)).is_empty());
}

/// Mirrors [`dht_at_defaults_records_no_notice`].
#[test]
fn probe_at_defaults_records_no_notice() {
    assert!(notices_from(|bag| resolve_probe_into(None, bag)).is_empty());
}

#[test]
fn probe_zero_max_tracked_warns_per_ip_and_per_peer() {
    let p = probe_rl_with(|r| {
        r.max_tracked_per_ip = Some(0);
        r.max_tracked_per_peer = Some(0);
    });
    let notices = notices_from(|bag| resolve_probe_into(Some(&p), bag));
    let fields: Vec<&str> = notices.iter().map(|n| n.field.as_str()).collect();
    assert_eq!(
        fields,
        [
            "probe.rate_limit.max_tracked_per_ip",
            "probe.rate_limit.max_tracked_per_peer"
        ]
    );
    assert!(notices.iter().all(|n| n.level == ConfigNoticeLevel::Warn));
    assert!(notices.iter().all(|n| n.message.contains("unbounded")));
    let messages: Vec<&str> = notices.iter().map(|n| n.message.as_str()).collect();
    assert!(messages[0].contains("per-IP"), "{messages:?}");
    assert!(messages[1].contains("per-peer"), "{messages:?}");
}

/// #843: a stale exported `DECDN_RELAY_URL` collapses a multi-entry
/// failover list to one entry. Silent, and months later it reads as an
/// unexplained outage — which is exactly why the notice has to be
/// reachable rather than merely emitted.
#[test]
fn network_cli_relay_override_of_a_file_list_warns() {
    let cli = crate::cli::run::NetworkArgs {
        bind_port: None,
        relay_url: Some("https://cli.example".to_string()),
    };
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec![
            "https://a.example".to_string(),
            "https://b.example".to_string(),
        ]),
        discovery: None,
    };
    let notice = only_notice(notices_from(|bag| {
        resolve_network_into(&cli, Some(&file), bag)
    }));
    assert_eq!(notice.field, "network.relay_url");
    assert_eq!(notice.level, ConfigNoticeLevel::Warn);
    assert!(
        notice.message.contains("2-entry") && notice.message.contains("failover is disabled"),
        "the notice must name what was lost: {}",
        notice.message
    );
}

/// No override, no notice — the guard against a notice that fires on every
/// boot and trains the operator to ignore it.
#[test]
fn network_file_relay_list_alone_records_no_notice() {
    let cli = empty_network_args();
    let file = types::NetworkConfig {
        bind_port: None,
        relay_urls: Some(vec!["https://a.example".to_string()]),
        discovery: None,
    };
    assert!(
        notices_from(|bag| resolve_network_into(&cli, Some(&file), bag)).is_empty(),
        "a file-only relay list is the normal configuration"
    );
}

#[test]
fn well_known_port_warns_naming_the_field_the_operator_wrote() {
    let network = ResolvedNetwork {
        bind_port: 80,
        ..resolve_network(&empty_network_args(), None)
    };
    let observability = resolve_observability(&empty_observability_args(), None)
        .expect("observability defaults are valid");
    let notice = only_notice(notices_from(|bag| {
        validate_port_layout_into(&network, &observability, bag);
    }));
    assert_eq!(notice.field, "network.bind_port");
    assert_eq!(notice.level, ConfigNoticeLevel::Warn);
    assert!(notice.message.contains("well-known range"));
}

/// The `(1..1024)` boundary. 1023 is the last privileged port and 1024 the
/// first unprivileged one, so an off-by-one either warns about a port that
/// binds fine or stays silent about one that needs `CAP_NET_BIND_SERVICE`.
/// Port 0 is excluded on purpose: it means "let the OS pick".
#[test]
fn well_known_port_notice_respects_the_range_edges() {
    let observability = resolve_observability(&empty_observability_args(), None)
        .expect("observability defaults are valid");
    let notices_for = |port| {
        let network = ResolvedNetwork {
            bind_port: port,
            ..resolve_network(&empty_network_args(), None)
        };
        notices_from(|bag| validate_port_layout_into(&network, &observability, bag))
    };
    assert_eq!(notices_for(1).len(), 1, "1 is the first privileged port");
    assert_eq!(notices_for(1023).len(), 1, "1023 is still privileged");
    assert!(notices_for(1024).is_empty(), "1024 is unprivileged");
    assert!(
        notices_for(0).is_empty(),
        "0 means the OS picks; warning about it is noise"
    );
}

/// `resolve_config` hands notices back rather than printing them, and
/// drains them from the same bag the problems ride on — so a clean resolve
/// still carries whatever the section resolvers recorded.
#[test]
fn resolve_config_returns_the_notices_its_resolvers_recorded() -> anyhow::Result<()> {
    let dir = data_dir_with_keystore()?;
    let body = format!(
        "{}\n[security]\nmax_tracked_sources = 0\n",
        complete_toml_body()
    );
    let path = write_minimal_toml(&dir, &body)?;
    let args = run_args_with_data_dir(dir.path());

    let (resolved, notices) = resolve_config(Some(&path), &args)?;
    assert_eq!(resolved.security.max_tracked_sources, 0);
    assert!(
        notices.iter().any(
            |n| n.field == "security.max_tracked_sources" && n.level == ConfigNoticeLevel::Warn
        ),
        "the security notice must survive the trip out of resolve_config: {notices:?}"
    );
    Ok(())
}
