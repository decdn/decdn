use super::*;

#[test]
fn explicit_multiaddrs_win_over_the_default() {
    let explicit = vec!["/ip4/198.51.100.7/udp/5000/quic-v1".to_string()];
    assert_eq!(
        registration_multiaddrs(&explicit, &chain_ctx::FileConfig::default(), None).unwrap(),
        explicit
    );
}

fn file(toml_src: &str) -> chain_ctx::FileConfig {
    toml::from_str(toml_src).unwrap()
}

#[test]
fn registration_multiaddrs_default_to_configured_external_addrs() {
    // Each entry registers on its own port, not on `bind_port`.
    let file = file(
        "[network]\nbind_port = 4433\n\
         external_addrs = [\"8.8.8.8:30443\", \"[2606:4700::1111]:4434\"]\n",
    );
    assert_eq!(
        registration_multiaddrs(&[], &file, None).unwrap(),
        vec![
            "/ip4/8.8.8.8/udp/30443/quic-v1".to_string(),
            "/ip6/2606:4700::1111/udp/4434/quic-v1".to_string(),
        ]
    );
}

#[test]
fn registration_multiaddrs_explicit_skips_a_bad_external_addr() {
    let file = file("[network]\nexternal_addrs = [\"node.example:4433\"]\n");
    let explicit = vec!["/ip4/8.8.8.8/udp/5000/quic-v1".to_string()];
    assert_eq!(
        registration_multiaddrs(&explicit, &file, None).unwrap(),
        explicit
    );
}

#[test]
fn registration_multiaddrs_refuse_a_private_external_addr() {
    let file = file("[network]\nexternal_addrs = [\"8.8.8.8:4433\", \"10.0.0.5:4433\"]\n");
    let msg = format!(
        "{:#}",
        registration_multiaddrs(&[], &file, None).unwrap_err()
    );
    assert!(msg.contains("network.external_addrs[1]"), "{msg}");
    assert!(msg.contains("10.0.0.5"), "{msg}");
}

#[test]
fn registration_multiaddrs_use_the_list_already_parsed() {
    // The file's list is bad, so a result means the parsed list was used and
    // the file was not parsed again.
    let file = file("[network]\nexternal_addrs = [\"10.0.0.5:4433\"]\n");
    let parsed = vec!["8.8.8.8:30443".parse().unwrap()];
    assert_eq!(
        registration_multiaddrs(&[], &file, Some(parsed)).unwrap(),
        vec!["/ip4/8.8.8.8/udp/30443/quic-v1".to_string()]
    );
}

#[test]
fn notes_name_the_config_file_read() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "[network]\nbind_port = 4433\n").unwrap();
    let file = chain_ctx::load_optional_config(Some(&path)).unwrap();
    assert_eq!(file.source(), &chain_ctx::ConfigSource::Read(path.clone()));
    assert_eq!(
        no_external_addrs(file.source()),
        format!("{} sets no network.external_addrs", path.display())
    );
    assert_eq!(
        read_from(file.source()),
        format!(" from {}", path.display())
    );
}

#[test]
fn notes_say_no_config_file_was_found() {
    let path = std::path::PathBuf::from("/nonexistent/.decdn/node.toml");
    let source = chain_ctx::ConfigSource::Missing(path.clone());
    assert_eq!(
        no_external_addrs(&source),
        format!("no config file at {}", path.display())
    );
    assert_eq!(read_from(&source), "");
    assert_eq!(
        no_external_addrs(&chain_ctx::ConfigSource::None),
        "no config file"
    );
}

#[test]
fn default_multiaddrs_cover_each_public_address_on_the_bind_port() {
    let public = ["8.8.8.8".parse().unwrap(), "2a01:4f8::1".parse().unwrap()];
    assert_eq!(
        default_multiaddrs(&public, 4433),
        vec![
            "/ip4/8.8.8.8/udp/4433/quic-v1".to_string(),
            "/ip6/2a01:4f8::1/udp/4433/quic-v1".to_string(),
        ]
    );
    assert!(default_multiaddrs(&[], 4433).is_empty(), "behind NAT: none");
}

fn sample_outcome(tx: Option<B256>) -> RegisterOutcome {
    RegisterOutcome {
        node_id: B256::repeat_byte(0xAB),
        operator: Address::repeat_byte(0xCD),
        chain_id: 31337,
        capacity_bond: Address::repeat_byte(0x01),
        region: "DE".to_string(),
        registration_nonce: 0,
        multiaddr_count: 1,
        ed25519_sig: vec![0x22; 64],
        tx,
    }
}

#[test]
fn dry_run_output_has_signatures() {
    let o = sample_outcome(None);
    let mut buf = Vec::new();
    write_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("submitted=false dry_run=true"), "{s}");
    assert!(s.contains("ed25519_sig=0x2222"), "{s}");
    assert!(s.contains("region=DE"), "{s}");
}

#[test]
fn submitted_output_has_tx() {
    let o = sample_outcome(Some(B256::repeat_byte(0x55)));
    let mut buf = Vec::new();
    write_outcome(&mut buf, &o, false).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("submitted=true tx=0x5555"), "{s}");
}

/// `ensure_multiaddrs_fit` against a mocked provider that serves no
/// responses, so every chain read fails. The size bound itself is
/// `update_multiaddrs::ensure_packed_within`, tested there.
mod fit {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    use super::*;

    /// Run the check for `multiaddrs` against a chain that answers nothing.
    async fn fit(multiaddrs: &[String]) -> anyhow::Result<()> {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let addr = Address::repeat_byte(0xCB);
        let bond = CapacityBond::new(addr, provider);
        ensure_multiaddrs_fit(&bond, addr, multiaddrs).await
    }

    /// Packing runs before the chain read: a read would fail with an
    /// empty-queue error instead of the length-prefix one.
    #[tokio::test]
    async fn an_unpackable_entry_fails_before_the_chain_read() {
        let err = fit(&["a".repeat(usize::from(u16::MAX) + 1)])
            .await
            .expect_err("an entry past the u16 prefix must not proceed");
        assert!(
            format!("{err}").contains("uint16 length prefix"),
            "reports the packing error: {err}"
        );
    }

    #[tokio::test]
    async fn a_failed_read_names_the_ceiling() {
        let err = fit(&["/ip4/203.0.113.10/udp/4433/quic-v1".to_string()])
            .await
            .expect_err("an unreadable ceiling must not pass");
        assert!(
            format!("{err:#}").contains("failed to read maxMultiaddrSize"),
            "names the failed read: {err:#}"
        );
    }
}
