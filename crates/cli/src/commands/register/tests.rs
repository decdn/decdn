use super::*;

#[test]
fn explicit_multiaddrs_win_over_the_default() {
    let explicit = vec!["/ip4/198.51.100.7/udp/5000/quic-v1".to_string()];
    assert_eq!(
        registration_multiaddrs(&explicit, &chain_ctx::FileConfig::default()).unwrap(),
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
        registration_multiaddrs(&[], &file).unwrap(),
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
    assert_eq!(registration_multiaddrs(&explicit, &file).unwrap(), explicit);
}

#[test]
fn registration_multiaddrs_refuse_a_private_external_addr() {
    let file = file("[network]\nexternal_addrs = [\"8.8.8.8:4433\", \"10.0.0.5:4433\"]\n");
    let msg = format!("{:#}", registration_multiaddrs(&[], &file).unwrap_err());
    assert!(msg.contains("network.external_addrs[1]"), "{msg}");
    assert!(msg.contains("10.0.0.5"), "{msg}");
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
        binding_nonce: 0,
        registration_nonce: 0,
        multiaddr_count: 1,
        binding_sig: vec![0x11; 65],
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

/// `ensure_multiaddrs_fit` against a mocked provider. `Asserter` serves
/// `eth_call` responses FIFO, so the queued responses are also an assertion
/// about which chain reads happen.
mod fit {
    use alloy::primitives::U256;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    use super::*;

    /// Run the check for `multiaddrs` against a chain serving `ceiling`, or
    /// serving nothing when `ceiling` is `None`.
    async fn fit(multiaddrs: &[String], ceiling: Option<U256>) -> anyhow::Result<()> {
        let asserter = Asserter::new();
        if let Some(c) = ceiling {
            asserter.push_success(&Bytes::from(c.to_be_bytes::<32>().to_vec()));
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let addr = Address::repeat_byte(0xCB);
        let bond = CapacityBond::new(addr, provider);
        ensure_multiaddrs_fit(&bond, addr, multiaddrs).await
    }

    /// One entry that packs to `packed` bytes (a 2-byte length prefix plus
    /// its body).
    fn entry(packed: usize) -> Vec<String> {
        vec!["a".repeat(packed - 2)]
    }

    #[tokio::test]
    async fn at_the_ceiling_passes() {
        fit(&entry(64), Some(U256::from(64)))
            .await
            .expect("packed == maxMultiaddrSize is within bounds");
    }

    #[tokio::test]
    async fn over_the_ceiling_names_register_node() {
        let err = fit(&entry(65), Some(U256::from(64)))
            .await
            .expect_err("over the ceiling must not proceed");
        let msg = format!("{err}");
        assert!(msg.contains("`registerNode`"), "names the call: {msg}");
        assert!(
            msg.contains("MultiaddrsTooLarge"),
            "names the revert: {msg}"
        );
        assert!(msg.contains("65 bytes"), "reports the packed size: {msg}");
        assert!(msg.contains("64 bytes"), "reports the ceiling: {msg}");
    }

    /// Packing runs before the chain read: nothing is queued, so a read
    /// would fail with an empty-queue error instead of the length-prefix one.
    #[tokio::test]
    async fn an_unpackable_entry_fails_before_the_chain_read() {
        let err = fit(&["a".repeat(usize::from(u16::MAX) + 1)], None)
            .await
            .expect_err("an entry past the u16 prefix must not proceed");
        assert!(
            format!("{err}").contains("uint16 length prefix"),
            "reports the packing error: {err}"
        );
    }

    #[tokio::test]
    async fn a_failed_read_names_the_ceiling() {
        let err = fit(&entry(10), None)
            .await
            .expect_err("an unreadable ceiling must not pass");
        assert!(
            format!("{err:#}").contains("failed to read maxMultiaddrSize"),
            "names the failed read: {err:#}"
        );
    }

    #[tokio::test]
    async fn an_empty_set_passes() {
        fit(&[], Some(U256::from(64)))
            .await
            .expect("an empty set packs to 0 bytes");
    }
}
