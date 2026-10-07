use super::*;

#[test]
fn explicit_multiaddrs_win_over_the_default() {
    let explicit = vec!["/ip4/198.51.100.7/udp/5000/quic-v1".to_string()];
    assert_eq!(
        registration_multiaddrs(&explicit, &chain_ctx::FileConfig::default()),
        explicit
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
