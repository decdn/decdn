use super::*;

/// A governance manifest in the shape `DeployProtocol.s.sol` writes.
const GOVERNANCE_MANIFEST: &str = r#"{
  "chainId": 11155111,
  "config": { "minBond": "50000000000000000000000" },
  "contracts": {
    "CapacityBond": "0x0000000000000000000000000000000000000001",
    "CctpSweepReceiver": "0x0000000000000000000000000000000000000002",
    "ContentBlacklist": "0x0000000000000000000000000000000000000003",
    "FeeRouter": "0x0000000000000000000000000000000000000004",
    "OriginAssignment": "0x0000000000000000000000000000000000000005",
    "PublisherRegistry": "0x0000000000000000000000000000000000000006",
    "Token": "0x0000000000000000000000000000000000000009"
  },
  "deployBlock": 100,
  "deployer": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
  "externalDeps": { "usdc": "0x0000000000000000000000000000000000000010" }
}"#;

/// A payment manifest in the shape `DeployPaymentPool.s.sol` writes.
const PAYMENT_MANIFEST: &str = r#"{
  "chainId": 84532,
  "config": { "operatorBps": 6000, "sweepDomain": 0 },
  "contracts": { "PaymentPool": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045" },
  "deployBlock": 200,
  "deployer": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
  "externalDeps": {
    "cctpTokenMessenger": "0x0000000000000000000000000000000000000011",
    "pauser": "0x0000000000000000000000000000000000000012",
    "sweepRecipient": "0x0000000000000000000000000000000000000002",
    "usdc": "0x0000000000000000000000000000000000000013"
  }
}"#;

const FIXTURE_PAYMENT: &[KnownPaymentChain] = &[KnownPaymentChain {
    label: "Base Sepolia",
    chain_id: 84_532,
    public_rpc: "https://sepolia.base.org",
    start_block: 2_000,
    manifest: PAYMENT_MANIFEST,
}];

/// A target built from the fixture manifests, for the template tests that
/// need one while [`KNOWN_CHAINS`] lists none.
pub(crate) static FIXTURE: KnownChain = KnownChain {
    name: "fixture",
    label: "Fixture testnet",
    chain_id: 11_155_111,
    public_rpc: "https://gov.example/rpc",
    l2_start_block: 1_000,
    usdc_faucet: Some("https://faucet.example"),
    cctp_iris_url: "https://iris-api-sandbox.circle.com",
    governance_manifest: GOVERNANCE_MANIFEST,
    payment_chains: FIXTURE_PAYMENT,
};

#[test]
fn the_governance_manifest_yields_the_governance_set() -> anyhow::Result<()> {
    let a = FIXTURE.addresses()?;
    assert_eq!(
        a.capacity_bond,
        "0x0000000000000000000000000000000000000001"
    );
    assert_eq!(
        a.cctp_sweep_receiver,
        "0x0000000000000000000000000000000000000002"
    );
    Ok(())
}

#[test]
fn the_payment_manifest_yields_its_pool_and_token() -> anyhow::Result<()> {
    let chains = FIXTURE.payment_chains()?;
    assert_eq!(
        chains,
        vec![PaymentChainAddresses {
            label: "Base Sepolia",
            chain_id: 84_532,
            public_rpc: "https://sepolia.base.org",
            start_block: 2_000,
            payment_pools: vec!["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045".to_owned()],
            usdc: "0x0000000000000000000000000000000000000013".to_owned(),
        }]
    );
    Ok(())
}

/// A governance manifest carries no `PaymentPool` and needs none; one without
/// the `CctpSweepReceiver` is from before the split and is refused.
#[test]
fn a_governance_manifest_without_the_sweep_receiver_is_refused() {
    let stale = GOVERNANCE_MANIFEST.replace("CctpSweepReceiver", "Removed");
    let err = parse_governance_manifest(&stale, 11_155_111, "fixture")
        .unwrap_err()
        .to_string();
    assert!(err.contains("CctpSweepReceiver"), "{err}");
}

#[test]
fn a_manifest_for_another_chain_is_refused() {
    let err = parse_governance_manifest(GOVERNANCE_MANIFEST, 1, "fixture")
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not match"), "{err}");
}

#[test]
fn a_payment_manifest_without_a_pool_is_refused() {
    static STALE: &[KnownPaymentChain] = &[KnownPaymentChain {
        label: "stale",
        chain_id: 84_532,
        public_rpc: "https://sepolia.base.org",
        start_block: 0,
        manifest: r#"{"chainId": 84532, "contracts": {}, "externalDeps": {"usdc": "0x0000000000000000000000000000000000000013"}}"#,
    }];
    let err = STALE
        .iter()
        .map(parse_payment_manifest)
        .next()
        .expect("one chain")
        .unwrap_err()
        .to_string();
    assert!(err.contains("PaymentPool"), "{err}");
}

#[test]
fn every_known_chain_manifest_parses_and_matches() {
    for chain in KNOWN_CHAINS.iter().chain(std::iter::once(&FIXTURE)) {
        let addrs = chain
            .addresses()
            .expect("known-chain governance manifest must parse");
        // Every baked field must pass the same EIP-55 check the resolver
        // applies to a user-supplied address — a bad-checksum manifest is
        // caught here at CI, not on an operator's first `config validate`.
        for (field, value) in [
            ("capacity_bond", &addrs.capacity_bond),
            ("content_blacklist", &addrs.content_blacklist),
            ("origin_assignment", &addrs.origin_assignment),
            ("publisher_registry", &addrs.publisher_registry),
            ("cctp_sweep_receiver", &addrs.cctp_sweep_receiver),
        ] {
            decdn_common::config::parse_contract_address(field, value)
                .expect("baked contract address must pass the EIP-55 check");
        }
        for payment in chain
            .payment_chains()
            .expect("known-chain payment manifest must parse")
        {
            for pool in payment.payment_pools.iter().chain([&payment.usdc]) {
                decdn_common::config::parse_contract_address("payment_pools", pool)
                    .expect("baked payment address must pass the EIP-55 check");
            }
        }
    }
}

/// `l2_start_block` is an L2 height and the manifest's `deployBlock` is the
/// L1 height the deploy script saw; on Arbitrum the L2 number is the larger
/// by far. A redeploy that copies the manifest's number into the registry
/// would start every scan hundreds of millions of blocks early.
#[test]
fn l2_start_block_is_above_the_manifests_l1_deploy_block() {
    for chain in KNOWN_CHAINS {
        let manifest: serde_json::Value =
            serde_json::from_str(chain.governance_manifest).expect("manifest parses");
        let deploy_block = manifest
            .get("deployBlock")
            .and_then(serde_json::Value::as_u64)
            .expect("manifest carries deployBlock");
        assert!(
            chain.l2_start_block > deploy_block,
            "{}: l2_start_block {} must be the L2 height, above the manifest's L1 deployBlock {}",
            chain.name,
            chain.l2_start_block,
            deploy_block
        );
    }
}

#[test]
fn resolve_none_selector_yields_blank_template() {
    assert!(resolve(Some("none")).unwrap().is_none());
}

#[test]
fn resolve_omitted_selector_with_no_known_chain_is_the_blank_template() {
    static NONE: &[KnownChain] = &[];
    assert!(resolve_in(NONE, None).unwrap().is_none());
}

#[test]
fn resolve_omitted_selector_picks_the_sole_chain() {
    let only = std::slice::from_ref(&FIXTURE);
    let chain = resolve_in(only, None)
        .unwrap()
        .expect("sole chain is selected");
    assert_eq!(chain.name, "fixture");
}

#[test]
fn resolve_unknown_selector_errors() {
    let err = resolve(Some("mainnet")).unwrap_err().to_string();
    assert!(err.contains("unknown --chain `mainnet`"), "{err}");
}
