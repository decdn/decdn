use super::*;

#[test]
fn every_known_chain_manifest_parses_and_matches() {
    for chain in KNOWN_CHAINS {
        // The error from `addresses()` names the chain; `parse_contract_address`
        // errors name the field — so `expect` needs no extra interpolation.
        let addrs = chain
            .addresses()
            .expect("known-chain deployment manifest must parse");
        // Every baked field must pass the same EIP-55 check the resolver
        // applies to a user-supplied address — a bad-checksum manifest is
        // caught here at CI, not on an operator's first `config validate`.
        for (field, value) in [
            ("payment_pool", &addrs.payment_pool),
            ("capacity_bond", &addrs.capacity_bond),
            ("slash_judge", &addrs.slash_judge),
            ("content_blacklist", &addrs.content_blacklist),
            ("origin_assignment", &addrs.origin_assignment),
            ("publisher_registry", &addrs.publisher_registry),
            ("slash_appeal", &addrs.slash_appeal),
            ("usdc", &addrs.usdc),
        ] {
            decdn_common::config::parse_contract_address(field, value)
                .expect("baked contract address must pass the EIP-55 check");
        }
    }
}

/// `l2_start_block` is an L2 height and the manifest's `deployBlock` is the
/// L1 height the deploy script saw; on Arbitrum the L2 number is the larger
/// by far. A redeploy that copies the manifest's number into the registry
/// would start every `--address` scan hundreds of millions of blocks early.
#[test]
fn l2_start_block_is_above_the_manifests_l1_deploy_block() {
    for chain in KNOWN_CHAINS {
        let manifest: serde_json::Value =
            serde_json::from_str(chain.manifest_json).expect("manifest parses");
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

/// The testnet points an unfunded buyer wallet at a USDC faucet (`decdn
/// setup`'s `buyer_usdc` row); the pointer is an https URL.
#[test]
fn testnet_carries_an_https_usdc_faucet() {
    let chain = by_chain_id(421_614).expect("arbitrum-sepolia is known");
    let faucet = chain.usdc_faucet.expect("the testnet has a USDC faucet");
    assert!(faucet.starts_with("https://"), "{faucet}");
}

#[test]
fn resolve_none_selector_yields_blank_template() {
    assert!(resolve(Some("none")).unwrap().is_none());
}

#[test]
fn resolve_omitted_selector_picks_the_sole_chain() {
    // Precondition for the "omit --chain" UX: exactly one chain today.
    assert_eq!(KNOWN_CHAINS.len(), 1);
    let chain = resolve(None).unwrap().expect("sole chain is selected");
    assert_eq!(chain.name, "arbitrum-sepolia");
}

#[test]
fn resolve_unknown_selector_errors_with_known_names() {
    let err = resolve(Some("mainnet")).unwrap_err().to_string();
    assert!(err.contains("arbitrum-sepolia"), "{err}");
}
