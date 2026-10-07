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
