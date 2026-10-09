use alloy::primitives::Address;

use super::*;

fn profile(min: Option<&str>) -> Profile {
    Profile {
        chain_id: 1,
        rpc_url: "https://rpc".into(),
        payment_pool: Address::repeat_byte(1),
        capacity_bond: Address::repeat_byte(2),
        slash_judge: None,
        min_cli_version: min.map(|m| m.parse().unwrap()),
    }
}

#[test]
fn an_older_cli_is_told_to_upgrade() {
    assert!(check_version(&profile(None), "0.1.0").is_ok());
    assert!(check_version(&profile(Some("0.1.0")), "0.1.0").is_ok());
    let err = check_version(&profile(Some("0.2.0")), "0.1.9").unwrap_err();
    assert!(err.to_string().contains("re-run its installer"), "{err}");
}
