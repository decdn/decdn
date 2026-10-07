use super::*;

// All-digit address: valid hex, checksum-neutral (EIP-55 only affects a-f),
// and non-zero — a stand-in for a real deployment.
const NONZERO: &str = "0x1111111111111111111111111111111111111111";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

#[test]
fn parse_nonzero_address_accepts_a_real_address() {
    let addr =
        parse_nonzero_address(NONZERO, "payment_pool_address").expect("a non-zero address parses");
    assert_eq!(addr, NONZERO.parse::<Address>().expect("valid hex"));
}

#[test]
fn parse_nonzero_address_rejects_the_zero_address() {
    let err = parse_nonzero_address(ZERO, "payment_pool_address").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("payment_pool_address"), "labelled: {err}");
    assert!(msg.contains("must not be the zero address"), "{err}");
}

#[test]
fn parse_nonzero_address_reports_malformed_input_with_its_label() {
    let err = parse_nonzero_address("not-an-address", "content_blacklist_address").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("content_blacklist_address"), "labelled: {err}");
    assert!(msg.contains("is not a valid address"), "{err}");
}

#[test]
fn parse_address_accepts_the_zero_address_for_eoa_sites() {
    // The permissive parse must NOT reject zero — EOA/account sites rely on it.
    parse_address(ZERO, "--provider-address").expect("zero is a valid EOA parse");
}
