use super::*;

#[test]
fn pool_id_matches_packed_keccak() {
    // `keccak256(abi.encodePacked(owner, uint256(nonce)))` — the derivation
    // `PaymentPool.openPool` uses. `expected` is an independent Foundry-produced
    // vector (not recomputed from this Rust code), so a wrong *original* packing
    // — field order, endianness, nonce width — is caught, not just a later
    // refactor. The packed preimage is 20-byte owner ‖ 32-byte big-endian
    // uint256(7); regenerate the expected hash with:
    //   cast keccak 0x0000000000000000000000000000000000000001\
    //                 0000000000000000000000000000000000000000000000000000000000000007
    let owner: Address = "0x0000000000000000000000000000000000000001"
        .parse()
        .unwrap();
    let expected = alloy::primitives::b256!(
        "0xb04aad3ec8e9b0d16a001f5bfe99a4b491a4397ce09795032b68ffd53dd08ee9"
    );
    assert_eq!(pool_id(owner, 7), expected);
}
