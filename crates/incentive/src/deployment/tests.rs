use super::*;
use std::assert_matches;

const DEPLOYMENT: Deployment = Deployment {
    chain_id: 0x0102_0304_0506_0708,
    payment_pool: Address::repeat_byte(0x9c),
};

#[test]
fn bytes_round_trip() -> anyhow::Result<()> {
    let bytes = DEPLOYMENT.to_bytes();
    anyhow::ensure!(
        bytes[..8] == [1, 2, 3, 4, 5, 6, 7, 8],
        "chain id is big-endian first"
    );
    anyhow::ensure!(bytes[8..] == [0x9c; 20], "the address follows the chain id");
    anyhow::ensure!(Deployment::from_bytes(&bytes)? == DEPLOYMENT);
    Ok(())
}

#[test]
fn a_wrong_width_is_corrupt() {
    for len in [0, 20, 27, 29] {
        assert_matches!(
            Deployment::from_bytes(&vec![0u8; len]),
            Err(StoreError::Corrupt { .. }),
            "{len} bytes must not decode"
        );
    }
}

#[test]
fn the_voucher_domain_names_the_chain_and_the_contract() {
    let domain = DEPLOYMENT.voucher_domain();
    assert_eq!(
        domain,
        voucher_domain(DEPLOYMENT.chain_id, DEPLOYMENT.payment_pool)
    );
}
