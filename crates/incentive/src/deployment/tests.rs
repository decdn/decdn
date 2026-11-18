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
fn pool_account_round_trips() {
    let account = DEPLOYMENT.pool_account();
    assert_eq!(
        account.to_string(),
        format!("eip155:{}:0x{}", DEPLOYMENT.chain_id, "9c".repeat(20))
    );
    assert_eq!(Deployment::from_pool_account(&account), Some(DEPLOYMENT));
}

#[test]
fn a_checksummed_pool_account_maps_to_the_same_deployment() -> anyhow::Result<()> {
    let address: Address = "0x351E5544471e11638b89DF8E3A3299c28E00cEc3".parse()?;
    let account = decdn_protocol::PoolAccount::new(
        "eip155:421614:0x351E5544471e11638b89DF8E3A3299c28E00cEc3",
    )?;
    anyhow::ensure!(
        Deployment::from_pool_account(&account)
            == Some(Deployment {
                chain_id: 421_614,
                payment_pool: address,
            })
    );
    Ok(())
}

#[test]
fn a_non_evm_pool_account_has_no_deployment() -> anyhow::Result<()> {
    for id in [
        "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp:7S3P4HxJpyyigGzodYwHtCxZyUQe9JiBMHyRWXArAaKv",
        "eip155:not-a-number:0x351e5544471e11638b89df8e3a3299c28e00cec3",
        "eip155:1:0x351e",
    ] {
        let account = decdn_protocol::PoolAccount::new(id)?;
        anyhow::ensure!(
            Deployment::from_pool_account(&account).is_none(),
            "{id} must not map to a deployment"
        );
    }
    Ok(())
}

#[test]
fn ordering_follows_the_encoded_bytes() {
    let low = Deployment {
        chain_id: 1,
        payment_pool: Address::repeat_byte(0xff),
    };
    let high = Deployment {
        chain_id: 2,
        payment_pool: Address::repeat_byte(0x00),
    };
    assert!(low < high);
    assert!(low.to_bytes() < high.to_bytes());
}

#[test]
fn the_voucher_domain_names_the_chain_and_the_contract() {
    let domain = DEPLOYMENT.voucher_domain();
    assert_eq!(
        domain,
        voucher_domain(DEPLOYMENT.chain_id, DEPLOYMENT.payment_pool)
    );
}
