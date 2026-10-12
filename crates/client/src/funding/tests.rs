use super::*;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;

/// `cached_pool_token` returns the persisted pool's immutable token without
/// any contract read, and `None` (so the caller reads `usdc()` on-chain) when
/// no pool row exists for the owner yet.
#[test]
fn cached_pool_token_uses_persisted_row_and_skips_rpc() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    // A fresh subpath so the store's `ensure_data_dir` creates it at 0o700;
    // the tempdir root itself is 0o755, which the store rejects.
    let store = RedbBuyerPoolStore::open(&dir.path().join("data"))?;
    let owner = Address::repeat_byte(0x11);
    let token = Address::repeat_byte(0x22);
    let deployment = Deployment {
        chain_id: 421_614,
        payment_pool: Address::repeat_byte(0x9c),
    };

    // No row yet -> None -> caller must read usdc() on the open path.
    assert_eq!(cached_pool_token(&store, owner, deployment)?, None);

    // Persist a pool row for this owner.
    let state = BuyerPoolState::new(
        B256::repeat_byte(0xAB),
        deployment,
        owner,
        token,
        U256::from(1_000u64),
    );
    store.record(&state)?;

    // Row present -> the immutable token comes back with no contract read.
    assert_eq!(cached_pool_token(&store, owner, deployment)?, Some(token));
    // A different owner has no row -> still None.
    assert_eq!(
        cached_pool_token(&store, Address::repeat_byte(0x33), deployment)?,
        None
    );
    // The row is the same, but it names another deployment's `usdc()`.
    // Answering with it would approve and price the wrong token. The
    // address alone does not name a deployment: the same address on
    // another chain is another contract.
    let other_address = Deployment {
        payment_pool: Address::repeat_byte(0xDE),
        ..deployment
    };
    let other_chain = Deployment {
        chain_id: 1,
        ..deployment
    };
    for foreign in [other_address, other_chain] {
        assert_eq!(
            cached_pool_token(&store, owner, foreign)?,
            None,
            "a row from another PaymentPool deployment must not seed the token cache \
             ({foreign:?})"
        );
    }
    Ok(())
}

/// The delegated signer gate accepts the authorized key and rejects any
/// other, naming both addresses so the operator can see the mismatch.
#[test]
fn ensure_delegate_signer_matches_or_rejects() {
    let key = Address::repeat_byte(0xa1);
    assert!(super::ensure_delegate_signer(key, key).is_ok());

    let other = Address::repeat_byte(0xb2);
    let err = super::ensure_delegate_signer(other, key)
        .expect_err("a non-authorized key must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains(&key.to_string()),
        "names the authorized signer: {msg}"
    );
    assert!(
        msg.contains(&other.to_string()),
        "names the loaded key: {msg}"
    );
}
