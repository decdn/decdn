use super::*;
use alloy::primitives::B256;
use decdn_incentive::StoreError;
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

/// #2468: an owned lane binds its endpoint with the lane key, the key its
/// vouchers sign under and its capability names. The node keys the lane on the
/// binding's signer, so a binding signed by the owner key would name a lane the
/// vouchers never pay on, and the node would refuse every request.
#[tokio::test]
async fn an_owned_lane_binds_its_endpoint_with_the_lane_key() -> anyhow::Result<()> {
    let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ))?
        .bind()
        .await?;
    let store = decdn_incentive::MemoryBuyerPoolStore::new();
    let payer = OwnedPayer::for_store(Arc::new(PrivateKeySigner::random()), &store)?;
    let deployment = Deployment {
        chain_id: 1,
        payment_pool: Address::repeat_byte(0x33),
    };
    let state = BuyerPoolState::new(
        B256::repeat_byte(0x11),
        deployment,
        payer.owner().address(),
        Address::repeat_byte(0x22),
        U256::from(1_000u64),
    );
    let ctx = self_owned_lane_ctx(
        &state,
        &payer,
        &deployment.voucher_domain(),
        Address::repeat_byte(0xaa),
        U256::ZERO,
        U256::ZERO,
    )?;
    let capacity_bond = Address::repeat_byte(0x55);
    let chain = ChainConfig {
        rpc_url: "http://config:8545".to_string(),
        chain_id: deployment.chain_id,
        payment_pool: deployment.payment_pool,
        slash_judge: Address::repeat_byte(0x44),
        capacity_bond: Some(capacity_bond),
    };
    let ctx = attach_client_binding(ctx, &chain, &endpoint)?;

    let lane = payer.lane().address();
    anyhow::ensure!(
        lane != payer.owner().address(),
        "a for_store lane key is the owner"
    );
    let binding = ctx
        .client_binding
        .as_ref()
        .context("a configured capacity bond attaches a binding")?;
    let recovered = decdn_incentive::verify_binding(
        B256::from(*endpoint.id().as_bytes()),
        decdn_incentive::EPHEMERAL_BINDING_NONCE,
        &binding.binding_signature,
        &bind_node_id_domain(chain.chain_id, capacity_bond),
    )?;
    anyhow::ensure!(
        recovered == lane,
        "the binding recovers to {recovered}, not the lane key"
    );
    anyhow::ensure!(Address::from(binding.ethereum_address) == lane);
    anyhow::ensure!(ctx.client_signer.address() == lane);
    let capability = ctx.capability.as_ref().context("a self capability")?;
    anyhow::ensure!(capability.capability.signer == lane);
    endpoint.close().await;
    Ok(())
}

/// A store that cannot read or record its lane salt.
struct UnreadableSalt;

/// Only `lane_salt` is reached: `for_store` reads nothing else.
impl BuyerPoolStore for UnreadableSalt {
    fn load_all(&self) -> Result<decdn_incentive::BuyerLoad, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn record(&self, _: &BuyerPoolState) -> Result<(), StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn forget(&self, _: Address) -> Result<(), StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn get_by_pool_id(&self, _: PoolId) -> Result<Option<BuyerPoolState>, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn forget_if_pool(&self, _: Address, _: PoolId) -> Result<bool, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn get_by_owner(&self, _: Address) -> Result<Option<BuyerPoolState>, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn advance_progress(
        &self,
        _: Address,
        _: PoolId,
        _: LaneKey,
        _: U256,
        _: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn rebase_progress(
        &self,
        _: Address,
        _: PoolId,
        _: LaneKey,
        _: decdn_incentive::BuyerLaneProgress,
        _: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn seed_progress(
        &self,
        _: Address,
        _: PoolId,
        _: LaneKey,
        _: U256,
        _: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn add_deposit(
        &self,
        _: Address,
        _: PoolId,
        _: U256,
    ) -> Result<decdn_incentive::DepositOutcome, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn delegate_lanes(
        &self,
        _: Deployment,
        _: PoolId,
        _: Address,
    ) -> Result<Vec<(Address, decdn_incentive::BuyerLaneProgress)>, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn advance_delegate_progress(
        &self,
        _: Deployment,
        _: LaneKey,
        _: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn rebase_delegate_progress(
        &self,
        _: Deployment,
        _: LaneKey,
        _: decdn_incentive::BuyerLaneProgress,
        _: decdn_incentive::BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        Err(StoreError::Backend("unused".into()))
    }
    fn lane_salt(&self) -> Result<decdn_incentive::lane_signer::LaneSalt, StoreError> {
        Err(StoreError::Backend("lane salt unreadable".into()))
    }
}

/// A salt the store cannot read fails the payer as this machine's fault. It
/// never falls back to the owner key or a fresh salt: either would sign on
/// another lane than the one the store records, and lose the lane's watermark.
#[test]
fn an_unreadable_lane_salt_fails_the_payer_as_a_local_fault() {
    let err = OwnedPayer::for_store(Arc::new(PrivateKeySigner::random()), &UnreadableSalt)
        .expect_err("an unreadable salt derives no lane key");
    assert!(
        err.downcast_ref::<crate::LocalPullFault>().is_some(),
        "the store fault is this machine's: {err:#}"
    );
    assert!(
        format!("{err:#}").contains("lane salt unreadable"),
        "{err:#}"
    );
}
