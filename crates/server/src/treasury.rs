//! `Treasury`: the on-chain payment-channel operations the sponsor server
//! needs (open a channel, read its escrow, top it up), plus `DecdnTreasury`,
//! the decdn-backed implementation.
//!
//! The trait is what the HTTP layer's tests mock (`FakeTreasury`, below);
//! `DecdnTreasury` is exercised end-to-end by the anvil integration test
//! (Task 8) — here it only needs to compile against the real decdn contract
//! bindings.
//!
//! # Why `top_up_to` does not call `decdn_client_pull::buyer_channel::top_up`
//!
//! The brief's sketch imported `top_up` from `decdn_client_pull::buyer_channel`
//! alongside `open_channel` and `ensure_allowance`. Reading the real signature
//! (`decdn/crates/client-pull/src/buyer_channel.rs:499`):
//!
//! ```ignore
//! pub async fn top_up<P, S>(
//!     contract: &PaymentChannel::PaymentChannelInstance<P>,
//!     store: &S,
//!     provider_addr: Address,
//!     additional: U256,
//! ) -> Result<DepositOutcome>
//! where
//!     P: Provider + Clone,
//!     S: BuyerChannelStore + ?Sized,
//! ```
//!
//! it takes a `&S: BuyerChannelStore` and a `provider_addr` (it looks up the
//! `channel_id` itself via `store.get_by_provider`, then credits the local row
//! via `store.add_deposit` after the tx lands). That helper exists to keep a
//! *persisted buyer channel store* (the node's reuse cache, the CLI's
//! auto-refill cache) in sync with the chain across a `topUp` call.
//!
//! `DecdnTreasury` has no `BuyerChannelStore` — the sponsor server does not
//! cache/reuse channels locally; the caller always names the `channel_id`
//! directly (it is the trait's own parameter) and the chain is authoritative
//! for `deposit_of`. So `top_up_to` calls the generated contract binding's
//! `topUp(channelId, additional)` directly — the same call `top_up` makes
//! internally (`buyer_channel.rs:518-525`) — and re-reads `getChannel` for the
//! committed deposit afterward, rather than threading a store this crate does
//! not have through a helper built for a different caller shape.
//!
//! `open_channel` and `ensure_allowance` do NOT take a store — those two match
//! the brief's guessed signatures once confirmed against
//! `decdn/crates/client-pull/src/buyer_channel.rs:100` and `:205` — so they are
//! used as sketched.

use std::sync::Arc;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use decdn_client_pull::buyer_channel::{ensure_allowance, open_channel};
use decdn_client_pull::provider::build_provider;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::voucher_domain;

use crate::money::MicroUsdc;

/// The on-chain payment-channel operations the sponsor server needs. Mocked
/// by `FakeTreasury` in the HTTP-layer tests; backed by `DecdnTreasury` in
/// production.
#[async_trait]
pub trait Treasury: Send + Sync {
    /// Open a fresh payment channel against `provider_addr`, escrowing
    /// `deposit` USDC, with `voucher_signer` pinned as the channel's
    /// EIP-712 signer. Returns the on-chain `channel_id`.
    async fn open(
        &self,
        provider_addr: Address,
        voucher_signer: Address,
        deposit: MicroUsdc,
    ) -> anyhow::Result<B256>;

    /// The channel's current on-chain escrow (`Channel.deposit`).
    async fn deposit_of(&self, channel_id: B256) -> anyhow::Result<MicroUsdc>;

    /// Top up `channel_id` so its on-chain deposit reaches `target`. A no-op
    /// (returns the current deposit) if it is already at or above `target`.
    async fn top_up_to(
        &self,
        channel_id: B256,
        provider_addr: Address,
        target: MicroUsdc,
    ) -> anyhow::Result<MicroUsdc>;
}

/// The pieces `DecdnTreasury::connect` needs to build its wallet-filled
/// provider and bind the `PaymentChannel` contract.
///
/// Task 9 (`crate::config::ServerConfig`) does not exist yet; this stands in
/// for it so `connect` has a concrete, self-contained signature today. When
/// Task 9 lands, its `ServerConfig` can either grow the same fields (and
/// `connect` switches to take `&ServerConfig` directly) or expose a
/// `to_treasury_config()` conversion — the `Treasury` trait and
/// `DecdnTreasury`'s three methods are the stable surface later tasks
/// depend on, not this struct.
#[derive(Clone, Debug)]
pub struct TreasuryConfig {
    pub rpc_url: String,
    pub payment_channel: Address,
    pub chain_id: u64,
    pub signer: PrivateKeySigner,
}

/// decdn-backed `Treasury`: opens and tops up USDC payment channels from a
/// hot wallet, reusing decdn's `client-pull` open/allowance kernel and the
/// generated `PaymentChannel` contract bindings.
pub struct DecdnTreasury<P: Provider + Clone> {
    provider: P,
    contract: PaymentChannel::PaymentChannelInstance<P>,
    signer: Arc<PrivateKeySigner>,
    self_address: Address,
    token: Address,
    domain: Eip712Domain,
}

impl<P: Provider + Clone> DecdnTreasury<P> {
    /// Read `channel_id`'s on-chain `Channel.deposit`, saturating a
    /// too-large `U256` to `u64::MAX` rather than panicking (anti-panic
    /// policy; in practice unreachable for a USDC-denominated deposit).
    async fn read_deposit(&self, channel_id: B256) -> anyhow::Result<MicroUsdc> {
        let ch = self
            .contract
            .getChannel(channel_id)
            .call()
            .await
            .map_err(|e| anyhow::anyhow!("getChannel({channel_id}): {e}"))?;
        Ok(MicroUsdc(u64::try_from(ch.deposit).unwrap_or(u64::MAX)))
    }
}

/// Build a `DecdnTreasury` from `cfg`: a wallet-filled HTTP provider signing
/// as `cfg.signer`, bound to the `PaymentChannel` at `cfg.payment_channel`,
/// with the settlement token read from the contract itself (`usdc()` —
/// never hardcoded) and the EIP-712 voucher domain derived from
/// `cfg.chain_id` + the contract address.
pub async fn connect(cfg: &TreasuryConfig) -> anyhow::Result<Box<dyn Treasury>> {
    let signer = cfg.signer.clone();
    let self_address = signer.address();
    let provider = build_provider(&cfg.rpc_url, &signer)?;
    let contract = PaymentChannel::new(cfg.payment_channel, provider.clone());
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentChannel.usdc(): {e}"))?;
    let domain = voucher_domain(cfg.chain_id, cfg.payment_channel);
    Ok(Box::new(DecdnTreasury {
        provider,
        contract,
        signer: Arc::new(signer),
        self_address,
        token,
        domain,
    }))
}

#[async_trait]
impl<P: Provider + Clone + 'static> Treasury for DecdnTreasury<P> {
    async fn open(
        &self,
        provider_addr: Address,
        voucher_signer: Address,
        deposit: MicroUsdc,
    ) -> anyhow::Result<B256> {
        let dep = U256::from(deposit.0);
        ensure_allowance(
            &self.provider,
            self.token,
            self.self_address,
            *self.contract.address(),
            Some(dep),
        )
        .await?;
        let opened = open_channel(
            &self.contract,
            self.signer.clone(),
            &self.domain,
            self.token,
            self.self_address,
            provider_addr,
            dep,
            voucher_signer,
        )
        .await?;
        Ok(opened.state.channel_id)
    }

    async fn deposit_of(&self, channel_id: B256) -> anyhow::Result<MicroUsdc> {
        self.read_deposit(channel_id).await
    }

    async fn top_up_to(
        &self,
        channel_id: B256,
        _provider_addr: Address,
        target: MicroUsdc,
    ) -> anyhow::Result<MicroUsdc> {
        let current = self.read_deposit(channel_id).await?;
        if current.0 >= target.0 {
            return Ok(current);
        }
        let delta = U256::from(target.0 - current.0);
        ensure_allowance(
            &self.provider,
            self.token,
            self.self_address,
            *self.contract.address(),
            Some(delta),
        )
        .await?;
        // Direct contract call — see the module doc comment on why this does
        // not go through `decdn_client_pull::buyer_channel::top_up` (that
        // helper requires a `BuyerChannelStore` this treasury does not have).
        let receipt = self
            .contract
            .topUp(channel_id, delta)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("submit topUp: {e}"))?
            .get_receipt()
            .await
            .map_err(|e| anyhow::anyhow!("await topUp receipt: {e}"))?;
        if !receipt.status() {
            anyhow::bail!("topUp reverted for channel {channel_id}");
        }
        self.read_deposit(channel_id).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    struct FakeTreasury {
        deposit: std::sync::Mutex<u64>,
    }

    #[async_trait::async_trait]
    impl Treasury for FakeTreasury {
        async fn open(&self, _p: Address, _vs: Address, d: MicroUsdc) -> anyhow::Result<B256> {
            *self.deposit.lock().unwrap() = d.0;
            Ok(B256::repeat_byte(9))
        }
        async fn deposit_of(&self, _id: B256) -> anyhow::Result<MicroUsdc> {
            Ok(MicroUsdc(*self.deposit.lock().unwrap()))
        }
        async fn top_up_to(
            &self,
            _id: B256,
            _p: Address,
            target: MicroUsdc,
        ) -> anyhow::Result<MicroUsdc> {
            let mut d = self.deposit.lock().unwrap();
            if *d < target.0 {
                *d = target.0;
            }
            Ok(MicroUsdc(*d))
        }
    }

    #[tokio::test]
    async fn top_up_to_is_a_noop_when_already_funded() {
        let t = FakeTreasury {
            deposit: std::sync::Mutex::new(2_000_000),
        };
        assert_eq!(
            t.top_up_to(B256::ZERO, Address::ZERO, MicroUsdc(2_000_000))
                .await
                .unwrap()
                .0,
            2_000_000
        );
    }
}
