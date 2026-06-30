//! Client fixture: drives the real paid client path (`cdn/client/v1`) against a
//! [`crate::node::NodeFixture`] — open an on-chain `PaymentChannel`, dial the
//! daemon's QUIC endpoint, and run a voucher-signed `stream_fetch`, returning
//! the delivered (BLAKE3-verified) bytes.
//!
//! The client runs in-process (it is not a daemon, so it has none of the
//! global-state constraints that push the node fixture to a subprocess) and
//! dials the daemon over real QUIC by direct loopback address.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client_pull::{ChannelContext, stream_fetch};
use decdn_incentive::{slash_judge_domain, voucher_domain};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey};

use crate::assert::{channel_id, client_channel_nonce};
use crate::bindings::{Erc20, PaymentChannelOpen};
use crate::chain::{CHAIN_ID, ChainFixture};
use crate::node::NodeFixture;

/// Default channel deposit: 10 USDC (≥ the contract `minDeposit`).
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// Fixed slash-receipt timestamp (µs). The node does not gate freshness on this
/// path; a constant keeps the voucher deterministic (matches the settlement
/// e2e).
const TIMESTAMP_US: u64 = 0x00c0_ffe1;

/// Result of a paid fetch: the delivered bytes and the channel they were paid
/// through.
#[derive(Debug)]
pub struct FetchOutcome {
    /// Delivered, BLAKE3-verified payload.
    pub bytes: Vec<u8>,
    /// On-chain `channelId` the vouchers were signed against.
    pub channel_id: B256,
}

/// A funded buyer that pays nodes for delivery over `cdn/client/v1`.
#[derive(Debug)]
pub struct ClientFixture {
    signer: Arc<PrivateKeySigner>,
    endpoint: Endpoint,
}

impl ClientFixture {
    /// Create a funded client: fresh eth key with gas, a mock-USDC balance, and
    /// a max approval for the `PaymentChannel`, plus a loopback iroh endpoint.
    pub async fn new(chain: &ChainFixture) -> anyhow::Result<Self> {
        let signer = Arc::new(PrivateKeySigner::random());
        let addr = signer.address();
        chain.fund_eth(addr, 100).await?;
        chain
            .mint_usdc(addr, U256::from(1_000_000_000u64))
            .await
            .context("mint client USDC")?;

        // One-time max-ish approval so repeated opens don't each re-approve.
        let provider = chain.provider_for(&signer);
        Erc20::new(chain.usdc, &provider)
            .approve(
                chain.addrs.payment_channel,
                U256::from(DEPOSIT_MICRO_USDC) * U256::from(100u64),
            )
            .send()
            .await
            .context("client approve PaymentChannel")?
            .get_receipt()
            .await
            .context("client approve receipt")?;

        let endpoint = loopback_endpoint().await?;
        Ok(Self { signer, endpoint })
    }

    /// Open a channel to `node`, then fetch `hash` over the paid path, retrying
    /// until the node's chain watcher has observed the `ChannelOpened` event and
    /// begins accepting the channel's vouchers. Verifies the delivered bytes
    /// hash to `hash`.
    pub async fn fetch(
        &self,
        chain: &ChainFixture,
        node: &NodeFixture,
        hash: Hash,
    ) -> anyhow::Result<FetchOutcome> {
        let client_addr = self.signer.address();
        let provider = chain.provider_for(&self.signer);
        let pc = PaymentChannelOpen::new(chain.addrs.payment_channel, &provider);

        let nonce = client_channel_nonce(&provider, chain.addrs.payment_channel, client_addr)
            .await
            .context("read client channel nonce")?;
        let deposit = U256::from(DEPOSIT_MICRO_USDC);
        pc.openChannel(node.operator_addr, deposit)
            .send()
            .await
            .context("openChannel send")?
            .get_receipt()
            .await
            .context("openChannel receipt")?;
        let cid = channel_id(
            client_addr,
            node.operator_addr,
            u64::try_from(nonce).context("channel nonce overflow")?,
        );

        let ctx = ChannelContext {
            channel_id: cid,
            token: chain.usdc,
            deposit,
            client_signer: Arc::clone(&self.signer),
            voucher_domain: voucher_domain(CHAIN_ID, chain.addrs.payment_channel),
            prior_nonce: U256::ZERO,
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
        };
        let slash_domain = slash_judge_domain(CHAIN_ID, chain.addrs.slash_judge);
        let target = EndpointAddr::new(node.node_id).with_ip_addr(SocketAddr::V4(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port),
        ));

        // The node accepts vouchers only once its chain watcher has decoded the
        // ChannelOpened event (poll cadence ~500ms). Retry the paid fetch until
        // that catch-up completes or the budget expires.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        loop {
            match stream_fetch(
                &self.endpoint,
                target.clone(),
                &ctx,
                &slash_domain,
                node.operator_addr,
                *hash.as_bytes(),
                0,
                TIMESTAMP_US,
                Duration::from_secs(30),
            )
            .await
            {
                Ok(bytes) => {
                    let got = bytes.as_ref().to_vec();
                    anyhow::ensure!(
                        Hash::new(&got) == hash,
                        "delivered bytes do not hash to the requested blob"
                    );
                    return Ok(FetchOutcome {
                        bytes: got,
                        channel_id: cid,
                    });
                }
                Err(e) if tokio::time::Instant::now() < deadline => {
                    tracing::debug!("paid fetch not ready ({e}); retrying after watcher catch-up");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e).context("paid fetch failed"),
            }
        }
    }
}

/// Bind a loopback iroh endpoint with relays disabled (no ALPNs — client only
/// dials). Mirrors the node integration-test `support::local_endpoint` helper.
async fn loopback_endpoint() -> anyhow::Result<Endpoint> {
    Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .alpns(vec![])
        .relay_mode(RelayMode::Disabled)
        .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind iroh endpoint: {e}"))
}
