//! The redeemer's per-chain redemption floor (ADR 003 § Redemption floor).
//!
//! A redemption costs the node gas, and the gas costs USDC. The floor is that
//! cost: the gas of a `redeemMany`, priced at the payment chain's live gas
//! price and at the operator's configured gas-token price
//! (`blockchain.payment_chains[].gas_token_price_micro_usdc`). On Arbitrum the
//! price adds the L1 data fee that the `ArbGasInfo` precompile quotes. A claim
//! that cannot pay for its own gas is not redeemed, so the node never redeems
//! at a loss.
//!
//! Two floors come from one quote:
//!
//! - **Chunk floor.** A `redeemMany` of `n` claims redeems when the claims pay
//!   [`CHUNK_COST_MULTIPLE`] times the transaction's gas: its fixed gas plus
//!   `n` claims' gas. Gas is then at most 5% of what a chunk redeems.
//! - **Final-claim floor.** A claim that can grow no more (its capability is
//!   inside the node's expiry margin) redeems when it pays
//!   [`FINAL_CLAIM_COST_MULTIPLE`] times one claim's gas, batched with other
//!   claims or alone.
//!
//! [`FloorSource`] reads the gas price at redeem time and keeps a quote for
//! [`QUOTE_TTL`], so a burst of redeem hints costs one read.

use std::sync::Mutex;
use std::time::Duration;

use alloy::primitives::{Address, U256, address};
use alloy::providers::Provider;
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::chain_events::timed;

#[cfg(test)]
mod tests;

/// Gas of one claim in a `redeemMany`: a first-time signer's lane, whose
/// capability registers in the same call. The gas difference between
/// `test_redeemMany_gas_firstTime_NPlus1Vouchers` and
/// `test_redeemMany_gas_firstTime_NVouchers` in `contracts/test/PaymentPool.t.sol`
/// (759,105 − 695,329).
pub const REDEEM_CLAIM_GAS: u64 = 63_776;

/// Fixed gas of one `redeemMany` transaction: the 21,000 intrinsic gas plus
/// the call's gas beyond its claims. `test_redeemMany_gas_firstTime_NVouchers`
/// (695,329 for ten claims) less ten [`REDEEM_CLAIM_GAS`], plus 21,000.
pub const REDEEM_TX_GAS: u64 = 78_569;

/// Calldata bytes of one claim: a `LaneVoucher` (eight words) and a
/// `CapabilityReg` (signer, cap, expiry, signature offset and length, the
/// 65-byte signature padded to three words, and its array offset).
pub const REDEEM_CLAIM_CALLDATA_BYTES: u64 = 544;

/// Execution gas of one non-zero calldata byte (EIP-2028). The floor prices
/// every claim byte at it.
const CALLDATA_GAS_PER_BYTE: u64 = 16;

/// A chunk redeems once its claims pay this multiple of its gas, so gas is at
/// most 5% of what a chunk redeems.
pub const CHUNK_COST_MULTIPLE: u64 = 20;

/// A final claim redeems once it pays this multiple of one claim's gas. One
/// claim's gas and a whole transaction's fixed gas together cost less than
/// three claims' gas, so a final claim that redeems alone still pays for its
/// transaction.
pub const FINAL_CLAIM_COST_MULTIPLE: u64 = 3;

/// How long a gas quote stays fresh. The redeemer reads the gas price again
/// only after this, so a burst of redeem hints costs one read.
pub const QUOTE_TTL: Duration = Duration::from_mins(1);

/// Wei in one whole gas token.
const WEI_PER_TOKEN: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// The `ArbGasInfo` precompile, present on every Arbitrum chain.
const ARB_GAS_INFO: Address = address!("000000000000000000000000000000000000006C");

#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    clippy::pub_underscore_fields,
    clippy::missing_docs_in_private_items,
    missing_debug_implementations,
    missing_docs,
    non_snake_case,
    unreachable_pub
)]
mod sol_types {
    alloy::sol! {
        #[sol(rpc)]
        interface ArbGasInfo {
            function getPricesInWei() external view returns (
                uint256 perL2Tx,
                uint256 perL1CalldataByte,
                uint256 perStorageAllocation,
                uint256 perArbGasBase,
                uint256 perArbGasCongestion,
                uint256 perArbGasTotal
            );
        }
    }
}
use sol_types::ArbGasInfo;

/// One read of a payment chain's gas prices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GasQuote {
    /// Wei per unit of execution gas (`eth_gasPrice`).
    pub gas_price_wei: u128,
    /// Wei of L1 data fee per calldata byte. `0` off Arbitrum.
    pub l1_wei_per_byte: U256,
    /// Wei of L1 fee per transaction. `0` off Arbitrum.
    pub l1_wei_per_tx: U256,
}

/// The redemption floors of one gas quote, in `µUSDC`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedeemFloors {
    /// Wei of a `redeemMany`'s fixed part.
    tx_wei: U256,
    /// Wei of one claim in a `redeemMany`.
    claim_wei: U256,
    /// `µUSDC` per whole gas token.
    price_micro_usdc: u64,
}

impl RedeemFloors {
    /// The floors of `quote` at a gas-token price of `price_micro_usdc`.
    #[must_use]
    pub fn new(quote: GasQuote, price_micro_usdc: u64) -> Self {
        let gas_price = U256::from(quote.gas_price_wei);
        let claim_gas = REDEEM_CLAIM_GAS
            .saturating_add(REDEEM_CLAIM_CALLDATA_BYTES.saturating_mul(CALLDATA_GAS_PER_BYTE));
        let claim_wei = U256::from(claim_gas)
            .saturating_mul(gas_price)
            .saturating_add(
                U256::from(REDEEM_CLAIM_CALLDATA_BYTES).saturating_mul(quote.l1_wei_per_byte),
            );
        let tx_wei = U256::from(REDEEM_TX_GAS)
            .saturating_mul(gas_price)
            .saturating_add(quote.l1_wei_per_tx);
        Self::from_costs(tx_wei, claim_wei, price_micro_usdc)
    }

    /// The floors of a transaction whose fixed part costs `tx_wei` and whose
    /// claims cost `claim_wei` each.
    #[must_use]
    pub const fn from_costs(tx_wei: U256, claim_wei: U256, price_micro_usdc: u64) -> Self {
        Self {
            tx_wei,
            claim_wei,
            price_micro_usdc,
        }
    }

    /// The least a chunk of `claims` claims must pay to redeem:
    /// [`CHUNK_COST_MULTIPLE`] times the gas of a `redeemMany` that carries
    /// them.
    #[must_use]
    pub fn chunk(&self, claims: usize) -> U256 {
        let wei = self
            .claim_wei
            .saturating_mul(U256::from(claims))
            .saturating_add(self.tx_wei)
            .saturating_mul(U256::from(CHUNK_COST_MULTIPLE));
        self.micro_usdc_of(wei)
    }

    /// The least a final claim must pay to redeem:
    /// [`FINAL_CLAIM_COST_MULTIPLE`] times one claim's gas.
    #[must_use]
    pub fn final_claim(&self) -> U256 {
        let wei = self
            .claim_wei
            .saturating_mul(U256::from(FINAL_CLAIM_COST_MULTIPLE));
        self.micro_usdc_of(wei)
    }

    /// `wei` of gas token in `µUSDC`, rounded up.
    fn micro_usdc_of(&self, wei: U256) -> U256 {
        wei.saturating_mul(U256::from(self.price_micro_usdc))
            .div_ceil(WEI_PER_TOKEN)
    }
}

/// Where the redeemer gets its floors.
enum Source<P> {
    /// The payment chain's live gas prices.
    Live {
        /// The payment chain's provider.
        provider: P,
        /// Whether the chain has the `ArbGasInfo` precompile, read once.
        arbitrum: OnceCell<bool>,
        /// The last quote read and when, reused for [`QUOTE_TTL`], and at any
        /// age when a read fails.
        last: Mutex<Option<(GasQuote, Instant)>>,
    },
    /// Fixed floors, for tests.
    #[cfg(test)]
    Fixed(RedeemFloors),
}

/// The redeemer's floors for one payment chain.
pub struct FloorSource<P> {
    /// Where the floors come from.
    source: Source<P>,
    /// `µUSDC` per whole gas token, from the payment chain's config.
    price_micro_usdc: u64,
}

impl<P> std::fmt::Debug for FloorSource<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FloorSource")
            .field("price_micro_usdc", &self.price_micro_usdc)
            .finish_non_exhaustive()
    }
}

impl<P: Provider> FloorSource<P> {
    /// Floors from `provider`'s live gas prices at a gas-token price of
    /// `price_micro_usdc`.
    pub const fn live(provider: P, price_micro_usdc: u64) -> Self {
        Self {
            source: Source::Live {
                provider,
                arbitrum: OnceCell::const_new(),
                last: Mutex::new(None),
            },
            price_micro_usdc,
        }
    }

    /// Fixed floors, for tests.
    #[cfg(test)]
    #[must_use]
    pub const fn fixed(floors: RedeemFloors) -> Self {
        Self {
            source: Source::Fixed(floors),
            price_micro_usdc: floors.price_micro_usdc,
        }
    }

    /// The floors now: from a quote younger than [`QUOTE_TTL`], or from a fresh
    /// read. When the read fails, from the last quote at any age. `None` when
    /// no quote was ever read: the redeemer then redeems nothing and retries.
    pub async fn floors(&self) -> Option<RedeemFloors> {
        match &self.source {
            #[cfg(test)]
            Source::Fixed(floors) => Some(*floors),
            Source::Live {
                provider,
                arbitrum,
                last,
            } => {
                let cached = *last
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some((quote, at)) = cached
                    && at.elapsed() < QUOTE_TTL
                {
                    return Some(RedeemFloors::new(quote, self.price_micro_usdc));
                }
                match read_quote(provider, arbitrum).await {
                    Ok(quote) => {
                        debug!(?quote, "redeemer: gas quote read");
                        *last
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            Some((quote, Instant::now()));
                        Some(RedeemFloors::new(quote, self.price_micro_usdc))
                    }
                    Err(err) => {
                        warn!(
                            error = %decdn_common::redact::sanitize_err_chain(&err),
                            stale = cached.is_some(),
                            "redeemer: gas quote read failed; using the last quote"
                        );
                        cached.map(|(quote, _)| RedeemFloors::new(quote, self.price_micro_usdc))
                    }
                }
            }
        }
    }
}

/// Read the chain's gas price and, on a chain with the `ArbGasInfo`
/// precompile, its L1 fee prices.
async fn read_quote<P: Provider>(
    provider: &P,
    arbitrum: &OnceCell<bool>,
) -> anyhow::Result<GasQuote> {
    let gas_price_wei = timed(None, "eth_gasPrice", provider.get_gas_price()).await?;
    let arbitrum = *arbitrum
        .get_or_try_init(|| async {
            let code = timed(
                None,
                "eth_getCode(ArbGasInfo)",
                provider.get_code_at(ARB_GAS_INFO),
            )
            .await?;
            anyhow::Ok(!code.is_empty())
        })
        .await?;
    if !arbitrum {
        return Ok(GasQuote {
            gas_price_wei,
            l1_wei_per_byte: U256::ZERO,
            l1_wei_per_tx: U256::ZERO,
        });
    }
    let prices = timed(
        None,
        "ArbGasInfo.getPricesInWei()",
        ArbGasInfo::new(ARB_GAS_INFO, provider)
            .getPricesInWei()
            .call(),
    )
    .await?;
    Ok(GasQuote {
        gas_price_wei,
        l1_wei_per_byte: prices.perL1CalldataByte,
        l1_wei_per_tx: prices.perL2Tx,
    })
}
