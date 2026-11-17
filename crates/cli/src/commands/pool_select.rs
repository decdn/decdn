//! Which `PaymentPool` contracts a command acts on: the `--pool` and
//! `--pool-rpc-url` flags ([`cli::PoolSelectArgs`]) over the
//! `[[blockchain.payment_chains]]` entries of the config file.
//!
//! Without `--pool`, a command acts on every configured pool, in configured
//! order. `--pool` names one pool as a CAIP-10 account. Its RPC endpoint is
//! `--pool-rpc-url`, else the `rpc_url` of the configured entry that lists the
//! pool, else the `rpc_url` of a configured entry on the same chain. A pool
//! the config does not list is accepted, so a command reaches any deployment
//! given its endpoint.

use anyhow::Context as _;
use decdn_client::session::PaymentPoolChain;
use decdn_common::cli;
use decdn_incentive::Deployment;
use decdn_protocol::PoolAccount;

use super::chain_ctx::parse_nonzero_address;

/// One configured payment chain, as either config reader holds it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ConfiguredChain<'a> {
    /// The chain's JSON-RPC endpoint.
    pub(crate) rpc_url: Option<&'a str>,
    /// The chain's EIP-155 id; `None` reads as the governance chain id.
    pub(crate) chain_id: Option<u64>,
    /// The `PaymentPool` addresses on it, as written.
    pub(crate) payment_pools: &'a [String],
}

impl<'a> From<&'a decdn_common::config::types::PaymentChainConfig> for ConfiguredChain<'a> {
    fn from(entry: &'a decdn_common::config::types::PaymentChainConfig) -> Self {
        Self {
            rpc_url: entry.rpc_url.as_deref(),
            chain_id: entry.chain_id,
            payment_pools: &entry.payment_pools,
        }
    }
}

impl<'a> From<&'a super::chain_ctx::FilePaymentChain> for ConfiguredChain<'a> {
    fn from(entry: &'a super::chain_ctx::FilePaymentChain) -> Self {
        Self {
            rpc_url: entry.rpc_url.as_deref(),
            chain_id: entry.chain_id,
            payment_pools: &entry.payment_pools,
        }
    }
}

/// Parse a `--pool` value: a CAIP-10 account naming a nonzero EVM contract.
///
/// # Errors
///
/// The value is not CAIP-10, not an `eip155` contract address, or the zero
/// address.
pub(crate) fn parse_pool_flag(raw: &str) -> anyhow::Result<Deployment> {
    let account = PoolAccount::new(raw).map_err(|e| {
        anyhow::anyhow!(
            "invalid --pool {raw:?}: {e}; expected a CAIP-10 account such as \
             eip155:421614:0x<address>"
        )
    })?;
    let deployment = Deployment::from_pool_account(&account).ok_or_else(|| {
        anyhow::anyhow!(
            "--pool {raw:?} is not an eip155 contract address; expected \
             eip155:<chainId>:0x<40 hex digits>"
        )
    })?;
    anyhow::ensure!(
        !deployment.payment_pool.is_zero(),
        "--pool {raw:?} names the zero address"
    );
    Ok(deployment)
}

/// The pools a command acts on: `--pool` alone, or every pool of every
/// configured chain. An entry with no chain id is on `governance_chain_id`.
///
/// # Errors
///
/// A malformed `--pool` or configured address; a pool with no RPC endpoint;
/// no pool at all.
pub(crate) fn resolve_pools<'a>(
    select: &cli::PoolSelectArgs,
    configured: impl IntoIterator<Item = ConfiguredChain<'a>>,
    governance_chain_id: u64,
) -> anyhow::Result<Vec<PaymentPoolChain>> {
    let mut pools: Vec<(Deployment, Option<&str>)> = Vec::new();
    let mut chain_rpcs: Vec<(u64, &str)> = Vec::new();
    for entry in configured {
        let chain_id = entry.chain_id.unwrap_or(governance_chain_id);
        if let Some(rpc) = entry.rpc_url {
            chain_rpcs.push((chain_id, rpc));
        }
        for raw in entry.payment_pools {
            let payment_pool = parse_nonzero_address(raw, "payment_pools")?;
            pools.push((
                Deployment {
                    chain_id,
                    payment_pool,
                },
                entry.rpc_url,
            ));
        }
    }
    if let Some(raw) = &select.pool {
        let deployment = parse_pool_flag(raw)?;
        let rpc_url = select
            .pool_rpc_url
            .clone()
            .or_else(|| {
                pools
                    .iter()
                    .find(|(d, _)| *d == deployment)
                    .and_then(|(_, rpc)| rpc.map(str::to_owned))
            })
            .or_else(|| {
                chain_rpcs
                    .iter()
                    .find(|(id, _)| *id == deployment.chain_id)
                    .map(|(_, rpc)| (*rpc).to_owned())
            })
            .with_context(|| {
                format!(
                    "no RPC endpoint for --pool {raw}: pass --pool-rpc-url, or configure a \
                     [[blockchain.payment_chains]] entry with chain_id = {} and an rpc_url",
                    deployment.chain_id
                )
            })?;
        return Ok(vec![PaymentPoolChain {
            rpc_url,
            deployment,
        }]);
    }
    anyhow::ensure!(
        !pools.is_empty(),
        "no payment pool configured: add a [[blockchain.payment_chains]] entry, or pass \
         --pool <eip155:chainId:address> with --pool-rpc-url"
    );
    pools
        .into_iter()
        .map(|(deployment, rpc)| {
            Ok(PaymentPoolChain {
                rpc_url: rpc
                    .with_context(|| {
                        format!(
                            "rpc_url not set for payment chain {} (a \
                             [[blockchain.payment_chains]] rpc_url)",
                            deployment.chain_id
                        )
                    })?
                    .to_owned(),
                deployment,
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
