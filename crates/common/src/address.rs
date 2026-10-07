//! Shared Ethereum address parsing for the deCDN binaries.
//!
//! Both the daemon (`decdn-node`) and the CLI (`decdn`) parse contract and
//! account addresses out of flags and `[blockchain]` config. This module is the
//! single home for the two parse helpers so the zero-address guard cannot drift
//! between binaries. It lives in `decdn-common` because that is the one crate on
//! the shared path of everything that needs it — `node → common`, `cli → common`,
//! and `incentive → common` — and it already depends on `alloy` (the
//! `decdn-config-types` leaf crate deliberately does not, to keep `alloy` off the
//! publisher-CLI path, so the guard cannot live there).

use alloy::primitives::Address;
use anyhow::Context;

/// Parse a contract/account address with a labelled error.
pub fn parse_address(value: &str, label: &str) -> anyhow::Result<Address> {
    value
        .parse()
        .with_context(|| format!("{label} {value:?} is not a valid address"))
}

/// Parse a contract address and reject the zero address. `Address::ZERO` parses
/// cleanly but is never a real deployment — it would surface only as an opaque
/// on-chain revert at call time, so reject it here with a clear, labelled error.
///
/// Use this for **every contract address** parsed from a flag/config: the CLI's
/// `resolve` / `fetch` / `channel` / `setup` sites (#1153/#1213), the daemon's
/// runtime bring-up sites (#1219), and the swap-venue addresses in
/// `decdn-incentive` (#1213). Account/EOA addresses (`--provider-address`,
/// `operator`) deliberately stay on [`parse_address`] — the "never a real
/// deployment" rationale is contract-specific.
pub fn parse_nonzero_address(value: &str, label: &str) -> anyhow::Result<Address> {
    let addr = parse_address(value, label)?;
    anyhow::ensure!(
        addr != Address::ZERO,
        "{label} must not be the zero address — set it to the deployed contract address"
    );
    Ok(addr)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
