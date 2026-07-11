//! Wallet-filled HTTP provider builder shared by the client commands
//! (`fetch`, `bundle pull`, `channel`) and the on-chain operator commands
//! (`register`, `bond`, `setup`).
//!
//! Extracted here so any client of the `cdn/client/v1` data plane — not just
//! the `decdn` CLI bin crate — can build the signing provider it opens and
//! settles payment channels with.

use alloy::network::EthereumWallet;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;

/// Build a wallet-filled HTTP provider that signs and sends transactions as
/// `signer`. The `Url` type is inferred from `connect_http`'s parameter;
/// naming it explicitly would need `alloy::transports`, which is not exposed
/// under this crate's alloy feature set.
///
/// # Errors
///
/// Fails if `rpc_url` is not a valid URL. The value is never echoed into the
/// error: an `rpc_url` secret commonly lives in the path/query, which userinfo
/// redaction wouldn't scrub, so the value is hidden entirely (matching
/// `config validate`'s `<redacted> (N chars)`).
// `use<>` pins the returned provider to capture *no* input lifetimes: it owns
// a clone of `signer` and the parsed URL, so it is genuinely `'static`. Without
// the precise-capturing bound, Rust 2024's RPIT rules over-capture `&signer`,
// which would stop callers (e.g. `setup`'s USDC swap venue) from handing the
// provider to APIs that need `P: 'static` (`DynProvider::erased`).
pub fn build_provider(
    rpc_url: &str,
    signer: &PrivateKeySigner,
) -> anyhow::Result<impl Provider + Clone + use<>> {
    Ok(ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(rpc_url.parse().with_context(|| {
            format!(
                "rpc_url is not a valid URL (<redacted>, {} chars)",
                rpc_url.len()
            )
        })?))
}
