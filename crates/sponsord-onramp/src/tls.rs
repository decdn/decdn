//! The process-wide rustls crypto provider.
//!
//! The workspace builds reqwest with `rustls-no-provider`, so that every
//! binary links one TLS backend, ring, and none links aws-lc-rs. The cost is
//! that `reqwest::Client::build` panics until a provider is installed. The
//! onramp installs ring itself rather than through `decdn-common`, which it
//! does not otherwise depend on.

/// Install ring as the process-wide rustls crypto provider.
///
/// Idempotent: a second call, or a host that installed its own provider
/// first, leaves the installed provider in place.
pub fn install_crypto_provider() {
    // `Err` means a provider is already installed, which is the goal.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// [`install_crypto_provider`], then a default `reqwest::Client`.
#[cfg(test)]
pub(crate) fn install_crypto_provider_and_client() -> reqwest::Client {
    install_crypto_provider();
    reqwest::Client::new()
}
