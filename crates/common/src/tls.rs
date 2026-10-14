//! The process-wide rustls crypto provider.
//!
//! Every client build links one TLS backend, ring: iroh's QUIC stack already
//! uses it, and the workspace builds reqwest with `rustls-no-provider` so that
//! aws-lc-rs stays out of the client SDK and the CLI. The cost of
//! `rustls-no-provider` is that reqwest takes its provider from the process
//! default and panics in `Client::build` when none is installed — and alloy's
//! HTTP transport builds a `reqwest::Client` for every `connect_http`, whatever
//! the URL scheme. Call [`install_crypto_provider`] before building one.

/// Install ring as the process-wide rustls crypto provider.
///
/// Idempotent and cheap: a second call, or a host application that installed
/// its own provider first, leaves the installed provider in place.
pub fn install_crypto_provider() {
    // `Err` means a provider is already installed, which is the goal.
    let _ = rustls::crypto::ring::default_provider().install_default();
}
