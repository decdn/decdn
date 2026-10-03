//! The onramp client the CLI uses: `sponsord_api`'s `OnrampClient` with a
//! per-request timeout and a `User-Agent` naming this CLI's version.

use std::time::Duration;

use sponsord_api::client::OnrampClient;

/// Bound on each request to the onramp, so a hung onramp fails the poll
/// instead of stalling it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// `decdn-sponsored/<version>`, sent with every request.
pub const USER_AGENT: &str = concat!("decdn-sponsored/", env!("CARGO_PKG_VERSION"));

/// A client for the onramp at `base`.
///
/// # Errors
///
/// The HTTP client cannot be built (no TLS backend).
pub fn client(base: &str) -> anyhow::Result<OnrampClient> {
    let http = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()?;
    Ok(OnrampClient::new(base, http))
}
