//! The sponsor's signing and pool top-up logic: sign capped, expiring
//! capabilities against the sponsor's `PaymentPool` and keep that pool
//! funded from the treasury. Embeddable on its own; the `sponsord` daemon
//! serves it over HTTP.

pub mod issuer;
pub mod money;
pub mod pool_watch;
pub mod sponsor;
pub mod treasury;

pub use sponsor::{Info, Issued, Sponsor, SponsorConfig, SponsorError};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
