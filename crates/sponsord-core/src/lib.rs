//! The sponsor's signing and pool top-up logic: sign capped, expiring
//! capabilities against the sponsor's `PaymentPool` and keep that pool
//! funded from the treasury. Embeddable on its own; the `sponsord` daemon
//! serves it over HTTP.

pub mod issuer;
pub mod keeper;
pub mod pool;
pub mod sponsor;

pub use sponsor::{Info, Issued, Sponsor, SponsorConfig, SponsorError};
pub use sponsord_api::MicroUsdc;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
