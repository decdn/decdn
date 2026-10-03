//! `decdn-sponsored`: the walletless CLI for a sponsord onramp. Gives each
//! download a throwaway key, obtains a sponsor capability for it through the
//! onramp's gate, and delegates the pull itself to the `decdn` binary.

pub mod config;
pub mod decdn;
pub mod hash;
pub mod onramp;
pub mod pull;
pub mod session;
