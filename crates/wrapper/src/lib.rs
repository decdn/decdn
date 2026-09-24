//! `decdn-sponsored`: the walletless client for the sponsord gateway. Gives
//! each download a throwaway key, obtains a sponsor capability for it via a
//! browser captcha, and delegates the pull itself to the `decdn` binary.

pub mod api;
pub mod config;
pub mod flow;
pub mod keystore;
pub mod runner;
pub mod session;
