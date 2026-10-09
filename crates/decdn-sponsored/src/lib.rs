//! `decdn-sponsored`: the walletless CLI for a sponsord onramp. Gives each
//! download a throwaway key, obtains a sponsor capability for it through the
//! onramp's gate, and delegates the pull itself to the `decdn` binary.
//!
//! Terminal UI: it tells the user what to open and how the pull went on
//! stdout and stderr, so this crate root allows the two print lints the rest
//! of the workspace denies.
#![allow(clippy::print_stdout, clippy::print_stderr)]

pub mod config;
pub mod decdn;
pub mod hash;
pub mod onramp;
pub mod pull;
pub mod session;
