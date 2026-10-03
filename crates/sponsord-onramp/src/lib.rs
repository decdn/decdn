//! `sponsord-onramp`: the public onramp in front of a `sponsord` daemon.
//! Serves the installers, the gate page, and the API `decdn-sponsored`
//! polls, and hands each capability from the browser to the CLI.
//!
//! The gate is pluggable ([`gate::Gate`]); [`gate::TurnstileGate`] is built in.

pub mod config;
pub mod daemon;
pub mod gate;
pub mod grants;
pub mod http;
pub mod installer;
pub mod net;
pub mod state;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
