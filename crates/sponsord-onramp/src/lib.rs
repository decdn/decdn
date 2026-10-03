//! `sponsord-onramp`: the captcha-gated, CLI-driven onramp in front of a `sponsord`
//! daemon. Serves the installers, the `/fund` captcha page, and the
//! `/capability` poll `decdn-sponsored` uses, and keeps the grant store that
//! hands a token from the browser to the CLI.

pub mod captcha;
pub mod config;
pub mod daemon;
pub mod http;
pub mod state;
pub mod store;
