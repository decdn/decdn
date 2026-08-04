//! `onramp`: the decdn-sponsor CLI wrapper. Wraps a plain `decdn` node
//! process, buys/tops-up a payment channel on the operator's behalf, and
//! (in later tasks) proxies requests through the sponsor's channel.

pub mod api;
pub mod config;
pub mod flow;
pub mod keystore;
pub mod runner;
