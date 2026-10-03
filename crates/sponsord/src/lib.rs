//! `sponsord`: the sponsor's signing and top-up service. Holds the pool
//! owner's key, signs capabilities for trusted callers over a bearer-token
//! HTTP API, and keeps the pool funded. Onramp-agnostic and stateless.

pub mod config;
pub mod http;
