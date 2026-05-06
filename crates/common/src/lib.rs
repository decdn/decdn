//! Shared types for the deCDN binaries.
//!
//! The daemon (`decdn-node`) and the user CLI (`decdn`) both depend on this
//! crate for the config schema, identity loading, and CLI argument structs
//! they share. Wire formats and protocol-level types live in
//! [`decdn_protocol`]; cache-engine types live in [`decdn_cache`]. This crate
//! holds only what *both* binaries need to parse, validate, or pass between
//! processes.

pub mod admin;
pub mod cli;
pub mod config;
pub mod eth_identity;
pub mod identity;
