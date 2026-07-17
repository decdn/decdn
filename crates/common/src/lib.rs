//! Shared types for the deCDN binaries.
//!
//! The daemon (`decdn-node`) and the user CLI (`decdn`) both depend on this
//! crate for the config schema, identity loading, and CLI argument structs
//! they share. Wire formats and protocol-level types live in
//! [`decdn_protocol`]; the config-vocabulary value types this schema is
//! built from (`RetryPolicy`, `DecompressMode`, `OriginUrl`,
//! `OriginKind`, `Hash`, `PinnedHashes`) live in the
//! [`decdn_config_types`] leaf crate, so this crate (and the publisher
//! CLI) never link the cache engine / iroh-blobs / AWS SDK (#578). The
//! cache-engine types proper stay in `decdn-cache`, a `decdn-node`-only
//! dependency. This crate holds only what *both* binaries need to
//! parse, validate, or pass between processes.

pub mod address;
pub mod admin;
pub mod cli;
pub mod config;
pub mod identity;
pub mod redact;
