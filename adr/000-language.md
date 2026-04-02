# ADR 000: Language and Core Networking Stack

**Date:** 2026-03-28
**Status:** Draft

## Context

We are building a decentralized CDN with two participant roles: **nodes** (providers) that cache and deliver content to clients — some configured with an origin backend (e.g., S3/R2/NFS/local disk) as the canonical source for specific content — and **clients** that consume content. The network requires:

- High-throughput, low-latency blob transfer between all node types over peer-to-peer connections
- Concurrent handling of many inbound connections per node
- Safe memory management without a garbage collector introducing latency spikes under load
- A QUIC-based transport with content-addressed verified transfer
- A single binary deployable as a node or client depending on configuration

## Decision

Use **Rust** as the implementation language and **iroh** as the core networking library.

iroh versioning policy: pin to a specific version in `Cargo.toml` (e.g. `iroh = "0.97"`) rather than an open range. The current evaluated baseline is **0.97**. Upgrades are deliberate — evaluate API compatibility, update `Cargo.toml`, and record the new baseline here before merging. `Cargo.lock` is committed and acts as the true pin within a given version constraint.

Specifically:
- `iroh::Endpoint` for QUIC-based peer-to-peer connectivity and ALPN protocol negotiation
- `iroh-blobs` with `fs-store` backend for content-addressed blob storage and verified transfer
- `iroh-gossip` for topic-based epidemic broadcast (node discovery and metadata announcements via `NodeAnnounce`)

## Consequences

**Positive:**

- Memory safety without GC pauses — predictable tail latency under concurrent delivery load
- Rust's async runtime (tokio) handles thousands of concurrent connections per node efficiently
- iroh bundles QUIC, NAT traversal, content-addressed transfer, and verified streaming — fewer moving parts than assembling these from separate libraries
- BLAKE3 is native to iroh's content model; blob IDs and transport layer use the same hash with no translation layer
- A single statically linked binary simplifies deployment with no runtime dependency management

**Negative:**

- Rust's compile times slow the development feedback loop compared to interpreted or JVM languages
- The team needs Rust proficiency; onboarding contributors takes longer
- iroh is a relatively young library; its APIs have changed across versions and may continue to do so — mitigated by pinning policy above
- Fewer off-the-shelf libraries for EVM interaction compared to TypeScript or Python — `alloy-rs` covers the gap but with less community documentation
