# ADR 000: Language and Core Networking Stack

**Date:** 2026-03-28
**Status:** Draft

## Context

We are building a decentralized CDN with three node roles: **vault nodes** that hold canonical content behind a hidden backend and seed it into the network, **edge nodes** that cache and deliver content close to clients, and **clients** that consume content. The network requires:

- High-throughput, low-latency blob transfer between all node types over peer-to-peer connections
- Concurrent handling of many inbound connections per node
- Safe memory management without a garbage collector introducing latency spikes under load
- A QUIC-based transport with content-addressed verified transfer
- A single binary deployable as a vault node, edge node, or client depending on configuration

## Decision

Use **Rust** as the implementation language and **iroh** (0.35+) as the core networking library.

Specifically:
- `iroh::Endpoint` for QUIC-based peer-to-peer connectivity and ALPN protocol negotiation
- `iroh-blobs` with `fs-store` backend for content-addressed blob storage and verified transfer
- `iroh-gossip` for topic-based epidemic broadcast (node discovery, cache and content availability announcements)

## Consequences

**Positive:**

- Memory safety without GC pauses — predictable tail latency under concurrent delivery load
- Rust's async runtime (tokio) handles thousands of concurrent connections per edge node efficiently
- iroh bundles QUIC, NAT traversal, content-addressed transfer, and verified streaming — fewer moving parts than assembling these from separate libraries
- BLAKE3 is native to iroh's content model; blob IDs and transport layer use the same hash with no translation layer
- A single statically linked binary simplifies deployment of all node types with no runtime dependency management

**Negative:**

- Rust's compile times slow the development feedback loop compared to interpreted or JVM languages
- The team needs Rust proficiency; onboarding contributors takes longer
- iroh is a relatively young library; its APIs have changed across versions and may continue to do so
- Fewer off-the-shelf libraries for EVM interaction compared to TypeScript or Python — `alloy-rs` covers the gap but with less community documentation
