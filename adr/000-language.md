# ADR 000: Language and Core Networking Stack

**Date:** 2026-03-28
**Status:** Draft

## Context

We are building a decentralized CDN where storage remains centralized (S3/R2/object store) and delivery is handled by incentivized edge nodes that cache and serve content to clients and each other. The network requires:

- High-throughput, low-latency blob transfer between edge nodes and to clients over peer-to-peer connections
- Concurrent handling of many inbound connections per edge node
- Safe memory management without a garbage collector introducing latency spikes under load
- A QUIC-based transport with content-addressed verified transfer
- A single binary deployable as an edge node or client depending on configuration

## Decision

Use **Rust** as the implementation language and **iroh** (0.35+) as the core networking library.

Specifically:
- `iroh::Endpoint` for QUIC-based peer-to-peer connectivity and ALPN protocol negotiation
- `iroh-blobs` with `fs-store` backend for content-addressed blob storage and verified transfer
- `iroh-gossip` for topic-based epidemic broadcast (edge node discovery, cache availability announcements)

## Consequences

**Positive:**

- Memory safety without GC pauses — predictable tail latency under concurrent delivery load
- Rust's async runtime (tokio) handles thousands of concurrent connections per edge node efficiently
- iroh bundles QUIC, NAT traversal, content-addressed transfer, and verified streaming — fewer moving parts than assembling these from separate libraries
- BLAKE3 is native to iroh's content model; blob IDs and transport layer use the same hash with no translation layer
- A single statically linked binary simplifies edge node deployment with no runtime dependency management

**Negative:**

- Rust's compile times slow the development feedback loop compared to interpreted or JVM languages
- The team needs Rust proficiency; onboarding contributors takes longer
- iroh is a relatively young library; its APIs have changed across versions and may continue to do so
- Fewer off-the-shelf libraries for EVM interaction compared to TypeScript or Python — `alloy-rs` covers the gap but with less community documentation
