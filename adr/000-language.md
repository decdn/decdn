# ADR 000: Language and Core Networking Stack

**Date:** 2026-03-28
**Status:** Accepted

## Context

The network has two participant roles:

- **Nodes** (providers) cache and deliver content. Some are configured with an origin backend (S3/R2/NFS/local disk) as the canonical source for specific content.
- **Clients** consume content.

Requirements:

- High-throughput, low-latency peer-to-peer blob transfer.
- Concurrent handling of many inbound connections per node.
- Memory safety without GC-induced latency spikes under load.
- QUIC transport with content-addressed verified transfer.
- Two self-contained binaries — a `decdn-node` daemon and a `decdn` CLI — that share a config schema and identity model (see [Appendix: deCDN Binaries](appendix-binaries.md#appendix-decdn-binaries--decdn-node--decdn-split)).

## Decision

Use **Rust** as the implementation language and **iroh** as the core networking library.

Components:

- `iroh::Endpoint` — QUIC peer-to-peer connectivity and ALPN protocol negotiation.
- `iroh-blobs` with the `fs-store` backend — content-addressed blob storage and verified transfer.

**iroh versioning policy.** Pin a caret requirement in `Cargo.toml` (e.g. `iroh = "1"`); the committed `Cargo.lock` is the true pin. Current baseline: **iroh 1.0**, iroh-blobs 0.103, iroh-metrics 1.0, noq-proto 1.0. Upgrade only on a deliberate `cargo update`: evaluate API compatibility, then record the new baseline here before merging.

**Direct `noq-proto` dependency.** The `node` crate depends on `noq-proto` directly. `noq-proto` is iroh's QUIC protocol crate. The node names `congestion::Bbr3Config` from it to select the BBR3 congestion controller on its QUIC endpoint. iroh re-exports the `ControllerFactory` trait but not the concrete controller configs, so the concrete type comes from `noq-proto`. This dependency must resolve to the same version that iroh pulls. One lock entry gives one crate instance, so the `ControllerFactory` trait object the node builds matches the one iroh's endpoint builder expects. A future iroh major that raises the `noq-proto` major needs the same bump on the node's direct pin, in the same `cargo update`.

## Consequences

### Positive

- Memory safety without GC pauses — predictable tail latency under concurrent delivery load.
- tokio handles thousands of concurrent connections per node efficiently.
- iroh bundles QUIC, NAT traversal, content-addressed transfer, and verified streaming — fewer moving parts than assembling separate libraries.
- BLAKE3 is native to iroh's content model; blob IDs and transport share one hash with no translation layer.
- Single-file binaries simplify deployment. Each binary carries all of its Rust dependencies. It is not fully static: the Linux targets are `*-unknown-linux-gnu` and link glibc dynamically. Linux release builds therefore use `cross`, which pins an old glibc and sets the support floor at glibc 2.31 (Debian 11, Ubuntu 20.04, RHEL 8). A native build on the CI runner links the runner's glibc instead — currently 2.39 — and the binary then fails to start on any older distribution. The release workflow asserts this floor with `objdump`.

### Negative

- Rust compile times slow the development feedback loop.
- The team needs Rust proficiency; onboarding takes longer.
- iroh is young and its APIs change across versions — mitigated by the pinning policy above.
- Fewer off-the-shelf EVM libraries than TypeScript or Python; `alloy-rs` covers the gap with less community documentation.
