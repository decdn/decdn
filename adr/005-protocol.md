# ADR 005: Wire Protocol

**Date:** 2026-03-28
**Status:** Draft

## Context

Nodes and clients communicate over QUIC connections established via iroh. We need to define what protocols run over those connections: how a client or node requests a blob and pays for it, and how content availability is broadcast across the network.

The protocol layer must be distinct from the transport layer (iroh/QUIC) and the payment layer (vouchers, channels) so each can evolve independently.

## Decision

Three protocols, each identified by an ALPN string:

| ALPN | Participants | Purpose |
| --- | --- | --- |
| `cdn/probe/v1` | any node ↔ any node | Latency and availability check before committing to a node |
| `cdn/client/v1` | payer ↔ delivering node | Paid blob delivery with payment vouchers (client→node, node→node on cache miss) |
| iroh-gossip built-in | all nodes | Content availability announcements, node discovery |

### `cdn/probe/v1` — latency probe

Before opening a payment channel or sending a `StreamRequest`, a node probes candidates to measure round-trip latency and confirm the node has the blob:

```text
Requester                       Candidate node
  |                                |
  |--- ProbeRequest {hash,        |
  |     timestamp_us} ------------>|
  |                                |
  |<-- ProbeResponse {has_blob,   |
  |     rate_per_mb, timestamp_us, |
  |     signature} ----------------|
```

`timestamp_us` is a requester-generated microsecond timestamp echoed back. RTT is `receive_time - timestamp_us`. `has_blob` confirms the node has the content. `rate_per_mb` lets the requester score candidates on both latency and price in a single round-trip.

`signature` is the candidate node's iroh private key signature over `{hash, has_blob, rate_per_mb, timestamp_us}`. This makes the probe response cryptographically attributable and enables two slashing mechanisms: (1) **phantom announcement slashing** — if `has_blob: true` but the node subsequently fails to deliver, the signed probe response is evidence; (2) **rate manipulation slashing** — if the probe response rate differs from the subsequent `StreamResponse` rate within 30 seconds, both signed messages constitute evidence of bait-and-switch. See ADR 004 for slashing details.

The requester probes candidates from the routing table in parallel, waits up to 200ms, then selects the winner using a composite score: `rate_per_mb × rtt_ms` (lower is better). This applies to clients picking nodes and nodes picking peers for a cache miss pull.

### `cdn/client/v1` — paid delivery protocol

Used for all paid delivery: client→node and node→node (cache miss pull from an origin-backed or cached node).

```text
Payer                           Delivering node
  |                                |
  |--- StreamRequest {hash,       |
  |     channel_id, byte_offset} ->|
  |                                |
  |<-- StreamResponse {ok,        |
  |     rate_per_mb, total_bytes,  |
  |     timestamp_us, signature,   |
  |     redirect?} ---------------|
  |                                |
  |  +--- chunk loop -----------+ |
  |  |<-- ChunkData {bytes} ----| |
  |  |   (every 1 MB:)          | |
  |  |--- Voucher {sig, amt} -->| |
  |  |<-- VoucherAck -----------| |
  |  +--------------------------+ |
  |                                |
  |--- StreamEnd ----------------->|
```

The delivering node advertises its `rate_per_mb` in `StreamResponse`. The payer accepts by sending the first voucher or disconnects and tries another node. No surprise pricing. `timestamp_us` and `signature` (node's iroh key signs all security-relevant fields: `{hash, ok, rate_per_mb, total_bytes, channel_id, timestamp_us, redirect}`) make the response cryptographically binding. Signing the full response prevents a malicious party from altering unsigned fields while reusing a valid signature — in particular, `ok` is needed for phantom announcement evidence (proving a node signed `ok: false` after claiming `has_blob: true` in a probe), and `redirect` ensures a node cannot silently alter routing without accountability. A rate mismatch between a signed `ProbeResponse` and a signed `StreamResponse` within 30 seconds is slashable evidence of rate manipulation. The `redirect` field in `StreamResponse` is used when a node cannot serve — it contains the NodeId of another node that can, never an external URL. The network is fully opaque.

`byte_offset` supports seek and resume: on failover, the requester reconnects to a different node and resumes from the last BLAKE3-verified byte.

The protocol is self-enforcing: payer stops sending vouchers → delivering node stops sending chunks; delivering node stops sending chunks → payer stops sending vouchers.

### Gossip — content availability

Content availability is broadcast over iroh-gossip on region-scoped topics (`cdn/region/{cc}/v1`) and a global topic (`cdn/global/v1`). All staked nodes publish `CacheAnnounce` messages listing hashes they hold (max 500 entries) or a Bloom filter for large sets. Clients and nodes maintain a local routing table (`hash → Vec<NodeId>`) from received announcements. The probe step determines cost and latency for each candidate.

### Serialization

All protocol messages use [postcard](https://docs.rs/postcard) — compact, no-std friendly, serde-based. Standard in the iroh ecosystem; avoids introducing a second serialization dependency alongside what iroh already uses internally.

## Consequences

**Positive:**

- ALPN separation means a single iroh `Endpoint` dispatches all connection types without ambiguity
- Probing in parallel before committing means no payment channel is opened with a slow or unresponsive node
- `cdn/client/v1` is reused for all paid delivery — no separate protocol needed for node→node pulls
- `redirect` always points to a NodeId, never an external URL; the backend topology of origin-backed nodes is fully hidden from the network
- The delivery protocol is self-enforcing — payment and data flow are coupled by design
- `byte_offset` in `StreamRequest` makes failover transparent; the requester resumes without restarting the stream

**Negative:**

- Probe RTT includes iroh's NAT traversal overhead on first connection, inflating the latency estimate. Reusing existing connections for probes gives a cleaner signal.
- A node under load can respond to probes quickly but deliver slowly — probe RTT is necessary but not sufficient. Reputation (separate system) provides the longer-term signal.
- The `cdn/client/v1` voucher cadence (1 MB) is coarser than iroh-blobs' internal chunk granularity (1024 bytes); the payment layer and transfer layer operate at different tick rates, requiring a buffering layer between them
- Postcard has no schema evolution story — adding fields requires a new ALPN version (`cdn/client/v2`); version negotiation must be planned before the first breaking change
