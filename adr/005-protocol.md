# ADR 005: Wire Protocol

**Date:** 2026-03-28
**Status:** Draft

## Context

Vault nodes, edge nodes, and clients communicate over QUIC connections established via iroh. We need to define what protocols run over those connections: how a client or edge node requests a blob and pays for it, how an edge node pulls a blob from a peer for free, and how content availability is broadcast across the network.

The protocol layer must be distinct from the transport layer (iroh/QUIC) and the payment layer (vouchers, channels) so each can evolve independently.

## Decision

Four protocols, each identified by an ALPN string:

| ALPN | Participants | Purpose |
| --- | --- | --- |
| `cdn/probe/v1` | any node ↔ any node | Latency and availability check before committing to a node |
| `cdn/client/v1` | payer ↔ delivering node | Blob delivery with payment vouchers (client→edge, edge→vault, client→vault) |
| `cdn/peer/v1` | edge node ↔ edge node | Unpaid peer blob pull between staked edge nodes |
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
  |     rate_per_mb, timestamp_us} |
```

`timestamp_us` is a requester-generated microsecond timestamp echoed back. RTT is `receive_time - timestamp_us`. `has_blob` confirms the node has the content. `rate_per_mb` lets the requester score candidates on both latency and price in a single round-trip.

The requester probes candidates from the routing table in parallel, waits up to 200ms, then selects the winner using a composite score: `rate_per_mb × rtt_ms` (lower is better). This applies to clients picking edge nodes, clients picking vault nodes directly, and edge nodes picking vault nodes for a cache miss pull.

### `cdn/client/v1` — paid delivery protocol

Used for all paid delivery: client→edge, client→vault (direct), and edge→vault (initial content pull).

```text
Payer                           Delivering node
  |                                |
  |--- StreamRequest {hash,       |
  |     channel_id, byte_offset} ->|
  |                                |
  |<-- StreamResponse {ok,        |
  |     rate_per_mb, total_bytes,  |
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

The delivering node advertises its `rate_per_mb` in `StreamResponse`. The payer accepts by sending the first voucher or disconnects and tries another node. No surprise pricing. The `redirect` field in `StreamResponse` is used when a node cannot serve — it contains the NodeId of another node that can (a vault node or a better-positioned edge node), never an external URL. The network is fully opaque.

`byte_offset` supports seek and resume: on failover, the requester reconnects to a different node and resumes from the last BLAKE3-verified byte.

The protocol is self-enforcing: payer stops sending vouchers → delivering node stops sending chunks; delivering node stops sending chunks → payer stops sending vouchers.

### `cdn/peer/v1` — unpaid edge-to-edge pull

Only between staked edge nodes. Vault nodes do not participate in this protocol — pulls from vault nodes are paid via `cdn/client/v1`.

```text
Requesting edge                 Serving edge
  |                                |
  |--- PeerPullRequest {hash,     |
  |     node_id, eth_addr, sig} -->|
  |                                | verify node_id is staked edge
  |<-- PeerPullResponse {ok |     |
  |     not_cached | reject} ------|
  |                                |
  |<====== blob chunks ============|
  |  (BLAKE3-verified, no voucher) |
```

The requesting edge signs `{hash, timestamp}` with its iroh private key. The serving edge verifies the requester is a staked edge node (not vault) in the on-chain registry before serving. No payment channel.

### Gossip — content availability

Content availability is broadcast over iroh-gossip on region-scoped topics (`cdn/region/{cc}/v1`) and a global topic (`cdn/global/v1`). Both vault nodes and edge nodes publish `CacheAnnounce` messages listing hashes they hold (max 500 entries) or a Bloom filter for large sets. Clients and edge nodes maintain a local routing table (`hash → Vec<NodeId>`) from received announcements. The routing table does not distinguish vault from edge nodes — the probe step determines cost.

### Serialization

All protocol messages use [postcard](https://docs.rs/postcard) — compact, no-std friendly, serde-based. Standard in the iroh ecosystem; avoids introducing a second serialization dependency alongside what iroh already uses internally.

## Consequences

**Positive:**

- ALPN separation means a single iroh `Endpoint` dispatches all connection types without ambiguity
- Probing in parallel before committing means no payment channel is opened with a slow or unresponsive node; applies equally to client→edge and edge→vault selection
- `cdn/client/v1` is reused for all paid delivery tiers — no separate protocol needed for edge→vault pulls
- `redirect` always points to a NodeId, never an external URL; the backend topology of vault nodes is fully hidden from the network
- The delivery protocol is self-enforcing — payment and data flow are coupled by design
- `byte_offset` in `StreamRequest` makes failover transparent; the requester resumes without restarting the stream

**Negative:**

- Probe RTT includes iroh's NAT traversal overhead on first connection, inflating the latency estimate. Reusing existing connections for probes gives a cleaner signal.
- A node under load can respond to probes quickly but deliver slowly — probe RTT is necessary but not sufficient. Reputation (separate system) provides the longer-term signal.
- The `cdn/client/v1` voucher cadence (1 MB) is coarser than iroh-blobs' internal chunk granularity (1024 bytes); the payment layer and transfer layer operate at different tick rates, requiring a buffering layer between them
- `cdn/peer/v1` registry lookup introduces a network dependency per connection; a stale local registry cache could reject a legitimate peer for up to 10 minutes after they stake
- Postcard has no schema evolution story — adding fields requires a new ALPN version (`cdn/client/v2`); version negotiation must be planned before the first breaking change
