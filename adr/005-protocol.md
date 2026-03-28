# ADR 005: Wire Protocol

**Date:** 2026-03-28
**Status:** Draft

## Context

Edge nodes communicate with clients and with each other over QUIC connections established via iroh. We need to define what protocols run over those connections: how a client requests a blob and pays for it, how an edge node requests a blob from a peer, and how cache state is broadcast across the network.

The protocol layer must be distinct from the transport layer (iroh/QUIC) and the payment layer (vouchers, channels) so each can evolve independently.

## Decision

Four protocols, each identified by an ALPN string:

| ALPN | Participants | Purpose |
|------|-------------|---------|
| `cdn/probe/v1` | client ↔ edge node | Latency and availability check before committing to a node |
| `cdn/client/v1` | client ↔ edge node | Blob delivery with payment vouchers |
| `cdn/peer/v1` | edge node ↔ edge node | Unpaid peer blob pull |
| iroh-gossip built-in | all nodes | Cache announcements, node discovery |

**`cdn/probe/v1` — latency probe**

Before opening a payment channel or sending a `StreamRequest`, a client probes candidate nodes to measure round-trip latency and confirm the node has the requested blob cached:

```
Client                          Edge node
  |                                |
  |--- ProbeRequest {hash,        |
  |     timestamp_us} ------------>|
  |                                |
  |<-- ProbeResponse {has_blob,   |
  |     rate_per_mb, timestamp_us} |
```

`timestamp_us` is a client-generated microsecond timestamp echoed back in the response. RTT is `receive_time - timestamp_us`. `has_blob` tells the client whether the node has the content cached (no point picking a node that will have to do a slower origin pull). `rate_per_mb` is included so the client can score candidates on both latency and price in a single round-trip.

The client probes its top candidates from the routing table in parallel, waits up to 200ms, then selects the winner using a composite score: `rate_per_mb × rtt_ms` (lower is better). Nodes that don't respond within the timeout are dropped from consideration for this request.

Probes are connectionless from a payment perspective — no channel is opened and no voucher is involved. The QUIC connection is opened, the probe exchange completes, and the connection is either reused for a subsequent `StreamRequest` or closed.

**`cdn/client/v1` — delivery protocol**

```
Client                          Edge node
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

The edge node advertises its `rate_per_mb` in `StreamResponse`. The client accepts by sending the first voucher or disconnects and tries another node. No surprise pricing. The `redirect` field in `StreamResponse` is used when the edge node chooses not to serve (e.g., `pull_through: false` config) — it points the client to the origin gateway.

`byte_offset` supports seek and resume: on failover, the client reconnects to a different edge node and resumes from the last BLAKE3-verified byte. The voucher amount covers only the bytes delivered from the offset onward.

The protocol is self-enforcing: client stops sending vouchers → edge node stops sending chunks; edge node stops sending chunks → client stops sending vouchers.

**`cdn/peer/v1` — peer pull protocol**

```
Requesting edge                 Serving edge
  |                                |
  |--- PeerPullRequest {hash,     |
  |     node_id, eth_addr, sig} -->|
  |                                | verify node_id is staked
  |<-- PeerPullResponse {ok |     |
  |     not_cached | reject} ------|
  |                                |
  |<====== blob chunks ============|
  |  (BLAKE3-verified, no voucher) |
```

The requesting edge authenticates by signing `{hash, timestamp}` with its iroh private key. The serving edge verifies the requester appears in the on-chain staking registry (cached locally, refreshed every 10 minutes) before serving. No payment channel; peer pulls are unpaid.

**Gossip — cache announcements**

Cache state is broadcast over iroh-gossip on region-scoped topics (`cdn/region/{cc}/v1`) and a global topic (`cdn/global/v1`). Nodes publish `CacheAnnounce` messages listing cached hashes (max 500 entries) or a Bloom filter for large caches. Both clients and edge nodes maintain a local routing table (`hash → Vec<NodeId>`) from received announcements.

**Serialization**

All protocol messages use [postcard](https://docs.rs/postcard) — compact, no-std friendly, serde-based. Standard in the iroh ecosystem; avoids introducing a second serialization dependency alongside what iroh already uses internally.

## Consequences

**Positive:**

- ALPN separation means a single iroh `Endpoint` can accept client, probe, and peer connections without ambiguity; connection handling is dispatched by ALPN at the transport level
- Probing candidates in parallel before committing means the client never opens a payment channel with a slow or unresponsive node; the 200ms probe window adds negligible perceived latency relative to stream startup time
- Combining `has_blob` and `rate_per_mb` in the probe response means node selection requires only one round-trip, not two separate exchanges
- The delivery protocol is self-enforcing without any additional coordination — payment and data flow are coupled by design
- `byte_offset` in `StreamRequest` makes failover transparent to the application layer; the client resumes without restarting the stream
- Postcard is already used by iroh internals; one serialization format across the stack

**Negative:**

- Probe RTT measures QUIC connection setup + one message exchange; it includes iroh's NAT traversal overhead on the first connection to a node, which may inflate the latency estimate relative to steady-state delivery RTT. Clients should reuse existing connections for probes where possible to get a cleaner signal.
- A node under load could respond to probes quickly but deliver slowly — probe RTT is a necessary but not sufficient signal for sustained throughput. The reputation score (separate system) provides the longer-term quality signal.
- The `cdn/client/v1` voucher cadence (1 MB) is coarser than iroh-blobs' internal chunk granularity (1024 bytes); this means the payment layer and the transfer layer operate at different tick rates, requiring a buffering layer between them
- Peer authentication via on-chain registry lookup introduces a network dependency per connection; a stale local cache of the registry could cause legitimate peers to be rejected for up to 10 minutes after they stake
- Postcard has no schema evolution story — adding fields to messages requires a new ALPN version (`cdn/client/v2`) rather than backward-compatible field addition; this is acceptable for now but means version negotiation must be planned before the first breaking change
