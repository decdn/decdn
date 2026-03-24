# Decentralized Audio Storage & Delivery Network

**Date:** 2026-03-24
**Status:** Draft
**Scope:** PoC — tens of nodes, proving the core protocol

## Overview

A decentralized storage and delivery network optimized for audio streaming (decentralized Spotify use case). Built on iroh for p2p connectivity and content-addressed blob transfer, with cryptocurrency incentives on an EVM L2 for storage and delivery payments.

The system is application-specific blob storage first (audio streaming), with the flexibility to serve as general-purpose storage. Providers choose what they store (hybrid permissioning). The network is permissionless to join but providers must stake tokens.

## Architecture

Three layers, cleanly separated:

```
+---------------------------------------------+
|  Application Layer (future)                 |
|  Catalog, search, playlists, client apps    |
+---------------------------------------------+
|  Incentive Layer                            |
|  Payment channels, staking, reputation      |
|  (EVM L2 smart contracts + off-chain sigs)  |
+---------------------------------------------+
|  Storage Layer                              |
|  iroh endpoints, blob store, replication,   |
|  content routing, streaming protocol        |
+---------------------------------------------+
```

### Node Types

All three types are the same binary with different configurations:

- **Provider node** — runs iroh endpoint + blob store, serves content, earns tokens. Must stake to join.
- **Client node** — lightweight iroh endpoint, streams content, pays per chunk. No staking required.
- **Uploader node** — pushes content to providers, sets replication factor, pays for initial storage.

### Key Design Decision

The storage layer and incentive layer are separate crates. The storage layer works without incentives (useful for testing, local dev, private deployments). The incentive layer wraps storage operations with payment logic. The `node` crate wires them together.

## Storage Layer

### Content Model

Audio files are stored as iroh blobs, content-addressed by BLAKE3 hash. A single track consists of:

- **Audio blob** — raw audio file (FLAC/Opus/MP3), stored as a single iroh blob
- **Metadata blob** — small JSON/CBOR blob with track info (title, artist, duration, format, bitrate)
- **Manifest blob** — iroh hash sequence (collection) linking audio + metadata, serves as the canonical track ID

The manifest hash is the canonical identifier. Clients request the manifest, get metadata to display, then stream the audio blob.

### Replication

- Full replication with configurable factor (default 3)
- Uploader specifies replication factor and pays for initial placement
- Provider selection is reputation-weighted (prefer online, fast, geographically diverse nodes)
- Providers can accept or reject storage requests (hybrid permissioning)
- Lazy replication maintenance — if a provider goes offline for too long, the network re-replicates to maintain the target factor

### Content Routing

- Providers announce content via a lightweight gossip protocol
- A distributed lookup table maps `blob_hash -> list of provider NodeIds`
- Clients query routing to find providers, then connect directly via iroh

### Local Store

- Providers: iroh-blobs `fs-store` (persistent, backed by redb)
- Clients: iroh-blobs `mem-store` (ephemeral, playback buffering only)

## Incentive Layer

### Token Model

A single ERC-20 token on an EVM L2 (e.g., Arbitrum, Base). Three payment flows:

| Flow | Direction | Trigger |
|------|-----------|---------|
| Storage payment | Uploader -> Provider | Storing a track for a duration |
| Delivery payment | Listener -> Provider | Streaming audio chunks |
| Staking | Provider -> Contract | Joining the network |

### Provider Staking

- Minimum stake deposit into a staking contract to join
- Two purposes: sybil resistance and slashing collateral
- Slashing conditions (PoC — narrow and provable):
  - Serving wrong data (BLAKE3 hash mismatch, client submits proof on-chain)
- Stake withdrawable after unbonding period (7 days)

### Payment Channels

Off-chain bidirectional payment channels between clients/uploaders and providers:

- **Opening:** Client calls `openChannel(provider, deposit)` on L2. Funds locked.
- **Payments:** Client signs incrementing vouchers off-chain: `{channelId, amount, nonce, signature}`. No on-chain tx per chunk.
- **Closing:** Either party submits latest voucher to contract. After dispute window (1 hour for PoC), funds distributed.
- **Disputes:** Stale voucher submitted? Other party submits newer one during dispute window. Highest nonce wins.
- **Chunk-to-payment ratio:** One voucher per 1024 chunks (~256KB). Configurable.
- **Vouchers are cumulative** — each supersedes the previous (higher amount, higher nonce).

### Storage Payments

- Uploader opens a channel per provider
- Pays upfront for storage duration (e.g., 30 days)
- Flat rate per GB per month for PoC (no auction)
- Renewal is explicit — top up or provider garbage-collects after expiry

### Delivery Payments

- Listener opens channel to streaming provider
- Pays per chunk via signed vouchers
- Channel stays open across multiple tracks/sessions
- Top up when low, or open a new channel

### Reputation System

Three tiers, using iroh-gossip for network-wide propagation:

| Tier | Mechanism |
|------|-----------|
| Local | Direct observations (delivery speed, uptime, data correctness) |
| Network | Gossip-propagated signed reports via iroh-gossip |
| On-chain | Stake weight for sybil resistance, slashing for provable faults |

**Gossip-based reputation:**

- Dedicated gossip topic (`reputation/v1`) — all nodes subscribe
- After interacting with a provider, a node broadcasts a signed reputation report:

```rust
struct ReputationReport {
    provider: NodeId,
    reporter: NodeId,
    metrics: ReportMetrics,
    timestamp: u64,
    signature: Signature,
}

struct ReportMetrics {
    delivery_speed: Option<u32>,  // bytes/sec
    uptime_observed: Option<bool>,
    data_correct: Option<bool>,
}
```

- Reports only accepted from staked nodes
- Rate-limited: max 1 report per (reporter, provider) per hour
- Each node aggregates locally, weighting by reporter stake and trust

### Smart Contracts (PoC)

| Contract | Purpose |
|----------|---------|
| `StakingRegistry` | Provider registration, stake deposit/withdrawal, slashing |
| `PaymentChannel` | Channel open/close/dispute, voucher verification |
| `Token` | ERC-20 token (or use existing testnet token) |

## Protocol & Wire Format

### ALPN Protocols

| ALPN | Purpose |
|------|---------|
| `storage-layer/stream/v1` | Audio streaming with payment vouchers |
| `storage-layer/store/v1` | Storage deals (upload, replication, status) |
| iroh-blobs built-in ALPN | Actual blob transfer |
| iroh-gossip built-in ALPN | Reputation report propagation |

### Streaming Protocol (`stream/v1`)

```
Client                          Provider
  |                                |
  |--- StreamRequest {hash} ------>|
  |                                |
  |<-- StreamResponse {ok, meta} --|
  |                                |
  |  +--- chunk loop ----------+   |
  |  |<-- ChunkData {bytes} ---|   |
  |  |                         |   |
  |  |  (every N chunks:)      |   |
  |  |-- Voucher {sig,amt} --->|   |
  |  |<-- VoucherAck ----------|   |
  |  +-------------------------+   |
  |                                |
  |--- StreamEnd ----------------->|
```

- Built on iroh-blobs verified streaming — chunks are BLAKE3-verified automatically
- Payment wrapper intercepts every N chunks (default 1024 ~256KB) and expects a signed voucher
- Self-enforcing: client stops paying, provider stops sending; provider stops sending, client stops paying

### Storage Protocol (`store/v1`)

```
Uploader                        Provider
  |                                |
  |--- StoreRequest {hash,        |
  |     duration, replication,     |
  |     payment_proof} ---------->|
  |                                |
  |<-- StoreResponse {accept/     |
  |     reject, terms} -----------|
  |                                |
  |  (if accepted, blob transfer   |
  |   via iroh-blobs ALPN)         |
  |                                |
  |--- iroh-blobs transfer ------>|
  |                                |
  |<-- StoreConfirm {stored,      |
  |     expires_at} --------------|
```

- Provider can reject based on content policy, capacity, or price
- Blob transfer uses iroh-blobs natively after acceptance
- Provider stores expiry metadata; garbage collects after expiry unless renewed

### Serialization

All protocol messages use postcard (compact, no-std friendly, serde-based) — standard in the iroh ecosystem.

## Crate Structure

```
storage-layer/
├── Cargo.toml                    # workspace root
├── crates/
│   ├── node/                     # Binary — CLI entry, config, wiring
│   ├── protocol/                 # Shared types, wire format, messages
│   ├── storage/                  # Storage engine wrapping iroh-blobs
│   ├── incentive/                # Payment channels, staking, vouchers
│   ├── reputation/               # Gossip-based reputation system
│   └── contracts/                # Solidity contracts + Foundry
├── tests/                        # Integration tests
└── docs/
```

### Dependency Chain

```
node -> storage, incentive, reputation, protocol
storage -> protocol, iroh, iroh-blobs
incentive -> protocol, alloy
reputation -> protocol, iroh-gossip
protocol -> serde, postcard, iroh types (minimal)
```

`protocol` is the leaf crate with minimal dependencies. Everything depends on it; it depends on almost nothing.

## Error Handling & Edge Cases

### Network Failures

- **Provider offline mid-stream:** Client picks next-best provider from routing table (reputation-ranked), resumes from last verified chunk. Partial voucher is still settleable.
- **Provider offline while storing:** Replication monitor detects gap after threshold (30 min for PoC). Triggers re-replication to maintain factor.
- **Client disappears mid-stream:** Provider stops sending. Last voucher is claimable. iroh cleans up QUIC connection.

### Payment Failures

- **Channel runs dry:** Provider sends `PaymentRequired`. Client tops up or opens new channel.
- **Stale voucher dispute:** Dispute window allows submitting newer voucher. Highest nonce wins.
- **Provider never settles:** Channel timeout (30 days). Client reclaims unsettled funds.

### Data Integrity

- **Corrupted data:** iroh-blobs BLAKE3 verified streaming rejects bad chunks at transport level. Provider marked faulty in reputation. Slashable if provable on-chain.
- **Provider claims to store but doesn't:** Challenge-response spot checks. Failure harms reputation. On-chain slashing deferred past PoC.

### Reputation Gaming

- **Sybil flood:** Reports only from staked nodes. Cost scales with stake.
- **Self-promotion:** Weighted by reporter history and interaction diversity.
- **Collusion:** Accepted risk for PoC at small network scale.

### Graceful Degradation

- L2 down: streaming continues, vouchers settle when chain returns
- Gossip partitioned: fall back to local reputation
- No provider has track: clear `ContentNotFound` error

## Testing Strategy

### Unit Tests (per crate)

- `protocol` — serialization round-trips, voucher signature verification, message validation
- `storage` — blob store/retrieve, replication factor enforcement, routing lookups
- `incentive` — payment channel state machine (open/pay/close), voucher nonce ordering, dispute logic
- `reputation` — score aggregation, rate limiting, report deduplication

### Integration Tests (multi-node, in-process)

- Happy path: upload -> discover -> stream with payments
- Provider failure + failover mid-stream
- Payment channel full lifecycle with dispute
- Reputation propagation via gossip
- Replication maintenance after provider loss

### Contract Tests

- Foundry tests against local Anvil chain
- Staking/unstaking, channel open/close/dispute, slashing

No end-to-end multi-machine tests for PoC.
