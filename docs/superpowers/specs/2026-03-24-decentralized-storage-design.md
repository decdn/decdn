# Decentralized Audio Storage & Delivery Network

**Date:** 2026-03-24
**Status:** Draft
**Scope:** PoC — tens of nodes, proving the core protocol
**Target L2:** Arbitrum Sepolia (testnet). Production L2 TBD.
**iroh version:** Latest stable (0.35+). Key APIs: `iroh::Endpoint`, `iroh-blobs` (fs-store, verified streaming), `iroh-gossip` (topic-based epidemic broadcast).

## Non-Goals (out of scope for PoC)

- DRM or content protection
- Audio transcoding or adaptive bitrate
- Search, discovery, or recommendation (future work section below outlines the planned approach)
- Mobile or web clients
- Multi-chain support (single L2 only for PoC)
- Erasure coding (full replication only)

## Overview

A decentralized storage and delivery network optimized for audio streaming (decentralized Spotify use case). Built on iroh for p2p connectivity and content-addressed blob transfer, with cryptocurrency incentives on an EVM L2 for storage and delivery payments.

The system is application-specific blob storage first (audio streaming), with the flexibility to serve as general-purpose storage. Providers choose what they store (hybrid permissioning). The network is permissionless to join but providers must stake tokens.

## Glossary

| Term | Definition |
|------|-----------|
| **Blob** | A content-addressed byte sequence in iroh, identified by its BLAKE3 hash |
| **Chunk** | A 1024-byte segment of a blob used by iroh-blobs for verified streaming |
| **Hash sequence** | An ordered collection of blob hashes (iroh's equivalent of a directory/manifest) |
| **Voucher** | A signed off-chain payment message: `{channelId, cumulativeAmount, nonce, signature}` |
| **Manifest** | A hash sequence linking a track's audio blob + metadata blob. Its hash is the track ID |
| **ALPN** | Application-Layer Protocol Negotiation — identifies which protocol a QUIC connection uses |
| **Provider** | A node that stores and serves blobs, earning tokens |
| **Staking** | Locking tokens in a smart contract as collateral to join the network as a provider |

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

**Why a manifest?** The indirection pays off in multiple ways: (1) the same audio blob can appear in multiple albums/playlists without duplication, (2) metadata can be updated without changing the audio hash, (3) the manifest naturally extends to albums (multiple audio blobs in one hash sequence), and (4) it separates the "what is this track" lookup from the "give me the bytes" transfer.

### Replication

- Full replication with configurable factor (default 3)
- Uploader specifies replication factor and pays for initial placement
- Provider selection is reputation-weighted (prefer online, fast, geographically diverse nodes)
- Providers can accept or reject storage requests (hybrid permissioning)

**Replication maintenance protocol:** The uploader is responsible for maintaining the replication factor. The uploader periodically polls providers (via a lightweight `Ping` on the `store/v1` ALPN) to confirm they still hold the blob. If a provider is unreachable for >30 minutes, the uploader selects a new provider (reputation-weighted) and re-uploads. For PoC, this polling interval is 5 minutes. Future: delegate monitoring to a "replication manager" role that other nodes can fill for a fee.

### Content Routing

For PoC (tens of nodes), content routing uses a **full-table gossip approach**:

- A dedicated iroh-gossip topic (`content-routing/v1`) where providers announce what they store
- Every node maintains a local routing table: `blob_hash -> Vec<(NodeId, last_seen)>`
- Providers broadcast `ContentAnnounce {hash, available: bool}` messages when they store or drop a blob
- At PoC scale, every node holds the full table (feasible with tens of nodes and thousands of blobs)
- Clients query their local table to find providers, then connect directly via iroh
- Future: replace with a DHT (Kademlia) for larger networks

### Local Store

- Providers: iroh-blobs `fs-store` (persistent, backed by redb)
- Clients: iroh-blobs `mem-store` (ephemeral, playback buffering only)

## Incentive Layer

### Token Model

A single ERC-20 token on Arbitrum Sepolia (testnet). Three payment flows:

| Flow | Direction | Trigger |
|------|-----------|---------|
| Storage payment | Uploader -> Provider | Storing a track for a duration |
| Delivery payment | Listener -> Provider | Streaming audio chunks |
| Staking | Provider -> Contract | Joining the network |

### Placeholder Economics (PoC)

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Minimum provider stake | 1000 tokens | High enough for sybil resistance, low enough for PoC participation |
| Storage rate | 10 tokens / GB / month | Simple flat rate |
| Delivery rate | 0.001 tokens / MB streamed | ~0.004 tokens per 4-min song at 128kbps |
| Voucher interval | Every 256KB (1024 chunks) | Balance between payment granularity and signature overhead |
| Unbonding period | 7 days | Prevents stake-and-run |
| Channel dispute window | 1 hour | Short for PoC; production should be longer |
| Channel expiry timeout | 30 days | Funds reclaimable if provider vanishes |

Token supply and distribution are out of scope for PoC — use a freely mintable testnet token. For comprehensive economics including supply model, slashing schedule, provider unit economics, reputation scoring math, governance, and attack analysis, see the [Tokenomics Spec](./2026-03-24-tokenomics.md).

### Provider Staking

- Minimum stake deposit into a staking contract to join
- Two purposes: sybil resistance and slashing collateral
- Slashing conditions (PoC — narrow and provable):
  - Serving wrong data: uses an **optimistic fraud proof** model. The client submits `{blob_hash, chunk_index, received_bytes, expected_blake3_root}` to a slashing contract. The contract does NOT verify BLAKE3 on-chain (too expensive). Instead, the provider has a challenge window (24 hours) to counter the claim by providing the correct chunk. If they fail to respond, slash is executed. This is optimistic — assumes the client is honest unless the provider disputes. In production, challengers must post a **challenge bond** (50 tokens) that is forfeited if the provider successfully counters — see [Tokenomics Spec](./2026-03-24-tokenomics.md) Section 4 for details.
- Stake withdrawable after unbonding period (7 days)

### Payment Channels

Off-chain **unidirectional** payment channels (client/uploader pays provider):

- **Opening:** Client calls `openChannel(provider, deposit)` on L2. Funds locked.
- **Payments:** Client signs incrementing vouchers off-chain: `{channelId, amount, nonce, signature}`. No on-chain tx per chunk.
- **Closing:** Either party submits latest voucher to contract. After dispute window (1 hour for PoC), funds distributed.
- **Disputes:** Stale voucher submitted? Other party submits newer one during dispute window. Highest nonce wins.
- **Client liveness requirement:** Clients must monitor for channel close attempts and submit a newer voucher within the dispute window if a stale one is submitted. A client offline for >1 hour during a dispute may lose funds. For PoC this is acceptable; production should use a watchtower pattern.
- **Vouchers are cumulative** — each supersedes the previous (higher amount, higher nonce).

**Failover and channel pre-warming:** Clients maintain a small pool of pre-opened channels with top-ranked providers (by reputation). When streaming a track, the client has channels ready with 2-3 providers so failover doesn't require an on-chain transaction. Channel deposits can be small (enough for a few tracks) and topped up as needed. For PoC, the client pre-opens channels with all known providers at startup (feasible at tens of nodes).

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
- Each node aggregates locally using **interaction-weighted scoring**: reports from nodes that have completed more verified transactions (voucher settlements visible on-chain) carry more weight. A node with 1 settled channel carries less influence than one with 100. This makes self-promotion expensive — you need real economic activity, not just stake.
- A reporter's score for a provider is a weighted average: 70% local observations, 30% network gossip. Nodes always trust their own experience more than the crowd.

### Smart Contracts (PoC)

| Contract | Purpose |
|----------|---------|
| `StakingRegistry` | Provider registration, stake deposit/withdrawal, slashing |
| `PaymentChannel` | Channel open/close/dispute, voucher verification |
| `Token` | ERC-20 token (freely mintable on testnet) |

## Protocol & Wire Format

### ALPN Protocols

| ALPN | Purpose |
|------|---------|
| `storage-layer/stream/v1` | Audio streaming with payment vouchers |
| `storage-layer/store/v1` | Storage deals (upload, replication, status, ping) |
| iroh-blobs built-in ALPN | Actual blob transfer |
| iroh-gossip built-in ALPN | Reputation + content routing propagation |

### Streaming Protocol (`stream/v1`)

```
Client                          Provider
  |                                |
  |--- StreamRequest {hash,       |
  |     byte_offset} ------------>|
  |                                |
  |<-- StreamResponse {ok, meta,  |
  |     total_size} --------------|
  |                                |
  |  +--- chunk loop ----------+  |
  |  |<-- ChunkData {bytes} ---|  |
  |  |                         |  |
  |  |  (every N chunks:)      |  |
  |  |-- Voucher {sig,amt} --->|  |
  |  |<-- VoucherAck ----------|  |
  |  +-------------------------+  |
  |                                |
  |--- StreamEnd ----------------->|
```

- `StreamRequest` includes an optional `byte_offset` field for seek and resume. On failover, the client sends the byte offset of the last successfully verified chunk to the new provider.
- Built on iroh-blobs verified streaming — chunks are BLAKE3-verified automatically
- Payment wrapper intercepts every N chunks (default 1024 ~256KB) and expects a signed voucher
- Self-enforcing: client stops paying, provider stops sending; provider stops sending, client stops paying
- Voucher amount accounts for the offset — client only pays for bytes actually delivered from the offset onward

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
- `Ping` message on this ALPN for uploader to check provider liveness and blob availability

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

### CLI & Configuration

The `node` binary uses clap for CLI:

```
storage-layer-node [OPTIONS] <COMMAND>

Commands:
  provider    Run as a storage provider
  upload      Upload a track to the network
  stream      Stream a track by manifest hash
  status      Show node status, channels, and stored blobs

Options:
  --config <PATH>     Path to config file (default: ~/.storage-layer/config.toml)
  --data-dir <PATH>   Data directory for blob store (default: ~/.storage-layer/data)
  --log-level <LEVEL> Log level: trace, debug, info, warn, error (default: info)
```

Config file (`config.toml`):

```toml
[node]
mode = "provider"           # provider | client | uploader
listen_port = 4433
secret_key_path = "~/.storage-layer/secret.key"

[storage]
replication_factor = 3
max_storage_gb = 100        # provider only

[incentive]
l2_rpc_url = "https://sepolia-rollup.arbitrum.io/rpc"
staking_contract = "0x..."
payment_contract = "0x..."
token_contract = "0x..."
wallet_key_path = "~/.storage-layer/wallet.key"

[reputation]
min_stake_for_reports = 1000
```

## Error Handling & Edge Cases

### Network Failures

- **Provider offline mid-stream:** Client picks next-best provider from routing table (reputation-ranked), resumes from last verified byte offset using `StreamRequest.byte_offset`. Partial voucher with the failed provider is still settleable. Pre-warmed channel with fallback provider avoids on-chain latency.
- **Provider offline while storing:** Uploader detects via periodic ping (5-min interval). After 30 minutes unreachable, uploader selects new provider and re-uploads to maintain replication factor.
- **Client disappears mid-stream:** Provider stops sending. Last voucher is claimable. iroh cleans up QUIC connection.

### Payment Failures

- **Channel runs dry:** Provider sends `PaymentRequired`. Client tops up or opens new channel.
- **Stale voucher dispute:** Dispute window (1 hour) allows submitting newer voucher. Highest nonce wins. Clients must be online during dispute windows to protect themselves; production should use watchtowers.
- **Provider never settles:** Channel timeout (30 days). Client reclaims unsettled funds.

### Data Integrity

- **Corrupted data:** iroh-blobs BLAKE3 verified streaming rejects bad chunks at transport level. Provider marked faulty in reputation. Slashable via optimistic fraud proof (client submits claim, provider has 24h to counter with correct data).
- **Provider claims to store but doesn't:** Challenge-response spot checks. Failure harms reputation. On-chain slashing deferred past PoC.

### Reputation Gaming

- **Sybil flood:** Reports only from staked nodes. Cost scales with minimum stake.
- **Self-promotion:** Interaction-weighted scoring means you need real settled payment channels (on-chain verifiable) to gain influence. Self-promotion requires actual economic activity.
- **Collusion:** Mitigated by interaction weighting. Remaining risk accepted for PoC at small network scale.

### Graceful Degradation

- L2 down: streaming continues, vouchers settle when chain returns
- Gossip partitioned: fall back to local reputation
- No provider has track: clear `ContentNotFound` error

## Observability

- **Structured logging** via `tracing` crate (standard in iroh ecosystem). JSON output for machine consumption.
- **Metrics** via `prometheus` crate, exposed on a configurable HTTP port:
  - `blobs_stored_total`, `blobs_stored_bytes` — provider storage usage
  - `streams_active`, `streams_completed`, `streams_failed` — streaming activity
  - `vouchers_signed`, `vouchers_received` — payment activity
  - `reputation_reports_sent`, `reputation_reports_received` — gossip health
  - `channels_open`, `channels_settled` — payment channel lifecycle
- **Health endpoint** at `/health` on the metrics HTTP port — returns node status, peer count, and channel balances

## Testing Strategy

### Unit Tests (per crate)

- `protocol` — serialization round-trips, voucher signature verification, message validation
- `storage` — blob store/retrieve, replication factor enforcement, routing lookups
- `incentive` — payment channel state machine (open/pay/close), voucher nonce ordering, dispute logic
- `reputation` — score aggregation, rate limiting, report deduplication, interaction weighting

### Integration Tests (multi-node, in-process)

- Happy path: upload -> discover -> stream with payments
- Provider failure + failover mid-stream (resume from byte offset)
- Payment channel full lifecycle with dispute
- Reputation propagation via gossip
- Replication maintenance after provider loss

### Contract Tests

- Foundry tests against local Anvil chain
- Staking/unstaking, channel open/close/dispute, slashing via optimistic fraud proof

No end-to-end multi-machine tests for PoC.

## Future Work: Search & Discovery (Federated Indexers)

Not in PoC scope, but the planned approach for the next phase.

### Overview

Dedicated **indexer nodes** crawl the network's content announcements (already broadcast on the `content-routing/v1` gossip topic), build a searchable index of track metadata, and expose a query API. Multiple independent indexers can coexist — clients pick one or query several for redundancy.

This is analogous to how The Graph indexes blockchain data: indexers are semi-trusted infrastructure, but they cannot tamper with content (blobs are content-addressed), only withhold search results. Clients can cross-reference multiple indexers to detect omissions.

### How It Works

1. **Ingestion:** Indexer subscribes to `content-routing/v1` gossip topic. When a `ContentAnnounce` arrives, the indexer fetches the manifest and metadata blobs from a provider, extracts searchable fields (title, artist, genre, duration), and adds them to a local full-text search index (e.g., `tantivy`).
2. **Query API:** Indexer exposes a query protocol on a custom ALPN (`storage-layer/search/v1`). Clients connect via iroh and send search queries. Results are manifest hashes + metadata summaries, ranked by relevance.
3. **Incentive:** Indexers earn query fees via the same payment channel mechanism used for streaming. Clients pay per query (or per batch of queries). This makes indexing a self-sustaining role in the network.
4. **Registration:** Indexers register in the `StakingRegistry` with an "indexer" role. Staking provides sybil resistance and a slashing mechanism if an indexer is proven to serve fabricated results (metadata doesn't match the actual blob).

### Discovery & Recommendations

Built on top of the indexer infrastructure:

- **Trending:** Most-streamed tracks = most voucher settlements. Indexers can query on-chain settlement data or track gossip volume to rank by popularity.
- **Genre browsing:** Structured metadata fields in the metadata blob enable category-based filtering.
- **Playlists:** User-created hash sequences (same mechanism as track manifests) containing ordered lists of manifest hashes. Stored as blobs, shareable by hash.
- **Algorithmic recommendations:** Application-layer concern. Could run on indexer nodes or as a separate service consuming indexer APIs.

### PoC Bridge

During PoC (before indexers exist), clients use the full-table gossip approach already in the design: every node holds the complete content routing table and metadata can be fetched directly. This works at tens of nodes. The migration path to indexers is additive — indexers subscribe to the same gossip topic, just with better query capabilities.
