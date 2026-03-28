# Decentralized Content Storage & Delivery Network

**Date:** 2026-03-24
**Status:** Draft
**Scope:** PoC — tens of nodes, proving the core protocol
**Target L2:** Arbitrum Sepolia (testnet). Production L2 TBD.
**iroh version:** Latest stable (0.35+). Key APIs: `iroh::Endpoint`, `iroh-blobs` (fs-store, verified streaming), `iroh-gossip` (topic-based epidemic broadcast).

## Non-Goals (out of scope for PoC)

- DRM or content protection
- Content transcoding or adaptive format conversion
- Search, discovery, or recommendation (future work section below outlines the planned approach)
- Mobile or web clients
- Multi-chain support (single L2 only for PoC)
- Erasure coding (full replication only)

## Overview

A decentralized storage and delivery network for content storage and delivery. Built on iroh for p2p connectivity and content-addressed blob transfer, with cryptocurrency incentives on an EVM L2 for storage and delivery payments.

The system is general-purpose blob storage. Providers choose what they store (hybrid permissioning). The network is permissionless to join but providers must stake tokens.

## Glossary

| Term | Definition |
|------|-----------|
| **Blob** | A content-addressed byte sequence in iroh, identified by its BLAKE3 hash |
| **Chunk** | A 1024-byte segment of a blob used by iroh-blobs for verified streaming |
| **Hash sequence** | An ordered collection of blob hashes (iroh's equivalent of a directory/manifest) |
| **Voucher** | A signed off-chain payment message: `{channelId, cumulativeAmount, nonce, signature}` |
| **Manifest** | An optional application-level hash sequence grouping related blobs |
| **ALPN** | Application-Layer Protocol Negotiation — identifies which protocol a QUIC connection uses |
| **Node** | A participant in the network; either a provider (staked, stores/serves blobs) or a client (lightweight, consumes content) |
| **Provider** | A staked node that stores and serves blobs, earning payment |
| **Staking** | Locking tokens in a smart contract as collateral to join the network as a provider |

## Architecture

Two layers, cleanly separated:

```
+---------------------------------------------+
|  Application Layer (future)                 |
|  Catalog, search, collections, client apps  |
+---------------------------------------------+
|  Storage + Incentive Layer                  |
|  iroh endpoints, blob store, replication,   |
|  content routing, delivery protocol,        |
|  payment channels, staking, reputation      |
|  (EVM L2 smart contracts + off-chain sigs)  |
+---------------------------------------------+
```

### Node Types

Both types are the same binary with different configurations:

- **Provider node** — runs iroh endpoint + blob store, serves and stores content, earns payment. Can also upload content to other nodes. Must stake to join.
- **Client node** — lightweight iroh endpoint, fetches content, pays per chunk. Can upload content to providers. No staking required.

### Key Design Decision

The storage layer and incentive layer are separate crates. The storage layer works without incentives (useful for testing, local dev, private deployments). The incentive layer wraps storage operations with payment logic. The `node` crate wires them together.

## Storage Layer

### Content Model

Blobs are arbitrary byte sequences, content-addressed by BLAKE3 hash. The protocol imposes no mandatory structure — applications define their own organization on top of raw blobs.

- **Content blob** — raw file bytes, format-agnostic
- **Metadata blob** — optional small JSON/CBOR blob with content metadata (application-defined fields)

Applications may optionally create manifest blobs (hash sequences) to group related blobs and metadata. For example, an application could bundle a content blob and its metadata blob into a single hash sequence for convenient retrieval. Hash sequences can also represent collections (e.g., albums, datasets, archives). The protocol does not require manifests — individual blobs are independently addressable and retrievable by hash.

### Replication

- Full replication with configurable factor (default 3)
- The uploading node specifies replication factor and pays for initial placement
- Provider selection is reputation-weighted (prefer online, fast, geographically diverse nodes)
- Providers can accept or reject storage requests (hybrid permissioning)

**Replication maintenance protocol:** The uploading node is responsible for maintaining the replication factor. It periodically polls providers (via a lightweight `Ping` on the `store/v1` ALPN) to confirm they still hold the blob. If a provider is unreachable for >30 minutes, the uploading node selects a new provider (reputation-weighted) and re-uploads. For PoC, this polling interval is 5 minutes. Future: delegate monitoring to a "replication manager" role that other nodes can fill for a fee.

### Content Routing

For PoC (tens of nodes), content routing uses a **full-table gossip approach**:

- A dedicated iroh-gossip topic (`content-routing/v1`) where providers announce what they store
- Every node maintains a local routing table: `blob_hash -> Vec<(NodeId, last_seen)>`
- Providers broadcast `ContentAnnounce {hash, available: bool}` messages when they store or drop a blob
- At PoC scale, every node holds the full table (feasible with tens of nodes and thousands of blobs)
- Clients query their local table to find providers, then connect directly via iroh
- Future: replace with a DHT (Kademlia) for larger networks

### Local Store

- Provider nodes: iroh-blobs `fs-store` (persistent, backed by redb)
- Client nodes: iroh-blobs `mem-store` (ephemeral, buffering only)

## Incentive Layer

### Token Model

A single ERC-20 token on Arbitrum Sepolia (testnet). Three payment flows:

| Flow | Direction | Trigger |
|------|-----------|---------|
| Storage payment | Uploading node -> Provider | Storing a blob for a duration |
| Delivery payment | Client -> Provider | Delivering content chunks |
| Staking | Provider -> Contract | Joining the network |

### Placeholder Economics (PoC)

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Minimum provider stake | 1000 tokens | High enough for sybil resistance, low enough for PoC participation |
| Storage rate | 10 tokens / GB / month | Simple flat rate |
| Delivery rate | 0.001 tokens / MB delivered | Simple per-MB rate |
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

Off-chain **unidirectional** payment channels (paying node pays provider):

- **Opening:** Client calls `openChannel(provider, deposit)` on L2. Funds locked.
- **Payments:** Client signs incrementing vouchers off-chain: `{channelId, amount, nonce, signature}`. No on-chain tx per chunk.
- **Closing:** Either party submits latest voucher to contract. After dispute window (1 hour for PoC), funds distributed.
- **Disputes:** Stale voucher submitted? Other party submits newer one during dispute window. Highest nonce wins.
- **Client liveness requirement:** Clients must monitor for channel close attempts and submit a newer voucher within the dispute window if a stale one is submitted. A client offline for >1 hour during a dispute may lose funds. For PoC this is acceptable; production should use a watchtower pattern.
- **Vouchers are cumulative** — each supersedes the previous (higher amount, higher nonce).

**Failover and channel pre-warming:** Clients maintain a small pool of pre-opened channels with top-ranked providers (by reputation). When fetching a blob, the client has channels ready with 2-3 providers so failover doesn't require an on-chain transaction. Channel deposits can be small (enough for a few blobs) and topped up as needed. For PoC, the client pre-opens channels with all known providers at startup (feasible at tens of nodes).

**Production: stablecoin channels.** For production, payments transition from the native token to stablecoins (USDC) via a `StablePaymentChannel` contract. This eliminates provider revenue volatility while the native token retains utility for staking and governance. See the [Stablecoin Payments Spec](./2026-03-24-stablecoin-payments.md) for the dual-currency design, migration path, and token utility preservation mechanisms.

### Storage Payments

- Uploading node opens a channel per provider
- Pays upfront for storage duration (e.g., 30 days)
- Flat rate per GB per month for PoC (no auction)
- Renewal is explicit — top up or provider garbage-collects after expiry

### Delivery Payments

- Client opens channel to provider
- Pays per chunk via signed vouchers
- Channel stays open across multiple blobs/sessions
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
| `storage-layer/stream/v1` | Content delivery with payment vouchers |
| `storage-layer/store/v1` | Storage deals (upload, replication, status, ping) |
| iroh-blobs built-in ALPN | Actual blob transfer |
| iroh-gossip built-in ALPN | Reputation + content routing propagation |

### Delivery Protocol (`stream/v1`)

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
- Built on iroh-blobs verified delivery — chunks are BLAKE3-verified automatically
- Payment wrapper intercepts every N chunks (default 1024 ~256KB) and expects a signed voucher
- Self-enforcing: client stops paying, provider stops sending; provider stops sending, client stops paying
- Voucher amount accounts for the offset — client only pays for bytes actually delivered from the offset onward

### Storage Protocol (`store/v1`)

```
Node (uploading)                Provider
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
- `Ping` message on this ALPN for the uploading node to check provider liveness and blob availability

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
  run         Run the node (provider or client, per config)
  upload      Upload content to the network
  stream      Fetch a blob by hash
  status      Show node status, channels, and stored blobs

Options:
  --config <PATH>     Path to config file (default: ~/.storage-layer/config.toml)
  --data-dir <PATH>   Data directory for blob store (default: ~/.storage-layer/data)
  --log-level <LEVEL> Log level: trace, debug, info, warn, error (default: info)
```

Config file (`config.toml`):

```toml
[node]
mode = "provider"           # provider | client
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

- **Provider offline mid-delivery:** Client picks next-best provider from routing table (reputation-ranked), resumes from last verified byte offset using `StreamRequest.byte_offset`. Partial voucher with the failed provider is still settleable. Pre-warmed channel with fallback provider avoids on-chain latency.
- **Provider offline while storing:** Uploading node detects via periodic ping (5-min interval). After 30 minutes unreachable, it selects a new provider and re-uploads to maintain replication factor.
- **Client disappears mid-delivery:** Provider stops sending. Last voucher is claimable. iroh cleans up QUIC connection.

### Payment Failures

- **Channel runs dry:** Provider sends `PaymentRequired`. Client tops up or opens new channel.
- **Stale voucher dispute:** Dispute window (1 hour) allows submitting newer voucher. Highest nonce wins. Clients must be online during dispute windows to protect themselves; production should use watchtowers.
- **Provider never settles:** Channel timeout (30 days). Client reclaims unsettled funds.

### Data Integrity

- **Corrupted data:** iroh-blobs BLAKE3 verified delivery rejects bad chunks at transport level. Provider marked faulty in reputation. Slashable via optimistic fraud proof (client submits claim, provider has 24h to counter with correct data).
- **Provider claims to store but doesn't:** Challenge-response spot checks. Failure harms reputation. On-chain slashing deferred past PoC.

### Reputation Gaming

- **Sybil flood:** Reports only from staked nodes. Cost scales with minimum stake.
- **Self-promotion:** Interaction-weighted scoring means you need real settled payment channels (on-chain verifiable) to gain influence. Self-promotion requires actual economic activity.
- **Collusion:** Mitigated by interaction weighting. Remaining risk accepted for PoC at small network scale.

### Graceful Degradation

- L2 down: delivery continues, vouchers settle when chain returns
- Gossip partitioned: fall back to local reputation
- No provider has blob: clear `ContentNotFound` error

## Observability

- **Structured logging** via `tracing` crate (standard in iroh ecosystem). JSON output for machine consumption.
- **Metrics** via `prometheus` crate, exposed on a configurable HTTP port:
  - `blobs_stored_total`, `blobs_stored_bytes` — provider storage usage
  - `streams_active`, `streams_completed`, `streams_failed` — delivery activity
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

- Happy path: upload -> discover -> fetch with payments
- Provider failure + failover mid-delivery (resume from byte offset)
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

Dedicated **indexer nodes** crawl the network's content announcements (already broadcast on the `content-routing/v1` gossip topic), build a searchable index of content metadata, and expose a query API. Multiple independent indexers can coexist — clients pick one or query several for redundancy.

This is analogous to how The Graph indexes blockchain data: indexers are semi-trusted infrastructure, but they cannot tamper with content (blobs are content-addressed), only withhold search results. Clients can cross-reference multiple indexers to detect omissions.

### How It Works

1. **Ingestion:** Indexer subscribes to `content-routing/v1` gossip topic. When a `ContentAnnounce` arrives, the indexer fetches the manifest and metadata blobs from a provider, extracts searchable fields (application-defined metadata fields), and adds them to a local full-text search index (e.g., `tantivy`).
2. **Query API:** Indexer exposes a query protocol on a custom ALPN (`storage-layer/search/v1`). Clients connect via iroh and send search queries. Results are manifest hashes + metadata summaries, ranked by relevance.
3. **Incentive:** Indexers earn query fees via the same payment channel mechanism used for delivery. Clients pay per query (or per batch of queries). This makes indexing a self-sustaining role in the network.
4. **Registration:** Indexers register in the `StakingRegistry` with an "indexer" role. Staking provides sybil resistance and a slashing mechanism if an indexer is proven to serve fabricated results (metadata doesn't match the actual blob).

### Discovery & Recommendations

Built on top of the indexer infrastructure:

- **Trending:** Most-delivered blobs = most voucher settlements. Indexers can query on-chain settlement data or track gossip volume to rank by popularity.
- **Category browsing:** Structured metadata fields in the metadata blob enable category-based filtering.
- **Collections:** User-created hash sequences (same mechanism as manifests) containing ordered lists of blob hashes. Stored as blobs, shareable by hash.
- **Algorithmic recommendations:** Application-layer concern. Could run on indexer nodes or as a separate service consuming indexer APIs.

### PoC Bridge

During PoC (before indexers exist), clients use the full-table gossip approach already in the design: every node holds the complete content routing table and metadata can be fetched directly. This works at tens of nodes. The migration path to indexers is additive — indexers subscribe to the same gossip topic, just with better query capabilities.
