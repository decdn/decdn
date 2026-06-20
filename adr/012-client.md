# ADR 012: Client Architecture, Bootstrap, and Trust Model

**Date:** 2026-04-03
**Status:** Draft

## Context

Clients are referenced throughout ADRs 001–011 — they pay for content, hold Ethereum keys that authorize fund movement, maintain peer tables, validate gossip, and decrypt content envelopes — but no ADR defines the client as a coherent entity. Five gaps block PoC functionality:

1. **Bootstrap** — how does a client discover initial peers? [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) specifies registry query and retry but interleaves it with node-specific concerns and is incomplete for clients (no gossip subscription policy, no identity loading).
2. **Key management** — clients hold an iroh Ed25519 key (NodeId) and an Ethereum secp256k1 key (voucher signing, channel operations). Generation, storage, and rotation are unspecified.
3. **Identity lifecycle** — [ADR 005](005-protocol.md#adr-005-wire-protocol) defines ephemeral NodeId-to-Ethereum bindings in `StreamRequest` but does not specify creation, rotation, or expiry.
4. **Eclipse attack resolution** — [ADR 003](003-payments.md#adr-003-payment-model) lists Options A/B/C with no decision.
5. **Trust boundary** — what does the client verify vs. trust? Implied across multiple ADRs but never stated explicitly.

This ADR consolidates all client-specific behavior into a single canonical specification.

## Decision

### Scope

This ADR targets **desktop and server clients** — POSIX or Windows hosts with filesystem access, a long-lived process, and the ability to run a QUIC stack and an Ethereum wallet (EOA or Safe):

- A writable home directory for `~/.decdn/` keys, peer cache, and download state.
- Direct UDP socket access for iroh QUIC and iroh-relay traversal.
- Local NTP synchronization (required for gossip validation per [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)).
- Either an OS keychain (production) or a local encrypted keystore file (PoC) for Ethereum key storage.

**Mobile clients (iOS, Android) and web clients (browser) are out of scope.** They require fundamentally different choices for key custody (platform secure enclave / WalletConnect rather than filesystem keystore), transport (WebTransport rather than raw QUIC; no UDP on browsers), and storage (platform sandbox rather than `~/.decdn/`). The voucher-signing UX (high-frequency session-key signatures per [ADR 024 § Session Keys — Deferred to Production via ERC-7579 smartsessions](024-account-abstraction.md#session-keys--deferred-to-production-via-erc-7579-smartsessions)) and the bootstrap procedure (registry RPC + DNS seeds) both assume desktop-class capabilities.

### Client Roles and Capabilities

A client is a lightweight QUIC endpoint that streams content and pays per MB. It is **not** a bonded node and has no on-chain registration requirement. Capabilities:

- Opens `cdn/client/v1` connections to nodes for paid content delivery
- Uses `cdn/dht/v1` FIND_VALUE for content discovery; falls back to `cdn/probe/v1` broadcast during bootstrap (see [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale))
- **Subscribes** to gossip topics to receive `NodeAnnounce` messages
- Does **not** publish `NodeAnnounce` (not a bonded node)
- Does **not** publish `ReputationReport` via gossip ([ADR 008](008-reputation.md#adr-008-reputation-system) — clients contribute local observations only)
- Maintains a local peer table (`NodeId → NodeAnnounce`) and reputation scores
- Signs vouchers authorizing off-chain USDC payments

### Bootstrap Procedure

Startup sequence from first launch to ready state:

```
 1. Load or generate iroh identity key (see Key Management below)
 2. Load Ethereum key from encrypted keystore
 3. Query on-chain registry: paginated getActiveNodes(offset, 100) calls,
      starting at offset 0, incrementing until a page returns fewer than 100
      On failure: retry 3× exponential backoff (1 s, 5 s, 30 s)
 4. (Production only) Resolve DNS bootstrap seeds from configured seed domains
      On failure: retry 3× exponential backoff (1 s, 5 s, 30 s)
 5. Merge peers: registry results ∪ DNS seed results (registry metadata
      takes precedence when the same NodeId appears in both — see
      DNS Seed Specification below)
 6. If no usable peers from live sources:
      On cached peers present: fall back to ~/.decdn/peers.json
      On no cache: exit with error —
        "Cannot reach bootstrap sources. Check network connectivity
         and RPC endpoint configuration."
 7. Connect to iroh relay (for NAT traversal)
 8. Subscribe to gossip topics:
      - cdn/global/v1  (mandatory)
      - cdn/region/{region}/v1  (if region configured)
 9. Build peer table from merged bootstrap peers + incoming NodeAnnounce messages
10. Persist peer list to ~/.decdn/peers.json
11. Begin periodic registry refresh (every 10 minutes)
```

For PoC, steps 4–5 are skipped (no DNS seeds configured). The registry is the sole bootstrap source.

**Gossip participation policy:**

| Behavior | Client | Node |
| --- | --- | --- |
| Subscribe to gossip topics | Yes | Yes |
| Publish `NodeAnnounce` | No | Yes |
| Relay (forward) received gossip messages | No | Yes |
| Validate incoming gossip (signature, registry, timestamp) | Yes | Yes |
| Publish `ReputationReport` | No | Yes (bonded only) |
| Maintain peer table | Yes | Yes |
| Require NTP synchronization | Yes (for gossip validation) | Yes |

The client validates gossip using the same rules as nodes: signature verification, registry membership check, and ±60-second timestamp freshness ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)). This requires NTP synchronization, as already mandated for "validating clients" in [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh).

Clients participate as gossip *leaves*: they subscribe and validate but never forward received messages back into the mesh. iroh-gossip propagation is the responsibility of bonded nodes, which carry economic accountability (slashing, reputation) for relay correctness and availability; clients carry none. This applies to both PoC and production — not a deployment-time toggle.

Node-side registry interaction and bootstrap is in [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-peer-table-from-on-chain-registry).

### Key Management

Clients manage two independent cryptographic keys.

#### iroh Identity Key (Ed25519)

- **Purpose:** Defines the client's `NodeId` in the peer mesh. Used for QUIC connection authentication on `cdn/client/v1` and `cdn/probe/v1`.
- **Generation:** Created at first startup via `iroh::SecretKey::generate()`.
- **Storage (PoC):** File at `~/.decdn/iroh_key`, permissions `0600`. No encryption — the file contains the raw 32-byte secret key.
- **Storage (production):** Platform keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service API).
- **Rotation (PoC):** Not supported. Deleting the key file and restarting generates a new identity.
- **Rotation (production):** Generate a new key, reconnect to all nodes. The old NodeId becomes unreachable. Open payment channels are unaffected — they are keyed by Ethereum address, not NodeId.

#### Ethereum Key (secp256k1)

- **Purpose:** Signs vouchers (EIP-712), opens/closes payment channels (on-chain transactions), and signs ephemeral `BindNodeId` messages. Only operators bond TOKEN (via `CapacityBond`); clients have no bonding path.
- **Generation:** Not generated by the client software. Imported from an existing wallet or created as a Safe smart wallet.
- **PoC:** Either an encrypted keystore file at `~/.decdn/eth_keystore` (Web3 Secret Storage format, EOA) or a Safe smart wallet address. The client supports both — all contracts use `SignatureChecker`, which transparently handles EOA and smart account signatures.
- **Production:** Safe smart wallet (recommended). 1-of-1 for simplicity, 2-of-3 for high-value accounts. A **session key** authorized via the Safe's Session Key Module handles high-frequency voucher signing — see [ADR 024 § Session Keys — Deferred to Production via ERC-7579 smartsessions](024-account-abstraction.md#session-keys--deferred-to-production-via-erc-7579-smartsessions).

**Voucher signing with session keys:** At the default 1 MB voucher cadence, a 100 MB download requires 100 EIP-712 voucher signatures. Hardware wallets require physical confirmation per signature (2–5 seconds each), making them infeasible. Session keys solve this: a lightweight secp256k1 key generated at session start, authorized by the Safe owners (one approval), held in memory for the session, signing vouchers at wire speed. It is time-bounded, scope-limited to voucher signatures, and revocable by the Safe owners. See [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support) for the full design.

#### Key Summary

| Key | Algorithm | PoC Storage | Production Storage | Rotation |
| --- | --- | --- | --- | --- |
| iroh identity | Ed25519 | `~/.decdn/iroh_key` (0600) | Platform keychain | New key + reconnect |
| Ethereum | secp256k1 | `~/.decdn/eth_keystore` (encrypted) or Safe address | Safe wallet + session key ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)) | Wallet-level |

### Identity Lifecycle

Client identity bindings are **ephemeral and per-connection**, per [ADR 003 — Off-Chain Ephemeral Binding](003-payments.md#off-chain-ephemeral-binding-for-clients) and [ADR 005](005-protocol.md#adr-005-wire-protocol).

**Lifecycle:**

1. **Startup:** Load iroh key (→ `NodeId`). Load Ethereum key from keystore (EOA) or configure a 1-of-1 Safe with its owner key loaded from keystore; Production migrates to Safe-7579 + `erc7579/smartsessions` for high-frequency signing ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)).
2. **Connect:** Establish QUIC connection to a node via `cdn/client/v1`.
3. **Bind:** First `StreamRequest` on the connection includes `ethereum_address` and `binding_signature` — an EIP-712 `BindNodeId(nodeId, nonce=0)` signature. The `nonce=0` sentinel indicates an ephemeral (off-chain) binding.
4. **Session:** The node verifies the signature via `SignatureChecker` semantics (`ecrecover` for EOA clients, ERC-1271 `isValidSignature` RPC call for smart account clients — see [ADR 024](024-account-abstraction.md#off-chain-erc-1271-verification)), caches the binding for the connection's lifetime, and uses the verified address for voucher attribution. Subsequent requests on the same connection omit these fields.
5. **Disconnect:** The node discards the cached binding. No on-chain state to clean up.

**Security properties of `nonce=0`:** The ephemeral binding is not a replay vulnerability because the node only uses it for the authenticated QUIC connection on which it was received — a binding from connection A is never applied to connection B. On-chain `bindNodeId` ([ADR 003](003-payments.md#adr-003-payment-model)) also starts at nonce 0 (`bindingNonce[msg.sender]` is initially 0), so the nonce value alone does not distinguish off-chain from on-chain bindings. Protection against on-chain replay is the EIP-712 domain separator: the node verifies the off-chain binding via `SignatureChecker` semantics (locally, not on-chain), while on-chain `bindNodeId` verifies against `DOMAIN_SEPARATOR` (which includes the `CapacityBond` contract address and chain ID). A signature produced for off-chain use cannot pass the on-chain domain check unless the client uses the exact same domain parameters — and if it does, the on-chain binding consumes the nonce, preventing reuse.

**Key rotation:** Generating a new iroh key and reconnecting produces a new NodeId. The client signs a fresh `BindNodeId` with the same Ethereum key and the new NodeId. Open payment channels remain valid — channels are keyed by `(client_ethereum_address, provider_ethereum_address, nonce)`, not by NodeId.

**Key compromise response:**

| Compromised key | Impact | Response |
| --- | --- | --- |
| iroh Ed25519 | Attacker can impersonate client NodeId (connect to nodes, receive gossip) but cannot sign vouchers or move funds. | Generate new iroh key, reconnect |
| Ethereum secp256k1 | Attacker can sign vouchers draining the payment channel balance | Race to close channels: call `closeChannel` with the latest voucher nonce. If attacker has already submitted a close with a higher-nonce voucher, dispute within the challenge window ([ADR 003](003-payments.md#adr-003-payment-model)). No revocation mechanism exists beyond racing to close. |
| Both | Full impersonation | Close all channels immediately. Generate new iroh key. Use a new Ethereum address for future sessions. |

### Eclipse Attack Mitigation

At small mesh scale, eclipse attacks require both Sybil-scale capital (bonding enough nodes to dominate the registry) and RPC endpoint compromise (returning a fabricated node list); the on-chain registry alone suffices as the discovery source. As the mesh grows and operator-set diversity increases, clients adopt **multi-source bootstrap (Option B)** — discovering initial peers from at least two independent sources:

1. **On-chain registry** — `CapacityBond.getActiveNodes()` via the configured RPC endpoint.
2. **DNS seed list** — TXT records at `_decdn-seeds.{domain}` for each domain in a governance-maintained seed list. Record format: `nodeId=<hex>; addrs=<multiaddr>,<multiaddr>`.

Each client release ships with a built-in default seed list compiled into the binary; the `dns_seeds` configuration key (see [Client Configuration](#client-configuration)) provides a runtime override. Seed domains are maintained by governance ([ADR 009](009-governance.md#adr-009-governance-model)) and updated via new client releases or local config. An attacker must compromise both the RPC endpoint and all effective DNS seed domains to fully eclipse a client.

**Supplementary: Option C — Minimum honest-peer diversity** is adopted as a client-side policy (not protocol-enforced). The client maintains connections to at least `min_peer_diversity` nodes (default: 3) discovered via different sources (registry vs. DNS vs. gossip). If all connected nodes share one discovery source, the client logs a warning. Advisory — not blocking.

**Option A — Origin-backed nodes as fallback** is rejected as a *trust* mechanism. Whether a node has an origin backend is an opaque deployment choice and origin URLs are never exposed (design invariant preserved by [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority): only operator Ethereum addresses are recorded on-chain via `OriginAssignment`, never backend URLs), and having one is no guarantee of honesty. The DAO-ratified authorized-origin set surfaced via `OriginAssignment.getOrigins(namespaceId)` is an *availability commitment* (publishers commit to serving via specific operators), not a trust ranking — clients still verify content integrity via BLAKE3 and apply standard reputation / probe scoring regardless of authorized-origin status.

### Trust Boundary

#### Verified (trustless) — the client cryptographically validates these

- **Content integrity:** BLAKE3 hash verification on every received blob. A malicious node cannot serve corrupted data.
- **Voucher binding:** EIP-712 signatures on vouchers are produced by the client's own key. The client controls how much it authorizes.
- **Rate commitment:** `StreamResponse` carries a `slash_sig` (EIP-712 secp256k1) over the response fields, binding the node's registered Ethereum identity to the quoted `rate_per_mb`. A rate mismatch vs. the probe response within 30 seconds is slashable ([ADR 005](005-protocol.md#adr-005-wire-protocol)).
- **Node identity:** QUIC handshake authenticates the remote NodeId (Ed25519). The client knows it is communicating with the registered node.
- **Payment channel state:** On-chain, publicly verifiable. The client can always settle or dispute.

#### Trusted — the client relies on external guarantees

- **RPC endpoint:** Returns correct registry data. A compromised RPC can return a fabricated node list (eclipse). Mitigated in production by multi-source bootstrap (Option B above).
- **Registry correctness:** The `CapacityBond` contract accurately reflects bonded operators. Enforced by EVM execution — trust in the chain, not any specific party.
- **Gossip integrity:** `NodeAnnounce` messages are signed by the announcing node's registered key and validated against the registry. A node cannot forge another's announcement.
- **Clock:** NTP-synchronized local clock, used for gossip validation (±60 s freshness). Drift beyond this window causes the client to reject valid gossip.

#### Not trusted — the client does not rely on these

- Any individual node's self-reported metadata (region) beyond what is signed and slashable. The region claim itself is mitigated by latency-based reputation: a node whose observed RTT contradicts its claimed region is penalized ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)).
- Network-level reputation scores (30% gossip weight in [ADR 008](008-reputation.md#adr-008-reputation-system); local observations dominate at 70%).
- Node availability promises beyond signed probe responses.

### Client Configuration

All client state resides under `~/.decdn/`:

```
~/.decdn/
├── config.toml       # Client configuration
├── iroh_key          # Ed25519 secret key (0600)
├── eth_keystore      # Encrypted Ethereum keystore (Web3 Secret Storage)
└── peers.json        # Cached peer list from last registry query
```

Default configuration:

```toml
[network]
rpc_url = "https://<arb-sepolia-rpc-endpoint>"
region = ""                                  # optional ISO 3166-1 alpha-2

[bootstrap]
registry_refresh_secs = 600                  # 10 minutes
# Production only — empty in PoC
dns_seeds = []
min_peer_diversity = 3                       # Option C threshold (production)

[keys]
# Paths use ~ as shorthand; the client MUST perform home-directory expansion.
iroh_key_path = "~/.decdn/iroh_key"
eth_keystore_path = "~/.decdn/eth_keystore"

[cache]
peer_cache_path = "~/.decdn/peers.json"
probe_cache_max_entries = 1024               # per ADR 001
probe_cache_ttl_secs = 15                    # per ADR 001

[payment]
voucher_interval_mb = 1                      # per ADR 003
```

### DNS Seed Specification (Production)

Each governance-managed domain publishes TXT records at `_decdn-seeds.{domain}`:

```
_decdn-seeds.seeds.decdn.network. 300 IN TXT "nodeId=a1b2...;addrs=/ip4/1.2.3.4/udp/4433/quic-v1"
```

The client queries all configured seed domains, cross-checks returned NodeIds against the on-chain registry (seeds must be bonded operators), and merges valid entries into the peer table. For matching NodeIds, registry multiaddrs are the canonical source of truth — DNS addresses are discarded in favour of registry data, preventing a compromised seed domain from redirecting traffic for a valid NodeId. Seeds whose NodeId is not in the registry are discarded entirely with a warning.

## Consequences

### Positive

- Consolidates client behavior scattered across ADRs 001, 003, 005, and 008 into a single canonical specification
- Specifies multi-source bootstrap (Option B) as the production eclipse-attack defense
- Establishes an explicit trust boundary, making security assumptions auditable
- PoC key management is simple (file-based EOA or Safe wallet) with a clear production upgrade path (Safe multisig with session keys — see [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support))
- Bootstrap procedure is fully specified end-to-end, unblocking PoC implementation

### Negative

- DNS seed list introduces a governance-maintained out-of-band dependency for production
- File-based key storage in PoC is not suitable for production (acceptable for testnet with test funds)
- NTP synchronization is a hard requirement for gossip validation — clients without NTP will reject valid gossip and build stale peer tables
- Hardware wallet voucher signing is confirmed infeasible — resolved by Safe session keys ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support))

## Multi-Node Parallel Download

A client can split a large blob across N nodes and download each byte range in parallel (BitTorrent-style), opt-in via a CLI flag:

```
decdn pull <hash> --max-channels <N> -o <output>
```

**Default:** `--max-channels 1` (sequential, existing behavior).

### Economic threshold

Opening, closing, and settling a payment channel costs ~$0.23 at the production L2's typical gas prices ([ADR 003](003-payments.md#adr-003-payment-model)). For N nodes that is N × $0.23 in fixed overhead before a byte is delivered. At $0.01/GB this overhead is significant, so parallelism is recommended only for large blobs (e.g., > 10 GiB) to amortize the per-channel cost. The `--min-blob-size` flag (default: 10 GiB) disables parallelism for smaller blobs.

### Range assignment

1. Probe N candidates; confirm `has_blob: true` and collect latency.
2. Learn `total_bytes` from the first `StreamResponse` (or from the manifest — see below).
3. Divide `total_bytes` into N ranges aligned to the 256 MiB chunk boundary; last range absorbs the remainder. Minimum range: 256 MiB — reduce N if necessary.
4. Assign lowest-latency node to first range (minimises time-to-first-byte).

One payment channel per node; channel deposit sized for its assigned range plus a 5% buffer. A shared multi-provider channel is not supported by the contract.

### Streaming incompatibility

`--streaming` (or piped output) forces single-channel sequential delivery. Multi-channel mode buffers all range parts to disk before producing output — the output file is only available after all ranges complete.

### Failure handling

Node failure mid-range: re-probe for a replacement, resume from last BLAKE3-verified byte within the range via `byte_offset`, open a new channel for the remaining bytes.

## Download Resume and Crash Recovery

The client persists download state to disk so that a crash, kill, or network drop never requires re-downloading already-verified bytes.

### On-disk layout

```
~/.decdn/downloads/<hash>/
  state.json        # authoritative resume state (atomically written)
  blob.partial      # bytes written sequentially (single-channel mode)
  range-0.part      # [multi-channel] bytes for range 0
  range-1.part      # [multi-channel] bytes for range 1
  ...
```

`<hash>` is the BLAKE3 hash of the requested blob (or manifest — see below). Location overridden by `DECDN_DOWNLOADS_DIR` env var or `--downloads-dir` flag.

### State file (`state.json`)

```jsonc
{
  "version": 1,
  "hash": "<BLAKE3 hex>",
  "total_bytes": 52428800000,   // null until first StreamResponse
  "mode": "single",             // or "parallel" or "manifest"
  "ranges": [
    {
      "start_byte": 0,
      "end_byte": 17476266666,
      "verified_offset": 8738133333,  // next byte to fetch; 0 = start of range
      "channel_id": "0xabc...def",    // null if not yet opened
      "voucher_nonce": 42,            // -1 = no vouchers sent yet
      "node_id": "..."
    }
  ],
  "updated_at": "2026-04-10T14:23:01Z"
}
```

### Atomic writes

State is written atomically: write to `state.json.tmp`, `fsync`, then `rename` (POSIX atomic). A crash during the write never corrupts the previous state.

State is flushed after each 64 MiB of BLAKE3-verified bytes. The voucher nonce is flushed on **every voucher send** (nonces must be strictly monotone and must never be reused after resume). Worst-case re-download after a crash: 64 MiB per range.

### Resume procedure

On startup, `decdn pull <hash>` checks for an existing download directory:

- **Not found:** start fresh.
- **Found:** load `state.json`; for each range with `verified_offset > 0`, reconnect to the same node (or re-probe a replacement), open or reuse the channel, send `StreamRequest{byte_offset: verified_offset}`, and continue writing from that offset.

If the original channel is still open on-chain, the client reuses it (avoids the $0.23 lifecycle cost). If the channel is already settled, a new channel is opened sized for the remaining bytes only.

### Cleanup commands

```
decdn downloads list              # show in-progress downloads
decdn downloads clean             # remove completed and failed downloads
decdn downloads clean --all       # remove all downloads including in-progress
```

State directories older than 30 days with no progress (`verified_offset = -1`) are treated as abandoned and purged by `clean`.

## File Manifests and Reconstruction

Large files are split into chunks at ingest. A **manifest blob** describes the ordered list of chunk hashes; its BLAKE3 hash is the canonical file identifier shared out-of-band.

### Chunk size

#### 256 MiB

(fixed at ingest). The last chunk is a partial chunk. Chunk size is a convention for origin-produced content — CDN nodes serve any BLAKE3-addressed blob regardless of size.

#### Chunking is the coarse answer for ranged access

Splitting a large file into 256 MiB chunk-blobs, each its own BLAKE3 hash, is the **sanctioned, zero-new-surface answer for ranged access against an origin**. A client wanting "the second GB" of a multi-gigabyte manifest-published file fetches only the ~4 chunk-blobs that cover it (`StreamRequest{hash: chunk.hash}` per chunk, [§ Download flow](#download-flow)); a node filling those from origin on a cache miss pulls only those chunk objects whole — never the entire file — and verifies each whole against its own hash with no bao tree or outboard needed. Chunking bounds origin egress to chunk granularity for the common case at the cost of nothing new.

Two residual cases chunking does not cover: (a) a **single-blob publish** that skips the manifest (one giant BLAKE3-addressed object), and (b) **sub-256-MiB precision** within a chunk. Both are addressed by the finer-grained range-scoped origin pull in [ADR 037 § Origin-tier pull-through](037-regional-proxy-warming.md#origin-tier-pull-through-ranged-fetch--external-outboard), which fetches a bounded `[a, b)` plus the `{H}.obao4` outboard and verifies the range against the root. Publishers SHOULD prefer the manifest/chunk path for large files; the origin-tier range pull is the fallback for content that is not chunked.

### Manifest format (postcard-encoded)

```rust
struct Manifest {
    magic: [u8; 8],       // b"DECDNMAN" — checked before deserialization
    version: u8,          // currently 1
    total_bytes: u64,
    mime_type: String,    // empty if unknown
    filename: String,     // basename hint; empty if not provided
    chunks: Vec<ChunkEntry>,
}

struct ChunkEntry {
    hash: [u8; 32],       // BLAKE3 hash of the chunk blob
    size: u64,
}
```

The manifest blob is pushed to the CDN like any other blob. It is typically < 1 MB even for 10,000-chunk files.

### Download flow

1. Fetch manifest blob (`StreamRequest{hash: H_manifest}`), verify BLAKE3, deserialise.
2. For each chunk in order: `StreamRequest{hash: chunk.hash}`, write to `~/.decdn/downloads/<H_manifest>/chunk-<index>.part`, verify BLAKE3.
3. After all chunks verified: concatenate in order → output file; delete part files.

### Blob retention

Chunk part files are **retained by default** after reconstruction so the client can re-serve them via iroh-blobs. Pass `--no-keep-blobs` to delete immediately after reconstruction.

### Backward compatibility

Raw single-blob downloads are unchanged. The client checks the `DECDNMAN` magic header; on failure (wrong or missing magic) it treats the bytes as a raw blob. Content providers signal manifest vs. raw out-of-band.
