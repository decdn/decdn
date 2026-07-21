# ADR 012: Client Architecture, Bootstrap, and Trust Model

**Date:** 2026-04-03
**Status:** Draft

## Context

Clients are referenced throughout ADRs 001–011 — they pay for content, hold Ethereum keys that authorize fund movement, maintain peer tables, and decrypt content envelopes — but no ADR defines the client as a coherent entity. Four gaps block PoC functionality:

1. **Bootstrap** — how does a client discover initial peers? [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) specifies registry query and retry but interleaves it with node-specific concerns and is incomplete for clients (no identity loading).
2. **Key management** — clients hold an iroh Ed25519 key (NodeId) and an Ethereum secp256k1 key (voucher signing, channel operations). Generation, storage, and rotation are unspecified.
3. **Identity lifecycle** — [ADR 005](005-protocol.md#adr-005-wire-protocol) defines ephemeral NodeId-to-Ethereum bindings in `StreamRequest` but does not specify creation, rotation, or expiry.
4. **Trust boundary** — what does the client verify vs. trust? Implied across multiple ADRs but never stated explicitly.

This ADR consolidates all client-specific behavior into a single canonical specification.

## Decision

### Scope

This ADR targets **desktop and server clients** — POSIX or Windows hosts with filesystem access, a long-lived process, and the ability to run a QUIC stack and an Ethereum wallet (EOA or Safe):

- A writable home directory for `~/.decdn/` keys, peer cache, and download state.
- Direct UDP socket access for iroh QUIC and iroh-relay traversal.
- Either an OS keychain (production) or a local encrypted keystore file (PoC) for Ethereum key storage.

**Mobile clients (iOS, Android) and web clients (browser) are out of scope.** They require fundamentally different choices for key custody (platform secure enclave / WalletConnect rather than filesystem keystore), transport (WebTransport rather than raw QUIC; no UDP on browsers), and storage (platform sandbox rather than `~/.decdn/`). The voucher-signing UX (high-frequency session-key signatures per [ADR 024 § Session Keys — Deferred to Production via ERC-7579 smartsessions](024-account-abstraction.md#session-keys--deferred-to-production-via-erc-7579-smartsessions)) and the bootstrap procedure (registry RPC) both assume desktop-class capabilities.

### Client Roles and Capabilities

A client is a lightweight QUIC endpoint that streams content and pays per MB. It is **not** a bonded node and has no on-chain registration requirement. Capabilities:

- Opens `cdn/client/v1` connections to nodes for paid content delivery
- Uses `cdn/dht/v1` FIND_VALUE for content discovery; falls back to `cdn/probe/v1` broadcast during bootstrap (see [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale))
- Contributes only local reputation observations ([ADR 008](008-reputation.md#adr-008-reputation-system)); it is not a bonded node and publishes nothing to the mesh
- Maintains a local peer table (from the on-chain registry) and reputation scores
- Signs vouchers authorizing off-chain USDC payments

### Bootstrap Procedure

Startup sequence from first launch to ready state:

```
1. Load or generate iroh identity key (see Key Management below)
2. Load Ethereum key from encrypted keystore
3. Query on-chain registry: paginated getActiveNodes(offset, 100) calls,
     starting at offset 0, incrementing until a page returns fewer than 100
     On failure: retry 3× exponential backoff (1 s, 5 s, 30 s)
     The budget is for the WHOLE read, not per page: a page that retries
     twice leaves one retry for every page after it. Per-page budgets would
     make the worst case 36 s × page-count, which scales with the size of
     the registry — something the caller cannot see or bound.
     Deterministic failures (no contract at the configured address, an ABI
     mismatch, an HTTP 4xx other than 429) are reported at once rather than
     retried — repeating them only spends the schedule.
     The whole of step 3 is additionally bounded by the client's
     `--timeout-ms`, so the retry schedule can never be the reason a fetch
     appears to hang past the deadline the user set.
4. If the registry read fails (retries exhausted, or a deterministic failure):
     On cached peers present: fall back to the peer cache, and tell the user
       the list is cached and how old it is — those nodes may have been
       deactivated or slashed since it was written
     On no cache: exit with error —
       "Cannot reach bootstrap sources. Check network connectivity
        and RPC endpoint configuration."
5. Connect to iroh relay (for NAT traversal)
6. Build peer table from the resolved bootstrap peers
     (registry results, or the cached peers.json on fallback)
7. Persist peer list to the peer cache
     A successful but *empty* read is the exception: it does not overwrite the
     cache, since an emptied registry is no reason to discard the last
     known-good peer list.
8. Begin periodic registry refresh (every 10 minutes)
```

The peer cache is `peers.json` under the resolved client data dir — `--data-dir`, else `[identity] data_dir`, defaulting to `~/.decdn/client` — so it moves with the rest of the client's state rather than living at a fixed path.

The on-chain registry is the sole discovery source; a cached peer list from the last successful query covers a transient RPC outage. Clients do not join the iroh-gossip mesh — they neither subscribe to nor relay `NodeAnnounce`. Gossip propagation is the responsibility of bonded nodes, which carry economic accountability (slashing, reputation) for relay correctness and availability; a client stays online only long enough to fetch and gains nothing from mesh participation.

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
| iroh Ed25519 | Attacker can impersonate client NodeId (connect to nodes) but cannot sign vouchers or move funds. | Generate new iroh key, reconnect |
| Ethereum secp256k1 | Attacker can sign vouchers draining the payment channel balance | Race to close channels: call `closeChannel` with the latest voucher nonce. If attacker has already submitted a close with a higher-nonce voucher, dispute within the challenge window ([ADR 003](003-payments.md#adr-003-payment-model)). No revocation mechanism exists beyond racing to close. |
| Both | Full impersonation | Close all channels immediately. Generate new iroh key. Use a new Ethereum address for future sessions. |

### Trust Boundary

#### Verified (trustless) — the client cryptographically validates these

- **Content integrity:** BLAKE3 hash verification on every received blob. A malicious node cannot serve corrupted data.
- **Voucher binding:** EIP-712 signatures on vouchers are produced by the client's own key. The client controls how much it authorizes.
- **Rate commitment:** `StreamResponse` carries a `slash_sig` (EIP-712 secp256k1) over the response fields, binding the node's registered Ethereum identity to the quoted `rate_per_mb`. A rate mismatch vs. the probe response within 30 seconds is slashable ([ADR 005](005-protocol.md#adr-005-wire-protocol)).
- **Node identity:** QUIC handshake authenticates the remote NodeId (Ed25519). The client knows it is communicating with the registered node.
- **Payment channel state:** On-chain, publicly verifiable. The client can always settle or dispute.

#### Trusted — the client relies on external guarantees

- **RPC endpoint:** Returns correct registry data. A compromised RPC can return a fabricated node list.
- **Registry correctness:** The `CapacityBond` contract accurately reflects bonded operators. Enforced by EVM execution — trust in the chain, not any specific party.

#### Not trusted — the client does not rely on these

- Any individual node's self-reported metadata (region) beyond what is signed and slashable. The region claim itself is mitigated by latency-based reputation: a node whose observed RTT contradicts its claimed region is penalized ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh)).
- Reputation reported by other peers. The client scores nodes solely from its own local observations ([ADR 008](008-reputation.md#adr-008-reputation-system)).
- Node availability promises beyond signed probe responses.

### Client Configuration

All client state resides under `~/.decdn/`, with the per-client data dir (`--data-dir` / `[identity] data_dir`) defaulting to `~/.decdn/client/`:

```
~/.decdn/
├── config.toml               # Client configuration
├── iroh_key                  # Ed25519 secret key (0600)
└── client/                   # the resolved client data dir
    ├── keystore.json         # Encrypted Ethereum keystore (Web3 Secret Storage)
    ├── buyer-channels.redb   # Buyer-side payment-channel store
    └── peers.json            # Cached peer list from last registry query
```

Default configuration:

```toml
[network]
rpc_url = "https://<arb-sepolia-rpc-endpoint>"
region = ""                                  # optional ISO 3166-1 alpha-2

[bootstrap]
registry_refresh_secs = 600                  # 10 minutes

[keys]
# Paths use ~ as shorthand; the client MUST perform home-directory expansion.
iroh_key_path = "~/.decdn/iroh_key"
eth_keystore_path = "~/.decdn/client/keystore.json"

[cache]
# NOT YET IMPLEMENTED: the peer cache is always `peers.json` inside the resolved
# client data dir, so move it with --data-dir rather than this key.
peer_cache_path = "~/.decdn/client/peers.json"
probe_cache_max_entries = 1024               # per ADR 001
probe_cache_ttl_secs = 15                    # per ADR 001

[payment]
voucher_interval_mb = 1                      # per ADR 003
```

## Consequences

### Positive

- Consolidates client behavior scattered across ADRs 001, 003, 005, and 008 into a single canonical specification
- Establishes an explicit trust boundary, making security assumptions auditable
- PoC key management is simple (file-based EOA or Safe wallet) with a clear production upgrade path (Safe multisig with session keys — see [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support))
- Bootstrap procedure is fully specified end-to-end, unblocking PoC implementation

### Negative

- File-based key storage in PoC is not suitable for production (acceptable for testnet with test funds)
- Hardware wallet voucher signing is confirmed infeasible — resolved by Safe session keys ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support))

## Multi-Source Download

Splitting a large blob across multiple nodes and fetching byte ranges in parallel is specified in [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1). Sequential single-source delivery is the default.

## Download Resume

A `decdn pull` interrupted by a crash, kill, or network drop resumes from the partial output file already on disk: the client re-hashes the bytes it has, discards any trailing unverified remainder, and continues the fetch from the last BLAKE3-verified offset via `StreamRequest{byte_offset}`. If the original payment channel is still open on-chain, the client reuses it; if it is closing or settled, a new channel is opened sized for the remaining bytes only.

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
2. For each chunk in order: `StreamRequest{hash: chunk.hash}`, write to `<data_dir>/downloads/<H_manifest>/chunk-<index>.part`, verify BLAKE3. A part already present that matches its declared size and BLAKE3 is reused rather than re-fetched, so an interrupted download resumes without paying twice.
3. After all chunks verified: concatenate in order → output file, whose length is checked against `total_bytes` **before** the file is renamed into place. Part files are then retained or deleted per § Blob retention below.

`<data_dir>` is the resolved client data dir — `--data-dir`/`identity.data_dir`, defaulting to `~/.decdn/client` — so parts share a root with the buyer-channel store rather than sitting in a fixed location an explicit `--data-dir` would not move.

A manifest is rejected at decode if it declares more than 1,000,000 chunks, any zero-size chunk, a `filename` that is not a bare basename, or trailing bytes after the record. Each chunk is a separate paid pull and a separate part file, so the chunk cap bounds spending and file count; it is not an allocation bound, since the chunk list is decoded before the cap is applied (an over-large list implies an over-large manifest blob, which `--max-blob-mb` already caps).

### Blob retention

Chunk part files are **retained by default** after reconstruction so the client can re-serve them via iroh-blobs. Pass `--no-keep-blobs` to delete immediately after reconstruction.

### Backward compatibility

Raw single-blob downloads are unchanged. The client checks the `DECDNMAN` magic header; on failure (wrong or missing magic) it treats the bytes as a raw blob. Content providers signal manifest vs. raw out-of-band.
