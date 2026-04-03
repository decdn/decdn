# ADR 012: Client Architecture, Bootstrap, and Trust Model

**Date:** 2026-04-03
**Status:** Draft

## Context

Clients are referenced throughout ADRs 001–011 — they pay for content, hold Ethereum keys that authorize fund movement, maintain peer tables, validate gossip, and decrypt sealed envelopes — but no ADR defines the client as a coherent entity. Five gaps block PoC functionality:

1. **Bootstrap** — how does a client discover initial peers? [ADR 001](001-network.md) specifies registry query and retry but the procedure is interleaved with node-specific concerns and is incomplete for clients (no gossip subscription policy, no identity loading).
2. **Key management** — clients hold an iroh Ed25519 key (NodeId), an Ethereum secp256k1 key (voucher signing, channel operations), and an implicit X25519 key (sealed envelope decryption). Generation, storage, and rotation are unspecified.
3. **Identity lifecycle** — [ADR 005](005-protocol.md) defines ephemeral NodeId-to-Ethereum bindings in `StreamRequest` but does not specify creation, rotation, or expiry.
4. **Eclipse attack resolution** — [ADR 003](003-payments.md) lists Options A/B/C with no decision.
5. **Trust boundary** — what does the client verify vs. trust? This is implied across multiple ADRs but never stated explicitly.

This ADR consolidates all client-specific behaviour into a single canonical specification.

## Decision

### Client Roles and Capabilities

A client is a lightweight QUIC endpoint that streams content and pays per MB. It is **not** a staked node and has no on-chain registration requirement.

Capabilities:
- Opens `cdn/client/v1` connections to nodes for paid content delivery
- Opens `cdn/probe/v1` connections for content discovery
- **Subscribes** to gossip topics to receive `NodeAnnounce` messages
- Does **not** publish `NodeAnnounce` (not a staked node)
- Does **not** publish `ReputationReport` via gossip ([ADR 008](008-reputation.md) — clients contribute local observations only)
- Maintains a local peer table (`NodeId → NodeAnnounce`) and reputation scores
- Signs vouchers authorizing off-chain USDC (or governance-approved token) payments
- Optionally stakes TOKEN for connection priority during congestion ([ADR 003 — Client Priority Staking](003-payments.md#client-priority-staking))

### Bootstrap Procedure

Complete startup sequence from first launch to ready state:

```
1. Load or generate iroh identity key (see Key Management below)
2. Load Ethereum key from encrypted keystore
3. Query on-chain registry: getActiveNodes(0, 100)
     On failure: retry 3× exponential backoff (1 s, 5 s, 30 s)
     On continued failure + cached peers: fall back to ~/.decdn/peers.json
     On continued failure + no cache: exit with error —
       "Cannot reach registry at {rpc_url}. Check network connectivity
        and RPC endpoint configuration."
4. Connect to iroh relay (for NAT traversal)
5. Subscribe to gossip topics:
     - cdn/global/v1  (mandatory)
     - cdn/region/{region}/v1  (if region configured)
6. Build peer table from registry results + incoming NodeAnnounce messages
7. Persist peer list to ~/.decdn/peers.json
8. Begin periodic registry refresh (every 10 minutes)
```

**Gossip participation policy:**

| Behaviour | Client | Node |
| --- | --- | --- |
| Subscribe to gossip topics | Yes | Yes |
| Publish `NodeAnnounce` | No | Yes |
| Validate incoming gossip (signature, registry, timestamp) | Yes | Yes |
| Publish `ReputationReport` | No | Yes (staked only) |
| Maintain peer table | Yes | Yes |
| Require NTP synchronization | Yes (for gossip validation) | Yes |

The client validates gossip messages using the same rules as nodes: signature verification, registry membership check, and ±60-second timestamp freshness ([ADR 001](001-network.md)). This requires NTP synchronization, as already mandated for "validating clients" in ADR 001.

The registry query, retry schedule, and `peers.json` fallback behaviour defined here supersede the client-specific portions of [ADR 001 — Registry Unavailability](001-network.md#registry-unavailability). ADR 001 retains the specification for node bootstrap and registry interaction.

### Key Management

Clients manage two independent cryptographic keys. A third (X25519) is derived deterministically and requires no separate storage.

#### iroh Identity Key (Ed25519)

- **Purpose:** Defines the client's `NodeId` in the peer mesh. Used for QUIC connection authentication and as the basis for X25519 derivation ([ADR 006](006-e2e-encryption.md)).
- **Generation:** Created at first startup via `iroh::SecretKey::generate()`.
- **Storage (PoC):** File at `~/.decdn/iroh_key`, permissions `0600`. No encryption — the file contains the raw 32-byte secret key.
- **Storage (production):** Platform keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service API).
- **Rotation (PoC):** Not supported. Deleting the key file and restarting generates a new identity.
- **Rotation (production):** Generate a new key, reconnect to all nodes. The old NodeId becomes unreachable. Open payment channels are unaffected — they are keyed by Ethereum address, not NodeId.

#### Ethereum Key (secp256k1)

- **Purpose:** Signs vouchers (EIP-712), opens/closes payment channels (on-chain transactions), signs ephemeral `BindNodeId` messages, and optionally stakes TOKEN for priority.
- **Generation:** Not generated by the client software. Imported from an existing wallet.
- **Storage (PoC):** Encrypted keystore file at `~/.decdn/eth_keystore` (Web3 Secret Storage format, password-protected). The client prompts for the password at startup and holds the decrypted key in memory for the session.
- **Storage (production):** Hardware wallet (Ledger/Trezor) for channel operations (open/close are infrequent on-chain transactions). A **derived hot key** handles voucher signing — see below.

**Hardware wallet feasibility for voucher signing:** Not feasible. At the default 1 MB voucher cadence, a 100 MB download requires 100 EIP-712 voucher signatures. Each hardware wallet signature requires physical confirmation (2–5 seconds). This is incompatible with streaming delivery.

**Derived hot key (production):** For production deployments using a hardware wallet, a session-lived voucher signing key is derived on demand:

```
voucher_key = HKDF-SHA256(
    ikm    = hardware_wallet_sign("decdn-voucher-signer-v1"),
    salt   = session_id,   // random 32 bytes, generated at startup
    info   = "decdn-voucher-signer-v1"
)
```

The resulting secp256k1 key is used exclusively for EIP-712 voucher signatures. It is held in memory only — never written to disk. The corresponding Ethereum address must be pre-authorized in the payment channel contract as a delegated signer (contract support for delegated signers is deferred to a future ADR).

#### X25519 Key (Sealed Envelope Decryption)

Derived deterministically from the iroh Ed25519 key via birational map to Curve25519 ([ADR 006](006-e2e-encryption.md)). No separate generation or storage. Available whenever the iroh key is loaded.

#### Key Summary

| Key | Algorithm | PoC Storage | Production Storage | Rotation |
| --- | --- | --- | --- | --- |
| iroh identity | Ed25519 | `~/.decdn/iroh_key` (0600) | Platform keychain | New key + reconnect |
| Ethereum | secp256k1 | `~/.decdn/eth_keystore` (encrypted) | Hardware wallet + derived hot key | Wallet-level |
| X25519 | X25519 | Derived from iroh key | Derived from iroh key | Follows iroh key |

### Identity Lifecycle

Client identity bindings are **ephemeral and per-connection**, as specified in [ADR 003 — Off-Chain Ephemeral Binding](003-payments.md#off-chain-ephemeral-binding-for-clients) and [ADR 005](005-protocol.md).

**Lifecycle:**

1. **Startup:** Load iroh key (→ `NodeId`). Load Ethereum key from keystore.
2. **Connect:** Establish QUIC connection to a node via `cdn/client/v1`.
3. **Bind:** First `StreamRequest` on the connection includes `ethereum_address` and `binding_signature` — an EIP-712 `BindNodeId(nodeId, nonce=0)` signature. The `nonce=0` sentinel indicates an ephemeral (off-chain) binding.
4. **Session:** The node verifies the signature via `ecrecover`, caches the binding for the connection's lifetime, and uses the recovered address for `clientStakeOf` lookups and voucher attribution. Subsequent requests on the same connection omit these fields.
5. **Disconnect:** The node discards the cached binding. No on-chain state to clean up.

**Security properties of `nonce=0`:** The ephemeral binding is not a replay vulnerability because the node only uses it for the authenticated QUIC connection on which it was received. A binding from connection A is never applied to connection B. The binding cannot be used to impersonate the client on-chain because on-chain `bindNodeId` requires `nonce > 0` (monotonic counter per address).

**Key rotation:** Generating a new iroh key and reconnecting produces a new NodeId. The client signs a fresh `BindNodeId` with the same Ethereum key and the new NodeId. Open payment channels remain valid — channels are keyed by `(client_ethereum_address, provider_ethereum_address, nonce)`, not by NodeId.

**Key compromise response:**

| Compromised key | Impact | Response |
| --- | --- | --- |
| iroh Ed25519 | Attacker can impersonate client NodeId (connect to nodes, receive gossip) but cannot sign vouchers or move funds | Generate new iroh key, reconnect |
| Ethereum secp256k1 | Attacker can sign vouchers draining the payment channel balance | Race to close channels: call `closeChannel` with the latest voucher nonce. If attacker has already submitted a close with a higher-nonce voucher, dispute within the challenge window ([ADR 003](003-payments.md)). No revocation mechanism exists beyond racing to close. |
| Both | Full impersonation | Close all channels immediately. Generate new iroh key. Use a new Ethereum address for future sessions. |

### Eclipse Attack Mitigation

This section resolves the open question in [ADR 003](003-payments.md) regarding eclipse attack options.

**PoC:** The on-chain registry is the single source of truth for peer discovery. Eclipse attacks require both Sybil-scale capital (staking enough nodes to dominate the registry) and RPC endpoint compromise (returning a fabricated node list). This is out of scope for a PoC threat model with tens of known-operator nodes on a testnet.

**Production:** Adopt **Option B — Multi-source bootstrap.**

Clients discover initial peers from at least two independent sources:

1. **On-chain registry** — `StakingRegistry.getActiveNodes()` via the configured RPC endpoint.
2. **DNS seed list** — TXT records at `_decdn-seeds.{domain}` for each domain in a governance-maintained seed list. Record format: `nodeId=<hex>; addrs=<multiaddr>,<multiaddr>`.

The DNS seed domains are maintained by governance ([ADR 009](009-governance.md)) and hardcoded in the client binary for each release. An attacker must compromise both the RPC endpoint and all DNS seed domains to fully eclipse a client.

**Supplementary: Option C — Minimum honest-peer diversity** is adopted as a client-side policy (not protocol-enforced). The client maintains connections to at least `min_peer_diversity` nodes (default: 3) discovered via different sources (registry vs. DNS vs. gossip). If all connected nodes were discovered via the same source, the client logs a warning. This is advisory — not blocking.

**Option A — Origin-backed nodes as fallback** is rejected. Whether a node has an origin backend is an opaque deployment choice (a design invariant: "no external origin URL is ever exposed"). Exposing origin-backed status on-chain to help clients find "trustworthy" nodes would undermine this opacity model. Furthermore, having an origin backend is not a guarantee of honesty.

### Trust Boundary

**Verified (trustless) — the client cryptographically validates these:**

- **Content integrity:** BLAKE3 hash verification on every received blob. A malicious node cannot serve corrupted data.
- **Voucher binding:** EIP-712 signatures on vouchers are produced by the client's own key. The client controls how much it authorizes.
- **Rate commitment:** `StreamResponse` is signed by the node's iroh key, binding it to the quoted `rate_per_mb`. A rate mismatch vs. the probe response within 30 seconds is slashable ([ADR 005](005-protocol.md)).
- **Node identity:** QUIC handshake authenticates the remote NodeId (Ed25519). The client knows it is communicating with the registered node.
- **Payment channel state:** On-chain, publicly verifiable. The client can always settle or dispute.

**Trusted — the client relies on external guarantees:**

- **RPC endpoint:** Returns correct registry data. A compromised RPC can return a fabricated node list (eclipse). Mitigated in production by multi-source bootstrap (Option B above).
- **Registry correctness:** The `StakingRegistry` contract accurately reflects staked nodes. Enforced by EVM execution — trust in the chain, not any specific party.
- **Gossip integrity:** `NodeAnnounce` messages are signed by the announcing node's registered key and validated against the registry. A node cannot forge another's announcement. However, `LoadHint` and `popular_hashes` are advisory — a node can lie, affecting selection quality but not safety.
- **App server:** For encrypted content ([ADR 006](006-e2e-encryption.md)), the client trusts the app server to deliver correct epoch keys and sealed envelopes. This is outside the CDN trust boundary.
- **Clock:** NTP-synchronized local clock, used for gossip validation (±60 s freshness). Drift beyond this window causes the client to reject valid gossip.

**Not trusted — the client does not rely on these:**

- Any individual node's self-reported metadata (region, load) beyond what is signed and slashable.
- Network-level reputation scores (30% gossip weight in [ADR 008](008-reputation.md); local observations dominate at 70%).
- Node availability promises beyond signed probe responses (unsigned gossip claims like `popular_hashes` are advisory).

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
rpc_url = "https://arb-sepolia.g.alchemy.com/v2/{key}"
region = ""                                  # optional ISO 3166-1 alpha-2

[bootstrap]
registry_refresh_secs = 600                  # 10 minutes
# Production only — empty in PoC
dns_seeds = []
min_peer_diversity = 3                       # Option C threshold (production)

[keys]
iroh_key_path = "~/.decdn/iroh_key"
eth_keystore_path = "~/.decdn/eth_keystore"

[cache]
peer_cache_path = "~/.decdn/peers.json"
probe_cache_max_entries = 1024               # per ADR 001
probe_cache_ttl_secs = 15                    # per ADR 001

[payment]
default_token = "USDC"                       # per ADR 010
voucher_interval_mb = 1                      # per ADR 003
```

### DNS Seed Specification (Production)

Each governance-managed domain publishes TXT records at `_decdn-seeds.{domain}`:

```
_decdn-seeds.seeds.decdn.network. 300 IN TXT "nodeId=a1b2...;addrs=/ip4/1.2.3.4/udp/4433/quic-v1"
```

The client queries all configured seed domains, cross-checks returned NodeIds against the on-chain registry (seeds must be staked nodes), and merges valid entries into the peer table. Seeds not found in the registry are discarded with a warning.

## Consequences

**Positive:**

- Consolidates all client behaviour scattered across ADRs 001, 003, 005, 006, and 008 into a single canonical specification
- Resolves the eclipse attack open question from ADR 003 with a concrete decision (Option B for production)
- Establishes an explicit trust boundary, making security assumptions auditable
- PoC key management is simple (file-based) with a clear production upgrade path (platform keychain, hardware wallet + derived hot key)
- Bootstrap procedure is fully specified end-to-end, unblocking PoC implementation

**Negative:**

- DNS seed list introduces a governance-maintained out-of-band dependency for production
- File-based key storage in PoC is not suitable for production (acceptable for testnet with test funds)
- Ephemeral bindings mean a disconnected client loses priority staking benefits until reconnection
- NTP synchronization is a hard requirement for gossip validation — clients without NTP will reject valid gossip and build stale peer tables
- Hardware wallet voucher signing is confirmed infeasible — the derived hot key requires delegated signer support in the payment channel contract (deferred)

## Open Questions

1. **Gossip relay:** Should clients forward received gossip messages to other peers, or only consume? Recommendation: consume-only for PoC to minimize client complexity. Evaluate relay participation for production to improve message propagation.
2. **Delegated voucher signer:** The derived hot key for production hardware wallet users requires contract-level support for delegated signers. This should be addressed in a future ADR or an amendment to [ADR 003](003-payments.md).
3. **Mobile/web clients:** This ADR assumes a desktop/server client with filesystem access. Mobile and web clients are listed as non-goals in the architecture overview but may need adapted key storage and bootstrap mechanisms.

## ADRs Affected

- **[ADR 001](001-network.md):** Client bootstrap and registry unavailability sections are superseded by this ADR for client-specific behaviour. ADR 001 retains the specification for node bootstrap.
- **[ADR 003](003-payments.md):** Eclipse attack options (A/B/C) are resolved — Option B for production, registry-only for PoC, Option C as supplementary policy.
- **[ADR 005](005-protocol.md):** `StreamRequest` ephemeral binding fields are specified in full lifecycle context here.
- **[ADR 008](008-reputation.md):** Client reputation contribution is clarified — local observations only, no gossip submissions.
