# ADR 012: Client Architecture, Bootstrap, and Trust Model

**Date:** 2026-04-03
**Status:** Draft

## Context

Clients are referenced throughout ADRs 001–011 — they pay for content, hold Ethereum keys that authorize fund movement, maintain peer tables, validate gossip, and decrypt content envelopes — but no ADR defines the client as a coherent entity. Five gaps block PoC functionality:

1. **Bootstrap** — how does a client discover initial peers? [ADR 001](001-network.md) specifies registry query and retry but the procedure is interleaved with node-specific concerns and is incomplete for clients (no gossip subscription policy, no identity loading).
2. **Key management** — clients hold an iroh Ed25519 key (NodeId) and an Ethereum secp256k1 key (voucher signing, channel operations). Generation, storage, and rotation are unspecified.
3. **Identity lifecycle** — [ADR 005](005-protocol.md) defines ephemeral NodeId-to-Ethereum bindings in `StreamRequest` but does not specify creation, rotation, or expiry.
4. **Eclipse attack resolution** — [ADR 003](003-payments.md) lists Options A/B/C with no decision.
5. **Trust boundary** — what does the client verify vs. trust? This is implied across multiple ADRs but never stated explicitly.

This ADR consolidates all client-specific behaviour into a single canonical specification.

## Decision

### Client Roles and Capabilities

A client is a lightweight QUIC endpoint that streams content and pays per MB. It is **not** a staked node and has no on-chain registration requirement.

Capabilities:

- Opens `cdn/client/v1` connections to nodes for paid content delivery
- Uses `cdn/dht/v1` FIND_VALUE for content discovery; falls back to `cdn/probe/v1` broadcast during bootstrap (see [ADR 022](022-content-discovery.md))
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

Clients manage two independent cryptographic keys.

#### iroh Identity Key (Ed25519)

- **Purpose:** Defines the client's `NodeId` in the peer mesh. Used for QUIC connection authentication — both for CDN delivery (`cdn/client/v1`, `cdn/probe/v1`) and for key delivery from the app server (`cdn/keys/v1`, [ADR 006](006-e2e-encryption.md)).
- **Generation:** Created at first startup via `iroh::SecretKey::generate()`.
- **Storage (PoC):** File at `~/.decdn/iroh_key`, permissions `0600`. No encryption — the file contains the raw 32-byte secret key.
- **Storage (production):** Platform keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service API).
- **Rotation (PoC):** Not supported. Deleting the key file and restarting generates a new identity.
- **Rotation (production):** Generate a new key, reconnect to all nodes. The old NodeId becomes unreachable. Open payment channels are unaffected — they are keyed by Ethereum address, not NodeId.

#### Ethereum Key (secp256k1)

- **Purpose:** Signs vouchers (EIP-712), opens/closes payment channels (on-chain transactions), signs ephemeral `BindNodeId` messages, and optionally stakes TOKEN for priority.
- **Generation:** Not generated by the client software. Imported from an existing wallet or created as a Safe smart wallet.
- **PoC:** Either an encrypted keystore file at `~/.decdn/eth_keystore` (Web3 Secret Storage format, EOA) or a Safe smart wallet address. The client software supports both — all contracts use `SignatureChecker` which transparently handles EOA and smart account signatures.
- **Production:** Safe smart wallet (recommended). 1-of-1 for simplicity, 2-of-3 for high-value accounts. A **session key** authorized via the Safe's Session Key Module handles high-frequency voucher signing — see [ADR 024](024-account-abstraction.md#3-session-keys-via-safe-modules).

**Voucher signing with session keys:** At the default 1 MB voucher cadence, a 100 MB download requires 100 EIP-712 voucher signatures. Hardware wallets require physical confirmation per signature (2–5 seconds each), making them infeasible for voucher signing. Session keys solve this: a lightweight secp256k1 key is generated at session start, authorized by the Safe owners (one approval), and held in memory for the session. The session key signs vouchers at wire speed. It is time-bounded, scope-limited to voucher signatures, and revocable by the Safe owners. See [ADR 024](024-account-abstraction.md) for the full design.

#### Key Summary

| Key | Algorithm | PoC Storage | Production Storage | Rotation |
| --- | --- | --- | --- | --- |
| iroh identity | Ed25519 | `~/.decdn/iroh_key` (0600) | Platform keychain | New key + reconnect |
| Ethereum | secp256k1 | `~/.decdn/eth_keystore` (encrypted) or Safe address | Safe wallet + session key ([ADR 024](024-account-abstraction.md)) | Wallet-level |

### Identity Lifecycle

Client identity bindings are **ephemeral and per-connection**, as specified in [ADR 003 — Off-Chain Ephemeral Binding](003-payments.md#off-chain-ephemeral-binding-for-clients) and [ADR 005](005-protocol.md).

**Lifecycle:**

1. **Startup:** Load iroh key (→ `NodeId`). Load Ethereum key from keystore (EOA) or configure Safe address and establish/refresh a session key for signing ([ADR 024](024-account-abstraction.md)).
2. **Connect:** Establish QUIC connection to a node via `cdn/client/v1`.
3. **Bind:** First `StreamRequest` on the connection includes `ethereum_address` and `binding_signature` — an EIP-712 `BindNodeId(nodeId, nonce=0)` signature. The `nonce=0` sentinel indicates an ephemeral (off-chain) binding.
4. **Session:** The node verifies the signature using `SignatureChecker` semantics (`ecrecover` for EOA clients, ERC-1271 `isValidSignature` RPC call for smart account clients — see [ADR 024](024-account-abstraction.md#4-off-chain-erc-1271-verification)), caches the binding for the connection's lifetime, and uses the verified address for `clientStakeOf` lookups and voucher attribution. Subsequent requests on the same connection omit these fields.
5. **Disconnect:** The node discards the cached binding. No on-chain state to clean up.

**Security properties of `nonce=0`:** The ephemeral binding is not a replay vulnerability because the node only uses it for the authenticated QUIC connection on which it was received. A binding from connection A is never applied to connection B. On-chain `bindNodeId` ([ADR 003](003-payments.md)) also starts at nonce 0 (`bindingNonce[msg.sender]` is initially 0), so the nonce value alone does not distinguish off-chain from on-chain bindings. The protection against on-chain replay is the EIP-712 domain separator: the off-chain binding is verified by the node via `SignatureChecker` semantics (locally, not on-chain), while on-chain `bindNodeId` verifies against `DOMAIN_SEPARATOR` (which includes the `StakingRegistry` contract address and chain ID). A signature produced for off-chain use cannot pass the on-chain domain check unless the client uses the exact same domain parameters — and if it does, the on-chain binding consumes the nonce, preventing reuse.

**Key rotation:** Generating a new iroh key and reconnecting produces a new NodeId. The client signs a fresh `BindNodeId` with the same Ethereum key and the new NodeId. Open payment channels remain valid — channels are keyed by `(client_ethereum_address, provider_ethereum_address, nonce)`, not by NodeId.

**Key compromise response:**

| Compromised key | Impact | Response |
| --- | --- | --- |
| iroh Ed25519 | Attacker can impersonate client NodeId (connect to nodes, receive gossip) but cannot sign vouchers or move funds. Attacker can connect to the app server as this NodeId via `cdn/keys/v1`, but cannot authenticate without the session token — no content access. | Generate new iroh key, reconnect |
| Ethereum secp256k1 | Attacker can sign vouchers draining the payment channel balance | Race to close channels: call `closeChannel` with the latest voucher nonce. If attacker has already submitted a close with a higher-nonce voucher, dispute within the challenge window ([ADR 003](003-payments.md)). No revocation mechanism exists beyond racing to close. |
| Both | Full impersonation | Close all channels immediately. Generate new iroh key. Use a new Ethereum address for future sessions. |

### Eclipse Attack Mitigation

This section resolves the open question in [ADR 003](003-payments.md) regarding eclipse attack options.

**PoC:** The on-chain registry is the single source of truth for peer discovery. Eclipse attacks require both Sybil-scale capital (staking enough nodes to dominate the registry) and RPC endpoint compromise (returning a fabricated node list). This is out of scope for a PoC threat model with tens of known-operator nodes on a testnet.

**Production:** Adopt **Option B — Multi-source bootstrap.**

Clients discover initial peers from at least two independent sources:

1. **On-chain registry** — `StakingRegistry.getActiveNodes()` via the configured RPC endpoint.
2. **DNS seed list** — TXT records at `_decdn-seeds.{domain}` for each domain in a governance-maintained seed list. Record format: `nodeId=<hex>; addrs=<multiaddr>,<multiaddr>`.

Each client release ships with a built-in default seed list compiled into the binary; the `dns_seeds` configuration key (see [Client Configuration](#client-configuration)) provides a runtime override. Seed domains are maintained by governance ([ADR 009](009-governance.md)) and updated via new client releases or local config. An attacker must compromise both the RPC endpoint and all effective DNS seed domains to fully eclipse a client.

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
- **App server:** For encrypted content ([ADR 006](006-e2e-encryption.md)), the client trusts the app server to deliver correct epoch keys and envelopes over `cdn/keys/v1`. The QUIC handshake authenticates the app server's NodeId; the session token binds the client to its subscriber account. The app server is outside the CDN protocol boundary but shares the iroh transport layer.
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
rpc_url = "https://<arb-sepolia-rpc-endpoint>"
region = ""                                  # optional ISO 3166-1 alpha-2

[bootstrap]
registry_refresh_secs = 600                  # 10 minutes
# Production only — empty in PoC
dns_seeds = []
min_peer_diversity = 3                       # Option C threshold (production)

[app_server]
# Required for encrypted content (ADR 006). Omit for plaintext PoC.
node_id = ""                                 # app server's iroh NodeId (hex)
addrs = []                                   # app server multiaddrs

[keys]
# Paths use ~ as shorthand; the client MUST perform home-directory expansion.
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

The client queries all configured seed domains, cross-checks returned NodeIds against the on-chain registry (seeds must be staked nodes), and merges valid entries into the peer table. For matching NodeIds, the multiaddrs from the on-chain registry are used as the canonical source of truth — addresses from DNS are discarded in favour of registry data, preventing a compromised seed domain from redirecting traffic for a valid NodeId. Seeds whose NodeId is not found in the registry are discarded entirely with a warning.

## Consequences

**Positive:**

- Consolidates all client behaviour scattered across ADRs 001, 003, 005, 006, and 008 into a single canonical specification
- Resolves the eclipse attack open question from ADR 003 with a concrete decision (Option B for production)
- Establishes an explicit trust boundary, making security assumptions auditable
- PoC key management is simple (file-based EOA or Safe wallet) with a clear production upgrade path (Safe multisig with session keys — see [ADR 024](024-account-abstraction.md))
- Bootstrap procedure is fully specified end-to-end, unblocking PoC implementation

**Negative:**

- DNS seed list introduces a governance-maintained out-of-band dependency for production
- File-based key storage in PoC is not suitable for production (acceptable for testnet with test funds)
- Ephemeral bindings mean a disconnected client loses priority staking benefits until reconnection
- NTP synchronization is a hard requirement for gossip validation — clients without NTP will reject valid gossip and build stale peer tables
- Hardware wallet voucher signing is confirmed infeasible — resolved by Safe session keys ([ADR 024](024-account-abstraction.md))

## Open Questions

1. **Gossip relay:** Should clients forward received gossip messages to other peers, or only consume? Recommendation: consume-only for PoC to minimize client complexity. Evaluate relay participation for production to improve message propagation.
2. ~~**Delegated voucher signer:**~~ Resolved — [ADR 024](024-account-abstraction.md) specifies Safe session keys as the mechanism for high-frequency voucher signing, replacing both the derived hot key and the delegated signer contract approach (PR 196).
3. **Mobile/web clients:** This ADR assumes a desktop/server client with filesystem access. Mobile and web clients are listed as non-goals in the architecture overview but may need adapted key storage and bootstrap mechanisms.

## ADRs Affected

- **[ADR 001](001-network.md):** Client bootstrap and registry unavailability sections are superseded by this ADR for client-specific behaviour. ADR 001 retains the specification for node bootstrap.
- **[ADR 003](003-payments.md):** Eclipse attack options (A/B/C) are resolved — Option B for production, registry-only for PoC, Option C as supplementary policy.
- **[ADR 005](005-protocol.md):** `StreamRequest` ephemeral binding fields are specified in full lifecycle context here.
- **[ADR 008](008-reputation.md):** Client reputation contribution is clarified — local observations only, no gossip submissions.
