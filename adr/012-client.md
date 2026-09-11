# ADR 012: Client Architecture, Bootstrap, and Trust Model

**Date:** 2026-04-03
**Status:** Draft

## Context

Clients are referenced throughout ADRs 001–011 — they pay for content, hold Ethereum keys that authorize fund movement, track the set of nodes from the on-chain registry, and decrypt content envelopes — but no ADR defines the client as a coherent entity. Four gaps block PoC functionality:

1. **Bootstrap** — how a client discovers initial peers. [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) specifies registry query and retry, but interleaves it with node-specific concerns and omits client identity loading.
2. **Key management** — clients hold an iroh Ed25519 key (NodeId) and an Ethereum secp256k1 key. That key carries two roles the pool keeps separate: **owner** (opening pools, topping up, signing capabilities, receiving reclaims) and **voucher signer** (a capability-authorized key whose vouchers a node accepts). Generation, storage, and rotation are unspecified.
3. **Identity lifecycle** — [ADR 005](005-protocol.md#adr-005-wire-protocol) defines ephemeral NodeId-to-Ethereum bindings in `StreamRequest` but does not specify creation, rotation, or expiry.
4. **Trust boundary** — what the client verifies vs. trusts. Implied across multiple ADRs, never stated explicitly.

This ADR consolidates all client-specific behavior into one canonical specification.

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
- Picks the node to stream from out of the bonded set it already knows — its persisted peer store, or the on-chain registry — and probes candidates over `cdn/probe/v1` ([ADR 039 § Source set and selection](039-multi-source-parallel-fetch.md#source-set-and-selection)). It issues no `cdn/dht/v1` lookup: a node that lacks the blob resolves holders on the client's behalf through its own pull leg ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale))
- Ranks candidates by measured RTT only ([ADR 037 § Client selection policy](037-regional-proxy-warming.md#client-selection-policy-latency-driven-proxy-preference)); it keeps no reputation score and shares no observations with anyone
- Maintains a local peer store: registry-fed identity plus a per-peer latency EWMA and a failure-suppression stamp ([ADR 037 § Client RTT map](037-regional-proxy-warming.md#client-rtt-map-and-latency-discovery))
- Signs vouchers authorizing off-chain USDC payments, from a capability-authorized signer key the pool owner delegates (its own key by default)

### Bootstrap Procedure

Startup sequence from first launch to ready state:

```
1. Load or generate iroh identity key (see Key Management below)
2. Load Ethereum key from encrypted keystore
3. Query on-chain registry: paginated getRegisteredNodes(offset, 100) calls,
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
     On stored peer identity present: fall back to the peer store, and tell
       the user the identity is cached and how old it is — those nodes may
       have been deactivated or slashed since it was last seen
     On no stored identity: exit with error —
       "Cannot reach bootstrap sources. Check network connectivity
        and RPC endpoint configuration."
5. Feed each peer's registry multiaddrs to iroh as direct-address hints;
     connect directly to a reachable peer, using an iroh relay only as the
     NAT-traversal fallback
6. Build node list from the resolved bootstrap peers
     (registry results, or the store's identity records on fallback)
7. Upsert identity for every returned node into the peer store
     A successful but *empty* read is the exception: it does not touch the
     store, since an emptied registry is no reason to discard the last
     known-good peer identities.
8. Begin periodic registry refresh (every 10 minutes)
```

The peer store is a directory of per-node JSON files under the resolved client data dir — `--data-dir`, else `[identity] data_dir`, defaulting to `~/.decdn/client` — so it moves with the rest of the client's state rather than living at a fixed path.

The on-chain registry is the sole discovery source. Nodes and clients both build their node list from it. The peer store's identity half — the last registry snapshot — covers a transient RPC outage. That snapshot includes each peer's last-seen `multiaddrs`, so an outage-fallback dial reaches a reachable peer directly, without iroh discovery ([ADR 001 § Node Discovery](001-network.md#node-discovery-registry)); a stale cached address loses the iroh path race but never fails a dial, so the store only ever speeds a fetch. The store's stats half — round-trip latency and quoted price, sampled from probes and completed streams — lets a client route a fetch without probing when it holds enough fresh candidates, going straight to a `StreamRequest` and taking price from the node's `StreamResponse`. The two halves of a record age on independent clocks: identity refreshes and prunes on the registry's own horizon (the periodic registry refresh of step 8), while stats age against the RTT map's own staleness TTL ([ADR 037 § Client RTT map and latency discovery](037-regional-proxy-warming.md#client-rtt-map-and-latency-discovery), `rtt_map.staleness_secs`) — a record can carry fresh identity with stale stats, or the reverse. A client stays online only long enough to fetch, so it holds no long-lived network role. The store persists across invocations, so a later run can skip both the registry read and the probe.

Node-side registry interaction and bootstrap is in [ADR 019 § Step 3.3](019-node-onboarding.md#step-33--build-initial-node-view-from-on-chain-registry).

### Key Management

Clients manage two independent cryptographic keys.

#### iroh Identity Key (Ed25519)

- **Purpose:** Defines the client's `NodeId` in the peer mesh. Used for QUIC connection authentication on `cdn/client/v1` and `cdn/probe/v1`.
- **Generation:** Created at first startup via `iroh::SecretKey::generate()`.
- **Storage (PoC):** File at `~/.decdn/iroh_key`, permissions `0600`. No encryption — the file contains the raw 32-byte secret key.
- **Storage (production):** Platform keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service API).
- **Rotation (PoC):** Not supported. Deleting the key file and restarting generates a new identity.
- **Rotation (production):** Generate a new key, reconnect to all nodes. The old NodeId becomes unreachable. Open payment pools are unaffected — they are keyed by Ethereum address, not NodeId.

#### Ethereum Key (secp256k1)

- **Purpose:** Signs vouchers (EIP-712), opens/closes payment pools (on-chain transactions), signs capabilities that delegate signers, and signs ephemeral `BindNodeId` messages. Only operators bond TOKEN (via `CapacityBond`); clients have no bonding path.
- **Two roles, one key by default:** the pool `owner` funds the deposit and signs capabilities; a **signer** signs vouchers. A single-user client is its own sole signer, so one key funds and signs. To split the roles, the owner key stays cold and holds the deposit while it issues a capped, expiring capability to a hot signer key that is online at delivery speed. The split is decided per capability — the owner can delegate many signers, cap each, and retire any by letting its `expiry` lapse.
- **Generation:** Not generated by the client software. Imported from an existing wallet.
- **PoC:** An encrypted keystore file at `~/.decdn/eth_keystore` (Web3 Secret Storage format, EOA) — the documented default. A Safe or other smart-account address is accepted by every contract, since all contracts use `SignatureChecker`. It cannot be *served* as a signer — the node verifies bindings and vouchers off-chain by recovery only ([ADR 024 § Off-Chain ERC-1271 Verification](024-account-abstraction.md#off-chain-erc-1271-verification)) — but it can own a pool and issue a capability to an EOA signer, in which case only the EOA is ever recovered and its vouchers are servable.
- **Production:** Safe smart wallet, required for the session-key path. 1-of-1 for simplicity, 2-of-3 for high-value accounts. A **session key** authorized via the Safe's Session Key Module handles high-frequency voucher signing — see [ADR 024 § Session Keys — Deferred to Production via ERC-7579 smartsessions](024-account-abstraction.md#session-keys--deferred-to-production-via-erc-7579-smartsessions).

**Voucher signing with session keys:** A hash chain meters delivery between signatures ([ADR 003 § Hash-chain metering (PayWord)](003-payments.md#hash-chain-metering-payword)), so a 100 MB download needs two EIP-712 voucher signatures rather than one per interval. The frequency argument now rests on the residual signatures — one per 255 MiB rollover, one per stream open, and one per close — and on holding a key hot at all. Hardware wallets require physical confirmation per signature (2–5 seconds each), which is infeasible. A session key solves this: a lightweight secp256k1 key generated at session start, authorized once by the Safe owners, held in memory for the session, signing vouchers at wire speed. It is time-bounded, scope-limited to voucher signatures, and revocable by the Safe owners. See [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support) for the full design.

A capability-authorized signer already delivers most of that shape without any account-abstraction machinery: a fresh EOA issued a capability is scope-limited to that pool's vouchers, bounded by its `spending_cap`, retired by its `expiry`, and needs no owner confirmation per signature. So it gives scope-limiting, a spending bound, *and* time-bounded revocation — more than an immutable per-channel pin gives. What it does not help is a smart account that must sign for itself. That is what the ERC-7579 session-key path is still for.

#### Key Summary

| Key | Algorithm | PoC Storage | Production Storage | Rotation |
| --- | --- | --- | --- | --- |
| iroh identity | Ed25519 | `~/.decdn/iroh_key` (0600) | Platform keychain | New key + reconnect |
| Ethereum | secp256k1 | `~/.decdn/eth_keystore` (encrypted) | Safe wallet + session key ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)) | Wallet-level |

### Identity Lifecycle

Client identity bindings are **ephemeral and per-connection**, per [ADR 003 — Off-Chain Ephemeral Binding](003-payments.md#off-chain-ephemeral-binding-for-clients) and [ADR 005](005-protocol.md#adr-005-wire-protocol).

**Lifecycle:**

1. **Startup:** Load iroh key (→ `NodeId`). Load the Ethereum key from the keystore (EOA); Production migrates to Safe-7579 + `erc7579/smartsessions` for high-frequency signing ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)).
2. **Connect:** Establish QUIC connection to a node via `cdn/client/v1`.
3. **Bind:** First `StreamRequest` on the connection includes `ethereum_address` and `binding_signature` — an EIP-712 `BindNodeId(nodeId, nonce=0)` signature. The `nonce=0` sentinel indicates an ephemeral (off-chain) binding.
4. **Session:** The node verifies the signature by recovering the signer from the fixed 65-byte form and comparing it against the claimed address; smart-account clients are rejected fail-closed until off-chain ERC-1271 verification lands ([ADR 024](024-account-abstraction.md#off-chain-erc-1271-verification)). It then caches the verified address for the connection's lifetime and refuses any request whose voucher `signer` is not that address; the address a voucher must recover to is the capability-authorized signer, not the binding ([ADR 003 § Off-Chain Ephemeral Binding](003-payments.md#off-chain-ephemeral-binding-for-clients)). Subsequent requests on the same connection omit these fields.
5. **Disconnect:** The node discards the cached binding. No on-chain state to clean up.

**Security properties of `nonce=0`:** The ephemeral binding is not a replay vulnerability: the node uses it only for the authenticated QUIC connection on which it arrived — a binding from connection A is never applied to connection B. On-chain `bindNodeId` ([ADR 003](003-payments.md#adr-003-payment-model)) also starts at nonce 0 (`bindingNonce[msg.sender]` is initially 0), so the nonce value alone does not distinguish off-chain from on-chain bindings. Protection against on-chain replay is the EIP-712 domain separator: the node verifies the off-chain binding locally by recovery (not on-chain), while on-chain `bindNodeId` verifies against `DOMAIN_SEPARATOR` (which includes the `CapacityBond` contract address and chain ID). A signature produced for off-chain use cannot pass the on-chain domain check unless the client uses the exact same domain parameters — and if it does, the on-chain binding consumes the nonce, preventing reuse.

**Key rotation:** Generating a new iroh key and reconnecting produces a new NodeId. The client signs a fresh `BindNodeId` with the same Ethereum key and the new NodeId. Open payment channels remain valid — channels are keyed by `(client_ethereum_address, provider_ethereum_address, nonce)`, not by NodeId.

**Key compromise response:**

| Compromised key | Impact | Response |
| --- | --- | --- |
| iroh Ed25519 | Attacker can impersonate client NodeId (connect to nodes) but cannot sign vouchers or move funds. | Generate new iroh key, reconnect |
| Ethereum secp256k1 (voucher signer) | Attacker can sign vouchers up to this signer's remaining `spending_cap` — and no further. Vouchers for a different signer, and every asset the owner holds outside this pool, are untouched. | The owner stops serving this signer and lets its capability `expiry` lapse (native revocation), so the exposure is bounded by the cap and retired at expiry — no on-chain move is needed against already-signed vouchers, which nodes redeem within the grace window regardless ([ADR 003 § Revocation](003-payments.md#revocation)). The owner may then `closePool` and `reclaim` the unspent remainder, and issue a fresh capability to a new signer. |
| Ethereum secp256k1 (owner) | Attacker can move the owner's balance, open pools in its name, and sign capabilities delegating any signer (including itself, the default). | Close and reclaim every pool from the owner key and move to a new owner address. |
| Both | Full impersonation | Close and reclaim all pools immediately. Generate new iroh key. Use a new Ethereum address for future sessions. |

### Trust Boundary

#### Verified (trustless) — the client cryptographically validates these

- **Content integrity:** BLAKE3 hash verification on every received blob. A malicious node cannot serve corrupted data.
- **Voucher binding:** EIP-712 signatures on vouchers are produced by a capability-authorized signer key. The owner controls how much it authorizes (the `spending_cap`) and — since it issues the capability — who may sign and until when (`expiry`).
- **Rate commitment:** `StreamResponse` carries a `slash_sig` (EIP-712 secp256k1) over the response fields, binding the node's registered Ethereum identity to the quoted `rate_per_mb`. A rate mismatch vs. the probe response within 30 seconds is slashable ([ADR 005](005-protocol.md#adr-005-wire-protocol)).
- **Node identity:** QUIC handshake authenticates the remote NodeId (Ed25519). The client knows it is communicating with the registered node.
- **Payment pool state:** On-chain, publicly verifiable. The owner can always close and reclaim; a node can always redeem its lanes.

#### Trusted — the client relies on external guarantees

- **RPC endpoint:** Returns correct registry data. A compromised RPC can return a fabricated node list.
- **Registry correctness:** The `CapacityBond` contract accurately reflects bonded operators. Enforced by EVM execution — trust in the chain, not any specific party.

#### Not trusted — the client does not rely on these

- Any individual node's self-reported metadata (region) beyond what is signed and slashable. The region claim never enters selection: the client ranks by measured RTT, which a node cannot forge ([ADR 037 § Client selection policy](037-regional-proxy-warming.md#client-selection-policy-latency-driven-proxy-preference)).
- Reputation reported by other peers. The client ranks nodes solely from its own measured RTT and failure history; it consumes no reputation score, its own or anyone else's.
- Node availability promises beyond signed probe responses.

### Client Configuration

All client state resides under `~/.decdn/`, with the per-client data dir (`--data-dir` / `[identity] data_dir`) defaulting to `~/.decdn/client/`:

```
~/.decdn/
├── config.toml               # Client configuration
├── iroh_key                  # Ed25519 secret key (0600)
└── client/                   # the resolved client data dir
    ├── keystore.json         # Encrypted Ethereum keystore (Web3 Secret Storage)
    ├── buyer-pools.redb   # Buyer-side payment-pool store
    └── peers/                 # Per-node peer knowledge base (identity + stats), one JSON file per node
```

Default configuration:

A client and a node share one schema (`decdn_common::config::FileConfig`), and `deny_unknown_fields`
rejects only unknown keys, not node-only sections a client leaves unused. So a client needs no config
of its own: `decdn config init --chain <name>` writes a ready-to-run file — chain id, RPC endpoint,
and contract addresses baked in from the shipped deployment manifest — and a client uses the same
file the node does, ignoring `[cache]`, `[payment]`, and the bond. `decdn fetch` also reads every one
of these values as a flag, so a client can run with no config file at all. The client-relevant shape,
in current schema terms:

- `[identity]` — `data_dir` (holds the buyer payment-pool store and, by default, the ETH
  keystore) and an optional ISO 3166-1 alpha-2 `region`.
- `[blockchain]` — `rpc_url`, `eth_keystore`, `chain_id`, plus the `payment_channel_address` /
  `slash_judge_address` (and `capacity_bond_address` for auto-discovery) a client needs to pay for
  and verify delivery. The RPC URL and keystore live here, **not** under `[network]`/`[keys]`.
- `[client]` — an optional `region_allowlist` list of regions. When set, it restricts discovery and
  probing to nodes whose `region_hint` matches; empty or absent means no filter.

The voucher byte-accounting granularity is a fixed protocol constant that every client and node
reads directly ([ADR 003 § Key parameters](003-payments.md#adr-003-payment-model)), so a client has
nothing to configure for it — which is why a client leaves the `[payment]` section unset.

The Ed25519 node key and the peer store have no config keys of their own: both live inside
the resolved data dir (see the tree above), so they move with `--data-dir` / `[identity] data_dir`.

**Registry refresh (not yet implemented).** The bootstrap step-8 periodic registry refresh has no
code behind it: `decdn fetch` is one-shot today and re-reads the registry per invocation. When
implemented it will be a client knob (a `registry_refresh_secs`-style ~10-minute cadence); it is
recorded here as intended design, not a live config key.

## Consequences

### Positive

- Consolidates client behavior scattered across ADRs 001, 003, 005, and 008 into a single canonical specification
- Establishes an explicit trust boundary, making security assumptions auditable
- PoC key management is simple (a file-based EOA keystore) with a clear production upgrade path (Safe multisig with session keys — see [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support))
- Bootstrap procedure is fully specified end-to-end, unblocking PoC implementation

### Negative

- File-based key storage in PoC is not suitable for production (acceptable for testnet with test funds)
- Hardware wallet voucher signing is confirmed infeasible — resolved by Safe session keys ([ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support))

## Multi-Source Download

Splitting a large blob across multiple nodes and fetching byte ranges in parallel is specified in [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1). Sequential single-source delivery is the default.

## Download Resume

A `decdn pull` interrupted by a crash, kill, or network drop resumes from the partial output file already on disk: the client re-hashes the bytes it has, discards any trailing unverified remainder, and continues the fetch from the last BLAKE3-verified offset via `StreamRequest{byte_offset}`. If the client's pool is still open on-chain, it reuses it — the same deposit backs the resumed fetch; only if the pool is closing or closed does the client open a new one.
