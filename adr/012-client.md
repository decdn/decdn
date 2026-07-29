# ADR 012: Client Architecture, Bootstrap, and Trust Model

**Date:** 2026-04-03
**Status:** Draft

## Context

Clients are referenced throughout ADRs 001–011 — they pay for content, hold Ethereum keys that authorize fund movement, maintain peer tables, and decrypt content envelopes — but no ADR defines the client as a coherent entity. Four gaps block PoC functionality:

1. **Bootstrap** — how a client discovers initial peers. [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh) specifies registry query and retry, but interleaves it with node-specific concerns and omits client identity loading.
2. **Key management** — clients hold an iroh Ed25519 key (NodeId) and an Ethereum secp256k1 key. That key carries two roles a channel keeps separate: **funder** (opening channels, topping up, receiving refunds) and **voucher signer** (the address the channel pins, and the only one whose vouchers it accepts). Generation, storage, and rotation are unspecified.
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
- Uses `cdn/dht/v1` FIND_VALUE for content discovery; falls back to `cdn/probe/v1` broadcast during bootstrap (see [ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale))
- Contributes only local reputation observations ([ADR 008](008-reputation.md#adr-008-reputation-system)); publishes nothing to the mesh
- Maintains a local peer table (from the on-chain registry) and reputation scores
- Signs vouchers authorizing off-chain USDC payments, from the address the channel pinned as its `voucherSigner`

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

The on-chain registry is the sole discovery source; a cached peer list from the last successful query covers a transient RPC outage. Clients do not join the iroh-gossip mesh: they neither subscribe to nor relay `NodeAnnounce`. Gossip propagation is the job of bonded nodes, which carry economic accountability (slashing, reputation) for relay correctness and availability. A client stays online only long enough to fetch and gains nothing from mesh participation.

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
- **Two roles, one key by default:** `openChannel` pins the address whose vouchers the channel accepts. A client that passes the zero address pins itself, so the same key funds and signs. A client that names another address splits the roles: the funder key stays cold and holds the deposit, and only the pinned signer needs to be online at delivery speed. The pin is permanent for the life of the channel — there is no setter — so the split is decided per channel at open.
- **Generation:** Not generated by the client software. Imported from an existing wallet.
- **PoC:** An encrypted keystore file at `~/.decdn/eth_keystore` (Web3 Secret Storage format, EOA) — the documented default. A Safe or other smart-account address is accepted by every contract, since all contracts use `SignatureChecker`. It cannot be *served* as a signer — the node verifies bindings and vouchers off-chain by recovery only ([ADR 024 § Off-Chain ERC-1271 Verification](024-account-abstraction.md#off-chain-erc-1271-verification)) — but it can fund a channel that pins an EOA as `voucherSigner`, in which case only the EOA is ever recovered and the channel is servable.
- **Production:** Safe smart wallet, required for the session-key path. 1-of-1 for simplicity, 2-of-3 for high-value accounts. A **session key** authorized via the Safe's Session Key Module handles high-frequency voucher signing — see [ADR 024 § Session Keys — Deferred to Production via ERC-7579 smartsessions](024-account-abstraction.md#session-keys--deferred-to-production-via-erc-7579-smartsessions).

**Voucher signing with session keys:** At the default 1 MB voucher cadence, a 100 MB download requires 100 EIP-712 voucher signatures. Hardware wallets require physical confirmation per signature (2–5 seconds each), which is infeasible. A session key solves this: a lightweight secp256k1 key generated at session start, authorized once by the Safe owners, held in memory for the session, signing vouchers at wire speed. It is time-bounded, scope-limited to voucher signatures, and revocable by the Safe owners. See [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support) for the full design.

The channel's pinned `voucherSigner` already delivers most of that shape without any account-abstraction machinery: a fresh EOA pinned at open is scope-limited to one channel's vouchers, bounded by that channel's deposit, and needs no owner confirmation per signature. What it does not give is revocation or a time bound — the pin is immutable and lives as long as the channel — nor does it help a smart account that must sign for itself. Those two are what the ERC-7579 session-key path is still for.

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
4. **Session:** The node verifies the signature by recovering the signer from the fixed 65-byte form and comparing it against the claimed address; smart-account clients are rejected fail-closed until off-chain ERC-1271 verification lands ([ADR 024](024-account-abstraction.md#off-chain-erc-1271-verification)). It then caches the verified address for the connection's lifetime and refuses any request naming a channel whose pinned `voucherSigner` is not that address; the address a voucher must recover to is that on-chain pin, not the binding ([ADR 003 § Off-Chain Ephemeral Binding](003-payments.md#off-chain-ephemeral-binding-for-clients)). Subsequent requests on the same connection omit these fields.
5. **Disconnect:** The node discards the cached binding. No on-chain state to clean up.

**Security properties of `nonce=0`:** The ephemeral binding is not a replay vulnerability: the node uses it only for the authenticated QUIC connection on which it arrived — a binding from connection A is never applied to connection B. On-chain `bindNodeId` ([ADR 003](003-payments.md#adr-003-payment-model)) also starts at nonce 0 (`bindingNonce[msg.sender]` is initially 0), so the nonce value alone does not distinguish off-chain from on-chain bindings. Protection against on-chain replay is the EIP-712 domain separator: the node verifies the off-chain binding locally by recovery (not on-chain), while on-chain `bindNodeId` verifies against `DOMAIN_SEPARATOR` (which includes the `CapacityBond` contract address and chain ID). A signature produced for off-chain use cannot pass the on-chain domain check unless the client uses the exact same domain parameters — and if it does, the on-chain binding consumes the nonce, preventing reuse.

**Key rotation:** Generating a new iroh key and reconnecting produces a new NodeId. The client signs a fresh `BindNodeId` with the same Ethereum key and the new NodeId. Open payment channels remain valid — channels are keyed by `(client_ethereum_address, provider_ethereum_address, nonce)`, not by NodeId.

**Key compromise response:**

| Compromised key | Impact | Response |
| --- | --- | --- |
| iroh Ed25519 | Attacker can impersonate client NodeId (connect to nodes) but cannot sign vouchers or move funds. | Generate new iroh key, reconnect |
| Ethereum secp256k1 (voucher signer) | Attacker can sign vouchers draining the deposits of the channels that pinned this address — and no others. Channels pinning a different signer, and every asset the funder holds outside those deposits, are untouched. | Close the affected channels from the funder key, which the attacker does not hold. `closeChannelWithoutVoucher(channelId)` is the primitive: it needs no voucher, closes at the recorded watermark, and starts the dispute window at once — bounding the exposure to that window rather than to the channel's remaining lifetime, since a forged voucher can then only enter via `disputeChannel` before the deadline. If the attacker submitted a higher-nonce close first, dispute within the window ([ADR 003](003-payments.md#adr-003-payment-model)). The pin is immutable, so there is no revocation short of closing; open later channels against a fresh signer. |
| Ethereum secp256k1 (funder) | Attacker can move the funder's balance and open channels in its name, in addition to signing on every channel that pinned the funder as its own signer (the default). | Close every channel as above and move to a new funding address. |
| Both | Full impersonation | Close all channels immediately. Generate new iroh key. Use a new Ethereum address for future sessions. |

### Trust Boundary

#### Verified (trustless) — the client cryptographically validates these

- **Content integrity:** BLAKE3 hash verification on every received blob. A malicious node cannot serve corrupted data.
- **Voucher binding:** EIP-712 signatures on vouchers are produced by the key the channel pinned as its `voucherSigner`. The client controls how much it authorizes, and — since it chose that signer at open — who may authorize it.
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

The canonical, always-current client config lives at
[`examples/configs/arbitrum-sepolia-client.toml`](../examples/configs/arbitrum-sepolia-client.toml) —
CI-validated against the live schema (`decdn_common::config::FileConfig`), so it cannot drift from
the implementation the way an inline copy here would. Per the workspace single-source-of-truth rule
this ADR points at that file rather than restating the schema (and risking the two disagreeing). The
client-relevant shape, in current schema terms:

- `[identity]` — `data_dir` (holds the buyer payment-channel store and, by default, the ETH
  keystore) and an optional ISO 3166-1 alpha-2 `region`.
- `[blockchain]` — `rpc_url`, `eth_keystore`, `chain_id`, plus the `payment_channel_address` /
  `slash_judge_address` (and `capacity_bond_address` for auto-discovery) a client needs to pay for
  and verify delivery. The RPC URL and keystore live here, **not** under `[network]`/`[keys]`.

A client does not configure the voucher cadence: it sends no `voucher_interval_mb` and follows the
cadence the seller advertises on `cdn/client/v1`
([ADR 003 § Voucher Interval Negotiation](003-payments.md#voucher-interval-negotiation)), which is
why the cited example carries no `[payment]` section.

The Ed25519 node key and the `peers.json` cache have no config keys of their own: both live inside
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

A `decdn pull` interrupted by a crash, kill, or network drop resumes from the partial output file already on disk: the client re-hashes the bytes it has, discards any trailing unverified remainder, and continues the fetch from the last BLAKE3-verified offset via `StreamRequest{byte_offset}`. If the original payment channel is still open on-chain, the client reuses it; if it is closing or settled, a new channel is opened sized for the remaining bytes only.
