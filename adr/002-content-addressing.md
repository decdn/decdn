# ADR 002: Content Addressing

**Date:** 2026-03-28
**Status:** Draft

## Context

Nodes hold and deliver content. Clients need to verify that the bytes they receive are the bytes they asked for, without trusting any node. The network also needs a stable identifier for each blob — one that doesn't change as content propagates across nodes.

## Decision

All blobs are content-addressed by their BLAKE3 hash. The hash is computed once when content is uploaded to a node and serves as the canonical identifier clients use to request content.

The protocol is content-agnostic. It stores and delivers arbitrary blobs with no assumptions about format, structure, or metadata. Applications may define their own metadata or manifest layers on top (e.g., linking multiple blobs, adding descriptive fields), but these are opaque to the delivery network — the protocol sees only hashes and bytes.

Blob identity is intrinsic to the content: the same bytes always produce the same hash, regardless of which node holds them. Clients verify every received blob against its known hash — no node can serve corrupted data without immediate detection.

The mapping from hash to the actual backing storage location (e.g., which S3 key, which local file path) is internal to each origin-backed node and never exposed to the network. Other nodes and clients have no knowledge of a node's backend — they only know hashes and NodeIds.

BLAKE3 is iroh's native hash function, so there is no translation layer between blob IDs and the transport layer.

## Publisher Identity and Namespaces

Content addressing answers "what is this blob?". Origin governance answers "who is responsible for serving this blob?". The two questions are independent: a BLAKE3 hash is intrinsic to the bytes, but the network needs a stable identity for the party that publishes the bytes so that the DAO can authorize specific operators to act as origins for their content (see [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)).

A **publisher** is an Ethereum address that owns at least one namespace in the `PublisherRegistry` contract ([ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model)). Publisher status is acquired implicitly on the first successful `createNamespace()` call from an address — there is no separate registration transaction. Transferring control of an individual namespace is a gated action (see [§ Contract: PublisherRegistry](#contract-publisherregistry) for the namespace lifecycle).

A **namespace** is a publisher-owned `uint256` identifier under which blob hashes are claimed. Publishers may register multiple namespaces (subject to the anti-squatting cap in [ADR 009](009-governance.md#adr-009-governance-model)) so that distinct content sets can be governed independently — for example, a media company may operate one namespace per product line so that takedowns and origin assignments for one product do not entangle the others.

A **content claim** is an on-chain binding of a `(namespaceId, blake3Hash)` pair, recorded by the namespace's owner via `PublisherRegistry.claimContent`. Claims are append-only and content-immutable: a claim, once recorded, cannot be moved or revoked; the underlying bytes are untouched (BLAKE3 makes the hash binding cryptographic). Multiple non-zero namespaces may claim the same hash independently, with no coordination between publishers — see [§ Multi-claim semantics](#multi-claim-semantics) below. Claims are the authoritative on-chain answer to "which registered namespaces, if any, claim this blob?".

### Default-open namespace

A reserved namespace ID (`namespaceId == 0`) is the **default-open namespace**. Any blob is implicitly claimed under it; no `claimContent` call is required. Operators authorized by the DAO via the default-open allow-list (see [ADR 011 § Default-open allow-list](011-content-takedown.md#default-open-allow-list)) may serve as origin for default-open blobs; cache-only serving by any staked operator remains permissionless. The allow-list is the single global authority over default-open origin behaviour. Publishers seeking per-content durability guarantees, takedown accountability, or namespace-scoped origin sets opt in by registering and claiming under a non-zero namespace.

Default-open is the operative regime for any hash with no non-zero claims; once at least one non-zero namespace claims the hash, those namespaces' authorized operator sets become origin authorities for that hash alongside the default-open allow-list (see [§ Multi-claim semantics](#multi-claim-semantics)).

### Multi-claim semantics

Any number of non-zero namespaces may claim the same `blake3Hash`, by independent publishers, with no coordination. Each claim registers a namespace as a responsible origin authority for the hash. The set of operators authorized to serve the hash as origin is the union of the operator sets across all claiming namespaces, plus the default-open allow-list if the hash has no non-zero claims.

This design choice is deliberate. A first-write-wins / one-claim rule would create two failure modes:

- **Stranded hashes.** A namespace owner who loses keys, abandons, or refuses cooperation strands every hash they have claimed: no path exists for another publisher or the DAO to authorize fresh origin operators for that hash. The namespace lifecycle (revocation, transfer) all require the current owner.
- **Squatting.** Any party could pre-claim popular hashes (e.g., a future release ISO's BLAKE3) and block the legitimate publisher, with no anti-squatting mechanism short of governance overrides.

Multi-claim eliminates both: claims do not block other claims; defunct namespaces do not block alternative origin authorities; squatting gates nothing because the squatter cannot prevent independent claims. The trade-off — that the protocol does not present an on-chain claim as "the official publisher of this content" — is accepted because that framing is not load-bearing on any protocol primitive: takedown is hash-keyed, and origin authorization is consumed off-chain by routing/discovery layers that can OR-search the claiming set without protocol-level signaling.

### Why this lives in [ADR 002](002-content-addressing.md#adr-002-content-addressing)

Content identity (the BLAKE3 hash) and publisher identity are paired: every claim is a binding between the two. Defining publisher and namespace here keeps the identity primitives in one place so that ADRs [011](011-content-takedown.md#origin-assignment-authority) (governance authority), [016](016-contract-interactions.md#adr-016-smart-contract-interaction-model) (contract surface), and [022](022-content-discovery.md#adr-022--content-discovery-at-scale) (DHT publication semantics) can refer back to a single canonical definition.

### Contract: PublisherRegistry

```solidity
interface IPublisherRegistry {
    // Namespace creation — permissionless. The first successful call from a
    // given address implicitly registers it as a publisher (no separate
    // registration step). Every call — including the first — reverts if it
    // would push namespaceCount(caller) above maxNamespacesPerPublisher.
    function createNamespace() external returns (uint256 namespaceId);

    // Namespace ownership transfer with a 7-day timelock.
    // initiateTransfer queues the transfer; finalizeTransfer completes it after
    // the timelock; cancelTransfer (callable by current owner only) aborts.
    function initiateNamespaceTransfer(uint256 namespaceId, address newOwner) external;
    function finalizeNamespaceTransfer(uint256 namespaceId) external;
    function cancelNamespaceTransfer(uint256 namespaceId) external;

    // Content claim — only callable by the namespace's current owner.
    // Multi-claim: any number of non-zero namespaces may claim the same hash
    // independently. Reverts only if THIS namespace has already claimed this
    // hash (idempotency); other namespaces' prior claims do not block.
    function claimContent(uint256 namespaceId, bytes32 blake3Hash) external;

    // Views
    function namespaceOf(bytes32 blake3Hash) external view returns (uint256[] memory namespaceIds);
    function ownerOf(uint256 namespaceId) external view returns (address);
    function namespaceCount(address publisher) external view returns (uint256);
    function pendingTransfer(uint256 namespaceId) external view returns (address newOwner, uint256 readyAt);
    function maxNamespacesPerPublisher() external view returns (uint256);
    function namespaceTransferTimelock() external view returns (uint64);

    // Governable parameter setters (GOVERNANCE_ROLE; standard 48h timelock).
    // Bounds enforced at the contract layer per ADR 009:
    //   - maxNamespacesPerPublisher: [1, 1000]
    //   - namespaceTransferTimelock: [86400, 2592000] (in seconds; 24 h – 30 days)
    function setMaxNamespacesPerPublisher(uint256 newMax) external;
    function setNamespaceTransferTimelock(uint64 newTimelock) external;

    // Events
    event NamespaceCreated(uint256 indexed namespaceId, address indexed owner);
    event NamespaceTransferInitiated(uint256 indexed namespaceId, address indexed from, address indexed to, uint256 readyAt);
    event NamespaceTransferred(uint256 indexed namespaceId, address indexed from, address indexed to);
    event ContentClaimed(uint256 indexed namespaceId, bytes32 indexed blake3Hash, address indexed claimant);
    event MaxNamespacesPerPublisherUpdated(uint256 oldValue, uint256 newValue);
    event NamespaceTransferTimelockUpdated(uint64 oldValue, uint64 newValue);
}
```

`namespaceOf(hash)` returns an empty array for any hash not explicitly claimed; default-open semantics apply. The view never reverts on unknown hashes — callers cannot distinguish "hash unknown to the protocol" from "hash served as default-open" via this view, which is correct: both states are operationally identical. Storage is a per-hash `uint256[]` set of claiming namespaces — append-only since claims are content-immutable, never moved or revoked.

Per-publisher namespace cap and ownership-transfer timelock are governable parameters with safety bounds (see [ADR 009](009-governance.md#adr-009-governance-model)). Defaults: `maxNamespacesPerPublisher = 100` (anti-squatting; bounded `[1, 1000]`), `namespaceTransferTimelock = 604800` seconds / 7 days (key-compromise mitigation; bounded `[86400, 2592000]` / `[24 h, 30 days]`). Values are stored on `PublisherRegistry` itself and updated via `setMaxNamespacesPerPublisher` / `setNamespaceTransferTimelock` under the standard `TimelockController` delay (`172800` seconds / 48 h); the contract enforces the safety bounds at the setter in seconds and rejects out-of-range writes regardless of caller.

## Consequences

### Positive

- Delivery verification is inherent: hash mismatch on receipt is both detection and proof. No separate proof-of-delivery oracle is needed.
- Content is location-independent: a blob from any node is interchangeable as long as the hash matches. This makes the entire delivery layer transparent to clients.
- Deduplication is automatic: two nodes holding identical bytes share one logical blob identity.
- The hash serves as the slash evidence primitive: a client detecting a BLAKE3 mismatch on received bytes can trigger an on-chain slash via a challenge process. **Resolved:** [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) specifies a single-round optimistic challenge-response for the PoC (challenger posts a signed `StreamResponse` + 100 TOKEN bond; node has 24 hours to counter) with a production upgrade path using interactive keccak256 Merkle proofs over 1024-byte chunks for cryptographic on-chain verification. See [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling) for challenge bond requirements (amount, transfer, forfeit rules) and [ADR 005](005-protocol.md#cdnprobev1--latency-probe) for slashing evidence mechanisms.
- The origin-backed node's backing storage is completely opaque to the network — nobody can discover the origin URL or bypass the payment layer.

### Negative

- BLAKE3 is not a native EVM precompile, so on-chain verification requires an intermediate scheme. [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) specifies an optimistic challenge-response for PoC and a keccak256 Merkle proof protocol for production.
- Content is immutable: updating a blob produces a new hash and a new identity. Applications that need mutable references (e.g., "latest version of X") must manage their own indirection layer above the protocol.
- If an origin-backed node loses its backing data (disk failure, misconfiguration), the content is gone from the network unless another node holds the same blob. Operators are responsible for their own backend durability.
- The protocol imposes no inherent size limit on content addressing — any byte sequence produces a valid BLAKE3 hash regardless of length. Individual nodes may enforce a configurable `max_blob_size` for operational reasons (cache management, connection resource limits). This is a node-level resource policy, not a content-addressing constraint — see [ADR 005](005-protocol.md#error-handling-and-retry-semantics) for the `BlobTooLarge` enforcement mechanism.
