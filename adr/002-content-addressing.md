# ADR 002: Content Addressing

**Date:** 2026-03-28
**Status:** Accepted

## Context

Nodes hold and deliver content. Clients need to verify that the bytes they receive are the bytes they asked for, without trusting any node. The network also needs a stable identifier for each blob — one that does not change as content propagates across nodes.

## Decision

All blobs are content-addressed by their BLAKE3 hash. The hash is computed once when content is uploaded to a node and serves as the canonical identifier clients use to request content.

The protocol is content-agnostic. It stores and delivers arbitrary blobs with no assumptions about format, structure, or metadata. Applications may define their own metadata or manifest layers on top (e.g. linking multiple blobs, adding descriptive fields), but these are opaque to the delivery network — the protocol sees only hashes and bytes.

Publishers may encrypt content before upload. The protocol content-addresses and delivers only the resulting ciphertext, like any other blob. Key distribution is an application concern outside the protocol.

Blob identity is intrinsic to the content: the same bytes always produce the same hash, regardless of which node holds them. Clients verify every received blob against its known hash, so no node can serve corrupted data without immediate detection. Because BLAKE3 is a Merkle tree whose root is the content hash, verification is per-range, not only whole-blob: any chunk range — partial, resumed from an offset, or sourced from a distinct node — is independently verifiable against the root, so a corrupt range is rejected on receipt without possessing the rest of the blob (see [ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)).

The mapping from hash to backing storage location (which S3 key, which local file path) is internal to each origin-backed node and never exposed to the network. Other nodes and clients know only hashes and NodeIds.

BLAKE3 is iroh's native hash function, so there is no translation layer between blob IDs and the transport layer.

## Publisher Identity and Namespaces

Content addressing answers "what is this blob?" — a BLAKE3 hash, intrinsic to the bytes. It does not answer "who serves this blob, and where do I fetch it from?". That is the **namespace's** job. The bare hash carries no origin information; a request that wants a guaranteed origin must also carry the namespace under which the content is published.

A **publisher** is an Ethereum address that owns at least one namespace in the `PublisherRegistry` contract ([ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model)). Publisher status is acquired implicitly on the first successful `createNamespace()` call — there is no separate registration transaction. Transferring a namespace is gated (see [§ Contract: PublisherRegistry](#contract-publisherregistry) for the lifecycle).

A **namespace** is a publisher-owned `uint256` identifier for a content set, and it is the unit of origin addressing and governance. The publisher seats a set of origin operators per namespace via `OriginAssignment`, once governance has vetted the publisher (see [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)); multiple operators may serve one namespace, so redundancy is a namespace-level property. Publishers may create multiple namespaces (subject to the anti-squatting cap in [ADR 009](009-governance.md#adr-009-governance-model)) so distinct content sets are governed independently — for example, one namespace per product line, so takedowns and origin assignments for one product do not entangle the others.

### Hash-to-namespace association

A request pairs the hash with the namespace its content is published under. The requester supplies this pair at fetch time — the application already knows its namespace (e.g. a music app fetches a track with `namespaceId 3`). The chain stores namespace ownership and each namespace's authorized origin set, so on-chain state is bounded by the number of namespaces, independent of how much content each one serves.

The namespace on a request is a **routing hint, not a trust anchor.** A node uses it only to decide which origins to ask; BLAKE3 verification of the returned bytes is independent, so a wrong or hostile namespace hint can only cause a *failed fetch*, never corrupt or mis-attributed delivery.

### Retrieval by namespace

A `cdn/client/v1` request carries a `namespaceId` alongside the hash (see [ADR 005](005-protocol.md#cdnclientv1--paid-delivery-protocol)). The node routes on it:

- **`namespaceId != 0`** — the node routes to the namespace's authorized origins (`OriginAssignment`, [ADR 011](011-content-takedown.md#origin-assignment-authority)). If none hold the bytes, or the origin fetch fails, the fetch fails.
- **`namespaceId == 0`** — the request names no namespace, so there are no authorized origins. The node serves only from its local cache or from peers discovered via the DHT ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)). If no peer advertises the hash, the fetch fails. Namespace-0 content therefore has no durability or availability guarantee — reliable retrieval wants a namespace.

Cache-only serving stays permissionless: any staked operator may re-serve bytes it already holds, for any hash, regardless of namespace. Only the **origin** role is namespace-gated.

### Namespace 0

`namespaceId == 0` is the default namespace, for content published without one. It has no publisher and no authorized origins; the DAO authorizes origins only for registered (non-zero) namespaces. Namespace-0 content is served best-effort from cache or DHT-discovered holders (the path above) and carries no availability guarantee. A publisher wanting durable origins, takedown accountability, or a stable origin set creates a non-zero namespace and has the DAO authorize origins for it.

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

    // Views
    function ownerOf(uint256 namespaceId) external view returns (address);
    function namespaceCount(address publisher) external view returns (uint256);
    function pendingTransfer(uint256 namespaceId) external view returns (address newOwner, uint64 readyAt);
    function maxNamespacesPerPublisher() external view returns (uint64);
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
    event MaxNamespacesPerPublisherUpdated(uint256 oldValue, uint256 newValue);
    event NamespaceTransferTimelockUpdated(uint64 oldValue, uint64 newValue);
}
```

`PublisherRegistry` records namespace ownership and lifecycle. The hash→namespace association is supplied by the requester at fetch time (see [§ Hash-to-namespace association](#hash-to-namespace-association)). Origin authorization for a namespace lives in `OriginAssignment` ([ADR 011](011-content-takedown.md#origin-assignment-authority)).

The per-publisher namespace cap and the ownership-transfer timelock are governable parameters with safety bounds (see [ADR 009](009-governance.md#adr-009-governance-model)). Defaults: `maxNamespacesPerPublisher = 100` (anti-squatting; bounded `[1, 1000]`) and `namespaceTransferTimelock = 604800` seconds / 7 days (key-compromise mitigation; bounded `[86400, 2592000]` / `[24 h, 30 days]`). Both are stored on `PublisherRegistry` itself and updated via `setMaxNamespacesPerPublisher` / `setNamespaceTransferTimelock` under the standard `TimelockController` delay (`172800` seconds / 48 h); the contract enforces the safety bounds at the setter in seconds and rejects out-of-range writes regardless of caller. The `maxNamespacesPerPublisher` cap is enforced at both `createNamespace` and `finalizeNamespaceTransfer` (on the recipient; self-transfers exempt), so it cannot be bypassed by minting namespaces under throwaway addresses and transferring them to a single publisher.

## Consequences

### Positive

- Delivery verification is inherent: hash mismatch on receipt is both detection and proof. No separate proof-of-delivery oracle is needed.
- Content is location-independent: a blob from any node is interchangeable as long as the hash matches. This makes the entire delivery layer transparent to clients.
- Deduplication is automatic: two nodes holding identical bytes share one logical blob identity.
- The origin-backed node's backing storage is completely opaque to the network — nobody can discover the origin URL or bypass the payment layer.
- The chain never stores content hashes: on-chain state is bounded by the number of namespaces and their origin sets, not by the volume of content served.

### Negative

- Content is immutable: updating a blob produces a new hash and a new identity. Applications that need mutable references (e.g. "latest version of X") must manage their own indirection layer above the protocol.
- Reliable retrieval requires knowing the namespace. A bare hash with no namespace (`namespaceId == 0`) is served only best-effort from cache/DHT and has no guaranteed origin; applications are expected to carry the namespace for content they publish.
- If a namespace's authorized origins all lose their backing data (disk failure, misconfiguration), the content is gone from the network unless another node caches it. Redundancy is the publisher's responsibility, met by having the DAO authorize multiple origins for the namespace.
- The protocol imposes no inherent size limit on content addressing — any byte sequence produces a valid BLAKE3 hash regardless of length. A node admits a blob only if it fits the node's disk budget (`cache.cache_size_mb`). This is a node-level resource policy, not a content-addressing constraint — see [ADR 005](005-protocol.md#error-handling-and-retry-semantics) for the `BlobTooLarge` enforcement mechanism.
