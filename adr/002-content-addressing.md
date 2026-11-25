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

A **publisher** is an Ethereum address that owns at least one namespace in the `PublisherRegistry` contract ([ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model)). Publisher status is acquired implicitly on the first successful `createNamespace()` call — there is no separate registration transaction. The owner transfers a namespace in two steps: an offer and an acceptance (see [§ Namespace transfer](#namespace-transfer)).

A **namespace** is a publisher-owned `uint256` identifier for a content set, and it is the unit of origin addressing and governance. The publisher seats a set of origin operators per namespace via `OriginAssignment`, once governance has vetted the publisher (see [ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)); multiple operators may serve one namespace, so redundancy is a namespace-level property. Publishers may create multiple namespaces (subject to the anti-squatting cap in [ADR 009](009-governance.md#adr-009-governance-model)) so distinct content sets are governed independently — for example, one namespace per product line, so takedowns and origin assignments for one product do not entangle the others.

### Hash-to-namespace association

A node serves and advertises a hash only while it holds a live origin attestation for it ([ADR 022 § Origin Attestation](022-content-discovery.md#origin-attestation)). The attestation is signed by an operator that holds an origin seat in the namespace that the attestation names, and it expires. The chain does not record which namespace a hash is published under. An attestation shows one namespace that a seated origin attests the hash in. One origin can serve several namespaces. A request can pair the hash with a namespace that its content is published under. The requester supplies this pair at fetch time when it knows it (e.g. a music app fetches a track with `namespaceId 3`). The chain stores namespace ownership and each namespace's authorized origin set, so on-chain state is bounded by the number of namespaces, independent of how much content each one serves.

The namespace on a request is a **routing hint, not a trust anchor.** A node uses it only to decide which origins to ask; BLAKE3 verification of the returned bytes is independent, so a wrong or hostile namespace hint can only cause a *failed fetch*, never corrupt or mis-attributed delivery.

### Retrieval by namespace

A `cdn/client/v1` request carries a `namespaceId` alongside the hash (see [ADR 005](005-protocol.md#cdnclientv1--paid-delivery-protocol)). The node routes on it:

- **`namespaceId != 0`** — the node routes to the namespace's authorized origins (`OriginAssignment`, [ADR 011](011-content-takedown.md#origin-assignment-authority)). If none hold the bytes, or the origin fetch fails, the fetch fails.
- **`namespaceId == 0`** — the request does not name the namespace. The node serves from its local cache, from peers discovered via the DHT, or from the origins that those peers' origin attestations name ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)). If no valid DHT record exists for the hash, the fetch fails. The namespace is the only route to an origin for content that no node caches, so a request for cold content names its namespace.

Cache-only serving stays permissionless: any staked operator may re-serve bytes it already holds, for any hash that it holds a live origin attestation for, whatever namespace the request names. Only the **origin** role is namespace-gated.

Whether a node relays a miss it cannot serve from itself — a blob it neither holds in cache nor has in its own origin backend — is a node-local policy, `cache.relay_foreign_namespaces`. The default is role-derived. A node with no origin configured is a pure relay edge and relays by default. A node that configures an origin backend serves only what that backend can produce and relays nothing else by default, because an origin is not a general proxy. The gate is backend-authoritative: the node asks its own backend for the hash and declines when the backend lacks it. It does not consult the request's `namespaceId` — that stays a routing hint (above), never a serve gate. The decline is categorical: the node applies it above the cache-hit branch, so it declines a blob its backend lacks even after an earlier relay seeded that blob into the local cache. An operator sets the field explicitly to override either default. A decline is a serve refusal, not a routing change. The node answers `Declined` ([ADR 005 § Open-time refusal classes](005-protocol.md#open-time-refusal-classes)), so the client removes it for this hash and fails over to another candidate. [ADR 037 § Warming is a relay-edge mechanism](037-regional-proxy-warming.md#warming-is-a-relay-edge-mechanism) covers how this composes with proxy warming. A relay node fills a miss it relays with a paid pull from a peer. The node-local policy `cache.node_to_node_pull_through_enabled` controls that pull, and its default is role-derived in the same way: on for a node with no origin configured, off for a node that configures an origin backend. An operator sets the field explicitly to override either default.

### Namespace 0

`namespaceId == 0` is not a namespace. It has no publisher and no authorized origins; `PublisherRegistry` allocates namespace ids from 1. On a request, `0` means that the requester does not name the namespace (above). Namespace 0 holds no seats, so it attests nothing: an attestation that names namespace 0 is invalid. A request that names namespace 0 is still served by a node that holds a live attestation for the hash in any namespace. A hash that no seated origin attests has no valid DHT record, so no node discovers it, relays it or serves it. A publisher creates a namespace and seats an origin to put content into the network.

### Contract: PublisherRegistry

```solidity
interface IPublisherRegistry {
    // Namespace creation — permissionless. The first successful call from a
    // given address implicitly registers it as a publisher (no separate
    // registration step). Every call — including the first — reverts if it
    // would push namespaceCount(caller) above maxNamespacesPerPublisher.
    function createNamespace() external returns (uint256 namespaceId);

    // Namespace ownership transfer in two steps. transferNamespace (owner
    // only) offers the namespace; a new offer replaces the open one, and
    // newOwner == address(0) withdraws it. acceptNamespace (pending owner
    // only) moves ownership at once.
    function transferNamespace(uint256 namespaceId, address newOwner) external;
    function acceptNamespace(uint256 namespaceId) external;

    // Views
    function ownerOf(uint256 namespaceId) external view returns (address);
    function namespaceCount(address publisher) external view returns (uint256);
    function pendingOwnerOf(uint256 namespaceId) external view returns (address);
    function maxNamespacesPerPublisher() external view returns (uint64);

    // Governable parameter setter (GOVERNANCE_ROLE; standard 48h timelock).
    // Bounds enforced at the contract layer per ADR 009:
    //   - maxNamespacesPerPublisher: [1, 1000]
    function setMaxNamespacesPerPublisher(uint256 newMax) external;

    // Events
    event NamespaceCreated(uint256 indexed namespaceId, address indexed owner);
    event NamespaceTransferStarted(uint256 indexed namespaceId, address indexed from, address indexed to);
    event NamespaceTransferred(uint256 indexed namespaceId, address indexed from, address indexed to);
    event MaxNamespacesPerPublisherUpdated(uint256 oldValue, uint256 newValue);
}
```

`PublisherRegistry` records namespace ownership and lifecycle. The hash→namespace association is supplied by the requester at fetch time (see [§ Hash-to-namespace association](#hash-to-namespace-association)). Origin authorization for a namespace lives in `OriginAssignment` ([ADR 011](011-content-takedown.md#origin-assignment-authority)).

The per-publisher namespace cap is a governable parameter with safety bounds (see [ADR 009](009-governance.md#adr-009-governance-model)). The default is `maxNamespacesPerPublisher = 100` (anti-squatting; bounded `[1, 1000]`). It is stored on `PublisherRegistry` itself and updated via `setMaxNamespacesPerPublisher` under the standard `TimelockController` delay (`172800` seconds / 48 h); the contract enforces the safety bounds at the setter and rejects out-of-range writes regardless of caller. The `maxNamespacesPerPublisher` cap is enforced at both `createNamespace` and `acceptNamespace` (on the recipient; self-transfers exempt), so it cannot be bypassed by minting namespaces under throwaway addresses and transferring them to a single publisher.

### Namespace transfer

The owner offers a namespace with `transferNamespace`. The recipient takes it with `acceptNamespace`. Ownership moves when the recipient accepts. No delay applies.

The acceptance step protects the owner from a mistyped address. An offer to an address that nobody controls never completes, and the owner can replace or withdraw it.

The owner key is the only guard on a namespace. A holder of a leaked key can already seat origins and sign gateway pointers for the namespace, so a delay on the transfer protects nothing. A delay also blocks recovery: each holder of the key can replace or withdraw the offer of the other. Without a delay, the first holder to complete a transfer to a key that only they control keeps the namespace. The other holder then controls nothing.

## Consequences

### Positive

- Delivery verification is inherent: hash mismatch on receipt is both detection and proof. No separate proof-of-delivery oracle is needed.
- Content is location-independent: a blob from any node is interchangeable as long as the hash matches. This makes the entire delivery layer transparent to clients.
- Deduplication is automatic: two nodes holding identical bytes share one logical blob identity.
- The origin-backed node's backing storage is completely opaque to the network — nobody can discover the origin URL or bypass the payment layer.
- The chain never stores content hashes: on-chain state is bounded by the number of namespaces and their origin sets, not by the volume of content served.

### Negative

- Content is immutable: updating a blob produces a new hash and a new identity. Applications that need mutable references (e.g. "latest version of X") must manage their own indirection layer above the protocol.
- Retrieval of cold content requires knowing the namespace. A request that does not name it (`namespaceId == 0`) reaches only cached copies and the origins that their attestations name; applications are expected to carry the namespace for content they publish.
- Content that no seated origin attests cannot spread through the network, and no node serves it. A publisher must be vetted and seat an origin before its content is served.
- Content stays available only while a seated origin of its namespace signs attestations. When every seated origin of a namespace is offline for longer than the attestation TTL (at most 24 h), its content leaves the network. A publisher that wants resilience seats more than one origin.
- If a namespace's authorized origins all lose their backing data (disk failure, misconfiguration), the content is gone from the network unless another node caches it. Redundancy is the publisher's responsibility, met by having the DAO authorize multiple origins for the namespace.
- The protocol imposes no inherent size limit on content addressing — any byte sequence produces a valid BLAKE3 hash regardless of length. Individual nodes may enforce a configurable `max_blob_size` for operational reasons (cache management, connection resource limits). This is a node-level resource policy, not a content-addressing constraint — see [ADR 005](005-protocol.md#error-handling-and-retry-semantics) for the `max_blob_size` enforcement mechanism.
