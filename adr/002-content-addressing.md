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

A **publisher** is an Ethereum address that has registered through the `PublisherRegistry` contract ([ADR 016](016-contract-interactions.md)). Registration is permissionless and one-shot per address; transferring publisher ownership is a separate gated action (see ADR 016 for the lifecycle).

A **namespace** is a publisher-owned `uint256` identifier under which blob hashes are claimed. Publishers may register multiple namespaces (subject to the anti-squatting cap in [ADR 009](009-governance.md)) so that distinct content sets can be governed independently — for example, a media company may operate one namespace per product line so that takedowns and origin assignments for one product do not entangle the others.

A **content claim** is an on-chain binding of a `(namespaceId, blake3Hash)` pair, recorded by the namespace's owner via `PublisherRegistry.claimContent`. Claims are append-only and content-immutable: once a hash is claimed under a namespace it cannot be moved, but the underlying bytes are untouched (BLAKE3 makes the hash binding cryptographic). Claims are the authoritative on-chain answer to "is this blob in a registered namespace, and if so which one?".

### Default-open namespace

A reserved namespace ID (`namespaceId == 0`) is the **default-open namespace**. Any blob is implicitly claimed under it; no `claimContent` call is required. Operators authorized by the DAO via the default-open allow-list (see [ADR 011 § Default-open allow-list](011-content-takedown.md#default-open-allow-list)) may serve as origin for default-open blobs; cache-only serving by any staked operator remains permissionless. The allow-list is the single global authority over default-open origin behaviour; until governance activates a non-empty list at least once, the bootstrap rule preserves the prior permissive semantics so PoC and pre-activation deployments are not stranded. Publishers seeking per-content durability guarantees, takedown accountability, or namespace-scoped origin sets opt in by registering and claiming under a non-zero namespace.

A blob may be claimed under at most one non-zero namespace. If a publisher wants to assert ownership of a blob currently in default-open, the claim succeeds and the blob is thereafter governed under the registered namespace; the default-open implicit claim is overridden. Two publishers attempting to claim the same hash under different namespaces is resolved first-write-wins at the contract layer (see [ADR 016](016-contract-interactions.md) for the dispute resolution rules and the consequences if a second claim is rejected).

### Why this lives in ADR 002

Content identity (the BLAKE3 hash) and publisher identity are paired: every claim is a binding between the two. Defining publisher and namespace here keeps the identity primitives in one place so that ADRs 011 (governance authority), 016 (contract surface), 005 (probe-time enforcement), and 022 (DHT publication semantics) can refer back to a single canonical definition.

### Contract: PublisherRegistry

```solidity
interface IPublisherRegistry {
    // Publisher registration — permissionless, one-shot per address
    function registerPublisher() external returns (uint256 publisherId);

    // Namespace lifecycle — only callable by the namespace owner (or DEFAULT_ADMIN_ROLE)
    function createNamespace() external returns (uint256 namespaceId);

    // Namespace ownership transfer with a 7-day timelock.
    // initiateTransfer queues the transfer; finalizeTransfer completes it after
    // the timelock; cancelTransfer (callable by current owner only) aborts.
    function initiateNamespaceTransfer(uint256 namespaceId, address newOwner) external;
    function finalizeNamespaceTransfer(uint256 namespaceId) external;
    function cancelNamespaceTransfer(uint256 namespaceId) external;

    // Content claim — only callable by the namespace's current owner.
    // First-write-wins across non-zero namespaces; reverts if the hash is already
    // claimed under a different non-zero namespace. Claiming a hash currently
    // implicit in default-open (namespaceId == 0) succeeds and overrides.
    function claimContent(uint256 namespaceId, bytes32 blake3Hash) external;

    // Views
    function namespaceOf(bytes32 blake3Hash) external view returns (uint256 namespaceId);
    // Historical view used by SlashJudge for phantom-origin evidence: returns
    // the namespace this hash was bound to at `timestamp`. Returns 0 if the hash
    // was unclaimed (default-open) at that time.
    function namespaceOfAt(bytes32 blake3Hash, uint64 timestamp) external view returns (uint256 namespaceId);
    function ownerOf(uint256 namespaceId) external view returns (address);
    function namespaceCount(address publisher) external view returns (uint256);
    function pendingTransfer(uint256 namespaceId) external view returns (address newOwner, uint256 readyAt);

    // Events
    event PublisherRegistered(address indexed publisher, uint256 indexed publisherId);
    event NamespaceCreated(uint256 indexed namespaceId, address indexed owner);
    event NamespaceTransferInitiated(uint256 indexed namespaceId, address indexed from, address indexed to, uint256 readyAt);
    event NamespaceTransferred(uint256 indexed namespaceId, address indexed from, address indexed to);
    event ContentClaimed(uint256 indexed namespaceId, bytes32 indexed blake3Hash, address indexed claimant);
}
```

`namespaceOf(hash)` returns `0` for any hash not explicitly claimed; that is the default-open namespace. The view never reverts on unknown hashes — callers cannot distinguish "hash unknown to the protocol" from "hash served as default-open" via this view, which is correct: both states are operationally identical.

`namespaceOfAt(hash, timestamp)` is the historical counterpart used by `SlashJudge` to evaluate phantom-origin evidence at the timestamp embedded in a signed `ProbeResponse` (see [ADR 005 § cdn/probe/v1](005-protocol.md#cdnprobev1--latency-probe)). It returns `0` for any timestamp before the hash was claimed, otherwise the namespace it was bound to at that time. Because content claims are append-only — a hash may move from default-open (`0`) to a non-zero namespace exactly once and never moves again — the historical lookup needs to store only a single `(namespaceId, claimedAt)` entry per claimed hash, and the view is `t < claimedAt[hash] ? 0 : namespaceId[hash]`.

Per-publisher namespace cap and ownership-transfer timelock are governable parameters with safety bounds (see [ADR 009](009-governance.md)). The 7-day default transfer timelock is documented for clarity; the contract reads its current value from the governance-controlled parameter store at call time.

> **PoC simplification.** During the PoC, `PublisherRegistry` is admin-key controlled (the deployer EOA can override any publisher action) consistent with the broader admin-key governance model in [ADR 009](009-governance.md). The interface above describes the production semantics; the PoC adds an `onlyOwner` escape hatch that is removed when admin authority transfers to the timelock.

## Consequences

**Positive:**

- Delivery verification is inherent: hash mismatch on receipt is both detection and proof. No separate proof-of-delivery oracle is needed.
- Content is location-independent: a blob from any node is interchangeable as long as the hash matches. This makes the entire delivery layer transparent to clients.
- Deduplication is automatic: two nodes holding identical bytes share one logical blob identity.
- The hash serves as the slash evidence primitive: a client detecting a BLAKE3 mismatch on received bytes can trigger an on-chain slash via a challenge process. **Resolved:** [ADR 014](014-on-chain-verification.md) specifies a single-round optimistic challenge-response for the PoC (challenger posts a signed `StreamResponse` + 100 TOKEN bond; node has 24 hours to counter) with a production upgrade path using interactive keccak256 Merkle proofs over 1024-byte chunks for cryptographic on-chain verification. See [ADR 014 § Bond Handling](014-on-chain-verification.md#bond-handling) for challenge bond requirements (amount, transfer, forfeit rules) and [ADR 005](005-protocol.md#cdnprobev1--latency-probe) for slashing evidence mechanisms.
- The origin-backed node's backing storage is completely opaque to the network — nobody can discover the origin URL or bypass the payment layer.

**Negative:**

- BLAKE3 is not a native EVM precompile, so on-chain verification requires an intermediate scheme. [ADR 014](014-on-chain-verification.md) specifies an optimistic challenge-response for PoC and a keccak256 Merkle proof protocol for production.
- Content is immutable: updating a blob produces a new hash and a new identity. Applications that need mutable references (e.g., "latest version of X") must manage their own indirection layer above the protocol.
- If an origin-backed node loses its backing data (disk failure, misconfiguration), the content is gone from the network unless another node holds the same blob. Operators are responsible for their own backend durability.
- The protocol imposes no inherent size limit on content addressing — any byte sequence produces a valid BLAKE3 hash regardless of length. Individual nodes may enforce a configurable `max_blob_size` for operational reasons (cache management, connection resource limits). This is a node-level resource policy, not a content-addressing constraint — see [ADR 005](005-protocol.md#error-handling-and-retry-semantics) for the `BlobTooLarge` enforcement mechanism.
