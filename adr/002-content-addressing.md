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

## Consequences

**Positive:**

- Delivery verification is inherent: hash mismatch on receipt is both detection and proof. No separate proof-of-delivery oracle is needed.
- Content is location-independent: a blob from any node is interchangeable as long as the hash matches. This makes the entire delivery layer transparent to clients.
- Deduplication is automatic: two nodes holding identical bytes share one logical blob identity.
- The hash serves as the slash evidence primitive: a client submitting a slash claim provides the expected hash and the received bytes; the mismatch is verifiable on-chain (via a chunk Merkle proof for the PoC, since BLAKE3 is not an EVM precompile). See [ADR 004](004-tokenomics.md#challenge-bond) for challenge bond requirements and [ADR 005](005-protocol.md#cdnprobev1--latency-probe) for slashing evidence mechanisms.
- The origin-backed node's backing storage is completely opaque to the network — nobody can discover the origin URL or bypass the payment layer.

**Negative:**

- BLAKE3 is not a native EVM precompile, so on-chain verification requires an intermediate scheme (Merkle proof over chunks using keccak256) for the PoC. Full BLAKE3 verification on-chain is deferred to a later version.
- Content is immutable: updating a blob produces a new hash and a new identity. Applications that need mutable references (e.g., "latest version of X") must manage their own indirection layer above the protocol.
- If an origin-backed node loses its backing data (disk failure, misconfiguration), the content is gone from the network unless another node holds the same blob. Operators are responsible for their own backend durability.
- The protocol imposes no inherent size limit on content addressing — any byte sequence produces a valid BLAKE3 hash regardless of length. Individual nodes may enforce a configurable `max_blob_size` for operational reasons (cache management, connection resource limits). This is a node-level resource policy, not a content-addressing constraint — see [ADR 005](005-protocol.md#error-handling-and-retry-semantics) for the `BlobTooLarge` enforcement mechanism.
