# ADR 002: Content Addressing

**Date:** 2026-03-28
**Status:** Draft

## Context

Vault nodes hold canonical content and seed it into the network. Edge nodes cache and serve it. Clients need to verify that the bytes they receive are the bytes they asked for, without trusting any node. The network also needs a stable identifier for each blob — one that doesn't change as content propagates from vault nodes to edge caches.

## Decision

All blobs are content-addressed by their BLAKE3 hash. The hash is computed once when content is uploaded to a vault node and stored in a manifest. The manifest hash is the canonical identifier clients use to request content.

Blob identity is intrinsic to the content: the same bytes always produce the same hash, regardless of which node holds them. Clients verify every received blob against its known hash — no node can serve corrupted data without immediate detection.

The mapping from hash to the actual backing storage location (e.g., which S3 key, which local file path) is internal to each vault node and never exposed to the network. Edge nodes and clients have no knowledge of a vault node's backend — they only know hashes and NodeIds.

BLAKE3 is iroh's native hash function, so there is no translation layer between blob IDs and the transport layer.

## Consequences

**Positive:**

- Delivery verification is inherent: hash mismatch on receipt is both detection and proof. No separate proof-of-delivery oracle is needed.
- Content is location-independent: a blob from a vault node and a blob from an edge cache are interchangeable as long as the hash matches. This makes the entire delivery layer transparent to clients.
- Deduplication is automatic: two vault nodes holding identical bytes share one logical blob identity.
- The hash serves as the slash evidence primitive: a client submitting a slash claim provides the expected hash and the received bytes; the mismatch is verifiable on-chain (via a chunk Merkle proof for the PoC, since BLAKE3 is not an EVM precompile).
- The vault node's backing storage is completely opaque to the network — nobody can discover the origin URL or bypass the payment layer.

**Negative:**

- BLAKE3 is not a native EVM precompile, so on-chain verification requires an intermediate scheme (Merkle proof over chunks using keccak256) for the PoC. Full BLAKE3 verification on-chain is deferred to a later version.
- Content is immutable: updating a blob produces a new hash and a new identity. Manifest indirection handles metadata updates, but clients must re-fetch the manifest to discover a new hash.
- If a vault node loses its backing data (disk failure, misconfiguration), the content is gone from the network unless another vault node holds the same blob. Vault node operators are responsible for their own backend durability.
