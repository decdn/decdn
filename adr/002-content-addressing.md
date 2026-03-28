# ADR 002: Content Addressing

**Date:** 2026-03-28
**Status:** Draft

## Context

Edge nodes cache and serve blobs. Clients need to verify that the bytes they receive are the bytes they asked for, without trusting the serving node. The network also needs a stable identifier for each piece of content — one that doesn't change when the content moves between origin and edge caches.

## Decision

All blobs are content-addressed by their BLAKE3 hash. The hash is computed once at upload time and stored in a manifest. The manifest hash is the canonical identifier clients use to request content.

Blob identity is intrinsic to the content: the same bytes always produce the same hash, regardless of which node holds them or where the origin lives. Clients verify every received blob against its known hash — an edge node cannot serve corrupted data without immediate detection.

BLAKE3 is iroh's native hash function, so there is no translation layer between blob IDs and the transport layer.

## Consequences

**Positive:**

- Delivery verification is inherent: hash mismatch on receipt is both detection and proof. No separate proof-of-delivery oracle is needed.
- Content is location-independent: a blob pulled from a peer edge and a blob pulled from origin are interchangeable as long as the hash matches. This makes peer-assisted delivery transparent to clients.
- Deduplication is automatic: two uploads of identical bytes share one blob.
- The hash serves as the slash evidence primitive: a client submitting a slash claim provides the expected hash and the received bytes; the mismatch is verifiable on-chain (via a chunk Merkle proof for the PoC, since BLAKE3 is not an EVM precompile).

**Negative:**

- BLAKE3 is not a native EVM precompile, so on-chain verification requires an intermediate scheme (Merkle proof over chunks using keccak256) for the PoC. Full BLAKE3 verification on-chain is deferred to a later version.
- Content is immutable: updating a blob produces a new hash and a new identity. Manifest indirection handles this for metadata updates, but clients must re-fetch the manifest to discover the new hash.
- The content catalog (hash → S3 object key) is a centralized operational dependency — without it, edge nodes cannot resolve a cache miss to an origin URL. This is acceptable for the PoC but is flagged as a future decentralization target.
