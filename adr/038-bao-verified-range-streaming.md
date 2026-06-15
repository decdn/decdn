# ADR 038: Bao Verified-Range Streaming on `cdn/client/v1`

**Date:** 2026-06-15
**Status:** Draft

## Context

`cdn/client/v1` delivery verifies integrity with a **flat, sequential** BLAKE3 hash over the assembled stream: the receiver feeds bytes into a single `blake3::Hasher` in order and compares the digest against the requested content hash, either after `StreamEnd` (buffered) or at stream finalization (progressive). A flat digest commits only to the **whole blob**, and it is inherently sequential — byte `N` cannot be verified without bytes `0..N`. Two capabilities the network needs are blocked by this:

- **Verified resume.** [ADR 005 § Redirect loop prevention](005-protocol.md#redirect-loop-prevention) specifies that `byte_offset` supports seek and resume, and that a failover requester "resumes from the last BLAKE3-verified byte." A flat hasher cannot honor this: a request that starts at `byte_offset > 0` has no earlier bytes to hash, so its range cannot self-verify. A node can serve a corrupt **tail** on a resumed range, and the receiver has no way to reject it on its own — the corruption surfaces only later, if and when the client ever possesses the entire blob.

- **Parallel multi-source fetch.** Disjoint ranges fetched from different nodes cannot be independently checked against the content address, so a blob cannot be safely assembled from multiple sources. One lying source poisons the whole result with no localized, immediate rejection.

BLAKE3 is a Merkle tree, not a flat hash: the content hash a client already requests **is** the root of a binary tree over the blob's chunks. Given the tree's interior nodes (the **outboard**), any chunk or range verifies independently against the root with an `O(log n)` proof — no earlier bytes required. This is the bao verified-streaming format, and it closes both gaps with one mechanism.

This realizes assumptions already present elsewhere in the canon. [ADR 005 § Relationship to iroh-blobs](005-protocol.md#cdnclientv1--paid-delivery-protocol) states the protocol "wraps iroh-blobs' verified streaming" with "BLAKE3 tree-hash verification at the chunk level" such that "receivers need not buffer the full blob before confirming integrity." [ADR 002 § Content Addressing](002-content-addressing.md#adr-002-content-addressing) commits clients to verifying "every received blob against its known hash." [ADR 037 § Node serving](037-regional-proxy-warming.md#node-serving-window-paced-pull-through) assumes "bao verified-streaming make[s] a partially-held blob first-class" and a node "verifies and serves any chunk range against the root hash," while explicitly **deferring** the concrete partial-blob bao range store/serving and treating a resumed cache-miss (`byte_offset > 0`) as a buffered-path fallback that verifies the whole blob. This ADR specifies the wire encoding and verification model those statements presuppose.

The outboard is **content-derived and self-validating**: BLAKE3 is canonical, so every party that holds the bytes derives the identical tree, and any proof is checked against the root the receiver already wanted. There is no trusted computer and nothing transmitted as authoritative. Each node materializes the outboard for free at import — `decdn-cache` imports every blob through `iroh-blobs`' `FsStore`, which builds and persists the outboard as a byproduct — so the serving path reads it rather than recomputing it. The publisher CLI is unaffected: it computes only the flat BLAKE3 root and links no blob store, and because the flat root **is** the bao tree root by construction, the first node to import the bytes derives an outboard that matches the advertised hash.

## Decision

The `cdn/client/v1` delivery payload carries bao's native **interleaved verified-stream encoding** — proof nodes appear inline, immediately before the data they authenticate, in tree order. Receivers feed the stream into a bao verifying decoder that validates each chunk group against the root content hash **as it arrives, at any byte offset**, and rejects on the first mismatch. This is the option that reuses the existing codec rather than transmitting a separate boundary proof and re-implementing the proof/data association by hand.

### Verification model

A blob's content hash is the root of a BLAKE3 Merkle tree over fixed-size chunk groups (`iroh-blobs`' group size, 16 KiB). To deliver a range, the serving node emits the chunk-group data interleaved with the sibling/spine hashes that connect those groups to the root. The receiver:

- Verifies each group against the root **incrementally**, rejecting corruption at the offending group rather than only at stream end (early rejection).
- Verifies a range that begins at any offset, because the proof anchors that range to the root independently of earlier bytes.

Proof overhead is `O(log n)` in the blob size — on the order of a kilobyte of sibling hashes for a multi-gigabyte blob — and is paid only at range boundaries, not per chunk.

### Wire format

The `StreamResponse` / `Voucher` / `VoucherAck` / `StreamEnd` envelope is unchanged. `ChunkData` payloads carry bao-encoded verified-stream bytes (interleaved data and proof nodes) rather than raw content bytes. Payment vouchers remain interleaved exactly as today: the bao byte stream is framed into `voucher_interval_mb`-sized pieces, and the receiver reassembles those pieces before feeding the decoder. The bao codec is the source of truth for verification, so wire framing boundaries are independent of chunk-group boundaries (partial final group and right-edge spine are handled by the codec).

`cdn/client/v1` is pre-finalisation (no testnet deployment), so the payload format changes **in place** — there is no version bump and no backwards-compatibility shim. The flat-hasher payload is replaced, not coexisted with.

### Serve side

A node serving a range exports a verified range from its store: it reads the persisted outboard and emits the bao encoding for the requested chunk-group range. The outboard is already on disk from import; the serve path performs no re-hashing of held content.

The **origin-cold** path is exempt by nature: on the very first pull straight from an origin, the node streams while still building the outboard during import, so it cannot yet emit proofs for not-yet-imported bytes. That path is inherently sequential single-source (one origin, whole object). Verified-range serving applies to every subsequent serve from a node that holds the blob's outboard.

### Receive side

The receiver replaces the flat `blake3::Hasher` with a bao verifying decoder fed the reassembled stream. A range that begins at `byte_offset > 0` verifies on its own against the root, closing the resumed-tail gap: a corrupt tail is rejected at the offending group with no dependency on possessing earlier bytes. The buffered whole-blob fallback for resumed requests is removed.

### Payment metering

Paid bytes are the bytes transferred on the wire — content data **plus** the interleaved proof nodes. The proof is real bandwidth the operator serves, and metering the raw stream keeps payment a single byte count with no special case. Excluding proof bytes would require payer and node to agree on how many proof bytes were interleaved, adding a dispute and reconciliation surface for a `~0.4%` overhead; the byte count over the delivered stream is the no-special-case choice and is the one adopted.

### Scope boundary

This ADR specifies **verification only** — the property that any range is independently checkable against the content address. The **multi-source fetch scheduler** (splitting a blob into ranges across peers, concurrency and reassembly, straggler hedging and re-dispatch on a failed or lying source, and voucher accounting across multiple channels) is a separate, larger subsystem and is deferred to its own ADR. Verified ranges are the safety prerequisite that makes that subsystem possible; they are independently valuable without it, because they close the resumed-tail gap on the existing single-source path.

Range-addressed discovery — advertising "I hold bytes `[a, b)` of `H`" on `cdn/dht/v1` — remains deferred per [ADR 037 § DHT advertising stays whole-blob](037-regional-proxy-warming.md#dht-advertising-stays-whole-blob); discovery stays hash-level. Multi-source fetch will need it eventually, but it is out of scope here.

## Consequences

### Positive

- Closes the resumed-tail verification gap: a request at any `byte_offset` verifies its own range against the content hash, with no trust in the serving node and no dependency on later possessing the whole blob.
- Establishes the safety precondition for parallel multi-source fetch — each range is independently verifiable against the same root — without committing to the scheduler that exploits it.
- Early rejection: corruption is caught at the offending chunk group rather than after the entire transfer, so a corrupt or malicious stream is abandoned sooner.
- No new trusted surface and no new computation on the hot path: the outboard is content-derived and already materialized at import; the serve path reads it.
- The publisher CLI is untouched — it keeps the flat-root, no-blob-store property, and the flat root is the bao root by construction.

### Negative

- The delivery payload format changes; every node and client must produce/consume the bao encoding. This is a clean break rather than a migration only because `cdn/client/v1` is pre-finalisation.
- Payment meters proof bytes as well as content bytes (`~0.4%` overhead), so payers pay a small premium for the integrity proofs.
- The origin-cold first pull cannot offer verified ranges and remains sequential single-source until the blob is imported and its outboard exists.

### Risks

- **Codec coupling.** Verification now depends on the bao encoding produced and consumed via `iroh-blobs`/`bao-tree`. A change in that format across dependency upgrades would be a wire concern; mitigated by pinning the dependency and treating its encoding as part of the protocol contract.
- **Framing/grouping mismatch bugs.** Wire framing (`voucher_interval_mb` pieces) and bao chunk groups are different sizes; an implementation that reassembles incorrectly before decoding would fail verification. Mitigated by making the codec the sole verifier and covering reassembly with tests (resumed range, corrupt tail, whole-blob equivalence).

## Cross-ADR Impact

- [ADR 002 § Content Addressing](002-content-addressing.md#adr-002-content-addressing): the "verify every received blob against its known hash" invariant is realized as per-range Merkle verification against the BLAKE3 root, not only a whole-blob digest. Any range — partial, resumed, or sourced from a distinct node — is independently verifiable. The content-addressing identity and the canonicity of the hash are unchanged.
- [ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol): `ChunkData` payloads carry the bao interleaved verified-stream encoding rather than raw bytes; this is the concrete realization of the "Relationship to iroh-blobs" note (chunk-level tree-hash verification, no full-blob buffering) and of the [§ Redirect loop prevention](005-protocol.md#redirect-loop-prevention) resume-from-last-verified-byte guarantee. The `StreamRequest` / `StreamResponse` / voucher envelope is unchanged.
- [ADR 037 § Node serving](037-regional-proxy-warming.md#node-serving-window-paced-pull-through): this ADR supplies the partial-blob bao range store/serving that the proxy-warming design names as first-class but leaves deferred — a resumed cache-miss (`byte_offset > 0`) is served and verified as a range rather than falling back to the buffered whole-blob path. The window-paced pull-through and seed-leech caps are otherwise unchanged.
- [ADR 022 § Content Discovery](022-content-discovery.md#adr-022--content-discovery-at-scale): unchanged. Discovery stays hash-level; range-addressed availability remains deferred. Multi-source fetch (a future ADR) will revisit this.

## Acceptance Criteria

1. A `cdn/client/v1` request that begins at `byte_offset > 0` verifies the received range against the requested content hash on its own, with no dependency on bytes before the offset; a corrupt tail is rejected.
2. Verification is incremental: a corrupt chunk group is rejected at that group rather than only at stream finalization.
3. `ChunkData` payloads carry the bao interleaved verified-stream encoding; the `StreamResponse` / `Voucher` / `VoucherAck` / `StreamEnd` envelope and the voucher cadence are unchanged, with vouchers interleaved by framing the bao byte stream into `voucher_interval_mb` pieces.
4. The serving node emits verified ranges from the persisted outboard without re-hashing held content; the origin-cold first pull is exempt and remains sequential single-source until import completes.
5. The publisher CLI is unchanged — it computes the flat BLAKE3 root, links no blob store, and the root matches the outboard a node derives on first import.
6. Paid bytes equal the bytes delivered on the wire, inclusive of interleaved proof nodes; there is no separate metering path that excludes proof bytes.
7. A whole-blob fetch from `byte_offset == 0` against a single source yields bytes identical to the prior flat-hasher path and verifies against the same content hash.
8. Multi-source fetch scheduling and range-addressed discovery are out of scope; this ADR delivers only the per-range verification property they depend on.
