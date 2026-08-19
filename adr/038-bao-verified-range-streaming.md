# ADR 038: Bao Verified-Range Streaming on `cdn/client/v1`

**Date:** 2026-06-15
**Status:** Draft

## Context

`cdn/client/v1` currently verifies integrity with a **flat, sequential** BLAKE3 hash: the receiver feeds bytes into one `blake3::Hasher` in order and compares the digest to the requested content hash, either after `StreamEnd` (buffered) or at finalization (progressive). A flat digest commits only to the whole blob and is inherently sequential — byte `N` cannot be verified without bytes `0..N`. This blocks two needed capabilities:

- **Verified resume.** [ADR 005 § Redirect loop prevention](005-protocol.md#redirect-loop-prevention) has a failover requester "resume from the last BLAKE3-verified byte." A flat hasher cannot honor this: a request starting at `byte_offset > 0` has no earlier bytes to hash, so a corrupt tail on a resumed range cannot be rejected until the client eventually holds the entire blob.
- **Parallel multi-source fetch.** Disjoint ranges from different nodes cannot be checked against the content address independently, so one lying source poisons the assembled result with no localized rejection.

BLAKE3 is a Merkle tree, not a flat hash: the requested content hash **is** the root of a binary tree over the blob's chunks. Given the interior nodes (the **outboard**), any range verifies against the root with an `O(log n)` proof, no earlier bytes required. This is the bao verified-streaming format, and it closes both gaps.

The wire model here is already presupposed across the other ADRs: [ADR 005 § Relationship to iroh-blobs](005-protocol.md#cdnclientv1--paid-delivery-protocol) ("wraps iroh-blobs' verified streaming", chunk-level tree-hash verification, no full-blob buffering), [ADR 002](002-content-addressing.md#adr-002-content-addressing) ("verify every received blob against its known hash"), and [ADR 037 § Node serving](037-regional-proxy-warming.md#node-serving-window-paced-pull-through) (a node "verifies and serves any chunk range against the root hash", with the concrete partial-blob range store deferred). This ADR specifies the encoding and verification model they assume.

The outboard is content-derived and self-validating: BLAKE3 is canonical, so every holder derives the identical tree and every proof is checked against the root the receiver already wanted — no trusted computer, nothing transmitted as authoritative. `decdn-cache` imports blobs through `iroh-blobs`' `FsStore`, which builds and persists the outboard as a byproduct, so the serve path reads it rather than recomputing. An origin backend can also publish the same tree next to its data object, at the sibling key `{H}.obao4` ([ADR 037 § Origin-tier pull-through](037-regional-proxy-warming.md#origin-tier-pull-through-ranged-fetch--external-outboard)). That gives a node proof nodes for a blob it does not hold. The publisher CLI is unaffected: it computes only the flat root and links no blob store, and because the flat root **is** the bao tree root by construction, the first node to import the bytes derives a matching outboard.

## Decision

The `cdn/client/v1` payload carries bao's native **interleaved verified-stream encoding**: proof nodes appear inline, immediately before the data they authenticate, in tree order. The receiver feeds the stream into a bao verifying decoder that validates each chunk group against the root content hash **as it arrives, at any byte offset**, and rejects on first mismatch. This reuses the existing codec rather than transmitting a separate boundary proof.

### Verification model

The content hash roots a BLAKE3 Merkle tree over fixed-size chunk groups (`iroh-blobs`' group size, 16 KiB). To deliver a range, the serving node interleaves chunk-group data with the sibling/spine hashes connecting those groups to the root. The receiver:

- Verifies each group against the root **incrementally**, rejecting corruption at the offending group rather than at stream end.
- Verifies at chunk-group granularity, since the proof anchors the range to the root independently of earlier bytes. A resumed fetch restarts at the next unverified 16 KiB group boundary (group-aligned, not mid-group), avoiding any off-by-one between the verified prefix and the resume request.

Proof overhead is `O(log n)` — on the order of a kilobyte of sibling hashes for a multi-gigabyte blob — paid only at range boundaries, not per chunk.

### Wire format

The `StreamResponse` / `Voucher` / `ChunkPreimage` / `StreamEnd` envelope is unchanged. `ChunkData` payloads carry bao verified-stream bytes (interleaved data and proof nodes) instead of raw content. Payment messages remain interleaved as today: the bao stream is framed into fixed 1 MiB pieces (`CHUNK_BYTES`) and the receiver reassembles them before feeding the decoder. The codec is the sole verifier, so wire framing boundaries are independent of chunk-group boundaries (the codec handles the partial final group and right-edge spine).

`cdn/client/v1` is pre-finalisation (no testnet deployment), so the payload format changes **in place** — no version bump, no compatibility shim. The flat-hasher payload is replaced; the two formats do not coexist.

### Serve side

A serving node emits the bao encoding for the requested chunk-group range from one of two proof sources. The client-facing payload is **always** bao-encoded — there is no raw-byte path on the wire, so the decoder never needs a flat-hasher fallback. Both sources produce the same wire bytes, so the client cannot tell them apart.

- **Persisted outboard.** For a blob the node holds, the node reads the outboard its store materialized at import and exports the requested range, with no re-hashing of held content.
- **Streamed local-outboard pull.** A node that does not hold the blob serves a whole-blob request directly from an origin backend (S3/R2/B2/HTTP/filesystem) that publishes `{H}.obao4`. It fetches that outboard and drives a streaming bao encoder over the origin's plaintext bytes as they arrive. It forwards the encoded wire to the paying client and tees the same bytes into its store at the same time, so the serve pipelines origin→client and starts before the blob lands locally. The fetched outboard is **untrusted**: the encoder verifies every chunk group against the requested root `H` while it streams, a mismatch aborts the stream mid-flight, and the partial bytes already teed are never committed as a local copy. [ADR 037 § Origin-tier whole-blob miss](037-regional-proxy-warming.md#origin-tier-whole-blob-miss-stream-while-store) specifies the window pacing and voucher gating this path shares with the node-to-node one.

The **origin acquisition hop is internal, not `cdn/client/v1`**, so which source a node uses stays invisible to the client. An origin that publishes no `{H}.obao4` hands the node raw bytes and no proof: the node imports and verifies against the requested content hash, materializes the outboard, and then serves bao ranges. That serve pays a one-time import latency, borne once per origin-backed node per blob. Any request against an origin-cold blob that is not a whole-blob request (`byte_offset > 0` or `byte_len > 0`, so an unbounded tail counts) takes a third route: the node fetches the requested `[a, b)` plus `{H}.obao4`, verifies the range against `H`, writes it as a partial blob, and exports it from the store ([ADR 037 § Origin-tier pull-through](037-regional-proxy-warming.md#origin-tier-pull-through-ranged-fetch--external-outboard)). A **node-to-node** cache-miss pull is unaffected: the upstream holder serves bao, which the serving node verifies and forwards as it tees to its own store ([ADR 037 § Node serving](037-regional-proxy-warming.md#node-serving-window-paced-pull-through)), staying pipelined and bao end-to-end.

### Receive side

The receiver replaces the flat `blake3::Hasher` with a bao verifying decoder fed the reassembled stream. A range beginning at `byte_offset > 0` verifies on its own against the root, closing the resumed-tail gap with no dependency on earlier bytes. The buffered whole-blob fallback for resumed requests is removed.

### Payment metering

Paid bytes are the bytes on the wire — content data **plus** interleaved proof nodes. The proof is real bandwidth the operator serves, and metering the raw stream keeps payment a single byte count. Excluding proof bytes would force payer and node to agree on how many proof bytes were interleaved — a dispute surface for a `~0.4%` overhead (one 64-byte node per 16 KiB group ≈ 1/256 of content). The delivered byte count is adopted.

### Scope boundary

This ADR specifies **verification only** — that any range is independently checkable against the content address. The **multi-source fetch scheduler** (range splitting across peers, concurrency and reassembly, deadline-based re-dispatch, voucher accounting across sources) is a separate, larger subsystem in [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1). Verified ranges are its safety prerequisite but are independently valuable, since they also close the resumed-tail gap on the single-source path.

Range-addressed discovery — advertising "I hold bytes `[a, b)` of `H`" on `cdn/dht/v1` — stays deferred per [ADR 037 § DHT advertising stays whole-blob](037-regional-proxy-warming.md#dht-advertising-stays-whole-blob); discovery stays hash-level.

## Consequences

### Positive

- Closes the resumed-tail gap: a request at any `byte_offset` verifies its own range against the content hash, with no trust in the serving node and no dependency on later holding the whole blob.
- Establishes the safety precondition for parallel multi-source fetch (each range independently verifiable against the same root) without committing to the scheduler.
- Early rejection: corruption is caught at the offending chunk group, so a bad stream is abandoned sooner.
- No new trusted surface: the outboard is content-derived and self-validating, whether the node reads it from its own store or fetches it from an origin. A held blob also costs no hot-path computation — its outboard is already materialized at import.
- The publisher CLI is untouched — flat-root, no-blob-store, and the flat root is the bao root by construction.

### Negative

- The payload format changes; every node and client must produce/consume the bao encoding. This is a clean break only because `cdn/client/v1` is pre-finalisation.
- Payment meters proof bytes as well as content (`~0.4%` overhead).
- An origin-cold serve from an origin that publishes no `{H}.obao4` must import the blob and build its outboard before it serves verified ranges, so that hop does not pipeline origin→client and pays a one-time import latency. The client-facing wire is bao either way; only this fallback hop is non-pipelined.
- The pipelined origin-cold path costs two extra origin round-trips per cold serve: a body-free size probe (HTTP `HEAD` / `HeadObject` / `stat`, needed because the outboard alone does not pin the final chunk group's length) and the `{H}.obao4` object itself, `~0.4%` of blob size. It also hashes the origin's bytes on the serve path, because those bytes arrive unverified.

### Risks

- **Codec coupling.** Verification depends on the bao encoding via `iroh-blobs`/`bao-tree`; a format change across dependency upgrades is a wire concern. Mitigated by pinning the dependency and treating its encoding as part of the protocol contract.
- **Framing/grouping mismatch bugs.** Wire framing (fixed 1 MiB payment chunks) and bao chunk groups differ in size; incorrect reassembly before decoding fails verification. Mitigated by making the codec the sole verifier and testing reassembly (resumed range, corrupt tail, whole-blob equivalence).

## Cross-ADR Impact

- [ADR 002 § Content Addressing](002-content-addressing.md#adr-002-content-addressing): the "verify every received blob against its known hash" invariant becomes per-range Merkle verification against the BLAKE3 root, not only a whole-blob digest. Content-addressing identity and hash canonicity are unchanged.
- [ADR 005 § `cdn/client/v1`](005-protocol.md#cdnclientv1--paid-delivery-protocol): `ChunkData` carries the bao interleaved encoding rather than raw bytes — the concrete realization of the "Relationship to iroh-blobs" note and the [§ Redirect loop prevention](005-protocol.md#redirect-loop-prevention) resume-from-last-verified-byte guarantee. The envelope is unchanged.
- [ADR 037 § Node serving](037-regional-proxy-warming.md#node-serving-window-paced-pull-through): supplies the partial-blob bao range store/serving that proxy-warming names first-class but defers — a resumed cache-miss (`byte_offset > 0`) is served and verified as a range instead of the buffered whole-blob path. Window-paced pull-through and its ramped credit window are unchanged.
- [ADR 022 § Content Discovery](022-content-discovery.md#adr-022--content-discovery-at-scale): unchanged. Discovery stays hash-level; range-addressed availability stays deferred, and the [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1) scheduler operates over full holders only.

## Acceptance Criteria

1. A request beginning at `byte_offset > 0` verifies the received range against the requested content hash on its own, with no dependency on bytes before the offset; a corrupt tail is rejected.
2. Verification is incremental: a corrupt chunk group is rejected at that group, not only at finalization.
3. `ChunkData` carries the bao interleaved verified-stream encoding; the `StreamResponse` / `Voucher` / `ChunkPreimage` / `StreamEnd` envelope and the fixed payment quantum are unchanged, with payment messages interleaved by framing the bao stream into fixed 1 MiB pieces.
4. The serving node emits verified ranges from two proof sources, and both produce identical wire bytes. For a blob it holds, it reads the persisted outboard and re-hashes nothing. For a whole-blob serve of an unheld blob whose origin publishes `{H}.obao4`, it streams the origin bytes to the client and into its store at the same time, verifies each chunk group against the root as it streams, and commits no local copy when a group fails. The payload is always bao-encoded, with no raw-byte fallback. An origin that publishes no outboard makes the node import and build the outboard first, at a one-time import latency; a node-to-node pull stays pipelined.
5. The publisher CLI is unchanged — it computes the flat BLAKE3 root, links no blob store, and the root matches the outboard a node derives on first import.
6. Paid bytes equal the bytes delivered on the wire, inclusive of proof nodes; no separate metering path excludes proof bytes.
7. A whole-blob fetch from `byte_offset == 0` against a single source yields bytes identical to the prior flat-hasher path and verifies against the same content hash.
8. Multi-source fetch scheduling and range-addressed discovery are out of scope; this ADR delivers only the per-range verification property they depend on.
