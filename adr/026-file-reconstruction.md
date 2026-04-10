# ADR 026 — File-to-Blob Mapping, Manifests, and Reconstruction

**Status:** Accepted
**Date:** 2026-04-10
**Deciders:** Core team

---

## Context

The deCDN network serves content-addressed blobs. A single logical file — an AI model checkpoint,
a video file, a software archive — may be large enough that storing it as a single blob is
impractical: iroh-blobs operates in memory during verification, and a single 100 GB blob would
require proportional memory. More importantly, a single-blob model offers no parallelism: the
client cannot download different parts of the file from different nodes simultaneously (ADR 024)
without a protocol-level mechanism for range assignment that is aware of chunk boundaries.

The current protocol (ADR 005, ADR 012) assumes the client already knows the blob hash for
what it wants. For multi-chunk files, the client additionally needs:

1. How many blobs make up the file and in what order.
2. The BLAKE3 hash of each blob (to verify chunks individually).
3. The total file size (for range assignment — ADR 024).
4. Optionally: per-chunk encryption keys if the file is encrypted (ADR 006 applies per-chunk).

This is the role of a **manifest**: a content-addressed blob that describes a file as an ordered
sequence of chunks.

---

## Decision

### Chunk size policy

Files are split into chunks at ingest time by the content origin. The canonical chunk size is
**256 MiB** (268,435,456 bytes). The last chunk is a partial chunk — it contains the remaining
bytes and may be smaller than 256 MiB.

Rationale for 256 MiB:

- Large enough that per-chunk metadata overhead is negligible for files above 1 GB
  (a 10 GB file = 40 chunks; manifest is < 10 KB)
- Small enough for iroh-blobs in-memory verification to be comfortably below 1 GB working set
- Aligns with common CDN origin multipart upload sizes (AWS S3 default: 8 MB–5 GB; 256 MiB is
  a round multiple of S3's 8 MB minimum)
- Larger chunks reduce the benefit of parallelism; smaller chunks increase manifest overhead
  and per-chunk channel interaction

The chunk size is a convention for origin-produced content; the protocol does not enforce a
specific size. Nodes serve any BLAKE3-addressed blob regardless of size.

### Manifest format

A manifest is itself a content-addressed blob. Its BLAKE3 hash is the **canonical identifier for
the file** that clients request and share. The manifest is a compact binary encoding (postcard,
consistent with ADR 013).

```rust
/// Manifest: describes a file as an ordered list of chunks.
struct Manifest {
    /// Manifest format version; currently 1.
    version: u8,

    /// Total file size in bytes (sum of all chunk sizes).
    total_bytes: u64,

    /// MIME type of the reconstructed file, e.g. "application/octet-stream".
    /// Empty string if unknown.
    mime_type: String,

    /// Original filename hint (basename only, no path components).
    /// Empty string if not provided by origin.
    filename: String,

    /// Ordered list of chunks. Concatenating in index order reconstructs the file.
    chunks: Vec<ChunkEntry>,
}

struct ChunkEntry {
    /// BLAKE3 hash of the chunk blob (raw ciphertext if encrypted).
    hash: [u8; 32],

    /// Size of this chunk blob in bytes.
    size: u64,

    /// For encrypted files (ADR 006): wrapping metadata for this chunk's K_blob.
    /// None for unencrypted blobs.
    encryption: Option<ChunkEncryption>,
}

struct ChunkEncryption {
    /// Epoch ID under which K_blob is wrapped.
    /// Clients use this to request the correct epoch key from the app server.
    epoch_id: u32,
    // Note: K_blob itself is NOT in the manifest. It is delivered per-play-request
    // by the app server (cdn/keys/v1) as in ADR 006.
}
```

The manifest blob is pushed to the CDN network as an ordinary blob. It is small (typically <
1 MB even for a 10,000-chunk file) and is fetched first before any chunk download begins.

### Manifest hash distribution

The manifest hash is the file's canonical identifier. It is distributed out-of-band by the
content provider — e.g., embedded in a web page, API response, QR code, or DNS TXT record.
The CDN protocol has no registry of manifest hashes; content discovery uses the DHT (ADR 022)
keyed on the manifest hash.

### Download flow (client)

```
1. Client requests manifest hash H_manifest.
2. Client fetches manifest blob (via cdn/client/v1, StreamRequest{hash: H_manifest}).
   - Verify BLAKE3(manifest_bytes) == H_manifest.
   - Deserialize manifest → total_bytes, chunk list.
3. If --max-channels > 1 (ADR 024):
   - Probe N nodes for each chunk hash (or subset of top chunks for large manifests).
   - Assign byte ranges to nodes based on chunk boundaries where possible.
   - Prefer assigning whole chunks to a single node (avoids mid-chunk node failover).
4. For each chunk in order:
   - Send StreamRequest{hash: chunk.hash, byte_offset: 0}.
   - Receive and write chunk bytes to ~/.decdn/downloads/<H_manifest>/chunk-<index>.part.
   - Verify BLAKE3(chunk_bytes) == chunk.hash after receiving all chunk bytes.
5. After all chunks verified:
   - Concatenate chunk-<i>.part files in order → output file.
   - Verify optional full-file BLAKE3 if provided in manifest (future extension).
   - Delete chunk part files.
```

For encrypted files, the client additionally fetches `K_blob` for each chunk from the app server
(ADR 006) and decrypts before writing to the output file.

### On-disk layout during download

```
~/.decdn/downloads/<H_manifest>/
  manifest.bin          # raw manifest bytes (persisted after fetch)
  state.json            # from ADR 025: per-chunk channel state
  chunk-0000.part       # bytes for chunk index 0
  chunk-0001.part       # bytes for chunk index 1
  ...
  chunk-NNNN.part       # last (partial) chunk
```

The state.json from ADR 025 is extended to include manifest metadata:

```jsonc
{
  "version": 1,
  "hash": "<H_manifest>",    // manifest hash (file identifier)
  "mode": "manifest",        // new mode value (vs. "single" for raw blobs)
  "manifest_verified": true, // true once manifest.bin is BLAKE3-verified
  "total_bytes": 268435456000,
  "ranges": [
    {
      // Each range corresponds to one or more whole chunks
      "chunk_index": 0,
      "start_byte": 0,
      "end_byte": 268435456,
      "verified_offset": 134217728,
      "channel_id": "0x...",
      "voucher_nonce": 17,
      "node_id": "..."
    }
  ]
}
```

### Blob retention after reconstruction

After the output file is written, the client **retains the chunk part files** under
`~/.decdn/downloads/<H_manifest>/` unless `--no-keep-blobs` is passed. This serves two purposes:

1. **Re-serving:** An iroh-blobs node embedded in the client can serve the retained chunks to
   other peers, turning every downloading client into a network contributor (optional, opt-in).
2. **Partial re-download:** If the output file is deleted but blobs are retained, a re-download
   needs only to re-fetch missing chunks rather than the full file.

The default is to keep blobs; they can be cleaned up with `decdn downloads clean`.

### Unencrypted vs. encrypted manifests

For public/unencrypted content, the manifest contains raw chunk hashes. Any node with the chunk
blobs can serve them to any client.

For encrypted content (ADR 006), each chunk is individually encrypted with its own `K_blob` and
nonce (following the same per-blob XChaCha20-Poly1305 scheme). The manifest records the
`epoch_id` for each chunk (not `K_blob` itself). The client fetches `K_blob` per-chunk from the
app server via the `cdn/keys/v1` play-request mechanism. This preserves ADR 006's property that
CDN nodes never see plaintext.

### Backward compatibility: raw (single-blob) downloads

The manifest layer is additive. The existing `StreamRequest{hash: blob_hash}` for a raw blob
remains unchanged — a blob that is not a manifest (e.g., a small file stored as a single blob)
is served exactly as before. The client distinguishes manifest blobs from raw blobs by attempting
to deserialize the fetched bytes as a `Manifest`; if deserialization fails, it treats the bytes
as a raw blob. Alternatively, the content provider signals manifest vs. raw out-of-band.

---

## Cargo Feature: `poc`

In the PoC:

- Only single-chunk manifests are supported (i.e., the manifest wraps a single blob). This
  validates the manifest format end-to-end without requiring multi-chunk orchestration logic.
- Blob retention (`--no-keep-blobs`) is not implemented; blobs are always deleted after
  reconstruction in the PoC.
- Full-file BLAKE3 verification (future extension) is not implemented.

---

## Consequences

### Positive

- Files of arbitrary size can be distributed as chunks served by many nodes simultaneously
- The manifest hash is a stable, content-addressed, shareable file identifier
- Chunk-level BLAKE3 verification catches corruption early — no need to re-download the full file
  on a single corrupt chunk
- Blob retention turns clients into voluntary network participants, increasing availability
- Clean separation: nodes serve blobs; the manifest layer is purely a client concern

### Negative

- Clients must now handle manifest fetch before chunk download — two-phase start-up latency
- On-disk layout is more complex: manifest file + per-chunk part files vs. a single `blob.partial`
- Encryption per-chunk (ADR 006) requires N key requests to the app server for an N-chunk file
  (partially mitigated by the offline lease mechanism in ADR 006 which provides keys for up to
  500 tracks)
- Chunk boundaries do not align with QUIC stream boundaries — a node may have some chunks of a
  file but not others; cache-miss rates depend on chunk popularity

### Neutral

- The manifest format version field allows future evolution without a new ALPN
- Single-blob files that predate the manifest layer continue to work unchanged

---

## Alternatives Considered

### Variable-size chunking (content-defined chunking, CDC)

Content-defined chunking (e.g., FastCDC) splits files at content-dependent boundaries to
maximise deduplication across similar files. Rejected for PoC: CDC adds significant ingest
complexity and the deduplication benefit is relevant mainly for versioned datasets (e.g.,
successive model checkpoints that share layers). Noted as a future optimisation for the
production node's ingest pipeline.

### Fixed chunk size other than 256 MiB

- **64 MiB:** Too small for AI model files (a 70B parameter model = ~131 GB = ~2,048 chunks;
  manifest is 2 MB). Parallel benefit is limited because each chunk is below the economic
  threshold for its own channel (ADR 024).
- **1 GiB:** Large enough to require iroh-blobs to hold 1 GB in memory during verification.
  On devices with 8 GB RAM, this is problematic if multiple downloads run simultaneously.
- **256 MiB** balances manifest size, memory pressure, and economic viability per chunk.

### Embedding K_blob in the manifest

For encrypted files, embed the wrapped `K_blob` for each chunk directly in the manifest blob.
Rejected: the manifest is served by CDN nodes who must not see `K_blob`. Including wrapped keys
in the manifest would require the manifest to be encrypted too (adding a circular dependency on
key delivery). The current design keeps the app server as the sole gatekeeper for `K_blob`.

### Merkle tree / tree hash (iroh collection format)

iroh-blobs supports a "collection" format — a Merkle DAG of blobs with a root hash. This is
more flexible than a flat ordered list but adds significant complexity to verification and
partial download logic. The flat manifest format is sufficient for PoC and initial production;
a Merkle structure is noted as a future upgrade path if partial verification of arbitrary
sub-ranges becomes necessary.

---

## Cross-ADR Consistency

| ADR | Relationship |
|-----|-------------|
| ADR 005 | `StreamRequest{hash}` fetches individual blobs (manifest or chunk); no protocol change needed |
| ADR 006 | Encryption is per-chunk; `epoch_id` in `ChunkEncryption` maps to ADR 006 epoch key scheme |
| ADR 012 | Client download flow extended: fetch manifest first, then chunks |
| ADR 022 | DHT is keyed on the manifest hash; nodes advertise chunk hashes individually |
| ADR 024 | Chunk boundaries are the natural unit for range assignment across parallel nodes |
| ADR 025 | On-disk layout extended: `manifest.bin` + `chunk-NNNN.part` files; state.json gains manifest fields |
