# Appendix: Directory Bundles (`decdn bundle`)

> **This is an appendix, not a core protocol ADR.** Bundles are a
> publisher-side convenience for grouping multiple BLAKE3-content-addressed
> blobs into a single JSON manifest file. Nodes do not require bundle
> support — they deliver individual hashes. Alternative grouping
> mechanisms (tarballs and the like) remain valid for their own use
> cases.

**Date:** 2026-05-07 — **Status:** Accepted (`bundle create` and
`bundle pull` both ship). **Touches:** `decdn` CLI, content
publishing workflow.

## Context

[ADR 002](002-content-addressing.md#adr-002-content-addressing) makes the network single-blob
content-addressed: every byte stream is identified by its BLAKE3 hash,
and the protocol is "content-agnostic … applications may define their
own metadata or manifest layers on top". Multi-file publishing is one
such layer. Hand-authoring `path → hash` JSON is error-prone, so the
`decdn bundle` subcommand ships a deterministic generator alongside the
on-disk format spec it produces — the format is validated by its own
generator and publishers never write a bundle file by hand.

The deterministic-emit requirement (see [Determinism](#determinism)) is
what makes single-hash distribution viable: a bundle file is just bytes,
so it has its own BLAKE3 like any other blob. **Publishers announce a
single bundle hash**, clients fetch the bundle blob first, parse it, then
fetch every entry by hash. No out-of-band file distribution.

## Bundle JSON v1 schema

```json
{"version":1,"entries":[{"path":"a/b.txt","hash":"b3:abc...","size":12},{"path":"a/c.txt","hash":"b3:def...","size":34},{"path":"index.html","hash":"b3:123...","size":4096}]}
```

Top-level keys are `version` (integer, currently `1`) and `entries`
(array). Each entry's keys come in this order: `path` (string), `hash`
(string), `size` (integer), `chunks` (array). `path` and `hash` are
required. `bundle create` always emits `size`, but a reader treats it as
optional and informational (see the `size` bullet). `chunks` is optional
and present only for a chunked file. When a key is present it holds this
order — field declaration order is load-bearing, see
[Determinism](#determinism).

- `path` — relative POSIX path (`/` separator on every platform), UTF-8.
  `..`, absolute paths, root prefixes, and non-UTF-8 components are
  rejected at create time.
- `hash` — `b3:` followed by the 64-character lowercase hex of the
  file's BLAKE3. See [Hash format](#hash-format).
- `size` — file size in bytes, unsigned 64-bit integer. Optional and
  informational on the read side: `bundle pull` uses it for the dry-run
  plan and size hints, and fetches without it.
- `chunks` — optional ordered chunk decomposition of the file. See
  [Chunked files](#chunked-files). When absent, the file is one blob
  addressed by `hash`.

## Chunked files

A `chunks` array represents a file as the in-order concatenation of
independently content-addressed chunk blobs. It is a **dedup helper**: a
chunk shared by two files (two textures that share a region) is one blob,
fetched and paid for once.

```json
{"path":"model.safetensors","hash":"b3:whole...","size":100,"chunks":[{"hash":"b3:c0...","size":60},{"hash":"b3:c1...","size":40}]}
```

- `hash` and `size` always describe the **whole file**. `hash` is the
  file's identity and the end-to-end validator.
- Each element of `chunks` has `hash` (`b3:<hex>`, the chunk blob's
  BLAKE3) then `size` (the chunk's byte length). The chunk sizes sum to
  the entry `size`.
- Chunks are listed in **content order** — the file is their
  concatenation. This order is load-bearing and never sorted.

The chunk blobs are ordinary blobs. The network sees only blobs, never
chunk structure, exactly as it sees only the bundle blob and each entry
blob. A file with unique chunks costs the same as an unchunked file plus
the manifest; the win comes only from shared chunks.

## Hash format

Bundle entries use a `b3:<64-hex>` prefix on every hash. The prefix
makes the hash function explicit and leaves room for a later
`<algorithm>:<hex>` tag if a different hash function ever needs to live
at this layer. **This is a bundle-format convention**, not a
project-wide rename: on-wire fields elsewhere carry bare hex (e.g.
`decdn node evict <hash>`), and that stays unchanged. Bundles are
publisher-side and free to set their own conventions.

## Path-safety rules (creation-time invariants)

`bundle create` enforces every rule below; consumers SHOULD re-validate
on read.

1. **Canonicalize-and-contain.** The `--input` directory is
   canonicalized once. Every walked path is canonicalized and required
   to sit under that root via `starts_with`. Same shape as
   `FilesystemOrigin::fetch` in the cache crate. With
   `--follow-symlinks`, a symlink that resolves outside the root is a
   hard error — the `path` field is a relative POSIX string and cannot
   truthfully describe an external target. Without the flag, symlinks
   are skipped before this check fires (see Rule 5).
2. **No `..`, root, or current-directory components.** Each component
   of the produced relative path must be `Component::Normal`. The
   canonicalize-and-strip-prefix flow shouldn't produce other variants;
   we still assert because the invariant is load-bearing.
3. **No absolute paths.** Falls out of the no-root rule.
4. **UTF-8 only.** Non-UTF-8 path components are a hard error; no
   `to_string_lossy` substitution.
5. **Symlinks.** Off by default: any symlink encountered is skipped
   silently and surfaced via the `skipped_symlinks` counter on the
   `--json` status report. With `--follow-symlinks`: walked
   transparently, and the canonicalize-and-contain check still fires.
6. **Directory state.** Empty input directory is valid and produces
   `entries: []`. Missing or non-directory `--input` errors before
   walking.

## Determinism

Same input directory → byte-identical bundle bytes → byte-identical
bundle BLAKE3. A publisher and an independent verifier producing the
bundle from the same source tree get the same hash. The exact axes:

- **Sort.** `entries` is sorted by `path.as_bytes()` ascending. Bytewise
  on UTF-8 paths equals Unicode codepoint order under UTF-8's prefix-free
  encoding, and matches every other content-addressed sort in the
  workspace.
- **Key order.** Outer `(version, entries)`; per-entry `(path, hash,
  size, chunks)`, with `chunks` omitted entirely when the file is
  unchunked; per-chunk `(hash, size)`. `serde_json` serializes structs
  in declaration order — the Rust struct definitions are the canonical
  key-order spec. An unchunked entry serializes byte-identically to a
  bundle that predates chunked entries.
- **Encoding.** UTF-8, no BOM.
- **Single-line compact.** No whitespace between tokens, no indentation,
  no internal newlines. `jq .` pretty-prints when humans need it; the
  bytes themselves are content-addressed.
- **No trailing newline.** The last byte is the closing `}`. Trailing
  whitespace is the most common source of byte-identical drift across
  editors and shells; omitting it removes the foot-gun.

## Bundle as blob

A bundle file is just bytes — it has a BLAKE3 like any other blob.
Origins serve the bundle bytes as one regular blob; publishers announce
the bundle hash; clients fetch the bundle hash, parse the JSON, then
fetch every `entries[].hash` separately. The bundle is leaf data, not a
manifest in the protocol sense.

## Relationship to [ADR 002](002-content-addressing.md#adr-002-content-addressing) / [ADR 013](013-schema-evolution.md#adr-013-schema-evolution)

- [ADR 002](002-content-addressing.md#adr-002-content-addressing) declares multi-file an
  application concern. This appendix is that concern, layered above the
  protocol — the network sees only the bundle blob and (later) each
  entry blob, never the bundle's structure.
- [ADR 013](013-schema-evolution.md#adr-013-schema-evolution) bounds *wire-format* evolution
  (varint framing, ALPN tiers, `#[serde(default)]` rules for signed
  payloads). Bundles are a **file format**, never on the wire, so those
  rules do not apply. Future versions bump `version` directly; parsers
  dispatch on the literal integer.

## Non-Goals

- `bundle inspect`, signing/attestation, encryption, compression,
  nested bundles, network publishing of the produced file — all v2+
  concerns, none of which the bundle BLAKE3 invariant depends on.
- **Generation of chunked entries.** `bundle create` and `origin
  import` emit whole-file entries. Choosing chunk boundaries that
  maximize sharing across a directory is a separate indexing problem; an
  external chunker produces a chunked manifest and `bundle pull`
  consumes it.
- This appendix does not add a wire format. Bundles never ride an ALPN;
  they exist only as on-disk JSON or as opaque blob bytes when published
  through the regular content path.

## Pull

`decdn bundle pull` reads a manifest — from a local file (`-i`) or by
fetching its own blob hash first (`--hash`, mutually exclusive) — and
fetches every entry over the paid `cdn/client/v1` path (the same kernel
as `decdn fetch`):

- **Node selection is per distinct blob.** With an explicit `--node-id`
  every entry is pulled from that one node; otherwise each distinct blob
  independently discovers a holder among the region-nearest active nodes
  (`CapacityBond`, read once), so different blobs may come from different
  nodes.
- **Duplicate blobs are fetched once.** A bundle may hold one file's
  content at two paths (nothing dedupes `entries[]` by `hash`). Entries
  sharing a `hash` are grouped and the blob is fetched — and **paid for**
  — a single time; the first destination receives the materialized bytes
  and every other destination is a hard link (or a copy, on a
  cross-device target) of it. Skip-existing and `--overwrite` are still
  evaluated **per destination**, so an already-present duplicate path
  triggers no fetch of its own (the group still fetches once if any
  sibling path needs bytes).
- **Concurrency** is bounded by `--jobs` (over distinct blobs). A payment pool's vouchers
  use a strictly increasing cumulative amount, so fetches that share one provider's
  lane are serialized by a per-provider lock; distinct providers proceed
  in parallel. A blob that clears the multi-source gate fans out to its
  admitted holders per [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1).
- **Output** files are written under `-o <dir>` at each entry's relative
  path, resolved with the § Path-safety rules above (`..`, absolute, and
  escaping paths rejected). Writes are atomic (temp-then-rename after the
  BLAKE3 check the fetch path already performs), so a present file is
  verified-good: pull **skips existing files** by default (re-runs
  resume), and `--overwrite` forces a re-fetch.
- **`--dry-run`** reports the plan without any network/chain activity
  (entries are only enumerable for the `-i` form; `--hash` cannot list
  them without first fetching the manifest).

Verification is intrinsic: content addressing means every fetched blob
is BLAKE3-checked against its `entries[].hash` by the fetch path, so
there is no separate verify toggle.

### Chunked entries

A `chunks` entry (see [Chunked files](#chunked-files)) is pulled in two
phases. First, `bundle pull` fetches every distinct chunk blob the
about-to-be-written entries reference, once each, over the same paid
path — so a chunk shared across entries is fetched and paid for once.
Then it concatenates each entry's chunk blobs in order into the
destination and verifies the assembled bytes against the whole-file
`hash`. The destination appears only after that check passes, by atomic
rename, so a present file is verified-good and re-runs resume. Each
chunk blob is BLAKE3-checked against its own hash on the way in; the
whole-file check additionally rejects a chunk list that is individually
valid but wrong or misordered.

Chunk dedup holds within one pull. Reuse across separate pulls — v2 of a
model skipping the chunks it already fetched for v1 — needs a persistent
chunk-addressed cache and is a follow-up.

### Deferred

- Coverage is limited to the region-nearest candidate set probed per
  distinct blob; a blob no probed node holds fails every entry naming it
  (re-runnable). Broadening beyond that set is a follow-up.
