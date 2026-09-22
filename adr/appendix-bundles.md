# Appendix: Directory Bundles (`decdn bundle`)

> **This is an appendix, not a core protocol ADR.** Bundles are a
> publisher-side convenience for grouping multiple BLAKE3-content-addressed
> blobs into a single JSON manifest file. Nodes do not require bundle
> support — they deliver individual hashes. Alternative grouping
> mechanisms (tarballs and the like) remain valid for their own use
> cases.

**Date:** 2026-05-07 — **Status:** Accepted (`decdn origin import`
generates manifests and `bundle pull` consumes them; both ship).
**Touches:** `decdn` CLI, content publishing workflow.

## Context

[ADR 002](002-content-addressing.md#adr-002-content-addressing) makes the network single-blob
content-addressed: every byte stream is identified by its BLAKE3 hash,
and the protocol is "content-agnostic … applications may define their
own metadata or manifest layers on top". Multi-file publishing is one
such layer. Hand-authoring `path → hash` JSON is error-prone, so `decdn
origin import` ships a deterministic manifest generator alongside the
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
required. `origin import` always emits `size`, but a reader treats it as
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
- `chunks` — optional ordered list of range-dedup hints over the file's
  bytes. See [Chunked files](#chunked-files). When absent, the file is
  one blob addressed by `hash`.

## Chunked files

A node stores and serves one file as one whole-file blob. It never
stores or serves a chunk as its own blob. A chunk hash is not an
address a client can fetch.

A `chunks` array is a list of hints. Each hint names a byte range of
the file and its BLAKE3 hash. A client uses a hint to recognize bytes
it already holds from another file in the same pull. It skips the
download of that byte range and splices its local copy in. The
`hash` field stays the file's only fetchable and authoritative
identity: the client verifies the assembled file against `hash`, and
a wrong or stale hint only costs a re-download, never a wrong file.

```json
{"path":"model.safetensors","hash":"b3:whole...","size":100,"chunks":[{"hash":"b3:c0...","size":60},{"hash":"b3:c1...","size":40}]}
```

- `hash` and `size` always describe the **whole file**. `hash` is the
  file's identity and the end-to-end validator. The client always
  fetches (or completes, via splice) the whole file and always checks
  it against `hash`.
- Each element of `chunks` has `hash` (`b3:<hex>`, a hash over that
  byte range) then `size` (the range's byte length). The sizes sum to
  the entry `size`.
- Hints are listed in **content order**: the ranges they name, in
  sequence, cover the whole file. This order is load-bearing and never
  sorted.

A hint earns its keep only when two files in the same pull share a byte
range (two textures that share a region, or two model checkpoints that
share most of their weights). The client fetches that range once, pays
for it once, and splices it into every file that names it. A file with
no shared ranges costs the same as an unchunked file plus the manifest
entry.

## Hash format

Bundle entries use a `b3:<64-hex>` prefix on every hash. The prefix
makes the hash function explicit and leaves room for a later
`<algorithm>:<hex>` tag if a different hash function ever needs to live
at this layer. **This is a bundle-format convention**, not a
project-wide rename: on-wire fields elsewhere carry bare hex (e.g.
`decdn node evict <hash>`), and that stays unchanged. Bundles are
publisher-side and free to set their own conventions.

## Path-safety rules (creation-time invariants)

`origin import` enforces every rule below; consumers SHOULD re-validate
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
- This appendix does not add a wire format. Bundles never ride an ALPN;
  they exist only as on-disk JSON or as opaque blob bytes when published
  through the regular content path.

## Generation

`decdn origin import` walks a local file or directory and emits the
canonical bundle manifest for it (see [Determinism](#determinism)). By
default each file becomes one whole-file blob, addressed by `hash`.

With `--optimize`, `origin import` stores each file as one whole-file blob
(as it always does) and additionally content-defines chunks (fastcdc
v2020) over its bytes to compute the manifest's range-dedup hints.
`--chunk-avg` is the primary dial: it sets the target chunk size and
defaults to 4 MiB, the ceiling fastcdc's averaging window supports.
`--chunk-min` and `--chunk-max` are advanced rails around that target;
`--chunk-min` is pinned at a 1 MiB floor, matching `MB_BYTES`, the fixed
payment interval — a hint smaller than one payment interval buys nothing.
Each chunk becomes one hint: its BLAKE3 and its byte length. A chunk hash
is never stored or served as its own blob; `origin import` writes only
the one whole-file blob, and the hint exists solely so a later `bundle
pull` can recognize the same bytes in another file (see
[Chunked files](#chunked-files)).

A single-file `--optimize` import produces one manifest entry with a
`chunks` hint list; a directory import produces one entry per file, each
with hints computed over that file alone. A shared range between two
files is recognized at pull time, by hash equality between their hint
lists, not at import time.

`bundle pull` consumes a chunked manifest exactly as it consumes an
unchunked one — see [Chunked entries](#chunked-entries) under Pull.

`--to <dir>` writes blobs into a local filesystem cache origin store and
is normally required. The filesystem is the only write backend. To fill an
S3 origin, import to a directory and then sync it up with
`aws s3 sync <dir> s3://<bucket>/<prefix>`. The fs and S3 object layouts are
identical, so the sync renames nothing. An `s3://` or `http(s)://` value is
rejected with that recipe. `--dry-run` makes `--to` optional: it writes no blobs
anywhere and prints the canonical manifest bytes to stdout instead, so
`origin import --dry-run` is the local "just make me a manifest" path —
a publisher who only wants the manifest, without seeding an origin
store yet, runs this and nothing else. `--bundle FILE` additionally
writes the manifest to a file, with or without `--dry-run`.

`--subfolder DIR` prefixes every manifest entry's `path` with `DIR`, so a
pull writes the whole bundle under one directory rather than into the
output root. `DIR` is a relative POSIX path and obeys the same path-safety
rules as an entry path — `..`, absolute paths, and root prefixes are
rejected; nesting (`a/b`) is allowed. A backslash is rejected too: `\` is
not a POSIX separator, so it would produce a manifest path that a pull
reads differently across platforms. The prefix is uniform, so it keeps
the entries in their by-path-bytes order. The prefix is part of the
manifest bytes, so it changes the bundle hash and every downstream pull
sees the folder.

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
- **Concurrency** is bounded by `--jobs` (over distinct blobs). Fetches share a
  `LaneLedger` per `(pool, signer, provider)` lane. `--max-lane-streams`
  (default 4) caps how many streams run at once on one lane. The shared
  ledger keeps voucher issuance monotonic across concurrent streams on a
  lane. The serving node credits each stream's delivered bytes from the
  lane's one cumulative watermark, so a fast stream does not starve a slow
  co-stream. Distinct lanes proceed in parallel. A range-dedup entry pays for
  its ranges through one session per provider. The session opens one
  connection and reuses it for every range of the entry. It fills up to
  `--max-lane-streams` ranges at once, within the lane permits that are free.
  A blob that clears the
  multi-source gate fans out to its admitted holders per
  [ADR 039](039-multi-source-parallel-fetch.md#adr-039-multi-source-parallel-fetch-scheduling-on-cdnclientv1).
- **Failover and retry.** Each entry tries its probed candidates in order. A
  retryable failure moves to the next candidate. A terminal failure stops
  the entry. When the last candidate fails, the entry's error says that
  every candidate failed. After the first pass over the bundle,
  `--entry-retries` (default 2) runs each entry that failed retryably again.
  Before each round the pull waits: 2 s, then double the last wait, to a
  maximum of 30 s. Each round probes the holders again, so a provider that
  failed before is a candidate again. Each round continues from the entry's
  `.partial`, so no byte is paid for twice. A pool exhaustion moves to the
  next candidate in a pass, but it does not start a new round: every
  provider refuses the same deposit.
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

A `chunks` entry (see [Chunked files](#chunked-files)) is still one
whole-file blob, fetched over the paid path. When a chunk hint hash
appears in two or more entries, the run assigns that shared chunk to one
entry — its smallest holder — and only that entry pays to fetch it. Every
other entry that names the chunk splices the range from the assigned
holder's on-disk bytes and pays nothing for it. A splice source is always
the verified bytes of a completed, whole-file-hash-checked entry, never an
independently fetched chunk.

The run schedules entries smallest whole-file first, so a shared chunk's
assigned holder starts — and finishes — before the larger entries that
splice from it. Each entry drives and pays for its own ranges up front (its
unique chunks, the chunks assigned to it, and any un-splice-able edge
groups); it does not drive the ranges it defers to a sibling. It reconciles
those deferred ranges at its tail: it splices each one as the assigned
holder registers it, and waits on the holder rather than a clock, because
the deferred bytes are exactly what that holder is still downloading. A
deferred range is driven and paid for by the waiting entry only if its
assigned holder FINISHES without producing the chunk — a failed fetch — so
the run always completes and never pays more than a direct fetch.

The reassembled bytes are verified against the whole-file `hash` before
the destination appears, by atomic rename, so a present file is
verified-good and re-runs resume. A spliced range is re-checked against
its own hint hash before it is trusted, so a donor entry whose bytes
were tampered with cannot poison a recipient; the whole-file check is
the final backstop against a hint that is individually valid but wrong
or misordered — the client re-fetches the entry whole rather than trust
it.

Range-dedup holds within one pull. Reuse across separate pulls — v2 of a
model skipping the ranges it already fetched for v1 — needs a persistent
range-addressed cache and is a follow-up.

At the end of a pull the report states the distinct content bytes fetched
and the bytes written to disk — `downloaded X → reconstructed Y` — whenever
dedup made them differ, which happens only when one blob is materialized
to several paths. Range-dedup savings from spliced chunk hints do not
show up in `downloaded`; they are reported separately as `spliced_bytes`.
When `downloaded` equals `reconstructed` the report shows a single
`downloaded X`. `downloaded` sums content lengths, not exact on-wire bytes.

### Deferred

- Coverage is limited to the region-nearest candidate set probed per
  distinct blob; a blob no probed node holds fails every entry naming it
  (re-runnable). Broadening beyond that set is a follow-up.
