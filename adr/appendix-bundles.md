# Appendix: Directory Bundles (`decdn bundle`)

> **This is an appendix, not a core protocol ADR.** Bundles are a
> publisher-side convenience for grouping multiple BLAKE3-content-addressed
> blobs into a single JSON manifest file. Nodes do not require bundle
> support — they deliver individual hashes. Alternative grouping
> mechanisms (tarballs, the per-blob chunk manifest from
> [ADR 012 § File Manifests and Reconstruction](012-client.md#file-manifests-and-reconstruction))
> remain valid for their own use cases.

**Date:** 2026-05-07 — **Status:** Accepted (`bundle create`
ships; `bundle pull` deferred). **Touches:** `decdn` CLI, content
publishing workflow.

## Context

[ADR 002](002-content-addressing.md) makes the network single-blob
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
(array). Each entry has exactly three keys, in this order: `path`
(string), `hash` (string), `size` (integer). Field declaration order
is load-bearing — see [Determinism](#determinism).

- `path` — relative POSIX path (`/` separator on every platform), UTF-8.
  `..`, absolute paths, root prefixes, and non-UTF-8 components are
  rejected at create time.
- `hash` — `b3:` followed by the 64-character lowercase hex of the
  file's BLAKE3. See [Hash format](#hash-format).
- `size` — file size in bytes, unsigned 64-bit integer. Currently
  informational; reserved for `bundle pull` (progress + pre-flight)
  once the fetch primitive ships.

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
  size)`. `serde_json` serializes structs in declaration order — the
  Rust struct definitions are the canonical key-order spec.
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
the bundle hash; clients (eventually) fetch the bundle hash, parse the
JSON, then fetch every `entries[].hash` separately. The bundle is leaf
data, not a manifest in the protocol sense.

## Non-relationship to ADR 012's `DECDNMAN` chunk manifest

[ADR 012 § File Manifests and Reconstruction](012-client.md#file-manifests-and-reconstruction)
defines a binary chunk manifest with the magic prefix `DECDNMAN` for
streaming a single large blob in pieces. Bundles are **not** that:

- `DECDNMAN` is a **transport-side, intra-file** binary format — one
  logical blob split into ordered chunks with shared total-size
  metadata. Clients reassemble it into one file.
- Bundles are a **publisher-side, inter-file** JSON format — many
  independent content blobs grouped by relative path. Clients
  reconstruct a directory tree.

Different layer, different audience, different magic. The two compose:
an entry's `hash` may itself be a `DECDNMAN` manifest blob hash for a
chunked single file inside a bundled directory.

## Relationship to ADR 002 / ADR 013

- [ADR 002](002-content-addressing.md) declares multi-file an
  application concern. This appendix is that concern, layered above the
  protocol — the network sees only the bundle blob and (later) each
  entry blob, never the bundle's structure.
- [ADR 013](013-schema-evolution.md) bounds *wire-format* evolution
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

## Deferred & Open

- `decdn bundle pull -i <file>` and `decdn bundle pull --hash <b3>`
  are blocked on a client-side fetch primitive that does not exist
  yet. Both `bundle pull` variants will land in
  a follow-up once the fetch primitive is implemented.
