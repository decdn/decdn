# ADR 025 — Client Download Resume: Crash Recovery and State Persistence

**Status:** Accepted
**Date:** 2026-04-10
**Deciders:** Core team

---

## Context

A client downloading a large blob (e.g., a 50 GB AI model) may crash, lose network connectivity,
or be killed (SIGKILL) at any point mid-download. Without persistent state, the client must
restart the download from byte 0 — discarding all previously delivered bytes, all open payment
channels, and all BLAKE3-verified progress.

This is both wasteful (retransmitting already-verified data) and potentially expensive: payment
channels already spent vouchers for the abandoned bytes. A node that received vouchers for the
delivered bytes would rightfully settle them; the client would have paid for bytes it no longer
has locally. A resume mechanism avoids both problems.

### Existing primitives

- **`byte_offset` in `StreamRequest`** (ADR 005): The server resumes delivery from an arbitrary
  byte offset. This enables the protocol to resume a stream mid-blob.
- **BLAKE3 incremental verification** (iroh-blobs): Bytes can be verified incrementally; the last
  successfully verified byte is the safe resume point.
- **Payment channels with nonce** (ADR 003): Vouchers are sequenced by nonce. A persisted nonce
  prevents double-spend and enables the channel to continue from where it left off.

---

## Decision

### On-disk layout

Each in-progress download occupies a directory under the user's downloads directory:

```
~/.decdn/downloads/<hash>/
  state.json        # authoritative resume state (atomically written)
  blob.partial      # raw bytes, written sequentially (or as ranges in multi-channel mode)
  range-0.part      # [only in multi-channel mode] bytes for range 0
  range-1.part      # [only in multi-channel mode] bytes for range 1
  ...
```

The `<hash>` is the BLAKE3 hash of the blob being downloaded (the request hash). This is stable,
collision-resistant, and content-identified — two downloads of the same blob share the same
directory and can resume each other's work.

Downloads directory location resolution order:

1. `DECDN_DOWNLOADS_DIR` environment variable
2. `--downloads-dir` CLI flag
3. `~/.decdn/downloads/` (default)

### State file format (`state.json`)

```jsonc
{
  // protocol version for forward compatibility
  "version": 1,

  // the requested blob hash (BLAKE3 hex)
  "hash": "...",

  // total size in bytes; null until known (after first StreamResponse)
  "total_bytes": 52428800000,

  // download mode: "single" or "parallel"
  "mode": "single",

  // list of ranges; single-channel mode has exactly one entry covering [0, total_bytes)
  "ranges": [
    {
      "start_byte": 0,
      "end_byte": 17476266666,
      // last byte index confirmed received AND BLAKE3-verified; -1 = none
      "verified_offset": 8738133333,
      // null if channel not yet opened
      "channel_id": "0xabc...def",
      // last committed voucher nonce; -1 = no vouchers sent yet
      "voucher_nonce": 42,
      // node public key (iroh NodeId) serving this range
      "node_id": "..."
    }
  ],

  // ISO 8601 timestamp of last state write
  "updated_at": "2026-04-10T14:23:01Z"
}
```

Fields are written atomically (see below). Consumers of `state.json` must tolerate a missing
`total_bytes` (null) — this is valid while the first `StreamResponse` is in flight.

### Atomic writes

`state.json` must never be partially written. A crash during a non-atomic write would corrupt
the file, losing all resume state. The client uses write-to-temp-then-rename:

```
1. Write new state to  ~/.decdn/downloads/<hash>/state.json.tmp
2. fsync(state.json.tmp)
3. rename(state.json.tmp, state.json)   // atomic on POSIX
```

`rename` is atomic on all POSIX filesystems. On Windows, `MoveFileExW(MOVEFILE_REPLACE_EXISTING)`
provides the same guarantee.

### State flush frequency

The client flushes `state.json` after each successfully BLAKE3-verified segment. Segment size is
capped at 64 MB — larger segments increase data loss on crash; smaller segments increase I/O
overhead. The 64 MB cadence means worst-case re-download after a crash is 64 MB per range.

The voucher nonce is flushed to `state.json` on every voucher send, not just at segment
boundaries. Voucher nonces must be strictly monotone — a nonce must never be reused across a
resume. Losing a nonce would allow a node to reject the resumed session as a replay.

### Resume procedure

On startup, `decdn pull <hash>` checks for an existing `~/.decdn/downloads/<hash>/` directory:

1. If absent: start fresh — create directory, initialise `state.json`, open channels.
2. If present: load `state.json`. For each range with `verified_offset >= 0`:
   a. Attempt to reconnect to the same `node_id` (probe first to confirm it still has the blob).
   b. If the node is unreachable, probe for a replacement node.
   c. Open a new connection (potentially a new channel if the old one is settled).
   d. Send `StreamRequest` with `byte_offset = verified_offset`.
   e. Continue writing `blob.partial` (or `range-N.part`) from `verified_offset`.

### Channel resume vs. new channel

If the original payment channel (`channel_id` in state) is still open (not settled on-chain),
the client **reuses it** — sends vouchers starting from `voucher_nonce + 1`. This avoids paying
the $0.23 channel lifecycle cost again.

If the channel is already settled (node closed it after the crash), the client opens a new channel.
It is safe for the client to deposit only enough for the remaining bytes:

```
remaining_deposit = ((end_byte - verified_offset) / 1e6) × rate_per_mb × 1.05
```

### Completion and cleanup

After all bytes are downloaded and the full-blob BLAKE3 hash is verified:

1. Move or copy `blob.partial` (or reassemble from `range-N.part` files) to the output path.
2. Delete `~/.decdn/downloads/<hash>/` (the entire directory).

On verification failure (full-blob BLAKE3 mismatch), the download state is preserved for
debugging but the partial file is not moved. The user is notified and can retry (`decdn pull`
will resume from the last verified offset).

### Stale state cleanup

State directories accumulate for abandoned downloads. The client provides:

```
decdn downloads list            # show in-progress downloads and their state
decdn downloads clean           # remove completed and failed downloads
decdn downloads clean --all     # remove all downloads including in-progress
decdn downloads resume <hash>   # explicit resume (same as decdn pull <hash>)
```

State directories older than 30 days with `verified_offset = 0` (no progress) are considered
abandoned and can be purged by `clean`.

---

## Cargo Feature: `poc`

In the PoC, the state file and resume logic are simplified:

- Single-channel mode only (no multi-channel `range-N.part` files)
- State flush frequency is 256 MB (coarser; acceptable for PoC small files)
- `decdn downloads` subcommands are not implemented; only `decdn pull` with auto-resume

---

## Consequences

### Positive

- No data is re-downloaded after a crash; worst case is 64 MB per range
- No double-payment: resumed sessions reuse open channels, new channels cover only remaining bytes
- Stable directory layout makes progress visible (`ls ~/.decdn/downloads/`)
- Atomic writes prevent state corruption even under SIGKILL

### Negative

- Disk space: `blob.partial` holds bytes before final verification; for a 50 GB download, 50 GB
  of temp space is needed during the download
- State version migration is required when `state.json` format changes
- `rename` atomicity on network filesystems (NFS, SMB) is not guaranteed; users mounting
  `~/.decdn/` on a network share get no crash-safety guarantee

### Neutral

- The `byte_offset` resume point is the protocol's safe restart position — no change to wire
  protocol is needed (ADR 005)
- Multi-channel mode from ADR 024 is a superset of single-channel; the range array naturally
  degenerates to one entry for single-channel

---

## Alternatives Considered

### Sparse file with byte-range tracking

Instead of range part files, use a single sparse file and a separate byte-range bitmap to track
which bytes are verified. Rejected because: sparse file support varies across OS (macOS, Linux,
Windows behave differently); a byte-range bitmap at 64 MB granularity is exactly the range list
already in `state.json`. The two approaches are equivalent; explicit part files are simpler and
portable.

### WAL (write-ahead log) for state

Instead of a single atomically-replaced `state.json`, maintain a write-ahead log of state deltas.
Rejected: the state object is small (a few KB) and rewrites entirely on each flush. WAL
complexity is not justified. The atomic rename pattern is sufficient and well-understood.

### Server-side resume tokens

The server could issue a signed resume token encoding the channel state. Rejected: nodes are
not required to maintain session state — the `cdn/client/v1` protocol is stateless per stream.
The client owns the resume state.

---

## Cross-ADR Consistency

| ADR | Relationship |
|-----|-------------|
| ADR 003 | Channel resume reuses open channels to avoid $0.23 lifecycle cost |
| ADR 005 | `byte_offset` in `StreamRequest` is the protocol mechanism for resume |
| ADR 012 | Client architecture; this ADR specifies the persistence layer under ADR 012 |
| ADR 024 | Multi-channel mode adds per-range state entries; layout is defined here |
| ADR 026 | Manifest download is also persisted in the state file (manifest hash + verified flag) |
