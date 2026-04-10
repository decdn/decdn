# ADR 024 — Client Parallelism: Multi-Node Parallel Blob Download

**Status:** Accepted
**Date:** 2026-04-10
**Deciders:** Core team

---

## Context

ADR 012 defines a single-node streaming model: a client opens one `cdn/client/v1` connection and
downloads a blob sequentially. ADR 005 already supports concurrent QUIC streams (up to 100 per
connection) and `byte_offset` for range-based fetching. What is missing is a design for splitting
a single large blob across multiple nodes simultaneously — BitTorrent-style — where each node
serves a distinct byte range in parallel.

### Economics of multi-channel parallelism

Opening, closing, and settling a payment channel costs approximately $0.23 in L2 gas
(ADR 003). For parallelism across N nodes, a naive approach of one channel per node costs
N × $0.23 in fixed overhead before a single byte is delivered. This overhead dominates for small
blobs:

| Blob size | Rate ($0.01/GB) | Download cost | 4-node channel overhead | Overhead % |
|-----------|-----------------|---------------|------------------------|------------|
| 100 MB | $0.001 | $0.001 | $0.92 | >99% |
| 1 GB | $0.01 | $0.01 | $0.92 | 99% |
| 5 GB | $0.05 | $0.05 | $0.92 | 95% |
| 50 GB | $0.50 | $0.50 | $0.92 | 65% |
| 500 GB | $5.00 | $5.00 | $0.92 | 15% |

Parallelism is clearly not economically viable below ~5 GB at market rates. The breakeven
threshold where overhead is less than 10% of download cost is approximately:

```
breakeven_bytes = (N × $0.23) / (0.10 × rate_per_byte)
                = (4 × $0.23) / (0.10 × $0.01/1e9)
                ≈ 9.2 GB  (for N=4 at $0.01/GB)
```

The threshold scales linearly with N (number of channels) and inversely with rate.

### When parallelism is worthwhile

Parallelism makes sense for:

- **Bulk, order-independent downloads:** AI models (often 5–100+ GB), large datasets, archives.
- **High-latency nodes:** A slow node serving one range while fast nodes serve others improves
  overall throughput.
- **Content served by many nodes with different cached ranges:** Distributes load across the
  network organically.

Parallelism does **not** make sense for:

- **Sequential streaming:** Video playback, audio streaming, progressive rendering. These require
  in-order byte delivery; reassembly buffering would introduce unbounded latency.
- **Small blobs:** Below the economic threshold, overhead exceeds savings.

---

## Decision

### Model

The client splits a blob into N contiguous byte ranges and opens one connection per range to a
different node. Each connection uses a separate payment channel. Ranges are assigned at download
start based on known `total_bytes` from an initial probe; the client reassembles the complete blob
after all ranges complete.

### CLI interface

```
decdn pull <hash> [OPTIONS] -o <output>

Options:
  --max-channels <N>      Maximum number of parallel node connections [default: 1]
  --min-blob-size <SIZE>  Disable parallelism if blob smaller than SIZE [default: 10GiB]
  --streaming             Force sequential delivery (for piped / streaming output)
```

`--max-channels 1` (the default) is identical to the existing sequential model. Setting
`--max-channels N` where N > 1 opts into parallel mode subject to the `--min-blob-size` check.

`--streaming` overrides `--max-channels` and forces sequential delivery. This is the correct
mode for piped output (`decdn pull <hash> --streaming | ffplay -`) where in-order bytes are
required.

### Range assignment

1. **Probe phase:** The client probes candidate nodes to confirm `has_blob: true` and collect
   `rate_per_mb` and round-trip latency. At least N nodes with `has_blob: true` are needed; if
   fewer are found, `--max-channels` is reduced to the number of available nodes.
2. **Total size:** `total_bytes` is provided by the node in `StreamResponse` (ADR 005). The
   client sends a speculative `StreamRequest` to the fastest probe candidate to learn
   `total_bytes`, then cancels and reassigns ranges. Alternatively, the manifest (ADR 026)
   provides `total_bytes` before the first stream connection.
3. **Range split:** Divide `[0, total_bytes)` into N equal ranges. The last range absorbs the
   remainder (`total_bytes % N` bytes). Minimum range size: 256 MB; if `total_bytes / N <
   256 MB`, reduce N until the minimum is satisfied.
4. **Node assignment:** Assign the lowest-latency node (from probe) to the first range, to
   minimize time-to-first-byte. Remaining ranges assigned in latency order.

### Payment channel strategy

One payment channel per node. Vouchers on each channel are independent — there is no cross-channel
voucher aggregation. Each channel is sized to cover its assigned range:

```
channel_deposit = (range_bytes / 1e6) × rate_per_mb × safety_margin
safety_margin   = 1.05   // 5% buffer for rate fluctuation
```

The minimum deposit floor from ADR 003 (1 USDC, governable) still applies; if the calculated
deposit falls below it, the floor is used.

**Why separate channels (not a shared channel across nodes)?**

A shared channel would require the client to be connected to all N nodes on the same
`(client, provider)` channel. Payment channels are per `(client, provider)` pair — each
node is a distinct provider. Multi-provider sharing is not supported by the contract.

### Reassembly

Each range is written to a temporary range file (see ADR 025 for on-disk layout). After all
ranges are downloaded and BLAKE3-verified at the range level, the client concatenates ranges
in order into the final output file. The full-blob BLAKE3 hash is then verified against the
requested hash. If verification fails, the download is aborted and the output file is deleted.

```
~/.decdn/downloads/<hash>/
  range-0.part   # bytes [0,        total/N)
  range-1.part   # bytes [total/N,  2*total/N)
  ...
  range-N-1.part
  state.json     # range assignments, channel IDs, verified offsets
```

### Failure handling

- **Node failure mid-range:** If a node connection drops or stalls (10-second timeout per ADR 005),
  the client re-probes for a replacement node and resumes from the last BLAKE3-verified byte
  within that range. The failed channel is closed with the latest voucher. A new channel is opened
  with the replacement node.
- **All nodes fail:** Falls back to sequential download from any remaining node.
- **Rate mismatch:** If a node's `StreamResponse.rate_per_mb` exceeds the probed rate by more than
  30 seconds, the client disconnects and finds a replacement (slashable — see ADR 005).

### Sequential streaming mode (ordering guarantee)

When `--streaming` is set or `--max-channels 1` (the default), the client uses a single
connection to a single node with sequential byte delivery. This is the correct mode for:

- Piped output (`| ffplay`, `| python model_load.py`)
- Any consumer that requires bytes in order before the full blob is available

For multi-channel mode, the client buffers all range parts to disk before producing output, so
the output file is only available after all ranges complete. There is no partial streaming output
in multi-channel mode.

---

## Cargo Feature: `poc`

In the PoC, `--max-channels` is accepted but capped at 1. Multi-channel parallelism is a
production feature — the PoC targets small testnets where single-node throughput is sufficient.
This avoids the complexity of multi-channel channel management in the initial implementation.

---

## Consequences

### Positive

- Significant throughput improvement for large bulk downloads (AI models, datasets)
- Distributes load across network organically — popular large blobs naturally use many nodes
- CLI flag makes the trade-off explicit and user-controllable
- Graceful degradation: falls back to single-node if insufficient nodes available

### Negative

- N × channel overhead makes this uneconomical below ~10 GB at market rates
- Adds reassembly complexity: temporary part files, state persistence, full-blob recheck
- Multi-channel mode cannot stream output progressively — output only available after all parts complete
- Node failure mid-range requires re-probing and new channel open (additional gas cost)

### Neutral

- The `byte_offset` field in `StreamRequest` already supports range fetching — no protocol changes needed
- BLAKE3 incremental verification (iroh-blobs) works per range without modification

---

## Alternatives Considered

### Single channel, multiple ranges from the same node

Rejected for the parallel case. The goal is to use multiple nodes simultaneously to increase
aggregate throughput. Multiple streams on one connection to one node (already supported via QUIC
stream multiplexing — ADR 005) parallelizes within one node but does not distribute across the
network and does not improve throughput for bandwidth-bound nodes.

### Dynamic range reassignment (work-stealing)

Considered: split blob into many small chunks (e.g., 64 MB each), maintain a work queue, assign
chunks to nodes as they complete previous chunks. Rejected for PoC complexity. Added as a future
production optimization — particularly useful when nodes have widely varying throughput.

### Channel pre-funding (single deposit covering all ranges)

Considered: deposit all funds into one channel before splitting ranges. Not viable — payment
channels are per `(client, provider)` pair. The contract does not support multi-provider channels.

---

## Cross-ADR Consistency

| ADR | Relationship |
|-----|-------------|
| ADR 003 | Channel cost (~$0.23 lifecycle) drives the economic threshold |
| ADR 005 | `byte_offset` and `total_bytes` in `StreamRequest`/`StreamResponse` enable range fetching |
| ADR 012 | Client architecture; probe and node selection extended for multi-node selection |
| ADR 022 | DHT lookup finds nodes with `has_blob: true`; parallel download uses N such nodes |
| ADR 025 | Range part files and state persistence are specified in ADR 025 |
| ADR 026 | Manifest provides `total_bytes` before first stream, improving range assignment |
