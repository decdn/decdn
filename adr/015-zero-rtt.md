# ADR 015: QUIC 0-RTT Connection Establishment

**Date:** 2026-04-04
**Status:** Draft

## Context

Every QUIC connection begins with a TLS 1.3 handshake that costs one round trip (1-RTT) before application data can flow. In a CDN where probe fan-out is the critical latency path — a cache miss at PoC scale sends `ProbeRequest` to 30 peers, each requiring a separate QUIC connection when no connection to that peer already exists — this overhead is significant. At inter-continental RTTs (250-300ms), the handshake alone can consume half the probe collection window (`probe_max_wait` = 500ms, [ADR 001](001-network.md#content-discovery-probe-fan-out)).

TLS 1.3 defines a **0-RTT** mode: after a successful 1-RTT handshake, the server issues a session ticket. On the next connection to that server, the client sends early data (application bytes) alongside the TLS ClientHello, eliminating the round-trip wait. The trade-off is that 0-RTT data is **replayable** — a network adversary can capture and resend the early-data packet, causing the server to process the same request twice. This is acceptable for idempotent, read-only operations but dangerous for state-changing ones.

iroh's `Endpoint` exposes `connect_with_0rtt()` for this purpose — the same mechanism used by [iroh-experiments/content-discovery](https://github.com/n0-computer/iroh-experiments/tree/main/content-discovery) for tracker queries and announcements.

This ADR defines which protocols are eligible for 0-RTT, the replay safety rationale for each, and session ticket management requirements.

## Decision

### 0-RTT Eligibility by ALPN

| ALPN | 0-RTT | Rationale |
| --- | --- | --- |
| `cdn/probe/v1` | **Yes** | `ProbeRequest` is read-only and idempotent. A replayed probe produces a duplicate `ProbeResponse` that the requester deduplicates by `NodeId` in the probe cache. No state change on the responder. |
| `cdn/client/v1` | **No** | `StreamRequest` initiates a payment relationship. Replay could cause duplicate byte delivery or voucher accounting confusion. Subsequent streams on an established connection already benefit from QUIC stream multiplexing (zero additional handshake cost). |
| `cdn/watchtower/v1` | **No** | `WatchtowerRegister` has side effects (begins channel monitoring, allocates state). Connections are long-lived — handshake cost is amortized over hours or days. |
| `cdn/keys/v1` | **No** | ADR 006 requires an authenticated `EpochKeyAuth` stream before `PlayRequest` or `OfflineLeaseRequest` streams are accepted. Since `EpochKeyAuth` has authentication side effects (not 0-RTT safe), and all other stream types depend on it, 0-RTT is not viable for any `KeysMessage` variant on a new connection. On an existing connection the handshake is already complete. |

### Replay Safety Analysis

**`ProbeRequest` (safe):** Contains `{hash, timestamp_us}`. The `timestamp_us` field is requester-generated and echoed back in `ProbeResponse` for RTT measurement. A replayed probe causes the responder to evaluate `has_blob` and compute `rate_per_mb` a second time — both are pure reads with no side effects. The requester's probe cache deduplicates by `(hash, NodeId)`, so the duplicate response is discarded. The echoed `timestamp_us` from a replayed probe produces a stale RTT measurement, but since it arrives as a duplicate for an already-cached `NodeId`, it is never used.

Responders MUST NOT use `ProbeRequest` receipt to trigger any state change (e.g., cache priority boosting, demand-signal updates). If future protocol versions add such behavior, probe processing must be made replay-aware or 0-RTT eligibility must be revoked.

**`KeysMessage::PlayRequest` (side-effect-free but blocked by auth sequencing):** Contains `{blob_hash}`. The app server wraps the blob key with the current epoch key and a fresh random nonce per response — the operation is side-effect-free and read-only, but response bytes are not deterministic across calls (due to `nonce_wrap`). Despite being replay-safe in isolation, `PlayRequest` cannot be sent as 0-RTT early data because ADR 006 requires an authenticated `EpochKeyAuth` stream on the connection before the server will accept it. On a new 0-RTT connection, no auth stream exists yet, so the server would reject the request.

**`StreamRequest` (unsafe):** Initiates paid byte delivery. Replay could cause a node to begin streaming bytes and expect voucher payment for a transfer the client did not request. Even if the node detects the duplicate `channel_id` + `byte_offset` combination, the window between replay receipt and detection creates accounting ambiguity.

**`WatchtowerRegister` (unsafe):** Allocates monitoring state on the watchtower. Replay could cause duplicate channel registration, resource exhaustion, or incorrect `latest_voucher` state if the replayed registration carries a stale voucher.

### Session Ticket Management

After a successful 1-RTT handshake on a 0-RTT-eligible ALPN, the implementation MUST:

1. **Cache the session ticket** keyed by `(remote_node_id, ALPN)` in an in-memory LRU cache.
2. **Wait for the ticket before closing.** After the application exchange completes, spawn a background task that waits up to 2x the measured RTT (minimum 100ms, maximum 2 seconds) for the server's NewSessionTicket message before closing the connection. This matches the pattern used by iroh-experiments/content-discovery.
3. **Honor server-advertised expiry.** Discard tickets whose `ticket_lifetime` has elapsed. Additionally, implementations SHOULD discard tickets after 24 hours regardless of the advertised lifetime (which RFC 8446 caps at 7 days) to limit the window of ticket theft impact.
4. **LRU eviction.** Maximum 1,000 cached tickets. At PoC scale (30 nodes x 1 eligible ALPN = 30 active entries), this provides ample headroom.

### 0-RTT Rejection Handling

Servers MAY reject 0-RTT at any time (e.g., after key rotation, ticket expiry, or deliberate policy). On rejection:

1. The QUIC handshake completes as a normal 1-RTT connection.
2. The client MUST re-send the request on a confirmed (post-handshake) stream.
3. The client caches the new session ticket for subsequent attempts.

Implementations SHOULD NOT treat 0-RTT rejection as an error — it is a normal part of the protocol. The rejection path is functionally equivalent to a cold connection; the only cost is one wasted round trip of early data.

### Server-Side Controls

Nodes MUST configure 0-RTT acceptance per ALPN:

- **`cdn/probe/v1`:** Accept 0-RTT. Process early-data `ProbeRequest` immediately.
- **`cdn/client/v1`:** Reject 0-RTT (do not configure `max_early_data_size`). This is the default — QUIC servers that do not explicitly enable 0-RTT will reject it.
- **`cdn/watchtower/v1`:** Reject 0-RTT.
- **`cdn/keys/v1`:** Reject 0-RTT. Authentication sequencing ([ADR 006](006-e2e-encryption.md)) requires `EpochKeyAuth` before any other stream type, which is incompatible with 0-RTT early data.

### Impact on Probe Fan-Out Latency

At PoC scale with 30 peers, a cache miss sends `ProbeRequest` to all known nodes. When connections must be (re-)established — after the first probe cycle or after idle timeout closes them — each new connection costs 1 RTT before the probe is sent:

| Scenario | Probe send delay | Notes |
| --- | --- | --- |
| Cold (no ticket) | 1 RTT | Full TLS 1.3 handshake |
| Warm (cached ticket, 0-RTT accepted) | 0 RTT | Probe sent with ClientHello |
| Warm (cached ticket, 0-RTT rejected) | 1 RTT | Fallback to 1-RTT, re-send |

After the first probe cycle, all 30 peer connections have cached tickets. Subsequent cache misses send probes with zero handshake delay. Combined with the adaptive early-exit mechanism ([ADR 001](001-network.md#content-discovery-probe-fan-out)), this reduces P50 cache-miss latency for reconnections by eliminating the handshake round trip — probes complete in one RTT (probe send + response) rather than two (handshake + probe), making `probe_min_wait` the dominant factor for nearby peers.

### Observability

Implementations SHOULD expose the following metrics, labeled by ALPN:

| Metric | Type | Description |
| --- | --- | --- |
| `quic_0rtt_attempts_total` | Counter | 0-RTT connection attempts |
| `quic_0rtt_accepted_total` | Counter | 0-RTT accepted by server |
| `quic_0rtt_rejected_total` | Counter | 0-RTT rejected, fell back to 1-RTT |
| `quic_session_ticket_cache_size` | Gauge | Current number of cached session tickets |
| `probe_fanout_latency_seconds` | Histogram | Probe collection duration (labels: `0rtt=warm\|cold`) |

The `probe_fanout_latency_seconds` histogram with the `0rtt` label enables operators to measure the real-world impact of 0-RTT on cache-miss latency and tune `probe_min_wait` accordingly.

## Consequences

- **Probe latency improves** for warm connections (all connections after the first cycle). The 1-RTT handshake cost is eliminated from the critical path.
- **No change to payment security.** `cdn/client/v1` and `cdn/watchtower/v1` remain 1-RTT-only. Payment channel setup, voucher exchange, and watchtower registration are never sent as early data.
- **Session ticket storage** adds a small memory footprint (~200 bytes per ticket x 1,000 max = ~200 KB).
- **Complexity cost** is modest: iroh's `connect_with_0rtt()` handles the transport-level details. The implementation burden is the ticket cache, the per-ALPN accept/reject configuration, and the fallback path.
- **Future protocol versions** that add state-changing behavior to `ProbeRequest` must re-evaluate 0-RTT eligibility. The replay safety analysis in this ADR is tied to the current message semantics. If ADR 006's authentication sequencing for `cdn/keys/v1` is relaxed in the future, `PlayRequest` could become 0-RTT eligible (it is side-effect-free), but that would require a separate decision.

## References

- [iroh-experiments/content-discovery](https://github.com/n0-computer/iroh-experiments/tree/main/content-discovery) — demonstrates 0-RTT for tracker queries over iroh QUIC
- [RFC 8446 Section 8](https://www.rfc-editor.org/rfc/rfc8446#section-8) — TLS 1.3 0-RTT and anti-replay
- [ADR 001 — Probe Fan-Out](001-network.md#content-discovery-probe-fan-out)
- [ADR 005 — Connection Management](005-protocol.md#connection-management)
- [ADR 006 — E2E Encryption and Key Distribution](006-e2e-encryption.md) — authentication sequencing that precludes `cdn/keys/v1` 0-RTT
- [ADR 013 — Schema Evolution](013-schema-evolution.md) — `KeysMessage` enum framing
