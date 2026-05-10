# ADR 015: QUIC 0-RTT Connection Establishment

**Date:** 2026-04-04
**Status:** Draft

## Context

Every QUIC connection begins with a TLS 1.3 handshake that costs one round trip (1-RTT) before application data can flow. In a CDN where probing is the critical latency path — a cache miss runs DHT FIND_VALUE to obtain 3-5 candidate NodeIds, then sends `ProbeRequest` to each in parallel, each requiring a separate QUIC connection when no connection to that peer already exists — this overhead is significant. At inter-continental RTTs (250-300ms), the handshake alone can consume half the probe collection window (`probe_max_wait` = 500ms, [ADR 001](001-network.md#content-discovery-dht--probe)).

TLS 1.3 defines a **0-RTT** mode: after a successful 1-RTT handshake, the server issues a session ticket. On the next connection to that server, the client sends early data (application bytes) alongside the TLS ClientHello, eliminating the round-trip wait. The trade-off is that 0-RTT data is **replayable** — a network adversary can capture and resend the early-data packet, causing the server to process the same request twice. This is acceptable for idempotent, read-only operations but dangerous for state-changing ones.

iroh's `Endpoint` exposes `connect_with_0rtt()` for this purpose — the same mechanism used by [iroh-experiments/content-discovery](https://github.com/n0-computer/iroh-experiments/tree/main/content-discovery) for tracker queries and announcements.

This ADR defines which protocols are eligible for 0-RTT, the replay safety rationale for each, and session ticket management requirements.

## Decision

### 0-RTT Eligibility by ALPN

| ALPN | 0-RTT | Rationale |
| --- | --- | --- |
| `cdn/probe/v1` | **Yes** | `ProbeRequest` is read-only and idempotent. A replayed probe produces a duplicate `ProbeResponse` that the requester deduplicates by `NodeId` in the probe cache. No state change on the responder. |
| `cdn/client/v1` | **No** | `StreamRequest` initiates a payment relationship. Replay could cause duplicate byte delivery or voucher accounting confusion. Subsequent streams on an established connection already benefit from QUIC stream multiplexing (zero additional handshake cost). |
| `cdn/dht/v1` | **No** | The ALPN multiplexes `FindValueRequest` / `FindNodeRequest` (read-only, replay-safe) with `StoreRequest` (state-changing — refreshes the receiver's `(hash, holder)` record TTL per [ADR 022 §1.4](022-content-discovery.md#14-content-records-and-ttl)). QUIC's 0-RTT acceptance is negotiated at ALPN granularity, and per-message gating within an accepted ALPN is not idiomatic in iroh/quinn — the multiplexed ALPN therefore takes the safety floor of its least-safe message. The `FindValue` 1-RTT handshake cost is paid once per peer (connection reuse via stream multiplexing per [ADR 005 §Connection Management](005-protocol.md#connection-management)); steady-state cache-miss lookups mostly hit warm connections, especially as routing tables fill (k=20, hourly bucket refresh per [ADR 022 §1.3](022-content-discovery.md#13-routing-table)). If FIND_VALUE 0-RTT later proves load-bearing, the right evolution is a split ALPN (`cdn/dht-find/v1` 0-RTT-yes, `cdn/dht-store/v1` 0-RTT-no), not per-message gating within one ALPN. |

> External ALPNs (e.g. companion-protocol ALPNs documented in appendices) make their own 0-RTT decisions; they are out of scope here.

### Replay Safety Analysis

**`ProbeRequest` (safe):** Contains `{hash, timestamp_us}`. The `timestamp_us` field is requester-generated and echoed back in `ProbeResponse` for RTT measurement. A replayed probe causes the responder to evaluate `has_blob` and compute `rate_per_mb` a second time — both are pure reads with no side effects. The requester's probe cache deduplicates by `(hash, NodeId)`, so the duplicate response is discarded. The echoed `timestamp_us` from a replayed probe produces a stale RTT measurement, but since it arrives as a duplicate for an already-cached `NodeId`, it is never used.

Responders MUST NOT use `ProbeRequest` receipt to trigger any state change (e.g., cache priority boosting, demand-signal updates). If future protocol versions add such behavior, probe processing must be made replay-aware or 0-RTT eligibility must be revoked.

> **Implementation note:** Probe-receipt handling functions MUST be annotated as 0-RTT-safe (no side effects). If future implementations add demand-signal tracking or cache-priority boosting to probe handling, they MUST check the QUIC transport layer's early-data/replayed indicator before applying side effects.

**`StreamRequest` (unsafe):** Initiates paid byte delivery. Replay could cause a node to begin streaming bytes and expect voucher payment for a transfer the client did not request. Even if the node detects the duplicate `channel_id` + `byte_offset` combination, the window between replay receipt and detection creates accounting ambiguity.

**`StoreRequest` (unsafe):** Publishes a `(hash, holder)` content record at the receiver, with TTL up to 1 hour ([ADR 022 §1.4](022-content-discovery.md#14-content-records-and-ttl)). A captured 0-RTT `StoreRequest` packet can be replayed within the cached session-ticket lifetime (capped at 24h by §Session Ticket Management below). QUIC PSK resumption authenticates the replayed connection as the original holder `H`, so the receiver-side equality check `holder == authenticated NodeId` ([ADR 022 §1.5](022-content-discovery.md#15-store-flow-cache-event--dht-publish)) passes and the record TTL is refreshed. Each replay extends the record by up to one hour; an attacker who re-replays before each TTL expiry can sustain a stale advertisement for as long as the cached session ticket remains valid (up to ~24 hours past the holder's eviction). This is independent of whether `StoreRequest` carries an application-level signature: the captured bytes are constant, so signature replay and bytes replay are equivalent. `FindValueRequest` and `FindNodeRequest` are themselves replay-safe (read-only iterative lookups), but per the table above they share the ALPN with `StoreRequest` and inherit its 1-RTT requirement.

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
- **`cdn/dht/v1`:** Reject 0-RTT (do not configure `max_early_data_size`). Same default as `cdn/client/v1`.

### Impact on Probe Latency

At PoC scale, a cache miss sends `ProbeRequest` to the 3-5 NodeIds returned by DHT FIND_VALUE. When connections must be (re-)established — after the first probe cycle or after idle timeout closes them — each new connection costs 1 RTT before the probe is sent:

| Scenario | Probe send delay | Notes |
| --- | --- | --- |
| Cold (no ticket) | 1 RTT | Full TLS 1.3 handshake |
| Warm (cached ticket, 0-RTT accepted) | 0 RTT | Probe sent with ClientHello |
| Warm (cached ticket, 0-RTT rejected) | 1 RTT | Fallback to 1-RTT, re-send |

After the first probe cycles to a given peer, that connection has a cached ticket. Subsequent probes to the same peer send with zero handshake delay; the broader peer-table warmup happens incrementally as different content sets are probed. Combined with the adaptive early-exit mechanism ([ADR 001](001-network.md#content-discovery-dht--probe)), this reduces P50 cache-miss latency for reconnections by eliminating the handshake round trip — probes complete in one RTT (probe send + response) rather than two (handshake + probe), making `probe_min_wait` the dominant factor for nearby peers.

### Observability

Implementations SHOULD expose the following metrics, labeled by ALPN:

| Metric | Type | Description |
| --- | --- | --- |
| `quic_0rtt_attempts_total` | Counter | 0-RTT connection attempts |
| `quic_0rtt_accepted_total` | Counter | 0-RTT accepted by server |
| `quic_0rtt_rejected_total` | Counter | 0-RTT rejected, fell back to 1-RTT |
| `quic_session_ticket_cache_size` | Gauge | Current number of cached session tickets |
| `probe_collection_latency_seconds` | Histogram | Probe collection duration (labels: `0rtt=warm\|cold`) |

The `probe_collection_latency_seconds` histogram with the `0rtt` label enables operators to measure the real-world impact of 0-RTT on cache-miss latency and tune `probe_min_wait` accordingly.

## Consequences

- **Probe latency improves** for warm connections (all connections after the first cycle). The 1-RTT handshake cost is eliminated from the critical path.
- **No change to payment security.** `cdn/client/v1` remains 1-RTT-only. Payment channel setup and voucher exchange are never sent as early data.
- **Session ticket storage** adds a small memory footprint (~200 bytes per ticket x 1,000 max = ~200 KB).
- **Complexity cost** is modest: iroh's `connect_with_0rtt()` handles the transport-level details. The implementation burden is the ticket cache, the per-ALPN accept/reject configuration, and the fallback path.
- **Future protocol versions** that add state-changing behavior to `ProbeRequest` must re-evaluate 0-RTT eligibility. The replay safety analysis in this ADR is tied to the current message semantics.

## References

- [iroh-experiments/content-discovery](https://github.com/n0-computer/iroh-experiments/tree/main/content-discovery) — demonstrates 0-RTT for tracker queries over iroh QUIC
- [RFC 8446 Section 8](https://www.rfc-editor.org/rfc/rfc8446#section-8) — TLS 1.3 0-RTT and anti-replay
- [ADR 001 — Content Discovery](001-network.md#content-discovery-dht--probe)
- [ADR 005 — Connection Management](005-protocol.md#connection-management)
- [ADR 013 — Schema Evolution](013-schema-evolution.md) — protocol-enum framing rules
