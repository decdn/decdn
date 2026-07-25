# ADR 015: QUIC 0-RTT Connection Establishment

> **Status:** Retired 2026-07-25 under the protocol simplification audit. QUIC 0-RTT is not used: every connection, `cdn/probe/v1` included, completes a full handshake before application bytes flow. The mechanism removed exactly one round trip, and only on a warm reconnection to an already-probed peer, in exchange for a replay-safety surface that no server-side gate could enforce — iroh sets `max_early_data_size = u32::MAX` on every server TLS config, so the invariant was "exactly one early-data-emitting client path, hard-wired to the idempotent probe ALPN", which any future side effect on the probe path would have silently voided. Session resumption for full handshakes is retained (rustls skips the handshake's asymmetric crypto), as is stream multiplexing on open connections per [ADR 005 § Connection Management](../005-protocol.md#connection-management). The `network.enable_0rtt` switch, the `decdn_quic_0rtt_*` / `decdn_quic_session_ticket_*` metrics, and the ticket-cache sizing are gone with it. Original ADR body preserved verbatim below for historical reference; do not link to from canonical ADRs.

**Date:** 2026-04-04
**Status (pre-retirement):** Draft

## Context

Every QUIC connection begins with a TLS 1.3 handshake that costs one round trip (1-RTT) before application data flows. Probing is the critical latency path: a cache miss runs DHT FIND_VALUE for 3-5 candidate NodeIds, then sends `ProbeRequest` to each in parallel. Each probe needs a separate QUIC connection when no connection to that peer exists. At inter-continental RTTs (250-300ms), the handshake alone can consume half the 500ms probe collection window ([ADR 001](../001-network.md#content-discovery-dht--probe)).

TLS 1.3 defines a **0-RTT** mode. After a successful 1-RTT handshake, the server issues a session ticket. On the next connection to that server, the client sends early data (application bytes) with the TLS ClientHello, removing the round-trip wait. The trade-off is that 0-RTT data is **replayable**: a network adversary can capture and resend the early-data packet, making the server process the same request twice. This is acceptable for idempotent, read-only operations but dangerous for state-changing ones.

iroh exposes 0-RTT through `Connecting::into_0rtt()` (client) and `Accepting::into_0rtt()` (server, via a `ProtocolHandler::on_accepting` override) — the same mechanism [iroh-experiments/content-discovery](https://github.com/n0-computer/iroh-experiments/tree/main/content-discovery) uses for tracker queries and announcements.

This ADR defines which protocols are eligible for 0-RTT, the replay-safety rationale for each, and session-ticket management requirements.

## Decision

### 0-RTT Eligibility by ALPN

| ALPN | 0-RTT | Rationale |
| --- | --- | --- |
| `cdn/probe/v1` | **Yes** | `ProbeRequest` is read-only and idempotent. A replayed probe produces a duplicate `ProbeResponse` that the requester deduplicates by `NodeId` in the probe cache. No state change on the responder. |
| `cdn/client/v1` | **No** | `StreamRequest` initiates a payment relationship. Replay could cause duplicate byte delivery or voucher accounting confusion. Subsequent streams on an established connection already benefit from QUIC stream multiplexing (zero additional handshake cost). |
| `cdn/dht/v1` | **No** | The ALPN multiplexes `FindValueRequest` / `FindNodeRequest` (read-only, replay-safe) with `StoreRequest` (state-changing — refreshes the receiver's `(hash, holder)` record TTL per [ADR 022 § Content Records and TTL](../022-content-discovery.md#content-records-and-ttl)). QUIC's 0-RTT acceptance is negotiated at ALPN granularity, and per-message gating within an accepted ALPN is not idiomatic in iroh/quinn — the multiplexed ALPN therefore takes the safety floor of its least-safe message. The `FindValue` 1-RTT handshake cost is paid once per peer (connection reuse via stream multiplexing per [ADR 005 § Connection Management](../005-protocol.md#connection-management)); steady-state cache-miss lookups mostly hit warm connections, especially as routing tables fill (k=20, hourly bucket refresh per [ADR 022 § Routing Table](../022-content-discovery.md#routing-table)). If FIND_VALUE 0-RTT later proves load-bearing, the right evolution is a split ALPN (`cdn/dht-find/v1` 0-RTT-yes, `cdn/dht-store/v1` 0-RTT-no), not per-message gating within one ALPN. |

> External ALPNs (e.g. companion-protocol ALPNs documented in appendices) make their own 0-RTT decisions; they are out of scope here.

### Replay Safety Analysis

**`ProbeRequest` (safe):** Contains `{hash, timestamp_us}`. `timestamp_us` is requester-generated and echoed back in `ProbeResponse` for RTT measurement. A replayed probe makes the responder evaluate `has_blob` and compute `rate_per_mb` a second time — both are pure reads with no side effects. The requester's probe cache deduplicates by `(hash, NodeId)`, so the duplicate response is discarded. The echoed `timestamp_us` from a replay gives a stale RTT measurement, but it arrives as a duplicate for an already-cached `NodeId` and is never used.

Responders MUST NOT use `ProbeRequest` receipt to trigger any state change (e.g. cache priority boosting, demand-signal updates). If future protocol versions add such behavior, probe processing must be made replay-aware or 0-RTT eligibility must be revoked.

> **Implementation note:** Probe-receipt handling functions MUST be annotated as 0-RTT-safe (no side effects). If future implementations add demand-signal tracking or cache-priority boosting to probe handling, they MUST check the QUIC transport layer's early-data/replayed indicator before applying side effects.

**`StreamRequest` (unsafe):** Initiates paid byte delivery. Replay could make a node begin streaming bytes and expect voucher payment for a transfer the client did not request. Even if the node detects the duplicate `channel_id` + `byte_offset` combination, the window between replay receipt and detection creates accounting ambiguity.

**`StoreRequest` (unsafe):** Publishes a `(hash, holder)` content record at the receiver, with TTL up to 1 hour ([ADR 022 § Content Records and TTL](../022-content-discovery.md#content-records-and-ttl)). A captured 0-RTT `StoreRequest` packet can be replayed for as long as the server still accepts the resumption ticket — bounded by the server-side acceptance window, which [§ Session Ticket Management](#session-ticket-management) caps at 24 hours. QUIC PSK resumption authenticates the replayed connection as the original holder `H`, so the receiver-side equality check `holder == authenticated NodeId` ([ADR 022 § STORE Flow (Cache Event → DHT Publish)](../022-content-discovery.md#store-flow-cache-event--dht-publish)) passes and the record TTL is refreshed. Each replay extends the record by up to one hour; an attacker who re-replays before each TTL expiry can sustain a stale advertisement for as long as the cached session ticket remains valid (up to ~24 hours past the holder's eviction). This is independent of whether `StoreRequest` carries an application-level signature: the captured bytes are constant, so signature replay and bytes replay are equivalent. `FindValueRequest` and `FindNodeRequest` are themselves replay-safe (read-only iterative lookups), but per the table above they share the ALPN with `StoreRequest` and inherit its 1-RTT requirement.

### Session Ticket Management

Session-ticket storage is **delegated to iroh's internal TLS session cache**, not a hand-rolled cache. iroh wires `rustls::client::ClientSessionMemoryCache` into every endpoint's client config (`iroh::tls`); the cache is an LRU keyed solely by the TLS server name, which for iroh's raw-public-key TLS is the remote endpoint id — i.e. a per-`remote_node_id` LRU, **not** partitioned by ALPN. ALPN isolation applies one level up: per RFC 8446 §4.2.10, rustls only *accepts 0-RTT early data* when the resuming connection's ALPN matches the ticket's, so early data sent under one ALPN's ticket is rejected if replayed against a different ALPN — but the 1-RTT session resumption itself is not ALPN-scoped. The implementation therefore MUST:

1. **Size the client ticket cache to 1,000 entries** via `Endpoint::builder().max_tls_tickets(1000)` (iroh's default is 256). This knob sizes only the *client-side* `ClientSessionMemoryCache` — the tickets a node caches for servers it probes. The *serving* side's resumption store is rustls-internal at rustls's own default and is **not** sized by this knob; that is acceptable because the serving side only needs enough state to honor recent resumptions and is not the latency-critical path. At PoC scale (~30 nodes x 1 eligible ALPN) the client cache never approaches 1,000; LRU eviction beyond it is handled by rustls.
2. **Linger briefly before closing** so the server's NewSessionTicket frame arrives and rustls can store it. After the application exchange completes, wait (bounded: minimum 100ms, target 2x the measured RTT, maximum 2 seconds) on the connection's close future before closing. iroh/rustls ingest and store the ticket transparently; no application-level ticket handling is required.
3. **Ticket expiry is bounded by rustls and the server.** rustls honors the `ticket_lifetime` advertised by the server and will not resume with an expired ticket. Servers SHOULD advertise `ticket_lifetime` ≤ 24 hours (RFC 8446 caps it at 7 days). A separate client-side 24-hour cap is **not** independently enforced — iroh exposes no hook into the rustls session store for age-based eviction — so the server's advertised lifetime is the binding bound on the 0-RTT replay window. (Recorded under Consequences as a known deviation from defense-in-depth.)

### 0-RTT Rejection Handling

Servers MAY reject 0-RTT at any time (e.g. after key rotation, ticket expiry, or deliberate policy). On rejection:

1. The QUIC handshake completes as a normal 1-RTT connection.
2. The client MUST re-send the request on a confirmed (post-handshake) stream.
3. The client caches the new session ticket for subsequent attempts.

Implementations SHOULD NOT treat 0-RTT rejection as an error — it is a normal part of the protocol. The rejection path is functionally equivalent to a cold connection; the only cost is one wasted round trip of early data.

### Replay Safety Is Client-Side, Not Server-Gated

> **Implementation reality (iroh 0.98.x).** 0-RTT is **not** gated *server-side per
> handler*: it is not the case that only an `on_accepting` override exposes early data
> while the default handler makes the QUIC stack drop it. iroh sets
> `crypto.max_early_data_size = u32::MAX` on *every* server TLS config
> (`iroh::tls::make_server_config`), so the TLS/QUIC layer accepts 0-RTT early data
> for **any** ALPN regardless of the handler. `Accepting::into_0rtt()` only changes
> *when* the application reads the early-data streams (before vs. after handshake
> completion); it does **not** decide whether 0-RTT is accepted. A handler that keeps
> the default `accepting.await` still has the client's 0-RTT accepted and still
> processes the early data (post-handshake). This is pinned by the characterization
> test `default_on_accepting_still_accepts_0rtt_safety_is_client_side`.

The replay-safety guarantee is therefore **enforced on the client side**. The only code path that calls `Connecting::into_0rtt()` / transmits early data is the probe client (`probe_once`), hard-wired to `cdn/probe/v1`. No `cdn/client/v1` or `cdn/dht/v1` client sends early data, so a state-changing `StreamRequest` / `StoreRequest` is never transmitted as replayable 0-RTT — exactly the property [§ Replay Safety Analysis](#replay-safety-analysis) requires. The load-bearing invariant is "exactly one 0-RTT-emitting client path, hard-wired to the idempotent probe ALPN", not any server-side per-handler gate.

Server-side, the probe handler still overrides `on_accepting` (to read the probe as true 0-RTT pre-handshake rather than post-handshake) and other handlers keep the default. This is a latency/structuring choice, **not** the safety boundary:

- **`cdn/probe/v1`:** `on_accepting` override → early-data `ProbeRequest` read pre-handshake. Replay-safe because the request is idempotent and read-only.
- **`cdn/client/v1` / `cdn/dht/v1`:** default `on_accepting`. The TLS layer would still accept 0-RTT, but no client sends any, so none is processed.

If a future non-probe client is ever made to attempt 0-RTT, this client-side invariant breaks and a real server-side barrier (or removing the global `max_early_data_size`) would be required first — see Consequences.

A node-wide master switch, `network.enable_0rtt` (default `true`), is the operational kill switch. Its effective mechanism is **client-side**: when `false`, `probe_once` takes the plain `connect().await` path and transmits no early data, so the connection is genuinely 1-RTT. Setting it `false` also makes the probe handler keep the default `on_accepting` (so it stops reading early data pre-handshake and stops feeding the session-ticket gauge), but on its own that server-side change does not reject 0-RTT — the client not sending early data is what does.

### Impact on Probe Latency

At PoC scale, a cache miss sends `ProbeRequest` to the 3-5 NodeIds returned by DHT FIND_VALUE. When connections must be (re-)established — after the first probe cycle or after idle timeout closes them — each new connection costs 1 RTT before the probe is sent:

| Scenario | Probe send delay | Notes |
| --- | --- | --- |
| Cold (no ticket) | 1 RTT | Full TLS 1.3 handshake |
| Warm (cached ticket, 0-RTT accepted) | 0 RTT | Probe sent with ClientHello |
| Warm (cached ticket, 0-RTT rejected) | 1 RTT | Fallback to 1-RTT, re-send |

After the first probe cycles to a given peer, that connection has a cached ticket. Subsequent probes to the same peer send with zero handshake delay; the broader peer-table warmup happens incrementally as different content sets are probed. This reduces P50 cache-miss latency for reconnections by eliminating the handshake round trip — probes complete in one RTT (probe send + response) rather than two (handshake + probe), so nearby peers answer well inside the probe collection window ([ADR 001](../001-network.md#content-discovery-dht--probe)).

### Observability

Implementations SHOULD expose the following metrics. They live in the `decdn_*` metric family (per [Appendix: Observability](../appendix-observability.md#appendix-observability-and-metrics) / the node metrics registry); the OpenMetrics encoder appends `_total` to counters. No ALPN label is applied — only one ALPN is 0-RTT-eligible, so the label would be constant, and the metrics house style is label-free:

| Metric | Type | Description |
| --- | --- | --- |
| `decdn_quic_0rtt_attempts_total` | Counter | 0-RTT connection attempts (a cached ticket existed and early data was sent) |
| `decdn_quic_0rtt_accepted_total` | Counter | 0-RTT accepted by server |
| `decdn_quic_0rtt_rejected_total` | Counter | 0-RTT rejected, fell back to 1-RTT |
| `decdn_quic_session_ticket_cache_size` | Gauge | Approximate 0-RTT working-set proxy (see note) |
| `decdn_quic_session_ticket_peers_dropped_total` | Counter | Distinct new peers dropped because the tracking set hit its memory-safety ceiling (Sybil-saturation signal) |

`decdn_quic_session_ticket_cache_size` is an **approximation / proxy**, not a read of any rustls cache: rustls owns the session stores and exposes no size API, and the server-side store these handshakes populate is rustls-internal at rustls's own default (iroh's `max_tls_tickets` sizes only the client store). The node instead tracks the number of distinct remote endpoints with which it completed a 0-RTT-eligible *server* handshake, in a set bounded for memory safety at the same 1,000-entry ceiling the client cache uses. At PoC scale the working set is far below that ceiling, so the proxy tracks the real working set closely; once the bound engages, `decdn_quic_session_ticket_peers_dropped_total` rises so saturation is distinguishable from organic growth.

`probe_collection_latency_seconds` (a histogram labeled `0rtt=warm|cold`) is **deferred**: it measures the node's cache-miss probe-collection loop, which does not exist yet (no DHT→probe candidate path is implemented). It is recorded as a follow-up under Consequences and SHOULD be added when that loop lands.

## Consequences

- **Probe latency improves** for warm connections (all connections after the first cycle). The 1-RTT handshake cost is eliminated from the critical path.
- **No change to payment security** — but the mechanism is client-side. `cdn/client/v1` / `cdn/dht/v1` are never sent as 0-RTT because no client emits early data for them, **not** because the server rejects it (iroh's global `max_early_data_size` means the server-side TLS would accept 0-RTT for any ALPN). Safety rests on there being exactly one 0-RTT-emitting client path, hard-wired to the idempotent probe ALPN.
- **Defense-in-depth gap (accepted at PoC scale):** there is no server-side barrier. A future `cdn/client/v1` / `cdn/dht/v1` client that mistakenly called `into_0rtt()` would have its state-changing request accepted and replayably processed by the server. Mitigation if that risk materializes: drop the unconditional global `max_early_data_size` (needs an iroh change or fork), or have non-probe handlers detect and reset 0-RTT-opened streams (`RecvStream::is_0rtt`). Tracked as a follow-up; not required while the single hard-wired probe client is the only 0-RTT emitter.
- **Session ticket storage** adds a small memory footprint (~200 bytes per ticket x 1,000 max = ~200 KB), owned by rustls inside iroh.
- **Complexity cost** is modest: iroh's `Connecting::into_0rtt()` / `Accepting::into_0rtt()` handle the transport-level details and the ticket store. The implementation burden is the per-handler `on_accepting` override, the client attempt/fallback path, the `max_tls_tickets` sizing, and the metrics.
- **Future protocol versions** that add state-changing behavior to `ProbeRequest` must re-evaluate 0-RTT eligibility. The replay safety analysis in this ADR is tied to the current message semantics.
- **Known deviation (defense-in-depth):** an independent client-side 24-hour ticket-age cap is not enforced — iroh exposes no hook into the rustls session store — so the server-advertised `ticket_lifetime` is the sole bound on the 0-RTT replay window. Acceptable at PoC scale; revisit if iroh exposes session-store control.
- **Follow-up:** `decdn_quic_session_ticket_cache_size` is an approximation (rustls has no size API), and the `probe_collection_latency_seconds` histogram is deferred until the node's DHT→probe cache-miss loop exists. The 0-RTT mechanism is implemented as a reusable probe-client helper so that loop can adopt it without rework.

## References

- [iroh-experiments/content-discovery](https://github.com/n0-computer/iroh-experiments/tree/main/content-discovery) — demonstrates 0-RTT for tracker queries over iroh QUIC
- [RFC 8446 Section 8](https://www.rfc-editor.org/rfc/rfc8446#section-8) — TLS 1.3 0-RTT and anti-replay
- [ADR 001 — Content Discovery](../001-network.md#content-discovery-dht--probe)
- [ADR 005 — Connection Management](../005-protocol.md#connection-management)
- [ADR 013 — Schema Evolution](../013-schema-evolution.md#adr-013-schema-evolution) — protocol-enum framing rules
