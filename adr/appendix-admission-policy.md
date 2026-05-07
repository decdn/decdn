# Appendix: Node Admission and Priority Policy

> **This is an appendix, not a core protocol ADR.** Admission and queueing — how a node decides which incoming `StreamRequest`s to accept under congestion, and in what order — is a node-side scheduling concern, not a protocol invariant. The wire format carries no priority bits; the contract surface has no priority-aware entrypoints. This appendix documents one workable design using two signals already present in the protocol. Operators are free to adopt it, vary it, or implement an entirely different policy.

## Context

[ADR 003](003-payments.md) specifies the per-MB voucher payment mechanism but does not say how a node decides which `StreamRequest`s to admit when its concurrent-stream limit, bandwidth, or backend capacity is saturated. Admission policy is a node-side scheduling concern: different operators (small home node, CDN-scale, enterprise SLA tier) will tune it differently, and pinning a single policy as protocol-normative would either freeze a bad default or be ignored. The wire format carries no priority bits, the contract surface has no priority-aware entrypoints, and the design below is positioned as one workable implementation rather than as a binding spec.

The design uses two signals already present in the protocol:

- **Committed voucher rate.** [ADR 003](003-payments.md) verifies `amount_delta / bytes_delta >= rate_per_mb`, so the advertised rate is a floor and clients may commit at higher rates. The premium goes to the node as USDC revenue, aligning revenue with the prioritization decision.
- **Registered node-stake.** `StakingRegistry.stakeOf(address)` returns the registered stake of any operator — available as a binary eligibility signal for a higher-priority admission lane.

## Signal details

### Voucher rate (primary)

A node sorts admission by the committed per-MB rate of the incoming stream. Any voucher whose committed rate is at least `rate_per_mb` is acceptable; rates above the floor become the per-stream priority key. The premium is paid directly via `FeeRouter.routeSettlement`, so prioritizing a higher-bid stream produces matching node revenue.

### Registered node-stake (optional, eligibility flag)

`StakingRegistry.stakeOf(address)` returns the registered stake of an Ethereum address. A node may treat addresses with `stakeOf >= MIN_STAKE` (`MIN_STAKE = 50,000 TOKEN` per [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)) as eligible for a higher-priority admission lane. Two natural consumers:

- **Node-to-node cache-miss pulls** ([ADR 003](003-payments.md)): the requesting node is a registered staked operator, and prioritizing the pull improves cache fill rate across the mesh.
- **Clients who register as nodes** to opt into the priority lane — they bear the same 50K TOKEN minimum and 7-day unstake delay as serving operators, so the commitment is bonded rather than refundable-anytime.

The lookup uses the existing node-registration bookkeeping; no priority-specific contract surface or wire-level binding ceremony is required.

## Recommended admission policy

A simple two-lane scheduler that uses both signals:

```
on incoming StreamRequest:
    requester = recover_address_from_voucher_or_binding()
    if voucher.bytes_delta == 0:
        reject(ErrorCode::RateBelowFloor, hint: rate_per_mb_floor)
    committed_rate = voucher.amount_delta / voucher.bytes_delta

    if committed_rate < rate_per_mb_floor:
        reject(ErrorCode::RateBelowFloor, hint: rate_per_mb_floor)

    lane = if StakingRegistry.stakeOf(requester) >= MIN_STAKE { Priority } else { Regular }
    queue = admission_queue[lane]

    if queue.is_full():
        if committed_rate <= queue.lowest_committed_rate():
            reject(ErrorCode::Overloaded, hint: queue.lowest_committed_rate() + epsilon)
        else:
            // Admit; do NOT preempt already-admitted streams.
            // The Overloaded response simply tells future arrivals to bid higher.
            admit(StreamRequest)
    else:
        admit(StreamRequest)
```

Lane drain order: Priority lane first, Regular lane only when Priority is empty.
Within a lane, sort admitted streams by `committed_rate` descending (so a node with bandwidth headroom services the highest-bid in-lane stream first).

### Properties

- **Per-stream.** Each `StreamRequest` is admitted independently against its own committed rate. There is no per-session, per-connection, or per-client priority state.
- **Admission-time-only.** Priority is fixed at admission. Subsequent changes to `stakeOf` or to other inputs do not re-rank in-flight streams.
- **Non-preemptive.** Once a stream is admitted it runs to completion (or its own protocol-level timeout). A late-arriving higher-rate stream does not bump an admitted stream — instead it competes against future arrivals.
- **No waiting queue.** A `StreamRequest` is either admitted or rejected. There is no pending state in which a request waits indefinitely. This makes "starvation forever" impossible by construction; the worst case is repeated rejection, which is observable and recoverable client-side (retry elsewhere, or commit at a higher rate).
- **Reject-with-hint.** Rejections carry an actionable next step: either the rate floor (for under-floor commits) or the current admission floor in the relevant lane (for full-queue rejections). Clients can adjust and retry without guessing.
- **Per-lane fairness within bid.** Within a lane, the highest-committed-rate stream sits at the head. Equal-bid streams break ties by arrival order (FCFS within a price tier).
- **Address resolution.** Lane eligibility uses the address recovered from the voucher signature or `channel.client`. The optional ephemeral binding in `StreamRequest` ([ADR 005](005-protocol.md#client-identity-binding)) is for voucher attribution and is not a priority requirement.

## Per-client concurrent-stream cap (recommended)

Independent of the lane mechanism, nodes SHOULD apply a per-client concurrent-stream cap (e.g., 10 streams per Ethereum address simultaneously) to prevent a single wealthy client from monopolizing all admission slots. The cap is a node-policy parameter — different operators will pick different values based on their typical traffic profile. This cap is orthogonal to the per-ALPN concurrent-stream limit ([ADR 005](005-protocol.md)) and to any lane-level capacity.

## Variations operators may implement

This appendix is one workable design. Operators may:

- **Use voucher-rate alone** (no priority lane). Simplest.
- **Use a different lane eligibility check** — e.g., reputation-based, region-based, or a private allow-list for an enterprise SLA tier.
- **Apply rate-window aging** — bump effective priority of a stream that has been waiting at the floor rate for some time. Adds complexity but smooths starvation in heavy-load steady-state.
- **Operate without a lane at all** — admit FCFS at the rate floor, reject otherwise. Suitable for small home nodes without congestion.
- **Implement weighted-fair queueing across clients** — in environments where per-client fairness is more important than revenue maximization.

The protocol invariants are unaffected: the floor-rate semantics of `rate_per_mb`, the voucher signing format, and the channel-settlement flow are all defined in [ADR 003](003-payments.md). This appendix only documents one way to make scheduling decisions on top of those invariants.

## References

- [ADR 003](003-payments.md) — payment channels, voucher format, `rate_per_mb` floor semantics
- [ADR 005](005-protocol.md) — `StreamRequest` / `StreamResponse` / voucher wire format, per-ALPN concurrent-stream limits
- [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake) — `MIN_STAKE = 50,000 TOKEN`, 7-day unstake delay
