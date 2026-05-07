# Appendix: Node Admission and Priority Policy

> **This is an appendix, not a core protocol ADR.** Admission and queueing — how a node decides which incoming `StreamRequest`s to accept under congestion, and in what order — is a node-side scheduling concern, not a protocol invariant. The wire format carries no priority bits; the contract surface has no priority-aware entrypoints. This appendix documents one workable design using two signals already present in the protocol. Operators are free to adopt it, vary it, or implement an entirely different policy.

## Context

[ADR 003](003-payments.md) specifies the per-MB voucher payment mechanism but does not say how a node decides which `StreamRequest` to admit when its concurrent-stream limit, bandwidth, or backend capacity is saturated. Earlier drafts of ADR 003 included a `clientStake` / `clientStakeOf` mechanism — a refundable TOKEN deposit a client could make to be prioritized during congestion. That mechanism was removed (see #402) for three structural weaknesses:

1. Withdraw-anytime semantics meant the cost of priority was just opportunity cost of holding TOKEN — sophisticated clients could cycle a single deposit across many nodes.
2. The signal was misaligned with revenue: a node prioritizing a high-staker earned the same as serving a low-staker.
3. It added contract surface (three entrypoints, a storage map, two events) and a wire-level binding ceremony for marginal value.

The replacement is to use signals already present in the protocol — committed voucher rate and (optionally) registered node-stake — as inputs to a node-side policy, leaving the policy itself out of the protocol so different operators can tune it differently.

## Two signals already in the protocol

### Voucher rate (primary)

[ADR 003](003-payments.md) verifies `amount_delta / bytes_delta >= rate_per_mb` — the advertised `rate_per_mb` is a **floor**, not equality. A client may sign vouchers committing to a higher per-MB rate than the node advertised. Nodes that do this:

- Accept any voucher whose committed rate is at least `rate_per_mb`.
- Treat the committed rate as a per-stream priority key.
- Earn the premium directly via `FeeRouter.routeSettlement`, aligning revenue with the prioritization decision.

No protocol change is required — this is already valid wire behaviour. The cost of priority becomes a real per-MB premium paid in USDC, not a refundable TOKEN deposit.

### Registered node-stake (optional, eligibility flag)

`StakingRegistry.stakeOf(address)` returns the registered node-stake of an Ethereum address. A node may treat addresses with `stakeOf >= MIN_STAKE` (`MIN_STAKE = 50,000 TOKEN` per [ADR 026 §7](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake)) as eligible for a higher-priority admission lane. Two natural consumers:

- **Node-to-node cache-miss pulls** ([ADR 003](003-payments.md)): the requesting node is a registered staked operator, and prioritizing the pull improves cache fill rate across the mesh.
- **Clients who register as nodes** to opt into the priority lane — they bear the same 50K TOKEN minimum and 7-day unstake delay as serving operators, so the commitment is real (not refundable-anytime).

The `stakeOf` lookup uses the existing node-registration bookkeeping. No new contract surface, no client-specific staking mechanism, no ephemeral binding ceremony for the lookup.

## Recommended admission policy

A simple two-lane scheduler that uses both signals:

```
on incoming StreamRequest:
    requester = recover_address_from_voucher_or_binding()
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

- **Non-preemptive.** Once a stream is admitted it runs to completion (or its own protocol-level timeout). A late-arriving higher-rate stream does not bump an admitted stream — instead it competes against future arrivals.
- **No waiting queue.** A `StreamRequest` is either admitted or rejected. There is no pending state in which a request waits indefinitely. This makes "starvation forever" impossible by construction; the worst case is repeated rejection, which is observable and recoverable client-side (retry elsewhere, or commit at a higher rate).
- **Reject-with-hint.** Rejections carry an actionable next step: either the rate floor (for under-floor commits) or the current admission floor in the relevant lane (for full-queue rejections). Clients can adjust and retry without guessing.
- **Per-lane fairness within bid.** Within a lane, the highest-committed-rate stream sits at the head. Equal-bid streams break ties by arrival order (FCFS within a price tier).

## Per-client concurrent-stream cap (recommended)

Independent of the lane mechanism, nodes SHOULD apply a per-client concurrent-stream cap (e.g., 10 streams per Ethereum address simultaneously) to prevent a single wealthy client from monopolizing all admission slots. The cap is a node-policy parameter — different operators will pick different values based on their typical traffic profile. This cap is orthogonal to the per-ALPN concurrent-stream limit ([ADR 005](005-protocol.md)) and to any lane-level capacity.

## How this resolves the prior #402 questions

The four edge cases in the original `clientStake`-based design dissolve under voucher-rate priority:

| Original question | Resolution |
| --- | --- |
| Mid-session stake change → re-evaluate when? | N/A — no per-stream stake state. Voucher rate is committed at admission and fixed for the stream. |
| In-flight on `clientUnstake` | N/A — no `clientUnstake`. Already-admitted streams complete normally regardless of subsequent stake changes. |
| Priority granularity (stream / session / connection) | Per-stream — each `StreamRequest` is admitted independently against its own committed rate. |
| Ephemeral binding eligibility for priority | N/A — priority lookup uses the address recovered from the voucher signature or `channel.client`; the optional ephemeral binding in `StreamRequest` ([ADR 005](005-protocol.md#client-identity-binding)) is for voucher attribution before the first voucher is signed, not a priority requirement. |

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
- Issue #402 — original ADR-gap discussion, closed as decided-against
