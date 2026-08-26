# Appendix: Permissionless Settlement Analysis

> **Status:** Retired 2026-08-26 under the stale design-ahead cleanup (#1845). The appendix described a permissionless off-chain settlement-analysis role: subscribing to the public `PaymentPool.PoolRedeemed` / `FeeRouter.Settled` events and looking for self-routing patterns. It specified no protocol surface — no wire interface, registration, or economics — and everything load-bearing in it lives elsewhere: there is no stale-close vector under the shared pool and node self-protection is the periodic redeem sweep inside the close grace window ([ADR 003 § Redemption and Close](../003-payments.md#redemption-and-close), implemented in the `decdn-node` runtime), while the structural wash-trading deterrent is the 40% non-base skim plus the per-operator vote cap ([ADR 036](../036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)). Anyone can still analyze the public events; a role description for doing so is not a design artifact. Original body preserved verbatim below for historical reference; do not link to from canonical ADRs.

> **This is an appendix, not a core protocol ADR.** It describes the operational role of running an off-chain analyzer of on-chain settlement flows — a permissionless side-effect of the pool's redemption events being public.

## Context

Under the shared payment pool ([ADR 003](003-payments.md#adr-003-payment-model)) there is **no stale-close vector**. Redemption is provider-only and final: a node redeems only its own `(signer, provider)` lane, and no party submits a competing value on its behalf. An owner's `closePool` only starts the redemption grace window — it moves no node's earnings and cannot understate a lane — and `reclaim` returns only the unspent remainder. So there is no on-chain surface where one party publishes a falsified value and benefits unless a third party disputes it, and therefore **no third-party dispute role**.

What remains permissionless is **off-chain analysis of public settlement flows**. The wash-trading defense is structural and lives elsewhere: per-byte payment (60% operator base) is paid by real clients from real pool deposits, and governance vote weight is sourced from `FeeRouter.bytesInWindow` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) (proven delivered bytes), so neither faked traffic nor over-declared capacity yields revenue or governance influence. There is no operator-asserted summary, no fraud-challenge mechanism, and no monitoring role for capacity-share. Permissionless analysis of on-chain redemption flows (operator-cluster / self-routing detection) is the soft layer informing governance threshold-tuning if persistent patterns surface.

## Role

Anyone with the technical capacity to subscribe to L2 RPC `eth_getLogs` for `PaymentPool.PoolRedeemed` and `FeeRouter.Settled` events can act as a settlement analyzer. No registration, registry, fees, or wire interface (see [Why this is an appendix](#why-this-is-an-appendix-not-an-adr)).

The analyzer correlates the two events from the same transaction — `PoolRedeemed(poolId, signer, provider, …)` and `Settled(operator, epoch, …)` — to recover the pool and lane context of each payout, then looks for self-routing patterns: a pool whose `owner` and redeeming `provider` are the same operator cluster, cycling USDC to inflate an operator's served bytes. The analysis recovers nothing from the protocol; its only output is evidence that governance may weigh when tuning the vote-cap and burn-share parameters.

This is fundamentally an informational layer, not a paid service or a protocol primitive.

## Node self-protection (not a third-party role)

A node protects its own earnings without any external actor:

1. **Periodic redeem sweep** ([ADR 003](003-payments.md#adr-003-payment-model)). The node redeems its outstanding vouchers on a fixed interval, capped well inside the grace window, so a pool that closes between sweeps is still swept before the owner can `reclaim`. It watches no close event — any interval short of the window suffices. Handles the common online case.
2. **Expiry margin.** A node stops serving a signer before the capability's `expiry`, so it always holds redeemable vouchers with time to redeem.

Because `redeem` is provider-only (`provider == msg.sender`), no third party could redeem a node's lane even if it wanted to — self-protection is the only path, and it is fully in the node's own hands. A node offline for the full grace window forfeits its unredeemed vouchers; this is a node-operations failure mode, not a protocol gap.

## Contract integration

One contract-level requirement, already specified in its owning ADR: **`PaymentPool` emits `PoolOpened`, `PoolRedeemed`, `PoolCloseInitiated`, and `PoolReclaimed`, and `FeeRouter` emits `Settled`** ([ADR 003](003-payments.md#adr-003-payment-model)). No `WatchtowerEscrow` contract, heartbeat protocol, per-pool registration, or `cdn/watchtower/v1` ALPN.

## Privacy

The analyzer consumes only public on-chain data — redemption and settlement events. No node-shared voucher state, no privileged access. Privacy considerations from [ADR 017](017-privacy.md#adr-017-privacy-analysis) apply to pool parties, not analyzers.

## Why this is an appendix, not an ADR

A protocol decision establishes a participant role with a defined wire interface, on-chain registration, fee/payment economics, or off-chain coordination protocol. The settlement-analysis layer has none — it is a permissionless side-effect of public redemption events from [ADR 003](003-payments.md#adr-003-payment-model), like [Observability](appendix-observability.md#appendix-observability-and-metrics): operationally relevant, but not a protocol primitive.

## Cross-ADR Impact

- [ADR 003 — Payments](003-payments.md#adr-003-payment-model) — redemption and grace-window close, the periodic redeem sweep, self-routing skim
- [ADR 036 — Served-Bytes Voting Weight](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) — the wash-trading-as-vote-buying cost model the analysis informs
- [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target) — gas-cost context
