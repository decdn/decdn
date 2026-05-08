# Appendix: Permissionless Stale-Close Detection

> **This is an appendix, not a core protocol ADR.** It describes the operational role of running a stale-close detector — a permissionless side-effect of `closeChannel` / `disputeChannel` access being open to any voucher holder.

## Context

The protocol has one on-chain surface where one party can publish a falsified value and benefit if no third party objects within a bounded window:

**`closeChannel` / `disputeChannel`** ([ADR 003](003-payments.md)). A client may close a channel with a stale (low-nonce) voucher; the dispute window is 48 hours; if no one submits a higher-nonce voucher in that window, settlement uses the stale value.

`disputeChannel` is permissionless from the protocol's side: any address holding a higher-nonce voucher signed by the same channel funder can submit it. This appendix describes the operational role of running such monitoring as a third party.

Note: distinct-client diversity gating ([ADR 027](027-distinct-client-receipts.md)) is enforced inline in `FeeRouter.routeSettlement` from settled-voucher state. There is no operator-asserted summary, no fraud-challenge mechanism, and therefore no monitoring role for diversity gating — the contract computes the count directly. The wash-trading defense is the [ADR 026 §3 per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap), also enforced by the contract without external attestation.

## Role

Anyone with the technical capacity to:

- subscribe to L2 RPC `eth_getLogs` for `ChannelCloseInitiated` events, and
- hold a hot wallet with enough gas to submit `disputeChannel`,

can act as a stale-close detector. The role has no on-chain registration, no off-chain registry, no fees from monitored parties, and no protocol-level wire interface.

When `ChannelCloseInitiated(channelId, ..., nonce)` fires, the detector checks whether it holds a voucher with a higher nonce for that channel. If so, it submits `disputeChannel`.

- A successful dispute updates on-chain `claimedAmount` / `claimedNonce` / `claimedBytes`.
- The detector spends gas (~$0.05–$0.10 on the production L2 — see [Appendix: L2 Deployment](appendix-l2-deployment.md)).
- The detector recovers nothing from the protocol — they must have an out-of-band reason to hold a higher voucher (i.e., they are the node operator, the operator's hot-spare infrastructure, or a counterparty).

This is fundamentally a self-protection mechanism, not a paid service. **Nodes that want offline-window protection arrange their own redundancy** — multiple node instances sharing voucher state, hot-standby relays, or peer agreements to relay vouchers. The protocol does not define a wire format for that arrangement; it is a node-operations responsibility, comparable to running redundant origin backends. ADR 003's local in-process dispute monitor handles the online-node case automatically; the offline-for-the-full-48h case is acknowledged as an operational failure mode rather than a protocol gap.

## Contract integration

The role depends on two contract-level requirements, already specified in their owning ADRs:

1. **`disputeChannel` accepts submissions from any address** — the voucher's EIP-712 signature is the sole authorization (`ecrecover(signature) == channel.client`), no `msg.sender` access check ([ADR 003 §closeChannel/disputeChannel](003-payments.md)).
2. **`ChannelCloseInitiated`, `ChannelDisputed`, `ChannelSettled` events are emitted** ([ADR 003](003-payments.md)).

No `WatchtowerEscrow` contract, no heartbeat protocol, no per-channel registration, no `cdn/watchtower/v1` ALPN.

## Defense in depth (stale-close)

Stale-close protection has three layers, with the first as the primary mechanism:

1. **Local in-process dispute monitor** ([ADR 003](003-payments.md) Option C). A lightweight thread in the node binary that watches the chain for `ChannelCloseInitiated` events on its channels and auto-submits the latest voucher. Handles the common case where the node is online.
2. **Operator-arranged redundancy.** Multi-instance node deployments, hot-standby relays, or peer agreements to hold latest vouchers. Out of protocol scope; node-operations responsibility.
3. **Dispute window** (48h PoC default, governable 12h–72h per [ADR 009](009-governance.md)). Provides the time budget for layers 1 and 2 to respond. The forced-inclusion deadline extension in the payment-channel contract preserves an effective response window even under L2 sequencer censorship — see [ADR 003](003-payments.md).

## Privacy

The detector role consumes only public on-chain data — channel close events. No node-shared voucher state, no privileged access. Privacy considerations from [ADR 017](017-privacy.md) apply to channel parties, not detectors.

## Why this is an appendix, not an ADR

A protocol decision establishes a participant role with a defined wire interface, on-chain registration, fee/payment economics, or off-chain coordination protocol. The fraud-detection layer has none of these — it is a permissionless side-effect of `disputeChannel` access from [ADR 003](003-payments.md). Documenting it as an appendix matches the role of [Encrypted Content Publishing](appendix-encrypted-content-publishing.md) or [Observability](appendix-observability.md): operationally relevant, but not a protocol primitive.

## Cross-references

- [ADR 003 — Payments](003-payments.md) — `closeChannel` / `disputeChannel` flow, dispute window, local-monitor Option C, forced-inclusion deadline extension
- [ADR 014 — On-chain Verification](014-on-chain-verification.md) — `SlashJudge` challenge-bond mechanism (Bond Handling) for the three signature-dependent offenses
- [ADR 027 — Distinct-Client Diversity Gating](027-distinct-client-receipts.md) — diversity gating computed inline by `FeeRouter`; no external attestation surface
- [Appendix: L2 Deployment](appendix-l2-deployment.md) — gas-cost context for detector economics
