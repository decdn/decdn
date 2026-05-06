# Appendix: Permissionless Fraud-Detection Layer

> **This is an appendix, not a core protocol ADR.** Earlier drafts specified a "watchtower" as a protocol participant with a dedicated ALPN (`cdn/watchtower/v1`), an on-chain `WatchtowerEscrow` contract, a heartbeat-based liveness mechanism, and per-channel subscription fees. With [ADR 027](027-distinct-client-receipts.md)'s permissionless-challenger model and the existing `SlashJudge` bond machinery from [ADR 014](014-on-chain-verification.md), no dedicated subscription role, on-chain registry, or wire protocol is needed: anyone can monitor the L2 chain for fraudulent settlements or epoch summaries and earn the bond reward by submitting a successful challenge. This appendix documents the role as it now exists — an optional, permissionless layer that anyone may run, parallel to the "anyone can call `settleChannel`" pattern in [ADR 003](003-payments.md).

## Context

The protocol has two on-chain surfaces where one party can publish a falsified value and benefit if no third party objects within a bounded window:

1. **`closeChannel` / `disputeChannel`** ([ADR 003](003-payments.md)). A client may close a channel with a stale (low-nonce) voucher; the dispute window is 48 hours; if no one submits a higher-nonce voucher in that window, settlement uses the stale value.
2. **`commitEpochSummary`** ([ADR 027](027-distinct-client-receipts.md)). An operator may commit an `EpochReceiptSummary` claiming an inflated `claimedDistinctClients`; the challenge window is 7 days; if no one submits a `ChallengeReceiptSummary` in that window, gauge-pool eligibility uses the inflated count.

Both submission paths are permissionless from the protocol's side: any address holding contradicting evidence can submit. This appendix describes the operational role of running such monitoring as a third party.

## Role

Anyone with the technical capacity to:

- subscribe to L2 RPC `eth_getLogs` for `ChannelCloseInitiated` and per-operator `EpochReceiptSummary` commits, and
- hold a hot wallet with enough gas + bond capital,

can act as a fraud-detector. The role has no on-chain registration, no off-chain registry, no fees from monitored parties, and no protocol-level wire interface.

### Stale-close detection

When `ChannelCloseInitiated(channelId, ..., nonce)` fires, the detector checks whether it holds a voucher with a higher nonce for that channel. If so, it submits `disputeChannel`.

- A successful dispute updates on-chain `claimedAmount` / `claimedNonce` / `claimedBytes`.
- The detector spends gas (~$0.05–$0.10 on the production L2 — see [Appendix: L2 Deployment](appendix-l2-deployment.md)).
- The detector recovers nothing from the protocol — they must have an out-of-band reason to hold a higher voucher (i.e., they are the node operator, the operator's hot-spare infrastructure, or a counterparty).

This is fundamentally a self-protection mechanism, not a paid service. **Nodes that want offline-window protection arrange their own redundancy** — multiple node instances sharing voucher state, hot-standby relays, or peer agreements to relay vouchers. The protocol does not define a wire format for that arrangement; it is a node-operations responsibility, comparable to running redundant origin backends. ADR 003's local in-process dispute monitor handles the online-node case automatically; the offline-for-the-full-48h case is acknowledged as an operational failure mode rather than a protocol gap.

### Receipt-summary fraud detection

When an operator commits an `EpochReceiptSummary` via `FeeRouter.commitEpochSummary`, a detector that wishes to challenge ingests the operator's per-epoch settlements (already on-chain via each `routeSettlement`'s `receiptBatchRoot` argument) and pulls the underlying receipt batches from the operator's published surface. It then runs the [ADR 027 §3](027-distinct-client-receipts.md) checks:

- Signature recovers to `clientPubKey == channel.client` (cryptographic, definitive).
- Identity-diversity rules (funded-channel minimum, funding-age, per-operator cooldown, funding-source diversity).
- Heuristic flags for self-routed traffic patterns (operator-as-client overlap, funder clustering, settlement-cadence anomalies).

If any check fails, the detector submits `ChallengeReceiptSummary` with the standard bond ([ADR 014 Bond Handling](014-on-chain-verification.md)). On a successful challenge, the operator's `claimedDistinctClients` is zeroed for the epoch and the detector receives back its bond plus a challenger reward sourced from the forfeited gauge payout (size governed per [ADR 027 §7](027-distinct-client-receipts.md)). On a dismissed challenge, the bond is forfeit per the standard 50% burn / 50% to operator rule ([ADR 014](014-on-chain-verification.md)).

### Heuristics are a tool, not a protocol input

Pattern heuristics — operator-as-client overlap, funder clustering, settlement-cadence anomalies, contiguous-EOA "address generator" patterns — are useful for choosing which `EpochReceiptSummary` commitments to investigate. They are **not** themselves slashable, and the on-chain dispute always resolves on cryptographic evidence (signature validity, channel-funding ancestry traces). A detector that publishes flag indicators publicly contributes to a public good; the protocol consumes only the on-chain bonded challenge.

## Contract integration

The new model preserves three contract-level requirements that the original watchtower design also depended on. These are the only protocol-level shims required and are already specified in their owning ADRs:

1. **`disputeChannel` accepts submissions from any address** — the voucher's EIP-712 signature is the sole authorization (`ecrecover(signature) == channel.client`), no `msg.sender` access check ([ADR 003 §closeChannel/disputeChannel](003-payments.md)).
2. **`ChannelCloseInitiated`, `ChannelDisputed`, `ChannelSettled` events are emitted** ([ADR 003](003-payments.md)).
3. **`ChallengeReceiptSummary` is callable by any address** holding sufficient bond — the standard `SlashJudge` challenge interface ([ADR 014 Bond Handling](014-on-chain-verification.md), [ADR 027 §4](027-distinct-client-receipts.md)).

No `WatchtowerEscrow` contract, no heartbeat protocol, no per-channel registration, no `cdn/watchtower/v1` ALPN.

## Defense in depth (stale-close)

Stale-close protection has three layers, with the first as the primary mechanism:

1. **Local in-process dispute monitor** ([ADR 003](003-payments.md) Option C). A lightweight thread in the node binary that watches the chain for `ChannelCloseInitiated` events on its channels and auto-submits the latest voucher. Handles the common case where the node is online.
2. **Operator-arranged redundancy.** Multi-instance node deployments, hot-standby relays, or peer agreements to hold latest vouchers. Out of protocol scope; node-operations responsibility.
3. **Dispute window** (48h PoC default, governable 12h–72h per [ADR 009](009-governance.md)). Provides the time budget for layers 1 and 2 to respond. The forced-inclusion deadline extension in the payment-channel contract preserves an effective response window even under L2 sequencer censorship — see [ADR 003](003-payments.md).

## Privacy

The detector role consumes only public on-chain data — channel close events, epoch summary commits, and receipt batches operators publish. No node-shared voucher state, no privileged access. Privacy considerations from [ADR 017](017-privacy.md) apply to channel parties, not detectors.

## Why this is an appendix, not an ADR

A protocol decision establishes a participant role with a defined wire interface, on-chain registration, fee/payment economics, or off-chain coordination protocol. The fraud-detection layer has none of these — it is a permissionless side-effect of protocols specified elsewhere (`disputeChannel` access from [ADR 003](003-payments.md), `ChallengeReceiptSummary` from [ADR 027](027-distinct-client-receipts.md), bond mechanics from [ADR 014](014-on-chain-verification.md)). Documenting it as an appendix matches the role of [Encrypted Content Publishing](appendix-encrypted-content-publishing.md) or [Observability](appendix-observability.md): operationally relevant, but not a protocol primitive.

## Cross-references

- [ADR 003 — Payments](003-payments.md) — `closeChannel` / `disputeChannel` flow, dispute window, local-monitor Option C, forced-inclusion deadline extension
- [ADR 014 — On-chain Verification](014-on-chain-verification.md) — `SlashJudge` challenge-bond mechanism (Bond Handling)
- [ADR 027 — Distinct-Client Receipts](027-distinct-client-receipts.md) — `EpochReceiptSummary`, `ChallengeReceiptSummary`, identity-diversity rules
- [ADR 026 §Risks](026-gauge-boost-tokenomics.md) — wash-trading layered defenses (per-settlement gas, FeeRouter skim, ADR 027 receipts)
- [Appendix: L2 Deployment](appendix-l2-deployment.md) — gas-cost context for detector economics
