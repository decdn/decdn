# Appendix: Permissionless Stale-Close Detection

> **This is an appendix, not a core protocol ADR.** It describes the operational role of running a stale-close detector — a permissionless side-effect of `closeChannel` / `disputeChannel` being open to any voucher holder.

## Context

The protocol has one on-chain surface where a party can publish a falsified value and benefit if no third party objects within a bounded window — **`closeChannel` / `disputeChannel`** ([ADR 003](003-payments.md#adr-003-payment-model)): a client may close a channel with a stale (low-nonce) voucher; the 48-hour dispute window settles at the stale value unless a higher-nonce voucher (signed by the same channel funder) is submitted in time. Submitting is permissionless from the protocol's side — see Contract integration below.

The wash-trading defense is separately the [ADR 026 §3 per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap), contract-enforced without external attestation — no operator-asserted gauge summary, no fraud-challenge mechanism, no monitoring role for gauge-share; the cap binds in the formula directly. Off-chain reputation observation ([ADR 008 §12](008-reputation.md#12-gauge-pool-wash-trading-reputation-as-off-chain-signal)) and operator-cluster detection are the soft layer informing governance cap-tuning if persistent wash-trading patterns surface.

## Role

Anyone with the technical capacity to:

- subscribe to L2 RPC `eth_getLogs` for `ChannelCloseInitiated` events, and
- hold a hot wallet with enough gas to submit `disputeChannel`,

can act as a stale-close detector. No registration, registry, fees, or wire interface (see [Why this is an appendix](#why-this-is-an-appendix-not-an-adr)).

When `ChannelCloseInitiated(channelId, ..., nonce)` fires, the detector checks whether it holds a voucher with a higher nonce for that channel. If so, it submits `disputeChannel`.

- A successful dispute updates on-chain `claimedAmount` / `claimedNonce` / `claimedBytes`.
- The detector spends gas (~$0.05–$0.10 on the production L2 — see [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)) and recovers nothing from the protocol — they must have an out-of-band reason to hold a higher voucher (i.e., they are the node operator, the operator's hot-spare infrastructure, or a counterparty).

This is fundamentally a self-protection mechanism, not a paid service: the offline-for-the-full-48h case is an operational failure mode, not a protocol gap (the online-node case is handled automatically — see Defense in depth).

## Contract integration

Two contract-level requirements, already specified in their owning ADRs:

1. **`disputeChannel` accepts submissions from any address** — the voucher's EIP-712 signature is the sole authorization (`ecrecover(signature) == channel.client`), no `msg.sender` access check ([ADR 003 § closeChannel/disputeChannel](003-payments.md#adr-003-payment-model)).
2. **`ChannelCloseInitiated`, `ChannelDisputed`, `ChannelSettled` events are emitted** ([ADR 003](003-payments.md#adr-003-payment-model)).

No `WatchtowerEscrow` contract, heartbeat protocol, per-channel registration, or `cdn/watchtower/v1` ALPN.

## Defense in depth (stale-close)

Three layers, with the first as the primary mechanism:

1. **Local in-process dispute monitor** ([ADR 003](003-payments.md#adr-003-payment-model) Option C). A lightweight thread in the node binary that watches the chain for `ChannelCloseInitiated` events on its channels and auto-submits the latest voucher. Handles the common online case.
2. **Operator-arranged redundancy.** Multi-instance node deployments sharing voucher state, hot-standby relays, or peer agreements to hold latest vouchers. The protocol defines no wire format for this; it is node-operations responsibility, like running redundant origin backends.
3. **Dispute window** (48h PoC default, governable 12h–72h per [ADR 009](009-governance.md#adr-009-governance-model)). The time budget for layers 1 and 2 to respond. The forced-inclusion deadline extension in the payment-channel contract preserves an effective response window even under L2 sequencer censorship — see [ADR 003](003-payments.md#adr-003-payment-model).

## Privacy

The detector consumes only public on-chain data — channel close events. No node-shared voucher state, no privileged access. Privacy considerations from [ADR 017](017-privacy.md#adr-017-privacy-analysis) apply to channel parties, not detectors.

## Why this is an appendix, not an ADR

A protocol decision establishes a participant role with a defined wire interface, on-chain registration, fee/payment economics, or off-chain coordination protocol. The fraud-detection layer has none — it is a permissionless side-effect of `disputeChannel` access from [ADR 003](003-payments.md#adr-003-payment-model), like [Encrypted Content Publishing](appendix-encrypted-content-publishing.md#appendix-encrypted-content-publishing-on-decdn) or [Observability](appendix-observability.md#appendix-observability-and-metrics): operationally relevant, but not a protocol primitive.

## Cross-ADR Impact

- [ADR 003 — Payments](003-payments.md#adr-003-payment-model) — `closeChannel` / `disputeChannel` flow, dispute window, local-monitor Option C, forced-inclusion deadline extension
- [ADR 014 — On-chain Verification](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) — `SlashJudge` challenge-bond mechanism (Bond Handling) for the three signature-dependent offenses
- [ADR 026 §3 Per-operator gauge-share cap](026-gauge-boost-tokenomics.md#per-operator-gauge-share-cap) — wash-trading defense; contract-enforced, no external attestation surface
- [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target) — gas-cost context for detector economics
