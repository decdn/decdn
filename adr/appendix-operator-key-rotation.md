# Appendix: Operator Key Rotation Runbook

> **This is an appendix, not a core protocol ADR.** It documents the safe sequence for rotating the keys an operator holds, against the API surface defined elsewhere in the ADR set. The protocol semantics live in [ADR 001](001-network.md), [ADR 003](003-payments.md), [ADR 019](019-node-onboarding.md), and [ADR 024](024-account-abstraction.md) — this appendix only sequences operator actions.

## Context

A node operator routinely holds three keys:

| Key | Curve / scheme | Purpose | Where it lives |
|-----|----------------|---------|----------------|
| **iroh node-key** | Ed25519 | Wire identity (`NodeId`); authenticates the iroh QUIC handshake for every connection and signs `NodeAnnounce` gossip ([ADR 005](005-protocol.md)). `ProbeResponse` and `StreamResponse` body attribution moved to `slash_sig` — see [ADR 014 §1](014-on-chain-verification.md#1-slash-signatures--secp256k1-eip-712). | iroh keystore on the signing host |
| **Ethereum signing key** | secp256k1 | On-chain identity for staking, channel ops, voucher receipt. Signs the EIP-712 `BindNodeId` and `bindingSignature` ([ADR 003 §NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding)) **and** the `slash_sig` field on every `ProbeResponse` / `StreamResponse` for on-chain accountability ([ADR 014](014-on-chain-verification.md)). The hot-signing burden of the latter motivates the production session-key path below. | EVM keystore (EOA) **or** Safe owner key (PoC 1-of-1) **or** session key delegated by a Safe (production §3 of [ADR 024](024-account-abstraction.md)) |
| **Slash-sig session key** *(production only)* | secp256k1 | Per-message hot-signing of `slash_sig` digests on `ProbeResponse` / `StreamResponse` at wire speed when the operator runs a 2-of-3 Safe; authorized via `erc7579/smartsessions` ([ADR 024](024-account-abstraction.md) §3). Note: client-side voucher session keys (also production-only, also via smartsessions) are a separate concern owned by clients, not operators. | Signing host, scoped by the session-key policy |

Rotation reasons:

1. **Compromise.** A key is suspected leaked — must be revoked **today**.
2. **Scheduled hygiene.** Routine annual rotation; no time pressure.
3. **Hardware migration.** Host replacement, HSM enrolment, or moving from PoC EOA to a production Safe.

The rotation path differs by which key and by account type. This runbook is a sequenced checklist; it does **not** redefine any protocol mechanism.

## Decision tree

```
                  ┌─────────────────────────────┐
                  │ Which key needs rotation?   │
                  └──────────────┬──────────────┘
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
   ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
   │ iroh node-key    │ │ Ethereum signing │ │ slash-sig session│
   │ (Ed25519)        │ │ key (secp256k1)  │ │ key (production) │
   └─────────┬────────┘ └────────┬─────────┘ └────────┬─────────┘
             │                   │                    │
             ▼                   ▼                    ▼
        §1 procedure       Account / goal?         §3 procedure
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
        EOA staying EOA   EOA → Safe (one-time)   Already on Safe
            §2.1                §2.2                  §2.3
```

If both the iroh and Ethereum keys must rotate, rotate the **iroh key first** (cheap, atomic on-chain, preserves Ethereum identity) and the Ethereum key second (heavyweight). See §4.

## 1. iroh node-key rotation only

**API used:** `StakingRegistry.bindNodeId(newNodeId, signature)` ([ADR 003 §NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding)).

The standalone `bindNodeId` function is intended for rebinding only — it deletes the old `nodeId → ethAddress` mapping atomically and writes the new one. The Ethereum address is unchanged; stake, payment channels keyed by `(client_eth, operator_eth, nonce)`, and `firstRegisteredAt` all carry over.

### What carries over

| Survives rotation | Reason |
|-------------------|--------|
| Stake, ve-locks, gauge-claim history | Keyed by Ethereum address ([ADR 026](026-gauge-boost-tokenomics.md)) |
| Open payment channels (inbound from clients) | Channel ID is `keccak256(client_eth, operator_eth, nonce[, token])` ([ADR 003](003-payments.md), [ADR 010](010-multi-token.md)) — the Ethereum address is unchanged |
| `firstRegisteredAt` | Cleared only by `deregisterNode`; `bindNodeId` does not touch it ([ADR 019 §Re-Onboarding](019-node-onboarding.md#re-onboarding-after-deregistration-or-auto-ejection)) |
| Receipt history (ADR 027) | Receipts are signed by *requester* keys, not the operator's |

### What does not carry over

| Resets on rotation | Impact |
|--------------------|--------|
| Reputation observations from peers | Other nodes index reputation reports by `(reporter, provider)` NodeId pair ([ADR 008](008-reputation.md)). The new NodeId starts at the cold-start bootstrap score. |
| Local DHT routing-table position | The new NodeId reseeds the Kademlia bucket structure ([ADR 022](022-content-discovery.md)). |
| 0-RTT session tickets cached by clients | Clients fall back to 1-RTT until they re-cache ([ADR 015](015-zero-rtt.md)). Brief P95 latency bump. |

### Procedure

1. **Generate** a new iroh ed25519 key offline on the signing host. Keep the old keystore until the rotation completes successfully.
2. **Drain** the node — refuse new inbound connections, let existing streams complete (`decdn node drain`, surfaced via the `admin_v1_drain` admin RPC method listed in [`appendix-local-admin-http.md`](appendix-local-admin-http.md); the subcommand itself is tracked in issue [#244](https://github.com/decdn/decdn/issues/244)). Confirm:
   - `decdn_streams_active{direction="inbound"} == 0`
   - `decdn_probe_hold_slots_used == 0`

   The probe-hold drain is critical: rotating before holds clear opens a phantom-slash window, since outstanding holds were signed by the *old* NodeId but the *new* NodeId would not honor them. See [ADR 005 §Probe-Triggered Eviction Hold](005-protocol.md#probe-triggered-eviction-hold).
3. **Stop** the node process.
4. **Build the EIP-712 `BindNodeId` binding signature** with the operator's Ethereum key, using the current `bindingNonce[ethAddress]` — `bindingSignature = EIP-712 sign(ethKey, BindNodeId { nodeId: newNodeId, nonce: bindingNonce[ethAddress] })`.
5. **Submit** `StakingRegistry.bindNodeId(newNodeId, bindingSignature)`. The transaction must originate from the same Ethereum address that owns the existing binding. Wait for one block confirmation and verify the `NodeIdBound` event.
6. **Update** the node config to point at the new keystore. Replace the iroh keystore file at the path specified in the operator's config.
7. **Restart** the node, confirm:
   - `/health` reports `ready`
   - `decdn_node_uptime_seconds` advancing
   - Outgoing `NodeAnnounce` carries the new NodeId (visible in peers' gossip logs)
8. **Un-drain** — accept inbound connections again.
9. **Archive** the old iroh keystore offline. Retain it for at least `MAX_EVIDENCE_AGE_US` (default 5 days, governable [1d, 30d] per [ADR 014](014-on-chain-verification.md) §2 Evidence Verification Per Offense Type) — that is the staleness ceiling beyond which slash evidence containing signatures from the old key cannot be submitted on-chain. The retention is for forensic inspection (which signatures the old key produced); `bindNodeId` does not initiate unbonding, and all `SlashJudge` offenses (phantom, rate, blacklist) resolve synchronously at submit time per [ADR 014 §Bond Handling](014-on-chain-verification.md#bond-handling) — no key-bound counter-evidence flow exists for the old iroh key to produce. Holding longer is harmless; deleting earlier means losing the ability to forensically reconstruct what the old key signed.

### Failure modes

| Symptom | Likely cause | Recovery |
|---------|--------------|----------|
| `bindNodeId` reverts with `"NodeId bound to another address"` | A separate operator already registered `newNodeId` | Generate a different keypair and try again. The on-chain ed25519 ownership proof prevents squatting attacks, but pre-existing binding by a different *legitimate* owner is still a hard collision. |
| `bindNodeId` reverts with `"invalid signature"` | `bindingNonce[ethAddress]` advanced (e.g. another operation incremented it) or the EIP-712 digest was mis-built | Re-read the on-chain nonce, re-sign, resubmit |
| Restarted node throws phantom-announcement self-detection | Probe holds were not drained before stop | Stop immediately, file a bug, **do not restart** until the slash-evidence-exposure window (30s per [ADR 005](005-protocol.md)) has elapsed |
| Peers continue to address the old NodeId | Gossip re-propagation lag | Up to one `node_announce_interval`; if it persists past two intervals, restart gossip subscription |

## 2. Ethereum signing-key rotation only

The path is account-type-dependent. **Identify the account** before starting:

```bash
# EOA path: address is a regular externally-owned account
cast code <addr>      # returns 0x

# Safe path: address has bytecode (the Safe proxy)
cast code <addr>      # returns deployed proxy code
```

### 2.1 EOA → EOA migration (PoC default)

**There is no rebinding API for the on-chain Ethereum address.** The address is the stake owner — to rotate, the operator must move the entire identity. Expect downtime and a loss of `firstRegisteredAt` (the cold-start bootstrap signal in [ADR 008 §10](008-reputation.md#10-cold-start-bootstrap)).

Procedure:

1. **Drain** and **stop** the node (same as §1 steps 2–3).
2. **Wait** for all open inbound channels to settle. Watch `decdn_channels_open`. If any channel is in the dispute window, do **not** rotate yet — settling a stale state in step 7 below requires the old keystore. The PoC dispute window is 48h ([ADR 003](003-payments.md)); production is governable 12h–72h ([ADR 009](009-governance.md)).
3. **`StakingRegistry.deregisterNode()`** from the old address. This sets `active = false`, starts the unbonding period, and increments `registrationNonce[nodeId]`. Stake remains slashable during unbonding ([ADR 019 §Re-Onboarding](019-node-onboarding.md#re-onboarding-after-deregistration-or-auto-ejection)).
4. **Wait** the full unbonding period (default 7d, minimum 3d governable). Stake remains slashable in this window — do not relax monitoring.
5. **`StakingRegistry.withdraw()`** to the old address.
6. **Generate** the new EOA. Fund it with TOKEN (transfer from old, or from treasury) and a small ETH float for gas.
7. **Keep the old EVM keystore reachable while any outbound channels remain open.** Vouchers are *client*-signed ([ADR 003](003-payments.md)), so as a node-operator-as-client (cache-miss pulls upstream) you have signed vouchers paying upstream nodes under the old Ethereum address. Until those channels are settled, the upstream counterparty may submit your latest signed voucher via `closeChannel` and trigger the 48h dispute window. Channels live up to `maxChannelDuration` (default 90 days per [ADR 010](010-multi-token.md)); the practical window for keeping the old keystore reachable is "until your last open outbound channel settles or expires." As a node-operator-as-provider you do not sign vouchers, so inbound channels carry no key-side liability after deregistration.
8. **Re-stake from the new address with a fresh NodeId.** `TOKEN.approve` → `StakingRegistry.stake` → atomic `registerNode(newNodeId, multiaddrs, regionHint, bindingSignature, ed25519Signature)` (see [ADR 019](019-node-onboarding.md) §Phase 2 — On-chain Setup). **Reusing the original NodeId is not possible** without a separate `bindNodeId` step on the *old* address before deregister: `deregisterNode` only sets `active = false` and increments `registrationNonce`; it does **not** clear `nodeIdToAddress[originalNodeId]` ([ADR 001](001-network.md) §340–342, [ADR 003](003-payments.md#nodeid-to-ethereum-binding)), so a fresh address calling `registerNode(originalNodeId, …)` reverts with `"NodeId bound to another address"`. If reusing the original NodeId is important (e.g., for historical reputation continuity), chain §1 → §2.1 instead: §1's `bindNodeId` to a temporary new NodeId clears the original mapping, then §2.1 re-registers the original NodeId from the new address.
9. **Restart** the node with config pointing at the new EVM keystore. Confirm `/health` reports `ready` and `decdn_channel_deposit_usdc` is zero (no channels yet).
10. **Re-open outbound channels** as needed for cache-miss pulls — there is no carry-over.

**Real cost:** `firstRegisteredAt` resets to the new registration timestamp; the cold-start bootstrap window starts again. Reputation observations on the old NodeId↔Ethereum-address pair are stranded — peers' caches will time out per [ADR 008 §Score decay](008-reputation.md). Stake-time-weighted gauge-boost (if `firstRegisteredAt` is consumed by the gauge formula in any deployment) is lost.

> **Recommendation.** Treat EOA rotation as a last resort. If at all possible, perform the **one-time migration to a Safe smart account** (§2.2 path) instead of rotating EOA-to-EOA — once on a Safe, all future "key rotations" are owner/session-key swaps with no on-chain identity change.

### 2.2 EOA → Safe migration (one-time, recommended)

`SignatureChecker` is wired across every contract per [ADR 024 §1](024-account-abstraction.md), so a Safe is a drop-in replacement for an EOA. The migration itself follows §2.1 (deregister → unbond → withdraw → re-stake from the Safe), but is performed once and yields a stable on-chain identity for all subsequent rotations.

After migration, the Safe holds TOKEN and executes `stake` / `registerNode` / `openChannel` / `closeChannel` / `topUp`. The Safe's owner key is what rotates from then on (§2.3).

### 2.3 Safe owner / session-key rotation (production preferred path)

This is the cheap path — the on-chain Safe address does not change.

#### PoC (1-of-1 Safe)

Replace the single owner via `Safe.swapOwner(prevOwner, oldOwner, newOwner)`. The Safe address is unchanged; stake, channels, ve-locks, and `firstRegisteredAt` all carry over. No iroh-side action needed.

#### Production (2-of-3 + `erc7579/smartsessions`)

Two cases:

- **Owner rotation:** standard 2-of-3 owner swap via the multisig owners. No protocol-level action needed.
- **Session-key rotation:** revoke the old session key via the `erc7579/smartsessions` revocation interface; install a new session key with the same policy ([ADR 024 §3](024-account-abstraction.md)). Slash-evidence signing resumes against the new session key with no on-chain identity change.

Procedure (session-key rotation):

1. Generate a new session-key keypair on the signing host.
2. Build the install-session-key transaction — owners co-sign per the Safe threshold.
3. Submit; wait for confirmation.
4. Update the node config to load the new session key. Restart the node (no drain needed — slash-evidence signing is the only path that uses this key, and it is per-message).
5. **After** the new session key has signed at least one wire-level slash digest successfully, revoke the old session key via the same module.

If the old session key is *suspected compromised*, reverse the order: revoke first, then install the new key. Brief slash-signing outage is acceptable to ensure the compromised key cannot be used.

## 3. Voucher session-key rotation (production)

This is the same as [§2.3 — production](#23-safe-owner--session-key-rotation-production-preferred-path), with no caveats for an iroh-side action.

## 4. Rotating both the iroh and Ethereum keys

Rotate **iroh first**, then Ethereum. Specifically:

1. Run §1 to completion. `bindNodeId` finality is one block confirmation plus the `NodeIdBound` event — there is no unbonding period (only `deregisterNode` triggers unbonding per [ADR 001](001-network.md#contract-interface-node-registry)).
2. Run §2.1, §2.2, or §2.3 depending on the account-type goal. Replay protection between the two steps is handled by the per-address `bindingNonce`, which incremented when §1 ran ([ADR 003 §NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding)) — no additional cooling-off window is required.

The reverse order works but is wasteful: §2 takes the node offline for the unbonding window anyway, so any §1 work performed after §2 is a no-op against an already-deregistered node.

Exception: **emergency compromise of the Ethereum key.** If the Ethereum key is the compromised one, run §2 immediately and skip §1 unless the iroh key is also suspected. The on-chain identity is the higher-value target — protect it first.

## 5. Failure modes and rollback

| Scenario | Detection | Rollback |
|----------|-----------|----------|
| `bindNodeId` succeeded but the node won't restart | `decdn_node_uptime_seconds` reset and `/health` `not_ready` | Repoint config at old keystore, re-`bindNodeId` to old NodeId from same Ethereum key, restart |
| Mid-procedure abort during §2.1 (deregister submitted, withdraw not yet executed) | `StakingRegistry.isActiveNode(nodeId) == false`, stake still locked | Wait the unbonding window; you may also re-register from the same address before the period elapses if you change your mind — the old `nodeId` is reusable |
| §2.1 step 7: latent voucher submitted by a counterparty | `closeChannel` event against the old address after deregister | The old keystore must be reachable; the `closeChannel` flow runs against the old address regardless of your registration state |
| §2.3 session-key revocation fails | Module revert | Revert to the old session key, file a bug; the protocol-level slash signing degrades gracefully (signatures from the still-valid old key continue to be accepted) |

## 6. What this runbook does not cover

- **Client-side iroh-key rotation.** See [ADR 012](012-client.md), the inline "Key rotation" paragraph: open client→node channels survive client iroh-key rotation because they are keyed by the client's Ethereum address, mirroring the operator-side carry-over in §1.
- **Deferred design decisions:**
  - Hardware-wallet-based hot signing for production EOA operators ([ADR 012 §HW-wallet pattern](012-client.md)). The runbook assumes software keystores; HSM/HW-wallet flows are a deployment concern.
  - Delegated voucher-signer support for clients without smart-account migration (tracked as the unresolved item in [#190](https://github.com/decdn/decdn/issues/190)).
- **Compromised-key incident response.** This runbook describes mechanics. If a key is *believed compromised*, the operator should also: file an incident report with the SafetyReserve registry ([ADR 026 §5](026-gauge-boost-tokenomics.md)) if losses occurred, alert peer operators via reputation gossip, and rotate before any further wire-level signature is produced under the compromised key.

## Cross-references

- [ADR 001 — On-chain registry, `registerNode`, NodeId ownership verification](001-network.md)
- [ADR 003 — `bindNodeId` rebinding, `nodeIdToAddress`, EIP-712 `BindNodeId` schema](003-payments.md#nodeid-to-ethereum-binding)
- [ADR 005 — Probe-triggered eviction hold (drain prerequisite)](005-protocol.md#probe-triggered-eviction-hold)
- [ADR 008 — Reputation per `(reporter, provider)` NodeId pair](008-reputation.md)
- [ADR 012 — Client iroh-key rotation analogue](012-client.md)
- [ADR 019 — `registerNode`, `deregisterNode`, re-onboarding flow](019-node-onboarding.md)
- [ADR 024 — Smart-account session keys via `erc7579/smartsessions`](024-account-abstraction.md)
- [`appendix-local-admin-http.md` — `admin_v1_drain` invocation](appendix-local-admin-http.md)
