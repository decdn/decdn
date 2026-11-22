# Appendix: Operator Key Rotation Runbook

> **Appendix, not a core protocol ADR.** It sequences operator actions against the API surface defined elsewhere; it does not redefine any mechanism. Protocol semantics: [ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 003](003-payments.md#adr-003-payment-model), [ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow), [ADR 024](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support).

## Context

A node operator routinely holds two keys:

| Key | Curve / scheme | Purpose | Where it lives |
|-----|----------------|---------|----------------|
| **iroh node-key** | Ed25519 | Wire identity (`NodeId`); authenticates the iroh QUIC handshake ([ADR 005](005-protocol.md#adr-005-wire-protocol)). `StreamResponse` body attribution is `operator_sig` — see [ADR 005 § Signer binding](005-protocol.md#signer-binding). | iroh keystore on the signing host |
| **Ethereum signing key** | secp256k1 | On-chain identity for staking, pool ops, voucher receipt. Signs EIP-712 `BindNodeId`/`bindingSignature` ([ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding)) **and** `operator_sig` on every `StreamResponse` ([ADR 005 § Signer binding](005-protocol.md#signer-binding)). | EVM keystore (EOA) — the default and the only form requesters can verify off-chain — **or** a Safe owner key, if the operator chose a Safe (cold paths only; see [§ EOA → Safe migration (one-time, optional)](#eoa--safe-migration-one-time-optional)) |

A node whose serving identity is a smart account — a Safe custodying funds while a hot key signs `operator_sig` — is unimplemented ([ADR 024 § Unimplemented — Operator Custody While Serving](024-account-abstraction.md#unimplemented--operator-custody-while-serving)). There is no third operator key and no procedure to rotate one.

Rotation reasons: (1) **compromise** — suspected leak, revoke **today**; (2) **scheduled hygiene** — routine annual, no time pressure; (3) **hardware migration** — host replacement, HSM enrolment, or EOA → Safe on the cold paths.

The path differs by key and account type.

## Decision tree

```
                  ┌─────────────────────────────┐
                  │ Which key needs rotation?   │
                  └──────────────┬──────────────┘
            ┌───────────────────┴───────────────────┐
            ▼                                       ▼
   ┌──────────────────┐                    ┌──────────────────┐
   │ iroh node-key    │                    │ Ethereum signing │
   │ (Ed25519)        │                    │ key (secp256k1)  │
   └─────────┬────────┘                    └────────┬─────────┘
             │                                      │
             ▼                                      ▼
        § iroh node-key rotation only procedure       Account / goal?
                                                    │
                           ┌────────────────────────┼────────────────────────┐
                           ▼                        ▼                        ▼
                     EOA staying EOA          EOA → Safe (one-time)     Already on Safe
              § EOA → EOA migration (PoC default)   § EOA → Safe migration (one-time, optional)   § Safe owner rotation (optional)
```

If both keys must rotate, rotate the **iroh key first** (cheap, atomic on-chain, preserves Ethereum identity), Ethereum key second (heavyweight). See [§ Rotating both the iroh and Ethereum keys](#rotating-both-the-iroh-and-ethereum-keys).

## iroh node-key rotation only

**API used:** `CapacityBond.bindNodeId(newNodeId, bindingSignature, ed25519Signature)` ([ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding)).

`bindNodeId` is rebind-only — atomically deletes the old `nodeId → ethAddress` mapping and writes the new one. Ethereum address unchanged.

### What carries over

| Survives rotation | Reason |
|-------------------|--------|
| Capacity bond | Keyed by Ethereum address ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) |
| Open lanes paying this node (inbound from clients) | Vouchers name the node's Ethereum `provider` address, unchanged by an iroh-key rotation; the pool id `keccak256(owner, poolNonce)` and the redemption lane are unaffected ([ADR 003](003-payments.md#adr-003-payment-model)) |
| `firstBondedAt` | Kept while the bond stays at or above `bondFloorAtActivation`. `bindNodeId`, `deregisterNode` and ejection do not touch it. An `unbond` below `bondFloorAtActivation` ends the tenure and clears it, so the [§ EOA → EOA migration (PoC default)](#eoa--eoa-migration-poc-default) path loses it ([ADR 003 § Node Registry](003-payments.md#node-registry); [ADR 019 § Re-Onboarding](019-node-onboarding.md#re-onboarding-after-deregistration-or-ejection)) |
| `declaredMbps` | `bindNodeId` does not touch it, so the [§ iroh node-key rotation only](#iroh-node-key-rotation-only) path keeps the tier. The [§ EOA → EOA migration (PoC default)](#eoa--eoa-migration-poc-default) path deregisters, which clears it — re-declare after re-registering ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)) |

### What does not carry over

| Resets on rotation | Impact |
|--------------------|--------|
| Reputation observations | Each peer scores this operator locally, keyed on the observed NodeId ([ADR 008](008-reputation.md#adr-008-reputation-system)); a new NodeId starts at the default 0.5 neutral score |
| Local DHT routing-table position | New NodeId reseeds the Kademlia bucket structure ([ADR 022](022-content-discovery.md#adr-022--content-discovery-at-scale)) |
| TLS session tickets cached by clients | Resumption is keyed on the endpoint id, so rotation invalidates every cached ticket. No round trip is lost — every connection already completes a full handshake ([ADR 005 § Connection Management](005-protocol.md#connection-management)) — clients merely re-pay certificate verification until they re-cache; negligible |

### Procedure

`decdn node rotate-key --key iroh` performs steps 1, 4, 5, and 6 as one command. It generates the key, builds both signatures, submits `bindNodeId`, and installs the new key. It does the steps in that order on purpose: it replaces `node.secret` only after the transaction confirms and emits `NodeIdBound`. A failed transaction therefore leaves the old key in place. The reverse order can leave the node with a key that no binding names, and peers drop every answer such a node signs.

The command does not drain, stop, or restart the node. The iroh key is not hot-reloadable, and the command must also work when the daemon is down. Do steps 2, 3, 7, and 8 yourself.

Use `--dry-run` to print both signatures and both nonces. A dry run writes no file and sends no transaction. It also creates no directory. The node id it prints belongs to a key that the dry run discards. Do not record that id as the next identity of the node. The receipt marks it `preview_key=true`. With `--bind-existing` the printed id is the key already at `node.secret`, and the receipt marks `preview_key=false`.

The printed signatures are not usable by another person. `bindNodeId` derives the binding digest from `bindingNonce[msg.sender]`. It verifies that digest against `msg.sender`. The ed25519 proof also puts `msg.sender` in its preimage. A different submitter is a different `msg.sender`, so both proofs fail. An EOA submitter reverts `InvalidBindingSignature`. A contract submitter with a permissive ERC-1271 `isValidSignature` passes the first check and then reverts `InvalidEd25519Signature`.

Use `--bind-existing` to bind the key that is already at `node.secret`. This repairs a node whose key was replaced by hand. It is also the rollback lever in [§ Failure modes and rollback](#failure-modes-and-rollback).

1. **Generate** a new iroh ed25519 key offline on the signing host. Keep the old keystore until rotation completes.
2. **Drain** the node — refuse new inbound connections, let existing streams complete (`decdn node drain`, surfaced via `admin_v1_drain` in [`appendix-local-admin-http.md`](appendix-local-admin-http.md#appendix-local-admin-http-surface)). Confirm:
   - `decdn_streams_active{direction="inbound"} == 0`

   The gauge counts unpaid discovery opens too. Rotating before it reaches zero ends the open streams of requesters that were about to pay on them — an availability/reputation ding with those requesters. See [ADR 005 § Open-stream pin](005-protocol.md#open-stream-pin).
3. **Stop** the node process.
4. **Build the two signatures `bindNodeId` requires.** (a) The EIP-712 `BindNodeId` binding signature with the operator's Ethereum key at the current `bindingNonce[ethAddress]` — `bindingSignature = EIP-712 sign(ethKey, BindNodeId { nodeId: newNodeId, nonce: bindingNonce[ethAddress] })`. (b) The ed25519 ownership proof, signed with the **new** iroh ed25519 private key, proving control of the NodeId being bound — `ed25519Signature = ed25519_sign(newIrohKey, keccak256(abi.encodePacked(newNodeId, ethAddress, block.chainid, registrationNonce[newNodeId])))` (see [ADR 003 § NodeId Ownership Verification](003-payments.md#nodeid-ownership-verification) for the exact preimage). Signing over `newNodeId` alone builds the wrong digest and reverts with `InvalidEd25519Signature`; the `ethAddress` in the preimage is the `msg.sender` of the submit in step 5, so both signatures must come from the same operator.
5. **Submit** `CapacityBond.bindNodeId(newNodeId, bindingSignature, ed25519Signature)`. The transaction must originate from the same Ethereum address that owns the existing binding. Wait one block confirmation and verify the `NodeIdBound` event.
6. **Update** the node config to point at the new keystore; replace the iroh keystore file at the configured path.
7. **Restart** the node, confirm:
   - `admin_v1_health` answers, with `registry_active` true
   - `decdn_node_uptime_seconds` advancing
   - Peers reach the node at the new NodeId once their registry view reflects the `NodeIdBound` event
8. **Un-drain** — accept inbound connections again.
9. **Archive** the old iroh keystore offline. Retention is forensic-only: no protocol flow reads the old iroh key after `bindNodeId`. Holding it is harmless.

### Failure modes

| Symptom | Likely cause | Recovery |
|---------|--------------|----------|
| `bindNodeId` reverts `"NodeId bound to another address"` | A separate operator already registered `newNodeId` | Generate a different keypair and retry. The ed25519 ownership proof prevents squatting, but a pre-existing binding by a different *legitimate* owner is a hard collision. |
| `bindNodeId` reverts `"invalid signature"` | `bindingNonce[ethAddress]` advanced or EIP-712 digest mis-built | Re-read the on-chain nonce, re-sign, resubmit |
| Requesters report open streams cut at the rotation | Inbound streams not drained before stop | Availability/reputation ding only — the requesters go to other nodes. Wait for `decdn_streams_active{direction="inbound"} == 0` before the stop next rotation |

## Ethereum signing-key rotation only

Account-type-dependent. **Identify the account** before starting:

```bash
cast code <addr>      # EOA → returns 0x ; Safe → returns deployed proxy code
```

### EOA → EOA migration (PoC default)

**No rebinding API exists for the on-chain Ethereum address.** The address is the bond owner — to rotate, move the entire identity. Expect downtime and loss of `firstBondedAt` (the `age_ramp` governance-weight anchor in [ADR 026 § Governance](026-tokenomics.md#governance)).

`decdn node rotate-key --key eth` performs steps 3, 4, and 7. Run the same command at each step. It reads the chain and does the next action, in the same way `decdn node unbond` does. It never reads a flag to find out which step already ran, so a run that lost its receipt is safe to repeat.

Two inputs the chain cannot supply:

- **The declared capacity tier.** Step 3 clears it. The command prints the tier before it clears it. Give it back with `--mbps` at step 7.
- **The new keystore.** Give it with `--new-keystore` at step 7. Fund the new address with TOKEN and a small ETH float first. The command reports a shortfall; it never moves funds between your addresses.

The region and the multiaddrs stay on the registration record, so the command carries them forward. Override them with `--region` and `--multiaddr`.

Step 7 registers a **fresh NodeId**, for the reason step 7 gives below. Use `--accept-terms` for a headless run: a new address is a new registration, so it must accept the operator terms again. [§ iroh node-key rotation only](#iroh-node-key-rotation-only) never re-accepts terms, because a rebinding signs the terms-free `BindNodeId` payload.

A zero exit means "the step completed", not "the migration completed".

Procedure:

1. **Drain** and **stop** the node (same as [§ iroh node-key rotation only](#iroh-node-key-rotation-only) steps 2–3).
2. **Redeem** every outstanding voucher the node holds as a provider from the old address. Each `(signer, provider)` lane is redeemed provider-only and immediately, with no dispute window on the payee side ([ADR 003 § Redemption and Close](003-payments.md#redemption-and-close)), so nothing blocks rotation on the inbound side once the lanes are cashed. Watch `decdn_lanes_open`. Its own outbound pools are handled in step 6.
3. **`CapacityBond.deregisterNode()`** from the old address. Sets `active = false`, increments `registrationNonce[nodeId]`, and clears `declaredMbps` (the step that lets the next one release the whole bond — see [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)). Does **not** return the bond by itself — `deregisterNode` only deactivates; bond exit is a separate operation ([ADR 003 § Node Registry](003-payments.md#node-registry)).
4. **`CapacityBond.unbond(activeBond)`** transfers the whole bond to the old address in the same transaction ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)). This full exit leaves the bond below `bondFloorAtActivation`, so it ends the old address's tenure and clears its `firstBondedAt` ([ADR 036 § Behaviors that follow from the formula](036-served-bytes-voting-weight.md#behaviors-that-follow-from-the-formula)).
5. **Generate** the new EOA. Fund it with TOKEN (transfer from old, or from treasury) and a small ETH float for gas.
6. **Keep the old EVM keystore reachable until every outbound pool whose vouchers it signs is fully redeemed by its upstreams, closed, and reclaimed — or has expired.** On the buying leg (cache-miss pulls upstream) the old key is the capability-authorized `signer` those pools' vouchers are signed by ([ADR 003 § PaymentPool](003-payments.md#paymentpool)), so it remains the key you need to sign a further voucher; if the old address is also the pool `owner`, it is the key that calls `closePool` and `reclaim`. The retention obligation is scoped to exactly those pools: a pool whose signer is a different key needs nothing from this keystore, and a lane where this node is the provider (payee) needs no signing at all — you only redeem. A pool does not expire, so this retention lasts until the old-key pools are closed and reclaimed; the tail is bounded per signer by the capability `expiry` (after which its vouchers are unredeemable per [ADR 003](003-payments.md#adr-003-payment-model)). An operator rotating on a schedule shortens the tail by issuing capabilities from the new key on every pool it opens from the changeover onward, leaving only the pre-rotation pools holding the old keystore open.
7. **Re-bond from the new address with a fresh NodeId.** `TOKEN.approve(capacityBond, amount)` → `CapacityBond.bond(amount)` + `CapacityBond.declareMbps(declaredMbps)`, where `amount = max(minBond, bond_required(declaredMbps))` (deposits the bond and attests capacity per [ADR 019 § Step 2.2](019-node-onboarding.md#step-22--register-at-declared-capacity)) → `CapacityBond.registerNode(newNodeId, multiaddrs, regionHint, termsHash, bindingSignature, ed25519Signature)` for the NodeId binding (see [ADR 019 § Phase 2 — On-chain Setup](019-node-onboarding.md#phase-2--on-chain-setup)). **Reusing the original NodeId is not possible** without a separate `bindNodeId` from the *old* address: `deregisterNode` only sets `active = false` and increments `registrationNonce`, and does **not** clear `nodeIdToAddress[originalNodeId]` ([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh), [ADR 003](003-payments.md#nodeid-to-ethereum-binding)), so a fresh address calling `registerNode(originalNodeId, …)` reverts with `NodeIdAlreadyBound(currentOwner)`. To reuse it (e.g., reputation continuity), chain [§ iroh node-key rotation only](#iroh-node-key-rotation-only) → [§ EOA → EOA migration (PoC default)](#eoa--eoa-migration-poc-default). The chained procedure has three steps, and the middle one is easy to miss. First, `bindNodeId` to a temporary NodeId from the old address. This clears the original mapping. It also archives the original key to `node.secret.bak.<ts>` and installs the temporary key. Second, **restore that archive over `node.secret`**. Third, re-register from the new address. If you skip the restore, the temporary key is the key on disk, and the re-onboarding step registers that key instead of the original.
8. **Restart** the node with config pointing at the new EVM keystore. Confirm `admin_v1_health` answers with `registry_active` true. `decdn_pool_deposit_usdc` reads zero until the rotated key is paid on a pool.
9. **Re-open outbound pools** as needed for cache-miss pulls — no carry-over.

**Real cost:** the new address gets a new `firstBondedAt` from its step 7 `bond()`, and the step 4 full unbond already cleared the old address's value. The operator's `age_ramp` resets to zero and rebuilds to full weight at 6 months per [ADR 026 § Governance](026-tokenomics.md#governance); reputation observations against the old NodeId are stranded — each peer's local score for it decays back to neutral per [ADR 008 § Score Decay](008-reputation.md#score-decay).

> **Note.** EOA rotation is heavyweight: it moves the entire on-chain identity, costs downtime from deregistration to re-registration, and resets `firstBondedAt` (and with it `age_ramp`). Rotating a Safe *owner* key is cheap by comparison — the on-chain address never changes. An operator who expects to rotate more than once may prefer to hold the on-chain identity in a Safe ([§ EOA → Safe migration (one-time, optional)](#eoa--safe-migration-one-time-optional)) and accept the `operator_sig` constraint documented there. The default is a plain EOA keystore; deCDN neither recommends the migration nor tools it ([ADR 024 § Wallet Support — EOA Default, Safe Supported](024-account-abstraction.md#wallet-support--eoa-default-safe-supported)).

### EOA → Safe migration (one-time, optional)

`SignatureChecker` is wired across every contract per [ADR 024 § Universal `SignatureChecker` in All Contracts](024-account-abstraction.md#universal-signaturechecker-in-all-contracts), so a Safe is a drop-in EOA replacement. The migration follows [§ EOA → EOA migration (PoC default)](#eoa--eoa-migration-poc-default) (deregister → unbond → re-register from the Safe), performed once, yielding a stable on-chain identity for all subsequent rotations.

After migration, the Safe holds TOKEN and executes `register` / `openPool` / `closePool` / `topUp`. The Safe's owner key rotates thereafter ([§ Safe owner rotation (optional)](#safe-owner-rotation-optional)).

> **Constraint — off-chain `operator_sig`.** A Safe-addressed operator's `StreamResponse` `operator_sig` is verified **off-chain** by the requester, on a discovery open and on a paid stream alike, which recovers the signer from the 65-byte signature and compares it against the registered address. A Safe owner-key signature recovers to the owner, not to the Safe, so the requester rejects the response, and a Safe-addressed node is neither discovered nor paid. A Safe is therefore usable for the **cold** paths — bond, register, pool open/close, unbond — but not as the address a node serves traffic under. Serving from a Safe while a hot key signs `operator_sig` is unimplemented, and the ERC-1271 route that looks obvious is unsound for the response signature ([ADR 024 § Unimplemented — Operator Custody While Serving](024-account-abstraction.md#unimplemented--operator-custody-while-serving)). Keep the serving identity on an EOA.

### Safe owner rotation (optional)

The cheap path — the on-chain Safe address does not change. It applies to an operator who moved the on-chain identity to a Safe for cold-path custody ([§ EOA → Safe migration (one-time, optional)](#eoa--safe-migration-one-time-optional)); the Safe still cannot serve traffic (constraint above).

Replace an owner via `Safe.swapOwner(prevOwner, oldOwner, newOwner)` (1-of-1), or a standard owner swap via the multisig owners (2-of-3). The Safe address is unchanged; bond, pools, `firstBondedAt`, and `age_ramp` progress all carry over. No iroh-side action needed, and no protocol-level action needed.

## Rotating both the iroh and Ethereum keys

Rotate **iroh first**, then Ethereum:

1. Run [§ iroh node-key rotation only](#iroh-node-key-rotation-only) to completion. `bindNodeId` finality is one block confirmation plus the `NodeIdBound` event (nothing on this path touches the bond; bond exit is always a separate `unbond`, per [ADR 003 § Node Registry](003-payments.md#node-registry)).
2. Run [§ EOA → EOA migration (PoC default)](#eoa--eoa-migration-poc-default), [§ EOA → Safe migration (one-time, optional)](#eoa--safe-migration-one-time-optional), or [§ Safe owner rotation (optional)](#safe-owner-rotation-optional) per the account-type goal. Replay protection between the steps is the per-address `bindingNonce`, incremented when [§ iroh node-key rotation only](#iroh-node-key-rotation-only) ran ([ADR 003 § NodeId-to-Ethereum Binding](003-payments.md#nodeid-to-ethereum-binding)) — no extra cooling-off window required.

Reverse order works but is wasteful: [§ Ethereum signing-key rotation only](#ethereum-signing-key-rotation-only) deregisters the node anyway, so [§ iroh node-key rotation only](#iroh-node-key-rotation-only) work after [§ Ethereum signing-key rotation only](#ethereum-signing-key-rotation-only) is a no-op against an already-deregistered node.

Exception: **emergency compromise of the Ethereum key.** Run [§ Ethereum signing-key rotation only](#ethereum-signing-key-rotation-only) immediately, skip [§ iroh node-key rotation only](#iroh-node-key-rotation-only) unless the iroh key is also suspected. The on-chain identity is the higher-value target — protect it first.

## Failure modes and rollback

| Scenario | Detection | Rollback |
|----------|-----------|----------|
| `bindNodeId` succeeded but node won't restart | `decdn_node_uptime_seconds` reset, `admin_v1_health` unreachable | Restore the old keystore from its `node.secret.bak.<ts>` archive, then run `decdn node rotate-key --key iroh --bind-existing` to bind it again from the same Ethereum key. Restart |
| `bindNodeId` was sent, but the command could not confirm the binding moved (no receipt read within the 3-minute bound, an unreadable receipt, or a receipt with no decodable `NodeIdBound`) | The command exits non-zero and reports `parked_key=<path>`; `submitted=unknown` means the transaction may still be in the mempool | The new key is preserved at `node.secret.pending.<node_id>` and is NOT installed. Check the transaction. If it confirmed, move that file over `node.secret` — the rotation is then complete and needs no second transaction. If it did not, delete the file and re-run. The error names which of the two is more likely: an ABI-drift diagnosis means the binding probably DID move |
| Node key replaced with no on-chain rebinding | `decdn node health` reports `binding=mismatch`, and the daemon logs a WARN at start. Peers resolve the node's operator through the binding, so they drop every answer it signs | Bind the key on disk with `decdn node rotate-key --key iroh --bind-existing`, or restore the key the binding names. Restart, then confirm `binding=bound` |
| Mid-procedure abort during [§ EOA → EOA migration (PoC default)](#eoa--eoa-migration-poc-default) (deregister submitted, unbond not done) | `CapacityBond.isActiveNode(nodeId) == false`, bond still deposited | Re-run the command to unbond, or re-register from the same address — old `nodeId` is reusable |
| [§ EOA → EOA migration (PoC default)](#eoa--eoa-migration-poc-default) step 6: latent voucher submitted by a counterparty | `closePool` event against old address post-deregister | Old keystore must be reachable; `closePool` runs against the old address regardless of registration state |

## What this runbook does not cover

- **Client-side iroh-key rotation.** See [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model), inline "Key rotation": open client pools and their client→node lanes survive client iroh-key rotation (neither key contains a NodeId), mirroring the operator-side carry-over in [§ iroh node-key rotation only](#iroh-node-key-rotation-only).
- **Hot-signing alternatives.** Hardware-wallet and HSM-backed voucher signing are infeasible per [ADR 012 § Consequences](012-client.md#consequences) — 2–5s confirmation latencies cannot keep pace with stream-open, rollover, and close signatures, nor with the per-response `operator_sig`. On the buyer side the pool already authorizes capped, expiring signers off-chain by owner-signed capability ([ADR 003 § PaymentPool](003-payments.md#paymentpool)), which covers delegation without new contract surface and, unlike an immutable pin, carries native revocation via `expiry`. On the operator side, separating a hot serving key from Safe-custodied funds is unimplemented; the shape a future design would take is an EOA delegation mirroring that same capability, not an ERC-1271 session key ([ADR 024 § Unimplemented — Operator Custody While Serving](024-account-abstraction.md#unimplemented--operator-custody-while-serving)).
- **Compromised-key incident response.** This runbook describes mechanics. If a key is *believed compromised*, the operator should also: alert peer operators out-of-band and rotate before any further wire-level signature under the compromised key. There is no protocol-funded incident reserve (the `SafetyReserve` was retired — see [ADR 026 § Incident recourse](026-tokenomics.md#incident-recourse-no-standing-reserve)); any discretionary restitution for losses is a DAO Treasury governance matter.

## Cross-ADR Impact

- [ADR 001 — On-chain registry, `registerNode`, NodeId ownership verification](001-network.md#adr-001-network-topology-and-peer-mesh)
- [ADR 003 — `bindNodeId` rebinding, `nodeIdToAddress`, EIP-712 `BindNodeId` schema](003-payments.md#nodeid-to-ethereum-binding)
- [ADR 005 — Open-stream pin (drain prerequisite)](005-protocol.md#open-stream-pin)
- [ADR 008 — Local per-peer reputation](008-reputation.md#adr-008-reputation-system)
- [ADR 012 — Client iroh-key rotation analogue](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model)
- [ADR 019 — `registerNode`, `deregisterNode`, re-onboarding flow](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow)
- [ADR 024 — `SignatureChecker` on-chain, EOA recovery off-chain, and the unimplemented operator-custody residual](024-account-abstraction.md#adr-024-account-abstraction-and-safe-smart-wallet-support)
- [`appendix-local-admin-http.md` — `admin_v1_drain` invocation](appendix-local-admin-http.md#appendix-local-admin-http-surface)
