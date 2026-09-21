# ADR 024: Account Abstraction and Safe Smart Wallet Support

**Date:** 2026-04-09
**Status:** Draft

## Context

Three independent design pressures bear on wallet choice and signature verification:

1. **High-frequency signing needs a hot key.** A hash chain meters delivery between signatures ([ADR 003 § Hash-chain metering (PayWord)](003-payments.md#hash-chain-metering-payword)), so a 100 MB download needs two EIP-712 voucher signatures rather than 25. That removes the per-interval signature but not the requirement: a payer still signs at stream open, at each 255 MiB rollover, and at close, and nodes sign a `slash_sig` on every `ProbeResponse` / `StreamResponse`. A hardware wallet requires a physical confirmation (2–5 seconds) per signature, so hardware-wallet-only operation is infeasible at this signing frequency, whatever the wallet model. [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) identified this and explored a derived hot key (HKDF from a hardware-wallet signature) as a narrow workaround.

2. **Multisig custody is desirable for staked funds.** Nodes stake significant TOKEN and accumulate USDC earnings. A single EOA controlling staked funds is a single point of compromise, and Safe multisig wallets are the industry standard for protocol-managed funds. But multisig only helps on the infrequent cold paths (bond, withdraw, pool open/close): a quorum cannot be reached per message at `slash_sig` wire speed. So the requirement is that a Safe be *possible*, not that it be required or tooled.

3. **All signature verification should support smart accounts.** `ECDSA.recover` / `ecrecover` works with EOAs only. Any smart account (Safe, Kernel, Biconomy, etc.) produces signatures that must be verified via ERC-1271 (`isValidSignature`). EOA-only verification on-chain forces a retrofit of every verification site the day a smart account needs to transact.

deCDN resolves these as follows:

- **On-chain (problems 2 and 3):** every contract verifies signatures through OpenZeppelin's `SignatureChecker`, so a Safe — or any ERC-1271 account — works as `msg.sender`, a pool `owner`, or a voucher `signer` on every on-chain path with no further feature work. This is the one piece whose omission would force a coordinated retrofit of every deployed contract later.
- **Off-chain hot signing (problem 1):** the signing host holds a software EOA (the same trust posture as an `eth_keystore` today) and signs `slash_sig`, vouchers, and `BindNodeId` bindings with it. Off-chain verifiers recover the signer from the fixed 65-byte secp256k1 form and compare it against the expected address.

The buyer side needs nothing more. A pool owner — EOA or Safe — funds a pool and issues a capped, expiring capability to a hot EOA `signer` ([ADR 003 § PaymentPool](003-payments.md#paymentpool)); only that EOA is ever recovered, so a Safe funder is servable today with no account-abstraction machinery. What remains open is one operator-side residual — a node whose *serving* identity is a smart account — recorded in [§ Unimplemented — Operator Custody While Serving](#unimplemented--operator-custody-while-serving).

## Decision

### Universal `SignatureChecker` in All Contracts

Every signature verification site across all deCDN contracts uses OpenZeppelin's [`SignatureChecker`](https://docs.openzeppelin.com/contracts/5.x/api/utils#SignatureChecker) library instead of direct `ECDSA.recover` or `ecrecover`.

`SignatureChecker.isValidSignatureNow(signer, digest, signature)` transparently handles both:

- **EOA signers:** falls through to `ECDSA.recover` (3,000 gas via `ecrecover` precompile + ~2,600 gas for `EXTCODESIZE` check)
- **Smart account signers:** calls `IERC1271.isValidSignature(digest, signature)` on the signer contract (~10,000–15,000 gas for a Safe wallet)

The EIP-712 domain separators, typed data hashes, and voucher formats are the same for both. Signature validation is a mechanical concern here: `SignatureChecker` decides *who* counts as a valid signer, and nothing else.

#### Verification Sites Affected

**PaymentPool ([ADR 003](003-payments.md#adr-003-payment-model)):**

| Function | Verification |
| --- | --- |
| `redeem` — voucher signature | `SignatureChecker.isValidSignatureNow(signer, voucherDigest, voucherSig)` |
| `redeem` — capability signature (first redemption per signer) | `SignatureChecker.isValidSignatureNow(pool.owner, capabilityDigest, capability)` |

`signer` is the capability-authorized voucher key; `pool.owner` is the address that signs the capability ([ADR 003 § PaymentPool](003-payments.md#paymentpool)). A pool has no single pinned `voucherSigner` — it authorizes many capped signers off-chain, registered lazily on first redemption.

**Signature validation and the accounting layer are separate concerns.** Account abstraction operates on signature *validation* — *who* counts as a valid signer, under what on-chain policy. It does not, on its own, give one pool multiple concurrent independent signers: that is a property of the payment *accounting* layer, and it is provided by the two-dimensional sharded register in [ADR 003](003-payments.md#redemption-and-close) (`authorized[poolId][signer]` plus `watermark[poolId][signer][provider]`), not by any signature scheme. `SignatureChecker` improves the *key*; the sharded register is what lets many keys draw from one pool with independent ordering and per-signer caps. The two compose: each valid signer is one `signer`, and the register runs many such signers in parallel on a single deposit.

**CapacityBond ([ADR 003](003-payments.md#adr-003-payment-model)):**

| Function | Verification |
| --- | --- |
| `registerNode` — `bindingSignature` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |
| `bindNodeId` — `signature` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |

**SlashJudge ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)):**

| Function | Verification |
| --- | --- |
| `submitRateChallenge` | `SignatureChecker.isValidSignatureNow(challengedNode, probeDigest, probeSig)` + `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSig)` |
| `submitBlacklistChallenge` | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |

The challenger supplies the `challengedNode` address (the node's Ethereum / Safe address); the contract verifies the signature against that address, then confirms the address is registered. This avoids a recover-then-lookup pattern, which does not work for ERC-1271: a smart-account signature has no recovery, only validation. The consequences of that difference for slash-critical signing are the subject of [§ Unimplemented — Operator Custody While Serving](#unimplemented--operator-custody-while-serving).

#### Gas Impact

| Signer Type | Additional Gas vs `ecrecover` | Acceptable? |
| --- | --- | --- |
| EOA | +2,600 (EXTCODESIZE check) | Yes — negligible |
| Safe (1-of-1) | +~12,000 (external call + `checkSignatures`) | Yes — on-chain verification happens only at redemption, pool close, and slash — not per off-chain voucher |
| Safe (2-of-3) | +~15,000 | Yes — same rationale |

### Wallet Support — EOA Default, Safe Supported

The encrypted EOA keystore is deCDN's documented default wallet for both node operators and clients. Safe — and any other ERC-1271-compliant smart account — is **supported**: because `SignatureChecker` is wired at every verification site, a Safe works as `msg.sender`, the pool `owner`, or a voucher `signer` on every on-chain path with no deCDN feature work, no config surface, and no setup tooling.

deCDN recommends none of them. A 1-of-1 Safe carries the same trust posture as the single software-held key that owns it, and the multi-owner threshold that would buy real security cannot be reached on the hot path (below). The choice is the operator's, and the contract-level `SignatureChecker` is what makes it free.

#### Node Operators

**An EOA keystore on the signing host is the default.**

- **Hot signing (`slash_sig`) — the binding constraint.** Nodes sign a `slash_sig` on every `ProbeResponse` / `StreamResponse`. A paying client verifies the `StreamResponse` signature **off-chain**, recovering the signer and comparing it against the delivering node's registered address; the probe leg is shape-checked only today, with full attribution left to the on-chain `SlashJudge`. A Safe owner-key signature recovers to the *owner*, not to the Safe, so a Safe-addressed operator's stream responses are rejected — the node can be probed but cannot be paid. Multi-owner thresholds are separately infeasible here: `SignatureChecker` → Safe's `checkSignatures` cannot reach a quorum at wire speed, and pre-approving every digest via `signMessage` is impractical. Closing this gap is [§ Unimplemented — Operator Custody While Serving](#unimplemented--operator-custody-while-serving).
- **Cold paths are wallet-agnostic.** Bond management (`CapacityBond.bond(...)` + `declareMbps(...)`, `registerNode()`, `deregisterNode()`, `requestUnbond(...)`, `unbond()`) and node-to-node pool operations (`openPool()`, `closePool()`, `topUp()`) are all on-chain and infrequent, so `SignatureChecker` accepts an EOA or a Safe of any threshold. But the registered address is also the address a node serves under — `registerNode` binds `msg.sender` — so there is no split where a Safe custodies the bond while an EOA serves. Holding the on-chain identity in a Safe today means holding a **non-serving** identity: the bond and earnings are multisig-protected, and the node cannot complete a paid delivery.

#### Clients

**An EOA keystore is the default.**

- **Pool operations:** The EOA (or Safe) deposits USDC into `PaymentPool.openPool()`. That address is the pool `owner` — the funder — and `SignatureChecker` makes the choice transparent to every contract.
- **Voucher signing:** EIP-712 vouchers are signed by a capability-authorized `signer`, which the owner delegates (and which defaults to the owner itself). On-chain, `SignatureChecker` validates against it. Off-chain, nodes verify vouchers and `BindNodeId` ephemeral bindings by recovery only, against that signer — so the signer is an EOA.
- **A smart-account funder is servable today, with no residual.** Because the owner and the signer are separate roles, a Safe funds a pool and issues a capability to a plain EOA `signer`. The Safe custodies the deposit and receives the reclaim; the vouchers recover by `ecrecover` and clear the off-chain path unchanged. This is the capability delegation of [ADR 003](003-payments.md#paymentpool) doing exactly what a smart-account client needs — scope-limiting, a spending bound, and expiry-based revocation — without any account-abstraction machinery. The buyer side has no open residual.

### Off-Chain Signature Verification — EOA Recovery Only

[ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) specifies that nodes verify client ephemeral binding signatures (`BindNodeId` with `nonce=0`) off-chain, without an on-chain call.

Every signature deCDN produces or verifies off-chain — the node's `slash_sig`, client vouchers, and `BindNodeId` bindings — is the fixed 65-byte secp256k1 `r‖s‖v` form, verified by recovering the signer and comparing against the expected address. Verifiers reject any other length **fail-closed**. A smart-account signer is therefore un-servable off-chain even though the contracts accept it on-chain, and a party whose registered address is a smart account cannot be verified by recovery at all.

On the buyer side this costs nothing: the owner issues a capability to an EOA `signer`, and only that signer is ever recovered. On the operator side it is the residual below — `registerNode` binds `msg.sender`, and requesters recover `slash_sig` against that registered address, so a Safe-addressed operator cannot serve traffic.

### Unimplemented — Operator Custody While Serving

**A node whose serving identity is a smart account is not supported, and no implementation is scheduled.** deCDN expects mainnet to want it: an operator custodying a large bond and accumulating USDC earnings has the same reason to hold funds in a multisig that a client does, and unlike a client the operator cannot get there through capability delegation, because a node's serving identity and its fund-holding identity are the same address. `registerNode` binds `msg.sender`, and every `slash_sig` is verified against that address, so separating "hot serving key" from "multisig-custodied funds" needs a mechanism that does not exist yet.

This section records the requirement and the constraints any future design must satisfy. It commits to no implementation. It exists because the constraints are sharp enough that the obvious approach — verifying `slash_sig` through ERC-1271, the way the contracts already verify cold-path signatures — is unsound, and a future author must not reach for it by default.

#### The signature partition

ERC-1271 validity is not a property of a keypair; it is `isValidSignature(digest, sig)` evaluated against operator-controlled contract state at call time. An EOA signature is eternally valid — `ecrecover` is deterministic and context-free. A smart-account signature is valid only as long as its account says so. That difference partitions every deCDN signature:

| Class | Signatures | ERC-1271 safe? | Why |
| --- | --- | --- | --- |
| **Consumed at call time** | bond, `registerNode` / `bindNodeId`, `redeem` voucher + capability, pool close, buyer capability grant | Yes — this is what `SignatureChecker` already does | The question is "is it valid *now*?", answered and consumed in the same transaction |
| **Re-verified later as evidence** | `slash_sig` on `ProbeResponse` / `StreamResponse`, and anything else `SlashJudge` re-verifies to punish an offense | **No** | The attacker controls validity in the gap between taking payment and the on-chain slash |

The slashing model ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)) assumes the incriminating signature is the same signature that authorized the paid action and that it stays provably valid until it reaches `SlashJudge`. Smart-account signers break both assumptions. `slash_sig` sits in the second class, so it cannot move to ERC-1271.

#### Hazards a future design must answer

1. **Selective validator.** The operator's `isValidSignature` returns the magic value for serve / voucher / probe digests — so the node is paid and serves — but invalid for the exact rate-challenge and blacklist digests `SlashJudge` re-verifies. The function receives only a digest and a signature; the operator does not need caller context, only to mark the evidence digests invalid. The node serves, collects, and is categorically un-slashable.

2. **Post-payment revocation.** Simpler, and needs no per-digest cunning: the client verifies off-chain and pays *now*; the operator flips a storage flag or revokes the signing session *after*, and the later on-chain re-verification returns invalid. First-class, immediate revocation — the very property a session-key module advertises as a feature — is here the attack: it is exactly what defeats "verify now, slash later."

3. **Gas-bomb on the slash path.** `SlashJudge` → `SignatureChecker` → an external call into the challenged node's contract. A malicious validator burns all forwarded gas or reverts, so the *challenger's* slash transaction fails. `SignatureChecker` puts no gas cap on the ERC-1271 call. Slashing-evasion by denial of service. Any on-chain re-verification of a smart-account signer in a slash path must cap the gas it forwards.

4. **Off-chain ERC-1271 is unsound as evidence.** An off-chain verifier could probe the signer for code and, when present, call `isValidSignature` over an L2 RPC round-trip. But that answers "valid as of block N", and validity is mutable after N. Caching *code presence* is safe (it does not change post-deployment); caching *validity* is not. Fail-open accepts forged signatures; fail-closed makes serving depend on RPC liveness. The recovery-only off-chain path pays none of this.

#### The direction, not the design

Because `slash_sig` cannot be ERC-1271, a viable design keeps it EOA-recovered while letting funds sit in a Safe. The shape that does this is a **native operator-signer delegation** that mirrors the buyer capability of [ADR 003 § PaymentPool](003-payments.md#paymentpool): the Safe custodies the bond and earnings and binds, on-chain, a capped hot EOA to the node identity; that EOA signs `slash_sig`, and requesters verify it by recovery exactly as today. The bound EOA is covered by the same bond and the same slashing, so the penalty still lands on a signature the operator cannot avoid emitting to get paid — the property the second signature class requires. This is lower-altitude than a Safe-7579 adapter plus a session-key module, and it does not import a mutable-validity signer into the slash path. It is a direction for a future ADR written against a live consumer, not a commitment here.

### Safe Infrastructure on the Canonical Testnet

deCDN deploys none of the following and depends on none of it. It is recorded because it is what makes "supported" free: the infrastructure an operator or client would need to run a Safe is already deployed on the testnet sibling of the canonical L2 (Arbitrum Sepolia, per [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)), so choosing a Safe costs deCDN nothing.

| Contract | Status | Notes |
| --- | --- | --- |
| Safe Singleton (v1.4.1) | Deployed | Core Safe logic |
| Safe Proxy Factory | Deployed | Deterministic Safe deployment |
| Safe Singleton Factory | Deployed | `CREATE2` deployment |
| Compatibility Fallback Handler | Deployed | Stock ERC-1271 routing for an operator or client who chooses a Safe |
| Multi Send | Deployed | Batch transactions |

A 1-of-1 Safe and any EOA are fully served by the stock `CompatibilityFallbackHandler` above. deCDN runs no ERC-4337 entry point, paymaster, or bundler: operators and clients submit transactions directly and hold ETH for gas, the same assumption as [ADR 003](003-payments.md#adr-003-payment-model). A future operator-custody design ([§ Unimplemented — Operator Custody While Serving](#unimplemented--operator-custody-while-serving)) would assess its own dependencies against the hazards recorded there.

## Consequences

### Positive

- All deCDN contracts support smart account wallets, eliminating a future retrofit across every verification site.
- deCDN ships no bespoke Safe modules or custom fallback handlers — stock `CompatibilityFallbackHandler` + `SignatureChecker` are sufficient for 1-of-1 Safes and EOAs.
- No contract-level changes beyond the mechanical `SignatureChecker` verification at every site.
- EOA users are unaffected — `SignatureChecker` is a transparent superset of `ECDSA.recover`.
- The approach is wallet-agnostic at the contract level. Any ERC-1271-compliant smart account, Safe included, works without contract changes — deCDN recommends and tools none of them.
- Operator onboarding is one `decdn key-gen` keystore. No wallet-choice decision and no Safe deployment sit on the critical path to serving traffic.
- A smart-account client is fully served today through capability-delegated EOA signers, with no open residual on the buyer side.

### Negative

- Hot-signing parties (nodes, clients) hold a single software-held EOA key. Multisig protection of the high-frequency signing path is not available; a multi-owner Safe is viable only on the infrequent stake / withdraw / pool-open paths, for parties who choose that split themselves.
- A node whose serving identity is a smart account cannot participate off-chain. The contracts accept it; the node's off-chain verifiers do not, and reject anything but the 65-byte recovery form fail-closed. Closing that gap is unimplemented, and the ERC-1271 route that looks obvious is unsound for slash-critical signing ([§ Unimplemented — Operator Custody While Serving](#unimplemented--operator-custody-while-serving)).
- Gas overhead for smart account signature verification is higher than pure `ecrecover` (+10–15k gas per verification). Applies only to on-chain operations (redemption, pool close, slash submission), not the high-frequency off-chain signing path.
