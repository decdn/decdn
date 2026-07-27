# ADR 024: Account Abstraction and Safe Smart Wallet Support

**Date:** 2026-04-09
**Status:** Draft

## Context

Three independent design pressures bear on wallet choice and signature verification:

1. **High-frequency signing needs a hot key.** At the default 1 MB voucher cadence, a 100 MB download needs 100 EIP-712 signatures. A hardware wallet requires a physical confirmation (2–5 seconds) per signature, and nodes sign a `slash_sig` on every `ProbeResponse` / `StreamResponse`. Hardware-wallet-only operation is infeasible at this cadence, whatever the wallet model. [ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) identified this and explored a derived hot key (HKDF from a hardware-wallet signature) as a narrow workaround.

2. **Multisig custody is desirable for staked funds.** Nodes stake significant TOKEN and accumulate USDC earnings. A single EOA controlling staked funds is a single point of compromise, and Safe multisig wallets are the industry standard for protocol-managed funds. But multisig only helps on the infrequent cold paths (bond, withdraw, channel open/close): a quorum cannot be reached per message at `slash_sig` wire speed. So the requirement is that a Safe be *possible*, not that it be required or tooled.

3. **All signature verification should support smart accounts.** Current contracts use `ECDSA.recover` / `ecrecover` only, which works with EOAs only. Any smart account (Safe, Kernel, Biconomy, etc.) produces signatures that must be verified via ERC-1271 (`isValidSignature`). EOA-only verification now forces a retrofit of every verification site later.

This ADR splits the response along a sharp PoC vs Production line:

- **PoC:** solve problem 3 now. Adopt `SignatureChecker` across every contract — it is the one piece whose omission forces a coordinated retrofit of every deployed contract later, and it makes a Safe usable on every on-chain path without any further deCDN feature work (problem 2). Leave problem 1 on its existing footing: a software-held signing key on the signing host (same trust posture as today's `eth_keystore`), used as a plain EOA. That EOA keystore is deCDN's documented default wallet.
- **Production:** solve problem 1 by migrating to ERC-7579 smart accounts (Safe via the Safe-7579 adapter) and installing [`erc7579/smartsessions`](https://github.com/erc7579/smartsessions) — a standardized session-key module with native ERC-1271 validation, policy enforcement (time windows, allowed selectors/domains, spending caps), and first-class revocation.

Deferring session keys to Production keeps the PoC PR small and avoids bespoke security-critical contract code for a problem that has a standardized upstream solution.

## Decision

### Universal `SignatureChecker` in All Contracts

Every signature verification site across all deCDN contracts MUST use OpenZeppelin's [`SignatureChecker`](https://docs.openzeppelin.com/contracts/5.x/api/utils#SignatureChecker) library instead of direct `ECDSA.recover` or `ecrecover`.

`SignatureChecker.isValidSignatureNow(signer, digest, signature)` transparently handles both:

- **EOA signers:** falls through to `ECDSA.recover` (3,000 gas via `ecrecover` precompile + ~2,600 gas for `EXTCODESIZE` check)
- **Smart account signers:** calls `IERC1271.isValidSignature(digest, signature)` on the signer contract (~10,000–15,000 gas for a Safe wallet)

This is a mechanical replacement. The EIP-712 domain separators, typed data hashes, and voucher formats are unchanged.

#### Verification Sites Affected

**PaymentChannel ([ADR 003](003-payments.md#adr-003-payment-model)):**

| Function | Current | After |
| --- | --- | --- |
| `closeChannel` — voucher signature | `ECDSA.recover(digest, sig) == channel.voucherSigner` | `SignatureChecker.isValidSignatureNow(channel.voucherSigner, digest, sig)` |

`withdraw`, `disputeChannel`, and `cooperativeClose` validate the voucher signature using the same scheme as `closeChannel` and migrate the same way. `channel.voucherSigner` is the address the funder pinned at `openChannel` ([ADR 003 § PaymentChannel](003-payments.md#paymentchannel)); it equals `channel.client` where no delegate was named.

**CapacityBond ([ADR 003](003-payments.md#adr-003-payment-model)):**

| Function | Current | After |
| --- | --- | --- |
| `registerNode` — `bindingSignature` | `ECDSA.recover(digest, sig) == msg.sender` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |
| `bindNodeId` — `signature` | `ECDSA.recover(digest, sig) == msg.sender` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |

**SlashJudge ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)):**

| Function | Current | After |
| --- | --- | --- |
| `submitPhantomChallenge` | `ecrecover` → address A, `ecrecover` → address B, verify A == B | `SignatureChecker.isValidSignatureNow(challengedNode, probeDigest, probeSig)` + `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSig)` |
| `submitRateChallenge` | `ecrecover` → address A, `ecrecover` → address B, verify A == B | `SignatureChecker.isValidSignatureNow(challengedNode, probeDigest, probeSig)` + `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSig)` |
| `submitBlacklistChallenge` | `ecrecover` → address, verify registered | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |

**Note on SlashJudge pattern change:** The current design recovers an address via `ecrecover`, then looks it up in `CapacityBond`. With `SignatureChecker`, the challenger provides the `challengedNode` address (the node's Ethereum address / Safe address); the contract verifies the signature against that address, then confirms the address is registered. This is equivalent and avoids the recover-then-lookup pattern, which does not work for ERC-1271: a smart-account signature has no recovery, only validation.

#### Gas Impact

| Signer Type | Additional Gas vs `ecrecover` | Acceptable? |
| --- | --- | --- |
| EOA | +2,600 (EXTCODESIZE check) | Yes — negligible |
| Safe (1-of-1) | +~12,000 (external call + `checkSignatures`) | Yes — on-chain verification happens only at channel close/dispute/slash, not per-voucher |
| Safe (2-of-3) | +~15,000 | Yes — same rationale |

### Wallet Support — EOA Default, Safe Supported

The encrypted EOA keystore is deCDN's documented default wallet for both node operators and clients. Safe — and any other ERC-1271-compliant smart account — is **supported**: because `SignatureChecker` is wired at every verification site, a Safe works as `msg.sender`, `channel.client`, or `channel.voucherSigner` on every on-chain path with no deCDN feature work, no config surface, and no setup tooling.

deCDN does not recommend one. A 1-of-1 Safe carries the same trust posture as the single software-held key that owns it, and the multi-owner threshold that would buy real security cannot be reached on the hot path (below). The choice is the operator's, and the retained contract-level `SignatureChecker` is what makes it free.

#### Node Operators

**Default: an EOA keystore on the signing host.** **Production: 2-of-3 Safe + session keys (see [§ Session Keys — Deferred to Production via ERC-7579 smartsessions](#session-keys--deferred-to-production-via-erc-7579-smartsessions)).**

- **Hot signing (`slash_sig`) — the binding constraint.** Nodes sign a `slash_sig` on every `ProbeResponse` / `StreamResponse`. A paying client verifies the `StreamResponse` signature **off-chain**, recovering the signer and comparing it against the delivering node's registered address; the probe leg is shape-checked only today, with full attribution left to the on-chain `SlashJudge`. A Safe owner-key signature recovers to the *owner*, not to the Safe, so a Safe-addressed operator's stream responses are rejected until off-chain ERC-1271 verification lands ([§ Off-Chain ERC-1271 Verification](#off-chain-erc-1271-verification)) — the node can be probed but cannot be paid. Multi-owner thresholds are separately infeasible here: `SignatureChecker` → Safe's `checkSignatures` cannot reach a quorum at wire speed, and pre-approving every digest via `signMessage` is impractical.
- **Cold paths are wallet-agnostic.** Bond management (`CapacityBond.bond(...)` + `declareMbps(...)`, `registerNode()`, `deregisterNode()`, `requestUnbond(...)`, `unbond()`) and node-to-node channel operations (`openChannel()`, `closeChannel()`, `topUp()`) are all on-chain and infrequent, so `SignatureChecker` accepts an EOA or a Safe of any threshold. But the registered address is also the address a node serves under — `registerNode` binds `msg.sender` — so there is no split where a Safe custodies the bond while an EOA serves. Holding the on-chain identity in a Safe today means holding a **non-serving** identity: the bond and earnings are multisig-protected, and the node cannot complete a paid delivery until the deferred off-chain path lands.
- **Hot signing (`slash_sig`) — Production:** 2-of-3 Safe with a session key authorized via `erc7579/smartsessions` (see [§ Session Keys — Deferred to Production via ERC-7579 smartsessions](#session-keys--deferred-to-production-via-erc-7579-smartsessions)). Multisig protects stake/withdraw/channel-open; the session key signs `slash_sig` at wire speed without a quorum per message.

#### Clients

**Default: an EOA keystore.** High-value client accounts migrate to 2-of-3 + session keys in Production ([§ Session Keys — Deferred to Production via ERC-7579 smartsessions](#session-keys--deferred-to-production-via-erc-7579-smartsessions)); the same threshold constraint that applies to node operators applies here.

- **Channel operations:** The EOA (or Safe) deposits USDC into `PaymentChannel.openChannel()`. That address is the `channel.client` — the funder — and `SignatureChecker` makes the choice transparent to every contract.
- **Voucher signing:** EIP-712 vouchers are signed by the channel's pinned `voucherSigner`, which the funder fixes at open and which defaults to the funder itself. On-chain, `SignatureChecker` validates against it (EOA → ECDSA; 1-of-1 Safe → `checkSignatures` via the stock handler). **Off-chain, nodes verify vouchers and `BindNodeId` ephemeral bindings by recovery only, against that pinned signer** — so a channel whose *signer* is a smart account cannot be served, and its binding is rejected fail-closed ([§ Off-Chain ERC-1271 Verification](#off-chain-erc-1271-verification)).
- **A smart-account funder is servable today.** Because the funder and the signer are separate roles, a Safe can fund a channel and pin a plain EOA as its `voucherSigner`. The Safe custodies the deposit and receives the refund; the vouchers recover by `ecrecover` and clear the off-chain path unchanged. The residual gap is narrower than a wallet choice: it is a channel that names a smart account as the *signer*. The session-key path — signing at delivery speed via `erc7579/smartsessions` without exposing the Safe owner key — lands with the Production plan below, and is what a smart account signing for itself needs.

### Session Keys — Deferred to Production via ERC-7579 smartsessions

**Session keys are out of scope for the PoC.** The PoC uses the direct key-signature path from [§ Wallet Support — EOA Default, Safe Supported](#wallet-support--eoa-default-safe-supported) (a software-held key on the signing host).

#### Production plan

High-frequency signing (node `slash_sig`, client vouchers) migrates to a standardized ERC-7579 session-key module:

- **Account type:** Safe with the [Safe-7579 adapter](https://github.com/rhinestonewtf/safe7579) — turns a Safe into an ERC-7579 modular account while preserving its owner model and deployed address.
- **Session-key module:** [`erc7579/smartsessions`](https://github.com/erc7579/smartsessions) — a standardized ERC-7579 session-key validator that natively implements ERC-1271 `isValidSignature` for session-key-authorized digests. Because the module is an ERC-7579 *validator*, `isValidSignature` on a Safe-7579 account routes through it without any custom fallback handler on deCDN's side; `SignatureChecker.isValidSignatureNow(safe, digest, sessionKeySig)` returns the ERC-1271 magic value once a session is enabled.
- **Policies:** smartsessions' existing policy system covers what this ADR would otherwise have had to invent — time windows, action policies scoped to selectors / EIP-712 domains, per-session spending caps, and first-class revocation via `removeSession`. A follow-up ADR will pin down the specific policy encodings deCDN uses for node and client sessions.

##### Production authorization flow (sketch)

```
Safe (2-of-3) owners →
  smartsessions.enableSession(sessionKey, policies)   (one multisig tx) →
  session key signs slash_sig / vouchers at wire speed →
  owners rotate or revoke via smartsessions.removeSession.
```

No bespoke Safe module, no custom fallback handler, no new security-critical contract code owned by deCDN.

### Off-Chain ERC-1271 Verification

[ADR 012](012-client.md#adr-012-client-architecture-bootstrap-and-trust-model) specifies that nodes verify client ephemeral binding signatures (`BindNodeId` with `nonce=0`) off-chain, without an on-chain call.

**PoC — EOA only.** Every signature deCDN produces or verifies off-chain — the node's `slash_sig`, client vouchers, `BindNodeId` bindings, and cooperative-close waivers — is the fixed 65-byte secp256k1 `r‖s‖v` form, verified by recovering the signer and comparing against the expected address. Verifiers reject any other length **fail-closed**. A smart-account signer is therefore un-servable off-chain even though the contracts accept it on-chain, and a party whose registered address is a smart account cannot be verified by recovery at all. On the buyer side this is avoidable without the branch: the funder pins an EOA as the channel's `voucherSigner`, and only the signer is ever recovered. On the operator side it is not — `registerNode` binds `msg.sender`, and requesters recover `slash_sig` against that registered address, so a Safe-addressed operator cannot serve traffic.

**Production.** When smart-account clients arrive, nodes gain an ERC-1271 branch: probe the signer address for code, fall through to recovery when it is empty, and otherwise call `isValidSignature(bytes32,bytes)` on the signer and compare against the ERC-1271 magic value. The cost is one L2 RPC round-trip per client connection at binding time, not per message; nodes SHOULD cache the per-address code result, which does not change after deployment (ignoring `SELFDESTRUCT`, deprecated and irrelevant for Safe wallets).

This branch is node-side and additive. It verifies against contracts that already ship `SignatureChecker`, so it rolls out node by node with no coordinated migration — nothing about deferring it makes it harder to add. It is also not useful on its own: a smart-account client could bind but could not sustainably sign vouchers at the per-MB cadence with an owner key, so it belongs with the session-key work above rather than ahead of it.

### Safe Infrastructure on the Canonical Testnet

deCDN deploys none of the following and depends on none of it. It is recorded because it is what makes "supported" free: the infrastructure an operator or client would need to run a Safe is already deployed on the testnet sibling of the canonical L2 (Arbitrum Sepolia, per [Appendix: L2 Deployment](appendix-l2-deployment.md#appendix-production-l2-deployment-target)), so choosing a Safe costs deCDN nothing.

| Contract | Status | Notes |
| --- | --- | --- |
| Safe Singleton (v1.4.1) | Deployed | Core Safe logic |
| Safe Proxy Factory | Deployed | Deterministic Safe deployment |
| Safe Singleton Factory | Deployed | `CREATE2` deployment |
| Compatibility Fallback Handler | Deployed | Stock ERC-1271 routing for an operator or client who chooses a Safe |
| Multi Send | Deployed | Batch transactions |

#### Not required for PoC

- ERC-4337 Entry Point interaction — operators submit transactions directly via Safe SDK
- Paymaster contracts — operators hold ETH for gas (same as current [ADR 003](003-payments.md#adr-003-payment-model) assumption)
- Bundler infrastructure
- Safe-7579 adapter and any session-key module (deferred to Production per [§ Session Keys — Deferred to Production via ERC-7579 smartsessions](#session-keys--deferred-to-production-via-erc-7579-smartsessions))

#### Production additions (documented, deferred)

- [Safe-7579 adapter](https://github.com/rhinestonewtf/safe7579) on each participating Safe, unlocking ERC-7579 modules
- [`erc7579/smartsessions`](https://github.com/erc7579/smartsessions) session-key module (ERC-1271 validator)
- ERC-4337 paymaster for gas-in-USDC (eliminates ETH requirement for clients)
- Bundler integration for UserOperation submission

## Consequences

### Positive

- All deCDN contracts support smart account wallets from day one, eliminating a future retrofit across every verification site.
- PoC ships no bespoke Safe modules or custom fallback handlers — stock `CompatibilityFallbackHandler` + `SignatureChecker` are sufficient for 1-of-1 Safes and EOAs.
- Production adopts a standardized, audited session-key module (`erc7579/smartsessions`) rather than deCDN-owned security-critical contract code.
- No contract-level changes beyond the mechanical `ECDSA.recover` → `SignatureChecker` replacement.
- EOA users are unaffected — `SignatureChecker` is a transparent superset of `ECDSA.recover`.
- The approach is wallet-agnostic at the contract level. Any ERC-1271-compliant smart account, Safe included, works without contract changes — deCDN recommends and tools none of them.
- Operator onboarding is one `decdn key-gen` keystore. No wallet-choice decision and no Safe deployment sit on the critical path to serving traffic.

### Negative

- Hot-signing parties (nodes, clients) hold a single software-held key. Multisig protection of the high-frequency signing path is deferred to Production; a multi-owner Safe is viable only on the infrequent stake/withdraw/channel-open paths, for parties who choose that split themselves.
- Smart-account clients and smart-account-addressed node operators cannot participate off-chain. The contracts accept them; the node's off-chain verifiers do not, and reject anything but the 65-byte recovery form fail-closed ([§ Off-Chain ERC-1271 Verification](#off-chain-erc-1271-verification)). Closing that gap is Production work, gated on the session-key path that makes it useful.
- The Production off-chain ERC-1271 branch will cost an L2 RPC round-trip at client binding time (~100–200ms), one-time per connection. Acceptable, but it is latency the recovery-only path does not pay.
- Gas overhead for smart account signature verification is higher than pure `ecrecover` (+10–15k gas per verification). Applies only to on-chain operations (channel close/dispute, slash submission), not the high-frequency off-chain signing path.
- Production rollout requires coordinated migration to Safe-7579 + smartsessions on every participating wallet; operators running on the PoC path must rotate to the new account type as part of the cutover.
