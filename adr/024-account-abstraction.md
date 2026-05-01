# ADR 024: Account Abstraction and Safe Smart Wallet Support

**Date:** 2026-04-09
**Status:** Draft

## Context

Three independent design pressures converge on the need for smart-account support from the PoC:

1. **High-frequency signing needs a hot key.** At the default 1 MB voucher cadence, a 100 MB download requires 100 EIP-712 signatures, each requiring physical confirmation on a hardware wallet (2–5 seconds); nodes sign a `slash_sig` on every `ProbeResponse` / `StreamResponse`. Hardware-wallet-only operation is infeasible at that cadence regardless of wallet model. [ADR 012](012-client.md) identified this and explored a derived hot key (HKDF from hardware wallet signature) as a narrow workaround.

2. **Node operators need multisig security.** Nodes stake significant TOKEN and accumulate USDC earnings. A single EOA controlling staked funds is a single point of compromise. Safe multisig wallets are the industry standard for securing protocol-managed funds — most DeFi operators use them. Deferring this to production means the PoC cannot demonstrate the intended security model.

3. **All signature verification should support smart accounts.** The current contract designs use `ECDSA.recover` / `ecrecover` exclusively, which only works with EOAs. Any smart account (Safe, Kernel, Biconomy, etc.) produces signatures that must be verified via ERC-1271 (`isValidSignature`). Baking in EOA-only verification now means retrofitting every verification site later.

This ADR splits the response along a sharp PoC vs Production line:

- **PoC:** solve problems 2 and 3 now — adopt `SignatureChecker` across every contract (problem 3) and make Safe the recommended wallet for node operators and clients (problem 2). Leave problem 1 on the existing footing: a software-held signing key on the signing host (same trust posture as today's `eth_keystore`), wrapped by a 1-of-1 Safe or used as a plain EOA.
- **Production:** solve problem 1 cleanly by migrating to ERC-7579 smart accounts (Safe via the Safe-7579 adapter) and installing [`erc7579/smartsessions`](https://github.com/erc7579/smartsessions) — a standardized session-key module with native ERC-1271 validation, policy enforcement (time windows, allowed selectors/domains, spending caps), and first-class revocation.

Deferring session keys to Production keeps the PoC PR small and avoids shipping bespoke security-critical contract code for a problem that has a standardized upstream solution.

## Decision

### 1. Universal `SignatureChecker` in All Contracts

Every signature verification site across all deCDN contracts MUST use OpenZeppelin's [`SignatureChecker`](https://docs.openzeppelin.com/contracts/5.x/api/utils#SignatureChecker) library instead of direct `ECDSA.recover` or `ecrecover`.

`SignatureChecker.isValidSignatureNow(signer, digest, signature)` transparently handles both:

- **EOA signers:** falls through to `ECDSA.recover` (3,000 gas via `ecrecover` precompile + ~2,600 gas for `EXTCODESIZE` check)
- **Smart account signers:** calls `IERC1271.isValidSignature(digest, signature)` on the signer contract (~10,000–15,000 gas for a Safe wallet)

This is a mechanical replacement. The EIP-712 domain separators, typed data hashes, and voucher formats are unchanged.

#### Verification Sites Affected

**StablePaymentChannel ([ADR 003](003-payments.md)):**

| Function | Current | After |
| --- | --- | --- |
| `closeChannel` — voucher signature | `ECDSA.recover(digest, sig) == channel.client` | `SignatureChecker.isValidSignatureNow(channel.client, digest, sig)` |

`disputeChannel` validates the voucher signature using the same scheme as `closeChannel` and migrates the same way.

**StakingRegistry ([ADR 003](003-payments.md)):**

| Function | Current | After |
| --- | --- | --- |
| `registerNode` — `bindingSignature` | `ECDSA.recover(digest, sig) == msg.sender` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |
| `bindNodeId` — `signature` | `ECDSA.recover(digest, sig) == msg.sender` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |

**SlashJudge ([ADR 014](014-on-chain-verification.md)):**

| Function | Current | After |
| --- | --- | --- |
| `submitPhantomChallenge` | `ecrecover` → address A, `ecrecover` → address B, verify A == B | `SignatureChecker.isValidSignatureNow(challengedNode, probeDigest, probeSig)` + `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSig)` |
| `submitBlacklistChallenge` | `ecrecover` → address, verify registered | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |
| `submitCorruptionChallenge` | `ecrecover` → address, verify registered | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |
| `counterChallenge` (rate) | `ecrecover` → verify same node | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |

**Note on SlashJudge pattern change:** The current design recovers an address from `ecrecover` and then looks it up in `StakingRegistry`. With `SignatureChecker`, the pattern becomes: the challenger provides the `challengedNode` address (the node's Ethereum address / Safe address), the contract verifies the signature against that address, then confirms the address is registered. This is equivalent but avoids the "recover then lookup" pattern which does not work for ERC-1271 (there is no "recovery" from a smart account signature — only validation).

**DeliveryReceipt counter-evidence ([ADR 014](014-on-chain-verification.md)):** The `counterChallenge` for corruption challenges verifies a `DeliveryReceipt` signed by the requester. If the requester is a smart account, this verification also uses `SignatureChecker`.

#### Gas Impact

| Signer Type | Additional Gas vs `ecrecover` | Acceptable? |
| --- | --- | --- |
| EOA | +2,600 (EXTCODESIZE check) | Yes — negligible |
| Safe (1-of-1) | +~12,000 (external call + `checkSignatures`) | Yes — on-chain verification happens only at channel close/dispute/slash, not per-voucher |
| Safe (2-of-3) | +~15,000 | Yes — same rationale |

### 2. Safe as Recommended Wallet

Safe smart wallets are the **recommended** wallet type for both node operators and clients. EOAs remain fully functional — `SignatureChecker` makes this transparent at the contract level.

#### Node Operators

**Recommended PoC configuration: 1-of-1 Safe.** **Production: 2-of-3 + session keys (see §3).**

- **Owners (PoC):** 1-of-1. The single owner is a software-held key on the signing host — same trust posture as today's `eth_keystore`. 2-of-3 is not recommended for the PoC because `SignatureChecker` → Safe's `checkSignatures` cannot reach a multi-owner threshold at wire speed: every `slash_sig` would need signatures from multiple owners, and pre-approving every digest via `signMessage` is infeasible.
- **Stake management:** The Safe holds TOKEN and executes `StakingRegistry.stake()`, `registerNode()`, `deregister()`.
- **Channel operations:** The Safe executes `openChannel()`, `closeChannel()`, `topUp()` for outbound node-to-node payment channels (cache-miss pulls). Infrequent — not on the hot path.
- **Hot signing (slash_sig) — PoC:** The 1-of-1 owner signs the EIP-712 digest directly. `SignatureChecker.isValidSignatureNow(safeAddress, digest, sig)` routes through Safe's stock `CompatibilityFallbackHandler` → `checkSignatures`, which passes at threshold 1.
- **Hot signing (slash_sig) — Production:** 2-of-3 Safe with a session key authorized via `erc7579/smartsessions` (see §3). Multisig protects stake/withdraw/channel-open; the session key signs `slash_sig` at wire speed without quorum per message.

#### Clients

**Recommended PoC configuration: 1-of-1 Safe or plain EOA.** High-value client accounts migrate to 2-of-3 + session keys in Production (§3); the same threshold constraint that applies to node operators applies here.

- **Channel operations:** The Safe (or EOA) deposits USDC into `StablePaymentChannel.openChannel()`. That address is the `channel.client`.
- **Voucher signing — PoC:** The client process signs EIP-712 vouchers with its owner key directly. `SignatureChecker` on the channel contract validates against `channel.client` (EOA → ECDSA; 1-of-1 Safe → `checkSignatures` via stock handler).
- **Voucher signing — Production:** A session key authorized via `erc7579/smartsessions` signs vouchers at delivery speed without exposing the Safe owner key (see §3).
- **Priority staking:** If the client stakes TOKEN for priority ([ADR 003](003-payments.md)), the wallet holds the staked TOKEN.

**PoC allowance:** Clients MAY use a plain EOA; the client software supports both. `SignatureChecker` makes the wallet type transparent to every contract.

### 3. Session Keys — Deferred to Production via ERC-7579 smartsessions

**Session keys are out of scope for the PoC.** The PoC uses the direct owner-signature path described in §2 (1-of-1 Safe or EOA, software-held key on the signing host). This matches today's `eth_keystore` trust posture and keeps the PoC from shipping bespoke security-critical contract code.

**Production plan.** High-frequency signing (node `slash_sig`, client vouchers) migrates to a standardized ERC-7579 session-key module:

- **Account type:** Safe with the [Safe-7579 adapter](https://github.com/rhinestonewtf/safe7579) — turns a Safe into an ERC-7579 modular account while preserving its owner model and deployed address.
- **Session-key module:** [`erc7579/smartsessions`](https://github.com/erc7579/smartsessions) — a standardized ERC-7579 session-key validator that natively implements ERC-1271 `isValidSignature` for session-key-authorized digests. Because the module is an ERC-7579 *validator*, `isValidSignature` on a Safe-7579 account routes through it without any custom fallback handler on deCDN's side; `SignatureChecker.isValidSignatureNow(safe, digest, sessionKeySig)` returns the ERC-1271 magic value once a session is enabled.
- **Policies:** smartsessions' existing policy system covers what this ADR would otherwise have had to invent — time windows, action policies scoped to selectors / EIP-712 domains, per-session spending caps, and first-class revocation via `removeSession`. A follow-up ADR will pin down the specific policy encodings deCDN uses for node and client sessions.

**Production authorization flow (sketch):**

```
Safe (2-of-3) owners →
  smartsessions.enableSession(sessionKey, policies)   (one multisig tx) →
  session key signs slash_sig / vouchers at wire speed →
  owners rotate or revoke via smartsessions.removeSession.
```

No bespoke Safe module, no custom fallback handler, no new security-critical contract code owned by deCDN.

### 4. Off-Chain ERC-1271 Verification

[ADR 012](012-client.md) specifies that nodes verify client ephemeral binding signatures (`BindNodeId` with `nonce=0`) via `ecrecover`. When the client's Ethereum address is a smart account, this verification must use ERC-1271 instead.

**Node-side verification logic (Rust, using alloy):**

```rust
async fn verify_binding_signature(
    provider: &impl Provider,
    client_address: Address,
    digest: B256,
    signature: &[u8],
) -> Result<bool> {
    // Check if the address has code (is a contract)
    let code = provider.get_code_at(client_address).await?;
    if code.is_empty() {
        // EOA: use ecrecover
        let recovered = signature.recover_address_from_prehash(&digest)?;
        Ok(recovered == client_address)
    } else {
        // Smart account: call isValidSignature(bytes32, bytes)
        let result = IERC1271::new(client_address, provider)
            .isValidSignature(digest, signature.into())
            .call()
            .await?;
        Ok(result == ERC1271_MAGIC_VALUE)
    }
}
```

**Performance:** The `EXTCODESIZE` + potential `isValidSignature` RPC call adds one round-trip to the L2 RPC endpoint. This happens once per client connection (at binding time), not per message. Acceptable latency.

**Caching:** Nodes SHOULD cache the result of `get_code_at` for known client addresses to avoid repeated RPC calls. The code at an address does not change after deployment (ignoring `SELFDESTRUCT`, which is deprecated and irrelevant for Safe wallets).

### 5. Safe Infrastructure on the Canonical Testnet

The following Safe infrastructure is already deployed on the testnet sibling of the canonical L2 (Arbitrum Sepolia, per [Appendix: L2 Deployment](appendix-l2-deployment.md)):

| Contract | Status | Notes |
| --- | --- | --- |
| Safe Singleton (v1.4.1) | Deployed | Core Safe logic |
| Safe Proxy Factory | Deployed | Deterministic Safe deployment |
| Safe Singleton Factory | Deployed | `CREATE2` deployment |
| Compatibility Fallback Handler | Deployed | Stock ERC-1271 routing — used as-is in the PoC |
| Multi Send | Deployed | Batch transactions |

**Not required for PoC:**

- ERC-4337 Entry Point interaction — operators submit transactions directly via Safe SDK
- Paymaster contracts — operators hold ETH for gas (same as current [ADR 003](003-payments.md) assumption)
- Bundler infrastructure
- Safe-7579 adapter and any session-key module (deferred to Production per §3)

**Production additions (documented, deferred):**

- [Safe-7579 adapter](https://github.com/rhinestonewtf/safe7579) on each participating Safe, unlocking ERC-7579 modules
- [`erc7579/smartsessions`](https://github.com/erc7579/smartsessions) session-key module (ERC-1271 validator)
- ERC-4337 paymaster for gas-in-USDC (eliminates ETH requirement for clients)
- Bundler integration for UserOperation submission

### 6. PoC vs Production Scope

| Capability | PoC | Production |
| --- | --- | --- |
| `SignatureChecker` in all contracts | Yes | Yes |
| Safe wallet support (EOA + smart account) | Yes | Yes |
| Recommended Safe threshold for hot-signing parties | 1-of-1 | 2-of-3 (hot signing via session keys) |
| Session keys | No — direct owner-signature path | Yes (via `erc7579/smartsessions` on Safe-7579) |
| Off-chain ERC-1271 verification | Yes | Yes |
| ERC-4337 paymaster (gas-in-USDC) | No — operators hold ETH | Yes |
| ERC-4337 bundler integration | No — direct Safe SDK tx | Yes |
| Per-session spending / action policies | No | Yes (smartsessions policies) |
| Mobile/web Safe integration | No | Evaluated separately |

## Consequences

**Positive:**

- All deCDN contracts support smart account wallets from day one, eliminating a future retrofit across every verification site.
- PoC ships no bespoke Safe modules or custom fallback handlers — stock `CompatibilityFallbackHandler` + `SignatureChecker` are sufficient for 1-of-1 Safes and EOAs.
- Production adopts a standardized, audited session-key module (`erc7579/smartsessions`) rather than deCDN-owned security-critical contract code.
- No contract-level changes beyond the mechanical `ECDSA.recover` → `SignatureChecker` replacement.
- EOA users are unaffected — `SignatureChecker` is a transparent superset of `ECDSA.recover`.
- The approach is wallet-agnostic at the contract level. Safe is recommended; any ERC-1271-compliant smart account works without contract changes.

**Negative:**

- PoC hot-signing parties (nodes, clients) use 1-of-1 Safes (or EOAs). Multisig protection of the high-frequency signing path is deferred to Production; 2-of-3 is only viable on the infrequent stake/withdraw/channel-open paths for parties comfortable with that split.
- Safe wallet setup is more complex than generating an EOA. Operator tooling (`decdn-node setup`) must guide Safe creation and, in Production, Safe-7579 adapter installation + `enableSession`.
- Off-chain ERC-1271 verification requires an RPC call to the L2, adding latency at client binding time (~100–200ms). One-time per connection — acceptable.
- Gas overhead for smart account signature verification is higher than pure `ecrecover` (+10–15k gas per verification). Applies only to on-chain operations (channel close/dispute, slash submission), not the high-frequency off-chain signing path.
- Production rollout requires coordinated migration to Safe-7579 + smartsessions on every participating wallet; operators running on the PoC path must rotate to the new account type as part of the cutover.

## Amendments to Existing ADRs

This ADR amends the following:

- **[ADR 003](003-payments.md):** `SignatureChecker` replaces `ECDSA.recover` in voucher verification and `bindNodeId`. "Gasless Channel Opens" section updated — ERC-1271 is PoC, paymaster deferred. PR 196's `setDelegate`/`clearDelegate` amendment is superseded; the hot-key signing problem is addressed in Production via `erc7579/smartsessions` (§3), not a contract-level delegate.
- **[ADR 012](012-client.md):** Ethereum key management restructured — Safe wallet as recommended option (1-of-1 in PoC); derived hot key section removed; open question 2 (delegated voucher signer) resolved by the Production smartsessions plan (§3); ephemeral binding verification updated for ERC-1271.
- **[ADR 014](014-on-chain-verification.md):** All `ecrecover` sites in `SlashJudge` updated to `SignatureChecker`. Challenger provides target node address; contract verifies signature against it.
- **[ADR 016](016-contract-interactions.md):** `SignatureChecker` added to OpenZeppelin framework usage table.
- **[ADR 019](019-node-onboarding.md):** "ETH gas sponsor" PoC simplification updated. "Delegated voucher signer" future work item resolved by the Production smartsessions plan.

## ADRs Affected

- [ADR 003 — Payment Model](003-payments.md): Signature verification, gasless channel opens
- [ADR 012 — Client Architecture](012-client.md): Key management, identity lifecycle
- [ADR 014 — On-Chain Verification](014-on-chain-verification.md): SlashJudge signature verification
- [ADR 016 — Smart Contract Interactions](016-contract-interactions.md): OZ framework table
- [ADR 019 — Node Onboarding](019-node-onboarding.md): Wallet setup, delegated signer resolution
