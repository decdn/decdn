# ADR 024: Account Abstraction and Safe Smart Wallet Support

**Date:** 2026-04-09
**Status:** Draft

## Context

Three independent design pressures converge on the need for smart-account support from the PoC:

1. **Hardware wallet voucher signing is infeasible.** At the default 1 MB voucher cadence, a 100 MB download requires 100 EIP-712 signatures, each requiring physical confirmation on a hardware wallet (2–5 seconds). [ADR 012](012-client.md) identified this problem and proposed a derived hot key (HKDF from hardware wallet signature). PR 196 proposed adding `setDelegate`/`clearDelegate` to `StablePaymentChannel` to authorize that hot key on-chain. This works but is a narrow contract-level workaround for one use case.

2. **Node operators need multisig security.** Nodes stake significant TOKEN and accumulate USDC earnings. A single EOA controlling staked funds is a single point of compromise. Safe multisig wallets are the industry standard for securing protocol-managed funds — most DeFi operators use them. Deferring this to production means the PoC cannot demonstrate the intended security model.

3. **All signature verification should support smart accounts.** The current contract designs use `ECDSA.recover` / `ecrecover` exclusively, which only works with EOAs. Any smart account (Safe, Kernel, Biconomy, etc.) produces signatures that must be verified via ERC-1271 (`isValidSignature`). Baking in EOA-only verification now means retrofitting every verification site later.

Rather than solving problem 1 with a delegated-signer patch (PR 196) and deferring problems 2 and 3, this ADR adopts a unified approach: **ERC-1271 support in all contracts from day one, Safe as the recommended wallet for both node operators and clients, and Safe Session Key Modules for high-frequency signing.**

This supersedes PR 196's `setDelegate`/`clearDelegate` approach. Session keys via Safe modules provide the same functionality (authorizing a hot key for voucher/slash_sig signing) with better security properties (time-bounded, scope-limited, multi-sig authorized, revocable without on-chain payment channel transactions).

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
| `disputeChannel` — voucher signature | Same as `closeChannel` | Same |

**StakingRegistry ([ADR 003](003-payments.md)):**

| Function | Current | After |
| --- | --- | --- |
| `registerNode` — `bindingSignature` | `ECDSA.recover(digest, sig) == msg.sender` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |
| `bindNodeId` — `signature` | `ECDSA.recover(digest, sig) == msg.sender` | `SignatureChecker.isValidSignatureNow(msg.sender, digest, sig)` |

**SlashJudge ([ADR 014](014-on-chain-verification.md)):**

| Function | Current | After |
| --- | --- | --- |
| `submitPhantomChallenge` | `ecrecover` → address A, `ecrecover` → address B, verify A == B | `SignatureChecker.isValidSignatureNow(challengedNode, probeDigest, probeSig)` + `SignatureChecker.isValidSignatureNow(challengedNode, streamDigest, streamSig)` |
| `submitRateChallenge` | Same pattern as phantom | Same pattern |
| `submitBlacklistChallenge` | `ecrecover` → address, verify registered | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |
| `submitCorruptionChallenge` | `ecrecover` → address, verify registered | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |
| `counterChallenge` (rate) | `ecrecover` → verify same node | `SignatureChecker.isValidSignatureNow(challengedNode, digest, sig)` |

**Note on SlashJudge pattern change:** The current design recovers an address from `ecrecover` and then looks it up in `StakingRegistry`. With `SignatureChecker`, the pattern becomes: the challenger provides the `challengedNode` address (the node's Ethereum address / Safe address), the contract verifies the signature against that address, then confirms the address is registered. This is equivalent but avoids the "recover then lookup" pattern which does not work for ERC-1271 (there is no "recovery" from a smart account signature — only validation).

**DeliveryReceipt counter-evidence ([ADR 014](014-on-chain-verification.md)):** The `counterChallenge` for corruption challenges verifies a `DeliveryReceipt` signed by the requester. If the requester is a smart account, this verification also uses `SignatureChecker`.

#### Gas Impact

| Signer Type | Additional Gas vs `ecrecover` | Acceptable? |
| --- | --- | --- |
| EOA | +2,600 (EXTCODESIZE check) | Yes — negligible |
| Safe (1-of-1) | +~12,000 (external call + Safe module routing) | Yes — on-chain verification happens only at channel close/dispute/slash, not per-voucher |
| Safe (2-of-3) | +~15,000 | Yes — same rationale |

### 2. Safe as Recommended Wallet

Safe smart wallets are the **recommended** wallet type for both node operators and clients. EOAs remain fully functional — `SignatureChecker` makes this transparent at the contract level.

#### Node Operators

**Recommended configuration: 2-of-3 Safe multisig.**

- **Owners:** 2-of-3 threshold. Typical setup: operator key, backup key, hardware wallet.
- **Stake management:** The Safe holds TOKEN and executes `StakingRegistry.stake()`, `registerNode()`, `deregister()`. Multisig approval protects staked funds.
- **Channel operations:** The Safe executes `openChannel()`, `closeChannel()`, `topUp()` for outbound node-to-node payment channels (cache-miss pulls). These are infrequent transactions — multisig approval is not a bottleneck.
- **Hot signing (slash_sig):** A session key (see Section 3) is authorized by the Safe to produce `slash_sig` signatures at wire speed. The session key runs on the server and is the only key that needs to be hot.

**Why 2-of-3 for PoC:** It demonstrates the intended production security model (multisig protection for staked funds) without adding significant operational overhead. Single-key Safes (1-of-1) are also supported for operators who prefer simplicity.

#### Clients

**Recommended configuration: 1-of-1 Safe** (single owner) for simplicity, or **2-of-3** for high-value accounts.

- **Channel operations:** The Safe deposits USDC into `StablePaymentChannel.openChannel()`. The Safe address is the `channel.client`.
- **Voucher signing:** A session key (see Section 3) is authorized for EIP-712 voucher signatures. The session key runs in the client process.
- **Priority staking:** If the client stakes TOKEN for priority ([ADR 003](003-payments.md)), the Safe holds the staked TOKEN.

**PoC allowance:** Clients MAY use a plain EOA for the PoC. The client software supports both — `SignatureChecker` makes this transparent. Safe is recommended but not required.

### 3. Session Keys via Safe Modules

Session keys replace both the derived hot key scheme ([ADR 012](012-client.md), lines 103–117) and PR 196's `setDelegate`/`clearDelegate` approach.

#### Concept

A **session key** is a lightweight secp256k1 key pair generated at process startup and held in memory only (never written to disk). The Safe owners authorize this key via a Safe module, granting it permission to produce EIP-712 signatures on behalf of the Safe within defined constraints.

When a contract calls `SignatureChecker.isValidSignatureNow(safeAddress, digest, sessionKeySig)`, the Safe's `isValidSignature` implementation routes through its enabled modules. The Session Key Module validates that:

1. The session key is authorized
2. The current time is within the key's validity window
3. The operation is within the key's authorized scope

If all checks pass, the module confirms the signature is valid. The contract never needs to know about session keys — it just sees a valid signature from the Safe address.

#### Session Key Authorization

**For node operators (slash_sig signing):**

```
Safe owners (2-of-3) → enable Session Key Module → authorize session key:
  - key:        <server-generated secp256k1 public key>
  - validAfter: <node startup timestamp>
  - validUntil: <startup + 7 days>  (renewable)
  - scope:      EIP-712 signatures for SlashJudge domain
                (ProbeResponse, StreamResponse type hashes)
```

The session key is authorized once at node startup (one multisig transaction). It then signs `slash_sig` fields at wire speed for the validity window. Renewal requires another multisig transaction — operators SHOULD automate this via a Safe transaction queue.

**For clients (voucher signing):**

```
Safe owner(s) → enable Session Key Module → authorize session key:
  - key:        <client-process-generated secp256k1 public key>
  - validAfter: <session start timestamp>
  - validUntil: <start + 24 hours>  (configurable)
  - scope:      EIP-712 signatures for StablePaymentChannel domain
                (Voucher type hash)
```

The session key signs vouchers at delivery speed. It cannot execute channel operations (`openChannel`, `closeChannel`, `topUp`) — those require the Safe owner(s).

#### Comparison with PR 196's Delegated Signer

| Aspect | PR 196 (`setDelegate`/`clearDelegate`) | Safe Session Keys (this ADR) |
| --- | --- | --- |
| Contract changes needed | New `delegate` mapping + 2 functions in `StablePaymentChannel` | None — authorization lives in the Safe's module system |
| Revocation | On-chain `clearDelegate` transaction | Safe owners remove key via module — no payment channel tx |
| Scope limitation | Delegate can sign any voucher for any channel | Session key scoped to specific EIP-712 domains, type hashes, time windows |
| Multi-sig approval | Not built in — single EOA calls `setDelegate` | Safe owners (multisig) must approve the session key |
| Time bounding | No expiry — delegate is permanent until cleared | Built-in `validAfter` / `validUntil` |
| Multiple delegates | One per client address | Multiple session keys supported (e.g., separate keys per device) |

#### Session Key Module

**Preferred:** The [Safe Session Key Module](https://github.com/safe-global/safe-modules) from Safe{Core} Protocol, if deployed on Arbitrum Sepolia.

**Fallback:** If the official module is not available on Arbitrum Sepolia, deploy a custom minimal module:

```solidity
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {ISafe} from "@safe-global/safe-contracts/contracts/interfaces/ISafe.sol";

/// @title Minimal Session Key Module for deCDN
/// @notice Authorizes time-bounded session keys to produce EIP-712 signatures
///         on behalf of a Safe. Does NOT execute transactions — only validates
///         signatures via ERC-1271.
contract SessionKeyModule {
    struct SessionKey {
        uint48 validAfter;
        uint48 validUntil;
    }

    // safe => session key address => session key config
    mapping(address => mapping(address => SessionKey)) public sessionKeys;

    event SessionKeyAdded(address indexed safe, address indexed key, uint48 validAfter, uint48 validUntil);
    event SessionKeyRemoved(address indexed safe, address indexed key);

    /// @notice Authorize a session key. Must be called via Safe's execTransactionFromModule.
    function addSessionKey(address key, uint48 validAfter, uint48 validUntil) external {
        // msg.sender is the Safe (called via delegatecall or module exec)
        require(validUntil > validAfter, "invalid validity window");
        sessionKeys[msg.sender][key] = SessionKey(validAfter, validUntil);
        emit SessionKeyAdded(msg.sender, key, validAfter, validUntil);
    }

    /// @notice Revoke a session key.
    function removeSessionKey(address key) external {
        delete sessionKeys[msg.sender][key];
        emit SessionKeyRemoved(msg.sender, key);
    }

    /// @notice Check if a key is a valid session key for the given Safe.
    function isValidSessionKey(address safe, address key) public view returns (bool) {
        SessionKey memory sk = sessionKeys[safe][key];
        return sk.validUntil > 0
            && block.timestamp >= sk.validAfter
            && block.timestamp <= sk.validUntil;
    }
}
```

This module is ~50 lines and covers the PoC requirements. The Safe's `isValidSignature` implementation must be configured to check this module when validating signatures from authorized session keys. In practice, the Safe's fallback handler routes `isValidSignature` calls through enabled modules.

**Deployment:** The module is deployed once per network (singleton pattern). Each Safe enables it via `enableModule()`.

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

### 5. Safe Infrastructure on Arbitrum Sepolia

The following Safe infrastructure is already deployed on Arbitrum Sepolia:

| Contract | Status | Notes |
| --- | --- | --- |
| Safe Singleton (v1.4.1) | Deployed | Core Safe logic |
| Safe Proxy Factory | Deployed | Deterministic Safe deployment |
| Safe Singleton Factory | Deployed | `CREATE2` deployment |
| Compatibility Fallback Handler | Deployed | ERC-1271 routing |
| Multi Send | Deployed | Batch transactions |

**Not required for PoC:**

- ERC-4337 Entry Point interaction — operators submit transactions directly via Safe SDK
- Paymaster contracts — operators hold ETH for gas (same as current [ADR 003](003-payments.md) assumption)
- Bundler infrastructure

**Production additions (documented, deferred):**

- ERC-4337 paymaster for gas-in-USDC (eliminates ETH requirement for clients)
- Bundler integration for UserOperation submission
- Advanced session key policies (per-channel spending limits, rate limits)

### 6. PoC vs Production Scope

| Capability | PoC | Production |
| --- | --- | --- |
| `SignatureChecker` in all contracts | Yes | Yes |
| Safe wallet support (EOA + smart account) | Yes | Yes |
| Session Key Module | Yes (minimal or official) | Yes (official Safe module with advanced policies) |
| Off-chain ERC-1271 verification | Yes | Yes |
| ERC-4337 paymaster (gas-in-USDC) | No — operators hold ETH | Yes |
| ERC-4337 bundler integration | No — direct Safe SDK tx | Yes |
| Per-channel session key spending caps | No | Yes |
| Mobile/web Safe integration | No | Evaluated separately |

## Consequences

**Positive:**

- All deCDN contracts support smart account wallets from day one, eliminating a future retrofit across every verification site.
- Node operators can protect staked TOKEN with multisig security (2-of-3 Safe), demonstrating the intended production security model in the PoC.
- Session keys provide a cleaner, more secure solution to high-frequency signing than PR 196's delegated signer: time-bounded, scope-limited, multi-sig authorized, and revocable without payment channel contract transactions.
- No contract-level changes beyond the mechanical `ECDSA.recover` → `SignatureChecker` replacement. The `StablePaymentChannel` contract is *simpler* than PR 196's version (no `delegate` mapping, no `setDelegate`/`clearDelegate` functions).
- EOA users are unaffected — `SignatureChecker` is a transparent superset of `ECDSA.recover`.
- The approach is wallet-agnostic at the contract level. While Safe is recommended, any ERC-1271-compliant smart account (Kernel, Biconomy, etc.) works without contract changes.

**Negative:**

- Safe wallet setup is more complex than generating an EOA. Operator tooling (`decdn-node setup`) must guide Safe creation, module enablement, and session key authorization.
- Off-chain ERC-1271 verification requires an RPC call to the L2, adding latency at client binding time (~100–200ms). This is one-time per connection and acceptable.
- The Session Key Module (if custom) adds a deployable contract to the PoC scope. The minimal module is ~50 lines and low-risk, but it is still custom code that needs review.
- Gas overhead for smart account signature verification is higher than pure `ecrecover` (+10–15k gas per verification). This applies only to on-chain operations (channel close/dispute, slash submission) — not to the high-frequency off-chain voucher signing path.
- Operators must manage session key renewal (re-authorization before expiry). If a session key expires mid-operation, the node cannot produce `slash_sig` until renewed. Nodes SHOULD monitor key expiry and alert operators.

## Amendments to Existing ADRs

This ADR amends the following:

- **[ADR 003](003-payments.md):** `SignatureChecker` replaces `ECDSA.recover` in voucher verification and `bindNodeId`. "Gasless Channel Opens" section updated — ERC-1271 is PoC, paymaster deferred. PR 196's `setDelegate`/`clearDelegate` amendment is superseded.
- **[ADR 012](012-client.md):** Ethereum key management restructured — Safe wallet as recommended option; derived hot key section removed (replaced by session keys); open question 2 (delegated voucher signer) resolved; ephemeral binding verification updated for ERC-1271.
- **[ADR 014](014-on-chain-verification.md):** All `ecrecover` sites in `SlashJudge` updated to `SignatureChecker`. Challenger provides target node address; contract verifies signature against it.
- **[ADR 016](016-contract-interactions.md):** `SignatureChecker` added to OpenZeppelin framework usage table.
- **[ADR 019](019-node-onboarding.md):** "ETH gas sponsor" PoC simplification updated. "Delegated voucher signer" future work item resolved.

## ADRs Affected

- [ADR 003 — Payment Model](003-payments.md): Signature verification, gasless channel opens
- [ADR 012 — Client Architecture](012-client.md): Key management, identity lifecycle
- [ADR 014 — On-Chain Verification](014-on-chain-verification.md): SlashJudge signature verification
- [ADR 016 — Smart Contract Interactions](016-contract-interactions.md): OZ framework table
- [ADR 019 — Node Onboarding](019-node-onboarding.md): Wallet setup, delegated signer resolution
