//! `alloy::sol!` bindings for the contract write/setup surface the fixtures
//! drive, which the production `decdn-incentive` bindings (read/seller-only)
//! deliberately omit.
//!
//! The pool surface — `openPool`, `getPool`, `getAuthorization`, `getWatermark`,
//! `ownerPoolNonce`, the `Pool`/`Authorization`/`Lane` structs, and the
//! `PoolOpened`/`PoolRedeemed` events — is reused from
//! [`decdn_incentive::payment_pool::PaymentPool`] (the same ABI the runtime
//! decodes) rather than re-declared, so the load-bearing pool layout has a
//! single source of truth. Everything here is buyer/operator/publisher setup
//! the production binding does not add on top.

// The `sol!`-generated bindings use patterns the workspace clippy denies (raw
// indexing into ABI fixed-size byte arrays, `unwrap` on infallible
// conversions). These allows scope the relaxation to this module only.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    clippy::pub_underscore_fields,
    clippy::missing_docs_in_private_items,
    clippy::too_many_arguments,
    missing_debug_implementations,
    missing_docs,
    non_snake_case,
    non_camel_case_types
)]

// Reuse the production pool binding for the `Pool`/`Authorization`/`Lane`
// structs + reads (`getPool`/`getAuthorization`/`getWatermark`) and the
// owner/redeemer writes (`openPool`/`topUp`/`redeemMany`).
pub use decdn_incentive::payment_pool::PaymentPool;
// Reuse the node-side ContentBlacklist binding (read view + membership events +
// governance add/remove writes) — same ABI the runtime watcher decodes.
pub use decdn_incentive::content_blacklist::ContentBlacklist;

alloy::sol! {
    /// Mintable ERC-20 surface (mock USDC + the staking TOKEN). `mint` exists
    /// only on the test MintableUSDC and on Token-with-a-minter; the fixtures
    /// fund accounts through it.
    #[sol(rpc)]
    contract Erc20 {
        function mint(address to, uint256 amount) external;
        function approve(address spender, uint256 amount) external returns (bool);
        function transfer(address to, uint256 amount) external returns (bool);
        function balanceOf(address account) external view returns (uint256);
    }

    /// `CapacityBond` operator-onboarding writes + activation reads.
    #[sol(rpc)]
    contract CapacityBond {
        function bond(uint256 amount) external;
        function pause() external;
        function unpause() external;
        function currentTermsHash() external view returns (bytes32);
        function registerNode(
            bytes32 nodeId,
            bytes multiaddrs,
            string regionHint,
            bytes32 termsHash,
            bytes bindingSignature,
            bytes ed25519Signature
        ) external;
        function isActive(address operator) external view returns (bool);
        // Raw registered-set membership (#1359, G-NODE-06's exit leg). `active`
        // here is `_nodes[operator].active` alone — the flag `deregisterNode`
        // gates on — unlike `isActive`, which also requires bond over the
        // operator's floor. Only `active` is read; the rest of the
        // struct is declared because the ABI decode needs the full shape.
        struct NodeInfo {
            bytes32 nodeId;
            address ethAddress;
            bool active;
            uint64 lastMultiaddrUpdate;
            bytes multiaddrs;
            string regionHint;
        }
        function getNodeByAddress(address ethAddress) external view returns (NodeInfo memory);
        function bindingNonce(address operator) external view returns (uint64);
        function registrationNonce(bytes32 nodeId) external view returns (uint64);
        // Key-rotation surface (#1034, G-NODE-07). `bindNodeId` is the rebind
        // itself; the two raw mappings are what the journey asserts on, and
        // they are read directly rather than through `nodeIdOf`/`isActiveNode`
        // because a rotation swaps the binding alone — an assertion routed
        // through a liveness-aware helper would not be testing the binding.
        function bindNodeId(bytes32 nodeId, bytes bindingSignature, bytes ed25519Signature) external;
        function nodeIdToAddress(bytes32 nodeId) external view returns (address);
        function addressToNodeId(address operator) external view returns (bytes32);
        function deregisterNode() external;
        function updateRegion(string newRegion) external;
        // ADR 030 region-change cooldown / ripening window (seconds). Read so the
        // scope-transition journey can advance past the cooldown before its
        // `updateRegion`, rather than hardcoding the 7-day default.
        function regionStabilityWindow() external view returns (uint256);
        function declareMbps(uint256 mbps) external;
        function bondRequired(uint256 mbps) external view returns (uint256);
        function activeBond(address operator) external view returns (uint256);
        function declaredMbps(address operator) external view returns (uint256);
        function minBond() external view returns (uint256);
        // `firstBondedAt` feeds the Governor age-ramp; an iroh-key rotation
        // keeps it and an Ethereum-key migration resets it (G-NODE-07).
        function firstBondedAt(address operator) external view returns (uint64);
        // Onboarding negatives (#1030, G-NODE-01). Declared so a reverted
        // `registerNode` decodes to a *named* error rather than an opaque
        // selector, and so `crate::assert::expect_revert_anyhow` can match on
        // `E::SELECTOR` — compiler-checked against the ABI instead of a
        // hand-copied hex literal. Same reasoning as the `PublisherRegistry`
        // error block below.
        //
        // The ORDER these fire in is what the journey's negatives depend on:
        // `registerNode` runs `_checkRegistrationPreconditions` (which raises
        // `BondBelowMinimum`) BEFORE `_checkBindingOneToOne` (which raises
        // `NodeIdAlreadyBound`), so an under-bonded impostor never reaches the
        // binding guard.
        error BondBelowMinimum(uint256 bond, uint256 required);
        error BondBelowCurve(uint256 bond, uint256 required);
        /// Someone else's address already owns this node id — the one-to-one
        /// binding rule. `currentOwner` names the operator that holds it.
        error NodeIdAlreadyBound(address currentOwner);
        /// This address is already bound to a DIFFERENT node id. The mirror of
        /// `NodeIdAlreadyBound`; declared alongside it so a negative that trips
        /// the wrong half of `_checkBindingOneToOne` reports which half.
        error AddressAlreadyBound(bytes32 currentNodeId);
    }

    /// `FeeRouter.bytesPerEpoch` — governance-canonical served-bytes counter
    /// (ADR 036) used to assert delivery landed on-chain.
    #[sol(rpc)]
    contract FeeRouter {
        function bytesPerEpoch(address operator, uint64 epoch) external view returns (uint256);
    }

    /// `PublisherRegistry` namespace control plane (ADR 002) — the write
    /// surface the origin-publisher journeys (#1038/#1039) drive.
    #[sol(rpc)]
    contract PublisherRegistry {
        function createNamespace() external returns (uint256 namespaceId);
        function ownerOf(uint256 namespaceId) external view returns (address);
        // Timelocked 2-step namespace ownership transfer (ADR 002). No CLI
        // subcommand drives these yet, so the G-ORIGIN-01 transfer leg (#1038)
        // calls them directly. `pendingTransfer` is the public mapping getter.
        function initiateNamespaceTransfer(uint256 namespaceId, address newOwner) external;
        function finalizeNamespaceTransfer(uint256 namespaceId) external;
        function cancelNamespaceTransfer(uint256 namespaceId) external;
        function pendingTransfer(uint256 namespaceId)
            external
            view
            returns (address newOwner, uint64 readyAt);
        function namespaceTransferTimelock() external view returns (uint64);
        // Declared so a reverted `.call()` decodes to a *named* error rather
        // than an opaque selector — the negatives assert the specific guard
        // that fired, not merely that something reverted.
        error NotNamespaceOwner(uint256 namespaceId, address caller);
        error NoPendingTransfer(uint256 namespaceId);
        error TransferNotReady(uint256 readyAt);
        error NotPendingOwner(uint256 namespaceId, address caller);
    }

    /// `OriginAssignment` origin seating (publisher) + reads (ADR 011). Whether a
    /// publisher wallet may seat origins is decided by a swappable
    /// `IVettingPolicy` (see [`ManualVettingPolicy`], the genesis default);
    /// `vettingPolicy` is the installed address and `setVettingPolicy` re-points
    /// it under `GOVERNANCE_ROLE`. A vetted publisher seats and unseats origins
    /// itself with no wait.
    #[sol(rpc)]
    contract OriginAssignment {
        // The installed vetting policy, and the governance setter that swaps it.
        function vettingPolicy() external view returns (address);
        function setVettingPolicy(address newPolicy) external;
        // Passthrough to `vettingPolicy().isVetted(publisher)`.
        function isVettedPublisher(address publisher) external view returns (bool);
        // A vetted namespace owner seats one origin. Instant: `getOrigins`
        // reflects it in the same transaction.
        function addOrigin(uint256 namespaceId, address operator) external;
        // Publisher (own namespace) or governance removes one operator, closing
        // the authorized-origin gate for a fresh backend-only hash (#1373).
        function removeOrigin(uint256 namespaceId, address operator) external;
        function getOrigins(uint256 namespaceId) external view returns (address[] memory);
        function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool);
    }

    /// `ManualVettingPolicy` — the genesis vetting policy (ADR 011). A
    /// `VETTER_ROLE` holder approves publishers via `setVetted`; `GOVERNANCE_ROLE`
    /// admins `VETTER_ROLE` (the Timelock post-handoff). The `ChainFixture` vets a
    /// publisher by impersonating the Timelock to grant itself `VETTER_ROLE` and
    /// then calling `setVetted`. There is no self-service request path.
    #[sol(rpc)]
    contract ManualVettingPolicy {
        function VETTER_ROLE() external view returns (bytes32);
        function grantRole(bytes32 role, address account) external;
        function setVetted(address publisher, bool vetted) external;
        function isVetted(address publisher) external view returns (bool);
    }

    /// `ContentBlacklist` origin/operator blacklist writes (ADR 011 § Hash
    /// Evasion and Origin Blacklisting), absent from the re-exported production
    /// binding — the watcher only READS the `OriginBlacklistUpdated` /
    /// `OperatorBlacklisted` events these emit. Bound at the `ContentBlacklist`
    /// address. `setOriginBlacklist` / `addOperator` are `GOVERNANCE_ROLE`
    /// (driven under Timelock impersonation, like `addHashGlobal`).
    ///
    /// Declaring these here is also the ABI-drift guard for the hand-written
    /// origin declarations the node watcher decodes (#1398): the origin-blacklist
    /// journey is the only test that exercises the on-chain write → event →
    /// watcher path end to end, so a signature drift surfaces here rather than
    /// silently mis-decoding in production.
    #[sol(rpc)]
    contract ContentBlacklistOrigin {
        function setOriginBlacklist(address origin, bool blacklisted) external;
        function addOperator(address operator) external;
        function removeOperator(address operator) external;
        function isOriginBlacklisted(address origin) external view returns (bool);
        /// The operator-level deny list, which `addOperator` writes and
        /// `isOriginBlacklisted` does NOT reflect — the two are independent
        /// mappings that `OriginAssignment` unions.
        function isOperatorBlacklisted(address operator) external view returns (bool);
    }

    /// OpenZeppelin `AccessControl` surface, bound at a governed contract's
    /// address so the fixture can grant `REGIONAL_BODY_ROLE` (impersonating the
    /// role admin) for the regional-blacklist journey.
    #[sol(rpc)]
    contract AccessControl {
        function grantRole(bytes32 role, address account) external;
        function hasRole(bytes32 role, address account) external view returns (bool);
    }

    /// `PaymentPool` construction parameters, read back so a redeployed pool
    /// copies the snapshot pool's economics and wiring (see
    /// `ChainFixture::redeploy_payment_pool`).
    #[sol(rpc)]
    contract PaymentPoolParams {
        function usdc() external view returns (address);
        function tokenMessenger() external view returns (address);
        function sweepRecipient() external view returns (address);
        function sweepDomain() external view returns (uint32);
        function disputeWindow() external view returns (uint32);
        function operatorBps() external view returns (uint256);
        function minSweep() external view returns (uint64);
        function minDeposit() external view returns (uint64);
        function pauser() external view returns (address);
    }
}
