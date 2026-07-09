//! `alloy::sol!` bindings for the contract write/setup surface the fixtures
//! drive, which the production `decdn-incentive` bindings (read/seller-only)
//! deliberately omit.
//!
//! The seller-path reads — `getChannel`, the `Channel` struct, the `Status`
//! enum — are reused from [`decdn_incentive::payment_channel::PaymentChannel`]
//! (the same ABI the runtime decodes) rather than re-declared, so the
//! load-bearing `Channel` layout has a single source of truth. Everything here
//! is buyer/operator/publisher setup the seller binding does not expose.

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

// Reuse the production seller-path binding for the `Channel` struct + reads.
pub use decdn_incentive::payment_channel::PaymentChannel;
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
        function bindingNonce(address operator) external view returns (uint64);
        function registrationNonce(bytes32 nodeId) external view returns (uint64);
        function updateRegion(string newRegion) external;
    }

    /// Buyer-side `openChannel` (omitted by the seller-only production binding)
    /// plus the channel-derivation reads.
    #[sol(rpc)]
    contract PaymentChannelOpen {
        function openChannel(address provider, uint256 deposit) external returns (bytes32 channelId);
        function minDeposit() external view returns (uint256);
        function clientChannelNonce(address client) external view returns (uint256);
    }

    /// `FeeRouter.bytesPerEpoch` — governance-canonical served-bytes counter
    /// (ADR 036) used to assert delivery landed on-chain.
    #[sol(rpc)]
    contract FeeRouter {
        function bytesPerEpoch(address operator, uint64 epoch) external view returns (uint256);
    }

    /// `PublisherRegistry` namespace/claim control plane (ADR 002) — the write
    /// surface the origin-publisher journeys (#1038/#1039) drive.
    #[sol(rpc)]
    contract PublisherRegistry {
        function createNamespace() external returns (uint256 namespaceId);
        function claimContent(uint256 namespaceId, bytes32 blake3Hash) external;
        function namespaceOf(bytes32 blake3Hash) external view returns (uint256[] memory);
        function ownerOf(uint256 namespaceId) external view returns (address);
    }

    /// `OriginAssignment` propose (publisher) + activate (governance) + reads
    /// (ADR 011). Activation requires `GOVERNANCE_ROLE`; a fixture helper to
    /// drive it under role impersonation is not yet implemented (forward surface
    /// for the origin-recognition journeys #1038/#1039).
    #[sol(rpc)]
    contract OriginAssignment {
        function proposeAssignment(uint256 namespaceId, address[] operators) external;
        function activateAssignment(uint256 namespaceId) external;
        function getOrigins(uint256 namespaceId) external view returns (address[] memory);
        function isAuthorizedOrigin(uint256 namespaceId, address operator) external view returns (bool);
    }

    /// `SlashJudge.submitBlacklistChallenge` — the blacklist-violation slash
    /// entry point (ADR 014 §Blacklist violation). The G-NODE-04 negative case
    /// drives it with a signed post-window `ProbeResponse` to prove the on-chain
    /// slashability of serving blacklisted content.
    /// `ProbeResponse` fields SlashJudge `abi.decode`s from `responseData` (the
    /// EIP-712 `ProbeResponse` type). Mirrors `decdn_incentive::ProbeSlashData`.
    struct ProbeMsg {
        bytes32 hash;
        bool hasBlob;
        uint64 ratePerMb;
        uint64 timestampUs;
    }

    #[sol(rpc)]
    contract SlashJudge {
        function commitChallenge(bytes32 commitment) external;
        function submitBlacklistChallenge(
            address challengedNode,
            bytes32 nodeId,
            bytes32 blobHash,
            bytes responseData,
            bytes slashSig,
            bool isStreamResponse,
            bytes32 salt
        ) external;
        function challengeBond() external view returns (uint256);
    }

    /// OpenZeppelin `AccessControl` surface, bound at a governed contract's
    /// address so the fixture can grant `REGIONAL_BODY_ROLE` (impersonating the
    /// role admin) for the regional-blacklist journey.
    #[sol(rpc)]
    contract AccessControl {
        function grantRole(bytes32 role, address account) external;
        function hasRole(bytes32 role, address account) external view returns (bool);
    }
}
