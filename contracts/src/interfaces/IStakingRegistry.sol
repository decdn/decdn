// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IStakingRegistry
/// @notice External interface for cross-contract consumers of the StakingRegistry.
/// @dev See ADR 003 (§ Staking), ADR 004 (§ Slashing) and ADR 016 (§3 Call Table).
interface IStakingRegistry {
    /// @notice Offense categories enumerated by ADR 014.
    /// @dev `Unspecified` is the zero sentinel. Default-initialized storage
    ///      or an accidentally-omitted argument reads as Unspecified — not
    ///      as a valid offense — so callers surface the bug rather than
    ///      silently slashing for the wrong reason.
    enum OffenseType {
        Unspecified,
        PhantomBlob,
        RateManipulation,
        BlacklistViolation,
        Corruption
    }

    /// @notice Snapshot of a registered operator's bootstrap-relevant fields.
    /// @dev Returned by `getActiveNodes`. Per ADR 016 §3 Off-Chain Read API,
    ///      clients receive raw signals only — ranking is composed off-chain.
    ///      `lastSettlementAt` is zero for operators who have never had a
    ///      payment channel settle in their favour.
    struct NodeInfo {
        address operator;
        bytes32 nodeId;
        uint64 lastSettlementAt;
    }

    /// @notice Slash a node's stake. Only callable by holders of `SLASH_ROLE`.
    /// @dev Sends 50% of the slashed amount to `msg.sender` and burns 50%.
    ///      Amount is computed internally from the flat PoC schedule (10%).
    /// @return slashed Total amount slashed (sum of reward + burn). Returning
    ///         this lets callers compute their reward precisely without the
    ///         fragile `balanceOf`-delta trick.
    function slash(
        address node,
        OffenseType offense
    ) external returns (uint256 slashed);

    /// @notice Forcibly eject an operator. Only callable by holders of
    /// `BLACKLIST_ROLE`. No funds are moved.
    function ejectNode(
        address operator
    ) external;

    /// @notice Returns `floor(provider.active / minStake)` — the number of
    /// full minimum-stake multiples the provider has bonded. Scales
    /// automatically if governance changes `minStake`. Used by
    /// StablePaymentChannel to gate the discounted fee tier (ADR 003).
    function getStakeMultiple(
        address provider
    ) external view returns (uint256);

    /// @notice Returns the iroh NodeId currently bound to `operator`, or
    /// zero if the operator has never registered (or has been ejected).
    /// SlashJudge uses this snapshot at challenge-submit time to bind
    /// counter-evidence to the specific node identity under dispute.
    function nodeIdOf(
        address operator
    ) external view returns (bytes32);

    /// @notice Stamp the operator's `lastSettlementAt` to `block.timestamp`.
    /// Only callable by holders of `SETTLEMENT_REPORTER_ROLE` — granted to
    /// PaymentChannel contracts at deploy time. Used by clients to bias
    /// cold-start bootstrap toward proven deliverers (ADR 016 §3 Off-Chain
    /// Read API). One SSTORE; no slashing or fund movement.
    function recordSettlement(
        address operator
    ) external;

    /// @notice Number of currently-registered (non-ejected) operators.
    /// Off-chain clients use this to size their pagination loop over
    /// `getActiveNodes` (ADR 012 §Bootstrap).
    function getActiveNodeCount() external view returns (uint256);

    /// @notice Paginated list of registered operators with their bootstrap
    /// signals (operator, nodeId, lastSettlementAt). Returns up to `limit`
    /// entries starting at `offset`. Returns an empty array when `offset`
    /// is at or past the end of the active set. Order is unspecified —
    /// clients that care about ranking sort the returned array off-chain
    /// (ADR 016 §3 Off-Chain Read API).
    function getActiveNodes(
        uint256 offset,
        uint256 limit
    ) external view returns (NodeInfo[] memory);

    /// @notice Unix timestamp (seconds) of `ethAddress`'s very first
    /// `registerNode` call, or zero if never registered. Stable across
    /// re-registrations — used by the reputation cold-start bonus window
    /// (ADR 008). `ethAddress` is the operator address that registered.
    function getFirstRegisteredAt(
        address ethAddress
    ) external view returns (uint256);
}
