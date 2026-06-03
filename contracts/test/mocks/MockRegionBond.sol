// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ICapacityBondEjector } from "../../src/interfaces/ICapacityBondEjector.sol";
import { ICapacityBondRegionView, NodeInfo } from "../../src/interfaces/ICapacityBondRegionView.sol";

/// @title MockRegionBond
/// @notice Test double for `CapacityBond`'s `ContentBlacklist`-facing surface:
///         the eject sink (`ICapacityBondEjector`) plus the ADR 030 region
///         read surface (`ICapacityBondRegionView`). Region inputs are fully
///         settable so a test can drive the ripening predicate and path-2
///         standing across the stability-window boundary without registering
///         a real node or producing ed25519 vectors.
contract MockRegionBond is ICapacityBondEjector, ICapacityBondRegionView {
    address[] public ejected;

    uint256 internal _window;
    uint64 internal _gate;
    mapping(address => string) internal _regionHint;
    mapping(address => string) internal _regionPrev;
    mapping(address => uint64) internal _regionLastChanged;
    mapping(address => uint64) internal _firstBonded;

    // --- eject sink ---

    function ejectNode(address operator) external override {
        ejected.push(operator);
    }

    function ejectedCount() external view returns (uint256) {
        return ejected.length;
    }

    // --- test configuration ---

    function setWindow(uint256 window) external {
        _window = window;
    }

    function setGate(uint64 gate) external {
        _gate = gate;
    }

    /// @notice Configure the full ADR 030 region triple for `operator`.
    function setNode(
        address operator,
        string calldata regionHint_,
        string calldata regionPrev_,
        uint64 regionLastChanged_,
        uint64 firstBondedAt_
    ) external {
        _regionHint[operator] = regionHint_;
        _regionPrev[operator] = regionPrev_;
        _regionLastChanged[operator] = regionLastChanged_;
        _firstBonded[operator] = firstBondedAt_;
    }

    // --- ICapacityBondRegionView ---

    function getNodeByAddress(address ethAddress) external view override returns (NodeInfo memory node) {
        node.regionHint = _regionHint[ethAddress];
    }

    function regionPrev(address operator) external view override returns (string memory) {
        return _regionPrev[operator];
    }

    function regionLastChanged(address operator) external view override returns (uint64) {
        return _regionLastChanged[operator];
    }

    function firstBondedAt(address operator) external view override returns (uint64) {
        return _firstBonded[operator];
    }

    function regionGateActivatedAt() external view override returns (uint64) {
        return _gate;
    }

    function regionStabilityWindow() external view override returns (uint256) {
        return _window;
    }
}
