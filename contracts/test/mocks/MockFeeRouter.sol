// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IFeeRouter } from "../../src/interfaces/IFeeRouter.sol";

/// @notice Test-only stand-in for `FeeRouter`. Lets tests inject arbitrary
///         per-operator + global per-epoch byte counters and a window
///         length, so `DecdnGovernor._getVotes` math can be exercised in
///         isolation.
contract MockFeeRouter is IFeeRouter {
    uint64 public override windowEpochs;
    uint64 public override epochLength;

    mapping(address => mapping(uint64 => uint256)) internal _bytes;
    mapping(uint64 => uint256) internal _totalBytes;

    constructor(uint64 windowEpochs_, uint64 epochLength_) {
        windowEpochs = windowEpochs_;
        epochLength = epochLength_;
    }

    function setWindowEpochs(uint64 n) external {
        windowEpochs = n;
    }

    /// @notice The mock collapses `windowEpochsAt(timepoint)` to the current
    ///         `windowEpochs` — sufficient for `DecdnGovernor` math tests
    ///         that do not exercise mid-proposal window changes.
    function windowEpochsAt(uint48) external view override returns (uint64) {
        return windowEpochs;
    }

    function setBytes(address operator, uint64 epoch, uint256 amount) external {
        _bytes[operator][epoch] = amount;
    }

    function setTotalBytes(uint64 epoch, uint256 amount) external {
        _totalBytes[epoch] = amount;
    }

    function routeSettlement(address, uint256, uint256) external pure override {
        revert("MockFeeRouter: not implemented");
    }

    function bytesPerEpoch(address operator, uint64 epoch) external view override returns (uint256) {
        return _bytes[operator][epoch];
    }

    function totalBytesPerEpoch(uint64 epoch) external view override returns (uint256) {
        return _totalBytes[epoch];
    }

    function bytesInWindow(address operator, uint64 endEpoch, uint64 n) external view override returns (uint256 sum) {
        if (n == 0) return 0;
        uint64 startEpoch = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        for (uint64 e = startEpoch; e <= endEpoch; e++) {
            sum += _bytes[operator][e];
        }
    }

    function totalBytesInWindow(uint64 endEpoch, uint64 n) external view override returns (uint256 sum) {
        if (n == 0) return 0;
        uint64 startEpoch = endEpoch + 1 > n ? endEpoch + 1 - n : 0;
        for (uint64 e = startEpoch; e <= endEpoch; e++) {
            sum += _totalBytes[e];
        }
    }
}
