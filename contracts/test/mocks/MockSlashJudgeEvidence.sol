// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ISlashJudgeEvidenceView } from "../../src/interfaces/ISlashJudgeEvidenceView.sol";

/// @dev Test stub: returns a settable `maxEvidenceAgeUs` so CapacityBond's
///      unbonding mirror check can be exercised without a real SlashJudge.
contract MockSlashJudgeEvidence is ISlashJudgeEvidenceView {
    uint256 public maxEvidenceAgeUs;

    constructor(uint256 maxEvidenceAgeUs_) {
        maxEvidenceAgeUs = maxEvidenceAgeUs_;
    }

    function setMaxEvidenceAgeUs(uint256 v) external {
        maxEvidenceAgeUs = v;
    }
}
