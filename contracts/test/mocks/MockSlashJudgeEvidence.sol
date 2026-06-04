// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ISlashJudgeEvidenceView } from "../../src/interfaces/ISlashJudgeEvidenceView.sol";

/// @dev Test stub returning a constructor-set `maxEvidenceAgeUs` so CapacityBond's
///      unbonding mirror check can be exercised without a real SlashJudge.
contract MockSlashJudgeEvidence is ISlashJudgeEvidenceView {
    uint256 public maxEvidenceAgeUs;

    constructor(uint256 maxEvidenceAgeUs_) {
        maxEvidenceAgeUs = maxEvidenceAgeUs_;
    }
}
