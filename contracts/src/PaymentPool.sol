// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { PaymentPoolBase } from "./PaymentPoolBase.sol";
import { ITokenMessengerV2 } from "./interfaces/ICctpV2.sol";

/// @title PaymentPool
/// @notice The CCTP V2 payment pool (ADR 003 § PaymentPool, ADR 016 § Bridge
///         adapters). `sweep` burns a batch of operators' pending protocol
///         shares with one `depositForBurnWithHook` at standard finality,
///         minting the total to the governance-chain `CctpSweepReceiver`,
///         which deposits each share into `FeeRouter`.
/// @dev    The receiver is both `mintRecipient` and `destinationCaller`, so
///         only it can receive the message. `maxFee` is the messenger's
///         `getMinFeeAmount(total)`, or 0 when the messenger has no such
///         function.
contract PaymentPool is PaymentPoolBase {
    using SafeERC20 for IERC20;

    /// @dev CCTP V2 standard (finalized) transfer threshold.
    uint32 internal constant STANDARD_FINALITY = 2000;

    /// @notice The local CCTP `TokenMessengerV2`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    ITokenMessengerV2 public immutable tokenMessenger;

    /// @notice The governance-chain `CctpSweepReceiver`.
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    address public immutable sweepRecipient;

    /// @notice The CCTP domain of the governance chain (Ethereum is 0).
    // forge-lint: disable-next-line(screaming-snake-case-immutable)
    uint32 public immutable sweepDomain;

    /// @param usdc_           Native USDC on this chain.
    /// @param tokenMessenger_ Local CCTP `TokenMessengerV2`.
    /// @param sweepRecipient_ Governance-chain `CctpSweepReceiver`.
    /// @param sweepDomain_    CCTP domain of the governance chain.
    /// @param disputeWindow_  Grace window in seconds.
    /// @param operatorBps_    Same-transaction operator share (at most 10000).
    /// @param minSweep_       Smallest sweep batch total in USDC base units.
    /// @param minDeposit_     Minimum credited `openPool` deposit.
    /// @param pauser_         The address that may pause inflows.
    constructor(
        IERC20 usdc_,
        ITokenMessengerV2 tokenMessenger_,
        address sweepRecipient_,
        uint32 sweepDomain_,
        uint32 disputeWindow_,
        uint256 operatorBps_,
        uint64 minSweep_,
        uint64 minDeposit_,
        address pauser_
    ) PaymentPoolBase(usdc_, disputeWindow_, operatorBps_, minSweep_, minDeposit_, pauser_) {
        if (address(tokenMessenger_) == address(0) || sweepRecipient_ == address(0)) {
            revert ZeroAddress();
        }
        tokenMessenger = tokenMessenger_;
        sweepRecipient = sweepRecipient_;
        sweepDomain = sweepDomain_;
    }

    /// @dev `getMinFeeAmount(amount)` when the messenger exposes it, else 0.
    function _sweepFee(uint256 amount) internal view override returns (uint256) {
        (bool ok, bytes memory ret) =
            address(tokenMessenger).staticcall(abi.encodeCall(ITokenMessengerV2.getMinFeeAmount, (amount)));
        if (!ok || ret.length < 32) return 0;
        return abi.decode(ret, (uint256));
    }

    /// @dev Burn `amount` to the receiver with `abi.encode(shares)` as the hook
    ///      data.
    function _sendSweep(SweepShare[] memory shares, uint256 amount, uint256 fee) internal override {
        bytes32 recipient = bytes32(uint256(uint160(sweepRecipient)));
        usdc.forceApprove(address(tokenMessenger), amount);
        tokenMessenger.depositForBurnWithHook(
            amount, sweepDomain, recipient, address(usdc), recipient, fee, STANDARD_FINALITY, abi.encode(shares)
        );
    }
}
