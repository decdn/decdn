// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/// @notice Malicious ERC20 that re-enters a target contract during transfer
/// or transferFrom. Used to prove `nonReentrant` guards fire.
/// @dev The re-entry payload is set via `armAttack(target, data)`. The
///      target receives the *first* transfer(From); the re-entry attempts a
///      second call to `target` with `data`, which must revert with
///      `ReentrancyGuardReentrantCall`.
contract ReentrantERC20 is ERC20 {
    address public target;
    bytes public attack;
    bool internal _reentering;

    constructor() ERC20("Reentrant", "R20") {
        _mint(msg.sender, 1_000_000_000e18);
    }

    function decimals() public pure override returns (uint8) {
        return 18;
    }

    function mint(
        address to,
        uint256 amount
    ) external {
        _mint(to, amount);
    }

    function armAttack(
        address target_,
        bytes calldata attack_
    ) external {
        target = target_;
        attack = attack_;
    }

    function disarm() external {
        target = address(0);
        attack = "";
    }

    function _update(
        address from,
        address to,
        uint256 value
    ) internal override {
        super._update(from, to, value);
        if (target != address(0) && !_reentering) {
            _reentering = true;
            (bool ok, bytes memory ret) = target.call(attack);
            _reentering = false;
            // Bubble revert data so the outer call sees ReentrancyGuardReentrantCall
            // instead of a silent swallow.
            if (!ok) {
                assembly {
                    revert(add(ret, 0x20), mload(ret))
                }
            }
        }
    }
}
