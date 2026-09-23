"""Regression tests for the Solidity change detector in CI."""

from __future__ import annotations

import fnmatch
import re
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
CHANGES_JOB = WORKFLOW["jobs"]["changes"]
PATH_FILTER_STEPS = {
    step["id"]: step["with"]
    for step in CHANGES_JOB["steps"]
    if step.get("uses") == "dorny/paths-filter@v4"
}
DIRECT_OUTPUT = re.compile(r"\$\{\{\s*steps\.([\w-]+)\.outputs\.([\w-]+)\s*\}\}")
BOOLEAN_OUTPUT = re.compile(
    r"steps\.([\w-]+)\.outputs\.([\w-]+)\s*==\s*'true'"
)


def _rule_matches(path: str, rule: str) -> bool:
    """Evaluate the simple literal/glob rules used by the Solidity filters."""
    negated = rule.startswith("!")
    pattern = rule.removeprefix("!")
    matched = fnmatch.fnmatchcase(path, pattern)
    return not matched if negated else matched


def _filter_matches(step_id: str, filter_name: str, changed_paths: tuple[str, ...]) -> bool:
    config = PATH_FILTER_STEPS[step_id]
    filters = yaml.safe_load(config["filters"])
    rules = filters[filter_name]
    predicate = all if config.get("predicate-quantifier", "some") == "every" else any
    return any(predicate(_rule_matches(path, rule) for rule in rules) for path in changed_paths)


def _solidity_output(changed_paths: tuple[str, ...]) -> bool:
    expression = CHANGES_JOB["outputs"]["solidity"]
    direct = DIRECT_OUTPUT.fullmatch(expression)
    if direct:
        return _filter_matches(*direct.groups(), changed_paths)

    outputs = BOOLEAN_OUTPUT.findall(expression)
    assert outputs, f"unsupported Solidity output expression: {expression}"
    assert "&&" not in expression
    assert expression.count("||") == len(outputs) - 1
    return any(_filter_matches(*output, changed_paths) for output in outputs)


@pytest.mark.parametrize(
    ("changed_paths", "expected"),
    [
        (("contracts/src/DecdnToken.sol",), True),
        (("contracts/foundry.toml",), True),
        (("contracts/deployments/421614.json",), False),
        (("crates/node/src/slash_watcher.rs",), False),
        (("README.md",), False),
        ((".gitmodules",), True),
        ((".github/workflows/ci.yml",), True),
        ((".github/actions/foundry-setup/action.yml",), True),
        ((".github/scripts/post-gas-snapshot-comment.sh",), True),
        ((".github/scripts/lcov_to_cobertura.py",), True),
        (("contracts/deployments/421614.json", "README.md"), False),
        (("contracts/deployments/421614.json", "contracts/src/DecdnToken.sol"), True),
    ],
    ids=[
        "contract-source",
        "contract-config",
        "deployment-manifest",
        "rust-source",
        "documentation",
        "git-submodules",
        "ci-workflow",
        "shared-action",
        "gas-comment-script",
        "cobertura-converter",
        "manifest-and-docs",
        "manifest-and-contract-source",
    ],
)
def test_solidity_change_detection_truth_table(
    changed_paths: tuple[str, ...], expected: bool
) -> None:
    assert _solidity_output(changed_paths) is expected
