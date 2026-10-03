"""Regression tests for the decdn pin checker.

Cargo checks the decdn version requirement against the git crate only when it
resolves, and crates.io checks it only at publish time. A bug here is a quiet
false pass that surfaces as a release that cannot be published.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_decdn_pin.py"

_spec = importlib.util.spec_from_file_location("check_decdn_pin", MODULE_PATH)
assert _spec and _spec.loader
cdp = importlib.util.module_from_spec(_spec)
sys.modules["check_decdn_pin"] = cdp
_spec.loader.exec_module(cdp)

SHA = "f6de71c2b231c6d5683b8f9193add163a9444fdd"
OTHER_SHA = "0123456789abcdef0123456789abcdef01234567"
GIT = "https://github.com/decdn/decdn"


def build(
    tmp_path: Path,
    *,
    versions: dict[str, str | None] | None = None,
    locked: dict[str, tuple[str, str]] | None = None,
    branch: dict[str, str] | None = None,
    member_deps: str = "[dependencies]\ndecdn-client.workspace = true\n"
    "[dev-dependencies]\ndecdn-e2e.workspace = true\n",
    extra_alias: str = "",
) -> Path:
    """A repo with the given aliases, a member using them, and a Cargo.lock."""
    if versions is None:
        versions = {"decdn-client": "0.0.0", "decdn-incentive": "0.0.0", "decdn-e2e": None}
    if locked is None:
        locked = {name: ("0.0.0", SHA) for name in versions}
    branch = branch or {}
    repo = tmp_path / "repo"
    (repo / "crates/app").mkdir(parents=True)
    lines = ["[workspace]", 'members = ["crates/app"]', "", "[workspace.dependencies]"]
    for name, version in versions.items():
        v = f', version = "{version}"' if version is not None else ""
        b = branch.get(name, "main")
        lines.append(f'{name} = {{ git = "{GIT}", branch = "{b}"{v} }}')
    lines.append('serde = "1"')
    lines.append(extra_alias)
    (repo / "Cargo.toml").write_text("\n".join(lines) + "\n")
    (repo / "crates/app/Cargo.toml").write_text('[package]\nname = "app"\n' + member_deps)
    lock = ["version = 4"]
    for name, (version, sha) in locked.items():
        lock += [
            "",
            "[[package]]",
            f'name = "{name}"',
            f'version = "{version}"',
            f'source = "git+{GIT}?branch=main#{sha}"',
        ]
    lock += ["", "[[package]]", 'name = "serde"', 'version = "1.0.0"',
             'source = "registry+https://github.com/rust-lang/crates.io-index"']
    (repo / "Cargo.lock").write_text("\n".join(lock) + "\n")
    return repo


def errors(repo: Path) -> list[str]:
    return cdp.check(repo)[0]


def test_consistent_pin_passes(tmp_path):
    repo = build(tmp_path)
    errs, notes = cdp.check(repo)
    assert errs == []
    assert notes == [f"decdn@{SHA}"]


def test_prerelease_version_passes(tmp_path):
    v = {"decdn-client": "0.2.0-rc.1"}
    repo = build(tmp_path, versions=v, locked={"decdn-client": ("0.2.0-rc.1", SHA)},
                 member_deps="[dependencies]\ndecdn-client.workspace = true\n")
    assert errors(repo) == []


def test_version_disagreeing_with_the_lock_is_named(tmp_path):
    repo = build(tmp_path, locked={
        "decdn-client": ("0.1.0", SHA), "decdn-incentive": ("0.1.0", SHA),
        "decdn-e2e": ("0.1.0", SHA)})
    errs = errors(repo)
    assert sum("locked decdn commit is at 0.1.0" in e for e in errs) == 2, errs


def test_lock_spanning_two_commits_is_an_error(tmp_path):
    repo = build(tmp_path, locked={
        "decdn-client": ("0.0.0", SHA), "decdn-incentive": ("0.0.0", OTHER_SHA),
        "decdn-e2e": ("0.0.0", SHA)})
    errs = errors(repo)
    assert len(errs) == 1 and "2 commits" in errs[0], errs


def test_alias_missing_from_the_lock_is_an_error(tmp_path):
    repo = build(tmp_path, locked={"decdn-client": ("0.0.0", SHA), "decdn-e2e": ("0.0.0", SHA)})
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-incentive" in errs[0], errs


def test_unversioned_alias_is_fine_as_a_dev_dependency_only(tmp_path):
    assert errors(build(tmp_path)) == []
    repo = build(tmp_path / "b", member_deps="[dependencies]\ndecdn-e2e.workspace = true\n")
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-e2e" in errs[0] and "dependencies" in errs[0], errs


def test_range_requirement_is_an_error(tmp_path):
    repo = build(tmp_path, versions={"decdn-client": "^0.0", "decdn-incentive": "0.0.0",
                                     "decdn-e2e": None})
    errs = errors(repo)
    assert any("decdn-client" in e and "exact" in e for e in errs), errs


def test_aliases_disagreeing_with_each_other_is_an_error(tmp_path):
    repo = build(tmp_path, versions={"decdn-client": "0.0.0", "decdn-incentive": "0.1.0",
                                     "decdn-e2e": None})
    assert any("disagree" in e for e in errors(repo))


def test_aliases_on_different_refs_are_an_error(tmp_path):
    repo = build(tmp_path, branch={"decdn-incentive": "release"})
    assert any("different refs" in e for e in errors(repo))


def test_a_leftover_sibling_path_alias_is_an_error(tmp_path):
    repo = build(tmp_path, extra_alias='decdn-common = { path = "../decdn/crates/common" }')
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-common" in errs[0] and "git dependencies" in errs[0], errs


def test_no_decdn_alias_is_a_failure_not_a_pass(tmp_path):
    repo = build(tmp_path, versions={}, locked={}, member_deps="")
    errs = errors(repo)
    assert len(errs) == 1 and "inspected nothing" in errs[0], errs


def test_the_real_repository_passes():
    assert errors(REPO_ROOT) == []
