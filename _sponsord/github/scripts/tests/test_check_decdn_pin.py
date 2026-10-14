"""Regression tests for the decdn pin checker.

Cargo builds a decdn crate twice without complaint when a [patch] entry is
missing, and leaves an unused one in the lock with a warning nobody reads. A
bug here is a quiet false pass that surfaces as anvil tests with mismatched
types, or fixtures deploying contracts from another release than the crates.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

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
CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
DEFAULT_REGISTRY = {"decdn-client": "0.1", "decdn-incentive": "0.1"}
DEFAULT_PATCH = {"decdn-client": "0.1", "decdn-incentive": "0.1", "decdn-protocol": "0.1"}


def build(
    tmp_path: Path,
    *,
    registry: dict[str, str] | None = None,
    git: dict[str, str] | None = None,
    patch: dict[str, str] | None = None,
    locked: dict[str, str] | None = None,
    locked_git: dict[str, str] | None = None,
    unused: list[str] = (),
    member_deps: str = "[dependencies]\ndecdn-client.workspace = true\n"
    "[dev-dependencies]\ndecdn-e2e.workspace = true\n",
    extra_alias: str = "",
) -> Path:
    """A repo with decdn on crates.io, decdn-e2e on git, the [patch], and a lock.

    registry: {name: requirement}; git: {name: tag}; patch: {name: requirement};
    locked: {name: version} from crates.io; locked_git: {name: sha}.
    """
    registry = DEFAULT_REGISTRY if registry is None else registry
    git = {"decdn-e2e": "v0.1.0"} if git is None else git
    patch = DEFAULT_PATCH if patch is None else patch
    if locked is None:
        locked = {n: "0.1.0" for n in ("decdn-client", "decdn-incentive", "decdn-protocol")}
    if locked_git is None:
        locked_git = {"decdn-e2e": SHA, "decdn-node": SHA}
    repo = tmp_path / "repo"
    (repo / "crates/app").mkdir(parents=True)
    lines = ["[workspace]", 'members = ["crates/app"]', "", "[workspace.dependencies]"]
    lines += [f'{name} = "{req}"' for name, req in registry.items()]
    lines += [f'{name} = {{ git = "{GIT}", tag = "{tag}" }}' for name, tag in git.items()]
    lines += ['serde = "1"', extra_alias]
    if patch:
        lines += ["", f'[patch."{GIT}"]']
        lines += [f'{name} = "{req}"' for name, req in patch.items()]
    (repo / "Cargo.toml").write_text("\n".join(lines) + "\n")
    (repo / "crates/app/Cargo.toml").write_text('[package]\nname = "app"\n' + member_deps)
    lock = ["version = 4"]
    for name, version in locked.items():
        lock += ["", "[[package]]", f'name = "{name}"', f'version = "{version}"',
                 f'source = "{CRATES_IO}"']
    for name, sha in locked_git.items():
        lock += ["", "[[package]]", f'name = "{name}"', 'version = "0.1.0"',
                 f'source = "git+{GIT}?tag=v0.1.0#{sha}"']
    lock += ["", "[[package]]", 'name = "serde"', 'version = "1.0.0"', f'source = "{CRATES_IO}"']
    for name in unused:
        lock += ["", "[[patch.unused]]", f'name = "{name}"', 'version = "0.0.4"']
    (repo / "Cargo.lock").write_text("\n".join(lock) + "\n")
    return repo


def errors(repo: Path) -> list[str]:
    return cdp.check(repo)[0]


def test_consistent_pin_passes(tmp_path):
    errs, notes = cdp.check(build(tmp_path))
    assert errs == []
    assert notes == ["crates.io 0.1.0 at '0.1'", f"decdn-e2e at v0.1.0, decdn@{SHA}"]


def test_crates_io_only_needs_no_patch(tmp_path):
    # Once decdn publishes its fixtures, the git alias and the [patch] go.
    repo = build(tmp_path, git={}, patch={}, locked_git={},
                 member_deps="[dependencies]\ndecdn-client.workspace = true\n")
    errs, notes = cdp.check(repo)
    assert errs == [] and notes == ["crates.io 0.1.0 at '0.1'"]


def test_inline_table_requirement_counts(tmp_path):
    repo = build(tmp_path, registry={"decdn-client": "0.1"},
                 extra_alias='decdn-incentive = { version = "0.1", features = ["redb"] }')
    assert errors(repo) == []


def test_prerelease_requirement_passes(tmp_path):
    repo = build(tmp_path, registry={"decdn-client": "0.2.0-rc.1"},
                 patch={"decdn-client": "0.2.0-rc.1"}, git={"decdn-e2e": "v0.2.0-rc.1"},
                 locked={"decdn-client": "0.2.0-rc.1"})
    assert errors(repo) == []


def test_operator_requirement_is_an_error(tmp_path):
    for req in ("=0.1.0", ">=0.1", "0.*", "~0.1"):
        repo = build(tmp_path / req.replace("*", "x"), registry={"decdn-client": req},
                     patch={"decdn-client": req})
        errs = errors(repo)
        assert any("caret" in e and "decdn-client" in e for e in errs), (req, errs)


def test_requirements_disagreeing_is_an_error(tmp_path):
    repo = build(tmp_path, registry={"decdn-client": "0.1", "decdn-incentive": "0.2"})
    assert any("disagree" in e for e in errors(repo))


def test_patch_requirement_disagreeing_is_an_error(tmp_path):
    repo = build(tmp_path, patch={**DEFAULT_PATCH, "decdn-protocol": "0.0.4"})
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-protocol" in errs[0] and "[patch" in errs[0], errs


def test_patch_entry_on_git_is_an_error(tmp_path):
    repo = build(tmp_path, patch={})
    manifest = repo / "Cargo.toml"
    manifest.write_text(manifest.read_text() + f'\n[patch."{GIT}"]\n'
                        f'decdn-client = {{ git = "{GIT}", branch = "main" }}\n')
    assert any("[patch" in e and "decdn-client" in e for e in errors(repo))


def test_missing_patch_entry_shows_as_a_crate_built_twice(tmp_path):
    repo = build(tmp_path, locked_git={"decdn-e2e": SHA, "decdn-protocol": SHA})
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-protocol" in errs[0] and "both" in errs[0], errs


def test_unused_patch_entry_is_an_error(tmp_path):
    errs = errors(build(tmp_path, unused=["decdn-protocol"]))
    assert len(errs) == 1 and "decdn-protocol" in errs[0] and "unused" in errs[0], errs


def test_crates_io_versions_disagreeing_is_an_error(tmp_path):
    repo = build(tmp_path, locked={"decdn-client": "0.1.1", "decdn-incentive": "0.1.0",
                                   "decdn-protocol": "0.1.1"})
    assert any("different versions" in e for e in errors(repo))


def add_package(repo: Path, name: str, version: str, source: str) -> None:
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text() + f'\n[[package]]\nname = "{name}"\n'
                    f'version = "{version}"\nsource = "{source}"\n')


def test_a_second_crates_io_copy_of_a_crate_is_an_error(tmp_path):
    # Cargo keeps 0.1.x and 0.2.x side by side; the second entry must not
    # replace the first.
    repo = build(tmp_path)
    add_package(repo, "decdn-client", "0.2.0", CRATES_IO)
    errs = errors(repo)
    assert any("decdn-client 0.1.0" in e and "decdn-client 0.2.0" in e for e in errs), errs


def test_a_second_commit_of_a_git_crate_is_an_error(tmp_path):
    repo = build(tmp_path, locked_git={"decdn-e2e": SHA})
    add_package(repo, "decdn-e2e", "0.1.0", f"git+{GIT}?branch=main#{OTHER_SHA}")
    errs = errors(repo)
    assert len(errs) == 1 and "2 commits" in errs[0], errs


@pytest.mark.parametrize("req", ["0.2", "0.1.3", "0"])
def test_requirement_off_the_locked_version_is_an_error(tmp_path, req):
    repo = build(tmp_path, registry={"decdn-client": req, "decdn-incentive": req},
                 patch={n: req for n in DEFAULT_PATCH})
    errs = errors(repo)
    assert len(errs) == 1 and f"require {req!r}" in errs[0] and "'0.1'" in errs[0], errs


def test_dependency_missing_from_the_lock_is_an_error(tmp_path):
    repo = build(tmp_path, locked={"decdn-client": "0.1.0", "decdn-protocol": "0.1.0"})
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-incentive" in errs[0], errs


def test_lock_spanning_two_commits_is_an_error(tmp_path):
    repo = build(tmp_path, locked_git={"decdn-e2e": SHA, "decdn-node": OTHER_SHA})
    errs = errors(repo)
    assert len(errs) == 1 and "2 commits" in errs[0], errs


def test_e2e_tag_off_the_locked_version_is_an_error(tmp_path):
    errs = errors(build(tmp_path, git={"decdn-e2e": "v0.0.4"}))
    assert len(errs) == 1 and "v0.0.4" in errs[0] and "set its tag to v0.1.0" in errs[0], errs


def test_e2e_on_a_branch_is_an_error(tmp_path):
    repo = build(tmp_path, git={})
    manifest = repo / "Cargo.toml"
    manifest.write_text(manifest.read_text().replace(
        'serde = "1"', f'decdn-e2e = {{ git = "{GIT}", branch = "main" }}\nserde = "1"'))
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-e2e" in errs[0] and "tag" in errs[0], errs


def test_git_alias_outside_dev_dependencies_is_an_error(tmp_path):
    repo = build(tmp_path, member_deps="[dependencies]\ndecdn-e2e.workspace = true\n")
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-e2e" in errs[0] and "dev-dependency" in errs[0], errs


def test_a_leftover_sibling_path_alias_is_an_error(tmp_path):
    repo = build(tmp_path, extra_alias='decdn-common = { path = "../decdn/crates/common" }')
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-common" in errs[0] and "sibling" in errs[0], errs


def test_no_crates_io_decdn_dependency_is_a_failure_not_a_pass(tmp_path):
    repo = build(tmp_path, registry={}, patch={}, locked={})
    errs = errors(repo)
    assert len(errs) == 1 and "inspected nothing" in errs[0], errs


def test_caret_of_a_version():
    assert cdp.caret("1.4.2") == "1"
    assert cdp.caret("0.3.1") == "0.3"
    assert cdp.caret("0.0.7") == "0.0.7"
    assert cdp.caret("0.2.0-rc.1") == "0.2.0-rc.1"


def test_the_real_repository_passes():
    assert errors(REPO_ROOT) == []


# ---- what escapes [workspace.dependencies] -------------------------------


@pytest.mark.parametrize("alias", [
    'client = { package = "decdn-client", version = "0.1.3" }',
    f'e2e = {{ package = "decdn-e2e", git = "{GIT}", branch = "main" }}',
])
def test_renamed_workspace_alias_is_an_error(tmp_path, alias):
    # Classified by key, it would escape every check on the decdn crates.
    errs = errors(build(tmp_path, extra_alias=alias))
    assert any("renames" in e for e in errs), errs


def test_member_declaring_a_decdn_git_dependency_itself_is_an_error(tmp_path):
    repo = build(tmp_path, member_deps="[dependencies]\ndecdn-client.workspace = true\n"
                 f'[dev-dependencies]\ndecdn-e2e = {{ git = "{GIT}", branch = "main" }}\n')
    errs = errors(repo)
    assert any("crates/app/Cargo.toml" in e and "decdn-e2e" in e for e in errs), errs


def test_member_renaming_a_decdn_crate_is_an_error(tmp_path):
    repo = build(tmp_path, member_deps="[dependencies]\ndecdn-client.workspace = true\n"
                 'proto = { package = "decdn-protocol", version = "0.1" }\n')
    errs = errors(repo)
    assert len(errs) == 1 and "proto" in errs[0] and "decdn-protocol" in errs[0], errs


def test_member_path_that_does_not_resolve_is_an_error(tmp_path):
    repo = build(tmp_path)
    manifest = repo / "Cargo.toml"
    manifest.write_text(manifest.read_text().replace('members = ["crates/app"]',
                                                     'members = ["crates/*"]'))
    errs = errors(repo)
    assert any("crates/*" in e and "globs" in e for e in errs), errs


def test_decdn_crate_locked_from_a_path_is_an_error(tmp_path):
    # What CONTRIBUTING.md's local override leaves in the lock.
    repo = build(tmp_path)
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text() + '\n[[package]]\nname = "decdn-config-types"\n'
                    'version = "0.1.0"\n')
    errs = errors(repo)
    assert len(errs) == 1 and "decdn-config-types" in errs[0] and "path" in errs[0], errs


def test_git_crates_without_a_git_alias_are_an_error(tmp_path):
    repo = build(tmp_path, git={}, member_deps="[dependencies]\ndecdn-client.workspace = true\n")
    errs = errors(repo)
    assert len(errs) == 1 and "no [workspace.dependencies] git alias" in errs[0], errs


# ---- command line --------------------------------------------------------
# open-decdn-bump.sh and the pre-commit hook branch on the exit code.


def test_main_exits_0_when_consistent_and_1_when_not(tmp_path, monkeypatch, capsys):
    import subprocess

    repo = build(tmp_path)
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    monkeypatch.chdir(repo)
    assert cdp.main([]) == 0
    assert "decdn pin OK" in capsys.readouterr().out
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace('version = "0.1.0"', 'version = "0.1.1"', 1))
    assert cdp.main([]) == 1
    assert "inconsistent" in capsys.readouterr().err
