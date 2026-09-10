"""Regression tests for the toolchain-pin checker.

The Rust version is written in three kinds of place — `rust-toolchain.toml`,
`Cargo.toml`'s `rust-version`, and every `dtolnay/rust-toolchain@…` ref in the
workflows — and each is read by a different tool, so none can be derived from
another. A bug here degrades to a quiet false pass: the check says the sites
agree while CI compiles on a different compiler from every developer.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_toolchain_pin.py"

_spec = importlib.util.spec_from_file_location("check_toolchain_pin", MODULE_PATH)
assert _spec and _spec.loader
ctp = importlib.util.module_from_spec(_spec)
sys.modules["check_toolchain_pin"] = ctp
_spec.loader.exec_module(ctp)


def build_repo(
    tmp_path: Path,
    *,
    channel: str = "1.95.0",
    rust_version: str = "1.95.0",
    refs: dict[str, list[str]] | None = None,
) -> Path:
    """A repo with the three kinds of site.

    `refs` maps a workflow filename to the action versions it references; the
    default is two workflows carrying two refs each, like ci.yml/release.yml.
    """
    if refs is None:
        refs = {"ci.yml": ["1.95.0", "1.95.0"], "release.yml": ["1.95.0"]}
    (tmp_path / "rust-toolchain.toml").write_text(
        f'[toolchain]\nchannel = "{channel}"\ncomponents = ["rustfmt", "clippy"]\n'
    )
    (tmp_path / "Cargo.toml").write_text(
        f'[workspace]\nmembers = []\n\n[workspace.package]\nrust-version = "{rust_version}"\n'
    )
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    for name, versions in refs.items():
        body = "jobs:\n  build:\n    steps:\n"
        for v in versions:
            body += f"      - uses: dtolnay/rust-toolchain@{v}\n"
        (workflows / name).write_text(body)
    return tmp_path


def test_agreeing_sites_pass(tmp_path):
    assert ctp.check(build_repo(tmp_path)) == []


def test_drifted_toolchain_file_is_named(tmp_path):
    errors = ctp.check(build_repo(tmp_path, channel="1.96.0"))
    assert len(errors) == 1, errors
    assert "rust-toolchain.toml" in errors[0]
    assert "1.96.0" in errors[0]


def test_drifted_rust_version_is_named(tmp_path):
    errors = ctp.check(build_repo(tmp_path, rust_version="1.94.0"))
    assert len(errors) == 1, errors
    assert "Cargo.toml" in errors[0]


def test_one_drifted_action_ref_is_named_with_its_line(tmp_path):
    repo = build_repo(tmp_path, refs={"ci.yml": ["1.95.0", "1.96.0", "1.95.0"]})
    errors = ctp.check(repo)
    assert len(errors) == 1, errors
    assert "ci.yml:5" in errors[0], errors[0]
    assert "1.96.0" in errors[0]


def test_two_part_rust_version_is_refused_even_when_it_would_match(tmp_path):
    """`1.95` satisfies cargo but is not the string the other sites carry."""
    errors = ctp.check(build_repo(tmp_path, rust_version="1.95"))
    assert len(errors) == 1, errors
    assert "Cargo.toml" in errors[0]
    assert "X.Y.Z" in errors[0]


def test_floating_channel_is_refused(tmp_path):
    errors = ctp.check(build_repo(tmp_path, channel="stable"))
    assert any("rust-toolchain.toml" in e and "stable" in e for e in errors), errors


def test_zero_action_refs_is_a_failure_not_a_pass(tmp_path):
    """A regex that matches nothing must not report agreement."""
    errors = ctp.check(build_repo(tmp_path, refs={"ci.yml": []}))
    assert len(errors) == 1, errors
    assert "no dtolnay/rust-toolchain" in errors[0]


def test_a_comment_naming_the_action_is_not_a_site(tmp_path):
    repo = build_repo(tmp_path)
    ci = repo / ".github/workflows/ci.yml"
    ci.write_text("# bump every dtolnay/rust-toolchain@… ref together\n" + ci.read_text())
    assert ctp.check(repo) == []


def test_missing_workspace_rust_version_is_a_failure(tmp_path):
    repo = build_repo(tmp_path)
    (repo / "Cargo.toml").write_text("[workspace]\nmembers = []\n")
    errors = ctp.check(repo)
    assert len(errors) == 1, errors
    assert "rust-version" in errors[0]


def test_the_real_repository_agrees():
    assert ctp.check(REPO_ROOT) == []
