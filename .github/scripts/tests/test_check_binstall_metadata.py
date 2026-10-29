"""Regression tests for the binstall-metadata checker.

A crate whose `pkg-url` no longer names its release archive still releases
green; `cargo binstall` then quietly compiles from source. This check is the
only thing that notices, and a bug in it is a quiet false pass.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_binstall_metadata.py"

_spec = importlib.util.spec_from_file_location("check_binstall_metadata", MODULE_PATH)
assert _spec and _spec.loader
cbm = importlib.util.module_from_spec(_spec)
sys.modules["check_binstall_metadata"] = cbm
_spec.loader.exec_module(cbm)


ROOT = """\
[workspace]
members = ["crates/tool"]

[workspace.package]
version = "1.2.3"
repository = "https://github.com/example/tool"
"""

BINSTALL = """\
[package.metadata.binstall]
pkg-url = "{ repo }/releases/download/v{ version }/tool-{ version }-{ target }.tar.gz"
pkg-fmt = "tgz"
bin-dir = "{ bin }{ binary-ext }"
disabled-strategies = ["quick-install"]

[package.metadata.binstall.overrides.'cfg(target_os = "windows")']
pkg-url = "{ repo }/releases/download/v{ version }/tool-{ version }-{ target }.zip"
pkg-fmt = "zip"
"""

CRATE = """\
[package]
name = "tool-cli"
version.workspace = true
repository.workspace = true

{binstall}
[[bin]]
name = "tool"
path = "src/main.rs"
"""

WORKFLOW = """\
name: Release
jobs:
  tag:
    runs-on: ubuntu-latest
  build-binaries:
    name: Build ${{{{ matrix.target }}}}
    strategy:
      fail-fast: false
      matrix:
        include:
          # A comment between rows.
          - target: x86_64-unknown-linux-gnu
            runner: ubuntu-latest
            builds: {builds}
            archive: tar.gz
            cross: true
          - target: x86_64-pc-windows-msvc
            runner: windows-latest
            builds: {builds}
            archive: zip
    steps:
      - uses: actions/checkout@v7
        with:
          target: not-a-matrix-row
  upload-assets:
    runs-on: ubuntu-latest
"""


def make_repo(
    tmp_path: Path,
    *,
    binstall: str = BINSTALL,
    builds: str = "tool-cli:tool",
    workflow: str | None = None,
    extra_members: dict[str, str] | None = None,
) -> Path:
    members = ["crates/tool", *(f"crates/{d}" for d in (extra_members or {}))]
    root = ROOT.replace('["crates/tool"]', repr(members).replace("'", '"'))
    (tmp_path / "Cargo.toml").write_text(root)
    crate = tmp_path / "crates/tool"
    crate.mkdir(parents=True)
    (crate / "Cargo.toml").write_text(CRATE.format(binstall=binstall))
    for directory, manifest in (extra_members or {}).items():
        (tmp_path / "crates" / directory).mkdir(parents=True)
        (tmp_path / "crates" / directory / "Cargo.toml").write_text(manifest)
    wf = tmp_path / ".github/workflows/release.yml"
    wf.parent.mkdir(parents=True)
    wf.write_text(workflow if workflow is not None else WORKFLOW.format(builds=builds))
    return tmp_path


def errors_containing(errors: list[str], needle: str) -> list[str]:
    return [e for e in errors if needle in e]


# --- the real repository ---------------------------------------------------


def test_the_repository_passes():
    assert cbm.check(REPO_ROOT) == []


def test_the_line_reader_agrees_with_yaml_on_the_real_release_workflow():
    text = (REPO_ROOT / cbm.RELEASE_WORKFLOW).read_text()
    builds, errors = cbm.parse_build_matrix(text)
    assert errors == []
    rows = yaml.safe_load(text)["jobs"][cbm.BUILD_JOB]["strategy"]["matrix"]["include"]
    expected = [
        cbm.Build(
            row["target"],
            tuple(tuple(e.split(":", 1)) for e in row["builds"].split()),
            row["archive"],
        )
        for row in rows
    ]
    assert builds == expected


# --- the fixture -----------------------------------------------------------


def test_a_matching_crate_passes(tmp_path):
    assert cbm.check(make_repo(tmp_path)) == []


def test_the_line_reader_skips_comments_and_stops_at_steps(tmp_path):
    builds, errors = cbm.parse_build_matrix(WORKFLOW.format(builds="tool-cli:tool"))
    assert errors == []
    assert [b.target for b in builds] == ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"]
    assert builds[1].archive == "zip"


def test_a_renamed_archive_fails(tmp_path):
    errors = cbm.check(
        make_repo(tmp_path, binstall=BINSTALL.replace("tool-{ version }", "tool-v{ version }"))
    )
    assert len(errors_containing(errors, "`pkg-url` resolves to")) == 2


def test_a_crate_named_differently_from_its_binary_needs_its_own_url(tmp_path):
    # `{ name }` is the package, `tool-cli`; the archive is named for `tool`.
    errors = cbm.check(
        make_repo(tmp_path, binstall=BINSTALL.replace("tool-{ version }", "{ name }-{ version }"))
    )
    assert errors_containing(errors, "tool-cli-1.2.3-x86_64-unknown-linux-gnu.tar.gz")


def test_missing_binstall_metadata_fails(tmp_path):
    errors = cbm.check(make_repo(tmp_path, binstall=""))
    assert errors_containing(errors, "has no [package.metadata.binstall]")


def test_quick_install_left_enabled_fails(tmp_path):
    errors = cbm.check(
        make_repo(tmp_path, binstall=BINSTALL.replace('"quick-install"', '"compile"'))
    )
    assert errors_containing(errors, '"quick-install"')


def test_a_missing_windows_override_fails(tmp_path):
    binstall = BINSTALL.split("\n\n[package.metadata.binstall.overrides")[0] + "\n"
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    windows = errors_containing(errors, "x86_64-pc-windows-msvc")
    assert errors_containing(windows, "`pkg-url`")
    assert errors_containing(windows, "`pkg-fmt` resolves to 'tgz'")


def test_an_exact_target_override_beats_cfg(tmp_path):
    binstall = (
        BINSTALL
        + """
[package.metadata.binstall.overrides.x86_64-pc-windows-msvc]
pkg-fmt = "tgz"
"""
    )
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(
        errors, "(x86_64-pc-windows-msvc): binstall `pkg-fmt` resolves to 'tgz'"
    )


def test_an_unevaluable_cfg_override_is_refused(tmp_path):
    binstall = (
        BINSTALL
        + """
[package.metadata.binstall.overrides.'cfg(all(windows, target_arch = "aarch64"))']
pkg-fmt = "zip"
"""
    )
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, "only exact targets and cfg(target_os")


def test_a_bin_dir_inside_a_folder_fails(tmp_path):
    binstall = BINSTALL.replace(
        '"{ bin }{ binary-ext }"', '"tool-{ target }/{ bin }{ binary-ext }"'
    )
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert len(errors_containing(errors, "`bin-dir` resolves to")) == 2


def test_windows_bin_dir_carries_exe(tmp_path):
    binstall = BINSTALL.replace('"{ bin }{ binary-ext }"', '"{ bin }"')
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(
        errors, "(x86_64-pc-windows-msvc): binstall `bin-dir` resolves to 'tool'"
    )
    assert not errors_containing(errors, "x86_64-unknown-linux-gnu")


def test_an_unknown_placeholder_fails(tmp_path):
    errors = cbm.check(
        make_repo(tmp_path, binstall=BINSTALL.replace("{ target }.tar.gz", "{ triple }.tar.gz"))
    )
    assert errors_containing(errors, "unknown placeholder 'triple'")


def test_a_binary_the_crate_does_not_build_fails(tmp_path):
    errors = cbm.check(make_repo(tmp_path, builds="tool-cli:toolz"))
    assert errors_containing(errors, "the matrix ships binary 'toolz'")


def test_a_package_that_is_not_a_member_fails(tmp_path):
    errors = cbm.check(make_repo(tmp_path, builds="tool-cli:tool ghost:ghost"))
    assert errors_containing(errors, "builds ghost, which is not a workspace member")


def test_a_malformed_builds_entry_fails(tmp_path):
    errors = cbm.check(make_repo(tmp_path, builds="tool-cli:tool tool-cli"))
    assert errors_containing(errors, "builds entry 'tool-cli' is not package:binary")


def test_metadata_on_a_crate_the_matrix_does_not_build_fails(tmp_path):
    other = """\
[package]
name = "orphan"
version.workspace = true

[package.metadata.binstall]
pkg-url = "x"
"""
    errors = cbm.check(make_repo(tmp_path, extra_members={"orphan": other}))
    assert errors_containing(errors, "orphan/Cargo.toml: has [package.metadata.binstall]")


@pytest.mark.parametrize(
    "workflow, needle",
    [
        ("jobs:\n  other:\n    runs-on: x\n", "no `build-binaries` job"),
        ("jobs:\n  build-binaries:\n    runs-on: x\n", "has no `matrix.include`"),
        (
            "jobs:\n  build-binaries:\n    strategy:\n      matrix:\n        include:\n"
            "          - runner: x\n            target: y\n",
            "opens with `runner:`",
        ),
        (
            "jobs:\n  build-binaries:\n    strategy:\n      matrix:\n        include:\n"
            "          - target: x86_64-unknown-linux-gnu\n            archive: tar.gz\n",
            "has no builds",
        ),
    ],
)
def test_an_unreadable_matrix_fails(tmp_path, workflow, needle):
    errors = cbm.check(make_repo(tmp_path, workflow=workflow))
    assert errors_containing(errors, needle), errors


def test_an_unknown_archive_format_fails(tmp_path):
    workflow = WORKFLOW.format(builds="tool-cli:tool").replace("archive: zip", "archive: 7z")
    errors = cbm.check(make_repo(tmp_path, workflow=workflow))
    assert errors_containing(errors, "archive '7z'")
