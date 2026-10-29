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

[package.metadata.binstall.signing]
algorithm = "minisign"
pubkey = "RWQKEYONE"
file = "{ url }.minisig"

[package.metadata.binstall.overrides.x86_64-pc-windows-msvc]
pkg-url = "{ repo }/releases/download/v{ version }/tool-{ version }-{ target }.zip"
pkg-fmt = "zip"
"""

KEYS = {"minisign-one.pub": "RWQKEYONE", "minisign-two.pub": "RWQKEYTWO"}

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
{archive_steps}  upload-assets:
    runs-on: ubuntu-latest
"""

# release.yml's archive steps, which name each archive and lay it out.
ARCHIVE_STEPS = """\
      - run: |
          for build in $BUILDS; do
            bin="${build#*:}"
            tar czf "${bin}-${VERSION}-${TARGET}.tar.gz" -C "target/$TARGET/release" "$bin"
          done
      - shell: pwsh
        run: |
          foreach ($build in $env:BUILDS -split ' ') {
            $bin = ($build -split ':')[1]
            Compress-Archive -Path "target/$env:TARGET/release/$bin.exe" `
              -DestinationPath "$bin-$env:VERSION-$env:TARGET.zip"
          }
"""


def workflow(builds: str = "tool-cli:tool", archive_steps: str = ARCHIVE_STEPS) -> str:
    return WORKFLOW.format(builds=builds, archive_steps=archive_steps)


def make_repo(
    tmp_path: Path,
    *,
    binstall: str = BINSTALL,
    builds: str = "tool-cli:tool",
    text: str | None = None,
    extra_members: dict[str, str] | None = None,
    keys: dict[str, str] = KEYS,
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
    (tmp_path / "crates/cli").mkdir(exist_ok=True)
    for file, key in keys.items():
        (tmp_path / "crates/cli" / file).write_text(f"untrusted comment: {file}\n{key}\n")
    wf = tmp_path / ".github/workflows/release.yml"
    wf.parent.mkdir(parents=True)
    wf.write_text(text if text is not None else workflow(builds))
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
    builds, errors = cbm.parse_build_matrix(workflow())
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


def test_a_cfg_override_is_refused_once_per_crate(tmp_path):
    binstall = BINSTALL.replace(
        "overrides.x86_64-pc-windows-msvc]", "overrides.'cfg(target_os = \"windows\")']"
    )
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert len(errors_containing(errors, "key overrides by exact target triple")) == 1
    # And the guard models what an older binstall does: the base settings.
    assert errors_containing(errors, "(x86_64-pc-windows-msvc): binstall `pkg-fmt` resolves to")


def test_a_misspelled_override_triple_leaves_the_target_on_the_base(tmp_path):
    binstall = BINSTALL.replace(
        "overrides.x86_64-pc-windows-msvc]", "overrides.x86_64-pc-windows-msv]"
    )
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(
        errors, "(x86_64-pc-windows-msvc): binstall `pkg-fmt` resolves to 'tgz'"
    )


def test_an_override_for_an_unbuilt_target_is_ignored(tmp_path):
    binstall = (
        BINSTALL
        + """
[package.metadata.binstall.overrides.x86_64-unknown-linux-musl]
pkg-url = "nowhere"
"""
    )
    assert cbm.check(make_repo(tmp_path, binstall=binstall)) == []


def test_an_override_merges_with_the_base_key_by_key(tmp_path):
    # The override sets pkg-url and pkg-fmt correctly; its bad bin-dir must
    # still be caught, and only for the target it applies to.
    binstall = BINSTALL.replace(
        'pkg-fmt = "zip"\n', 'pkg-fmt = "zip"\nbin-dir = "tool-{ target }/{ bin }{ binary-ext }"\n'
    )
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert len(errors) == 1, errors
    assert errors_containing(errors, "(x86_64-pc-windows-msvc): binstall `bin-dir` resolves to")


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
    assert errors_containing(errors, "uses placeholder 'triple', which binstall does not offer")


def test_bin_in_pkg_url_is_refused(tmp_path):
    # Renders to the right URL, but binstall offers `pkg-url` no `{ bin }`: it
    # fails to render the template and compiles from source.
    binstall = BINSTALL.replace("tool-{ version }", "{ bin }-{ version }")
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert len(errors_containing(errors, "`pkg-url` uses placeholder 'bin'")) == 2, errors


def test_an_archive_placeholder_in_bin_dir_is_refused(tmp_path):
    binstall = BINSTALL.replace('"{ bin }{ binary-ext }"', '"{ bin }{ archive-suffix }"')
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert len(errors_containing(errors, "`bin-dir` uses placeholder 'archive-suffix'")) == 2


def test_format_in_bin_dir_is_the_binary_extension(tmp_path):
    binstall = BINSTALL.replace('"{ bin }{ binary-ext }"', '"{ bin }{ format }"')
    assert cbm.check(make_repo(tmp_path, binstall=binstall)) == []


def test_a_tgz_archive_format_may_be_spelled_tar_gz(tmp_path):
    # binstall tries both `tgz` and `tar.gz` as the format of a tgz archive.
    binstall = BINSTALL.replace("{ target }.tar.gz", "{ target }.{ archive-format }", 1)
    assert cbm.check(make_repo(tmp_path, binstall=binstall)) == []


@pytest.mark.parametrize("key", ["pkg-url", "pkg-fmt", "bin-dir"])
def test_a_missing_base_key_fails(tmp_path, key):
    binstall = (
        "\n".join(
            line for line in BINSTALL.split("\n\n")[0].splitlines() if not line.startswith(key)
        )
        + "\n"
        + BINSTALL.split("\n\n", 1)[1]
    )
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    missing = errors_containing(errors, f"binstall `{key}` is not set")
    # The Windows override still sets pkg-url and pkg-fmt.
    assert len(missing) == (2 if key == "bin-dir" else 1), errors
    assert errors_containing(missing, "(x86_64-unknown-linux-gnu)")


def test_disabled_strategies_as_a_string_fails(tmp_path):
    binstall = BINSTALL.replace('["quick-install"]', '"quick-install"')
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, "`disabled-strategies` must be a list")


def test_an_empty_matrix_fails(tmp_path):
    text = "jobs:\n  build-binaries:\n    strategy:\n      matrix:\n        include:\n" + (
        "    steps:\n      - run: x\n"
    )
    # The reader finds no rows and no error; check() is what refuses it.
    assert cbm.parse_build_matrix(text) == ([], [])
    errors = cbm.check(make_repo(tmp_path, text=text))
    assert errors_containing(errors, "matrix is empty; checked nothing")


@pytest.mark.parametrize("line", cbm.ARCHIVE_STEP_LINES)
def test_a_changed_archive_step_fails(tmp_path, line):
    steps = "\n".join(
        raw.replace(line, "echo renamed") if raw.strip() == line else raw
        for raw in ARCHIVE_STEPS.split("\n")
    )
    assert steps != ARCHIVE_STEPS
    errors = cbm.check(make_repo(tmp_path, text=workflow(archive_steps=steps)))
    assert errors_containing(errors, f"no longer contains the archive step line {line!r}"), errors


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
    "text, needle",
    [
        ("jobs:\n  other:\n    runs-on: x\n", "no `build-binaries` job"),
        ("jobs:\n  build-binaries:\n    runs-on: x\n", "has no `matrix.include`"),
        (
            (
                "jobs:\n  build-binaries:\n    strategy:\n      matrix:\n        include:\n"
                "          - runner: x\n            target: y\n"
            ),
            "opens with `runner:`",
        ),
        (
            (
                "jobs:\n  build-binaries:\n    strategy:\n      matrix:\n        include:\n"
                "          - target: x86_64-unknown-linux-gnu\n            archive: tar.gz\n"
            ),
            "has no builds",
        ),
    ],
)
def test_an_unreadable_matrix_fails(tmp_path, text, needle):
    errors = cbm.check(make_repo(tmp_path, text=text))
    assert errors_containing(errors, needle), errors


def test_an_unknown_archive_format_fails(tmp_path):
    text = workflow().replace("archive: zip", "archive: 7z")
    errors = cbm.check(make_repo(tmp_path, text=text))
    assert errors_containing(errors, "archive '7z'")


def test_a_flow_style_row_is_refused_not_skipped(tmp_path):
    # The Linux row is correct; the Windows row, if skipped, would go unchecked
    # and the check would pass.
    text = workflow().replace(
        """          - target: x86_64-pc-windows-msvc
            runner: windows-latest
            builds: tool-cli:tool
            archive: zip
""",
        """          - {target: x86_64-pc-windows-msvc, builds: "tool-cli:tool", archive: zip}
""",
    )
    errors = cbm.check(make_repo(tmp_path, text=text))
    assert errors_containing(errors, "is not a `key: value` scalar"), errors


MATRIX_HEAD = "jobs:\n  build-binaries:\n    strategy:\n      matrix:\n        include:\n"
ROW = (
    "          - target: x86_64-unknown-linux-gnu\n"
    "            builds: tool-cli:tool\n"
    "            archive: tar.gz\n"
)


@pytest.mark.parametrize(
    "lines, offending",
    [
        # A key before the first row.
        ("          runner: ubuntu-latest\n" + ROW, "runner: ubuntu-latest"),
        # A nested map under a row key.
        (ROW + "            env:\n              target: aarch64-apple-darwin\n", "env:"),
        # A block scalar.
        (
            ROW.replace("builds: tool-cli:tool", "builds: >-\n              tool-cli:tool"),
            "builds: >-",
        ),
        # A flow sequence.
        (
            ROW.replace("builds: tool-cli:tool", "builds: [tool-cli:tool]"),
            "builds: [tool-cli:tool]",
        ),
        # A key indented off the row's keys.
        (ROW + "              archive: zip\n", "archive: zip"),
        # A line that is not `key: value` at all.
        (ROW + "          - x86_64-apple-darwin\n", "- x86_64-apple-darwin"),
    ],
    ids=["key-before-row", "nested-map", "block-scalar", "flow-seq", "off-indent", "bare-item"],
)
def test_an_unsupported_include_line_is_refused(lines, offending):
    _, errors = cbm.parse_build_matrix(MATRIX_HEAD + lines + "    steps:\n      - run: x\n")
    # Each case names its own line, so it passes through the check it names.
    assert errors_containing(errors, f"line {offending!r} is not a `key: value` scalar"), errors


# --- signing ---------------------------------------------------------------


def test_missing_signing_fails(tmp_path):
    binstall = BINSTALL.replace("[package.metadata.binstall.signing]", "[package.metadata.x]")
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, "no [package.metadata.binstall.signing]")


def test_another_algorithm_fails(tmp_path):
    binstall = BINSTALL.replace('algorithm = "minisign"', 'algorithm = "gpg"')
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, '`algorithm` must be "minisign"')


def test_the_default_sig_file_fails(tmp_path):
    binstall = BINSTALL.replace('file = "{ url }.minisig"\n', "")
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, "signing `file` must be '{ url }.minisig'")


def test_a_key_no_maintainer_holds_fails(tmp_path):
    binstall = BINSTALL.replace("RWQKEYONE", "RWQSTRANGER")
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, "is not the key line of any crates/cli/minisign*.pub")


def test_any_maintainer_key_passes(tmp_path):
    binstall = BINSTALL.replace("RWQKEYONE", "RWQKEYTWO")
    assert cbm.check(make_repo(tmp_path, binstall=binstall)) == []


def test_crates_naming_different_keys_fail(tmp_path):
    other = (
        CRATE.format(binstall=BINSTALL.replace("RWQKEYONE", "RWQKEYTWO"))
        .replace('name = "tool-cli"', 'name = "tool-node"')
        .replace('name = "tool"', 'name = "tool-node"')
        .replace("tool-{ version }", "tool-node-{ version }")
    )
    errors = cbm.check(
        make_repo(
            tmp_path, builds="tool-cli:tool tool-node:tool-node", extra_members={"node": other}
        )
    )
    assert errors_containing(errors, "`pubkey` differs between crates")


@pytest.mark.parametrize(
    "line",
    [
        'pubkey  = "RWQKEYONE"',
        "pubkey = 'RWQKEYONE'",
        'pubkey = "RWQKEYONE"  # the maintainer key',
    ],
)
def test_a_pubkey_line_sign_release_cannot_read_fails(tmp_path, line):
    binstall = BINSTALL.replace('pubkey = "RWQKEYONE"', line)
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, 'exactly `pubkey = "<key>"`')


@pytest.mark.parametrize(
    "extra",
    [
        '\n[package.metadata.other]\npubkey = "RWQKEYTWO"\n',
        # Forms sign-release.sh's sed skips: only the any-assignment rule sees them.
        '\n[package.metadata.other]\npubkey="RWQKEYTWO"\n',
        '\n[package.metadata.other]\nsigning.pubkey = "RWQKEYTWO"\n',
        '\n[package.metadata.other]\nsigning = { pubkey = "RWQKEYTWO" }\n',
    ],
    ids=["exact", "no-spaces", "dotted", "inline"],
)
def test_a_second_pubkey_assignment_fails(tmp_path, extra):
    crate = CRATE.format(binstall=BINSTALL) + extra
    repo = make_repo(tmp_path)
    (repo / "crates/tool/Cargo.toml").write_text(crate)
    errors = cbm.check(repo)
    assert errors_containing(errors, "assign `pubkey` nowhere else"), errors


def test_a_crlf_pubkey_line_fails(tmp_path):
    # sign-release.sh's sed does not match `pubkey = "…"\r`.
    repo = make_repo(tmp_path)
    path = repo / "crates/tool/Cargo.toml"
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    errors = cbm.check(repo)
    assert errors_containing(errors, 'exactly `pubkey = "<key>"`'), errors


def test_a_missing_algorithm_fails(tmp_path):
    binstall = BINSTALL.replace('algorithm = "minisign"\n', "")
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, '`algorithm` must be "minisign"')


@pytest.mark.parametrize(
    "signing",
    [
        (
            "\n[package.metadata.binstall.overrides.x86_64-pc-windows-msvc.signing]\n"
            + 'algorithm = "minisign"\nfile = "{ url }.minisig"\n'
        ),
        'signing = { algorithm = "minisign", file = "{ url }.sig" }\n',
        'signing.file = "{ url }.sig"\n',
    ],
    ids=["table", "inline", "dotted"],
)
def test_signing_in_an_override_is_refused(tmp_path, signing):
    # binstall lets an override's `signing` replace the base table for that
    # target; none of these assign `pubkey`, so only this rule sees them.
    if signing.startswith("\n"):
        binstall = BINSTALL + signing
    else:
        binstall = BINSTALL.replace('pkg-fmt = "zip"\n', 'pkg-fmt = "zip"\n' + signing)
    errors = cbm.check(make_repo(tmp_path, binstall=binstall))
    assert errors_containing(errors, "override 'x86_64-pc-windows-msvc' carries `signing`"), errors


def test_a_pubkey_in_a_crate_without_binstall_metadata_fails(tmp_path):
    other = '[package]\nname = "lib"\nversion.workspace = true\n\n[package.metadata.x]\npubkey = "RWQKEYONE"\n'
    errors = cbm.check(make_repo(tmp_path, extra_members={"lib": other}))
    assert errors_containing(
        errors, "crates/lib/Cargo.toml: assigns `pubkey` but has no binstall"
    ), errors
