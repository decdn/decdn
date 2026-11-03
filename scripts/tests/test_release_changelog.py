"""Tests for release-changelog.sh, the cargo-release pre-release-hook.

cargo-release runs the hook once per package, with the package directory as
cwd, and also during a preview with DRY_RUN=true. These tests run the real
script with a controlled PATH: a preview must render and check but write
nothing, a real run must hand git-cliff absolute paths under WORKSPACE_ROOT so
every run writes the same file, and the renders git-cliff exits 0 on but a
release must not ship have to fail. The last test renders a small repository
through the real git-cliff and the repo's cliff.toml, and is skipped where
git-cliff is not installed.
"""

from __future__ import annotations

import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/release-changelog.sh"
# Resolved before the tests narrow PATH. The script and the stub need these
# external tools; everything else they run is a bash builtin.
BASH = shutil.which("bash") or "/bin/bash"
COREUTILS = {name: shutil.which(name) for name in ("cat", "grep", "mktemp", "mv", "rm")}
GIT_CLIFF = shutil.which("git-cliff")
GIT = [
    "git",
    "-c", "user.email=t@example.invalid",
    "-c", "user.name=t",
    "-c", "commit.gpgsign=false",
    "-c", "tag.gpgsign=false",
]  # fmt: skip

OLD_CHANGELOG = "# Changelog\n\n## [1.2.2]\n\n- old\n"
# What the real git-cliff leaves: both sections and two trailing blank lines.
RENDERED = "# Changelog\n\n## [1.2.3]\n\n- a line\n\n## [1.2.2]\n\n- old\n\n\n"
WRITTEN = "# Changelog\n\n## [1.2.3]\n\n- a line\n\n## [1.2.2]\n\n- old\n"


def tools_dir(tmp_path: Path) -> Path:
    """A PATH directory holding only the coreutils the script needs."""
    d = tmp_path / "tools"
    d.mkdir()
    for name, path in COREUTILS.items():
        assert path, f"{name} not found on PATH"
        (d / name).symlink_to(path)
    return d


def stub_git_cliff(tools: Path, body: str = RENDERED, exit_code: int = 0) -> Path:
    """A `git-cliff` on PATH that records its arguments, one per line, copies
    `body` to whatever `--output` names, then exits with `exit_code`. The
    real one opens --output before it renders, so a failing stub still
    writes, like a template error does. Returns the argument log."""
    log = tools / "args.log"
    body_file = tools / "body.txt"
    body_file.write_text(body)
    stub = tools / "git-cliff"
    stub.write_text(
        f"#!{BASH}\n"
        f'printf "%s\\n" "$@" > "{log}"\n'
        'out=""\n'
        'while [ $# -gt 0 ]; do [ "$1" = "--output" ] && out="$2"; shift; done\n'
        f'cat "{body_file}" > "$out"\n'
        f"exit {exit_code}\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    return log


def cargo_release_env(root: Path, **override: str) -> dict[str, str]:
    env = {
        "WORKSPACE_ROOT": str(root),
        "PREV_VERSION": "1.2.2",
        "NEW_VERSION": "1.2.3",
        "DRY_RUN": "false",
    }
    env.update(override)
    return env


def run(
    tmp_path: Path, tools: Path, env: dict[str, str], *, cwd: Path | None = None
) -> subprocess.CompletedProcess[str]:
    cwd = cwd or tmp_path / "crates" / "some-crate"
    cwd.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [BASH, str(SCRIPT)],
        cwd=cwd,
        env={"HOME": str(tmp_path), "PATH": str(tools), **env},
        capture_output=True,
        text=True,
        check=False,
    )


def workspace(tmp_path: Path) -> Path:
    """A fake workspace root with a cliff.toml and an existing changelog."""
    (tmp_path / "cliff.toml").write_text("[changelog]\n")
    (tmp_path / "CHANGELOG.md").write_text(OLD_CHANGELOG)
    return tmp_path


def untouched(root: Path) -> bool:
    return (root / "CHANGELOG.md").read_text() == OLD_CHANGELOG and not list(
        root.glob("CHANGELOG.md.*")
    )


@pytest.mark.parametrize("missing", ["WORKSPACE_ROOT", "NEW_VERSION", "PREV_VERSION", "DRY_RUN"])
def test_missing_cargo_release_env_fails(tmp_path, missing):
    env = cargo_release_env(tmp_path)
    del env[missing]
    r = run(tmp_path, tmp_path / "no-tools", env)
    assert r.returncode != 0
    assert "set by cargo-release" in r.stderr


# cargo-release sets the literal "true" or "false"; anything else is a caller
# the script does not know, and must not fall through to a write.
@pytest.mark.parametrize("value", ["1", "TRUE", "yes"])
def test_dry_run_other_than_true_or_false_fails(tmp_path, value):
    tools = tools_dir(tmp_path)
    stub_git_cliff(tools)
    r = run(tmp_path, tools, cargo_release_env(workspace(tmp_path), DRY_RUN=value))
    assert r.returncode == 1
    assert "DRY_RUN must be 'true' or 'false'" in r.stderr
    assert untouched(tmp_path)


def test_dry_run_renders_and_checks_but_writes_nothing(tmp_path):
    tools = tools_dir(tmp_path)
    log = stub_git_cliff(tools)
    r = run(tmp_path, tools, cargo_release_env(workspace(tmp_path), DRY_RUN="true"))
    assert r.returncode == 0, r.stderr
    assert log.exists()
    assert untouched(tmp_path)


@pytest.mark.parametrize("dry_run", ["true", "false"])
def test_missing_git_cliff_fails(tmp_path, dry_run):
    r = run(tmp_path, tools_dir(tmp_path), cargo_release_env(workspace(tmp_path), DRY_RUN=dry_run))
    assert r.returncode == 1
    assert "git-cliff not found on PATH" in r.stderr
    assert untouched(tmp_path)


def test_missing_cliff_toml_fails_instead_of_rendering_the_default_template(tmp_path):
    tools = tools_dir(tmp_path)
    log = stub_git_cliff(tools)
    (tmp_path / "CHANGELOG.md").write_text(OLD_CHANGELOG)
    r = run(tmp_path, tools, cargo_release_env(tmp_path))
    assert r.returncode == 1
    assert "cliff.toml not found" in r.stderr
    assert not log.exists()
    assert untouched(tmp_path)


def test_real_run_hands_git_cliff_absolute_workspace_paths(tmp_path):
    tools = tools_dir(tmp_path)
    log = stub_git_cliff(tools)
    r = run(tmp_path, tools, cargo_release_env(workspace(tmp_path)))
    assert r.returncode == 0, r.stderr
    args = log.read_text().splitlines()
    # Flags may come in any order; what matters is each value.
    flags = dict(zip(args[::2], args[1::2]))
    assert set(flags) == {"--repository", "--config", "--tag", "--output"}
    assert flags["--repository"] == str(tmp_path)
    assert flags["--config"] == str(tmp_path / "cliff.toml")
    assert flags["--tag"] == "v1.2.3"
    # Rendered beside the target, then moved over it.
    assert flags["--output"].startswith(str(tmp_path / "CHANGELOG.md."))
    assert (tmp_path / "CHANGELOG.md").read_text() == WRITTEN
    # Nothing is written relative to the package cwd, and no temporary file stays.
    assert not (tmp_path / "crates" / "some-crate" / "CHANGELOG.md").exists()
    assert list(tmp_path.glob("CHANGELOG.md.*")) == []


@pytest.mark.parametrize("tail", ["", "\n", "\n\n", "\n\n\n\n"])
def test_file_ends_in_exactly_one_newline(tmp_path, tail):
    tools = tools_dir(tmp_path)
    stub_git_cliff(tools, body=WRITTEN.rstrip("\n") + tail)
    r = run(tmp_path, tools, cargo_release_env(workspace(tmp_path)))
    assert r.returncode == 0, r.stderr
    assert (tmp_path / "CHANGELOG.md").read_text() == WRITTEN


def test_git_cliff_failure_aborts_and_leaves_the_old_changelog(tmp_path):
    tools = tools_dir(tmp_path)
    stub_git_cliff(tools, body="# Changelog\n\n## [1.2", exit_code=3)
    r = run(tmp_path, tools, cargo_release_env(workspace(tmp_path)))
    assert r.returncode == 3
    assert untouched(tmp_path)


# Renders git-cliff exits 0 on that a release must not ship.
@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("", "wrote an empty changelog"),
        ("# Changelog\n\n## [1.2.2]\n\n- old\n\n", "no section for 1.2.3"),
        ("# Changelog\n\n## [1.2.3]\n\n- everything\n\n", "no section for the previous release 1.2.2"),
    ],
    ids=["empty", "no-new-section", "no-previous-section"],
)
@pytest.mark.parametrize("dry_run", ["true", "false"])
def test_mis_shaped_render_fails(tmp_path, body, message, dry_run):
    tools = tools_dir(tmp_path)
    stub_git_cliff(tools, body=body)
    r = run(tmp_path, tools, cargo_release_env(workspace(tmp_path), DRY_RUN=dry_run))
    assert r.returncode == 1
    assert message in r.stderr
    assert untouched(tmp_path)


def commit(repo: Path, *messages: str) -> None:
    args = [a for m in messages for a in ("-m", m)]
    subprocess.run([*GIT, "commit", "-q", "--allow-empty", *args], cwd=repo, check=True)


@pytest.mark.skipif(GIT_CLIFF is None, reason="git-cliff is not installed")
def test_real_git_cliff_renders_the_repo_config(tmp_path):
    """The repo's cliff.toml through the real git-cliff: the header, Breaking
    first with the footer as a nested bullet, one blank line between
    sections, skipped `chore(deps)`, a single trailing newline, and the same
    bytes on a second run."""
    repo = tmp_path / "ws"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    shutil.copy(REPO_ROOT / "cliff.toml", repo / "cliff.toml")
    subprocess.run([*GIT, "add", "cliff.toml"], cwd=repo, check=True)
    commit(repo, "chore: init")
    commit(repo, "feat(cli): add thing (#1)")
    commit(repo, "fix!: drop the old wire format (#2)", "BREAKING CHANGE: old nodes cannot connect")
    subprocess.run([*GIT, "tag", "v0.1.0"], cwd=repo, check=True)
    commit(repo, "docs: explain it (#3)")
    commit(repo, "chore(deps): bump foo")
    tools = tools_dir(tmp_path)
    (tools / "git-cliff").symlink_to(GIT_CLIFF)
    env = cargo_release_env(repo, PREV_VERSION="0.1.0", NEW_VERSION="0.2.0")

    r = run(tmp_path, tools, env, cwd=repo / "crates" / "x")
    assert r.returncode == 0, r.stderr
    text = (repo / "CHANGELOG.md").read_text()
    assert text.startswith("# Changelog\n\ngit-cliff generates this file")
    assert "\n\n## [0.2.0] - " in text
    assert "\n\n### Documentation\n\n- Explain it (#3)" in text
    assert "\n\n## [0.1.0] - " in text
    assert "\n\n### Breaking\n\n- Drop the old wire format (#2)" in text
    assert "\n  - old nodes cannot connect\n" in text
    assert "\n\n### Features\n\n- **cli**: Add thing (#1)" in text
    assert "bump foo" not in text and "init" not in text
    assert "\n\n\n" not in text
    assert text.endswith(")\n")
    assert list(repo.glob("CHANGELOG.md.*")) == []

    again = run(tmp_path, tools, env, cwd=repo / "crates" / "x")
    assert again.returncode == 0, again.stderr
    assert (repo / "CHANGELOG.md").read_text() == text
