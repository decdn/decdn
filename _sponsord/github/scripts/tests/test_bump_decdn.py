"""Regression tests for the decdn bump script.

open-decdn-bump.sh opens its PR from what this script writes. A rewrite
that misses an alias, or a changelog entry in the wrong crate, reaches review
looking like a finished bump.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/bump_decdn.py"

_spec = importlib.util.spec_from_file_location("bump_decdn", MODULE_PATH)
assert _spec and _spec.loader
bd = importlib.util.module_from_spec(_spec)
sys.modules["bump_decdn"] = bd
_spec.loader.exec_module(bd)

from check_decdn_pin import check  # noqa: E402  (bump_decdn put its dir on sys.path)

SHA = "f6de71c2b231c6d5683b8f9193add163a9444fdd"
GIT = "https://github.com/decdn/decdn"
UNRELEASED = """# Changelog

## [Unreleased]

## [0.0.1] - 2026-10-09

### Added

- Something.
"""


def build(tmp_path: Path, *, ref: str = 'tag = "v0.0.1"', version: str = "0.0.1") -> Path:
    """A workspace with a published lib and CLI on decdn, a dev-only user and an e2e crate."""
    repo = tmp_path / "repo"
    members = {
        # name: (member Cargo.toml body after [package] name)
        "lib": '[dependencies]\ndecdn-client.workspace = true\n',
        "cli": "[target.'cfg(unix)'.dependencies]\ndecdn-incentive.workspace = true\n",
        "api": '[dependencies]\nserde.workspace = true\n'
               '[dev-dependencies]\ndecdn-e2e.workspace = true\n',
        "e2e": 'publish = false\n[dependencies]\ndecdn-client.workspace = true\n',
    }
    for name, body in members.items():
        (repo / "crates" / name).mkdir(parents=True)
        # `publish` belongs to [package]; the rest are tables after it.
        (repo / "crates" / name / "Cargo.toml").write_text(f'[package]\nname = "{name}"\n{body}')
        (repo / "crates" / name / "CHANGELOG.md").write_text(UNRELEASED)
    (repo / "Cargo.toml").write_text(
        "[workspace]\n"
        f"members = [{', '.join(repr(f'crates/{m}') for m in members)}]\n\n"
        "[workspace.dependencies]\n"
        "# decdn, at a release tag.\n"
        f'decdn-client = {{ git = "{GIT}", {ref}, version = "{version}" }}\n'
        f'decdn-incentive = {{ git = "{GIT}", {ref}, version = "{version}" }}\n'
        f'decdn-e2e = {{ git = "{GIT}", {ref} }}\n'
        'serde = "1"\n'
    )
    lock = ["version = 4"]
    for name in ("decdn-client", "decdn-incentive", "decdn-e2e"):
        lock += ["", "[[package]]", f'name = "{name}"', f'version = "{version}"',
                 f'source = "git+{GIT}?tag=v{version}#{SHA}"']
    (repo / "Cargo.lock").write_text("\n".join(lock) + "\n")
    return repo


def stub_fetch(monkeypatch, responses: dict[str, object]) -> None:
    """Route bump_decdn.fetch to `responses` (status, body) or an exception to raise."""

    def fetch(url: str):
        result = responses[url]
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(bd, "fetch", fetch)


# ---- rewrite -------------------------------------------------------------


@pytest.mark.parametrize("ref", ['tag = "v0.0.1"', 'branch = "main"', 'rev = "abc1234"'])
def test_rewrite_moves_every_alias_and_keeps_the_rest(tmp_path, ref):
    repo = build(tmp_path, ref=ref, version="0.0.0" if "tag" not in ref else "0.0.1")
    names = bd.rewrite(repo, "v0.1.0")
    assert names == ["decdn-client", "decdn-incentive", "decdn-e2e"]
    text = (repo / "Cargo.toml").read_text()
    assert text.count('tag = "v0.1.0"') == 3
    assert text.count('version = "0.1.0"') == 2  # decdn-e2e stays version-less
    assert "# decdn, at a release tag.\n" in text
    assert 'serde = "1"\n' in text


def test_rewrite_refuses_a_non_release_tag(tmp_path):
    repo = build(tmp_path)
    before = (repo / "Cargo.toml").read_text()
    with pytest.raises(bd.BumpError, match="not a decdn release tag"):
        bd.rewrite(repo, "main")
    assert (repo / "Cargo.toml").read_text() == before


def test_rewrite_fails_on_an_alias_it_cannot_see(tmp_path):
    repo = build(tmp_path)
    manifest = repo / "Cargo.toml"
    # A multi-line alias parses fine but escapes the line match.
    manifest.write_text(manifest.read_text().replace(
        f'decdn-e2e = {{ git = "{GIT}", tag = "v0.0.1" }}',
        f'[workspace.dependencies.decdn-e2e]\ngit = "{GIT}"\ntag = "v0.0.1"',
    ).replace('serde = "1"\n', ""))
    with pytest.raises(bd.BumpError, match="decdn-e2e not rewritten"):
        bd.rewrite(repo, "v0.1.0")


def test_rewrite_fails_without_any_alias(tmp_path):
    repo = build(tmp_path)
    (repo / "Cargo.toml").write_text('[workspace]\nmembers = []\n[workspace.dependencies]\n')
    with pytest.raises(bd.BumpError, match="no decdn alias"):
        bd.rewrite(repo, "v0.1.0")


# ---- changelog -----------------------------------------------------------


def test_changelog_reaches_published_non_dev_users_only(tmp_path):
    repo = build(tmp_path)
    written = bd.changelog(repo, "v0.0.1")
    assert sorted(p.parent.name for p in written) == ["cli", "lib"]
    assert (repo / "crates/api/CHANGELOG.md").read_text() == UNRELEASED
    assert (repo / "crates/e2e/CHANGELOG.md").read_text() == UNRELEASED


def test_changelog_entry_opens_a_changed_section(tmp_path):
    repo = build(tmp_path)
    bd.changelog(repo, "v0.0.1")
    text = (repo / "crates/lib/CHANGELOG.md").read_text()
    assert text.startswith(
        "# Changelog\n\n## [Unreleased]\n\n### Changed\n\n"
        "- **decdn v0.0.1.** The decdn crates are locked at decdn's `v0.0.1` release tag\n"
        f"  (`{SHA}`) and require `0.0.1`, which is on\n"
        "  crates.io.\n\n## [0.0.1] - 2026-10-09\n"
    )
    assert all(len(line) <= 80 for line in text.splitlines())


def test_changelog_entry_joins_an_existing_changed_section(tmp_path):
    existing = UNRELEASED.replace(
        "## [Unreleased]\n", "## [Unreleased]\n\n### Changed\n\n- Earlier change.\n"
    )
    out = bd.add_entry(existing, "v0.1.0", SHA)
    assert out.count("### Changed") == 1
    unreleased = out.split("## [0.0.1]")[0]
    assert unreleased.index("**decdn v0.1.0.**") < unreleased.index("- Earlier change.")


def test_changelog_ignores_a_changed_heading_of_an_older_release(tmp_path):
    old = UNRELEASED.replace("### Added", "### Changed")
    out = bd.add_entry(old, "v0.1.0", SHA)
    assert out.count("### Changed") == 2
    assert out.index("**decdn v0.1.0.**") < out.index("## [0.0.1]")


def test_changelog_is_idempotent(tmp_path):
    once = bd.add_entry(UNRELEASED, "v0.1.0", SHA)
    assert bd.add_entry(once, "v0.1.0", SHA) == once


def test_changelog_replaces_an_unreleased_bump_to_an_older_tag():
    # Two decdn releases before one sponsord release: only the last lock is true.
    first = bd.add_entry(UNRELEASED, "v0.1.0", SHA)
    second = bd.add_entry(first, "v0.1.1", "0" * 40)
    assert "**decdn v0.1.0.**" not in second
    assert second == bd.add_entry(UNRELEASED, "v0.1.1", "0" * 40)


def test_changelog_keeps_other_changed_entries_when_replacing():
    existing = UNRELEASED.replace(
        "## [Unreleased]\n", "## [Unreleased]\n\n### Changed\n\n- Earlier change.\n"
    )
    second = bd.add_entry(bd.add_entry(existing, "v0.1.0", SHA), "v0.1.1", SHA)
    assert second == bd.add_entry(existing, "v0.1.1", SHA)
    assert "- Earlier change." in second


def test_changelog_opens_changed_in_section_order():
    # RELEASING.md § Changelogs: Changed (BREAKING), Added, Changed, Fixed, ...
    existing = UNRELEASED.replace("## [Unreleased]\n", (
        "## [Unreleased]\n\n### Changed (BREAKING)\n\n- Broke.\n\n"
        "### Added\n\n- New.\n\n### Fixed\n\n- Bug.\n"
    ))
    unreleased = bd.add_entry(existing, "v0.1.0", SHA).split("## [0.0.1]")[0]
    order = ["### Changed (BREAKING)", "### Added", "### Changed\n", "### Fixed"]
    positions = [unreleased.index(h) for h in order]
    assert positions == sorted(positions)
    assert "\n\n\n" not in unreleased


def test_changelog_opens_changed_at_the_end_of_unreleased():
    existing = UNRELEASED.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Added\n\n- New.\n")
    unreleased = bd.add_entry(existing, "v0.1.0", SHA).split("## [0.0.1]")[0]
    assert unreleased.index("### Added") < unreleased.index("### Changed")
    assert unreleased.endswith("crates.io.\n\n")


def test_changelog_without_unreleased_heading_fails(tmp_path):
    with pytest.raises(bd.BumpError, match="Unreleased"):
        bd.add_entry("# Changelog\n", "v0.1.0", SHA)


def test_rewritten_repo_still_passes_the_pin_check(tmp_path):
    repo = build(tmp_path, ref='branch = "main"', version="0.0.1")
    bd.rewrite(repo, "v0.0.1")
    assert check(repo)[0] == []


# ---- locked-sha ----------------------------------------------------------


def test_locked_sha_reads_the_one_commit(tmp_path):
    assert bd.locked_sha(build(tmp_path)) == SHA


def test_locked_sha_refuses_two_commits(tmp_path):
    repo = build(tmp_path)
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace(SHA, "0" * 40, 1))
    with pytest.raises(bd.BumpError, match="2 commits"):
        bd.locked_sha(repo)


# ---- pinned --------------------------------------------------------------


def test_pinned_names_the_shared_tag(tmp_path):
    assert bd.pinned_tag(build(tmp_path)) == "v0.0.1"


def test_pinned_is_none_off_a_tag(tmp_path):
    assert bd.pinned_tag(build(tmp_path, ref='branch = "main"')) is None


# ---- latest / ready ------------------------------------------------------


def release(tag: str, **extra) -> tuple[int, bytes]:
    return 200, json.dumps({"tag_name": tag, "draft": False, "prerelease": False, **extra}).encode()


def test_latest_names_the_release_tag(monkeypatch):
    stub_fetch(monkeypatch, {bd.RELEASES_API: release("v0.1.0")})
    assert bd.latest() == "v0.1.0"


@pytest.mark.parametrize("response", [
    release("v0.1.0", prerelease=True),
    release("nightly"),
    (404, b"{}"),
])
def test_latest_refuses_anything_but_a_published_release(monkeypatch, response):
    stub_fetch(monkeypatch, {bd.RELEASES_API: response})
    with pytest.raises(bd.BumpError):
        bd.latest()


def crate_url(name: str, version: str = "0.1.0") -> str:
    return f"{bd.CRATES_API}/{name}/{version}"


def test_ready_when_crates_io_serves_every_versioned_alias(tmp_path, monkeypatch):
    # decdn-e2e has no version and is never published; it is not asked for.
    stub_fetch(monkeypatch, {crate_url("decdn-client"): (200, b""),
                             crate_url("decdn-incentive"): (200, b"")})
    assert bd.ready(build(tmp_path), "v0.1.0") == []


def test_not_ready_while_crates_io_has_no_such_version(tmp_path, monkeypatch):
    stub_fetch(monkeypatch, {crate_url("decdn-client"): (404, b""),
                             crate_url("decdn-incentive"): (200, b"")})
    assert bd.ready(build(tmp_path), "v0.1.0") == ["decdn-client 0.1.0"]


@pytest.mark.parametrize("response", [OSError("timed out"), (503, b""), (429, b"")])
def test_ready_fails_when_crates_io_cannot_answer(tmp_path, monkeypatch, response):
    # Index lag is a 404 and worth waiting out; anything else is not lag, and
    # "run it again later" would hide it.
    stub_fetch(monkeypatch, {crate_url("decdn-client"): response,
                             crate_url("decdn-incentive"): (200, b"")})
    with pytest.raises(bd.BumpError, match="decdn-client"):
        bd.ready(build(tmp_path), "v0.1.0")


# ---- compare -------------------------------------------------------------


@pytest.mark.parametrize(("pinned", "tag", "relation"), [
    ("v0.1.0", "v0.1.0", "same"),
    ("v0.1.0", "v0.1.1", "behind"),
    ("v0.1.1", "v0.1.0", "ahead"),
    ("v0.9.0", "v0.10.0", "behind"),
    ("v0.1.0-rc.1", "v0.1.0", "behind"),
    ("v0.1.0", "v0.1.0-rc.1", "ahead"),
    ("v0.1.0-rc.2", "v0.1.0-rc.10", "behind"),
    ("v0.1.0-alpha", "v0.1.0-alpha.1", "behind"),
    ("v0.1.0-rc.1", "v0.1.0-alpha", "ahead"),
])
def test_compare_orders_by_semver(tmp_path, pinned, tag, relation):
    repo = build(tmp_path, ref=f'tag = "{pinned}"', version=pinned.removeprefix("v"))
    assert bd.compare(repo, tag) == relation


def test_compare_off_a_tag_is_untagged(tmp_path):
    assert bd.compare(build(tmp_path, ref='branch = "main"'), "v0.1.0") == "untagged"


# ---- command line --------------------------------------------------------
# open-decdn-bump.sh branches on these exit codes and reads this output.


@pytest.fixture
def cli_repo(tmp_path, monkeypatch):
    repo = build(tmp_path)
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    monkeypatch.chdir(repo)
    return repo


def test_main_ready_exits_0_when_served(cli_repo, monkeypatch, capsys):
    stub_fetch(monkeypatch, {crate_url("decdn-client"): (200, b""),
                             crate_url("decdn-incentive"): (200, b"")})
    assert bd.main(["ready", "v0.1.0"]) == 0


def test_main_ready_exits_3_while_not_served(cli_repo, monkeypatch, capsys):
    stub_fetch(monkeypatch, {crate_url("decdn-client"): (404, b""),
                             crate_url("decdn-incentive"): (200, b"")})
    assert bd.main(["ready", "v0.1.0"]) == bd.NOT_READY == 3


def test_main_ready_exits_1_when_crates_io_cannot_answer(cli_repo, monkeypatch, capsys):
    stub_fetch(monkeypatch, {crate_url("decdn-client"): OSError("no route"),
                             crate_url("decdn-incentive"): (200, b"")})
    assert bd.main(["ready", "v0.1.0"]) == 1
    assert "no route" in capsys.readouterr().err


@pytest.mark.parametrize("argv", [["ready", "main"], ["rewrite", "1.0.0"], ["compare", "x"]])
def test_main_exits_1_on_a_bad_tag(cli_repo, capsys, argv):
    assert bd.main(argv) == 1
    assert "not a decdn release tag" in capsys.readouterr().err


@pytest.mark.parametrize("argv", [[], ["bogus"], ["ready"], ["latest", "v0.1.0"]])
def test_main_exits_2_on_usage(cli_repo, capsys, argv):
    assert bd.main(argv) == 2


def test_main_latest_prints_the_tag_alone(cli_repo, monkeypatch, capsys):
    stub_fetch(monkeypatch, {bd.RELEASES_API: release("v0.1.0")})
    assert bd.main(["latest"]) == 0
    assert capsys.readouterr().out == "v0.1.0\n"


@pytest.mark.parametrize(("tag", "out"), [("v0.0.1", "same"), ("v0.1.0", "behind")])
def test_main_compare_prints_the_relation(cli_repo, capsys, tag, out):
    assert bd.main(["compare", tag]) == 0
    assert capsys.readouterr().out == f"{out}\n"
