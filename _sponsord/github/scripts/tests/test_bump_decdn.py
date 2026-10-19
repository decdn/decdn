"""Regression tests for the decdn bump script.

open-decdn-bump.sh opens its PR from what this script writes. A rewrite
that misses an entry, or a changelog entry in the wrong crate, reaches review
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


CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
PATCHED = ("decdn-client", "decdn-incentive", "decdn-protocol")


def build(tmp_path: Path, *, version: str = "0.0.1", req: str | None = None,
          ref: str | None = None) -> Path:
    """A workspace with a published lib and CLI on decdn, a dev-only user and an e2e crate.

    The decdn crates come from crates.io at <req> (the caret range of
    <version>) and are locked at <version>; decdn-e2e from git at <ref>
    (`tag = "v<version>"`).
    """
    req = req or bd.caret(version)
    ref = ref or f'tag = "v{version}"'
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
        "# decdn, from crates.io.\n"
        f'decdn-client = "{req}"\n'
        f'decdn-incentive = {{ version = "{req}", features = ["redb"] }}\n'
        f'decdn-e2e = {{ git = "{GIT}", {ref} }}\n'
        'serde = "1"\n\n'
        f'[patch."{GIT}"]\n'
        + "".join(f'{name} = "{req}"\n' for name in PATCHED)
        + '\n[profile.release]\nlto = "fat"\n'
    )
    lock = ["version = 4"]
    for name in PATCHED:
        lock += ["", "[[package]]", f'name = "{name}"', f'version = "{version}"',
                 f'source = "{CRATES_IO}"']
    for name in ("decdn-e2e", "decdn-node"):
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


def requirements(repo: Path) -> set[str]:
    root = bd.manifest(repo)
    registry, _, _ = bd.decdn_deps(root)
    return set(registry.values()) | {bd.requirement(s) for s in bd.decdn_patch(root).values()}


def test_rewrite_within_the_caret_range_moves_only_the_e2e_tag(tmp_path):
    repo = build(tmp_path, version="0.1.0")
    before = (repo / "Cargo.toml").read_text()
    assert bd.rewrite(repo, "v0.1.3") == ["decdn-e2e"]
    after = (repo / "Cargo.toml").read_text()
    assert after == before.replace('tag = "v0.1.0"', 'tag = "v0.1.3"')


@pytest.mark.parametrize(("version", "tag", "req"), [
    ("0.1.0", "v0.2.0", "0.2"),
    ("0.9.1", "v1.0.0", "1"),
    ("0.1.0", "v0.2.0-rc.1", "0.2.0-rc.1"),
    ("0.2.0-rc.1", "v0.2.0", "0.2"),
])
def test_rewrite_across_the_caret_range_moves_every_requirement(tmp_path, version, tag, req):
    repo = build(tmp_path, version=version)
    bd.rewrite(repo, tag)
    assert requirements(repo) == {req}
    text = (repo / "Cargo.toml").read_text()
    assert f'decdn-e2e = {{ git = "{GIT}", tag = "{tag}" }}' in text
    assert 'features = ["redb"]' in text
    assert "# decdn, from crates.io.\n" in text
    assert 'serde = "1"\n' in text and 'lto = "fat"' in text


@pytest.mark.parametrize("ref", ['branch = "main"', 'rev = "abc1234"'])
def test_rewrite_puts_e2e_back_on_a_tag(tmp_path, ref):
    repo = build(tmp_path, ref=ref)
    bd.rewrite(repo, "v0.1.0")
    assert f'decdn-e2e = {{ git = "{GIT}", tag = "v0.1.0" }}' in (repo / "Cargo.toml").read_text()


def test_rewrite_refuses_a_non_release_tag(tmp_path):
    repo = build(tmp_path)
    before = (repo / "Cargo.toml").read_text()
    with pytest.raises(bd.BumpError, match="not a decdn release tag"):
        bd.rewrite(repo, "main")
    assert (repo / "Cargo.toml").read_text() == before


def test_rewrite_fails_on_an_entry_it_cannot_see(tmp_path):
    repo = build(tmp_path)
    manifest = repo / "Cargo.toml"
    # A multi-line alias parses fine but escapes the line match.
    manifest.write_text(manifest.read_text().replace(
        f'decdn-e2e = {{ git = "{GIT}", tag = "v0.0.1" }}\nserde = "1"\n',
        f'serde = "1"\n\n[workspace.dependencies.decdn-e2e]\ngit = "{GIT}"\ntag = "v0.0.1"\n',
    ))
    with pytest.raises(bd.BumpError, match="decdn-e2e not rewritten"):
        bd.rewrite(repo, "v0.1.0")


def test_rewrite_leaves_other_tables_alone(tmp_path):
    repo = build(tmp_path)
    manifest = repo / "Cargo.toml"
    manifest.write_text(manifest.read_text() + '\n[patch.other]\ndecdn-client = "9"\n')
    bd.rewrite(repo, "v0.2.0")
    assert '[patch.other]\ndecdn-client = "9"\n' in manifest.read_text()


def test_rewrite_fails_without_any_crates_io_decdn_dependency(tmp_path):
    repo = build(tmp_path)
    (repo / "Cargo.toml").write_text('[workspace]\nmembers = []\n[workspace.dependencies]\n')
    with pytest.raises(bd.BumpError, match="no decdn crate from crates.io"):
        bd.rewrite(repo, "v0.1.0")


def test_rewrite_exact_pins_every_requirement_to_the_version(tmp_path):
    repo = build(tmp_path, version="0.1.0")
    bd.rewrite(repo, "v0.1.3", exact=True)
    assert requirements(repo) == {"=0.1.3"}
    bd.rewrite(repo, "v0.1.3")
    assert requirements(repo) == {"0.1"}


def move_lock(repo: Path, old: str, new: str, sha: str) -> None:
    """What `cargo update` does to the fixture lock: every decdn crate to <new>."""
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace(f'version = "{old}"', f'version = "{new}"')
                    .replace(f"tag=v{old}#{SHA}", f"tag=v{new}#{sha}"))


def test_a_bump_across_the_caret_range_ends_where_the_checker_agrees(tmp_path):
    # The sequence open-decdn-bump.sh runs, with cargo's part simulated.
    repo = build(tmp_path, version="0.1.0")
    assert check(repo)[0] == []
    new_sha = "1" * 40
    bd.rewrite(repo, "v0.2.0", exact=True)
    move_lock(repo, "0.1.0", "0.2.0", new_sha)
    bd.rewrite(repo, "v0.2.0")
    assert check(repo)[0] == []
    assert bd.pinned_tag(repo) == "v0.2.0" and bd.locked_sha(repo) == new_sha
    bd.changelog(repo, "v0.2.0")
    assert "require `0.2`" in (repo / "crates/lib/CHANGELOG.md").read_text()


# ---- canary --------------------------------------------------------------


def test_canary_takes_every_decdn_crate_from_main(tmp_path):
    repo = build(tmp_path, version="0.1.0")
    assert bd.canary(repo, "0.2.0") == sorted(PATCHED)
    root = bd.manifest(repo)
    registry, git, _ = bd.decdn_deps(root)
    assert set(registry.values()) == {"0.2"}
    assert git == {"decdn-e2e": {"git": GIT, "branch": "main"}}
    assert bd.decdn_patch(root) == {}
    assert root["patch"]["crates-io"] == {
        name: {"git": GIT, "branch": "main"} for name in PATCHED
    }
    assert root["profile"]["release"] == {"lto": "fat"}


def test_canary_moves_a_version_on_the_git_alias(tmp_path):
    repo = build(tmp_path, ref='tag = "v0.0.1", version = "0.0.1"')
    bd.canary(repo, "0.2.0")
    _, git, _ = bd.decdn_deps(bd.manifest(repo))
    assert git == {"decdn-e2e": {"git": GIT, "branch": "main", "version": "0.2.0"}}


def main_lock(repo: Path, extra: str = "") -> None:
    """The lock `cargo update` writes after `canary`: every decdn crate from main."""
    lock = ["version = 4"]
    for name in (*PATCHED, "decdn-e2e"):
        lock += ["", "[[package]]", f'name = "{name}"', 'version = "0.2.0"',
                 f'source = "git+{GIT}?branch=main#{SHA}"']
    (repo / "Cargo.lock").write_text("\n".join(lock) + "\n" + extra)


def test_canary_check_passes_when_everything_is_on_main(tmp_path):
    repo = build(tmp_path)
    main_lock(repo)
    assert bd.canary_check(repo) == []


@pytest.mark.parametrize(("extra", "problem"), [
    (f'\n[[package]]\nname = "decdn-protocol"\nversion = "0.1.0"\nsource = "{CRATES_IO}"\n',
     "decdn-protocol 0.1.0 from crates.io"),
    ('\n[[patch.unused]]\nname = "decdn-protocol"\nversion = "0.3.0"\n',
     "[patch] decdn-protocol 0.3.0 unused"),
    (f'\n[[package]]\nname = "decdn-e2e"\nversion = "0.1.0"\nsource = "git+{GIT}?tag=v0.1.0#{SHA}"\n',
     "decdn-e2e from git+"),
])
def test_canary_check_names_what_stayed_off_main(tmp_path, extra, problem):
    repo = build(tmp_path)
    main_lock(repo, extra)
    assert any(problem in p for p in bd.canary_check(repo)), bd.canary_check(repo)


def test_canary_check_on_the_release_lock_fails(tmp_path):
    problems = bd.canary_check(build(tmp_path))
    assert any("from crates.io" in p for p in problems), problems


def test_canary_refuses_a_second_crates_io_patch(tmp_path):
    repo = build(tmp_path)
    manifest = repo / "Cargo.toml"
    manifest.write_text(manifest.read_text() + '\n[patch.crates-io]\nfoo = { path = "x" }\n')
    with pytest.raises(bd.BumpError, match="already has"):
        bd.canary(repo, "0.2.0")


def test_canary_refuses_a_non_version(tmp_path):
    with pytest.raises(bd.BumpError, match="not a version"):
        bd.canary(build(tmp_path), "main")


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
        "- **decdn v0.0.1.** The decdn crates are locked at `0.0.1` from crates.io and\n"
        "  require `0.0.1`, and the anvil fixtures are at decdn's `v0.0.1` release tag\n"
        f"  (`{SHA}`).\n\n## [0.0.1] - 2026-10-09\n"
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
    # One list: no blank line between the new bullet and the old one.
    assert f"(`{SHA}`).\n- Earlier change.\n" in unreleased


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
    assert unreleased.endswith(f"(`{SHA}`).\n\n")


def test_changelog_without_unreleased_heading_fails(tmp_path):
    with pytest.raises(bd.BumpError, match="Unreleased"):
        bd.add_entry("# Changelog\n", "v0.1.0", SHA)


def test_rewritten_repo_still_passes_the_pin_check(tmp_path):
    repo = build(tmp_path, ref='branch = "main"', version="0.0.1")
    assert check(repo)[0] != []
    bd.rewrite(repo, "v0.0.1")
    assert check(repo)[0] == []


# ---- locked-sha ----------------------------------------------------------


def test_locked_sha_reads_the_one_commit(tmp_path):
    assert bd.locked_sha(build(tmp_path)) == SHA


def test_locked_crates_lists_both_sources(tmp_path):
    assert bd.locked_crates(build(tmp_path)) == sorted([*PATCHED, "decdn-e2e", "decdn-node"])


def test_locked_sha_refuses_two_commits(tmp_path):
    repo = build(tmp_path)
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace(SHA, "0" * 40, 1))
    with pytest.raises(bd.BumpError, match="2 commits"):
        bd.locked_sha(repo)


# ---- pinned --------------------------------------------------------------


def test_pinned_names_the_locked_crates_io_version(tmp_path):
    # Not the e2e tag: the crates are what ships.
    assert bd.pinned_tag(build(tmp_path, ref='branch = "main"')) == "v0.0.1"


def test_pinned_is_none_for_mixed_versions(tmp_path):
    repo = build(tmp_path)
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace('version = "0.0.1"', 'version = "0.0.2"', 1))
    assert bd.pinned_tag(repo) is None


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


def crate_url(name: str) -> str:
    return bd.index_url(name)


def index_file(*versions: str) -> tuple[int, bytes]:
    """A sparse-index answer listing <versions>, one JSON line each."""
    lines = [json.dumps({"name": "x", "vers": v, "deps": [], "yanked": False}) for v in versions]
    return 200, "\n".join(lines).encode() + b"\n"


def served(**overrides) -> dict[str, object]:
    """The index listing 0.1.0 for every crate the workspace takes from crates.io."""
    return {
        crate_url(n): overrides.get(n.replace("-", "_"), index_file("0.0.1", "0.1.0"))
        for n in PATCHED
    }


def test_index_url_follows_the_crates_io_layout():
    assert bd.index_url("decdn-client") == "https://index.crates.io/de/cd/decdn-client"
    assert bd.index_url("abc") == "https://index.crates.io/3/a/abc"
    assert bd.index_url("ab") == "https://index.crates.io/2/ab"


def test_ready_when_the_index_lists_every_crate(tmp_path, monkeypatch):
    # decdn-e2e is never published; it is not asked for. decdn-protocol is
    # only in the [patch], and is.
    stub_fetch(monkeypatch, served())
    assert bd.ready(build(tmp_path), "v0.1.0") == []


@pytest.mark.parametrize("response", [index_file("0.0.1"), (404, b"")])
def test_not_ready_while_the_index_lacks_the_version(tmp_path, monkeypatch, response):
    # The crates.io API may already serve it; cargo reads the index.
    stub_fetch(monkeypatch, served(decdn_protocol=response))
    assert bd.ready(build(tmp_path), "v0.1.0") == ["decdn-protocol 0.1.0"]


def test_ready_fails_on_an_answer_that_is_not_an_index_file(tmp_path, monkeypatch):
    stub_fetch(monkeypatch, served(decdn_client=(200, b"<html>")))
    with pytest.raises(bd.BumpError, match="not an index file"):
        bd.ready(build(tmp_path), "v0.1.0")


@pytest.mark.parametrize("response", [OSError("timed out"), (503, b""), (429, b"")])
def test_ready_fails_when_the_index_cannot_answer(tmp_path, monkeypatch, response):
    # Index lag is a 404 and worth waiting out; anything else is not lag, and
    # "run it again later" would hide it.
    stub_fetch(monkeypatch, served(decdn_client=response))
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
    repo = build(tmp_path, version=pinned.removeprefix("v"))
    assert bd.compare(repo, tag) == relation


def test_compare_refuses_a_locked_version_it_cannot_read(tmp_path):
    # Not "untagged": that would let any tag, even an older one, move the pin.
    repo = build(tmp_path)
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace('version = "0.0.1"', 'version = "0.0.1+build"'))
    with pytest.raises(bd.BumpError):
        bd.compare(repo, "v0.1.0")


def test_compare_with_mixed_versions_is_untagged(tmp_path):
    repo = build(tmp_path)
    lock = repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace('version = "0.0.1"', 'version = "0.0.2"', 1))
    assert bd.compare(repo, "v0.1.0") == "untagged"


# ---- command line --------------------------------------------------------
# open-decdn-bump.sh branches on these exit codes and reads this output.


@pytest.fixture
def cli_repo(tmp_path, monkeypatch):
    repo = build(tmp_path)
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    monkeypatch.chdir(repo)
    return repo


def test_main_ready_exits_0_when_served(cli_repo, monkeypatch, capsys):
    stub_fetch(monkeypatch, served())
    assert bd.main(["ready", "v0.1.0"]) == 0


def test_main_ready_exits_3_while_not_served(cli_repo, monkeypatch, capsys):
    stub_fetch(monkeypatch, served(decdn_client=index_file("0.0.1")))
    assert bd.main(["ready", "v0.1.0"]) == bd.NOT_READY == 3


def test_main_ready_exits_1_when_crates_io_cannot_answer(cli_repo, monkeypatch, capsys):
    stub_fetch(monkeypatch, served(decdn_client=OSError("no route")))
    assert bd.main(["ready", "v0.1.0"]) == 1
    assert "no route" in capsys.readouterr().err


@pytest.mark.parametrize("argv", [["ready", "main"], ["rewrite", "1.0.0"], ["compare", "x"]])
def test_main_exits_1_on_a_bad_tag(cli_repo, capsys, argv):
    assert bd.main(argv) == 1
    assert "not a decdn release tag" in capsys.readouterr().err


@pytest.mark.parametrize("argv", [[], ["bogus"], ["ready"], ["latest", "v0.1.0"], ["canary"]])
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


def test_main_locked_crates_prints_one_per_line(cli_repo, capsys):
    assert bd.main(["locked-crates"]) == 0
    assert capsys.readouterr().out.split() == bd.locked_crates(cli_repo)


def test_main_pinned_prints_an_empty_line_for_mixed_versions(cli_repo, capsys):
    # open-decdn-bump.sh reads an empty `pinned` as "untagged"; it dies on a
    # non-zero exit.
    lock = cli_repo / "Cargo.lock"
    lock.write_text(lock.read_text().replace('version = "0.0.1"', 'version = "0.0.2"', 1))
    assert bd.main(["pinned"]) == 0
    assert capsys.readouterr().out == "\n"


@pytest.mark.parametrize(("argv", "req"), [(["rewrite", "v0.2.0"], "0.2"),
                                          (["rewrite", "v0.2.0", "--exact"], "=0.2.0")])
def test_main_rewrite(cli_repo, capsys, argv, req):
    assert bd.main(argv) == 0
    assert requirements(cli_repo) == {req}


@pytest.mark.parametrize("argv", [["rewrite", "v0.2.0", "--bogus"], ["canary-check", "x"]])
def test_main_rewrite_and_canary_check_usage(cli_repo, capsys, argv):
    assert bd.main(argv) == 2


def test_main_canary_check_exits_1_off_main(cli_repo, capsys):
    assert bd.main(["canary-check"]) == 1
    assert "not on decdn main" in capsys.readouterr().err
