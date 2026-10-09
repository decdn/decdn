"""Regression tests for the release plan.

The plan decides what a tag builds, signs and publishes. A wrong entry ships a
release missing archives or an image, or one the onramp's installers cannot
fetch — after the signed tag is already public.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/release_plan.py"

_spec = importlib.util.spec_from_file_location("release_plan", MODULE_PATH)
assert _spec and _spec.loader
rp = importlib.util.module_from_spec(_spec)
sys.modules["release_plan"] = rp
_spec.loader.exec_module(rp)


@pytest.mark.parametrize(
    ("tag", "version"),
    [
        ("v0.1.0", "0.1.0"),
        ("v1.2.3", "1.2.3"),
        ("v0.1.0-rc.1", "0.1.0-rc.1"),
        ("v10.20.30", "10.20.30"),
    ],
)
def test_tags_name_their_version(tag, version):
    assert rp.parse_tag(tag) == version


@pytest.mark.parametrize(
    "tag",
    [
        "sponsord-v0.1.0",  # the old per-crate tags
        "decdn-sponsored-v0.1.0",
        "v1",
        "v1.0",
        "1.0.0",
        "V1.0.0",
        "v",
        "v0.1.0-",
        "v0$(id)",
        "v0.1.0 ",
        "refs/tags/v0.1.0",
    ],
)
def test_anything_else_is_refused(tag):
    with pytest.raises(ValueError):
        rp.parse_tag(tag)


def test_every_release_carries_every_archive_and_image():
    p = rp.plan("v0.1.0")
    legs = json.loads(p["matrix"])["include"]
    assert p["archives"] == str(len(legs)) == "10"
    per_binary = {}
    for leg in legs:
        per_binary.setdefault(leg["binary"], set()).add(leg["target"])
        assert leg["package"] == leg["binary"]
    assert {b: len(t) for b, t in per_binary.items()} == {
        "sponsord": 2,
        "sponsord-onramp": 2,
        "decdn-sponsored": 6,
    }
    assert p["images"] == "sponsord sponsord-onramp"
    assert json.loads(p["images_json"]) == ["sponsord", "sponsord-onramp"]


def test_installer_contract_targets_for_the_wrapper():
    """decdn.sh / decdn.ps1 fetch these six; dropping one breaks an installer."""
    legs = json.loads(rp.plan("v0.1.0")["matrix"])["include"]
    assert {leg["target"] for leg in legs if leg["binary"] == "decdn-sponsored"} == {
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
        "x86_64-pc-windows-msvc",
        "aarch64-pc-windows-msvc",
    }


def test_archive_names_are_unique():
    """One SHA256SUMS covers every archive, so no two legs may share a name."""
    legs = json.loads(rp.plan("v0.1.0")["matrix"])["include"]
    names = [(leg["binary"], leg["target"]) for leg in legs]
    assert len(names) == len(set(names))


TAG_SAMPLES = [
    "v0.1.0",
    "v10.20.30",
    "v1.0.0-rc.1",
    "v1.0.0-beta-2",
    "sponsord-v0.1.0",
    "other-v0.1.0",
    "v0-wip",
    "v0.2.9.1",
    "v1.0",
    "v1.0.0-",
    "v1.0.0 ",
]


def test_tag_pattern_accepts_exactly_what_parse_tag_does():
    """A looser pattern would let a stray tag bound `git cliff --latest`."""
    pattern = re.compile(rp.tag_pattern())
    for tag in TAG_SAMPLES:
        try:
            rp.parse_tag(tag)
            valid = True
        except ValueError:
            valid = False
        assert bool(pattern.search(tag)) == valid, tag


def test_tag_pattern_reads_the_same_as_an_ere():
    """The same text is a bash ERE too; `(?:` would not be."""
    tags = "\n".join(TAG_SAMPLES) + "\n"
    out = subprocess.run(
        ["grep", "-E", rp.tag_pattern()], input=tags, capture_output=True, text=True
    ).stdout.splitlines()
    assert out == [t for t in TAG_SAMPLES if rp.TAG.match(t)]


def test_git_cliff_bounds_notes_at_the_same_tags():
    """cliff.toml's tag_pattern decides where `git cliff --latest` stops."""
    cliff = tomllib.loads((REPO_ROOT / "cliff.toml").read_text())
    assert cliff["git"]["tag_pattern"] == rp.tag_pattern()


def test_table_matches_the_workspace():
    """Every released member has an entry, at the directory and with the binary
    it has. A `publish = false` member (the e2e tests) is never released."""
    root = tomllib.loads((REPO_ROOT / "Cargo.toml").read_text())
    members = {}
    for rel in root["workspace"]["members"]:
        manifest = tomllib.loads((REPO_ROOT / rel / "Cargo.toml").read_text())
        if manifest["package"].get("publish") is False:
            continue
        bins = {b["name"] for b in manifest.get("bin", [])}
        if (REPO_ROOT / rel / "src/main.rs").is_file():
            bins.add(manifest["package"]["name"])
        members[manifest["package"]["name"]] = (rel, bins)
    assert set(rp.CRATES) == set(members)
    for crate, spec in rp.CRATES.items():
        rel, bins = members[crate]
        assert spec["dir"] == rel, crate
        if spec["binary"] is None:
            assert not spec["targets"], crate
        else:
            assert spec["binary"] in bins, crate


def test_images_have_dockerfile_targets():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text()
    for image in rp.images():
        assert re.search(rf"^FROM \S+ AS {re.escape(image)}$", dockerfile, re.M), image


def test_cli_output_and_errors():
    run = lambda *a: subprocess.run(
        [sys.executable, str(MODULE_PATH), *a], capture_output=True, text=True
    )
    ok = run("v1.0.0")
    assert ok.returncode == 0
    assert "version=1.0.0\n" in ok.stdout and "archives=10\n" in ok.stdout
    assert run("v1.0.0", "--get", "images").stdout == "sponsord sponsord-onramp\n"
    assert run("sponsord-v1.0.0").returncode == 1
    assert run("v1.0.0", "--get", "nope").returncode == 2


@pytest.mark.parametrize(
    ("tag", "prerelease"),
    [("v1.0.0", "false"), ("v1.0.0-rc.1", "true"), ("v0.2.0-alpha", "true")],
)
def test_prerelease_is_flagged(tag, prerelease):
    assert rp.plan(tag)["prerelease"] == prerelease


@pytest.mark.parametrize(
    ("tag", "latest"),
    [
        # The newest stable release, or a re-run of it.
        ("v1.2.0", True),
        ("v1.1.0", False),  # a re-run after 1.2.0 exists
        # A maintenance release under a newer one never takes it.
        ("v1.0.1", False),
        ("v1.1.1", False),
        ("v1.2.1", True),
        # A prerelease never does, even above every stable release.
        ("v1.3.0-rc.1", False),
        # A candidate above the newest release does not hold it back.
        ("v1.2.2", True),
    ],
)
def test_latest_is_the_highest_stable_release(tag, latest):
    tags = ["v1.0.0", "v1.1.0", "v1.2.0", "v1.3.0-rc.1", "v9.9.9.9", "sponsord-v9.0.0"]
    assert rp.is_latest(tag, tags) is latest


# The semver-checks baseline. Re-running a release's workflow is supported
# (RELEASING.md), and so is a maintenance release under a newer one, so the
# baseline is the highest release *below* the tag, never merely the
# newest tag: comparing against a newer release runs the check backwards.
RELEASE_TAGS = [
    "v0.1.0",
    "v0.2.0-rc.1",
    "v0.2.0",
    "v0.3.0",
    "v1.0.0",
    "v1.1.0",
    # The old per-crate tags and odd tags are never a baseline.
    "sponsord-v0.9.9",
    "decdn-sponsored-v0.2.5",
    "v0-wip",
    "v0.2.9.1",
]


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("v0.3.0", "v0.2.0"),
        # A rerun of 1.0.0 after 1.1.0 exists still checks against 0.3.0.
        ("v1.0.0", "v0.3.0"),
        # A maintenance release below a newer one.
        ("v1.0.1", "v1.0.0"),
        # A release candidate sorts below its release.
        ("v0.2.0", "v0.2.0-rc.1"),
        ("v0.2.0-rc.1", "v0.1.0"),
        ("v0.2.1", "v0.2.0"),
        # The first release has nothing to compare against.
        ("v0.1.0", None),
        ("v0.0.1", None),
    ],
)
def test_baseline_is_the_highest_release_below_the_tag(tag, expected):
    assert rp.baseline(tag, RELEASE_TAGS) == expected


@pytest.mark.parametrize(
    ("lower", "higher"),
    [
        ("1.0.0-alpha", "1.0.0-alpha.1"),
        ("1.0.0-alpha.1", "1.0.0-alpha.beta"),
        ("1.0.0-alpha.beta", "1.0.0-beta"),
        ("1.0.0-beta.2", "1.0.0-beta.11"),
        ("1.0.0-rc.1", "1.0.0"),
        ("1.9.0", "1.10.0"),
        ("0.9.9", "1.0.0"),
    ],
)
def test_baseline_follows_semver_precedence(lower, higher):
    tags = [f"v{lower}", f"v{higher}"]
    assert rp.baseline(f"v{higher}", tags) == f"v{lower}"
    assert rp.baseline(f"v{lower}", tags) is None


def test_baseline_cli_reads_the_repository_tags(tmp_path):
    # Signing off and an identity set: a developer's global config may sign
    # every commit and tag with a key this scratch repository cannot reach.
    config = ["-c", "commit.gpgSign=false", "-c", "tag.gpgSign=false"]
    config += ["-c", "user.name=t", "-c", "user.email=t@t"]
    git = lambda *a: subprocess.run(
        ["git", "-C", str(tmp_path), *config, *a], check=True, capture_output=True, text=True
    )
    git("init", "-q")
    git("commit", "-q", "--allow-empty", "-m", "c")
    for tag in ("v0.1.0", "v0.2.0", "v0.3.0"):
        git("tag", tag)
    run = lambda *a: subprocess.run(
        [sys.executable, str(MODULE_PATH), *a], capture_output=True, text=True, cwd=tmp_path
    )
    assert run("v0.2.0", "--baseline").stdout == "v0.1.0\n"
    assert run("v0.3.0", "--latest").stdout == "true\n"
    assert run("v0.2.1", "--latest").stdout == "false\n"
    none = run("v0.1.0", "--baseline")
    assert (none.returncode, none.stdout) == (0, "")
    assert run("sponsord-v0.2.0", "--baseline").returncode == 1
