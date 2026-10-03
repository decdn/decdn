"""Regression tests for the release plan.

The plan decides what a tag builds, signs and publishes. A wrong entry ships a
release missing its archives or image, or one the onramp's installers cannot
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
    ("tag", "crate", "version"),
    [
        ("sponsord-core-v0.1.0", "sponsord-core", "0.1.0"),
        ("sponsord-v1.2.3", "sponsord", "1.2.3"),
        ("sponsord-onramp-v0.1.0-rc.1", "sponsord-onramp", "0.1.0-rc.1"),
        ("decdn-sponsored-v10.20.30", "decdn-sponsored", "10.20.30"),
    ],
)
def test_tags_name_their_crate_and_version(tag, crate, version):
    assert rp.parse_tag(tag) == (crate, version)


@pytest.mark.parametrize(
    "tag",
    [
        "v0.1.0",  # the old shared-version tag
        "sponsord-v1",
        "sponsord-v1.0",
        "sponsord-1.0.0",
        "foo-v1.0.0",
        "sponsord-onramp-v",
        "sponsord-v0.1.0-",
        "sponsord-v0$(id)",
        "sponsord-v0.1.0 ",
        "refs/tags/sponsord-v0.1.0",
    ],
)
def test_anything_else_is_refused(tag):
    with pytest.raises(ValueError):
        rp.parse_tag(tag)


def test_onramp_tag_is_not_read_as_the_daemon():
    assert rp.plan("sponsord-onramp-v1.0.0")["crate"] == "sponsord-onramp"


def test_archive_counts_and_images():
    expect = {
        "sponsord-api": ("0", ""),
        "sponsord-core": ("0", ""),
        "sponsord": ("2", "sponsord"),
        "sponsord-onramp": ("2", "sponsord-onramp"),
        "decdn-sponsored": ("6", ""),
    }
    for crate, (archives, image) in expect.items():
        p = rp.plan(f"{crate}-v0.1.0")
        assert (p["archives"], p["image"]) == (archives, image), crate
        legs = json.loads(p["matrix"])["include"]
        assert len(legs) == int(archives)
        assert all(leg["package"] == crate for leg in legs)


def test_installer_contract_targets_for_the_wrapper():
    """decdn.sh / decdn.ps1 fetch these six; dropping one breaks an installer."""
    legs = json.loads(rp.plan("decdn-sponsored-v0.1.0")["matrix"])["include"]
    assert {leg["target"] for leg in legs} == {
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
        "x86_64-pc-windows-msvc",
        "aarch64-pc-windows-msvc",
    }
    assert {leg["binary"] for leg in legs} == {"decdn-sponsored"}


def test_only_the_wrapper_carries_the_onramp_pin():
    pins = {c for c in rp.CRATES if rp.plan(f"{c}-v0.1.0")["onramp_pin"] == "true"}
    assert pins == {"decdn-sponsored"}


def test_tag_pattern_matches_every_crate_and_nothing_else():
    pattern = re.compile(rp.tag_pattern())
    for crate in rp.CRATES:
        assert pattern.match(f"{crate}-v0.1.0"), crate
    assert not pattern.match("v0.1.0")
    assert not pattern.match("other-v0.1.0")


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
    ok = run("sponsord-v1.0.0")
    assert ok.returncode == 0
    assert "crate=sponsord\n" in ok.stdout and "archives=2\n" in ok.stdout
    assert run("sponsord-v1.0.0", "--get", "image").stdout == "sponsord\n"
    assert run("v1.0.0").returncode == 1
    assert run("sponsord-v1.0.0", "--get", "nope").returncode == 2


@pytest.mark.parametrize(
    ("tag", "prerelease", "latest"),
    [
        ("decdn-sponsored-v1.0.0", "false", "true"),
        ("decdn-sponsored-v1.0.0-rc.1", "true", "false"),
        ("sponsord-v1.0.0", "false", "false"),
        ("sponsord-v1.0.0-rc.1", "true", "false"),
        ("sponsord-core-v0.2.0-alpha", "true", "false"),
    ],
)
def test_prerelease_is_flagged_and_never_latest(tag, prerelease, latest):
    p = rp.plan(tag)
    assert (p["prerelease"], p["latest"]) == (prerelease, latest)
