"""The cargo-release configuration against the files it rewrites.

`cargo release` applies its `pre-release-replacements` to the release commit,
and only the tag's CI run notices a replacement that matched the wrong line or
none: by then the signed tag exists. These checks hold the configuration and
the files together at every PR instead.
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ROOT = tomllib.loads((REPO_ROOT / "Cargo.toml").read_text())
RELEASE = tomllib.loads((REPO_ROOT / "release.toml").read_text())
VERSION = ROOT["workspace"]["package"]["version"]


def members() -> dict[str, tuple[Path, dict]]:
    out = {}
    for rel in ROOT["workspace"]["members"]:
        manifest = tomllib.loads((REPO_ROOT / rel / "Cargo.toml").read_text())
        out[manifest["package"]["name"]] = (REPO_ROOT / rel, manifest)
    return out


def release_meta(manifest: dict) -> dict:
    return manifest.get("package", {}).get("metadata", {}).get("release", {})


def released() -> dict[str, tuple[Path, dict]]:
    return {n: m for n, m in members().items() if release_meta(m[1]).get("release", True)}


def replacements(manifest: dict) -> list[dict]:
    """A package's own list replaces the root one; it does not add to it."""
    return release_meta(manifest).get("pre-release-replacements", RELEASE["pre-release-replacements"])


def test_unreleased_members_are_unpublished_and_vice_versa():
    """`release = false` with `publish` left on would ship a crate nobody tags;
    a private crate left in the release would trip the changelog replacement."""
    for name, (_, manifest) in members().items():
        private = manifest["package"].get("publish") is False
        assert private == (not release_meta(manifest).get("release", True)), name


def test_every_released_member_has_a_changelog():
    for name, (path, _) in released().items():
        assert (path / "CHANGELOG.md").is_file(), name


def test_package_lists_keep_every_root_replacement():
    for name, (_, manifest) in released().items():
        own = replacements(manifest)
        for entry in RELEASE["pre-release-replacements"]:
            assert entry in own, (name, entry)


def test_every_replacement_matches_exactly_once():
    """`exactly = 1` aborts the release otherwise, after nothing is pushed but
    only once someone runs it."""
    for name, (path, manifest) in released().items():
        for entry in replacements(manifest):
            text = (path / entry["file"]).read_text()
            hits = len(re.findall(entry["search"], text, flags=re.M))
            assert hits == entry.get("exactly", hits), (name, entry["file"], hits)


def test_openapi_replacements_hit_info_version():
    """The one match must be the document's own version, at the workspace's."""
    for name, (path, manifest) in released().items():
        for entry in replacements(manifest):
            if not entry["file"].endswith(".json"):
                continue
            target = (path / entry["file"]).resolve()
            doc = json.loads(target.read_text())
            assert doc["info"]["version"] == VERSION, target
            line = next(
                l for l in target.read_text().splitlines() if re.search(entry["search"], l)
            )
            assert line.strip().rstrip(",") == f'"version": "{VERSION}"', target
