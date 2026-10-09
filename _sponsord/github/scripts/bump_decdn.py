#!/usr/bin/env python3
"""Move the decdn pin to a decdn release, for open-decdn-bump.sh.

The decdn crates are git dependencies on github.com/decdn/decdn at a release
`tag` (RELEASING.md § Pinning decdn). A bump PR changes three things: the
`tag` and `version` on every decdn alias in the root Cargo.toml, the decdn
packages in Cargo.lock (`cargo update`), and a changelog entry in every
published crate the bump reaches. open-decdn-bump.sh runs `cargo update`
and opens the PR; this script does the rest, one subcommand per step:

    latest            decdn's latest published release tag
    pinned            the tag every decdn alias names (empty when they
                      name a branch, a rev or different tags)
    compare <tag>     where the pin stands against <tag>, by SemVer
                      precedence: same, behind, ahead, or untagged when
                      `pinned` is empty
    ready <tag>       exit 0 when crates.io serves every versioned alias at
                      <tag>, 3 while it answers that one is missing (index
                      lag), 1 when it cannot answer
    rewrite <tag>     point every decdn alias at <tag>
    changelog <tag>   add the bump entry under [Unreleased], naming the
                      commit Cargo.lock pins
    locked-sha        the decdn commit Cargo.lock pins

Run: .github/scripts/bump-decdn.sh <subcommand> [args]
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_decdn_pin import (  # noqa: E402
    DECDN_GIT,
    DEP_TABLES,
    EXACT_VERSION,
    LOCK_SOURCE,
    decdn_aliases,
)

RELEASES_API = "https://api.github.com/repos/decdn/decdn/releases/latest"
CRATES_API = "https://crates.io/api/v1/crates"
USER_AGENT = "decdn-sponsord-bump-decdn (https://github.com/decdn/sponsord)"
NOT_READY = 3

# An alias line in [workspace.dependencies], as the decdn main canary in
# ci.yml matches it.
ALIAS_LINE = re.compile(r'^(decdn-[a-z0-9-]+) = \{ git = "' + re.escape(DECDN_GIT) + r'"')
REF_FIELD = re.compile(r'\b(?:tag|rev|branch) = "[^"]*"')
VERSION_FIELD = re.compile(r'\bversion = "[^"]*"')


class BumpError(Exception):
    """A precondition the bump cannot continue past."""


def fetch(url: str) -> tuple[int, bytes]:
    """(HTTP status, body) for a GET. Raises OSError on a transport failure."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    # Optional: only lifts api.github.com's anonymous rate limit.
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token and url.startswith("https://api.github.com/"):
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def version_of(tag: str) -> str:
    """`X.Y.Z[-pre]` from `vX.Y.Z[-pre]`, or BumpError."""
    version = tag.removeprefix("v")
    if not tag.startswith("v") or not EXACT_VERSION.match(version):
        raise BumpError(f"{tag!r} is not a decdn release tag (vX.Y.Z)")
    return version


def manifest(repo_root: Path) -> dict:
    return tomllib.loads((repo_root / "Cargo.toml").read_text())


def pinned_tag(repo_root: Path) -> str | None:
    """The tag every decdn alias names, or None when they name anything else."""
    aliases, _ = decdn_aliases(manifest(repo_root))
    tags = {spec.get("tag") for spec in aliases.values()}
    if len(tags) == 1:
        return next(iter(tags))
    return None


def compare(repo_root: Path, tag: str) -> str:
    """Where the pin stands against <tag>: same, behind, ahead or untagged."""
    target = semver_key(version_of(tag))
    pinned = pinned_tag(repo_root)
    try:
        current = semver_key(version_of(pinned)) if pinned else None
    except BumpError:
        current = None
    if current is None:
        return "untagged"
    if current == target:
        return "same"
    return "behind" if current < target else "ahead"


def semver_key(version: str) -> tuple:
    """A sort key in SemVer precedence: a release ranks above its prereleases."""
    core, _, pre = version.partition("-")
    numbers = tuple(int(n) for n in core.split("."))
    if not pre:
        return (numbers, (1,))
    # Numeric identifiers compare as numbers and rank below alphanumeric ones;
    # a shorter run of equal identifiers ranks lower.
    ids = tuple((0, int(i), "") if i.isdigit() else (1, 0, i) for i in pre.split("."))
    return (numbers, (0, ids))


def latest() -> str:
    """decdn's latest published release tag."""
    status, body = fetch(RELEASES_API)
    if status != 200:
        raise BumpError(f"GET {RELEASES_API}: HTTP {status}")
    release = json.loads(body)
    # /releases/latest never returns a draft or a prerelease; checked anyway,
    # because pinning one would ship a lock that cannot go to crates.io.
    if release.get("draft") or release.get("prerelease"):
        raise BumpError(f"{RELEASES_API} returned a draft or prerelease")
    tag = str(release.get("tag_name", ""))
    version_of(tag)
    return tag


def ready(repo_root: Path, tag: str) -> list[str]:
    """The versioned aliases crates.io does not serve at <tag>; empty when ready.

    Only a 404 counts as missing: that is index lag, which a wait outlasts.
    Any other answer, or none, is a BumpError, so that a permanent failure is
    not reported as "try again later".
    """
    version = version_of(tag)
    aliases, _ = decdn_aliases(manifest(repo_root))
    if not aliases:
        raise BumpError(f"Cargo.toml has no decdn alias on {DECDN_GIT}")
    missing = []
    for name in sorted(n for n, spec in aliases.items() if "version" in spec):
        url = f"{CRATES_API}/{name}/{version}"
        try:
            status, _ = fetch(url)
        except OSError as e:
            raise BumpError(f"{name} {version}: crates.io unreachable: {e}") from None
        if status == 404:
            missing.append(f"{name} {version}")
        elif status != 200:
            raise BumpError(f"{name} {version}: GET {url}: HTTP {status}")
    return missing


def rewrite(repo_root: Path, tag: str) -> list[str]:
    """Point every decdn alias at <tag>; returns the rewritten alias names."""
    version = version_of(tag)
    path = repo_root / "Cargo.toml"
    lines = path.read_text().splitlines(keepends=True)
    rewritten = []
    for i, line in enumerate(lines):
        alias = ALIAS_LINE.match(line)
        if not alias:
            continue
        line, refs = REF_FIELD.subn(f'tag = "{tag}"', line)
        if refs != 1:
            raise BumpError(f"Cargo.toml: {alias[1]} names {refs} git refs, expected one")
        lines[i] = VERSION_FIELD.sub(f'version = "{version}"', line)
        rewritten.append(alias[1])
    path.write_text("".join(lines))

    # The line match is a convenience; the parsed manifest is the proof.
    aliases, _ = decdn_aliases(manifest(repo_root))
    if not aliases:
        raise BumpError(f"Cargo.toml has no decdn alias on {DECDN_GIT}")
    stale = sorted(
        name
        for name, spec in aliases.items()
        if spec.get("tag") != tag
        or "branch" in spec
        or "rev" in spec
        or spec.get("version", version) != version
    )
    if stale:
        raise BumpError(
            f"Cargo.toml: {', '.join(stale)} not rewritten to {tag} — each decdn alias "
            'must sit on one line, as `decdn-x = { git = "…", tag = "…", … }`'
        )
    return rewritten


def locked_sha(repo_root: Path) -> str:
    """The one decdn commit Cargo.lock pins."""
    lock = tomllib.loads((repo_root / "Cargo.lock").read_text())
    commits = set()
    for package in lock.get("package", []):
        source = LOCK_SOURCE.match(str(package.get("source", "")))
        if source and source["url"].rstrip("/").removesuffix(".git") == DECDN_GIT:
            commits.add(source["sha"])
    if len(commits) != 1:
        raise BumpError(f"Cargo.lock pins decdn at {len(commits)} commits, expected one")
    return next(iter(commits))


def reached_crates(repo_root: Path) -> list[Path]:
    """Directories of the published members that depend on decdn outside dev-deps."""
    root = manifest(repo_root)
    aliases, _ = decdn_aliases(root)
    kept_tables = [t for t in DEP_TABLES if t != "dev-dependencies"]
    reached = []
    for rel in root.get("workspace", {}).get("members", []):
        member = tomllib.loads((repo_root / rel / "Cargo.toml").read_text())
        if member.get("package", {}).get("publish", True) in (False, []):
            continue
        tables = [member.get(t, {}) for t in kept_tables]
        for target in member.get("target", {}).values():
            tables.extend(target.get(t, {}) for t in kept_tables)
        if any(name in aliases for deps in tables for name in deps):
            reached.append(repo_root / rel)
    return reached


def entry(tag: str, sha: str) -> str:
    version = version_of(tag)
    text = (
        f"**decdn {tag}.** The decdn crates are locked at decdn's `{tag}` release "
        f"tag (`{sha}`) and require `{version}`, which is on crates.io."
    )
    return textwrap.fill(
        text,
        width=80,
        initial_indent="- ",
        subsequent_indent="  ",
        break_long_words=False,
        break_on_hyphens=False,
    )


ENTRY_PREFIX = "- **decdn v"

# RELEASING.md § Changelogs: the sections after Changed, in their order.
AFTER_CHANGED = ("### Fixed", "### Removed", "### Security")


def add_entry(changelog: str, tag: str, sha: str) -> str:
    """<changelog> with the bump entry first under [Unreleased] → ### Changed.

    An unreleased entry for an earlier bump is replaced, not kept: only the
    last lock before a release is the one it ships.
    """
    lines = changelog.splitlines()
    try:
        start = lines.index("## [Unreleased]")
    except ValueError:
        raise BumpError("no `## [Unreleased]` heading") from None

    def unreleased_end() -> int:
        return next(
            (i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")),
            len(lines),
        )

    # Drop an earlier bump's bullet and its continuation lines, and the blank
    # line after it when one precedes it too.
    old = next(
        (i for i in range(start + 1, unreleased_end()) if lines[i].startswith(ENTRY_PREFIX)),
        None,
    )
    if old is not None:
        stop = old + 1
        while stop < len(lines) and lines[stop].startswith("  "):
            stop += 1
        if lines[old - 1] == "" and stop < len(lines) and lines[stop] == "":
            stop += 1
        del lines[old:stop]

    end = unreleased_end()
    bullet = entry(tag, sha).splitlines()
    changed = next((i for i in range(start + 1, end) if lines[i] == "### Changed"), None)
    if changed is not None:
        insert_at, block = changed + 2, [*bullet, ""]
        if insert_at > len(lines) or lines[insert_at - 1] != "":
            raise BumpError(f"expected a blank line after line {insert_at - 1}")
    else:
        insert_at = next(
            (i for i in range(start + 1, end) if lines[i] in AFTER_CHANGED),
            end,
        )
        block = ["### Changed", "", *bullet, ""]
        if lines[insert_at - 1] != "":
            block.insert(0, "")
    lines[insert_at:insert_at] = block
    return "\n".join(lines).rstrip("\n") + "\n"


def changelog(repo_root: Path, tag: str) -> list[Path]:
    """Add the bump entry to every reached crate's CHANGELOG.md."""
    sha = locked_sha(repo_root)
    written = []
    for crate in reached_crates(repo_root):
        path = crate / "CHANGELOG.md"
        if not path.is_file():
            raise BumpError(f"{path.relative_to(repo_root)} is missing")
        try:
            path.write_text(add_entry(path.read_text(), tag, sha))
        except BumpError as e:
            raise BumpError(f"{path.relative_to(repo_root)}: {e}") from None
        written.append(path)
    return written


def main(argv: list[str]) -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )
    command, args = (argv[0], argv[1:]) if argv else ("", [])
    try:
        if command == "latest" and not args:
            print(latest())
        elif command == "compare" and len(args) == 1:
            print(compare(repo_root, args[0]))
        elif command == "ready" and len(args) == 1:
            missing = ready(repo_root, args[0])
            if missing:
                print(f"crates.io does not serve {args[0]} yet:", file=sys.stderr)
                for m in missing:
                    print(f"  {m}", file=sys.stderr)
                return NOT_READY
            print(f"crates.io serves every decdn crate at {args[0]}")
        elif command == "rewrite" and len(args) == 1:
            names = rewrite(repo_root, args[0])
            print(f"pinned {', '.join(names)} to {args[0]}")
        elif command == "changelog" and len(args) == 1:
            for path in changelog(repo_root, args[0]):
                print(f"updated {path.relative_to(repo_root)}")
        elif command == "pinned" and not args:
            print(pinned_tag(repo_root) or "")
        elif command == "locked-sha" and not args:
            print(locked_sha(repo_root))
        else:
            print(__doc__, file=sys.stderr)
            return 2
    except BumpError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
