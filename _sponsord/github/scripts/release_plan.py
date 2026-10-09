#!/usr/bin/env python3
"""What a release tag releases: the one table every release step reads.

The workspace is released as a whole under one version, so a tag is
`v<MAJOR.MINOR.PATCH[-pre]>`, as `cargo release <level>` pushes it, and every
release carries every crate: the archives of each binary, its container
image, and every library on crates.io. The table decides what release.yml
builds, what sign-release.sh signs and promotes, and what publish-crates.sh
uploads. Keeping it in one place, rather than in a workflow matrix, a signing
script and a publishing script separately, is what stops the three from
disagreeing about a crate.

The archive names (`<binary>-<version>-<target>.{tar.gz,zip}`, binary at the
archive root) are a contract with every installer the onramp has served:
`decdn.sh` / `decdn.ps1` fetch exactly that name for `decdn-sponsored`.

Usage:
  release_plan.py <tag>              key=value lines for $GITHUB_OUTPUT
  release_plan.py <tag> --get KEY    one value (tag, version, archives, images,
                                     images_json, prerelease, matrix)
  release_plan.py <tag> --baseline   the tag cargo semver-checks compares
                                     against, from this repository's tags
                                     (nothing for the first release or a
                                     0.0.z tag)
  release_plan.py <tag> --latest     `true` if the tag should be the
                                     repository's "Latest release", from
                                     this repository's tags
  release_plan.py --images           every image name, one per line
  release_plan.py --tag-pattern      an ERE matching every release tag

A tag that is not a release tag is an error, never an empty plan.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections.abc import Iterable

LINUX = [
    ("x86_64-unknown-linux-gnu", "ubuntu-22.04"),
    ("aarch64-unknown-linux-gnu", "ubuntu-22.04-arm"),
]
CLIENT = LINUX + [
    ("x86_64-apple-darwin", "macos-latest"),
    ("aarch64-apple-darwin", "macos-latest"),
    ("x86_64-pc-windows-msvc", "windows-latest"),
    # Cross-compiled on the x64 runner with the MSVC ARM64 toolchain.
    ("aarch64-pc-windows-msvc", "windows-latest"),
]

# crate -> where it lives, the binary its archives carry (if any), the targets
# they are built for, and the container image (if any). Every release carries
# all of them.
CRATES: dict[str, dict] = {
    "sponsord-api": {"dir": "crates/sponsord-api", "binary": None, "targets": [], "image": None},
    "sponsord-core": {"dir": "crates/sponsord-core", "binary": None, "targets": [], "image": None},
    "sponsord": {
        "dir": "crates/sponsord",
        "binary": "sponsord",
        "targets": LINUX,
        "image": "sponsord",
    },
    "sponsord-onramp": {
        "dir": "crates/sponsord-onramp",
        "binary": "sponsord-onramp",
        "targets": LINUX,
        "image": "sponsord-onramp",
    },
    "decdn-sponsored": {
        "dir": "crates/decdn-sponsored",
        "binary": "decdn-sponsored",
        "targets": CLIENT,
        # Also carries decdn's `decdn`, which release.yml's docker job fetches
        # from decdn's release (fetch-decdn.sh), not from this one's archives.
        "image": "decdn-sponsored",
    },
}

# Plain groups, no `(?:`: tag_pattern() hands the same text to git-cliff (Rust
# regex) and to bash (ERE), and ERE has no non-capturing group.
SEMVER = r"[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?"
TAG = re.compile(rf"^v({SEMVER})$")


def parse_tag(tag: str) -> str:
    """The version a release tag names."""
    m = TAG.match(tag)
    if not m:
        raise ValueError(f"{tag!r} is not a release tag: expected vMAJOR.MINOR.PATCH[-pre]")
    return m.group(1)


def legs() -> list[dict[str, str]]:
    """One build job per target, each building every binary released for it.

    Per target, not per binary: the binaries share most of their dependency
    graph, so one `target/` per target compiles the shared part once. `builds`
    lists `package:binary` pairs, space-separated for the workflow's bash loop.
    """
    by_target: dict[str, dict[str, str]] = {}
    for crate, spec in CRATES.items():
        for target, runner in spec["targets"]:
            leg = by_target.setdefault(target, {"target": target, "runner": runner, "builds": ""})
            if leg["runner"] != runner:
                raise ValueError(f"{target} is built on both {leg['runner']} and {runner}")
            leg["builds"] = f"{leg['builds']} {crate}:{spec['binary']}".lstrip()
    return list(by_target.values())


def archive_count() -> int:
    """Every (binary, target) pair is one archive."""
    return sum(len(spec["targets"]) for spec in CRATES.values())


def plan(tag: str) -> dict[str, str]:
    version = parse_tag(tag)
    prerelease = "-" in version
    return {
        "tag": tag,
        "version": version,
        "archives": str(archive_count()),
        # Space-separated for the scripts, JSON for the docker job's matrix.
        "images": " ".join(images()),
        "images_json": json.dumps(images(), separators=(",", ":")),
        # GitHub's prerelease flag, set on the draft.
        "prerelease": "true" if prerelease else "false",
        # A GitHub Actions matrix: one leg per target (legs()).
        "matrix": json.dumps({"include": legs()}, separators=(",", ":")),
    }


def _precedence(version: str) -> tuple:
    """A sort key in SemVer 2.0.0 precedence: a pre-release sorts below its
    release, numeric identifiers numerically and below alphanumeric ones."""
    core, _, pre = version.partition("-")
    numbers = tuple(int(n) for n in core.split("."))
    if not pre:
        return (numbers, 1, ())
    ids = tuple((0, int(i), "") if i.isdigit() else (1, 0, i) for i in pre.split("."))
    return (numbers, 0, ids)


def baseline(tag: str, tags: Iterable[str]) -> str | None:
    """The release `tag` is checked against by cargo semver-checks: the highest
    release tag below it, or None for the first release.

    Below the tag, not merely the newest tag: re-running a release's workflow
    and cutting a maintenance release under a newer one are both supported, and
    a newer baseline would run the check backwards.

    None for a 0.0.z tag as well. Cargo treats every 0.0.z bump as breaking,
    so cargo semver-checks accepts any API change there and has nothing to
    find, while building the baseline can still fail: v0.0.1 required decdn
    `0.0.0` from decdn's `main`, which stopped resolving once decdn's `main`
    moved to 0.0.1, and cargo semver-checks builds the baseline without its
    Cargo.lock.
    """
    current = _precedence(parse_tag(tag))
    if current[0][:2] == (0, 0):
        return None
    below = []
    for t in tags:
        m = TAG.match(t)
        if m and _precedence(m.group(1)) < current:
            below.append((_precedence(m.group(1)), t))
    return max(below)[1] if below else None


def is_latest(tag: str, tags: Iterable[str]) -> bool:
    """Whether `tag` should be the repository's "Latest release": stable, and
    not below any other stable release tag. A maintenance release cut under a
    newer release (supported, see baseline) never takes it."""
    current = _precedence(parse_tag(tag))
    if "-" in parse_tag(tag):
        return False
    for t in tags:
        m = TAG.match(t)
        if m and "-" not in m.group(1) and _precedence(m.group(1)) > current:
            return False
    return True


def images() -> list[str]:
    return sorted(spec["image"] for spec in CRATES.values() if spec["image"])


def tag_pattern() -> str:
    """An ERE matching exactly the tags parse_tag() accepts, for git-cliff.

    As strict as parse_tag(), not just a `v<digit>` prefix: a stray tag the
    release workflow refuses (`v0-wip`) stays in the repository, and a looser
    pattern would let `git cliff --latest` stop at it and drop commits from
    the next release's notes.
    """
    return rf"^v{SEMVER}$"


def main(argv: list[str]) -> int:
    if argv == ["--images"]:
        print("\n".join(images()))
        return 0
    if argv == ["--tag-pattern"]:
        print(tag_pattern())
        return 0
    if len(argv) == 2 and argv[1] in ("--baseline", "--latest"):
        tags = subprocess.run(
            ["git", "tag", "--list"], capture_output=True, text=True, check=True
        ).stdout.split()
        try:
            if argv[1] == "--latest":
                print("true" if is_latest(argv[0], tags) else "false")
                return 0
            prev = baseline(argv[0], tags)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if prev:
            print(prev)
        return 0
    if len(argv) not in (1, 3) or (len(argv) == 3 and argv[1] != "--get"):
        print(__doc__.split("\n\n")[-2], file=sys.stderr)
        return 2
    try:
        result = plan(argv[0])
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if len(argv) == 3:
        key = argv[2]
        if key not in result:
            print(f"error: unknown key {key!r}; one of {', '.join(result)}", file=sys.stderr)
            return 2
        print(result[key])
        return 0
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
