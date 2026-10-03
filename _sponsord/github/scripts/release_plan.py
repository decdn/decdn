#!/usr/bin/env python3
"""What a release tag releases: the one table every release step reads.

Each crate is versioned and released on its own, so a tag names one crate:
`<crate>-v<MAJOR.MINOR.PATCH[-pre]>`, as `cargo release -p <crate>` pushes it.
The tag decides everything downstream — which archives release.yml builds,
whether it builds an image, what sign-release.sh signs and promotes, and what
publish-crates.sh uploads. Keeping that in one table, rather than in a
workflow matrix, a signing script and a publishing script separately, is what
stops the three from disagreeing about a crate.

The archive names (`<binary>-<version>-<target>.{tar.gz,zip}`, binary at the
archive root) are a contract with every installer the onramp has served:
`decdn.sh` / `decdn.ps1` fetch exactly that name for `decdn-sponsored`.

Usage:
  release_plan.py <tag>              key=value lines for $GITHUB_OUTPUT
  release_plan.py <tag> --get KEY    one value (crate, version, dir, image,
                                     archives, onramp_pin, prerelease,
                                     latest, matrix)
  release_plan.py --images           every image name, one per line
  release_plan.py --tag-pattern      an ERE matching every release tag

A tag this table does not know is an error, never an empty plan.
"""

from __future__ import annotations

import json
import re
import sys

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
# they are built for, the container image (if any), and whether its release is
# the one the onramp's installers pin (ONRAMP_CLI_RELEASE).
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
        "image": None,
        "onramp_pin": True,
    },
}

SEMVER = r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?"
# Longest names first, so the alternation never prefers `sponsord` over
# `sponsord-onramp` (the `-v` anchor already rules that out; this is belt and
# braces for a future name that would not).
# Plain `[a-z-]` names, joined unescaped so the same pattern reads the same in
# Python, git-cliff (Rust regex) and bash ERE.
_NAMES = "|".join(sorted(CRATES, key=len, reverse=True))
assert all(re.fullmatch(r"[a-z][a-z-]*", c) for c in CRATES)
TAG = re.compile(rf"^({_NAMES})-v({SEMVER})$")


def parse_tag(tag: str) -> tuple[str, str]:
    m = TAG.match(tag)
    if not m:
        raise ValueError(
            f"{tag!r} is not a release tag: expected <crate>-vMAJOR.MINOR.PATCH[-pre] "
            f"for one of {', '.join(sorted(CRATES))}"
        )
    return m.group(1), m.group(2)


def plan(tag: str) -> dict[str, str]:
    crate, version = parse_tag(tag)
    spec = CRATES[crate]
    prerelease = "-" in version
    legs = [
        {"package": crate, "binary": spec["binary"], "target": target, "runner": runner}
        for target, runner in spec["targets"]
    ]
    return {
        "tag": tag,
        "crate": crate,
        "version": version,
        "dir": spec["dir"],
        "image": spec["image"] or "",
        "archives": str(len(legs)),
        "onramp_pin": "true" if spec.get("onramp_pin") else "false",
        # GitHub's prerelease flag, set on the draft.
        "prerelease": "true" if prerelease else "false",
        # The repository's "Latest release": the CLI users install, and never a
        # prerelease of it.
        "latest": "true" if spec.get("onramp_pin") and not prerelease else "false",
        # A GitHub Actions matrix; an empty `include` is never used, because
        # release.yml skips the build when `archives` is 0.
        "matrix": json.dumps({"include": legs}, separators=(",", ":")),
    }


def images() -> list[str]:
    return sorted(spec["image"] for spec in CRATES.values() if spec["image"])


def tag_pattern() -> str:
    """An ERE for every release tag, for git-cliff and the scripts' tag filters."""
    return rf"^({_NAMES})-v[0-9]"


def main(argv: list[str]) -> int:
    if argv == ["--images"]:
        print("\n".join(images()))
        return 0
    if argv == ["--tag-pattern"]:
        print(tag_pattern())
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
