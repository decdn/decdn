#!/usr/bin/env python3
"""Fail if the sites that hold the Rust version disagree.

The version is written in three kinds of place, each read by a different tool
and none derivable from another:

    rust-toolchain.toml   [toolchain] channel        what rustup installs locally
    Cargo.toml            [workspace.package]        what cargo refuses to build
                          rust-version               with (the MSRV)
    .github/workflows/*   dtolnay/rust-toolchain@X   what CI actually compiles on

Nothing compares them. Dependabot's github-actions ecosystem bumps every
action ref on its own and touches neither TOML file, so one merged Dependabot
PR leaves CI compiling on a different compiler from every developer — which is
exactly the drift `rust-toolchain.toml` is pinned to prevent. This check turns
that PR red until the two TOML sites move in the same change.

All sites must carry the identical `X.Y.Z` string. A two-part `rust-version`
("1.95") satisfies cargo but is not the string the other sites carry, and
`stable` is the floating spelling the pin exists to refuse, so both fail here
rather than being normalised. Zero action refs is a failure too: a regex that
matches nothing must not report agreement.

There is no Dockerfile site. The image is a single Debian stage that copies
prebuilt release binaries, so no `rust:` tag ever names a version.

Run: .github/scripts/check-toolchain-pin.sh
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
# Text, not YAML: the workflows are read with the standard library only, and
# the shape being matched is one literal `uses:` value. Anchored on `uses:` so
# a comment that mentions the action is not read as a site.
ACTION_REF = re.compile(r"\buses:\s*dtolnay/rust-toolchain@(\S+)")


def collect_sites(repo_root: Path) -> tuple[list[tuple[str, str]], list[str]]:
    """Every (label, value) pair, plus the errors met while reading them."""
    sites: list[tuple[str, str]] = []
    errors: list[str] = []

    toolchain = tomllib.loads((repo_root / "rust-toolchain.toml").read_text())
    channel = toolchain.get("toolchain", {}).get("channel")
    if channel is None:
        errors.append("rust-toolchain.toml: no [toolchain] channel")
    else:
        sites.append(("rust-toolchain.toml", str(channel)))

    manifest = tomllib.loads((repo_root / "Cargo.toml").read_text())
    rust_version = manifest.get("workspace", {}).get("package", {}).get("rust-version")
    if rust_version is None:
        errors.append("Cargo.toml: no [workspace.package] rust-version")
    else:
        sites.append(("Cargo.toml", str(rust_version)))

    refs = 0
    for workflow in sorted((repo_root / ".github" / "workflows").glob("*.yml")):
        for lineno, line in enumerate(workflow.read_text().splitlines(), start=1):
            for m in ACTION_REF.finditer(line):
                refs += 1
                sites.append((f"{workflow.relative_to(repo_root)}:{lineno}", m.group(1)))
    if refs == 0:
        errors.append(
            "no dtolnay/rust-toolchain@… ref found under .github/workflows — "
            "this check inspected no CI site"
        )

    return sites, errors


def check(repo_root: Path) -> list[str]:
    sites, errors = collect_sites(repo_root)

    malformed = [(label, value) for label, value in sites if not SEMVER.match(value)]
    for label, value in malformed:
        errors.append(f"{label}: {value!r} is not an exact X.Y.Z version")
    if errors:
        return errors

    values = {value for _, value in sites}
    if len(values) > 1:
        # The majority is the intended version; name every site that differs.
        # A pure tie is reported in full.
        counts = {v: sum(1 for _, s in sites if s == v) for v in values}
        top = max(counts.values())
        majority = sorted(v for v, c in counts.items() if c == top)
        if len(majority) == 1:
            odd = [(label, v) for label, v in sites if v != majority[0]]
            errors.append(
                f"Rust version drift: {len(sites) - len(odd)} site(s) say {majority[0]} but "
                + ", ".join(f"{label} says {v}" for label, v in odd)
            )
        else:
            errors.append(
                "Rust version drift: " + ", ".join(f"{label} says {v}" for label, v in sites)
            )
    return errors


def main() -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    errors = check(repo_root)
    if errors:
        print("error: the Rust toolchain pin is inconsistent:\n", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nrust-toolchain.toml, Cargo.toml's rust-version and every\n"
            "dtolnay/rust-toolchain@… ref in .github/workflows must carry the same\n"
            "X.Y.Z. Move them together in one change (see CONTRIBUTING.md § Rust\n"
            "Toolchain).",
            file=sys.stderr,
        )
        return 1
    sites, _ = collect_sites(repo_root)
    print(f"toolchain pin OK ({sites[0][1]} at {len(sites)} sites)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
