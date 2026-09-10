#!/usr/bin/env python3
"""Fail if the sites that hold the Rust version disagree.

The version is written in three kinds of place, each read by a different tool
and none derivable from another:

    rust-toolchain.toml   [toolchain] channel        what rustup installs locally
    Cargo.toml            [workspace.package]        what cargo refuses to build
                          rust-version               with (the MSRV)
    .github/workflows/*   dtolnay/rust-toolchain@X   what CI actually compiles on
    .github/actions/*/    (or its `with: toolchain:`
      action.y*ml         input, which overrides the @ref)

Without this check nothing compares them. Dependabot's github-actions
ecosystem bumps every action ref on its own and touches neither TOML file, so
one merged Dependabot PR leaves CI compiling on a different compiler from
every developer — which is exactly the drift `rust-toolchain.toml` is pinned
to prevent. This check turns that PR red until the two TOML sites move in the
same change.

All sites must carry the identical `X.Y.Z` string. A two-part `rust-version`
("1.95") satisfies cargo but is not the string the other sites carry, and
`stable` is the floating spelling the pin exists to refuse, so both fail here
rather than being normalised. Zero action refs is a failure too: a regex that
matches nothing must not report agreement.

The workflow text is read line by line with YAML comments stripped, so a
commented-out step is neither a site nor enough to satisfy the zero-refs
floor. A `with: toolchain:` input under a `dtolnay/rust-toolchain` step is
the version that step installs, whatever the `@ref` says, so it replaces the
ref as that site's value; an expression there (`${{ … }}`) cannot be read and
is refused rather than skipped. Out of scope: a `RUSTUP_TOOLCHAIN` env var,
which no workflow here sets.

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
# Text, not YAML: the workflows are read with the standard library only. Each
# line has its comment stripped first, so `uses:` is matched only on a live
# step; the value may be bare or quoted.
ACTION_REF = re.compile(r"\buses:\s*['\"]?dtolnay/rust-toolchain@([^\s'\"]+)")
WITH_KEY = re.compile(r"^\s*with:\s*$")
TOOLCHAIN_INPUT = re.compile(r"^\s*toolchain:\s*['\"]?(.+?)['\"]?\s*$")
# A `#` at line start or after whitespace opens a YAML comment; one inside a
# value does not, and a `uses:` value never carries one.
COMMENT = re.compile(r"(^|\s)#.*$")


def indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def action_sites(path: Path, label: str) -> list[tuple[str, str]]:
    """Every `dtolnay/rust-toolchain` step in one file, as (label:line, version).

    The version is the `with: toolchain:` input when the step has one — that
    is what the action installs — and the `@ref` otherwise.
    """
    lines = [COMMENT.sub("", raw) for raw in path.read_text().splitlines()]
    sites: list[tuple[str, str]] = []
    for i, line in enumerate(lines):
        m = ACTION_REF.search(line)
        if not m:
            continue
        version = m.group(1)
        # `uses:` sits at the step's key indent (`- uses:` puts it two past the
        # dash). Sibling keys share that indent; the next step's dash or an
        # outer key sits at less. A `with:` sibling's inputs sit deeper.
        key_indent = line.index("uses:")
        in_with = False
        for follow in lines[i + 1 :]:
            if not follow.strip():
                continue
            ind = indent_of(follow)
            if ind < key_indent:
                break
            if ind == key_indent:
                in_with = bool(WITH_KEY.match(follow))
                continue
            if in_with:
                t = TOOLCHAIN_INPUT.match(follow)
                if t:
                    version = t.group(1).strip()
                    break
        sites.append((f"{label}:{i + 1}", version))
    return sites


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

    # Both spellings: GitHub Actions reads `.yml` and `.yaml` alike, so a file
    # added under the other extension must not escape the scan. Composite
    # actions under .github/actions/ run steps too.
    github = repo_root / ".github"
    files = sorted(
        list((github / "workflows").glob("*.yml"))
        + list((github / "workflows").glob("*.yaml"))
        + list(github.glob("actions/*/action.yml"))
        + list(github.glob("actions/*/action.yaml"))
    )
    refs = 0
    for path in files:
        found = action_sites(path, str(path.relative_to(repo_root)))
        refs += len(found)
        sites.extend(found)
    if refs == 0:
        errors.append(
            "no dtolnay/rust-toolchain@… step found under .github/workflows or "
            ".github/actions — this check inspected no CI site"
        )

    return sites, errors


def check(repo_root: Path) -> list[str]:
    sites, errors = collect_sites(repo_root)

    for label, value in sites:
        if "${{" in value:
            errors.append(
                f"{label}: `with: toolchain: {value}` is an expression this check cannot "
                "read — write the version literally"
            )
        elif not SEMVER.match(value):
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
            stdout=subprocess.PIPE,
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
            "dtolnay/rust-toolchain step under .github/ must carry the same X.Y.Z.\n"
            "Move them together in one change (see CONTRIBUTING.md § Rust Toolchain).",
            file=sys.stderr,
        )
        return 1
    sites, _ = collect_sites(repo_root)
    print(f"toolchain pin OK ({sites[0][1]} at {len(sites)} sites)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
