#!/usr/bin/env python3
"""Fail if a crate's `cargo binstall` metadata does not name its release archive.

`cargo binstall <crate>` reads `[package.metadata.binstall]` from the manifest
published on crates.io and downloads the archive its `pkg-url` names. The
archives themselves are named by release.yml's `build-binaries` matrix:
`<binary>-<version>-<target>.<archive>`, attached to the tag `v<version>`.
Nothing links the two. A renamed archive, a new target or a new binary still
releases green, and binstall then 404s; with `quick-install` disabled it
compiles from source, so the only symptom is a slow install.

For every `package:binary` pair the matrix builds, on every target it builds:

* the package is a workspace member with a `[[bin]]` of that name (or no
  `[[bin]]` and the package's own name), and it carries
  `[package.metadata.binstall]`;
* `pkg-url`, with the target's override applied (an exact target key wins over
  `cfg(target_os = "…")`; any other key is refused, since this check cannot
  evaluate it), renders to
  `<repository>/releases/download/v<version>/<binary>-<version>-<target>.<archive>`;
* `pkg-fmt` is `tgz` for a `tar.gz` archive and `zip` for a `zip`;
* `bin-dir` renders to the bare binary, `.exe` on Windows: the archives carry
  it at their root;
* `disabled-strategies` includes `quick-install`, so binstall never falls back
  to a third-party build.

And the other way: a member with binstall metadata is built by the matrix.

release.yml is read line by line, not as YAML: the `packaging` job runs this
with the standard library only. The test suite parses the real file with
pyyaml and compares, so the line reader cannot drift into a quiet false pass.

Run: .github/scripts/check-binstall-metadata.sh
"""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

RELEASE_WORKFLOW = Path(".github/workflows/release.yml")
BUILD_JOB = "build-binaries"
PKG_FMT = {"tar.gz": "tgz", "zip": "zip"}
OVERRIDABLE = ("pkg-url", "pkg-fmt", "bin-dir")
PLACEHOLDER = re.compile(r"\{\s*([a-z-]+)\s*\}")
CFG_TARGET_OS = re.compile(r'^cfg\(\s*target_os\s*=\s*"([a-z]+)"\s*\)$')


@dataclass(frozen=True)
class Build:
    """One row of the release matrix: a target and what it builds."""

    target: str
    # (package, binary) pairs, in matrix order.
    builds: tuple[tuple[str, str], ...]
    archive: str


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _value(raw: str) -> str:
    raw = raw.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "'\"":
        return raw[1:-1]
    return raw


def parse_build_matrix(text: str) -> tuple[list[Build], list[str]]:
    """The `build-binaries` job's `matrix.include` rows, and any parse errors.

    Reads only the shape release.yml uses: block-style rows, each opening with
    `- target:` and carrying one `key: value` scalar per line at the row's
    indent. Any other line inside `include:` (a flow-style row, a nested map or
    list, a block scalar, an empty value) is an error, never skipped: a row
    this reader passed over would leave its target unchecked.
    """
    lines = text.splitlines()
    errors: list[str] = []
    start = next(
        (i for i, line in enumerate(lines) if re.fullmatch(rf"  {BUILD_JOB}:\s*", line)), None
    )
    if start is None:
        return [], [f"{RELEASE_WORKFLOW}: no `{BUILD_JOB}` job"]

    rows: list[dict[str, str]] = []
    include_indent: int | None = None
    key_indent: int | None = None  # where the current row's keys sit
    for lineno, line in enumerate(lines[start + 1 :], start + 2):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if _indent(line) <= 2:
            break  # the next job
        if include_indent is None:
            if stripped == "include:":
                include_indent = _indent(line)
            continue
        if _indent(line) <= include_indent:
            break  # past `include:`
        m = re.fullmatch(r"(- )?([A-Za-z0-9_-]+):\s*(.*)", stripped)
        item, key, value = m.groups() if m else (None, "", "")
        if item:
            key_indent = _indent(line) + len(item)
        if (
            not m
            or not value
            or value[0] in "{[|>&*!"
            or (not item and _indent(line) != key_indent)
        ):
            errors.append(
                f"{RELEASE_WORKFLOW}:{lineno}: `{BUILD_JOB}` matrix.include line {stripped!r} "
                "is not a `key: value` scalar of a block-style row; this check reads only rows "
                "written as `- target: …` with one `key: value` per line"
            )
            continue
        if item:
            if key != "target":
                errors.append(
                    f"{RELEASE_WORKFLOW}:{lineno}: a `{BUILD_JOB}` matrix row opens with "
                    f"`{key}:`, not `target:`; this check reads only rows that open with "
                    "`- target:`"
                )
            rows.append({})
        rows[-1][key] = _value(value)

    if include_indent is None:
        errors.append(f"{RELEASE_WORKFLOW}: `{BUILD_JOB}` has no `matrix.include`")

    builds: list[Build] = []
    for row in rows:
        target = row.get("target", "?")
        missing = [k for k in ("target", "builds", "archive") if not row.get(k)]
        if missing:
            errors.append(f"{RELEASE_WORKFLOW}: matrix row {target} has no {', '.join(missing)}")
            continue
        pairs = []
        for entry in row["builds"].split():
            package, sep, binary = entry.partition(":")
            if not (sep and package and binary):
                errors.append(
                    f"{RELEASE_WORKFLOW}: matrix row {target}: builds entry {entry!r} is "
                    "not package:binary"
                )
                continue
            pairs.append((package, binary))
        builds.append(Build(target, tuple(pairs), row["archive"]))
    return builds, errors


def workspace_members(repo_root: Path, manifest: dict) -> list[Path]:
    """Every member directory, from the root manifest's `members`."""
    members: list[Path] = []
    for pattern in manifest.get("workspace", {}).get("members", []):
        if any(ch in pattern for ch in "*?["):
            members.extend(sorted(p for p in repo_root.glob(pattern) if p.is_dir()))
        else:
            members.append(repo_root / pattern)
    return members


def target_os(target: str) -> str | None:
    """The `target_os` cfg value of a target triple this matrix builds."""
    if "-windows-" in target:
        return "windows"
    if target.endswith("-apple-darwin"):
        return "macos"
    if "-linux-" in target:
        return "linux"
    return None


def render(template: str, values: dict[str, str]) -> str:
    """Fill binstall's `{ name }` placeholders; an unknown one raises KeyError."""
    return PLACEHOLDER.sub(lambda m: values[m.group(1)], template)


def effective(binstall: dict, target: str) -> tuple[dict, list[str]]:
    """The binstall settings for `target`, its override applied."""
    settings = {k: binstall[k] for k in OVERRIDABLE if k in binstall}
    errors: list[str] = []
    exact: dict | None = None
    by_os: dict | None = None
    for key, override in binstall.get("overrides", {}).items():
        m = CFG_TARGET_OS.match(key)
        if key == target:
            exact = override
        elif m:
            if m.group(1) == target_os(target):
                by_os = override
        elif "(" in key:
            errors.append(
                f'override {key!r}: only exact targets and cfg(target_os = "…") are '
                "checked; write the override in one of those forms"
            )
    for override in (by_os, exact):  # exact last, so it wins
        if override:
            settings.update({k: override[k] for k in OVERRIDABLE if k in override})
    return settings, errors


def check(repo_root: Path) -> list[str]:
    errors: list[str] = []
    workflow = repo_root / RELEASE_WORKFLOW
    if not workflow.is_file():
        return [f"{RELEASE_WORKFLOW}: not found"]
    builds, parse_errors = parse_build_matrix(workflow.read_text())
    errors.extend(parse_errors)
    if not builds:
        errors.append(f"{RELEASE_WORKFLOW}: the `{BUILD_JOB}` matrix is empty; checked nothing")
        return errors

    root = tomllib.loads((repo_root / "Cargo.toml").read_text())
    ws_package = root.get("workspace", {}).get("package", {})
    version = ws_package.get("version")
    if not isinstance(version, str):
        return errors + ["Cargo.toml: [workspace.package] version is missing"]

    manifests: dict[str, tuple[Path, dict]] = {}
    for crate_root in workspace_members(repo_root, root):
        path = crate_root / "Cargo.toml"
        if path.is_file():
            manifest = tomllib.loads(path.read_text())
            name = manifest.get("package", {}).get("name")
            if name:
                manifests[name] = (path.relative_to(repo_root), manifest)

    built: set[str] = set()
    for build in builds:
        archive_fmt = PKG_FMT.get(build.archive)
        if archive_fmt is None:
            errors.append(
                f"{RELEASE_WORKFLOW}: matrix row {build.target} has archive "
                f"{build.archive!r}; binstall formats are known here only for "
                f"{', '.join(PKG_FMT)}"
            )
            continue
        os_name = target_os(build.target)
        if os_name is None:
            errors.append(
                f"{RELEASE_WORKFLOW}: matrix row {build.target}: unknown target_os; "
                "teach target_os() this triple"
            )
            continue
        for package, binary in build.builds:
            built.add(package)
            if package not in manifests:
                errors.append(
                    f"{RELEASE_WORKFLOW}: matrix row {build.target} builds {package}, "
                    "which is not a workspace member"
                )
                continue
            rel, manifest = manifests[package]
            where = f"{rel} ({build.target})"
            bins = [b.get("name") for b in manifest.get("bin", [])] or [package]
            if binary not in bins:
                errors.append(
                    f"{where}: the matrix ships binary {binary!r}, but the crate's "
                    f"binaries are {bins}"
                )
            binstall = manifest.get("package", {}).get("metadata", {}).get("binstall")
            if binstall is None:
                errors.append(
                    f"{rel}: the release ships {binary} archives but the crate has no "
                    "[package.metadata.binstall]; `cargo binstall` would not find them"
                )
                continue
            if "quick-install" not in binstall.get("disabled-strategies", []):
                errors.append(
                    f'{rel}: binstall `disabled-strategies` must include "quick-install"; '
                    "otherwise a missing archive installs a third-party build"
                )
            settings, override_errors = effective(binstall, build.target)
            errors.extend(f"{rel}: {e}" for e in override_errors)

            package_table = manifest["package"]
            repository = package_table.get("repository")
            if repository == {"workspace": True}:
                repository = ws_package.get("repository")
            if not isinstance(repository, str):
                errors.append(f"{rel}: no `repository`, so binstall's {{ repo }} is empty")
                continue
            ext = ".exe" if os_name == "windows" else ""
            values = {
                "repo": repository.rstrip("/"),
                "name": package,
                "version": version,
                "target": build.target,
                "bin": binary,
                "binary-ext": ext,
                "archive-format": archive_fmt,
                "archive-suffix": f".{build.archive}",
                "format": archive_fmt,
            }
            expected = {
                "pkg-url": (
                    f"{values['repo']}/releases/download/v{version}/"
                    f"{binary}-{version}-{build.target}.{build.archive}"
                ),
                "pkg-fmt": archive_fmt,
                "bin-dir": f"{binary}{ext}",
            }
            for key, want in expected.items():
                template = settings.get(key)
                if template is None:
                    errors.append(f"{where}: binstall `{key}` is not set")
                    continue
                try:
                    got = render(template, values)
                except KeyError as e:
                    errors.append(f"{where}: binstall `{key}` uses unknown placeholder {e}")
                    continue
                if got != want:
                    errors.append(
                        f"{where}: binstall `{key}` resolves to {got!r}, but release.yml "
                        f"ships {want!r}"
                    )

    for name, (rel, manifest) in sorted(manifests.items()):
        if "binstall" in manifest.get("package", {}).get("metadata", {}) and name not in built:
            errors.append(
                f"{rel}: has [package.metadata.binstall] but release.yml's `{BUILD_JOB}` "
                "matrix builds no archive for it"
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
        print("error: cargo binstall metadata drifts from the release archives:\n", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nEach binary crate's [package.metadata.binstall] must name the archive\n"
            "release.yml's build-binaries job attaches to the GitHub Release.\n"
            "See .github/scripts/check_binstall_metadata.py.",
            file=sys.stderr,
        )
        return 1
    builds, _ = parse_build_matrix((repo_root / RELEASE_WORKFLOW).read_text())
    n = sum(len(b.builds) for b in builds)
    print(f"binstall metadata OK ({n} archives across {len(builds)} targets)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
