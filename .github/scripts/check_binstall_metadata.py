#!/usr/bin/env python3
"""Fail if a crate's `cargo binstall` metadata does not name its release archive.

`cargo binstall <crate>` reads `[package.metadata.binstall]` from the manifest
published on crates.io and downloads the archive its `pkg-url` names. The
archives themselves are named by release.yml's `build-binaries` job:
`<binary>-<version>-<target>.<archive>`, with the binary at the archive root,
attached to the tag `v<version>`. Nothing else links the two. A renamed
archive, a moved binary or a new archive format still releases green; binstall
then fails to find the archive and, with `quick-install` disabled, compiles
from source, so the only symptom is a slow install. A binary crate with no
metadata at all falls through to cargo-quickinstall's third-party build.

For every `package:binary` pair the matrix builds, on every target it builds:

* the package is a workspace member with a `[[bin]]` of that name (or no
  `[[bin]]` and the package's own name), and it carries
  `[package.metadata.binstall]`;
* `pkg-url`, with the target's override applied, renders to
  `<repository>/releases/download/v<version>/<binary>-<version>-<target>.<archive>`;
* `pkg-fmt` is `tgz` for a `tar.gz` archive and `zip` for a `zip`;
* `bin-dir` renders to the bare binary, `.exe` on Windows;
* `disabled-strategies` is a list that includes `quick-install`, so binstall
  never falls back to a third-party build unless the user overrides it with
  `--strategies`.

Each template is filled from the placeholders binstall offers that key, and no
others: `pkg-url` has no `{ bin }`, `bin-dir` has no archive placeholders, and
`{ format }` means the archive format in one and the binary extension in the
other. An unknown placeholder is an error, as binstall fails to render it too.
For a `tgz` archive binstall tries both `tgz` and `tar.gz` as the format, so
either spelling passes.

Overrides are keyed by exact target triples. A `cfg(…)` key is refused:
cargo-binstall releases before cfg support ignore it, so those users would
silently get the base settings. A key that names a target the matrix does not
build is ignored, as binstall ignores it; the base settings are then checked
for every built target.

The archive steps of `build-binaries` must contain the lines that produce
that name and layout (ARCHIVE_STEP_LINES), so a rename there fails here too.

And the other way: a member with binstall metadata is built by the matrix.

Every member with binstall metadata also carries
`[package.metadata.binstall.signing]`, which binstall checks each archive
against:

* `algorithm = "minisign"` and `file = "{ url }.minisig"`, the
  `<archive>.minisig` that scripts/sign-release.sh uploads;
* `pubkey` is the same in every crate, and is one of the maintainer keys in
  `crates/cli/minisign*.pub` (the key line of the file). sign-release.sh signs
  every archive with one key and reads that key from the crates;
* `pubkey` sits on a line of its own, exactly `pubkey = "<key>"`, which is the
  only form sign-release.sh's `sed` reads, and no other manifest line starts
  with `pubkey =`.

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
# Every format spelling binstall tries for an archive, by release.yml archive.
FORMAT_SPELLINGS = {"tar.gz": ("tgz", "tar.gz"), "zip": ("zip",)}
OVERRIDABLE = ("pkg-url", "pkg-fmt", "bin-dir")
PLACEHOLDER = re.compile(r"\{\s*([a-z-]+)\s*\}")
# The lines in `build-binaries` that name each archive and put the bare binary
# at its root, as `pkg-url` and `bin-dir` expect. `$bin` is the binary half of
# a `package:binary` pair.
ARCHIVE_STEP_LINES = (
    'bin="${build#*:}"',
    'tar czf "${bin}-${VERSION}-${TARGET}.tar.gz" -C "target/$TARGET/release" "$bin"',
    "$bin = ($build -split ':')[1]",
    'Compress-Archive -Path "target/$env:TARGET/release/$bin.exe" `',
    '-DestinationPath "$bin-$env:VERSION-$env:TARGET.zip"',
)
MINISIGN_PUBS = "crates/cli/minisign*.pub"
SIGNATURE_FILE = "{ url }.minisig"
# The line scripts/sign-release.sh's sed reads the key from.
PUBKEY_LINE = re.compile(r'^pubkey = "([^"]*)"$', re.MULTILINE)
PUBKEY_ANY = re.compile(r"^\s*pubkey\s*=", re.MULTILINE)


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


def job_lines(text: str) -> list[tuple[int, str]] | None:
    """The `build-binaries` job's lines with their 1-based numbers, or None."""
    lines = text.splitlines()
    start = next(
        (i for i, line in enumerate(lines) if re.fullmatch(rf"  {BUILD_JOB}:\s*", line)), None
    )
    if start is None:
        return None
    body: list[tuple[int, str]] = []
    for lineno, line in enumerate(lines[start + 1 :], start + 2):
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and _indent(line) <= 2:
            break  # the next job
        body.append((lineno, line))
    return body


def check_archive_steps(text: str) -> list[str]:
    """An error for each ARCHIVE_STEP_LINES entry `build-binaries` lacks."""
    body = job_lines(text)
    if body is None:
        return []  # parse_build_matrix reports the missing job
    present = {line.strip() for _, line in body}
    return [
        f"{RELEASE_WORKFLOW}: `{BUILD_JOB}` no longer contains the archive step line "
        f"{want!r}; binstall's `pkg-url` and `bin-dir` expect the archive name and layout "
        "it produces. Change both together, then update ARCHIVE_STEP_LINES"
        for want in ARCHIVE_STEP_LINES
        if want not in present
    ]


def parse_build_matrix(text: str) -> tuple[list[Build], list[str]]:
    """The `build-binaries` job's `matrix.include` rows, and any parse errors.

    Reads only the shape release.yml uses: block-style rows, each opening with
    `- target:` and carrying one `key: value` scalar per line at the row's
    indent. Any other line inside `include:` (a flow-style row, a nested map or
    list, a block scalar, an empty value) is an error, never skipped: a row
    this reader passed over would leave its target unchecked.
    """
    errors: list[str] = []
    body = job_lines(text)
    if body is None:
        return [], [f"{RELEASE_WORKFLOW}: no `{BUILD_JOB}` job"]

    rows: list[dict[str, str]] = []
    include_indent: int | None = None
    key_indent: int | None = None  # where the current row's keys sit
    for lineno, line in body:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
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


def render(template: str, values: dict[str, str]) -> str:
    """Fill binstall's `{ name }` placeholders; an unknown one raises KeyError."""
    return PLACEHOLDER.sub(lambda m: values[m.group(1)], template)


def pkg_url_values(common: dict[str, str], archive: str) -> list[dict[str, str]]:
    """The placeholder sets binstall fills `pkg-url` from, one per format spelling."""
    return [
        {**common, "archive-format": fmt, "format": fmt, "archive-suffix": f".{fmt}"}
        for fmt in FORMAT_SPELLINGS[archive]
    ]


def bin_dir_values(common: dict[str, str], bin_name: str) -> dict[str, str]:
    """The placeholders binstall fills `bin-dir` from: `{ format }` is the binary extension."""
    return {**common, "bin": bin_name, "format": common["binary-ext"]}


def effective(binstall: dict, target: str) -> dict:
    """The binstall settings for `target`: the base table, with the override keyed
    by exactly `target` applied key by key."""
    settings = {k: binstall[k] for k in OVERRIDABLE if k in binstall}
    override = binstall.get("overrides", {}).get(target, {})
    settings.update({k: override[k] for k in OVERRIDABLE if k in override})
    return settings


def check(repo_root: Path) -> list[str]:
    errors: list[str] = []
    workflow = repo_root / RELEASE_WORKFLOW
    if not workflow.is_file():
        return [f"{RELEASE_WORKFLOW}: not found"]
    text = workflow.read_text()
    builds, parse_errors = parse_build_matrix(text)
    errors.extend(parse_errors)
    errors.extend(check_archive_steps(text))
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
        ext = ".exe" if "-windows-" in build.target else ""
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
            strategies = binstall.get("disabled-strategies", [])
            if not isinstance(strategies, list) or "quick-install" not in strategies:
                errors.append(
                    f"{rel}: binstall `disabled-strategies` must be a list that includes "
                    '"quick-install"; otherwise a missing archive installs a third-party '
                    "build (and a non-list fails to parse in binstall)"
                )
            settings = effective(binstall, build.target)

            package_table = manifest["package"]
            repository = package_table.get("repository")
            if repository == {"workspace": True}:
                repository = ws_package.get("repository")
            if not isinstance(repository, str):
                errors.append(f"{rel}: no `repository`, so binstall's {{ repo }} is empty")
                continue
            common = {
                "repo": repository.rstrip("/"),
                "name": package,
                "version": version,
                "target": build.target,
                "binary-ext": ext,
            }
            expected = {
                "pkg-url": (
                    f"{common['repo']}/releases/download/v{version}/"
                    f"{binary}-{version}-{build.target}.{build.archive}",
                    pkg_url_values(common, build.archive),
                ),
                "pkg-fmt": (archive_fmt, [{}]),
                "bin-dir": (f"{binary}{ext}", [bin_dir_values(common, binary)]),
            }
            for key, (want, value_sets) in expected.items():
                template = settings.get(key)
                if template is None:
                    errors.append(f"{where}: binstall `{key}` is not set")
                    continue
                try:
                    got = [render(template, values) for values in value_sets]
                except KeyError as e:
                    errors.append(
                        f"{where}: binstall `{key}` uses placeholder {e}, which binstall does "
                        "not offer that key"
                    )
                    continue
                if want not in got:
                    errors.append(
                        f"{where}: binstall `{key}` resolves to {got[0]!r}, but release.yml "
                        f"ships {want!r}"
                    )

    for name, (rel, manifest) in sorted(manifests.items()):
        binstall = manifest.get("package", {}).get("metadata", {}).get("binstall")
        if binstall is None:
            continue
        if name not in built:
            errors.append(
                f"{rel}: has [package.metadata.binstall] but release.yml's `{BUILD_JOB}` "
                "matrix builds no archive for it"
            )
        for key in binstall.get("overrides", {}):
            if key.startswith("cfg("):
                errors.append(
                    f"{rel}: binstall override {key!r}: key overrides by exact target triple; "
                    "cargo-binstall releases before cfg support ignore a cfg(…) key and use "
                    "the base settings"
                )
    errors.extend(check_signing(repo_root, manifests))
    return errors


def maintainer_keys(repo_root: Path) -> dict[str, str]:
    """The key line of each `crates/cli/minisign*.pub`, mapped to its file."""
    keys: dict[str, str] = {}
    for path in sorted(repo_root.glob(MINISIGN_PUBS)):
        lines = path.read_text().splitlines()
        if len(lines) >= 2:
            keys[lines[1].strip()] = str(path.relative_to(repo_root))
    return keys


def check_signing(repo_root: Path, manifests: dict[str, tuple[Path, dict]]) -> list[str]:
    errors: list[str] = []
    keys = maintainer_keys(repo_root)
    pubkeys: dict[str, list[str]] = {}
    for _, (rel, manifest) in sorted(manifests.items()):
        binstall = manifest.get("package", {}).get("metadata", {}).get("binstall")
        if binstall is None:
            continue
        signing = binstall.get("signing")
        if signing is None:
            errors.append(
                f"{rel}: no [package.metadata.binstall.signing]; binstall would install "
                "the archive without checking its signature"
            )
            continue
        if signing.get("algorithm") != "minisign":
            errors.append(f'{rel}: binstall signing `algorithm` must be "minisign"')
        if signing.get("file") != SIGNATURE_FILE:
            errors.append(
                f"{rel}: binstall signing `file` must be {SIGNATURE_FILE!r}, the "
                "<archive>.minisig sign-release.sh uploads"
            )
        pubkey = signing.get("pubkey")
        if not isinstance(pubkey, str):
            errors.append(f"{rel}: binstall signing has no `pubkey`")
            continue
        pubkeys.setdefault(pubkey, []).append(str(rel))
        if pubkey not in keys:
            errors.append(
                f"{rel}: binstall signing `pubkey` is not the key line of any "
                f"{MINISIGN_PUBS}; sign-release.sh signs only with a maintainer key"
            )
        text = (repo_root / rel).read_text()
        if PUBKEY_LINE.findall(text) != [pubkey] or len(PUBKEY_ANY.findall(text)) != 1:
            errors.append(
                f'{rel}: write the binstall key as one line, exactly `pubkey = "<key>"`, '
                "and no other line starting `pubkey =`; that is the only form "
                "sign-release.sh reads"
            )
    if len(pubkeys) > 1:
        listing = "; ".join(f"{k} in {', '.join(v)}" for k, v in sorted(pubkeys.items()))
        errors.append(
            f"binstall signing `pubkey` differs between crates ({listing}); "
            "sign-release.sh signs every archive with one key"
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
