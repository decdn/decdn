#!/usr/bin/env python3
"""Fail if a publishable crate embeds a file from outside its own package.

`include!`, `include_str!` and `include_bytes!` resolve against the source tree,
so a path reaching the workspace root builds perfectly in-tree and then fails
for every `cargo install` user: a published `.crate` contains only its own
package directory. The failure surfaces at publish time — after a signed release
exists and after the crates it depends on are already on crates.io, where a
version can never be replaced.

`cargo publish --workspace --dry-run` catches this properly by verify-building
each packaged crate, but it costs a full rebuild of the workspace. That runs in
release.yml, before the draft is created. This is the cheap PR-time signal that
stops the mistake from ever reaching a tag.

Two deliberate calibrations:

* Sites under `#[cfg(test)]` are *warnings*, not errors — the verify build does
  not enable `cfg(test)`, so they do not block publishing. They are still real
  (the file is absent from the `.crate`, so the shipped crate's own tests cannot
  run), so every one must be listed in KNOWN_TEST_ONLY below. A new one fails.
  That keeps a permanent, unread warning from masking the next regression.
* An embedded path that is inside the crate but *not tracked by git* is an
  error: `cargo package` ships tracked files, so a gitignored target is just as
  absent from the `.crate` as one outside the directory.

Crates with `publish = false` are exempt: they are never packaged, so reaching
out to workspace fixtures is legitimate for them.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

# Every test-only escape that is known and accepted, as
# "<path to the .rs file>" -> "<embedded path, relative to the repo root>".
# Entries must stay exact: an unlisted warning fails, and a listed one that no
# longer fires also fails, so this cannot rot into a stale allowlist.
KNOWN_TEST_ONLY: dict[str, str] = {}

# Rust accepts all three delimiters and rustfmt does not normalise them, so
# keying only on `(` would miss `include_str!["…"]` entirely — a miss, not the
# "unresolvable" report the narrow literal regex below is designed to produce.
OPENERS = {"(": ")", "[": "]", "{": "}"}
MACRO = re.compile(r"\binclude(?:_str|_bytes)?!\s*([(\[{])")
# Located on the masked copy (so a commented-out attribute is skipped) but read
# from the original text, because masking blanks the quoted path itself.
PATH_ATTR_START = re.compile(r"#\s*\[\s*path\s*=")
PATH_ATTR = re.compile(r"#\s*\[\s*path\s*=\s*r?\"([^\"\\]*)\"\s*\]")
# A plain "…" or r"…" literal. Deliberately narrow: anything this does not match
# is reported as unresolvable rather than assumed fine, so a construction that
# hides the path cannot slip through.
LITERAL = re.compile(r'^r?"([^"\\]*)"$')
MANIFEST_DIR = re.compile(
    r'^concat!\s*\(\s*env!\s*\(\s*"CARGO_MANIFEST_DIR"\s*\)\s*,\s*r?"([^"\\]*)"\s*,?\s*\)$'
)
CFG_TEST = re.compile(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]")


def mask(src: str) -> str:
    """`src` with comments and literals blanked out, offsets preserved.

    Brace counting and macro-site detection both run on this, so a `{` inside a
    string or a `//` comment cannot throw off the structure, and an
    `include_str!` named in a doc comment is not mistaken for a real one.
    """
    out = list(src)
    i, n = 0, len(src)

    def blank(start: int, end: int) -> None:
        for j in range(start, min(end, n)):
            if out[j] != "\n":
                out[j] = " "

    while i < n:
        two = src[i : i + 2]
        if two == "//":
            end = src.find("\n", i)
            end = n if end == -1 else end
            blank(i, end)
            i = end
        elif two == "/*":
            depth, j = 1, i + 2  # Rust block comments nest
            while j < n and depth:
                if src[j : j + 2] == "/*":
                    depth, j = depth + 1, j + 2
                elif src[j : j + 2] == "*/":
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif m := re.match(r'(?:b|r|br|rb)?(#*)"', src[i : i + 8]):
            hashes = m.group(1)
            body = i + m.end()
            if "r" in m.group(0):
                close = src.find('"' + hashes, body)
                j = n if close == -1 else close + 1 + len(hashes)
            else:
                j = body
                while j < n:
                    if src[j] == "\\":
                        j += 2
                        continue
                    if src[j] == '"':
                        j += 1
                        break
                    j += 1
            blank(i, j)
            i = j
        elif src[i] == "'" and (cm := re.match(r"'(?:\\.|[^\\'])'", src[i : i + 4])):
            j = i + len(cm.group(0))
            blank(i, j)
            i = j
        else:
            i += 1
    return "".join(out)


def cfg_test_regions(masked: str) -> list[tuple[int, int]]:
    """Byte spans of every `#[cfg(test)]`-annotated braced item."""
    regions = []
    for m in CFG_TEST.finditer(masked):
        brace = masked.find("{", m.end())
        semi = masked.find(";", m.end())
        # `#[cfg(test)] mod tests;` — an out-of-line module, no body here.
        if brace == -1 or (semi != -1 and semi < brace):
            continue
        depth, j = 0, brace
        while j < len(masked):
            if masked[j] == "{":
                depth += 1
            elif masked[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        regions.append((m.start(), j))
    return regions


def macro_arg(text: str, masked: str, open_pos: int) -> str | None:
    """Source between the macro's delimiters, or None if unbalanced."""
    opener = masked[open_pos]
    closer = OPENERS[opener]
    depth = 0
    for i in range(open_pos, len(masked)):
        if masked[i] == opener:
            depth += 1
        elif masked[i] == closer:
            depth -= 1
            if depth == 0:
                return " ".join(text[open_pos + 1 : i].split())
    return None


def line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def workspace_members(repo_root: Path) -> list[Path]:
    """Every workspace member directory, from the root manifest.

    Read from `members` rather than globbed as `crates/*`: a member added
    outside `crates/` (an `xtask/`, or a nested path) is exactly the case a
    hardcoded glob would silently skip.
    """
    manifest = tomllib.loads((repo_root / "Cargo.toml").read_text())
    members: list[Path] = []
    for pattern in manifest.get("workspace", {}).get("members", []):
        if any(ch in pattern for ch in "*?["):
            members.extend(sorted(p for p in repo_root.glob(pattern) if p.is_dir()))
        else:
            members.append(repo_root / pattern)
    return members


def is_publishable(crate_root: Path) -> bool:
    manifest = tomllib.loads((crate_root / "Cargo.toml").read_text())
    return manifest.get("package", {}).get("publish") is not False


def tracked_files(repo_root: Path) -> set[Path]:
    out = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "-z"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return {(repo_root / p).resolve() for p in out.split("\0") if p}


def scan_source(
    src_path: Path, crate_root: Path, repo_root: Path, tracked: set[Path]
) -> tuple[list[str], list[tuple[str, str]]]:
    """Returns (errors, warnings); warnings are (rel_src, rel_target) pairs."""
    text = src_path.read_text(encoding="utf-8", errors="replace")
    masked = mask(text)
    regions = cfg_test_regions(masked)
    errors: list[str] = []
    warnings: list[tuple[str, str]] = []
    rel_src = src_path.relative_to(repo_root).as_posix()

    def report(offset: int, target: Path | None, message: str | None = None) -> None:
        where = f"{rel_src}:{line_of(text, offset)}"
        is_test = any(a <= offset < b for a, b in regions)
        if message is not None:
            errors.append(f"{where}: {message}")
            return
        assert target is not None
        try:
            rel_target = target.relative_to(repo_root).as_posix()
        except ValueError:
            rel_target = str(target)
        if target.is_relative_to(crate_root.resolve()):
            if target.resolve() in tracked:
                return
            detail = f"embeds {rel_target}, which is inside the crate but not tracked by git"
        else:
            detail = f"embeds {rel_target}, outside {crate_root.relative_to(repo_root)}/"
        if is_test:
            warnings.append((rel_src, rel_target))
        else:
            errors.append(f"{where}: {detail}")

    for m in MACRO.finditer(masked):
        open_pos = m.end() - 1
        arg = macro_arg(text, masked, open_pos)
        if arg is None:
            report(m.start(), None, "unbalanced delimiters after include*!")
        elif lit := LITERAL.match(arg):
            report(m.start(), (src_path.parent / lit.group(1)).resolve())
        elif md := MANIFEST_DIR.match(arg):
            report(m.start(), (crate_root / md.group(1).lstrip("/")).resolve())
        else:
            report(
                m.start(),
                None,
                f"cannot statically resolve the embedded path: {arg}"
                " — use a plain string literal",
            )

    for m in PATH_ATTR_START.finditer(masked):
        attr = PATH_ATTR.match(text, m.start())
        if attr is None:
            report(
                m.start(),
                None,
                "cannot statically resolve the #[path] attribute"
                " — use a plain string literal",
            )
            continue
        report(m.start(), (src_path.parent / attr.group(1)).resolve())

    return errors, warnings


def main() -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    tracked = tracked_files(repo_root)

    errors: list[str] = []
    warnings: list[tuple[str, str]] = []
    crates_checked = 0

    for crate_root in workspace_members(repo_root):
        if not (crate_root / "Cargo.toml").is_file():
            errors.append(f"workspace member {crate_root} has no Cargo.toml")
            continue
        if not is_publishable(crate_root):
            continue
        crates_checked += 1
        # Tracked files only: an untracked stray .rs is not published and would
        # otherwise produce local-only failures CI never reproduces.
        for src_path in sorted(
            p for p in tracked if p.suffix == ".rs" and p.is_relative_to(crate_root.resolve())
        ):
            e, w = scan_source(src_path, crate_root, repo_root, tracked)
            errors.extend(e)
            warnings.extend(w)

    # A guard that inspects nothing must not report success. check-deployment-
    # mirror.sh floor-checks its own inputs for the same reason.
    if crates_checked == 0:
        print(
            "error: no publishable workspace members found — this check inspected nothing",
            file=sys.stderr,
        )
        return 1

    seen = {src: target for src, target in warnings}
    unlisted = {s: t for s, t in seen.items() if KNOWN_TEST_ONLY.get(s) != t}
    stale = {s: t for s, t in KNOWN_TEST_ONLY.items() if seen.get(s) != t}

    for src, target in sorted(unlisted.items()):
        errors.append(
            f"{src}: test-only embed of {target} is not in KNOWN_TEST_ONLY.\n"
            "    It does not block publishing (the verify build does not enable\n"
            "    cfg(test)), but the file is absent from the .crate so that test\n"
            "    cannot run from it. Move the fixture into the crate, or add the\n"
            "    entry to check_package_embeds.py if that is genuinely accepted."
        )
    for src, target in sorted(stale.items()):
        errors.append(
            f"{src}: KNOWN_TEST_ONLY lists an embed of {target} that no longer "
            "exists — remove the entry."
        )

    if errors:
        print(
            "error: publishable crates embed files they do not ship:\n", file=sys.stderr
        )
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nA published .crate contains only its own package directory, and only\n"
            "the files git tracks. Move the file into the crate — and if another\n"
            "directory owns it, mirror it under a drift check (see\n"
            "crates/cli/deployments/ and check-deployment-mirror.sh).",
            file=sys.stderr,
        )
        return 1

    print(
        f"package embeds OK ({crates_checked} publishable crate(s), "
        f"{len(seen)} accepted test-only embed(s))"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
