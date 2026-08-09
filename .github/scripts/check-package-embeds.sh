#!/usr/bin/env bash
# Fails if a publishable crate embeds a file from outside its own package.
#
# `include_str!` / `include_bytes!` resolve against the source tree, so a path
# reaching the workspace root builds perfectly in-tree and then fails for every
# `cargo install` user, because a published .crate contains only its own package
# directory. The failure surfaces at publish time — after a signed release
# exists and after the crates it depends on are already on crates.io, where a
# version can never be replaced.
#
# `cargo publish --workspace --dry-run` catches this properly by verify-building
# each packaged crate, but it costs a full rebuild of the workspace. That runs in
# release.yml, before the draft is created. This is the cheap PR-time signal that
# stops the mistake from ever reaching a tag.
#
# Sites under `#[cfg(test)]` are reported but do not fail: the verify build does
# not enable `cfg(test)`, so they do not block publishing. They are still real —
# the file is absent from the .crate, so the shipped crate's own test suite
# cannot run — just not release-blocking.
#
# Crates with `publish = false` are exempt: they are never packaged, so reaching
# out to workspace fixtures is legitimate for them.
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

python3 - <<'PY'
import pathlib, re, sys

MACRO = re.compile(r'\binclude_(?:str|bytes)!\s*\(')
# A plain "..." or r"..." literal. Deliberately narrow: anything this does not
# match is reported as unresolvable rather than assumed fine, so a construction
# that hides the path cannot slip through.
LITERAL = re.compile(r'^r?"([^"\\]*)"$')
MANIFEST_DIR = re.compile(
    r'^concat!\s*\(\s*env!\s*\(\s*"CARGO_MANIFEST_DIR"\s*\)\s*,\s*r?"([^"\\]*)"\s*,?\s*\)$'
)
CFG_TEST = re.compile(r'#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]')


def mask(src: str) -> str:
    """`src` with comments and literals blanked out, offsets preserved.

    Brace counting and macro-site detection both run on this, so a `{` inside a
    string or a `//` comment cannot throw off the structure, and an
    `include_str!` mentioned in a doc comment is not mistaken for a real one.
    """
    out = list(src)
    i, n = 0, len(src)

    def blank(start, end):
        for j in range(start, min(end, n)):
            if out[j] != '\n':
                out[j] = ' '

    while i < n:
        two = src[i:i + 2]
        if two == '//':
            end = src.find('\n', i)
            end = n if end == -1 else end
            blank(i, end)
            i = end
        elif two == '/*':
            depth, j = 1, i + 2          # Rust block comments nest
            while j < n and depth:
                if src[j:j + 2] == '/*':
                    depth, j = depth + 1, j + 2
                elif src[j:j + 2] == '*/':
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif m := re.match(r'(?:b|r|br|rb)?(#*)"', src[i:i + 8]):
            hashes = m.group(1)
            body = i + m.end()
            if 'r' in m.group(0):
                close = src.find('"' + hashes, body)
                j = n if close == -1 else close + 1 + len(hashes)
            else:
                j = body
                while j < n:
                    if src[j] == '\\':
                        j += 2
                        continue
                    if src[j] == '"':
                        j += 1
                        break
                    j += 1
            blank(i, j)
            i = j
        elif src[i] == "'" and re.match(r"'(?:\\.|[^\\'])'", src[i:i + 4]):
            j = i + len(re.match(r"'(?:\\.|[^\\'])'", src[i:i + 4]).group(0))
            blank(i, j)
            i = j
        else:
            i += 1
    return ''.join(out)


def cfg_test_regions(masked: str) -> list[tuple[int, int]]:
    """Byte spans of every `#[cfg(test)]`-annotated braced item."""
    regions = []
    for m in CFG_TEST.finditer(masked):
        brace = masked.find('{', m.end())
        semi = masked.find(';', m.end())
        # `#[cfg(test)] mod tests;` — an out-of-line module, no body here.
        if brace == -1 or (semi != -1 and semi < brace):
            continue
        depth, j = 0, brace
        while j < len(masked):
            if masked[j] == '{':
                depth += 1
            elif masked[j] == '}':
                depth -= 1
                if depth == 0:
                    break
            j += 1
        regions.append((m.start(), j))
    return regions


def macro_arg(text: str, masked: str, open_paren: int) -> str | None:
    """Source between the macro's parentheses, or None if unbalanced."""
    depth = 0
    for i in range(open_paren, len(masked)):
        if masked[i] == '(':
            depth += 1
        elif masked[i] == ')':
            depth -= 1
            if depth == 0:
                return ' '.join(text[open_paren + 1:i].split())
    return None


errors, warnings = [], []
checked = 0

for manifest in sorted(pathlib.Path('crates').glob('*/Cargo.toml')):
    crate_root = manifest.parent
    if re.search(r'^\s*publish\s*=\s*false', manifest.read_text(), re.M):
        continue

    for src_path in sorted(crate_root.rglob('*.rs')):
        text = src_path.read_text(encoding='utf-8', errors='replace')
        masked = mask(text)
        regions = cfg_test_regions(masked)

        for m in MACRO.finditer(masked):
            checked += 1
            where = f"{src_path}:{text.count(chr(10), 0, m.start()) + 1}"
            is_test = any(a <= m.start() < b for a, b in regions)
            bucket = warnings if is_test else errors

            arg = macro_arg(text, masked, m.end() - 1)
            if arg is None:
                bucket.append(f"{where}: unbalanced parentheses after include_*!")
                continue

            if lit := LITERAL.match(arg):
                target = (src_path.parent / lit.group(1)).resolve()
            elif md := MANIFEST_DIR.match(arg):
                target = (crate_root / md.group(1).lstrip('/')).resolve()
            else:
                bucket.append(
                    f"{where}: cannot statically resolve the embedded path: {arg}"
                    " — use a plain string literal"
                )
                continue

            if not target.is_relative_to(crate_root.resolve()):
                try:
                    shown = target.relative_to(pathlib.Path.cwd())
                except ValueError:
                    shown = target
                bucket.append(f"{where}: embeds {shown}, outside {crate_root}/")

if warnings:
    print("warning: test-only embeds reach outside their package. These do not",
          file=sys.stderr)
    print("block publishing (the verify build does not enable cfg(test)), but the",
          file=sys.stderr)
    print("file is absent from the .crate, so those tests cannot run from it:\n",
          file=sys.stderr)
    for w in warnings:
        print(f"  {w}", file=sys.stderr)
    print(file=sys.stderr)

if errors:
    print("error: publishable crates embed files from outside their package:\n",
          file=sys.stderr)
    for e in errors:
        print(f"  {e}", file=sys.stderr)
    print(
        "\nA published .crate contains only its own package directory, so these\n"
        "build in-tree and fail for every `cargo install` user. Move the file\n"
        "into the crate — and if another directory owns it, mirror it under a\n"
        "drift check (see crates/cli/deployments/ and check-deployment-mirror.sh).",
        file=sys.stderr,
    )
    sys.exit(1)

print(f"package embeds OK ({checked} site(s) checked, {len(warnings)} test-only warning(s))")
PY
