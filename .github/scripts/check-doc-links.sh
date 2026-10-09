#!/usr/bin/env bash
# Fails if a doc comment under crates/ links to a repo file by relative path.
#
# Rustdoc copies a Markdown link target into the HTML unchanged, and the
# browser resolves it against the rendered page's URL. On docs.rs,
# `../../../adr/038-….md` resolves to `docs.rs/adr/038-….md`, which does not
# exist. Rustdoc's broken-link lint checks intra-doc links only, so it passes
# these. A link to a repo file uses the absolute GitHub URL:
#   https://github.com/decdn/decdn/blob/main/adr/038-….md
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

# Matches a target that starts with `./` or `../`, or a scheme-less `.md`
# target such as `](adr/038-….md)`. URLs carry a `:` and intra-doc paths
# carry no `.md`, so neither matches.
if git grep -nE '\]\((\./|\.\./|[^):#[:space:]]*\.md(#[^)]*)?\))' -- 'crates/*.rs'; then
  cat >&2 <<'EOF'

error: relative link in a doc comment. It breaks on docs.rs.
Use https://github.com/decdn/decdn/blob/main/<path> instead.
EOF
  exit 1
fi

echo "no relative file links in crate doc comments"
