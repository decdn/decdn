#!/usr/bin/env bash
# Fails if crates/cli/deployments/ has drifted from contracts/deployments/.
#
# `decdn config init --chain <name>` bakes contract addresses read from the
# deployment manifest the Foundry `DeployProtocol` script writes. The CLI embeds
# that manifest with `include_str!`, which cannot escape the package — a path
# outside the crate is unreachable from the published `.crate`, so
# `cargo install decdn-cli` would fail to build. Hence the mirror.
#
# contracts/deployments/ stays canonical (the deploy script writes it);
# crates/cli/deployments/ is a byte-identical copy that ships in the .crate.
# Without this check a redeploy would update one and leave the CLI baking stale
# addresses, which no test would catch.
#
# Only chains the CLI actually embeds are mirrored — local dev chain ids such as
# 31337 exist under contracts/deployments/ and are deliberately not mirrored, so
# this iterates the mirror rather than the canonical directory.
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

MIRROR_DIR="crates/cli/deployments"
CANONICAL_DIR="contracts/deployments"

# An unmatched glob expands to the literal pattern, which would sail past a
# naive loop as a single nonexistent path. Count first.
shopt -s nullglob
mirrored=("$MIRROR_DIR"/*.json)
shopt -u nullglob

if (( ${#mirrored[@]} == 0 )); then
  echo "error: $MIRROR_DIR contains no manifests." >&2
  echo "The CLI embeds at least one (see crates/cli/src/known_chains.rs)." >&2
  exit 1
fi

drift=0
for m in "${mirrored[@]}"; do
  canonical="$CANONICAL_DIR/$(basename "$m")"
  if [[ ! -f "$canonical" ]]; then
    echo "error: $m has no canonical source at $canonical" >&2
    drift=1
    continue
  fi
  if ! cmp -s "$canonical" "$m"; then
    echo "error: $m differs from $canonical" >&2
    diff -u "$canonical" "$m" >&2 || true
    drift=1
  fi
done

if (( drift )); then
  cat >&2 <<EOF

$CANONICAL_DIR is canonical — the deploy script writes it. Re-copy the manifest:

  cp $CANONICAL_DIR/<chainId>.json $MIRROR_DIR/<chainId>.json

Do not edit the mirror by hand; a redeploy would silently overwrite the fix.
EOF
  exit 1
fi

echo "deployment manifest mirror in sync (${#mirrored[@]} chain(s))"
