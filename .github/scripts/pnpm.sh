#!/usr/bin/env bash
# Run pnpm with these arguments: from PATH, else through corepack, which
# Node 24 ships, so `corepack enable` is not a precondition. Used by the
# dashboard-lint hook.
set -euo pipefail

if command -v pnpm >/dev/null; then
  exec pnpm "$@"
fi
if command -v corepack >/dev/null; then
  exec corepack pnpm "$@"
fi
echo "pnpm not found: install Node 24, then run \`corepack enable\`" >&2
exit 127
