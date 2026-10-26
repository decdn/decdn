#!/usr/bin/env bash
# The dashboard-lint pre-commit hook: eslint and prettier over
# crates/cli/dashboard. pnpm is used from PATH, else through corepack (which
# Node 24 ships), so `corepack enable` is not a precondition.
set -euo pipefail

dir=crates/cli/dashboard
if command -v pnpm >/dev/null; then
  pnpm=(pnpm)
elif command -v corepack >/dev/null; then
  pnpm=(corepack pnpm)
else
  echo "dashboard-lint: needs pnpm 10 (Node 24 and \`corepack enable\`)" >&2
  exit 1
fi
if [ ! -d "$dir/node_modules" ]; then
  echo "dashboard-lint: run \`${pnpm[*]} -C $dir install --frozen-lockfile\` first" >&2
  exit 1
fi
exec "${pnpm[@]}" -C "$dir" lint
