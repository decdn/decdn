#!/usr/bin/env bash
# The dashboard-lint pre-commit hook: eslint and prettier over
# crates/cli/dashboard, with pnpm from PATH or corepack (pnpm.sh).
set -euo pipefail

dir=crates/cli/dashboard
if [ ! -d "$dir/node_modules" ]; then
  echo "dashboard-lint: run \`pnpm -C $dir install --frozen-lockfile\` first" >&2
  exit 1
fi
exec "$(dirname "$0")/pnpm.sh" -C "$dir" lint
