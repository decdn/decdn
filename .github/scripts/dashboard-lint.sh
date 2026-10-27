#!/usr/bin/env bash
# The dashboard-lint pre-commit hook: eslint and prettier over
# crates/cli/dashboard, with pnpm from PATH or corepack (pnpm.sh). With the
# app's dependencies not installed it skips, and says so: `pre-commit run
# --all-files` runs every hook, and a contributor who never touches the app
# needs no Node. CI's `dashboard` job lints every change to it.
set -euo pipefail

dir=crates/cli/dashboard
if [ ! -d "$dir/node_modules" ]; then
  echo "dashboard-lint: skipped, the dashboard's dependencies are not installed" \
       "(pnpm -C $dir install --frozen-lockfile); CI lints it" >&2
  exit 0
fi
exec "$(dirname "$0")/pnpm.sh" -C "$dir" lint
