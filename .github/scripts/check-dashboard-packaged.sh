#!/usr/bin/env bash
# Fail unless the decdn-cli .crate carries the dashboard's Vite build.
#
# `dashboard/dist/` is gitignored, so it reaches the .crate only through the
# `include` list in crates/cli/Cargo.toml and only when it was built first. A
# .crate without it still builds, and embeds the fallback page that says how
# to build the dashboard: every `cargo install` user would get that page.
# Run after the dashboard build (.github/actions/dashboard-build).
set -euo pipefail

list=$(cargo package -p decdn-cli --list --locked --allow-dirty)
missing=0
for file in index.html app.js app.css; do
  if ! grep -qx "dashboard/dist/$file" <<<"$list"; then
    echo "::error::the decdn-cli .crate does not carry dashboard/dist/$file;" \
         "build the dashboard first (pnpm -C crates/cli/dashboard build)"
    missing=1
  fi
done
[ "$missing" -eq 0 ] && echo "decdn-cli packages the dashboard build"
exit "$missing"
