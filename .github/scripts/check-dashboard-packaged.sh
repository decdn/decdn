#!/usr/bin/env bash
# Run after the dashboard build (.github/actions/dashboard-build) and before
# any `cargo package` or `cargo publish`, which then take `--allow-dirty`.
#
# `dashboard/dist/` is gitignored and reaches decdn-cli's .crate only through
# the `include` list in crates/cli/Cargo.toml, so cargo counts it as an
# uncommitted change and refuses to package without `--allow-dirty`. That flag
# would let any other change through too, and an `include` list packages what
# is on disk, gitignored or not. So this proves three things:
#
# - git sees no change in the checkout;
# - decdn-cli's .crate carries the three files of the build: without them
#   every `cargo install` user would get the page that says how to build it;
# - it carries no other file that git does not track, beyond the ones cargo
#   writes itself.
set -euo pipefail

status=$(git status --porcelain) || {
  echo "::error::git status failed"
  exit 1
}
if [ -n "$status" ]; then
  echo "::error::the checkout has changes beyond the dashboard build:"
  echo "$status"
  exit 1
fi

list=$(cargo package -p decdn-cli --list --locked --allow-dirty)
failed=0
build=()
for file in index.html app.js app.css; do
  build+=("dashboard/dist/$file")
  if ! grep -qx "dashboard/dist/$file" <<<"$list"; then
    echo "::error::the decdn-cli .crate does not carry dashboard/dist/$file;" \
         "build the dashboard first (pnpm -C crates/cli/dashboard build)"
    failed=1
  fi
done

tracked=$(git ls-files -- crates/cli | sed 's#^crates/cli/##')
known=$(printf '%s\n' "$tracked" "${build[@]}" .cargo_vcs_info.json Cargo.lock Cargo.toml.orig | sort -u)
extra=$(comm -23 <(sort -u <<<"$list") <(printf '%s\n' "$known"))
if [ -n "$extra" ]; then
  echo "::error::the decdn-cli .crate would carry files git does not track:"
  echo "$extra"
  failed=1
fi
[ "$failed" -eq 0 ] && echo "decdn-cli packages the dashboard build, and the checkout is otherwise clean"
exit "$failed"
