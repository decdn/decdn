#!/usr/bin/env bash
# Thin wrapper around check_package_embeds.py — see that file for the rationale.
#
# The logic lives in a .py rather than a heredoc so it can be imported and unit
# tested (.github/scripts/tests/test_check_package_embeds.py). Its masking and
# cfg(test) region tracking are the most intricate thing in the release
# tooling, and a bug there degrades to a quiet false pass, which is the exact
# failure mode this check exists to prevent.
set -euo pipefail

command -v python3 >/dev/null || {
  echo "error: python3 not found on PATH; it is required to parse the crate sources" >&2
  exit 1
}

exec python3 "$(dirname "${BASH_SOURCE[0]}")/check_package_embeds.py" "$@"
