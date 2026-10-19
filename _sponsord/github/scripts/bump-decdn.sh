#!/usr/bin/env bash
# Thin wrapper around bump_decdn.py — see that file for the subcommands.
#
# The logic lives in a .py rather than a heredoc so it can be imported and unit
# tested (.github/scripts/tests/test_bump_decdn.py).
set -euo pipefail

command -v python3 >/dev/null || {
  echo "error: python3 not found on PATH; it is required to run this script" >&2
  exit 1
}

exec python3 "$(dirname "${BASH_SOURCE[0]}")/bump_decdn.py" "$@"
