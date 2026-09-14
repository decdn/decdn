#!/usr/bin/env bash
# Thin wrapper around cargo_deny_warn.py — see that file for the rationale.
#
# The logic lives in a .py rather than a heredoc so it can be imported and unit
# tested (.github/scripts/tests/test_cargo_deny_warn.py). A bug in the outcome
# classification degrades to a quiet false pass: a run that checked nothing
# reads as a clean audit.
set -euo pipefail

command -v python3 >/dev/null || {
  echo "error: python3 not found on PATH; it is required to run this check" >&2
  exit 1
}

exec python3 "$(dirname "${BASH_SOURCE[0]}")/cargo_deny_warn.py" "$@"
