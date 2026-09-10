#!/usr/bin/env bash
# Thin wrapper around check_workspace_manifests.py — see that file for the rationale.
#
# The logic lives in a .py rather than a heredoc so it can be imported and unit
# tested (.github/scripts/tests/test_check_workspace_manifests.py). A bug in a guard like this
# degrades to a quiet false pass, which is the exact failure mode it exists to
# prevent.
set -euo pipefail

command -v python3 >/dev/null || {
  echo "error: python3 not found on PATH; it is required to run this check" >&2
  exit 1
}

exec python3 "$(dirname "${BASH_SOURCE[0]}")/check_workspace_manifests.py" "$@"
