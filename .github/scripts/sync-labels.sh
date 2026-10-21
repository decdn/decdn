#!/usr/bin/env bash
# Thin wrapper around sync_labels.py — see that file for the rationale.
#
# The logic lives in a .py rather than a heredoc so it can be imported and unit
# tested (.github/scripts/tests/test_sync_labels.py). Dry run by default;
# `--apply` changes the live labels through `gh`.
set -euo pipefail

command -v python3 >/dev/null || {
  echo "error: python3 not found on PATH; it is required to run this script" >&2
  exit 1
}
command -v gh >/dev/null || {
  echo "error: gh not found on PATH; it is required to read and edit labels" >&2
  exit 1
}

exec python3 "$(dirname "${BASH_SOURCE[0]}")/sync_labels.py" "$@"
