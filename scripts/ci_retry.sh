#!/usr/bin/env bash
#
# #1747 — retry a flaky network command with linear backoff.
#
# Why: the CI Python toolchain bootstrap (`pip install uv`, `uv sync`)
# fetches wheels from PyPI/CDN with no retry. A transient fetch error
# (broken pipe, connection reset) fails the whole job and needs a manual
# `gh run rerun --failed`. Observed on PR #1744 (lxml download) and PR
# #1746 (httptools download) in consecutive sessions. Wrapping the fetch
# steps in this retry turns a one-in-N CDN hiccup into a non-event.
#
# Usage: scripts/ci_retry.sh <command> [args...]
# Env:
#   CI_RETRY_ATTEMPTS  total attempts before giving up (default 3)
#   CI_RETRY_DELAY     base backoff seconds; delay = DELAY * attempt# (default 5)
#
# On final failure the original command's exit status is propagated, so
# the CI step still fails red on a genuine (non-transient) error.
set -euo pipefail

attempts="${CI_RETRY_ATTEMPTS:-3}"
delay="${CI_RETRY_DELAY:-5}"

if [[ $# -eq 0 ]]; then
  echo "::error::ci_retry.sh requires a command to run" >&2
  exit 2
fi

# Validate the tunables up front: a non-integer would make the arithmetic
# below abort under `set -e` with a cryptic message (Codex #1747).
if ! [[ $attempts =~ ^[1-9][0-9]*$ ]]; then
  echo "::error::CI_RETRY_ATTEMPTS must be a positive integer (got '${attempts}')" >&2
  exit 2
fi
if ! [[ $delay =~ ^[0-9]+$ ]]; then
  echo "::error::CI_RETRY_DELAY must be a non-negative integer (got '${delay}')" >&2
  exit 2
fi

n=1
while true; do
  status=0
  "$@" || status=$?
  if [[ $status -eq 0 ]]; then
    exit 0
  fi
  if [[ $n -ge $attempts ]]; then
    echo "::error::command failed after ${attempts} attempts (exit ${status}): $*" >&2
    exit "$status"
  fi
  echo "::warning::attempt ${n}/${attempts} failed (exit ${status}); retrying in $((delay * n))s: $*" >&2
  sleep "$((delay * n))"
  n=$((n + 1))
done
