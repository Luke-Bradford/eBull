#!/usr/bin/env bash
#
# #2695 — every revert-probe anchor still matches its source exactly once,
# and every harness can still be STARTED.
#
# Why this is a push gate and the probes themselves are not: a revert probe
# injects a defect by replacing a VERBATIM copy of a source line, so it decays
# the moment that line moves — a rename, a dedent, `ruff format` alone. The
# harness detects its own stale anchor and reports *** BAD ANCHOR ***, but only
# on a day somebody runs it, and nothing runs them: a probe script is not a
# test, CI does not collect it (by design — it mutates tracked source on disk),
# and this hook does not either. Meanwhile the CLAIM derived from a run
# ("28 probes, all CAUGHT") is written into a PR description or a spec's
# acceptance section and read forever after as a property of the code.
#
# Measured on 5faeaeb6 (2026-08-14): 14 of 284 anchors were dead, NINE of them
# in probe_2240_result_model.py — the M9 Tier 1 promotion gate — so its refusals
# for carry, trial count, Deflated Sharpe and the basis allowlist had no working
# probe while the gate's coverage claim stood unchanged.
#
# A full probe sweep costs ~50s/probe (two pytest subprocesses each), near two
# hours for the 285 below. This is the part of that sweep needing no pytest at
# all, so it runs on every push. It does NOT establish that a mutation still
# fails its test — only a real run settles that.
#
# Exits 1 on any dead anchor or unlaunchable harness, printing every finding.
#
# ⚠⚠ MUST NOT RUN WHILE A PROBE SWEEP IS RUNNING. A harness mutates the source
# it probes for the duration of one pytest run and restores it after, so an
# audit landing in that window counts anchors against INJECTED source and
# reports dead anchors that are not dead. Same window as the standing "must not
# run concurrently with verify_2240_*" rule the harnesses already carry. If this
# gate fails and `pgrep -f probe_2240` shows a live run, that is the cause.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> check_probe_anchors: revert-probe anchors + harness launchability (#2695)"

# ⚠ NOT piped. `cmd | tail` returns tail's status, which is always 0 — the
# repo's standing gotcha, re-committed twice. Let it print in full so `set -e`
# sees the real exit code.
uv run python -m scripts.audit_probe_anchors
