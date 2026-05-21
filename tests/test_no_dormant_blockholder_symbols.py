"""PR11 #1233 invariant G backstop — dormant 13D/G entrypoints stay deleted.

Phase 8 of PR11 retired the legacy seed-walking ingester entrypoints
once the manifest-worker discovery + parser path took over end-to-end.
The shell lint at ``scripts/check_13dg_retention.sh`` (invariant G)
already enforces this at the script layer, but it greps a fixed
``BLOCKHOLDER_PATHS`` list. A pure-Python pytest backstop:

  * Runs inside the normal pytest gate (no shell pre-push wiring drift).
  * Surfaces in CI even if the shell lint is skipped / reordered.
  * Pins the SCOPING decision explicitly: the symbol names are LIVE in
    other modules (13F-HR / N-CEN / scheduler / seed scripts) under
    different domain semantics. The dormant retirement is
    MODULE-LOCAL to the blockholders subsystem.

Codex 1b HIGH lesson: a lint-as-test that greps for forbidden symbols
MUST exclude itself from the search — otherwise the test's own
``FORBIDDEN`` list trips the lint. The ``:!<path>`` pathspec on
``git grep`` is the canonical mechanism (one negative entry, file
self-exclusion, no false positives if the test is later renamed
because the path appears explicitly here).

Allow-list semantics: any double-backtick-wrapped historical mention
(e.g. ``ingest_filer_blockholders`` inside an RST docstring) is OK —
those are retirement notes, not live references. Bare module-scope
references would fail the test.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Paths scoped MODULE-LOCALLY to the blockholders subsystem. The same
# symbol names are LIVE in:
#   - app/services/institutional_holdings.py (13F-HR variant)
#   - app/services/ncen_classifier.py        (N-CEN variant)
#   - app/workers/scheduler.py               (scheduler dispatch)
#   - scripts/seed_top_13f_filers.py         (13F seed script)
# Scoping the grep to blockholder modules avoids cross-domain false
# positives.
BLOCKHOLDER_PATHS: tuple[str, ...] = (
    "app/services/blockholders.py",
    "app/services/manifest_parsers/sec_13dg.py",
    "app/services/sec_13dg_discovery.py",
    "tests/test_manifest_parser_sec_13dg.py",
    "tests/test_sec_13dg_discovery.py",
    "tests/test_blockholders_ingester.py",
    "tests/test_schedule13_adapter.py",
    "tests/test_rewash_blockholders_cap.py",
    "tests/test_ownership_observations_sync_blockholders_cap.py",
    "tests/test_refresh_blockholders_current_uncapped.py",
    "tests/test_blockholders_retention_helpers.py",
)

# Dormant entrypoints retired in PR11 Phase 8. Resurrection in any of
# the BLOCKHOLDER_PATHS above is forbidden — the manifest-worker path
# (sec_13dg_discovery → manifest worker → sec_13dg parser) replaces
# them end-to-end.
FORBIDDEN: tuple[str, ...] = (
    "ingest_all_active_filers",
    "ingest_filer_blockholders",
    "_list_active_filer_seeds",
    "seed_filer",
)

# Self-exclusion pathspec — this test file references each FORBIDDEN
# symbol by string literal, which would otherwise self-trip the lint.
# ``:!<path>`` is the git-grep pathspec negation form.
_SELF_EXCLUDE = ":!tests/test_no_dormant_blockholder_symbols.py"


def _git_grep_symbol(symbol: str) -> list[str]:
    """Return ``git grep`` hits (``path:line:text``) for ``symbol`` as a
    whole word inside BLOCKHOLDER_PATHS, with this file excluded."""
    proc = subprocess.run(
        [
            "git",
            "grep",
            "-n",
            "-w",
            symbol,
            "--",
            *BLOCKHOLDER_PATHS,
            _SELF_EXCLUDE,
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )
    # ``git grep`` exits 1 when there are no matches; treat that as
    # success (empty hit list). Other non-zero exits indicate a real
    # error (e.g. malformed pathspec) — surface them.
    if proc.returncode not in (0, 1):
        raise RuntimeError(
            f"git grep failed for symbol={symbol!r}: "
            f"rc={proc.returncode} stderr={proc.stderr!r}"
        )
    return [line for line in proc.stdout.splitlines() if line]


def _is_allowed_mention(symbol: str, line: str) -> bool:
    """A line is an allowed historical mention iff the symbol appears
    wrapped in double-backticks (RST docstring convention) — e.g.
    ``ingest_filer_blockholders``. Bare references are NOT allowed."""
    return f"``{symbol}``" in line


def test_dormant_blockholder_symbols_stay_deleted() -> None:
    """PR11 invariant G backstop: every FORBIDDEN symbol either does
    not appear in BLOCKHOLDER_PATHS at all, or appears only inside
    double-backtick historical retirement notes."""
    offenders: list[str] = []
    for symbol in FORBIDDEN:
        for hit in _git_grep_symbol(symbol):
            # ``path:line:text`` — strip ``path:line:`` for the
            # allow-list check (the file path itself could
            # accidentally contain ``symbol`` text, but the grep is
            # already whole-word + path-scoped).
            try:
                _path, _line_no, text = hit.split(":", 2)
            except ValueError:
                # Defensive — surface as offender if the format is
                # unexpected. Should not happen with ``-n``.
                offenders.append(hit)
                continue
            if _is_allowed_mention(symbol, text):
                continue
            offenders.append(hit)

    assert not offenders, (
        "PR11 (#1233) Phase 8 deleted these dormant 13D/G entrypoints; "
        "resurrection detected in blockholder modules:\n  "
        + "\n  ".join(offenders)
        + "\n\nIf the resurrection is intentional (e.g. a new live "
        "implementation under the same name), update FORBIDDEN in "
        "tests/test_no_dormant_blockholder_symbols.py with a written "
        "rationale and re-run."
    )
