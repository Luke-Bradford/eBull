"""#1221 — pure alignment guard: the postgres_health junk-window
constants must mirror the parser-side period sanity guard.

`app.services.postgres_health` deliberately duplicates the
`[_PERIOD_MIN, _PERIOD_MAX)` window from
`app.providers.implementations.sec_fundamentals` instead of importing
the provider module at runtime. This test is the single-source-of-truth
enforcement (same pattern as the DB_SIZE_WARN_BYTES ↔ pre-push-hook
alignment test in `tests/test_pre_push_hook_bloat_warn.py`).
"""

from __future__ import annotations

from app.providers.implementations import sec_fundamentals
from app.services import postgres_health


def test_junk_window_matches_parser_guard() -> None:
    assert postgres_health.FACTS_PERIOD_MIN == sec_fundamentals._PERIOD_MIN
    assert postgres_health.FACTS_PERIOD_MAX == sec_fundamentals._PERIOD_MAX
