"""#1184 — morning_candidate_review source-registry coverage.

The composite orchestrator adapter ``refresh_scoring_and_recommendations``
acquires ``JobLock(database_url, JOB_MORNING_CANDIDATE_REVIEW)``
directly (not via ``_run_with_lock``). Without a source-registry entry
the acquisition KeyErrors at construction. This test pins the entry so
a future cleanup pass can't drop it silently.

Pre-#1184 the dormant KeyError was masked because the composite
adapter's upstream layers (``candles`` → ``daily_candle_refresh``,
``fundamentals`` → ``daily_research_refresh``) typically PREREQ_SKIPed
on partial-bootstrap dev DBs, so the scoring layer became DEP_SKIPPED
before the adapter was called.
"""

from __future__ import annotations

from app.jobs.sources import source_for


def test_morning_candidate_review_resolves_to_db_source() -> None:
    assert source_for("morning_candidate_review") == "db"
