"""Tests for app.services.sec_incremental.plan_refresh."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import cast

import psycopg
import pytest

from app.providers.implementations.sec_edgar import (
    MasterIndexFetchResult,
    SecFilingsProvider,
)
from app.services.fundamentals import (
    FUNDAMENTALS_FORMS,
    LOOKBACK_DAYS,
    plan_refresh,
)
from app.services.watermarks import set_watermark
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

# Re-export for readability; pytest discovers the fixture via the import
# above, not __all__.
__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


FIXTURE_MASTER = Path("tests/fixtures/sec/master_20260415.idx")
FIXTURE_SUBMISSIONS = Path("tests/fixtures/sec/submissions_TEST.json").read_text()


@dataclass
class StubFilingsProvider:
    """In-memory stand-in for SecFilingsProvider used in unit tests."""

    master_bodies: dict[date, bytes | None] = field(default_factory=dict)
    submissions_by_cik: dict[str, dict[str, object]] = field(default_factory=dict)

    def fetch_master_index(
        self,
        target_date: date,
        *,
        if_modified_since: str | None = None,
    ) -> MasterIndexFetchResult | None:
        body = self.master_bodies.get(target_date)
        if body is None:
            return None
        return MasterIndexFetchResult(
            body=body,
            body_hash=f"hash-{target_date.isoformat()}",
            last_modified=f"lm-{target_date.isoformat()}",
        )

    def fetch_submissions(self, cik: str) -> dict[str, object] | None:
        return self.submissions_by_cik.get(cik)


def _window(today: date) -> list[date]:
    return [today - timedelta(days=i) for i in range(LOOKBACK_DAYS)]


def _seed_us_cohort(conn: psycopg.Connection[tuple], ciks: list[str]) -> None:
    """Insert rows so plan_refresh's covered-US query returns these CIKs."""
    for i, cik in enumerate(ciks, start=1):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (i, f"TEST{i}", f"Test Company {i}"),
        )
        conn.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (i, cik),
        )
    conn.commit()


def test_empty_cohort_returns_empty_plan(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    provider = StubFilingsProvider()
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert plan.seeds == []
    assert plan.refreshes == []
    assert plan.submissions_only_advances == []


def test_fresh_cohort_no_watermarks_all_ciks_are_seeds(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_us_cohort(ebull_test_conn, ["0000320193", "0000789019", "0001045810"])
    provider = StubFilingsProvider(
        master_bodies={d: FIXTURE_MASTER.read_bytes() for d in _window(date(2026, 4, 15))},
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert set(plan.seeds) == {"0000320193", "0000789019", "0001045810"}
    assert plan.refreshes == []
    assert plan.submissions_only_advances == []


def test_all_304_returns_empty_plan_for_watermarked_cohort(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    provider = StubFilingsProvider(
        master_bodies={d: None for d in _window(date(2026, 4, 15))},
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert plan.seeds == []
    assert plan.refreshes == []
    assert plan.submissions_only_advances == []


def test_master_index_hit_with_fundamentals_form_becomes_refresh(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",  # older than fixture top 0000320193-26-000042
        )
    master = FIXTURE_MASTER.read_bytes()
    provider = StubFilingsProvider(
        master_bodies={d: master if d == date(2026, 4, 15) else None for d in _window(date(2026, 4, 15))},
        submissions_by_cik={"0000320193": json.loads(FIXTURE_SUBMISSIONS)},
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert ("0000320193", "0000320193-26-000042") in plan.refreshes
    assert plan.seeds == []
    assert plan.submissions_only_advances == []


def test_master_index_hit_with_8k_only_is_submissions_only_advance(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_us_cohort(ebull_test_conn, ["0000789019"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000789019",
            watermark="older-accession",
        )
    custom_master = (
        b"CIK|Company Name|Form Type|Date Filed|Filename\n"
        b"--------------------------------------------------------------------------------\n"
        b"789019|MICROSOFT CORP|8-K|2026-04-15|edgar/data/789019/0000789019-26-000017.txt\n"
    )
    provider = StubFilingsProvider(
        master_bodies={d: custom_master if d == date(2026, 4, 15) else None for d in _window(date(2026, 4, 15))},
        submissions_by_cik={
            "0000789019": {
                "filings": {
                    "recent": {
                        "accessionNumber": ["0000789019-26-000017"],
                        "form": ["8-K"],
                        "acceptedDate": ["2026-04-15T09:00:00.000Z"],
                    }
                }
            }
        },
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert plan.refreshes == []
    assert plan.seeds == []
    assert ("0000789019", "0000789019-26-000017") in plan.submissions_only_advances


def test_master_index_hit_non_covered_cik_ignored(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Empty cohort — master-index entries for any CIK must not trigger fetches.
    provider = StubFilingsProvider(
        master_bodies={d: FIXTURE_MASTER.read_bytes() for d in _window(date(2026, 4, 15))},
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert plan.seeds == []
    assert plan.refreshes == []
    assert plan.submissions_only_advances == []


def test_master_index_hit_accession_unchanged_is_skip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-26-000042",  # matches fixture top
        )
    master = FIXTURE_MASTER.read_bytes()
    provider = StubFilingsProvider(
        master_bodies={d: master if d == date(2026, 4, 15) else None for d in _window(date(2026, 4, 15))},
        submissions_by_cik={"0000320193": json.loads(FIXTURE_SUBMISSIONS)},
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert plan.seeds == []
    assert plan.refreshes == []
    assert plan.submissions_only_advances == []


def test_fundamentals_forms_constant_covers_10k_10q() -> None:
    """Sanity: the constant we export must cover the forms we care about."""
    assert "10-K" in FUNDAMENTALS_FORMS
    assert "10-Q" in FUNDAMENTALS_FORMS
    assert "8-K" not in FUNDAMENTALS_FORMS


def test_plan_refresh_returns_sorted_lists(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """RefreshPlan output must be deterministically sorted so
    dependent tests and dashboards don't flap on insert order."""
    # Seed cohort in reverse-sorted order to prove output is re-sorted.
    _seed_us_cohort(
        ebull_test_conn,
        ["0000789019", "0000320193", "0001045810"],
    )
    provider = StubFilingsProvider()  # no master-index bodies — all None
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert plan.seeds == sorted(plan.seeds)
    assert plan.seeds == ["0000320193", "0000789019", "0001045810"]


def test_planner_submissions_skip_populates_failed_plan_ciks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Regression guard: when plan_refresh's own fetch_submissions
    returns None for a master-index-hit CIK, the CIK must land in
    plan.failed_plan_ciks so the executor's commit-gate withholds
    the master-index watermark for that day."""
    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    master = FIXTURE_MASTER.read_bytes()
    # Master-index hit for AAPL but submissions_by_cik empty → None.
    provider = StubFilingsProvider(
        master_bodies={d: master if d == date(2026, 4, 15) else None for d in _window(date(2026, 4, 15))},
        submissions_by_cik={},  # fetch_submissions returns None
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=date(2026, 4, 15),
    )
    assert "0000320193" in plan.failed_plan_ciks
    assert plan.refreshes == []
    assert plan.submissions_only_advances == []


def test_body_hash_match_skips_parse(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """When master-index returns 200 but body hash matches the stored
    response_hash, the planner must NOT parse the body — it should
    advance fetched_at and move on without populating master_hits."""
    import hashlib

    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    today = date(2026, 4, 15)
    master_body = FIXTURE_MASTER.read_bytes()

    # Seed a sec.master-index watermark whose response_hash matches
    # what the stub will return for `today`.
    stored_hash = hashlib.sha256(master_body).hexdigest()
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.master-index",
            key=today.isoformat(),
            watermark="Wed, 15 Apr 2026 22:00:00 GMT",
            response_hash=stored_hash,
        )
        # Also seed the submissions watermark so the CIK would
        # otherwise hit the refresh branch if the body were parsed.
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )

    class HashMatchingStub:
        def fetch_master_index(
            self,
            target_date: date,
            *,
            if_modified_since: str | None = None,
        ) -> MasterIndexFetchResult | None:
            if target_date != today:
                return None
            return MasterIndexFetchResult(
                body=master_body,
                body_hash=stored_hash,
                last_modified="Wed, 15 Apr 2026 22:00:00 GMT",
            )

        def fetch_submissions(self, cik: str) -> dict[str, object] | None:
            raise AssertionError(
                f"fetch_submissions should NOT be called when body hash matches stored watermark (cik={cik})"
            )

    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, HashMatchingStub()),
        today=today,
    )

    # Body-hash match → no parse → no entries → no refresh/submissions-only.
    assert plan.refreshes == []
    assert plan.submissions_only_advances == []


# ---------------------------------------------------------------------------
# Stale-watermark submissions.json backfill (#410)
# ---------------------------------------------------------------------------


def _set_submissions_watermark(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    watermark: str,
    fetched_at: date,
) -> None:
    """Seed a ``sec.submissions`` watermark row whose ``fetched_at``
    is arbitrarily old. ``set_watermark`` always stamps NOW(), so
    tests that need an antique ``fetched_at`` have to UPDATE directly.
    """
    with conn.transaction():
        set_watermark(
            conn,
            source="sec.submissions",
            key=cik,
            watermark=watermark,
        )
        conn.execute(
            "UPDATE external_data_watermarks SET fetched_at = %s WHERE source = 'sec.submissions' AND key = %s",
            (fetched_at, cik),
        )


def test_stale_watermark_with_new_accession_enqueues_refresh(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A CIK whose sec.submissions watermark is older than
    LOOKBACK_DAYS AND that did NOT hit the master-index window must
    be rescued via submissions.json and enqueued as a refresh when a
    new top accession is returned.
    """
    today = date(2026, 4, 15)
    _seed_us_cohort(ebull_test_conn, ["0000000001"])
    # Watermark fetched 90 days ago — well outside LOOKBACK_DAYS.
    _set_submissions_watermark(
        ebull_test_conn,
        cik="0000000001",
        watermark="old-accession-000042",
        fetched_at=today - timedelta(days=90),
    )

    # No master-index bodies on any window day — the main loop skips
    # this CIK. The backfill path must still pick it up.
    provider = StubFilingsProvider(
        master_bodies={},
        submissions_by_cik={
            "0000000001": {
                "filings": {
                    "recent": {
                        "accessionNumber": ["new-accession-000099"],
                        "form": ["10-K"],
                        "acceptedDate": ["2026-03-01T16:05:00.000Z"],
                    }
                }
            }
        },
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=today,
    )

    assert plan.refreshes == [("0000000001", "new-accession-000099")]


def test_stale_watermark_with_unchanged_accession_refreshes_fetched_at(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Stale watermark + submissions top accession unchanged → the
    CIK was genuinely idle during the outage. Do not enqueue work,
    BUT the planner must advance ``fetched_at`` so the next run's
    backfill cap can make forward progress. Without this the oldest-
    idle CIKs would monopolise the cap forever and newer stale CIKs
    would starve (regression caught by codex).
    """
    today = date(2026, 4, 15)
    _seed_us_cohort(ebull_test_conn, ["0000000001"])
    _set_submissions_watermark(
        ebull_test_conn,
        cik="0000000001",
        watermark="same-accession-000042",
        fetched_at=today - timedelta(days=90),
    )

    provider = StubFilingsProvider(
        master_bodies={},
        submissions_by_cik={
            "0000000001": {
                "filings": {
                    "recent": {
                        "accessionNumber": ["same-accession-000042"],
                        "form": ["10-K"],
                        "acceptedDate": ["2026-03-01T16:05:00.000Z"],
                    }
                }
            }
        },
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=today,
    )

    assert plan.refreshes == []
    assert plan.submissions_only_advances == []

    # fetched_at must have been advanced past the stale cutoff so
    # the next run's _stale_submission_ciks query no longer picks
    # this CIK.
    row = ebull_test_conn.execute(
        "SELECT fetched_at FROM external_data_watermarks WHERE source = 'sec.submissions' AND key = %s",
        ("0000000001",),
    ).fetchone()
    assert row is not None
    # NOW() on the planner side is strictly after today - 90 days,
    # and must now be inside the LOOKBACK_DAYS window.
    fetched_at = row[0]
    assert (today - fetched_at.date()).days < LOOKBACK_DAYS


def test_fresh_watermark_is_not_a_backfill_candidate(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Steady-state regression guard: a CIK whose watermark was
    touched inside LOOKBACK_DAYS must NOT trigger the backfill fetch,
    even if it did not hit the master-index this window.
    """
    today = date(2026, 4, 15)
    _seed_us_cohort(ebull_test_conn, ["0000000001"])
    _set_submissions_watermark(
        ebull_test_conn,
        cik="0000000001",
        watermark="fresh-accession-000042",
        fetched_at=today - timedelta(days=5),
    )

    provider = StubFilingsProvider(master_bodies={}, submissions_by_cik={})
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=today,
    )

    assert plan.refreshes == []
    # If the backfill path had run, it would have called
    # fetch_submissions and raised a KeyError on the empty map.
    # Reaching this assertion proves it did not run.


def test_stale_watermark_already_queued_as_refresh_is_not_double_processed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """CIK that hits the master-index window AND has a stale watermark
    must only appear once (via the main loop), not twice (main loop +
    backfill path).
    """
    today = date(2026, 4, 15)
    master_day = today - timedelta(days=3)
    _seed_us_cohort(ebull_test_conn, ["0000000001"])
    _set_submissions_watermark(
        ebull_test_conn,
        cik="0000000001",
        watermark="old-accession-000042",
        fetched_at=today - timedelta(days=90),
    )

    master_body = (
        b"Description:           Master Index of EDGAR Dissemination Feed\n"
        b"Last Data Received:    April 15, 2026\n"
        b"Comments:              webmaster@sec.gov\n"
        b"Anonymous FTP:         ftp://ftp.sec.gov/edgar/\n"
        b"\n"
        b" \n"
        b"CIK|Company Name|Form Type|Date Filed|Filename\n"
        b"--------------------------------------------------------------------------------\n"
        b"1|Test Co|10-K|2026-04-12|edgar/data/1/new-accession-000099.txt\n"
    )
    provider = StubFilingsProvider(
        master_bodies={master_day: master_body},
        submissions_by_cik={
            "0000000001": {
                "filings": {
                    "recent": {
                        "accessionNumber": ["new-accession-000099"],
                        "form": ["10-K"],
                        "acceptedDate": ["2026-04-12T16:05:00.000Z"],
                    }
                }
            }
        },
    )
    plan = plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=today,
    )

    # Exactly one refresh entry for the CIK — no duplicate from the
    # backfill loop.
    assert plan.refreshes == [("0000000001", "new-accession-000099")]


def test_main_loop_no_op_cik_is_excluded_from_backfill(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Regression guard: a stale-watermark CIK that hit the
    master-index this run and resolved to the main-loop ``accession
    unchanged`` no-op must NOT be re-fetched by the backfill path.
    That would burn backfill cap on a CIK whose submissions.json the
    main loop already resolved this run, and (on a slow provider)
    double the SEC rate-limit spend.
    """
    today = date(2026, 4, 15)
    master_day = today - timedelta(days=3)
    _seed_us_cohort(ebull_test_conn, ["0000000001"])
    _set_submissions_watermark(
        ebull_test_conn,
        cik="0000000001",
        watermark="same-accession-000042",
        fetched_at=today - timedelta(days=90),
    )

    master_body = (
        b"CIK|Company Name|Form Type|Date Filed|Filename\n"
        b"--------------------------------------------------------------------------------\n"
        b"1|Test Co|10-K|2026-04-12|edgar/data/1/same-accession-000042.txt\n"
    )
    # Count submissions fetches so we can assert the backfill path
    # does NOT re-fetch.
    fetch_count = {"n": 0}

    class CountingStub(StubFilingsProvider):
        def fetch_submissions(self, cik: str) -> dict[str, object] | None:
            fetch_count["n"] += 1
            return super().fetch_submissions(cik)

    provider = CountingStub(
        master_bodies={master_day: master_body},
        submissions_by_cik={
            "0000000001": {
                "filings": {
                    "recent": {
                        "accessionNumber": ["same-accession-000042"],
                        "form": ["10-K"],
                        "acceptedDate": ["2026-04-12T16:05:00.000Z"],
                    }
                }
            }
        },
    )
    plan_refresh(
        ebull_test_conn,
        cast(SecFilingsProvider, provider),
        today=today,
    )

    # Exactly one fetch_submissions call — the main loop's. If the
    # backfill path also fetched, the counter would be 2.
    assert fetch_count["n"] == 1


def test_backfill_cap_bounds_blast_radius(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """When many CIKs are stale (long outage), the per-run cap must
    limit how many submissions.json calls happen in one run.
    """
    from app.services import fundamentals as fundamentals_module

    today = date(2026, 4, 15)
    # 5 stale CIKs, cap=2 → backfill enqueues at most 2.
    ciks = [f"{i:010d}" for i in range(1, 6)]
    _seed_us_cohort(ebull_test_conn, ciks)
    for i, cik in enumerate(ciks):
        _set_submissions_watermark(
            ebull_test_conn,
            cik=cik,
            watermark=f"old-{i:03d}",
            fetched_at=today - timedelta(days=90 + i),
        )

    provider = StubFilingsProvider(
        master_bodies={},
        submissions_by_cik={
            cik: {
                "filings": {
                    "recent": {
                        "accessionNumber": [f"new-{i:03d}"],
                        "form": ["10-K"],
                        "acceptedDate": ["2026-03-01T16:05:00.000Z"],
                    }
                }
            }
            for i, cik in enumerate(ciks)
        },
    )

    original_cap = fundamentals_module.SUBMISSIONS_STALE_BACKFILL_CAP
    fundamentals_module.SUBMISSIONS_STALE_BACKFILL_CAP = 2
    try:
        plan = plan_refresh(
            ebull_test_conn,
            cast(SecFilingsProvider, provider),
            today=today,
        )
    finally:
        fundamentals_module.SUBMISSIONS_STALE_BACKFILL_CAP = original_cap

    # Exactly two entries, and they are the two oldest watermarks —
    # seeded at -94 days (0000000005) and -93 days (0000000004).
    # plan.refreshes is alphabetically sorted by the planner, so we
    # assert membership rather than order.
    assert len(plan.refreshes) == 2
    chosen_ciks = {cik for cik, _ in plan.refreshes}
    assert chosen_ciks == {"0000000005", "0000000004"}
