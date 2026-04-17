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
from app.services.sec_incremental import (
    FUNDAMENTALS_FORMS,
    LOOKBACK_DAYS,
    RefreshPlan,
    plan_refresh,
)
from app.services.watermarks import set_watermark
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

# ``ebull_test_conn`` is imported into this module's namespace so pytest
# picks it up as a fixture during collection. ``test_db_available`` is
# aliased to a non-``test_*`` name so pytest does not mis-collect it as
# a test function.
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

    def _fetch_submissions(self, cik: str) -> dict[str, object] | None:
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
    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])


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
    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])


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
    assert "0000320193" in plan.refreshes
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
    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])


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
    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])


def test_fundamentals_forms_constant_covers_10k_10q() -> None:
    """Sanity: the constant we export must cover the forms we care about."""
    assert "10-K" in FUNDAMENTALS_FORMS
    assert "10-Q" in FUNDAMENTALS_FORMS
    assert "8-K" not in FUNDAMENTALS_FORMS
