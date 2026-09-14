"""#3040 — the coverage reconciliation, against a real backend.

Separate module from ``test_research_quarantine_refresh.py`` on purpose: the
``db`` marker is applied per MODULE, so mixing these in would evict the pure
work-condition tests from the ``-m "not db"`` push gate.

⚠ These need a real Postgres because the invariant under test is a property of
the QUERY PLAN, not of the Python. ``uncovered_series_count`` has to count a
series with no coverage row at all, and that is precisely the row a negated
LEFT JOIN drops: every comparison against a missing row is UNKNOWN, and
``NOT UNKNOWN`` is UNKNOWN. A mocked connection would assert the decision while
the real statement returned the opposite (prevention log, #2647/#2650).
"""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg
import pytest

from app.services.price_quarantine import RULE_SET_VERSION
from app.services.research_corpus_ingest import (
    HF_ARCHIVE,
    ArchiveProvenance,
    uncovered_series_count,
)

_OTHER = ArchiveProvenance(
    vendor="test/3040-other-archive",
    upstream_source="unknown",
    licence="test-fixture",
    adjustment_basis="unadjusted",
    quarantine_as_of=date(2020, 1, 1),
)


def _seed_series(
    conn: psycopg.Connection[Any],
    *,
    vendor: str,
    tag: str,
    first_bar: date = date(2001, 1, 1),
    last_bar: date = date(2001, 1, 10),
) -> int:
    row = conn.execute(
        """
        INSERT INTO research_price_series
            (vendor, vendor_symbol, upstream_source, licence, adjustment_basis,
             first_bar, last_bar, bar_count)
        VALUES (%s, %s, 'unknown', 'test-fixture', 'unadjusted', %s, %s, 10)
        RETURNING series_id
        """,
        (vendor, f"T3040-{tag}", first_bar, last_bar),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_coverage(
    conn: psycopg.Connection[Any],
    series_id: int,
    *,
    quarantine_as_of: date,
    rule_set_version: str = RULE_SET_VERSION,
    first_bar: date = date(2001, 1, 1),
    last_bar: date = date(2001, 1, 10),
) -> None:
    conn.execute(
        """
        INSERT INTO research_price_quarantine_coverage
            (series_id, rule_set_version, quarantine_as_of, first_bar, last_bar,
             bars_evaluated, transitions_evaluated)
        VALUES (%s, %s, %s, %s, %s, 10, 9)
        """,
        (series_id, rule_set_version, quarantine_as_of, first_bar, last_bar),
    )


def test_a_series_with_no_coverage_row_is_counted(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The case three-valued logic drops, and the only one that matters here.

    A corpus that has NEVER been evaluated at the live rule set is exactly the
    state a version bump creates, and it is the state the masked reader turns
    into zero bars.
    """
    series_id = _seed_series(ebull_test_conn, vendor=HF_ARCHIVE.vendor, tag="nocov")
    assert uncovered_series_count(ebull_test_conn, HF_ARCHIVE) == 1

    _seed_coverage(ebull_test_conn, series_id, quarantine_as_of=HF_ARCHIVE.quarantine_as_of)
    assert uncovered_series_count(ebull_test_conn, HF_ARCHIVE) == 0


def test_a_matching_version_with_a_divergent_as_of_is_still_uncovered(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The hole the ``quarantine_as_of`` column exists to close.

    Before it, ``--as-of 2020-01-01`` wrote different ``provisional`` verdicts —
    and therefore different T3 corroboration — under an unchanged rule-set
    version, and no check anywhere could see the divergence.
    """
    series_id = _seed_series(ebull_test_conn, vendor=HF_ARCHIVE.vendor, tag="asof")
    _seed_coverage(ebull_test_conn, series_id, quarantine_as_of=date(2020, 1, 1))
    assert uncovered_series_count(ebull_test_conn, HF_ARCHIVE) == 1


def test_a_stale_rule_set_version_is_uncovered(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    series_id = _seed_series(ebull_test_conn, vendor=HF_ARCHIVE.vendor, tag="version")
    _seed_coverage(
        ebull_test_conn,
        series_id,
        quarantine_as_of=HF_ARCHIVE.quarantine_as_of,
        rule_set_version="price-quarantine-v1+deadbeefcafe",
    )
    assert uncovered_series_count(ebull_test_conn, HF_ARCHIVE) == 1


def test_a_coverage_row_that_does_not_span_the_series_is_uncovered(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A corpus that grew past its last evaluation is not covered by it."""
    series_id = _seed_series(ebull_test_conn, vendor=HF_ARCHIVE.vendor, tag="span")
    _seed_coverage(
        ebull_test_conn,
        series_id,
        quarantine_as_of=HF_ARCHIVE.quarantine_as_of,
        last_bar=date(2001, 1, 2),
    )
    assert uncovered_series_count(ebull_test_conn, HF_ARCHIVE) == 1


def test_the_count_is_per_archive(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """A complete Intrader must not mask a stale HF, and vice versa."""
    _seed_series(ebull_test_conn, vendor=HF_ARCHIVE.vendor, tag="scope")
    assert uncovered_series_count(ebull_test_conn, _OTHER) == 0
    assert uncovered_series_count(ebull_test_conn, HF_ARCHIVE) == 1


def test_a_series_with_no_bars_loaded_is_not_counted(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``bar_count IS NULL`` means the series was registered but never loaded.

    There is nothing to quarantine, so it must not hold the whole vendor
    permanently off-policy — which would make the job re-run the full corpus on
    every walk forever.
    """
    ebull_test_conn.execute(
        """
        INSERT INTO research_price_series
            (vendor, vendor_symbol, upstream_source, licence, adjustment_basis)
        VALUES (%s, %s, 'unknown', 'test-fixture', 'unadjusted')
        """,
        (HF_ARCHIVE.vendor, "T3040-unloaded"),
    )
    assert uncovered_series_count(ebull_test_conn, HF_ARCHIVE) == 0


@pytest.mark.parametrize("as_of", [date(2026, 7, 14), date(2024, 9, 27)])
def test_the_declared_as_of_round_trips_through_the_column(
    ebull_test_conn: psycopg.Connection[Any], as_of: date
) -> None:
    """The column stores a date, not a string, and compares as one."""
    series_id = _seed_series(ebull_test_conn, vendor=_OTHER.vendor, tag=f"roundtrip-{as_of.isoformat()}")
    _seed_coverage(ebull_test_conn, series_id, quarantine_as_of=as_of)
    archive = ArchiveProvenance(
        vendor=_OTHER.vendor,
        upstream_source="unknown",
        licence="test-fixture",
        adjustment_basis="unadjusted",
        quarantine_as_of=as_of,
    )
    assert uncovered_series_count(ebull_test_conn, archive) == 0
