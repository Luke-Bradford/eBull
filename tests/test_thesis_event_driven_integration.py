"""Integration tests for #273 — thesis event-driven trigger.

Extends ``find_stale_instruments`` with:
- event-based predicate: new 10-K / 10-Q / 8-K since latest thesis.
- ``filings_status = 'analysable'`` gate.
- optional ``tier=None`` + ``instrument_ids=[...]`` for cascade calls.

Real ``ebull_test`` DB required so the SQL aggregate + LATERAL work.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from app.services.thesis import StaleInstrument, find_stale_instruments
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    tier: int = 1,
    filings_status: str = "analysable",
    review_frequency: str = "weekly",
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, symbol, symbol),
    )
    conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier, review_frequency, filings_status) VALUES (%s, %s, %s, %s)",
        (instrument_id, tier, review_frequency, filings_status),
    )
    conn.commit()


def _seed_thesis(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    thesis_version: int = 1,
    created_at: datetime | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO theses (
            instrument_id, thesis_version, created_at,
            thesis_type, stance, memo_markdown
        ) VALUES (%s, %s, %s, 'compounder', 'buy', 'test memo')
        """,
        (instrument_id, thesis_version, created_at or datetime.now(UTC)),
    )
    conn.commit()


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    filing_date: date,
    filing_type: str,
    accession: str,
    created_at: datetime | None = None,
) -> None:
    """Insert a filing_events row.

    ``created_at`` defaults to NOW() (DB default). Tests that need to
    pin the ingest timestamp (e.g. to assert no event-trigger fires
    when a filing was already in the DB before the thesis ran) should
    pass this explicitly. find_stale_instruments's event trigger
    compares ``filing_events.created_at`` vs ``theses.created_at``, not
    filing_date.
    """
    if created_at is None:
        conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type,
                provider, provider_filing_id
            ) VALUES (%s, %s, %s, 'sec', %s)
            """,
            (instrument_id, filing_date, filing_type, accession),
        )
    else:
        conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type,
                provider, provider_filing_id, created_at
            ) VALUES (%s, %s, %s, 'sec', %s, %s)
            """,
            (instrument_id, filing_date, filing_type, accession, created_at),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Analysable gate
# ---------------------------------------------------------------------------


def test_non_analysable_excluded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Instruments with filings_status != 'analysable' never appear,
    regardless of thesis staleness."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="GOOD", filings_status="analysable")
    _seed_instrument(ebull_test_conn, instrument_id=2, symbol="BAD", filings_status="insufficient")

    result = find_stale_instruments(ebull_test_conn, tier=1)

    symbols = {r.symbol for r in result}
    assert "GOOD" in symbols
    assert "BAD" not in symbols


def test_fpi_excluded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """FPI instruments are excluded (v1 — UK-equivalent bar tracked in #279)."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="FPI", filings_status="fpi")
    result = find_stale_instruments(ebull_test_conn, tier=1)
    assert [r.symbol for r in result] == []


# ---------------------------------------------------------------------------
# Event-driven trigger
# ---------------------------------------------------------------------------


def test_new_10k_since_thesis_triggers_event_reason(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Thesis was fresh by cadence, but a new 10-K landed after it —
    must surface with reason='event_new_10k'."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    thesis_at = datetime.now(UTC) - timedelta(days=3)  # well within weekly window
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=thesis_at)
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today() - timedelta(days=1),  # newer than thesis
        filing_type="10-K",
        accession="0000320193-26-000001",
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)

    assert len(result) == 1
    assert result[0].instrument_id == 1
    assert result[0].reason == "event_new_10k"


def test_new_10q_triggers_event_new_10q(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=2))
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="10-Q",
        accession="0000320193-26-000002",
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)
    assert result[0].reason == "event_new_10q"


def test_new_8k_triggers_event_new_8k(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=2))
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="8-K",
        accession="0000320193-26-000003",
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)
    assert result[0].reason == "event_new_8k"


def test_amendment_triggers_same_reason_as_base(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """10-K/A amendment should trigger event_new_10k (not a separate reason)."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=2))
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="10-K/A",
        accession="0000320193-26-000004",
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)
    assert result[0].reason == "event_new_10k"


def test_old_filing_no_event_trigger(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Filing INGESTED (``created_at``) before the thesis must NOT
    trigger event-based staleness — the thesis had the filing
    available as input."""
    thesis_at = datetime.now(UTC) - timedelta(days=3)
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    # Filing ingested BEFORE the thesis ran (ingested at days=10, then
    # thesis at days=3). find_stale must not treat this as a new event.
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today() - timedelta(days=30),
        filing_type="10-K",
        accession="0000320193-25-000001",
        created_at=datetime.now(UTC) - timedelta(days=10),
    )
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=thesis_at)

    result = find_stale_instruments(ebull_test_conn, tier=1)
    # Within weekly window + filing ingested pre-thesis → fresh.
    assert result == []


def test_same_day_post_thesis_filing_triggers(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Thesis generated at 10:00, filing ingested same day at 15:00 →
    event trigger. Uses created_at timestamps (not filing_date). Codex
    flagged this gap — same-day filings with identical filing_date as
    thesis-day would've been missed under the prior date-only check."""
    now = datetime.now(UTC)
    thesis_at = now - timedelta(hours=5)  # today at ~10:00 if now=15:00
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=thesis_at)
    # Filing ingested at now (5h after thesis). Both have filing_date=today.
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="8-K",
        accession="SAMEDAY-1",
        created_at=now,
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)

    assert result[0].reason == "event_new_8k"


def test_backfilled_filing_with_old_filing_date_triggers(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A backfilled filing with a filing_date that predates the thesis
    but a created_at that postdates it MUST trigger — the thesis
    could not have incorporated the row because it wasn't in the DB
    yet. Codex-flagged second case."""
    thesis_at = datetime.now(UTC) - timedelta(days=1)
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=thesis_at)
    # Old filing_date but ingested just now via backfill.
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today() - timedelta(days=90),  # old filing_date
        filing_type="10-Q",
        accession="BACKFILLED-1",
        created_at=datetime.now(UTC),  # but freshly ingested
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)

    assert result[0].reason == "event_new_10q"


def test_non_fundamentals_filing_ignored(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Form types outside {10-K, 10-Q, 8-K} (+ amendments) do NOT
    trigger event-based refresh — 20-F is for #279."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=2))
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="20-F",
        accession="0000320193-26-000005",
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)
    # No event trigger; thesis is fresh by cadence → empty result.
    assert result == []


# ---------------------------------------------------------------------------
# Tier + instrument_ids parameters
# ---------------------------------------------------------------------------


def test_tier_none_plus_instrument_ids_bypasses_tier_filter(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Cascade caller: tier=None + instrument_ids=[...] should scope to
    those instruments across any tier."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="T3_A", tier=3)
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=2))
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="10-Q",
        accession="A-1",
    )
    _seed_instrument(ebull_test_conn, instrument_id=2, symbol="T3_B", tier=3)
    # Instrument 2 has no thesis → also surfaces under "no_thesis".

    result = find_stale_instruments(ebull_test_conn, tier=None, instrument_ids=[1, 2])

    by_id = {r.instrument_id: r for r in result}
    assert set(by_id) == {1, 2}
    assert by_id[1].reason == "event_new_10q"
    assert by_id[2].reason == "no_thesis"


def test_tier_none_without_instrument_ids_scans_all_analysable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """tier=None + no instrument_ids → every tier's analysable instruments."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="T1", tier=1)
    _seed_instrument(ebull_test_conn, instrument_id=2, symbol="T2", tier=2)
    _seed_instrument(ebull_test_conn, instrument_id=3, symbol="T3", tier=3)

    # All three have no thesis → all stale with reason="no_thesis".
    result = find_stale_instruments(ebull_test_conn, tier=None)

    symbols = {r.symbol for r in result}
    assert symbols == {"T1", "T2", "T3"}
    assert all(r.reason == "no_thesis" for r in result)


def test_instrument_ids_scopes_result(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """instrument_ids narrows the scan even if more instruments in the
    same tier are stale."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="A")
    _seed_instrument(ebull_test_conn, instrument_id=2, symbol="B")
    _seed_instrument(ebull_test_conn, instrument_id=3, symbol="C")
    # All have no thesis.

    result = find_stale_instruments(ebull_test_conn, tier=1, instrument_ids=[1, 3])

    ids = {r.instrument_id for r in result}
    assert ids == {1, 3}


# ---------------------------------------------------------------------------
# Existing reasons still work
# ---------------------------------------------------------------------------


def test_time_based_cadence_still_triggers_stale(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Old thesis, no new filings → still stale via time-based rule."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=30))

    result = find_stale_instruments(ebull_test_conn, tier=1)
    assert len(result) == 1
    assert result[0].reason == "stale"


def test_same_second_filings_use_tiebreak_deterministically(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR review BLOCKING #301 regression guard: if two filings have
    identical created_at, the LATERAL subquery's ORDER BY
    (created_at DESC, filing_event_id DESC) resolves tiebreaks
    deterministically. Latest inserted (highest filing_event_id) wins.
    Asserting the *form type* matches the tiebreaker catches the class
    of bug where an aggregate MAX disagrees with a correlated
    subquery's ORDER BY."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=2))
    same_ts = datetime.now(UTC)
    # Insert 10-K first, then 8-K — same timestamp. filing_event_id
    # is BIGSERIAL so 8-K gets the higher value and wins the tiebreak.
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="10-K",
        accession="TIE-1",
        created_at=same_ts,
    )
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="8-K",
        accession="TIE-2",
        created_at=same_ts,
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)

    # The reason must be the tiebreak-winner's form (8-K, higher id),
    # not the aggregate-MAX-winner's form. Both timestamps are equal
    # so without a single deterministic source of truth the reason
    # could report 10-K while the newest row is 8-K.
    assert result[0].reason == "event_new_8k"


def test_event_reason_takes_precedence_over_time_based(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """If both event AND time-based would fire, event reason wins
    (informative for operator dashboards)."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="AAPL")
    _seed_thesis(ebull_test_conn, instrument_id=1, created_at=datetime.now(UTC) - timedelta(days=30))
    _seed_filing(
        ebull_test_conn,
        instrument_id=1,
        filing_date=date.today(),
        filing_type="10-Q",
        accession="E-1",
    )

    result = find_stale_instruments(ebull_test_conn, tier=1)
    assert isinstance(result[0], StaleInstrument)
    assert result[0].reason == "event_new_10q"  # not "stale"
