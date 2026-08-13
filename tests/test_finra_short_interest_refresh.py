"""Tests for ``run_finra_short_interest_refresh`` — G6/#915.

ScheduledJob-level integration against ``ebull_test_conn``. Covers:

* ``_settlement_dates_to_fetch`` weekday-aware behaviour (Sat/Sun
  candidate dates walk back to Friday).
* ``_walk_back_to_weekday`` unit.
* ``_compute_targets`` revision-window inclusion (two most-recent
  candidates always re-fetched, even if manifest-parsed).
* Happy path: fake provider returns the pristine fixture → 5 panel
  rows land in observations + _current + manifest.
* FinraNotFound on one target → benign skip; other targets processed.
* Fetch 5xx → per-file failed; ``RuntimeError`` raised at end.
* Empty file (0 bytes) → per-file failed; no store_raw attempted.
* Match-rate < 50% → WARNING logger captured.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
import pytest

from app.jobs.finra_short_interest_refresh import (
    _MAX_ANCHOR_WALKBACK_DAYS,
    FinraRefreshStats,
    _compute_targets,
    _fetch_designated_file,
    _is_disseminated,
    _settlement_dates_to_fetch,
    _walk_back_to_weekday,
    run_finra_short_interest_refresh,
)
from app.providers.implementations.finra_short_interest import FinraNotFound

_PRISTINE = Path("tests/fixtures/finra/shrt20260430_sample.csv")


# ----------------------------------------------------------------------
# Seed helpers
# ----------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, instrument_id: int, symbol: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
            "VALUES (%s, %s, %s, TRUE) ON CONFLICT (instrument_id) DO NOTHING",
            (instrument_id, symbol, symbol),
        )


def _seed_panel(conn: psycopg.Connection[tuple]) -> None:
    _seed_instrument(conn, instrument_id=1001, symbol="AAPL")
    _seed_instrument(conn, instrument_id=1002, symbol="GME")
    _seed_instrument(conn, instrument_id=1003, symbol="MSFT")
    _seed_instrument(conn, instrument_id=1004, symbol="JPM")
    _seed_instrument(conn, instrument_id=1005, symbol="HD")


def _seed_parsed_finra_manifest(conn: psycopg.Connection[tuple], settlement_date: date) -> None:
    """Seed a `parsed` finra_short_interest manifest row for the
    given settlement date — simulates a prior successful ingest.
    """
    accession = f"FINRA_SI_{settlement_date.strftime('%Y%m%d')}"
    filed_at = datetime.combine(settlement_date, datetime.min.time(), tzinfo=UTC)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_filing_manifest (
                accession_number, cik, form, source,
                subject_type, subject_id, instrument_id,
                filed_at, primary_document_url,
                ingest_status, parser_version, raw_status,
                last_attempted_at
            ) VALUES (
                %s, 'FINRA_SI', 'SHRT', 'finra_short_interest',
                'finra_universe', 'FINRA_SI', NULL,
                %s, %s,
                'parsed', 'finra-si-bimonthly-v1', 'stored',
                NOW()
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                ingest_status = 'parsed',
                last_attempted_at = NOW()
            """,
            (
                accession,
                filed_at,
                f"https://cdn.finra.org/equity/otcmarket/biweekly/shrt{settlement_date.strftime('%Y%m%d')}.csv",
            ),
        )


# ----------------------------------------------------------------------
# Fake provider for test isolation
# ----------------------------------------------------------------------


class _FakeProvider:
    """Test-injection provider. Records every fetched date in
    ``calls`` so tests can assert revision-window re-fetch behaviour.
    """

    BASE = "https://cdn.finra.org/equity/otcmarket/biweekly/"

    def __init__(
        self,
        settlements: dict[date, bytes] | None = None,
        notfound: set[date] | None = None,
        errors: dict[date, Exception] | None = None,
    ) -> None:
        self._settlements = settlements or {}
        self._notfound = notfound or set()
        self._errors = errors or {}
        self.calls: list[date] = []

    def settlement_file_url(self, settlement_date: date) -> str:
        return f"{self.BASE}shrt{settlement_date.strftime('%Y%m%d')}.csv"

    def fetch_settlement_file(self, settlement_date: date) -> bytes:
        self.calls.append(settlement_date)
        if settlement_date in self._notfound:
            raise FinraNotFound(self.settlement_file_url(settlement_date))
        if settlement_date in self._errors:
            raise self._errors[settlement_date]
        if settlement_date in self._settlements:
            return self._settlements[settlement_date]
        # Default: return the pristine fixture rebadged to this date.
        return _rebadge_pristine(settlement_date)


def _rebadge_pristine(target: date) -> bytes:
    """Take the pristine 2026-04-30 fixture; replace settlement_date
    occurrences so it parses cleanly against ``target``."""
    raw = _PRISTINE.read_bytes()
    target_iso = target.strftime("%Y-%m-%d").encode()
    target_compact = target.strftime("%Y%m%d").encode()
    return raw.replace(b"2026-04-30", target_iso).replace(b"20260430", target_compact)


# ----------------------------------------------------------------------
# 1 — _walk_back_to_weekday
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_date, expected",
    [
        (date(2026, 2, 28), date(2026, 2, 27)),  # Sat → Fri
        (date(2026, 3, 15), date(2026, 3, 13)),  # Sun → Fri
        (date(2026, 5, 15), date(2026, 5, 15)),  # Fri unchanged
        (date(2026, 4, 30), date(2026, 4, 30)),  # Thu unchanged
        (date(2024, 2, 29), date(2024, 2, 29)),  # Thu (leap-year) unchanged
    ],
)
def test_walk_back_to_weekday(input_date: date, expected: date) -> None:
    assert _walk_back_to_weekday(input_date) == expected


# ----------------------------------------------------------------------
# 2 — _settlement_dates_to_fetch
# ----------------------------------------------------------------------


def test_settlement_dates_weekday_aware() -> None:
    """May 2026 EOM=Sun (May 31) → walk back to Fri May 29.
    May 15 is Fri (unchanged). Apr 15 + Apr 30 are both weekdays
    (Wed, Thu) — unchanged.
    """
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    dates = _settlement_dates_to_fetch(now, backfill_window_days=100)
    # We expect at least May 15 + Apr 30 + Apr 15 + Mar 31 in the
    # 100-day window from 2026-05-18.
    assert date(2026, 5, 15) in dates
    assert date(2026, 4, 30) in dates
    assert date(2026, 4, 15) in dates
    assert date(2026, 3, 31) in dates
    # Mar 15 (Sun) walks back to Mar 13 (Fri).
    assert date(2026, 3, 13) in dates
    assert date(2026, 3, 15) not in dates
    # Feb 28 (Sat) walks back to Feb 27 (Fri).
    assert date(2026, 2, 27) in dates
    assert date(2026, 2, 28) not in dates


def test_settlement_dates_excludes_future() -> None:
    """A target after ``now`` (e.g. May 29 when now=May 18) is excluded."""
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    dates = _settlement_dates_to_fetch(now, backfill_window_days=100)
    assert date(2026, 5, 29) not in dates


# ----------------------------------------------------------------------
# 3 — _compute_targets (revision window)
# ----------------------------------------------------------------------


def test_compute_targets_subtracts_parsed_excluding_revision_window() -> None:
    candidates = [
        date(2026, 3, 13),
        date(2026, 3, 31),
        date(2026, 4, 15),
        date(2026, 4, 30),
        date(2026, 5, 15),
    ]
    # All but the two most-recent are parsed.
    already_parsed = {
        date(2026, 3, 13),
        date(2026, 3, 31),
        date(2026, 4, 15),
        date(2026, 4, 30),
    }
    targets = _compute_targets(candidates, already_parsed)
    # Revision window = candidates[-2:] = {4/30, 5/15} — always re-fetched.
    # 5/15 wasn't parsed (so anyway included); 4/30 IS parsed but
    # the revision window forces re-fetch.
    assert date(2026, 5, 15) in targets
    assert date(2026, 4, 30) in targets
    # Earlier parsed dates remain excluded.
    assert date(2026, 4, 15) not in targets
    assert date(2026, 3, 31) not in targets
    assert date(2026, 3, 13) not in targets


def test_compute_targets_empty_candidates_returns_empty() -> None:
    assert _compute_targets([], set()) == []


def test_compute_targets_no_parsed_returns_all() -> None:
    candidates = [date(2026, 4, 15), date(2026, 4, 30)]
    assert _compute_targets(candidates, set()) == candidates


# ----------------------------------------------------------------------
# 4 — Happy path: fake provider returns fixture, observations + _current
#     + manifest all land
# ----------------------------------------------------------------------


def test_run_happy_path_writes_observations(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    provider = _FakeProvider()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    stats = run_finra_short_interest_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=30,
        provider=provider,  # type: ignore[arg-type]
    )

    # 30-day window from 2026-05-01 covers 2026-04-15 + 2026-04-30.
    # Provider returns the rebadged pristine for both. 5 panel
    # symbols × 2 settlement dates = 10 upserts.
    assert stats.total_upserted == 10
    assert stats.failed_files == 0

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM finra_short_interest_observations WHERE settlement_date = ANY(%s)",
            ([date(2026, 4, 15), date(2026, 4, 30)],),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 10

        cur.execute(
            "SELECT COUNT(*) FROM finra_short_interest_current WHERE instrument_id = ANY(%s)",
            ([1001, 1002, 1003, 1004, 1005],),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 5

        cur.execute(
            "SELECT COUNT(*) FROM sec_filing_manifest "
            "WHERE source = 'finra_short_interest' AND ingest_status = 'parsed'"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 2


# ----------------------------------------------------------------------
# 4b — Designated-calendar resolution (#2234)
# ----------------------------------------------------------------------
#
# FINRA DESIGNATES the settlement calendar (Rule 4560(a)) rather than
# publishing a formula, so ``_settlement_dates_to_fetch`` derives an
# ANCHOR and the fetch step resolves it. Good Friday 2022-04-15 is the
# live case: the CDN serves shrt20220414.csv, not shrt20220415.csv.


def test_fetch_designated_file_walks_back_over_a_holiday() -> None:
    provider = _FakeProvider(notfound={date(2022, 4, 15)})

    resolved = _fetch_designated_file(
        provider,  # type: ignore[arg-type]
        date(2022, 4, 15),
        allow_walkback=True,
    )

    assert resolved is not None
    designated, payload = resolved
    assert designated == date(2022, 4, 14)
    assert payload.startswith(b"accountingYearMonthNumber|")
    assert provider.calls == [date(2022, 4, 15), date(2022, 4, 14)]


def test_fetch_designated_file_without_walkback_probes_once() -> None:
    """The two most-recent anchors get a single probe.

    There a 403/404 means "not disseminated yet" — FINRA publishes about
    a week after the Rule 4560(a) due date — so walking back would spend
    a request per day rediscovering a file that does not exist.
    """
    provider = _FakeProvider(notfound={date(2026, 7, 31)})

    resolved = _fetch_designated_file(
        provider,  # type: ignore[arg-type]
        date(2026, 7, 31),
        allow_walkback=False,
    )

    assert resolved is None
    assert provider.calls == [date(2026, 7, 31)]


def test_fetch_designated_file_stops_at_already_ingested_date() -> None:
    """A holiday-shifted date already held is not re-downloaded each fire."""
    provider = _FakeProvider(notfound={date(2022, 4, 15)})

    resolved = _fetch_designated_file(
        provider,  # type: ignore[arg-type]
        date(2022, 4, 15),
        allow_walkback=True,
        skip_dates=frozenset({date(2022, 4, 14)}),
    )

    assert resolved is None
    assert provider.calls == [date(2022, 4, 15)]


def test_fetch_designated_file_bounded_walkback() -> None:
    """Nothing served in range → ``None`` after exactly bound+1 probes."""
    anchor = date(2022, 4, 15)
    provider = _FakeProvider(notfound={anchor - timedelta(days=n) for n in range(_MAX_ANCHOR_WALKBACK_DAYS + 3)})

    resolved = _fetch_designated_file(
        provider,  # type: ignore[arg-type]
        anchor,
        allow_walkback=True,
    )

    assert resolved is None
    assert len(provider.calls) == _MAX_ANCHOR_WALKBACK_DAYS + 1
    # The bound must not reach the adjacent half-month's designated date.
    assert min(provider.calls) > date(2022, 3, 31)


@pytest.mark.parametrize(
    "anchor, expected",
    [
        (date(2026, 5, 1), False),  # 0 days old — FINRA has not published
        (date(2026, 4, 17), False),  # 14 days — still inside the lag
        (date(2026, 4, 16), True),  # 15 days — publication window has passed
        (date(2026, 3, 31), True),
    ],
)
def test_is_disseminated_boundary(anchor: date, expected: bool) -> None:
    """A 403 only means "never designated" once FINRA has had time to publish.

    Rule 4560(a) puts the report due on the second business day after
    settlement; FINRA's published schedule runs publication about a week
    beyond that.
    """
    assert _is_disseminated(anchor, datetime(2026, 5, 1, 12, 0, tzinfo=UTC)) is expected


def test_run_revision_window_re_fetches_a_holiday_shifted_date(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A shifted date already ingested is STILL re-fetched while its anchor
    is in the revision window.

    Codex ckpt 2 (#2234): the designated date is never itself an anchor,
    so without dropping ``skip_dates`` inside the window it would land in
    the skip list forever and receive no in-place-revision coverage.
    """
    _seed_panel(ebull_test_conn)
    # 2024-01-15 (MLK) is designated 2024-01-12; seed that as parsed.
    _seed_parsed_finra_manifest(ebull_test_conn, date(2024, 1, 12))
    ebull_test_conn.commit()

    # The real MLK weekend: Sat 13th, Sun 14th, holiday Mon 15th — the
    # CDN serves none of them, so the walk-back has to reach the 12th.
    provider = _FakeProvider(notfound={date(2024, 1, 15), date(2024, 1, 14), date(2024, 1, 13)})
    # 2024-01-15 is then 16 days old — past the dissemination lag, so
    # walk-back is allowed — and still one of the two candidates, so it
    # is inside the revision window.
    now = datetime(2024, 1, 31, 12, 0, tzinfo=UTC)

    run_finra_short_interest_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=20,  # candidates = [2024-01-15, 2024-01-31]
        provider=provider,  # type: ignore[arg-type]
    )

    assert date(2024, 1, 12) in provider.calls, "shifted date must be re-fetched for revisions"


def test_run_holiday_anchor_stores_under_designated_date(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Job-level: observations land under the DESIGNATED date, not the anchor.

    Window is wide enough that the holiday anchor is outside the
    revision window, which is what enables walk-back.
    """
    _seed_panel(ebull_test_conn)
    provider = _FakeProvider(notfound={date(2022, 4, 15)})
    now = datetime(2022, 5, 20, 12, 0, tzinfo=UTC)

    stats = run_finra_short_interest_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=40,
        provider=provider,  # type: ignore[arg-type]
    )

    assert stats.failed_files == 0
    stored = {s.settlement_date for s in stats.settlement_files}
    assert date(2022, 4, 14) in stored
    assert date(2022, 4, 15) not in stored

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM finra_short_interest_observations WHERE settlement_date = %s",
            (date(2022, 4, 14),),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 5

        cur.execute(
            "SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number = %s",
            ("FINRA_SI_20220414",),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 1


# ----------------------------------------------------------------------
# 5 — FinraNotFound on one target → benign skip
# ----------------------------------------------------------------------


def test_run_skips_not_yet_published(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    provider = _FakeProvider(notfound={date(2026, 4, 30)})
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    stats = run_finra_short_interest_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=30,
        provider=provider,  # type: ignore[arg-type]
    )

    # 4/15 succeeds; 4/30 raises FinraNotFound → silently skipped, no
    # stats row.
    assert stats.failed_files == 0
    assert len(stats.settlement_files) == 1
    assert stats.settlement_files[0].settlement_date == date(2026, 4, 15)


# ----------------------------------------------------------------------
# 6 — 5xx → per-file failed; RuntimeError raised
# ----------------------------------------------------------------------


def test_run_fetch_5xx_records_failed_continues(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    provider = _FakeProvider(
        errors={
            date(2026, 4, 30): httpx.HTTPStatusError(
                "500", request=httpx.Request("GET", "x"), response=httpx.Response(500)
            )
        }
    )
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="files failed"):
        run_finra_short_interest_refresh(
            ebull_test_conn,
            now=now,
            backfill_window_days=30,
            provider=provider,  # type: ignore[arg-type]
        )


# ----------------------------------------------------------------------
# 7 — Empty file → per-file failed (no store_raw attempted)
# ----------------------------------------------------------------------


def test_run_empty_file_records_failed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    provider = _FakeProvider(settlements={date(2026, 4, 15): b"", date(2026, 4, 30): b""})
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="files failed"):
        run_finra_short_interest_refresh(
            ebull_test_conn,
            now=now,
            backfill_window_days=30,
            provider=provider,  # type: ignore[arg-type]
        )

    # store_raw should NOT have run — no filing_raw_documents rows for
    # the synthetic FINRA accessions.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM filing_raw_documents WHERE accession_number LIKE 'FINRA_SI_2026%'")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 0


# ----------------------------------------------------------------------
# 8 — Match-rate < 50% logs WARNING
# ----------------------------------------------------------------------


def test_run_match_rate_below_threshold_logs_warning(
    ebull_test_conn: psycopg.Connection[tuple], caplog: pytest.LogCaptureFixture
) -> None:
    # Seed only 1 of 5 panel symbols → match rate = 1/9 = 11% (well below 50%).
    _seed_instrument(ebull_test_conn, instrument_id=1001, symbol="AAPL")
    provider = _FakeProvider(settlements={date(2026, 4, 30): _PRISTINE.read_bytes()})
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    with caplog.at_level(logging.WARNING, logger="app.jobs.finra_short_interest_refresh"):
        run_finra_short_interest_refresh(
            ebull_test_conn,
            now=now,
            backfill_window_days=20,
            provider=provider,  # type: ignore[arg-type]
        )

    assert any("match rate" in rec.message and "below 50%" in rec.message for rec in caplog.records)


# ----------------------------------------------------------------------
# 9 — Revision window re-fetches two most-recent
# ----------------------------------------------------------------------


def test_run_revision_window_re_fetches_two_most_recent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Seed manifest as parsed for BOTH the two most-recent candidates;
    verify the fake provider still gets fetched for both.
    """
    _seed_panel(ebull_test_conn)
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    # Window covers 4/15 + 4/30; seed both as parsed.
    _seed_parsed_finra_manifest(ebull_test_conn, date(2026, 4, 15))
    _seed_parsed_finra_manifest(ebull_test_conn, date(2026, 4, 30))
    ebull_test_conn.commit()

    provider = _FakeProvider()
    run_finra_short_interest_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=30,
        provider=provider,  # type: ignore[arg-type]
    )

    assert set(provider.calls) == {date(2026, 4, 15), date(2026, 4, 30)}


# ----------------------------------------------------------------------
# 10 — FinraRefreshStats aggregates
# ----------------------------------------------------------------------


def test_stats_aggregates_total_upserted_failed_files() -> None:
    from app.services.finra_short_interest_ingest import SettlementIngestStats

    stats = FinraRefreshStats(
        settlement_files=[
            SettlementIngestStats(
                settlement_date=date(2026, 4, 15),
                rows_parsed=10,
                rows_resolved=5,
                rows_upserted=5,
            ),
            SettlementIngestStats(
                settlement_date=date(2026, 4, 30),
                failed=True,
                error_detail="oops",
            ),
        ]
    )
    assert stats.total_upserted == 5
    assert stats.total_parsed == 10
    assert stats.total_resolved == 5
    assert stats.failed_files == 1
