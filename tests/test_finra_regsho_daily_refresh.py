"""Tests for ``run_finra_regsho_daily_refresh`` — G6/#916.

ScheduledJob-level integration. Mirrors test_finra_short_interest_refresh,
adapted for the (trade_date, prefix) cross-product + accession parsing.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import psycopg
import pytest

from app.jobs.finra_regsho_daily_refresh import (
    RegShoDailyRefreshStats,
    _compute_targets,
    _parse_accession,
    _trade_dates_to_fetch,
    run_finra_regsho_daily_refresh,
)
from app.providers.implementations.finra_regsho import PREFIXES
from app.providers.implementations.finra_short_interest import FinraNotFound

_PANEL = Path("tests/fixtures/finra/regsho/CNMS_panel_20260515.txt")
_EMPTY_FNRA = Path("tests/fixtures/finra/regsho/FNRA_empty_20260515.txt")


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


def _rebadge_panel(target: date, prefix: str) -> bytes:
    """Replace 20260515 → target's date in the panel fixture. CRLF
    preserved; binary replace.
    """
    raw = _PANEL.read_bytes()
    target_compact = target.strftime("%Y%m%d").encode()
    rebadged = raw.replace(b"20260515", target_compact)
    # CNMS panel rows have market 'B,Q,N'. For non-CNMS prefixes
    # facility is single-char; tests rewrite per-prefix when needed.
    if prefix != "CNMS":
        rebadged = rebadged.replace(b"B,Q,N", b"B")
    return rebadged


def _seed_parsed_manifest(conn: psycopg.Connection[tuple], trade_date: date, prefix: str) -> None:
    accession = f"FINRA_REGSHO_{prefix}_{trade_date.strftime('%Y%m%d')}"
    filed_at = datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC)
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
                %s, 'FINRA_REGSHO', 'REGSHO', 'finra_regsho_daily',
                'finra_universe', 'FINRA_REGSHO', NULL,
                %s, %s,
                'parsed', 'finra-regsho-daily-v1', 'stored',
                NOW()
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                ingest_status = 'parsed',
                last_attempted_at = NOW()
            """,
            (
                accession,
                filed_at,
                f"https://cdn.finra.org/equity/regsho/daily/{prefix}shvol{trade_date.strftime('%Y%m%d')}.txt",
            ),
        )


# ----------------------------------------------------------------------
# Fake provider
# ----------------------------------------------------------------------


class _FakeProvider:
    BASE = "https://cdn.finra.org/equity/regsho/daily/"

    def __init__(
        self,
        *,
        files: dict[tuple[date, str], bytes] | None = None,
        notfound: set[tuple[date, str]] | None = None,
        errors: dict[tuple[date, str], Exception] | None = None,
    ) -> None:
        self._files = files or {}
        self._notfound = notfound or set()
        self._errors = errors or {}
        self.calls: list[tuple[date, str]] = []

    def regsho_daily_url(self, trade_date: date, prefix: str) -> str:
        return f"{self.BASE}{prefix}shvol{trade_date.strftime('%Y%m%d')}.txt"

    def fetch_regsho_daily_file(self, trade_date: date, prefix: str) -> bytes:
        key = (trade_date, prefix)
        self.calls.append(key)
        if key in self._notfound:
            raise FinraNotFound(self.regsho_daily_url(trade_date, prefix))
        if key in self._errors:
            raise self._errors[key]
        if key in self._files:
            return self._files[key]
        # Default — rebadged panel.
        return _rebadge_panel(trade_date, prefix)


# ----------------------------------------------------------------------
# 1 — _parse_accession
# ----------------------------------------------------------------------


def test_parse_accession_clean() -> None:
    assert _parse_accession("FINRA_REGSHO_CNMS_20260515") == (date(2026, 5, 15), "CNMS")


def test_parse_accession_all_prefixes() -> None:
    for prefix in PREFIXES:
        assert _parse_accession(f"FINRA_REGSHO_{prefix}_20260515") == (date(2026, 5, 15), prefix)


def test_parse_accession_unknown_prefix_returns_none() -> None:
    assert _parse_accession("FINRA_REGSHO_XXXX_20260515") is None


def test_parse_accession_malformed_date_returns_none() -> None:
    assert _parse_accession("FINRA_REGSHO_CNMS_NOT_A_DATE") is None


def test_parse_accession_wrong_root_returns_none() -> None:
    """Sibling bimonthly accession FINRA_SI_YYYYMMDD must NOT match —
    avoids cross-source filter contamination."""
    assert _parse_accession("FINRA_SI_20260515") is None


def test_parse_accession_missing_underscore_returns_none() -> None:
    assert _parse_accession("FINRA_REGSHO_") is None
    assert _parse_accession("FINRA_REGSHO_CNMS") is None  # no date


# ----------------------------------------------------------------------
# 2 — _trade_dates_to_fetch
# ----------------------------------------------------------------------


def test_trade_dates_weekday_only() -> None:
    """now = Mon 2026-05-18; 7-day window covers Mon-back-to-prev-Tue.
    Saturday (5/16) + Sunday (5/17) must be filtered out.
    """
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    dates = _trade_dates_to_fetch(now, backfill_window_days=7)
    # All returned dates must be weekdays.
    for d in dates:
        assert d.weekday() < 5, f"{d} is weekend"
    # Sat 5/16 + Sun 5/17 excluded.
    assert date(2026, 5, 16) not in dates
    assert date(2026, 5, 17) not in dates
    # Mon 5/18 + Fri 5/15 included.
    assert date(2026, 5, 18) in dates
    assert date(2026, 5, 15) in dates


def test_trade_dates_30_day_window() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    dates = _trade_dates_to_fetch(now, backfill_window_days=30)
    # 30 calendar days contains ~22 trading days.
    assert 20 <= len(dates) <= 23


# ----------------------------------------------------------------------
# 3 — _compute_targets
# ----------------------------------------------------------------------


def test_compute_targets_revision_window_always_present() -> None:
    candidates = [date(2026, 5, 13), date(2026, 5, 14), date(2026, 5, 15)]
    # Mark every (date, prefix) pair as parsed.
    already = {(d, p) for d in candidates for p in PREFIXES}
    targets = _compute_targets(candidates, already)
    # Last 2 candidates × 6 prefixes = 12 entries always included.
    assert len(targets) == 12
    expected_dates = {date(2026, 5, 14), date(2026, 5, 15)}
    for td, _ in targets:
        assert td in expected_dates


def test_compute_targets_subtracts_parsed_pairs_outside_revision_window() -> None:
    candidates = [date(2026, 5, 13), date(2026, 5, 14), date(2026, 5, 15)]
    # Earliest date partly parsed; rest unparsed.
    already = {(date(2026, 5, 13), "CNMS"), (date(2026, 5, 13), "FNQC")}
    targets = _compute_targets(candidates, already)
    # 5/13 CNMS + FNQC are parsed AND outside revision window (last-2 =
    # 5/14, 5/15) → excluded. 5/13 FNRA/FNSQ/FNYX/FORF still unparsed →
    # included.
    assert (date(2026, 5, 13), "CNMS") not in targets
    assert (date(2026, 5, 13), "FNQC") not in targets
    assert (date(2026, 5, 13), "FNRA") in targets


def test_compute_targets_empty_candidates() -> None:
    assert _compute_targets([], set()) == []


def test_compute_targets_no_parsed_returns_full_cross_product() -> None:
    candidates = [date(2026, 5, 15)]
    targets = _compute_targets(candidates, set())
    assert len(targets) == 6
    assert {p for _, p in targets} == set(PREFIXES)


# ----------------------------------------------------------------------
# 4 — Happy path
# ----------------------------------------------------------------------


def test_run_happy_path_writes_observations(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    provider = _FakeProvider()
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)

    stats = run_finra_regsho_daily_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=3,
        provider=provider,  # type: ignore[arg-type]
    )

    # 3-day window from 2026-05-18 (Mon): weekdays = {5/15 Fri, 5/18 Mon};
    # 5/16 Sat + 5/17 Sun excluded. 2 dates × 6 prefixes = 12 fetches.
    # 5 panel symbols × 12 files = 60 upserts.
    assert len(provider.calls) == 12
    assert stats.total_upserted == 60
    assert stats.failed_files == 0
    assert len(stats.daily_files) == 12

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM finra_regsho_daily_observations WHERE trade_date >= %s",
            (date(2026, 5, 15),),
        )
        assert cur.fetchone() == (60,)


# ----------------------------------------------------------------------
# 5 — FinraNotFound → benign skip
# ----------------------------------------------------------------------


def test_run_skips_not_yet_published(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    # Mark all 6 CNMS-on-5/18 as not-found — file not yet published.
    notfound = {(date(2026, 5, 18), p) for p in PREFIXES}
    provider = _FakeProvider(notfound=notfound)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)

    stats = run_finra_regsho_daily_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=3,
        provider=provider,  # type: ignore[arg-type]
    )

    # 5/15 × 6 prefixes succeed; 5/18 × 6 prefixes silently skipped.
    assert stats.failed_files == 0
    assert len(stats.daily_files) == 6


# ----------------------------------------------------------------------
# 6 — Fetch 5xx → per-file failed; RuntimeError raised at end
# ----------------------------------------------------------------------


def test_run_fetch_5xx_records_failed_raises(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    provider = _FakeProvider(
        errors={
            (date(2026, 5, 18), "CNMS"): httpx.HTTPStatusError(
                "500", request=httpx.Request("GET", "x"), response=httpx.Response(500)
            )
        }
    )
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="files failed"):
        run_finra_regsho_daily_refresh(
            ebull_test_conn,
            now=now,
            backfill_window_days=3,
            provider=provider,  # type: ignore[arg-type]
        )


# ----------------------------------------------------------------------
# 7 — Empty file → per-file failed
# ----------------------------------------------------------------------


def test_run_empty_file_records_failed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    # All 12 fetches return 0 bytes.
    empties = {(d, p): b"" for d in (date(2026, 5, 15), date(2026, 5, 18)) for p in PREFIXES}
    provider = _FakeProvider(files=empties)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="files failed"):
        run_finra_regsho_daily_refresh(
            ebull_test_conn,
            now=now,
            backfill_window_days=3,
            provider=provider,  # type: ignore[arg-type]
        )

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM filing_raw_documents WHERE accession_number LIKE 'FINRA_REGSHO_%'")
        assert cur.fetchone() == (0,)


# ----------------------------------------------------------------------
# 8 — FNRA empty success path is NOT a failure
# ----------------------------------------------------------------------


def test_run_fnra_empty_body_is_success(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    # Only FNRA-on-5/15 returns empty body; everything else uses rebadged panel.
    files = {(date(2026, 5, 15), "FNRA"): _EMPTY_FNRA.read_bytes()}
    # Tell provider to NotFound the other 5/15 prefixes + all 5/18, so
    # the only file processed is the empty FNRA.
    notfound = {(date(2026, 5, 15), p) for p in PREFIXES if p != "FNRA"}
    notfound |= {(date(2026, 5, 18), p) for p in PREFIXES}
    provider = _FakeProvider(files=files, notfound=notfound)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)

    stats = run_finra_regsho_daily_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=3,
        provider=provider,  # type: ignore[arg-type]
    )

    assert stats.failed_files == 0
    assert len(stats.daily_files) == 1
    assert stats.daily_files[0].rows_parsed == 0
    assert stats.daily_files[0].rows_upserted == 0


# ----------------------------------------------------------------------
# 9 — Match-rate WARNING below 50%
# ----------------------------------------------------------------------


def test_match_rate_warning_logged_below_50pct(
    ebull_test_conn: psycopg.Connection[tuple], caplog: pytest.LogCaptureFixture
) -> None:
    # Seed only 1 panel symbol → match rate 1/5 = 20%.
    _seed_instrument(ebull_test_conn, instrument_id=1001, symbol="AAPL")
    # NotFound everything except a single 5/15 CNMS file.
    notfound = {(d, p) for d in (date(2026, 5, 15), date(2026, 5, 18)) for p in PREFIXES}
    notfound.discard((date(2026, 5, 15), "CNMS"))
    provider = _FakeProvider(notfound=notfound)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)

    with caplog.at_level(logging.WARNING, logger="app.jobs.finra_regsho_daily_refresh"):
        run_finra_regsho_daily_refresh(
            ebull_test_conn,
            now=now,
            backfill_window_days=3,
            provider=provider,  # type: ignore[arg-type]
        )

    assert any("match rate" in rec.message and "below 50%" in rec.message for rec in caplog.records)


def test_match_rate_no_warning_on_zero_parsed(
    ebull_test_conn: psycopg.Connection[tuple], caplog: pytest.LogCaptureFixture
) -> None:
    """All files empty (FNRA-shape) → total_parsed=0; no division-by-zero
    + no false WARNING.
    """
    _seed_panel(ebull_test_conn)
    files = {(d, p): _EMPTY_FNRA.read_bytes() for d in (date(2026, 5, 15), date(2026, 5, 18)) for p in PREFIXES}
    provider = _FakeProvider(files=files)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)

    with caplog.at_level(logging.WARNING, logger="app.jobs.finra_regsho_daily_refresh"):
        run_finra_regsho_daily_refresh(
            ebull_test_conn,
            now=now,
            backfill_window_days=3,
            provider=provider,  # type: ignore[arg-type]
        )

    assert not any("match rate" in rec.message for rec in caplog.records)


# ----------------------------------------------------------------------
# 10 — Revision window re-fetches two most-recent × 6 prefixes
# ----------------------------------------------------------------------


def test_run_revision_window_re_fetches_two_most_recent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_panel(ebull_test_conn)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
    # Pre-seed ALL prefixes for both 5/15 + 5/18 as parsed.
    for d in (date(2026, 5, 15), date(2026, 5, 18)):
        for p in PREFIXES:
            _seed_parsed_manifest(ebull_test_conn, d, p)
    ebull_test_conn.commit()

    provider = _FakeProvider()
    run_finra_regsho_daily_refresh(
        ebull_test_conn,
        now=now,
        backfill_window_days=3,
        provider=provider,  # type: ignore[arg-type]
    )

    # Revision window = last-2 dates × 6 prefixes = 12 fetches re-issued.
    assert len(provider.calls) == 12
    expected = {(d, p) for d in (date(2026, 5, 15), date(2026, 5, 18)) for p in PREFIXES}
    assert set(provider.calls) == expected


# ----------------------------------------------------------------------
# 11 — Stats aggregate properties
# ----------------------------------------------------------------------


def test_stats_aggregates() -> None:
    from app.services.finra_regsho_ingest import RegShoDailyIngestStats

    stats = RegShoDailyRefreshStats(
        daily_files=[
            RegShoDailyIngestStats(
                trade_date=date(2026, 5, 15),
                prefix="CNMS",
                rows_parsed=5,
                rows_resolved=5,
                rows_upserted=5,
            ),
            RegShoDailyIngestStats(
                trade_date=date(2026, 5, 15),
                prefix="FNQC",
                failed=True,
                error_detail="oops",
            ),
        ]
    )
    assert stats.total_upserted == 5
    assert stats.total_parsed == 5
    assert stats.total_resolved == 5
    assert stats.failed_files == 1
