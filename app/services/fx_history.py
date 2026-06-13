"""Dated historical FX (``fx_rates_daily``) — backfill + read.

#1594 PR-A. Stores USD-base ECB reference rates per date (Frankfurter
time-series), mirroring the live ``live_fx_rates`` convention so the
per-day ``app.services.fx.convert`` path has parity (direct + inverse
only — no USD cross-rate). Distinct from the tax ``fx_rates`` table
(sql/013); see spec §21 R1.

Two responsibilities:

- ``ensure_fx_history`` — gap-fill the dated table from earliest ledger
  activity → today. Bulk on first load (one time-series call), forward
  tail thereafter. Idempotent (``ON CONFLICT DO NOTHING``; ECB rates are
  immutable per date).
- ``load_fx_rates_for_date`` — carry-forward read: the most-recent rate
  on/before a target date for each pair, as a ``convert``-ready dict.
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.providers.implementations.frankfurter import fetch_timeseries_rates
from app.services.runtime_config import SUPPORTED_CURRENCIES

logger = logging.getLogger(__name__)

# USD base mirrors the live fx_rates_refresh convention. Targets are every
# other supported display currency (GBP, EUR) — the pairs the chart needs.
FX_BASE = "USD"


def supported_targets(base: str = FX_BASE) -> list[str]:
    """The quote currencies to fetch for ``base`` (sorted, deterministic)."""
    return sorted(c for c in SUPPORTED_CURRENCIES if c != base)


def fetch_ranges(
    min_existing: date | None,
    max_existing: date | None,
    since: date,
    until: date,
) -> list[tuple[date, date]]:
    """Compute the date ranges still needing a fetch — pure, table-tested.

    - Empty table → fetch the whole ``[since, until]`` (bulk first load).
    - Otherwise fetch only the gaps outside ``[min_existing, max_existing]``:
      an older span if ``since`` extended earlier, and/or the forward tail.
      Range endpoints deliberately touch the existing boundary by one day —
      harmless under ``ON CONFLICT DO NOTHING``.
    """
    if until < since:
        return []
    if min_existing is None or max_existing is None:
        return [(since, until)]
    ranges: list[tuple[date, date]] = []
    if since < min_existing:
        ranges.append((since, min_existing))
    if max_existing < until:
        ranges.append((max_existing, until))
    return ranges


def _earliest_ledger_date(conn: psycopg.Connection[Any]) -> date | None:
    """Earliest activity across trade_events + cash_ledger → backfill floor."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT LEAST(
                (SELECT MIN(executed_at) FROM trade_events),
                (SELECT MIN(event_time) FROM cash_ledger)
            )::date
            """
        )
        row = cur.fetchone()
    return row[0] if row else None


def ensure_fx_history(
    conn: psycopg.Connection[Any],
    *,
    until: date,
    base: str = FX_BASE,
    targets: list[str] | None = None,
    since: date | None = None,
) -> int:
    """Gap-fill ``fx_rates_daily`` for ``base`` up to ``until``.

    ``since`` defaults to the earliest ledger activity (bulk first load);
    pass an explicit floor to widen. Returns rows written. Network failure
    on any range is logged and the gap left for the next run (self-healing)
    — partial progress from earlier ranges is kept.
    """
    quote_targets = targets if targets is not None else supported_targets(base)
    if not quote_targets:
        return 0

    floor = since or _earliest_ledger_date(conn) or until
    if floor > until:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT MIN(rate_date), MAX(rate_date) FROM fx_rates_daily WHERE base_currency = %s",
            (base,),
        )
        row = cur.fetchone()
    min_existing: date | None = row[0] if row else None
    max_existing: date | None = row[1] if row else None

    written = 0
    for start, end in fetch_ranges(min_existing, max_existing, floor, until):
        try:
            by_date = fetch_timeseries_rates(base, quote_targets, start, end)
        except Exception:
            logger.warning(
                "ensure_fx_history: Frankfurter fetch failed for %s..%s (base=%s)",
                start,
                end,
                base,
                exc_info=True,
            )
            continue
        rows = [(rate_date, b, q, rate) for rate_date, pairs in by_date.items() for (b, q), rate in pairs.items()]
        if not rows:
            continue
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO fx_rates_daily (rate_date, base_currency, quote_currency, rate)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rate_date, base_currency, quote_currency) DO NOTHING
                """,
                rows,
            )
            written += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    logger.info("ensure_fx_history: base=%s floor=%s until=%s rows_written=%d", base, floor, until, written)
    return written


def load_fx_rates_for_date(
    conn: psycopg.Connection[Any],
    rate_date: date,
) -> tuple[dict[tuple[str, str], Decimal], date | None]:
    """Carry-forward FX rates as of ``rate_date`` — convert()-ready dict.

    Returns ``(rates, fx_rate_date)`` where *rates* is keyed
    ``(base, quote) -> rate`` (the most-recent row on/before ``rate_date``
    per pair) and *fx_rate_date* is the newest underlying ``rate_date``
    actually used (NULL when the table has nothing on/before the target).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (base_currency, quote_currency)
                base_currency, quote_currency, rate, rate_date
            FROM fx_rates_daily
            WHERE rate_date <= %(d)s
            ORDER BY base_currency, quote_currency, rate_date DESC
            """,
            {"d": rate_date},
        )
        rows = cur.fetchall()

    rates: dict[tuple[str, str], Decimal] = {}
    used: date | None = None
    for row in rows:
        rates[(str(row["base_currency"]), str(row["quote_currency"]))] = Decimal(str(row["rate"]))
        rd: date = row["rate_date"]
        if used is None or rd > used:
            used = rd
    return rates, used
