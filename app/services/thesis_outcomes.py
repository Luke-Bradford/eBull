"""Calibration-ledger outcome capture — nightly deterministic job (#2002).

Theses are versioned, dated forecasts; nothing ever scored them against
what the market subsequently did. This module captures realized returns
per THESIS VERSION at fixed horizons (30/90/365d) into the append-only
``thesis_outcomes`` ledger. Passive measurement only: no LLM, no scoring
feed, no trade-path contact, zero cloud spend.

Contracts (spec: docs/proposals/thesis/2026-07-16-calibration-ledger-schema.md):

* Anchor semantics = #2014/#2017: the write-time reference is the minting
  run's ``context_summary.blocks.price_anchor`` (persisted PRE-LLM); the
  trusted close is re-read deterministically from ``price_daily`` at or
  before ``as_of`` — never taken from a JSON copy.
* Maturity is DATA-anchored, not wall-clock: a (thesis, horizon) pair is
  mature when ``max(price_date) >= anchor_date + horizon``. A stale
  series defers capture; it can never mint a wrong-horizon row.
* Insert-once: PK (thesis_id, horizon_days) + ON CONFLICT DO NOTHING.
  Re-runs insert nothing. Rows are never updated.
* Honest missingness: anchorless theses (pre-#2017 rows, unavailable or
  unparseable anchors) get NO rows — a queryable gap, never a neutral
  value. Non-positive closes are skipped and counted, never written.

Pure predicates are module-level functions (table-testable, no DB);
``capture_thesis_outcomes`` is the one DB reader+writer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.services.thesis_dq_audit import _anchor_block, _parse_iso_date

HORIZONS: tuple[int, ...] = (30, 90, 365)

METHOD_VERSION = "oc_v1"

# A never-matured pair whose price series ended more than this many days
# before the horizon due date is a dead series (delisted/acquired/halted),
# not temporarily stale data. No outcome row is ever written for it — a
# return against a halted print is not a market outcome.
_SERIES_DEAD_GRACE_DAYS = 30


@dataclass(frozen=True)
class ThesisOutcomeCaptureReport:
    """Census of one capture run. ``inserted`` is the job row_count
    (0 = healthy steady state on quiet days); everything else is honest
    missingness, first-class by spec."""

    scanned_theses: int = 0
    anchorless: int = 0
    inserted: int = 0
    mature_pairs: int = 0
    immature_data_current: int = 0
    immature_series_stalled: int = 0
    series_dead: int = 0
    skipped_missing_close: int = 0
    skipped_nonpositive_close: int = 0


def anchor_date_from_summary(summary: object) -> date | None:
    """The usable anchor date of a minting run, or None.

    Usable = ``price_anchor.available`` is True AND ``as_of`` parses as an
    ISO date. Anything else (no summary, no block, available false,
    unparseable as_of) is anchorless — the thesis gets no ledger rows.
    """
    anchor = _anchor_block(summary)
    if anchor is None or anchor.get("available") is not True:
        return None
    return _parse_iso_date(anchor.get("as_of"))


def is_mature(anchor_date: date, horizon_days: int, max_price_date: date | None) -> bool:
    """DATA-anchored maturity: the series has printed at or past the due
    date. Wall-clock never participates."""
    if max_price_date is None:
        return False
    return max_price_date >= anchor_date + timedelta(days=horizon_days)


def classify_immature(
    *,
    is_tradable: bool,
    max_price_date: date | None,
    due_date: date,
    today: date,
) -> str:
    """Split never-matured pairs so a dead series is not indistinguishable
    from temporarily stale data (spec, Codex ckpt-1 Medium).

    * ``immature_data_current`` — series is alive (tradable, printed within
      the grace window of today): the pair simply isn't due yet.
    * ``series_dead`` — the series ended more than the grace window before
      the due date: the print this pair needs will never come. A rowless
      series is dead only when the instrument is also untradable — for a
      tradable instrument, no rows is absent data (ingest gap / fresh
      listing), never a terminal verdict (Codex ckpt-2 Medium).
    * ``immature_series_stalled`` — series has stopped (untradable, no
      recent print, or no rows yet while tradable) but not provably dead.

    ``today`` participates only in the alive-vs-stopped liveness split —
    the counters are telemetry; row insertion is purely data-anchored.
    """
    ended = not is_tradable or max_price_date is None or (today - max_price_date).days > _SERIES_DEAD_GRACE_DAYS
    if not ended:
        return "immature_data_current"
    if max_price_date is None:
        return "series_dead" if not is_tradable else "immature_series_stalled"
    if (due_date - max_price_date).days > _SERIES_DEAD_GRACE_DAYS:
        return "series_dead"
    return "immature_series_stalled"


def close_row_at_or_before(
    conn: psycopg.Connection[Any], instrument_id: int, as_of: date
) -> tuple[date, Decimal] | None:
    """(price_date, close) of the last print at or before ``as_of``.

    Same read as ``thesis_dq_audit._close_at_or_before`` but the ledger
    also needs the trading day actually used (``realized_date``), hence
    the widened return shape rather than a shared helper. The close stays
    ``Decimal`` end-to-end (NUMERIC in, NUMERIC out — no binary-float
    round-trip; PR #2061 review); a non-finite NUMERIC (NaN) maps to None
    the same way the ``_to_float`` chokepoint would.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT price_date, close FROM price_daily
            WHERE instrument_id = %(iid)s AND price_date <= %(as_of)s
            ORDER BY price_date DESC LIMIT 1
            """,
            {"iid": instrument_id, "as_of": as_of},
        )
        row = cur.fetchone()
    if row is None:
        return None
    close = row[1]
    if not isinstance(close, Decimal) or not close.is_finite():
        return None
    return (row[0], close)


# All thesis versions with at least one horizon uncaptured. The run
# LATERAL takes the latest context_summary-BEARING run per thesis (spec:
# availability truth is the persisted PRE-LLM summary, #2017); theses
# whose runs all lack a summary come back with NULL and count as
# anchorless. cardinality < n_horizons keeps the scan bounded to
# unfinished theses without repeating the horizon list in SQL.
_CANDIDATES_SQL = """
SELECT t.thesis_id, t.instrument_id, i.is_tradable,
       r.context_summary,
       p.max_price_date,
       COALESCE(o.done, '{}') AS horizons_done
FROM theses t
JOIN instruments i ON i.instrument_id = t.instrument_id
LEFT JOIN LATERAL (
    SELECT context_summary
    FROM thesis_runs r
    WHERE r.thesis_id = t.thesis_id AND r.context_summary IS NOT NULL
    ORDER BY r.run_id DESC
    LIMIT 1
) r ON TRUE
LEFT JOIN LATERAL (
    SELECT max(price_date) AS max_price_date
    FROM price_daily p
    WHERE p.instrument_id = t.instrument_id
) p ON TRUE
LEFT JOIN LATERAL (
    SELECT array_agg(o.horizon_days) AS done
    FROM thesis_outcomes o
    WHERE o.thesis_id = t.thesis_id
) o ON TRUE
WHERE COALESCE(cardinality(o.done), 0) < %(n_horizons)s
ORDER BY t.thesis_id
"""

_INSERT_SQL = """
INSERT INTO thesis_outcomes
    (thesis_id, horizon_days, anchor_date, anchor_close,
     realized_date, realized_close, realized_return, method_version)
VALUES
    (%(thesis_id)s, %(horizon_days)s, %(anchor_date)s, %(anchor_close)s,
     %(realized_date)s, %(realized_close)s, %(realized_return)s, %(method_version)s)
ON CONFLICT (thesis_id, horizon_days) DO NOTHING
"""


def capture_thesis_outcomes(conn: psycopg.Connection[Any]) -> ThesisOutcomeCaptureReport:
    """Insert realized outcomes for every mature, uncaptured
    (thesis, horizon) pair. Idempotent; deterministic; append-only."""
    today = date.today()
    scanned = anchorless = inserted = mature_pairs = 0
    immature_current = immature_stalled = dead = 0
    missing_close = nonpositive = 0

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_CANDIDATES_SQL, {"n_horizons": len(HORIZONS)})
        candidates = cur.fetchall()

    with conn.transaction():
        for row in candidates:
            scanned += 1
            anchor_date = anchor_date_from_summary(row["context_summary"])
            if anchor_date is None:
                anchorless += 1
                continue
            done = set(row["horizons_done"] or ())
            max_price_date = row["max_price_date"]
            mature_missing: list[int] = []
            for horizon in HORIZONS:
                if horizon in done:
                    continue
                if is_mature(anchor_date, horizon, max_price_date):
                    mature_missing.append(horizon)
                    continue
                kind = classify_immature(
                    is_tradable=bool(row["is_tradable"]),
                    max_price_date=max_price_date,
                    due_date=anchor_date + timedelta(days=horizon),
                    today=today,
                )
                if kind == "immature_data_current":
                    immature_current += 1
                elif kind == "series_dead":
                    dead += 1
                else:
                    immature_stalled += 1
            if not mature_missing:
                continue
            mature_pairs += len(mature_missing)
            anchor_row = close_row_at_or_before(conn, row["instrument_id"], anchor_date)
            if anchor_row is None:
                missing_close += len(mature_missing)
                continue
            anchor_close = anchor_row[1]
            if anchor_close <= 0:
                nonpositive += len(mature_missing)
                continue
            for horizon in mature_missing:
                realized = close_row_at_or_before(conn, row["instrument_id"], anchor_date + timedelta(days=horizon))
                if realized is None:
                    missing_close += 1
                    continue
                realized_date, realized_close = realized
                if realized_close <= 0:
                    nonpositive += 1
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        _INSERT_SQL,
                        {
                            "thesis_id": row["thesis_id"],
                            "horizon_days": horizon,
                            "anchor_date": anchor_date,
                            "anchor_close": anchor_close,
                            "realized_date": realized_date,
                            "realized_close": realized_close,
                            "realized_return": (realized_close - anchor_close) / anchor_close,
                            "method_version": METHOD_VERSION,
                        },
                    )
                    inserted += cur.rowcount

    return ThesisOutcomeCaptureReport(
        scanned_theses=scanned,
        anchorless=anchorless,
        inserted=inserted,
        mature_pairs=mature_pairs,
        immature_data_current=immature_current,
        immature_series_stalled=immature_stalled,
        series_dead=dead,
        skipped_missing_close=missing_close,
        skipped_nonpositive_close=nonpositive,
    )
