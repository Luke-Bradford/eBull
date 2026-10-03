"""Ranking-pot-v2 reader (#3592 slice 3b): the SQL behind the snapshot's ``v2`` block (spec §5) and the inputs of the
§4 / Appendix A S2-a pre-score gates.

Reads only, in the caller's transaction (the rebalance's REPEATABLE READ snapshot, or the pre-score check's). The pure
core (``ranking_pot_v2``) re-applies every rule, so a reader result may be a superset; the SQL narrows only to bound
the snapshot, and each narrowing below mirrors a core predicate exactly:

- purchases: ``txn_code = 'P'`` rows of S₀ names dated in the look-back window and filed by ``as_of``. Every such row
  is stored, whatever its direction flag, derivative flag or date validity, so the core's exclusions stay visible.
- history: for each (filer CIK, issuer CIK, purchase year) of a qualifying in-window purchase, that pair's open-market
  rows (``P``/``A`` or ``S``/``D``, non-derivative, valid date) dated in the three years before the purchase year and
  filed before that year's 1 January UTC and by ``as_of`` — exactly ``ranking_pot_v2.pair_cells``' rows. The issuer
  side joins on ``insider_filings.issuer_cik``, so a pair's history spans every instrument of its issuer.
- DTC: every revision at S* observed by ``as_of`` for S₀ names; the core picks the latest. ⚠ FINRA's ingest re-stamps
  ``known_from = NOW()`` on every refresh of a settlement it already holds (``finra_short_interest`` upsert), so
  ``known_from`` is LAST-refreshed, not first-observed: a row refreshed after ``as_of`` vanishes from an ``as_of`` read.
  Callers therefore pass, for the DTC read and ``ranking_pot_v2.dtc_read`` alike, the reading transaction's own time
  (``transaction_timestamp()``), never an earlier fire time — the snapshot stores each row's ``known_from``, so the
  replay is exact (spec §5; Codex ckpt-2 slice 3b).

Manifest status is NOT an evidence filter: ``insider_transactions`` has a second writer (the bulk insider dataset), and
on dev 2026-10-03 14,458 accessions holding 38,570 rows sit under a ``tombstoned`` manifest row, mostly "missing
instrument_id" (8,433) or "retention floor" (4,194) — valid rows the per-filing parser never needed to produce; 0 sit
under ``pending``. The operator-visible insider readers do not filter on it either.

Known-at is the manifest ``filed_at`` (§2 (b)). Counted, never gated (§5): Form 3/4/5 manifest rows of S₀ names filed
in the window that are not parsed, and those tombstoned.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final

import psycopg

from app.services import ranking_pot_v2 as v2

Conn = psycopg.Connection[Any]

#: Manifest sources whose filings carry Table I transactions (Form 3 carries holdings only).
TRANSACTION_SOURCES: Final = ("sec_form4", "sec_form5")

_INSIDER_COLUMNS: Final = """
    t.id, t.accession_number, t.instrument_id, t.filer_cik, f.issuer_cik, t.txn_date, t.txn_code,
    t.acquired_disposed_code, t.is_derivative, t.txn_date_invalid, m.filed_at
"""

#: ``ranking_pot_v2.is_open_market`` in SQL.
_OPEN_MARKET: Final = """
    NOT {t}.is_derivative AND NOT {t}.txn_date_invalid
    AND (({t}.txn_code = 'P' AND {t}.acquired_disposed_code = 'A')
         OR ({t}.txn_code = 'S' AND {t}.acquired_disposed_code = 'D'))
"""


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts.astimezone(UTC)


def _insider_row(r: tuple[Any, ...]) -> v2.InsiderRow:
    return v2.InsiderRow(
        txn_id=int(r[0]),
        accession=str(r[1]),
        instrument_id=int(r[2]),
        filer_cik=r[3],
        issuer_cik=r[4],
        txn_date=r[5],
        txn_code=r[6],
        acquired_disposed_code=r[7],
        is_derivative=bool(r[8]),
        txn_date_invalid=bool(r[9]),
        filed_at=_utc(r[10]),
    )


def read_purchases(
    conn: Conn, *, s0_ids: Iterable[int], target_session: date, as_of: datetime
) -> tuple[v2.InsiderRow, ...]:
    first, end = v2.lookback_window(target_session)
    rows = conn.execute(
        f"""
        SELECT {_INSIDER_COLUMNS}
          FROM insider_transactions t
          JOIN insider_filings f ON f.accession_number = t.accession_number
          JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
         WHERE t.instrument_id = ANY(%(ids)s) AND t.txn_code = 'P'
           AND t.txn_date >= %(first)s AND t.txn_date < %(end)s AND m.filed_at <= %(as_of)s
         ORDER BY t.id
        """,
        {"ids": sorted(set(s0_ids)), "first": first, "end": end, "as_of": _utc(as_of)},
    ).fetchall()
    return tuple(_insider_row(r) for r in rows)


def read_history(conn: Conn, purchases: Iterable[v2.InsiderRow], *, as_of: datetime) -> tuple[v2.InsiderRow, ...]:
    """The classification history of every keyable qualifying purchase in ``purchases`` (already window- and
    S₀-filtered by ``read_purchases``)."""
    pairs = sorted(
        {
            (r.filer_cik, r.issuer_cik, r.txn_date.year)
            for r in purchases
            if v2.is_qualifying_purchase(r) and r.filer_cik is not None and r.issuer_cik is not None
        }
    )
    if not pairs:
        return ()
    rows = conn.execute(
        f"""
        WITH pairs AS (
            SELECT * FROM unnest(%(filers)s::text[], %(issuers)s::text[], %(years)s::int[])
                       AS p(filer_cik, issuer_cik, y)
        )
        SELECT DISTINCT ON (t.id) {_INSIDER_COLUMNS}
          FROM pairs p
          JOIN insider_filings f ON f.issuer_cik = p.issuer_cik
          JOIN insider_transactions t ON t.accession_number = f.accession_number AND t.filer_cik = p.filer_cik
          JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
         WHERE {_OPEN_MARKET.format(t="t")}
           AND t.txn_date >= make_date(p.y - %(hy)s, 1, 1) AND t.txn_date < make_date(p.y, 1, 1)
           AND m.filed_at < make_timestamptz(p.y, 1, 1, 0, 0, 0, 'UTC') AND m.filed_at <= %(as_of)s
         ORDER BY t.id
        """,
        {
            "filers": [p[0] for p in pairs],
            "issuers": [p[1] for p in pairs],
            "years": [p[2] for p in pairs],
            "hy": v2.CMP_HISTORY_YEARS,
            "as_of": _utc(as_of),
        },
    ).fetchall()
    return tuple(_insider_row(r) for r in rows)


def usable_history_counts(conn: Conn, *, first_month: date, end_month: date) -> dict[date, int]:
    """§8 usable history rows per calendar month in ``[first_month, end_month)``: §2's history trades with both CIKs
    and a manifest ``filed_at`` — the population ``ranking_pot_v2.history_floor`` and the S2-a guard count."""
    if first_month.day != 1 or end_month.day != 1:
        raise ValueError("months are month starts")
    rows = conn.execute(
        f"""
        SELECT date_trunc('month', t.txn_date)::date, count(*)
          FROM insider_transactions t
          JOIN insider_filings f ON f.accession_number = t.accession_number
          JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
         WHERE {_OPEN_MARKET.format(t="t")}
           AND t.filer_cik IS NOT NULL AND f.issuer_cik IS NOT NULL AND m.filed_at IS NOT NULL
           AND t.txn_date >= %(first)s AND t.txn_date < %(end)s
         GROUP BY 1
        """,
        {"first": first_month, "end": end_month},
    ).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def read_manifest_gaps(conn: Conn, *, s0_ids: Iterable[int], target_session: date, as_of: datetime) -> dict[str, int]:
    """§5 reader counts: Form 4/5 manifest rows of S₀ names filed from the window's first day to ``as_of`` that are
    not parsed, and the tombstoned among them. Their transactions are invisible to this rebalance."""
    first, _ = v2.lookback_window(target_session)
    row = conn.execute(
        """
        SELECT count(*) FILTER (WHERE ingest_status <> 'parsed'), count(*) FILTER (WHERE ingest_status = 'tombstoned')
          FROM sec_filing_manifest
         WHERE source = ANY(%(sources)s) AND instrument_id = ANY(%(ids)s)
           AND filed_at >= make_timestamptz(%(y)s, %(m)s, 1, 0, 0, 0, 'UTC') AND filed_at <= %(as_of)s
        """,
        {
            "sources": list(TRANSACTION_SOURCES),
            "ids": sorted(set(s0_ids)),
            "y": first.year,
            "m": first.month,
            "as_of": _utc(as_of),
        },
    ).fetchone()
    if row is None:  # an aggregate always returns one row; explicit, not an assert, so `python -O` keeps it
        raise RuntimeError("manifest gap count returned no row")
    return {"form4_manifest_unparsed": int(row[0]), "form4_manifest_tombstoned": int(row[1])}


def _adv(value: Decimal | None) -> int | None:
    if value is None:
        return None
    if value != value.to_integral_value():
        raise ValueError(f"average_daily_volume {value} is not a whole share count")
    return int(value)


def read_dtc_rows(conn: Conn, *, s0_ids: Iterable[int], as_of: datetime) -> tuple[v2.DtcRow, ...]:
    """Every revision observed by ``as_of`` at S*, the latest settlement date ≤ ``as_of``'s UTC date with an observed
    row for an S₀ name (``ranking_pot_v2.dtc_read``'s S*). Empty when there is none."""
    ids = sorted(set(s0_ids))
    as_of = _utc(as_of)
    rows = conn.execute(
        """
        WITH s AS (
            SELECT max(settlement_date) AS d FROM finra_short_interest_observations
             WHERE instrument_id = ANY(%(ids)s) AND known_from <= %(as_of)s AND settlement_date <= %(day)s
        )
        SELECT o.instrument_id, o.settlement_date, o.days_to_cover, o.known_from, o.source_document_id,
               o.average_daily_volume
          FROM finra_short_interest_observations o JOIN s ON o.settlement_date = s.d
         WHERE o.instrument_id = ANY(%(ids)s) AND o.known_from <= %(as_of)s
         ORDER BY o.instrument_id, o.known_from, o.source_document_id
        """,
        {"ids": ids, "as_of": as_of, "day": as_of.date()},
    ).fetchall()
    return tuple(
        v2.DtcRow(
            instrument_id=int(r[0]),
            settlement_date=r[1],
            days_to_cover=r[2],
            known_from=_utc(r[3]),
            source_document_id=str(r[4]),
            average_daily_volume=_adv(r[5]),
        )
        for r in rows
    )


def read_v2_inputs(conn: Conn, *, s0_ids: Iterable[int], target_session: date, as_of: datetime) -> v2.V2Inputs:
    """The ``v2`` block's inputs, all read in the caller's transaction."""
    ids = tuple(sorted(set(s0_ids)))
    purchases = read_purchases(conn, s0_ids=ids, target_session=target_session, as_of=as_of)
    return v2.V2Inputs(
        purchases=purchases,
        history=read_history(conn, purchases, as_of=as_of),
        dtc=read_dtc_rows(conn, s0_ids=ids, as_of=as_of),
        reader_counts=read_manifest_gaps(conn, s0_ids=ids, target_session=target_session, as_of=as_of),
    )
