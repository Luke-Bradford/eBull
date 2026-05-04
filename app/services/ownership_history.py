"""Ownership history reader — time-bucketed dedup over observations
(#840.F).

Operator-facing surface for "show me Vanguard's AAPL position over
the last 8 quarters". Reads ``ownership_*_observations`` (immutable,
append-only) and applies the same two-axis dedup logic per time
bucket as the rollup endpoint applies for a single point in time.

Per Codex plan-review #6: this is NOT raw observation history. The
spec calls for time-bucketed running deduped totals — one row per
``(period_end, ownership_nature)`` after dedup, so the chart shows
a coherent timeseries instead of a messy stack of competing source
rows for the same period.

Categories supported (all observations tables built in #840.A-D):
  - ``insiders`` — Form 4 + Form 3 (direct + indirect natures kept
    distinct).
  - ``blockholders`` — 13D / 13G amendments per primary filer.
  - ``institutions`` — 13F-HR equity exposure per filer per quarter.
  - ``treasury`` — XBRL DEI per period.
  - ``def14a`` — proxy bene table per (holder, period).

Each category reader returns a list of
:class:`OwnershipHistoryPoint`. The API layer (#840.F) wraps these in
a uniform response shape.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

HistoryCategory = Literal["insiders", "blockholders", "institutions", "treasury", "def14a"]


@dataclass(frozen=True)
class OwnershipHistoryPoint:
    """One time-bucket on a holder's history series.

    Fields:
      - ``period_end`` — valid-time end (e.g. quarter end for 13F).
      - ``ownership_nature`` — direct / indirect / beneficial /
        voting / economic. Kept distinct so a holder's beneficial
        and direct series render as TWO lines, not one.
      - ``shares`` — deduped total for that period × nature.
      - ``source`` — winning source tag for the bucket.
      - ``source_accession`` — winning accession for click-through.
      - ``filed_at`` — when the winning source was published.
    """

    period_end: date
    ownership_nature: str
    shares: Decimal | None
    source: str
    source_accession: str | None
    filed_at: Any


# ---------------------------------------------------------------------------
# Per-category history readers
# ---------------------------------------------------------------------------


def _insiders_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    holder_cik: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[OwnershipHistoryPoint]:
    """Per-holder insider series. Holder is identified by CIK when
    available; the caller filters via ``holder_cik`` to scope to one
    person (e.g. Cohen's GME series).

    Time-bucket dedup: for each ``(period_end, ownership_nature)``,
    pick the highest-priority source — form4 > form3 > def14a (DEF
    14A bene rows that resolve to a CIK end up in insiders too).
    Final tie-breakers: ``filed_at DESC, source_document_id ASC`` so
    the chart timeseries is deterministic across runs."""
    where_extra = ""
    params: dict[str, Any] = {"iid": instrument_id}
    if holder_cik is not None:
        where_extra += " AND holder_identity_key = %(holder_key)s"
        params["holder_key"] = f"CIK:{holder_cik.strip()}" if holder_cik.strip() else None
    if from_date is not None:
        where_extra += " AND period_end >= %(from_date)s"
        params["from_date"] = from_date
    if to_date is not None:
        where_extra += " AND period_end <= %(to_date)s"
        params["to_date"] = to_date

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (period_end, ownership_nature)
                period_end, ownership_nature,
                source, source_accession, filed_at, shares
            FROM ownership_insiders_observations
            WHERE instrument_id = %(iid)s
              AND known_to IS NULL
              AND shares IS NOT NULL
              {where_extra}
            ORDER BY
                period_end,
                ownership_nature,
                CASE source WHEN 'form4' THEN 1 WHEN 'form3' THEN 2 WHEN 'def14a' THEN 4 ELSE 10 END ASC,
                filed_at DESC,
                source_document_id ASC
            """,
            params,
        )
        return [
            OwnershipHistoryPoint(
                period_end=row["period_end"],
                ownership_nature=row["ownership_nature"],
                shares=row["shares"],
                source=row["source"],
                source_accession=row["source_accession"],
                filed_at=row["filed_at"],
            )
            for row in cur.fetchall()
        ]


def _institutions_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    filer_cik: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[OwnershipHistoryPoint]:
    """Per-filer 13F-HR series. Equity exposure only (PUT / CALL are
    option overlays, see #840.B). One point per quarter per nature."""
    where_extra = ""
    params: dict[str, Any] = {"iid": instrument_id}
    if filer_cik is not None and filer_cik.strip():
        where_extra += " AND filer_cik = %(cik)s"
        params["cik"] = filer_cik.strip()
    if from_date is not None:
        where_extra += " AND period_end >= %(from_date)s"
        params["from_date"] = from_date
    if to_date is not None:
        where_extra += " AND period_end <= %(to_date)s"
        params["to_date"] = to_date

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (period_end, ownership_nature)
                period_end, ownership_nature,
                source, source_accession, filed_at, shares
            FROM ownership_institutions_observations
            WHERE instrument_id = %(iid)s
              AND known_to IS NULL
              AND shares IS NOT NULL
              AND exposure_kind = 'EQUITY'
              {where_extra}
            ORDER BY
                period_end,
                ownership_nature,
                filed_at DESC,
                source_document_id ASC
            """,
            params,
        )
        return [
            OwnershipHistoryPoint(
                period_end=row["period_end"],
                ownership_nature=row["ownership_nature"],
                shares=row["shares"],
                source=row["source"],
                source_accession=row["source_accession"],
                filed_at=row["filed_at"],
            )
            for row in cur.fetchall()
        ]


def _blockholders_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    reporter_cik: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[OwnershipHistoryPoint]:
    """Per-primary-filer 13D/G amendment series. Each amendment is a
    distinct point on the timeline; dedup picks the latest amendment
    per period_end day if multiple landed the same day."""
    where_extra = ""
    params: dict[str, Any] = {"iid": instrument_id}
    if reporter_cik is not None and reporter_cik.strip():
        where_extra += " AND reporter_cik = %(cik)s"
        params["cik"] = reporter_cik.strip()
    if from_date is not None:
        where_extra += " AND period_end >= %(from_date)s"
        params["from_date"] = from_date
    if to_date is not None:
        where_extra += " AND period_end <= %(to_date)s"
        params["to_date"] = to_date

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (period_end, ownership_nature)
                period_end, ownership_nature,
                source, source_accession, filed_at,
                aggregate_amount_owned AS shares
            FROM ownership_blockholders_observations
            WHERE instrument_id = %(iid)s
              AND known_to IS NULL
              AND aggregate_amount_owned IS NOT NULL
              {where_extra}
            ORDER BY
                period_end,
                ownership_nature,
                filed_at DESC,
                source_document_id ASC
            """,
            params,
        )
        return [
            OwnershipHistoryPoint(
                period_end=row["period_end"],
                ownership_nature=row["ownership_nature"],
                shares=row["shares"],
                source=row["source"],
                source_accession=row["source_accession"],
                filed_at=row["filed_at"],
            )
            for row in cur.fetchall()
        ]


def _treasury_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[OwnershipHistoryPoint]:
    """Per-period treasury series. One point per filing period."""
    where_extra = ""
    params: dict[str, Any] = {"iid": instrument_id}
    if from_date is not None:
        where_extra += " AND period_end >= %(from_date)s"
        params["from_date"] = from_date
    if to_date is not None:
        where_extra += " AND period_end <= %(to_date)s"
        params["to_date"] = to_date

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (period_end)
                period_end, ownership_nature,
                source, source_accession, filed_at,
                treasury_shares AS shares
            FROM ownership_treasury_observations
            WHERE instrument_id = %(iid)s
              AND known_to IS NULL
              AND treasury_shares IS NOT NULL
              {where_extra}
            ORDER BY
                period_end,
                filed_at DESC,
                source_document_id ASC
            """,
            params,
        )
        return [
            OwnershipHistoryPoint(
                period_end=row["period_end"],
                ownership_nature=row["ownership_nature"],
                shares=row["shares"],
                source=row["source"],
                source_accession=row["source_accession"],
                filed_at=row["filed_at"],
            )
            for row in cur.fetchall()
        ]


def _def14a_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    holder_name: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[OwnershipHistoryPoint]:
    """Per-holder DEF 14A proxy series. Identity is the normalised
    holder_name_key; caller filters via ``holder_name`` (case +
    whitespace insensitive)."""
    where_extra = ""
    params: dict[str, Any] = {"iid": instrument_id}
    if holder_name is not None and holder_name.strip():
        where_extra += " AND holder_name_key = %(key)s"
        params["key"] = holder_name.strip().lower()
    if from_date is not None:
        where_extra += " AND period_end >= %(from_date)s"
        params["from_date"] = from_date
    if to_date is not None:
        where_extra += " AND period_end <= %(to_date)s"
        params["to_date"] = to_date

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (period_end, ownership_nature)
                period_end, ownership_nature,
                source, source_accession, filed_at, shares
            FROM ownership_def14a_observations
            WHERE instrument_id = %(iid)s
              AND known_to IS NULL
              AND shares IS NOT NULL
              {where_extra}
            ORDER BY
                period_end,
                ownership_nature,
                filed_at DESC,
                source_document_id ASC
            """,
            params,
        )
        return [
            OwnershipHistoryPoint(
                period_end=row["period_end"],
                ownership_nature=row["ownership_nature"],
                shares=row["shares"],
                source=row["source"],
                source_accession=row["source_accession"],
                filed_at=row["filed_at"],
            )
            for row in cur.fetchall()
        ]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _normalise_holder_id(holder_id: str | None) -> str | None:
    """Bot review for #840.F: blank / whitespace-only ``holder_id``
    used to fall through to a SQL ``= NULL`` predicate (because
    ``holder_id.strip() else None``) which silently returns an empty
    series. The API layer guards against this for holder-scoped
    categories, but direct service callers (tests, future internal
    paths) hit the silent-empty trap. Normalise here so an empty
    string maps to ``None`` (full series) and a meaningful value
    becomes a stripped form ready for parameterised lookup."""
    if holder_id is None:
        return None
    stripped = holder_id.strip()
    return stripped or None


def get_ownership_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    category: HistoryCategory,
    holder_id: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[OwnershipHistoryPoint]:
    """Return time-bucketed deduped ownership history for one
    instrument × category × optional holder.

    ``holder_id`` semantics per category:
      - ``insiders`` → holder_cik (CIK).
      - ``blockholders`` → primary reporter CIK.
      - ``institutions`` → filer_cik.
      - ``treasury`` → ignored (issuer-level series).
      - ``def14a`` → holder_name (case + whitespace insensitive).

    Codex plan-review #6: this returns DEDUPED points, not raw
    observations — one point per ``(period_end, ownership_nature)``
    after applying the source-priority chain + deterministic
    tie-breakers."""
    holder = _normalise_holder_id(holder_id)
    if category == "insiders":
        return _insiders_history(
            conn,
            instrument_id=instrument_id,
            holder_cik=holder,
            from_date=from_date,
            to_date=to_date,
        )
    if category == "blockholders":
        return _blockholders_history(
            conn,
            instrument_id=instrument_id,
            reporter_cik=holder,
            from_date=from_date,
            to_date=to_date,
        )
    if category == "institutions":
        return _institutions_history(
            conn,
            instrument_id=instrument_id,
            filer_cik=holder,
            from_date=from_date,
            to_date=to_date,
        )
    if category == "treasury":
        return _treasury_history(
            conn,
            instrument_id=instrument_id,
            from_date=from_date,
            to_date=to_date,
        )
    if category == "def14a":
        return _def14a_history(
            conn,
            instrument_id=instrument_id,
            holder_name=holder,
            from_date=from_date,
            to_date=to_date,
        )
    raise ValueError(f"unknown category {category!r}")


def iter_categories() -> Iterator[HistoryCategory]:
    """Iteration helper for callers that need to enumerate every
    category (e.g. test surface)."""
    yield from ("insiders", "blockholders", "institutions", "treasury", "def14a")
