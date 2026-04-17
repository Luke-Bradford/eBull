"""Coverage audit v1 (#268 Chunk D).

Classifies every tradable instrument's ``coverage.filings_status`` so
downstream thesis / scoring / cascade work (#273, #276) can gate on
``filings_status = 'analysable'``.

Classifier outputs (one of four; ``unknown`` is a pre-audit placeholder
written elsewhere and ``structurally_young`` is assigned by Chunk E,
not here):

- ``analysable`` — US domestic issuer, 10-K count >= 2 in 3y AND
  10-Q count >= 4 in 18mo. Base forms only — amendments do NOT count
  toward history-depth thresholds.
- ``insufficient`` — has primary SEC CIK but below the bar.
- ``fpi`` — Foreign Private Issuer: SEC CIK, zero US base-or-amend
  filings, at least one of {20-F, 40-F, 6-K} (base or amendment).
- ``no_primary_sec_cik`` — no primary ``sec``/``cik`` row in
  ``external_identifiers``. Non-US, crypto, ETFs, etc.

Windows are computed in SQL via ``COUNT(*) FILTER (WHERE ...)`` so the
Python classifier receives exact per-window counts. ``INTERVAL '18
months'`` is calendar-correct — no ``timedelta(days=548)`` drift.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditCounts:
    """Per-instrument row returned by the audit aggregate."""

    instrument_id: int
    ten_k_in_3y: int
    ten_q_in_18m: int
    us_base_or_amend_total: int
    fpi_total: int


@dataclass(frozen=True)
class AuditSummary:
    """Run result for ``audit_all_instruments``.

    ``null_anomalies`` is the count from a post-update query over
    tradable instruments left-joined to ``coverage`` — captures both
    missing coverage rows (Chunk B regression) AND rows whose
    ``filings_status`` remained NULL after the bulk UPDATE. Either is
    a data-integrity bug and is logged at WARNING.
    """

    analysable: int
    insufficient: int
    fpi: int
    no_primary_sec_cik: int
    total_updated: int
    null_anomalies: int


def _classify(agg: AuditCounts | None, has_sec_cik: bool) -> str:
    """Pure classification — windows/counts pre-computed in SQL.

    ``agg`` is ``None`` when the instrument has zero SEC filings in the
    filing_events table. ``has_sec_cik`` comes from the cohort query.
    """
    if not has_sec_cik:
        return "no_primary_sec_cik"

    if agg is None:
        # SEC CIK present but no SEC filings yet — typical for a
        # newly-added cohort member before Chunk E backfills history.
        return "insufficient"

    # FPI check FIRST: SEC CIK, zero US base-or-amend filings, at
    # least one 20-F/40-F/6-K family filing.
    if agg.us_base_or_amend_total == 0 and agg.fpi_total > 0:
        return "fpi"

    # US history bar — base forms only. Amendments do NOT count
    # toward distinct-period depth (a 10-K/A restates the same year,
    # not an additional one).
    if agg.ten_k_in_3y >= 2 and agg.ten_q_in_18m >= 4:
        return "analysable"

    return "insufficient"


def _load_aggregates(conn: psycopg.Connection[Any]) -> dict[int, AuditCounts]:
    """Return per-instrument SEC filing counts for the four dimensions
    the classifier needs.

    Windows computed in SQL via ``COUNT(*) FILTER`` so the Python
    side doesn't re-derive dates. Amendments are counted toward
    ``us_base_or_amend_total`` (for FPI detection) but NOT toward the
    history-depth thresholds ``ten_k_in_3y`` / ``ten_q_in_18m``.

    Only covered SEC CIKs (primary) are joined in, so rows for
    instruments without a primary ``sec``/``cik`` row are naturally
    excluded.
    """
    rows = conn.execute(
        """
        SELECT
            fe.instrument_id,
            COUNT(*) FILTER (
                WHERE fe.filing_type = '10-K'
                  AND fe.filing_date >= (CURRENT_DATE - INTERVAL '3 years')
            ) AS ten_k_in_3y,
            COUNT(*) FILTER (
                WHERE fe.filing_type = '10-Q'
                  AND fe.filing_date >= (CURRENT_DATE - INTERVAL '18 months')
            ) AS ten_q_in_18m,
            COUNT(*) FILTER (
                WHERE fe.filing_type IN ('10-K','10-K/A','10-Q','10-Q/A')
            ) AS us_base_or_amend_total,
            COUNT(*) FILTER (
                WHERE fe.filing_type IN ('20-F','20-F/A','40-F','40-F/A','6-K','6-K/A')
            ) AS fpi_total
        FROM filing_events fe
        JOIN external_identifiers ei
            ON ei.instrument_id = fe.instrument_id
           AND ei.provider = 'sec'
           AND ei.identifier_type = 'cik'
           AND ei.is_primary = TRUE
        WHERE fe.provider = 'sec'
        GROUP BY fe.instrument_id
        """
    ).fetchall()

    return {
        int(r[0]): AuditCounts(
            instrument_id=int(r[0]),
            ten_k_in_3y=int(r[1]),
            ten_q_in_18m=int(r[2]),
            us_base_or_amend_total=int(r[3]),
            fpi_total=int(r[4]),
        )
        for r in rows
    }


def _load_cohort(
    conn: psycopg.Connection[Any],
) -> list[tuple[int, bool]]:
    """Every tradable instrument + whether it has a primary SEC CIK."""
    rows = conn.execute(
        """
        SELECT
            i.instrument_id,
            EXISTS (
                SELECT 1 FROM external_identifiers ei
                WHERE ei.instrument_id = i.instrument_id
                  AND ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                  AND ei.is_primary = TRUE
            ) AS has_sec_cik
        FROM instruments i
        WHERE i.is_tradable = TRUE
        ORDER BY i.instrument_id
        """
    ).fetchall()
    return [(int(r[0]), bool(r[1])) for r in rows]


def _count_null_anomalies(conn: psycopg.Connection[Any]) -> int:
    """Tradable instruments missing a coverage row OR with NULL filings_status."""
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM instruments i
        LEFT JOIN coverage c ON c.instrument_id = i.instrument_id
        WHERE i.is_tradable = TRUE
          AND (c.instrument_id IS NULL OR c.filings_status IS NULL)
        """
    ).fetchone()
    return int(row[0]) if row is not None else 0


def audit_all_instruments(conn: psycopg.Connection[Any]) -> AuditSummary:
    """Full-universe audit. Writes ``filings_status`` for every tradable
    instrument via a single bulk UPDATE. Wrapped in ``conn.transaction()``
    so a mid-flight failure rolls back the whole audit rather than
    leaving the table in a partial state.

    Does NOT touch ``filings_backfill_*`` columns (Chunk E owns those)
    and never assigns ``structurally_young`` (Chunk E owns that too).
    """
    with conn.transaction():
        aggregates = _load_aggregates(conn)
        cohort = _load_cohort(conn)

        classifications: list[tuple[int, str]] = []
        counts = {
            "analysable": 0,
            "insufficient": 0,
            "fpi": 0,
            "no_primary_sec_cik": 0,
        }
        for instrument_id, has_sec_cik in cohort:
            status = _classify(aggregates.get(instrument_id), has_sec_cik)
            classifications.append((instrument_id, status))
            counts[status] += 1

        total_updated = 0
        if classifications:
            instrument_ids = [c[0] for c in classifications]
            statuses = [c[1] for c in classifications]
            result = conn.execute(
                """
                UPDATE coverage c
                SET filings_status = v.status,
                    filings_audit_at = NOW()
                FROM unnest(%s::bigint[], %s::text[]) AS v(instrument_id, status)
                WHERE c.instrument_id = v.instrument_id
                """,
                (instrument_ids, statuses),
            )
            if result.rowcount == -1:
                raise RuntimeError("audit_all_instruments UPDATE: server did not report a command tag (rowcount=-1)")
            total_updated = result.rowcount

    # Null-anomaly check runs AFTER the transaction commits so the
    # count reflects durable state. Running it inside the `with` block
    # would count uncommitted rows; on commit failure those counts
    # would be stale. Post-commit makes the check unambiguous.
    null_anomalies = _count_null_anomalies(conn)

    if null_anomalies > 0:
        logger.warning(
            "coverage_audit: %d null_anomalies detected — either tradable "
            "instruments without a coverage row (Chunk B regression) or "
            "coverage rows whose filings_status remained NULL after UPDATE. "
            "Investigate before the next thesis/scoring cycle.",
            null_anomalies,
        )

    logger.info(
        "coverage_audit: analysable=%d insufficient=%d fpi=%d no_primary_sec_cik=%d total_updated=%d",
        counts["analysable"],
        counts["insufficient"],
        counts["fpi"],
        counts["no_primary_sec_cik"],
        total_updated,
    )

    return AuditSummary(
        analysable=counts["analysable"],
        insufficient=counts["insufficient"],
        fpi=counts["fpi"],
        no_primary_sec_cik=counts["no_primary_sec_cik"],
        total_updated=total_updated,
        null_anomalies=null_anomalies,
    )


def audit_instrument(conn: psycopg.Connection[Any], instrument_id: int) -> str:
    """Single-instrument version of ``audit_all_instruments``.

    Returns the classified status. Used by Chunk G's universe-sync
    hook when a new instrument needs an immediate audit pass, and by
    Chunk E's post-backfill re-audit loop.

    Wrapped in a savepoint so the SELECT + UPDATE are atomic even
    when called inside an outer transaction.
    """
    with conn.transaction():
        agg_rows = conn.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE fe.filing_type = '10-K'
                      AND fe.filing_date >= (CURRENT_DATE - INTERVAL '3 years')
                ),
                COUNT(*) FILTER (
                    WHERE fe.filing_type = '10-Q'
                      AND fe.filing_date >= (CURRENT_DATE - INTERVAL '18 months')
                ),
                COUNT(*) FILTER (
                    WHERE fe.filing_type IN ('10-K','10-K/A','10-Q','10-Q/A')
                ),
                COUNT(*) FILTER (
                    WHERE fe.filing_type IN ('20-F','20-F/A','40-F','40-F/A','6-K','6-K/A')
                )
            FROM filing_events fe
            JOIN external_identifiers ei
                ON ei.instrument_id = fe.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
            WHERE fe.provider = 'sec'
              AND fe.instrument_id = %s
            """,
            (instrument_id,),
        ).fetchone()

        has_cik_row = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM external_identifiers ei
                WHERE ei.instrument_id = %s
                  AND ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                  AND ei.is_primary = TRUE
            )
            """,
            (instrument_id,),
        ).fetchone()
        has_sec_cik = bool(has_cik_row[0]) if has_cik_row is not None else False

        agg: AuditCounts | None
        if agg_rows is None or all(v == 0 for v in agg_rows):
            # No SEC filings for this instrument.
            agg = None
        else:
            agg = AuditCounts(
                instrument_id=instrument_id,
                ten_k_in_3y=int(agg_rows[0]),
                ten_q_in_18m=int(agg_rows[1]),
                us_base_or_amend_total=int(agg_rows[2]),
                fpi_total=int(agg_rows[3]),
            )

        status = _classify(agg, has_sec_cik)

        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status = %s,
                filings_audit_at = NOW()
            WHERE instrument_id = %s
            """,
            (status, instrument_id),
        )
        if result.rowcount == 0:
            # No coverage row for this instrument. Post-#292 this
            # should never happen — universe sync + the weekly backfill
            # together guarantee coverage rows for every tradable
            # instrument. Raise loudly rather than return a status
            # string that was never persisted.
            raise RuntimeError(
                f"audit_instrument: no coverage row for instrument_id={instrument_id}; "
                f"classifier returned {status!r} but UPDATE matched zero rows. "
                f"Check coverage bootstrap (#292) + universe sync wiring."
            )

    return status
