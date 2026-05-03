"""DEF 14A vs Form 4 cumulative drift detector (#769 PR 3 of N).

Compares each named insider's DEF 14A snapshot (the latest
``def14a_beneficial_holdings`` row per (instrument, holder)) against
the equivalent Form 4 cumulative running total + Form 3 baseline.
Emits ``def14a_drift_alerts`` rows when the absolute drift exceeds
the warning threshold so the ops monitor (#13) can render a
per-issuer reconciliation health indicator.

Three drift outcomes:

  * ``info`` — DEF 14A names a holder that has NO matching Form 4
    filer. Most common reasons: officer never traded post-baseline
    (and Form 3 isn't on file either), DEF 14A holder name is a
    name variant the matcher didn't catch (per #769 the holder→CIK
    auto-resolution is out of scope for v1; a curated mapping seed
    table is a follow-up). Either way the operator should see the
    gap.
  * ``warning`` — drift >= 5%. Likely a missed Form 4 transaction or
    a baseline mis-classification.
  * ``critical`` — drift >= 25%. Strong signal of a systematic
    coverage gap; flagged loudly so the operator triages first.

Holder-name match is exact case-insensitive equality after
normalising both sides via :func:`_normalise_name` — strips
trailing role suffixes (``", CEO"`` / ``" - Director"`` /
``" — Director"`` / ``" – Director"``). The detector deliberately
does NOT do fuzzy substring matching: ``"Ann"`` would silently
match ``"Joanne Smith"`` and ``"John Doe"`` would silently match
``"John Doe Jr"``. Variants that genuinely refer to the same
person (mid-life name changes, suffix differences) fall through
to the info-severity coverage gap, which the v2 curated holder→
filer mapping seed table will resolve.

For each matched filer we take:

  1. Form 4 latest ``post_transaction_shares`` for the same
     ``(instrument_id, filer)`` — the most recent cumulative
     position post-transaction.
  2. Falling back to Form 3 baseline ``shares`` when no Form 4 row
     matches (officer who was granted shares on appointment and
     never traded).

Match accepts ``filer_cik IS NULL`` rows: legacy / backfilled
Form 4 rows can have NULL CIK and a zero-drift reconciliation on
such a row is still a real reconciliation, not a coverage gap.

Idempotent + self-clearing: re-running the detector on the same
accession promotes existing alert rows in place via UPSERT, AND
clears stale alerts when the underlying drift has since been
resolved (the detector deletes any row whose new severity is
``None``). An operator who wants to suppress an alert temporarily
should mark the source row instead of deleting from
``def14a_drift_alerts``, since the next detector run would
re-emit it.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.services.holder_name_resolver import (
    normalise_name,
    resolve_holder_to_filer,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DriftAlert:
    """One drift finding. Mirrors the table column set 1:1 so the
    detector can build the row dict directly from the dataclass."""

    instrument_id: int
    holder_name: str
    matched_filer_cik: str | None
    def14a_shares: Decimal | None
    form4_cumulative: Decimal | None
    drift_pct: Decimal | None
    severity: str  # 'info' | 'warning' | 'critical'
    accession_number: str
    as_of_date: date | None


@dataclass(frozen=True)
class DriftReport:
    """Per-run rollup. Drives the ops monitor's coverage chip and
    the run-status logging. ``alerts`` is the full set the detector
    persisted (or refreshed) on this pass."""

    holders_evaluated: int
    alerts_emitted: int
    alerts_by_severity: dict[str, int]


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------


# Drift fraction (not percent) thresholds. Stored on the alert row
# as a fraction so ``Decimal('0.05')`` = 5%; the ops monitor's UI
# multiplies by 100 for display. Issue #769's "drift > 5% surfaces
# on ops monitor" maps to >= ``WARNING_THRESHOLD``.
WARNING_THRESHOLD: Decimal = Decimal("0.05")
CRITICAL_THRESHOLD: Decimal = Decimal("0.25")

# Sentinel — DEF 14A holder rows whose ``issuer_cik`` is the
# placeholder from the ingester (#769 PR 2) for instruments without
# an ``instrument_sec_profile`` row. Skipped during detection
# because the issuer-side context is incomplete.
_CIK_MISSING_SENTINEL = "CIK-MISSING"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _classify_severity(
    *,
    matched: bool,
    drift_pct: Decimal | None,
) -> str | None:
    """Map a (matched, drift) tuple to a severity tag, or ``None``
    when the drift is below the warning threshold AND a Form 4
    match was found.

    Returning ``None`` lets the caller skip emitting alerts for
    holders that reconcile cleanly — the alert table is intended as
    "open issues", not a full audit log of every reconciliation
    pass."""
    if not matched:
        # Holder visible on the proxy but no Form 4 match — coverage
        # gap, surfaces as info-severity.
        return "info"
    if drift_pct is None:
        return None
    if drift_pct >= CRITICAL_THRESHOLD:
        return "critical"
    if drift_pct >= WARNING_THRESHOLD:
        return "warning"
    return None


def _compute_drift_pct(
    *,
    def14a_shares: Decimal | None,
    form4_cumulative: Decimal | None,
) -> Decimal | None:
    """Compute |def14a - form4| / def14a as a Decimal fraction.

    Returns ``None`` when:
      * Either side is NULL (cannot compute).
      * ``def14a_shares == 0`` AND ``form4_cumulative == 0`` (both
        zero is a clean reconciliation, not a divide-by-zero
        failure).
      * ``def14a_shares == 0`` while ``form4_cumulative != 0`` —
        infinite drift; the detector treats this as a critical
        finding via a pinned ``Decimal('999')`` so the severity
        classifier still fires loudly. Codex pre-push review of
        the parser caught the equivalent issue with bare ``*``
        percentages — same pattern here.
    """
    if def14a_shares is None or form4_cumulative is None:
        return None
    if def14a_shares == 0:
        if form4_cumulative == 0:
            return None
        return Decimal("999")
    diff = abs(def14a_shares - form4_cumulative)
    return diff / def14a_shares


_normalise_name = normalise_name
_resolve_holder_match = resolve_holder_to_filer
# Keep the underscore-prefixed names for in-module callers; the
# canonical implementation now lives in
# ``app.services.holder_name_resolver`` so the ownership-rollup
# service (#789) shares the same match semantics. Codex spec review
# flagged the duplication risk before #789 landed.


def _delete_alert(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    holder_name: str,
    accession_number: str,
) -> None:
    """Clear any stale alert row for a (instrument, holder,
    accession) tuple that has since reconciled cleanly. Idempotent:
    no-op when the row never existed.
    """
    conn.execute(
        """
        DELETE FROM def14a_drift_alerts
        WHERE instrument_id = %(iid)s
          AND holder_name = %(name)s
          AND accession_number = %(accession)s
        """,
        {"iid": instrument_id, "name": holder_name, "accession": accession_number},
    )


def _upsert_alert(conn: psycopg.Connection[tuple], alert: DriftAlert) -> None:
    """Idempotent INSERT — refreshes ``detected_at`` on conflict."""
    conn.execute(
        """
        INSERT INTO def14a_drift_alerts (
            instrument_id, holder_name, matched_filer_cik,
            def14a_shares, form4_cumulative, drift_pct,
            severity, accession_number, as_of_date
        ) VALUES (
            %(iid)s, %(name)s, %(cik)s,
            %(def14a)s, %(form4)s, %(drift)s,
            %(severity)s, %(accession)s, %(as_of)s
        )
        ON CONFLICT (instrument_id, holder_name, accession_number) DO UPDATE SET
            matched_filer_cik = EXCLUDED.matched_filer_cik,
            def14a_shares = EXCLUDED.def14a_shares,
            form4_cumulative = EXCLUDED.form4_cumulative,
            drift_pct = EXCLUDED.drift_pct,
            severity = EXCLUDED.severity,
            as_of_date = EXCLUDED.as_of_date,
            detected_at = NOW()
        """,
        {
            "iid": alert.instrument_id,
            "name": alert.holder_name,
            "cik": alert.matched_filer_cik,
            "def14a": alert.def14a_shares,
            "form4": alert.form4_cumulative,
            "drift": alert.drift_pct,
            "severity": alert.severity,
            "accession": alert.accession_number,
            "as_of": alert.as_of_date,
        },
    )


# ---------------------------------------------------------------------------
# Detector entry points
# ---------------------------------------------------------------------------


def _select_latest_def14a_holders(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return the latest DEF 14A holder rows per (instrument,
    holder), excluding rows with the CIK-MISSING sentinel.

    DISTINCT ON (instrument_id, holder_name) keyed on
    ``as_of_date DESC`` so re-filings or amendments use the most
    recent snapshot. Rows without numeric shares (defer-to-prior-
    cover-page entries) are skipped — there's nothing to drift-
    check.

    The optional ``instrument_id`` filter uses an
    ``%(iid)s IS NULL`` short-circuit so the SQL is a fixed literal
    string (pyright's LiteralString contract) regardless of whether
    the caller passes a scoping value.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id, holder_name)
                instrument_id, holder_name, holder_role,
                shares, accession_number, as_of_date
            FROM def14a_beneficial_holdings
            WHERE issuer_cik <> %(sentinel)s
              AND instrument_id IS NOT NULL
              AND shares IS NOT NULL
              AND (%(iid)s::BIGINT IS NULL OR instrument_id = %(iid)s::BIGINT)
            ORDER BY instrument_id, holder_name, as_of_date DESC NULLS LAST,
                     accession_number DESC
            """,
            {"sentinel": _CIK_MISSING_SENTINEL, "iid": instrument_id},
        )
        return list(cur.fetchall())


def detect_drift(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int | None = None,
) -> DriftReport:
    """Run the drift detector across DEF 14A holders.

    ``instrument_id=None`` evaluates every holder in the system;
    otherwise scopes to a single issuer (used by ad-hoc re-runs and
    PR 4's per-instrument reconciliation view).

    Caller is responsible for committing — this function writes
    via ``_upsert_alert`` but does NOT commit, so a batch
    invocation can wrap multiple runs in one transaction. Tests
    explicitly commit after each call.
    """
    holders = _select_latest_def14a_holders(conn, instrument_id=instrument_id)

    holders_evaluated = 0
    alerts_emitted = 0
    by_severity: dict[str, int] = {"info": 0, "warning": 0, "critical": 0}

    for row in holders:
        holders_evaluated += 1
        iid = int(row["instrument_id"])
        holder_name = str(row["holder_name"])
        def14a_shares: Decimal | None = row["shares"]
        accession = str(row["accession_number"])
        as_of: date | None = row["as_of_date"]

        matched, matched_cik, form4_cumulative = _resolve_holder_match(conn, instrument_id=iid, holder_name=holder_name)

        drift_pct = _compute_drift_pct(def14a_shares=def14a_shares, form4_cumulative=form4_cumulative)
        severity = _classify_severity(matched=matched, drift_pct=drift_pct)
        if severity is None:
            # Reconciliation is now clean — clear any stale alert
            # row for this (instrument, holder, accession) so a
            # since-resolved finding doesn't sit in the table
            # forever. Codex pre-push review caught the prior code
            # that silently left stale rows after the underlying
            # drift was fixed.
            _delete_alert(
                conn,
                instrument_id=iid,
                holder_name=holder_name,
                accession_number=accession,
            )
            continue

        _upsert_alert(
            conn,
            DriftAlert(
                instrument_id=iid,
                holder_name=holder_name,
                matched_filer_cik=matched_cik,
                def14a_shares=def14a_shares,
                form4_cumulative=form4_cumulative,
                drift_pct=drift_pct,
                severity=severity,
                accession_number=accession,
                as_of_date=as_of,
            ),
        )
        alerts_emitted += 1
        by_severity[severity] += 1

    return DriftReport(
        holders_evaluated=holders_evaluated,
        alerts_emitted=alerts_emitted,
        alerts_by_severity=by_severity,
    )


# ---------------------------------------------------------------------------
# Reader (exposed for the ops monitor view + ad-hoc admin queries)
# ---------------------------------------------------------------------------


def iter_alerts(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int | None = None,
    severity: str | None = None,
    limit: int = 100,
) -> Iterator[dict[str, Any]]:
    """Yield drift alert rows in detected_at-DESC order.

    ``severity`` filter accepts ``'info'`` / ``'warning'`` /
    ``'critical'`` or ``None`` for all. ``instrument_id`` scopes
    to a single issuer.
    """
    # Always pass NULL params for unspecified filters and rely on
    # ``%(param)s IS NULL`` short-circuits in the WHERE clause —
    # avoids dynamic SQL composition (which trips pyright's
    # LiteralString check) while keeping the filter logic
    # parameter-driven and SQL-injection-safe.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT alert_id, instrument_id, holder_name, matched_filer_cik,
                   def14a_shares, form4_cumulative, drift_pct, severity,
                   accession_number, as_of_date, detected_at
            FROM def14a_drift_alerts
            WHERE (%(iid)s::BIGINT IS NULL OR instrument_id = %(iid)s::BIGINT)
              AND (%(severity)s::TEXT IS NULL OR severity = %(severity)s::TEXT)
            ORDER BY detected_at DESC, alert_id DESC
            LIMIT %(limit)s
            """,
            {"iid": instrument_id, "severity": severity, "limit": limit},
        )
        for row in cur.fetchall():
            yield dict(row)
