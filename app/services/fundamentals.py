"""Fundamentals pipeline — consolidated service module.

Per the 2026-04-19 research-tool refocus §1.1 (Chunk 4), this module merges:

- fundamentals.py — FMP snapshot upserts (kept as Section 1)
- financial_facts.py — XBRL fact storage + ingestion run tracking (Section 2)
- financial_normalization.py — period derivation + canonical merge (Section 3)
- sec_incremental.py — SEC change-driven planner/executor (Section 4)

External import contract: everything previously importable from the four
retired modules is now importable from ``app.services.fundamentals``.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import psycopg

from app.providers.fundamentals import FundamentalsProvider, FundamentalsSnapshot, XbrlFact
from app.providers.implementations.sec_edgar import (
    MasterIndexEntry,
    SecFilingsProvider,
    parse_master_index,
)
from app.providers.implementations.sec_fundamentals import TRACKED_CONCEPTS
from app.services.sync_orchestrator.progress import report_progress
from app.services.watermarks import get_watermark, set_watermark

if TYPE_CHECKING:
    from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider

logger = logging.getLogger(__name__)


# ============================================================================
# Section 1: Fundamentals snapshot (was fundamentals.py)
# ============================================================================


@dataclass(frozen=True)
class FundamentalsRefreshSummary:
    symbols_attempted: int
    snapshots_upserted: int
    symbols_skipped: int  # no FMP coverage or identifier missing


def refresh_fundamentals(
    provider: FundamentalsProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbols: list[tuple[str, str]],  # [(symbol, instrument_id), ...]
) -> FundamentalsRefreshSummary:
    """
    For each symbol, fetch the latest fundamentals snapshot and upsert it.

    symbols is a list of (symbol, instrument_id) tuples. FMP uses the ticker
    symbol as its primary identifier, so no external_identifiers lookup is
    needed for FMP in v1. If the provider returns None for a symbol, that
    symbol is skipped and counted.
    """
    upserted = 0
    skipped = 0
    fresh_skipped = 0
    today = date.today()

    for symbol, instrument_id in symbols:
        if _fundamentals_are_fresh(conn, instrument_id, today):
            fresh_skipped += 1
            continue
        try:
            snap = provider.get_latest_snapshot(symbol)
            if snap is None:
                logger.info("Fundamentals: no data from provider for %s, skipping", symbol)
                skipped += 1
                continue
            _upsert_snapshot(conn, instrument_id, snap)
            upserted += 1
        except Exception:
            logger.warning("Fundamentals: failed to refresh %s, skipping", symbol, exc_info=True)
            skipped += 1

    if fresh_skipped:
        logger.info(
            "Fundamentals freshness skip: %d/%d instruments already current-quarter",
            fresh_skipped,
            len(symbols),
        )

    return FundamentalsRefreshSummary(
        symbols_attempted=len(symbols),
        snapshots_upserted=upserted,
        symbols_skipped=skipped,
    )


def refresh_fundamentals_history(
    provider: FundamentalsProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbols: list[tuple[str, str]],
    from_date: date,
    to_date: date,
    limit: int = 40,
) -> FundamentalsRefreshSummary:
    """
    Backfill historical fundamentals snapshots for each symbol.

    Each snapshot is upserted idempotently. Useful for initial population
    and for catching up after provider outages.
    """
    upserted = 0
    skipped = 0

    for symbol, instrument_id in symbols:
        try:
            snaps = provider.get_snapshot_history(symbol, from_date, to_date, limit=limit)
            if not snaps:
                logger.info("Fundamentals history: no data for %s in range, skipping", symbol)
                skipped += 1
                continue
            with conn.transaction():
                for snap in snaps:
                    _upsert_snapshot(conn, instrument_id, snap)
            # Count only after the transaction commits successfully
            upserted += len(snaps)
        except Exception:
            logger.warning("Fundamentals history: failed to refresh %s, skipping", symbol, exc_info=True)
            skipped += 1

    return FundamentalsRefreshSummary(
        symbols_attempted=len(symbols),
        snapshots_upserted=upserted,
        symbols_skipped=skipped,
    )


def _current_quarter_start(today: date) -> date:
    """Return the first day of the current calendar quarter."""
    quarter_month = ((today.month - 1) // 3) * 3 + 1
    return date(today.year, quarter_month, 1)


def _fundamentals_are_fresh(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    today: date,
) -> bool:
    """Return True if fundamentals_snapshot has a row with as_of_date in the
    current calendar quarter.  Fundamentals update quarterly — daily re-fetch
    for an instrument that already has current-quarter data is pure waste.
    """
    quarter_start = _current_quarter_start(today)
    row = conn.execute(
        """
        SELECT 1
        FROM fundamentals_snapshot
        WHERE instrument_id = %(instrument_id)s
          AND as_of_date >= %(quarter_start)s
        LIMIT 1
        """,
        {"instrument_id": instrument_id, "quarter_start": quarter_start},
    ).fetchone()
    return row is not None


def _upsert_snapshot(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    snap: FundamentalsSnapshot,
) -> None:
    """
    Upsert a single fundamentals snapshot into fundamentals_snapshot.
    Idempotent — keyed on (instrument_id, as_of_date).
    """
    conn.execute(
        """
        INSERT INTO fundamentals_snapshot (
            instrument_id, as_of_date,
            revenue_ttm, gross_margin, operating_margin,
            fcf, cash, debt, net_debt,
            shares_outstanding, book_value, eps
        )
        VALUES (
            %(instrument_id)s, %(as_of_date)s,
            %(revenue_ttm)s, %(gross_margin)s, %(operating_margin)s,
            %(fcf)s, %(cash)s, %(debt)s, %(net_debt)s,
            %(shares_outstanding)s, %(book_value)s, %(eps)s
        )
        ON CONFLICT (instrument_id, as_of_date) DO UPDATE SET
            revenue_ttm       = EXCLUDED.revenue_ttm,
            gross_margin      = EXCLUDED.gross_margin,
            operating_margin  = EXCLUDED.operating_margin,
            fcf               = EXCLUDED.fcf,
            cash              = EXCLUDED.cash,
            debt              = EXCLUDED.debt,
            net_debt          = EXCLUDED.net_debt,
            shares_outstanding = EXCLUDED.shares_outstanding,
            book_value        = EXCLUDED.book_value,
            eps               = EXCLUDED.eps
        WHERE (
            fundamentals_snapshot.revenue_ttm      IS DISTINCT FROM EXCLUDED.revenue_ttm      OR
            fundamentals_snapshot.gross_margin     IS DISTINCT FROM EXCLUDED.gross_margin     OR
            fundamentals_snapshot.operating_margin IS DISTINCT FROM EXCLUDED.operating_margin OR
            fundamentals_snapshot.fcf              IS DISTINCT FROM EXCLUDED.fcf              OR
            fundamentals_snapshot.cash             IS DISTINCT FROM EXCLUDED.cash             OR
            fundamentals_snapshot.debt             IS DISTINCT FROM EXCLUDED.debt             OR
            fundamentals_snapshot.net_debt         IS DISTINCT FROM EXCLUDED.net_debt         OR
            fundamentals_snapshot.shares_outstanding IS DISTINCT FROM EXCLUDED.shares_outstanding OR
            fundamentals_snapshot.book_value       IS DISTINCT FROM EXCLUDED.book_value       OR
            fundamentals_snapshot.eps              IS DISTINCT FROM EXCLUDED.eps
        )
        """,
        {
            "instrument_id": instrument_id,
            "as_of_date": snap.as_of_date,
            "revenue_ttm": snap.revenue_ttm,
            "gross_margin": snap.gross_margin,
            "operating_margin": snap.operating_margin,
            "fcf": snap.fcf,
            "cash": snap.cash,
            "debt": snap.debt,
            "net_debt": snap.net_debt,
            "shares_outstanding": snap.shares_outstanding,
            "book_value": snap.book_value,
            "eps": snap.eps,
        },
    )


# ============================================================================
# Section 2: XBRL fact storage + ingestion runs (was financial_facts.py)
# ============================================================================


@dataclass(frozen=True)
class FactsRefreshSummary:
    symbols_attempted: int
    facts_upserted: int
    facts_skipped: int
    symbols_failed: int


def start_ingestion_run(
    conn: psycopg.Connection[tuple],
    *,
    source: str,
    endpoint: str | None = None,
    instrument_count: int | None = None,
) -> int:
    """Insert a new data_ingestion_runs row with status='running'. Returns the run ID."""
    cur = conn.execute(
        """
        INSERT INTO data_ingestion_runs (source, endpoint, instrument_count)
        VALUES (%(source)s, %(endpoint)s, %(instrument_count)s)
        RETURNING ingestion_run_id
        """,
        {"source": source, "endpoint": endpoint, "instrument_count": instrument_count},
    )
    row = cur.fetchone()
    assert row is not None
    return row[0]


def finish_ingestion_run(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    status: str,
    rows_upserted: int = 0,
    rows_skipped: int = 0,
    error: str | None = None,
) -> None:
    """Update an ingestion run with final status and counts."""
    conn.execute(
        """
        UPDATE data_ingestion_runs
        SET finished_at = NOW(),
            status = %(status)s,
            rows_upserted = %(rows_upserted)s,
            rows_skipped = %(rows_skipped)s,
            error = %(error)s
        WHERE ingestion_run_id = %(run_id)s
        """,
        {
            "run_id": run_id,
            "status": status,
            "rows_upserted": rows_upserted,
            "rows_skipped": rows_skipped,
            "error": error,
        },
    )


_UPSERT_FACT_SQL = """
INSERT INTO financial_facts_raw (
    instrument_id, taxonomy, concept, unit,
    period_start, period_end, val, frame,
    accession_number, form_type, filed_date,
    fiscal_year, fiscal_period, decimals,
    ingestion_run_id
) VALUES (
    %(instrument_id)s, %(taxonomy)s, %(concept)s, %(unit)s,
    %(period_start)s, %(period_end)s, %(val)s, %(frame)s,
    %(accession_number)s, %(form_type)s, %(filed_date)s,
    %(fiscal_year)s, %(fiscal_period)s, %(decimals)s,
    %(ingestion_run_id)s
)
ON CONFLICT (
    instrument_id, concept, unit,
    COALESCE(period_start, '0001-01-01'::date),
    period_end, accession_number
)
DO UPDATE SET
    val = EXCLUDED.val,
    frame = EXCLUDED.frame,
    form_type = EXCLUDED.form_type,
    filed_date = EXCLUDED.filed_date,
    fiscal_year = EXCLUDED.fiscal_year,
    fiscal_period = EXCLUDED.fiscal_period,
    decimals = EXCLUDED.decimals,
    ingestion_run_id = EXCLUDED.ingestion_run_id,
    fetched_at = NOW()
WHERE financial_facts_raw.val IS DISTINCT FROM EXCLUDED.val
   OR financial_facts_raw.frame IS DISTINCT FROM EXCLUDED.frame
"""


# Batch size for executemany. A 10-K carries ~10k facts; a chunk of
# 1000 keeps each round trip well under Postgres's default
# max_parameter size (65k params ÷ 15 columns ≈ 4300 rows) while
# being large enough to amortise per-round-trip latency. The ADR
# 0004 bench showed this shape ~18× faster than the prior row-loop.
_UPSERT_PAGE_SIZE = 1000


def upsert_facts_for_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    facts: Sequence[XbrlFact],
    ingestion_run_id: int,
) -> tuple[int, int]:
    """Upsert XBRL facts into ``financial_facts_raw``.

    Returns ``(upserted_count, skipped_count)``. "Upserted" means the
    row was either INSERTed fresh or UPDATEd in place because the WHERE
    guard on ``IS DISTINCT FROM`` matched a change; "skipped" means the
    ON CONFLICT fired but the WHERE filter short-circuited because the
    value was unchanged (idempotent re-upsert).

    Uses ``cur.executemany`` with a 1000-row page size — the DB path
    from ADR 0004. Same SQL as the previous row-loop shape; only the
    call shape changed, so the identity index, ON CONFLICT semantics,
    and `IS DISTINCT FROM` filter are unchanged.

    Connection-level rowcount after ``executemany`` aggregates across
    all parameter sets (psycopg3 contract), so ``upserted`` is
    ``sum(cur.rowcount)`` per chunk and ``skipped`` is
    ``len(facts) − upserted``.
    """
    if not facts:
        return 0, 0

    rows: list[dict[str, object]] = [
        {
            "instrument_id": instrument_id,
            "taxonomy": fact.taxonomy,
            "concept": fact.concept,
            "unit": fact.unit,
            "period_start": fact.period_start,
            "period_end": fact.period_end,
            "val": fact.val,
            "frame": fact.frame,
            "accession_number": fact.accession_number,
            "form_type": fact.form_type,
            "filed_date": fact.filed_date,
            "fiscal_year": fact.fiscal_year,
            "fiscal_period": fact.fiscal_period,
            "decimals": fact.decimals,
            "ingestion_run_id": ingestion_run_id,
        }
        for fact in facts
    ]

    upserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), _UPSERT_PAGE_SIZE):
            chunk = rows[start : start + _UPSERT_PAGE_SIZE]
            cur.executemany(_UPSERT_FACT_SQL, chunk)
            # rowcount == -1 means the driver/pool adapter did not
            # report a command tag. That breaks the upserted/skipped
            # accounting contract — treating it as zero would
            # silently mis-count every fact as "skipped" and
            # contaminate downstream metrics. Fail loudly so the
            # caller surfaces it as a per-CIK failure (the watermark
            # then stays at its previous value and the next run
            # retries) rather than drifting silently.
            if cur.rowcount < 0:
                raise RuntimeError(
                    "upsert_facts_for_instrument: driver returned rowcount=-1 "
                    "after executemany; unable to account for upsert/skip counts"
                )
            upserted += cur.rowcount

    skipped = len(rows) - upserted
    return upserted, skipped


def refresh_financial_facts(
    provider: SecFundamentalsProvider,
    conn: psycopg.Connection[tuple],
    symbols: Sequence[tuple[str, int, str]],
) -> FactsRefreshSummary:
    """Fetch and store XBRL facts for all given symbols.

    Parameters
    ----------
    symbols:
        List of (symbol, instrument_id, cik) tuples.
    """
    run_id = start_ingestion_run(
        conn,
        source="sec_edgar",
        endpoint="/api/xbrl/companyfacts",
        instrument_count=len(symbols),
    )

    total_upserted = 0
    total_skipped = 0
    failed = 0
    total = len(symbols)

    for idx, (symbol, instrument_id, cik) in enumerate(symbols, start=1):
        try:
            with conn.transaction():
                facts = provider.extract_facts(symbol, cik)
                if not facts:
                    logger.info("No XBRL facts for %s (CIK %s)", symbol, cik)
                    continue
                upserted, skipped = upsert_facts_for_instrument(
                    conn,
                    instrument_id=instrument_id,
                    facts=facts,
                    ingestion_run_id=run_id,
                )
                total_upserted += upserted
                total_skipped += skipped
                logger.info(
                    "SEC facts for %s: %d upserted, %d skipped",
                    symbol,
                    upserted,
                    skipped,
                )
        except Exception:
            failed += 1
            logger.exception("Failed to refresh SEC facts for %s", symbol)
        report_progress(idx, total)

    report_progress(total, total, force=True)

    status = "success" if failed == 0 else ("partial" if total_upserted > 0 else "failed")
    finish_ingestion_run(
        conn,
        run_id=run_id,
        status=status,
        rows_upserted=total_upserted,
        rows_skipped=total_skipped,
        error=f"{failed} symbols failed" if failed > 0 else None,
    )

    return FactsRefreshSummary(
        symbols_attempted=len(symbols),
        facts_upserted=total_upserted,
        facts_skipped=total_skipped,
        symbols_failed=failed,
    )


# ============================================================================
# Section 3: Period normalization (was financial_normalization.py)
# ============================================================================


@dataclass(frozen=True)
class FactRow:
    """Minimal fact representation for normalization (subset of DB columns)."""

    concept: str
    unit: str
    period_start: date | None
    period_end: date
    val: Decimal
    frame: str | None
    form_type: str
    fiscal_year: int
    fiscal_period: str
    accession_number: str
    filed_date: date


# Build reverse map: XBRL tag -> (canonical_column, priority_index)
# Lower priority_index = higher priority.
_TAG_TO_COLUMN: dict[str, tuple[str, int]] = {}
for _col_name, _tags in TRACKED_CONCEPTS.items():
    for _idx, _tag in enumerate(_tags):
        _TAG_TO_COLUMN[_tag] = (_col_name, _idx)

# Financial columns that are flow items (income/CF -- get summed in TTM).
# Balance sheet items are point-in-time (latest value used in TTM).
_FLOW_COLUMNS: frozenset[str] = frozenset(
    {
        "revenue",
        "cost_of_revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "eps_basic",
        "eps_diluted",
        "research_and_dev",
        "sga_expense",
        "depreciation_amort",
        "interest_expense",
        "income_tax",
        "sbc_expense",
        "operating_cf",
        "investing_cf",
        "financing_cf",
        "capex",
        "dividends_paid",
        "dps_declared",
        "buyback_spend",
        # shares_basic/shares_diluted are weighted averages for a period, so they
        # belong to the period, but TTM uses latest rather than sum.
    }
)

_BALANCE_SHEET_COLUMNS: frozenset[str] = frozenset(
    {
        "total_assets",
        "total_liabilities",
        "shareholders_equity",
        "cash",
        "long_term_debt",
        "short_term_debt",
        "shares_outstanding",
        "inventory",
        "receivables",
        "payables",
        "goodwill",
        "ppe_net",
    }
)

# Fiscal period label -> (period_type, fiscal_quarter)
_FP_MAP: dict[str, tuple[str, int | None]] = {
    "Q1": ("Q1", 1),
    "Q2": ("Q2", 2),
    "Q3": ("Q3", 3),
    "Q4": ("Q4", 4),
    "FY": ("FY", None),
}


@dataclass
class PeriodRow:
    """Wide period row ready for insertion into financial_periods_raw."""

    period_end_date: date
    period_type: str
    fiscal_year: int
    fiscal_quarter: int | None
    period_start_date: date | None
    months_covered: int | None

    # Financial columns -- all optional
    revenue: Decimal | None = None
    cost_of_revenue: Decimal | None = None
    gross_profit: Decimal | None = None
    operating_income: Decimal | None = None
    net_income: Decimal | None = None
    eps_basic: Decimal | None = None
    eps_diluted: Decimal | None = None
    research_and_dev: Decimal | None = None
    sga_expense: Decimal | None = None
    depreciation_amort: Decimal | None = None
    interest_expense: Decimal | None = None
    income_tax: Decimal | None = None
    shares_basic: Decimal | None = None
    shares_diluted: Decimal | None = None
    sbc_expense: Decimal | None = None

    total_assets: Decimal | None = None
    total_liabilities: Decimal | None = None
    shareholders_equity: Decimal | None = None
    cash: Decimal | None = None
    long_term_debt: Decimal | None = None
    short_term_debt: Decimal | None = None
    shares_outstanding: Decimal | None = None
    inventory: Decimal | None = None
    receivables: Decimal | None = None
    payables: Decimal | None = None
    goodwill: Decimal | None = None
    ppe_net: Decimal | None = None

    operating_cf: Decimal | None = None
    investing_cf: Decimal | None = None
    financing_cf: Decimal | None = None
    capex: Decimal | None = None
    dividends_paid: Decimal | None = None
    dps_declared: Decimal | None = None
    buyback_spend: Decimal | None = None

    # Provenance
    source: str = "sec_edgar"
    source_ref: str = ""
    reported_currency: str = "USD"
    form_type: str | None = None
    filed_date: date | None = None
    is_restated: bool = False
    is_derived: bool = False


def _months_between(start: date | None, end: date) -> int | None:
    """Approximate months between two dates."""
    if start is None:
        return None
    delta_days = (end - start).days
    if delta_days <= 0:
        return None
    return round(delta_days / 30.44)


def _derive_periods_from_facts(
    facts: Sequence[FactRow],
    reported_currency: str = "USD",
) -> list[PeriodRow]:
    """Derive wide period rows from individual XBRL facts.

    Groups facts by (fiscal_year, fiscal_period) and merges values into
    PeriodRow objects. Uses tag priority to pick the best value when
    multiple XBRL tags map to the same canonical column.

    YTD disambiguation: only facts with a non-null ``frame`` field are
    included for duration items (income/CF). Instant items (balance sheet)
    are always included regardless of frame.

    Q4 derivation: if FY exists but Q4 does not, derives Q4 = FY - Q1 - Q2 - Q3
    for all flow columns.
    """
    # Group facts by (fiscal_year, fiscal_period)
    grouped: dict[tuple[int, str], list[FactRow]] = defaultdict(list)
    for fact in facts:
        fp = fact.fiscal_period
        if fp not in _FP_MAP:
            continue  # skip unknown periods (e.g. 'H1', '9M')

        is_instant = fact.period_start is None
        is_duration = not is_instant

        # YTD disambiguation: for duration items, require frame to be set.
        # Entries without frame are YTD cumulative -- exclude them.
        if is_duration and fact.frame is None:
            continue

        grouped[(fact.fiscal_year, fp)].append(fact)

    # Build period rows
    periods: list[PeriodRow] = []
    for (fy, fp), period_facts in grouped.items():
        period_type, fiscal_quarter = _FP_MAP[fp]

        # Determine period dates from facts
        period_end = max(f.period_end for f in period_facts)
        starts = [f.period_start for f in period_facts if f.period_start is not None]
        period_start = min(starts) if starts else None
        months = _months_between(period_start, period_end)

        # Collect accession numbers for source_ref
        accession_numbers = sorted({f.accession_number for f in period_facts})
        source_ref = accession_numbers[0] if len(accession_numbers) == 1 else ",".join(accession_numbers)

        # Find the most recent filed_date and form_type
        latest_filing = max(period_facts, key=lambda f: f.filed_date)

        row = PeriodRow(
            period_end_date=period_end,
            period_type=period_type,
            fiscal_year=fy,
            fiscal_quarter=fiscal_quarter,
            period_start_date=period_start,
            months_covered=months,
            source="sec_edgar",
            source_ref=source_ref,
            reported_currency=reported_currency,
            form_type=latest_filing.form_type,
            filed_date=latest_filing.filed_date,
        )

        # Apply values with tag priority
        # Track which columns have been set and at what priority
        col_priority: dict[str, int] = {}
        for fact in period_facts:
            mapping = _TAG_TO_COLUMN.get(fact.concept)
            if mapping is None:
                continue
            col_name, priority = mapping
            current_priority = col_priority.get(col_name)
            if current_priority is not None and priority >= current_priority:
                continue  # existing value has higher or equal priority
            setattr(row, col_name, fact.val)
            col_priority[col_name] = priority

        periods.append(row)

    # Q4 derivation: if FY exists but Q4 does not, derive Q4 = FY - Q1 - Q2 - Q3
    fy_periods = {p.fiscal_year: p for p in periods if p.period_type == "FY"}
    existing_quarters: dict[int, dict[str, PeriodRow]] = defaultdict(dict)
    for p in periods:
        if p.period_type in ("Q1", "Q2", "Q3", "Q4"):
            existing_quarters[p.fiscal_year][p.period_type] = p

    for fy_year, fy_row in fy_periods.items():
        quarters = existing_quarters.get(fy_year, {})
        if "Q4" in quarters:
            continue  # Q4 already exists
        if not all(q in quarters for q in ("Q1", "Q2", "Q3")):
            continue  # need all three quarters to derive Q4

        q1, q2, q3 = quarters["Q1"], quarters["Q2"], quarters["Q3"]

        # Determine Q4 period dates
        q3_end = q3.period_end_date
        q4_start = date(q3_end.year, q3_end.month + 1, 1) if q3_end.month < 12 else date(q3_end.year + 1, 1, 1)
        q4_end = fy_row.period_end_date

        q4 = PeriodRow(
            period_end_date=q4_end,
            period_type="Q4",
            fiscal_year=fy_year,
            fiscal_quarter=4,
            period_start_date=q4_start,
            months_covered=_months_between(q4_start, q4_end),
            source="sec_edgar",
            source_ref=fy_row.source_ref,
            reported_currency=reported_currency,
            form_type=fy_row.form_type,
            filed_date=fy_row.filed_date,
            is_derived=True,
        )

        # Derive flow columns: Q4 = FY - Q1 - Q2 - Q3
        # Only derive when FY AND all three quarters have the column (missing → skip, not zero).
        # EPS is included: while EPS isn't perfectly additive across periods with
        # changing share counts, the subtraction approximation (< 5% error typically)
        # is far better than assigning FY EPS to Q4 (which would cause TTM = Q1+Q2+Q3+FY).
        for col in _FLOW_COLUMNS:
            fy_val = getattr(fy_row, col)
            if fy_val is None:
                continue
            q1_val = getattr(q1, col)
            q2_val = getattr(q2, col)
            q3_val = getattr(q3, col)
            if q1_val is None or q2_val is None or q3_val is None:
                continue  # cannot derive — would overstate Q4
            derived = fy_val - q1_val - q2_val - q3_val
            setattr(q4, col, derived)

        # Balance sheet columns: use FY values (they're the same date as Q4 end)
        for col in _BALANCE_SHEET_COLUMNS:
            fy_val = getattr(fy_row, col)
            if fy_val is not None:
                setattr(q4, col, fy_val)

        # Weighted average shares: use FY values for Q4 (approximate)
        for col in ("shares_basic", "shares_diluted"):
            fy_val = getattr(fy_row, col)
            if fy_val is not None:
                setattr(q4, col, fy_val)

        periods.append(q4)

    return periods


def _upsert_period_raw(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period: PeriodRow,
    ingestion_run_id: int | None = None,
) -> bool:
    """Upsert a single period row into financial_periods_raw.

    Returns True if a row was inserted/updated, False if skipped (unchanged).
    """
    cur = conn.execute(
        """
        INSERT INTO financial_periods_raw (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter, period_start_date, months_covered,
            revenue, cost_of_revenue, gross_profit, operating_income,
            net_income, eps_basic, eps_diluted, research_and_dev,
            sga_expense, depreciation_amort, interest_expense, income_tax,
            shares_basic, shares_diluted, sbc_expense,
            total_assets, total_liabilities, shareholders_equity, cash,
            long_term_debt, short_term_debt, shares_outstanding,
            inventory, receivables, payables, goodwill, ppe_net,
            operating_cf, investing_cf, financing_cf, capex,
            dividends_paid, dps_declared, buyback_spend,
            source, source_ref, reported_currency,
            form_type, filed_date, is_restated, is_derived,
            ingestion_run_id
        ) VALUES (
            %(instrument_id)s, %(period_end_date)s, %(period_type)s,
            %(fiscal_year)s, %(fiscal_quarter)s, %(period_start_date)s, %(months_covered)s,
            %(revenue)s, %(cost_of_revenue)s, %(gross_profit)s, %(operating_income)s,
            %(net_income)s, %(eps_basic)s, %(eps_diluted)s, %(research_and_dev)s,
            %(sga_expense)s, %(depreciation_amort)s, %(interest_expense)s, %(income_tax)s,
            %(shares_basic)s, %(shares_diluted)s, %(sbc_expense)s,
            %(total_assets)s, %(total_liabilities)s, %(shareholders_equity)s, %(cash)s,
            %(long_term_debt)s, %(short_term_debt)s, %(shares_outstanding)s,
            %(inventory)s, %(receivables)s, %(payables)s, %(goodwill)s, %(ppe_net)s,
            %(operating_cf)s, %(investing_cf)s, %(financing_cf)s, %(capex)s,
            %(dividends_paid)s, %(dps_declared)s, %(buyback_spend)s,
            %(source)s, %(source_ref)s, %(reported_currency)s,
            %(form_type)s, %(filed_date)s, %(is_restated)s, %(is_derived)s,
            %(ingestion_run_id)s
        )
        ON CONFLICT (instrument_id, period_end_date, period_type, source, source_ref)
        DO UPDATE SET
            revenue = EXCLUDED.revenue,
            cost_of_revenue = EXCLUDED.cost_of_revenue,
            gross_profit = EXCLUDED.gross_profit,
            operating_income = EXCLUDED.operating_income,
            net_income = EXCLUDED.net_income,
            eps_basic = EXCLUDED.eps_basic,
            eps_diluted = EXCLUDED.eps_diluted,
            research_and_dev = EXCLUDED.research_and_dev,
            sga_expense = EXCLUDED.sga_expense,
            depreciation_amort = EXCLUDED.depreciation_amort,
            interest_expense = EXCLUDED.interest_expense,
            income_tax = EXCLUDED.income_tax,
            shares_basic = EXCLUDED.shares_basic,
            shares_diluted = EXCLUDED.shares_diluted,
            sbc_expense = EXCLUDED.sbc_expense,
            total_assets = EXCLUDED.total_assets,
            total_liabilities = EXCLUDED.total_liabilities,
            shareholders_equity = EXCLUDED.shareholders_equity,
            cash = EXCLUDED.cash,
            long_term_debt = EXCLUDED.long_term_debt,
            short_term_debt = EXCLUDED.short_term_debt,
            shares_outstanding = EXCLUDED.shares_outstanding,
            inventory = EXCLUDED.inventory,
            receivables = EXCLUDED.receivables,
            payables = EXCLUDED.payables,
            goodwill = EXCLUDED.goodwill,
            ppe_net = EXCLUDED.ppe_net,
            operating_cf = EXCLUDED.operating_cf,
            investing_cf = EXCLUDED.investing_cf,
            financing_cf = EXCLUDED.financing_cf,
            capex = EXCLUDED.capex,
            dividends_paid = EXCLUDED.dividends_paid,
            dps_declared = EXCLUDED.dps_declared,
            buyback_spend = EXCLUDED.buyback_spend,
            form_type = EXCLUDED.form_type,
            filed_date = EXCLUDED.filed_date,
            is_restated = EXCLUDED.is_restated,
            is_derived = EXCLUDED.is_derived,
            ingestion_run_id = EXCLUDED.ingestion_run_id,
            fetched_at = NOW()
        """,
        {
            "instrument_id": instrument_id,
            "period_end_date": period.period_end_date,
            "period_type": period.period_type,
            "fiscal_year": period.fiscal_year,
            "fiscal_quarter": period.fiscal_quarter,
            "period_start_date": period.period_start_date,
            "months_covered": period.months_covered,
            "revenue": period.revenue,
            "cost_of_revenue": period.cost_of_revenue,
            "gross_profit": period.gross_profit,
            "operating_income": period.operating_income,
            "net_income": period.net_income,
            "eps_basic": period.eps_basic,
            "eps_diluted": period.eps_diluted,
            "research_and_dev": period.research_and_dev,
            "sga_expense": period.sga_expense,
            "depreciation_amort": period.depreciation_amort,
            "interest_expense": period.interest_expense,
            "income_tax": period.income_tax,
            "shares_basic": period.shares_basic,
            "shares_diluted": period.shares_diluted,
            "sbc_expense": period.sbc_expense,
            "total_assets": period.total_assets,
            "total_liabilities": period.total_liabilities,
            "shareholders_equity": period.shareholders_equity,
            "cash": period.cash,
            "long_term_debt": period.long_term_debt,
            "short_term_debt": period.short_term_debt,
            "shares_outstanding": period.shares_outstanding,
            "inventory": period.inventory,
            "receivables": period.receivables,
            "payables": period.payables,
            "goodwill": period.goodwill,
            "ppe_net": period.ppe_net,
            "operating_cf": period.operating_cf,
            "investing_cf": period.investing_cf,
            "financing_cf": period.financing_cf,
            "capex": period.capex,
            "dividends_paid": period.dividends_paid,
            "dps_declared": period.dps_declared,
            "buyback_spend": period.buyback_spend,
            "source": period.source,
            "source_ref": period.source_ref,
            "reported_currency": period.reported_currency,
            "form_type": period.form_type,
            "filed_date": period.filed_date,
            "is_restated": period.is_restated,
            "is_derived": period.is_derived,
            "ingestion_run_id": ingestion_run_id,
        },
    )
    return cur.rowcount > 0


def _canonical_merge_instrument(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
) -> int:
    """Merge financial_periods_raw into financial_periods for one instrument.

    For each (period_end_date, period_type), picks the row from the
    highest-priority source.  Returns count of rows upserted.
    """
    cur = conn.execute(
        """
        WITH best_source AS (
            SELECT DISTINCT ON (period_end_date, period_type)
                *
            FROM financial_periods_raw
            WHERE instrument_id = %(iid)s
            ORDER BY period_end_date, period_type,
                     CASE source
                         WHEN 'sec_edgar' THEN 1
                         WHEN 'companies_house' THEN 2
                         WHEN 'fmp' THEN 3
                         ELSE 99
                     END,
                     filed_date DESC NULLS LAST
        )
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type,
            fiscal_year, fiscal_quarter, period_start_date, months_covered,
            revenue, cost_of_revenue, gross_profit, operating_income,
            net_income, eps_basic, eps_diluted, research_and_dev,
            sga_expense, depreciation_amort, interest_expense, income_tax,
            shares_basic, shares_diluted, sbc_expense,
            total_assets, total_liabilities, shareholders_equity, cash,
            long_term_debt, short_term_debt, shares_outstanding,
            inventory, receivables, payables, goodwill, ppe_net,
            operating_cf, investing_cf, financing_cf, capex,
            dividends_paid, dps_declared, buyback_spend,
            source, source_ref, reported_currency,
            form_type, filed_date, is_restated, is_derived,
            normalization_status
        )
        SELECT
            %(iid)s, period_end_date, period_type,
            fiscal_year, fiscal_quarter, period_start_date, months_covered,
            revenue, cost_of_revenue, gross_profit, operating_income,
            net_income, eps_basic, eps_diluted, research_and_dev,
            sga_expense, depreciation_amort, interest_expense, income_tax,
            shares_basic, shares_diluted, sbc_expense,
            total_assets, total_liabilities, shareholders_equity, cash,
            long_term_debt, short_term_debt, shares_outstanding,
            inventory, receivables, payables, goodwill, ppe_net,
            operating_cf, investing_cf, financing_cf, capex,
            dividends_paid, dps_declared, buyback_spend,
            source, source_ref, reported_currency,
            form_type, filed_date, is_restated, is_derived,
            'normalized'
        FROM best_source
        ON CONFLICT (instrument_id, period_end_date, period_type)
        DO UPDATE SET
            fiscal_year = EXCLUDED.fiscal_year,
            fiscal_quarter = EXCLUDED.fiscal_quarter,
            period_start_date = EXCLUDED.period_start_date,
            months_covered = EXCLUDED.months_covered,
            reported_currency = EXCLUDED.reported_currency,
            revenue = EXCLUDED.revenue,
            cost_of_revenue = EXCLUDED.cost_of_revenue,
            gross_profit = EXCLUDED.gross_profit,
            operating_income = EXCLUDED.operating_income,
            net_income = EXCLUDED.net_income,
            eps_basic = EXCLUDED.eps_basic,
            eps_diluted = EXCLUDED.eps_diluted,
            research_and_dev = EXCLUDED.research_and_dev,
            sga_expense = EXCLUDED.sga_expense,
            depreciation_amort = EXCLUDED.depreciation_amort,
            interest_expense = EXCLUDED.interest_expense,
            income_tax = EXCLUDED.income_tax,
            shares_basic = EXCLUDED.shares_basic,
            shares_diluted = EXCLUDED.shares_diluted,
            sbc_expense = EXCLUDED.sbc_expense,
            total_assets = EXCLUDED.total_assets,
            total_liabilities = EXCLUDED.total_liabilities,
            shareholders_equity = EXCLUDED.shareholders_equity,
            cash = EXCLUDED.cash,
            long_term_debt = EXCLUDED.long_term_debt,
            short_term_debt = EXCLUDED.short_term_debt,
            shares_outstanding = EXCLUDED.shares_outstanding,
            inventory = EXCLUDED.inventory,
            receivables = EXCLUDED.receivables,
            payables = EXCLUDED.payables,
            goodwill = EXCLUDED.goodwill,
            ppe_net = EXCLUDED.ppe_net,
            operating_cf = EXCLUDED.operating_cf,
            investing_cf = EXCLUDED.investing_cf,
            financing_cf = EXCLUDED.financing_cf,
            capex = EXCLUDED.capex,
            dividends_paid = EXCLUDED.dividends_paid,
            dps_declared = EXCLUDED.dps_declared,
            buyback_spend = EXCLUDED.buyback_spend,
            source = EXCLUDED.source,
            source_ref = EXCLUDED.source_ref,
            form_type = EXCLUDED.form_type,
            filed_date = EXCLUDED.filed_date,
            is_restated = EXCLUDED.is_restated,
            is_derived = EXCLUDED.is_derived,
            normalization_status = 'normalized'
        """,
        {"iid": instrument_id},
    )
    return cur.rowcount


@dataclass(frozen=True)
class NormalizationSummary:
    instruments_processed: int
    periods_raw_upserted: int
    periods_canonical_upserted: int


def normalize_financial_periods(
    conn: psycopg.Connection[tuple],
    instrument_ids: Sequence[int] | None = None,
) -> NormalizationSummary:
    """Full normalization pipeline: facts_raw -> periods_raw -> canonical.

    If ``instrument_ids`` is None, processes all instruments that have
    facts in financial_facts_raw.
    """
    # Determine which instruments to process
    if instrument_ids is None:
        cur = conn.execute("SELECT DISTINCT instrument_id FROM financial_facts_raw")
        instrument_ids = [row[0] for row in cur.fetchall()]

    total_raw = 0
    total_canonical = 0

    for iid in instrument_ids:
        try:
            with conn.transaction():
                # Step 1: Read facts for this instrument
                cur = conn.execute(
                    """
                    SELECT concept, unit, period_start, period_end, val,
                           frame, form_type, fiscal_year, fiscal_period,
                           accession_number, filed_date
                    FROM financial_facts_raw
                    WHERE instrument_id = %(iid)s
                      AND fiscal_year IS NOT NULL
                      AND fiscal_period IS NOT NULL
                    ORDER BY period_end, concept
                    """,
                    {"iid": iid},
                )
                fact_rows = [
                    FactRow(
                        concept=r[0],
                        unit=r[1],
                        period_start=r[2],
                        period_end=r[3],
                        val=r[4],
                        frame=r[5],
                        form_type=r[6],
                        fiscal_year=r[7],
                        fiscal_period=r[8],
                        accession_number=r[9],
                        filed_date=r[10],
                    )
                    for r in cur.fetchall()
                ]

                if not fact_rows:
                    continue

                # Determine reported currency (always USD for SEC)
                reported_currency = "USD"

                # Step 2: Derive periods from facts
                periods = _derive_periods_from_facts(fact_rows, reported_currency)

                # Step 3: Upsert into financial_periods_raw
                raw_count = 0
                for period in periods:
                    if _upsert_period_raw(conn, instrument_id=iid, period=period):
                        raw_count += 1
                total_raw += raw_count

                # Step 4: Canonical merge
                canonical_count = _canonical_merge_instrument(conn, iid)
                total_canonical += canonical_count

                logger.info(
                    "Normalized instrument %d: %d raw periods, %d canonical",
                    iid,
                    raw_count,
                    canonical_count,
                )
        except Exception:
            logger.exception("Failed to normalize instrument %d", iid)

    return NormalizationSummary(
        instruments_processed=len(instrument_ids),
        periods_raw_upserted=total_raw,
        periods_canonical_upserted=total_canonical,
    )


# ============================================================================
# Section 4: SEC change-driven planner/executor (was sec_incremental.py)
# ============================================================================


# 30-day rolling window covers typical outage / offline scenarios
# (weekend + holiday + a few days of developer laptop being off)
# without explicit backfill. Each day has its own watermark keyed by
# ISO date, so once a day's master-index has been committed the next
# run gets a 304 and pays only the conditional-GET round-trip. A
# 30-call burst at the 10 rps SEC cap is bounded at ~3s.
#
# Gaps longer than this window are handled by the stale-watermark
# submissions.json backfill path (issue #410) — see
# ``_stale_submission_ciks`` below.
LOOKBACK_DAYS = 30


# Per-run cap on the submissions.json backfill for stale-watermark
# CIKs (#410). This is a rare-path branch; in steady state zero CIKs
# are stale and the extra SQL query is a bounded no-op. After a long
# outage many CIKs may qualify — the cap bounds blast radius so a
# single run cannot burn the SEC rate budget on backfill alone. At
# 10 rps the SEC cap is 36,000 calls/hour, so 200 is generous without
# being reckless.
SUBMISSIONS_STALE_BACKFILL_CAP = 200

# 6-K (foreign-private-issuer interim reports) is deliberately
# excluded — typically lacks structured XBRL, so refreshing
# companyfacts on 6-K yields no new fundamentals rows.
FUNDAMENTALS_FORMS: frozenset[str] = frozenset(
    {
        "10-K",
        "10-K/A",
        "10-Q",
        "10-Q/A",
        "20-F",
        "20-F/A",
        "40-F",
        "40-F/A",
    }
)


@dataclass(frozen=True)
class RefreshPlan:
    """One run's worth of work for ``daily_financial_facts``.

    - ``seeds`` — CIKs with no prior watermark row; full backfill.
    - ``refreshes`` — CIKs that filed a fundamentals form in the window
      with an accession newer than the stored watermark.
    - ``submissions_only_advances`` — CIKs that filed a non-fundamentals
      form (e.g. 8-K). Advance ``sec.submissions`` watermark only; no
      companyfacts pull.
    - ``pending_master_index_writes`` — per-day master-index watermarks
      that the planner parsed but has NOT yet committed. The executor
      commits each one only when every covered CIK whose filing appeared
      in that day's hits has been processed successfully. A failed CIK
      leaves its day's watermark un-advanced so the next run re-fetches
      the master-index on 200, re-parses, and re-plans the failed CIK.
    - ``ciks_by_day`` — ISO-date to list-of-hit-CIKs mapping used by
      the executor to decide which pending master-index writes are safe
      to commit.
    - ``new_filings_by_cik`` — per-CIK list of master-index entries
      that landed in this cycle. Populated for every covered CIK that
      had at least one master-index hit in the 7-day window (including
      seeds that happened to file this week). The executor only
      consumes this dict on the refresh + submissions-only paths; the
      seed path ignores it because seeds need full historical backfill
      (#268 Chunk E), not just this week's entries.
    """

    seeds: list[str] = field(default_factory=list)
    # refreshes carries (cik, top_accession) so the executor reuses
    # the accession the planner already fetched — no second
    # submissions.json request per refresh CIK.
    refreshes: list[tuple[str, str]] = field(default_factory=list)
    submissions_only_advances: list[tuple[str, str]] = field(default_factory=list)
    pending_master_index_writes: list[tuple[str, str, str]] = field(default_factory=list)
    ciks_by_day: dict[str, list[str]] = field(default_factory=dict)
    new_filings_by_cik: dict[str, list[MasterIndexEntry]] = field(default_factory=dict)
    # CIKs skipped during planning itself (fetch_submissions returned None
    # or filings.recent was empty). These never make it to
    # seeds/refreshes/submissions_only_advances, so the executor's
    # ``failed`` list does not capture them — but their master-index
    # day must still withhold. Executor unions this set into its
    # commit-gate so planner-phase transient skips block the watermark
    # advance exactly like executor-phase failures do.
    failed_plan_ciks: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RefreshOutcome:
    """Per-category counters + per-CIK failure list for one run.

    ``failed`` is ``list[(cik, exception_class_name)]`` — a CIK appears
    here iff its per-CIK transaction was rolled back. Successful CIKs
    do not appear regardless of category.
    """

    seeded: int = 0
    refreshed: int = 0
    submissions_advanced: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)


def _load_covered_us_ciks(conn: psycopg.Connection[tuple]) -> list[str]:
    cur = conn.execute(
        """
        SELECT ei.identifier_value
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        ORDER BY ei.identifier_value
        """
    )
    return [row[0] for row in cur.fetchall()]


def _lookback_dates(today: date) -> list[date]:
    return [today - timedelta(days=i) for i in range(LOOKBACK_DAYS)]


def _stale_submission_ciks(
    conn: psycopg.Connection[tuple],
    *,
    covered: list[str],
    today: date,
    exclude: set[str],
    limit: int,
) -> list[str]:
    """Covered CIKs whose ``sec.submissions`` watermark has not been
    refreshed in the last ``LOOKBACK_DAYS`` days.

    Steady-state (short outage, at most a few days) returns an empty
    list — every active CIK's watermark was touched on the last run.
    After a long outage — longer than ``LOOKBACK_DAYS`` — CIKs that
    filed during the gap are silently skipped by the master-index
    window because their filings fall outside the planner's lookback.
    Those CIKs are rescued here: the backfill path fetches
    ``submissions.json`` unconditionally and diffs the top accession
    against the stored watermark, enqueuing any new filings for a
    refresh.

    - ``covered`` is the list of covered US CIKs, already loaded by
      the caller.  Passed in so we do not re-query the cohort.
    - ``exclude`` is the set of CIKs already queued as a seed or
      refresh this run — they do not need a second backfill pass.
    - ``limit`` bounds the number of CIKs returned per run.  Returns
      the oldest-watermark CIKs first so a long-outage backfill
      progresses steadily rather than rotating through the same
      hundred CIKs forever.
    """
    if not covered or limit <= 0:
        return []
    cutoff = today - timedelta(days=LOOKBACK_DAYS)
    # Parameterise the IN clause via a single ARRAY param to avoid
    # fan-out over N placeholders and to stay safe against CIK lists
    # that grow to thousands of entries.
    rows = conn.execute(
        """
        SELECT key
        FROM external_data_watermarks
        WHERE source = 'sec.submissions'
          AND fetched_at < %s
          AND key = ANY(%s)
          AND NOT (key = ANY(%s))
        ORDER BY fetched_at ASC
        LIMIT %s
        """,
        (cutoff, covered, list(exclude), limit),
    ).fetchall()
    return [str(r[0]) for r in rows]


def _top_accession_from_submissions(
    submissions: dict[str, object],
) -> str | None:
    """Return the top accession number or None for empty submissions."""
    filings_block = submissions.get("filings")
    if not isinstance(filings_block, dict):
        return None
    recent = filings_block.get("recent")
    if not isinstance(recent, dict):
        return None
    accessions = recent.get("accessionNumber") or []
    if not accessions:
        return None
    return str(accessions[0])


def plan_refresh(
    conn: psycopg.Connection[tuple],
    provider: SecFilingsProvider,
    *,
    today: date,
) -> RefreshPlan:
    """Derive the work for a single daily_financial_facts run.

    Steps:

    1. Load covered-US CIKs (tradable instruments with a primary
       ``sec.cik`` external identifier).
    2. Fetch the 7-day master-index window with conditional GET. Each
       day has its own ``sec.master-index`` watermark keyed by ISO
       date. 304 and 404 both short-circuit (no watermark write).
    3. Intersect the master-index hits with the covered cohort.
    4. For each covered CIK, compare against its ``sec.submissions``
       watermark and bucket into seeds / refreshes /
       submissions_only_advances.

    The planner is pure (no data writes except watermark rows on the
    master-index). Actual companyfacts pulls happen in Task 5's
    ``execute_refresh``.
    """
    covered = _load_covered_us_ciks(conn)
    if not covered:
        return RefreshPlan()

    master_hits_by_cik: dict[str, list[MasterIndexEntry]] = {}
    # Per-day provenance so the executor can commit master-index
    # watermarks only when every CIK hit on that day was processed.
    ciks_by_day: dict[str, set[str]] = {}
    pending_master_index_writes: list[tuple[str, str, str]] = []

    for target in _lookback_dates(today):
        wm = get_watermark(conn, "sec.master-index", target.isoformat())
        if_modified_since = wm.watermark if wm else None
        result = provider.fetch_master_index(target, if_modified_since=if_modified_since)
        if result is None:
            # 304 Not Modified OR 404 (weekend / holiday): nothing to
            # parse, and no Last-Modified to persist on 404. The 304
            # path is safe — the stored watermark is still the correct
            # ``If-Modified-Since`` for next run.
            continue

        if wm is not None and wm.response_hash == result.body_hash:
            # Body identical to the last run but without a 304 —
            # watermark + hash are unchanged so no commit is required.
            # Skip re-parsing; next run still has a valid watermark.
            continue

        entries = parse_master_index(result.body)
        day_ciks: set[str] = set()
        for entry in entries:
            master_hits_by_cik.setdefault(entry.cik, []).append(entry)
            day_ciks.add(entry.cik)

        # Capture the watermark write as pending — executor commits it
        # only if every covered CIK on this day completes successfully.
        # A mid-run failure leaves the watermark un-advanced so the next
        # run re-fetches this day's master-index (200), re-parses, and
        # re-plans the missed CIK instead of 304-skipping it forever.
        iso = target.isoformat()
        pending_master_index_writes.append((iso, result.last_modified or "", result.body_hash))
        ciks_by_day[iso] = day_ciks

    seeds: list[str] = []
    refreshes: list[tuple[str, str]] = []
    submissions_only: list[tuple[str, str]] = []
    failed_plan_ciks: list[str] = []

    covered_set = set(covered)
    # Drop hits outside the cohort before the per-CIK loop so we never
    # issue a submissions fetch for a rogue master-index entry. The
    # ``.get(cik)`` lookup below would implicitly filter anyway, but
    # an explicit intersect documents intent.
    master_hits_by_cik = {cik: entries for cik, entries in master_hits_by_cik.items() if cik in covered_set}
    # Restrict per-day cohort tracking to covered CIKs too — the
    # executor's commit-if-all-succeeded check only cares about CIKs
    # that were actually planned this run.
    ciks_by_day_filtered: dict[str, list[str]] = {iso: sorted(ciks & covered_set) for iso, ciks in ciks_by_day.items()}

    for cik in covered:
        wm = get_watermark(conn, "sec.submissions", cik)
        if wm is None:
            seeds.append(cik)
            continue

        entries = master_hits_by_cik.get(cik)
        if not entries:
            continue

        submissions = provider.fetch_submissions(cik)
        if submissions is None:
            # Transient planner-phase skip — feed into failed_plan_ciks
            # so the executor's commit-gate withholds this day's
            # master-index watermark. Without this, the day would
            # commit, the next run would 304, and this CIK would be
            # permanently skipped.
            logger.warning(
                "plan_refresh: fetch_submissions returned None for cik=%s "
                "despite master-index hit — withholding master-index watermark",
                cik,
            )
            failed_plan_ciks.append(cik)
            continue
        top_accession = _top_accession_from_submissions(submissions)
        if top_accession is None:
            logger.warning(
                "plan_refresh: submissions.json for cik=%s has empty filings.recent "
                "despite master-index hit — withholding master-index watermark",
                cik,
            )
            failed_plan_ciks.append(cik)
            continue
        if top_accession == wm.watermark:
            # Amendment or re-listing of a filing we already have.
            continue

        hit_forms = {e.form_type for e in entries}
        if hit_forms & FUNDAMENTALS_FORMS:
            refreshes.append((cik, top_accession))
        else:
            submissions_only.append((cik, top_accession))

    # --- Stale-watermark backfill (#410) ------------------------------
    # Covered CIKs whose sec.submissions watermark is older than
    # LOOKBACK_DAYS AND that did not hit the master-index this window
    # would otherwise be silently skipped — they filed during a gap
    # longer than the window. Rescue them by fetching submissions.json
    # unconditionally and comparing top_accession against the stored
    # watermark. Rare-path branch: in steady state this SQL returns
    # zero rows and the extra work is bounded.
    #
    # Exclude every CIK the main loop already inspected — ``seeds``
    # covers CIKs with no watermark, and ``master_hits_by_cik`` covers
    # CIKs that went through the main-loop submissions.json lookup
    # (whether that produced a refresh, a submissions-only advance, a
    # failed-plan flag, or the "accession unchanged" no-op). Either
    # way, the main loop has already fetched submissions.json for them
    # this run; the backfill path must not double-fetch.
    already_handled = set(seeds) | set(master_hits_by_cik.keys())
    stale_ciks = _stale_submission_ciks(
        conn,
        covered=covered,
        today=today,
        exclude=already_handled,
        limit=SUBMISSIONS_STALE_BACKFILL_CAP,
    )
    if stale_ciks:
        logger.info(
            "plan_refresh: stale-watermark backfill for %d CIK(s) (cap=%d)",
            len(stale_ciks),
            SUBMISSIONS_STALE_BACKFILL_CAP,
        )
    for cik in stale_ciks:
        wm = get_watermark(conn, "sec.submissions", cik)
        if wm is None:
            # Belt-and-braces: _stale_submission_ciks guarantees this
            # CIK has a watermark row; a concurrent delete between the
            # SELECT and this lookup would fall here. Treat as seed so
            # the executor runs a full backfill.
            seeds.append(cik)
            continue
        submissions = provider.fetch_submissions(cik)
        if submissions is None:
            # Transient. Same invariant as the main loop: withhold the
            # master-index watermark by flagging this CIK so the next
            # run retries.
            logger.warning(
                "plan_refresh: backfill fetch_submissions returned None for cik=%s — will retry next run",
                cik,
            )
            failed_plan_ciks.append(cik)
            continue
        top_accession = _top_accession_from_submissions(submissions)
        if top_accession is None:
            logger.warning(
                "plan_refresh: backfill submissions.json for cik=%s has empty filings.recent",
                cik,
            )
            failed_plan_ciks.append(cik)
            continue
        if top_accession == wm.watermark:
            # No new filings since the last watermark — this CIK has
            # genuinely been idle during the outage. Advance
            # ``fetched_at`` so the ``ORDER BY fetched_at ASC LIMIT``
            # cap can make forward progress: without this, the same
            # oldest-fetched CIKs would monopolise the cap on every
            # run and newer stale CIKs would starve indefinitely. The
            # UPDATE is idempotent and touches a single row.
            conn.execute(
                "UPDATE external_data_watermarks SET fetched_at = NOW() WHERE source = 'sec.submissions' AND key = %s",
                (cik,),
            )
            continue
        # A new filing arrived during the outage. Enqueue as refresh —
        # the executor will fetch companyfacts and advance both
        # watermarks atomically.
        refreshes.append((cik, top_accession))

    return RefreshPlan(
        seeds=sorted(seeds),
        refreshes=sorted(refreshes),
        submissions_only_advances=sorted(submissions_only),
        pending_master_index_writes=pending_master_index_writes,
        ciks_by_day=ciks_by_day_filtered,
        failed_plan_ciks=sorted(failed_plan_ciks),
        # master_hits_by_cik has already been intersected with the
        # covered cohort above; pass it through so the executor can
        # upsert each hit into filing_events.
        new_filings_by_cik=master_hits_by_cik,
    )


def _instrument_for_cik(
    conn: psycopg.Connection[tuple],
    cik: str,
) -> tuple[int, str] | None:
    """Resolve a CIK to (instrument_id, symbol) via external_identifiers.

    Returns None if no tradable instrument has a primary sec.cik
    identifier for this CIK. A non-None result guarantees the
    instrument is currently tradable.
    """
    row = conn.execute(
        """
        SELECT i.instrument_id, i.symbol
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.identifier_value = %s
            AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        """,
        (cik,),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), str(row[1])


def _upsert_filing_from_master_index(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    entry: MasterIndexEntry,
    symbol: str,
) -> None:
    """Upsert a filing_events row from a master-index entry.

    Distinct from ``filings._upsert_filing`` on the ON CONFLICT path:
    when the row already exists, we DO NOT overwrite ``primary_document_url``
    or ``source_url``. Master-index only carries the generic
    ``{accession}-index.htm`` landing page, whereas the submissions-
    based ingest (``daily_research_refresh``) stores the specific
    primary document (e.g. ``aapl-20260330.htm``). A master-index
    upsert arriving after the richer ingest must not downgrade the URL.
    COALESCE preserves the existing value unless it is NULL.

    ``filing_date`` and ``filing_type`` still refresh on conflict —
    both are authoritative from either source and carry no loss-of-
    detail risk.
    """
    accession_no_dashes = entry.accession_number.replace("-", "")
    cik_int = int(entry.cik)
    master_index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_no_dashes}/{entry.accession_number}-index.htm"
    )
    try:
        filed_at = datetime.fromisoformat(entry.date_filed).replace(tzinfo=UTC)
    except ValueError:
        # Master-index dates are always ISO; ValueError here indicates
        # corrupt data. Log loudly so operators can investigate rather
        # than silently substituting now().
        logger.warning(
            "sec_incremental: malformed date_filed %r for accession %s (cik=%s) — "
            "falling back to now() so upsert proceeds",
            entry.date_filed,
            entry.accession_number,
            entry.cik,
        )
        filed_at = datetime.now(UTC)
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id, source_url, primary_document_url,
            raw_payload_json
        )
        VALUES (
            %(instrument_id)s, %(filing_date)s, %(filing_type)s,
            %(provider)s, %(provider_filing_id)s, %(source_url)s, %(primary_document_url)s,
            %(raw_payload_json)s
        )
        ON CONFLICT (provider, provider_filing_id) DO UPDATE SET
            filing_date          = EXCLUDED.filing_date,
            filing_type          = EXCLUDED.filing_type,
            source_url           = COALESCE(filing_events.source_url, EXCLUDED.source_url),
            primary_document_url = COALESCE(filing_events.primary_document_url, EXCLUDED.primary_document_url)
        """,
        {
            "instrument_id": instrument_id,
            "filing_date": filed_at.date(),
            "filing_type": entry.form_type,
            "provider": "sec",
            "provider_filing_id": entry.accession_number,
            "source_url": master_index_url,
            "primary_document_url": master_index_url,
            "raw_payload_json": json.dumps(
                {
                    "source": "master-index",
                    "provider_filing_id": entry.accession_number,
                    "symbol": symbol,
                    "filed_at": filed_at.isoformat(),
                    "filing_type": entry.form_type,
                    "company_name": entry.company_name,
                    "date_filed": entry.date_filed,
                }
            ),
        },
    )


def _run_cik_upsert(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    filings_provider: SecFilingsProvider,
    fundamentals_provider: SecFundamentalsProvider,
    run_id: int,
    failed: list[tuple[str, str]],
    known_top_accession: str | None = None,
    new_filings: list[MasterIndexEntry] | None = None,
) -> int | None:
    """Per-CIK seed/refresh body.

    ``known_top_accession`` lets callers pass an accession the planner
    already fetched — avoids a second ``fetch_submissions`` call on the
    refresh path (planner fetches to decide refresh vs submissions-only;
    executor would otherwise re-fetch to read the top accession again).
    Seeds pass ``None`` because the planner has no prior watermark and
    doesn't call ``fetch_submissions`` for them.

    Returns the number of fact rows upserted on success (``int >= 0``)
    or ``None`` on skip or failure. Failures additionally append
    ``(cik, ExceptionName)`` to ``failed``; skips do not.

    All writes for one CIK happen inside one ``with conn.transaction()``
    block so on exception the facts upsert AND both watermark writes
    roll back together — watermarks never drift ahead of data.

    Emits a single ``fundamentals.cik_timing`` log line per invocation
    (success OR failure) carrying wall-clock seconds, facts_upserted,
    and seed-vs-refresh mode. This is the observability signal required
    by issue #418 — per-CIK timing isolates whether the Shape-B DB-path
    fix (ADR 0004) landed in production. ``data_ingestion_runs`` today
    is per provider batch, not per CIK, so log parsing is the smallest
    surface that answers the question without a schema change.
    """
    started = time.perf_counter()
    mode = "refresh" if known_top_accession is not None else "seed"
    facts_upserted = 0
    outcome = "unknown"

    try:
        inst = _instrument_for_cik(conn, cik)
        if inst is None:
            # Plan-time drift: CIK was covered during planning but no
            # longer resolves to a tradable instrument. Record as a
            # failure so the master-index watermark for this CIK's day
            # is WITHHELD — a future run re-checks after universe
            # reconciliation rather than 304-skipping forever.
            logger.warning(
                "sec_incremental: no tradable instrument found for cik=%s (plan drift?)",
                cik,
            )
            failed.append((cik, "InstrumentMissing"))
            outcome = "skip_instrument_missing"
            return None
        instrument_id, symbol = inst
        # Close the implicit read transaction opened by _instrument_for_cik
        # before the HTTP calls below so the session is not idle-in-
        # transaction for multi-second windows × hundreds of CIKs.
        conn.commit()

        # Skip the second fetch_submissions round-trip if the planner
        # already captured the top accession (refresh / submissions-only
        # paths). Seeds have no prior watermark, so the planner never
        # fetched for them — executor still fetches once.
        if known_top_accession is not None:
            top_accession: str | None = known_top_accession
        else:
            submissions = filings_provider.fetch_submissions(cik)
            if submissions is None:
                # Transient: submissions endpoint unavailable (404 on a
                # CIK the master-index says filed today — private /
                # de-registered issuer, or a provider glitch). Record as
                # failure so the master-index watermark for this day is
                # NOT committed and the next run re-fetches + re-plans.
                logger.warning(
                    "sec_incremental: no submissions.json for cik=%s (private/de-registered?)",
                    cik,
                )
                failed.append((cik, "SubmissionsMissing"))
                outcome = "skip_submissions_missing"
                return None
            top_accession = _top_accession_from_submissions(submissions)
            if top_accession is None:
                # Transient: submissions.json returned but filings.recent
                # is empty despite a master-index hit. Same invariant —
                # withhold the master-index watermark so next run retries.
                logger.warning(
                    "sec_incremental: submissions.json for cik=%s has empty filings.recent",
                    cik,
                )
                failed.append((cik, "EmptyFilingsRecent"))
                outcome = "skip_empty_filings"
                return None
            # #427: extract rich entity metadata from the submissions
            # dict we already have in memory. Zero extra HTTP. Only
            # happens when the executor fetches submissions itself
            # (seed path + first-time refresh) — on the refresh-via-
            # planner path the dict is thrown away before we get here,
            # which is acceptable because entity metadata (description,
            # SIC, exchanges, former names) changes rarely; any stale
            # row converges on the next seed cycle.
            try:
                from app.services.sec_entity_profile import (
                    parse_entity_profile,
                    upsert_entity_profile,
                )

                profile = parse_entity_profile(
                    submissions,
                    instrument_id=instrument_id,
                    cik=cik,
                )
                upsert_entity_profile(conn, profile)
            except Exception:
                logger.warning(
                    "sec_incremental: entity-profile upsert failed for cik=%s",
                    cik,
                    exc_info=True,
                )

        facts = fundamentals_provider.extract_facts(symbol, cik)
        upserted_in_tx = 0

        with conn.transaction():
            if facts:
                upserted, _skipped = upsert_facts_for_instrument(
                    conn,
                    instrument_id=instrument_id,
                    facts=facts,
                    ingestion_run_id=run_id,
                )
                upserted_in_tx = upserted
            # Upsert each master-index entry for this CIK into
            # filing_events so downstream event-driven triggers
            # (#273 thesis, #276 cascade) have a timestamped signal.
            # Idempotent: ON CONFLICT preserves richer URLs stored by
            # the submissions-based ingest path. Atomic with the facts
            # upsert and watermark writes below.
            if new_filings:
                for entry in new_filings:
                    _upsert_filing_from_master_index(
                        conn,
                        instrument_id=instrument_id,
                        entry=entry,
                        symbol=symbol,
                    )
            set_watermark(
                conn,
                source="sec.submissions",
                key=cik,
                watermark=top_accession,
            )
            set_watermark(
                conn,
                source="sec.companyfacts",
                key=cik,
                watermark=top_accession,
            )
        conn.commit()
        # Only credit facts_upserted *after* the watermark writes and
        # the commit have succeeded — if any step inside the
        # transaction block raises, Postgres rolls back the fact
        # upserts with it and the timing log must report 0 (not the
        # count that was never actually committed).
        facts_upserted = upserted_in_tx
        outcome = "success"
        return facts_upserted
    except Exception as exc:
        # ``with conn.transaction()`` already rolled back on exception;
        # the explicit rollback here covers the pre-transaction path
        # (fetch_submissions raising, extract_facts raising) where no
        # transaction block had been entered yet.
        try:
            conn.rollback()
        except psycopg.Error:
            logger.debug("rollback suppressed after executor exception", exc_info=True)
        failed.append((cik, type(exc).__name__))
        logger.exception("sec_incremental per-CIK upsert failed for cik=%s", cik)
        outcome = f"error_{type(exc).__name__}"
        return None
    finally:
        # Structured per-CIK timing log — required by #418 so
        # production ratios can be validated against the ADR 0004 bench
        # (Shape B was ~18x faster on the DB-path bench). Emits on
        # every exit path (success, skip, exception). Log keys are
        # machine-parseable so grep-based alerting still works even if
        # the persist path below fails.
        finished_ts = time.perf_counter()
        elapsed = finished_ts - started
        logger.info(
            "fundamentals.cik_timing cik=%s mode=%s outcome=%s facts_upserted=%d seconds=%.3f",
            cik,
            mode,
            outcome,
            facts_upserted,
            elapsed,
        )
        # Persist the timing row (#418 acceptance: p50/p95 per-CIK
        # timing surfaced in the admin UI without tailing logs). Writes
        # on the caller's connection inside its own transaction block;
        # by this point both the exception path and the success path
        # have called commit/rollback, so the connection is in a clean
        # state. Skip-path exits (no run body) pass ``run_id=None``
        # into a NULL FK; the DDL allows NULL + ON DELETE SET NULL for
        # safe ``data_ingestion_runs`` pruning.
        try:
            # Close any outstanding implicit read transaction (e.g.
            # the one opened by ``_instrument_for_cik`` on the
            # skip-path early returns) so ``conn.transaction()`` in
            # ``persist_cik_timing`` opens a real BEGIN/COMMIT pair
            # rather than nesting a savepoint that the caller's
            # later ``conn.rollback()`` would drop. Safe — by this
            # point the per-CIK success/error branches have already
            # committed or rolled back their own work, and a
            # rollback on a clean session is a no-op.
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug("pre-timing rollback suppressed", exc_info=True)
            persist_cik_timing(
                conn,
                cik=cik,
                ingestion_run_id=run_id,
                mode=mode,
                outcome=outcome,
                facts_upserted=facts_upserted,
                seconds=elapsed,
                started_at=_utc_from_perf(started),
                finished_at=_utc_from_perf(finished_ts),
            )
        except Exception:
            logger.warning("fundamentals.cik_timing persist failed", exc_info=True)


def _utc_from_perf(perf: float) -> datetime:
    """Convert ``time.perf_counter`` timestamp to a wall-clock UTC
    datetime by anchoring on ``datetime.now(tz=UTC)`` minus the
    perf-counter delta. Used only for the cik_upsert_timing audit row
    — elapsed seconds remain the source of truth for duration.
    """
    return datetime.now(tz=UTC) - timedelta(seconds=time.perf_counter() - perf)


def persist_cik_timing(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    ingestion_run_id: int | None,
    mode: str,
    outcome: str,
    facts_upserted: int,
    seconds: float,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Insert one ``cik_upsert_timing`` row on the caller-supplied
    connection. Uses its own transaction block so the write commits
    independently of whatever the surrounding logic does next.
    """
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO cik_upsert_timing (
                ingestion_run_id, cik, mode, outcome,
                facts_upserted, seconds, started_at, finished_at
            ) VALUES (
                %(run_id)s, %(cik)s, %(mode)s, %(outcome)s,
                %(facts)s, %(seconds)s, %(started)s, %(finished)s
            )
            """,
            {
                "run_id": ingestion_run_id,
                "cik": cik,
                "mode": mode,
                "outcome": outcome,
                "facts": facts_upserted,
                "seconds": round(seconds, 3),
                "started": started_at,
                "finished": finished_at,
            },
        )


def execute_refresh(
    conn: psycopg.Connection[tuple],
    *,
    filings_provider: SecFilingsProvider,
    fundamentals_provider: SecFundamentalsProvider,
    plan: RefreshPlan,
) -> RefreshOutcome:
    """Execute a RefreshPlan against the database.

    Per-CIK isolation: each CIK's facts upsert + both watermark
    advances run inside a single ``with conn.transaction()`` block and
    commit atomically or roll back together. A per-CIK failure
    records the exception class name in ``RefreshOutcome.failed`` and
    continues — one bad CIK never aborts the layer. After each CIK's
    block we call ``conn.commit()`` so progress survives a later
    crash.

    The ``submissions_only_advances`` path skips both the submissions
    fetch AND the companyfacts fetch — the planner already decided
    that path is correct for 8-K-style hits where XBRL facts would
    not change.
    """
    total = len(plan.seeds) + len(plan.refreshes) + len(plan.submissions_only_advances)
    if total == 0:
        return RefreshOutcome()

    run_id = start_ingestion_run(
        conn,
        source="sec_edgar",
        endpoint="/api/xbrl/companyfacts",
        instrument_count=total,
    )
    conn.commit()

    seeded = 0
    refreshed = 0
    submissions_advanced = 0
    facts_upserted_total = 0
    failed: list[tuple[str, str]] = []
    done = 0
    catastrophic_error: str | None = None

    try:
        # Seeds + refreshes share one per-CIK body. _run_cik_upsert
        # returns the fact-row count (int >= 0) on success, or None
        # on skip / failure. Failures additionally append to `failed`.
        #
        # Seeds deliberately do NOT pass ``new_filings`` even if the
        # CIK has entries in ``plan.new_filings_by_cik`` — seeds need
        # full historical backfill (#268 Chunk E), not just this
        # cycle's master-index hits. Writing only this week's filings
        # for a seed would give downstream event triggers a misleading
        # signal ("look, a filing landed") when the instrument still
        # lacks most of its history. Chunk E owns the seed-time
        # filing_events population.
        for cik in plan.seeds:
            done += 1
            upserted = _run_cik_upsert(
                conn,
                cik=cik,
                filings_provider=filings_provider,
                fundamentals_provider=fundamentals_provider,
                run_id=run_id,
                failed=failed,
            )
            if upserted is not None:
                seeded += 1
                facts_upserted_total += upserted
            report_progress(done, total)

        for cik, top_accession in plan.refreshes:
            done += 1
            upserted = _run_cik_upsert(
                conn,
                cik=cik,
                filings_provider=filings_provider,
                fundamentals_provider=fundamentals_provider,
                run_id=run_id,
                failed=failed,
                known_top_accession=top_accession,
                new_filings=plan.new_filings_by_cik.get(cik),
            )
            if upserted is not None:
                refreshed += 1
                facts_upserted_total += upserted
            report_progress(done, total)

        for cik, accession in plan.submissions_only_advances:
            done += 1
            try:
                inst = _instrument_for_cik(conn, cik)
                conn.commit()  # close implicit read tx from the SELECT
                if inst is None:
                    # Plan-drift: CIK fell out of the tradable-with-SEC
                    # cohort between planning and execution. Record as
                    # failed so the master-index watermark for this
                    # day is withheld.
                    logger.warning(
                        "sec_incremental: submissions-only path — no tradable instrument for cik=%s (plan drift?)",
                        cik,
                    )
                    failed.append((cik, "InstrumentMissing"))
                    report_progress(done, total)
                    continue
                instrument_id, symbol = inst
                new_filings = plan.new_filings_by_cik.get(cik)
                with conn.transaction():
                    # Upsert filing_events for each master-index entry
                    # on this CIK so the 8-K (or similar) is visible to
                    # downstream event-driven triggers, even though we
                    # don't fetch companyfacts.
                    if new_filings:
                        for entry in new_filings:
                            _upsert_filing_from_master_index(
                                conn,
                                instrument_id=instrument_id,
                                entry=entry,
                                symbol=symbol,
                            )
                    set_watermark(
                        conn,
                        source="sec.submissions",
                        key=cik,
                        watermark=accession,
                    )
                conn.commit()
                submissions_advanced += 1
            except Exception as exc:
                try:
                    conn.rollback()
                except psycopg.Error:
                    logger.debug("rollback suppressed after executor exception", exc_info=True)
                failed.append((cik, type(exc).__name__))
                logger.exception(
                    "sec_incremental submissions-only advance failed for cik=%s",
                    cik,
                )
            report_progress(done, total)

        report_progress(done, total, force=True)

        # Commit pending master-index watermarks ONLY for days where
        # every covered CIK that appeared in that day's hits was
        # processed without failure. A failed CIK leaves its day's
        # watermark un-advanced so the next run re-fetches that day's
        # master-index on 200, re-parses, and re-plans the failed CIK
        # instead of 304-skipping it forever.
        # Union executor-phase failures with planner-phase skips so
        # both sources withhold the master-index watermark for their day.
        failed_ciks = {cik for cik, _ in failed} | set(plan.failed_plan_ciks)
        for iso_date, last_modified, body_hash in plan.pending_master_index_writes:
            day_ciks = set(plan.ciks_by_day.get(iso_date, []))
            if day_ciks & failed_ciks:
                logger.info(
                    "sec_incremental: withholding master-index watermark for %s due to failed CIKs in its hit set (%s)",
                    iso_date,
                    sorted(day_ciks & failed_ciks),
                )
                continue
            try:
                with conn.transaction():
                    set_watermark(
                        conn,
                        source="sec.master-index",
                        key=iso_date,
                        watermark=last_modified,
                        response_hash=body_hash,
                    )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except psycopg.Error:
                    logger.debug(
                        "rollback suppressed after master-index watermark commit failure",
                        exc_info=True,
                    )
                logger.exception(
                    "sec_incremental: master-index watermark commit failed for %s",
                    iso_date,
                )
    except Exception as exc:
        # Non-per-CIK failure escaped (per-CIK exceptions are caught
        # inside _run_cik_upsert and the submissions-only try block).
        # Typical triggers: DB connection drop, unhandled programming
        # error. Record it so the audit trail still has a terminal
        # status instead of orphaning the run row in 'running'.
        catastrophic_error = f"{type(exc).__name__}: {exc}"
        logger.exception("sec_incremental: catastrophic failure in execute_refresh")
        raise
    finally:
        # Always record a terminal status — required by the audit
        # non-negotiable (settled-decisions.md Auditability).
        progressed = seeded + refreshed + submissions_advanced
        if catastrophic_error is not None:
            status = "failed"
            error_msg: str | None = catastrophic_error
        elif failed and progressed == 0:
            status = "failed"
            error_msg = f"{len(failed)} CIKs failed"
        elif failed:
            status = "partial"
            error_msg = f"{len(failed)} CIKs failed"
        else:
            status = "success"
            error_msg = None
        try:
            # Clear any aborted transaction state left over from a
            # catastrophic psycopg error before the audit write — an
            # InFailedSqlTransaction on the next execute would orphan
            # the run row. Rollback on a clean connection is a no-op.
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug("pre-finish rollback suppressed", exc_info=True)
            finish_ingestion_run(
                conn,
                run_id=run_id,
                status=status,
                rows_upserted=facts_upserted_total,
                error=error_msg,
            )
            conn.commit()
        except Exception:
            # Roll back the aborted tx so the next caller gets a clean
            # session, and log regardless of outcome.
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug(
                    "rollback after finish_ingestion_run failure suppressed",
                    exc_info=True,
                )
            logger.exception("sec_incremental: finish_ingestion_run failed")
            # On a clean run path (no catastrophic exception already
            # being re-raised) we MUST surface the audit failure so the
            # scheduler's _tracked_job marks the job failed. Swallowing
            # here would report job success despite an orphaned run row.
            # On the catastrophic path the original exception is already
            # re-raised by the `except` above; don't mask it.
            if catastrophic_error is None:
                raise

    return RefreshOutcome(
        seeded=seeded,
        refreshed=refreshed,
        submissions_advanced=submissions_advanced,
        failed=failed,
    )
