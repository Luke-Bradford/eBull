"""Financial normalization service.

Derives financial_periods_raw from financial_facts_raw, then merges
into canonical financial_periods.

Pipeline:
  financial_facts_raw -> _derive_periods_from_facts() -> financial_periods_raw
  financial_periods_raw -> _canonical_merge() -> financial_periods
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from app.providers.implementations.sec_fundamentals import TRACKED_CONCEPTS

logger = logging.getLogger(__name__)


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

# EPS is NOT safely derivable via Q4 = FY - Q1 - Q2 - Q3 because the
# denominator (share count) changes across periods.  These columns are
# excluded from Q4 subtraction — use FY EPS values for Q4 instead.
_NON_DERIVABLE_FLOW: frozenset[str] = frozenset({"eps_basic", "eps_diluted"})

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

# All financial columns on PeriodRow
_ALL_FINANCIAL_COLUMNS: frozenset[str] = (
    _FLOW_COLUMNS
    | _BALANCE_SHEET_COLUMNS
    | {
        "shares_basic",
        "shares_diluted",
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
        for col in _FLOW_COLUMNS:
            if col in _NON_DERIVABLE_FLOW:
                continue  # EPS not safely subtractive across different share counts
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

        # EPS: use FY values directly for Q4 (approximate; correct derivation
        # would require share-count weighting which we don't have).
        for col in _NON_DERIVABLE_FLOW:
            fy_val = getattr(fy_row, col)
            if fy_val is not None:
                setattr(q4, col, fy_val)

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


# -- Source priority for canonical merge ------------------------------------
_SOURCE_PRIORITY = {"sec_edgar": 1, "companies_house": 2, "fmp": 3}


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
