"""
Tax ledger: the legacy year-to-date tax view.

The matcher and its writers moved to ``app/services/tax_matching.py`` (#3616 slice 3d-2), which identifies disposals
from the broker evidence generation. ``tax_lots``, ``disposal_matches`` and ``s104_pool`` have no writer now and
serve empty through the read functions below until slice 3d-4 replaces this read surface.

Issue #11.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

MatchingRule = Literal["same_day", "bed_and_breakfast", "s104_pool"]

_UK_TZ = ZoneInfo("Europe/London")
_D = Decimal

# ---------------------------------------------------------------------------
# CGT rate periods for listed shares / non-residential assets
# ---------------------------------------------------------------------------

# (from_date_inclusive, to_date_inclusive, basic_rate, higher_rate)
# Earliest supported disposal date: 2024-04-06 (start of 2024/25 tax year).
# Disposals before this date will raise RuntimeError. To support earlier
# years, add the applicable rate period to this list.
_CGT_RATE_PERIODS: list[tuple[date, date, Decimal, Decimal]] = [
    # Pre-Autumn-Budget 2024/25
    (date(2024, 4, 6), date(2024, 10, 29), _D("0.10"), _D("0.20")),
    # Post-Autumn-Budget 2024/25
    (date(2024, 10, 30), date(2025, 4, 5), _D("0.18"), _D("0.24")),
    # 2025/26 onwards (until legislation changes)
    (date(2025, 4, 6), date(2099, 4, 5), _D("0.18"), _D("0.24")),
]

# First tax-year the engine can price a disposal for — derived from the CGT
# rate table so the API's accepted-year floor tracks rate coverage (a request
# for an earlier year is a 422, never a 500 from ``_cgt_rates_for_disposal``).
_MIN_SUPPORTED_TAX_YEAR_START = _CGT_RATE_PERIODS[0][0].year

# Annual exempt amount, 2024/25 onwards
ANNUAL_EXEMPT = _D("3000")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaxYearSummary:
    tax_year: str
    total_gains_gbp: Decimal
    total_losses_gbp: Decimal
    net_gain_gbp: Decimal
    dividend_total_gbp: Decimal
    disposals_same_day: int
    disposals_bed_and_breakfast: int
    disposals_s104: int
    # Statutory disposals (one per security per day, s105(1)(a)). One disposal can be identified under more
    # than one rule, so the three per-rule counts can sum to more than this.
    disposals_total: int
    # Annual exempt amount (£3,000) and how much of it is unused after this
    # year's net gain. Computed here — never in the API handler — so the CGT
    # treatment stays owned by the engine (source rule).
    annual_exempt_gbp: Decimal
    exempt_remaining_gbp: Decimal
    # Scenario estimates — actual CGT depends on taxpayer's income and band
    estimated_cgt_basic_scenario: Decimal
    estimated_cgt_higher_scenario: Decimal


@dataclass(frozen=True)
class DisposalMatchDetail:
    match_id: int
    instrument_id: int
    disposal_tax_lot_id: int
    acquisition_tax_lot_id: int | None
    matching_rule: str
    matched_units: Decimal
    acquisition_cost_gbp: Decimal
    disposal_proceeds_gbp: Decimal
    gain_or_loss_gbp: Decimal
    disposal_uk_date: date
    tax_year: str
    matched_at: datetime


@dataclass(frozen=True)
class TaxDisposalRow:
    """A disposal match for a whole tax year, with the instrument symbol.

    Same engine output as ``DisposalMatchDetail`` but tax-year-scoped (not
    per-instrument) and carrying ``symbol`` for the disposal table UI.
    """

    match_id: int
    instrument_id: int
    symbol: str
    matching_rule: str
    matched_units: Decimal
    acquisition_cost_gbp: Decimal
    disposal_proceeds_gbp: Decimal
    gain_or_loss_gbp: Decimal
    disposal_uk_date: date
    tax_year: str
    disposal_tax_lot_id: int
    acquisition_tax_lot_id: int | None
    matched_at: datetime


@dataclass(frozen=True)
class S104PoolRow:
    """Current s104 pool state for an instrument, with the symbol."""

    instrument_id: int
    symbol: str
    pool_units: Decimal
    pool_cost_gbp: Decimal
    pool_avg_cost_gbp: Decimal
    updated_at: datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _to_uk_date(dt: datetime) -> date:
    """Convert a timezone-aware datetime to a UK calendar date."""
    return dt.astimezone(_UK_TZ).date()


def _compute_tax_year(uk_date: date) -> str:
    """UK tax year: 6 April to 5 April.

    2025-04-05 -> "2024/25"
    2025-04-06 -> "2025/26"
    """
    year = uk_date.year
    month = uk_date.month
    day = uk_date.day

    if month < 4 or (month == 4 and day <= 5):
        start_year = year - 1
    else:
        start_year = year

    end_year_short = (start_year + 1) % 100
    return f"{start_year}/{end_year_short:02d}"


def _cgt_rates_for_disposal(uk_date: date) -> tuple[Decimal, Decimal]:
    """Return (basic_rate, higher_rate) for a disposal on the given UK date."""
    for from_d, to_d, basic, higher in _CGT_RATE_PERIODS:
        if from_d <= uk_date <= to_d:
            return basic, higher
    raise RuntimeError(f"No CGT rate period covers disposal date {uk_date}. Update _CGT_RATE_PERIODS in tax_ledger.py.")


# ---------------------------------------------------------------------------
# Reporting — read-only queries
# ---------------------------------------------------------------------------


def tax_year_summary(
    conn: psycopg.Connection[Any],
    tax_year: str,
) -> TaxYearSummary:
    """Read-only: aggregated tax year view with CGT scenario estimates."""
    # Three separate cursors so each query gets its own result set.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # A gain or loss belongs to a statutory disposal, which is every disposal of one security on one day
        # (TCGA 1992 s105(1)(a)), so its sign is read after summing that day's match rows. The counts are statutory
        # disposals too: per rule, those identified (wholly or partly) under it, and in total.
        cur.execute(
            """
            WITH disposals AS (
                SELECT SUM(gain_or_loss_gbp) AS gain
                FROM disposal_matches
                WHERE tax_year = %(ty)s
                GROUP BY instrument_id, disposal_uk_date
            )
            SELECT
                (SELECT COALESCE(SUM(gain) FILTER (WHERE gain > 0), 0) FROM disposals) AS total_gains,
                (SELECT COALESCE(SUM(gain) FILTER (WHERE gain < 0), 0) FROM disposals) AS total_losses,
                COALESCE(SUM(gain_or_loss_gbp), 0) AS net_gain,
                COUNT(DISTINCT (instrument_id, disposal_uk_date)) FILTER (WHERE matching_rule = 'same_day')
                    AS cnt_same_day,
                COUNT(DISTINCT (instrument_id, disposal_uk_date)) FILTER (WHERE matching_rule = 'bed_and_breakfast')
                    AS cnt_bnb,
                COUNT(DISTINCT (instrument_id, disposal_uk_date)) FILTER (WHERE matching_rule = 's104_pool')
                    AS cnt_s104,
                COUNT(DISTINCT (instrument_id, disposal_uk_date)) AS cnt_total
            FROM disposal_matches
            WHERE tax_year = %(ty)s
            """,
            {"ty": tax_year},
        )
        agg = cur.fetchone()
        if agg is None:
            raise RuntimeError("Aggregate query returned no rows")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT SUM(gain_or_loss_gbp) AS gain_or_loss_gbp, disposal_uk_date
            FROM disposal_matches
            WHERE tax_year = %(ty)s
            GROUP BY instrument_id, disposal_uk_date
            HAVING SUM(gain_or_loss_gbp) > 0
            """,
            {"ty": tax_year},
        )
        gain_rows = cur.fetchall()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(amount_gbp), 0) AS dividend_total
            FROM tax_lots
            WHERE tax_year = %(ty)s
              AND direction = 'dividend'
            """,
            {"ty": tax_year},
        )
        div_row = cur.fetchone()
        if div_row is None:
            raise RuntimeError("Dividend query returned no rows")

    total_gains = Decimal(str(agg["total_gains"]))
    total_losses = Decimal(str(agg["total_losses"]))
    net_gain = Decimal(str(agg["net_gain"]))
    dividend_total = Decimal(str(div_row["dividend_total"]))

    # Per-disposal weighted CGT estimates.
    # Invariant: gain_rows holds one row per statutory disposal (security, day)
    # whose net result is > 0 (filtered in the SQL query). total_gains is the
    # sum of the same positive values. The scale factor below (taxable_net / total_gains)
    # is therefore a valid ratio. If the query is ever changed to include
    # losses, the denominator must also change.
    weighted_basic = _D("0")
    weighted_higher = _D("0")
    for gr in gain_rows:
        gain = Decimal(str(gr["gain_or_loss_gbp"]))
        basic_rate, higher_rate = _cgt_rates_for_disposal(gr["disposal_uk_date"])
        weighted_basic += gain * basic_rate
        weighted_higher += gain * higher_rate

    # Apply annual exemption proportionally
    taxable_net = max(net_gain - ANNUAL_EXEMPT, _D("0"))
    if total_gains > 0 and taxable_net > 0:
        scale = taxable_net / total_gains
        est_basic = weighted_basic * scale
        est_higher = weighted_higher * scale
    else:
        est_basic = _D("0")
        est_higher = _D("0")

    # Unused annual exemption: the allowance offsets positive net gains only
    # (losses do not restore it). Complement of ``taxable_net``.
    exempt_remaining = max(ANNUAL_EXEMPT - max(net_gain, _D("0")), _D("0"))

    return TaxYearSummary(
        tax_year=tax_year,
        total_gains_gbp=total_gains,
        total_losses_gbp=total_losses,
        net_gain_gbp=net_gain,
        dividend_total_gbp=dividend_total,
        disposals_same_day=int(agg["cnt_same_day"]),
        disposals_bed_and_breakfast=int(agg["cnt_bnb"]),
        disposals_s104=int(agg["cnt_s104"]),
        disposals_total=int(agg["cnt_total"]),
        annual_exempt_gbp=ANNUAL_EXEMPT,
        exempt_remaining_gbp=exempt_remaining,
        estimated_cgt_basic_scenario=est_basic,
        estimated_cgt_higher_scenario=est_higher,
    )


def disposal_audit_trail(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    tax_year: str | None = None,
) -> list[DisposalMatchDetail]:
    """Read-only: full match provenance for an instrument."""
    sql = """
        SELECT match_id, instrument_id, disposal_tax_lot_id,
               acquisition_tax_lot_id, matching_rule, matched_units,
               acquisition_cost_gbp, disposal_proceeds_gbp,
               gain_or_loss_gbp, disposal_uk_date, tax_year, matched_at
        FROM disposal_matches
        WHERE instrument_id = %(iid)s
    """
    params: dict[str, Any] = {"iid": instrument_id}
    if tax_year is not None:
        sql += " AND tax_year = %(ty)s"
        params["ty"] = tax_year
    sql += " ORDER BY disposal_uk_date ASC, match_id ASC"

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        DisposalMatchDetail(
            match_id=row["match_id"],
            instrument_id=row["instrument_id"],
            disposal_tax_lot_id=row["disposal_tax_lot_id"],
            acquisition_tax_lot_id=row["acquisition_tax_lot_id"],
            matching_rule=row["matching_rule"],
            matched_units=Decimal(str(row["matched_units"])),
            acquisition_cost_gbp=Decimal(str(row["acquisition_cost_gbp"])),
            disposal_proceeds_gbp=Decimal(str(row["disposal_proceeds_gbp"])),
            gain_or_loss_gbp=Decimal(str(row["gain_or_loss_gbp"])),
            disposal_uk_date=row["disposal_uk_date"],
            tax_year=row["tax_year"],
            matched_at=row["matched_at"],
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Tax-year helpers + tax-year-scoped read surface (#1905 — Tax & CGT API)
# ---------------------------------------------------------------------------


def current_tax_year() -> str:
    """The UK tax year containing the current instant, e.g. ``2026/27``."""
    return _compute_tax_year(_to_uk_date(_utcnow()))


def valid_tax_year(tax_year: str) -> bool:
    """True iff ``tax_year`` is a well-formed, engine-priceable UK tax-year label.

    Shape ``\\d{4}/\\d{2}`` alone is not enough — the two-digit suffix must be
    the arithmetic successor ``(start_year + 1) % 100`` (rejects impossible
    labels like ``2026/99``). ``start_year`` must be in
    ``[MIN_SUPPORTED_TAX_YEAR_START, current+1]``: the lower bound is the first
    year the engine has CGT rate coverage for (``_CGT_RATE_PERIODS[0]``), so a
    year the engine cannot price is a clean rejection rather than a later
    ``_cgt_rates_for_disposal`` RuntimeError (→ 500) on any positive-gain row.
    """
    parts = tax_year.split("/")
    if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
        return False
    start_s, suffix_s = parts
    if not (start_s.isdigit() and suffix_s.isdigit()):
        return False
    start_year = int(start_s)
    if int(suffix_s) != (start_year + 1) % 100:
        return False
    current_start = int(current_tax_year().split("/")[0])
    return _MIN_SUPPORTED_TAX_YEAR_START <= start_year <= current_start + 1


def available_tax_years(conn: psycopg.Connection[Any]) -> list[str]:
    """Distinct tax years with any tax data, newest first.

    Union across ``tax_lots`` and ``disposal_matches`` so a year with only
    dividend lots (no disposals) still appears. The current tax year is always
    included so the UI selector is never empty on a fresh account.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT tax_year FROM tax_lots
            UNION
            SELECT DISTINCT tax_year FROM disposal_matches
            """
        )
        years = {row["tax_year"] for row in cur.fetchall()}
    years.add(current_tax_year())
    return sorted(years, reverse=True)


def disposals_for_tax_year(
    conn: psycopg.Connection[Any],
    tax_year: str,
) -> list[TaxDisposalRow]:
    """Read-only: every disposal match in a tax year, with instrument symbol."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT dm.match_id, dm.instrument_id, i.symbol,
                   dm.disposal_tax_lot_id, dm.acquisition_tax_lot_id,
                   dm.matching_rule, dm.matched_units,
                   dm.acquisition_cost_gbp, dm.disposal_proceeds_gbp,
                   dm.gain_or_loss_gbp, dm.disposal_uk_date, dm.tax_year,
                   dm.matched_at
            FROM disposal_matches dm
            JOIN instruments i ON i.instrument_id = dm.instrument_id
            WHERE dm.tax_year = %(ty)s
            ORDER BY dm.disposal_uk_date ASC, dm.match_id ASC
            """,
            {"ty": tax_year},
        )
        rows = cur.fetchall()

    return [
        TaxDisposalRow(
            match_id=row["match_id"],
            instrument_id=row["instrument_id"],
            symbol=row["symbol"],
            matching_rule=row["matching_rule"],
            matched_units=Decimal(str(row["matched_units"])),
            acquisition_cost_gbp=Decimal(str(row["acquisition_cost_gbp"])),
            disposal_proceeds_gbp=Decimal(str(row["disposal_proceeds_gbp"])),
            gain_or_loss_gbp=Decimal(str(row["gain_or_loss_gbp"])),
            disposal_uk_date=row["disposal_uk_date"],
            tax_year=row["tax_year"],
            disposal_tax_lot_id=row["disposal_tax_lot_id"],
            acquisition_tax_lot_id=row["acquisition_tax_lot_id"],
            matched_at=row["matched_at"],
        )
        for row in rows
    ]


def s104_pool_rows(conn: psycopg.Connection[Any]) -> list[S104PoolRow]:
    """Read-only: current s104 pool state per instrument, largest cost first."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT sp.instrument_id, i.symbol, sp.pool_units,
                   sp.pool_cost_gbp, sp.pool_avg_cost_gbp, sp.updated_at
            FROM s104_pool sp
            JOIN instruments i ON i.instrument_id = sp.instrument_id
            ORDER BY sp.pool_cost_gbp DESC, i.symbol ASC
            """
        )
        rows = cur.fetchall()

    return [
        S104PoolRow(
            instrument_id=row["instrument_id"],
            symbol=row["symbol"],
            pool_units=Decimal(str(row["pool_units"])),
            pool_cost_gbp=Decimal(str(row["pool_cost_gbp"])),
            pool_avg_cost_gbp=Decimal(str(row["pool_avg_cost_gbp"])),
            updated_at=row["updated_at"],
        )
        for row in rows
    ]
