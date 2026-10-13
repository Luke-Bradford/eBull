"""
Tax ledger: UK disposal matching and year-to-date tax view.

Consumes fills from the order client and produces disposal matches
using HMRC's same-day, 30-day (bed and breakfast), and Section 104
pool rules. All monetary amounts are converted to GBP for matching.

Issue #11.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

TaxEventDirection = Literal["acquisition", "disposal", "dividend", "fee", "adjustment"]
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
class TaxLot:
    tax_lot_id: int
    instrument_id: int
    event_time: datetime
    uk_date: date  # event_time in Europe/London
    event_type: str  # original action: BUY, ADD, EXIT, dividend, fee
    direction: TaxEventDirection
    quantity: Decimal  # always positive
    cost_or_proceeds: Decimal  # original-currency total
    amount_gbp: Decimal  # GBP-converted total
    tax_year: str
    reference_fill_id: int | None


@dataclass(frozen=True)
class DisposalMatch:
    disposal_lot: TaxLot
    acquisition_lot: TaxLot | None  # None for s104_pool
    matching_rule: MatchingRule
    matched_units: Decimal
    acquisition_cost_gbp: Decimal  # units (count) * per_unit_cost (GBP/unit) = GBP
    disposal_proceeds_gbp: Decimal  # units (count) * per_unit_price (GBP/unit) = GBP
    gain_or_loss_gbp: Decimal  # proceeds (GBP) - cost (GBP) = gain (GBP)


@dataclass
class MutablePoolState:
    """Mutable accumulator for the Section 104 pool during matching."""

    units: Decimal
    cost_gbp: Decimal

    @property
    def avg_cost_gbp(self) -> Decimal:
        """Average cost per unit. Returns 0 when pool is empty.

        Only valid for gain/loss computation when units > 0.
        Callers in _match_disposals_for_instrument always guard with
        ``if pool.units > 0`` before using the average.
        """
        if self.units <= 0:
            return _D("0")
        return self.cost_gbp / self.units


@dataclass(frozen=True)
class PoolState:
    """Immutable snapshot of the Section 104 pool."""

    instrument_id: int
    units: Decimal
    cost_gbp: Decimal
    avg_cost_gbp: Decimal


@dataclass(frozen=True)
class IngestionResult:
    fills_ingested: int
    cash_events_ingested: int
    already_present: int


@dataclass(frozen=True)
class MatchingResult:
    instruments_processed: int
    matches_created: int
    total_gain_gbp: Decimal
    total_loss_gbp: Decimal


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
# Ingestion
# ---------------------------------------------------------------------------


def ingest_tax_events(conn: psycopg.Connection[Any]) -> IngestionResult:
    """Ingest cash events into tax_lots. Idempotent.

    The fills route is retired (#3616 slice 3c): ``fills`` is the app's own order record, a subset of the account's
    trades, and reading it beside the broker record would count those trades twice. Tax evidence comes from the
    broker record (the slice 3c evidence run); sql/485 refuses any insert into ``tax_lots``. The signature and the
    zero ``fills_ingested`` stay until slice 3d moves matching onto the evidence generation.
    """
    inserted = 0
    cash = _ingest_cash_events(conn)

    already = 0

    logger.info(
        "ingest_tax_events: fills_ingested=%d cash_events=%d already_present=%d",
        inserted,
        cash,
        already,
    )
    return IngestionResult(
        fills_ingested=inserted,
        cash_events_ingested=cash,
        already_present=already,
    )


def _ingest_cash_events(conn: psycopg.Connection[Any]) -> int:
    """Ingest dividend/fee events from cash_ledger into tax_lots.

    Stub: returns 0 today. When cash_ledger gains dividend/fee event_types
    with instrument references, this will query and ingest them.

    # TODO: dedup key for cash-event ingestion (#11 follow-up)
    # reference_fill_id only covers fills. Cash-ledger events will need
    # their own dedup key — likely (event_type, event_time, instrument_id,
    # amount) natural key or a dedicated reference_event_id column.
    """
    del conn  # unused until dividend/fee event_types exist
    return 0


# ---------------------------------------------------------------------------
# Disposal matching — pure functions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _DayIdentification:
    """One identification between a day's single disposal and a day's single acquisition (or the pool)."""

    rule: MatchingRule
    disposal_day: date
    acquisition_day: date | None  # None for s104_pool
    units: Decimal
    cost_gbp: Decimal


#: Every quantity and GBP column the matcher writes is NUMERIC(18,6) (sql/001, sql/013). Each computed share is
#: quantized to that scale and the last share of a split takes the exact remainder, so the rows still sum to their
#: total after the database stores them.
_DB_SCALE = _D("0.000001")


def _split_exact(total: Decimal, weights: list[Decimal]) -> list[Decimal]:
    """A non-negative ``total`` split pro rata to ``weights`` at the database scale.

    Each share is the difference of two rounded cumulative shares, so no share is negative (rounding a running
    total is monotone) and the shares sum to ``total``; the last share takes the exact remainder.
    """
    weight_sum = sum(weights, _D("0"))
    cumulative = _D("0")
    previous = _D("0")
    shares: list[Decimal] = []
    for w in weights[:-1]:
        cumulative += w
        rounded = (total * cumulative / weight_sum).quantize(_DB_SCALE)
        shares.append(rounded - previous)
        previous = rounded
    return [*shares, total - previous]


def _match_disposals_for_instrument(
    acquisitions: list[TaxLot],
    disposals: list[TaxLot],
) -> tuple[list[DisposalMatch], PoolState]:
    """Pure function: identify disposals with acquisitions of one security under the share identification rules.

    Returns (matches, final_pool_state). All date comparisons use uk_date (Europe/London calendar date).

    The statutory order (TCGA 1992 ss104-106A; HMRC CG51560):

    1. **One transaction per day.** All acquisitions of the security on one day are a single acquisition, and all
       disposals on one day a single disposal (s105(1)(a)), so each day's cost and proceeds are averaged before any
       identification.
    2. **Same day** (s105(1)(b)), on every day, before anything else. A later day's acquisition therefore serves that
       day's own disposal before it can serve an earlier disposal under the 30-day rule.
    3. **Following 30 days** (s106A(5)): earlier disposals first, each against the earliest remaining acquisitions in
       the 30 days after it.
    4. **Section 104 pool** (s104), walked in date order with every acquisition's unidentified remainder entering on
       its own day.

    Each day-level identification is then allocated back to the individual lots pro rata to their units, with exact
    remainders, so ``disposal_matches`` keeps a row per (disposal lot, acquisition lot) for the audit trail and the
    rows of a day sum exactly to the day's identification.
    """
    instrument_id = (disposals or acquisitions)[0].instrument_id if (disposals or acquisitions) else 0

    acq_by_day: dict[date, list[TaxLot]] = {}
    for acq in sorted(acquisitions, key=lambda a: (a.event_time, a.tax_lot_id)):
        acq_by_day.setdefault(acq.uk_date, []).append(acq)
    disp_by_day: dict[date, list[TaxLot]] = {}
    for disp in sorted(disposals, key=lambda d: (d.event_time, d.tax_lot_id)):
        disp_by_day.setdefault(disp.uk_date, []).append(disp)

    acq_units_left = {d: sum((a.quantity for a in lots), _D("0")) for d, lots in acq_by_day.items()}
    acq_cost_left = {d: sum((a.amount_gbp for a in lots), _D("0")) for d, lots in acq_by_day.items()}
    disp_units_left = {d: sum((x.quantity for x in lots), _D("0")) for d, lots in disp_by_day.items()}
    identifications: list[_DayIdentification] = []

    def take(acq_day: date, units: Decimal) -> Decimal:
        """Withdraw ``units`` from a day's acquisition at that day's average cost; the last withdrawal takes the
        exact remaining cost, so the day's cost is conserved."""
        if units == acq_units_left[acq_day]:
            cost = acq_cost_left[acq_day]
        else:
            cost = (units * acq_cost_left[acq_day] / acq_units_left[acq_day]).quantize(_DB_SCALE)
        acq_units_left[acq_day] -= units
        acq_cost_left[acq_day] -= cost
        return cost

    # Step 1: same day, on every day first.
    for day in sorted(disp_by_day.keys() & acq_by_day.keys()):
        units = min(disp_units_left[day], acq_units_left[day])
        if units > 0:
            identifications.append(_DayIdentification("same_day", day, day, units, take(day, units)))
            disp_units_left[day] -= units

    # Step 2: the following 30 days, earlier disposals first, earliest acquisitions first.
    acq_days = sorted(acq_by_day)
    for day in sorted(disp_by_day):
        window_end = day + timedelta(days=30)
        for acq_day in acq_days:
            if disp_units_left[day] <= 0:
                break
            if acq_day <= day or acq_day > window_end or acq_units_left[acq_day] <= 0:
                continue
            units = min(disp_units_left[day], acq_units_left[acq_day])
            identifications.append(_DayIdentification("bed_and_breakfast", day, acq_day, units, take(acq_day, units)))
            disp_units_left[day] -= units

    # Step 3: the s104 pool, in date order. A day with an acquisition remainder had its disposal fully identified
    # same-day, so the order of the two within one day cannot matter.
    pool = MutablePoolState(units=_D("0"), cost_gbp=_D("0"))
    for day in sorted(acq_by_day.keys() | disp_by_day.keys()):
        if day in acq_by_day:
            pool.units += acq_units_left[day]
            pool.cost_gbp += acq_cost_left[day]
        if day not in disp_by_day or disp_units_left[day] <= 0:
            continue
        if pool.units > 0:
            units = min(disp_units_left[day], pool.units)
            if units == pool.units:
                # Full pool depletion — take exact remaining cost, no rounding
                cost = pool.cost_gbp
            else:
                # Quantize to 6 dp (matching DB NUMERIC(18,6)) so that
                # cumulative subtractions leave an exact remainder for the
                # final full-depletion withdrawal — guarantees cost conservation.
                cost = ((units / pool.units) * pool.cost_gbp).quantize(_DB_SCALE)
            identifications.append(_DayIdentification("s104_pool", day, None, units, cost))
            pool.units -= units
            pool.cost_gbp -= cost
            disp_units_left[day] -= units
        if disp_units_left[day] > 0:
            logger.warning(
                "Disposal lots %s have %s unmatched units for instrument %d (incomplete acquisition data).",
                [x.tax_lot_id for x in disp_by_day[day]],
                disp_units_left[day],
                instrument_id,
            )

    # Allocate each day-level identification back to the lots, at the database scale.
    #
    # The day's proceeds are averaged over its units (s105(1)(a)). An identification's proceeds are the difference
    # of two rounded running totals of the day's identified units, so a fully identified day's rows sum exactly to
    # its proceeds. Each identification is then split over the day's disposal lots, and each lot's share over the
    # acquisition day's lots, by ``_split_exact``, so every identification's rows sum exactly to it and none is
    # negative. A broker lot's own rows can differ from its pro-rata share by one unit of the scale per
    # identification; the statutory figures are the day's, not the lot's.
    matches: list[DisposalMatch] = []
    identified_units: dict[date, Decimal] = {}
    for ident in identifications:
        disp_lots = disp_by_day[ident.disposal_day]
        day_units = sum((x.quantity for x in disp_lots), _D("0"))
        day_proceeds = sum((x.amount_gbp for x in disp_lots), _D("0"))
        u0 = identified_units.get(ident.disposal_day, _D("0"))
        u1 = u0 + ident.units
        identified_units[ident.disposal_day] = u1
        p0 = day_proceeds if u0 == day_units else (u0 * day_proceeds / day_units).quantize(_DB_SCALE)
        p1 = day_proceeds if u1 == day_units else (u1 * day_proceeds / day_units).quantize(_DB_SCALE)
        weights = [x.quantity for x in disp_lots]
        per_disp = zip(
            disp_lots,
            _split_exact(ident.units, weights),
            _split_exact(ident.cost_gbp, weights),
            _split_exact(p1 - p0, weights),
            strict=True,
        )
        for disp, d_units, d_cost, d_proceeds in per_disp:
            acq_lots: list[TaxLot | None] = (
                [None] if ident.acquisition_day is None else list(acq_by_day[ident.acquisition_day])
            )
            acq_weights = [_D("1") if a is None else a.quantity for a in acq_lots]
            for acq, units, cost, lot_proceeds in zip(
                acq_lots,
                _split_exact(d_units, acq_weights),
                _split_exact(d_cost, acq_weights),
                _split_exact(d_proceeds, acq_weights),
                strict=True,
            ):
                if units == 0 and cost == 0 and lot_proceeds == 0:
                    continue  # a share that rounded to nothing on every column is not a row
                matches.append(
                    DisposalMatch(
                        disposal_lot=disp,
                        acquisition_lot=acq,
                        matching_rule=ident.rule,
                        matched_units=units,
                        acquisition_cost_gbp=cost,
                        disposal_proceeds_gbp=lot_proceeds,
                        gain_or_loss_gbp=lot_proceeds - cost,
                    )
                )

    # Invariant checks
    if pool.units < 0:
        raise RuntimeError(
            f"S104 pool units negative ({pool.units}) for instrument {instrument_id}. "
            "This indicates a matching algorithm bug."
        )
    if pool.cost_gbp < 0:
        raise RuntimeError(
            f"S104 pool cost negative ({pool.cost_gbp}) for instrument {instrument_id}. "
            "This indicates a matching algorithm bug."
        )

    return matches, PoolState(
        instrument_id=instrument_id,
        units=pool.units,
        cost_gbp=pool.cost_gbp,
        avg_cost_gbp=pool.avg_cost_gbp,
    )


# ---------------------------------------------------------------------------
# Matching — DB orchestration
# ---------------------------------------------------------------------------


def run_disposal_matching(
    conn: psycopg.Connection[Any],
    instrument_id: int | None = None,
) -> MatchingResult:
    """Run disposal matching for one instrument or all with disposals.

    Idempotent: deletes prior matches for the instrument and recomputes.
    """
    # Read phase: load tax_lots
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if instrument_id is not None:
            cur.execute(
                """
                SELECT tax_lot_id, instrument_id, event_time, event_type,
                       direction, quantity, cost_or_proceeds, amount_gbp,
                       tax_year, reference_fill_id
                FROM tax_lots
                WHERE instrument_id = %(iid)s
                ORDER BY event_time ASC
                """,
                {"iid": instrument_id},
            )
        else:
            cur.execute(
                """
                SELECT tax_lot_id, instrument_id, event_time, event_type,
                       direction, quantity, cost_or_proceeds, amount_gbp,
                       tax_year, reference_fill_id
                FROM tax_lots
                WHERE direction IN ('acquisition', 'disposal')
                ORDER BY instrument_id, event_time ASC
                """
            )
        all_lots = cur.fetchall()

    if not all_lots:
        return MatchingResult(
            instruments_processed=0,
            matches_created=0,
            total_gain_gbp=_D("0"),
            total_loss_gbp=_D("0"),
        )

    # Group by instrument
    lots_by_instrument: dict[int, list[dict[str, Any]]] = {}
    for lot in all_lots:
        iid = lot["instrument_id"]
        lots_by_instrument.setdefault(iid, []).append(lot)

    # Determine which instruments to process (those with at least one disposal)
    instrument_ids = [
        iid for iid, lots in lots_by_instrument.items() if any(lot["direction"] == "disposal" for lot in lots)
    ]

    if not instrument_ids:
        return MatchingResult(
            instruments_processed=0,
            matches_created=0,
            total_gain_gbp=_D("0"),
            total_loss_gbp=_D("0"),
        )

    total_matches = 0
    total_gain = _D("0")
    total_loss = _D("0")

    # Write phase: single transaction for all instruments
    with conn.transaction():
        for iid in instrument_ids:
            lots = lots_by_instrument[iid]

            # Delete prior matches for this instrument (idempotency)
            conn.execute(
                "DELETE FROM disposal_matches WHERE instrument_id = %(iid)s",
                {"iid": iid},
            )

            # Build TaxLot objects
            acquisitions: list[TaxLot] = []
            disposals_list: list[TaxLot] = []
            for lot in lots:
                event_time: datetime = lot["event_time"]
                uk_dt = _to_uk_date(event_time)
                tax_lot = TaxLot(
                    tax_lot_id=lot["tax_lot_id"],
                    instrument_id=lot["instrument_id"],
                    event_time=event_time,
                    uk_date=uk_dt,
                    event_type=lot["event_type"],
                    direction=lot["direction"],
                    quantity=Decimal(str(lot["quantity"])),
                    cost_or_proceeds=Decimal(str(lot["cost_or_proceeds"])),
                    amount_gbp=Decimal(str(lot["amount_gbp"])),
                    tax_year=lot["tax_year"],
                    reference_fill_id=lot["reference_fill_id"],
                )
                if lot["direction"] == "acquisition":
                    acquisitions.append(tax_lot)
                elif lot["direction"] == "disposal":
                    disposals_list.append(tax_lot)

            # Run pure matching algorithm
            matches, pool_state = _match_disposals_for_instrument(acquisitions, disposals_list)

            # Persist matches
            for m in matches:
                conn.execute(
                    """
                    INSERT INTO disposal_matches (
                        instrument_id, disposal_tax_lot_id,
                        acquisition_tax_lot_id, matching_rule,
                        matched_units, acquisition_cost_gbp,
                        disposal_proceeds_gbp, gain_or_loss_gbp,
                        disposal_uk_date, tax_year, matched_at
                    )
                    VALUES (
                        %(iid)s, %(d_lot)s, %(a_lot)s, %(rule)s,
                        %(units)s, %(acq_cost)s, %(disp_proc)s,
                        %(gain)s, %(d_date)s, %(ty)s, %(now)s
                    )
                    """,
                    {
                        "iid": iid,
                        "d_lot": m.disposal_lot.tax_lot_id,
                        "a_lot": (m.acquisition_lot.tax_lot_id if m.acquisition_lot else None),
                        "rule": m.matching_rule,
                        "units": m.matched_units,
                        "acq_cost": m.acquisition_cost_gbp,
                        "disp_proc": m.disposal_proceeds_gbp,
                        "gain": m.gain_or_loss_gbp,
                        "d_date": m.disposal_lot.uk_date,
                        "ty": m.disposal_lot.tax_year,
                        "now": _utcnow(),
                    },
                )

            # Upsert S104 pool state
            conn.execute(
                """
                INSERT INTO s104_pool (
                    instrument_id, pool_units, pool_cost_gbp,
                    pool_avg_cost_gbp, updated_at
                )
                VALUES (%(iid)s, %(units)s, %(cost)s, %(avg)s, %(now)s)
                ON CONFLICT (instrument_id) DO UPDATE SET
                    pool_units = EXCLUDED.pool_units,
                    pool_cost_gbp = EXCLUDED.pool_cost_gbp,
                    pool_avg_cost_gbp = EXCLUDED.pool_avg_cost_gbp,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "iid": iid,
                    "units": pool_state.units,
                    "cost": pool_state.cost_gbp,
                    "avg": pool_state.avg_cost_gbp,
                    "now": _utcnow(),
                },
            )

            total_matches += len(matches)
            for m in matches:
                if m.gain_or_loss_gbp > 0:
                    total_gain += m.gain_or_loss_gbp
                else:
                    total_loss += m.gain_or_loss_gbp

    logger.info(
        "run_disposal_matching: instruments=%d matches=%d gain=%.2f loss=%.2f",
        len(instrument_ids),
        total_matches,
        total_gain,
        total_loss,
    )
    return MatchingResult(
        instruments_processed=len(instrument_ids),
        matches_created=total_matches,
        total_gain_gbp=total_gain,
        total_loss_gbp=total_loss,
    )


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
