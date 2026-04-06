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
_CGT_RATE_PERIODS: list[tuple[date, date, Decimal, Decimal]] = [
    # Pre-Autumn-Budget 2024/25
    (date(2024, 4, 6), date(2024, 10, 29), _D("0.10"), _D("0.20")),
    # Post-Autumn-Budget 2024/25
    (date(2024, 10, 30), date(2025, 4, 5), _D("0.18"), _D("0.24")),
    # 2025/26 onwards (until legislation changes)
    (date(2025, 4, 6), date(2099, 4, 5), _D("0.18"), _D("0.24")),
]

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


def _load_fx_rate(
    conn: psycopg.Connection[Any],
    rate_date: date,
    from_currency: str,
) -> Decimal:
    """Look up FX rate for from_currency -> GBP on the given date.

    Returns Decimal("1") for GBP. Raises RuntimeError if missing.
    """
    if from_currency.upper() == "GBP":
        return _D("1")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT rate
            FROM fx_rates
            WHERE rate_date = %(rate_date)s
              AND from_currency = %(from_currency)s
              AND to_currency = 'GBP'
            """,
            {"rate_date": rate_date, "from_currency": from_currency.upper()},
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(
            f"Missing FX rate: {from_currency}->GBP on {rate_date}. Populate fx_rates before ingesting tax events."
        )
    return Decimal(str(row["rate"]))


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


def ingest_tax_events(conn: psycopg.Connection[Any]) -> IngestionResult:
    """Ingest fills and cash events into tax_lots. Idempotent."""
    fills = _ingest_fills(conn)
    cash = _ingest_cash_events(conn)

    # Count how many fills were already present (skipped by ON CONFLICT)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM tax_lots WHERE reference_fill_id IS NOT NULL")
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("COUNT query returned no rows")
        total_fill_lots = int(row["cnt"])

    already = total_fill_lots - fills

    logger.info(
        "ingest_tax_events: fills_ingested=%d cash_events=%d already_present=%d",
        fills,
        cash,
        already,
    )
    return IngestionResult(
        fills_ingested=fills,
        cash_events_ingested=cash,
        already_present=already,
    )


def _ingest_fills(conn: psycopg.Connection[Any]) -> int:
    """Read fills not yet in tax_lots, convert to GBP, write tax_lots rows.

    Returns the number of new rows written.
    """
    # Load un-ingested fills (read phase — before transaction)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT f.fill_id, f.filled_at, f.price, f.units,
                   f.gross_amount, f.fees,
                   o.instrument_id, o.action,
                   i.currency AS instrument_currency
            FROM fills f
            JOIN orders o ON o.order_id = f.order_id
            JOIN instruments i ON i.instrument_id = o.instrument_id
            WHERE f.fill_id NOT IN (
                SELECT reference_fill_id
                FROM tax_lots
                WHERE reference_fill_id IS NOT NULL
            )
              AND f.units > 0
            ORDER BY f.filled_at ASC
            """
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    # Resolve FX rates (read phase — before transaction)
    # Collect unique (uk_date, currency) pairs
    rate_cache: dict[tuple[date, str], Decimal] = {}
    for row in rows:
        uk_dt = _to_uk_date(row["filled_at"])
        currency = row["instrument_currency"] or "USD"
        key = (uk_dt, currency.upper())
        if key not in rate_cache:
            rate_cache[key] = _load_fx_rate(conn, uk_dt, currency)

    # Write phase — single atomic transaction
    written = 0
    with conn.transaction():
        for row in rows:
            action: str = row["action"]
            gross_amount = Decimal(str(row["gross_amount"]))
            fees = Decimal(str(row["fees"]))
            units = Decimal(str(row["units"]))
            filled_at: datetime = row["filled_at"]
            uk_dt = _to_uk_date(filled_at)
            currency = (row["instrument_currency"] or "USD").upper()
            fx_rate = rate_cache[(uk_dt, currency)]

            if action in ("BUY", "ADD"):
                direction: TaxEventDirection = "acquisition"
                # cost_or_proceeds = gross + fees (total allowable cost)
                # gross_amount (USD) + fees (USD) = total_cost (USD)
                cost_or_proceeds = gross_amount + fees
            elif action == "EXIT":
                direction = "disposal"
                # cost_or_proceeds = gross - fees (net proceeds)
                # gross_amount (USD) - fees (USD) = net_proceeds (USD)
                cost_or_proceeds = gross_amount - fees
            else:
                logger.warning(
                    "_ingest_fills: unknown action=%s for fill_id=%d, skipping",
                    action,
                    row["fill_id"],
                )
                continue

            # total (USD) * rate (GBP/USD) = amount (GBP)
            amount_gbp = cost_or_proceeds * fx_rate
            tax_year = _compute_tax_year(uk_dt)

            conn.execute(
                """
                INSERT INTO tax_lots (
                    instrument_id, event_time, event_type, direction,
                    quantity, cost_or_proceeds,
                    original_currency, fx_rate_to_gbp, amount_gbp,
                    tax_year, reference_fill_id
                )
                VALUES (
                    %(instrument_id)s, %(event_time)s, %(event_type)s,
                    %(direction)s, %(quantity)s, %(cost_or_proceeds)s,
                    %(original_currency)s, %(fx_rate)s, %(amount_gbp)s,
                    %(tax_year)s, %(fill_id)s
                )
                ON CONFLICT (reference_fill_id)
                    WHERE reference_fill_id IS NOT NULL
                DO NOTHING
                """,
                {
                    "instrument_id": row["instrument_id"],
                    "event_time": filled_at,
                    "event_type": action,
                    "direction": direction,
                    "quantity": units,
                    "cost_or_proceeds": cost_or_proceeds,
                    "original_currency": currency,
                    "fx_rate": fx_rate,
                    "amount_gbp": amount_gbp,
                    "tax_year": tax_year,
                    "fill_id": row["fill_id"],
                },
            )
            written += 1

    return written


def _ingest_cash_events(conn: psycopg.Connection[Any]) -> int:
    """Ingest dividend/fee events from cash_ledger into tax_lots.

    Stub: returns 0 today. When cash_ledger gains dividend/fee event_types
    with instrument references, this will query and ingest them.

    # TODO: dedup key for cash-event ingestion (#11 follow-up)
    # reference_fill_id only covers fills. Cash-ledger events will need
    # their own dedup key — likely (event_type, event_time, instrument_id,
    # amount) natural key or a dedicated reference_event_id column.
    """
    _ = conn
    return 0


# ---------------------------------------------------------------------------
# Disposal matching — pure functions
# ---------------------------------------------------------------------------


def _match_disposals_for_instrument(
    acquisitions: list[TaxLot],
    disposals: list[TaxLot],
) -> tuple[list[DisposalMatch], PoolState]:
    """Pure function: match disposals against acquisitions using HMRC rules.

    Returns (matches, final_pool_state).
    All date comparisons use uk_date (Europe/London calendar date).
    """
    if not disposals:
        # No disposals — just compute the pool from all acquisitions
        pool = MutablePoolState(units=_D("0"), cost_gbp=_D("0"))
        for acq in acquisitions:
            # per_unit_cost (GBP/unit) = amount_gbp (GBP) / quantity (units)
            per_unit = acq.amount_gbp / acq.quantity
            pool.units += acq.quantity
            # quantity (units) * per_unit (GBP/unit) = cost (GBP)
            pool.cost_gbp += acq.quantity * per_unit
        instrument_id = acquisitions[0].instrument_id if acquisitions else 0
        return [], PoolState(
            instrument_id=instrument_id,
            units=pool.units,
            cost_gbp=pool.cost_gbp,
            avg_cost_gbp=pool.avg_cost_gbp,
        )

    instrument_id = disposals[0].instrument_id

    # remaining_acq is the single source of truth for unmatched units
    remaining_acq: dict[int, Decimal] = {a.tax_lot_id: a.quantity for a in acquisitions}
    in_pool: set[int] = set()  # lot IDs whose units have been added to pool
    pool = MutablePoolState(units=_D("0"), cost_gbp=_D("0"))
    matches: list[DisposalMatch] = []

    acq_sorted = sorted(acquisitions, key=lambda a: (a.uk_date, a.event_time))
    disposals_sorted = sorted(disposals, key=lambda d: (d.uk_date, d.event_time))

    for disposal in disposals_sorted:
        d_uk = disposal.uk_date
        disposal_remaining = disposal.quantity

        # Step 1: add pre-disposal acquisitions to pool (excluding same-day)
        for acq in acq_sorted:
            if acq.tax_lot_id in in_pool:
                continue
            if acq.uk_date >= d_uk:
                break  # same-day and future — skip for now
            remaining = remaining_acq[acq.tax_lot_id]
            if remaining <= 0:
                in_pool.add(acq.tax_lot_id)
                continue
            # per_unit_cost (GBP/unit) = amount_gbp (GBP) / quantity (units)
            per_unit = acq.amount_gbp / acq.quantity
            # remaining (units) * per_unit (GBP/unit) = cost (GBP)
            pool.units += remaining
            pool.cost_gbp += remaining * per_unit
            in_pool.add(acq.tax_lot_id)

        # Step 2: same-day rule
        same_day_acqs = [a for a in acq_sorted if a.uk_date == d_uk and remaining_acq[a.tax_lot_id] > 0]
        for acq in same_day_acqs:
            if disposal_remaining <= 0:
                break
            match_units = min(disposal_remaining, remaining_acq[acq.tax_lot_id])
            # per_unit_cost (GBP/unit) = amount_gbp (GBP) / quantity (units)
            per_unit_cost = acq.amount_gbp / acq.quantity
            # match_units (units) * per_unit_cost (GBP/unit) = acq_cost (GBP)
            acq_cost = match_units * per_unit_cost
            # per_unit_proceeds (GBP/unit) = amount_gbp (GBP) / quantity (units)
            per_unit_proceeds = disposal.amount_gbp / disposal.quantity
            # match_units (units) * per_unit_proceeds (GBP/unit) = proceeds (GBP)
            disp_proceeds = match_units * per_unit_proceeds

            matches.append(
                DisposalMatch(
                    disposal_lot=disposal,
                    acquisition_lot=acq,
                    matching_rule="same_day",
                    matched_units=match_units,
                    acquisition_cost_gbp=acq_cost,
                    disposal_proceeds_gbp=disp_proceeds,
                    gain_or_loss_gbp=disp_proceeds - acq_cost,
                )
            )
            remaining_acq[acq.tax_lot_id] -= match_units
            disposal_remaining -= match_units

        # After same-day matching: remaining same-day units enter the pool
        for acq in same_day_acqs:
            if acq.tax_lot_id in in_pool:
                continue
            remaining = remaining_acq[acq.tax_lot_id]
            if remaining > 0:
                per_unit = acq.amount_gbp / acq.quantity
                pool.units += remaining
                pool.cost_gbp += remaining * per_unit
            in_pool.add(acq.tax_lot_id)

        # Step 3: 30-day rule (bed and breakfast)
        if disposal_remaining > 0:
            window_start = d_uk + timedelta(days=1)
            window_end = d_uk + timedelta(days=30)
            bnb_acqs = sorted(
                [
                    a
                    for a in acquisitions
                    if window_start <= a.uk_date <= window_end and remaining_acq[a.tax_lot_id] > 0
                ],
                key=lambda a: (a.uk_date, a.event_time),
            )
            for acq in bnb_acqs:
                if disposal_remaining <= 0:
                    break
                match_units = min(disposal_remaining, remaining_acq[acq.tax_lot_id])
                per_unit_cost = acq.amount_gbp / acq.quantity
                acq_cost = match_units * per_unit_cost
                per_unit_proceeds = disposal.amount_gbp / disposal.quantity
                disp_proceeds = match_units * per_unit_proceeds

                matches.append(
                    DisposalMatch(
                        disposal_lot=disposal,
                        acquisition_lot=acq,
                        matching_rule="bed_and_breakfast",
                        matched_units=match_units,
                        acquisition_cost_gbp=acq_cost,
                        disposal_proceeds_gbp=disp_proceeds,
                        gain_or_loss_gbp=disp_proceeds - acq_cost,
                    )
                )
                remaining_acq[acq.tax_lot_id] -= match_units
                # remaining_acq still holds any leftover units — they enter
                # the pool when Step 1 of a later disposal reaches this date
                disposal_remaining -= match_units

        # Step 4: Section 104 pool
        if disposal_remaining > 0 and pool.units > 0:
            match_units = min(disposal_remaining, pool.units)
            # avg_cost (GBP/unit) = pool_cost (GBP) / pool_units (units)
            avg_cost = pool.avg_cost_gbp
            # match_units (units) * avg_cost (GBP/unit) = acq_cost (GBP)
            acq_cost = match_units * avg_cost
            per_unit_proceeds = disposal.amount_gbp / disposal.quantity
            disp_proceeds = match_units * per_unit_proceeds

            matches.append(
                DisposalMatch(
                    disposal_lot=disposal,
                    acquisition_lot=None,
                    matching_rule="s104_pool",
                    matched_units=match_units,
                    acquisition_cost_gbp=acq_cost,
                    disposal_proceeds_gbp=disp_proceeds,
                    gain_or_loss_gbp=disp_proceeds - acq_cost,
                )
            )
            pool.units -= match_units
            pool.cost_gbp -= acq_cost
            disposal_remaining -= match_units

        if disposal_remaining > 0:
            logger.warning(
                "Disposal lot %d has %s unmatched units for instrument %d. This indicates incomplete acquisition data.",
                disposal.tax_lot_id,
                disposal_remaining,
                instrument_id,
            )

    # After all disposals: add any remaining acquisitions to pool
    for acq in acq_sorted:
        if acq.tax_lot_id in in_pool:
            continue
        remaining = remaining_acq[acq.tax_lot_id]
        if remaining > 0:
            per_unit = acq.amount_gbp / acq.quantity
            pool.units += remaining
            pool.cost_gbp += remaining * per_unit
        in_pool.add(acq.tax_lot_id)

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
        cur.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN gain_or_loss_gbp > 0
                    THEN gain_or_loss_gbp ELSE 0 END), 0) AS total_gains,
                COALESCE(SUM(CASE WHEN gain_or_loss_gbp < 0
                    THEN gain_or_loss_gbp ELSE 0 END), 0) AS total_losses,
                COALESCE(SUM(gain_or_loss_gbp), 0) AS net_gain,
                COUNT(*) FILTER (WHERE matching_rule = 'same_day')
                    AS cnt_same_day,
                COUNT(*) FILTER (WHERE matching_rule = 'bed_and_breakfast')
                    AS cnt_bnb,
                COUNT(*) FILTER (WHERE matching_rule = 's104_pool')
                    AS cnt_s104
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
            SELECT gain_or_loss_gbp, disposal_uk_date
            FROM disposal_matches
            WHERE tax_year = %(ty)s
              AND gain_or_loss_gbp > 0
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

    # Per-match weighted CGT estimates
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

    return TaxYearSummary(
        tax_year=tax_year,
        total_gains_gbp=total_gains,
        total_losses_gbp=total_losses,
        net_gain_gbp=net_gain,
        dividend_total_gbp=dividend_total,
        disposals_same_day=int(agg["cnt_same_day"]),
        disposals_bed_and_breakfast=int(agg["cnt_bnb"]),
        disposals_s104=int(agg["cnt_s104"]),
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
