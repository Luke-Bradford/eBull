"""Compact official account-equity evidence for Foundation F-0 (#2559)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerInstrumentInvestment
from app.services.fx import FxRateNotFound, convert
from app.services.fx_history import load_fx_rates_for_date

SOURCE_VERSION = "etoro-pnl-v1"

# eToro's `trading--demo/get-account-pnl-and-portfolio-details` response schema documents
# `clientPortfolio.accountCurrencyId` as "Currency ID of the account (1 = USD)" (portal
# fetched 2026-08-13; recorded in .claude/skills/data-sources/etoro-api.md).  ONE id is
# documented, so exactly one is mapped.  Adding a member REQUIRES a portal citation --
# an id whose code we infer is an assumption wearing a measurement's clothes, which is
# the defect #2602 item 2 exists to remove.  Widening this does NOT widen what may be
# traded: the deployment / pool / core-mandate authorities keep their own USD CHECKs.
#
# It DOES require a migration in the same PR.  `broker_account_equity_snapshots_currency_
# observed` (sql/341) enumerates the documented ids literally, and its ELSE branch
# demands `currency IS NULL` -- so a member added here without widening that CHECK makes
# every write in the new currency fail closed, silently, and only once the account stops
# being USD.  `test_every_documented_currency_id_is_admitted_by_the_check` parametrizes
# off this dict so the drift fails there first.
DOCUMENTED_ACCOUNT_CURRENCIES: dict[int, str] = {1: "USD"}


class AccountEquityEvidenceError(ValueError):
    """An official account snapshot cannot be trusted or persisted."""


#: The declared reconciliation rule, returned with every verdict.
#:
#: ⚠ No published rule fixes a broker-reconciliation tolerance; searched, none exists,
#: and none is borrowed (an earlier draft cited SEC Reg NMS Rule 612 and was withdrawn
#: — Rule 612 governs the increments on which NMS stocks may be QUOTED, not how far two
#: feeds valuing the same holding may differ, and it does not reach CFDs). The rule is
#: therefore fixed BY CONSTRUCTION at the tightest bound defensible without measurement:
#: ``portfolio_eod.MARK_ROUNDING_PER_UNIT`` per unit held, plus one cent of cash.
#:
#: ⚠⚠ Widening it is a VERSION BUMP, never an edit to a constant. Editing a constant in
#: place silently re-verdicts every past comparison, including ones already read.
RECONCILIATION_RULE_VERSION = "f0-reconcile-v1"

#: Cash leg of the tolerance. Both sides carry the same ledger in the same currency, so
#: only rounding is allowed for -- the line `portfolio_sync._CASH_SYNC_TOLERANCE` already
#: holds for the same decision on the same ledger.
CASH_ROUNDING_TOLERANCE = Decimal("0.01")


@dataclass(frozen=True)
class AccountEquityEvidence:
    """The official/local comparison, its verdict, and everything that blocked it.

    ⚠ ``difference`` is NOT ``equity - total_value``. Those two value different
    populations: eToro's published formula folds copy-trader MIRRORS and PENDING ORDERS
    into ``total_invested`` and hence into ``equity``, while the local end-of-day total
    values direct positions and ``cash_ledger`` only. Measured 2026-08-19,
    ``total_invested`` was 104,060.06 USD against 64,529.06 in direct positions -- so the
    old subtraction would have declared a tolerance across a 39.8%-of-equity structural
    gap. ``difference`` compares against ``official_comparand`` instead, and the folded
    remainder is reported separately as ``residual_not_in_local_book``.

    ⚠ ``residual_not_in_local_book`` is a RESIDUAL, not an attribution. It is dominated
    by mirrors and pending orders, but it also absorbs any provider-parse or valuation
    error on the official side, so neither this field nor the UI may claim it *is* the
    non-engine holdings.

    ⚠ The money fields stay populated on a ``refused`` verdict wherever they are
    computable -- the operator repairing the condition needs the numbers, and today every
    real row is refused, so blanking them would ship an empty panel. The invariant is
    therefore an IMPLICATION and deliberately not a biconditional:

        ``comparable`` is true ==> ``difference`` and ``tolerance`` are both non-NULL.

    The converse does NOT hold. ``official_pending_orders_outstanding`` and
    ``mark_rounding_tolerance_not_recorded`` can fire while ``difference`` is a perfectly
    good number. ``comparable`` is the single load-bearing flag: a populated
    ``difference`` beside ``comparable = False`` is a diagnostic, not a verdict, and no
    consumer may read it as one.
    """

    status: Literal["unavailable", "collecting"]
    reconciliation_state: Literal["unavailable", "refused", "reconciled", "diverged"]
    reconciliation_rule_version: str
    days_collected: int
    snapshot_date: date | None
    observed_at: datetime | None
    account_currency_id: int | None
    currency: str | None
    official_equity: Decimal | None
    official_available_cash: Decimal | None
    official_total_invested: Decimal | None
    official_unrealised_pnl: Decimal | None
    official_direct_long_market_value: Decimal | None
    official_comparand: Decimal | None
    residual_not_in_local_book: Decimal | None
    local_eod_currency: str | None
    local_eod_value: Decimal | None
    local_eod_value_in_account_currency: Decimal | None
    local_eod_positions_priced: int | None
    local_eod_stale_mark_positions: int | None
    difference: Decimal | None
    tolerance: Decimal | None
    comparable: bool
    incomplete_reasons: tuple[str, ...]


def _validate_snapshot(snapshot: BrokerAccountRiskSnapshot) -> int:
    """Refuse an untrustworthy snapshot; return the observed account currency id."""
    values = (
        snapshot.available_cash,
        snapshot.total_invested,
        snapshot.unrealized_pnl,
        snapshot.equity,
    )
    if snapshot.observed_at.tzinfo is None:
        raise AccountEquityEvidenceError("observed_at must be timezone-aware")
    if not all(value.is_finite() for value in values):
        raise AccountEquityEvidenceError("account equity values must be finite")
    if snapshot.available_cash < 0 or snapshot.total_invested < 0 or snapshot.equity <= 0:
        raise AccountEquityEvidenceError("account equity values are outside safe bounds")
    if abs(snapshot.equity - snapshot.available_cash - snapshot.total_invested - snapshot.unrealized_pnl) > Decimal(
        "0.000001"
    ):
        raise AccountEquityEvidenceError("account equity components do not reconcile")
    if snapshot.pending_order_amount is not None and (
        not snapshot.pending_order_amount.is_finite() or snapshot.pending_order_amount < 0
    ):
        # eToro accumulates pending order amounts additively and SUBTRACTS the total
        # from credit, so a negative one would silently ADD to available cash.
        raise AccountEquityEvidenceError("pending order amount is outside safe bounds")
    if snapshot.account_currency_id is None:
        # #2602 item 2.  The alternative -- store it and stamp 'USD' -- is what this
        # table did until now, and it makes the assumption indistinguishable from an
        # observation forever, because the raw payload is deliberately not retained.
        raise AccountEquityEvidenceError("account currency was not reported; refusing to assume one")
    return snapshot.account_currency_id


@dataclass(frozen=True)
class DirectPositionTotals:
    """The DIRECT half of the official snapshot — mirrors and pending orders excluded.

    #2602 item 4. ``BrokerAccountRiskSnapshot.total_invested`` is not this: eToro's
    published formula folds copy-trader mirrors and pending orders into it, and on
    this account that is most of the number (2026-08-19: ``total_invested``
    104,060.06 USD against 64,529.06 in direct positions). Reconciling the local
    ledger — which values direct positions only — against the folded total compares
    two different populations and would declare a tolerance on a 39.8% mismatch.

    ⚠ ``long_market_value`` is a MARKET VALUE and ``total_invested`` is COST. They
    are not two roundings of one figure; see ``BrokerInstrumentInvestment`` (#2704),
    where all 38 reported instruments disagreed and 33 had no direct position at all.
    """

    long_market_value: Decimal
    long_positions: int
    short_positions: int


def summarise_direct_positions(
    investments: tuple[BrokerInstrumentInvestment, ...],
) -> DirectPositionTotals:
    """Reduce the per-instrument official rows to the direct-position comparand.

    Pure. ⚠ Short positions are COUNTED and not VALUED, because
    ``direct_long_market_value`` covers longs only. The count exists so the reader
    can refuse a comparison it cannot complete, rather than silently under-state the
    official side by the whole of a short book.
    """
    return DirectPositionTotals(
        long_market_value=sum((investment.direct_long_market_value for investment in investments), Decimal("0")),
        long_positions=sum(investment.direct_long_positions for investment in investments),
        short_positions=sum(investment.direct_short_positions for investment in investments),
    )


def record_account_equity_snapshot(
    conn: psycopg.Connection[Any],
    *,
    environment: Literal["demo", "real"],
    snapshot: BrokerAccountRiskSnapshot,
) -> bool:
    """Store at most the newest official observation for one UTC day.

    A currency id the portal does not document is stored WITH a NULL code rather
    than dropped: the money columns are still true, and losing the row would hide
    the one fact that matters most -- that this account is not the USD account
    every capital authority assumes.
    """
    account_currency_id = _validate_snapshot(snapshot)
    direct = summarise_direct_positions(snapshot.instrument_investments)
    observed_at = snapshot.observed_at.astimezone(UTC)
    row = conn.execute(
        """
        INSERT INTO broker_account_equity_snapshots (
            environment,snapshot_date,observed_at,source_version,account_currency_id,currency,
            available_cash,total_invested,unrealised_pnl,equity,
            official_direct_long_market_value,official_direct_long_positions,
            official_direct_short_positions,official_pending_order_amount
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (environment,snapshot_date) DO UPDATE SET
            observed_at=EXCLUDED.observed_at,
            source_version=EXCLUDED.source_version,
            account_currency_id=EXCLUDED.account_currency_id,
            currency=EXCLUDED.currency,
            available_cash=EXCLUDED.available_cash,
            total_invested=EXCLUDED.total_invested,
            unrealised_pnl=EXCLUDED.unrealised_pnl,
            equity=EXCLUDED.equity,
            official_direct_long_market_value=EXCLUDED.official_direct_long_market_value,
            official_direct_long_positions=EXCLUDED.official_direct_long_positions,
            official_direct_short_positions=EXCLUDED.official_direct_short_positions,
            official_pending_order_amount=EXCLUDED.official_pending_order_amount,
            recorded_at=now()
        WHERE EXCLUDED.observed_at > broker_account_equity_snapshots.observed_at
          AND broker_account_equity_snapshots.snapshot_date=(now() AT TIME ZONE 'UTC')::date
        RETURNING snapshot_date
        """,
        (
            environment,
            observed_at.date(),
            observed_at,
            SOURCE_VERSION,
            account_currency_id,
            DOCUMENTED_ACCOUNT_CURRENCIES.get(account_currency_id),
            snapshot.available_cash,
            snapshot.total_invested,
            snapshot.unrealized_pnl,
            snapshot.equity,
            direct.long_market_value,
            direct.long_positions,
            direct.short_positions,
            snapshot.pending_order_amount,
        ),
    ).fetchone()
    return row is not None


def mark_effectiveness_reasons(
    *,
    snapshot_date: date,
    oldest_mark_date: date | None,
    positions_priced: int,
) -> tuple[str, ...]:
    """Name what is unknown about WHEN the local valuation's marks were effective.

    #2602 item 4. Until ``sql/350`` this function did not exist and its caller
    appended ``local_eod_effective_time_unknown`` **unconditionally**, on the
    reasoning that ``computed_at`` records when the local job ran rather than
    when its closing prices were effective. That was true, and it was permanent
    by construction — nothing recorded the marks' dates, so no evidence could
    ever retire the caveat. ``portfolio_eod`` now stores them, so the caveat is
    measured and usually absent.

    ⚠ A DATE, not a timestamp, is the defensible effective time here. The marks
    are daily closes, and "the close of session D" identifies a market instant
    exactly; a wall-clock stamp would add precision the input does not have.
    What the date does NOT settle is whether a same-day bar was final when it was
    read — deliberately not modelled, because on this corpus it does not arise
    (``max(price_daily.price_date)`` trails ``current_date``, and the EOD job
    runs after the US close). Inventing a refusal for a state we have never
    observed would be the mirror of the defect this replaces.

    ⚠ ``local_eod_effective_time_unknown`` keeps its slug rather than gaining a
    clearer one. It now means exactly one thing — the row predates ``sql/350`` —
    and renaming it would make the pre-migration rows, which are the only rows it
    can describe, read as a new condition.
    """
    if oldest_mark_date is None:
        # Two shapes, one of which is not a caveat at all: no priced position
        # means nothing carried a mark, so there is no effective time to be
        # unknown (an all-cash snapshot, or one whose positions all failed to
        # price — the latter already reported by `local_eod_valuation_incomplete`).
        return ("local_eod_effective_time_unknown",) if positions_priced > 0 else ()
    if oldest_mark_date < snapshot_date:
        # The total is stamped `snapshot_date` but at least one of its inputs is
        # older, so the valuation is a blend of sessions. ⚠ The verdict is taken
        # from the DATE BOUND alone and not from `stale_mark_positions`, which
        # would be a second source of truth for the same fact and could disagree
        # with it. The count is stored for magnitude — "3 of 7 positions" is what
        # makes the caveat actionable — and is deliberately not a decision input.
        return ("local_eod_marks_carried_forward",)
    # Every priced mark is on the snapshot's own session. Nothing to caveat.
    return ()


def _decimal(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def official_direct_position_reasons(
    *,
    direct_long_market_value: Decimal | None,
    direct_long_positions: int | None,
    direct_short_positions: int | None,
    pending_order_amount: Decimal | None,
) -> tuple[str, ...]:
    """Name what stops the OFFICIAL side forming a comparand. Pure.

    ⚠ NULL is not zero on any of these. Every one is nullable only because the rows
    written before ``sql/363`` cannot be backfilled -- neither the broker's
    direct/mirror split nor its pending-order total was retained. Reading a NULL count
    as "none" would turn "never looked" into a clean bill of health on exactly the two
    columns whose safety argument is that there are none of these.
    """
    reasons: list[str] = []
    if direct_long_market_value is None or direct_long_positions is None:
        reasons.append("official_direct_position_value_not_recorded")
    elif direct_long_market_value < 0 or direct_long_positions < 0:
        # A negative direct long value is deliberately NOT refused at parse time
        # (`BrokerInstrumentInvestment`: an extreme-but-legitimate signed sum, refused
        # where it is used). This is where it is used.
        reasons.append("reconciliation_inputs_out_of_bounds")
    if direct_short_positions is None or direct_short_positions > 0:
        # `direct_long_market_value` values LONGS ONLY, so a short book would make the
        # official side under-state by its whole size against a complete local one.
        reasons.append("official_direct_short_positions_unvalued")
    elif direct_short_positions < 0:
        reasons.append("reconciliation_inputs_out_of_bounds")
    if pending_order_amount is None or pending_order_amount != 0:
        # eToro subtracts pending order amounts from `credit` to reach `available_cash`;
        # `cash_ledger` has never heard of them. The CASH legs are incomparable while
        # any are outstanding, and that presents as a valuation error if not named.
        reasons.append("official_pending_orders_outstanding")
    return tuple(dict.fromkeys(reasons))


def _convert_local_total(
    conn: psycopg.Connection[Any],
    *,
    local_value: Decimal,
    mark_rounding_tolerance: Decimal | None,
    local_currency: str,
    official_currency: str,
    fx_rate_date: date | None,
) -> tuple[Decimal | None, Decimal | None, str | None]:
    """Restate the local total (and its tolerance) in the ACCOUNT currency.

    Returns ``(value, tolerance, refusal_reason)`` with exactly one of the first two
    pairs / the reason populated.

    The rates are re-loaded at the local snapshot's OWN ``fx_rate_date`` -- the
    carry-forward date the local total was built from -- so no new FX source and no new
    as-of date enters the comparison. ⚠ It is a re-load at the same date, not the
    identical in-memory dict: a later revision of a rate row would move this number.
    That is accepted and stated rather than claimed away, because the alternative
    (storing every rate used) buys precision the comparison does not need.

    ⚠ Same-currency is not an FX question. An all-USD account with a USD display
    currency has no rate to look up and a NULL ``fx_rate_date`` is not a refusal there;
    demanding one would refuse the simplest correct configuration.
    """
    if local_currency == official_currency:
        return local_value, mark_rounding_tolerance, None
    if fx_rate_date is None:
        return None, None, "account_currency_fx_rate_missing"
    rates, _ = load_fx_rates_for_date(conn, fx_rate_date)
    try:
        value = convert(local_value, local_currency, official_currency, rates)
        tolerance = (
            None
            if mark_rounding_tolerance is None
            else convert(mark_rounding_tolerance, local_currency, official_currency, rates)
        )
    except FxRateNotFound:
        return None, None, "account_currency_fx_rate_missing"
    return value, tolerance, None


def load_account_equity_evidence(
    conn: psycopg.Connection[Any], *, environment: Literal["demo", "real"]
) -> AccountEquityEvidence:
    """Return the latest official/local comparison, its verdict, and every blocker."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH latest AS (
                SELECT *,count(*) OVER () AS days_collected
                FROM broker_account_equity_snapshots
                WHERE environment=%s
                ORDER BY snapshot_date DESC
                LIMIT 1
            )
            SELECT latest.days_collected,latest.snapshot_date,latest.observed_at,latest.currency,
                   latest.available_cash,latest.total_invested,latest.unrealised_pnl,latest.equity,
                   latest.account_currency_id,
                   latest.official_direct_long_market_value,latest.official_direct_long_positions,
                   latest.official_direct_short_positions,latest.official_pending_order_amount,
                   local.display_currency,local.total_value,local.fx_rate_date,
                   coalesce(local.positions_no_price,0) > 0
                     OR coalesce(local.positions_no_fx,0) > 0
                     OR coalesce(local.cash_no_fx_currencies,0) > 0 AS local_valuation_incomplete,
                   local.oldest_mark_date,local.positions_priced,local.stale_mark_positions,
                   local.positions_total,local.mark_rounding_tolerance
            FROM latest
            LEFT JOIN portfolio_eod_snapshots local ON local.snapshot_date=latest.snapshot_date
            """,
            (environment,),
        )
        row = cur.fetchone()
    if row is None:
        return AccountEquityEvidence(
            status="unavailable",
            reconciliation_state="unavailable",
            reconciliation_rule_version=RECONCILIATION_RULE_VERSION,
            days_collected=0,
            snapshot_date=None,
            observed_at=None,
            account_currency_id=None,
            currency=None,
            official_equity=None,
            official_available_cash=None,
            official_total_invested=None,
            official_unrealised_pnl=None,
            official_direct_long_market_value=None,
            official_comparand=None,
            residual_not_in_local_book=None,
            local_eod_currency=None,
            local_eod_value=None,
            local_eod_value_in_account_currency=None,
            local_eod_positions_priced=None,
            local_eod_stale_mark_positions=None,
            difference=None,
            tolerance=None,
            comparable=False,
            incomplete_reasons=("official_account_equity_missing",),
        )

    snapshot_date = row["snapshot_date"]
    local_value = _decimal(row["total_value"])
    official_equity = Decimal(str(row["equity"]))
    official_available_cash = Decimal(str(row["available_cash"]))
    account_currency_id = None if row["account_currency_id"] is None else int(row["account_currency_id"])
    official_currency = None if row["currency"] is None else str(row["currency"])
    local_currency = None if row["display_currency"] is None else str(row["display_currency"])
    direct_long_market_value = _decimal(row["official_direct_long_market_value"])
    direct_long_positions = (
        None if row["official_direct_long_positions"] is None else int(row["official_direct_long_positions"])
    )
    direct_short_positions = (
        None if row["official_direct_short_positions"] is None else int(row["official_direct_short_positions"])
    )
    mark_rounding_tolerance = _decimal(row["mark_rounding_tolerance"])

    reasons: list[str] = []
    if account_currency_id is None:
        # Written before #2602 item 2; its 'USD' is this codebase's assumption, and the
        # payload that would settle it was never retained.  Permanent, not pending.
        reasons.append("account_currency_assumed_not_observed")
    elif official_currency is None:
        # The broker named a currency id we have no documented code for.  Every capital
        # authority is USD-locked, so this is the loudest fact on the panel.
        reasons.append("account_currency_not_documented")
    reasons.extend(
        official_direct_position_reasons(
            direct_long_market_value=direct_long_market_value,
            direct_long_positions=direct_long_positions,
            direct_short_positions=direct_short_positions,
            pending_order_amount=_decimal(row["official_pending_order_amount"]),
        )
    )

    local_in_account_currency: Decimal | None = None
    tolerance: Decimal | None = None
    if local_value is None:
        reasons.append("same_day_local_eod_snapshot_missing")
    else:
        if bool(row["local_valuation_incomplete"]):
            reasons.append("local_eod_valuation_incomplete")
        reasons.extend(
            mark_effectiveness_reasons(
                snapshot_date=snapshot_date,
                oldest_mark_date=row["oldest_mark_date"],
                positions_priced=int(row["positions_priced"]),
            )
        )
        if mark_rounding_tolerance is None:
            reasons.append("mark_rounding_tolerance_not_recorded")
        elif mark_rounding_tolerance < 0:
            reasons.append("reconciliation_inputs_out_of_bounds")
        if (
            direct_long_positions is not None
            and direct_short_positions is not None
            and row["positions_total"] is not None
            and int(row["positions_total"]) != direct_long_positions + direct_short_positions
        ):
            # ⚠ Two sums can agree while the books disagree: one MISSING holding and one
            # EXTRA holding of equal value net to `reconciled`. The counts are the
            # structural check the value comparison cannot perform on itself.
            reasons.append("direct_position_count_mismatch")
        # `local_eod_currency_mismatch` is NOT raised here any more. A GBP display
        # currency against a USD account is the ordinary configured state (the operator
        # picks the display currency), not a defect -- it only blocks the comparison
        # when no rate bridges it, which is what the reason below actually says.
        if official_currency is not None and local_currency is not None:
            local_in_account_currency, converted_tolerance, fx_reason = _convert_local_total(
                conn,
                local_value=local_value,
                mark_rounding_tolerance=mark_rounding_tolerance,
                local_currency=local_currency,
                official_currency=official_currency,
                fx_rate_date=row["fx_rate_date"],
            )
            if fx_reason is not None:
                reasons.append(fx_reason)
            elif converted_tolerance is not None:
                tolerance = converted_tolerance + CASH_ROUNDING_TOLERANCE

    official_comparand = (
        None if direct_long_market_value is None else official_available_cash + direct_long_market_value
    )
    difference = (
        None
        if official_comparand is None or local_in_account_currency is None
        else official_comparand - local_in_account_currency
    )
    incomplete_reasons = tuple(dict.fromkeys(reasons))
    decided = not incomplete_reasons and difference is not None and tolerance is not None
    reconciliation_state: Literal["unavailable", "refused", "reconciled", "diverged"]
    if not decided:
        reconciliation_state = "refused"
    elif abs(difference or Decimal("0")) <= (tolerance or Decimal("0")):
        reconciliation_state = "reconciled"
    else:
        reconciliation_state = "diverged"
    return AccountEquityEvidence(
        status="collecting",
        reconciliation_state=reconciliation_state,
        reconciliation_rule_version=RECONCILIATION_RULE_VERSION,
        days_collected=int(row["days_collected"]),
        snapshot_date=snapshot_date,
        observed_at=row["observed_at"],
        account_currency_id=account_currency_id,
        currency=official_currency,
        official_equity=official_equity,
        official_available_cash=official_available_cash,
        official_total_invested=Decimal(str(row["total_invested"])),
        official_unrealised_pnl=Decimal(str(row["unrealised_pnl"])),
        official_direct_long_market_value=direct_long_market_value,
        official_comparand=official_comparand,
        residual_not_in_local_book=(None if official_comparand is None else official_equity - official_comparand),
        local_eod_currency=local_currency,
        local_eod_value=local_value,
        local_eod_value_in_account_currency=local_in_account_currency,
        local_eod_positions_priced=None if row["positions_priced"] is None else int(row["positions_priced"]),
        local_eod_stale_mark_positions=(
            None if row["stale_mark_positions"] is None else int(row["stale_mark_positions"])
        ),
        difference=difference,
        tolerance=tolerance,
        comparable=decided,
        incomplete_reasons=incomplete_reasons,
    )
