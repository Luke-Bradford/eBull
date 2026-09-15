"""Compact official account-equity evidence for Foundation F-0 (#2559)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerDirectPositionInvestment,
    BrokerInstrumentInvestment,
)
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
#:
#: ⚠⚠ ``v2`` (2026-09-15, #3068) — the COMPARAND changed, not the tolerance. ``v1``
#: compared the broker's 23:55 UTC valuation against the same holdings marked at the
#: regular-session close, and its one-cent-per-unit allowance modelled rounding of one
#: mark against the SAME mark, with no term for the two being struck at different
#: instants. Measured on 2026-09-14, the countdown's first session: ``diverged``,
#: ``difference`` −204.65 against ``tolerance`` 31.56, no incomplete reasons, and the
#: whole of it attributable to two evening-quoted names. ``v2`` re-prices the local book
#: at the broker's own published ``closeRate`` per position, so our mark cancels out of
#: the comparand entirely. The bump RESETS THE COUNTDOWN by construction, which is
#: intended (settled decision 2026-09-13) and free today: both stored greens (08-24,
#: 08-25) are already outside ``MAX_EVIDENCE_AGE_DAYS``.
RECONCILIATION_RULE_VERSION = "f0-reconcile-v2"

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
    #: The local book RE-PRICED at the broker's own per-position marks — the actual left
    #: operand of ``difference`` since ``f0-reconcile-v2`` (#3068).
    #:
    #: ⚠ A SEPARATE field rather than a redefinition of the one above, deliberately.
    #: ``local_eod_value_in_account_currency`` is the stored end-of-day total converted,
    #: and the panel already shows it; silently changing what it means would leave a
    #: number the operator recognises standing beside a ``difference`` it no longer
    #: explains. ``difference = official_comparand − local_eod_value_at_official_marks``
    #: and that subtraction must be checkable on the panel.
    local_eod_value_at_official_marks: Decimal | None
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

    ⚠⚠ The per-position child rows (#3068, sql/383) are replaced WHOLESALE exactly
    when the parent accepts the write, and left untouched when it does not. The
    parent is an ``ON CONFLICT DO UPDATE`` guarded by ``EXCLUDED.observed_at >``
    the stored one AND "today", so a stale or historical observation is silently
    rejected -- and writing its children anyway would pair one snapshot's totals
    with a different snapshot's positions, which is a worse state than having no
    children at all. ``RETURNING`` already reports which happened, so the decision
    is read from the database rather than re-derived from the timestamps.
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
    if row is None:
        return False
    _replace_position_marks(
        conn,
        environment=environment,
        snapshot_date=observed_at.date(),
        positions=snapshot.direct_positions,
    )
    return True


def _replace_position_marks(
    conn: psycopg.Connection[Any],
    *,
    environment: str,
    snapshot_date: date,
    positions: tuple[BrokerDirectPositionInvestment, ...],
) -> None:
    """Make the stored child set exactly this snapshot's positions (#3068).

    DELETE-then-INSERT rather than an upsert: a position CLOSED since the previous
    write of the same day must disappear, and an upsert has no arm that removes it.
    The caller holds the same transaction as the parent write, so the set is never
    observable half-replaced.

    ⚠ SHORTS ARE STORED TOO, even though ``official_direct_long_market_value``
    values longs only. The parent's short arm is deliberately a COUNT -- "no
    monetary sum can carry 'a short exists'" -- and that argument is about the
    AGGREGATE. Per position there is no ambiguity to protect against, and dropping
    shorts here would make the child set silently not-the-book, which is the one
    property a reconciliation reads it for.
    """
    conn.execute(
        "DELETE FROM broker_account_position_marks WHERE environment=%s AND snapshot_date=%s",
        (environment, snapshot_date),
    )
    if not positions:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO broker_account_position_marks (
                environment,snapshot_date,position_id,instrument_id,is_buy,
                units,amount,unrealized_pnl,market_value,is_partially_altered,
                close_rate,close_conversion_rate,asset_currency_id,pnl_timestamp
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            [
                (
                    environment,
                    snapshot_date,
                    position.position_id,
                    position.instrument_id,
                    position.is_buy,
                    position.units,
                    position.amount,
                    position.unrealized_pnl,
                    position.market_value,
                    position.is_partially_altered,
                    position.close_rate,
                    position.close_conversion_rate,
                    position.asset_currency_id,
                    position.pnl_timestamp,
                )
                for position in positions
            ],
        )


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


@dataclass(frozen=True)
class OfficialPositionMark:
    """One official per-position mark, as stored by ``_replace_position_marks``.

    ⚠ Every operand is OPTIONAL on this type and required by the rule. The columns are
    nullable because ``sql/385`` could not backfill them, so "the row exists" and "the row
    is usable" are different questions and the type must be able to express the gap.
    """

    position_id: int
    instrument_id: int
    is_buy: bool
    close_rate: Decimal | None
    close_conversion_rate: Decimal | None
    asset_currency_id: int | None


@dataclass(frozen=True)
class LocalPositionMark:
    """One local end-of-day position, as ``portfolio_eod._write_snapshot`` stored it."""

    position_id: int
    instrument_id: int
    is_buy: bool | None
    units: Decimal | None
    close_price: Decimal | None
    native_currency: str | None
    price_status: str


@dataclass(frozen=True)
class MarkSubstitution:
    """The account-currency correction that re-prices the local book at broker marks."""

    #: ``None`` exactly when ``reasons`` is non-empty.
    correction: Decimal | None
    reasons: tuple[str, ...]


def _usable(value: Decimal | None) -> bool:
    """A money/price operand that may be multiplied. ⚠ ``is_finite`` is NOT redundant with
    the table's ``CHECK (... > 0)``: PostgreSQL ``numeric`` admits ``NaN`` and orders it
    ABOVE every non-NaN value, so ``NaN > 0`` is TRUE and passes the constraint."""
    return value is not None and value.is_finite()


def substitute_official_marks(
    *,
    official: Sequence[OfficialPositionMark],
    local: Sequence[LocalPositionMark],
    declared_long: int | None,
    declared_short: int | None,
) -> MarkSubstitution:
    """Re-price the local book at the broker's own marks. Pure (#3068).

    The local end-of-day value of a position is ``amount + s × units × (close_local −
    open_rate)`` (``portfolio_eod.compute_eod_equity``, ``s = +1`` long / ``−1`` short).
    Replacing our close with the broker's published one adds

        ``s × units_local × (close_rate_official − close_local) × close_conversion_rate``

    and nothing else: ``amount`` and ``open_rate`` are untouched, so no relationship
    between them is assumed, and ``close_local`` cancels — it appears once with each sign
    and the subtraction is exact in ``Decimal``. The comparand therefore does not depend
    on our marks, which is the entire point of #3068 and why the two mark-effectiveness
    caveats no longer gate this comparison.

    ⚠ The cancellation is exact in the ARITHMETIC and not quite exact END TO END, because
    the left-hand side is read back from ``portfolio_eod_snapshots.total_value``, which is
    ``NUMERIC(20,4)``. The stored total therefore contributes up to half a unit in its
    last decimal place, converted at the display→account rate. Measured on the 2026-09-14
    shape that residual is 2.9e-5 USD against a 31.55 tolerance; it is stated rather than
    claimed away, because "cancels exactly" is the kind of sentence a later reader builds
    a tighter bound on.

    ⚠⚠ ``(amount + unrealized_pnl) / units`` is NOT an acceptable substitute for
    ``close_rate`` and this function must never be rewritten to use it. That quotient is
    equity per unit; ``amount`` is documented to include "additional margin allocated to
    the position as collateral", so it equals ``units × openRate`` only at leverage 1 with
    no added collateral. At leverage 2 it invents a difference on a healthy position, and
    at leverage 1 the substitution it feeds reduces algebraically to
    ``units × open_rate_local − amount_local`` — identically zero on an unleveraged book,
    i.e. a comparison that cannot fail. Both measured; see the proposal's revision 4.

    ⚠ The broker's own ``close_conversion_rate`` is used rather than our daily ECB rate,
    so the correction and the rate applied to it come from ONE observation. Our rates keep
    converting the stored local total, which is unchanged behaviour.

    Evaluated in a fixed precedence — evidence presence, then operand validity, then
    identity, then set membership, then arithmetic — so that a legacy row's ABSENCE can
    never present as a fabricated missing-position finding, and nothing multiplies or
    divides before its operands have been checked.
    """
    reasons: list[str] = []
    declared = None if declared_long is None or declared_short is None else declared_long + declared_short

    if not official:
        # ⚠ Zero children is ambiguous on its own and the parent is the discriminator
        # (sql/383). An empty official book would otherwise reconcile against an empty
        # local side and manufacture a green out of a snapshot that recorded nothing.
        if declared is None or declared > 0:
            return MarkSubstitution(None, ("official_position_marks_not_recorded",))
        if local:
            return MarkSubstitution(None, ("local_position_missing_officially",))
        return MarkSubstitution(Decimal("0"), ())

    official_long = sum(1 for mark in official if mark.is_buy)
    if declared is None or len(official) != declared or official_long != declared_long:
        # Catches PARTIAL child loss, which zero-child checking cannot see: a child set
        # that lost rows still pairs with matching local rows and would substitute
        # cleanly for the survivors while the parent's value covers all of them.
        reasons.append("official_position_marks_incomplete")

    by_id: dict[int, LocalPositionMark] = {row.position_id: row for row in local}
    correction = Decimal("0")
    for mark in official:
        if not _usable(mark.close_rate) or not _usable(mark.close_conversion_rate):
            reasons.append("official_position_marks_unusable")
            continue
        asset_currency = (
            None if mark.asset_currency_id is None else DOCUMENTED_ACCOUNT_CURRENCIES.get(mark.asset_currency_id)
        )
        if asset_currency is None:
            # An id with no documented code cannot be compared against the local row's
            # currency, and guessing one would make the guess indistinguishable from an
            # observation — the posture sql/341 already takes on the parent.
            reasons.append("official_position_marks_unusable")
            continue
        row = by_id.get(mark.position_id)
        if row is None:
            # In the official set, absent locally. The official comparand covers it and
            # the local total does not, so the books disagree structurally.
            reasons.append("official_position_missing_locally")
            continue
        if row.is_buy is None:
            reasons.append("local_eod_position_direction_not_recorded")
            continue
        if (
            row.instrument_id != mark.instrument_id
            or row.is_buy != mark.is_buy
            or row.native_currency != asset_currency
        ):
            # Same position id, different position. ⚠ The currency arm is not cosmetic:
            # matching ids and directions do not establish that the two sides agree on
            # what currency the price is quoted in, and the correction is a price
            # difference.
            reasons.append("position_identity_mismatch")
            continue
        if (
            row.price_status != "priced"
            or not _usable(row.close_price)
            or not _usable(row.units)
            or (row.units is not None and row.units <= 0)
        ):
            # Nothing of this position reached `positions_value`, so there is no value to
            # correct. Its absence from the total is already under-stated against an
            # official side that includes it.
            reasons.append("local_position_mark_unusable")
            continue
        assert row.units is not None and row.close_price is not None  # narrowed by `_usable`
        assert mark.close_rate is not None and mark.close_conversion_rate is not None
        sign = Decimal("1") if mark.is_buy else Decimal("-1")
        correction += sign * row.units * (mark.close_rate - row.close_price) * mark.close_conversion_rate

    official_ids = {mark.position_id for mark in official}
    if any(row.position_id not in official_ids for row in local):
        # Locally held, no official mark: its value sits in the local total at OUR close
        # with nothing to substitute, so the comparand as defined is not computable.
        reasons.append("local_position_missing_officially")

    if reasons:
        return MarkSubstitution(None, tuple(dict.fromkeys(reasons)))
    return MarkSubstitution(correction, ())


def _reread_write_stamps(
    conn: psycopg.Connection[Any], *, environment: str, snapshot_date: date
) -> tuple[datetime | None, datetime | None]:
    """``(broker recorded_at, local computed_at)`` as they stand right now.

    ⚠⚠ THE PAIRING GUARD, and it is load-bearing rather than defensive. The totals come
    from one statement and the per-position rows from two more, on a READ COMMITTED
    connection — so a writer that commits in between hands this function one snapshot's
    TOTAL beside a different snapshot's MARKS. The correction would then subtract closes
    that never contributed to that total, and `run_reconciliation_check` would freeze the
    resulting verdict permanently.

    ⚠ It is not a theoretical race on the local side. `portfolio_eod._write_snapshot`
    upserts the parent and replaces the children for whatever date it resolves, which can
    be a PAST one — the 2026-09-12 recovery burst re-stamped a row 18 days old. (The
    official side is narrower: `record_account_equity_snapshot` only ever accepts a write
    for today. Guarded anyway, because "only today" is a property of another module's
    `WHERE` clause.)

    ⚠ `snapshot_read` is deliberately NOT used. It COMMITS the caller's pending
    transaction before switching isolation, and this loader is called from inside
    `run_reconciliation_check`'s transaction — so buying read consistency that way would
    commit a job's in-flight work as a side effect. Both writers bump their stamp in the
    same transaction as their rows, so an unchanged stamp across the whole read proves no
    version boundary was crossed.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT recorded_at FROM broker_account_equity_snapshots WHERE environment=%s AND snapshot_date=%s",
            (environment, snapshot_date),
        )
        official = cur.fetchone()
        cur.execute(
            "SELECT computed_at FROM portfolio_eod_snapshots WHERE snapshot_date=%s",
            (snapshot_date,),
        )
        local = cur.fetchone()
    return (
        None if official is None else official[0],
        None if local is None else local[0],
    )


def _read_official_position_marks(
    conn: psycopg.Connection[Any], *, environment: str, snapshot_date: date
) -> tuple[OfficialPositionMark, ...]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT position_id,instrument_id,is_buy,close_rate,close_conversion_rate,asset_currency_id
            FROM broker_account_position_marks
            WHERE environment=%s AND snapshot_date=%s
            """,
            (environment, snapshot_date),
        )
        return tuple(
            OfficialPositionMark(
                position_id=int(row["position_id"]),
                instrument_id=int(row["instrument_id"]),
                is_buy=bool(row["is_buy"]),
                close_rate=_decimal(row["close_rate"]),
                close_conversion_rate=_decimal(row["close_conversion_rate"]),
                asset_currency_id=None if row["asset_currency_id"] is None else int(row["asset_currency_id"]),
            )
            for row in cur.fetchall()
        )


def _read_local_position_marks(conn: psycopg.Connection[Any], *, snapshot_date: date) -> tuple[LocalPositionMark, ...]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT position_id,instrument_id,is_buy,units,close_price,native_currency,price_status
            FROM portfolio_eod_position_snapshots
            WHERE snapshot_date=%s
            """,
            (snapshot_date,),
        )
        return tuple(
            LocalPositionMark(
                position_id=int(row["position_id"]),
                instrument_id=int(row["instrument_id"]),
                is_buy=None if row["is_buy"] is None else bool(row["is_buy"]),
                units=_decimal(row["units"]),
                close_price=_decimal(row["close_price"]),
                native_currency=None if row["native_currency"] is None else str(row["native_currency"]),
                price_status=str(row["price_status"]),
            )
            for row in cur.fetchall()
        )


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
    conn: psycopg.Connection[Any],
    *,
    environment: Literal["demo", "real"],
    snapshot_date: date | None = None,
) -> AccountEquityEvidence:
    """Return one official/local comparison, its verdict, and every blocker.

    ``snapshot_date=None`` returns the LATEST stored broker day, which is what the
    ``/strategies`` panel has always shown. #2844's countdown needs a named day instead,
    because the latest broker day is precisely the one that cannot yet be decided: the
    local comparand is stamped ``MAX(price_daily.price_date)`` and lands 0-3 days late.

    ⚠ ``days_collected`` is counted over the whole environment BEFORE the date filter, so
    it keeps meaning "broker days collected" for every caller. It never meant "days
    reconciled" — ``account_reconciliation_ledger`` is what counts those.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH env AS (
                SELECT *,count(*) OVER () AS days_collected
                FROM broker_account_equity_snapshots
                WHERE environment=%(environment)s
            ), chosen AS (
                SELECT * FROM env
                -- ⚠ CAST REQUIRED. An uncast nullable filter parameter raises psycopg3
                -- AmbiguousParameter -- it appears only inside IS NULL / equality here,
                -- so the server cannot infer its type.
                WHERE %(snapshot_date)s::date IS NULL OR snapshot_date=%(snapshot_date)s::date
                ORDER BY snapshot_date DESC
                LIMIT 1
            )
            SELECT chosen.days_collected,chosen.snapshot_date,chosen.observed_at,chosen.currency,
                   chosen.available_cash,chosen.total_invested,chosen.unrealised_pnl,chosen.equity,
                   chosen.account_currency_id,
                   chosen.official_direct_long_market_value,chosen.official_direct_long_positions,
                   chosen.official_direct_short_positions,chosen.official_pending_order_amount,
                   local.display_currency,local.total_value,local.fx_rate_date,
                   coalesce(local.positions_no_price,0) > 0
                     OR coalesce(local.positions_no_fx,0) > 0
                     OR coalesce(local.cash_no_fx_currencies,0) > 0 AS local_valuation_incomplete,
                   chosen.recorded_at,local.computed_at,
                   local.oldest_mark_date,local.positions_priced,local.stale_mark_positions,
                   local.positions_total,local.mark_rounding_tolerance
            FROM chosen
            LEFT JOIN portfolio_eod_snapshots local ON local.snapshot_date=chosen.snapshot_date
            """,
            {"environment": environment, "snapshot_date": snapshot_date},
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
            local_eod_value_at_official_marks=None,
            local_eod_positions_priced=None,
            local_eod_stale_mark_positions=None,
            difference=None,
            tolerance=None,
            comparable=False,
            incomplete_reasons=("official_account_equity_missing",),
        )

    # ⚠ Distinct name from the PARAMETER. `snapshot_date` is `date | None` on the
    # signature (None = latest); the column is NOT NULL, so rebinding the parameter
    # here would carry the Optional into every downstream use.
    observed_date: date = row["snapshot_date"]
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
    local_at_official_marks: Decimal | None = None
    tolerance: Decimal | None = None
    if local_value is None:
        reasons.append("same_day_local_eod_snapshot_missing")
    else:
        if bool(row["local_valuation_incomplete"]):
            reasons.append("local_eod_valuation_incomplete")
        # ⚠⚠ `mark_effectiveness_reasons` USED TO BE CALLED HERE and has been DELETED
        # (#3068). Its two slugs — `local_eod_marks_carried_forward` and
        # `local_eod_effective_time_unknown` — both describe `close_price`, and since
        # `f0-reconcile-v2` the comparand re-prices every position at the broker's own
        # mark: `close_price` enters `positions_value` with one sign and the correction
        # with the other, so a carried-forward or undated local mark cannot move
        # `difference`. This was its only caller, so the function was dead, not merely
        # unused here.
        #
        # This is a WIDENING and it is deliberate. Measured on the stored population, 4 of
        # the 10 local snapshots carrying recorded mark dates have
        # `stale_mark_positions > 0`, so retaining the caveat as a refusal would block
        # roughly two days in five of a five-day countdown, over a quantity the verdict
        # provably does not read. The magnitude counters
        # (`local_eod_positions_priced`, `local_eod_stale_mark_positions`) are unchanged
        # and still reported — the operator keeps the information, it just stops being a
        # refusal. The FE labels for both slugs are kept so that verdicts STORED under
        # `f0-reconcile-v1`, which still carry them, keep rendering.
        marks = substitute_official_marks(
            official=_read_official_position_marks(conn, environment=environment, snapshot_date=observed_date),
            local=_read_local_position_marks(conn, snapshot_date=observed_date),
            declared_long=direct_long_positions,
            declared_short=direct_short_positions,
        )
        if _reread_write_stamps(conn, environment=environment, snapshot_date=observed_date) != (
            row["recorded_at"],
            row["computed_at"],
        ):
            # A writer committed while this comparison was being assembled, so the totals
            # and the marks may be from different versions. Refuse and re-decide on the
            # next pass rather than freeze a verdict computed across a boundary — the
            # ledger only writes an UNDECIDED day again, so a refusal is recoverable and a
            # wrong `reconciled` is not.
            reasons.append("reconciliation_inputs_changed_during_read")
        reasons.extend(marks.reasons)
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
            if local_in_account_currency is not None and marks.correction is not None:
                # ⚠ The correction is already in the ACCOUNT currency — the broker's own
                # `close_conversion_rate` carried it there — so it is added AFTER the
                # display→account conversion rather than being pushed through the display
                # currency first. Routing it through display would convert it with our
                # ECB rate and back again for no reason, and on the live configuration
                # (display GBP, account USD, assets USD) that is a pure round trip.
                local_at_official_marks = local_in_account_currency + marks.correction

    official_comparand = (
        None if direct_long_market_value is None else official_available_cash + direct_long_market_value
    )
    difference = (
        None
        if official_comparand is None or local_at_official_marks is None
        else official_comparand - local_at_official_marks
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
        snapshot_date=observed_date,
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
        local_eod_value_at_official_marks=local_at_official_marks,
        local_eod_positions_priced=None if row["positions_priced"] is None else int(row["positions_priced"]),
        local_eod_stale_mark_positions=(
            None if row["stale_mark_positions"] is None else int(row["stale_mark_positions"])
        ),
        difference=difference,
        tolerance=tolerance,
        comparable=decided,
        incomplete_reasons=incomplete_reasons,
    )
