"""Compact official account-equity evidence for Foundation F-0 (#2559)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg

from app.providers.broker import BrokerAccountRiskSnapshot

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


@dataclass(frozen=True)
class AccountEquityEvidence:
    status: Literal["unavailable", "collecting"]
    days_collected: int
    snapshot_date: date | None
    observed_at: datetime | None
    account_currency_id: int | None
    currency: str | None
    official_equity: Decimal | None
    official_available_cash: Decimal | None
    official_total_invested: Decimal | None
    official_unrealised_pnl: Decimal | None
    local_eod_currency: str | None
    local_eod_value: Decimal | None
    local_eod_positions_priced: int | None
    local_eod_stale_mark_positions: int | None
    difference: Decimal | None
    comparable: Literal[False]
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
    if snapshot.account_currency_id is None:
        # #2602 item 2.  The alternative -- store it and stamp 'USD' -- is what this
        # table did until now, and it makes the assumption indistinguishable from an
        # observation forever, because the raw payload is deliberately not retained.
        raise AccountEquityEvidenceError("account currency was not reported; refusing to assume one")
    return snapshot.account_currency_id


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
    observed_at = snapshot.observed_at.astimezone(UTC)
    row = conn.execute(
        """
        INSERT INTO broker_account_equity_snapshots (
            environment,snapshot_date,observed_at,source_version,account_currency_id,currency,
            available_cash,total_invested,unrealised_pnl,equity
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (environment,snapshot_date) DO UPDATE SET
            observed_at=EXCLUDED.observed_at,
            source_version=EXCLUDED.source_version,
            account_currency_id=EXCLUDED.account_currency_id,
            currency=EXCLUDED.currency,
            available_cash=EXCLUDED.available_cash,
            total_invested=EXCLUDED.total_invested,
            unrealised_pnl=EXCLUDED.unrealised_pnl,
            equity=EXCLUDED.equity,
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


def load_account_equity_evidence(
    conn: psycopg.Connection[Any], *, environment: Literal["demo", "real"]
) -> AccountEquityEvidence:
    """Return the latest official/local comparison with explicit caveats."""
    row = conn.execute(
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
               local.display_currency,local.total_value,
               coalesce(local.positions_no_price,0) > 0
                 OR coalesce(local.positions_no_fx,0) > 0
                 OR coalesce(local.cash_no_fx_currencies,0) > 0 AS local_valuation_incomplete,
               latest.account_currency_id,
               local.oldest_mark_date,local.positions_priced,local.stale_mark_positions
        FROM latest
        LEFT JOIN portfolio_eod_snapshots local ON local.snapshot_date=latest.snapshot_date
        """,
        (environment,),
    ).fetchone()
    if row is None:
        return AccountEquityEvidence(
            status="unavailable",
            days_collected=0,
            snapshot_date=None,
            observed_at=None,
            account_currency_id=None,
            currency=None,
            official_equity=None,
            official_available_cash=None,
            official_total_invested=None,
            official_unrealised_pnl=None,
            local_eod_currency=None,
            local_eod_value=None,
            local_eod_positions_priced=None,
            local_eod_stale_mark_positions=None,
            difference=None,
            comparable=False,
            incomplete_reasons=("official_account_equity_missing",),
        )

    observed_at = row[2]
    local_value = Decimal(str(row[9])) if row[9] is not None else None
    official_equity = Decimal(str(row[7]))
    account_currency_id = None if row[11] is None else int(row[11])
    official_currency = None if row[3] is None else str(row[3])
    local_currency = None if row[8] is None else str(row[8])
    reasons: list[str] = []
    if account_currency_id is None:
        # Written before #2602 item 2; its 'USD' is this codebase's assumption, and the
        # payload that would settle it was never retained.  Permanent, not pending.
        reasons.append("account_currency_assumed_not_observed")
    elif official_currency is None:
        # The broker named a currency id we have no documented code for.  Every capital
        # authority is USD-locked, so this is the loudest fact on the panel.
        reasons.append("account_currency_not_documented")
    if local_value is None:
        reasons.append("same_day_local_eod_snapshot_missing")
    else:
        # Only a mismatch when BOTH sides are named.  An unnamed official currency is
        # already reported above; calling it a local mismatch would blame the wrong side.
        if official_currency is not None and local_currency != official_currency:
            reasons.append("local_eod_currency_mismatch")
        if bool(row[10]):
            reasons.append("local_eod_valuation_incomplete")
        reasons.extend(
            mark_effectiveness_reasons(
                snapshot_date=row[1],
                oldest_mark_date=row[12],
                positions_priced=int(row[13]),
            )
        )
    return AccountEquityEvidence(
        status="collecting",
        days_collected=int(row[0]),
        snapshot_date=row[1],
        observed_at=observed_at,
        account_currency_id=account_currency_id,
        currency=official_currency,
        official_equity=official_equity,
        official_available_cash=Decimal(str(row[4])),
        official_total_invested=Decimal(str(row[5])),
        official_unrealised_pnl=Decimal(str(row[6])),
        local_eod_currency=local_currency,
        local_eod_value=local_value,
        local_eod_positions_priced=None if row[13] is None else int(row[13]),
        local_eod_stale_mark_positions=None if row[14] is None else int(row[14]),
        difference=(
            official_equity - local_value
            if local_value is not None and official_currency is not None and local_currency == official_currency
            else None
        ),
        comparable=False,
        incomplete_reasons=tuple(reasons),
    )
