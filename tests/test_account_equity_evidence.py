"""Prospective broker account-equity evidence remains compact and fail-closed."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerDirectPositionInvestment,
    BrokerInstrumentInvestment,
)
from app.services import account_equity_evidence
from app.services.account_equity_evidence import (
    DOCUMENTED_ACCOUNT_CURRENCIES,
    RECONCILIATION_RULE_VERSION,
    AccountEquityEvidenceError,
    LocalPositionMark,
    OfficialPositionMark,
    load_account_equity_evidence,
    official_direct_position_reasons,
    record_account_equity_snapshot,
    substitute_official_marks,
    summarise_direct_positions,
)


def _position(
    position_id: int,
    *,
    units: str,
    amount: str,
    pnl: str,
    instrument_id: int = 1,
    is_buy: bool = True,
    close_rate: str | None = None,
    asset_currency_id: int = 1,
) -> BrokerDirectPositionInvestment:
    # ⚠ `close_rate` defaults to the unleveraged-long identity `(amount + pnl) / units`
    # purely so existing callers keep a self-consistent row. Any test that is ABOUT the
    # mark must pass it explicitly: a fixture that derives every operand from one constant
    # cannot see a disagreement between two endpoints, which is the defect #3068 exists
    # for. The derived quotient is NOT the mark in general — see
    # `substitute_official_marks`.
    derived = (Decimal(amount) + Decimal(pnl)) / Decimal(units)
    return BrokerDirectPositionInvestment(
        position_id=position_id,
        instrument_id=instrument_id,
        is_buy=is_buy,
        units=Decimal(units),
        amount=Decimal(amount),
        unrealized_pnl=Decimal(pnl),
        market_value=Decimal(amount) + Decimal(pnl),
        is_partially_altered=False,
        close_rate=derived if close_rate is None else Decimal(close_rate),
        close_conversion_rate=Decimal("1"),
        asset_currency_id=asset_currency_id,
    )


def _snapshot(
    *,
    observed_at: datetime,
    cash: str = "500",
    invested: str = "400",
    pnl: str = "100",
    account_currency_id: int | None = 1,
    direct_long_market_value: str = "495",
    direct_long_positions: int = 1,
    direct_short_positions: int = 0,
    pending_order_amount: str | None = "0",
    direct_positions: tuple[BrokerDirectPositionInvestment, ...] | None = None,
) -> BrokerAccountRiskSnapshot:
    available_cash = Decimal(cash)
    total_invested = Decimal(invested)
    unrealized_pnl = Decimal(pnl)
    # ⚠ The direct book is deliberately SMALLER than `total_invested` in the defaults,
    # because that is the shape of the real account: eToro folds copy-trader mirrors and
    # pending orders into `total_invested`, and the local ledger holds neither. A fixture
    # where the two agree would test a configuration we have never observed (#2602 item 4).
    return BrokerAccountRiskSnapshot(
        available_cash=available_cash,
        total_invested=total_invested,
        unrealized_pnl=unrealized_pnl,
        equity=available_cash + total_invested + unrealized_pnl,
        instrument_investments=(
            BrokerInstrumentInvestment(
                instrument_id=1,
                amount=total_invested,
                direct_long_market_value=Decimal(direct_long_market_value),
                direct_long_positions=direct_long_positions,
                direct_short_positions=direct_short_positions,
            ),
        ),
        observed_at=observed_at,
        raw_payload={"not": "persisted"},
        account_currency_id=account_currency_id,
        pending_order_amount=None if pending_order_amount is None else Decimal(pending_order_amount),
        # ⚠ `None` means "generate a child set that AGREES with the declared counts", which
        # is the only shape that can reach a decided verdict since #3068: a snapshot whose
        # parent declares positions and whose child set is empty is missing evidence and
        # refuses. Passing `()` explicitly still means a genuinely empty book.
        direct_positions=(
            tuple(
                _position(_AUTO_POSITION_ID + offset, units="1", amount="1", pnl="0", close_rate=_AUTO_CLOSE_RATE)
                for offset in range(direct_long_positions)
            )
            if direct_positions is None
            else direct_positions
        ),
    )


#: Base id and mark for the auto-generated child set above, mirrored by
#: `_seed_local_positions` so that the substitution correction is exactly zero and the
#: pre-#3068 expectations about `difference` still hold.
_AUTO_POSITION_ID = 9001
_AUTO_CLOSE_RATE = "1"


def _seed_local_positions(
    conn: psycopg.Connection[tuple],
    snapshot_date: date,
    *,
    count: int,
    close: str = _AUTO_CLOSE_RATE,
    units: str = "1",
    instrument_id: int = 1,
) -> None:
    """Insert local end-of-day position rows that pair with `_snapshot`'s child set.

    The local close defaults to the official mark, so the correction is zero and a test
    written before #3068 keeps its expected `difference`. A test that is ABOUT the
    substitution passes a different `close` and asserts the movement.
    """
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,currency,is_tradable) "
        "VALUES (%s,%s,%s,'USD',true) ON CONFLICT (instrument_id) DO NOTHING",
        (instrument_id, f"T{instrument_id}", f"Test {instrument_id}"),
    )
    for offset in range(count):
        conn.execute(
            """
            INSERT INTO portfolio_eod_position_snapshots (
              snapshot_date,position_id,instrument_id,units,close_price,native_currency,
              value_display,price_status,mark_price_date,is_buy
            ) VALUES (%s,%s,%s,%s,%s,'USD',%s,'priced',%s,true)
            """,
            (
                snapshot_date,
                _AUTO_POSITION_ID + offset,
                instrument_id,
                Decimal(units),
                Decimal(close),
                Decimal(units) * Decimal(close),
                snapshot_date,
            ),
        )


def test_empty_account_equity_evidence_is_explicit(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.status == "unavailable"
    assert evidence.days_collected == 0
    assert evidence.local_eod_positions_priced is None
    assert evidence.local_eod_stale_mark_positions is None
    assert evidence.incomplete_reasons == ("official_account_equity_missing",)


def test_newest_same_day_observation_wins_without_appending(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    first = _snapshot(observed_at=now - timedelta(minutes=2))
    latest = _snapshot(observed_at=now, cash="525")
    assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=first)
    assert not record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=replace(first, observed_at=now - timedelta(minutes=3)),
    )
    assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=latest)

    row = ebull_test_conn.execute(
        "SELECT count(*),max(equity) FROM broker_account_equity_snapshots WHERE environment='demo'"
    ).fetchone()
    assert row == (1, Decimal("1025.000000"))


def test_historical_observation_is_immutable(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    yesterday = datetime.now(UTC).replace(microsecond=0) - timedelta(days=1)
    first = _snapshot(observed_at=yesterday)
    assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=first)
    assert not record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=yesterday + timedelta(minutes=2), cash="600"),
    )


def test_sub_micro_unit_component_rounding_is_accepted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    snapshot = replace(
        _snapshot(observed_at=datetime.now(UTC)),
        equity=Decimal("1000.000001"),
    )
    assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)


@pytest.mark.parametrize(
    "snapshot",
    [
        _snapshot(observed_at=datetime.now(UTC), cash="NaN"),
        _snapshot(observed_at=datetime.now(UTC), cash="-1"),
        replace(_snapshot(observed_at=datetime.now(UTC)), equity=Decimal("999")),
        _snapshot(observed_at=datetime.now()),
        _snapshot(observed_at=datetime.now(UTC), account_currency_id=None),
    ],
)
def test_invalid_official_values_fail_closed(
    ebull_test_conn: psycopg.Connection[tuple], snapshot: BrokerAccountRiskSnapshot
) -> None:
    with pytest.raises(AccountEquityEvidenceError):
        record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)


def test_local_total_remains_diagnostic_until_effective_time_is_known(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    observed = datetime.now(UTC).replace(microsecond=0)
    snapshot = _snapshot(observed_at=observed)
    record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,computed_at
        ) VALUES (%s,'USD',995,495,500,1,1,%s)
        """,
        (observed.date(), observed + timedelta(minutes=1)),
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=1)

    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.status == "collecting"
    assert evidence.days_collected == 1
    assert evidence.official_equity == Decimal("1000.000000")
    assert evidence.local_eod_value == Decimal("995.0000")
    assert evidence.local_eod_currency == "USD"
    # ⚠ The comparand is `available_cash + direct_long_market_value` = 995, NOT `equity`
    # = 1000. The 5 that `equity` carries on top is the mirror/pending-order fold, and it
    # is reported as a residual rather than charged to the local book as a difference.
    assert evidence.official_comparand == Decimal("995.000000")
    assert evidence.difference == Decimal("0.000000")
    assert evidence.residual_not_in_local_book == Decimal("5.000000")
    # ⚠ `local_eod_effective_time_unknown` is GONE from this set since #3068. The row
    # still predates sql/350 and still has no recorded mark dates — that fact simply no
    # longer refuses, because the comparand re-prices at the broker's marks and ours
    # cancel out of it. What is left is the one input that genuinely blocks a verdict.
    assert set(evidence.incomplete_reasons) == {"mark_rounding_tolerance_not_recorded"}
    # A zero difference does NOT reconcile a row whose tolerance was never recorded.
    assert evidence.reconciliation_state == "refused"
    assert not evidence.comparable
    assert evidence.tolerance is None


def test_incomplete_local_valuation_exposes_reasons_not_false_comparison(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,positions_no_price,computed_at
        ) VALUES (%s,'GBP',900,400,500,1,0,1,%s)
        """,
        (observed.date(), observed - timedelta(hours=1)),
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=1)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.status == "collecting"
    assert not evidence.comparable
    assert evidence.difference is None
    # ⚠ No `local_eod_effective_time_unknown` here, and its absence is the point
    # (#2602 item 4). `positions_priced = 0` — the single position failed to
    # price — so nothing contributed a mark and there is no effective time to be
    # unknown. The row's real defect is already named twice over. Before sql/350
    # the caveat was appended unconditionally and said the same thing about every
    # row, priced or not, which is what made it unactionable.
    # ⚠ `local_eod_currency_mismatch` is GONE and its absence is deliberate (#2602 item
    # 4). A GBP display currency against a USD account is the operator's own display
    # setting, not a defect — it blocks the comparison only when no rate bridges it, and
    # that is what `account_currency_fx_rate_missing` says. This row has no
    # `fx_rate_date`, so nothing bridges it.
    assert set(evidence.incomplete_reasons) == {
        "account_currency_fx_rate_missing",
        "local_eod_valuation_incomplete",
        "mark_rounding_tolerance_not_recorded",
    }


def test_observed_usd_account_reports_no_currency_caveat(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The whole point of #2602 item 2: a row that MEASURED USD says nothing about it."""
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=_snapshot(observed_at=observed))
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.account_currency_id == 1
    assert evidence.currency == "USD"
    assert "account_currency_assumed_not_observed" not in evidence.incomplete_reasons
    assert "account_currency_not_documented" not in evidence.incomplete_reasons


def test_undocumented_account_currency_is_stored_and_refused_by_name(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A non-USD account must be recordable. Dropping the row would hide the finding."""
    observed = datetime.now(UTC).replace(microsecond=0)
    assert record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed, account_currency_id=7),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,computed_at
        ) VALUES (%s,'USD',995,495,500,1,1,%s)
        """,
        (observed.date(), observed),
    )

    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.account_currency_id == 7
    assert evidence.currency is None
    assert evidence.official_equity == Decimal("1000.000000")
    # No difference against a USD local total: the official side has no known unit, so
    # subtracting is meaningless. And the local side is not blamed for it.
    assert evidence.difference is None
    assert "account_currency_not_documented" in evidence.incomplete_reasons
    assert "local_eod_currency_mismatch" not in evidence.incomplete_reasons


def test_pre_measurement_row_is_named_as_assumed_not_observed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Rows written before sql/341 carry a USD nobody measured, permanently."""
    observed = datetime.now(UTC).replace(microsecond=0)
    ebull_test_conn.execute(
        """
        INSERT INTO broker_account_equity_snapshots (
            environment,snapshot_date,observed_at,source_version,account_currency_id,currency,
            available_cash,total_invested,unrealised_pnl,equity
        ) VALUES ('demo',%s,%s,'etoro-pnl-v1',NULL,'USD',500,400,100,1000)
        """,
        (observed.date(), observed),
    )
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.account_currency_id is None
    assert evidence.currency == "USD"
    assert "account_currency_assumed_not_observed" in evidence.incomplete_reasons


@pytest.mark.parametrize(
    ("account_currency_id", "currency"),
    [
        (1, None),  # documented USD id must carry its code
        (7, "USD"),  # an undocumented id must never wear a code we invented
        (None, "GBP"),  # an unobserved row can only be the legacy USD assumption
    ],
)
def test_currency_and_reported_id_cannot_disagree_at_rest(
    ebull_test_conn: psycopg.Connection[tuple], account_currency_id: int | None, currency: str | None
) -> None:
    observed = datetime.now(UTC).replace(microsecond=0)
    with pytest.raises(psycopg.errors.CheckViolation) as excinfo:
        ebull_test_conn.execute(
            """
            INSERT INTO broker_account_equity_snapshots (
                environment,snapshot_date,observed_at,source_version,account_currency_id,currency,
                available_cash,total_invested,unrealised_pnl,equity
            ) VALUES ('demo',%s,%s,'etoro-pnl-v1',%s,%s,500,400,100,1000)
            """,
            (observed.date(), observed, account_currency_id, currency),
        )
    assert excinfo.value.diag.constraint_name == "broker_account_equity_snapshots_currency_observed"


@pytest.mark.parametrize("account_currency_id", sorted(DOCUMENTED_ACCOUNT_CURRENCIES))
def test_every_documented_currency_id_is_admitted_by_the_check(
    ebull_test_conn: psycopg.Connection[tuple], account_currency_id: int
) -> None:
    """The dict and sql/341's CHECK must be widened together, or neither.

    The CHECK enumerates documented ids literally and its ELSE branch demands
    `currency IS NULL`, while the writer binds the mapped code -- so a member added to
    DOCUMENTED_ACCOUNT_CURRENCIES without a migration refuses every write in the new
    currency. Fail-closed, but silent, and only reached once the account is not USD.
    The parametrize is driven off the dict so that day fails here first.
    """
    observed = datetime.now(UTC).replace(microsecond=0)
    currency = DOCUMENTED_ACCOUNT_CURRENCIES[account_currency_id]
    ebull_test_conn.execute(
        """
        INSERT INTO broker_account_equity_snapshots (
            environment,snapshot_date,observed_at,source_version,account_currency_id,currency,
            available_cash,total_invested,unrealised_pnl,equity
        ) VALUES ('demo',%s,%s,'etoro-pnl-v1',%s,%s,500,400,100,1000)
        """,
        (observed.date(), observed, account_currency_id, currency),
    )
    stored = ebull_test_conn.execute(
        "SELECT account_currency_id,currency FROM broker_account_equity_snapshots WHERE environment='demo'"
    ).fetchone()
    assert stored == (account_currency_id, currency)


def test_current_marks_and_complete_evidence_reach_a_decided_verdict(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The first state in which the reconciliation is allowed to decide at all.

    Named for what it asserts. It was `test_marks_on_the_session_retire_the_effective_time_
    caveat`, but that caveat is no longer a refusal at all since #3068, so the old name
    described a condition this row can no longer fail.
    """
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed, direct_long_positions=2),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'USD',995,495,500,2,2,%(d)s,0,0.20,%(c)s)
        """,
        {"d": observed.date(), "c": observed + timedelta(minutes=1)},
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=2)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.incomplete_reasons == ()
    assert evidence.local_eod_positions_priced == 2
    assert evidence.local_eod_stale_mark_positions == 0
    # Every named caveat cleared AND a like-for-like comparand AND a recorded tolerance:
    # this is the first state in which the reconciliation is allowed to decide at all.
    assert evidence.comparable
    assert evidence.reconciliation_state == "reconciled"
    assert evidence.reconciliation_rule_version == RECONCILIATION_RULE_VERSION
    assert evidence.difference == Decimal("0.000000")
    assert evidence.tolerance == Decimal("0.21")
    assert evidence.residual_not_in_local_book == Decimal("5.000000")


def test_a_divergence_past_the_tolerance_is_named_not_absorbed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2602 item 4's whole point: past the declared bound, the panel says so."""
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed, direct_long_positions=2),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'USD',990,490,500,2,2,%(d)s,0,0.20,%(c)s)
        """,
        {"d": observed.date(), "c": observed + timedelta(minutes=1)},
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=2)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    # No caveat is raised — the inputs are all present and sound. The comparison ran and
    # DISAGREED, which is a different thing from being unable to run, and the two must
    # not share a state.
    assert evidence.incomplete_reasons == ()
    assert evidence.comparable
    assert evidence.reconciliation_state == "diverged"
    assert evidence.difference == Decimal("5.000000")
    assert evidence.tolerance == Decimal("0.21")


def test_a_mismatched_display_currency_reconciles_once_a_rate_bridges_it(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The GBP-display / USD-account case, which is the live configuration.

    Before this slice it produced `local_eod_currency_mismatch` and a NULL difference on
    every one of the 6 overlapping days on the dev DB — so the 39.8% population gap
    underneath was invisible behind a currency complaint.
    """
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed, direct_long_positions=2),
    )
    ebull_test_conn.execute(
        "INSERT INTO fx_rates_daily (rate_date,base_currency,quote_currency,rate) VALUES (%s,'GBP','USD',2)",
        (observed.date(),),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,fx_rate_date,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'GBP',497.5,247.5,250,%(d)s,2,2,%(d)s,0,0.10,%(c)s)
        """,
        {"d": observed.date(), "c": observed + timedelta(minutes=1)},
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=2)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.incomplete_reasons == ()
    assert evidence.local_eod_currency == "GBP"
    # The stored local total stays in the currency it was computed in; the comparison
    # happens in the ACCOUNT currency, and both the value and its tolerance cross.
    assert evidence.local_eod_value == Decimal("497.5000")
    assert evidence.local_eod_value_in_account_currency == Decimal("995.0000")
    assert evidence.tolerance == Decimal("0.21")
    assert evidence.reconciliation_state == "reconciled"


def test_a_carried_forward_mark_no_longer_refuses_because_it_cannot_move_the_comparand(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ REVERSED BY #3068, and the reversal is the fix working.

    This asserted that a carried-forward local mark REFUSES, on the reasoning that "a
    blended-session total that happens to agree is not evidence that the books agree on
    any single session". That reasoning was sound while the comparand read our marks.
    Since `f0-reconcile-v2` it does not: every position is re-priced at the broker's own
    published close, so our close enters `positions_value` with one sign and the mark
    correction with the other and cancels exactly. The caveat now names a quantity the
    verdict provably does not read.

    Retaining it would not be conservatism, it would be a self-inflicted block: measured
    on the stored dev population, 4 of the 10 local snapshots carrying recorded mark dates
    have `stale_mark_positions > 0`, against a countdown that needs 5 consecutive greens.

    The magnitude counters stay populated — the operator still wants to know — they are
    simply no longer a refusal.
    """
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed, direct_long_positions=2),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'USD',995,495,500,2,2,%(old)s,1,0.20,%(c)s)
        """,
        {"d": observed.date(), "old": observed.date() - timedelta(days=3), "c": observed},
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=2)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.incomplete_reasons == ()
    assert evidence.comparable
    assert evidence.reconciliation_state == "reconciled"
    assert evidence.local_eod_positions_priced == 2
    assert evidence.local_eod_stale_mark_positions == 1


class TestSummariseDirectPositions:
    """#2602 item 4 — the DIRECT half of the official snapshot, mirrors excluded.

    Pure-logic: it is a fold over the per-instrument rows, and the interesting cases are
    all about which arm a row lands in rather than about any SQL.
    """

    @staticmethod
    def _investment(
        instrument_id: int, *, amount: str, direct: str, longs: int, shorts: int
    ) -> BrokerInstrumentInvestment:
        return BrokerInstrumentInvestment(
            instrument_id=instrument_id,
            amount=Decimal(amount),
            direct_long_market_value=Decimal(direct),
            direct_long_positions=longs,
            direct_short_positions=shorts,
        )

    def test_an_empty_book_is_zero_and_not_none(self) -> None:
        totals = summarise_direct_positions(())
        assert totals == type(totals)(long_market_value=Decimal("0"), long_positions=0, short_positions=0)

    def test_mirror_only_instruments_contribute_investment_but_no_direct_holding(self) -> None:
        """The dominant real shape: 33 of 38 reported instruments were mirror-only (#2704).

        `amount` is large and the direct arms are empty — which is precisely why the
        comparand cannot be built from `total_invested`.
        """
        totals = summarise_direct_positions(
            (
                self._investment(1, amount="9000", direct="0", longs=0, shorts=0),
                self._investment(2, amount="1000", direct="800", longs=1, shorts=0),
            )
        )
        assert totals.long_market_value == Decimal("800")
        assert totals.long_positions == 1

    def test_shorts_are_counted_and_never_valued(self) -> None:
        # A short contributes nothing to `direct_long_market_value` by construction, so
        # the count is the ONLY carrier of "a short exists" — and the reader refuses on
        # it rather than under-stating the official side by the whole short book.
        totals = summarise_direct_positions((self._investment(1, amount="500", direct="0", longs=0, shorts=2),))
        assert totals.long_market_value == Decimal("0")
        assert totals.short_positions == 2
        assert "official_direct_short_positions_unvalued" in official_direct_position_reasons(
            direct_long_market_value=totals.long_market_value,
            direct_long_positions=totals.long_positions,
            direct_short_positions=totals.short_positions,
            pending_order_amount=Decimal("0"),
        )


class TestOfficialDirectPositionReasons:
    """Every NULL is refused BY NAME, and none of them is read as a zero."""

    SOUND = {
        "direct_long_market_value": Decimal("495"),
        "direct_long_positions": 1,
        "direct_short_positions": 0,
        "pending_order_amount": Decimal("0"),
    }

    def test_a_complete_sound_official_side_raises_nothing(self) -> None:
        assert official_direct_position_reasons(**self.SOUND) == ()

    @pytest.mark.parametrize(
        ("field", "reason"),
        [
            ("direct_long_market_value", "official_direct_position_value_not_recorded"),
            ("direct_long_positions", "official_direct_position_value_not_recorded"),
            ("direct_short_positions", "official_direct_short_positions_unvalued"),
            ("pending_order_amount", "official_pending_orders_outstanding"),
        ],
    )
    def test_a_null_is_never_a_zero(self, field: str, reason: str) -> None:
        """A NULL count means "this row never looked", not "there are none".

        The distinction is load-bearing on exactly the two columns whose safety argument
        IS "there are none of these" — a defaulted 0 would read as a clean bill of health
        on a row written before the split existed.
        """
        assert reason in official_direct_position_reasons(**{**self.SOUND, field: None})

    def test_an_outstanding_pending_order_makes_the_cash_legs_incomparable(self) -> None:
        # eToro subtracts pending amounts from `credit` to reach `available_cash`;
        # `cash_ledger` models no such thing, so the gap would surface as a phantom
        # valuation error rather than as the accounting difference it is.
        assert official_direct_position_reasons(**{**self.SOUND, "pending_order_amount": Decimal("25")}) == (
            "official_pending_orders_outstanding",
        )

    def test_a_negative_direct_long_value_is_refused_here_and_not_at_parse_time(self) -> None:
        """`BrokerInstrumentInvestment` admits it deliberately; this is where it is used.

        It sums a signed term, so an extreme-but-legitimate account can produce one.
        Refusing it at parse time would lose the whole equity observation with it.
        """
        assert official_direct_position_reasons(**{**self.SOUND, "direct_long_market_value": Decimal("-1")}) == (
            "reconciliation_inputs_out_of_bounds",
        )

    def test_reasons_are_deduplicated_when_several_inputs_fail_the_same_way(self) -> None:
        reasons = official_direct_position_reasons(
            direct_long_market_value=Decimal("-1"),
            direct_long_positions=1,
            direct_short_positions=-1,
            pending_order_amount=Decimal("0"),
        )
        assert reasons.count("reconciliation_inputs_out_of_bounds") == 1


def test_a_populated_difference_never_implies_a_verdict(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The money fields are diagnostics; only `comparable` is a verdict.

    The invariant is an IMPLICATION and not a biconditional, which is easy to get
    backwards because the natural reading of "not comparable" is "nothing to show". It is
    the other way round: every row on the dev DB is refused today, so gating the numbers
    on the verdict would ship an empty panel to the operator who has to repair it.
    """
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        # An outstanding pending order refuses the CASH leg — while leaving the
        # positions arithmetic entirely intact and worth showing.
        snapshot=_snapshot(observed_at=observed, direct_long_positions=2, pending_order_amount="25"),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'USD',995,495,500,2,2,%(d)s,0,0.20,%(c)s)
        """,
        {"d": observed.date(), "c": observed + timedelta(minutes=1)},
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=2)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")

    assert evidence.incomplete_reasons == ("official_pending_orders_outstanding",)
    assert evidence.reconciliation_state == "refused"
    assert not evidence.comparable
    # ...and yet the numbers are all there. This is the direction the spec used to claim
    # was impossible.
    assert evidence.difference == Decimal("0.000000")
    assert evidence.official_comparand == Decimal("995.000000")
    assert evidence.local_eod_value_in_account_currency == Decimal("995.0000")
    assert evidence.residual_not_in_local_book == Decimal("5.000000")

    # The implication that DOES hold, asserted as a rule rather than on this one row.
    if evidence.comparable:  # pragma: no cover - the assertion above fixes this branch
        assert evidence.difference is not None
        assert evidence.tolerance is not None


def test_a_decided_verdict_always_carries_its_difference_and_tolerance(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The forward half of the implication, on a row that actually decides."""
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed, direct_long_positions=2),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'USD',995,495,500,2,2,%(d)s,0,0.20,%(c)s)
        """,
        {"d": observed.date(), "c": observed + timedelta(minutes=1)},
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=2)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.comparable
    assert evidence.difference is not None
    assert evidence.tolerance is not None


class TestOfficialPositionMarks:
    """#3068 — the official per-position terms, retained so our book can be re-priced.

    ⚠⚠ The mark is ``unrealizedPnL.closeRate``, which the broker PUBLISHES. This class
    originally documented it as ``(amount + unrealized_pnl) / units``; that is equity per
    unit, and the portal documents ``amount`` as including "additional margin allocated to
    the position as collateral", so the two coincide only at leverage 1 with no added
    collateral. Where they do coincide the substitution built on the quotient reduces to
    ``units * open_rate_local - amount_local`` — identically zero on an unleveraged book,
    i.e. a comparison that cannot fail. Both measured; see the proposal's revision 4.
    """

    def test_marks_are_stored_with_their_snapshot(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        now = datetime.now(UTC).replace(microsecond=0)
        # The GME shape from #3068: 1500 units the broker marks at 21.50 while our own
        # session close said 21.63. ⚠ `close_rate` is passed INDEPENDENTLY of amount/pnl
        # on purpose — deriving it from them is the withdrawn model, and a fixture that
        # computes every operand from one constant cannot see two endpoints disagree.
        snapshot = _snapshot(
            observed_at=now,
            direct_positions=(_position(9001, units="1500", amount="33885", pnl="-1635", close_rate="21.50"),),
        )
        assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)

        row = ebull_test_conn.execute(
            """
            SELECT units, amount, unrealized_pnl, market_value, close_rate,
                   close_conversion_rate, asset_currency_id
              FROM broker_account_position_marks
             WHERE environment='demo' AND position_id=9001
            """
        ).fetchone()
        assert row is not None
        assert row[0] == Decimal("1500.00000000")
        assert row[4] == Decimal("21.50000000")
        assert row[5] == Decimal("1.0000000000")
        assert row[6] == 1

    def test_a_rejected_parent_write_leaves_the_stored_marks_untouched(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """A stale observation must not pair its positions with the newer totals.

        The parent refuses an older same-day observation; writing its children anyway
        would leave one snapshot's totals beside a different snapshot's book, which is
        worse than having no children at all.
        """
        now = datetime.now(UTC).replace(microsecond=0)
        accepted = _snapshot(
            observed_at=now,
            direct_positions=(_position(9001, units="10", amount="100", pnl="5"),),
        )
        assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=accepted)
        stale = _snapshot(
            observed_at=now - timedelta(minutes=5),
            direct_positions=(_position(9002, units="99", amount="990", pnl="0"),),
        )
        assert not record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=stale)

        stored = ebull_test_conn.execute(
            "SELECT position_id FROM broker_account_position_marks WHERE environment='demo' ORDER BY position_id"
        ).fetchall()
        assert stored == [(9001,)]

    def test_an_accepted_rewrite_replaces_the_set_so_a_closed_position_disappears(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Wholesale replacement, not an upsert — an upsert has no arm that removes."""
        now = datetime.now(UTC).replace(microsecond=0)
        before = _snapshot(
            observed_at=now - timedelta(minutes=2),
            direct_positions=(
                _position(9001, units="10", amount="100", pnl="5"),
                _position(9002, units="20", amount="200", pnl="-7"),
            ),
        )
        assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=before)
        after = _snapshot(
            observed_at=now,
            direct_positions=(_position(9001, units="10", amount="100", pnl="6"),),
        )
        assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=after)

        stored = ebull_test_conn.execute(
            """
            SELECT position_id, unrealized_pnl
              FROM broker_account_position_marks
             WHERE environment='demo' ORDER BY position_id
            """
        ).fetchall()
        assert stored == [(9001, Decimal("6.000000"))]

    def test_a_short_is_stored_even_though_the_parent_only_counts_it(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """The parent's short arm is a COUNT because no monetary sum can carry "a short
        exists". That argument is about the AGGREGATE. Per position there is nothing to
        protect against, and dropping shorts would make the child set not-the-book.
        """
        now = datetime.now(UTC).replace(microsecond=0)
        snapshot = _snapshot(
            observed_at=now,
            direct_short_positions=1,
            direct_positions=(
                _position(9001, units="10", amount="100", pnl="5"),
                _position(9002, units="4", amount="40", pnl="-2", is_buy=False),
            ),
        )
        assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)

        stored = ebull_test_conn.execute(
            """
            SELECT position_id, is_buy
              FROM broker_account_position_marks
             WHERE environment='demo' ORDER BY position_id
            """
        ).fetchall()
        assert stored == [(9001, True), (9002, False)]

    def test_the_child_count_matches_the_parents_own_direct_position_counts(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """The discriminator that makes "zero children" readable (sql/383).

        A legacy snapshot and a genuinely empty book both store zero child rows. Only
        the parent's own long+short counts tell them apart, so the writer must keep the
        two in agreement — otherwise a missing capture reads as an empty book and
        reconciles against an empty local side to manufacture a green.
        """
        now = datetime.now(UTC).replace(microsecond=0)
        snapshot = _snapshot(
            observed_at=now,
            direct_long_positions=2,
            direct_short_positions=1,
            direct_positions=(
                _position(9001, units="10", amount="100", pnl="5"),
                _position(9002, units="20", amount="200", pnl="-7"),
                _position(9003, units="4", amount="40", pnl="-2", is_buy=False),
            ),
        )
        assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)

        row = ebull_test_conn.execute(
            """
            SELECT s.official_direct_long_positions + s.official_direct_short_positions,
                   (SELECT count(*) FROM broker_account_position_marks m
                     WHERE m.environment=s.environment AND m.snapshot_date=s.snapshot_date)
              FROM broker_account_equity_snapshots s
             WHERE s.environment='demo'
            """
        ).fetchone()
        assert row is not None
        declared, stored = row
        assert declared == 3
        assert stored == declared

    def test_an_empty_book_stores_no_marks_and_says_so_on_the_parent(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        now = datetime.now(UTC).replace(microsecond=0)
        snapshot = _snapshot(
            observed_at=now,
            direct_long_market_value="0",
            direct_long_positions=0,
            direct_short_positions=0,
            direct_positions=(),
        )
        assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)

        row = ebull_test_conn.execute(
            """
            SELECT s.official_direct_long_positions + s.official_direct_short_positions,
                   (SELECT count(*) FROM broker_account_position_marks m
                     WHERE m.environment=s.environment AND m.snapshot_date=s.snapshot_date)
              FROM broker_account_equity_snapshots s
             WHERE s.environment='demo'
            """
        ).fetchone()
        # Zero children AND zero declared — the one shape that is a real empty book
        # rather than a snapshot whose capture is missing.
        assert row == (0, 0)


def _official_mark(
    position_id: int,
    *,
    close_rate: str | None = "21.50",
    instrument_id: int = 1699,
    is_buy: bool = True,
    conversion: str | None = "1",
    asset_currency_id: int | None = 1,
) -> OfficialPositionMark:
    return OfficialPositionMark(
        position_id=position_id,
        instrument_id=instrument_id,
        is_buy=is_buy,
        close_rate=None if close_rate is None else Decimal(close_rate),
        close_conversion_rate=None if conversion is None else Decimal(conversion),
        asset_currency_id=asset_currency_id,
    )


def _local_mark(
    position_id: int,
    *,
    units: str | None = "1500",
    close: str | None = "21.63",
    instrument_id: int = 1699,
    is_buy: bool | None = True,
    currency: str | None = "USD",
    price_status: str = "priced",
) -> LocalPositionMark:
    return LocalPositionMark(
        position_id=position_id,
        instrument_id=instrument_id,
        is_buy=is_buy,
        units=None if units is None else Decimal(units),
        close_price=None if close is None else Decimal(close),
        native_currency=currency,
        price_status=price_status,
    )


class TestSubstituteOfficialMarks:
    """#3068 — re-price the local book at the broker's own published marks.

    ⚠ Every fixture here sets the official ``close_rate`` and the local ``close_price``
    INDEPENDENTLY. A test that derives both from one constant cannot see the defect this
    function exists for, which is precisely that the two sides were struck at different
    instants.
    """

    def test_the_correction_is_the_measured_gme_gap(self) -> None:
        """The 2026-09-14 attribution, to the cent.

        Broker mark 21.50 against our 21.63 close on 1500 units is the −195.00 that made
        up the bulk of the day's −204.65, and it is what the correction must restore.
        """
        result = substitute_official_marks(
            official=(_official_mark(3308442058, close_rate="21.50"),),
            local=(_local_mark(3308442058, units="1500", close="21.63"),),
            declared_long=1,
            declared_short=0,
        )
        assert result.reasons == ()
        assert result.correction == Decimal("-195.0000")

    def test_an_evening_mark_move_of_arbitrary_size_does_not_move_the_verdict(self) -> None:
        """⚠⚠ THE REGRESSION THIS TICKET EXISTS FOR.

        Our own close cancels exactly out of the comparand: it enters ``positions_value``
        with one sign and the correction with the other. So moving the LOCAL mark by any
        amount moves ``local_value + correction`` by nothing — which is what makes a
        carried-forward or stale local mark irrelevant, and why the two mark-effectiveness
        caveats were retired as refusals.
        """
        official = (_official_mark(1, close_rate="21.50"),)
        baseline_local_value = Decimal("1500") * Decimal("21.63")
        baseline = substitute_official_marks(
            official=official,
            local=(_local_mark(1, units="1500", close="21.63"),),
            declared_long=1,
            declared_short=0,
        )
        for moved_close in ("0.01", "5.00", "21.63", "99.99", "1000000"):
            moved_local_value = Decimal("1500") * Decimal(moved_close)
            moved = substitute_official_marks(
                official=official,
                local=(_local_mark(1, units="1500", close=moved_close),),
                declared_long=1,
                declared_short=0,
            )
            assert moved.reasons == ()
            assert moved.correction is not None and baseline.correction is not None
            # The comparand — not the correction — is what must be invariant.
            assert moved_local_value + moved.correction == baseline_local_value + baseline.correction

    def test_a_units_disagreement_survives_at_its_own_value(self) -> None:
        """The check the substitution must NOT swallow.

        With the broker at 21.50 on 1500 units and our book holding 1400, the substituted
        local value falls short by 100 × 21.50. Under the withdrawn derived-mark model
        this could vanish entirely; here it cannot.
        """
        result = substitute_official_marks(
            official=(_official_mark(1, close_rate="21.50"),),
            local=(_local_mark(1, units="1400", close="21.63"),),
            declared_long=1,
            declared_short=0,
        )
        assert result.reasons == ()
        assert result.correction is not None
        local_at_marks = Decimal("1400") * Decimal("21.63") + result.correction
        assert local_at_marks == Decimal("1400") * Decimal("21.50")
        assert Decimal("1500") * Decimal("21.50") - local_at_marks == Decimal("100") * Decimal("21.50")

    def test_a_leveraged_position_is_re_priced_without_assuming_amount_equals_units_times_rate(
        self,
    ) -> None:
        """Codex checkpoint 1, finding 1 — the case that killed the derived mark.

        amount 50 (margin), units 1, entry 100, broker mark 110. The local value is
        ``50 + 1 × (109 − 100) = 59`` at our close of 109; at the broker's mark it must be
        60, a correction of exactly +1. The withdrawn model produced −49 here.
        """
        result = substitute_official_marks(
            official=(_official_mark(1, close_rate="110"),),
            local=(_local_mark(1, units="1", close="109"),),
            declared_long=1,
            declared_short=0,
        )
        assert result.reasons == ()
        assert result.correction == Decimal("1")

    def test_a_short_correction_carries_the_opposite_sign(self) -> None:
        result = substitute_official_marks(
            official=(_official_mark(1, close_rate="21.50", is_buy=False),),
            local=(_local_mark(1, units="1500", close="21.63", is_buy=False),),
            declared_long=0,
            declared_short=1,
        )
        assert result.reasons == ()
        assert result.correction == Decimal("195.0000")

    def test_the_brokers_own_conversion_rate_carries_the_correction_to_account_currency(self) -> None:
        result = substitute_official_marks(
            official=(_official_mark(1, close_rate="21.50", conversion="2"),),
            local=(_local_mark(1, units="1500", close="21.63"),),
            declared_long=1,
            declared_short=0,
        )
        assert result.correction == Decimal("-390.0000")

    def test_an_empty_official_book_reconciles_only_when_the_parent_declares_one(self) -> None:
        assert substitute_official_marks(official=(), local=(), declared_long=0, declared_short=0) == (
            substitute_official_marks(official=(), local=(), declared_long=0, declared_short=0)
        )
        empty = substitute_official_marks(official=(), local=(), declared_long=0, declared_short=0)
        assert empty.reasons == ()
        assert empty.correction == Decimal("0")

    @pytest.mark.parametrize(
        ("declared_long", "declared_short"),
        [(1, 0), (0, 1), (None, 0), (0, None)],
    )
    def test_no_children_against_a_non_empty_parent_is_missing_evidence(
        self, declared_long: int | None, declared_short: int | None
    ) -> None:
        """A legacy snapshot must never read as "the broker holds nothing".

        An empty official book reconciles against an empty local side and manufactures a
        green, which is the one outcome this control must not be able to produce by
        accident. 2026-09-14 is exactly this row.
        """
        result = substitute_official_marks(
            official=(), local=(), declared_long=declared_long, declared_short=declared_short
        )
        assert result.correction is None
        assert result.reasons == ("official_position_marks_not_recorded",)

    def test_a_partially_lost_child_set_is_caught_by_the_parents_counts(self) -> None:
        """Zero-child checking cannot see this: the survivors still pair and substitute
        cleanly while the parent's value covers positions that are no longer there."""
        result = substitute_official_marks(
            official=(_official_mark(1),),
            local=(_local_mark(1),),
            declared_long=2,
            declared_short=0,
        )
        assert result.correction is None
        assert "official_position_marks_incomplete" in result.reasons

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"close_rate": None}, "official_position_marks_unusable"),
            ({"close_rate": "NaN"}, "official_position_marks_unusable"),
            ({"conversion": None}, "official_position_marks_unusable"),
            ({"conversion": "NaN"}, "official_position_marks_unusable"),
            ({"asset_currency_id": None}, "official_position_marks_unusable"),
            ({"asset_currency_id": 99}, "official_position_marks_unusable"),
        ],
    )
    def test_an_unusable_official_operand_refuses_rather_than_multiplying(
        self, kwargs: dict[str, object], expected: str
    ) -> None:
        """⚠ ``NaN`` is not excluded by the table's ``CHECK (close_rate > 0)`` — PostgreSQL
        `numeric` orders NaN ABOVE every non-NaN value, so the constraint passes it. The
        finiteness check here is the one that holds. An id with no documented currency
        code is equally unusable: the correction is a price difference, and a price whose
        currency is unknown cannot be differenced against a known one."""
        result = substitute_official_marks(
            official=(_official_mark(1, **kwargs),),  # type: ignore[arg-type]
            local=(_local_mark(1),),
            declared_long=1,
            declared_short=0,
        )
        assert result.correction is None
        assert expected in result.reasons

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"is_buy": None}, "local_eod_position_direction_not_recorded"),
            ({"instrument_id": 4238}, "position_identity_mismatch"),
            ({"is_buy": False}, "position_identity_mismatch"),
            ({"currency": "GBP"}, "position_identity_mismatch"),
            ({"price_status": "no_price"}, "local_position_mark_unusable"),
            ({"price_status": "no_fx"}, "local_position_mark_unusable"),
            ({"close": None}, "local_position_mark_unusable"),
            ({"close": "NaN"}, "local_position_mark_unusable"),
            ({"units": None}, "local_position_mark_unusable"),
            ({"units": "0"}, "local_position_mark_unusable"),
            ({"units": "-5"}, "local_position_mark_unusable"),
        ],
    )
    def test_same_id_corruption_and_unusable_local_operands_refuse(
        self, kwargs: dict[str, object], expected: str
    ) -> None:
        """Matching position ids do not establish that the two sides hold the same thing.

        ⚠ The currency arm is the one a reader skips: same id, same direction, same
        instrument, and a price quoted in a different currency still differences to a
        number. It is the one that would look most like a small divergence.
        """
        result = substitute_official_marks(
            official=(_official_mark(1),),
            local=(_local_mark(1, **kwargs),),  # type: ignore[arg-type]
            declared_long=1,
            declared_short=0,
        )
        assert result.correction is None
        assert expected in result.reasons

    def test_each_side_of_the_position_set_difference_is_named_separately(self) -> None:
        """A count cannot say WHICH side is short, and the two have different causes."""
        official_only = substitute_official_marks(
            official=(_official_mark(1), _official_mark(2)),
            local=(_local_mark(1),),
            declared_long=2,
            declared_short=0,
        )
        assert official_only.correction is None
        assert "official_position_missing_locally" in official_only.reasons

        local_only = substitute_official_marks(
            official=(_official_mark(1),),
            local=(_local_mark(1), _local_mark(2)),
            declared_long=1,
            declared_short=0,
        )
        assert local_only.correction is None
        assert "local_position_missing_officially" in local_only.reasons

    def test_a_local_only_position_against_a_declared_empty_book_still_refuses(self) -> None:
        result = substitute_official_marks(official=(), local=(_local_mark(1),), declared_long=0, declared_short=0)
        assert result.correction is None
        assert result.reasons == ("local_position_missing_officially",)


def test_the_measured_2026_09_14_divergence_reconciles_under_v2(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ THE DEFECT, END TO END, ON THE MEASURED NUMBERS.

    2026-09-14 was the #2844 countdown's first session and it came back `diverged`:
    `difference` −204.65 against `tolerance` 31.56, with NO incomplete reasons — a hard
    red on a healthy pipeline. The whole of it was attributable to two evening-quoted
    names, whose broker marks at the 23:55 UTC snapshot instant were below our
    regular-session closes:

        GME  1500 units   our close 21.63   broker 21.50   →  −195.00
        QQQ  17.252438    our close 710.02  broker 709.46  →    −9.66

    Under `f0-reconcile-v1` the official side carried the broker's marks and the local
    side carried ours, and the tolerance had no term for the gap. Under `v2` both sides
    carry the broker's, so the same book reconciles — and the row below reproduces that
    with the official comparand deliberately built from the BROKER's valuation while the
    local total is built from OURS.
    """
    observed = datetime.now(UTC).replace(microsecond=0)
    gme_units, qqq_units = Decimal("1500"), Decimal("17.252438")
    gme_broker, qqq_broker = Decimal("21.50"), Decimal("709.46")
    gme_local, qqq_local = Decimal("21.63"), Decimal("710.02")
    cash = Decimal("1262.33")
    official_positions_value = gme_units * gme_broker + qqq_units * qqq_broker
    local_positions_value = gme_units * gme_local + qqq_units * qqq_local

    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(
            observed_at=observed,
            cash=str(cash),
            invested=str(official_positions_value),
            pnl="0",
            direct_long_market_value=str(official_positions_value),
            direct_long_positions=2,
            direct_positions=(
                _position(
                    3308442058,
                    units=str(gme_units),
                    amount=str(gme_units * gme_broker),
                    pnl="0",
                    close_rate=str(gme_broker),
                ),
                _position(
                    3308441899,
                    units=str(qqq_units),
                    amount=str(qqq_units * qqq_broker),
                    pnl="0",
                    close_rate=str(qqq_broker),
                ),
            ),
        ),
    )
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,currency,is_tradable) "
        "VALUES (1,'GME','GameStop','USD',true) ON CONFLICT (instrument_id) DO NOTHING"
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'USD',%(total)s,%(pos)s,%(cash)s,2,2,%(d)s,0,%(tol)s,%(c)s)
        """,
        {
            "d": observed.date(),
            "total": local_positions_value + cash,
            "pos": local_positions_value,
            "cash": cash,
            # The rule's own allowance: one cent per unit held. 31.55 before the cash cent
            # — which is the tolerance the real row was judged against.
            "tol": (gme_units + qqq_units) * Decimal("0.01"),
            "c": observed + timedelta(minutes=1),
        },
    )
    for position_id, units, close in (
        (3308442058, gme_units, gme_local),
        (3308441899, qqq_units, qqq_local),
    ):
        ebull_test_conn.execute(
            """
            INSERT INTO portfolio_eod_position_snapshots (
              snapshot_date,position_id,instrument_id,units,close_price,native_currency,
              value_display,price_status,mark_price_date,is_buy
            ) VALUES (%s,%s,1,%s,%s,'USD',%s,'priced',%s,true)
            """,
            (observed.date(), position_id, units, close, units * close, observed.date()),
        )

    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.incomplete_reasons == ()
    assert evidence.comparable
    # The v1 comparand — the stored local total against the broker's — is still visible,
    # and it is still the −204.66 that red-flagged the day. That number has not been
    # hidden; it has stopped being the VERDICT.
    assert evidence.official_comparand is not None and evidence.local_eod_value_in_account_currency is not None
    v1_difference = evidence.official_comparand - evidence.local_eod_value_in_account_currency
    assert v1_difference.quantize(Decimal("0.01")) == Decimal("-204.66")
    assert evidence.tolerance is not None and v1_difference < -evidence.tolerance
    # ...and the v2 comparand, on the same row, agrees to within the STORED TOTAL's own
    # rounding and nothing else. ⚠ Not exactly zero, and the residual is worth naming:
    # `portfolio_eod_snapshots.total_value` is NUMERIC(20,4), so the local total was
    # quantized to four decimal places before this comparison ever saw it. Our close
    # cancels exactly in `Decimal`; the persisted total does not carry every digit of it.
    # The bound is half a unit in the last stored place, ~6 orders of magnitude inside
    # the tolerance.
    assert evidence.difference is not None
    assert abs(evidence.difference) <= Decimal("0.00005")
    assert abs(evidence.difference) < evidence.tolerance
    assert evidence.reconciliation_state == "reconciled"
    assert evidence.reconciliation_rule_version == "f0-reconcile-v2"


def test_a_snapshot_rewritten_mid_read_refuses_rather_than_pairing_two_versions(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    """⚠⚠ Codex checkpoint 2, P2 — the totals and the marks must be one version.

    The local total comes from one statement and the per-position rows from another, on a
    READ COMMITTED connection. If `portfolio_eod._write_snapshot` commits in between, the
    correction subtracts closes that never contributed to the total that is still in hand,
    and `run_reconciliation_check` freezes whatever that produces.

    ⚠ Not theoretical on the local side: the EOD writer upserts whatever date it resolves,
    which can be a PAST one — the 2026-09-12 recovery burst re-stamped a row 18 days old.

    Simulated here by advancing `computed_at` between the two reads, which is exactly the
    observable the guard keys on. The row would otherwise reconcile cleanly, so a passing
    assertion on `refused` can only come from the guard.
    """
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed, direct_long_positions=2),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,
          mark_rounding_tolerance,computed_at
        ) VALUES (%(d)s,'USD',995,495,500,2,2,%(d)s,0,0.20,%(c)s)
        """,
        {"d": observed.date(), "c": observed + timedelta(minutes=1)},
    )
    _seed_local_positions(ebull_test_conn, observed.date(), count=2)
    assert load_account_equity_evidence(ebull_test_conn, environment="demo").comparable

    # The concurrent recompute, placed exactly in the window the guard protects: after the
    # totals statement, before the stamps are re-read. Reduced to the `computed_at` bump
    # that `_write_snapshot` always performs in the same transaction as its rows.
    original = account_equity_evidence._read_local_position_marks

    def _rewrite_then_read(conn: psycopg.Connection[tuple], *, snapshot_date: date):  # type: ignore[no-untyped-def]
        conn.execute(
            "UPDATE portfolio_eod_snapshots SET computed_at = computed_at + interval '1 second' WHERE snapshot_date=%s",
            (snapshot_date,),
        )
        return original(conn, snapshot_date=snapshot_date)

    monkeypatch.setattr(account_equity_evidence, "_read_local_position_marks", _rewrite_then_read)
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert not evidence.comparable
    assert evidence.reconciliation_state == "refused"
    assert "reconciliation_inputs_changed_during_read" in evidence.incomplete_reasons
