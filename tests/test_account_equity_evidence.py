"""Prospective broker account-equity evidence remains compact and fail-closed."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services.account_equity_evidence import (
    DOCUMENTED_ACCOUNT_CURRENCIES,
    AccountEquityEvidenceError,
    load_account_equity_evidence,
    mark_effectiveness_reasons,
    record_account_equity_snapshot,
)


def _snapshot(
    *,
    observed_at: datetime,
    cash: str = "500",
    invested: str = "400",
    pnl: str = "100",
    account_currency_id: int | None = 1,
) -> BrokerAccountRiskSnapshot:
    available_cash = Decimal(cash)
    total_invested = Decimal(invested)
    unrealized_pnl = Decimal(pnl)
    return BrokerAccountRiskSnapshot(
        available_cash=available_cash,
        total_invested=total_invested,
        unrealized_pnl=unrealized_pnl,
        equity=available_cash + total_invested + unrealized_pnl,
        instrument_investments=(),
        observed_at=observed_at,
        raw_payload={"not": "persisted"},
        account_currency_id=account_currency_id,
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

    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.status == "collecting"
    assert evidence.days_collected == 1
    assert evidence.official_equity == Decimal("1000.000000")
    assert evidence.local_eod_value == Decimal("995.0000")
    assert evidence.local_eod_currency == "USD"
    assert evidence.difference == Decimal("5.000000")
    assert evidence.incomplete_reasons == ("local_eod_effective_time_unknown",)


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
    assert set(evidence.incomplete_reasons) == {
        "local_eod_currency_mismatch",
        "local_eod_valuation_incomplete",
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


class TestMarkEffectivenessReasons:
    """#2602 item 4 — the effective-time caveat is measured, not assumed.

    Pure-logic, because the decision is a three-way branch over two stored
    columns and a db-tier test per branch would buy nothing the table does not.
    The two db-tier tests below cover the wiring: that the columns reach it and
    that the caveat actually leaves the panel.
    """

    SNAPSHOT = date(2025, 6, 12)
    EARLIER = date(2025, 6, 10)

    def test_a_row_written_before_the_marks_were_recorded_is_still_unknown(self) -> None:
        # NULL bound WITH priced positions can only mean "predates sql/350".
        assert mark_effectiveness_reasons(snapshot_date=self.SNAPSHOT, oldest_mark_date=None, positions_priced=3) == (
            "local_eod_effective_time_unknown",
        )

    def test_a_snapshot_with_no_priced_position_has_no_effective_time_to_be_unknown(self) -> None:
        # All cash, or every position unpriced. Pre- and post-migration rows are
        # indistinguishable here and need not be distinguished — neither has a mark.
        assert mark_effectiveness_reasons(snapshot_date=self.SNAPSHOT, oldest_mark_date=None, positions_priced=0) == ()

    def test_marks_on_the_snapshots_own_session_carry_no_caveat(self) -> None:
        assert (
            mark_effectiveness_reasons(snapshot_date=self.SNAPSHOT, oldest_mark_date=self.SNAPSHOT, positions_priced=3)
            == ()
        )

    def test_a_mark_older_than_the_snapshot_is_named_as_carried_forward(self) -> None:
        assert mark_effectiveness_reasons(
            snapshot_date=self.SNAPSHOT, oldest_mark_date=self.EARLIER, positions_priced=3
        ) == ("local_eod_marks_carried_forward",)

    def test_one_day_of_staleness_is_enough(self) -> None:
        # No tolerance is applied here on purpose: "the total is a blend of
        # sessions" is a fact about the valuation, and how much divergence the
        # operator will accept is item 4's separate tolerance rule.
        assert mark_effectiveness_reasons(
            snapshot_date=self.SNAPSHOT,
            oldest_mark_date=self.SNAPSHOT - timedelta(days=1),
            positions_priced=1,
        ) == ("local_eod_marks_carried_forward",)


def test_marks_on_the_session_retire_the_effective_time_caveat(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The caveat leaves the panel once the marks are recorded and current."""
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=_snapshot(observed_at=observed))
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,computed_at
        ) VALUES (%(d)s,'USD',995,495,500,2,2,%(d)s,0,%(c)s)
        """,
        {"d": observed.date(), "c": observed + timedelta(minutes=1)},
    )
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.incomplete_reasons == ()
    assert evidence.local_eod_positions_priced == 2
    assert evidence.local_eod_stale_mark_positions == 0
    # ⚠ Still not comparable. Clearing every named caveat does NOT make the two
    # totals reconciled — that needs item 4's declared tolerance, which this
    # slice does not ship. `comparable` staying False with an empty reason list
    # is the honest state, not a contradiction.
    assert not evidence.comparable
    assert evidence.difference == Decimal("5.000000")


def test_a_carried_forward_mark_is_named_rather_than_absorbed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=_snapshot(observed_at=observed))
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,oldest_mark_date,stale_mark_positions,computed_at
        ) VALUES (%(d)s,'USD',995,495,500,2,2,%(old)s,1,%(c)s)
        """,
        {"d": observed.date(), "old": observed.date() - timedelta(days=3), "c": observed},
    )
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.incomplete_reasons == ("local_eod_marks_carried_forward",)
    assert evidence.local_eod_positions_priced == 2
    assert evidence.local_eod_stale_mark_positions == 1
