"""The two genuinely-new SQL mechanisms behind #2844 clause 3.

Deliberately narrow: the countdown's decision logic is pure and table-tested in
``test_account_reconciliation_countdown.py``. What needs a real Postgres is the pair of
things the database itself owns — the freeze trigger and the upgrade-an-undecided-day
upsert — plus the NaN CHECK, which cannot be exercised in Python at all because PG NUMERIC
NaN is not IEEE.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.account_equity_evidence import (
    RECONCILIATION_RULE_VERSION,
    AccountEquityEvidence,
)
from app.services.account_reconciliation_ledger import (
    COUNTDOWN_RULE_VERSION,
    ReconciliationLedgerError,
    load_reconciliation_days,
    record_reconciliation_day,
)

DAY = date(2026, 9, 1)


def _evidence(
    *,
    state: str = "reconciled",
    comparable: bool = True,
    difference: str | None = "1.00",
    tolerance: str | None = "5.00",
    reasons: tuple[str, ...] = (),
    snapshot_date: date | None = DAY,
) -> AccountEquityEvidence:
    return AccountEquityEvidence(
        status="collecting",
        reconciliation_state=state,  # type: ignore[arg-type]
        reconciliation_rule_version=RECONCILIATION_RULE_VERSION,
        days_collected=1,
        snapshot_date=snapshot_date,
        observed_at=datetime(2026, 9, 1, 23, 0, tzinfo=UTC),
        account_currency_id=1,
        currency="USD",
        official_equity=Decimal("100"),
        official_available_cash=Decimal("10"),
        official_total_invested=Decimal("90"),
        official_unrealised_pnl=Decimal("0"),
        official_direct_long_market_value=Decimal("90"),
        official_comparand=Decimal("100"),
        residual_not_in_local_book=Decimal("0"),
        local_eod_currency="USD",
        local_eod_value=Decimal("99"),
        local_eod_value_in_account_currency=Decimal("99"),
        local_eod_positions_priced=1,
        local_eod_stale_mark_positions=0,
        difference=None if difference is None else Decimal(difference),
        tolerance=None if tolerance is None else Decimal(tolerance),
        comparable=comparable,
        incomplete_reasons=reasons,
    )


def test_an_undecided_day_upgrades_and_counts_its_revisions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The measured 0-3 day local-snapshot lag depends on this.

    A session is routinely judged before its local row exists. Freezing that refusal would
    make 11 of 49 observed days permanently red and the clause unreachable.
    """
    undecided = _evidence(
        state="refused",
        comparable=False,
        difference=None,
        tolerance=None,
        reasons=("same_day_local_eod_snapshot_missing",),
    )
    assert record_reconciliation_day(ebull_test_conn, environment="demo", evidence=undecided) is True
    assert record_reconciliation_day(ebull_test_conn, environment="demo", evidence=undecided) is True
    assert record_reconciliation_day(ebull_test_conn, environment="demo", evidence=_evidence()) is True

    rows = load_reconciliation_days(ebull_test_conn, environment="demo", as_of=DAY)
    assert rows[DAY].reconciliation_state == "reconciled"
    assert rows[DAY].comparable is True
    assert rows[DAY].countdown_rule_version == COUNTDOWN_RULE_VERSION
    revision = ebull_test_conn.execute(
        "SELECT revision_count FROM account_reconciliation_days WHERE snapshot_date=%s", (DAY,)
    ).fetchone()
    assert revision is not None and revision[0] == 2


def test_a_decided_day_is_frozen_against_the_writer(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Without this a later FX-rate revision could quietly turn a diverged day green.

    ``load_account_equity_evidence`` re-loads rates at the local snapshot's own
    ``fx_rate_date``, and its docstring records that a later revision of a rate row moves
    the number. Re-verdicting is a ``RECONCILIATION_RULE_VERSION`` bump, not an overwrite.
    """
    assert record_reconciliation_day(ebull_test_conn, environment="demo", evidence=_evidence()) is True
    flipped = _evidence(state="diverged", difference="999.00")
    assert record_reconciliation_day(ebull_test_conn, environment="demo", evidence=flipped) is False

    rows = load_reconciliation_days(ebull_test_conn, environment="demo", as_of=DAY)
    assert rows[DAY].reconciliation_state == "reconciled"


def test_a_decided_day_is_frozen_against_a_direct_update(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ON CONFLICT predicate protects one write path; the invariant needs every path."""
    record_reconciliation_day(ebull_test_conn, environment="demo", evidence=_evidence())
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "UPDATE account_reconciliation_days SET reconciliation_state='diverged' WHERE snapshot_date=%s",
            (DAY,),
        )


def test_a_rule_version_bump_starts_a_parallel_series_it_does_not_overwrite(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """So "bump the version, re-verdict only the red days" cannot work: greens go too."""
    record_reconciliation_day(ebull_test_conn, environment="demo", evidence=_evidence())
    bumped = replace(_evidence(state="diverged", difference="999.00"), reconciliation_rule_version="f0-reconcile-v2")
    assert record_reconciliation_day(ebull_test_conn, environment="demo", evidence=bumped) is True

    # The counter reads only the CURRENT tolerance rule, so the old series is invisible
    # to it -- which is what makes a bump reset the countdown rather than curate it.
    current = load_reconciliation_days(ebull_test_conn, environment="demo", as_of=DAY)
    assert current[DAY].reconciliation_state == "reconciled"
    total = ebull_test_conn.execute("SELECT count(*) FROM account_reconciliation_days").fetchone()
    assert total is not None and total[0] == 2


def test_a_nan_money_term_cannot_reach_a_decided_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ PG NUMERIC NaN is not IEEE: `'NaN' >= 0` is TRUE, so a one-sided CHECK admits it.

    Python's side is exercised too, in the same test: ``Decimal('NaN').is_finite()`` is
    False, so the writer drops the term, and a *comparable* verdict missing a money term is
    refused rather than written as a contradiction of the table's own CHECK.
    """
    with pytest.raises(ReconciliationLedgerError):
        record_reconciliation_day(ebull_test_conn, environment="demo", evidence=_evidence(difference="NaN"))
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            """
            INSERT INTO account_reconciliation_days (
                environment,reconciliation_rule_version,snapshot_date,countdown_rule_version,
                reconciliation_state,comparable,difference,tolerance,decided_at
            ) VALUES ('demo',%s,%s,%s,'reconciled',true,'NaN'::numeric,1,now())
            """,
            (RECONCILIATION_RULE_VERSION, DAY, COUNTDOWN_RULE_VERSION),
        )


def test_a_day_with_no_broker_snapshot_is_not_recorded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """`unavailable` means no broker day exists, so there is no day to record."""
    nothing = replace(_evidence(), status="unavailable", snapshot_date=None)
    assert record_reconciliation_day(ebull_test_conn, environment="demo", evidence=nothing) is False
    assert load_reconciliation_days(ebull_test_conn, environment="demo", as_of=DAY) == {}


def test_a_run_where_every_candidate_raises_is_a_failed_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A no-op that reports success is invisible to every automated check we have.

    `_tracked_job` writes `status='success'` unless the body raises, so a silent `return 0`
    here would be indistinguishable from the documented healthy steady state while the
    reconciliation pipeline was dead.
    """
    import app.services.account_reconciliation_ledger as ledger

    session = date(2026, 9, 2)  # a Wednesday, so it is on the NYSE calendar
    monkey = pytest.MonkeyPatch()
    monkey.setattr(ledger, "pending_reconciliation_dates", lambda *a, **k: (session,))
    monkey.setattr(
        ledger,
        "load_account_equity_evidence",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("comparison unavailable")),
    )
    try:
        with pytest.raises(ReconciliationLedgerError, match="2026-09-02"):
            ledger.run_reconciliation_check(ebull_test_conn, as_of=date(2026, 9, 10))
    finally:
        monkey.undo()


def test_an_undecided_verdict_with_no_reason_is_refused_not_papered_over(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Unreachable by the loader's construction, which is exactly why it must raise.

    Synthesising a placeholder reason would satisfy the table's CHECK and turn a loader
    regression into ordinary-looking data. The mirrored case — a comparable verdict missing
    a money term — already raises; this closes the asymmetry.
    """
    with pytest.raises(ReconciliationLedgerError, match="no refusal reason"):
        record_reconciliation_day(
            ebull_test_conn,
            environment="demo",
            evidence=_evidence(state="refused", comparable=False, difference=None, tolerance=None),
        )
