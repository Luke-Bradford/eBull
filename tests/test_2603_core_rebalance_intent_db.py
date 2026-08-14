"""#2603 item 3 step 1 — what ``sql/348`` enforces, and the writer's round trip.

⚠ THE MIGRATION'S BACKSTOP, NOT THE PYTHON RULE. Every rejection case below
INSERTs raw SQL that bypasses ``record_core_rebalance_intent``, because a CHECK
that only ever sees writer-produced values is not demonstrably a backstop at all —
a later migration relaxing one would be invisible.

⚠ One case per NULL-BEARING combination, not just the happy path. A CHECK passes
on NULL, so ``col = 'x'`` does not require ``col = 'x'``; the constraint keeps
refusing every wrong VALUE you thought to test and admits the OMISSION it exists
to catch, which is what a writer bug actually produces
(``docs/review-prevention-log.md``: #2679, and ``sql/341``).

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.strategy_core_allocator import CoreSleeveState
from app.services.strategy_core_mandate import CORE_MANDATE_POLICY_VERSION
from app.services.strategy_core_rebalance_intent import record_core_rebalance_intent

_INSERT = """
INSERT INTO strategy_core_rebalance_intents (
    core_mandate_event_id, allocator_policy_version, recorded_by,
    core_instrument_id, currency, core_market_value, cash_balance, state_as_of,
    action, reason_code, amount, core_pct, target_pct, lower_pct, upper_pct,
    effective_floor, floor_source, reserve_breached, reserve_margin_pct
) VALUES (
    %(core_mandate_event_id)s, %(allocator_policy_version)s, %(recorded_by)s,
    %(core_instrument_id)s, %(currency)s, %(core_market_value)s,
    %(cash_balance)s, %(state_as_of)s,
    %(action)s, %(reason_code)s, %(amount)s, %(core_pct)s, %(target_pct)s,
    %(lower_pct)s, %(upper_pct)s, %(effective_floor)s, %(floor_source)s,
    %(reserve_breached)s, %(reserve_margin_pct)s
)
"""

_INSTRUMENT_ID = 920604

#: A valuation instant safely before ``now()``. Fixed rather than relative so the
#: row is deterministic; the ``state_as_of <= evaluated_at`` CHECK is real and a
#: same-day wall-clock literal trips it depending on the hour the suite runs.
_PAST = datetime(2026, 1, 2, 9, 0, tzinfo=UTC)

#: A schema-valid HOLD, in band. Every rejection case below is this row with one
#: field moved, so a failure names the field rather than the row.
_HOLD: dict[str, Any] = {
    "core_mandate_event_id": None,  # filled by the fixture
    "allocator_policy_version": CORE_MANDATE_POLICY_VERSION,
    "recorded_by": "test",
    "core_instrument_id": _INSTRUMENT_ID,
    "currency": "USD",
    "core_market_value": Decimal("600.000000"),
    "cash_balance": Decimal("400.000000"),
    "state_as_of": _PAST,
    "action": "hold",
    "reason_code": None,
    "amount": Decimal("0"),
    "core_pct": Decimal("60"),
    "target_pct": Decimal("60"),
    "lower_pct": Decimal("55"),
    "upper_pct": Decimal("65"),
    "effective_floor": Decimal("25"),
    "floor_source": "mandate",
    "reserve_breached": False,
    "reserve_margin_pct": Decimal("20"),
}

_DERIVED = (
    "core_pct",
    "target_pct",
    "lower_pct",
    "upper_pct",
    "effective_floor",
    "floor_source",
    "reserve_breached",
    "reserve_margin_pct",
)


def _seed_mandate(conn: psycopg.Connection[Any]) -> int:
    """One instrument and one enabled mandate revision to point the FK at."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'CORE.INTENT','Core Intent Test',TRUE) ON CONFLICT DO NOTHING",
        (_INSTRUMENT_ID,),
    )
    row = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason
        ) VALUES (1,TRUE,'USD',%s,60,20,5,25,%s,'test','test')
        RETURNING core_mandate_event_id
        """,
        (_INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _row(event_id: int, **overrides: Any) -> dict[str, Any]:
    return {**_HOLD, "core_mandate_event_id": event_id, **overrides}


def test_the_reference_hold_inserts(ebull_test_conn: psycopg.Connection[Any]) -> None:
    event_id = _seed_mandate(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _row(event_id))


def test_a_hold_may_carry_the_floor_suppression_reason(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ NOT an error, and the constraint set nearly said it was.

    ``strategy_core_allocator.py:303`` returns
    ``_decide("hold", _ZERO, "below_min_rebalance_amount", core_pct)`` when the gap
    to the band edge is under the floor. A ``hold ⟹ reason_code IS NULL`` rule
    would have made the allocator's own output unstorable.
    """
    event_id = _seed_mandate(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _row(event_id, reason_code="below_min_rebalance_amount"))


def test_a_buy_inserts(ebull_test_conn: psycopg.Connection[Any]) -> None:
    event_id = _seed_mandate(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _row(event_id, action="buy_core", amount=Decimal("50")))


def test_a_mandate_absent_refusal_inserts_with_no_event_and_no_valuation(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The one verdict with no revision to cite, carrying unrepresentable inputs.

    Both NULLs together: this is the row the naive design could not write at all.
    """
    _seed_mandate(ebull_test_conn)
    ebull_test_conn.execute(
        _INSERT,
        {
            **_HOLD,
            "core_mandate_event_id": None,
            "action": "refused",
            "reason_code": "core_mandate_absent",
            "amount": Decimal("0"),
            "core_market_value": None,
            "cash_balance": None,
            **dict.fromkeys(_DERIVED),
        },
    )


@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        # --- refused: no weights were computed, so a derived field is a writer bug
        *[
            (
                f"refused_with_{field}",
                {
                    "action": "refused",
                    "reason_code": "core_mandate_disabled",
                    **dict.fromkeys(_DERIVED),
                    field: _HOLD[field],
                },
            )
            for field in _DERIVED
        ],
        (
            "refused_without_reason",
            {"action": "refused", "reason_code": None, **dict.fromkeys(_DERIVED)},
        ),
        (
            "refused_with_amount",
            {
                "action": "refused",
                "reason_code": "core_mandate_disabled",
                "amount": Decimal("1"),
                **dict.fromkeys(_DERIVED),
            },
        ),
        # --- hold
        ("hold_with_amount", {"amount": Decimal("1")}),
        ("hold_with_a_foreign_reason", {"reason_code": "core_sleeve_empty"}),
        # ⚠ The OMISSION case, one per derived field. This is what a writer bug
        # produces, and what a happy-path-only test cannot see.
        *[(f"hold_missing_{field}", {field: None}) for field in _DERIVED],
        ("hold_with_band_inverted", {"lower_pct": Decimal("70")}),
        ("hold_with_target_above_upper", {"upper_pct": Decimal("55")}),
        ("hold_with_zero_floor", {"effective_floor": Decimal("0")}),
        # --- buy / sell
        ("buy_with_zero_amount", {"action": "buy_core", "amount": Decimal("0")}),
        (
            "buy_with_a_reason",
            {"action": "buy_core", "amount": Decimal("50"), "reason_code": "core_sleeve_empty"},
        ),
        (
            "sell_missing_floor_source",
            {"action": "sell_core", "amount": Decimal("50"), "floor_source": None},
        ),
        # --- enums
        ("unknown_action", {"action": "rebalance"}),
        ("unknown_reason", {"reason_code": "made_up", "action": "hold"}),
        ("unknown_floor_source", {"floor_source": "operator"}),
        # --- the null-observation rule: unstorable inputs imply a refusal
        ("hold_with_null_core_value", {"core_market_value": None}),
        ("hold_with_null_cash", {"cash_balance": None}),
        ("hold_with_null_currency", {"currency": None}),
        (
            "buy_with_null_cash",
            {"action": "buy_core", "amount": Decimal("50"), "cash_balance": None},
        ),
        # --- temporal
        (
            "state_after_evaluation",
            {"state_as_of": datetime.now(UTC) + timedelta(hours=1)},
        ),
        # --- text shape
        ("blank_recorded_by", {"recorded_by": "   "}),
        ("blank_currency", {"currency": ""}),
        ("blank_policy_version", {"allocator_policy_version": ""}),
    ],
)
def test_the_checks_reject_raw_inserts(
    ebull_test_conn: psycopg.Connection[Any], label: str, overrides: dict[str, Any]
) -> None:
    event_id = _seed_mandate(ebull_test_conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(_INSERT, _row(event_id, **overrides))


@pytest.mark.parametrize(
    ("label", "event_id_is_null", "reason_code", "action"),
    [
        # The event id is absent for EXACTLY one verdict. Both directions, because
        # a one-directional rule admits the other half silently.
        ("absent_without_event_is_required", False, "core_mandate_absent", "refused"),
        ("other_refusal_needs_an_event", True, "core_mandate_disabled", "refused"),
        ("a_hold_needs_an_event", True, None, "hold"),
    ],
)
def test_the_event_link_is_absent_for_exactly_one_verdict(
    ebull_test_conn: psycopg.Connection[Any],
    label: str,
    event_id_is_null: bool,
    reason_code: str | None,
    action: str,
) -> None:
    event_id = _seed_mandate(ebull_test_conn)
    overrides: dict[str, Any] = {"action": action, "reason_code": reason_code}
    if action == "refused":
        overrides.update(dict.fromkeys(_DERIVED))
        overrides["amount"] = Decimal("0")
    row = _row(event_id, **overrides)
    if event_id_is_null:
        row["core_mandate_event_id"] = None
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(_INSERT, row)


def test_the_mandate_event_fk_restricts_deletes(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """An evidence row may not be orphaned by deleting what it cites."""
    event_id = _seed_mandate(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _row(event_id))
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        ebull_test_conn.execute(
            "DELETE FROM strategy_core_mandate_events WHERE core_mandate_event_id=%s",
            (event_id,),
        )


def test_the_writer_stores_the_verdict_it_returns(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Round trip through the real writer against the real mandate.

    600/400 against a 60% target with a 5-point band is exactly in band, so the
    verdict is a hold — and the stored row must carry the same numbers the
    returned decision does, which is what a positional-list mistake breaks.
    """
    event_id = _seed_mandate(ebull_test_conn)
    intent = record_core_rebalance_intent(
        ebull_test_conn,
        state=CoreSleeveState(
            core_instrument_id=_INSTRUMENT_ID,
            core_market_value=Decimal("600"),
            cash_balance=Decimal("400"),
            currency="USD",
            as_of=_PAST,
        ),
        recorded_by="test",
    )
    assert intent.decision.action == "hold"
    assert intent.core_mandate_event_id == event_id

    row = ebull_test_conn.execute(
        "SELECT action, core_pct, target_pct, lower_pct, upper_pct, amount, "
        "floor_source, core_market_value, allocator_policy_version "
        "FROM strategy_core_rebalance_intents WHERE core_rebalance_intent_id=%s",
        (intent.core_rebalance_intent_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "hold"
    assert row[1] == intent.decision.core_pct
    assert row[2] == Decimal("60.0000")
    assert row[3] == Decimal("55.0000")
    assert row[4] == Decimal("65.0000")
    assert row[5] == Decimal("0.000000")
    # `broker_minimum` is not a parameter of this writer, so the floor is always
    # the mandate's own — stated as an assertion so the executor slice that
    # widens it has to change a test rather than slip past one.
    assert row[6] == "mandate"
    assert row[7] == Decimal("600.000000")
    assert row[8] == CORE_MANDATE_POLICY_VERSION


def test_an_unstorable_valuation_is_recorded_rather_than_lost(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The trap the NULL semantics exists for, end to end through the writer.

    A 10^30 cash balance refuses ``sleeve_valuation_invalid``. Storing it into
    NUMERIC(18,6) would raise ``numeric field overflow``, making the evidence for
    an unrepresentable valuation the one row that cannot be written.
    """
    _seed_mandate(ebull_test_conn)
    intent = record_core_rebalance_intent(
        ebull_test_conn,
        state=CoreSleeveState(
            core_instrument_id=_INSTRUMENT_ID,
            core_market_value=Decimal("600"),
            cash_balance=Decimal("1e30"),
            currency="USD",
            as_of=_PAST,
        ),
        recorded_by="test",
    )
    assert intent.decision.action == "refused"
    assert intent.decision.reason_code == "sleeve_valuation_invalid"

    row = ebull_test_conn.execute(
        "SELECT cash_balance, core_market_value, reason_code "
        "FROM strategy_core_rebalance_intents WHERE core_rebalance_intent_id=%s",
        (intent.core_rebalance_intent_id,),
    ).fetchone()
    assert row is not None
    assert row[0] is None
    # The representable component is still stored: NULL is per-column and means
    # "this value had no representation", not "the observation was discarded".
    assert row[1] == Decimal("600.000000")
    assert row[2] == "sleeve_valuation_invalid"


def test_a_nan_valuation_is_recorded_rather_than_raising(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``Decimal("NaN")`` is the other unrepresentable shape, and the one that
    would reach ``InvalidOperation`` if finiteness were checked after magnitude."""
    _seed_mandate(ebull_test_conn)
    intent = record_core_rebalance_intent(
        ebull_test_conn,
        state=CoreSleeveState(
            core_instrument_id=_INSTRUMENT_ID,
            core_market_value=Decimal("NaN"),
            cash_balance=Decimal("400"),
            currency="USD",
            as_of=_PAST,
        ),
        recorded_by="test",
    )
    assert intent.decision.reason_code == "sleeve_valuation_invalid"
    row = ebull_test_conn.execute(
        "SELECT core_market_value FROM strategy_core_rebalance_intents WHERE core_rebalance_intent_id=%s",
        (intent.core_rebalance_intent_id,),
    ).fetchone()
    assert row is not None and row[0] is None


def test_no_mandate_configured_records_the_absent_verdict(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The FK-nullable path through the real writer, with nothing seeded."""
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'CORE.INTENT','Core Intent Test',TRUE) ON CONFLICT DO NOTHING",
        (_INSTRUMENT_ID,),
    )
    intent = record_core_rebalance_intent(
        ebull_test_conn,
        state=CoreSleeveState(
            core_instrument_id=_INSTRUMENT_ID,
            core_market_value=Decimal("600"),
            cash_balance=Decimal("400"),
            currency="USD",
            as_of=_PAST,
        ),
        recorded_by="test",
    )
    assert intent.decision.action == "refused"
    assert intent.decision.reason_code == "core_mandate_absent"
    assert intent.core_mandate_event_id is None


def test_an_unstorable_currency_is_recorded_rather_than_lost(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The same trap as the valuations, on the column where it is easiest to miss.

    A blank currency never matches the mandate's base currency, so
    ``_state_refusal`` returns ``sleeve_currency_mismatch`` — and a NOT NULL
    non-blank column would then make that refusal the one row that cannot be
    written. Caught by Codex checkpoint 2 after the valuation columns had already
    been shaped for exactly this.
    """
    _seed_mandate(ebull_test_conn)
    intent = record_core_rebalance_intent(
        ebull_test_conn,
        state=CoreSleeveState(
            core_instrument_id=_INSTRUMENT_ID,
            core_market_value=Decimal("600"),
            cash_balance=Decimal("400"),
            currency="   ",
            as_of=_PAST,
        ),
        recorded_by="test",
    )
    assert intent.decision.action == "refused"
    assert intent.decision.reason_code == "sleeve_currency_mismatch"

    row = ebull_test_conn.execute(
        "SELECT currency, core_market_value FROM strategy_core_rebalance_intents WHERE core_rebalance_intent_id=%s",
        (intent.core_rebalance_intent_id,),
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] == Decimal("600.000000")


def test_the_observed_currency_is_stored_unnormalised(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """What was OBSERVED is the evidence: ``" usd "`` matches and is stored as sent.

    Normalising on the way in would hide the difference between a caller sending
    ``"USD"`` and one sending ``" usd "`` — which is the kind of thing an
    unexplained mismatch a month from now turns on.
    """
    _seed_mandate(ebull_test_conn)
    intent = record_core_rebalance_intent(
        ebull_test_conn,
        state=CoreSleeveState(
            core_instrument_id=_INSTRUMENT_ID,
            core_market_value=Decimal("600"),
            cash_balance=Decimal("400"),
            currency=" usd ",
            as_of=_PAST,
        ),
        recorded_by="test",
    )
    assert intent.decision.action == "hold"
    row = ebull_test_conn.execute(
        "SELECT currency FROM strategy_core_rebalance_intents WHERE core_rebalance_intent_id=%s",
        (intent.core_rebalance_intent_id,),
    ).fetchone()
    assert row is not None and row[0] == " usd "


def test_a_snapshot_taken_after_the_transaction_opened_is_not_future_dated(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``evaluated_at`` defaults to ``clock_timestamp()``, not ``now()``.

    ``now()`` is TRANSACTION start time. A writer called inside a transaction that
    opened before the sleeve was valued would stamp the evaluation earlier than
    the observation, and the ``state_as_of <= evaluated_at`` CHECK would reject a
    perfectly fresh snapshot for no reason but the caller's transaction boundary.

    The 50ms sleep pins the transaction start well behind the wall clock, so the
    10ms-after-``now()`` snapshot is unambiguously inside the window ``now()``
    rejects and ``clock_timestamp()`` accepts.
    """
    event_id = _seed_mandate(ebull_test_conn)  # opens the transaction
    ebull_test_conn.execute("SELECT pg_sleep(0.05)")
    row = ebull_test_conn.execute("SELECT now() + interval '10 milliseconds', now() < clock_timestamp()").fetchone()
    assert row is not None
    snapshot_at, clock_has_advanced = row
    assert clock_has_advanced, "transaction time did not lag the wall clock; test is inert"

    ebull_test_conn.execute(_INSERT, _row(event_id, state_as_of=snapshot_at))
    stored = ebull_test_conn.execute(
        "SELECT evaluated_at > now() FROM strategy_core_rebalance_intents "
        "ORDER BY core_rebalance_intent_id DESC LIMIT 1"
    ).fetchone()
    assert stored is not None and stored[0] is True
