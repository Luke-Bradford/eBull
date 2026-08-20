"""Phase 4b — the outcome ledger against a real database.

Spec: ``docs/proposals/ta/2026-08-06-outcome-ledger.md`` §5 acceptances 1, 2, 3,
5 and 6. The pure half is ``tests/test_outcome_ledger.py``.

Four genuinely-new SQL mechanisms live here and nowhere else:

1. **The constraint set**, checked EXHAUSTIVELY against the Python mirror rather
   than by example. A mocked cursor asserts the parameters while passing against
   a constraint that would reject them — and the 3c defect (prevention log,
   #2240 3c) was exactly a mirror that disagreed with its CHECK in a state the
   writer cannot emit. Enumerating the product is the only way to find that
   class by construction instead of by memory.
2. **The INSERT's parent predicate.** The writer stores through
   ``SELECT … FROM strategy_signals WHERE signal_kind = 'entry' AND verdict =
   'fired'``, so a non-qualifying parent inserts zero rows. Asserting the SQL
   text says so proves nothing about what the database does.
3. **The anti-join selector**, whose wrong form (version predicates moved to the
   ``WHERE``) returns zero pending fills and reads as "nothing to do".
4. **CASCADE**, which only a real delete shows.
"""

from __future__ import annotations

import itertools
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services.outcome_ledger import (
    OutcomeRow,
    select_pending_fills,
    store_outcomes,
)
from app.services.outcome_resolver import (
    OUTCOME_CLASSES,
    UNRESOLVED_REASONS,
)

_STRATEGY_ID = "S-TEST-4B"
_STRATEGY_VERSION = "strategy-registry-v1+4b0000"
_RULE_SET = "outcome-resolver-v1+aaaaaa"
_INPUT_RULE_SET = "price-quarantine-v1+bbbbbb"
_INSTRUMENT_ID = 999_004

_SIGNAL_INSERT = """
    INSERT INTO strategy_signals (
        strategy_id, strategy_version, instrument_id, signal_bar_date,
        signal_kind, verdict, not_evaluable_reason, fill_bar_date,
        fill_price, universe, input_rule_set_versions
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(instrument_id)s, %(signal_bar_date)s,
        %(signal_kind)s, %(verdict)s, %(not_evaluable_reason)s, %(fill_bar_date)s,
        %(fill_price)s, %(universe)s, %(input_rule_set_versions)s
    ) RETURNING signal_id
"""

#: ⚠ A RAW insert, bypassing ``store_outcomes``' parent predicate on purpose:
#: this file's matrix arm is testing the CHECK CONSTRAINTS, and routing through
#: the writer would test the writer's own validation instead.
_RAW_OUTCOME_INSERT = """
    INSERT INTO strategy_outcomes (
        signal_id, rule_set_version, input_rule_set_version, outcome,
        resolution_method, reason, exit_bar_date, exit_price, bars_held,
        gross_return_pct
    ) VALUES (
        %(signal_id)s, %(rule_set_version)s, %(input_rule_set_version)s, %(outcome)s,
        %(resolution_method)s, %(reason)s, %(exit_bar_date)s, %(exit_price)s,
        %(bars_held)s, %(gross_return_pct)s
    )
"""


def _insert_signal(
    conn: psycopg.Connection[tuple],
    *,
    signal_bar_date: date,
    signal_kind: str = "entry",
    verdict: str = "fired",
    not_evaluable_reason: str | None = None,
    fill_bar_date: date | None = date(2024, 1, 3),
    fill_price: Decimal | None = Decimal("100"),
) -> int:
    row = conn.execute(
        _SIGNAL_INSERT,
        {
            "strategy_id": _STRATEGY_ID,
            "strategy_version": _STRATEGY_VERSION,
            "instrument_id": _INSTRUMENT_ID,
            "signal_bar_date": signal_bar_date,
            "signal_kind": signal_kind,
            "verdict": verdict,
            "not_evaluable_reason": not_evaluable_reason,
            "fill_bar_date": fill_bar_date,
            "fill_price": fill_price,
            "universe": "survivor_only",
            # #2333 / sql/257 — NOT NULL with no default on the parent table.
            # Immaterial to 4b's own assertions; stated so the parent row is
            # constructible at all.
            "input_rule_set_versions": Jsonb({"indicator_series": "indicator-series-v1+4b0000"}),
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


@pytest.fixture
def signals(ebull_test_conn: psycopg.Connection[tuple]) -> dict[str, int]:
    """One signal of every shape the writer must accept or refuse.

    ⚠ ``instruments.instrument_id`` is eToro's identifier, assigned upstream —
    not a serial, so it must be supplied.
    """
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'OUTX', 'Outcome Test Co', TRUE)",
        (_INSTRUMENT_ID,),
    )
    ids = {
        "fired_entry": _insert_signal(ebull_test_conn, signal_bar_date=date(2024, 1, 2)),
        # ⚠ Its own fill date: `strategy_signals_fill_after_signal` requires
        # fill_bar_date > signal_bar_date, so a shared default cannot serve two
        # signal bars.
        "second_fired_entry": _insert_signal(
            ebull_test_conn, signal_bar_date=date(2024, 1, 4), fill_bar_date=date(2024, 1, 5)
        ),
        "fired_exit": _insert_signal(ebull_test_conn, signal_bar_date=date(2024, 1, 2), signal_kind="exit"),
        "not_fired": _insert_signal(
            ebull_test_conn,
            signal_bar_date=date(2024, 1, 5),
            verdict="not_fired",
            fill_bar_date=None,
            fill_price=None,
        ),
        "not_evaluable": _insert_signal(
            ebull_test_conn,
            signal_bar_date=date(2024, 1, 8),
            verdict="not_evaluable",
            not_evaluable_reason="no_fill_bar",
            fill_bar_date=None,
            fill_price=None,
        ),
    }
    ebull_test_conn.commit()
    return ids


def _row(signal_id: int, **overrides: Any) -> OutcomeRow:
    kwargs: dict[str, Any] = {
        "signal_id": signal_id,
        "rule_set_version": _RULE_SET,
        "input_rule_set_version": _INPUT_RULE_SET,
        "outcome": "tp_hit",
        "resolution_method": "daily_bar",
        "exit_bar_date": date(2024, 1, 10),
        "exit_price": Decimal("110"),
        "bars_held": 5,
        "gross_return_pct": Decimal("0.10"),
    }
    kwargs.update(overrides)
    return OutcomeRow(**kwargs)


class _Rollback(Exception):
    """Sentinel — unwinds the savepoint after a successful trial insert."""


class TestConstraintSetMirrorsThePythonValidator:
    """⚠ Acceptance 1 — EXHAUSTIVE, not by example.

    Every (outcome × reason × payload-presence) combination is offered to both
    ``OutcomeRow`` and to Postgres, and the two verdicts must agree on every
    one. The half-populated states the writer cannot emit — a date with no bar
    count, a price with no return — are enumerated here rather than remembered,
    which is what the 3c mirror defect needed and did not have.
    """

    _PAYLOAD_VALUES: dict[str, Any] = {
        "exit_bar_date": date(2024, 1, 10),
        "exit_price": Decimal("110"),
        "bars_held": 5,
        "gross_return_pct": Decimal("0.10"),
    }

    @staticmethod
    def _python_accepts(candidate: dict[str, Any]) -> bool:
        try:
            OutcomeRow(**candidate)
        except ValueError:
            return False
        return True

    @staticmethod
    def _postgres_accepts(conn: psycopg.Connection[tuple], candidate: dict[str, Any]) -> bool:
        try:
            with conn.transaction():
                conn.execute(_RAW_OUTCOME_INSERT, candidate)
                raise _Rollback
        except _Rollback:
            return True
        except psycopg.errors.CheckViolation, psycopg.errors.NotNullViolation:
            return False

    def _candidates(self, signal_id: int) -> list[dict[str, Any]]:
        fields = sorted(self._PAYLOAD_VALUES)
        # ⚠ One invalid member per vocabulary, so the matrix covers the
        # out-of-vocabulary case in both directions rather than only the
        # arity ones.
        outcomes = [*sorted(OUTCOME_CLASSES), "win"]
        reasons: list[str | None] = [None, *sorted(UNRESOLVED_REASONS), "delisted"]
        out: list[dict[str, Any]] = []
        for outcome, reason, present in itertools.product(
            outcomes, reasons, itertools.product([False, True], repeat=len(fields))
        ):
            candidate: dict[str, Any] = {
                "signal_id": signal_id,
                "rule_set_version": _RULE_SET,
                "input_rule_set_version": _INPUT_RULE_SET,
                "outcome": outcome,
                "resolution_method": "daily_bar",
                "reason": reason,
            }
            for field, is_present in zip(fields, present, strict=True):
                candidate[field] = self._PAYLOAD_VALUES[field] if is_present else None
            out.append(candidate)

        booked = {
            "outcome": "tp_hit",
            "reason": None,
            **self._PAYLOAD_VALUES,
            "signal_id": signal_id,
            "rule_set_version": _RULE_SET,
            "input_rule_set_version": _INPUT_RULE_SET,
            "resolution_method": "daily_bar",
        }
        # The cases the product cannot express: a bad scalar rather than a bad
        # presence pattern.
        out.append({**booked, "resolution_method": "intraday"})
        out.append({**booked, "bars_held": -1})
        out.append({**booked, "rule_set_version": ""})
        out.append({**booked, "input_rule_set_version": ""})
        return out

    def test_every_combination_agrees(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        candidates = self._candidates(signals["fired_entry"])
        disagreements = [
            (candidate["outcome"], candidate["reason"], {k: v for k, v in candidate.items() if v is not None})
            for candidate in candidates
            if self._python_accepts(candidate) != self._postgres_accepts(ebull_test_conn, candidate)
        ]
        assert not disagreements, (
            f"{len(disagreements)} of {len(candidates)} candidates disagree between the Python mirror "
            f"and the SQL constraints: {disagreements[:5]}"
        )

    def test_the_matrix_exercises_both_verdicts(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        """⚠ A matrix where everything is rejected agrees trivially and proves
        nothing. This pins that both sides of the line are populated."""
        candidates = self._candidates(signals["fired_entry"])
        accepted = sum(1 for c in candidates if self._python_accepts(c))
        assert 0 < accepted < len(candidates)


class TestUniqueness:
    """Acceptance 2 — the three-part key."""

    def test_the_same_signal_and_versions_collide(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        row = _row(signals["fired_entry"])
        assert store_outcomes(ebull_test_conn, [row]) == 1
        with pytest.raises(psycopg.errors.UniqueViolation):
            store_outcomes(ebull_test_conn, [row])

    @pytest.mark.parametrize("member", ["rule_set_version", "input_rule_set_version"])
    def test_either_version_moving_stores_a_second_outcome(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int], member: str
    ) -> None:
        """⚠ The point of both version members: a re-resolution stores BESIDE
        the old outcome rather than overwriting it. ``input_rule_set_version``
        is the one a two-part key would make unstorable — a quarantine rule-set
        change can flip a class with the resolver byte-identical."""
        signal_id = signals["fired_entry"]
        assert store_outcomes(ebull_test_conn, [_row(signal_id)]) == 1
        assert store_outcomes(ebull_test_conn, [_row(signal_id, **{member: "moved+cccccc"})]) == 1
        count = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_outcomes WHERE signal_id = %s", (signal_id,)
        ).fetchone()
        assert count is not None and count[0] == 2


class TestTheWriterRefusesAParentItMayNotResolve:
    """Acceptance 3 — the invariant the FK cannot express.

    ⚠ The refusal must be LOUD. A non-qualifying parent inserts zero rows, so
    without the rowcount comparison the writer would return a short count and
    report success having silently dropped outcomes.
    """

    @pytest.mark.parametrize("shape", ["fired_exit", "not_fired", "not_evaluable"])
    def test_it_raises_and_writes_nothing(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int], shape: str
    ) -> None:
        with pytest.raises(ValueError, match="wrote 0 of 1 rows"):
            store_outcomes(ebull_test_conn, [_row(signals[shape])])
        count = ebull_test_conn.execute("SELECT count(*) FROM strategy_outcomes").fetchone()
        assert count is not None and count[0] == 0

    @pytest.mark.parametrize("shape", ["fired_exit", "not_fired", "not_evaluable"])
    def test_an_unresolved_outcome_cannot_attach_to_an_unfilled_parent(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int], shape: str
    ) -> None:
        """⚠ THE CASE THE TEST ABOVE CANNOT REACH, and a revert probe found it.

        Injecting "drop ``AND s.verdict = 'fired'``" reported NOT CAUGHT. Reading
        the injected source rather than the test: a ``not_fired`` /
        ``not_evaluable`` parent has a NULL ``fill_bar_date``
        (``strategy_signals_fill_matches_verdict`` guarantees it), so with a
        BOOKED outcome the surviving clause
        ``exit_bar_date >= s.fill_bar_date`` evaluates to NULL and refuses the
        row anyway — the verdict clause looks redundant when it is not.

        It stops being redundant the moment the outcome is ``unresolved``:
        ``exit_bar_date`` is NULL, the fill-date clause short-circuits on
        ``IS NULL``, and ONLY ``verdict = 'fired'`` stands between a not-fired
        signal and a stored outcome. That is not a hypothetical shape — it is
        7,780 of the 103,041 rows the full-population arm produced.
        """
        row = _row(
            signals[shape],
            outcome="unresolved",
            reason="window_truncated",
            exit_bar_date=None,
            exit_price=None,
            bars_held=None,
            gross_return_pct=None,
        )
        with pytest.raises(ValueError, match="wrote 0 of 1 rows"):
            store_outcomes(ebull_test_conn, [row])
        count = ebull_test_conn.execute("SELECT count(*) FROM strategy_outcomes").fetchone()
        assert count is not None and count[0] == 0

    def test_the_message_names_the_offending_parent(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        with pytest.raises(ValueError, match=r"parent is fired/exit"):
            store_outcomes(ebull_test_conn, [_row(signals["fired_exit"])])

    def test_an_exit_before_its_fill_is_refused(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        """⚠ Closed by the same predicate, for free. The fill is 2024-01-03."""
        with pytest.raises(ValueError, match="precedes fill"):
            store_outcomes(ebull_test_conn, [_row(signals["fired_entry"], exit_bar_date=date(2024, 1, 2))])

    def test_an_exit_on_the_fill_bar_is_accepted(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        """⚠ ``>=``, not ``>`` — bars_held = 0 is a level touched on the fill bar."""
        row = _row(signals["fired_entry"], exit_bar_date=date(2024, 1, 3), bars_held=0)
        assert store_outcomes(ebull_test_conn, [row]) == 1

    def test_a_partial_batch_raises_rather_than_returning_short(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        rows = [_row(signals["fired_entry"]), _row(signals["fired_exit"])]
        with pytest.raises(ValueError, match="wrote 1 of 2 rows"):
            store_outcomes(ebull_test_conn, rows)


class TestSelectPendingFills:
    """Acceptance 5 — including the anti-join whose wrong form reads as 'nothing to do'."""

    def _pending(
        self,
        conn: psycopg.Connection[tuple],
        *,
        rule_set_version: str = _RULE_SET,
        input_rule_set_version: str = _INPUT_RULE_SET,
    ) -> list[int]:
        return [
            fill.signal_id
            for fill in select_pending_fills(
                conn,
                strategy_id=_STRATEGY_ID,
                strategy_version=_STRATEGY_VERSION,
                rule_set_version=rule_set_version,
                input_rule_set_version=input_rule_set_version,
            )
        ]

    def test_only_fired_entries_are_pending(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        assert sorted(self._pending(ebull_test_conn)) == sorted([signals["fired_entry"], signals["second_fired_entry"]])

    def test_a_resolved_fill_drops_out(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        store_outcomes(ebull_test_conn, [_row(signals["fired_entry"])])
        assert self._pending(ebull_test_conn) == [signals["second_fired_entry"]]

    @pytest.mark.parametrize("member", ["rule_set_version", "input_rule_set_version"])
    def test_a_fill_resolved_at_a_different_version_is_pending_again(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int], member: str
    ) -> None:
        """⚠ The re-resolution path. Bump the resolver OR the quarantine rule
        set and every fill is pending again, old outcomes intact beside it."""
        store_outcomes(ebull_test_conn, [_row(signals["fired_entry"])])
        assert sorted(self._pending(ebull_test_conn, **{member: "moved+cccccc"})) == sorted(
            [signals["fired_entry"], signals["second_fired_entry"]]
        )

    def test_it_carries_the_universe_label(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        """#2288: a label the consumer has to fetch separately is one it omits."""
        fills = select_pending_fills(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_STRATEGY_VERSION,
            rule_set_version=_RULE_SET,
            input_rule_set_version=_INPUT_RULE_SET,
        )
        assert {fill.universe for fill in fills} == {"survivor_only"}
        assert all(fill.fill_price == Decimal("100") for fill in fills)

    def test_the_signal_id_batch_is_bounded(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        signals: dict[str, int],
    ) -> None:
        fills = select_pending_fills(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_STRATEGY_VERSION,
            rule_set_version=_RULE_SET,
            input_rule_set_version=_INPUT_RULE_SET,
            limit=1,
        )
        assert len(fills) == 1
        assert fills[0].signal_id == signals["fired_entry"]
        assert fills[0].signal_bar_date == date(2024, 1, 2)

    def test_cursor_bounds_select_the_next_slice(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        signals: dict[str, int],
    ) -> None:
        after_first = select_pending_fills(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_STRATEGY_VERSION,
            rule_set_version=_RULE_SET,
            input_rule_set_version=_INPUT_RULE_SET,
            after_signal_id=signals["fired_entry"],
        )
        wrapped = select_pending_fills(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_STRATEGY_VERSION,
            rule_set_version=_RULE_SET,
            input_rule_set_version=_INPUT_RULE_SET,
            after_signal_id=0,
            at_or_before_signal_id=signals["fired_entry"],
        )
        assert [fill.signal_id for fill in after_first] == [signals["second_fired_entry"]]
        assert [fill.signal_id for fill in wrapped] == [signals["fired_entry"]]

    def test_non_positive_limit_is_refused(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with pytest.raises(ValueError, match="limit must be positive"):
            select_pending_fills(
                ebull_test_conn,
                strategy_id=_STRATEGY_ID,
                strategy_version=_STRATEGY_VERSION,
                rule_set_version=_RULE_SET,
                input_rule_set_version=_INPUT_RULE_SET,
                limit=0,
            )

    def test_another_strategy_version_is_out_of_scope(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        assert (
            select_pending_fills(
                ebull_test_conn,
                strategy_id=_STRATEGY_ID,
                strategy_version="strategy-registry-v1+other0",
                rule_set_version=_RULE_SET,
                input_rule_set_version=_INPUT_RULE_SET,
            )
            == []
        )


class TestCascade:
    def test_deleting_a_signal_removes_its_outcomes(
        self, ebull_test_conn: psycopg.Connection[tuple], signals: dict[str, int]
    ) -> None:
        """Acceptance 6. An outcome is DERIVED from its signal — one left behind
        is an orphan no query can interpret."""
        store_outcomes(ebull_test_conn, [_row(signals["fired_entry"])])
        ebull_test_conn.execute("DELETE FROM strategy_signals WHERE signal_id = %s", (signals["fired_entry"],))
        count = ebull_test_conn.execute("SELECT count(*) FROM strategy_outcomes").fetchone()
        assert count is not None and count[0] == 0
