"""Phase 4b — the outcome-ledger writer, pure-logic half.

Spec: ``docs/proposals/ta/2026-08-06-outcome-ledger.md`` §5, acceptances 1
(the Python half of the mirror), 4 and 8. The database half — the constraint
set, the INSERT's parent predicate, the anti-join selector and CASCADE — is
``tests/test_outcome_ledger_db.py``.

⚠ The rejection table below deliberately includes states the WRITER CANNOT
EMIT: a half exit location, a half booked pair. That is the whole point of
mirroring a SQL constraint — the 3c defect (prevention log, #2240 3c) was
invisible to every test written from writer output, because the writer sets
both fields or neither.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.indicator_series import BarSeries
from app.services.outcome_ledger import OutcomeRow, locate_fill_index
from app.services.outcome_resolver import (
    OUTCOME_CLASSES,
    RESOLUTION_METHODS,
    UNRESOLVED_REASONS,
    Outcome,
)
from app.services.technical_analysis import OHLCVRow

_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "256_strategy_outcomes.sql"
_REASON_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "296_strategy_outcomes_unorderable_exit_levels.sql"

_VERSIONS = {"rule_set_version": "outcome-resolver-v1+abc123", "input_rule_set_version": "price-quarantine-v1+def456"}

_BOOKED_PAYLOAD = {
    "exit_bar_date": date(2024, 3, 1),
    "exit_price": Decimal("110"),
    "bars_held": 4,
    "gross_return_pct": Decimal("0.10"),
}
#: `ambiguous` knows its bar and not the touch order, so it carries a location
#: and no price (4a §3.7).
_AMBIGUOUS_PAYLOAD = {"exit_bar_date": date(2024, 3, 1), "bars_held": 4}


def _row(**overrides: object) -> OutcomeRow:
    kwargs: dict[str, object] = {"signal_id": 1, "resolution_method": "daily_bar", **_VERSIONS}
    kwargs.update(overrides)
    return OutcomeRow(**kwargs)  # type: ignore[arg-type]


class TestOutcomeRowAccepts:
    @pytest.mark.parametrize("outcome", ["tp_hit", "sl_hit", "expired"])
    def test_a_booked_outcome_carries_a_location_and_a_price(self, outcome: str) -> None:
        row = _row(outcome=outcome, **_BOOKED_PAYLOAD)
        assert row.exit_price == Decimal("110")
        assert row.gross_return_pct == Decimal("0.10")

    def test_ambiguous_carries_a_location_but_no_price(self) -> None:
        row = _row(outcome="ambiguous", **_AMBIGUOUS_PAYLOAD)
        assert row.exit_bar_date == date(2024, 3, 1)
        assert row.exit_price is None
        assert row.gross_return_pct is None

    @pytest.mark.parametrize("reason", sorted(UNRESOLVED_REASONS))
    def test_unresolved_carries_a_reason_and_nothing_else(self, reason: str) -> None:
        row = _row(outcome="unresolved", reason=reason)
        assert (row.exit_bar_date, row.bars_held, row.exit_price, row.gross_return_pct) == (None, None, None, None)

    def test_bars_held_zero_is_legal(self) -> None:
        """A level touched on the fill bar. Not exposure time (criterion 7)."""
        row = _row(outcome="tp_hit", **{**_BOOKED_PAYLOAD, "bars_held": 0})
        assert row.bars_held == 0


class TestOutcomeRowRejects:
    """⚠ Every case here is a state the SQL CHECKs reject. The mirror is only
    worth having if the two agree exactly — see the module docstring."""

    def test_unknown_outcome(self) -> None:
        with pytest.raises(ValueError, match="unknown outcome"):
            _row(outcome="win", **_BOOKED_PAYLOAD)

    def test_unknown_reason(self) -> None:
        with pytest.raises(ValueError, match="unknown reason"):
            _row(outcome="unresolved", reason="delisted")

    def test_unknown_resolution_method(self) -> None:
        with pytest.raises(ValueError, match="unknown resolution_method"):
            _row(outcome="unresolved", reason="series_break", resolution_method="intraday")

    @pytest.mark.parametrize("blank", ["rule_set_version", "input_rule_set_version"])
    def test_a_blank_version_is_present_and_meaningless(self, blank: str) -> None:
        with pytest.raises(ValueError, match="blank version"):
            _row(outcome="unresolved", reason="series_break", **{blank: ""})

    def test_a_reason_on_a_resolved_outcome(self) -> None:
        with pytest.raises(ValueError, match="a reason is required exactly"):
            _row(outcome="expired", reason="window_truncated", **_BOOKED_PAYLOAD)

    def test_unresolved_without_a_reason(self) -> None:
        with pytest.raises(ValueError, match="a reason is required exactly"):
            _row(outcome="unresolved")

    # ⚠ THE ARITY-1 CASES. `resolve_outcome` cannot produce either — it sets the
    # location fields together — so nothing but this table exercises them, and
    # an ANDed mirror passes both while the SQL CHECK rejects both.
    @pytest.mark.parametrize(
        "payload",
        [
            {"exit_bar_date": date(2024, 3, 1)},
            {"bars_held": 4},
        ],
        ids=["only-a-date", "only-a-bar-count"],
    )
    def test_a_half_exit_location(self, payload: dict[str, object]) -> None:
        with pytest.raises(ValueError, match="partial exit location"):
            _row(outcome="ambiguous", **payload)

    @pytest.mark.parametrize(
        "payload",
        [
            {"exit_bar_date": date(2024, 3, 1)},
            {"bars_held": 4},
        ],
        ids=["only-a-date", "only-a-bar-count"],
    )
    def test_a_half_exit_location_on_an_unresolved_row(self, payload: dict[str, object]) -> None:
        with pytest.raises(ValueError, match="partial exit location"):
            _row(outcome="unresolved", reason="series_break", **payload)

    @pytest.mark.parametrize(
        "payload",
        [
            {"exit_price": Decimal("110")},
            {"gross_return_pct": Decimal("0.10")},
        ],
        ids=["only-a-price", "only-a-return"],
    )
    def test_a_half_booked_pair(self, payload: dict[str, object]) -> None:
        with pytest.raises(ValueError, match="a price and a return exist exactly"):
            _row(outcome="tp_hit", **{**_AMBIGUOUS_PAYLOAD, **payload})

    # ⚠ THE ASYMMETRIC HALF. On a BOOKED outcome an ANDed mirror still rejects a
    # half pair (one field absent scores False against an expected True). It is
    # the NON-booked outcomes where an ANDed form silently passes — half a pair
    # scores False, which matches "not booked" — while the SQL CHECK compares
    # the two nullities separately and rejects it. This is the 3c defect's exact
    # shape, and without these two cases the probe for it reports CAUGHT on the
    # strength of a test that would have caught nothing.
    @pytest.mark.parametrize(
        "payload",
        [
            {"exit_price": Decimal("110")},
            {"gross_return_pct": Decimal("0.10")},
        ],
        ids=["only-a-price", "only-a-return"],
    )
    def test_half_a_booked_pair_on_an_ambiguous_outcome(self, payload: dict[str, object]) -> None:
        with pytest.raises(ValueError, match="a price and a return exist exactly"):
            _row(outcome="ambiguous", **{**_AMBIGUOUS_PAYLOAD, **payload})

    @pytest.mark.parametrize(
        "payload",
        [
            {"exit_price": Decimal("110")},
            {"gross_return_pct": Decimal("0.10")},
        ],
        ids=["only-a-price", "only-a-return"],
    )
    def test_half_a_booked_pair_on_an_unresolved_outcome(self, payload: dict[str, object]) -> None:
        with pytest.raises(ValueError, match="a price and a return exist exactly"):
            _row(outcome="unresolved", reason="series_break", **payload)

    def test_a_price_on_an_ambiguous_outcome(self) -> None:
        """§3.5.4 excludes ambiguous from the win rate; a populated return is a
        column something eventually averages."""
        with pytest.raises(ValueError, match="a price and a return exist exactly"):
            _row(outcome="ambiguous", **_BOOKED_PAYLOAD)

    def test_a_location_missing_from_a_booked_outcome(self) -> None:
        with pytest.raises(ValueError, match="partial exit location"):
            _row(outcome="expired", exit_price=Decimal("110"), gross_return_pct=Decimal("0.10"))

    def test_negative_bars_held(self) -> None:
        with pytest.raises(ValueError, match="bars_held must be non-negative"):
            _row(outcome="tp_hit", **{**_BOOKED_PAYLOAD, "bars_held": -1})


class TestFromOutcome:
    def _outcome(self) -> Outcome:
        return Outcome(
            outcome="tp_hit",
            resolution_method="daily_bar",
            rule_set_version="outcome-resolver-v1+aaaaaa",
            exit_index=7,
            exit_bar_date=date(2024, 3, 1),
            exit_price=Decimal("110"),
            bars_held=4,
            gross_return_pct=Decimal("0.10"),
        )

    def test_it_carries_the_resolvers_version_not_one_of_its_own(self) -> None:
        row = OutcomeRow.from_outcome(42, self._outcome(), input_rule_set_version="price-quarantine-v1+bbbbbb")
        assert row.rule_set_version == "outcome-resolver-v1+aaaaaa"
        assert row.input_rule_set_version == "price-quarantine-v1+bbbbbb"
        assert row.signal_id == 42

    def test_it_drops_the_exit_index(self) -> None:
        """4a §3.7: an index is not durable across a corpus rebuild, the date is."""
        row = OutcomeRow.from_outcome(42, self._outcome(), input_rule_set_version="q+1")
        assert not hasattr(row, "exit_index")
        assert row.exit_bar_date == date(2024, 3, 1)

    def test_the_input_version_has_no_default(self) -> None:
        """#2288: a field with a default is a field a writer can forget — and
        the resolver cannot supply this one, it never sees the loader."""
        with pytest.raises(TypeError):
            OutcomeRow.from_outcome(42, self._outcome())  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "outcome_kwargs",
        [
            {"outcome": "ambiguous", "exit_index": 7, "exit_bar_date": date(2024, 3, 1), "bars_held": 4},
            {"outcome": "unresolved", "reason": "window_truncated"},
        ],
        ids=["ambiguous", "unresolved"],
    )
    def test_every_outcome_shape_projects_to_a_valid_row(self, outcome_kwargs: dict[str, object]) -> None:
        outcome = Outcome(resolution_method="daily_bar", rule_set_version="r+1", **outcome_kwargs)  # type: ignore[arg-type]
        row = OutcomeRow.from_outcome(1, outcome, input_rule_set_version="q+1")
        assert row.outcome == outcome.outcome
        assert row.reason == outcome.reason


class TestLocateFillIndex:
    def _series(self) -> BarSeries:
        dates = (date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8))
        row: OHLCVRow = {
            "open": Decimal("10"),
            "high": Decimal("11"),
            "low": Decimal("9"),
            "close": Decimal("10"),
            "volume": 1000,
        }
        return BarSeries(dates=dates, rows=(row, row, row))

    @pytest.mark.parametrize(
        ("fill_date", "expected"),
        [(date(2024, 1, 4), 0), (date(2024, 1, 5), 1), (date(2024, 1, 8), 2)],
    )
    def test_it_finds_the_stored_date(self, fill_date: date, expected: int) -> None:
        assert locate_fill_index(self._series(), fill_date) == expected

    def test_a_date_the_corpus_no_longer_holds_raises(self) -> None:
        """⚠ The corpus was rebuilt or re-segmented under a recorded decision.
        Silently re-reading whatever bar sits there now is how a ledger stops
        being a record of what was decided."""
        with pytest.raises(ValueError, match="2024-01-06 is not in the 3-bar series"):
            locate_fill_index(self._series(), date(2024, 1, 6))


class TestMigrationVocabularyContract:
    """⚠ Acceptance 8. A closed vocabulary declared in two places and validated
    in neither is the #2218 defect — there, a widened CHECK silently dropped a
    member and applied clean, because Postgres validates a new CHECK against
    EXISTING rows only.
    """

    @staticmethod
    def _check_members(column: str) -> set[str]:
        text = _MIGRATION.read_text()
        match = re.search(rf"CHECK \({column} IN \(([^)]*)\)\)", text, re.DOTALL)
        assert match is not None, f"no CHECK … {column} IN (…) found in {_MIGRATION.name}"
        return set(re.findall(r"'([^']+)'", match.group(1)))

    def test_outcome_classes(self) -> None:
        assert self._check_members("outcome") == set(OUTCOME_CLASSES)

    def test_resolution_methods(self) -> None:
        assert self._check_members("resolution_method") == set(RESOLUTION_METHODS)

    def test_unresolved_reasons(self) -> None:
        text = _REASON_MIGRATION.read_text()
        match = re.search(r"reason IS NULL OR reason IN \(([^)]*)\)", text, re.DOTALL)
        assert match is not None
        assert set(re.findall(r"'([^']+)'", match.group(1))) == set(UNRESOLVED_REASONS)
