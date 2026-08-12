"""#2621 — the frozen-universe refusal function and canonical form (pure tier).

⚠ Lives in the PURE-tier module and is imported by the DB-tier one
(``tests/test_strategy_result_universe_db.py``), never the other way round —
the conftest db-marks a module whose source references the DB fixtures, and a
pure test that leaves the fast tier is a gate that stopped running on push.
"""

from __future__ import annotations

import pytest

from app.services.strategies.validated_universe import VALIDATED_UNIVERSE_RULE_VERSION
from app.services.strategy_result_universe import (
    ResultUniverseRecord,
    record_sha256,
    universe_promotion_refusals,
)


def build_universe_record(
    *,
    version: str = VALIDATED_UNIVERSE_RULE_VERSION,
    evaluated: frozenset[int] = frozenset({1, 2, 3}),
    universe: frozenset[int] = frozenset({1, 2, 3, 4, 5}),
) -> ResultUniverseRecord:
    return ResultUniverseRecord(
        universe_rule_version=version,
        evaluated_instrument_ids=evaluated,
        validated_universe_ids=universe,
    )


class TestTheRefusalFunction:
    def test_a_consistent_subset_record_passes(self) -> None:
        assert universe_promotion_refusals(build_universe_record(), evaluated_instrument_count=3) == ()

    def test_no_record_is_the_one_lone_refusal(self) -> None:
        assert universe_promotion_refusals(None, evaluated_instrument_count=3) == ("evaluated_universe_unrecorded",)

    def test_an_unrecognised_rule_version_refuses(self) -> None:
        refusals = universe_promotion_refusals(
            build_universe_record(version="validated-universe-v0"), evaluated_instrument_count=3
        )
        assert refusals == ("evaluated_universe_rule_unrecognised",)

    def test_a_record_that_disagrees_with_its_row_refuses(self) -> None:
        refusals = universe_promotion_refusals(build_universe_record(), evaluated_instrument_count=4)
        assert refusals == ("evaluated_universe_count_mismatch",)

    def test_an_empty_evaluated_set_is_not_vacuously_inside(self) -> None:
        refusals = universe_promotion_refusals(
            build_universe_record(evaluated=frozenset()), evaluated_instrument_count=0
        )
        assert refusals == ("no_instruments_evaluated",)

    def test_an_instrument_outside_the_frozen_universe_refuses(self) -> None:
        refusals = universe_promotion_refusals(
            build_universe_record(evaluated=frozenset({1, 99})), evaluated_instrument_count=2
        )
        assert refusals == ("instrument_outside_validated_universe",)

    def test_all_refusals_are_returned_not_the_first(self) -> None:
        refusals = universe_promotion_refusals(
            build_universe_record(version="validated-universe-v0", evaluated=frozenset({99})),
            evaluated_instrument_count=7,
        )
        assert refusals == (
            "evaluated_universe_rule_unrecognised",
            "evaluated_universe_count_mismatch",
            "instrument_outside_validated_universe",
        )


class TestTheCanonicalForm:
    def test_the_hash_is_order_independent(self) -> None:
        assert record_sha256(build_universe_record(evaluated=frozenset({3, 1, 2}))) == record_sha256(
            build_universe_record(evaluated=frozenset({1, 2, 3}))
        )

    def test_a_delimiter_in_the_version_cannot_collide_two_records(self) -> None:
        # JSON framing, not joined CSV: a version carrying the join character
        # must not hash like a different version with different ids.
        first = build_universe_record(version='v",1', evaluated=frozenset({1}))
        second = build_universe_record(version="v", evaluated=frozenset({1, 2}))
        assert record_sha256(first) != record_sha256(second)

    def test_an_empty_version_is_refused_at_construction(self) -> None:
        with pytest.raises(ValueError, match="universe_rule_version must be non-empty"):
            build_universe_record(version="")

    def test_a_boolean_id_is_refused_at_construction(self) -> None:
        with pytest.raises(ValueError, match="must contain only ints"):
            build_universe_record(evaluated=frozenset({True, 2}))
