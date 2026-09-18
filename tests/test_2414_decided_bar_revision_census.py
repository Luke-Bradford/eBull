"""#2414 — the census's arm declaration is the thing under test, not Postgres.

Pure-logic, no DB (repo default: extract the decision and table-test it). The arms
ARE the data-treatment decision — which (signal, event) pairs count as staleness —
so changing one of these predicates must be a review event, and these tests are what
make it one.

Spec: ``docs/proposals/ta/2026-09-18-decided-bar-revision-census.md``.
"""

from __future__ import annotations

import pytest

from scripts.census_2414_decided_bar_revisions import (
    ARMS,
    CROSS_CHECK_SQL,
    DIRECT_ARM_RATIOS_SQL,
    DIRECT_ARM_SQL,
    INSERT_SOURCE,
    REVISION_SOURCE,
    Arm,
    arm_sql,
    observable_split,
    union_sql,
)

_SOURCES = (REVISION_SOURCE, INSERT_SOURCE)


def _arm(key: str) -> Arm:
    return next(a for a in ARMS if a.key == key)


class TestObservableSplit:
    """The denominator. ⚠ The bug this guards is reporting a measured zero over a
    population no instrument was watching — see the ``no floor`` case."""

    @pytest.mark.parametrize(
        ("total", "before_floor", "expected"),
        [
            (59069, 58886, (183, 58886)),
            (100, 0, (100, 0)),  # relation predates the whole ledger — all observable
            (100, 100, (0, 100)),  # no floor (empty relation) — NOTHING observable
            (0, 0, (0, 0)),  # empty ledger
        ],
    )
    def test_split(self, total: int, before_floor: int, expected: tuple[int, int]) -> None:
        assert observable_split(total, before_floor) == expected

    def test_observable_plus_unmeasurable_is_the_whole_ledger(self) -> None:
        observable, unmeasurable = observable_split(59069, 58886)
        assert observable + unmeasurable == 59069

    def test_more_signals_before_the_floor_than_exist_is_a_contradiction(self) -> None:
        with pytest.raises(ValueError, match="exceeds total"):
            observable_split(10, 11)

    def test_negative_counts_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            observable_split(-1, -1)


class TestArmDeclaration:
    """The predicates are the spec. A change here changes what "stale" means."""

    def test_the_three_arms_are_exactly_these(self) -> None:
        assert [a.key for a in ARMS] == ["decision_bar", "fill_bar", "prefix"]

    def test_decision_bar_is_equality_on_the_signals_own_bar(self) -> None:
        assert _arm("decision_bar").date_predicate == "e.price_date = s.signal_bar_date"

    def test_fill_bar_is_equality_on_the_fill_bar_and_is_fired_only(self) -> None:
        # `strategy_signals_fill_matches_verdict` makes fill_bar_date NULL on every
        # non-fired row, so without the filter the arm would join on NULL and
        # silently report only fired rows anyway — by accident rather than by rule.
        arm = _arm("fill_bar")
        assert arm.date_predicate == "e.price_date = s.fill_bar_date"
        assert arm.fired_only is True

    def test_prefix_is_STRICTLY_before_the_decision_bar(self) -> None:
        # ⚠ Strict `<`, not `<=`. With `<=` the prefix arm would swallow every
        # decision_bar hit and the two would stop being disjoint.
        assert _arm("prefix").date_predicate == "e.price_date < s.signal_bar_date"

    def test_only_the_fill_arm_is_fired_only(self) -> None:
        assert [a.key for a in ARMS if a.fired_only] == ["fill_bar"]

    def test_every_arm_states_its_claim(self) -> None:
        for arm in ARMS:
            assert arm.claim.strip()

    def test_the_prefix_arms_claim_says_it_is_not_a_bound(self) -> None:
        # sql/387's header kills the join as "neither an upper nor a lower bound".
        # A claim that dropped that wording would be the over-read the header exists
        # to prevent.
        assert "NEITHER BOUND" in _arm("prefix").claim


class TestArmSql:
    @pytest.mark.parametrize("arm", ARMS, ids=[a.key for a in ARMS])
    @pytest.mark.parametrize("source", _SOURCES, ids=[s[0] for s in _SOURCES])
    def test_every_arm_compares_the_event_stamp_against_the_signals_write_time(
        self, arm: Arm, source: tuple[str, str]
    ) -> None:
        # Without this the arm counts bars overwritten BEFORE the verdict was
        # stored — which the scan read in their corrected form.
        table, stamp = source
        assert f"e.{stamp} > s.created_at" in arm_sql(arm, table=table, stamp=stamp, by_cause=False)

    @pytest.mark.parametrize("arm", ARMS, ids=[a.key for a in ARMS])
    def test_every_arm_counts_distinct_signals_never_pairs(self, arm: Arm) -> None:
        # `_record_bar_revisions` documents that revised dates MAY REPEAT, so one
        # signal can match through several rows.
        table, stamp = REVISION_SOURCE
        for by_cause in (False, True):
            assert "COUNT(DISTINCT s.signal_id)" in arm_sql(
                arm, table=table, stamp=stamp, by_cause=by_cause
            )

    @pytest.mark.parametrize("arm", ARMS, ids=[a.key for a in ARMS])
    def test_every_arm_is_floor_gated(self, arm: Arm) -> None:
        table, stamp = REVISION_SOURCE
        assert "s.created_at >= %(floor)s" in arm_sql(arm, table=table, stamp=stamp, by_cause=False)

    def test_by_cause_groups_and_plain_does_not(self) -> None:
        table, stamp = REVISION_SOURCE
        arm = _arm("decision_bar")
        assert "GROUP BY e.cause" in arm_sql(arm, table=table, stamp=stamp, by_cause=True)
        assert "GROUP BY" not in arm_sql(arm, table=table, stamp=stamp, by_cause=False)

    def test_the_fired_filter_reaches_the_sql_for_the_fill_arm_only(self) -> None:
        table, stamp = REVISION_SOURCE
        rendered = {a.key: arm_sql(a, table=table, stamp=stamp, by_cause=False) for a in ARMS}
        assert "s.verdict = 'fired'" in rendered["fill_bar"]
        assert "s.verdict = 'fired'" not in rendered["decision_bar"]
        assert "s.verdict = 'fired'" not in rendered["prefix"]

    def test_the_union_carries_every_arms_predicate(self) -> None:
        table, stamp = REVISION_SOURCE
        rendered = union_sql(table=table, stamp=stamp)
        for arm in ARMS:
            assert arm.date_predicate in rendered
        # ⚠ The union must be an OR of the arms, not a sum of them: one signal can
        # match several and summing double-counts it.
        assert rendered.count(" OR ") == len(ARMS) - 1

    def test_the_union_keeps_the_fill_arms_fired_filter_scoped_to_that_arm(self) -> None:
        # A stray `AND s.verdict = 'fired'` outside the parenthesised OR would
        # silently restrict the whole union to fired signals.
        table, stamp = REVISION_SOURCE
        rendered = union_sql(table=table, stamp=stamp)
        assert "(e.price_date = s.fill_bar_date AND s.verdict = 'fired')" in rendered

    @pytest.mark.parametrize("source", _SOURCES, ids=[s[0] for s in _SOURCES])
    def test_arms_read_only_the_named_audit_relation(self, source: tuple[str, str]) -> None:
        table, stamp = source
        other = next(t for t, _ in _SOURCES if t != table)
        rendered = arm_sql(_arm("decision_bar"), table=table, stamp=stamp, by_cause=False)
        assert table in rendered
        assert other not in rendered


class TestDirectArm:
    """Part 1 — the exact arm, which needs no audit relation at all."""

    def test_it_is_fired_only(self) -> None:
        # `fill_price` is NULL on every non-fired verdict, so without this the
        # "agrees" bucket would absorb them.
        assert "s.verdict = 'fired'" in DIRECT_ARM_SQL

    def test_it_left_joins_so_a_vanished_bar_is_its_own_bucket(self) -> None:
        # An INNER join would silently drop a fill whose bar no longer exists —
        # a distinct failure from a price that moved.
        assert "LEFT JOIN price_daily" in DIRECT_ARM_SQL
        assert "p.open IS NULL" in DIRECT_ARM_SQL

    def test_it_joins_on_the_fill_bar_not_the_signal_bar(self) -> None:
        # fill_price is the OPEN of the bar AFTER the signal (`resolve_fills`).
        assert "p.price_date = s.fill_bar_date" in DIRECT_ARM_SQL
        assert "p.price_date = s.signal_bar_date" not in DIRECT_ARM_SQL

    def test_it_carries_no_floor_because_it_needs_no_telemetry(self) -> None:
        # The whole point of part 1: it reads a stored copy, so it covers the whole
        # ledger. A floor here would reintroduce the denominator problem.
        assert "%(floor)s" not in DIRECT_ARM_SQL

    def test_the_ratio_query_guards_a_zero_fill_price(self) -> None:
        assert "nullif(s.fill_price, 0)" in DIRECT_ARM_RATIOS_SQL


class TestCrossCheck:
    """The reconciliation that turns sql/387's field-blindness into a number."""

    def test_it_partitions_the_fill_arms_rows_by_whether_the_open_moved(self) -> None:
        assert "p.open =  s.fill_price" in CROSS_CHECK_SQL
        assert "p.open <> s.fill_price" in CROSS_CHECK_SQL

    def test_it_is_scoped_to_the_same_observable_window_as_the_arms(self) -> None:
        # Comparing an unfloored part-1 population against a floored part-2 arm
        # would manufacture a disagreement out of the window, not the fields.
        assert "s.created_at >= %(floor)s" in CROSS_CHECK_SQL
        assert "r.revised_at > s.created_at" in CROSS_CHECK_SQL
