"""#2414 — the corpus-provenance stamp. Pure-logic; no database.

The one DB-shaped property worth asserting here is the SQL CHECK mirror, and it
is asserted against the regex rather than a cursor, so the whole file stays in
the fast tier.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date
from decimal import Decimal, localcontext
from typing import cast

import pytest

from app.services.corpus_generation import (
    CORPUS_GENERATION_RULE_VERSION,
    GENERATION_PATTERN,
    CorpusGenerationBuilder,
    encode_value,
)
from app.services.indicator_series import BarSeries
from app.services.market_regime import Regime
from app.services.price_masked_bars import InstrumentBarSpan
from app.services.technical_analysis import OHLCVRow


def _series(rows: Sequence[tuple[date, str | None]]) -> BarSeries:
    # ⚠ `cast`, with the same rationale `price_masked_bars` records for its own
    # `type: ignore`s: `OHLCVRow` declares non-optional Decimals while every
    # field the masked loader produces is maskable. The TypedDict is wrong about
    # nullability; mirroring the production shape is the point of this fixture.
    return BarSeries(
        dates=tuple(day for day, _ in rows),
        rows=tuple(
            cast(
                OHLCVRow,
                {
                    "open": None if close is None else Decimal(close),
                    "high": None if close is None else Decimal(close),
                    "low": None if close is None else Decimal(close),
                    "close": None if close is None else Decimal(close),
                    "volume": Decimal("1000.0000"),
                },
            )
            for _, close in rows
        ),
    )


def _builder() -> CorpusGenerationBuilder:
    builder = CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version="q-v1")
    builder.add_spans({1: _Span(date(2026, 9, 11), 10), 2: _Span(date(2026, 9, 10), 4)})
    builder.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
    builder.add_unresolved_breaks({2: (date(2025, 1, 2),)})
    builder.add_regime([(date(2026, 9, 10), Regime.BULL_QUIET), (date(2026, 9, 11), None)])
    return builder


#: ⚠ The REAL `InstrumentBarSpan`, not a stub. The builder takes the concrete
#: type (review NITPICK on PR #3054), and a stub here would let the fixture drift
#: from the shape the scan actually hands it.
_Span = InstrumentBarSpan

BARS_A: Sequence[tuple[date, str | None]] = [
    (date(2026, 9, 10), "10.500000"),
    (date(2026, 9, 11), "11.000000"),
]


class TestEncodeValue:
    def test_none_uses_a_sentinel_no_decimal_can_produce(self) -> None:
        # A masked field is an INPUT to the decision — the mask is why the
        # strategy saw an absence — so it must be distinguishable from a value.
        assert encode_value(None) == "~"
        assert "~" not in encode_value(Decimal("0.000000"))

    @pytest.mark.parametrize(
        "value",
        [
            # Both columns' extremes. numeric(18,6) reaches 10**12 - 10**-6;
            # numeric(20,4) reaches 10**16 - 10**-4.
            "999999999999.999999",
            "-999999999999.999999",
            "0.000001",
            "0.000000",
            "9999999999999999.9999",
            "0.0000",
        ],
    )
    def test_no_reachable_stored_value_renders_in_exponent_form(self, value: str) -> None:
        """⚠ THE BOUND THAT MAKES ``str`` SAFE HERE, PINNED RATHER THAN COMMENTED.

        ``str(Decimal)`` is context-dependent in general — ``capitals`` selects
        ``E`` or ``e`` — but only in EXPONENT form, which needs a non-negative
        exponent. psycopg's ``NumericLoader`` builds the value from Postgres'
        own text output, so a stored ``numeric(18,6)`` / ``numeric(20,4)`` always
        carries its column scale and the exponent is always negative.

        The context is mutated here deliberately: asserting plain form at the
        default settings would prove nothing about a process that changed them.
        """
        for capitals in (0, 1):
            with localcontext() as ctx:
                ctx.capitals = capitals
                rendered = encode_value(Decimal(value))
            assert "e" not in rendered.lower(), rendered

    def test_the_general_context_claim_is_false_and_that_is_why_the_bound_is_stated(self) -> None:
        """The counterexample itself, so the bound is not mistaken for a law."""
        with localcontext() as ctx:
            ctx.capitals = 1
            upper = encode_value(Decimal("1e30"))
        with localcontext() as ctx:
            ctx.capitals = 0
            lower = encode_value(Decimal("1e30"))
        assert (upper, lower) == ("1E+30", "1e+30")

    def test_a_non_finite_value_is_encoded_rather_than_refused(self) -> None:
        """⚠ ``load_masked_bars`` compares ``open`` ONLY — ``high``/``low``/
        ``close``/``volume`` carry a numeric NaN straight through. A digest that
        refused a value the corpus can hold could not describe the corpus."""
        assert encode_value(Decimal("NaN")) == "NaN"
        assert encode_value(Decimal("NaN")) != encode_value(Decimal("0.000000"))

    def test_two_different_values_never_share_an_encoding(self) -> None:
        values = ["0.000000", "0.000001", "1.000000", "1.500000", "-1.500000", "NaN"]
        encoded = [encode_value(Decimal(value)) for value in values]
        assert len(set(encoded)) == len(values)


class TestBuilder:
    def test_the_stamp_matches_the_sql_check(self) -> None:
        builder = _builder()
        builder.add_series(1, _series(BARS_A))
        generation = builder.finish()
        assert re.fullmatch(GENERATION_PATTERN, generation), generation

    def test_one_corpus_gives_one_stamp(self) -> None:
        first, second = _builder(), _builder()
        first.add_series(1, _series(BARS_A))
        second.add_series(1, _series(BARS_A))
        assert first.finish() == second.finish()

    def test_a_revised_bar_rotates_the_stamp(self) -> None:
        """The motivating case: a value changes, nothing else does."""
        revised: Sequence[tuple[date, str | None]] = [
            (date(2026, 9, 10), "10.500000"),
            (date(2026, 9, 11), "11.250000"),
        ]
        first, second = _builder(), _builder()
        first.add_series(1, _series(BARS_A))
        second.add_series(1, _series(revised))
        assert first.finish() != second.finish()

    def test_a_newly_masked_field_rotates_the_stamp(self) -> None:
        """Masking is an input, not a presentation detail — ``None`` must differ
        from every value, including from a zero."""
        masked: Sequence[tuple[date, str | None]] = [
            (date(2026, 9, 10), "10.500000"),
            (date(2026, 9, 11), None),
        ]
        first, second = _builder(), _builder()
        first.add_series(1, _series(BARS_A))
        second.add_series(1, _series(masked))
        assert first.finish() != second.finish()

    def test_each_non_bar_component_rotates_the_stamp_on_its_own(self) -> None:
        """⚠ Every component, separately. A component folded in but not actually
        reaching the digest is the declared-but-unwired shape, and it is
        invisible to a test that only varies the bars."""
        base = _builder()
        base.add_series(1, _series(BARS_A))
        baseline = base.finish()

        variants: dict[str, CorpusGenerationBuilder] = {}

        moved_span = CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version="q-v1")
        moved_span.add_spans({1: _Span(date(2026, 9, 11), 11), 2: _Span(date(2026, 9, 10), 4)})
        moved_span.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
        moved_span.add_unresolved_breaks({2: (date(2025, 1, 2),)})
        moved_span.add_regime([(date(2026, 9, 10), Regime.BULL_QUIET), (date(2026, 9, 11), None)])
        variants["spans"] = moved_span

        resolved_break = CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version="q-v1")
        resolved_break.add_spans({1: _Span(date(2026, 9, 11), 10), 2: _Span(date(2026, 9, 10), 4)})
        resolved_break.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
        resolved_break.add_unresolved_breaks({})
        resolved_break.add_regime([(date(2026, 9, 10), Regime.BULL_QUIET), (date(2026, 9, 11), None)])
        variants["breaks"] = resolved_break

        reclassified = CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version="q-v1")
        reclassified.add_spans({1: _Span(date(2026, 9, 11), 10), 2: _Span(date(2026, 9, 10), 4)})
        reclassified.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
        reclassified.add_unresolved_breaks({2: (date(2025, 1, 2),)})
        reclassified.add_regime([(date(2026, 9, 10), Regime.BULL_VOLATILE), (date(2026, 9, 11), None)])
        variants["regime"] = reclassified

        moved_frontier = CorpusGenerationBuilder(frontier_date=date(2026, 9, 10), quarantine_rule_set_version="q-v1")
        moved_frontier.add_spans({1: _Span(date(2026, 9, 11), 10), 2: _Span(date(2026, 9, 10), 4)})
        moved_frontier.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
        moved_frontier.add_unresolved_breaks({2: (date(2025, 1, 2),)})
        moved_frontier.add_regime([(date(2026, 9, 10), Regime.BULL_QUIET), (date(2026, 9, 11), None)])
        variants["frontier_date"] = moved_frontier

        rerouted_mask = CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version="q-v2")
        rerouted_mask.add_spans({1: _Span(date(2026, 9, 11), 10), 2: _Span(date(2026, 9, 10), 4)})
        rerouted_mask.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
        rerouted_mask.add_unresolved_breaks({2: (date(2025, 1, 2),)})
        rerouted_mask.add_regime([(date(2026, 9, 10), Regime.BULL_QUIET), (date(2026, 9, 11), None)])
        variants["quarantine_rule_set_version"] = rerouted_mask

        for name, builder in variants.items():
            builder.add_series(1, _series(BARS_A))
            assert builder.finish() != baseline, f"{name} did not reach the digest"

    def test_a_benchmark_date_the_map_lost_rotates_the_stamp(self) -> None:
        """⚠ MEMBERSHIP, not just values. ``for_dates`` reads absence from the
        map as ``not_evaluable`` and a present ``None`` as warm-up, and those
        produce different reason codes — so dropping a date must be visible even
        though its value was already ``None``."""
        dropped = CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version="q-v1")
        dropped.add_spans({1: _Span(date(2026, 9, 11), 10), 2: _Span(date(2026, 9, 10), 4)})
        dropped.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
        dropped.add_unresolved_breaks({2: (date(2025, 1, 2),)})
        dropped.add_regime([(date(2026, 9, 10), Regime.BULL_QUIET)])
        dropped.add_series(1, _series(BARS_A))

        kept = _builder()
        kept.add_series(1, _series(BARS_A))
        assert dropped.finish() != kept.finish()

    def test_the_rule_version_is_in_the_payload(self) -> None:
        """The gap checkpoint 1 found in the parent's first draft: a
        ``..._RULE_VERSION`` constant that rotates nothing."""
        bumped = CorpusGenerationBuilder(
            frontier_date=date(2026, 9, 11),
            quarantine_rule_set_version="q-v1",
            rule_version=f"{CORPUS_GENERATION_RULE_VERSION}-next",
        )
        bumped.add_spans({1: _Span(date(2026, 9, 11), 10), 2: _Span(date(2026, 9, 10), 4)})
        bumped.add_panel_calendars({"s2": frozenset({date(2026, 9, 10), date(2026, 9, 11)})})
        bumped.add_unresolved_breaks({2: (date(2025, 1, 2),)})
        bumped.add_regime([(date(2026, 9, 10), Regime.BULL_QUIET), (date(2026, 9, 11), None)])
        bumped.add_series(1, _series(BARS_A))

        base = _builder()
        base.add_series(1, _series(BARS_A))
        assert bumped.finish() != base.finish()

    def test_instruments_must_arrive_in_ascending_order(self) -> None:
        """The digest streams, so the caller's order IS the digest."""
        builder = _builder()
        builder.add_series(5, _series(BARS_A))
        with pytest.raises(ValueError, match="strictly ascending"):
            builder.add_series(2, _series(BARS_A))

    def test_the_same_instrument_cannot_be_folded_in_twice(self) -> None:
        builder = _builder()
        builder.add_series(1, _series(BARS_A))
        with pytest.raises(ValueError, match="strictly ascending"):
            builder.add_series(1, _series(BARS_A))

    def test_a_missing_component_raises_rather_than_defaulting(self) -> None:
        """A generation computed without the regime would be indistinguishable
        from one computed with it — the one failure this value must not have."""
        builder = CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version="q-v1")
        builder.add_spans({1: _Span(date(2026, 9, 11), 10)})
        builder.add_series(1, _series(BARS_A))
        with pytest.raises(RuntimeError, match=r"missing component\(s\) \['breaks', 'panel_calendars', 'regime'\]"):
            builder.finish()

    def test_finishing_twice_raises(self) -> None:
        builder = _builder()
        builder.add_series(1, _series(BARS_A))
        builder.finish()
        with pytest.raises(RuntimeError, match="already finished"):
            builder.finish()

    def test_a_series_after_finish_raises(self) -> None:
        builder = _builder()
        builder.add_series(1, _series(BARS_A))
        builder.finish()
        with pytest.raises(RuntimeError, match="arrived too late"):
            builder.add_series(9, _series(BARS_A))

    @pytest.mark.parametrize("bad", ["", "q\x1fv1", "q\x1ev1", "q=v1"])
    def test_a_version_string_carrying_a_delimiter_is_refused(self, bad: str) -> None:
        """The payload is delimiter-framed, so an embedded separator could make
        two different payloads serialise identically."""
        with pytest.raises(ValueError, match="delimiters"):
            CorpusGenerationBuilder(frontier_date=date(2026, 9, 11), quarantine_rule_set_version=bad)


class TestWriterMirror:
    """``store_signals``' validation against ``sql/382``'s CHECK.

    ⚠ Here rather than in the DB-tier writer test on purpose: this file is
    pure-logic, and the whole value of mirroring a constraint is that the mirror
    can be exercised without the constraint. The DB tier asserts the other half —
    that a row this accepts is a row Postgres accepts.
    """

    @pytest.mark.parametrize(
        "bad", ["", "0123456789ABCDEF", "0123456789abcde", "0123456789abcdef0", "zzzzzzzzzzzzzzzz"]
    )
    def test_a_malformed_stamp_is_refused(self, bad: str) -> None:
        from app.services.signal_ledger import store_signals

        with pytest.raises(ValueError, match="16 lowercase hex"):
            store_signals(None, [], corpus_generation=bad)  # type: ignore[arg-type]

    def test_the_refusal_fires_on_an_EMPTY_batch_too(self) -> None:
        """⚠ Validation runs BEFORE the empty-batch return. Otherwise a writer
        bug only surfaces on the runs that happen to have rows, which is the
        shape that reads as intermittent."""
        from app.services.signal_ledger import store_signals

        with pytest.raises(ValueError, match="16 lowercase hex"):
            store_signals(None, [], corpus_generation="nope")  # type: ignore[arg-type]

    def test_a_well_formed_stamp_on_an_empty_batch_is_a_no_op(self) -> None:
        from app.services.signal_ledger import store_signals

        assert store_signals(None, [], corpus_generation="0123456789abcdef") == 0  # type: ignore[arg-type]


class TestPanelCalendarGap:
    """⚠ CHECKPOINT 2's P2, pinned. A span summary is not the calendar.

    ``InstrumentBarSpan`` carries ``last_bar`` and ``bars``. A stale instrument
    whose INTERIOR dates move keeps both — and its bars are never folded in,
    because it is not eligible — so before ``add_panel_calendars`` the pass
    produced one identical stamp for two corpora that rebalanced S-2 on
    different dates. Codex reproduced exactly that (02-02 vs 02-03).
    """

    @staticmethod
    def _with_calendar(calendar: frozenset[date]) -> str:
        builder = CorpusGenerationBuilder(frontier_date=date(2026, 2, 4), quarantine_rule_set_version="q-v1")
        # Identical spans in both arms: same last_bar, same bar count. That is
        # what makes this a test of the calendar component and not of the spans.
        builder.add_spans({1: _Span(date(2026, 2, 4), 3), 2: _Span(date(2026, 2, 3), 3)})
        builder.add_panel_calendars({"s2-cross-sectional-momentum": calendar})
        builder.add_unresolved_breaks({})
        builder.add_regime([(date(2026, 2, 4), Regime.BULL_QUIET)])
        builder.add_series(1, _series(BARS_A))
        return builder.finish()

    def test_a_moved_interior_date_rotates_the_stamp(self) -> None:
        earlier = self._with_calendar(frozenset({date(2026, 1, 29), date(2026, 2, 2), date(2026, 2, 3)}))
        later = self._with_calendar(frozenset({date(2026, 1, 28), date(2026, 1, 29), date(2026, 2, 3)}))
        assert earlier != later

    def test_an_empty_calendar_is_recorded_rather_than_omitted(self) -> None:
        """ "No cross-sectional plan" and "calendar not recorded" must stay
        distinguishable — the second is a missing component and raises."""
        assert re.fullmatch(GENERATION_PATTERN, self._with_calendar(frozenset()))
