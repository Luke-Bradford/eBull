"""#2840 — the per-series price-basis carrier.

Pure logic, no DB. What these pin, in order of how much they matter:

1. the carrier satisfies ``EvaluableSeries`` **by being used**, not by an
   ``isinstance`` — the protocol is structural, so only a real ``evaluate`` run
   proves it;
2. the two fail-opens Codex checkpoint 1 found (an unmarked ``None``, and
   ``"unknown"`` as a carried value) are refused at construction;
3. the refusal is **bar-local**, which is the direct contrast with masking's
   measured-terminal behaviour (`3c6bb73f` §3.3: 80 refusals from one bar).
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from app.services.indicator_series import BarSeries
from app.services.technical_analysis import OHLCVRow
from app.services.strategy_price_basis import (
    CERTIFIED_PRICE_BASES,
    CERTIFYING_ARCHIVE_BASES,
    PRICE_BASIS_RULE_VERSION,
    PriceBasisSeries,
    bind_bar,
    bindings_for,
    from_archive_basis,
    from_undeclared_source,
)
from app.services.strategy_registry import StrategyInput, evaluate

_BARS = 200
_START = date(2024, 1, 1)


def _series(n: int = _BARS, *, first_close: str = "100") -> BarSeries:
    """``n`` ascending bars. ⚠ Dates step by TWO days, not one: ``BarSeries``
    allows calendar gaps and never interpolates, and a two-day step leaves room to
    move a single date by one day without breaking the ascending invariant — which
    is how the binding's date sensitivity is tested."""
    close = Decimal(first_close)
    return BarSeries(
        dates=tuple(_START + timedelta(days=2 * index) for index in range(n)),
        rows=tuple(
            {
                "open": close + index,
                "high": close + index + 1,
                "low": close + index - 1,
                "close": close + index,
                "volume": 1_000 + index,
            }
            for index in range(n)
        ),
    )


def _certified(n: int = _BARS) -> PriceBasisSeries:
    return from_archive_basis("unadjusted", series=_series(n))


# ------------------------------------------------------------------ the protocol


def test_the_carrier_is_an_evaluable_series_because_evaluate_accepts_it() -> None:
    """⚠ Asserted by USE, never by ``isinstance``.

    ``EvaluableSeries`` is a structural ``Protocol`` (``strategy_registry.py:336``)
    and ``StrategyInput`` is not runtime-checked, so an ``isinstance`` assertion
    would pass on an object ``evaluate`` cannot actually read. Running the real
    ``evaluate`` over it is the only check that touches ``values``,
    ``not_evaluable_indices`` and ``__len__`` the way production does.
    """
    signals = evaluate(
        lambda index: True,
        inputs=(StrategyInput(series=_certified(), reason="missing_market_context"),),
        n_bars=_BARS,
    )
    assert len(signals) == _BARS
    # Every bar fires except the last, which has no t+1 to fill at.
    assert [s.verdict for s in signals[:-1]] == ["fired"] * (_BARS - 1)
    assert (signals[-1].verdict, signals[-1].reason) == ("not_evaluable", "no_fill_bar")


def test_an_uncertified_bar_is_refused_with_the_declared_reason() -> None:
    carrier = PriceBasisSeries(
        values=tuple(None if index == 120 else "observed_unadjusted" for index in range(_BARS)),
        not_evaluable_indices=(120,),
        bar_bindings=bindings_for(_series()),
    )
    signals = evaluate(
        lambda index: True,
        inputs=(StrategyInput(series=carrier, reason="missing_market_context"),),
        n_bars=_BARS,
    )
    assert (signals[120].verdict, signals[120].reason) == ("not_evaluable", "missing_market_context")


def test_the_refusal_is_bar_local_unlike_masking() -> None:
    """⚠⚠ THE POINT OF THE WHOLE DESIGN, stated as the contrast it was chosen over.

    ``3c6bb73f`` §3.3 measured the alternative: one masked close at index 120 of
    200 makes ``atr_series`` ``None`` for 120…199 — **80** refusals from one bar,
    recovering never, because Wilder smoothing is recursive. The carrier does not
    enter that recursion, so one uncertified bar costs exactly one verdict.

    ⚠ This is a statement about VERDICTS. The uncertified bar's OHLC still feeds
    every later indicator; scale continuity is ``price_segments``' obligation and
    stays there. See the module docstring of ``strategy_price_basis``.
    """
    carrier = PriceBasisSeries(
        values=tuple(None if index == 120 else "observed_unadjusted" for index in range(_BARS)),
        not_evaluable_indices=(120,),
        bar_bindings=bindings_for(_series()),
    )
    signals = evaluate(
        lambda index: True,
        inputs=(StrategyInput(series=carrier, reason="missing_market_context"),),
        n_bars=_BARS,
    )
    refused = [s.signal_index for s in signals if s.reason == "missing_market_context"]
    assert refused == [120], "one uncertified bar must refuse one bar, not every bar after it"


# ------------------------------------------------------------------ the invariants


def test_an_unmarked_none_is_refused_at_construction() -> None:
    """The first of the two checkpoint-1 fail-opens.

    ``RegimeSeries`` permits this and is right to — there it means warm-up. Here
    it would reach ``_unevaluable_reason_at``'s warm-up branch and be stored as
    ``insufficient_warmup``: a provenance gap wearing an indicator's reason code.
    """
    with pytest.raises(ValueError, match="price provenance has no warm-up state"):
        PriceBasisSeries(values=("observed_unadjusted", None))


def test_unknown_cannot_be_carried_as_a_value() -> None:
    """The second, and the sharper one.

    ``"unknown"`` is a member of ``AsTradedPriceBasis`` and ``evaluate`` tests only
    ``is None``, so a carrier of ``"unknown"`` values would evaluate EVERY bar —
    a fail-open wearing the vocabulary this module reuses.
    """
    assert "unknown" not in CERTIFIED_PRICE_BASES
    with pytest.raises(ValueError, match="not one of"):
        PriceBasisSeries(values=("unknown", "observed_unadjusted"))  # type: ignore[arg-type]


def test_an_off_vocabulary_value_is_refused_at_runtime() -> None:
    """``Literal`` enforces nothing at runtime — ``StrategySignal.__post_init__``'s point."""
    with pytest.raises(ValueError, match="not one of"):
        PriceBasisSeries(values=("as_traded_probably",))  # type: ignore[arg-type]


def test_a_marked_index_carrying_a_value_is_refused() -> None:
    with pytest.raises(ValueError, match="a certified bar is not an uncertified one"):
        PriceBasisSeries(values=("observed_unadjusted",), not_evaluable_indices=(0,))


@pytest.mark.parametrize(
    ("indices", "message"),
    [
        ((5,), "outside a series of 2 bars"),
        ((-1,), "outside a series of 2 bars"),
        ((0, 0), "repeats index 0"),
        ((1, 0), "is not sorted"),
    ],
)
def test_malformed_indices_are_refused(indices: tuple[int, ...], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        PriceBasisSeries(values=(None, None), not_evaluable_indices=indices)


# ------------------------------------------------------------------ segmentation


def test_segment_remaps_the_indices_and_a_raw_slice_does_not() -> None:
    """``RegimeSeries.segment``'s test shape, because it is the same bug.

    A raw slice type-checks and silently drops ``not_evaluable_indices``. Here it
    ALSO raises, because the dropped indices leave unmarked ``None`` values — so
    the stricter invariant turns a silent miscount into a loud failure. Both
    halves are asserted.
    """
    carrier = PriceBasisSeries(
        values=tuple(None if index in (2, 7) else "observed_unadjusted" for index in range(10)),
        not_evaluable_indices=(2, 7),
        bar_bindings=bindings_for(_series(10)),
    )
    segment = carrier.segment(5, 10)
    assert segment.not_evaluable_indices == (2,)
    assert segment.values[2] is None
    assert segment.rule_set_version == carrier.rule_set_version, "a segment is the same rule over fewer bars"

    with pytest.raises(ValueError, match="price provenance has no warm-up state"):
        PriceBasisSeries(values=carrier.values[5:10])


def test_segment_refuses_bounds_outside_the_series() -> None:
    with pytest.raises(ValueError, match="is not inside a series"):
        _certified(10).segment(0, 11)


# ------------------------------------------------------------------ the archive source


def test_the_certifying_basis_is_the_only_one_that_certifies() -> None:
    assert CERTIFYING_ARCHIVE_BASES == frozenset({"unadjusted"})
    carrier = from_archive_basis("unadjusted", series=_series(3))
    assert carrier.values == ("observed_unadjusted",) * 3
    assert carrier.not_evaluable_indices == ()
    assert not carrier.certifies_nothing()


def test_every_other_sql_249_member_refuses_every_bar() -> None:
    """⚠ The members are PARSED FROM ``sql/249``, not read from this module's own
    constant — a Codex checkpoint-1 finding, and the reason is that a test reading
    the constant it is checking cannot detect a fifth member added only in SQL.
    """
    migration = Path("sql/249_research_price_corpus.sql").read_text()
    block = re.search(r"adjustment_basis TEXT NOT NULL\s*CHECK \(adjustment_basis IN\s*\(([^)]*)\)", migration)
    assert block is not None, "sql/249's adjustment_basis CHECK moved; this test must follow it"
    stored = {member.strip().strip("'") for member in block.group(1).split(",")}
    assert stored == {"unadjusted", "split_adjusted", "split_and_dividend_adjusted", "unknown"}, (
        f"sql/249 now declares {sorted(stored)}; a new member must be classified deliberately"
    )

    for member in stored - CERTIFYING_ARCHIVE_BASES:
        carrier = from_archive_basis(member, series=_series(4))
        assert carrier.certifies_nothing(), f"{member!r} must certify nothing"
        assert carrier.values == (None,) * 4


def test_a_withheld_policy_and_an_unrecognised_token_both_refuse() -> None:
    """``None`` is WITHHOLDING, never eligibility — ``archive_policy_for``'s rule."""
    for basis in (None, "a_basis_from_the_future"):
        assert from_archive_basis(basis, series=_series(4)).certifies_nothing()


def test_an_empty_series_builds_an_empty_carrier() -> None:
    """⚠ Replaces ``test_a_negative_bar_count_is_refused``, and the guard it
    replaces took nothing with it: ``n_bars < 0`` was refused because an ``int``
    can be negative, and a ``BarSeries`` cannot be. The class is now
    unconstructible rather than rejected.

    ⚠ An EMPTY carrier still passes ``certifies_nothing()`` vacuously, which is
    why that property is never asserted alone anywhere in this suite.
    """
    for build in (
        lambda series: from_archive_basis("unadjusted", series=series),
        lambda series: from_archive_basis(None, series=series),
        lambda series: from_undeclared_source(series=series),
    ):
        carrier = build(_series(0))
        assert len(carrier) == 0
        assert carrier.bar_bindings == ()


# ------------------------------------------------------------------ versioning


def test_the_version_names_its_dependency_and_not_just_itself() -> None:
    """⚠ COMPOSED, not a bare constant (checkpoint 1).

    The rule's verdict depends on ``RESEARCH_ARCHIVES``, which it does not own, so
    a re-declared vendor basis must move the version even though this module's
    bytes did not.
    """
    assert PRICE_BASIS_RULE_VERSION.startswith("price-basis-carrier-v1+")
    assert "+archives-" in PRICE_BASIS_RULE_VERSION


def test_s12_is_the_only_consumer_while_the_rule_stays_out_of_input_rule_sets() -> None:
    """⚠⚠ THE BOUND ON §7's EXCEPTION, enforced rather than promised.

    ``PRICE_BASIS_RULE_VERSION`` lives in ``S12_PARAMS`` and NOT in
    ``strategy_registry.INPUT_RULE_SETS``, because that mapping is hashed into
    every ``StrategyIdentity`` and installing it would rotate all 11 strategies.
    That is only safe while there is exactly one consumer — ``INPUT_RULE_SETS``
    exists because author-maintained per-strategy coverage drifts.

    So this test is the drift detector: a SECOND strategy importing this module is
    the moment the exception stops holding and the sixth entry has to be installed
    (#2840 §6 item 3).
    """
    importers = sorted(
        path.name
        for path in Path("app/services/strategies").glob("s*.py")
        if "strategy_price_basis" in path.read_text()
    )
    assert importers == ["s12_cheapest_band_price_gated_breakout.py"], (
        f"{importers} now read the price-basis rule; a second consumer means INPUT_RULE_SETS must carry "
        "PRICE_BASIS_RULE_VERSION, which rotates every strategy identity — see the spec's §7"
    )


# ------------------------------------------------- the undeclared-source constructor


@pytest.mark.parametrize("n_bars", [0, 1, 2, 200])
def test_an_undeclared_source_certifies_nothing(n_bars: int) -> None:
    """⚠⚠ THE SCAN'S CONSTRUCTOR, and it replaces a ``str | None`` MODULE CONSTANT.

    Once #2840 §6 item 3 removed S-12's universe token,
    ``strategy_signal_scan.SCAN_ARCHIVE_ADJUSTMENT_BASIS`` was the only thing between
    the live scan and a nominal ``>= $100`` gate on back-adjusted ``price_daily``
    levels — and it was one token edit from ``None`` to ``"unadjusted"``. There is no
    token here.
    """
    carrier = from_undeclared_source(series=_series(n_bars))
    assert len(carrier) == n_bars
    assert carrier.certifies_nothing()
    assert carrier.not_evaluable_indices == tuple(range(n_bars))
    assert set(carrier.values) <= {None}
    # ⚠ The scan's carrier binds NOTHING, deliberately: a carrier that certifies
    # no bar cannot certify a foreign one, so the hot path pays no binding cost.
    assert carrier.bar_bindings == ()


@pytest.mark.parametrize("n_bars", [0, 1, 2, 200])
def test_the_undeclared_source_equals_a_withheld_archive_basis(n_bars: int) -> None:
    """⚠ Pinned so the two cannot drift into disagreeing about the withheld case.

    ``from_undeclared_source`` is the value ``from_archive_basis(None, ...)`` already
    returned; asserting the equality is what keeps a later edit to either one from
    making the scan's carrier quietly different from the backtest's withheld carrier.
    """
    series = _series(n_bars)
    assert from_undeclared_source(series=series) == from_archive_basis(None, series=series)


# ------------------------------------------------------------------ the binding (#2840 §8b)


@pytest.mark.parametrize("n_bars", [0, 1, 2, 175])
@pytest.mark.parametrize(
    "basis", [*sorted(CERTIFYING_ARCHIVE_BASES), None, "split_adjusted", "a_basis_from_the_future"]
)
def test_the_constructors_observable_output_is_unchanged_by_the_binding(basis: str | None, n_bars: int) -> None:
    """⚠⚠ THE OTHER HALF OF THE FULL-POPULATION ARGUMENT, and it needs no corpus.

    The sweep in ``scripts/ab_2840_carrier_binding.py`` shows the guard never fires
    on production data. That only implies "verdicts unchanged" if the constructors'
    OBSERVABLE output — ``values`` and ``not_evaluable_indices`` — is also unchanged,
    because those are what ``evaluate`` reads. They are a pure function of
    ``(adjustment_basis, n)``, so this pins them exhaustively over every
    ``sql/249`` member, the withheld case and an unrecognised token.
    """
    carrier = from_archive_basis(basis, series=_series(n_bars))
    certifying = basis in CERTIFYING_ARCHIVE_BASES
    assert carrier.values == (("observed_unadjusted",) if certifying else (None,)) * n_bars
    assert carrier.not_evaluable_indices == ((), tuple(range(n_bars)))[not certifying]
    assert (carrier.bar_bindings != ()) == (certifying and n_bars > 0)


def test_a_certified_carrier_refuses_a_foreign_series_of_the_same_length() -> None:
    """⚠⚠ THE DEFECT, closed. Measured before the fix on a carrier built for
    series B and handed to same-length series A: ``s12_signals`` accepted it and
    S-12 FIRED — ``Counter({'not_evaluable': 114, 'not_fired': 60, 'fired': 1})``.

    Three variants, because the key must distinguish all three: a changed price, a
    changed date, and both.
    """
    mine = _series(20)
    carrier = from_archive_basis("unadjusted", series=mine)
    assert carrier.binding_mismatch(mine) is None

    changed_price = BarSeries(
        dates=mine.dates, rows=(*mine.rows[:7], {**mine.rows[7], "close": Decimal("9")}, *mine.rows[8:])
    )
    shifted_dates = BarSeries(dates=tuple(day + timedelta(days=365) for day in mine.dates), rows=mine.rows)
    both = BarSeries(dates=shifted_dates.dates, rows=changed_price.rows)
    for foreign in (changed_price, shifted_dates, both):
        assert len(foreign) == len(mine)
        mismatch = carrier.binding_mismatch(foreign)
        assert mismatch is not None and "built for different bars" in mismatch


def test_the_binding_reports_the_LOWEST_differing_index() -> None:
    """⚠ Deterministic diagnostic: a date differing at 10 and a price at 2 must
    report 2, not whichever component the implementation happened to compare first."""
    mine = _series(20)
    carrier = from_archive_basis("unadjusted", series=mine)
    rows = list(mine.rows)
    rows[2] = {**rows[2], "close": Decimal("9")}
    dates = list(mine.dates)
    dates[10] = dates[10] + timedelta(days=1)
    mismatch = carrier.binding_mismatch(BarSeries(dates=tuple(dates), rows=tuple(rows)))
    assert mismatch is not None and "index 2 is bound" in mismatch


@pytest.mark.parametrize(
    ("scale", "other"),
    [("1.50", "1.5"), ("100", "100.0")],
)
def test_a_RESTATED_value_at_another_scale_is_a_mismatch(scale: str, other: str) -> None:
    """⚠⚠ WHY THE ENCODING IS ``repr`` AND NOT A TUPLE OF THE RAW VALUES.
    Measured: ``(Decimal("1.50"),) == (Decimal("1.5"),)`` is ``True`` — a tuple
    snapshot collapses scale, so a re-stated value would bind silently.
    """
    row: OHLCVRow = {
        "open": Decimal(scale),
        "high": Decimal(scale),
        "low": Decimal(scale),
        "close": Decimal(scale),
        "volume": 1,
    }
    mine = BarSeries(dates=(_START,), rows=(row,))
    restated = BarSeries(dates=(_START,), rows=(cast(OHLCVRow, {**row, "close": Decimal(other)}),))
    carrier = from_archive_basis("unadjusted", series=mine)
    assert carrier.binding_mismatch(restated) is not None


def test_a_float_and_a_decimal_of_the_same_value_do_not_bind() -> None:
    """⚠ Measured: ``(Decimal("1.5"),) == (1.5,)`` is ``True``. ``repr`` separates
    them, so a fixture that silently changed type cannot pass as the same bar."""
    assert bind_bar(_START, cast(OHLCVRow, {"close": Decimal("1.5")})) != bind_bar(
        _START, cast(OHLCVRow, {"close": 1.5})
    )


def test_two_independently_built_NaNs_bind_and_do_not_depend_on_object_sharing() -> None:
    """⚠⚠ THE SHARP ONE. Tuple comparison takes an identity shortcut, so a SHARED
    ``Decimal("NaN")`` compared equal while two independently built ones did not —
    acceptance would have depended on whether the loader happened to share an
    object. And ``hash((Decimal("sNaN"),))`` raises. ``repr`` is deterministic and
    always hashable.
    """
    row: OHLCVRow = {
        "open": Decimal("NaN"),
        "high": Decimal("1"),
        "low": Decimal("1"),
        "close": Decimal("1"),
        "volume": 1,
    }
    mine = BarSeries(dates=(_START,), rows=(row,))
    rebuilt = BarSeries(dates=(_START,), rows=(cast(OHLCVRow, {**row, "open": Decimal("NaN")}),))
    carrier = from_archive_basis("unadjusted", series=mine)
    assert carrier.binding_mismatch(rebuilt) is None
    assert hash(carrier.bar_bindings) is not None


def test_a_missing_ohlc_key_binds_rather_than_raising() -> None:
    """⚠ ``row.get``, never ``row[…]``. S-12 tolerates a missing key through its own
    ``.get()`` consumers, so indexing would raise ``KeyError`` on a bar the strategy
    currently evaluates. A missing field encodes as a masked one does."""
    partial = BarSeries(dates=(_START,), rows=(cast(OHLCVRow, {"close": Decimal("1")}),))
    carrier = from_archive_basis("unadjusted", series=partial)
    assert carrier.binding_mismatch(partial) is None
    assert bind_bar(_START, cast(OHLCVRow, {"close": Decimal("1")})) == bind_bar(
        _START,
        cast(OHLCVRow, {"open": None, "high": None, "low": None, "close": Decimal("1"), "volume": None}),
    )


def test_a_certification_without_bindings_is_refused_at_construction() -> None:
    """⚠⚠ THE INVARIANT, one-directional. A certified value REQUIRES bindings; the
    converse is not required, because slicing the uncertified suffix of a mixed
    carrier legitimately leaves bindings with nothing certified."""
    with pytest.raises(ValueError, match="certifies a bar but carries no bar_bindings"):
        PriceBasisSeries(values=("observed_unadjusted",))
    # The converse is legal.
    PriceBasisSeries(values=(None,), not_evaluable_indices=(0,), bar_bindings=bindings_for(_series(1)))


def test_bindings_of_the_wrong_length_are_refused() -> None:
    with pytest.raises(ValueError, match="a binding covers every bar of the carrier or none"):
        PriceBasisSeries(values=("observed_unadjusted",) * 3, bar_bindings=bindings_for(_series(2)))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"values": ["observed_unadjusted"]}, "values must be a tuple"),
        ({"values": (None,), "not_evaluable_indices": [0]}, "not_evaluable_indices must be a tuple"),
        ({"values": (None,), "not_evaluable_indices": (0,), "bar_bindings": ["x"]}, "bar_bindings must be a tuple"),
        ({"values": ("observed_unadjusted",), "bar_bindings": (7,)}, "bar_bindings\\[0\\] must be a str"),
    ],
)
def test_a_mutable_or_mistyped_field_is_refused_at_runtime(kwargs: dict[str, object], message: str) -> None:
    """⚠⚠ REPRODUCED AT CHECKPOINT 1, and it is why these checks are runtime.
    ``frozen=True`` freezes the ATTRIBUTE, never a mutable object bound to it.
    Build an all-uncertified carrier from LISTS with no bindings, then append a
    certification and drop its refusal index: the ``certifies_nothing`` shortcut
    accepts it and S-12 fires on an unbound certificate.
    """
    with pytest.raises(TypeError, match=message):
        PriceBasisSeries(**kwargs)  # type: ignore[arg-type]


def test_a_segment_slices_its_bindings_and_still_binds_the_segment_bars() -> None:
    """⚠ Nested and non-zero-offset slices, because ``segment`` remaps indices and a
    binding that were not sliced with them would bind the wrong bars."""
    series = _series(30)
    carrier = from_archive_basis("unadjusted", series=series)
    outer = carrier.segment(5, 25)
    inner = outer.segment(3, 10)
    assert outer.binding_mismatch(BarSeries(dates=series.dates[5:25], rows=series.rows[5:25])) is None
    assert inner.binding_mismatch(BarSeries(dates=series.dates[8:15], rows=series.rows[8:15])) is None
    # ⚠ And it must REJECT the parent's own bars at the wrong offset.
    assert inner.binding_mismatch(BarSeries(dates=series.dates[0:7], rows=series.rows[0:7])) is not None


def test_a_carrier_that_binds_nothing_accepts_any_series_of_its_length() -> None:
    """⚠ Stated rather than hidden: an all-uncertified carrier is unbound ON PURPOSE.
    It refuses every bar whatever series it came from, and the one length-dependent
    difference — ``no_fill_bar`` at the last index — is covered by the caller's
    length check, which runs first."""
    carrier = from_undeclared_source(series=_series(10))
    assert carrier.binding_mismatch(_series(10, first_close="500")) is None
    assert carrier.binding_mismatch(_series(11)) is not None
