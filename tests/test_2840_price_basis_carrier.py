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
from pathlib import Path

import pytest

from app.services.strategy_price_basis import (
    CERTIFIED_PRICE_BASES,
    CERTIFYING_ARCHIVE_BASES,
    PRICE_BASIS_RULE_VERSION,
    PriceBasisSeries,
    from_archive_basis,
    from_undeclared_source,
)
from app.services.strategy_registry import StrategyInput, evaluate

_BARS = 200


def _certified(n: int = _BARS) -> PriceBasisSeries:
    return from_archive_basis("unadjusted", n_bars=n)


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
    carrier = from_archive_basis("unadjusted", n_bars=3)
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
        carrier = from_archive_basis(member, n_bars=4)
        assert carrier.certifies_nothing(), f"{member!r} must certify nothing"
        assert carrier.values == (None,) * 4


def test_a_withheld_policy_and_an_unrecognised_token_both_refuse() -> None:
    """``None`` is WITHHOLDING, never eligibility — ``archive_policy_for``'s rule."""
    for basis in (None, "a_basis_from_the_future"):
        assert from_archive_basis(basis, n_bars=4).certifies_nothing()


def test_a_negative_bar_count_is_refused() -> None:
    with pytest.raises(ValueError, match="n_bars must be non-negative"):
        from_archive_basis("unadjusted", n_bars=-1)


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


@pytest.mark.parametrize("n_bars", [0, 1, 200])
def test_an_undeclared_source_certifies_nothing(n_bars: int) -> None:
    """⚠⚠ THE SCAN'S CONSTRUCTOR, and it replaces a ``str | None`` MODULE CONSTANT.

    Once #2840 §6 item 3 removed S-12's universe token,
    ``strategy_signal_scan.SCAN_ARCHIVE_ADJUSTMENT_BASIS`` was the only thing between
    the live scan and a nominal ``>= $100`` gate on back-adjusted ``price_daily``
    levels — and it was one token edit from ``None`` to ``"unadjusted"``. There is no
    token here.
    """
    carrier = from_undeclared_source(n_bars=n_bars)
    assert len(carrier) == n_bars
    assert carrier.certifies_nothing()
    assert carrier.not_evaluable_indices == tuple(range(n_bars))
    assert set(carrier.values) <= {None}


@pytest.mark.parametrize("n_bars", [0, 1, 200])
def test_the_undeclared_source_equals_a_withheld_archive_basis(n_bars: int) -> None:
    """⚠ Pinned so the two cannot drift into disagreeing about the withheld case.

    ``from_undeclared_source`` is the value ``from_archive_basis(None, ...)`` already
    returned; asserting the equality is what keeps a later edit to either one from
    making the scan's carrier quietly different from the backtest's withheld carrier.
    """
    assert from_undeclared_source(n_bars=n_bars) == from_archive_basis(None, n_bars=n_bars)


def test_both_constructors_refuse_a_negative_length() -> None:
    """⚠ The one input that distinguished them before the guard (Codex checkpoint 1).

    ``from_archive_basis`` raised at ``n_bars < 0`` while a naive
    ``from_undeclared_source`` returned an EMPTY carrier — and an empty carrier passes
    ``certifies_nothing()``, so the equality above would have held nowhere useful and
    failed exactly where a length bug lives.
    """
    for build in (from_undeclared_source, lambda *, n_bars: from_archive_basis(None, n_bars=n_bars)):
        with pytest.raises(ValueError, match="n_bars must be non-negative"):
            build(n_bars=-1)
