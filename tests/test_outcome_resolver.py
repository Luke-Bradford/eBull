"""Phase 4a — the outcome resolver, as pure logic.

Spec: ``docs/proposals/ta/2026-08-06-outcome-resolver.md``, acceptance 1-11.
(12-14 are full-corpus figures and live in
``scripts/verify_2240_outcome_resolver.py`` — a hand-copied statistic goes
stale in the place a reader trusts most.)

⚠ DB-free by design. Everything here is a walk over a bar array; the repo's
stated default is to extract the decision into a pure function and table-test
it. There is no new SQL mechanism in this module to integration-test.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries
from app.services.outcome_resolver import (
    INHERITED_REASONS,
    OUR_ADDITIONAL_REASONS,
    OUTCOME_CLASSES,
    RULE_SET_VERSION,
    UNRESOLVED_REASONS,
    ExitLevels,
    Outcome,
    resolve_outcome,
)
from app.services.strategy_registry import NOT_EVALUABLE_REASONS
from app.services.technical_analysis import OHLCVRow

_D0 = date(2026, 1, 5)
#: Entry is always ``open[fill_index]``; these bracket it symmetrically enough
#: that a bar has to be built deliberately to touch either side.
_ENTRY = Decimal("100")
_STOP = Decimal("95")
_TARGET = Decimal("110")


def _bar(
    open_: Decimal | None = _ENTRY,
    high: Decimal | None = Decimal("101"),
    low: Decimal | None = Decimal("99"),
    close: Decimal | None = Decimal("100"),
) -> OHLCVRow:
    """One bar. Fields accept ``None`` because the real columns are NULLABLE and
    ``load_masked_series`` deliberately writes ``None`` into ``high``/``low`` for
    a masked-range bar — the runtime type is wider than ``OHLCVRow``'s
    annotation, so the resolver has to cope with it rather than trust it."""
    return {
        "open": open_,  # type: ignore[typeddict-item]
        "high": high,  # type: ignore[typeddict-item]
        "low": low,  # type: ignore[typeddict-item]
        "close": close,  # type: ignore[typeddict-item]
        "volume": 1_000,
    }


def _series(rows: list[OHLCVRow]) -> BarSeries:
    return BarSeries(dates=tuple(_D0 + timedelta(days=i) for i in range(len(rows))), rows=tuple(rows))


def _resolve(
    rows: list[OHLCVRow],
    *,
    fill_index: int = 0,
    max_hold_bars: int = 3,
    masked_bar_reasons: dict[int, str] | None = None,
    segment_end_index: int | None = None,
    stop: Decimal = _STOP,
    target: Decimal | None = _TARGET,
) -> Outcome:
    return resolve_outcome(
        series=_series(rows),
        fill_index=fill_index,
        entry_price=_ENTRY,
        levels=ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold_bars),
        masked_bar_reasons=masked_bar_reasons or {},  # type: ignore[arg-type]
        segment_end_index=segment_end_index,
    )


# ---------------------------------------------------------------------------
# Acceptance 1-3 — the per-bar rules
# ---------------------------------------------------------------------------


def test_target_touched_intrabar_books_tp_at_the_level() -> None:
    out = _resolve([_bar(), _bar(high=Decimal("112"))])
    assert out.outcome == "tp_hit"
    assert out.exit_price == _TARGET  # the LEVEL, not the high
    assert out.exit_index == 1
    assert out.bars_held == 1
    assert out.gross_return_pct == Decimal("0.1")


def test_stop_touched_intrabar_books_sl_at_the_level() -> None:
    out = _resolve([_bar(), _bar(low=Decimal("90"))])
    assert out.outcome == "sl_hit"
    assert out.exit_price == _STOP
    assert out.gross_return_pct == Decimal("-0.05")


def test_one_bar_spanning_both_levels_is_ambiguous() -> None:
    """Acceptance 1. §3.5.4: the order of touch is unknowable from OHLC."""
    out = _resolve([_bar(), _bar(high=Decimal("115"), low=Decimal("90"))])
    assert out.outcome == "ambiguous"
    # Excluded from the win rate WITH its count shown — so it locates itself,
    # but books no price and no return.
    assert out.exit_index == 1
    assert out.exit_bar_date == _D0 + timedelta(days=1)
    assert out.exit_price is None
    assert out.gross_return_pct is None


def test_ambiguity_is_not_resolved_favourably_at_any_tp_distance() -> None:
    """S5's distortion argument: silently resolving favourably manufactures
    edge, and the distortion scales with how tight the target is."""
    for target in (Decimal("101"), Decimal("110"), Decimal("140")):
        # ⚠ The fill bar is deliberately narrower than the tightest target
        # under test — the default one has high 101 and would resolve on bar 0.
        out = _resolve(
            [_bar(high=Decimal("100.5"), low=Decimal("99.5")), _bar(high=Decimal("150"), low=Decimal("50"))],
            target=target,
        )
        assert out.outcome == "ambiguous", target


def test_gap_below_the_stop_resolves_at_the_open_even_when_the_high_clears_the_target() -> None:
    """Acceptance 2. The open is the FIRST price of the bar, so the touch order
    is known and the bar is not ambiguous — and the fill is the open (worse than
    the stop), not the stop."""
    out = _resolve([_bar(), _bar(open_=Decimal("90"), high=Decimal("115"), low=Decimal("88"))])
    assert out.outcome == "sl_hit"
    assert out.exit_price == Decimal("90")  # the OPEN, worse than the 95 stop
    assert out.gross_return_pct == Decimal("-0.1")


def test_gap_above_the_target_resolves_at_the_open_even_when_the_low_breaks_the_stop() -> None:
    out = _resolve([_bar(), _bar(open_=Decimal("120"), high=Decimal("125"), low=Decimal("80"))])
    assert out.outcome == "tp_hit"
    assert out.exit_price == Decimal("120")  # the OPEN, better than the 110 target
    assert out.gross_return_pct == Decimal("0.2")


def test_a_stop_only_bracket_books_sl_at_the_level() -> None:
    """S-7's shape: ``take_profit=None``. The stop side is unchanged."""
    out = _resolve([_bar(), _bar(low=Decimal("90"))], target=None)
    assert out.outcome == "sl_hit"
    assert out.exit_price == _STOP


def test_a_stop_only_bracket_gap_below_the_stop_books_at_the_open() -> None:
    out = _resolve([_bar(), _bar(open_=Decimal("92"), low=Decimal("90"))], target=None)
    assert out.outcome == "sl_hit"
    assert out.exit_price == Decimal("92")  # rule 1: the open, not the level


def test_a_stop_only_bracket_never_books_tp_or_ambiguous() -> None:
    """With one level there is no unknowable touch order: a bar that would have
    spanned BOTH levels of a full bracket is a plain stop touch here, and a
    huge high alone decides nothing."""
    spanning = _resolve([_bar(), _bar(high=Decimal("500"), low=Decimal("90"))], target=None)
    assert spanning.outcome == "sl_hit"
    soaring = _resolve([_bar(), _bar(high=Decimal("500")), _bar()], target=None, max_hold_bars=2)
    assert soaring.outcome == "expired"


def test_a_stop_only_bracket_expires_at_the_next_open() -> None:
    out = _resolve([_bar(), _bar(), _bar(), _bar(open_=Decimal("104"))], target=None)
    assert out.outcome == "expired"
    assert out.exit_price == Decimal("104")


def test_a_stop_only_bracket_still_requires_the_stop_below_the_entry() -> None:
    with pytest.raises(ValueError, match="not below the entry"):
        _resolve([_bar()], target=None, stop=Decimal("100"))


def test_stop_only_levels_still_validate_the_stop() -> None:
    with pytest.raises(ValueError, match="stop_loss must be > 0"):
        ExitLevels(take_profit=None, stop_loss=Decimal("0"), max_hold_bars=3)


def test_the_fill_bar_is_inside_the_window() -> None:
    """Excluding it understates both levels, and is the flattering error for a
    tight stop."""
    out = _resolve([_bar(high=Decimal("112")), _bar()])
    assert out.outcome == "tp_hit"
    assert out.exit_index == 0
    assert out.bars_held == 0  # correct as a bar count; NOT exposure time


# ---------------------------------------------------------------------------
# Acceptance 3-5 — expiry, and what expiry must not absorb
# ---------------------------------------------------------------------------


def test_expiry_fills_at_the_open_after_the_window_not_the_last_close() -> None:
    """Acceptance 3. §3.5.1 binds exits as well as entries, so booking the
    window's last close would be a same-bar fill."""
    rows = [_bar(), _bar(), _bar(close=Decimal("108")), _bar(open_=Decimal("103"))]
    out = _resolve(rows, max_hold_bars=3)
    assert out.outcome == "expired"
    assert out.exit_index == 3
    assert out.exit_price == Decimal("103")  # open[3], NOT close[2] = 108
    assert out.bars_held == 3


def test_a_window_running_off_the_series_is_unresolved_not_expired() -> None:
    """Acceptance 4. Booking a return for a window that never ran is a
    one-directional bias landing on every open trade at the corpus edge."""
    out = _resolve([_bar(), _bar()], max_hold_bars=5)
    assert out.outcome == "unresolved"
    assert out.reason == "window_truncated"
    assert out.exit_price is None


def test_expiry_needs_the_bar_after_the_window_or_it_is_truncated() -> None:
    """The window itself completes; the exit bar does not exist."""
    out = _resolve([_bar(), _bar(), _bar()], max_hold_bars=3)
    assert out.outcome == "unresolved"
    assert out.reason == "window_truncated"


def test_a_level_touched_before_the_truncation_point_resolves_normally() -> None:
    """Acceptance 5. A shorter window cannot un-hit a level already hit."""
    out = _resolve([_bar(), _bar(high=Decimal("112"))], max_hold_bars=20)
    assert out.outcome == "tp_hit"


# ---------------------------------------------------------------------------
# Acceptance 6-8 — masking and segments
# ---------------------------------------------------------------------------


def test_masking_is_per_field_a_masked_range_bar_still_serves_as_an_expiry_exit() -> None:
    """Acceptance 6. ``load_masked_series`` masks high/low and CARRIES the open;
    a whole-bar rejection would have thrown this exit away."""
    rows = [_bar(), _bar(), _bar(open_=Decimal("104"), high=None, low=None)]
    out = _resolve(rows, max_hold_bars=2)
    assert out.outcome == "expired"
    assert out.exit_price == Decimal("104")


def test_a_masked_range_bar_inside_the_window_refuses_the_touch_test() -> None:
    out = _resolve([_bar(), _bar(high=None, low=None), _bar()], max_hold_bars=3)
    assert out.outcome == "unresolved"
    assert out.reason == "missing_bar_data"


def test_a_touch_test_needs_the_open_too_not_only_the_range() -> None:
    """Without the open, rule 1 is indistinguishable from rule 4 (different exit
    PRICE) and from rule 3 (different CLASS)."""
    out = _resolve([_bar(), _bar(open_=None, high=Decimal("112"))], max_hold_bars=3)
    assert out.outcome == "unresolved"
    assert out.reason == "missing_bar_data"


def test_a_declared_reason_beats_the_null_fallback() -> None:
    """Acceptance 7, and criterion 8's whole point: a NULL the quarantine rules
    never looked at is a data gap, and calling it a quarantine verdict collapses
    two classes with different bias implications."""
    out = _resolve(
        [_bar(), _bar(high=None, low=None), _bar()],
        max_hold_bars=3,
        masked_bar_reasons={1: "quarantined_bar"},
    )
    assert out.reason == "quarantined_bar"


def test_a_masked_bar_after_the_decisive_one_changes_nothing() -> None:
    out = _resolve(
        [_bar(), _bar(high=Decimal("112")), _bar(high=None, low=None)],
        max_hold_bars=3,
        masked_bar_reasons={2: "quarantined_bar"},
    )
    assert out.outcome == "tp_hit"
    assert out.exit_index == 1


def test_annotating_a_fully_populated_bar_is_a_no_op() -> None:
    """⚠ The map ANNOTATES; the absent fields refuse. Stated as a test because
    it is the one footgun in the contract: to refuse a bar whose fields are all
    present — a B1/B4 sentinel, a provisional part-session bar — the caller masks
    the field it does not trust."""
    out = _resolve(
        [_bar(), _bar(), _bar(open_=Decimal("104"))],
        max_hold_bars=2,
        masked_bar_reasons={1: "quarantined_bar"},
    )
    assert out.outcome == "expired"
    assert out.exit_price == Decimal("104")


def test_a_window_crossing_the_segment_end_is_unresolved() -> None:
    """Acceptance 8, first half."""
    out = _resolve([_bar(), _bar(), _bar()], max_hold_bars=3, segment_end_index=1)
    assert out.outcome == "unresolved"
    assert out.reason == "series_break"


def test_a_fill_inside_the_next_segment_resolves_normally() -> None:
    """Acceptance 8, second half — and the reason a break is a SEGMENT boundary
    and not a masked bar. ``break_date`` is the bar at the NEW scale, so a trade
    entered on or after it is entirely within that scale."""
    rows = [_bar(), _bar(), _bar(high=Decimal("112")), _bar()]
    out = _resolve(rows, fill_index=1, max_hold_bars=3, segment_end_index=3)
    assert out.outcome == "tp_hit"
    assert out.exit_index == 2


def test_the_expiry_exit_bar_is_also_checked_against_the_segment() -> None:
    out = _resolve([_bar(), _bar(), _bar()], max_hold_bars=2, segment_end_index=1)
    assert out.outcome == "unresolved"
    assert out.reason == "series_break"


# ---------------------------------------------------------------------------
# Acceptance 9-11 — the contract itself
# ---------------------------------------------------------------------------


def test_the_mask_map_and_segment_end_have_no_defaults() -> None:
    """Acceptance 9. #2288: a field with a default is a field a writer can
    forget, and forgetting either produces the phantom fills decision 10 names."""
    series = _series([_bar(), _bar()])
    levels = ExitLevels(take_profit=_TARGET, stop_loss=_STOP, max_hold_bars=1)
    with pytest.raises(TypeError):
        resolve_outcome(series=series, fill_index=0, entry_price=_ENTRY, levels=levels)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        resolve_outcome(  # type: ignore[call-arg]
            series=series, fill_index=0, entry_price=_ENTRY, levels=levels, masked_bar_reasons={}
        )
    with pytest.raises(TypeError):
        resolve_outcome(  # type: ignore[call-arg]
            series=series, fill_index=0, entry_price=_ENTRY, levels=levels, segment_end_index=None
        )


@pytest.mark.parametrize(
    ("stop", "target", "max_hold", "match"),
    [
        (Decimal("95"), Decimal("110"), 0, "max_hold_bars must be >= 1"),
        (Decimal("0"), Decimal("110"), 3, "stop_loss must be > 0"),
        (Decimal("110"), Decimal("95"), 3, "not below take_profit"),
    ],
)
def test_levels_validate_themselves(stop: Decimal, target: Decimal, max_hold: int, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)


@pytest.mark.parametrize(
    ("stop", "target"),
    [(Decimal("100"), Decimal("110")), (Decimal("95"), Decimal("100")), (Decimal("101"), Decimal("102"))],
)
def test_levels_must_bracket_the_entry(stop: Decimal, target: Decimal) -> None:
    """Acceptance 11. A stop at or above entry, or a target at or below it,
    triggers immediately and is not a trade."""
    with pytest.raises(ValueError, match="triggers immediately and is not a trade"):
        _resolve([_bar(), _bar()], stop=stop, target=target)


def test_an_entry_price_disagreeing_with_the_fill_bars_open_raises() -> None:
    """The guard against resolving a stored ledger row against a corpus that has
    since been rebuilt or re-adjusted."""
    with pytest.raises(ValueError, match="disagrees with open"):
        resolve_outcome(
            series=_series([_bar(open_=Decimal("100")), _bar()]),
            fill_index=0,
            entry_price=Decimal("99.5"),
            levels=ExitLevels(take_profit=_TARGET, stop_loss=_STOP, max_hold_bars=1),
            masked_bar_reasons={},
            segment_end_index=None,
        )


def test_a_fill_bar_with_no_open_raises_rather_than_resolving() -> None:
    with pytest.raises(ValueError, match="cannot be a fill bar"):
        resolve_outcome(
            series=_series([_bar(open_=None), _bar()]),
            fill_index=0,
            entry_price=_ENTRY,
            levels=ExitLevels(take_profit=_TARGET, stop_loss=_STOP, max_hold_bars=1),
            masked_bar_reasons={},
            segment_end_index=None,
        )


def test_a_segment_end_before_the_fill_raises() -> None:
    with pytest.raises(ValueError, match="precedes fill_index"):
        _resolve([_bar(), _bar()], fill_index=1, segment_end_index=0)


@pytest.mark.parametrize("fill_index", [-1, 2])
def test_a_fill_index_outside_the_series_raises(fill_index: int) -> None:
    with pytest.raises(ValueError, match="outside the 2-bar series"):
        _resolve([_bar(), _bar()], fill_index=fill_index)


# ---------------------------------------------------------------------------
# Acceptance 10 — the recorded shape
# ---------------------------------------------------------------------------


def test_a_reason_is_required_exactly_when_unresolved() -> None:
    with pytest.raises(ValueError, match="a reason is required exactly when"):
        Outcome(outcome="unresolved", resolution_method="daily_bar", rule_set_version=RULE_SET_VERSION)
    with pytest.raises(ValueError, match="a reason is required exactly when"):
        Outcome(
            outcome="ambiguous",
            resolution_method="daily_bar",
            rule_set_version=RULE_SET_VERSION,
            reason="window_truncated",
            exit_index=1,
            exit_bar_date=_D0,
            bars_held=1,
        )


def test_a_half_populated_exit_location_is_rejected() -> None:
    """⚠ The counted-not-ANDed invariant. ``exit_index is not None and
    exit_bar_date is not None`` reads as "has a location" and silently admits
    half of one — the defect Codex caught at phase 3c's checkpoint 2, now in the
    prevention log."""
    with pytest.raises(ValueError, match="partial exit location"):
        Outcome(
            outcome="ambiguous",
            resolution_method="daily_bar",
            rule_set_version=RULE_SET_VERSION,
            exit_index=1,
            bars_held=1,
        )


def test_a_price_without_a_return_is_rejected() -> None:
    with pytest.raises(ValueError, match="move together"):
        Outcome(
            outcome="expired",
            resolution_method="daily_bar",
            rule_set_version=RULE_SET_VERSION,
            exit_index=1,
            exit_bar_date=_D0,
            bars_held=1,
            exit_price=Decimal("100"),
        )


def test_ambiguous_and_unresolved_never_carry_a_return() -> None:
    with pytest.raises(ValueError, match="move together"):
        Outcome(
            outcome="ambiguous",
            resolution_method="daily_bar",
            rule_set_version=RULE_SET_VERSION,
            exit_index=1,
            exit_bar_date=_D0,
            bars_held=1,
            exit_price=Decimal("100"),
            gross_return_pct=Decimal("0"),
        )


def test_every_outcome_carries_the_rule_set_version() -> None:
    """Criterion 11 makes the execution assumption part of identity, and the two
    constructions this module makes live nowhere else."""
    out = _resolve([_bar(), _bar(high=Decimal("112"))])
    assert out.rule_set_version == RULE_SET_VERSION
    assert RULE_SET_VERSION.startswith("outcome-resolver-v1+")
    assert out.resolution_method == "daily_bar"


# ---------------------------------------------------------------------------
# Vocabulary — closed, countable, and pinned against its neighbours
# ---------------------------------------------------------------------------


def test_the_inherited_reason_codes_match_the_registrys_spelling() -> None:
    """⚠ Criterion 8's vocabulary is countable only if two modules spell it the
    same way. This is the drift guard the closed-vocabulary-in-N-places defect
    (#2218) exists for."""
    assert INHERITED_REASONS <= NOT_EVALUABLE_REASONS
    assert INHERITED_REASONS == {"series_break", "quarantined_bar"}
    assert OUR_ADDITIONAL_REASONS == {
        "window_truncated",
        "missing_bar_data",
        "unorderable_exit_levels",
    }
    assert OUR_ADDITIONAL_REASONS | INHERITED_REASONS == UNRESOLVED_REASONS
    assert not OUR_ADDITIONAL_REASONS & NOT_EVALUABLE_REASONS


def test_the_outcome_vocabulary_is_the_parents_four_plus_ours() -> None:
    assert OUTCOME_CLASSES == {"tp_hit", "sl_hit", "expired", "ambiguous", "unresolved"}
