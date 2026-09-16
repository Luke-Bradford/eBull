"""#3104 slice 7a — the per-leg entry-liquidity observation.

Pure-logic only: the module reads no database, and the acceptance list in
``docs/proposals/ta/2026-09-16-promotion-evidence-capacity.md`` is table tests
over the arithmetic, per the producer proposal's rule that interim slices carry
no end-to-end test.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.strategy_entry_liquidity import (
    ELIGIBLE_ADJUSTMENT_BASES,
    EXCLUSION_PRECEDENCE,
    LOOKBACK_SESSIONS,
    EntryLiquidityMeasurement,
    archive_policy_for,
    bar_index_for,
    causal_close_volume_means,
    summarise,
    window_spans_break,
)

#: Far enough past every fixture date that nothing is provisional unless a test
#: makes it so.
NEVER_PROVISIONAL = date(2100, 1, 1)
START = date(2020, 1, 1)


def _dates(count: int) -> list[date]:
    """Consecutive calendar days. The module indexes SESSIONS, not calendar
    gaps, so a synthetic dense axis exercises the same code path a real sparse
    one does."""
    return [START + timedelta(days=offset) for offset in range(count)]


def _means(
    closes: list[Decimal | float | None],
    volumes: list[Decimal | float | int | None],
    *,
    provisional_from: date = NEVER_PROVISIONAL,
) -> tuple[tuple[float, ...], tuple[str | None, ...]]:
    return causal_close_volume_means(
        dates=_dates(len(closes)),
        closes=closes,
        volumes=volumes,
        provisional_from=provisional_from,
    )


# ---------------------------------------------------------------------------
# The causal boundary — the single rule most likely to be got wrong
# ---------------------------------------------------------------------------


def test_window_includes_the_signal_bar_and_nothing_after_it() -> None:
    """``signal_ledger`` §3.5: signal on the CLOSE of bar t, fill at the OPEN of
    t+1. Bar t has closed, so its volume is known and belongs in the window;
    bar t+1 is the fill and must not reach it."""
    count = LOOKBACK_SESSIONS + 2
    closes: list[Decimal | float | None] = [10.0] * count
    volumes: list[Decimal | float | int | None] = [100] * count
    baseline, _ = _means(closes, volumes)
    signal_index = LOOKBACK_SESSIONS - 1

    spiked_signal = list(volumes)
    spiked_signal[signal_index] = 100_000
    moved, _ = _means(closes, spiked_signal)
    assert moved[signal_index] > baseline[signal_index], "the signal bar's own volume must be in the window"

    spiked_fill = list(volumes)
    spiked_fill[signal_index + 1] = 100_000
    unmoved, _ = _means(closes, spiked_fill)
    assert unmoved[signal_index] == baseline[signal_index], "the fill bar is t+1 and is not knowable at signal time"


def test_the_mean_is_over_exactly_the_lookback() -> None:
    count = LOOKBACK_SESSIONS
    values, reasons = _means([2.0] * count, [3] * count)
    assert reasons[-1] is None
    assert values[-1] == pytest.approx(6.0)


# ---------------------------------------------------------------------------
# Exclusions — a failing session kills the WINDOW, never a TERM
# ---------------------------------------------------------------------------


def test_short_history_excludes_rather_than_part_averaging() -> None:
    count = LOOKBACK_SESSIONS - 1
    _, reasons = _means([10.0] * count, [100] * count)
    assert set(reasons) == {"window_short"}


def test_every_index_before_the_first_full_window_is_short() -> None:
    count = LOOKBACK_SESSIONS + 3
    _, reasons = _means([10.0] * count, [100] * count)
    assert list(reasons[: LOOKBACK_SESSIONS - 1]) == ["window_short"] * (LOOKBACK_SESSIONS - 1)
    assert set(reasons[LOOKBACK_SESSIONS - 1 :]) == {None}


@pytest.mark.parametrize("bad_volume", [0, None, -5])
def test_one_unusable_volume_excludes_the_window(bad_volume: int | None) -> None:
    """⚠ It does NOT average the surviving 19 terms. A mean over 19 of 20 is a
    different estimator, and ``volume_lookback_sessions`` is a declared 20."""
    count = LOOKBACK_SESSIONS
    volumes: list[Decimal | float | int | None] = [100] * count
    volumes[5] = bad_volume
    values, reasons = _means([10.0] * count, volumes)
    assert reasons[-1] == "volume_unusable"
    assert values[-1] == 0.0


@pytest.mark.parametrize("bad_close", [None, 0.0, -1.0, float("nan"), float("inf")])
def test_one_unusable_close_excludes_the_window(bad_close: float | None) -> None:
    count = LOOKBACK_SESSIONS
    closes: list[Decimal | float | None] = [10.0] * count
    closes[3] = bad_close
    _, reasons = _means(closes, [100] * count)
    assert reasons[-1] == "close_unusable"


def test_a_masked_close_is_already_none_and_excludes() -> None:
    """``_apply_arm`` masks a ``return_usable = False`` close to ``None`` under
    the production arm, so the usability test is arm-correct without this module
    knowing about arms. A ``range_usable = False`` bar keeps its close — that is
    ``StructureBar``'s per-field masking and it must stay usable here."""
    count = LOOKBACK_SESSIONS
    closes: list[Decimal | float | None] = [10.0] * count
    closes[0] = None  # what the masked arm does to a bad-close bar
    _, masked_reasons = _means(closes, [100] * count)
    assert masked_reasons[-1] == "close_unusable"

    # A range-only quarantine never touches close/volume, so the same bar with
    # its close intact measures normally.
    _, intact_reasons = _means([10.0] * count, [100] * count)
    assert intact_reasons[-1] is None


def test_a_bad_term_cannot_poison_a_later_clean_window() -> None:
    """Bad terms are zeroed before the windowed sum, so once the offending bar
    leaves the window the estimator recovers exactly."""
    count = LOOKBACK_SESSIONS * 2
    closes: list[Decimal | float | None] = [10.0] * count
    closes[0] = float("nan")
    values, reasons = _means(closes, [100] * count)
    assert reasons[LOOKBACK_SESSIONS - 1] == "close_unusable"
    assert reasons[LOOKBACK_SESSIONS] is None
    assert values[LOOKBACK_SESSIONS] == pytest.approx(1000.0)


def test_provisional_bars_exclude_and_take_precedence_over_close_and_volume() -> None:
    count = LOOKBACK_SESSIONS
    dates = _dates(count)
    closes: list[Decimal | float | None] = [10.0] * count
    closes[1] = None
    volumes: list[Decimal | float | int | None] = [100] * count
    volumes[2] = 0
    _, reasons = causal_close_volume_means(
        dates=dates,
        closes=closes,
        volumes=volumes,
        provisional_from=dates[-1],
    )
    assert reasons[-1] == "provisional_bar", "frozen precedence: provisional outranks close and volume"


def test_provisional_cutoff_comes_from_the_pinned_archive_not_today() -> None:
    """⚠ A diagnostic whose value moves with the wall clock is not
    reproducible, so the cutoff is derived from the archive's PINNED
    ``quarantine_as_of``."""
    policy = archive_policy_for("icyDenev/Intrader")
    assert policy is not None
    assert policy.adjustment_basis == "unadjusted"
    assert policy.eligible
    # 2024-09-27 capture minus the 5-day provisional window.
    assert policy.provisional_from == date(2024, 9, 22)


def test_the_split_adjusted_archive_is_not_eligible() -> None:
    policy = archive_policy_for("paperswithbacktest/Stocks-Daily-Price")
    assert policy is not None
    assert policy.adjustment_basis == "split_adjusted"
    assert not policy.eligible, "sql/305 refuses a split-adjusted level for dollar-volume attribution"


def test_an_unknown_vendor_withholds_rather_than_defaulting() -> None:
    assert archive_policy_for("some/archive-we-have-never-loaded") is None


def test_eligible_bases_is_exactly_unadjusted() -> None:
    """``reconstructed_unadjusted`` is admitted by sql/305 but unreachable —
    the corpus has no point-in-time split factors — so it must not appear."""
    assert ELIGIBLE_ADJUSTMENT_BASES == frozenset({"unadjusted"})


# ---------------------------------------------------------------------------
# Scale breaks
# ---------------------------------------------------------------------------


def test_a_break_inside_the_window_spans_it() -> None:
    dates = _dates(LOOKBACK_SESSIONS + 1)
    index = LOOKBACK_SESSIONS
    inside = dates[index - 3]
    assert window_spans_break(dates=dates, index=index, unresolved_breaks=[inside])


def test_a_break_at_the_windows_first_bar_does_not_span_it() -> None:
    """``price_series_break.break_date`` is the FIRST date at the new scale, so
    a break exactly at the window's opening bar puts the whole window inside one
    segment."""
    dates = _dates(LOOKBACK_SESSIONS + 1)
    index = LOOKBACK_SESSIONS
    first = dates[index - (LOOKBACK_SESSIONS - 1)]
    assert not window_spans_break(dates=dates, index=index, unresolved_breaks=[first])


def test_a_break_after_the_window_does_not_span_it() -> None:
    dates = _dates(LOOKBACK_SESSIONS + 5)
    index = LOOKBACK_SESSIONS - 1
    assert not window_spans_break(dates=dates, index=index, unresolved_breaks=[dates[index + 2]])


def test_no_breaks_never_spans() -> None:
    dates = _dates(LOOKBACK_SESSIONS + 1)
    assert not window_spans_break(dates=dates, index=LOOKBACK_SESSIONS, unresolved_breaks=[])


# ---------------------------------------------------------------------------
# bar_index_for
# ---------------------------------------------------------------------------


def test_bar_index_finds_and_misses_honestly() -> None:
    dates = _dates(5)
    assert bar_index_for(dates, dates[3]) == 3
    assert bar_index_for(dates, date(1999, 1, 1)) is None
    assert bar_index_for(dates, date(2031, 1, 1)) is None


# ---------------------------------------------------------------------------
# The measurement's accounting invariant
# ---------------------------------------------------------------------------


def _measurement(**overrides: object) -> EntryLiquidityMeasurement:
    base: dict[str, object] = {
        "rule_version": "entry-liquidity-v1:test",
        "lookback_sessions": LOOKBACK_SESSIONS,
        "adjustment_basis": "unadjusted",
        "realised_leg_count": 3,
        "measured_leg_count": 2,
        "excluded": {"window_short": 1},
        "unlinked_series_leg_count": 0,
        "minimum": Decimal("1.00"),
        "p05": Decimal("1.00"),
        "p50": Decimal("2.00"),
        "binding_name_key": 7,
        "binding_signal_date": START,
    }
    base.update(overrides)
    return EntryLiquidityMeasurement(**base)  # type: ignore[arg-type]


def test_measured_plus_excluded_must_equal_realised() -> None:
    _measurement()
    with pytest.raises(ValueError, match="every leg carries exactly one verdict"):
        _measurement(excluded={"window_short": 2})


def test_no_measured_legs_forbids_a_summary() -> None:
    with pytest.raises(ValueError, match="no measured legs cannot carry a summary"):
        _measurement(realised_leg_count=3, measured_leg_count=0, excluded={"window_short": 3})


def test_measured_legs_require_a_complete_summary() -> None:
    with pytest.raises(ValueError, match="must carry a complete summary"):
        _measurement(p05=None)


def test_unknown_exclusion_reasons_are_refused() -> None:
    with pytest.raises(ValueError, match="unknown exclusion reasons"):
        _measurement(excluded={"because_i_said_so": 1})


def test_precedence_covers_every_declared_reason() -> None:
    """The frozen order and the Literal must not drift apart — the order IS the
    thing that keeps ``measured + excluded == realised`` exact."""
    assert EXCLUSION_PRECEDENCE[0] == "basis_ineligible"
    assert EXCLUSION_PRECEDENCE[1] == "window_short"
    assert "scale_break_spanned" in EXCLUSION_PRECEDENCE
    assert "scale_break_unknowable" not in EXCLUSION_PRECEDENCE, (
        "an unlinked series is a CAVEAT carried on the measurement, not an exclusion — "
        "excluding it would apply a stricter standard than the positions it describes"
    )
    assert len(set(EXCLUSION_PRECEDENCE)) == len(EXCLUSION_PRECEDENCE)


# ---------------------------------------------------------------------------
# summarise
# ---------------------------------------------------------------------------


def test_summarise_reports_the_binding_leg_and_reconciles() -> None:
    measurement = summarise(
        values=[300.0, 100.0, 200.0],
        name_keys=[1, 2, 3],
        signal_dates=[START, START, START],
        exit_dates=[START, START, START],
        realised_leg_count=5,
        excluded={"window_short": 1, "volume_unusable": 1},
        adjustment_basis="unadjusted",
    )
    assert measurement.measured_leg_count == 3
    assert measurement.minimum == Decimal("100.00")
    assert measurement.binding_name_key == 2
    assert sum(measurement.excluded.values()) + measurement.measured_leg_count == 5


def test_summarise_tie_breaks_deterministically() -> None:
    """⚠ ``argmin`` alone returns the first in corpus-sweep order, which is not
    reproducible. The declared tie-break is the lowest
    ``(name_key, signal_date, exit_date)``."""
    later = START + timedelta(days=1)
    measurement = summarise(
        values=[100.0, 100.0],
        name_keys=[9, 4],
        signal_dates=[START, later],
        exit_dates=[START, later],
        realised_leg_count=2,
        excluded={},
        adjustment_basis="unadjusted",
    )
    assert measurement.binding_name_key == 4


def test_summarise_with_nothing_measured_carries_counts_and_no_summary() -> None:
    measurement = summarise(
        values=[],
        name_keys=[],
        signal_dates=[],
        exit_dates=[],
        realised_leg_count=4,
        excluded={"basis_ineligible": 4},
        adjustment_basis=None,
    )
    assert measurement.measured_leg_count == 0
    assert measurement.minimum is None and measurement.p05 is None and measurement.p50 is None
    assert measurement.excluded == {"basis_ineligible": 4}
    assert measurement.adjustment_basis is None


def test_summarise_drops_zero_counts_but_keeps_the_total_exact() -> None:
    measurement = summarise(
        values=[10.0],
        name_keys=[1],
        signal_dates=[START],
        exit_dates=[START],
        realised_leg_count=2,
        excluded={"window_short": 1, "non_finite": 0},
        adjustment_basis="unadjusted",
    )
    assert measurement.excluded == {"window_short": 1}


def test_summarise_refuses_misaligned_columns() -> None:
    with pytest.raises(ValueError, match="positionally parallel"):
        summarise(
            values=[1.0, 2.0],
            name_keys=[1],
            signal_dates=[START, START],
            exit_dates=[START, START],
            realised_leg_count=2,
            excluded={},
            adjustment_basis="unadjusted",
        )


def test_causal_means_refuses_misaligned_inputs() -> None:
    with pytest.raises(ValueError, match="positionally parallel"):
        causal_close_volume_means(
            dates=_dates(3),
            closes=[1.0, 2.0],
            volumes=[1, 2, 3],
            provisional_from=NEVER_PROVISIONAL,
        )
