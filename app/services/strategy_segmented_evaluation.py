"""Strategy evaluation with indicator state isolated by price-scale segment."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import date

from app.services.indicator_series import BarSeries, Universe
from app.services.market_regime import RegimeSeries
from app.services.price_segments import series_segment_bounds
from app.services.strategy_manifest import StrategyEntry
from app.services.strategy_registry import (
    NotEvaluableReason,
    SignalKind,
    StagedMember,
    StrategySignal,
    stage_cross_sectional_member,
)


def segmented_signals(
    entry: StrategyEntry,
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    unresolved_breaks: Sequence[date],
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """Evaluate every per-series leg with fresh state inside each segment.

    ⚠⚠ THE REGIME IS SLICED WITH THE SEGMENT, NOT PASSED WHOLE. Each segment is
    a fresh ``BarSeries`` indexed from zero, so a full-length regime handed to a
    segment starting at bar 400 would align bar 0 of the segment with bar 0 of
    the market — every regime verdict off by the segment offset, and a strategy
    gated on it would fire in the wrong conditions with no error anywhere.

    S-6 validates ``len(regime) == len(series)`` and would raise on most
    mismatches, which is why that check exists — but a segment whose length
    happens to match a prefix of the regime would pass the check and still be
    wrong. Slicing here is the fix; the length check is the backstop.
    """
    if entry.signals is None:
        raise ValueError(f"{entry.strategy_id} has no per-series signal function")
    if len(regime) != len(series):
        raise ValueError(f"regime has {len(regime)} bars against {len(series)} price bars; they must align")
    signals: list[StrategySignal] = []
    for start, end in series_segment_bounds(series, unresolved_breaks=unresolved_breaks):
        segment = BarSeries(dates=series.dates[start:end], rows=series.rows[start:end])
        # ⚠ `regime.segment(...)`, NOT `RegimeSeries(values=regime.values[start:end])`.
        # The latter type-checks and silently drops `not_evaluable_indices`, so
        # every benchmark hole inside the segment would be re-counted as the
        # benchmark's own warm-up (#2437). The remap lives on the data.
        segment_regime = regime.segment(start, end)
        signals.extend(
            StrategySignal(
                verdict=signal.verdict,
                signal_index=signal.signal_index + start,
                kind=signal.kind,
                reason=signal.reason,
            )
            for signal in entry.signals(segment, universe=universe, masked_reason=masked_reason, regime=segment_regime)
        )
    per_kind = Counter(signal.kind for signal in signals)
    if not per_kind or any(count != len(series) for count in per_kind.values()):
        raise RuntimeError(
            f"{entry.strategy_id} produced segmented verdict counts {dict(per_kind)} for {len(series)} bars"
        )
    return signals


def segmented_member(
    entry: StrategyEntry,
    series: BarSeries,
    *,
    panel_decision_dates: frozenset[date],
    universe: Universe,
    masked_reason: NotEvaluableReason,
    unresolved_breaks: Sequence[date],
    regime: RegimeSeries,
    leg: SignalKind = "entry",
) -> StagedMember:
    """Stage one ranked member with fresh state inside each scale segment.

    ``leg`` selects which member function stages — ``"entry"`` is
    ``entry.member``, ``"exit"`` is ``entry.exit_leg.member`` — and is also
    the ``kind`` stamped on every verdict, so a leg cannot be staged under
    the other leg's name. The regime is SLICED with the segment, exactly as
    ``segmented_signals`` does and for its stated reason.

    ⚠ ``admissible_dates`` / ``mandatory_dates`` merge by UNION across
    segments — they are DATE-keyed on ``StagedMember`` precisely so this
    merge needs no index remapping. ``None`` (unrefined) survives only when
    EVERY segment returned ``None``; the same member function produces the
    same shape per segment, so a mix is a bug and raises.
    """
    if leg == "entry":
        member_stager = entry.member
    elif entry.exit_leg is not None:
        member_stager = entry.exit_leg.member
    else:
        member_stager = None
    if member_stager is None:
        raise ValueError(f"{entry.strategy_id} has no cross-sectional member function for the {leg!r} leg")
    if len(regime) != len(series):
        raise ValueError(f"regime has {len(regime)} bars against {len(series)} price bars; they must align")
    verdicts: list[StrategySignal | None] = []
    scores: dict[date, float] = {}
    admissible: set[date] | None = None
    mandatory: set[date] | None = None
    segments = 0
    for start, end in series_segment_bounds(series, unresolved_breaks=unresolved_breaks):
        segments += 1
        segment = BarSeries(dates=series.dates[start:end], rows=series.rows[start:end])
        staged = stage_cross_sectional_member(
            member_stager(
                segment,
                panel_decision_dates=panel_decision_dates,
                universe=universe,
                masked_reason=masked_reason,
                regime=regime.segment(start, end),
            ),
            kind=leg,
        )
        verdicts.extend(
            None
            if verdict is None
            else StrategySignal(
                verdict=verdict.verdict,
                signal_index=verdict.signal_index + start,
                kind=verdict.kind,
                reason=verdict.reason,
            )
            for verdict in staged.verdicts
        )
        overlap = scores.keys() & staged.scores.keys()
        if overlap:  # pragma: no cover - BarSeries dates and segments are disjoint
            raise RuntimeError(f"{entry.strategy_id} produced duplicate segmented score dates {sorted(overlap)}")
        scores.update(staged.scores)
        for label, merged, this_segment in (
            ("admissible_dates", admissible, staged.admissible_dates),
            ("mandatory_dates", mandatory, staged.mandatory_dates),
        ):
            if segments > 1 and (merged is None) != (this_segment is None):
                raise RuntimeError(
                    f"{entry.strategy_id} {leg} leg produced {label}={'None' if this_segment is None else 'a set'} "
                    "in one segment and the opposite in another — one member function must be one shape"
                )
        if staged.admissible_dates is not None:
            admissible = (admissible or set()) | staged.admissible_dates
        if staged.mandatory_dates is not None:
            mandatory = (mandatory or set()) | staged.mandatory_dates
    if len(verdicts) != len(series):
        raise RuntimeError(f"{entry.strategy_id} staged {len(verdicts)} segmented bars for {len(series)} inputs")
    return StagedMember(
        verdicts=tuple(verdicts),
        scores=scores,
        admissible_dates=None if admissible is None else frozenset(admissible),
        mandatory_dates=None if mandatory is None else frozenset(mandatory),
    )


__all__ = ["segmented_member", "segmented_signals"]
