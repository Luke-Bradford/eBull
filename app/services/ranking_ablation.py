"""#1822 route F: the v1.5 family ablation over stored scores. Pure; no database, no reader.

Spec ``docs/proposals/ta/2026-09-28-1822-ranking-ablation.md`` (v5). This module is slice 1a:
the construction every number of the readout depends on. Slice 1b loads the scores, runs,
prices and termination, freezes the declaration and calls it.

What is here, each rule the spec's:

- **Keys** ("Ablation (construction)"). ``key_full = clip(Σ_g w_g·s_g − P + R)`` and
  ``key_{−f} = clip(Σ_{g≠f} w_g·s_g − P + R)``, float64, families summed in
  :data:`FAMILY_ORDER`, P and R from the row's own ``penalties_json``. **No
  renormalisation**, and ``arm_full`` selects on the rebuilt ``key_full``, never on the
  stored ``total_score``.
- **Row validity** (Route F "Population"). A row with a null rank, a non-finite family
  score, a non-finite ``raw_total`` or an unparseable ``penalties_json`` leaves both keys; the reason is returned so
  the caller counts it.
- **Timeline** (Route F "Timeline"). A run is known at its completion witness; its entry
  session is the first NYSE session whose 09:30 ET open is strictly after that instant; of
  several runs mapping to one entry session the latest known wins (ties: latest
  ``scored_at``) and the others are superseded.
- **Control bar rule** ("Prices", v5). The harness's eligibility rule
  (:func:`hunt_compute.bar_exclusion`), except that a NULL volume passes. ``verbatim`` is the
  sensitivity that applies the harness rule unchanged.
- **``t3_excluded``** ("Prices" 1). Names with a computed T3 verdict dated (by its later bar)
  inside the declared span.
- **Books and Δ** ("Statistics per family f"). ``evaluate_books`` once for ``arm_full`` and
  once per family for ``arm_{−f}``, over one shared control; Δ_f(d) is the paired arm
  difference. A refusal in the full book or the control refuses every family; a refusal in
  ``arm_{−f}`` refuses f only. The grid ends at G_k, so ``book_ruin`` is judged on the
  reported grid, while every admitted path is evaluated through its exit.
- **Statistics**: the HAC mean and SE of Δ_f at a caller-supplied lag (the spec's lag rule
  lives in the readout, not here), ×252 annualised, and MDE = (z_.975 + z_.80) · SE.
  Descriptive: nothing here classifies, passes or fails anything.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from statistics import NormalDist
from typing import Any, Final
from zoneinfo import ZoneInfo

from app.services import market_calendar
from app.services.hunt_compute import bar_exclusion
from app.services.hunt_evaluator import (
    BookSeries,
    Cohort,
    Grid,
    HalfSpread,
    SeriesPrices,
    evaluate_books,
    select_arm,
)
from app.services.hunt_inference import StatRefused, hac_estimate
from app.services.price_quarantine import TransitionVerdict
from app.services.scoring import _clip, family_weights

MODEL_VERSION: Final = "v1.5-balanced"
#: The fixed summation order of the rebuilt keys (spec "Ablation (construction)").
FAMILY_ORDER: Final = ("quality", "value", "turnaround", "momentum", "sentiment", "confidence")
#: Holding period in sessions, top fraction, and formation → entry lag (entry at the open of t + 1).
H: Final = 21
FRACTION: Final = 0.2
LAG: Final = 1
ANNUALISATION: Final = 252
#: Declared, not detected (spec v8 "Declaration"): bump with ANY change to selection,
#: aggregation or statistics code here or in the readout. It is a semantic term of the
#: frozen declaration, so a bump is a new declaration. The commit recorded at freeze and
#: readout is the backstop for a change nobody declared.
CONSTRUCTION_REVISION: Final = 1
#: (z_.975 + z_.80): the normal-approximation planning multiplier (Cohen 1988 ch. 1).
MDE_MULTIPLIER: Final = NormalDist().inv_cdf(0.975) + NormalDist().inv_cdf(0.80)

_NEW_YORK: Final = ZoneInfo("America/New_York")
_NYSE_OPEN: Final = time(9, 30)


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScoreRow:
    """One stored v1.5 row, reduced to what the keys read."""

    instrument_id: int
    families: Mapping[str, float]
    #: P: the sum of every ``kind = 'penalty'`` deduction.
    deductions: float
    #: R: the sum of every ``kind = 'reward'`` addition.
    additions: float


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except TypeError, ValueError:
        return None
    return number if math.isfinite(number) else None


def _penalty_terms(penalties_json: Any) -> tuple[float, float] | None:
    """(P, R) from ``penalties_json``, or None when it does not parse as the writer's shape.

    The writer (``scoring._persist_score``) stores a list of objects, each ``kind`` =
    ``'penalty'`` with ``deduction`` or ``'reward'`` with ``addition``. It always writes a
    list (an empty one when nothing fired), so NULL is a malformed row, never "no terms".
    """
    if isinstance(penalties_json, str):
        try:
            penalties_json = json.loads(penalties_json)
        except ValueError:
            return None
    if not isinstance(penalties_json, list):
        return None
    deductions: list[float] = []
    additions: list[float] = []
    for item in penalties_json:
        if not isinstance(item, Mapping):
            return None
        kind = item.get("kind")
        if kind == "penalty":
            field, bucket = "deduction", deductions
        elif kind == "reward":
            field, bucket = "addition", additions
        else:
            return None
        amount = _finite(item.get(field))
        if amount is None:
            return None
        bucket.append(amount)
    return math.fsum(deductions), math.fsum(additions)


def parse_score_row(
    instrument_id: int,
    *,
    rank: int | None,
    family_scores: Mapping[str, Any],
    raw_total: Any,
    penalties_json: Any,
) -> ScoreRow | str:
    """A valid :class:`ScoreRow`, or the reason the row leaves both keys.

    ``raw_total`` is only validated: the keys are rebuilt from the family scores, but a row
    whose stored total is not finite is excluded (spec "Lane eligibility").
    """
    if rank is None:
        return "null_rank"
    if _finite(raw_total) is None:
        return "non_finite_raw_total"
    families: dict[str, float] = {}
    for family in FAMILY_ORDER:
        value = _finite(family_scores.get(family))
        if value is None:
            return "non_finite_family_score"
        families[family] = value
    terms = _penalty_terms(penalties_json)
    if terms is None:
        return "unparseable_penalties_json"
    return ScoreRow(instrument_id=instrument_id, families=families, deductions=terms[0], additions=terms[1])


def weights() -> Mapping[str, float]:
    found = family_weights(MODEL_VERSION)
    if found is None or set(found) != set(FAMILY_ORDER):
        raise ValueError(f"{MODEL_VERSION} weights do not cover exactly {FAMILY_ORDER}: {found}")
    return found


def pre_clip_total(row: ScoreRow, weight: Mapping[str, float], *, drop: str | None = None) -> float:
    """Σ_{g≠drop} w_g·s_g − P + R, summed in :data:`FAMILY_ORDER` in float64."""
    if drop is not None and drop not in FAMILY_ORDER:
        raise ValueError(f"unknown family {drop!r}")
    total = 0.0
    for family in FAMILY_ORDER:
        if family != drop:
            total += weight[family] * row.families[family]
    return total - row.deductions + row.additions


def rebuilt_key(row: ScoreRow, weight: Mapping[str, float], *, drop: str | None = None) -> float:
    """``key_full`` (``drop=None``) or ``key_{−drop}``: the clipped pre-clip total."""
    return _clip(pre_clip_total(row, weight, drop=drop))


# ---------------------------------------------------------------------------
# Timeline
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Run:
    scored_at: datetime
    #: The completion witness: ``finished_at`` of the covering ``morning_candidate_review``.
    known_at: datetime

    def __post_init__(self) -> None:
        for name in ("scored_at", "known_at"):
            stamp = getattr(self, name)
            if stamp.tzinfo is None or stamp.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")


def session_open(day: date) -> datetime:
    """The NYSE open of a session, 09:30 America/New_York (half days open at the same time)."""
    return datetime.combine(day, _NYSE_OPEN, tzinfo=_NEW_YORK)


def nyse_sessions(start: date, end: date) -> tuple[date, ...]:
    """NYSE sessions in [start, end]; half days are sessions (``hunt_panel.nyse_sessions``' rule)."""
    days = (date.fromordinal(ordinal) for ordinal in range(start.toordinal(), end.toordinal() + 1))
    return tuple(day for day in days if market_calendar.us_market_status(day) != "closed")


def entry_session(known_at: datetime, sessions: Sequence[date]) -> date | None:
    """The first session whose open is strictly after ``known_at``; None past the calendar."""
    for day in sessions:
        if session_open(day) > known_at:
            return day
    return None


@dataclass(frozen=True)
class RunMap:
    #: entry session → the run that forms the cohort entered there.
    entries: Mapping[date, Run]
    superseded: tuple[Run, ...]
    beyond_calendar: tuple[Run, ...]


def map_runs(runs: Iterable[Run], sessions: Sequence[date]) -> RunMap:
    """One run per entry session: the latest known (ties: the latest ``scored_at``)."""
    by_entry: dict[date, list[Run]] = {}
    beyond: list[Run] = []
    for run in runs:
        entry = entry_session(run.known_at, sessions)
        if entry is None:
            beyond.append(run)
        else:
            by_entry.setdefault(entry, []).append(run)
    entries: dict[date, Run] = {}
    superseded: list[Run] = []
    for entry, candidates in sorted(by_entry.items()):
        ordered = sorted(candidates, key=lambda run: (run.known_at, run.scored_at))
        entries[entry] = ordered[-1]
        superseded.extend(ordered[:-1])
    return RunMap(entries=entries, superseded=tuple(superseded), beyond_calendar=tuple(beyond))


# ---------------------------------------------------------------------------
# Control bar rule and t3_excluded
# ---------------------------------------------------------------------------


def _as_float(value: Decimal | float | None) -> float:
    return math.nan if value is None else float(value)


def bar_valid(
    open_: Decimal | float | None,
    high: Decimal | float | None,
    low: Decimal | float | None,
    close: Decimal | float | None,
    volume: Decimal | float | None,
    *,
    verbatim: bool = False,
) -> bool:
    """The control bar rule on the masked bar on t.

    Canonical: the harness rule except that a NULL volume passes; a reported volume must
    still be finite and > 0. ``verbatim``: the harness rule unchanged (NULL fails).
    ⚠ The canonical passes a placeholder volume of 1 for a NULL so that ONLY the volume
    clause is bypassed; every other clause and its order stay :func:`bar_exclusion`'s.
    """
    return bar_reason(open_, high, low, close, volume, verbatim=verbatim) == 0


def bar_reason(
    open_: Decimal | float | None,
    high: Decimal | float | None,
    low: Decimal | float | None,
    close: Decimal | float | None,
    volume: Decimal | float | None,
    *,
    verbatim: bool = False,
) -> int:
    """:func:`bar_exclusion`'s code under the control bar rule (0 = valid): the funnel's reason."""
    reported = _as_float(volume)
    checked = 1.0 if volume is None and not verbatim else reported
    return bar_exclusion(_as_float(open_), _as_float(high), _as_float(low), _as_float(close), checked)


def t3_excluded(verdicts: Mapping[int, Sequence[TransitionVerdict]], *, start: date, end: date) -> frozenset[int]:
    """Names with a T3 verdict whose later bar falls in [start, end] (the look-ahead sensitivity)."""
    return frozenset(
        name
        for name, series in verdicts.items()
        if any("T3" in verdict.rules and start <= verdict.price_date <= end for verdict in series)
    )


# ---------------------------------------------------------------------------
# Cohorts, books and Δ
# ---------------------------------------------------------------------------


def formation_cohort(keys: Mapping[int, float], control: frozenset[int]) -> Cohort | None:
    """A(t) = the top :data:`FRACTION` of ``keys`` restricted to C(t) BEFORE selection.

    None when C(t) is empty (an idle slot). Every name in C(t) must carry a key.
    """
    if not control:
        return None
    missing = control - set(keys)
    if missing:
        raise ValueError(f"control names without a key: {sorted(missing)[:3]}")
    arm = select_arm({name: keys[name] for name in control}, sign=+1, fraction=FRACTION)
    return Cohort(control=control, arm=arm)


def reporting_grid(sessions: Sequence[date], *, first_formation: date, cutoff: date) -> Grid | StatRefused:
    """Formations t with e = t + 1 ≤ G_k = c_k − (h − 1) sessions; returns reported first entry … G_k.

    ``Grid.last`` is G_k, not c_k: ``evaluate_books`` computes book returns on the grid's
    sessions only, so ``book_ruin`` is judged on the reported grid, while
    ``position_path`` still evaluates every admitted path through its exit (≤ c_k).
    """
    index = {day: ordinal for ordinal, day in enumerate(sessions)}
    if first_formation not in index or cutoff not in index:
        raise ValueError("first formation and cutoff must be sessions of the calendar")
    last_reported = index[cutoff] - (H - 1)
    first_entry = index[first_formation] + LAG
    if last_reported < first_entry:
        return StatRefused("empty_grid", f"G_k precedes the first entry ({first_formation}, cutoff {cutoff})")
    formations = tuple(range(index[first_formation], last_reported - LAG + 1))
    return Grid(formations=formations, first=first_entry, last=last_reported)


@dataclass(frozen=True)
class AblationBooks:
    full: BookSeries
    #: family → its ``arm_{−f}`` books, or the refusal that stopped them.
    ablated: Mapping[str, BookSeries | StatRefused]


def ablation_books(
    grid: Grid,
    control: Mapping[int, frozenset[int]],
    keys: Mapping[int, Mapping[str | None, Mapping[int, float]]],
    prices: Mapping[int, SeriesPrices],
    *,
    half_spread: HalfSpread,
    terminal_fractions: Mapping[int, float],
) -> AblationBooks | StatRefused:
    """``arm_full`` and every ``arm_{−f}`` over one shared control.

    ``control[t]`` is C(t); ``keys[t][None]`` the full key and ``keys[t][f]`` ``key_{−f}``,
    per name. A refusal of the full book (arm or control) refuses the whole cell.
    """

    def books(drop: str | None) -> BookSeries | StatRefused:
        cohorts: dict[int, Cohort] = {}
        for t in grid.formations:
            names = control.get(t, frozenset())
            cohort = formation_cohort(keys[t][drop], names) if names else None
            if cohort is not None:
                cohorts[t] = cohort
        if not cohorts:
            return StatRefused("empty_grid", "no formation on the grid has a non-empty control")
        return evaluate_books(
            grid,
            cohorts,
            prices,
            lag=LAG,
            h=H,
            entry_point="open",
            exit_point="close",
            half_spread=half_spread,
            terminal_fractions=terminal_fractions,
            with_dividends=False,
        )

    full = books(None)
    if isinstance(full, StatRefused):
        return full
    return AblationBooks(full=full, ablated={family: books(family) for family in FAMILY_ORDER})


def paired_delta(full: BookSeries, ablated: BookSeries) -> tuple[float, ...]:
    """Δ_f(d) = r_{arm_full}(d) − r_{arm_{−f}}(d) on the same sessions (the control cancels)."""
    if full.sessions != ablated.sessions:
        raise ValueError("the two books must share one grid")
    return tuple(a - b for a, b in zip(full.arm, ablated.arm, strict=True))


@dataclass(frozen=True)
class DeltaStatistics:
    observations: int
    lag: int
    nonzero_sessions: int
    mean_annual: float
    se_annual: float
    t_stat: float
    mde_annual: float


def delta_statistics(delta: Sequence[float], *, lag: int) -> DeltaStatistics | StatRefused:
    """HAC mean/SE of the paired daily Δ series at ``lag``, ×252; MDE = (z_.975 + z_.80) · SE."""
    estimate = hac_estimate(delta, lag)
    if isinstance(estimate, StatRefused):
        return estimate
    se_annual = estimate.standard_error * ANNUALISATION
    return DeltaStatistics(
        observations=estimate.observations,
        lag=estimate.lag,
        nonzero_sessions=sum(1 for value in delta if value != 0.0),
        mean_annual=estimate.mean * ANNUALISATION,
        se_annual=se_annual,
        t_stat=estimate.t_stat,
        mde_annual=MDE_MULTIPLIER * se_annual,
    )


__all__ = [
    "ANNUALISATION",
    "CONSTRUCTION_REVISION",
    "FAMILY_ORDER",
    "FRACTION",
    "H",
    "LAG",
    "MDE_MULTIPLIER",
    "MODEL_VERSION",
    "AblationBooks",
    "DeltaStatistics",
    "Run",
    "RunMap",
    "ScoreRow",
    "ablation_books",
    "bar_reason",
    "bar_valid",
    "delta_statistics",
    "entry_session",
    "formation_cohort",
    "map_runs",
    "nyse_sessions",
    "paired_delta",
    "parse_score_row",
    "pre_clip_total",
    "rebuilt_key",
    "reporting_grid",
    "session_open",
    "t3_excluded",
    "weights",
]
