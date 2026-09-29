"""#3471 spec v6 — the decision schema, the per-decision guard and the control pre-filter (pure).

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §16.3 (schema and
guard), §16.4 (control), §16.11 (the library row as the stated baseline) and obligations
O-v6-1, O-v6-2 and O-v6-6.

The model no longer types a percentage: it names a setup and two level ids, and every figure is
derived here from the pack's own ``levels`` and §6 ATR measurement (``ai_trial_plan``). The
model's output is UNTRUSTED input at a system boundary: parsed strictly, refused, never
repaired (§6).

This module sits above ``ai_trial_decision`` (primitives, strict parse, the §7 draw),
``ai_trial_levels`` and ``ai_trial_plan``, which import ``ai_trial_decision`` themselves; putting
the guard there would make an import cycle.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any, Final, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.services.ai_trial_decision import (
    MAX_ENTRIES_PER_RUN,
    SYMBOL_MAX_CHARS,
    THESIS_MAX_CHARS,
    THESIS_MAX_SENTENCES,
    AtrMeasurement,
    ControlDraw,
    ControlPoolExhausted,
    Horizon,
    SizeTier,
    WholeRefusal,
    count_sentences,
    draw_control,
    measure_atr,
)
from app.services.ai_trial_levels import LEVEL_IDS, SETUP_TYPES, SUPPORT_LEVEL_IDS, TARGET_LEVEL_IDS, Level
from app.services.ai_trial_pack_reader import exact_decimal
from app.services.ai_trial_plan import PlanFigures, PlanRefusal, derive_plan, plan_figures

# ---------------------------------------------------------------------------
# §16.3 schema vocabularies — spelled out for pydantic, pinned to ``ai_trial_levels`` below
# ---------------------------------------------------------------------------
SetupChoice = Literal[
    "breakout_donchian20",
    "pullback_rising_sma20",
    "pullback_rising_sma50",
    "range_support_bounce",
    "trend_continuation_flag",
    "none",
]
InvalidationLevelId = Literal[
    "swing_low_1",
    "swing_low_2",
    "swing_low_3",
    "donchian20_low",
    "donchian55_low",
    "sma20",
    "sma50",
    "sma200",
    "vwap20_proxy",
]
TargetLevelId = Literal[
    "swing_high_1",
    "swing_high_2",
    "swing_high_3",
    "donchian20_high",
    "donchian55_high",
    "range20_projection",
    "mm_up",
]
if (
    get_args(SetupChoice) != (*SETUP_TYPES, "none")
    or get_args(InvalidationLevelId) != SUPPORT_LEVEL_IDS
    or get_args(TargetLevelId) != TARGET_LEVEL_IDS
):
    raise RuntimeError("the §16.3 schema enums must be ai_trial_levels' vocabularies, in order")

#: §16.1 order-6 minimums per half (by construction).
LIBRARY_MIN_PLANS: Final = 100
LIBRARY_MIN_NAMES: Final = 30
LIBRARY_HALVES: Final = ("train", "holdout")
#: §16.5 row contract: counts, then statistics. ``avg_win_net_pct`` / ``avg_loss_net_pct`` are
#: ``null`` over an empty subset (r2-47), so only their presence is required.
_LIBRARY_COUNTS: Final = ("firings", "no_valid_plan", "plans", "n_names", "truncated", "purged", "deduped")
_LIBRARY_STATISTICS: Final = ("pct_stop", "pct_target", "pct_time", "mean_net_pct", "mean_net_r")
_LIBRARY_NULLABLE: Final = ("avg_win_net_pct", "avg_loss_net_pct")
#: §16.11 canonical fraction, as the DB CHECK in ``sql/439`` states it.
_FRACTION: Final = re.compile(r"-?[1-9][0-9]*/[1-9][0-9]*|0/1")

#: §16.11: the whole run refuses before the model call (pack build) or before validation.
LIBRARY_SHA_MISMATCH: Final = "library_sha_mismatch"

#: §16.3 per-decision precedence (orders 1–6, 8–13; order 7 retired in v6.3).
DecisionRefusal = Literal[
    "not_in_shortlist",
    "duplicate_symbol",
    "already_held",
    "no_valid_plan",
    "setup_not_detected",
    "setup_base_rate_missing",
    "level_unavailable",
    "stop_below_horizon_floor",
    "stop_outside_atr_band",
    "reward_risk_below_min",
    "plan_outside_bounds",
    "thesis_too_long",
]


class TrialDecision(BaseModel):
    """One ``enter_long`` decision (§16.3). ``extra='forbid'`` + strict types: no coercion."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    action: Literal["enter_long"]
    # No ``min_length``: the frozen schema has only ``maxLength``, so an empty symbol is a
    # per-decision ``not_in_shortlist``, not a whole-response refusal (ckpt-2).
    symbol: str = Field(max_length=SYMBOL_MAX_CHARS)
    setup_type: SetupChoice
    invalidation_level_id: InvalidationLevelId
    target_level_id: TargetLevelId
    horizon_days: Horizon
    size_tier: SizeTier
    confidence: int = Field(ge=1, le=5)
    thesis: str = Field(min_length=1, max_length=THESIS_MAX_CHARS)


class TrialResponse(BaseModel):
    """The whole structured response (§16.3)."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    decisions: list[TrialDecision] = Field(max_length=MAX_ENTRIES_PER_RUN)
    no_trade_reason: str | None = Field(max_length=THESIS_MAX_CHARS)


def decision_json_schema() -> dict[str, Any]:
    """The JSON Schema handed to ``claude -p --json-schema``.

    Generated from ``TrialResponse`` so the CLI's schema and the Python re-validation are one
    source; the declaration freezes this output's sha256.
    """
    return TrialResponse.model_json_schema()


# ---------------------------------------------------------------------------
# §16.11 library baseline (order 6)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Baseline:
    """The library row's ``mean_net_r`` per half, verbatim: the decision row's baseline."""

    train_mean_net_r: str
    holdout_mean_net_r: str


def is_canonical_fraction(value: object) -> bool:
    """§16.11 shape: ``p/q`` reduced (gcd 1), positive denominator, or ``0/1``."""
    if not isinstance(value, str) or not _FRACTION.fullmatch(value):
        return False
    number = Fraction(value)
    return f"{number.numerator}/{number.denominator}" == value


def _finite(value: object) -> bool:
    """A §16.5 statistic: an exact fraction string or a finite decimal string."""
    if not isinstance(value, str):
        return False
    if "/" in value:
        return is_canonical_fraction(value)
    try:
        return Decimal(value).is_finite()
    except ArithmeticError:
        return False


def _half_usable(half: object) -> bool:
    if not isinstance(half, Mapping):
        return False
    for key in _LIBRARY_COUNTS:
        count = half.get(key)
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            return False
    if half["plans"] < LIBRARY_MIN_PLANS or half["n_names"] < LIBRARY_MIN_NAMES:
        return False
    if not all(_finite(half.get(key)) for key in _LIBRARY_STATISTICS):
        return False
    if not all(key in half and (half[key] is None or _finite(half[key])) for key in _LIBRARY_NULLABLE):
        return False
    return is_canonical_fraction(half["mean_net_r"])


def library_baseline(rows: Sequence[Mapping[str, Any]], setup_type: str, horizon_days: int) -> Baseline | None:
    """Order 6: the (setup, horizon) row's baseline, or ``None`` = ``setup_base_rate_missing``
    (no row, a duplicate row, a missing half or statistic, a non-finite value, or a half below
    the minimums). The retired v6.2 sign rule is not applied (§16.11)."""
    matches = [r for r in rows if r.get("setup_type") == setup_type and r.get("horizon_days") == horizon_days]
    if len(matches) != 1:
        return None
    halves = matches[0].get("halves")
    if not isinstance(halves, Mapping) or not all(_half_usable(halves.get(h)) for h in LIBRARY_HALVES):
        return None
    return Baseline(str(halves["train"]["mean_net_r"]), str(halves["holdout"]["mean_net_r"]))


# ---------------------------------------------------------------------------
# The pack's per-name structure, as the guard reads it
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class NameStructure:
    """One pack-complete name's §6 measurement, §16.2 ``levels`` and detected setups, read from
    the pack alone (in memory or stored), so every recorded figure re-derives from it (O-v6-1)."""

    atr: AtrMeasurement | None
    levels: Mapping[str, Level | None]
    detected: frozenset[str]


def _level(value: object) -> Level | None:
    if not isinstance(value, Mapping):
        return None
    price = value.get("price")
    if isinstance(price, float) or not isinstance(price, Decimal | str):
        return None
    try:
        number = Decimal(price)
    except ArithmeticError:
        return None
    if not number.is_finite() or number <= 0:
        return None
    origin = value.get("origin_bar")
    return Level(Fraction(number), origin if isinstance(origin, int) and not isinstance(origin, bool) else None)


def name_structure(entry: Mapping[str, Any]) -> NameStructure:
    bars = entry.get("bars") or []
    levels = entry.get("levels") or {}
    setups = entry.get("setups") or {}
    return NameStructure(
        atr=measure_atr((entry.get("indicators") or {}).get("atr14"), bars[-1].get("c") if bars else None),
        levels={level_id: _level(levels.get(level_id)) for level_id in LEVEL_IDS},
        detected=frozenset(
            s for s in SETUP_TYPES if isinstance(setups.get(s), Mapping) and setups[s].get("detected") is True
        ),
    )


def pack_structures(names: Sequence[Mapping[str, Any]]) -> dict[int, NameStructure]:
    return {int(entry["instrument_id"]): name_structure(entry) for entry in names}


# ---------------------------------------------------------------------------
# §16.3 validation
# ---------------------------------------------------------------------------
_NO_FIGURES: Final = PlanFigures(None, None, None, None, None)


@dataclass(frozen=True)
class PlanRecord:
    """A leg's recorded plan: its measurement, the two chosen level prices and the §16.3
    figures. Every field is ``None`` when the name is unknown or an input is missing."""

    atr: AtrMeasurement | None
    invalidation_price: Fraction | None
    target_price: Fraction | None
    figures: PlanFigures


def _plan_record(name: NameStructure | None, invalidation_level_id: str, target_level_id: str) -> PlanRecord:
    if name is None:
        return PlanRecord(None, None, None, _NO_FIGURES)
    invalidation, target = name.levels.get(invalidation_level_id), name.levels.get(target_level_id)
    return PlanRecord(
        name.atr,
        None if invalidation is None else invalidation.price,
        None if target is None else target.price,
        plan_figures(name.atr, invalidation, target),
    )


@dataclass(frozen=True)
class DecisionVerdict:
    response_position: int
    decision: TrialDecision
    instrument_id: int | None
    reason_code: DecisionRefusal | None
    #: Recorded whatever check fails first (§16.3).
    plan: PlanRecord
    #: Set exactly when order 6 passed, accepted or refused later (§16.11).
    baseline: Baseline | None

    @property
    def accepted(self) -> bool:
        return self.reason_code is None


@dataclass(frozen=True)
class ValidationResult:
    """Either a whole-response refusal, or one verdict per decision in response order."""

    whole_refusal: WholeRefusal | None
    verdicts: tuple[DecisionVerdict, ...]
    no_trade_reason: str | None = None

    @property
    def accepted(self) -> tuple[DecisionVerdict, ...]:
        return tuple(v for v in self.verdicts if v.accepted)


def _judge(
    decision: TrialDecision,
    *,
    instrument_id: int | None,
    duplicate: bool,
    held: bool,
    name: NameStructure | None,
    library_rows: Sequence[Mapping[str, Any]],
) -> tuple[DecisionRefusal | None, Baseline | None]:
    """Orders 1–13 (§16.3), the first failure named; the baseline once order 6 has passed."""
    if instrument_id is None or name is None:
        return "not_in_shortlist", None
    if duplicate:
        return "duplicate_symbol", None
    if held:
        return "already_held", None
    if decision.setup_type == "none":
        return "no_valid_plan", None
    if decision.setup_type not in name.detected:
        return "setup_not_detected", None
    baseline = library_baseline(library_rows, decision.setup_type, decision.horizon_days)
    if baseline is None:
        return "setup_base_rate_missing", None
    refusal: PlanRefusal | None = derive_plan(
        name.atr,
        name.levels.get(decision.invalidation_level_id),
        name.levels.get(decision.target_level_id),
        horizon_days=decision.horizon_days,
    ).refusal
    if refusal is not None:
        return refusal, baseline
    if count_sentences(decision.thesis) > THESIS_MAX_SENTENCES:
        return "thesis_too_long", baseline
    return None, baseline


def validate_response(
    structured_output: object,
    *,
    shortlist: Mapping[str, int],
    structures: Mapping[int, NameStructure],
    library_rows: Sequence[Mapping[str, Any]],
    arm_held_instrument_ids: frozenset[int],
    max_new_entries: int,
) -> ValidationResult:
    """§6 whole-response refusals, then the §16.3 per-decision precedence.

    ``shortlist`` maps the pack-complete shortlist's symbols to instrument ids and
    ``structures`` holds each one's pack structure (``pack_structures``). ``library_rows`` are
    the declaration-bound library's rows, re-read by the caller — never the pack's copy
    (§16.11). ``max_new_entries`` is ``min(2, arm free slots, control free slots)``.
    """
    if structured_output is None:
        return ValidationResult("no_structured_output", ())
    try:
        response = TrialResponse.model_validate(structured_output)
    except ValidationError:
        return ValidationResult("malformed_response", ())
    if len(response.decisions) > max(0, min(max_new_entries, MAX_ENTRIES_PER_RUN)):
        return ValidationResult("over_entry_cap", ())
    if not response.decisions and not (response.no_trade_reason or "").strip():
        return ValidationResult("no_trade_reason_missing", ())

    symbol_counts: dict[str, int] = {}
    for decision in response.decisions:
        symbol_counts[decision.symbol] = symbol_counts.get(decision.symbol, 0) + 1

    verdicts: list[DecisionVerdict] = []
    for position, decision in enumerate(response.decisions):
        instrument_id = shortlist.get(decision.symbol)
        name = None if instrument_id is None else structures.get(instrument_id)
        reason, baseline = _judge(
            decision,
            instrument_id=instrument_id,
            duplicate=symbol_counts[decision.symbol] > 1,
            held=instrument_id in arm_held_instrument_ids,
            name=name,
            library_rows=library_rows,
        )
        plan = _plan_record(name, decision.invalidation_level_id, decision.target_level_id)
        verdicts.append(DecisionVerdict(position, decision, instrument_id, reason, plan, baseline))
    return ValidationResult(None, tuple(verdicts), response.no_trade_reason)


# ---------------------------------------------------------------------------
# §16.4 control
# ---------------------------------------------------------------------------
def control_plan(verdict: DecisionVerdict, name: NameStructure) -> PlanRecord | None:
    """The arm's setup and level ids applied to ``name``: its record when the setup is detected
    there and the plan passes orders 8–12, else ``None``. Order 6 depends on (setup, horizon)
    only, so it is the arm's and is not re-judged."""
    d = verdict.decision
    if d.setup_type not in name.detected:
        return None
    verdict_plan = derive_plan(
        name.atr,
        name.levels.get(d.invalidation_level_id),
        name.levels.get(d.target_level_id),
        horizon_days=d.horizon_days,
    )
    if not verdict_plan.feasible:
        return None
    return _plan_record(name, d.invalidation_level_id, d.target_level_id)


def control_pool(
    shortlist_instrument_ids: Sequence[int],
    *,
    control_held_instrument_ids: frozenset[int],
    drawn_this_run: frozenset[int],
    feasible_instrument_ids: frozenset[int],
) -> tuple[int, ...]:
    """§16.4 pool, built PER DECISION: the §7 pool (pack-complete shortlist minus the CONTROL
    leg's holdings and this run's earlier draws) restricted to names on which the arm's plan is
    feasible, ordered by ``instrument_id`` ascending.

    ⚠ The arm's picks are deliberately NOT excluded (ckpt-1 r1-5): the arm's own name passes its
    own filter by construction (O-v6-2), so it leaves the pool only through the control leg's
    holdings or an earlier draw.
    """
    excluded = control_held_instrument_ids | drawn_this_run
    return tuple(sorted({i for i in shortlist_instrument_ids if i not in excluded and i in feasible_instrument_ids}))


@dataclass(frozen=True)
class PairPlan:
    pair_seq: int
    draw: ControlDraw
    control: PlanRecord


@dataclass(frozen=True)
class PlannedDecision:
    """One decision row as it will be stored: the validator's verdict, the pairing outcome."""

    verdict: DecisionVerdict
    #: ``verdict.reason_code``, or ``control_pool_exhausted`` for an accepted decision that
    #: could not be paired (§7: every accepted arm entry has a control).
    reason_code: DecisionRefusal | Literal["control_pool_exhausted"] | None
    pair: PairPlan | None


def plan_pairs(
    verdicts: Sequence[DecisionVerdict],
    *,
    shortlist_instrument_ids: Sequence[int],
    structures: Mapping[int, NameStructure],
    control_held_instrument_ids: frozenset[int],
    declaration_sha256_hex: str,
    session_date: date,
    first_pair_seq: int,
) -> tuple[PlannedDecision, ...]:
    """§3 step 5 over one run's verdicts, in ascending ``response_position``.

    Each accepted decision gets its own §16.4 pool, then the §7 draw. An empty pool refuses the
    arm decision ``control_pool_exhausted`` and consumes no ``pair_seq``; the next pool excludes
    only the controls of pairs actually created.
    """
    planned: list[PlannedDecision] = []
    drawn: set[int] = set()
    pair_seq = first_pair_seq
    for verdict in sorted(verdicts, key=lambda v: v.response_position):
        if not verdict.accepted:
            planned.append(PlannedDecision(verdict, verdict.reason_code, None))
            continue
        plans = {
            iid: plan
            for iid in shortlist_instrument_ids
            if iid in structures and (plan := control_plan(verdict, structures[iid])) is not None
        }
        pool = control_pool(
            shortlist_instrument_ids,
            control_held_instrument_ids=control_held_instrument_ids,
            drawn_this_run=frozenset(drawn),
            feasible_instrument_ids=frozenset(plans),
        )
        try:
            draw = draw_control(
                declaration_sha256_hex=declaration_sha256_hex,
                session_date=session_date,
                pair_seq=pair_seq,
                pool=pool,
            )
        except ControlPoolExhausted:
            planned.append(PlannedDecision(verdict, "control_pool_exhausted", None))
            continue
        drawn.add(draw.instrument_id)
        planned.append(PlannedDecision(verdict, None, PairPlan(pair_seq, draw, plans[draw.instrument_id])))
        pair_seq += 1
    return tuple(planned)


def price_decimal(value: Fraction | None) -> Decimal | None:
    """A level or stop price as the Decimal that denotes it exactly, for a ``numeric`` column.
    Every §16.3 price is a finite decimal (stored prices, a quarter of a ``repr`` ATR), so an
    inexact value is a defect and raises — it is never rounded."""
    if value is None:
        return None
    result = exact_decimal(value)
    if result is None:
        raise ValueError(f"price {value} has no exact decimal form")
    return result


__all__ = [
    "LIBRARY_MIN_NAMES",
    "LIBRARY_MIN_PLANS",
    "LIBRARY_SHA_MISMATCH",
    "Baseline",
    "DecisionRefusal",
    "DecisionVerdict",
    "NameStructure",
    "PairPlan",
    "PlanRecord",
    "PlannedDecision",
    "TrialDecision",
    "TrialResponse",
    "ValidationResult",
    "control_plan",
    "control_pool",
    "decision_json_schema",
    "is_canonical_fraction",
    "library_baseline",
    "name_structure",
    "pack_structures",
    "plan_pairs",
    "price_decimal",
    "validate_response",
]
