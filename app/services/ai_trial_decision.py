"""AI-discretionary-v1 decision layer (#3471 slice 1a): schema, strict parse, validation, control draw.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §5 (schema), §6
(validation, v5 ATR band), §7 (control draw, v5 derived control levels) and obligations O3/O5.

Everything here is pure. The model's output is UNTRUSTED input at a system boundary: it is
parsed strictly, validated against frozen bounds, and REFUSED — never repaired. A repair
would decide which of the model's entries survive, and that decision is not the model's.

⚠ The constants below are by construction (spec §2), not fitted or sourced. They are frozen
into the trial declaration by hash; changing one is a new strategy version, not an edit.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

#: §5 — frozen bounds. ``MAX_ENTRIES_PER_RUN`` is also the schema's ``maxItems``.
MAX_ENTRIES_PER_RUN: Final = 2
STOP_PCT_MIN: Final = 2.0
STOP_PCT_MAX: Final = 25.0
TARGET_PCT_MIN: Final = 2.0
TARGET_PCT_MAX: Final = 100.0
HORIZON_SESSIONS: Final = (5, 10, 20)
THESIS_MAX_CHARS: Final = 600
THESIS_MAX_SENTENCES: Final = 3
SYMBOL_MAX_CHARS: Final = 16

#: §6 v5 ATR band and reward/risk floor (supervisor rule, #3471 2026-09-28 15:45Z), inclusive.
STOP_ATR_MULTIPLE_MIN: Final = Fraction(1)
STOP_ATR_MULTIPLE_MAX: Final = Fraction(4)
R_MULTIPLE_MIN: Final = Fraction(3, 2)
#: §6 v5: every recorded ATR / multiple / derived level is quantized to this many decimals,
#: half up. ``sql/433`` verifies the same grid without dividing.
QUANTUM_DECIMALS: Final = 4

SizeTier = Literal["half", "full"]
Horizon = Literal[5, 10, 20]

WholeRefusal = Literal["no_structured_output", "malformed_response", "over_entry_cap", "no_trade_reason_missing"]
#: §6 precedence order (v5). ``target_not_above_stop`` stays in ``sql/432``'s vocabulary but
#: is no longer emitted: every such decision now refuses at the ATR band or the R floor.
DecisionRefusal = Literal[
    "not_in_shortlist",
    "duplicate_symbol",
    "already_held",
    "stop_outside_atr_band",
    "reward_risk_below_min",
    "thesis_too_long",
]


class TrialDecision(BaseModel):
    """One ``enter_long`` decision (§5). ``extra='forbid'`` + strict types: no coercion."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    action: Literal["enter_long"]
    # No ``min_length``: the frozen §5 schema has only ``maxLength``, so an empty symbol is a
    # per-decision ``not_in_shortlist``, not a whole-response refusal (ckpt-2).
    symbol: str = Field(max_length=SYMBOL_MAX_CHARS)
    stop_pct: float = Field(ge=STOP_PCT_MIN, le=STOP_PCT_MAX, allow_inf_nan=False)
    target_pct: float = Field(ge=TARGET_PCT_MIN, le=TARGET_PCT_MAX, allow_inf_nan=False)
    horizon_days: Horizon
    size_tier: SizeTier
    confidence: int = Field(ge=1, le=5)
    thesis: str = Field(min_length=1, max_length=THESIS_MAX_CHARS)


class TrialResponse(BaseModel):
    """The whole structured response (§5)."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    decisions: list[TrialDecision] = Field(max_length=MAX_ENTRIES_PER_RUN)
    no_trade_reason: str | None = Field(max_length=THESIS_MAX_CHARS)


def decision_json_schema() -> dict[str, Any]:
    """The JSON Schema handed to ``claude -p --json-schema``.

    Generated from ``TrialResponse`` so the CLI's schema and the Python re-validation are
    one source; the declaration freezes this output's sha256.
    """
    return TrialResponse.model_json_schema()


class StrictJSONError(ValueError):
    """The text is not strict JSON (duplicate key or a non-finite constant)."""


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise StrictJSONError(f"duplicate key {key!r}")
        out[key] = value
    return out


def _reject_constant(name: str) -> Any:
    raise StrictJSONError(f"non-finite constant {name}")


def strict_json_loads(text: str) -> Any:
    """``json.loads`` that refuses duplicate keys and ``NaN``/``Infinity`` (O5).

    Stdlib ``json`` silently keeps the LAST duplicate key and accepts ``NaN``; either would let
    a response say two things and have us pick one.
    """
    try:
        return json.loads(text, object_pairs_hook=_reject_duplicates, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as exc:
        # ``JSONDecodeError`` is a ``ValueError``; so are the decoder's int-digit limit and our own
        # hooks' refusals. ``RecursionError`` is deep nesting. All are "not strict JSON" (ckpt-2).
        if isinstance(exc, StrictJSONError):
            raise
        raise StrictJSONError(f"{type(exc).__name__}: {exc}"[:500]) from exc


# A sentence ends at ``.``, ``!`` or ``?`` followed by whitespace or end of text (O3). A decimal
# point ("3.5%") is followed by a digit, so it never ends a sentence.
_SENTENCE_END = re.compile(r"[.!?]+(?=\s|$)")


def count_sentences(text: str) -> int:
    stripped = text.strip()
    if not stripped:
        return 0
    ends = len(_SENTENCE_END.findall(stripped))
    # Trailing text with no terminator is still a sentence.
    return ends + (0 if _SENTENCE_END.search(stripped[-1:] + " ") else 1)


def exact(value: float | Decimal) -> Fraction:
    """The exact rational a stored value denotes (§6 v5): a float by its shortest round-trip
    form (``repr``), which is what Postgres ``float8::text::numeric`` reads back."""
    return Fraction(Decimal(repr(value)) if isinstance(value, float) else value)


def quantize(value: Fraction) -> Decimal:
    """§6 v5 ``q``: 4 decimals, half up (``floor(x·10⁴ + ½) / 10⁴``) — exact, never via a float.
    Only positive values reach it."""
    scaled = math.floor(value * 10**QUANTUM_DECIMALS + Fraction(1, 2))
    return Decimal(scaled).scaleb(-QUANTUM_DECIMALS)


@dataclass(frozen=True)
class AtrMeasurement:
    """One name's §6 ATR measurement from the pack's own masked series. ``atr14`` is in price
    units; ``atr14_pct = q(100·atr14/close)``."""

    atr14: Decimal
    close: Decimal
    atr14_pct: Decimal


def _as_decimal(value: float | Decimal | str | None) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    number = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
    return number if number.is_finite() else None


def measure_atr(atr14: float | Decimal | str | None, close: float | Decimal | str | None) -> AtrMeasurement | None:
    """The §6 measurement, or ``None`` when it is invalid: either input missing, non-finite or
    ≤ 0, or ``atr14_pct`` quantizing to 0 (a flat or near-flat history). A pack carries
    ``close`` as a Decimal (or its canonical-JSON string) and ``atr14`` as a float."""
    atr, px = _as_decimal(atr14), _as_decimal(close)
    if atr is None or px is None or atr <= 0 or px <= 0:
        return None
    atr14_pct = quantize(100 * Fraction(atr) / Fraction(px))
    if atr14_pct <= 0:
        return None
    return AtrMeasurement(atr, px, atr14_pct)


def pack_atr_measurements(names: Sequence[Mapping[str, Any]]) -> dict[int, AtrMeasurement | None]:
    """§6 v5 measurement per pack-complete name: its ``indicators.atr14`` and the close of its
    latest bar — the same masked series the ATR was computed on (``pack_name_entry``)."""
    out: dict[int, AtrMeasurement | None] = {}
    for entry in names:
        bars = entry.get("bars") or []
        out[int(entry["instrument_id"])] = measure_atr(
            (entry.get("indicators") or {}).get("atr14"), bars[-1].get("c") if bars else None
        )
    return out


@dataclass(frozen=True)
class DecisionMetrics:
    """§6 v5 recorded figures: ``r_multiple`` always; the ATR fields only for a shortlist name
    with a valid measurement (``NULL`` otherwise)."""

    r_multiple: Decimal
    atr: AtrMeasurement | None
    stop_atr_multiple: Decimal | None


def decision_metrics(stop_pct: float, target_pct: float, atr: AtrMeasurement | None) -> DecisionMetrics:
    r_multiple = quantize(exact(target_pct) / exact(stop_pct))
    if atr is None:
        return DecisionMetrics(r_multiple, None, None)
    return DecisionMetrics(r_multiple, atr, quantize(exact(stop_pct) / Fraction(atr.atr14_pct)))


@dataclass(frozen=True)
class DecisionVerdict:
    response_position: int
    decision: TrialDecision
    instrument_id: int | None
    reason_code: DecisionRefusal | None
    metrics: DecisionMetrics

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


def validate_response(
    structured_output: object,
    *,
    shortlist: Mapping[str, int],
    atr_by_instrument: Mapping[int, AtrMeasurement | None],
    arm_held_instrument_ids: frozenset[int],
    max_new_entries: int,
) -> ValidationResult:
    """§6. ``shortlist`` maps the pack-complete shortlist's symbols to instrument ids;
    ``atr_by_instrument`` holds each shortlist name's §6 measurement (``None`` = invalid).

    Whole-response refusals first (schema, entry cap, missing no-trade reason), then per-decision
    refusals in the §6 precedence order; the first failing check names the reason.
    ``max_new_entries`` is ``min(2, arm free slots, control free slots)`` — computed by the
    caller from open + pending + uncertain positions, and also shown to the model.
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
        # Recorded independently of which check fails first (§6 v5).
        metrics = decision_metrics(
            decision.stop_pct,
            decision.target_pct,
            None if instrument_id is None else atr_by_instrument.get(instrument_id),
        )
        reason: DecisionRefusal | None
        if instrument_id is None:
            reason = "not_in_shortlist"
        elif symbol_counts[decision.symbol] > 1:
            reason = "duplicate_symbol"
        elif instrument_id in arm_held_instrument_ids:
            reason = "already_held"
        elif metrics.stop_atr_multiple is None or not (
            STOP_ATR_MULTIPLE_MIN <= Fraction(metrics.stop_atr_multiple) <= STOP_ATR_MULTIPLE_MAX
        ):
            reason = "stop_outside_atr_band"
        elif Fraction(metrics.r_multiple) < R_MULTIPLE_MIN:
            reason = "reward_risk_below_min"
        elif count_sentences(decision.thesis) > THESIS_MAX_SENTENCES:
            reason = "thesis_too_long"
        else:
            reason = None
        verdicts.append(DecisionVerdict(position, decision, instrument_id, reason, metrics))
    return ValidationResult(None, tuple(verdicts), response.no_trade_reason)


class ControlPoolExhausted(ValueError):
    """No eligible control name remains (§7) — the arm decision is refused with it."""


@dataclass(frozen=True)
class ControlDraw:
    seed_material: str
    pool: tuple[int, ...]
    index: int

    @property
    def instrument_id(self) -> int:
        return self.pool[self.index]


@dataclass(frozen=True)
class ControlLevels:
    """§7 v5: a control name's measurement and its derived, never chosen, levels."""

    atr: AtrMeasurement
    stop_pct: Decimal
    target_pct: Decimal


def derive_control_levels(metrics: DecisionMetrics, control_atr: AtrMeasurement | None) -> ControlLevels | None:
    """§7 v5: ``stop = q(stop_atr_multiple × control atr14_pct)``, ``target = q(r_multiple ×
    stop)`` from the ARM row's recorded multiples; ``None`` when not placeable — an invalid
    measurement, or derived levels outside the §5 bounds, the ATR band (``atr ≤ stop ≤ 4·atr``)
    or the R floor (``target ≥ 1.5·stop``), each judged on the stored quantized values.

    The arm's own name gets no special case: quantization can move its levels, or make it
    infeasible at a boundary (spec §7, reported as a selection effect).
    """
    if control_atr is None or metrics.stop_atr_multiple is None:
        return None
    stop = quantize(Fraction(metrics.stop_atr_multiple) * Fraction(control_atr.atr14_pct))
    target = quantize(Fraction(metrics.r_multiple) * Fraction(stop))
    atr_pct, s, t = Fraction(control_atr.atr14_pct), Fraction(stop), Fraction(target)
    placeable = (
        exact(STOP_PCT_MIN) <= s <= exact(STOP_PCT_MAX)
        and exact(TARGET_PCT_MIN) <= t <= exact(TARGET_PCT_MAX)
        and STOP_ATR_MULTIPLE_MIN * atr_pct <= s <= STOP_ATR_MULTIPLE_MAX * atr_pct
        and t >= R_MULTIPLE_MIN * s
    )
    return ControlLevels(control_atr, stop, target) if placeable else None


def control_pool(
    shortlist_instrument_ids: Sequence[int],
    *,
    control_held_instrument_ids: frozenset[int],
    drawn_this_run: frozenset[int],
    placeable_instrument_ids: frozenset[int],
) -> tuple[int, ...]:
    """§7 pool, built PER DECISION: the pack-complete shortlist minus the CONTROL leg's
    holdings, this run's earlier draws (without replacement) and (v5) names whose derived
    levels are not placeable for THIS decision's multiples (``derive_control_levels``), ordered
    by ``instrument_id`` ascending.

    ⚠ The arm's picks are deliberately NOT excluded: a control that draws the arm's own name
    is a legitimate random outcome. Excluding it would benchmark the arm against the
    unchosen remainder rather than against a random pick (ckpt-1 r1-5).
    """
    excluded = control_held_instrument_ids | drawn_this_run
    return tuple(sorted({i for i in shortlist_instrument_ids if i not in excluded and i in placeable_instrument_ids}))


def draw_control(
    *,
    declaration_sha256_hex: str,
    session_date: date,
    pair_seq: int,
    pool: tuple[int, ...],
) -> ControlDraw:
    """§7 draw: ``int.from_bytes(sha256(m), "big") mod len(pool)``.

    ``m`` is ``UTF-8("{declaration_sha256_hex}|{session_date ISO}|{pair_seq}")``. No library
    RNG, so the draw is reproducible from the stored material in any runtime. Modulo bias is
    below ``len(pool) / 2**256`` — negligible, not zero, and stated in the spec.
    """
    if not re.fullmatch(r"[0-9a-f]{64}", declaration_sha256_hex):
        raise ValueError("declaration_sha256_hex must be 64 lowercase hex characters")
    if pair_seq < 0:
        raise ValueError("pair_seq must be non-negative")
    if not pool:
        raise ControlPoolExhausted("control pool is empty")
    seed_material = f"{declaration_sha256_hex}|{session_date.isoformat()}|{pair_seq}"
    digest = hashlib.sha256(seed_material.encode("utf-8")).digest()
    return ControlDraw(seed_material, pool, int.from_bytes(digest, "big") % len(pool))


@dataclass(frozen=True)
class PairPlan:
    pair_seq: int
    draw: ControlDraw
    control: ControlLevels


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
    atr_by_instrument: Mapping[int, AtrMeasurement | None],
    control_held_instrument_ids: frozenset[int],
    declaration_sha256_hex: str,
    session_date: date,
    first_pair_seq: int,
) -> tuple[PlannedDecision, ...]:
    """§3 step 5 over one run's verdicts, in ascending ``response_position``.

    Each accepted decision gets a pool built from ITS OWN multiples (§7 v5), then the draw. An
    empty pool refuses the arm decision ``control_pool_exhausted`` and consumes no ``pair_seq``;
    the next pool excludes only the controls of pairs actually created.
    """
    planned: list[PlannedDecision] = []
    drawn: set[int] = set()
    pair_seq = first_pair_seq
    for verdict in sorted(verdicts, key=lambda v: v.response_position):
        if not verdict.accepted:
            planned.append(PlannedDecision(verdict, verdict.reason_code, None))
            continue
        levels = {iid: derive_control_levels(verdict.metrics, atr) for iid, atr in atr_by_instrument.items()}
        pool = control_pool(
            shortlist_instrument_ids,
            control_held_instrument_ids=control_held_instrument_ids,
            drawn_this_run=frozenset(drawn),
            placeable_instrument_ids=frozenset(iid for iid, lv in levels.items() if lv is not None),
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
        control = levels[draw.instrument_id]
        if control is None:  # the pool admits placeable names only; never strip this under -O
            raise RuntimeError(f"drawn control {draw.instrument_id} has no placeable levels")
        drawn.add(draw.instrument_id)
        planned.append(PlannedDecision(verdict, None, PairPlan(pair_seq, draw, control)))
        pair_seq += 1
    return tuple(planned)
