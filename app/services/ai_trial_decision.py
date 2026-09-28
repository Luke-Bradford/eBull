"""AI-discretionary-v1 decision layer (#3471 slice 1a): schema, strict parse, validation, control draw.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §5 (schema), §6
(validation), §7 (control draw) and obligations O3/O5.

Everything here is pure. The model's output is UNTRUSTED input at a system boundary: it is
parsed strictly, validated against frozen bounds, and REFUSED — never repaired. A repair
would decide which of the model's entries survive, and that decision is not the model's.

⚠ The constants below are by construction (spec §2), not fitted or sourced. They are frozen
into the trial declaration by hash; changing one is a new strategy version, not an edit.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
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

SizeTier = Literal["half", "full"]
Horizon = Literal[5, 10, 20]

WholeRefusal = Literal["no_structured_output", "malformed_response", "over_entry_cap", "no_trade_reason_missing"]
DecisionRefusal = Literal[
    "not_in_shortlist", "duplicate_symbol", "already_held", "target_not_above_stop", "thesis_too_long"
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


@dataclass(frozen=True)
class DecisionVerdict:
    response_position: int
    decision: TrialDecision
    instrument_id: int | None
    reason_code: DecisionRefusal | None

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
    arm_held_instrument_ids: frozenset[int],
    max_new_entries: int,
) -> ValidationResult:
    """§6. ``shortlist`` maps the pack-complete shortlist's symbols to instrument ids.

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
        reason: DecisionRefusal | None
        if instrument_id is None:
            reason = "not_in_shortlist"
        elif symbol_counts[decision.symbol] > 1:
            reason = "duplicate_symbol"
        elif instrument_id in arm_held_instrument_ids:
            reason = "already_held"
        elif decision.target_pct <= decision.stop_pct:
            reason = "target_not_above_stop"
        elif count_sentences(decision.thesis) > THESIS_MAX_SENTENCES:
            reason = "thesis_too_long"
        else:
            reason = None
        verdicts.append(DecisionVerdict(position, decision, instrument_id, reason))
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


def control_pool(
    shortlist_instrument_ids: Sequence[int],
    *,
    control_held_instrument_ids: frozenset[int],
    drawn_this_run: frozenset[int],
) -> tuple[int, ...]:
    """§7 pool: the pack-complete shortlist minus the CONTROL leg's holdings and this run's
    earlier draws (without replacement), ordered by ``instrument_id`` ascending.

    ⚠ The arm's picks are deliberately NOT excluded: a control that draws the arm's own name
    is a legitimate random outcome. Excluding it would benchmark the arm against the
    unchosen remainder rather than against a random pick (ckpt-1 r1-5).
    """
    excluded = control_held_instrument_ids | drawn_this_run
    return tuple(sorted({i for i in shortlist_instrument_ids if i not in excluded}))


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
