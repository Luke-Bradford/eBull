"""AI-discretionary-v1 decision primitives (#3471 slice 1a): bounds, strict parse, the §6 ATR
measurement and exact arithmetic, and the §7 control draw.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §5, §6, §7 and
obligations O3/O5. The v6 schema, per-decision guard and control pre-filter (§16.3, §16.4) live
in ``ai_trial_guard``, which builds on this module, ``ai_trial_levels`` and ``ai_trial_plan``.

Everything here is pure. The model's output is UNTRUSTED input at a system boundary: it is
parsed strictly and REFUSED — never repaired. A repair would decide which of the model's
entries survive, and that decision is not the model's.

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

#: §16.1 stop ceiling in ATR14 (supervisor 2026-09-28 15:45Z, v5; kept by v6), inclusive.
STOP_ATR_MULTIPLE_MAX: Final = Fraction(4)
#: §6 v5: every recorded ATR / multiple / derived level is quantized to this many decimals,
#: half up. ``sql/433`` / ``sql/439`` verify the same grid without dividing.
QUANTUM_DECIMALS: Final = 4

SizeTier = Literal["half", "full"]
Horizon = Literal[5, 10, 20]

WholeRefusal = Literal["no_structured_output", "malformed_response", "over_entry_cap", "no_trade_reason_missing"]


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
