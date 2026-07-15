"""Pure structured diff between two consecutive thesis versions (#2013).

``theses`` is append-only and versioned by insert (settled-decisions "Thesis
semantics"), so the diff of version N vs N-1 is a pure function of two
immutable rows — computed on read, never stored. Callers pair rows via an
explicit ``(instrument_id, thesis_version - 1)`` join; a missing predecessor
means "no diff", never an error.

Null semantics follow #2007: a NULL target is an *abstention*, so null↔value
transitions are first-class ``added``/``removed`` events, never numeric moves
from/to zero.

The materiality predicate lives ONLY here (single source — the alert feed,
the library summary and the pane all call this module; SQL only pairs rows).
Full-population verification (dev 2026-07-15, all 43 N>1 pairs): the
predicate marks 22 pairs material — identical to the 22 with ANY field
change; all 21 observed numeric moves are ≥5% (median 50%), so the threshold
loses nothing today and only guards future small-jitter regens (#2010).

Pure: no DB, no I/O, no imports from ``thesis`` (no cycle).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

# A numeric target/zone move below this relative magnitude is not material
# on its own (full-pop verified: zero current events fall under it).
_MATERIAL_REL_MOVE = 0.05

# Diffed value fields, in render order. Keep in sync with the theses columns
# consumed by _validate_writer_output (app/services/thesis.py).
_TARGET_FIELDS: tuple[str, ...] = (
    "buy_zone_low",
    "buy_zone_high",
    "base_value",
    "bull_value",
    "bear_value",
)

# Compact display labels for summary strings.
_FIELD_LABELS: dict[str, str] = {
    "buy_zone_low": "zone low",
    "buy_zone_high": "zone high",
    "base_value": "base",
    "bull_value": "bull",
    "bear_value": "bear",
}

# Memo bodies with no ### heading fall into one pseudo-section so heading-less
# memos (15/325 on dev) still diff as a unit.
_PREAMBLE_SECTION = "(body)"

_HEADING_RE = re.compile(r"^###\s+(.*\S)\s*$")


@dataclass(frozen=True)
class FieldChange:
    """One changed enum/string field: stance, thesis_type, prompt_version, model."""

    from_value: str | None
    to_value: str | None


@dataclass(frozen=True)
class ConfidenceChange:
    from_value: float | None
    to_value: float | None
    delta: float | None  # None when either side is None


@dataclass(frozen=True)
class TargetChange:
    """One changed numeric target/zone field.

    ``kind``: 'added' (null→value), 'removed' (value→null), 'moved'
    (value→different value). ``rel_move`` = |new−old|/|old|; None unless
    kind == 'moved' with a nonzero old value.
    """

    field: str
    from_value: float | None
    to_value: float | None
    kind: str
    rel_move: float | None


@dataclass(frozen=True)
class ThesisDiff:
    prev_version: int
    curr_version: int
    stance: FieldChange | None
    thesis_type: FieldChange | None
    confidence: ConfidenceChange | None
    targets: tuple[TargetChange, ...]
    break_conditions_added: tuple[str, ...]
    break_conditions_removed: tuple[str, ...]
    memo_sections_added: tuple[str, ...]
    memo_sections_removed: tuple[str, ...]
    memo_sections_changed: tuple[str, ...]
    prompt_version: FieldChange | None = None
    model: FieldChange | None = None
    material: bool = False
    summary: str = ""


def _num(value: object) -> float | None:
    """Nullable DB numeric (Decimal | float | int | None) → float | None."""
    if value is None:
        return None
    return float(value)  # type: ignore[arg-type]


def _str_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _fmt(value: float) -> str:
    """Compact number for summary strings: trim trailing zeros."""
    return f"{value:g}"


def _target_changes(prev: Mapping[str, object], curr: Mapping[str, object]) -> tuple[TargetChange, ...]:
    changes: list[TargetChange] = []
    for name in _TARGET_FIELDS:
        old = _num(prev.get(name))
        new = _num(curr.get(name))
        if old is None and new is None:
            continue
        if old is None:
            changes.append(TargetChange(name, None, new, "added", None))
        elif new is None:
            changes.append(TargetChange(name, old, None, "removed", None))
        elif new != old:
            rel = abs(new - old) / abs(old) if old != 0 else None
            changes.append(TargetChange(name, old, new, "moved", rel))
    return tuple(changes)


def _normalize_condition(text: str) -> str:
    return " ".join(text.split()).casefold()


def _break_condition_diff(
    prev: Mapping[str, object], curr: Mapping[str, object]
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Set diff on normalized condition strings; originals kept for display."""

    def conditions(row: Mapping[str, object]) -> list[str]:
        raw = row.get("break_conditions_json")
        if not isinstance(raw, list):
            return []
        return [item for item in raw if isinstance(item, str)]

    prev_list, curr_list = conditions(prev), conditions(curr)
    prev_keys = {_normalize_condition(c) for c in prev_list}
    curr_keys = {_normalize_condition(c) for c in curr_list}
    added = tuple(c for c in curr_list if _normalize_condition(c) not in prev_keys)
    removed = tuple(c for c in prev_list if _normalize_condition(c) not in curr_keys)
    return added, removed


def _split_sections(memo: str) -> dict[str, str]:
    """memo_markdown → {heading: whitespace-normalized body}.

    Splits on ###-prefixed heading lines (the only level the writer emits —
    full-pop survey 2026-07-15: 942/942 headings). Duplicate headings within
    one memo are collapsed (bodies concatenated) so heading text is a unique
    key — 0 duplicates exist on dev; this is a guard. Content before the
    first heading (or a heading-less memo) becomes the ``(body)``
    pseudo-section; an empty preamble is dropped.
    """
    sections: dict[str, list[str]] = {}
    current = _PREAMBLE_SECTION
    for line in memo.splitlines():
        match = _HEADING_RE.match(line)
        if match:
            current = match.group(1)
            sections.setdefault(current, [])
        else:
            sections.setdefault(current, []).append(line)
    normalized = {heading: " ".join(" ".join(lines).split()) for heading, lines in sections.items()}
    if normalized.get(_PREAMBLE_SECTION) == "":
        del normalized[_PREAMBLE_SECTION]
    return normalized


def _memo_section_diff(
    prev: Mapping[str, object], curr: Mapping[str, object]
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    prev_sections = _split_sections(str(prev.get("memo_markdown") or ""))
    curr_sections = _split_sections(str(curr.get("memo_markdown") or ""))
    added = tuple(h for h in curr_sections if h not in prev_sections)
    removed = tuple(h for h in prev_sections if h not in curr_sections)
    changed = tuple(h for h in curr_sections if h in prev_sections and curr_sections[h] != prev_sections[h])
    return added, removed, changed


def _string_change(prev: Mapping[str, object], curr: Mapping[str, object], key: str) -> FieldChange | None:
    old, new = _str_or_none(prev.get(key)), _str_or_none(curr.get(key))
    if old == new:
        return None
    return FieldChange(from_value=old, to_value=new)


def _summary(
    stance: FieldChange | None,
    thesis_type: FieldChange | None,
    targets: tuple[TargetChange, ...],
) -> str:
    """Deterministic one-liner over the material-class fields.

    Confidence, break conditions, memo sections and provenance never appear:
    they never make a diff material, and the pane's expandable detail carries
    them.
    """
    parts: list[str] = []
    if stance is not None:
        parts.append(f"stance {stance.from_value}→{stance.to_value}")
    if thesis_type is not None:
        parts.append(f"type {thesis_type.from_value}→{thesis_type.to_value}")
    for t in targets:
        label = _FIELD_LABELS[t.field]
        if t.kind == "added":
            assert t.to_value is not None
            parts.append(f"{label} added ({_fmt(t.to_value)})")
        elif t.kind == "removed":
            parts.append(f"{label} removed")
        else:
            assert t.from_value is not None and t.to_value is not None
            move = ""
            if t.rel_move is not None:
                signed = (t.to_value - t.from_value) / abs(t.from_value)
                move = f" ({signed:+.0%})"
            parts.append(f"{label} {_fmt(t.from_value)}→{_fmt(t.to_value)}{move}")
    return " · ".join(parts)


def compute_thesis_diff(prev: Mapping[str, object], curr: Mapping[str, object]) -> ThesisDiff:
    """Structured diff of thesis row ``curr`` vs its predecessor ``prev``.

    Both mappings carry the theses columns (stance, thesis_type,
    confidence_score, the five target/zone fields, break_conditions_json,
    memo_markdown, thesis_version, and optionally prompt_version / model).
    Numeric values may be Decimal (raw DB row) or float (parsed API row).
    """
    stance = _string_change(prev, curr, "stance")
    thesis_type = _string_change(prev, curr, "thesis_type")

    old_conf, new_conf = _num(prev.get("confidence_score")), _num(curr.get("confidence_score"))
    confidence: ConfidenceChange | None = None
    if old_conf != new_conf:
        delta = (new_conf - old_conf) if old_conf is not None and new_conf is not None else None
        confidence = ConfidenceChange(from_value=old_conf, to_value=new_conf, delta=delta)

    targets = _target_changes(prev, curr)
    bc_added, bc_removed = _break_condition_diff(prev, curr)
    memo_added, memo_removed, memo_changed = _memo_section_diff(prev, curr)

    # Material: stance/type change, any target null-transition, any move ≥5%,
    # or a move from a zero base (unbounded relative move — treat as material).
    material = (
        stance is not None
        or thesis_type is not None
        or any(t.kind != "moved" or t.rel_move is None or t.rel_move >= _MATERIAL_REL_MOVE for t in targets)
    )

    return ThesisDiff(
        prev_version=int(prev["thesis_version"]),  # type: ignore[arg-type]
        curr_version=int(curr["thesis_version"]),  # type: ignore[arg-type]
        stance=stance,
        thesis_type=thesis_type,
        confidence=confidence,
        targets=targets,
        break_conditions_added=bc_added,
        break_conditions_removed=bc_removed,
        memo_sections_added=memo_added,
        memo_sections_removed=memo_removed,
        memo_sections_changed=memo_changed,
        prompt_version=_string_change(prev, curr, "prompt_version"),
        model=_string_change(prev, curr, "model"),
        material=material,
        summary=_summary(stance, thesis_type, targets),
    )
