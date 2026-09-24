"""#2351 M2 — bind DEF 14A Item 403 class-grain lines to an instrument.

For one legacy row ``(instrument, accession, holder)`` (``def14a_beneficial_holdings``),
decide whether the instrument shows the legacy figure, a different class line's figure,
or nothing. Pure: the caller supplies the instrument's cover identity and the holder's
M1 lines (``def14a_item403_lines``). Warrant / preferred instruments are NOT decided
here: the shipped slice 2 / 2b functions in ``def14a_recipients`` decide them, unchanged.

Source rule: Item 403(a)/(b) (17 CFR 229.403) — amount and percent are figures of the
class in *Title of class*. Which class an instrument is: the cover's 12(b) table
(``dei:Security12bTitle`` + ``dei:TradingSymbol``). No published rule maps proxy class
wording to a 12(b) title; the comparison here is fixed by construction and frozen by
``BINDING_RULE_VERSION``.

Spec: ``docs/proposals/ownership/2026-09-24-2351-m2-binding.md``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Literal

from app.services.def14a_item403_lines import Item403Line, class_evidence, keyed_designators
from app.services.def14a_recipients import (
    _DESIGNATOR,
    _NON_COMMON_CAPTION,
    Cover,
    Sibling,
    symbol_key,
    title_kind,
    usable_common_classes,
)

BINDING_RULE_VERSION: Final = 1

_DEPOSITARY = re.compile(r"depositar[yi]|depositor[yi]|\bAD[RS]s?\b", re.IGNORECASE)
# Designators the class grammar does not model: "Class A-1", "A and A-1", "Class II", "Series 3".
UNSUPPORTED: Final = re.compile(r"\b[a-z]\s*-\s*\d|\b(?:class(?:es)?|series)\s+(?:[ivx]+|\d+)\b", re.IGNORECASE)
_CLASS_WORD = re.compile(r"\b(?:class(?:es)?|series)\b", re.IGNORECASE)
_NEUTRAL_KINDS: Final = frozenset({"warrant", "unit", "right", "note"})

Relation = Literal["match", "mismatch", "neutral"]
Outcome = Literal["keep", "confirmed", "rebind", "withhold"]


@dataclass(frozen=True)
class Identity:
    """A common 12(b) class. ``designator`` is None for a plain ``Common Stock`` identity."""

    designator: tuple[str, str] | None
    title: str
    cover_accession: str


def common_identity(sibling: Sibling, siblings: Iterable[Sibling], cover: Cover) -> Identity | str:
    """The sibling's common identity on COVER, or the reason it has none."""
    key = symbol_key(sibling.symbol)
    titles = {title for title, symbol in cover.pairs if symbol == key}
    if not titles:
        return "not_on_cover"
    if len(titles) > 1:
        return "several_titles"
    title = next(iter(titles))
    if title_kind(title) != "common":
        return "not_common"
    if _NON_COMMON_CAPTION.search(title) or _DEPOSITARY.search(title) or UNSUPPORTED.search(title):
        return "unsupported_title"
    if not _CLASS_WORD.search(title):
        others_common = any(title_kind(t) == "common" for t, _ in cover.pairs if t != title)
        return "plain_not_sole_common" if others_common else Identity(None, title, cover.accession)
    usable = usable_common_classes(siblings, cover)
    if not any(cc.key == key for ccs in usable.values() for cc in ccs):
        return "letter_not_usable"
    keyed = keyed_designators(title)
    if len(keyed) != 1 or _CLASS_WORD.search(_DESIGNATOR.sub("", title)):
        return "designator_ambiguous"
    return Identity(next(iter(keyed)), title, cover.accession)


def line_evidence(line: Item403Line) -> frozenset[str]:
    """E(line): class evidence of its row class cells, section label and group."""
    label = [line.section_label] if line.section_label else []
    return class_evidence(line.row_class_cells) | class_evidence(label) | line.group_evidence


def relation(line: Item403Line, identity: Identity) -> Relation:
    if line.class_state == "conflict":
        return "neutral"
    texts = [*line.row_class_cells, *([line.section_label] if line.section_label else [])]
    if any("%" in t or UNSUPPORTED.search(t) for t in texts):
        return "neutral"
    evidence = line_evidence(line)
    designators = {tuple(t.split(":", 1)) for t in evidence if ":" in t}
    kinds = {t for t in evidence if ":" not in t}
    if kinds & _NEUTRAL_KINDS:
        return "neutral"
    if "preferred" in kinds:
        return "mismatch"
    if identity.designator is None:
        return "match" if not designators and kinds == {"common"} else "neutral"
    if designators == {identity.designator}:
        return "match"
    if len(designators) == 1:
        keyword, letter = next(iter(designators))
        if keyword == identity.designator[0] and letter != identity.designator[1]:
            return "mismatch"
    return "neutral"


def share_counts(line: Item403Line) -> list[Decimal]:
    """Every share-count cell of the line (a percent cell is never one)."""
    return [c.shares for c in line.amount_cells if c.shares is not None and c.percent is None]


def figure(line: Item403Line) -> Decimal | None:
    """The line's figure: its one share cell, when it is a whole count > 0. Several cells
    (direct + options + total), a dash / ``*`` / zero, or a fractional count give none —
    never a pick. A fractional count is refused because a thousands separator typed as
    a decimal point reads as one (Ginkgo 2025: ``48.176`` for 48,176 in its footnote)."""
    counts = share_counts(line)
    if len(counts) != 1 or counts[0] <= 0 or counts[0] != counts[0].to_integral_value():
        return None
    return counts[0]


@dataclass(frozen=True)
class Decision:
    outcome: Outcome
    reason: str
    shares: Decimal | None = None  # rebind only
    lines: tuple[tuple[int, int], ...] = ()  # (table_ordinal, grid_row) of the lines used


def decide_row(*, legacy_shares: Decimal | None, identity: Identity, lines: list[Item403Line]) -> Decision:
    """Steps 5-7 of the spec for one holder's legacy row; ``lines`` are that holder's."""
    if legacy_shares is None:
        return Decision("keep", "abstained:no_figure")
    if not lines:
        return Decision("keep", "abstained:no_line")
    source = [ln for ln in lines if legacy_shares in share_counts(ln)]
    refs = tuple((ln.table_ordinal, ln.grid_row) for ln in source)
    if not source:
        return Decision("keep", "abstained:no_source")
    relations = [relation(ln, identity) for ln in source]
    if "match" in relations:
        return Decision("confirmed", "confirmed", lines=refs)
    if any(r != "mismatch" for r in relations):
        return Decision("keep", "abstained:ambiguous", lines=refs)
    tables = {ln.table_ordinal for ln in source}
    if identity.designator is not None and len(tables) == 1:
        matches = [ln for ln in lines if ln.table_ordinal in tables and relation(ln, identity) == "match"]
        if len(matches) == 1 and (fig := figure(matches[0])) is not None:
            used = (*refs, (matches[0].table_ordinal, matches[0].grid_row))
            return Decision("rebind", "other_class:rebound", shares=fig, lines=used)
    return Decision("withhold", "other_class", lines=refs)
