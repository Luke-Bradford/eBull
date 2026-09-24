"""#2351 M1 — DEF 14A Item 403 lines at their own grain: (holder, title of class).

17 CFR 229.403(a)/(b) prescribe the columns *(1) Title of class, (2) Name of beneficial
owner, (3) Amount and nature of beneficial ownership, (4) Percent of class*; amount and
percent are figures of the class in column (1). The legacy parser
(``parse_beneficial_ownership_table``) keeps one row per holder and one shares column, so
a holder's other-class lines are discarded. This module exposes every line with its
class EVIDENCE. It interprets table geometry only; it does not decide which class a line
is, which amount is the total or which percent is percent-of-class (M2's job). Every rule
fails toward ``unlabelled`` / ``conflict`` / no line, never toward a confident label.

Pure: no DB, no network. Spec: ``docs/proposals/ownership/2026-09-24-2351-def14a-class-grain-model.md``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal

from app.providers.implementations.sec_def14a import (
    _TITLE_OF_CLASS,
    _clean_beneficial_holder_name,
    _extract_table_holders,
    _is_percent_caption,
    _is_share_cell,
    _layout_rows,
    _parse_percent,
    _parse_share_count,
    _score_table_headers,
    _select_item403_tables,
)

# A keyword inside a list switches the keyword for the letters after it:
# "Class A and Series B" -> (class, A), (series, B). A letter followed by a word
# character or hyphen ("Class A-1", "Class II") is not a designator.
_LETTER = r"[a-z](?![\w-])"
_LIST = re.compile(
    rf"\b(class|series)(?:es)?\s+({_LETTER}(?:\s*(?:,|and|or|&|/)\s*(?:(?:class|series)(?:es)?\s+)?{_LETTER})*)",
    re.IGNORECASE,
)
_ITEM = re.compile(rf"(?:\b(class|series)(?:es)?\s+)?(?<![\w-])({_LETTER})", re.IGNORECASE)
_KINDS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("common", re.compile(r"\b(?:common|ordinary|capital\s+(?:stock|shares?))\b", re.IGNORECASE)),
    ("preferred", re.compile(r"\b(?:preferred|preference)\b", re.IGNORECASE)),
    ("warrant", re.compile(r"\bwarrants?\b", re.IGNORECASE)),
    ("unit", re.compile(r"\bunits?\b", re.IGNORECASE)),
    ("right", re.compile(r"\brights?\b", re.IGNORECASE)),
    ("note", re.compile(r"\b(?:notes?|debentures?)\b", re.IGNORECASE)),
)
_PENDING_ROWS = 3
_MARKERS = frozenset({"*", "**", "—", "–", "-", "--", "n/a", "na"})


def keyed_designators(text: str) -> frozenset[tuple[str, str]]:
    """``(keyword, letter)`` pairs, keyword ``class`` or ``series``."""
    out: set[tuple[str, str]] = set()
    for keyword, group in _LIST.findall(text):
        current = keyword.lower()
        for switch, letter in _ITEM.findall(f"{keyword} {group}"):
            current = switch.lower() or current
            out.add((current, letter.upper()))
    return frozenset(out)


# In an AMOUNT column's caption, warrant / unit / right / note words describe the NATURE of
# beneficial ownership, not a class: Rule 13d-3(d)(1)(i) counts securities a person may
# acquire within 60 days "through the exercise of any option, warrant or right" or "through
# the conversion of a security", and proxies caption those columns so ("Restricted Stock
# Units", "Rights to Acquire Within 60 Days"). Only designators and ``preferred`` are class
# evidence there. A *Title of class* cell or a section label keeps every kind.
_CAPTION_KINDS = frozenset({"common", "preferred"})


def class_evidence(texts: list[str] | tuple[str, ...], *, caption: bool = False) -> frozenset[str]:
    """Class tokens in TEXTS: ``class:A`` / ``series:B`` designators plus security kinds."""
    tokens: set[str] = set()
    for text in texts:
        tokens.update(f"{k}:{letter}" for k, letter in keyed_designators(text))
        tokens.update(
            kind for kind, pattern in _KINDS if (not caption or kind in _CAPTION_KINDS) and pattern.search(text)
        )
    return frozenset(tokens)


def _state(evidence: frozenset[str]) -> str:
    designators = [t for t in evidence if ":" in t]
    kinds = [t for t in evidence if ":" not in t]
    if not evidence:
        return "unlabelled"
    return "labelled" if len(designators) <= 1 and len(kinds) <= 1 else "conflict"


@dataclass(frozen=True)
class AmountCell:
    first_column: int
    captions: tuple[str, ...]
    interior: tuple[str, ...]
    raw_text: str
    shares: Decimal | None
    percent: Decimal | None


@dataclass(frozen=True)
class Item403Line:
    holder_name: str
    holder_role: str | None
    table_ordinal: int
    grid_row: int
    continuation: bool
    row_class_cells: tuple[str, ...]
    section_label: str | None
    group_evidence: frozenset[str]
    amount_cells: tuple[AmountCell, ...]
    class_state: str


@dataclass
class Item403Extraction:
    lines: list[Item403Line] = field(default_factory=list)
    # Keys the parser emitted under more than one holder_name (e.g. dot leaders of
    # different lengths): one person, attributed to the first-emitted name.
    duplicate_keys: list[str] = field(default_factory=list)
    errors: list[tuple[int, str]] = field(default_factory=list)


def _full_key(text: str) -> str:
    """The whole cleaned name, alphanumerics only. ``_layout_name_key`` is a 16-character
    prefix, which collides on ``Entities affiliated with …``; a holder row's owner cell
    carries the name plus, often, an address, so the join is "cell key starts with the
    holder key", longest holder key winning."""
    return re.sub(r"[^a-z0-9]+", "", _clean_beneficial_holder_name(text).lower())


def _match(cell_key: str, keys: dict[str, list[tuple[str, str | None]]]) -> list[tuple[str, str | None]] | None:
    """The accepted holders whose key is the longest prefix of CELL_KEY; None if none."""
    best = max((k for k in keys if cell_key.startswith(k)), key=len, default=None)
    return keys[best] if best else None


def _has_word(text: str) -> bool:
    """A cell with any letter or digit (a zero-width space or a bare footnote mark is not
    an owner cell)."""
    return any(ch.isalnum() for ch in text)


def _is_marker(text: str) -> bool:
    return text.strip().lower() in _MARKERS


def item403_lines(html_text: str) -> Item403Extraction:
    out = Item403Extraction()
    if not html_text:
        return out
    tables = _select_item403_tables(html_text)[0]
    # The parser's own iteration order and holders (parse_beneficial_ownership_table).
    ordered = sorted(tables, key=lambda t: -_score_table_headers(t.score_headers))
    holders = []
    seen: set[str] = set()
    for table in ordered:
        _extract_table_holders(table, rows=holders, seen=seen)
    by_key: dict[str, list[tuple[str, str | None]]] = {}
    for h in holders:
        if key := _full_key(h.holder_name):
            by_key.setdefault(key, []).append((h.holder_name, h.holder_role))
    out.duplicate_keys = sorted(k for k, v in by_key.items() if len(v) > 1)
    for ordinal, table in enumerate(ordered):
        try:
            out.lines.extend(_table_lines(ordinal, table.table_html, by_key))
        except Exception as exc:  # noqa: BLE001 — best-effort, recorded per table
            out.errors.append((ordinal, repr(exc)[:200]))
    return out


def _table_lines(ordinal: int, table_html: str, by_key: dict[str, list[tuple[str, str | None]]]) -> list[Item403Line]:
    grid = _layout_rows(table_html)
    # First row carrying a figure: a share count, or a %-signed percent (a table whose
    # share cells are all dashes still reports its holders by percent).
    first_share = next(
        (
            r
            for r, row in enumerate(grid)
            if any(_is_share_cell(t) or ("%" in t and _parse_percent(t) is not None) for t in row.values() if t)
        ),
        None,
    )
    if first_share is None:
        return []
    # The header block ends at the first share cell, or earlier at an accepted holder's
    # name row (a name-and-address row whose figures sit on the row below).
    header_end = next(
        (
            r
            for r, row in enumerate(grid[:first_share])
            if any((k := _full_key(t)) and _match(k, by_key) for t in row.values() if t and _has_word(t))
        ),
        first_share,
    )
    header_texts: dict[int, list[str]] = {}
    for above in grid[:header_end]:
        for c, text in above.items():
            if text and _parse_share_count(text) is None and text not in header_texts.setdefault(c, []):
                header_texts[c].append(text)
    class_cols = {c for c, texts in header_texts.items() if _TITLE_OF_CLASS.search(" ".join(texts))}
    percent_cols = {c for c, texts in header_texts.items() if any(_is_percent_caption(t) for t in texts)}
    value_cols = {
        c
        for row in grid[header_end:]
        for c, t in row.items()
        if t and c not in class_cols and (_is_share_cell(t) or ("%" in t and _parse_percent(t) is not None))
    } | percent_cols

    def amount(c: int, text: str) -> tuple[Decimal | None, Decimal | None] | None:
        if not text or c in class_cols:
            return None
        if _is_share_cell(text) and c not in percent_cols:
            return _parse_share_count(text), None
        has_digit = any(ch.isdigit() for ch in text)
        if has_digit and ("%" in text or c in percent_cols) and (p := _parse_percent(text)) is not None:
            return None, p
        if c in value_cols and _is_marker(text):
            return None, None
        return None

    lines: list[Item403Line] = []
    label: str | None = None
    # Interior evidence (a mid-table re-header over one column) is read only below the
    # ACTIVE section label, so a label that a later one replaced stops counting.
    section_start = 0
    label_evidence: frozenset[str] = frozenset()
    # (holder_name, role) of the last holder row, for V continuations; reset by any
    # non-empty row that is neither.
    last_holder: tuple[str, str | None] | None = None
    # A holder whose NAME row carried no amount: its figures sit on a following row (name
    # and address first, values below). Survives up to _PENDING_ROWS unmatched rows (the
    # address lines); a label or another holder's row ends it.
    pending: tuple[str, str | None] | None = None
    pending_gap = 0
    for r in range(len(grid)):
        # A zero-width space inside a cell ("\u200b17,779") is layout, not content.
        cells = {c: t for c, raw in grid[r].items() if (t := raw.replace("\u200b", "").strip())}
        if not cells:
            continue
        amounts = {c: a for c, t in cells.items() if (a := amount(c, t)) is not None}
        owner_col = min(
            (c for c, t in cells.items() if c not in amounts and c not in class_cols and _has_word(t)), default=None
        )
        owner_key = _full_key(cells[owner_col]) if owner_col is not None else ""
        matches = _match(owner_key, by_key) if owner_key else None
        others = [
            m
            for c, t in cells.items()
            if c != owner_col
            and c not in amounts
            and c not in class_cols
            and (k := _full_key(t))
            and (m := _match(k, by_key)) is not None
            and m != matches
        ]
        if r < header_end or not amounts:
            ev = class_evidence(list(dict.fromkeys(cells.values())))
            designators = [t for t in ev if ":" in t]
            is_label = (
                bool(ev) and len(designators) <= 1 and not (r < header_end and not _is_row_label(cells, class_cols))
            )
            if is_label:
                label, label_evidence = next(iter(dict.fromkeys(cells.values()))), ev
                section_start = r + 1
            elif r >= header_end and _is_full_row(cells):
                # An ignored full-row title (two designators) closes the section too.
                section_start = r + 1
            if r >= header_end:
                last_holder = None
                if matches and not others and not is_label:
                    pending, pending_gap = matches[0], 0
                elif pending is not None and not matches and not is_label and pending_gap < _PENDING_ROWS:
                    pending_gap += 1
                else:
                    pending = None
            continue
        class_cells = tuple(dict.fromkeys(cells[c] for c in sorted(class_cols) if c in cells))
        continuation = False
        if matches and not others:
            holder = matches[0]
        elif pending is not None and not matches and not others:
            holder, continuation = pending, True
        elif not owner_key and last_holder is not None and class_cells:
            holder, continuation = last_holder, True
        else:
            last_holder = pending = None
            continue
        pending = None
        last_holder = holder
        # One cell per run of identical adjacent slots (an expanded colspan).
        runs: list[tuple[int, int, str]] = []
        for c in sorted(amounts):
            text = cells[c]
            if (
                runs
                and runs[-1][1] == c - 1
                and runs[-1][2] == text
                and header_texts.get(c) == header_texts.get(runs[-1][0])
            ):
                runs[-1] = (runs[-1][0], c, text)
            else:
                runs.append((c, c, text))
        groups: dict[frozenset[str], list[AmountCell]] = {}
        for first, _last, text in runs:
            caps = tuple(
                t for t in header_texts.get(first, []) if owner_col is None or t not in header_texts.get(owner_col, [])
            )
            interior = tuple(
                t
                for mid in grid[max(header_end, section_start) : r]
                if (t := mid.get(first, "")) and amount(first, t) is None and not _is_full_row(mid)
            )
            shares, percent = amounts[first]
            cell = AmountCell(first, caps, interior, text, shares, percent)
            # Group by designator and non-common kind; ``common`` is the default kind and
            # never splits a shares column from its percent column.
            groups.setdefault(class_evidence(caps + interior, caption=True) - {"common"}, []).append(cell)
        row_ev = class_evidence(class_cells)
        for group_ev, group_cells in groups.items():
            lines.append(
                Item403Line(
                    holder_name=holder[0],
                    holder_role=holder[1],
                    table_ordinal=ordinal,
                    grid_row=r,
                    continuation=continuation,
                    row_class_cells=class_cells,
                    section_label=label,
                    group_evidence=group_ev,
                    amount_cells=tuple(group_cells),
                    class_state=_state(row_ev | label_evidence | group_ev),
                )
            )
    return lines


def _is_full_row(cells: dict[int, str]) -> bool:
    """One distinct non-empty text across the row: a section title, never a column caption."""
    return len({t for t in cells.values() if t}) == 1


def _is_row_label(cells: dict[int, str], class_cols: set[int]) -> bool:
    """A header-block row is a section label only when one text spans the whole row (a
    label before the first holder), not a caption row of several column headings."""
    return len(set(cells.values())) == 1 and not (set(cells) & class_cols)
