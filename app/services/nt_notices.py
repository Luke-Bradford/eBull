"""NT 10-K / NT 10-Q late-filing notice extractor (SEC Form 12b-25).

Issue #1015 item 1. Pure-Python extraction of the high-signal fields from a
Form 12b-25 body, plus the typed-table upsert. The manifest parser
(``app/services/manifest_parsers/sec_nt.py``) orchestrates fetch / store_raw /
transition around :func:`parse_nt_notice`; keeping the extraction pure here lets
it table-test against real fixtures with no DB (single-chokepoint discipline —
all field logic lives in this one function).

Source rule: SEC Form 12b-25 / Rule 12b-25 (17 CFR 240.12b-25). Cover form
NT 10-K (annual) / NT 10-Q (quarterly). Verified structure on a 32-filing
population sample (2024-2026):

  * ``12b-25`` present 32/32 — used as the "is this a Form 12b-25" gate.
  * ``PART III`` 32/32, ``Period Ended`` 32/32 — reliable anchors.
  * ``results of operations`` (Part IV item 3 question) 31/32 — anchored on the
    question text, NOT the ``PART IV`` heading (only 22/32) which renders
    inconsistently.
  * Unicode checkbox glyphs 31/32 — checkbox booleans are nullable, so the ~1
    non-glyph straggler yields NULL rather than a guessed value.

The 8-K module (``eight_k_events``) has similar strip/date helpers but is a
heavy import (bootstrap_state, dividend fan-out); the helpers here are trivial,
so we keep them local rather than import that module for two regexes.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

PARSER_VERSION = 1

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# Checked-box glyphs: U+2612 (☒) / U+2611 (☑). Empty: U+2610 (☐).
_CHECKED = "☒☑"
_EMPTY = "☐"
_BOX_CLASS = _CHECKED + _EMPTY

_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_MONTH_ALT = "|".join(sorted(_MONTHS, key=len, reverse=True))
_NAME_DATE_RE = re.compile(
    rf"(?P<mon>{_MONTH_ALT})\s+(?P<day>\d{{1,2}}),?\s+(?P<year>\d{{4}})",
    re.IGNORECASE,
)
_NUM_DATE_RE = re.compile(
    r"(?P<m>\d{1,2})/(?P<d>\d{1,2})/(?P<y>\d{4})"
    r"|(?P<y2>\d{4})-(?P<m2>\d{1,2})-(?P<d2>\d{1,2})"
)

# Maximum length we retain for the free-text narrative fields. The reason
# narrative is operator-facing context, not a document store; cap it.
_REASON_MAX = 4000
_EXPLANATION_MAX = 2000


@dataclass(frozen=True)
class NtNotice:
    """Extracted fields from a Form 12b-25 NT 10-K / NT 10-Q body."""

    late_form: str  # '10-K' | '10-Q'
    period_of_report: date | None
    is_transition_report: bool
    grace_period_days: int  # 15 for 10-K, 5 for 10-Q (Rule 12b-25(b))
    reason_text: str | None
    results_change_anticipated: bool | None
    results_change_explanation: str | None


def _strip_html(raw: str) -> str:
    # Strip tags FIRST (so an entity-encoded ``&lt;`` in text doesn't become a
    # fake tag), THEN unescape entities — this turns ``&#9746;`` / ``&rsquo;``
    # etc. into the literal glyph (checkbox ☒ detection depends on it) and
    # ``&nbsp;`` (\xa0) into whitespace, which ``\s+`` then collapses.
    no_tags = _HTML_TAG_RE.sub(" ", raw)
    unescaped = html.unescape(no_tags)
    return _WS_RE.sub(" ", unescaped).strip()


def _parse_date(blob: str) -> date | None:
    """Parse the first Month-name or numeric date in *blob* (a short window)."""
    m = _NAME_DATE_RE.search(blob)
    if m:
        mon = _MONTHS.get(m.group("mon").lower())
        if mon:
            try:
                return date(int(m.group("year")), mon, int(m.group("day")))
            except ValueError:
                return None
    n = _NUM_DATE_RE.search(blob)
    if n:
        try:
            if n.group("m"):
                return date(int(n.group("y")), int(n.group("m")), int(n.group("d")))
            return date(int(n.group("y2")), int(n.group("m2")), int(n.group("d2")))
        except ValueError:
            return None
    return None


def _extract_period(text: str, low: str) -> tuple[date | None, bool]:
    """Return (period_of_report, is_transition_report).

    Prefer the regular "For Period Ended" line; fall back to the
    "Transition Period Ended" line (a transition-report NT). A checked
    "Transition Report on Form ..." box also marks a transition report.
    """
    is_transition = bool(re.search(rf"[{_CHECKED}]\s*Transition\s+Report\s+on\s+Form", text, re.IGNORECASE))
    # "For Period Ended:" (avoid matching the transition variant, which also
    # contains "period ended" — require it NOT be preceded by "transition").
    for m in re.finditer(r"period\s+ended[:\s]*", low):
        start = m.start()
        preceding = low[max(0, start - 14) : start]
        is_trans_line = "transition" in preceding
        window = text[m.end() : m.end() + 48]
        parsed = _parse_date(window)
        if parsed is not None:
            if is_trans_line:
                return parsed, True
            return parsed, is_transition
    return None, is_transition


_LABEL_RES = {
    "Yes": re.compile(r"\bYes\b", re.IGNORECASE),
    "No": re.compile(r"\bNo\b", re.IGNORECASE),
}


# Bracketed-box variants some filers type instead of glyphs: ``[X]`` / ``[x]``
# = checked, ``[ ]`` / ``[]`` = empty. A standalone ``x``/``X`` (not inside a
# word) also marks checked.
_BRACKET_CHECKED_RE = re.compile(r"\[\s*[xX]\s*\]")
_BRACKET_EMPTY_RE = re.compile(r"\[\s*\]")
_BARE_X_RE = re.compile(r"(?<![A-Za-z])[xX](?![A-Za-z])")

# Width of the adjacent cell scanned on each side of a Yes/No label — wide
# enough to capture a ``[ X ]`` box, narrow enough to stay on the label's own
# checkbox.
_CELL_WIDTH = 6


def _cell_state(cell: str) -> bool | None:
    """Classify a short checkbox cell adjacent to a Yes/No label.

    ``True`` = checked, ``False`` = empty box, ``None`` = no box found.
    Handles Unicode glyphs (☒/☑/☐), bracketed boxes (``[X]``/``[ ]``), and a
    standalone typed ``x``/``X``.
    """
    if any(c in cell for c in _CHECKED):
        return True
    if any(c in cell for c in _EMPTY):
        return False
    if _BRACKET_CHECKED_RE.search(cell):
        return True
    if _BRACKET_EMPTY_RE.search(cell):
        return False
    if _BARE_X_RE.search(cell):
        return True
    return None


def _label_box_state(window: str, label: str) -> bool | None:
    """True/False = the checkbox for *label* (Yes/No) is checked/empty;
    None = indeterminate.

    The box sits on a consistent side of its label within a filing but the
    side varies across filings ("☐ Yes ☒ No" vs "Yes ☐ No ☒"). Orientation is
    decided by which side of the "Yes" anchor carries a box cell; the same side
    is then read for *label*. Cells are classified (not single chars) so
    bracketed ``[X]`` boxes and typed ``x`` marks are recognised, not just
    glyphs.
    """
    m = _LABEL_RES[label].search(window)
    if not m:
        return None

    def _cell_before(start: int) -> str:
        return window[max(0, start - _CELL_WIDTH) : start]

    def _cell_after(end: int) -> str:
        return window[end : end + _CELL_WIDTH]

    # Orientation: is the box BEFORE or AFTER its label? Decide from "Yes"
    # (present before "No" in the item-3 question). A box cell immediately
    # before "Yes" ⇒ "before" layout; otherwise a box cell after ⇒ "after".
    anchor = _LABEL_RES["Yes"].search(window) or m
    before_is_box = _cell_state(_cell_before(anchor.start())) is not None
    orient_after = not before_is_box

    cell = _cell_after(m.end()) if orient_after else _cell_before(m.start())
    return _cell_state(cell)


# Anchors that mark the END of the Part III narrative (whichever comes first).
_REASON_END_ANCHORS = (
    "part iv",
    "other information",
    "(1) name",
    "name and telephone",
    "has caused this notification",
    "is it anticipated",
    "/s/",
    " by: ",
)


def _extract_reason(text: str, low: str) -> str | None:
    p3 = low.find("part iii")
    if p3 == -1:
        return None
    # The boilerplate prompt ends "...within the prescribed time period."
    # Start the narrative after the first "time period" following PART III.
    tp = low.find("time period", p3)
    start = (tp + len("time period")) if tp != -1 else (p3 + len("part iii"))
    # Skip leading punctuation/space.
    while start < len(text) and not text[start].isalnum():
        start += 1
    end = len(text)
    for anchor in _REASON_END_ANCHORS:
        idx = low.find(anchor, start)
        if idx != -1:
            end = min(end, idx)
    reason = text[start:end].strip(" .;:-")
    return reason[:_REASON_MAX] or None


def _extract_results_change(text: str, low: str) -> tuple[bool | None, str | None]:
    """Part IV item (3): anticipated significant change in results of operations
    vs the corresponding prior-year period. Returns (anticipated, explanation).

    NOT a restatement field — it is an earnings-direction disclosure.
    """
    # Anchor on the item-(3) question. "significant change in results of
    # operations" is the verbatim item-(3) phrasing; prefer it so a Part III
    # narrative that happens to mention "results of operations" doesn't steal
    # the checkbox window. Fall back to the LAST bare "results of operations"
    # (item 3 sits after Part III) only if the exact phrase is absent.
    q = low.find("significant change in results of operations")
    if q == -1:
        q = low.rfind("results of operations")
    if q == -1:
        return None, None
    # The Yes/No boxes follow the question, before "if so" / "if answer".
    win_end = q + 600
    for marker in ("if so", "if answer"):
        mi = low.find(marker, q)
        if mi != -1:
            win_end = min(win_end, mi)
    window = text[q:win_end]
    yes_state = _label_box_state(window, "Yes")
    no_state = _label_box_state(window, "No")
    if yes_state is True and no_state is not True:
        anticipated: bool | None = True
    elif no_state is True and yes_state is not True:
        anticipated = False
    else:
        anticipated = None

    explanation: str | None = None
    if anticipated:
        explanation = _extract_results_explanation(text, low)
    return anticipated, explanation


def _extract_results_explanation(text: str, low: str) -> str | None:
    """Best-effort capture of the attached results-change explanation when
    item (3) is Yes. Often the explanation is an attached exhibit (absent from
    the body) → None. We capture only substantive inline prose after the
    boilerplate, before the signature block.
    """
    # Boilerplate the explanation follows: "...reasons why a reasonable estimate
    # of the results cannot be made." Then any real prose, then the signature.
    anchor = low.find("cannot be made")
    if anchor == -1:
        return None
    start = anchor + len("cannot be made")
    while start < len(text) and not text[start].isalnum():
        start += 1
    end = len(text)
    for stop in ("has caused this notification", "/s/", " by: "):
        idx = low.find(stop, start)
        if idx != -1:
            end = min(end, idx)
    body = text[start:end].strip(" .;:-")
    # Reject trivial trailing boilerplate / empty captures.
    if len(body) < 20:
        return None
    return body[:_EXPLANATION_MAX]


def parse_nt_notice(body: str, late_form: str) -> NtNotice | None:
    """Extract Form 12b-25 fields from *body* (HTML or text).

    Returns ``None`` when *body* is not a recognizable Form 12b-25 (caller
    tombstones). *late_form* is the authoritative subject form ('10-K' /
    '10-Q'), derived by the caller from the manifest form.
    """
    if late_form not in ("10-K", "10-Q"):
        raise ValueError(f"late_form must be '10-K' or '10-Q', got {late_form!r}")
    text = _strip_html(body)
    low = text.lower()
    if "12b-25" not in low:
        return None
    period, is_transition = _extract_period(text, low)
    reason = _extract_reason(text, low)
    anticipated, explanation = _extract_results_change(text, low)
    grace = 15 if late_form == "10-K" else 5
    return NtNotice(
        late_form=late_form,
        period_of_report=period,
        is_transition_report=is_transition,
        grace_period_days=grace,
        reason_text=reason,
        results_change_anticipated=anticipated,
        results_change_explanation=explanation,
    )


def upsert_nt_notice(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    notice: NtNotice,
) -> None:
    """Upsert one parsed Form 12b-25 into ``nt_filing_notices``."""
    conn.execute(
        """
        INSERT INTO nt_filing_notices (
            accession_number, instrument_id, late_form, period_of_report,
            is_transition_report, grace_period_days, reason_text,
            results_change_anticipated, results_change_explanation,
            parser_version, parsed_at
        ) VALUES (
            %(accession)s, %(instrument_id)s, %(late_form)s, %(period)s,
            %(is_transition)s, %(grace)s, %(reason)s,
            %(results_change)s, %(results_expl)s, %(parser_version)s, NOW()
        )
        ON CONFLICT (accession_number) DO UPDATE SET
            instrument_id = EXCLUDED.instrument_id,
            late_form = EXCLUDED.late_form,
            period_of_report = EXCLUDED.period_of_report,
            is_transition_report = EXCLUDED.is_transition_report,
            grace_period_days = EXCLUDED.grace_period_days,
            reason_text = EXCLUDED.reason_text,
            results_change_anticipated = EXCLUDED.results_change_anticipated,
            results_change_explanation = EXCLUDED.results_change_explanation,
            parser_version = EXCLUDED.parser_version,
            parsed_at = NOW()
        """,
        {
            "accession": accession_number,
            "instrument_id": instrument_id,
            "late_form": notice.late_form,
            "period": notice.period_of_report,
            "is_transition": notice.is_transition_report,
            "grace": notice.grace_period_days,
            "reason": notice.reason_text,
            "results_change": notice.results_change_anticipated,
            "results_expl": notice.results_change_explanation,
            "parser_version": PARSER_VERSION,
        },
    )
