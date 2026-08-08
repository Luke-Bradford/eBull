"""SEC DEF 14A (proxy statement) beneficial-ownership table parser.

DEF 14A is filed annually by every Section 12-registered issuer.
Item 12 of the proxy carries the "Security Ownership of Certain
Beneficial Owners and Management" table — every officer + director
+ 5%+ holder with their share count and percent of class as of the
proxy's record date.

Use cases (per #769):

  * Cross-check Form 4 cumulative running total — flag drift > 5%.
  * Backfill historical insider holdings before Form 4 coverage
    starts.
  * Catch insiders who hold but never trade (no Form 4 events).
  * Validate 13D/G blockholder ingest (#766) once shipped — Item 12
    lists 5%+ holders independently of the holders' own filings.

This module is a pure parser: HTML strings in, typed dataclasses
out. HTTP fetch + DB resolution stay in the service layer per the
settled provider-design rule.

Parser strategy (deliberately conservative):

  1. **Section locator** — find the "Security Ownership" / "5%
     Holders" / "Beneficial Ownership" heading in the HTML stream.
     Returns the byte offset of the heading; the caller scans for
     ``<table>`` blocks at or after that offset.
  2. **Table scoring** — for each candidate ``<table>`` block, score
     the headers row by how many of {name|holder|owner,
     shares|number, percent|%} substrings it contains. The
     highest-scoring table within the section's window is the
     beneficial-ownership table.
  3. **Row extraction** — walk rows, extract holder name, shares,
     percent. Tolerate footnote markers, asterisks, and numeric
     formatting (commas, parentheses for negatives, leading "(1)" /
     "(*)" footnote refs).
  4. **Role inference** — section subheadings ("Directors and
     Executive Officers", "5% Holders", "Principal Stockholders")
     drive a heuristic role tag on each row. Defaults to NULL when
     the table is one flat list with no subheadings.

Variance tolerance:

DEF 14A tables vary wildly across filers. Some put officers and 5%
holders in one table; others split them. Some include "All directors
and executive officers as a group" as a synthesis row; others don't.
Some footnote shares with explanatory notes that the parser must
preserve as suffixes on the holder_name (so audit trails stay
intact) without polluting the share-count column.

The parser errs on the side of returning fewer rows when a table is
ambiguous. Empty result = "could not confidently identify the
table"; the ingester (PR 2) tombstones the accession.

#769 PR 1 of N. Subsequent PRs add the ingester (PR 2) and the
drift-detector job that compares DEF 14A snapshots against Form 4
cumulative balances (PR 3).
"""

from __future__ import annotations

import html
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final, NamedTuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Def14ABeneficialHolder:
    """One row from the Item 12 beneficial-ownership table.

    Field semantics:

      * ``holder_name`` — the holder's name as it appears in the
        table's first column. Footnote markers (``(1)``, ``(*)``,
        ``[a]``) are stripped; explanatory parenthetical suffixes
        in the same cell (e.g. ``"John Doe, CFO"``) are preserved
        so a downstream Form-4 reconciliation can use them.
      * ``holder_role`` — one of ``'officer'`` / ``'director'`` /
        ``'principal'`` / ``'group'`` (for a synthesis row like
        "All directors and executive officers as a group") or
        ``None`` when the table is one flat list and the parser
        cannot infer a role from a section subheading. The
        ingester layer is free to enrich this via a curated
        name→role seed table.
      * ``shares`` — share count as ``Decimal``. ``None`` when the
        cell is empty, dashed, or unparseable; this is rare but
        legal (some issuers redact closely-held positions).
      * ``percent_of_class`` — percent as ``Decimal``. ``None``
        under the same rule. Asterisk markers (``*``) typically
        denote "less than 1%" in the proxy footnotes — those map
        to ``Decimal('0.5')`` per industry convention since "less
        than 1%" is not literally zero.
    """

    holder_name: str
    holder_role: str | None
    shares: Decimal | None
    percent_of_class: Decimal | None


@dataclass(frozen=True)
class Def14ABeneficialOwnershipTable:
    """The full parsed payload from a DEF 14A primary doc.

    Field semantics:

      * ``as_of_date`` — the table's "as of" record date when the
        parser can extract one from the surrounding prose
        (typical: ``"as of March 1, 2026"`` in the section
        introduction). NULL when no date is found.
      * ``rows`` — 0..N. An empty list signals "table not
        confidently identified"; the ingester tombstones the
        accession.
      * ``raw_table_score`` — internal diagnostics: the score of
        the chosen table, exposed so the ingester's audit log can
        record how confident the parser was. Higher is better.
    """

    as_of_date: date | None
    rows: list[Def14ABeneficialHolder]
    raw_table_score: int


# ---------------------------------------------------------------------------
# Regex helpers (mirror the proven patterns from
# app/services/business_summary.py — duplicated rather than imported
# to keep the parser provider-side / pure)
# ---------------------------------------------------------------------------


_TABLE_OPEN_RE: Final[re.Pattern[str]] = re.compile(r"<table\b[^>]*>", re.IGNORECASE)
_TABLE_CLOSE_RE: Final[re.Pattern[str]] = re.compile(r"</table\s*>", re.IGNORECASE)
_TR_RE: Final[re.Pattern[str]] = re.compile(r"<tr\b[^>]*>(.*?)</tr\s*>", re.IGNORECASE | re.DOTALL)
# Group 1 is the tag's ATTRIBUTES, group 2 the cell contents. The attributes
# are needed for ``rowspan`` — see :func:`_expand_row_spans`.
_CELL_RE: Final[re.Pattern[str]] = re.compile(r"<(?:t[hd])\b([^>]*)>(.*?)</t[hd]\s*>", re.IGNORECASE | re.DOTALL)
# Every cell tag, SELF-CLOSING included, each paired with its OWN attributes.
#
# ``_CELL_RE`` above cannot do this and is left alone deliberately — its cell
# tuples are the index space that header scoring, two-row-header promotion and
# ``_resolve_columns`` are all tuned against, and moving it re-selects which
# table wins corpus-wide (#2175's prevention entry). This pattern feeds the
# read-only layout grid in :func:`_layout_percent_by_row` instead.
#
# The defect it exists to route around: ``<td style="x"/>`` matches
# ``<t[hd]\b([^>]*)>`` because ``[^>]*`` accepts the trailing ``/``, so
# ``_CELL_RE`` reads a self-closing spacer as an OPENING tag and runs ``(.*?)``
# on to the next real ``</td>``. The emitted cell then carries the SPACER's
# attributes with the FOLLOWING cell's text, which is why a ``colspan="4"``
# caption reads as ``colspan=1``.
_ANY_CELL_RE: Final[re.Pattern[str]] = re.compile(
    r"<t[hd]\b([^>]*?)/>|<t[hd]\b([^>]*)>(.*?)</t[hd]\s*>", re.IGNORECASE | re.DOTALL
)
# A QUANTITY immediately left of the sign, as in ``5% Beneficial owners`` — the
# shape that separates a threshold PHRASE from a percent CAPTION. A caption names
# a column and states no quantity ("Percent of Class", "% (2)"); a section label
# binds the sign to the number on its left.
#
# Both the sign and the word, because the label is written both ways and only the
# ``%`` half was covered at first: ``5 Percent Beneficial Owners`` and ``Ten
# Percent Holders`` are the same label. The number words are the two the reg
# actually produces — 17 CFR 229.403(a) is the 5% threshold and Section 16 is the
# 10% one — so this is the source's vocabulary, not an open-ended list.
_THRESHOLD_LABEL_RE: Final[re.Pattern[str]] = re.compile(r"(?:\d|\bfive\b|\bten\b)\s*(?:%|percent\b)")
_ROWSPAN_RE: Final[re.Pattern[str]] = re.compile(r"\browspan\s*=\s*[\"']?\s*(\d+)", re.IGNORECASE)
_COLSPAN_RE: Final[re.Pattern[str]] = re.compile(r"\bcolspan\s*=\s*[\"']?\s*(\d+)", re.IGNORECASE)
_HTML_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")
_NBSP_RE: Final[re.Pattern[str]] = re.compile(r"&nbsp;| ")
_INLINE_WHITESPACE_RE: Final[re.Pattern[str]] = re.compile(r"[ \t\r\f\v]+")

# Block-level markup, for the OPT-IN line-structured cell rendering (#2358).
# HTML's line structure inside a cell is carried by these elements, NOT by the
# source newlines that happen to survive ``_HTML_TAG_RE`` — a filer agent that
# emits ``<p>A</p><p>B</p>`` on one source line renders two lines and parses as
# one. ``ul`` / ``ol`` are listed for completeness; ``td`` / ``tr`` are not,
# because ``_parse_table_html`` has already split on them and scrubbed nested
# tables by the time a cell's interior reaches here.
_BLOCK_ELEMENTS: Final[str] = r"p|div|li|ul|ol|blockquote|h[1-6]"
# A close tag ADJACENT to the next open tag is ONE line boundary, not two. This
# runs first so ``</p>\n  <p ...>`` collapses to a single ``\n`` and cannot be
# mistaken for the empty block below.
_BLOCK_BOUNDARY_RE: Final[re.Pattern[str]] = re.compile(
    rf"</(?:{_BLOCK_ELEMENTS})\s*>\s*<(?:{_BLOCK_ELEMENTS})(?=[\s/>])[^>]*>", re.IGNORECASE
)
_BLOCK_TAG_RE: Final[re.Pattern[str]] = re.compile(rf"</?(?:{_BLOCK_ELEMENTS})(?=[\s/>])[^>]*>", re.IGNORECASE)
_LINE_BREAK_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<br(?=[\s/>])[^>]*>", re.IGNORECASE)
# Cheap precondition for the line-structured pass: a cell carrying none of these
# renders identically with and without ``block_breaks``. Keep the alternation in
# step with the rules that consume it — every one is keyed on ``br`` or on
# ``_BLOCK_ELEMENTS``, and nothing else can introduce a sentinel.
_LINE_STRUCTURE_TAG_RE: Final[re.Pattern[str]] = re.compile(rf"<(?:br|{_BLOCK_ELEMENTS})(?=[\s/>])", re.IGNORECASE)
# Tag-derived breaks are marked with a SENTINEL, not a newline, so they can be
# told apart from the source newlines that reach this function as ordinary text.
# NUL cannot occur in an EDGAR document (SEC EDGAR Filer Manual vol. II §5.2.2
# restricts primary documents to ASCII 32-127 plus tab/CR/LF/FF).
_BREAK_SENTINEL: Final[str] = "\x00"
# A RENDERED BLANK LINE, normalised to ``sentinel SPACE sentinel`` so the run
# collapse below cannot swallow it. Two markup shapes produce one:
#
#   ``<p>&nbsp;</p>`` / ``<p><br/></p>``  a block with no content of its own
#   ``A<br/><br/>B``                      consecutive explicit breaks
#
# ⚠ The ``<br>``-only block is Codex checkpoint 2's finding on this branch, and
# it is not rare: over the first 4,000 accessions of the corpus it appears in
# **293 Item 403 candidate cells across 59 accessions** (``<br><br>`` runs in
# 444 / 124). Without it the sentinel run reads ``<p>A</p><p><br/></p><p>B</p>``
# as adjacent boundaries, ``_stacked_name_blocks`` sees ONE block, and two
# stacked owners merge into one holder identity.
_BLANK_LINE: Final[str] = f"{_BREAK_SENTINEL} {_BREAK_SENTINEL}"
_EMPTY_BLOCK_RE: Final[re.Pattern[str]] = re.compile(
    rf"<({_BLOCK_ELEMENTS})(?=[\s/>])[^>]*>(?:\s|&nbsp;|&#160;|&#xa0;|<br\b[^>]*>)*</\1\s*>", re.IGNORECASE
)
_BREAK_RUN_RE: Final[re.Pattern[str]] = re.compile(r"<br(?=[\s/>])[^>]*>(?:\s*<br(?=[\s/>])[^>]*>)+", re.IGNORECASE)
# ⚠ Load-bearing for #2169's holder split, which separates stacked owners on a
# BLANK line — and each of these shapes is in the corpus:
#
#   ``<br/></p> <p>``     two ADJACENT tag breaks; renders as ONE. Collapses.
#   ``</p>\n<p>``         a tag break beside a source newline; ONE. Collapses.
#   ``<p>&nbsp;</p>``     an empty block; a real blank line. Already rewritten
#                         to ``_BLANK_LINE`` above, whose SPACE breaks this run.
#
# A run of literal source newlines carrying no sentinel is left exactly as the
# flat rendering has it (``\n\n\n\n`` between two stacked holders on
# 0000351998-18-000006 is that shape) — this must not re-cut line structure the
# tags did not ask for.
_SENTINEL_RUN_RE: Final[re.Pattern[str]] = re.compile(rf"\n*{_BREAK_SENTINEL}[\n{_BREAK_SENTINEL}]*")

# Section heading variants. Case-insensitive; tolerate intervening
# punctuation / line breaks. The proxy form mandates the heading
# wording but issuers vary in casing and punctuation.
_SECTION_HEADING_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:Security\s+Ownership\s+of\s+Certain\s+Beneficial\s+Owners"
    r"|Beneficial\s+Ownership\s+of\s+(?:Common\s+Stock|Securities)"
    r"|Principal\s+Stockholders"
    r"|5\s*%\s*(?:or\s+(?:more|greater)\s+)?(?:Beneficial\s+)?(?:Stock)?holders?)",
    re.IGNORECASE,
)

# "as of <date>" extraction — accepts both ``January 1, 2026`` and
# ``1/1/2026`` formats. The proxy form requires a record date but
# issuers vary in surface format.
_AS_OF_DATE_RE: Final[re.Pattern[str]] = re.compile(
    r"as\s+of\s+("
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4}"
    r"|\d{1,2}/\d{1,2}/\d{4}"
    r"|\d{4}-\d{2}-\d{2}"
    r")",
    re.IGNORECASE,
)


def _strip_inline_html(raw: str, *, block_breaks: bool = False) -> str:
    """Strip HTML tags + entities, collapse whitespace. Used on cell
    contents so footnote-superscript ``<sup>(1)</sup>`` markers
    survive as plain ``(1)`` text and can be detected by the
    footnote-stripping regex below.

    ``block_breaks`` (#2358) renders ``<br>`` and every block element as a
    LINE BREAK instead of a space. **Off by default, and every existing caller
    keeps the default** — this function feeds header text into
    ``_score_table_headers`` / ``_resolve_columns``, both of which substring-
    match SEC-prescribed multi-word captions on ``" ".join(headers).lower()``,
    so a newline inside ``Amount<br/>and Nature of<br/>Beneficial Ownership``
    would stop "amount and nature" matching and move which table wins. That is
    the #2164 incident exactly (prevention log: "a fix placed at a shared
    chokepoint reaches consumers you did not enumerate"), so the line-structured
    rendering is carried on a PARALLEL grid — ``_RawTable.line_rows`` — and only
    the Item 403 holder split reads it.

    Without it, ``'486,340<br>658,400'`` arrives as ``'486,340 658,400'`` and
    ``_parse_share_count`` — which strips spaces AND commas — returns
    486,340,658,400.
    """
    if block_breaks:
        # Blank lines FIRST: both shapes contain the tags the rules below
        # rewrite, and once ``<p><br/></p>`` has become three bare sentinels
        # nothing can tell it from two adjacent block boundaries.
        raw = _EMPTY_BLOCK_RE.sub(_BLANK_LINE, raw)
        raw = _BREAK_RUN_RE.sub(_BLANK_LINE, raw)
        raw = _BLOCK_BOUNDARY_RE.sub(_BREAK_SENTINEL, raw)
        raw = _LINE_BREAK_TAG_RE.sub(_BREAK_SENTINEL, raw)
        raw = _BLOCK_TAG_RE.sub(_BREAK_SENTINEL, raw)
    no_tags = _HTML_TAG_RE.sub(" ", raw)
    no_nbsp = _NBSP_RE.sub(" ", no_tags)
    decoded = html.unescape(no_nbsp)
    # Fold unicode spaces to plain spaces (#2140). ``_NBSP_RE`` only catches
    # the ``&nbsp;`` ENTITY, so a literal U+00A0 in the markup survived — and
    # Item 403's prescribed caption then failed to match: 'Amount\xa0and\xa0
    # Nature\xa0of\xa0Beneficial\xa0Ownership' does not contain the substring
    # "amount and nature", so it scored as a weak generic "amount" and LOST the
    # shares tiering to a "Common Shares of <Issuer>" title column, putting
    # shares_idx on the NAME column and dropping all 17 rows of
    # 0001466593-25-000049 (found by the full-population A/B).
    # No-op for the Item 402(c) path, which already folds these in ``_sct_norm``
    # and ``_split_name_position``.
    decoded = _UNICODE_SPACE_RE.sub(" ", decoded)
    if block_breaks:
        # AFTER the entity/unicode-space folding above, so an empty block's
        # ``&nbsp;`` is already a plain space and breaks the run — see
        # ``_SENTINEL_RUN_RE``.
        decoded = _SENTINEL_RUN_RE.sub(_BREAK_SENTINEL, decoded).replace(_BREAK_SENTINEL, "\n")
    # Zero-width spacers are deliberately NOT stripped here (#2164). They are
    # scrubbed at the three points that consume a cell's MEANING —
    # ``_parse_share_count``, ``_parse_percent`` and
    # ``_clean_beneficial_holder_name`` — and nowhere upstream of table
    # SELECTION. This function feeds header text into ``_score_table_headers``,
    # ``_looks_like_label_row`` and the two-row-header promotion, so scrubbing
    # here changes which table wins: on 0001558370-25-003243 the Item 403(b)
    # table's header row is ENTIRELY U+200B, scores 0, and its window falls
    # below the floor, so the loop moves on to the window holding the 403(a)
    # 5%-holder table at 15. Emptying that header promoted the caption row, the
    # table scored 4, the EARLIER window then cleared the floor and won first,
    # and Vanguard / BlackRock / Dimensional / Harris / Victory were lost. The
    # full-population A/B measured 35 such genuine holders lost across ~12
    # accessions. The window loop takes the FIRST qualifying window rather than
    # the best — that is #2160's subject, and this ticket must not perturb it.
    return _INLINE_WHITESPACE_RE.sub(" ", decoded).strip()


def _scan_outer_tables(raw_html: str, *, start: int = 0, end: int | None = None) -> list[tuple[int, int]]:
    """Return ``(start, end)`` offsets for every OUTERMOST
    ``<table>...</table>`` block in ``raw_html`` between the given
    bounds. Mirrors :func:`app.services.business_summary._scan_outer_tables`
    but adds optional bounds so the caller can scope the scan to the
    section window after locating the heading.
    """
    if end is None:
        end = len(raw_html)
    spans: list[tuple[int, int]] = []
    pos = start
    depth = 0
    span_start = -1
    while pos < end:
        open_match = _TABLE_OPEN_RE.search(raw_html, pos, end)
        close_match = _TABLE_CLOSE_RE.search(raw_html, pos, end)
        if open_match is None and close_match is None:
            break
        if open_match is not None and (close_match is None or open_match.start() < close_match.start()):
            if depth == 0:
                span_start = open_match.start()
            depth += 1
            pos = open_match.end()
        else:
            assert close_match is not None
            depth -= 1
            if depth == 0 and span_start != -1:
                spans.append((span_start, close_match.end()))
                span_start = -1
            elif depth < 0:
                depth = 0
                span_start = -1
            pos = close_match.end()
    return spans


# ---------------------------------------------------------------------------
# Section locator
# ---------------------------------------------------------------------------


# Window of HTML to scan for the beneficial-ownership table after
# the section heading. Half a megabyte is enough for any DEF 14A
# table even on the largest filers (Atlassian's iXBRL DEF 14A is
# ~1.5MB total; the section + table fit in a 500KB tail).
_SECTION_SCAN_BYTES: Final[int] = 500 * 1024


def _is_inside_table(raw_html: str, position: int) -> bool:
    """True when ``position`` falls inside an open ``<table>`` block.

    Counts ``<table`` / ``</table`` tags before ``position`` — if
    open > close, the position is inside a table cell. Used to
    filter out section-heading regex matches that surface inside
    table data cells (e.g. a row whose text reads ``"5% Holders"``
    as a mid-table subheading) — those are not real headings and
    should not anchor the section locator. Codex pre-push review
    caught this on the multi-pass fix.
    """
    prefix = raw_html[:position]
    opens = sum(1 for _ in _TABLE_OPEN_RE.finditer(prefix))
    closes = sum(1 for _ in _TABLE_CLOSE_RE.finditer(prefix))
    return opens > closes


def _find_section_windows(raw_html: str) -> list[tuple[int, int]]:
    """Find candidate byte ranges for the beneficial-ownership
    section, in priority order.

    Returns a list of ``(start, end)`` windows that the table
    scorer tries in sequence. The first window whose best table
    meets the score floor wins.

    Priority order:

      1. **Last heading match** — handles the TOC trap. Real DEF
         14As open with a Table of Contents listing every section
         heading verbatim; the actual section header is the last
         occurrence in the document.
      2. **First heading match** — handles the in-cell false
         positive. Some tables have a row whose text reads
         ``"5% Holders"`` (mid-table subheading); that pattern
         matches our heading regex so last-match is wrong in that
         case but first-match (the real ``<h2>`` in proxy header)
         is correct.
      3. **Whole document** — handles small DEF 14As that inline
         the table without a dedicated section heading.

    Codex pre-push review identified the TOC trap; the in-cell
    false positive surfaced when fixing it. Multi-pass falls back
    cleanly across both.
    """
    # Filter out heading matches that occur inside an open
    # ``<table>`` — those are mid-table subheading rows
    # (e.g. ``"5% Holders"`` in a cell that splits officers from
    # principals), not actual section headings. Without this
    # filter, an in-cell match could anchor a window that starts
    # mid-ownership-table and miss the real table entirely.
    matches = [m for m in _SECTION_HEADING_RE.finditer(raw_html) if not _is_inside_table(raw_html, m.start())]
    windows: list[tuple[int, int]] = []
    seen_starts: set[int] = set()

    if matches:
        # Last match — TOC fix.
        last_start = matches[-1].start()
        last_end = min(last_start + _SECTION_SCAN_BYTES, len(raw_html))
        windows.append((last_start, last_end))
        seen_starts.add(last_start)
        # First match — fallback when the last match doesn't yield a
        # scoring table (e.g. a heading mention in body prose
        # without a following table). Skip when last == first.
        first_start = matches[0].start()
        if first_start not in seen_starts:
            first_end = min(first_start + _SECTION_SCAN_BYTES, len(raw_html))
            windows.append((first_start, first_end))
            seen_starts.add(first_start)

    # Whole-document fallback always tried last.
    if 0 not in seen_starts:
        windows.append((0, len(raw_html)))
    return windows


def _extract_as_of_date(raw_html: str, *, window_start: int, window_end: int) -> date | None:
    """Find the ``"as of <date>"`` phrase nearest the section heading.

    Scans the windowed slice (the heading + ~500KB tail). Returns
    ``None`` when no recognisable date phrase is found.
    """
    text = _strip_inline_html(raw_html[window_start:window_end])
    match = _AS_OF_DATE_RE.search(text)
    if match is None:
        return None
    raw_date = match.group(1).strip().rstrip(",")
    for fmt in ("%B %d, %Y", "%B %d %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_date, fmt).date()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Table scoring + row extraction
# ---------------------------------------------------------------------------


# Header substring → score weight. Higher weight = more diagnostic
# of the beneficial-ownership table specifically (vs e.g. the
# stock-options table, the executive-compensation table).
_HEADER_KEYWORDS: Final[tuple[tuple[str, int], ...]] = (
    ("beneficial", 4),
    ("amount and nature", 3),  # SEC-prescribed column heading
    ("number of shares", 3),
    ("name and address", 2),
    ("name of beneficial", 3),
    ("percent of class", 3),
    ("percentage of", 2),
    ("shares owned", 2),
    ("shares beneficially", 3),
    ("name", 1),  # weak — only fires when paired with a stronger keyword
    ("shares", 1),
    ("percent", 1),
    ("%", 1),
)


# Captions PRESCRIBED by Reg S-K Item 402 for the equity-award tables —
# 402(d) Grants of Plan-Based Awards and 402(f) Outstanding Equity Awards at
# Fiscal Year-End — and used by NO Item 403 beneficial-ownership table. A
# header carrying one of these is disqualified outright (#2140).
#
# Why a hard disqualifier rather than a penalty: the two table families share
# the phrase "number of shares", so an award table can out-score a genuine
# ownership table on keyword weight alone. That is not hypothetical — folding
# unicode spaces (also #2140) made "Number\xa0of\xa0Shares of Stock or Units"
# start matching, which lifted Hershey's 402(d) grants table from 2 to 5 and
# beat the real ownership table at 4, taking 26 holders down to 7 rows of
# grant data (0000047111-25-000035, found by the full-population A/B).
#
# Deliberately narrow — every marker is a multi-word Item 402 caption with no
# Item 403 collision. "exercise price" is safe where a bare "exercisable"
# would NOT be: Hershey's real ownership table has an "Exercisable Stock
# Options" column.
#
# 402(g) added by #2158. Item 402(g)(2) (17 CFR § 229.402(g)(2)) prescribes the
# "Option Exercises and Stock Vested" captions "Number of Shares Acquired on
# Exercise", "Value Realized on Exercise", "Number of Shares Acquired on
# Vesting", "Value Realized on Vesting". Those tables were previously invisible
# to the scorer because their column labels sit under a spanning "Option Awards
# | Stock Awards" row and #2140's label-arm promotion left them unscored; once
# that row is folded in (this ticket's D14) they score 5 on "number of shares"
# + "name" + "shares", clearing the floor of 3 and able to outrank a genuine
# Item 403 table. The full-population promotion audit found this class in 47 of
# the first 125 promoted header shapes — a hazard created by D14, so it is
# fixed in the same change.
#
# No Item 403 collision: Item 403 reports a HOLDING as of a date, never an
# exercise or vesting EVENT, so it has no occasion to caption a column with
# either phrase. "acquired on exercise" is the safe form where a bare "exercise"
# would not be (see the "Exercisable Stock Options" note above).
#
# ``value realized`` is deliberately left BROADER than the two paired markers
# (review round 1 WARNING). Full-population check over all 42,505 stored bodies:
# it fires on 227 tables / 135 distinct header shapes that carry NEITHER paired
# marker, and **zero** of those shapes contain any Item 403 caption
# ("beneficial owner" / "name and address" / "percent of class" / "amount and
# nature"). They are the phrase's other Item 402 homes — 402(g) wordings the
# prescribed captions do not cover ("Value Realized Upon Exercise", "Value
# Realized Upon Acceleration") and 402(j) termination / change-in-control tables
# ("Cash Severance", "Value Realized from Equity Acceleration"). Narrowing it to
# "value realized on" would re-admit all of them. Note also that scoring reads
# ``score_headers`` only, so a footnote or data cell mentioning the phrase can
# never reach this check.
_ITEM_402_AWARD_MARKERS: Final[tuple[str, ...]] = (
    "grant date",
    "estimated future payouts",
    "exercise price",
    "expiration date",
    "unexercised",
    "incentive plan award",
    "payout value",
    "market value of shares",
    "acquired on exercise",
    "acquired on vesting",
    "value realized",
)


# D4 (#2160) — Item 403's VALUE-column signature.
#
# Source rule: 17 CFR 229.403 prescribes column 3 "Amount and nature of
# beneficial ownership" and column 4 "Percent of class". Column 4 is
# CLASS-denominated by definition — a fraction of a class of securities. An
# Item 402 compensation table's percent is of *salary*, *target*, *payout* or
# *vesting*, never of a class. That is the discriminator row identity cannot
# supply: comp-table rows ARE people ('Kevin R.M. Smith', 'Dr. Hou'), so they
# score owner_identity_fraction = 1.00 and pass D2 untouched.
#
# Measured on the full admit cohort (spec, census pass 2): the gate takes
# 1,668 -> 45 candidate tables, and the survivors are the genuine shapes.
_ITEM403_CLASS_PCT_RE: Final[re.Pattern[str]] = re.compile(
    r"(percent|percentage|%)\s*(of\s+)?(the\s+)?(all\s+|total\s+|outstanding\s+)*"
    # The class-noun RUN is possessive (``*+``) so the engine cannot backtrack
    # into it: without that, 'Percentage of Common Stock Earned' matches by
    # consuming only 'Common', seeing 'Stock' in the allowed-follow set, and
    # succeeding -- the exact leak this lookahead exists to close.
    r"(?:class|common|shares?|stock|voting|ownership|beneficial|equity|units?)"
    r"(?:\s+(?:class|common|shares?|stock|voting|ownership|beneficial|equity|units?"
    r"|rights?|power|securities|interests?))*+"
    # 229.403 column 4 is "Percent of CLASS" -- the denominator is a CLASS OF
    # SECURITIES, so the class-noun run ENDS the denominator phrase. A CLOSED
    # rule, not a blocklist: only punctuation, a footnote marker, or a
    # continuation preposition may follow. Any trailing participle makes it a
    # percent of an OUTCOME -- 'Percentage of Shares Earned', 'Percentage of
    # Stock Options Vesting' (Codex ckpt-1 HIGH). This arm is STRONG, admitting
    # ahead of the Item 402 veto, so its precision is load-bearing: every one of
    # those shapes was being emitted as beneficial ownership.
    r"(?=\s*($|[|(),.;:%*†#\d]|of\b|outstanding\b|beneficially\b|owned\b))",
    re.IGNORECASE,
)
# Column 3 subdivided. Rule 13d-3 (17 CFR 240.13d-3) defines beneficial
# ownership as voting OR investment power, so issuers legitimately split
# "Amount and nature" into Sole/Shared voting and dispositive power columns and
# carry no separate percent column at all. ``_resolve_columns`` already tiers
# Sole|Shared|Total. Without this arm those tables fail D4 and are rejected.
_ITEM403_AMOUNT_NATURE_RE: Final[re.Pattern[str]] = re.compile(
    r"(amount\s+and\s+nature)|((sole|shared)\s+(voting|dispositive|investment))",
    re.IGNORECASE,
)
# Column 3's ordinary caption — issuers writing "Shares Beneficially Owned" or
# "Beneficial Stock Ownership" are quoting 229.403's own noun phrase.
#
# Keyed on own(ed|ership), NOT owner. "Name of Beneficial OWNER" is column 2's
# caption and says nothing about the VALUE columns, so accepting it as Item 403
# evidence admits 'Beneficial Owner | Number of RSUs' — a junk shape from this
# spec's own probe list. Gap-tolerant but confined within a single header cell
# (no "|"), so it cannot bridge two unrelated captions.
_ITEM403_BENEFICIAL_RE: Final[re.Pattern[str]] = re.compile(
    r"beneficial(ly)?\b[^|]{0,30}?\bown(ed|ership)\b",
    re.IGNORECASE,
)
# 229.403 column 2's LITERAL prescribed caption — "Name and address of
# beneficial owner" (403(a)) / "Name of beneficial owner" (403(b)).
#
# Bare "Beneficial Owner" is deliberately NOT enough (it admits 'Beneficial
# Owner | Number of RSUs'), but the full prescribed phrase is Item 403's own
# and no Item 402 table uses it. This arm is what recovers tables whose VALUE
# column is just the class name — 'Name and Address of Beneficial Owner |
# Common Stock' carries no percent and no "owned" anywhere, yet it is the
# reg's own shape with column 1 (Title of class) as the value caption.
_ITEM403_OWNER_CAPTION_RE: Final[re.Pattern[str]] = re.compile(
    r"name\s+(and\s+address(es)?\s+)?of\s+(the\s+)?beneficial\s+owner",
    re.IGNORECASE,
)
# 229.403 column 1's literal prescribed caption. Both subsections prescribe it
# and nothing else in a proxy uses the phrase, so it is Item 403 on its face —
# it recovers tables whose remaining captions are degraded to empty cells
# ('| Title of Class | ... | Name and Address of | Class A Common Stock (2) |').
_ITEM403_TITLE_OF_CLASS_RE: Final[re.Pattern[str]] = re.compile(r"title\s+of\s+class", re.IGNORECASE)
# The reg's adverb on its own. Issuers truncate column 3 to 'Shares
# Beneficially' when the header wraps, losing 'Owned' to the next cell. Weak,
# so it is paired with an amount indicator and stays under the vetoes.
_ITEM403_BENEFICIALLY_RE: Final[re.Pattern[str]] = re.compile(r"\bbeneficially\b", re.IGNORECASE)
# WEAK evidence — some amount column, some percent column, something "owned".
# Individually meaningless; an Item 402 payout table has them too. Only used in
# combination, and only under the vetoes below.
_ITEM403_AMOUNT_IND_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(shares?|number|amount|units?|stock)\b",
    re.IGNORECASE,
)
# Word-bounded so "Percentile" (TSR payout-curve tables) does not read as a
# percent-of-class column.
_ITEM403_PERCENT_IND_RE: Final[re.Pattern[str]] = re.compile(r"\bpercent(age|ages)?\b|%", re.IGNORECASE)
_ITEM403_OWNED_IND_RE: Final[re.Pattern[str]] = re.compile(r"\bown(ed|ership)\b", re.IGNORECASE)
# Rule 13d-3(d)(1)(i) (17 CFR 240.13d-3) DEEMS a person the beneficial owner of
# securities they have the right to acquire "within sixty days". A column
# captioned "Options Exercisable or Vesting Within 60 Days" is therefore quoting
# the ownership rule, NOT Item 402 — so this is STRONG Item 403 evidence and
# must outrank the comp veto, which would otherwise fire on "vesting".
#
# The ACQUISITION VERB is required, not just the phrase. 13d-3(d)(1)(i) is about
# securities a person has "the right to acquire" within sixty days; the bare
# phrase also appears in change-in-control and termination tables, and admitting
# those before the Item 402 veto would emit severance data as ownership (Codex
# ckpt-1 HIGH). Verb and window must sit in the SAME header cell.
_RULE_13D3_60_DAY_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:(?:exercisab|acquir|issuab|vest|convert|settl)\w*[^|]{0,40}?(?:with)?in\s+(?:\d{1,3}|sixty)\s+days"
    r"|(?:with)?in\s+(?:\d{1,3}|sixty)\s+days[^|]{0,40}?(?:exercisab|acquir|issuab|vest|convert|settl)\w*)",
    re.IGNORECASE,
)
# Comp-denominated percents — Item 402, not Item 403.
_COMP_PCT_RE: Final[re.Pattern[str]] = re.compile(
    r"(base\s+salary|of\s+target|target\s+bonus|payout|vesting"
    r"|bonus\s+opportunity|salary|earned|performance"
    # Item 402 is titled "Executive COMPENSATION"; 229.403 column 4 is a percent
    # OF CLASS, never of compensation. Board-attendance tables are Item 407.
    # Added when the data-row fallback (weak evidence) began admitting
    # 'Percentage of Annual Total Direct Compensation' and
    # 'Attendance Percentage of All Meetings'.
    r"|compensation|attendance|meetings?\s+attended|dilution"
    r"|\bpsus?\b|\bltip\b|incentive\s+award|as\s+settled"
    r"|long.?term\s+incentive|incentive\s+(program|opportunity)|base\s+pay|\bbonus\b"
    r"|option\s+awards?|stock\s+option\s+awards?"
    # Ownership GUIDELINES (a policy multiple, not a holding) and deferred-comp
    # ALLOCATION tables (a percent of a dollar deferral across funds) were the
    # only junk left riding the data-row fallback.
    r"|ownership\s+guidelines|amount\s+deferred|deferred\s+amount"
    # Item 402 OUTCOME words. A percent of an outcome (earned / vested /
    # achieved / at target) is 402, never 229.403 col 4 (Codex ckpt-1 HIGH).
    r"|\bearned\b|\bvested\b|achievement|attained|achieved|at\s+target)",
    re.IGNORECASE,
)
# Item 402(a)(3) DEFINES "named executive officer"; Item 403 says "name of
# beneficial owner". A header captioning its name column with Item 402's term of
# art, carrying none of Item 403's column-3/4 wording, is an Item 402 table.
_ITEM402_NEO_CAPTION_RE: Final[re.Pattern[str]] = re.compile(r"named\s+executive\s+officer", re.IGNORECASE)
# ...but 229.403(b) itself requires the table to cover "each of the registrant's
# directors, each of the nominees for election as a director, each of the named
# executive officers ... and directors and executive officers as a group". So a
# caption naming DIRECTORS alongside NEOs is Item 403(b)'s OWN wording and must
# not be vetoed: 'Directors and Named Executive Officers (1) | Number of shares
# of Common Stock | %' is a genuine 403(b) table this veto was emptying.
# Only a caption naming NEOs and nobody else is Item 402's term of art.
_ITEM403B_DIRECTOR_CAPTION_RE: Final[re.Pattern[str]] = re.compile(
    r"\bdirectors?\b|\bnominees?\b|as\s+a\s+group",
    re.IGNORECASE,
)


def _item403_value_signature(headers: tuple[str, ...], *, data_row_evidence: bool = False) -> bool:
    """True when HEADERS carry Item 403's prescribed VALUE columns.

    Precedence, not a flat vocabulary match — and the ordering is the whole
    design (#2160, measured in both directions over all 42,566 stored bodies):

    1. A header quoting 229.403's own column 3 ("amount and nature of beneficial
       ownership", "shares beneficially owned") or column 4 ("percent of class"),
       or Rule 13d-3(d)(1)(i)'s "within 60 days" acquisition window, is an
       Item 403 table **on its face** and is admitted outright.
    2. Otherwise Item 402 vocabulary vetoes.
    3. Otherwise the weak generic pair — an amount column plus either a percent
       column or an "owned" column — admits.

    The comp veto CANNOT be applied over the whole header, which is what the
    first cut did. Rule 13d-3(d)(1)(i) DEEMS a person the beneficial owner of
    shares acquirable within 60 days, so a genuine Item 403 table legitimately
    captions columns "Options Exercisable or Vesting Within 60 Days" and
    "Number of Performance Shares Granted". A blanket veto on "vesting" /
    "performance" deleted 18-, 22- and 10-holder Vanguard / BlackRock / First
    Eagle tables. Ordering the reg's own wording above the veto is what fixes
    it, and it is why the veto can safely stay broad.

    Step 3's two pairs both exist because issuers omit column 4 outright:
    dual-class and direct/indirect tables caption their value columns
    "Class A Common Stock Owned | Class B Common Stock Owned | Total Voting
    Power" with no percent anywhere.
    """
    joined = " | ".join(headers)
    if (
        _ITEM403_BENEFICIAL_RE.search(joined)
        or _ITEM403_OWNER_CAPTION_RE.search(joined)
        or _ITEM403_AMOUNT_NATURE_RE.search(joined)
        or _ITEM403_CLASS_PCT_RE.search(joined)
        or _ITEM403_TITLE_OF_CLASS_RE.search(joined)
        or _RULE_13D3_60_DAY_RE.search(joined)
    ):
        return True
    if _COMP_PCT_RE.search(joined) or (
        _ITEM402_NEO_CAPTION_RE.search(joined) and not _ITEM403B_DIRECTOR_CAPTION_RE.search(joined)
    ):
        return False
    if data_row_evidence:
        # Step 3b — the captions carry nothing, but the ROWS show both of
        # 229.403's value columns parsing. Below the vetoes by construction.
        return True
    return bool(
        _ITEM403_AMOUNT_IND_RE.search(joined)
        and (
            _ITEM403_PERCENT_IND_RE.search(joined)
            or _ITEM403_OWNED_IND_RE.search(joined)
            or _ITEM403_BENEFICIALLY_RE.search(joined)
        )
    )


def _score_table_headers(headers: tuple[str, ...]) -> int:
    """Score a candidate table's header row. Higher is better.

    Returns 0 for a table carrying an Item 402 EQUITY-AWARD caption, however
    well it otherwise scores — see :data:`_ITEM_402_AWARD_MARKERS`.
    """
    if not headers:
        return 0
    joined = " ".join(headers).lower()
    if any(marker in joined for marker in _ITEM_402_AWARD_MARKERS):
        return 0
    score = 0
    for keyword, weight in _HEADER_KEYWORDS:
        if keyword in joined:
            score += weight
    return score


@dataclass(frozen=True)
class _RawTable:
    """Internal carrier from ``_parse_table_html``.

    ``score_headers`` is what the table scorer reads to decide if
    this is the beneficial-ownership table — it merges parent-row
    keywords with sub-header keywords when a two-row header layout
    is detected, so the SEC-prescribed phrase ``"Amount and Nature
    of Beneficial Ownership"`` from the parent row keeps boosting
    the score even after the sub-row is promoted to canonical
    column labels.

    ``column_headers`` is what ``_resolve_columns`` reads to map
    canonical columns (Name / Shares / Percent) to indices. In the
    single-row-header case ``column_headers == score_headers``; in
    the two-row case ``column_headers`` is just the sub-row so the
    ``Total`` sub-column wins over ``Sole`` / ``Shared``.

    ``line_rows`` (#2358) is ``rows`` re-rendered with ``<br>`` and every block
    element as a LINE BREAK. Same shape, same order, index-for-index — the
    row-drop mask and the two-row-header trim are taken from the FLAT grid and
    applied to both, so ``line_rows[i]`` is always ``rows[i]``. It exists as a
    parallel grid rather than replacing ``rows`` because header scoring and
    column resolution substring-match multi-word captions and would break on an
    interior newline; see :func:`_strip_inline_html`. Read by
    :func:`_split_stacked_holder_row` and nothing else.
    """

    score_headers: tuple[str, ...]
    column_headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    line_rows: tuple[tuple[str, ...], ...]
    # The table's own markup, kept so :func:`_layout_percent_by_row` can rebuild
    # a true table-model grid on demand (#2376). Defaulted because it is read
    # ONLY by that rescue: an empty string disables it and changes nothing else,
    # which is what keeps ``scripts/ab_2140_def14a_parser.py``'s reconstruction
    # of this dataclass valid.
    table_html: str = ""


_NUMERIC_LIKE_RE: Final[re.Pattern[str]] = re.compile(r"\d{2,}")

# A cell that IS a numeric value — share count, percent, dash-with-footnote —
# as opposed to a caption that merely mentions a number. Used by
# :func:`_looks_like_label_row`; the legacy arm keeps ``_NUMERIC_LIKE_RE``
# verbatim so #2140's Sole/Shared/Total behaviour does not move.
_NUMERIC_VALUE_CELL_RE: Final[re.Pattern[str]] = re.compile(r"^[\s$(\[<*—–-]*\d[\d,.\s%]*[\s$)\]>*%—–-]*$")

# Column-label classes for two-row-header detection (#2140 D2). A row must
# match at least TWO distinct classes to be treated as the real header row —
# see :func:`_looks_like_subheader` for why substring matching and a
# single-class match are both unsafe.
# Item 403's prescribed AMOUNT captions. A header carrying one of these is the
# share-count column even if it also mentions a percent (issuers merge the two
# into a single column). Deliberately excludes the generic ``total`` /
# ``shares`` / ``number`` / ``amount`` tiers, which are too weak to override a
# percent caption. Kept in sync with the strong tiers of ``SHARES_TIERS`` in
# :func:`_resolve_columns`.
_STRONG_SHARES_KEYWORDS: Final[tuple[str, ...]] = (
    "amount and nature",
    "shares beneficially",
    "shares owned",
    "number of shares",
)

# Legacy (pre-#2140) trigger, preserved VERBATIM as substring matching so the
# Sole/Shared/Total merged-header behaviour is unchanged by this ticket.
_SUBHEADER_SUBDIVISION_KEYWORDS: Final[tuple[str, ...]] = (
    "sole",
    "shared",
    "total",
    "voting",
    "dispositive",
)
_SUBHEADER_LABEL_CLASSES: Final[tuple[tuple[str, re.Pattern[str]], ...]] = (
    ("name", re.compile(r"\b(?:name|owner|stockholder|shareholder|holder)\b", re.IGNORECASE)),
    (
        "amount",
        re.compile(r"\b(?:shares?|number|amount|sole|shared|total|voting|dispositive)\b", re.IGNORECASE),
    ),
    ("percent", re.compile(r"\bpercent(?:age)?\b|%", re.IGNORECASE)),
)


def _looks_like_subheader(cells: tuple[str, ...]) -> bool:
    """True when a row is a header continuation rather than a data row.

    Two shapes qualify:

      * the pre-#2140 SEC-prescribed subdivisions of the "Amount and Nature of
        Beneficial Ownership" merged header (``Sole`` / ``Shared`` / ``Total``
        / ``Voting`` / ``Dispositive``) — :func:`_looks_like_legacy_subheader`;
      * a genuine column-LABEL row under a spanning row 0 (#2140 D2) —
        :func:`_looks_like_label_row`.

    Callers that promote a row must keep the two apart: only the legacy arm may
    fold the promoted row into ``score_headers``. See ``_parse_table_html``.
    """
    return _looks_like_legacy_subheader(cells) or _looks_like_label_row(cells)


def _looks_like_legacy_subheader(cells: tuple[str, ...]) -> bool:
    """Pre-#2140 sub-header test: an all-text row carrying one of the
    SEC-prescribed subdivisions of the "Amount and Nature of Beneficial
    Ownership" merged header. Preserved VERBATIM (substring matching) so this
    ticket does not move the Sole/Shared/Total behaviour, and kept SEPARATE
    from :func:`_looks_like_label_row` because only this arm may combine both
    rows into ``score_headers``."""
    if not cells:
        return False
    for c in cells:
        if _NUMERIC_LIKE_RE.search(c):
            return False
    joined = " ".join(cells).lower()
    return any(k in joined for k in _SUBHEADER_SUBDIVISION_KEYWORDS)


def _looks_like_label_row(cells: tuple[str, ...]) -> bool:
    """True when ``cells`` is a genuine COLUMN-LABEL row (#2140 D2).

    Stricter than :func:`_looks_like_subheader`'s legacy arm, because this
    predicate also authorises promotion when the parent header is merely the
    SAME width as the data rows (see ``_parse_table_html``), where a false
    positive would silently discard a real data row.

    The real label row must name the HOLDER column (Item 403 always has one)
    AND label at least one value column.

    Requiring the name class specifically is load-bearing, not decoration: a
    bare ">= 2 of {name, amount, percent}" test also promotes performance-
    award tables — CYH's PSU row ('% of Target Achieved', '% of Granted
    Shares Earned', '', 'Percentile Rank', …) matches percent + amount, and
    promoting it inflated that table's score_headers enough to beat the real
    ownership table, silently swapping which table the parser reads.
    """
    if not cells:
        return False
    # A DATA cell is a numeric VALUE; a LABEL cell is prose that may happen to
    # contain a number. Testing "contains a 2+ digit run" (#2140's guard,
    # retained verbatim on the legacy arm) conflates the two and rejects the one
    # numeric literal Item 403 captions legitimately carry: Rule 13d-3(d)(1)(i)
    # deems a person the beneficial owner of securities acquirable "within sixty
    # days", so issuers caption a column "Options Exercisable within 60 days of
    # April 1, 2025". That caption row scores 15 once promoted — but the digit
    # guard rejected it, the table stayed at 4 on its spanning title alone, and
    # the sibling gate then dropped the entire Item 403(b) management
    # subsection (#2158: 13 holders on 0001437749-25-011586, 48 full-pop).
    # Footnote markers are stripped first — ``'1,000,000 [2]'`` is a share
    # count, and without the strip it reads as prose and a data row whose
    # holder is literally named "… Holder" gets promoted over its own table.
    for c in cells:
        if _NUMERIC_VALUE_CELL_RE.match(_FOOTNOTE_RE.sub("", c).strip()):
            return False
    # A label row labels SEPARATE columns, so it needs at least two non-empty
    # cells and its label classes must come from DIFFERENT cells. Testing the
    # joined text instead accepts a single-cell SECTION HEADING: '5% Stockholders'
    # matches percent (via '%') and name (via 'stockholders') at once, and
    # promoting it over the real header wrecked the table — column resolution
    # collapsed, the ownership table stopped scoring, and an Item 402(f)
    # equity-awards table won selection instead (0001628280-25-020660, found by
    # the full-population A/B). Single-cell heading rows are already handled by
    # ``_detect_role_heading`` in the row loop.
    non_empty = [c for c in cells if c.strip()]
    if len(non_empty) < 2:
        return False
    per_cell = [{cls for cls, pattern in _SUBHEADER_LABEL_CLASSES if pattern.search(c.lower())} for c in non_empty]
    labelled_cells = [classes for classes in per_cell if classes]
    if len(labelled_cells) < 2:
        return False
    matched = set().union(*labelled_cells)
    return "name" in matched and len(matched) >= 2


class _ExpandedRow(NamedTuple):
    """One row after :func:`_expand_row_spans`.

    ``own_cells`` is the row's own ``<td>``s, unshifted; ``inherited_cells`` is
    what a live ``rowspan`` places into the row; ``cells`` is both, in layout
    order. ``inherited_cells`` is empty exactly when the row inherited nothing.
    """

    cells: tuple[str, ...]
    own_cells: tuple[str, ...]
    inherited_cells: tuple[str, ...]


def _expand_row_spans(rows: list[tuple[tuple[str, int, int], ...]]) -> list[_ExpandedRow]:
    """Restore a ``rowspan`` cell into the rows it covers, at the right position.

    ROWS is one ``(text, rowspan, colspan)`` triple per ``<td>``/``<th>`` in
    document order. Returns one text tuple per row.

    Source rule — the HTML Living Standard's table model (§4.9.12 "Forming a
    table", *downward-growing cells*): a cell whose ``rowspan`` is N occupies the
    same COLUMN SLOT in the next N-1 rows, so those rows carry fewer ``<td>``s in
    the markup and every later cell in them sits further LEFT than its own markup
    position suggests. Reading ``<td>`` position as column index — which this
    parser did — mis-columns every continuation row of a spanned table.

    Concretely (#2175, ``0001104659-25-029081``, Liberty Media): the holder cell
    carries ``rowspan="6"`` over the issuer's six share series, so rows 2-6 lose
    three leading cells. ``Title of Series`` then landed on ``name_idx`` and the
    parser stored ``LLYVB`` / ``FWONK`` / ``LLYVK`` as beneficial owners, with the
    share count read from whichever cell the value-recovery scan reached.

    Two properties are deliberate, and BOTH were forced by the full-population
    A/B — the naive form (markup-index carry, every row expanded) regressed three
    accession classes:

    1. **``colspan`` is read but NOT expanded.** A carried cell is placed by
       LAYOUT column, which needs the colspan arithmetic; but it is emitted ONCE,
       not ``colspan`` times. Expanding it would multiply captions inside
       ``score_headers``, and :func:`_score_table_headers` SUMS keyword weights —
       so a ``colspan=6`` caption would score six times and reshuffle table
       selection corpus-wide. Ignoring colspan entirely is equally wrong: the
       carry index is then a markup index, and on ``0001628280-26-025998``
       (``colspan=3|6`` before a ``rowspan=4`` caption) it inserted the caption
       three columns early, wrecked the promoted header and took a 16-holder
       Item 403 table to ZERO.
    2. **A ``<tr>`` carrying no cells of its own emits nothing, but still consumes
       one row of every live span.** Consuming is the table model and is what
       keeps the span decaying at the right rate. Emitting is not: EDGAR's
       generated markup is full of cell-less spacer ``<tr>``s (the same accession
       has two before its header row and one after), every cell such a row could
       show is a repeat of one already emitted above, and materialising it
       inserted a phantom one-cell row between the two header rows — which the
       two-row-header promotion then adopted as ``column_headers``.

    Reuse check (standard-filing rule). ``pandas.read_html`` implements the full
    model and was tested on that exact Liberty table: pandas 3.0.2 returns a
    uniform 24-column frame with ``Chase Carey Director`` repeated across all six
    series rows, where this parser returned 24 cells then 21. Not adoptable
    wholesale — it also expands colspan (see 1) and re-does cell-text extraction,
    which every header-scoring constant here is tuned against. So the ALGORITHM
    is mirrored from ``pandas.io.html._HtmlFrameParser._expand_colspan_rowspan``
    (a remainder list drained ahead of each source cell) and the extraction is
    left alone.

    ``rowspan="0"`` (HTML: "span to the end of the row group") is treated as 1,
    matching the pandas reference. EDGAR proxies do not use it and inventing a
    to-end-of-table span for a malformed attribute is the riskier reading.
    """
    out: list[_ExpandedRow] = []
    # (layout_column, columns_spanned, text, rows_left), ascending by column.
    remainder: list[tuple[int, int, str, int]] = []
    for cells in rows:
        if not any(text for text, _, _ in cells):
            # Spacer row: consume a row of every live span, emit nothing. Covers
            # BOTH shapes EDGAR generates — a `<tr>` with no cells at all, and one
            # whose cells are all blank (`<tr><td>&#8203;</td></tr>`). Codex
            # checkpoint 2 (P2) caught the second: the first draft tested `not
            # cells`, so a blank-cell spacer crossed by a span was materialised
            # into a phantom row instead. Both shapes contribute nothing of their
            # own, and ``main`` dropped both on its `any(c for c in cells)` filter,
            # so treating them alike is what keeps this a pure re-COLUMNING of the
            # rows main already saw.
            remainder = [(col, width, text, left - 1) for col, width, text, left in remainder if left > 1]
            out.append(_ExpandedRow((), (), ()))
            continue
        texts: list[str] = []
        inherited: list[str] = []
        next_remainder: list[tuple[int, int, str, int]] = []
        column = 0
        pending = iter(remainder)
        carried = next(pending, None)
        for text, rowspan, colspan in cells:
            # Layout slots claimed by an earlier row's spanning cell come first.
            while carried is not None and carried[0] <= column:
                col, width, prev_text, left = carried
                texts.append(prev_text)
                inherited.append(prev_text)
                column = max(column, col) + width
                if left > 1:
                    next_remainder.append((col, width, prev_text, left - 1))
                carried = next(pending, None)
            texts.append(text)
            if rowspan > 1:
                next_remainder.append((column, colspan, text, rowspan - 1))
            column += colspan
        while carried is not None:
            col, width, prev_text, left = carried
            texts.append(prev_text)
            inherited.append(prev_text)
            if left > 1:
                next_remainder.append((col, width, prev_text, left - 1))
            carried = next(pending, None)
        out.append(_ExpandedRow(tuple(texts), tuple(text for text, _, _ in cells), tuple(inherited)))
        # A span opened by THIS row can start left of one carried into it, so the
        # append order is not sorted; the drain above requires that it is.
        next_remainder.sort(key=lambda entry: entry[0])
        remainder = next_remainder
    return out


def _row_contributes_only_inherited_values(row: _ExpandedRow) -> bool:
    """True when ROW inherited a cell and contributes no value of its OWN.

    #2175, third regression class. Issuers put ``rowspan="2"`` on a holder's
    VALUE cells and stack the holder's name and address as two ``<tr>``s under
    it. Restoring the span hands the address row the holder's own figures, and
    it stores as a second holder at an identical share count —
    ``0000107140-24-000176`` gained 'New York, NY 10055' at BlackRock's
    6,782,743 / 14.97% and 'Baker Botts L.L.P. 2001 Ross Avenue…' at E.P.
    Hamilton's 462,338 / 1.02%. A name test cannot separate these: the Baker
    Botts line is a law firm's name and address, indistinguishable in isolation
    from a genuine nominee holder.

    The markup separates them. A row whose every number is INHERITED is a
    continuation of the row that opened those spans — it is one holder rendered
    over two lines, not two holders. A row with a value of its own is a holder
    (Liberty's per-series continuation rows carry their own share counts).

    ⚠ This test makes the value-carry class strictly non-regressive against
    ``main``: for any row with an inherited cell, ``main`` saw the same ``<tr>``
    with its own cells only, so ``main`` kept it exactly when it had an own value
    — the same decision this makes. What changes for a kept row is its column
    ALIGNMENT, which is the point of the expansion.

    ⚠ The inherited cells must themselves be VALUES. Testing only "no own value"
    also matched the second row of a two-row HEADER, whose own cells are captions
    and which inherits a spanning caption — dropping it left the table with no
    label row and took the same two accessions to zero a second time.
    """
    if not row.inherited_cells:
        return False
    if not any(_is_value_cell(cell) for cell in row.inherited_cells):
        return False
    return not any(_is_value_cell(cell) for cell in row.own_cells)


def _is_value_cell(cell: str) -> bool:
    """True when CELL parses as an Item 403 share count or percent."""
    return _parse_share_count(cell) is not None or _parse_percent(cell) is not None


def _table_inner_html(table_html: str) -> str | None:
    """TABLE_HTML's own body, with every NESTED table blanked out.

    Extracted verbatim from :func:`_parse_table_html` so the layout grid below
    reads exactly the same bytes the main parse does -- the two must agree on
    which ``<tr>``s belong to this table, or a nested table's rows would appear
    in one grid and not the other.
    """
    open_match = _TABLE_OPEN_RE.search(table_html)
    close_idx = table_html.rfind("</table")
    if open_match is None or close_idx == -1:
        return None
    inner = table_html[open_match.end() : close_idx]
    nested = _scan_outer_tables(inner)
    if not nested:
        return inner
    pieces: list[str] = []
    cursor = 0
    for start, end in nested:
        pieces.append(inner[cursor:start])
        pieces.append(" ")
        cursor = end
    pieces.append(inner[cursor:])
    return "".join(pieces)


def _parse_table_html(table_html: str, *, expand_spans: bool = True) -> _RawTable | None:
    """Extract one ``<table>`` block. Mirrors the helper in
    business_summary but kept inlined so this module is provider-
    side / self-contained (parsers should not import from
    services).

    Detects two-row header tables. Some DEF 14As use a merged top
    header (``Name | Amount and Nature of Beneficial Ownership |
    Percent``) with a sub-header (``Sole | Shared | Total``)
    underneath. When the row-0 header has fewer cells than the
    median data row AND row 1 looks like a sub-header (all-text,
    contains column-label keywords), promote row 1 to canonical
    headers so the column resolver can find ``Total``. Codex
    pre-push review caught this on PR review.
    """
    scrubbed = _table_inner_html(table_html)
    if scrubbed is None:
        return None
    # Two passes, and the ORDER is load-bearing: the row-span expansion below
    # counts rows, so every ``<tr>`` must reach it — including the all-empty
    # spacer rows the old single pass dropped here. Dropping a row first would
    # make an earlier ``rowspan`` decay one row too fast and shift the rows
    # after it back the way this expansion exists to fix.
    spanned_rows: list[tuple[tuple[str, int, int], ...]] = []
    line_spanned_rows: list[tuple[tuple[str, int, int], ...]] = []
    for tr_match in _TR_RE.finditer(scrubbed):
        cells = _CELL_RE.findall(tr_match.group(1))
        spans = [
            (
                int(rs.group(1)) if (rs := _ROWSPAN_RE.search(attrs)) else 1,
                max(1, int(cs.group(1))) if (cs := _COLSPAN_RE.search(attrs)) else 1,
            )
            for attrs, _ in cells
        ]
        flat_cells: list[tuple[str, int, int]] = []
        line_cells: list[tuple[str, int, int]] = []
        for (_, inner), (rs, cs) in zip(cells, spans, strict=True):
            flat = _strip_inline_html(inner)
            # A cell carrying NO line-structure tag renders identically both
            # ways — provably, not approximately: every rewrite `block_breaks`
            # performs is keyed on one of these tags, and the sentinel-run
            # collapse needs a sentinel that only they can introduce. So the
            # second pass is skipped, which is most cells. Measured before and
            # after on 250 payloads, paired against the control checkout under
            # the same load; without it the second `_strip_inline_html` was a
            # real cost on the rewash path (#2171's surface).
            line = _strip_inline_html(inner, block_breaks=True) if _LINE_STRUCTURE_TAG_RE.search(inner) else flat
            flat_cells.append((flat, rs, cs))
            line_cells.append((line, rs, cs))
        spanned_rows.append(tuple(flat_cells))
        line_spanned_rows.append(tuple(line_cells))
    # The two grids MUST stay index-aligned, so the drop decisions are taken on
    # the FLAT grid and the same indices are applied to the line grid. Deriving
    # them independently would desync: ``_row_contributes_only_inherited_values``
    # asks ``_parse_share_count``, and a glued ``'118,028 165,426'`` parses on
    # the flat side and does not on the line side — which is the whole point of
    # the second grid.
    if expand_spans:
        flat_expanded = _expand_row_spans(spanned_rows)
        line_expanded = _expand_row_spans(line_spanned_rows)
        kept = [
            index
            for index, row in enumerate(flat_expanded)
            if any(row.cells) and not _row_contributes_only_inherited_values(row)
        ]
        cells_per_row: list[tuple[str, ...]] = [flat_expanded[index].cells for index in kept]
        line_per_row: list[tuple[str, ...]] = [line_expanded[index].cells for index in kept]
    else:
        kept = [index for index, row in enumerate(spanned_rows) if any(text for text, _, _ in row)]
        cells_per_row = [tuple(text for text, _, _ in spanned_rows[index]) for index in kept]
        line_per_row = [tuple(text for text, _, _ in line_spanned_rows[index]) for index in kept]
    if not cells_per_row:
        return None

    parent_headers = cells_per_row[0]
    body = cells_per_row[1:]
    line_body = line_per_row[1:]
    column_headers = parent_headers
    score_headers = parent_headers

    # Two-row header detection: when row 0 is narrower than the
    # data rows AND row 1 looks like a sub-header continuation,
    # the canonical column labels come from row 1 (so ``Total``
    # wins over ``Sole`` / ``Shared``) but score still considers
    # the parent row's SEC-prescribed keywords (so the table is
    # still recognisable as the beneficial-ownership table). Codex
    # pre-push review caught the missing parent-row score combine.
    if body:
        max_data_width = max(len(r) for r in body)
        # Two promotion arms (#2140 D2):
        #   * NARROWER parent + any sub-header shape — the original
        #     merged-header case (Sole/Shared/Total under a spanning cell).
        #   * SAME-WIDTH parent + a strict column-LABEL row. An issuer can
        #     render a header row that is full width yet still not the label
        #     row: 0001308179-25-000615 has row 0
        #     ('Name and Address of Beneficial Owner', '', '', '',
        #     'Number of Shares Beneficially Owned') over row 1
        #     ('5% or more Stockholders', '', 'Number', '', 'Percentage'),
        #     both 5 cells. The strict `<` test left row 0 as the header, so
        #     shares resolved onto the PERCENT column and all 20 real holders
        #     (Vanguard/FMR/BlackRock…) were dropped. Full-population A/B
        #     caught this — 159 accessions lost rows, 20 → 0 here.
        #     `_looks_like_label_row` (name class REQUIRED + a value class +
        #     no multi-digit cell) is what makes the looser width test safe.
        # NOTE: the legacy arm tests ``_looks_like_legacy_subheader``, NOT
        # ``_looks_like_subheader`` — the latter now also delegates to
        # ``_looks_like_label_row``, so using it here would let a label row
        # take the legacy path and re-inflate ``score_headers`` with the very
        # combine this arm exists for, resurrecting the equity-awards
        # mis-selection (0001628280-25-020660).
        legacy_arm = len(parent_headers) < max_data_width and _looks_like_legacy_subheader(body[0])
        label_arm = len(parent_headers) <= max_data_width and _looks_like_label_row(body[0])
        if legacy_arm or label_arm:
            column_headers = body[0]
            # BOTH arms combine the two rows for SCORING (#2158). Item 403
            # prescribes the CAPTIONS, not a layout, so wherever the issuer puts
            # them is the row that identifies the table — and when row 0 is the
            # share-class row that 403(a)'s "any class" disclosure produces, the
            # captions are entirely in row 1. Scoring row 0 alone took
            # 0000908311-26-000065 to 0 where its caption row scores 13, and 181
            # accessions returned zero rows as a result.
            #
            # #2140 deliberately did NOT combine on the label arm, because a
            # generic label row ('Name', 'Grant Date', 'Number of securities
            # underlying unexercised options…') belongs to the Item 402(f)
            # Outstanding Equity Awards table as readily as to Item 403, and
            # folding it lifted that table to a tie with the real ownership
            # table (0001628280-25-020660: 20 real holders → 0). That is
            # INVERTED by ``_ITEM_402_AWARD_MARKERS``, which #2140 itself added
            # later: the fold now feeds the disqualifier the very text that
            # identifies an award table, so the cited counter-example scores 0
            # rather than tying. Do not restore the split without re-checking
            # that disqualifier — see the #2158 prevention-log entry on
            # promoted rows and scorer identity evidence.
            score_headers = parent_headers + body[0]
            body = body[1:]
            line_body = line_body[1:]

    return _RawTable(
        score_headers=score_headers,
        column_headers=column_headers,
        rows=tuple(body),
        line_rows=tuple(line_body),
        table_html=table_html,
    )


def _layout_rows(table_html: str) -> list[dict[int, str]]:
    """TABLE_HTML as a proper table-model grid: one ``{layout column: text}`` per row.

    Source rule -- the HTML Living Standard's table model (SS4.9.12 "Forming a
    table"). DEF 14A carries no structured-data mandate (sec-edgar skill SS2.2's
    form table: "narrative HTML; no structured-XBRL mandate"), so the markup's own
    model IS the source rule for which caption governs which cell; there is no
    XBRL tagging to consult and nothing here is inferred from first principles.

    Differs from :func:`_expand_row_spans` in exactly two ways, both required to
    answer "which caption is above this cell":

    1. ``colspan`` is EXPANDED -- a cell spanning N columns occupies N slots.
       ``_expand_row_spans`` deliberately emits it once, because its output feeds
       ``score_headers`` and :func:`_score_table_headers` SUMS keyword weights, so
       a ``colspan=6`` caption would score six times. That constraint does not
       apply here: this grid is never scored and never selects a table.
    2. Self-closing ``<td/>`` cells are read (see :data:`_ANY_CELL_RE`), so
       ``colspan`` is taken off the tag that carries it.

    Verified against ``pandas.read_html`` 3.0.2 (the reuse oracle the #2175
    prevention entry names) on ``0001193125-25-103261``'s Item 403(a) table: both
    return 10 rows x 23 columns with ``Name and address(1)`` at column 2,
    ``Shares`` spanning 4-7, ``%(2)`` spanning 9-12, and the BlackRock row's
    ``23,308,871`` at column 6 and ``14.33`` at column 11. pandas is not adopted
    wholesale for the same reason #2175 gave -- it re-does cell-text extraction,
    which every header-scoring constant in this module is tuned against.
    """
    scrubbed = _table_inner_html(table_html)
    if scrubbed is None:
        return []
    out: list[dict[int, str]] = []
    # (layout_column, columns_spanned, text, rows_left), ascending by column.
    remainder: list[tuple[int, int, str, int]] = []
    for tr_match in _TR_RE.finditer(scrubbed):
        cells: list[tuple[str, int, int]] = []
        for match in _ANY_CELL_RE.finditer(tr_match.group(1)):
            if match.group(1) is not None:  # self-closing: an empty cell
                attrs, text = match.group(1), ""
            else:
                attrs, text = match.group(2), _strip_inline_html(match.group(3))
            rowspan = int(rs.group(1)) if (rs := _ROWSPAN_RE.search(attrs)) else 1
            colspan = max(1, int(cs.group(1))) if (cs := _COLSPAN_RE.search(attrs)) else 1
            cells.append((text, max(1, rowspan), colspan))
        row: dict[int, str] = {}
        next_remainder: list[tuple[int, int, str, int]] = []
        column = 0
        pending = iter(remainder)
        carried = next(pending, None)
        for text, rowspan, colspan in cells:
            while carried is not None and carried[0] <= column:
                col, width, prev_text, left = carried
                for slot in range(col, col + width):
                    row.setdefault(slot, prev_text)
                column = max(column, col) + width
                if left > 1:
                    next_remainder.append((col, width, prev_text, left - 1))
                carried = next(pending, None)
            for slot in range(column, column + colspan):
                row[slot] = text
            if rowspan > 1:
                next_remainder.append((column, colspan, text, rowspan - 1))
            column += colspan
        while carried is not None:
            col, width, prev_text, left = carried
            for slot in range(col, col + width):
                row.setdefault(slot, prev_text)
            if left > 1:
                next_remainder.append((col, width, prev_text, left - 1))
            carried = next(pending, None)
        next_remainder.sort(key=lambda entry: entry[0])
        remainder = next_remainder
        out.append(row)
    return out


def _is_percent_caption(text: str) -> bool:
    """True when TEXT is a caption naming Item 403's column 4, ``Percent of class``.

    Excludes the strong AMOUNT captions for the reason :func:`_resolve_columns`
    already records: issuers merge columns 3 and 4 into one ("Amount and Nature
    of Beneficial Ownership and Percent of Class"), and that single column holds
    the SHARE COUNT. Treating it as a percent column here would attest a share
    count as a percent, which is the one failure this helper must not have.
    """
    lowered = _FOOTNOTE_RE.sub("", text).strip().lower()
    if not lowered:
        return False
    if any(keyword in lowered for keyword in _STRONG_SHARES_KEYWORDS):
        return False
    # ``5% Beneficial owners`` is Item 403(a)'s SECTION LABEL, not column 4's
    # caption: there the sign binds to a QUANTITY on its left. Admitting it made
    # ExlService's own table look like it carried two different percent captions
    # and refused the very rows this ticket exists to recover.
    #
    # The test runs FIRST, ahead of the plain ``percent`` match, and that order
    # is the whole fix for the spelled-out spellings — ``5 Percent Beneficial
    # Owners`` and ``Ten Percent Holders`` are the same section label with the
    # sign written as a word, and a ``"percent" in lowered`` early return admits
    # them. Codex caught it at checkpoint 2, after the ``%`` half was already
    # fixed: the guard was written against the character, not the phenomenon.
    if _THRESHOLD_LABEL_RE.search(lowered):
        return False
    return "percent" in lowered or "%" in lowered


def _layout_name_key(text: str) -> str:
    """Normalised join key between an extracted holder and its raw grid row.

    Prefix-based, and it has to be: the extractor's ``holder_name`` has already
    had footnote markers and any trailing address removed, so ``"BlackRock, Inc.
    50 Hudson Yards New York, NY 10001"`` in the cell becomes ``"BlackRock,
    Inc."`` in the holder. An equality join on the full string matches neither.
    """
    collapsed = re.sub(r"[^a-z0-9]+", "", _clean_beneficial_holder_name(text).lower())
    return collapsed[:16]


def _layout_percent_by_row(table_html: str) -> dict[str, Decimal]:
    """Percent values attested by the TABLE MODEL, keyed by :func:`_layout_name_key`.

    For each data row, the value returned is the one sitting in a layout column
    covered by a percent CAPTION -- which is what makes a bare ``14.33`` (no
    ``%`` sign, no ``*``) unambiguous. The positional rescue in
    :func:`_extract_holder_rows` cannot accept such a cell, and is right not to:
    without the layout it cannot tell ``14.33`` from a 14-share holding.

    Fails closed in three places, because a wrong percent is worse than a null:

    * no caption row carrying a percent caption -> empty result;
    * a name key appearing on rows with DIFFERENT percent values -> that key is
      dropped, since the prefix join cannot say which row is the holder's. This
      is also what makes keying the row's first TWO text cells safe: a repeated
      ``Title of class`` label collides with itself and drops out;
    * a cell mixing a DIGIT with an asterisk -> declined, because ``1*`` against
      a ``* Less than 1.0%`` legend states a THRESHOLD, not a holding;
    * a cell that ``_parse_percent`` rejects -> no entry.
    """
    rows = _layout_rows(table_html)
    percent_columns: set[int] = set()
    captions: set[str] = set()
    header_index = -1
    for index, row in enumerate(rows):
        populated = [text for text in row.values() if text.strip()]
        if not populated:
            continue
        # A caption row states what the columns hold; a data row holds values.
        # Requiring no value cell keeps a holder called "Percentage Partners LP"
        # from being read as the header.
        if any(_NUMERIC_VALUE_CELL_RE.match(_FOOTNOTE_RE.sub("", text).strip()) for text in populated):
            continue
        columns = {column for column, text in row.items() if _is_percent_caption(text)}
        if columns:
            percent_columns |= columns
            captions |= {_FOOTNOTE_RE.sub("", row[column]).strip().lower() for column in columns}
            header_index = max(header_index, index)
    if not percent_columns:
        return {}
    # Everything below fails CLOSED, because a null percent is recoverable from
    # the filing and a wrong one is not. Two ways a header offers more than one
    # percent column, both measured on the full population:
    #
    # * TWO RUNS of the same caption — a dual-class table carries 229.403 column
    #   4 once per class. Regeneron (0001308179-25-000518) heads ``Number |
    #   Percent of Class`` over Class A Stock at layout columns 2-7 and again
    #   over Common Stock at 10-16; pooling them bound a Class A percent to a
    #   Common Stock share count and attested a flat 28 to fifteen directors
    #   holding between 4,472 and 268,499 shares.
    # * TWO DISTINCT CAPTIONS, which is why the scan above unions across EVERY
    #   caption row instead of stopping at the first. Domo (0001505952-25-000062)
    #   renders ``Shares | % | Shares | % | % of Total Voting Power``, and its
    #   first percent-captioned row carries only the voting-power caption — one
    #   clean contiguous run, so a contiguity test alone passes it and stores
    #   VOTING POWER as percent of class. Josh James' row reads
    #   ``3,263,659 100 1,022,375 2.8 78.5``: 78.5% of the vote, 2.8% of Class A.
    #
    # Binding a percent to the share count the extractor actually used needs the
    # whole grid re-columned, which is the wider ticket; per-class binding is
    # tracked on #2351.
    if len(captions) > 1:
        return {}
    if max(percent_columns) - min(percent_columns) + 1 != len(percent_columns):
        return {}

    found: dict[str, Decimal] = {}
    ambiguous: set[str] = set()
    for row in rows[header_index + 1 :]:
        # The FIRST TWO text cells, not the first — 17 CFR 229.403(a) prescribes
        # column 1 "Title of class" ahead of column 2 "Name and address of
        # beneficial owner", so on a table that renders column 1 the holder's
        # name is the SECOND text cell and keying only the first files the
        # percent under ``commonstock``. Caught by Codex at checkpoint 2 and
        # reproduced: ``Common Stock | Acme Capital LLC | 1,000 | 7.7`` recovered
        # nothing, because the lookup is by ``_layout_name_key(holder_name)``.
        #
        # Two is the reg's own bound, not a tuned constant, and registering the
        # class label costs nothing: it is either never looked up, or it repeats
        # across rows with different percents and the ambiguity guard below drops
        # it — which is the same mechanism, not a second one.
        name_keys: list[str] = []
        for column in sorted(row):
            text = row[column].strip()
            if not text or column in percent_columns:
                continue
            if _NUMERIC_VALUE_CELL_RE.match(_FOOTNOTE_RE.sub("", text).strip()):
                continue
            if (key := _layout_name_key(text)) and key not in name_keys:
                name_keys.append(key)
            if len(name_keys) == 2:
                break
        if not name_keys:
            continue
        percent: Decimal | None = None
        for column in sorted(percent_columns):
            text = row.get(column, "").strip()
            if not text:
                continue
            # A DIGIT beside an ASTERISK is the issuer writing "less than N%",
            # where the digit is the THRESHOLD and not the holding. Declined
            # outright, because ``_parse_percent`` strips a trailing footnote
            # marker and would read the threshold as the value.
            #
            # Found by the gain-side arm, not by reasoning: on
            # 0001437749-25-025111 the percent column renders ``1*`` against a
            # ``* Less than 1.0%`` legend, and seven holders would have stored a
            # flat 1 — a figure the filing does not state, and one the parser's
            # own settled convention for that meaning writes as 0.5 (a BARE
            # ``*``, which is unambiguous and is still accepted; Campbell Soup
            # 0001308179-25-000618 recovers thirteen of them correctly).
            #
            # ``5.2*`` — a real value carrying a footnote marker — is declined
            # by the same rule, and that is the intended trade: the two readings
            # are indistinguishable from the cell alone, and a null is
            # recoverable from the filing where a wrong percent is not.
            if "*" in text and any(character.isdigit() for character in text):
                percent = None
                break
            percent = _parse_percent(text)
            if percent is not None:
                break
        if percent is None:
            continue
        for name_key in name_keys:
            if name_key in found and found[name_key] != percent:
                ambiguous.add(name_key)
            found[name_key] = percent
    for key in ambiguous:
        found.pop(key, None)
    return found


# Footnote / asterisk markers stripped from holder-name cells. The
# raw cell often looks like ``"John Doe (1)"``, ``"John Doe[a]"``,
# ``"John Doe*"``, or ``"John Doe (*)"`` — the marker is dropped,
# the rest preserved.
#
# Three alternation branches:
#   1. Bracketed numeric / asterisk / single alphabetic letter:
#      ``(1)`` / ``[a]`` / ``[*]``. Single letter only — multi-letter
#      bracketed strings (``[abc]``) are rare in proxies and may be
#      legitimate suffixes (e.g. tickers in cross-references).
#   2. Trailing single asterisk(s): ``"name*"`` / ``"name**"``.
#   3. Parenthesised asterisks: ``"(*)"`` / ``"(**)"``.
#
# Codex pre-push review caught the prior version which only matched
# digits or asterisks inside brackets — alphabetic markers like
# ``[a]`` survived through the share-count parser and dropped the
# whole row.
_FOOTNOTE_RE: Final[re.Pattern[str]] = re.compile(r"\s*[\(\[](?:\d+|\*+|[a-zA-Z])[\)\]]|\s*\*+\s*$|\s*\(\*+\)")
_LESS_THAN_ONE_PERCENT_VALUE: Final[Decimal] = Decimal("0.5")


def _clean_holder_name(raw: str) -> str:
    """Strip footnote markers from the holder name; keep the rest.

    SHARED with the Item 402(c) SCT path (``_split_name_position``,
    ``_normalize_first_cell``, ``_looks_like_name_cell``) — do NOT add
    ownership-specific cleaning here. #2140 initially did, and
    ``_normalize_first_cell`` then fed a de-punctuated ``'EVP'`` into the
    stacked-title fragment join, turning HBNC's
    ``'EVP, Chief Financial Officer'`` into ``'EVP Chief Financial Officer'``
    (caught by ``test_hbnc_non_lexicon_second_row_fragment``). Beneficial-
    ownership-only cleaning belongs in
    :func:`_clean_beneficial_holder_name`.
    """
    return _FOOTNOTE_RE.sub("", raw).strip()


def _clean_beneficial_holder_name(raw: str) -> str:
    """Clean an Item 403 beneficial-ownership holder name.

    ``_clean_holder_name`` plus two repairs that must NOT reach the Item 402(c)
    path (see that function's docstring):

    1. **Flatten interior line breaks** (#2140 D5). ``_INLINE_WHITESPACE_RE``
       deliberately excludes ``\\n`` because the SCT name/title split needs it,
       but ``ownership_def14a_observations.holder_name_key`` is
       ``lower(trim(holder_name))`` and ``trim`` does not touch INTERIOR
       whitespace. A render wrap inside the cell therefore made
       ``'Michael\\n O. Johnson'`` and ``'Michael O. Johnson'`` two different
       holder identities, splitting one person across two rows of
       ``ownership_def14a_current`` — 704 rows / 117 instruments full-pop,
       51 of them Item 403(b) group rows.
    2. **Strip an unbracketed trailing footnote digit.** A superscript that
       carried no parentheses survives ``_FOOTNOTE_RE`` as a bare trailing
       number once the line break around it is flattened (MKTX:
       ``'BlackRock, Inc. 1'``, ``'The Vanguard Group 2'``).
       ``_clean_name_footnote`` is the repair the SCT path already applies to
       NEO names (#2094) — reused, not re-derived.
    """
    scrubbed = _ZERO_WIDTH_RE.sub("", raw)
    flattened = _INLINE_WHITESPACE_RE.sub(" ", _clean_holder_name(scrubbed).replace("\n", " ")).strip()
    return _clean_name_footnote(flattened)


# Beneficial-owner identity, POSITIVE test (#2164). Used ONLY to decide whether
# a value-less row that precedes an address row is a HOLDER NAME or a section
# heading — see :func:`_extract_holder_rows`'s stacked name/address recovery.
#
# Positive rather than a heading blocklist: a blocklist needs an entry per new
# heading wording, and the prevention log's rule on hand-enumerated tuples
# applies. This is deliberately narrow and scoped to that one decision; #2160
# generalises row identity into the table-SELECTION path.
_OWNER_ENTITY_RE: Final[re.Pattern[str]] = re.compile(
    r"\b("
    r"LLC|L\.L\.C|LP|L\.P|LLP|Inc|Incorporated|Corp|Corporation|Company|Ltd|Limited"
    r"|Trust|Fund|Funds|Partners|Partnership|Capital|Management|Advisers|Advisors"
    r"|Holdings|Associates|Ventures|Bank|N\.A|plc|GmbH|S\.A|AG|NV|PLC"
    r"|Foundation|Insurance|Investments?|Securities|Financial"
    r")\b",
    re.IGNORECASE,
)
# A person name at the START of the cell — NOT anchored at the end, because
# Item 403(a)'s column legitimately continues into an address, and issuers
# append titles ('Mr. Michael J. Gerdin, Chief Executive Officer, Chairman,
# President and Director'). The trailing optional comma covers the
# surname-first rendering issuers use for alphabetised management tables.
_OWNER_PERSON_RE: Final[re.Pattern[str]] = re.compile(
    r"^[A-Z][A-Za-z.'’-]*,?(?:\s+(?:[A-Z][A-Za-z.'’-]*|[A-Z]\.|van|von|de|del|der|di|la|le),?)+"
)
# Item 403's own subsection wording. 403(a) covers "any person … who is known
# to the registrant to be the beneficial owner of more than five percent";
# 403(b) covers "each of the registrant's directors … each of the named
# executive officers". Issuers head the two blocks with exactly those CLASS
# nouns, PLURALISED — and a member of the class is named individually. That is
# the discriminator: 'Directors and Executive Officers:' names the class,
# 'Mr. Michael J. Gerdin, …, and Director' names one member of it.
#
# Necessary because a Title-Cased heading matches ``_OWNER_PERSON_RE`` —
# 'Other Shareowners that Beneficially Own More than 5%:' reads as the
# two-token name 'Other Shareowners' (caught by this ticket's own unit test,
# not in review). Checked BEFORE the entity arm, so 'Directors and Executive
# Officers of the Company' cannot be rescued by its trailing 'Company'.
_HOLDER_CLASS_PLURAL_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(?:share(?:holders|owners)|stockholders|holders|owners|directors|trustees"
    r"|officers|executives|nominees|persons)\b",
    re.IGNORECASE,
)


# D1 (#2160) — corrections the spec's measurement forced on the row-identity
# predicate when it moved from #2164's single-cell use into table SELECTION.
#
# 1. LENGTH CAP. Sized for an Item 403(a) "name AND address" cell, the longest
#    legitimate form. Without it, Schedule 13G footnote PARAGRAPHS ("Based
#    solely on an amendment to a Schedule 13G filed by BlackRock…", "Consists of
#    10,500 shares held directly…") pass the person-name test and were extracted
#    as holder names.
_OWNER_IDENTITY_MAX_LEN: Final[int] = 120
# 2. Entity designators, CASE-SENSITIVE. An Item 403 owner is a proper noun:
#    'Smith Family Trust' matches, the instrument types 'trust interests' and
#    'allocation interests' do not. ``_OWNER_ENTITY_RE`` is deliberately
#    IGNORECASE and load-bearing for #2164's stacked-pair path, so selection
#    gets its own stricter variant rather than tightening that one.
_OWNER_ENTITY_CASED_RE: Final[re.Pattern[str]] = re.compile(
    r"\b("
    r"LLC|L\.L\.C|LP|L\.P|LLP|Inc|Incorporated|Corp|Corporation|Company|Ltd|Limited"
    r"|Trust|Fund|Funds|Partners|Partnership|Capital|Management|Advisers|Advisors"
    r"|Holdings|Associates|Ventures|Bank|N\.A|plc|PLC|GmbH|S\.A|AG|NV"
    r"|Foundation|Insurance|Investments?|Securities|Financial"
    r")\b"
)
# 3. INSTRUMENT-TYPE vocabulary. A name composed ENTIRELY of equity/award nouns
#    is a security, not a beneficial owner under Rule 13d-3. A closed
#    vocabulary set, deliberately not a table-shape blocklist — the
#    prevention-log rule on hand-enumerated tuples targets the latter.
_INSTRUMENT_VOCAB: Final[frozenset[str]] = frozenset(
    {
        "equity",
        "equities",
        "stock",
        "stocks",
        "share",
        "shares",
        "option",
        "options",
        "warrant",
        "warrants",
        "unit",
        "units",
        "restricted",
        "unrestricted",
        "deferred",
        "performance",
        "incentive",
        "award",
        "awards",
        "grant",
        "grants",
        "rsu",
        "rsus",
        "psu",
        "psus",
        "sar",
        "sars",
        "appreciation",
        "right",
        "rights",
        "common",
        "preferred",
        "ordinary",
        "class",
        "series",
        "voting",
        "nonvoting",
        "convertible",
        "note",
        "notes",
        "debenture",
        "debentures",
        "interest",
        "interests",
        "plan",
        "plans",
        "total",
        "subtotal",
        "authorized",
        "unissued",
        "issued",
        "outstanding",
        "treasury",
        "reserved",
        "available",
        "and",
        "or",
        "the",
        "of",
        "for",
        "but",
        "a",
    }
)
_WORD_RE: Final[re.Pattern[str]] = re.compile(r"[A-Za-z]+")
# Presentation debris the identity test must look through. Leader dots are an
# HTML table-of-contents/figure-rule artefact and land INSIDE the name cell, so
# they inflate its length without being part of the name.
_IDENTITY_DEBRIS_RE: Final[re.Pattern[str]] = re.compile(r"[.…]{3,}")
# D1 clauses 4-5 — Item 403(a) prescribes "Name AND ADDRESS of beneficial owner"
# in ONE column, so an entity that carries no corporate suffix is identified by
# the address that follows it: 'MUFG 4-5, Marunouchi 1-chome Chiyoda-ku, Tokyo',
# 'Vanguard 100 Vanguard Boulevard Malvern, PA', 'BlackRock 50 Hudson Yards'.
# None of them reach the two-capitalised-token person pattern (the second token
# is a street number) and none carry LLC/Inc/Trust, so without this arm all
# three are rejected and their table falls under _ROW_IDENTITY_FLOOR —
# 0001140361-25-012302 scored 0.25 and was emptied.
#
# A proper-noun run followed by a NUMBER, which is the name/street-number seam.
# Deliberately not a hardcoded issuer list (the spec's clause 5): the signal is
# the reg's own one-column name-and-address form, not who the filer is.
# ``_is_name_then_address`` implements this; it is defined next to
# ``_is_address_fragment`` so it can share ``_STREET_TYPE``, which this module
# declares further down.
_OWNER_NAME_THEN_ADDRESS_RE: Final[re.Pattern[str]] = re.compile(
    r"^[A-Z][\w&.'’-]*(?:\s+(?:[A-Z][\w&.'’-]*|&|and)){0,4}\s+\d(?P<tail>.*)$",
    re.DOTALL,
)


def _is_instrument_not_owner(text: str, *, strip_class_designator: bool = False) -> bool:
    """True when every word in TEXT is equity/award vocabulary.

    'Authorized But Unissued' is Title-Cased and matches the two-capitalised-
    token person pattern, so neither D1's person arm nor an address test rejects
    it. Rule 13d-3 makes the test principled: a beneficial owner is a person or
    entity holding voting or investment power, and an instrument is neither.

    #2176 — a CLASS DESIGNATOR is not a word. 17 CFR 229.403 column 1 is 'Title
    of class', and issuers spell its values 'Class A Common Stock' / 'Series B
    Preferred Stock' / 'Class AA'. The designator itself carries no meaning the
    vocabulary can hold, and leaving it out produced an absurd asymmetry:
    ``a`` is in ``_INSTRUMENT_VOCAB`` as a connective ARTICLE, so 'Class A
    Common Stock' tested as an instrument while 'Class B Common Stock' did not
    — and the latter then PASSED ``_is_beneficial_owner_identity`` and was
    stored as a beneficial owner. Codex checkpoint 2 caught it: the per-row
    guard prunes the Class A row, which RAISES the identity fraction, so a
    table of nothing but class labels could newly clear
    ``_ROW_IDENTITY_FLOOR`` on the strength of its Class B row alone.

    Scoped to names that actually name a class, so a stray initial elsewhere
    cannot make an unrelated name test as an instrument.

    ⚠ ``strip_class_designator`` is OFF by default and the STORAGE guard is its
    only caller, because this predicate is not private to that guard:
    ``_is_beneficial_owner_identity`` short-circuits on it, and that feeds
    ``_owner_identity_fraction`` and so ``_ROW_IDENTITY_FLOOR``. Turning the
    strip on unconditionally therefore narrows OWNER IDENTITY and de-admits
    tables — a selection change wearing a row-filter's clothes.

    Measured, which is the only reason this parameter exists: on
    ``0000062234-25-000015`` the strip flips 'Class B Shares' from owner to
    instrument, taking the table from 2/3 = 0.667 to 1/3 = 0.333. It falls under
    the floor and the whole table goes, including its 229.403(b) Instruction 5
    group row — the row this change exists to protect. Same on
    ``0000062234-26-000018``. Default-off keeps eligibility byte-identical to
    ``origin/main``; the guard opts in, where the only effect is which rows are
    STORED.
    """
    words = _WORD_RE.findall(text.lower())
    if not words:
        return False
    if strip_class_designator and ("class" in words or "series" in words):
        # The trigger token always SURVIVES its own filter (`class` / `series`
        # are both longer than 2), so this cannot empty `words` — 'Class B'
        # reduces to ['class'], which is in the vocabulary and returns True on
        # the line below. An earlier revision guarded an empty result here;
        # review NITPICK on PR #2373 showed the branch was unreachable.
        words = [w for w in words if len(w) > 2]
    return all(w in _INSTRUMENT_VOCAB for w in words)


# Heading vocabulary. A HEADING names a class in the abstract; a HOLDER carries a
# SPECIFIC proper name. That is the discriminator ``_HOLDER_CLASS_PLURAL_RE``
# lacks on its own -- it is a heading test (#2164: '5% Stockholders', 'Other
# Shareowners that Beneficially Own More than 5%') and it outranks the entity
# arm, so it was rejecting genuine 403(a) holders named in Schedule 13D/G
# joint-filer form: 'Trustees of the Thomas J. Pritzker Family Trusts and Other
# Reporting Persons', 'CIBC Caribbean and Other Reporting Persons'. Hyatt
# (0001104659-26-038759) lost an 11-holder sibling table that way, taking MFS,
# Baron Capital and the Pritzker trusts with it.
_HEADING_STOPWORDS: Final[frozenset[str]] = frozenset(
    {
        # class nouns already in _HOLDER_CLASS_PLURAL_RE
        "shareholders",
        "shareowners",
        "stockholders",
        "holders",
        "owners",
        "directors",
        "trustees",
        "officers",
        "executives",
        "nominees",
        "persons",
        # generic heading connectives and qualifiers
        "other",
        "certain",
        "all",
        "total",
        "more",
        "than",
        "and",
        "the",
        "of",
        "family",
        "trusts",
        "trust",
        "reporting",
        "beneficially",
        "own",
        "owned",
        "ownership",
        "group",
        "each",
        "who",
        "that",
        "are",
        "not",
        "as",
        "a",
        "current",
        "former",
        "named",
        "executive",
        "non",
        "employee",
        "employees",
        "affiliated",
        "entities",
        "associates",
        "including",
        "our",
        "its",
    }
)


# 'Thomas J. Pritzker' / 'Karen L. Pritzker' -- a middle initial is strong
# evidence of a personal name; 'CIBC' -- an all-caps token is strong evidence of
# an entity. Both survive inside a 13D/G joint-filer phrase where the
# start-anchored person pattern cannot reach.
_PERSONAL_INITIAL_RE: Final[re.Pattern[str]] = re.compile(r"\b[A-Z]\.")
_ALLCAPS_ENTITY_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"\b[A-Z]{3,}\b")


# A partnership-style firm name joined by '&' -- 'Dodge & Cox', 'Cohen & Steers',
# 'Ruane, Cunniff & Goldfarb'. These carry no corporate suffix, so the entity arm
# misses them, and the start-anchored person arm stops at the '&' (Codex ckpt-2
# P2). Common enough in Item 403(a) to matter: the corpus holds Cohen & Steers,
# Cooke & Bieler, Cede & Co, Bill & Melinda Gates Foundation Trust.
_AMPERSAND_FIRM_RE: Final[re.Pattern[str]] = re.compile(
    r"^[A-Z][\w.'’-]*(?:,?\s+[A-Z][\w.'’-]*)*,?\s+&\s+[A-Z]",
)


def _contains_specific_name(text: str) -> bool:
    """True when TEXT carries a run of >=2 capitalised tokens that name someone.

    Tokens drawn from :data:`_HEADING_STOPWORDS` do not count, so
    'Directors and Executive Officers of the Company' has no qualifying run while
    'Trustees of the Thomas J. Pritzker Family Trusts' has 'Thomas J. Pritzker'
    and 'CIBC Caribbean and Other Reporting Persons' has 'CIBC Caribbean'.
    """
    run = 0
    for token in re.findall(r"[A-Za-z][\w.'\u2019-]*", text):
        if token[:1].isupper() and token.lower().strip(".") not in _HEADING_STOPWORDS:
            run += 1
            if run >= 2:
                return True
        else:
            run = 0
    return False


def _is_beneficial_owner_identity(text: str) -> bool:
    """D1 (#2160) — row-identity test used to gate table SELECTION.

    Stricter than :func:`_is_owner_identity`, which #2164 uses for the
    single-cell stacked-pair decision. Both share the class-noun rejection; this
    one adds the three corrections the full-population census forced (length
    cap, case-sensitive entity designators, instrument vocabulary).

    Presentation debris is stripped BEFORE the length cap, not after. Issuers
    pad the name column with HTML leader dots to draw a dotted rule across to
    the figures, and the run is far longer than the name: 'Hotchkis & Wiley
    Capital Management, LLC ....(63 dots)' is 104 characters of dots on a
    40-character name. Testing the raw cell blew the 120-char cap and rejected
    the holder, which took the whole table below ``_ROW_IDENTITY_FLOOR`` and
    dropped a genuine ``Amount and Nature of Beneficial Ownership | Percent of
    Class`` table (0000074303-25-000056, 3 holders including BlackRock).
    """
    stripped = _IDENTITY_DEBRIS_RE.sub(" ", text).strip(" .,​﻿*†#")
    if not stripped or len(stripped) > _OWNER_IDENTITY_MAX_LEN:
        return False
    if _is_instrument_not_owner(stripped):
        return False
    if "as a group" in stripped.lower():
        return True
    if _HOLDER_CLASS_PLURAL_RE.search(stripped):
        # A heading names a class abstractly; a HOLDER carries a specific proper
        # name. Rescue only on BOTH a qualifying name run AND hard proper-noun
        # evidence -- a personal initial, an all-caps entity token, or a
        # corporate designator. Without the second test 'Compensation Discussion
        # and Analysis' (a table-of-contents row) reads as a name run.
        if not (
            _contains_specific_name(stripped)
            and (
                _PERSONAL_INITIAL_RE.search(stripped)
                or _ALLCAPS_ENTITY_TOKEN_RE.search(stripped)
                or _OWNER_ENTITY_CASED_RE.search(stripped)
            )
        ):
            return False
        return True
    if _OWNER_ENTITY_CASED_RE.search(stripped):
        return True
    if _AMPERSAND_FIRM_RE.match(stripped):
        return True
    if _OWNER_PERSON_RE.match(stripped):
        return True
    return _is_name_then_address(stripped)


def _is_owner_identity(text: str) -> bool:
    """True when TEXT names a beneficial owner (person, entity, or Item 403(b)
    Instruction 5's directors-and-officers-as-a-group aggregate).

    Rejects the section headings that share the same single-cell row shape —
    'Directors and Executive Officers:', 'Other Shareowners that Beneficially
    Own More than 5%:', '5% Stockholders' — which name the CLASS rather than a
    member of it.
    """
    stripped = text.strip()
    if not stripped:
        return False
    # Instruction 5's aggregate row legitimately carries the plural class nouns,
    # so it is settled before the class-noun rejection below.
    if "as a group" in stripped.lower():
        return True
    if _HOLDER_CLASS_PLURAL_RE.search(stripped):
        return False
    if _OWNER_ENTITY_RE.search(stripped):
        return True
    return bool(_OWNER_PERSON_RE.match(stripped))


def _parse_share_count(raw: str) -> Decimal | None:
    """Parse a share-count cell. Accepts ``"1,234,567"`` /
    ``"1234567"`` / ``"1,234,567(1)"`` / dash / em-dash / empty."""
    if not raw:
        return None
    # Zero-width spacers (#2164). iXBRL-rendered proxies pad value cells with
    # U+200B et al; ``str.strip()`` does not treat them as whitespace, so
    # '<ZWSP> 17,464 (2)' failed to parse and the row died on the "neither
    # shares nor percent parsed" guard (0001140361-26-008786: 18 raw rows, 0
    # holders). Scrubbed HERE and not in ``_strip_inline_html`` — see that
    # function for why touching header text changes table selection.
    raw = _ZERO_WIDTH_RE.sub("", raw)
    # Strip an unbracketed trailing footnote superscript BEFORE spaces are
    # removed (#2140): "52,606,862 1" is BlackRock's 52.6M holding with a
    # footnote marker, and collapsing the space first turned it into
    # 526,068,621 — a 10x overstatement (0000080661-25-000018).
    cleaned = _TRAILING_FOOTNOTE_RE.sub("", _FOOTNOTE_RE.sub("", raw).strip())
    cleaned = cleaned.strip().replace(",", "").replace(" ", "")
    if cleaned in ("", "-", "—", "–", "N/A", "n/a"):
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _parse_percent(raw: str) -> Decimal | None:
    """Parse a percent-of-class cell. Accepts ``"5.5%"`` /
    ``"5.5"`` / ``"*"`` (less than 1% per industry convention) /
    dash / empty.

    The lone-asterisk check happens BEFORE the footnote-stripping
    regex because that regex's trailing-asterisk branch would
    otherwise erase the cell content and return None — losing the
    less-than-1% signal the proxy explicitly conveys.

    Out-of-range guard (#1228): clamps to ``[Decimal(0), Decimal(100)]``.
    A real percent-of-class is bounded by definition (ownership is a
    fraction of total shares). Values outside that band are almost
    always a column-resolver misfire (positional fallback in
    ``_resolve_columns`` mapped a shares-count column into
    percent_idx). The schema is ``NUMERIC(8, 4)`` (max 9999.9999)
    which raises ``NumericValueOutOfRange`` on 7-digit shares values
    and previously aborted the whole batch in ``ingest_def14a``.
    Returning ``None`` lets shares parse independently and the row
    survives without a spurious percent.
    """
    if not raw:
        return None
    # Zero-width spacers (#2164) — '<ZWSP> *' is the less-than-1% marker and
    # must reach the lone-asterisk check below. Scrubbed here rather than in
    # ``_strip_inline_html``; see that function.
    cleaned = _ZERO_WIDTH_RE.sub("", raw).strip().replace("%", "").replace(",", "").strip()
    if cleaned in ("*", "**"):
        # Industry convention: ``*`` denotes "less than 1%" in the
        # proxy footnote. Persist as 0.5 so the cell is non-NULL but
        # operators can still distinguish it from a real 0.5%
        # holding (rare; the holder would then surface in Form 4
        # cumulative anyway).
        return _LESS_THAN_ONE_PERCENT_VALUE
    cleaned = _FOOTNOTE_RE.sub("", cleaned).strip()
    if cleaned in ("", "-", "—", "–"):
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    # #1228 — clamp to the natural [0, 100] band. See docstring.
    # NaN / Inf survive ``Decimal(cleaned)`` for inputs like ``"NaN"``
    # or ``"Infinity"``; comparison against finite Decimals would
    # raise ``InvalidOperation`` so reject them first.
    if value.is_nan() or value.is_infinite():
        return None
    if value < Decimal(0) or value > Decimal(100):
        return None
    return value


# Column-finder. DEF 14A tables vary in column order and labelling;
# the parser locates each canonical column by header substring and
# falls back to positional defaults (col 0 = name, col 1 = shares,
# col -1 = percent) when the headers are missing or ambiguous.
def _shares_cell_percent_signature(cells: list[str], shares_idx: int, headers: tuple[str, ...]) -> str | None:
    """Return the signature by which the cell resolved at ``shares_idx`` is a
    PERCENT rather than a share count, or ``None`` when it reads as a count.

    Source rule — 17 CFR 229.403 (Reg S-K Item 403) prescribes column 3 as
    "Amount and nature of beneficial ownership" (a COUNT of shares) and column 4
    as "Percent of class" (a percentage). They are two distinct columns, and the
    reg's own captions are the discriminator.

    The failure this guards (#2163): a header row carrying empty SPACER cells
    that its data rows do not carry is WIDER than the data, so ``shares_idx``
    lands one or more columns to the right — on the percent. The value still
    parses (``_parse_share_count('17.4')`` is a valid ``Decimal``), so the
    ``if shares is None`` recovery below never fires and the real count one cell
    to the left is discarded. 0001308179-24-000672 stored BlackRock at 17.4
    shares and threw away 6,236,345.

    ``'17.4%'`` in ONE cell is NOT this bug — ``_parse_share_count`` already
    rejects it. The defect needs the value and its sign in SEPARATE cells, which
    is what HTML table rendering produces.

    Two signature strengths, split on SEMANTIC vs POSITIONAL evidence:

    * ``"decisive"`` — the resolved CAPTION names a percent and carries none of
      ``_STRONG_SHARES_KEYWORDS``. The column says what it holds; that is not
      an artefact of anything else in the row, so the value is a percent and is
      never restored as a share count.
    * ``"weak"`` — a lone ``%`` sibling cell, or a non-integral value. Both are
      suggestive but circumstantial: a fractional holding is unusual and not
      impossible, and a ``%`` sign rendered in its OWN cell can sit to the right
      of a genuine small holding (``[name, '50', '%', '0.1']`` — Codex ckpt-2).
      The caller restores the original reading when the row offers no
      whole-number alternative, so a real count is never dropped on positional
      evidence alone.
    """
    raw = cells[shares_idx] if 0 <= shares_idx < len(cells) else ""
    value = _parse_share_count(raw)
    if value is None:
        return None
    # A percent of class is bounded by definition — ownership is a fraction of a
    # class — and ``_parse_percent`` has clamped to [0, 100] since #1228. A value
    # above that ceiling therefore CANNOT be the 229.403 column-4 percent no
    # matter what the surrounding cells look like, so it is never held back.
    #
    # Load-bearing, not defensive: without it a row rendered
    # ``[name, '1,234,567', '%', '5.6']`` — the sign cell BEFORE the percent
    # value — would read the sibling '%' as decisive, hold back a genuine
    # 1.2M-share holding, find no whole alternative (5.6 is fractional), and
    # drop the count entirely.
    if value.is_nan() or value.is_infinite() or value > Decimal(100) or value < Decimal(0):
        return None
    caption = headers[shares_idx].lower() if 0 <= shares_idx < len(headers) else ""
    if ("percent" in caption or "%" in caption) and not any(k in caption for k in _STRONG_SHARES_KEYWORDS):
        return "decisive"
    nxt = next((c.strip() for c in cells[shares_idx + 1 :] if c.strip()), "")
    if nxt in ("%", "(%)") or value != value.to_integral_value():
        return "weak"
    return None


def _resolve_columns(headers: tuple[str, ...]) -> tuple[int, int, int]:
    """Return ``(name_idx, shares_idx, percent_idx)``.

    Shares column resolution is tiered. Some DEF 14As subdivide the
    SEC-prescribed "Amount and Nature of Beneficial Ownership"
    column into ``Sole | Shared | Total`` voting-power sub-columns.
    A naive first-match-on-"shares"-or-"amount" picks ``Sole`` when
    the real total lives in the ``Total`` column. The tiered
    preference order is:

      1. ``"total"`` (explicit total column wins)
      2. ``"amount and nature"`` (SEC-prescribed merged-header text)
      3. ``"shares beneficially"`` / ``"shares owned"``
      4. ``"shares"`` / ``"number"`` / ``"amount"`` (weakest fallback)

    Codex pre-push review caught this — without the tiered
    preference, a Sole/Shared/Total/Percent layout reads ``Sole`` as
    shares and ``Shared`` as percent.

    Resolution order is ``shares`` → ``percent`` → ``name``, each step
    EXCLUDING the indices already claimed, so two columns can never collide
    (#2140 D1). ``percent_idx`` is ``-1`` when the table has no percent column
    distinguishable from the shares column — callers must treat a negative
    index as "absent", never as a Python end-relative index.

    Defaults to ``(0, 1, len(headers) - 1)`` when no header match
    fires.
    """
    percent_idx = -1
    # Tiered shares search — try strongest signal first, fall back.
    shares_idx = -1
    shares_tier_priority: list[tuple[str, int]] = []  # (header_substring, score)
    SHARES_TIERS: tuple[tuple[str, int], ...] = (
        ("total", 4),
        ("amount and nature", 3),
        ("shares beneficially", 3),
        ("shares owned", 3),
        ("number of shares", 2),
        ("shares", 1),
        ("number", 1),
        ("amount", 1),
    )

    # PERCENT first. A percent caption is the least ambiguous of the three —
    # "percent"/"%" appears in no legitimate name or amount caption — whereas
    # the shares tiering's generic ``total`` keyword happily matches
    # "Total as a Percentage of Shares Outstanding". Resolving shares first
    # and then dropping the collision let that percent column win shares_idx,
    # so the row parser read "5.0%" as a share count, found no percent, and
    # dropped EVERY row of such a table (Codex pre-push review caught this;
    # the same header shape previously survived with a percent-only row).
    # ...EXCEPT that a header carrying one of Item 403's strong AMOUNT captions
    # is the amount column even when it also names a percent, because issuers
    # merge the two: "Amount and Nature of Beneficial Ownership and Percent of
    # Class" is ONE column holding the share count. Letting percent claim it
    # left shares unresolved and dropped all 16 rows of 0001140361-25-008248
    # (found by the full-population A/B). The generic ``total`` tier is NOT
    # strong enough to qualify — "Total as a Percentage of Shares Outstanding"
    # is a real percent column.
    for i, h in enumerate(headers):
        lower = h.lower()
        if "percent" not in lower and "%" not in lower:
            continue
        if any(k in lower for k in _STRONG_SHARES_KEYWORDS):
            continue
        percent_idx = i
        break

    # SHARES next, excluding the percent column from the tiering entirely.
    for i, h in enumerate(headers):
        if i == percent_idx:
            continue
        lower = h.lower()
        # …and excluding every OTHER percent caption too, unless it carries a
        # strong AMOUNT keyword. This is the exact mirror of the rule the percent
        # pass above applies, and the same source rule drives it: 17 CFR 229.403
        # column 3 is a COUNT, column 4 a PERCENT. The percent pass takes only the
        # FIRST match, so on a multi-class table a SECOND percent caption stayed
        # eligible for the shares tiering — and ``total`` is a tier-4 keyword, so
        # '% of Total Voting Power' outranked the genuine 'Shares' column
        # (#2175, 0001628280-26-025998: Dustin Moskovitz's Class B count filed
        # against his Class A percent). The docstring above already states this
        # for the percent side — "'Total as a Percentage of Shares Outstanding'
        # is a real percent column" — it simply was not applied here.
        if ("percent" in lower or "%" in lower) and not any(k in lower for k in _STRONG_SHARES_KEYWORDS):
            continue
        for keyword, weight in SHARES_TIERS:
            if keyword in lower:
                shares_tier_priority.append((str(i), weight))
                break

    if shares_tier_priority:
        # Sort by weight DESC (highest tier first), then by original
        # column position ASC so the leftmost top-tier column wins.
        shares_tier_priority.sort(key=lambda x: (-x[1], int(x[0])))
        shares_idx = int(shares_tier_priority[0][0])

    # NAME next — still before any positional FALLBACK, because a fallback is
    # a guess and must never outrank real header evidence. Resolving the shares
    # fallback first let it claim column 1 blindly, and on a table whose name
    # caption genuinely sits at index 1 ("Name of Beneficial Owner") that stole
    # the name column and dropped all 18 rows (0001104659-25-025144, found by
    # the full-population A/B).
    #
    # Name LAST of the three EVIDENCE passes, with claimed indices excluded
    # (#2140 D1).
    #
    # Source rule — Reg S-K Item 403 (via Schedule 14A Item 6(d)) prescribes
    # BOTH "Name and address of beneficial owner" / "Name of beneficial owner"
    # (403(a)/(b)) AND "Amount and nature of beneficial ownership". The token
    # ``beneficial`` therefore appears in every caption on both sides of the
    # table and carries NO discriminating signal. Matching it (as this
    # function used to) makes the SHARES column win ``name_idx`` whenever the
    # issuer leaves the name caption blank — very common — so the share count
    # was persisted as ``holder_name`` on 3,209 rows (13.4% of the corpus).
    # Discriminate on the name-side tokens only.
    name_idx = -1
    NAME_KEYWORDS: Final[tuple[str, ...]] = (
        "name and address",
        "name of",
        "name",
        "beneficial owner",
        "stockholder",
        "shareholder",
        "holder",
    )
    for keyword in NAME_KEYWORDS:
        for i, h in enumerate(headers):
            if i in (shares_idx, percent_idx):
                continue
            if keyword in h.lower():
                name_idx = i
                break
        if name_idx != -1:
            break
    # POSITIONAL FALLBACKS last, for whatever header evidence did not resolve.
    if name_idx == -1:
        # Blank name caption (the common Item 403 layout) — the name sits in
        # column 0 unless that column is already claimed.
        name_idx = next((i for i in range(max(len(headers), 1)) if i not in (shares_idx, percent_idx)), 0)
    if shares_idx == -1:
        # Prefer the column just right of the name, then any other free one.
        shares_idx = next(
            (i for i in (name_idx + 1, 1, 0) if i < len(headers) and i not in (name_idx, percent_idx)),
            name_idx + 1,
        )
    if percent_idx == -1:
        # Positional fallback: the percent is conventionally the LAST column.
        # Pre-#2140 this was unconditional (`len(headers) - 1`) and could alias
        # the shares column; it is now guarded, but dropping it entirely lost
        # real percents whose caption lives in an unpromoted sub-header row
        # (0001437749-25-013824: 15 rows -> 4).
        last = len(headers) - 1
        if last >= 0 and last not in (name_idx, shares_idx):
            percent_idx = last
    return (name_idx, shares_idx, percent_idx)


# Section sub-heading detection within rows. Some issuers split the
# table into "Officers and Directors" + "5% Holders" with bold
# section-heading rows between groups. Any single-cell row whose
# text matches one of these patterns flips the role tag for
# subsequent rows.
_ROLE_HEADING_PATTERNS: Final[tuple[tuple[re.Pattern[str], str], ...]] = (
    (re.compile(r"\b(directors?|trustees?)\b.*\b(officers?|executives?)\b", re.IGNORECASE), "officer"),
    (re.compile(r"\bofficers?\s+and\s+directors?\b", re.IGNORECASE), "officer"),
    (re.compile(r"\bdirectors?\b", re.IGNORECASE), "director"),
    (re.compile(r"\bofficers?\b", re.IGNORECASE), "officer"),
    # Order-insensitive (#2164): the noun can precede the threshold. 'Other
    # Shareowners that Beneficially Own More than 5%:' matched nothing under the
    # old ``5\s*%.*holders?`` (noun first, and 'shareowners' not 'holders'), so
    # it fell through as a value-less data row, ``current_role`` stayed on the
    # management block above it, and the 403(a) 5% holder beneath inherited
    # 'director' — BlackRock at 9.96% filed into the insiders slice
    # (0001140361-26-008786). Two lookaheads rather than an alternation so the
    # two tokens may appear in either order.
    (
        # No LEADING word boundary, deliberately: issuers compound the noun —
        # '5% Equityholders', 'Unitholders', 'Noteholders' — and a leading \b
        # cannot match inside 'Equityholders', so anchoring it dropped the
        # 'principal' role from 40 holders (BlackRock, The Vanguard Group,
        # State Street on 0001193125-26-170704 / -25-096950 / -26-173833). The
        # pre-#2164 pattern was `5\s*%.*holders?`, which matched the suffix;
        # that behaviour must survive the order-insensitivity change. Found by
        # the full-population role audit (A/B arm 3), not by review.
        # The TRAILING \b is kept so 'ownership' does not match.
        re.compile(r"(?=.*5\s*%)(?=.*(?:holder|owner)s?\b)", re.IGNORECASE),
        "principal",
    ),
    (re.compile(r"principal\s+(?:share|stock)holders?", re.IGNORECASE), "principal"),
)
# NO ``group`` pattern here, deliberately (#2140 D4). Per Item 403(b) the
# "all directors and executive officers as a group" aggregate is a ROW, not a
# section: it carries its own share count and sits inside the management block.
# A heading match sets ``current_role`` for EVERY SUBSEQUENT ROW, so making a
# group pattern reachable here would turn that one row into a sticky 'group'
# context that mislabels the rest of the table — strictly worse than the
# unreachable-pattern state it replaced. Group detection is inline-only, in the
# row loop of :func:`parse_beneficial_ownership_table`.


def _detect_role_heading(cells: tuple[str, ...]) -> str | None:
    """If ``cells`` is a single-text section heading row, return the
    role tag for subsequent rows; else ``None``."""
    non_empty = [c for c in cells if c.strip()]
    if not non_empty:
        return None
    # A heading row is typically one cell or one cell plus a few empties.
    if len(non_empty) > 1:
        return None
    text = non_empty[0]
    for pattern, role in _ROLE_HEADING_PATTERNS:
        if pattern.search(text):
            return role
    return None


def _detect_inline_role(holder_name: str) -> str | None:
    """Heuristic: when the holder cell carries the role inline
    (e.g. ``"John Doe, CFO"`` / ``"Jane Smith — Director"``),
    return the role tag. Used as a fallback when section
    subheadings are missing."""
    if not holder_name:
        return None
    lower = holder_name.lower()
    if "as a group" in lower:
        return "group"
    role_keywords = (
        ("director", "director"),
        ("trustee", "director"),
        ("ceo", "officer"),
        ("cfo", "officer"),
        ("coo", "officer"),
        ("president", "officer"),
        ("chairman", "officer"),
        ("officer", "officer"),
    )
    for keyword, role in role_keywords:
        if keyword in lower:
            return role
    return None


# ---------------------------------------------------------------------------
# ESOP / employee-benefit-plan detection (#843)
# ---------------------------------------------------------------------------


# Conservative regex set per Codex round-1 sign-off
# (`.claude/codex-843-r1-review.txt`). Each pattern matches a
# canonical employee-benefit-plan label that proxies use when a plan
# crosses the 5% disclosure threshold and lands in the bene table.
#
# Explicitly NOT matched (false-positive guard): generic ``trust``,
# ``trustee``, ``trustee for`` alone — these surface on every Vanguard
# Fiduciary Trust / BlackRock Institutional Trust 5%-holder row and
# would over-tag.
#
# Spec: docs/superpowers/specs/2026-05-06-def14a-bene-table-extension-design.md
_ESOP_NAME_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\bESOP\b", re.IGNORECASE),
    re.compile(r"\bemployee\s+stock\s+ownership\s+plan\b", re.IGNORECASE),
    # ``\(?k\)?`` makes the parens optional but ``k`` was required —
    # but ``_clean_holder_name`` strips ``(k)`` as a footnote marker
    # so legacy stored holder_name reads ``401 Plan`` not ``401(k)
    # Plan``. Make the entire ``k``-suffix optional and require a
    # ``Plan`` suffix to bound the match (so a bare numeric ``401``
    # doesn't false-match). Codex pre-push review #843 round 5.
    re.compile(r"\b401(?:\s*\(?k\)?)?\s+plan\b", re.IGNORECASE),
    re.compile(r"\bemployee\s+savings\s+plan\b", re.IGNORECASE),
    re.compile(r"\bretirement\s+savings\s+plan\b", re.IGNORECASE),
    re.compile(r"\bprofit[-\s]sharing\s+plan\b", re.IGNORECASE),
    re.compile(r"\bemployee\s+benefit\s+plan\b", re.IGNORECASE),
    re.compile(r"\bcompany\s+stock\s+fund\b", re.IGNORECASE),
    re.compile(r"\b(?:savings|retirement|profit[-\s]sharing)\s+plan\s+trust\b", re.IGNORECASE),
)


def is_esop_plan(holder_name: str) -> bool:
    """True when ``holder_name`` matches any of the conservative
    ESOP-plan patterns. Used by the parser to override the
    section-derived ``holder_role`` for plan rows, and re-used by
    the ingester to decide whether to write through to
    ``ownership_esop_observations``."""
    if not holder_name:
        return False
    return any(pat.search(holder_name) for pat in _ESOP_NAME_PATTERNS)


# Trustee-suffix extraction. Proxy bene tables routinely format ESOP
# rows as ``"<plan name>, c/o <trustee> as Trustee"`` or
# ``"<plan name> Trust (<trustee>, Trustee)"``. We split on the
# common separators so the canonical ``plan_name`` is the issuer's
# plan identity and ``plan_trustee_name`` carries the third-party
# fiduciary (typically a Vanguard / Fidelity / Computershare entity
# that's resolvable against ``external_identifiers`` for cross-
# reference with the funds slice in #961).
_TRUSTEE_SUFFIX_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    # "<plan>, c/o <trustee> as Trustee"
    re.compile(r"^(?P<plan>.+?),\s*c/o\s+(?P<trustee>.+?)\s+(?:as\s+)?trustee\b.*$", re.IGNORECASE),
    # "<plan>, <trustee>, Trustee"
    re.compile(r"^(?P<plan>.+?),\s*(?P<trustee>.+?),\s*trustee\b.*$", re.IGNORECASE),
    # "<plan> (<trustee>, Trustee)"
    re.compile(r"^(?P<plan>.+?)\s*\(\s*(?P<trustee>.+?),\s*trustee\s*\).*$", re.IGNORECASE),
    # "<plan> by <trustee> as trustee"
    re.compile(r"^(?P<plan>.+?)\s+by\s+(?P<trustee>.+?)\s+as\s+trustee\b.*$", re.IGNORECASE),
)


def extract_plan_name_and_trustee(holder_name: str) -> tuple[str, str | None]:
    """Split a raw ESOP holder_name into ``(plan_name, trustee_name)``.

    When no trustee suffix is recognised, returns the holder_name
    as plan_name and ``None`` as trustee. The ingester treats a
    ``None`` trustee as "trustee unknown" — the row still lands in
    ``ownership_esop_observations`` with ``plan_trustee_name=NULL``,
    but the funds-slice overlay in #961 cannot tag it (no key to
    join against fund_filer_cik).
    """
    if not holder_name:
        return "", None
    cleaned = holder_name.strip()
    for pat in _TRUSTEE_SUFFIX_PATTERNS:
        m = pat.match(cleaned)
        if m is not None:
            plan = m.group("plan").strip().rstrip(",").strip()
            trustee = m.group("trustee").strip().rstrip(",").strip()
            return (plan, trustee or None)
    return (cleaned, None)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


# Score floor for a table to be a WINDOW CANDIDATE — below this we don't trust
# the match. Empirically tuned: a minimal-header beneficial-ownership table
# (``Name`` / ``Shares`` / ``Percent``) scores 3; tables with SEC-prescribed
# wording score 6+. Compensation / option-grant tables typically score 0-2 even
# when they include a "Name" column, because they lack ``shares`` / ``percent``
# cues.
#
# Module-level rather than a local of ``parse_beneficial_ownership_table``
# because the offline census scripts have to reproduce the same candidate set —
# ``scripts/census_def14a_stacked_cell_holders.py`` re-derived it as its own
# literal 3, and a local cannot be imported, so the two would have drifted the
# first time this moved (review NITPICK on PR #2359).
_WINDOW_SCORE_FLOOR: Final[int] = 3


def parse_beneficial_ownership_table(html_text: str) -> Def14ABeneficialOwnershipTable:
    """Parse a DEF 14A primary doc HTML body and extract the
    Item 12 beneficial-ownership table.

    Returns an empty-rows result when no candidate table scores
    above the floor. The ingester (PR 2) tombstones the accession
    in that case so a non-standard issuer layout doesn't tight-loop
    re-fetching the same proxy.

    Does not raise on malformed HTML — best-effort extraction. The
    surrounding ingester is responsible for fetch failures and for
    persisting the result (or its absence) to the audit log.
    """
    if not html_text:
        return Def14ABeneficialOwnershipTable(as_of_date=None, rows=[], raw_table_score=0)

    candidate_windows = _find_section_windows(html_text)
    best_score = 0
    best_table: _RawTable | None = None
    chosen_window: tuple[int, int] | None = None

    # Multi-pass: try each priority window in order; the first one
    # whose best table meets the floor wins.
    # Item 403 has TWO subsections — 403(a) >5% owners and 403(b) management +
    # the group aggregate — and issuers routinely render them as TWO SEPARATE
    # TABLES in the same section. Taking only the single best-scoring table
    # therefore drops one subsection outright, and which one survives is
    # decided by incidental header wording: 0001193125-26-119922 has a 2-row
    # 403(a) table scoring 14 and a 14-row 403(b) table scoring 12, and any
    # scoring tweak flips which is kept (the full-population A/B caught this as
    # 14 rows -> 0). So keep EVERY qualifying table in the winning window and
    # concatenate their rows; ``best_score`` stays the best single score for
    # tombstone diagnostics.
    qualifying: list[_RawTable] = []
    for window_start, window_end in candidate_windows:
        candidate_tables = _scan_outer_tables(html_text, start=window_start, end=window_end)
        window_best_score = 0
        window_qualifying: list[tuple[int, _RawTable]] = []
        for start, end in candidate_tables:
            parsed = _parse_table_html(html_text[start:end])
            if parsed is None:
                continue
            if not parsed.rows:
                # A table with no DATA rows cannot be the Item 403 table — it
                # has no holders to report — but it can still out-score one and
                # take the window, because ``_score_table_headers`` reads
                # headers only. Proxies are full of layout ``<table>`` blocks
                # holding a single prose paragraph, and once #2158 stopped
                # treating a digit-bearing cell as data those paragraphs began
                # promoting as "label rows" and scoring 6-11: a voting-methods
                # block beat the real 5%-holder table on 0000936468-25-000015
                # (3 holders -> 0) and an advance-notice-bylaw paragraph beat it
                # on 0001104659-26-036909 (19 -> 0). Both had zero rows.
                continue
            score = _score_table_headers(parsed.score_headers)
            if score > window_best_score:
                window_best_score = score
            if score >= _WINDOW_SCORE_FLOOR:
                window_qualifying.append((score, parsed))
        # D2 / D3 (#2160) — ELIGIBILITY decides both which table wins the window
        # and which tables join it as Item 403 siblings. Header score no longer
        # decides either; it is retained for RANKING only (the sort at the
        # concatenation loop below, so the best-captioned table's figures win a
        # holder reported twice).
        #
        # Header score is a SUM OF KEYWORD WEIGHTS and cannot separate a genuine
        # Item 403 table from a comp / prose / capitalisation table that hits the
        # same keywords: 0001628280-26-044960's genuine 403(a) table
        # (Name and Address | Number | Percent) scores 4 while a
        # Delaware-vs-Texas charter prose block scores 5 and takes the window.
        #
        # Row identity ALONE is also insufficient, which is why eligibility has
        # two limbs. Item 402 compensation tables' rows ARE people
        # ('Kevin R.M. Smith', 'Dr. Hou', 'Jennifer F. Scanlon'), so they score
        # owner_identity_fraction = 1.00 and would pass a row-identity gate
        # untouched. D4's value signature is what rejects them. Both limbs gate
        # WINNER selection as well as sibling membership — correction of
        # 2026-07-29b, measured: the draft applied D4 to the sibling set only,
        # which left this ticket's central case untouched.
        #
        # ``_SIBLING_SCORE_FLOOR`` is deleted. Its absolute 6 existed only
        # because header score was the sole signal; a genuine 403(b) table on a
        # bare Name|Shares|Percent header scores 3 and is now admitted on its
        # rows and value columns instead.
        eligible = [t for _, t in window_qualifying if _is_item403_eligible(t)]
        if eligible:
            # #2176 class 1 — 229.403's OTHER subsection, whose header is this
            # one's plus a Rule 13d-3 component column. Strictly additive inside
            # a window already won, so it cannot re-rank window selection.
            chosen = {id(t) for t in eligible}
            eligible = eligible + _subsection_sibling_tables(
                eligible, [t for _, t in window_qualifying if id(t) not in chosen]
            )
            best_score = window_best_score
            qualifying = eligible
            best_table = eligible[0]
            chosen_window = (window_start, window_end)
            break
        # Also track the global best in case no window meets floor —
        # lets callers see the best diagnostic score for tombstone
        # logging (PR 2 will surface this).
        if window_best_score > best_score:
            best_score = window_best_score

    if best_table is None or chosen_window is None:
        logger.debug(
            "DEF 14A: no beneficial-ownership table met score floor across %d window(s); best_score=%d",
            len(candidate_windows),
            best_score,
        )
        return Def14ABeneficialOwnershipTable(as_of_date=None, rows=[], raw_table_score=best_score)

    window_start, window_end = chosen_window
    as_of_date = _extract_as_of_date(html_text, window_start=window_start, window_end=window_end)

    rows: list[Def14ABeneficialHolder] = []
    seen: set[str] = set()
    # Best-captioned table first, so when two tables report the same holder the
    # figures from the one with the strongest Item 403 header survive. Stable,
    # so document order breaks ties.
    for table in sorted(qualifying, key=lambda t: -_score_table_headers(t.score_headers)):
        _extract_table_holders(table, rows=rows, seen=seen)
    return Def14ABeneficialOwnershipTable(
        as_of_date=as_of_date,
        rows=rows,
        raw_table_score=best_score,
    )


def _pad_row(raw_row: tuple[str, ...], *, name_idx: int, shares_idx: int, percent_idx: int) -> list[str]:
    """Pad short rows (some issuers omit trailing cells when the value is
    blank) so positional access doesn't IndexError."""
    return list(raw_row) + [""] * max(0, max(name_idx, shares_idx, percent_idx) + 1 - len(raw_row))


def _cell_segments(cell: str) -> list[str]:
    """Non-empty LINES of CELL, in document order.

    ``_strip_inline_html`` collapses every whitespace class EXCEPT ``\\n``
    (``_INLINE_WHITESPACE_RE``), so a cell's interior line breaks survive into
    the parsed table. That is deliberate — the Item 402(c) SCT path splits a
    name from its title on exactly that character — and it is what makes the
    ``<br>``-stacked Item 403 row below recoverable at all.

    ⚠ **It recovers only the half of that shape where a SOURCE newline follows
    the tag.** ``_strip_inline_html`` replaces ``<br>`` itself with a SPACE, so
    ``'486,340<br>658,400'`` arrives as one line and nothing here can segment
    it — and ``_parse_share_count`` strips spaces and commas, so that cell
    parses to 486,340,658,400. Measured, not hypothetical: 30 rows across 9
    accessions of ``def14a_beneficial_holdings`` hold a share count above 1e10.
    Filed as **#2358** rather than fixed here, because representing ``<br>`` as
    a line break has to happen in the cell extractor that
    ``parse_summary_compensation_table`` also uses — the shared-chokepoint class
    #2175's prevention-log entry was written about, where the same move drifted
    580 accessions of Item 402(c) output.
    """
    return [line.strip() for line in cell.split("\n") if line.strip()]


def _stacked_name_blocks(cell: str) -> list[str]:
    """Split a stacked Item 403 NAME cell into one block per beneficial owner.

    Split on BLANK LINES, then flatten each block the way
    ``_clean_beneficial_holder_name`` flattens a whole cell.

    ⚠ Line ordinal cannot be used to align the name cell against the value
    cells, and the ticket's own accession is the proof: on
    ``0000351998-18-000006`` the value cells run 10 lines (``486,340`` at 0,
    ``658,400`` at 9) while the name cell runs 17, because the issuer's
    ``880 Third Avenue, 16 th`` / ``Floor`` and ``New`` / ``York, NY 10022``
    are SOURCE WRAPS inside one holder's address. Index 9 of the name cell is
    blank; the second holder starts at 10.

    No published rule governs this — 17 CFR 229.403 prescribes the columns, not
    the markup — so the split is fixed BY CONSTRUCTION on the blank line, and
    the caller gates it on the block count matching the value count exactly.
    A cell with no blank-line separator yields one block, the gate fails, and
    the row is left exactly as it is today.
    """
    blocks: list[str] = []
    current: list[str] = []
    for line in cell.split("\n"):
        if line.strip():
            current.append(line.strip())
        elif current:
            blocks.append(" ".join(current))
            current = []
    if current:
        blocks.append(" ".join(current))
    return blocks


# The placeholders ``_parse_share_count`` and ``_parse_percent`` already accept
# as "no value stated". A stacked value column may legitimately carry one per
# line — an issuer reporting a percent for a holder whose count it omits — so
# they neither evidence a holder nor contradict one.
_ABSENT_VALUE_SEGMENTS: Final[frozenset[str]] = frozenset({"-", "—", "–", "N/A", "n/a"})


def _is_whole_share_segment(segment: str) -> bool:
    """True when SEGMENT is a 229.403 column-3 amount — a WHOLE share count."""
    if "%" in segment:
        return False
    value = _parse_share_count(segment)
    return value is not None and value == value.to_integral_value()


def _is_percent_segment(segment: str) -> bool:
    """True when SEGMENT is UNAMBIGUOUSLY a 229.403 column-4 percent.

    Same bar as the ragged-row percent recovery in ``_extract_holder_rows``: a
    bare number is not accepted, because ``_parse_percent``'s [0, 100] clamp
    reads a small share count as a percent.
    """
    if "%" not in segment and segment not in ("*", "**"):
        return False
    return _parse_percent(segment) is not None


def _value_stack_state(segments: list[str], is_value: Callable[[str], bool]) -> str:
    """Classify a value column's line stack: ``none`` / ``stack`` / ``veto``.

    ``none``  — fewer than two lines, or no line states a value at all.
    ``stack`` — two or more lines, every one either a value or an explicit
                placeholder, at least one a value.
    ``veto``  — a multi-line cell carrying something that is neither.
    """
    if len(segments) < 2 or not any(is_value(segment) for segment in segments):
        return "none"
    if all(is_value(segment) or segment in _ABSENT_VALUE_SEGMENTS for segment in segments):
        return "stack"
    return "veto"


def _split_stacked_holder_row(
    raw_row: tuple[str, ...],
    *,
    name_idx: int,
    shares_idx: int,
    percent_idx: int,
) -> list[tuple[str, ...]] | None:
    """Expand ONE ``<tr>`` holding N beneficial owners into N rows, or ``None``.

    Source rule: 17 CFR 229.403(a) and (b) prescribe a TABLE with one entry per
    beneficial owner, and column 3 ("Amount and nature of beneficial
    ownership") states ONE amount for ONE owner. A value cell that holds two
    whole share counts on separate lines is therefore two owners the issuer
    rendered inside a single ``<tr>`` with ``<br>``, not one owner with two
    figures — the same class of defect as #2175, where the parser had not
    implemented the ``rowspan`` half of the table model. Here the issuer's row
    model is the LINE STACK and ours is the ``<tr>``.

    Neither value parser tolerates the stack (``_parse_share_count`` of
    ``'486,340 \\n \\n 658,400'`` is ``None``, and so is ``_parse_percent`` of
    ``'5.86% \\n \\n 7.94%'``), so today the row dies on the "neither shares nor
    percent parsed" guard and takes every holder in it with it —
    ``0000351998-18-000006`` scores 12 on a textbook Item 403 header and stores
    zero holders.

    The trigger is the VALUE side, never the name side. A name cell with
    interior line breaks is ambiguous by itself: a render wrap produces exactly
    that, which is why ``_clean_beneficial_holder_name`` flattens it (#2140 D5 —
    an interior wrap otherwise split ONE person across two holder identities on
    704 rows / 117 instruments). Requiring the amounts to stack first is what
    keeps that flatten intact: a wrapped name over a single amount never
    reaches this function.

    Every gate below must hold, or the row is returned unsplit:

    1. A value column stacks ``k >= 2`` segments and **every** segment parses —
       all whole share counts, or all percents. A partial stack ('486,340' and
       a stray footnote line) is not evidence of k owners, and admitting it
       would align the columns by a count nothing supports.
    2. When BOTH value columns stack, they agree on ``k``.
    3. The name cell yields exactly ``k`` blank-line-separated blocks.

    Cells other than the name and the aligned value columns are distributed
    when they carry exactly ``k`` non-empty lines (the footnote column of the
    cited accession does: ``'(1)'`` and ``'(2)'``) and blanked otherwise —
    leaving a stacked string in place would feed the value-recovery scans a
    two-number cell that neither parser accepts.
    """
    cells = _pad_row(raw_row, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx)

    share_segs = _cell_segments(cells[shares_idx]) if 0 <= shares_idx < len(cells) else []
    percent_segs = _cell_segments(cells[percent_idx]) if 0 <= percent_idx < len(cells) else []
    shares_state = _value_stack_state(share_segs, _is_whole_share_segment)
    percent_state = _value_stack_state(percent_segs, _is_percent_segment)

    # A value column that stacks but does not fully resolve CONTRADICTS whatever
    # count the other column offers, so it vetoes the split rather than being
    # ignored. Without this, '486,340 / see note / 658,400' against two percents
    # splits on the percent column's count of 2 and drops both amounts.
    if "veto" in (shares_state, percent_state):
        return None

    if shares_state == "stack" and percent_state == "stack":
        if len(share_segs) != len(percent_segs):
            return None
        count = len(share_segs)
    elif shares_state == "stack":
        count = len(share_segs)
    elif percent_state == "stack":
        count = len(percent_segs)
    else:
        return None

    name_blocks = _stacked_name_blocks(cells[name_idx]) if 0 <= name_idx < len(cells) else []
    if len(name_blocks) != count:
        return None

    split: list[tuple[str, ...]] = []
    for ordinal in range(count):
        row: list[str] = []
        for index, cell in enumerate(cells):
            if index == name_idx:
                row.append(name_blocks[ordinal])
                continue
            segments = _cell_segments(cell)
            row.append(segments[ordinal] if len(segments) == count else "")
        split.append(tuple(row))
    return split


def _resolve_row_name(cells: list[str], *, name_idx: int, shares_idx: int) -> tuple[int, str]:
    """Return ``(source_index, raw_name_cell)`` for one data row.

    Structural guard (#2140): a holder name must carry NAME evidence. Stated as
    a positive invariant rather than a numeric blacklist so it also rejects
    '*', '<1%', '—' and footnote-only cells. When the resolved cell has no name
    evidence, fall back to the leftmost cell in THIS row that does; if none
    does, return ``(-1, "")`` and the row has no holder. This is what makes a
    share count landing in ``holder_name`` impossible regardless of future
    header shapes.
    """
    raw = cells[name_idx] if 0 <= name_idx < len(cells) else ""
    if _looks_like_name_cell(raw):
        return (name_idx, raw)
    # The shares column is eligible as a name source ONLY when it holds no
    # share count — a mis-resolved shares_idx can land on the name column
    # itself, and excluding it unconditionally then leaves the row with no name
    # at all and drops it.
    shares_cell_is_numeric = _parse_share_count(cells[shares_idx] if 0 <= shares_idx < len(cells) else "") is not None
    return next(
        (
            (i, c)
            for i, c in enumerate(cells)
            if not (i == shares_idx and shares_cell_is_numeric) and _looks_like_name_cell(c)
        ),
        (-1, ""),
    )


def _row_name_is_address(raw_row: tuple[str, ...] | None, *, name_idx: int, shares_idx: int, percent_idx: int) -> bool:
    """True when RAW_ROW's resolved name cell holds only address material — the
    ADDRESS half of a stacked Item 403(a) "Name and address" pair (#2164)."""
    if raw_row is None:
        return False
    cells = _pad_row(raw_row, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx)
    _, raw_name = _resolve_row_name(cells, name_idx=name_idx, shares_idx=shares_idx)
    return _is_address_fragment(_clean_beneficial_holder_name(raw_name))


_ROW_IDENTITY_FLOOR: Final[float] = 0.5


def _owner_identity_fraction(holders: list[Def14ABeneficialHolder]) -> float:
    """D0 (#2160) — fraction of HOLDERS that name a beneficial owner.

    Takes the ALREADY-EXTRACTED holders rather than the table, so there is
    exactly one place this fraction is computed. It previously took the table
    and re-ran the extraction itself, which left it orphaned once
    ``_is_item403_eligible`` started extracting once and deriving both the
    fraction and the value evidence from the same list (review WARNING on
    PR #2177: a future fix to one would silently not reach the other).

    The caller must extract through the real ``_resolve_columns`` +
    ``_extract_holder_rows`` path, NOT the raw first cell: Item 403's prescribed
    column 1 is ``Title of class``, so a genuine table rendering
    ``Common Stock | The Vanguard Group | 5,799,197 | 5.3%`` would fail a
    first-cell test outright (Codex ckpt-1 BLOCKING on the spec). Sharing
    ``_extract_holder_rows`` is required, not incidental: it already drops
    section headings, address-continuation fragments and value-less rows, and
    recovers ragged cells. Measuring on raw rows would count a
    ``Named Executive Officers`` heading as an identity and would penalise a
    genuine table whose rows are address continuations.

    Denominator is the extracted holder count. A table extracting zero holders
    cannot win its window anyway (#2158 element 4), so it scores 0.0.
    """
    if not holders:
        return 0.0
    hits = sum(1 for h in holders if _is_beneficial_owner_identity(h.holder_name))
    return hits / len(holders)


def _extract_table_holders(
    table: _RawTable,
    *,
    rows: list[Def14ABeneficialHolder] | None = None,
    seen: set[str] | None = None,
    drop_non_owner_rows: bool = True,
) -> list[Def14ABeneficialHolder]:
    """Resolve TABLE's columns and extract its holders into ROWS.

    The single place ``_resolve_columns`` is paired with ``_extract_holder_rows``.
    Two callers, with deliberately different dedup semantics, which is why the
    ``rows`` / ``seen`` handles are parameters rather than locals:

    - ``parse_beneficial_ownership_table``'s concatenation loop passes a SHARED
      ``rows`` and ``seen`` so sibling Item 403 tables dedup against each other
      (a holder reported in both keeps the better-captioned table's figures).
    - ``_is_item403_eligible`` passes neither, judging one table in isolation —
      a fresh ``seen`` per table, because eligibility is a property of that
      table alone and must not depend on what a sibling already contributed.

    Review NITPICK on PR #2177 flagged the paired calls as duplication. They are
    not the same operation, but they are the same five lines, so they live here.
    """
    name_idx, shares_idx, percent_idx = _resolve_columns(table.column_headers)
    out = [] if rows is None else rows
    _extract_holder_rows(
        table,
        name_idx=name_idx,
        shares_idx=shares_idx,
        percent_idx=percent_idx,
        rows=out,
        seen=set() if seen is None else seen,
        drop_non_owner_rows=drop_non_owner_rows,
        # The layout-attested percent (#2376) is an EXTRACTION rescue, never a
        # SELECTION input — so it is off for the eligibility probe, which is the
        # ``rows is None`` caller. ``_is_item403_eligible`` judges a table by
        # extracting it and asking ``_has_item403_value_rows``, so a percent
        # recovered here decides which tables ARE Item 403 tables, and that gate
        # is calibrated against the flat grid.
        #
        # Measured, not defensive: with the rescue live during the probe, the
        # full-population A/B admitted a junk row on 0001140361-25-012231 and
        # -26-013118 — ``Brian H. Hertzman`` at 446,200 shares / 89.2%, beside
        # the real ``Brian S. Hertzman`` at 18,832 / 0.5%. An 89.2% holding
        # implies 500,224 shares outstanding against the ~83m the same table's
        # other rows imply. Table selection belongs to #2160 / #2176 and moves
        # only under its own A/B.
        attest_percent=rows is not None,
    )
    return out


def _is_item403_eligible(table: _RawTable) -> bool:
    """True when TABLE may win its window / join the Item 403 sibling set.

    Both limbs required — see the call site for why row identity alone admits
    Item 402 compensation tables.

    The holders are extracted FIRST, before the header signature is consulted.
    That ordering is necessary, not accidental: D4's data-row fallback needs the
    parsed values, and the identity fraction needs the same list, so extracting
    once and deriving both is the only way to avoid running the extraction
    twice. (An earlier revision claimed the cheap header regex ran first and
    short-circuited the extraction; that stopped being true when the fallback
    landed — review NITPICK on PR #2177.)

    The signature reads BOTH header tuples. ``column_headers`` alone is wrong:
    when a two-row header promotes the SUB-header row, ``column_headers``
    becomes ``('', 'Sole', 'Shared', 'Total', '')`` and the parent caption
    ``Amount and Nature of Beneficial Ownership | Percent of Class`` survives
    only in ``score_headers`` — so reading the narrower tuple rejected the most
    prescribed shape the reg has, at scores 14 and 16.
    """
    # ⚠ #2176 — eligibility scores the UNPRUNED rows, deliberately. The per-row
    # owner guard is a STORAGE filter; letting it also feed this function makes
    # it a SELECTION change, and the two must not be coupled. Pruning non-owner
    # rows raises ``_owner_identity_fraction`` for every table, which is exactly
    # what this limb exists to stop — see the docstring above: "row identity
    # alone admits Item 402 compensation tables".
    #
    # Not hypothetical. The full-population A/B for the coupled version gained
    # 10 rows across 3 accessions, every one an Item 402 equity-compensation
    # plan row ('weighted average exercise price', 'total shares subject to
    # outstanding awards') newly clearing ``_ROW_IDENTITY_FLOOR`` on tables that
    # correctly failed it on origin/main — the #2158 failure mode the A/B skill
    # documents. The same run gained ZERO genuine holders, so the coupling paid
    # nothing and cost 10 junk admissions.
    holders = _extract_table_holders(table, drop_non_owner_rows=False)
    if not holders:
        return False
    headers = tuple(table.score_headers) + tuple(table.column_headers)
    if not _item403_value_signature(headers, data_row_evidence=_has_item403_value_rows(holders)):
        return False
    return _owner_identity_fraction(holders) >= _ROW_IDENTITY_FLOOR


# Punctuation, footnote debris and unicode spaces, so two renderings of the same
# prescribed caption ("Name and address (1)" / "Name and Address(3)") compare equal.
_CAPTION_NOISE_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9%]+")
# How many distinct captions an ANCHOR must carry before it may vouch for a
# sibling. 229.403 prescribes four columns; a one- or two-caption anchor
# ("Name", "Shares") is a subset of most headers in a proxy, so the superset
# test would be satisfied vacuously.
#
# ⚠ The corpus does not distinguish 2 from 3 — both admit exactly the same one
# table across the 25,954 accessions the arm ranking covered, because no anchor
# that thin is eligible in practice. 3 is therefore chosen as the tighter of two
# equal arms, not because a measurement separated them. Said plainly so the next
# reader does not cite a number the data never supplied.
_SUBSECTION_CAPTION_FLOOR: Final[int] = 3


def _header_caption_set(table: _RawTable) -> frozenset[str]:
    """TABLE's header as a set of normalised captions.

    Reads BOTH header tuples, exactly as :func:`_is_item403_eligible` does — a
    promoted sub-header row leaves the prescribed parent caption in
    ``score_headers`` only.
    """
    cells = tuple(table.score_headers) + tuple(table.column_headers)
    out = {_CAPTION_NOISE_RE.sub(" ", _FOOTNOTE_RE.sub("", cell).lower()).strip() for cell in cells}
    return frozenset(caption for caption in out if caption)


def _subsection_sibling_tables(eligible: list[_RawTable], others: list[_RawTable]) -> list[_RawTable]:
    """Item 403's OTHER subsection, admitted on the Item's structure (#2176 class 1).

    17 CFR 229.403 is ONE Item with TWO subsections — (a) beneficial owners of
    more than five percent, (b) directors and management, plus the Instruction 5
    group row — and issuers render them as two tables under one heading. That is
    already why the window loop concatenates every eligible table instead of
    taking the best-scoring one.

    Both subsections carry 229.403's same prescribed captions, so 403(b) is
    commonly 403(a)'s header plus extra columns. Rule 13d-3(d)(1)(i)
    (17 CFR 240.13d-3) DEEMS a person the beneficial owner of securities they
    have the right to acquire, so an issuer legitimately subdivides column 3
    into its components: ExlService renders
    ``Name and address | Shares | % | Vested but unsettled RSUs | Total``
    (0001193125-25-103261, 0001193125-26-181891). ONE of those component columns
    carries Item 402 vocabulary, and :func:`_item403_value_signature` vetoes on
    the JOINED header, so that single column condemns a table whose other four
    captions are byte-identical to the 403(a) table sitting above it — 13
    holders each, including the Instruction 5 group row.

    So admit on the Item's STRUCTURE rather than widening the comp veto's
    vocabulary a fifth time (#2088 / #2094 / #2097 / #2169 were all vocabulary
    patches): a table whose captions are a SUPERSET of an independently eligible
    table's, in the same window, is that Item's other subsection.

    Cannot move window SELECTION. It takes the eligible set as a precondition,
    so it only ever runs inside a window some other table has already won — a
    pure widening there, never a re-ranking. The row-identity floor still
    applies, because this admits a table the value-signature gate rejected.
    """
    anchors = [
        captions for captions in map(_header_caption_set, eligible) if len(captions) >= _SUBSECTION_CAPTION_FLOOR
    ]
    if not anchors:
        return []
    admitted: list[_RawTable] = []
    for table in others:
        captions = _header_caption_set(table)
        if not any(anchor <= captions for anchor in anchors):
            continue
        holders = _extract_table_holders(table, drop_non_owner_rows=False)
        if not holders or _owner_identity_fraction(holders) < _ROW_IDENTITY_FLOOR:
            continue
        admitted.append(table)
    return admitted


_VALUE_ROW_EVIDENCE_FLOOR: Final[float] = 0.5


def _has_item403_value_rows(holders: list[Def14ABeneficialHolder]) -> bool:
    """D4 data-row fallback (#2160) — Item 403's value columns, evidenced by the ROWS.

    Technique borrowed from ``edgar.proxy.html_extractor._build_column_map``,
    which falls back to classifying the first DATA ROWS when the headers
    classify too few columns (skill G17). Our gate was header-only, and that is
    exactly why a genuine table rendering
    ``| | | Number of Shares | | | | | | Number of Shares | |`` over
    BlackRock / First Light / Soleus rows was emptied: the captions had degraded
    to blank cells and carried no percent token at all.

    17 CFR 229.403 prescribes column 3 (amount) AND column 4 (percent of class).
    When the CAPTIONS are gone, both columns PARSING for a majority of extracted
    rows is the remaining evidence that both exist. Deliberately requires BOTH —
    an Item 402 award table routinely has an amount column and no percent.

    Ranks BELOW the Item 402 vetoes on purpose: this is weak evidence, and a
    comp table with a payout-percent column also satisfies it.
    """
    if not holders:
        return False
    n = len(holders)
    shares = sum(1 for h in holders if h.shares is not None)
    percents = sum(1 for h in holders if h.percent_of_class is not None)
    floor = _VALUE_ROW_EVIDENCE_FLOOR * n
    return shares >= floor and percents >= floor


def _collapse_stacked_value_cells(flat_row: tuple[str, ...], line_row: tuple[str, ...]) -> tuple[str, ...]:
    """Read a value cell the markup stacks as its FIRST entry (#2358).

    Source rule: 17 CFR 229.403(a) column 3 — "Amount and nature of beneficial
    ownership" — states ONE amount for ONE entry, and column 1 is the "Title of
    class" that entry is scoped to. A cell rendering ``118,028<br/>165,426``
    beside ``Class A<br/>Class B`` is therefore TWO entries for one holder, one
    per class, not one entry with two figures.

    Reached only AFTER :func:`_split_stacked_holder_row` has declined the row —
    a stack the NAME column corroborates is N distinct holders and is recovered
    there. This is the residue: one holder across N classes.

    Taking the first entry is not a new convention, it is the one this parser
    already applies to the ``rowspan`` rendering of the identical shape. Liberty
    Media (``0001104659-25-029081``) renders Chase Carey's six series as six
    ``<tr>``s under a spanning name cell; ``_extract_holder_rows`` dedups on
    ``lower(trim(holder_name))`` and keeps the FIRST — see
    ``test_multi_series_table_names_the_holder_not_the_series_ticker``. The
    class is dropped either way because ``def14a_beneficial_holdings`` has
    nowhere to put it (#2176). Rendering the same filing shape two ways must not
    produce two different answers.

    Doing nothing is not an option, because the flat rendering PARSES:
    ``'486,340<br>658,400'`` reaches ``_parse_share_count`` as
    ``'486,340 658,400'``, which strips spaces and commas and returns
    **486,340,658,400** — a stored value wrong by five orders of magnitude.

    ONE gate, deliberately #2169's: ``_value_stack_state == "stack"`` — every
    line of the cell is a whole share count, or every line is a percent,
    explicit placeholders aside. That is also what keeps the NAME column safe
    without a special case for it, and the reasoning is worth stating because
    the special case looks obligatory: a 229.403 column-2 name never has every
    line parse as a value, so the wrapped-name shape #2140 D5's flatten exists
    for (``'Michael\\nO. Johnson'`` — 704 rows / 117 instruments split across
    two holder identities) lands on ``none`` or ``veto`` and is never reached.
    An explicit ``index != name_idx`` exemption was written first and removed:
    no fixture can construct the case it guards, so no test can hold it, and an
    unreachable guard reads as coverage it does not have.

    Applied to EVERY cell rather than to ``shares_idx``, because the
    resolved shares column is frequently not where the value is read from: on
    0001193125-26-140058 (Lamar) ``_resolve_columns`` puts ``shares_idx`` on an
    empty layout cell and the ragged-row recovery scan in
    :func:`_extract_holder_rows` picks the amount out of cell 7 —
    ``'500,183 11,362,250'``, stored as 50,018,311,362,250.
    """
    # Review NITPICK on PR #2361. The two grids are equal cell-for-cell on the
    # overwhelming majority of rows — a cell with no ``<br>`` and no block tag
    # renders identically both ways, by construction — and a row that differs
    # nowhere cannot contain a stack, so the per-cell segmentation below is pure
    # waste on the common case. One tuple compare replaces it.
    if line_row == flat_row:
        return flat_row
    collapsed = list(flat_row)
    changed = False
    for index, line_cell in enumerate(line_row):
        # CORRECTIVE ONLY — the flat cell must already parse, i.e. it is one of
        # the cells storing a wrong number today. A stacked cell that does NOT
        # parse flat is a row the parser drops, and resurrecting it is a
        # different ticket: the full-population A/B found exactly one
        # (0001999371-25-003796), where the two 229.403 Instruction 5 group
        # captions share a cell with a single ``\n`` and no blank line, so
        # ``_stacked_name_blocks`` cannot separate them. Storing the first
        # amount under ``'All Non-Employee Directors All Executive Officers and
        # Directors as a Group (18 Persons)'`` adds a mangled holder identity to
        # a table keyed on ``lower(trim(holder_name))`` — #2176's junk-floor
        # class. Left dropped, which is what main does.
        if index >= len(flat_row) or _parse_share_count(flat_row[index]) is None:
            continue
        # AMOUNTS only, and there is no percent arm to add — review WARNING on
        # PR #2361 read the missing one as an oversight. A percent column cannot
        # reach the corruption this function repairs: ``_is_percent_segment``
        # requires ``%`` or a bare ``*``, so a percent STACK always leaves one of
        # those inside the flat cell, and ``_parse_share_count`` — which strips
        # spaces, commas and a TRAILING footnote, never an interior ``%`` or
        # ``*`` — then returns ``None``. Measured, not argued: '5.86% 7.94%',
        # '10% 20%', '* *' and '* **' all parse to None as a share count AND as
        # a percent. So a glued percent stores NULL on main and on this branch,
        # which is what #2359's review settled ("one percent beside two amounts
        # belongs to at most ONE of them, and the markup does not say which").
        # Collapsing it would ADD a figure, and the full-population A/B caught a
        # mid-branch revision doing exactly that on 0001213900-26-076369.
        segments = _cell_segments(line_cell)
        if _value_stack_state(segments, _is_whole_share_segment) != "stack" or segments[0] == flat_row[index]:
            continue
        collapsed[index] = segments[0]
        changed = True
    return tuple(collapsed) if changed else flat_row


def _extract_holder_rows(
    table: _RawTable,
    *,
    name_idx: int,
    shares_idx: int,
    percent_idx: int,
    rows: list[Def14ABeneficialHolder],
    seen: set[str],
    drop_non_owner_rows: bool = True,
    attest_percent: bool = True,
) -> None:
    """Append one :class:`Def14ABeneficialHolder` per data row of ONE Item 403
    table, skipping rows already collected from a sibling table.

    Section-heading role context is local to ``table`` — each Item 403 table
    re-establishes its own headings."""
    current_role: str | None = None
    # Stacked name/address recovery (#2164). Source rule: 17 CFR 229.403(a)
    # prescribes ONE column — "Name and address of beneficial owner" — and
    # issuers routinely render it as TWO STACKED ROWS: the name (often
    # colspan-collapsed to a single cell) on row N, the address plus the share
    # count and percent on row N+1. Neither row survives alone. Row N has no
    # values and dies on the "neither shares nor percent parsed" guard (or is
    # eaten as a role heading, because a name carrying a title matches the
    # director/officer patterns); row N+1's name cell is a pure address and
    # dies on ``_is_address_fragment``. Every holder of 0000799233-25-000020
    # (16), -26-000017 (17) and 0000950170-25-048978 (2: Vanguard 94,052,723 /
    # 12.76%, BlackRock 64,137,817 / 8.70%) was lost this way.
    #
    # Carrying the name forward is purely ADDITIVE at the row level: it fires
    # only on a pair where BOTH rows are already being dropped.
    pending_owner_name: str | None = None
    # Built at most once per table, and only when a row actually reaches the
    # layout-attested rescue below — rebuilding the grid costs a second cell
    # parse, and the rewash path is the surface #2171 was about.
    layout_percents: dict[str, Decimal] | None = None
    # One-<tr>-N-holders expansion (#2169), BEFORE the loop rather than inside
    # it: the stacked-name/address recovery above looks at ``raw_rows[idx + 1]``,
    # so the sequence it walks must already be the expanded one or a split row's
    # successor is the un-split original.
    #
    # The split reads ``line_rows``, NOT ``rows`` (#2358): the issuer's row
    # separator inside the cell is ``<br>`` or a block boundary, and the flat
    # grid renders both as a space, so the stack is only visible on the
    # line-structured grid. A row the split declines keeps its FLAT cells — the
    # two grids differ in the whole corpus, not just on the stacked shape, and
    # every other consumer here (role headings, address fragments, the value
    # recovery scans) is tuned against the flat text.
    raw_rows: list[tuple[str, ...]] = []
    for source_row, line_row in zip(table.rows, table.line_rows, strict=True):
        split = _split_stacked_holder_row(line_row, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx)
        if split is not None:
            raw_rows.extend(split)
            continue
        raw_rows.append(_collapse_stacked_value_cells(source_row, line_row))
    for idx, raw_row in enumerate(raw_rows):
        # The diversions below are gated on the NEXT row actually being an
        # address row, so the behaviour change is bounded to the stacked shape
        # rather than applying to every value-less row in the corpus.
        #
        # Evaluated LAZILY (review NITPICK on PR #2170): only the two diversion
        # branches consult it, and most rows reach neither — the empty-row and
        # ordinary-data paths would otherwise pay for a full name resolution of
        # the following row on every iteration.
        def stacked_next(idx: int = idx) -> bool:
            return _row_name_is_address(
                raw_rows[idx + 1] if idx + 1 < len(raw_rows) else None,
                name_idx=name_idx,
                shares_idx=shares_idx,
                percent_idx=percent_idx,
            )

        # Single-cell heading rows flip the role tag.
        heading_role = _detect_role_heading(raw_row)
        if heading_role is not None:
            heading_text = _clean_beneficial_holder_name(next((c for c in raw_row if c.strip()), ""))
            if _is_owner_identity(heading_text) and stacked_next():
                # Not a section heading — the NAME half of a stacked pair whose
                # inline title ('Mr. Michael J. Gerdin, Chief Executive
                # Officer, …, and Director') happens to match a role pattern.
                # Deliberately does NOT set current_role: tagging every
                # subsequent 5% holder 'director' off one holder's own title
                # would mis-file them into the insiders slice.
                pending_owner_name = heading_text
            else:
                current_role = heading_role
                pending_owner_name = None
            continue

        # Skip totally-empty rows defensively (the regex above
        # already filters most but trailing footnote rows can slip
        # through with one whitespace-only cell).
        if not any(c.strip() for c in raw_row):
            continue

        cells = _pad_row(raw_row, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx)

        shares_raw = cells[shares_idx] if shares_idx < len(cells) else ""
        # percent_idx < 0 means "no distinguishable percent column" — never
        # index end-relative into the row (that would read a footnote cell).
        percent_raw = cells[percent_idx] if 0 <= percent_idx < len(cells) else ""

        name_src_idx, holder_name_raw = _resolve_row_name(cells, name_idx=name_idx, shares_idx=shares_idx)

        holder_name = _clean_beneficial_holder_name(holder_name_raw)
        if _SCHEDULE_13D_COVER_LABEL_RE.match(holder_name):
            # Proxies embed Schedule 13D/G COVER PAGES as exhibits. Their
            # numbered rows read as a table whose "names" are the cover-page
            # item labels and whose "share counts" are the ROW NUMBERS
            # (0001104659-17-023458 stored 'SHARED VOTING POWER -0' with
            # shares=8). Not an Item 403 table — see the regex for the rule.
            pending_owner_name = None
            continue
        if _is_address_fragment(holder_name):
            # ADDRESS half of a stacked pair — take the name carried from the
            # preceding row. ``name_src_idx`` deliberately keeps pointing at
            # the address cell so the value-recovery scans below still exclude
            # it; an address is never a share count or a percent.
            holder_name = pending_owner_name or ""
            pending_owner_name = None
        if not holder_name:
            pending_owner_name = None
            continue
        if drop_non_owner_rows and _is_instrument_not_owner(holder_name, strip_class_designator=True):
            # #2176 — the row names no beneficial owner. 17 CFR 229.403 column
            # 2 is "Name and address of beneficial owner", and Rule 13d-3
            # defines that as a person or entity holding voting or investment
            # power. A name composed ENTIRELY of equity/award vocabulary is
            # neither: it is either the table's own column-1 'Title of class'
            # value leaking into the name column ('Series A Common Shares') or
            # a presentation aggregate ('Total', 'Total Shares Outstanding').
            #
            # The predicate is not new and neither is the vocabulary. What was
            # missing is a place to apply it PER ROW: #2176 §1 measured
            # ``_is_beneficial_owner_identity`` down to a single call site,
            # inside ``_owner_identity_fraction``, which gates TABLE selection
            # only — so an admitted table wrote every row it produced, and
            # ``_ROW_IDENTITY_FLOOR`` tolerates up to 49% non-owners by design.
            #
            # Deliberately the NEGATIVE test and not the positive one. Applying
            # ``_is_beneficial_owner_identity`` per row was measured on the full
            # population (#2176 §2) and rejects genuine holders — bare
            # 'BlackRock', 'Margareth Øvrum', and the 229.403(b) Instruction 5
            # group row itself — at many times the rate it removes junk.
            #
            # Removing these rows RAISES ``_owner_identity_fraction`` for the
            # table, which is the intended direction: #2176 class 2 is a
            # genuine Item 403 table pushed under the floor by its own
            # class-label rows.
            pending_owner_name = None
            continue
        shares = _parse_share_count(shares_raw)
        # #2163 — 17 CFR 229.403 column 3 is a COUNT, column 4 is a PERCENT.
        # A percent-signatured value at shares_idx is not a holding; hold it
        # back so the recovery scan below runs and finds the real count. See
        # ``_shares_cell_percent_signature`` for the two signature strengths.
        percent_signature = (
            _shares_cell_percent_signature(cells, shares_idx, table.column_headers) if shares is not None else None
        )
        if percent_signature is not None:
            shares = None
        shares_src_idx = shares_idx if shares is not None else -1
        # Parse the positional percent BEFORE recovering shares, so the
        # recovery knows whether percent_idx actually yielded a percent. When
        # it did, that cell is off limits; when it did not (a header narrower
        # than its data rows puts a SHARE COUNT at percent_idx — 5,439,432
        # under "Percent of Total (%)" on 0002077096-26-000092), the cell is
        # fair game and skipping it unconditionally loses the row.
        percent = _parse_percent(percent_raw)
        percent_src_idx = percent_idx if percent is not None else -1
        if shares is None:
            # Ragged/narrow header (#2140): a 3-cell header row over 5-cell
            # data rows leaves shares_idx pointing at the NAME column
            # (0002077096-26-000092 headers ('Number of Shares (#)', '',
            # 'Percent of Total (%)') over rows [name, '', '5,439,432', '',
            # '24.3']). Recover the share count from the first cell that
            # parses as one, skipping the cell the holder name came from.
            for i, cell in enumerate(cells):
                if i in (name_src_idx, percent_src_idx):
                    continue
                if "%" in cell:
                    continue
                candidate = _parse_share_count(cell)
                # Share counts are WHOLE shares. Requiring an integer stops the
                # scan reading a bare percent as a holding — a row with
                # ``Shares = —`` and ``Percent = 8.4`` would otherwise store
                # shares=8.4 (Codex pre-push review). Leaving shares NULL is the
                # safe fallback; _parse_percent already handles bare percents.
                if candidate is None or candidate != candidate.to_integral_value():
                    continue
                # #2163 A/B round 2: when the PRIMARY cell was held back as a
                # percent, the recovery must not simply grab the next percent
                # along. 0001177394-25-000016 ('Minimum Payment … as Percentage
                # of Base Salary') held back 30.0 and then recovered '100.0'
                # from the next column — still a percent, now stored as a share
                # count. Apply the same signature test to the candidate.
                if percent_signature is not None and _shares_cell_percent_signature(cells, i, table.column_headers):
                    continue
                shares, shares_src_idx = candidate, i
                break
        if shares is None and percent_signature == "weak":
            # Nothing whole anywhere in the row, and the evidence against the
            # cell was only circumstantial — a non-integral value (a fractional
            # DRIP/plan holding is unusual but real) or a lone '%' sibling cell,
            # which can sit to the RIGHT of a genuine small holding when the
            # issuer renders the sign in its own column (Codex ckpt-2:
            # ``[name, '50', '%', '0.1']`` would otherwise lose the 50 shares).
            # Restore rather than drop the row's only number. ``"decisive"`` is
            # NOT restored — a percent CAPTION states what the column holds.
            shares = _parse_share_count(shares_raw)
            shares_src_idx = shares_idx if shares is not None else -1
        if percent is None:
            # Ragged row (#2140 D3): issuers interleave footnote-only cells
            # ('(2)') mid-row, so a data row can be WIDER than its header row
            # and the positionally-resolved percent cell lands on the wrong
            # column — CYH stored percent NULL on every row while the filing
            # plainly shows 8.4 / 6.9 / 6.0.
            #
            # Only cells that are UNAMBIGUOUSLY a percent are eligible: they
            # carry a '%' or are the industry '*' (<1%) marker. A bare number
            # is NOT accepted — _parse_percent's [0,100] clamp rejects large
            # share counts but would happily read a small one (e.g. '50') as
            # 50%, which is exactly the misfire this ticket exists to remove.
            for i, cell in enumerate(cells):
                # Skip the cells the name and the share COUNT actually came
                # from — not the resolved indices. When a header resolves onto
                # a percent column ("10.2%" under "Number of Shares
                # Beneficially Owned") the share count is recovered from
                # elsewhere in the row, and that percent cell must stay
                # eligible or the row loses both values.
                if i in (name_src_idx, shares_src_idx):
                    continue
                text = cell.strip()
                # A value and its '%' sign are often SEPARATE cells ('17.1', '%'),
                # so a bare number immediately followed by a lone '%' is just as
                # unambiguous as '17.1%' in one cell.
                followed_by_percent_sign = any(c.strip() for c in cells[i + 1 : i + 3]) and next(
                    (c.strip() for c in cells[i + 1 :] if c.strip()), ""
                ) in ("%", "(%)")
                if "%" in text or text in ("*", "**") or (text and followed_by_percent_sign):
                    percent = _parse_percent(text)
                    if percent is not None:
                        break

        # LAST resort, and deliberately after the scan above (#2163 A/B round
        # 3): a hold-back that SUCCEEDED — the real whole count was found at a
        # DIFFERENT index — proves the held-back cell was 229.403 column 4.
        # But it is weaker evidence than an unambiguous '%'-bearing cell, so it
        # must not pre-empt the scan. Running it FIRST cost the row its real
        # percent on tables carrying two percent-ish columns:
        # 0000950170-24-100030 'Edward J. Richardson' has 98.1 at shares_idx and
        # 14.8 under 'Percent of Class'; pre-empting stored 98.1.
        held_back_succeeded = percent_signature is not None and shares is not None and shares_src_idx != shares_idx
        if percent is None and (percent_signature == "decisive" or held_back_succeeded):
            percent = _parse_percent(shares_raw)
            if percent is not None:
                percent_src_idx = shares_idx

        # Drop rows where neither shares nor percent parsed — that's
        # almost always a free-text annotation row ("Notes:",
        # "(continued from previous page)") and not real data.
        if shares is None and percent is None:
            # …unless it is the NAME half of a stacked pair (#2164): carry the
            # name to the address row that follows and carries the values. The
            # identity test is what keeps section headings ('Directors and
            # Executive Officers:', '5% Stockholders') out of holder_name.
            pending_owner_name = holder_name if (_is_owner_identity(holder_name) and stacked_next()) else None
            continue
        pending_owner_name = None

        # LAYOUT-ATTESTED percent (#2376), deliberately the LAST rescue and
        # deliberately AFTER the drop guard above.
        #
        # Every rescue before this one works inside the flat cell grid, where a
        # data row and its header need not share a column space at all — issuers
        # interleave footnote-only cells, and ``_CELL_RE`` silently drops
        # self-closing ``<td/>`` spacers and mis-pairs the ``colspan`` of the
        # cell after one. So the positional percent cell can land on a spacer
        # while the real percent sits further right, and the ragged-row scan is
        # RIGHT to decline it: a bare ``14.33`` with no ``%`` and no ``*`` is
        # indistinguishable from a 14-share holding without the layout.
        #
        # Rebuilding the table under the HTML table model supplies exactly the
        # missing fact — which caption covers this cell — so a bare number under
        # a percent caption becomes unambiguous. ExlService
        # (0001193125-25-103261) is the worked case: header ``Shares`` spans
        # layout columns 4-7 and ``% (2)`` spans 9-12, BlackRock's 23,308,871 is
        # at column 6 and its 14.33 at column 11, and all three 5% holders stored
        # a NULL percent against a filing that plainly shows 14.33 / 10.46 / 5.76.
        #
        # Placement after the drop guard is what keeps this purely ADDITIVE: it
        # can only fill a percent on a row that already survives, never admit a
        # row, never change a share count, and never overwrite a percent another
        # rescue found.
        if percent is None and attest_percent and table.table_html:
            if layout_percents is None:
                layout_percents = _layout_percent_by_row(table.table_html)
            percent = layout_percents.get(_layout_name_key(holder_name))
            # A percent equal to the row's own share count is the SAME CELL read
            # twice, not a second fact. It happens on the group-total row of a
            # table whose "shares" column already holds a percentage —
            # 0001375365-25-000009's "Total executive officers, directors & 5%
            # holders" parses shares as 33.6 on main, and attesting 33.6 against
            # it manufactures agreement out of one number. The share-count
            # defect there is pre-existing and out of scope; echoing it is not.
            if percent is not None and shares is not None and percent == shares:
                percent = None
            if percent is not None:
                percent_src_idx = -1

        role = current_role or _detect_inline_role(holder_name)

        # Item 403(b) group-aggregate override (#2140 D4). The "all directors
        # and executive officers as a group" row is the ONE row that must stay
        # distinguishable — its share count already contains every individual
        # management row above it, so it is NON-ADDITIVE with them and a
        # sum over a table that tags it 'officer' double-counts management.
        #
        # It sits INSIDE the 403(b) management block, so section context has
        # already set current_role='officer' by the time it is read and the
        # ``or`` above short-circuits _detect_inline_role (which does detect
        # it). Hence the override — same shape as the ESOP override below,
        # deliberately scoped to the aggregate row only: for individuals a
        # section heading is better evidence than an inline job title, so the
        # rest of _detect_inline_role keeps its current precedence.
        if "as a group" in holder_name.lower():
            role = "group"

        # ESOP override (#843): name-pattern detection wins over
        # section-derived role. ESOP plans routinely land in the
        # 5%-holders block (so section context tags them as
        # 'principal') but we want them in the dedicated
        # ownership_esop_* slice, not the blockholders slice.
        #
        # Run the detection on the RAW holder_name (pre-clean):
        # ``_clean_holder_name`` strips ``(k)`` as a footnote marker
        # (single-alpha-in-parens pattern), so ``Apple Inc. 401(k)
        # Plan`` becomes ``Apple Inc. 401 Plan`` after cleaning,
        # breaking the ``\b401\s*\(?k\)?\b`` regex. Detecting on raw
        # avoids this without weakening the footnote stripper.
        if is_esop_plan(holder_name_raw) or is_esop_plan(holder_name):
            role = "esop"

        # Dedup across sibling tables on HOLDER IDENTITY alone — the same key
        # the observations layer uses, ``lower(trim(holder_name))``.
        #
        # Not (name, shares, percent): a filing often renders a BREAKDOWN table
        # beside the real one (0000080661-25-000018 has "Total Common Shares
        # Beneficially Owned" for 16 people and, below it, the same 16 split
        # into "Restricted Stock Awards / Equivalent Units / Other"). Keying on
        # the figures too would keep both and put every one of those people in
        # twice — reintroducing exactly the duplicate-holder defect this ticket
        # removes. Item 403 counts a beneficial owner ONCE, and the def14a
        # slice is a non-additive memo overlay (data-engineer I21), so
        # collapsing to one row per holder is both safer and more correct.
        key = holder_name.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            Def14ABeneficialHolder(
                holder_name=holder_name,
                holder_role=role,
                shares=shares,
                percent_of_class=percent,
            )
        )


# ===========================================================================
# Item 402(c) — Summary Compensation Table (exec comp) — #1945
# ===========================================================================
#
# Governed by Regulation S-K Item 402 (17 CFR § 229.402). The Summary
# Compensation Table (SCT) columns are prescribed and ORDERED by
# § 229.402(c)(2)(i)–(x); the scaled SRC variant (§ 229.402(n)) simply omits
# some columns but preserves the order. We therefore resolve the PRESENT
# dollar columns from the header (by matched text, never fixed positional
# index) into their reg order, then zip data-row values against that ordered
# subset. This survives the wildly heterogeneous real-world markup (verified
# on AAPL / HD / JPM / MSFT full-proxy fixtures):
#   * name-cell ``rowspan`` → continuation-year rows are index-shifted (AAPL/MSFT)
#   * year folded into the name column, ``—`` for null bonus (HD)
#   * lone ``$`` spacer cells + a bare footnote-superscript cell mid-row (JPM)
#   * empty layout-spacer columns interleaved between values (AAPL/MSFT)
# Positional header→column mapping cannot survive these; token classification
# + reg-fixed ordering can.


@dataclass(frozen=True)
class Def14AExecCompRow:
    """One (executive, fiscal_year) row of the Item 402(c) SCT.

    Dollar fields are ``None`` when the column is absent (SRC scaled
    table drops pension/NQDC) OR the cell is an explicit ``—`` / ``N/A``
    null. ``principal_position`` is stored raw free-text (v1; the
    thesis consumer canonicalises CEO/CFO — open-question #1 in the spec).
    """

    executive_name: str
    principal_position: str | None
    fiscal_year: int
    salary: Decimal | None
    bonus: Decimal | None
    stock_awards: Decimal | None
    option_awards: Decimal | None
    non_equity_incentive: Decimal | None
    pension_nqdc: Decimal | None
    other_comp: Decimal | None
    total_comp: Decimal | None


@dataclass(frozen=True)
class Def14ASummaryCompTable:
    """Parsed Item 402(c) SCT payload. ``rows`` empty = no SCT
    confidently identified (log, don't guess). ``raw_table_score`` is
    the chosen table's header score for audit diagnostics (mirror
    :class:`Def14ABeneficialOwnershipTable`)."""

    rows: tuple[Def14AExecCompRow, ...]
    raw_table_score: int


# Section anchor for the SCT. Kept SEPARATE from ``_SECTION_HEADING_RE`` so
# the ownership parser is unaffected; passed into ``_find_section_windows``.
_SCT_SECTION_HEADING_RE: Final[re.Pattern[str]] = re.compile(r"Summary\s+Compensation\s+Table", re.IGNORECASE)

# Header keywords that identify the SCT specifically (vs the Director
# Compensation table 402(k) or Grants-of-Plan-Based-Awards table, which
# share layout but lack the salary+total+name/position combination).
_SCT_HEADER_KEYWORDS: Final[tuple[tuple[str, int], ...]] = (
    ("name and principal position", 4),
    ("named executive", 3),
    ("principal position", 3),
    ("salary", 3),
    ("stock award", 3),
    ("option award", 3),
    ("all other compensation", 3),
    ("non-equity", 2),
    ("nonequity", 2),
    ("non equity", 2),
    ("bonus", 2),
    ("change in pension", 2),
    ("total", 1),
    ("year", 1),
)

# SCT dollar fields in reg (c)(2)(iii)–(x) order. Each carries the header
# substrings that identify its column. Matchers are tested per header cell
# in THIS order (most specific first) so e.g. "Change in Pension Value and
# Nonqualified Deferred Compensation Earnings" claims ``pension_nqdc`` and
# never leaks into ``other_comp``. ``total`` is last (most generic).
_SCT_FIELD_MATCHERS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    ("pension_nqdc", ("change in pension", "pension value", "nqdc", "deferred compensation earnings")),
    ("non_equity_incentive", ("non-equity", "nonequity", "non equity")),
    ("stock_awards", ("stock award",)),
    ("option_awards", ("option award",)),
    ("other_comp", ("all other compensation", "all other")),
    ("bonus", ("bonus",)),
    ("salary", ("salary",)),
    ("total_comp", ("total",)),
)

_SCT_ALL_FIELDS: Final[tuple[str, ...]] = tuple(f for f, _ in _SCT_FIELD_MATCHERS)

# Zero-width chars (ZWSP/ZWNJ/ZWJ/WORD-JOINER/BOM) used as layout spacers in
# iXBRL-rendered proxies — Python ``str.strip()`` does NOT treat these as
# whitespace, so a ``​``-filled spacer cell reads as "non-empty" and
# hides the real name/value cells unless scrubbed first.
_ZERO_WIDTH_RE: Final[re.Pattern[str]] = re.compile("[\u200b\u200c\u200d\u2060\ufeff]")
# Non-breaking / unicode spaces normalised to a plain space so tokenisation
# (year detection, dash-null detection) is uniform.
_UNICODE_SPACE_RE: Final[re.Pattern[str]] = re.compile(
    "[\xa0\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u202f\u205f\u3000]"
)


def _sct_norm(cell: str) -> str:
    """Normalise an SCT cell: drop zero-width spacers, fold unicode spaces
    to plain spaces, collapse inline whitespace. Preserves ``\\n`` so the
    name/title split (line 1 = name, line 2 = title) still works."""
    s = _ZERO_WIDTH_RE.sub("", cell)
    s = _UNICODE_SPACE_RE.sub(" ", s)
    return _INLINE_WHITESPACE_RE.sub(" ", s).strip()


_YEAR_RE: Final[re.Pattern[str]] = re.compile(r"^(?:19|20)\d{2}$")
# Bare 1–2 digit non-zero integer = footnote superscript in its own cell
# (JPM's stray ``'6'``), never a real dollar amount (no exec is paid $6).
# ``0`` is preserved (legitimate zero salary/bonus/option — MSFT's bonus).
_BARE_FOOTNOTE_INT_RE: Final[re.Pattern[str]] = re.compile(r"^[1-9]\d?$")
_DASH_NULLS: Final[frozenset[str]] = frozenset({"-", "—", "–", "n/a", "na"})

# Role keywords marking where a position title begins inside a combined
# "Name  Title" cell (used to split executive_name from principal_position
# when no newline delimiter is present). Ordered longest-first so
# "executive vice president" wins over "president".
#
# The leading-modifier prefix (``senior``/``former``/``acting``/``interim``/
# ``executive``/``group``/``managing``/``co-``) pulls those title words into
# the MATCH so the split boundary lands before them — otherwise "Ann-Marie
# Campbell Senior Executive Vice President" leaves "Senior" glued to the name
# and "Bradford L. Smith Vice Chair" splits at "Chair" (#1967). ``executive``
# was added (#2097) so "Raymond R. Quirk Executive Chairman" / "Executive
# Vice- Chairman" split at "Executive", not one word late — and so
# ``_position_only_cell`` recognises a bare "Executive Chairman" title row
# instead of minting a bogus "Executive" NEO. ``group``/``managing`` were
# added (#2100 Class 3) for "Group President" / "Managing Director" /
# "Senior Managing Director" titles ("David E. Govrin Group President…" split
# at "Group"; a bare "Managing Director" row classifies position-only instead
# of minting a bogus "Managing" NEO). Full-pop verified (6,042 accessions):
# 141 leak names fixed, zero real-surname regressions. ``vice[-\s]+chair`` is
# listed explicitly (before bare ``chair``, and hyphen-tolerant for "Vice-
# Chairman") so "Vice Chair" splits at "Vice".
#
# The modifier prefix is bounded ``{0,3}`` (real titles carry at most one or two
# leading modifiers, e.g. "Former Senior") rather than ``*`` — an unbounded
# repeat is quadratic on a long adversarial modifier run with no trailing role
# ("Senior Senior … X"), and this parser runs on untrusted SEC filing HTML.
_POSITION_ROLE_RE: Final[re.Pattern[str]] = re.compile(
    r"\b("
    r"(?:(?:senior|former|acting|interim|executive|group|managing)\s+|co-?\s*){0,3}"
    r"(?:"
    r"chief\s+\w+|"
    r"executive\s+vice\s+president|senior\s+vice\s+president|vice\s+president|"
    r"vice[-\s]+chair(?:man|woman|person)?|"
    r"president|chair(?:man|woman|person)?|"
    r"general\s+counsel|chief|ceo|cfo|coo|cto|evp|svp|"
    r"executive\s+officer|principal\s+\w+|treasurer|secretary|"
    r"director|founder"
    r")"
    r")\b",
    re.IGNORECASE,
)

# Bare 1–2 digit footnote reference left inline between a name and its title
# (JPM's "Daniel Pinto 11 Vice Chair"). Stripped from the trailing edge of the
# extracted name — NEO names never end in a bare integer (#1967).
_TRAILING_FOOTNOTE_RE: Final[re.Pattern[str]] = re.compile(r"\s+[1-9]\d?$")


def _score_sct_headers(headers: tuple[str, ...]) -> int:
    """Score a candidate table by SCT header keywords. Higher = better."""
    if not headers:
        return 0
    joined = " ".join(headers).lower()
    return sum(weight for keyword, weight in _SCT_HEADER_KEYWORDS if keyword in joined)


def _resolve_sct_fields(headers: tuple[str, ...]) -> tuple[str, ...]:
    """Return the PRESENT dollar fields in HEADER (document) order.

    Walk header cells left→right; map each to at most one field (first
    matcher wins per cell). Dedup keeping first occurrence. For a
    reg-compliant filing this equals reg (c)(2) order because
    § 229.402(c)(2) fixes the column ORDER; the parser's correctness in the
    equal-length zip relies on header order == data-row order (always true
    within one table), NOT on reg order per se. Only :func:`_map_sct_values`'s
    Total anchor (mismatch branch) assumes the reg's Total-is-last rule, and it
    guards for it explicitly.
    """
    ordered: list[str] = []
    for cell in headers:
        low = cell.lower()
        for field, needles in _SCT_FIELD_MATCHERS:
            if field in ordered:
                continue
            if any(n in low for n in needles):
                ordered.append(field)
                break
    return tuple(ordered)


def _parse_dollar(raw: str) -> Decimal | None:
    """Parse an SCT dollar cell. Strips footnote markers, ``$``,
    thousands separators, NBSP; ``()`` → negative; dash/``N/A``/empty →
    ``None``. (Share parser strips commas but not ``$`` — hence a
    dedicated dollar variant per the spec.)"""
    if not raw:
        return None
    cleaned = _FOOTNOTE_RE.sub("", raw).replace("$", "").replace("\xa0", "").replace(",", "").strip()
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1].strip()
    if cleaned == "" or cleaned.lower() in _DASH_NULLS:
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    if value.is_nan() or value.is_infinite():
        return None
    return -value if negative else value


def _split_name_position(cell: str) -> tuple[str, str | None]:
    """Split a "Name and Principal Position" SCT first cell (Item 402(c)(2)(i))
    into (name, position).

    An HTML line break inside this cell is a RENDER wrap at an arbitrary point,
    NOT a name/title delimiter — it falls mid-name ("Sundar\\nPichai") or
    mid-title ("…Officer\\nand President"), so a newline-first rule truncated the
    name both too short and too long (#2097). We therefore flatten newlines to
    spaces and split at the ONSET of the position title (the first
    ``_POSITION_ROLE_RE`` keyword — the semantic boundary). When no role keyword
    is present the whole flattened cell is the name (the title, if any, rides a
    later stacked physical row and is attached by ``_position_only_cell`` /
    ``_backfill_position``, #2088); this only ever preserves the name, never
    truncates it. Footnote markers are stripped from the name; position is raw
    free-text (v1).

    Corroborated by edgartools' own SCT extractor, which flattens whitespace then
    keyword-splits (edgartools skill G16). Our first-role-keyword split avoids the
    title leak edgartools' comma-first split produces on multi-clause NEO cells
    ("Sundar Pichai Chief Executive Officer")."""
    text = _clean_holder_name(cell).replace("\xa0", " ")
    # Flatten newlines to spaces FIRST — a \n here is a render wrap, not a
    # delimiter (#2097). _INLINE_WHITESPACE_RE does not match \n, hence the
    # explicit replace before collapsing the rest of the inline whitespace.
    text = _INLINE_WHITESPACE_RE.sub(" ", text.replace("\n", " ")).strip()
    if not text:
        return "", None
    # Split at the first role keyword (the name/title boundary).
    m = _POSITION_ROLE_RE.search(text)
    if m and m.start() > 0:
        return _clean_name_footnote(text[: m.start()].rstrip(",")), text[m.start() :].strip() or None
    # No role keyword → the whole cell is the name (no title in this cell).
    return _clean_name_footnote(text), None


def _clean_name_footnote(name: str) -> str:
    """Strip a trailing inline footnote reference digit from an executive name
    (JPM's "Daniel Pinto 11" → "Daniel Pinto") and any trailing connector
    punctuation left by a split mid-phrase ("Adolphus B. Baker," → same
    without the comma; #2094). Periods are kept — "Jr." / "M.D." are real
    name endings."""
    cleaned = _TRAILING_FOOTNOTE_RE.sub("", name.strip()).strip()
    return re.sub(r"[,;&–—/-]+\s*$", "", cleaned).strip()


# Item 403's name column is "Name AND ADDRESS of beneficial owner", so issuers
# put the address in the same cell — and when they split it across sibling <tr>
# rows, the continuation lines land in the name column of their own row and
# parse as holders with real share numbers ("c/o Dolan Family Office" @
# 11,484,408 / 100%, "P.O. Box 420" @ 2,010,611 on 0001193125-25-095068).
#
# Only ADDRESS-ONLY cells are rejected — a leading c/o / PO box / attn / street
# number. A combined name+address cell still passes because it LEADS with the
# holder ("BlackRock, Inc. 55 East 52nd Street New York, NY 10055"). Anchored
# so a company name starting with a digit is unaffected: "3M Company" and
# "1st Source Corp" have no whitespace after the digits.
_STREET_TYPE = (
    r"street|st\.|avenue|ave\.?|road|rd\.?|boulevard|blvd\.?|drive|dr\.?|lane|ln\.?|way|place|pl\.?"
    r"|court|ct\.?|plaza|parkway|pkwy\.?|circle|highway|hwy\.?|terrace|square|yards|center|centre"
    r"|suite|floor|building"
)
# A LEADING street number is only an address when a street-type token follows
# within a few words. Without that constraint the rule eats real entities whose
# name starts with digits — "325 Capital LLC", "2025 Acquisition Corp",
# "2025 Irrevocable Two-Year Grantor Retained Annuity Trust" were all dropped
# (caught by the full-population A/B's distinct-holder check). The window also
# keeps a name that LEADS with the holder and merely carries an address after
# it ("325 Capital LLC 200 Park Avenue, 17th Floor"), while still catching
# "462 S. 4 th Street, Suite 2000".
_ADDRESS_ONLY_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:c/o\b"
    r"|p\.?\s*o\.?\s*box\b"
    r"|post\s+office\s+box\b"
    r"|attn\b|attention\b"
    r"|\d{1,6}\s+(?:[A-Za-z0-9.'-]+\s+){0,3}(?:" + _STREET_TYPE + r")\b"
    r")",
    re.IGNORECASE,
)

# The SECOND line of a split US address — locality, state, ZIP and nothing else
# ('New York, NY 10055', 'Bloomfield Hills, MI 48304'). It carries no street
# number, so ``_ADDRESS_ONLY_RE`` above does not see it.
#
# #2175: this was a latent gap that the row-span expansion turned into a live
# defect. Issuers put ``rowspan="2"`` on the VALUE cells of a holder whose name
# and address are stacked; before the expansion the address row parsed no values
# and died on the "neither shares nor percent" guard, and afterwards it inherited
# the holder's own figures — so ``0000107140-24-000176`` gained 'New York, NY
# 10055' at BlackRock's 6,782,743 / 14.97%, a verbatim duplicate of the row above
# it. The prevention log already recorded 'malvern, pa 19355' as this shape
# (#2164); that one lowercases, which is why the state anchor is a case-sensitive
# two-letter group inside an otherwise case-insensitive pattern.
#
# Anchored at BOTH ends and capped at four locality words: a holder whose name
# merely ENDS in this shape ('BlackRock, Inc. 55 East 52nd Street New York, NY
# 10055') must keep passing, and does, because it does not match from the start.
_CITY_STATE_ZIP_ONLY_RE: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z][A-Za-z.'\-]*(?:\s+[A-Za-z][A-Za-z.'\-]*){0,3}\s*,\s*(?i:[A-Z]{2})\.?\s+\d{5}(?:-\d{4})?\.?$"
)


def _is_address_fragment(name: str) -> bool:
    """True when a holder-name cell holds only address material (#2140, #2175)."""
    stripped = name.strip()
    return bool(_ADDRESS_ONLY_RE.match(stripped) or _CITY_STATE_ZIP_ONLY_RE.match(stripped))


# D1 clause 4-5 (#2160) — the tail after the name must carry ADDRESS evidence,
# not merely a number: a proper-noun run followed by any digit also matches
# metric rows like 'Adjusted EBITDA 2024' (Codex ckpt-1 MED). Accepted evidence
# is a US street type, a 5-digit ZIP, or the multi-comma locality chain a
# non-US address uses ('4-5, Marunouchi 1-chome Chiyoda-ku, Tokyo 100-8330,
# Japan') — the last is what carries the international filers, which have
# neither a street type nor a ZIP.
_ADDRESS_TAIL_EVIDENCE_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(?:" + _STREET_TYPE + r")\b|\b\d{5}\b",
    re.IGNORECASE,
)


def _is_name_then_address(text: str) -> bool:
    """True when TEXT is Item 403(a)'s one-column 'name AND address' form."""
    m = _OWNER_NAME_THEN_ADDRESS_RE.match(text)
    if m is None:
        return False
    tail = m.group("tail")
    return bool(_ADDRESS_TAIL_EVIDENCE_RE.search(tail) or tail.count(",") >= 2)


# Schedule 13D/G COVER-PAGE item labels (#2163).
#
# Source rule — 17 CFR 240.13d-101 (Schedule 13D) and 240.13d-102 (Schedule 13G)
# prescribe a numbered cover page. Rows 7-11 are the voting/dispositive power
# and aggregate-amount lines; rows 1-6 and 12-14 are the reporting-person,
# funding, citizenship and type-of-person lines. Proxies embed these cover pages
# as exhibits, and the numbered layout parses as a table whose "holder names"
# are these labels and whose "share counts" are the ROW NUMBERS.
#
# They are not Item 403 rows: 229.403 column 2 is a *beneficial owner*, which
# Rule 13d-3 (17 CFR 240.13d-3) defines as a person or entity holding voting or
# investment power. A cover-page item label is neither. The all-caps two-token
# shape ('SHARED VOTING POWER') otherwise reads as a person name, so neither the
# owner-identity test nor an address test rejects it.
_SCHEDULE_13D_COVER_LABEL_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    # Rows 7-10. ``investment`` is included alongside the cover page's own
    # ``dispositive`` because Rule 13d-3 defines beneficial ownership as voting
    # OR INVESTMENT power and issuers paraphrase the cover page with it
    # (0001308179-25-000114 renders a TRANSPOSED table — holders are COLUMNS,
    # rows are the four power types — and stored 'Sole investment power' as a
    # holder holding 82,447,476 shares). Note this matches resolved HOLDER
    # NAMES; #2160's D4 matches the same vocabulary in HEADERS, where it is a
    # POSITIVE Item 403 signal. Different surfaces, no contradiction.
    r"(?:sole|shared)\s+(?:voting|dispositive|investment)\s+power\b"
    r"|aggregate\s+amount\s+beneficially\s+owned\b"  # row 11
    r"|percent\s+of\s+class\s+represented\s+by\s+amount\s+in\s+row\b"  # row 13
    r"|type\s+of\s+reporting\s+person\b"  # row 14
    r"|name[s]?\s+of\s+reporting\s+person\b"  # row 1
    r"|check\s+(?:the\s+appropriate\s+)?box\b"  # rows 2, 5, 12
    r"|sec\s+use\s+only\b"  # row 3
    r"|source\s+of\s+funds\b"  # row 4
    r"|citizenship\s+or\s+place\s+of\s+organization\b"  # row 6
    r")",
    re.IGNORECASE,
)


def _looks_like_name_cell(cell: str) -> bool:
    """True when a cell is a NEO name/title (has letters, is not a bare
    year or a pure number/footnote)."""
    stripped = _clean_holder_name(cell).strip()
    if not stripped:
        return False
    if _YEAR_RE.match(stripped):
        return False
    # Needs an alphabetic run of 2+ (filters "$", "(1)", "2,500,000").
    return bool(re.search(r"[A-Za-z]{2,}", stripped))


def _normalize_first_cell(cell: str) -> str:
    """Flatten an SCT first-column cell to single-line, single-spaced text."""
    text = _clean_holder_name(cell).replace("\xa0", " ").replace("\n", " ").strip()
    return _INLINE_WHITESPACE_RE.sub(" ", text)


def _position_only_cell(cell: str) -> str | None:
    """Return the cleaned title text when CELL is a position-only fragment,
    else ``None``.

    Stacked name/position SCT layouts (GME) render the title on its OWN
    physical row below the name row, so the continuation row's first cell is
    a bare title ("Chief Executive Officer") that would otherwise pass
    ``_looks_like_name_cell`` and clobber the carried NEO name (#2088). A
    cell is position-only when the first role keyword matches at offset 0 —
    a genuine "Name [Title]" cell always LEADS with the person's name."""
    text = _normalize_first_cell(cell)
    if not text:
        return None
    m = _POSITION_ROLE_RE.search(text)
    if m is not None and m.start() == 0:
        return text
    return None


# Words that appear in SCT title text but essentially never inside a person's
# legal name. Used as NEGATIVE evidence when deciding whether a year-descending
# first cell opens a new NEO block (#2094 Codex ckpt-2 High): a candidate name
# containing any of these is a wrapped-title fragment, not a person. A negative
# vocabulary on the NAME side is robust where positive enumeration of every
# possible fragment START word is impossible.
_TITLE_VOCAB: Final[frozenset[str]] = frozenset(
    {
        "officer",
        "counsel",
        "secretary",
        "treasurer",
        "president",
        "chair",
        "chairman",
        "chairwoman",
        "chairperson",
        "chief",
        "executive",
        "vice",
        "senior",
        "principal",
        "general",
        "director",
        "founder",
        "ceo",
        "cfo",
        "coo",
        "cto",
        "evp",
        "svp",
        "vp",
        "former",
        "interim",
        "acting",
        "division",
        "group",
        "university",
        "system",
        "company",
        "corporation",
        "bank",
        "banking",
        "operations",
        "operating",
        "financial",
        "finance",
        "technology",
        "administrative",
        "administration",
        "compliance",
        "accounting",
        "marketing",
        "commercial",
        "resources",
        "human",
        "legal",
        "global",
        "strategy",
        "and",
        "of",
        "the",
    }
)

_TRAILING_CONNECTOR_RE: Final[re.Pattern[str]] = re.compile(r"[,&–—/-]\s*$")


def _plausible_person_name(text: str) -> bool:
    """True when TEXT plausibly is a person's name (not a title fragment).

    Person names have 2+ tokens, never end in a connector, and contain no
    title vocabulary. Only consulted on year-descending rows (#2094) to let
    a genuine new NEO whose block starts below the previous row's fiscal
    year (e.g. a departed exec listed after a current-year-only NEO) open a
    fresh block instead of being absorbed as a title continuation."""
    stripped = text.strip()
    if not stripped or _TRAILING_CONNECTOR_RE.search(stripped):
        return False
    tokens = [t for t in re.split(r"[\s,]+", stripped) if t]
    if len(tokens) < 2:
        return False
    return not any(t.lower().strip(".&/") in _TITLE_VOCAB for t in tokens)


def _backfill_position(rows: list[Def14AExecCompRow], name: str, position: str) -> None:
    """Rewrite the position on the carried NEO's already-emitted rows.

    Walks back over the contiguous tail of ROWS belonging to NAME and
    replaces the position outright — earlier rows may hold either ``None``
    (name row emitted before any title row, #2088) or a partial prefix of a
    title that wrapped over several physical rows (#2094)."""
    for i in range(len(rows) - 1, -1, -1):
        if rows[i].executive_name != name:
            break
        rows[i] = replace(rows[i], principal_position=position)


# ---------------------------------------------------------------------------
# PvP iXBRL NEO-name oracle + truncated-name repair (#2099 / #2100)
# ---------------------------------------------------------------------------
# Item 402(v)(3) requires the PvP footnotes to name every NEO; 402(v)(7) puts
# that disclosure inside the Inline-XBRL mandate. Filers tag the names as
# ``PeoName`` facts (ECD taxonomy, ns ``http://xbrl.sec.gov/ecd/YYYY``) in
# contexts dimensioned by ``ExecutiveCategoryAxis`` × ``IndividualAxis``.
# Specs: docs/proposals/etl/2026-07-22-def14a-pvp-neo-name-oracle.md and
# …-def14a-sct-residual-name-classes.md (full-population verification there).

_ECD_NS_URI_PREFIX: Final[str] = "http://xbrl.sec.gov/ecd"

# Suspicious executive_name trigger class: a single token is a truncation
# fingerprint (surname-only SCT labels, glued CJK romanisations).
_SUSPICIOUS_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z'’\-]+$")

# Glued camel name (``HechunWei``). First run ≥3 lowercase chars excludes
# Mc/La/De/Di-prefixed real surnames (McDonald, LaBelle, DiCaprio).
_CAMEL_GLUED_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Z][a-z]{2,}[A-Z][a-z]+$")

_HONORIFIC_RE: Final[re.Pattern[str]] = re.compile(r"^(?:mr|ms|mrs|dr)\.?\s+", re.IGNORECASE)


@dataclass(frozen=True)
class Def14APvpNeoName:
    """One person named in the PvP iXBRL facts.

    ``name_text`` is the fact value as the footnote renders it — may be an
    honorific form ("Mr. Cook"), never treated as more authoritative than the
    SCT's own HTML (the oracle is corroboration, not ground truth: a filer
    typo'd "Douglas P. Pferdehirt" for the real Douglas J.)."""

    name_text: str
    individual_member: str | None
    executive_category: str | None
    covered_end_years: frozenset[int]


def parse_pvp_neo_names(html_text: str) -> tuple[Def14APvpNeoName, ...]:
    """Extract the Item 402(v) ``PeoName`` facts from an iXBRL DEF 14A body.

    HTML-mode lxml traps (each verified empirically, spec D1): the HTML parser
    does NOT namespace-expand — ``ix:nonNumeric`` survives as the literal
    lowercased tag ``ix:nonnumeric``, ``nsmap`` is not populated, and
    attribute NAMES are lowercased (``contextRef`` → ``contextref``) while
    values keep their case. So: harvest ``xmlns:*`` declarations as literal
    attributes, split QNames manually, read lowercased attribute names.
    Matching is namespace-URI-resolved (ECD ns is versioned yearly), with a
    literal-``ecd`` fallback when no declaration is found (fact-level prefix
    drift measured 0 full-pop; the fallback covers mangled declarations).

    Best-effort: returns ``()`` on any parse failure — an absent oracle must
    never break the SCT parse."""
    if not html_text:
        return ()
    try:
        from lxml import html as lxml_html
    except ImportError:  # pragma: no cover - lxml is an app dependency
        return ()
    try:
        tree = lxml_html.fromstring(html_text.encode("utf-8", errors="replace"))
    except Exception:
        return ()

    ecd_prefixes: set[str] = set()
    contexts: list = []
    facts: list = []
    for el in tree.iter():
        tag = el.tag if isinstance(el.tag, str) else ""
        for attr, value in el.attrib.items():
            if attr.startswith("xmlns:") and value.startswith(_ECD_NS_URI_PREFIX):
                ecd_prefixes.add(attr[len("xmlns:") :].lower())
        if tag.endswith("context"):
            contexts.append(el)
        elif tag.endswith("nonnumeric"):
            facts.append(el)
    if not ecd_prefixes:
        ecd_prefixes = {"ecd"}

    def _is_ecd(qname: str | None, localname: str) -> bool:
        if not qname or ":" not in qname:
            return False
        prefix, local = qname.rsplit(":", 1)
        return prefix.lower() in ecd_prefixes and local.lower() == localname.lower()

    # contextRef → (individual member, executive category, period end-year)
    ctx_info: dict[str, tuple[str | None, str | None, int | None]] = {}
    for ctx in contexts:
        cid = ctx.get("id")
        if not cid:
            continue
        individual: str | None = None
        category: str | None = None
        end_year: int | None = None
        for child in ctx.iter():
            ctag = child.tag if isinstance(child.tag, str) else ""
            if ctag.endswith("explicitmember"):
                dim = child.get("dimension") or ""
                member = (child.text or "").strip()
                if _is_ecd(dim, "IndividualAxis"):
                    individual = member or None
                elif _is_ecd(dim, "ExecutiveCategoryAxis"):
                    category = member or None
            elif ctag.endswith("enddate") or ctag.endswith("instant"):
                raw = (child.text or "").strip()
                if len(raw) >= 4 and raw[:4].isdigit():
                    end_year = int(raw[:4])
        ctx_info[cid] = (individual, category, end_year)

    # Group facts by person (IndividualAxis member when present, else the
    # normalised name text) and union the covered end-years.
    grouped: dict[str, dict] = {}
    for fact in facts:
        if not _is_ecd(fact.get("name"), "PeoName"):
            continue
        text = _INLINE_WHITESPACE_RE.sub(" ", fact.text_content().replace("\n", " ")).strip()
        text = _TRAILING_FOOTNOTE_RE.sub("", text).strip()
        if not text:
            continue
        individual, category, end_year = ctx_info.get(fact.get("contextref") or "", (None, None, None))
        key = individual or text.lower()
        entry = grouped.setdefault(key, {"name": text, "individual": individual, "category": category, "years": set()})
        if len(text) > len(entry["name"]):
            entry["name"] = text  # keep the most complete rendering
        entry["category"] = entry["category"] or category
        if end_year is not None:
            entry["years"].add(end_year)

    return tuple(
        Def14APvpNeoName(
            name_text=e["name"],
            individual_member=e["individual"],
            executive_category=e["category"],
            covered_end_years=frozenset(e["years"]),
        )
        for e in grouped.values()
    )


def _name_token_seq(name: str) -> tuple[str, ...]:
    """Lowercased comparison tokens IN ORDER: honorific-stripped, punctuation
    split. Order is kept because token order is identity-bearing — "Hechun
    Wei" and "Wei Hechun" are different people (fresh-agent review)."""
    s = _HONORIFIC_RE.sub("", name.strip())
    return tuple(t for t in re.split(r"[^\w'’]+", s.lower()) if t)


def _name_tokens(name: str) -> frozenset[str]:
    """Set form of :func:`_name_token_seq` for the subset tests (initials
    included — they stay in replacement text and are material for
    disagreement, spec C2)."""
    return frozenset(_name_token_seq(name))


def _candidates_agree(a: str, b: str) -> bool:
    """Spec C2 agreement: STRICT full-token subset either way ("Cook" ⊆
    "Tim Cook"; "Damon Hininger" ⊆ "Damon T. Hininger" — the one-side-initials
    case is covered by the subset branch). Equal token SETS agree only when
    the token ORDER also matches — a permutation ("Hechun Wei" vs
    "Wei Hechun") is two different people, not agreement. Conflicting
    initials ("Douglas J." vs "Douglas P.": neither set a subset) are a
    disagreement."""
    ta, tb = _name_tokens(a), _name_tokens(b)
    if ta == tb:
        return _name_token_seq(a) == _name_token_seq(b)
    return ta < tb or tb < ta


def _flatten_document_text(html_text: str) -> str:
    """Tag-strip + entity-decode + whitespace-collapse the whole body (camel
    verbatim check only — no pattern harvesting, spec C2). ``<script>`` /
    ``<style>`` blocks are dropped WITH their contents so embedded JS/CSS
    text can never "validate" a camel split (review NITPICK)."""
    text = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", " ", html_text)
    return _INLINE_WHITESPACE_RE.sub(" ", html.unescape(re.sub(r"<[^>]+>", " ", text)).replace("\n", " "))


def _repair_truncated_names(rows: list[Def14AExecCompRow], html_text: str) -> list[Def14AExecCompRow]:
    """Repair single-token executive names from same-document evidence
    (#2100 C2): intra-SCT sibling superset, camel-verbatim spaced form, and
    the FY-gated PvP oracle. Repair fires only on unanimous candidates; a
    (replacement, fiscal_year) collision with an existing parsed row skips
    the repair entirely (no partial renames — the FTI case, where a
    conflicting same-FY total must stay visible under its own label)."""
    suspicious = sorted({r.executive_name for r in rows if _SUSPICIOUS_NAME_RE.fullmatch(r.executive_name.strip())})
    if not suspicious:
        return rows

    fy_by_name: dict[str, set[int]] = {}
    for r in rows:
        fy_by_name.setdefault(r.executive_name, set()).add(r.fiscal_year)
    distinct_names = sorted(fy_by_name)

    doc_text: str | None = None
    oracle: tuple[Def14APvpNeoName, ...] | None = None
    renames: dict[str, str] = {}

    for name in suspicious:
        ntoks = _name_tokens(name)
        if not ntoks:
            continue
        # (source-priority order; replacement picks the most complete form,
        # ties broken sibling > camel > oracle)
        candidates: list[str] = []

        for other in distinct_names:
            if other != name and ntoks < _name_tokens(other):
                candidates.append(other)

        if _CAMEL_GLUED_RE.fullmatch(name.strip()):
            if doc_text is None:
                doc_text = _flatten_document_text(html_text)
            spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name.strip())
            # Word-bounded, not substring — "Jon Smith" must not be
            # "validated" by an unrelated "Jon Smithson" in the prose
            # (fresh-agent review).
            if re.search(rf"(?<![A-Za-z]){re.escape(spaced)}(?![A-Za-z])", doc_text):
                candidates.append(spaced)

        if oracle is None:
            oracle = parse_pvp_neo_names(html_text)
        row_years = fy_by_name[name]
        for person in oracle:
            ptoks = _name_tokens(person.name_text)
            if not (ntoks < ptoks):
                continue
            # Per-name atomic FY gate: EVERY row year must fall inside the
            # person's covered period-end years (fy label ≤ end-year ≤
            # label+1 tolerates non-calendar fiscal years).
            covered = person.covered_end_years
            if not covered or not all((fy in covered or fy + 1 in covered) for fy in row_years):
                continue
            cleaned = _HONORIFIC_RE.sub("", person.name_text).strip()
            if len(_name_tokens(cleaned)) > len(ntoks):
                candidates.append(cleaned)

        if not candidates:
            continue
        if any(not _candidates_agree(a, b) for i, a in enumerate(candidates) for b in candidates[i + 1 :]):
            continue
        replacement = max(candidates, key=lambda c: (len(_name_tokens(c)), -candidates.index(c)))
        if replacement == name:
            continue
        # Collision guard: never merge onto an existing (name, fy) row — and
        # check EVERY agreeing candidate, not just the chosen text (Codex
        # ckpt-2 P2: a same-FY sibling row under a shorter agreeing spelling
        # must block the repair too, else the same person splits across two
        # spellings).
        if any(fy_by_name.get(c, set()) & row_years for c in candidates):
            continue
        renames[name] = replacement

    if not renames:
        return rows
    return [replace(r, executive_name=renames[r.executive_name]) if r.executive_name in renames else r for r in rows]


def _extract_sct_row_values(cells_after_year: list[str]) -> list[Decimal | None]:
    """Compact the post-year cells into ordered value slots.

    Drops layout spacers ('' / lone '$' / footnote-only / bare
    footnote-superscript integers) but KEEPS explicit ``—``/``N/A`` nulls
    (they are real columns with no value). Returns the value list to zip
    against the reg-ordered present fields."""
    values: list[Decimal | None] = []
    for cell in cells_after_year:
        s = cell.strip()
        if s == "" or s == "$":
            continue
        # Footnote-only cell (e.g. "(3)(4)") strips to empty → spacer.
        stripped_fn = _FOOTNOTE_RE.sub("", s).strip()
        if stripped_fn == "":
            continue
        low = stripped_fn.lower()
        if low in _DASH_NULLS:
            values.append(None)  # explicit null column
            continue
        # Bare footnote superscript ('6') — not a dollar amount.
        if _BARE_FOOTNOTE_INT_RE.match(stripped_fn):
            continue
        parsed = _parse_dollar(s)
        if parsed is not None:
            values.append(parsed)
    return values


def _map_sct_values(fields: tuple[str, ...], values: list[Decimal | None]) -> dict[str, Decimal | None]:
    """Map extracted values onto the reg-ordered present fields.

    Clean case (``len(values) == len(fields)``) zips directly. When the
    counts differ — a filer rendered an interior null column as a BLANK
    cell (dropped as a spacer) rather than ``—``, or emitted an extra
    stray cell — the interior mapping is ambiguous. Rather than emit
    WRONG middle components, trust only the reg-anchored ends: Total is
    always the last SCT column (§ 229.402(c)(2)(x)) and Salary the first
    dollar column ((c)(2)(iii)), so those two are read off the ends and
    the ambiguous middle is left NULL. This keeps the headline thesis
    figure (total_comp) correct on every emitted row."""
    mapped: dict[str, Decimal | None] = dict.fromkeys(_SCT_ALL_FIELDS, None)
    if not values:
        return mapped
    if len(values) == len(fields):
        for field, value in zip(fields, values, strict=True):
            mapped[field] = value
        return mapped
    mapped[fields[0]] = values[0]
    # Anchor Total to the last value ONLY when Total is the last resolved field
    # — reg § 229.402(c)(2)(x) mandates Total as the rightmost SCT column, so a
    # compliant header resolves total_comp last. If a non-compliant header put
    # Total elsewhere, we do NOT mis-anchor (leave total NULL) rather than emit
    # a wrong figure.
    if fields[-1] == "total_comp":
        mapped["total_comp"] = values[-1]
    return mapped


def _find_sct_windows(html_text: str) -> list[tuple[int, int]]:
    """Candidate byte windows for the SCT — ONE per "Summary Compensation
    Table" heading occurrence (not inside a table), each capped to
    ``_SECTION_SCAN_BYTES``, with overlapping/adjacent windows merged into
    contiguous ranges. Occurrences arrive in document order (finditer), so a
    single left-to-right merge pass suffices. Falls back to the whole document
    only when the phrase never appears (a non-SCT proxy — parse returns 0 rows
    fast anyway)."""
    windows: list[tuple[int, int]] = []
    for match in _SCT_SECTION_HEADING_RE.finditer(html_text):
        start = match.start()
        if _is_inside_table(html_text, start):
            continue
        end = min(start + _SECTION_SCAN_BYTES, len(html_text))
        if windows and start <= windows[-1][1]:
            windows[-1] = (windows[-1][0], max(windows[-1][1], end))
        else:
            windows.append((start, end))
    if not windows:
        windows.append((0, len(html_text)))
    return windows


# Score floor: a genuine SCT (name/position + salary + stock/option + total)
# scores well above this; the director-comp / plan-awards look-alikes lack
# the salary keyword and score below it.
_SCT_SCORE_FLOOR: Final[int] = 6


def parse_summary_compensation_table(html_text: str) -> Def14ASummaryCompTable:
    """Parse a DEF 14A body and extract the Item 402(c) Summary
    Compensation Table.

    Returns empty rows when no candidate table scores above the floor
    (many proxies carry no SCT — DEFA14A soliciting material, merger
    proxies, notice-only meetings; expected, not a defect). Best-effort:
    does not raise on malformed HTML."""
    if not html_text:
        return Def14ASummaryCompTable(rows=(), raw_table_score=0)

    # The phrase "Summary Compensation Table" recurs MANY times in a proxy
    # (TOC, CD&A cross-references, Pay-vs-Performance footnotes, the table
    # caption itself) — HD's proxy has 22 hits. The real table sits at an
    # arbitrary MIDDLE occurrence, so first/last-window heuristics miss it.
    # Evaluate a window at EVERY occurrence (merged; each capped to
    # _SECTION_SCAN_BYTES) and pick the GLOBAL highest-scoring table that is a
    # VALID SCT (has both the mandatory Salary § 229.402(c)(2)(iii) and Total
    # (c)(2)(x) columns). Folding the salary+total requirement into SELECTION
    # — not just a post-hoc gate — is what stops a higher-scoring
    # Pay-versus-Performance look-alike (Total but no Salary; negative
    # "Compensation Actually Paid" values) from beating the real SCT.
    best_score = 0
    best_table: _RawTable | None = None
    for window_start, window_end in _find_sct_windows(html_text):
        for start, end in _scan_outer_tables(html_text, start=window_start, end=window_end):
            # ``expand_spans=False``: the Item 402(c) path carries its OWN
            # compensation for rowspan-shifted rows (see the module comment above
            # `Def14AExecCompRow` — "name-cell rowspan → continuation-year rows
            # are index-shifted (AAPL/MSFT)"), built and tuned across #1945,
            # #1967, #2088, #2094 and #2097. Feeding it the span-restored rows
            # re-bases that machinery, and the full-population A/B measured the
            # result: **580 accessions** drifted, every one of them the same way
            # — ``executive_name``, ``fiscal_year``, ``salary`` and ``total_comp``
            # identical, and ``principal_position`` repeated once per
            # continuation year ('Executive Vice President and Chief Financial
            # Officer' ×3 on 0000004904-25-000043). Re-basing this arm on the
            # table model is the right end state and is NOT this ticket — #2175
            # is Item 403. Keeping the SCT input byte-identical is what makes
            # that separable. See the follow-up issue in the PR description.
            parsed = _parse_table_html(html_text[start:end], expand_spans=False)
            if parsed is None:
                continue
            score = _score_sct_headers(parsed.score_headers)
            if score < _SCT_SCORE_FLOOR or score <= best_score:
                continue
            # Require a NAME column header (§ 229.402(c)(2)(i) "Name and
            # Principal Position") — some filers header it just "Name" with the
            # title shown inline in the data cells, so match the broad "name"
            # substring rather than the full phrase (requiring "principal
            # position" cost ~14pp of real yield). Combined with the
            # salary+total requirement below this discriminates the SCT from
            # adjacent look-alikes: the Pay-vs-Performance table has no "name"
            # column (Year / PEO / Non-PEO), and the Director Compensation table
            # has no Salary column.
            header_join = " ".join(parsed.score_headers).lower()
            if "name" not in header_join:
                continue
            candidate_fields = _resolve_sct_fields(parsed.column_headers)
            if "salary" not in candidate_fields or "total_comp" not in candidate_fields:
                continue
            best_score = score
            best_table = parsed

    if best_table is None:
        logger.debug("DEF 14A: no valid SCT met score floor; best_score=%d", best_score)
        return Def14ASummaryCompTable(rows=(), raw_table_score=best_score)

    fields = _resolve_sct_fields(best_table.column_headers)

    rows: list[Def14AExecCompRow] = []
    current_name = ""
    current_position: str | None = None
    prev_row_year: int | None = None

    for raw_row in best_table.rows:
        cells = [_sct_norm(c) for c in raw_row]
        if not any(cells):
            continue

        first_nonempty_idx = next((i for i, c in enumerate(cells) if c), None)
        if first_nonempty_idx is None:
            continue

        # Fiscal-year token first — the name-cell decision below needs THIS
        # row's year to detect wrapped-title continuations (#2094).
        year_idx = None
        for i, c in enumerate(cells):
            if _YEAR_RE.match(_FOOTNOTE_RE.sub("", c).strip()):
                year_idx = i
                break
        row_year = int(_FOOTNOTE_RE.sub("", cells[year_idx]).strip()) if year_idx is not None else None

        # Leading name cell? (present on the first row per NEO; absent on
        # rowspan continuation rows.)
        if _looks_like_name_cell(cells[first_nonempty_idx]):
            first_cell = cells[first_nonempty_idx]
            cleaned = _normalize_first_cell(first_cell)
            position_fragment = _position_only_cell(first_cell)
            # #2094 — wrapped first-column layouts (PRDO/HBNC) spread ONE
            # logical name+title cell across the NEO block's physical
            # per-fiscal-year rows. A new NEO's block always restarts at a
            # NEWER year (§ 229.402(c)(2)(ii) rows render newest-first), so a
            # name-like cell on a year-DESCENDING row is a continuation
            # fragment of the carried NEO's title, whatever word it starts
            # with — a role lexicon cannot enumerate mid-title fragments
            # ("Officer", "EVP,", "Technical University").
            year_descends = (
                bool(current_name) and row_year is not None and prev_row_year is not None and row_year < prev_row_year
            )
            restated_name = bool(current_name) and (cleaned == current_name or cleaned.startswith(current_name + " "))
            if restated_name:
                # Per-year name repeat (no rowspan) — same NEO, keep the carry.
                pass
            elif position_fragment is not None and current_name:
                # Stacked name/position layout (GME, #2088): the title rides
                # a LATER physical row — it belongs to the carried NEO, it is
                # not a new name. Earlier emitted rows get the position
                # backfilled; a lexicon-matching TAIL of a wrapped title
                # (#2094 "…Chief Financial Officer") appends instead.
                if current_position is None:
                    current_position = position_fragment
                    _backfill_position(rows, current_name, current_position)
                elif year_descends:
                    current_position = f"{current_position} {position_fragment}"
                    _backfill_position(rows, current_name, current_position)
                # else: repeated full-title row — drop, keep the carry.
            elif year_descends:
                # A genuine NEW NEO may still open on a descending year — a
                # departed exec's block starts below a current-year-only
                # NEO's row (Codex ckpt-2 High). Escape when the cell splits
                # into a plausible person name + a title; otherwise it is a
                # wrapped-title fragment and appends to the carry.
                cand_name, cand_pos = _split_name_position(first_cell)
                if cand_pos is not None and _plausible_person_name(cand_name):
                    current_name, current_position = cand_name, cand_pos
                else:
                    current_position = f"{current_position} {cleaned}" if current_position else cleaned
                    _backfill_position(rows, current_name, current_position)
            else:
                current_name, current_position = _split_name_position(first_cell)

        if row_year is not None:
            prev_row_year = row_year

        if year_idx is None or row_year is None:
            # Name-only header row (HD) or a prose row — nothing to emit.
            continue
        if not current_name:
            continue  # values with no NEO context yet — skip defensively.

        fiscal_year = row_year
        values = _extract_sct_row_values(cells[year_idx + 1 :])
        if not values:
            continue

        mapped = _map_sct_values(fields, values)

        # Defensive: an SCT total is non-negative by construction. A negative
        # here means a Pay-vs-Performance "Compensation Actually Paid" row
        # slipped through — drop it rather than store a wrong figure.
        total = mapped["total_comp"]
        if total is not None and total < 0:
            continue

        rows.append(
            Def14AExecCompRow(
                executive_name=current_name,
                principal_position=current_position,
                fiscal_year=fiscal_year,
                salary=mapped["salary"],
                bonus=mapped["bonus"],
                stock_awards=mapped["stock_awards"],
                option_awards=mapped["option_awards"],
                non_equity_incentive=mapped["non_equity_incentive"],
                pension_nqdc=mapped["pension_nqdc"],
                other_comp=mapped["other_comp"],
                total_comp=mapped["total_comp"],
            )
        )

    # #2100 C2 — same-document truncated-name repair (single-token names
    # only; unanimous evidence only). Best-effort: never fails the parse.
    try:
        rows = _repair_truncated_names(rows, html_text)
    except Exception:  # pragma: no cover - defensive; parser must not raise
        logger.exception("SCT name repair failed; keeping unrepaired names")

    return Def14ASummaryCompTable(rows=tuple(rows), raw_table_score=best_score)
