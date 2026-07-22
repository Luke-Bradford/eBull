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
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final

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
_CELL_RE: Final[re.Pattern[str]] = re.compile(r"<(?:t[hd])\b[^>]*>(.*?)</t[hd]\s*>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")
_NBSP_RE: Final[re.Pattern[str]] = re.compile(r"&nbsp;| ")
_INLINE_WHITESPACE_RE: Final[re.Pattern[str]] = re.compile(r"[ \t\r\f\v]+")

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


def _strip_inline_html(raw: str) -> str:
    """Strip HTML tags + entities, collapse whitespace. Used on cell
    contents so footnote-superscript ``<sup>(1)</sup>`` markers
    survive as plain ``(1)`` text and can be detected by the
    footnote-stripping regex below.
    """
    no_tags = _HTML_TAG_RE.sub(" ", raw)
    no_nbsp = _NBSP_RE.sub(" ", no_tags)
    decoded = html.unescape(no_nbsp)
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


def _score_table_headers(headers: tuple[str, ...]) -> int:
    """Score a candidate table's header row. Higher is better."""
    if not headers:
        return 0
    joined = " ".join(headers).lower()
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
    """

    score_headers: tuple[str, ...]
    column_headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


_NUMERIC_LIKE_RE: Final[re.Pattern[str]] = re.compile(r"\d{2,}")


def _looks_like_subheader(cells: tuple[str, ...]) -> bool:
    """True when a row looks like a sub-header continuation rather
    than a data row.

    A sub-header row:
      * Has no cell containing a multi-digit run (data rows have
        share counts like ``1,500,000``).
      * Has at least one cell containing a column-label keyword
        like ``Sole`` / ``Shared`` / ``Total`` / ``Voting`` /
        ``Dispositive`` — these are the SEC-prescribed subdivisions
        of the ``Amount and Nature of Beneficial Ownership``
        merged-header column.
    Codex pre-push review caught the merged-header case where a
    proxy uses two header rows and the parser only saw row 0.
    """
    if not cells:
        return False
    for c in cells:
        if _NUMERIC_LIKE_RE.search(c):
            return False
    joined = " ".join(cells).lower()
    # Sub-header keywords scope tightly to ownership-block subdivisions.
    # ``common`` / ``preferred`` were originally on this list as share
    # class indicators but they collide with legitimate holder names
    # (e.g. ``"Common Fund LLC"``) — Codex / bot review caught the
    # false positive: a one-cell holder-name row with "common" in the
    # name AND no numeric cell would be silently promoted to column
    # headers and dropped from the data set. Removed both.
    sub_keywords = ("sole", "shared", "total", "voting", "dispositive")
    return any(k in joined for k in sub_keywords)


def _parse_table_html(table_html: str) -> _RawTable | None:
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
    open_match = _TABLE_OPEN_RE.search(table_html)
    close_idx = table_html.rfind("</table")
    if open_match is None or close_idx == -1:
        return None
    inner = table_html[open_match.end() : close_idx]
    nested = _scan_outer_tables(inner)
    if nested:
        pieces: list[str] = []
        cursor = 0
        for start, end in nested:
            pieces.append(inner[cursor:start])
            pieces.append(" ")
            cursor = end
        pieces.append(inner[cursor:])
        scrubbed = "".join(pieces)
    else:
        scrubbed = inner
    cells_per_row: list[tuple[str, ...]] = []
    for tr_match in _TR_RE.finditer(scrubbed):
        cells = tuple(_strip_inline_html(cell) for cell in _CELL_RE.findall(tr_match.group(1)))
        if any(c for c in cells):
            cells_per_row.append(cells)
    if not cells_per_row:
        return None

    parent_headers = cells_per_row[0]
    body = cells_per_row[1:]
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
        if len(parent_headers) < max_data_width and _looks_like_subheader(body[0]):
            column_headers = body[0]
            score_headers = parent_headers + body[0]
            body = body[1:]

    return _RawTable(score_headers=score_headers, column_headers=column_headers, rows=tuple(body))


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
    """Strip footnote markers from the holder name; keep the rest."""
    return _FOOTNOTE_RE.sub("", raw).strip()


def _parse_share_count(raw: str) -> Decimal | None:
    """Parse a share-count cell. Accepts ``"1,234,567"`` /
    ``"1234567"`` / ``"1,234,567(1)"`` / dash / em-dash / empty."""
    if not raw:
        return None
    cleaned = _FOOTNOTE_RE.sub("", raw).strip().replace(",", "").replace(" ", "")
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
    cleaned = raw.strip().replace("%", "").replace(",", "").strip()
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

    Defaults to ``(0, 1, len(headers) - 1)`` when no header match
    fires.
    """
    name_idx = -1
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

    for i, h in enumerate(headers):
        lower = h.lower()
        if name_idx == -1 and ("name" in lower or "beneficial" in lower):
            name_idx = i
        if percent_idx == -1 and ("percent" in lower or "%" in lower):
            percent_idx = i
        for keyword, weight in SHARES_TIERS:
            if keyword in lower:
                shares_tier_priority.append((str(i), weight))
                break

    if shares_tier_priority:
        # Sort by weight DESC (highest tier first), then by original
        # column position ASC so the leftmost top-tier column wins.
        shares_tier_priority.sort(key=lambda x: (-x[1], int(x[0])))
        shares_idx = int(shares_tier_priority[0][0])

    # Fallbacks. Many issuers use a multi-line nested-header style
    # where the first row is "Name and Address / Amount and Nature
    # of Beneficial Ownership / Percent of Class" with a sub-row
    # "Sole / Shared / Total". The scoring pass picks the merged
    # header row but column resolution can still see ambiguous
    # entries.
    if name_idx == -1:
        name_idx = 0
    if shares_idx == -1:
        shares_idx = 1 if len(headers) >= 2 else 0
    if percent_idx == -1:
        percent_idx = len(headers) - 1
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
    (re.compile(r"5\s*%.*holders?", re.IGNORECASE), "principal"),
    (re.compile(r"principal\s+(?:share|stock)holders?", re.IGNORECASE), "principal"),
    (re.compile(r"all\s+(?:directors?\s+and\s+)?executive\s+officers?\s+as\s+a\s+group", re.IGNORECASE), "group"),
)


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

    # Score floor — below this we don't trust the match. Empirically
    # tuned: a minimal-header beneficial-ownership table
    # (``Name`` / ``Shares`` / ``Percent``) scores 3; tables with
    # SEC-prescribed wording score 6+. Compensation / option-grant
    # tables typically score 0-2 even when they include a "Name"
    # column because they lack ``shares``/``percent`` cues.
    SCORE_FLOOR = 3

    candidate_windows = _find_section_windows(html_text)
    best_score = 0
    best_table: _RawTable | None = None
    chosen_window: tuple[int, int] | None = None

    # Multi-pass: try each priority window in order; the first one
    # whose best table meets the floor wins.
    for window_start, window_end in candidate_windows:
        candidate_tables = _scan_outer_tables(html_text, start=window_start, end=window_end)
        window_best_score = 0
        window_best_table: _RawTable | None = None
        for start, end in candidate_tables:
            parsed = _parse_table_html(html_text[start:end])
            if parsed is None:
                continue
            score = _score_table_headers(parsed.score_headers)
            if score > window_best_score:
                window_best_score = score
                window_best_table = parsed
        if window_best_table is not None and window_best_score >= SCORE_FLOOR:
            best_score = window_best_score
            best_table = window_best_table
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

    name_idx, shares_idx, percent_idx = _resolve_columns(best_table.column_headers)
    rows: list[Def14ABeneficialHolder] = []
    current_role: str | None = None

    for raw_row in best_table.rows:
        # Single-cell heading rows flip the role tag.
        heading_role = _detect_role_heading(raw_row)
        if heading_role is not None:
            current_role = heading_role
            continue

        # Skip totally-empty rows defensively (the regex above
        # already filters most but trailing footnote rows can slip
        # through with one whitespace-only cell).
        if not any(c.strip() for c in raw_row):
            continue

        # Pad short rows (some issuers omit trailing cells when the
        # value is blank) so positional access doesn't IndexError.
        cells = list(raw_row) + [""] * max(0, percent_idx + 1 - len(raw_row))

        holder_name_raw = cells[name_idx] if name_idx < len(cells) else ""
        shares_raw = cells[shares_idx] if shares_idx < len(cells) else ""
        percent_raw = cells[percent_idx] if percent_idx < len(cells) else ""

        holder_name = _clean_holder_name(holder_name_raw)
        if not holder_name:
            continue
        shares = _parse_share_count(shares_raw)
        percent = _parse_percent(percent_raw)

        # Drop rows where neither shares nor percent parsed — that's
        # almost always a free-text annotation row ("Notes:",
        # "(continued from previous page)") and not real data.
        if shares is None and percent is None:
            continue

        role = current_role or _detect_inline_role(holder_name)

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

        rows.append(
            Def14ABeneficialHolder(
                holder_name=holder_name,
                holder_role=role,
                shares=shares,
                percent_of_class=percent,
            )
        )

    return Def14ABeneficialOwnershipTable(
        as_of_date=as_of_date,
        rows=rows,
        raw_table_score=best_score,
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
            parsed = _parse_table_html(html_text[start:end])
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
