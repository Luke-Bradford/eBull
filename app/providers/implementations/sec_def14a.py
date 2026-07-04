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
from dataclasses import dataclass
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
_POSITION_ROLE_RE: Final[re.Pattern[str]] = re.compile(
    r"\b("
    r"chief\s+\w+|"
    r"executive\s+vice\s+president|senior\s+vice\s+president|vice\s+president|"
    r"president|chair(?:man|woman|person)?|"
    r"general\s+counsel|chief|ceo|cfo|coo|cto|"
    r"executive\s+officer|principal\s+\w+|treasurer|secretary|"
    r"director|founder"
    r")\b",
    re.IGNORECASE,
)


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
    """Split a "Name / Title" SCT first cell into (name, position).

    Prefers a newline delimiter (AAPL/MSFT put the name on line 1, the
    title on line 2 — ``_strip_inline_html`` preserves ``\\n``). Falls
    back to splitting at the first position-role keyword (JPM/HD render
    "James Dimon Chairman and CEO" on one line). Footnote markers are
    stripped from the name. Position is raw free-text (v1)."""
    text = _clean_holder_name(cell).replace("\xa0", " ").strip()
    text = _INLINE_WHITESPACE_RE.sub(" ", text)
    if not text:
        return "", None
    # Newline delimiter → line 1 is the name, remainder is the title.
    if "\n" in text:
        name, _, position = text.partition("\n")
        pos = _INLINE_WHITESPACE_RE.sub(" ", position.replace("\n", " ")).strip()
        return name.strip(), (pos or None)
    # Otherwise split at the first role keyword.
    m = _POSITION_ROLE_RE.search(text)
    if m and m.start() > 0:
        return text[: m.start()].strip().rstrip(",").strip(), text[m.start() :].strip() or None
    return text, None


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

    for raw_row in best_table.rows:
        cells = [_sct_norm(c) for c in raw_row]
        if not any(cells):
            continue

        # Leading name cell? (present on the first row per NEO; absent on
        # rowspan continuation rows.)
        first_nonempty_idx = next((i for i, c in enumerate(cells) if c), None)
        if first_nonempty_idx is None:
            continue
        if _looks_like_name_cell(cells[first_nonempty_idx]):
            current_name, current_position = _split_name_position(cells[first_nonempty_idx])

        # Locate the fiscal-year token.
        year_idx = None
        for i, c in enumerate(cells):
            if _YEAR_RE.match(_FOOTNOTE_RE.sub("", c).strip()):
                year_idx = i
                break
        if year_idx is None:
            # Name-only header row (HD) or a prose row — nothing to emit.
            continue
        if not current_name:
            continue  # values with no NEO context yet — skip defensively.

        fiscal_year = int(_FOOTNOTE_RE.sub("", cells[year_idx]).strip())
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

    return Def14ASummaryCompTable(rows=tuple(rows), raw_table_score=best_score)
