"""10-K Item 1 "Business" narrative extractor + ingester (#428 / #449).

Replaces the Yahoo ``longBusinessSummary`` blurb with the
authoritative multi-page description every SEC 10-K carries under
Item 1. Free, official, bounded per issuer to ~quarterly cadence
(10-K + 10-K/A amendments).

Shape mirrors :mod:`app.services.dividend_calendar` (#434):

- :func:`extract_business_section` is a pure function over raw HTML
  returning the whole-Item-1 blob (#428).
- :func:`extract_business_sections` (#449) is a pure function over
  raw HTML returning the subsection-level breakdown: every heading
  inside Item 1 becomes its own :class:`ParsedBusinessSection` with
  a canonical ``section_key``, the verbatim ``section_label``, the
  body text, and a list of cross-references to other items /
  exhibits / notes.
- :func:`ingest_business_summaries` is the DB path, bounded per run
  with a 7-day TTL on ``last_parsed_at`` so repeat fetches don't
  consume SEC rate-limit budget. It populates both the blob table
  (``instrument_business_summary``) and the sections table
  (``instrument_business_summary_sections``).

Acceptance bar from issue #428: instrument page shows an authentic
10-K business description for US tickers with a 10-K on file;
yfinance fallback only when absent.

Acceptance bar from issue #449: every Item 1 subsection lands as a
queryable row — no silent drops; unmapped headings surface as
``section_key='other'`` with the original heading preserved.
"""

from __future__ import annotations

import html
import logging
import re
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Failure-reason taxonomy + exponential backoff (#533)
# ---------------------------------------------------------------------


# Closed set of failure categories the ingester records on each
# parse miss. Operator-facing — surfaced verbatim in the admin
# failure dashboard. Adding a new value means updating both this
# Literal and the admin UI's reason→label map.
FailureReason = Literal[
    "fetch_http_5xx",  # SEC server error
    "fetch_http_4xx",  # 4xx other than 404/410 (which return None)
    "fetch_timeout",  # connection timeout / read timeout
    "fetch_other",  # unclassified fetch failure (provider returned None or unknown exception)
    "no_item_1_marker",  # Item 1 heading absent from document (10-K/A Part-III amendments etc.)
    "body_too_short",  # marker found but slice below ``_MIN_BODY_LEN`` (TOC-only)
    "parse_exception",  # extractor raised
    "upsert_exception",  # DB write raised
    "legacy_tombstone",  # backfill marker for tombstones predating #533
]


# Backoff schedule: attempt_count → days until next retry. Caps at
# 365 days (effective quarantine) — a row that has missed 4 times
# in a row is almost always a real classification or content issue
# the operator needs to address (filing_type leak, broken URL,
# parser gap), not a transient blip.
_BACKOFF_SCHEDULE_DAYS: dict[int, int] = {
    1: 1,
    2: 7,
    3: 30,
}
_QUARANTINE_DAYS = 365


def _next_retry_days(attempt_count: int) -> int:
    """Return the number of days until the next retry given the
    attempt counter. ``attempt_count`` is the count AFTER the
    current failure has been recorded (i.e. 1 means "first failure",
    not "before any failure"). Quarantine kicks in at 4+."""
    return _BACKOFF_SCHEDULE_DAYS.get(attempt_count, _QUARANTINE_DAYS)


# ---------------------------------------------------------------------
# HTML stripping — shared with dividend_calendar in spirit, duplicated
# here so the two parsers don't couple on each other.
# ---------------------------------------------------------------------


_IXBRL_TAG_RE = re.compile(r"<ix:[^>]*>|</ix:[^>]*>", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NBSP_RE = re.compile(r"&nbsp;|&#160;|&#xa0;| ", re.IGNORECASE)

# Block-level boundary tags that should produce a paragraph break in
# the stripped output. Closing tags (``</p>``, ``</div>``, ``</li>``,
# ``</tr>``, ``</h*>``) plus self-closing line breaks (``<br>``,
# ``<br/>``). Pre-fix the stripped output collapsed every block
# boundary to a single space, producing run-on text where every
# original paragraph fused into one wall.
_BLOCK_BREAK_RE = re.compile(
    r"</p\s*>|</div\s*>|</li\s*>|</tr\s*>|</h[1-6]\s*>|<br\s*/?>",
    re.IGNORECASE,
)

# Inline whitespace (spaces, tabs) but NOT newlines — preserves the
# paragraph breaks injected by ``_BLOCK_BREAK_RE``. Followed by a
# pass that collapses 3+ consecutive newlines to exactly two so the
# final body has at most one blank line between paragraphs.
_INLINE_WHITESPACE_RE = re.compile(r"[ \t\r\f\v]+")
_EXCESS_NEWLINES_RE = re.compile(r"\n{3,}")


def _strip_html(raw: str) -> str:
    """Strip HTML + iXBRL to a plain-text stream with paragraph
    structure preserved.

    iXBRL tags are stripped independently first because their
    attribute content (``contextref``, ``unitref``, ...) otherwise
    leaks through a naive ``<[^>]+>`` pass when the browser-tolerant
    markup has nested/unbalanced tags that confuse the simpler
    regex. Attribute content is never user-facing narrative.

    Block-boundary tags (``</p>``, ``</div>``, ``</li>``, ``<br>``,
    ``</h*>``) become double newlines BEFORE the generic tag strip so
    the resulting text retains paragraph breaks. Inline whitespace
    (spaces / tabs) collapses to single space; consecutive newlines
    cap at two (one blank line between paragraphs).

    HTML entities (``&#8220;``, ``&amp;``, ``&rsquo;``, etc.) get
    decoded via ``html.unescape`` so the rendered narrative reads as
    natural prose instead of ``GameStop Corp. (&#8220;GameStop&#8221;)``.
    """
    # Block boundaries → paragraph break sentinel BEFORE the generic
    # tag strip so the boundary survives.
    with_breaks = _BLOCK_BREAK_RE.sub("\n\n", raw)
    # Strip iXBRL element wrappers; the inner text survives.
    no_ix = _IXBRL_TAG_RE.sub(" ", with_breaks)
    no_tags = _HTML_TAG_RE.sub(" ", no_ix)
    no_nbsp = _NBSP_RE.sub(" ", no_tags)
    # Decode HTML entities — &#8220; → ", &amp; → &, &rsquo; → ', etc.
    decoded = html.unescape(no_nbsp)
    # Collapse runs of inline whitespace (NOT newlines) to single space.
    inline_collapsed = _INLINE_WHITESPACE_RE.sub(" ", decoded)
    # Cap consecutive newlines at two so the body has at most one
    # blank line between paragraphs.
    paragraph_collapsed = _EXCESS_NEWLINES_RE.sub("\n\n", inline_collapsed)
    return paragraph_collapsed.strip()


# ---------------------------------------------------------------------
# Section extraction
# ---------------------------------------------------------------------


# Byte cap on the stored body. 10 KB is large enough for multi-page
# Item 1 bodies (empirical: biggest Aristocrats 10-Ks run 6–8 KB after
# whitespace collapse) with headroom for future "more…" expanders,
# and small enough that TOASTing stays cheap.
MAX_BODY_BYTES = 10 * 1024

# "Item 1. Business" — case-insensitive, tolerant of extra
# whitespace. The dot after the 1 is mandatory — 10-Ks consistently
# use the dotted form, and a dot-less match picked up false positives
# mid-sentence ("item 1 cause of action" etc.) in pilot runs.
_ITEM_1_RE = re.compile(r"\bItem\s+1\.\s*Business\b", re.IGNORECASE)

# "Item 1A. Risk Factors" is the universal end marker. If absent we
# fall back to a byte-cap slice from the Item 1 position.
_ITEM_1A_RE = re.compile(r"\bItem\s+1A\.\s*Risk\s+Factors\b", re.IGNORECASE)


def extract_business_section(raw_html: str) -> str | None:
    """Return the "Item 1. Business" narrative as plain text.

    Returns ``None`` when the Item 1 heading is absent. Otherwise
    returns the slice between the last Item 1 heading preceding the
    Item 1A marker and the Item 1A marker itself. When Item 1A is
    missing (malformed 10-K), takes a byte-bounded tail after Item 1.

    The "last before 1A" choice is deliberate: 10-Ks place a
    table-of-contents entry with the same "Item 1. Business" text
    earlier in the document, followed by the actual narrative
    heading. Picking the last occurrence before Item 1A skips the
    TOC entry and lands on the real section header.
    """
    if not raw_html:
        return None
    text = _strip_html(raw_html)

    matches_1 = list(_ITEM_1_RE.finditer(text))
    if not matches_1:
        return None

    # Pick the LAST Item 1 occurrence as the anchor — TOC at the top
    # of a 10-K lists "Item 1. Business" once as a link target; the
    # real heading appears later.
    last_item_1 = matches_1[-1]
    start = last_item_1.end()

    # End boundary = FIRST Item 1A AFTER the chosen Item 1 anchor
    # (#550). Pre-#550 the parser picked the LAST Item 1A in the
    # whole document, which on filings whose Risk Factors body
    # references "Item 1A" again later (GME's 10-K mentions
    # "Item 1A" inside body prose) over-extended the slice into the
    # next 100 KB of risk-factors content. Anchoring to first-after-
    # Item-1 correctly skips both the TOC link (which is BEFORE the
    # last Item 1 heading) and any body references (which are
    # AFTER the next Item 1A heading).
    matches_1a = list(_ITEM_1A_RE.finditer(text))
    matches_1a_after = [m for m in matches_1a if m.start() > start]
    end = matches_1a_after[0].start() if matches_1a_after else len(text)

    body = text[start:end].strip()
    if not body:
        return None

    # Byte-cap on plain text. UTF-8 encoding is ASCII-dominated for
    # English 10-Ks so .encode() is cheap.
    encoded = body.encode("utf-8")
    if len(encoded) > MAX_BODY_BYTES:
        # Decode-safe truncation: step back to a valid codepoint if
        # the cap landed mid-byte-sequence.
        truncated = encoded[:MAX_BODY_BYTES]
        body = truncated.decode("utf-8", errors="ignore")

    return body


# ---------------------------------------------------------------------
# Subsection extraction (#449)
# ---------------------------------------------------------------------


# Canonical section-key mapping. Keys are lowercased heading phrases;
# values are the canonical ``section_key`` stored on
# ``instrument_business_summary_sections``. Order-insensitive.
#
# The mapping tolerates common wording variants — e.g. "Human Capital",
# "Human Capital Resources", "Our People", "Our People and Culture"
# all resolve to ``human_capital``. Headings that don't match any key
# fall through as ``section_key='other'`` with the verbatim label
# preserved, so nothing is silently dropped.
_CANONICAL_KEY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("general", re.compile(r"^(general|overview|our company|business overview|company overview)\b", re.IGNORECASE)),
    ("strategy", re.compile(r"^(our )?(business )?strategy\b|^our strategic priorities\b", re.IGNORECASE)),
    ("history", re.compile(r"^(our )?history\b|^background\b", re.IGNORECASE)),
    (
        "segments",
        re.compile(
            r"^(reportable |business |operating )?segments?\b|^segment (information|reporting)\b", re.IGNORECASE
        ),
    ),
    ("products", re.compile(r"^(our )?products\b(?! and services)|^principal products\b", re.IGNORECASE)),
    ("services", re.compile(r"^(our )?services\b(?! and products)", re.IGNORECASE)),
    ("products_and_services", re.compile(r"^products and services\b|^services and products\b", re.IGNORECASE)),
    (
        "customers",
        re.compile(r"^(our )?customers\b|^principal customers\b|^major customers\b|^clients\b", re.IGNORECASE),
    ),
    ("markets", re.compile(r"^(our )?markets\b|^geograph(?:ic|ical) (areas|markets|information)\b", re.IGNORECASE)),
    ("competition", re.compile(r"^competit(ion|ive (landscape|environment))\b", re.IGNORECASE)),
    ("seasonality", re.compile(r"^seasonal(ity|\s)\b", re.IGNORECASE)),
    ("backlog", re.compile(r"^backlog\b", re.IGNORECASE)),
    (
        "raw_materials",
        re.compile(r"^(raw materials|supply chain|sources (of|and) (supply|materials))\b", re.IGNORECASE),
    ),
    ("manufacturing", re.compile(r"^(manufacturing|production|operations)\b", re.IGNORECASE)),
    ("sales_marketing", re.compile(r"^(sales|marketing|sales and marketing|distribution)\b", re.IGNORECASE)),
    (
        "ip",
        re.compile(
            r"^(intellectual property|patents|trademarks?|proprietary rights?|patents?,?\s*trademarks?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "r_and_d",
        re.compile(r"^(research and development|r&\s*d|r and d|research & development)\b", re.IGNORECASE),
    ),
    (
        "regulatory",
        re.compile(
            r"^((government )?regulat(ion|ory)|regulatory (matters|environment|landscape))\b",
            re.IGNORECASE,
        ),
    ),
    ("environmental", re.compile(r"^environmental (matters|compliance|regulation)\b", re.IGNORECASE)),
    ("climate", re.compile(r"^(climate change|climate-related|sustainability|esg)\b", re.IGNORECASE)),
    (
        "human_capital",
        re.compile(
            r"^(human capital(\s+(resources|management))?|employees|our (people|employees|workforce|team)|"
            r"our people and culture)\b",
            re.IGNORECASE,
        ),
    ),
    ("properties", re.compile(r"^(properties|facilities|real estate)\b", re.IGNORECASE)),
    (
        "corporate_info",
        re.compile(r"^(corporate (information|history)|about (us|our company))\b", re.IGNORECASE),
    ),
    (
        "available_information",
        re.compile(r"^(available information|sec filings|where you can find more information)\b", re.IGNORECASE),
    ),
)


# Heading detection. SEC 10-Ks express subsection headings through
# structural HTML — ``<h1>``..``<h6>``, ``<b>``, ``<strong>``, or
# styled ``<p>`` / ``<div>`` blocks. Detecting headings from fully-
# stripped plain text is unreliable (title-case noun phrases like
# "The Company" match as easily as a real heading).
#
# Strategy: before stripping the body to plain text, wrap the inner
# text of every heading tag with a unit-separator sentinel
# (``␟``). The sentinel survives the subsequent HTML strip, so we
# can split the stripped body on sentinel boundaries and recover every
# text span that was originally inside a heading tag. We then test
# each candidate heading text against ``_looks_like_heading`` to
# filter out boilerplate (bold inline emphasis, TOC link text, etc.).
_HEADING_MAX_LEN = 80
_SENTENCE_ENDER_RE = re.compile(r"[.!?;](?!$)")
_MIN_WORD_COUNT = 1
_MAX_WORD_COUNT = 10

_HEADING_SENTINEL = "␟"

# Match <h1>..<h6>, <b>, <strong>, and their closing tags. Inner text
# gets wrapped with the sentinel before HTML strip. Non-greedy so a
# single unclosed tag doesn't swallow the whole document.
_HEADING_WRAP_RE = re.compile(
    r"<(?P<tag>h[1-6]|b|strong)(?:\s[^>]*)?>(?P<inner>.*?)</(?P=tag)>",
    re.IGNORECASE | re.DOTALL,
)

# Match any tag carrying a ``font-weight: bold`` (or ``700+`` /
# ``bolder``) inline style. Modern iXBRL filings (#550) express
# subsection headings via inline-styled <span>/<p>/<div> rather than
# explicit <h*>/<b>/<strong> tags. The previous heading detector
# (``_HEADING_WRAP_RE`` above) missed these entirely — GME's recent
# 10-K parsed to a single 102 KB ``general`` block as a result.
#
# We match the *opening* tag's style attribute, then capture the
# inner text up to the matching closing tag of the same name. Tag
# alternation is restricted to common heading-host tags so a stray
# bold-styled <a> link or <em> doesn't get promoted to a heading.
#
# Intentionally narrow on the style pattern: ``font-weight: bold``,
# ``font-weight:bold``, ``font-weight:700|800|900``, ``font-weight:bolder``.
# Filings use a mix of these. Numeric weights below 700 are NOT
# treated as bold (medium / semibold are common body styling).
#
# Style-attribute matching avoids constraining on quote characters
# inside the value: SEC filings routinely embed both quote types in
# the same style attribute (e.g. ``style="font-family:'Arial'"``),
# and a strict ``[^"']`` exclusion stops at the first inner
# apostrophe and misses the heading entirely. We match liberally up
# to ``>`` (style attrs never contain ``>``) and rely on the
# ``font-weight`` substring for the heading test.
# Adjacent bold-bold ``<span style="...bold...">X</span><span style="...bold...">Y</span>``
# pair — collapsed before heading-tag wrapping (#550). Targets the
# drop-cap pattern where iXBRL filings break a logical heading across
# two sibling bold spans. Scoped to bold-on-BOTH-sides — body prose
# transitions from a non-bold span into a bold inline span (e.g.
# ``text</span><span style="font-weight:bold">term</span>``) are
# preserved so the trailing word doesn't collide with the leading
# text into a run-together word.
#
# Codex review on #550 round 2 — the previous regex constrained only
# the trailing span; this version captures leading + trailing as a
# pair and replaces via callback so body prose is never touched.
_BOLD_SPAN_PAIR_RE = re.compile(
    r"(?P<lead_open><span\b[^>]*?style\s*=\s*[\"'][^>]*?"
    r"font-weight\s*:\s*(?:bold|bolder|[7-9]\d\d)[^>]*?>)"
    r"(?P<lead_inner>[^<]*)"
    r"</span>\s*"
    r"(?P<trail_open><span\b[^>]*?style\s*=\s*[\"'][^>]*?"
    r"font-weight\s*:\s*(?:bold|bolder|[7-9]\d\d)[^>]*?>)",
    re.IGNORECASE,
)


def _merge_bold_span_pair(m: re.Match[str]) -> str:
    """Collapse a bold-bold drop-cap pair into the leading span only.

    Drops the ``</span>`` close + whitespace + opening of the trailing
    bold span, leaving ``<span style=bold>X`` + ``Y`` text continuous
    inside the leading span (the original trailing span's closer is
    still in the surrounding HTML and pairs with the now-unified
    open). Empty-string concat (no separator) so "ITEM 1. B" + "USINESS"
    becomes "ITEM 1. BUSINESS", not "ITEM 1. B USINESS".
    """
    return m.group("lead_open") + m.group("lead_inner")


_BOLD_STYLE_WRAP_RE = re.compile(
    r"<(?P<tag>span|p|div|font|i|em)\b"
    r"(?P<attrs>[^>]*?style\s*=\s*[\"'][^>]*?"
    r"font-weight\s*:\s*(?:bold|bolder|[7-9]\d\d)"
    r"[^>]*?)>"
    r"(?P<inner>.*?)"
    r"</(?P=tag)>",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class ParsedCrossReference:
    """One cross-reference pointer extracted from a section body."""

    reference_type: str  # "item" / "exhibit" / "note" / "filing" / "part"
    target: str  # canonical short label, e.g. "Item 1A", "Exhibit 21.1"
    context: str  # sentence-sized phrase around the reference


# ---------------------------------------------------------------------
# Table extraction — sentinel substitution (#559)
# ---------------------------------------------------------------------

_TABLE_SENTINEL = "␞"  # SYMBOL FOR RECORD SEPARATOR — never appears in 10-K prose
# Used as scan anchors by _scan_outer_tables; the actual table extent is
# determined by the depth-aware walker, not by these patterns directly.
_TABLE_OPEN_RE = re.compile(r"<table\b[^>]*>", re.IGNORECASE)
_TABLE_CLOSE_RE = re.compile(r"</table\s*>", re.IGNORECASE)
# DOTALL: cell / row contents span multiple lines in iXBRL filings.
_TR_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr\s*>", re.IGNORECASE | re.DOTALL)
_CELL_RE = re.compile(r"<(?:t[hd])\b[^>]*>(.*?)</t[hd]\s*>", re.IGNORECASE | re.DOTALL)

_MAX_TABLE_ROWS = 200  # truncate beyond — pathological 10-Ks rarely list >200 rows
_MAX_CELL_LEN = 200  # truncate cell content beyond — single cells should be short


def _scan_outer_tables(raw_html: str) -> list[tuple[int, int]]:
    """Return (start, end) offsets for every OUTERMOST <table>...</table>
    block in ``raw_html``. Nested tables are ignored — only their outer
    wrapper appears in the result list. ``end`` is exclusive (one past
    the last char of </table>).

    Walks the HTML character by character using regex anchors (cheap —
    10-K bodies are typically <2 MB). Increments depth on each <table
    opening, decrements on each </table closing; only emits a span when
    depth returns to 0.
    """
    spans: list[tuple[int, int]] = []
    pos = 0
    depth = 0
    span_start = -1
    while pos < len(raw_html):
        open_match = _TABLE_OPEN_RE.search(raw_html, pos)
        close_match = _TABLE_CLOSE_RE.search(raw_html, pos)
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
                # Stray closing tag — reset and continue defensively.
                depth = 0
                span_start = -1
            pos = close_match.end()
    return spans


@dataclass(frozen=True)
class ParsedTable:
    """One <table> block extracted from a section body.

    ``headers`` is the first row's cell contents (treated as headers
    even when the source uses <td> rather than <th> — many 10-K
    issuers do).  ``rows`` are subsequent rows. Cells are plain text
    after entity decode + tag strip.
    """

    order: int
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class _RawTable:
    """Intermediate carrier from _parse_table_html — caller assigns
    the final ``order`` once it knows the global position."""

    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


def _parse_table_html(table_html: str) -> _RawTable | None:
    """Extract a single OUTER <table>...</table> block. Nested inner
    tables are blanked before row scan so their cells don't bleed into
    the outer table's row list. Applies caps: at most _MAX_TABLE_ROWS
    rows and _MAX_CELL_LEN chars per cell."""
    # Strip the outer <table> wrapper so _scan_outer_tables on the
    # inner content finds nested tables only, not the outer one.
    inner_open = _TABLE_OPEN_RE.search(table_html)
    inner_close_idx = table_html.rfind("</table")
    if inner_open is None or inner_close_idx == -1:
        return None
    inner = table_html[inner_open.end() : inner_close_idx]
    nested = _scan_outer_tables(inner)
    if nested:
        # Replace each nested table block with a single space so the
        # outer row scanner doesn't pick up its cells.
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
        cells = tuple(_strip_html(cell).strip() for cell in _CELL_RE.findall(tr_match.group(1)))
        if any(c for c in cells):
            cells_per_row.append(cells)
    if not cells_per_row:
        return None
    headers, *body_rows = cells_per_row
    headers = tuple(c[:_MAX_CELL_LEN] for c in headers)
    body_rows = tuple(tuple(c[:_MAX_CELL_LEN] for c in row) for row in body_rows[:_MAX_TABLE_ROWS])
    return _RawTable(headers=headers, rows=body_rows)


def _extract_tables(raw_html: str) -> tuple[str, tuple[ParsedTable, ...]]:
    """Replace every OUTERMOST <table> block in ``raw_html`` with a
    sentinel ``␞TABLE_N␞`` and return the rewritten HTML + the
    parsed tables in source order."""
    spans = _scan_outer_tables(raw_html)
    if not spans:
        return raw_html, ()
    tables: list[ParsedTable] = []
    pieces: list[str] = []
    cursor = 0
    for start, end in spans:
        pieces.append(raw_html[cursor:start])
        outer_html = raw_html[start:end]
        parsed = _parse_table_html(outer_html)
        if parsed is None:
            pieces.append(" ")  # drop layout-only tables
        else:
            order = len(tables)
            tables.append(ParsedTable(order=order, headers=parsed.headers, rows=parsed.rows))
            pieces.append(f" {_TABLE_SENTINEL}TABLE_{order}{_TABLE_SENTINEL} ")
        cursor = end
    pieces.append(raw_html[cursor:])
    return "".join(pieces), tuple(tables)


@dataclass(frozen=True)
class ParsedBusinessSection:
    """One subsection extracted from Item 1."""

    section_order: int
    section_key: str  # canonical key from _CANONICAL_KEY_PATTERNS or "other"
    section_label: str  # heading as it appeared in the filing, verbatim
    body: str
    cross_references: tuple[ParsedCrossReference, ...]
    tables: tuple[ParsedTable, ...] = ()


# Cross-reference regex. Matches the common forms "Item 1A", "Item 7",
# "Exhibit 21", "Note 15", "Part II". Captures the reference_type and
# short target; the context is the surrounding ~120-char window.
_REF_ITEM_RE = re.compile(r"\bItem\s+(\d{1,2}[A-Za-z]?)\b")
_REF_EXHIBIT_RE = re.compile(r"\bExhibit\s+(\d{1,2}(?:\.\d{1,2})?)\b")
_REF_NOTE_RE = re.compile(r"\bNote\s+(\d{1,2}[A-Za-z]?)\b")
_REF_PART_RE = re.compile(r"\bPart\s+(I{1,3}V?|IV|[1-4])\b")


def _classify_section_heading(heading: str) -> str:
    """Map a heading phrase to a canonical ``section_key``.

    Returns ``"other"`` when no pattern matches — callers preserve
    the verbatim heading in ``section_label`` so nothing is lost.
    """
    h = heading.strip()
    for key, pattern in _CANONICAL_KEY_PATTERNS:
        if pattern.search(h):
            return key
    return "other"


def _looks_like_heading(line: str) -> bool:
    """True if ``line`` (originally inside an HTML heading tag)
    plausibly stands alone as a subsection heading.

    The sentinel wrapper already proves the text was emphasised in
    the source HTML — our job here is to reject obvious non-headings:

    - Too long (> 80 chars, so inline bold emphasis spanning a full
      sentence is filtered out).
    - Contains internal sentence punctuation (the DOM sometimes bolds
      an opening clause of a paragraph; those aren't headings).
    - Too many words for a typical heading.
    - All digits / single character / empty.

    What we do *not* check: casing. Inside a heading tag, any
    non-empty short phrase is a candidate regardless of Title-Case /
    ALL-CAPS / sentence-case shape.
    """
    stripped = line.strip().rstrip(":.")
    if not stripped or len(stripped) > _HEADING_MAX_LEN:
        return False
    words = stripped.split()
    if len(words) < _MIN_WORD_COUNT or len(words) > _MAX_WORD_COUNT:
        return False
    if _SENTENCE_ENDER_RE.search(stripped):
        return False
    # Must contain at least one alphabetic character — reject
    # "3.", "(a)", etc.
    return any(c.isalpha() for c in stripped)


def _extract_cross_references(body: str) -> tuple[ParsedCrossReference, ...]:
    """Harvest Item / Exhibit / Note / Part references from a section
    body. Each hit carries a ~120-char context window for audit."""
    refs: list[ParsedCrossReference] = []
    for ref_type, pattern, label_fmt in (
        ("item", _REF_ITEM_RE, "Item {}"),
        ("exhibit", _REF_EXHIBIT_RE, "Exhibit {}"),
        ("note", _REF_NOTE_RE, "Note {}"),
        ("part", _REF_PART_RE, "Part {}"),
    ):
        for m in pattern.finditer(body):
            target = label_fmt.format(m.group(1))
            start = max(0, m.start() - 60)
            end = min(len(body), m.end() + 60)
            context = body[start:end].strip()
            refs.append(
                ParsedCrossReference(
                    reference_type=ref_type,
                    target=target,
                    context=context,
                )
            )
    return tuple(refs)


def _wrap_heading_tags(raw_html: str) -> str:
    """Wrap the inner text of every heading-candidate tag in
    ``<␟…␟>`` sentinels so the subsequent HTML strip preserves a
    machine-detectable boundary at each original heading position.

    Two passes:

    1. Explicit heading tags (``<h1>``..``<h6>``, ``<b>``, ``<strong>``).
    2. Bold-styled inline tags (``<span style="font-weight:bold">``,
       etc.) — modern iXBRL filings express subsection headings this
       way (#550). Without this pass GME's 10-K parsed to a single
       102 KB ``general`` block.

    Order matters: explicit tags first so a ``<b>`` already wrapped
    in pass 1 is unaffected by pass 2's broader style match.
    """

    def _wrap(m: re.Match[str]) -> str:
        inner = m.group("inner")
        return f" {_HEADING_SENTINEL}{inner}{_HEADING_SENTINEL} "

    # Pre-step: collapse adjacent ``</span><span ...>`` boundaries
    # so a heading split across sibling spans with a drop-cap (e.g.
    # MSFT's "ITEM 1. B" + "USINESS" pattern, common in modern
    # iXBRL filings) becomes a single span before wrapping. The
    # collapsed boundary loses styling continuity within paragraphs
    # but the tradeoff is acceptable for sectioning — body content
    # passes through ``_strip_html`` later anyway.
    # Drop-cap collapse — bold-bold span pairs only (#550). The
    # callback replaces only the close-then-open boundary, never
    # touching body-prose spans where the leading side isn't bold.
    # See ``_merge_bold_span_pair`` doc.
    collapsed = _BOLD_SPAN_PAIR_RE.sub(_merge_bold_span_pair, raw_html)
    pass1 = _HEADING_WRAP_RE.sub(_wrap, collapsed)
    return _BOLD_STYLE_WRAP_RE.sub(_wrap, pass1)


def _attach_tables(
    sections: list[ParsedBusinessSection],
    all_tables: tuple[ParsedTable, ...],
) -> list[ParsedBusinessSection]:
    """Walk each section body, find ``␞TABLE_N␞`` markers,
    and attach the matching ParsedTable. Re-numbers tables per
    section so the renderer can index by ``section.tables[order]``."""
    result: list[ParsedBusinessSection] = []
    for s in sections:
        attached: list[ParsedTable] = []
        body = s.body
        for table in all_tables:
            marker = f"{_TABLE_SENTINEL}TABLE_{table.order}{_TABLE_SENTINEL}"
            if marker in body:
                local_order = len(attached)
                attached.append(
                    ParsedTable(
                        order=local_order,
                        headers=table.headers,
                        rows=table.rows,
                    )
                )
                body = body.replace(
                    marker,
                    f"{_TABLE_SENTINEL}TABLE_{local_order}{_TABLE_SENTINEL}",
                )
        result.append(
            ParsedBusinessSection(
                section_order=s.section_order,
                section_key=s.section_key,
                section_label=s.section_label,
                body=body,
                cross_references=s.cross_references,
                tables=tuple(attached),
            )
        )
    return result


def extract_business_sections(raw_html: str) -> tuple[ParsedBusinessSection, ...]:
    """Extract Item 1 as an ordered list of subsections.

    Returns an empty tuple when Item 1 can't be located. Otherwise
    returns every detectable subsection in source order. The first
    element (section_order=0) is the general / overview text that
    precedes the first subsection heading, labelled ``"General"``
    when no explicit heading is present.

    Unmapped headings get ``section_key='other'`` with their
    verbatim text preserved in ``section_label`` — no silent drops.

    Heading detection uses HTML structure (``<h1>``..``<h6>``,
    ``<b>``, ``<strong>``) rather than plain-text shape inference,
    so title-case noun phrases in narrative prose aren't confused
    for real subsection headings.
    """
    if not raw_html:
        return ()
    # Extract <table> blocks first, replacing each with a ␞TABLE_N␞
    # sentinel so the table content survives the subsequent HTML strip
    # as structured data rather than prose noise (#559).
    table_stripped_html, all_tables = _extract_tables(raw_html)
    # Pre-strip: wrap heading-tag inner text with sentinels so the
    # boundaries survive the subsequent plain-text collapse.
    marked_html = _wrap_heading_tags(table_stripped_html)
    text = _strip_html(marked_html)

    matches_1 = list(_ITEM_1_RE.finditer(text))
    if not matches_1:
        return ()
    # Anchor on the LAST Item 1 occurrence (skips TOC link). End on
    # the FIRST Item 1A AFTER that anchor (#550) — picking the LAST
    # Item 1A would over-extend through any later body references
    # to "Item 1A" and pull risk-factor content into the section
    # set. This mirrors the boundary fix in ``extract_business_section``.
    last_item_1 = matches_1[-1]
    start = last_item_1.end()
    matches_1a = list(_ITEM_1A_RE.finditer(text))
    matches_1a_after = [m for m in matches_1a if m.start() > start]
    end = matches_1a_after[0].start() if matches_1a_after else len(text)

    # If the Item 1 heading was itself wrapped in sentinels, the first
    # sentinel immediately after ``start`` is the closing of that
    # heading — skip past it so the body doesn't begin with an orphan
    # closing marker (which would mis-pair subsequent headings).
    if start < len(text) and text[start : start + 3].lstrip().startswith(_HEADING_SENTINEL):
        s_idx = text.find(_HEADING_SENTINEL, start)
        if s_idx != -1 and s_idx < end:
            start = s_idx + 1
    # Same on the end side: if the Item 1A heading was wrapped, back
    # off before its opening sentinel so we don't leave an orphan
    # opening marker dangling at the tail of the body.
    if end > 0:
        back_idx = text.rfind(_HEADING_SENTINEL, start, end)
        if back_idx != -1 and back_idx > start:
            # Only trim when the trailing region between the sentinel
            # and ``end`` is entirely whitespace — otherwise we'd cut
            # off real body content.
            if text[back_idx + 1 : end].strip() == "":
                end = back_idx

    item_1_body = text[start:end].strip()
    if not item_1_body:
        return ()

    # Each sentinel pair wraps one heading-tag's content. Walk the
    # body and collect (start, end, heading_text) triples, keeping
    # only those whose text passes _looks_like_heading — inline bold
    # emphasis inside a paragraph usually isn't heading-shaped
    # (contains sentence punctuation, is too long, etc.).
    headings: list[tuple[int, int, str]] = []
    cursor = 0
    while cursor < len(item_1_body):
        open_idx = item_1_body.find(_HEADING_SENTINEL, cursor)
        if open_idx == -1:
            break
        close_idx = item_1_body.find(_HEADING_SENTINEL, open_idx + 1)
        if close_idx == -1:
            break
        heading_text = item_1_body[open_idx + 1 : close_idx].strip()
        if _looks_like_heading(heading_text):
            headings.append((open_idx, close_idx + 1, heading_text))
        cursor = close_idx + 1

    # Build the section list. Strip sentinels from each body slice so
    # the stored ``body`` is clean narrative. Body cap (#550) mirrors
    # the blob's ``MAX_BODY_BYTES`` so a single section can't blow out
    # the panel UI when heading detection partially fails.
    def _clean(s: str) -> str:
        cleaned = s.replace(_HEADING_SENTINEL, " ").strip()
        if len(cleaned.encode("utf-8")) > MAX_BODY_BYTES:
            cleaned = cleaned.encode("utf-8")[:MAX_BODY_BYTES].decode("utf-8", errors="ignore")
        return cleaned

    sections: list[ParsedBusinessSection] = []
    if not headings:
        # No subsections detected — emit a single "general" block.
        body_clean = _clean(item_1_body)
        if body_clean:
            sections.append(
                ParsedBusinessSection(
                    section_order=0,
                    section_key="general",
                    section_label="General",
                    body=body_clean,
                    cross_references=_extract_cross_references(body_clean),
                )
            )
        if all_tables:
            sections = _attach_tables(sections, all_tables)
        return tuple(sections)

    # Pre-heading general block.
    first_heading_start = headings[0][0]
    if first_heading_start > 0:
        pre_body = _clean(item_1_body[:first_heading_start])
        if pre_body:
            sections.append(
                ParsedBusinessSection(
                    section_order=0,
                    section_key="general",
                    section_label="General",
                    body=pre_body,
                    cross_references=_extract_cross_references(pre_body),
                )
            )

    for idx, (_h_start, h_end, heading_text) in enumerate(headings):
        next_start = headings[idx + 1][0] if idx + 1 < len(headings) else len(item_1_body)
        body_slice = _clean(item_1_body[h_end:next_start])
        if not body_slice:
            continue
        section_key = _classify_section_heading(heading_text)
        sections.append(
            ParsedBusinessSection(
                section_order=len(sections),
                section_key=section_key,
                section_label=heading_text,
                body=body_slice,
                cross_references=_extract_cross_references(body_slice),
            )
        )

    if all_tables:
        sections = _attach_tables(sections, all_tables)
    return tuple(sections)


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


def upsert_business_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    body: str,
    source_accession: str,
) -> bool:
    """Insert or update one ``instrument_business_summary`` row.

    Returns ``True`` on INSERT, ``False`` on UPDATE. The UPDATE path
    overwrites the body + source_accession + timestamps so a later
    10-K supersedes an older one cleanly. Resets the failure-tracking
    columns (#533) so a previously-quarantined instrument that now
    parses successfully exits quarantine cleanly."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_business_summary
                (instrument_id, body, source_accession)
            VALUES (%s, %s, %s)
            ON CONFLICT (instrument_id) DO UPDATE SET
                body                = EXCLUDED.body,
                source_accession    = EXCLUDED.source_accession,
                fetched_at          = NOW(),
                last_parsed_at      = NOW(),
                attempt_count       = 0,
                last_failure_reason = NULL,
                next_retry_at       = NULL
            RETURNING (xmax = 0) AS inserted
            """,
            (instrument_id, body, source_accession),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False


def upsert_business_sections(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source_accession: str,
    sections: tuple[ParsedBusinessSection, ...],
) -> int:
    """Replace the sections snapshot for ``(instrument, accession)``
    with the parsed list.

    The sections table is keyed by ``(instrument, accession, order)``,
    so a re-parse of the same accession under a better heading detector
    must not leak stale rows. Clear the prior snapshot for the same
    accession, then re-insert — atomically, inside a savepoint so an
    INSERT failure mid-loop rolls back the DELETE too (without this
    guard, a caller that commits on its own error path would wipe
    prior sections permanently while the blob survived — Claude
    review PR #460 BLOCKING).

    Tombstone path (empty sections) writes nothing.
    """
    if not sections:
        return 0
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM instrument_business_summary_sections
                WHERE instrument_id = %s AND source_accession = %s
                """,
                (instrument_id, source_accession),
            )
            inserted = 0
            for section in sections:
                cross_refs_json = Jsonb(
                    [
                        {
                            "reference_type": ref.reference_type,
                            "target": ref.target,
                            "context": ref.context,
                        }
                        for ref in section.cross_references
                    ]
                )
                tables_json = Jsonb(
                    [
                        {
                            "order": t.order,
                            "headers": list(t.headers),
                            "rows": [list(r) for r in t.rows],
                        }
                        for t in section.tables
                    ]
                )
                cur.execute(
                    """
                    INSERT INTO instrument_business_summary_sections
                        (instrument_id, source_accession, section_order,
                         section_key, section_label, body, cross_references,
                         tables_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        instrument_id,
                        source_accession,
                        section.section_order,
                        section.section_key,
                        section.section_label,
                        section.body,
                        cross_refs_json,
                        tables_json,
                    ),
                )
                inserted += 1
            return inserted


def record_parse_attempt(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source_accession: str,
    reason: FailureReason,
) -> None:
    """Stamp a parse attempt without ever overwriting a real body.

    INSERT path (first-time failure): writes a tombstone row with
    ``body = ''`` and ``attempt_count = 1``, plus the chosen
    ``next_retry_at`` from the backoff schedule. The next ingester
    pass sees a row with ``next_retry_at`` in the future and skips
    it.

    UPDATE path (prior row exists, failed retry): increments
    ``attempt_count`` and recomputes ``next_retry_at`` from the
    schedule. Preserves any real ``body`` from an earlier successful
    parse so a transient error on a later 10-K can never destroy
    the extracted narrative (Codex #434 / #446 BLOCKING pattern
    applied to #428). Records ``last_failure_reason`` so the
    operator-facing admin dashboard can surface the failure category.

    Backoff schedule (in days): 1 → 1, 2 → 7, 3 → 30, 4+ → 365
    (effective quarantine).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_business_summary
                (instrument_id, body, source_accession,
                 attempt_count, last_failure_reason, next_retry_at)
            VALUES (
                %(iid)s, '', %(acc)s,
                1, %(reason)s,
                NOW() + (%(days_first)s || ' days')::INTERVAL
            )
            ON CONFLICT (instrument_id) DO UPDATE SET
                source_accession    = EXCLUDED.source_accession,
                last_parsed_at      = NOW(),
                attempt_count       = instrument_business_summary.attempt_count + 1,
                last_failure_reason = EXCLUDED.last_failure_reason,
                next_retry_at       = NOW() + (
                    CASE
                        WHEN instrument_business_summary.attempt_count + 1 = 1 THEN '1 days'::INTERVAL
                        WHEN instrument_business_summary.attempt_count + 1 = 2 THEN '7 days'::INTERVAL
                        WHEN instrument_business_summary.attempt_count + 1 = 3 THEN '30 days'::INTERVAL
                        ELSE '365 days'::INTERVAL
                    END
                )
            """,
            {
                "iid": instrument_id,
                "acc": source_accession,
                "reason": reason,
                "days_first": _next_retry_days(1),
            },
        )


# ---------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------


def get_business_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> str | None:
    """Return the stored Item 1 body, or None when no body exists.

    A row with ``body = ''`` is a tombstone (the ingester tried and
    failed to extract) — treated as "no body available" by callers
    so the SEC-profile endpoint still falls through to the yfinance
    description fallback. Read-only."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT body FROM instrument_business_summary WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    body = str(row[0])
    return body if body else None


@dataclass(frozen=True)
class BusinessSectionRow:
    """One section as surfaced to the UI: parsed shape + audit fields."""

    section_order: int
    section_key: str
    section_label: str
    body: str
    cross_references: tuple[ParsedCrossReference, ...]
    source_accession: str
    tables: tuple[ParsedTable, ...] = ()


def get_business_sections(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> tuple[BusinessSectionRow, ...]:
    """Return Item 1 subsections for an instrument in source order.

    Empty tuple when no sections are stored. Filters to the most
    recent accession — later 10-Ks supersede via the ingester, so
    this keeps the UI on the freshest filing.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT section_order, section_key, section_label, body,
                   cross_references, source_accession, tables_json
            FROM instrument_business_summary_sections
            WHERE instrument_id = %s
              AND source_accession = (
                  SELECT source_accession
                  FROM instrument_business_summary_sections
                  WHERE instrument_id = %s
                  ORDER BY fetched_at DESC
                  LIMIT 1
              )
            ORDER BY section_order ASC
            """,
            (instrument_id, instrument_id),
        )
        raw_rows = cur.fetchall()
    rows: list[BusinessSectionRow] = []
    for r in raw_rows:
        refs_raw = r[4] or []
        refs_list: list[Any] = refs_raw if isinstance(refs_raw, list) else []
        refs = tuple(
            ParsedCrossReference(
                reference_type=str(ref.get("reference_type", "")),
                target=str(ref.get("target", "")),
                context=str(ref.get("context", "")),
            )
            for ref in refs_list
            if isinstance(ref, dict)
        )
        tables_raw = r[6] or []
        tables_list: list[Any] = tables_raw if isinstance(tables_raw, list) else []
        tables = tuple(
            ParsedTable(
                order=int(tbl.get("order", 0)),
                headers=tuple(str(h) for h in tbl.get("headers", [])),
                rows=tuple(tuple(str(c) for c in row) for row in tbl.get("rows", []) if isinstance(row, list)),
            )
            for tbl in tables_list
            if isinstance(tbl, dict)
        )
        rows.append(
            BusinessSectionRow(
                section_order=int(r[0]),
                section_key=str(r[1]),
                section_label=str(r[2]),
                body=str(r[3]),
                cross_references=refs,
                source_accession=str(r[5]),
                tables=tables,
            )
        )
    return tuple(rows)


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class _DocFetcher(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one ``ingest_business_summaries`` call."""

    filings_scanned: int
    rows_inserted: int
    rows_updated: int
    fetch_errors: int
    parse_misses: int


# Soft minimum body length below which the extractor is treated as a
# parse miss. Tuned to exclude TOC-only fragments ("Item 1. Business
# ... 3") while keeping short-but-real business descriptions.
_MIN_BODY_LEN = 120


def _find_prior_plain_10k(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    before_accession: str,
) -> tuple[str, str] | None:
    """Find the most recent plain ``10-K`` (NOT ``10-K/A``) filing for
    ``instrument_id`` strictly older than the filing keyed by
    ``before_accession``.

    Returns ``(provider_filing_id, primary_document_url)`` or ``None``
    when no prior plain 10-K exists.

    Used by the 10-K/A fallback path (#534): when the latest filing is
    an amendment that omits Item 1 (Part-III amendments do this
    routinely), the ingester re-attempts parsing against the original
    10-K so the operator still gets the authoritative business
    narrative. Without this fallback, every Part-III amendment
    instrument permanently lost its Item 1 view.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.provider_filing_id,
                   fe.primary_document_url
              FROM filing_events fe
             WHERE fe.provider = 'sec'
               AND fe.filing_type = '10-K'
               AND fe.instrument_id = %(iid)s
               AND fe.primary_document_url IS NOT NULL
               AND fe.filing_date < (
                    SELECT filing_date FROM filing_events
                     WHERE provider = 'sec' AND provider_filing_id = %(acc)s
                     LIMIT 1
                   )
             ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
             LIMIT 1
            """,
            {"iid": instrument_id, "acc": before_accession},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0]), str(row[1])


def bootstrap_business_summaries(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    chunk_limit: int = 500,
    max_runtime_seconds: int = 3600,
) -> IngestResult:
    """One-shot drain of the entire business_summary candidate set.

    Calls :func:`ingest_business_summaries` repeatedly with a chunked
    limit until either the candidate query returns zero rows or the
    runtime deadline elapses. Idempotent — safe to re-run; subsequent
    invocations no-op fast once the queue is empty.

    Designed for first-time backfill of the 4031-instrument SEC-CIK
    universe (#535). Steady-state daily ingest (limit=200) is too
    slow at first-time bootstrap; this drain processes the entire
    backlog in one bounded session under SEC fair-use limits.

    Returns aggregate :class:`IngestResult` summing every chunk's
    counts. Quarantined rows (next_retry_at > NOW()) stay excluded
    via the standard candidate query — bootstrap doesn't override
    backoff.
    """
    import time

    deadline = time.monotonic() + max_runtime_seconds
    total_scanned = 0
    total_inserted = 0
    total_updated = 0
    total_fetch_errors = 0
    total_parse_misses = 0

    while time.monotonic() < deadline:
        chunk = ingest_business_summaries(conn, fetcher, limit=chunk_limit)
        total_scanned += chunk.filings_scanned
        total_inserted += chunk.rows_inserted
        total_updated += chunk.rows_updated
        total_fetch_errors += chunk.fetch_errors
        total_parse_misses += chunk.parse_misses
        if chunk.filings_scanned == 0:
            break

    logger.info(
        "bootstrap_business_summaries complete: scanned=%d inserted=%d updated=%d fetch_errors=%d parse_misses=%d",
        total_scanned,
        total_inserted,
        total_updated,
        total_fetch_errors,
        total_parse_misses,
    )

    return IngestResult(
        filings_scanned=total_scanned,
        rows_inserted=total_inserted,
        rows_updated=total_updated,
        fetch_errors=total_fetch_errors,
        parse_misses=total_parse_misses,
    )


def _classify_fetch_exception(exc: Exception) -> FailureReason:
    """Map a fetch-side exception to a FailureReason value.

    Concrete HTTP errors with a status code on a ``response`` attribute
    split into 5xx vs 4xx. Connection / read timeouts share one category
    so the operator dashboard can group them cleanly. Anything else
    falls through to ``fetch_other``.
    """
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if isinstance(status_code, int):
            if status_code >= 500:
                return "fetch_http_5xx"
            if status_code >= 400:
                return "fetch_http_4xx"
    name = type(exc).__name__.lower()
    if "timeout" in name or "connection" in name:
        return "fetch_timeout"
    return "fetch_other"


def ingest_business_summaries(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    limit: int = 200,
) -> IngestResult:
    """Scan 10-K filings, fetch primary doc, extract Item 1, upsert.

    Candidate selector (shape addresses Codex #428 findings, extended
    in #533 with backoff/quarantine):

    1. ``fe.filing_type IN ('10-K', '10-K/A')`` — amendments retain
       their ``/A`` suffix through the SEC pipeline (see
       ``app/services/fundamentals.py``); narrowing to plain ``'10-K'``
       misses restated annual reports and pins ``source_accession`` to
       a stale pre-amendment filing.
    2. ``fe.primary_document_url IS NOT NULL`` — unparseable without.
    3. No ``instrument_business_summary`` row, OR the stored
       ``source_accession`` differs from this filing's accession
       (later 10-K supersedes), OR the existing row's ``next_retry_at``
       has elapsed.
    4. Newest filing wins per instrument (``DISTINCT ON`` resolves to
       the latest filing_date, tie-break on filing_event_id); the
       outer query then sorts GLOBALLY newest-first so a backlog
       doesn't delay fresh filings for higher instrument_ids
       indefinitely.

    Bounded per run (``limit=200``). 10-Ks are quarterly so the
    steady-state backlog is small; a large limit protects against a
    catch-up after a scheduler outage without starving other SEC
    calls on the same rate-limit pool.
    """
    conn.commit()

    candidates: list[tuple[int, str, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest_per_instrument AS (
                SELECT DISTINCT ON (fe.instrument_id)
                       fe.instrument_id,
                       fe.provider_filing_id,
                       fe.primary_document_url,
                       fe.filing_type,
                       fe.filing_date,
                       fe.filing_event_id
                FROM filing_events fe
                WHERE fe.provider = 'sec'
                  AND fe.filing_type IN ('10-K', '10-K/A')
                  AND fe.primary_document_url IS NOT NULL
                ORDER BY fe.instrument_id, fe.filing_date DESC, fe.filing_event_id DESC
            )
            SELECT lpi.instrument_id,
                   lpi.provider_filing_id,
                   lpi.primary_document_url,
                   lpi.filing_type
            FROM latest_per_instrument lpi
            LEFT JOIN instrument_business_summary bs
                   ON bs.instrument_id = lpi.instrument_id
            WHERE bs.instrument_id IS NULL
               OR bs.source_accession <> lpi.provider_filing_id
               OR (bs.next_retry_at IS NOT NULL AND bs.next_retry_at <= NOW())
            ORDER BY lpi.filing_date DESC, lpi.filing_event_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1]), str(row[2]), str(row[3])))
    conn.commit()

    inserted = 0
    updated = 0
    fetch_errors = 0
    parse_misses = 0

    for instrument_id, accession, url, filing_type in candidates:
        try:
            html = fetcher.fetch_document_text(url)
        except Exception as exc:
            reason = _classify_fetch_exception(exc)
            logger.warning(
                "ingest_business_summaries: fetch failed accession=%s url=%s reason=%s",
                accession,
                url,
                reason,
                exc_info=True,
            )
            fetch_errors += 1
            record_parse_attempt(
                conn,
                instrument_id=instrument_id,
                source_accession=accession,
                reason=reason,
            )
            conn.commit()
            continue
        if html is None:
            # Provider returned None on 404/410 (filing withdrawn) or
            # other "no body" path. Classify as fetch_other so the
            # operator dashboard separates it from real HTTP errors.
            fetch_errors += 1
            record_parse_attempt(
                conn,
                instrument_id=instrument_id,
                source_accession=accession,
                reason="fetch_other",
            )
            conn.commit()
            continue

        try:
            body = extract_business_section(html)
        except Exception:
            logger.warning(
                "ingest_business_summaries: parse exception accession=%s",
                accession,
                exc_info=True,
            )
            parse_misses += 1
            record_parse_attempt(
                conn,
                instrument_id=instrument_id,
                source_accession=accession,
                reason="parse_exception",
            )
            conn.commit()
            continue
        if body is None:
            # 10-K/A fallback (#534): Part-III amendments routinely
            # omit Item 1. Before tombstoning, retry against the most
            # recent prior plain 10-K from the same instrument. If
            # the fallback succeeds, persist with the fallback's
            # accession so the next run sees a real body and the
            # ``source_accession <> latest`` predicate alone keeps
            # the row out of the candidate set until a fresh 10-K
            # arrives.
            fallback_used = False
            if filing_type == "10-K/A":
                prior = _find_prior_plain_10k(
                    conn,
                    instrument_id=instrument_id,
                    before_accession=accession,
                )
                if prior is not None:
                    fallback_acc, fallback_url = prior
                    logger.info(
                        "ingest_business_summaries: 10-K/A fallback accession=%s -> prior plain 10-K accession=%s",
                        accession,
                        fallback_acc,
                    )
                    fallback_html: str | None = None
                    try:
                        fallback_html = fetcher.fetch_document_text(fallback_url)
                    except Exception:
                        logger.warning(
                            "ingest_business_summaries: 10-K/A fallback fetch failed accession=%s url=%s",
                            fallback_acc,
                            fallback_url,
                            exc_info=True,
                        )
                    if fallback_html is not None:
                        try:
                            fallback_body = extract_business_section(fallback_html)
                        except Exception:
                            logger.warning(
                                "ingest_business_summaries: 10-K/A fallback parse exception accession=%s",
                                fallback_acc,
                                exc_info=True,
                            )
                            fallback_body = None
                        if fallback_body is not None and len(fallback_body) >= _MIN_BODY_LEN:
                            try:
                                did_insert = upsert_business_summary(
                                    conn,
                                    instrument_id=instrument_id,
                                    body=fallback_body,
                                    source_accession=fallback_acc,
                                )
                                fallback_sections = extract_business_sections(fallback_html)
                                if fallback_sections:
                                    try:
                                        upsert_business_sections(
                                            conn,
                                            instrument_id=instrument_id,
                                            source_accession=fallback_acc,
                                            sections=fallback_sections,
                                        )
                                    except Exception:
                                        logger.warning(
                                            "ingest_business_summaries: 10-K/A fallback section "
                                            "upsert failed accession=%s",
                                            fallback_acc,
                                            exc_info=True,
                                        )
                                conn.commit()
                                if did_insert:
                                    inserted += 1
                                else:
                                    updated += 1
                                fallback_used = True
                            except Exception:
                                conn.rollback()
                                logger.warning(
                                    "ingest_business_summaries: 10-K/A fallback upsert failed accession=%s",
                                    fallback_acc,
                                    exc_info=True,
                                )
            if fallback_used:
                continue

            parse_misses += 1
            record_parse_attempt(
                conn,
                instrument_id=instrument_id,
                source_accession=accession,
                reason="no_item_1_marker",
            )
            conn.commit()
            continue
        if len(body) < _MIN_BODY_LEN:
            parse_misses += 1
            record_parse_attempt(
                conn,
                instrument_id=instrument_id,
                source_accession=accession,
                reason="body_too_short",
            )
            conn.commit()
            continue

        try:
            did_insert = upsert_business_summary(
                conn,
                instrument_id=instrument_id,
                body=body,
                source_accession=accession,
            )
            # #449 — also populate the sections table. We re-run the
            # sections extractor over the same HTML so the two writes
            # share a single fetch. Sections are a best-effort
            # enrichment: if subsection detection fails (no headings)
            # the extractor returns a single "general" block and the
            # blob view still renders. A failure inside the sections
            # upsert must not roll back the blob write.
            sections = extract_business_sections(html)
            if sections:
                try:
                    upsert_business_sections(
                        conn,
                        instrument_id=instrument_id,
                        source_accession=accession,
                        sections=sections,
                    )
                except Exception:
                    logger.warning(
                        "ingest_business_summaries: section upsert failed accession=%s "
                        "(blob already stored; rendering degrades to blob-only)",
                        accession,
                        exc_info=True,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_business_summaries: upsert failed accession=%s",
                accession,
                exc_info=True,
            )
            record_parse_attempt(
                conn,
                instrument_id=instrument_id,
                source_accession=accession,
                reason="upsert_exception",
            )
            conn.commit()
            continue

        if did_insert:
            inserted += 1
        else:
            updated += 1

    return IngestResult(
        filings_scanned=len(candidates),
        rows_inserted=inserted,
        rows_updated=updated,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
    )
