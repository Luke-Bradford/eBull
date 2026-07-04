"""PRE 14A / PRER14A meeting-agenda proposal-signal extractor.

Issue #1892 (#1015 item 3). Pure-Python extraction of the Rule 14a-4(a)(3)
numbered "purposes" / "items of business" agenda list from a proxy's Notice
of Meeting section, plus the typed-table upsert. The manifest parser
(``app/services/manifest_parsers/sec_pre14a.py``) orchestrates fetch /
store_raw / transition around :func:`parse_pre14a_proposals`; keeping the
extraction pure here lets it table-test against real fixtures with no DB
(single-chokepoint discipline — all field logic lives in this one function).

Source rule: Regulation 14A, Schedule 14A (17 CFR 240.14a-101) + Rule
14a-4(a)(3) (17 CFR 240.14a-4), which requires the proxy to "identify
clearly and impartially each separate matter intended to be acted upon" —
the numbered agenda list every proxy's Notice of Meeting carries. Category
anchors verified against eCFR: Item 11 (share-authorization increases),
Item 19 (charter amendments incl. reverse stock splits), Item 24 / Rule
14a-21(a) (say-on-pay, distinct from the Rule 14a-21(b) say-on-frequency
vote — see ``_SAY_ON_PAY_RE`` note below).

Full-population verification (10 real PRE 14A filings, 2026-06-26..07-02):
anchoring classification on the numbered agenda-list items ONLY (not the
whole document) eliminates the false-positive class a naive whole-document
keyword scan produces (risk-factor / contingency prose mentioning
"authorized shares" without an actual proposal) — see #1892 for the sample
detail.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

PARSER_VERSION = 1

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# Maximum length retained per agenda-item string. These are operator/LLM
# context, not a document store; cap it (mirrors nt_notices._REASON_MAX).
_ITEM_MAX_CHARS = 2000
# Safety cap on the number of agenda items retained per accession — real
# proxies carry at most a handful of proposals; a much larger match count
# signals the end-anchor search failed to bound the block (defensive, not
# expected to trigger on real filings).
_MAX_ITEMS = 30

_INTRO_RE = re.compile(
    r"following\s+(?:proposals|items?\s+of\s+business|matters|purposes)"
    r"|you\s+will\s+be\s+asked\s+to"
    r"|items?\s+of\s+business\s*:?"
    r"|purpose[^.]{0,40}?is\s+the\s+following",
    re.IGNORECASE,
)

# Bound the intro search to the front of the document — the Notice of
# Meeting always opens a proxy (SEC convention), so this both keeps the
# broadened _INTRO_RE from false-positive-matching similar phrasing deep in
# the body, and avoids the whole-document keyword-scan false-positive class
# #1892's full-population check flagged.
_INTRO_SEARCH_CHARS = 15000

# Phrases that mark the END of the numbered agenda block — whichever comes
# first, searched only AFTER item 1 is found (so a "more fully described"
# clause in the INTRO sentence, before any items, can't truncate the block
# to zero items — see #1892 Earth Science Tech fixture).
_END_ANCHORS = (
    "more fully described",
    "the board recommends",
    "other business will be transacted",
    "your vote is important",
    "all stockholders as of",
    "we are providing our proxy materials",
    "table of contents",
    "only stockholders of record",
    "who may vote",
)

# One numbered agenda item start, in either of two real-world renderings:
#   - a bare or period-terminated number at a word boundary, followed by a
#     capital letter or opening paren ("1. To approve...", "1 A proposal
#     to...")
#   - a parenthesized number followed by a letter ("(1) Elect...", "(1) To
#     elect...") — the GameStop / Home Depot Notice-of-Meeting convention.
# Sequential-only acceptance (see below) rejects stray digit matches
# unrelated to the agenda numbering.
_ITEM_START_RE = re.compile(r"(?:(?<=\s)|^)(\d{1,2})\.?\s+(?=[A-Z(])|\((\d{1,2})\)\s*(?=[A-Za-z])")

# Window (chars) after the intro sentence to search for item "1" — bounds
# the search so an unrelated "1." elsewhere in the preamble isn't mistaken
# for the agenda start.
_FIRST_ITEM_WINDOW = 600

_REVERSE_SPLIT_RE = re.compile(r"reverse\s+(?:stock\s+)?split", re.IGNORECASE)

# Item 11 (17 CFR 240.14a-101, Schedule 14A) — authorization/issuance of
# securities via a CHARTER-LEVEL increase to the corporation's total
# authorized share count. Requires "increase" AND "authoriz*" AND
# "share"/"stock" all within the single (already-bounded) agenda item, in
# either phrase order.
#
# A textually-similar but DIFFERENT proposal is increasing the share pool
# reserved for an equity/incentive/option PLAN ("increase the number of
# shares of common stock authorized for issuance under the 2020 Equity
# Incentive Plan") — that is Schedule 14A Item 10 (compensation plans), not
# the Item 11 charter amendment this category targets. The two can use
# near-identical "authorized ... shares ... common stock" phrasing, so the
# regex alone can't distinguish them; ``parse_pre14a_proposals`` additionally
# excludes any item whose text contains "plan" (Codex ckpt-2 finding, #1892)
# — the conservative choice (fails toward NOT flagging a charter-increase
# signal, per the project's established policy for structurally ambiguous
# signals). This is a plain substring check, not folded into the regex: a
# negative lookahead here would only anchor the "no plan ahead" assertion at
# the match's OWN start position, and ``re.search`` tries every start
# position in the string — once the scan advances past the word "plan", a
# later start position sees no "plan" ahead and the lookahead trivially
# passes, silently defeating the exclusion.
_SHARE_INCREASE_RE = re.compile(
    r"(?=.*\bincreas\w*\b)(?=.*\bauthoriz\w*\b)(?=.*\b(?:share|stock)s?\b)",
    re.IGNORECASE | re.DOTALL,
)

# Rule 14a-21(a) — shareholder advisory vote to approve executive
# compensation ("say-on-pay"). Deliberately does NOT match the Rule
# 14a-21(b) say-on-FREQUENCY vote ("how often" / "frequency" of future
# say-on-pay votes) — that is a distinct proposal category #1892 does not
# ask for, and matching it here would misclassify it as say-on-pay.
_SAY_ON_PAY_RE = re.compile(
    r"say-on-pay|say\s+on\s+pay"
    r"|advisory,?\s+(?:\(?non-binding\)?,?\s+)?vote\s+(?:on|to\s+approve)\s+(?:the\s+)?"
    r"(?:compensation\s+of\s+(?:our|the)\s+named\s+executive\s+officers|executive\s+compensation)",
    re.IGNORECASE,
)


def _item_number(match: re.Match[str]) -> int:
    """Return the numbered-item value from an ``_ITEM_START_RE`` match —
    whichever of its two alternative groups (bare/period-terminated vs
    parenthesized) fired."""
    group1, group2 = match.group(1), match.group(2)
    return int(group1 if group1 is not None else group2)  # type: ignore[arg-type]


def _strip_html(raw: str) -> str:
    # Strip tags FIRST, then unescape entities (mirrors nt_notices._strip_html
    # — an entity-encoded ``&lt;`` in text must not become a fake tag).
    no_tags = _HTML_TAG_RE.sub(" ", raw)
    unescaped = html.unescape(no_tags)
    return _WS_RE.sub(" ", unescaped).strip()


def _extract_agenda_items(text: str) -> list[str] | None:
    """Return the numbered agenda-item texts, or ``None`` if no recognizable
    Rule 14a-4(a)(3) numbered proposals list is present."""
    low = text.lower()
    intro = _INTRO_RE.search(low[:_INTRO_SEARCH_CHARS])
    if intro is None:
        return None

    window = text[intro.end() : intro.end() + _FIRST_ITEM_WINDOW]
    first_start: int | None = None
    for m in _ITEM_START_RE.finditer(window):
        if _item_number(m) == 1:
            first_start = intro.end() + m.start()
            break
    if first_start is None:
        return None

    end = len(text)
    for anchor in _END_ANCHORS:
        idx = low.find(anchor, first_start + 1)
        if idx != -1:
            end = min(end, idx)

    block = text[first_start:end]
    matches = list(_ITEM_START_RE.finditer(block))
    accepted = []
    expected = 1
    for m in matches:
        if _item_number(m) == expected:
            accepted.append(m)
            expected += 1
            if len(accepted) >= _MAX_ITEMS:
                break

    if not accepted:
        return None

    items: list[str] = []
    for i, m in enumerate(accepted):
        start = m.end()
        stop = accepted[i + 1].start() if i + 1 < len(accepted) else len(block)
        item_text = block[start:stop].strip()
        if item_text:
            items.append(item_text[:_ITEM_MAX_CHARS])
    return items or None


@dataclass(frozen=True)
class Pre14aProposalSignal:
    """Extracted proposal-signal fields from a PRE 14A / PRER14A body."""

    proposal_count: int
    reverse_stock_split_proposal: bool
    authorized_share_increase_proposal: bool
    say_on_pay_advisory_vote: bool
    agenda_items: tuple[str, ...]


def parse_pre14a_proposals(body: str) -> Pre14aProposalSignal | None:
    """Extract the Rule 14a-4(a)(3) numbered agenda list + category flags
    from *body* (HTML or text).

    Returns ``None`` when *body* has no recognizable numbered proposals list
    (caller tombstones).
    """
    text = _strip_html(body)
    items = _extract_agenda_items(text)
    if items is None:
        return None
    return Pre14aProposalSignal(
        proposal_count=len(items),
        reverse_stock_split_proposal=any(_REVERSE_SPLIT_RE.search(i) for i in items),
        authorized_share_increase_proposal=any(_SHARE_INCREASE_RE.search(i) and "plan" not in i.lower() for i in items),
        say_on_pay_advisory_vote=any(_SAY_ON_PAY_RE.search(i) for i in items),
        agenda_items=tuple(items),
    )


def upsert_pre14a_proposal_signal(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    signal: Pre14aProposalSignal,
) -> None:
    """Upsert one parsed PRE 14A / PRER14A into ``pre14a_proposal_signals``."""
    conn.execute(
        """
        INSERT INTO pre14a_proposal_signals (
            accession_number, instrument_id, proposal_count,
            reverse_stock_split_proposal, authorized_share_increase_proposal,
            say_on_pay_advisory_vote, agenda_items, parser_version, parsed_at
        ) VALUES (
            %(accession)s, %(instrument_id)s, %(proposal_count)s,
            %(reverse_split)s, %(share_increase)s,
            %(say_on_pay)s, %(agenda_items)s, %(parser_version)s, NOW()
        )
        ON CONFLICT (accession_number) DO UPDATE SET
            instrument_id = EXCLUDED.instrument_id,
            proposal_count = EXCLUDED.proposal_count,
            reverse_stock_split_proposal = EXCLUDED.reverse_stock_split_proposal,
            authorized_share_increase_proposal = EXCLUDED.authorized_share_increase_proposal,
            say_on_pay_advisory_vote = EXCLUDED.say_on_pay_advisory_vote,
            agenda_items = EXCLUDED.agenda_items,
            parser_version = EXCLUDED.parser_version,
            parsed_at = NOW()
        """,
        {
            "accession": accession_number,
            "instrument_id": instrument_id,
            "proposal_count": signal.proposal_count,
            "reverse_split": signal.reverse_stock_split_proposal,
            "share_increase": signal.authorized_share_increase_proposal,
            "say_on_pay": signal.say_on_pay_advisory_vote,
            "agenda_items": Jsonb(list(signal.agenda_items)),
            "parser_version": PARSER_VERSION,
        },
    )
