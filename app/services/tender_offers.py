"""Tender / going-private schedule extractor (Schedule TO / 14D-9 / 13E-3).

Issue #1982 (child of #1015 item 4; spec
docs/specs/filings/2026-07-05-tender-going-private-parser.md). Pure-Python
extraction of the Reg M-A cover disclosures + the EDGAR SGML header party
blocks from a tender-offer schedule, plus the typed-table upsert. The manifest
parser (``app/services/manifest_parsers/sec_tender.py``) orchestrates fetch /
store_raw / transition around :func:`parse_tender_offer`; extraction is pure
so it table-tests against real fixtures with no DB (single-chokepoint
discipline).

Source rule: Schedule TO (17 CFR 240.14d-100, Rules 14d-1(g) third-party /
13e-4 issuer self-tender), Schedule 14D-9 (17 CFR 240.14d-101, Rule 14d-9),
Schedule 13E-3 (17 CFR 240.13e-100, Rule 13e-3). Content per Reg M-A (17 CFR
229.1000-1016):

* **Item 1004(a)(1)(ii)/(v)** mandate the consideration + scheduled
  expiration CONTENT but not a standardized presentation — both are extracted
  by anchored body formulas ("for $124.00 per Share, net ... in cash";
  "expire ... on July 27, 2026"), nullable on no-match, gated by the #1982
  full-population dry-run.
* **Item 1012(a)** itself enumerates the only permitted 14D-9 board positions
  (accept / reject / no-opinion-neutral / unable-to-take-a-position) — a
  closed reg-enumerated vocabulary, which is what makes
  ``board_recommendation`` a deterministic pattern extraction, not free-text
  classification (prevention #1659 does not apply; unmatched prose stays
  NULL).
* **Attribution — EDGAR SGML filing header**: every accession's
  ``<acc>.hdr.sgml`` carries structured ``<SUBJECT-COMPANY>`` / ``<FILED-BY>``
  blocks with CIKs — the ONLY deterministic subject-vs-offeror source (the
  master index attributes a dual-party accession to BOTH CIKs; cover
  name-matching is fuzzy). A CIK appearing in both blocks (self-filed TO-I /
  14D-9 / most 13E-3) collapses to ``role='subject'``.

Checkbox glyph drift is real: modern filings use ``☒``/``☐``; older filer
agents emit ``x``/``¨`` (Wingdings — DSX 2026 amendments still do). Boxes are
anchored on their own LABEL text (never position) and duplicate labels
(NUVL's filer typo repeats the third-party line) resolve checked-anywhere-wins.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg

PARSER_VERSION = 1

# The eight forms _FORM_TO_SOURCE routes to ``sec_tender``. PREM14C/DEFM14C
# (Schedule 14A Item 14 prose — the #1659 trap) and SC TO-C (pre-commencement
# communications, Rule 14d-2(b)(1) — no Item 1004 terms attach) are
# deliberately out of scope; see the spec's Deferred section.
IN_SCOPE_FORMS: frozenset[str] = frozenset(
    {
        "SC TO-T",
        "SC TO-T/A",
        "SC TO-I",
        "SC TO-I/A",
        "SC 14D9",
        "SC 14D9/A",
        "SC 13E3",
        "SC 13E3/A",
    }
)

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# --- SGML header party blocks ----------------------------------------------
# ``<acc>.hdr.sgml`` tag-per-line format: ``<SUBJECT-COMPANY> ... <CONFORMED-
# NAME>Nuvalent, Inc. ... <CIK>0001861560 ... </SUBJECT-COMPANY>``. Values are
# not tag-closed; blocks may repeat (multiple FILED-BY filers).
_SUBJECT_BLOCK_RE = re.compile(r"<SUBJECT-COMPANY>(.*?)</SUBJECT-COMPANY>", re.DOTALL)
_FILED_BY_BLOCK_RE = re.compile(r"<FILED-BY>(.*?)</FILED-BY>", re.DOTALL)
_CONFORMED_NAME_RE = re.compile(r"<CONFORMED-NAME>([^\r\n<]+)")
_CIK_RE = re.compile(r"<CIK>(\d+)")

# --- Cover checkboxes -------------------------------------------------------
# Glyph sets observed on the real panel: modern ☒/☐ and legacy filer-agent
# x/X/¨/o (Wingdings). The label anchors are the Schedule TO cover's own
# statutory box texts; 13E-3's a-d context boxes and 14D-9 (no boxes) simply
# never match, leaving all four transaction booleans NULL for those forms.
_CHECKED_GLYPHS = frozenset({"☒", "☑", "x", "X"})
_UNCHECKED_GLYPHS = frozenset({"☐", "¨", "o", "¡"})

_BOX_LABELS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("is_third_party_tender", re.compile(r"third-?party tender offer subject to rule 14d-1", re.IGNORECASE)),
    ("is_issuer_tender", re.compile(r"issuer tender offer subject to rule 13e-4", re.IGNORECASE)),
    ("is_going_private", re.compile(r"going-?private transaction subject to rule 13e-3", re.IGNORECASE)),
    ("amends_13d", re.compile(r"amendment to schedule 13d under rule 13d-2", re.IGNORECASE)),
)
# The final-amendment box renders AFTER its sentence ("Check the following box
# if the filing is a final amendment reporting the results of the tender
# offer: ☐"), unlike the transaction boxes whose glyph precedes the label.
_FINAL_AMENDMENT_LABEL_RE = re.compile(r"final amendment reporting the results", re.IGNORECASE)
_FINAL_AMENDMENT_GLYPH_WINDOW = 80

# Cover region: checkboxes / amendment number / class title / CUSIP all sit on
# the statutory cover page. 8k chars of stripped text comfortably covers the
# largest fixture cover (BALY's 8-filer block) while excluding body prose.
_COVER_CHARS = 8000

_AMENDMENT_NO_RE = re.compile(r"\(Amendment No\.?\s*(\d+)\)", re.IGNORECASE)

_TITLE_OF_CLASS_RE = re.compile(r"\(Title of Class(?:es)? of Securities\)", re.IGNORECASE)
_CUSIP_HEADING_RE = re.compile(r"\(CUSIP Number", re.IGNORECASE)
_CUSIP_TOKEN_RE = re.compile(r"\b([0-9A-Z]{5,9})\*?\s*$")
_CLASS_TITLE_WINDOW = 220
_CUSIP_WINDOW = 40

# --- Item 1004(a)(1)(ii) offer price ---------------------------------------
# Anchored on the conventional offer formulas ONLY (empirical, dry-run
# gated). A naive ``$X per share`` false-positives on par value ("Common
# Stock, par value $0.01 per share" — every cover), hence the hard exclusion
# window before each candidate match.
_UNIT_WORDS = r"share|ads|american depositary share|note|unit|warrant"
_PRICE_FORMULA_RE = re.compile(
    rf"(?:for|at|of)\s+(?:US\s?)?([$€£])\s?([0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    rf"(?:in cash\s+)?per\s+({_UNIT_WORDS})s?\b[^.]{{0,60}}?(?:net|in cash)",
    re.IGNORECASE,
)
_PRICE_LABEL_RE = re.compile(
    r"(?:purchase price|offer price)[^$€£.]{0,80}?([$€£])\s?([0-9][0-9,]*(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
_PAR_VALUE_WINDOW = 40
_PAR_VALUE_RE = re.compile(r"par value", re.IGNORECASE)
_CURRENCY_BY_GLYPH = {"$": "USD", "€": "EUR", "£": "GBP"}
# Item 1004(a) terms are stated in the schedule's cover/introduction/items
# region at the FRONT of the document. A long 14D-9's Item 4 "Background"
# recounts superseded bids much deeper (LPRO: real $3.15 at char 3.7k;
# rejected $3.50 / $2.00 history from 65k) — scanning the whole body would
# trip the distinct-amounts ambiguity rule on genuinely priced offers.
# Window measured on the fixture panel (max real-price offset: DSX 8.0k);
# gated by the full-population dry-run.
_PRICE_SCAN_CHARS = 15_000

# --- Item 1004(a)(1)(v) scheduled expiration --------------------------------
_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_EXPIRATION_RE = re.compile(
    # "offer ... expire ... on <Month D, YYYY>". The offer→expire anchor
    # rejects other expiries ("Debt Commitment Letter expires on ..." — DSX).
    # The expire→date gap must admit periods ("one minute after 11:59 p.m.,
    # New York City time, on July 27, 2026" — LPRO), so it is length-bounded
    # rather than sentence-bounded.
    rf"offer[^.]{{0,80}}?\bexpir\w*.{{0,140}}?\bon\s+({_MONTHS})\s+(\d{{1,2}}),\s+(\d{{4}})",
    re.IGNORECASE,
)

# --- Item 1012(a) board recommendation (14D-9 only) -------------------------
# The rule enumerates the only permitted positions; each maps to one anchored
# formula. Multiple DISTINCT positions matching => NULL (ambiguous prose),
# never a guess.
_RECOMMENDATION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("accept", re.compile(r"recommends?\b[^.]{0,160}?\baccept the offer", re.IGNORECASE)),
    ("reject", re.compile(r"recommends?\b[^.]{0,160}?\breject(?:ion of)? the offer", re.IGNORECASE)),
    ("neutral", re.compile(r"no opinion\b[^.]{0,80}?remaining neutral", re.IGNORECASE)),
    ("unable", re.compile(r"unable to take a position", re.IGNORECASE)),
)


@dataclass(frozen=True)
class HeaderParty:
    """One SGML header party block: zero-padded CIK + conformed name."""

    cik: str
    name: str


@dataclass(frozen=True)
class TenderOfferParse:
    """Extracted header parties + Reg M-A cover/body fields."""

    form: str
    subject: HeaderParty
    filed_by: tuple[HeaderParty, ...]
    is_third_party_tender: bool | None
    is_issuer_tender: bool | None
    is_going_private: bool | None
    amends_13d: bool | None
    is_final_amendment: bool | None
    amendment_no: int | None
    offer_price_per_unit: Decimal | None
    unit_label: str | None
    currency: str | None
    expiration_date: date | None
    board_recommendation: str | None
    security_class_title: str | None
    cusip: str | None


def _strip_html(raw: str) -> str:
    # Tags first, entities second (same rationale as nt_notices._strip_html).
    no_tags = _HTML_TAG_RE.sub(" ", raw)
    unescaped = html.unescape(no_tags)
    return _WS_RE.sub(" ", unescaped).strip()


def _parse_header_block(block: str) -> HeaderParty | None:
    name_m = _CONFORMED_NAME_RE.search(block)
    cik_m = _CIK_RE.search(block)
    if name_m is None or cik_m is None:
        return None
    return HeaderParty(cik=cik_m.group(1).zfill(10), name=name_m.group(1).strip())


def _parse_header_parties(header_sgml: str) -> tuple[HeaderParty, tuple[HeaderParty, ...]] | None:
    """(subject, filed_by...) from the SGML header, or ``None`` if unusable."""
    subjects = [p for b in _SUBJECT_BLOCK_RE.findall(header_sgml) if (p := _parse_header_block(b))]
    if not subjects:
        return None
    filed_by = tuple(p for b in _FILED_BY_BLOCK_RE.findall(header_sgml) if (p := _parse_header_block(b)))
    return subjects[0], filed_by


def _glyph_state(glyph: str) -> bool | None:
    if glyph in _CHECKED_GLYPHS:
        return True
    if glyph in _UNCHECKED_GLYPHS:
        return False
    return None


def _box_state_before_label(cover: str, label_re: re.Pattern[str]) -> bool | None:
    """Transaction-type box: glyph is the last token BEFORE the label.

    Duplicate labels (NUVL filer typo) resolve checked-anywhere-wins; all
    occurrences unchecked => False; no occurrence / no resolvable glyph =>
    None (never guessed).
    """
    state: bool | None = None
    for m in label_re.finditer(cover):
        preceding = cover[: m.start()].rstrip()
        if not preceding:
            continue
        got = _glyph_state(preceding[-1])
        if got is True:
            return True
        if got is False:
            state = False
    return state


def _final_amendment_state(cover: str) -> bool | None:
    """Final-amendment box: glyph FOLLOWS the sentence's colon."""
    state: bool | None = None
    for m in _FINAL_AMENDMENT_LABEL_RE.finditer(cover):
        window = cover[m.end() : m.end() + _FINAL_AMENDMENT_GLYPH_WINDOW]
        for ch in window:
            got = _glyph_state(ch)
            if got is True:
                return True
            if got is False:
                state = False
                break
    return state


def _has_par_value_context(text: str, match_start: int) -> bool:
    return _PAR_VALUE_RE.search(text, max(0, match_start - _PAR_VALUE_WINDOW), match_start) is not None


def _extract_price(text: str) -> tuple[Decimal | None, str | None, str | None]:
    """(price, unit_label, currency) via the anchored Item 1004(a)(1)(ii)
    formulas, scanned over the front ``_PRICE_SCAN_CHARS`` region only.
    Conflicting distinct amounts across matches => all-NULL (ambiguous).
    Currency comes from the glyph AT the matched price and is NULL whenever
    the price is NULL — never defaulted."""
    text = text[:_PRICE_SCAN_CHARS]
    candidates: list[tuple[Decimal, str | None, str]] = []
    for m in _PRICE_FORMULA_RE.finditer(text):
        if _has_par_value_context(text, m.start()):
            continue
        amount = _to_decimal(m.group(2))
        if amount is None:
            continue
        unit = m.group(3).lower()
        label = "ADS" if unit == "ads" else " ".join(w.capitalize() for w in unit.split())
        candidates.append((amount, label, _CURRENCY_BY_GLYPH[m.group(1)]))
    for m in _PRICE_LABEL_RE.finditer(text):
        if _has_par_value_context(text, m.start()):
            continue
        amount = _to_decimal(m.group(2))
        if amount is None:
            continue
        candidates.append((amount, None, _CURRENCY_BY_GLYPH[m.group(1)]))
    if not candidates:
        return None, None, None
    amounts = {c[0] for c in candidates}
    if len(amounts) > 1:
        return None, None, None
    # Prefer the formula match's unit label when any candidate carried one.
    unit_label = next((c[1] for c in candidates if c[1] is not None), None)
    return candidates[0][0], unit_label, candidates[0][2]


def _to_decimal(token: str) -> Decimal | None:
    try:
        return Decimal(token.replace(",", ""))
    except InvalidOperation:  # pragma: no cover — regex pre-validates
        return None


def _extract_expiration(text: str) -> date | None:
    m = _EXPIRATION_RE.search(text)
    if m is None:
        return None
    try:
        return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%B %d %Y").date()
    except ValueError:  # pragma: no cover — month alternation pre-validates
        return None


def _extract_recommendation(text: str) -> str | None:
    hits = {name for name, pattern in _RECOMMENDATION_PATTERNS if pattern.search(text)}
    if len(hits) == 1:
        return hits.pop()
    return None


def _extract_class_title(cover: str) -> str | None:
    m = _TITLE_OF_CLASS_RE.search(cover)
    if m is None:
        return None
    window = cover[max(0, m.start() - _CLASS_TITLE_WINDOW) : m.start()].strip()
    # Cut at the previous parenthesised cover heading, if one is in-window.
    paren = window.rfind(")")
    if paren != -1:
        window = window[paren + 1 :]
    title = window.strip()
    return title or None


def _extract_cusip(cover: str) -> str | None:
    m = _CUSIP_HEADING_RE.search(cover)
    if m is None:
        return None
    window = cover[max(0, m.start() - _CUSIP_WINDOW) : m.start()].strip()
    token_m = _CUSIP_TOKEN_RE.search(window)
    return token_m.group(1) if token_m else None


def parse_tender_offer(body_html: str, header_sgml: str, form: str) -> TenderOfferParse | None:
    """Extract the header parties + Reg M-A cover/body fields.

    Returns ``None`` when the SGML header carries no usable SUBJECT-COMPANY
    block or the body is not a recognizable schedule (caller tombstones). A
    recognizable schedule with unresolved cover fields yields a row of NULLs —
    "a tender event exists, parties known" is itself the thesis signal.
    """
    if form not in IN_SCOPE_FORMS:
        raise ValueError(f"form must be one of {sorted(IN_SCOPE_FORMS)}, got {form!r}")
    parties = _parse_header_parties(header_sgml)
    if parties is None:
        return None
    subject, filed_by = parties

    text = _strip_html(body_html)
    low = text.lower()
    if "schedule to" not in low and "schedule 14d-9" not in low and "schedule 13e-3" not in low:
        return None

    cover = text[:_COVER_CHARS]

    price, unit_label, currency = _extract_price(text)
    amendment_m = _AMENDMENT_NO_RE.search(cover)

    return TenderOfferParse(
        form=form,
        subject=subject,
        filed_by=filed_by,
        is_third_party_tender=_box_state_before_label(cover, _BOX_LABELS[0][1]),
        is_issuer_tender=_box_state_before_label(cover, _BOX_LABELS[1][1]),
        is_going_private=_box_state_before_label(cover, _BOX_LABELS[2][1]),
        amends_13d=_box_state_before_label(cover, _BOX_LABELS[3][1]),
        is_final_amendment=_final_amendment_state(cover),
        amendment_no=int(amendment_m.group(1)) if amendment_m else None,
        offer_price_per_unit=price,
        unit_label=unit_label,
        currency=currency,
        expiration_date=_extract_expiration(text),
        board_recommendation=_extract_recommendation(text) if form.startswith("SC 14D9") else None,
        security_class_title=_extract_class_title(cover),
        cusip=_extract_cusip(cover),
    )


def resolve_party_roles(parse: TenderOfferParse) -> dict[str, str]:
    """CIK -> role for every header party. A CIK in BOTH blocks (self-filed
    forms carry SUBJECT-COMPANY and FILED-BY for the same company) collapses
    to ``subject`` — there is never a per-CIK role conflict."""
    roles: dict[str, str] = {p.cik: "offeror" for p in parse.filed_by}
    roles[parse.subject.cik] = "subject"
    return roles


def map_ciks_to_instruments(conn: psycopg.Connection[Any], ciks: list[str]) -> dict[str, int]:
    """CIK -> instrument_id via ``external_identifiers`` (values stored
    zero-padded to 10). Primary-preferred to mirror the manifest seed's
    LATERAL resolution. CIKs outside the universe are simply absent."""
    if not ciks:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (identifier_value) identifier_value, instrument_id
            FROM external_identifiers
            WHERE provider = 'sec'
              AND identifier_type = 'cik'
              AND identifier_value = ANY(%(ciks)s)
            ORDER BY identifier_value, is_primary DESC, external_identifier_id ASC
            """,
            {"ciks": ciks},
        )
        return {str(value): int(instrument_id) for value, instrument_id in cur.fetchall()}


def upsert_tender_offer_events(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    parse: TenderOfferParse,
    instrument_roles: list[tuple[int, str]],
) -> None:
    """Upsert one typed row per (accession, matched instrument)."""
    offeror_names = [p.name for p in parse.filed_by if p.cik != parse.subject.cik]
    for instrument_id, role in instrument_roles:
        conn.execute(
            """
            INSERT INTO tender_offer_events (
                accession_number, instrument_id, role, form,
                subject_company_name, subject_cik, offeror_names,
                is_third_party_tender, is_issuer_tender, is_going_private,
                amends_13d, is_final_amendment, amendment_no,
                offer_price_per_unit, unit_label, currency, expiration_date,
                board_recommendation, security_class_title, cusip,
                parser_version, parsed_at
            ) VALUES (
                %(accession)s, %(instrument_id)s, %(role)s, %(form)s,
                %(subject_name)s, %(subject_cik)s, %(offeror_names)s,
                %(third_party)s, %(issuer)s, %(going_private)s,
                %(amends_13d)s, %(final_amendment)s, %(amendment_no)s,
                %(price)s, %(unit_label)s, %(currency)s, %(expiration)s,
                %(recommendation)s, %(class_title)s, %(cusip)s,
                %(parser_version)s, NOW()
            )
            ON CONFLICT (accession_number, instrument_id) DO UPDATE SET
                role = EXCLUDED.role,
                form = EXCLUDED.form,
                subject_company_name = EXCLUDED.subject_company_name,
                subject_cik = EXCLUDED.subject_cik,
                offeror_names = EXCLUDED.offeror_names,
                is_third_party_tender = EXCLUDED.is_third_party_tender,
                is_issuer_tender = EXCLUDED.is_issuer_tender,
                is_going_private = EXCLUDED.is_going_private,
                amends_13d = EXCLUDED.amends_13d,
                is_final_amendment = EXCLUDED.is_final_amendment,
                amendment_no = EXCLUDED.amendment_no,
                offer_price_per_unit = EXCLUDED.offer_price_per_unit,
                unit_label = EXCLUDED.unit_label,
                currency = EXCLUDED.currency,
                expiration_date = EXCLUDED.expiration_date,
                board_recommendation = EXCLUDED.board_recommendation,
                security_class_title = EXCLUDED.security_class_title,
                cusip = EXCLUDED.cusip,
                parser_version = EXCLUDED.parser_version,
                parsed_at = NOW()
            """,
            {
                "accession": accession_number,
                "instrument_id": instrument_id,
                "role": role,
                "form": parse.form,
                "subject_name": parse.subject.name,
                "subject_cik": parse.subject.cik,
                "offeror_names": json.dumps(offeror_names) if offeror_names else None,
                "third_party": parse.is_third_party_tender,
                "issuer": parse.is_issuer_tender,
                "going_private": parse.is_going_private,
                "amends_13d": parse.amends_13d,
                "final_amendment": parse.is_final_amendment,
                "amendment_no": parse.amendment_no,
                "price": parse.offer_price_per_unit,
                "unit_label": parse.unit_label,
                "currency": parse.currency,
                "expiration": parse.expiration_date,
                "recommendation": parse.board_recommendation,
                "class_title": parse.security_class_title,
                "cusip": parse.cusip,
                "parser_version": PARSER_VERSION,
            },
        )
