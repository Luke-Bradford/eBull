"""424B prospectus cover-page offering extractor (Rule 424(b) → structured).

Issue #1816 (child of #1015 item 2; spec
docs/specs/filings/2026-07-05-424b-prospectus-offering-parser.md). Pure-Python
extraction of the Reg S-K Item 501(b)(3) cover offering disclosure ("Price to
Public" / "Underwriting Discounts and Commissions" / "Proceeds to ...") from a
424B prospectus body, plus the typed-table upsert. The manifest parser
(``app/services/manifest_parsers/sec_424b.py``) orchestrates fetch / store_raw /
transition around :func:`parse_prospectus_offering`; extraction is pure so it
table-tests against real fixtures with no DB (single-chokepoint discipline).

Source rule: Securities Act Rule 424(b)(1)-(8) (17 CFR 230.424(b)) — the
subtype is a filing-trigger bucket, NOT an instrument taxonomy. The economic
facts come from the parsed cover per Reg S-K Item 501(b)(3) (17 CFR
229.501(b)(3)), which mandates the Price-to-Public / Underwriting-Discount /
Proceeds disclosure "where you offer securities for cash" but does NOT mandate
a table — extraction is best-effort and every money field is nullable.

Three physical cover layouts observed on the real-fixture panel (FPS 424B4,
MLCI 424B1, TD 424B3, JEF 424B5, ADT 424B7):

  * row-major   — ``Per Share Total <label> $ per $ total ...`` (FPS, MLCI)
  * column-major — labels first, then ``Per Note $a $b $c Total $A $B $C`` (TD)
  * percent-of-principal — structured notes price as ``100.00%`` with empty
    ``$`` cells (JEF) → money fields NULL, never fabricated.

A cover with no resolvable Item 501(b)(3) presentation (ADT B7 resale shelf)
yields a row with NULL money fields — that is a VALID outcome ("an offering
happened"), not a tombstone. ``None`` is returned only when the body is not a
recognizable prospectus at all.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg

PARSER_VERSION = 1

IN_SCOPE_SUBTYPES: frozenset[str] = frozenset({"424B1", "424B3", "424B4", "424B5", "424B7"})

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# --- Item 501(b)(3) label anchors (longest alternative first) --------------
# "Public offering price" dominates modern covers; "Price to (the) Public" is
# the literal Item 501(b)(3) wording still used by some filers.
_PRICE_LABEL_RE = re.compile(
    r"(?:initial )?public offering price|price to (?:the )?public",
    re.IGNORECASE,
)
_UW_LABEL_RE = re.compile(
    r"underwriting discounts? and commissions?"
    r"|underwriting discounts?"
    r"|underwriting commissions?"
    r"|selling agent'?s? fees?"
    r"|placement agent'?s? fees?",
    re.IGNORECASE,
)
_PROCEEDS_LABEL_RE = re.compile(
    r"(?:net )?proceeds(?:, before expenses,?)? to\b",
    re.IGNORECASE,
)

# Cover pricing tables cluster the three labels tightly; the TOC / running
# headers never carry all three adjacent. Windows measured on the fixtures
# (max observed: price→underwriting +45 chars, price→proceeds +80).
_UW_WINDOW = 300
_PROCEEDS_WINDOW = 700

# ``$ 2,138,850,000`` / ``$1,000.00`` / ``$ 49.000`` — the ``$`` prefix is the
# value/footnote discriminator: footnote superscript digits ("(1)", bare "1 2")
# are never ``$``-prefixed.
_MONEY_RE = re.compile(r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)")
# Price range ("$8.00 to $10.00" / "$8.00 – $10.00") → per-unit price NULL.
_RANGE_RE = re.compile(r"\$\s*[0-9][0-9,.]*\s*(?:-|–|—|to)\s*\$\s*[0-9]")

_UNIT_WORDS = (
    r"american depositary share|depositary share|share|note|unit|ads|bond|"
    r"warrant|right|security"
)
_UNIT_LABEL_RE = re.compile(rf"per ({_UNIT_WORDS})s?\b", re.IGNORECASE)
_TOTAL_WORD_RE = re.compile(r"\btotal\b", re.IGNORECASE)

# Both closed ("securityholders") and spaced ("security holders") renderings
# appear on real covers (Codex ckpt-2).
_SELLING_HOLDER_RE = re.compile(r"selling\s+(?:stock|share|security)\s*holders?", re.IGNORECASE)
# Chars scanned after a "Proceeds ... to" label to classify issuer-vs-selling.
_PROCEEDS_CLASSIFY_WINDOW = 60

# Money rows scanned per segment. Segments are cut at the NEXT label, but the
# last proceeds segment has no following label — cap it so trailing footnote
# dollars ("estimated expenses of $800,000") don't leak in.
_LAST_SEGMENT_CAP = 250

# A follow-on proceeds row (selling holders after issuer) sits one table row
# below the previous one — max observed pitch ~60 chars of values between the
# labels. A "proceeds to" further away belongs to different prose / a second
# table (TD carries two pricing tables 1k chars apart) and must NOT be chained.
_PROCEEDS_ROW_PITCH = 180

# Coarse cover-title security-type patterns, matched over the first slice of
# the stripped body; the earliest match wins. Advisory display label only —
# drives no semantic flag (spec: is_issuer_offering is proceeds-derived).
_SECURITY_TYPE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bcommon stock\b", re.IGNORECASE), "Common Stock"),
    (re.compile(r"\bordinary shares?\b", re.IGNORECASE), "Ordinary Shares"),
    (re.compile(r"\bamerican depositary shares?\b", re.IGNORECASE), "ADSs"),
    (re.compile(r"\bpreferred stock\b", re.IGNORECASE), "Preferred Stock"),
    (re.compile(r"\bdepositary shares?\b", re.IGNORECASE), "Depositary Shares"),
    (re.compile(r"\bnotes?\b", re.IGNORECASE), "Notes"),
    (re.compile(r"\bwarrants?\b", re.IGNORECASE), "Warrants"),
    (re.compile(r"\bunits?\b", re.IGNORECASE), "Units"),
)
_SECURITY_TYPE_SCAN_CHARS = 2500

_CURRENCY_GLYPHS: tuple[tuple[str, str], ...] = (("€", "EUR"), ("£", "GBP"), ("C$", "CAD"))


@dataclass(frozen=True)
class ProspectusOffering:
    """Extracted Item 501(b)(3) cover fields from a 424B prospectus body."""

    subtype: str
    is_issuer_offering: bool | None
    price_per_unit: Decimal | None
    unit_label: str | None
    aggregate_offering_amount: Decimal | None
    underwriting_discount: Decimal | None
    net_proceeds_to_issuer: Decimal | None
    proceeds_to_selling_holders: Decimal | None
    currency: str
    security_type: str | None


def _strip_html(raw: str) -> str:
    # Tags first, entities second (same rationale as nt_notices._strip_html).
    no_tags = _HTML_TAG_RE.sub(" ", raw)
    unescaped = html.unescape(no_tags)
    return _WS_RE.sub(" ", unescaped).strip()


def _to_decimal(token: str) -> Decimal | None:
    try:
        return Decimal(token.replace(",", ""))
    except InvalidOperation:  # pragma: no cover — regex pre-validates
        return None


def _segment_money(segment: str) -> list[Decimal]:
    """``$``-prefixed numerals in *segment*, in order.

    A ``%`` before the first ``$`` value marks a percent-of-principal row
    (structured-note covers): the dollar cells are empty and any trailing
    ``$ 1`` is a footnote marker, not money → no values.
    """
    first_money = _MONEY_RE.search(segment)
    if first_money is None:
        return []
    pct = segment.find("%")
    if pct != -1 and pct < first_money.start():
        return []
    return [d for m in _MONEY_RE.finditer(segment) if (d := _to_decimal(m.group(1))) is not None]


def _find_cover_cluster(text: str) -> tuple[re.Match[str], re.Match[str], re.Match[str]] | None:
    """First (price, underwriting, proceeds) label cluster — the cover table.

    Anchoring on the CLUSTER (not the first label hit) skips TOC / running-
    header duplicates: only the cover pricing presentation carries all three
    labels within a few hundred chars.
    """
    for price_m in _PRICE_LABEL_RE.finditer(text):
        uw_m = _UW_LABEL_RE.search(text, price_m.end(), price_m.end() + _UW_WINDOW)
        if uw_m is None:
            continue
        proceeds_m = _PROCEEDS_LABEL_RE.search(text, uw_m.end(), price_m.end() + _PROCEEDS_WINDOW)
        if proceeds_m is None:
            continue
        # Tighten the price anchor: prose can mention "the public offering
        # price of the Notes" just before the actual table label (TD 424B3).
        # The table row is the LAST price label preceding the underwriting one.
        while True:
            closer = _PRICE_LABEL_RE.search(text, price_m.end(), uw_m.start())
            if closer is None:
                break
            price_m = closer
        return price_m, uw_m, proceeds_m
    return None


def _unit_label_before(text: str, pos: int) -> str | None:
    """Column-header unit label ("Per Share Total") just before the cluster."""
    window = text[max(0, pos - 120) : pos]
    m = _UNIT_LABEL_RE.search(window)
    if m and _TOTAL_WORD_RE.search(window, m.end()):
        return _normalize_unit(m.group(1))
    return None


def _normalize_unit(word: str) -> str:
    low = word.lower()
    if low == "ads":
        return "Per ADS"
    return "Per " + " ".join(w.capitalize() for w in low.split())


def _classify_proceeds(text: str, proceeds_m: re.Match[str]) -> bool:
    """True = selling-holder proceeds row, False = issuer proceeds row."""
    tail = text[proceeds_m.end() : proceeds_m.end() + _PROCEEDS_CLASSIFY_WINDOW]
    return _SELLING_HOLDER_RE.search(tail) is not None


def _detect_currency(cluster_window: str) -> str:
    for glyph, code in _CURRENCY_GLYPHS:
        if glyph in cluster_window:
            return code
    return "USD"


def _detect_security_type(text: str) -> str | None:
    head = text[:_SECURITY_TYPE_SCAN_CHARS]
    best: tuple[int, str] | None = None
    for pattern, label in _SECURITY_TYPE_PATTERNS:
        m = pattern.search(head)
        if m and (best is None or m.start() < best[0]):
            best = (m.start(), label)
    return best[1] if best else None


def _row_values(segment: str, *, has_unit_header: bool) -> tuple[Decimal | None, Decimal | None]:
    """(per_unit, total) for one row-major cover row.

    Two ``$`` values ⇒ (per-unit, total) — the Item 501(b)(3) two-column
    presentation. One value with NO per-unit header ⇒ a total-only cover.
    One value WITH a per-unit header is ambiguous (which column is empty?) ⇒
    both NULL — never guess.
    """
    if _RANGE_RE.search(segment):
        return None, None
    values = _segment_money(segment)
    if len(values) >= 2:
        return values[0], values[1]
    if len(values) == 1 and not has_unit_header:
        return None, values[0]
    return None, None


def parse_prospectus_offering(body: str, subtype: str) -> ProspectusOffering | None:
    """Extract the Item 501(b)(3) cover disclosure from a 424B *body*.

    Returns ``None`` when *body* is not a recognizable prospectus (caller
    tombstones). A recognizable prospectus whose cover presentation can't be
    resolved yields a row with NULL money fields (valid outcome, not a
    tombstone).
    """
    if subtype not in IN_SCOPE_SUBTYPES:
        raise ValueError(f"subtype must be one of {sorted(IN_SCOPE_SUBTYPES)}, got {subtype!r}")
    text = _strip_html(body)
    low = text.lower()
    if "prospectus" not in low and "pricing supplement" not in low:
        return None

    security_type = _detect_security_type(text)

    cluster = _find_cover_cluster(text)
    if cluster is None:
        # Recognizable prospectus, no resolvable Item 501(b)(3) presentation
        # (non-cash / resale-shelf / free-form covers). Store the fact of the
        # offering; every derived field stays NULL.
        return ProspectusOffering(
            subtype=subtype,
            is_issuer_offering=None,
            price_per_unit=None,
            unit_label=None,
            aggregate_offering_amount=None,
            underwriting_discount=None,
            net_proceeds_to_issuer=None,
            proceeds_to_selling_holders=None,
            currency="USD",
            security_type=security_type,
        )

    price_m, uw_m, first_proceeds_m = cluster

    # Proceeds rows of THIS table (issuer and/or selling holders): chained by
    # row pitch so a second pricing table further down isn't swept in.
    cluster_end = price_m.start() + 1500
    proceeds_matches = [first_proceeds_m]
    while True:
        nxt = _PROCEEDS_LABEL_RE.search(
            text,
            proceeds_matches[-1].end(),
            min(proceeds_matches[-1].end() + _PROCEEDS_ROW_PITCH, cluster_end),
        )
        if nxt is None:
            break
        proceeds_matches.append(nxt)

    currency = _detect_currency(text[price_m.start() : cluster_end])
    unit_label = _unit_label_before(text, price_m.start())

    price_seg = text[price_m.end() : uw_m.start()]
    uw_seg = text[uw_m.end() : first_proceeds_m.start()]

    if not _segment_money(price_seg) and "%" not in price_seg and not _segment_money(uw_seg):
        # Column-major cover (labels first, value rows after — TD 424B3 shape).
        return _parse_column_major(
            text,
            subtype=subtype,
            proceeds_matches=proceeds_matches,
            cluster_end=cluster_end,
            currency=currency,
            security_type=security_type,
        )

    has_unit_header = unit_label is not None
    price_per_unit, aggregate = _row_values(price_seg, has_unit_header=has_unit_header)
    _, uw_total = _row_values(uw_seg, has_unit_header=has_unit_header)

    issuer_total: Decimal | None = None
    selling_total: Decimal | None = None
    saw_issuer_row = False
    saw_selling_row = False
    for i, pm in enumerate(proceeds_matches):
        seg_end = (
            proceeds_matches[i + 1].start()
            if i + 1 < len(proceeds_matches)
            else min(pm.end() + _LAST_SEGMENT_CAP, len(text))
        )
        seg = text[pm.end() : seg_end]
        _, total = _row_values(seg, has_unit_header=has_unit_header)
        if _classify_proceeds(text, pm):
            saw_selling_row = True
            if selling_total is None:
                selling_total = total
        else:
            saw_issuer_row = True
            if issuer_total is None:
                issuer_total = total

    is_issuer_offering: bool | None
    if saw_issuer_row:
        is_issuer_offering = True
    elif saw_selling_row:
        is_issuer_offering = False
    else:  # pragma: no cover — cluster requires ≥1 proceeds label
        is_issuer_offering = None

    return ProspectusOffering(
        subtype=subtype,
        is_issuer_offering=is_issuer_offering,
        price_per_unit=price_per_unit,
        unit_label=unit_label,
        aggregate_offering_amount=aggregate,
        underwriting_discount=uw_total,
        net_proceeds_to_issuer=issuer_total,
        proceeds_to_selling_holders=selling_total,
        currency=currency,
        security_type=security_type,
    )


def _parse_column_major(
    text: str,
    *,
    subtype: str,
    proceeds_matches: list[re.Match[str]],
    cluster_end: int,
    currency: str,
    security_type: str | None,
) -> ProspectusOffering:
    """Column-major cover: ``<labels...> Per Note $a $b $c Total $A $B $C``.

    Values align positionally to the label order (price, underwriting,
    proceeds...). Assign only when the count matches exactly; a mismatch means
    the physical table isn't the assumed shape → NULLs, never a guess.
    """
    n_labels = 2 + len(proceeds_matches)
    last_label_end = proceeds_matches[-1].end()
    region = text[last_label_end:cluster_end]

    per_unit_values: list[Decimal] = []
    total_values: list[Decimal] = []
    unit_label: str | None = None

    unit_m = _UNIT_LABEL_RE.search(region)
    if unit_m:
        unit_label = _normalize_unit(unit_m.group(1))
        total_m = _TOTAL_WORD_RE.search(region, unit_m.end())
        if total_m:
            per_unit_values = _segment_money(region[unit_m.end() : total_m.start()])
            total_values = _segment_money(region[total_m.end() : total_m.end() + _LAST_SEGMENT_CAP])[:n_labels]

    def _at(values: list[Decimal], idx: int) -> Decimal | None:
        return values[idx] if len(values) == n_labels else None

    issuer_total: Decimal | None = None
    selling_total: Decimal | None = None
    saw_issuer_row = False
    saw_selling_row = False
    for i, pm in enumerate(proceeds_matches):
        total = _at(total_values, 2 + i)
        if _classify_proceeds(text, pm):
            saw_selling_row = True
            if selling_total is None:
                selling_total = total
        else:
            saw_issuer_row = True
            if issuer_total is None:
                issuer_total = total

    return ProspectusOffering(
        subtype=subtype,
        is_issuer_offering=True if saw_issuer_row else (False if saw_selling_row else None),
        price_per_unit=_at(per_unit_values, 0),
        unit_label=unit_label,
        aggregate_offering_amount=_at(total_values, 0),
        underwriting_discount=_at(total_values, 1),
        net_proceeds_to_issuer=issuer_total,
        proceeds_to_selling_holders=selling_total,
        currency=currency,
        security_type=security_type,
    )


def upsert_prospectus_offering(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    offering: ProspectusOffering,
) -> None:
    """Upsert one parsed 424B cover into ``prospectus_offerings``."""
    conn.execute(
        """
        INSERT INTO prospectus_offerings (
            accession_number, instrument_id, subtype, is_issuer_offering,
            price_per_unit, unit_label, aggregate_offering_amount,
            underwriting_discount, net_proceeds_to_issuer,
            proceeds_to_selling_holders, currency, security_type,
            parser_version, parsed_at
        ) VALUES (
            %(accession)s, %(instrument_id)s, %(subtype)s, %(is_issuer)s,
            %(price_per_unit)s, %(unit_label)s, %(aggregate)s,
            %(uw_discount)s, %(net_proceeds)s,
            %(selling_proceeds)s, %(currency)s, %(security_type)s,
            %(parser_version)s, NOW()
        )
        ON CONFLICT (accession_number) DO UPDATE SET
            instrument_id = EXCLUDED.instrument_id,
            subtype = EXCLUDED.subtype,
            is_issuer_offering = EXCLUDED.is_issuer_offering,
            price_per_unit = EXCLUDED.price_per_unit,
            unit_label = EXCLUDED.unit_label,
            aggregate_offering_amount = EXCLUDED.aggregate_offering_amount,
            underwriting_discount = EXCLUDED.underwriting_discount,
            net_proceeds_to_issuer = EXCLUDED.net_proceeds_to_issuer,
            proceeds_to_selling_holders = EXCLUDED.proceeds_to_selling_holders,
            currency = EXCLUDED.currency,
            security_type = EXCLUDED.security_type,
            parser_version = EXCLUDED.parser_version,
            parsed_at = NOW()
        """,
        {
            "accession": accession_number,
            "instrument_id": instrument_id,
            "subtype": offering.subtype,
            "is_issuer": offering.is_issuer_offering,
            "price_per_unit": offering.price_per_unit,
            "unit_label": offering.unit_label,
            "aggregate": offering.aggregate_offering_amount,
            "uw_discount": offering.underwriting_discount,
            "net_proceeds": offering.net_proceeds_to_issuer,
            "selling_proceeds": offering.proceeds_to_selling_holders,
            "currency": offering.currency,
            "security_type": offering.security_type,
            "parser_version": PARSER_VERSION,
        },
    )
