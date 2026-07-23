"""DRS / registered-share split extraction (#844 PR-2).

Reg S-K Item 201(b)(1) mandates only the holders-of-record COUNT in 10-K
Item 5; the registered-vs-street SHARE split is voluntary narrative with no
XBRL tag and no mandated location (spec
docs/specs/etl/2026-07-23-drs-rsu-issuer-disclosures.md). Extraction is
therefore issuer-disclosed-only over a curated CIK allowlist, searching the
whole normalized primary doc (never an Item-5 anchor), fail-open: no match →
no row, never blocks the filing's other parsers.

Corpus (34 era-filings scanned 2026-07-23): GME disclosing 15/15 since the
FY2022 10-K (2023-03); AMC 3/3 since the 2025-11 10-Q, zero before. Two
sentence shapes (registered-first / Cede-first), "million" suffix sometimes
omitted (AMC), holder count inline (AMC) or in a separate record-holders
sentence (GME 10-Ks).

Normalization heals the two corpus landmines: decimal-split ("382.4"
severed by sentence-splitting — we regex whole-text windows instead) and
iXBRL word fragmentation ("approxim ately" — inline tags strip to ''
while block tags become ' ').
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final, Protocol

import psycopg
from psycopg.rows import dict_row

from app.services.sec_identity import siblings_for_issuer_cik

logger = logging.getLogger(__name__)

PARSER_VERSION: Final[str] = "drs_disclosure_v1"

# Curated cohort — single source of truth (spec "DRS — curated text").
# Zero-padded 10-digit issuer CIKs.
DRS_DISCLOSURE_CIKS: Final[frozenset[str]] = frozenset(
    {
        "0001326380",  # GME — discloses every 10-K/10-Q since 2023-03
        "0001411579",  # AMC — discloses since the 2025-11 10-Q
    }
)

# --- HTML → text normalization ---------------------------------------------

# Inline elements strip to '' (iXBRL wraps mid-word spans — replacing with a
# space fragments words: "approxim ately"); block elements become ' '.
_INLINE_TAG = re.compile(
    r"</?(?:span|ix:[a-z0-9-]+|a|b|i|u|em|strong|font|sup|sub)\b[^>]*>",
    re.IGNORECASE,
)
_ANY_TAG = re.compile(r"<[^>]+>")
_ENTITY = re.compile(r"&(?:#\d+|#x[0-9a-fA-F]+|[a-zA-Z]+);")

_ENTITY_MAP: Final[dict[str, str]] = {
    "&amp;": "&",
    "&nbsp;": " ",
    "&#160;": " ",
    "&#8217;": "'",
    "&#8220;": '"',
    "&#8221;": '"',
}


def normalize_filing_text(html: str) -> str:
    """Strip a filing's HTML to searchable text: inline tags → '' (heals
    iXBRL word fragmentation), remaining (block) tags → ' ', entities
    mapped or spaced, whitespace collapsed."""
    text = _INLINE_TAG.sub("", html)
    text = _ANY_TAG.sub(" ", text)
    text = _ENTITY.sub(lambda m: _ENTITY_MAP.get(m.group(0), " "), text)
    return re.sub(r"\s+", " ", text)


# --- extraction -------------------------------------------------------------


@dataclass(frozen=True)
class DrsDisclosure:
    """One filing's registered-vs-street split. ``as_of_date`` is None when
    the sentence carries no date (AMC dates the outstanding sentence, not
    the split) — the reader then falls back to filed_at."""

    registered_shares: Decimal
    registered_pct: Decimal | None
    street_shares: Decimal | None
    street_pct: Decimal | None
    holders_of_record: int | None
    as_of_date: date | None


# Corpus shapes: "registered holders with our transfer agent" (GME 2023-06+,
# AMC) and the GME FY2022-10-K variant "were held by record holders" (guarded
# by Cede-in-window — the mandated Item 201(b) holders-COUNT sentence alone
# must never anchor an extraction).
_ANCHOR = re.compile(
    r"registered holders? with (?:our|the Company's) transfer agent|held by record holders",
    re.IGNORECASE,
)

# A share quantity: "382.4 million", "529,547,465", "527.5" (AMC omits
# "million"; disclosed splits are never <10k shares absolute, so bare small
# numbers scale by 1e6 — the cross-side sanity gate then validates).
_NUM = r"([\d][\d,]*(?:\.\d+)?)"
_UNIT = r"\s*(million|billion)?"
_PCT = r"\(\s*(?:or\s+)?(?:approximately\s+)?([\d.]+)\s*%[^)]*\)"

# registered-side: "approximately 66.2 million shares (15%) were held by
# [14,021 ]registered holders with our transfer agent"
_REGISTERED = re.compile(
    rf"{_NUM}{_UNIT}\s*(?:shares?)?[^.()]{{0,60}}?(?:{_PCT})?[^.()]{{0,60}}?"
    rf"were held by\s+(?:([\d,]+)\s+)?"
    rf"(?:registered holders? with (?:our|the Company's) transfer agent|record holders)",
    re.IGNORECASE,
)
# street-side: "approximately 382.4 million shares (85%) were held by Cede" /
# GME FY2022: "…shares of our Class A Common Stock held by Cede" (no "were").
_STREET = re.compile(
    rf"{_NUM}{_UNIT}\s*(?:shares?)?[^.()]{{0,80}}?(?:{_PCT})?[^.()]{{0,80}}?(?:were\s+)?held by\s+Cede",
    re.IGNORECASE,
)
_RECORD_HOLDERS = re.compile(
    r"(?:there were|held by)\s+([\d,]+)\s+(?:record holders?|registered holders?)",
    re.IGNORECASE,
)
# IGNORECASE: 10-Ks open the sentence with "As of March 18, 2026, …".
_AS_OF = re.compile(r"as of\s+([A-Z][a-z]+ \d{1,2}, \d{4})", re.IGNORECASE)

_WINDOW = 1200  # chars either side of the anchor


def _parse_quantity(num: str, unit: str | None) -> Decimal:
    value = Decimal(num.replace(",", ""))
    if unit:
        value *= Decimal(1_000_000_000) if unit.lower() == "billion" else Decimal(1_000_000)
    elif value < 10_000:
        # AMC's "approximately 527.5 (or 99.6%)" — "million" omitted. A real
        # split share count is never < 10k absolute.
        value *= Decimal(1_000_000)
    return value


def _parse_pct(raw: str | None) -> Decimal | None:
    if raw is None:
        return None
    try:
        return Decimal(raw)
    except ArithmeticError:
        return None


def extract_drs_disclosure(text: str) -> DrsDisclosure | None:
    """Extract the registered/street split from normalized filing text, or
    None. Sanity (spec): when both sides parse, registered + street must
    reconcile — each side's pct within 3pp of its implied share — else drop
    (log at the caller, never guess)."""
    anchor = _ANCHOR.search(text)
    if anchor is None:
        return None
    lo = max(0, anchor.start() - _WINDOW)
    hi = min(len(text), anchor.end() + _WINDOW)
    window = text[lo:hi]
    wlower = window.lower()
    if "transfer agent" not in wlower and "cede" not in wlower:
        # The record-holders anchor variant matched the mandated Item 201(b)
        # holders-COUNT sentence with no split context — not a disclosure.
        return None

    reg = _REGISTERED.search(window)
    if reg is None:
        return None
    registered = _parse_quantity(reg.group(1), reg.group(2))
    registered_pct = _parse_pct(reg.group(3))
    holders: int | None = None
    if reg.group(4):
        holders = int(reg.group(4).replace(",", ""))

    street = _STREET.search(window)
    street_shares: Decimal | None = None
    street_pct: Decimal | None = None
    if street is not None:
        street_shares = _parse_quantity(street.group(1), street.group(2))
        street_pct = _parse_pct(street.group(3))

    # Scope date/holders to the registered-sentence NEIGHBOURHOOD, not the
    # whole window/filing — an unrelated earlier "As of …" or holders count
    # in nearby Item 201/business text must not date the split (codex
    # ckpt-2 finding 2; as_of drives the 400d staleness policy).
    neighbourhood = window[max(0, reg.start() - 400) : min(len(window), reg.end() + 500)]
    if holders is None:
        rh = _RECORD_HOLDERS.search(neighbourhood)
        if rh is not None:
            holders = int(rh.group(1).replace(",", ""))

    as_of: date | None = None
    m = _AS_OF.search(neighbourhood)
    if m is not None:
        try:
            as_of = datetime.strptime(m.group(1), "%B %d, %Y").date()
        except ValueError:
            as_of = None

    # Cross-side sanity: both sides + both pcts present → each side's pct
    # must be within 3pp of its implied share of the combined total.
    if street_shares is not None and registered_pct is not None and street_pct is not None:
        total = registered + street_shares
        if total > 0:
            implied_reg = registered / total * 100
            implied_street = street_shares / total * 100
            if abs(implied_reg - registered_pct) > 3 or abs(implied_street - street_pct) > 3:
                return None

    return DrsDisclosure(
        registered_shares=registered,
        registered_pct=registered_pct,
        street_shares=street_shares,
        street_pct=street_pct,
        holders_of_record=holders,
        as_of_date=as_of,
    )


# --- persistence ------------------------------------------------------------


def upsert_drs_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source_accession: str,
    form_type: str,
    filed_at: datetime,
    disclosure: DrsDisclosure,
) -> None:
    """Idempotent per (instrument, accession); re-parse updates in place
    (parser-version bump semantics match the other observation writers)."""
    conn.execute(
        """
        INSERT INTO ownership_drs_observations (
            instrument_id, source_accession, form_type, filed_at, as_of_date,
            registered_shares, registered_pct, street_shares, street_pct,
            holders_of_record, parser_version
        ) VALUES (
            %(iid)s, %(acc)s, %(form)s, %(filed)s, %(as_of)s,
            %(reg)s, %(reg_pct)s, %(street)s, %(street_pct)s,
            %(holders)s, %(pv)s
        )
        ON CONFLICT (instrument_id, source_accession) DO UPDATE SET
            form_type = EXCLUDED.form_type,
            filed_at = EXCLUDED.filed_at,
            as_of_date = EXCLUDED.as_of_date,
            registered_shares = EXCLUDED.registered_shares,
            registered_pct = EXCLUDED.registered_pct,
            street_shares = EXCLUDED.street_shares,
            street_pct = EXCLUDED.street_pct,
            holders_of_record = EXCLUDED.holders_of_record,
            parser_version = EXCLUDED.parser_version,
            fetched_at = NOW()
        """,
        {
            "iid": instrument_id,
            "acc": source_accession,
            "form": form_type,
            "filed": filed_at,
            "as_of": disclosure.as_of_date,
            "reg": disclosure.registered_shares,
            "reg_pct": disclosure.registered_pct,
            "street": disclosure.street_shares,
            "street_pct": disclosure.street_pct,
            "holders": disclosure.holders_of_record,
            "pv": PARSER_VERSION,
        },
    )


class _DocumentTextFetcher(Protocol):
    """The slice of ``SecFilingsProvider`` this refresher needs (#453
    fetch_document_text writer-discipline contract — every body routes
    through the SQL upsert below, never to disk)."""

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


@dataclass
class DrsRefreshResult:
    filings_examined: int = 0
    filings_extracted: int = 0
    filings_no_disclosure: int = 0
    fetch_failures: int = 0


def refresh_drs_disclosures(
    conn: psycopg.Connection[Any],
    provider: _DocumentTextFetcher,
    *,
    since: date | None = None,
) -> DrsRefreshResult:
    """Weekly refresher + backfill (one code path): for each allowlisted
    CIK, parse manifest 10-K/10-K/A/10-Q filings newer than the CIK's
    newest stored observation (monotone frontier — non-disclosing filings
    ahead of the frontier cost at most one re-fetch per run; disclosure is
    continuous once an issuer starts, per the corpus). ``since`` overrides
    the frontier for explicit backfill (e.g. the 2023-01-01 era floor).

    NOT a manifest parser by design: the sec_10q synth no-op is a defended
    decision (3 Codex BLOCKINGs against fetch designs there) and a
    parser-version bump would rescope the whole 10-K lane for a 2-issuer
    feature. Fail-open per filing; never raises past a single filing.
    """
    result = DrsRefreshResult()
    for cik in sorted(DRS_DISCLOSURE_CIKS):
        siblings = siblings_for_issuer_cik(conn, cik)
        if not siblings:
            continue  # no resolved instruments — skip before the filings query
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT m.accession_number, m.form, m.filed_at, m.primary_document_url
                FROM sec_filing_manifest m
                WHERE m.cik = %(cik)s
                  AND m.form IN ('10-K', '10-K/A', '10-Q')
                  AND m.primary_document_url IS NOT NULL
                  AND m.filed_at > COALESCE(
                        %(since)s::timestamptz,
                        (
                            SELECT MAX(o.filed_at)
                              FROM ownership_drs_observations o
                              JOIN external_identifiers e
                                ON e.instrument_id = o.instrument_id
                               AND e.provider = 'sec' AND e.identifier_type = 'cik'
                             WHERE e.identifier_value = %(cik)s
                        ),
                        '0001-01-01'::timestamptz
                      )
                ORDER BY m.filed_at
                """,
                {"cik": cik, "since": since},
            )
            filings = cur.fetchall()
        for f in filings:
            result.filings_examined += 1
            try:
                html = provider.fetch_document_text(f["primary_document_url"])
            except Exception:  # noqa: BLE001 — one filing must not sink the run
                logger.warning(
                    "drs_disclosure: fetch failed cik=%s accession=%s",
                    cik,
                    f["accession_number"],
                    exc_info=True,
                )
                result.fetch_failures += 1
                continue
            if not html:
                result.fetch_failures += 1
                continue
            disclosure = extract_drs_disclosure(normalize_filing_text(html))
            if disclosure is None:
                logger.info(
                    "drs_disclosure: cik=%s accession=%s — no extractable split (fail-open)",
                    cik,
                    f["accession_number"],
                )
                result.filings_no_disclosure += 1
                continue
            with conn.transaction():
                for iid in siblings:
                    upsert_drs_observation(
                        conn,
                        instrument_id=iid,
                        source_accession=f["accession_number"],
                        form_type=f["form"],
                        filed_at=f["filed_at"],
                        disclosure=disclosure,
                    )
            result.filings_extracted += 1
    logger.info(
        "drs_disclosure refresh: examined=%d extracted=%d no_disclosure=%d fetch_failures=%d",
        result.filings_examined,
        result.filings_extracted,
        result.filings_no_disclosure,
        result.fetch_failures,
    )
    return result
