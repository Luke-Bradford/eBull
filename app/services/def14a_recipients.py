"""#2351 slice 2 — which DEF 14A Item 403 rows a warrant / preferred sibling must not own.

Every DEF 14A writer fans an accession's Item 403 rows out to every instrument sharing
the issuer CIK. This module maintains ``def14a_recipient_suppressions``: the
(instrument, accession) pairs that readers exclude through the ``*_attributed`` views
(sql/420). The writers and the fact tables are untouched.

Spec: docs/proposals/ownership/2026-09-24-2351-def14a-class-recipients.md (slice 2).

Source rule. Item 403 reports beneficial ownership per class, determined under Rule
13d-3; Rule 13d-3(d)(1)(i) counts warrants into the UNDERLYING common class's figure,
so a parsed Item 403 figure is never a warrant instrument's own holding. Which class an
instrument is comes from the cover 12(b) table (``app.services.sec_cover_identity``).
The title classifier and the witness requirement are fixed by construction — no
published rule classifies a 12(b) title — and frozen by ``RECIPIENT_RULE_VERSION``.

Positive evidence only: a sibling is suppressed only when its OWN point-in-time cover
title is a warrant / preferred AND a different sibling's title on the same cover is
common. Absence, ambiguity, an unrecognised title and an unresolvable cover never
create a suppression; an unresolvable cover also never removes one.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Final, Literal

import httpx
import lxml.etree as ET
import psycopg

from app.services.def14a_drift import detect_drift
from app.services.ownership_observations import refresh_def14a_current, refresh_esop_current
from app.services.raw_filings import store_raw
from app.services.sec_cover_identity import cover_pairs, entity_ciks, parse_cover_contexts_root
from app.services.xbrl_instance import SAFE_XML_PARSER

logger = logging.getLogger(__name__)

RECIPIENT_RULE_VERSION: Final = 1
REASON_NON_COMMON_SIBLING: Final = "non_common_sibling"

COVER_FORMS: Final = ("10-K", "10-Q", "20-F")  # originals only
MAX_COVER_CANDIDATES: Final = 4
MAX_COVER_AGE: Final = timedelta(days=400)  # annual cover cycle plus slack

TitleKind = Literal["common", "non_common", "other"]

_BUNDLE = re.compile(r"\b(units?|rights?)\b", re.IGNORECASE)
_WARRANT = re.compile(r"\bwarrants?\b", re.IGNORECASE)
_EQUITY = re.compile(r"\b(preferred|preference|common|ordinary|capital\s+(?:stock|shares?))\b", re.IGNORECASE)


def title_kind(title: str) -> TitleKind:
    """Classify a 12(b) ``Security12bTitle`` (by construction, rule version 1).

    A unit or rights instrument is ``other`` — neither a clean witness nor a clean
    suppression. Any warrant is ``non_common`` (``Common Stock Purchase Warrants``
    names the underlying first). Otherwise the first equity word decides.
    """
    if _BUNDLE.search(title):
        return "other"
    if _WARRANT.search(title):
        return "non_common"
    m = _EQUITY.search(title)
    if m is None:
        return "other"
    return "non_common" if m.group(1).lower() in ("preferred", "preference") else "common"


def symbol_key(symbol: str) -> str:
    """eToro symbol → cover ``TradingSymbol`` key: trim, upper-case, drop a trailing ``.US``.

    No punctuation stripping, so ``ABC.D`` and ``ABCD`` never collide.
    """
    key = symbol.strip().upper()
    return key[:-3] if key.endswith(".US") else key


@dataclass(frozen=True)
class Sibling:
    instrument_id: int
    symbol: str


@dataclass(frozen=True)
class Cover:
    accession: str
    pairs: frozenset[tuple[str, str]]  # (title, symbol)


@dataclass(frozen=True)
class Suppression:
    instrument_id: int
    accession_number: str
    issuer_cik: str
    cover_accession: str
    cover_title: str
    cover_symbol: str
    witness_instrument_id: int
    witness_title: str
    rule_version: int = RECIPIENT_RULE_VERSION
    reason: str = REASON_NON_COMMON_SIBLING


def decide(*, accession_number: str, issuer_cik: str, siblings: Iterable[Sibling], cover: Cover) -> list[Suppression]:
    titles_by_symbol: dict[str, set[str]] = {}
    for title, symbol in cover.pairs:
        titles_by_symbol.setdefault(symbol, set()).add(title)

    matched: list[tuple[Sibling, str, str]] = []  # (sibling, key, sole title)
    for sib in sorted(siblings, key=lambda s: s.instrument_id):
        key = symbol_key(sib.symbol)
        titles = titles_by_symbol.get(key, set())
        if len(titles) == 1:
            matched.append((sib, key, next(iter(titles))))

    out: list[Suppression] = []
    for sib, key, title in matched:
        if title_kind(title) != "non_common":
            continue
        witness = next(
            ((w, wt) for w, wkey, wt in matched if wkey != key and title_kind(wt) == "common"),
            None,
        )
        if witness is None:
            continue
        out.append(
            Suppression(
                instrument_id=sib.instrument_id,
                accession_number=accession_number,
                issuer_cik=issuer_cik,
                cover_accession=cover.accession,
                cover_title=title,
                cover_symbol=key,
                witness_instrument_id=witness[0].instrument_id,
                witness_title=witness[1],
            )
        )
    return out


# ---------------------------------------------------------------------------
# Cover resolution
# ---------------------------------------------------------------------------

FetchText = Callable[[str], str | None]
"""``SecFilingsProvider.fetch_document_text``: body on 2xx, None on 404/410, raises otherwise."""


class CoverUnresolved(Exception):
    """A candidate cover could not be read for a transient reason. Never cached."""


def instance_url(primary_document_url: str) -> str | None:
    # An inline-XBRL filing's extracted instance sits beside the primary document
    # as ``<stem>_htm.xml``.
    if not primary_document_url.lower().endswith(".htm"):
        return None
    return primary_document_url[:-4] + "_htm.xml"


@dataclass(frozen=True)
class CachedCover:
    outcome: Literal["pairs", "no_pairs", "not_found"]
    entity_cik: str | None
    pairs: frozenset[tuple[str, str]]


def _read_cache(conn: psycopg.Connection[Any], cover_accession: str) -> CachedCover | None:
    row = conn.execute(
        "SELECT outcome, entity_cik FROM sec_cover_12b_fetches WHERE cover_accession = %s",
        (cover_accession,),
    ).fetchone()
    if row is None:
        return None
    pairs = frozenset(
        (str(t), str(s))
        for t, s in conn.execute(
            "SELECT security_title, trading_symbol FROM sec_cover_12b_pairs WHERE cover_accession = %s",
            (cover_accession,),
        ).fetchall()
    )
    return CachedCover(outcome=row[0], entity_cik=row[1], pairs=pairs)


def _write_cache(conn: psycopg.Connection[Any], cover_accession: str, cached: CachedCover) -> None:
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO sec_cover_12b_fetches (cover_accession, outcome, entity_cik)
            VALUES (%s, %s, %s) ON CONFLICT (cover_accession) DO NOTHING
            """,
            (cover_accession, cached.outcome, cached.entity_cik),
        )
        for title, symbol in sorted(cached.pairs):
            conn.execute(
                """
                INSERT INTO sec_cover_12b_pairs (cover_accession, security_title, trading_symbol)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """,
                (cover_accession, title, symbol),
            )


def parse_cover_instance(body: str) -> CachedCover:
    """Parse a fetched instance. Raises ``CoverUnresolved`` on a non-XBRL / broken body."""
    try:
        root = ET.fromstring(body.encode("utf-8"), parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError as exc:
        raise CoverUnresolved(f"xml parse error: {exc}") from exc
    if not isinstance(root.tag, str) or ET.QName(root.tag).localname != "xbrl":
        raise CoverUnresolved("root element is not xbrl")
    ciks = entity_ciks(root)
    pairs = cover_pairs(parse_cover_contexts_root(root, require_dei=True))
    return CachedCover(
        outcome="pairs" if pairs else "no_pairs",
        entity_cik=next(iter(ciks)) if len(ciks) == 1 else None,
        pairs=pairs,
    )


def load_cover(
    conn: psycopg.Connection[Any], fetch_text: FetchText, *, cover_accession: str, primary_document_url: str | None
) -> tuple[CachedCover, bool]:
    """Cached or freshly fetched terminal outcome for one candidate. ``(cover, fetched)``."""
    cached = _read_cache(conn, cover_accession)
    if cached is not None:
        return cached, False
    url = instance_url(primary_document_url) if primary_document_url else None
    if url is None:
        cached = CachedCover(outcome="not_found", entity_cik=None, pairs=frozenset())
        _write_cache(conn, cover_accession, cached)
        return cached, False
    try:
        body = fetch_text(url)
    except httpx.HTTPError as exc:  # transport error, 3xx/401/403/429/5xx (raise_for_status)
        raise CoverUnresolved(f"fetch {url}: {exc}") from exc
    if body is None:  # 404 / 410
        cached = CachedCover(outcome="not_found", entity_cik=None, pairs=frozenset())
    else:
        # Raw before parse; committed on its own (the job connection is autocommit).
        store_raw(
            conn,
            accession_number=cover_accession,
            document_kind="xbrl_cover_instance",
            payload=body,
            source_url=url,
        )
        cached = parse_cover_instance(body)
    _write_cache(conn, cover_accession, cached)
    return cached, True


@dataclass
class CoverResolution:
    cover: Cover | None = None
    unresolved: bool = False
    fetches: int = 0


def resolve_cover(
    conn: psycopg.Connection[Any],
    fetch_text: FetchText,
    *,
    issuer_cik: str,
    sibling_ids: list[int],
    proxy_date: date,
) -> CoverResolution:
    """The newest usable cover filed strictly before the proxy, within the age bound."""
    candidates = conn.execute(
        """
        SELECT DISTINCT ON (provider_filing_id) provider_filing_id, filing_date, primary_document_url
          FROM filing_events
         WHERE provider = 'sec'
           AND instrument_id = ANY(%(iids)s)
           AND filing_type = ANY(%(forms)s)
           AND filing_date < %(d)s
           AND filing_date >= %(floor)s
         ORDER BY provider_filing_id, filing_event_id
        """,
        {"iids": sibling_ids, "forms": list(COVER_FORMS), "d": proxy_date, "floor": proxy_date - MAX_COVER_AGE},
    ).fetchall()
    candidates.sort(key=lambda r: (r[1], r[0]), reverse=True)
    res = CoverResolution()
    for accession, _filed, url in candidates[:MAX_COVER_CANDIDATES]:
        try:
            cached, fetched = load_cover(conn, fetch_text, cover_accession=accession, primary_document_url=url)
        except CoverUnresolved as exc:
            logger.warning("def14a_recipients: cover %s unresolved: %s", accession, exc)
            res.unresolved = True
            return res
        res.fetches += int(fetched)
        if cached.outcome != "pairs" or cached.entity_cik != issuer_cik:
            continue
        res.cover = Cover(accession=accession, pairs=cached.pairs)
        return res
    return res


# ---------------------------------------------------------------------------
# Job body — full recompute, diff, apply
# ---------------------------------------------------------------------------


@dataclass
class RunReport:
    accessions: int = 0
    accessions_unresolved: int = 0
    accessions_no_date: int = 0
    fetches: int = 0
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    instruments_refreshed: int = 0
    instruments_failed: list[int] = field(default_factory=list)


def load_population(conn: psycopg.Connection[Any]) -> dict[str, list[Sibling]]:
    """Issuer CIK → siblings, for CIKs backing >1 distinct instrument.

    An instrument carrying more than one sec CIK is excluded from every group (it can
    be neither suppressed nor a witness).
    """
    rows = conn.execute(
        """
        WITH single_cik AS (
            SELECT instrument_id, min(identifier_value) AS cik
              FROM external_identifiers
             WHERE provider = 'sec' AND identifier_type = 'cik'
             GROUP BY instrument_id
            HAVING count(DISTINCT identifier_value) = 1),
        multi AS (
            SELECT cik FROM single_cik GROUP BY cik HAVING count(*) > 1)
        SELECT s.cik, i.instrument_id, i.symbol
          FROM single_cik s
          JOIN multi m USING (cik)
          JOIN instruments i ON i.instrument_id = s.instrument_id
         ORDER BY s.cik, i.instrument_id
        """
    ).fetchall()
    out: dict[str, list[Sibling]] = {}
    for cik, iid, symbol in rows:
        out.setdefault(str(cik), []).append(Sibling(instrument_id=int(iid), symbol=str(symbol)))
    return out


def _accessions_with_dates(conn: psycopg.Connection[Any], sibling_ids: list[int]) -> list[tuple[str, date | None]]:
    return [
        (str(acc), filed)
        for acc, filed in conn.execute(
            """
            WITH acc AS (
                SELECT accession_number AS a FROM def14a_beneficial_holdings WHERE instrument_id = ANY(%(iids)s)
                UNION
                SELECT source_accession FROM ownership_def14a_observations WHERE instrument_id = ANY(%(iids)s)
                UNION
                SELECT source_accession FROM ownership_esop_observations WHERE instrument_id = ANY(%(iids)s))
            SELECT acc.a,
                   (SELECT min(f.filing_date) FROM filing_events f
                     WHERE f.provider = 'sec' AND f.provider_filing_id = acc.a)
              FROM acc
             WHERE acc.a IS NOT NULL
             ORDER BY acc.a
            """,
            {"iids": sibling_ids},
        ).fetchall()
    ]


# Column order of the ledger SELECT / INSERT below; must match both.
_LEDGER_COLS: Final = (
    "instrument_id",
    "accession_number",
    "issuer_cik",
    "reason",
    "rule_version",
    "cover_accession",
    "cover_title",
    "cover_symbol",
    "witness_instrument_id",
    "witness_title",
)


def _row(s: Suppression) -> tuple[Any, ...]:
    return tuple(getattr(s, c) for c in _LEDGER_COLS)


def compute_desired(
    conn: psycopg.Connection[Any], fetch_text: FetchText, report: RunReport
) -> tuple[dict[tuple[int, str], tuple[Any, ...]], set[str]]:
    """Desired ledger rows, and the accessions whose stored rows must be kept as-is."""
    desired: dict[tuple[int, str], tuple[Any, ...]] = {}
    keep: set[str] = set()
    for cik, siblings in load_population(conn).items():
        ids = [s.instrument_id for s in siblings]
        for accession, proxy_date in _accessions_with_dates(conn, ids):
            report.accessions += 1
            if proxy_date is None:
                report.accessions_no_date += 1
                keep.add(accession)
                continue
            res = resolve_cover(conn, fetch_text, issuer_cik=cik, sibling_ids=ids, proxy_date=proxy_date)
            report.fetches += res.fetches
            if res.unresolved:
                report.accessions_unresolved += 1
                keep.add(accession)
                continue
            if res.cover is None:
                continue
            for s in decide(accession_number=accession, issuer_cik=cik, siblings=siblings, cover=res.cover):
                desired[(s.instrument_id, s.accession_number)] = _row(s)
    return desired, keep


def _apply_instrument(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    *,
    upserts: list[tuple[Any, ...]],
    deletes: list[str],
) -> None:
    with conn.transaction():
        for accession in deletes:
            conn.execute(
                "DELETE FROM def14a_recipient_suppressions WHERE instrument_id = %s AND accession_number = %s",
                (instrument_id, accession),
            )
        for row in upserts:
            conn.execute(
                """
                INSERT INTO def14a_recipient_suppressions (
                    instrument_id, accession_number, issuer_cik, reason, rule_version,
                    cover_accession, cover_title, cover_symbol, witness_instrument_id, witness_title)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (instrument_id, accession_number) DO UPDATE SET
                    issuer_cik = EXCLUDED.issuer_cik,
                    reason = EXCLUDED.reason,
                    rule_version = EXCLUDED.rule_version,
                    cover_accession = EXCLUDED.cover_accession,
                    cover_title = EXCLUDED.cover_title,
                    cover_symbol = EXCLUDED.cover_symbol,
                    witness_instrument_id = EXCLUDED.witness_instrument_id,
                    witness_title = EXCLUDED.witness_title,
                    created_at = now()
                """,
                row,
            )
        refresh_def14a_current(conn, instrument_id=instrument_id)
        refresh_esop_current(conn, instrument_id=instrument_id)
        detect_drift(conn, instrument_id=instrument_id)


def run_recipient_suppressions(conn: psycopg.Connection[Any], fetch_text: FetchText) -> RunReport:
    """Recompute every suppression, diff against the ledger, apply per instrument.

    ``conn`` must be autocommit: each cover cache write, raw store and per-instrument
    apply commits on its own.
    """
    report = RunReport()
    desired, keep = compute_desired(conn, fetch_text, report)
    stored = {
        (int(r[0]), str(r[1])): tuple(r)
        for r in conn.execute(
            """
            SELECT instrument_id, accession_number, issuer_cik, reason, rule_version,
                   cover_accession, cover_title, cover_symbol, witness_instrument_id, witness_title
              FROM def14a_recipient_suppressions
            """
        ).fetchall()
    }

    per_instrument: dict[int, tuple[list[tuple[Any, ...]], list[str]]] = {}
    for key, row in desired.items():
        if stored.get(key) != row:
            per_instrument.setdefault(key[0], ([], []))[0].append(row)
            if key in stored:
                report.updated += 1
            else:
                report.inserted += 1
    for key in stored:
        if key not in desired and key[1] not in keep:
            per_instrument.setdefault(key[0], ([], []))[1].append(key[1])
            report.deleted += 1

    for instrument_id, (upserts, deletes) in sorted(per_instrument.items()):
        try:
            _apply_instrument(conn, instrument_id, upserts=upserts, deletes=deletes)
            report.instruments_refreshed += 1
        except Exception:
            logger.exception("def14a_recipients: apply failed for instrument %d", instrument_id)
            report.instruments_failed.append(instrument_id)
    return report


def run_with_sec_provider(conn: psycopg.Connection[Any]) -> RunReport:
    """Job entry: the shared SEC client (and its rate gate) supplies cover instances."""
    from app.config import settings
    from app.providers.implementations.sec_edgar import SecFilingsProvider

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        return run_recipient_suppressions(conn, provider.fetch_document_text)
