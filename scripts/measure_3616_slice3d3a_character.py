"""#3616 slice 3d-3a fact-finding: the tax character each security of the effective matching run would get.

Read-only. Resolves the effective matching run with ``effective_tax_matching`` (3d-2 Design 9). Then, in one
REPEATABLE READ transaction on the dev DB, reads that run's rows (sealed, so immutable): securities, disposals,
identifications, dependencies, pools, placements and each security's first admitted acquisition date; and the inputs
Design 2 names: per member its symbol, ``instrument_type_id``, primary SEC CIK, symbol history and SEC profile
(``instrument_sec_profile``), the members' SEC 10-K / 10-Q / 8-K rows of ``filing_events``, and the stored cover
instance (``filing_raw_documents``, kind ``xbrl_cover_instance``) of only the two covers Design 2 selects. In the same
snapshot it checks that ``instruments`` still matches the run's sealed mapping and that no unmapped row now points at,
or is the ``.RTH`` variant of, a member; after it, that the effective run is unchanged. A selected cover with no
stored instance is fetched (SEC's extracted instance beside the primary document, ``<stem>_htm.xml`` via
``def14a_recipients.instance_url``; User-Agent ``settings.sec_user_agent``; abandoned past 16 MiB) and not stored.
Each instance is read by ``read_cover``, the spec's strict reader of ``dei:EntityCentralIndexKey``,
``dei:Security12bTitle`` and ``dei:TradingSymbol``, and ``classify`` applies the ``tax_character_v1`` table. It prints
per security every input, the character and the run's figures. Fetched bodies go to a new directory under
``var/research/3616/slice3d3a/`` with their SHA-256; the printout carries dates, amounts, symbols, CIKs and accession
numbers, never an account identifier.

``--universe ARCHIVE`` applies the profile lines (entity type, SIC) to every Stocks instrument with a primary SEC CIK
(tradable or not, variants included), reading those two fields from a local copy of SEC's nightly
``submissions.zip``, and counts outcomes with the annual-report forms of the ``other`` ones. It does not read covers,
so it measures coverage of those lines only.

    uv run python -m scripts.measure_3616_slice3d3a_character
    uv run python -m scripts.measure_3616_slice3d3a_character --universe <submissions.zip>
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import sys
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import lxml.etree as ET
import psycopg

from app.config import settings
from app.services.def14a_recipients import instance_url, symbol_key
from app.services.raw_filings import stored_body
from app.services.tax_matching import effective_tax_matching
from app.services.xbrl_instance import SAFE_XML_PARSER

INSTANCE_MAX_BYTES = 16 * 1024 * 1024
PROFILE_MAX_AGE = timedelta(days=30)
STOCKS_TYPE_ID = 5
ETF_TYPE_ID = 6
INVESTMENT_SIC = frozenset({"6722", "6726", "6770", "6799"})
COVER_FORMS = ("10-K", "10-Q", "8-K")
_SIC = re.compile(r"[0-9]{4}")
_COMMON = re.compile(
    r"(CLASS [A-Z] )?(COMMON STOCK|ORDINARY SHARES)"
    r"(,? ((US)?\$?[0-9]*\.?[0-9]+ PAR VALUE|PAR VALUE (US)?\$?[0-9]*\.?[0-9]+|NO PAR VALUE)( PER SHARE)?)?"
)


@dataclass(frozen=True)
class Profile:
    entity_type: str | None
    sic: str | None
    fetched_at: datetime


@dataclass(frozen=True)
class CoverFiling:
    accession: str
    form: str
    filed_on: date
    primary_document_url: str | None


XBRLI = "http://www.xbrl.org/2003/instance"
SEC_CIK_SCHEME = "http://www.sec.gov/CIK"
_DEI = re.compile(r"http://xbrl\.sec\.gov/dei/[0-9]{4}")


@dataclass(frozen=True)
class Cover:
    """A strictly read cover instance: its registrant CIKs, unambiguous (title, symbol) pairs, ambiguous symbols."""

    entity_ciks: frozenset[str]
    pairs: frozenset[tuple[str, str]]
    ambiguous: frozenset[str]


def read_cover(body: bytes) -> Cover | None:
    """Strict tax reader of SEC's extracted instance; None means unusable (over ``INSTANCE_MAX_BYTES``, not XBRL, a
    context without a unique non-empty id, a ``dei`` fact without a ``contextRef`` naming one, or a non-digit
    CIK). A symbol whose context has several titles or symbols, or that two contexts give different titles, is
    ambiguous; a context with a title and no symbol is ignored (untraded notes)."""
    if len(body) > INSTANCE_MAX_BYTES:
        return None
    try:
        root = ET.fromstring(body, parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError:
        return None
    if not isinstance(root.tag, str) or ET.QName(root).text != f"{{{XBRLI}}}xbrl":
        return None
    ids = [el.get("id") or "" for el in root.iter(f"{{{XBRLI}}}context")]
    if "" in ids or len(set(ids)) != len(ids):
        return None
    issuer: dict[str, str | None] = {}
    for ctx in root.iter(f"{{{XBRLI}}}context"):
        idents = list(ctx.iter(f"{{{XBRLI}}}identifier"))
        text = (idents[0].text or "").strip() if len(idents) == 1 else ""
        sec = len(idents) == 1 and idents[0].get("scheme") == SEC_CIK_SCHEME and text.isdigit()
        issuer[str(ctx.get("id"))] = text.zfill(10) if sec else None
    contexts = set(ids)
    ciks: set[str] = set()
    by_ctx: dict[str, dict[str, set[str]]] = {}
    for el in root.iter():
        if not isinstance(el.tag, str):
            continue
        q = ET.QName(el)
        if not (q.namespace and _DEI.fullmatch(q.namespace)):
            continue
        if q.localname not in ("EntityCentralIndexKey", "Security12bTitle", "TradingSymbol"):
            continue
        ref = el.get("contextRef") or ""
        if ref not in contexts or issuer[ref] is None:
            return None
        ciks.add(str(issuer[ref]))
        text = " ".join("".join(el.itertext()).split())
        if q.localname == "EntityCentralIndexKey":
            if not text.isdigit():
                return None
            ciks.add(text.zfill(10))
        else:
            by_ctx.setdefault(ref, {}).setdefault(q.localname, set()).add(text)
    titles_of: dict[str, set[str]] = {}
    ambiguous: set[str] = set()
    for facts in by_ctx.values():
        symbols = {symbol_key(x) for x in facts.get("TradingSymbol", set()) if x}
        titles = facts.get("Security12bTitle", set())
        if len(symbols) == 1 and len(titles) == 1:
            titles_of.setdefault(next(iter(symbols)), set()).update(titles)
        else:
            ambiguous |= symbols
    ambiguous |= {sym for sym, ts in titles_of.items() if len(ts) > 1}
    pairs = frozenset((next(iter(ts)), sym) for sym, ts in titles_of.items() if sym not in ambiguous)
    return Cover(frozenset(ciks), pairs, frozenset(ambiguous))


def common_title(title: str) -> bool:
    return _COMMON.fullmatch(" ".join(title.split()).upper()) is not None


def consolidate(filings: list[CoverFiling]) -> list[CoverFiling] | None:
    """One filing per accession, newest first by (date, accession); None when one accession carries two dates."""
    by_acc: dict[str, CoverFiling] = {}
    for f in filings:
        seen = by_acc.get(f.accession)
        if seen is not None and seen.filed_on != f.filed_on:
            return None
        if seen is None or (f.primary_document_url or "") < (seen.primary_document_url or ""):
            by_acc[f.accession] = f
    return sorted(by_acc.values(), key=lambda f: (f.filed_on, f.accession), reverse=True)


def classify(
    *,
    types: set[int | None],
    ciks: set[str],
    symbol: str | None,
    symbols_ever: set[str],
    profiles: list[Profile | None],
    as_of: datetime,
    filings: list[CoverFiling],
    covers: dict[str, Cover | None],
    first_acq: date | None,
) -> tuple[str, str | None]:
    """``tax_character_v1``: (character, detail), detail None for a share. ``profiles``: one per member carrying the
    CIK (None: no row). ``covers``: accession → strictly read cover (None: unusable); absent when no body is stored."""
    if len(types) != 1 or len(ciks) > 1:
        return "unestablished", "member_conflict"
    (kind,) = types
    if kind == ETF_TYPE_ID:
        return "unestablished", "exchange_traded_fund"
    if kind != STOCKS_TYPE_ID:
        return "unestablished", "instrument_type"
    if not ciks or symbol is None:
        return "unestablished", "cik_unestablished"
    (cik,) = ciks
    if not profiles or any(p is None or not (as_of - PROFILE_MAX_AGE <= p.fetched_at <= as_of) for p in profiles):
        return "unestablished", "sec_profile_unavailable"
    facts = {(p.entity_type, p.sic) for p in profiles if p is not None}
    if len(facts) != 1:
        return "unestablished", "sec_profile_conflict"
    ((entity_type, sic),) = facts
    if entity_type != "operating":
        return "unestablished", "entity_type"
    if sic is None or not _SIC.fullmatch(sic) or sic in INVESTMENT_SIC:
        return "unestablished", "sic"
    ordered = consolidate(filings)
    if ordered is None:
        return "unestablished", "cover_metadata_conflict"

    def read(filing: CoverFiling, prefix: str) -> Cover | str:
        if filing.accession not in covers:
            return f"{prefix}cover_unavailable"
        got = covers[filing.accession]
        if got is None or got.entity_ciks != {cik}:
            return f"{prefix}cover_unusable"
        return got

    if not ordered:
        return "unestablished", "cover_unavailable"
    latest = read(ordered[0], "")
    if isinstance(latest, str):
        return "unestablished", latest
    listed = [t for t, s in latest.pairs if s == symbol]
    if symbol in latest.ambiguous or len(listed) != 1 or not common_title(listed[0]):
        return "unestablished", "listed_class_not_common_stock"
    if first_acq is not None:
        start = next((f for f in ordered if f.filed_on <= first_acq), None)
        if start is None:
            return "unestablished", "holding_start_cover_unavailable"
        then = read(start, "holding_start_")
        if isinstance(then, str):
            return "unestablished", then
        bound = [t for t, s in then.pairs if s in symbols_ever]
        if then.ambiguous & symbols_ever or len(bound) != 1 or not common_title(bound[0]):
            return "unestablished", "holding_start_class_not_common_stock"
    return "share", None


def _fetch_instance(client: httpx.Client, url: str, out: Path, accession: str) -> bytes | None:
    """The instance body, refused while reading past ``INSTANCE_MAX_BYTES``; None on any non-200 or refusal."""
    with client.stream("GET", url, headers={"User-Agent": settings.sec_user_agent}) as r:
        size, chunks = 0, []
        for chunk in r.iter_bytes():
            size += len(chunk)
            if size > INSTANCE_MAX_BYTES:
                print(f"    {accession}: refused, over {INSTANCE_MAX_BYTES} bytes")
                return None
            chunks.append(chunk)
        status = r.status_code
    body = b"".join(chunks)
    (out / f"instance_{accession}.xml.gz").write_bytes(gzip.compress(body))
    print(f"    fetched {url} http={status} sha256={hashlib.sha256(body).hexdigest()}")
    return body if status == 200 else None


def main() -> None:
    out = Path("var/research/3616/slice3d3a") / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=False)
    with psycopg.connect(settings.database_url) as conn:
        effective = effective_tax_matching(conn)
        if effective is None:
            print("no effective matching run")
            return
        conn.commit()
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        run_id, evidence_run_id = effective.matching_run_id, effective.evidence_run_id
        clock = conn.execute("SELECT clock_timestamp()").fetchone()
        assert clock is not None
        as_of: datetime = clock[0]
        print(f"[run] effective matching_run_id={run_id} evidence_run_id={evidence_run_id} as_of={as_of}")
        securities = conn.execute(
            "SELECT tax_security_id, instrument_ids, status, pool_units, pool_cost_gbp, pool_reasons "
            "FROM tax_matching_securities WHERE matching_run_id = %s ORDER BY tax_security_id",
            (run_id,),
        ).fetchall()
        disposals = conn.execute(
            "SELECT tax_security_id, uk_date, tax_year, gain_gbp, status, reasons FROM tax_statutory_disposals "
            "WHERE matching_run_id = %s ORDER BY 1, 2",
            (run_id,),
        ).fetchall()
        idents = dict(
            conn.execute(
                "SELECT tax_security_id, count(*) FROM tax_identifications WHERE matching_run_id = %s GROUP BY 1",
                (run_id,),
            ).fetchall()
        )
        dependencies = conn.execute(
            "SELECT tax_security_id, reason, count(*) FROM tax_matching_dependencies WHERE matching_run_id = %s "
            "GROUP BY 1, 2 ORDER BY 1, 2",
            (run_id,),
        ).fetchall()
        first_acq: dict[int, date] = dict(
            conn.execute(
                "SELECT i.tax_security_id, min(l.uk_date) FROM tax_evidence_lots l "
                "JOIN tax_matching_instruments i ON i.matching_run_id = %s AND i.instrument_id = l.instrument_id "
                "WHERE l.run_id = %s AND l.direction = 'acquisition' AND i.tax_security_id IS NOT NULL GROUP BY 1",
                (run_id, evidence_run_id),
            ).fetchall()
        )
        unplaced = {
            r[0]
            for r in conn.execute(
                "SELECT instrument_id FROM tax_matching_instruments WHERE matching_run_id = %s "
                "AND tax_security_id IS NULL",
                (run_id,),
            ).fetchall()
        }
        members = sorted({i for row in securities for i in row[1]})
        inst = {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT instrument_id, symbol, instrument_type_id FROM instruments WHERE instrument_id = ANY(%s)",
                (members,),
            ).fetchall()
        }
        ciks: dict[int, str] = dict(
            conn.execute(
                "SELECT instrument_id, identifier_value FROM external_identifiers WHERE instrument_id = ANY(%s) "
                "AND provider = 'sec' AND identifier_type = 'cik' AND is_primary",
                (members,),
            ).fetchall()
        )
        history = conn.execute(
            "SELECT instrument_id, symbol FROM instrument_symbol_history WHERE instrument_id = ANY(%s)", (members,)
        ).fetchall()
        profiles = {
            r[0]: Profile(r[1], r[2], r[3])
            for r in conn.execute(
                "SELECT instrument_id, entity_type, sic, fetched_at FROM instrument_sec_profile "
                "WHERE instrument_id = ANY(%s)",
                (members,),
            ).fetchall()
        }
        filings: dict[int, list[CoverFiling]] = {}
        for iid, acc, form, filed, url in conn.execute(
            "SELECT instrument_id, provider_filing_id, filing_type, filing_date, primary_document_url "
            "FROM filing_events WHERE provider = 'sec' AND instrument_id = ANY(%s) AND filing_type = ANY(%s)",
            (members, list(COVER_FORMS)),
        ).fetchall():
            filings.setdefault(iid, []).append(CoverFiling(acc, form, filed, url))
        needed: set[str] = set()
        for sec_id, member_ids, *_ in securities:
            ordered = consolidate([f for i in member_ids for f in filings.get(i, [])]) or []
            needed.update(f.accession for f in ordered[:1])
            if sec_id in first_acq:
                needed.update([f.accession for f in ordered if f.filed_on <= first_acq[sec_id]][:1])
        raw: dict[str, bytes] = {}
        oversized: set[str] = set()
        for acc in sorted(needed):
            size = conn.execute(
                "SELECT octet_length(payload) FROM filing_raw_documents WHERE accession_number = %s "
                "AND document_kind = 'xbrl_cover_instance'",
                (acc,),
            ).fetchone()
            if size is not None and size[0] is not None and size[0] > INSTANCE_MAX_BYTES:
                oversized.add(acc)
                continue
            body = stored_body(conn, accession_number=acc, document_kind="xbrl_cover_instance")
            if body is not None:
                raw[acc] = body.encode("utf-8")
        mismatched = conn.execute(
            "SELECT count(*) FROM tax_matching_instruments m LEFT JOIN instruments i USING (instrument_id) "
            "WHERE m.matching_run_id = %s AND (m.symbol IS DISTINCT FROM i.symbol "
            "OR m.canonical_instrument_id IS DISTINCT FROM i.canonical_instrument_id)",
            (run_id,),
        ).fetchone()
        assert mismatched is not None
        unsealed = conn.execute(
            "SELECT count(*) FROM instruments i WHERE NOT EXISTS (SELECT 1 FROM tax_matching_instruments m "
            "WHERE m.matching_run_id = %(r)s AND m.instrument_id = i.instrument_id) AND ("
            "i.canonical_instrument_id = ANY(%(m)s) OR upper(i.symbol) = ANY(SELECT upper(symbol) || '.RTH' "
            "FROM instruments WHERE instrument_id = ANY(%(m)s)) OR upper(i.symbol) || '.RTH' = ANY(SELECT "
            "upper(symbol) FROM instruments WHERE instrument_id = ANY(%(m)s)))",
            {"r": run_id, "m": members},
        ).fetchone()
        assert unsealed is not None
        if mismatched[0] or unsealed[0]:
            print(f"instruments differ from run {run_id}'s sealed mapping ({mismatched[0]}, {unsealed[0]}); rerun")
            return
        conn.commit()
    with psycopg.connect(settings.database_url) as conn:
        again = effective_tax_matching(conn)
    if again is None or again.matching_run_id != run_id:
        print(f"effective run changed during the read ({run_id} → {again}); rerun")
        return
    ever: dict[int, set[str]] = {}
    for iid, sym in history:
        ever.setdefault(iid, set()).add(symbol_key(sym))
    client = httpx.Client(timeout=60, follow_redirects=False)
    for sec_id, member_ids, status, pool_units, pool_cost, pool_reasons in securities:
        sec_types = {inst.get(i, (None, None))[1] for i in member_ids}
        sec_ciks = {ciks[i] for i in member_ids if i in ciks}
        symbol = symbol_key(inst[sec_id][0]) if sec_id in inst else None
        symbols_ever = {symbol_key(inst[i][0]) for i in member_ids if i in inst} | {
            x for i in member_ids for x in ever.get(i, set())
        }
        carriers = [i for i in member_ids if i in ciks]
        sec_profiles = [profiles.get(i) for i in carriers]
        sec_filings = [f for i in member_ids for f in filings.get(i, [])]
        ordered = consolidate(sec_filings) or []
        wanted = ordered[:1]
        start = first_acq.get(sec_id)
        if start is not None:
            wanted += [f for f in ordered if f.filed_on <= start][:1]
        covers: dict[str, Cover | None] = {}
        print(f"== security {sec_id} members={member_ids} types={sorted(sec_types, key=str)} ciks={sorted(sec_ciks)}")
        print(f"  symbol={symbol} symbols_ever={sorted(symbols_ever)} profiles={sec_profiles}")
        for f in dict.fromkeys(wanted):
            if f.accession in oversized:
                covers[f.accession] = None
                print(f"  [cover {f.accession} stored body over {INSTANCE_MAX_BYTES} bytes] unusable")
                continue
            body = raw.get(f.accession)
            source = "stored"
            if body is None and f.primary_document_url and (url := instance_url(f.primary_document_url)):
                body = _fetch_instance(client, url, out, f.accession)
                source = f"fetched {url}"
            if body is not None:
                covers[f.accession] = read_cover(body)
            print(f"  [cover {f.accession} {f.form} {f.filed_on} {source}] {covers.get(f.accession, 'none')}")
        if set(member_ids) & unplaced:
            character, detail = "unestablished", "identity_unestablished"
        else:
            character, detail = classify(
                types=sec_types,
                ciks=sec_ciks,
                symbol=symbol,
                symbols_ever=symbols_ever,
                profiles=sec_profiles,
                as_of=as_of,
                filings=sec_filings,
                covers=covers,
                first_acq=start,
            )
        print(f"  first_acq={start} character={character} detail={detail}")
        print(f"  run {run_id}: status={status} pool={pool_units} / {pool_cost} reasons={pool_reasons}")
        print(f"  identifications={idents.get(sec_id, 0)}")
        for s, d, ty, gain, st, reasons in disposals:
            if s == sec_id:
                print(f"  disposal {d} {ty} gain={gain} status={st} reasons={reasons}")
        for s, reason, n in dependencies:
            if s == sec_id:
                print(f"  dependency {reason} x{n}")
        print()
    print(f"bodies: {out}")


def universe(archive: Path) -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        rows = conn.execute(
            "SELECT e.identifier_value FROM instruments i JOIN external_identifiers e "
            "ON e.instrument_id = i.instrument_id AND e.provider = 'sec' AND e.identifier_type = 'cik' "
            "AND e.is_primary WHERE i.instrument_type_id = %s",
            (STOCKS_TYPE_ID,),
        ).fetchall()
    outcomes: Counter[str] = Counter()
    other_forms: Counter[str] = Counter()
    sics: Counter[str] = Counter()
    with zipfile.ZipFile(archive) as z:
        names = set(z.namelist())
        for (cik,) in rows:
            member = f"CIK{cik}.json"
            if member not in names:
                outcomes["cik_not_in_archive"] += 1
                continue
            doc = json.loads(z.read(member))
            entity_type, sic = doc.get("entityType"), doc.get("sic")
            if entity_type != "operating":
                outcomes[f"entity_type={entity_type}"] += 1
                if entity_type == "other":
                    forms = set(doc.get("filings", {}).get("recent", {}).get("form", []))
                    other_forms[next((x for x in ("20-F", "40-F", "10-K") if x in forms), "none")] += 1
            elif not isinstance(sic, str) or not _SIC.fullmatch(sic) or sic in INVESTMENT_SIC:
                outcomes["sic"] += 1
                sics[sic or "empty"] += 1
            else:
                outcomes["to_cover_test"] += 1
    print(f"[universe] archive={archive} instruments={len(rows)}")
    for key, n in outcomes.most_common():
        print(f"  {key}: {n}")
    print(f"  entity_type=other by annual form: {dict(other_forms)}")
    print(f"  sic: {dict(sics)}")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--universe":
        universe(Path(sys.argv[2]))
    else:
        main()
