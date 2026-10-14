"""#3616 slice 3d-3a fact-finding: the tax character each security of the effective matching run would get.

Read-only. Resolves the effective matching run with ``effective_tax_matching`` (3d-2 Design 9). Then, in one
REPEATABLE READ transaction on the dev DB, reads that run's rows (sealed, so immutable): securities, disposals,
identifications, dependencies, pools, placements and each security's first admitted acquisition date; and the inputs
Design 2 names: per member its symbol, ``instrument_type_id``, primary SEC CIK, symbol history and SEC profile
(``instrument_sec_profile``), the members' SEC 10-K / 10-Q / 8-K rows of ``filing_events``, and the cached cover
outcome (``sec_cover_12b_fetches`` / ``sec_cover_12b_pairs``) of the two covers Design 2 reads. After the transaction
it checks the effective run is unchanged. A cover not yet cached is fetched: SEC's extracted instance beside the
primary document, ``<stem>_htm.xml`` (``def14a_recipients.instance_url``; User-Agent ``settings.sec_user_agent``, at
most 16 MiB), parsed with ``def14a_recipients.parse_cover_instance``, without writing the cache. It applies
``classify``, the spec's ``tax_character_v1`` table, and prints per security every input, the character and the run's
figures. Fetched bodies go to a new directory under ``var/research/3616/slice3d3a/`` with their SHA-256; the printout
carries dates, amounts, symbols, CIKs and accession numbers, never an account identifier.

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
import psycopg

from app.config import settings
from app.services.def14a_recipients import CachedCover, CoverUnresolved, instance_url, parse_cover_instance, symbol_key
from app.services.tax_matching import effective_tax_matching

INSTANCE_MAX_BYTES = 16 * 1024 * 1024
PROFILE_MAX_AGE = timedelta(days=30)
STOCKS_TYPE_ID = 5
ETF_TYPE_ID = 6
INVESTMENT_SIC = frozenset({"6722", "6726", "6770", "6799"})
COVER_FORMS = ("10-K", "10-Q", "8-K")
_SIC = re.compile(r"^[0-9]{4}$")
_COMMON = re.compile(
    r"^(CLASS [A-Z] )?(COMMON STOCK|ORDINARY SHARES)"
    r"(,? ((US)?\$?[0-9]*\.?[0-9]+ PAR VALUE|PAR VALUE (US)?\$?[0-9]*\.?[0-9]+|NO PAR VALUE)( PER SHARE)?)?$"
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


def common_title(title: str) -> bool:
    return _COMMON.match(" ".join(title.split()).upper()) is not None


def classify(
    *,
    types: set[int | None],
    ciks: set[str],
    symbol: str | None,
    symbols_ever: set[str],
    profile: Profile | None,
    as_of: datetime,
    covers: list[CoverFiling],
    outcomes: dict[str, CachedCover],
    first_acq: date | None,
) -> tuple[str, str | None]:
    """``tax_character_v1``: (character, detail), detail None for a share. ``covers`` newest first; ``outcomes`` the
    cached outcome per accession (absent: not cached)."""
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
    if profile is None or not (as_of - PROFILE_MAX_AGE <= profile.fetched_at <= as_of):
        return "unestablished", "sec_profile_unavailable"
    if profile.entity_type != "operating":
        return "unestablished", "entity_type"
    if profile.sic is None or not _SIC.match(profile.sic) or profile.sic in INVESTMENT_SIC:
        return "unestablished", "sic"

    def pairs_of(filing: CoverFiling) -> frozenset[tuple[str, str]] | str:
        got = outcomes.get(filing.accession)
        if got is None:
            return "unavailable"
        if got.outcome != "pairs" or cik not in got.entity_ciks:
            return "unusable"
        return got.pairs

    if not covers:
        return "unestablished", "cover_unavailable"
    latest = pairs_of(covers[0])
    if isinstance(latest, str):
        return "unestablished", f"cover_{latest}"
    listed = [t for t, s in latest if s == symbol]
    if len(listed) != 1 or not common_title(listed[0]):
        return "unestablished", "listed_class_not_common_stock"
    if first_acq is not None:
        start = next((f for f in covers if f.filed_on <= first_acq), None)
        if start is None:
            return "unestablished", "holding_start_cover_unavailable"
        then = pairs_of(start)
        if isinstance(then, str):
            return "unestablished", f"holding_start_cover_{then}"
        bound = [t for t, s in then if s in symbols_ever]
        if len(bound) != 1 or not common_title(bound[0]):
            return "unestablished", "holding_start_class_not_common_stock"
    return "share", None


def _fetch_instance(client: httpx.Client, url: str, out: Path, accession: str) -> CachedCover | None:
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
    if status in (404, 410):
        return CachedCover(outcome="not_found", entity_ciks=frozenset(), pairs=frozenset())
    if status != 200:
        return None
    try:
        return parse_cover_instance(body.decode("utf-8"))
    except (CoverUnresolved, UnicodeDecodeError) as exc:
        print(f"    {accession}: unresolved: {exc}")
        return None


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
            "SELECT DISTINCT ON (instrument_id, provider_filing_id) instrument_id, provider_filing_id, filing_type, "
            "filing_date, primary_document_url FROM filing_events WHERE provider = 'sec' "
            "AND instrument_id = ANY(%s) AND filing_type = ANY(%s) ORDER BY instrument_id, provider_filing_id, "
            "filing_event_id",
            (members, list(COVER_FORMS)),
        ).fetchall():
            filings.setdefault(iid, []).append(CoverFiling(acc, form, filed, url))
        cached: dict[str, CachedCover] = {}
        accs = [f.accession for fs in filings.values() for f in fs]
        for acc, outcome, ents in conn.execute(
            "SELECT cover_accession, outcome, entity_ciks FROM sec_cover_12b_fetches WHERE cover_accession = ANY(%s)",
            (accs,),
        ).fetchall():
            pairs = frozenset(
                (str(t), str(s))
                for t, s in conn.execute(
                    "SELECT security_title, trading_symbol FROM sec_cover_12b_pairs WHERE cover_accession = %s", (acc,)
                ).fetchall()
            )
            cached[acc] = CachedCover(outcome=outcome, entity_ciks=frozenset(ents), pairs=pairs)
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
        carrier = [i for i in member_ids if i in ciks]
        profile = profiles.get(carrier[0]) if len(sec_ciks) == 1 and carrier else None
        covers = sorted(
            {f for i in member_ids for f in filings.get(i, [])}, key=lambda f: (f.filed_on, f.accession), reverse=True
        )
        wanted = covers[:1]
        start = first_acq.get(sec_id)
        if start is not None:
            wanted += [f for f in covers if f.filed_on <= start][:1]
        outcomes: dict[str, CachedCover] = {}
        print(f"== security {sec_id} members={member_ids} types={sorted(sec_types, key=str)} ciks={sorted(sec_ciks)}")
        print(f"  symbol={symbol} symbols_ever={sorted(symbols_ever)} profile={profile}")
        for f in dict.fromkeys(wanted):
            got = cached.get(f.accession)
            source = "cache"
            if got is None and f.primary_document_url and (url := instance_url(f.primary_document_url)):
                got = _fetch_instance(client, url, out, f.accession)
                source = f"fetched {url}"
            if got is not None:
                outcomes[f.accession] = got
            print(f"  [cover {f.accession} {f.form} {f.filed_on} {source}] {got}")
        if set(member_ids) & unplaced:
            character, detail = "unestablished", "identity_unestablished"
        else:
            character, detail = classify(
                types=sec_types,
                ciks=sec_ciks,
                symbol=symbol,
                symbols_ever=symbols_ever,
                profile=profile,
                as_of=as_of,
                covers=covers,
                outcomes=outcomes,
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
            elif not isinstance(sic, str) or not _SIC.match(sic) or sic in INVESTMENT_SIC:
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
