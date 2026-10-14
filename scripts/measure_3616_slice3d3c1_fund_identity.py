"""#3616 slice 3d-3c-1 fact-finding: the public evidence about each held fund, and the observations the spec makes.

Read-only on the dev DB. Reads the effective evidence generation the way ``refresh_cover_instances`` does
(``tax_matching._read_inputs`` without bodies, then ``security_members``) in one REPEATABLE READ snapshot, and takes
every fund security (all members ``instrument_type_id`` 6). For each, from the snapshot: its members, symbol, primary
CIK and SEC ``N-CEN`` / ``N-CEN/A`` rows of ``filing_events``. Then it fetches, without storing anything in the DB:

- the raw XML of the newest N-CEN (spec Design 1), whose URL must be the accession's own SEC archive directory;
- GLEIF's LEI record and the first page (200) of its ISIN relationship;
- OpenFIGI's mapping of the ISIN set, one ``ID_ISIN`` job per ISIN on the N-CEN listing's ``micCode`` (keyless);
- once, HMRC's "Approved offshore reporting funds" content item and its ``.ods`` attachment.

It applies the spec's pure readers (``read_ncen``, ``read_gleif_record``, ``read_gleif_isins``, ``read_figi``,
``read_reporting_list``) and its decisions (``identity_outcome``, ``search_version``, ``search_outcome``), from
``app.services.tax_fund_evidence``, printing every input and
outcome, and the population counts of the list the spec cites. It measures one fresh capture: the capture
preconditions it checks are printed, and stored-history behaviour (search reuse, outcomes over several versions) is
the build's. Bodies go gzipped to a new directory under
``var/research/3616/slice3d3c1/`` with their SHA-256. The printout carries dates, symbols, CIKs, LEIs, ISINs and
accession numbers, never an account identifier.

``--universe`` counts, DB-only, every ETF instrument (type 6, tradable or not, variants included): with a primary SEC
CIK, with at least one N-CEN in ``filing_events``, by country. Coverage, not correctness.

    uv run python -m scripts.measure_3616_slice3d3c1_fund_identity
    uv run python -m scripts.measure_3616_slice3d3c1_fund_identity --universe
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import sys
import time
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg

from app.config import settings
from app.services.def14a_recipients import symbol_key
from app.services.tax_fund_evidence import (
    Filing,
    consolidate,
    figi_jobs,
    figi_request_key,
    identity_outcome,
    ncen_sound,
    ncen_xml_url,
    newest_period,
    padded_cik,
    read_content_item,
    read_figi,
    read_gleif_isins,
    read_gleif_record,
    read_ncen,
    read_reporting_list,
    search_outcome,
    search_version,
    valid_isin,
)
from app.services.tax_matching import (  # pyright: ignore[reportPrivateUsage]
    _read_inputs,
    effective_tax_matching,
    security_members,
)

ETF_TYPE_ID = 6
HMRC_CONTENT_URL = "https://www.gov.uk/api/content/government/publications/approved-offshore-reporting-funds"
GLEIF = "https://api.gleif.org/api/v1/lei-records"
OPENFIGI = "https://api.openfigi.com/v3/mapping"
OPENFIGI_SPACING_S = 2.4
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")


# --- measurement -------------------------------------------------------------------------------------------------


def _save(out: Path, name: str, body: bytes) -> str:
    sha = hashlib.sha256(body).hexdigest()
    (out / f"{name}.gz").write_bytes(gzip.compress(body))
    return sha


def cell_class(cell: str) -> str:
    if cell == cell.strip() and valid_isin(cell):
        return "valid"
    if valid_isin(cell.strip()):
        return "valid_after_trim"
    if _ISIN.fullmatch(cell.strip()):
        return "isin_shape_bad_check"
    if re.fullmatch(r"[A-Za-z0-9]{12}", cell.strip()):
        return "other_12_alnum"
    return "no_data" if cell == "no data" else "other"


def universe() -> None:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            "SELECT coalesce(i.country, '?'), "
            "EXISTS (SELECT 1 FROM external_identifiers e WHERE e.instrument_id = i.instrument_id "
            "AND e.provider = 'sec' AND e.identifier_type = 'cik' AND e.is_primary), "
            "EXISTS (SELECT 1 FROM filing_events f WHERE f.instrument_id = i.instrument_id AND f.provider = 'sec' "
            "AND f.filing_type = 'N-CEN') FROM instruments i WHERE i.instrument_type_id = %s",
            (ETF_TYPE_ID,),
        ).fetchall()
    print(f"[universe] ETF instruments={len(rows)}")
    for (country, cik, ncen), n in Counter(rows).most_common():
        print(f"  country={country} primary_cik={cik} has_ncen={ncen}: {n}")


def _list_counts(rows: list[tuple[str, ...]]) -> Iterator[str]:
    isin_cells = [r[5] for r in rows]
    yield f"isin_cells={dict(Counter(cell_class(c) for c in isin_cells))}"
    shape = Counter(
        "no_data" if r[6] == "no data" else ("cusip_shape" if re.fullmatch(r"[A-Z0-9]{9}", r[6].strip()) else "other")
        for r in rows
    )
    yield f"cusip_cells={dict(shape)}"
    neither = [r for r in rows if r[5] == "no data" and r[6] == "no data"]
    odd_refs = [r[3] for r in rows if not re.fullmatch(r"[A-Z][0-9]{4}-[0-9]{4}", r[3])]
    yield (
        f"both identifiers 'no data'={len(neither)} "
        f"(no cessation date {sum(1 for r in neither if r[8] == 'no data')}); "
        f"US-prefixed ISIN cells={sum(1 for c in isin_cells if c.startswith('US'))}; "
        f"share-class refs not [A-Z]dddd-dddd={odd_refs}"
    )
    for col, label in ((7, "with effect from"), (8, "ceased")):
        kinds = Counter("date" if re.fullmatch(r"\d\d/\d\d/\d{4}", r[col]) else r[col] for r in rows)
        yield f"{label}: {kinds.most_common(4)}"
    named = [r for r in rows if "SPDR S&P 500" in " ".join(r[2:5]).upper()]
    yield f"rows naming 'SPDR S&P 500': {len(named)} {sorted((r[2], r[5]) for r in named)}"
    yield f"rows without a valid ISIN after trimming: {sum(1 for c in isin_cells if not valid_isin(c.strip()))}"


Chain = tuple[dict[str, bytes], bytes | None, bytes | None, tuple[str, bytes] | None]


def _fetch_chain(
    client: httpx.Client, out: Path, ua: dict[str, str], newest: Filing, cik: str, symbol: str | None
) -> tuple[str | None, Chain]:
    """The fetched bodies, and the LEI read from the N-CEN (None when none was read)."""
    ncen: dict[str, bytes] = {}
    record = isins = None
    figi: tuple[str, bytes] | None = None
    url = ncen_xml_url(newest, cik)
    if url is None:
        return None, (ncen, record, isins, figi)
    r = client.get(url, headers=ua)
    sha = _save(out, "ncen_" + newest.accession, r.content)
    print(f"  [n-cen {newest.accession}] {url} http={r.status_code} sha256={sha}")
    sound = ncen_sound(r.content)
    print(f"    capture sound (2xx, N-CEN root and submissionType)={200 <= r.status_code < 300 and sound}")
    if not (200 <= r.status_code < 300 and sound):
        return None, (ncen, record, isins, figi)
    ncen[newest.accession] = r.content
    doc = read_ncen(r.content, cik)
    print(f"    read_ncen={doc}")
    if doc is None:
        return None, (ncen, record, isins, figi)
    rec = client.get(f"{GLEIF}/{doc.lei}")
    print(f"  [gleif record] http={rec.status_code} sha256={_save(out, 'gleif_record_' + doc.lei, rec.content)}")
    record = rec.content if rec.status_code == 200 else None
    print(f"    read_gleif_record={read_gleif_record(rec.content, doc.lei) if record else None}")
    if record and read_gleif_record(record, doc.lei) is not None:
        entity = json.loads(record)["data"]["attributes"]["entity"]
        print(
            f"    legalForm={entity.get('legalForm')} jurisdiction={entity.get('jurisdiction')} "
            f"legalAddress={entity.get('legalAddress') if isinstance(entity.get('legalAddress'), dict) else None} "
            f"successorEntity={entity.get('successorEntity')} successorEntities={entity.get('successorEntities')}"
        )
    ir = client.get(f"{GLEIF}/{doc.lei}/isins", params={"page[size]": "200", "page[number]": "1"})
    print(f"  [gleif isins] http={ir.status_code} sha256={_save(out, 'gleif_isins_' + doc.lei, ir.content)}")
    isins = ir.content if ir.status_code == 200 else None
    parsed = read_gleif_isins(ir.content) if isins else None
    print(f"    read_gleif_isins={parsed}")
    keyed = [symbol_key(t) for _mic, t in doc.listings]
    if parsed and symbol is not None and keyed.count(symbol) == 1 and 0 < len(parsed.entries) <= 10:
        mic = doc.listings[keyed.index(symbol)][0]
        jobs = figi_jobs(parsed.entries, mic)
        time.sleep(OPENFIGI_SPACING_S)
        m = client.post(OPENFIGI, json=jobs)
        print(f"  [openfigi {jobs}] http={m.status_code} sha256={_save(out, 'openfigi_' + doc.lei, m.content)}")
        print(f"    body={m.text[:600]}")
        if m.status_code == 200:
            figi = (figi_request_key(parsed.entries, mic), m.content)
            print(f"    read_figi={read_figi(m.content, len(jobs))}")
    return doc.lei, (ncen, record, isins, figi)


def main() -> None:
    if "--universe" in sys.argv[1:]:
        universe()
        return
    out = Path("var/research/3616/slice3d3c1") / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=False)
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        inputs = _read_inputs(conn, bodies=False)
        if inputs is None:
            print("no effective evidence generation")
            return
        members = security_members(inputs)
        funds = {s: m for s, m in members.items() if inputs.characters[s].types == frozenset({ETF_TYPE_ID})}
        all_ids = sorted({i for m in funds.values() for i in m})
        rows = conn.execute(
            "SELECT instrument_id, filing_type, provider_filing_id, filing_date, report_date, primary_document_url "
            "FROM filing_events WHERE provider = 'sec' AND instrument_id = ANY(%s) "
            "AND filing_type IN ('N-CEN', 'N-CEN/A')",
            (all_ids,),
        ).fetchall()
        shapes = conn.execute(
            "SELECT filing_type, regexp_replace(primary_document_url, "
            "'^https://www[.]sec[.]gov/Archives/edgar/data/[0-9]+/[0-9]{18}/', '<base>/'), count(*) "
            "FROM filing_events WHERE provider = 'sec' AND filing_type IN ('N-CEN', 'N-CEN/A') GROUP BY 1, 2"
        ).fetchall()
        country = dict(
            conn.execute(
                "SELECT instrument_id, country FROM instruments WHERE instrument_id = ANY(%s)", (all_ids,)
            ).fetchall()
        )
        nsin_rows = conn.execute(
            "SELECT coalesce(resolution_status, ''), coalesce(source, ''), coalesce(name_of_issuer, ''), "
            "observation_count FROM unresolved_13f_cusips WHERE cusip = '78462F103' ORDER BY 1, 2"
        ).fetchall()
        conn.commit()
    with psycopg.connect(settings.database_url) as conn:
        effective = effective_tax_matching(conn)
        characters = (
            conn.execute(
                "SELECT tax_security_id, character, detail FROM tax_matching_characters WHERE matching_run_id = %s "
                "AND tax_security_id = ANY(%s) ORDER BY 1",
                (effective.matching_run_id, sorted(funds)),
            ).fetchall()
            if effective is not None
            else []
        )
    print(f"[generation] evidence_run_id={inputs.evidence_run_id} as_of={inputs.as_of} securities={len(members)}")
    print(f"[effective matching] {effective} fund characters={characters}")
    print(f"[unresolved_13f_cusips 78462F103] {nsin_rows}")
    print(f"[raw N-CEN/N-CEN/A filing_events rows for fund members] {Counter((r[0], r[1]) for r in rows)}")
    print(f"[fund securities] {sorted(funds)}")
    print(f"[n-cen url shapes, all of dev] {shapes}")
    filings_of: dict[int, list[Filing]] = {}
    for iid, form, acc, filed, report, url in rows:
        filings_of.setdefault(iid, []).append(Filing(acc, form, filed, report, url))

    ua = {"User-Agent": settings.sec_user_agent}
    client = httpx.Client(timeout=60, follow_redirects=False)
    observed_on = datetime.now(UTC).date()
    candidates: dict[int, tuple[str, list[str]]] = {}
    for sec_id, member_ids in sorted(funds.items()):
        ev = inputs.characters[sec_id]
        filings = consolidate([f for i in member_ids for f in filings_of.get(i, [])], observed_on)
        print(f"\n== security {sec_id} members={list(member_ids)} symbol={ev.symbol} ciks={sorted(ev.ciks)}")
        print(f"  countries={sorted({str(country.get(i)) for i in member_ids})}")
        print(f"  filings={[(f.accession, f.form, str(f.filing_date), str(f.report_date)) for f in filings or []]}")
        padded = {padded_cik(c) for c in ev.ciks}
        _period, newest, _amendments = newest_period(filings or [])
        lei: str | None = None
        chain: Chain = ({}, None, None, None)
        if newest is not None and len(padded) == 1 and None not in padded:
            lei, chain = _fetch_chain(client, out, ua, newest, str(next(iter(padded))), ev.symbol)
        ncen, record, isins, figi = chain
        outcome, got = identity_outcome(
            ciks=ev.ciks,
            symbol=ev.symbol,
            filings=filings,
            ncen=ncen,
            records={lei: record} if lei is not None and record is not None else {},
            isin_sets={lei: isins} if lei is not None and isins is not None else {},
            mappings=dict([figi]) if figi is not None else {},
        )
        print(f"  IDENTITY outcome={outcome} fields={json.dumps(got, default=str)}")
        if outcome == "candidate":
            candidates[sec_id] = (got["isin"], got["name_keys"])

    content = client.get(HMRC_CONTENT_URL)
    print(f"\n[hmrc content] http={content.status_code} sha256={_save(out, 'hmrc_content_item', content.content)}")
    item = read_content_item(content.content, datetime.now(UTC)) if 200 <= content.status_code < 300 else None
    print(f"  read_content_item={item}")
    if item is None:
        print("  precondition failed: the content item is not sound (spec Design 1)")
        return
    size = item.file_size
    r = client.get(item.attachment_url)
    print(
        f"[hmrc list] http={r.status_code} received={len(r.content)} sha256={_save(out, 'hmrc_weblist_ods', r.content)}"
    )
    print(f"  preconditions: http 200={r.status_code == 200} size equals file_size={len(r.content) == size}")
    if r.status_code != 200 or len(r.content) != size:
        return
    started = time.monotonic()
    assessed = read_reporting_list(r.content, None)
    print(f"  read_reporting_list (no earlier version) in {time.monotonic() - started:.1f}s")
    print(f"  [reader] zip members={list(assessed.members)}")
    print(f"  [reader] logical text of the version: {assessed.logical_chars} characters")
    if assessed.rows is None:
        print(f"  UNUSABLE: {assessed.refusal}")
        return
    parsed_list = list(assessed.rows)
    print(f"  usable; data rows={len(parsed_list)}")
    for line in _list_counts(parsed_list):
        print(f"  {line}")
    for sec_id, (isin, keys) in candidates.items():
        found = search_version(isin, keys, parsed_list)
        obs = search_outcome([found.counts], ["searched"], content_current=True, child_list_usable=True)
        print(
            f"\n  LIST security={sec_id} isin={isin} name_keys={keys} outcome={obs} "
            f"counts={dict(found.counts)} examples={list(found.examples)}"
        )


if __name__ == "__main__":
    main()
