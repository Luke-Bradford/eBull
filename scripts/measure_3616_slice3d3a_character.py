"""#3616 slice 3d-3a fact-finding: the tax character each security of the effective matching run would get.

Read-only. Reads, in one REPEATABLE READ transaction on the dev DB, the latest published ``tax_matching_runs`` row,
its securities and statutory disposals, the earliest admitted acquisition date of each security in the run's evidence
generation, and per member instrument its ``instrument_type_id``, its primary SEC CIK and its CUSIPs
(``external_identifiers``). Then it fetches the public sources the spec names (no account, no key):

- the SEC submissions JSON of each CIK (``https://data.sec.gov/submissions/CIK##########.json``, declared
  User-Agent ``settings.sec_user_agent``);
- the GOV.UK content item of HMRC's "Approved offshore reporting funds" publication, and the ``.ods`` attachment it
  names, parsed with the standard library (an ODS file is a zip holding ``content.xml``).

It applies the spec's ``tax_character_v1`` rules (Design 2 to 4) and prints, per security, every input it used, the
outcome and, per disposal, the routed amounts. Response bodies go to a new directory under ``var/research/3616/
slice3d3a/`` with their SHA-256; the printout carries dates, amounts, symbols, CIKs and CUSIPs, never an account
identifier.

    uv run python -m scripts.measure_3616_slice3d3a_character
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import sys
import zipfile
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import httpx
import psycopg

from app.config import settings

HMRC_CONTENT_URL = "https://www.gov.uk/api/content/government/publications/approved-offshore-reporting-funds"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
STOCKS_TYPE_ID = 5
ETF_TYPE_ID = 6
CORPORATE_SUFFIXES = frozenset(
    {
        "INC",
        "INCORPORATED",
        "CORP",
        "CORPORATION",
        "CO",
        "COMPANY",
        "LTD",
        "LIMITED",
        "PLC",
        "NV",
        "SA",
        "AG",
        "SE",
        "PBC",
    }
)
LIST_HEADERS = (
    "Letter Group",
    "Reporting Fund Ref",
    "Parent Fund",
    "HMRC Share Class Ref No.’s",
    "Sub Fund Name",
    "ISIN No",
    "CUSIP No",
    "Reporting Fund, with effect from",
    "Ceased to be an RF on",
)
_NS = {
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
}
_DMY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def _q(ns: str, tag: str) -> str:
    return "{" + _NS[ns] + "}" + tag


def _save(out: Path, name: str, body: bytes) -> str:
    sha = hashlib.sha256(body).hexdigest()
    (out / f"{name}.gz").write_bytes(gzip.compress(body))
    return sha


def ods_rows(body: bytes) -> Iterator[list[str]]:
    """Yield each row of the first sheet as displayed cell texts, expanding repeated cells (empty trailing rows end)."""
    with zipfile.ZipFile(io.BytesIO(body)) as z, z.open("content.xml") as f:
        for _event, el in ElementTree.iterparse(f, events=("end",)):
            if el.tag == _q("table", "table"):
                return
            if el.tag != _q("table", "table-row"):
                continue
            cells: list[str] = []
            for cell in el:
                if cell.tag not in (_q("table", "table-cell"), _q("table", "covered-table-cell")):
                    continue
                repeat = int(cell.get(_q("table", "number-columns-repeated"), "1"))
                text = "\n".join("".join(p.itertext()) for p in cell.findall("text:p", _NS))
                cells.extend([text] * min(repeat, 64))
            while cells and cells[-1] == "":
                cells.pop()
            el.clear()
            if cells:
                yield cells


def cusip_check_digit(nsin: str, country: str) -> str:
    """ISO 6166 check digit: letters to numbers (A=10), Luhn over the digit string from the right."""
    digits = "".join(str(int(ch, 36)) for ch in country + nsin)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 0:
            d *= 2
        total += d // 10 + d % 10
    return str((10 - total % 10) % 10)


def parse_list_date(text: str) -> date | None | str:
    """``dd/mm/yyyy`` → date; ``no data`` → None (open); anything else → the raw text (unparseable)."""
    if text == "no data":
        return None
    m = _DMY.match(text)
    if m is None:
        return text
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return text


_SEC_ANNOTATION = re.compile(r"\s*[/\\][A-Z]{2,3}[/\\]?\s*$|\s*/NEW/?\s*$")


def corporate_suffix(name: str) -> bool:
    """The SEC conformed name ends in a corporate legal-form word, after SEC's trailing state or ``/NEW/`` annotation
    (``INC /DE/``, ``Inc./NV``, ``\\DE\\``) is removed."""
    text = name.upper().strip()
    while (stripped := _SEC_ANNOTATION.sub("", text)) != text:
        text = stripped
    tokens = [t for t in re.split(r"[\s,]+", text) if t]
    return bool(tokens) and tokens[-1].replace(".", "") in CORPORATE_SUFFIXES


def main() -> None:
    out = Path("var/research/3616/slice3d3a") / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=False)
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        run = conn.execute(
            "SELECT matching_run_id, evidence_run_id, history_fetch_started_at FROM tax_matching_runs "
            "WHERE state = 'published' ORDER BY matching_run_id DESC LIMIT 1"
        ).fetchone()
        if run is None:
            print("no published matching run")
            return
        run_id, evidence_run_id, fetch_started = run
        print(
            f"[run] matching_run_id={run_id} evidence_run_id={evidence_run_id} history_fetch_started_at={fetch_started}"
        )
        securities = conn.execute(
            "SELECT tax_security_id, instrument_ids, status FROM tax_matching_securities WHERE matching_run_id = %s "
            "ORDER BY tax_security_id",
            (run_id,),
        ).fetchall()
        disposals = conn.execute(
            "SELECT tax_security_id, uk_date, gain_gbp, status FROM tax_statutory_disposals WHERE matching_run_id = %s "
            "ORDER BY tax_security_id, uk_date",
            (run_id,),
        ).fetchall()
        members = sorted({i for _s, ids, _st in securities for i in ids})
        first_acq = dict(
            conn.execute(
                "SELECT i.tax_security_id, min(l.uk_date) FROM tax_evidence_lots l "
                "JOIN tax_matching_instruments i ON i.matching_run_id = %s AND i.instrument_id = l.instrument_id "
                "WHERE l.run_id = %s AND l.direction = 'acquisition' GROUP BY i.tax_security_id",
                (run_id, evidence_run_id),
            ).fetchall()
        )
        rows = conn.execute(
            "SELECT instrument_id, symbol, instrument_type_id FROM instruments WHERE instrument_id = ANY(%s)",
            (members,),
        ).fetchall()
        inst = {r[0]: (r[1], r[2]) for r in rows}
        ids = conn.execute(
            "SELECT instrument_id, provider, identifier_type, identifier_value, is_primary FROM external_identifiers "
            "WHERE instrument_id = ANY(%s) AND identifier_type IN ('cik', 'cusip') ORDER BY 1, 3, 4",
            (members,),
        ).fetchall()
    ciks: dict[int, set[str]] = {}
    cusips: dict[int, set[str]] = {}
    for iid, provider, kind, value, primary in ids:
        print(f"[identifier] instrument={iid} provider={provider} type={kind} value={value} primary={primary}")
        if kind == "cik" and primary:
            ciks.setdefault(iid, set()).add(value)
        if kind == "cusip":
            cusips.setdefault(iid, set()).add(value.upper())

    client = httpx.Client(timeout=60, follow_redirects=False)
    subs: dict[str, dict[str, Any]] = {}
    for cik in sorted({c for cs in ciks.values() for c in cs}):
        r = client.get(SEC_SUBMISSIONS_URL.format(cik=cik), headers={"User-Agent": settings.sec_user_agent})
        sha = _save(out, f"sec_submissions_{cik}", r.content)
        doc = r.json() if r.status_code == 200 else {}
        subs[cik] = doc
        forms = doc.get("filings", {}).get("recent", {}).get("form", [])
        print(
            f"[sec {cik}] http={r.status_code} sha256={sha} name={doc.get('name')!r} "
            f"entityType={doc.get('entityType')!r} "
            f"tickers={doc.get('tickers')} 10-K={forms.count('10-K')} N-CEN={forms.count('N-CEN')}"
        )
    ua = {"User-Agent": "eBull/1.0 (+https://github.com/Luke-Bradford/eBull)"}
    content = client.get(HMRC_CONTENT_URL, headers=ua)
    _save(out, "hmrc_content_item", content.content)
    item = content.json()
    attachments = [
        a for a in item.get("details", {}).get("attachments", []) if str(a.get("url", "")).lower().endswith(".ods")
    ]
    print(
        f"[hmrc content] http={content.status_code} public_updated_at={item.get('public_updated_at')} "
        f"ods={len(attachments)}"
    )
    if len(attachments) != 1:
        print("hmrc: not exactly one .ods attachment; every fund is unestablished")
        list_rows: list[list[str]] = []
        list_date = None
    else:
        r = client.get(attachments[0]["url"], headers=ua)
        sha = _save(out, "hmrc_weblist_ods", r.content)
        list_date = datetime.now(UTC).date()
        all_rows = list(ods_rows(r.content))
        header, list_rows = all_rows[0], all_rows[1:]
        print(f"[hmrc list] url={attachments[0]['url']} http={r.status_code} sha256={sha} rows={len(list_rows)}")
        print(f"[hmrc list] header_matches={tuple(header) == LIST_HEADERS} header={header}")

    gains = {(s, d): (g, st) for s, d, g, st in disposals}
    for sec_id, member_ids, status in securities:
        print(f"\n== security {sec_id} ({', '.join(str(inst.get(i)) for i in member_ids)}) matching status={status}")
        types = {inst[i][1] for i in member_ids if i in inst}
        sec_ciks = {c for i in member_ids for c in ciks.get(i, set())}
        sec_cusips = {c for i in member_ids for c in cusips.get(i, set())}
        print(
            f"  types={sorted(types)} ciks={sorted(sec_ciks)} cusips={sorted(sec_cusips)} "
            f"first_acq={first_acq.get(sec_id)}"
        )
        character, detail = "unestablished", ""
        doc = subs.get(next(iter(sec_ciks))) if len(sec_ciks) == 1 else None
        forms = doc.get("filings", {}).get("recent", {}).get("form", []) if doc else []
        if types == {STOCKS_TYPE_ID}:
            if doc is None:
                detail = "cik_unestablished"
            elif doc.get("entityType") != "operating":
                detail = f"entity_type={doc.get('entityType')}"
            elif "10-K" not in forms:
                detail = "no_10k"
            elif not corporate_suffix(str(doc.get("name", ""))):
                detail = f"legal_form_unestablished name={doc.get('name')!r}"
            else:
                character, detail = "share", f"name={doc.get('name')!r}"
        elif types == {ETF_TYPE_ID}:
            if len(sec_cusips) != 1:
                detail = "security_identifier_unestablished"
            elif list_date is None:
                detail = "reporting_list_unavailable"
            else:
                cusip = next(iter(sec_cusips))
                hits = [
                    row
                    for row in list_rows
                    if len(row) >= 9
                    and (row[6].upper() == cusip or (row[5][:2] in ("US", "CA") and row[5][2:11].upper() == cusip))
                ]
                print(f"  list rows for {cusip} (ISIN US{cusip}{cusip_check_digit(cusip, 'US')}): {len(hits)}")
                periods = [(parse_list_date(h[7]), parse_list_date(h[8])) for h in hits]
                for h, p in zip(hits, periods, strict=True):
                    print(f"    {h[3]} {h[4]!r} isin={h[5]} cusip={h[6]} from={p[0]} ceased={p[1]}")
                start = first_acq.get(sec_id)
                ceased = [b for a, b in periods if isinstance(a, date) and not isinstance(b, str)]
                if len(ceased) != len(periods):
                    detail = "reporting_status_unparseable"
                elif ceased and (start is None or any(b is None or b >= start for b in ceased)):
                    character, detail = "reporting_fund_in_holding", "withheld: reportable_income_unestablished"
                elif doc is None or doc.get("entityType") not in ("other", "investment") or "N-CEN" not in forms:
                    detail = "offshore_fund_unestablished"
                else:
                    character = "non_reporting_offshore_fund"
        else:
            detail = f"instrument_type={sorted(types)}"
        print(f"  character={character} {detail}")
        for (s, d), (g, st) in gains.items():
            if s != sec_id:
                continue
            if character == "share":
                print(f"  disposal {d}: capital gain {g} ({st})")
            elif character == "non_reporting_offshore_fund" and g is not None:
                oig = g if g > 0 else Decimal(0)
                print(f"  disposal {d}: OIG {oig}, chargeable {g - oig} ({st})")
            else:
                print(f"  disposal {d}: withheld under character ({st})")
    print(f"\nbodies: {out}")
    sys.stdout.flush()


def universe(archive: Path) -> None:
    """Apply the ``share`` rule to every tradable Stocks instrument with a primary SEC CIK, from a local copy of SEC's
    nightly ``submissions.zip`` (the archive ``sec_submissions_bulk_refresh`` keeps), and count the outcomes."""
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        rows = conn.execute(
            "SELECT i.instrument_id, i.symbol, e.identifier_value FROM instruments i "
            "JOIN external_identifiers e ON e.instrument_id = i.instrument_id AND e.provider = 'sec' "
            "AND e.identifier_type = 'cik' AND e.is_primary "
            "WHERE i.instrument_type_id = %s AND i.is_tradable AND i.canonical_instrument_id IS NULL",
            (STOCKS_TYPE_ID,),
        ).fetchall()
    outcomes: dict[str, list[str]] = {}
    with zipfile.ZipFile(archive) as z:
        names = set(z.namelist())
        for _iid, symbol, cik in rows:
            member = f"CIK{cik}.json"
            if member not in names:
                outcomes.setdefault("cik_not_in_archive", []).append(symbol)
                continue
            doc = json.loads(z.read(member))
            forms = doc.get("filings", {}).get("recent", {}).get("form", [])
            if doc.get("entityType") != "operating":
                key = f"entity_type={doc.get('entityType')}"
            elif "10-K" not in forms:
                key = "no_10k"
            elif not corporate_suffix(str(doc.get("name", ""))):
                key = "legal_form_unestablished"
            else:
                key = "share"
            outcomes.setdefault(key, []).append(f"{symbol}|{doc.get('name')}")
    print(f"[universe] archive={archive} instruments={len(rows)}")
    for key, items in sorted(outcomes.items(), key=lambda kv: -len(kv[1])):
        print(f"  {key}: {len(items)}")
        if key != "share":
            for item in sorted(items):
                print(f"    {item}")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--universe":
        universe(Path(sys.argv[2]))
    else:
        main()
