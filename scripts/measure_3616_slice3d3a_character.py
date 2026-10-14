"""#3616 slice 3d-3a fact-finding: the tax character each security of the effective matching run would get.

Read-only. Resolves the effective matching run with ``effective_tax_matching`` (3d-2 Design 9). Then, in one
REPEATABLE READ transaction on the dev DB, reads that run's rows (sealed, so immutable): securities, disposals,
identifications, dependencies, pools, instrument placements and each security's first admitted acquisition date; and
the mutable inputs: each member's symbol, ``instrument_type_id`` and primary SEC CIK (``external_identifiers``,
provider ``sec``, type ``cik``). After the transaction it calls ``effective_tax_matching`` again and stops if the run
changed. It then fetches, with User-Agent ``settings.sec_user_agent`` (public, no account): each CIK's submissions JSON
(``data.sec.gov/submissions/CIK##########.json``) and the two cover filings Design 2 names (the latest 10-K, 10-Q or
8-K with an inline-XBRL cover, overall and on or before the first acquisition), and applies ``classify``, the spec's
``tax_character_v1`` table, printing per security every input, the character and the run's figures. Bodies go to a
new directory under ``var/research/3616/slice3d3a/`` with their SHA-256; the printout carries dates, amounts, symbols,
CIKs and accession numbers, never an account identifier.

``--universe ARCHIVE`` applies the submissions-level lines of ``classify`` (entity type, SIC) to every Stocks
instrument with a primary SEC CIK (tradable or not, variants included), from a local copy of SEC's nightly
``submissions.zip``, and counts outcomes, with the annual-report forms of the ``other`` ones. Cover filings are not
fetched for the universe, so it measures coverage of those lines only.

    uv run python -m scripts.measure_3616_slice3d3a_character
    uv run python -m scripts.measure_3616_slice3d3a_character --universe <submissions.zip>
"""

from __future__ import annotations

import gzip
import hashlib
import html
import json
import re
import sys
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg

from app.config import settings
from app.services.tax_matching import effective_tax_matching

SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"
STOCKS_TYPE_ID = 5
ETF_TYPE_ID = 6
INVESTMENT_SIC = frozenset({"6722", "6726", "6770", "6799"})
COVER_FORMS = frozenset({"10-K", "10-Q", "8-K"})
_SIC = re.compile(r"^\d{4}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_COMMON = re.compile(r"^(CLASS [A-Z] )?(COMMON STOCK|ORDINARY SHARES)(,| |$)")
_NON_NUMERIC = re.compile(r"<ix:nonNumeric\b([^>]*)>(.*?)</ix:nonNumeric>", re.S | re.I)
_ATTR = re.compile(r'\b(name|contextRef)="([^"]*)"')
_TAG = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class Filing:
    accession: str
    form: str
    filing_date: date
    document: str


@dataclass(frozen=True)
class SecFacts:
    entity_type: str
    sic: str
    covers: tuple[Filing, ...]  # inline-XBRL 10-K/10-Q/8-K, newest first


def sec_facts(doc: Any) -> SecFacts | None:
    """The fields Design 2 reads, validated; None when any is missing or ill-typed (``sec_capture_invalid``)."""
    if not isinstance(doc, dict):
        return None
    entity_type, sic = doc.get("entityType"), doc.get("sic")
    recent = doc.get("filings", {}).get("recent") if isinstance(doc.get("filings"), dict) else None
    if not isinstance(entity_type, str) or not isinstance(sic, str) or not isinstance(recent, dict):
        return None
    keys = ("accessionNumber", "form", "filingDate", "primaryDocument", "isInlineXBRL")
    raw = [recent.get(k) for k in keys]
    cols = [c for c in raw if isinstance(c, list)]
    if len(cols) != len(keys) or len({len(c) for c in cols}) != 1:
        return None
    covers: list[Filing] = []
    for acc, form, filed, document, inline in zip(*cols, strict=True):
        if not (isinstance(acc, str) and isinstance(form, str) and isinstance(document, str)):
            return None
        if not (isinstance(filed, str) and _ISO_DATE.match(filed)) or inline not in (0, 1):
            return None
        if form in COVER_FORMS and inline == 1:
            covers.append(Filing(acc, form, date.fromisoformat(filed), document))
    covers.sort(key=lambda f: (f.filing_date, f.accession), reverse=True)
    return SecFacts(entity_type, sic.strip() if _SIC.match(sic.strip()) else sic, tuple(covers))


def cover_classes(body: str) -> dict[str, dict[str, str]]:
    """``dei:Security12bTitle`` and ``dei:TradingSymbol`` facts by ``contextRef``, as displayed text."""
    out: dict[str, dict[str, str]] = {}
    for m in _NON_NUMERIC.finditer(body):
        attrs = dict(_ATTR.findall(m.group(1)))
        name = attrs.get("name", "")
        if name in ("dei:Security12bTitle", "dei:TradingSymbol") and "contextRef" in attrs:
            text = " ".join(html.unescape(_TAG.sub("", m.group(2))).split())
            out.setdefault(attrs["contextRef"], {})[name.split(":")[1]] = text
    return out


def common_title(title: str | None) -> bool:
    return title is not None and _COMMON.match(title.upper()) is not None


def classify(
    types: set[int | None],
    ciks: set[str],
    symbol: str | None,
    facts: SecFacts | None,
    first_acq: date | None,
    covers: dict[str, dict[str, dict[str, str]]],
) -> tuple[str, str | None]:
    """``tax_character_v1``: (character, detail); detail is None for a share. ``covers`` maps accession → classes."""
    if len(types) != 1 or len(ciks) > 1:
        return "unestablished", "member_conflict"
    (kind,) = types
    if kind == ETF_TYPE_ID:
        return "unestablished", "exchange_traded_fund"
    if kind != STOCKS_TYPE_ID:
        return "unestablished", "instrument_type"
    if not ciks or symbol is None:
        return "unestablished", "cik_unestablished"
    if facts is None:
        return "unestablished", "sec_capture_unavailable"
    if facts.entity_type != "operating":
        return "unestablished", "entity_type"
    if not _SIC.match(facts.sic) or facts.sic in INVESTMENT_SIC:
        return "unestablished", "sic"
    if not facts.covers or facts.covers[0].accession not in covers:
        return "unestablished", "cover_unavailable"
    latest = [c for c in covers[facts.covers[0].accession].values() if c.get("TradingSymbol", "").upper() == symbol]
    if len(latest) != 1 or not common_title(latest[0].get("Security12bTitle")):
        return "unestablished", "listed_class_not_common_stock"
    if first_acq is not None:
        start = next((f for f in facts.covers if f.filing_date <= first_acq), None)
        if start is None or start.accession not in covers:
            return "unestablished", "holding_start_cover_unavailable"
        classes = covers[start.accession].values()
        paired = [c for c in classes if c.get("TradingSymbol", "").upper() == symbol]
        if not any(common_title(c.get("Security12bTitle")) for c in classes) or any(
            not common_title(c.get("Security12bTitle")) for c in paired
        ):
            return "unestablished", "holding_start_class_not_common_stock"
    return "share", None


def _fetch(client: httpx.Client, url: str, out: Path, name: str) -> tuple[int, bytes, str]:
    r = client.get(url, headers={"User-Agent": settings.sec_user_agent})
    sha = hashlib.sha256(r.content).hexdigest()
    (out / f"{name}.gz").write_bytes(gzip.compress(r.content))
    return r.status_code, r.content, sha


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
        print(f"[run] effective matching_run_id={run_id} evidence_run_id={evidence_run_id}")
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
        cik_rows = conn.execute(
            "SELECT instrument_id, identifier_value FROM external_identifiers WHERE instrument_id = ANY(%s) "
            "AND provider = 'sec' AND identifier_type = 'cik' AND is_primary",
            (members,),
        ).fetchall()
        conn.commit()
    with psycopg.connect(settings.database_url) as conn:
        again = effective_tax_matching(conn)
    if again is None or again.matching_run_id != run_id:
        print(f"effective run changed during the read ({run_id} → {again}); rerun")
        return
    ciks: dict[int, set[str]] = {}
    for iid, value in cik_rows:
        ciks.setdefault(iid, set()).add(value)
    client = httpx.Client(timeout=60, follow_redirects=False)
    facts: dict[str, SecFacts | None] = {}
    for cik in sorted({c for cs in ciks.values() for c in cs}):
        status, body, sha = _fetch(client, SEC_SUBMISSIONS_URL.format(cik=cik), out, f"sec_submissions_{cik}")
        doc = json.loads(body) if status == 200 else None
        ok = isinstance(doc, dict) and str(doc.get("cik", "")).zfill(10) == cik
        facts[cik] = sec_facts(doc) if ok else None
        f = facts[cik]
        print(
            f"[sec {cik}] http={status} sha256={sha} valid={f is not None} "
            f"entityType={f and f.entity_type} sic={f and f.sic}"
        )
    for sec_id, member_ids, status, pool_units, pool_cost, pool_reasons in securities:
        sec_types = {inst.get(i, (None, None))[1] for i in member_ids}
        sec_ciks = {c for i in member_ids for c in ciks.get(i, set())}
        base = [i for i in member_ids if i in {j for j, cs in ciks.items() if cs}]
        symbol = inst[base[0]][0].upper() if len(base) == 1 and base[0] in inst else None
        one = facts.get(next(iter(sec_ciks))) if len(sec_ciks) == 1 else None
        covers: dict[str, dict[str, dict[str, str]]] = {}
        if one is not None and one.covers:
            wanted = [one.covers[0]]
            start = first_acq.get(sec_id)
            if start is not None:
                wanted += [f for f in one.covers if f.filing_date <= start][:1]
            cik = next(iter(sec_ciks))
            for f in wanted:
                url = SEC_ARCHIVE_URL.format(cik=int(cik), accession=f.accession.replace("-", ""), document=f.document)
                code, body, sha = _fetch(client, url, out, f"cover_{f.accession}")
                if code == 200:
                    covers[f.accession] = cover_classes(body.decode("utf-8", errors="replace"))
                print(
                    f"  [cover {f.accession} {f.form} {f.filing_date}] http={code} sha256={sha} "
                    f"{covers.get(f.accession)}"
                )
        if set(member_ids) & unplaced:
            character, detail = "unestablished", "identity_unestablished"
        else:
            character, detail = classify(sec_types, sec_ciks, symbol, one, first_acq.get(sec_id), covers)
        print(f"== security {sec_id} members={member_ids} types={sorted(sec_types, key=str)} ciks={sorted(sec_ciks)}")
        print(f"  symbol={symbol} first_acq={first_acq.get(sec_id)} character={character} detail={detail}")
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
            "SELECT i.symbol, e.identifier_value FROM instruments i JOIN external_identifiers e "
            "ON e.instrument_id = i.instrument_id AND e.provider = 'sec' AND e.identifier_type = 'cik' "
            "AND e.is_primary WHERE i.instrument_type_id = %s",
            (STOCKS_TYPE_ID,),
        ).fetchall()
    outcomes: Counter[str] = Counter()
    other_forms: Counter[str] = Counter()
    sics: Counter[str] = Counter()
    with zipfile.ZipFile(archive) as z:
        names = set(z.namelist())
        for _symbol, cik in rows:
            member = f"CIK{cik}.json"
            if member not in names:
                outcomes["cik_not_in_archive"] += 1
                continue
            doc = json.loads(z.read(member))
            f = sec_facts(doc)
            if f is None:
                outcomes["sec_capture_invalid"] += 1
            elif f.entity_type != "operating":
                outcomes[f"entity_type={f.entity_type}"] += 1
                if f.entity_type == "other":
                    forms = set(doc["filings"]["recent"]["form"])
                    other_forms[next((x for x in ("20-F", "40-F", "10-K") if x in forms), "none")] += 1
            elif not _SIC.match(f.sic) or f.sic in INVESTMENT_SIC:
                outcomes["sic"] += 1
                sics[f.sic or "empty"] += 1
            elif not f.covers:
                outcomes["no_inline_cover_filing"] += 1
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
