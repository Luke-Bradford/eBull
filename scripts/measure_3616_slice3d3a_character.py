"""#3616 slice 3d-3a fact-finding: the tax character each security of the effective matching run would get.

Read-only. Resolves the effective matching run with ``effective_tax_matching`` (3d-2 Design 9), then reads, in one
REPEATABLE READ transaction on the dev DB: the run's securities, disposals, identifications, dependencies and pools;
the first admitted acquisition date of each security in the run's evidence generation; and per member instrument its
``instrument_type_id`` and its primary SEC CIK (``external_identifiers``, provider ``sec``, type ``cik``). It fetches
the SEC submissions JSON of each CIK (``https://data.sec.gov/submissions/CIK##########.json``, User-Agent
``settings.sec_user_agent``; no account, no key), applies the spec's ``tax_character_v1`` table (Design 2) through
``classify``, and prints per security every input it used, the character, and the run's figures the acceptance
table needs. Response bodies go to a new directory under ``var/research/3616/slice3d3a/`` with their SHA-256; the
printout carries dates, amounts, symbols and CIKs, never an account identifier.

``--universe ARCHIVE`` applies the same ``classify`` to every Stocks instrument with a primary SEC CIK (tradable or
not, variants included), from a local copy of SEC's nightly ``submissions.zip``, with the first acquisition date
unknown (current name only), and counts outcomes. It measures coverage, not correctness.

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
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg

from app.config import settings
from app.services.tax_matching import effective_tax_matching

SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
STOCKS_TYPE_ID = 5
ETF_TYPE_ID = 6
LEGAL_FORM_WORDS = frozenset(
    {"INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "LTD", "LIMITED", "PLC", "NV", "SA", "AG", "SE"}
)
INVESTMENT_SIC = frozenset({"6722", "6726", "6770", "6799"})
_SEC_ANNOTATION = re.compile(r"\s*[/\\][A-Z]{2,3}[/\\]?\s*$|\s*/NEW/?\s*$")


@dataclass(frozen=True)
class SecFacts:
    name: str
    entity_type: str | None
    sic: str | None
    has_10k: bool
    former_names: tuple[tuple[str, str | None, str | None], ...]


def sec_facts(doc: dict[str, Any]) -> SecFacts:
    forms = doc.get("filings", {}).get("recent", {}).get("form", [])
    former = tuple(
        (str(f.get("name", "")), f.get("from"), f.get("to")) for f in doc.get("formerNames", []) if isinstance(f, dict)
    )
    return SecFacts(str(doc.get("name", "")), doc.get("entityType"), doc.get("sic") or None, "10-K" in forms, former)


def legal_form_word(name: str) -> bool:
    """The name, after SEC's trailing state or ``/NEW/`` annotations are removed, ends in a corporate form word."""
    text = name.upper().strip()
    while (stripped := _SEC_ANNOTATION.sub("", text)) != text:
        text = stripped
    tokens = [t for t in re.split(r"[\s,]+", text) if t]
    return bool(tokens) and tokens[-1].replace(".", "") in LEGAL_FORM_WORDS


def _overlaps(start: date | None, frm: str | None, to: str | None) -> bool:
    """A former name's [from, to] meets [start, ∞); an unknown start or an unparseable bound counts as meeting it."""
    if start is None or to is None:
        return True
    try:
        return date.fromisoformat(to[:10]) >= start
    except ValueError:
        return True


def classify(types: set[int | None], ciks: set[str], facts: SecFacts | None, first_acq: date | None) -> tuple[str, str]:
    """``tax_character_v1``: (character, detail). ``facts`` is None when the single CIK has no fresh capture."""
    if len(types) != 1 or len(ciks) > 1:
        return "unestablished", "member_conflict"
    (kind,) = types
    if kind == ETF_TYPE_ID:
        return "unestablished", "exchange_traded_fund"
    if kind != STOCKS_TYPE_ID:
        return "unestablished", f"instrument_type={kind}"
    if not ciks:
        return "unestablished", "cik_unestablished"
    if facts is None:
        return "unestablished", "sec_capture_unavailable"
    if facts.entity_type != "operating":
        return "unestablished", f"entity_type={facts.entity_type}"
    if not facts.has_10k:
        return "unestablished", "no_10k"
    if facts.sic is None or facts.sic in INVESTMENT_SIC:
        return "unestablished", f"sic={facts.sic}"
    if not legal_form_word(facts.name):
        return "unestablished", "legal_form_unestablished"
    for name, frm, to in facts.former_names:
        if _overlaps(first_acq, frm, to) and not legal_form_word(name):
            return "unestablished", "legal_form_unestablished_in_holding"
    return "share", ""


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
        identifications = conn.execute(
            "SELECT tax_security_id, count(*) FROM tax_identifications WHERE matching_run_id = %s GROUP BY 1",
            (run_id,),
        ).fetchall()
        dependencies = conn.execute(
            "SELECT tax_security_id, reason, count(*) FROM tax_matching_dependencies WHERE matching_run_id = %s "
            "GROUP BY 1, 2 ORDER BY 1, 2",
            (run_id,),
        ).fetchall()
        first_acq: dict[int, date] = dict(
            conn.execute(
                "SELECT i.tax_security_id, min(l.uk_date) FROM tax_evidence_lots l "
                "JOIN tax_matching_instruments i ON i.matching_run_id = %s AND i.instrument_id = l.instrument_id "
                "WHERE l.run_id = %s AND l.direction = 'acquisition' GROUP BY 1",
                (run_id, evidence_run_id),
            ).fetchall()
        )
        members = sorted({i for row in securities for i in row[1]})
        types = dict(
            conn.execute(
                "SELECT instrument_id, instrument_type_id FROM instruments WHERE instrument_id = ANY(%s)", (members,)
            ).fetchall()
        )
        cik_rows = conn.execute(
            "SELECT instrument_id, identifier_value FROM external_identifiers WHERE instrument_id = ANY(%s) "
            "AND provider = 'sec' AND identifier_type = 'cik' AND is_primary",
            (members,),
        ).fetchall()
    ciks: dict[int, set[str]] = {}
    for iid, value in cik_rows:
        ciks.setdefault(iid, set()).add(value)
    client = httpx.Client(timeout=60, follow_redirects=False)
    facts: dict[str, SecFacts] = {}
    for cik in sorted({c for cs in ciks.values() for c in cs}):
        r = client.get(SEC_SUBMISSIONS_URL.format(cik=cik), headers={"User-Agent": settings.sec_user_agent})
        sha = hashlib.sha256(r.content).hexdigest()
        (out / f"sec_submissions_{cik}.json.gz").write_bytes(gzip.compress(r.content))
        doc = r.json() if r.status_code == 200 else None
        if isinstance(doc, dict) and str(doc.get("cik", "")).zfill(10) == cik:
            facts[cik] = sec_facts(doc)
        print(f"[sec {cik}] http={r.status_code} sha256={sha} facts={facts.get(cik)}")
    idents = dict(identifications)
    for sec_id, member_ids, status, pool_units, pool_cost, pool_reasons in securities:
        sec_types = {types.get(i) for i in member_ids}
        sec_ciks = {c for i in member_ids for c in ciks.get(i, set())}
        one = facts.get(next(iter(sec_ciks))) if len(sec_ciks) == 1 else None
        character, detail = classify(sec_types, sec_ciks, one, first_acq.get(sec_id))
        print(f"\n== security {sec_id} members={member_ids} types={sorted(sec_types, key=str)} ciks={sorted(sec_ciks)}")
        print(f"  first_acq={first_acq.get(sec_id)} character={character} {detail}")
        print(f"  run 1: status={status} pool={pool_units} / {pool_cost} reasons={pool_reasons}")
        print(f"  identifications={idents.get(sec_id, 0)}")
        for s, d, ty, gain, st, reasons in disposals:
            if s == sec_id:
                print(f"  disposal {d} {ty} gain={gain} status={st} reasons={reasons}")
        for s, reason, n in dependencies:
            if s == sec_id:
                print(f"  dependency {reason} x{n}")
    print(f"\nbodies: {out}")


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
    unestablished: list[str] = []
    with zipfile.ZipFile(archive) as z:
        names = set(z.namelist())
        for symbol, cik in rows:
            member = f"CIK{cik}.json"
            if member not in names:
                outcomes["cik_not_in_archive"] += 1
                continue
            f = sec_facts(json.loads(z.read(member)))
            character, detail = classify({STOCKS_TYPE_ID}, {cik}, f, None)
            key = character if character == "share" else detail.split("=")[0] if "=" in detail else detail
            outcomes[key if key != "entity_type" else f"entity_type={f.entity_type}"] += 1
            if character != "share" and detail.startswith(("sic", "legal_form")):
                unestablished.append(f"{detail}|{symbol}|{f.name}")
    print(f"[universe] archive={archive} instruments={len(rows)}")
    for key, n in outcomes.most_common():
        print(f"  {key}: {n}")
    for line in sorted(unestablished):
        print(f"    {line}")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--universe":
        universe(Path(sys.argv[2]))
    else:
        main()
