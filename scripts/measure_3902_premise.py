"""#3902 premise: why the #3841 PIT build refuses whole CIKs, measured over its full scope.

Read-only. For every in-scope instrument (``pit_market_cap_build.load_scope``) it parses the primary CIK's
submissions with the frozen ``pit_fundamentals.parse_submissions`` and prints, per refused CIK:

1. ``submissions_accession_conflict``: every conflicting accession (form and both records), and whether any of the
   instrument's share-count or public-float facts (the build's ``_FACTS_SQL`` rows) cites one;
2. ``untimed_accession``: every share-fact accession the primary index cannot time, its filer prefix, and which of
   the instrument's other SEC CIKs (``external_identifiers``, non-primary) lists it;
3. the in-scope instruments holding a non-primary SEC CIK, and how many each holds;
4. the in-scope instruments with an ``ads_ratio`` row.

    PYTHONPATH=. uv run python scripts/measure_3902_premise.py
"""

from __future__ import annotations

import json
import zipfile
from collections import Counter, defaultdict
from typing import Any

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.factor_panel import MAX_SHARES_AGE_MONTHS, add_months
from app.services.pit_fundamentals import REQUIRED_SUBMISSION_COLUMNS, submissions_reader
from app.services.pit_market_cap_build import _FACTS_SQL, BALANCE_SHEET, COVER, FIRST_SESSION, load_scope

_OTHER_CIKS = """
    SELECT instrument_id, identifier_value FROM external_identifiers
    WHERE provider = 'sec' AND identifier_type = 'cik' AND NOT is_primary AND instrument_id = ANY(%(ids)s::bigint[])
"""
_SYMBOLS = "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%(ids)s::bigint[])"
_ADS = "SELECT a.instrument_id, a.ratio FROM ads_ratio a WHERE a.instrument_id = ANY(%(ids)s::bigint[])"


def _conflicts(archive: zipfile.ZipFile, cik: str) -> dict[str, set[tuple[Any, ...]]]:
    """Every accession listed with more than one distinct (acceptance, form, items) record across the CIK's pages."""
    main = json.loads(archive.read(f"CIK{cik}.json"))
    blocks = [main["filings"]["recent"]] + [json.loads(archive.read(p["name"])) for p in main["filings"]["files"]]
    records: dict[str, set[tuple[Any, ...]]] = defaultdict(set)
    for block in blocks:
        for accn, accepted, form, items in zip(*(block[c] for c in REQUIRED_SUBMISSION_COLUMNS), strict=True):
            records[accn].add((accepted, form, items))
    return {accn: recs for accn, recs in records.items() if len(recs) > 1}


def main() -> None:
    archive_path = resolve_data_dir() / "sec" / "bulk" / "submissions.zip"
    with psycopg.connect(settings.database_url) as conn, zipfile.ZipFile(archive_path) as archive:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        scope = load_scope(conn)
        ids = [iid for iid, _ in scope]
        symbols = dict(conn.execute(_SYMBOLS, {"ids": ids}).fetchall())
        others: dict[int, list[str]] = defaultdict(list)
        for iid, cik in conn.execute(_OTHER_CIKS, {"ids": ids}):
            others[iid].append(f"{int(cik):010d}")
        accessions: dict[int, set[str]] = defaultdict(set)
        float_accessions: dict[int, set[str]] = defaultdict(set)
        min_context = add_months(FIRST_SESSION, -MAX_SHARES_AGE_MONTHS)
        for iid, taxonomy, concept, accession, _ctx, _val in conn.execute(
            _FACTS_SQL, {"ids": ids, "min_context": min_context}
        ):
            if (taxonomy, concept) in (COVER, BALANCE_SHEET):
                accessions[iid].add(accession)
            else:
                float_accessions[iid].add(accession)
        ads = dict(conn.execute(_ADS, {"ids": ids}).fetchall())

        index_of = submissions_reader(archive)
        tally: Counter[str] = Counter()
        print(f"scope {len(scope)} instruments; submissions.zip {archive_path}")
        for iid, cik in scope:
            index = index_of(cik)
            label = f"{symbols.get(iid)} iid={iid} cik={cik}"
            if index == "submissions_accession_conflict":
                conflicts = _conflicts(archive, cik)
                cited = sorted(set(conflicts) & accessions[iid])
                floats = sorted(set(conflicts) & float_accessions[iid])
                tally[f"conflict:{'fact_cites' if cited or floats else 'no_fact_cites'}"] += 1
                print(
                    f"\n[conflict] {label}: {len(conflicts)} conflicting accessions; "
                    f"share facts cite {cited}; float facts cite {floats}"
                )
                for accn, recs in sorted(conflicts.items()):
                    print(f"   {accn}: {sorted(recs, key=repr)}")
                continue
            if isinstance(index, str):
                tally[f"index:{index}"] += 1
                print(f"\n[index] {label}: {index}")
                continue
            untimed = sorted(
                a for a in accessions[iid] if a not in index.filings or index.filings[a].acceptance is None
            )
            if not untimed:
                tally["timed"] += 1
                continue
            listed: dict[str, list[str]] = defaultdict(list)
            for other in others[iid]:
                other_index = index_of(other)
                if isinstance(other_index, str):
                    print(f"   other cik {other}: {other_index}")
                    continue
                for accn in untimed:
                    filing = other_index.filings.get(accn)
                    if filing is not None and filing.acceptance is not None:
                        listed[accn].append(other)
            unresolved = [a for a in untimed if a not in listed]
            tally[f"untimed:{'all_listed_by_other_cik' if not unresolved else 'some_unlisted'}"] += 1
            print(f"\n[untimed] {label}: other ciks {others[iid]}; {len(untimed)} untimed, {len(unresolved)} unlisted")
            for accn in untimed:
                print(f"   {accn} prefix {accn[:10]} listed by {listed.get(accn, [])}")
        print("\nnon-primary SEC CIKs per instrument:", dict(Counter(len(v) for v in others.values())))
        print("\nads_ratio in scope:", {symbols.get(i): str(r) for i, r in ads.items()})
        print("\ntally:", dict(sorted(tally.items())))


if __name__ == "__main__":
    main()
