"""#3740 slice F: EXPLORATORY pilot of EDGAR dividend extraction against Intrader's event columns.

Spec: ``docs/research/2026-10-10-3740-slice-f-dividend-measurement.md`` (the round-8 decision paragraph). The output
is exploratory and decides nothing: it exists to count, on real filings, the extraction failure classes the v3
contract must handle. The cohort is names ranked 1,001-1,100 by ME at the stage-B 2023-09-30 formation, disjoint
from the decisive top 1,000 (ranks 1-1,000), which this script never reads past ranking. Like
``probe_3740_slice_f_dividend_reference`` it reads only ``M``, ``exclusion``, ``me``, ``name_key``, ``series_id`` and
``cik`` of the panel rows.

Two phases:

- ``fetch``: for each cohort CIK, the submissions JSON (and continuation pages), then every accession of the forms in
  ``FORMS`` accepted in [2023-01-01, 2025-01-01) ET: its ``index.json`` and each HTML/text document. Bytes are cached
  under ``CACHE``; a cached file is never refetched. SEC traffic goes through the cross-process ``PostgresFloorGate``
  (the jobs daemon's budget), as ``measure_3518_mdna_extraction_census`` does.
- ``score``: offline. Splits each document into sentences, keeps dividend candidates (a dividend/distribution word,
  a money amount, "per share"), reads record/ex/payable dates from the same sentence, derives an ex-date from a
  record date by the Nasdaq/FINRA 11140 table, and compares with Intrader's (series, ex-date, amount) events in the
  window 2023-10-02..2024-08-30.

Usage: ``PYTHONPATH=. uv run python -m scripts.pilot_3740_slice_f_dividend_extraction fetch|score``
"""

from __future__ import annotations

import gzip
import hashlib
import html
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg

from app.config import settings
from app.services.market_calendar import us_market_status

ARTEFACT = (
    Path.home()
    / "Library/Application Support/eBull/research/factor_panel_3609"
    / "2026-10-09-25577cff-stageB-1c5de22cccda4937b55eb7e16e505b7f"
)
FORMATION = "2023-09-30"
RANKS = (1000, 1100)  # zero-based slice: ranks 1,001-1,100
CACHE = Path("var/research/3740_slicef_pilot")
FORMS = frozenset({"8-K", "8-K/A", "10-Q", "10-Q/A", "10-K", "10-K/A", "6-K", "20-F", "40-F"})
ACCEPTED_FROM, ACCEPTED_TO = "2023-01-01", "2025-01-01"
EX_FIRST, EX_LAST = date(2023, 10, 2), date(2024, 8, 30)
MIRROR = Path.home() / "Dev/eBull/var/research_corpus/mirrors/icyDenev_Intrader/Data/Day"

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_DATE = rf"(?:{_MONTHS})\.?\s+\d{{1,2}},?\s+\d{{4}}"
_TAG = re.compile(r"<[^>]+>")
_ENTITY = re.compile(r"&nbsp;|&#160;|&#xa0;|\xa0", re.IGNORECASE)
_SPACE = re.compile(r"\s+")
_SENTENCE = re.compile(r"(?<=[.;])\s+(?=[A-Z(])")
_CONTEXT = re.compile(r"\b(dividends?|distributions?)\b", re.IGNORECASE)
_PER_SHARE = re.compile(r"per\s+(?:common\s+|ordinary\s+)?share", re.IGNORECASE)
_AMOUNT = re.compile(r"\$\s?(\d*\.\d+|\d+(?:\.\d+)?)|(\d+(?:\.\d+)?)\s+cents", re.IGNORECASE)
_RECORD = re.compile(rf"(?:of\s+record|record\s+date)[^.;]{{0,60}}?({_DATE})", re.IGNORECASE)
_EX = re.compile(rf"ex-?dividend\s+date[^.;]{{0,40}}?({_DATE})", re.IGNORECASE)
_TO_AMOUNT = re.compile(r"\bto\s+\$\s?(\d*\.\d+|\d+(?:\.\d+)?)")
_PAYABLE = re.compile(rf"(?:payable|paid|payment\s+date)[^.;]{{0,30}}?({_DATE})", re.IGNORECASE)


def _cohort() -> list[dict[str, object]]:
    admitted: list[tuple[float, int, int, str]] = []
    with gzip.open(ARTEFACT / "rows.jsonl.gz", "rt") as handle:
        for line in handle:
            row = json.loads(line)
            if row["M"] == FORMATION and row["exclusion"] is None:
                admitted.append((float(row["me"]["value"]), row["name_key"], row["series_id"], row["cik"]))
    admitted.sort(key=lambda t: (-t[0], t[1]))
    return [
        {"rank": RANKS[0] + i + 1, "series_id": s, "cik": c} for i, (_, _, s, c) in enumerate(admitted[slice(*RANKS)])
    ]


def _accessions(sec: object, cik: str) -> list[tuple[str, str, str]]:
    blocks: list[dict[str, list[str]]] = []
    root = sec.fetch_submissions(cik)  # type: ignore[attr-defined]
    if root is None:
        return []
    blocks.append(root["filings"]["recent"])
    for page in root["filings"].get("files", []):
        extra = sec.fetch_submissions_page(page["name"])  # type: ignore[attr-defined]
        if extra is not None:
            blocks.append(extra)
    out = []
    for block in blocks:
        for accn, form, accepted in zip(block["accessionNumber"], block["form"], block["acceptanceDateTime"]):
            if form in FORMS and ACCEPTED_FROM <= accepted[:10] < ACCEPTED_TO:
                out.append((accn, form, accepted))
    return sorted(set(out), key=lambda t: (t[2], t[0]))


def fetch(limit: int | None = None) -> None:
    from psycopg_pool import ConnectionPool

    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.providers.postgres_rate_gate import PostgresFloorGate
    from app.providers.rate_gate import SEC_MIN_REQUEST_INTERVAL_S
    from app.providers.sec_rate_gate_holder import set_sec_rate_gate

    pool = ConnectionPool(settings.database_url, min_size=1, max_size=1, kwargs={"application_name": "pilot-3740"})
    set_sec_rate_gate(PostgresFloorGate(pool, budget="sec", floor_s=SEC_MIN_REQUEST_INTERVAL_S))
    CACHE.mkdir(parents=True, exist_ok=True)
    cohort = _cohort()
    (CACHE / "cohort.json").write_text(json.dumps(cohort))
    cohort = cohort[:limit]
    with SecFilingsProvider(user_agent=settings.sec_user_agent) as sec:
        for member in cohort:
            cik = str(member["cik"])
            listing_path = CACHE / f"{cik}.accessions.json"
            if not listing_path.exists():
                listing_path.write_text(json.dumps(_accessions(sec, cik)))
            for accn, _form, _accepted in json.loads(listing_path.read_text()):
                folder = CACHE / cik / accn
                if (folder / "index.json").exists():
                    continue
                index = sec.fetch_filing_index(accn, issuer_cik=cik)
                if index is None:
                    continue
                folder.mkdir(parents=True, exist_ok=True)
                base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accn.replace('-', '')}/"
                for item in index["directory"]["item"]:  # type: ignore[index]
                    name = item["name"]
                    if not name.lower().endswith((".htm", ".html", ".txt")) or name.endswith("-index.htm"):
                        continue
                    # R*.htm are XBRL viewer pages; <accession>.txt is the complete submission (every document again).
                    if re.fullmatch(r"R\d+\.htm", name) or name.startswith("Financial_Report") or name == f"{accn}.txt":
                        continue
                    body = sec.fetch_document_text(base + name)
                    if body is not None:
                        (folder / name).write_text(body)
                (folder / "index.json").write_text(json.dumps(index))
            print(f"rank {member['rank']} cik {cik}: done", flush=True)
    pool.close()


def _text(raw: str) -> str:
    return _SPACE.sub(" ", html.unescape(_ENTITY.sub(" ", _TAG.sub(" ", raw)))).strip()


def _day(raw: str | None) -> date | None:
    if raw is None:
        return None
    cleaned = raw.replace(".", "").replace(",", "")
    try:
        return datetime.strptime(_SPACE.sub(" ", cleaned), "%B %d %Y").date()
    except ValueError:
        return None


def _session_before(d: date) -> date:
    d -= timedelta(days=1)
    while us_market_status(d) == "closed":
        d -= timedelta(days=1)
    return d


def derive_ex(record: date) -> date | None:
    """Nasdaq ETA2024-29 / FINRA 11140(b)(1) for a < 25% distribution; None for a non-session record date."""
    if record == date(2024, 5, 28):
        return date(2024, 5, 24)
    if us_market_status(record) == "closed":
        return None
    return record if record >= date(2024, 5, 29) else _session_before(record)


def _first_day(pattern: re.Pattern[str], *texts: str) -> tuple[date | None, int]:
    """The pattern's date in the first text that has one, and that text's index (-1 for none)."""
    for i, text in enumerate(texts):
        if (m := pattern.search(text)) is not None:
            return _day(m.group(1)), i
    return None, -1


def candidates(text: str) -> list[dict[str, object]]:
    out = []
    sentences = _SENTENCE.split(text)
    for i, sentence in enumerate(sentences):
        if not (_CONTEXT.search(sentence) and _PER_SHARE.search(sentence)):
            continue
        amounts = []
        for m in _AMOUNT.finditer(sentence):
            try:
                amounts.append(Decimal(m.group(1)) if m.group(1) else Decimal(m.group(2)) / 100)
            except InvalidOperation:
                continue
        if not amounts:
            continue
        # "increase from $X to $Y" / "to $Y from $X": the declared amount is the one after "to".
        if len(amounts) > 1 and re.search(r"\bfrom\b", sentence) and (m := _TO_AMOUNT.search(sentence)):
            amounts = [Decimal(m.group(1))]
        # Dates may sit in the declaring sentence or the next one ("... will be paid on X to holders of record on Y").
        pair = (sentence, sentences[i + 1] if i + 1 < len(sentences) else "")
        record, where_record = _first_day(_RECORD, *pair)
        ex, where_ex = _first_day(_EX, *pair)
        pay, _ = _first_day(_PAYABLE, *pair)
        out.append(
            {
                "amounts": amounts,
                "record": record,
                "ex": ex,
                "pay": pay,
                "next_sentence_date": max(where_record, where_ex) == 1,
                "sentence": sentence[:400],
            }
        )
    return out


def _reference(cohort: list[dict[str, object]]) -> dict[int, dict[date, Decimal]]:
    ids = [int(str(m["series_id"])) for m in cohort]
    with psycopg.connect(settings.database_url) as conn:
        symbols = dict(
            conn.execute(
                "select series_id, vendor_symbol from research_price_series where series_id = any(%s)", (ids,)
            ).fetchall()
        )
    events: dict[int, dict[date, Decimal]] = defaultdict(dict)
    for series, symbol in symbols.items():
        path = MIRROR / f"{symbol}.csv"
        if not path.exists():
            raise SystemExit(f"no mirror file for series {series} ({symbol})")
        for line in path.read_text().splitlines():
            fields = line.split(",")
            day = date.fromisoformat(fields[0])
            if EX_FIRST <= day <= EX_LAST and Decimal(fields[7]) != 0:
                events[series][day] = events[series].get(day, Decimal(0)) + Decimal(fields[7])
    return events


def score() -> None:
    cohort = json.loads((CACHE / "cohort.json").read_text())
    reference = _reference(cohort)
    tally: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    extracted_keys: set[tuple[int, date]] = set()
    for member in cohort:
        cik, series = str(member["cik"]), int(member["series_id"])
        if not (CACHE / f"{cik}.accessions.json").exists():
            tally["name:not_fetched"] += 1
            continue
        tally["name:scored"] += 1
        found: list[tuple[str, dict[str, object]]] = []
        for doc in sorted((CACHE / cik).glob("*/*")):
            if doc.name == "index.json":
                continue
            listing = {a: (f, t) for a, f, t in json.loads((CACHE / f"{cik}.accessions.json").read_text())}
            accepted = listing[doc.parent.name][1]
            found += [(accepted, c) for c in candidates(_text(doc.read_text(errors="replace")))]
        derived: dict[date, set[Decimal]] = defaultdict(set)
        for _accepted, c in found:
            ex = c["ex"] or (derive_ex(c["record"]) if c["record"] else None)  # type: ignore[arg-type]
            tally["candidate:" + ("E1" if c["ex"] else "E2" if c["record"] else "no_date")] += 1
            if c["next_sentence_date"]:
                tally["candidate:date_in_next_sentence"] += 1
            if len(c["amounts"]) > 1:  # type: ignore[arg-type]
                tally["candidate:multi_amount"] += 1
            if isinstance(ex, date) and len(c["amounts"]) == 1:  # type: ignore[arg-type]
                derived[ex].add(c["amounts"][0])  # type: ignore[index]
        for ex, amounts in derived.items():
            if EX_FIRST <= ex <= EX_LAST:
                extracted_keys.add((series, ex))
                if ex not in reference.get(series, {}):
                    tally["key:route_only"] += 1
                    examples["route_only"].append(f"{cik} {ex} {sorted(amounts)}")
        all_amounts = {a for _, c in found for a in c["amounts"]}  # type: ignore[attr-defined]
        for ex, amount in reference.get(series, {}).items():
            tally["ref:event"] += 1
            got = derived.get(ex, set())
            if any(a.quantize(Decimal("0.0001")) == amount.quantize(Decimal("0.0001")) for a in got):
                tally["ref:matched"] += 1
            elif got:
                tally["ref:amount_conflict"] += 1
                examples["amount_conflict"].append(f"{cik} {ex} ref {amount} got {sorted(got)}")
            elif any(a.quantize(Decimal("0.0001")) == amount.quantize(Decimal("0.0001")) for a in all_amounts):
                tally["ref:amount_seen_date_unresolved"] += 1
                examples["date_unresolved"].append(f"{cik} {ex} {amount}")
            else:
                tally["ref:not_found"] += 1
                examples["not_found"].append(f"{cik} {ex} {amount}")
    print(json.dumps(dict(sorted(tally.items())), indent=1))
    for kind, rows in examples.items():
        print(f"--- {kind} ({len(rows)}), first 12")
        print("\n".join(rows[:12]))
    digest = hashlib.sha256(json.dumps(dict(sorted(tally.items()))).encode()).hexdigest()
    print(f"tally sha256 {digest}")


if __name__ == "__main__":
    if sys.argv[1] == "fetch":
        fetch(int(sys.argv[2]) if len(sys.argv) > 2 else None)
    else:
        score()
