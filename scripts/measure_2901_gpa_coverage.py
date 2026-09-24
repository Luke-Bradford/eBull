"""#2901 premise measurement: GP/A input coverage on the linked Intrader population.

Spec: ``docs/proposals/ta/2026-09-24-2901-quality-arm.md`` ("Premise measurements"). Descriptive
only: it reads no price at all. Every input is a linkage or fundamentals read at the formation date.

For each June formation D (``census_3360_pit_fundamentals.formation_dates``) it takes every
in-scope Intrader series whose ``link_as_of(S, D)`` is ``linked`` with ``plain``/``class`` grammar
(so D lies inside the series' bar range), de-duplicates to CIKs, and walks each CIK down the spec's
ladder (construction rules 3-6), counting the first rung it fails. Field statuses are also counted
independently of the ladder. It stops before the security choice and executability (rules 2 and
the X(D) bar), so its eligible counts are an UPPER bound on the production E(D).

Usage::

    PYTHONPATH=. uv run python -m scripts.measure_2901_gpa_coverage \\
        --fundamentals <#3360 bundle dir> --fundamentals-sha256 <sha> \\
        --linkage <#3361 bundle dir> --linkage-sha256 <sha> --out <new json file>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from app.services import pit_fundamentals as pf
from app.services import security_linkage as sl
from scripts import census_3360_pit_fundamentals as census_3360

YEARS: Final = tuple(range(2012, 2025))
PRODUCTION_YEARS: Final = tuple(range(2013, 2025))
REVENUE: Final = (
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
)
COGS: Final = ("CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold")
ASSETS: Final = ("Assets",)
EQUITY: Final = ("StockholdersEquity",)
GROSS_PROFIT: Final = ("GrossProfit",)
ANNUAL_DAYS: Final = (350, 380)  # by construction: covers 52/53-week and calendar fiscal years
FPI_FORMS: Final = frozenset({"20-F", "40-F"})
ANNUAL_FORMS: Final = frozenset({"10-K", *FPI_FORMS})  # form families (``pf.form_family``)
RUNGS: Final = (
    "fundamentals_integrity_excluded",
    "no_companyfacts_entry",
    "sic_missing",
    "sic_financial",
    "no_public_annual_period",
    "several_annual_starts",
    "foreign_private_issuer",
    "gross_profit_unavailable",
    "component_sign_invalid",
    "assets_unavailable",
    "assets_nonpositive",
    "equity_unavailable",
    "equity_negative",
    "eligible",
)
NOT_EVALUATED: Final = "not_evaluated"

_STATE: dict[str, Any] = {}


def _init(root: str, sha: str) -> None:
    bundle = pf.load_pit_fundamentals(Path(root), expected_manifest_sha256=sha)
    _STATE["bundle"] = bundle
    _STATE["ciks"] = frozenset(bundle.ciks)
    _STATE["subs"] = zipfile.ZipFile(Path(root) / "inputs" / "submissions.zip")


def _sic(cik10: str) -> int | None:
    """Rule 3: a 1-4 digit SIC in [100, 9999], as a digit string or an integer; else missing."""
    subs: zipfile.ZipFile = _STATE["subs"]
    try:
        raw = json.loads(subs.read(f"CIK{cik10}.json")).get("sic")
    except KeyError:
        return None
    if isinstance(raw, str) and raw.isascii() and raw.isdigit() and len(raw) <= 4:
        value = int(raw)
    elif isinstance(raw, int) and not isinstance(raw, bool):
        value = raw
    else:
        return None
    return value if 100 <= value <= 9999 else None


Read = tuple[str, Decimal | None, tuple[str, ...], bool]


def read_component(
    bundle: pf.PitFundamentalsBundle, cik10: str, concepts: tuple[str, ...], start: str | None, end: str, d: date
) -> Read:
    """Rule 5: among aliases that are not ABSENT, the latest public acceptance decides (the reader
    returns an acceptance for value, ambiguous and blocked reads alike); at that acceptance the
    declared alias order decides (broadest concept first: REVT is total revenue). Returns
    (status, value, winning accns, aliases_disagree)."""
    reads: list[pf.ValueRead] = []
    for concept in concepts:
        read = bundle.value_as_of(cik10, pf.FactKey("us-gaap", concept, "USD", start, end), d)
        if read.status in (pf.ReadStatus.AFTER_CAPTURE, pf.ReadStatus.CONCEPT_NOT_IN_POLICY):
            raise RuntimeError(f"configuration read {read.status} for {cik10} {concept} at {d}")
        if read.status is not pf.ReadStatus.ABSENT:
            if read.acceptance is None:
                raise RuntimeError(f"non-absent read without acceptance: {cik10} {concept} {d}")
            reads.append(read)
    if not reads:
        return "absent", None, (), False
    latest = max(read.acceptance or "" for read in reads)
    top = [read for read in reads if read.acceptance == latest]  # in declared alias order
    disagree = len({read.values for read in top}) > 1 or any(r.status is not pf.ReadStatus.VALUE for r in top)
    winner = top[0]
    if winner.status is not pf.ReadStatus.VALUE:
        return str(winner.status), None, (), disagree
    return "value", Decimal(winner.values[0]), winner.accns, disagree


def accession_index(bundle: pf.PitFundamentalsBundle, cik10: str, d: date) -> tuple[Mapping[str, Any], ...]:
    """The CIK's whole public accession index at D (#3360 rule 6). ``public_events`` returns the
    shard-wide index whichever concept is named; ``REVENUE[0]`` only satisfies its policy gate."""
    return bundle.public_events(cik10, "us-gaap", REVENUE[0], d).accessions


def annual_period(bundle: pf.PitFundamentalsBundle, cik10: str, d: date) -> tuple[str, str] | str:
    """Rule 4: (start, end) or the rung that stops it."""
    forms = {a["accn"]: pf.form_family(a["form"]) for a in accession_index(bundle, cik10, d)}
    starts: dict[str, set[str]] = defaultdict(set)
    for concept in (*REVENUE, *COGS):
        read = bundle.public_events(cik10, "us-gaap", concept, d)
        for row in (*read.events, *read.rejections):
            if row["unit"] != "USD" or row["start"] is None or forms.get(row["accn"]) not in ANNUAL_FORMS:
                continue
            start, end = date.fromisoformat(row["start"]), date.fromisoformat(row["end"])
            if end.year == d.year - 1 and ANNUAL_DAYS[0] <= (end - start).days + 1 <= ANNUAL_DAYS[1]:
                starts[row["end"]].add(row["start"])
    if not starts:
        return "no_public_annual_period"
    end = max(starts)
    if len(starts[end]) > 1:
        return "several_annual_starts"
    return next(iter(starts[end])), end


def is_foreign_private_issuer(bundle: pf.PitFundamentalsBundle, cik10: str, d: date, winners: set[str]) -> bool:
    """Rule 6: the latest public ORIGINAL annual-report accession is a 20-F/40-F (any FPI form at
    that acceptance counts), or any winning component accession is."""
    accessions = accession_index(bundle, cik10, d)
    forms = {a["accn"]: pf.form_family(a["form"]) for a in accessions}
    originals = [
        (a["acceptance"], pf.form_family(a["form"]))
        for a in accessions
        if not a["form"].endswith("/A") and pf.form_family(a["form"]) in ANNUAL_FORMS
    ]
    latest = max((acc for acc, _ in originals), default=None)
    latest_fpi = any(form in FPI_FORMS for acc, form in originals if acc == latest)
    return latest_fpi or any(forms.get(accn) in FPI_FORMS for accn in winners)


def classify(cik10: str, d: date) -> tuple[str, dict[str, str]]:
    """The first failing rung (or ``eligible``) plus field statuses counted independently."""
    bundle: pf.PitFundamentalsBundle = _STATE["bundle"]
    fields: dict[str, str] = dict.fromkeys(
        ("period", "revenue", "cogs", "assets", "equity", "gp_tag", "fpi"), NOT_EVALUATED
    )
    if cik10 in bundle.integrity_excluded:
        return "fundamentals_integrity_excluded", fields
    if cik10 not in _STATE["ciks"]:
        return "no_companyfacts_entry", fields
    ladder: list[str] = []
    sic = _sic(cik10)
    fields["sic"] = "missing" if sic is None else "financial" if 6000 <= sic <= 6999 else "other"
    if sic is None:
        ladder.append("sic_missing")
    elif 6000 <= sic <= 6999:
        ladder.append("sic_financial")
    period = annual_period(bundle, cik10, d)
    if isinstance(period, str):
        fields["period"] = period
        return (ladder + [period])[0], fields
    fields["period"] = "found"
    start, end = period
    rev = read_component(bundle, cik10, REVENUE, start, end, d)
    cogs = read_component(bundle, cik10, COGS, start, end, d)
    assets = read_component(bundle, cik10, ASSETS, None, end, d)
    equity = read_component(bundle, cik10, EQUITY, None, end, d)
    gp_tag = read_component(bundle, cik10, GROSS_PROFIT, start, end, d)
    for name, read in (("revenue", rev), ("cogs", cogs), ("assets", assets), ("equity", equity), ("gp_tag", gp_tag)):
        fields[name] = read[0]
        if read[3]:
            fields[f"{name}_aliases_disagree_at_latest"] = "true"
    winners = {*rev[2], *cogs[2], *assets[2], *equity[2]}
    fpi = is_foreign_private_issuer(bundle, cik10, d, winners)
    fields["fpi"] = str(fpi).lower()
    if fpi:
        ladder.append("foreign_private_issuer")
    if rev[1] is None or cogs[1] is None:
        ladder.append("gross_profit_unavailable")
    elif rev[1] < 0 or cogs[1] < 0:
        ladder.append("component_sign_invalid")
    if assets[1] is None:
        ladder.append("assets_unavailable")
    elif assets[1] <= 0:
        ladder.append("assets_nonpositive")
    if equity[1] is None:
        ladder.append("equity_unavailable")
    elif equity[1] < 0:
        ladder.append("equity_negative")
    if rev[1] is not None and cogs[1] is not None and assets[1] is not None and equity[1] is not None:
        fields["winning_accessions"] = "one" if len(winners) == 1 else "several"
        bad = [
            n
            for n, ok in (
                ("revenue", rev[1] >= 0),
                ("cogs", cogs[1] >= 0),
                ("assets", assets[1] > 0),
                ("equity", equity[1] >= 0),
            )
            if not ok
        ]
        fields["signs"] = "ok" if not bad else "+".join(bad)
    else:
        fields["winning_accessions"] = fields["signs"] = "components_unavailable"
    return (ladder[0] if ladder else "eligible"), fields


def _job(job: tuple[str, tuple[str, ...]]) -> tuple[str, dict[str, tuple[str, dict[str, str]]]]:
    cik10, days = job
    return cik10, {day: classify(cik10, date.fromisoformat(day)) for day in days}


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fundamentals", type=Path, required=True)
    parser.add_argument("--fundamentals-sha256", required=True)
    parser.add_argument("--linkage", type=Path, required=True)
    parser.add_argument("--linkage-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--processes", type=int, default=max(1, (multiprocessing.cpu_count() or 2) - 2))
    args = parser.parse_args()
    if args.out.exists():
        parser.error(f"{args.out} exists")

    formations = census_3360.formation_dates(YEARS)
    linkage = sl.load_security_linkage(args.linkage, expected_manifest_sha256=args.linkage_sha256)
    linkage_inputs = json.loads((args.linkage / "manifest.json").read_text())["input_sha256"]
    if linkage_inputs["pit_manifest"] != args.fundamentals_sha256:
        parser.error("the linkage bundle was built over a different #3360 manifest")
    link_reasons: dict[str, Counter[str]] = {}
    linked_series: dict[str, int] = {}
    series_per_cik: dict[str, Counter[int]] = {}
    work: dict[str, list[str]] = defaultdict(list)
    for d in formations:
        reasons: Counter[str] = Counter()
        per_cik: Counter[str] = Counter()
        for sid in linkage.series_ids:
            result = linkage.link_as_of(sid, d)
            if result.reason in (sl.Reason.OUTSIDE_SERIES, sl.Reason.VENDOR_OUT_OF_SCOPE):
                continue
            reasons[result.label] += 1
            kind = (result.grammar or "").split(":")[0]
            if result.reason is sl.Reason.LINKED and kind in ("plain", "class") and result.cik is not None:
                per_cik[result.cik] += 1
        link_reasons[d.isoformat()] = reasons
        linked_series[d.isoformat()] = sum(per_cik.values())
        series_per_cik[d.isoformat()] = Counter(per_cik.values())
        for cik10 in per_cik:
            work[cik10].append(d.isoformat())

    jobs = [(cik10, tuple(days)) for cik10, days in sorted(work.items())]
    rungs: dict[str, Counter[str]] = defaultdict(Counter)
    fields: dict[str, Counter[str]] = defaultdict(Counter)
    with multiprocessing.Pool(
        args.processes, initializer=_init, initargs=(str(args.fundamentals), args.fundamentals_sha256)
    ) as pool:
        for _cik10, by_day in pool.imap_unordered(_job, jobs, chunksize=32):
            for day, (rung, statuses) in by_day.items():
                if rung not in RUNGS:
                    raise RuntimeError(f"undeclared rung {rung}")
                rungs[day][rung] += 1
                for key, value in statuses.items():
                    fields[day][f"{key}={value}"] += 1
                    fields[day][f"{rung}|{key}={value}"] += 1

    here = Path(__file__)
    payload: dict[str, Any] = {
        "code_sha256": {
            path.name: _sha256(path)
            for path in (here, Path(pf.__file__), Path(sl.__file__), Path(census_3360.__file__))
        },
        "fundamentals_manifest_sha256": args.fundamentals_sha256,
        "linkage_manifest_sha256": args.linkage_sha256,
        "production_formations": [d.isoformat() for d in census_3360.formation_dates(PRODUCTION_YEARS)],
        "formations": {},
    }
    for d in formations:
        day = d.isoformat()
        ladder = {rung: rungs[day][rung] for rung in RUNGS}
        if sum(ladder.values()) != sum(series_per_cik[day].values()):
            raise RuntimeError(f"{day}: ladder does not reconcile to linked CIKs")
        payload["formations"][day] = {
            "link_reasons": dict(sorted(link_reasons[day].items())),
            "linked_plain_or_class_series": linked_series[day],
            "series_per_linked_cik": {str(k): v for k, v in sorted(series_per_cik[day].items())},
            "cik_rungs": ladder,
            "fields": dict(sorted(fields[day].items())),
        }
    with args.out.open("x") as handle:
        json.dump(payload, handle, indent=1, sort_keys=True)
    for day, cell in payload["formations"].items():
        print(day, sum(cell["cik_rungs"].values()), cell["cik_rungs"]["eligible"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
