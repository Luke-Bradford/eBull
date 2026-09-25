"""Build the #2901 quality-arm input artefact (spec rule 8). Reads no return.

Spec: ``docs/proposals/ta/2026-09-24-2901-quality-arm.md`` ("Construction"). The rules live in
``app/services/r6_quality_universe.py``; this script gathers their inputs at each June formation
D, applies them and publishes one document per formation plus a manifest.

Inputs, each pinned: the #3360 fundamentals bundle and the #3361 linkage bundle (manifest
digests; the linkage must have been built over this #3360 manifest), the #3360 copy of
``submissions.zip`` (SIC), the Intrader mirror at a clean pinned commit (liquidity and the X(D)
row come ONLY from its ``Data/Day`` CSVs), and one REPEATABLE READ snapshot of the Intrader rows of
``research_price_series`` (symbol, bounds, termination evidence) and of the Form 25 provisions,
dumped into ``<out>/inputs/`` and read back from the copy.

Publish protocol (as ``scripts/build_3361_security_linkage.py``): exclusive ``mkdir``; every
document written with fsync and the no-replace link; the manifest LAST; a crashed build is
deleted, never resumed. :func:`load_quality_input` is the verified loader.

Usage::

    PYTHONPATH=. uv run python -m scripts.build_2901_quality_input \\
        --fundamentals <#3360 bundle> --fundamentals-sha256 <sha> \\
        --linkage <#3361 bundle> --linkage-sha256 <sha> \\
        --mirror ~/Dev/eBull/var/research_corpus/mirrors/icyDenev_Intrader --mirror-commit <sha> \\
        --out <new directory>
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import multiprocessing
import os
import platform
import shutil
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from app.services import pit_fundamentals as pf
from app.services import r6_quality_universe as quality
from app.services import security_linkage as sl
from app.services.market_calendar import us_market_status
from app.services.r6_pit_bundle import read_verified_document
from app.services.research_corpus_ingest import (
    normalise_vendor_symbol,
    parse_intrader_rows,
    vendor_symbol_has_bankruptcy_suffix,
)
from app.services.series_termination import TerminationEvidence, classify_termination
from app.services.universe_selection import ALIVE_CUT_DAYS, INTRADER_CAPTURE_DATE
from scripts import census_3360_pit_fundamentals as census_3360
from scripts import census_3361_security_linkage as census_3361
from scripts.build_2900_pit_bundle import _write_exclusive
from scripts.evaluate_2908_exclusion import _verify_mirror

MANIFEST_SCHEMA: Final = "quality-2901-manifest-v1"
FORMATION_SCHEMA: Final = "quality-2901-formation-v1"
MANIFEST_FILENAME: Final = "manifest.json"
FORMATIONS_DIRNAME: Final = "formations"
YEARS: Final = tuple(range(2013, 2025))  # D5: the production formations
OUTCOME_HORIZON_DAYS: Final = 730  # the census's (D, D + 730] outcome channels

#: Standard label and definition of each alias, quoted from the FASB US GAAP taxonomy
#: (``elts/us-gaap-lab-*.xml`` roles ``label`` and ``documentation``). The 2024 release is used
#: where it still carries the element; ``SalesRevenueNet`` and ``CostOfGoodsSold`` were
#: deprecated before it, so theirs are from 2017-01-31. The scopes differ -- ``Revenues``
#: includes investment and interest income and trading gains; ``CostOfGoodsSold`` excludes
#: services -- which is residual R2, not a claim of Compustat equivalence.
TAXONOMY_SOURCES: Final = {
    "us-gaap-2024": {
        "url": "https://xbrl.fasb.org/us-gaap/2024/us-gaap-2024.zip",
        "sha256": "decdd417d86ff7bfb5ca166c0ca1001017aea873673544a8d7f91c34bf5d82df",
    },
    "us-gaap-2017-01-31": {
        "url": "https://xbrl.fasb.org/us-gaap/2017/us-gaap-2017-01-31.zip",
        "sha256": "77694ed4226feda79577487151118d0eb7e1df679c08ab13a5c575e11429a758",
    },
}
ALIAS_TAXONOMY: Final = {
    "Revenues": (
        "us-gaap-2024",
        "Revenues",
        "Amount of revenue recognized from goods sold, services rendered, insurance premiums, or other activities"
        " that constitute an earning process. Includes, but is not limited to, investment and interest income"
        " before deduction of interest expense when recognized as a component of revenue, and sales and trading"
        " gain (loss).",
    ),
    "RevenueFromContractWithCustomerExcludingAssessedTax": (
        "us-gaap-2024",
        "Revenue from Contract with Customer, Excluding Assessed Tax",
        "Amount, excluding tax collected from customer, of revenue from satisfaction of performance obligation by"
        " transferring promised good or service to customer. Tax collected from customer is tax assessed by"
        " governmental authority that is both imposed on and concurrent with specific revenue-producing"
        " transaction, including, but not limited to, sales, use, value added and excise.",
    ),
    "RevenueFromContractWithCustomerIncludingAssessedTax": (
        "us-gaap-2024",
        "Revenue from Contract with Customer, Including Assessed Tax",
        "Amount, including tax collected from customer, of revenue from satisfaction of performance obligation by"
        " transferring promised good or service to customer. Tax collected from customer is tax assessed by"
        " governmental authority that is both imposed on and concurrent with specific revenue-producing"
        " transaction, including, but not limited to, sales, use, value-added and excise.",
    ),
    "SalesRevenueNet": (
        "us-gaap-2017-01-31",
        "Revenue, Net",
        "Total revenue from sale of goods and services rendered during the reporting period, in the normal course"
        " of business, reduced by sales returns and allowances, and sales discounts.",
    ),
    "CostOfRevenue": (
        "us-gaap-2024",
        "Cost of Revenue",
        "The aggregate cost of goods produced and sold and services rendered during the reporting period.",
    ),
    "CostOfGoodsAndServicesSold": (
        "us-gaap-2024",
        "Cost of Goods and Services Sold",
        "The aggregate costs related to goods produced and sold and services rendered by an entity during the"
        " reporting period. This excludes costs incurred during the reporting period related to financial"
        " services rendered and other revenue generating activities.",
    ),
    "CostOfGoodsSold": (
        "us-gaap-2017-01-31",
        "Cost of Goods Sold",
        "Total costs related to goods produced and sold during the reporting period.",
    ),
    "Assets": ("us-gaap-2024", "Assets", "Amount of asset recognized for present right to economic benefit."),
    "StockholdersEquity": (
        "us-gaap-2024",
        "Equity, Attributable to Parent",
        "Amount of equity (deficit) attributable to parent. Excludes temporary equity and equity attributable to"
        " noncontrolling interest.",
    ),
}

_REPO_ROOT: Final = Path(__file__).resolve().parents[1]
POLICY_FILES: Final = (
    "app/services/r6_quality_universe.py",
    "scripts/build_2901_quality_input.py",
    "scripts/census_2901_quality.py",
    "app/services/pit_fundamentals.py",
    "app/services/security_linkage.py",
    "app/services/series_termination.py",
    "app/services/market_calendar.py",
    "app/services/research_corpus_ingest.py",
    "app/services/universe_selection.py",
    "app/services/r6_pit_bundle.py",
    "scripts/build_2900_pit_bundle.py",
    "scripts/census_3360_pit_fundamentals.py",
    "scripts/census_3361_security_linkage.py",
)


class QualityInputError(RuntimeError):
    """An input or a published document fails a precondition."""


# --------------------------------------------------------------------------- policy


def policy_document() -> dict[str, Any]:
    code = {}
    for relative in POLICY_FILES:
        with (_REPO_ROOT / relative).open("rb") as handle:
            code[relative] = hashlib.file_digest(handle, "sha256").hexdigest()
    return {
        "code": code,
        "constants": {
            **quality.POLICY,
            "alias_taxonomy": {k: list(v) for k, v in sorted(ALIAS_TAXONOMY.items())},
            "taxonomy_sources": TAXONOMY_SOURCES,
            "formation_years": list(YEARS),
            "formation_rule": "last NYSE session of June (census_3360.formation_dates)",
            "execution_rule": "X(D) = first NYSE session after D (market_calendar.us_market_status)",
            "liquidity_bars": census_3361.LIQUIDITY_BARS,
            "liquidity_span_days": census_3361.LIQUIDITY_SPAN_DAYS,
            "outcome_horizon_days": OUTCOME_HORIZON_DAYS,
            "alive_cut_days": ALIVE_CUT_DAYS,
            "intrader_capture_date": INTRADER_CAPTURE_DATE.isoformat(),
            "manifest_schema": MANIFEST_SCHEMA,
            "formation_schema": FORMATION_SCHEMA,
        },
        "python": platform.python_version(),
        "tzdata": importlib.metadata.version("tzdata"),
    }


def policy_sha256() -> str:
    return hashlib.sha256(pf.canonical_json(policy_document())).hexdigest()


# --------------------------------------------------------------------------- pure pieces


def execution_date(decision: date) -> date:
    """X(D): the first NYSE session after D."""
    day = decision + timedelta(days=1)
    while us_market_status(day) == "closed":
        day += timedelta(days=1)
    return day


@dataclass(frozen=True)
class MirrorBar:
    day: date
    raw_open: Decimal | None
    raw_close: Decimal | None
    adjusted_close: Decimal | None
    volume: int | None


def read_mirror_series(path: Path) -> tuple[MirrorBar, ...]:
    """One Intrader CSV through the ingest's own parser (an unparseable or non-finite field is
    ``None``, a fractional volume is ``None``). Dates must be strictly increasing, else refuse."""
    with path.open(newline="", encoding="utf-8", errors="strict") as handle:
        bars = tuple(
            MirrorBar(row.bar_date, row.open, row.close, row.adj_close, row.volume)
            for row in parse_intrader_rows(path.stem.upper(), handle)
        )
    days = [bar.day for bar in bars]
    if any(later <= earlier for earlier, later in zip(days, days[1:], strict=False)):
        raise QualityInputError(f"mirror dates are not strictly increasing: {path}")
    return bars


def liquidity(bars: Sequence[MirrorBar], decision: date) -> Decimal | None:
    """``census_3361.liquidity`` over the mirror's raw close x raw volume."""
    return census_3361.liquidity([(b.day, b.raw_close, b.volume) for b in bars], decision)


def x_row_executable(bars: Sequence[MirrorBar], x_day: date) -> bool:
    row = next((bar for bar in bars if bar.day == x_day), None)
    if row is None:
        return False
    return quality.is_executable(row.raw_open, row.raw_close, row.adjusted_close, row.volume)


def form25_in_horizon(
    rows: Sequence[Mapping[str, Any]],
    cik: str,
    symbols: frozenset[str],
    decision: date,
    provisions: Mapping[str, str | None],
) -> list[dict[str, Any]]:
    """#3361 Form 25 rows on the linked CIK filed in (D, D + 730], each with its symbol match
    and raw ``rule_provision``. The census keeps ``symbol_match`` only."""
    horizon = decision + timedelta(days=OUTCOME_HORIZON_DAYS)
    return [
        {
            "accession": row["accession"],
            "filed_date": row["filed_date"],
            "match": sl.form25_match(row["resolved_symbol"], symbols),
            "rule_provision": provisions.get(row["accession"]),
        }
        for row in rows
        if row["issuer_cik"] == cik and decision < date.fromisoformat(row["filed_date"]) <= horizon
    ]


# --------------------------------------------------------------------------- classification workers

_STATE: dict[str, Any] = {}


def _init(root: str, sha: str) -> None:
    _STATE["bundle"] = pf.load_pit_fundamentals(Path(root), expected_manifest_sha256=sha)
    _STATE["subs"] = zipfile.ZipFile(Path(root) / "inputs" / "submissions.zip")


def _sic(cik10: str) -> int | None:
    try:
        raw = json.loads(_STATE["subs"].read(f"CIK{cik10}.json")).get("sic")
    except KeyError:
        return None
    return quality.parse_sic(raw)


def _classify(job: tuple[str, tuple[str, ...]]) -> tuple[str, dict[str, dict[str, Any]]]:
    cik10, days = job
    sic = _sic(cik10)
    out = {}
    for day in days:
        result = quality.classify(_STATE["bundle"], cik10, sic, date.fromisoformat(day))
        out[day] = {
            "rung": result.rung,
            "fields": result.fields,
            "sic": result.sic,
            "period": list(result.period) if result.period else None,
            "components": {name: {**asdict(c), "accns": list(c.accns)} for name, c in result.components.items()},
        }
    return cik10, out


# --------------------------------------------------------------------------- DB snapshot


def snapshot_db(conn: Any) -> dict[str, Any]:
    """⚠ First statement of a non-autocommit transaction: one REPEATABLE READ snapshot."""
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

    def rows(sql: str) -> list[list[Any]]:
        return [[v.isoformat() if isinstance(v, date) else v for v in r] for r in conn.execute(sql).fetchall()]

    if sl.IN_SCOPE_VENDOR != "icyDenev/Intrader":
        raise QualityInputError("the evidence query's vendor literal is stale")
    return {
        "series_evidence": sorted(
            rows(
                "SELECT series_id, vendor_symbol, first_bar, last_bar, delisting_source, delisting_provision"
                " FROM research_price_series WHERE vendor = 'icyDenev/Intrader'"
            )
        ),
        "form25_provisions": sorted(
            rows(
                "SELECT accession_number, rule_provision FROM sec_form25_register"
                " WHERE provision_class = 'equity_delisting'"
            )
        ),
    }


# --------------------------------------------------------------------------- build


def build(
    *,
    fundamentals: Path,
    fundamentals_sha256: str,
    linkage: Path,
    linkage_sha256: str,
    mirror: Path,
    mirror_commit: str,
    db: Mapping[str, Any],
    out: Path,
    processes: int = 1,
) -> dict[str, Any]:
    _verify_mirror(mirror, mirror_commit)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.mkdir()  # exclusive: an existing directory is refused, never resumed
    try:
        return _build(
            fundamentals, fundamentals_sha256, linkage, linkage_sha256, mirror, mirror_commit, db, out, processes
        )
    except BaseException:
        shutil.rmtree(out, ignore_errors=True)
        raise


def _build(
    fundamentals: Path,
    fundamentals_sha256: str,
    linkage_root: Path,
    linkage_sha256: str,
    mirror: Path,
    mirror_commit: str,
    db: Mapping[str, Any],
    out: Path,
    processes: int,
) -> dict[str, Any]:
    pit = pf.load_pit_fundamentals(fundamentals, expected_manifest_sha256=fundamentals_sha256)
    pit_manifest = json.loads(read_verified_document(fundamentals / pf.MANIFEST_FILENAME)[1])
    linkage = sl.load_security_linkage(linkage_root, expected_manifest_sha256=linkage_sha256)
    linkage_manifest = json.loads(read_verified_document(linkage_root / sl.MANIFEST_FILENAME)[1])
    if linkage_manifest["input_sha256"]["pit_manifest"] != fundamentals_sha256:
        raise QualityInputError("the linkage bundle was built over a different #3360 manifest")
    with (fundamentals / "inputs" / "submissions.zip").open("rb") as handle:
        submissions_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    if submissions_sha != pit_manifest["input_sha256"]["submissions"]:
        raise QualityInputError("#3360 submissions.zip does not match its manifest")

    input_sha: dict[str, Any] = {
        "fundamentals_manifest": fundamentals_sha256,
        "linkage_manifest": linkage_sha256,
        "submissions": submissions_sha,
        "mirror_commit": mirror_commit,
        "market_calendar_rule_set": _market_calendar_version(),
    }
    dumps: dict[str, Any] = {}
    for name in ("series_evidence", "form25_provisions"):
        path = out / "inputs" / f"{name}.json"
        _write_exclusive(path, db[name])
        input_sha[name], data = read_verified_document(path)
        dumps[name] = json.loads(data)

    evidence = _series_evidence(dumps["series_evidence"])
    provisions = {accession: provision for accession, provision in dumps["form25_provisions"]}
    entries = {entry["series_id"]: entry for entry in linkage_manifest["series"]}
    day_dir = mirror / "Data" / "Day"
    files: dict[str, list[Path]] = defaultdict(list)
    for path in day_dir.glob("*.csv"):
        files[normalise_vendor_symbol(path.stem)].append(path)

    formations = census_3360.formation_dates(YEARS)
    candidates: dict[date, dict[str, list[tuple[int, sl.LinkResult]]]] = {}
    work: dict[str, list[str]] = defaultdict(list)
    for d in formations:
        per_cik: dict[str, list[tuple[int, sl.LinkResult]]] = defaultdict(list)
        for sid in linkage.series_ids:
            result = linkage.link_as_of(sid, d)
            if quality.is_candidate(result):
                assert result.cik is not None
                per_cik[result.cik].append((sid, result))
        candidates[d] = per_cik
        for cik in per_cik:
            work[cik].append(d.isoformat())

    classified: dict[str, dict[str, dict[str, Any]]] = {}
    jobs = [(cik, tuple(days)) for cik, days in sorted(work.items())]
    with multiprocessing.Pool(processes, initializer=_init, initargs=(str(fundamentals), fundamentals_sha256)) as pool:
        for cik, by_day in pool.imap_unordered(_classify, jobs, chunksize=32):
            classified[cik] = by_day
    del pit  # verified above; the workers read their own copies

    series_cache: dict[int, dict[str, Any]] = {}

    def series(sid: int) -> dict[str, Any]:
        cached = series_cache.get(sid)
        if cached is None:
            cached = series_cache[sid] = _series_inputs(sid, entries, linkage_root, evidence, files, formations)
        return cached

    manifest_formations = []
    for d in formations:
        document = _formation(d, candidates[d], classified, series, provisions)
        relative = f"{FORMATIONS_DIRNAME}/{d.isoformat()}.json"
        _write_exclusive(out / relative, document)
        digest, _ = read_verified_document(out / relative)
        manifest_formations.append(
            {"formation": d.isoformat(), "path": relative, "sha256": digest, "counts": document["counts"]}
        )

    manifest = {
        "schema": MANIFEST_SCHEMA,
        "policy": policy_sha256(),
        "input_sha256": input_sha,
        "form25_span": linkage_manifest["form25_span"],
        "linkage_supported_through": linkage_manifest["supported_through"],
        "formations": manifest_formations,
    }
    _write_exclusive(out / MANIFEST_FILENAME, manifest)
    descriptor = os.open(out, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return manifest


def _market_calendar_version() -> str:
    from app.services.market_calendar import RULE_SET_VERSION

    return RULE_SET_VERSION


def _series_evidence(rows: Sequence[Sequence[Any]]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    symbols: Counter[str] = Counter()
    for series_id, vendor_symbol, first_bar, last_bar, source, provision in rows:
        out[series_id] = {
            "vendor_symbol": vendor_symbol,
            "first_bar": first_bar,
            "last_bar": last_bar,
            "termination": TerminationEvidence(
                linked=source == "sec_form25",
                provision=provision,
                q_suffix=vendor_symbol_has_bankruptcy_suffix(str(vendor_symbol)),
            ),
        }
        symbols[vendor_symbol] += 1
    for record in out.values():
        record["symbol_unique"] = symbols[record["vendor_symbol"]] == 1
    return out


def _series_inputs(
    sid: int,
    entries: Mapping[int, Mapping[str, Any]],
    linkage_root: Path,
    evidence: Mapping[int, Mapping[str, Any]],
    files: Mapping[str, Sequence[Path]],
    formations: Sequence[date],
) -> dict[str, Any]:
    """The series' linkage document (verified against its manifest digest), its DB row and its
    single mirror file; any mismatch between the three refuses the build. The bars are reduced
    to what the rules read -- liquidity at each D and the X(D) row -- and then dropped."""
    entry = entries[sid]
    digest, data = read_verified_document(linkage_root / entry["path"])
    if digest != entry["sha256"]:
        raise QualityInputError(f"series {sid}: linkage document digest moved")
    document = json.loads(data)
    row = evidence.get(sid)
    if row is None or not row["symbol_unique"]:
        raise QualityInputError(f"series {sid}: no research_price_series row, or its symbol is not unique")
    if (row["vendor_symbol"], row["first_bar"], row["last_bar"]) != (
        document["vendor_symbol"],
        document["first_bar"],
        document["last_bar"],
    ):
        raise QualityInputError(f"series {sid}: research_price_series moved since the linkage build")
    paths = files.get(row["vendor_symbol"], [])
    if len(paths) != 1:
        raise QualityInputError(f"series {sid} ({row['vendor_symbol']}): {len(paths)} mirror files")
    bars = read_mirror_series(paths[0])
    bounds = (bars[0].day.isoformat(), bars[-1].day.isoformat()) if bars else None
    if bounds != (str(row["first_bar"]), str(row["last_bar"])):
        raise QualityInputError(
            f"series {sid} ({row['vendor_symbol']}): mirror bar bounds differ from the stored series"
        )
    return {
        "vendor_symbol": row["vendor_symbol"],
        "last_bar": row["last_bar"],
        "termination_class": str(classify_termination(row["termination"])),
        "symbols": sl.match_set(sl.parse_vendor_symbol(row["vendor_symbol"])),
        "form25": document["form25"],
        "liquidity": {d: liquidity(bars, d) for d in formations},
        "executable": {d: x_row_executable(bars, execution_date(d)) for d in formations},
    }


def _formation(
    d: date,
    candidates: Mapping[str, Sequence[tuple[int, sl.LinkResult]]],
    classified: Mapping[str, Mapping[str, Mapping[str, Any]]],
    series: Any,
    provisions: Mapping[str, str | None],
) -> dict[str, Any]:
    day, x_day = d.isoformat(), execution_date(d)
    capture_floor = INTRADER_CAPTURE_DATE - timedelta(days=ALIVE_CUT_DAYS)
    rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    for cik in sorted(candidates):
        links = dict(candidates[cik])
        liquidities = {sid: series(sid)["liquidity"][d] for sid in links}
        chosen = quality.choose_series(liquidities)
        for sid in sorted(links):
            candidate_rows.append(
                {
                    "series_id": sid,
                    "cik": cik,
                    "disposition": "chosen" if sid == chosen else "not_chosen",
                    "liquidity": None if liquidities[sid] is None else str(liquidities[sid]),
                }
            )
        link, info, result = links[chosen], series(chosen), classified[cik][day]
        executable = info["executable"][d]
        rung = result["rung"]
        if rung == quality.ELIGIBLE and not executable:
            rung = quality.NOT_EXECUTABLE
        rows.append(
            {
                "cik": cik,
                "series_id": chosen,
                "vendor_symbol": info["vendor_symbol"],
                "link_basis": link.basis,
                "link_grammar": link.grammar,
                "q_alias": link.q_alias,
                "link_accessions": [o.accession for o in link.observations if o.cik == cik],
                "liquidity": None if liquidities[chosen] is None else str(liquidities[chosen]),
                "x_date": x_day.isoformat(),
                "executable": executable,
                "pre_x_rung": result["rung"],
                "rung": rung,
                "sic": result["sic"],
                "fields": result["fields"],
                "period": result["period"],
                "components": result["components"],
                "last_bar": info["last_bar"],
                "alive_at_capture": date.fromisoformat(info["last_bar"]) > capture_floor,
                "termination_class": info["termination_class"],
                "form25_horizon": form25_in_horizon(info["form25"], cik, info["symbols"], d, provisions),
            }
        )

    eligible = {row["cik"]: row for row in rows if row["rung"] == quality.ELIGIBLE}
    signal = {
        cik: quality.gpa_of(*(row["components"][name]["value"] for name in ("revenue", "cogs", "assets")))
        for cik, row in eligible.items()
    }
    deciles = quality.top_decile(signal)
    for row in rows:
        value = signal.get(row["cik"])
        row["gpa"] = None if value is None else [str(value.numerator), str(value.denominator)]
        row["decile"] = deciles.decile.get(row["cik"])
        row["in_arm"] = row["cik"] in deciles.arm
    counts = {
        "rungs": {rung: sum(1 for r in rows if r["rung"] == rung) for rung in quality.RUNGS},
        "pre_x_rungs": {
            rung: sum(1 for r in rows if r["pre_x_rung"] == rung) for rung in quality.RUNGS if rung != "not_executable"
        },
        "population": len(rows),
        "eligible": len(eligible),
        "arm": len(deciles.arm),
        "boundary_ties": deciles.boundary_ties,
        "not_chosen": sum(1 for c in candidate_rows if c["disposition"] == "not_chosen"),
    }
    return {
        "schema": FORMATION_SCHEMA,
        "formation": day,
        "x_date": x_day.isoformat(),
        "counts": counts,
        "candidates": candidate_rows,
        "rows": rows,
    }


# --------------------------------------------------------------------------- loader


def load_quality_input(root: Path, *, expected_manifest_sha256: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """The manifest and every formation document, each verified against its digest and this
    code's policy. Anything else refuses."""
    digest, data = read_verified_document(root / MANIFEST_FILENAME)
    if digest != expected_manifest_sha256:
        raise QualityInputError(f"manifest digest {digest} != pinned {expected_manifest_sha256}")
    manifest = json.loads(data)
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise QualityInputError("manifest schema mismatch")
    if manifest.get("policy") != policy_sha256():
        raise QualityInputError("manifest policy differs from this code's POLICY")
    documents = []
    for entry in manifest["formations"]:
        expected = f"{FORMATIONS_DIRNAME}/{entry['formation']}.json"
        if entry["path"] != expected:
            raise QualityInputError(f"formation path must be {expected!r}")
        file_digest, body = read_verified_document(root / entry["path"])
        if file_digest != entry["sha256"]:
            raise QualityInputError(f"{entry['path']}: digest moved")
        document = json.loads(body)
        if document.get("schema") != FORMATION_SCHEMA or document.get("formation") != entry["formation"]:
            raise QualityInputError(f"{entry['path']}: schema or formation mismatch")
        documents.append(document)
    return manifest, documents


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fundamentals", type=Path, required=True)
    parser.add_argument("--fundamentals-sha256", required=True)
    parser.add_argument("--linkage", type=Path, required=True)
    parser.add_argument("--linkage-sha256", required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--mirror-commit", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--processes", type=int, default=max(1, (multiprocessing.cpu_count() or 2) - 2))
    args = parser.parse_args()

    import psycopg

    from app.config import settings

    with psycopg.connect(settings.database_url) as conn:
        db = snapshot_db(conn)
    manifest = build(
        fundamentals=args.fundamentals,
        fundamentals_sha256=args.fundamentals_sha256,
        linkage=args.linkage,
        linkage_sha256=args.linkage_sha256,
        mirror=args.mirror,
        mirror_commit=args.mirror_commit,
        db=db,
        out=args.out,
        processes=args.processes,
    )
    with (args.out / MANIFEST_FILENAME).open("rb") as handle:
        manifest_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    json.dump(
        {"manifest_sha256": manifest_sha, "formations": {f["formation"]: f["counts"] for f in manifest["formations"]}},
        sys.stdout,
        indent=1,
        sort_keys=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
