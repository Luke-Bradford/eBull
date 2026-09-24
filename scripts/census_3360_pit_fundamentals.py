"""#3360 acceptance item 3: descriptive census of the PIT fundamentals bundle.

Spec ``docs/proposals/ta/2026-09-24-3360-companyfacts-pit-bundle.md`` §"Census". Descriptive
only: no strategy, no return, no economic reading of a Form 25 label (#3362 owns that).

* **Formations:** the last NYSE session of each June, 2011-2024.
* **Population** is built from ``submissions.zip`` (every member, not only companyfacts
  CIKs), so it is independent of companyfacts: a CIK is a member at D when it has an
  admitted-form accession accepted in ``[D - 730 days, D)`` (NY date). A CIK whose
  submissions fail snapshot integrity (rule 5) cannot be placed and is reported as the
  ``eligibility_unknown`` cohort instead.
* Each member carries two axes: **snapshot status** (``in_bundle`` / ``no_companyfacts_entry``
  / ``integrity_excluded``) and **historical status at D** (``has_public_event`` /
  ``no_public_event``).
* **Concept coverage** per formation x concept: members with at least one key whose rule-8
  status at D is ``value`` / ``ambiguous`` / ``blocked_by_rejection``. By construction the
  same 18-month recency window (key ``end`` >= D - 548 days) bounds all three counts, so they
  are comparable; a key whose ``end`` is not an ISO date (only possible on a rejection) is
  outside the window.
* **Size** (census-only): ``Assets`` / ``USD``, the latest ``end`` among instant keys whose
  status at D is ``value``; zero-based rank over ``(value, CIK)``, decile = floor(10 * rank / n).
* **Outcome:** ``sec_form25_register`` rows with ``provision_class = 'equity_delisting'`` by
  ``issuer_cik``, filed in ``(D, D + 730]``: an indicator per raw ``rule_provision`` label and
  the label set of the first filing date. With no filing, ``no_form25_observed`` only when
  ``[D, D + 730]`` lies inside the register's own ``[min, max](filed_date)``; otherwise
  ``unobserved_horizon``. Completeness inside that span is not certified.
* **Ledger:** raw rows = sum of rule-4 outcomes + integrity-excluded rows, per concept, from
  the manifest's per-(concept, unit) ledger; ``no_acceptance`` is reported per CIK and never
  assigned to a formation.

Usage::

    PYTHONPATH=. uv run python scripts/census_3360_pit_fundamentals.py \\
        --bundle <bundle dir> --manifest-sha256 <sha> --out <new evidence file>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import re
import sys
import zipfile
from bisect import bisect_left
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from app.services import pit_fundamentals as pf
from app.services.market_calendar import us_market_status
from app.services.sec_form25_register import PROVISION_CLASSES

EVIDENCE_SCHEMA: Final = "pit-fundamentals-census-v1"
FORMATION_YEARS: Final = tuple(range(2011, 2025))
POPULATION_LOOKBACK_DAYS: Final = 730
OUTCOME_HORIZON_DAYS: Final = 730
RECENCY_DAYS: Final = 548  # descriptive 18-month window, stated as such by the spec

EQUITY_DELISTING: Final = "equity_delisting"
if EQUITY_DELISTING not in PROVISION_CLASSES:
    raise ImportError(f"{EQUITY_DELISTING!r} is not a sec_form25_register provision class")

#: Raw ``rule_provision`` labels reported individually; anything else is ``other``.
NAMED_LABELS: Final = ("(b)", "(a)(4)", "(a)(3)")
NULL_LABEL: Final = "NULL"
OTHER_LABEL: Final = "other"

IN_BUNDLE: Final = "in_bundle"
NO_COMPANYFACTS_ENTRY: Final = "no_companyfacts_entry"
INTEGRITY_EXCLUDED: Final = "integrity_excluded"
HAS_PUBLIC_EVENT: Final = "has_public_event"
NO_PUBLIC_EVENT: Final = "no_public_event"

FORM25_OBSERVED: Final = "form25_observed"
NO_FORM25_OBSERVED: Final = "no_form25_observed"
UNOBSERVED_HORIZON: Final = "unobserved_horizon"

COVERAGE_STATUSES: Final = (pf.ReadStatus.VALUE, pf.ReadStatus.AMBIGUOUS, pf.ReadStatus.BLOCKED_BY_REJECTION)
ASSETS: Final = ("us-gaap", "Assets", "USD")
ASSETS_UNAVAILABLE: Final = "assets_unavailable"

_MAIN_MEMBER: Final = re.compile(r"CIK(\d{10})\.json")


# ------------------------------------------------------------------ pure pieces


def formation_dates(years: Iterable[int] = FORMATION_YEARS) -> tuple[date, ...]:
    """The last NYSE session of each June."""
    result = []
    for year in years:
        day = date(year, 6, 30)
        while us_market_status(day) == "closed":
            day -= timedelta(days=1)
        result.append(day)
    return tuple(result)


def membership_mask(admitted_ny_dates: Sequence[date], formations: Sequence[date]) -> int:
    """Bit i set iff an admitted accession was accepted in ``[D_i - 730, D_i)``."""
    mask = 0
    for i, decision in enumerate(formations):
        lo = bisect_left(admitted_ny_dates, decision - timedelta(days=POPULATION_LOOKBACK_DAYS))
        if lo < bisect_left(admitted_ny_dates, decision):
            mask |= 1 << i
    return mask


def admitted_dates(index: pf.SubmissionsIndex) -> list[date]:
    return sorted(
        {
            pf.acceptance_ny_date(f.acceptance)
            for f in index.filings.values()
            if f.form in pf.ADMITTED_FORMS and f.acceptance is not None
        }
    )


@dataclass(frozen=True)
class MemberFacts:
    has_public_event: bool
    #: concept label -> the coverage statuses seen among its recent keys at D
    coverage: Mapping[str, frozenset[str]]
    assets: Decimal | None


def _iso_date(raw: object) -> date | None:
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def member_facts(bundle: pf.PitFundamentalsBundle, cik10: str, decision: date) -> MemberFacts:
    """One in-bundle member at D, read only through the public readers (rules 8 and 9)."""
    cutoff = decision - timedelta(days=RECENCY_DAYS)
    has_event = False
    coverage: dict[str, frozenset[str]] = {}
    assets: tuple[str, Decimal] | None = None  # (end, value)
    for taxonomy, concept in pf.CONCEPT_SET:
        prefix = bundle.public_events(cik10, taxonomy, concept, decision)
        if prefix.status is not pf.ReadStatus.OK:
            raise RuntimeError(f"{cik10} {taxonomy}/{concept} at {decision}: {prefix.status}")
        has_event = has_event or bool(prefix.events)
        keys = {
            pf.FactKey(r["taxonomy"], r["concept"], r["unit"], r["start"], r["end"])
            for r in (*prefix.events, *prefix.rejections)
        }
        seen: set[str] = set()
        for key in keys:
            end = _iso_date(key.end)
            is_instant_assets = (key.taxonomy, key.concept, key.unit) == ASSETS and key.start is None
            if not is_instant_assets and (end is None or end < cutoff):
                continue
            read = bundle.value_as_of(cik10, key, decision)
            if end is not None and end >= cutoff and read.status in COVERAGE_STATUSES:
                seen.add(read.status.value)
            if is_instant_assets and read.status is pf.ReadStatus.VALUE and (assets is None or key.end > assets[0]):
                assets = (key.end, Decimal(read.values[0]))
        coverage[f"{taxonomy}/{concept}"] = frozenset(seen)
    return MemberFacts(has_event, coverage, None if assets is None else assets[1])


def size_buckets(assets: Mapping[str, Decimal]) -> dict[str, int]:
    """CIK -> decile; zero-based rank over ``(value, CIK)``; n = 0 -> no deciles."""
    ordered = sorted(assets, key=lambda cik: (assets[cik], cik))
    n = len(ordered)
    return {cik: 10 * rank // n for rank, cik in enumerate(ordered)}


def label_bucket(raw: str | None) -> str:
    if raw is None:
        return NULL_LABEL
    return raw if raw in NAMED_LABELS else OTHER_LABEL


@dataclass(frozen=True)
class Outcome:
    category: str
    labels: frozenset[str] = frozenset()
    first_date_labels: str | None = None


def outcome_at(filings: Sequence[tuple[date, str | None]], decision: date, span: tuple[date, date]) -> Outcome:
    """Issuer-level neutral outcome over Form 25 filings in ``(D, D + 730]``."""
    horizon = decision + timedelta(days=OUTCOME_HORIZON_DAYS)
    hits = [(filed, label_bucket(raw)) for filed, raw in filings if decision < filed <= horizon]
    if not hits:
        observed = span[0] <= decision and horizon <= span[1]
        return Outcome(NO_FORM25_OBSERVED if observed else UNOBSERVED_HORIZON)
    first = min(filed for filed, _ in hits)
    first_labels = "+".join(sorted({label for filed, label in hits if filed == first}))
    return Outcome(FORM25_OBSERVED, frozenset(label for _, label in hits), first_labels)


def _outcome_counts(outcome: Outcome) -> Counter[str]:
    counts: Counter[str] = Counter({outcome.category: 1})
    for label in outcome.labels:
        counts[f"label:{label}"] += 1
    if outcome.first_date_labels is not None:
        counts[f"first_date:{outcome.first_date_labels}"] += 1
    return counts


def tabulate_formation(
    decision: date,
    members: Mapping[str, str],
    facts: Mapping[str, MemberFacts],
    outcomes: Mapping[str, Outcome],
) -> dict[str, Any]:
    """``members``: CIK -> snapshot status. ``facts``: in-bundle members only."""
    status: Counter[str] = Counter()
    concepts: dict[str, Counter[str]] = {f"{t}/{c}": Counter() for t, c in pf.CONCEPT_SET}
    by_snapshot: dict[str, Counter[str]] = {}
    deciles = size_buckets({cik: f.assets for cik, f in facts.items() if f.assets is not None})
    by_size: dict[str, Counter[str]] = {}
    for cik, snapshot in sorted(members.items()):
        fact = facts.get(cik)
        historical = HAS_PUBLIC_EVENT if fact is not None and fact.has_public_event else NO_PUBLIC_EVENT
        status[f"{snapshot}/{historical}"] += 1
        counts = _outcome_counts(outcomes[cik])
        by_snapshot.setdefault(snapshot, Counter()).update(counts)
        bucket = f"decile_{deciles[cik]}" if cik in deciles else ASSETS_UNAVAILABLE
        size_row = by_size.setdefault(bucket, Counter())
        size_row["members"] += 1
        size_row.update(counts)
        if fact is None:
            continue
        for label, seen in fact.coverage.items():
            concepts[label].update(seen)
            if pf.ReadStatus.VALUE.value in seen:
                size_row[f"value:{label}"] += 1
    horizon = decision + timedelta(days=OUTCOME_HORIZON_DAYS)
    return {
        "decision": decision.isoformat(),
        "population_window": [
            (decision - timedelta(days=POPULATION_LOOKBACK_DAYS)).isoformat(),
            decision.isoformat(),
        ],
        "outcome_window": [decision.isoformat(), horizon.isoformat()],
        "members": len(members),
        "status": dict(sorted(status.items())),
        "concepts": {label: dict(sorted(c.items())) for label, c in concepts.items()},
        "assets_available": len(deciles),
        "outcomes_by_snapshot": {k: dict(sorted(v.items())) for k, v in sorted(by_snapshot.items())},
        "by_size": {k: dict(sorted(v.items())) for k, v in sorted(by_size.items())},
    }


def reconcile_ledger(rows: Mapping[str, Mapping[str, int]]) -> dict[str, dict[str, int]]:
    """Aggregate the manifest's per-(concept, unit) ledger to concepts and re-check it."""
    per_concept: dict[str, Counter[str]] = {}
    for key, counts in rows.items():
        taxonomy, concept, _unit = key.split("/", 2)
        per_concept.setdefault(f"{taxonomy}/{concept}", Counter()).update(counts)
    for label, counts in per_concept.items():
        accounted = sum(n for outcome, n in counts.items() if outcome != "raw")
        if accounted != counts["raw"]:
            raise RuntimeError(f"ledger does not reconcile for {label}: {dict(counts)}")
    return {label: dict(sorted(c.items())) for label, c in sorted(per_concept.items())}


# ------------------------------------------------------------------ process pool

_STATE: dict[str, Any] = {}


def _init_population(submissions: str, formations: tuple[date, ...]) -> None:
    _STATE["subs"] = subs = zipfile.ZipFile(submissions)
    _STATE["names"] = set(subs.namelist())
    _STATE["formations"] = formations


def _population(name: str) -> tuple[str, str | None, int]:
    subs: zipfile.ZipFile = _STATE["subs"]
    names: set[str] = _STATE["names"]
    match = _MAIN_MEMBER.fullmatch(name)
    if match is None:
        raise ValueError(f"not a submissions main member: {name!r}")
    cik10 = match.group(1)

    def read_page(page: str) -> object | None:
        return json.loads(subs.read(page)) if page in names else None

    index = pf.parse_submissions(cik10, json.loads(subs.read(name)), read_page)
    if isinstance(index, str):
        return cik10, index, 0
    return cik10, None, membership_mask(admitted_dates(index), _STATE["formations"])


def _init_facts(bundle_root: str, manifest_sha: str, formations: tuple[date, ...]) -> None:
    _STATE["bundle"] = pf.load_pit_fundamentals(Path(bundle_root), expected_manifest_sha256=manifest_sha)
    _STATE["formations"] = formations


def _facts(job: tuple[str, int]) -> tuple[str, dict[int, MemberFacts]]:
    cik10, mask = job
    formations: tuple[date, ...] = _STATE["formations"]
    return cik10, {
        i: member_facts(_STATE["bundle"], cik10, decision) for i, decision in enumerate(formations) if mask >> i & 1
    }


# ------------------------------------------------------------------ I/O


def _read_form25() -> tuple[list[tuple[str, str, str, str | None, str]], tuple[date, date]]:
    import psycopg

    from app.config import settings

    with psycopg.connect(settings.database_url) as conn:
        span = conn.execute("SELECT min(filed_date), max(filed_date) FROM sec_form25_register").fetchone()
        rows = conn.execute(
            """
            SELECT accession_number, issuer_cik, filed_date, rule_provision, form
            FROM sec_form25_register
            WHERE provision_class = %(cls)s AND issuer_cik IS NOT NULL
            ORDER BY issuer_cik, filed_date, accession_number
            """,
            {"cls": EQUITY_DELISTING},
        ).fetchall()
    if span is None or span[0] is None:
        raise SystemExit("sec_form25_register is empty")
    return [(a, cik, filed.isoformat(), rule, form) for a, cik, filed, rule, form in rows], (span[0], span[1])


def _sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True, help="evidence file; must not exist")
    parser.add_argument("--processes", type=int, default=max(1, (multiprocessing.cpu_count() or 2) - 2))
    args = parser.parse_args()
    if args.out.exists():
        raise SystemExit(f"{args.out} exists")

    bundle = pf.load_pit_fundamentals(args.bundle, expected_manifest_sha256=args.manifest_sha256)
    manifest = json.loads((args.bundle / pf.MANIFEST_FILENAME).read_bytes())
    for name, expected in sorted(manifest["input_sha256"].items()):
        if (measured := _sha256_file(args.bundle / "inputs" / f"{name}.zip")) != expected:
            raise SystemExit(f"input {name}.zip digest {measured} != manifest {expected}")
    formations = formation_dates()
    if max(formations) > bundle.supported_through:
        raise SystemExit("a formation is after the bundle's supported_through")
    ledger = reconcile_ledger(manifest["ledger"]["rows"])
    form25_rows, span = _read_form25()

    submissions = args.bundle / "inputs" / "submissions.zip"
    with zipfile.ZipFile(submissions) as archive:
        main_members = sorted(n for n in archive.namelist() if _MAIN_MEMBER.fullmatch(n))
    masks: dict[str, int] = {}
    eligibility_unknown: dict[str, str] = {}
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(args.processes, initializer=_init_population, initargs=(str(submissions), formations)) as pool:
        for cik10, failure, mask in pool.imap_unordered(_population, main_members, chunksize=512):
            if failure is not None:
                eligibility_unknown[cik10] = failure
            elif mask:
                masks[cik10] = mask
    print(f"population: {len(main_members)} submissions CIKs, {len(masks)} ever members", file=sys.stderr)

    in_bundle = set(bundle.ciks)
    jobs = sorted((cik, mask) for cik, mask in masks.items() if cik in in_bundle)
    facts: list[dict[str, MemberFacts]] = [{} for _ in formations]
    with ctx.Pool(
        args.processes, initializer=_init_facts, initargs=(str(args.bundle), args.manifest_sha256, formations)
    ) as pool:
        for cik10, per_formation in pool.imap_unordered(_facts, jobs, chunksize=16):
            for i, fact in per_formation.items():
                facts[i][cik10] = fact

    by_issuer: dict[str, list[tuple[date, str | None]]] = {}
    for _, cik, filed, rule, _ in form25_rows:
        by_issuer.setdefault(cik, []).append((date.fromisoformat(filed), rule))

    def snapshot(cik10: str) -> str:
        if cik10 in in_bundle:
            return IN_BUNDLE
        return INTEGRITY_EXCLUDED if cik10 in bundle.integrity_excluded else NO_COMPANYFACTS_ENTRY

    table = []
    for i, decision in enumerate(formations):
        members = {cik: snapshot(cik) for cik, mask in masks.items() if mask >> i & 1}
        outcomes = {cik: outcome_at(by_issuer.get(cik, ()), decision, span) for cik in members}
        table.append(tabulate_formation(decision, members, facts[i], outcomes))

    ever = set(masks)
    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "bundle_manifest_sha256": args.manifest_sha256,
        "policy": pf.policy_sha256(),
        "input_sha256": manifest["input_sha256"],
        "supported_through": bundle.supported_through.isoformat(),
        "script_sha256": _sha256_file(Path(__file__)),
        "parameters": {
            "formation_years": list(FORMATION_YEARS),
            "population_lookback_days": POPULATION_LOOKBACK_DAYS,
            "outcome_horizon_days": OUTCOME_HORIZON_DAYS,
            "recency_days": RECENCY_DAYS,
            "named_labels": list(NAMED_LABELS),
        },
        "form25": {
            "provision_class": EQUITY_DELISTING,
            "rows": len(form25_rows),
            "issuers": len(by_issuer),
            "rows_sha256": hashlib.sha256(pf.canonical_json(form25_rows)).hexdigest(),
            "register_span": [span[0].isoformat(), span[1].isoformat()],
        },
        "population": {
            "submissions_ciks": len(main_members),
            "ever_member": len(ever),
            "eligibility_unknown": dict(sorted(Counter(eligibility_unknown.values()).items())),
            "eligibility_unknown_with_companyfacts": len(
                set(eligibility_unknown) & (in_bundle | bundle.integrity_excluded)
            ),
            "bundle_ciks_never_member": len(in_bundle - ever),
        },
        "ledger": {
            "per_concept": ledger,
            "no_acceptance_by_cik": manifest["ledger"]["no_acceptance_by_cik"],
            "no_acceptance_rows": sum(manifest["ledger"]["no_acceptance_by_cik"].values()),
        },
        "formations": table,
    }
    body = json.dumps(evidence, sort_keys=True, indent=1).encode()
    with args.out.open("xb") as handle:
        handle.write(body)
    print(f"{args.out} sha256={hashlib.sha256(body).hexdigest()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
