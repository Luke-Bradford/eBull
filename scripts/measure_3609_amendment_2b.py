"""#3609 step 1 Amendment 2b: period-level measurement of the ``ni*`` = IB, else NI − ``xido*`` and the ``ocf*`` =
OANCF, else continuing + discontinued branches (spec §"Accounting", Amendment 2b).

Measurement only: nothing here changes the panel. Three sub-commands.

``tags`` lists, for the admitted CIKs of a rows file, every us-gaap concept whose name contains "Discontinued" or
"Extraordinary" with a USD duration fact in a 10-K/10-Q filed 2013-01-01 .. 2021-06-30, by CIK count, read from the
pinned bundle's ``inputs/companyfacts.zip``. The witness concepts were chosen from its output.

``build`` writes a scratch #3360 bundle from the pinned bundle's own ``inputs/`` with ``CONCEPT_SET`` extended by
exactly ``EXTRA_CONCEPTS``. Its policy hash differs from the canonical one by construction; it is never published or
read by the panel.

``measure`` replays the panel's own period choice (``factor_panel.characteristic``) on every admitted name-month of
a panel rows file, for the current ``ni_me`` / ``ocf_me`` and for each variant below, and writes per-row outcomes, a
summary and the sha256 of the decompressed rows (the slice 3d-iv oracle). The input rows and the scratch bundle are
bound by sha256.

Closed research: it reproduces at ``62602dca``. From slice 3d-iv part 2 the panel's own ``ni_me`` / ``ocf_me`` are
the adopted variants (``factor_panel.fallback_flow``), so the "current" readings here no longer mean IB / OANCF alone.

    PYTHONPATH=. uv run python scripts/measure_3609_amendment_2b.py tags --rows <rows.jsonl.gz>
    PYTHONPATH=. uv run python scripts/measure_3609_amendment_2b.py build --out <scratch bundle dir>
    PYTHONPATH=. uv run python scripts/measure_3609_amendment_2b.py measure --bundle <dir> --bundle-sha256 <d> \\
        --rows <rows.jsonl.gz> --rows-sha256 <d> --out <dir>
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from app.services import factor_panel as fp
from app.services import pit_fundamentals

#: Branch concepts (Amendment 2b).
XIDO_PARENT = "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity"
XIDO_CONSOLIDATED = "IncomeLossFromDiscontinuedOperationsNetOfTax"
OCF_CONTINUING = "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
OCF_DISCONTINUED = "CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations"
#: Read as a term (JKP ``xido*`` = XI + DO) and as a witness.
XI = "ExtraordinaryItemNetOfTax"
#: Read as a term (consolidated DO minus it restores the parent scope) and as a witness.
DO_NCI = "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToNoncontrollingInterest"
#: Witness-only concepts, chosen from the admitted CIKs' USD duration tags (``tags``).
DO_DETAIL = (
    "DiscontinuedOperationIncomeLossFromDiscontinuedOperationBeforeIncomeTax",
    "DiscontinuedOperationGainLossOnDisposalOfDiscontinuedOperationNetOfTax",
    "DiscontinuedOperationIncomeLossFromDiscontinuedOperationDuringPhaseOutPeriodNetOfTax",
)
DISCONTINUED_CASH = "NetCashProvidedByUsedInDiscontinuedOperations"
#: Exactly the 10 concepts the amendment adds; the scratch bundle holds these and nothing else extra.
EXTRA_CONCEPTS = tuple(
    ("us-gaap", c)
    for c in (
        XIDO_PARENT,
        XIDO_CONSOLIDATED,
        OCF_CONTINUING,
        OCF_DISCONTINUED,
        XI,
        DO_NCI,
        *DO_DETAIL,
        DISCONTINUED_CASH,
    )
)

NI = "NetIncomeLoss"
IB_CONT = fp.IB[0]
OCF_TOTAL = fp.OANCF[0]
#: Witnesses veto an imputed zero: a public non-zero fact on one of these overlaps the interval.
XI_WITNESSES: Final = (XI,)
DO_WITNESSES: Final = (XIDO_PARENT, XIDO_CONSOLIDATED, DO_NCI, *DO_DETAIL)
#: Conservative: discontinued operations existing in the interval leaves a zero operating cash flow unsupported.
OCF_DO_WITNESSES: Final = (OCF_DISCONTINUED, DISCONTINUED_CASH, *DO_WITNESSES)
#: ASU 2015-01 eliminated extraordinary items for fiscal years beginning after 2015-12-15. An interval starting on
#: or after these dates lies in such a fiscal year whatever the filer's year end (a fiscal year spans <= 12 months).
ASU_2015_01_ANNUAL_START: Final = date(2015, 12, 16)
ASU_2015_01_QUARTER_START: Final = date(2016, 12, 16)
DEFAULT_PANEL: Final = ("AAPL", "GME", "HD", "JPM", "MSFT")
DEFAULT_PANEL_M: Final = "2019-06-30"

Reader = Callable[[fp.CikView, str, date], fp.Term]


@dataclass(frozen=True)
class Variant:
    name: str
    characteristic: str  # ni_me / ocf_me
    zero: bool  # an absent companion may become zero
    witnessed: bool  # ... only when no witness overlaps


VARIANTS: Final = (
    Variant("ni_strict", "ni_me", zero=False, witnessed=False),
    Variant("ni_zero", "ni_me", zero=True, witnessed=False),
    Variant("ni_adopted", "ni_me", zero=True, witnessed=True),
    Variant("ocf_strict", "ocf_me", zero=False, witnessed=False),
    Variant("ocf_zero", "ocf_me", zero=True, witnessed=False),
    Variant("ocf_adopted", "ocf_me", zero=True, witnessed=True),
)
_BY_NAME: Final = {v.name: v for v in VARIANTS}

#: Period evaluations, per variant (not name-months): reset at ``measure`` entry.
EVALUATIONS: Counter[str] = Counter()
_CURRENT: list[str] = [""]


def extend_concept_set() -> None:
    """Patch the module globals the builder and the loader read at call time (scratch bundles only)."""
    extended = tuple(sorted(set(pit_fundamentals.CONCEPT_SET) | set(EXTRA_CONCEPTS)))
    pit_fundamentals.CONCEPT_SET = extended  # type: ignore[misc]
    pit_fundamentals._CONCEPTS = frozenset(extended)  # type: ignore[misc]


# --------------------------------------------------------------------------- the branches


def _start(term: fp.Term, end: date, kind: fp.Kind) -> date | None:
    """The interval start of a VALUE term ending at ``end``."""
    if term.status is not fp.TermStatus.VALUE:
        return None
    if kind is fp.Kind.QUARTERLY:
        return term.start
    starts = {f.key.start for f in term.facts if f.key.start is not None and f.key.end == end.isoformat()}
    return date.fromisoformat(min(starts)) if len(starts) == 1 else None


def witnessed(view: fp.CikView, concepts: tuple[str, ...], start: date, end: date) -> list[str]:
    """Concepts with a public fact overlapping [start, end] whose CURRENT value is non-zero.

    ``view.prefix`` is the acceptance-NY-date < s(M) slice (``PrefixCache.at``), so a later filing cannot veto an
    earlier formation. Per key, only the rows at the key's latest public acceptance count, so a non-zero fact later
    corrected to zero does not veto.
    """
    hits: list[str] = []
    for concept in concepts:
        latest: dict[tuple[str, str], tuple[str, set[str]]] = {}
        for row in view.prefix("us-gaap", concept).events:
            if row["unit"] != fp.USD or row["start"] is None:
                continue
            key = (row["start"], row["end"])
            held = latest.get(key)
            if held is None or row["acceptance"] > held[0]:
                latest[key] = (row["acceptance"], {row["value"]})
            elif row["acceptance"] == held[0]:
                held[1].add(row["value"])
        for (s, e), (_, values) in latest.items():
            if date.fromisoformat(s) <= end and date.fromisoformat(e) >= start and any(Decimal(v) != 0 for v in values):
                hits.append(concept)
                break
    return hits


def _label(term: fp.Term, *labels: str) -> fp.Term:
    return fp.Term(term.status, term.value, term.facts, term.start, (*labels, *term.branches))


def companion(
    view: fp.CikView,
    reader: Reader,
    read: Callable[[], fp.Term],
    end: date,
    kind: fp.Kind,
    start: date | None,
    *,
    name: str,
    witnesses: tuple[str, ...],
    variant: Variant,
) -> fp.Term:
    """A deducted or added term. Only ``absent`` may become zero; blocking states are returned for ``combine``."""
    term = read()
    if term.status is fp.TermStatus.VALUE:
        if start is not None and _start(term, end, kind) != start:
            EVALUATIONS[f"{variant.name}: interval mismatch"] += 1
            return fp.ABSENT
        return _label(term, f"{name}_filed_zero" if term.value == 0 else f"{name}_filed")
    if term.status is not fp.TermStatus.ABSENT or not variant.zero:
        return term
    if start is None:
        # No base interval to test the zero against: the base is blocked or absent (``combine`` returns its status
        # either way), or an annual base with no single start, which stays missing.
        return fp.ABSENT
    if variant.witnessed and witnessed(view, witnesses, start, end):
        EVALUATIONS[f"{variant.name}: zero refused by a witness ({name})"] += 1
        return fp.ABSENT
    return fp.Term(fp.TermStatus.VALUE, Decimal(0), branches=(f"zero_{name}:{start}:{end}",))


def do_read(view: fp.CikView, reader: Reader, end: date) -> fp.Term:
    """DO: the parent tag; else consolidated minus the noncontrolling share; else consolidated (proxy)."""
    parent = reader(view, XIDO_PARENT, end)
    if parent.status is not fp.TermStatus.ABSENT:
        return _label(parent, "do_parent")
    consolidated = reader(view, XIDO_CONSOLIDATED, end)
    if consolidated.status is fp.TermStatus.ABSENT:
        return fp.ABSENT
    nci = reader(view, DO_NCI, end)
    if nci.status is fp.TermStatus.ABSENT:
        return _label(consolidated, "do_consolidated_proxy")
    term = fp.combine((1, consolidated), (-1, nci))
    return fp.Term(
        term.status, term.value, term.facts, consolidated.start, ("do_consolidated_minus_nci", *term.branches)
    )


def unit_term(view: fp.CikView, end: date, kind: fp.Kind, variant: Variant) -> fp.Term:
    """One period (an annual period, or one quarter before the TTM chain)."""
    reader: Reader = fp.annual_flow if kind is fp.Kind.ANNUAL else fp.quarter_flow
    if variant.characteristic == "ni_me":
        primary = reader(view, IB_CONT, end)
        if primary.status is not fp.TermStatus.ABSENT:
            return _label(primary, "ib")
        base = reader(view, NI, end)
        start = _start(base, end, kind)
        xi = companion(
            view,
            reader,
            lambda: reader(view, XI, end),
            end,
            kind,
            start,
            name="xi",
            witnesses=XI_WITNESSES,
            variant=variant,
        )
        do = companion(
            view,
            reader,
            lambda: do_read(view, reader, end),
            end,
            kind,
            start,
            name="do",
            witnesses=DO_WITNESSES,
            variant=variant,
        )
        term = fp.combine((1, base), (-1, xi), (-1, do))
        label = "ni_minus_xido"
    else:
        primary = reader(view, OCF_TOTAL, end)
        if primary.status is not fp.TermStatus.ABSENT:
            return _label(primary, "oancf")
        base = reader(view, OCF_CONTINUING, end)
        start = _start(base, end, kind)
        disc = companion(
            view,
            reader,
            lambda: reader(view, OCF_DISCONTINUED, end),
            end,
            kind,
            start,
            name="ocf_disc",
            witnesses=OCF_DO_WITNESSES,
            variant=variant,
        )
        term = fp.combine((1, base), (1, disc))
        label = "continuing_plus_discontinued"
    out_start = base.start if kind is fp.Kind.QUARTERLY else None
    return fp.Term(term.status, term.value, term.facts, out_start, (label, *term.branches))


def variant_flow(view: fp.CikView, end: date, kind: fp.Kind, variant: Variant) -> fp.Term:
    """The branch is chosen per period: per quarter before the TTM chain."""
    if kind is fp.Kind.ANNUAL:
        return unit_term(view, end, kind, variant)
    return fp._chain(view, lambda e: unit_term(view, e, fp.Kind.QUARTERLY, variant), end, 4)


_original_compute_ratio = fp.compute_ratio


def _compute_ratio(name: str, view: fp.CikView, end: date, kind: fp.Kind) -> fp.Ratio:
    variant = _BY_NAME.get(name)
    if variant is None:
        return _original_compute_ratio(name, view, end, kind)
    EVALUATIONS[f"{variant.name}: periods evaluated"] += 1
    return fp.Ratio(variant_flow(view, end, kind, variant), None)


# --------------------------------------------------------------------------- classification


def _zeros(branches: tuple[str, ...], name: str) -> list[tuple[date, date]]:
    out = []
    for b in branches:
        if b.startswith(f"zero_{name}:"):
            _, s, e = b.split(":")
            out.append((date.fromisoformat(s), date.fromisoformat(e)))
    return out


def _companion_names(variant: Variant) -> tuple[str, ...]:
    return ("xi", "do") if variant.characteristic == "ni_me" else ("ocf_disc",)


def _witnesses_for(name: str) -> tuple[str, ...]:
    return {"xi": XI_WITNESSES, "do": DO_WITNESSES, "ocf_disc": OCF_DO_WITNESSES}[name]


def classify(view: fp.CikView, c: fp.Characteristic, variant: Variant) -> dict[str, Any]:
    """Per-name-month facts about a variant's chosen reading (value rows only for the branch fields)."""
    out: dict[str, Any] = _char_json(c)
    branches = c.branches
    fallback_label = "ni_minus_xido" if variant.characteristic == "ni_me" else "continuing_plus_discontinued"
    primary_label = "ib" if variant.characteristic == "ni_me" else "oancf"
    out["fallback"] = fallback_label in branches
    out["mixed_ttm"] = out["fallback"] and primary_label in branches
    zeros = {name: _zeros(branches, name) for name in _companion_names(variant)}
    out["imputed"] = sorted(name for name, z in zeros.items() if z)
    out["filed_zero"] = sorted(name for name in _companion_names(variant) if f"{name}_filed_zero" in branches)
    # Disjoint partition by precedence: imputed zero > non-imputed (filed or derived) zero > filed non-zero only.
    if not out["fallback"]:
        out["partition"] = None
    elif out["imputed"]:
        out["partition"] = "imputed_zero"
    elif out["filed_zero"]:
        out["partition"] = "non_imputed_zero"
    else:
        out["partition"] = "non_zero"
    out["do_branch"] = sorted(b for b in set(branches) if b.startswith("do_") and not b.startswith("do_filed"))
    out["overlapping_witness"] = sorted(
        {h for name, z in zeros.items() for s, e in z for h in witnessed(view, _witnesses_for(name), s, e)}
    )
    out["xi_zero_pre_asu"] = any(
        s < (ASU_2015_01_ANNUAL_START if c.kind is fp.Kind.ANNUAL else ASU_2015_01_QUARTER_START)
        for s, _ in zeros.get("xi", [])
    )
    return out


def _char_json(c: fp.Characteristic) -> dict[str, Any]:
    return {
        "value": c.value,
        "missing": None if c.missing is None else c.missing.value,
        "period_end": None if c.period_end is None else c.period_end.isoformat(),
        "kind": None if c.kind is None else c.kind.value,
    }


def _state(c: Mapping[str, Any]) -> str:
    return "value" if c["value"] is not None else f"missing:{c['missing']}"


def _sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def tally(bucket: Counter[str], cur: Mapping[str, Any], new: Mapping[str, Any]) -> None:
    fields = ("value", "missing", "period_end", "kind")
    if all(cur[f] == new[f] for f in fields):
        bucket["transition unchanged"] += 1
    else:
        bucket[f"transition {_state(cur)} -> {_state(new)}"] += 1
        if cur["value"] is not None and new["value"] is not None:
            if new["period_end"] != cur["period_end"]:
                direction = "newer" if new["period_end"] > cur["period_end"] else "older"
                bucket[f"value -> value: period end {direction}"] += 1
            elif new["kind"] != cur["kind"]:
                bucket["value -> value: same period end, kind changed"] += 1
            else:
                bucket["value -> value: same period end and kind, value changed"] += 1
        if cur["value"] is not None and new["value"] is None:
            direction = "newer" if (new["period_end"] or "") > (cur["period_end"] or "") else "not newer"
            bucket[f"value -> missing: chosen period end {direction}"] += 1
    if new["value"] is None:
        return
    bucket["values"] += 1
    if not new["fallback"]:
        return
    bucket["values using the fallback (name-months)"] += 1
    bucket[f"  partition {new['partition']}"] += 1
    if new["mixed_ttm"]:
        bucket["  TTM mixing primary and fallback quarters"] += 1
    for name in new["imputed"]:
        bucket[f"  imputed zero: {name}"] += 1
    for branch in new["do_branch"]:
        bucket[f"  DO branch: {branch}"] += 1
    if new["overlapping_witness"]:
        bucket["  imputed zero with an overlapping non-zero witness"] += 1
        for concept in new["overlapping_witness"]:
            bucket[f"    witness {concept}"] += 1
    if new["xi_zero_pre_asu"]:
        bucket["  imputed XI zero in an interval not certainly under ASU 2015-01"] += 1


# --------------------------------------------------------------------------- sub-commands


def measure(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--bundle-sha256", required=True)
    parser.add_argument("--rows", type=Path, required=True)
    parser.add_argument("--rows-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    if _sha(args.rows) != args.rows_sha256:
        raise SystemExit("rows file does not match its pin")
    # ``load_pit_fundamentals`` also refuses a manifest digest mismatch (``pit_fundamentals.py``); checked here too
    # so the pin is visible where it is passed. Shards are digest-verified by the loader on first read.
    if _sha(args.bundle / pit_fundamentals.MANIFEST_FILENAME) != args.bundle_sha256:
        raise SystemExit("bundle manifest does not match its pin")
    bundle = pit_fundamentals.load_pit_fundamentals(args.bundle, expected_manifest_sha256=args.bundle_sha256)
    fp.compute_ratio = _compute_ratio  # type: ignore[assignment]
    EVALUATIONS.clear()
    by_cik: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(args.rows, "rt") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("exclusion") is None:
                by_cik[row["cik"]].append(row)
    args.out.mkdir(parents=True, exist_ok=False)
    summary: dict[str, Counter[str]] = defaultdict(Counter)
    replay_mismatch: Counter[str] = Counter()
    panel: list[dict[str, Any]] = []
    rows_path = args.out / "rows.jsonl.gz"
    rows_digest = hashlib.sha256()
    with gzip.open(rows_path, "wt") as out:
        for done, (cik10, rows) in enumerate(sorted(by_cik.items()), start=1):
            rows.sort(key=lambda r: r["s_M"])
            cache = fp.PrefixCache(bundle, cik10, date.fromisoformat(rows[-1]["s_M"]))
            for row in rows:
                formation = date.fromisoformat(row["M"])
                view = fp.CikView(bundle, cik10, date.fromisoformat(row["s_M"]), prefixes=cache)
                me = Decimal(row["me"]["value"])
                record: dict[str, Any] = {"cik": cik10, "M": row["M"], "symbol": row["symbol"]}
                for name in ("ni_me", "ocf_me"):
                    cur = _char_json(fp.characteristic(name, view, formation, me))
                    stored = row["characteristics"][name]
                    for f in ("value", "missing", "period_end", "kind"):
                        if cur[f] != stored[f]:
                            replay_mismatch[f"{name}.{f}"] += 1
                    record[name] = cur
                    summary[name]["values"] += cur["value"] is not None
                for variant in VARIANTS:
                    new = classify(view, fp.characteristic(variant.name, view, formation, me), variant)
                    record[variant.name] = new
                    tally(summary[variant.name], record[variant.characteristic], new)
                line = json.dumps(record, sort_keys=True) + "\n"
                rows_digest.update(line.encode())
                out.write(line)
                if row["M"] == DEFAULT_PANEL_M and row["symbol"] in DEFAULT_PANEL:
                    panel.append(record)
            bundle._cache.pop(cik10, None)  # type: ignore[attr-defined]
            if done % 500 == 0:
                print(f"  {done}/{len(by_cik)} CIKs", flush=True)
    result = {
        "bundle_sha256": args.bundle_sha256,
        "input_rows_sha256": args.rows_sha256,
        # Over the decompressed JSONL: the gzip header carries a timestamp, so the compressed file's digest is not
        # reproducible across runs.
        "output_rows_sha256": rows_digest.hexdigest(),
        "admitted_rows": sum(len(v) for v in by_cik.values()),
        "replay_mismatch_vs_stored": dict(replay_mismatch),
        "period_evaluations": dict(sorted(EVALUATIONS.items())),
        "counts": {k: dict(sorted(v.items())) for k, v in summary.items()},
        "default_panel": sorted(panel, key=lambda r: r["symbol"]),
    }
    # ``characteristic`` resolves ``compute_ratio`` at call time; refuse if the patch never reached a variant.
    unreached = [v.name for v in VARIANTS if not EVALUATIONS[f"{v.name}: periods evaluated"]]
    if unreached:
        raise SystemExit(f"compute_ratio patch never reached {unreached}")
    (args.out / "summary.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "default_panel"}, indent=1))
    return 1 if replay_mismatch else 0


def tags(argv: list[str]) -> int:
    from scripts.build_3609_factor_panel import BUNDLE

    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=Path, required=True)
    args = parser.parse_args(argv)
    ciks = set()
    with gzip.open(args.rows, "rt") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("exclusion") is None:
                ciks.add(row["cik"])
    pattern = re.compile(r"(?i)discontinued|extraordinary")
    by_concept: dict[str, set[str]] = defaultdict(set)
    missing_members = 0
    with zipfile.ZipFile(BUNDLE[0] / "inputs/companyfacts.zip") as archive:
        for cik in sorted(ciks):
            try:
                payload = json.loads(archive.read(f"CIK{cik}.json"))
            except KeyError:
                missing_members += 1
                continue
            for concept, body in payload.get("facts", {}).get("us-gaap", {}).items():
                if pattern.search(concept) and any(
                    f.get("start")
                    and str(f.get("form", "")).startswith(("10-K", "10-Q"))
                    and "2013-01-01" <= str(f.get("filed", "")) <= "2021-06-30"
                    for f in body.get("units", {}).get("USD", [])
                ):
                    by_concept[concept].add(cik)
    print(f"admitted CIKs {len(ciks)}; without a companyfacts member {missing_members}")
    for concept, members in sorted(by_concept.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        print(len(members), concept)
    return 0


def main() -> int:
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    extend_concept_set()
    if command == "tags":
        return tags(sys.argv[2:])
    if command == "build":
        from scripts import build_3360_pit_fundamentals
        from scripts.build_3609_factor_panel import BUNDLE

        sys.argv = [
            "build",
            "--companyfacts",
            str(BUNDLE[0] / "inputs/companyfacts.zip"),
            "--submissions",
            str(BUNDLE[0] / "inputs/submissions.zip"),
            *sys.argv[2:],
        ]
        return build_3360_pit_fundamentals.main()
    if command == "measure":
        return measure(sys.argv[2:])
    raise SystemExit("usage: tags | build | measure")


if __name__ == "__main__":
    raise SystemExit(main())
