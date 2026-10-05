"""#3609 step 1 Amendment 2c: measurement of the ``ope*`` reads (spec §"Accounting", Amendment 2c).

Measurement only: nothing here changes the panel. Sub-commands:

``tags`` lists, for the admitted CIKs of a rows file, every us-gaap concept matching ``--pattern`` (default
``TAG_PATTERN``) with a USD duration fact in a 10-K/10-Q filed 2013-01-01 .. 2021-06-30, by CIK count, read from the
pinned bundle's ``inputs/companyfacts.zip``. The witness and fallback concepts were chosen from its output.

``components`` replays the panel's period walk on every admitted name-month whose stored ``ope_be`` is
``no_period`` and counts which of today's components are absent at the latest lag-eligible period of each kind.

``build`` writes a scratch #3360 bundle from the pinned bundle's own ``inputs/`` with ``CONCEPT_SET`` extended by
exactly ``EXTRA_CONCEPTS``. Its policy hash differs from the canonical one by construction; it is never published.

``measure`` replays ``factor_panel.characteristic`` on every admitted name-month of a rows file for each of
``VARIANTS`` (r0..r5 cumulative, ``adopted``, and each rejected branch on top of ``adopted``) and the per-period
``gp_at`` reads, and writes per-row outcomes, a summary and the sha256 of the decompressed rows.

    PYTHONPATH=. uv run python scripts/measure_3609_amendment_2c.py tags --rows <rows.jsonl.gz> [--pattern <re>]
    PYTHONPATH=. uv run python scripts/measure_3609_amendment_2c.py components --rows <rows.jsonl.gz> \\
        --rows-sha256 <d> --out <summary.json>
    PYTHONPATH=. uv run python scripts/measure_3609_amendment_2c.py build --out <scratch bundle dir>
    PYTHONPATH=. uv run python scripts/measure_3609_amendment_2c.py measure --bundle <dir> --bundle-sha256 <d> \\
        --rows <rows.jsonl.gz> --rows-sha256 <d> --out <dir> [--variants r0,...]
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
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from app.services import factor_panel as fp
from app.services import pit_fundamentals

TAG_PATTERN: Final = re.compile(
    r"(?i)SellingGeneral|GeneralAndAdministrative|Selling|Marketing|ResearchAndDevelopment|^InterestExpense"
    r"|InterestAndDebtExpense|InterestCosts|^OperatingExpenses|^CostsAndExpenses|^OperatingCostsAndExpenses"
    r"|^GrossProfit|^OperatingIncomeLoss"
)
#: The pre-2c reads the r0..r3 variants and ``components`` measure from; the panel no longer holds them.
PRE_2C_COGS: Final = ("CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold")
PRE_2C_XINT: Final = ("InterestExpense",)
COMPONENTS: Final = {
    "sale": fp.SALE,
    "cogs": PRE_2C_COGS,
    "gp": fp.GP,
    "xsga": fp.XSGA,
    "xint": PRE_2C_XINT,
}

#: The adopted rule's tags live in ``factor_panel`` (slice 3d-v part 2); only the rejected XOPR branch's is here.
RD_EXCL_IPR: Final = fp.RD_EXCL_IPR
RD_SOFTWARE: Final = fp.RD_SOFTWARE
RD_INCL_IPR: Final = fp.RD_INCL_IPR
RD_WITNESSES: Final = fp.RD_WITNESSES
GA: Final = fp.GA
SELL: Final = fp.SELL
SELL_WITNESSES: Final = fp.SELL_WITNESSES
XINT_TAGS: Final = fp.XINT_TAGS
XINT_WITNESSES: Final = fp.XINT_WITNESSES
COGS_TOTAL: Final = fp.COGS_TOTAL
COGS_GOODS: Final = fp.COGS_GOODS
COGS_SERVICES: Final = fp.COGS_SERVICES
COGS_WITNESSES: Final = (*COGS_TOTAL, *COGS_GOODS, *COGS_SERVICES)
OPEX: Final = fp.OPEX
XOPR: Final = ("CostsAndExpenses",)
#: Exactly the concepts the scratch bundle adds; the canonical bundle already holds the rest.
EXTRA_CONCEPTS: Final = tuple(
    ("us-gaap", c)
    for c in sorted(
        {*RD_WITNESSES, *GA, *SELL_WITNESSES, *XINT_WITNESSES, *COGS_WITNESSES, *OPEX, *XOPR}
        - {c for _, c in pit_fundamentals.CONCEPT_SET}
    )
)
OPEX_TOLERANCE: Final = fp.OPEX_TOLERANCE
DEFAULT_PANEL: Final = ("AAPL", "GME", "HD", "JPM", "MSFT")
DEFAULT_PANEL_M: Final = "2019-06-30"


@dataclass(frozen=True)
class Variant:
    gp_branch: bool = False
    rd: bool = False
    parts: bool = False
    xint: bool = False
    cogs: bool = False
    bound: bool = False
    cogs_zero: bool = False  # rejected
    xopr: bool = False  # rejected


_R5: Final = Variant(gp_branch=True, rd=True, parts=True, xint=True, cogs=True)
_ADOPTED: Final = replace(_R5, bound=True)
#: r0..r5 are cumulative; ``adopted`` is r5 with the bound; each rejected branch is measured on ``adopted`` alone.
VARIANTS: Final = {
    "r0": Variant(),
    "r1": Variant(gp_branch=True),
    "r2": Variant(gp_branch=True, rd=True),
    "r3": Variant(gp_branch=True, rd=True, parts=True),
    "r4": Variant(gp_branch=True, rd=True, parts=True, xint=True),
    "r5": _R5,
    "adopted": _ADOPTED,
    "rejected_cogs_zero": replace(_ADOPTED, cogs_zero=True),
    "rejected_xopr": replace(_ADOPTED, xopr=True),
}


#: Period evaluations (not name-months), per variant: imputed zeros, veto refusals and the bound's outcomes. A
#: refusal inside a period whose every ``ebitda*`` branch is absent leaves no label on the characteristic, so it is
#: counted here.
EVALUATIONS: Counter[str] = Counter()
_CURRENT: list[str] = [""]


def extend_concept_set() -> None:
    """Patch the module globals the builder and the loader read at call time (scratch bundles only)."""
    extended = tuple(sorted(set(pit_fundamentals.CONCEPT_SET) | set(EXTRA_CONCEPTS)))
    pit_fundamentals.CONCEPT_SET = extended  # type: ignore[misc]
    pit_fundamentals._CONCEPTS = frozenset(extended)  # type: ignore[misc]


# --------------------------------------------------------------------------- the reads


class Period:
    """One annual period or one quarter of one CIK, with its base interval (the branch's base term)."""

    def __init__(self, view: fp.CikView, end: date, kind: fp.Kind, start: date | None) -> None:
        self.view, self.end, self.kind, self.start = view, end, kind, start

    def read(self, concepts: tuple[str, ...]) -> fp.Term:
        reader = fp.annual_flow if self.kind is fp.Kind.ANNUAL else fp.quarter_flow
        return fp.first_available((c, lambda c=c: reader(self.view, c, self.end)) for c in concepts)

    def aligned(self, term: fp.Term) -> fp.Term:
        """A value over another interval than the base's is ``absent`` (as ``companion`` treats its terms)."""
        if term.status is not fp.TermStatus.VALUE or self.start is None:
            return term
        if fp._interval_start(term, self.end, self.kind) != self.start:
            return fp.Term(fp.TermStatus.ABSENT, branches=("interval_mismatch",))
        return term

    def companion(self, read: Callable[[], fp.Term], name: str, witnesses: tuple[str, ...]) -> fp.Term:
        term = fp.companion(self.view, read, self.end, self.kind, self.start, name=name, witnesses=witnesses)
        for b in term.branches:
            if b.startswith((fp.VETO, f"zero_{name}:")):
                EVALUATIONS[f"{_CURRENT[0]}: {b.split(':')[0]}"] += 1
        return term


def _labelled(term: fp.Term, *labels: str) -> fp.Term:
    return fp.Term(term.status, term.value, term.facts, term.start, (*labels, *term.branches))


def rd_read(p: Period) -> fp.Term:
    """RD*: excluding-IPR&D + software (zero if absent, under its own witness); else the inclusive tag (proxy)."""
    excl = p.read((RD_EXCL_IPR,))
    if excl.status is fp.TermStatus.ABSENT:
        return _labelled(p.read((RD_INCL_IPR,)), "rd_incl_ipr")
    if excl.status is not fp.TermStatus.VALUE:
        return excl
    software = p.companion(lambda: p.read((RD_SOFTWARE,)), "rd_software", (RD_SOFTWARE,))
    total = fp.combine((1, excl), (1, software))
    return fp.Term(total.status, total.value, total.facts, excl.start, ("rd_excl_ipr", *total.branches))


def bounded(p: Period, total: fp.Term, *, refuse: fp.Term) -> fp.Term:
    """A sum the rule makes, tested against the filer's ``OperatingExpenses`` for the same interval.

    A blocked ``OperatingExpenses`` blocks the sum (a guard in doubt is not a pass); an absent or other-interval one
    leaves it unchecked. Above the bound, ``refuse`` is returned.
    """
    if total.status is not fp.TermStatus.VALUE:
        return total
    opex = p.aligned(p.read(OPEX))
    if opex.status in fp._BLOCKING:
        EVALUATIONS[f"{_CURRENT[0]}: opex bound blocked"] += 1
        return fp.Term(opex.status, facts=opex.facts, branches=("opex_bound_blocked",))
    if opex.status is not fp.TermStatus.VALUE:
        EVALUATIONS[f"{_CURRENT[0]}: opex bound unchecked"] += 1
        return _labelled(total, "opex_unchecked")
    assert total.value is not None and opex.value is not None
    if total.value > opex.value + abs(opex.value) * OPEX_TOLERANCE:
        EVALUATIONS[f"{_CURRENT[0]}: opex bound refused ({refuse.status.value})"] += 1
        return refuse
    EVALUATIONS[f"{_CURRENT[0]}: opex bound passed"] += 1
    return _labelled(total, "opex_checked")


def xsga_term(p: Period, v: Variant) -> fp.Term:
    sga = p.aligned(p.read(fp.XSGA))
    if not v.rd:
        return sga
    rd = p.companion(lambda: rd_read(p), "rd", RD_WITNESSES)
    if sga.status is not fp.TermStatus.ABSENT or not v.parts:
        total = fp.combine((1, sga), (1, rd))
        # The bound tests only a sum the rule makes: SG&A with a non-zero R&D added. A filed SG&A is taken as filed.
        if not v.bound or rd.status is not fp.TermStatus.VALUE or not rd.value:
            return total
        return bounded(p, total, refuse=_labelled(sga, "rd_excluded_opex_bound"))
    ga = p.aligned(p.read(GA))
    if ga.status is fp.TermStatus.ABSENT:
        return fp.combine((1, ga), (1, rd))  # a blocked R&D still blocks
    sell = p.companion(lambda: p.read(SELL), "sell", SELL_WITNESSES)
    total = _labelled(fp.combine((1, ga), (1, sell), (1, rd)), "xsga_parts")
    if not v.bound:
        return total
    refusal = fp.Term(fp.TermStatus.ABSENT, branches=(f"{fp.VETO}opex_bound:{p.start}:{p.end}:{OPEX[0]}",))
    return bounded(p, total, refuse=refusal)


def cogs_term(p: Period, v: Variant) -> fp.Term:
    if not v.cogs:
        return p.aligned(p.read(PRE_2C_COGS))
    total = p.aligned(p.read(COGS_TOTAL))
    if total.status is not fp.TermStatus.ABSENT:
        return total
    goods, services = p.aligned(p.read(COGS_GOODS)), p.aligned(p.read(COGS_SERVICES))
    if goods.status is fp.TermStatus.ABSENT and services.status is fp.TermStatus.ABSENT:
        if v.cogs_zero:
            return p.companion(lambda: fp.ABSENT, "cogs", COGS_WITNESSES)
        return fp.combine((1, goods), (1, services))
    parts = (
        p.companion(lambda: goods, "cogs_goods", COGS_GOODS),
        p.companion(lambda: services, "cogs_services", COGS_SERVICES),
    )
    return _labelled(fp.combine(*((1, t) for t in parts)), "cogs_goods_plus_services")


def xint_term(p: Period, v: Variant) -> fp.Term:
    if not v.xint:
        return p.aligned(p.read(PRE_2C_XINT))
    return p.companion(lambda: p.read(XINT_TAGS), "xint", XINT_WITNESSES)


def ope_unit(view: fp.CikView, end: date, kind: fp.Kind, v: Variant) -> fp.Term:
    """One annual period or one quarter of ``ope*`` = ``ebitda*`` − XINT*. Every part is aligned to the interval of
    the chosen ``ebitda*`` branch's base (``sale*``, or GP)."""

    def sale_branch() -> fp.Term:
        sale = p_read(fp.SALE)
        p = Period(view, end, kind, fp._interval_start(sale, end, kind))
        opex: list[tuple[str, Callable[[], fp.Term]]] = []
        if v.xopr:
            opex.append(("xopr", lambda: p.aligned(p.read(XOPR))))
        opex.append(("cogs_plus_xsga", lambda: fp.combine((1, cogs_term(p, v)), (1, xsga_term(p, v)))))
        ebitda = fp.combine((1, sale), (-1, fp._negated(fp.first_available(opex))))
        term = fp.combine((1, ebitda), (-1, fp._negated(xint_term(p, v))))
        return fp.Term(term.status, term.value, term.facts, sale.start, term.branches)

    def gp_branch() -> fp.Term:
        gp = p_read(fp.GP)
        p = Period(view, end, kind, fp._interval_start(gp, end, kind))
        ebitda = fp.combine((1, gp), (-1, fp._negated(xsga_term(p, v))))
        term = fp.combine((1, ebitda), (-1, fp._negated(xint_term(p, v))))
        return fp.Term(term.status, term.value, term.facts, gp.start, term.branches)

    def p_read(concepts: tuple[str, ...]) -> fp.Term:
        return Period(view, end, kind, None).read(concepts)

    branches: list[tuple[str, Callable[[], fp.Term]]] = [("sale_minus_opex", sale_branch)]
    if v.gp_branch:
        branches.append(("gp_minus_xsga", gp_branch))
    term = fp.first_available(branches)
    return term


def gp_unit(view: fp.CikView, end: date, kind: fp.Kind, v: Variant) -> fp.Term:
    """``gp*`` = GP, else ``sale*`` − COGS*, per period."""
    gp = Period(view, end, kind, None).read(fp.GP)
    if gp.status is not fp.TermStatus.ABSENT:
        return _labelled(gp, "GP")
    sale = Period(view, end, kind, None).read(fp.SALE)
    p = Period(view, end, kind, fp._interval_start(sale, end, kind))
    term = fp.combine((1, sale), (-1, fp._negated(cogs_term(p, v))))
    return fp.Term(term.status, term.value, term.facts, sale.start, ("sale-cogs", *term.branches))


def per_period(unit: Callable[[fp.CikView, date, fp.Kind, Variant], fp.Term], v: Variant) -> Callable[..., fp.Term]:
    def flow(view: fp.CikView, end: date, kind: fp.Kind) -> fp.Term:
        if kind is fp.Kind.ANNUAL:
            return unit(view, end, kind, v)
        return fp._chain(view, lambda e: unit(view, e, fp.Kind.QUARTERLY, v), end, 4)

    return flow


_original_compute_ratio = fp.compute_ratio


def _compute_ratio(name: str, view: fp.CikView, end: date, kind: fp.Kind) -> fp.Ratio:
    _CURRENT[0] = name
    if name.startswith("ope:"):
        return fp.Ratio(per_period(ope_unit, VARIANTS[name[4:]])(view, end, kind), fp.be(view, end))
    if name.startswith("gp:"):
        variant = VARIANTS[name[3:]]
        return fp.Ratio(per_period(gp_unit, variant)(view, end, kind), fp.instant(view, fp.AT, end))
    return _original_compute_ratio(name, view, end, kind)


def _admitted(rows: Path) -> dict[str, list[dict[str, Any]]]:
    by_cik: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(rows, "rt") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("exclusion") is None:
                by_cik[row["cik"]].append(row)
    return by_cik


def _sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def tags(argv: list[str]) -> int:
    from scripts.build_3609_factor_panel import BUNDLE

    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=Path, required=True)
    parser.add_argument("--pattern", default=TAG_PATTERN.pattern)
    args = parser.parse_args(argv)
    pattern = re.compile(args.pattern)
    ciks = set(_admitted(args.rows))
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


def _latest_eligible(view: fp.CikView, kind: fp.Kind, formation: date) -> date | None:
    return next((e for e in view.periods[kind] if fp.lag_eligible(e, formation)), None)


def components(argv: list[str]) -> int:
    from scripts.build_3609_factor_panel import BUNDLE

    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=Path, required=True)
    parser.add_argument("--rows-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    if _sha(args.rows) != args.rows_sha256:
        raise SystemExit("rows file does not match its pin")
    bundle = pit_fundamentals.load_pit_fundamentals(BUNDLE[0], expected_manifest_sha256=BUNDLE[1])
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    by_cik = _admitted(args.rows)
    for done, (cik10, rows) in enumerate(sorted(by_cik.items()), start=1):
        rows = [r for r in rows if r["characteristics"]["ope_be"]["missing"] == fp.Missing.NO_PERIOD.value]
        if not rows:
            continue
        rows.sort(key=lambda r: r["s_M"])
        cache = fp.PrefixCache(bundle, cik10, date.fromisoformat(rows[-1]["s_M"]))
        for row in rows:
            formation = date.fromisoformat(row["M"])
            view = fp.CikView(bundle, cik10, date.fromisoformat(row["s_M"]), prefixes=cache)
            counts["name_months"]["no_period"] += 1
            for kind in (fp.Kind.ANNUAL, fp.Kind.QUARTERLY):
                end = _latest_eligible(view, kind, formation)
                if end is None:
                    counts[kind.value]["no lag-eligible period"] += 1
                    continue
                status = {k: fp.flow(view, c, end, kind).status.value for k, c in COMPONENTS.items()}
                status["be"] = fp.be(view, end).status.value
                absent = sorted(k for k, s in status.items() if s == fp.TermStatus.ABSENT.value)
                other = sorted(f"{k}={s}" for k, s in status.items() if s not in ("value", "absent"))
                counts[kind.value][
                    "absent: " + (",".join(absent) or "-") + ("; " + ",".join(other) if other else "")
                ] += 1
                for k in absent:
                    counts[f"{kind.value} absent component"][k] += 1
        bundle._cache.pop(cik10, None)  # type: ignore[attr-defined]
        if done % 500 == 0:
            print(f"  {done}/{len(by_cik)} CIKs", flush=True)
    result = {
        "input_rows_sha256": args.rows_sha256,
        "bundle_sha256": BUNDLE[1],
        "counts": {k: dict(v.most_common()) for k, v in counts.items()},
    }
    args.out.write_text(json.dumps(result, indent=1) + "\n")
    print(json.dumps(result, indent=1))
    return 0


def _char_json(c: fp.Characteristic) -> dict[str, Any]:
    return {
        "value": c.value,
        "missing": None if c.missing is None else c.missing.value,
        "period_end": None if c.period_end is None else c.period_end.isoformat(),
        "kind": None if c.kind is None else c.kind.value,
        "branches": sorted({b.split(":")[0] for b in c.branches}),
        "vetoes": sorted({b.split(":")[0] for b in c.vetoes}),
    }


#: Branch labels counted on value rows: every label the amendment adds, and the fallback XINT and COGS tags.
COUNTED: Final = (
    "zero_",
    "gp_minus_xsga",
    "xopr",
    "xsga_parts",
    "cogs_goods_plus_services",
    "rd_",
    "opex_",
    "interval_mismatch",
    "InterestAndDebtExpense",
    "InterestExpenseDebt",
    *COGS_TOTAL[:1],
    *COGS_GOODS,
    *COGS_SERVICES,
)
#: Same-period value changes, per variant, for the quantiles in the summary.
DELTAS: dict[str, list[float]] = defaultdict(list)


def tally(name: str, bucket: Counter[str], cur: dict[str, Any], new: dict[str, Any]) -> None:
    state = "value" if new["value"] is not None else f"missing:{new['missing']}"
    bucket[state] += 1
    before = "value" if cur["value"] is not None else f"missing:{cur['missing']}"
    if before != state:
        bucket[f"transition {before} -> {state}"] += 1
    elif state == "value" and (cur["value"], cur["period_end"], cur["kind"]) != (
        new["value"],
        new["period_end"],
        new["kind"],
    ):
        if (cur["period_end"], cur["kind"]) == (new["period_end"], new["kind"]):
            bucket["value -> value: same period, value changed"] += 1
            DELTAS[name].append(new["value"] - cur["value"])
        else:
            bucket["value -> value: period changed"] += 1
    for veto in new["vetoes"]:
        bucket[f"  name-months with a refusal {veto}"] += 1
    if new["value"] is not None:
        for b in new["branches"]:
            if b.startswith(COUNTED):
                bucket[f"  values with {b}"] += 1


def _quantiles(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    n = len(ordered)
    out: dict[str, float | int] = {"n": n}
    if n:
        for q in (0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99):
            out[f"p{round(q * 100)}"] = ordered[min(n - 1, int(q * n))]
        out["negative"] = sum(1 for x in ordered if x < 0)
    return out


def decompositions(view: fp.CikView, bucket: Counter[str]) -> None:
    """Descriptive only, per annual anchor at the CIK's last formation: how the filer's ``OperatingExpenses``
    compares with the sums the rule makes. Every term must be a value over ``OperatingExpenses``' interval; "within"
    means within ``OPEX_TOLERANCE`` of ``OperatingExpenses``."""
    for end in view.periods[fp.Kind.ANNUAL]:
        opex = Period(view, end, fp.Kind.ANNUAL, None).read(OPEX)
        if opex.status is not fp.TermStatus.VALUE or not opex.value:
            continue
        p = Period(view, end, fp.Kind.ANNUAL, fp._interval_start(opex, end, fp.Kind.ANNUAL))
        if p.start is None:
            continue
        terms = {"sga": p.aligned(p.read(fp.XSGA)), "ga": p.aligned(p.read(GA)), "sell": p.aligned(p.read(SELL))}
        terms["rd"] = p.aligned(rd_read(p))
        v = {k: t.value for k, t in terms.items() if t.status is fp.TermStatus.VALUE and t.value is not None}

        def verdict(total: Decimal) -> str:
            assert opex.value is not None
            gap = (opex.value - total) / abs(opex.value)
            return "within" if abs(gap) <= OPEX_TOLERANCE else "opex larger" if gap > 0 else "opex SMALLER"

        if {"sga", "rd"} <= v.keys():
            bucket["opex vs sga+rd: " + verdict(v["sga"] + v["rd"])] += 1
        if terms["sga"].status is fp.TermStatus.ABSENT and {"ga", "rd"} <= v.keys():
            bucket["sga absent; opex vs ga+sell+rd: " + verdict(v["ga"] + v.get("sell", Decimal(0)) + v["rd"])] += 1


def measure(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--bundle-sha256", required=True)
    parser.add_argument("--rows", type=Path, required=True)
    parser.add_argument("--rows-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--variants", default=",".join(VARIANTS))
    args = parser.parse_args(argv)
    variants = args.variants.split(",")
    if _sha(args.rows) != args.rows_sha256:
        raise SystemExit("rows file does not match its pin")
    bundle = pit_fundamentals.load_pit_fundamentals(args.bundle, expected_manifest_sha256=args.bundle_sha256)
    fp.compute_ratio = _compute_ratio  # type: ignore[assignment]
    DELTAS.clear()
    EVALUATIONS.clear()
    by_cik = _admitted(args.rows)
    args.out.mkdir(parents=True, exist_ok=False)
    summary: dict[str, Counter[str]] = defaultdict(Counter)
    replay_mismatch: Counter[str] = Counter()
    panel: list[dict[str, Any]] = []
    rows_digest = hashlib.sha256()
    with gzip.open(args.out / "rows.jsonl.gz", "wt") as out:
        for done, (cik10, rows) in enumerate(sorted(by_cik.items()), start=1):
            rows.sort(key=lambda r: r["s_M"])
            cache = fp.PrefixCache(bundle, cik10, date.fromisoformat(rows[-1]["s_M"]))
            for row in rows:
                formation = date.fromisoformat(row["M"])
                view = fp.CikView(bundle, cik10, date.fromisoformat(row["s_M"]), prefixes=cache)
                me = Decimal(row["me"]["value"])
                record: dict[str, Any] = {"cik": cik10, "M": row["M"], "symbol": row["symbol"]}
                for name in ("ope_be", "gp_at"):
                    cur = _char_json(fp.characteristic(name, view, formation, me))
                    for f in ("value", "missing", "period_end", "kind"):
                        if cur[f] != row["characteristics"][name][f]:
                            replay_mismatch[f"{name}.{f}"] += 1
                    record[name] = cur
                    summary[f"{name} stored"]["value" if cur["value"] is not None else f"missing:{cur['missing']}"] += 1
                measured = [(f"ope:{v}", "ope_be") for v in variants] + [("gp:r0", "gp_at"), ("gp:r5", "gp_at")]
                for key, stored in measured:
                    new = _char_json(fp.characteristic(key, view, formation, me))
                    record[key] = new
                    tally(key, summary[key], record[stored], new)
                line = json.dumps(record, sort_keys=True) + "\n"
                rows_digest.update(line.encode())
                out.write(line)
                if row["M"] == DEFAULT_PANEL_M and row["symbol"] in DEFAULT_PANEL:
                    panel.append(record)
            _CURRENT[0] = "decompositions"
            decompositions(
                fp.CikView(bundle, cik10, date.fromisoformat(rows[-1]["s_M"]), prefixes=cache),
                summary["decompositions (annual anchors, descriptive)"],
            )
            bundle._cache.pop(cik10, None)  # type: ignore[attr-defined]
            if done % 500 == 0:
                print(f"  {done}/{len(by_cik)} CIKs", flush=True)
    result = {
        "bundle_sha256": args.bundle_sha256,
        "input_rows_sha256": args.rows_sha256,
        "output_rows_sha256": rows_digest.hexdigest(),
        "admitted_rows": sum(len(v) for v in by_cik.values()),
        "replay_mismatch_vs_stored": dict(replay_mismatch),
        "counts": {k: dict(sorted(v.items())) for k, v in summary.items()},
        "period_evaluations": dict(sorted(EVALUATIONS.items())),
        "same_period_change_quantiles": {k: _quantiles(v) for k, v in sorted(DELTAS.items())},
        "default_panel": sorted(panel, key=lambda r: r["symbol"]),
    }
    (args.out / "summary.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "default_panel"}, indent=1))
    return 1 if replay_mismatch else 0


def main() -> int:
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command in ("build", "measure"):
        extend_concept_set()
    if command == "tags":
        return tags(sys.argv[2:])
    if command == "components":
        return components(sys.argv[2:])
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
    raise SystemExit("usage: tags | components | build | measure")


if __name__ == "__main__":
    raise SystemExit(main())
