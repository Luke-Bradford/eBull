"""#3609 step 1 slice 4: the construction-fidelity report, a counted, non-claiming trial.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Fidelity", §"Alignment matrix", §"Census",
§"Registration, ledger and what step 2 inherits" and the slice 4 plan under §"Slices". The comparison logic is in
``app/services/factor_panel_fidelity.py``; this script reads one published stage-A artefact, gates the run on the
trial register and the ledger, and prints the verdicts.

It never prints or stores a monthly factor value, a mean, a cumulative return or the offset bar's magnitude.

Run (from a clean checkout): ``PYTHONPATH=. uv run python -m scripts.report_3609_fidelity --artefact <dir>
--manifest-sha256 <hex> [--census-form25] [--revision-reason TEXT]``.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import statistics
import sys
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.factor_panel import PanelError, formation_months
from app.services.factor_panel_artefact import (
    construction_versions,
    read_gz_lines,
    sha256_file,
    write_gz_lines,
    write_json_once,
)
from app.services.factor_panel_fidelity import (
    ACCOUNTING,
    CHARACTERISTICS,
    PRICE_CHARACTERISTICS,
    FactorMonth,
    FidelityError,
    Form25Record,
    Holding,
    SeriesAccumulator,
    Verdict,
    append_ledger,
    delayed_name_months,
    evidence_sha256,
    factor_month,
    fidelity_evidence,
    form25_table,
    month_last_day,
    read_ledger,
    register_gate,
    version_history,
    versions_digest,
)
from app.services.factor_panel_reference import load_table9_signs
from app.services.security_linkage import Reason, load_security_linkage
from app.services.trial_register import TRIAL_REGISTER
from app.system.git_identity import head_commit, is_dirty
from scripts.build_3609_factor_panel import (
    LINKAGE,
    OUT_DIR,
    REPO_ROOT,
    SPEC_PATH,
    TRIAL_REGISTER_PATH,
    Frozen,
    construction_sources,
    holding_month,
    verify_artefact,
)

LEDGER_PATH: Final = OUT_DIR / "ledger.jsonl"
COMMITTED_LEDGER_PATH: Final = REPO_ROOT / "docs/research/3609-ledger.jsonl"
RESULTS_DIR: Final = OUT_DIR / "fidelity"
RESULTS_SCHEMA: Final = "fidelity-3609-v1"
JKP_RETURNS: Final = "jkp_usa_monthly_vw_cap"
JKP_CUTOFFS: Final = "jkp_nyse_cutoffs"
TABLE9_FROZEN: Final = Path(Frozen.REFERENCE) / "inputs" / "3609-jkp-table9-signs.csv"
#: Premise 1: the selected Form 25 reference population ends with filings before this date.
FORM25_FILED_BEFORE: Final = date(2024, 9, 1)
INTRADER: Final = "icyDenev/Intrader"
#: Every label step 2 inherits (§"What step 2 inherits", "Labels"), printed with every result.
POPULATION_LABELS: Final = (
    "restricted population: 10-K/10-Q filers our archive prices and our linkage identifies, not CRSP",
    "retrospectively filtered: undated integrity masks (premise 4)",
    "survivorship: survivor-conditioned before 2014-09; terminations recorded, coverage unverified 2014-09..2018; "
    "checked against a selected Form 25 set from 2019 (premise 1)",
)

_FORM25_SQL = """
SELECT DISTINCT ON (issuer_cik, resolved_symbol)
       issuer_cik, resolved_symbol, filed_date, coalesce(suspension_date, filed_date) AS event_date
  FROM sec_form25_common_equity_delistings
 WHERE resolved_symbol IS NOT NULL AND filed_date < %(before)s
 ORDER BY issuer_cik, resolved_symbol, filed_date, accession_number
"""
_FORM25_SERIES_SQL = """
SELECT vendor_symbol, last_bar
  FROM research_price_series
 WHERE vendor = %(vendor)s AND last_bar IS NOT NULL AND vendor_symbol = ANY(%(symbols)s)
"""


# --------------------------------------------------------------------------- gates


def current_versions() -> tuple[str, dict[str, str]]:
    spec_sha256 = sha256_file(SPEC_PATH)
    return spec_sha256, construction_versions(list(CHARACTERISTICS), spec_sha256, construction_sources())


def check_artefact(manifest: Mapping[str, Any], spec_sha256: str, versions: Mapping[str, str]) -> None:
    if manifest.get("stage") != "A":
        raise FidelityError("artefact_stage", "the artefact is not stage A")
    if manifest["formations"] != [m.isoformat() for m in formation_months()]:
        raise FidelityError("artefact_grid", "the artefact's formations are not the 80 stage-A formations")
    if manifest["spec_sha256"] != spec_sha256:
        raise FidelityError("artefact_spec", "the artefact was built under another spec")
    if manifest["construction_versions"] != dict(versions):
        raise FidelityError("artefact_versions", "the artefact's construction versions differ from this code's")


# --------------------------------------------------------------------------- reads


def _snapshot(artefact: Path, dataset: str) -> list[list[str]]:
    return list(read_gz_lines(artefact / "inputs" / Frozen.snapshot(dataset)))


def read_cutoffs(artefact: Path) -> dict[str, dict[date, float]]:
    """``nyse_p20`` and ``nyse_p80`` by month-end, in USD (the snapshot is USD millions)."""
    out: dict[str, dict[date, float]] = {"nyse_p20": {}, "nyse_p80": {}}
    for name, day, value, unit in _snapshot(artefact, JKP_CUTOFFS):
        if name in out:
            if unit != "usd_millions":
                raise FidelityError("cutoff_unit", f"{name} unit {unit!r}, expected usd_millions")
            out[name][date.fromisoformat(day)] = float(value) * 1e6
    return out


def read_published(artefact: Path, grid: Sequence[str]) -> dict[str, dict[str, float]]:
    """JKP's published factor returns on the grid, keyed by the holding month whose last day dates the row."""
    on_grid = set(grid)
    out: dict[str, dict[str, float]] = {name: {} for name in CHARACTERISTICS}
    for name, day, value, unit in _snapshot(artefact, JKP_RETURNS):
        if name not in out:
            continue
        when = date.fromisoformat(day)
        key = f"{when.year:04d}-{when.month:02d}"
        if when != month_last_day(key):
            raise FidelityError("published_date", f"{name} row dated {day} is not a month-end")
        if key not in on_grid:
            continue
        if unit != "decimal_return":
            raise FidelityError("published_unit", f"{name} unit {unit!r}, expected decimal_return")
        out[name][key] = float(value)
    return out


_DAILY_HEAD: Final = re.compile(r'^\[(\d+),\[(?:\["(\d{4}-\d{2}-\d{2})")?')


def first_daily_bars(artefact: Path) -> dict[int, date]:
    """Each series' first frozen daily bar, read from the line head only (the lines hold whole series)."""
    out: dict[int, date] = {}
    with gzip.open(artefact / "inputs" / Frozen.DAILY, "rt", encoding="utf-8") as handle:
        for line in handle:
            match = _DAILY_HEAD.match(line)
            if match is None:
                raise FidelityError("daily_format", "a frozen daily line does not start [series_id,[...")
            if match.group(2) is not None:
                out[int(match.group(1))] = date.fromisoformat(match.group(2))
    return out


class Panel:
    """What the report keeps from the rows: compact admitted holdings per formation, and diagnostics inputs."""

    def __init__(self) -> None:
        #: M -> [(name_key, me, status, by_arm, {characteristic: value or None})]
        self.by_formation: dict[date, list[tuple[int, float, str, dict[str, float], dict[str, float | None]]]] = (
            defaultdict(list)
        )
        #: characteristic -> series -> [(M, period end used or None)]
        self.periods: dict[str, dict[int, list[tuple[date, date | None]]]] = {c: defaultdict(list) for c in ACCOUNTING}
        self.unquoted: list[tuple[int, date]] = []
        self.rows = 0

    def add(self, row: Mapping[str, Any]) -> None:
        self.rows += 1
        formation = date.fromisoformat(row["M"])
        if row["holding_month"] != holding_month(formation):
            raise FidelityError("row_holding_month", f"series {row['series_id']}: holding month does not follow M")
        if row["exclusion"] == "not_priced":
            self.unquoted.append((row["series_id"], date.fromisoformat(row["s_M"])))
        if row["exclusion"] is not None:
            return
        chars = row["characteristics"]
        prices = row["prices"]
        values: dict[str, float | None] = {c: chars[c]["value"] for c in ACCOUNTING}
        values.update({c: prices[c]["value"] for c in PRICE_CHARACTERISTICS})
        for c in ACCOUNTING:
            used = chars[c]["period_end"] if chars[c]["value"] is not None else None
            self.periods[c][row["series_id"]].append((formation, None if used is None else date.fromisoformat(used)))
        holding = prices["holding"]
        self.by_formation[formation].append(
            (row["name_key"], float(row["me"]["value"]), holding["status"], dict(holding["by_arm"]), values)
        )


def read_panel(artefact: Path, manifest: Mapping[str, Any]) -> Panel:
    panel = Panel()
    seen: set[tuple[str, int]] = set()
    for row in read_gz_lines(artefact / manifest["rows"]["path"]):
        key = (row["M"], row["series_id"])
        if key in seen:
            raise FidelityError("row_unique", f"(M, series) {key} is repeated")
        seen.add(key)
        panel.add(row)
    if panel.rows != manifest["rows"]["count"]:
        raise FidelityError("row_count", "the rows read differ from the manifest's count")
    return panel


# --------------------------------------------------------------------------- evaluation


def evaluate(
    panel: Panel,
    cutoffs: Mapping[str, Mapping[date, float]],
    signs: Mapping[str, int],
    published: Mapping[str, Mapping[str, float]],
    names: Sequence[str],
) -> dict[str, Any]:
    formations = formation_months()
    grid = [holding_month(m) for m in formations]
    accumulators = {name: SeriesAccumulator(grid) for name in names}
    for formation in formations:
        p20, p80 = cutoffs["nyse_p20"].get(formation), cutoffs["nyse_p80"].get(formation)
        if p20 is None or p80 is None:
            raise FidelityError("cutoff_missing", f"no NYSE cutoff dated {formation}")
        admitted = panel.by_formation.get(formation, [])
        for name in names:
            holdings = [
                Holding(key, value, me, status, by_arm)
                for key, me, status, by_arm, values in admitted
                if (value := values[name]) is not None
            ]
            got: FactorMonth = (
                factor_month(holdings, p20, p80, signs[name]) if holdings else FactorMonth(0, 0, None, None)
            )
            accumulators[name].add(holding_month(formation), got)
    return {name: accumulators[name].result(published[name], name) for name in names}


def linked_unquoted(panel: Panel, artefact: Path) -> dict[str, Any]:
    """``not_priced`` candidates alive at s(M) (first frozen bar <= s(M) <= last_bar) that the pinned linkage
    links at s(M). Filer status is not evaluated for them."""
    first = first_daily_bars(artefact)
    last = {line["series_id"]: line["last_bar"] for line in read_gz_lines(artefact / "inputs" / Frozen.ADMITTED)}
    alive = [
        (sid, session)
        for sid, session in panel.unquoted
        if sid in first
        and first[sid] <= session
        and last.get(sid) is not None
        and session <= date.fromisoformat(last[sid])
    ]
    linkage = load_security_linkage(LINKAGE[0], expected_manifest_sha256=LINKAGE[1])
    linked = sum(1 for sid, session in alive if linkage.link_as_of(sid, session).reason is Reason.LINKED)
    return {"not_priced": len(panel.unquoted), "alive_unquoted": len(alive), "alive_unquoted_linked": linked}


def census_form25(out: Path) -> dict[str, Any]:
    """Premise 1's table, from a read-only extract frozen beside the results."""
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        records = [
            Form25Record(cik, symbol, filed, event)
            for cik, symbol, filed, event in conn.execute(_FORM25_SQL, {"before": FORM25_FILED_BEFORE}).fetchall()
        ]
        symbols = sorted({r.symbol for r in records} | {r.symbol.rstrip("Q") for r in records})
        series = conn.execute(_FORM25_SERIES_SQL, {"vendor": INTRADER, "symbols": symbols}).fetchall()
        conn.rollback()
    last_bars: dict[str, list[date]] = defaultdict(list)
    for symbol, bar in series:
        last_bars[symbol].append(bar)
    extract = [
        *(["record", r.issuer_cik, r.symbol, r.filed_date.isoformat(), r.event_date.isoformat()] for r in records),
        *(["series", symbol, bar.isoformat()] for symbol, bars in sorted(last_bars.items()) for bar in sorted(bars)),
    ]
    write_gz_lines(out, extract)
    return {
        "extract": out.name,
        "extract_sha256": sha256_file(out),
        "label": "selected reference set (Form 25 records with a cover-page symbol), not verification of the archive",
        "table": form25_table(records, last_bars),
    }


def mean_me_share(by_formation: Mapping[str, Mapping[str, Any]], key: str) -> float | None:
    """A reason's ME share averaged over every formation: a formation without the reason contributes 0. ``None``
    when the reason's ME is unknown wherever it occurs (count-only reasons)."""
    shares: list[float] = []
    for cell in by_formation.values():
        if key not in cell:
            shares.append(0.0)
        elif cell[key]["me_share"] is not None:
            shares.append(cell[key]["me_share"])
        else:
            return None
    return statistics.fmean(shares) if shares else None


def alignment_matrix(
    census: Mapping[str, Any],
    results: Mapping[str, Any],
    delayed: Mapping[str, int],
    unquoted: Mapping[str, Any],
    form25: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """§"Alignment matrix", row by row, each with its stored figures or "not measurable"."""
    funnel = census["funnel_total_counts"]
    funnel_me = {key: mean_me_share(census["funnel_by_formation"], key) for key in funnel}
    recon = census["me_reconciliation"]
    return [
        {
            "difference": "universe: linked 10-K/10-Q filers, one linked security, quoted on s(M); not CRSP 10/11/12",
            "measured": {"funnel_counts": funnel, "funnel_mean_me_share_by_formation": funnel_me},
        },
        {
            "difference": "survivorship: unverified 2014-09..2018; selected Form 25 set from 2019",
            "measured": form25 if form25 is not None else "run with --census-form25",
        },
        {
            "difference": "retrospective integrity masks",
            "measured": {
                "bundle_integrity_excluded": funnel.get("integrity_excluded", 0),
                "linkage_effect": "not measurable",
            },
        },
        {
            "difference": "commodity pools, other funds and OTC-quoted filers not separable",
            "measured": "not measurable",
        },
        {
            "difference": "REIT exclusion by SIC 6798, not share code",
            "measured": {"reit": funnel.get("reit", 0), "sic_status": census["sic_status"]},
        },
        {
            "difference": "XBRL as filed vs Compustat; dropped branches; ope* and TXDITC proxies; COGS goods-only flag",
            "measured": {"branch_use": census["branch_use"], "period_kind": census["period_kind"]},
        },
        {
            "difference": "acceptance required on top of the 4-month lag; 18-month maximum age",
            "measured": {
                "delayed_lower_bound": dict(delayed),
                "aged_out": {c: census["characteristics_total_counts"][c].get("aged_out", 0) for c in ACCOUNTING},
            },
        },
        {
            "difference": "issuer-level shares; unresolved class scope; 15-month share age; balance-sheet fallback",
            "measured": {
                "shares_scope": census["shares_scope"],
                "discontinuity_flagged": recon["discontinuity"]["flagged"],
                "class_scope": "not measurable",
            },
        },
        {
            "difference": "Amendment 2 share checks; identical mis-scaling of both counts undetected",
            "measured": {
                "outcomes": census["me_checks"]["outcomes"],
                "removed_names": {k: v["names"] for k, v in census["me_checks"]["removed"].items()},
                "undetected_residual": "not measurable",
            },
        },
        {
            "difference": "quarterly items reconstructed (YTD differences, Q4 residuals)",
            "measured": census["branch_use"],
        },
        {
            "difference": "Amendment 2b ni*/ocf* branches and imputed zeros",
            "measured": {"fallback_use": census["fallback_use"], "undetected_residual": "not measurable"},
        },
        {
            "difference": "Amendment 2c ope*/gp* reads, bound and vetoes",
            "measured": {"veto_use": census["veto_use"], "residuals": "not measurable"},
        },
        {
            "difference": "ret_12_1 needs 11 of 11; rvol_21d minimum by analogy; daily screen",
            "measured": {
                **{c: census["characteristics_total_counts"][c] for c in PRICE_CHARACTERISTICS},
                "daily_screen_flags_by_year": census["daily_screen_flags_by_year"],
            },
        },
        {"difference": "tercile tie and quantile convention is ours", "measured": "-"},
        {"difference": "holdings need a quote on s(M)", "measured": dict(unquoted)},
        {
            "difference": "coverage exits and terminations imputed, not CRSP delisting returns",
            "measured": {
                "holding_status_counts": census["diagnostics_total_counts"]["holding_status"],
                "leg_weight_share_both_arms": {
                    name: result["holding_status_weight_both_arms"]
                    for name, result in results.items()
                    if "arms" in result
                },
            },
        },
    ]


def census_summary(census: Mapping[str, Any]) -> dict[str, Any]:
    recon = census["me_reconciliation"]
    return {
        "liquidity_tercile": census["diagnostics_total_counts"]["liquidity_tercile"],
        "daily_monthly": census["diagnostics_total_counts"]["daily_monthly"],
        "daily_monthly_unexplained": len(census["daily_monthly_unexplained"]),
        "split_reconciliation": {
            "pairs_with_a_stamp": recon["split_reconciliation"]["pairs_with_a_stamp"],
            "failures": len(recon["split_reconciliation"]["failures"]),
            "dispositions": dict(Counter(d["status"] for d in census["split_failure_dispositions"])),
        },
        "discontinuity_flagged": recon["discontinuity"]["flagged"],
        "linkage_reasons": {k: v for k, v in census["funnel_total_counts"].items() if k != "admitted"},
    }


# --------------------------------------------------------------------------- run


def run(args: argparse.Namespace, argv: Sequence[str], ledger: Path) -> dict[str, Any]:
    """Gate, evaluate, write the results, and record the run; returns the results only after the terminal row."""
    if is_dirty() is not False:
        raise FidelityError("dirty_checkout", "the fidelity report runs from a clean checkout only")
    run_id = uuid.uuid4().hex
    base = {"run_id": run_id}
    spec_sha256, versions = current_versions()
    digest = versions_digest(versions)
    append_ledger(
        ledger,
        {
            **base,
            "event": "started",
            "at": datetime.now(UTC).isoformat(),
            "git_sha": head_commit(),
            "argv": list(argv),
            "trial_register_sha256": sha256_file(REPO_ROOT / TRIAL_REGISTER_PATH),
            "spec_sha256": spec_sha256,
            "versions": versions,
            "versions_digest": digest,
        },
    )
    began = False
    try:
        manifest = verify_artefact(args.artefact, args.manifest_sha256)
        check_artefact(manifest, spec_sha256, versions)
        history = read_ledger(COMMITTED_LEDGER_PATH, ledger)
        began_rows = [row for row in history if row["event"] == "evaluation_began" and row["run_id"] != run_id]
        trial = register_gate(TRIAL_REGISTER, spec_sha256, digest, began_rows)
        past = version_history(row for row in history if row["event"] == "completed")
        if any(row["versions_digest"] != digest for row in began_rows) and not args.revision_reason:
            raise FidelityError("revision_reason", "a later version needs --revision-reason naming its difference")
        names = [name for name in CHARACTERISTICS if not past[name]["rejected"]]
        append_ledger(
            ledger,
            {
                **base,
                "event": "evaluation_began",
                "trial_id": trial.trial_id,
                "trial_evidence_sha256": evidence_sha256(trial),
                "spec_sha256": spec_sha256,
                "versions": versions,
                "versions_digest": digest,
                "artefact": str(args.artefact),
                "artefact_manifest_sha256": args.manifest_sha256,
                "revision_reason": args.revision_reason,
            },
        )
        began = True
        grid = [holding_month(m) for m in formation_months()]
        signs = load_table9_signs(args.artefact / "inputs" / TABLE9_FROZEN)
        panel = read_panel(args.artefact, manifest)
        results: dict[str, Any] = evaluate(
            panel, read_cutoffs(args.artefact), signs, read_published(args.artefact, grid), names
        )
        results.update({name: {"verdict": Verdict.REJECTED.value} for name in CHARACTERISTICS if name not in names})
        census = json.loads((args.artefact / manifest["census"]["path"]).read_text())
        delayed = {c: delayed_name_months(panel.periods[c]) for c in ACCOUNTING}
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        form25 = census_form25(RESULTS_DIR / f"{run_id}-form25.jsonl.gz") if args.census_form25 else None
        if form25 is not None:
            append_ledger(
                ledger,
                {**base, "event": "form25_extract", "extract": form25["extract"], "sha256": form25["extract_sha256"]},
            )
        document = {
            "schema": RESULTS_SCHEMA,
            "run_id": run_id,
            "trial_id": trial.trial_id,
            "population_labels": list(POPULATION_LABELS),
            "version_history_before_run": past,
            "characteristics": results,
            "alignment_matrix": alignment_matrix(
                census, results, delayed, linked_unquoted(panel, args.artefact), form25
            ),
            "census_summary": census_summary(census),
        }
        out = RESULTS_DIR / f"{run_id}.json"
        write_json_once(out, document)
        append_ledger(
            ledger,
            {
                **base,
                "event": "completed",
                "trial_id": trial.trial_id,
                "versions_digest": digest,
                "results_sha256": sha256_file(out),
                "form25_extract_sha256": None if form25 is None else form25["extract_sha256"],
                "verdicts": {name: results[name]["verdict"] for name in CHARACTERISTICS},
            },
        )
        return document
    except BaseException as exc:
        code = exc.code if isinstance(exc, FidelityError) else "panel" if isinstance(exc, PanelError) else "unexpected"
        append_ledger(
            ledger,
            {**base, "event": "failed", "error_class": type(exc).__name__, "code": code, "evaluation_began": began},
        )
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--artefact", type=Path, help="a published stage-A artefact directory")
    parser.add_argument("--manifest-sha256", help="the artefact's pinned manifest digest")
    parser.add_argument("--census-form25", action="store_true", help="reproduce premise 1's Form 25 table")
    parser.add_argument("--revision-reason", help="for a later version: its named difference from the JKP rule")
    parser.add_argument(
        "--print-evidence", action="store_true", help="print the register evidence this code and spec need; no run"
    )
    args = parser.parse_args(argv)
    if args.print_evidence:
        spec_sha256, versions = current_versions()
        print(fidelity_evidence(spec_sha256, versions_digest(versions)))
        return 0
    if args.artefact is None or args.manifest_sha256 is None:
        parser.error("--artefact and --manifest-sha256 are required for a run")
    document = run(args, list(sys.argv if argv is None else argv), LEDGER_PATH)
    print(json.dumps(document, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
