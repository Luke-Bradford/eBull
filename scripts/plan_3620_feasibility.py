"""#3620 slice 2a: the registered condition-4 feasibility look on W1, the development window.

Spec: ``docs/research/2026-10-06-3620-cross-asset-tsmom.md`` §"Condition-4 feasibility on W1 (slice 2a)". One
configuration (every fund, primary scenario: net cost, 0% cash, lagged timing) against B1 (SPY) in the same scenario,
over W1 = S+1..min(2021-05, E). It prints the two annualised log growths, n, the bounds and the verdict, nothing else.

    PYTHONPATH=. uv run python -m scripts.plan_3620_feasibility

It refuses before computing unless register entry ``FEASIBILITY_TRIAL_ID`` exists, the checkout is clean at
``origin/main`` and its ledger holds no ``completed`` row. An ``evaluation_began`` row is fsynced before any outcome.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services import cost_model
from app.services.etf_total_return_reader import EtfMonthlyReturn, EtfVerdict, load_etf_total_return_panel
from app.services.factor_book_declaration import canonical_json
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.total_return_reader import INTRADER_VENDOR, Month, MonthEnd, add_months, load_month_ends
from app.services.trial_register import TRIAL_REGISTER
from app.services.tsmom_etf import (
    BASELINES,
    COMPARATORS,
    FUNDS,
    STALE_DAYS,
    FundCoverage,
    TsmomRefusal,
    baseline_fills,
    build_census,
    date_of,
    evaluated_formations,
    fill_month,
    form,
    fund_coverage,
    simulate,
    tsmom_weights,
)
from scripts.report_3609_baselines import load_factors

FEASIBILITY_TRIAL_ID: Final = "3620-condition4-feasibility-2026-10-08"
#: W1's last month before clipping by E (spec §Windows: before the house hold-out boundary 2021-06-29).
W1_LAST: Final[Month] = (2021, 5)
MIN_MONTHS: Final = 12
TIMING: Final = "lagged"
REFUSED: Final = "REFUSED"
NOT_DECLARED: Final = "NOT_DECLARED"
CONTINUE: Final = "CONTINUE"

_REPO_ROOT: Final = Path(__file__).resolve().parents[1]
SPEC_PATH: Final = _REPO_ROOT / "docs/research/2026-10-06-3620-cross-asset-tsmom.md"
#: Where the follow-up PR commits a completed attempt's output. The run itself writes only under ``var/`` (gitignored),
#: so a refused or failed attempt leaves the checkout clean for its retry.
COMMITTED_OUTPUT_PATH: Final = _REPO_ROOT / "docs/research/3620-condition4-feasibility.json"
LEDGER_PATH: Final = _REPO_ROOT / "var/research/3620/feasibility-ledger.jsonl"
COMMITTED_LEDGER_PATH: Final = _REPO_ROOT / "docs/research/3620-feasibility-ledger.jsonl"
HASHED_CODE: Final = (
    "scripts/plan_3620_feasibility.py",
    "app/services/tsmom_etf.py",
    "app/services/etf_total_return_reader.py",
    "app/services/total_return_reader.py",
    "app/services/cost_model.py",
    "app/services/trial_register.py",
)

_INTRADER_SERIES_SQL = """
SELECT vendor_symbol, series_id
FROM research_price_series
WHERE vendor = %(vendor)s AND vendor_symbol = ANY(%(symbols)s::text[])
"""


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def code_hashes() -> dict[str, str]:
    return {name: _sha256_file(_REPO_ROOT / name) for name in HASHED_CODE}


def growth(returns: Sequence[float]) -> float:
    """Annualised log growth G = (12/n) Σ ln(1 + r) (#3609 step 2's G2)."""
    return 12.0 / len(returns) * math.fsum(math.log1p(r) for r in returns)


def truncate(coverage: FundCoverage, last: Month) -> FundCoverage:
    """Outcome isolation: the fund's months after ``last`` are dropped before anything is computed."""
    kept = {m: r for m, r in coverage.returns.items() if m <= last}
    anchors = {m: a for m, a in coverage.anchors.items() if m <= last}
    return replace(coverage, returns=kept, anchors=anchors, last_month=min(coverage.last_month, last))


def evaluate(
    *,
    start: Month,
    w1_end: Month,
    coverage: Mapping[str, FundCoverage],
    rf: Mapping[Month, float],
    entry_half_spread: Callable[[str, Month], float],
) -> dict[str, Any]:
    """Spec §"The rule, frozen before the look". ``coverage`` and ``rf`` must already be truncated to ``w1_end``."""
    for symbol, cov in coverage.items():
        if any(m > w1_end for m in cov.returns):
            raise TsmomRefusal("isolation", f"{symbol} carries a month after {w1_end}")
    if any(m > w1_end for m in rf):
        raise TsmomRefusal("isolation", f"RF carries a month after {w1_end}")
    returns = {s: c.returns for s, c in coverage.items()}
    fills = {
        fill_month(t, TIMING): tsmom_weights(form(t, FUNDS, coverage, rf))
        for t in evaluated_formations(start, w1_end, TIMING)
    }
    common = {"start": start, "end": w1_end, "returns": returns, "cash_return": None}
    book = simulate(fills=fills, entry_half_spread=entry_half_spread, cost_multiplier=1.0, **common)
    b1 = simulate(
        fills=baseline_fills(BASELINES["B1"], start, w1_end, TIMING),
        entry_half_spread=entry_half_spread,
        cost_multiplier=1.0,
        **common,
    )
    months = sorted(book.returns)
    result: dict[str, Any] = {
        "months": [list(m) for m in months],
        "book_returns": [book.returns[m] for m in months],
        "b1_returns": [b1.returns.get(m) for m in months],
    }
    if months != sorted(b1.returns):
        return {**result, "verdict": REFUSED, "reason": "reported months differ"}
    if len(months) < MIN_MONTHS:
        return {**result, "verdict": REFUSED, "reason": f"n = {len(months)} < {MIN_MONTHS}"}
    g_book = growth([book.returns[m] for m in months])
    g_b1 = growth([b1.returns[m] for m in months])
    result.update(n=len(months), g_book=g_book, g_b1=g_b1)
    if not (math.isfinite(g_book) and math.isfinite(g_b1)):
        return {**result, "verdict": REFUSED, "reason": "non-finite G"}
    return {**result, "verdict": NOT_DECLARED if g_book <= g_b1 else CONTINUE, "reason": None}


def band_lookup(closes: Mapping[str, Mapping[Month, MonthEnd]]) -> Callable[[str, Month], float]:
    """Spec §"Month-end anchors" and §Costs: the entry band from the fill month's Intrader raw close."""

    def entry_half_spread(symbol: str, month: Month) -> float:
        bar = closes.get(symbol, {}).get(month)
        if bar is None or not math.isfinite(bar.close) or bar.close <= 0:
            raise TsmomRefusal("raw_close", f"{symbol} has no usable raw close at {month}")
        if (date_of(add_months(month, 1)) - bar.bar_date).days - 1 > STALE_DAYS:
            raise TsmomRefusal("raw_close", f"{symbol}'s {month} bar {bar.bar_date} is stale")
        return float(cost_model.cost_band_for(Decimal(repr(bar.close)), price_basis="as_traded").half_spread)

    return entry_half_spread


def inputs_record(
    start: Month,
    w1_end: Month,
    coverage: Mapping[str, FundCoverage],
    rf: Mapping[Month, float],
    closes: Mapping[str, Mapping[Month, MonthEnd]],
) -> dict[str, Any]:
    """The consumed inputs, saved so ``reproduce`` can re-evaluate them (spec §"Registration and attempt record")."""
    return {
        "start": list(start),
        "w1_end": list(w1_end),
        "funds": {
            s: {
                "verdict": c.verdict.value,
                "first_month": list(c.first_month),
                "returns": sorted([list(m), r] for m, r in c.returns.items()),
            }
            for s, c in sorted(coverage.items())
        },
        "rf": sorted([list(m), v] for m, v in rf.items()),
        "closes": {
            s: sorted([list(m), e.bar_date.isoformat(), e.close] for m, e in months.items())
            for s, months in sorted(closes.items())
        },
    }


def evaluate_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """``evaluate`` on a saved ``inputs_record``."""

    def month(value: Sequence[int]) -> Month:
        return (int(value[0]), int(value[1]))

    coverage = {}
    for symbol, fund in record["funds"].items():
        returns = {month(m): float(r) for m, r in fund["returns"]}
        coverage[symbol] = FundCoverage(
            symbol, EtfVerdict(fund["verdict"]), month(fund["first_month"]), max(returns), returns
        )
    closes = {
        s: {month(m): MonthEnd(date.fromisoformat(d), math.nan, float(c)) for m, d, c in rows}
        for s, rows in record["closes"].items()
    }
    try:
        return evaluate(
            start=month(record["start"]),
            w1_end=month(record["w1_end"]),
            coverage=coverage,
            rf={month(m): float(v) for m, v in record["rf"]},
            entry_half_spread=band_lookup(closes),
        )
    except TsmomRefusal as exc:
        return {"months": [], "verdict": REFUSED, "reason": str(exc)}


SUMMARY_KEYS: Final = ("verdict", "reason", "n", "g_book", "g_b1", "months")


def output_path(run_id: str) -> Path:
    return LEDGER_PATH.parent / f"feasibility-{run_id}.json"


def reproduce(path: Path) -> bool:
    """Re-evaluate the saved inputs; True iff the summary and verdict match the file's."""
    output = json.loads(path.read_text(encoding="utf-8"))
    if hashlib.sha256(canonical_json(output["inputs"])).hexdigest() != output["input_sha256"]:
        return False
    again = json.loads(canonical_json(evaluate_record(output["inputs"])))
    return all(again.get(k) == output.get(k) for k in SUMMARY_KEYS)


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=_REPO_ROOT, check=True, capture_output=True, text=True).stdout.strip()


TERMINAL: Final = frozenset({"completed", "refused", "failed", "abandoned"})


def preflight(git: Callable[..., str] = _git) -> str:
    """Refusals before any read (spec §"Registration and attempt record"). Run under ``ledger_lock``."""
    if FEASIBILITY_TRIAL_ID not in TRIAL_REGISTER.trial_ids:
        raise SystemExit(f"refused: register has no {FEASIBILITY_TRIAL_ID} entry")
    git("fetch", "--quiet", "origin", "main")
    if git("status", "--porcelain"):
        raise SystemExit("refused: the checkout is not clean")
    head = git("rev-parse", "HEAD")
    if head != git("rev-parse", "origin/main"):
        raise SystemExit("refused: HEAD is not origin/main")
    rows = read_ledger(LEDGER_PATH, COMMITTED_LEDGER_PATH)
    if any(row.get("event") == "completed" for row in rows):
        raise SystemExit("refused: the look has already completed")
    began = {row["run_id"] for row in rows if row.get("event") == "evaluation_began"}
    ended = {row["run_id"] for row in rows if row.get("event") in TERMINAL}
    if began - ended:
        raise SystemExit(f"refused: attempts {sorted(began - ended)} have no terminal row; reconcile them first")
    return head


@contextmanager
def ledger_lock() -> Iterator[None]:
    """One attempt at a time: an exclusive, non-blocking lock held for the whole run."""
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER_PATH.with_suffix(".lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("refused: another attempt holds the ledger lock") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def write_durably(path: Path, data: bytes) -> None:
    """The output is on disk, its directory entry included, before the terminal row can name it."""
    with path.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _row(event: str, run_id: str, **fields: Any) -> dict[str, Any]:
    return {"event": event, "run_id": run_id, "at": datetime.now(UTC).isoformat(), **fields}


def run(conn: psycopg.Connection[Any], head: str) -> dict[str, Any]:
    symbols = sorted(set(FUNDS) | set(COMPARATORS))
    panel = load_etf_total_return_panel(conn, symbols, include_price_return=False)
    rows: dict[str, list[EtfMonthlyReturn]] = {}
    for row in panel.rows:
        rows.setdefault(row.symbol, []).append(row)
    coverage = {
        s: fund_coverage(s, panel.verdicts.get(s, EtfVerdict.NO_INTRADER_SERIES), rows.get(s, [])) for s in symbols
    }
    census = build_census({s: coverage[s] for s in FUNDS}, {s: coverage[s] for s in COMPARATORS})
    w1_end = min(W1_LAST, census.end)
    factors, snapshots = load_factors(conn, {})
    meta = {
        "trial_id": FEASIBILITY_TRIAL_ID,
        "git_sha": head,
        "spec_sha256": _sha256_file(SPEC_PATH),
        "code_sha256": code_hashes(),
        "cost_model_id": cost_model.COST_MODEL_ID,
        "panel_version": panel.version,
        "factor_snapshots": snapshots,
        "start": list(census.start),
        "census_end": list(census.end),
        "census_limiting": list(census.limiting),
        "w1_end": list(w1_end),
    }
    run_id = hashlib.sha256(canonical_json({**meta, "at": datetime.now(UTC).isoformat()})).hexdigest()[:32]
    append_ledger(LEDGER_PATH, _row("evaluation_began", run_id, **meta))
    try:
        isolated = {s: truncate(coverage[s], w1_end) for s in FUNDS}
        rf = {m: v for m, v in factors["RF"].items() if m <= w1_end}
        series = {
            str(s): int(i)
            for s, i in conn.execute(_INTRADER_SERIES_SQL, {"vendor": INTRADER_VENDOR, "symbols": list(FUNDS)})
        }
        ends = load_month_ends(conn, sorted(series.values()))
        closes = {s: {m: e for m, e in ends.get(series[s], {}).items() if m <= w1_end} for s in series}
        record = inputs_record(census.start, w1_end, isolated, rf, closes)
        output = {
            **meta,
            "run_id": run_id,
            "census": {
                s: {
                    "first_month": list(c.first_month),
                    "last_month": list(c.last_month),
                    "first_eligible": list(c.first_eligible),
                }
                for s, c in sorted(coverage.items())
            },
            "inputs": record,
            "input_sha256": hashlib.sha256(canonical_json(record)).hexdigest(),
            **evaluate_record(record),
        }
        data = canonical_json(output) + b"\n"
        write_durably(output_path(run_id), data)
    except BaseException as exc:
        append_ledger(LEDGER_PATH, _row("failed", run_id, error=repr(exc)))
        raise
    terminal = "refused" if output["verdict"] == REFUSED else "completed"
    # Nothing fallible sits between the fsynced output and this row: its sha256 comes from the bytes in memory. If the
    # append itself fails, nothing can record that; the attempt stays unterminated and `preflight` refuses every later
    # one until it is reconciled by hand (`abandoned`), because the output may already hold a result.
    path = output_path(run_id)
    append_ledger(
        LEDGER_PATH,
        _row(
            terminal,
            run_id,
            verdict=output["verdict"],
            output=str(path.relative_to(_REPO_ROOT)),
            output_sha256=hashlib.sha256(data).hexdigest(),
        ),
    )
    return output


def main(argv: Sequence[str] | None = None) -> int:
    argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0]).parse_args(argv)
    with ledger_lock():
        head = preflight()
        with psycopg.connect(settings.database_url) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            out = run(conn, head)
    first = out["months"][0] if out["months"] else None
    last = out["w1_end"]
    print(
        f"W1 {f'{first[0]}-{first[1]:02d}' if first else '-'}..{last[0]}-{last[1]:02d}, "
        f"n = {out.get('n', len(out['months']))}"
    )
    if "g_book" in out:
        print(f"G TSMOM {out['g_book']!r}; G B1 {out['g_b1']!r}")
    print(f"verdict {out['verdict']}" + (f" ({out['reason']})" if out["reason"] else ""))
    path = output_path(out["run_id"])
    try:
        reproduced: object = reproduce(path)
    except Exception as exc:  # the attempt is already recorded; a reproduce failure is reported, not raised
        reproduced = f"error {exc!r}"
    print(f"output {path}; reproduces: {reproduced}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
