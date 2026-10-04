"""#3619 slice 3 — full-population A/B of the total-return splice: PWB 2026-07-08 vs 2026-09-09.

Arm A is the reader with its three capture constants pinned back to the 2026-07-08 load; arm B is
the reader as committed (the 2026-09-09 capture). Both captures are separate vendors in the DB, so
the A/B re-runs at any time from one command. Everything else (selection, quarantine rule set,
Intrader, the identity gate) is shared, so every difference is the capture.

    PYTHONPATH=. uv run python -m scripts.ab_3619_pwb_capture [--baseline var/ab_3619_s3/panel_A.pkl]

``--baseline`` checks arm A row-for-row against a panel dumped by the unmodified reader at
``24c37c0b``, so the pin is shown to reproduce the old code. Format: JSON
``{"rows": [[name_key, "YYYY-MM-DD", total_return, vendor, series_id], ...], "verdicts": {name_key: verdict}}``.

Read-only against the DB. A data-quality census: compares two captures of one vendor's price
paths and ranks, selects or scores nothing.
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services import research_corpus_ingest as ingest
from app.services import total_return_reader as tr
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import load_universe_selection

_A_CAPTURE = date(2026, 7, 8)


@contextmanager
def _pinned_to_a() -> Iterator[None]:
    # The reader reads these module globals at call time; ``setattr`` because they are ``Final``.
    pinned = {
        "PWB_VENDOR": ingest.HF_ARCHIVE.vendor,
        "PWB_CAPTURE_DATE": _A_CAPTURE,
        "LAST_PWB_MONTH": tr.add_months(tr.month_of(_A_CAPTURE), -1),
    }
    saved = {name: getattr(tr, name) for name in pinned}
    for name, value in pinned.items():
        setattr(tr, name, value)
    try:
        yield
    finally:
        for name, value in saved.items():
            setattr(tr, name, value)


def _key(row: tr.MonthlyTotalReturn) -> tuple[int, date]:
    return (row.name_key, row.month)


def _quantiles(values: list[float]) -> str:
    if not values:
        return "n=0"
    ordered = sorted(values)
    pick = [ordered[min(len(ordered) - 1, int(q * len(ordered)))] for q in (0.5, 0.9, 0.99)]
    return f"n={len(values)} p50={pick[0]:.6f} p90={pick[1]:.6f} p99={pick[2]:.6f} max={ordered[-1]:.6f}"


def compare_baseline(arm_a: tr.TotalReturnPanel, path: Path) -> bool:
    baseline: dict[str, Any] = json.loads(path.read_text())
    expected = {(int(k), date.fromisoformat(m)): (v, vendor, int(s)) for k, m, v, vendor, s in baseline["rows"]}
    observed = {_key(r): (r.total_return, r.vendor, r.series_id) for r in arm_a.rows}
    same_rows = expected == observed
    same_verdicts = {int(k): v for k, v in baseline["verdicts"].items()} == {
        k: v.value for k, v in arm_a.verdicts.items()
    }
    print(f"\n## Arm A vs unmodified-code baseline ({path})")
    print(f"rows identical: {same_rows} ({len(expected)} vs {len(observed)})")
    print(f"verdicts identical: {same_verdicts}")
    return same_rows and same_verdicts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        validated = load_validated_universe(conn)
        selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=frozenset(validated))
        with _pinned_to_a():
            arm_a = tr.load_total_return_panel(conn, selection)
        arm_b = tr.load_total_return_panel(conn, selection)
        rows_a = {_key(r): r for r in arm_a.rows}
        rows_b = {_key(r): r for r in arm_b.rows}
        disputed = sorted(
            k
            for k in rows_a.keys() & rows_b.keys()
            if rows_a[k].vendor != tr.INTRADER_VENDOR
            and rows_b[k].vendor != tr.INTRADER_VENDOR
            and abs(rows_a[k].total_return - rows_b[k].total_return) > 1e-6
        )
        intrader_series = {s.name_key: s.series_id for s in selection.admitted}
        arbiter_ids = sorted({intrader_series[k[0]] for k in disputed})
        month_ends = tr.load_month_ends(conn, arbiter_ids)
    arbiter = {
        series_id: tr.monthly_returns(ends, field="adj_close", through=tr.LAST_SURVIVORSHIP_FREE_MONTH)
        for series_id, ends in month_ends.items()
    }

    ok = compare_baseline(arm_a, args.baseline) if args.baseline else True

    print(f"\nadmitted names: {len(selection.admitted)}")
    print(f"arm A: {ingest.HF_ARCHIVE.vendor}, LAST_PWB_MONTH {arm_a_last(arm_a)}")
    print(f"arm B: {tr.PWB_VENDOR}, LAST_PWB_MONTH {tr.LAST_PWB_MONTH}")

    print("\n## Verdicts (A -> B)")
    census_a, census_b = arm_a.census(), arm_b.census()
    for verdict in tr.SpliceVerdict:
        print(f"{verdict.value}: {census_a[verdict]} -> {census_b[verdict]}")
    transitions = Counter(
        (arm_a.verdicts[key].value, arm_b.verdicts[key].value)
        for key in arm_a.verdicts
        if arm_a.verdicts[key] is not arm_b.verdicts[key]
    )
    print("transitions:", dict(transitions) or "none")

    only_a, only_b = rows_a.keys() - rows_b.keys(), rows_b.keys() - rows_a.keys()
    print("\n## Rows")
    print(f"A {len(rows_a)}  B {len(rows_b)}  only A {len(only_a)}  only B {len(only_b)}")
    print("only B by month:", dict(sorted(Counter(k[1].isoformat()[:7] for k in only_b).items())))
    print("only A by month:", dict(sorted(Counter(k[1].isoformat()[:7] for k in only_a).items())))

    shared = rows_a.keys() & rows_b.keys()
    vendor_moves = Counter((rows_a[k].vendor, rows_b[k].vendor) for k in shared if rows_a[k].vendor != rows_b[k].vendor)
    print("vendor changes on shared rows:", dict(vendor_moves) or "none")

    # Same-source rows: a PWB-sourced month in both arms is the same month measured by two captures.
    pwb_both = [k for k in shared if rows_a[k].vendor != tr.INTRADER_VENDOR and rows_b[k].vendor != tr.INTRADER_VENDOR]
    diffs = [abs(rows_a[k].total_return - rows_b[k].total_return) for k in pwb_both]
    print("\n## PWB months in both arms: |return A - return B|")
    print(_quantiles(diffs))
    for edge in (1e-6, 1e-4, 1e-3, 1e-2):
        print(f"  > {edge:g}: {sum(d > edge for d in diffs)}")
    bar_moves = sum(
        (rows_a[k].start_bar, rows_a[k].end_bar) != (rows_b[k].start_bar, rows_b[k].end_bar) for k in pwb_both
    )
    print(f"  interval bars differ: {bar_moves}")
    worst = sorted(pwb_both, key=lambda k: -abs(rows_a[k].total_return - rows_b[k].total_return))[:15]
    for k in worst:
        a, b = rows_a[k], rows_b[k]
        print(
            f"  name {k[0]} {k[1]:%Y-%m}  A {a.total_return:+.6f}  B {b.total_return:+.6f}"
            f"  bars {b.start_bar}..{b.end_bar}"
        )

    # Up to 2024-08 Intrader (the survivorship-free vendor, a separate Yahoo redistribution) measured the
    # same month, so it arbitrates which capture moved: the arm whose return sits nearer Intrader's.
    print(f"\n## Disputed PWB months (|A - B| > 1e-6): {len(disputed)}; Intrader arbitration through 2024-08")
    tally: Counter[str] = Counter()
    for k in disputed:
        ret = arbiter.get(intrader_series[k[0]], {}).get(tr.month_of(k[1]))
        if ret is None:
            tally["no Intrader month" if k[1] <= date(2024, 8, 1) else "after 2024-08 (no arbiter)"] += 1
            continue
        gap_a, gap_b = abs(rows_a[k].total_return - ret.value), abs(rows_b[k].total_return - ret.value)
        tally["A nearer" if gap_a < gap_b else "B nearer" if gap_b < gap_a else "tie"] += 1
    print(dict(tally))
    late = Counter(k[1].isoformat()[:7] for k in disputed if k[1] > date(2024, 8, 1))
    print("disputed after 2024-08 by month:", dict(sorted(late.items())))

    per_month = Counter(k[1].isoformat()[:7] for k in pwb_both)
    print("\nPWB months in both arms, latest 6:", dict(sorted(per_month.items())[-6:]))
    new_months = sorted(k for k in only_b if k[1] > date(2026, 6, 1))
    if new_months:
        values = [rows_b[k].total_return for k in new_months]
        print(f"new months 2026-07..: {len(new_months)} rows, median {statistics.median(values):+.6f}")
    return 0 if ok else 1


def arm_a_last(panel: tr.TotalReturnPanel) -> str:
    months = [r.month for r in panel.rows if r.vendor != tr.INTRADER_VENDOR]
    return max(months).isoformat()[:7] if months else "none"


if __name__ == "__main__":
    raise SystemExit(main())
