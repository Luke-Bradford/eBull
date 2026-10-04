"""#3619 slice 2: census of the total-return splice over the survivorship-free selection.

Reproduces every figure the splice spec quotes (``docs/proposals/etl/2026-10-04-3619-total-return-splice.md``):
the verdict per admitted name, the identity statistic's distribution, the refused tail by symbol, and the
panel's rows by vendor and flag. A data-quality census: it compares two vendors' price paths and ranks,
selects or scores nothing.

    PYTHONPATH=. uv run python -m scripts.report_3619_splice_census

Read-only against the DB.
"""

from __future__ import annotations

from collections import Counter

import psycopg

from app.config import settings
from app.services.strategies.validated_universe import load_validated_universe
from app.services.total_return_reader import load_total_return_panel
from app.services.universe_selection import load_universe_selection

_BUCKETS = (
    (0.0005, "<5bp"),
    (0.001, "5-10bp"),
    (0.0025, "10-25bp"),
    (0.005, "25-50bp"),
    (0.01, "50-100bp"),
    (0.02, "1-2pp"),
    (0.05, "2-5pp"),
    (float("inf"), ">=5pp"),
)


def bucket(value: float) -> str:
    return next(label for edge, label in _BUCKETS if value < edge)


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        validated = load_validated_universe(conn)
        selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=frozenset(validated))
        panel = load_total_return_panel(conn, selection)
        refused = sorted(
            ((key, check) for key, check in panel.identity.items() if check.median_abs_diff is not None),
            key=lambda item: -(item[1].median_abs_diff or 0.0),
        )
        tail_keys = [key for key, check in refused if not check.passed]
        symbols = dict(
            conn.execute(
                "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%s)", (tail_keys,)
            ).fetchall()
        )

    print(f"splice version: {panel.version}")
    print(f"admitted names: {len(selection.admitted)}")
    print("\n## Verdicts")
    for verdict, count in sorted(panel.census().items()):
        print(f"{verdict.value}: {count}")

    print("\n## Identity statistic (median |close-return difference|), names with enough overlap")
    buckets = Counter(bucket(check.median_abs_diff) for _, check in refused if check.median_abs_diff is not None)
    for _, label in _BUCKETS:
        print(f"{label}: {buckets.get(label, 0)}")

    print("\n## Sensitivity: names that would pass at other thresholds (enough overlap only)")
    for threshold in (0.00025, 0.0005, 0.001, 0.0025):
        passing = sum(1 for _, check in refused if (check.median_abs_diff or 0.0) < threshold)
        print(f"< {threshold * 1e4:g}bp: {passing}")

    print("\n## Refused on price disagreement, largest first")
    print("instrument_id | symbol | paired months | median diff")
    for key, check in refused:
        if not check.passed:
            print(f"{key} | {symbols.get(key, '?')} | {check.paired_months} | {check.median_abs_diff:.5f}")

    print("\n## Panel rows")
    rows = Counter((r.vendor, r.dividend_capture_degraded, r.survivor_only) for r in panel.rows)
    print("vendor | dividend_capture_degraded | survivor_only | rows")
    for (vendor, degraded, survivor_only), count in sorted(rows.items()):
        print(f"{vendor} | {degraded} | {survivor_only} | {count}")
    print("\n## Rows flagged dividend_capture_degraded, by the name's verdict (survival-dependent quality)")
    degraded = Counter(panel.verdicts[r.name_key] for r in panel.rows if r.dividend_capture_degraded)
    post_switch = Counter(panel.verdicts[r.name_key] for r in panel.rows if r.month.year >= 2022)
    print("verdict | degraded rows | rows from 2022")
    for verdict in sorted(post_switch):
        print(f"{verdict.value} | {degraded.get(verdict, 0)} | {post_switch[verdict]}")
    months = sorted({r.month for r in panel.rows})
    print(f"months: {months[0]} .. {months[-1]} ({len(months)})")


if __name__ == "__main__":
    main()
