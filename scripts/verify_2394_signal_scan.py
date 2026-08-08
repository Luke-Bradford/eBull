"""Dev-verification for the daily signal scan (#2394 §3.1).

    PYTHONPATH=. uv run python scripts/verify_2394_signal_scan.py --all

⚠⚠ THIS ONE **WRITES**, unlike ``verify_2394_signal_scan_cost.py``, which is
read-only. It runs the real ``run_signal_scan`` against the dev corpus, checks
the spec's acceptance criteria against what actually landed, and then **removes
exactly what it wrote** — the signals by ``signal_id`` above the pre-run maximum,
and any watermark row that did not exist before. Every row this scan writes is
terminal (``store_signals`` has no ``ON CONFLICT``), so a verification run must
not leave a track record behind that a later code change cannot correct.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress a multi-minute run is
judged by (`.claude/CLAUDE.md`).

Gate on the EXIT CODE — 0 means every arm passed.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

import psycopg

from app.config import settings
from app.services.price_masked_bars import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_signal_scan import ScanReport, run_signal_scan

_LAST_BAR_OF_WRITTEN = """
    SELECT count(*)
    FROM strategy_signals s
    WHERE s.signal_id > %(floor_id)s
      AND s.signal_bar_date >= (
          SELECT max(d.price_date)
          FROM price_daily d
          JOIN price_quarantine_coverage cov
            ON cov.instrument_id = d.instrument_id
           AND cov.rule_set_version = %(v)s
           AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
          WHERE d.instrument_id = s.instrument_id
      )
"""


def _print_report(report: ScanReport) -> None:
    frontier = report.frontier
    print(f"  status {report.status}")
    if frontier is not None:
        print(
            f"  frontier {frontier.bar_date} held by {frontier.modal_count}/{frontier.loadable} "
            f"({frontier.share_pct:.1f}%), floor met: {frontier.meets_floor}"
        )
    print(
        f"  universe {report.universe_size}; eligible {report.eligible_instruments}; excluded "
        f"no_bars={report.excluded_no_bars} stale={report.excluded_stale_series} "
        f"short={report.excluded_short_series} moved={report.excluded_moved_mid_scan}"
    )
    for result in report.per_strategy:
        print(
            f"  {result.strategy_id} {result.status} rows={result.rows_written} "
            f"expected_per_leg={result.expected_per_leg} resumed_from={result.resumed_from}"
            + (f" ERROR {result.error}" if result.error else "")
        )
        for (kind, verdict, reason), count in sorted(result.census.items()):
            print(f"    {kind} {verdict}{f' ({reason})' if reason else ''}: {count}")


def _scalar(conn: psycopg.Connection[Any], sql: str, params: dict[str, Any] | None = None) -> int:
    row = conn.execute(sql, params).fetchone()  # type: ignore[arg-type]
    assert row is not None
    return int(row[0] or 0)


#: Definition-of-Done clause 8's default panel.
PANEL: tuple[str, ...] = ("AAPL", "GME", "MSFT", "JPM", "HD")


def _panel(conn: psycopg.Connection[Any], floor_id: int) -> bool:
    """Clause 8 — the written rows for five known instruments, printed in full.

    ⚠ Every panel member must carry one row per declared leg per strategy. A
    missing one is a member the scan silently did not cover, which is the
    population defect the census gate exists for, checked here on names a reader
    can sanity-check by eye.
    """
    print(f"  clause 8 — the default panel {list(PANEL)}:")
    rows = conn.execute(
        """
        SELECT i.symbol, s.strategy_id, s.signal_kind, s.signal_bar_date, s.verdict,
               coalesce(s.not_evaluable_reason, ''), s.fill_bar_date, s.fill_price
        FROM strategy_signals s
        JOIN instruments i ON i.instrument_id = s.instrument_id
        WHERE s.signal_id > %(f)s AND i.symbol = ANY(%(panel)s)
        ORDER BY i.symbol, s.strategy_id, s.signal_kind
        """,
        {"f": floor_id, "panel": list(PANEL)},
    ).fetchall()
    for symbol, strategy_id, kind, bar, verdict, reason, fill_bar, fill_price in rows:
        suffix = f" ({reason})" if reason else ""
        fill = f" fill {fill_bar} @ {fill_price}" if fill_bar else ""
        print(f"    {symbol:6} {strategy_id:36} {kind:5} {bar} {verdict}{suffix}{fill}")
    covered = {str(row[0]) for row in rows}
    missing = sorted(set(PANEL) - covered)
    if missing:
        print(f"  ⚠⚠ panel members with NO row at all: {missing}")
    return not missing


def _cross_source_fill(conn: psycopg.Connection[Any], floor_id: int) -> bool:
    """Clause 9 — a stored fill price against the raw bar it claims to come from.

    ⚠ The independent source here is ``price_daily`` itself, read WITHOUT the
    masked loader. That is the point: the whole fill path runs through
    ``load_masked_bars`` → ``resolve_fills``, so checking the stored price
    against the raw column is the one comparison the pipeline cannot make true
    by agreeing with itself.
    """
    mismatched = _scalar(
        conn,
        """
        SELECT count(*)
        FROM strategy_signals s
        JOIN price_daily d
          ON d.instrument_id = s.instrument_id AND d.price_date = s.fill_bar_date
        WHERE s.signal_id > %(f)s AND s.verdict = 'fired' AND s.fill_price IS DISTINCT FROM d.open
        """,
        {"f": floor_id},
    )
    checked = _scalar(
        conn,
        """
        SELECT count(*)
        FROM strategy_signals s
        JOIN price_daily d
          ON d.instrument_id = s.instrument_id AND d.price_date = s.fill_bar_date
        WHERE s.signal_id > %(f)s AND s.verdict = 'fired'
        """,
        {"f": floor_id},
    )
    print(f"  clause 9 — stored fill_price vs raw price_daily.open: {mismatched} mismatched of {checked} checked")
    return mismatched == 0 and checked > 0


def scan() -> bool:
    """Run the scan for real, check the acceptance criteria, then undo it."""
    print("SCAN — one real run against the dev corpus, then cleaned up")
    ok = True
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        floor_id = _scalar(conn, "SELECT coalesce(max(signal_id), 0) FROM strategy_signals")
        # ⚠ The VALUE, not just the key (Codex, checkpoint 2). Snapshotting only
        # the keys leaves a pre-existing watermark ADVANCED after the cleanup has
        # deleted the signals it advanced past — so a later real scan treats a
        # window with no rows in it as already covered, which is the permanent
        # hole this whole job is shaped to avoid.
        before_watermarks = {
            (str(sid), str(ver)): frontier
            for sid, ver, frontier in conn.execute(
                "SELECT strategy_id, strategy_version, frontier_date FROM strategy_scan_watermark"
            ).fetchall()
        }
        print(f"  pre-run: max(signal_id)={floor_id}, {len(before_watermarks)} watermark row(s)")

        started = time.monotonic()
        report = run_signal_scan(conn)
        print(f"  first run took {time.monotonic() - started:.1f}s")
        _print_report(report)

        try:
            written = _scalar(conn, "SELECT count(*) FROM strategy_signals WHERE signal_id > %(f)s", {"f": floor_id})
            if written != report.rows_written:
                print(f"  ⚠⚠ report claims {report.rows_written} rows, table gained {written}")
                ok = False

            if report.status == "scanned" and written:
                # Acceptance 1 — no signal_bar_date equals its instrument's last bar.
                on_last_bar = _scalar(
                    conn, _LAST_BAR_OF_WRITTEN, {"floor_id": floor_id, "v": QUARANTINE_RULE_SET_VERSION}
                )
                print(f"  acceptance 1 — rows written on an instrument's LAST bar: {on_last_bar} (must be 0)")
                ok = ok and on_last_bar == 0

                labels = conn.execute(
                    "SELECT DISTINCT universe FROM strategy_signals WHERE signal_id > %(f)s",
                    {"f": floor_id},
                ).fetchall()
                print(f"  acceptance 1 — universe labels on written rows: {sorted(str(r[0]) for r in labels)}")
                ok = ok and [str(r[0]) for r in labels] == ["survivor_only"]

                fills = _scalar(
                    conn,
                    """
                    SELECT count(*) FROM strategy_signals
                    WHERE signal_id > %(f)s AND verdict = 'fired'
                      AND (fill_bar_date IS NULL OR fill_price IS NULL OR fill_price <= 0
                           OR fill_bar_date <= signal_bar_date)
                    """,
                    {"f": floor_id},
                )
                print(f"  fired rows with an absent/backwards/non-positive fill: {fills} (must be 0)")
                ok = ok and fills == 0

                ok = _panel(conn, floor_id) and ok
                ok = _cross_source_fill(conn, floor_id) and ok

                # Acceptance 2 — a re-run on the same frontier is a watermark no-op.
                rerun = run_signal_scan(conn)
                after = _scalar(conn, "SELECT count(*) FROM strategy_signals WHERE signal_id > %(f)s", {"f": floor_id})
                statuses = sorted({result.status for result in rerun.per_strategy})
                print(f"  acceptance 2 — re-run status {rerun.status}, per-strategy {statuses}, rows now {after}")
                ok = ok and rerun.status == "up_to_date" and after == written
        finally:
            removed = _scalar(
                conn,
                "WITH d AS (DELETE FROM strategy_signals WHERE signal_id > %(f)s RETURNING 1) SELECT count(*) FROM d",
                {"f": floor_id},
            )
            keys = conn.execute("SELECT strategy_id, strategy_version FROM strategy_scan_watermark").fetchall()
            restored = 0
            for strategy_id, version in keys:
                was = before_watermarks.get((str(strategy_id), str(version)))
                if was is None:
                    conn.execute(
                        "DELETE FROM strategy_scan_watermark WHERE strategy_id = %(s)s AND strategy_version = %(v)s",
                        {"s": strategy_id, "v": version},
                    )
                    continue
                conn.execute(
                    "UPDATE strategy_scan_watermark SET frontier_date = %(d)s "
                    "WHERE strategy_id = %(s)s AND strategy_version = %(v)s AND frontier_date <> %(d)s",
                    {"d": was, "s": strategy_id, "v": version},
                )
                restored += 1
            after = {
                (str(sid), str(ver)): frontier
                for sid, ver, frontier in conn.execute(
                    "SELECT strategy_id, strategy_version, frontier_date FROM strategy_scan_watermark"
                ).fetchall()
            }
            print(
                f"  cleanup: removed {removed} signal row(s); {len(after)} watermark row(s) remain "
                f"({restored} restored to their pre-run frontier)"
            )
            ok = ok and after == before_watermarks
    return ok


ARMS = {"scan": scan}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ARMS:
        parser.add_argument(f"--{name}", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    selected = [name for name in ARMS if getattr(args, name)] or (list(ARMS) if args.all else [])
    if not selected:
        parser.print_help()
        return 2

    ok = True
    for name in selected:
        print()
        ok = ARMS[name]() and ok
    print()
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
