"""Verify phase 5e-1 — criterion 5's namespace, on the full population (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_holdout_namespace.py --all

Two arms, and they answer different questions:

``--mechanism``  the relations themselves, and the three refusals they carry,
                 against the DEV database. ⚠ It DOES write — the refusals cannot
                 be observed without attempting the writes they refuse — and
                 every attempt sits inside a transaction that is rolled back, so
                 the arm re-counts all three relations afterwards and FAILS if
                 anything survived. Seconds.

                 ⚠ Exercising them here is not redundant with
                 ``tests/test_strategy_holdout_namespace.py``: those run against
                 the test template, and a migration that half-applied to the DEV
                 database would leave the tests green and this arm red. The
                 sequence behind ``strategy_holdout_accesses.access_id`` DOES
                 advance across the rollback, which is what sequences do and is
                 harmless — no row survives.

``--census``     ⚠⚠ THE FULL-POPULATION ARM, and the number spec §5.2 requires
                 and nothing has yet measured: *"The split is over corpus bars,
                 not over each strategy's own signals. A strategy whose signals
                 cluster outside the modern era gets a hold-out that is 25% of
                 BARS and some other fraction of its TRADES. The realised
                 in-sample/hold-out trade counts are therefore reported per
                 strategy."* Minutes — it rebuilds every position over the whole
                 corpus.

⚠ THE CENSUS IS NOT A PERFORMANCE MEASUREMENT and reports no return. It counts
where trades LAND, because criterion 5's hold-out gate is *"the hold-out arm's
effective sample size must be large enough for its own confidence interval to
exclude the random cohort's 95th percentile"* — and a strategy whose hold-out
arm holds a handful of trades fails that before any statistic is computed.

⚠ REUSED, NOT RE-DERIVED. The corpus→positions path comes from stage 5a's
verifier and the namespace rules from ``strategy_result``; a second copy of
either would be a second place for the boundary to drift. What is new here is
only the classification.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/probe_2240_result_ledger.py`` — the
probe mutates tracked source on disk, and a sweep that imports it mid-mutation
reports figures produced by an injected defect (phase 4b's lesson).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter

import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.position_builder import Window, build_positions
from app.services.research_price_structure_store import load_masked_series
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import (
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    namespace_for_position,
    namespace_for_signal,
)
from scripts.verify_2240_position_builder import (
    _S1_REGIME,
    _S3_REGIME,
    UNIVERSE,
    _fills,
    _stamped_versions,
    _to_series,
)

_OCCUPANCY_SQL = """
    SELECT
        (SELECT count(*) FROM strategy_results_store)      AS stored,
        (SELECT count(*) FROM strategy_results)            AS visible,
        (SELECT count(*) FROM strategy_holdout_accesses)   AS accesses
"""

#: ⚠ A strategy id no real strategy uses, so the rolled-back probe rows can be
#: counted apart from anything a future writer stores.
_PROBE_STRATEGY = "S-VERIFY-5E1"

_PROBE_ACCESS = """
    INSERT INTO strategy_holdout_accesses
        (strategy_id, strategy_version, result_version, access_kind, accessed_by, purpose)
    VALUES (%(sid)s, 'verify-5e1', 'verify-5e1-result', 'evaluate', 'verify_2240_holdout_namespace.py',
            'mechanism arm — rolled back')
"""

#: ⚠ FIXED statements, never built from a column list. Both write the SAME row;
#: only the relation differs, which is the whole point of the pair.
_PROBE_TAIL = """
        'verify-5e1', 'verify-5e1-result', 'sleeve', 'hold_out', 'worst_case', 'masked', '1962-01-02', '2026-07-08',
        'capital_candidate', 'survivor_only', 'verify', 'verify', true, 'verify', 'p', 'o', 'i', 1,
        0.5, 1.18, 3.9, 14.2, 0.27, 0.39, -31.4, 62.1, 3.05, 100, -2.4, 50, 20, 4, 0, 251.67, 418.0, 420.4,
        'criterion7-v1'
"""

_PROBE_COLUMNS = """
        strategy_id, strategy_version, result_version, result_scope, namespace,
        ambiguity_arm, quarantine_arm, window_start, window_end, purpose, universe_basis, corpus_version,
        cost_model_id, carry_unmodelled, fx_unmodelled, sizing_rule, position_rule_set_version,
        outcome_rule_set_version, input_rule_set_version, evaluated_instrument_count,
        expectancy_per_trade_pct, profit_factor, cagr_pct, annualised_volatility_pct, sharpe, sortino,
        max_drawdown_pct, exposure_time_pct, turnover_annualised, trade_count,
        return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count,
        unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id
"""

_PROBE_INSERT_STORE = f"""
    INSERT INTO strategy_results_store ({_PROBE_COLUMNS}) VALUES (%(sid)s, {_PROBE_TAIL})
"""  # noqa: S608 - both fragments are module-level literals, no caller input reaches them

_PROBE_INSERT_VIEW = f"""
    INSERT INTO strategy_results ({_PROBE_COLUMNS}) VALUES (%(sid)s, {_PROBE_TAIL})
"""  # noqa: S608 - as above

_PROBE_VISIBILITY = """
    SELECT
        (SELECT count(*) FROM strategy_results_store WHERE strategy_id = %(sid)s) AS stored,
        (SELECT count(*) FROM strategy_results       WHERE strategy_id = %(sid)s) AS visible
"""

_PROBE_STALE = """
    SELECT
        (SELECT count(*) FROM strategy_results_store    WHERE strategy_id = %(sid)s) AS results,
        (SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = %(sid)s) AS accesses
"""

#: The trigger's own SQLSTATE — ``integrity_constraint_violation``, chosen in
#: ``sql/264`` and asserted rather than inferred from the exception class.
_TRIGGER_SQLSTATE = "23000"


class _Rollback(Exception):
    """Unwinds the probe transaction. ⚠ Never a real error — see ``_refusals``."""


def _refusals(conn: psycopg.Connection[tuple]) -> list[str]:
    """The three refusals, attempted for real and then rolled back.

    ⚠⚠ THE ORDER OF THE FIRST TWO IS FORCED AND IS EASY TO GET WRONG. The
    store's BEFORE INSERT trigger fires before the view's ``WITH CHECK OPTION``
    is evaluated, so a hold-out INSERT through the view with NO access record is
    refused by the TRIGGER (23000) and says nothing about the check option. The
    access record has to be written first to reach it (44000). A single
    "it was refused" assertion would pass while testing the wrong mechanism.
    """
    problems: list[str] = []
    params = {"sid": _PROBE_STRATEGY}

    # ⚠ NOTHING OF OURS MAY BE HERE ALREADY. Every probe below writes the same
    # key, so a leftover row would collide on `strategy_results_unique` — and a
    # collision is ALSO an integrity error, which would read as "the trigger
    # refused" while the trigger was doing nothing. Checked rather than assumed,
    # even though the rollback makes it unreachable: an unreachable state that
    # would be MISREPORTED if reached is worth one query.
    stale = conn.execute(_PROBE_STALE, params).fetchone()
    if stale is not None and stale != (0, 0):
        return [f"{_PROBE_STRATEGY} rows already exist (results/accesses = {stale}) — refusing to probe over them"]

    try:
        with conn.transaction():
            # 1 — the trigger: a hold-out row with no evaluate record.
            try:
                with conn.transaction():
                    conn.execute(_PROBE_INSERT_STORE, params)
                problems.append("an unrecorded hold-out row was STORED — the trigger is not enforcing criterion 5")
            except psycopg.errors.IntegrityError as caught:
                # ⚠⚠ THE SQLSTATE IS THE ASSERTION, not the exception class.
                # `IntegrityError` also covers 23505 (unique), 23502 (not null)
                # and 23503 (foreign key), so catching it bare would report any
                # of those as the refusal under test. The trigger raises
                # `integrity_constraint_violation` = 23000 specifically.
                if caught.sqlstate != _TRIGGER_SQLSTATE:
                    problems.append(
                        f"the unrecorded hold-out row was refused with {caught.sqlstate}, not the trigger's "
                        f"{_TRIGGER_SQLSTATE} — something else rejected it and the trigger was never reached: {caught}"
                    )
                else:
                    print(f"  trigger refusal          {caught.sqlstate} (unrecorded hold-out evaluation)", flush=True)

            conn.execute(_PROBE_ACCESS, params)

            # 2 — the check option: same row, through the view, now that the
            # trigger is satisfied.
            try:
                with conn.transaction():
                    conn.execute(_PROBE_INSERT_VIEW, params)
                problems.append("a hold-out row was inserted THROUGH THE VIEW — the check option is not enforcing")
            except psycopg.errors.WithCheckOptionViolation as caught:
                # ⚠ This one's exception class IS specific to the mechanism —
                # only a view's check option raises it — so no sqlstate check is
                # needed and adding one would assert nothing extra.
                print(
                    f"  check-option refusal     {caught.sqlstate} (hold-out row through the in-sample view)",
                    flush=True,
                )

            # 3 — the filter: the recorded row stores, and stays invisible.
            conn.execute(_PROBE_INSERT_STORE, params)
            seen = conn.execute(_PROBE_VISIBILITY, params).fetchone()
            print(f"  recorded hold-out row    stored/visible = {seen}", flush=True)
            if seen != (1, 0):
                problems.append(f"a recorded hold-out row reads as {seen}, expected (1, 0) — the view filter is gone")
            raise _Rollback
    except _Rollback:
        pass
    return problems


def mechanism() -> int:
    """The relations, asserted against the real database. Nothing is left behind."""
    problems: list[str] = []
    print("\n[mechanism]", flush=True)
    with psycopg.connect(settings.database_url) as conn:
        kinds = dict(
            conn.execute(
                "SELECT relname, relkind FROM pg_class WHERE relname IN "
                "('strategy_results', 'strategy_results_store', 'strategy_holdout_accesses')"
            ).fetchall()
        )
        print(f"  relkinds                 {kinds}", flush=True)
        if kinds.get("strategy_results") != "v":
            problems.append("strategy_results is not a VIEW — criterion 5's filter is gone")
        if kinds.get("strategy_results_store") != "r":
            problems.append("strategy_results_store is not a TABLE")
        if kinds.get("strategy_holdout_accesses") != "r":
            problems.append("strategy_holdout_accesses is missing")

        check_option = conn.execute(
            "SELECT check_option FROM information_schema.views WHERE table_name = 'strategy_results'"
        ).fetchone()
        print(f"  view check_option        {check_option[0] if check_option else None}", flush=True)
        if check_option is None or check_option[0] != "CASCADED":
            problems.append("the view has no cascaded check option — an in-sample write could smuggle a hold-out row")

        store_cols = [c for (c,) in _columns(conn, "strategy_results_store")]
        view_cols = [c for (c,) in _columns(conn, "strategy_results")]
        print(f"  column parity            store {len(store_cols)}   view {len(view_cols)}", flush=True)
        if store_cols != view_cols:
            missing = [c for c in store_cols if c not in view_cols]
            problems.append(f"the view is missing {missing} — sql/264's SELECT * was expanded before they existed")

        triggers = [
            t
            for (t,) in conn.execute(
                "SELECT tgname FROM pg_trigger WHERE tgrelid = 'strategy_results_store'::regclass AND NOT tgisinternal"
            ).fetchall()
        ]
        print(f"  store triggers           {triggers}", flush=True)
        if "trg_strategy_results_holdout_access" not in triggers:
            problems.append("the hold-out access trigger is absent — an unrecorded evaluation would store")

        # ⚠ The RLS measurement sql/264's header rests on, re-run rather than
        # quoted. If a non-superuser role ever exists this flips, and that is a
        # decision to revisit, not a regression.
        privileged = conn.execute(
            "SELECT current_user, rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        print(f"  connection role          {privileged}", flush=True)
        if privileged is not None and not (privileged[1] or privileged[2]):
            problems.append(
                "this connection is no longer superuser/bypassrls — sql/264's RLS rejection can and should be revisited"
            )

        occupancy = conn.execute(_OCCUPANCY_SQL).fetchone()
        print(f"  occupancy                stored/visible/accesses = {occupancy}", flush=True)

        problems.extend(_refusals(conn))

        # ⚠ NOW this comparison can fail: `_refusals` really wrote three rows
        # and really rolled them back, so an unequal count means the rollback
        # did not happen. Run before any write it would be a check that cannot
        # fire — the dead-branch shape phase 5d's probes caught.
        after = conn.execute(_OCCUPANCY_SQL).fetchone()
        print(f"  occupancy after probes   stored/visible/accesses = {after}", flush=True)
        if after != occupancy:
            problems.append(f"the probe transaction did not roll back: {occupancy} → {after}")

    print(f"\n  problems: {len(problems)}", flush=True)
    for problem in problems:
        print(f"    {problem}", flush=True)
    return 1 if problems else 0


def _columns(conn: psycopg.Connection[tuple], relation: str) -> list[tuple[str]]:
    return conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %(r)s ORDER BY ordinal_position",
        {"r": relation},
    ).fetchall()


def census(*, limit: int | None) -> int:
    """⚠⚠ Where every trade LANDS, per strategy, over the whole corpus."""
    started = time.monotonic()
    s1_version, s3_version, builder_version = _stamped_versions()
    window = Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)
    print(f"\n[census] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"         {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"         builder {builder_version}", flush=True)
    print(f"         window {window.start} … {window.end}   boundary {HOLDOUT_BOUNDARY}", flush=True)

    positions: dict[str, Counter[str]] = {"S-1": Counter(), "S-3": Counter()}
    signals_seen: dict[str, Counter[str]] = {"S-1": Counter(), "S-3": Counter()}

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        if limit is not None:
            pairs = pairs[:limit]
            print(f"  ⚠ LIMITED to the first {len(pairs)} series — NOT a full-population figure", flush=True)
        print(f"  universe {len(universe):,} instruments   {len(pairs):,} series", flush=True)

        empty = 0
        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            masked = load_masked_series(conn, series_id)
            if not masked.bars:
                empty += 1
                continue
            series, _masked_opens = _to_series(masked.bars)

            for label, identity, signals, regime, strategy_id, version in (
                (
                    "S-1",
                    s1_identity,
                    s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S1_REGIME,
                    S1_STRATEGY_ID,
                    s1_version,
                ),
                (
                    "S-3",
                    s3_identity,
                    s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S3_REGIME,
                    S3_STRATEGY_ID,
                    s3_version,
                ),
            ):
                rows = resolve_fills(
                    signals,
                    series=series,
                    identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
                    instrument_id=int(instrument_id),
                )
                # §5.2's purge, at SIGNAL level: decided in-sample, filled on the
                # withheld side. ⚠ Counted separately from the position census
                # because a purged signal opens NOTHING — it is a narrowing this
                # phase introduces (acceptance C8) and an uncounted narrowing is
                # a narrowing asserted safe.
                for row in rows:
                    # ⚠ Only a `fired` row carries a fill (`sql/255`'s
                    # fill-matches-verdict CHECK), so everything else has no
                    # namespace to be in — bucketed, never silently dropped.
                    if row.verdict != "fired" or row.fill_bar_date is None:
                        signals_seen[label]["not_fired"] += 1
                        continue
                    signals_seen[label][namespace_for_signal(row.signal_bar_date, row.fill_bar_date)] += 1

                entries, exits = _fills(rows, int(instrument_id))
                built = build_positions(
                    strategy_id=strategy_id,
                    strategy_version=version,
                    entries=entries,
                    exits=exits,
                    outcomes=[],
                    outcome_pin=None,
                    series={int(instrument_id): series},
                    regime=regime,
                    window=window,
                )
                for position in built.positions:
                    positions[label][namespace_for_position(position.entry_fill_bar_date, position.close_bar_date)] += 1
                    if position.close_bar_date is None:
                        positions[label]["open_at_window_end"] += 1

            if n % 500 == 0:
                print(f"  {n}/{len(pairs)} series ({time.monotonic() - started:.0f}s)", flush=True)

    print(f"\n  series with usable bars  {len(pairs) - empty}   (fail-closed empties: {empty})", flush=True)
    problems: list[str] = []
    for label in ("S-1", "S-3"):
        sig = signals_seen[label]
        pos = positions[label]
        total_pos = pos["in_sample"] + pos["hold_out"]
        print(f"\n  {label}", flush=True)
        print(
            f"    signals   in_sample {sig['in_sample']:,}   hold_out {sig['hold_out']:,}   "
            f"purged {sig['purged']:,}   not_fired {sig['not_fired']:,}",
            flush=True,
        )
        print(
            f"    positions in_sample {pos['in_sample']:,}   hold_out {pos['hold_out']:,}   "
            f"(open at window end {pos['open_at_window_end']:,}, all hold_out by §5.2)",
            flush=True,
        )
        if total_pos:
            print(f"    hold-out share of trades  {100.0 * pos['hold_out'] / total_pos:.3f}%", flush=True)
        # ⚠ NOT a threshold anybody picked. An EMPTY hold-out arm cannot produce
        # a confidence interval at all, so criterion 5 is unsatisfiable for that
        # strategy before any statistic is computed — which is a finding, not a
        # failure of this script.
        if total_pos and pos["hold_out"] == 0:
            problems.append(f"{label}: {total_pos:,} positions and NOT ONE lands in the hold-out — criterion 5 cannot")

    print(f"\n  problems: {len(problems)}", flush=True)
    for problem in problems:
        print(f"    {problem}", flush=True)
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    # ⚠ Re-checked AFTER the sweep as well as before — a probe harness that
    # mutated and restored a source file mid-run would pass an entry check alone.
    _stamped_versions()
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mechanism", action="store_true", help="the relations, against the dev database")
    parser.add_argument("--census", action="store_true", help="full-population per-strategy namespace split")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.mechanism or args.census or args.all):
        parser.error("pick --mechanism, --census or --all")

    rc = 0
    if args.mechanism or args.all:
        rc |= mechanism()
    if args.census or args.all:
        rc |= census(limit=args.limit)
    print("\nPASS" if rc == 0 else "\n*** FAIL ***", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
