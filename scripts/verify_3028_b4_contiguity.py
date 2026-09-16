"""#3028 full-population A/B — B4's gap tolerance, and the per-class threshold floor.

THREE ARMS, per ``.claude/skills/engineering/full-population-ab.md``:

* ``stored``    — what ``price_bar_quarantine`` actually holds right now.
* ``control``   — the rules re-parsed with B4's HISTORIC gap tolerance (the removed
                  ``contiguous_days``: 4 on a 5-day venue, 2 on a 7-day one).
* ``treatment`` — the rules exactly as they ship, B4's tolerance being ``hole_days``.

⚠ The ``stored`` reconciliation is the arm that earns its keep. It is not a
restatement of the treatment: it proves the harness reproduces PRODUCTION before the
treatment is believed at all, which is the one thing a simulated control can never
do. WHICH arm it reconciles against is read off the corpus, not assumed — the
coverage table is asked for its rule-set version, and before the re-evaluation that
is the control while after it is the treatment. Assuming the current
``RULE_SET_VERSION`` instead matches zero rows the moment the rules are edited, and
then reports "stored holds nothing", which reads exactly like agreement.

WHY THAT TREATMENT. B4 used to decline to evaluate when either neighbour sat more
than ``contiguous_days`` away (4 for a 5-day-week venue), while T2 quarantines a
transition only when the gap exceeds ``hole_days`` (10). Between the two lay a band —
5 to 10 calendar days — in which NEITHER rule evaluated the bar, so a reverting
one-bar spike next to a two-holiday weekend kept ``range_usable = true`` and reached
every ``price_masked_bars`` consumer as a real price level. One tolerance instead of
two removes the unowned band rather than inventing a constant.

``--thresholds`` reproduces S7 §5's own calibration statistic (p99.99 of |ln r| on
the POST-quarantine population, where "post-quarantine" means transitions the rule
set did not quarantine) so the per-class magnitude floor can be checked rather than
asserted.

``--research`` runs the same three arms over the research corpus, which is the
backtest substrate and the half that matters: a flagged bar there masks a field, and
``docs/review-prevention-log.md`` measured 643 flagged bars becoming 174,978
``quarantined_bar`` verdicts in S-3 (2,692x) because Wilder smoothing poisons the
recursion from a NULL close onward. Series are pre-filtered in SQL to those holding
at least one calendar gap inside the unowned band — a purely calendar superset of the
gain set, since a gain REQUIRES such a gap — so the rules themselves are still the
one implementation in ``price_quarantine`` and are never re-expressed here.

Run from this worktree:

    PYTHONPATH=. uv run python -m scripts.verify_3028_b4_contiguity --thresholds
    PYTHONPATH=. uv run python -m scripts.verify_3028_b4_contiguity --ab
    PYTHONPATH=. uv run python -m scripts.verify_3028_b4_contiguity --research
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from typing import LiteralString

import psycopg

from app.config import settings
from app.services.price_quarantine import (
    RULE_SET_VERSION,
    Bar,
    ClassParams,
    evaluate_bars,
    params_for,
)
from app.services.price_quarantine_store import _series_batches

# S7 §5's calibration statistic, measured the way S7 measured it: the p99.99 of
# |ln r| over the transitions the rule set did NOT quarantine. Excluding the
# quarantined transitions is the whole point of "post-quarantine population" —
# leaving them in measures the defects the threshold exists to catch.
THRESHOLD_SQL = """
WITH pairs AS (
  SELECT pd.instrument_id, pd.price_date, pd.close,
         lag(pd.price_date) OVER w AS prev_date,
         lag(pd.close)      OVER w AS prev_close
  FROM price_daily pd
  WINDOW w AS (PARTITION BY pd.instrument_id ORDER BY pd.price_date)
),
scoped AS (
  SELECT coalesce(c.asset_class, 'NULL') AS asset_class,
         p.close, p.prev_close, tq.rules AS trules
  FROM pairs p
  JOIN price_quarantine_coverage c
    ON c.instrument_id = p.instrument_id
   AND c.rule_set_version = %(v)s
   AND p.price_date BETWEEN c.first_bar AND c.last_bar
   AND p.prev_date  >= c.first_bar
  LEFT JOIN price_transition_quarantine tq
    ON tq.instrument_id = p.instrument_id
   AND tq.price_date = p.price_date
   AND tq.rule_set_version = %(v)s
  WHERE p.prev_close IS NOT NULL AND p.prev_close > 0 AND p.close > 0
)
SELECT asset_class,
       count(*),
       exp(percentile_cont(0.999)  WITHIN GROUP (ORDER BY abs(ln(close/prev_close)))),
       exp(percentile_cont(0.9999) WITHIN GROUP (ORDER BY abs(ln(close/prev_close))))
FROM scoped
WHERE trules IS NULL OR cardinality(trules) = 0
GROUP BY asset_class
ORDER BY count(*) DESC
"""

STORED_SQL = """
SELECT instrument_id, price_date
FROM price_bar_quarantine
WHERE rule_set_version = %(v)s AND 'B4' = ANY(rules)
"""

#: The version the stored rows were actually produced at, asked of the corpus rather
#: than assumed. ⚠ Pinning this to ``RULE_SET_VERSION`` is the trap Codex caught at
#: checkpoint 2: that constant hashes the CURRENT source, so the moment the rules are
#: edited it stops matching the rows the reconciliation exists to compare against —
#: and the arm then reports "stored holds nothing", which reads like agreement.
#: Written out per table rather than interpolated: psycopg types ``execute`` against
#: ``LiteralString`` precisely so a table name cannot arrive through ``.format``, and
#: two short literals are cheaper than earning an exemption from that.
PRICE_VERSION_SQL = "SELECT DISTINCT rule_set_version FROM price_quarantine_coverage ORDER BY 1"
RESEARCH_VERSION_SQL = "SELECT DISTINCT rule_set_version FROM research_price_quarantine_coverage ORDER BY 1"


def stored_version(conn: psycopg.Connection, table: str, sql: LiteralString) -> str:  # type: ignore[type-arg]
    """The single rule-set version present in a coverage table, or refuse.

    A corpus mid-re-evaluation holds two, and averaging over both would compare
    each arm against a mixture of itself and the other. That is not a state this
    script can report on, so it stops instead of reporting on it.
    """
    found = [r[0] for r in conn.execute(sql).fetchall()]
    if len(found) != 1:
        raise SystemExit(
            f"{table} holds {len(found)} rule-set versions {found}; re-run once the corpus is evaluated at exactly one"
        )
    return found[0]


def arm_of(version: str) -> str:
    """Which arm the stored rows represent: they are evidence either way.

    Before the re-evaluation the corpus IS the control, and the diff proves the
    harness reproduces production. After it, the corpus IS the treatment, and the
    same diff proves the backfill actually landed what this script predicted. Both
    are worth having; silently mislabelling one as the other is not.
    """
    return "treatment" if version == RULE_SET_VERSION else "control"


#: B4's gap tolerance as it stood before #3028, keyed on the class's hole tolerance
#: because that is the pairing the module shipped: hole 10 <-> contiguous 4 (exchange
#: venues), hole 4 <-> contiguous 2 (the 7-day crypto/FX markets). Frozen here so the
#: control arm survives the field's removal.
_HISTORIC_B4_GAP = {10: 4, 4: 2}


def control_params(params: ClassParams) -> ClassParams:
    """The pre-#3028 B4 behaviour, expressed through the surviving field.

    Overriding ``hole_days`` is sound for THIS arm and only this one: B4 is the
    only rule ``evaluate_bars`` consults that reads a calendar tolerance, and T2 —
    the other reader — lives in ``evaluate_transitions``, which this script never
    calls. Asserting on the lookup rather than defaulting keeps a future class
    with a new hole tolerance from silently comparing against itself.
    """
    gap = _HISTORIC_B4_GAP.get(params.hole_days)
    if gap is None:
        raise SystemExit(
            f"no historic B4 gap recorded for hole_days={params.hole_days}; "
            "the control arm would silently equal the treatment"
        )
    return replace(params, hole_days=gap)


@dataclass
class ClassTally:
    bars: int = 0
    instruments: int = 0
    control_b4: int = 0
    treatment_b4: int = 0
    gained: int = 0
    gained_instruments: int = 0


def b4_dates(bars: list[Bar], params: ClassParams, as_of: date) -> set[date]:
    return {v.price_date for v in evaluate_bars(bars, params, as_of=as_of) if "B4" in v.rules}


def run_ab(conn: psycopg.Connection, as_of: date, sample_limit: int) -> int:  # type: ignore[type-arg]
    baseline = stored_version(conn, "price_quarantine_coverage", PRICE_VERSION_SQL)
    baseline_arm = arm_of(baseline)
    stored: dict[int, set[date]] = defaultdict(set)
    for iid, pdate in conn.execute(STORED_SQL, {"v": baseline}).fetchall():
        stored[int(iid)].add(pdate)

    tallies: dict[str, ClassTally] = defaultdict(ClassTally)
    stored_only = 0
    control_only = 0
    samples: list[tuple[str, int, date, object, object, object, int, int]] = []

    for iid, asset_class, bars in _series_batches(conn, None):
        params = params_for(asset_class)
        control = b4_dates(bars, control_params(params), as_of)
        treatment = b4_dates(bars, params, as_of)
        key = asset_class or "NULL"
        t = tallies[key]
        t.bars += len(bars)
        t.instruments += 1
        t.control_b4 += len(control)
        t.treatment_b4 += len(treatment)

        # parse-vs-STORED arm, reconciled against whichever arm the corpus holds.
        stored_set = stored.get(iid, set())
        reparsed = control if baseline_arm == "control" else treatment
        stored_only += len(stored_set - reparsed)
        control_only += len(reparsed - stored_set)

        gain = treatment - control
        if gain:
            t.gained += len(gain)
            t.gained_instruments += 1
            if len(samples) < sample_limit:
                by_date = {b.price_date: b for b in bars}
                order = [b.price_date for b in bars]
                for d in sorted(gain):
                    if len(samples) >= sample_limit:
                        break
                    i = order.index(d)
                    prev, cur, nxt = bars[i - 1], bars[i], bars[i + 1]
                    assert by_date[d] is cur
                    samples.append(
                        (
                            key,
                            iid,
                            d,
                            prev.close,
                            cur.close,
                            nxt.close,
                            (cur.price_date - prev.price_date).days,
                            (nxt.price_date - cur.price_date).days,
                        )
                    )

    print(f"\n[A/B] rule set {RULE_SET_VERSION}   as_of {as_of}", flush=True)
    print(
        f"\n[parse-vs-STORED] corpus is at {baseline}"
        f"\n  -> those rows are the {baseline_arm.upper()} arm; reconciling against it"
        f"\n  stored B4 not reproduced: {stored_only}"
        f"\n  reparsed B4 not stored:   {control_only}",
        flush=True,
    )

    print(
        f"\n{'class':<12}{'instruments':>12}{'bars':>12}"
        f"{'B4 control':>12}{'B4 treat':>10}{'gained':>8}{'gain insts':>11}",
        flush=True,
    )
    total = ClassTally()
    for key in sorted(tallies, key=lambda k: -tallies[k].bars):
        t = tallies[key]
        total.bars += t.bars
        total.instruments += t.instruments
        total.control_b4 += t.control_b4
        total.treatment_b4 += t.treatment_b4
        total.gained += t.gained
        total.gained_instruments += t.gained_instruments
        print(
            f"{key:<12}{t.instruments:>12,}{t.bars:>12,}"
            f"{t.control_b4:>12,}{t.treatment_b4:>10,}{t.gained:>8,}{t.gained_instruments:>11,}",
            flush=True,
        )
    print(
        f"{'TOTAL':<12}{total.instruments:>12,}{total.bars:>12,}"
        f"{total.control_b4:>12,}{total.treatment_b4:>10,}{total.gained:>8,}"
        f"{total.gained_instruments:>11,}",
        flush=True,
    )

    print(f"\n[gain side] up to {sample_limit} newly-quarantined bars, for inspection", flush=True)
    print(
        f"{'class':<12}{'iid':>7} {'date':<12}{'prev':>14}{'close':>14}{'next':>14}"
        f"{'gap_in':>8}{'gap_out':>9}{'next/prev':>11}",
        flush=True,
    )
    for key, iid, d, prev_c, cur_c, next_c, gin, gout in samples:
        ratio = float(next_c) / float(prev_c) if prev_c else float("nan")  # type: ignore[arg-type]
        print(
            f"{key:<12}{iid:>7} {str(d):<12}{float(prev_c):>14.6f}"  # type: ignore[arg-type]
            f"{float(cur_c):>14.6f}{float(next_c):>14.6f}{gin:>8}{gout:>9}{ratio:>11.4f}",  # type: ignore[arg-type]
            flush=True,
        )
    return 0


# A gain requires a neighbour gap in the formerly-unowned band — that band IS the
# treatment. Selecting series that hold such a gap is therefore a superset of the
# gain set, decided on the calendar alone, with no rule re-expressed in SQL.
RESEARCH_SCOPE_SQL = """
WITH g AS (
  SELECT series_id, bar_date,
         bar_date - lag(bar_date) OVER (PARTITION BY series_id ORDER BY bar_date) AS gap
  FROM research_price_daily
)
SELECT DISTINCT series_id FROM g
WHERE gap > %(lo)s AND gap <= %(hi)s
ORDER BY series_id
"""

RESEARCH_SERIES_SQL = """
SELECT bar_date, open, high, low, close, volume
FROM research_price_daily
WHERE series_id = %(sid)s
ORDER BY bar_date
"""

RESEARCH_STORED_SQL = """
SELECT series_id, bar_date
FROM research_bar_quarantine
WHERE rule_set_version = %(v)s AND 'B4' = ANY(rules)
"""


def run_research(conn: psycopg.Connection, as_of: date, sample_limit: int) -> int:  # type: ignore[type-arg]
    from app.services.research_corpus_ingest import ASSET_CLASS

    params = params_for(ASSET_CLASS)
    ctrl = control_params(params)
    baseline = stored_version(conn, "research_price_quarantine_coverage", RESEARCH_VERSION_SQL)
    baseline_arm = arm_of(baseline)

    stored: dict[int, set[date]] = defaultdict(set)
    for sid, bdate in conn.execute(RESEARCH_STORED_SQL, {"v": baseline}).fetchall():
        stored[int(sid)].add(bdate)

    conn.execute("SET statement_timeout = '1800s'")
    scope = [
        int(r[0])
        for r in conn.execute(
            RESEARCH_SCOPE_SQL,
            {"lo": ctrl.hole_days, "hi": params.hole_days},
        ).fetchall()
    ]
    print(
        f"\n[research] class {ASSET_CLASS}   T={params.magnitude_threshold}"
        f"   control B4 gap {ctrl.hole_days}d -> treatment {params.hole_days}d"
        f"\n[research] {len(scope):,} series hold a gap in the formerly-unowned band"
        f" ({ctrl.hole_days + 1}..{params.hole_days}d); a gain requires one",
        flush=True,
    )

    bars_seen = 0
    control_b4 = 0
    treatment_b4 = 0
    gained = 0
    gain_series = 0
    stored_only = 0
    control_only = 0
    samples: list[tuple[int, date, object, object, object, int, int]] = []

    for n, sid in enumerate(scope, start=1):
        rows = conn.execute(RESEARCH_SERIES_SQL, {"sid": sid}).fetchall()
        if not rows:
            continue
        bars = [
            Bar(
                price_date=r[0],
                open=r[1],
                high=r[2],
                low=r[3],
                close=r[4],
                volume=Decimal(r[5]) if r[5] is not None else None,
            )
            for r in rows
        ]
        bars_seen += len(bars)
        control = b4_dates(bars, ctrl, as_of)
        treatment = b4_dates(bars, params, as_of)
        control_b4 += len(control)
        treatment_b4 += len(treatment)

        stored_set = stored.get(sid, set())
        reparsed = control if baseline_arm == "control" else treatment
        stored_only += len(stored_set - reparsed)
        control_only += len(reparsed - stored_set)

        gain = treatment - control
        if gain:
            gained += len(gain)
            gain_series += 1
            order = [b.price_date for b in bars]
            for d in sorted(gain):
                if len(samples) >= sample_limit:
                    break
                i = order.index(d)
                prev, cur, nxt = bars[i - 1], bars[i], bars[i + 1]
                samples.append(
                    (
                        sid,
                        d,
                        prev.close,
                        cur.close,
                        nxt.close,
                        (cur.price_date - prev.price_date).days,
                        (nxt.price_date - cur.price_date).days,
                    )
                )
        if n % 1000 == 0:
            print(
                f"[research] {n:,}/{len(scope):,} series  {bars_seen:,} bars  gained {gained}",
                flush=True,
            )

    print(
        f"\n[research parse-vs-STORED] (scoped series only) corpus is at {baseline}"
        f"\n  -> those rows are the {baseline_arm.upper()} arm; reconciling against it"
        f"\n  stored B4 not reproduced: {stored_only}"
        f"\n  reparsed B4 not stored:   {control_only}",
        flush=True,
    )
    print(
        f"\n[research] series {len(scope):,}  bars {bars_seen:,}"
        f"\n  B4 control   {control_b4:,}"
        f"\n  B4 treatment {treatment_b4:,}"
        f"\n  gained       {gained:,} bars on {gain_series:,} series",
        flush=True,
    )
    print(f"\n[research gain side] up to {sample_limit} newly-quarantined bars", flush=True)
    print(
        f"{'series':>8} {'date':<12}{'prev':>14}{'close':>14}{'next':>14}{'gap_in':>8}{'gap_out':>9}{'next/prev':>11}",
        flush=True,
    )
    for sid, d, prev_c, cur_c, next_c, gin, gout in samples:
        ratio = float(next_c) / float(prev_c) if prev_c else float("nan")  # type: ignore[arg-type]
        print(
            f"{sid:>8} {str(d):<12}{float(prev_c):>14.6f}"  # type: ignore[arg-type]
            f"{float(cur_c):>14.6f}{float(next_c):>14.6f}{gin:>8}{gout:>9}{ratio:>11.4f}",  # type: ignore[arg-type]
            flush=True,
        )
    return 0


def run_thresholds(conn: psycopg.Connection) -> int:  # type: ignore[type-arg]
    conn.execute("SET statement_timeout = '1200s'")
    rows = conn.execute(THRESHOLD_SQL, {"v": RULE_SET_VERSION}).fetchall()
    print(
        f"\n[S7 §5] p99.99 |ln r| as a ratio, POST-quarantine population\n  rule set {RULE_SET_VERSION}\n",
        flush=True,
    )
    print(
        f"{'class':<12}{'n returns':>14}{'p99.9':>9}{'p99.99':>9}{'T today':>9}{'floor ok':>10}",
        flush=True,
    )
    for asset_class, n, p999, p9999 in rows:
        params = params_for(None if asset_class == "NULL" else asset_class)
        t = float(params.magnitude_threshold)
        ok = "yes" if t > float(p9999) else "NO"
        print(
            f"{asset_class:<12}{n:>14,}{float(p999):>9.3f}{float(p9999):>9.3f}{t:>9.1f}{ok:>10}",
            flush=True,
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ab", action="store_true", help="run the three-arm B4 A/B on price_daily")
    ap.add_argument("--research", action="store_true", help="same three arms on the research corpus")
    ap.add_argument("--thresholds", action="store_true", help="reproduce S7 §5's statistic")
    ap.add_argument("--as-of", type=date.fromisoformat, default=None)
    ap.add_argument("--sample-limit", type=int, default=40)
    args = ap.parse_args(argv)
    if not (args.ab or args.thresholds or args.research):
        ap.error("pass --ab, --research and/or --thresholds")

    as_of = args.as_of or date.today()
    rc = 0
    with psycopg.connect(settings.database_url) as conn:
        if args.thresholds:
            rc |= run_thresholds(conn)
        if args.ab:
            rc |= run_ab(conn, as_of, args.sample_limit)
        if args.research:
            rc |= run_research(conn, as_of, args.sample_limit)
    return rc


if __name__ == "__main__":
    sys.exit(main())
