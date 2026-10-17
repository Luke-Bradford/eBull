# ruff: noqa: E501
"""Momentum book out of sample with the dead names #3843 can price (#3822, #2437 queue item 2).

`momentum_oos.py` replays the book on 2024-10..2026-08 over PWB series linked to an instrument tradable TODAY, so a name
that delisted inside the window is there only if its PWB series is still linked to a tradable instrument, and then with
no termination treatment. #3843's manifest (regenerated here by `post_2024_series.main`) marks an event COVERED when a
PWB@2026-09-09 series or eToro `price_daily` ends with the listing. This adds every covered event:
- PWB-covered: the PWB series, total return from `adj_close`, as the survivor universe.
- eToro-covered: the instrument's `price_daily`, close ratios (price return only; no dividends) and close x volume.
- One series per instrument: a survivor series for an instrument whose delisting another source covers is replaced by
  the covering series (a PWB `after_capture` series cannot show the termination; post_2024_series.py).

Termination, per `app/services/series_termination.py` (fires at the series' LAST BAR, never at a Form 25 date): the
terminal month's return realises `terminal_value_fraction` of the last close, 1.0 for `(a)(3)` and 0.45 for `(b)` and
`(a)(4)` (Shumway & Warther 1999, the adverse anchor). It is rebuilt from levels, outside the outlier filter, so a
collapse to zero is charged. Every covered event has one parsed provision (asserted), so no two-armed class arises.

Formation, a research CONVENTION (no source rule states it): a name is not a formation candidate in the month its series
ends, since by the month-end formation it has stopped trading. The `terminal month eligible` arm is its sensitivity.
A held name with no return for the holding month earns 0%, the backtest's cash-out.

Universe arms, because the manifest does not say whether eToro listed a dead PWB name:
- `holdable`: the name's instrument passes today's candidate predicate without `is_tradable` (US equity, stock type,
  canonical): a current-metadata proxy for historical eToro availability. A PWB series reaches its instrument through
  `research_price_series.instrument_id`.
- `all covered`: every covered event.

Uncovered events stay absent (the script prints their count by provision), so every arm is still survivor-only for
them, and nothing here bounds their effect. SPY, the backtest reference and the stop rule are `momentum_oos.py`'s.

Run: PYTHONPATH=. uv run python scripts/research_3822/momentum_oos_dead.py
"""

import contextlib
import io
import json
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from statistics import median

import psycopg

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402
from app.services.series_termination import (  # noqa: E402
    TerminationEvidence,
    classify_termination,
    terminal_value_fraction,
)
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS, resolve_stocks_type_id  # noqa: E402
from scripts.research_3822.momentum_oos import (  # noqa: E402
    BACKTEST,
    OOS,
    SEEDS,
    N,
    ann,
    cum,
    load_intrader,
    load_pwb,
    max_dd,
    months_apart,
    prior_month,
    radj_score,
    run,
    spy_rets,
    worst_lag,
    yahoo_spy,
)
from scripts.research_3843 import post_2024_series  # noqa: E402

ETORO = post_2024_series.ETORO

PWB_SERIES_SQL = """
with d as (
  select series_id, bar_date, close::float c, adj_close::float a, close::float * coalesce(volume,0)::float dvol,
         date_trunc('month', bar_date)::date m
  from research_price_daily where series_id = any(%s) and bar_date >= '2012-06-01'
)
select series_id, m, (array_agg(c order by bar_date desc))[1], (array_agg(a order by bar_date desc))[1], avg(dvol)
from d group by 1, 2
"""

ETORO_SQL = """
with d as (
  select instrument_id, price_date, close::float c, close::float * coalesce(volume,0)::float dvol,
         date_trunc('month', price_date)::date m
  from price_daily where instrument_id = any(%s) and price_date >= '2012-06-01'
)
select instrument_id, m, (array_agg(c order by price_date desc))[1], avg(dvol)
from d group by 1, 2
"""

HOLDABLE_SQL = """
select i.instrument_id
from instruments i join exchanges e on e.exchange_id = i.exchange
where e.asset_class = %(asset_class)s and i.instrument_type_id = %(type_id)s and i.canonical_instrument_id is null
  and i.instrument_id = any(%(ids)s)
"""


def monthly_returns(levels: dict) -> dict:
    """Month -> return against the calendar-prior month's level, with momentum_oos.load_pwb's gap and outlier rules."""
    out = {}
    ks = sorted(levels)
    for a, b in zip(ks, ks[1:]):
        if months_apart(a, b) == 1 and levels[a] and levels[a] > 0:
            r = levels[b] / levels[a] - 1
            if -1 < r < 5:
                out[b] = r
    return out


def manifest() -> list[dict]:
    """Every #3843 event, regenerated just before the replay (a separate transaction: the manifest is not pinned to the
    replay's snapshot, so a concurrent ingest between the two can shift a terminal date)."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "m3843.json"
        with contextlib.redirect_stdout(io.StringIO()):
            post_2024_series.main(["--json", str(path)])
        return json.loads(path.read_text())


def main() -> None:
    events = manifest()
    covered = [e for e in events if e["status"] == "covered"]
    bad = [e["symbol"] for e in covered if e["provision_variants"] != 1 or e["provision"] is None]
    assert not bad, f"covered events without one parsed provision (two-armed class): {bad}"
    assert all(e["ref"] > 0 for e in covered), "a source id is not positive; the negated eToro key would collide"
    with psycopg.connect(settings.database_url) as c:
        c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        ib, iret, ispy, _ = load_intrader(c)
        pb, pret = load_pwb(c)
        pwb_ids = [e["ref"] for e in covered if e["source"] != ETORO]
        et_ids = [e["ref"] for e in covered if e["source"] == ETORO]
        series_instr = dict(
            c.execute(
                "select series_id, instrument_id from research_price_series where series_id = any(%s)",
                (sorted(set(pb) | set(pwb_ids)),),
            ).fetchall()
        )
        dead_pwb_rows = c.execute(PWB_SERIES_SQL, (pwb_ids,)).fetchall()
        et_rows = c.execute(ETORO_SQL, (et_ids,)).fetchall()
        linked = sorted({series_instr[s] for s in pwb_ids if series_instr.get(s)} | set(et_ids))
        holdable_instr = {
            r[0]
            for r in c.execute(
                HOLDABLE_SQL,
                {"asset_class": US_EQUITY_ASSET_CLASS, "type_id": resolve_stocks_type_id(c), "ids": linked},
            ).fetchall()
        }
    assert all(k > 0 for k in pb), "a survivor series id is not positive"

    # Dead series, keyed like the survivor universe: a PWB series id, or the NEGATED eToro instrument id (both ids are
    # asserted positive, so the spaces cannot collide and run()'s tie-break sort stays int-typed).
    dby: dict = defaultdict(dict)
    dlev: dict = defaultdict(dict)
    for sid, m, lc, a, adv in dead_pwb_rows:
        dby[sid][m] = (lc, adv or 0.0)
        dlev[sid][m] = a
    for iid, m, lc, adv in et_rows:
        dby[-iid][m] = (lc, adv or 0.0)
        dlev[-iid][m] = lc
    dret = {k: monthly_returns(v) for k, v in dlev.items()}

    info = {}  # dead key -> (symbol, provision, last_bar, fraction, holdable)
    covered_instr = {}  # instrument -> dead key: one series per name, the one whose source covers the event
    for e in covered:
        key = e["ref"] if e["source"] != ETORO else -e["ref"]
        instr = series_instr.get(key) if key > 0 else -key
        assert key not in info, f"two covered events share one series: {e['symbol']}, {info[key][0]}"
        cls = classify_termination(TerminationEvidence(linked=True, provision=e["provision"], q_suffix=False))
        info[key] = (
            e["symbol"],
            e["provision"],
            date.fromisoformat(e["last_bar"]),
            terminal_value_fraction(cls, None),
            instr in holdable_instr,
        )
        if instr is not None:
            assert instr not in covered_instr, f"two covered events map to instrument {instr}"
            covered_instr[instr] = key

    # A survivor series for an instrument whose delisting another source covers is replaced by the covering series:
    # e.g. PWB `after_capture` cannot show the termination (post_2024_series.py), the covering eToro series does.
    replaced = {s: covered_instr[series_instr[s]] for s in pb if series_instr.get(s) in covered_instr}
    replaced = {s: k for s, k in replaced.items() if k != s}
    already = sorted(k for k in info if k in pb)

    unpriced: set = set()

    def universe(arm: str, exclude_terminal: bool = True):
        drop = {s for s, k in replaced.items() if arm == "all covered" or info[k][4]}
        by = {k: dict(v) for k, v in pb.items() if k not in drop}
        ret = {k: dict(v) for k, v in pret.items() if k not in drop}
        for key, (sym, _prov, last, frac, holdable) in info.items():
            if arm == "holdable" and not holdable:
                continue
            if key not in by:
                by[key], ret[key] = dict(dby.get(key, {})), dict(dret.get(key, {}))
            # The terminal month's return is rebuilt from levels, outside monthly_returns' outlier filter, so a
            # genuine collapse to zero is charged, not dropped to a 0% cash-out.
            t, lev = last.replace(day=1), dlev.get(key, {})
            prev = lev.get(prior_month(t))
            if t in lev and prev and prev > 0:
                ret[key][t] = lev[t] * frac / prev - 1
            else:
                ret[key].pop(t, None)
                unpriced.add((sym, f"{t:%Y-%m}"))
            if exclude_terminal:
                by[key].pop(t, None)
        return by, ret

    ys = yahoo_spy()
    ymaps = {m: a for m, (a, _) in ys.items()}
    bt = run(ib, iret, *BACKTEST, 0.001)
    bt_book = [r for _, r in bt]
    bt_spy = spy_rets(ispy, [m for m, _ in bt])

    print(f"#3843 events {len(events)}: {dict(Counter(e['status'] for e in events))}")
    unc = [e for e in events if e["status"] == "uncovered"]
    print(
        "  uncovered by provision: "
        f"{dict(Counter('no symbol' if e['symbol'] is None else e['provision'] or '(unparsed)' for e in unc))}"
    )
    print(f"covered {len(covered)}: by source {dict(Counter(e['source'] for e in covered))}")
    print(f"  by provision {dict(Counter(e['provision'] for e in covered))}")
    print(f"  pass the holdable predicate {sum(v[4] for v in info.values())}")
    print(f"  dead series with no bars loaded: {sorted(info[k][0] for k in info if k not in dby)}")
    print(f"  covering series already in the survivor universe (termination applied there): {len(already)}")
    print(f"  survivor series replaced by the covering series: {sorted(info[k][0] for k in replaced.values())}")

    arms = (
        ("survivor-only", None),
        ("holdable", True),
        ("holdable, terminal month eligible", False),
        ("all covered", True),
    )
    results = {}
    for arm, excl in arms:
        by, ret = (pb, pret) if excl is None else universe(arm.split(",")[0], excl)
        picks: list = []  # (formation month, top N): the book's own scores, recorded on the way through

        def rank(uni, m, picks=picks):
            scored = [(radj_score(rs), sid) for _, sid, rs in uni]
            picks.append((m, [sid for _, sid in sorted(scored, reverse=True)[:N]]))
            return scored

        audit: list = []
        book = run(by, ret, *OOS, 0.001, audit=audit, rank=rank)
        ms = [m for m, _ in book]
        b = [r for _, r in book]
        raudit: list = []
        rc = sorted(cum([r for _, r in run(by, ret, *OOS, 0.001, f"rand{s}", audit=raudit)]) for s in range(SEEDS))
        results[arm] = (ms, b)
        print(f"\n# {arm}: {len(by)} series")
        print(f"book 0.1%/side: cum {cum(b):+.2%}, {ann(b):+.2%}/yr, maxDD {max_dd(b):.1%}")
        print(
            f"random-30 ({SEEDS} seeds): median {median(rc):+.2%}, range {rc[0]:+.2%}..{rc[-1]:+.2%}; book beats {sum(x < cum(b) for x in rc)}/{SEEDS}"
        )
        print(f"held name-months earning 0% (no holding-month return): book {[(f'{m:%Y-%m}', s) for m, s in audit]}")
        dead_r = Counter(s in info for _, s in raudit)
        print(f"  random, all seeds: {len(raudit)} of {SEEDS * N * len(ms)} (dead names {dead_r[True]})")
        if excl is None:
            continue
        held = [(fm, s) for fm, top in picks for s in top if s in info]
        print(f"held dead-name months: {len(held)}")
        for fm, s in held:
            sym, prov, last, frac, _ = info[s]
            hm = date(fm.year + fm.month // 12, fm.month % 12 + 1, 1)
            r = ret[s].get(hm)
            tag = f"terminal x{frac:g}" if hm == last.replace(day=1) else ""
            print(
                f"  {hm:%Y-%m} {sym:<6} {prov:<6} last bar {last}  return {'none' if r is None else f'{r:+.1%}'} {tag}"
            )

    ms = results["survivor-only"][0]
    spy_o = spy_rets(ymaps, ms)
    k = len(ms)
    ref_lag, _ = worst_lag(bt_book, bt_spy, k)
    print(f"\nSPY: cum {cum(spy_o):+.2%}; backtest worst {k}-month lag {ref_lag:+.2%}")
    for arm, (ams, b) in results.items():
        assert ams == ms, f"{arm} scored different months"
        lag = cum(spy_o) - cum(b)
        print(f"stop rule, {arm}: lag {lag:+.2%} vs {ref_lag:+.2%} -> {'FIRES' if lag > ref_lag else 'does not fire'}")
    print(f"the rule fires if the book's cumulative return is below {cum(spy_o) - ref_lag:+.2%}")
    print(
        f"dead series whose terminal month has no positive prior-month level (termination unpriced): {sorted(unpriced)}"
    )


if __name__ == "__main__":
    main()
