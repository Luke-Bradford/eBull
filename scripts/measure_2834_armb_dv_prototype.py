"""#2834 ARM B stage (i): the dollar-volume-weighted 12-2 replication. Read-only.

The frozen declaration is ``docs/proposals/ta/2026-09-22-armb-dv-weighting-prototype.md``
(merged ``936da1a3``), and it governs every rule here. §-references below point into
it. The trial register charges this run as
``armb-12-2-dv-weighted-stage-i-2026-09-22`` (r10), and the charge was made BEFORE
the first run (§3).

The decision is #2834's bar (i): *"long-decile minus market gross monthly premium
positive with year-clustered t >= 2"*. The long decile is DV-weighted, the market is
DV-weighted, and the t statistic is CR1.

Exit code is the verdict (§1):

- ``0`` PASS: both termination arms clear the bar.
- ``1`` FAIL: inference is valid on both arms, and either arm misses.
- ``2`` UNMEASURABLE: no formations, fewer than 2 year-clusters, a zero or
  non-finite SE, or any formation dropped for a zero DV denominator.

Run::

    PYTHONPATH=. uv run python -m scripts.measure_2834_armb_dv_prototype

``--selection-only`` stops before the first return is computed. It exercises the
corpus pass, the hold-out bound and the oracle cross-check without reading an outcome.

⚠ Changing any rule below after a result has been seen is a NEW trial, and the
register must charge it again. The constants are the declaration.
"""

from __future__ import annotations

import argparse
import bisect
import math
import statistics
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final

import psycopg

from app.config import settings
from app.services.indicator_series import BarSeries, Universe
from app.services.market_regime import REGIME_RULE_VERSION
from app.services.market_regime_provider import RULE_SET_VERSION as BENCHMARK_RULE_SET_VERSION
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION, load_masked_series
from app.services.research_split_corrected_reader import load_ratio_basis
from app.services.series_termination import TERMINATION_RULE_VERSION, classify_termination, terminal_value_fraction
from app.services.strategies.s2_cross_sectional_momentum import (
    DECILE,
    ELIGIBILITY_BARS,
    LOOKBACK_BARS,
    MIN_CLOSE,
    MIN_CROSS_SECTION,
    SKIP_BARS,
    rebalance_dates,
    s2_member,
    s2_select,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_registry import stage_cross_sectional_member
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.technical_analysis import OHLCVRow
from app.services.universe_selection import AdmittedSeries, load_universe_selection
from scripts.verify_2240_s2_cross_sectional import _RANKED_SQL

UNIVERSE: Final[Universe] = "survivorship_free"
#: §2.1: formations from here...
WINDOW_START: Final = date(2000, 1, 1)
#: ...and every bar read is on or before this date. The bound is applied inside
#: the query (``through_date``), so nothing past the hold-out boundary enters memory.
THROUGH: Final = HOLDOUT_BOUNDARY - timedelta(days=1)
#: §2.4, set BY CONSTRUCTION: the skip-month length, ending strictly before ``t``.
DV_BARS: Final = 21
#: §1: #2834's own bar.
T_BAR: Final = 2.0
#: §2.5: both arms are computed, and PASS requires both.
ARMS: Final = ("worst_case", "best_case")

PASS, FAIL, UNMEASURABLE = "PASS", "FAIL", "UNMEASURABLE"
_EXIT: Final = {PASS: 0, FAIL: 1, UNMEASURABLE: 2}


# ---------------------------------------------------------------------------
# Pure logic (tests/test_2834_armb_dv_prototype.py)
# ---------------------------------------------------------------------------


def dollar_volume(qualifying: Sequence[float]) -> float:
    """§2.4: mean ``close × volume`` over the last ``DV_BARS`` qualifying bars.

    ``qualifying`` holds the per-bar products of the series' bars before ``t``
    that have a usable positive close and a non-null volume, oldest first. An
    empty window gives 0.0, and the caller zero-weights it.
    """
    window = qualifying[-DV_BARS:]
    return math.fsum(window) / len(window) if window else 0.0


def normalised(raw: Mapping[int, float]) -> dict[int, float] | None:
    """Weights proportional to ``raw``. ``None`` when the denominator is 0 (§2.4)."""
    total = math.fsum(raw.values())
    if total <= 0.0:
        return None
    return {key: value / total for key, value in raw.items()}


def holding_return(entry: float, exit_mark: float, fraction: float = 1.0) -> float:
    """§2.5: buy and hold from ``entry`` to ``exit_mark`` on the wealth basis.

    ``fraction`` is ``terminal_value_fraction`` for a series that terminates
    inside the interval, and 1 otherwise. The proceeds sit in cash at 0% until
    ``t_next``.
    """
    if entry <= 0.0:
        raise ValueError(f"entry mark {entry} is not positive; the caller must exclude it before weighting")
    return exit_mark * fraction / entry - 1.0


@dataclass(frozen=True)
class Inference:
    """§2.7: mean, CR1 year-clustered SE, and t. ``se``/``t`` are ``None`` when invalid."""

    n: int
    clusters: int
    mean: float | None
    se: float | None
    t: float | None


def cr1(values: Sequence[float], clusters: Sequence[int]) -> Inference:
    """Cluster-robust SE of a mean, CR1 finite-sample factor.

    ``V = G/(G-1) × Σ_g (Σ_{i∈g} (x_i − x̄))² / n²``. Cameron & Miller (2015),
    *A Practitioner's Guide to Cluster-Robust Inference*, J. Human Resources 50(2).

    Validity is judged on the SE itself and not on the variance of the values:
    per-cluster residual sums can cancel to zero while the values still vary.
    """
    if len(values) != len(clusters):
        raise ValueError("values and clusters must align")
    n = len(values)
    groups = len(set(clusters))
    if n == 0:
        return Inference(n=0, clusters=0, mean=None, se=None, t=None)
    mean = math.fsum(values) / n
    if groups < 2:
        return Inference(n=n, clusters=groups, mean=mean, se=None, t=None)
    sums: dict[int, float] = {}
    for value, cluster in zip(values, clusters, strict=True):
        sums[cluster] = sums.get(cluster, 0.0) + (value - mean)
    variance = groups / (groups - 1) * math.fsum(s * s for s in sums.values()) / (n * n)
    se = math.sqrt(variance)
    if not math.isfinite(se) or se == 0.0:
        return Inference(n=n, clusters=groups, mean=mean, se=None, t=None)
    return Inference(n=n, clusters=groups, mean=mean, se=se, t=mean / se)


def profit_factor(values: Sequence[float]) -> float | None:
    """§2.7: ``Σ max(P,0) / Σ max(−P,0)``. ``inf`` with no losses; ``None`` if empty or all zero."""
    gains = math.fsum(v for v in values if v > 0.0)
    losses = math.fsum(-v for v in values if v < 0.0)
    if losses == 0.0:
        return math.inf if gains > 0.0 else None
    return gains / losses


def verdict(by_arm: Mapping[str, Inference], *, dropped_formations: int) -> str:
    """§1, the frozen table."""
    if dropped_formations > 0 or not by_arm:
        return UNMEASURABLE
    if any(inf.t is None or inf.mean is None for inf in by_arm.values()):
        return UNMEASURABLE
    if all(inf.mean is not None and inf.t is not None and inf.mean > 0.0 and inf.t >= T_BAR for inf in by_arm.values()):
        return PASS
    return FAIL


# ---------------------------------------------------------------------------
# The corpus pass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Obs:
    """One eligible name at one formation: its score and inputs, with no outcome."""

    score: float
    dv: float
    entry: float | None
    live_exit: float | None


@dataclass
class _Pass:
    obs: dict[date, dict[int, _Obs]]
    terminal_mark: dict[int, float | None]
    admitted: dict[int, AdmittedSeries]
    rebals: list[date]
    #: Module cross-section size per rebalance date on or after WINDOW_START, for
    #: the date-set half of the oracle cross-check.
    module_sizes: Counter[date]
    max_loaded: date | None
    ratio_methods: Counter[str]


def _calendar(conn: psycopg.Connection[tuple], ids: list[int]) -> list[date]:
    """The panel calendar, as the verify script builds it, bounded at ``THROUGH``."""
    return sorted(
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT d.bar_date
            FROM research_price_daily d
            JOIN research_price_quarantine_coverage cov
              ON cov.series_id = d.series_id
             AND cov.rule_set_version = %(version)s
             AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
            WHERE d.series_id = ANY(%(ids)s)
              AND d.bar_date <= %(through)s
            """,
            {"ids": ids, "version": QUARANTINE_RULE_SET_VERSION, "through": THROUGH},
        ).fetchall()
    )


def _mark_at(dates: list[date], values: list[float], when: date) -> float | None:
    """The last usable wealth close on or before ``when``, or ``None`` if there is none."""
    index = bisect.bisect_right(dates, when) - 1
    return values[index] if index >= 0 else None


def _admission(conn: psycopg.Connection[tuple]) -> dict[int, AdmittedSeries]:
    validated = load_validated_universe(conn)
    selection = load_universe_selection(conn, universe=UNIVERSE, validated_ids=frozenset(validated))
    admitted = {series.name_key: series for series in selection.admitted}
    if len(admitted) != len(selection.admitted):
        raise RuntimeError("two admitted series share one name_key; the admission is broken")
    print(f"  {UNIVERSE} admits {len(admitted)} series from {selection.vendor}", flush=True)
    print(f"  linked_early_reuse_suspect {selection.linked_early_reuse_suspect}", flush=True)
    return admitted


def _stream(
    conn: psycopg.Connection[tuple],
    admitted: Mapping[int, AdmittedSeries],
    *,
    in_window: frozenset[date],
    next_formation: Mapping[date, date],
    with_marks: bool,
) -> _Pass:
    """One pass over the admitted series. ``t_next`` is the next FORMATION (§2.1), not
    the next rebalance date: a rebalance date with no cross-section forms nothing,
    and the book is held through it."""
    rebals = sorted(rebalance_dates(_calendar(conn, [s.series_id for s in admitted.values()])))
    rebal_set = frozenset(rebals)
    module_sizes: Counter[date] = Counter()

    obs: dict[date, dict[int, _Obs]] = {}
    terminal_mark: dict[int, float | None] = {}
    max_loaded: date | None = None
    methods: Counter[str] = Counter()
    started = time.monotonic()
    for n, (key, series_row) in enumerate(admitted.items(), start=1):
        masked = load_masked_series(conn, series_row.series_id, through_date=THROUGH)
        if not masked.bars:
            continue
        last_loaded = masked.bars[-1].bar_date
        max_loaded = last_loaded if max_loaded is None else max(max_loaded, last_loaded)
        rows: list[OHLCVRow] = [
            {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}  # type: ignore[typeddict-item]
            for b in masked.bars
        ]
        series = BarSeries(dates=tuple(b.bar_date for b in masked.bars), rows=tuple(rows))
        corrected, method = load_ratio_basis(conn, series_row.series_id, series, through_date=THROUGH)
        methods[method] += 1
        staged = stage_cross_sectional_member(
            s2_member(
                series,
                ratio_basis=corrected.ratio_basis,
                panel_rebalance_dates=rebal_set,
                universe=UNIVERSE,
                close_reason="quarantined_bar",
            )
        )
        for t in staged.scores:
            if t >= WINDOW_START:
                module_sizes[t] += 1
        scored = {t: s for t, s in staged.scores.items() if t in in_window}
        if not scored:
            continue

        # §2.4: qualifying bars for dollar volume are raw (as-traded) close × volume,
        # with a usable positive close and a non-null volume.
        q_dates: list[date] = []
        q_dv: list[float] = []
        for bar in masked.bars:
            if bar.close is not None and bar.close > 0 and bar.volume is not None and bar.volume >= 0:
                q_dates.append(bar.bar_date)
                q_dv.append(float(bar.close) * float(bar.volume))
        w_dates: list[date] = []
        w_vals: list[float] = []
        for bar, wealth in zip(masked.bars, masked.wealth_closes, strict=True):
            if wealth is not None and wealth > 0:
                w_dates.append(bar.bar_date)
                w_vals.append(float(wealth))

        for t, score in scored.items():
            k = bisect.bisect_left(q_dates, t)
            entry_index = bisect.bisect_left(w_dates, t)
            entry = w_vals[entry_index] if entry_index < len(w_dates) and w_dates[entry_index] == t else None
            obs.setdefault(t, {})[key] = _Obs(
                score=score,
                dv=dollar_volume(q_dv[max(0, k - DV_BARS) : k]),
                entry=entry,
                # ⚠ Outcome marks are derived only on a full run: --selection-only
                # must not read anything dated after a formation.
                live_exit=_mark_at(w_dates, w_vals, next_formation[t]) if with_marks else None,
            )
        if (
            with_marks
            and series_row.termination is not None
            and series_row.last_bar is not None
            and series_row.last_bar <= THROUGH
        ):
            terminal_mark[key] = _mark_at(w_dates, w_vals, series_row.last_bar)
        if n % 1000 == 0:
            print(f"  {n}/{len(admitted)} series ({time.monotonic() - started:.0f}s)", flush=True)
    return _Pass(
        obs=obs,
        terminal_mark=terminal_mark,
        admitted=dict(admitted),
        rebals=rebals,
        module_sizes=module_sizes,
        max_loaded=max_loaded,
        ratio_methods=methods,
    )


def _oracle(
    conn: psycopg.Connection[tuple], admitted: Mapping[int, AdmittedSeries]
) -> dict[date, tuple[set[int], set[int]]]:
    """``t -> (eligible, selected)`` from the independent SQL derivation (§2.3)."""
    out: dict[date, tuple[set[int], set[int]]] = {}
    ids = [s.series_id for s in admitted.values()]
    keys = list(admitted)
    for bar_date, name_key, position, n in conn.execute(
        _RANKED_SQL + "SELECT bar_date, name_key, position, n FROM ranked WHERE n >= %(min_cross_section)s",
        {
            "version": QUARANTINE_RULE_SET_VERSION,
            "ids": ids,
            "keys": keys,
            "skip": SKIP_BARS,
            "lookback": LOOKBACK_BARS,
            "eligibility": ELIGIBILITY_BARS,
            "floor": MIN_CLOSE,
            "min_cross_section": MIN_CROSS_SECTION,
            "through": THROUGH,
        },
    ).fetchall():
        eligible, selected = out.setdefault(bar_date, (set(), set()))
        eligible.add(int(name_key))
        if position <= n // DECILE:
            selected.add(int(name_key))
    return out


@dataclass(frozen=True)
class _Formation:
    when: date
    selected: frozenset[int]
    eligible: frozenset[int]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#2834 ARM B stage (i): DV-weighted 12-2 replication")
    parser.add_argument(
        "--selection-only",
        action="store_true",
        help=(
            "run the corpus pass, the hold-out bound and the oracle cross-check, then stop before any "
            "return is computed. Not a look: it reads no outcome, so it charges nothing."
        ),
    )
    args = parser.parse_args(argv)
    started = time.monotonic()
    print(f"[2834 ARM B stage (i)] window {WINDOW_START} .. through {THROUGH}; DV_BARS={DV_BARS}", flush=True)
    with psycopg.connect(settings.database_url) as conn:
        # One snapshot for every query, so a concurrent ingest cannot mix two corpus states.
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        admitted = _admission(conn)
        oracle = _oracle(conn, admitted)
        formation_dates = sorted(oracle)
        next_formation = dict(zip(formation_dates, formation_dates[1:], strict=False))
        in_window = frozenset(
            t
            for t in formation_dates
            if t >= WINDOW_START and t in next_formation and next_formation[t] < HOLDOUT_BOUNDARY
        )
        run = _stream(
            conn,
            admitted,
            in_window=in_window,
            next_formation=next_formation,
            with_marks=not args.selection_only,
        )
        regimes = MarketRegimeProvider.load_research(conn, through_date=THROUGH)
        conn.rollback()

    if run.max_loaded is not None and run.max_loaded >= HOLDOUT_BOUNDARY:
        raise RuntimeError(f"read a bar on {run.max_loaded}, at or past HOLDOUT_BOUNDARY {HOLDOUT_BOUNDARY}")

    formations: list[_Formation] = []
    mismatches: list[str] = []
    for t in sorted(run.obs):
        scores = {key: o.score for key, o in run.obs[t].items()}
        if len(scores) < MIN_CROSS_SECTION:
            continue
        selected = s2_select(t, scores)
        o_eligible, o_selected = oracle.get(t, (set(), set()))
        if set(scores) != o_eligible or set(selected) != o_selected:
            mismatches.append(
                f"{t}: eligible Δ {len(set(scores) ^ o_eligible)}, selected Δ {len(set(selected) ^ o_selected)}"
            )
        for key in selected:
            if run.admitted[key].last_bar == t:
                raise RuntimeError(f"{t}: selected name {key} has last_bar = t, which s2's refusal forbids")
        formations.append(_Formation(when=t, selected=frozenset(selected), eligible=frozenset(scores)))
    # The date sets must agree too, or a module-only formation would go unchecked.
    module_dates = {t for t, size in run.module_sizes.items() if size >= MIN_CROSS_SECTION}
    oracle_dates = {t for t in oracle if t >= WINDOW_START}
    date_diff = sorted(module_dates ^ oracle_dates)
    missing_obs = sorted(in_window - set(run.obs))
    if mismatches or date_diff or missing_obs:
        print(
            f"REFUSED: oracle and module disagree on {len(mismatches)} formations, "
            f"{len(date_diff)} formation dates {date_diff[:5]}, {len(missing_obs)} unstreamed"
        )
        for line in mismatches[:20]:
            print("   ", line)
        return 2
    in_range = [t for t in run.rebals if WINDOW_START <= t < HOLDOUT_BOUNDARY]
    no_formation = [t for t in in_range if t not in oracle]
    print(f"  in-range rebalance dates with no formation (held through): {len(no_formation)} {no_formation}")
    entry_missing = sum(1 for f in formations for key in f.eligible if run.obs[f.when][key].entry is None)
    print(
        f"  formations {len(formations)} (of {len(in_range)} in-range rebalance dates); "
        f"entry marks missing {entry_missing}"
    )
    if args.selection_only:
        print("  selection-only: oracle agrees on every formation; no outcome mark derived, no return computed")
        return 0

    regime_of = dict(
        zip((f.when for f in formations), regimes.for_dates(tuple(f.when for f in formations)).values, strict=True)
    )

    results: dict[str, dict[str, list[float]]] = {}
    counts: Counter[str] = Counter()
    classes: dict[str, Counter[str]] = {arm: Counter() for arm in ARMS}
    concentration_top1: list[float] = []
    concentration_top5: list[float] = []
    kept: list[_Formation] = []
    effective: list[int] = []
    zero_equal_share: list[float] = []
    dropped = 0
    for arm in ARMS:
        series_out = results.setdefault(arm, {"dv": [], "eq": [], "m_dv": [], "m_eq": []})
        for formation in formations:
            t, t_next = formation.when, next_formation[formation.when]
            obs = run.obs[t]
            h: dict[int, float] = {}
            missing = 0
            for key in formation.eligible:
                o = obs[key]
                if o.entry is None:
                    missing += 1
                    continue
                row = run.admitted[key]
                if row.termination is not None and row.last_bar is not None and row.last_bar <= t_next:
                    termination_class = classify_termination(row.termination)
                    classes[arm][termination_class.value] += 1
                    mark = run.terminal_mark.get(key)
                    if mark is None:
                        missing += 1
                        continue
                    h[key] = holding_return(o.entry, mark, terminal_value_fraction(termination_class, arm))  # type: ignore[arg-type]
                else:
                    if o.live_exit is None:
                        missing += 1
                        continue
                    h[key] = holding_return(o.entry, o.live_exit)
            # ⚠ A missing mark DROPS the formation rather than renormalising over the
            # survivors (Codex ckpt-2): the declared decile and market are the
            # whole eligible sets, and a drop makes the verdict UNMEASURABLE.
            decile = list(formation.selected)
            market = list(formation.eligible)
            w_dv = normalised({key: obs[key].dv for key in decile})
            m_dv = normalised({key: obs[key].dv for key in market})
            if missing or w_dv is None or m_dv is None:
                if arm == ARMS[0]:
                    dropped += 1
                    counts["dropped_missing_mark" if missing else "dropped_zero_dv"] += 1
                continue
            if arm == ARMS[0]:
                kept.append(formation)
                zero = [key for key in decile if obs[key].dv == 0.0]
                counts["zero_dv_members"] += len(zero)
                counts["decile_members"] += len(decile)
                effective.append(len(decile) - len(zero))
                zero_equal_share.append(len(zero) / len(decile))
                shares = sorted(w_dv.values(), reverse=True)
                concentration_top1.append(shares[0])
                concentration_top5.append(math.fsum(shares[:5]))
            series_out["dv"].append(math.fsum(w_dv[key] * h[key] for key in decile))
            series_out["eq"].append(math.fsum(h[key] for key in decile) / len(decile))
            series_out["m_dv"].append(math.fsum(m_dv[key] * h[key] for key in market))
            series_out["m_eq"].append(math.fsum(h[key] for key in market) / len(market))

    years = [f.when.year for f in kept]
    by_arm: dict[str, Inference] = {}
    print(f"\n  formations {len(formations)}; kept {len(kept)}; dropped (zero DV denominator) {dropped}")
    print(f"  first {kept[0].when if kept else None} last {kept[-1].when if kept else None}")
    for arm in ARMS:
        r = results.get(arm, {"dv": [], "eq": [], "m_dv": [], "m_eq": []})
        if len(r["dv"]) != len(years):
            # The dropped set is arm-independent (no DV or mark depends on the arm);
            # a divergence would misalign years with premia, so refuse it.
            raise RuntimeError(f"{arm}: {len(r['dv'])} formations against {len(years)} kept")
        premium = [a - b for a, b in zip(r["dv"], r["m_dv"], strict=True)]
        premium_eq_market = [a - b for a, b in zip(r["dv"], r["m_eq"], strict=True)]
        weighting = [a - b for a, b in zip(r["dv"], r["eq"], strict=True)]
        by_arm[arm] = inf = cr1(premium, years)
        need = 2 * inf.se if inf.se is not None else None
        print(f"\n  [{arm}] DECISION premium R_dv − M_dv: {_fmt(inf)}  PF {_pf(premium)}  needed-to-clear {_pct(need)}")
        print(f"  [{arm}] sensitivity R_dv − M_eq:       {_fmt(cr1(premium_eq_market, years))}")
        print(f"  [{arm}] weighting effect R_dv − R_eq:   {_fmt(cr1(weighting, years))}")
        print(f"  [{arm}] terminations by class: {dict(sorted(classes[arm].items()))}")
        print(f"  [{arm}] per regime (formation date):")
        for regime in sorted(
            {(str(regime_of[f.when]) if regime_of[f.when] is not None else "unclassified") for f in kept}
        ):
            idx = [
                i
                for i, f in enumerate(kept)
                if (str(regime_of[f.when]) if regime_of[f.when] is not None else "unclassified") == regime
            ]
            cohort = cr1([premium[i] for i in idx], [years[i] for i in idx])
            print(f"      {regime:<16} {_fmt(cohort)}")
    if concentration_top1:
        print(
            f"\n  DV concentration top-1 median {statistics.median(concentration_top1):.3f} "
            f"max {max(concentration_top1):.3f}; top-5 median {statistics.median(concentration_top5):.3f} "
            f"max {max(concentration_top5):.3f}"
        )
    if effective:
        print(
            f"  DV effective holdings median {statistics.median(effective)} min {min(effective)}; "
            f"zero-DV members' equal-arm weight median {statistics.median(zero_equal_share):.4f} "
            f"max {max(zero_equal_share):.4f}"
        )
    print(f"  counts {dict(sorted(counts.items()))}")
    print(
        f"  stamp: main {_git_sha()} · quarantine {QUARANTINE_RULE_SET_VERSION} · "
        f"termination {TERMINATION_RULE_VERSION} · regime {REGIME_RULE_VERSION} / {BENCHMARK_RULE_SET_VERSION} · "
        f"admitted {len(run.admitted)} · ratio basis {dict(run.ratio_methods)} · "
        f"max bar read {run.max_loaded}"
    )
    outcome = verdict(by_arm, dropped_formations=dropped)
    print(f"\n  VERDICT {outcome}  (elapsed {time.monotonic() - started:.0f}s)", flush=True)
    return _EXIT[outcome]


def _git_sha() -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    return result.stdout.strip() or "unknown"


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{100 * value:+.3f}%"


def _fmt(inf: Inference) -> str:
    t = "n/a" if inf.t is None else f"{inf.t:+.2f}"
    return f"n={inf.n} G={inf.clusters} mean {_pct(inf.mean)}/mo se {_pct(inf.se)} t {t}"


def _pf(values: Sequence[float]) -> str:
    pf = profit_factor(values)
    return "n/a" if pf is None else ("inf" if math.isinf(pf) else f"{pf:.3f}")


if __name__ == "__main__":
    sys.exit(main())
