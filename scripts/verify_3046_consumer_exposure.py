"""#3046 residual 4 — which raw ``price_daily`` consumers span a quarantined transition.

Spec: ``docs/proposals/ta/2026-09-15-3046-consumer-exposure.md``.
Read-only. Writes nothing. Run from a worktree:

    PYTHONPATH=. uv run python -m scripts.verify_3046_consumer_exposure

WHAT THIS ANSWERS. #3046 scope item 1, verbatim: *"for each raw price_daily
consumer, determine whether its window can span a stored
price_transition_quarantine transition, and report the count of affected
instrument/metric pairs. If zero, close this with that measurement recorded."*

⚠⚠ THE CROSSING PREDICATE IS NOT INVENTED HERE. ``price_quarantine.rule_w1``
already owns it — *"a transition counts as inside the window when that date is in
(window_start, window_end] — the transition INTO the first bar happened before the
window opened and does not contaminate it"* — and it is imported and CALLED, not
mirrored. The first draft of the spec invented ``prior_date >= start AND
price_date <= end`` and the first draft of this script re-implemented it in SQL;
Codex checkpoint 1 killed the first and the second was deleted with it. The two
forms disagree when ``window_start`` falls strictly inside a hole, which is why
every window below is expressed as **the first operand bar the consumer actually
reads** rather than as a raw calendar cutoff.

⚠⚠ ``price_masked_bars`` DOES NOT HONOUR TRANSITION VERDICTS. Its ``_LOAD_SQL``
LEFT JOINs ``price_bar_quarantine`` (B1-B4) and joins ``price_quarantine_coverage``.
It never reads ``price_transition_quarantine``. #3046's body says the strategy path
"therefore honours the verdicts" — true of bar verdicts, false of transitions. The
transition-aware helper is ``price_segments`` (unresolved ``price_series_break``
rows, i.e. T3-minted breaks only), applied by the scan's caller. T2 is consumed by
nothing outside ``price_quarantine_store``'s census.

⚠ A WINDOW HAS TWO HALVES AND THEY ARE NOT THE SAME NUMBER: the OPERAND span (which
bars the value is computed from) and the COMPUTABILITY floor (how many bars the
producer needs before it writes anything at all). Codex checkpoint 2 caught four
places where collapsing them produced a false positive — the clearest being
``ema_12`` on a two-bar series, which this script counted as exposed while
``compute_indicators`` returns NULL for it. ``WindowSpec`` now carries both.

⚠ NO WINDOW CONSTANT IS WRITTEN DOWN HERE. ``_RETURN_WINDOWS``,
``WINDOW_LOOKBACK_DAYS``, ``TRAILING_LOOKBACK_DAYS``, ``_ROLLING_PERIODS``,
``_RISK_BUSINESS_COLS`` and the price-anchor lookback are imported from the modules
that own them, so a producer changing its window moves this measurement instead of
silently invalidating it. The indicator periods are literal at
``technical_analysis``'s own call sites and are cited entry by entry.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, LiteralString

import psycopg

from app.api.portfolio import _ROLLING_PERIODS
from app.config import settings
from app.services.market_data import _RETURN_WINDOWS, most_recent_trading_day
from app.services.price_quarantine import RULE_SET_VERSION, rule_w1
from app.services.risk_metrics import (
    _RISK_BUSINESS_COLS,
    TRAILING_LOOKBACK_DAYS,
    WINDOW_LOOKBACK_DAYS,
)
from app.services.thesis import _PRICE_ANCHOR_LOOKBACK_DAYS
from app.workers.scheduler import BENCHMARK_SYMBOLS

#: Version of this measurement's own classification, so a later run that reports a
#: different number can be told apart from a later run of a different rule.
EXPOSURE_RULE_VERSION = "consumer-exposure-v2"

#: The producer's load cap. ⚠ A CAP, NOT A WINDOW — ``_compute_and_store_features``
#: takes ``LIMIT 400`` twice (once on non-null closes, once on complete OHLCV) and
#: each metric then uses its own slice of that. Treating 400 as the window would
#: over-report every short-window indicator.
PRODUCER_LOAD_CAP = 400

_UNCAPPED = 2147483647

_FROM_PRICE_DAILY = re.compile(r"(?i)\bfrom\s+price_daily\b")


# ---------------------------------------------------------------------------
# 1. The inventory
# ---------------------------------------------------------------------------
#
# One entry per ``FROM price_daily`` occurrence under ``app/``. Line numbers are
# for navigation and are NOT asserted — they churn on every unrelated edit. The
# guard asserts per-file OCCURRENCE COUNTS, which is a drift detector on the file
# set and nothing stronger (see :func:`guard_inventory`).


@dataclass(frozen=True)
class Occurrence:
    path: str
    line: int
    label: str
    kind: str
    #: Which :data:`SPECS` entries report this occurrence's exposure. Every exposed
    #: kind must name at least one, so a consumer cannot be classified as exposed
    #: and then quietly left out of the table — Codex checkpoint 2 found
    #: ``research_comparator_snapshot`` and ``fair_value_band._OWN_HISTORY_SQL``
    #: inventoried as WINDOWED with no measurement attached and no way to notice.
    specs: tuple[str, ...] = ()
    note: str = ""


#: ``kind`` vocabulary. Exposure is a property of the operand pair a metric spans
#: (sql/247 decision 10: *"it is the ratio between them that is not a return"*),
#: never of how many rows a query returns.
KINDS = {
    "PROSE": "comment or docstring, not executed",
    "METADATA": "max(price_date) / count(*) / EXISTS — no price arithmetic",
    "SINGLE_BAR": "one bar's price, used on its own",
    "COMPOSED": "two or more single-bar reads combined into one quantity",
    "DERIVED_COLUMN": "single-bar read of a column market_data computed over a window",
    "WINDOWED": "the consumer does its own multi-bar arithmetic",
    "BAR_MASKED": "reads through the B1-B4 field mask, which carries no transition verdict",
    "PRODUCER": "the quarantine detector itself — excluded, it is the source",
}

#: Kinds that MUST name a spec.
EXPOSED_KINDS = ("WINDOWED", "COMPOSED", "DERIVED_COLUMN", "BAR_MASKED")

#: Named once so the DERIVED_COLUMN readers below can cite exactly what they consume.
_RETURN_COLS = tuple(_RETURN_WINDOWS)
_TA_TREND_COLS = ("sma_50", "sma_200")
_SCORING_COLS = (
    "return_1m",
    "return_3m",
    "return_6m",
    "sma_200",
    "macd_histogram",
    "rsi_14",
    "stoch_k",
    "stoch_d",
    "bb_upper",
    "bb_lower",
    "atr_14",
)
_TA_COLS = (
    "sma_20",
    "sma_50",
    "sma_200",
    "bb_upper",
    "bb_lower",
    "stoch_k",
    "stoch_d",
    "ema_12",
    "ema_26",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    "rsi_14",
    "atr_14",
)
_ROLLING_PNL_SPECS = tuple(f"rolling_pnl_{label}" for label, _ in _ROLLING_PERIODS)

INVENTORY: tuple[Occurrence, ...] = (
    # --- the producer of every DERIVED_COLUMN below -----------------------
    Occurrence(
        "app/services/market_data.py",
        1583,
        "_compute_and_store_features closes",
        "WINDOWED",
        (*_RETURN_COLS, "volatility_30d"),
        "400 non-null closes -> the rolling returns + volatility_30d",
    ),
    Occurrence(
        "app/services/market_data.py",
        1611,
        "_compute_and_store_features OHLCV",
        "WINDOWED",
        _TA_COLS,
        "400 complete-OHLCV bars -> the 14 TA columns",
    ),
    Occurrence(
        "app/services/market_data.py",
        254,
        "load_day_changes",
        "WINDOWED",
        ("day_change",),
        "the two most recent STRICTLY POSITIVE closes",
    ),
    Occurrence(
        "app/services/market_data.py",
        1235,
        "_stored_overlap_closes",
        "SINGLE_BAR",
        (),
        "#2066 overlap read: stored closes compared to freshly fetched ones on the SAME dates",
    ),
    Occurrence(
        "app/services/market_data.py",
        273,
        "load_day_changes bar_count",
        "METADATA",
        (),
        "#3046: count(*) of STORED bars between the two operands — rule_w2's interval "
        "count, never a rank and never a calendar estimate. No price is read.",
    ),
    Occurrence("app/services/market_data.py", 148, "_last_bar", "METADATA"),
    # #2414 item 2 — the backdated-insert classification frontier. Deliberately a
    # SECOND read of the same aggregate `_last_bar` returns: that one runs before
    # `BEGIN`, this one inside the bar write's transaction. Same kind, different
    # snapshot, and the difference is the whole point.
    Occurrence("app/services/market_data.py", 1520, "_observed_frontier", "METADATA"),
    # --- #3046 build item 1: the verdict-aware window loader ---------------
    # ⚠ All three read ``price_date`` ONLY — no close, no OHLC, no arithmetic on a
    # price. This module's job is to say whether somebody ELSE's window is sound;
    # it computes no quantity of its own, so it is not itself exposed.
    Occurrence(
        "app/services/price_window_verdict.py",
        251,
        "load_window_inputs weekend bar dates",
        "METADATA",
        (),
        "weekend days this instrument PRINTED on — a printed day is not deducted from an observed span",
    ),
    Occurrence(
        "app/services/price_window_verdict.py",
        266,
        "load_window_inputs weekend habit",
        "METADATA",
        (),
        "weekend share of bars over the instrument's own last WEEKEND_HABIT_DAYS — "
        "decides whether an ABSENT weekend day is a closure or a hole",
    ),
    Occurrence(
        "app/services/price_window_verdict.py",
        269,
        "load_window_inputs habit anchor",
        "METADATA",
        (),
        "max(price_date), the instrument's own last bar, so the habit lookback ends "
        "at its own history rather than at current_date",
    ),
    Occurrence("app/services/market_data.py", 1042, "_candles_are_fresh", "METADATA"),
    Occurrence("app/services/market_data.py", 1114, "_candles_fetch_count", "METADATA"),
    # --- own multi-bar arithmetic -----------------------------------------
    Occurrence(
        "app/services/risk_metrics.py",
        1396,
        "_load_closes",
        "WINDOWED",
        tuple(f"risk_window_{k}" for k in WINDOW_LOOKBACK_DAYS)
        + tuple(f"risk_trailing_{k}" for k in TRAILING_LOOKBACK_DAYS),
        "whole series <= as_of; sliced to 1y/3y/full + trailing_* by compute_instrument_risk",
    ),
    Occurrence("app/services/risk_metrics.py", 1659, "eligibility pre-filter", "METADATA"),
    Occurrence(
        "app/services/market_regime_provider.py",
        206,
        "MarketRegimeProvider.load",
        "WINDOWED",
        ("benchmark_propagation",),
        "loads the SPY series only; its exposure is the benchmark section of the report",
    ),
    Occurrence(
        "app/services/research_comparator_snapshot.py", 318, "comparator bars", "WINDOWED", ("comparator_series",)
    ),
    Occurrence(
        "app/services/return_attribution.py",
        141,
        "_load_prices",
        "WINDOWED",
        ("caller_span_whole_history",),
        "caller-supplied [start, end] — upper bound only",
    ),
    Occurrence(
        "app/services/reporting.py",
        1091,
        "_close (period endpoints)",
        "COMPOSED",
        ("caller_span_whole_history",),
        "called twice; the two closes become a period return — upper bound only",
    ),
    Occurrence(
        "app/services/thesis.py",
        1219,
        "52w high/low aggregate",
        "WINDOWED",
        ("thesis_52w_range",),
        "every non-null close at-or-after the cutoff, uncapped",
    ),
    Occurrence("app/services/thesis_outcomes.py", 144, "maturity max(price_date)", "METADATA"),
    Occurrence("app/services/thesis_outcomes.py", 282, "scoreboard max(price_date)", "METADATA"),
    Occurrence(
        "app/services/fair_value_band.py",
        1369,
        "_OWN_HISTORY_SQL",
        "WINDOWED",
        ("fvb_own_history",),
        "one close per fundamentals quarter -> a multiple series",
    ),
    Occurrence(
        "app/api/portfolio.py",
        757,
        "rolling P&L current close",
        "COMPOSED",
        _ROLLING_PNL_SPECS,
        "the numerator; the prior close below is the denominator",
    ),
    Occurrence(
        "app/api/portfolio.py",
        764,
        "rolling P&L prior close",
        "COMPOSED",
        _ROLLING_PNL_SPECS,
        "one lateral per _ROLLING_PERIODS lookback",
    ),
    Occurrence(
        "app/api/portfolio.py",
        995,
        "equity-curve closes",
        "WINDOWED",
        ("caller_span_whole_history",),
        "carry-forward series over the requested span",
    ),
    Occurrence(
        "app/api/instruments.py",
        1232,
        "price-history (max range)",
        "WINDOWED",
        ("price_history_chart",),
        "frontend ChartWorkspaceCanvas.tsx computes SMA/EMA/normalised returns from these bars",
    ),
    Occurrence(
        "app/api/instruments.py",
        1242,
        "price-history (bounded range)",
        "WINDOWED",
        ("price_history_chart",),
        "same consumer, request-bounded span",
    ),
    # --- reads of the producer's stored window columns ---------------------
    Occurrence("app/services/scoring.py", 1261, "single-instrument price features", "DERIVED_COLUMN", _SCORING_COLS),
    Occurrence(
        "app/services/scoring.py",
        1622,
        "bulk price features",
        "DERIVED_COLUMN",
        _SCORING_COLS,
        "same columns as the single-instrument path; the pair count de-duplicates them",
    ),
    Occurrence(
        "app/services/entry_timing.py",
        125,
        "latest TA row",
        "DERIVED_COLUMN",
        ("sma_200", "rsi_14", "macd_histogram", "bb_upper", "bb_lower", "atr_14"),
    ),
    Occurrence(
        "app/services/thesis.py",
        1202,
        "price anchor row",
        "DERIVED_COLUMN",
        (*_RETURN_COLS, *_TA_TREND_COLS, "rsi_14", "macd_histogram", "atr_14", "volatility_30d"),
    ),
    Occurrence(
        "app/services/thesis_break_scan.py", 109, "regime metrics", "DERIVED_COLUMN", (*_TA_TREND_COLS, "rsi_14")
    ),
    # --- composed single-bar reads ------------------------------------------
    Occurrence(
        "app/services/thesis.py",
        564,
        "latest close",
        "COMPOSED",
        ("caller_span_whole_history",),
        "_price_move_pct divides this against the mint close below; the span is the "
        "thesis's own age, so it is a caller span",
    ),
    Occurrence(
        "app/services/thesis.py",
        574,
        "mint close at-or-before",
        "COMPOSED",
        ("caller_span_whole_history",),
        "the denominator of the staleness move",
    ),
    # --- single-bar price reads --------------------------------------------
    Occurrence("app/services/portfolio_eod.py", 372, "EOD mark", "SINGLE_BAR"),
    Occurrence("app/services/valuation.py", 145, "position mark", "SINGLE_BAR"),
    Occurrence("app/services/strategy_monitoring.py", 690, "position mark fallback", "SINGLE_BAR"),
    Occurrence("app/services/portfolio.py", 248, "mirror mark", "SINGLE_BAR"),
    Occurrence("app/services/portfolio.py", 337, "mirror equity mark", "SINGLE_BAR"),
    Occurrence("app/api/portfolio.py", 575, "instrument drill mark", "SINGLE_BAR"),
    Occurrence("app/api/copy_trading.py", 288, "mirror position mark", "SINGLE_BAR"),
    Occurrence("app/api/copy_trading.py", 409, "mirror position mark", "SINGLE_BAR"),
    Occurrence("app/services/thesis_dq_audit.py", 271, "close_row_at_or_before", "SINGLE_BAR"),
    Occurrence("app/services/xbrl_derived_stats.py", 289, "period-end close", "SINGLE_BAR"),
    Occurrence("app/services/fcf_yield.py", 117, "quarterly period-end close", "SINGLE_BAR"),
    Occurrence("app/services/fcf_yield.py", 136, "annual period-end close", "SINGLE_BAR"),
    Occurrence("app/services/fair_value_band.py", 888, "as-of close", "SINGLE_BAR"),
    Occurrence("app/services/fair_value_band.py", 1407, "_TARGET_PRICE_SQL", "SINGLE_BAR"),
    # --- metadata / freshness ----------------------------------------------
    Occurrence("app/services/portfolio_eod.py", 337, "_resolve_snapshot_date", "METADATA"),
    Occurrence("app/services/fair_value_band.py", 853, "corpus frontier", "METADATA"),
    Occurrence("app/services/strategy_scan_freshness.py", 119, "frontier", "METADATA"),
    Occurrence("app/services/strategy_scan_freshness.py", 121, "prior trading day walk", "METADATA"),
    Occurrence("app/services/sync_orchestrator/content_predicates.py", 35, "candle content predicate", "METADATA"),
    Occurrence("app/services/sync_orchestrator/freshness.py", 143, "candle freshness", "METADATA"),
    Occurrence("app/services/sync_orchestrator/freshness.py", 208, "latest_candle", "METADATA"),
    Occurrence("app/services/sync_orchestrator/registry.py", 108, "candles EXISTS", "METADATA"),
    Occurrence("app/services/processes/watermarks.py", 312, "prices watermark", "METADATA"),
    Occurrence("app/services/ops_monitor.py", 212, "prices layer freshness", "METADATA"),
    Occurrence("app/services/scoring.py", 1378, "price-history depth", "METADATA"),
    Occurrence("app/services/scoring.py", 1733, "price-history depth (bulk)", "METADATA"),
    Occurrence("app/services/scoring.py", 2277, "eligibility EXISTS", "METADATA"),
    Occurrence("app/workers/scheduler.py", 664, "eligibility EXISTS", "METADATA"),
    Occurrence("app/workers/scheduler.py", 3175, "_T3_CANDLE_SELECT last_bar", "METADATA"),
    Occurrence("app/workers/scheduler.py", 3418, "post-refresh usable count", "METADATA"),
    # --- the masked loader, which carries bar verdicts and no transition ----
    Occurrence(
        "app/services/price_masked_bars.py",
        75,
        "_LOAD_SQL",
        "BAR_MASKED",
        ("caller_span_whole_history",),
        "B1-B4 field mask; transitions reach the scan via price_segments, T3-minted breaks only",
    ),
    Occurrence("app/services/price_masked_bars.py", 95, "_LAST_BAR_SQL", "METADATA"),
    Occurrence("app/services/price_masked_bars.py", 119, "_RECENT_LAST_BAR_COUNTS_SQL", "METADATA"),
    Occurrence("app/services/price_masked_bars.py", 133, "_UNION_CALENDAR_SQL", "METADATA"),
    # --- the detector itself ------------------------------------------------
    Occurrence("app/services/price_quarantine_store.py", 50, "coverage scope", "PRODUCER"),
    Occurrence("app/services/price_quarantine_store.py", 59, "_SERIES_SQL", "PRODUCER"),
    Occurrence("app/services/price_quarantine_store.py", 421, "census denominator", "PRODUCER"),
    # --- prose ---------------------------------------------------------------
    Occurrence("app/services/account_reconciliation_ledger.py", 214, "docstring", "PROSE"),
    Occurrence("app/services/signal_ledger.py", 174, "docstring", "PROSE"),
    Occurrence("app/services/return_attribution.py", 329, "docstring", "PROSE"),
    Occurrence("app/services/sync_orchestrator/freshness.py", 300, "comment", "PROSE"),
    Occurrence("app/services/strategies/s2_cross_sectional_momentum.py", 206, "docstring", "PROSE"),
    Occurrence("app/services/strategies/s2_cross_sectional_momentum.py", 300, "docstring", "PROSE"),
    Occurrence("app/workers/scheduler.py", 419, "comment", "PROSE"),
)


def scan_occurrences(repo_root: Path) -> Counter[str]:
    """Per-file ``FROM price_daily`` occurrence counts under ``repo_root / "app"``.

    Keys are repo-relative POSIX paths, matching :data:`INVENTORY`.
    """
    found: Counter[str] = Counter()
    for path in sorted((repo_root / "app").rglob("*.py")):
        n = sum(1 for line in path.read_text().splitlines() if _FROM_PRICE_DAILY.search(line))
        if n:
            found[path.relative_to(repo_root).as_posix()] = n
    return found


def guard_inventory(repo_root: Path) -> list[str]:
    """Return the drift between the declared inventory and the codebase.

    ⚠ WHAT THIS GUARD DOES **NOT** DO, stated because the obvious claim overreaches
    and Codex checkpoint 1 said so:

    * it binds occurrence COUNTS, not classifications — swapping one reader for
      another inside a file passes;
    * a new consumer of an EXISTING helper (``risk_metrics._load_closes``, the
      price-history endpoint) adds no occurrence and is invisible to it;
    * it does not cover ``frontend/``, where ``ChartWorkspaceCanvas.tsx`` computes
      SMA/EMA and normalised returns from the raw candles the price-history
      endpoint serves.

    It is a drift detector on the file set. That is worth having, and it is all
    this is.
    """
    declared = Counter(occ.path for occ in INVENTORY)
    found = scan_occurrences(repo_root)
    problems: list[str] = []
    for path in sorted(set(declared) | set(found)):
        if declared[path] != found[path]:
            problems.append(f"{path}: inventory declares {declared[path]}, codebase has {found[path]}")
    unknown = sorted({occ.kind for occ in INVENTORY} - set(KINDS))
    if unknown:
        problems.append(f"unknown kind(s) in inventory: {unknown}")
    return problems


def unmeasured_exposed_occurrences() -> list[str]:
    """Exposed inventory entries that no :data:`SPECS` entry reports."""
    known = {s.name for s in SPECS} | {"benchmark_propagation"}
    problems: list[str] = []
    for occ in INVENTORY:
        if occ.kind not in EXPOSED_KINDS:
            continue
        if not occ.specs:
            problems.append(f"{occ.path}:{occ.line} is {occ.kind} but names no spec")
            continue
        for name in occ.specs:
            if name not in known:
                problems.append(f"{occ.path}:{occ.line} names unknown spec {name!r}")
    return problems


# ---------------------------------------------------------------------------
# 2. Window specifications
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WindowSpec:
    """One measurable window. ``name`` is the metric half of the reported pair.

    ⚠ ``min_bars`` is the COMPUTABILITY floor and is not the same thing as the
    operand span. ``ema_12`` reads its whole supplied slice but needs 12 bars
    before ``compute_indicators`` writes anything; ``stoch_k`` reads the last 14
    bars but is only produced when 16 exist. Collapsing the two counted metrics
    the producer never wrote.
    """

    name: str
    consumer: str
    slice_: str
    #: window start — exactly one of these
    rank: int | None = None  # the nth-most-recent bar of the slice
    rank_or_oldest: int | None = None  # the nth, or the oldest when the slice is shorter
    whole_slice: bool = False
    calendar_days: int | None = None  # latest bar at-or-before end - days, excluding the end bar
    oldest_after: int | None = None  # oldest bar at-or-after end - days
    trailing_from_as_of: int | None = None  # latest bar at-or-before end - days
    #: computability floor in bars, when it is not implied by ``rank``
    min_bars: int | None = None
    #: the producer only writes the TA columns when the newest complete-OHLCV bar
    #: is also the newest close row; otherwise it stores NULL and nothing is exposed
    ta_gate: bool = False
    #: caller- or request-parameterised span: reported, not counted in the headline
    upper_bound: bool = False
    #: how many stored metric columns this window produces (risk windows produce many)
    metric_columns: int = 1
    source: str = ""


#: ``_RISK_BUSINESS_COLS`` partitioned by what ``compute_instrument_risk``'s own
#: docstring says: *"trailing_* / excess_trailing_* are calendar-lookback from
#: as_of_date and therefore window-INDEPENDENT ... intentionally identical across
#: the 1y/3y/full rows."* Everything else is computed from the SLICED window.
#: Counts, ids, the drawdown DATES and the ``*_status`` enums are excluded: they
#: describe the window rather than carrying a price statistic a level break
#: corrupts.
_RISK_NON_METRIC = {
    "n_returns",
    "beta_n_obs",
    "benchmark_instrument_id",
    "window_days",
    "max_dd_peak_date",
    "max_dd_trough_date",
}
_RISK_TRAILING_COLS = tuple(c for c in _RISK_BUSINESS_COLS if c.startswith(("trailing_", "excess_trailing_")))
_RISK_WINDOW_COLS = tuple(
    c
    for c in _RISK_BUSINESS_COLS
    if c not in _RISK_NON_METRIC and c not in _RISK_TRAILING_COLS and not c.endswith("_status")
)
#: The columns an exposed SPY or sector SPDR would corrupt on an instrument whose
#: own series is clean.
#:
#: ⚠ ``excess_kurtosis`` IS EXCLUDED AND THE PREFIX TEST ALONE GETS IT WRONG. It is a
#: distribution moment of the instrument's OWN returns — "excess" there means "over
#: the normal distribution", not "over a benchmark" — and the first run printed it in
#: the dependent list. Statuses and observation counts go out for the same reason they
#: are out of :data:`_RISK_WINDOW_COLS`: they describe the window, not a price statistic.
_RISK_BENCHMARK_DEPENDENT_COLS = tuple(
    c
    for c in _RISK_BUSINESS_COLS
    if c.startswith(("beta", "excess_"))
    and c != "excess_kurtosis"
    and c not in _RISK_NON_METRIC
    and not c.endswith("_status")
)


def _producer_specs() -> list[WindowSpec]:
    """The 20 stored feature columns, each at its own operand span and floor."""
    specs: list[WindowSpec] = [
        WindowSpec(col, "market_data._compute_rolling_returns", "close", calendar_days=days, source="_RETURN_WINDOWS")
        for col, days in _RETURN_WINDOWS.items()
    ]
    # ⚠ NOT rank=31. ``_compute_volatility_30d`` takes ``prices[-31:]`` — UP TO 31 —
    # and returns a value whenever 5 log returns survive. Demanding a 31st bar
    # excluded every 6-to-30-bar history that has a stored volatility.
    specs.append(
        WindowSpec(
            "volatility_30d",
            "market_data._compute_volatility_30d",
            "close",
            rank_or_oldest=31,
            min_bars=6,
            source="prices[-31:] with a >= 5 valid-return floor",
        )
    )
    for col, period in (("sma_20", 20), ("sma_50", 50), ("sma_200", 200), ("bb_upper", 20), ("bb_lower", 20)):
        specs.append(
            WindowSpec(
                col,
                "technical_analysis.sma/bollinger",
                "ohlcv",
                rank=period,
                ta_gate=True,
                source=f"period={period} at the compute_indicators call site",
            )
        )
    # ⚠ %K reads the LAST 14 bars; only %D spans all 16. Both need 16 to exist.
    specs.append(
        WindowSpec(
            "stoch_k",
            "technical_analysis.stochastic (%K)",
            "ohlcv",
            rank=14,
            min_bars=16,
            ta_gate=True,
            source="14 operands, 16-bar floor",
        )
    )
    specs.append(
        WindowSpec(
            "stoch_d",
            "technical_analysis.stochastic (%D)",
            "ohlcv",
            rank=16,
            ta_gate=True,
            source="k_period + d_period - 1 = 16",
        )
    )
    # Seeded-recursive: the whole supplied slice is the operand span, but each has
    # its own minimum before compute_indicators emits it.
    for col, floor, why in (
        ("ema_12", 12, "ema seeds on the first 12 closes"),
        ("ema_26", 26, "ema seeds on the first 26 closes"),
        ("macd_line", 34, "slow + signal - 1 = 26 + 9 - 1"),
        ("macd_signal", 34, "slow + signal - 1"),
        ("macd_histogram", 34, "slow + signal - 1"),
        ("rsi_14", 15, "period + 1 deltas"),
        ("atr_14", 15, "period + 1 bars (the first is the prev-close anchor)"),
    ):
        specs.append(
            WindowSpec(
                col,
                "technical_analysis (seeded-recursive)",
                "ohlcv",
                whole_slice=True,
                min_bars=floor,
                ta_gate=True,
                source=why,
            )
        )
    return specs


def _risk_specs() -> list[WindowSpec]:
    specs: list[WindowSpec] = []
    weight = len(_RISK_WINDOW_COLS)
    for key, lookback in WINDOW_LOOKBACK_DAYS.items():
        if lookback is None:
            specs.append(
                WindowSpec(
                    f"risk_window_{key}",
                    "risk_metrics._slice_window",
                    "valid",
                    whole_slice=True,
                    min_bars=2,
                    metric_columns=weight,
                    source=f"WINDOW_LOOKBACK_DAYS[{key}] is None x {weight} stored cols",
                )
            )
        else:
            specs.append(
                WindowSpec(
                    f"risk_window_{key}",
                    "risk_metrics._slice_window",
                    "valid",
                    oldest_after=lookback,
                    min_bars=2,
                    metric_columns=weight,
                    source=f"WINDOW_LOOKBACK_DAYS[{key}] x {weight} stored cols",
                )
            )
    for key, lookback in TRAILING_LOOKBACK_DAYS.items():
        # trailing_<key> and its excess_trailing_<key> twin.
        trailing_weight = sum(1 for c in _RISK_TRAILING_COLS if c.endswith(key))
        specs.append(
            WindowSpec(
                f"risk_trailing_{key}",
                "risk_metrics.trailing_return",
                "valid",
                trailing_from_as_of=lookback,
                metric_columns=trailing_weight,
                source=f"TRAILING_LOOKBACK_DAYS[{key}] x {trailing_weight} stored cols",
            )
        )
    return specs


def _consumer_specs() -> list[WindowSpec]:
    specs: list[WindowSpec] = [
        # ⚠ load_day_changes ranks ``close > 0`` rows, NOT ``close IS NOT NULL``.
        # A non-positive sentinel between the two bars changes which pair is compared.
        WindowSpec(
            "day_change",
            "market_data.load_day_changes",
            "positive",
            rank=2,
            source="the two most recent strictly-positive closes",
        ),
        # ⚠ The thesis aggregate keeps every non-null close at-or-after the cutoff and
        # applies no 400-bar cap, so its first operand is the OLDEST surviving bar.
        WindowSpec(
            "thesis_52w_range",
            "thesis price anchor",
            "close_uncapped",
            oldest_after=_PRICE_ANCHOR_LOOKBACK_DAYS,
            min_bars=1,
            source="_PRICE_ANCHOR_LOOKBACK_DAYS, uncapped",
        ),
        WindowSpec(
            "comparator_series",
            "research_comparator_snapshot",
            "close_uncapped",
            whole_slice=True,
            min_bars=2,
            upper_bound=True,
            source="fixed comparator frontier — reported as whole-history",
        ),
        WindowSpec(
            "fvb_own_history",
            "fair_value_band._OWN_HISTORY_SQL",
            "close_uncapped",
            whole_slice=True,
            min_bars=2,
            upper_bound=True,
            source="one close per fundamentals quarter — span follows the fundamentals history",
        ),
        WindowSpec(
            "price_history_chart",
            "api.instruments price-history -> ChartWorkspaceCanvas.tsx",
            "close_uncapped",
            whole_slice=True,
            min_bars=2,
            upper_bound=True,
            source="max range is literally whole history; bounded ranges are a subset",
        ),
        WindowSpec(
            "caller_span_whole_history",
            "return_attribution / reporting / equity curve / thesis staleness / masked loader",
            "close_uncapped",
            whole_slice=True,
            min_bars=2,
            upper_bound=True,
            source="span comes from the caller or the request, not from the corpus",
        ),
    ]
    for label, days in _ROLLING_PERIODS:
        specs.append(
            WindowSpec(
                f"rolling_pnl_{label}",
                "api.portfolio rolling P&L",
                "close_uncapped",
                calendar_days=days,
                source=f"_ROLLING_PERIODS[{label}] = {days} calendar days, uncapped",
            )
        )
    return specs


SPECS: tuple[WindowSpec, ...] = tuple(_producer_specs() + _risk_specs() + _consumer_specs())


# ---------------------------------------------------------------------------
# 3. Anchors — fixed statements per slice, then the rule itself in Python
# ---------------------------------------------------------------------------
#
# ⚠⚠ THE CROSSING TEST CALLS ``rule_w1`` DIRECTLY. The first implementation
# mirrored it as a SQL predicate and ran one full-corpus window scan PER METRIC —
# 30 passes over 6.7M bars, which did not finish inside ten minutes. Both halves of
# that were wrong: a mirror of an imported rule can drift from it silently, and a
# per-metric scan re-derives the same row_number() thirty times.
#
# ⚠⚠ FIXED STATEMENTS, NOT f-STRINGS. ``result_ledger`` states the repo's rule —
# *"FIXED statements, never f-strings built from a column list. psycopg types
# ``query`` as ``LiteralString`` precisely to stop dynamic SQL, and the chokepoint
# lint catches the f-string form — correctly."* The slice predicate, the load cap
# and the as-of ceiling are PARAMETERS, and every offset arrives as an array.

_SLICE_FILTER: LiteralString = """
    WITH sl AS (
        SELECT d.instrument_id, d.price_date,
               row_number() OVER (PARTITION BY d.instrument_id ORDER BY d.price_date DESC) AS rn
        FROM price_daily d
        WHERE d.close IS NOT NULL
          AND (NOT %(require_ohlcv)s OR (d.open IS NOT NULL AND d.high IS NOT NULL AND d.low IS NOT NULL))
          AND (NOT %(require_positive)s OR (NOT (d.close = 'NaN'::numeric) AND d.close > 0))
          AND (NOT %(cap_today)s OR d.price_date <= CURRENT_DATE)
    ),
    capped AS (SELECT * FROM sl WHERE rn <= %(cap)s),
    ends AS (
        SELECT instrument_id,
               max(price_date) FILTER (WHERE rn = 1) AS win_end,
               min(price_date) AS oldest,
               count(*) AS depth
        FROM capped GROUP BY 1
    )
"""

_ENDS_AND_RANKS_SQL: LiteralString = (
    _SLICE_FILTER
    + """
    SELECT e.instrument_id, e.win_end, e.oldest, e.depth, c.rn, c.price_date
    FROM ends e
    JOIN capped c USING (instrument_id)
    WHERE c.rn = ANY(%(ranks)s)
    """
)

_ENDS_ONLY_SQL: LiteralString = (
    _SLICE_FILTER
    + """
    SELECT e.instrument_id, e.win_end, e.oldest, e.depth FROM ends e
    """
)

#: All three offset-relative anchors in one pass. ``unnest`` carries the offsets as
#: DATA, which is what keeps the statement fixed however many windows SPECS grows.
_OFFSET_ANCHORS_SQL: LiteralString = (
    _SLICE_FILTER
    + """
    SELECT e.instrument_id,
           o.days,
           -- the producer excludes the latest bar itself (``prices[:-1]``) and takes
           -- the closest available close at-or-before the target date
           max(c.price_date) FILTER (WHERE c.rn >= 2 AND c.price_date <= e.win_end - o.days) AS cal,
           -- _slice_window keeps cutoff <= d <= as_of, so the first operand is the
           -- OLDEST surviving bar, not the cutoff date
           min(c.price_date) FILTER (WHERE c.price_date >= e.win_end - o.days) AS oldest_after,
           -- trailing_return takes the nearest valid close at or before as_of - lookback
           max(c.price_date) FILTER (WHERE c.price_date <= e.win_end - o.days) AS trail
    FROM ends e
    JOIN capped c USING (instrument_id)
    CROSS JOIN unnest(%(offsets)s::int[]) AS o(days)
    GROUP BY 1, 2
    """
)

#: The load predicates of the modules that produce each metric, as parameters.
#: ⚠ ``cap_today`` is on for ``valid`` ALONE: ``compute_all_instrument_risk`` calls
#: ``load_close_series(..., current_date)`` and ``_slice_window`` re-enforces the
#: ceiling, so a future-dated bar cannot participate. The producer's own slices take
#: the newest STORED bar and have no such ceiling. (Measured 2026-09-15: zero
#: future-dated bars exist, so this is currently inert — correct, not load-bearing.)
SLICES: dict[str, dict[str, Any]] = {
    "close": {"require_ohlcv": False, "require_positive": False, "cap": PRODUCER_LOAD_CAP, "cap_today": False},
    "ohlcv": {"require_ohlcv": True, "require_positive": False, "cap": PRODUCER_LOAD_CAP, "cap_today": False},
    "valid": {"require_ohlcv": False, "require_positive": True, "cap": _UNCAPPED, "cap_today": True},
    "close_uncapped": {"require_ohlcv": False, "require_positive": False, "cap": _UNCAPPED, "cap_today": False},
    "positive": {"require_ohlcv": False, "require_positive": True, "cap": _UNCAPPED, "cap_today": False},
}


@dataclass(frozen=True)
class Anchors:
    """Every window boundary one slice can produce, per instrument."""

    win_end: date
    oldest: date
    depth: int
    by_rank: dict[int, date] = field(default_factory=dict)
    by_calendar: dict[int, date] = field(default_factory=dict)
    by_oldest_after: dict[int, date] = field(default_factory=dict)
    by_trailing: dict[int, date] = field(default_factory=dict)


def _ranks_for(slice_: str) -> tuple[int, ...]:
    wanted: set[int] = set()
    for s in SPECS:
        if s.slice_ != slice_:
            continue
        for value in (s.rank, s.rank_or_oldest):
            if value is not None:
                wanted.add(value)
    return tuple(sorted(wanted))


def _offsets_for(slice_: str) -> tuple[int, ...]:
    wanted: set[int] = set()
    for s in SPECS:
        if s.slice_ != slice_:
            continue
        for value in (s.calendar_days, s.oldest_after, s.trailing_from_as_of):
            if value is not None:
                wanted.add(value)
    return tuple(sorted(wanted))


def load_anchors(conn: psycopg.Connection[Any], slice_: str) -> dict[int, Anchors]:
    """Every window boundary ``slice_`` can produce, in two passes over the corpus."""
    ranks = _ranks_for(slice_)
    offsets = _offsets_for(slice_)
    params = SLICES[slice_]

    ends: dict[int, tuple[date, date, int]] = {}
    by_rank: dict[int, dict[int, date]] = {}
    if ranks:
        for iid_raw, win_end, oldest, depth, rn, price_date in conn.execute(
            _ENDS_AND_RANKS_SQL, {**params, "ranks": list(ranks)}
        ).fetchall():
            iid = int(iid_raw)
            ends[iid] = (win_end, oldest, int(depth))
            by_rank.setdefault(iid, {})[int(rn)] = price_date
    # ⚠ A rank query only returns instruments that HAVE one of those ranks, so the
    # ends map has to be filled independently or every short series vanishes from
    # the denominator — which would make the table read as better-covered than it
    # is. Revert-probed on the corpus: dropping this pass loses 22 instruments on
    # the close slice, 102 on ohlcv, and all 12,284 on valid (which asks no ranks).
    for iid_raw, win_end, oldest, depth in conn.execute(_ENDS_ONLY_SQL, params).fetchall():
        ends[int(iid_raw)] = (win_end, oldest, int(depth))

    by_cal: dict[int, dict[int, date]] = {}
    by_after: dict[int, dict[int, date]] = {}
    by_trail: dict[int, dict[int, date]] = {}
    if offsets:
        for iid_raw, days_raw, cal, after, trail in conn.execute(
            _OFFSET_ANCHORS_SQL, {**params, "offsets": list(offsets)}
        ).fetchall():
            iid, days = int(iid_raw), int(days_raw)
            if cal is not None:
                by_cal.setdefault(iid, {})[days] = cal
            if after is not None:
                by_after.setdefault(iid, {})[days] = after
            if trail is not None:
                by_trail.setdefault(iid, {})[days] = trail

    return {
        iid: Anchors(
            win_end=win_end,
            oldest=oldest,
            depth=depth,
            by_rank=by_rank.get(iid, {}),
            by_calendar=by_cal.get(iid, {}),
            by_oldest_after=by_after.get(iid, {}),
            by_trailing=by_trail.get(iid, {}),
        )
        for iid, (win_end, oldest, depth) in ends.items()
    }


def window_start(spec: WindowSpec, a: Anchors) -> date | None:
    """The first operand bar the consumer actually reads, or None if not computable.

    None is the warm-up case and is CORRECT to exclude: a metric the producer never
    wrote cannot be exposed.
    """
    if spec.min_bars is not None and a.depth < spec.min_bars:
        return None
    if spec.rank is not None:
        return a.by_rank.get(spec.rank)
    if spec.rank_or_oldest is not None:
        return a.by_rank.get(spec.rank_or_oldest, a.oldest)
    if spec.whole_slice:
        return a.oldest
    if spec.calendar_days is not None:
        return a.by_calendar.get(spec.calendar_days)
    if spec.oldest_after is not None:
        return a.by_oldest_after.get(spec.oldest_after)
    if spec.trailing_from_as_of is not None:
        return a.by_trailing.get(spec.trailing_from_as_of)
    raise ValueError(f"{spec.name}: no window start rule")


@dataclass(frozen=True)
class Measurement:
    spec: WindowSpec
    computable_all: int
    exposed_all: int
    computable_fresh: int
    exposed_fresh: int
    computable_ranked: int
    exposed_ranked: int


def measure(
    spec: WindowSpec,
    anchors: dict[str, dict[int, Anchors]],
    transitions: dict[int, list[date]],
    ta_last: dict[int, date],
    ranked: set[int],
    at_frontier: date,
) -> Measurement:
    counts = dict.fromkeys(
        ("computable_all", "exposed_all", "computable_fresh", "exposed_fresh", "computable_ranked", "exposed_ranked"),
        0,
    )
    close_anchors = anchors["close"]
    for iid, a in anchors[spec.slice_].items():
        # The producer writes the TA columns only when the newest complete-OHLCV
        # bar is also the newest close row; otherwise it stores NULL.
        #
        # ⚠ FAIL CLOSED ON EITHER SIDE BEING ABSENT, rather than letting `None ==
        # None` satisfy the gate. Review nitpick on PR #3071. The both-absent case
        # is unreachable today — the `ohlcv` predicate requires a non-null close, so
        # every instrument in that slice has a `close` anchor and a `ta_last`, and
        # the corpus agrees (0 instruments with a complete-OHLCV bar and no non-null
        # close). But it is unreachable by an invariant of the slice PREDICATES, not
        # by anything stated here, and a comparison that reads "both missing, so the
        # producer must have written TA" is the wrong default if a slice ever changes.
        if spec.ta_gate:
            close_anchor = close_anchors.get(iid)
            instrument_ta_last = ta_last.get(iid)
            if close_anchor is None or instrument_ta_last is None:
                continue
            if instrument_ta_last != close_anchor.win_end:
                continue
        start = window_start(spec, a)
        if start is None:
            continue
        crossed = rule_w1(start, a.win_end, transitions.get(iid, ()))
        counts["computable_all"] += 1
        counts["exposed_all"] += crossed
        if a.win_end >= at_frontier:
            counts["computable_fresh"] += 1
            counts["exposed_fresh"] += crossed
        if iid in ranked:
            counts["computable_ranked"] += 1
            counts["exposed_ranked"] += crossed
    return Measurement(spec, **counts)


# ---------------------------------------------------------------------------
# 4. Reconciliation — the run refuses rather than reporting a number
# ---------------------------------------------------------------------------


def reconcile(conn: psycopg.Connection[Any]) -> tuple[list[str], list[str]]:
    """Coverage and version checks. Returns ``(fatal, disclosed)``.

    ⚠ THE UNCOVERED TAIL IS DISCLOSED, NOT FATAL, AND THAT IS A DELIBERATE SPLIT.
    ``price_quarantine`` evaluates on a schedule while ``daily_candle_refresh``
    appends bars continuously, so on any ordinary day some bars sit past their
    instrument's ``last_bar``. Aborting on that would make this script unrunnable
    on almost every day — a gate that always fires trains you to bypass it. The
    bias it introduces has a KNOWN DIRECTION: a transition into an unevaluated bar
    was never minted, so every count below is a **lower bound**. That is stated in
    the report rather than hidden behind a pass.

    Genuinely fatal, because either makes the numbers mean something else: an
    instrument with bars and NO coverage row at all, and a transition row at a
    rule-set version other than the current one.
    """
    fatal: list[str] = []
    disclosed: list[str] = []

    uncovered = conn.execute(
        """
        SELECT count(*) FROM (SELECT DISTINCT instrument_id FROM price_daily) p
        WHERE NOT EXISTS (
            SELECT 1 FROM price_quarantine_coverage cov
            WHERE cov.instrument_id = p.instrument_id AND cov.rule_set_version = %(ver)s
        )
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    if uncovered and int(uncovered[0]):
        fatal.append(f"{int(uncovered[0])} instrument(s) with bars have no coverage row at {RULE_SET_VERSION}")

    tail = conn.execute(
        """
        SELECT
            count(*) FILTER (WHERE d.price_date > cov.last_bar) AS after_last,
            count(DISTINCT d.instrument_id) FILTER (WHERE d.price_date > cov.last_bar) AS after_last_ids,
            count(*) FILTER (WHERE d.price_date < cov.first_bar) AS before_first,
            count(DISTINCT d.instrument_id) FILTER (WHERE d.price_date < cov.first_bar) AS before_first_ids
        FROM price_daily d
        JOIN price_quarantine_coverage cov
          ON cov.instrument_id = d.instrument_id AND cov.rule_set_version = %(ver)s
        WHERE d.price_date > cov.last_bar OR d.price_date < cov.first_bar
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    if tail is not None and (int(tail[0]) or int(tail[2])):
        disclosed.append(
            f"{int(tail[0])} bar(s) / {int(tail[1])} instrument(s) sit AFTER the evaluated interval "
            f"(the normal append lag), and {int(tail[2])} bar(s) / {int(tail[3])} instrument(s) BEFORE "
            "it (a backfill the detector has not revisited). Transitions into those bars were never "
            "minted, so every exposure count below is a LOWER BOUND."
        )

    stale_versions = conn.execute(
        "SELECT count(*) FROM price_transition_quarantine WHERE rule_set_version <> %(ver)s",
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    if stale_versions and int(stale_versions[0]):
        fatal.append(
            f"{int(stale_versions[0])} transition row(s) carry a rule_set_version other than {RULE_SET_VERSION}"
        )

    return fatal, disclosed


def _git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except Exception:  # noqa: BLE001 — run identity is best-effort, the measurement is not
        return "unknown"


def load_transitions(conn: psycopg.Connection[Any]) -> dict[int, list[date]]:
    """Quarantined transitions, keyed by instrument, as their LATER dates.

    ``rule_w1`` is documented to take *"the LATER bar of each quarantined
    transition"*, so the prior date is deliberately not carried — passing the
    earlier date would invert the rule's own boundary.
    """
    out: dict[int, list[date]] = {}
    rows = conn.execute(
        """
        SELECT instrument_id, price_date
        FROM price_transition_quarantine
        WHERE rule_set_version = %(ver)s AND cardinality(rules) > 0
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchall()
    for iid, price_date in rows:
        out.setdefault(int(iid), []).append(price_date)
    return out


def load_ranked(conn: psycopg.Connection[Any]) -> set[int]:
    """The population ``app/api/scores.py`` will actually surface.

    Latest ``scored_at`` per model version, ``is_tradable``, and
    ``coverage.filings_status = 'analysable'`` (#268 chunk J, #1918). "Has a
    ``scores`` row" is a different and larger set, and is not used.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT s.instrument_id
        FROM scores s
        JOIN instruments i USING (instrument_id)
        JOIN coverage c USING (instrument_id)
        WHERE i.is_tradable AND c.filings_status = 'analysable'
          AND (s.model_version, s.scored_at) IN (
              SELECT model_version, max(scored_at) FROM scores GROUP BY model_version
          )
        """
    ).fetchall()
    return {int(r[0]) for r in rows}


# ---------------------------------------------------------------------------
# 5. Report
# ---------------------------------------------------------------------------


def _print_header(drift: list[str]) -> None:
    print("=" * 100)
    print("#3046 residual 4 — raw price_daily consumer exposure")
    print("=" * 100)
    print(f"  git                       {_git_sha()}")
    print(f"  exposure rule             {EXPOSURE_RULE_VERSION}")
    print(f"  quarantine RULE_SET       {RULE_SET_VERSION}")
    print("  crossing predicate        price_quarantine.rule_w1 — (window_start, window_end] on the LATER date")
    if drift:
        print("\n⛔ INVENTORY DRIFT — a reader appeared, vanished, or lost its measurement:")
        for line in drift:
            print(f"    {line}")
        return
    counts = Counter(occ.kind for occ in INVENTORY)
    print(
        f"\n  inventory                 {len(INVENTORY)} occurrences across "
        f"{len({o.path for o in INVENTORY})} files, guard clean"
    )
    for kind, n in sorted(counts.items()):
        print(f"    {kind:<16} {n:>3}   {KINDS[kind]}")


def _print_benchmarks(
    benchmarks: dict[str, int],
    anchors: dict[str, dict[int, Anchors]],
    transitions: dict[int, list[date]],
) -> None:
    print("\n" + "-" * 100)
    print("BENCHMARK PROPAGATION — an exposed benchmark corrupts dependent metrics on instruments")
    print("whose own series is clean")
    print("-" * 100)
    exposed: list[str] = []
    for symbol, iid in sorted(benchmarks.items()):
        a = anchors["valid"].get(iid)
        if a is None:
            print(f"  {symbol:<10} no valid closes")
            continue
        whole = rule_w1(a.oldest, a.win_end, transitions.get(iid, ()))
        if whole:
            exposed.append(symbol)
        print(
            f"  {symbol:<10} instrument {iid:<8} {a.depth:>6} bars to {a.win_end}   "
            f"whole-history exposure: {'EXPOSED' if whole else 'clean'}"
        )
    dependent_cols = len(_RISK_BENCHMARK_DEPENDENT_COLS)
    if exposed:
        pairs = len(anchors["valid"]) * dependent_cols * len(WINDOW_LOOKBACK_DAYS)
        print(f"\n  ⛔ {len(exposed)} benchmark(s) exposed: {', '.join(exposed)}")
        print(
            f"     Upper bound on dependent pairs: {pairs} ({len(anchors['valid'])} instruments x "
            f"{dependent_cols} benchmark-dependent columns x {len(WINDOW_LOOKBACK_DAYS)} windows)."
        )
        print("     ⚠ compute_instrument_risk slices the benchmark at EACH dependent instrument's")
        print("       as_of_date, so a per-pair verdict needs that date, not the benchmark's own.")
    else:
        print("\n  Dependent pairs: 0, and it is a CONTAINMENT argument rather than a sample.")
        print("  Every benchmark is clean over its WHOLE history, and every window any consumer takes")
        print("  against a benchmark is a sub-interval of that history, so no slice of it can contain")
        print("  a transition — at any dependent instrument's as_of_date.")
        print(
            f"  ({dependent_cols} benchmark-dependent risk columns would be affected otherwise: "
            f"{', '.join(_RISK_BENCHMARK_DEPENDENT_COLS)})"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".", help="repo root for the inventory guard")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    drift = guard_inventory(repo_root) + unmeasured_exposed_occurrences()
    _print_header(drift)
    if drift:
        return 1

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        fatal, disclosed = reconcile(conn)
        if fatal:
            print("\n⛔ RECONCILIATION FAILED — refusing to report a number:")
            for line in fatal:
                print(f"    {line}")
            return 1
        print("  coverage reconciliation   every instrument with bars has a coverage row at the current")
        print("                            version; every stored transition carries the current version")
        for line in disclosed:
            print(f"  ⚠ uncovered tail          {line}")

        frontier_row = conn.execute("SELECT max(price_date) FROM price_daily").fetchone()
        today_row = conn.execute("SELECT CURRENT_DATE").fetchone()
        assert frontier_row is not None and today_row is not None
        frontier: date = frontier_row[0]
        fetch_target = most_recent_trading_day(today_row[0])
        # ⚠ THE STRATUM IS THE CORPUS FRONTIER, NOT market_data's FRESHNESS TARGET,
        # and the first run of this script proved why. ``most_recent_trading_day``
        # is a FETCH target — "the bar we should try to have" — so before the day's
        # candle refresh lands it sits one session AHEAD of every stored series and
        # the stratum came back empty (0/0 on every window). An empty stratum reads
        # exactly like "nothing is exposed".
        at_frontier = frontier
        print(f"  corpus frontier           {frontier}   <- the 'at frontier' stratum boundary")
        print(
            f"  market_data fetch target  {fetch_target}   (most_recent_trading_day; ahead of the "
            "frontier until the day's refresh lands — NOT used as the stratum)"
        )

        deferred = conn.execute(
            """
            SELECT count(*), count(DISTINCT instrument_id)
            FROM price_transition_quarantine
            WHERE rule_set_version = %(ver)s AND cardinality(rules) = 0
            """,
            {"ver": RULE_SET_VERSION},
        ).fetchone()
        quarantined = conn.execute(
            """
            SELECT count(*), count(DISTINCT instrument_id)
            FROM price_transition_quarantine
            WHERE rule_set_version = %(ver)s AND cardinality(rules) > 0
            """,
            {"ver": RULE_SET_VERSION},
        ).fetchone()
        assert deferred is not None and quarantined is not None
        print(f"  quarantined transitions   {quarantined[0]} rows / {quarantined[1]} instruments  (counted)")
        print(
            f"  zero-rule transitions     {deferred[0]} rows / {deferred[1]} instruments  "
            "(deferred or admitted — NOT counted, and not evidence of safety)"
        )

        transitions = load_transitions(conn)
        ranked = load_ranked(conn)
        anchors = {slice_: load_anchors(conn, slice_) for slice_ in sorted({s.slice_ for s in SPECS})}
        ta_last = {
            int(r[0]): r[1]
            for r in conn.execute(
                """
                SELECT instrument_id, max(price_date)
                FROM price_daily
                WHERE open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
                GROUP BY 1
                """
            ).fetchall()
        }
        benchmarks = {
            str(r[0]): int(r[1])
            for r in conn.execute(
                "SELECT symbol, instrument_id FROM instruments WHERE symbol = ANY(%(s)s)",
                {"s": sorted(BENCHMARK_SYMBOLS)},
            ).fetchall()
        }
        print(
            f"  ranked population         {len(ranked)} instruments "
            "(latest scored_at per model + is_tradable + filings_status='analysable')"
        )

        measurements = [measure(spec, anchors, transitions, ta_last, ranked, at_frontier) for spec in SPECS]

    print("\n" + "-" * 100)
    print("EXPOSED WINDOWS — instruments whose metric window spans a quarantined transition")
    print("-" * 100)
    print(f"{'metric':<24}{'cols':>5}{'all':>16}{'at frontier':>16}{'ranked':>16}   window source")
    for m in measurements:
        flag = "  [upper bound]" if m.spec.upper_bound else ""
        print(
            f"{m.spec.name:<24}{m.spec.metric_columns:>5}"
            f"{m.exposed_all:>7}/{m.computable_all:<8}"
            f"{m.exposed_fresh:>7}/{m.computable_fresh:<8}"
            f"{m.exposed_ranked:>7}/{m.computable_ranked:<8}"
            f"   {m.spec.source}{flag}"
        )

    _print_benchmarks(benchmarks, anchors, transitions)

    counted = [m for m in measurements if not m.spec.upper_bound]
    print("\n" + "-" * 100)
    print("PAIR COUNT — scope item 1's answer. A pair is (instrument_id, stored metric column).")
    print("A column read by two call sites counts once; a risk WINDOW carries several columns.")
    print("-" * 100)
    for label, attr in (("all", "exposed_all"), ("at frontier", "exposed_fresh"), ("ranked", "exposed_ranked")):
        windows = sum(getattr(m, attr) for m in counted)
        pairs = sum(getattr(m, attr) * m.spec.metric_columns for m in counted)
        print(
            f"  {label:<12} {pairs:>8} instrument/metric pairs "
            f"({windows} instrument/window, weighted by each window's stored column count)"
        )
    print("\n  Upper-bound (caller- or request-parameterised) windows are printed above and excluded")
    print("  from these totals; their span is not a property of the corpus.")

    print("\n" + "-" * 100)
    print("EVERY DECLARED CONSUMER — printed so the table above cannot be read as a partial list")
    print("-" * 100)
    for occ in INVENTORY:
        specs = f"  -> {', '.join(occ.specs)}" if occ.specs else ""
        print(f"  {occ.kind:<15} {occ.path}:{occ.line}  {occ.label}{specs}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
