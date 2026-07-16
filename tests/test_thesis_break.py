"""Pure-logic tests for the #2012 break-predicate layer (fast tier, no DB).

Extractor strings are REAL dev-corpus break conditions (2026-07-16 census)
— the table pins the precision contract: everything ambiguous, composite,
duration-qualified, float-denominated or direction-conflicted fails OPEN.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from app.services.thesis_break import (
    BASELINE_GRACE,
    FRESHNESS_BOUNDS,
    METRIC_UNITS,
    BreakPredicate,
    MetricInput,
    evaluate_predicate,
    extract_predicates,
    next_baseline_state,
    observe,
    sanitize_writer_break_predicates,
)

# ---------------------------------------------------------------------------
# Extractor — corpus table
# ---------------------------------------------------------------------------

EXTRACTS = [
    # altman numeric — verb families + prepositions + annotations
    ("Altman Z-score falls below 1.8", ("altman_z", "<", 1.8)),
    ("Altman Z-score falls below 1.8 (current 1.805)", ("altman_z", "<", 1.8)),
    ("Altman Z-score drops below 2.0", ("altman_z", "<", 2.0)),
    ("Altman Z-score rises above 2.99 (indicating reduced distress risk)", ("altman_z", ">", 2.99)),
    ("Altman Z-score moves above 2.99 (indicating reduced distress)", ("altman_z", ">", 2.99)),
    ("Altman Z-score improves to >2.99 (non-distress)", ("altman_z", ">", 2.99)),
    ("Altman Z-score improvement to >2.99 (indicating non-distress)", ("altman_z", ">", 2.99)),
    ("Altman Z-score crosses below -1.8 (indicating severe distress)", ("altman_z", "<", -1.8)),
    ("Altman Z-score crossing into bankruptcy territory (<1.8)", ("altman_z", "<", 1.8)),
    ("Altman Z-score crosses into 'non-distress' territory (>2.99)", ("altman_z", ">", 2.99)),
    ("improvement in Altman Z-score above 1.23", ("altman_z", ">", 1.23)),
    ("material improvement in Altman Z-score above -2.99 (distress threshold)", ("altman_z", ">", -2.99)),
    ("reduction in Altman Z-score below -16.2 (unlikely without drastic restructuring)", ("altman_z", "<", -16.2)),
    ("Altman Z-score deteriorates below 1.8 (distress threshold)", ("altman_z", "<", 1.8)),
    ("Altman Z-score worsens below -1.0", ("altman_z", "<", -1.0)),
    ("Dropbox's Altman Z-score deteriorates below -5 (indicating heightened bankruptcy risk)", ("altman_z", "<", -5.0)),
    # rsi
    ("RSI-14 crosses above 70 (overbought territory)", ("rsi_14", ">", 70.0)),
    ("RSI-14 drops below 30 (oversold territory)", ("rsi_14", "<", 30.0)),
    # short interest — EXPLICIT shares-outstanding denominator only
    ("Short interest rises above 5% of shares outstanding", ("short_interest_pct_shares_out", ">", 5.0)),
    (
        "Short interest rises above 5% of shares outstanding (current: 4.21%)",
        ("short_interest_pct_shares_out", ">", 5.0),
    ),
    ("Short interest exceeds 20% of shares outstanding", ("short_interest_pct_shares_out", ">", 20.0)),
    (
        "Material increase in short interest above 20% of shares outstanding",
        ("short_interest_pct_shares_out", ">", 20.0),
    ),
    ("Short interest falls below 5% of shares outstanding", ("short_interest_pct_shares_out", "<", 5.0)),
    # price vs sma200 regime
    ("Technical breakdown below 200-day SMA", ("price_vs_sma200", "<", None)),
    ("Technical breakdown below 116.05 (200-day SMA)", ("price_vs_sma200", "<", None)),
    ("Technical breakdown below SMA 200 ($354.40)", ("price_vs_sma200", "<", None)),
    ("Price drops below 200-day SMA (current: $26.29)", ("price_vs_sma200", "<", None)),
    # sma 50/200 regime (cross wording extracts as the regime, spec Design 5)
    ("SMA 50 crosses below SMA 200 (death cross confirmation)", ("sma_50_vs_sma_200", "<", None)),
    ("sma50 crosses below sma200 (current 'death' regime)", ("sma_50_vs_sma_200", "<", None)),
    ("Technical indicators confirm bearish regime (sma_50 below sma_200)", ("sma_50_vs_sma_200", "<", None)),
    ("Technical indicators show bearish crossover (sma_50 below sma_200)", ("sma_50_vs_sma_200", "<", None)),
]

FAIL_OPEN = [
    # float / bare denominator (spec Design 4 — never substitute shares_out)
    "Short interest exceeds 10% of float",
    "Material increase in short interest above 10% of float",
    "Short interest increases above 15% (current: 12.99%)",
    "Increase in short interest above 10%",
    # composite / disjunction / conjunction (Design 7)
    "Short interest above 12% of float or days-to-cover >5",
    "Short interest falls below 5% of shares outstanding with a 50% decline in days-to-cover",
    "RSI-14 drops below 30 with confirmed bearish crossover",
    "Altman Z-score transitions to 'safe' or 'cautious' band",
    # illustrative e.g. wrappers — the head is the real, broader condition
    "Technical indicators confirm bearish regime (e.g., sma 50 crosses below sma 200)",
    "Reversal of negative technical indicators (e.g., rsi above 50, sma 50 crosses above sma 200)",
    # duration / persistence — not a single-scan edge trigger
    "Sustained RSI below 40 (oversold) for 2+ weeks",
    "RSI-14 falls below 40 for 30 days",
    "RSI-14 crosses below 30 for 2+ weeks",
    "Price closes below 200-day SMA ($120.20) for 20+ days",
    "Price breaks below 200-day SMA ($22.30) for sustained period",
    "Technical indicators confirm bearish regime (sma 50 < sma 200 for 60+ days)",
    "EPS remains negative for consecutive quarters",
    # deadline / failure-to-achieve semantics
    "Altman Z-score improves to >2.99 (non-distress) within 12 months",
    "Failure to secure partnership or financing within 6 months",
    "failure to improve altman z-score above 1.8 (distress band)",
    # direction conflict (verb says up, preposition says down) — writer error
    "Altman Z-score improves below -1.5 (current: -2.1853)",
    # band-only wording — writer band vocabulary ≠ our band cut, fail open
    "Downgrade in Altman Z score below 'safe' band",
    "Altman Z-score confirms insolvency risk",
    "Altman Z-score improves to non-distress levels",
    # metric not in vocabulary / leaked context metadata
    "Net debt exceeds $700 million",
    "Piotroski F-score falls below 5",
    "beta exceeding 1.6 {window_key: '1y', as_of_date: '2026-07-07', metric_version: 'risk_v1'}",
    "Loss of key intellectual property",
    "",
]


@pytest.mark.parametrize(("text", "expected"), EXTRACTS)
def test_extracts(text: str, expected: tuple[str, str, float | None] | None) -> None:
    (pred,) = extract_predicates([text])
    if expected is None:
        assert pred is None
        return
    assert pred is not None, text
    assert (pred.metric, pred.op, pred.threshold) == expected
    assert pred.source_text == text


@pytest.mark.parametrize("text", FAIL_OPEN)
def test_fail_open(text: str) -> None:
    (pred,) = extract_predicates([text])
    assert pred is None, text


def test_index_alignment_and_non_strings() -> None:
    out = extract_predicates(["Altman Z-score falls below 1.8", "prose only", "RSI-14 crosses above 70"])
    assert out[0] is not None and out[0].metric == "altman_z"
    assert out[1] is None
    assert out[2] is not None and out[2].metric == "rsi_14"


def test_non_string_element_never_shifts_later_indexes() -> None:
    # Codex ckpt-2: a malformed (non-string) element must map to None IN
    # PLACE so predicate_index stays aligned with break_conditions_json.
    out = extract_predicates([42, "RSI-14 crosses above 70"])
    assert out[0] is None
    assert out[1] is not None and out[1].metric == "rsi_14"


# ---------------------------------------------------------------------------
# observe() — per-input freshness bounds, fail-closed
# ---------------------------------------------------------------------------

TODAY = date(2026, 7, 16)


def _inp(value: float, age_days: int, source: str = "s") -> MetricInput:
    return MetricInput(value, TODAY - timedelta(days=age_days), source)


def test_observe_ok_takes_stalest_as_of() -> None:
    obs = observe(
        "short_interest_pct_shares_out",
        {"finra_settlement": _inp(1e6, 10), "share_count_filed": _inp(1e8, 100)},
        1.0,
        today=TODAY,
    )
    assert obs.status == "ok"
    assert obs.as_of == TODAY - timedelta(days=100)  # stalest bounded input


def test_observe_each_input_bounded_independently() -> None:
    # FINRA fresh but share count beyond ITS OWN 183d bound → stale_input,
    # even though 200d would be inside a 456d-style collapsed bound.
    obs = observe(
        "short_interest_pct_shares_out",
        {"finra_settlement": _inp(1e6, 10), "share_count_filed": _inp(1e8, 200)},
        1.0,
        today=TODAY,
    )
    assert obs.status == "stale_input"


def test_observe_missing_input_and_missing_value() -> None:
    assert (
        observe(
            "short_interest_pct_shares_out",
            {"finra_settlement": _inp(1e6, 10), "share_count_filed": None},
            1.0,
            today=TODAY,
        ).status
        == "no_input"
    )
    assert observe("rsi_14", {"price_date": _inp(50.0, 1)}, None, today=TODAY).status == "no_input"


def test_observe_stale_price() -> None:
    assert observe("rsi_14", {"price_date": _inp(75.0, 11)}, 75.0, today=TODAY).status == "stale_input"


# ---------------------------------------------------------------------------
# evaluate_predicate — thresholds + regime gap
# ---------------------------------------------------------------------------


def test_evaluate_threshold_ops() -> None:
    below = BreakPredicate("altman_z", "<", 1.8, "zscore", "t")
    obs = observe("altman_z", {"fy_period_end": _inp(1.5, 100)}, 1.5, today=TODAY)
    assert evaluate_predicate(below, obs) == "true"
    obs = observe("altman_z", {"fy_period_end": _inp(1.9, 100)}, 1.9, today=TODAY)
    assert evaluate_predicate(below, obs) == "false"


def test_evaluate_regime_gap_against_zero() -> None:
    death = BreakPredicate("sma_50_vs_sma_200", "<", None, "regime", "t")
    obs = observe("sma_50_vs_sma_200", {"price_date": _inp(-2.5, 1)}, -2.5, today=TODAY)
    assert evaluate_predicate(death, obs) == "true"
    obs = observe("sma_50_vs_sma_200", {"price_date": _inp(2.5, 1)}, 2.5, today=TODAY)
    assert evaluate_predicate(death, obs) == "false"


def test_evaluate_passes_through_non_ok() -> None:
    pred = BreakPredicate("rsi_14", ">", 70.0, "index", "t")
    stale = observe("rsi_14", {"price_date": _inp(80.0, 30)}, 80.0, today=TODAY)
    assert evaluate_predicate(pred, stale) == "stale_input"


# ---------------------------------------------------------------------------
# Baseline state machine (spec Design 5)
# ---------------------------------------------------------------------------

NOW = datetime(2026, 7, 16, 5, 22, tzinfo=UTC)
FRESH = NOW - timedelta(hours=6)  # thesis created inside grace
OLD = NOW - BASELINE_GRACE - timedelta(days=5)  # rollout / late first scan


def test_pending_false_arms() -> None:
    assert next_baseline_state("pending", "false", newly_inserted=True, thesis_created_at=FRESH, now=NOW) == (
        "armed",
        False,
    )


def test_pending_true_contemporaneous_is_premise() -> None:
    assert next_baseline_state("pending", "true", newly_inserted=True, thesis_created_at=FRESH, now=NOW) == (
        "already_true",
        False,
    )


def test_pending_true_rollout_is_after_gap() -> None:
    # Never pending, but the first scan ran > grace after thesis creation.
    assert next_baseline_state("pending", "true", newly_inserted=True, thesis_created_at=OLD, now=NOW) == (
        "already_true_after_gap",
        False,
    )


def test_pending_true_ever_pending_is_after_gap_even_inside_grace() -> None:
    # Row left pending by a prior scan → gap is unobserved REGARDLESS of size.
    assert next_baseline_state("pending", "true", newly_inserted=False, thesis_created_at=FRESH, now=NOW) == (
        "already_true_after_gap",
        False,
    )


def test_armed_true_fires_once_per_transition() -> None:
    assert next_baseline_state("armed", "true", newly_inserted=False, thesis_created_at=FRESH, now=NOW) == (
        "armed",
        True,
    )


def test_armed_false_holds() -> None:
    assert next_baseline_state("armed", "false", newly_inserted=False, thesis_created_at=FRESH, now=NOW) == (
        "armed",
        False,
    )


@pytest.mark.parametrize("state", ["already_true", "already_true_after_gap"])
def test_premise_rearms_on_observed_false(state: str) -> None:
    assert next_baseline_state(state, "false", newly_inserted=False, thesis_created_at=OLD, now=NOW) == (  # type: ignore[arg-type]
        "armed",
        False,
    )


@pytest.mark.parametrize("state", ["pending", "armed", "already_true", "already_true_after_gap"])
@pytest.mark.parametrize("result", ["no_input", "stale_input"])
def test_non_ok_input_never_moves_state_or_fires(state: str, result: str) -> None:
    assert next_baseline_state(state, result, newly_inserted=False, thesis_created_at=OLD, now=NOW) == (  # type: ignore[arg-type]
        state,
        False,
    )


def test_premise_true_stays() -> None:
    assert next_baseline_state("already_true", "true", newly_inserted=False, thesis_created_at=FRESH, now=NOW) == (
        "already_true",
        False,
    )


# ---------------------------------------------------------------------------
# sanitize_writer_break_predicates — writer-native channel (#2010)
# ---------------------------------------------------------------------------


def test_sanitize_none_is_empty_ok() -> None:
    assert sanitize_writer_break_predicates(None, 3) == ([], [])


def test_sanitize_non_list_dropped_whole() -> None:
    survivors, reasons = sanitize_writer_break_predicates({"metric": "rsi_14"}, 3)
    assert survivors == []
    assert reasons == ["break_predicates is not a list: dict"]


def test_sanitize_valid_threshold_entry_survives_normalized() -> None:
    raw = [{"condition_index": 1, "metric": "altman_z", "op": "<", "threshold": 2}]
    survivors, reasons = sanitize_writer_break_predicates(raw, 2)
    assert reasons == []
    assert survivors == [{"condition_index": 1, "metric": "altman_z", "op": "<", "threshold": 2.0}]


def test_sanitize_valid_regime_entry_survives() -> None:
    raw = [{"condition_index": 0, "metric": "price_vs_sma200", "op": "<", "threshold": None}]
    survivors, reasons = sanitize_writer_break_predicates(raw, 1)
    assert reasons == []
    assert survivors == [{"condition_index": 0, "metric": "price_vs_sma200", "op": "<", "threshold": None}]


@pytest.mark.parametrize(
    "entry, reason_fragment",
    [
        ("not a dict", "not an object"),
        ({"condition_index": 0, "metric": "float_pct", "op": "<", "threshold": 1.0}, "unknown metric"),
        ({"condition_index": 0, "metric": "rsi_14", "op": "<=", "threshold": 30}, "invalid op"),
        ({"condition_index": 2, "metric": "rsi_14", "op": "<", "threshold": 30}, "out of range"),
        ({"condition_index": -1, "metric": "rsi_14", "op": "<", "threshold": 30}, "out of range"),
        ({"condition_index": True, "metric": "rsi_14", "op": "<", "threshold": 30}, "out of range"),
        ({"condition_index": "0", "metric": "rsi_14", "op": "<", "threshold": 30}, "out of range"),
        ({"condition_index": 0, "metric": "rsi_14", "op": "<", "threshold": "30"}, "non-numeric threshold"),
        ({"condition_index": 0, "metric": "rsi_14", "op": "<", "threshold": True}, "non-numeric threshold"),
        ({"condition_index": 0, "metric": "rsi_14", "op": "<", "threshold": None}, "non-numeric threshold"),
        ({"condition_index": 0, "metric": "rsi_14", "op": "<", "threshold": float("nan")}, "non-finite"),
        ({"condition_index": 0, "metric": "sma_50_vs_sma_200", "op": "<", "threshold": 1.0}, "regime metric"),
    ],
)
def test_sanitize_invalid_entries_dropped(entry: object, reason_fragment: str) -> None:
    survivors, reasons = sanitize_writer_break_predicates([entry], 2)
    assert survivors == []
    assert len(reasons) == 1 and reason_fragment in reasons[0]


def test_sanitize_duplicate_condition_index_second_dropped() -> None:
    raw = [
        {"condition_index": 0, "metric": "rsi_14", "op": "<", "threshold": 30},
        {"condition_index": 0, "metric": "altman_z", "op": "<", "threshold": 1.8},
    ]
    survivors, reasons = sanitize_writer_break_predicates(raw, 1)
    assert [s["metric"] for s in survivors] == ["rsi_14"]
    assert len(reasons) == 1 and "duplicate condition_index" in reasons[0]


def test_sanitize_mixed_keeps_survivors() -> None:
    raw = [
        {"condition_index": 0, "metric": "short_interest_pct_shares_out", "op": ">", "threshold": 20},
        {"condition_index": 1, "metric": "made_up", "op": ">", "threshold": 5},
        {"condition_index": 2, "metric": "sma_50_vs_sma_200", "op": "<", "threshold": None},
    ]
    survivors, reasons = sanitize_writer_break_predicates(raw, 3)
    assert [s["condition_index"] for s in survivors] == [0, 2]
    assert len(reasons) == 1 and "unknown metric" in reasons[0]


def test_metric_units_cover_full_vocabulary() -> None:
    # The writer channel derives its unit from METRIC_UNITS — every metric
    # the sanitizer can pass must have one (KeyError at scan time otherwise).
    assert set(METRIC_UNITS) == set(FRESHNESS_BOUNDS)
