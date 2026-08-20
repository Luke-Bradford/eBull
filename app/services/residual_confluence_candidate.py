"""Causal feature and trade contract for research candidate #2499.

This module deliberately contains no database loader, outcome reader, fitted
coefficient or strategy-manifest entry.  It freezes the transformation that
must be reviewed and tested before the recent outcome interval is opened.

The candidate asks whether a stock-specific daily shock, after removing market
and sector returns, has short-horizon reversal value when its completed candle,
abnormal volume, liquidity and market-volatility context are considered
together.  Five differently named price indicators are not independent
confirmation; the one interaction below is fixed before measurement.

Spec: ``docs/proposals/ta/2026-08-10-residual-confluence-preregistration.md``.
Refs #2499, parent #2469.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from statistics import median
from typing import Final, Literal

import numpy as np

from app.services.cost_model import COST_MODEL_ID
from app.services.research_comparator_snapshot import SNAPSHOT_ID
from app.services.strategy_result import CORPUS_VERSION

OLS_LOOKBACK: Final = 126
RESIDUAL_VOL_LOOKBACK: Final = 20
VOLUME_LOOKBACK: Final = 20
MARKET_VOL_SHORT_LOOKBACK: Final = 20
MARKET_VOL_LONG_LOOKBACK: Final = 252
ATR_PERIOD: Final = 14

MIN_SIGNAL_CLOSE: Final = Decimal("20")
MIN_MEDIAN_DOLLAR_VOLUME: Final = Decimal("10000000")
TARGET_ATR_MULTIPLE: Final = Decimal("1.5")
STOP_ATR_MULTIPLE: Final = Decimal("1.0")
MAX_HOLD_SESSIONS: Final = 5

MODEL_FAMILY: Final = "multinomial-logistic"
MODEL_L2_PENALTY: Final = Decimal("1.0")
MODEL_CALIBRATION: Final = "none-fail-on-poor-raw-calibration"
MODEL_OPTIMIZER: Final = "full-batch-gradient-descent-v1"
MODEL_LEARNING_RATE: Final = Decimal("0.1")
MODEL_MAX_ITERATIONS: Final = 10_000
MODEL_GRADIENT_TOLERANCE: Final = Decimal("1e-9")
MODEL_FEATURE_NAMES: Final = (
    "shock_z",
    "close_location",
    "abnormal_volume",
    "log_dollar_liquidity",
    "market_stress",
    "shock_x_location_x_volume",
)

OutcomeClass = Literal["target_first", "stop_first", "timeout"]
OUTCOME_CLASSES: Final[tuple[OutcomeClass, ...]] = ("target_first", "stop_first", "timeout")


class FeatureRefusal(ValueError):
    """The point-in-time input cannot support the frozen feature contract."""


@dataclass(frozen=True)
class CandidateDefinition:
    corpus_version: str = CORPUS_VERSION
    comparator_snapshot_id: str = SNAPSHOT_ID
    cost_model_id: str = COST_MODEL_ID
    direction: str = "long_reversal"
    ols_lookback: int = OLS_LOOKBACK
    residual_vol_lookback: int = RESIDUAL_VOL_LOOKBACK
    volume_lookback: int = VOLUME_LOOKBACK
    market_vol_short_lookback: int = MARKET_VOL_SHORT_LOOKBACK
    market_vol_long_lookback: int = MARKET_VOL_LONG_LOOKBACK
    atr_period: int = ATR_PERIOD
    min_signal_close: str = str(MIN_SIGNAL_CLOSE)
    min_median_dollar_volume: str = str(MIN_MEDIAN_DOLLAR_VOLUME)
    target_atr_multiple: str = str(TARGET_ATR_MULTIPLE)
    stop_atr_multiple: str = str(STOP_ATR_MULTIPLE)
    max_hold_sessions: int = MAX_HOLD_SESSIONS
    model_family: str = MODEL_FAMILY
    model_l2_penalty: str = str(MODEL_L2_PENALTY)
    model_calibration: str = MODEL_CALIBRATION
    model_optimizer: str = MODEL_OPTIMIZER
    model_learning_rate: str = str(MODEL_LEARNING_RATE)
    model_max_iterations: int = MODEL_MAX_ITERATIONS
    model_gradient_tolerance: str = str(MODEL_GRADIENT_TOLERANCE)
    model_features: tuple[str, ...] = MODEL_FEATURE_NAMES
    feature_standardisation: str = "training-fold-mean-and-sample-std"
    interaction_basis: str = "raw-shock_z*raw-close_location*raw-abnormal_volume"
    market_history_alignment: str = "factor-market-is-last-126-of-required-prior-252"
    liquidity_input: str = "prior-20-close*volume-computed-in-feature-engine"
    outcome_classes: tuple[str, ...] = OUTCOME_CLASSES
    primary_start: str = "2022-01-01"
    candidate_filter: str = "shock_z<0"
    trade_decision: str = "predicted-net-ev>0"
    payoff_basis: str = "after-static-spread-per-outcome"
    timeout_payoff: str = "training-fold-mean-net-timeout-return"
    same_instrument_overlap: str = "first-accepted-signal-wins-until-exit"
    walk_forward_tests: tuple[str, ...] = ("2024-01-01/2024-12-31", "2025-01-01/2025-12-31")
    terminal_holdout: str = "2026-01-01/2026-07-08"
    fold_training: str = "anchored-prior-only-outcomes-complete-before-test"


DEFINITION: Final = CandidateDefinition()


def definition_json() -> str:
    """Canonical, reviewable identity.  No measured result enters this value."""
    return json.dumps(asdict(DEFINITION), sort_keys=True, separators=(",", ":"))


def definition_hash() -> str:
    return hashlib.sha256(definition_json().encode()).hexdigest()


CANDIDATE_VERSION: Final = f"residual-confluence-v1+{definition_hash()[:12]}"


@dataclass(frozen=True)
class ResidualConfluenceFeatures:
    alpha: float
    beta_market: float
    beta_sector: float
    residual_return: float
    residual_volatility: float
    shock_z: float
    close_location: float
    abnormal_volume: float
    log_dollar_liquidity: float
    market_stress: float
    shock_x_location_x_volume: float

    @property
    def model_row(self) -> tuple[float, ...]:
        return (
            self.shock_z,
            self.close_location,
            self.abnormal_volume,
            self.log_dollar_liquidity,
            self.market_stress,
            self.shock_x_location_x_volume,
        )


@dataclass(frozen=True)
class ExitBracket:
    target_price: Decimal
    stop_price: Decimal
    max_hold_sessions: int = MAX_HOLD_SESSIONS


@dataclass(frozen=True)
class FeatureStandardisation:
    means: tuple[float, ...]
    sample_stds: tuple[float, ...]

    def __post_init__(self) -> None:
        expected = len(MODEL_FEATURE_NAMES)
        if len(self.means) != expected or len(self.sample_stds) != expected:
            raise FeatureRefusal(f"standardisation requires {expected} means and sample standard deviations")
        means = np.asarray(self.means, dtype=float)
        sample_stds = np.asarray(self.sample_stds, dtype=float)
        if not np.isfinite(means).all() or not np.isfinite(sample_stds).all():
            raise FeatureRefusal("standardisation contains a non-finite value")
        if np.any(sample_stds <= np.finfo(float).eps):
            raise FeatureRefusal("standardisation sample deviation is zero or unavailable")

    def transform(self, row: Sequence[float]) -> tuple[float, ...]:
        if len(row) != len(MODEL_FEATURE_NAMES):
            raise FeatureRefusal(f"model row requires {len(MODEL_FEATURE_NAMES)} features, got {len(row)}")
        values = np.asarray(row, dtype=float)
        if not np.isfinite(values).all():
            raise FeatureRefusal("model row contains a missing or non-finite feature")
        return tuple((values - np.asarray(self.means)) / np.asarray(self.sample_stds))


@dataclass(frozen=True)
class FittedConfluenceModel:
    standardisation: FeatureStandardisation
    # One softmax row per OUTCOME_CLASSES member; intercept is column zero.
    weights: tuple[tuple[float, ...], ...]
    iterations: int

    def probabilities(self, row: Sequence[float]) -> dict[OutcomeClass, float]:
        standardised = self.standardisation.transform(row)
        design = np.asarray((1.0, *standardised), dtype=float)
        scores = np.asarray(self.weights, dtype=float) @ design
        scores -= float(np.max(scores))
        exponentials = np.exp(scores)
        probabilities = exponentials / float(np.sum(exponentials))
        return {label: float(probabilities[index]) for index, label in enumerate(OUTCOME_CLASSES)}


def _finite_vector(name: str, values: Sequence[float], *, exact: int) -> np.ndarray:
    if len(values) != exact:
        raise FeatureRefusal(f"{name} requires exactly {exact} observations, got {len(values)}")
    array = np.asarray(values, dtype=float)
    if array.shape != (exact,) or not np.isfinite(array).all():
        raise FeatureRefusal(f"{name} contains a missing or non-finite observation")
    return array


def _finite_scalar(name: str, value: float) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise FeatureRefusal(f"{name} is non-finite")
    return result


def compute_features(
    *,
    prior_instrument_returns: Sequence[float],
    prior_market_returns: Sequence[float],
    prior_sector_returns: Sequence[float],
    prior_closes: Sequence[float],
    prior_volumes: Sequence[float],
    signal_instrument_return: float,
    signal_market_return: float,
    signal_sector_return: float,
    signal_open: float,
    signal_high: float,
    signal_low: float,
    signal_close: float,
    signal_volume: float,
) -> ResidualConfluenceFeatures:
    """Compute the completed signal-bar snapshot using prior history only.

    The two-factor OLS and every trailing scale end at ``t-1``.  Only the
    explicitly named signal values come from completed bar ``t``.  In
    particular, neither a next-session open nor an outcome price is accepted by
    this function, making that common leakage route impossible at the API.
    """
    instrument = _finite_vector("prior_instrument_returns", prior_instrument_returns, exact=OLS_LOOKBACK)
    market_history = _finite_vector("prior_market_returns", prior_market_returns, exact=MARKET_VOL_LONG_LOOKBACK)
    market = market_history[-OLS_LOOKBACK:]
    sector = _finite_vector("prior_sector_returns", prior_sector_returns, exact=OLS_LOOKBACK)
    closes = _finite_vector("prior_closes", prior_closes, exact=VOLUME_LOOKBACK)
    volumes = _finite_vector("prior_volumes", prior_volumes, exact=VOLUME_LOOKBACK)

    if np.any(closes <= 0) or np.any(volumes <= 0):
        raise FeatureRefusal("prior close and volume observations must be positive")
    dollar_volumes = closes * volumes

    design = np.column_stack((np.ones(OLS_LOOKBACK), market, sector))
    if int(np.linalg.matrix_rank(design)) != design.shape[1]:
        raise FeatureRefusal("market and sector OLS design is rank deficient")
    coefficients, *_ = np.linalg.lstsq(design, instrument, rcond=None)
    alpha, beta_market, beta_sector = (float(item) for item in coefficients)
    residual_history = instrument - design @ coefficients
    residual_volatility = float(np.std(residual_history[-RESIDUAL_VOL_LOOKBACK:], ddof=1))
    # An exact factor combination leaves only floating-point solver residue
    # (~1e-18 in the fixture), not an economically meaningful volatility
    # scale.  Machine epsilon is a numerical zero guard, not a fitted strategy
    # threshold.
    if not math.isfinite(residual_volatility) or residual_volatility <= np.finfo(float).eps:
        raise FeatureRefusal("trailing residual volatility is unavailable or non-positive")

    current_instrument = _finite_scalar("signal_instrument_return", signal_instrument_return)
    current_market = _finite_scalar("signal_market_return", signal_market_return)
    current_sector = _finite_scalar("signal_sector_return", signal_sector_return)
    residual_return = current_instrument - alpha - beta_market * current_market - beta_sector * current_sector
    shock_z = residual_return / residual_volatility

    open_price = _finite_scalar("signal_open", signal_open)
    high = _finite_scalar("signal_high", signal_high)
    low = _finite_scalar("signal_low", signal_low)
    signal_close_decimal = Decimal(str(signal_close))
    if not signal_close_decimal.is_finite():
        raise FeatureRefusal("signal_close is non-finite")
    close = _finite_scalar("signal_close", signal_close)
    volume = _finite_scalar("signal_volume", signal_volume)
    if min(open_price, high, low, close, volume) <= 0:
        raise FeatureRefusal("signal OHLCV values must be positive")
    if high < max(open_price, close) or low > min(open_price, close) or high <= low:
        raise FeatureRefusal("signal bar has an invalid or zero-range OHLC envelope")
    if signal_close_decimal < MIN_SIGNAL_CLOSE:
        raise FeatureRefusal(f"signal close is below the frozen {MIN_SIGNAL_CLOSE} USD floor")

    median_volume = float(median(float(item) for item in volumes))
    median_dollar_volume = float(median(float(item) for item in dollar_volumes))
    if Decimal(str(median_dollar_volume)) < MIN_MEDIAN_DOLLAR_VOLUME:
        raise FeatureRefusal("trailing median dollar volume is below the frozen liquidity floor")
    abnormal_volume = math.log(volume / median_volume)
    log_dollar_liquidity = math.log(median_dollar_volume)
    close_location = (2.0 * close - high - low) / (high - low)

    short_market_vol = float(np.std(market_history[-MARKET_VOL_SHORT_LOOKBACK:], ddof=1))
    long_market_vol = float(np.std(market_history, ddof=1))
    if not math.isfinite(short_market_vol) or not math.isfinite(long_market_vol) or long_market_vol <= 0:
        raise FeatureRefusal("market-stress volatility scale is unavailable or non-positive")
    market_stress = short_market_vol / long_market_vol

    result = ResidualConfluenceFeatures(
        alpha=alpha,
        beta_market=beta_market,
        beta_sector=beta_sector,
        residual_return=residual_return,
        residual_volatility=residual_volatility,
        shock_z=shock_z,
        close_location=close_location,
        abnormal_volume=abnormal_volume,
        log_dollar_liquidity=log_dollar_liquidity,
        market_stress=market_stress,
        shock_x_location_x_volume=shock_z * close_location * abnormal_volume,
    )
    if not all(math.isfinite(item) for item in result.model_row):
        raise FeatureRefusal("computed model feature is non-finite")
    return result


def build_exit_bracket(*, entry_price: Decimal, signal_atr: Decimal) -> ExitBracket:
    """Fix the candidate's long bracket from ATR known on signal bar ``t``."""
    if not entry_price.is_finite() or entry_price <= 0:
        raise FeatureRefusal("entry price must be positive and finite")
    if not signal_atr.is_finite() or signal_atr <= 0:
        raise FeatureRefusal("signal ATR must be positive and finite")
    target = entry_price + TARGET_ATR_MULTIPLE * signal_atr
    stop = entry_price - STOP_ATR_MULTIPLE * signal_atr
    if stop <= 0:
        raise FeatureRefusal("ATR stop is non-positive and cannot be broker-orderable")
    return ExitBracket(target_price=target, stop_price=stop)


def _training_matrix(rows: Sequence[Sequence[float]]) -> tuple[np.ndarray, FeatureStandardisation]:
    matrix = np.asarray(rows, dtype=float)
    expected_columns = len(MODEL_FEATURE_NAMES)
    if matrix.ndim != 2 or matrix.shape[1:] != (expected_columns,):
        raise FeatureRefusal(f"training matrix requires shape (n, {expected_columns})")
    if matrix.shape[0] < 2 or not np.isfinite(matrix).all():
        raise FeatureRefusal("training matrix needs at least two finite rows")
    means = np.mean(matrix, axis=0)
    sample_stds = np.std(matrix, axis=0, ddof=1)
    if not np.isfinite(sample_stds).all() or np.any(sample_stds <= np.finfo(float).eps):
        raise FeatureRefusal("training feature has zero or unavailable sample variation")
    standardisation = FeatureStandardisation(
        tuple(float(item) for item in means), tuple(float(item) for item in sample_stds)
    )
    transformed = (matrix - means) / sample_stds
    return np.column_stack((np.ones(matrix.shape[0]), transformed)), standardisation


def fit_model(
    rows: Sequence[Sequence[float]],
    labels: Sequence[OutcomeClass],
) -> FittedConfluenceModel:
    """Fit the frozen deterministic softmax model on one training fold only.

    The objective is mean multiclass cross-entropy plus
    ``0.5 * L2 * sum(non_intercept_weight**2)``.  Standardisation is learned
    from these rows and travels with the fitted model; callers cannot supply a
    full-period scale that would leak validation or holdout observations.
    """
    design, standardisation = _training_matrix(rows)
    if len(labels) != design.shape[0]:
        raise FeatureRefusal(f"training labels count {len(labels)} does not match row count {design.shape[0]}")
    unknown = set(labels) - set(OUTCOME_CLASSES)
    if unknown:
        raise FeatureRefusal(f"unknown outcome class(es): {sorted(unknown)}")
    missing = set(OUTCOME_CLASSES) - set(labels)
    if missing:
        raise FeatureRefusal(f"training fold is missing outcome class(es): {sorted(missing)}")

    class_index = {label: index for index, label in enumerate(OUTCOME_CLASSES)}
    target = np.zeros((design.shape[0], len(OUTCOME_CLASSES)), dtype=float)
    target[np.arange(design.shape[0]), [class_index[label] for label in labels]] = 1.0
    weights = np.zeros((len(OUTCOME_CLASSES), design.shape[1]), dtype=float)
    learning_rate = float(MODEL_LEARNING_RATE)
    penalty = float(MODEL_L2_PENALTY)
    tolerance = float(MODEL_GRADIENT_TOLERANCE)

    for iteration in range(1, MODEL_MAX_ITERATIONS + 1):
        scores = design @ weights.T
        scores -= np.max(scores, axis=1, keepdims=True)
        exponentials = np.exp(scores)
        probabilities = exponentials / np.sum(exponentials, axis=1, keepdims=True)
        gradient = (probabilities - target).T @ design / design.shape[0]
        gradient[:, 1:] += penalty * weights[:, 1:]
        weights -= learning_rate * gradient
        if float(np.max(np.abs(gradient))) <= tolerance:
            return FittedConfluenceModel(
                standardisation=standardisation,
                weights=tuple(tuple(float(value) for value in row) for row in weights),
                iterations=iteration,
            )
    raise FeatureRefusal(f"multinomial model did not converge in {MODEL_MAX_ITERATIONS} frozen iterations")


def expected_net_value_pct(
    probabilities: dict[OutcomeClass, float],
    *,
    target_payoff_pct: float,
    stop_loss_pct: float,
    expected_timeout_payoff_pct: float,
    total_cost_pct: float,
) -> float:
    """Apply the declared probability-to-trade equation, without a threshold."""
    if set(probabilities) != set(OUTCOME_CLASSES):
        raise FeatureRefusal("net EV requires exactly the three declared outcome probabilities")
    values = tuple(probabilities[label] for label in OUTCOME_CLASSES)
    if any(not math.isfinite(item) or item < 0 or item > 1 for item in values) or not math.isclose(
        sum(values), 1.0, rel_tol=0, abs_tol=1e-9
    ):
        raise FeatureRefusal("outcome probabilities must be finite, bounded and sum to one")
    payoffs = (target_payoff_pct, stop_loss_pct, expected_timeout_payoff_pct, total_cost_pct)
    if any(not math.isfinite(float(item)) for item in payoffs) or stop_loss_pct < 0 or total_cost_pct < 0:
        raise FeatureRefusal("net EV payoffs/costs are invalid")
    return (
        probabilities["target_first"] * target_payoff_pct
        - probabilities["stop_first"] * stop_loss_pct
        + probabilities["timeout"] * expected_timeout_payoff_pct
        - total_cost_pct
    )


def expected_net_value_from_net_payoffs_pct(
    probabilities: dict[OutcomeClass, float],
    *,
    target_net_payoff_pct: float,
    stop_net_payoff_pct: float,
    mean_timeout_net_payoff_pct: float,
) -> float:
    """Expected return when each class payoff already includes both spreads.

    The target and stop payoffs come from the signal's fixed bracket and entry
    price.  The timeout payoff is the training fold's mean net return among
    timeout-labelled rows.  Carry and FX remain unavailable and therefore
    remain promotion refusals rather than being silently treated as zero.
    """
    if set(probabilities) != set(OUTCOME_CLASSES):
        raise FeatureRefusal("net EV requires exactly the three declared outcome probabilities")
    values = tuple(probabilities[label] for label in OUTCOME_CLASSES)
    if any(not math.isfinite(item) or item < 0 or item > 1 for item in values) or not math.isclose(
        sum(values), 1.0, rel_tol=0, abs_tol=1e-9
    ):
        raise FeatureRefusal("outcome probabilities must be finite, bounded and sum to one")
    payoffs = (target_net_payoff_pct, stop_net_payoff_pct, mean_timeout_net_payoff_pct)
    if any(not math.isfinite(float(item)) for item in payoffs):
        raise FeatureRefusal("net outcome payoff is non-finite")
    if target_net_payoff_pct <= 0 or stop_net_payoff_pct >= 0:
        raise FeatureRefusal("target net payoff must be positive and stop net payoff negative")
    return sum(probability * payoff for probability, payoff in zip(values, payoffs, strict=True))


__all__ = [
    "ATR_PERIOD",
    "CANDIDATE_VERSION",
    "DEFINITION",
    "FeatureRefusal",
    "FittedConfluenceModel",
    "MODEL_FEATURE_NAMES",
    "ResidualConfluenceFeatures",
    "build_exit_bracket",
    "compute_features",
    "definition_hash",
    "definition_json",
    "expected_net_value_pct",
    "expected_net_value_from_net_payoffs_pct",
    "fit_model",
]
