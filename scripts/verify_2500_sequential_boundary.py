"""Empirical consistency + power check for #2500's declared repeated-look boundary.

Runs against the SHIPPED module (``app.services.strategy_decay_sequential_test``), not a
copy of its arithmetic -- a verification script that reimplements the rule verifies nothing
about the rule that ships.

⚠ What this can and cannot establish.  The theorem bounds the INFINITE-horizon crossing
probability for every sub-psi process; a finite simulation of two particular processes can
only fail to contradict it.  It is a consistency check, and the power table is the number
that actually decides whether the monitor is worth having.

    PYTHONPATH=. uv run python scripts/verify_2500_sequential_boundary.py
"""

from __future__ import annotations

import json
import math
import sys

import numpy as np

from app.services.strategy_decay_sequential_test import (
    hoeffding_variance_proxy,
    mixture_rho,
    mixture_tuning_k,
    uniform_boundary,
)

SEED = 20260916
ALPHA = 0.05
TARGET_TRADES = 200
HORIZON = 2_000
COVERAGE_PATHS = 200_000
POWER_PATHS = 40_000
CHUNK = 20_000
#: Observations on [-1, 1], so Hoeffding's proxy is exactly 1 and intrinsic time is the
#: trade count.  Chosen to make the units transparent, NOT as a production setting.
LOWER, UPPER = -1.0, 1.0


def _boundary_curve(rho: float, proxy: float) -> np.ndarray:
    return np.array(
        [uniform_boundary(step * proxy, rho=rho, alpha=ALPHA) for step in range(1, HORIZON + 1)],
        dtype=np.float64,
    )


def _crossing_rate(rng: np.random.Generator, curve: np.ndarray, *, gaussian: bool) -> float:
    crossed = np.zeros(COVERAGE_PATHS, dtype=bool)
    for start in range(0, COVERAGE_PATHS, CHUNK):
        n = min(CHUNK, COVERAGE_PATHS - start)
        if gaussian:
            draws = rng.standard_normal((n, HORIZON))
        else:
            draws = (rng.integers(0, 2, size=(n, HORIZON)) * 2 - 1).astype(np.float64)
        crossed[start : start + n] = (np.abs(np.cumsum(draws, axis=1)) >= curve).any(axis=1)
    return float(crossed.mean())


def _power(rng: np.random.Generator, curve: np.ndarray, shift: float) -> tuple[float, int | None, float]:
    """Trades to a one-sided LOWER crossing under a same-support alternative.

    ⚠ The alternative is a BIASED SIGN, ``P(+1) = (1 - shift) / 2``, whose mean is ``-shift``
    and whose support is still ``[-1, 1]``.  A shifted Rademacher would leave the declared
    bounds, and the production rule would refuse it rather than judge it -- so measuring
    power that way would measure a configuration the monitor never accepts.
    """
    firsts: list[np.ndarray] = []
    for start in range(0, POWER_PATHS, CHUNK):
        n = min(CHUNK, POWER_PATHS - start)
        draws = np.where(rng.random((n, HORIZON)) < (1.0 - shift) / 2.0, 1.0, -1.0)
        below = np.cumsum(draws, axis=1) <= -curve
        firsts.append(np.where(below.any(axis=1), below.argmax(axis=1) + 1, HORIZON + 1))
    first = np.concatenate(firsts)
    detected = first <= HORIZON
    rate = float(detected.mean())
    stderr = math.sqrt(max(rate * (1.0 - rate), 0.0) / POWER_PATHS)
    # ⚠ MEDIAN OVER ALL PATHS, censored ones included at their HORIZON+1 sentinel -- NOT
    # `median(first[detected])`.  Conditioning on detection understates latency whenever some
    # paths never detect: at a -0.10 shift, 94% detect and the conditional median is ~692,
    # by which point only ~47% of ALL paths have detected.  Caught by Codex checkpoint 2.
    censored_median = float(np.median(first))
    median = int(censored_median) if censored_median <= HORIZON else None
    return rate, median, stderr


def main() -> int:
    proxy = hoeffding_variance_proxy(lower_bound=LOWER, upper_bound=UPPER)
    rho = mixture_rho(target_intrinsic_time=TARGET_TRADES * proxy, alpha=ALPHA)
    report: dict[str, object] = {
        "seed": SEED,
        "alpha": ALPHA,
        "target_trades": TARGET_TRADES,
        "variance_proxy": proxy,
        "rho": rho,
        "horizon": HORIZON,
        "coverage_paths": COVERAGE_PATHS,
        "power_paths": POWER_PATHS,
    }

    # The tuning identity, in RELATIVE terms.  An absolute tolerance accepts useless roots at
    # small alpha, where alpha^2 is far below the comparison scale.
    identity = {}
    for alpha in (0.01, 0.05, 0.10):
        k = mixture_tuning_k(alpha)
        identity[str(alpha)] = abs((math.log1p(k) - k) / (2.0 * math.log(alpha)) - 1.0)
    report["tuning_identity_max_relative_error"] = max(identity.values())

    rng = np.random.default_rng(SEED)
    curve = _boundary_curve(rho, proxy)
    report["crossing_rate_rademacher"] = _crossing_rate(rng, curve, gaussian=False)
    report["crossing_rate_gaussian"] = _crossing_rate(rng, curve, gaussian=True)

    power = []
    for shift in (0.05, 0.10, 0.20, 0.40):
        rate, median, stderr = _power(rng, curve, shift)
        power.append({"mean_shift": -shift, "detected": rate, "stderr": stderr, "median_trades": median})
    report["power"] = power

    print(json.dumps(report, indent=2, sort_keys=True))
    coverage_ok = (
        float(report["crossing_rate_rademacher"]) <= ALPHA and float(report["crossing_rate_gaussian"]) <= ALPHA
    )
    print("COVERAGE CONSISTENT" if coverage_ok else "COVERAGE CONTRADICTED", file=sys.stderr)
    return 0 if coverage_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
