"""#3609 step 2: the planning inputs behind condition 3's power check and condition 1's prior (spec premise 6).

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` premise 6 and §"Adoption rationale" (PR #3666). Run:

    PYTHONPATH=. uv run python -m scripts.plan_3609_step2_exposure

Three blocks, all read-only, none reading a month after 2021-05:

* **Stage-A exposures (development data).** For each of three family sets (the three-family book step 2 was first
  drafted with, value alone, value with GP/A) it scores stage A's pinned artefact with the book's scorer, runs the
  book's decisions and path at base cost in both termination arms, and regresses the book's monthly return in excess
  of RF on FF5 plus momentum with step 0's Newey-West rule. It prints each loading with its standard error and t, the
  residual standard deviation, R² and the holding counts. **No mean return, alpha, growth or comparison is printed.**
  Factor rows are read with an SQL bound at 2021-05-31, stage A's last holding month.
* **Condition 3's power on stage B** for the value book: the stage-A HML loading and its standard error scaled to
  stage B's 39 months, s × sqrt(80 / 39), which assumes stage B's residual and factor variances equal stage A's.
  Power is Φ(b / s − z(0.95)), at the point estimate and at its one-sided 95% lower bound b − z(0.95) × s_A.
* **Condition 1's prior:** mean monthly return and Newey-West t of published value long-short series over months
  up to 2014-09-30, before the path starts: French's HML after Fama and French (1992, published June 1992), and
  AQR's large-cap stock value series for the US, UK, Europe ex-UK and Japan (Asness, Moskowitz & Pedersen 2013).

The family sets are applied by swapping ``factor_book``'s module constants inside :func:`families`, which restores
them afterwards; the scorer reads them at call time.
"""

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from statistics import NormalDist
from typing import Any, Final

import numpy as np
import psycopg

import app.services.factor_book as factor_book
import scripts.report_3609_step2 as report
from app.config import settings
from app.services.factor_book_path import book_decisions, value_path
from app.services.factor_book_reference import load_ff12
from app.services.factor_book_series import ARMS
from scripts.build_3609_factor_panel import read_verified_artefact
from scripts.report_3609_baselines import COSTS, FACTOR_REGRESSORS, ols_newey_west
from scripts.report_3609_step2 import CONSUMED_INPUTS, STAGE_A_ARTEFACT, STAGE_A_MANIFEST_SHA256

VALUE: Final = ("be_me", "ni_me", "ocf_me")
FAMILY_SETS: Final[Mapping[str, tuple[Mapping[str, tuple[str, ...]], int]]] = {
    "three families (first draft)": ({"gp_a": ("gp_at",), "value": VALUE, "investment": ("at_gr1",)}, 2),
    "value": ({"value": VALUE}, 1),
    "value + GP/A": ({"gp_a": ("gp_at",), "value": VALUE}, 2),
}
STAGE_A_MONTHS: Final = 80
STAGE_B_MONTHS: Final = 39
PRIOR_END: Final = "2014-09-30"
#: (snapshot id, series key, first month, label): step 0's French five-factor snapshot and AQR's VME snapshot.
PRIOR_SERIES: Final = (
    (39, "HML", "1992-07-01", "French HML after Fama-French 1992"),
    (54, "VALLS_VME_US90", "1972-01-01", "AQR US large-cap value"),
    (54, "VALLS_VME_UK90", "1972-01-01", "AQR UK large-cap value"),
    (54, "VALLS_VME_ROE90", "1972-01-01", "AQR Europe ex-UK large-cap value"),
    (54, "VALLS_VME_JP90", "1972-01-01", "AQR Japan large-cap value"),
)
Z_95: Final = NormalDist().inv_cdf(0.95)


@contextmanager
def families(sets: Mapping[str, tuple[str, ...]], minimum: int) -> Iterator[None]:
    """Score with ``sets`` and ``minimum`` families, restoring the book's own constants afterwards."""
    saved = (factor_book.FAMILIES, factor_book.MIN_FAMILIES, factor_book.CHARACTERISTICS, report.CHARACTERISTICS)
    characteristics = tuple(c for members in sets.values() for c in members)
    names = ("FAMILIES", "MIN_FAMILIES", "CHARACTERISTICS")
    try:
        for name, value in zip(names, (sets, minimum, characteristics), strict=True):
            setattr(factor_book, name, value)
        report.CHARACTERISTICS = characteristics
        yield
    finally:
        for name, value in zip(names, saved[:3], strict=True):
            setattr(factor_book, name, value)
        report.CHARACTERISTICS = saved[3]


def stage_a_factors(conn: psycopg.Connection[Any]) -> dict[str, dict[tuple[int, int], float]]:
    out: dict[str, dict[tuple[int, int], float]] = {}
    rows = conn.execute(
        "SELECT series_key, observation_date, value FROM reference_data_observations "
        "WHERE snapshot_id IN (39, 40) AND observation_date BETWEEN '2014-10-01' AND '2021-05-31'"
    ).fetchall()
    for key, day, value in rows:
        out.setdefault(key, {})[(day.year, day.month)] = float(value)
    return out


def exposures(panel: Sequence[Any], factors: Mapping[str, Mapping[tuple[int, int], float]]) -> dict[str, Any]:
    """Per arm: loadings, standard errors, t, residual sd, R² and holding counts of the book at base cost."""
    scored = [report.score(month) for month in panel]
    formations = [report.formation_inputs(month, s) for month, s in zip(panel, scored, strict=True)]
    decisions = book_decisions(formations)
    out: dict[str, Any] = {"insufficient": decisions.insufficient}
    for arm in ARMS:
        path = value_path(decisions.decisions, arm=arm, cost_multiplier=COSTS["net"])
        months = sorted(path.returns)
        y = np.array([path.returns[m] - factors["RF"][m] for m in months])
        x = np.array([[factors[n][m] for n in FACTOR_REGRESSORS] for m in months])
        fit = ols_newey_west(y, x)
        residual = y - np.column_stack([np.ones(len(y)), x]) @ fit.coefficients
        dof = len(y) - x.shape[1] - 1
        holdings = sorted(path.holdings.values())
        out[arm] = {
            "months": len(y),
            "lag": fit.lag,
            "loadings": {
                n: (float(fit.coefficients[i]), float(fit.standard_errors[i]), float(fit.t_stats[i]))
                for i, n in enumerate(FACTOR_REGRESSORS, start=1)
            },
            "residual_sd": math.sqrt(float(residual @ residual) / dof),
            "r2": 1.0 - float(residual @ residual) / float(((y - y.mean()) ** 2).sum()),
            "holdings": (holdings[0], holdings[len(holdings) // 2], holdings[-1]),
        }
    return out


def power(b: float, s_a: float) -> tuple[float, float, float, float]:
    """Stage B's planned t and power at ``b``, and at its one-sided 95% lower bound from stage A's ``s_a``."""
    s_b = s_a * math.sqrt(STAGE_A_MONTHS / STAGE_B_MONTHS)
    lower = b - Z_95 * s_a
    cdf = NormalDist().cdf
    return b / s_b, cdf(b / s_b - Z_95), lower / s_b, cdf(lower / s_b - Z_95)


def prior(conn: psycopg.Connection[Any]) -> None:
    for snapshot, key, first, label in PRIOR_SERIES:
        rows = conn.execute(
            "SELECT observation_date, value FROM reference_data_observations WHERE snapshot_id = %s "
            "AND series_key = %s AND observation_date BETWEEN %s AND %s ORDER BY observation_date",
            (snapshot, key, first, PRIOR_END),
        ).fetchall()
        y = np.array([float(v) for _, v in rows])
        fit = ols_newey_west(y, np.empty((len(y), 0)))
        print(
            f"  {label}: {rows[0][0]}..{rows[-1][0]}, n={len(y)}, mean {fit.coefficients[0]:.5f}/month "
            f"({12 * fit.coefficients[0]:.4f}/yr), Newey-West t {fit.t_stats[0]:.3f} (lag {fit.lag})"
        )


def main() -> None:
    verified = read_verified_artefact(STAGE_A_ARTEFACT, STAGE_A_MANIFEST_SHA256, keep=CONSUMED_INPUTS)
    ff12 = load_ff12()
    with psycopg.connect(settings.database_url, options="-c default_transaction_read_only=on") as conn:
        factors = stage_a_factors(conn)
        print("Stage A exposures, formations 2014-09..2021-04, base cost:")
        value_hml: dict[str, tuple[float, float]] = {}
        for label, (sets, minimum) in FAMILY_SETS.items():
            # The loader reads only the scorer's characteristics, so each set reads the panel inside its own swap.
            with families(sets, minimum):
                result = exposures(report.read_panel(verified, ff12), factors)
            print(f"  {label}: INSUFFICIENT formations {len(result['insufficient'])}")
            for arm in ARMS:
                r = result[arm]
                print(
                    f"    {arm}: T={r['months']} lag={r['lag']} residual sd {r['residual_sd']:.5f} "
                    f"R2 {r['r2']:.4f} holdings min/median/max {r['holdings']}"
                )
                for n, (b, s, t) in r["loadings"].items():
                    print(f"      {n:7s} b={b: .4f} se={s:.4f} t={t: .3f}")
                if label == "value":
                    value_hml[arm] = r["loadings"]["HML"][:2]
        print(f"Condition 3, value book, HML on stage B's {STAGE_B_MONTHS} months, one-sided z(0.95) = {Z_95:.4f}:")
        for arm, (b, s) in value_hml.items():
            t, p, t_low, p_low = power(b, s)
            print(f"  {arm}: planned t {t:.3f}, power {p:.4f}; at the lower bound t {t_low:.3f}, power {p_low:.4f}")
        print(f"Condition 1, published value long-short series, months to {PRIOR_END}:")
        prior(conn)


if __name__ == "__main__":
    main()
