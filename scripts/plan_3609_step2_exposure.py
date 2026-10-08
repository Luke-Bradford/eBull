"""#3609 step 2: the planning inputs behind condition 3's power check and condition 1's prior (spec premise 6).

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` premise 6 and §"Adoption rationale" (PR #3666). Run:

    PYTHONPATH=. uv run python -m scripts.plan_3609_step2_exposure [--out PATH]

It prints the plan and writes it as canonical JSON (default ``PLAN_PATH``, committed): the script's own sha256, stage
A's manifest pin, a digest of every factor and prior observation it read, and every figure printed. The trial
register's ``3609-step2-exposure-planning-2026-10-08`` row pins that file's sha256. Three blocks, all read-only, none
reading a month after 2021-05:

* **Stage-A exposures (development data).** For each of three family sets (the three-family book step 2 was first
  drafted with, value alone, value with GP/A) it scores stage A's pinned artefact with the book's scorer, runs the
  book's decisions and path at base cost in both termination arms, and regresses the book's monthly return in excess
  of RF on FF5 plus momentum with step 0's Newey-West rule. It prints each loading with its standard error and t, the
  residual standard deviation, R² and the holding counts. The regression reads the book's returns; **no mean return,
  alpha, growth or comparison is printed**, and none is a selection criterion. Factor rows are read with an SQL bound
  at 2021-05-31, stage A's last holding month. It refuses unless every factor month of stage A is present exactly
  once, the path has exactly stage A's 80 holding months, no formation is ``INSUFFICIENT`` and every standard error is
  finite and positive.
* **Condition 3's planned power on stage B** for the value book (approximate; spec premise 6 lists its assumptions):
  stage A's HML standard error scaled to stage B's 39 months, s × sqrt(T_A / 39), and per arm Φ(b / s_B − z(0.95)).
  The declared criterion is the distribution-free conjunction bound over the two arms, 1 − Σ (1 − power), at the
  point estimates, which must be at least 0.80. Also printed: the same at the unadjusted one-sided 95% lower bound
  b − z(0.95) × s_A (a sensitivity, not selection-adjusted), and the smallest planned t at which the criterion still
  holds.
* **Condition 1's prior:** mean monthly return and Newey-West t of published value long-short series over months
  up to 2014-09-30, before the path starts: French's HML after Fama and French (1992, published June 1992), and
  AQR's large-cap stock value series for the US, UK, Europe ex-UK and Japan (Asness, Moskowitz & Pedersen 2013).

The family sets are applied by swapping ``factor_book``'s module constants inside :func:`families`, which restores
them afterwards; the scorer reads them at call time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from statistics import NormalDist
from typing import Any, Final

import numpy as np
import psycopg

import app.services.factor_book as factor_book
import scripts.report_3609_step2 as report
from app.config import settings
from app.services.factor_book_declaration import construction_sha256
from app.services.factor_book_path import Month, book_decisions, next_month, value_path
from app.services.factor_book_reference import SICCODES12_SHA256, load_ff12
from app.services.factor_book_series import ARMS
from scripts.build_3609_factor_panel import read_verified_artefact
from scripts.report_3609_baselines import COSTS, FACTOR_REGRESSORS, FACTOR_UNIT, ols_newey_west
from scripts.report_3609_step2 import CONSUMED_INPUTS, STAGE_A_ARTEFACT, STAGE_A_MANIFEST_SHA256

REPO: Final = Path(__file__).resolve().parents[1]
PLAN_PATH: Final = REPO / "docs" / "research" / "3609-step2-exposure-plan.json"
VALUE: Final = ("be_me", "ni_me", "ocf_me")
FAMILY_SETS: Final[Mapping[str, tuple[Mapping[str, tuple[str, ...]], int]]] = {
    "three families (first draft)": ({"gp_a": ("gp_at",), "value": VALUE, "investment": ("at_gr1",)}, 2),
    "value": ({"value": VALUE}, 1),
    "value + GP/A": ({"gp_a": ("gp_at",), "value": VALUE}, 2),
}
STAGE_A_FIRST: Final[Month] = (2014, 10)
STAGE_A_LAST: Final[Month] = (2021, 5)
STAGE_B_MONTHS: Final = 39
#: The declared criterion: the two-arm conjunction bound at the planning point estimates.
TARGET_POWER: Final = 0.80
PRIOR_END: Final = "2014-09-30"
PRIOR_LAST: Final[Month] = (2014, 9)
#: (snapshot id, series key, first month, label): step 0's French five-factor snapshot and AQR's VME snapshot.
PRIOR_SERIES: Final = (
    (39, "HML", "1992-07-01", "French HML after Fama-French 1992"),
    (54, "VALLS_VME_US90", "1972-01-01", "AQR US large-cap value"),
    (54, "VALLS_VME_UK90", "1972-01-01", "AQR UK large-cap value"),
    (54, "VALLS_VME_ROE90", "1972-01-01", "AQR Europe ex-UK large-cap value"),
    (54, "VALLS_VME_JP90", "1972-01-01", "AQR Japan large-cap value"),
)
Z_95: Final = NormalDist().inv_cdf(0.95)


class PlanError(RuntimeError):
    """An input the plan cannot be computed from."""


def stage_a_months() -> tuple[Month, ...]:
    out: list[Month] = [STAGE_A_FIRST]
    while out[-1] < STAGE_A_LAST:
        out.append(next_month(out[-1]))
    return tuple(out)


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


def digest(rows: Sequence[Sequence[object]]) -> str:
    """sha256 of the rows' canonical JSON, each ``[snapshot, series, date, value, unit]`` as strings, sorted."""
    canonical = sorted([str(field) for field in row] for row in rows)
    return hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()


def stage_a_factors(conn: psycopg.Connection[Any]) -> tuple[dict[str, dict[Month, float]], str]:
    """Every FF5, RF and momentum month of stage A exactly once, and the rows' digest."""
    rows = conn.execute(
        "SELECT snapshot_id, series_key, observation_date, value, unit FROM reference_data_observations "
        "WHERE snapshot_id IN (39, 40) AND observation_date BETWEEN '2014-10-01' AND '2021-05-31'"
    ).fetchall()
    out: dict[str, dict[Month, float]] = {}
    for _, key, day, value, unit in rows:
        if unit != FACTOR_UNIT:
            raise PlanError(f"factor {key} {day} is in {unit!r}, not {FACTOR_UNIT}")
        month = (day.year, day.month)
        if month in out.setdefault(key, {}):
            raise PlanError(f"factor {key} repeats {month}")
        out[key][month] = float(value)
    expected = set(stage_a_months())
    for key in (*FACTOR_REGRESSORS, "RF"):
        if set(out.get(key, {})) != expected:
            raise PlanError(f"factor {key} does not cover stage A's {len(expected)} months exactly")
        if not all(math.isfinite(v) for v in out[key].values()):
            raise PlanError(f"factor {key} has a non-finite value")
    return out, digest(rows)


def exposures(panel: Sequence[Any], factors: Mapping[str, Mapping[Month, float]]) -> dict[str, Any]:
    """Per arm: loadings with standard errors and t, residual sd, R² and holding counts of the book at base cost."""
    scored = [report.score(month) for month in panel]
    formations = [report.formation_inputs(month, s) for month, s in zip(panel, scored, strict=True)]
    decisions = book_decisions(formations)
    if decisions.insufficient:
        raise PlanError(f"{len(decisions.insufficient)} INSUFFICIENT formations, first {decisions.insufficient[0]}")
    out: dict[str, Any] = {}
    for arm in ARMS:
        path = value_path(decisions.decisions, arm=arm, cost_multiplier=COSTS["net"])
        months = sorted(path.returns)
        if tuple(months) != stage_a_months():
            raise PlanError(f"{arm}: the path's months are not stage A's {len(stage_a_months())}")
        y = np.array([path.returns[m] - factors["RF"][m] for m in months])
        x = np.array([[factors[n][m] for n in FACTOR_REGRESSORS] for m in months])
        if not np.isfinite(y).all():
            raise PlanError(f"{arm}: a book return is not finite")
        fit = ols_newey_west(y, x)
        if not all(math.isfinite(e) and e > 0 for e in fit.standard_errors):
            raise PlanError(f"{arm}: a Newey-West standard error is not finite and positive")
        residual = y - np.column_stack([np.ones(len(y)), x]) @ fit.coefficients
        holdings = sorted(path.holdings.values())
        out[arm] = {
            "months": len(y),
            "lag": fit.lag,
            "loadings": {
                n: [float(fit.coefficients[i]), float(fit.standard_errors[i]), float(fit.t_stats[i])]
                for i, n in enumerate(FACTOR_REGRESSORS, start=1)
            },
            "residual_sd": math.sqrt(float(residual @ residual) / (len(y) - x.shape[1] - 1)),
            "r2": 1.0 - float(residual @ residual) / float(((y - y.mean()) ** 2).sum()),
            "holdings_min_median_max": [holdings[0], holdings[len(holdings) // 2], holdings[-1]],
        }
    return out


def arm_power(b: float, s_a: float, months_a: int) -> dict[str, float]:
    """One arm's planned stage-B t and power at ``b``, and at its unadjusted one-sided 95% lower bound."""
    s_b = s_a * math.sqrt(months_a / STAGE_B_MONTHS)
    lower = b - Z_95 * s_a
    cdf = NormalDist().cdf
    return {
        "t": b / s_b,
        "power": cdf(b / s_b - Z_95),
        "t_at_lower_bound": lower / s_b,
        "power_at_lower_bound": cdf(lower / s_b - Z_95),
    }


def condition_3(value: Mapping[str, Any]) -> dict[str, Any]:
    """Per-arm power, the conjunction bounds and the break-even planned t; refuses if the criterion fails."""
    arms = {
        arm: arm_power(value[arm]["loadings"]["HML"][0], value[arm]["loadings"]["HML"][1], value[arm]["months"])
        for arm in ARMS
    }
    joint = 1.0 - sum(1.0 - a["power"] for a in arms.values())
    joint_lower = 1.0 - sum(1.0 - a["power_at_lower_bound"] for a in arms.values())
    # With equal arms, the bound is TARGET_POWER when each arm's power is 1 - (1 - TARGET_POWER) / 2.
    break_even = Z_95 + NormalDist().inv_cdf(1.0 - (1.0 - TARGET_POWER) / len(arms))
    out = {
        "critical_t": Z_95,
        "arms": arms,
        "conjunction_bound": joint,
        "conjunction_bound_at_lower_bounds": joint_lower,
        "break_even_t": break_even,
        "target": TARGET_POWER,
    }
    if not joint >= TARGET_POWER:
        raise PlanError(f"condition 3's conjunction bound {joint:.4f} is below {TARGET_POWER}: not powered")
    return out


def prior(conn: psycopg.Connection[Any]) -> tuple[list[dict[str, Any]], str]:
    out: list[dict[str, Any]] = []
    read: list[Sequence[object]] = []
    for snapshot, key, first, label in PRIOR_SERIES:
        rows = conn.execute(
            "SELECT observation_date, value, unit FROM reference_data_observations WHERE snapshot_id = %s "
            "AND series_key = %s AND observation_date BETWEEN %s AND %s ORDER BY observation_date",
            (snapshot, key, first, PRIOR_END),
        ).fetchall()
        days = [d for d, _, _ in rows]
        months = [(d.year, d.month) for d in days]
        # One observation per calendar month, consecutive, ending at the last month before the path.
        if not months or months[-1] != PRIOR_LAST or any(next_month(a) != b for a, b in zip(months, months[1:])):
            raise PlanError(f"{label} is not one observation a month, consecutive, through {PRIOR_LAST}")
        if any(unit != FACTOR_UNIT for _, _, unit in rows):
            raise PlanError(f"{label} has a unit other than {FACTOR_UNIT}")
        read += [(snapshot, key, d, v, u) for d, v, u in rows]
        y = np.array([float(v) for _, v, _ in rows])
        if not np.isfinite(y).all():
            raise PlanError(f"{label} has a non-finite value")
        fit = ols_newey_west(y, np.empty((len(y), 0)))
        out.append(
            {
                "series": label,
                "first": str(days[0]),
                "last": str(days[-1]),
                "months": len(y),
                "mean_per_month": float(fit.coefficients[0]),
                "newey_west_t": float(fit.t_stats[0]),
                "lag": fit.lag,
            }
        )
    return out, digest(read)


def show(plan: Mapping[str, Any]) -> None:
    print("Stage A exposures, formations 2014-09..2021-04, base cost:")
    for label, result in plan["stage_a"].items():
        print(f"  {label}:")
        for arm in ARMS:
            r = result[arm]
            print(
                f"    {arm}: T={r['months']} lag={r['lag']} residual sd {r['residual_sd']:.5f} R2 {r['r2']:.4f} "
                f"holdings min/median/max {r['holdings_min_median_max']}"
            )
            for n, (b, s, t) in r["loadings"].items():
                print(f"      {n:7s} b={b: .4f} se={s:.4f} t={t: .3f}")
    c3 = plan["condition_3"]
    print(
        f"Condition 3, value book, HML on stage B's {STAGE_B_MONTHS} months, "
        f"one-sided z(0.95) = {c3['critical_t']:.4f}:"
    )
    for arm, a in c3["arms"].items():
        print(
            f"  {arm}: planned t {a['t']:.3f}, power {a['power']:.4f}; at the unadjusted lower bound "
            f"t {a['t_at_lower_bound']:.3f}, power {a['power_at_lower_bound']:.4f}"
        )
    print(
        f"  conjunction bound {c3['conjunction_bound']:.4f} (criterion >= {c3['target']}); at both lower bounds "
        f"{c3['conjunction_bound_at_lower_bounds']:.4f}; criterion holds while each arm's planned t >= "
        f"{c3['break_even_t']:.3f}"
    )
    print(f"Condition 1, published value long-short series, months to {PRIOR_END}:")
    for p in plan["prior"]:
        print(
            f"  {p['series']}: {p['first']}..{p['last']}, n={p['months']}, mean {p['mean_per_month']:.5f}/month "
            f"({12 * p['mean_per_month']:.4f}/yr), Newey-West t {p['newey_west_t']:.3f} (lag {p['lag']})"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="#3609 step 2 condition-3 power plan and condition-1 prior")
    parser.add_argument("--out", type=Path, default=PLAN_PATH)
    args = parser.parse_args()
    verified = read_verified_artefact(STAGE_A_ARTEFACT, STAGE_A_MANIFEST_SHA256, keep=CONSUMED_INPUTS)
    ff12 = load_ff12()
    with psycopg.connect(settings.database_url, options="-c default_transaction_read_only=on") as conn:
        factors, factor_digest = stage_a_factors(conn)
        stage_a: dict[str, Any] = {}
        for label, (sets, minimum) in FAMILY_SETS.items():
            # The loader reads only the scorer's characteristics, so each set reads the panel inside its own swap.
            with families(sets, minimum):
                stage_a[label] = exposures(report.read_panel(verified, ff12), factors)
        prior_rows, prior_digest = prior(conn)
    plan = {
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        # The script's import closure (scorer, path, regression, loader, builder), as the construction hash takes it.
        "code_closure_sha256": construction_sha256([Path(__file__).resolve()], REPO),
        "siccodes12_sha256": SICCODES12_SHA256,
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "numpy": np.__version__,
        "stage_a_manifest_sha256": STAGE_A_MANIFEST_SHA256,
        "factor_rows_sha256": factor_digest,
        "prior_rows_sha256": prior_digest,
        "stage_a": stage_a,
        "condition_3": condition_3(stage_a["value"]),
        "prior": prior_rows,
    }
    show(plan)
    document = json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    args.out.write_bytes(document)
    print(f"wrote {args.out} sha256 {hashlib.sha256(document).hexdigest()}")


if __name__ == "__main__":
    main()
