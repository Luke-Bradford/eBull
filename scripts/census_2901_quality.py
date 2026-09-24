"""#2901 census before any look: descriptive counts over the frozen quality-input artefact.

Spec: ``docs/proposals/ta/2026-09-24-2901-quality-arm.md`` ("Census before any look"). Reads
only the artefact (``scripts/build_2901_quality_input.py``, verified loader) -- no price, no
return. Descriptive only; it gates nothing. Runs AFTER construction freezes.

Per formation: the ladder and independent field counts; |E(D)|, |A(D)|, boundary ties,
``not_executable``, ``not_chosen``; alias-disagreement counts; joint cross-tabulations of
rung x size x outcome and of each required field x size x outcome; and the composition of
A(D) against E(D).

- **Size, two proxies (D8):** liquidity decile and assets decile over P(D), ordered by
  (value, CIK); an unavailable value is its own bucket; fewer than 10 values → no deciles.
- **Outcome, two channels, never merged**, each over (D, D + 730]:
  price (the chosen series' termination class, ``alive_730`` or ``censored_alive``) and
  Form 25 (``symbol_match`` rows by raw provision label, ``no_form25_observed`` or
  ``unobserved_horizon`` when the horizon leaves the register's span).

Usage::

    PYTHONPATH=. uv run python -m scripts.census_2901_quality \\
        --input <artefact dir> --input-sha256 <manifest sha> --out <new json file>
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any, Final

from app.services import r6_quality_universe as quality
from app.services.universe_selection import INTRADER_CAPTURE_DATE
from scripts.build_2900_pit_bundle import _write_exclusive
from scripts.build_2901_quality_input import OUTCOME_HORIZON_DAYS, load_quality_input

UNAVAILABLE: Final = "unavailable"
NO_DECILES: Final = "no_deciles"
REQUIRED_FIELDS: Final = ("period", "revenue", "cogs", "assets", "equity")
#: SIC divisions, 1987 SIC Manual (OSHA SIC Division Structure). Ranges are inclusive.
SIC_DIVISIONS: Final = (
    (100, 999, "A_agriculture"),
    (1000, 1499, "B_mining"),
    (1500, 1799, "C_construction"),
    (2000, 3999, "D_manufacturing"),
    (4000, 4999, "E_transport_utilities"),
    (5000, 5199, "F_wholesale"),
    (5200, 5999, "G_retail"),
    (6000, 6799, "H_finance"),
    (7000, 8999, "I_services"),
    (9100, 9729, "J_public_administration"),
    (9900, 9999, "K_nonclassifiable"),
)
TAIL_QUANTILES: Final = (0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0)


# --------------------------------------------------------------------------- pure pieces


def deciles(values: Mapping[str, Decimal | None]) -> dict[str, str]:
    """Decile by (value, CIK) over the available values; unavailable is its own bucket."""
    available = {cik: v for cik, v in values.items() if v is not None}
    n = len(available)
    ranked = sorted(available, key=lambda cik: (available[cik], cik))
    out = {cik: UNAVAILABLE for cik in values if values[cik] is None}
    for rank, cik in enumerate(ranked):
        out[cik] = NO_DECILES if n < 10 else str(10 * rank // n)
    return out


def price_outcome(decision: date, last_bar: date, alive_at_capture: bool, termination_class: str) -> str:
    horizon = decision + timedelta(days=OUTCOME_HORIZON_DAYS)
    if alive_at_capture and horizon > INTRADER_CAPTURE_DATE:
        return "censored_alive"
    if last_bar > horizon:
        return "alive_730"
    if alive_at_capture:
        return "censored_alive"
    return termination_class


def form25_outcome(decision: date, rows: Sequence[Mapping[str, Any]], span: Sequence[str] | None) -> str:
    matched = sorted((r for r in rows if r["match"] == "symbol_match"), key=lambda r: (r["filed_date"], r["accession"]))
    if matched:
        return f"form25:{matched[0]['rule_provision']}"
    horizon = decision + timedelta(days=OUTCOME_HORIZON_DAYS)
    observed = span is not None and date.fromisoformat(span[0]) <= decision and horizon <= date.fromisoformat(span[1])
    return "no_form25_observed" if observed else "unobserved_horizon"


def sic_division(sic: int | None) -> str:
    if sic is None:
        return "missing"
    return next((name for lo, hi, name in SIC_DIVISIONS if lo <= sic <= hi), "unassigned")


def tails(values: Sequence[Fraction | Decimal]) -> dict[str, str] | None:
    """Nearest-rank quantiles, as decimal strings (6 significant digits for display only)."""
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    return {f"q{q:g}": f"{float(ordered[min(n - 1, int(q * (n - 1) + 0.5))]):.6g}" for q in TAIL_QUANTILES}


# --------------------------------------------------------------------------- census


def formation_census(document: Mapping[str, Any], span: Sequence[str] | None) -> dict[str, Any]:
    d = date.fromisoformat(document["formation"])
    rows = document["rows"]
    liquidity = deciles({r["cik"]: None if r["liquidity"] is None else Decimal(r["liquidity"]) for r in rows})
    assets = deciles(
        {
            r["cik"]: Decimal(r["components"]["assets"]["value"])
            if r["components"].get("assets", {}).get("status") == "value"
            else None
            for r in rows
        }
    )
    fields: Counter[str] = Counter()
    joint: Counter[str] = Counter()
    per_field: Counter[str] = Counter()
    disagreement: Counter[str] = Counter()
    for r in rows:
        cik = r["cik"]
        outcomes = {
            "price": price_outcome(d, date.fromisoformat(r["last_bar"]), r["alive_at_capture"], r["termination_class"]),
            "form25": form25_outcome(d, r["form25_horizon"], span),
        }
        sizes = {"liquidity": liquidity[cik], "assets": assets[cik]}
        for key, value in r["fields"].items():
            fields[f"{key}={value}"] += 1
            if key.endswith("_aliases_disagree_at_latest") and value == "true":
                disagreement[key.removesuffix("_aliases_disagree_at_latest")] += 1
        for size, bucket in sizes.items():
            for channel, outcome in outcomes.items():
                joint[f"{r['rung']}|{size}={bucket}|{channel}={outcome}"] += 1
                for name in REQUIRED_FIELDS:
                    per_field[f"{name}={r['fields'].get(name)}|{size}={bucket}|{channel}={outcome}"] += 1
    eligible = [r for r in rows if r["rung"] == quality.ELIGIBLE]
    arm = [r for r in eligible if r["in_arm"]]
    return {
        "counts": document["counts"],
        "fields": dict(sorted(fields.items())),
        "alias_disagreement": dict(sorted(disagreement.items())),
        "joint_rung_size_outcome": dict(sorted(joint.items())),
        "joint_field_size_outcome": dict(sorted(per_field.items())),
        "composition": {name: _composition(group, liquidity, assets) for name, group in (("E", eligible), ("A", arm))},
    }


def _composition(
    rows: Sequence[Mapping[str, Any]], liquidity: Mapping[str, str], assets: Mapping[str, str]
) -> dict[str, Any]:
    return {
        "n": len(rows),
        "sic_division": dict(sorted(Counter(sic_division(r["sic"]) for r in rows).items())),
        "liquidity_decile": dict(sorted(Counter(liquidity[r["cik"]] for r in rows).items())),
        "assets_decile": dict(sorted(Counter(assets[r["cik"]] for r in rows).items())),
        "gpa_tails": tails([Fraction(int(r["gpa"][0]), int(r["gpa"][1])) for r in rows]),
        "assets_tails": tails([Decimal(r["components"]["assets"]["value"]) for r in rows]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        parser.error(f"{args.out} exists")
    manifest, documents = load_quality_input(args.input, expected_manifest_sha256=args.input_sha256)
    census = {
        "input_manifest_sha256": args.input_sha256,
        # Series-level linkage abstentions: the #3361 census, cited and not recomputed.
        "linkage_census_cited": "research/security_linkage_3361/census-2026-09-24-872d7256",
        "formations": {doc["formation"]: formation_census(doc, manifest["form25_span"]) for doc in documents},
    }
    _write_exclusive(args.out, census)
    for day, cell in census["formations"].items():
        counts = cell["counts"]
        print(day, counts["population"], counts["eligible"], counts["arm"], counts["boundary_ties"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
