"""#3609 step 1 slices 3d and 3d-ii: full-population A/B of the Amendment 2 share checks and 2.1's references.

Compares two stage-A row files built from the SAME frozen inputs (arm A: before the change, arm B: after). Spec
§"Slices" 3d and 3d-ii: every row whose ME is unchanged is identical (the keys ``me.raw``, ``me.checks``,
``me.verified`` and ``prices.liquidity_screened`` aside); every changed row either lost its ME to exactly one
Amendment 2 reason (its admission and characteristics following; a row check 2 recovered may still fail another
check), was DQC-recovered (only ME and the ME-denominated characteristics change), or had its Amendment 2 ME-missing
reason lifted because its reference changed (3d-ii). Anything else is a refusal and exits 1.

    PYTHONPATH=. uv run python -m scripts.ab_3609_share_checks --a <rows A> --b <rows B> --out <changes.json> \
        [--csv <evidence.csv>]
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from app.services.factor_panel import SCALE_TOLERANCE, MeMissing
from app.services.factor_panel_artefact import read_gz_lines

AMENDMENT_2: frozenset[str] = frozenset(
    {
        MeMissing.SHARES_BASIS_AMBIGUOUS,
        MeMissing.SHARES_SCALE_CONFLICT,
        MeMissing.SHARES_TURNOVER_IMPLAUSIBLE,
        MeMissing.SHARES_DISCONTINUITY,
    }
)
NEW_ME_KEYS: tuple[str, ...] = ("raw", "checks", "verified")
ME_DENOMINATED: frozenset[str] = frozenset({"be_me", "ni_me", "ocf_me"})


def _stripped(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    if isinstance(out.get("me"), dict):
        out["me"] = {k: v for k, v in out["me"].items() if k not in NEW_ME_KEYS}
    if isinstance(out.get("prices"), dict):
        out["prices"] = {k: v for k, v in out["prices"].items() if k != "liquidity_screened"}
    return out


def classify(a: Mapping[str, Any], b: Mapping[str, Any]) -> str:
    """``unchanged``, ``removed:<reason>``, ``recovered``, ``restored:<old reason>`` or ``other:<why>``."""
    old, new = _stripped(a), _stripped(b)
    if old == new:
        return "unchanged"
    me_a, me_b = old.get("me") or {}, new.get("me") or {}
    rest_a = {k: v for k, v in old.items() if k not in ("me", "characteristics", "exclusion")}
    rest_b = {k: v for k, v in new.items() if k not in ("me", "characteristics", "exclusion")}
    if rest_a != rest_b:
        return "other:fields_outside_me_changed"
    reason = me_b.get("missing")
    if me_a.get("value") is not None and me_b.get("value") is None and reason in AMENDMENT_2:
        # A row check 2 recovered can still fail another check: its provenance is the recovered side's.
        recovered = str(me_b.get("shares_scope", "")).startswith("dqc_recovered:")
        unchanged_me = {k: v for k, v in me_a.items() if k not in ("value", "missing")}
        if not recovered and unchanged_me != {k: v for k, v in me_b.items() if k not in ("value", "missing")}:
            return "other:removed_me_provenance_changed"
        if old["exclusion"] is None and new["exclusion"] != reason:
            return "other:removed_but_admitted_differently"
        if "characteristics" in new:
            return "other:removed_row_kept_characteristics"
        if old["exclusion"] is not None and new["exclusion"] != old["exclusion"]:
            return "other:earlier_exclusion_changed"
        return f"removed{'_after_recovery' if recovered else ''}:{reason}"
    old_reason = me_a.get("missing")
    if me_a.get("value") is None and old_reason in AMENDMENT_2 and me_b.get("value") is not None:
        # 3d-ii: the check that removed it no longer fails. Only the ME funnel step may lift; an earlier exclusion
        # stays, and an admitted row gains its characteristics.
        if old["exclusion"] == old_reason:
            if new["exclusion"] is not None or "characteristics" not in new:
                return "other:restored_but_not_admitted"
        elif new["exclusion"] != old["exclusion"]:
            return "other:earlier_exclusion_changed"
        return f"restored:{old_reason}"
    if str(me_b.get("shares_scope", "")).startswith("dqc_recovered:"):
        if old["exclusion"] != new["exclusion"]:
            return "other:recovered_admission_changed"
        chars_a, chars_b = old.get("characteristics") or {}, new.get("characteristics") or {}
        if {k: v for k, v in chars_a.items() if k not in ME_DENOMINATED} != {
            k: v for k, v in chars_b.items() if k not in ME_DENOMINATED
        }:
            return "other:recovered_non_me_characteristic_changed"
        return "recovered"
    return "other:unclassified"


CAUSES: dict[str, str] = {
    "recovered": "DQC_0095 conflict resolved to the side within 100x of the verified reference",
    MeMissing.SHARES_SCALE_CONFLICT: (
        "cover vs same-filing balance-sheet count differ >100x, no verified reference to choose"
    ),
    MeMissing.SHARES_TURNOVER_IMPLAUSIBLE: "dollar volume >10x ME (count too small: subsidiary/shell or mis-scaled)",
    MeMissing.SHARES_DISCONTINUITY: ">100x jump vs last verified count of the series",
}
CSV_COLUMNS: tuple[str, ...] = (
    "M",
    "symbol",
    "cik",
    "verdict",
    "shares_scope",
    "fact_used",
    "filed_value",
    "split_product",
    "raw_me",
    "dollar_volume_over_raw_me",
    "checks",
    "cause",
)


def _cause(verdict: str, me: Mapping[str, Any]) -> str:
    kind, _, reason = verdict.partition(":")
    if kind == "restored" and reason == MeMissing.SHARES_SCALE_CONFLICT:
        return "DQC_0095 conflict now resolved to the side within 100x of a reference Amendment 2.1 made eligible"
    if kind == "restored":
        return f"{reason} no longer applies: the row's check-4 reference changed"
    if reason == MeMissing.SHARES_BASIS_AMBIGUOUS:
        product = float(me["split_product"])
        within = 1 / float(SCALE_TOLERANCE) <= product <= float(SCALE_TOLERANCE)
        return "split stamp between cover date and filing" if within else "split stamp beyond [1/100,100]"
    return CAUSES[reason or kind]


def csv_row(verdict: str, b: Mapping[str, Any]) -> dict[str, str]:
    """One evidence line per changed row, from arm B's row: the count used, the unchecked ME and the cause."""
    me = b.get("me") or {}
    raw = me.get("raw") or me.get("value")
    dollar_volume = (b.get("prices") or {}).get("dollar_volume")
    fact = (me.get("facts") or [{}])[0]
    return {
        "M": b["M"],
        "symbol": b.get("symbol") or "",
        "cik": b.get("cik") or "",
        "verdict": verdict,
        "shares_scope": me.get("shares_scope") or "",
        "fact_used": fact.get("concept", ""),
        "filed_value": fact.get("value", ""),
        "split_product": me.get("split_product") or "",
        "raw_me": "" if raw is None else f"{float(raw):.4g}",
        "dollar_volume_over_raw_me": ""
        if raw is None or dollar_volume is None
        else f"{dollar_volume / float(raw):.3g}",
        "checks": json.dumps(me.get("checks") or {}, sort_keys=True),
        "cause": _cause(verdict, me),
    }


def _pairs(a: Path, b: Path) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    rows_a = read_gz_lines(a)
    rows_b = read_gz_lines(b)
    for left, right in zip(rows_a, rows_b, strict=True):
        if (left["M"], left["series_id"]) != (right["M"], right["series_id"]):
            raise SystemExit(f"row order differs at {left['M']} series {left['series_id']}")
        yield left, right


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--a", type=Path, required=True, help="rows before the checks")
    parser.add_argument("--b", type=Path, required=True, help="rows after the checks")
    parser.add_argument("--out", type=Path, required=True, help="every changed row, for the adjudication")
    parser.add_argument("--csv", type=Path, help="the evidence CSV, one line per changed row, by symbol and M")
    args = parser.parse_args(argv)
    counts: Counter[str] = Counter()
    changed: list[dict[str, Any]] = []
    evidence: list[dict[str, str]] = []
    for left, right in _pairs(args.a, args.b):
        verdict = classify(left, right)
        counts[verdict] += 1
        if verdict != "unchanged" and not verdict.startswith("other:"):
            evidence.append(csv_row(verdict, right))
        if verdict != "unchanged":
            me = right.get("me") or {}
            changed.append(
                {
                    "verdict": verdict,
                    "M": right["M"],
                    "series_id": right["series_id"],
                    "symbol": right.get("symbol"),
                    "cik": right.get("cik"),
                    "me_before": (left.get("me") or {}).get("value"),
                    "me_after": me.get("value"),
                    "shares": me.get("shares"),
                    "shares_scope": me.get("shares_scope"),
                    "checks": me.get("checks"),
                    "dollar_volume": (right.get("prices") or {}).get("dollar_volume"),
                }
            )
    args.out.write_text(json.dumps(changed, indent=0))
    if args.csv is not None:
        with args.csv.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(sorted(evidence, key=lambda r: (r["symbol"], r["M"])))
    print(json.dumps(dict(sorted(counts.items())), indent=1))
    return 1 if any(k.startswith("other:") for k in counts) else 0


if __name__ == "__main__":
    raise SystemExit(main())
