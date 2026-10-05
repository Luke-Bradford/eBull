"""#3609 step 1 slice 3d: full-population A/B of the Amendment 2 share checks.

Compares two stage-A row files built from the SAME frozen inputs (arm A: before the checks, arm B: after). Spec
§"Slices" 3d: every row whose ME is unchanged is identical (the new keys ``me.raw``, ``me.checks``, ``me.verified``
and ``prices.liquidity_screened`` aside); every changed row either lost its ME to exactly one Amendment 2 reason
(its admission and characteristics following) or was DQC-recovered (only ME and the ME-denominated characteristics
change). Anything else is a refusal and exits 1.

    PYTHONPATH=. uv run python -m scripts.ab_3609_share_checks --a <rows A> --b <rows B> --out <changes.json>
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from app.services.factor_panel import MeMissing
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
    """``unchanged``, ``removed:<reason>``, ``recovered`` or ``other:<why>``."""
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
        unchanged_me = {k: v for k, v in me_a.items() if k not in ("value", "missing")}
        if unchanged_me != {k: v for k, v in me_b.items() if k not in ("value", "missing")}:
            return "other:removed_me_provenance_changed"
        if old["exclusion"] is None and new["exclusion"] != reason:
            return "other:removed_but_admitted_differently"
        if old["exclusion"] is not None and new["exclusion"] != old["exclusion"]:
            return "other:earlier_exclusion_changed"
        return f"removed:{reason}"
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
    args = parser.parse_args(argv)
    counts: Counter[str] = Counter()
    changed: list[dict[str, Any]] = []
    for left, right in _pairs(args.a, args.b):
        verdict = classify(left, right)
        counts[verdict] += 1
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
    print(json.dumps(dict(sorted(counts.items())), indent=1))
    return 1 if any(k.startswith("other:") for k in counts) else 0


if __name__ == "__main__":
    raise SystemExit(main())
