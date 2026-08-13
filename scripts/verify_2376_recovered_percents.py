"""Gain-side validation for #2376 -- corroborate every RECOVERED percent.

The A/B says how many percents moved NULL -> value. It cannot say whether any of
them is right, and "a change that only adds looks safe and isn't" is the
full-population-ab skill's own warning about exactly this arm.

The check uses the filing's OWN arithmetic. Item 403 column 4 is a percentage of
the same denominator for every row of a table, so ``shares / (percent/100)``
must land on one shares-outstanding figure across the accession. For each
accession carrying a recovered percent this compares the implied denominator
against the median implied denominator of the holders whose percent the CONTROL
arm already knew -- an independent anchor, since those values are untouched by
this change.

A recovered percent that agrees is corroborated by a number this change did not
produce. One that disagrees is enumerated in full, never averaged away.

    PYTHONPATH=. uv run python -m scripts.verify_2376_recovered_percents \
        /tmp/2376-control.jsonl /tmp/2376-treatment.jsonl
"""

from __future__ import annotations

import json
import statistics
import sys
from decimal import Decimal

# A holder's implied denominator is a ROUNDED percent divided into an exact share
# count, so it carries the rounding error of the published percent. At one
# decimal place (the common Item 403 precision) a 0.05 error on a 5.0% holding is
# a 1% error in the implied denominator; small percents amplify it further. 15%
# accommodates that without admitting a genuinely different denominator, which in
# practice differs by a share CLASS -- i.e. by a factor, not by a few percent.
_TOLERANCE = Decimal("0.15")
# Below this the rounding amplification swamps the test: a holder published as
# "0.1%" implies a denominator anywhere in a +/-50% band.
_MIN_PERCENT = Decimal("1.0")


def _load(path: str) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = {}
    with open(path) as handle:
        for line in handle:
            entry = json.loads(line)
            out[entry["acc"]] = entry["holders"]
    return out


def _implied(holder: dict) -> Decimal | None:
    if holder["shares"] is None or holder["percent"] is None:
        return None
    shares, percent = Decimal(holder["shares"]), Decimal(holder["percent"])
    if percent < _MIN_PERCENT or shares <= 0:
        return None
    return shares / (percent / Decimal(100))


def main() -> int:
    control, treatment = _load(sys.argv[1]), _load(sys.argv[2])
    recovered = 0
    # Per accession: (n recovered, recovered implied denominators, anchor ones).
    coherent_with_anchor: list[str] = []
    self_coherent_only: list[tuple[str, Decimal, Decimal, int]] = []
    incoherent: list[tuple[str, Decimal, Decimal, int]] = []
    untestable_accessions = 0

    for accession, before in control.items():
        after = treatment.get(accession, {})
        gains = [
            name
            for name in before.keys() & after.keys()
            if before[name]["percent"] is None and after[name]["percent"] is not None
        ]
        if not gains:
            continue
        recovered += len(gains)
        gained_implied = [value for name in gains if (value := _implied(after[name])) is not None]
        # Anchor on holders the CONTROL arm already resolved — untouched by this
        # change, so they cannot be corroborating themselves.
        anchors = [
            value for name, holder in before.items() if name not in gains and (value := _implied(holder)) is not None
        ]
        if not gained_implied or not anchors:
            untestable_accessions += 1
            continue
        recovered_median = statistics.median(gained_implied)
        anchor_median = statistics.median(anchors)
        if abs(recovered_median - anchor_median) / anchor_median <= _TOLERANCE:
            coherent_with_anchor.append(accession)
            continue
        # The recovered values disagree with the anchor. Do they at least agree
        # with EACH OTHER? A tight internal spread means they share one
        # denominator that is not the anchor's — a different share class or a
        # pro-forma table, i.e. a table-SELECTION question rather than a wrong
        # percent. A wide spread means the recovered values are noise, which is
        # the failure mode this arm exists to find.
        spread = max(gained_implied) / min(gained_implied) if min(gained_implied) > 0 else Decimal("inf")
        bucket = self_coherent_only if len(gained_implied) > 1 and spread <= (1 + _TOLERANCE) else incoherent
        bucket.append((accession, recovered_median, anchor_median, len(gains)))

    testable = len(coherent_with_anchor) + len(self_coherent_only) + len(incoherent)
    print("== #2376 gain-side corroboration, per ACCESSION ==")
    print(f"  percents recovered NULL -> value              {recovered:>8}")
    print(f"  accessions carrying a recovery, testable     {testable:>8}")
    print(f"  accessions not testable (no anchor / <1% / no shares) {untestable_accessions:>8}")
    print(f"    recovered denominator MATCHES the anchor   {len(coherent_with_anchor):>8}")
    print(f"    self-coherent but a DIFFERENT denominator  {len(self_coherent_only):>8}")
    print(f"    INCOHERENT — the arm's real finding        {len(incoherent):>8}")
    for label, rows in (
        ("self-coherent, different denominator", self_coherent_only),
        ("INCOHERENT", incoherent),
    ):
        print(f"\n== {label} — every one, enumerated ==")
        for accession, rec, anchor, count in sorted(rows):
            print(f"  {accession}  n={count:>3}  recovered={rec:>16.0f}  anchor={anchor:>16.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
