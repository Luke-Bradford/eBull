"""Per-ARM full-population audit of the Item 403 selection gate (#2160).

Answers the three Codex ckpt-1 findings that rest on unmeasured claims, using
the pass-3 census dump so nothing is re-parsed:

  A. SCORE FLOOR (HIGH). ``window_qualifying`` still filters ``score >= 3``, so
     ``_is_item403_eligible`` is never evaluated below it and the spec's "header
     score no longer decides sibling membership" is only true ABOVE 3. Counts
     what removing the floor would newly admit, so the floor's retention is a
     measured decision rather than an oversight.

  B. STRONG-ARM SOLE ADMITS (MED). Each strong arm admits OUTRIGHT, ahead of the
     Item 402 vetoes, so "nothing else in a proxy uses this caption" is a safety
     claim that has to be measured. For every arm, counts the tables it and it
     alone admits, and prints their headers and rows for hand classification.

  C. THE 120-CHAR D1 CAP (MED). The cap is empirical, not a documented SEC
     limit. Enumerates every extracted name that exceeds it AFTER debris
     stripping, so the loss it causes is inspected rather than assumed.

    PYTHONPATH=. uv run python scripts/audit_def14a_gate_arms.py DUMP.jsonl
"""

from __future__ import annotations

import json
import sys
from collections import Counter

import app.providers.implementations.sec_def14a as P
from scripts.analyse_def14a_window_tables import (
    ROW_IDENTITY_FLOOR,
    SCORE_FLOOR,
    branch_selection,
    identity_fraction,
    joined_headers,
    main_selection,
)

# Each strong arm, by the 229.403 / 240.13d-3 clause it encodes.
STRONG_ARMS = {
    "col3_beneficially_owned": P._ITEM403_BENEFICIAL_RE,
    "col2_owner_caption": P._ITEM403_OWNER_CAPTION_RE,
    "col3_amount_and_nature": P._ITEM403_AMOUNT_NATURE_RE,
    "col4_percent_of_class": P._ITEM403_CLASS_PCT_RE,
    "col1_title_of_class": P._ITEM403_TITLE_OF_CLASS_RE,
    "rule_13d3_60_day": P._RULE_13D3_60_DAY_RE,
}


def _sole_arm(joined: str) -> str | None:
    """The strong arm that admits JOINED when it is the ONLY one that does."""
    hits = [name for name, rx in STRONG_ARMS.items() if rx.search(joined)]
    return hits[0] if len(hits) == 1 else None


def main() -> int:
    recs = []
    with open(sys.argv[1]) as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                continue
            r = json.loads(line)
            if "error" not in r:
                recs.append(r)
    print(f"accessions: {len(recs)}\n")

    # --- A. score floor -----------------------------------------------------
    below_floor_eligible: Counter[str] = Counter()
    below_floor_rows = 0
    below_examples: dict[str, str] = {}
    for r in recs:
        for tables in r["windows"]:
            for t in tables:
                if t["s"] >= SCORE_FLOOR or t["n"] == 0:
                    continue
                j = joined_headers(t, "both")
                if P._item403_value_signature((j,)) and identity_fraction(t) >= ROW_IDENTITY_FLOOR:
                    h = j[:110]
                    below_floor_eligible[h] += 1
                    below_floor_rows += t["n"]
                    below_examples.setdefault(h, f"{r['accession']} n={t['n']} {t['nm'][:3]}")
    print("=== A. tables scoring 0-2 that WOULD pass eligibility if the floor were dropped ===")
    print(f"tables {sum(below_floor_eligible.values())}  rows {below_floor_rows}  shapes {len(below_floor_eligible)}")
    for h, c in below_floor_eligible.most_common(15):
        print(f"  {c:>5}  {h}")
        print(f"         {below_examples[h]}")

    # --- B. strong-arm sole admits -----------------------------------------
    print("\n=== B. tables admitted by exactly ONE strong arm (and by no other) ===")
    per_arm: dict[str, Counter[str]] = {k: Counter() for k in STRONG_ARMS}
    per_arm_ex: dict[str, dict[str, str]] = {k: {} for k in STRONG_ARMS}
    for r in recs:
        _, sel = branch_selection(r["windows"], lambda j: P._item403_value_signature((j,)), "both")
        for t in sel:
            if t["n"] == 0:
                continue
            j = joined_headers(t, "both")
            arm = _sole_arm(j)
            if arm is None:
                continue
            h = j[:110]
            per_arm[arm][h] += 1
            per_arm_ex[arm].setdefault(h, f"{r['accession']} n={t['n']} {t['nm'][:3]}")
    for arm, c in per_arm.items():
        print(f"\n-- {arm}: {sum(c.values())} tables, {len(c)} shapes")
        for h, n in c.most_common(6):
            print(f"  {n:>5}  {h}")
            print(f"         {per_arm_ex[arm][h]}")

    # --- C. the 120-char cap ------------------------------------------------
    print("\n=== C. extracted names exceeding the 120-char D1 cap AFTER debris stripping ===")
    over: Counter[str] = Counter()
    for r in recs:
        _, sel = main_selection(r["windows"])
        for t in sel:
            for nm in t["nm"]:
                cleaned = P._IDENTITY_DEBRIS_RE.sub(" ", nm).strip(" .,​﻿*†#")
                if len(cleaned) > P._OWNER_IDENTITY_MAX_LEN:
                    over[cleaned[:150]] += 1
    print(f"distinct over-cap names: {len(over)}  occurrences: {sum(over.values())}")
    for nm, c in over.most_common(25):
        print(f"  {c:>4}  {nm}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
