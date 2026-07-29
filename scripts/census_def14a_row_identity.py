"""Measure separability of the row-identity test on the full census.

17 CFR 229.403 prescribes the ROW identity: "Name [and address] of beneficial
owner". Instruction 5 adds 403(b)'s directors-and-officers-as-a-group row.

Question: does "fraction of rows whose first cell is a beneficial-owner
identity" separate genuine Item 403 tables from the share-talking tables that
currently beat them (income statements, cap tables, plan pools, TSR metrics)?

If there is no clean separation, the model needs changing again -- report that
rather than picking a threshold that papers over an overlap.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter

# --- Beneficial-owner identity, POSITIVE test only --------------------------
# A blocklist of junk labels would need an entry per new junk table shape; the
# prevention log's rule on hand-enumerated tuples applies. A positive test is
# closed under new junk types.

# Entity designators. Deliberately broad -- an Item 403 5%-holder list is full
# of funds, advisers and nominees.
_ENTITY = re.compile(
    r"\b("
    r"LLC|L\.L\.C|LP|L\.P|LLP|Inc|Incorporated|Corp|Corporation|Company|Co|Ltd|Limited"
    r"|Trust|Fund|Funds|Partners|Partnership|Capital|Management|Advisers|Advisors"
    r"|Holdings|Group|Associates|Ventures|Bank|N\.A|plc|GmbH|S\.A|AG|NV|PLC"
    r"|Foundation|Insurance|Investments?|Asset|Securities|Financial"
    r")\b",
    re.IGNORECASE,
)

# Person name at the START of the cell. NOT anchored at the end: Item 403(a)
# prescribes "Name AND ADDRESS of beneficial owner", so the address is
# legitimately part of the same cell, and issuers routinely append a title
# ("David J. Mazzo, Ph.D., President and Chief Executive Officer") or several
# credentials. Anchoring to $ rejected all of those -- measured, not assumed.
# The optional comma covers the surname-first rendering ("Bunch, Charles E.",
# "Allen, Dwayne L."), which issuers use for alphabetised management tables.
_PERSON = re.compile(r"^[A-Z][A-Za-z.'’-]*,?(?:\s+(?:[A-Z][A-Za-z.'’-]*|[A-Z]\.|van|von|de|del|der|di|la|le),?)+")

# Instruction 5's aggregate row.
_GROUP = re.compile(r"as a group", re.IGNORECASE)

# Presentation debris. Leader dots are an HTML-table artefact and appear INSIDE
# the name cell, not only at the end.
_TRAIL = re.compile(r"\s*[\(\[]\s*[\d,\s]+\s*[\)\]]\s*$|\s*\*+\s*$")
_LEADER = re.compile(r"[.…]{3,}")
_FOOTNOTE_DIGITS = re.compile(r"\s+[\d,]+\s*$")


def _clean(name: str) -> str:
    n = _LEADER.sub(" ", (name or "").strip())
    n = _TRAIL.sub("", n)
    n = _FOOTNOTE_DIGITS.sub("", n)
    return n.strip(" .,​﻿")


def is_owner_identity(name: str) -> bool:
    n = _clean(name)
    if not n:
        return False
    if _GROUP.search(n):
        return True
    if _ENTITY.search(n):
        return True
    return bool(_PERSON.match(n))


def main() -> int:
    path = sys.argv[1]
    recs = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith(("DONE", "...")):
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "error" not in r and r.get("n_rows", 0) > 0:
                recs.append(r)

    print(f"accessions with rows: {len(recs)}\n")

    # Reuse the Q1 cohort split so the two measurements are comparable.
    def is_c_only(r: dict) -> bool:
        w = [c for c in r["candidates"] if c["at_winning_score"]]
        return bool(w) and all(c["klass"] == "C_neither" for c in w)

    hist_all: Counter[int] = Counter()
    hist_conly: Counter[int] = Counter()
    low_examples = []

    for r in recs:
        hs = r["holders"]
        if not hs:
            continue
        frac = sum(1 for h in hs if is_owner_identity(h)) / len(hs)
        bucket = int(frac * 10) * 10
        hist_all[bucket] += 1
        if is_c_only(r):
            hist_conly[bucket] += 1
        if frac < 0.5 and len(low_examples) < 20:
            low_examples.append((r["accession"], round(frac, 2), hs[:3]))

    print("owner-identity fraction, ALL accessions with rows")
    print(f"{'bucket':>8} {'count':>8}  {'cum%':>7}")
    tot = sum(hist_all.values())
    cum = 0
    for b in sorted(hist_all):
        cum += hist_all[b]
        print(f"{b:>6}-% {hist_all[b]:>8}  {100 * cum / tot:>6.2f}")
    print()

    print("same, restricted to the C_neither-winner cohort (junk-suspect)")
    tot2 = sum(hist_conly.values())
    for b in sorted(hist_conly):
        print(f"{b:>6}-% {hist_conly[b]:>8}")
    print(f"  cohort total: {tot2}")
    print()

    print("accessions below 50% owner-identity (candidates for rejection):")
    n_low = sum(v for k, v in hist_all.items() if k < 50)
    print(f"  count: {n_low}  ({100 * n_low / tot:.2f}% of accessions with rows)")
    for acc, frac, hs in low_examples:
        print(f"  {acc}  frac={frac}  {hs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
