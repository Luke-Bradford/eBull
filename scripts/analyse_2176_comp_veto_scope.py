"""#2176 class 1 — rank candidate SCOPES for the Item 402 comp veto, offline.

Consumes ``census_def14a_window_tables.py``'s dump and replays the SHIPPED
selector against each candidate over the same substrate, so every variant is
measured on the full population without re-parsing 42k payloads per variant.
Same technique, same dump format and the same replay functions as
``analyse_def14a_window_tables.py`` — this script differs only in its CONTROL.

⚠ The control here is the gate as SHIPPED (``_item403_value_signature``,
IMPORTED, never re-expressed), not the pre-#2160 score selector that
``analyse_def14a_window_tables.py`` compares against. That script's baseline is
historical; #2176 is a delta on top of what runs today.

The candidates are simulated, which is the right way round: the A/B skill's rule
is that the CONTROL must never be simulated. A candidate that wins here is then
measured end-to-end against a real ``origin/main`` worktree with
``ab_2358_def14a_line_structure.py`` before anything ships.

    PYTHONPATH=. uv run python scripts/analyse_2176_comp_veto_scope.py DUMP.jsonl

⚠ The dump clips each table's name list to 40 (``_MAX_NAMES``), so the holder
counts below rank variants; they are not the shipping figure.

⚠ It also clips each header tuple to 12 cells (``_MAX_HEADERS``), and that is
visible in the fidelity gate rather than hidden by it: 0001140361-25-046376 and
0001140361-25-046469 carry an 18-cell header whose ``Percent (2)`` caption sits
at index 13, so the replay sees no percent column and reports 0 rows where the
real parser returns 19. Two accessions of 25,954. A variant that fires only on a
wide header would be invisible here, which is one more reason the end-to-end
A/B is the authority and this script is a ranking.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter

import app.providers.implementations.sec_def14a as P
from scripts.analyse_def14a_window_tables import (
    ROW_IDENTITY_FLOOR,
    SCORE_FLOOR,
    identity_fraction,
    joined_headers,
    table_key,
)


def _evidence(t: dict) -> bool:
    """``_has_item403_value_rows`` over the dump's stored counts."""
    n = t.get("n") or 0
    if not n:
        return False
    return t.get("nsh", 0) >= P._VALUE_ROW_EVIDENCE_FLOOR * n and t.get("npc", 0) >= P._VALUE_ROW_EVIDENCE_FLOOR * n


def _cells(t: dict) -> list[str]:
    """Exactly what ``_is_item403_eligible`` passes: score_headers + column_headers."""
    return list(t["sh"]) + list(t["ch"])


def sig_shipped(t: dict) -> bool:
    """CONTROL — imported, not re-expressed."""
    return P._item403_value_signature(tuple(_cells(t)), data_row_evidence=_evidence(t))


def _shipped2(t: dict, _anchors: list[frozenset[str]]) -> bool:
    """``sig_shipped`` under the two-argument variant signature."""
    return sig_shipped(t)


def _strong(joined: str) -> bool:
    return bool(
        P._ITEM403_BENEFICIAL_RE.search(joined)
        or P._ITEM403_OWNER_CAPTION_RE.search(joined)
        or P._ITEM403_AMOUNT_NATURE_RE.search(joined)
        or P._ITEM403_CLASS_PCT_RE.search(joined)
        or P._ITEM403_TITLE_OF_CLASS_RE.search(joined)
        or P._RULE_13D3_60_DAY_RE.search(joined)
    )


def _weak(joined: str) -> bool:
    return bool(
        P._ITEM403_AMOUNT_IND_RE.search(joined)
        and (
            P._ITEM403_PERCENT_IND_RE.search(joined)
            or P._ITEM403_OWNED_IND_RE.search(joined)
            or P._ITEM403_BENEFICIALLY_RE.search(joined)
        )
    )


def _neo_veto(joined: str) -> bool:
    return bool(P._ITEM402_NEO_CAPTION_RE.search(joined) and not P._ITEM403B_DIRECTOR_CAPTION_RE.search(joined))


def _candidate(t: dict, *, need_amount: bool, need_percent: bool) -> bool:
    """Comp veto scoped to the COLUMN rather than the joined header.

    ``_COMP_PCT_RE`` detects a percent whose DENOMINATOR is compensation, and a
    denominator is a property of one COLUMN. Applied to the joined header, one
    extra column condemns the whole table — which is #2176 class 1: ExlService's
    403(b) table carries a clean ``Shares`` and a clean ``% (2)`` beside a
    ``Vested but unsettled RSUs (3)`` breakdown column that 17 CFR 240.13d-3(d)(1)(i)
    makes a legitimate COMPONENT of 229.403 column 3.

    So the veto stands unless the prescribed value columns survive it: a cell
    carrying an amount and (per the flags) a cell carrying a percent, neither
    matching Item 402 vocabulary.
    """
    cells = _cells(t)
    joined = " | ".join(cells)
    if _strong(joined):
        return True
    if _neo_veto(joined):
        return False
    if any(P._COMP_PCT_RE.search(c) for c in cells):
        clean = [c for c in cells if not P._COMP_PCT_RE.search(c)]
        ok = True
        if need_amount:
            ok = ok and any(P._ITEM403_AMOUNT_IND_RE.search(c) for c in clean)
        if need_percent:
            ok = ok and any(P._ITEM403_PERCENT_IND_RE.search(c) for c in clean)
        if not ok:
            return False
    if _evidence(t):
        return True
    return _weak(joined)


# 13d-3(d)(1)(i) wording WITHOUT the sixty-day phrase — the per-case arm, kept
# only as a comparison point for the scope change. "Vested but unsettled RSUs"
# is a right to acquire that has already vested, so the window is moot.
def _vested_unsettled(t: dict) -> bool:
    cells = _cells(t)
    joined = " | ".join(cells)
    if _strong(joined):
        return True
    if re.search(r"vested\b[^|]{0,20}?\b(but\s+)?(un(settled|issued|delivered|paid))", joined, re.IGNORECASE):
        return True
    if P._COMP_PCT_RE.search(joined) or _neo_veto(joined):
        return False
    if _evidence(t):
        return True
    return _weak(joined)


_NON_CAPTION_RE = re.compile(r"[^a-z0-9%]+")


def _norm_cells(t: dict) -> frozenset[str]:
    """A table's header as a set of footnote-stripped, case-folded captions."""
    out = set()
    for cell in _cells(t):
        norm = _NON_CAPTION_RE.sub(" ", P._FOOTNOTE_RE.sub("", cell).lower()).strip()
        if norm:
            out.add(norm)
    return frozenset(out)


def _sibling_admits(t: dict, anchors: list[frozenset[str]], *, floor: int, symmetric: bool) -> bool:
    """229.403(a) and (b) are TWO subsections of ONE Item.

    Issuers render them as two tables in the same section, and the parser
    already concatenates every eligible table in the winning window. So a table
    sitting beside an INDEPENDENTLY eligible Item 403 table, whose header repeats
    that table's captions and merely adds columns, is the Item's other
    subsection. ExlService's 403(b) table adds one column — "Vested but
    unsettled RSUs", which 17 CFR 240.13d-3(d)(1)(i) makes a COMPONENT of
    229.403 column 3 — and that single column carries the Item 402 vocabulary
    that vetoes the whole table today.

    Cannot flip window selection: it only fires inside a window an
    independently-eligible table has already won.
    """
    mine = _norm_cells(t)
    for anchor in anchors:
        if len(anchor) < floor:
            continue
        if anchor <= mine:
            return True
        if symmetric and mine <= anchor and len(mine) >= floor:
            return True
    return False


VARIANTS = {
    "c1_amount_and_percent": lambda t, _a: _candidate(t, need_amount=True, need_percent=True),
    "c2_percent_only": lambda t, _a: _candidate(t, need_amount=False, need_percent=True),
    "c3_vested_unsettled_arm": lambda t, _a: _vested_unsettled(t),
    "s1_sibling_superset_min2": lambda t, a: sig_shipped(t) or _sibling_admits(t, a, floor=2, symmetric=False),
    "s1_sibling_superset_min3": lambda t, a: sig_shipped(t) or _sibling_admits(t, a, floor=3, symmetric=False),
    "s2_sibling_symmetric_min3": lambda t, a: sig_shipped(t) or _sibling_admits(t, a, floor=3, symmetric=True),
}


def _select(windows: list[list[dict]], sig) -> list[dict]:
    """Replay the shipped window loop under SIG.

    SIG takes (table, anchors) where ANCHORS are the header-cell sets of the
    tables the SHIPPED gate already admits in this window — the sibling rule's
    precondition, and empty for every non-sibling variant.
    """
    for tables in windows:
        qualifying = [t for t in tables if t["s"] >= SCORE_FLOOR]
        anchors = [
            _norm_cells(t)
            for t in qualifying
            if sig_shipped(t) and identity_fraction(t) >= ROW_IDENTITY_FLOOR and t["n"] > 0
        ]
        eligible = [t for t in qualifying if sig(t, anchors) and identity_fraction(t) >= ROW_IDENTITY_FLOOR]
        if eligible:
            return eligible
    return []


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
    print(f"accessions parsed from dump: {len(recs)}\n")

    # FIDELITY GATE — against the SHIPPED replay, which is this script's control.
    checked = [r for r in recs if "real_rows" in r]
    if not checked:
        print("!! FIDELITY UNVERIFIED — dump carries no `real_rows`. Re-run the census.\n")
        return 1
    mismatched = [
        (r["accession"], sum(t["n"] for t in _select(r["windows"], _shipped2)), r["real_rows"])
        for r in checked
        if (any(t["n"] > 0 for t in _select(r["windows"], _shipped2))) != (r["real_rows"] > 0)
    ]
    rate = 100 * (1 - len(mismatched) / len(checked))
    print(f"fidelity: shipped replay reproduces real rows/no-rows on {rate:.3f}% of {len(checked)} accessions")
    for acc, rep, real in mismatched[:15]:
        print(f"  !! {acc} replay_rows={rep} real_rows={real}")
    print()

    for vname, sig in VARIANTS.items():
        gained_tables: Counter[str] = Counter()
        gained_examples: dict[str, list[str]] = {}
        lost_tables: Counter[str] = Counter()
        lost_examples: dict[str, list[str]] = {}
        to_zero: list[str] = []
        from_zero: list[str] = []
        n_ctl = n_var = 0
        acc_changed = 0

        for r in recs:
            ctl = _select(r["windows"], _shipped2)
            var = _select(r["windows"], sig)
            n_ctl += sum(t["n"] for t in ctl)
            n_var += sum(t["n"] for t in var)
            ck = {table_key(t) for t in ctl}
            vk = {table_key(t) for t in var}
            if ck != vk:
                acc_changed += 1
            ctl_has = any(t["n"] > 0 for t in ctl)
            var_has = any(t["n"] > 0 for t in var)
            if ctl_has and not var_has:
                to_zero.append(r["accession"])
            if var_has and not ctl_has:
                from_zero.append(r["accession"])
            for t in var:
                if table_key(t) not in ck and t["n"] > 0:
                    h = joined_headers(t, "both")[:130]
                    gained_tables[h] += 1
                    gained_examples.setdefault(h, []).append(f"{r['accession']} n={t['n']} {t['nm'][:3]}")
            for t in ctl:
                if table_key(t) not in vk and t["n"] > 0:
                    h = joined_headers(t, "both")[:130]
                    lost_tables[h] += 1
                    lost_examples.setdefault(h, []).append(f"{r['accession']} n={t['n']} {t['nm'][:3]}")

        print(f"===== {vname} =====")
        print(f"extracted-name total  shipped={n_ctl}  variant={n_var}")
        print(f"accessions whose selected TABLE SET changes: {acc_changed}")
        print(f"accessions rows -> ZERO: {len(to_zero)}   ZERO -> rows: {len(from_zero)}")
        print(f"tables newly admitted: {sum(gained_tables.values())} ({len(gained_tables)} header shapes)")
        print(f"tables de-admitted   : {sum(lost_tables.values())} ({len(lost_tables)} header shapes)")
        print("\n-- WIDENING: every newly-admitted header shape --")
        for h, c in gained_tables.most_common():
            print(f"  {c:>5}  {h}")
            print(f"         e.g. {gained_examples[h][0]}")
        print("\n-- NARROWING: every de-admitted header shape --")
        for h, c in lost_tables.most_common():
            print(f"  {c:>5}  {h}")
            print(f"         e.g. {lost_examples[h][0]}")
        print(f"\n-- accessions to ZERO: {to_zero[:40]}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
