"""Evaluate Item 403 selection-gate variants offline, in BOTH directions.

Consumes ``census_def14a_window_tables.py``'s dump. Replays the real selector's
window loop -- ``main``'s score rule and the branch's eligibility rule -- over
the same substrate, so every variant is measured on the full population without
re-parsing 42,566 payloads per variant.

Both directions, because a selection gate has two failure modes:

  NARROW  a table that wins on ``main`` is now ineligible. If nothing else in
          its window is eligible the accession falls through to the next window
          or emits zero rows. This is the direction census pass 2 never
          measured, and it is where the D4 over-rejection lives
          (#2160 comment 5125091295).
  WIDEN   a table that ``main`` excluded joins the winning window's sibling set.
          Failure mode is admitting an Item 402 comp table whose rows are people
          (spec acceptance: that count must be zero).

Usage:
    PYTHONPATH=. uv run python scripts/analyse_def14a_window_tables.py DUMP.jsonl [variant]
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter

from app.providers.implementations.sec_def14a import (
    _is_beneficial_owner_identity,
    _item403_value_signature,
)


def sig_final(joined: str, table: dict | None = None) -> bool:
    """The gate as SHIPPED — imported, not re-expressed.

    The v0..v6 ladder below exists to justify the choice; this variant is what
    actually runs in ``parse_beneficial_ownership_table``. Keeping it a direct
    import is deliberate harness integrity: a re-typed copy drifts from the
    implementation, and then the measurement describes a gate nobody ships
    (`.claude/skills/engineering/full-population-ab.md`, "never simulate").
    """
    # MUST pass the data-row evidence too, or this measures a header-only gate
    # nobody ships. `_is_item403_eligible` derives it from the extracted holders;
    # the dump carries the same counts as `nsh` / `npc`.
    evidence = False
    if table is not None and table.get("n"):
        n = table["n"]
        evidence = table.get("nsh", 0) >= 0.5 * n and table.get("npc", 0) >= 0.5 * n
    return _item403_value_signature((joined,), data_row_evidence=evidence)


SCORE_FLOOR = 3
MAIN_SIBLING_FLOOR = 6
ROW_IDENTITY_FLOOR = 0.5

# --- gate vocabulary, each arm tied to a column of 17 CFR 229.403 ------------
# Column 4: "Percent of class" -- class-denominated by definition.
CLASS_PCT = re.compile(
    r"(percent|percentage|%)\s*(of\s+)?(the\s+)?(all\s+|total\s+|outstanding\s+)*"
    r"(class|common|shares|stock|voting|ownership|beneficial|equity|units)",
    re.IGNORECASE,
)
# Column 3: "Amount and nature of beneficial ownership", and the Rule 13d-3
# (17 CFR 240.13d-3) subdivision into voting / investment power.
AMOUNT_NATURE = re.compile(
    r"(amount\s+and\s+nature)|((sole|shared)\s+(voting|dispositive|investment))",
    re.IGNORECASE,
)
# Column 3's ordinary caption. Issuers who write "Shares Beneficially Owned" or
# "Beneficial Stock Ownership" are quoting the reg's own noun phrase.
#
# Keyed on own(ed|ership), NOT owner: "Name of Beneficial OWNER" is column 2's
# caption and says nothing about the VALUE columns, so treating it as Item 403
# evidence admits 'Beneficial Owner | Number of RSUs' -- a junk shape from the
# spec's own probe list. Gap-tolerant but confined within one header cell (no
# '|'), so it cannot bridge two unrelated captions.
BENEFICIAL = re.compile(r"beneficial(ly)?\b[^|]{0,30}?\bown(ed|ership)\b", re.IGNORECASE)
# Column 3 without the reg's wording -- a share/unit count of some kind.
AMOUNT_IND = re.compile(r"\b(shares?|number|amount|units?|stock)\b", re.IGNORECASE)
# Column 4 without the class noun. Word-bounded so 'Percentile' (TSR tables)
# does not read as a percent column.
PERCENT_IND = re.compile(r"\bpercent(age|ages)?\b|%", re.IGNORECASE)
# Item 402, not Item 403: a percent of salary / target / payout / vesting.
COMP_PCT = re.compile(
    r"(base\s+salary|of\s+target|target\s+bonus|payout|vesting"
    r"|bonus\s+opportunity|salary|earned|performance)",
    re.IGNORECASE,
)


def sig_v0(joined: str) -> bool:
    """As pinned in the spec and implemented on the branch."""
    if COMP_PCT.search(joined):
        return False
    return bool(CLASS_PCT.search(joined) or AMOUNT_NATURE.search(joined))


def sig_v1(joined: str) -> bool:
    """Columns 3 and 4 as a PAIR (the issue comment's candidate amendment)."""
    if COMP_PCT.search(joined):
        return False
    return bool((AMOUNT_IND.search(joined) and PERCENT_IND.search(joined)) or AMOUNT_NATURE.search(joined))


def sig_v2(joined: str) -> bool:
    """v1 plus column 3's own reg wording as an independent arm."""
    if COMP_PCT.search(joined):
        return False
    if BENEFICIAL.search(joined) or AMOUNT_NATURE.search(joined) or CLASS_PCT.search(joined):
        return True
    return bool(AMOUNT_IND.search(joined) and PERCENT_IND.search(joined))


def sig_v4(joined: str) -> bool:
    """Precedence, not vocabulary: explicit reg wording beats the comp veto.

    The comp veto cannot be applied over the whole header. Rule 13d-3(d)(1)(i)
    DEEMS a person the beneficial owner of shares acquirable within 60 days, so
    a genuine Item 403 table legitimately carries columns captioned 'Options
    Exercisable or Vesting Within 60 Days' and 'Number of Performance Shares
    Granted' -- and a blanket veto on 'vesting' / 'performance' deletes them
    (measured: 18-, 22- and 10-holder Vanguard / BlackRock / First Eagle tables).

    So: a header quoting 229.403's own column 3 or column 4 wording is Item 403
    on its face and is admitted outright. Only the WEAK generic evidence -- some
    amount column plus some percent column, which an Item 402 payout table also
    has -- is subject to the comp veto.
    """
    if BENEFICIAL.search(joined) or AMOUNT_NATURE.search(joined) or CLASS_PCT.search(joined):
        return True
    if COMP_PCT.search(joined):
        return False
    return bool(AMOUNT_IND.search(joined) and PERCENT_IND.search(joined))


# A VALUE column reporting what is held. 'owner' is excluded on purpose -- that
# is column 2's caption, not column 3's.
OWNED_IND = re.compile(r"\bown(ed|ership)\b", re.IGNORECASE)


def sig_v5(joined: str) -> bool:
    """v4, plus 'amount + owned' as a second weak pair.

    Dual-class and direct/indirect Item 403 tables caption their value columns
    'Class A Common Stock Owned | Class B Common Stock Owned | Total Voting
    Power' or 'Directly Owned | Indirectly Owned | Options to Acquire Stock' --
    genuine, and carrying no percent column at all, so v4's amount+percent pair
    rejects them (measured: 12- and 13-holder tables). The word 'Beneficial'
    sits in the NAME column, one '|' away, so the strong arm cannot reach it.

    Kept in the WEAK set, under the comp veto: a stock-ownership-GUIDELINES
    table also says 'Shares Owned', and Item 402 vocabulary must still be able
    to reject it.
    """
    if BENEFICIAL.search(joined) or AMOUNT_NATURE.search(joined) or CLASS_PCT.search(joined):
        return True
    if COMP_PCT.search(joined):
        return False
    return bool(AMOUNT_IND.search(joined) and (PERCENT_IND.search(joined) or OWNED_IND.search(joined)))


# Item 402(a)(3) DEFINES "named executive officer". Item 403 says "name of
# beneficial owner". A header that captions its name column with Item 402's own
# term of art, and carries none of Item 403's column-3/4 wording, is an Item 402
# table -- which is how 'Named Executive Officer | PSU Shares Granted (#) |
# Final Achievement %' leaked through v5's generic amount+percent pair.
# Safe as a veto because it sits BELOW the strong arms: a genuine 403(b) table
# captioned 'Named Executive Officer | Shares Beneficially Owned | Percent of
# Class' is admitted by the strong arm before this is reached.
NEO_CAPTION = re.compile(r"named\s+executive\s+officer", re.IGNORECASE)


def sig_v6(joined: str) -> bool:
    """v5, plus Item 402's own name-column term of art as a weak-set veto."""
    if BENEFICIAL.search(joined) or AMOUNT_NATURE.search(joined) or CLASS_PCT.search(joined):
        return True
    if COMP_PCT.search(joined) or NEO_CAPTION.search(joined):
        return False
    return bool(AMOUNT_IND.search(joined) and (PERCENT_IND.search(joined) or OWNED_IND.search(joined)))


def sig_v3(joined: str) -> bool:
    """Loosest reference point: any Item 403 value vocabulary at all."""
    if COMP_PCT.search(joined):
        return False
    return bool(AMOUNT_IND.search(joined) or PERCENT_IND.search(joined) or AMOUNT_NATURE.search(joined))


# (signature fn, which header tuple it reads).
#
# The header source is itself a measured decision. The branch implementation
# evaluates D4 on ``column_headers``, but the spec's census measured
# ``score_headers`` -- and they differ whenever a two-row header promotes a
# SUB-header row: ``column_headers`` becomes ``('', 'Sole', 'Shared', 'Total', '')``
# while the parent caption ``Amount and Nature of Beneficial Ownership |
# Percent of Class`` survives only in ``score_headers``. Reading the wrong tuple
# rejects the most prescribed shape there is (observed at score 14 and 16 in
# tests/test_sec_def14a_parser.py).
VARIANTS = {
    "v0_ch": (sig_v0, "ch"),  # exactly what the branch implements today
    "v0_sh": (sig_v0, "both"),  # the spec's pinned regex, right header source
    "v1": (sig_v1, "both"),  # issue comment's pair amendment
    "v2": (sig_v2, "both"),  # pair + column-3 reg wording
    "v4": (sig_v4, "both"),  # v2, but reg wording OUTRANKS the comp veto
    "v5": (sig_v5, "both"),  # v4 + amount-and-owned weak pair
    "v6": (sig_v6, "both"),  # v5 + Item 402 NEO-caption veto
    "final": (sig_final, "both"),  # the gate as SHIPPED (imported)
    "v3": (sig_v3, "both"),  # loosest reference point
}


def joined_headers(t: dict, source: str = "both") -> str:
    if source == "ch":
        return " | ".join(t["ch"])
    if source == "sh":
        return " | ".join(t["sh"])
    return " | ".join(list(t["sh"]) + list(t["ch"]))


def identity_fraction(t: dict) -> float:
    names = t["nm"]
    if not names:
        return 0.0
    return sum(1 for nm in names if _is_beneficial_owner_identity(nm)) / len(names)


def main_selection(windows: list[list[dict]]) -> tuple[int, list[dict]]:
    """Replay ``main``: first window whose best table clears the score floor."""
    for wi, tables in enumerate(windows):
        if not tables:
            continue
        best = max(t["s"] for t in tables)
        qualifying = [t for t in tables if t["s"] >= SCORE_FLOOR]
        if qualifying and best >= SCORE_FLOOR:
            return wi, [t for t in qualifying if t["s"] >= MAIN_SIBLING_FLOOR or t["s"] == best]
    return -1, []


def _apply_sig(sig, joined: str, table: dict) -> bool:
    """Call a variant, passing the table only to variants that accept it."""
    try:
        return sig(joined, table)
    except TypeError:
        return sig(joined)


def branch_selection(windows: list[list[dict]], sig, source: str) -> tuple[int, list[dict]]:
    """Replay the branch: first window with an ELIGIBLE table (D2/D3/D4)."""
    for wi, tables in enumerate(windows):
        qualifying = [t for t in tables if t["s"] >= SCORE_FLOOR]
        eligible = [
            t
            for t in qualifying
            if _apply_sig(sig, joined_headers(t, source), t) and identity_fraction(t) >= ROW_IDENTITY_FLOOR
        ]
        if eligible:
            return wi, eligible
    return -1, []


def table_key(t: dict) -> tuple:
    return (t["s"], tuple(t["sh"]), t["nr"], t["n"], tuple(t["nm"][:3]))


def main() -> int:
    path = sys.argv[1]
    only = sys.argv[2] if len(sys.argv) > 2 else None
    variants = {only: VARIANTS[only]} if only else VARIANTS

    recs = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "error" not in r:
                recs.append(r)
    print(f"accessions parsed from dump: {len(recs)}\n")

    # FIDELITY GATE. The replay stands in for the real parser; if it disagrees
    # with it, every variant ranking below is measuring a selector nobody runs.
    #
    # #2160 round 1 shipped a whole analysis before noticing 28 accessions where
    # the dump said "no table scores >=3 with rows" while real ``main`` produced
    # rows. Silence is not agreement -- assert it, and say so out loud when the
    # dump predates the check (`real_rows` absent).
    checked = [r for r in recs if "real_rows" in r]
    if not checked:
        print(
            "!! FIDELITY UNVERIFIED — this dump carries no `real_rows` field.\n"
            "   Re-run scripts/census_def14a_window_tables.py before trusting any\n"
            "   number below. Rankings only; never state an outcome from this.\n"
        )
    else:
        mismatched = []
        for r in checked:
            _, sel = main_selection(r["windows"])
            replay_has = any(t["n"] > 0 for t in sel)
            real_has = r["real_rows"] > 0
            if replay_has != real_has:
                mismatched.append((r["accession"], sum(t["n"] for t in sel), r["real_rows"]))
        rate = 100 * (1 - len(mismatched) / len(checked))
        print(f"fidelity: replay reproduces real `main` rows/no-rows on {rate:.3f}% of {len(checked)} accessions")
        if mismatched:
            print(f"!! {len(mismatched)} DIVERGENT — the replay is not a faithful control. First 15:")
            for acc, rep, real in mismatched[:15]:
                print(f"     {acc}  replay_rows={rep}  real_rows={real}")
        print()

    for vname, (sig, source) in variants.items():
        lost_all: list[tuple[str, list[dict]]] = []  # main had holders, branch has none
        lost_tables: Counter[str] = Counter()  # main-selected tables now ineligible
        lost_examples: dict[str, list[str]] = {}
        gained_tables: Counter[str] = Counter()
        gained_examples: dict[str, list[str]] = {}
        n_main_rows = n_branch_rows = 0

        for r in recs:
            windows = r["windows"]
            _, mainsel = main_selection(windows)
            _, brsel = branch_selection(windows, sig, source)
            main_has = any(t["n"] > 0 for t in mainsel)
            branch_has = any(t["n"] > 0 for t in brsel)
            n_main_rows += sum(t["n"] for t in mainsel)
            n_branch_rows += sum(t["n"] for t in brsel)
            if main_has and not branch_has:
                lost_all.append((r["accession"], mainsel))

            mk = {table_key(t) for t in mainsel}
            bk = {table_key(t) for t in brsel}
            for t in mainsel:
                if table_key(t) not in bk and t["n"] > 0:
                    h = joined_headers(t, source)[:110]
                    lost_tables[h] += 1
                    lost_examples.setdefault(h, []).append(f"{r['accession']} n={t['n']} {t['nm'][:2]}")
            for t in brsel:
                if table_key(t) not in mk and t["n"] > 0:
                    h = joined_headers(t, source)[:110]
                    gained_tables[h] += 1
                    gained_examples.setdefault(h, []).append(f"{r['accession']} n={t['n']} {t['nm'][:2]}")

        print(f"===== VARIANT {vname} =====")
        print(f"extracted-row total   main={n_main_rows}  branch={n_branch_rows}")
        print(f"accessions main->rows, branch->ZERO : {len(lost_all)}")
        print(f"distinct main-selected tables dropped: {sum(lost_tables.values())} ({len(lost_tables)} header shapes)")
        print(
            f"distinct tables newly admitted       : {sum(gained_tables.values())} ({len(gained_tables)} header shapes)"
        )
        print("\n-- NARROWING: top dropped header shapes --")
        for h, c in lost_tables.most_common(30):
            print(f"  {c:>5}  {h}")
            print(f"         e.g. {lost_examples[h][0]}")
        print("\n-- WIDENING: top newly-admitted header shapes --")
        for h, c in gained_tables.most_common(30):
            print(f"  {c:>5}  {h}")
            print(f"         e.g. {gained_examples[h][0]}")
        print("\n-- accessions that go to ZERO rows (first 40) --")
        for acc, sel in lost_all[:40]:
            hdr = joined_headers(sel[0], source)[:90] if sel else ""
            nm = sel[0]["nm"][:2] if sel else []
            print(f"  {acc}  {hdr}  {nm}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
