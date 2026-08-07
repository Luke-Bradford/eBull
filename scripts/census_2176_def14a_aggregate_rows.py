"""Census for #2176 class 4 — rows whose NAME is not a beneficial owner at all.

Reads a full-population parse emitted by ``ab_2358_def14a_line_structure.py``
and enumerates, over every distinct ``lower(trim(holder_name))`` the parser
produces, the names a per-row application of ``_is_instrument_not_owner`` would
reject.

⚠ Point it at a CONTROL (pre-guard) parse. Run against a treatment parse the
guard has already applied, arms 1/2/4 all report zero — which is
indistinguishable from "the rule matches nothing" and is the same clean-result
trap the A/B skill warns about.

Why the census reads a PARSE and not ``def14a_beneficial_holdings``: the #2175
and #2169 fixes have not been backfilled, so 110,091 of the table's 110,748
rows still carry ``fetched_at = 2026-07-30`` and a census over them measures
the OLD parser. #2176's own last comment asks for the residue "after the
backfill"; re-parsing the stored payloads is the same measurement without
waiting on an operator step.

    PYTHONPATH=. uv run python scripts/census_2176_def14a_aggregate_rows.py \
        --parse /tmp/2176-control.jsonl

Prints every rejected name in full — the population is the deliverable, not a
summary of it, and a "top 20" would hide exactly the genuine holder this test
must not be allowed to eat.

Arm 5 sizes the widening the guard deliberately does NOT take, so the test
suite can cite a computed figure instead of carrying a hand-written one.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter

from app.providers.implementations.sec_def14a import _INSTRUMENT_VOCAB, _WORD_RE, _is_instrument_not_owner

# Arm 3 — candidate vocabulary EXTENSIONS, measured rather than chosen. The
# aggregate labels the shipped vocabulary misses are 'grand total' and 'overall
# total', and the tempting fix is to add the missing adjectives. Whether that is
# safe is a measurement: 17 CFR 229.403(b) Instruction 5 REQUIRES the
# directors-and-officers-as-a-group row, and issuers label it 'Total shares
# owned by executive officers and directors (13 persons)'. Any extension that
# reaches one of those rows is disqualified regardless of how much junk it also
# catches.
_CANDIDATE_EXTENSIONS = frozenset({"grand", "overall", "aggregate", "sum", "combined"})
# Words that make a row the Instruction 5 aggregate — a row that MUST survive.
_INSTRUCTION_5_MARKERS = re.compile(r"\b(director|directors|officer|officers|group|nominee|nominees|person|persons)\b")
# Arm 5 — the aggregate noun a substring rule would key on (probe B's defect).
_AGGREGATE_NOUN = re.compile(r"\btotals?\b")


def _guard_rejects(name: str) -> bool:
    """Exactly the STORAGE guard's predicate, designator strip and all.

    ⚠ Not ``_is_instrument_not_owner(name)`` bare. The strip is a keyword the
    guard opts into and ``_is_beneficial_owner_identity`` deliberately does not,
    so a census calling the bare predicate measures the ELIGIBILITY semantics
    and silently under-reports what the guard drops.
    """
    return _is_instrument_not_owner(name, strip_class_designator=True)


def _census(parse_path: str) -> None:
    # name -> (rows, accessions). "rows" is the per-accession appearance count,
    # which is what a DELETE would remove; "accessions" is the blast radius.
    rows: Counter[str] = Counter()
    accessions: dict[str, set[str]] = {}
    total_rows = 0
    total_accessions = 0
    with open(parse_path) as fh:
        for line in fh:
            entry = json.loads(line)
            total_accessions += 1
            for name in entry["holders"]:
                total_rows += 1
                rows[name] += 1
                accessions.setdefault(name, set()).add(entry["acc"])

    rejected = sorted(name for name in rows if _guard_rejects(name))
    rejected_rows = sum(rows[name] for name in rejected)
    rejected_accessions = {acc for name in rejected for acc in accessions[name]}

    # A table losing EVERY row is the failure mode that matters: the per-row
    # test then does what a gate rejection would have done, silently. Counted
    # against the parse, so it is the real post-fix number rather than an
    # estimate off the stale table.
    wiped = 0
    with open(parse_path) as fh:
        for line in fh:
            entry = json.loads(line)
            names = list(entry["holders"])
            if names and all(_guard_rejects(n) for n in names):
                wiped += 1

    print("== corpus ==")
    print(f"  accessions parsed          {total_accessions:>8}")
    print(f"  holder rows                {total_rows:>8}")
    print(f"  distinct names             {len(rows):>8}")
    print("== per-row _is_instrument_not_owner would reject ==")
    print(f"  distinct names             {len(rejected):>8}")
    print(f"  rows                       {rejected_rows:>8}")
    print(f"  accessions touched         {len(rejected_accessions):>8}")
    print(f"  accessions wiped to ZERO   {wiped:>8}")
    print("\n== every rejected name, enumerated ==")
    for name in rejected:
        print(f"  {rows[name]:>5} rows  {len(accessions[name]):>4} acc  {name}")

    extended = _INSTRUMENT_VOCAB | _CANDIDATE_EXTENSIONS
    newly: list[str] = []
    for name in rows:
        if _guard_rejects(name):
            continue
        words = _WORD_RE.findall(name.lower())
        if words and all(w in extended for w in words):
            newly.append(name)
    print(f"\n== arm 3: names a vocabulary extension {sorted(_CANDIDATE_EXTENSIONS)} would NEWLY reject ==")
    for name in sorted(newly):
        verdict = "⚠ INSTRUCTION 5 ROW — extension DISQUALIFIED" if _INSTRUCTION_5_MARKERS.search(name) else "junk"
        print(f"  {rows[name]:>5} rows  {len(accessions[name]):>4} acc  {name}   [{verdict}]")
    if not newly:
        print("  (none)")

    # Arm 4 — what the CLASS-DESIGNATOR rule moves, on its own. Codex
    # checkpoint 2 found that `a` sits in the vocabulary as a connective
    # ARTICLE, so 'Class A Common Stock' tested as an instrument while 'Class B
    # Common Stock' did not. Stripping the designator fixes the asymmetry; this
    # arm enumerates every name whose verdict that strip changes, so the rule is
    # sized against the corpus rather than against the two examples that
    # prompted it.
    designator_only: list[str] = []
    for name in rows:
        words = _WORD_RE.findall(name.lower())
        if not words:
            continue
        plain = all(w in _INSTRUMENT_VOCAB for w in words)
        if _guard_rejects(name) and not plain:
            designator_only.append(name)
    print("\n== arm 4: names the CLASS-DESIGNATOR strip newly rejects (Codex ckpt-2) ==")
    print(f"  distinct names             {len(designator_only):>8}")
    for name in sorted(designator_only):
        print(f"  {rows[name]:>5} rows  {len(accessions[name]):>4} acc  {name}")
    if not designator_only:
        print("  (none)")

    # Arm 5 — the widening NOT taken, sized. The shipped rule is "every word is
    # instrument vocabulary"; the tempting simplification is "the name contains
    # the aggregate noun", which is what probe B injects. 229.403(b)
    # Instruction 5 REQUIRES the directors-and-officers-as-a-group row, and
    # issuers spell it with that very noun — so the substring rule deletes a row
    # the reg mandates. Computed here rather than written into a test docstring:
    # a hand-copied figure goes stale silently the moment the vocabulary moves.
    contains_total = [name for name in rows if _AGGREGATE_NOUN.search(name) and not _guard_rejects(name)]
    mandated = [name for name in contains_total if _INSTRUCTION_5_MARKERS.search(name)]
    survives_rows = sum(rows[n] for n in contains_total)
    mandated_rows = sum(rows[n] for n in mandated)
    print("\n== arm 5: what a `contains the aggregate noun` rule would delete (the widening NOT taken) ==")
    print(f"  survives the shipped guard   {len(contains_total):>6} names  {survives_rows:>6} rows")
    print(f"    of which Instruction 5     {len(mandated):>6} names  {mandated_rows:>6} rows  <- 229.403(b) MANDATES")
    for name in sorted(mandated, key=lambda n: -rows[n])[:15]:
        print(f"  {rows[name]:>5} rows  {len(accessions[name]):>4} acc  {name[:100]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--parse", required=True, help="JSONL emitted by the #2176 / #2358 A/B harness")
    args = ap.parse_args()
    _census(args.parse)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
