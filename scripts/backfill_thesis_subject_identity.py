"""Re-verdict every stored thesis against the subject-identity rule (#2436).

    PYTHONPATH=. uv run python scripts/backfill_thesis_subject_identity.py            # dry run
    PYTHONPATH=. uv run python scripts/backfill_thesis_subject_identity.py --apply

⚠ NEVER PIPE THIS INTO ``head``/``tail`` — a pipe returns the pipe's status, so
a failure reads as success (``.claude/CLAUDE.md``). Redirect and read the file.

WHAT IT DOES
------------
Runs ``thesis_subject_identity.memo_names_subject`` over EVERY row in ``theses``
and stores the verdict, the rule version that produced it, and the check time.
Nothing else is written: no memo is repaired, no row is deleted
(``docs/settled-decisions.md:147`` — "do not overwrite prior thesis rows").

IDEMPOTENCE IS VERDICT-EQUIVALENCE, NOT A NO-OP WRITE
-----------------------------------------------------
A row is UPDATEd only when its stored verdict or rule version differs from what
the current rule produces. A second run therefore writes zero rows and leaves
every ``subject_identity_checked_at`` untouched — the timestamp records when the
verdict was last *decided*, not when this script last ran. Bump the rule (any
edit to ``app/services/thesis_subject_identity.py`` changes its code hash) and
the next run re-verdicts and re-stamps the whole corpus.

THE FALSE-POSITIVE AUDIT IS THE LOAD-BEARING NUMBER
---------------------------------------------------
This backfill makes the gate SUBTRACTIVE: a quarantined row loses its stance,
its buy zone and its valuation band in ``portfolio.py``, ``scoring.py``,
``entry_timing.py`` and ``reporting.py``. So the number that decides whether it
is safe is not the reject rate — a rule rejecting everything would score well
there — but the rejections on v1-v4, the prompt versions where the writer
demonstrably named the right company. That must be ZERO, and this script exits 1
if it is not, BEFORE writing anything.

⚠ The residual, stated rather than hidden: v1-v4 is a labelled population of
known-good memos, not a labelled sample of ALL memos. It bounds false positives
on the population where correctness is known; it cannot prove a v5/v6 rejection
is correct. The same bound is all ``scripts/verify_2431_subject_identity.py``
ever claimed.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter

import psycopg

from app.config import settings
from app.services.thesis_subject_identity import (
    RULE_SET_VERSION,
    ensure_subject_identity_verdicts,
    memo_names_subject,
    subject_is_checkable,
)

#: Prompt versions where the writer demonstrably named the right company, so a
#: rejection here is a FALSE POSITIVE and the backfill must not write.
KNOWN_GOOD_VERSIONS = frozenset({"v1", "v2", "v3", "v4"})

_SELECT = """
    SELECT t.thesis_id, t.prompt_version, i.symbol, i.company_name, t.memo_markdown,
           t.subject_identity_ok, t.subject_identity_rule_version
    FROM theses t
    JOIN instruments i USING (instrument_id)
    ORDER BY t.thesis_id
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write the verdicts (default: dry run)")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SELECT).fetchall()

        verdicts: list[tuple[int, bool | None]] = []
        by_version: Counter[str] = Counter()
        rejects_by_version: Counter[str] = Counter()
        unchecked = 0
        false_positives: list[tuple[str, int, str]] = []

        for thesis_id, prompt_version, symbol, company_name, memo, _stored_ok, _stored_ver in rows:
            version = str(prompt_version or "?")
            subject = {"symbol": symbol, "company_name": company_name}
            if not subject_is_checkable(subject):
                # No subject to check against — NULL, not False. See
                # thesis_subject_identity.subject_is_checkable.
                verdicts.append((int(thesis_id), None))
                unchecked += 1
                continue
            ok = memo_names_subject(str(memo), subject)
            verdicts.append((int(thesis_id), ok))
            by_version[version] += 1
            if not ok:
                rejects_by_version[version] += 1
                if version in KNOWN_GOOD_VERSIONS:
                    false_positives.append((version, int(thesis_id), str(symbol)))

        print(f"rule version: {RULE_SET_VERSION}")
        print(f"rows scanned: {len(rows)}   unchecked (no usable subject): {unchecked}\n")
        print(f"{'ver':>4}  {'n':>5}  {'passes':>6}  {'REJECTS':>7}   reject-rate")
        for version in sorted(by_version):
            n = by_version[version]
            rejected = rejects_by_version[version]
            print(f"{version:>4}  {n:>5}  {n - rejected:>6}  {rejected:>7}   {rejected / n * 100.0:6.1f}%")

        print(f"\n=== FALSE-POSITIVE AUDIT: rejections on {'/'.join(sorted(KNOWN_GOOD_VERSIONS))} ===")
        if false_positives:
            for version, thesis_id, symbol in false_positives[:25]:
                print(f"  {version} thesis {thesis_id} [{symbol}]")
            print(f"\n  !! {len(false_positives)} FALSE POSITIVE(S) — the rule rejects memos the writer got RIGHT.")
            print("  !! NOTHING WRITTEN. Widen the accepted spellings until this is zero.")
            return 1
        checked_known_good = sum(by_version[v] for v in KNOWN_GOOD_VERSIONS if v in by_version)
        print(f"  NONE — 0 rejections across {checked_known_good} known-good memos.")

        # Verdict-equivalence: only rows whose stored verdict or rule version
        # disagrees with the current rule are written, so a re-run is a true
        # no-op and checked_at keeps meaning "when this was decided".
        stored = {int(r[0]): (r[5], r[6]) for r in rows}
        stale = [
            (thesis_id, ok)
            for thesis_id, ok in verdicts
            if stored[thesis_id] != ((ok, RULE_SET_VERSION) if ok is not None else (None, None))
        ]
        print(f"\nrows whose stored verdict/rule-version is stale: {len(stale)}")

        if not args.apply:
            print("DRY RUN — nothing written. Re-run with --apply.")
            return 0

        # ⚠ The WRITE is ensure_subject_identity_verdicts — the same function
        # app/main.py's lifespan probe calls. This script owns the
        # false-positive AUDIT above (which a human can act on) and nothing
        # else; duplicating the update here would let the two paths drift, and
        # the boot path is the one that actually runs unattended.
        with conn.transaction():
            written = ensure_subject_identity_verdicts(conn)
        print(f"WROTE {written} verdict(s).")
        return 0


if __name__ == "__main__":
    sys.exit(main())
