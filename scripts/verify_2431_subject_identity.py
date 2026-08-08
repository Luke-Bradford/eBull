"""Full-corpus verification of the thesis subject-identity gate (#2431).

    PYTHONPATH=. uv run python scripts/verify_2431_subject_identity.py

⚠ NOTHING IS WRITTEN. Reads every stored memo and reports whether it names its
own instrument. Gate on the EXIT CODE — 0 means the gate is still safe to ship,
1 means it has started rejecting memos from a population where the writer was
demonstrably correct.

⚠ NEVER PIPE THIS INTO ``head``/``tail`` — a pipe returns the pipe's status, so
a failure reads as success (`.claude/CLAUDE.md`). Redirect and read the file.

WHAT THE NUMBERS MEAN, AND WHICH ONE IS LOAD-BEARING
----------------------------------------------------
The rejection rate on v5/v6 is the DEFECT being measured, and it is not
evidence the gate is safe — a gate that rejects everything would score just as
well there. The load-bearing measurement is the FALSE-POSITIVE rate on **v1-v4**,
which are the prompt versions where the writer named the right company. A
narrowing gate is only safe if it rejects nothing correct
(`.claude/CLAUDE.md`: "a NARROWING gate → enumerate what it REJECTS").

Measured 2026-08-08 on the dev corpus:

    ver      n   REJECTS   reject-rate
     v1     25         0       0.0%
     v2     36         0       0.0%
     v3     71         0       0.0%
     v4    355         0       0.0%      <- 487 known-good memos, ZERO rejected
     v5   2038      1378      67.6%
     v6    127       127     100.0%

⚠ These move as the corpus grows. Re-run rather than citing them; that is the
whole reason this file exists rather than a number in a docstring.
"""

from __future__ import annotations

import sys
from collections import defaultdict

import psycopg

from app.config import settings
from app.services.thesis import _memo_names_subject

#: Prompt versions where the writer demonstrably named the right company, so a
#: rejection here is a FALSE POSITIVE and the gate must not ship.
KNOWN_GOOD_VERSIONS = frozenset({"v1", "v2", "v3", "v4"})

_SQL = """
    SELECT t.prompt_version, t.thesis_id, i.symbol, i.company_name, t.memo_markdown
    FROM theses t
    JOIN instruments i USING (instrument_id)
    WHERE t.memo_markdown IS NOT NULL
    ORDER BY t.prompt_version, t.thesis_id
"""


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_SQL).fetchall()

    buckets: dict[str, list[tuple[int, bool, str, str]]] = defaultdict(list)
    for prompt_version, thesis_id, symbol, company_name, memo in rows:
        subject = {"symbol": symbol, "company_name": company_name}
        buckets[str(prompt_version or "?")].append(
            (int(thesis_id), _memo_names_subject(str(memo), subject), str(symbol), str(memo)[:70].replace("\n", " "))
        )

    print(f"{'ver':>4}  {'n':>5}  {'passes':>6}  {'REJECTS':>7}   reject-rate")
    for version in sorted(buckets):
        entries = buckets[version]
        rejects = sum(1 for entry in entries if not entry[1])
        rate = rejects / len(entries) * 100.0
        print(f"{version:>4}  {len(entries):>5}  {len(entries) - rejects:>6}  {rejects:>7}   {rate:6.1f}%")

    false_positives = [
        (version, entry)
        for version, entries in buckets.items()
        if version in KNOWN_GOOD_VERSIONS
        for entry in entries
        if not entry[1]
    ]

    print(f"\n=== FALSE-POSITIVE AUDIT: rejections on {'/'.join(sorted(KNOWN_GOOD_VERSIONS))} ===")
    if not false_positives:
        checked = sum(len(buckets[v]) for v in KNOWN_GOOD_VERSIONS if v in buckets)
        print(f"  NONE — the gate rejects 0 of {checked} memos from the known-good populations.")
        return 0

    for version, (thesis_id, _ok, symbol, head) in false_positives[:25]:
        print(f"  {version} thesis {thesis_id} [{symbol}] {head}")
    print(f"\n  !! {len(false_positives)} FALSE POSITIVE(S). The gate rejects memos the writer got RIGHT.")
    print("  !! Do not ship it in this form — widen the accepted spellings until this is zero.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
