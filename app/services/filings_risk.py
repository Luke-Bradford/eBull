"""Filings risk-scorer (#1748) — derive ``filing_events.red_flag_score``.

The score is an eBull metric, but its inputs are fixed by settled
invariants: 8-K item severity from ``sec_8k_item_codes`` (sql/053) and
Form NT late-filing (SEC Rule 12b-25). See
``docs/specs/filings/2026-06-27-1748-filings-risk-scorer.md``.

We write ONLY genuine red flags, and ONLY at a high value:

  * a critical 8-K item  -> 1.0
  * a Form NT late filing -> 0.7
  * anything else         -> None (no red flag asserted)

Why never a low score: scoring's turnaround component is
``1.0 - avg_red_flag_score`` with a 0.5-neutral default when NULL
(``app/services/scoring.py``). Any non-null score below 0.5 would
*reward* the instrument relative to having no data. The scoring penalty
and the portfolio EXIT guard both aggregate over ``WHERE
red_flag_score IS NOT NULL`` (AVG > 0.60 / MAX >= 0.80), so scoring a
benign filing at, say, 0.1 would also dilute the average and suppress a
genuine critical signal. Leaving non-flags NULL keeps a lone critical at
avg = 1.0.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import psycopg

#: A critical 8-K item (bankruptcy, delisting, non-reliance/Item 4.02,
#: auditor change, material impairment, cyber incident, change of control,
#: failed distribution) -> top of scale.
CRITICAL_8K_SCORE = 1.0

#: Form NT late filing (Rule 12b-25). Above the scoring penalty threshold
#: (0.60) so a recent NT alone dings the score, but below the portfolio
#: EXIT threshold (0.80) so a missed deadline is not on its own
#: auto-exit-grade.
NT_LATE_FILING_SCORE = 0.7


def load_severity_by_code(conn: psycopg.Connection[Any]) -> dict[str, str]:
    """Load the settled 8-K item-code -> severity map (32 rows)."""
    with conn.cursor() as cur:
        cur.execute("SELECT code, severity FROM sec_8k_item_codes")
        return {str(code): str(sev) for code, sev in cur.fetchall()}


def score_filing_red_flag(
    filing_type: str | None,
    items: Sequence[str] | None,
    severity_by_code: Mapping[str, str],
) -> float | None:
    """Return the red-flag score for one filing, or ``None`` for no flag.

    Pure: callers supply ``severity_by_code`` (see ``load_severity_by_code``).
    ``items`` is the ``filing_events.items[]`` array (8-K item codes),
    ``None``/empty for non-8-K or unparsed 8-K rows.
    """
    ft = (filing_type or "").strip().upper()

    # Form NT (late filing). NT 10-K, NT 10-Q, NT 20-F, NT-NCSR,
    # NT NPORT-P, ... — "NT" followed by a space / hyphen / slash so a
    # hypothetical "NTxx" form can never match.
    if len(ft) >= 3 and ft.startswith("NT") and ft[2] in " /-":
        return NT_LATE_FILING_SCORE

    # 8-K / 8-K/A: flag iff any item is critical per the settled lookup.
    # Unknown codes contribute nothing (fail-closed: never a false
    # critical); a new SEC critical code is caught once sec_8k_item_codes
    # is updated.
    if ft.startswith("8-K") and items:
        if any(severity_by_code.get(code) == "critical" for code in items):
            return CRITICAL_8K_SCORE

    return None
