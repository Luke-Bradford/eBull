"""Pure-logic tests for the filings risk-scorer (#1748).

No DB — the severity map is supplied directly (mirrors the settled
``sec_8k_item_codes`` seed in sql/053).
"""

from __future__ import annotations

import pytest

from app.services.filings_risk import (
    CRITICAL_8K_SCORE,
    NT_LATE_FILING_SCORE,
    score_filing_red_flag,
)

# Subset of the settled sec_8k_item_codes seed (sql/053).
SEVERITY = {
    "1.01": "material",  # Material Definitive Agreement
    "1.03": "critical",  # Bankruptcy
    "4.02": "critical",  # Non-Reliance / restatement
    "7.01": "informational",  # Reg FD
    "9.01": "informational",  # Exhibits
    "8.01": "informational",  # Other Events
}


@pytest.mark.parametrize(
    "filing_type,items,expected",
    [
        # 8-K critical present -> 1.0 (worst item drives it)
        ("8-K", ["1.03"], CRITICAL_8K_SCORE),
        ("8-K", ["4.02"], CRITICAL_8K_SCORE),
        ("8-K/A", ["1.03"], CRITICAL_8K_SCORE),
        ("8-K", ["7.01", "1.03", "9.01"], CRITICAL_8K_SCORE),  # critical among benign
        # 8-K with no critical item -> NULL (not a red flag; avoids reward+dilution)
        ("8-K", ["1.01"], None),  # material only
        ("8-K", ["7.01", "9.01"], None),  # informational only
        ("8-K", [], None),  # parsed, no items
        ("8-K", None, None),  # items not yet applied
        ("8-K", ["6.99"], None),  # unknown code only -> fail-closed
        # Form NT late filings -> 0.7
        ("NT 10-K", None, NT_LATE_FILING_SCORE),
        ("NT 10-Q", None, NT_LATE_FILING_SCORE),
        ("NT 20-F", None, NT_LATE_FILING_SCORE),
        ("NT-NCSR", None, NT_LATE_FILING_SCORE),
        ("NT NPORT-P", None, NT_LATE_FILING_SCORE),
        ("nt 10-k", None, NT_LATE_FILING_SCORE),  # case-insensitive
        # Routine / non-risk forms -> NULL
        ("10-K", None, None),
        ("10-Q", None, None),
        ("4", None, None),
        ("DEF 14A", None, None),
        ("SC 13G", None, None),
        ("", None, None),
        (None, None, None),
        # "NT"-prefixed but not a real NT form must NOT match (guard on the
        # separator char after NT).
        ("NTRS", None, None),
    ],
)
def test_score_filing_red_flag(filing_type, items, expected):
    assert score_filing_red_flag(filing_type, items, SEVERITY) == expected


def test_only_ever_writes_high_scores_or_none():
    """Invariant the whole model rests on: a written score is never below
    0.5, so it can never *reward* turnaround (1.0 - avg) vs the 0.5-neutral
    NULL default."""
    cases = [
        ("8-K", ["1.03"], SEVERITY),
        ("NT 10-K", None, SEVERITY),
        ("8-K", ["1.01"], SEVERITY),
        ("10-Q", None, SEVERITY),
    ]
    for ft, items, sev in cases:
        score = score_filing_red_flag(ft, items, sev)
        assert score is None or score >= 0.7
