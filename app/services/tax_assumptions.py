"""The assumptions stated beside every tax figure (#3616 slice 3d-4a, Design 5).

One constant module: the tax view, the budget reserve and their API responses read the definitions from here. Each
sentence is copied from the spec that names the assumption:
- A1, A4: ``docs/specs/metrics/2026-10-14-3616-slice3d1-consideration-basis.md`` ("Consideration basis");
- A5: ``…2026-10-14-3616-slice3d2-statutory-matching.md`` (Source rules, residence);
- A8: ``…2026-10-14-3616-slice3d3b-position-income.md``;
- F1, F3–F9: ``…2026-10-15-3616-slice3d3c3-fund-disposals.md`` (Source rules);
- L1–L7: ``…2026-10-15-3616-slice3d4a-tax-year-view.md`` (Design 5).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

ASSUMPTIONS: Final[dict[str, str]] = {
    "A1": "The amount received on a disposal is the broker's investment plus net profit, in USD; no documented field "
    "is the amount received.",
    "A4": "Any spread is inside the executed price and so inside the investment, not debited separately.",
    "A5": "The account holder was UK resident at every acquisition.",
    "A8": "A distribution observed on a position is income, not a capital distribution; the broker's field does not "
    "say which kind it is.",
    "F1": "No remittance-basis year applies to a disposal before 2025/26 and no foreign income and gains claim exists "
    "from 2025/26; an offshore income gain is stored as the gross amount arising.",
    "F3": "No other allowable expenditure was incurred, no incidental cost beyond the consideration basis, no sum in "
    "the consideration is charged to income tax otherwise than under the offshore funds rules, and no expenditure "
    "counted is deductible for income tax.",
    "F4": "The account holder is an individual holding the units as investments, not as trading stock or in the "
    "course of a trade, and acquired each by an ordinary broker purchase, not under a written agreement entered into "
    "before 30 April 2009.",
    "F5": "The account holder was UK resident and not Treaty non-resident at every acquisition.",
    "F6": "Each close of a fund position is a separate disposal, not a fragment of one bargain recorded as several "
    "closes.",
    "F7": "The fund interest is not a loan, and the fund's sole or main purpose is not investing in qualifying "
    "unlisted companies, as reviewed for the fund's determination.",
    "F8": "The units apportionment of the fund holding holds.",
    "F9": "No acquisition, disposal or holding of the class outside this account, in the same capacity, affects "
    "identification.",
    "L1": "No allowable losses are brought forward from earlier years; none are evidenced.",
    "L2": "Each loss in the computation is allowable: notified within the TMA 1970 s43(1) time limit (TCGA s16(2A)), "
    "and not restricted by s16A.",
    "L3": "For the year the individual makes no foreign gain claim, foreign income claim or foreign employment "
    "election, so the annual exempt amount applies (TCGA s1K(6)).",
    "L4": "The person's unused basic-rate band is unknown, so tax is shown as two scenarios: every taxable gain inside "
    "the band, and none inside it.",
    "L5": "The person has no other gains, losses or annual exempt amount use in the year.",
    "L6": "No loss is restricted to particular gains and no foreign tax credit relief applies, so deducting from the "
    "highest-rate gains first is the most beneficial allocation and the scenarios are gross.",
    "L7": "The individual is UK resident for each disposal's tax year, not in a split year, not temporarily "
    "non-resident and not Treaty non-resident.",
}

# Stated beside every figure of this slice. A8 is added only where a figure rests on a lot carrying its reason.
ALWAYS_STATED: Final[tuple[str, ...]] = (
    "A1",
    "A4",
    "A5",
    "F1",
    "F3",
    "F4",
    "F5",
    "F6",
    "F7",
    "F8",
    "F9",
    "L1",
    "L2",
    "L3",
    "L4",
    "L5",
    "L6",
    "L7",
)

DISTRIBUTION_CHARACTER_REASON: Final = "distribution_character_assumed"


def assumption_ids(reasons: Iterable[str]) -> tuple[str, ...]:
    """The assumptions stated beside a figure whose rows carry ``reasons``."""
    if DISTRIBUTION_CHARACTER_REASON in set(reasons):
        return (*ALWAYS_STATED, "A8")
    return ALWAYS_STATED
