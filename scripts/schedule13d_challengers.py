"""Pure, frozen challenger selection for the sealed Schedule 13D study.

No function in this module imports the application or reads a price table.
Selections can therefore be reviewed before the outcome gate is opened.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final, Literal

PRICE_EDGES: Final = tuple(map(Decimal, ("5", "10", "25", "50", "100")))
LIQUIDITY_EDGES: Final = tuple(map(Decimal, ("10000000", "25000000", "100000000", "500000000")))
MARKET_RETURN_EDGES: Final = tuple(map(Decimal, ("-5", "0", "5")))
RULE_13G = Literal["1b", "1c", "both", "unknown"]


@dataclass(frozen=True)
class MatchFeatures:
    accession_number: str
    issuer_cik: str
    filing_date: date
    entry_date: date
    entry_price: Decimal
    trailing_median_dollar_volume: Decimal
    prior_20_market_return_pct: Decimal
    rule: RULE_13G | None = None


@dataclass(frozen=True)
class ChallengerMatch:
    treatment_accession: str
    challenger_accession: str
    rule: RULE_13G


def _bucket(value: Decimal, edges: tuple[Decimal, ...]) -> int:
    if not value.is_finite():
        raise ValueError("matching features must be finite")
    return sum(value >= edge for edge in edges)


def exact_stratum(item: MatchFeatures) -> tuple[int, int, int, int, int]:
    """Frozen year-month and three numeric matching cells."""

    return (
        item.filing_date.year,
        item.filing_date.month,
        _bucket(item.entry_price, PRICE_EDGES),
        _bucket(item.trailing_median_dollar_volume, LIQUIDITY_EDGES),
        _bucket(item.prior_20_market_return_pct, MARKET_RETURN_EDGES),
    )


def _tie_break(treatment_accession: str, challenger_accession: str, *, seed: int) -> str:
    payload = f"{treatment_accession}\x1f{challenger_accession}\x1f{seed}".encode()
    return hashlib.sha256(payload).hexdigest()


def match_initial_13g_without_replacement(
    treatments: Sequence[MatchFeatures],
    challengers: Sequence[MatchFeatures],
    *,
    rule: RULE_13G,
    seed: int = 2582,
) -> tuple[ChallengerMatch, ...]:
    """Greedily match exact cells in accession order with a SHA-256 tie-break.

    Rule populations are intentionally separate. Unknown never enters a known
    rule population, and a challenger accession can be consumed only once.
    Within an exact cell candidates are otherwise interchangeable, so the
    deterministic greedy order still obtains the maximum possible match count.
    """

    if rule not in ("1b", "1c", "both", "unknown"):
        raise ValueError("unsupported Schedule 13G rule")
    eligible = [item for item in challengers if item.rule == rule]
    by_stratum: dict[tuple[int, int, int, int, int], list[MatchFeatures]] = {}
    for item in eligible:
        by_stratum.setdefault(exact_stratum(item), []).append(item)
    used: set[str] = set()
    matches: list[ChallengerMatch] = []
    for treatment in sorted(treatments, key=lambda item: item.accession_number):
        candidates = [
            item for item in by_stratum.get(exact_stratum(treatment), ()) if item.accession_number not in used
        ]
        if not candidates:
            continue
        selected = min(
            candidates,
            key=lambda item: (
                _tie_break(treatment.accession_number, item.accession_number, seed=seed),
                item.accession_number,
            ),
        )
        used.add(selected.accession_number)
        matches.append(ChallengerMatch(treatment.accession_number, selected.accession_number, rule))
    return tuple(matches)


def select_random_time_sessions(
    treatment_sessions: Mapping[str, Sequence[date]],
    prohibited_sessions: Mapping[str, set[date]],
    *,
    seed: int = 2582,
) -> dict[str, date]:
    """Select one deterministic non-event entry session per treatment.

    The caller supplies all price-covered candidate sessions for the same
    instrument and treatment entry year-month. Sessions within the frozen
    exclusion halo are removed here. Empty sets remain explicitly unmatched.
    """

    selected: dict[str, date] = {}
    for accession in sorted(treatment_sessions):
        allowed = sorted(set(treatment_sessions[accession]) - prohibited_sessions.get(accession, set()))
        if not allowed:
            continue
        selected[accession] = min(
            allowed,
            key=lambda value: (
                hashlib.sha256(f"{accession}\x1f{value.isoformat()}\x1f{seed}".encode()).hexdigest(),
                value,
            ),
        )
    return selected
