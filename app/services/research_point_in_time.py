"""Fail-closed point-in-time authorization for R6 historical rankings (#2900).

There is deliberately no ranking reader in this module.  The frozen #2900
inventory found no current input family satisfying all four admissibility
conditions, so making a SQL callback injectable here would turn the guard into
an honour system.  A future reader must live in the private dispatch table
beside an identity whose evidence matrix has moved to eligible.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Literal

from app.services.market_calendar import us_market_status
from app.services.universe_selection import INTRADER_CAPTURE_DATE


class RankingFamily(StrEnum):
    RESEARCH_PRICES = "research_prices"
    FUNDAMENTAL_FACTS = "fundamental_facts"
    DERIVED_FUNDAMENTALS = "derived_fundamentals"
    DIMENSIONAL_XBRL = "dimensional_xbrl"
    OWNERSHIP_OBSERVATIONS = "ownership_observations"
    FILING_RED_FLAGS = "filing_red_flags"
    FINRA_SHORT_INTEREST = "finra_short_interest"
    LIVE_ETORO_STATE = "live_etoro_state"
    HISTORICAL_POPULATION = "historical_population"


class R6RankingIdentity(StrEnum):
    DILUTION_RED_FLAGS = "2908-dilution-red-flags"
    SHORT_INTEREST = "2913-short-interest"
    QUALITY = "2901-quality"
    SHAREHOLDER_YIELD = "2917-shareholder-yield"
    MOMENTUM = "2916-momentum"
    DEFENSIVE = "2910-defensive"
    VALUATION = "2902-valuation"
    OWNERSHIP = "2903-ownership"
    COMBINATION = "2904-combination"
    MACHINE_LEARNING = "2911-machine-learning"


Condition = Literal["public_clock", "system_versions", "historical_population", "causal_transform"]
Outcome = Literal["pass", "fail"]


@dataclass(frozen=True)
class ConditionEvidence:
    outcome: Outcome
    probes: tuple[str, ...]
    qualification: str | None = None


@dataclass(frozen=True)
class FieldVerdict:
    status: Literal["refused"]
    reason: str


def _cell(outcome: Outcome, *probes: str, qualification: str | None = None) -> ConditionEvidence:
    if not probes:
        raise ValueError("condition evidence requires at least one probe")
    return ConditionEvidence(outcome=outcome, probes=tuple(probes), qualification=qualification)


PROBE_MATRIX: Final[Mapping[RankingFamily, Mapping[Condition, ConditionEvidence]]] = MappingProxyType(
    {
        RankingFamily.RESEARCH_PRICES: MappingProxyType(
            {
                "public_clock": _cell("fail", "P1"),
                "system_versions": _cell("fail", "P1"),
                "historical_population": _cell("fail", "P2", "P5"),
                "causal_transform": _cell("fail", "P3", "P4"),
            }
        ),
        RankingFamily.FUNDAMENTAL_FACTS: MappingProxyType(
            {
                "public_clock": _cell("pass", "F0"),
                "system_versions": _cell("fail", "F1", "F2"),
                "historical_population": _cell("fail", "P2", "P5"),
                "causal_transform": _cell("pass", "F0"),
            }
        ),
        RankingFamily.DERIVED_FUNDAMENTALS: MappingProxyType(
            {
                condition: _cell("fail", "D1")
                for condition in (
                    "public_clock",
                    "system_versions",
                    "historical_population",
                    "causal_transform",
                )
            }
        ),
        RankingFamily.DIMENSIONAL_XBRL: MappingProxyType(
            {
                condition: _cell("fail", "X1")
                for condition in (
                    "public_clock",
                    "system_versions",
                    "historical_population",
                    "causal_transform",
                )
            }
        ),
        RankingFamily.OWNERSHIP_OBSERVATIONS: MappingProxyType(
            {
                "public_clock": _cell("fail", "O2"),
                "system_versions": _cell("fail", "O1", "O2", "O3"),
                "historical_population": _cell("fail", "H2", "H3"),
                "causal_transform": _cell("fail", "O3"),
            }
        ),
        RankingFamily.FILING_RED_FLAGS: MappingProxyType(
            {
                "public_clock": _cell("pass", "R0"),
                "system_versions": _cell("fail", "R1"),
                "historical_population": _cell("fail", "H2", "H3"),
                "causal_transform": _cell("fail", "R1"),
            }
        ),
        RankingFamily.FINRA_SHORT_INTEREST: MappingProxyType(
            {
                "public_clock": _cell("fail", "N1"),
                "system_versions": _cell("fail", "N1"),
                "historical_population": _cell("fail", "N2", "H2", "H3"),
                "causal_transform": _cell("pass", "N0"),
            }
        ),
        RankingFamily.LIVE_ETORO_STATE: MappingProxyType(
            {
                "public_clock": _cell("fail", "L1"),
                "system_versions": _cell("fail", "L1"),
                "historical_population": _cell("fail", "L1", "H1"),
                "causal_transform": _cell("fail", "L1"),
            }
        ),
        RankingFamily.HISTORICAL_POPULATION: MappingProxyType(
            {
                "public_clock": _cell("fail", "H1"),
                "system_versions": _cell("pass", "H2", qualification="prospective coverage only"),
                "historical_population": _cell("fail", "H1", "H2", "H3"),
                "causal_transform": _cell("pass", "H2", qualification="prospective coverage only"),
            }
        ),
    }
)


_REASONS: Final[Mapping[RankingFamily, str]] = MappingProxyType(
    {
        RankingFamily.RESEARCH_PRICES: "no publication vintage; current/eventual population and non-causal transforms",
        RankingFamily.FUNDAMENTAL_FACTS: "same-accession updates and destructive filing retention",
        RankingFamily.DERIVED_FUNDAMENTALS: "rebuilt/current winner state lacks a historical public version",
        RankingFamily.DIMENSIONAL_XBRL: "stored facts omit the dimension/member identity",
        RankingFamily.OWNERSHIP_OBSERVATIONS: "writers overwrite or supersede without retaining prior payload versions",
        RankingFamily.FILING_RED_FLAGS: "mutable event/classifier state has no historical scorer version",
        RankingFamily.FINRA_SHORT_INTEREST: "settlement is mislabelled as publication and revisions overwrite",
        RankingFamily.LIVE_ETORO_STATE: "live broker state is not historical evidence",
        RankingFamily.HISTORICAL_POPULATION: (
            "forward membership begins after the frozen archive and has unknown imports"
        ),
    }
)

FIELD_REGISTRY: Final[Mapping[RankingFamily, FieldVerdict]] = MappingProxyType(
    {family: FieldVerdict(status="refused", reason=_REASONS[family]) for family in RankingFamily}
)

_ALL_FAMILIES = frozenset(RankingFamily)
IDENTITY_FAMILIES: Final[Mapping[R6RankingIdentity, frozenset[RankingFamily]]] = MappingProxyType(
    {
        R6RankingIdentity.DILUTION_RED_FLAGS: frozenset(
            {
                RankingFamily.FUNDAMENTAL_FACTS,
                RankingFamily.DERIVED_FUNDAMENTALS,
                RankingFamily.DIMENSIONAL_XBRL,
                RankingFamily.FILING_RED_FLAGS,
                RankingFamily.HISTORICAL_POPULATION,
            }
        ),
        R6RankingIdentity.SHORT_INTEREST: frozenset(
            {
                RankingFamily.FINRA_SHORT_INTEREST,
                RankingFamily.FUNDAMENTAL_FACTS,
                RankingFamily.HISTORICAL_POPULATION,
            }
        ),
        R6RankingIdentity.QUALITY: frozenset(
            {
                RankingFamily.FUNDAMENTAL_FACTS,
                RankingFamily.DERIVED_FUNDAMENTALS,
                RankingFamily.HISTORICAL_POPULATION,
            }
        ),
        R6RankingIdentity.SHAREHOLDER_YIELD: frozenset(
            {
                RankingFamily.FUNDAMENTAL_FACTS,
                RankingFamily.DERIVED_FUNDAMENTALS,
                RankingFamily.HISTORICAL_POPULATION,
            }
        ),
        R6RankingIdentity.MOMENTUM: frozenset({RankingFamily.RESEARCH_PRICES, RankingFamily.HISTORICAL_POPULATION}),
        R6RankingIdentity.DEFENSIVE: frozenset({RankingFamily.RESEARCH_PRICES, RankingFamily.HISTORICAL_POPULATION}),
        R6RankingIdentity.VALUATION: frozenset(
            {
                RankingFamily.FUNDAMENTAL_FACTS,
                RankingFamily.DERIVED_FUNDAMENTALS,
                RankingFamily.DIMENSIONAL_XBRL,
                RankingFamily.LIVE_ETORO_STATE,
                RankingFamily.HISTORICAL_POPULATION,
            }
        ),
        R6RankingIdentity.OWNERSHIP: frozenset(
            {RankingFamily.OWNERSHIP_OBSERVATIONS, RankingFamily.HISTORICAL_POPULATION}
        ),
        R6RankingIdentity.COMBINATION: _ALL_FAMILIES,
        R6RankingIdentity.MACHINE_LEARNING: _ALL_FAMILIES,
    }
)


def _registry_payload() -> dict[str, object]:
    return {
        "families": {
            family.value: {
                "reason": FIELD_REGISTRY[family].reason,
                "status": FIELD_REGISTRY[family].status,
                "conditions": {
                    condition: {
                        "outcome": evidence.outcome,
                        "probes": list(evidence.probes),
                        "qualification": evidence.qualification,
                    }
                    for condition, evidence in sorted(PROBE_MATRIX[family].items())
                },
            }
            for family in sorted(RankingFamily, key=lambda item: item.value)
        },
        "identities": {
            identity.value: sorted(family.value for family in IDENTITY_FAMILIES[identity])
            for identity in sorted(R6RankingIdentity, key=lambda item: item.value)
        },
    }


REGISTRY_VERSION: Final = (
    "r6-pit-registry-v1+"
    + hashlib.sha256(json.dumps(_registry_payload(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]
)


@dataclass(frozen=True)
class R6RankingRequest:
    identity: R6RankingIdentity
    decision_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.identity, R6RankingIdentity):
            raise ValueError(f"unknown R6 ranking identity: {self.identity!r}")
        if not isinstance(self.decision_session, date):
            raise ValueError("decision_session must be a date")


class PointInTimeUnavailableError(RuntimeError):
    """The requested historical ranking has no admissible PIT input path."""


def source_date_is_public(source_public_date: date, *, decision_session: date) -> bool:
    """Date-resolution sources are usable only strictly before the decision."""
    return source_public_date < decision_session


def execute_r6_ranking(request: R6RankingRequest | None) -> None:
    """Authorize and dispatch one R6 ranking, or refuse before SQL is possible."""
    if request is None:
        raise PointInTimeUnavailableError("R6 ranking requires a non-empty typed request")
    if not isinstance(request, R6RankingRequest):
        raise PointInTimeUnavailableError("R6 ranking requires a non-empty typed request")
    if us_market_status(request.decision_session) == "closed":
        raise PointInTimeUnavailableError(f"decision_session {request.decision_session} is a closed NYSE date")

    families = IDENTITY_FAMILIES[request.identity]
    if RankingFamily.RESEARCH_PRICES in families and request.decision_session > INTRADER_CAPTURE_DATE:
        raise PointInTimeUnavailableError(
            f"decision_session {request.decision_session} is after research-price capture {INTRADER_CAPTURE_DATE}"
        )

    refusals = "; ".join(
        f"{family.value}: {FIELD_REGISTRY[family].reason}" for family in sorted(families, key=lambda item: item.value)
    )
    raise PointInTimeUnavailableError(
        f"{request.identity.value}: no admissible historical read path under {REGISTRY_VERSION}; {refusals}"
    )


__all__ = [
    "FIELD_REGISTRY",
    "IDENTITY_FAMILIES",
    "PROBE_MATRIX",
    "REGISTRY_VERSION",
    "ConditionEvidence",
    "FieldVerdict",
    "PointInTimeUnavailableError",
    "R6RankingIdentity",
    "R6RankingRequest",
    "RankingFamily",
    "execute_r6_ranking",
    "source_date_is_public",
]
