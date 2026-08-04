"""Price-quarantine census endpoint (#2261, phase 0a of #2240).

``GET /price-quarantine/census`` — the operator-visible rejection census.

WHY THIS IS AN ENDPOINT AND NOT A NUMBER IN A PR DESCRIPTION. T3 is the only
quarantine rule that can reject legitimate data, and it does so at roughly 10:1
against split-like breaks at every threshold, while turnover corroboration
reaches only ~30% of the population (volume is equity-only, S3 #2243). "No
volume -> quarantine" therefore embeds an asset-class bias against non-equity
and illiquid names. Every backtest win rate the platform ever reports inherits
that bias, so the figure that discloses it has to be live and standing, not a
one-off in a spike comment.

Auth: operator-only, mounted on the router — the census exposes data-quality
gaps across the universe.
"""

from __future__ import annotations

import psycopg
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.price_quarantine_store import census

router = APIRouter(
    prefix="/price-quarantine",
    tags=["price-quarantine"],
    dependencies=[Depends(require_session_or_service_token)],
)


class CensusResponse(BaseModel):
    rule_set_version: str
    instruments_evaluated: int
    bars_evaluated: int
    transitions_evaluated: int

    bars_return_unusable: int
    bars_range_unusable: int
    bars_provisional: int
    bar_rule_counts: dict[str, int]
    """Per-rule bar rejections. B1/B4 -> both verdicts false; B2/B3 -> range only."""

    transitions_quarantined: int
    transition_rule_counts: dict[str, int]
    transitions_provisional_deferred: int
    """Transitions that CROSSED the T3 magnitude threshold but touch a bar inside
    the trailing correction window. T3's corroboration reads volume, and a
    part-session bar's volume is a part-session count, so the verdict is DEFERRED
    rather than decided — not quarantined, not admitted, and recomputed once the
    bar is final. Ordinary recent transitions that never approached the threshold
    are NOT counted here: they are provisional too, but they have nothing
    deferred about them."""

    t3_corroboration: dict[str, int]
    """The narrowing-gate census: every transition whose magnitude triggered T3,
    split by what turnover said. ``spike`` was admitted back; ``flat``,
    ``collapse`` and ``unclassifiable`` were quarantined. ``unclassifiable``
    dominates and is the bias to publish — it is mostly "no volume either side",
    not "we looked and it was wrong"."""

    instruments_with_unresolved_break: int
    bars_stranded_pre_break: int
    """Bars sitting before an instrument's last unresolved break, in a unit
    regime that cannot be joined to the current one without a factor. Marked,
    never dropped — silent exclusion biases the eligible universe."""

    stale_version_instruments: int
    """Evaluated at an older rule set. Their bars read as UNKNOWN, not usable."""


@router.get("/census", response_model=CensusResponse)
def get_census(conn: psycopg.Connection = Depends(get_conn)) -> CensusResponse:  # type: ignore[type-arg]
    """Rejection census over the currently stored verdicts.

    Counted over what ``price_quarantine.evaluate_series`` actually wrote —
    there is deliberately no second SQL expression of the rules to drift from
    the prose (the failure mode S7's Codex pass found twice in one document).
    """
    return CensusResponse(**vars(census(conn)))
