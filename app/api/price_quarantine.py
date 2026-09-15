"""Price-quarantine census endpoint (#2261, phase 0a of #2240).

``GET /price-quarantine/census`` — the operator-visible rejection census.

WHY THIS IS AN ENDPOINT AND NOT A NUMBER IN A PR DESCRIPTION. T3 is the only
quarantine rule that can reject legitimate data, and it does so at roughly 10:1
against split-like breaks at every threshold, while turnover corroboration
reaches only a minority of the trigger population. Every backtest win rate the
platform ever reports inherits that bias, so the figure that discloses it has to
be live and standing, not a one-off in a spike comment.

⚠⚠ WHAT THAT MINORITY IS DRIVEN BY — AND THE CAUSE THIS FILE USED TO GIVE, WHICH
IS SUPERSEDED (#3046 residual 3). The line here read *"reaches only ~30% of the
population (volume is equity-only, S3 #2243)"*, and the same sentence still
stands verbatim at ``price_quarantine.py:30,430`` and
``sql/247_price_quarantine.sql:102``. Both of those are immutable for independent
reasons — the rule module's source is hashed into ``RULE_SET_VERSION`` and thence
into ``strategy_registry.INPUT_RULE_SETS``, and an applied migration's content is
sha256-checked at boot (``app/db/migrations.py:188``) — so the correction lands
HERE, at the publisher, and supersedes them. Measured on the full corpus, no
figure repeated by hand:

    PYTHONPATH=. uv run python -m scripts.verify_3046_t3_corroboration_ceiling

- "Equity-only" is a real per-instrument TENDENCY and is NOT the explanation. The
  structurally volume-free classes do have a far higher all-null rate than the
  equity classes, but they are a small minority of instruments and the T3 trigger
  population is overwhelmingly ONE equity class.
- The dominant carrier of blindness is instruments that DO report volume
  elsewhere — volume-free RUNS inside an otherwise-covered series. The
  never-any-volume instruments and the pre-onset era are 100% blind but are both
  SMALL, so they set the floor, not the total.
- ⚠ **Blindness is NOT uniform, and an earlier draft of this paragraph said it
  was.** The narrow ~67-78% band holds only for the two cohort splits in the
  script's arm 6 (series ended vs live, symbol cohort). Across ASSET CLASS and
  across the ONSET ERA the spread is wide — one equity class runs far blinder
  than another, and the never-volume and pre-onset strata are totally blind.
  Excluding the never-volume stratum alone moves reachability by several points,
  so "no sub-population whose exclusion would raise it" is false. Read the arms;
  do not carry a single band.
- "Reachable" and "admitted back" are DIFFERENT figures — ``collapse`` and
  ``flat`` are classified and still quarantined. ``t3_corroboration`` below
  carries both; do not quote one as the other.
- The blind set is measured with PROVISIONAL transitions excluded. A forming bar
  is stored ``unclassifiable`` without its volume ever being read
  (``price_quarantine.py:486-492`` defers the verdict), so counting it as
  blindness would attribute a deferral to a data gap.

The bias disclosure therefore stands, and is larger than the superseded sentence
claimed. What changes is the CAUSE a reader should carry away from it.

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
