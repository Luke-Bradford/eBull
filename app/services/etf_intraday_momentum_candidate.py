"""Frozen, data-compatible ETF intraday-momentum candidate (#2502).

Gao, Han, Li and Zhou measure the return from the prior regular-session close
to 10:00 New York and use its sign to trade the 15:30-16:00 interval.  Their
primary data are TAQ price points and their executable rule is long/short.

eBull currently has completed eToro OHLCV candles, not historical 15:30
bid/ask quotes or shortability.  This module therefore freezes two visibly
different things before reading retained outcomes:

* the published signed direction, retained only as a replication diagnostic;
* a long-only adaptation that fires when the opening return is positive.

Neither can receive capital from this module.  A candle-open/close gross
outcome is feasibility evidence only; promotion requires prospectively
observed entry/exit quotes and every standing evidence/execution gate.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final, Literal
from zoneinfo import ZoneInfo

from app.services.market_calendar import us_market_status
from app.services.strategy_observation_storage import IntradayBar

TRIAL_ID: Final = "etf-first-last-half-hour-long-only-v1"
PRIMARY_SYMBOL: Final = "SPY"
ROBUSTNESS_SYMBOLS: Final = ("QQQ", "IWM")
MIN_COMPLETE_PRIMARY_SESSIONS: Final = 60
_NY: Final = ZoneInfo("America/New_York")

PublishedSide = Literal["long", "short"]
AdaptationVerdict = Literal["fired_long", "not_fired"]


class CandidateRefusal(ValueError):
    """The supplied point-in-time observation cannot satisfy the trial."""


@dataclass(frozen=True)
class CandidateDefinition:
    paper_doi: str = "10.1016/j.jfineco.2018.05.009"
    primary_symbol: str = PRIMARY_SYMBOL
    robustness_symbols: tuple[str, ...] = ROBUSTNESS_SYMBOLS
    source: str = "etoro/<universe_version>/nyse_rth"
    timeframe: str = "30m"
    session: str = "NYSE regular full session; half-days excluded"
    opening_return: str = "09:30 candle close / prior full-session 15:30 candle close - 1"
    published_rule: str = "long if opening_return > 0 else short"
    executable_adaptation: str = "long if opening_return > 0 else abstain"
    decision_known_at: str = "10:00 America/New_York after completed opening candle"
    entry_observation: str = "15:30 America/New_York quote required prospectively"
    exit_observation: str = "16:00 America/New_York quote/confirmed close required prospectively"
    gross_feasibility_proxy: str = "15:30 candle close / 15:30 candle open - 1"
    minimum_complete_primary_sessions: int = MIN_COMPLETE_PRIMARY_SESSIONS
    primary_metric: str = "after-observed-cost expectancy with session-blocked lower confidence bound > 0"
    comparators: tuple[str, ...] = ("always-long-last-half-hour", "same-session-random-sign")
    promotion_refusals: tuple[str, ...] = (
        "sample_immature",
        "historical_entry_exit_quotes_unavailable",
        "published_short_leg_not_executable",
        "prospective_outcome_interval_missing",
    )


DEFINITION: Final = CandidateDefinition()


def definition_json() -> str:
    return json.dumps(asdict(DEFINITION), sort_keys=True, separators=(",", ":"))


def definition_hash() -> str:
    payload = definition_json().encode() + b"\0" + Path(__file__).read_bytes()
    return hashlib.sha256(payload).hexdigest()


CANDIDATE_VERSION: Final = f"etf-intraday-momentum-v1+{definition_hash()[:12]}"


@dataclass(frozen=True)
class OpeningSignal:
    symbol: str
    instrument_id: int
    session_date: date
    known_at: datetime
    prior_close: Decimal
    opening_close: Decimal
    opening_return: Decimal
    published_side: PublishedSide
    adaptation_verdict: AdaptationVerdict


@dataclass(frozen=True)
class GrossFeasibilityOutcome:
    signal: OpeningSignal
    entry_proxy: Decimal
    exit_proxy: Decimal
    gross_return: Decimal
    promotion_refusals: tuple[str, ...] = DEFINITION.promotion_refusals


def _require_bar(bar: IntradayBar, *, expected_start: time, label: str) -> None:
    if bar.timeframe != "30m":
        raise CandidateRefusal(f"{label} requires a 30m bar")
    if not is_eligible_source(bar.source):
        raise CandidateRefusal(f"{label} requires the frozen namespaced etoro RTH source")
    local = bar.bar_time.astimezone(_NY)
    if local.time().replace(tzinfo=None) != expected_start:
        raise CandidateRefusal(f"{label} must start at {expected_start.isoformat()} America/New_York")
    if us_market_status(local.date()) != "open":
        raise CandidateRefusal("candidate excludes closed and half-day sessions")


def is_eligible_source(source: str) -> bool:
    """Accept only the harvester's durable, universe-versioned RTH provenance."""
    parts = source.split("/")
    return len(parts) == 3 and parts[0] == "etoro" and bool(parts[1]) and parts[2] == "nyse_rth"


def opening_signal(
    *,
    symbol: str,
    prior_close_bar: IntradayBar,
    opening_bar: IntradayBar,
) -> OpeningSignal:
    """Create the 10:00 decision without accepting any later-session input."""
    normalised = symbol.strip().upper()
    if normalised not in {PRIMARY_SYMBOL, *ROBUSTNESS_SYMBOLS}:
        raise CandidateRefusal(f"symbol {normalised!r} is outside the preregistered ETF set")
    _require_bar(prior_close_bar, expected_start=time(15, 30), label="prior close observation")
    _require_bar(opening_bar, expected_start=time(9, 30), label="opening observation")
    local = opening_bar.bar_time.astimezone(_NY)
    prior_local = prior_close_bar.bar_time.astimezone(_NY)
    if prior_close_bar.instrument_id != opening_bar.instrument_id:
        raise CandidateRefusal("prior close and opening observation must share an instrument")
    probe = prior_local.date() + timedelta(days=1)
    while probe < local.date():
        if us_market_status(probe) != "closed":
            raise CandidateRefusal("prior close is not from the immediately preceding trading session")
        probe += timedelta(days=1)
    if probe != local.date():
        raise CandidateRefusal("prior close must precede the opening session")
    prior_close = prior_close_bar.close
    opening_return = opening_bar.close / prior_close - Decimal(1)
    published_side: PublishedSide = "long" if opening_return > 0 else "short"
    verdict: AdaptationVerdict = "fired_long" if opening_return > 0 else "not_fired"
    return OpeningSignal(
        symbol=normalised,
        instrument_id=opening_bar.instrument_id,
        session_date=local.date(),
        known_at=local.replace(hour=10, minute=0, second=0, microsecond=0),
        prior_close=prior_close,
        opening_close=opening_bar.close,
        opening_return=opening_return,
        published_side=published_side,
        adaptation_verdict=verdict,
    )


def resolve_gross_feasibility(
    signal: OpeningSignal,
    *,
    last_half_hour_bar: IntradayBar,
) -> GrossFeasibilityOutcome:
    """Resolve the candle proxy; never rename it an executable net return."""
    _require_bar(last_half_hour_bar, expected_start=time(15, 30), label="last-half-hour observation")
    local = last_half_hour_bar.bar_time.astimezone(_NY)
    if local.date() != signal.session_date:
        raise CandidateRefusal("opening signal and last-half-hour observation must share a session")
    if last_half_hour_bar.instrument_id != signal.instrument_id:
        raise CandidateRefusal("opening signal and last-half-hour observation must share an instrument")
    gross = last_half_hour_bar.close / last_half_hour_bar.open - Decimal(1)
    if signal.adaptation_verdict == "not_fired":
        gross = Decimal(0)
    return GrossFeasibilityOutcome(
        signal=signal,
        entry_proxy=last_half_hour_bar.open,
        exit_proxy=last_half_hour_bar.close,
        gross_return=gross,
    )


__all__ = [
    "CANDIDATE_VERSION",
    "DEFINITION",
    "MIN_COMPLETE_PRIMARY_SESSIONS",
    "PRIMARY_SYMBOL",
    "ROBUSTNESS_SYMBOLS",
    "CandidateRefusal",
    "GrossFeasibilityOutcome",
    "OpeningSignal",
    "definition_hash",
    "definition_json",
    "is_eligible_source",
    "opening_signal",
    "resolve_gross_feasibility",
]
