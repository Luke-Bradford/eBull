"""AI-discretionary-v1 shortlist + pack, pure half (#3471 slice 1b-ii).

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §3.1 (shortlist),
§3.2 (pack) and obligations O1 (per-name validity), O2 (knowledge time) and O3 (determinism).

Everything here is pure: the job (slice 1b-iii) reads the rows at ``as_of`` and hands them in.
A name that fails a check is INCOMPLETE and drops from both legs' universe (§3 step 2) — the
functions return a reason code rather than repairing the input.

⚠ The constants are by construction (spec §2), frozen into the declaration by hash.

⚠ Indicators REUSE ``app/services/indicator_series`` (Wilder RSI/ATR seeded with the simple
mean of the first 14, SMA). Its ATR takes the first true range at bar 1 (it needs the prior
close); O1's "first true range uses high − low" names a bar-0 seed instead. On the ≥ 60 bars a
complete name has, the seed's weight in the latest ATR is at most (13/14)^46 ≈ 3%, and at the
usual 260 bars it is below 1e-8. The house function wins on the reuse rule, and the
declaration freezes it by name.
"""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

from app.services.indicator_series import BarSeries, OHLCVRow, atr_series, rsi_series, sma_series

#: §3.1 — frozen shortlist terms.
TOP_N: Final = 30
SMALL_CAP_N: Final = 20
SMALL_CAP_MAX_USD: Final = Decimal("2000000000")
MIN_BID: Final = Decimal("3")
MAX_SPREAD_FRACTION: Final = Decimal("0.01")
QUOTE_MAX_AGE: Final = timedelta(hours=24)

#: §3.2 — frozen pack terms.
MIN_BARS: Final = 60
PROMPT_BARS: Final = 60
INDICATOR_BARS: Final = 260
SMA_PERIODS: Final = (20, 50, 200)
WILDER_PERIOD: Final = 14
VOLUME_RATIO_WINDOW: Final = 20
VWAP_WINDOW: Final = 20
DISCLOSURE_LIMIT: Final = 5
DISCLOSURE_WINDOW: Final = timedelta(days=30)
TITLE_MAX_CHARS: Final = 200

BarIncomplete = Literal[
    "too_few_bars",
    "duplicate_session",
    "not_ascending",
    "stale_last_bar",
    "non_finite_ohlcv",
    "bar_range_inconsistent",
    "negative_volume",
]


# ---------------------------------------------------------------------------
# §3.1 shortlist
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ShortlistCandidate:
    """One tradable US-equity name as read at ``as_of``.

    ``market_cap_usd`` is ``instrument_valuation.market_cap_live`` with the #1664 overlay the
    scorer applies (``scoring._apply_market_cap_basis``), resolved by the caller.
    """

    instrument_id: int
    symbol: str
    bid: Decimal | None
    ask: Decimal | None
    quoted_at: datetime | None
    total_score: float | None
    market_cap_usd: Decimal | None


@dataclass(frozen=True)
class ShortlistName:
    instrument_id: int
    symbol: str
    total_score: float
    slice: Literal["top", "small_cap"]


@dataclass(frozen=True)
class Shortlist:
    names: tuple[ShortlistName, ...]
    #: Recorded daily (§3.1 "the run records its own eligible count").
    eligible_count: int


def _finite_positive(value: Decimal | float | None) -> bool:
    if value is None:
        return False
    if isinstance(value, Decimal):
        return value.is_finite() and value > 0
    return math.isfinite(value) and value > 0


def is_eligible(candidate: ShortlistCandidate, *, as_of: datetime) -> bool:
    """§3.1 eligibility: fresh quote, ``bid ≥ 3``, ``ask ≥ bid``, spread ≤ 1% of mid, and a
    finite positive score (O1)."""
    bid, ask, quoted_at = candidate.bid, candidate.ask, candidate.quoted_at
    if quoted_at is None or not (as_of - QUOTE_MAX_AGE < quoted_at <= as_of):
        return False
    if not (_finite_positive(bid) and _finite_positive(ask)):
        return False
    assert bid is not None and ask is not None
    if bid < MIN_BID or ask < bid:
        return False
    if (ask - bid) / ((ask + bid) / 2) > MAX_SPREAD_FRACTION:
        return False
    return _finite_positive(candidate.total_score)


def select_shortlist(candidates: Iterable[ShortlistCandidate], *, as_of: datetime) -> Shortlist:
    """Top 30 by ``total_score``, plus the top 20 of the REMAINING names with a market cap
    under $2B. Ties break by ``instrument_id`` ascending. A NULL or non-finite cap excludes a
    name from the small-cap slice only."""
    eligible = [c for c in candidates if is_eligible(c, as_of=as_of)]
    ids = [c.instrument_id for c in eligible]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate instrument_id among shortlist candidates")
    # `total_score` is not None here: `is_eligible` required it finite and positive.
    ranked = sorted(eligible, key=lambda c: (-(c.total_score or 0.0), c.instrument_id))
    top = ranked[:TOP_N]
    rest = [
        c
        for c in ranked[TOP_N:]
        if _finite_positive(c.market_cap_usd) and (c.market_cap_usd or Decimal(0)) < SMALL_CAP_MAX_USD
    ]
    names = tuple(ShortlistName(c.instrument_id, c.symbol, c.total_score or 0.0, "top") for c in top) + tuple(
        ShortlistName(c.instrument_id, c.symbol, c.total_score or 0.0, "small_cap") for c in rest[:SMALL_CAP_N]
    )
    return Shortlist(names, len(eligible))


# ---------------------------------------------------------------------------
# §3.2 bars + indicators (O1)
# ---------------------------------------------------------------------------
def _finite_number(value: object) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, Decimal):
        return value.is_finite()
    if isinstance(value, int | float):
        return math.isfinite(value)
    return False


def build_bar_series(
    dates: Sequence[date], rows: Sequence[Mapping[str, Any]], *, last_session: date
) -> BarSeries | BarIncomplete:
    """The last ``INDICATOR_BARS`` bars as a validated ``BarSeries``, or why the name is
    incomplete (O1). ``last_session`` is the global last completed NYSE session."""
    if len(dates) != len(rows):
        raise ValueError(f"{len(dates)} dates for {len(rows)} rows")
    dates, rows = dates[-INDICATOR_BARS:], rows[-INDICATOR_BARS:]
    if len(dates) < MIN_BARS:
        return "too_few_bars"
    for i in range(1, len(dates)):
        if dates[i] == dates[i - 1]:
            return "duplicate_session"
        if dates[i] < dates[i - 1]:
            return "not_ascending"
    if dates[-1] != last_session:
        return "stale_last_bar"
    for row in rows:
        values = [row.get(k) for k in ("open", "high", "low", "close", "volume")]
        if not all(_finite_number(v) for v in values):
            return "non_finite_ohlcv"
        o, h, low, c = (Decimal(str(row[k])) for k in ("open", "high", "low", "close"))
        if low > min(o, c) or h < max(o, c):
            return "bar_range_inconsistent"
        if row["volume"] < 0:
            return "negative_volume"
    return BarSeries(
        tuple(dates),
        tuple(
            OHLCVRow(
                open=Decimal(str(r["open"])),
                high=Decimal(str(r["high"])),
                low=Decimal(str(r["low"])),
                close=Decimal(str(r["close"])),
                volume=int(r["volume"]),
            )
            for r in rows
        ),
    )


def _last(values: Sequence[float | None]) -> float | None:
    return values[-1] if values else None


def indicators(series: BarSeries) -> dict[str, float | None]:
    """Latest-bar indicators over a ``build_bar_series`` result. A zero denominator gives
    ``None`` (O1); SMA200 is ``None`` below 200 bars (§3.2)."""
    out: dict[str, float | None] = {
        f"sma{p}": _last(sma_series(series, universe="survivor_only", period=p).values) if len(series) >= p else None
        for p in SMA_PERIODS
    }
    out["rsi14"] = _last(rsi_series(series, universe="survivor_only", period=WILDER_PERIOD).values)
    out["atr14"] = _last(atr_series(series, universe="survivor_only", period=WILDER_PERIOD).values)

    volumes = [float(row["volume"] or 0) for row in series.rows]
    prior = volumes[-VOLUME_RATIO_WINDOW - 1 : -1]
    prior_mean = sum(prior) / len(prior) if len(prior) == VOLUME_RATIO_WINDOW else 0.0
    out["volume_ratio20"] = volumes[-1] / prior_mean if prior_mean > 0 else None

    # "VWAP20": a daily-bar PROXY (§3.2), labelled as such in the prompt.
    window = list(zip(series.float_highs, series.float_lows, series.float_closes, volumes, strict=True))[-VWAP_WINDOW:]
    volume_sum = sum(v for *_, v in window)
    weighted = sum(
        (h + low + c) / 3 * v for h, low, c, v in window if h is not None and low is not None and c is not None
    )
    out["vwap20_proxy"] = weighted / volume_sum if volume_sum > 0 else None
    return out


# ---------------------------------------------------------------------------
# §3.2 disclosures (O2, O3)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Disclosure:
    """A filing or news title. ``known_at`` is OUR ingestion time — the O2 knowledge key —
    and ``event_at`` the filing / publication time."""

    source: Literal["filing", "news"]
    source_id: int
    title: str
    event_at: datetime
    known_at: datetime


def clean_title(title: str) -> str:
    """Control characters → space, whitespace runs collapsed, truncated to 200 characters."""
    spaced = "".join(" " if unicodedata.category(ch) == "Cc" else ch for ch in title)
    return " ".join(spaced.split())[:TITLE_MAX_CHARS]


def select_disclosures(items: Iterable[Disclosure], *, as_of: datetime) -> tuple[Disclosure, ...]:
    """At most 5, known by ``as_of`` and dated within the 30 days before it; newest first, ties
    by source id, exact (cleaned) title de-duplicated keeping the first. Amendments are their
    own rows and are kept (O3). The cap is PER SOURCE (§3.2: 5 filings and 5 headlines), so
    one call takes one source and a mixed input is refused."""
    items = list(items)
    if len({d.source for d in items}) > 1:
        raise ValueError("select_disclosures takes one source per call; the cap of 5 is per source")
    known = [d for d in items if d.known_at <= as_of and as_of - DISCLOSURE_WINDOW <= d.event_at <= as_of]
    known.sort(key=lambda d: (-d.event_at.timestamp(), d.source_id))
    out: list[Disclosure] = []
    seen: set[str] = set()
    for d in known:
        title = clean_title(d.title)
        if not title or title in seen:
            continue
        seen.add(title)
        out.append(Disclosure(d.source, d.source_id, title, d.event_at, d.known_at))
        if len(out) == DISCLOSURE_LIMIT:
            break
    return tuple(out)


# ---------------------------------------------------------------------------
# O3 canonical JSON
# ---------------------------------------------------------------------------
class NonCanonicalValue(ValueError):
    """A value the pack cannot carry canonically (non-finite, naive datetime, unknown type)."""


def _canonical_default(value: object) -> object:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise NonCanonicalValue(f"non-finite Decimal {value}")
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise NonCanonicalValue(f"naive datetime {value}")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise NonCanonicalValue(f"unsupported type {type(value).__name__}")


def canonical_json(value: object) -> str:
    """O3: sorted keys, compact separators, UTF-8 (no ASCII escaping), Decimals as strings,
    timestamps as ISO-8601 UTC. Non-finite floats and Decimals are refused."""
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=_canonical_default,
        )
    except ValueError as exc:
        if isinstance(exc, NonCanonicalValue):
            raise
        raise NonCanonicalValue(str(exc)) from exc


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
