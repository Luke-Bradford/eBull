"""Census for #2508's producer half — which decision-context inputs a RETROSPECTIVE
producer can actually resolve, over the full fired-signal population.

Read-only. Opens one ``REPEATABLE READ READ ONLY`` transaction, writes no row, runs no
job and touches no broker. It builds no ``DecisionContext`` and calls no
``store_decision_context``; it resolves the AVAILABILITY of each required input and
counts the verdict that ``build_decision_context`` would return.

WHY THIS EXISTS
---------------
``build_decision_context`` (``app/services/strategy_decision_context.py:245``) has no
production caller and ``strategy_decision_contexts`` holds 0 rows against 59,230
``strategy_signals``. #2840 arm 2's forward instrument was recorded as queueing behind
"wire that builder to a caller". Before wiring anything, the question that decides the
SHAPE of the producer is which of the thirteen ``_REQUIRED_INPUTS`` a caller standing at
the daily scan can resolve at all — because ``sql/305``'s
``strategy_decision_context_eligible_complete`` CHECK admits an ``eligible`` row only
when every one of them is present.

⚠⚠ WHAT THIS MEASURES IS THE CURRENT STORE, NOT OBTAINABILITY, AND THE TWO ARE NOT THE
SAME CLAIM. A first draft of this census hard-coded ``spread_bps`` and
``market_sector_residual_z`` to unavailable and reported the resulting zero as a finding.
Both constants were wrong in the interesting direction, and Codex checkpoint 1 refused
the framing for exactly that circularity — an assumption cannot be a measurement:

* ``strategy_quote_observations`` (``sql/306``, #2485/#2484) stores HISTORICAL
  best-bid/ask with a computed ``spread_bps``, bounded to the predeclared intraday
  research panel. It is queried here rather than assumed away.
* ``strategy_regime_context.decompose_return`` (#2523) already returns the market /
  sector-relative / instrument-residual split. It has no production caller and no
  standardisation step, so the ``_z`` remains undefined — but "no producer" is a
  narrower statement than "no arithmetic", and only the narrow one is true.

⚠⚠ AND INTRADAY HISTORY IS FETCHABLE ON DEMAND — a stored-bar count is NOT a capability
claim. ``.claude/skills/data-sources/etoro-api.md`` §"WE HAVE INTRADAY HISTORY":
``get_intraday_candles`` serves intraday OHLCV *"on demand, per instrument, with no
recorder required"*, capped at 1,000 bars with NO date anchor — measured reach ~5 days at
``FiveMinutes``, ~1 month at ``ThirtyMinutes``, ~8 months at ``FourHours``. So a low
``intraday_coverage`` count here means the STORE is panel-scoped; it does not mean a
producer could not fetch. What a fetch cannot do is reach past the cap, and it is a
read taken NOW rather than an observation taken THEN.

⚠ EVERY COUNT IS AN UPPER BOUND ON ELIGIBILITY WHERE THE SEMANTICS ARE UNSETTLED.

* ``realised_volatility`` is counted from ``price_daily.volatility_30d``. That column is
  a stored 30-day volatility; whether it is the realised volatility the contract means is
  a separate decision this script does not make.
* ``intraday_coverage`` is counted available on ANY ``strategy_intraday_bars`` row inside
  the lookback. ``sql/304:51`` requires *"Observed / expected completed intraday bars in
  the declared lookback"* — a RATIO whose denominator needs a declared timeframe, which
  the contract does not fix. A single bar satisfies this census and would not satisfy the
  contract.
* ``spread_bps`` is counted available on ANY ``observed`` panel sample in the decision's
  own session. Which sample a producer should take — and whether a same-session bucket
  after the decision instant is admissible at all — is undecided, so this counts the
  session and not an instant.

So an input reported unavailable IS unavailable from the store; an input reported
available may still fail the contract.

WHAT IT MEASURES, AND AGAINST WHICH RULE
----------------------------------------
``_REQUIRED_INPUTS`` and ``DEFINITION`` are IMPORTED rather than restated, so a change to
the contract moves this census with it instead of leaving a second copy to drift.

⚠ THE WINDOW EXCLUDES THE DECISION BAR, AND THAT IS A CENSUS CONVENTION RATHER THAN A
SOURCE RULE. ``sql/304`` says *"completed causal sessions"*, and at a 16:00 decision the
signal bar has itself completed — so the contract does not settle whether it belongs in
its own baseline. It is excluded here because ``relative_volume`` divides the decision
bar's volume BY that baseline, and a bar inside its own denominator attenuates the ratio
it exists to express. The choice moves availability by at most one session and settles no
part of the finding; a producer must declare it rather than inherit it from this script.

``decision_at`` is the signal bar's close on ``signal_bar_date``. The resolvers this
census reproduces all key on ``decision_at.astimezone(NEW_YORK).date()``, so the SQL
works in NY civil dates and never constructs the timestamp — which also means it makes no
assumption about the closing time and is unaffected by half-days. The scan runs in
ARREARS (``strategy_signal_scan.py`` module header), so the instant a row is WRITTEN is a
day or more after the instant the decision was made.

Usage::

    PYTHONPATH=. uv run python -m scripts.census_2508_decision_context_inputs
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from typing import Any, Final

import psycopg
import psycopg.rows

from app.config import settings
from app.services.cboe_vix import RETENTION_START, VENDOR, VENDOR_SYMBOL
from app.services.market_calendar import us_market_status
from app.services.strategy_decision_context import (
    _REQUIRED_INPUTS,
    CONTEXT_VERSION,
    DEFINITION,
)

_NEW_YORK: Final = "America/New_York"

#: The classification resolver refuses unless the row is non-``unknown`` on BOTH axes and
#: carries a ``provider_industry_id`` (``build_decision_context`` lines 287-295). These
#: three are appended to the missing list separately from ``_REQUIRED_INPUTS``, so the
#: taxonomy below reproduces them by name rather than collapsing them into one flag.
_CLASSIFICATION_PARTS: Final = (
    "point_in_time_classification",
    "security_type",
    "primary_listing_market",
    "provider_industry_id",
)


_SIGNAL_INPUT_SQL: Final = """
SELECT s.signal_id,
       s.strategy_id,
       s.signal_bar_date,
       cls.effective_from IS NOT NULL                     AS has_classification,
       coalesce(cls.security_type, 'unknown') <> 'unknown' AS has_security_type,
       coalesce(cls.primary_listing_market, 'unknown') <> 'unknown' AS has_listing,
       cls.provider_industry_id IS NOT NULL               AS has_industry,
       bar.close IS NOT NULL AND bar.close > 0            AS has_decision_close,
       unadj.series_id IS NOT NULL                        AS has_unadjusted_basis,
       w.sessions                                          AS window_sessions,
       w.usable_sessions                                   AS window_usable_sessions,
       w.mean_share_volume                                 AS mean_share_volume,
       bar.volume IS NOT NULL                              AS has_decision_volume,
       prev.close IS NOT NULL AND prev.close > 0          AS has_prior_close,
       bar.open IS NOT NULL                                AS has_decision_open,
       bar.volatility_30d IS NOT NULL                      AS has_stored_volatility,
       intra.bars                                          AS intraday_bars,
       spread.observations                                 AS spread_observations,
       q.instrument_id IS NOT NULL                         AS has_current_quote_row
  FROM strategy_signals s
  LEFT JOIN LATERAL (
        SELECT h.effective_from, h.security_type, h.primary_listing_market,
               h.provider_industry_id
          FROM instrument_market_classification_history h
         WHERE h.instrument_id = s.instrument_id
           AND daterange(h.effective_from, h.effective_to, '[]') @> s.signal_bar_date
         LIMIT 1
  ) cls ON TRUE
  LEFT JOIN price_daily bar
         ON bar.instrument_id = s.instrument_id
        AND bar.price_date = s.signal_bar_date
  LEFT JOIN LATERAL (
        SELECT p.close
          FROM price_daily p
         WHERE p.instrument_id = s.instrument_id
           AND p.price_date < s.signal_bar_date
         ORDER BY p.price_date DESC
         LIMIT 1
  ) prev ON TRUE
  LEFT JOIN LATERAL (
        -- ⚠ THE EXACT BAR, NOT THE SERIES' SPAN. `first_bar <= D <= last_bar` is
        -- satisfied by a series with a HOLE on D, and `sql/249` documents that this
        -- metadata drifts. `observed_unadjusted` is a claim about one observation, so
        -- the join has to reach it.
        SELECT d.series_id
          FROM research_price_series r
          JOIN research_price_daily d ON d.series_id = r.series_id
         WHERE r.instrument_id = s.instrument_id
           AND r.adjustment_basis = 'unadjusted'
           AND d.bar_date = s.signal_bar_date
           AND d.close IS NOT NULL
           AND d.close > 0
         LIMIT 1
  ) unadj ON TRUE
  LEFT JOIN LATERAL (
        SELECT count(*)                                    AS sessions,
               -- A session the baseline can USE needs both legs: dollar volume is
               -- close x volume, so a NULL/non-positive close disqualifies the session
               -- for the dollar legs exactly as a NULL volume does for the share legs.
               count(*) FILTER (
                   WHERE win.volume IS NOT NULL AND win.close IS NOT NULL AND win.close > 0
               )                                           AS usable_sessions,
               avg(win.volume)                             AS mean_share_volume,
               min(win.price_date)                         AS window_start
          FROM (
                SELECT p.price_date, p.close, p.volume
                  FROM price_daily p
                 WHERE p.instrument_id = s.instrument_id
                   AND p.price_date < s.signal_bar_date
                 ORDER BY p.price_date DESC
                 LIMIT %(lookback)s
          ) win
  ) w ON TRUE
  LEFT JOIN LATERAL (
        -- ⚠ The SAME span the daily window actually covered, not a calendar
        -- approximation of it: `w.window_start` is the oldest bar the LIMIT kept, so
        -- the two windows cannot disagree about which sessions are "in the lookback".
        SELECT count(*) AS bars
          FROM strategy_intraday_bars b
         WHERE b.instrument_id = s.instrument_id
           AND w.window_start IS NOT NULL
           AND b.bar_time < (s.signal_bar_date::timestamp AT TIME ZONE %(tz)s)
           AND b.bar_time >= (w.window_start::timestamp AT TIME ZONE %(tz)s)
  ) intra ON TRUE
  LEFT JOIN LATERAL (
        -- #2485/#2484's bounded prospective best-bid/ask panel. Only `observed` rows
        -- carry a spread (`sql/306`: *"absence is not a zero spread"*), and only
        -- buckets at or before the decision's own session close are causal.
        SELECT count(*) AS observations
          FROM strategy_quote_observations o
         WHERE o.instrument_id = s.instrument_id
           AND o.observation_status = 'observed'
           AND (o.sample_bucket AT TIME ZONE %(tz)s)::date = s.signal_bar_date
  ) spread ON TRUE
  LEFT JOIN quotes q ON q.instrument_id = s.instrument_id
 WHERE s.verdict = 'fired'
"""


def _prior_us_session(day: date) -> date:
    """``strategy_decision_context._prior_us_session`` on a civil NY date.

    Reproduced against a DATE rather than imported against a ``datetime`` because this
    census resolves VIX once per distinct decision DATE (37 of them) instead of once per
    signal (59,230). The stepping rule is the same and is asserted in the tests.
    """
    candidate = day - timedelta(days=1)
    while us_market_status(candidate) == "closed":
        candidate -= timedelta(days=1)
    return candidate


def vix_refusals(vix_bar_dates: frozenset[date], decision_dates: Sequence[date]) -> dict[date, str | None]:
    """Per decision date, the name ``build_decision_context`` would append, or ``None``.

    ⚠ The name is TYPED, not the bare ``"vix"``. ``load_decision_vix`` returns
    ``missing_source`` or ``stale_source:<stored>` <expected:<expected>>``, and the builder
    prefixes it — ``missing.append(f"vix_{inputs.vix.refusal_reason}")``. A census that
    collapses both to ``"vix"`` cannot call its taxonomy the exact stored string, and it
    merges two different defects: a source that has not been fetched, and a source that
    has been fetched and carries a row the calendar says cannot exist.

    ⚠ This reproduces BOTH of that function's steps, and the second one is easy to drop.
    ``load_vix_close_as_known`` takes the LATEST stored bar strictly before the decision
    date (``ORDER BY bar_date DESC LIMIT 1``), and ``load_decision_vix`` then refuses it
    unless it equals the expected prior US session. So "the expected date is somewhere in
    the set" is NOT the rule: a stray non-session row — a Saturday bar, a vendor
    correction dated to a holiday — is selected first and produces a typed
    ``stale_source`` refusal even though the correct close is also stored. Testing only
    for membership reports that decision as available, which is the optimistic direction
    and therefore the one to get right.
    """
    refusals: dict[date, str | None] = {}
    for day in decision_dates:
        earlier = [bar_date for bar_date in vix_bar_dates if bar_date < day]
        if not earlier:
            refusals[day] = "vix_missing_source"
            continue
        selected = max(earlier)
        expected = _prior_us_session(day)
        refusals[day] = (
            None if selected == expected else f"vix_stale_source:{selected.isoformat()}<expected:{expected.isoformat()}"
        )
    return refusals


def missing_inputs(row: Mapping[str, Any], *, vix_refusal: str | None) -> tuple[str, ...]:
    """The names ``build_decision_context`` would put in ``refusal_reason``.

    ⚠ The names and their ORDER follow ``build_decision_context`` lines 280-296: the
    ``_REQUIRED_INPUTS`` members first, then ``vix``, then the classification parts. The
    stored reason is ``"missing:" + ",".join(sorted(missing))``, so ordering does not
    reach the row — it is preserved here only so the taxonomy reads in contract order.
    """
    lookback = DEFINITION.volume_lookback_sessions
    complete_volume = (row["window_usable_sessions"] or 0) >= lookback
    mean_share = row["mean_share_volume"]

    available: dict[str, bool] = {
        # `sql/305`: an eligible price needs a DIRECTLY OBSERVED unadjusted level or a
        # causal reconstruction. Reconstruction needs point-in-time adjustment factors
        # and `price_adjustments` holds none, so the observed archive is the only path
        # this corpus offers — see the module header for what that does and does not
        # prove.
        "as_traded_price": bool(row["has_decision_close"]),
        "as_traded_price_basis": bool(row["has_unadjusted_basis"]),
        # One threshold for all five, because dollar volume is close x volume: a session
        # missing EITHER leg cannot contribute, so the share legs inherit the dollar
        # legs' stricter requirement rather than the other way round.
        "trailing_mean_share_volume": complete_volume,
        "trailing_median_share_volume": complete_volume,
        "trailing_mean_dollar_volume": complete_volume,
        "trailing_median_dollar_volume": complete_volume,
        "zero_volume_frequency": complete_volume,
        "intraday_coverage": (row["intraday_bars"] or 0) > 0,
        # Relative volume divides the decision bar's volume by the trailing mean, so it
        # needs both, and needs the denominator to be non-zero.
        "relative_volume": bool(row["has_decision_volume"])
        and complete_volume
        and mean_share is not None
        and mean_share > 0,
        "spread_bps": (row["spread_observations"] or 0) > 0,
        "realised_volatility": bool(row["has_stored_volatility"]),
        "gap_pct": bool(row["has_decision_open"]) and bool(row["has_prior_close"]),
        # ⚠ NOT "no arithmetic exists". `strategy_regime_context.decompose_return`
        # (#2523) returns the market / sector-relative / instrument-residual split, but
        # it is a pure module whose only caller is `scripts/verify_2523_regime_context.py`
        # — no production producer — and it performs no STANDARDISATION, so the `_z` this
        # field names has no definition anywhere in the repo to resolve against.
        "market_sector_residual_z": False,
    }
    unknown = set(available) ^ set(_REQUIRED_INPUTS)
    if unknown:
        raise AssertionError(f"required-input set drifted from the contract: {sorted(unknown)}")

    missing = [name for name in _REQUIRED_INPUTS if not available[name]]
    if vix_refusal is not None:
        missing.append(vix_refusal)
    if not row["has_classification"]:
        missing.append("point_in_time_classification")
    else:
        if not row["has_security_type"]:
            missing.append("security_type")
        if not row["has_listing"]:
            missing.append("primary_listing_market")
        if not row["has_industry"]:
            missing.append("provider_industry_id")
    return tuple(missing)


def _fetch(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        return cur.execute(
            _SIGNAL_INPUT_SQL,
            {"lookback": DEFINITION.volume_lookback_sessions, "tz": _NEW_YORK},
        ).fetchall()


def _vix_bar_dates(conn: psycopg.Connection[Any]) -> frozenset[date]:
    rows = conn.execute(
        """
        SELECT d.bar_date
          FROM research_price_series s
          JOIN research_price_daily d USING (series_id)
         WHERE s.vendor = %(vendor)s
           AND s.vendor_symbol = %(symbol)s
           AND d.bar_date >= %(start)s
        """,
        {"vendor": VENDOR, "symbol": VENDOR_SYMBOL, "start": RETENTION_START},
    ).fetchall()
    return frozenset(row[0] for row in rows)


def _report(rows: Sequence[Mapping[str, Any]], vix: Mapping[date, str | None]) -> str:
    total = len(rows)
    out: list[str] = []
    out.append(f"context_version        {CONTEXT_VERSION}")
    out.append(f"lookback_sessions      {DEFINITION.volume_lookback_sessions}")
    out.append(f"fired signals          {total:,}")
    if total == 0:
        return "\n".join(out)

    missing_by_signal = [missing_inputs(row, vix_refusal=vix[row["signal_bar_date"]]) for row in rows]
    eligible = sum(1 for names in missing_by_signal if not names)
    out.append(f"eligible contexts      {eligible:,}  ({eligible / total:.4%})")
    out.append(f"refused contexts       {total - eligible:,}")

    per_input: Counter[str] = Counter()
    for names in missing_by_signal:
        per_input.update(names)
    out.append("")
    out.append("MISSING BY INPUT (share of fired signals that could not resolve it)")
    for name in (*_REQUIRED_INPUTS, *_CLASSIFICATION_PARTS):
        count = per_input.get(name, 0)
        out.append(f"  {name:<32} {count:>9,}  {count / total:7.2%}")
    # VIX names carry the stored bar date, so they are grouped by their typed prefix
    # rather than listed as one row per affected session.
    for name, count in sorted(per_input.items()):
        if name.startswith("vix_"):
            out.append(f"  {name:<32} {count:>9,}  {count / total:7.2%}")

    out.append("")
    out.append(
        "CLASSIFICATION, AS ONE GAP — the four parts above are conditional on each other "
        "and must not be read as independent shares"
    )
    classification_gap = sum(1 for names in missing_by_signal if any(name in _CLASSIFICATION_PARTS for name in names))
    out.append(
        f"  decisions missing ANY classification part {classification_gap:>9,}  {classification_gap / total:7.2%}"
    )

    out.append("")
    out.append("REFUSAL REASON (the exact string a refused row would store) — ALL of them")
    reasons: Counter[str] = Counter(
        "missing:" + ",".join(sorted(names)) if names else "<eligible>" for names in missing_by_signal
    )
    for reason, count in reasons.most_common():
        out.append(f"  {count:>9,}  {count / total:7.2%}  {reason}")
    out.append(f"  distinct reasons: {len(reasons):,}")

    out.append("")
    out.append("PER STRATEGY")
    by_strategy: Counter[str] = Counter(row["strategy_id"] for row in rows)
    eligible_by_strategy: Counter[str] = Counter(
        row["strategy_id"] for row, names in zip(rows, missing_by_signal, strict=True) if not names
    )
    for strategy_id, count in by_strategy.most_common():
        out.append(f"  {strategy_id:<38} {count:>8,} fired  {eligible_by_strategy[strategy_id]:>6,} eligible")

    out.append("")
    out.append("THE TWO PANEL-SCOPED INPUTS — where the observation record reaches at all")
    spread = sum(1 for row in rows if (row["spread_observations"] or 0) > 0)
    intraday = sum(1 for row in rows if (row["intraday_bars"] or 0) > 0)
    out.append(f"  decisions with an OBSERVED spread sample  {spread:>9,}  {spread / total:7.2%}")
    out.append(f"  decisions with ANY intraday bar in window {intraday:>9,}  {intraday / total:7.2%}")
    quoted = sum(1 for row in rows if row["has_current_quote_row"])
    out.append(f"  decisions whose instrument has a quotes row {quoted:>8,}  {quoted / total:7.2%}")
    out.append(
        "    ^ counted over DECISIONS, not instruments, and it is the MUTABLE current row: "
        "it says nothing about the spread that stood at the decision"
    )
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        rows = _fetch(conn)
        vix_bar_dates = _vix_bar_dates(conn)
    decision_dates = sorted({row["signal_bar_date"] for row in rows})
    vix = vix_refusals(vix_bar_dates, decision_dates)
    print(_report(rows, vix))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())


__all__ = ["main", "missing_inputs", "vix_refusals"]
