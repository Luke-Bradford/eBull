"""Census for #2840 arm 2 step 1 — what shape must the forward instrument's observation
panel have, and does the declared one have it?

Read-only. One ``REPEATABLE READ READ ONLY`` transaction. Writes no row, freezes no
declaration, runs no trial, resolves no fill, reads no outcome. Every quantity is a
POPULATION COUNT.

⚠⚠ THE QUESTION THIS ANSWERS IS *COMPOSITION*, NOT SIZE, AND THAT IS A CORRECTION.
A first version of this census asked "how many names clear S-12's gate, and how large is
the population we could widen to" — and proposed widening to the ``>= $100`` names.
**Codex checkpoint 1 showed that widening would destroy the experiment**, and the argument
is short enough to state in full:

    S-12 is S-4 gated to ``close >= CHEAPEST_BAND.lower``. The trial's estimand is the
    PAIRED difference between the two books on one panel. On a panel where every member
    clears the gate, the gate rejects nothing, the two books are identical, and the
    difference is identically zero — a guaranteed null, produced by the panel rather than
    by the market.

**So the panel must SPAN the gate.** Its sub-gate members are not filler: they are the
entries the control takes and the candidate refuses, which is the whole measurement. That
inverts the first version's conclusion and is why this script reports the split on each
side rather than a count above the threshold.

THE BINDING CAP IS 50, NOT THE STORAGE CAP
------------------------------------------
``strategy_quote_observation.MAX_PANEL_INSTRUMENTS`` is **50** — the spread lane refuses
above it before fetching anything. The first version compared the candidate population to
#2477's 30m retention cap of ~1,000 instruments and called that the constraint; it is not
the binding one, and the two lanes are different collectors on different cadences.

THE UNIVERSE AUTHORITY, WHICH THE FIRST VERSION GOT WRONG
---------------------------------------------------------
⚠ The universe is resolved through ``strategies.validated_universe``, NOT by a literal
``instrument_type_id = 5``. #2289 §4.0, quoted in that module: *"``instruments.
instrument_type_id`` carries no foreign key … the universe query must assert the type-id
lookup resolves, not assume it."* A literal asserts nothing; ``resolve_stocks_type_id``
raises when the anchor is gone, and that raise is the assertion. The first version of this
script hardcoded the 5 — a source-rule violation in the same session that wrote a
prevention-log entry about assuming instead of querying.

⚠⚠ WHAT "AS-TRADED" DOES AND DOES NOT MEAN HERE, NARROWED.
An earlier draft argued the LATEST stored bar is on the current basis "by construction, as
there is no later fetch to re-base it". That is too strong: a refresh can rewrite the same
``price_date`` row on a new basis, and equally a split can occur with no refresh since, so
a fresh-looking row can still be pre-split. **``sql/305`` would classify every price here
as ``unknown`` basis and refuse it for cohort attribution.** The latest close is used as
the best available PROXY for an as-traded level and is labelled as one; nothing in this
readout is admissible as `observed_unadjusted` evidence.

⚠ HISTORICAL closes are worse, and the panel carries the proof: CENN shows hundreds of
bars at or above $100 against a latest close under $5. Whether that is a reverse split, a
vendor error or something else is NOT established here — only that the historical column
disagrees with the current one by two orders of magnitude, which is enough to keep it out
of any threshold claim.

Usage::

    PYTHONPATH=. uv run python -m scripts.census_2840_forward_panel_supply
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, Final

import psycopg
import psycopg.rows

from app.config import settings
from app.services.strategies.s12_cheapest_band_price_gated_breakout import (
    CHEAPEST_BAND,
    S12_STRATEGY_ID,
)
from app.services.strategies.validated_universe import (
    US_EQUITY_ASSET_CLASS,
    VALIDATED_UNIVERSE_RULE_VERSION,
    resolve_stocks_type_id,
)
from app.services.strategy_quote_observation import MAX_PANEL_INSTRUMENTS

#: ⚠ A bar older than this is not evidence the instrument still trades at that level.
#: DECLARED, not derived, and applied to the panel and the population ALIKE — the first
#: version applied it only to the population, so a stale panel member counted as admitted
#: against a freshness rule its neighbours had to meet.
_FRESH_BAR_DAYS: Final = 7

PanelVerdict = str


def classify_panel_member(
    *,
    in_universe: bool,
    latest_close: Decimal | None,
    stale: bool,
    gate_lower: Decimal,
) -> PanelVerdict:
    """Where a declared panel member sits relative to S-12's gate.

    ⚠ ``below_gate`` IS NOT A FAILURE. It is the half of the panel that carries the
    control's rejected entries, without which the paired difference is identically zero.
    The verdict names a position, not a verdict on the member.

    ⚠ Order is load-bearing. Universe membership first: an ETF at $700 clears the gate and
    still cannot host a strategy whose universe excludes it, so checking price first would
    report it admitted. Then staleness, then value validity, then the threshold.
    """
    if not in_universe:
        return "outside_strategy_universe"
    if latest_close is None:
        return "no_daily_bar"
    if stale:
        return "stale_bar"
    # `Decimal('NaN') < x` raises InvalidOperation and `Infinity` compares as admitted;
    # neither is a price, and both reached the threshold in the first version.
    if not latest_close.is_finite() or latest_close <= 0:
        return "unusable_close"
    return "at_or_above_gate" if latest_close >= gate_lower else "below_gate"


_PANEL_SQL: Final = """
SELECT m.symbol,
       i.instrument_id,
       i.instrument_type_id,
       (i.is_tradable AND e.asset_class = %(asset_class)s
        AND i.instrument_type_id = %(stocks_type_id)s) AS in_universe,
       latest.close      AS latest_close,
       latest.price_date AS latest_bar_date,
       (latest.price_date IS NULL
        OR latest.price_date < current_date - %(fresh)s) AS stale,
       hist.max_close    AS max_close
  FROM (SELECT DISTINCT symbol FROM strategy_intraday_universe_members
         WHERE universe_version = %(universe_version)s) m
  LEFT JOIN instruments i ON i.symbol = m.symbol
  LEFT JOIN exchanges e ON e.exchange_id = i.exchange
  LEFT JOIN LATERAL (
        SELECT p.close, p.price_date
          FROM price_daily p
         WHERE p.instrument_id = i.instrument_id
         ORDER BY p.price_date DESC
         LIMIT 1
  ) latest ON TRUE
  LEFT JOIN LATERAL (
        SELECT max(p.close) AS max_close
          FROM price_daily p
         WHERE p.instrument_id = i.instrument_id
  ) hist ON TRUE
 ORDER BY m.symbol
"""

#: The population a panel is DRAWN from, split on the gate. Both sides are reported
#: because both are required — see the module header.
_POPULATION_SQL: Final = """
WITH latest AS (
    SELECT DISTINCT ON (p.instrument_id)
           p.instrument_id, p.price_date, p.close
      FROM price_daily p
      JOIN instruments i ON i.instrument_id = p.instrument_id
      JOIN exchanges e ON e.exchange_id = i.exchange
     WHERE i.is_tradable
       AND e.asset_class = %(asset_class)s
       AND i.instrument_type_id = %(stocks_type_id)s
     ORDER BY p.instrument_id, p.price_date DESC
), fresh AS (
    -- ⚠ `close <> 'NaN'` IS NOT REDUNDANT. PostgreSQL orders numeric NaN ABOVE every
    -- other value, so `NaN > 0` and `NaN >= gate` are both TRUE and an unusable bar would
    -- be counted as fresh AND above the gate. `_normalise_candle` accepts `Decimal("NaN")`
    -- and the column carries no finite-value constraint, so this is reachable rather than
    -- theoretical. (⚠ `close = 'NaN'` is also TRUE for numeric — equality, not IS NULL,
    -- is the test.) `classify_panel_member` makes the same refusal in Python.
    SELECT * FROM latest
     WHERE price_date >= current_date - %(fresh)s
       AND close IS NOT NULL
       AND close <> 'NaN'::numeric
       AND close > 0
)
SELECT (SELECT count(*) FROM latest)                                    AS with_a_bar,
       (SELECT count(*) FROM fresh)                                     AS fresh_and_usable,
       (SELECT count(*) FROM fresh WHERE close >= %(gate)s)             AS at_or_above_gate,
       (SELECT count(*) FROM fresh WHERE close <  %(gate)s)             AS below_gate
"""

#: ⚠ INDICATIVE ONLY, AND THE LABEL IS NOT A FORMALITY. This reads TODAY's `price_daily`
#: close for a PAST signal bar, so it is neither the price the scan saw nor an as-traded
#: level. It is reported because it is the only direct observation of how the live scan's
#: own decisions sit relative to the gate, and the window is printed beside it so nobody
#: compares it to a census over a different era.
#: ⚠ THE `<> 'NaN'` IS LOAD-BEARING, NOT DEFENSIVE NOISE. PostgreSQL orders numeric NaN
#: ABOVE every other value, so a bare `close >= gate` counts an unusable bar as clearing
#: the gate. `_normalise_candle` accepts `Decimal("NaN")` and the column has no
#: finite-value constraint, so the row is reachable. Written once and interpolated into
#: every FILTER below so the three counts cannot drift apart.
_AT_GATE: Final = "p.close IS NOT NULL AND p.close <> 'NaN'::numeric AND p.close >= %(gate)s"

_ADMISSION_SQL: Final = f"""
SELECT s.strategy_id,
       s.strategy_version,
       count(*)                                                  AS fires,
       count(*) FILTER (WHERE {_AT_GATE})                        AS fires_at_gate,
       count(DISTINCT s.signal_bar_date)                         AS dates,
       count(DISTINCT s.signal_bar_date) FILTER (WHERE {_AT_GATE}) AS dates_at_gate,
       count(DISTINCT s.instrument_id) FILTER (WHERE {_AT_GATE})   AS names_at_gate,
       min(s.signal_bar_date)                                               AS first_date,
       max(s.signal_bar_date)                                               AS last_date
  FROM strategy_signals s
  JOIN price_daily p
    ON p.instrument_id = s.instrument_id
   AND p.price_date = s.signal_bar_date
 WHERE s.verdict = 'fired'
 GROUP BY 1, 2
 ORDER BY 3 DESC
"""

#: A fired signal whose daily bar is gone cannot be classified either way. The inner join
#: above drops it silently, so the count is taken separately rather than left implicit.
_UNJOINED_SQL: Final = """
SELECT count(*)
  FROM strategy_signals s
 WHERE s.verdict = 'fired'
   AND NOT EXISTS (
        SELECT 1 FROM price_daily p
         WHERE p.instrument_id = s.instrument_id
           AND p.price_date = s.signal_bar_date)
"""


def _active_universe_version(conn: psycopg.Connection[Any]) -> str:
    rows = conn.execute(
        """
        SELECT universe_version FROM strategy_intraday_universe_versions
         WHERE status = 'active'
        """
    ).fetchall()
    if len(rows) != 1:
        raise RuntimeError(f"expected exactly one active intraday universe, found {len(rows)}")
    return str(rows[0][0])


def _report(
    *,
    universe_version: str,
    panel: Sequence[Mapping[str, Any]],
    population: Mapping[str, Any],
    admission: Sequence[Mapping[str, Any]],
    unjoined: int,
) -> str:
    gate = CHEAPEST_BAND.lower
    assert gate is not None  # the strategy module refuses a band open below
    out: list[str] = []
    out.append(f"strategy         {S12_STRATEGY_ID}")
    out.append(f"gate             close >= {gate}  (CHEAPEST_BAND {CHEAPEST_BAND.label})")
    out.append(f"universe rule    {VALIDATED_UNIVERSE_RULE_VERSION}  (type id RESOLVED, never a literal)")
    out.append(f"panel cap        {MAX_PANEL_INSTRUMENTS}  (strategy_quote_observation.MAX_PANEL_INSTRUMENTS)")
    out.append(f"freshness        latest bar within {_FRESH_BAR_DAYS} days")
    out.append("")

    out.append(f"THE DECLARED OBSERVATION PANEL — {universe_version}")
    verdicts: dict[str, int] = {}
    for member in panel:
        verdict = classify_panel_member(
            in_universe=bool(member["in_universe"]),
            latest_close=member["latest_close"],
            stale=bool(member["stale"]),
            gate_lower=gate,
        )
        verdicts[verdict] = verdicts.get(verdict, 0) + 1
        out.append(
            f"  {str(member['symbol']):<6} type={member['instrument_type_id']!s:<4}"
            f" latest={member['latest_close']!s:>10} ({member['latest_bar_date']})"
            f" max={member['max_close']!s:>11}  {verdict}"
        )
    out.append("  " + ", ".join(f"{k} {v}" for k, v in sorted(verdicts.items())) + f"  (of {len(panel)})")
    out.append(
        "  ⚠ BOTH sides of the gate are required: a panel where every member clears it "
        "makes the two books identical and the paired difference identically zero."
    )
    out.append(
        "  ⚠ max_close is HISTORICAL and back-adjusted. Where it towers over the latest "
        "close the two columns disagree by orders of magnitude; the cause is not "
        "established here and the historical column is used for no claim."
    )
    out.append("")

    out.append("THE POPULATION A PANEL IS DRAWN FROM — latest bar only, an as-traded PROXY")
    out.append(f"  validated universe with a daily bar   {population['with_a_bar']:>8,}")
    out.append(f"  ... fresh and usable                  {population['fresh_and_usable']:>8,}")
    out.append(f"  ...... at or above the gate           {population['at_or_above_gate']:>8,}")
    out.append(f"  ...... below the gate                 {population['below_gate']:>8,}")
    out.append(
        f"  ⚠ neither side is a panel size: the spread lane caps a panel at "
        f"{MAX_PANEL_INSTRUMENTS}, so what these bound is the pool a stratified draw "
        f"selects from."
    )
    out.append("")

    out.append("LIVE-SCAN POSITION RELATIVE TO THE GATE — ⚠ INDICATIVE ONLY, see the header")
    for arm in admission:
        fires, at_gate = int(arm["fires"]), int(arm["fires_at_gate"])
        out.append(
            f"  {str(arm['strategy_id']):<38} {str(arm['strategy_version']):<46}"
            f" {at_gate:>6,}/{fires:<7,} ({at_gate / fires:6.2%})"
            f"  {arm['dates_at_gate']:>3}/{arm['dates']!s:<3} dates"
            f"  {arm['names_at_gate']:>4} names  {arm['first_date']}..{arm['last_date']}"
        )
    out.append(f"  fired signals with no daily bar to classify: {unjoined:,}")
    out.append(
        "  ⚠ NOT comparable to the in-sample admission rates recorded in "
        "docs/proposals/ta/2026-09-20-sh-top-band-price-gated-breakout.md — a different "
        "era, universe and nominal price level, and no matched-strata comparison exists. "
        "Those figures are deliberately NOT copied here: this script does not derive them, "
        "so printing them beside this run's output would make a stale number look current."
    )
    out.append(
        "  ⚠ a row per strategy_version, NOT per strategy: a cost-model or universe change "
        "rotates the identity and leaves the earlier rows standing, so pooling them would "
        "average decisions taken under different rules."
    )
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    gate = CHEAPEST_BAND.lower
    if gate is None:  # pragma: no cover - the strategy module refuses this at import
        raise RuntimeError("cheapest band is open below; there is no gate to measure")
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        universe_version = _active_universe_version(conn)
        stocks_type_id = resolve_stocks_type_id(conn)
        scope: dict[str, Any] = {
            "asset_class": US_EQUITY_ASSET_CLASS,
            "stocks_type_id": stocks_type_id,
            "fresh": _FRESH_BAR_DAYS,
            "gate": gate,
        }
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            panel = cur.execute(_PANEL_SQL, {**scope, "universe_version": universe_version}).fetchall()
            # ⚠ `instruments.symbol` carries no uniqueness constraint, so the symbol join
            # can fan a member out into several rows and inflate every verdict tally. It
            # does not today; refuse rather than report a count that is right by luck.
            symbols = [str(member["symbol"]) for member in panel]
            if len(symbols) != len(set(symbols)):
                raise RuntimeError(f"panel symbols resolved to multiple instruments: {sorted(symbols)}")
            population = cur.execute(_POPULATION_SQL, scope).fetchone()
            admission = cur.execute(_ADMISSION_SQL, {"gate": gate}).fetchall()
        unjoined = conn.execute(_UNJOINED_SQL).fetchone()
    if population is None or unjoined is None:  # pragma: no cover - aggregates return a row
        raise RuntimeError("an aggregate returned no row")
    print(
        _report(
            universe_version=universe_version,
            panel=panel,
            population=population,
            admission=admission,
            unjoined=int(unjoined[0]),
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())


__all__ = ["classify_panel_member", "main"]
