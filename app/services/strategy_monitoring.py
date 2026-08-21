"""Read models for strategy attribution and exact-owned P&L (#2453).

The strategy surface deliberately derives from existing compact ledgers.  It
does not snapshot quotes, copy broker payloads, or write periodic P&L rows.
Manual positions are excluded structurally: a broker position contributes only
when its exact id is present in ``strategy_position_ownership``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg
import psycopg.rows

from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_signal_scan import SCAN_UNIVERSE
from app.services.valuation import resolve_quote_price

logger = logging.getLogger(__name__)

#: Calendar days per week. Not a trading-day convention — the weekly rate divides
#: by the measured CALENDAR span of the scanned-bar axis, exactly as
#: ``strategy_statistics.periods_per_year`` divides by ``span_days / 365.25``.
_DAYS_PER_WEEK: Final = 7
_SHARE_PRECISION: Final = Decimal("0.0001")
_RATE_PRECISION: Final = Decimal("0.01")

#: Why `fired_share_of_evaluable` is null. `no_evaluable_decisions` means every bar
#: was `not_evaluable`, so the strategy was never offered a decision — its propensity
#: is unknown rather than zero.
#:
#: ⚠ `no_decision_date_scanned` is a THIRD, and it is not a variant of the second
#: (#2811). A periodic strategy is `not_fired` on every bar that is not one of its
#: rebalance dates — `strategy_registry` rule 3, and deliberately not
#: `not_evaluable`, because those bars were judged perfectly well. So a version the
#: scan has never carried to a decision date has a denominator in the THOUSANDS and
#: a numerator of zero, and reports `0.0000` with no reason at all: a confident
#: measured zero standing for a non-measurement. Measured on dev 2026-08-21: S-2 and
#: S-10 both rebalance monthly, their only decision date in live coverage was
#: 2026-08-03, and no scan run of any kind existed before 2026-08-09.
ShareUnavailableReason = Literal["never_scanned", "no_decision_date_scanned", "no_evaluable_decisions"]

#: Why `entries_per_calendar_week` is null. Independent of the share's reason: a
#: multi-day all-`not_evaluable` version rates at 0.00/week and has no share at all.
WeeklyRateUnavailableReason = Literal["never_scanned", "single_scan_day"]


@dataclass(frozen=True)
class StrategyAttribution:
    fired_entries: int = 0
    funded_entries: int = 0
    rejected_entries: int = 0
    resolved_entries: int = 0
    winning_entries: int = 0
    win_rate: Decimal | None = None
    median_days_to_outcome: Decimal | None = None
    signals_last_30_days: int = 0
    shadow_average_return_pct: Decimal | None = None
    funded_shadow_average_return_pct: Decimal | None = None
    rejected_shadow_average_return_pct: Decimal | None = None
    opportunity_gap_pct: Decimal | None = None
    funded_capture_rate: Decimal | None = None
    filled_entries: int = 0
    broker_rejected_entries: int = 0
    fill_rate: Decimal | None = None
    broker_rejection_rate: Decimal | None = None
    average_slippage_pct: Decimal | None = None
    average_stressed_cost_usd: Decimal | None = None
    max_observed_account_drawdown_pct: Decimal | None = None


@dataclass(frozen=True)
class StrategyFireRate:
    """How often a strategy version fired an entry, from the durable census.

    Defaults describe a version the scan has never reached — which is the state a
    freshly-rotated version is in, so it must be representable rather than absent.
    """

    #: #2288's labelling contract: a metric computed on a survivor-only universe is
    #: marked as such. The census carries no `universe` column and does not need
    #: one — a universe change mints a new `strategy_version` (`strategy_signal_scan`),
    #: and this reads one version at a time, so every row was written under this label.
    universe: str = SCAN_UNIVERSE
    #: Distinct census bar dates — COVERAGE, and deliberately not restricted to
    #: decision days. "How much corpus this version saw" and "how many chances it
    #: had" are different questions and the card answers both.
    scanned_days: int = 0
    #: Scanned bar dates that this version could actually act on: in its published
    #: decision calendar AND carrying at least `min_participants` evaluable entry
    #: rows. ``None`` means NO CALENDAR IS KNOWN — the `DecisionCalendar` protocol's
    #: own convention, where a per-series strategy acts on every bar it is evaluable
    #: at. ``0`` means a calendar is known and the scan has covered none of it.
    decision_days: int | None = None
    fired_days: int = 0
    fired_entry_signals: int = 0
    evaluable_entry_decisions: int = 0
    #: Reported, not hidden: these are excluded from `fired_share_of_evaluable`'s
    #: denominator, and an exclusion nobody can see is an exclusion nobody checks.
    not_evaluable_entry_decisions: int = 0
    fired_share_of_evaluable: Decimal | None = None
    entries_per_calendar_week: Decimal | None = None
    first_scanned_bar: date | None = None
    last_scanned_bar: date | None = None
    #: Each nullable value above names its own reason. Invariant, both directions:
    #: a value is None if and only if its reason is not.
    share_unavailable_reason: ShareUnavailableReason | None = "never_scanned"
    weekly_rate_unavailable_reason: WeeklyRateUnavailableReason | None = "never_scanned"


@dataclass(frozen=True)
class StrategyPnl:
    currency: str = "USD"
    strategy_trade_count: int = 0
    owned_position_count: int = 0
    active_position_count: int = 0
    close_event_count: int = 0
    invested_capital: Decimal | None = Decimal("0")
    realised_pnl: Decimal | None = Decimal("0")
    unrealised_pnl: Decimal | None = Decimal("0")
    total_pnl: Decimal | None = Decimal("0")
    observed_fees: Decimal | None = Decimal("0")
    complete: bool = True
    incomplete_reasons: tuple[str, ...] = ()
    # Completed close events remain usable for bankroll accounting while a
    # different newly allocated order is awaiting reconciliation. The public
    # realised_pnl stays conservative and may still be unknown in that state.
    reconciled_realised_pnl: Decimal = Decimal("0")


@dataclass(frozen=True)
class StrategyControlState:
    stage: str | None = None
    pinned_evidence_ready: bool = False
    deployment_id: int | None = None
    capital_limit: Decimal = Decimal("0")
    currency: str = "USD"
    enabled: bool = False
    revision: int | None = None
    reserved_capital: Decimal = Decimal("0")
    policy_configured: bool = False
    max_drawdown_limit_pct: Decimal | None = None
    ticket_sizing_mode: str | None = None
    ticket_fraction: Decimal | None = None
    fixed_ticket_amount: Decimal | None = None
    max_ticket_amount: Decimal | None = None


@dataclass(frozen=True)
class StrategyEntryBlockState:
    global_kill_active: bool
    global_kill_reason: str | None
    global_kill_activated_at: datetime | None
    global_kill_activated_by: str | None
    execution_block_reasons: tuple[str, ...]
    auto_trading_enabled: bool
    live_trading_enabled: bool

    @property
    def new_entries_blocked(self) -> bool:
        return self.global_kill_active or bool(self.execution_block_reasons) or not self.auto_trading_enabled


_ATTRIBUTION_SQL = """
    WITH entry_execution AS (
        SELECT sto.strategy_trade_id,
               SUM(e.opening_units * e.average_price)
                   / NULLIF(SUM(e.opening_units), 0) AS average_price
        FROM strategy_trade_orders sto
        JOIN strategy_order_position_executions e ON e.order_id = sto.order_id
        WHERE sto.purpose = 'entry'
          AND e.opening_units > 0 AND e.average_price > 0
        GROUP BY sto.strategy_trade_id
    ), entry_order AS (
        SELECT DISTINCT ON (sto.strategy_trade_id)
               sto.strategy_trade_id, o.status
        FROM strategy_trade_orders sto
        JOIN orders o ON o.order_id = sto.order_id
        WHERE sto.purpose = 'entry'
        ORDER BY sto.strategy_trade_id, sto.linked_at DESC, sto.order_id DESC
    )
    SELECT s.strategy_id, s.strategy_version,
           COUNT(*) AS fired_entries,
           COUNT(*) FILTER (WHERE fd.verdict = 'allocated') AS funded_entries,
           COUNT(*) FILTER (WHERE fd.verdict IS DISTINCT FROM 'allocated') AS rejected_entries,
           COUNT(*) FILTER (WHERE o.gross_return_pct IS NOT NULL) AS resolved_entries,
           COUNT(*) FILTER (WHERE o.gross_return_pct > 0) AS winning_entries,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY (o.exit_bar_date - s.fill_bar_date)
           ) FILTER (WHERE o.gross_return_pct IS NOT NULL AND o.exit_bar_date IS NOT NULL)
               AS median_days_to_outcome,
           COUNT(*) FILTER (WHERE s.signal_bar_date >= CURRENT_DATE - 30) AS signals_last_30_days,
           AVG(o.gross_return_pct) AS shadow_average_return_pct,
           AVG(o.gross_return_pct) FILTER (WHERE fd.verdict = 'allocated')
               AS funded_shadow_average_return_pct,
           AVG(o.gross_return_pct) FILTER (WHERE fd.verdict IS DISTINCT FROM 'allocated')
               AS rejected_shadow_average_return_pct,
           COUNT(*) FILTER (WHERE fd.verdict = 'allocated' AND ee.average_price IS NOT NULL)
               AS filled_entries,
           COUNT(*) FILTER (
               WHERE fd.verdict = 'allocated'
                 AND ee.average_price IS NULL
                 AND eo.status = 'rejected'
           )
               AS broker_rejected_entries,
           AVG(((ee.average_price - s.fill_price) / NULLIF(s.fill_price, 0)) * 100)
               FILTER (WHERE fd.verdict = 'allocated' AND ee.average_price IS NOT NULL)
               AS average_slippage_pct,
           AVG(pre.stressed_cost_amount) FILTER (WHERE pre.stressed_cost_amount IS NOT NULL)
               AS average_stressed_cost_usd,
           MAX(pre.account_drawdown_pct) AS max_observed_account_drawdown_pct
    FROM strategy_signals s
    LEFT JOIN strategy_outcomes o
      ON o.signal_id = s.signal_id
     AND o.rule_set_version = %(outcome_version)s
     AND o.input_rule_set_version = %(input_version)s
    LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = s.signal_id
    LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
    LEFT JOIN entry_execution ee ON ee.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN entry_order eo ON eo.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN strategy_entry_preflights pre ON pre.signal_id = s.signal_id
    WHERE s.verdict = 'fired' AND s.signal_kind = 'entry'
      AND s.strategy_version = ANY(%(versions)s)
    GROUP BY s.strategy_id, s.strategy_version
"""


def load_attribution(
    conn: psycopg.Connection[Any],
    *,
    versions: Sequence[str],
    outcome_version: str,
    input_version: str,
) -> dict[tuple[str, str], StrategyAttribution]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            _ATTRIBUTION_SQL,
            {
                "versions": versions,
                "outcome_version": outcome_version,
                "input_version": input_version,
            },
        )
        rows = cur.fetchall()
    result: dict[tuple[str, str], StrategyAttribution] = {}
    for row in rows:
        fired = int(row["fired_entries"])
        funded = int(row["funded_entries"])
        funded_shadow = row["funded_shadow_average_return_pct"]
        rejected_shadow = row["rejected_shadow_average_return_pct"]
        resolved = int(row["resolved_entries"])
        winners = int(row["winning_entries"])
        filled = int(row["filled_entries"])
        broker_rejected = int(row["broker_rejected_entries"])
        result[(str(row["strategy_id"]), str(row["strategy_version"]))] = StrategyAttribution(
            fired_entries=fired,
            funded_entries=funded,
            rejected_entries=int(row["rejected_entries"]),
            resolved_entries=resolved,
            winning_entries=winners,
            win_rate=Decimal(winners) / Decimal(resolved) if resolved else None,
            median_days_to_outcome=(
                Decimal(str(row["median_days_to_outcome"])) if row["median_days_to_outcome"] is not None else None
            ),
            signals_last_30_days=int(row["signals_last_30_days"]),
            shadow_average_return_pct=row["shadow_average_return_pct"],
            funded_shadow_average_return_pct=funded_shadow,
            rejected_shadow_average_return_pct=rejected_shadow,
            opportunity_gap_pct=(
                rejected_shadow - funded_shadow if rejected_shadow is not None and funded_shadow is not None else None
            ),
            funded_capture_rate=Decimal(funded) / Decimal(fired) if fired else None,
            filled_entries=filled,
            broker_rejected_entries=broker_rejected,
            fill_rate=Decimal(filled) / Decimal(funded) if funded else None,
            broker_rejection_rate=Decimal(broker_rejected) / Decimal(funded) if funded else None,
            average_slippage_pct=row["average_slippage_pct"],
            average_stressed_cost_usd=row["average_stressed_cost_usd"],
            max_observed_account_drawdown_pct=row["max_observed_account_drawdown_pct"],
        )
    return result


#: Entry-signal census aggregates for one strategy version. The population is
#: ``strategy_signal_daily_counts`` — the DURABLE census — and NOT
#: ``strategy_signals``. See ``derive_fire_rate`` for why that distinction is the
#: whole point of this query.
#:
#: ⚠ ``sum(...) FILTER`` returns NULL, not 0, for a version with no matching
#: bucket. s2 has no fired bucket at all, so an uncoalesced numerator reports a
#: scanned strategy as unmeasured. Coalesced here rather than at the call site so
#: the pure derivation never has to distinguish "absent bucket" from "no rows".
#: ⚠ PER BAR DATE, not per version (#2811). The caller must partition these rows
#: by decision-date membership before it can sum anything, and a pre-summed
#: version-level row cannot be partitioned afterwards. The aggregates themselves
#: are unchanged; only the grain is.
_FIRE_RATE_SQL = """
    SELECT strategy_id, strategy_version, signal_bar_date,
           COALESCE(SUM(row_count) FILTER (WHERE verdict = 'fired'), 0)
               AS fired_entry_signals,
           COALESCE(SUM(row_count) FILTER (WHERE verdict IN ('fired', 'not_fired')), 0)
               AS evaluable_entry_decisions,
           COALESCE(SUM(row_count) FILTER (WHERE verdict = 'not_evaluable'), 0)
               AS not_evaluable_entry_decisions
    FROM strategy_signal_daily_counts
    WHERE signal_kind = 'entry' AND strategy_version = ANY(%(versions)s)
    GROUP BY strategy_id, strategy_version, signal_bar_date
"""

#: The scan's OWN decision dates, republished by `publish_decision_calendar`. Read
#: rather than recomputed for the reason that function's docstring gives at length:
#: recomputing resolves a historical version against today's corpus.
#:
#: ⚠⚠ DRIVEN FROM THE PUBLICATION HEADER, LEFT-JOINED TO THE DATES — never from the
#: dates alone. A rule that names no date in this corpus publishes a header and zero
#: dates, and selecting only from the date table would make that byte-identical to a
#: version that never published at all. The reader's whole contract is that those
#: two are different answers (`0` refuses the share, `None` leaves it untouched).
_DECISION_CALENDAR_SQL = """
    SELECT p.strategy_id, p.strategy_version, p.frontier_date, c.decision_date
    FROM strategy_decision_calendar_publications p
    LEFT JOIN strategy_decision_calendar c
           ON c.strategy_id = p.strategy_id AND c.strategy_version = p.strategy_version
    WHERE p.strategy_version = ANY(%(versions)s)
"""


def derive_fire_rate(
    *,
    scanned_days: int,
    fired_days: int,
    fired_entry_signals: int,
    evaluable_entry_decisions: int,
    not_evaluable_entry_decisions: int,
    first_scanned_bar: date | None,
    last_scanned_bar: date | None,
    decision_days: int | None = None,
) -> StrategyFireRate:
    """Turn one version's entry-census aggregates into the two rates.

    Spec: ``docs/proposals/ta/2026-08-14-strategy-fire-rate.md``. Issue #2623 gap 2.

    ⚠⚠ THE SOURCE IS THE CENSUS BECAUSE ``strategy_signals`` IS SPARSE. Fired rows
    stay in ``strategy_signals`` durably because outcomes refer to ``signal_id``;
    routine negatives are pruned to a 90-day partition and survive only as
    ``strategy_signal_daily_counts`` rows. So ``strategy_signals`` can see only the
    days on which something fired — a correct numerator over an unusable
    denominator. Measured on the full population 2026-08-14: it shows 1 bar date
    where the census shows 5 on four of nine version keys (a **5x** overstatement),
    and s2 — which scanned and fired nothing — has no rows in it at all, so it
    would vanish from the surface rather than read an honest zero. See
    ``docs/review-prevention-log.md`` on a sparse table's storage predicate being
    part of the census definition.

    ⚠⚠ TWO RATES, BECAUSE THEY ANSWER DIFFERENT QUESTIONS AND NEITHER SUBSTITUTES.

    ``fired_share_of_evaluable`` is the propensity: what fraction of the decisions
    the strategy was actually able to make came out ``fired``. Dimensionless, so
    universe growth does not move it. ``not_evaluable`` decisions are excluded from
    its denominator — those bars were never judged (``insufficient_warmup``,
    ``no_fill_bar`` and six siblings, ``sql/276:20``), and counting them as declined
    opportunities would suppress the share for a chance never offered.

    ``entries_per_calendar_week`` is the throughput: how many opportunities land per
    week of market. It is universe-size-dependent BY DESIGN — the operator trades
    this universe — which is exactly why it cannot stand alone as "the fire rate".

    ⚠⚠ THE WEEKLY RATE IS MEASURED OFF THE BAR-DATE AXIS AND REFUSES A ONE-DAY
    AXIS. It is NOT ``fires per scanned day x 5``. ``strategy_statistics`` settled
    this class of decision already: *"the annualisation factor is measured off the
    date axis, never the 252 convention … 252 is a convention, not a source rule"*,
    and ``periods_per_year`` raises rather than invent a span for a single-date
    axis, because *"inventing one would put a divide-by-zero behind a plausible
    number"*. A frozen trading-week constant is that same mistake at the week
    scale, so the span comes off the axis and a rate that the axis cannot carry is
    ``None`` with a stated reason.

    ⚠ Every current strategy version holds exactly one scanned bar date today, so
    the weekly rate is ``None`` for all four. That is the refusal working, not a
    gap — the prior versions carry 5-date axes and rate fine.

    ⚠⚠ THE TWO NULLS NEED TWO REASONS, BECAUSE THEY ARE INDEPENDENT. A version
    with two or more scanned days on which EVERY bar was ``not_evaluable`` has a
    computable weekly rate (no fires over a real span is ``0.00``) and an
    UNCOMPUTABLE share — its propensity is unknown, not zero, because it was never
    offered a decision. One enum cannot carry both, so a single
    ``rate_unavailable_reason`` left that share null with nothing saying why. Each
    nullable value names its own reason, and the invariant is exact in both
    directions: a value is ``None`` if and only if its reason is not.
    """
    if scanned_days == 0:
        return StrategyFireRate(
            decision_days=decision_days,
            share_unavailable_reason="never_scanned",
            weekly_rate_unavailable_reason="never_scanned",
        )

    # The producer's invariants, asserted rather than trusted. A decision day is a
    # scanned day by construction (it is drawn from the census's own dates), and a
    # day on which the strategy fired is a decision day under registry rule 3.
    if decision_days is not None and not fired_days <= decision_days <= scanned_days:
        raise ValueError(
            f"fired_days {fired_days} <= decision_days {decision_days} <= scanned_days {scanned_days} "
            "is violated — a fire off the decision calendar, or a decision day the census never carried"
        )
    if fired_entry_signals > evaluable_entry_decisions:
        raise ValueError(
            f"fired_entry_signals {fired_entry_signals} exceeds evaluable_entry_decisions "
            f"{evaluable_entry_decisions} — the share's numerator and denominator are not one population"
        )

    share: Decimal | None = None
    share_reason: ShareUnavailableReason | None = None
    if decision_days == 0:
        # ⚠ ORDERED BEFORE the evaluable test and NOT merged with it. With no
        # decision date covered, `evaluable_entry_decisions` is zero because the
        # partition is empty, not because every bar refused — and
        # `no_evaluable_decisions` would assert the second. The two are
        # distinguishable only here, in the order.
        share_reason = "no_decision_date_scanned"
    elif evaluable_entry_decisions > 0:
        share = (Decimal(fired_entry_signals) / Decimal(evaluable_entry_decisions)).quantize(_SHARE_PRECISION)
    else:
        share_reason = "no_evaluable_decisions"

    per_week: Decimal | None = None
    weekly_reason: WeeklyRateUnavailableReason | None = None
    # `periods_per_year`'s own guard, at the week scale: an axis needs two distinct
    # dates and a positive span before any rate can be measured off it.
    span_days = 0
    if first_scanned_bar is not None and last_scanned_bar is not None:
        span_days = (last_scanned_bar - first_scanned_bar).days
    if scanned_days < 2 or span_days <= 0:
        weekly_reason = "single_scan_day"
    else:
        per_week = (Decimal(fired_entry_signals) * Decimal(_DAYS_PER_WEEK) / Decimal(span_days)).quantize(
            _RATE_PRECISION
        )

    return StrategyFireRate(
        scanned_days=scanned_days,
        decision_days=decision_days,
        fired_days=fired_days,
        fired_entry_signals=fired_entry_signals,
        evaluable_entry_decisions=evaluable_entry_decisions,
        not_evaluable_entry_decisions=not_evaluable_entry_decisions,
        fired_share_of_evaluable=share,
        entries_per_calendar_week=per_week,
        first_scanned_bar=first_scanned_bar,
        last_scanned_bar=last_scanned_bar,
        share_unavailable_reason=share_reason,
        weekly_rate_unavailable_reason=weekly_reason,
    )


def _panel_floor(strategy_id: str) -> int:
    """The smallest cross-section this strategy can act on — its OWN declaration.

    Not a threshold chosen here. ``StrategyEntry.min_participants`` is the panel
    floor the strategy refuses below (``MIN_CROSS_SECTION = 10`` for S-2, whose own
    comment is *"below ten names, ``N // 10`` is zero and no name can be in the top
    tenth of the panel"*), and a date carrying fewer evaluable rows than that is one
    on which the strategy provably could not have fired.

    ⚠ WITHOUT THIS THE FIX REPRODUCES ITS OWN ROOT CAUSE ONE LAYER UP. Census bar
    dates are per INSTRUMENT: a cold start writes each instrument's last bar
    strictly before the frontier, so a single sparse series lands a lone row on an
    arbitrary date — which is exactly how 2026-07-31 and 2026-08-04 came to look
    like scanned days (#2811). One such row falling on a rebalance date would set
    ``decision_days = 1`` and re-enable the fake share this exists to remove.

    ⚠ A strategy absent from the manifest floors at 1. It has no published calendar
    either, so it never reaches the decision-day partition; this is the defensive
    arm, not a live path.
    """
    entry = STRATEGY_MANIFEST.get(strategy_id)
    if entry is None or entry.min_participants is None:
        return 1
    return entry.min_participants


def load_fire_rate(
    conn: psycopg.Connection[Any], *, versions: Sequence[str]
) -> dict[tuple[str, str], StrategyFireRate]:
    """Entry fire evidence per ``(strategy_id, strategy_version)`` from the census.

    A key absent from the result has never been scanned; the caller supplies
    ``StrategyFireRate()``, whose default ``rate_unavailable_reason`` says so.

    ⚠⚠ THE SHARE IS MEASURED ON DECISION DAYS ONLY, WHICH IS NOT AN OPTIMISATION
    (#2811). A periodic strategy is ``not_fired`` on every bar that is not one of
    its rebalance dates — `strategy_registry` rule 3, correctly, since the data was
    present and the calendar is what excluded it. Summing those into the
    denominator makes the share a measurement of the CALENDAR rather than of the
    strategy, and when no decision date has been covered at all it manufactures a
    confident ``0.0000`` out of a non-measurement.

    ⚠ `scanned_days` stays whole-coverage while the four decision counts do not, so
    the two populations are named separately on the card rather than silently
    averaged. Everything the SHARE is built from — numerator, denominator and the
    reported not-evaluable exclusion — comes from the one decision-day population
    (#2802's rule), so the ratio is the ratio of the two numbers rendered beside it.

    ⚠ `entries_per_calendar_week` deliberately keeps the whole-COVERAGE span in its
    divisor while its numerator is decision-day restricted, and the two do not
    conflict: rule 3 makes a fire on a non-decision bar unrepresentable, so the
    restriction removes nothing from the numerator, and the metric's own question is
    "how many opportunities land per week of MARKET" — which is a coverage span by
    definition, not a rebalance-date one.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_FIRE_RATE_SQL, {"versions": list(versions)})
        rows = cur.fetchall()
        cur.execute(_DECISION_CALENDAR_SQL, {"versions": list(versions)})
        calendar_rows = cur.fetchall()

    # ⚠ The header creates the key; the LEFT JOIN's NULL date adds nothing to it. A
    # published-but-empty calendar is therefore an EMPTY SET and an unpublished one
    # is a MISSING KEY, which is the distinction the whole reader turns on.
    calendars: dict[tuple[str, str], set[date]] = {}
    calendar_frontiers: dict[tuple[str, str], date] = {}
    for row in calendar_rows:
        key = (str(row["strategy_id"]), str(row["strategy_version"]))
        dates = calendars.setdefault(key, set())
        calendar_frontiers[key] = row["frontier_date"]
        if row["decision_date"] is not None:
            dates.add(row["decision_date"])

    per_version: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        per_version[(str(row["strategy_id"]), str(row["strategy_version"]))].append(row)

    rates: dict[tuple[str, str], StrategyFireRate] = {}
    for key, version_rows in per_version.items():
        strategy_id, strategy_version = key
        # Absent, not empty: a version that never published a calendar has an
        # UNKNOWN cadence, and `None` is what carries that through to the card.
        calendar = calendars.get(key)
        floor = _panel_floor(strategy_id)

        scanned_bars = {row["signal_bar_date"] for row in version_rows}
        if calendar is None:
            counted = version_rows
            decision_days: int | None = None
        else:
            counted = [
                row
                for row in version_rows
                if row["signal_bar_date"] in calendar and int(row["evaluable_entry_decisions"]) >= floor
            ]
            decision_days = len({row["signal_bar_date"] for row in counted})
            off_calendar = sum(
                int(row["fired_entry_signals"]) for row in version_rows if row["signal_bar_date"] not in calendar
            )
            if off_calendar:
                # Unrepresentable under registry rule 3, so its appearance is a
                # producer-invariant violation and not a rounding concern. Dropped
                # silently is how a corruption stays invisible.
                logger.warning(
                    "load_fire_rate: %s/%s carries %d fired entry signals OFF its decision calendar — "
                    "registry rule 3 makes a fire on a non-decision bar unrepresentable",
                    strategy_id,
                    strategy_version,
                    off_calendar,
                )
            # ⚠ The calendar and the census can describe DIFFERENT corpus states, and
            # the stamp is what makes that visible instead of silent. Publication is
            # deliberately not tied to writing census rows (otherwise the card is
            # inert on any run with nothing to write), so a historical backfill can
            # move a month boundary under a stationary frontier. The direction is
            # fail-safe — a calendar disagreeing with the census yields "no decision
            # date scanned", never a wrong share — but a share refused for a reason
            # nobody can see is the defect this whole ticket is about.
            published_frontier = calendar_frontiers[key]
            if scanned_bars and max(scanned_bars) > published_frontier:
                logger.warning(
                    "load_fire_rate: %s/%s has census bars to %s but its decision calendar was published "
                    "at frontier %s — the two describe different corpus states",
                    strategy_id,
                    strategy_version,
                    max(scanned_bars),
                    published_frontier,
                )

        rates[key] = derive_fire_rate(
            scanned_days=len(scanned_bars),
            decision_days=decision_days,
            fired_days=len({row["signal_bar_date"] for row in counted if int(row["fired_entry_signals"]) > 0}),
            fired_entry_signals=sum(int(row["fired_entry_signals"]) for row in counted),
            evaluable_entry_decisions=sum(int(row["evaluable_entry_decisions"]) for row in counted),
            not_evaluable_entry_decisions=sum(int(row["not_evaluable_entry_decisions"]) for row in counted),
            first_scanned_bar=min(scanned_bars),
            last_scanned_bar=max(scanned_bars),
        )
    return rates


_OWNED_LIFECYCLE_SQL = """
    WITH closes AS (
        SELECT position_id, COUNT(*) AS close_event_count,
               SUM(realized_pnl_usd) AS realised_pnl,
               SUM(fees_usd) AS fees,
               BOOL_AND(realized_pnl_usd IS NOT NULL) AS pnl_complete,
               BOOL_AND(fees_usd IS NOT NULL) AS fees_complete
        FROM trade_events
        WHERE event_kind = 'close'
        GROUP BY position_id
    )
    SELECT s.strategy_id, s.strategy_version, t.strategy_trade_id, t.status AS trade_status,
           own.ownership_id, own.broker_position_id, own.status AS ownership_status,
           bp.position_id AS active_broker_position_id, bp.is_buy, bp.units, bp.amount,
           bp.open_rate, bp.open_conversion_rate, bp.total_fees,
           q.last, q.bid, q.ask, pd.close AS daily_close,
           COALESCE(c.close_event_count, 0) AS close_event_count,
           c.realised_pnl, c.fees, c.pnl_complete, c.fees_complete
    FROM strategy_funding_decisions fd
    JOIN strategy_signals s ON s.signal_id = fd.signal_id
    LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
    LEFT JOIN strategy_position_ownership own ON own.strategy_trade_id = t.strategy_trade_id
    LEFT JOIN broker_positions bp ON bp.position_id = own.broker_position_id
    LEFT JOIN quotes q ON q.instrument_id = bp.instrument_id
    LEFT JOIN LATERAL (
        SELECT p.close
        FROM price_daily p
        WHERE p.instrument_id = bp.instrument_id AND p.close IS NOT NULL
        ORDER BY p.price_date DESC
        LIMIT 1
    ) pd ON TRUE
    LEFT JOIN closes c ON c.position_id = own.broker_position_id
    WHERE fd.verdict = 'allocated' AND s.strategy_version = ANY(%(versions)s)
    ORDER BY s.strategy_id, s.strategy_version, t.strategy_trade_id, own.ownership_id
"""


def _mark(row: dict[str, Any]) -> Decimal | None:
    price = resolve_quote_price(
        float(row["last"]) if row["last"] is not None else None,
        float(row["bid"]) if row["bid"] is not None else None,
        float(row["ask"]) if row["ask"] is not None else None,
    )
    if price is not None:
        return Decimal(str(price))
    daily = row["daily_close"]
    return Decimal(str(daily)) if daily is not None and Decimal(str(daily)) > 0 else None


def load_owned_pnl(conn: psycopg.Connection[Any], *, versions: Sequence[str]) -> dict[tuple[str, str], StrategyPnl]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_OWNED_LIFECYCLE_SQL, {"versions": versions})
        rows = list(cur.fetchall())

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["strategy_id"]), str(row["strategy_version"]))].append(row)

    result: dict[tuple[str, str], StrategyPnl] = {}
    for key, strategy_rows in grouped.items():
        realised = Decimal("0")
        unrealised = Decimal("0")
        invested = Decimal("0")
        fees = Decimal("0")
        reasons: set[str] = set()
        ownership_count = 0
        active_count = 0
        close_count = 0
        trade_ids: set[int] = set()
        for row in strategy_rows:
            if row["strategy_trade_id"] is None:
                reasons.add("funding_not_reconciled_to_trade")
                continue
            trade_ids.add(int(row["strategy_trade_id"]))
            ownership_id = row["ownership_id"]
            if ownership_id is None:
                if row["trade_status"] != "failed":
                    reasons.add("trade_not_reconciled_to_position")
                continue
            ownership_count += 1
            row_close_count = int(row["close_event_count"])
            close_count += row_close_count
            if row_close_count:
                if row["pnl_complete"] is True:
                    realised += Decimal(str(row["realised_pnl"]))
                else:
                    reasons.add("realised_pnl_missing_from_history")
                if row["fees_complete"] is True:
                    fees += Decimal(str(row["fees"]))
                else:
                    reasons.add("fees_missing_from_history")
            elif row["ownership_status"] == "released":
                reasons.add("released_position_missing_close_history")

            if row["ownership_status"] != "active":
                continue
            active_count += 1
            if row["active_broker_position_id"] is None:
                reasons.add("active_position_missing_from_broker_snapshot")
                continue
            invested += Decimal(str(row["amount"]))
            mark = _mark(row)
            if mark is None:
                reasons.add("active_position_mark_unavailable")
                continue
            units = Decimal(str(row["units"]))
            open_rate = Decimal(str(row["open_rate"]))
            conversion = Decimal(str(row["open_conversion_rate"]))
            direction = Decimal("1") if bool(row["is_buy"]) else Decimal("-1")
            unrealised += direction * units * (mark - open_rate) * conversion

        realised_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "realised_pnl_missing_from_history",
            "released_position_missing_close_history",
        }.intersection(reasons)
        unrealised_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "active_position_missing_from_broker_snapshot",
            "active_position_mark_unavailable",
        }.intersection(reasons)
        invested_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "active_position_missing_from_broker_snapshot",
        }.intersection(reasons)
        fees_known = not {
            "funding_not_reconciled_to_trade",
            "trade_not_reconciled_to_position",
            "released_position_missing_close_history",
            "fees_missing_from_history",
        }.intersection(reasons)
        realised_value = realised if realised_known else None
        unrealised_value = unrealised if unrealised_known else None
        result[key] = StrategyPnl(
            strategy_trade_count=len(trade_ids),
            owned_position_count=ownership_count,
            active_position_count=active_count,
            close_event_count=close_count,
            invested_capital=invested if invested_known else None,
            realised_pnl=realised_value,
            unrealised_pnl=unrealised_value,
            total_pnl=(
                realised_value + unrealised_value
                if realised_value is not None and unrealised_value is not None
                else None
            ),
            observed_fees=fees if fees_known else None,
            complete=not reasons,
            incomplete_reasons=tuple(sorted(reasons)),
            reconciled_realised_pnl=realised,
        )
    return result


def realised_pnl_for_keys(
    pnl_by_strategy: dict[tuple[str, str], StrategyPnl],
    keys: Sequence[tuple[str, str]],
) -> dict[tuple[str, str], Decimal] | None:
    """Project reconciled realised values, failing closed on any unknown."""
    result: dict[tuple[str, str], Decimal] = {}
    for key in keys:
        pnl = pnl_by_strategy.get(key, StrategyPnl())
        if {
            "realised_pnl_missing_from_history",
            "released_position_missing_close_history",
        }.intersection(pnl.incomplete_reasons):
            return None
        result[key] = pnl.reconciled_realised_pnl
    return result


def load_paper_realised_pnl(conn: psycopg.Connection[Any]) -> dict[tuple[str, str], Decimal] | None:
    """Return exact-owned realised P&L for every paper deployment.

    Old strategy versions remain part of the shared pot after they are retired;
    limiting this calculation to the current manifest would make realised gains
    or losses disappear from the capital base. ``None`` is fail-closed whenever
    any deployed lifecycle cannot be reconciled completely.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT strategy_id,strategy_version
        FROM strategy_deployments
        WHERE mode='paper'
        """
    ).fetchall()
    keys = [(str(row[0]), str(row[1])) for row in rows]
    pnl_by_strategy = load_owned_pnl(conn, versions=sorted({version for _strategy_id, version in keys}))
    return realised_pnl_for_keys(pnl_by_strategy, keys)


_CONTROL_SQL = """
    WITH current_stage AS (
        SELECT DISTINCT ON (strategy_id, strategy_version)
               strategy_id, strategy_version, to_stage
        FROM strategy_promotions
        WHERE strategy_version = ANY(%(versions)s)
        ORDER BY strategy_id, strategy_version, promotion_id DESC
    ), strategy_keys AS (
        SELECT strategy_id, strategy_version FROM current_stage
        UNION
        SELECT strategy_id, strategy_version
        FROM strategy_deployments
        WHERE strategy_version = ANY(%(versions)s) AND mode = 'paper'
    ), pinned AS (
        SELECT p.strategy_id, p.strategy_version,
               COUNT(*) AS result_count,
               COUNT(*) FILTER (WHERE
                   r.expectancy_ci_low_pct IS NOT NULL
                   AND r.namespace = 'hold_out'
                   AND r.window_start >= DATE '2022-01-01'
                   AND r.universe_basis = 'survivorship_free'
                   AND r.carry_unmodelled = false
                   AND r.fx_unmodelled = false
                   AND r.trial_count IS NOT NULL
                   AND r.deflated_sharpe IS NOT NULL
                   AND r.effective_sample_size IS NOT NULL
                   AND control_support.candidate_count = 1
                   AND control_result.synthetic_control_passed = true
               ) AS qualified_result_count
        FROM strategy_promotions p
        JOIN strategy_promotion_results pr ON pr.promotion_id = p.promotion_id
        JOIN strategy_results_store r ON r.result_id = pr.result_id
        LEFT JOIN strategy_result_control_support control_support
          ON control_support.holdout_result_id = r.result_id
        LEFT JOIN strategy_results_store control_result
          ON control_result.result_id = control_support.control_result_id
        WHERE p.strategy_version = ANY(%(versions)s)
        GROUP BY p.strategy_id, p.strategy_version
    ), reserved AS (
        SELECT fd.deployment_id, COALESCE(SUM(fd.amount), 0) AS amount
        FROM strategy_funding_decisions fd
        LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
        WHERE fd.verdict = 'allocated'
          AND (t.strategy_trade_id IS NULL OR t.status NOT IN ('closed', 'failed'))
        GROUP BY fd.deployment_id
    )
    SELECT keys.strategy_id, keys.strategy_version, cs.to_stage,
           COALESCE(pin.result_count, 0) AS result_count,
           COALESCE(pin.qualified_result_count, 0) AS qualified_result_count,
           d.deployment_id, d.capital_limit, d.currency, d.enabled, d.revision,
           COALESCE(res.amount, 0) AS reserved_capital,
           (pol.deployment_id IS NOT NULL) AS policy_configured,
           pol.max_drawdown_pct AS max_drawdown_limit_pct,
           pol.ticket_sizing_mode, pol.ticket_fraction,
           pol.fixed_ticket_amount, pol.max_ticket_amount
    FROM strategy_keys keys
    LEFT JOIN current_stage cs USING (strategy_id, strategy_version)
    LEFT JOIN pinned pin USING (strategy_id, strategy_version)
    LEFT JOIN strategy_deployments d
      ON d.strategy_id = keys.strategy_id AND d.strategy_version = keys.strategy_version
     AND d.mode = 'paper'
    LEFT JOIN reserved res ON res.deployment_id = d.deployment_id
    LEFT JOIN strategy_execution_policies pol ON pol.deployment_id = d.deployment_id
"""


def load_control_state(
    conn: psycopg.Connection[Any], *, versions: Sequence[str]
) -> dict[tuple[str, str], StrategyControlState]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_CONTROL_SQL, {"versions": versions})
        rows = cur.fetchall()
    return {
        (str(row["strategy_id"]), str(row["strategy_version"])): StrategyControlState(
            stage=str(row["to_stage"]) if row["to_stage"] is not None else None,
            pinned_evidence_ready=(
                int(row["result_count"]) > 0 and int(row["qualified_result_count"]) == int(row["result_count"])
            ),
            deployment_id=int(row["deployment_id"]) if row["deployment_id"] is not None else None,
            capital_limit=Decimal(str(row["capital_limit"] or 0)),
            currency=str(row["currency"] or "USD"),
            enabled=bool(row["enabled"]),
            revision=int(row["revision"]) if row["revision"] is not None else None,
            reserved_capital=Decimal(str(row["reserved_capital"])),
            policy_configured=bool(row["policy_configured"]),
            max_drawdown_limit_pct=(
                Decimal(str(row["max_drawdown_limit_pct"])) if row["max_drawdown_limit_pct"] is not None else None
            ),
            ticket_sizing_mode=(str(row["ticket_sizing_mode"]) if row["ticket_sizing_mode"] is not None else None),
            ticket_fraction=(Decimal(str(row["ticket_fraction"])) if row["ticket_fraction"] is not None else None),
            fixed_ticket_amount=(
                Decimal(str(row["fixed_ticket_amount"])) if row["fixed_ticket_amount"] is not None else None
            ),
            max_ticket_amount=(
                Decimal(str(row["max_ticket_amount"])) if row["max_ticket_amount"] is not None else None
            ),
        )
        for row in rows
    }


def load_entry_block_state(conn: psycopg.Connection[Any]) -> StrategyEntryBlockState:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT is_active, reason, activated_at, activated_by FROM kill_switch ORDER BY id DESC LIMIT 1")
        kill = cur.fetchone()
        cur.execute("SELECT reason FROM strategy_execution_blocks WHERE active ORDER BY source")
        blocks = tuple(str(row["reason"]) for row in cur.fetchall())
        cur.execute("SELECT enable_auto_trading, enable_live_trading FROM runtime_config ORDER BY id DESC LIMIT 1")
        runtime = cur.fetchone()
    # Missing singleton rows are fail-closed and visible rather than treated as
    # disabled-but-healthy configuration.
    if kill is None:
        return StrategyEntryBlockState(True, "kill switch state unavailable", None, None, blocks, False, False)
    if runtime is None:
        blocks = (*blocks, "runtime configuration unavailable")
    elif not bool(runtime["enable_auto_trading"]):
        blocks = (*blocks, "automatic trading disabled")
    return StrategyEntryBlockState(
        global_kill_active=bool(kill["is_active"]),
        global_kill_reason=str(kill["reason"]) if kill["reason"] is not None else None,
        global_kill_activated_at=kill["activated_at"],
        global_kill_activated_by=(str(kill["activated_by"]) if kill["activated_by"] is not None else None),
        execution_block_reasons=blocks,
        auto_trading_enabled=bool(runtime["enable_auto_trading"]) if runtime is not None else False,
        live_trading_enabled=bool(runtime["enable_live_trading"]) if runtime is not None else False,
    )


__all__ = [
    "ShareUnavailableReason",
    "StrategyAttribution",
    "StrategyControlState",
    "StrategyEntryBlockState",
    "StrategyFireRate",
    "StrategyPnl",
    "WeeklyRateUnavailableReason",
    "derive_fire_rate",
    "load_attribution",
    "load_control_state",
    "load_entry_block_state",
    "load_fire_rate",
    "load_owned_pnl",
]
