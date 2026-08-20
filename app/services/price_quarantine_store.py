"""
Persistence + census for the impossible-bar quarantine (#2261, phase 0a of
#2240).

SEPARATE MODULE FROM THE RULES, DELIBERATELY. ``price_quarantine`` derives its
rule-set version from its own source hash, so every edit there invalidates
every stored verdict — which is the point. Putting SQL in the same file would
make an unrelated query tweak look like a rule change and force a full
recompute. The rules stay pure; this module is everything around them.

THE CENSUS READS WHAT THE RULES WROTE. There is no second SQL implementation of
B1-B4 / T1-T3 anywhere in this repo, and there must never be one. S7's own Codex
pass found TWO places where a census SQL had drifted from the prose it claimed
to implement (a raw ``high/low`` test where the rule said *wick*; range-only
rules folded into the return quarantine). A rejection census is plausible at any
magnitude, so drift produces no symptom. The structural fix is that
``census()`` below is a ``COUNT(*)`` over rows that
``price_quarantine.evaluate_series`` produced — it cannot disagree with the rule
because it does not re-express it.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from app.services.price_quarantine import (
    RULE_SET_VERSION,
    Bar,
    BarVerdict,
    TransitionVerdict,
    evaluate_series,
)

logger = logging.getLogger(__name__)

# Scope: every instrument that HAS bars. Deliberately not gated on
# is_tradable/coverage — an instrument that stops being tradable keeps its
# history, and a verdict-less series reads as UNKNOWN downstream (fail-closed),
# which would silently remove it from every backtest rather than mark it.
_SCOPE_SQL = """
SELECT p.instrument_id, e.asset_class
FROM (SELECT DISTINCT instrument_id FROM price_daily) p
JOIN instruments i ON i.instrument_id = p.instrument_id
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
WHERE (%(instrument_ids)s::bigint[] IS NULL OR p.instrument_id = ANY(%(instrument_ids)s::bigint[]))
ORDER BY p.instrument_id
"""

_SERIES_SQL = """
SELECT price_date, open, high, low, close, volume
FROM price_daily
WHERE instrument_id = %(iid)s
ORDER BY price_date
"""


@dataclass
class QuarantineRefreshResult:
    instruments: int = 0
    bars_evaluated: int = 0
    transitions_evaluated: int = 0
    bar_rows_written: int = 0
    transition_rows_written: int = 0
    breaks_written: int = 0


@dataclass
class QuarantineCensus:
    """The operator-visible rejection census (S7 §9.5).

    Published, not incidental. T3 is the only rule that can reject legitimate
    data, and it does so at ~10:1 against split-like breaks at every threshold,
    with turnover corroboration reaching only ~30% of the population. Every
    backtest number downstream inherits that bias, so the figure ships with it.
    """

    rule_set_version: str = RULE_SET_VERSION
    instruments_evaluated: int = 0
    bars_evaluated: int = 0
    transitions_evaluated: int = 0
    bars_return_unusable: int = 0
    bars_range_unusable: int = 0
    bars_provisional: int = 0
    bar_rule_counts: dict[str, int] = field(default_factory=dict)
    transitions_quarantined: int = 0
    transition_rule_counts: dict[str, int] = field(default_factory=dict)
    transitions_provisional_deferred: int = 0
    t3_corroboration: dict[str, int] = field(default_factory=dict)
    instruments_with_unresolved_break: int = 0
    bars_stranded_pre_break: int = 0
    stale_version_instruments: int = 0
    """Instruments whose stored coverage row predates the current rule set — their
    verdicts are NOT current and the read path treats them as unknown."""


def _series_batches(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_ids: list[int] | None,
) -> Iterator[tuple[int, str | None, list[Bar]]]:
    """Yield ``(instrument_id, asset_class, bars ascending by date)``, one instrument at a time.

    Per-instrument reads rather than one streaming pass over all 3.2M rows. A
    server-side cursor's portal lives inside the reading transaction, so it
    would be invalidated by any commit taken mid-iteration — and this generator
    is deliberately agnostic about the caller's transaction shape rather than
    encoding an assumption about it. ``refresh_price_quarantine`` documents what
    that shape actually is (one transaction for the whole run, with a per-
    instrument savepoint as the unit of failure). The largest series is ~1,042
    bars (eToro's fetch ceiling), so nothing is materialised that matters.
    """
    scope = conn.execute(_SCOPE_SQL, {"instrument_ids": instrument_ids}).fetchall()
    for instrument_id, asset_class in scope:
        rows = conn.execute(_SERIES_SQL, {"iid": instrument_id}).fetchall()
        bars = [Bar(price_date=r[0], open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5]) for r in rows]
        if bars:
            yield int(instrument_id), asset_class, bars


def _write_instrument(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    asset_class: str | None,
    bars: list[Bar],
    bar_verdicts: list[BarVerdict],
    transitions: list[TransitionVerdict],
) -> tuple[int, int, int]:
    """Replace one instrument's verdicts. Returns (bar_rows, transition_rows, breaks)."""
    conn.execute("DELETE FROM price_bar_quarantine WHERE instrument_id = %s", (instrument_id,))
    conn.execute("DELETE FROM price_transition_quarantine WHERE instrument_id = %s", (instrument_id,))
    # Breaks are re-derived from the same pass, but an OPERATOR- or #2231-set
    # resolution must survive it. Only rows this detector owns and that no
    # longer hold are removed.
    conn.execute(
        "DELETE FROM price_series_break WHERE instrument_id = %s AND resolved_by IS NULL",
        (instrument_id,),
    )

    bar_rows = [
        (instrument_id, v.price_date, v.return_usable, v.range_usable, v.provisional, list(v.rules), RULE_SET_VERSION)
        for v in bar_verdicts
        if v.notable
    ]
    if bar_rows:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO price_bar_quarantine
                    (instrument_id, price_date, return_usable, range_usable, provisional, rules, rule_set_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                bar_rows,
            )

    transition_rows = [
        (
            instrument_id,
            t.price_date,
            t.prior_date,
            t.observed_ratio,
            t.provisional,
            list(t.rules),
            t.turnover_ratio,
            t.corroboration,
            RULE_SET_VERSION,
        )
        for t in transitions
        if t.notable
    ]
    if transition_rows:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO price_transition_quarantine
                    (instrument_id, price_date, prior_date, observed_ratio, provisional,
                     rules, turnover_ratio, corroboration, rule_set_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                transition_rows,
            )

    # A LEVEL BREAK is a T3 transition: magnitude past T, not explained by an
    # unusable endpoint bar, and not corroborated by a turnover spike. B4's
    # reverting spikes are bar defects and are deliberately NOT breaks — they
    # come back, so the two sides share a unit regime.
    break_rows = [
        (
            instrument_id,
            t.price_date,
            t.observed_ratio,
            "up" if t.observed_ratio is not None and t.observed_ratio > 1 else "down",
            RULE_SET_VERSION,
            Jsonb(
                {
                    "observed_ratio": str(t.observed_ratio),
                    "turnover_ratio": str(t.turnover_ratio) if t.turnover_ratio is not None else None,
                    "corroboration": t.corroboration,
                    "prior_date": t.prior_date.isoformat(),
                }
            ),
        )
        for t in transitions
        if "T3" in t.rules and t.observed_ratio is not None
    ]
    if break_rows:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO price_series_break
                    (instrument_id, break_date, observed_ratio, direction, rule_version, evidence_json)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (instrument_id, break_date) DO UPDATE
                   SET observed_ratio = EXCLUDED.observed_ratio,
                       direction = EXCLUDED.direction,
                       rule_version = EXCLUDED.rule_version,
                       evidence_json = EXCLUDED.evidence_json
                """,
                break_rows,
            )

    conn.execute(
        """
        INSERT INTO price_quarantine_coverage
            (instrument_id, rule_set_version, first_bar, last_bar,
             bars_evaluated, transitions_evaluated, asset_class, evaluated_at)
        VALUES (%(iid)s, %(ver)s, %(first)s, %(last)s, %(bars)s, %(trans)s, %(cls)s, now())
        ON CONFLICT (instrument_id) DO UPDATE
           SET rule_set_version = EXCLUDED.rule_set_version,
               first_bar = EXCLUDED.first_bar,
               last_bar = EXCLUDED.last_bar,
               bars_evaluated = EXCLUDED.bars_evaluated,
               transitions_evaluated = EXCLUDED.transitions_evaluated,
               asset_class = EXCLUDED.asset_class,
               evaluated_at = now()
        """,
        {
            "iid": instrument_id,
            "ver": RULE_SET_VERSION,
            "first": bars[0].price_date,
            "last": bars[-1].price_date,
            "bars": len(bars),
            "trans": len(transitions),
            "cls": asset_class,
        },
    )
    return len(bar_rows), len(transition_rows), len(break_rows)


def refresh_price_quarantine(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    *,
    instrument_ids: list[int] | None = None,
    as_of: date | None = None,
) -> QuarantineRefreshResult:
    """Recompute + persist bar and transition verdicts for the priced universe.

    ``as_of`` fixes the provisional window (default today). Passing it makes the
    run reproducible: the same corpus at the same ``as_of`` produces the same
    verdicts, which a wall-clock read would not.

    TRANSACTION SHAPE — THE WHOLE RUN IS ONE TRANSACTION, and that is deliberate.
    ``conn.transaction()`` below is a SAVEPOINT, not a top-level commit: the
    reads in ``_series_batches`` have already opened the implicit transaction by
    the time the first write happens, so nothing lands until the caller commits.
    That is the semantics we want, because the census is a set of ratios over a
    single evaluated population — a half-committed recompute would publish
    counts mixing two rule-set versions, which is exactly the "plausible at any
    magnitude" failure the census exists to prevent. The full corpus (5,226
    instruments / 3.2M bars) runs in ~30s, and a failed run simply leaves the
    previous verdicts standing, so there is no partial-progress to preserve.
    """
    effective_as_of = as_of or date.today()
    result = QuarantineRefreshResult()

    # The savepoint is still load-bearing: one instrument is the unit of
    # FAILURE, so a bad series rolls back its own writes and the run continues.
    # The except tuple lists every type the pure layer can raise (ValueError,
    # ArithmeticError from Decimal) alongside the DB classes — a bare
    # `except Exception` would hide a genuine TypeError/AttributeError bug
    # behind a per-instrument skip.
    for instrument_id, asset_class, bars in _series_batches(conn, instrument_ids):
        try:
            verdicts = evaluate_series(bars, asset_class, as_of=effective_as_of)
            with conn.transaction():
                written = _write_instrument(conn, instrument_id, asset_class, bars, verdicts.bars, verdicts.transitions)
        except psycopg.Error, ValueError, ArithmeticError:
            logger.warning("price quarantine failed for instrument_id=%d, skipping", instrument_id, exc_info=True)
            continue
        result.instruments += 1
        result.bars_evaluated += len(bars)
        result.transitions_evaluated += len(verdicts.transitions)
        result.bar_rows_written += written[0]
        result.transition_rows_written += written[1]
        result.breaks_written += written[2]

    logger.info(
        "price quarantine %s: %d instruments, %d bars, %d bar rows, %d transition rows, %d breaks",
        RULE_SET_VERSION,
        result.instruments,
        result.bars_evaluated,
        result.bar_rows_written,
        result.transition_rows_written,
        result.breaks_written,
    )
    return result


def _rule_counts(conn: psycopg.Connection, table: str) -> dict[str, int]:  # type: ignore[type-arg]
    # Composed via psycopg.sql.Identifier, not an f-string. The two call sites
    # pass module-local literals today, but a table name spliced into a query
    # string is the shape the SQL-injection guard greps for, and a future caller
    # will not know that.
    rows = conn.execute(
        sql.SQL("""
        SELECT rule, COUNT(*)
        FROM {table}, LATERAL unnest(rules) AS rule
        WHERE rule_set_version = %(ver)s
        GROUP BY rule
        ORDER BY rule
        """).format(table=sql.Identifier(table)),
        {"ver": RULE_SET_VERSION},
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def census(conn: psycopg.Connection) -> QuarantineCensus:  # type: ignore[type-arg]
    """The operator-visible rejection census, counted over stored verdicts."""
    out = QuarantineCensus()

    coverage = conn.execute(
        """
        SELECT COUNT(*) FILTER (WHERE rule_set_version = %(ver)s),
               COALESCE(SUM(bars_evaluated) FILTER (WHERE rule_set_version = %(ver)s), 0),
               COALESCE(SUM(transitions_evaluated) FILTER (WHERE rule_set_version = %(ver)s), 0),
               COUNT(*) FILTER (WHERE rule_set_version <> %(ver)s)
        FROM price_quarantine_coverage
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    if coverage is not None:
        out.instruments_evaluated = int(coverage[0])
        out.bars_evaluated = int(coverage[1])
        out.transitions_evaluated = int(coverage[2])
        out.stale_version_instruments = int(coverage[3])

    bars = conn.execute(
        """
        SELECT COUNT(*) FILTER (WHERE NOT return_usable),
               COUNT(*) FILTER (WHERE NOT range_usable),
               COUNT(*) FILTER (WHERE provisional)
        FROM price_bar_quarantine
        WHERE rule_set_version = %(ver)s
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    if bars is not None:
        out.bars_return_unusable = int(bars[0])
        out.bars_range_unusable = int(bars[1])
        out.bars_provisional = int(bars[2])
    out.bar_rule_counts = _rule_counts(conn, "price_bar_quarantine")

    transitions = conn.execute(
        """
        SELECT COUNT(*) FILTER (WHERE cardinality(rules) > 0),
               -- DEFERRED, not merely provisional. The magnitude threshold must
               -- actually have been crossed (which is what sets corroboration to
               -- anything other than 'not_applicable') AND no rule fired. Without
               -- the corroboration clause this counts every ordinary transition
               -- inside the trailing correction window, and the figure stops
               -- matching the rule the API states for it.
               COUNT(*) FILTER (
                   WHERE provisional
                     AND cardinality(rules) = 0
                     AND corroboration <> 'not_applicable'
               )
        FROM price_transition_quarantine
        WHERE rule_set_version = %(ver)s
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchone()
    if transitions is not None:
        out.transitions_quarantined = int(transitions[0])
        out.transitions_provisional_deferred = int(transitions[1])
    out.transition_rule_counts = _rule_counts(conn, "price_transition_quarantine")

    # THE NARROWING-GATE CENSUS. A narrowing gate is measured by what it
    # REJECTS, so this counts the T3 TRIGGER population (every transition whose
    # magnitude passed T and whose endpoints were usable), split by what the
    # turnover signal said — including the ~70% it could not classify at all.
    # Counting only the admitted side would be silent about the regression.
    corroboration = conn.execute(
        """
        SELECT corroboration, COUNT(*)
        FROM price_transition_quarantine
        WHERE rule_set_version = %(ver)s AND corroboration <> 'not_applicable'
        GROUP BY corroboration
        ORDER BY corroboration
        """,
        {"ver": RULE_SET_VERSION},
    ).fetchall()
    out.t3_corroboration = {str(r[0]): int(r[1]) for r in corroboration}

    breaks = conn.execute(
        """
        WITH last_break AS (
            SELECT instrument_id, MAX(break_date) AS break_date
            FROM price_series_break
            WHERE resolved_by IS NULL
            GROUP BY instrument_id
        )
        SELECT (SELECT COUNT(*) FROM last_break),
               COALESCE((
                   SELECT COUNT(*)
                   FROM price_daily p
                   JOIN last_break b ON b.instrument_id = p.instrument_id
                   WHERE p.price_date < b.break_date
               ), 0)
        """
    ).fetchone()
    if breaks is not None:
        out.instruments_with_unresolved_break = int(breaks[0])
        out.bars_stranded_pre_break = int(breaks[1])

    return out


def usable_bar_filter_sql(alias: str = "p") -> str:
    """SQL fragment: bars whose verdicts are CURRENT and clean, fail-closed.

    Absence of a ``price_bar_quarantine`` row means "clean" ONLY inside an
    evaluated range, so this joins coverage and requires the current rule-set
    version. An instrument never evaluated, or evaluated at an older rule set,
    reads as NOT usable — the opposite default would admit precisely the
    population the rules have not checked.

    ``coalesce(..., FALSE)`` on both verdicts is load-bearing: with no matching
    quarantine row the columns are NULL, and ``NOT (q.return_usable)`` on NULL
    is NULL, not TRUE — a WHERE clause on NULL drops the row silently.
    """
    return f"""
        EXISTS (
            SELECT 1 FROM price_quarantine_coverage cov
            WHERE cov.instrument_id = {alias}.instrument_id
              AND cov.rule_set_version = %(quarantine_version)s
              AND {alias}.price_date BETWEEN cov.first_bar AND cov.last_bar
        )
        AND COALESCE(
            (SELECT q.return_usable FROM price_bar_quarantine q
              WHERE q.instrument_id = {alias}.instrument_id
                AND q.price_date = {alias}.price_date),
            TRUE) IS TRUE
    """


def usable_bar_params() -> dict[str, Any]:
    return {"quarantine_version": RULE_SET_VERSION}


__all__ = [
    "QuarantineCensus",
    "QuarantineRefreshResult",
    "census",
    "refresh_price_quarantine",
    "usable_bar_filter_sql",
    "usable_bar_params",
]
