"""Phase 4b — the outcome-ledger writer.

Spec: ``docs/proposals/ta/2026-08-06-outcome-ledger.md``. Resolver:
``app/services/outcome_resolver.py`` (4a) — this module stores what that one
returns and re-derives nothing. Table: ``sql/256_strategy_outcomes.sql``.
Sibling: ``app/services/signal_ledger.py`` (3c), whose shape this mirrors.
Refs #2240, #2245, #2288.

⚠⚠ THE KEY HAS THREE PARTS, AND THE THIRD IS THE ONE THAT IS EASY TO MISS.

``(signal_id, rule_set_version, input_rule_set_version)``. The first two are
4a §6's: the resolver's source hash is not inside ``strategy_version``, so a
changed execution assumption must produce a SECOND outcome beside the first
rather than overwrite it — the same argument phase 3b made for the ledger.

The third is this module's. **The resolver is not the only thing that decides an
outcome; its INPUTS do too.** ``load_masked_series`` masks high/low/close per
``price_quarantine``'s rule set — and, since #2354, a non-positive open — and
4a's masking contract is that an absent field REFUSES. Re-run the quarantine under a changed rule set and the same
signal can resolve differently with ``outcome_resolver.py`` byte-identical.
Without the column that is not merely unrecorded, it is UNSTORABLE: the
corrected outcome collides on a two-part key, and with no ``ON CONFLICT`` the
only way to write it down would be to touch the resolver's source so its hash
moves.

⚠ EVERY AGGREGATE OVER THIS TABLE MUST PIN ONE ``(rule_set_version,
input_rule_set_version)`` PAIR. Two versions coexist by design, so an unpinned
``count(*) FILTER (WHERE outcome = 'tp_hit')`` counts one trade twice.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from app.services.indicator_series import BarSeries, Universe
from app.services.outcome_resolver import (
    OUTCOME_CLASSES,
    RESOLUTION_METHODS,
    UNRESOLVED_REASONS,
    Outcome,
    OutcomeClass,
    ResolutionMethod,
    UnresolvedReason,
)

#: Outcomes that book a trade, and therefore carry a price and a return. ⚠
#: DERIVED from the resolver's own set rather than restated — a vocabulary
#: written down twice is validated in neither (#2218). The SQL CHECK is the
#: third copy and is pinned to these constants by test.
_BOOKED: frozenset[str] = frozenset({"tp_hit", "sl_hit", "expired"})


@dataclass(frozen=True)
class OutcomeRow:
    """One resolved outcome, ready to store.

    ⚠ ``exit_index`` is NOT here. 4a §3.7: *"an index is not durable across a
    corpus rebuild"* — the date is. Storing both would let them disagree after a
    re-adjustment, and the index is the one that would be wrong.

    ⚠ ``rule_set_version`` and ``resolution_method`` come from the ``Outcome``
    and are never re-stamped by the writer. A writer that stamped its own
    version could store an outcome under a version that did not produce it,
    which is precisely what the key exists to prevent.

    The validation below MIRRORS ``sql/256``'s CHECK constraints — deliberate
    duplication, not redundancy: a bad row fails at construction with a message
    naming the field, while the constraints stay as the backstop for any writer
    that bypasses this class.
    """

    signal_id: int
    rule_set_version: str
    input_rule_set_version: str
    outcome: OutcomeClass
    resolution_method: ResolutionMethod
    reason: UnresolvedReason | None = None
    exit_bar_date: date | None = None
    exit_price: Decimal | None = None
    bars_held: int | None = None
    gross_return_pct: Decimal | None = None

    def __post_init__(self) -> None:
        if self.outcome not in OUTCOME_CLASSES:
            raise ValueError(f"unknown outcome {self.outcome!r}; must be one of {sorted(OUTCOME_CLASSES)}")
        if self.resolution_method not in RESOLUTION_METHODS:
            raise ValueError(
                f"unknown resolution_method {self.resolution_method!r}; must be one of {sorted(RESOLUTION_METHODS)}"
            )
        if self.reason is not None and self.reason not in UNRESOLVED_REASONS:
            raise ValueError(f"unknown reason {self.reason!r}; must be one of {sorted(UNRESOLVED_REASONS)}")
        # strategy_outcomes_versions_non_empty. ⚠ A blank version is PRESENT and
        # meaningless — the #2286 shape, where an empty `EBULL_SERVICE_TOKEN=`
        # won an alias race against a real credential. NOT NULL does not catch
        # it and neither does a type checker.
        if not self.rule_set_version or not self.input_rule_set_version:
            raise ValueError(
                f"blank version on signal {self.signal_id}: "
                f"{(self.rule_set_version, self.input_rule_set_version)!r} — both key members must be stated"
            )
        # strategy_outcomes_reason_matches_outcome
        if (self.outcome == "unresolved") != (self.reason is not None):
            raise ValueError(
                f"outcome {self.outcome!r} and reason {self.reason!r} disagree: "
                "a reason is required exactly when the outcome is unresolved"
            )
        # strategy_outcomes_location_matches_outcome.
        #
        # ⚠ COUNTED, not ANDed — the 3c mirror defect (prevention log, #2240
        # 3c). `a is not None and b is not None` reads as "has a location" and
        # silently admits HALF one: an unresolved row carrying a bars_held with
        # no date scores False on that expression, matches `outcome ==
        # "unresolved"`, and passes — while the SQL CHECK compares the two
        # nullities separately and rejects it. The whole value of mirroring a
        # constraint is that the two agree, so the mirror has to be exact.
        located = (self.exit_bar_date is not None) + (self.bars_held is not None)
        if located != (0 if self.outcome == "unresolved" else 2):
            raise ValueError(
                f"outcome {self.outcome!r} carries a partial exit location "
                f"{(self.exit_bar_date, self.bars_held)!r}: an unresolved outcome has neither and every "
                "other outcome — including ambiguous, whose bar is known — has both"
            )
        # strategy_outcomes_booked_matches_outcome
        booked = (self.exit_price is not None) + (self.gross_return_pct is not None)
        if booked != (2 if self.outcome in _BOOKED else 0):
            raise ValueError(
                f"outcome {self.outcome!r} carries {(self.exit_price, self.gross_return_pct)!r}: "
                "a price and a return exist exactly for tp_hit / sl_hit / expired, and move together"
            )
        # strategy_outcomes_bars_held_non_negative. ⚠ 0 is LEGAL — a level
        # touched on the fill bar.
        if self.bars_held is not None and self.bars_held < 0:
            raise ValueError(f"bars_held must be non-negative, got {self.bars_held} — the exit precedes the fill")

    @classmethod
    def from_outcome(cls, signal_id: int, outcome: Outcome, *, input_rule_set_version: str) -> OutcomeRow:
        """Project an ``Outcome`` onto the stored fields.

        ⚠ ``input_rule_set_version`` is a REQUIRED keyword with no default. The
        resolver never sees it — it reads bars, not the pipeline that produced
        them — so only the caller can state it, and #2288's lesson is that a
        field with a default is a field a writer can forget. A caller reading
        bars from an unversioned path must say so explicitly.
        """
        return cls(
            signal_id=signal_id,
            rule_set_version=outcome.rule_set_version,
            input_rule_set_version=input_rule_set_version,
            outcome=outcome.outcome,
            resolution_method=outcome.resolution_method,
            reason=outcome.reason,
            exit_bar_date=outcome.exit_bar_date,
            exit_price=outcome.exit_price,
            bars_held=outcome.bars_held,
            gross_return_pct=outcome.gross_return_pct,
        )


def locate_fill_index(series: BarSeries, fill_bar_date: date) -> int:
    """Find the index of a STORED fill date in ``series``.

    ⚠ A guard, not a convenience. ``strategy_signals.fill_bar_date`` is durable;
    a bar index is not. A fill date no longer in the corpus means the corpus was
    rebuilt, re-adjusted or re-segmented under a recorded decision — and the
    resolver's own entry-price check (4a §3.7) catches only the other half, where
    the date still exists but the open moved. Silently re-reading "whatever bar
    sits at that position now" is how a ledger stops being a record of what was
    actually decided.

    ⚠ Duplicate dates need no handling: ``BarSeries.__post_init__`` rejects
    duplicate and non-ascending dates at construction, so the lookup is
    unambiguous by the input TYPE rather than by a rule restated here.
    """
    try:
        return series.dates.index(fill_bar_date)
    except ValueError:
        raise ValueError(
            f"fill_bar_date {fill_bar_date} is not in the {len(series)}-bar series "
            f"({series.dates[0] if series.dates else 'empty'} … "
            f"{series.dates[-1] if series.dates else 'empty'}) — the stored fill and the series "
            "must come from the same corpus"
        ) from None


@dataclass(frozen=True)
class PendingFill:
    """A stored fill with no outcome yet at the requested version pair."""

    signal_id: int
    instrument_id: int
    signal_bar_date: date
    fill_bar_date: date
    fill_price: Decimal
    #: ⚠ Carried even though it is a join away. #2288's labelling contract: a
    #: metric computed on a survivor-only universe must be marked as such, and a
    #: label the consumer has to fetch separately is a label it will omit.
    universe: Universe


_PENDING = """
    SELECT s.signal_id, s.instrument_id, s.signal_bar_date,
           s.fill_bar_date, s.fill_price, s.universe
    FROM strategy_signals s
    LEFT JOIN strategy_outcomes o
           ON o.signal_id = s.signal_id
          AND o.rule_set_version = %(rule_set_version)s
          AND o.input_rule_set_version = %(input_rule_set_version)s
    WHERE s.strategy_id = %(strategy_id)s
      AND s.strategy_version = %(strategy_version)s
      AND s.signal_kind = 'entry'
      AND s.verdict = 'fired'
      AND o.outcome_id IS NULL
      AND s.signal_id > %(after_signal_id)s
      AND (%(at_or_before_signal_id)s::bigint IS NULL
           OR s.signal_id <= %(at_or_before_signal_id)s::bigint)
    ORDER BY s.signal_id
    LIMIT %(limit)s
"""


def select_pending_fills(
    conn: psycopg.Connection[tuple],
    *,
    strategy_id: str,
    strategy_version: str,
    rule_set_version: str,
    input_rule_set_version: str,
    limit: int | None = None,
    after_signal_id: int = 0,
    at_or_before_signal_id: int | None = None,
) -> list[PendingFill]:
    """Every fired entry for one strategy version still unresolved at this version pair.

    ⚠⚠ **Both version predicates are in the JOIN, not the WHERE.** Moved to the
    ``WHERE`` they turn the outer join into an inner one and the query returns
    NOTHING — and "nothing" is indistinguishable from "nothing to do", so the
    backfill reports success having done zero work. Pinned by test.

    ⚠ Scoped to one strategy version, and required to be: no defaults, no "all
    strategies" mode. A backfill that fans out across every strategy in the
    table is a backfill nobody can size before starting it.

    ⚠ This is a RE-RESOLUTION path, not only a first-run path. Bump the resolver
    — or the quarantine rule set — and every fill becomes pending again under
    the new pair, with the old outcomes intact beside it. That is the same shape
    as bumping ``strategy_version`` in the ledger, and it is why both versions
    are in the key.

    ⚠ ``signal_kind = 'entry'`` AND ``verdict = 'fired'`` are here as well as in
    the writer's INSERT. Not redundant: this one decides what phase 5 spends
    time resolving, that one decides what may be stored. A caller that built its
    own list still cannot write an outcome for an exit.
    """
    if limit is not None and limit < 1:
        raise ValueError(f"limit must be positive or None, got {limit}")
    if after_signal_id < 0:
        raise ValueError(f"after_signal_id must be non-negative, got {after_signal_id}")
    if at_or_before_signal_id is not None and at_or_before_signal_id <= after_signal_id:
        raise ValueError(
            "at_or_before_signal_id must be greater than after_signal_id when supplied, "
            f"got {at_or_before_signal_id} <= {after_signal_id}"
        )
    with conn.cursor() as cur:
        cur.execute(
            _PENDING,
            {
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "rule_set_version": rule_set_version,
                "input_rule_set_version": input_rule_set_version,
                "limit": limit,
                "after_signal_id": after_signal_id,
                "at_or_before_signal_id": at_or_before_signal_id,
            },
        )
        rows = cur.fetchall()
    return [
        PendingFill(
            signal_id=int(row[0]),
            instrument_id=int(row[1]),
            signal_bar_date=row[2],
            fill_bar_date=row[3],
            fill_price=row[4],
            universe=row[5],
        )
        for row in rows
    ]


# ⚠⚠ AN INSERT … SELECT FROM THE PARENT, NOT `INSERT … VALUES`.
#
# The FK proves the signal EXISTS. It cannot prove the signal was a FIRED ENTRY,
# and 4a resolves nothing else: a `signal_kind = 'exit'` row is not an input
# (4a §1) and a not_fired / not_evaluable row has no fill. A CHECK cannot read
# the parent row, so the predicate lives in the writing statement — which also
# makes it race-free, since it is evaluated inside the INSERT rather than by a
# read that precedes it.
#
# The same WHERE closes the second cross-table invariant for free: an exit
# cannot precede its fill. `>=`, not `>`, because bars_held = 0 is legal — a
# level touched on the fill bar itself.
#
# ⚠ A non-qualifying parent inserts ZERO ROWS, which is SILENT. `store_outcomes`
# turns that into a raise; see its docstring.
#
# ⚠ Every parameter in the SELECT list carries an explicit cast. An untyped NULL
# parameter there is psycopg3's `AmbiguousParameter` — the prevention-log entry
# from #1961, which is about a nullable filter and applies verbatim to a
# nullable projected value.
_INSERT = """
    INSERT INTO strategy_outcomes (
        signal_id, rule_set_version, input_rule_set_version, outcome,
        resolution_method, reason, exit_bar_date, exit_price, bars_held,
        gross_return_pct
    )
    SELECT s.signal_id,
           %(rule_set_version)s::text,
           %(input_rule_set_version)s::text,
           %(outcome)s::text,
           %(resolution_method)s::text,
           %(reason)s::text,
           %(exit_bar_date)s::date,
           %(exit_price)s::numeric,
           %(bars_held)s::integer,
           %(gross_return_pct)s::numeric
    FROM strategy_signals s
    WHERE s.signal_id = %(signal_id)s
      AND s.signal_kind = 'entry'
      AND s.verdict = 'fired'
      AND (%(exit_bar_date)s::date IS NULL OR %(exit_bar_date)s::date >= s.fill_bar_date)
"""

# The diagnostic for a shortfall. Returns the parent's actual shape so the
# message says WHICH rule rejected it, rather than "some row did not insert".
_DIAGNOSE = """
    SELECT s.signal_id, s.signal_kind, s.verdict, s.fill_bar_date
    FROM strategy_signals s
    WHERE s.signal_id = ANY(%(signal_ids)s)
"""

#: How many offending signals a shortfall message names before truncating. The
#: message says how many were omitted; it never trims silently.
_DIAGNOSTIC_LIMIT = 5


def _shortfall_detail(conn: psycopg.Connection[tuple], rows: Sequence[OutcomeRow]) -> str:
    """Name the rows whose parent failed the INSERT's predicate."""
    ids = [row.signal_id for row in rows]
    with conn.cursor() as cur:
        cur.execute(_DIAGNOSE, {"signal_ids": ids})
        parents = {int(r[0]): (r[1], r[2], r[3]) for r in cur.fetchall()}

    by_id = {row.signal_id: row for row in rows}
    offenders: list[str] = []
    for signal_id in ids:
        parent = parents.get(signal_id)
        if parent is None:
            offenders.append(f"signal {signal_id}: no such signal")
            continue
        kind, verdict, fill_bar_date = parent
        row = by_id[signal_id]
        if kind != "entry" or verdict != "fired":
            offenders.append(f"signal {signal_id}: parent is {verdict}/{kind}, not fired/entry")
        elif row.exit_bar_date is not None and fill_bar_date is not None and row.exit_bar_date < fill_bar_date:
            offenders.append(f"signal {signal_id}: exit {row.exit_bar_date} precedes fill {fill_bar_date}")

    if not offenders:
        # Every parent looks fine now — so the rowcount is telling us something
        # else, and saying "no offenders" is more honest than inventing one.
        return "no offending parent found on re-read; the batch may have raced a concurrent delete"
    shown = offenders[:_DIAGNOSTIC_LIMIT]
    if len(offenders) > _DIAGNOSTIC_LIMIT:
        shown.append(f"… and {len(offenders) - _DIAGNOSTIC_LIMIT} more")
    return "; ".join(shown)


def store_outcomes(conn: psycopg.Connection[tuple], rows: Sequence[OutcomeRow]) -> int:
    """Insert ``rows``, returning the number written.

    ⚠⚠ **A SHORTFALL RAISES.** The INSERT selects from the parent, so a signal
    that is not a fired entry — or an exit dated before its fill — contributes
    ZERO rows instead of a bad one. That is the guarantee, and it is silent: a
    writer that returned 998 for a 1,000-row batch would have refused two
    outcomes and reported success. The count is compared and the offenders are
    named.

    ⚠⚠ **NO** ``ON CONFLICT``**, deliberately** — the argument is
    ``store_signals``', unchanged:

    - ``DO UPDATE`` would let a re-run overwrite a recorded classification,
      which is exactly what the version members of the key exist to prevent.
    - ``DO NOTHING`` would silently keep the old row when the new one
      DISAGREES. Given a fixed ``(rule_set_version, input_rule_set_version)``,
      an outcome is a pure function of the bars — so a disagreement means the
      corpus moved under us, the one case worth hearing about and the one
      ``DO NOTHING`` hides.

    A deliberate re-resolution changes the resolver or the quarantine rule set,
    which changes a key member and inserts cleanly. That is the intended path.

    This function does NOT own the transaction: on a raise the caller rolls
    back, exactly as with ``store_signals``.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            _INSERT,
            [
                {
                    "signal_id": row.signal_id,
                    "rule_set_version": row.rule_set_version,
                    "input_rule_set_version": row.input_rule_set_version,
                    "outcome": row.outcome,
                    "resolution_method": row.resolution_method,
                    "reason": row.reason,
                    "exit_bar_date": row.exit_bar_date,
                    "exit_price": row.exit_price,
                    "bars_held": row.bars_held,
                    "gross_return_pct": row.gross_return_pct,
                }
                for row in rows
            ],
        )
        # psycopg3 executemany rowcount is cumulative across the batch. ⚠ -1 is
        # psycopg's "server reported nothing" sentinel and must not be returned
        # as a count (prevention log: "psycopg v3 rowcount sentinel (-1)
        # treated as valid count").
        written = cur.rowcount
        if written < 0:
            raise RuntimeError(f"strategy_outcomes INSERT reported rowcount {written} for {len(rows)} rows")
        if written != len(rows):
            detail = _shortfall_detail(conn, rows)
            raise ValueError(
                f"strategy_outcomes INSERT wrote {written} of {len(rows)} rows — an outcome may only be "
                f"stored against a FIRED ENTRY whose fill precedes the exit: {detail}"
            )
    return written
