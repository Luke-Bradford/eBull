"""Phase 4a — acceptance harness for `outcome_resolver`.

Run from repo root:

    uv run python -m scripts.verify_2240_outcome_resolver --distribution
    uv run python -m scripts.verify_2240_outcome_resolver --equivalence

⚠ **Do NOT pipe this into `head`/`tail`.** A pipe buffers, so the flushed
progress lines go nowhere and the output file sits empty while the run is
perfectly healthy — that cost 7 minutes on 2026-08-05. Redirect to a file and
read the file. Same rule as `.claude/CLAUDE.md`'s "never pipe a gate command".

WHY THIS IS A COMMITTED SCRIPT AND NOT A NUMBER IN A PR
-------------------------------------------------------
Acceptance 12-14 of `docs/proposals/ta/2026-08-06-outcome-resolver.md` are
full-corpus figures. A figure written by hand into prose goes stale silently
the moment the derivation changes, and it goes stale in the place a reader
trusts most — so the repo's rule is to compute it or omit it. This computes it.

Sister to `scripts/verify_2240_indicator_series.py` and
`scripts/verify_2279_price_structure.py`.

WHAT EACH ARM PROVES, AND WHAT IT DOES NOT
------------------------------------------
`--distribution` runs the resolver over every bar of the corpus as a
hypothetical entry, across a grid of take-profit multiple × max-hold, and
reports the outcome census plus the `unresolved` census broken out by reason.
That is criterion 9's "measure what you reject" applied to this module's own
refusals. It is a MEASUREMENT, not a proof.

`--equivalence` is the proof. For one grid cell it recomputes, in SQL and
independently of the walk, the **first bar in the window that touches the stop**
and the **first that touches the target** — a set-based `min(rn) FILTER (…)`
rather than an ordered scan. Finding those two indices in the right order is
precisely what the walk does, so a walk bug (off-by-one window, mis-ordered
rule table, a refusal that fires late) surfaces as a disagreeing index.

⚠ It does NOT independently check the masking, the coverage gate or the segment
model: both arms read the same quarantine verdicts, and the segment model has no
research-corpus break table to read. Those are covered by
`tests/test_outcome_resolver.py` acceptance 6-8, not here. Saying which is the
point — an equivalence run quietly assumed to prove everything is worse than no
equivalence run.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import date
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.entry_timing import _compute_stop_loss
from app.services.indicator_series import BarSeries, atr_series
from app.services.outcome_resolver import (
    RULE_SET_VERSION,
    ExitLevels,
    resolve_outcome,
)
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.research_price_structure_store import load_masked_series
from app.services.technical_analysis import OHLCVRow

#: The corpus is survivor-only — #2284 measured 0 of 259 known delisted names
#: served. Hard-coded because this harness only ever reads that corpus.
UNIVERSE = "survivor_only"

#: S5's own sweep (#2245), so the distribution here is directly comparable to
#: the surface phase 3/4 were told to read their numbers off. ⚠ The SL rule is
#: held FIXED at `entry_timing._compute_stop_loss` — the repo's own, and what S5
#: swept against — so the grid varies what this phase actually parameterises.
TP_MULTIPLES = (Decimal("1.0"), Decimal("1.5"), Decimal("2.0"), Decimal("3.0"), Decimal("4.0"))
MAX_HOLDS = (10, 20)

#: The cell the equivalence arm proves. 20 bars is S5's max hold; 2.0×ATR sits
#: in the middle of its sweep.
EQUIVALENCE_TP = Decimal("2.0")
EQUIVALENCE_HOLD = 20

#: Series per equivalence chunk. Bounded so the join never materialises the
#: whole 25.8M-bar × 20-bar product — the run stays streaming and its progress
#: stays visible, which a single corpus-wide query gives up.
_CHUNK = 20

#: Skips that depend only on the bar and its ATR, so one grid cell's count
#: stands for every cell. ASSERTED below, never assumed.
_GRID_INDEPENDENT_SKIPS = ("no_atr", "atr_not_positive", "entry_not_positive", "no_fill_bar")

#: ⚠ Skips that DO vary by grid cell, and must therefore be printed per cell.
#: There is exactly one, and it was found by the assertion below firing — see
#: `_levels`.
_GRID_DEPENDENT_SKIPS = ("levels_do_not_bracket",)


class Series:
    """One research series, masked, with everything the arms need."""

    __slots__ = ("series_id", "bars", "atr", "unusable", "range_masked")

    def __init__(self, series_id: int, bars: BarSeries, atr: list[float | None], unusable: dict[int, str]) -> None:
        self.series_id = series_id
        self.bars = bars
        self.atr = atr
        self.unusable = unusable
        self.range_masked = len(unusable)


def _load_quarantined(conn: psycopg.Connection[tuple]) -> dict[int, set[date]]:
    """Range-unusable bar dates per series, at the CURRENT quarantine version.

    Read directly rather than inferred from the masked loader's `None`s: a
    `None` could equally be a genuine NULL column, and criterion 8 exists to
    stop a data gap and a quarantine verdict collapsing into one another. The
    resolver's `missing_bar_data` is the fallback for the former; this map
    supplies the latter by name.
    """
    rows = conn.execute(
        "SELECT series_id, bar_date FROM research_bar_quarantine WHERE rule_set_version = %s AND NOT range_usable",
        (QUARANTINE_RULE_SET_VERSION,),
    ).fetchall()
    out: dict[int, set[date]] = {}
    for series_id, bar_date in rows:
        out.setdefault(series_id, set()).add(bar_date)
    return out


def _load(conn: psycopg.Connection[tuple], series_id: int, quarantined: set[date]) -> Series:
    masked = load_masked_series(conn, series_id)
    rows: list[OHLCVRow] = [
        {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}  # type: ignore[typeddict-item]
        for b in masked.bars
    ]
    bars = BarSeries(dates=tuple(b.bar_date for b in masked.bars), rows=tuple(rows))
    atr = list(atr_series(bars, universe=UNIVERSE).values) if len(bars) else []
    unusable = {i: "quarantined_bar" for i, d in enumerate(bars.dates) if d in quarantined}
    return Series(series_id, bars, atr, unusable)


def _levels(entry: Decimal, atr: float | None, tp_mult: Decimal, hold: int) -> tuple[ExitLevels | None, str | None]:
    """The bracket for one hypothetical entry, or why there isn't one.

    ⚠ `_compute_stop_loss` is imported from `entry_timing` rather than restated.
    It is private, and importing a private from a sibling is the lesser evil: a
    second copy of the ATR multiplier, the 5% floor and the 2% minimum distance
    is a second thing to keep in step, and S5 swept against THAT function.
    """
    if atr is None:
        return None, "no_atr"
    if atr <= 0:
        return None, "atr_not_positive"
    if entry <= 0:
        return None, "entry_not_positive"
    atr_dec = Decimal(str(atr))
    stop = _compute_stop_loss(entry, atr_dec)
    target = entry + tp_mult * atr_dec
    if not stop < entry < target:
        # ⚠ This branch was written as "unreachable — a positive ATR puts the
        # target above the entry" and labelled `atr_not_positive`. Both were
        # wrong. The cross-cell assertion below is what found it: the count
        # differed between grid cells, which a bar-and-ATR-only skip cannot.
        #
        # ROOT CAUSE — a FLAT price run, not a precision curiosity. On a run of
        # bars with open = high = low = close the true range is 0 every bar, and
        # Wilder smoothing (`atr = (13*atr_prev + tr)/14`) decays the ATR
        # geometrically toward zero WITHOUT EVER REACHING IT in float. So
        # `atr > 0` stays true while the ATR is economically zero. Worked
        # example, series 2016 around 2018-07-20 — 21 consecutive bars holding
        # ONE distinct OHLC tuple, o=h=l=c=1651206.625:
        #     SELECT count(*), count(DISTINCT (open, high, low, close))
        #     FROM (SELECT open, high, low, close,
        #                  row_number() OVER (ORDER BY bar_date) rn
        #           FROM research_price_daily WHERE series_id = 2016) t
        #     WHERE rn BETWEEN 3480 AND 3500;
        #
        # PROXIMATE CAUSE — `Decimal` is arbitrary-PRECISION, not infinite:
        # the default context is 28 significant digits, so once the ATR is that
        # small relative to the price, `entry + tp_mult * atr` ROUNDS BACK TO
        # `entry`. The rounding threshold scales with `tp_mult`, which is why
        # the count is GRID-DEPENDENT and the signal totals rise monotonically
        # across the grid. ⚠ The counts are NOT written here — this arm computes
        # and prints them per cell under `levels_do_not_bracket`, so a
        # re-harvest cannot leave a hand-copied figure lying:
        #     uv run python -m scripts.verify_2240_outcome_resolver --distribution
        #
        # ⚠ Consequence for phase 5, recorded because it is not obvious:
        # `atr > 0` is NOT a sufficient "this instrument has volatility" gate.
        # Skipping here is correct — a target equal to the entry is a zero-width
        # bracket and not a trade, and the resolver rejects it by its own
        # precondition — but only the NAME and the countability were wrong, and
        # a strategy filter that trusts `atr > 0` will admit these flat runs.
        return None, "levels_do_not_bracket"
    return ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=hold), None


def _series_ids(conn: psycopg.Connection[tuple]) -> list[int]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT series_id FROM research_price_series WHERE bar_count IS NOT NULL ORDER BY series_id"
        ).fetchall()
    ]


def distribution(conn: psycopg.Connection[tuple]) -> int:
    """Acceptance 13 + 14 — full corpus, every bar as a hypothetical entry."""
    ids = _series_ids(conn)
    quarantined = _load_quarantined(conn)
    cells: dict[tuple[Decimal, int], Counter[str]] = {
        (tp, hold): Counter() for tp in TP_MULTIPLES for hold in MAX_HOLDS
    }
    reasons: dict[tuple[Decimal, int], Counter[str]] = {k: Counter() for k in cells}
    bars_seen = 0
    ohlc_inconsistent = 0
    range_masked = 0
    started = time.perf_counter()

    for k, series_id in enumerate(ids):
        series = _load(conn, series_id, quarantined.get(series_id, set()))
        n = len(series.bars)
        bars_seen += n
        range_masked += series.range_masked

        # Acceptance 14 — MEASURED, never repaired. `low <= open <= high` is
        # definitional, and a bar violating it makes the rule table nonsense —
        # but bar validity belongs to `price_quarantine`, not here.
        for row in series.bars.rows:
            o, h, lo = row.get("open"), row.get("high"), row.get("low")
            if o is None or h is None or lo is None:
                continue
            if not lo <= o <= h or lo > h:
                ohlc_inconsistent += 1

        for i in range(n):
            fill_index = i + 1
            if fill_index >= n:
                for cell in cells.values():
                    cell["no_fill_bar"] += 1
                continue
            entry = series.bars.rows[fill_index].get("open")
            for (tp, hold), cell in cells.items():
                if entry is None:
                    cell["no_fill_bar"] += 1
                    continue
                levels, skip = _levels(entry, series.atr[i], tp, hold)
                if levels is None:
                    assert skip is not None
                    cell[skip] += 1
                    continue
                out = resolve_outcome(
                    series=series.bars,
                    fill_index=fill_index,
                    entry_price=entry,
                    levels=levels,
                    masked_bar_reasons=series.unusable,  # type: ignore[arg-type]
                    segment_end_index=None,
                )
                cell[out.outcome] += 1
                if out.reason is not None:
                    reasons[(tp, hold)][out.reason] += 1

        if (k + 1) % 500 == 0:
            print(f"  {k + 1:,}/{len(ids):,} series | bars={bars_seen:,}", flush=True)

    print(f"\n=== acceptance 13 — outcome distribution ({RULE_SET_VERSION}) ===", flush=True)
    print(f"  corpus         : research_price_daily, {bars_seen:,} bars over {len(ids):,} series", flush=True)
    print(f"  universe       : {UNIVERSE} (#2284: 0 of 259 known delisted names served)", flush=True)
    print(f"  quarantine     : {QUARANTINE_RULE_SET_VERSION}, {range_masked:,} range-masked bars", flush=True)
    print("  SL rule        : entry_timing._compute_stop_loss (fixed)\n", flush=True)
    header = f"  {'TP×ATR':>7} {'hold':>5} {'signals':>12} " + " ".join(
        f"{c:>10}" for c in ("tp_hit", "sl_hit", "expired", "ambiguous", "unresolved")
    )
    print(header, flush=True)
    for (tp, hold), cell in cells.items():
        resolved = sum(cell[c] for c in ("tp_hit", "sl_hit", "expired", "ambiguous", "unresolved"))
        pct = " ".join(
            f"{cell[c] / resolved:>9.4%}" if resolved else f"{'-':>10}"
            for c in ("tp_hit", "sl_hit", "expired", "ambiguous", "unresolved")
        )
        print(f"  {tp:>7} {hold:>5} {resolved:>12,} {pct}", flush=True)

    print("\n  --- criterion 9: what was REJECTED, by reason ---", flush=True)
    print(
        f"  {'TP×ATR':>7} {'hold':>5} "
        + " ".join(f"{r:>17}" for r in ("window_truncated", "series_break", "quarantined_bar", "missing_bar_data")),
        flush=True,
    )
    for key, counter in reasons.items():
        tp, hold = key
        print(
            f"  {tp:>7} {hold:>5} "
            + " ".join(
                f"{counter[r]:>17,}"
                for r in ("window_truncated", "series_break", "quarantined_bar", "missing_bar_data")
            ),
            flush=True,
        )

    print("\n  --- signals with no bracket at all (excluded above) ---", flush=True)
    # ⚠ ASSERTED, never assumed. These skips depend only on the bar and its ATR,
    # so one cell's count stands for every cell — but if that stops holding,
    # printing one cell silently under-reports the exclusions criterion 9
    # requires be visible. The assertion has already earned its keep once: it
    # caught `levels_do_not_bracket` (then mislabelled `atr_not_positive`)
    # varying across the grid, which is how the Decimal-precision rounding in
    # `_levels` was found at all.
    first = cells[(TP_MULTIPLES[0], MAX_HOLDS[0])]
    for label in _GRID_INDEPENDENT_SKIPS:
        if any(cell[label] != first[label] for cell in cells.values()):
            raise AssertionError(f"skip reason {label!r} differs across grid cells; it must not")
    for label in _GRID_INDEPENDENT_SKIPS:
        print(f"  {label:<24} {first[label]:>14,}", flush=True)

    # ⚠ Grid-DEPENDENT, so printed PER CELL. Collapsing these into one number is
    # the exact under-reporting the assertion above exists to prevent.
    for label in _GRID_DEPENDENT_SKIPS:
        print(f"\n  --- {label} (varies by cell — see `_levels`) ---", flush=True)
        print(f"  {'TP×ATR':>7} {'hold':>5} {'signals':>14}", flush=True)
        for (tp, hold), cell in cells.items():
            print(f"  {tp:>7} {hold:>5} {cell[label]:>14,}", flush=True)

    print(f"\n  acceptance 14 — bars violating low <= open <= high : {ohlc_inconsistent:,}", flush=True)
    print("    ⚠ measured, NOT repaired — bar validity is price_quarantine's, not this module's", flush=True)
    print(f"\n  elapsed: {time.perf_counter() - started:.1f}s", flush=True)
    return 0


# ⚠ The equivalence arm's own view of the corpus. It replicates the fail-closed
# coverage join from `research_price_structure_store._LOAD_SQL` — a coverage row
# at the CURRENT rule-set version, and the bar inside [first_bar, last_bar] —
# because `row_number()` must produce the SAME index the Python side sees. A
# LEFT JOIN + COALESCE handles the sparse verdict table; absence of a verdict row
# means clean, but ONLY inside an evaluated range.
#
# ⚠ MATERIALISED INTO AN INDEXED TEMP TABLE, not left as a CTE, and that is a
# 10x wall-clock difference measured on this corpus. `row_number()` is computed
# in a subquery, so a CTE form gives the planner nothing to index and the
# `rn BETWEEN rn AND rn + hold` join degrades to a per-series nested loop —
# 3,355 x 3,355 comparisons for an average series instead of 3,355 x 21. The
# first attempt ran 500 of 7,693 series in 7m19s (~112 min projected); with the
# index it is a fraction of that. The temp table is session-scoped, so it cleans
# itself up.
_CREATE_IDX = """
CREATE TEMP TABLE equiv_idx (
    series_id BIGINT NOT NULL,
    rn        INT    NOT NULL,
    open      NUMERIC,
    high      NUMERIC,
    low       NUMERIC
)
"""

_INDEX_IDX = "CREATE INDEX equiv_idx_series_rn ON equiv_idx (series_id, rn)"

_FILL_IDX = """
INSERT INTO equiv_idx (series_id, rn, open, high, low)
SELECT d.series_id,
       row_number() OVER (PARTITION BY d.series_id ORDER BY d.bar_date),
       d.open,
       CASE WHEN COALESCE(q.range_usable, TRUE) THEN d.high END,
       CASE WHEN COALESCE(q.range_usable, TRUE) THEN d.low  END
FROM research_price_daily d
JOIN research_price_quarantine_coverage cov
  ON cov.series_id = d.series_id
 AND cov.rule_set_version = %(qv)s
 AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
LEFT JOIN research_bar_quarantine q
  ON q.series_id = d.series_id
 AND q.bar_date = d.bar_date
 AND q.rule_set_version = %(qv)s
WHERE d.series_id = ANY(%(chunk)s::bigint[])
"""

_EQUIV_SQL = """
WITH sig AS (
    SELECT * FROM unnest(
        %(sids)s::bigint[], %(rns)s::int[], %(stops)s::numeric[], %(targets)s::numeric[]
    ) AS t(series_id, rn, stop, target)
)
SELECT s.series_id,
       s.rn,
       -- ⚠ Every touch filter is clamped to the WINDOW. The join reaches one
       -- bar further because the expiry exit fills at the open of bar
       -- `rn + hold`, which is outside the window it exits from.
       --
       -- ⚠⚠ THE OPEN IS IN BOTH TOUCH FILTERS. The resolver's "stop touched on
       -- this bar" predicate is rules 1 OR 4, i.e. `open <= stop OR low <=
       -- stop`. On a bar satisfying `low <= open <= high` the first disjunct
       -- IMPLIES the second, so `low <= stop` alone agrees — but that identity
       -- is exactly what acceptance 14 MEASURES rather than assumes.
       --
       -- ⚠ Be precise about why this matters TODAY, because the obvious
       -- stronger claim is false. The raw corpus holds 541 bars whose open lies
       -- outside [low, high], and ALL 541 are currently `range_usable = false`
       -- and inside quarantine coverage — so the masked view this arm and the
       -- resolver both read contains NONE of them (acceptance 14 prints 0), and
       -- a masked bar refuses the touch test before any rule runs. Measured:
       --     WITH bad AS (SELECT series_id, bar_date FROM research_price_daily
       --                  WHERE open IS NOT NULL AND high IS NOT NULL
       --                    AND low IS NOT NULL
       --                    AND NOT (low <= open AND open <= high))
       --     SELECT (SELECT count(*) FROM bad),
       --            (SELECT count(*) FROM bad b JOIN research_bar_quarantine q
       --               ON q.series_id = b.series_id AND q.bar_date = b.bar_date
       --              AND q.rule_set_version = <current> AND NOT q.range_usable);
       --     -- (541, 541)
       --
       -- So the low/high-only form was right by the QUARANTINE'S GRACE at this
       -- rule-set version, not by construction — nothing makes the range rule
       -- target "open outside [low, high]" specifically, and a re-derivation
       -- that stops flagging one of these, or a caller using a different
       -- loader, breaks the implication silently. Encoding the full predicate
       -- costs nothing and removes the dependency. Same class as the mask
       -- tie-break fixed in f602a760. Found by Codex at checkpoint 2.
       min(b.rn) FILTER (WHERE b.rn <= s.rn + %(span)s
                           AND (b.low  <= s.stop   OR b.open <= s.stop))   AS k_sl,
       min(b.rn) FILTER (WHERE b.rn <= s.rn + %(span)s
                           AND (b.high >= s.target OR b.open >= s.target)) AS k_tp,
       min(b.rn) FILTER (WHERE b.rn <= s.rn + %(span)s
                           AND (b.open IS NULL OR b.high IS NULL OR b.low IS NULL)) AS k_mask,
       count(*)  FILTER (WHERE b.rn <= s.rn + %(span)s)                        AS bars_in_window,
       bool_or(b.rn = s.rn + %(exit_span)s AND b.open IS NOT NULL)             AS exit_bar_usable
FROM sig s
JOIN equiv_idx b
  ON b.series_id = s.series_id
 AND b.rn BETWEEN s.rn AND s.rn + %(exit_span)s
GROUP BY s.series_id, s.rn
"""


def _expected_class(
    *,
    k_sl: int | None,
    k_tp: int | None,
    k_mask: int | None,
    bars_in_window: int,
    exit_bar_usable: bool,
    hold: int,
) -> str:
    """The class implied by SQL's first-touch indices alone.

    ⚠ This is a comparison of three integers, not a re-walk. The walk's job IS
    to find those indices in the right order; a walk defect shows up here as a
    disagreeing index, which is the whole point of the arm.

    Returns `unresolved` without a reason for every refusal, and the caller
    compares only the CLASS for those — SQL can distinguish a mask from a
    truncation but not a caller-declared reason from a NULL column, and
    asserting a reason it cannot know would be a fake check.
    """
    touches = [k for k in (k_sl, k_tp) if k is not None]
    first_touch = min(touches) if touches else None
    # ⚠ `<=`, not `<`. The resolver checks open/high/low for None BEFORE applying
    # any rule, so a bar that is masked AND satisfies a touch predicate is
    # refused — the mask wins the tie. With `<` this verifier reports a false
    # MISMATCH for exactly the inputs the resolver explicitly supports (`open`
    # NULL with `high >= target`; `high` NULL with `low <= stop`).
    #
    # ⚠ Unreachable on THIS corpus and still wrong: `range_usable = false` masks
    # high and low together, so a masked bar matches neither touch predicate,
    # and `select count(*) - count(open) from research_price_daily` is 0. A
    # verifier that is only accidentally right is a verifier that fails the
    # first time the corpus gains a NULL open. Caught by Codex at checkpoint 2.
    if k_mask is not None and (first_touch is None or k_mask <= first_touch):
        return "unresolved"
    if k_sl is not None and (k_tp is None or k_sl < k_tp):
        return "sl_hit"
    if k_tp is not None and (k_sl is None or k_tp < k_sl):
        return "tp_hit"
    if k_sl is not None and k_tp is not None:
        return "ambiguous_or_gap"  # the tie: needs the open, resolved by the caller
    # Nothing touched. The window must be complete AND the exit bar must exist
    # with a usable open — otherwise this is a truncation and NOT an expiry.
    if bars_in_window < hold or not exit_bar_usable:
        return "unresolved"
    return "expired"


def equivalence(conn: psycopg.Connection[tuple]) -> int:
    """Acceptance 12 — FULL corpus, Python walk vs SQL first-touch indices."""
    ids = _series_ids(conn)
    quarantined = _load_quarantined(conn)
    conn.execute(_CREATE_IDX)
    conn.execute(_INDEX_IDX)
    checked = mismatches = ties = 0
    sample: list[str] = []
    started = time.perf_counter()

    for start in range(0, len(ids), _CHUNK):
        chunk = ids[start : start + _CHUNK]
        loaded = {sid: _load(conn, sid, quarantined.get(sid, set())) for sid in chunk}

        sids: list[int] = []
        rns: list[int] = []
        stops: list[Decimal] = []
        targets: list[Decimal] = []
        mine: dict[tuple[int, int], str] = {}

        for sid, series in loaded.items():
            n = len(series.bars)
            for i in range(n - 1):
                fill_index = i + 1
                entry = series.bars.rows[fill_index].get("open")
                if entry is None:
                    continue
                levels, _ = _levels(entry, series.atr[i], EQUIVALENCE_TP, EQUIVALENCE_HOLD)
                if levels is None:
                    continue
                out = resolve_outcome(
                    series=series.bars,
                    fill_index=fill_index,
                    entry_price=entry,
                    levels=levels,
                    masked_bar_reasons=series.unusable,  # type: ignore[arg-type]
                    segment_end_index=None,
                )
                sids.append(sid)
                rns.append(fill_index + 1)  # row_number() is 1-based
                stops.append(levels.stop_loss)
                targets.append(levels.take_profit)
                mine[(sid, fill_index + 1)] = out.outcome

        if not sids:
            continue

        conn.execute("TRUNCATE equiv_idx")
        conn.execute(_FILL_IDX, {"qv": QUARANTINE_RULE_SET_VERSION, "chunk": chunk})
        conn.execute("ANALYZE equiv_idx")
        rows = conn.execute(
            _EQUIV_SQL,
            {
                "sids": sids,
                "rns": rns,
                "stops": stops,
                "targets": targets,
                "span": EQUIVALENCE_HOLD - 1,
                "exit_span": EQUIVALENCE_HOLD,
            },
        ).fetchall()

        for series_id, rn, k_sl, k_tp, k_mask, bars_in_window, exit_bar_usable in rows:
            expected = _expected_class(
                k_sl=k_sl,
                k_tp=k_tp,
                k_mask=k_mask,
                bars_in_window=bars_in_window,
                exit_bar_usable=bool(exit_bar_usable),
                hold=EQUIVALENCE_HOLD,
            )
            got = mine[(series_id, rn)]
            checked += 1
            if expected == "ambiguous_or_gap":
                # SQL says both levels are first touched on the SAME bar. The
                # class then turns on that bar's open, which is rule 1/2 — the
                # gap-through construction — so the arm checks only that the
                # walk agreed it is one of the three, and the exact split is
                # pinned by tests/test_outcome_resolver.py.
                ties += 1
                if got not in ("ambiguous", "sl_hit", "tp_hit"):
                    mismatches += 1
                    if len(sample) < 10:
                        sample.append(f"series {series_id} rn {rn}: tie bar, walk said {got}")
                continue
            if got != expected:
                mismatches += 1
                if len(sample) < 10:
                    sample.append(f"series {series_id} rn {rn}: SQL {expected}, walk {got}")

        if (start + _CHUNK) % 500 < _CHUNK:
            print(
                f"  {min(start + _CHUNK, len(ids)):,}/{len(ids):,} series | "
                f"checked={checked:,} mismatches={mismatches}",
                flush=True,
            )

    print(f"\n=== acceptance 12 — equivalence ({RULE_SET_VERSION}) ===", flush=True)
    print(f"  cell           : TP = {EQUIVALENCE_TP}×ATR14, max hold {EQUIVALENCE_HOLD} bars", flush=True)
    print(f"  signals checked: {checked:,}", flush=True)
    print(f"  same-bar ties  : {ties:,} (class turns on the open — see the code comment)", flush=True)
    print(f"  MISMATCHES     : {mismatches:,}", flush=True)
    for line in sample:
        print(f"    {line}", flush=True)
    print(f"  elapsed: {time.perf_counter() - started:.1f}s", flush=True)
    return 0 if mismatches == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--distribution", action="store_true", help="acceptance 13 + 14")
    parser.add_argument("--equivalence", action="store_true", help="acceptance 12")
    args = parser.parse_args(argv)
    if not (args.distribution or args.equivalence):
        parser.error("pick at least one of --distribution / --equivalence")

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        if args.equivalence and (rc := equivalence(conn)):
            return rc
        if args.distribution and (rc := distribution(conn)):
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
