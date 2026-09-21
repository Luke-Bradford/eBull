"""ARM B blocker 3, candidate 1: can a SPLIT-ONLY basis be built for Intrader? Refs #2834.

``008da625`` (PR #3278) measured that s2's momentum selection is basis-sensitive
on ``BACKTEST_UNIVERSE = "survivorship_free"``: 15.00% of the top decile changes
between ``close`` (raw traded level on this vendor) and ``adj_close`` (a TOTAL
return, which §4 rejects). It closed with three candidate answers and named the
first as *"derive a split-only series for Intrader from corporate-action
evidence"*, ⚠ NOT from ``close``/``adj_close`` endpoint ratios — an endpoint
ratio conflates the split and the dividend adjustment and cannot separate them.

This script tests whether that evidence exists and whether it is right.

Source rule — the archive's OWN reader, not an inference
--------------------------------------------------------

An ``icyDenev/Intrader`` daily CSV is headerless, so the column order is the
only contract there is, and the archive ships the code that reads it:

* ``IntraderFramework/src/DataLoader.cpp:250-255`` pushes CSV fields ``1..N`` in
  order into one ``prices`` vector per bar, dropping only field 0 (the date).
* ``IntraderFramework/include/TickerData.h`` indexes that vector::

      enum class BaseIndicators { OPEN=0, HIGH=1, LOW=2, CLOSE=3, VOLUME=4,
                                  SPLIT=5, DIVIDEND=6, ADJ_CLOSE=7 };

So CSV field **6 is a split ratio** and field **7 a cash dividend per share**.
* ``IntraderFramework/src/IntraderEngine.cpp::SplitCheck()`` fixes the split
  field's SEMANTICS: it fires when ``GetSplit() != 1.0`` and multiplies the held
  share count by that value, so the ratio is "new shares per old share" and it
  is stamped on the bar where the split takes effect — AAPL's 4:1 carries ``4``
  on 2020-08-31, whose raw close (129.04) is already post-split, while
  2020-08-27 reads 500.04. A back-adjusting scale therefore applies the factor
  to bars STRICTLY EARLIER than the event date.
* ``DividendCheck()`` pays ``dividend * shares`` in cash, so field 7 is an
  amount per share and not a yield.

⚠ ``research_corpus_ingest.py:507`` already NAMES both columns
(``_INTRADER_COLUMNS``) and stores neither. That naming rested on #2398's
shared-bar check, which pinned the date/OHLCV/adjclose positions and never
touched fields 6 and 7 — this docstring is the first time the two middle
columns are tied to the vendor's own reader.

⚠ A source rule verified on one vendor is not a source rule for another
(``docs/review-prevention-log.md``, the lesson #3278 shipped). Everything above
is `icyDenev/Intrader` only. Nothing here says anything about the HF archive,
whose OHLC are already split-adjusted (sql/251), or about ``Stonks/tickers``.

What is measured
----------------

**Phase A — census.** Over the SETTLED admission
(``load_universe_selection(universe="survivorship_free")``, so the vendor pin,
capture-date bound, alive-at-capture cut and exchange-test-issue exclusion all
apply), read every admitted series' mirror CSV and collect every bar whose
split field is not 1. Reports how many series carry evidence, the event count,
and the ratio distribution — a sanity read, because a column of noise would not
concentrate on 2, 3, 1.5 and their reciprocals.

**Phase B1 — internal consistency, every event.** The split field is only useful
if it is CORRECT, and the vendor supplies its own check: ``adj_close`` carries
both the split and the dividend adjustment, so across a split date — which
carries no cash distribution of its own — the cumulative adjustment
``close / adj_close`` must step by exactly the stamped factor::

    (prev_close / prev_adj) / (event_close / event_adj)  ==  factor

AAPL 2020-08-31: ``(500.04/122.169) / (129.04/126.108) = 4.0006`` against a
stamped 4. It reaches every event, including those no second source serves.

⚠⚠ **It is CIRCULAR and it is not the gate.** ``adj_close`` is produced by the
same pipeline that wrote the stamp, so agreeing with it is not evidence the
stamp is right. Measured, not assumed: of the 383 events the second processing
disagrees with, **364 pass this check**. It also cannot see a split omitted from
both columns, a wrong factor copied into both, a same-day dividend contaminating
the step, or the events with no usable bar pair.

**Phase B1b — does the vendor's own CLOSE step by the factor it stamps?** One
vendor only, so it reaches every event — and it is weak by construction, because
a single series cannot separate the stamped factor from the day's own return.
Reported across bands for exactly that reason.

**Phase B2 — second-processing check, per event.** The strongest check
available, and it is still not independent. ``paperswithbacktest/Stocks-Daily-Price``
stores split-adjusted OHLC (sql/251), so comparing against it asks whether the
stamp reproduces the adjustment another processor applied to the same prices.
⚠ Both archives redistribute Yahoo — ``research_corpus_ingest.py``'s own
provenance record says they are *"ONE observation and agreement between them is
circular, never corroborating"*. So a split Yahoo never recorded is invisible to
every check in this file, and nothing here reaches a primary corporate-action
source. For each split event, compare the ONE-BAR gross return across the event
date on the same two bar dates:

    intrader_raw   = close(event) / close(previous bar)
    intrader_split = close(event) / close(previous bar) * factor
    reference      = pwb.close(event) / pwb.close(previous bar)

If field 6 is the split ratio, ``intrader_split ≈ reference`` and
``intrader_raw ≈ reference / factor``. ⚠ Using a RATIO rather than a level is
load-bearing: the reference vendor is captured 2026-07-08 and Intrader
2024-09-27, so the reference's levels carry later splits Intrader cannot know
about — and those cancel in a same-vendor ratio. The market's own return on the
event bar cancels too, because both vendors are read on the same two dates.

⚠ It covers roughly half the events. The reference serves 7,693 series against
this vendor's 22,879 and returns nothing for the 2023 Form 25 common-equity
cohort (`research-price-corpus.md`, 259-name cohort — ⚠ not the superseded 382).
"No reference pair" is NOT a synonym for "delisted", so ``unmatched_census``
measures what it actually is rather than letting the doc assume.

B2's disagreements are CLASSIFIED rather than counted, because a raw count would
charge the other processing's defects to the field under test. Two classes are
separated, each with a worked case:

* **the REFERENCE is not split-adjusted at that date.** sql/251 established
  ``split_adjusted`` for that vendor on ONE bar (AAPL 2020-08-27), and it does
  not hold everywhere: CIVB 1996-05-09 prints ``81 -> 20.25`` on Intrader and
  ``20.25 -> 5.0625`` on the reference — BOTH carry the raw 4:1 step, so their
  ratios agree and correcting Intrader is what opens the gap. Here Intrader's
  own price step corroborates the stamped factor and the disagreement is the
  reference's.
* **the stamped factor has the wrong magnitude.** COO 2024-02-20 prints
  ``372.01 -> 95.70``, a 4:1, against a stamped **16** — and the vendor
  back-adjusted its own ``adj_close`` by 16 as well (23.25 -> 95.70), which is
  why B1 passes on it. This is the class B1 structurally cannot see, and it is
  the reason B2 is run at all.

**Phase C — materiality.** The same decile comparison #3278 ran, with the
second arm changed from ``adj_close`` to the split-only series
``close / scale``, where ``scale(d)`` is the product of the factors of every
event STRICTLY AFTER ``d``. This answers what #3278 could not: how much of s2's
selection the correction actually moves. Same window, same admission, same
quarantine version, so the two runs' figures are comparable.

Declared fidelity limits (all four of #3278's apply unchanged — weekends
dropped before the positional lags, no segment resets, Postgres ``numeric``,
reads unbounded by the window), plus one that is specific to this arm:

5. **``MIN_CLOSE`` is applied to the RAW close in both arms**, as s2 does
   today. Under a back-adjusted basis a name's adjusted close can sit below the
   floor while its raw close clears it; moving the floor would change
   eligibility between arms and break the shared cross-section that makes the
   overlap like-for-like. What the floor SHOULD read on a corrected basis is a
   separate question and is not answered here.

Run::

    PYTHONPATH=. uv run python -m scripts.measure_2834_armb_split_only_basis

Exit code: 0 when applying the stamped factor INCREASES agreement with the
second processing (B2), 1 when it does not, 2 when nothing could be checked.
That criterion is parameter-free on purpose — it asks whether the field carries
corporate-action information at all, which is the strongest question the
available evidence can answer. It is NOT a correctness verdict: see the
circularity warning above. Phase C is reported either way and has no bar.
"""

from __future__ import annotations

import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, TextIO

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION
from app.services.research_corpus_ingest import _INTRADER_COLUMNS
from app.services.strategies.s2_cross_sectional_momentum import (
    DECILE,
    LOOKBACK_BARS,
    MIN_CLOSE,
    MIN_CROSS_SECTION,
    SKIP_BARS,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import load_universe_selection

#: The mirror this vendor is read from. ⚠ NEVER fetched — see
#: ``IntraderCsvArchive``'s docstring: re-fetching would let the capture date,
#: the single property that makes an archive survivorship-free, drift.
_MIRROR: Final[Path] = Path.home() / "Dev/eBull/var/research_corpus/mirrors/icyDenev_Intrader/Data/Day"

#: 0-based CSV field indices, derived from ``_INTRADER_COLUMNS`` rather than
#: written twice — the ingest module owns that tuple.
_SPLIT_FIELD: Final[int] = _INTRADER_COLUMNS.index("split_factor")
_DIVIDEND_FIELD: Final[int] = _INTRADER_COLUMNS.index("dividend")

#: The independent split-adjusted vendor Phase B validates against (sql/251).
_REFERENCE_VENDOR: Final[str] = "paperswithbacktest/Stocks-Daily-Price"

# ⚠ PUBLIC, and deliberately: the three constants below are the MEASUREMENT
# CONTRACT this ticket's figures are comparable across. #3278's 15.00%, this
# script's 14.30% and #2834's policy displacement table are only comparable
# because all three score the same window with the same eligibility gate, so a
# sibling measurement must IMPORT them rather than restate them. Underscoring
# them would say "private", which would make the honest thing look like a
# violation and the drift-prone copy look correct (#3281 review).

#: s2's eligibility gate — the module ships both numbers literally (score from
#: ``t-252``, refuse until 273 bars) and #3278 read it the same way.
ELIGIBILITY_BARS: Final[int] = 273

#: Exploration window, IDENTICAL to #3278's so Phase C is comparable with the
#: 15.00% it measured. Lower bound: first ``spy_chain_v1`` regime date.
#: Upper bound clears every registered hold-out on this corpus.
WINDOW_START: Final[date] = date(1994, 1, 1)
WINDOW_END: Final[date] = date(2021, 6, 29)  # exclusive

#: Phase B tolerance on ``|intrader_split / reference - 1|``. BY CONSTRUCTION:
#: the two vendors round to different precisions (the reference stores 125.01
#: where the exact quarter of 500.04 is 125.01), so an exact match is not
#: available and no published rule fixes a bound. 1% is far tighter than the
#: factor being tested (the smallest ratio a split carries is 1.1) and far
#: looser than two vendors' rounding of the same bar.
_EVENT_TOLERANCE: Final[Decimal] = Decimal("0.01")

#: ⚠ There is deliberately NO agreement-percentage bar. An earlier version
#: gated on "95% of events agree", which is an invented threshold, and gated it
#: on the INTERNAL check — a check this script's own findings show is circular
#: (``adj_close`` moves with the stamp, so COO's 16-for-4 passes it). The gate
#: below is parameter-free instead: applying the stamped factor must INCREASE
#: agreement with the second processing. If it does not, field 6 is not a split
#: ratio and nothing else in the output matters. The residual rate is reported
#: as the quality measure the follow-on spec must carry, not as a pass mark.


@dataclass(frozen=True)
class SplitEvent:
    """One bar whose split field is not 1, for one admitted series."""

    series_id: int
    vendor_symbol: str
    bar_date: date
    factor: Decimal


@dataclass(frozen=True)
class EventCheck:
    """One split event, checked two independent ways.

    ``internal_error`` is the vendor against ITSELF and needs no second vendor,
    so it reaches all 8,073 events including the delisted half: the vendor's own
    cumulative adjustment ``close / adj_close`` must step by exactly the stamped
    factor across the event, because ``adj_close`` carries both the split and the
    dividend adjustment and a split date carries no distribution of its own.
    AAPL 2020-08-31: ``(500.04/122.169) / (129.04/126.108) = 4.0006`` against a
    stamped 4. ⚠ It cannot catch a split the vendor omitted from BOTH columns —
    it tests self-consistency, which is necessary and not sufficient.

    ``split_error`` is the independent cross-vendor check and is ``None`` where
    the reference vendor serves no comparable bar pair.
    """

    event: SplitEvent
    internal_error: Decimal | None
    raw_error: Decimal | None
    split_error: Decimal | None
    #: The one-bar gross returns the two errors are computed from, kept so a
    #: disagreement can be CLASSIFIED rather than only counted.
    raw_ratio: Decimal | None = None
    ref_ratio: Decimal | None = None

    @property
    def internal_agrees(self) -> bool:
        return self.internal_error is not None and self.internal_error <= _EVENT_TOLERANCE

    @property
    def agrees(self) -> bool:
        return self.split_error is not None and self.split_error <= _EVENT_TOLERANCE

    @property
    def raw_agrees(self) -> bool:
        """Whether the UNCORRECTED arm also agrees — the contrast that matters.

        If raw agreed too the event would be non-discriminating (a factor of 1
        in disguise), so this counts how much work the correction is doing. It
        is also the residual CLASSIFIER: a disagreement where raw agrees means
        neither vendor's prices step at that date, which is a different defect
        from a factor of the wrong size.
        """
        return self.raw_error is not None and self.raw_error <= _EVENT_TOLERANCE

    @property
    def reference_is_unadjusted(self) -> bool:
        """Does the REFERENCE vendor's own close carry the raw split step?

        sql/251 recorded ``paperswithbacktest/Stocks-Daily-Price`` as
        ``split_adjusted`` on ONE bar (AAPL 2020-08-27). Where that does not
        hold, the reference's close steps across the split like an unadjusted
        series — ``ref_ratio ~= 1 / factor`` — and it is the reference, not the
        stamped factor, that the disagreement is about. CIVB 1996-05-09: BOTH
        vendors print 81 -> 20.25 on a 4:1, so their ratios agree at 0.25 and
        correcting one of them is what creates the gap.
        """
        if self.ref_ratio is None or self.ref_ratio <= 0:
            return False
        return abs(self.ref_ratio * self.event.factor - 1) <= _EVENT_TOLERANCE

    @property
    def implied_factor(self) -> Decimal | None:
        """The step the raw series actually has, measured against the reference.

        Only meaningful where the reference is adjusted at that date; where it
        is not, this returns ~1 by construction. COO 2024-02-20: implied 4.0
        against a stamped 16, i.e. the true split is 4:1 and the vendor
        back-adjusted its own ``adj_close`` by the wrong factor too — which is
        why the internal check passes on it and only a second vendor catches it.
        """
        if self.raw_ratio is None or self.ref_ratio is None or self.raw_ratio <= 0:
            return None
        return self.ref_ratio / self.raw_ratio


@dataclass(frozen=True)
class Formation:
    """One rebalance date's two deciles and their overlap (#3278's shape)."""

    bar_date: date
    cross_section: int
    decile_size: int
    shared: int
    raw_only: int
    split_only: int

    @property
    def disagreement_pct(self) -> float:
        return 0.0 if self.decile_size <= 0 else 100.0 * self.raw_only / self.decile_size


def _admitted(conn: psycopg.Connection[tuple[Any, ...]]) -> dict[int, str]:
    """``series_id -> vendor_symbol`` for the settled ``survivorship_free`` admission.

    ⚠ The admission rule, never a bare vendor filter — #3278 corrected exactly
    that mistake, and the correction moved its headline figure.
    """
    validated = frozenset(load_validated_universe(conn))
    selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=validated)
    ids = [row.series_id for row in selection.admitted]
    rows = conn.execute(
        "SELECT series_id, vendor_symbol FROM research_price_series WHERE series_id = ANY(%s)",
        (ids,),
    ).fetchall()
    return {int(series_id): str(symbol) for series_id, symbol in rows}


def _name_keys(conn: psycopg.Connection[tuple[Any, ...]]) -> dict[int, int]:
    """``series_id -> name_key`` — the engine's own total ordering key."""
    validated = frozenset(load_validated_universe(conn))
    selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=validated)
    return {row.series_id: row.name_key for row in selection.admitted}


def read_split_events(symbols: dict[int, str], *, mirror: Path = _MIRROR) -> tuple[list[SplitEvent], int, int]:
    """Scan the mirror for every non-unit split field on the admitted series.

    Returns the events, the number of series whose CSV was missing, and the
    number of bars carrying a non-zero dividend field (reported for scale only
    — the dividend column is not used to build anything here, because §4 wants
    a price return).

    ⚠ A malformed field is SKIPPED rather than read as 1. Reading it as 1 would
    silently assert "no corporate action on this bar", which is the claim the
    whole script exists to establish.
    """
    events: list[SplitEvent] = []
    missing = 0
    dividend_bars = 0
    for series_id, symbol in sorted(symbols.items(), key=lambda item: item[1]):
        path = mirror / f"{symbol}.csv"
        if not path.exists():
            missing += 1
            continue
        with path.open() as handle:
            for line in handle:
                fields = line.rstrip("\n").split(",")
                if len(fields) < len(_INTRADER_COLUMNS):
                    continue
                try:
                    factor = Decimal(fields[_SPLIT_FIELD])
                    dividend = Decimal(fields[_DIVIDEND_FIELD])
                except Exception:
                    continue
                if dividend != 0:
                    dividend_bars += 1
                if factor == 1 or not factor.is_finite() or factor <= 0:
                    continue
                try:
                    bar_date = date.fromisoformat(fields[0].strip())
                except ValueError:
                    continue
                events.append(SplitEvent(series_id=series_id, vendor_symbol=symbol, bar_date=bar_date, factor=factor))
    return events, missing, dividend_bars


def back_adjust_scale(events: list[SplitEvent]) -> list[tuple[int, date, date, Decimal]]:
    """Events → ``(series_id, valid_from, valid_to, scale)`` segments.

    ``scale(d)`` is the product of the factors of every event STRICTLY AFTER
    ``d``, so the split-only series is ``close(d) / scale(d)`` — back-adjusted
    to the series' own last bar, whose scale is 1. The event date itself already
    prints the post-split level (``SplitCheck``'s semantics above), so it takes
    the scale of the bars after it, not before.

    Computed in ``Decimal`` and not as ``exp(sum(ln(f)))``: split ratios are
    exact small rationals and their product should stay exact, so any drift a
    reader sees is a real disagreement rather than float noise. Segments are
    half-open ``[valid_from, valid_to)`` and only cover dates where the scale is
    not 1, so a missing row means "no later split" and the reader coalesces.
    """
    by_series: dict[int, list[SplitEvent]] = {}
    for event in events:
        by_series.setdefault(event.series_id, []).append(event)

    segments: list[tuple[int, date, date, Decimal]] = []
    for series_id, series_events in by_series.items():
        # Walk the events newest-first, accumulating the product; `bounds` then
        # holds, per event date, the scale that applies to every bar BEFORE it.
        bounds: list[tuple[date, Decimal]] = []
        scale = Decimal(1)
        for event in sorted(series_events, key=lambda e: e.bar_date, reverse=True):
            scale *= event.factor
            bounds.append((event.bar_date, scale))
        bounds.reverse()
        previous = date.min
        for bar_date, cumulative in bounds:
            # Half-open [previous, bar_date): bars on or after the event date
            # already print the post-split level.
            segments.append((series_id, previous, bar_date, cumulative))
            previous = bar_date
    return segments


#: ⚠ Every reference join is a LEFT JOIN, so an event the reference vendor does
#: not serve still returns its row and reaches the INTERNAL check. An inner join
#: here would silently restrict both checks to the survivor half — the reference
#: serves 7,693 series against this vendor's 22,879 and is 0/382 on the delisted
#: cohort (`research-price-corpus.md`), which is exactly the half a
#: survivorship-free corpus exists for.
_EVENT_CHECK_SQL = """
    WITH ev AS (
        SELECT e.series_id, e.vendor_symbol, e.bar_date, e.factor
        FROM _split_events e
    ),
    prior AS (
        SELECT ev.*,
               (SELECT max(d.bar_date)
                  FROM research_price_daily d
                 WHERE d.series_id = ev.series_id
                   AND d.bar_date < ev.bar_date) AS prev_date
        FROM ev
    )
    SELECT p.series_id,
           p.vendor_symbol,
           p.bar_date,
           p.factor,
           cur.close      AS cur_close,
           prv.close      AS prev_close,
           cur.adj_close  AS cur_adj,
           prv.adj_close  AS prev_adj,
           rcur.close     AS ref_cur_close,
           rprv.close     AS ref_prev_close
    FROM prior p
    JOIN research_price_daily cur
      ON cur.series_id = p.series_id AND cur.bar_date = p.bar_date
    JOIN research_price_daily prv
      ON prv.series_id = p.series_id AND prv.bar_date = p.prev_date
    LEFT JOIN research_price_series ref
      ON ref.vendor = %(reference_vendor)s AND ref.vendor_symbol = p.vendor_symbol
    LEFT JOIN research_price_daily rcur
      ON rcur.series_id = ref.series_id AND rcur.bar_date = p.bar_date
     AND rcur.close > 0
    LEFT JOIN research_price_daily rprv
      ON rprv.series_id = ref.series_id AND rprv.bar_date = p.prev_date
     AND rprv.close > 0
    WHERE cur.close > 0 AND prv.close > 0
"""


def check_events(conn: psycopg.Connection[tuple[Any, ...]], events: list[SplitEvent]) -> tuple[list[EventCheck], int]:
    """Check every split event internally, and against the reference vendor where it can.

    Returns the checks and the number of events that produced no row at all —
    an event whose own bar or whose previous bar is missing or non-positive in
    ``research_price_daily``, which neither check can speak to.
    """
    conn.execute(
        "CREATE TEMP TABLE _split_events ("
        " series_id bigint, vendor_symbol text, bar_date date, factor numeric) ON COMMIT DROP"
    )
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO _split_events (series_id, vendor_symbol, bar_date, factor) VALUES (%s, %s, %s, %s)",
            [(e.series_id, e.vendor_symbol, e.bar_date, e.factor) for e in events],
        )
    conn.execute("CREATE INDEX ON _split_events (series_id, bar_date)")
    rows = conn.execute(_EVENT_CHECK_SQL, {"reference_vendor": _REFERENCE_VENDOR}).fetchall()

    checks: list[EventCheck] = []
    for series_id, symbol, bar_date, factor, cur_close, prev_close, cur_adj, prev_adj, ref_cur, ref_prev in rows:
        stamped = Decimal(factor)
        raw = Decimal(cur_close) / Decimal(prev_close)
        internal: Decimal | None = None
        if cur_adj is not None and prev_adj is not None and Decimal(cur_adj) > 0 and Decimal(prev_adj) > 0:
            # (close/adj_close) before ÷ (close/adj_close) on the event bar.
            implied = (Decimal(prev_close) / Decimal(prev_adj)) / (Decimal(cur_close) / Decimal(cur_adj))
            internal = abs(implied / stamped - 1)
        raw_error: Decimal | None = None
        split_error: Decimal | None = None
        ref_ratio: Decimal | None = None
        if ref_cur is not None and ref_prev is not None:
            reference = Decimal(ref_cur) / Decimal(ref_prev)
            raw_error = abs(raw / reference - 1)
            split_error = abs(raw * stamped / reference - 1)
            ref_ratio = reference
        checks.append(
            EventCheck(
                event=SplitEvent(
                    series_id=int(series_id),
                    vendor_symbol=str(symbol),
                    bar_date=bar_date,
                    factor=stamped,
                ),
                internal_error=internal,
                raw_error=raw_error,
                split_error=split_error,
                raw_ratio=raw,
                ref_ratio=ref_ratio,
            )
        )
    return checks, len(events) - len(checks)


#: For a DISAGREEING event, does the step the factor describes exist on a
#: NEIGHBOURING bar? That is the difference between a stamp the price series
#: does not support at all and a stamp whose date convention is off — the first
#: is unusable evidence, the second is a fixable offset. ⚠ The window is
#: deliberately narrow (±``_OFFSET_BARS``) and the match is on the RAW close
#: ratio alone, so a genuine market move of the same size in a volatile name can
#: register a false match; the figure is an UPPER bound on recoverability.
_OFFSET_SQL = """
    WITH nearby AS (
        SELECT r.series_id,
               r.bar_date AS event_date,
               r.factor,
               d.bar_date,
               d.close / lag(d.close) OVER (PARTITION BY d.series_id ORDER BY d.bar_date) AS ratio,
               (SELECT count(*)
                  FROM research_price_daily b
                 WHERE b.series_id = r.series_id
                   AND b.bar_date > least(d.bar_date, r.bar_date)
                   AND b.bar_date <= greatest(d.bar_date, r.bar_date)) AS bar_gap
        FROM _residual r
        JOIN research_price_daily d
          ON d.series_id = r.series_id
         AND d.bar_date BETWEEN r.bar_date - %(span)s AND r.bar_date + %(span)s
         AND d.close > 0
    )
    SELECT series_id,
           event_date,
           min(abs(ratio * factor - 1)) AS best_error
    FROM nearby
    WHERE ratio IS NOT NULL
      AND bar_gap <= %(bars)s
      AND bar_date <> event_date
    GROUP BY series_id, event_date
"""


#: Trading bars either side of a stamped event that the offset probe looks in.
_OFFSET_BARS: Final[int] = 3

#: Calendar days the probe reads to find those bars — wider than the bar count
#: so holidays cannot silently shorten the window; ``bar_gap`` does the real
#: bounding.
_OFFSET_SPAN_DAYS: Final[int] = 10


def find_offset_steps(
    conn: psycopg.Connection[tuple[Any, ...]], disagreeing: list[EventCheck]
) -> dict[tuple[int, date], Decimal]:
    """``(series_id, event_date) -> smallest |ratio * factor - 1|`` on a nearby bar."""
    if not disagreeing:
        return {}
    conn.execute("CREATE TEMP TABLE _residual (series_id bigint, bar_date date, factor numeric) ON COMMIT DROP")
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO _residual (series_id, bar_date, factor) VALUES (%s, %s, %s)",
            [(c.event.series_id, c.event.bar_date, c.event.factor) for c in disagreeing],
        )
    conn.execute("CREATE INDEX ON _residual (series_id, bar_date)")
    rows = conn.execute(_OFFSET_SQL, {"span": timedelta(days=_OFFSET_SPAN_DAYS), "bars": _OFFSET_BARS}).fetchall()
    return {(int(series_id), event_date): Decimal(best) for series_id, event_date, best in rows}


_CALENDAR_SQL = """
    CREATE TEMP TABLE so_rebalance ON COMMIT DROP AS
    WITH weekdays AS (
        SELECT DISTINCT d.bar_date
        FROM research_price_daily d
        WHERE d.series_id = ANY(%(series_ids)s)
          AND extract(isodow FROM d.bar_date) < 6
    ),
    seq AS (
        SELECT bar_date, lag(bar_date) OVER (ORDER BY bar_date) AS previous
        FROM weekdays
    )
    SELECT bar_date
    FROM seq
    WHERE previous IS NOT NULL
      AND date_trunc('month', bar_date) <> date_trunc('month', previous)
      AND bar_date >= %(window_start)s
      AND bar_date < %(window_end)s
"""

#: ⚠ The quarantine join pins ``rule_set_version`` on BOTH the coverage and the
#: verdict table, per ``_RANKING_SQL``. Joining verdicts alone fails OPEN
#: outside the rows the current rule set covers.
_PANEL_SQL = """
    CREATE TEMP TABLE so_panel ON COMMIT DROP AS
    WITH bars AS (
        SELECT d.series_id,
               d.bar_date,
               CASE WHEN COALESCE(q.return_usable, TRUE) THEN d.close END AS close,
               CASE WHEN COALESCE(q.return_usable, TRUE)
                    THEN d.close / COALESCE(sc.scale, 1) END              AS split_close
        FROM research_price_daily d
        JOIN research_price_quarantine_coverage cov
          ON cov.series_id = d.series_id
         AND cov.rule_set_version = %(version)s
         AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN research_bar_quarantine q
          ON q.series_id = d.series_id
         AND q.bar_date = d.bar_date
         AND q.rule_set_version = %(version)s
        LEFT JOIN _split_scale sc
          ON sc.series_id = d.series_id
         AND d.bar_date >= sc.valid_from
         AND d.bar_date <  sc.valid_to
        WHERE d.series_id = ANY(%(series_ids)s)
          AND extract(isodow FROM d.bar_date) < 6
    ),
    windowed AS (
        SELECT series_id,
               bar_date,
               close,
               lag(close, %(skip)s)             OVER s AS c_skip,
               lag(close, %(lookback)s)         OVER s AS c_back,
               lag(split_close, %(skip)s)       OVER s AS s_skip,
               lag(split_close, %(lookback)s)   OVER s AS s_back,
               row_number()                     OVER s AS rn,
               count(*) OVER (PARTITION BY series_id) AS n_bars
        FROM bars
        WINDOW s AS (PARTITION BY series_id ORDER BY bar_date)
    )
    SELECT w.series_id,
           w.bar_date,
           w.c_skip / w.c_back - 1 AS score_raw,
           w.s_skip / w.s_back - 1 AS score_split
    FROM windowed w
    JOIN so_rebalance r ON r.bar_date = w.bar_date
    WHERE w.rn >= %(eligibility)s
      AND w.rn < w.n_bars
      AND w.close IS NOT NULL
      AND w.close >= %(floor)s
      AND w.c_skip IS NOT NULL AND w.c_skip > 0
      AND w.c_back IS NOT NULL AND w.c_back > 0
      AND w.s_skip IS NOT NULL AND w.s_skip > 0
      AND w.s_back IS NOT NULL AND w.s_back > 0
"""

_OVERLAP_SQL = """
    WITH ranked AS (
        SELECT p.bar_date,
               p.series_id,
               nk.name_key,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_raw   DESC, nk.name_key) AS pos_raw,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_split DESC, nk.name_key) AS pos_split,
               count(*)     OVER (PARTITION BY p.bar_date) AS n
        FROM so_panel p
        JOIN so_name nk ON nk.series_id = p.series_id
    ),
    cut AS (
        SELECT bar_date,
               n,
               n / %(decile)s AS decile_size,
               pos_raw   <= n / %(decile)s AS in_raw,
               pos_split <= n / %(decile)s AS in_split
        FROM ranked
        WHERE n >= %(min_cross_section)s
    )
    SELECT bar_date,
           max(n)                                            AS cross_section,
           max(decile_size)                                  AS decile_size,
           count(*) FILTER (WHERE in_raw AND in_split)       AS shared,
           count(*) FILTER (WHERE in_raw AND NOT in_split)   AS raw_only,
           count(*) FILTER (WHERE in_split AND NOT in_raw)   AS split_only
    FROM cut
    GROUP BY bar_date
    ORDER BY bar_date
"""


def measure_displacement(
    conn: psycopg.Connection[tuple[Any, ...]],
    names: dict[int, int],
    segments: list[tuple[int, date, date, Decimal]],
) -> list[Formation]:
    """Phase C — top-decile membership under raw close vs the split-only series."""
    params: dict[str, Any] = {
        "series_ids": list(names),
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        "version": RULE_SET_VERSION,
        "skip": SKIP_BARS,
        "lookback": LOOKBACK_BARS,
        "eligibility": ELIGIBILITY_BARS,
        "floor": MIN_CLOSE,
        "decile": DECILE,
        "min_cross_section": MIN_CROSS_SECTION,
    }
    conn.execute("CREATE TEMP TABLE so_name (series_id bigint PRIMARY KEY, name_key bigint) ON COMMIT DROP")
    conn.execute(
        "CREATE TEMP TABLE _split_scale ("
        " series_id bigint, valid_from date, valid_to date, scale numeric) ON COMMIT DROP"
    )
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO so_name (series_id, name_key) VALUES (%s, %s)", list(names.items()))
        cur.executemany(
            "INSERT INTO _split_scale (series_id, valid_from, valid_to, scale) VALUES (%s, %s, %s, %s)",
            segments,
        )
    conn.execute("CREATE INDEX ON _split_scale (series_id, valid_from, valid_to)")
    conn.execute(_CALENDAR_SQL, params)
    conn.execute(_PANEL_SQL, params)
    return [
        Formation(
            bar_date=row[0],
            cross_section=int(row[1]),
            decile_size=int(row[2]),
            shared=int(row[3]),
            raw_only=int(row[4]),
            split_only=int(row[5]),
        )
        for row in conn.execute(_OVERLAP_SQL, params).fetchall()
    ]


def expected_months(start: date, end: date) -> int:
    """Calendar months whose first day falls in ``[start, end)``."""
    return (end.year - start.year) * 12 + (end.month - start.month)


def _report_census(
    events: list[SplitEvent], missing: int, dividend_bars: int, admitted: int, *, stream: TextIO
) -> None:
    series_with = len({e.series_id for e in events})
    print(f"admitted series            {admitted:,}", file=stream)
    print(f"mirror CSV absent          {missing:,}", file=stream)
    print(f"split events               {len(events):,} on {series_with:,} series", file=stream)
    print(f"bars with a dividend field {dividend_bars:,}  (not used — §4 wants a price return)", file=stream)
    if not events:
        return
    ratios = Counter(str(e.factor.normalize()) for e in events)
    print("\nmost common split ratios (field 6)", file=stream)
    for ratio, count in ratios.most_common(12):
        print(f"  {ratio:>12} {count:>8,}", file=stream)
    print(f"  distinct ratios {len(ratios):,}", file=stream)


def unmatched_census(conn: psycopg.Connection[tuple[Any, ...]], checks: list[EventCheck]) -> tuple[int, int, int, int]:
    """Why does an event have no reference bar pair, and is it a delisting?

    ⚠ This exists because "no reference pair" is NOT a synonym for "delisted",
    and an earlier draft of the verdict used the two interchangeably. A missing
    pair can be an absent symbol, a symbol the reference serves over a shorter
    span, a non-positive bar, or a join failure. Returns ``(unmatched,
    symbol_absent_from_reference, on_a_series_with_delisting_evidence,
    symbol_present_but_no_bar)``.
    """
    unmatched = [c for c in checks if c.split_error is None]
    if not unmatched:
        return (0, 0, 0, 0)
    symbols = sorted({c.event.vendor_symbol for c in unmatched})
    series_ids = sorted({c.event.series_id for c in unmatched})
    served = {
        str(row[0])
        for row in conn.execute(
            "SELECT vendor_symbol FROM research_price_series WHERE vendor = %s AND vendor_symbol = ANY(%s)",
            (_REFERENCE_VENDOR, symbols),
        ).fetchall()
    }
    delisted = {
        int(row[0])
        for row in conn.execute(
            "SELECT series_id FROM research_price_series WHERE series_id = ANY(%s) AND delisting_source IS NOT NULL",
            (series_ids,),
        ).fetchall()
    }
    absent = sum(1 for c in unmatched if c.event.vendor_symbol not in served)
    with_evidence = sum(1 for c in unmatched if c.event.series_id in delisted)
    return (len(unmatched), absent, with_evidence, len(unmatched) - absent)


def _report_events(
    checks: list[EventCheck],
    unusable: int,
    offsets: dict[tuple[int, date], Decimal],
    census: tuple[int, int, int, int] | None,
    *,
    stream: TextIO,
) -> tuple[float, float]:
    """Print both checks and return ``(internal_pct, reference_pct)``."""
    internal = [c for c in checks if c.internal_error is not None]
    referenced = [c for c in checks if c.split_error is not None]
    print("\nPhase B1 — INTERNAL: does the vendor's own adj_close honour its own split field?", file=stream)
    print(f"  events with both bars      {len(checks):,}  (unusable bar pair: {unusable:,})", file=stream)
    print(f"  adj_close on both bars     {len(internal):,}", file=stream)
    internal_pct = 0.0
    if internal:
        agree = sum(1 for c in internal if c.internal_agrees)
        internal_pct = 100.0 * agree / len(internal)
        print(f"  stamped factor reproduced  {agree:,} ({internal_pct:.2f}%) within {_EVENT_TOLERANCE}", file=stream)
        worst = sorted(internal, key=lambda c: c.internal_error or Decimal(0), reverse=True)[:8]
        print("  largest internal residuals", file=stream)
        for check in worst:
            print(
                f"    {check.event.vendor_symbol:<8} {check.event.bar_date}"
                f"  factor {check.event.factor.normalize():>20}  err {float(check.internal_error or 0):.4f}",
                file=stream,
            )

    # ── Corroboration by the vendor's OWN price step ────────────────────────
    # The decisive number for whether candidate 1 can be built WITHOUT the
    # survivor-only reference: does Intrader's own close step by the factor it
    # stamps? This needs one vendor, so it reaches all 8,073 events — and
    # splitting it by whether a reference exists tests whether the half B2
    # cannot see behaves like the half it can.
    print("\nPhase B1b — does the vendor's OWN close step by the factor it stamps?", file=stream)
    print("  (one vendor only, so it reaches every event — and cannot separate the day's own return)", file=stream)
    with_ratio = [c for c in checks if c.raw_ratio is not None and c.raw_ratio > 0]
    served = [c for c in with_ratio if c.ref_ratio is not None]
    unserved = [c for c in with_ratio if c.ref_ratio is None]
    header = f"  {'band':>8} {'all':>18} {'reference-served':>20} {'reference-absent':>20}"
    print(header, file=stream)
    for band in (Decimal("0.01"), Decimal("0.05"), Decimal("0.10"), Decimal("0.20")):

        def rate(rows: list[EventCheck], bound: Decimal = band) -> str:
            if not rows:
                return "—"
            hit = sum(1 for c in rows if c.raw_ratio is not None and abs(c.raw_ratio * c.event.factor - 1) <= bound)
            return f"{hit:,} ({100.0 * hit / len(rows):.2f}%)"

        print(
            f"  {float(band):>7.0%} {rate(with_ratio):>18} {rate(served):>20} {rate(unserved):>20}",
            file=stream,
        )
    print(
        "  ⚠ the band must absorb the DAY'S OWN market move as well as rounding, so a wide\n"
        "    band over-admits and a narrow one rejects real splits on volatile days. No\n"
        "    published rule fixes it; the figures are reported across bands rather than\n"
        "    one being chosen here.",
        file=stream,
    )

    print(f"\nPhase B2 — SECOND PROCESSING of the same upstream: {_REFERENCE_VENDOR}", file=stream)
    print(f"  events with a bar pair     {len(referenced):,}", file=stream)
    print(f"  no reference bar pair      {len(checks) - len(referenced):,}", file=stream)
    if census is not None and census[0]:
        total, absent, with_evidence, present = census
        print(
            f"    symbol not served by the reference at all    {absent:,}\n"
            f"    symbol served but no bar on one of the dates {present:,}\n"
            f"    on a series carrying DELISTING evidence      {with_evidence:,} of {total:,}"
            "   <- the only measured link to survivorship",
            file=stream,
        )
    reference_pct = 0.0
    if referenced:
        agree = sum(1 for c in referenced if c.agrees)
        raw_agree = sum(1 for c in referenced if c.raw_agrees)
        reference_pct = 100.0 * agree / len(referenced)
        print(f"  split-corrected agrees     {agree:,} ({reference_pct:.2f}%)", file=stream)
        print(
            f"  UNcorrected agrees         {raw_agree:,} ({100.0 * raw_agree / len(referenced):.2f}%)  <- the contrast",
            file=stream,
        )
        # ⚠ EVERY figure in this phase is conditioned on the tolerance, not just
        # the residual classes below, so the headline is printed across bands
        # rather than at one.
        print("  tolerance sensitivity of both arms", file=stream)
        for band in (Decimal("0.01"), Decimal("0.02"), Decimal("0.05"), Decimal("0.10")):
            ok = sum(1 for c in referenced if c.split_error is not None and c.split_error <= band)
            raw_ok = sum(1 for c in referenced if c.raw_error is not None and c.raw_error <= band)
            both = sum(
                1
                for c in referenced
                if c.split_error is not None
                and c.raw_error is not None
                and c.split_error <= band
                and c.raw_error <= band
            )
            print(
                f"    {float(band):>6.0%}  corrected {100.0 * ok / len(referenced):>6.2f}%"
                f"   uncorrected {100.0 * raw_ok / len(referenced):>6.2f}%"
                f"   BOTH, so non-discriminating {both:>5,}",
                file=stream,
            )
        # Residual classification. A disagreement where the UNCORRECTED arm
        # agrees means neither vendor's prices step at that date, which is a
        # different defect from a factor of the wrong size — and only the
        # second is a candidate-1 blocker.
        disagreeing = [c for c in referenced if not c.agrees]
        # Three mutually exclusive classes, keyed on the step Intrader's RAW
        # close actually has relative to the reference (``implied_factor``).
        # Only two of them are candidate-1 error, and only the last is invisible
        # to B1.
        # ⚠ Partitioned in ONE pass on the predicates themselves, not by list
        # membership. `c not in ref_unadjusted` would be an O(n) scan per item
        # using dataclass equality — and equality here compares four Decimals,
        # so two genuinely distinct events with identical errors could collide.
        # The predicates are exhaustive and ordered, so a single walk is both
        # faster and exactly mutually exclusive by construction.
        ref_unadjusted: list[EventCheck] = []
        no_step: list[EventCheck] = []
        magnitude: list[EventCheck] = []
        for check in disagreeing:
            implied = check.implied_factor
            if check.reference_is_unadjusted:
                ref_unadjusted.append(check)
            elif implied is not None and abs(implied - 1) <= _EVENT_TOLERANCE:
                no_step.append(check)
            else:
                magnitude.append(check)
        internally_ok = [c for c in disagreeing if c.internal_agrees]
        uncorroborated = no_step + magnitude
        recoverable = [
            c
            for c in uncorroborated
            if (offsets.get((c.event.series_id, c.event.bar_date)) or Decimal(9)) <= _EVENT_TOLERANCE
        ]
        print(f"\n  disagreeing                {len(disagreeing):,} of {len(referenced):,}", file=stream)
        print(
            f"    REFERENCE carries the raw step too, so the factor is corroborated  {len(ref_unadjusted):,}",
            file=stream,
        )
        print(f"    stamped factor ABSENT from the price series (implied step 1)       {len(no_step):,}", file=stream)
        print(f"    stamped factor of the WRONG MAGNITUDE                              {len(magnitude):,}", file=stream)
        print(
            f"    (across all {len(disagreeing):,}, the vendor's own adj_close still agrees with the stamped "
            f"factor on {len(internally_ok):,} — the limit of B1)",
            file=stream,
        )
        share = 0.0 if not referenced else 100.0 * len(uncorroborated) / len(referenced)
        print(
            f"    => UNCORROBORATED stamps: {len(uncorroborated):,} of {len(referenced):,} ({share:.2f}%)",
            file=stream,
        )
        print(
            f"       of those, the described step appears on a bar within ±{_OFFSET_BARS} "
            f"(date-convention, UPPER bound): {len(recoverable):,}",
            file=stream,
        )
        if magnitude:
            print("  wrong-magnitude sample — implied step against stamped factor", file=stream)
            for check in sorted(magnitude, key=lambda c: c.split_error or Decimal(0), reverse=True)[:6]:
                print(
                    f"    {check.event.vendor_symbol:<8} {check.event.bar_date}"
                    f"  stamped {check.event.factor.normalize():>20}"
                    f"  implied {float(check.implied_factor or 0):>10.4f}",
                    file=stream,
                )
        worst = sorted(disagreeing, key=lambda c: c.split_error or Decimal(0), reverse=True)[:8]
        print("  largest cross-vendor residuals", file=stream)
        for check in worst:
            internal_text = "—" if check.internal_error is None else f"{float(check.internal_error):.4f}"
            print(
                f"    {check.event.vendor_symbol:<8} {check.event.bar_date}"
                f"  factor {check.event.factor.normalize():>20}"
                f"  split_err {float(check.split_error or 0):.4f}"
                f"  raw_err {float(check.raw_error or 0):.4f}"
                f"  internal_err {internal_text}",
                file=stream,
            )
    return internal_pct, reference_pct


def _uncorrected_pct(checks: list[EventCheck]) -> float:
    """Agreement of the UNCORRECTED arm — the gate's comparison point."""
    referenced = [c for c in checks if c.raw_error is not None]
    if not referenced:
        return 0.0
    return 100.0 * sum(1 for c in referenced if c.raw_agrees) / len(referenced)


def _report_displacement(formations: list[Formation], *, stream: TextIO) -> float:
    expected = expected_months(WINDOW_START, WINDOW_END)
    print("\nPhase C — decile displacement, raw close vs split-only", file=stream)
    print(f"  window     {WINDOW_START} .. {WINDOW_END} (exclusive)", file=stream)
    print(f"  quarantine {RULE_SET_VERSION}", file=stream)
    print(f"  formations {len(formations):,} of {expected:,} calendar months", file=stream)
    if not formations:
        return 0.0
    decile_total = sum(f.decile_size for f in formations)
    displaced = sum(f.raw_only for f in formations)
    asymmetry = sum(abs(f.raw_only - f.split_only) for f in formations)
    pooled = 0.0 if decile_total <= 0 else 100.0 * displaced / decile_total
    print(
        f"\n  {'decade':<8} {'formations':>10} {'mean x-sec':>11} {'decile':>8} {'displaced':>10} {'pct':>7}",
        file=stream,
    )
    for decade in sorted({f.bar_date.year // 10 * 10 for f in formations}):
        rows = [f for f in formations if f.bar_date.year // 10 * 10 == decade]
        d_total = sum(f.decile_size for f in rows)
        d_moved = sum(f.raw_only for f in rows)
        mean_x = sum(f.cross_section for f in rows) / len(rows)
        pct = 0.0 if d_total <= 0 else 100.0 * d_moved / d_total
        print(
            f"  {decade:<8} {len(rows):>10,} {mean_x:>11,.0f} {d_total:>8,} {d_moved:>10,} {pct:>6.2f}%",
            file=stream,
        )
    worst = max(formations, key=lambda f: f.disagreement_pct)
    print(f"\n  pooled displacement  {pooled:.2f}%  ({displaced:,} of {decile_total:,})", file=stream)
    print(f"  worst formation      {worst.bar_date}  {worst.disagreement_pct:.2f}%", file=stream)
    print(f"  arm asymmetry (0)    {asymmetry:,}", file=stream)
    return pooled


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 1800000")
        symbols = _admitted(conn)
        events, missing, dividend_bars = read_split_events(symbols)
        _report_census(events, missing, dividend_bars, len(symbols), stream=sys.stdout)
        if not events:
            print("\nREFUSED: no split evidence found — candidate 1 is not constructible.", file=sys.stdout)
            return 2
        checks, unusable = check_events(conn, events)
        disagreeing = [c for c in checks if c.split_error is not None and not c.agrees]
        offsets = find_offset_steps(conn, disagreeing)
        census = unmatched_census(conn, checks)
        internal_pct, reference_pct = _report_events(checks, unusable, offsets, census, stream=sys.stdout)
        uncorrected_pct = _uncorrected_pct(checks)
        segments = back_adjust_scale(events)
        formations = measure_displacement(conn, _name_keys(conn), segments)
        pooled = _report_displacement(formations, stream=sys.stdout)

    if not [c for c in checks if c.split_error is not None]:
        print(
            "\nREFUSED: no split event produced a comparable bar pair on a second processing of the "
            "same upstream, so the field is unchecked. Candidate 1 is not established.",
            file=sys.stdout,
        )
        return 2
    # ⚠ Parameter-free gate: does APPLYING the stamp move the series toward the
    # second processing? An earlier version gated on the internal check, which
    # this script's own residual classification shows is circular — `adj_close`
    # moves with the stamp, so a factor that is wrong in both passes it (COO
    # 2024-02-20, stamped 16 against a 4:1).
    if reference_pct <= uncorrected_pct:
        print(
            f"\nFAIL: applying the stamped factor does NOT increase agreement with "
            f"{_REFERENCE_VENDOR} ({reference_pct:.2f}% corrected against {uncorrected_pct:.2f}% "
            f"uncorrected). Field {_SPLIT_FIELD} does not behave as a split ratio.",
            file=sys.stdout,
        )
        return 1
    print(
        f"\nPASS: applying the stamped factor moves agreement with {_REFERENCE_VENDOR} from "
        f"{uncorrected_pct:.2f}% to {reference_pct:.2f}%, so field {_SPLIT_FIELD} carries real "
        f"corporate-action information. Correcting the basis moves {pooled:.2f}% of the top decile.\n"
        f"⚠ NOT a correctness verdict. Both archives redistribute Yahoo "
        f"(`research_corpus_ingest.py`: 'ONE observation … agreement between them is circular'), the "
        f"internal check reproduces the stamp on {internal_pct:.2f}% of events and is circular by "
        "derivation, and no check here reaches a primary corporate-action record. The residual is the "
        "quality figure the follow-on spec must carry.",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
