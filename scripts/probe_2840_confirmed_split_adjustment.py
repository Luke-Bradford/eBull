"""#2840 — does the eToro candle endpoint BACK-ADJUST history across a confirmed basis event?

``app/services/bar_capture_certificate.py`` refuses every stored bar until this is settled:
``PROVIDER_REWRITE_TIMING_VERIFIED = False``, whose comment names the unblock as *"a confirmed
split inside a reachable window"*. ``91267518`` listed three routes to one and used none. This
probe takes the SEC route and measures the answer.

⚠ INFORMATIONAL BROKER READS ONLY. ``get_daily_candles`` is a candle fetch;
``app/security/unattended_guard.py`` refuses broker MUTATIONS from a linked worktree and
deliberately leaves informational calls reachable (#2645). No order, no position, no kill switch.
⚠ NOT WRITE-FREE: ``load_credentials`` commits a credential-access audit row per load (its own
docstring says so). The measurement queries are read-only; the credential path is not.

THE TEST
--------
For an event that re-denominated a share count by ``s``, a NOMINAL price series must step by
``1/s`` at the effective date — a forward 3:1 split (``s = 3``) divides price by three. So:

* a persistent level shift at ``1/s`` inside the event's bracket ⇒ the series is NOMINAL there;
* no shift at ``1/s`` ⇒ the pre-event bars already carry the new basis ⇒ BACK-ADJUSTED.

⚠⚠ THE PRICE SERIES MUST BE MEASURED, NEVER SELECTED ON. The first version of this register
required a price discontinuity and then corroborated it with the share restatement. That is
CIRCULAR: if the provider back-adjusts, a genuine event leaves NO discontinuity, so selecting on
one admits only the events the provider did not adjust and the measurement can only ever return
"nominal". It also returned 32 rows over 9 instruments that were ALL a single corrupt bar on
2025-12-09 (17 instruments that day at >5x; already caught by ``price_bar_quarantine``) matched
against restatement brackets that run about a YEAR wide. Recorded in the proposal doc and in
``docs/review-prevention-log.md``.

⚠ AND THE EXPECTED FACTOR IS ``1/s``, NOT "``1/s`` OR ``s``". Accepting the reciprocal too would
re-admit the same-direction cases the direction argument exists to exclude, and would score the
positive control below as a match when it is precisely not one.

SOURCE RULE — WHAT THE REGISTER IS, AND WHAT IT IS NOT
-----------------------------------------------------
``ASC 260-10-55-12`` requires retrospective restatement of **per-share** amounts for all periods
presented after a split or stock dividend. For the **share-count** disclosure this probe reads,
the governing interpretation is ``SEC SAB Topic 4.C``, which requires retroactive adjustment of
the capital-structure presentation for a change in capital structure effected without additional
consideration. Both are cited because the concept read here
(``CommonStockSharesOutstanding``) is a balance-sheet instant, not an EPS denominator.

⚠⚠ THE CONVERSE DOES NOT HOLD, and this register does not claim it does. "Splits require
restatement" does NOT give "a simple-ratio restatement is a split".
``docs/specs/ingest/2026-08-03-2231-split-adjustment.md`` keeps recapitalisations and filer
errors inside the same signal, ``ASC 805-40`` restates for reverse acquisitions by the exchange
ratio, and ``SAB Topic 4.D`` covers nominal issuances. So the register is a register of
**retroactive share-count RE-DENOMINATIONS** and is named that way throughout.

⚠ THAT IMPRECISION BIASES TOWARD THIS PROBE'S OWN CONCLUSION. A member that is an accounting
correction rather than a traded re-denomination has no price effect to adjust, so it lands in
``no_cliff`` for a reason that has nothing to do with the provider. The register-wide rate is
therefore an UPPER bound on back-adjustment, and the named events — whose corporate action is
independently documented — are the part that carries weight. Both are reported separately.

THE POSITIVE CONTROL, WHICH IS ALSO THE SHARPEST SINGLE RESULT
--------------------------------------------------------------
``HON`` re-denominated 1-for-2 on 2026-06-29 (Form 10-Q for period 2026-06-30: *"every two
shares ... were automatically combined into one share"*, *"All share and per share amounts have
been retrospectively adjusted"*), contingent on the Honeywell Aerospace spin-off the same day.
Expected nominal price factor is therefore ``x2``. The delivered series steps by **0.4925**
(a 10-session level shift; the bare adjacent-close ratio is 0.4919).

Two events, opposite directions, one session — and the split-off is the one that survives:

* Aerospace's own value is IN THE CORPUS. ``HONA`` (first bar 2026-06-24, when-issued) closed
  222.02 on 2026-06-29 on 316,952,725 shares per its 2026-08-05 filing => ~70.4bn, against
  ``HON``'s post-event ~72.3bn (316,900,000 x 228.21). Aerospace was ~49% of the whole, so the
  combined pre-event entity was ~142.7bn over 633,700,000 pre-split shares = **~225/share**.
* The delivered pre-event close is **463.98**, which is ~2.06x that. ⇒ the reverse split is
  ALREADY INSIDE the pre-event bars, and the surviving step is the spin-off distribution.

⇒ the provider back-adjusts the re-denomination and does NOT adjust the spin-off — and because
the detector SEES that residual step, ``HON`` proves the detector can find a cliff when one
exists. A register of pure ``no_cliff`` verdicts with no positive control would not.

⚠ The spin-off half is ``n = 1`` and is NOT generalised to a class.

WHAT A ``no_cliff`` VERDICT DOES NOT ESTABLISH
---------------------------------------------
* not that the provider REWROTE anything — an always-adjusted upstream and a historical rewrite
  are indistinguishable from one delivered snapshot;
* not WHEN an adjustment is applied relative to the effective session's open, which is the
  separate fact ``PROVIDER_REWRITE_TIMING_VERIFIED`` names;
* not that the member was a traded re-denomination at all (see the bias note above);
* not anything about INTRADAY. ``91267518``'s refusal stands — shared routing is not shared
  adjustment — and ``ThirtyMinutes`` reaches ~1 month, so no register member is inside its span.

Refs #2840. Refs #2437.
"""

from __future__ import annotations

import argparse
import math
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from scripts.probe_2840_intraday_adjustment_basis import is_split_scale, load_credentials

#: ⚠ NOT the price tolerance, and the difference is the point. ``is_split_scale``'s 1% default is
#: calibrated on the intraday-versus-daily close noise of a RATIO OF RATIOS. A share count is a
#: reported integer, so a genuine re-denomination lands on its rational within reporting rounding.
#: Reusing 1% here would import a price-noise floor into an arithmetic question and, at
#: ``limit = 30``, makes ambiguous rejections common (7:4 has three coprime matches within 1%).
SHARE_RATIO_TOLERANCE: Final = 0.002

#: The level-shift band for the PRICE side, where the noise is real.
PRICE_TOLERANCE: Final = 0.02

#: Sessions each side of a candidate date that form the two levels compared.
LEVEL_WINDOW: Final = 10

#: ⚠ SYMMETRIC, and stated as a log distance for exactly that reason. The first cut was
#: ``s > 1.5 OR s < 0.667``, which EXCLUDES an exact 3:2 (1.5 is not > 1.5) while ADMITTING its
#: reciprocal — a directional asymmetry that silently drops one of the commonest split shapes.
#: ``WRB.US`` survived that cut only because its ratio is 1.5000000019.
MIN_LOG_RATIO: Final = math.log(1.2)

_REGISTER_SQL = """
    SELECT i.symbol,
           a.instrument_id,
           a.concept,
           a.period_end,
           a.filed_date  AS old_filed,
           b.filed_date  AS new_filed,
           a.val         AS old_val,
           b.val         AS new_val
    FROM financial_facts_raw a
    JOIN financial_facts_raw b
      ON  b.instrument_id = a.instrument_id
      AND b.concept       = a.concept
      AND b.taxonomy      = a.taxonomy
      AND b.unit          = a.unit
      AND b.period_end   IS NOT DISTINCT FROM a.period_end
      AND b.period_start IS NOT DISTINCT FROM a.period_start
      AND b.filed_date    > a.filed_date
    JOIN instruments i ON i.instrument_id = a.instrument_id
    WHERE a.concept = 'CommonStockSharesOutstanding'
      AND a.val > 0 AND b.val > 0
      AND abs(ln(b.val / a.val)) >= %(min_log_ratio)s
      AND b.filed_date >= %(since)s
      AND i.is_tradable
"""

_CLOSES_SQL = """
    SELECT price_date, close
    FROM price_daily
    WHERE instrument_id = %(instrument_id)s
      AND close > 0 AND close <> 'NaN'::numeric
    ORDER BY price_date
"""

# Verdicts. ⚠ ``insufficient_bars`` and ``fetch_failed`` are SEPARATE from ``no_cliff`` on
# purpose: an event we could not look at must never be counted as an event with no cliff, which
# is the direction that inflates this probe's own conclusion.
CLIFF_AT_EXPECTED: Final = "cliff_at_expected_factor"
CLIFF_ELSEWHERE: Final = "cliff_at_other_factor"
NO_CLIFF: Final = "no_cliff"
INSUFFICIENT: Final = "insufficient_bars"
FETCH_FAILED: Final = "fetch_failed"


@dataclass(frozen=True)
class Redenomination:
    """One registered event: a share count restated for the SAME period at two filed dates."""

    symbol: str
    instrument_id: int
    share_ratio: float
    ratio_pq: tuple[int, int]
    old_filed: date
    new_filed: date
    period_end: date | None
    old_val: float
    new_val: float

    @property
    def expected_price_factor(self) -> float:
        """Shares multiply by ``s`` ⇒ a NOMINAL price multiplies by ``1/s``. Never also ``s``."""
        return 1.0 / self.share_ratio


@dataclass(frozen=True)
class Register:
    """The events, WITH the counts that show what selection threw away.

    ⚠ The rejected count is not decoration. "315 events" alone reads as a census; the pair count
    beside it shows how much of the raw signal a simple-ratio requirement discards, which is the
    number a reader needs to judge whether the register is representative.
    """

    events: list[Redenomination]
    raw_pairs: int
    rejected_not_simple_ratio: int
    empty_intersections: int


def intersect_bracket(current: Redenomination, event: Redenomination) -> Redenomination:
    """Latest lower bound, earliest upper bound — the #2231 treatment, and ORDER-INDEPENDENT.

    ⚠ The predecessor was a dominance test (``keep the new one only if it is wider on BOTH ends``),
    so two brackets each wider on a different side kept whichever arrived first. Extracted as its
    own function precisely so that property can be pinned by a test rather than asserted in a
    docstring.
    """
    return replace(
        current,
        old_filed=max(current.old_filed, event.old_filed),
        new_filed=min(current.new_filed, event.new_filed),
    )


def redenomination_register(
    conn: psycopg.Connection[Any],
    *,
    since: date,
    tolerance: float = SHARE_RATIO_TOLERANCE,
    limit: int = 30,
) -> Register:
    """Every tradable instrument with a simple-ratio retroactive share-count re-denomination.

    ONE ROW PER (instrument, ratio). An instrument re-files the same period many times, so the raw
    pair count over-represents chatty filers by an order of magnitude; collapsing on the ratio
    keeps one entry per distinct re-denomination while a second genuine event at a different ratio
    stays separate.

    ⚠⚠ THE BRACKETS ARE INTERSECTED, NOT WIDENED — and the first version did neither properly.
    It kept whichever pair "dominated" on BOTH ends, so two brackets each wider on a different side
    silently kept whichever arrived first (review WARNING). Chasing that found the real defect:
    widening is the wrong DIRECTION. ``docs/specs/ingest/2026-08-03-2231-split-adjustment.md``
    settles this treatment — *"an event that genuinely describes one split must contain that
    split's true effective date, so the brackets can be intersected"* — and measured the gain
    (median bracket 637d per instrument, 363d per consecutive pair, **273d intersected**).

    ⚠ Intersecting can EMPTY, which #2231 names as its known failure mode (130 empty intersections
    there) — a scale artefact, or two genuine events at the same ratio merged into one cluster.
    Those are counted and dropped rather than silently widened back, because an empty intersection
    means the cluster does not describe a single event and no bracket for it is honest.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_REGISTER_SQL, {"min_log_ratio": MIN_LOG_RATIO, "since": since})
        rows = cur.fetchall()

    rejected = 0
    clusters: dict[tuple[int, tuple[int, int]], Redenomination] = {}
    for row in rows:
        ratio = float(row["new_val"]) / float(row["old_val"])
        pq = is_split_scale(ratio, limit=limit, tolerance=tolerance)
        if pq is None:
            rejected += 1
            continue
        key = (int(row["instrument_id"]), pq)
        event = Redenomination(
            symbol=row["symbol"],
            instrument_id=int(row["instrument_id"]),
            share_ratio=ratio,
            ratio_pq=pq,
            old_filed=row["old_filed"],
            new_filed=row["new_filed"],
            period_end=row["period_end"],
            old_val=float(row["old_val"]),
            new_val=float(row["new_val"]),
        )
        current = clusters.get(key)
        clusters[key] = event if current is None else intersect_bracket(current, event)

    kept = [event for event in clusters.values() if event.old_filed <= event.new_filed]
    return Register(
        events=sorted(kept, key=lambda e: (e.symbol, e.ratio_pq)),
        raw_pairs=len(rows),
        rejected_not_simple_ratio=rejected,
        empty_intersections=len(clusters) - len(kept),
    )


def _geometric_mean(values: Sequence[float]) -> float:
    return math.exp(sum(math.log(v) for v in values) / len(values))


def cliff_profile(
    closes: Mapping[date, float],
    *,
    expected_factor: float,
    bracket: tuple[date, date],
    window: int = LEVEL_WINDOW,
    tolerance: float = PRICE_TOLERANCE,
) -> dict[str, Any]:
    """The largest persistent level shift inside ``bracket``, and whether it is ``expected_factor``.

    A level shift, not an adjacent jump. ⚠ An adjacent-close ratio cannot tell a re-basing from a
    single corrupt print — that is exactly what the falsified v1 register matched on — so both
    sides are GEOMETRIC MEANS over ``window`` sessions, which a one-bar outlier moves by at most
    ``outlier**(1/window)``.

    ⚠ The threshold is DERIVED from the event, never a flat percentage. A 3:2 split moves price by
    33.3%, so a fixed "35% jump" screen misses one of the commonest shapes while a 1-for-30 screen
    at the same number is absurdly loose.
    """
    ordered = sorted(closes)
    if len(ordered) < 2 * window + 1:
        return {"verdict": INSUFFICIENT, "reason": f"{len(ordered)} bars, need {2 * window + 1}", "bars": len(ordered)}

    lo, hi = bracket
    # ⚠⚠ THE WHOLE BRACKET MUST BE COVERED, WITH A FULL WINDOW OUTSIDE EACH END — Codex
    # checkpoint 2, and it is the finding that most inflates this probe's own conclusion.
    # The bracket bounds where the event MAY be. If the series begins after ``lo``, the event may
    # sit before the first bar, the remaining sessions are flat, and the verdict comes back
    # ``no_cliff`` for a window in which the transition was never observable. Partial coverage is
    # therefore ``insufficient_bars``, not evidence. A 1000-bar fetch against a 13-year bracket is
    # the concrete case.
    first, last = ordered[0], ordered[-1]
    if first > lo or last < hi:
        return {
            "verdict": INSUFFICIENT,
            "reason": f"series {first}..{last} does not cover bracket {lo}..{hi}",
            "bars": len(ordered),
        }
    # The candidate must also have a FULL window on each side, and those windows may extend outside
    # the bracket — the bracket bounds where the EVENT can be, not where the evidence may come
    # from. Bounding the windows too would silently shorten them at the edges.
    candidates = [
        index for index, day in enumerate(ordered) if lo <= day <= hi and window <= index <= len(ordered) - window - 1
    ]
    if not candidates:
        return {
            "verdict": INSUFFICIENT,
            "reason": f"no session in {lo}..{hi} with {window} bars each side",
            "bars": len(ordered),
        }

    shifts: list[tuple[date, float]] = []
    for index in candidates:
        before = _geometric_mean([closes[d] for d in ordered[index - window : index]])
        after = _geometric_mean([closes[d] for d in ordered[index : index + window]])
        shifts.append((ordered[index], after / before))

    # The shift furthest from 1.0 in log space, which is scale-symmetric: a x3 and a /3 are equally
    # large, where |shift - 1| would rank x3 above /3.
    at, largest = max(shifts, key=lambda item: abs(math.log(item[1])))
    # ⚠ DIAGNOSTIC ONLY — it is NOT what decides the verdict, and reading it that way was a bug.
    near_expected = [(d, s) for d, s in shifts if abs(s / expected_factor - 1.0) <= tolerance]
    profile: dict[str, Any] = {
        "bars": len(ordered),
        "candidates": len(candidates),
        "largest_shift": largest,
        "largest_shift_at": at,
        "expected_factor": expected_factor,
        "near_expected": [{"at": d, "shift": s} for d, s in near_expected],
    }
    # ⚠⚠ THE VERDICT READS THE LARGEST SHIFT, NOT "ANY WINDOW THAT MATCHED" — Codex checkpoint 2.
    # The windows OVERLAP, so a genuine cliff is seen partially by the ``window`` candidates either
    # side of it and manufactures a continuum of intermediate factors on the way. On a real x10
    # step, some intermediate window reads x1.995 — so "any match" scored a x10 event as a NOMINAL
    # DELIVERY at expected factor 2. Over year-wide brackets that turns any large unrelated move
    # into a false match at whatever factor its shoulder happens to cross.
    #
    # ⚠ The consequence of reading the largest instead: a genuine cliff that is NOT the biggest
    # move in its bracket is reported ``cliff_at_other_factor`` rather than ``cliff_at_expected``.
    # That is the safe direction — it withholds the event from BOTH tallies instead of counting it
    # as evidence of back-adjustment.
    #
    # The "any cliff at all" bar is the event's own size, not a constant. A flat bar (this module
    # first used ``MIN_LOG_RATIO``) makes ``cliff_at_other_factor`` swallow the register: a
    # sub-dollar small-cap routinely moves its 10-session level by 20-30%, so 255 of 285 events
    # landed there and the verdict carried no information.
    if abs(largest / expected_factor - 1.0) <= tolerance:
        profile["verdict"] = CLIFF_AT_EXPECTED
    elif abs(math.log(largest)) >= abs(math.log(expected_factor)):
        profile["verdict"] = CLIFF_ELSEWHERE
    else:
        profile["verdict"] = NO_CLIFF
    return profile


def _report(
    results: Sequence[Mapping[str, Any]],
    *,
    source: str,
    register_size: int,
    built: Register,
) -> str:
    counts: dict[str, int] = {}
    for result in results:
        counts[result["verdict"]] = counts.get(result["verdict"], 0) + 1

    out: list[str] = [
        f"# #2840 — provider adjustment across confirmed re-denominations ({source} series)",
        "",
        f"register: {built.raw_pairs} raw restatement pairs -> {len(built.events)} events "
        f"after collapsing on (instrument, ratio); {built.rejected_not_simple_ratio} pairs "
        f"rejected as not a simple ratio within {SHARE_RATIO_TOLERANCE:.3%}, "
        f"{built.empty_intersections} clusters dropped on an EMPTY bracket intersection",
        f"measured this run: {register_size}",
        "",
        "verdict distribution:",
    ]
    for verdict in (CLIFF_AT_EXPECTED, CLIFF_ELSEWHERE, NO_CLIFF, INSUFFICIENT, FETCH_FAILED):
        out.append(f"  {verdict:26} {counts.get(verdict, 0):>5}")
    measured = counts.get(CLIFF_AT_EXPECTED, 0) + counts.get(CLIFF_ELSEWHERE, 0) + counts.get(NO_CLIFF, 0)
    out.append("")
    out.append(
        f"  ⚠ {measured} of {register_size} events were actually MEASURABLE; "
        f"{register_size - measured} were not looked at and are NOT counted as 'no cliff'."
    )
    if measured:
        out.append(
            f"  nominal-delivery rate = {counts.get(CLIFF_AT_EXPECTED, 0)}/{measured} "
            "(a cliff at the event's own factor)"
        )

    # ⚠⚠ STRATIFIED, BECAUSE THE POOLED RATE IS NOT THE ANSWER AND A CUT WOULD BE INVENTED.
    # A re-denomination of 5:4 predicts a price factor inside the level noise of a volatile
    # small-cap, so a coincidental match there is expected and says nothing about the provider;
    # a 20:1 predicts a factor nothing else in an equity series produces. Rather than pick a
    # minimum ratio (which would be a threshold with no published basis), the bands are reported
    # and the reader can see where the signal actually lives. The band edges are the arithmetic
    # of the ratios themselves — 2:1 is the narrowest ratio conventionally effected as a split.
    out.append("")
    out.append("by expected-price-factor magnitude (the register's own arithmetic, not a chosen cut):")
    bands: tuple[tuple[str, float, float], ...] = (
        ("x>=4 or <=1/4", 4.0, math.inf),
        ("x2 .. x4", 2.0, 4.0),
        ("x1.5 .. x2", 1.5, 2.0),
        ("under x1.5", 1.0, 1.5),
    )
    for label, low, high in bands:
        members = [
            r
            for r in results
            if r["verdict"] in (CLIFF_AT_EXPECTED, CLIFF_ELSEWHERE, NO_CLIFF)
            and low <= max(r["expected_factor"], 1.0 / r["expected_factor"]) < high
        ]
        if not members:
            continue
        hits = sum(1 for r in members if r["verdict"] == CLIFF_AT_EXPECTED)
        out.append(f"  {label:16} {hits:>4} of {len(members):>4} delivered NOMINALLY (a cliff at the event's factor)")
    out.append("")
    out.append("per event:")
    for result in sorted(results, key=lambda r: (r["verdict"], r["symbol"])):
        pq = result["ratio_pq"]
        line = (
            f"  {result['symbol']:10} {pq[0]}:{pq[1]:<3} expect x{result['expected_factor']:.4f}  "
            f"{result['verdict']:26} bracket {result['old_filed']}..{result['new_filed']}"
        )
        if result.get("largest_shift") is not None:
            line += f"  largest shift x{result['largest_shift']:.4f} at {result['largest_shift_at']}"
        if result.get("reason"):
            line += f"  [{result['reason']}]"
        if result.get("live_mismatches") is not None:
            line += f"  live!=stored {result['live_mismatches']}/{result['live_overlap']}"
        out.append(line)
    out.append("")
    out.append(
        "⚠ A `no_cliff` member may be an accounting correction with no price effect to adjust, so "
        "the rate above is an UPPER bound on back-adjustment. The probe's weight rests on the "
        "named events whose corporate action is independently documented — see the module "
        "docstring's positive control."
    )
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#2840 confirmed re-denomination adjustment probe")
    parser.add_argument("--since", default="2025-09-01", help="earliest restating filed_date")
    parser.add_argument(
        "--live",
        action="store_true",
        help="fetch the DELIVERED series per event instead of reading price_daily, and assert live == stored",
    )
    parser.add_argument("--count", type=int, default=1000, help="bars per live request (provider cap is 1000)")
    parser.add_argument(
        "--max-instruments",
        type=int,
        default=0,
        help="0 = the whole register. A non-zero cap is PRINTED with what it dropped, never silent.",
    )
    args = parser.parse_args(argv)
    since = date.fromisoformat(args.since)

    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        built = redenomination_register(conn, since=since)
        register = built.events
        dropped = 0
        if args.max_instruments and len(register) > args.max_instruments:
            dropped = len(register) - args.max_instruments
            register = register[: args.max_instruments]
        with conn.cursor(row_factory=dict_row) as cur:
            stored: dict[int, dict[date, float]] = {}
            for event in register:
                if event.instrument_id in stored:
                    continue
                cur.execute(_CLOSES_SQL, {"instrument_id": event.instrument_id})
                stored[event.instrument_id] = {row["price_date"]: float(row["close"]) for row in cur.fetchall()}

    if dropped:
        print(f"⚠ --max-instruments dropped {dropped} register events from this run")

    results: list[dict[str, Any]] = []
    # ⚠ ExitStack, not a bare ``provider.close()``. ``EtoroMarketDataProvider`` exposes
    # ``__enter__``/``__exit__`` and NO ``close`` — the first draft called one that does not
    # exist, in a ``finally`` that only runs on the live path, so ruff and the stored-arm run
    # both stayed green and pyright was the gate that caught it.
    with ExitStack() as stack:
        provider = None
        if args.live:
            creds = load_credentials()
            provider = stack.enter_context(
                EtoroMarketDataProvider(api_key=creds[0], user_key=creds[1], env=settings.etoro_env)
            )

        # ⚠ THE FAILURE REASON IS CACHED WITH THE FAILURE, not written once at the call site.
        # An instrument can carry several register events; stamping the reason only on the event
        # that happened to trigger the fetch left every later one reporting ``fetch_failed`` with
        # no explanation, which reads as an unexplained gap in the population (review NITPICK).
        live_cache: dict[int, tuple[dict[date, float] | None, str | None]] = {}
        for event in register:
            entry: dict[str, Any] = {
                "symbol": event.symbol,
                "ratio_pq": event.ratio_pq,
                "expected_factor": event.expected_price_factor,
                "old_filed": event.old_filed,
                "new_filed": event.new_filed,
            }
            series = stored[event.instrument_id]
            if provider is not None:
                if event.instrument_id not in live_cache:
                    try:
                        bars = provider.get_daily_candles(event.instrument_id, args.count)
                        live_cache[event.instrument_id] = (
                            {
                                bar.price_date: float(bar.close)
                                for bar in bars
                                if bar.close is not None and float(bar.close) > 0
                            },
                            None,
                        )
                    except Exception as exc:  # noqa: BLE001 - one failure must not sink the probe
                        live_cache[event.instrument_id] = (None, f"{type(exc).__name__}: {exc}")
                fetched, failure = live_cache[event.instrument_id]
                if fetched is None:
                    entry["verdict"] = FETCH_FAILED
                    entry["reason"] = failure
                    results.append(entry)
                    continue
                overlap = sorted(set(fetched) & set(series))
                entry["live_overlap"] = len(overlap)
                entry["live_mismatches"] = sum(1 for d in overlap if abs(fetched[d] / series[d] - 1.0) > 1e-6)
                series = fetched
            entry.update(
                cliff_profile(
                    series,
                    expected_factor=event.expected_price_factor,
                    bracket=(event.old_filed, event.new_filed),
                )
            )
            results.append(entry)

    print(
        _report(
            results,
            source="live" if args.live else "stored price_daily",
            register_size=len(register),
            built=built,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())


__all__ = [
    "CLIFF_AT_EXPECTED",
    "CLIFF_ELSEWHERE",
    "INSUFFICIENT",
    "MIN_LOG_RATIO",
    "NO_CLIFF",
    "PRICE_TOLERANCE",
    "SHARE_RATIO_TOLERANCE",
    "Redenomination",
    "cliff_profile",
    "intersect_bracket",
    "main",
    "redenomination_register",
]
