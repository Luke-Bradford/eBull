"""#2840 — does ``get_intraday_candles`` BACK-ADJUST its history the way the daily path does?

The premise the whole composed-series nominality argument rests on, and it was untested.
``scripts/census_2840_forward_daily_provenance.py`` says so in terms: ``market_data.py:751``
records back-adjustment for the DAILY provider path and *"says nothing about
``get_intraday_candles``, about every interval, or about every instrument variant"*. This
probe answers it.

⚠ INFORMATIONAL BROKER READS ONLY. ``get_intraday_candles`` is a candle fetch;
``app/security/unattended_guard.py`` refuses BROKER MUTATIONS from a linked worktree and
deliberately leaves informational calls reachable (#2645, and #2644's preflight decode is the
precedent). No order, no position, no kill switch.

⚠⚠ THIS SCRIPT IS NOT WRITE-FREE, AND AN EARLIER HEADER CLAIMED IT WAS.
``scheduler._load_etoro_credentials`` states in its own docstring that *"Each credential load
is committed individually so audit rows are durable"* — so running this probe COMMITS
credential-access audit rows. Its own measurement queries are read-only under
``REPEATABLE READ READ ONLY``; the credential path is not. And the environment follows
``settings.etoro_env`` — calling it "demo credentials" was wrong too.

THE TEST, AND WHY IT NEEDS NO SESSION-CLOSE PROXY
------------------------------------------------
``price_daily`` is back-adjusted at fetch and an ``adjustment_heal`` triggers an in-run
full-history refetch (``market_data.py:751``, ``sql/387``). ⚠ "Whole history" is the heal's
INTENT, not a guarantee: production refetches ``lookback_days``, may receive fewer bars, and
validates no uniformity — rows older than the repair window can stay on the old basis. So a
reference mismatch can be stale daily data rather than intraday policy, and the stored series is
read BEFORE the live fetch with no vintage reconciliation. Per session date:

    ratio = intraday_close(date) / price_daily_close(date)

* ratio ≈ 1.0 across the whole window  ⇒ the provider back-adjusts INTRADAY too, so a bar
  captured long after it completed carries the adjustment that stood at capture, and the
  census's ``after_*_opens`` bars are NOT nominal.
* ratio steps from some k to 1.0 at a date ⇒ the provider serves intraday on the basis that
  stood AT THE BAR, i.e. nominal, and the census's arithmetic-only nominality claim was
  unnecessarily narrow.

A split ratio (2x, 10x, 1/10) dwarfs any intraday-vs-daily alignment noise, so the readout
does not need the intraday bar to be the session's last one. The ratio's LEVEL is noisy; its
STEP is not.

THE ANSWER IS STRUCTURAL, AND THE EMPIRICAL RUN ONLY CORROBORATES IT
-------------------------------------------------------------------
⚠⚠ THE TWO FETCHES ARE THE SAME ENDPOINT, WITH THE INTERVAL AS A PATH SLOT:

* daily    — ``/api/v1/market-data/instruments/{id}/history/candles/asc/OneDay/{lookback}``
* intraday — ``/api/v1/market-data/instruments/{id}/history/candles/asc/{interval}/{count}``

(``app/providers/implementations/etoro.py:305-341``; the intraday docstring says so itself —
*"Same URL family as ``get_daily_candles`` but the interval slot is variable ... Raw response
shape mirrors the daily endpoint exactly"*.)

⚠⚠ SHARED ROUTING IS NOT SHARED ADJUSTMENT, AND AN EARLIER VERSION OF THIS HEADER TREATED IT
AS IF IT WERE. One handler can select a different store, cache or adjustment rule per interval;
"there is no separate endpoint" is no evidence against that. The inference is only that
intraday would have to be adjusted PER INTERVAL to differ from daily — not that it does not.

⚠ AND THE DAILY-SIDE EVIDENCE IS WEAKER THAN A CONTRACT. ``market_data.py:751`` is a CODE
COMMENT explaining a repair heuristic, not a provider contract: the surrounding function
refetches when an overlap close mismatches by >= 1.2x, and it establishes neither that
mismatch's economic cause nor endpoint-wide semantics. eToro's own docs for the candle endpoint
and for historical closing prices (fetched 2026-09-20) are **silent** on splits, corporate
actions and adjustment — so those two pages specify no rule; that is narrower than "no
published rule exists".

⇒ WORKING CONCLUSION, in the only form the evidence supports: **nominality is UNVERIFIED for a
backfilled bar, so exclude it from an absolute-price gate.** That is not the same claim as
"these bars are non-nominal" — a provider that back-adjusts still returns nominal bars for any
window in which no adjustment intervened. The operative effect is the same admission policy,
and the difference matters because the stronger claim would license reversing an assumed factor,
which could MANUFACTURE >= $100 eligibility.

⚠⚠ WHAT THIS CANNOT ESTABLISH, stated because the conclusion is tempting.
* The structural argument is an inference about one endpoint's uniformity, NOT a demonstration.
  Only a confirmed split inside the reachable span would demonstrate it, and none is available
  (see below).
* ``adjustment_heal`` is *"A BRANCH, NOT AN ECONOMIC CAUSE ... an UPPER BOUND on attribution"*
  (``sql/387``). A healed instrument is a candidate for a basis event, not a confirmed split.
* A flat ratio on a name whose event fell OUTSIDE the tier's reach proves nothing. The probe
  reports each name's reachable span so that case is visible rather than counted as evidence.
* One interval on a few names is not the whole provider surface. Intervals are probed
  separately for exactly that reason.

Refs #2840. Refs #2437.
"""

from __future__ import annotations

import argparse
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from statistics import median
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.market_data import IntradayInterval

_NY = ZoneInfo("America/New_York")

#: Instruments whose stored daily history was REWRITTEN recently — i.e. basis-event
#: candidates — plus a control with no heal. Resolved from the DB, never hardcoded ids.
_HEAL_CANDIDATES = """
    SELECT r.instrument_id, i.symbol, count(*) AS revisions,
           min(r.price_date) AS first_date, max(r.price_date) AS last_date,
           max(r.revised_at) AS last_revised_at
    FROM price_daily_revision r
    JOIN instruments i USING (instrument_id)
    WHERE r.cause = 'adjustment_heal'
      AND r.revised_at >= now() - interval '120 days'
    GROUP BY 1, 2
    ORDER BY last_revised_at DESC
    LIMIT %(limit)s
"""

#: ⚠ The control's DEFINING property is asserted, not assumed. An earlier version selected on
#: symbol + tradability alone and then labelled the row "no recent heal" — it could have picked a
#: healed instrument and the label would still have been printed.
_CONTROL = """
    SELECT i.instrument_id, i.symbol,
           (SELECT count(*) FROM price_daily_revision r
             WHERE r.instrument_id = i.instrument_id
               AND r.cause = 'adjustment_heal'
               AND r.revised_at >= now() - interval '120 days') AS recent_heals
    FROM instruments i
    WHERE i.symbol = %(symbol)s
      AND i.is_tradable
"""

_DAILY_CLOSES = """
    SELECT price_date, close
    FROM price_daily
    WHERE instrument_id = %(instrument_id)s
      AND close IS NOT NULL
      AND close <> 'NaN'::numeric
      AND price_date >= %(since)s
    ORDER BY price_date
"""


def session_ratios(
    bars: Sequence[Any],
    daily_closes: Mapping[date, float],
    *,
    estimator: str = "last",
) -> list[tuple[date, float, int]]:
    """``(session_date, intraday/daily close ratio, bars in that session)``.

    ⚠⚠ THE ESTIMATOR MATTERS MORE THAN THE THRESHOLD, AND THE FIRST VERSION OF THIS PROBE GOT
    IT WRONG. It used the MEDIAN over the session's bars, reasoning that a split ratio "dwarfs
    alignment noise". It does not dwarf THIS noise: a session's median bar sits somewhere inside
    the day's range, so on a volatile small-cap the ratio wandered 0.86 .. 1.21 — wider than a
    1.15x reverse split would move it. **The control caught it**: AAPL, which has no
    ``adjustment_heal`` at all, tripped the same "step found" verdict as every candidate.

    ``last`` takes the session's final bar, whose close is the session close up to the
    extended-hours difference between the two series — measured at a median ~5 bps and a
    worst ~14 bps against the RTH variant elsewhere in this ticket. That is the estimator a
    2% threshold is meaningful against. ``median`` is kept so the noise difference between the
    two is visible in one run rather than argued.
    """
    by_day: dict[date, list[tuple[datetime, float]]] = defaultdict(list)
    for bar in bars:
        day = bar.timestamp.astimezone(_NY).date()
        reference = daily_closes.get(day)
        # ⚠ BOTH SIDES GUARDED IN PYTHON, not only in the SQL. The `close <> 'NaN'::numeric`
        # predicate covers the stored reference; a non-finite or non-positive value arriving
        # from the PROVIDER would otherwise propagate a `nan` ratio, and a single leading `nan`
        # makes every later comparison in `detect_step` return "no step" — the exact direction
        # that hides a real 2x jump.
        if reference is None or not math.isfinite(reference) or reference <= 0:
            continue
        close = float(bar.close)
        if not math.isfinite(close) or close <= 0:
            continue
        by_day[day].append((bar.timestamp, close / reference))
    out: list[tuple[date, float, int]] = []
    for day, values in sorted(by_day.items()):
        if estimator == "last":
            ratio = max(values, key=lambda item: item[0])[1]
        elif estimator == "median":
            ratio = median(value for _, value in values)
        else:
            raise ValueError(f"unknown estimator {estimator!r}; use 'last' or 'median'")
        out.append((day, ratio, len(values)))
    return out


#: Minutes per documented interval. ⚠ The names are the endpoint's own enum
#: (``docs/etoro-api-reference.md:409``); the probe accepts whatever is passed, so an interval
#: absent here falls back to the widest reach rather than silently under-fetching the reference.
_INTERVAL_MINUTES: Final[dict[str, int]] = {
    "OneMinute": 1,
    "FiveMinutes": 5,
    "TenMinutes": 10,
    "FifteenMinutes": 15,
    "ThirtyMinutes": 30,
    "OneHour": 60,
    "FourHours": 240,
    "OneDay": 390,
    "OneWeek": 1_950,
}


def reference_window_start(interval: str, count: int, *, slack: float = 2.0, today: date | None = None) -> date:
    """Lower bound for the daily reference fetch, DERIVED from the interval's reach.

    ⚠ REPLACES A HARDCODED ``date(2025, 1, 1)``, which had no stated rationale and would
    silently under-fetch the reference for a long-reach interval. The endpoint is count-based
    with no date anchor, so the furthest a request can reach back is ``count`` bars of
    ``interval``; ``slack`` covers the fact that only RTH bars are counted while calendar time
    runs continuously. ``OneDay``/``OneWeek`` are expressed in RTH minutes so one formula covers
    every documented interval.
    """
    minutes = _INTERVAL_MINUTES.get(interval, max(_INTERVAL_MINUTES.values()))
    sessions = max(1, int(count * minutes / _INTERVAL_MINUTES["OneDay"]))
    days = int(sessions * slack * 7 / 5) + 30
    return (today or datetime.now(UTC).date()) - timedelta(days=days)


def is_split_scale(factor: float, *, limit: int = 20, tolerance: float = 0.01) -> tuple[int, int] | None:
    """The ``p:q`` (both <= ``limit``) the factor matches UNAMBIGUOUSLY, or ``None``.

    ⚠⚠ THIS FUNCTION HAS BEEN WRONG TWICE IN OPPOSITE DIRECTIONS, so both failures are recorded.

    1. **A whitelist of round factors** (2, 3, 4, 5, 10 and reciprocals) classifies a clean
       **3:2** split and a **15:1** reverse split as ordinary disagreement. Real ratios, missed.
    2. **Tightening to 20 bps to fix the resulting density** broke detection instead. The factor
       is a RATIO OF RATIOS — it carries the intraday-versus-daily close mismatch on both sides
       of the event — and the control's measured adjacent noise is ~1%. A genuine 2:1 transition
       distorted by 0.5% is 0.4975, which is 50 bps from 1:2 and returns ``None``.

    The tolerance therefore sits at the measured noise floor (1%), and **ambiguity is what
    rejects a match** rather than tightness. A match counts only if exactly ONE coprime ``p:q``
    under ``limit`` lies within tolerance:

    * ``2.0`` — only ``2:1`` is within 1% (the nearest other coprime, ``19:10``, is 5% away) ⇒ a
      match, and it survives realistic distortion.
    * ``19/17`` — both ``19:17`` and ``9:8`` are within 1% ⇒ ambiguous ⇒ ``None``, which is the
      right answer for a factor that is not a split.

    ⚠ A MATCH IS STILL NOT A VERDICT. Plausibility is the caller's and persistence is
    ``detect_step``'s; this only says the factor is cleanly a simple ratio and which one.
    """
    matches: list[tuple[int, int]] = []
    for q in range(1, limit + 1):
        for p in range(1, limit + 1):
            if math.gcd(p, q) != 1:
                continue
            if abs(factor * q / p - 1.0) <= tolerance:
                matches.append((p, q))
    if len(matches) != 1:
        return None
    return matches[0]


def detect_step(
    ratios: Sequence[tuple[date, float, int]],
    *,
    tolerance: float = 0.02,
    persistence: int = 2,
) -> dict[str, Any]:
    """Every adjacent-session jump above ``tolerance``, classified — not just the largest.

    ⚠ CLASSIFYING ONLY THE LARGEST JUMP HID REAL EVENTS. On ``[2.0, 1.0, 0.13]`` the genuine
    0.5x transition is discarded in favour of the larger 0.13x one, so "no split-scale step
    anywhere" did not follow from the algorithm that produced it.

    ⚠ ``persistence`` is why a lone spike is not a basis event. A re-basing is a LEVEL SHIFT:
    the ratio moves and STAYS moved. ``[1, 2, 1]`` is a bad print, and an earlier version gave
    it the categorical nominal verdict.
    """
    if len(ratios) < 2:
        return {"sessions": len(ratios), "jumps": [], "reason": "fewer than two paired sessions"}
    jumps: list[dict[str, Any]] = []
    # ⚠ Every adjacent size, not only the ones above tolerance. Maxing over the FILTERED list
    # reported "largest adjacent jump 0.000000" for the control, which reads as a series with no
    # variation at all — the opposite of the noise floor the control exists to establish.
    all_sizes: list[float] = []
    for index in range(1, len(ratios)):
        earlier, later = ratios[index - 1], ratios[index]
        if earlier[1] == 0:
            continue
        factor = later[1] / earlier[1]
        size = abs(factor - 1.0)
        all_sizes.append(size)
        if size <= tolerance:
            continue
        # ⚠ STABLE ON BOTH SIDES, not just after. Looking only forward makes the RETURN EDGE of a
        # transient spike pass: on ``[1, 2, 1, 1]`` the 2->1 edge sees the tail ``[1, 1]``, holds,
        # matches 1:2, and the bad print the persistence check exists to reject becomes a positive
        # result. A re-basing has a settled level on each side of one transition.
        tail = [value for _, value, _ in ratios[index : index + persistence]]
        head = [value for _, value, _ in ratios[max(0, index - persistence) : index]]
        held_after = len(tail) >= persistence and all(abs(value / later[1] - 1.0) <= tolerance for value in tail)
        held_before = len(head) >= persistence and all(abs(value / earlier[1] - 1.0) <= tolerance for value in head)
        held = held_after and held_before
        jumps.append(
            {
                "at": later[0],
                "size": size,
                "factor": factor,
                "from_ratio": earlier[1],
                "to_ratio": later[1],
                "split_scale": is_split_scale(factor),
                "persisted": held,
                "held_before": held_before,
                "held_after": held_after,
                "persistence_window": len(tail),
            }
        )
    return {
        "sessions": len(ratios),
        "jumps": jumps,
        "largest_jump": max(all_sizes, default=0.0),
        "largest_jump_above_tolerance": max((jump["size"] for jump in jumps), default=0.0),
        "ratio_min": min(value for _, value, _ in ratios),
        "ratio_max": max(value for _, value, _ in ratios),
        "paired_first": ratios[0][0],
        "paired_last": ratios[-1][0],
        "max_abs_deviation": max(abs(value - 1.0) for _, value, _ in ratios),
    }


def _report(results: Sequence[Mapping[str, Any]], *, interval: IntradayInterval) -> str:
    out: list[str] = [f"# #2840 intraday adjustment basis — interval {interval}", ""]
    for result in results:
        out.append(f"## {result['symbol']} (instrument {result['instrument_id']}) — {result['role']}")
        if result.get("error"):
            out.append(f"  FETCH FAILED: {result['error']}")
            out.append("")
            continue
        out.append(f"  bars fetched {result['bars']}, fetched span {result['first_bar']} .. {result['last_bar']}")
        if result.get("last_revised_at"):
            out.append(f"  adjustment_heal last detected {result['last_revised_at']} (a CANDIDATE, not a split)")
        step = result["step"]
        if step.get("reason"):
            out.append(f"  {step['reason']}")
            out.append("")
            continue
        out.append(
            f"  PAIRED span {step['paired_first']} .. {step['paired_last']} over {step['sessions']} sessions "
            f"— ⚠ adjacent PAIRED sessions can be many calendar sessions apart; the fetched span above is "
            "not the measured one"
        )
        out.append(
            f"  ratio range {step['ratio_min']:.8f} .. {step['ratio_max']:.8f}; max |ratio - 1| = "
            f"{step['max_abs_deviation']:.2e}; largest adjacent jump {step['largest_jump']:.6f}"
        )
        noisy = result.get("step_median_estimator") or {}
        if noisy.get("largest_jump") is not None:
            out.append(
                f"  median-bar estimator for contrast: range {noisy['ratio_min']:.8f} .. "
                f"{noisy['ratio_max']:.8f}, largest adjacent jump {noisy['largest_jump']:.6f} — the estimator "
                "whose noise exceeded the threshold on the CONTROL, printed rather than argued"
            )
        if not step["jumps"]:
            out.append("  no adjacent jump above tolerance")
        for jump in step["jumps"]:
            ratio = jump["split_scale"]
            label = f"{ratio[0]}:{ratio[1]}" if ratio else "not a simple ratio"
            out.append(
                f"    jump at {jump['at']}: factor {jump['factor']:.6f} ({label}), "
                f"{'PERSISTED' if jump['persisted'] else 'did NOT persist'} "
                f"over the next {jump['persistence_window']} paired session(s)"
            )
        basis_events = [jump for jump in step["jumps"] if jump["split_scale"] is not None and jump["persisted"]]
        if basis_events:
            out.append(
                "  VERDICT: a persistent simple-ratio level shift — the fetched pre-event bars did NOT follow "
                "the daily series onto its new basis. Consistent with intraday being served on the basis that "
                "stood AT THE BAR. ⚠ Still not a proof: partial adjustment and a stale daily reference produce "
                "the same shape."
            )
        elif step["jumps"]:
            out.append(
                "  VERDICT: jumps above tolerance, none a persistent simple-ratio shift. That is ordinary "
                "series disagreement — do not read it either way."
            )
        else:
            out.append(
                "  VERDICT: no step — consistent with (a) the provider adjusting intraday in lockstep, (b) this "
                "heal not being a basis change, or (c) no event inside the PAIRED span. NOT separable here."
            )
        out.append("")
    out.append(
        "⚠ A flat ratio is evidence only when a CONFIRMED basis event falls inside the PAIRED span — and "
        "`adjustment_heal` dates the DETECTION, not the event, and per `sql/387` does not mean 'split'. The "
        "control establishes the estimator's noise floor, not the hypothesis."
    )
    out.append(
        "⚠ Candidate selection is BIASED by construction: it takes the latest `adjustment_heal` branches, which "
        "misses events ingested via initial/forced backfill and any adjustment below the repair's >= 1.2x "
        "trigger. Routes to a CONFIRMED split exist and are not used here — see the appendix."
    )
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", default="ThirtyMinutes", help="eToro IntradayInterval name")
    parser.add_argument("--count", type=int, default=1000, help="bars per request (provider cap is 1000)")
    parser.add_argument("--control", default="AAPL", help="a symbol with no recent adjustment_heal")
    parser.add_argument("--candidates", type=int, default=3, help="how many healed instruments to probe")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.02,
        # ⚠ Meaningful ONLY against the `last`-bar estimator. Against the median estimator the
        # control itself exceeds it, which is how the first run was caught.
        help="adjacent-session ratio jump counted as a step (default 0.02)",
    )
    args = parser.parse_args(argv)
    interval: IntradayInterval = args.interval

    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(_HEAL_CANDIDATES, {"limit": args.candidates})
            candidates = [dict(row) | {"role": "adjustment_heal candidate"} for row in cur.fetchall()]
            cur.execute(_CONTROL, {"symbol": args.control})
            control = cur.fetchone()
            if control is not None and int(control["recent_heals"]):
                raise RuntimeError(
                    f"control {args.control} has {control['recent_heals']} adjustment_heal revisions in the "
                    "last 120 days; it cannot serve as the no-heal control"
                )
            if control is None:
                # ⚠ The control is the method's noise floor, not a nicety. Running without it
                # would print verdicts whose threshold nothing calibrated — and the report says
                # the control established it.
                raise RuntimeError(
                    f"control symbol {args.control!r} resolved to no tradable instrument; the probe's "
                    "tolerance is calibrated from the control and must not run without one"
                )
            targets = candidates + [dict(control) | {"role": "control (asserted: 0 recent adjustment_heal rows)"}]
            daily: dict[int, dict[date, float]] = {}
            since = reference_window_start(interval, args.count)
            for target in targets:
                cur.execute(_DAILY_CLOSES, {"instrument_id": target["instrument_id"], "since": since})
                daily[int(target["instrument_id"])] = {row["price_date"]: float(row["close"]) for row in cur.fetchall()}

    creds = load_credentials()
    results: list[dict[str, Any]] = []
    with EtoroMarketDataProvider(api_key=creds[0], user_key=creds[1], env=settings.etoro_env) as provider:
        for target in targets:
            instrument_id = int(target["instrument_id"])
            entry: dict[str, Any] = {
                "instrument_id": instrument_id,
                "symbol": target["symbol"],
                "role": target["role"],
                "last_revised_at": target.get("last_revised_at"),
            }
            try:
                bars = provider.get_intraday_candles(instrument_id, interval, args.count)
            except Exception as exc:  # noqa: BLE001 - a per-instrument failure must not sink the probe
                entry["error"] = f"{type(exc).__name__}: {exc}"
                results.append(entry)
                continue
            ratios = session_ratios(bars, daily[instrument_id], estimator="last")
            noisy = session_ratios(bars, daily[instrument_id], estimator="median")
            entry.update(
                {
                    "bars": len(bars),
                    "first_bar": bars[0].timestamp.astimezone(_NY).date() if bars else None,
                    "last_bar": bars[-1].timestamp.astimezone(_NY).date() if bars else None,
                    "paired_sessions": len(ratios),
                    "step": detect_step(ratios, tolerance=args.tolerance),
                    "step_median_estimator": detect_step(noisy, tolerance=args.tolerance),
                }
            )
            results.append(entry)
    print(_report(results, interval=interval))
    return 0


def load_credentials() -> tuple[str, str]:
    """eToro credentials for ``settings.etoro_env`` — NOT necessarily demo.

    Imported lazily from the scheduler so this probe uses the same loader production does rather
    than re-implementing credential resolution.

    ⚠ TWO PROPERTIES THE CALLER INHERITS, both stated because an earlier version of this
    docstring said "Demo eToro credentials" and was wrong on both counts:

    * the environment is whatever ``settings.etoro_env`` resolves to, so this is not a demo-only
      path by construction;
    * ``_load_etoro_credentials`` COMMITS a credential-access audit row per load by its own
      docstring, so calling it makes this script a writer.
    """
    from app.workers.scheduler import _load_etoro_credentials

    creds = _load_etoro_credentials("probe_2840_intraday_adjustment_basis")
    if creds is None:
        raise RuntimeError("no eToro credentials available; this probe needs an informational read")
    return creds


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())


__all__ = [
    "detect_step",
    "is_split_scale",
    "load_credentials",
    "main",
    "reference_window_start",
    "session_ratios",
]
