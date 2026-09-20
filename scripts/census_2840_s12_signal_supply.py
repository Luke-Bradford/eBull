"""Census for #2840 arm 2 step 2 — S-12's in-sample SIGNAL supply, to derive the floor.

Read-only. Evaluates the RULE over the corpus and counts verdicts. It reads no
``gross_return_pct``, resolves no fill, computes no expectancy and writes no row —
the same class of pre-freeze fact as #2582's *"963 clean 13D events over 331 distinct
public filing dates"*, which ``docs/review-prevention-log.md``'s #2614 entry names as
the outcome-free half of a derived forward-shadow floor.

WHY THIS EXISTS
---------------
``prereg_contract.ForwardShadowFloor`` forbids a default and ``sql/333`` CHECKs both
numbers ``> 0``, so arm 2's declaration cannot be frozen without deriving them — and
no derivation can be honest without knowing what this rule's in-sample supply IS. This
script measures that supply. It does NOT derive the floor; see the note above
``_concentration`` for why the arithmetic was deliberately taken back out.

⚠⚠ THE MEASUREMENT SIZES THE FLOOR AND MUST NOT MOVE THE GATE. S-12 is merged and its
identity is already ``strategy-registry-v1+f6100a890599``; a supply count cannot
retro-tune a rule whose hash is fixed. ⚠ That is a narrower claim than it first reads
(Codex ckpt-1, finding 13): the hash fixes the RULE, not the floor, the census window
or the decision to continue, all of which are still open when this runs. The spec's
§"Measured premise" note that S-4's entry count above the edge *"stays unmeasured until
the declaration is frozen"* is superseded here and only here, because a floor nobody
measured is the #2600 padded floor this contract exists to forbid.

⚠⚠ NO HOLD-OUT BAR IS LOADED, LET ALONE EVALUATED. The corpus window ends the day
BEFORE ``HOLDOUT_BOUNDARY`` and ``through_date`` follows it, so the rule is never run
over a withheld bar. Filtering post-boundary VERDICTS after computing them would give
the same counts — every indicator here is causal — but "the numbers would have come out
the same" is not the access rule, and a census that opens the hold-out to discard it is
one edit away from reporting it (Codex ckpt-1, finding 21).

⚠ FIRED SIGNALS, NOT RESOLVED ONES, AND NOT TRADES. ``strategy_live_gate`` counts
forward decision dates over signals that RESOLVED (``strategy_live_gate.py:392-395``),
and the pass bar's own substrate is COSTED TRADES. Between a fired signal and a trade
sit: an unusable fill open (``signal_ledger.resolve_fills`` → ``unusable_fill_price``),
``superseded_open_position`` collapse, and ``namespace_for_signal``'s purge of a
pre-boundary signal whose FILL lands on or after the boundary. So a fired count is an
UPPER bound on the trade count, in both arms, and any derivation built on it must say
so rather than call a fire a trade (Codex ckpt-1, findings 4 and 22).

⚠ THE SERIES IS BUILT EXACTLY AS THE RUN BUILDS IT. ``evaluate_arm`` takes
``_dense_price_history(masked, ...)[0]``, and that tuple element IS
``_to_series(source.bars)`` (``backtest_run.py:1509``) whatever the return basis — the
dense arrays are the benchmark's, not the strategy's. So loading both arms off one
fetch and projecting with ``_to_series`` is the same input, at half the round trips.

⚠ S-4 IS COUNTED BESIDE S-12 and is not decoration: it is the control the pass bar
compares against, and the ratio of the two fired counts is the gate's SIGNAL-level
admission rate. The spec's census measured the BAR-level rate (3.10%) and said
explicitly that bar supply is an upper bound on signal supply and nothing more.

⚠⚠ ``masked`` IS THE PRODUCTION ARM, AND ``admitted`` IS NOT. ``price_masked_bars``,
the reader the live scan uses, carries ONE arm on purpose — *"criterion 9's `admitted`
arm is a sensitivity measurement that has no place in a scan"* (``price_masked_bars.py``
module header). Both arms are reported here because the pass bar requires both, but any
claim about what a FORWARD window would supply has to read the masked figures. An
earlier draft of the derivation had this exactly backwards (Codex ckpt-1, finding 1).

⚠ SAME-DAY CONCENTRATION IS REPORTED, because it is the entire reason the floor is
denominated in distinct dates rather than signals (``prereg_contract.py:127-132``): a
supply that arrives 70 names at a time on a handful of dates is not the evidence a
per-date count implies.

Refs #2840, #2832, #2437, #2829.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping
from datetime import date, timedelta
from typing import Final

import psycopg

from app.config import settings
from app.services.backtest_run import (
    BACKTEST_UNIVERSE,
    EVALUATION_WINDOW_START,
    Window,
    _to_series,
    load_corpus,
)
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.research_price_structure_store import load_arms
from app.services.strategies.s12_cheapest_band_price_gated_breakout import S12_STRATEGY_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.strategy_segmented_evaluation import segmented_signals

S4_STRATEGY_ID: Final = "s4-volatility-compression-breakout"

#: ⚠ ORDER MATTERS ONLY FOR THE REPORT. The derivation names each arm explicitly;
#: it does not read "the first arm".
ARMS: Final[tuple[str, ...]] = ("masked", "admitted")
STRATEGIES: Final[tuple[str, ...]] = (S12_STRATEGY_ID, S4_STRATEGY_ID)


#: ⚠⚠ NO FLOOR IS COMPUTED HERE, DELIBERATELY. The first draft of this script carried
#: the arithmetic, which put a candidate derivation inside the measurement that is
#: supposed to constrain it. Codex ckpt-1 refused that derivation on two structural
#: grounds (the production arm was the wrong one; "reproduce the whole historical
#: supply" is a design choice the pass bar never declared), and a measurement script
#: that ships a refuted formula is how the formula gets re-adopted by whoever reads it
#: next. The census measures; the freeze script owns the arithmetic, once there is one.
def _concentration(dates: Mapping[date, int]) -> dict[str, int]:
    """Fires per distinct signal date — max, median and the top date's share.

    The floor is denominated in DATES precisely because signals fan out within one,
    so the spread of this distribution is what says whether a date count is evidence.
    """
    if not dates:
        return {"max_fires_on_one_date": 0, "median_fires_per_date": 0, "dates_with_one_fire": 0}
    counts = sorted(dates.values())
    return {
        "max_fires_on_one_date": counts[-1],
        "median_fires_per_date": counts[len(counts) // 2],
        "dates_with_one_fire": sum(1 for count in counts if count == 1),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="evaluate only the first N admitted series — a TIMING SLICE, never a population figure",
    )
    parser.add_argument("--progress-every", type=int, default=500, help="series between progress lines")
    args = parser.parse_args(argv)

    fired: Counter[tuple[str, str]] = Counter()
    verdicts: Counter[tuple[str, str, str]] = Counter()
    signal_dates: dict[tuple[str, str], Counter[date]] = {(s, a): Counter() for s in STRATEGIES for a in ARMS}
    entries = {strategy_id: STRATEGY_MANIFEST[strategy_id] for strategy_id in STRATEGIES}

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        # ⚠ The window ENDS BEFORE the boundary, so no withheld bar is loaded. The
        # default window would end at the vendor's capture date (2024-09-27) and hand
        # every hold-out bar to the rule for verdicts this census then discards.
        corpus = load_corpus(
            conn,
            universe_basis=BACKTEST_UNIVERSE,
            limit=args.limit,
            evaluation_window=Window(start=EVALUATION_WINDOW_START, end=HOLDOUT_BOUNDARY - timedelta(days=1)),
        )
        # ⚠ The spec's guard 3, asserted here too: a run whose archive provenance went
        # missing is charged the maximum band on every leg, and a supply census taken
        # over a corpus that failed closed would be describing a different population
        # from the one the declaration names.
        if corpus.cost_price_basis != "as_traded":
            raise RuntimeError(
                f"corpus cost_price_basis resolved to {corpus.cost_price_basis!r}, not 'as_traded' — "
                "the pinned archive's provenance is missing and S-12's nominal gate is not measurable here"
            )
        regime_provider = MarketRegimeProvider.load_research(conn)
        opportunity = corpus.opportunity_records["in_sample"]
        opportunity_keys = set(opportunity.evaluated_instrument_ids) | {
            -series_id for series_id in opportunity.evaluated_series_ids
        }
        corpus_window_end = corpus.window.end
        total = len(corpus.pairs)
        evaluated = 0
        for series_seen, (name_key, series_id) in enumerate(corpus.pairs, start=1):
            if series_seen % args.progress_every == 0:
                print(f"... {series_seen:,}/{total:,} series seen, {evaluated:,} evaluated", flush=True)
            if name_key not in opportunity_keys:
                continue
            arms = load_arms(conn, series_id, through_date=corpus.window.end)
            breaks = corpus.unresolved_breaks.get(name_key, ())
            for arm in ARMS:
                loaded = arms[arm]
                if not loaded.bars:
                    continue
                series = _to_series(loaded.bars)
                if len(series) < 2:
                    continue
                regime = regime_provider.for_dates(series.dates)
                for strategy_id, entry in entries.items():
                    for signal in segmented_signals(
                        entry,
                        series,
                        universe=corpus.universe_basis,
                        masked_reason="quarantined_bar",
                        unresolved_breaks=breaks,
                        regime=regime,
                    ):
                        if signal.kind != "entry":
                            continue
                        when = series.dates[signal.signal_index]
                        # ⚠ The window already stops before the boundary, so this is a
                        # belt-and-braces assertion of the same fact rather than the
                        # filter it used to be.
                        assert when < HOLDOUT_BOUNDARY
                        verdicts[(strategy_id, arm, signal.verdict)] += 1
                        if signal.verdict == "fired":
                            fired[(strategy_id, arm)] += 1
                            signal_dates[(strategy_id, arm)][when] += 1
            evaluated += 1
        conn.rollback()

    def _supply(strategy_id: str, arm: str) -> dict[str, object]:
        by_date = signal_dates[(strategy_id, arm)]
        ordered = sorted(by_date)
        return {
            "fired": fired[(strategy_id, arm)],
            "distinct_signal_dates": len(by_date),
            "first_signal_date": ordered[0].isoformat() if ordered else None,
            "last_signal_date": ordered[-1].isoformat() if ordered else None,
            "span_days": (ordered[-1] - ordered[0]).days if len(ordered) > 1 else 0,
            "concentration": _concentration(by_date),
            "verdicts": {
                verdict: count
                for (sid, a, verdict), count in sorted(verdicts.items())
                if sid == strategy_id and a == arm
            },
        }

    report: dict[str, object] = {
        "universe": BACKTEST_UNIVERSE,
        "holdout_boundary": HOLDOUT_BOUNDARY.isoformat(),
        "evaluation_window_end": corpus_window_end.isoformat(),
        "production_arm": "masked",
        "limited_to_series": args.limit,
        "series_evaluated": evaluated,
        "supply": {f"{strategy_id}/{arm}": _supply(strategy_id, arm) for strategy_id in STRATEGIES for arm in ARMS},
    }
    if args.limit is not None:
        report["WARNING"] = (
            "LIMITED SLICE — a timing measurement, not a population figure; no floor may be frozen from it"
        )
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ARMS", "S4_STRATEGY_ID", "STRATEGIES", "_concentration", "main"]
