"""#2834 §7 item 2, slice B — full-population verification of the split-only correction.

Run from repo root::

    PYTHONPATH=. uv run python -m scripts.measure_2834_split_adjustment --derive
    PYTHONPATH=. uv run python -m scripts.measure_2834_split_adjustment --segments
    PYTHONPATH=. uv run python -m scripts.measure_2834_split_adjustment --floor

WHY THERE IS NO BEFORE/AFTER FINGERPRINT HERE
---------------------------------------------
Slice A mutated the corpus and needed one. **This slice writes nothing.** The
correction is a derivation over stamps that are already stored
(``app/services/research_split_adjustment``), so there is no control arm to take
and no rewrite to detect: the same query returns the same bytes before and after
this branch. What replaces it is a check the stored side CAN be held to — the
vendor's own independently-computed ``adj_close``.

THE ARMS
--------
``--derive`` runs the real :func:`split_scales` over **every bar of every
Intrader series** (22,879 series, 50,134,060 bars) and asserts the invariants a
unit test cannot reach, plus the corroboration arm:

1. **Coverage.** Every series either derives a scale for every bar or raises,
   and the refusals are enumerated rather than counted. Expected: 0.
2. **Terminal scale.** The last bar of every series scales to exactly 1.
3. **Turnover.** ``close * volume`` is split-INVARIANT, so the corrected product
   should reproduce the raw one. Measured on every bar the correction actually
   moves (``scale <> 1``; a bar at scale 1 is invariant by construction).

   ⚠⚠ THE AGREEMENT IS NOT EXACT, WHICH IS ITSELF THE ANSWER TO §7 CONTRACT (b).
   ``close / scale`` is a DIVISION, and a decimal quotient terminates only when
   the reduced denominator's prime factors are 2 and 5 AND the ambient ``prec``
   is wide enough to hold it. A scale of 4 terminates; AAPL's own 7:1 does not
   (92.7 / 7 = 13.242857142…). So a consumer that corrects both sides and
   multiplies recovers the raw product only to the quotient's rounding.
   ``price_quarantine``'s T3 should therefore keep reading the RAW basis — not
   because paired adjustment is wrong in exact arithmetic (it is exactly
   turnover-preserving there) but because the raw product needs no division at
   all, so it cannot acquire the error.

   ⚠ "The raw product is exact" is itself a bounded claim: ``close *
   Decimal(volume)`` runs in the caller's context too. What is measured below is
   a TOLERANCE, and the rounding explanation is the reading it supports, not a
   proof that every unit in the last place came from the quotient.
4. **Corroboration against ``adj_close``** — the arm slice A did not have. The
   vendor ships a ninth CSV column carrying the split AND dividend adjustment
   fused. On a bar with **no dividend strictly after it**, the dividend half is
   the identity, so ``close / scale`` must equal ``adj_close``. That is an
   independent computation of the same quantity by the party that issued the
   stamps.

   ⚠⚠ IT IS AN IMPLEMENTATION CHECK, NOT AN ADJUDICATOR, and the distinction is
   the whole of #3280's lesson. It can show that our product reproduces the
   vendor's own arithmetic; it cannot show the vendor's stamps are right, because
   both sides descend from one Yahoo observation. Agreement here is circular as
   EVIDENCE and non-circular as a TEST OF THIS CODE. Nothing in this file is
   offered as support for `apply_all`; that was settled by `d15e680e` on the
   direction of harm.

``--segments`` measures §7 contract (d): whether any Intrader segment already
carries an adjustment, which a uniform rule would double-adjust. The
discriminator is internal — the price STEP across the stamped bar against the
stamp itself — and its result is REPORTED, never used to gate a correction (see
:mod:`app.services.research_split_adjustment`'s policy note).

``--floor`` measures what §7 item 3 is about without deciding it: how many
bar-slots change ``MIN_CLOSE`` membership when the floor reads the corrected
close instead of the raw one.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.services.research_split_adjustment import (
    SPLIT_ADJUSTMENT_RULE_VERSION,
    SPLIT_CORRECTION_POLICY,
    StampsUnavailable,
    UncorrectableStamp,
    corrected_price,
    corrected_volume,
    split_scales,
)

#: s2's eligibility floor, imported rather than restated — `_source_hash()`
#: hashes that whole module, so importing the constant is also the only way to
#: quote it without risking an identity rotation on a docstring edit.
from app.services.strategies.s2_cross_sectional_momentum import MIN_CLOSE

VENDOR = "icyDenev/Intrader"

#: Relative-error bands for the ``adj_close`` corroboration. The vendor's column
#: is a rounded 15-significant-digit decimal and our quotient is exact, so exact
#: equality is not the expected outcome at any scale — the bands are what make
#: "agrees" a measured statement instead of a threshold nobody wrote down.
_BANDS: tuple[Decimal, ...] = (Decimal("1e-12"), Decimal("1e-9"), Decimal("1e-6"), Decimal("1e-3"), Decimal("1e-2"))


def _band(error: Decimal) -> str:
    """Relative error → the coarsest band it clears, or ``"worse"``."""
    for band in _BANDS:
        if error <= band:
            return f"<= {band:g}"
    return "worse"


def _series_rows(conn: psycopg.Connection[Any]) -> list[tuple[int, str, str, int | None]]:
    return conn.execute(
        """
        SELECT series_id, vendor_symbol, corporate_action_stamps, bar_count
          FROM research_price_series
         WHERE vendor = %s
         ORDER BY series_id
        """,
        (VENDOR,),
    ).fetchall()


def derive(conn: psycopg.Connection[Any]) -> int:
    """Arm 1-4: run the real derivation over every bar of every series."""
    series = _series_rows(conn)
    print(f"policy={SPLIT_CORRECTION_POLICY}  rule_version={SPLIT_ADJUSTMENT_RULE_VERSION}")
    print(f"series: {len(series):,}")

    totals = Counter[str]()
    refusals: list[str] = []
    band_hits = Counter[str]()
    turnover_bands = Counter[str]()
    #: Arm 4's GAIN SIDE, by series. A disagreement rate is not inspectable as a
    #: percentage — the full-population rule is to look at what moved.
    disagreeing = Counter[str]()
    bar_span: dict[str, tuple[int, int]] = {}
    worst: tuple[Decimal, str] | None = None
    scale_extremes: list[tuple[Decimal, str]] = []

    # ⚠ A NAMED (server-side) cursor, streamed one series at a time. The whole
    # vendor is 50.1M rows; materialising it client-side is several GB, and
    # psycopg's default client-side cursor does exactly that.
    with conn.cursor(name="c_2834_slice_b") as cur:
        cur.itersize = 100_000
        cur.execute(
            """
            SELECT d.series_id, d.bar_date, d.close, d.volume, d.adj_close, d.split_factor, d.dividend
              FROM research_price_daily d
              JOIN research_price_series s USING (series_id)
             WHERE s.vendor = %s
             ORDER BY d.series_id, d.bar_date
            """,
            (VENDOR,),
        )
        markers = {row[0]: (row[1], row[2]) for row in series}
        current: int | None = None
        bucket: list[tuple[Any, ...]] = []

        def flush(series_id: int, rows: list[tuple[Any, ...]]) -> None:
            nonlocal worst
            symbol, marker = markers[series_id]
            factors = [row[5] for row in rows]
            try:
                scales = split_scales(factors, stamps_marker=marker)
            except (StampsUnavailable, UncorrectableStamp) as exc:
                totals["series_refused"] += 1
                refusals.append(f"{symbol} (series {series_id}): {exc}")
                return
            totals["series_derived"] += 1
            totals["bars"] += len(rows)
            if rows:
                # Calendar span in days against bars actually shipped — the
                # discriminator for a SPARSE series, whose missing bars take
                # their stamps with them.
                bar_span[symbol] = ((rows[-1][1] - rows[0][1]).days, len(rows))
            if scales and scales[-1] != 1:
                totals["terminal_scale_not_one"] += 1
                refusals.append(f"{symbol}: last bar scales to {scales[-1]}, not 1")
            if scales:
                # ⚠ Comparisons, never ``log10`` per bar. The scale is a STEP
                # function so its extremes are its min and max, and a log per
                # bar is 50.1M ``Decimal.ln`` calls for a headline figure.
                low, high = min(scales), max(scales)
                extreme = high if high >= 1 / low else low
                if extreme != 1:
                    scale_extremes.append((extreme, symbol))

            # `dividend` strictly AFTER the bar — walked backwards so the
            # "no distribution later" suffix is known in one pass.
            clean_suffix = True
            for index in range(len(rows) - 1, -1, -1):
                _sid, _day, close, volume, adj_close, _factor, dividend = rows[index]
                scale = scales[index]

                if scale != 1:
                    totals["bars_moved"] += 1
                    # Arm 3: turnover, on the bars the correction MOVES. A bar
                    # at scale 1 is invariant by construction, so measuring it
                    # would be comparing a number to itself 50 million times.
                    #
                    # ⚠⚠ THE SKIP IS COUNTED, AND ITS TWO CAUSES ARE COUNTED
                    # SEPARATELY. A zero turnover makes the relative error
                    # undefined, and a band table whose shares do not sum to
                    # 100% reads as full coverage when it is not — the
                    # silent-truncation defect.
                    #
                    # ⚠ Volume and close are DISTINCT causes and an earlier
                    # draft reported both as "zero or absent volume" (review-bot
                    # NITPICK). `close` is NOT NULL but not positive-constrained:
                    # `select count(*) from research_price_daily where close <= 0`
                    # returns 2. Naming the wrong cause for an exclusion is the
                    # same defect as not naming it, one step smaller.
                    if not volume:
                        totals["turnover_undefined_volume"] += 1
                    elif not close:
                        totals["turnover_undefined_close"] += 1
                    else:
                        raw = close * Decimal(volume)
                        left = corrected_price(close, scale)
                        right = corrected_volume(volume, scale)
                        assert left is not None and right is not None
                        turnover_bands[_band(abs(left * right - raw) / abs(raw))] += 1

                # Arm 4: corroboration on the dividend-free suffix only.
                if clean_suffix and adj_close is not None and adj_close > 0:
                    totals["corroborable"] += 1
                    derived = corrected_price(close, scale)
                    assert derived is not None
                    error = abs(derived - adj_close) / adj_close
                    band_hits[_band(error)] += 1
                    # ⚠⚠ SPLIT BY WHETHER THE CORRECTION DID ANYTHING. A bar at
                    # scale 1 compares `close` against `adj_close` and exercises
                    # no arithmetic at all, so folding it into one headline
                    # agreement rate credits the derivation for identity
                    # comparisons (Codex checkpoint 1). The non-trivial
                    # denominator is the one the claim is about.
                    key = "nontrivial" if scale != 1 else "trivial"
                    totals[f"corroborable_{key}"] += 1
                    if error <= _BANDS[0]:
                        totals[f"agree_{key}"] += 1
                    if error > _BANDS[-1]:
                        disagreeing[symbol] += 1
                        if worst is None or error > worst[0]:
                            worst = (error, f"{symbol} {rows[index][1]} close={close} scale={scale} adj={adj_close}")
                if dividend:
                    clean_suffix = False

        for row in cur:
            if current is not None and row[0] != current:
                flush(current, bucket)
                bucket = []
                done = totals["series_derived"] + totals["series_refused"]
                if done % 2_000 == 0:
                    print(f"  ... {done:,} series, {totals['bars']:,} bars", flush=True)
            current = row[0]
            bucket.append(row)
        if current is not None:
            flush(current, bucket)

    print("\n=== arm 1-2: derivation invariants (full population) ===")
    for key in ("series_derived", "series_refused", "bars", "bars_moved", "terminal_scale_not_one"):
        print(f"  {key:26s} {totals[key]:>14,}")
    for line in refusals[:20]:
        print(f"  REFUSED {line}")
    if len(refusals) > 20:
        print(f"  ... and {len(refusals) - 20:,} more")

    scale_extremes.sort(key=lambda pair: abs(pair[0].log10()), reverse=True)
    print("\n  largest |scale| carried by a bar:")
    for value, symbol in scale_extremes[:10]:
        print(f"    {symbol:12s} {value}")

    def _report(title: str, hits: Counter[str], denominator: int) -> None:
        print(f"\n{title}  (n = {denominator:,})")
        for band in (*[f"<= {band:g}" for band in _BANDS], "worse"):
            count = hits[band]
            share = 100.0 * count / denominator if denominator else 0.0
            print(f"    {band:>10s}  {count:>12,}  {share:6.2f}%")

    excluded = totals["turnover_undefined_volume"] + totals["turnover_undefined_close"]
    measured = totals["bars_moved"] - excluded
    _report("=== arm 3: corrected turnover vs raw turnover, on moved bars ===", turnover_bands, measured)
    print(f"  excluded, turnover zero so the relative error is undefined: {excluded:,}")
    print(f"    zero or absent volume  {totals['turnover_undefined_volume']:>12,}")
    print(f"    non-positive close     {totals['turnover_undefined_close']:>12,}")
    print("  ⚠ The residual is the QUOTIENT'S ROUNDING, not a defect — see the module docstring.")

    _report("=== arm 4: corroboration against the vendor's own adj_close ===", band_hits, totals["corroborable"])
    print(
        f"  comparable bars: {totals['corroborable']:,} of {totals['bars']:,} "
        f"({100.0 * totals['corroborable'] / totals['bars']:.2f}% of the corpus)"
    )
    for key in ("nontrivial", "trivial"):
        denominator = totals[f"corroborable_{key}"]
        agree = totals[f"agree_{key}"]
        share = 100.0 * agree / denominator if denominator else 0.0
        label = "scale <> 1 (the correction did something)" if key == "nontrivial" else "scale = 1 (identity)"
        print(f"    {label:44s} {agree:>12,} / {denominator:>12,}  {share:6.2f}%")
    if worst is not None:
        print(f"  worst: rel err {worst[0]:.3e} at {worst[1]}")
    print(f"  disagreeing series: {len(disagreeing):,} of {totals['series_derived']:,}")
    print(f"  {'symbol':10s} {'bars':>8s} {'span(d)':>9s} {'bars/yr':>9s} {'disagreeing':>12s}")
    for symbol, count in disagreeing.most_common(15):
        span, bars = bar_span.get(symbol, (0, 0))
        per_year = bars / (span / 365.25) if span else 0.0
        print(f"  {symbol:10s} {bars:>8,} {span:>9,} {per_year:>9.1f} {count:>12,}")
    print(
        "\n  ⚠ An implementation check, NOT an adjudicator — both sides descend from one\n"
        "  Yahoo observation. ⚠⚠ A series shipping far fewer than ~252 bars/yr is SPARSE,\n"
        "  and a bar the vendor never shipped took its stamp with it: `vendor_supplied`\n"
        "  guarantees a stamp on every STORED bar, never a complete EVENT set."
    )

    # ⚠ The refusal is on the INVARIANTS only. A disagreement band is a
    # measurement of two processings and is reported, never asserted — §5's
    # policy does not depend on it and neither does this exit code.
    broken = totals["series_refused"] + totals["terminal_scale_not_one"]
    return 1 if broken else 0


def segments(conn: psycopg.Connection[Any]) -> int:
    """§7 contract (d): does any segment already carry an adjustment?

    The discriminator is the price STEP across the stamped bar. A stamp on an
    UNADJUSTED segment prints a step of the factor's size; on an ALREADY-ADJUSTED
    one it prints no step at all.

    ⚠⚠ REPORTED, NOT APPLIED. Turning this into a gate would reintroduce exactly
    what `d15e680e` §5 froze out: a free parameter (the band below) plus a
    discriminator of unmeasured precision. `b65abd9c` separately measured the
    internal check passing 364 of 383 stamps an external processing disputes, so
    its own error rate is known to be poor. It bounds the residual; it does not
    adjudicate it.
    """
    # ⚠⚠ EVERY EVENT LANDS IN EXACTLY ONE CLASS, and the first draft of this
    # query did not. It reported "step matches the factor" and "no step" as
    # independent FILTERs, and the two acceptance regions OVERLAP whenever the
    # factor is only just outside the band: at f = 1.26 both hold for any step in
    # [1.008, 1.25]. 12 events were counted twice and the columns summed to
    # 100.145% of their own row (Codex checkpoint 1). A CASE makes the
    # classification exhaustive and disjoint by construction, and the overlap
    # becomes its own named class instead of a silent double count.
    #
    # The band itself is stated, not implied: a step is "matching" when
    # step / factor lands in [0.8, 1.25], and "absent" when step alone does.
    rows = conn.execute(
        """
        WITH ev AS (
            SELECT d.series_id, d.bar_date, d.split_factor, d.close,
                   lag(d.close) OVER (PARTITION BY d.series_id ORDER BY d.bar_date) AS prev_close
              FROM research_price_daily d
              JOIN research_price_series s USING (series_id)
             WHERE s.vendor = %s
        ),
        e AS (
            SELECT *, prev_close / close AS step
              FROM ev
             WHERE split_factor <> 1 AND prev_close IS NOT NULL AND close > 0 AND prev_close > 0
        ),
        c AS (
            SELECT CASE WHEN split_factor BETWEEN 0.8 AND 1.25 THEN 'factor_inside_band' ELSE 'resolvable' END
                       AS cls,
                   CASE WHEN split_factor > 1 THEN 'forward' ELSE 'reverse' END AS dir,
                   CASE
                       WHEN step / split_factor BETWEEN 0.8 AND 1.25 AND step BETWEEN 0.8 AND 1.25
                           THEN 'both'
                       WHEN step / split_factor BETWEEN 0.8 AND 1.25 THEN 'step_matches_factor'
                       WHEN step BETWEEN 0.8 AND 1.25 THEN 'no_step'
                       ELSE 'neither'
                   END AS verdict
              FROM e
        )
        SELECT cls, dir, count(*),
               count(*) FILTER (WHERE verdict = 'step_matches_factor'),
               count(*) FILTER (WHERE verdict = 'no_step'),
               count(*) FILTER (WHERE verdict = 'both'),
               count(*) FILTER (WHERE verdict = 'neither')
          FROM c
         GROUP BY 1, 2
         ORDER BY 1, 2
        """,
        (VENDOR,),
    ).fetchall()
    stamped, comparable = conn.execute(
        """
        SELECT count(*) FILTER (WHERE d.split_factor <> 1),
               count(*) FILTER (WHERE d.split_factor <> 1 AND d.prev_close IS NOT NULL
                                  AND d.close > 0 AND d.prev_close > 0)
          FROM (SELECT d.*, lag(d.close) OVER (PARTITION BY d.series_id ORDER BY d.bar_date) AS prev_close
                  FROM research_price_daily d
                  JOIN research_price_series s USING (series_id)
                 WHERE s.vendor = %s) d
        """,
        (VENDOR,),
    ).fetchone() or (0, 0)
    print("=== §7 contract (d): already-adjusted segments — REPORTED, NOT GATED ===")
    print(f"  stamped events in the corpus: {stamped:,}")
    print(f"  of which testable (a prior bar with a positive close exists): {comparable:,}")
    print(f"  ⚠ EXCLUDED: {stamped - comparable:,} events with no comparable prior bar — no step test exists.")
    print(
        f"  {'class':20s} {'dir':8s} {'events':>8s} {'step=factor':>12s} {'no step':>8s} {'both':>6s} {'neither':>8s}"
    )
    for cls, direction, events, step_eq, no_step, both, neither in rows:
        print(f"  {cls:20s} {direction:8s} {events:>8,} {step_eq:>12,} {no_step:>8,} {both:>6,} {neither:>8,}")
    print(
        "\n  Classes are DISJOINT and EXHAUSTIVE — the four columns sum to 'events' per row.\n"
        "  'both' is the overlap region: the factor is outside the band but close enough that\n"
        "  a step of the factor's size is ALSO a step of no size. The test cannot separate them.\n"
        "  'no step' on a RESOLVABLE event is the already-adjusted CANDIDATE class — consistent\n"
        "  with a prior adjustment, and equally with a false, mis-dated or stub-bar stamp.\n"
        "  'neither' is a step of the wrong magnitude, which ordinary returns also produce."
    )
    return 0


def floor(conn: psycopg.Connection[Any]) -> int:
    """§7 item 3's inputs: what the eligibility floor rejects on each basis.

    ⚠ DOES NOT DECIDE IT. Whether ``MIN_CLOSE`` should read the raw or the
    corrected close is a strategy-definition question and it rotates s2's
    identity, so it belongs to the slice that mints the new id.
    """
    series = _series_rows(conn)
    markers = {row[0]: (row[1], row[2]) for row in series}
    counts = Counter[str]()
    with conn.cursor(name="c_2834_floor") as cur:
        cur.itersize = 100_000
        cur.execute(
            """
            SELECT d.series_id, d.close, d.split_factor
              FROM research_price_daily d
              JOIN research_price_series s USING (series_id)
             WHERE s.vendor = %s
             ORDER BY d.series_id, d.bar_date
            """,
            (VENDOR,),
        )
        current: int | None = None
        bucket: list[tuple[Any, ...]] = []

        def flush(series_id: int, rows: list[tuple[Any, ...]]) -> None:
            _symbol, marker = markers[series_id]
            try:
                scales = split_scales([row[2] for row in rows], stamps_marker=marker)
            except StampsUnavailable, UncorrectableStamp:
                counts["series_refused"] += 1
                return
            floor_value = Decimal(str(MIN_CLOSE))
            for (_sid, close, _factor), scale in zip(rows, scales, strict=True):
                raw_ok = close >= floor_value
                corrected = corrected_price(close, scale)
                assert corrected is not None
                corrected_ok = corrected >= floor_value
                counts["bars"] += 1
                if raw_ok and not corrected_ok:
                    counts["admitted_raw_rejected_corrected"] += 1
                elif corrected_ok and not raw_ok:
                    counts["rejected_raw_admitted_corrected"] += 1

        for row in cur:
            if current is not None and row[0] != current:
                flush(current, bucket)
                bucket = []
            current = row[0]
            bucket.append(row)
        if current is not None:
            flush(current, bucket)

    print(f"=== §7 item 3 inputs: MIN_CLOSE = {MIN_CLOSE} on each basis ===")
    for key in ("bars", "series_refused", "admitted_raw_rejected_corrected", "rejected_raw_admitted_corrected"):
        print(f"  {key:36s} {counts[key]:>14,}")
    moved = counts["admitted_raw_rejected_corrected"] + counts["rejected_raw_admitted_corrected"]
    share = 100.0 * moved / counts["bars"] if counts["bars"] else 0.0
    print(f"  {'bars whose membership moves':36s} {moved:>14,}  ({share:.3f}%)")
    print(
        "\n  ⚠ A membership count is not a decision. Both directions exist: a forward-split\n"
        "  name's early bars DIVIDE below the floor, a reverse-split name's INFLATE above it.\n"
        "  Which basis the floor should read is §7 item 3 and rotates s2's identity."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--derive", action="store_true")
    parser.add_argument("--segments", action="store_true")
    parser.add_argument("--floor", action="store_true")
    args = parser.parse_args(argv)
    if not (args.derive or args.segments or args.floor):
        parser.error("pass one of --derive / --segments / --floor")

    rc = 0
    with psycopg.connect(settings.database_url) as conn:
        if args.segments:
            rc |= segments(conn)
        if args.floor:
            rc |= floor(conn)
        if args.derive:
            rc |= derive(conn)
    return rc


if __name__ == "__main__":
    sys.exit(main())
