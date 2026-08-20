"""#2797 A/B — what S-2's weekday-cut rebalance calendar changes on the FULL population.

Two arms over the same production corpus (``price_daily`` through
``load_masked_bars``, which is exactly what ``strategy_signal_scan`` reads):

- **control** — ``rebalance_dates`` as it stood before #2797: first bar of a new
  month over the panel's raw union calendar, weekends included;
- **treatment** — the shipped rule: weekends dropped before the month rule runs.

Reported per rebalance date: the eligible cross-section it ranked and the number
of names the decile cut selected. The control's weekend dates are the defect;
the point of the arm is to show what they cost, not to assume it.

⚠ The control is RE-DERIVED here, not simulated by rerunning the treatment with
a flag: ``_control_rebalance_dates`` is a verbatim copy of the pre-fix function
body, so neither arm can inherit the other's bug.

Run::

    PYTHONPATH=. uv run python scripts/ab_2797_s2_weekday_rebalance.py

Exits non-zero if the treatment leaves any weekend rebalance date standing, or
if it moves a date whose control panel was already healthy.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable
from datetime import date

import psycopg

from app.config import settings
from app.services.price_masked_bars import load_masked_bars, load_union_calendar
from app.services.strategies.s2_cross_sectional_momentum import (
    ELIGIBILITY_BARS,
    MIN_CROSS_SECTION,
    momentum_series,
    rebalance_dates,
    s2_select,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_signal_scan import SCAN_UNIVERSE

#: A panel at or above this is unambiguously a real session, not a corpus
#: artefact. Only used to ASSERT the treatment did not move a healthy date —
#: never to choose one.
HEALTHY_PANEL = 1000


def _control_rebalance_dates(calendar: Iterable[date]) -> frozenset[date]:
    """The pre-#2797 rule, copied verbatim. Do not refactor into the new one."""
    ordered = sorted(set(calendar))
    return frozenset(
        when
        for previous, when in zip(ordered, ordered[1:], strict=False)
        if (when.year, when.month) != (previous.year, previous.month)
    )


def _scores_at(
    conn: psycopg.Connection[object], instrument_ids: list[int], wanted: set[date]
) -> dict[date, dict[int, float]]:
    """Every instrument's S-2 score on each wanted date, from the masked corpus.

    One streaming pass over the panel — the same shape and the same loader the
    scan uses, so a name excluded here is excluded there.
    """
    out: dict[date, dict[int, float]] = {when: {} for when in wanted}
    for position, instrument_id in enumerate(instrument_ids, start=1):
        if position % 500 == 0:
            print(f"  ... {position}/{len(instrument_ids)} instruments", flush=True)
        series = load_masked_bars(conn, instrument_id).series
        if len(series) < ELIGIBILITY_BARS:
            continue
        index_of = {when: index for index, when in enumerate(series.dates) if when in wanted}
        if not index_of:
            continue
        # SCAN_UNIVERSE, not a label of our own: the arm has to score the panel
        # under the same universe stamp the production scan does.
        scores = momentum_series(series, universe=SCAN_UNIVERSE)
        closes = series.float_closes
        for when, index in index_of.items():
            value = scores.values[index]
            close = closes[index]
            # The §9 Q3 price floor is an ELIGIBILITY rule: a sub-$1 or masked
            # bar is simply not a decision bar for this name.
            if value is None or close is None or close < 1.0:
                continue
            out[when][instrument_id] = value
    return out


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        instrument_ids = sorted(load_validated_universe(conn))
        calendar = load_union_calendar(conn, instrument_ids)
        control = sorted(_control_rebalance_dates(calendar))
        treatment = sorted(rebalance_dates(calendar))
        print(f"[ab] validated universe {len(instrument_ids)} instruments")
        print(f"[ab] union calendar {calendar[0]} -> {calendar[-1]} ({len(calendar)} dates)")
        print(f"[ab] control rebalance dates {len(control)}, treatment {len(treatment)}")

        wanted = set(control) | set(treatment)
        print(f"[ab] scoring the panel on {len(wanted)} distinct dates", flush=True)
        scores = _scores_at(conn, instrument_ids, wanted)

    def selected(when: date) -> tuple[int, int | None]:
        """Panel size and names selected, or ``None`` where the runner refuses.

        ``None`` rather than a sentinel int: "refused" is a different kind of
        answer from "selected zero", and a magic -1 that later gets summed or
        formatted alongside real counts is how the two get conflated.
        """
        panel = scores[when]
        if len(panel) < MIN_CROSS_SECTION:
            return len(panel), None  # thin_cross_section — the runner refuses it
        return len(panel), len(s2_select(when, panel))

    def _row(when: date, panel: int, picked: int | None) -> str:
        verdict = "refused" if picked is None else str(picked)
        return f"{when!s:<12} {when.strftime('%a'):<4} {panel:>7} {verdict:>9}"

    moved_out = sorted(set(control) - set(treatment))
    moved_in = sorted(set(treatment) - set(control))
    print(f"\n[ab] dates only in CONTROL: {len(moved_out)}   only in TREATMENT: {len(moved_in)}")

    print("\n[ab] CONTROL-only dates (what the fix removes)")
    print(f"{'date':<12} {'day':<4} {'panel':>7} {'selected':>9}")
    control_only_fired = 0
    for when in moved_out:
        panel, picked = selected(when)
        control_only_fired += picked or 0
        print(_row(when, panel, picked))

    print("\n[ab] TREATMENT-only dates (what the fix gains)")
    print(f"{'date':<12} {'day':<4} {'panel':>7} {'selected':>9}")
    treatment_only_fired = 0
    for when in moved_in:
        panel, picked = selected(when)
        treatment_only_fired += picked or 0
        print(_row(when, panel, picked))

    print(f"\n[ab] entry signals on control-only dates:   {control_only_fired}")
    print(f"[ab] entry signals on treatment-only dates: {treatment_only_fired}")

    failures = 0
    weekend_left = [when for when in treatment if when.weekday() >= 5]
    if weekend_left:
        print(f"[ab] FAIL treatment still rebalances on {len(weekend_left)} weekend dates: {weekend_left}")
        failures += 1

    # A moved date whose control panel was already healthy would mean the cut
    # changed a real session, not an artefact.
    for when in moved_out:
        panel, _ = selected(when)
        if panel >= HEALTHY_PANEL:
            print(f"[ab] FAIL {when} had a healthy panel of {panel} and was moved anyway")
            failures += 1

    print("[ab] OK" if failures == 0 else f"[ab] {failures} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
