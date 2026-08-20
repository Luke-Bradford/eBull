"""Decide what eToro's undocumented what-if ``value`` field is DENOMINATED IN.

Refs #2598. Informational demo endpoints only — this never places, modifies or
closes an order.

⚠⚠ SCALING ALONE CANNOT PROVE A UNIT, which is why #2446's census stopped short.
A component that doubles when the ticket doubles is proportional to notional;
that is equally true of a monetary amount and of a rate applied to notional. The
1x/10x arms in ``verify_2437_trading_preflight`` establish proportionality and
then have nothing left to say.

What settles it is an INDEPENDENT measurement of the same quantity. We hold one:
``quotes`` stores an observed ``bid``/``ask``/``spread_pct`` per instrument from
our own feed, produced by a code path that has never read a what-if response. If
``value`` is a monetary USD amount then ``value / ticket_amount`` is a spread
fraction and must land on the separately observed quoted spread. If ``value``
were a rate, that ratio would be smaller by four orders of magnitude and would
match nothing.

⚠ THE $100 TICKET IS THE CONTROL, NOT A SECOND SAMPLE. Costs come back rounded
to 0.01 USD, so on a $100 ticket the quantum alone is 1.0 bp and every tight
instrument reads ~1.0 bp regardless of its real spread. The agreement therefore
has to appear at $1,000 and be ABSENT at $100 — a run where both ticket sizes
agree with the quote is measuring something other than what this claims to.

Run (dev, demo credentials, ~2 requests per target within the dedicated
20/60s lane):

    PYTHONPATH=. uv run python -m scripts.verify_2598_preflight_quote_crosscheck

THE CENSUS ARM (#2598 scope 5 step 2)
-------------------------------------
``--census N`` samples N instruments from EACH frozen cost band and runs the
measuring ticket only::

    PYTHONPATH=. uv run python -m scripts.verify_2598_preflight_quote_crosscheck \
        --census 12 --out tests/fixtures/etoro_preflight_2598/band_census_<date>.json

⚠⚠ IT EXISTS BECAUSE FOUR NAMES CANNOT LICENSE A BOUND. The 2026-08-12 run
compared the banded model against ASH, KLXE, MU and SPY and the verdict turned
on a factor-of-two reading of ``marketSpread`` — one side, or the round trip.
The census answers that on a population, per band, and reports the exceedance
under BOTH readings rather than picking one.

⚠ THE RATIO IS NOT STABLE ON TIGHT NAMES, which is the trap this arm is built
around. Re-running the four-name cross-check on 2026-08-13
(``quote_crosscheck_2026-08-13_runC.json``) gave AAPL 0.71x and SPY 3.08x
against their own observed quotes, where the previous day's run had all three
of its names within 0.02x of 1.0 — because at 0.13 bp quoted, one cent of
rounding on a $1,000 ticket IS the measurement. Only observations well above
the quantum are counted as decidable; see ``DECIDABLE_MIN_QUANTA``.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

import httpx
import psycopg

from app.config import settings
from app.providers.broker import BrokerWhatIfOrder
from app.providers.implementations.etoro_broker import EtoroBrokerProvider, TradingPreflightParseError
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.cost_model import BANDS, band_for
from app.services.operators import sole_operator_id
from app.services.strategies.validated_universe import load_validated_universe

#: Instruments carried BOTH by the quote panel and by the demo broker, spanning
#: a large-cap stock, two ETFs and a thinner name. Deliberately not the
#: dollar-volume cohort ``verify_2437_trading_preflight`` selects: that cohort
#: has zero overlap with ``quotes`` (measured 2026-08-12), so it cannot be
#: cross-checked against anything at all.
DEFAULT_TARGETS: Final[tuple[int, ...]] = (1001, 3000, 3017, 9660)

#: The control ticket and the measuring ticket. See the module docstring: the
#: first exists to show the rounding quantum swamping the signal.
TICKETS: Final[tuple[Decimal, ...]] = (Decimal("100"), Decimal("1000"))

#: The census arm runs the MEASURING ticket only. The control's job is to show
#: the quantum swamping the signal on four names; repeating it on fifty spends
#: half the request budget re-proving it.
CENSUS_TICKET: Final = Decimal("1000")

#: eToro returns costs rounded to a whole cent.
COST_QUANTUM_USD: Final = Decimal("0.01")

#: ⚠ A RATIO IS ONLY DECIDABLE WELL ABOVE THE QUANTUM. On a $1,000 ticket one
#: cent is 0.1 bp, so a name quoted at 0.13 bp can return 0.4 bp and be off by
#: 3x while both readings are within a few cents of each other. A comparison is
#: counted as decidable only where the implied cost is at least this many
#: quanta, and the summary reports the decidable subset SEPARATELY rather than
#: averaging it with the noise.
DECIDABLE_MIN_QUANTA: Final = 10


def _load_demo_credentials(conn: psycopg.Connection[Any]) -> tuple[str, str]:
    ensure_broker_key_loaded(conn)
    operator_id = sole_operator_id(conn)
    keys: list[str] = []
    for label in ("api_key", "user_key"):
        keys.append(
            load_credential_for_provider_use(
                conn,
                operator_id=operator_id,
                provider="etoro",
                label=label,
                environment="demo",
                caller="verify_2598_preflight_quote_crosscheck",
            )
        )
        conn.commit()
    return keys[0], keys[1]


def _observed_quotes(conn: psycopg.Connection[Any], targets: tuple[int, ...]) -> dict[int, tuple[Any, ...]]:
    rows = conn.execute(
        """
        SELECT q.instrument_id, i.symbol, q.bid, q.ask, q.spread_pct, q.quoted_at, q.last
        FROM quotes q JOIN instruments i USING (instrument_id)
        WHERE q.instrument_id = ANY(%(ids)s)
        """,
        {"ids": list(targets)},
    ).fetchall()
    return {int(row[0]): row for row in rows}


def _census_targets(quotes: dict[int, tuple[Any, ...]], *, per_band: int) -> tuple[int, ...]:
    """Up to ``per_band`` instruments from EACH frozen cost band (#2598 step 2).

    ⚠ TAKES THE QUOTES, NOT THE CONNECTION (review NITPICK, round 2). Reading
    them inside meant the panel was fetched twice on one connection — and the
    version that mattered was the caller's, so the selection was being made from
    one read and annotated from another. Passing them in removes the duplicate
    AND makes this function pure, which is what lets the selection rule be
    table-tested instead of only exercised against a live database.

    ⚠ THE SELECTION RULE IS THE MEASUREMENT'S WEAKEST POINT, so it is stated
    rather than left in the SQL. Within a band the instruments are ordered by id
    and N positions are taken EVENLY ACROSS THE WHOLE INDEX RANGE, endpoints
    included:

    * not the first N — eToro ids run roughly in listing vintage, so the head of
      the list is the mega-caps and the sample would be the tightest names in
      every band;
    * not the widest N — that selects on the very quantity being measured, and
      would report a bound that the selection guaranteed;
    * the positions are reproducible (no RNG, no seed to carry) and independent
      of spread, which is what makes the exceedance counts mean anything.

    ⚠⚠ NOT A FIXED STRIDE, and the difference is not cosmetic (Codex ckpt-2 on
    this diff). ``ids[:: len(ids) // per_band]`` truncates: at 80 candidates and
    N=15 the step is 5 and the sample stops at index 70, leaving the last eighth
    of the band unsampled — and where a band holds fewer than ``2 × per_band``
    rows the step is 1 and it degenerates to exactly the "first N" this
    docstring claims to avoid. Interpolating the positions instead makes the
    last instrument in the band the last observation, for every N and every band
    size.

    The population is the §4.0 validated universe intersected with the quote
    panel — the set the cost model is actually applied to — and a row without a
    usable price or spread is dropped, because it can neither be banded nor
    compared.
    """
    by_band: dict[str, list[int]] = {band.label: [] for band in BANDS}
    for instrument_id, row in sorted(quotes.items()):
        spread_pct, last = row[4], row[6]
        if last is None or last <= 0 or spread_pct is None:
            continue
        by_band[band_for(last).label].append(instrument_id)
    chosen: list[int] = []
    for band in BANDS:
        chosen.extend(_even_positions(by_band[band.label], count=per_band))
    return tuple(chosen)


def _even_positions(ids: list[int], *, count: int) -> list[int]:
    """``count`` ids spread evenly over ``ids``, first and last always included.

    Strictly increasing positions (``count <= len(ids)`` after the clamp), so no
    instrument is probed twice and the request budget buys ``count`` distinct
    observations.
    """
    if not ids:
        return []
    taken = min(count, len(ids))
    if taken == 1:
        return [ids[0]]
    return [ids[index * (len(ids) - 1) // (taken - 1)] for index in range(taken)]


def _probe(
    broker: EtoroBrokerProvider,
    quotes: dict[int, tuple[Any, ...]],
    targets: tuple[int, ...],
    *,
    tickets: tuple[Decimal, ...] = TICKETS,
) -> list[dict]:
    observations: list[dict] = []
    for instrument_id in targets:
        quote = quotes.get(instrument_id)
        for ticket in tickets:
            record: dict[str, Any] = {
                "instrument_id": instrument_id,
                "symbol": None if quote is None else quote[1],
                "transaction": "buy",
                "settlement_type": "real",
                "ticket_amount_usd": str(ticket),
                "rounding_floor_bps": float(COST_QUANTUM_USD / ticket * 10000),
            }
            try:
                result = broker.get_what_if_costs(
                    BrokerWhatIfOrder(
                        instrument_id=instrument_id,
                        transaction="buy",
                        settlement_type="real",
                        amount=ticket,
                    )
                )
            # ⚠ Narrowed to the set the sibling census already established
            # (``verify_2437_trading_preflight._fetch_cost``) rather than a bare
            # ``Exception``: one malformed arm must not destroy the run, but an
            # unexpected failure class should surface rather than be recorded as
            # a data point. The MESSAGE is retained alongside the type — a type
            # name alone cannot tell a 429 from a 400 after the fact.
            except (httpx.HTTPError, TradingPreflightParseError, json.JSONDecodeError) as exc:
                record["error"] = type(exc).__name__
                record["error_detail"] = str(exc)[:300]
                observations.append(record)
                continue

            # ⚠ A COST ROW THAT IS ABSENT IS NOT A ZERO COST. Measured on AAPL at
            # a $100 ticket: the real spread is below the 0.01 USD quantum and
            # eToro OMITS the marketSpread row rather than sending 0.0. Coercing
            # the gap to zero would price the tightest names as free.
            #
            # ⚠⚠ MEMBERSHIP AND VALUE ARE TESTED SEPARATELY AND MUST STAY THAT
            # WAY. The rounding claim above rests on the row being ABSENT, and a
            # single `rows.get(...) is not None` cannot tell an omitted row from
            # a row present with a null value — it would report the second as
            # the first and corroborate the claim with the wrong observation.
            rows = {cost.cost_type: cost.value for cost in result.costs}
            spread_present = "marketSpread" in rows
            spread_value = rows.get("marketSpread")
            record["cost_rows"] = {name: (None if value is None else str(value)) for name, value in rows.items()}
            record["market_spread_row_present"] = spread_present
            record["market_spread_value_null"] = spread_present and spread_value is None
            record["implied_bps_if_monetary"] = (
                None if spread_value is None else round(float(spread_value) / float(ticket) * 10000, 2)
            )
            record["observed_quote_bps"] = None if quote is None else round(float(quote[4]) * 100, 2)
            record["quote_bid"] = None if quote is None else str(quote[2])
            record["quote_ask"] = None if quote is None else str(quote[3])
            record["quoted_at"] = None if quote is None else quote[5].isoformat()
            record["cost_last_updated"] = result.last_updated.isoformat()
            observations.append(record)
    return observations


def _annotate_bands(observations: list[dict], quotes: dict[int, tuple[Any, ...]]) -> None:
    """Attach each observation's frozen band to the RECORD, not just the report.

    The stored fixture has to be re-readable without re-running the band lookup
    — and without the reader having to know which snapshot's ``last`` was used
    to band it, which is why the price travels with it.
    """
    for record in observations:
        quote = quotes.get(record["instrument_id"])
        last = None if quote is None else quote[6]
        if last is None or last <= 0:
            record["band"] = None
            continue
        band = band_for(last)
        record["quote_last"] = str(last)
        record["band"] = band.label
        record["band_p75_bps"] = float(band.p75_spread_pct * 100)
        record["band_half_bps"] = float(band.half_spread_pct * 100)


@dataclass(frozen=True)
class BandCensus:
    """One band's census. ⚠ A VALUE, so the counting rules can be table-tested.

    The rules below are the whole content of this arm and none of them is
    obvious from a printed row; leaving them inside a ``print`` loop is how an
    omitted cost row quietly becomes a zero.
    """

    label: str
    #: Every non-error observation in the band.
    n: int
    #: ``marketSpread`` ABSENT — under the 0.01 USD quantum. NOT a zero cost.
    omitted: int
    #: Carries an implied cost. ``n − omitted``, unless a row arrived null.
    priced: int
    #: Priced AND far enough above the quantum for a ratio to mean anything.
    decidable: int
    #: implied / independently-observed quoted spread, over the decidable rows.
    ratios: tuple[float, ...]
    over_p75: int
    over_half: int
    #: The largest implied / band-p75. The rate says the statistic behaves; this
    #: says whether it can be a bound.
    worst_over_p75: float | None


def _census_statistics(observations: list[dict]) -> tuple[BandCensus, ...]:
    """Count the census, per band, under BOTH readings of ``marketSpread``.

    ⚠⚠ THE RATIO DECIDES WHAT THE EXCEEDANCE MEANS. An eToro ``marketSpread`` is
    either the full quoted spread or one side of it, and the two readings
    compare against different columns of the frozen table (``p75_spread_pct`` vs
    ``half_spread_pct``) — a factor of two, and on the 2026-08-12 sample it was
    the difference between ASH breaching its band and sitting inside it. Both
    are counted; neither is assumed.

    ⚠ AN OMITTED ``marketSpread`` ROW IS NOT A ZERO. eToro drops the row when
    the cost is under the 0.01 USD quantum, so those observations are counted on
    their own and excluded from every statistic rather than entered as 0 — which
    would price the tightest names as free and drag every mean down.

    ⚠ AN ERRORED OBSERVATION IS NOT AN OBSERVATION. A 429 or a parse failure is
    dropped from the band entirely rather than counted as a non-exceedance,
    which is the direction that would flatter the model.
    """
    floor = float(COST_QUANTUM_USD / CENSUS_TICKET * 10000)
    census: list[BandCensus] = []
    for label in [band.label for band in BANDS]:
        rows = [record for record in observations if record.get("band") == label and "error" not in record]
        if not rows:
            continue
        omitted = [record for record in rows if not record["market_spread_row_present"]]
        priced = [record for record in rows if record.get("implied_bps_if_monetary") is not None]
        ratios = tuple(
            record["implied_bps_if_monetary"] / record["observed_quote_bps"]
            for record in priced
            if record["implied_bps_if_monetary"] >= DECIDABLE_MIN_QUANTA * floor and record["observed_quote_bps"]
        )
        multiples = [record["implied_bps_if_monetary"] / record["band_p75_bps"] for record in priced]
        census.append(
            BandCensus(
                label=label,
                n=len(rows),
                omitted=len(omitted),
                priced=len(priced),
                decidable=len(ratios),
                ratios=ratios,
                over_p75=sum(1 for multiple in multiples if multiple > 1),
                over_half=sum(1 for record in priced if record["implied_bps_if_monetary"] > record["band_half_bps"]),
                worst_over_p75=max(multiples) if multiples else None,
            )
        )
    return tuple(census)


def _summarise_census(observations: list[dict]) -> None:
    """Print ``_census_statistics``. Formatting only — the rules live there."""
    print("\n  band          n   omitted   decidable   median implied/quoted   over p75   over half   worst over p75")
    every_ratio: list[float] = []
    for band in _census_statistics(observations):
        every_ratio.extend(band.ratios)
        median = f"{statistics.median(band.ratios):.2f}x" if band.ratios else "—"
        worst = "—" if band.worst_over_p75 is None else f"{band.worst_over_p75:.2f}x"
        print(
            f"  {band.label:<10} {band.n:>4}   {band.omitted:>7}   {band.decidable:>9}   {median:>21}   "
            f"{band.over_p75:>4}/{band.priced:<4}  {band.over_half:>4}/{band.priced:<4}   {worst:>13}"
        )
    # ⚠ The count prints even at zero — "errors: 0" is the evidence the arm
    # looked, and a line that appears only on failure cannot be distinguished
    # from a line nobody wrote. The TYPE LIST is what is conditional.
    errors = [record for record in observations if "error" in record]
    kinds = ", ".join(sorted({record["error"] for record in errors}))
    print(f"  errors: {len(errors)}" + (f"   {kinds}" if errors else ""))
    if every_ratio:
        print(
            f"\n  ratio over every decidable observation (n={len(every_ratio)}): "
            f"median {statistics.median(every_ratio):.3f}x   "
            f"min {min(every_ratio):.3f}x   max {max(every_ratio):.3f}x"
        )
        print(
            "  ⚠ ~1.0x reads marketSpread as the FULL quoted spread (compare against p75_spread_pct); "
            "~0.5x reads it as ONE SIDE (compare against half_spread_pct)."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instrument-id",
        type=int,
        action="append",
        help="Override a target; repeatable. Must be present in `quotes` or the cross-check is vacuous.",
    )
    parser.add_argument(
        "--census",
        type=int,
        metavar="PER_BAND",
        help="population arm: sample this many instruments from EACH frozen cost band, measuring ticket only",
    )
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        help="write the observations here as JSON instead of to stdout; the summary still prints",
    )
    parser.add_argument(
        "--replay",
        type=pathlib.Path,
        metavar="FIXTURE",
        help=(
            "re-summarise a stored census with NO network and NO credentials — the arm that makes the fixture "
            "worth storing, since a reviewer can reproduce every printed number from it"
        ),
    )
    args = parser.parse_args(argv)
    if args.replay is not None:
        _summarise_census(json.loads(args.replay.read_text()))
        return 0
    if args.census is not None and args.instrument_id:
        parser.error("--census selects its own targets; --instrument-id would silently override the population")
    if args.census is not None and args.census < 1:
        parser.error("--census must be at least 1")

    if settings.etoro_env != "demo":
        parser.error("refusing: ETORO_ENV must be demo for this probe")

    with psycopg.connect(settings.database_url) as conn:
        api_key, user_key = _load_demo_credentials(conn)
        if args.census is not None:
            # ⚠ ONE read of the panel, reused for selection AND annotation — see
            # ``_census_targets``. It is the whole §4.0 validated universe, so
            # `quotes` here is a superset of the targets; every consumer looks
            # up by instrument id.
            quotes = _observed_quotes(conn, tuple(load_validated_universe(conn)))
            targets = _census_targets(quotes, per_band=args.census)
        else:
            targets = tuple(args.instrument_id) if args.instrument_id else DEFAULT_TARGETS
            quotes = _observed_quotes(conn, targets)
        conn.rollback()

    missing = [instrument_id for instrument_id in targets if instrument_id not in quotes]
    if missing:
        sys.stderr.write(
            f"warning: no observed quote for {missing} — those rows cannot corroborate anything and are reported "
            "with a null observed_quote_bps rather than dropped\n"
        )

    broker = EtoroBrokerProvider(api_key, user_key, env="demo")
    if args.census is not None:
        # ⚠ The provider paces itself inside the dedicated 20/60s informational
        # lane, so a census of N targets takes ~3N seconds. Nothing here places,
        # modifies or closes an order.
        print(f"[census] {len(targets)} targets, ticket ${CENSUS_TICKET}, ~{3 * len(targets)}s", flush=True)
        observations = _probe(broker, quotes, targets, tickets=(CENSUS_TICKET,))
        _annotate_bands(observations, quotes)
        _summarise_census(observations)
    else:
        observations = _probe(broker, quotes, targets)

    payload = json.dumps(observations, indent=1, sort_keys=True) + "\n"
    if args.out is not None:
        args.out.write_text(payload)
        print(f"\n  wrote {len(observations)} observations to {args.out}")
    else:
        sys.stdout.write(payload)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
