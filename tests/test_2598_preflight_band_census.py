"""The preflight band census's counting rules (#2598 scope 5, step 2).

The arm itself needs demo credentials and a live informational endpoint; what
is tested here is the part that decides what the numbers MEAN — which
observations count, which are excluded, and against which column of the frozen
band table each is compared.

⚠ Every rule below has a failure mode that produces a plausible number rather
than an error: an omitted cost row read as a zero prices the tightest names as
free, an errored observation counted as a non-exceedance flatters the model, and
a ratio taken at the rounding quantum is noise reported to three decimals.

⚠ DB-free by design. ``_census_statistics`` is pure over the stored fixture
shape, which is also what ``--replay`` feeds it.
"""

from __future__ import annotations

from typing import Any

from app.services.cost_model import BANDS
from scripts.verify_2598_preflight_quote_crosscheck import (
    CENSUS_TICKET,
    COST_QUANTUM_USD,
    DECIDABLE_MIN_QUANTA,
    _census_statistics,
)

#: The measuring ticket's rounding floor in bps — 0.01 USD on $1,000 is 0.1 bp.
FLOOR_BPS = float(COST_QUANTUM_USD / CENSUS_TICKET * 10000)

BAND = BANDS[0]
BAND_P75_BPS = float(BAND.p75_spread_pct * 100)
BAND_HALF_BPS = float(BAND.half_spread_pct * 100)


def _observation(**overrides: Any) -> dict[str, Any]:
    """A priced observation in the lowest band, comfortably above the quantum."""
    record: dict[str, Any] = {
        "instrument_id": 1,
        "symbol": "TEST",
        "band": BAND.label,
        "band_p75_bps": BAND_P75_BPS,
        "band_half_bps": BAND_HALF_BPS,
        "market_spread_row_present": True,
        "market_spread_value_null": False,
        "implied_bps_if_monetary": BAND_P75_BPS / 2,
        "observed_quote_bps": BAND_P75_BPS / 2,
    }
    record.update(overrides)
    return record


def _only(observations: list[dict[str, Any]]) -> Any:
    census = _census_statistics(observations)
    assert len(census) == 1, census
    return census[0]


class TestWhatCounts:
    def test_an_omitted_market_spread_row_is_not_a_zero(self) -> None:
        """⚠⚠ THE LOAD-BEARING RULE. eToro DROPS the row when the cost is under
        the 0.01 USD quantum. Entering it as 0 would price the tightest names as
        free and count them as a comfortable non-exceedance."""
        band = _only(
            [
                _observation(),
                _observation(market_spread_row_present=False, implied_bps_if_monetary=None),
            ]
        )
        assert (band.n, band.omitted, band.priced) == (2, 1, 1)
        assert band.over_p75 == 0
        assert band.worst_over_p75 == 0.5

    def test_an_errored_observation_is_dropped_rather_than_counted_as_compliant(self) -> None:
        """A 429 is not evidence the band held."""
        band = _only([_observation(), _observation(error="HTTPStatusError", error_detail="429")])
        assert (band.n, band.priced) == (1, 1)

    def test_a_band_with_no_observations_is_absent_rather_than_reported_as_zero(self) -> None:
        assert _census_statistics([]) == ()


class TestDecidability:
    def test_a_ratio_at_the_rounding_quantum_is_excluded(self) -> None:
        """⚠ Measured 2026-08-13: SPY returned 0.4 bp against a 0.13 bp quoted
        spread — 3.08x, and entirely rounding. Counting it would put noise into
        the statistic that resolves the one-side-vs-round-trip reading."""
        band = _only([_observation(implied_bps_if_monetary=FLOOR_BPS, observed_quote_bps=FLOOR_BPS / 3)])
        assert (band.priced, band.decidable, band.ratios) == (1, 0, ())

    def test_an_observation_at_the_threshold_is_decidable(self) -> None:
        at_threshold = DECIDABLE_MIN_QUANTA * FLOOR_BPS
        band = _only([_observation(implied_bps_if_monetary=at_threshold, observed_quote_bps=at_threshold)])
        assert (band.decidable, band.ratios) == (1, (1.0,))

    def test_a_missing_quote_cannot_produce_a_ratio_but_still_counts_as_priced(self) -> None:
        """The instrument was measured; only the independent comparison is absent
        — so it belongs in the exceedance counts and not in the ratio."""
        band = _only([_observation(observed_quote_bps=None)])
        assert (band.priced, band.decidable) == (1, 0)
        assert band.over_p75 == 0


class TestBothReadingsOfMarketSpread:
    def test_the_two_readings_are_counted_separately(self) -> None:
        """⚠⚠ A FACTOR OF TWO DECIDES THE VERDICT. A cost between the half and
        the round trip breaches the band under the one-side reading and sits
        inside it under the round-trip reading; the arm must report both rather
        than pick one."""
        between = (BAND_HALF_BPS + BAND_P75_BPS) / 2
        band = _only([_observation(implied_bps_if_monetary=between)])
        assert (band.over_half, band.over_p75) == (1, 0)

    def test_exceedance_is_strict(self) -> None:
        """Landing exactly on the band is not a breach of it."""
        band = _only([_observation(implied_bps_if_monetary=BAND_P75_BPS)])
        assert band.over_p75 == 0
        assert band.worst_over_p75 == 1.0


class TestTheWorstMultiple:
    def test_it_is_the_maximum_and_not_the_mean(self) -> None:
        """⚠ The rate says a p75 is behaving like a p75; the worst multiple is
        what says it cannot be a bound. Measured 2026-08-13: MXC at 910 bps
        against a 57.1 bps band, 15.9x, inside a band whose rate was 3-in-15."""
        band = _only(
            [
                _observation(implied_bps_if_monetary=BAND_P75_BPS / 2),
                _observation(implied_bps_if_monetary=BAND_P75_BPS * 16),
            ]
        )
        assert band.over_p75 == 1
        assert band.worst_over_p75 == 16.0
