"""#2833 step 2 -- pure-logic tests for the core-candidate quote lane.

No DB fixture on purpose: the decision this module makes (what is a usable
observation, and what is its denomination) is a pure function of one provider
response, so it is table-tested rather than exercised through Postgres.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.providers.market_data import Quote
from app.services.strategy_core_quote_observation import (
    MAX_QUOTE_AGE,
    SOURCE,
    normalise_quote,
    sample_bucket,
)

OBSERVED_AT = datetime(2026, 8, 23, 14, 37, 12, tzinfo=UTC)
BUCKET = datetime(2026, 8, 23, 14, 0, 0, tzinfo=UTC)


def _quote(**overrides: object) -> Quote:
    base: dict[str, object] = {
        "instrument_id": 3417,
        "timestamp": OBSERVED_AT,
        "bid": Decimal("100.00"),
        "ask": Decimal("100.10"),
        "last": Decimal("100.05"),
        "conversion_rate": Decimal("1"),
    }
    base.update(overrides)
    return Quote(**base)  # type: ignore[arg-type]


class TestSampleBucket:
    def test_truncates_to_the_hour(self) -> None:
        assert sample_bucket(OBSERVED_AT) == BUCKET

    def test_converts_to_utc_before_truncating(self) -> None:
        # The SAME instant expressed as 16:37+02:00 must land in the 14:00 UTC
        # bucket, not a 16:00 one — otherwise a provider timestamp in a local
        # offset would silently open a second bucket for one tick.
        shifted = OBSERVED_AT.astimezone(timezone(timedelta(hours=2)))
        assert shifted.hour == 16
        assert sample_bucket(shifted) == BUCKET

    def test_naive_timestamp_is_refused(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            sample_bucket(OBSERVED_AT.replace(tzinfo=None))


class TestNormaliseQuote:
    def test_observed_row_carries_spread_and_rate(self) -> None:
        obs = normalise_quote(instrument_id=3417, quote=_quote(), observed_at=OBSERVED_AT)
        assert obs.observation_status == "observed"
        assert obs.refusal_reason is None
        assert obs.sample_bucket == BUCKET
        assert obs.source == SOURCE
        assert obs.conversion_rate == Decimal("1")
        # 0.10 spread on a 100.05 midpoint = 9.995... bps
        assert obs.spread_bps is not None
        assert obs.spread_bps == pytest.approx(Decimal("9.995"), abs=Decimal("0.001"))

    def test_a_missing_quote_is_recorded_not_dropped(self) -> None:
        """Absence of a quote is coverage evidence for the percentile."""
        obs = normalise_quote(instrument_id=3417, quote=None, observed_at=OBSERVED_AT)
        assert obs.observation_status == "missing"
        assert obs.refusal_reason == "provider_omitted_quote"
        assert obs.bid is None and obs.ask is None and obs.spread_bps is None
        assert obs.conversion_rate is None

    @pytest.mark.parametrize(
        ("overrides", "reason"),
        [
            ({"bid": Decimal("0")}, "nonpositive_bid_or_ask"),
            ({"ask": Decimal("0")}, "nonpositive_bid_or_ask"),
            ({"bid": Decimal("101"), "ask": Decimal("100")}, "crossed_market"),
            ({"last": Decimal("0")}, "nonpositive_last"),
            ({"timestamp": OBSERVED_AT.replace(tzinfo=None)}, "quote_timestamp_naive"),
        ],
    )
    def test_malformed_quotes_are_refused_with_a_reason(self, overrides: dict[str, object], reason: str) -> None:
        obs = normalise_quote(instrument_id=3417, quote=_quote(**overrides), observed_at=OBSERVED_AT)
        assert obs.observation_status == "invalid"
        assert obs.refusal_reason == reason
        # A refused row carries no denomination: the rate rides on the same
        # response the bid/ask came from (sql/366's shape CHECK).
        assert obs.conversion_rate is None

    def test_a_stale_quote_is_refused_not_counted_again(self) -> None:
        """The weekend case: a shut market re-serves Friday's quote hourly.

        Recording it would inflate the five-day percentile with duplicates of
        one spread — the single error that moves a percentile with no new
        information arriving. Dev-measured 2026-08-23 (a Sunday).
        """
        friday = OBSERVED_AT - timedelta(days=2)
        obs = normalise_quote(instrument_id=3417, quote=_quote(timestamp=friday), observed_at=OBSERVED_AT)
        assert obs.observation_status == "invalid"
        assert obs.refusal_reason == "quote_stale"
        assert obs.spread_bps is None
        assert obs.conversion_rate is None

    def test_a_quote_just_inside_the_bound_is_still_evidence(self) -> None:
        """A 5-second-old quote in a fresh bucket must NOT be refused."""
        obs = normalise_quote(
            instrument_id=3417,
            quote=_quote(timestamp=OBSERVED_AT - timedelta(seconds=5)),
            observed_at=OBSERVED_AT,
        )
        assert obs.observation_status == "observed"
        assert obs.spread_bps is not None

    def test_the_staleness_bound_is_the_sampling_interval(self) -> None:
        """Exactly one interval old is accepted; a second beyond is not."""
        at_bound = normalise_quote(
            instrument_id=3417,
            quote=_quote(timestamp=OBSERVED_AT - MAX_QUOTE_AGE),
            observed_at=OBSERVED_AT,
        )
        just_past = normalise_quote(
            instrument_id=3417,
            quote=_quote(timestamp=OBSERVED_AT - MAX_QUOTE_AGE - timedelta(seconds=1)),
            observed_at=OBSERVED_AT,
        )
        assert at_bound.observation_status == "observed"
        assert just_past.observation_status == "invalid"
        assert just_past.refusal_reason == "quote_stale"

    def test_a_future_dated_quote_is_refused(self) -> None:
        """The mirror of staleness: a negative age never expires.

        Left accepted, one future-dated response could fill every later
        bucket with the same quote — the same duplicate-evidence failure
        arriving from the other direction.
        """
        obs = normalise_quote(
            instrument_id=3417,
            quote=_quote(timestamp=OBSERVED_AT + MAX_QUOTE_AGE + timedelta(seconds=1)),
            observed_at=OBSERVED_AT,
        )
        assert obs.observation_status == "invalid"
        assert obs.refusal_reason == "quote_future"

    def test_small_clock_skew_is_tolerated(self) -> None:
        """Sub-interval skew is normal and must not discard good evidence."""
        obs = normalise_quote(
            instrument_id=3417,
            quote=_quote(timestamp=OBSERVED_AT + timedelta(seconds=2)),
            observed_at=OBSERVED_AT,
        )
        assert obs.observation_status == "observed"

    def test_a_malformed_quote_reports_its_own_reason_not_staleness(self) -> None:
        """Shape guards run first so the operator sees the actionable reason."""
        obs = normalise_quote(
            instrument_id=3417,
            quote=_quote(bid=Decimal("0"), timestamp=OBSERVED_AT - timedelta(days=2)),
            observed_at=OBSERVED_AT,
        )
        assert obs.refusal_reason == "nonpositive_bid_or_ask"

    def test_absent_conversion_rate_stays_null_and_is_not_read_as_usd(self) -> None:
        """NULL means the provider did not tell us -- never 'USD'.

        `instruments.currency` is a VENUE lookup, so it cannot be used as a
        fallback: every `.L` line reads GBP whatever its real denomination.
        """
        obs = normalise_quote(instrument_id=3434, quote=_quote(conversion_rate=None), observed_at=OBSERVED_AT)
        assert obs.observation_status == "observed"
        assert obs.conversion_rate is None

    def test_nonpositive_conversion_rate_is_dropped_to_null(self) -> None:
        obs = normalise_quote(
            instrument_id=3434,
            quote=_quote(conversion_rate=Decimal("0")),
            observed_at=OBSERVED_AT,
        )
        assert obs.observation_status == "observed"
        assert obs.conversion_rate is None

    def test_non_usd_denomination_is_preserved_verbatim(self) -> None:
        """IUSA.L measured 0.0136315 on 2026-08-23 -- GBX, stored as GBP."""
        obs = normalise_quote(
            instrument_id=3075,
            quote=_quote(conversion_rate=Decimal("0.0136315")),
            observed_at=OBSERVED_AT,
        )
        assert obs.conversion_rate == Decimal("0.0136315")
        assert obs.conversion_rate != Decimal("1")
