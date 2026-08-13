from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import psycopg
import pytest

from app.services.cboe_vix import (
    RETENTION_START,
    VixBar,
    VixSourceError,
    load_vix_close_as_known,
    parse_vix_history,
    refresh_cboe_vix,
    resolve_vix_close_as_known,
)

_HEADER = "DATE,OPEN,HIGH,LOW,CLOSE\n"


def test_parser_enforces_exact_schema_and_retention() -> None:
    bars = parse_vix_history(
        # Cboe contains legacy OHLC anomalies, but legacy values are outside
        # this bounded source contract and must not prevent the retained load.
        _HEADER + "02/11/1992,19.24,18.57,17.61,17.70\n" + "01/04/2021,24,25,23,24.5\n" + "01/05/2021,25,27,24,26\n"
    )
    assert [bar.bar_date for bar in bars] == [RETENTION_START.replace(day=4), RETENTION_START.replace(day=5)]
    assert bars[-1].close == Decimal("26")


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("DATE,OPEN,HIGH,LOW,SETTLE\n01/04/2021,1,1,1,1\n", "unexpected header"),
        (_HEADER + "bad,1,1,1,1\n", "DATE is not"),
        (_HEADER + "01/04/2021,NaN,1,1,1\n", "finite and positive"),
        (_HEADER + "01/04/2021,2,1,1,2\n", "envelope"),
        (_HEADER + "01/04/2021,1,1,1,1\n01/04/2021,1,1,1,1\n", "duplicate DATE"),
        (_HEADER + "12/31/2020,1,1,1,1\n", "no rows on or after"),
    ],
)
def test_parser_refuses_malformed_or_ambiguous_source(text: str, message: str) -> None:
    with pytest.raises(VixSourceError, match=message):
        parse_vix_history(text)


def test_historical_close_is_not_known_on_its_own_new_york_date() -> None:
    bars = (
        VixBar(date(2026, 8, 10), Decimal("15"), Decimal("16"), Decimal("14"), Decimal("15.5")),
        VixBar(date(2026, 8, 11), Decimal("16"), Decimal("17"), Decimal("15"), Decimal("16.5")),
    )
    same_day_after_close = datetime(2026, 8, 11, 21, tzinfo=UTC)  # 17:00 New York
    assert resolve_vix_close_as_known(bars, decision_at=same_day_after_close) == bars[0]
    assert resolve_vix_close_as_known(bars, decision_at=datetime(2026, 8, 12, 14, tzinfo=UTC)) == bars[1]


def test_causal_resolver_requires_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_vix_close_as_known((), decision_at=datetime(2026, 8, 12))


def test_refresh_reconciles_one_bounded_series_and_conditional_fetch(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    body = _HEADER + "12/31/2020,22,23,21,22.5\n" + "01/04/2021,24,25,23,24.5\n" + "01/05/2021,25,27,24,26\n"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 2:
            return httpx.Response(304, request=request)
        return httpx.Response(
            200,
            text=body,
            headers={"Last-Modified": "Wed, 06 Jan 2021 13:00:00 GMT"},
            request=request,
        )

    ebull_test_conn.execute("DELETE FROM external_data_watermarks WHERE source = 'cboe.vix-history'")
    ebull_test_conn.execute("DELETE FROM research_price_series WHERE vendor = 'cboe' AND vendor_symbol = 'VIX'")
    ebull_test_conn.commit()
    ebull_test_conn.autocommit = True
    try:
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            first = refresh_cboe_vix(ebull_test_conn, client=client)
            second = refresh_cboe_vix(ebull_test_conn, client=client)
        assert first.status == "refreshed"
        assert first.retained_bars == 2
        assert second.status == "not_modified"
        assert requests[1].headers["If-Modified-Since"] == "Wed, 06 Jan 2021 13:00:00 GMT"

        stored = ebull_test_conn.execute(
            """
            SELECT s.first_bar, s.last_bar, s.bar_count, s.instrument_id,
                   count(d.*), min(d.bar_date), max(d.bar_date)
            FROM research_price_series s
            JOIN research_price_daily d USING (series_id)
            WHERE s.vendor = 'cboe' AND s.vendor_symbol = 'VIX'
            GROUP BY s.series_id
            """
        ).fetchone()
        assert stored == (date(2021, 1, 4), date(2021, 1, 5), 2, None, 2, date(2021, 1, 4), date(2021, 1, 5))

        known = load_vix_close_as_known(
            ebull_test_conn,
            decision_at=datetime(2021, 1, 5, 15, tzinfo=UTC),
        )
        assert known is not None and known.bar_date == date(2021, 1, 4)
    finally:
        ebull_test_conn.execute("DELETE FROM external_data_watermarks WHERE source = 'cboe.vix-history'")
        ebull_test_conn.execute("DELETE FROM research_price_series WHERE vendor = 'cboe' AND vendor_symbol = 'VIX'")
        ebull_test_conn.autocommit = False
        ebull_test_conn.commit()
