"""Tests for the CIK discovery sweep (#794-derived follow-up).

Pins the contract: idempotent, no-clobber-on-conflict, fold over
no-CIK instruments only, miss-counter accurate.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.cik_discovery import (
    TickerMapEntry,
    discover_ciks,
    upsert_cik,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_existing_cik(conn: psycopg.Connection[tuple], *, iid: int, cik_padded: str) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        ) VALUES (%s, 'sec', 'cik', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (iid, cik_padded),
    )


def test_discover_inserts_cik_for_no_cik_instrument(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_001, symbol="AAPL")
    conn.commit()
    fake_map = {
        "AAPL": TickerMapEntry(cik_padded="0000320193", ticker="AAPL", title="Apple Inc."),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    assert result.instruments_scanned == 1
    assert result.matches_found == 1
    assert result.rows_inserted == 1
    assert result.misses == 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_value FROM external_identifiers
            WHERE instrument_id = %s AND provider = 'sec' AND identifier_type = 'cik'
            """,
            (900_001,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "0000320193"


def test_discover_skips_instruments_with_existing_cik(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Instruments that already have a CIK row are not in the
    discovery cohort. The sweep walks only ``no_cik`` instruments
    via the LEFT JOIN filter."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_002, symbol="MSFT")
    _seed_existing_cik(conn, iid=900_002, cik_padded="0000789019")
    conn.commit()
    fake_map = {
        "MSFT": TickerMapEntry(cik_padded="9999999999", ticker="MSFT", title="Spoof"),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    # MSFT already has a CIK → not in the no-CIK cohort → not scanned.
    assert result.instruments_scanned == 0

    # Existing CIK preserved.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier_value FROM external_identifiers WHERE instrument_id = %s",
            (900_002,),
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["0000789019"]


def test_discover_records_misses_when_ticker_not_in_map(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_003, symbol="NOTREAL")
    conn.commit()

    result = discover_ciks(conn, ticker_map={})

    assert result.instruments_scanned == 1
    assert result.matches_found == 0
    assert result.misses == 1
    assert result.rows_inserted == 0


def test_discover_is_idempotent(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A second pass with the same map produces zero new inserts —
    the inserted CIK from the first pass means the instrument is no
    longer in the no-CIK cohort."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_004, symbol="GME")
    conn.commit()
    fake_map = {
        "GME": TickerMapEntry(cik_padded="0001326380", ticker="GME", title="GameStop Corp."),
    }

    first = discover_ciks(conn, ticker_map=fake_map)
    second = discover_ciks(conn, ticker_map=fake_map)

    assert first.rows_inserted == 1
    assert second.instruments_scanned == 0
    assert second.rows_inserted == 0


def test_upsert_cik_does_not_clobber_existing_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Operator-curated CIK takes precedence over discovery match —
    ON CONFLICT DO NOTHING preserves the prior row."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_005, symbol="OVERRIDE")
    _seed_existing_cik(conn, iid=900_005, cik_padded="0000111111")
    conn.commit()

    inserted = upsert_cik(
        conn,
        instrument_id=900_005,
        cik_padded="0000999999",
        ticker="OVERRIDE",
    )
    conn.commit()

    assert inserted is False
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier_value FROM external_identifiers WHERE instrument_id = %s AND identifier_type = 'cik'",
            (900_005,),
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["0000111111"]


def test_discover_strips_rth_suffix_and_matches_underlying(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """eToro's ``.RTH`` (regular trading hours) listing is an
    operational duplicate of the underlying common stock — same
    issuer, same SEC CIK. Strip the suffix and re-try the lookup so
    the no-CIK cohort folds these in without a separate seed."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_010, symbol="AAPL.RTH")
    conn.commit()
    fake_map = {
        "AAPL": TickerMapEntry(cik_padded="0000320193", ticker="AAPL", title="Apple Inc."),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    assert result.matches_found == 1
    assert result.rows_inserted == 1
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_value FROM external_identifiers
            WHERE instrument_id = %s AND provider = 'sec' AND identifier_type = 'cik'
            """,
            (900_010,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "0000320193"


def test_discover_prefers_original_symbol_over_stripped(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """When the original symbol IS in the SEC map (rare hypothetical
    where SEC adds a ``.RTH`` ticker), the original must win over
    the stripped fallback. Order is original-first."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_011, symbol="X.RTH")
    conn.commit()
    fake_map = {
        "X.RTH": TickerMapEntry(cik_padded="9999999999", ticker="X.RTH", title="Hypothetical"),
        "X": TickerMapEntry(cik_padded="0000000001", ticker="X", title="X Corp"),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    assert result.rows_inserted == 1
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier_value FROM external_identifiers WHERE instrument_id = %s",
            (900_011,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "9999999999"  # original ticker won, not the stripped fallback


def test_discover_underlying_wins_over_suffix_duplicate_in_same_cohort(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """When BOTH the operational-duplicate (``.RTH``) and the
    underlying are in the no-CIK cohort, the underlying must claim
    the SEC CIK row — not the suffix-stripped fallback. Two-pass
    ordering: direct matches first, suffix fallbacks second.
    Regression for the high-severity Codex finding."""
    conn = ebull_test_conn
    # Seed the .RTH duplicate FIRST (lower instrument_id) so a
    # naive single-pass loop ordered by instrument_id would let it
    # win. The underlying must still claim the CIK.
    _seed_instrument(conn, iid=900_020, symbol="AAPL.RTH")
    _seed_instrument(conn, iid=900_021, symbol="AAPL")
    conn.commit()
    fake_map = {
        "AAPL": TickerMapEntry(cik_padded="0000320193", ticker="AAPL", title="Apple Inc."),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    assert result.rows_inserted == 1
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id FROM external_identifiers
            WHERE provider = 'sec' AND identifier_type = 'cik'
              AND identifier_value = '0000320193'
            """,
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == [900_021]  # underlying won, not the .RTH duplicate


def test_discover_does_not_fold_warrant_suffixes(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Warrants (``-W``, ``-WT``) and preferreds (``-PA`` etc.) are
    SEPARATE securities — folding them onto the common-stock CIK
    would mis-attribute filings on the ownership pie chart. The
    suffix list deliberately excludes them; they should miss."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_012, symbol="WARRANT-W")
    conn.commit()
    fake_map = {
        "WARRANT": TickerMapEntry(cik_padded="0000111111", ticker="WARRANT", title="Should not fold"),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    assert result.matches_found == 0
    assert result.rows_inserted == 0


def test_parse_company_tickers_exchange_yields_etf_entries() -> None:
    """The exchange.json export carries the full set including
    ETFs / NYSE Arca listings that company_tickers.json misses.
    Parser yields TickerMapEntry per row."""
    from app.services.cik_discovery import _parse_company_tickers_exchange

    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [1100663, "ISHARES TR", "IVV", "NYSE Arca"],
            [884394, "INVESCO QQQ TRUST", "QQQ", "Nasdaq"],
        ],
    }

    entries = list(_parse_company_tickers_exchange(payload))
    assert len(entries) == 2
    by_ticker = {e.ticker: e for e in entries}
    assert by_ticker["IVV"].cik_padded == "0001100663"
    assert by_ticker["IVV"].title == "ISHARES TR"
    assert by_ticker["IVV"].source == "company_tickers_exchange"
    assert by_ticker["QQQ"].cik_padded == "0000884394"


def test_parse_company_tickers_exchange_skips_malformed_rows() -> None:
    """Defensive parser: missing fields, non-list rows, non-int
    CIKs, empty tickers all skipped without raising."""
    from app.services.cik_discovery import _parse_company_tickers_exchange

    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],
            "not a row",
            [None, "X Corp", "XCORP", "NYSE"],
            [123, "Y Corp", "", "NYSE"],
            [123, "Z Corp", "ZCORP", "NYSE"],
        ],
    }

    entries = list(_parse_company_tickers_exchange(payload))
    tickers = {e.ticker for e in entries}
    assert tickers == {"AAPL", "ZCORP"}


def test_fetch_ticker_map_merges_canonical_and_exchange_with_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fetch_ticker_map merges the canonical company_tickers.json
    with the company_tickers_exchange.json supplement. On ticker
    collision, canonical wins; otherwise exchange.json fills gaps
    (ETFs, NYSE Arca listings)."""
    from app.services import cik_discovery

    payloads: dict[str, Any] = {
        cik_discovery._TICKERS_URL: {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        },
        cik_discovery._TICKERS_EXCHANGE_URL: {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                # Collision — canonical wins, this row should NOT overwrite.
                [9999999, "Spoof", "AAPL", "NYSE"],
                # New entry — should win on the exchange-only path.
                [1100663, "ISHARES TR", "IVV", "NYSE Arca"],
            ],
        },
    }

    monkeypatch.setattr(cik_discovery, "_fetch_sec_json", lambda url: payloads[url])

    out = cik_discovery.fetch_ticker_map()

    assert out["AAPL"].cik_padded == "0000320193"
    assert out["AAPL"].source == "company_tickers"
    assert out["IVV"].cik_padded == "0001100663"
    assert out["IVV"].source == "company_tickers_exchange"


def test_fetch_ticker_map_aborts_when_canonical_source_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail-closed on canonical: if company_tickers.json is
    unfetchable, the sweep MUST raise rather than proceeding with
    only the supplement. Otherwise a ticker whose canonical entry
    is currently unreachable could get a wrong CIK persisted from
    the supplement that later healthy sweeps can't repair (the
    one-instrument-per-CIK constraint plus upsert_cik's no-op on
    existing primary)."""
    from app.services import cik_discovery

    def _fetch(url: str) -> Any:
        if url == cik_discovery._TICKERS_URL:
            raise RuntimeError("simulated canonical outage")
        return {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[1100663, "ISHARES TR", "IVV", "NYSE Arca"]],
        }

    monkeypatch.setattr(cik_discovery, "_fetch_sec_json", _fetch)

    with pytest.raises(RuntimeError, match="simulated canonical outage"):
        cik_discovery.fetch_ticker_map()


def test_fetch_ticker_map_continues_on_supplement_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Best-effort on supplement: if exchange.json fails, sweep
    proceeds with canonical-only coverage. Missed ETF resolution
    today is recoverable; aborting on a supplement failure would
    block all common-stock discovery."""
    from app.services import cik_discovery

    def _fetch(url: str) -> Any:
        if url == cik_discovery._TICKERS_URL:
            return {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
        raise RuntimeError("simulated supplement outage")

    monkeypatch.setattr(cik_discovery, "_fetch_sec_json", _fetch)

    out = cik_discovery.fetch_ticker_map()
    assert "AAPL" in out


def test_discover_resolves_etf_ticker_via_exchange_export(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """End-to-end: an instrument with an ETF ticker that's NOT in
    company_tickers.json gets resolved via the exchange supplement."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_030, symbol="IVV")
    conn.commit()
    fake_map = {
        "IVV": TickerMapEntry(
            cik_padded="0001100663",
            ticker="IVV",
            title="ISHARES TR",
            source="company_tickers_exchange",
        ),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    assert result.matches_found == 1
    assert result.rows_inserted == 1
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier_value FROM external_identifiers WHERE instrument_id = %s",
            (900_030,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "0001100663"


def test_discover_handles_case_insensitive_ticker_lookup(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Instrument ``symbol`` may be stored mixed-case; the SEC map
    is uppercase. Lookup must normalise."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=900_006, symbol="Goog")
    conn.commit()
    fake_map = {
        "GOOG": TickerMapEntry(cik_padded="0001652044", ticker="GOOG", title="Alphabet Inc."),
    }

    result = discover_ciks(conn, ticker_map=fake_map)

    assert result.matches_found == 1
    assert result.rows_inserted == 1
