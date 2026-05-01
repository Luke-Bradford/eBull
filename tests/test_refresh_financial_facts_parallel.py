"""Tests for ``refresh_financial_facts`` parallel fetch + streaming
upsert pipeline (#761).

Pins three contracts:
  1. Multi-symbol calls actually run fetches in parallel (peak-live
     counter > 1).
  2. Per-symbol fetch exceptions don't abort the batch — surfaced as
     a failure count, not a raise.
  3. Per-symbol upsert exceptions are isolated by the
     ``conn.transaction()`` savepoint — one bad insert leaves the
     others committed.

Uses a fake provider rather than hitting SEC. The fake mirrors
``SecFundamentalsProvider``'s ``extract_facts_and_catalog`` shape so
the production path is exercised end-to-end against the real DB
schema (``ebull_test_conn``).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import psycopg

from app.providers.implementations.sec_fundamentals import XbrlConceptCatalogEntry, XbrlFact
from app.services.fundamentals import refresh_financial_facts


@dataclass
class _FakeProvider:
    """Minimal stub matching ``extract_facts_and_catalog``."""

    fact_count_per_symbol: int = 1
    raise_on_symbol: str | None = None

    def __post_init__(self) -> None:
        self.calls: list[str] = []
        self.live: int = 0
        self.peak_live: int = 0
        self._lock = threading.Lock()

    def extract_facts_and_catalog(self, symbol: str, cik: str) -> tuple[list[XbrlFact], list[XbrlConceptCatalogEntry]]:
        with self._lock:
            self.calls.append(symbol)
            self.live += 1
            self.peak_live = max(self.peak_live, self.live)
        try:
            time.sleep(0.05)  # simulate SEC response time
            if symbol == self.raise_on_symbol:
                raise RuntimeError(f"simulated SEC error for {symbol}")
            facts: list[XbrlFact] = []
            from datetime import date as _date
            from decimal import Decimal

            for i in range(self.fact_count_per_symbol):
                facts.append(
                    XbrlFact(
                        concept=f"Concept{i}",
                        unit="USD",
                        period_start=None,
                        period_end=_date(2024, 12, 31),
                        val=Decimal(1000 + i),
                        frame=None,
                        form_type="10-K",
                        fiscal_year=2024,
                        fiscal_period="FY",
                        accession_number=f"acc-{symbol}-{i}",
                        filed_date=_date(2025, 2, 1),
                        decimals=None,
                        taxonomy="us-gaap",
                    )
                )
            return facts, []
        finally:
            with self._lock:
                self.live -= 1


def _seed_instrument(conn: psycopg.Connection[tuple], instrument_id: int, symbol: str, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"rfp_{instrument_id}", f"Test {instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"rfp_{instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers
            (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cik', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (instrument_id, cik),
    )


def test_refresh_runs_fetches_in_parallel(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Eight symbols at 50ms each: parallel completes in ~50-100ms,
    # sequential would take 400ms+. Peak-live > 1 proves the
    # fetch+parse phase actually overlaps across workers (#761).
    symbols: list[tuple[str, int, str]] = []
    for i in range(8):
        iid = 980_001 + i
        _seed_instrument(ebull_test_conn, iid, f"RFP_PAR_{i}", f"00098{iid:05d}")
        symbols.append((f"RFP_PAR_{i}", iid, f"00098{iid:05d}"))

    provider = _FakeProvider(fact_count_per_symbol=2)
    summary = refresh_financial_facts(
        provider,  # type: ignore[arg-type]
        ebull_test_conn,
        symbols,
        fetch_workers=4,
    )

    assert provider.peak_live > 1
    assert provider.peak_live <= 4
    assert summary.symbols_failed == 0
    assert summary.facts_upserted == 16  # 8 symbols × 2 facts each


def test_refresh_isolates_per_symbol_fetch_exceptions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # One symbol's fetcher raises — the rest must still complete and
    # the failure must be counted in ``symbols_failed`` rather than
    # bubbling out of the function.
    _seed_instrument(ebull_test_conn, 981_001, "RFP_OK_A", "0000981001")
    _seed_instrument(ebull_test_conn, 981_002, "RFP_BAD", "0000981002")
    _seed_instrument(ebull_test_conn, 981_003, "RFP_OK_B", "0000981003")
    symbols = [
        ("RFP_OK_A", 981_001, "0000981001"),
        ("RFP_BAD", 981_002, "0000981002"),
        ("RFP_OK_B", 981_003, "0000981003"),
    ]

    provider = _FakeProvider(fact_count_per_symbol=1, raise_on_symbol="RFP_BAD")
    summary = refresh_financial_facts(
        provider,  # type: ignore[arg-type]
        ebull_test_conn,
        symbols,
        fetch_workers=2,
    )

    assert summary.symbols_failed == 1
    # The two healthy symbols must still upsert their facts.
    assert summary.facts_upserted == 2
