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
    # Capabilities must match the us_equity contract from migration
    # 071/072 — ``test_us_equity_seed_includes_sec_edgar_for_filings``
    # asserts every us_equity row has ``capabilities -> 'filings' =
    # ['sec_edgar']``. Without it the test cross-pollinates and fails
    # in shared pytest sessions where this fixture's rows leak. #797
    # B6 prevention.
    # Full us_equity capability shape (11 keys) per migration 072.
    # ``test_us_equity_seed_full_shape`` asserts every us_equity row
    # has all 11 keys including the empty-list ones; partial seeds
    # drift the test in shared pytest sessions where this fixture's
    # rows leak. #797 B6 prevention.
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class, capabilities)
        VALUES (%s, %s, 'US', 'us_equity', %s::jsonb)
        ON CONFLICT (exchange_id) DO UPDATE SET
            capabilities = EXCLUDED.capabilities
        """,
        (
            f"rfp_{instrument_id}",
            f"Test {instrument_id}",
            (
                '{"filings": ["sec_edgar"], '
                '"fundamentals": ["sec_xbrl"], '
                '"dividends": ["sec_dividend_summary"], '
                '"insider": ["sec_form4"], '
                '"analyst": [], "ratings": [], "esg": [], '
                '"ownership": ["sec_13f", "sec_13d_13g"], '
                '"corporate_events": ["sec_8k_events"], '
                '"business_summary": ["sec_10k_item1"], '
                '"officers": []}'
            ),
        ),
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


def test_upsert_dedupes_duplicate_conflict_keys_in_same_chunk(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Multi-row INSERT VALUES (#763) raises ``CardinalityViolation``
    # when two rows in the same statement hit the same ON CONFLICT
    # key. ``upsert_facts_for_instrument`` must dedupe by conflict
    # key before the INSERT. Codex review medium on PR #764.
    from datetime import date as _date
    from decimal import Decimal

    from app.providers.implementations.sec_fundamentals import XbrlFact
    from app.services.fundamentals import upsert_facts_for_instrument

    _seed_instrument(ebull_test_conn, 982_001, "RFP_DUP", "0000982001")
    # FK to data_ingestion_runs requires a real run id.
    cur = ebull_test_conn.execute(
        """
        INSERT INTO data_ingestion_runs (source, endpoint, instrument_count)
        VALUES ('test', '/api/test', 1)
        RETURNING ingestion_run_id
        """,
    )
    run_row = cur.fetchone()
    assert run_row is not None
    run_id = int(run_row[0])

    # Two facts with IDENTICAL conflict keys but different ``val``
    # values — pre-dedupe-fix this would raise CardinalityViolation.
    common_args = {
        "concept": "Revenues",
        "unit": "USD",
        "period_start": None,
        "period_end": _date(2024, 12, 31),
        "frame": None,
        "accession_number": "acc-dup-1",
        "form_type": "10-K",
        "filed_date": _date(2025, 2, 1),
        "fiscal_year": 2024,
        "fiscal_period": "FY",
        "decimals": None,
        "taxonomy": "us-gaap",
    }
    facts = [
        XbrlFact(val=Decimal(1000), **common_args),
        XbrlFact(val=Decimal(2000), **common_args),  # duplicate conflict key
    ]

    upserted, skipped = upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=982_001,
        facts=facts,
        ingestion_run_id=run_id,
    )

    # Only the deduplicated row lands; last occurrence wins (matches
    # the sequential ON CONFLICT DO UPDATE chain semantics).
    assert upserted == 1
    assert skipped == 0
    cur = ebull_test_conn.execute(
        "SELECT val FROM financial_facts_raw WHERE instrument_id = 982001 AND concept = 'Revenues'"
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == Decimal(2000)


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
