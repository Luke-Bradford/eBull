"""#1822 slice 1b: the reader's SQL against a real schema (spec v6 "Reader", "Population",
"Timeline", "Termination source"). One test per the lean-DB-test rule: every query once."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg

from app.services import ranking_ablation_reader as reader
from app.services.ranking_ablation import MODEL_VERSION
from app.services.strategies.validated_universe import STOCKS_TYPE_DESCRIPTION
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_STOCK, _ETF = 905, 906


def _seed(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        "INSERT INTO etoro_instrument_types (instrument_type_id, description) VALUES (%s, %s), (%s, 'ETF')",
        (_STOCK, STOCKS_TYPE_DESCRIPTION, _ETF),
    )
    conn.execute("INSERT INTO exchanges (exchange_id, asset_class) VALUES ('r1822', 'us_equity')")
    for iid, symbol, type_id, currency in (
        (1, "AAA", _STOCK, "USD"),
        (2, "ETFX", _ETF, "USD"),
        (3, "BTAIQ", _STOCK, "USD"),
    ):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable, "
            "instrument_type_id, currency) VALUES (%s, %s, %s, 'r1822', TRUE, %s, %s)",
            (iid, symbol, symbol, type_id, currency),
        )
    scored = datetime(2026, 8, 3, 13, 0, tzinfo=UTC)
    for iid in (1, 2, 3):
        conn.execute(
            "INSERT INTO scores (instrument_id, model_version, scored_at, rank, quality_score, value_score, "
            "turnaround_score, momentum_score, sentiment_score, confidence_score, raw_total, total_score, "
            "penalties_json, explanation) VALUES (%s, %s, %s, 1, 0.5, 0.4, 0.3, 0.2, 0.1, 0.5, 0.35, 0.35, "
            "'[]'::jsonb, 'confidence: no thesis; defaulting to 0.5')",
            (iid, MODEL_VERSION, scored),
        )
    conn.execute(
        "INSERT INTO scores (instrument_id, model_version, scored_at, rank) VALUES (1, 'v1.1-balanced', %s, 1)",
        (scored,),
    )
    for job, status, finished in (
        (reader.WITNESS_JOB, "success", datetime(2026, 8, 3, 13, 1, tzinfo=UTC)),
        (reader.WITNESS_JOB, "failure", datetime(2026, 8, 3, 13, 2, tzinfo=UTC)),
        (reader.WITNESS_JOB, "running", None),
        ("daily_candle_refresh", "success", datetime(2026, 8, 3, 13, 3, tzinfo=UTC)),
    ):
        conn.execute(
            "INSERT INTO job_runs (job_name, started_at, finished_at, status) VALUES (%s, %s, %s, %s)",
            (job, datetime(2026, 8, 3, 12, 59, tzinfo=UTC), finished, status),
        )
    # Instrument 1: bars on 07-29, 07-31 (the last before the first formation 08-03), 08-03, 08-05.
    for day in (date(2026, 7, 29), date(2026, 7, 31), date(2026, 8, 3), date(2026, 8, 5)):
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "VALUES (1, %s, 10, 11, 9, 10, NULL)",
            (day,),
        )
    # Instrument 3 starts after the first formation: its read starts at its first bar.
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
        "VALUES (3, '2026-08-04', 5, 6, 4, 5, 100)"
    )
    conn.execute(
        "INSERT INTO instrument_cik_history (instrument_id, cik, effective_from, source_event) "
        "VALUES (3, '0000000003', '2020-01-01', 'imported'), (1, '0000000001', '2020-01-01', 'imported')"
    )
    for accession, cik, filed, provision_class, provision in (
        ("a-1", "0000000003", date(2026, 8, 4), "equity_delisting", "(a)(3)"),
        ("a-2", "0000000003", date(2026, 8, 5), "equity_delisting", "(b)"),
        ("a-3", "0000000001", date(2026, 8, 4), "debt_lifecycle", "(b)"),
        ("a-4", "0000000001", date(2026, 7, 1), "equity_delisting", "(b)"),
    ):
        conn.execute(
            "INSERT INTO sec_form25_register (accession_number, form, filed_date, issuer_cik, provision_class, "
            "security_class, rule_provision) VALUES (%s, '25-NSE', %s, %s, %s, 'common_equity', %s)",
            (accession, filed, cik, provision_class, provision),
        )
    # A warrant's equity delisting by the stock's issuer does not end the stock.
    conn.execute(
        "INSERT INTO sec_form25_register (accession_number, form, filed_date, issuer_cik, provision_class, "
        "security_class, rule_provision) VALUES ('a-5', '25-NSE', '2026-08-04', '0000000001', "
        "'equity_delisting', 'warrant', '(b)')"
    )
    conn.commit()


def test_reader_queries(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed(conn)

    population = reader.load_population(conn)
    [scored_at] = population.runs
    assert set(population.runs[scored_at]) == {1, 3}
    assert population.excluded_rows == {"non_stock": 1}  # the v1.1 row is not in the population
    assert population.symbols[3] == "BTAIQ"
    assert not population.runs[scored_at][1].confidence_from_thesis
    assert population.runs[scored_at][1].value_from_thesis

    windows = reader.load_job_windows(conn)
    assert [w.finished_at for w in windows] == [datetime(2026, 8, 3, 13, 1, tzinfo=UTC)]

    series = reader.load_series(
        conn, [1, 3, 99], first_formation=date(2026, 8, 3), cutoff=date(2026, 8, 4), as_of=date(2026, 9, 28)
    )
    assert set(series) == {1, 3}
    assert [bar.price_date for bar in series[1].bars] == [date(2026, 7, 31), date(2026, 8, 3)]
    assert series[1].bars[0].volume is None and series[1].bars[0].close == Decimal(10)
    assert series[1].asset_class == "us_equity"
    assert [bar.price_date for bar in series[3].bars] == [date(2026, 8, 4)]

    links = reader.load_form25_links(conn, [1, 3], first_formation=date(2026, 8, 3), cutoff=date(2026, 8, 5))
    assert links == {3: "(b)"}  # common-equity delistings in the window only; the latest filed wins
