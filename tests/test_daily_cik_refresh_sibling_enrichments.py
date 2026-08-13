"""Integration tests for daily_cik_refresh's sibling-enrichment
contract after the G8 restructure.

Pre-G8: Stage 6 (MF) only fired on the full-upsert branch — the
304 / hash-unchanged early returns silently skipped it. Stage 7
(exchange directory) was added by G8 with the same "always fires"
contract.

These tests pin the new behaviour:

* Sibling enrichments fire on every equity-side branch (304 /
  hash-unchanged / full-upsert).
* Per-sibling failure is fail-soft: an exception in one enrichment
  rolls back its SAVEPOINT but does NOT cascade into the equity
  upsert or the OTHER enrichment.

Spec: ``docs/superpowers/specs/2026-05-17-g8-company-tickers-exchange-directory.md``.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import patch

import psycopg
import pytest

from app.providers.implementations.sec_edgar import CikMappingResult
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export
from tests.test_daily_cik_refresh_scope import _seed_instrument

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


def _patch_db_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect daily_cik_refresh's settings.database_url to the test DB.

    Mirrors ``test_daily_cik_refresh_scope.py``'s pattern at line 220.
    """
    from app.config import settings
    from tests.fixtures.ebull_test_db import test_database_url

    monkeypatch.setattr(settings, "database_url", test_database_url())


def _seed_aapl_us_equity(conn: psycopg.Connection[tuple]) -> None:
    """Seed an in-universe AAPL instrument so daily_cik_refresh's
    dest-empty invariant doesn't trip when we want to exercise the
    304 / hash-unchanged equity branches with non-empty external_identifiers.
    """
    conn.execute("UPDATE exchanges SET asset_class='us_equity' WHERE exchange_id='4'")
    _seed_instrument(conn, instrument_id=4901, symbol="AAPL", exchange="4")
    conn.execute("DELETE FROM external_identifiers WHERE provider='sec' AND identifier_type='cik'")
    # Seed an existing (sec, cik) row so dest_empty == False on the 304 /
    # hash-unchanged paths. We pin AAPL to its real CIK so the watermark
    # tests have a recognisable identifier_value.
    conn.execute(
        """
        INSERT INTO external_identifiers
            (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (4901, 'sec', 'cik', '0000320193', TRUE)
        ON CONFLICT DO NOTHING
        """
    )
    conn.commit()


def _fake_refresh_exchange_writes_row(conn: psycopg.Connection[Any], *, provider: Any) -> dict[str, int]:
    """Stub that mirrors the real service contract: writes one row +
    returns the counts dict. Lets integration tests assert that the
    directory tables are populated without coupling to the real
    field-parsing logic (unit-tested separately)."""
    del provider
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cik_refresh_exchange_directory
                (cik, ticker, name, exchange, last_seen)
            VALUES ('0000320193', 'AAPL', 'Apple Inc.', 'Nasdaq', NOW())
            ON CONFLICT (cik, ticker) DO UPDATE SET last_seen = NOW()
            """
        )
    return {"fetched": 1, "directory_rows": 1}


def _fake_refresh_mf_writes_row(conn: psycopg.Connection[Any], *, provider: Any) -> dict[str, int]:
    """Mirror of the MF stub for the integration tests."""
    del provider
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cik_refresh_mf_directory
                (class_id, series_id, symbol, trust_cik, last_seen)
            VALUES ('C000000001', 'S000000001', 'TEST', '0000000001', NOW())
            ON CONFLICT (class_id) DO UPDATE SET last_seen = NOW()
            """
        )
    return {"fetched": 1, "directory_rows": 1, "external_identifier_rows": 0}


def _raise_exchange_outage(conn: psycopg.Connection[Any], *, provider: Any) -> dict[str, int]:
    del conn, provider
    raise RuntimeError("simulated SEC outage — exchange directory")


def _raise_mf_outage(conn: psycopg.Connection[Any], *, provider: Any) -> dict[str, int]:
    del conn, provider
    raise RuntimeError("simulated SEC outage — mf directory")


def _directory_counts(conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    mf_row = conn.execute("SELECT COUNT(*) FROM cik_refresh_mf_directory").fetchone()
    exch_row = conn.execute("SELECT COUNT(*) FROM cik_refresh_exchange_directory").fetchone()
    assert mf_row is not None and exch_row is not None
    return int(mf_row[0]), int(exch_row[0])


def _aapl_cik_present(conn: psycopg.Connection[tuple]) -> bool:
    row = conn.execute(
        "SELECT identifier_value FROM external_identifiers "
        "WHERE provider='sec' AND identifier_type='cik' AND instrument_id = 4901"
    ).fetchone()
    return row is not None and row[0] == "0000320193"


# ---------------------------------------------------------------------------
# T1 — sibling enrichments fire on the 304 path
# ---------------------------------------------------------------------------


def test_sibling_enrichments_fire_on_304(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """G8 fix: pre-restructure, the 304 path returned before Stage 6
    fired. After G8, MF + exchange directories populate on 304 too."""
    _patch_db_url(monkeypatch)
    _seed_aapl_us_equity(ebull_test_conn)
    from app.workers.scheduler import daily_cik_refresh

    monkeypatch.setattr(
        "app.workers.scheduler.refresh_exchange_directory",
        _fake_refresh_exchange_writes_row,
    )
    monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", _fake_refresh_mf_writes_row)

    def fake_conditional(self: Any, *, if_modified_since: Any = None) -> Any:
        del self, if_modified_since
        return None  # 304 Not Modified

    with patch(
        "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
        new=fake_conditional,
    ):
        daily_cik_refresh()

    mf_count, exch_count = _directory_counts(ebull_test_conn)
    assert mf_count >= 1
    assert exch_count >= 1
    # Equity side untouched: AAPL CIK still the seeded value.
    assert _aapl_cik_present(ebull_test_conn)


# ---------------------------------------------------------------------------
# T2 — sibling enrichments fire on the hash-unchanged path
# ---------------------------------------------------------------------------


def test_sibling_enrichments_fire_on_hash_unchanged(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_db_url(monkeypatch)
    _seed_aapl_us_equity(ebull_test_conn)
    from app.services.watermarks import set_watermark
    from app.workers.scheduler import daily_cik_refresh

    # Seed a watermark with a known body_hash; the stub provider
    # returns the SAME body_hash so daily_cik_refresh takes the
    # hash-unchanged branch.
    matching_hash = "STEADY_STATE_HASH"
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.tickers",
            key="global",
            watermark="Wed, 17 May 2026 02:00:00 GMT",
            response_hash=matching_hash,
        )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        "app.workers.scheduler.refresh_exchange_directory",
        _fake_refresh_exchange_writes_row,
    )
    monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", _fake_refresh_mf_writes_row)

    def fake_conditional(self: Any, *, if_modified_since: Any = None) -> Any:
        del self, if_modified_since
        return CikMappingResult(
            mapping={"AAPL": "0000320193"},
            last_modified="Wed, 17 May 2026 02:00:00 GMT",
            body_hash=matching_hash,  # matches the seeded watermark
        )

    with patch(
        "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
        new=fake_conditional,
    ):
        daily_cik_refresh()

    mf_count, exch_count = _directory_counts(ebull_test_conn)
    assert mf_count >= 1
    assert exch_count >= 1
    # Equity: AAPL still present (no upsert run, but the seed remains).
    assert _aapl_cik_present(ebull_test_conn)


# ---------------------------------------------------------------------------
# T3 — sibling enrichments fire on the full-upsert path
# ---------------------------------------------------------------------------


def test_sibling_enrichments_fire_on_full_upsert(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_db_url(monkeypatch)
    _seed_aapl_us_equity(ebull_test_conn)
    from app.workers.scheduler import daily_cik_refresh

    monkeypatch.setattr(
        "app.workers.scheduler.refresh_exchange_directory",
        _fake_refresh_exchange_writes_row,
    )
    monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", _fake_refresh_mf_writes_row)

    def fake_conditional(self: Any, *, if_modified_since: Any = None) -> Any:
        del self, if_modified_since
        # Distinct body hash → full upsert branch.
        return CikMappingResult(
            mapping={"AAPL": "0000320193"},
            last_modified="Wed, 17 May 2026 02:00:00 GMT",
            body_hash="FRESH_HASH",
        )

    with patch(
        "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
        new=fake_conditional,
    ):
        daily_cik_refresh()

    mf_count, exch_count = _directory_counts(ebull_test_conn)
    assert mf_count >= 1
    assert exch_count >= 1
    assert _aapl_cik_present(ebull_test_conn)


# ---------------------------------------------------------------------------
# T4 — Stage 7 fail-soft preserves equity + MF
# ---------------------------------------------------------------------------


def test_stage7_failure_preserves_equity_and_mf(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _patch_db_url(monkeypatch)
    _seed_aapl_us_equity(ebull_test_conn)
    from app.workers.scheduler import daily_cik_refresh

    monkeypatch.setattr("app.workers.scheduler.refresh_exchange_directory", _raise_exchange_outage)
    monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", _fake_refresh_mf_writes_row)

    def fake_conditional(self: Any, *, if_modified_since: Any = None) -> Any:
        del self, if_modified_since
        return CikMappingResult(
            mapping={"AAPL": "0000320193"},
            last_modified="Wed, 17 May 2026 02:00:00 GMT",
            body_hash="FRESH_HASH_A",
        )

    with (
        caplog.at_level(logging.ERROR, logger="app.workers.scheduler"),
        patch(
            "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
            new=fake_conditional,
        ),
    ):
        daily_cik_refresh()

    mf_count, exch_count = _directory_counts(ebull_test_conn)
    assert mf_count >= 1
    assert exch_count == 0  # Stage 7 SAVEPOINT rolled back
    assert _aapl_cik_present(ebull_test_conn)
    # Exactly one ERROR record from the exchange try/except.
    exchange_errors = [r for r in caplog.records if "exchange_directory" in r.message and r.levelname == "ERROR"]
    assert len(exchange_errors) == 1


# ---------------------------------------------------------------------------
# T5 — Stage 6 fail-soft preserves equity + Stage 7 (MF parity regression)
# ---------------------------------------------------------------------------


def test_stage6_failure_preserves_equity_and_exchange(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _patch_db_url(monkeypatch)
    _seed_aapl_us_equity(ebull_test_conn)
    from app.workers.scheduler import daily_cik_refresh

    monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", _raise_mf_outage)
    monkeypatch.setattr(
        "app.workers.scheduler.refresh_exchange_directory",
        _fake_refresh_exchange_writes_row,
    )

    def fake_conditional(self: Any, *, if_modified_since: Any = None) -> Any:
        del self, if_modified_since
        return CikMappingResult(
            mapping={"AAPL": "0000320193"},
            last_modified="Wed, 17 May 2026 02:00:00 GMT",
            body_hash="FRESH_HASH_B",
        )

    with (
        caplog.at_level(logging.ERROR, logger="app.workers.scheduler"),
        patch(
            "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
            new=fake_conditional,
        ),
    ):
        daily_cik_refresh()

    mf_count, exch_count = _directory_counts(ebull_test_conn)
    assert mf_count == 0  # Stage 6 SAVEPOINT rolled back
    assert exch_count >= 1
    assert _aapl_cik_present(ebull_test_conn)
    mf_errors = [r for r in caplog.records if "mf_directory" in r.message and r.levelname == "ERROR"]
    assert len(mf_errors) == 1


# ---------------------------------------------------------------------------
# T6 — both stages fail-soft together preserves equity
# ---------------------------------------------------------------------------


def test_both_stages_fail_soft_preserves_equity(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _patch_db_url(monkeypatch)
    _seed_aapl_us_equity(ebull_test_conn)
    from app.workers.scheduler import daily_cik_refresh

    monkeypatch.setattr("app.workers.scheduler.refresh_mf_directory", _raise_mf_outage)
    monkeypatch.setattr("app.workers.scheduler.refresh_exchange_directory", _raise_exchange_outage)

    def fake_conditional(self: Any, *, if_modified_since: Any = None) -> Any:
        del self, if_modified_since
        return CikMappingResult(
            mapping={"AAPL": "0000320193"},
            last_modified="Wed, 17 May 2026 02:00:00 GMT",
            body_hash="FRESH_HASH_C",
        )

    with (
        caplog.at_level(logging.ERROR, logger="app.workers.scheduler"),
        patch(
            "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
            new=fake_conditional,
        ),
    ):
        daily_cik_refresh()

    mf_count, exch_count = _directory_counts(ebull_test_conn)
    assert mf_count == 0
    assert exch_count == 0
    # Equity write survived both sibling failures.
    assert _aapl_cik_present(ebull_test_conn)
    # Two ERROR records total (one per failing sibling).
    sibling_errors = [
        r
        for r in caplog.records
        if r.levelname == "ERROR" and ("mf_directory" in r.message or "exchange_directory" in r.message)
    ]
    assert len(sibling_errors) == 2
