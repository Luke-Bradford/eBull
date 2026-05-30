"""#1345 PR-B — sweep-wiring regression + fallback tests.

PR-A added the 4 batch MERGE writers but changed no behaviour: the sync
sweep (``ownership_observations_sync``) and the repair sweep
(``ownership_observations_repair``) still refreshed ``_current`` one
instrument at a time. PR-B routes BOTH sweeps through the whole-set
batch writers via :func:`refresh_current_with_batch_fallback`.

The headline win lives in the *wiring*, not the helpers (the helpers are
contract-tested in ``test_ownership_observations_refresh_batch.py``).
The existing ``test_ownership_observations_sync.py`` /
``test_ownership_observations_repair.py`` suites already prove the batch
path produces correct ``_current`` content end-to-end. These tests add
the guards that the contract suites cannot:

  1. **Wiring-regression** — each sweep invokes the *batch* writer ONCE
     with the full id-set (not N per-instrument calls). Without this a
     future refactor silently reverts the wiring while every contract
     test stays green.
  2. **Fallback isolation** — when the atomic batch fails, the
     per-instrument fallback isolates the poison id, refreshes the rest,
     surfaces the failure in the operator-visible sink
     (``SyncSummary.orphans`` / ``CategoryRepairStats.failed_instruments``),
     and leaves the connection usable.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

import app.jobs.ownership_observations_repair as repair_mod
import app.services.ownership_observations_sync as sync_mod
from app.jobs.ownership_observations_repair import (
    _CATEGORIES,
    run_observations_repair_sweep,
)
from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
    refresh_insiders_current_batch,
)
from app.services.ownership_observations_sync import sync_def14a, sync_insiders
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_form4(conn: psycopg.Connection[tuple], *, iid: int, accession: str, filer_cik: str) -> None:
    """Seed one syncable Form 4 row + its filing_events gate row."""
    _seed_instrument(conn, iid=iid, symbol=f"S{iid}")
    conn.execute(
        """
        INSERT INTO insider_filings (accession_number, instrument_id, document_type, issuer_cik)
        VALUES (%s, %s, '4', '0000000789')
        """,
        (accession, iid),
    )
    conn.execute(
        """
        INSERT INTO insider_transactions (
            accession_number, txn_row_num, instrument_id, filer_cik, filer_name,
            txn_date, txn_code, shares, post_transaction_shares, is_derivative
        ) VALUES (%s, 1, %s, %s, 'Insider', '2026-01-21', 'P', 100, 5000, FALSE)
        """,
        (accession, iid, filer_cik),
    )
    conn.execute(
        """
        INSERT INTO filing_events (instrument_id, provider, provider_filing_id, filing_type, filing_date)
        VALUES (%s, 'sec', %s, '4', %s)
        """,
        (iid, accession, date.today() - timedelta(days=30)),
    )


def _record_insider_obs(conn: psycopg.Connection[tuple], *, iid: int, doc: str, accession: str, shares: str) -> None:
    record_insider_observation(
        conn,
        instrument_id=iid,
        holder_cik="0000000001",
        holder_name="Alice",
        ownership_nature="direct",
        source="form4",
        source_document_id=doc,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        period_start=None,
        period_end=date(2026, 1, 1),
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
    )


def _seed_drifted_insider(conn: psycopg.Connection[tuple], *, iid: int) -> None:
    """Drive an instrument into the drifted state: record + refresh (writes
    the refresh_state watermark + _current), then record a SECOND
    observation without a refresh so obs-max > watermark → drift."""
    _seed_instrument(conn, iid=iid, symbol=f"D{iid}")
    _record_insider_obs(conn, iid=iid, doc=f"DOC-{iid}-1", accession=f"0001234527-25-{iid:06d}", shares="100")
    conn.commit()
    refresh_insiders_current(conn, instrument_id=iid)
    conn.commit()
    _record_insider_obs(conn, iid=iid, doc=f"DOC-{iid}-2", accession=f"0001234528-25-{iid:06d}", shares="200")
    conn.commit()


# ---------------------------------------------------------------------------
# Sync sweep wiring
# ---------------------------------------------------------------------------


class TestSyncSweepWiring:
    def test_sync_insiders_routes_through_batch_once(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn = ebull_test_conn
        _seed_form4(conn, iid=845_010, accession="0001767470-26-000010", filer_cik="0001767470")
        conn.commit()

        calls: list[list[int]] = []

        def _spy(c: psycopg.Connection[tuple], *, instrument_ids: list[int]) -> int:
            ids = sorted(int(i) for i in instrument_ids)
            calls.append(ids)
            return len(ids)

        monkeypatch.setattr(sync_mod, "refresh_insiders_current_batch", _spy)

        summary = sync_insiders(conn)
        conn.commit()

        # Exactly ONE batch call carrying the full touched id-set — not N
        # per-instrument calls.
        assert calls == [[845_010]]
        assert summary.instruments_refreshed == 1
        assert summary.orphans == []

    def test_sync_def14a_routes_def14a_and_esop_batches_once(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``sync_def14a`` refreshes TWO categories (def14a general +
        esop) and must route EACH through its own batch writer once —
        the ``+=`` second call is otherwise easy to silently revert."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=845_020, symbol="DEFA")
        _seed_instrument(conn, iid=845_021, symbol="ESOP")
        # General beneficial holder → def14a batch.
        conn.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                accession_number, issuer_cik, holder_name, holder_role,
                shares, percent_of_class, as_of_date, instrument_id
            ) VALUES ('0001234500-26-000020', '0000320193', 'Tim Cook', 'CEO',
                      3300000, 0.02, '2025-12-31', %s)
            """,
            (845_020,),
        )
        # ESOP-named holder → esop batch (name-pattern routing).
        conn.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                accession_number, issuer_cik, holder_name, holder_role,
                shares, percent_of_class, as_of_date, instrument_id
            ) VALUES ('0001234500-26-000021', '0000320193',
                      'Acme Inc. Employee Stock Ownership Plan', 'principal',
                      1000000, 0.05, '2025-12-31', %s)
            """,
            (845_021,),
        )
        conn.commit()

        def14a_calls: list[list[int]] = []
        esop_calls: list[list[int]] = []

        def _def_spy(c: psycopg.Connection[tuple], *, instrument_ids: list[int]) -> int:
            def14a_calls.append(sorted(int(i) for i in instrument_ids))
            return len(list(instrument_ids))

        def _esop_spy(c: psycopg.Connection[tuple], *, instrument_ids: list[int]) -> int:
            esop_calls.append(sorted(int(i) for i in instrument_ids))
            return len(list(instrument_ids))

        monkeypatch.setattr(sync_mod, "refresh_def14a_current_batch", _def_spy)
        monkeypatch.setattr(sync_mod, "refresh_esop_current_batch", _esop_spy)

        summary = sync_def14a(conn)
        conn.commit()

        assert def14a_calls == [[845_020]]
        assert esop_calls == [[845_021]]
        # Both refreshes summed into the single counter (`+=`, not overwrite).
        assert summary.instruments_refreshed == 2
        assert summary.orphans == []

    def test_sync_fallback_isolates_poison_records_orphan(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Batch raises → per-instrument fallback refreshes the healthy
        instrument, records the poison id in ``summary.orphans``, and
        leaves the connection usable."""
        conn = ebull_test_conn
        healthy, poison = 845_030, 845_031
        _seed_form4(conn, iid=healthy, accession="0001767470-26-000030", filer_cik="0001767470")
        _seed_form4(conn, iid=poison, accession="0001767470-26-000031", filer_cik="0001767471")
        conn.commit()

        def _raise_batch(c: psycopg.Connection[tuple], *, instrument_ids: list[int]) -> int:
            raise RuntimeError("synthetic batch failure")

        real_one = refresh_insiders_current

        def _one_with_poison(c: psycopg.Connection[tuple], *, instrument_id: int) -> int:
            if instrument_id == poison:
                raise RuntimeError(f"synthetic poison {instrument_id}")
            return real_one(c, instrument_id=instrument_id)

        monkeypatch.setattr(sync_mod, "refresh_insiders_current_batch", _raise_batch)
        monkeypatch.setattr(sync_mod, "refresh_insiders_current", _one_with_poison)

        summary = sync_insiders(conn)
        conn.commit()

        # Healthy instrument refreshed; poison surfaced once in the sink.
        assert summary.instruments_refreshed == 1
        assert len(summary.orphans) == 1
        assert f"instrument_id={poison}" in summary.orphans[0]

        # Connection usable + healthy _current populated for real.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ownership_insiders_current WHERE instrument_id = %s", (healthy,))
            row = cur.fetchone()
        assert row is not None and row[0] == 1


# ---------------------------------------------------------------------------
# Repair sweep wiring
# ---------------------------------------------------------------------------


class TestRepairSweepWiring:
    def test_categories_pair_batch_and_single_writer(self) -> None:
        """Every ``_CATEGORIES`` row must pair the category's batch writer
        (happy path) with its per-instrument writer (fallback). Guards
        all 7 categories against a future drift in either slot."""
        assert len(_CATEGORIES) == 7
        for _current_table, _obs_table, category_literal, batch_fn, one_fn in _CATEGORIES:
            assert batch_fn.__name__ == f"refresh_{category_literal}_current_batch"
            assert one_fn.__name__ == f"refresh_{category_literal}_current"

    def test_repair_routes_through_batch_once(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn = ebull_test_conn
        _seed_drifted_insider(conn, iid=846_010)
        _seed_drifted_insider(conn, iid=846_011)

        calls: list[list[int]] = []

        def _spy(c: psycopg.Connection[tuple], *, instrument_ids: list[int]) -> int:
            ids = sorted(int(i) for i in instrument_ids)
            calls.append(ids)
            return refresh_insiders_current_batch(c, instrument_ids=ids)

        # _CATEGORIES captured the real fn at import; swap the insiders
        # entry (index 0) for a single-category sweep through the spy.
        insiders = _CATEGORIES[0]
        monkeypatch.setattr(
            repair_mod,
            "_CATEGORIES",
            [(insiders[0], insiders[1], insiders[2], _spy, insiders[4])],
        )

        stats = run_observations_repair_sweep(conn)
        conn.commit()

        # ONE batch call carrying BOTH drifted ids.
        assert calls == [[846_010, 846_011]]
        per = stats.per_category[0]
        assert per.drifted_instruments == 2
        assert per.refreshed_instruments == 2
        assert per.failed_instruments == 0

        # Second sweep is a no-op (drift repaired).
        calls.clear()
        stats2 = run_observations_repair_sweep(conn)
        conn.commit()
        assert stats2.total_drifted == 0
        assert calls == []

    def test_repair_fallback_records_failed_instrument_every_sweep(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A permanently-failing instrument must surface in
        ``failed_instruments`` on EVERY sweep — never masked as transient
        drift-churn (its watermark never advances, so it re-drifts)."""
        conn = ebull_test_conn
        healthy, poison = 846_020, 846_021
        _seed_drifted_insider(conn, iid=healthy)
        _seed_drifted_insider(conn, iid=poison)

        def _raise_batch(c: psycopg.Connection[tuple], *, instrument_ids: list[int]) -> int:
            raise RuntimeError("synthetic batch failure")

        def _one_with_poison(c: psycopg.Connection[tuple], *, instrument_id: int) -> int:
            if instrument_id == poison:
                raise RuntimeError(f"synthetic poison {instrument_id}")
            return refresh_insiders_current(c, instrument_id=instrument_id)

        insiders = _CATEGORIES[0]
        monkeypatch.setattr(
            repair_mod,
            "_CATEGORIES",
            [(insiders[0], insiders[1], insiders[2], _raise_batch, _one_with_poison)],
        )

        stats = run_observations_repair_sweep(conn)
        conn.commit()
        per = stats.per_category[0]
        assert per.drifted_instruments == 2
        assert per.refreshed_instruments == 1  # healthy only
        assert per.failed_instruments == 1  # poison surfaced, not swallowed
        assert stats.total_failed == 1

        # Poison never advanced its watermark → still drifted + still
        # failing on the next sweep (operator-visible, not masked).
        stats2 = run_observations_repair_sweep(conn)
        conn.commit()
        per2 = stats2.per_category[0]
        assert per2.drifted_instruments == 1
        assert per2.failed_instruments == 1

        # Connection usable after the fallback path.
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
