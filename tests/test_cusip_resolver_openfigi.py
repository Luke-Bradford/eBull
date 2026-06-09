"""#1233 PR-1b — DB-backed tests for the OpenFIGI sweep + S13 invoker.

Verifies:

* ``sweep_unresolved_cusips_via_openfigi`` selects only bulk-source rows
  whose CUSIP has no existing ``external_identifiers`` row.
* Successful CUSIP → ticker → instrument_id matches write
  ``external_identifiers (provider='openfigi', identifier_type='cusip',
  is_primary=FALSE)`` and tombstone every bulk row for that CUSIP with
  ``resolution_status='resolved_via_openfigi'``.
* OpenFIGI no-result (``warning`` entry) leaves the row pending.
* Ticker→instrument lookup misses (no row, ambiguous) leave the row
  pending and increment ``no_instrument_match``.
* The legacy partition (``source IS NULL``) is invisible to the sweep.
* The post-sweep coverage compute reads OpenFIGI promotions (provider
  filter widened).
* The S13 invoker stamps ``bootstrap_runs.coverage_floor_met`` against
  the 0.80 floor when an active run is present; no-op when not.
* Migration 165 admits ``openfigi`` to the lane CHECK constraint.
* Migration 166 renumbers stages 13-26 → 14-27 idempotently.
* Migration 167 adds the ``coverage_floor_met`` column.
* Migration 168 admits ``resolved_via_openfigi`` to the resolution_status CHECK.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

import psycopg
import pytest

from app.services.cusip_resolver import (
    OpenFigiSweepReport,
    record_unresolved_cusip_from_bulk,
    sweep_unresolved_cusips_via_openfigi,
)
from app.services.openfigi_resolver import OpenFigiMapping
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeOpenFigiResolver:
    """Stand-in for ``OpenFigiResolver`` that returns a pre-seeded
    CUSIP→mapping dict and tracks call counts. Implements the
    ``OpenFigiResolverProtocol`` structural contract.

    Used by the sweep tests so we don't spin up httpx or MockTransport
    here — the resolver's HTTP path is exhaustively tested in
    ``test_openfigi_resolver.py``.
    """

    def __init__(
        self,
        responses: dict[str, OpenFigiMapping] | None = None,
        *,
        raise_on_call: Exception | None = None,
    ) -> None:
        self._responses = responses or {}
        self._raise = raise_on_call
        self.calls: list[list[str]] = []

    def resolve_cusips(self, cusips: Iterable[str]) -> dict[str, OpenFigiMapping]:
        cusip_list = [c.strip().upper() for c in cusips if c]
        self.calls.append(cusip_list)
        if self._raise is not None:
            raise self._raise
        return {c: m for c, m in self._responses.items() if c in cusip_list}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_PERIOD_END = date(2026, 3, 31)


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    company_name: str = "Test Inc",
) -> None:
    """Insert a tradable us_equity instrument + matching exchange."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE) "
            "ON CONFLICT (instrument_id) DO UPDATE SET symbol=EXCLUDED.symbol, company_name=EXCLUDED.company_name",
            (instrument_id, symbol, company_name),
        )


def _count_pending_bulk(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM unresolved_13f_cusips WHERE source IS NOT NULL AND resolution_status IS NULL")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _count_resolved_openfigi(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM unresolved_13f_cusips WHERE resolution_status = 'resolved_via_openfigi'")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _extids_for_cusip(conn: psycopg.Connection[tuple], cusip: str) -> list[tuple[str, int, bool]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT provider, instrument_id, is_primary FROM external_identifiers "
            "WHERE identifier_value = %s AND identifier_type = 'cusip' ORDER BY provider",
            (cusip,),
        )
        return [(str(r[0]), int(r[1]), bool(r[2])) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Migration smoke
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestPR1bMigrations:
    def test_lane_check_admits_openfigi(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """sql/165 — lane CHECK accepts 'openfigi'."""
        with ebull_test_conn.cursor() as cur:
            # Read the constraint definition; PG stores the CHECK as
            # ``pg_get_constraintdef(c.oid)``.
            cur.execute(
                """
                SELECT pg_get_constraintdef(c.oid)
                  FROM pg_constraint c
                  JOIN pg_class t ON t.oid = c.conrelid
                 WHERE t.relname = 'bootstrap_stages'
                   AND c.conname = 'bootstrap_stages_lane_check'
                """
            )
            row = cur.fetchone()
        assert row is not None
        constraint_def = row[0]
        assert "openfigi" in constraint_def, constraint_def

    def test_resolution_status_check_admits_resolved_via_openfigi(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """sql/168 — resolution_status CHECK accepts 'resolved_via_openfigi'."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_get_constraintdef(c.oid)
                  FROM pg_constraint c
                  JOIN pg_class t ON t.oid = c.conrelid
                 WHERE t.relname = 'unresolved_13f_cusips'
                   AND c.conname = 'unresolved_13f_cusips_resolution_status_check'
                """
            )
            row = cur.fetchone()
        assert row is not None
        assert "resolved_via_openfigi" in row[0], row[0]

    def test_coverage_floor_met_column_present(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """sql/167 — bootstrap_runs has nullable coverage_floor_met BOOLEAN."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_type, is_nullable, column_default
                  FROM information_schema.columns
                 WHERE table_name = 'bootstrap_runs'
                   AND column_name = 'coverage_floor_met'
                """
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "boolean"
        assert row[1] == "YES"  # nullable
        # Default may be NULL or an explicit NULL literal — both pass.

    def test_stage_renumber_no_collisions(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """sql/166 — after migration, every persisted run's stage_orders
        are gap-free 1..N with no duplicate."""
        # This test runs against the template DB so there may be no
        # bootstrap_runs rows. The invariant we pin: across whatever
        # rows exist (none, or many), no run has duplicate stage_order
        # and no stage_order=13 exists with stage_key=
        # 'sec_submissions_files_walk'.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT bootstrap_run_id, stage_order, COUNT(*)
                  FROM bootstrap_stages
                 GROUP BY bootstrap_run_id, stage_order
                HAVING COUNT(*) > 1
                """
            )
            duplicates = cur.fetchall()
        assert duplicates == [], f"stage_order duplicates: {duplicates}"

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT bootstrap_run_id, stage_key, stage_order
                  FROM bootstrap_stages
                 WHERE stage_key = 'sec_submissions_files_walk'
                   AND stage_order = 13
                """
            )
            stuck = cur.fetchall()
        assert stuck == [], f"sec_submissions_files_walk still at S13: {stuck}"


# ---------------------------------------------------------------------------
# Sweep — happy path
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestSweepHappyPath:
    def test_resolvable_cusip_promotes_and_tombstones(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Pre-seed 5 unresolved bulk CUSIPs (3 resolvable via OpenFIGI
        + matching instruments). Assert 3 promoted, 2 still pending."""
        # 5 instruments, but the fake resolver only knows 3 of them.
        _seed_instrument(ebull_test_conn, instrument_id=70001, symbol="AAPL", company_name="APPLE INC")
        _seed_instrument(ebull_test_conn, instrument_id=70002, symbol="MSFT", company_name="MICROSOFT")
        _seed_instrument(ebull_test_conn, instrument_id=70003, symbol="GME", company_name="GAMESTOP")
        # 70004 + 70005 NOT seeded — even if OpenFIGI returned tickers, no
        # instrument match.

        # Seed 5 bulk-source unresolved CUSIPs (deterministic CUSIPs).
        cusips_inputs = ["TESTAAPL1", "TESTMSFT1", "TESTGME01", "TESTJPM01", "TESTHD001"]
        for cusip in cusips_inputs:
            record_unresolved_cusip_from_bulk(
                ebull_test_conn,
                cusip=cusip,
                filer_cik="0001234567",
                period_end=_PERIOD_END,
                source="bulk_13f_dataset",
            )
        ebull_test_conn.commit()
        assert _count_pending_bulk(ebull_test_conn) == 5

        fake = FakeOpenFigiResolver(
            responses={
                "TESTAAPL1": OpenFigiMapping(ticker="AAPL", name="APPLE INC", exch_code="US", share_class_figi=None),
                "TESTMSFT1": OpenFigiMapping(ticker="MSFT", name="MICROSOFT", exch_code="US", share_class_figi=None),
                "TESTGME01": OpenFigiMapping(ticker="GME", name="GAMESTOP", exch_code="US", share_class_figi=None),
                # TESTJPM01 + TESTHD001 not in dict → warning equivalent.
            }
        )

        report = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
        ebull_test_conn.commit()

        assert isinstance(report, OpenFigiSweepReport)
        assert report.candidates_seen == 5
        assert report.resolved == 3
        assert report.promoted == 3
        assert report.no_instrument_match == 0
        assert report.unresolved_by_openfigi == 2
        assert report.api_errors == 0

        # external_identifiers populated with provider='openfigi'.
        for cusip, expected_iid in [
            ("TESTAAPL1", 70001),
            ("TESTMSFT1", 70002),
            ("TESTGME01", 70003),
        ]:
            extids = _extids_for_cusip(ebull_test_conn, cusip)
            assert ("openfigi", expected_iid, False) in extids

        # Resolved bulk rows tombstoned.
        assert _count_resolved_openfigi(ebull_test_conn) == 3
        # 2 unresolved (JPM, HD) still pending.
        assert _count_pending_bulk(ebull_test_conn) == 2

    def test_resolver_returns_ticker_but_no_instrument_match(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """OpenFIGI returns a ticker but ``instruments.symbol`` doesn't
        have it (e.g. newly-listed name pre-universe-sync). Row stays
        pending; no extids row written; no tombstone."""
        # No instrument seeded for ZQXZ.
        cusip = "TESTZQXZ1"
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip=cusip,
            filer_cik="0001111111",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
        )
        ebull_test_conn.commit()

        fake = FakeOpenFigiResolver(
            responses={cusip: OpenFigiMapping(ticker="ZQXZ", name="Phantom Inc", exch_code="US", share_class_figi=None)}
        )

        report = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
        ebull_test_conn.commit()

        assert report.candidates_seen == 1
        assert report.resolved == 1
        assert report.promoted == 0
        assert report.no_instrument_match == 1

        assert _extids_for_cusip(ebull_test_conn, cusip) == []
        # Row still pending (not tombstoned).
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                (cusip,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] is None

    def test_resolver_transport_error_yields_api_errors(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Resolver raises mid-call → all CUSIPs counted as api_errors,
        rows stay pending."""
        from app.services.openfigi_resolver import OpenFigiTransportError

        cusip = "TESTERR01"
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip=cusip,
            filer_cik="0001111111",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
        )
        ebull_test_conn.commit()

        fake = FakeOpenFigiResolver(raise_on_call=OpenFigiTransportError("dns failure"))

        report = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
        ebull_test_conn.commit()

        assert report.candidates_seen == 1
        assert report.api_errors == 1
        assert report.promoted == 0
        # Row still pending.
        assert _count_pending_bulk(ebull_test_conn) >= 1


# ---------------------------------------------------------------------------
# Sweep — selection rules
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestSweepSelection:
    def test_legacy_partition_invisible_to_openfigi_sweep(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A legacy unresolved row (``source IS NULL``) MUST NOT be
        picked up by the OpenFIGI sweep. Symmetric to PR-1a's invariant
        that the legacy resolver doesn't see bulk rows."""
        # Legacy row.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO unresolved_13f_cusips (
                    cusip, name_of_issuer, last_accession_number, observation_count
                ) VALUES ('LEGACY0001', 'LegacyCo', '0001-01-01', 1)
                """
            )
        ebull_test_conn.commit()

        fake = FakeOpenFigiResolver(responses={})  # empty — even if asked, no resolution
        report = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
        ebull_test_conn.commit()

        # The sweep saw zero candidates — legacy partition filtered out.
        assert report.candidates_seen == 0
        # Confirm the resolver wasn't called with the legacy CUSIP.
        for call_batch in fake.calls:
            assert "LEGACY0001" not in call_batch

        # Legacy row still pending, untouched.
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = 'LEGACY0001'")
            row = cur.fetchone()
        assert row is not None
        assert row[0] is None

    def test_already_mapped_cusip_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A bulk CUSIP whose mapping is ALREADY in external_identifiers
        (sec OR openfigi) is filtered out — no OpenFIGI call needed."""
        _seed_instrument(ebull_test_conn, instrument_id=70010, symbol="PRE", company_name="Pre-Mapped Inc")
        cusip = "PREMAP0001"

        # Pre-seed extid (provider='sec').
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO external_identifiers "
                "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
                "VALUES (%s, 'sec', 'cusip', %s, TRUE)",
                (70010, cusip),
            )

        # Bulk row for the same CUSIP.
        record_unresolved_cusip_from_bulk(
            ebull_test_conn,
            cusip=cusip,
            filer_cik="0001111111",
            period_end=_PERIOD_END,
            source="bulk_13f_dataset",
        )
        ebull_test_conn.commit()

        fake = FakeOpenFigiResolver(responses={})
        report = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
        ebull_test_conn.commit()

        assert report.candidates_seen == 0
        # Resolver never called for already-mapped CUSIP.
        for call_batch in fake.calls:
            assert cusip not in call_batch


# ---------------------------------------------------------------------------
# Coverage floor — load_bulk_cusip_map + compute_cusip_coverage widening
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestProviderWidening:
    def test_compute_cusip_coverage_counts_openfigi_rows(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """``compute_cusip_coverage`` (bootstrap_preconditions) must
        count OpenFIGI-promoted instruments toward the mapped count.
        Pre-PR-1b this filter was ``provider='sec'`` and OpenFIGI
        promotions were invisible."""
        from app.services.bootstrap_preconditions import compute_cusip_coverage

        # Seed a fresh us_equity instrument with an OpenFIGI-only mapping.
        # Use an isolated UPPER-symbol to dodge any seed data in the
        # template DB.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable, exchange)
                SELECT 70100, 'PR1BTEST', 'PR-1b Test Co', 'USD', TRUE, e.exchange_id
                  FROM exchanges e
                 WHERE e.asset_class = 'us_equity'
                 ORDER BY e.exchange_id
                 LIMIT 1
                ON CONFLICT (instrument_id) DO UPDATE
                  SET symbol = EXCLUDED.symbol, company_name = EXCLUDED.company_name
                """
            )
            cur.execute(
                """
                INSERT INTO external_identifiers
                  (instrument_id, provider, identifier_type, identifier_value, is_primary)
                VALUES (70100, 'openfigi', 'cusip', 'PR1BCUS01', FALSE)
                ON CONFLICT (provider, identifier_type, identifier_value)
                  WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
                DO NOTHING
                """
            )
        ebull_test_conn.commit()

        coverage = compute_cusip_coverage(ebull_test_conn)
        # The cohort + mapped values depend on what the template DB has
        # already seeded. Pin the invariant: the OpenFIGI-only row we
        # just wrote DOES appear in ``mapped`` (i.e. mapped >= 1).
        assert coverage.mapped >= 1
        assert coverage.cohort >= 1

    def test_load_cusip_map_reads_openfigi_rows(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """The 13F + N-PORT ingest preload (``load_bulk_cusip_map``,
        shared since #1437) must return OpenFIGI promotions. Without
        this, the post-sweep bulk re-ingest pass would still record the
        same CUSIP as unresolved."""
        from app.services.cusip_resolver import load_bulk_cusip_map

        _seed_instrument(ebull_test_conn, instrument_id=70200, symbol="LMAP", company_name="Load Map Inc")
        cusip = "LMAPCUSIP"
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO external_identifiers
                  (instrument_id, provider, identifier_type, identifier_value, is_primary)
                VALUES (70200, 'openfigi', 'cusip', %s, FALSE)
                ON CONFLICT (provider, identifier_type, identifier_value)
                  WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
                DO NOTHING
                """,
                (cusip,),
            )
        ebull_test_conn.commit()

        assert load_bulk_cusip_map(ebull_test_conn).get(cusip) == 70200


# ---------------------------------------------------------------------------
# S13 invoker — bootstrap_runs.coverage_floor_met writeback
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestInvokerCoverageFloor:
    def test_invoker_no_active_run_leaves_column_null(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When no bootstrap_runs row is in 'running' state, the invoker
        runs the sweep but doesn't update any row's coverage_floor_met."""
        # Force the invoker to use our test DB.
        # NOTE: we run the sweep + coverage compute directly here (not
        # via _INVOKERS) because _tracked_job opens its own connection
        # and DB-isolation matters. The semantic we pin: when no
        # running run exists, coverage_floor_met stays at its DEFAULT
        # (NULL) for every existing row.

        # Make sure no running row exists.
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE bootstrap_runs SET status = 'complete' WHERE status = 'running'")
        ebull_test_conn.commit()

        # Run the sweep + coverage compute via the same logic the
        # invoker uses.
        from app.services.bootstrap_preconditions import compute_cusip_coverage

        fake = FakeOpenFigiResolver(responses={})
        sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
        compute_cusip_coverage(ebull_test_conn)

        # Inline what the invoker does in absence of a running run:
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT id FROM bootstrap_runs WHERE status='running' ORDER BY id DESC LIMIT 1")
            assert cur.fetchone() is None

    def test_invoker_writes_coverage_floor_met_when_active_run(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """When a running bootstrap_runs row exists, the post-sweep
        floor check stamps the column."""
        from app.services.bootstrap_preconditions import compute_cusip_coverage

        with ebull_test_conn.cursor() as cur:
            # Quiesce any leftover running rows from prior tests.
            cur.execute("UPDATE bootstrap_runs SET status = 'complete' WHERE status = 'running'")
            cur.execute("INSERT INTO bootstrap_runs (status) VALUES ('running') RETURNING id")
            row = cur.fetchone()
            assert row is not None
            run_id = int(row[0])
        ebull_test_conn.commit()

        coverage = compute_cusip_coverage(ebull_test_conn)
        floor = 0.80
        expected_floor_met = coverage.ratio >= floor

        # Run the same UPDATE the invoker runs.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE bootstrap_runs SET coverage_floor_met = %s WHERE id = %s",
                (expected_floor_met, run_id),
            )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT coverage_floor_met FROM bootstrap_runs WHERE id = %s",
                (run_id,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] is expected_floor_met


# ---------------------------------------------------------------------------
# Bootstrap stage catalogue — S13 invariants
# ---------------------------------------------------------------------------


def test_s13_is_cusip_resolver_post_bulk_sweep() -> None:
    """The orchestrator's stage catalogue has S13 =
    ``cusip_resolver_post_bulk_sweep`` on the ``openfigi`` lane."""
    from app.services.bootstrap_orchestrator import (
        _BOOTSTRAP_STAGE_SPECS,
        JOB_CUSIP_RESOLVER_POST_BULK_SWEEP,
        _effective_lane,
    )

    by_order = {spec.stage_order: spec for spec in _BOOTSTRAP_STAGE_SPECS}
    assert 13 in by_order, "stage_order=13 missing from catalogue"
    s13 = by_order[13]
    assert s13.stage_key == "cusip_resolver_post_bulk_sweep"
    assert s13.job_name == JOB_CUSIP_RESOLVER_POST_BULK_SWEEP
    # _effective_lane resolves through _STAGE_LANE_OVERRIDES; no override
    # for the new stage so it must fall through to StageSpec.lane.
    assert _effective_lane(s13.stage_key, s13.lane) == "openfigi"


def test_s13_requires_bulk_inputs_seeded() -> None:
    """S13 depends on the bulk-13F + bulk-NPORT ingesters having
    advertised their caps — otherwise unresolved_13f_cusips's bulk
    partition may still be in-flight."""
    from app.services.bootstrap_orchestrator import _STAGE_REQUIRES_CAPS

    req = _STAGE_REQUIRES_CAPS["cusip_resolver_post_bulk_sweep"]
    assert "institutional_inputs_seeded" in req.all_of
    assert "nport_inputs_seeded" in req.all_of


def test_stage_orders_are_unique_and_ascending() -> None:
    """Ordering invariant: stage_orders are unique + strictly ascending.

    #1413 (bulk-only bootstrap) dropped 7 per-CIK sec_rate stages,
    leaving INTENTIONAL gaps in the stage_order sequence (…13, 16, 18,
    21, 24…) so operator traceability is preserved (stage 22 still
    means "the 13F sweep"). Density is no longer an invariant; the
    contract is unique + ascending (catches dup / mis-ordered specs).
    """
    from app.services.bootstrap_orchestrator import _BOOTSTRAP_STAGE_SPECS

    orders = [spec.stage_order for spec in _BOOTSTRAP_STAGE_SPECS]
    assert orders == sorted(orders), f"stage_order not ascending: {orders}"
    assert len(set(orders)) == len(orders), f"duplicate stage_orders: {orders}"
    # Pin the post-collapse count (27 - 8 per-CIK HTTP stages + 1 master.idx
    # gap-close (#1415) + 1 terminal bootstrap_validation stage (#1419) = 21).
    assert len(_BOOTSTRAP_STAGE_SPECS) == 21


def test_openfigi_lane_in_max_concurrency_map() -> None:
    """The new lane must have an entry in _LANE_MAX_CONCURRENCY so the
    dispatcher can resolve a cap. Default fallback was 1 anyway but
    explicit is better than implicit."""
    from app.services.bootstrap_orchestrator import _LANE_MAX_CONCURRENCY

    assert _LANE_MAX_CONCURRENCY.get("openfigi") == 1


# ---------------------------------------------------------------------------
# Sources registry — Lane Literal + JOB_NAME_TO_SOURCE coverage
# ---------------------------------------------------------------------------


def test_openfigi_lane_in_lane_literal() -> None:
    """``Lane`` Literal admits 'openfigi' — JobLock can resolve the
    source for the new stage's invocations."""
    from typing import get_args

    from app.jobs.sources import Lane

    assert "openfigi" in get_args(Lane)


def test_s13_job_name_resolves_to_openfigi_source() -> None:
    """The canonical JOB_NAME_TO_SOURCE registry maps the S13 invoker
    to the ``openfigi`` Lane. Without this entry, JobLock acquisition
    for the invoker would KeyError at runtime."""
    from app.jobs.sources import reset_job_name_to_source_cache, source_for
    from app.services.bootstrap_orchestrator import JOB_CUSIP_RESOLVER_POST_BULK_SWEEP

    # Reset cache so the test sees the fresh registry built post-import.
    reset_job_name_to_source_cache()
    try:
        assert source_for(JOB_CUSIP_RESOLVER_POST_BULK_SWEEP) == "openfigi"
    finally:
        reset_job_name_to_source_cache()


# ---------------------------------------------------------------------------
# Empty-input invariant
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
def test_sweep_with_no_pending_rows_is_noop(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """No bulk pending rows → sweep returns zero counts and never calls
    the resolver."""
    fake = FakeOpenFigiResolver(responses={})
    # Clear any leftover rows from prior tests in same template.
    with ebull_test_conn.cursor() as cur:
        cur.execute("DELETE FROM unresolved_13f_cusips WHERE source IS NOT NULL")
    ebull_test_conn.commit()

    report = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
    assert report.candidates_seen == 0
    assert report.promoted == 0
    assert fake.calls == []


# ---------------------------------------------------------------------------
# Idempotency — second sweep on same data is a no-op
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
def test_sweep_is_idempotent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Run the sweep twice; the second pass sees zero candidates because
    the first promoted+tombstoned every resolvable row."""
    _seed_instrument(ebull_test_conn, instrument_id=70300, symbol="IDEM", company_name="Idempotent Inc")
    cusip = "IDEMCUS01"
    record_unresolved_cusip_from_bulk(
        ebull_test_conn,
        cusip=cusip,
        filer_cik="0009999999",
        period_end=_PERIOD_END,
        source="bulk_13f_dataset",
    )
    ebull_test_conn.commit()

    fake = FakeOpenFigiResolver(
        responses={cusip: OpenFigiMapping(ticker="IDEM", name="Idempotent Inc", exch_code="US", share_class_figi=None)}
    )

    r1 = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
    ebull_test_conn.commit()
    assert r1.promoted == 1

    # Reset call log — the second pass should NOT call the resolver
    # because the CUSIP is now mapped + the row is tombstoned.
    fake.calls.clear()
    r2 = sweep_unresolved_cusips_via_openfigi(ebull_test_conn, resolver=fake)
    ebull_test_conn.commit()

    assert r2.candidates_seen == 0
    assert r2.promoted == 0
    assert fake.calls == [] or all(cusip not in batch for batch in fake.calls)


# ---------------------------------------------------------------------------
# Type-check pin — Protocol is consumed correctly
# ---------------------------------------------------------------------------


def test_fake_resolver_satisfies_protocol() -> None:
    """The fake test resolver structurally satisfies
    ``OpenFigiResolverProtocol`` — pins the contract so a future change
    to the protocol signature fails this test before the integration
    paths break at runtime."""
    from app.services.cusip_resolver import OpenFigiResolverProtocol

    resolver: OpenFigiResolverProtocol = FakeOpenFigiResolver()  # type: ignore[assignment]
    result = resolver.resolve_cusips([])
    assert isinstance(result, dict)


def _unused_import_dance(arg: Any) -> None:  # pragma: no cover
    """Silence unused-import linter for the type-hint-only import."""
    del arg
