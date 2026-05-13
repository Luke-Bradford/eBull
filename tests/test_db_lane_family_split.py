"""Tests for #1141 / Task E — DB-lane source split by table family.

Acceptance criterion (#1141):
> integration test demonstrating two db stages on disjoint table
> families run concurrently without lock contention

The split replaces the single ``db`` source for Phase C bulk
ingesters with five per-family sources (``db_filings``,
``db_fundamentals_raw``, ``db_ownership_inst``,
``db_ownership_insider``, ``db_ownership_funds``). Each source owns
exactly one Phase C bulk job; the parallelism win is cross-source.

Spec: docs/superpowers/specs/2026-05-13-db-lane-family-split.md
"""

from __future__ import annotations

import pytest
from psycopg import errors as psycopg_errors

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.jobs.sources import get_job_name_to_source, source_for

# Postgres advisory locks are cluster-wide, not database-scoped, and
# the tests below lock real production source keys (notably
# ``job_source:db`` via ``orchestrator_full_sync``). The existing
# ``tests/test_joblock_per_source.py`` locks the same shared keys
# under ``xdist_group="joblock_source_serial"``; reuse that group so
# every test that touches a real source key lands on the SAME xdist
# worker and the only contention is intra-test (which each test
# asserts). A different group name would flake under parallel
# workers as cross-worker advisory-lock collisions appear at random.
pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")


# Family-source assignments (mirror app/services/bootstrap_orchestrator.py
# ``_STAGE_LANE_OVERRIDES``). Pinned here so a future re-merge has to
# update this test in lockstep.
_FAMILY_ASSIGNMENTS: tuple[tuple[str, str], ...] = (
    ("sec_submissions_ingest", "db_filings"),
    ("sec_companyfacts_ingest", "db_fundamentals_raw"),
    ("sec_13f_ingest_from_dataset", "db_ownership_inst"),
    ("sec_insider_ingest_from_dataset", "db_ownership_insider"),
    ("sec_nport_ingest_from_dataset", "db_ownership_funds"),
)


class TestSourceRegistry:
    """JOB_NAME_TO_SOURCE round-trip for every Phase C bulk job."""

    @pytest.mark.parametrize(("job_name", "expected_source"), _FAMILY_ASSIGNMENTS)
    def test_phase_c_job_resolves_to_family_source(self, job_name: str, expected_source: str) -> None:
        assert source_for(job_name) == expected_source

    def test_each_family_source_has_exactly_one_job(self) -> None:
        """Inverse check: each family source maps to exactly one job_name.

        Catches accidental cross-wiring (e.g. two Phase C stages on the
        same family source would re-introduce intra-family
        serialisation that the split is designed to remove).
        """
        registry = get_job_name_to_source()
        family_sources = {assignment[1] for assignment in _FAMILY_ASSIGNMENTS}
        for source in family_sources:
            holders = [name for name, src in registry.items() if src == source]
            assert holders == [next(name for name, expected in _FAMILY_ASSIGNMENTS if expected == source)], (
                f"family source {source!r} owned by {holders!r}; expected exactly one Phase C job"
            )


class TestCrossFamilyConcurrency:
    """The acceptance criterion: cross-family JobLocks run concurrently."""

    def test_two_disjoint_families_run_concurrently(self) -> None:
        """``sec_submissions_ingest`` (``db_filings``) and
        ``sec_13f_ingest_from_dataset`` (``db_ownership_inst``) write
        disjoint table families. Both ``JobLock``s must succeed."""
        with JobLock(settings.database_url, "sec_submissions_ingest"):
            with JobLock(settings.database_url, "sec_13f_ingest_from_dataset"):
                # Both held simultaneously — no exception means success.
                pass

    def test_all_five_phase_c_families_run_concurrently(self) -> None:
        """Exhaustive — every Phase C family lock is disjoint from every
        other. Sequentially stack ``JobLock``s and assert each acquires
        without raising ``JobAlreadyRunning``."""
        from contextlib import ExitStack

        with ExitStack() as stack:
            for job_name, _source in _FAMILY_ASSIGNMENTS:
                stack.enter_context(JobLock(settings.database_url, job_name))
            # Reached only if every JobLock acquired without raising.

    def test_family_source_disjoint_from_db_source(self) -> None:
        """A Phase C family lock + a scheduler ``db``-source lock run
        in parallel — this is the "accepted loss of incidental ``db``
        serialisation" called out in the spec. Pinned so a future
        re-merge has to fight a red test.
        """
        with JobLock(settings.database_url, "sec_submissions_ingest"):  # db_filings
            with JobLock(settings.database_url, "orchestrator_full_sync"):  # db
                pass


class TestIntraFamilySerialisation:
    """Splitting the source MUST NOT relax same-source serialisation."""

    def test_same_family_source_still_serialises(self) -> None:
        """Two acquires of the same family source must contend.

        Each family source today owns exactly one job (see
        ``TestSourceRegistry::test_each_family_source_has_exactly_one_job``)
        so the only way to acquire the same source twice is to lock the
        same job_name twice. Exercises the source-keyed JobLock
        invariant (#1064) inside the family lane.
        """
        with JobLock(settings.database_url, "sec_submissions_ingest"):
            with pytest.raises(JobAlreadyRunning):
                with JobLock(settings.database_url, "sec_submissions_ingest"):
                    pytest.fail("re-acquiring db_filings source should have raised")


_FAMILY_LANES: tuple[str, ...] = tuple(source for _job, source in _FAMILY_ASSIGNMENTS)


class TestLaneCheckConstraint:
    """sql/147 extends bootstrap_stages.lane CHECK with the family lanes."""

    @pytest.mark.parametrize("lane_value", _FAMILY_LANES)
    def test_each_family_lane_accepted(self, ebull_test_conn, lane_value: str) -> None:  # type: ignore[no-untyped-def]
        """Every family lane added by sql/147 must satisfy the CHECK
        constraint. Parametrised so a missing entry in the CHECK is
        caught for ANY of the five family lanes, not just one."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("INSERT INTO bootstrap_runs DEFAULT VALUES RETURNING id")
            row = cur.fetchone()
            assert row is not None
            run_id = row[0]
            cur.execute(
                """
                INSERT INTO bootstrap_stages
                    (bootstrap_run_id, stage_key, stage_order, lane, job_name)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (run_id, f"fixture_family_lane_{lane_value}", 1, lane_value, "fixture_job"),
            )

    def test_legacy_db_lane_still_accepted(self, ebull_test_conn) -> None:  # type: ignore[no-untyped-def]
        """``lane='db'`` remains valid — Phase E derivations stay on it."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("INSERT INTO bootstrap_runs DEFAULT VALUES RETURNING id")
            row = cur.fetchone()
            assert row is not None
            run_id = row[0]
            cur.execute(
                """
                INSERT INTO bootstrap_stages
                    (bootstrap_run_id, stage_key, stage_order, lane, job_name)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (run_id, "fixture_db_lane_ok", 1, "db", "fixture_job"),
            )

    def test_garbage_lane_rejected(self, ebull_test_conn) -> None:  # type: ignore[no-untyped-def]
        """A lane name outside the CHECK vocabulary must raise
        ``CheckViolation`` — the migration's defensive boundary.

        The failing INSERT is wrapped in ``ebull_test_conn.transaction()``
        so the resulting tx-abort is scoped to a SAVEPOINT, NOT the
        outer implicit psycopg3 tx. Without the savepoint, raising
        ``CheckViolation`` inside an already-open implicit tx leaves
        the connection in ``InFailedSqlTransaction`` state — the
        fixture's per-test ``conn.rollback()`` teardown clears it, but
        any further statement on this cursor inside the same test
        would fail silently. PR #1150 review WARNING. See
        ``docs/review-prevention-log.md`` —
        "Aborted tx after pytest.raises(CheckViolation) inside an
        open implicit psycopg3 tx".
        """
        with ebull_test_conn.cursor() as cur:
            cur.execute("INSERT INTO bootstrap_runs DEFAULT VALUES RETURNING id")
            row = cur.fetchone()
            assert row is not None
            run_id = row[0]
            with pytest.raises(psycopg_errors.CheckViolation):
                with ebull_test_conn.transaction():
                    cur.execute(
                        """
                        INSERT INTO bootstrap_stages
                            (bootstrap_run_id, stage_key, stage_order, lane, job_name)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (run_id, "fixture_garbage_lane", 1, "garbage_lane", "fixture_job"),
                    )
            # Outer tx still alive — the SAVEPOINT absorbed the abort.
            # Prove it: a subsequent statement on the same cursor must
            # succeed against the un-aborted bootstrap_runs row.
            cur.execute(
                "SELECT 1 FROM bootstrap_runs WHERE id = %s",
                (run_id,),
            )
            assert cur.fetchone() is not None, (
                "outer tx was aborted by the CheckViolation — savepoint did not contain the failure"
            )
