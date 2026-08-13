"""Post-bootstrap auto-current activation (#1511 / T5) — parts (b) + (c).

Pure-predicate tests pin the candidate-selection logic; DB-backed tests pin the
audited enqueue, the double-fire guard, and the empty-by-construction (b) set
against the live ``SCHEDULED_JOBS`` registry.
"""

from __future__ import annotations

from types import SimpleNamespace

import psycopg

from app.services.processes import post_bootstrap_activation as pba
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn
from app.workers.scheduler import JOB_CUSIP_EXTID_SWEEP

_TRAP_SKIP = ("skipped", "bootstrap_not_complete")


def _job(*, catch_up: bool, prerequisite: object = None, exempt: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        name="x",
        catch_up_on_boot=catch_up,
        prerequisite=prerequisite,
        exempt_from_universal_bootstrap_gate=exempt,
    )


def _seed_gate_skip(conn: psycopg.Connection[tuple], job_name: str) -> None:
    """Insert the exact gate-skip row ``record_job_skip`` writes."""
    conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count, error_msg)
        VALUES (%s, now(), now(), 'skipped', 0, 'bootstrap_not_complete')
        """,
        (job_name,),
    )


# --- pure predicates ----------------------------------------------------


def test_catch_up_trap_matches_only_gate_skip() -> None:
    job = _job(catch_up=True)
    assert pba._is_catch_up_trap(job, _TRAP_SKIP) is True
    # benign skip reason (none-vs-skipped, prevention 249) is NOT the trap.
    assert pba._is_catch_up_trap(job, ("skipped", "no candidates")) is False
    assert pba._is_catch_up_trap(job, ("success", None)) is False
    assert pba._is_catch_up_trap(job, None) is False
    # a non-catch_up job on the same skip is not a (c) candidate.
    assert pba._is_catch_up_trap(_job(catch_up=False), _TRAP_SKIP) is False
    # an EXEMPT job bypasses the gate, so its gate-skip job_runs row is a stale
    # artifact (e.g. orchestrator_high_frequency_sync writes sync_runs + runs
    # every 5 min) — must NOT be kicked. Found by dev verification.
    assert pba._is_catch_up_trap(_job(catch_up=True, exempt=True), _TRAP_SKIP) is False


def test_genuine_gap_requires_uncovered_source_never_run_safe() -> None:
    base = _job(catch_up=False, prerequisite=None)
    # uncovered source + never-run + no prereq + no catch_up → gap.
    assert pba._is_genuine_gap(base, None, "sec_n_csr") is True
    # covered source → bootstrap filled it → not a gap.
    assert pba._is_genuine_gap(base, None, "sec_form4") is False
    # no freshness source → not a data-gap job.
    assert pba._is_genuine_gap(base, None, None) is False
    # already ran → not a gap.
    assert pba._is_genuine_gap(base, ("success", None), "sec_n_csr") is False
    # catch_up job is handled by (c)/boot, not (b).
    assert pba._is_genuine_gap(_job(catch_up=True), None, "sec_n_csr") is False
    # a prerequisite means not empty-DB-safe by the #1181 rule.
    assert pba._is_genuine_gap(_job(catch_up=False, prerequisite=object()), None, "sec_n_csr") is False


# --- DB-backed selection + enqueue --------------------------------------


def test_select_candidates_picks_gate_trapped_catch_up(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_gate_skip(ebull_test_conn, JOB_CUSIP_EXTID_SWEEP)
    ebull_test_conn.commit()

    candidates = pba._select_candidates(ebull_test_conn)
    assert (JOB_CUSIP_EXTID_SWEEP, "catch_up_trap_recovery") in candidates


def test_select_candidates_no_genuine_gap_on_bare_registry(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """(b) is ∅ by construction — every scheduled job's registered freshness
    source is bootstrap-covered, so even with all jobs never-run nothing is a
    genuine-gap kick."""
    ebull_test_conn.commit()
    candidates = pba._select_candidates(ebull_test_conn)
    assert [c for c in candidates if c[1] == "genuine_gap_kick"] == []


def test_activate_enqueues_and_audits_catch_up_trap(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_gate_skip(ebull_test_conn, JOB_CUSIP_EXTID_SWEEP)
    ebull_test_conn.commit()

    enqueued = pba.activate_post_bootstrap(ebull_test_conn, run_id=777)
    assert enqueued == [JOB_CUSIP_EXTID_SWEEP]

    # A durable manual_job request landed for the trapped job.
    req = ebull_test_conn.execute(
        """
        SELECT requested_by FROM pending_job_requests
         WHERE job_name = %s AND request_kind = 'manual_job'
        """,
        (JOB_CUSIP_EXTID_SWEEP,),
    ).fetchall()
    assert len(req) == 1
    assert req[0][0] == "system:post_bootstrap_activation"

    # An audit row records why, scoped to this run.
    audit = ebull_test_conn.execute(
        """
        SELECT pass_fail, evidence_json->>'job_name', evidence_json->>'reason'
          FROM decision_audit
         WHERE stage = 'post_bootstrap_activation'
           AND evidence_json->>'run_id' = '777'
        """
    ).fetchall()
    assert len(audit) == 1
    assert audit[0] == ("KICK", JOB_CUSIP_EXTID_SWEEP, "catch_up_trap_recovery")


def test_activate_double_fire_guard_skips_active_request(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An already-in-flight manual request for the job is not duplicated."""
    _seed_gate_skip(ebull_test_conn, JOB_CUSIP_EXTID_SWEEP)
    publish_manual_job_request_with_conn(ebull_test_conn, JOB_CUSIP_EXTID_SWEEP, requested_by="operator-test")
    ebull_test_conn.commit()

    enqueued = pba.activate_post_bootstrap(ebull_test_conn, run_id=778)
    assert JOB_CUSIP_EXTID_SWEEP not in enqueued

    count = ebull_test_conn.execute(
        """
        SELECT COUNT(*) FROM pending_job_requests
         WHERE job_name = %s AND request_kind = 'manual_job'
        """,
        (JOB_CUSIP_EXTID_SWEEP,),
    ).fetchone()
    assert count is not None and count[0] == 1


def test_activate_noop_when_no_candidates(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """No gate-trapped jobs + ∅ genuine gaps → nothing enqueued, no audit rows."""
    ebull_test_conn.commit()
    enqueued = pba.activate_post_bootstrap(ebull_test_conn, run_id=779)
    assert enqueued == []
    audit = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM decision_audit WHERE evidence_json->>'run_id' = '779'"
    ).fetchone()
    assert audit is not None and audit[0] == 0
