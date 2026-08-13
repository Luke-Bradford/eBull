"""#2218 — the degraded terminal, end to end against a real database.

ONE integration test, per the repo's test-tiering guidance: the genuinely-new
SQL mechanism is ``job_runs.status = 'degraded'`` + ``progress_json``, and the
policy that decides it is table-tested purely in ``test_job_progress.py``.
What only a real DB can prove is that the new status survives the
``job_runs_status_check`` constraint sql/254 rewrites and that the JSONB
round-trips — a mocked cursor asserts the parameters and would pass against a
constraint that rejects the value.

⚠ Exercised through the REAL ``_tracked_job`` rather than by calling
``record_job_finish`` directly. The bug class this ticket exists to close is a
rule enforced in one code path and not its duplicate: ``_tracked_job`` had two
byte-identical success branches, and a test that bypasses them proves nothing
about which one runs.

⚠⚠ ``_tracked_job`` calls ``psycopg.connect`` on ``settings.database_url``
itself — the operator's DEV DB — so ``ebull_test_conn`` alone does NOT
redirect it. The
first draft of this file wrote four rows into the dev ``job_runs`` before that
was noticed. Same shape as conftest's ``_filer_ingest_worker_conns_use_test_db``
fixture (#1274), and the fix is the same: patch the global for the duration.
"""

from __future__ import annotations

import psycopg
import pytest

from app.config import settings
from app.services.job_progress import JobProgress
from app.workers.scheduler import _tracked_job
from tests.fixtures.ebull_test_db import test_database_url

_JOB = "test_degraded_terminal_job"


@pytest.fixture(autouse=True)
def _tracked_job_writes_to_the_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect ``settings.database_url`` for the duration.

    ``_tracked_job``'s three connect calls (start, failure, success) read the
    global directly and are not injectable, so patching it is the only seam.
    ⚠ The wording here avoids the literal call spelling on purpose: the
    ``test_no_settings_url_in_destructive_paths`` smoke guard is a line-literal
    grep and does not know a docstring from code — it caught this file once.

    Autouse so a test added later cannot forget it and silently write to the
    operator's dev DB.
    """
    monkeypatch.setattr(settings, "database_url", test_database_url())


def _latest(conn: psycopg.Connection[tuple], job_name: str) -> tuple[str, int | None, str | None, dict]:
    row = conn.execute(
        """
        SELECT status, row_count, error_msg, progress_json
          FROM job_runs WHERE job_name = %s ORDER BY run_id DESC LIMIT 1
        """,
        (job_name,),
    ).fetchone()
    assert row is not None, "no job_runs row written"
    return (str(row[0]), row[1], row[2], row[3])


def test_a_run_that_errored_everything_is_degraded_not_success(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2213's exact shape: the resolver bound a whole-batch failure to a
    counter, nothing raised, and the run recorded ``success / row_count 0``
    for seven weeks."""
    with _tracked_job(_JOB) as tracker:
        tracker.row_count = 0
        tracker.progress = JobProgress(
            candidates_seen=44_195,
            outcomes={"promoted": 0},
            errors={"api_errors": 44_195},
        )

    status, row_count, error_msg, progress = _latest(ebull_test_conn, _JOB)
    assert status == "degraded"
    # ⚠ row_count is still 0 and still recorded. The old signal is not
    # replaced — it was never wrong, it was just never sufficient.
    assert row_count == 0
    assert error_msg is not None and "api_errors=44195" in error_msg
    assert progress["errors"] == {"api_errors": 44195}
    assert progress["candidates_seen"] == 44195


def test_a_run_that_progressed_is_still_success(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The inert half, and the one that would fail loudly if the verdict were
    wired the wrong way round — every healthy job in the scheduler depends on
    it."""
    job = f"{_JOB}_healthy"
    with _tracked_job(job) as tracker:
        tracker.row_count = 3
        tracker.progress = JobProgress(
            candidates_seen=10,
            outcomes={"promoted": 3, "no_instrument_match": 7},
            errors={"api_errors": 0},
        )

    status, row_count, error_msg, progress = _latest(ebull_test_conn, job)
    assert status == "success"
    assert row_count == 3
    assert error_msg is None
    assert progress["outcomes"]["no_instrument_match"] == 7


def test_a_job_reporting_no_progress_keeps_the_historical_contract(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Opt-in property, asserted against the DB because it is what makes this
    change safe to land on ~50 unwired jobs: no progress reported → ``success``
    and a NULL ``progress_json``, exactly as before.

    ⚠ NULL is asserted rather than ``{}``: an empty object would read as "this
    job reported zero of everything", which is a different and false claim.
    """
    job = f"{_JOB}_unwired"
    with _tracked_job(job) as tracker:
        tracker.row_count = 12

    status, row_count, error_msg, progress = _latest(ebull_test_conn, job)
    assert (status, row_count, error_msg, progress) == ("success", 12, None, None)


def test_an_operator_note_is_not_overwritten_by_the_degraded_reason(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``tracker.note`` is a deliberate operator message (a soft-skip digest).
    The derived reason must not clobber it — but the status must still be
    degraded, so the signal is not lost either."""
    job = f"{_JOB}_noted"
    with _tracked_job(job) as tracker:
        tracker.note = "all archives fresh"
        tracker.progress = JobProgress(candidates_seen=5, outcomes={"done": 0})

    status, _, error_msg, progress = _latest(ebull_test_conn, job)
    assert status == "degraded"
    assert error_msg == "all archives fresh"
    # The reason is not lost when the note wins — progress_json carries the
    # evidence, which is the whole point of persisting it.
    assert progress["candidates_seen"] == 5
