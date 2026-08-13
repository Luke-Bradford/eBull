"""#2650 — ``check_row_count_spike`` at its DEFAULT must plan, not just typecheck.

The comparison query filters the current run out of its own history with::

    AND (%(exclude_id)s IS NULL OR run_id != %(exclude_id)s)

psycopg3 dedups that repeated named parameter into one ``$n``. At the
documented default ``exclude_run_id=None`` it is sent untyped (OID 0), and its
only type-determining context is a ``NullTest``, which constrains nothing —
``run_id != $n`` does not rescue it, because operator resolution happens after
parameter-type resolution. Postgres cannot plan the statement::

    AmbiguousParameter: could not determine data type of parameter $2

⚠⚠ WHY THIS FILE IS DB-TIER AND ITS SIBLING IS NOT.
``test_row_count_spikes.py`` calls the ``None`` path five times and is green,
because it drives a ``MagicMock`` connection: a mocked cursor records parameters
and sends nothing, so it cannot observe a plan-time type-inference failure. The
invariant is a property of the PLAN, not of the Python. That file keeps the
ratio-threshold boundary cases (cheap, pure, and genuinely about arithmetic);
this one owns the single guard that needs a real backend.

⚠ Both tests below seed TWO prior successful runs and assert the exclusion
CHANGES the answer. A cast is not the only edit that makes the ``None`` path
plan — ``COALESCE(%(exclude_id)s, -1)`` does too, as does deleting the clause —
and both would silently destroy the self-comparison guard the clause exists for
(prevention-log §"check_row_count_spike compared the run against itself"). A
test that only asserted "does not raise" would pass on either.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.sync_orchestrator.row_count_spikes import check_row_count_spike
from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.db

_JOB = "row_count_spike_cast_probe"


def _seed_run(conn: psycopg.Connection[Any], *, minutes_ago: int, row_count: int) -> int:
    """Insert one successful ``job_runs`` row and return its ``run_id``.

    ``started_at`` is explicit because the query orders on it — a default-now
    seed would make the two rows' order depend on insert timing.
    """
    row = conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, status, row_count)
        VALUES (%s, now() - make_interval(mins => %s), 'success', %s)
        RETURNING run_id
        """,
        (_JOB, minutes_ago, row_count),
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def test_the_default_compares_against_the_latest_prior_run(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The defect: this call raised ``AmbiguousParameter`` instead of returning.

    The docstring promises the default compares against the previous successful
    run. Untyped, the documented default was the one binding that could not be
    planned — so the parameter could not be used at the value its own signature
    declares.
    """
    _seed_run(ebull_test_conn, minutes_ago=20, row_count=40)
    _seed_run(ebull_test_conn, minutes_ago=10, row_count=100)

    result = check_row_count_spike(ebull_test_conn, _JOB, current_count=45)

    assert result.previous_count == 100  # the LATEST prior run, not the older one
    assert result.flagged is True  # 45/100 = 0.45 < 0.5


def test_excluding_the_latest_run_falls_back_to_the_one_before(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The other side of the ``OR``, and the reason a bare ``COALESCE`` fix fails.

    Same seeded history and the same ``current_count`` as the test above, so the
    only difference is the exclusion — and it must flip the verdict. A change
    that neutralised the clause would leave ``previous_count == 100`` here.
    """
    _seed_run(ebull_test_conn, minutes_ago=20, row_count=40)
    latest = _seed_run(ebull_test_conn, minutes_ago=10, row_count=100)

    result = check_row_count_spike(ebull_test_conn, _JOB, current_count=45, exclude_run_id=latest)

    assert result.previous_count == 40  # the excluded run's 100 is not consulted
    assert result.flagged is False  # 45/40 = 1.125, above threshold


def test_the_default_plans_with_no_prior_history_at_all(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The no-prior-run branch binds ``None`` too, so it raised for the same
    reason — and this is the branch a job hits on its very first ever run.

    ⚠ Asserts on ``previous_count is None`` rather than only on ``flagged``,
    which is ``False`` on several other branches too.
    """
    result = check_row_count_spike(ebull_test_conn, _JOB, current_count=100)

    assert result.previous_count is None
    assert result.flagged is False
    assert "no prior row_count" in result.detail
