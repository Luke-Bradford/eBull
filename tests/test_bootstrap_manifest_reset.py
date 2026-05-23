"""Tests for ``bootstrap_orchestrator.reset_manifest_for_run`` (#1233 PR-5a).

Run-start prelude that flips stale ``ingest_status='failed'`` manifest
rows back to ``pending`` so a fresh bootstrap is not poisoned by
backoff watermarks from a cancelled prior run.

Coverage:

* Happy path: 10 failed rows pre-seeded, every one flipped + retry
  state cleared.
* Source-filter: a row owned by a non-bootstrap source (FINRA
  short-interest) is left untouched.
* Time-filter: a row whose ``last_attempted_at >= reset_started_at``
  (simulating a concurrent live cron writer) is left untouched.
* Opt-out: ``bootstrap_runs.params = {'reset_failed_manifest': False}``
  short-circuits the prelude inside ``run_bootstrap_orchestrator``.
* Non-``failed`` rows (pending / fetched / parsed / tombstoned) are
  untouched even when source + time-filter match.
* Idempotency: a second invocation returns 0.

The reset itself is a pure SQL UPDATE; we do NOT spin the full
dispatcher loop here. End-to-end orchestrator integration is covered
by ``tests/test_bootstrap_orchestrator.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services.bootstrap_orchestrator import (
    _BOOTSTRAP_MANIFEST_SOURCES,
    _MANIFEST_SOURCES_BY_STAGE,
    reset_manifest_for_run,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_TEST_INSTRUMENT_ID = 9251233  # PR-5a fixture instrument; pinned per-test to keep cohorts isolated.


def _seed_instrument(conn: psycopg.Connection[tuple]) -> int:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange,
                                  currency, is_tradable)
        VALUES (%s, 'TST_PR5A', 'TST_PR5A Co', 'TEST', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (_TEST_INSTRUMENT_ID,),
    )
    return _TEST_INSTRUMENT_ID


def _wipe_manifest(conn: psycopg.Connection[tuple]) -> None:
    """Clear every manifest row this test family writes. The shared
    ``ebull_test_conn`` truncate covers ``sec_filing_manifest`` already
    on setup; this is defense-in-depth for tests sharing a transaction.
    """
    conn.execute("DELETE FROM sec_filing_manifest WHERE accession_number LIKE 'PR5A-%'")


def _insert_manifest_row(
    conn: psycopg.Connection[tuple],
    *,
    accession_number: str,
    source: str,
    form: str,
    ingest_status: str,
    last_attempted_at: datetime | None,
    next_retry_at: datetime | None,
    error: str | None,
    instrument_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO sec_filing_manifest (
            accession_number, cik, form, source,
            subject_type, subject_id, instrument_id,
            filed_at,
            ingest_status, last_attempted_at, next_retry_at, error
        ) VALUES (
            %s, '0000000001', %s, %s,
            'issuer', %s, %s,
            now(),
            %s, %s, %s, %s
        )
        """,
        (
            accession_number,
            form,
            source,
            str(instrument_id),
            instrument_id,
            ingest_status,
            last_attempted_at,
            next_retry_at,
            error,
        ),
    )


# ---------------------------------------------------------------------------
# Catalogue / source-set sanity
# ---------------------------------------------------------------------------


def test_bootstrap_manifest_sources_includes_every_sec_source() -> None:
    """The aggregate union covers every SEC-family manifest source.

    FINRA sources (``finra_short_interest`` / ``finra_regsho_daily``)
    are intentionally excluded — they have non-bootstrap drivers.
    """
    expected_sec = {
        "sec_form3",
        "sec_form4",
        "sec_form5",
        "sec_13d",
        "sec_13g",
        "sec_13f_hr",
        "sec_def14a",
        "sec_n_port",
        "sec_n_csr",
        "sec_10k",
        "sec_10q",
        "sec_8k",
        "sec_xbrl_facts",
    }
    assert _BOOTSTRAP_MANIFEST_SOURCES == expected_sec
    # Defense against silent drift: the per-stage breakdown's union
    # must equal the aggregate constant.
    union: set[str] = set()
    for sources in _MANIFEST_SOURCES_BY_STAGE.values():
        union.update(sources)
    assert union == _BOOTSTRAP_MANIFEST_SOURCES


def test_finra_sources_not_in_bootstrap_set() -> None:
    """FINRA sources must NOT be in the bootstrap manifest set.

    Their failure state is owned by ``finra_*`` scheduled jobs; flipping
    them at bootstrap start would mask real failures from the operator.
    """
    assert "finra_short_interest" not in _BOOTSTRAP_MANIFEST_SOURCES
    assert "finra_regsho_daily" not in _BOOTSTRAP_MANIFEST_SOURCES


# ---------------------------------------------------------------------------
# Helper round-trip
# ---------------------------------------------------------------------------


def test_reset_flips_ten_failed_rows_to_pending(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """10 stale failed rows → 10 rows reset; retry state cleared."""
    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale_attempt = reset_started_at - timedelta(hours=1)
    future_retry = reset_started_at + timedelta(hours=6)

    sources = sorted(_BOOTSTRAP_MANIFEST_SOURCES)
    for i in range(10):
        _insert_manifest_row(
            ebull_test_conn,
            accession_number=f"PR5A-FAIL-{i:04d}",
            source=sources[i % len(sources)],
            form="4" if sources[i % len(sources)] == "sec_form4" else "10-K",
            ingest_status="failed",
            last_attempted_at=stale_attempt,
            next_retry_at=future_retry,
            error=f"forced failure {i}",
            instrument_id=iid,
        )
    ebull_test_conn.commit()

    reset_count = reset_manifest_for_run(
        ebull_test_conn,
        sources=_BOOTSTRAP_MANIFEST_SOURCES,
        reset_started_at=reset_started_at,
    )
    ebull_test_conn.commit()

    assert reset_count == 10

    rows = ebull_test_conn.execute(
        """
        SELECT accession_number, ingest_status, next_retry_at, error, last_attempted_at
          FROM sec_filing_manifest
         WHERE accession_number LIKE 'PR5A-FAIL-%'
         ORDER BY accession_number
        """
    ).fetchall()
    assert len(rows) == 10
    for row in rows:
        _, status_str, next_retry, error, last_attempted = row
        assert status_str == "pending"
        assert next_retry is None
        assert error is None
        assert last_attempted is None


def test_source_filter_leaves_non_bootstrap_sources_untouched(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A failed row from a non-bootstrap source (FINRA) is left alone."""
    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale_attempt = reset_started_at - timedelta(hours=1)

    # One SEC row (in scope) + one FINRA row (out of scope).
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-SEC",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=stale_attempt,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="sec failure",
        instrument_id=iid,
    )
    # FINRA rows are universe-scoped (no instrument); use subject_type
    # 'finra_universe' to satisfy the CHECK constraint.
    ebull_test_conn.execute(
        """
        INSERT INTO sec_filing_manifest (
            accession_number, cik, form, source,
            subject_type, subject_id, instrument_id,
            filed_at,
            ingest_status, last_attempted_at, next_retry_at, error
        ) VALUES (
            'PR5A-FAIL-FINRA', '0000000001', 'SI', 'finra_short_interest',
            'finra_universe', 'FINRA_SI', NULL,
            now(),
            'failed', %s, %s, 'finra failure'
        )
        """,
        (stale_attempt, reset_started_at + timedelta(hours=6)),
    )
    ebull_test_conn.commit()

    reset_count = reset_manifest_for_run(
        ebull_test_conn,
        sources=_BOOTSTRAP_MANIFEST_SOURCES,
        reset_started_at=reset_started_at,
    )
    ebull_test_conn.commit()
    assert reset_count == 1

    sec_row = ebull_test_conn.execute(
        "SELECT ingest_status, next_retry_at, error FROM sec_filing_manifest WHERE accession_number = 'PR5A-FAIL-SEC'"
    ).fetchone()
    assert sec_row == ("pending", None, None)

    finra_row = ebull_test_conn.execute(
        "SELECT ingest_status, next_retry_at, error FROM sec_filing_manifest WHERE accession_number = 'PR5A-FAIL-FINRA'"
    ).fetchone()
    assert finra_row is not None
    finra_status, finra_retry, finra_error = finra_row
    assert finra_status == "failed"
    assert finra_retry is not None
    assert finra_error == "finra failure"


def test_time_filter_leaves_concurrent_writes_untouched(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Row stamped AT or AFTER reset_started_at survives the reset."""
    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale = reset_started_at - timedelta(seconds=1)
    fresh_eq = reset_started_at  # boundary case: equal must NOT be reset.
    fresh_gt = reset_started_at + timedelta(seconds=1)  # strict greater.

    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-STALE",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=stale,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="stale failure",
        instrument_id=iid,
    )
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-FRESH-EQ",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=fresh_eq,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="fresh failure equal",
        instrument_id=iid,
    )
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-FRESH-GT",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=fresh_gt,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="fresh failure greater",
        instrument_id=iid,
    )
    ebull_test_conn.commit()

    reset_count = reset_manifest_for_run(
        ebull_test_conn,
        sources=_BOOTSTRAP_MANIFEST_SOURCES,
        reset_started_at=reset_started_at,
    )
    ebull_test_conn.commit()

    assert reset_count == 1

    stale_row = ebull_test_conn.execute(
        "SELECT ingest_status FROM sec_filing_manifest WHERE accession_number = 'PR5A-FAIL-STALE'"
    ).fetchone()
    assert stale_row == ("pending",)

    eq_row = ebull_test_conn.execute(
        "SELECT ingest_status, error FROM sec_filing_manifest WHERE accession_number = 'PR5A-FAIL-FRESH-EQ'"
    ).fetchone()
    assert eq_row == ("failed", "fresh failure equal")

    gt_row = ebull_test_conn.execute(
        "SELECT ingest_status, error FROM sec_filing_manifest WHERE accession_number = 'PR5A-FAIL-FRESH-GT'"
    ).fetchone()
    assert gt_row == ("failed", "fresh failure greater")


def test_non_failed_rows_left_untouched(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Pending / fetched / parsed / tombstoned rows are never flipped."""
    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale = reset_started_at - timedelta(hours=1)

    for status_value, error_value in (
        ("pending", None),
        ("fetched", None),
        ("parsed", None),
        ("tombstoned", "give up"),
    ):
        _insert_manifest_row(
            ebull_test_conn,
            accession_number=f"PR5A-{status_value.upper()}",
            source="sec_form4",
            form="4",
            ingest_status=status_value,
            last_attempted_at=stale,
            next_retry_at=None,
            error=error_value,
            instrument_id=iid,
        )
    ebull_test_conn.commit()

    reset_count = reset_manifest_for_run(
        ebull_test_conn,
        sources=_BOOTSTRAP_MANIFEST_SOURCES,
        reset_started_at=reset_started_at,
    )
    ebull_test_conn.commit()

    assert reset_count == 0

    statuses = ebull_test_conn.execute(
        """
        SELECT accession_number, ingest_status
          FROM sec_filing_manifest
         WHERE accession_number LIKE 'PR5A-%'
         ORDER BY accession_number
        """
    ).fetchall()
    assert statuses == [
        ("PR5A-FETCHED", "fetched"),
        ("PR5A-PARSED", "parsed"),
        ("PR5A-PENDING", "pending"),
        ("PR5A-TOMBSTONED", "tombstoned"),
    ]


def test_reset_is_idempotent(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Re-invoking after a successful reset returns 0 (nothing left)."""
    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale = reset_started_at - timedelta(hours=1)

    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-IDEM",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=stale,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="boom",
        instrument_id=iid,
    )
    ebull_test_conn.commit()

    first = reset_manifest_for_run(
        ebull_test_conn,
        sources=_BOOTSTRAP_MANIFEST_SOURCES,
        reset_started_at=reset_started_at,
    )
    ebull_test_conn.commit()
    assert first == 1

    second = reset_manifest_for_run(
        ebull_test_conn,
        sources=_BOOTSTRAP_MANIFEST_SOURCES,
        reset_started_at=reset_started_at,
    )
    ebull_test_conn.commit()
    assert second == 0


def test_empty_source_set_is_a_no_op(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Passing an empty source set short-circuits before SQL.

    Defense-in-depth: ``ANY('{}')`` would match no rows anyway, but the
    early return avoids the round trip and keeps the log line accurate.
    A row that WOULD have qualified under the normal source set must
    survive.
    """
    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale = reset_started_at - timedelta(hours=1)

    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-EMPTY",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=stale,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="boom",
        instrument_id=iid,
    )
    ebull_test_conn.commit()

    reset_count = reset_manifest_for_run(
        ebull_test_conn,
        sources=frozenset(),
        reset_started_at=reset_started_at,
    )
    ebull_test_conn.commit()

    assert reset_count == 0
    row = ebull_test_conn.execute(
        "SELECT ingest_status, error FROM sec_filing_manifest WHERE accession_number = 'PR5A-FAIL-EMPTY'"
    ).fetchone()
    assert row == ("failed", "boom")


# ---------------------------------------------------------------------------
# Orchestrator-level opt-out
# ---------------------------------------------------------------------------


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status            = 'pending',
               last_run_id       = NULL,
               last_completed_at = NULL
         WHERE id = 1
        """
    )
    conn.commit()


def test_opt_out_skips_reset_inside_orchestrator(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``params={'reset_failed_manifest': False}`` → orchestrator skips
    the reset prelude and the stale failed row survives.

    We bypass the full dispatcher loop by short-circuiting after the
    prelude — the test patches ``_phase_batched_dispatch`` to return
    an empty status set so the run terminalises cleanly.
    """
    from app.config import settings as app_settings
    from app.services import bootstrap_orchestrator
    from app.services.bootstrap_state import StageSpec, start_run
    from tests.fixtures.ebull_test_db import test_database_url

    _reset_state(ebull_test_conn)
    test_db_url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", test_db_url)

    # Register synthetic job-name in the source registry; the
    # orchestrator dispatch path will skip the stage entirely because
    # we monkeypatch ``_phase_batched_dispatch`` below, but JobLock
    # construction inside ``_RunnableStage`` does not need a registered
    # name (it is only constructed when dispatched).
    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale = reset_started_at - timedelta(hours=1)
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-OPTOUT",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=stale,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="boom",
        instrument_id=iid,
    )
    ebull_test_conn.commit()

    # Seed a run with the opt-out param set. ``start_run`` requires at
    # least one stage spec; use a single inert stage that the patched
    # dispatch will skip.
    specs = (StageSpec(stage_key="noop", stage_order=1, lane="init", job_name="noop_job"),)
    run_id = start_run(
        ebull_test_conn,
        operator_id=None,
        stage_specs=specs,
        params={"reset_failed_manifest": False},
    )
    ebull_test_conn.commit()
    assert run_id > 0

    # Confirm the JSONB persisted shape — the opt-out roundtrips.
    row = ebull_test_conn.execute("SELECT params FROM bootstrap_runs WHERE id = %s", (run_id,)).fetchone()
    assert row is not None
    assert row[0] == {"reset_failed_manifest": False}

    # Patch the dispatcher so the run terminalises without trying to
    # invoke the noop stage. The prelude runs BEFORE the dispatcher
    # call, so this patch does not gate the reset under test.
    def _noop_dispatch(*_args: object, **_kwargs: object) -> tuple[dict[str, str], bool]:
        return ({}, False)

    monkeypatch.setattr(
        bootstrap_orchestrator,
        "_phase_batched_dispatch",
        _noop_dispatch,
    )

    bootstrap_orchestrator.run_bootstrap_orchestrator()

    # The opt-out row must still be ``failed`` with its original
    # error + next_retry_at intact.
    after = ebull_test_conn.execute(
        """
        SELECT ingest_status, next_retry_at, error
          FROM sec_filing_manifest
         WHERE accession_number = 'PR5A-FAIL-OPTOUT'
        """
    ).fetchone()
    assert after is not None
    status_after, retry_after, error_after = after
    assert status_after == "failed"
    assert retry_after is not None
    assert error_after == "boom"


def test_default_run_resets_manifest_inside_orchestrator(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run with no ``reset_failed_manifest`` override flips the row.

    Mirror of the opt-out test with the default-TRUE param semantic.
    """
    from app.config import settings as app_settings
    from app.services import bootstrap_orchestrator
    from app.services.bootstrap_state import StageSpec, start_run
    from tests.fixtures.ebull_test_db import test_database_url

    _reset_state(ebull_test_conn)
    test_db_url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", test_db_url)

    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale = reset_started_at - timedelta(hours=1)
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-DEFAULT",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=stale,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="boom",
        instrument_id=iid,
    )
    ebull_test_conn.commit()

    specs = (StageSpec(stage_key="noop", stage_order=1, lane="init", job_name="noop_job"),)
    # No params → defaults to '{}'::jsonb on the column.
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()
    assert run_id > 0

    def _noop_dispatch(*_args: object, **_kwargs: object) -> tuple[dict[str, str], bool]:
        return ({}, False)

    monkeypatch.setattr(
        bootstrap_orchestrator,
        "_phase_batched_dispatch",
        _noop_dispatch,
    )

    bootstrap_orchestrator.run_bootstrap_orchestrator()

    after = ebull_test_conn.execute(
        """
        SELECT ingest_status, next_retry_at, error, last_attempted_at
          FROM sec_filing_manifest
         WHERE accession_number = 'PR5A-FAIL-DEFAULT'
        """
    ).fetchone()
    assert after is not None
    status_after, retry_after, error_after, attempted_after = after
    assert status_after == "pending"
    assert retry_after is None
    assert error_after is None
    assert attempted_after is None


# ---------------------------------------------------------------------------
# Migration smoke — ``params`` column shape
# ---------------------------------------------------------------------------


def test_opt_out_only_honours_exact_boolean_false(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persisted truthy / falsy non-bool values do NOT opt out.

    The JSONB CHECK constraint (sql/169) pins object SHAPE but not the
    inner value type. A future internal writer that persisted
    ``{"reset_failed_manifest": "false"}`` (string) or ``0`` (number)
    would pass shape validation. Naive ``.get()`` truthiness would
    treat the string ``"false"`` as truthy — easy to mis-read in
    review. The orchestrator uses ``is not False`` so ONLY the exact
    JSON boolean ``false`` opts out; every other persisted value
    preserves the default reset-on semantic.
    """
    from app.config import settings as app_settings
    from app.services import bootstrap_orchestrator
    from app.services.bootstrap_state import StageSpec, start_run
    from tests.fixtures.ebull_test_db import test_database_url

    _reset_state(ebull_test_conn)
    test_db_url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", test_db_url)

    iid = _seed_instrument(ebull_test_conn)
    _wipe_manifest(ebull_test_conn)
    reset_started_at = datetime.now(tz=UTC)
    stale = reset_started_at - timedelta(hours=1)
    _insert_manifest_row(
        ebull_test_conn,
        accession_number="PR5A-FAIL-TYPE",
        source="sec_form4",
        form="4",
        ingest_status="failed",
        last_attempted_at=stale,
        next_retry_at=reset_started_at + timedelta(hours=6),
        error="boom",
        instrument_id=iid,
    )
    ebull_test_conn.commit()

    # Persist a string "false" — passes JSONB object-shape check but
    # is NOT the boolean ``false``.
    specs = (StageSpec(stage_key="noop", stage_order=1, lane="init", job_name="noop_job"),)
    run_id = start_run(
        ebull_test_conn,
        operator_id=None,
        stage_specs=specs,
        params={"reset_failed_manifest": "false"},
    )
    ebull_test_conn.commit()
    assert run_id > 0

    def _noop_dispatch(*_args: object, **_kwargs: object) -> tuple[dict[str, str], bool]:
        return ({}, False)

    monkeypatch.setattr(
        bootstrap_orchestrator,
        "_phase_batched_dispatch",
        _noop_dispatch,
    )

    bootstrap_orchestrator.run_bootstrap_orchestrator()

    # Reset MUST have fired — string "false" did NOT opt out.
    after = ebull_test_conn.execute(
        """
        SELECT ingest_status, error
          FROM sec_filing_manifest
         WHERE accession_number = 'PR5A-FAIL-TYPE'
        """
    ).fetchone()
    assert after == ("pending", None)


def test_bootstrap_runs_params_jsonb_object_check(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """sql/169 enforces ``params`` is a JSONB object (not array / scalar)."""
    _reset_state(ebull_test_conn)
    # Object is allowed.
    ebull_test_conn.execute(
        "INSERT INTO bootstrap_runs (status, params) VALUES ('complete', %s)",
        (Jsonb({"reset_failed_manifest": True}),),
    )
    ebull_test_conn.commit()

    # Array must trip the CHECK constraint.
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "INSERT INTO bootstrap_runs (status, params) VALUES ('complete', %s)",
            (Jsonb([1, 2, 3]),),
        )
    ebull_test_conn.rollback()

    # Scalar must also trip.
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "INSERT INTO bootstrap_runs (status, params) VALUES ('complete', %s)",
            (Jsonb("not an object"),),
        )
    ebull_test_conn.rollback()
