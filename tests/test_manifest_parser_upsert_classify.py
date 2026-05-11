"""Tests for the #1131 transient-vs-deterministic upsert discriminator
+ the ``tombstone_stale_failed_upserts`` backfill sweep.

Covers:

* ``is_transient_upsert_error`` returns True only for
  ``psycopg.errors.OperationalError`` (and its subclasses
  ``SerializationFailure`` / ``DeadlockDetected``), False for every
  other exception class — DB-side (``IntegrityError`` / ``DataError`` /
  ``ProgrammingError``) plus non-DB Python (``RuntimeError`` /
  ``ValueError`` / ``KeyError``).
* ``format_upsert_error`` embeds the exception class name so the
  backfill at :func:`tombstone_stale_failed_upserts` can skip
  transient-shape rows precisely.
* The backfill scans + tombstones rows older than ``age``, skips
  fresh ones, skips post-#1131 transient-shape rows, and uses
  ``transition_status`` so the manifest state machine stays
  enforced.

Per-parser discrimination is tested in the parser-specific files
(``test_manifest_parser_eight_k.py``, ``..._def14a.py``,
``..._sec_13dg.py``, ``..._insider_345.py``).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import psycopg.errors
import pytest

from app.services.manifest_parsers._classify import (
    format_upsert_error,
    is_transient_upsert_error,
)
from app.services.sec_manifest import (
    get_manifest_row,
    record_manifest_entry,
    tombstone_stale_failed_upserts,
    transition_status,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# ---------------------------------------------------------------------------
# Classifier unit tests — pure Python, no DB
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc_factory",
    [
        # SerializationFailure / DeadlockDetected both subclass
        # OperationalError per the psycopg3 SQLSTATE map, so the single
        # isinstance check at ``is_transient_upsert_error`` covers all
        # three. Construct instances rather than the classes so the
        # check exercises ``isinstance`` against a runtime exception.
        lambda: psycopg.errors.OperationalError("connection dropped"),
        lambda: psycopg.errors.SerializationFailure("retry me"),
        lambda: psycopg.errors.DeadlockDetected("deadlock"),
    ],
)
def test_classifier_marks_operational_errors_transient(exc_factory) -> None:
    assert is_transient_upsert_error(exc_factory()) is True


@pytest.mark.parametrize(
    "exc_factory",
    [
        # DB-side deterministic — constraint / data violation; bad SQL.
        lambda: psycopg.errors.IntegrityError("unique violation"),
        lambda: psycopg.errors.UniqueViolation("dupe pk"),
        lambda: psycopg.errors.CheckViolation("bad enum"),
        lambda: psycopg.errors.NotNullViolation("missing field"),
        lambda: psycopg.errors.ForeignKeyViolation("orphan fk"),
        lambda: psycopg.errors.DataError("bad date"),
        lambda: psycopg.errors.ProgrammingError("malformed sql"),
        # Non-DB Python exceptions that escape the parser into the
        # upsert path — bug shape, but never self-fix on retry.
        lambda: RuntimeError("synthetic crash"),
        lambda: ValueError("bad value"),
        lambda: KeyError("missing key"),
        lambda: TypeError("type mismatch"),
    ],
)
def test_classifier_marks_non_operational_errors_deterministic(exc_factory) -> None:
    assert is_transient_upsert_error(exc_factory()) is False


def test_format_upsert_error_embeds_class_name() -> None:
    """Backfill skip-list keys on the class name. Make the format
    contract explicit so a future refactor can't silently drop the
    prefix and break the sweep's transient-skip logic."""
    rendered = format_upsert_error(psycopg.errors.DeadlockDetected("boom"))
    assert rendered.startswith("upsert error: ")
    assert "DeadlockDetected" in rendered
    assert "boom" in rendered

    # Non-DB exception class name also lands.
    rendered2 = format_upsert_error(RuntimeError("synthetic"))
    assert "RuntimeError" in rendered2
    assert "synthetic" in rendered2


# ---------------------------------------------------------------------------
# Backfill sweep integration tests
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, f"SWP{iid}", f"SwpCo {iid}"),
    )


def _seed_failed_manifest(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    error: str,
    last_attempted_at: datetime,
) -> None:
    """Seed a failed manifest row with a controllable last_attempted_at.

    ``record_manifest_entry`` defaults to pending; we then mutate the
    row directly into the failed-with-upsert-error state because the
    state machine forbids stamping a synthetic ``last_attempted_at`` via
    ``transition_status`` (which always defaults to NOW()).
    """
    record_manifest_entry(
        conn,
        accession,
        cik="0000320193",
        form="8-K",
        source="sec_8k",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    # Direct UPDATE — bypasses ``transition_status`` so the test can
    # control ``last_attempted_at`` precisely. The sweep itself goes
    # through ``transition_status`` for the failed->tombstoned move.
    conn.execute(
        """
        UPDATE sec_filing_manifest
        SET ingest_status = 'failed',
            error = %s,
            last_attempted_at = %s,
            next_retry_at = %s,
            raw_status = 'stored'
        WHERE accession_number = %s
        """,
        (error, last_attempted_at, last_attempted_at + timedelta(hours=1), accession),
    )


def test_backfill_tombstones_old_upsert_failed_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Row older than ``age`` with a pre-#1131 upsert-error message
    (no class-name prefix) gets promoted to ``tombstoned``."""
    _seed_instrument(ebull_test_conn, 8810001)
    long_ago = datetime.now(tz=UTC) - timedelta(hours=72)
    _seed_failed_manifest(
        ebull_test_conn,
        accession="0000111000-26-000001",
        instrument_id=8810001,
        error="upsert error: bad date past CHECK constraint",
        last_attempted_at=long_ago,
    )
    ebull_test_conn.commit()

    result = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24))
    ebull_test_conn.commit()

    assert result.rows_scanned == 1
    assert result.rows_tombstoned == 1
    assert result.rows_skipped_transient == 0
    assert result.rows_skipped_race == 0

    row = get_manifest_row(ebull_test_conn, "0000111000-26-000001")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    # Raw evidence preserved through the promotion (the #948 invariant).
    assert row.raw_status == "stored"
    # Original error carried forward in the new error so an operator
    # debugging post-tombstone has the context.
    assert row.error is not None and "bad date past CHECK constraint" in row.error
    assert row.error.startswith("#1131 backfill:")


def test_backfill_skips_recent_upsert_failed_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Row younger than ``age`` is left alone — could still recover on
    the next retry cycle. Conservative sweep avoids masking real
    transients with the heuristic."""
    _seed_instrument(ebull_test_conn, 8810002)
    recent = datetime.now(tz=UTC) - timedelta(hours=2)
    _seed_failed_manifest(
        ebull_test_conn,
        accession="0000111000-26-000002",
        instrument_id=8810002,
        error="upsert error: transient-looking thing",
        last_attempted_at=recent,
    )
    ebull_test_conn.commit()

    result = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24))
    ebull_test_conn.commit()

    assert result.rows_scanned == 0
    assert result.rows_tombstoned == 0

    row = get_manifest_row(ebull_test_conn, "0000111000-26-000002")
    assert row is not None
    assert row.ingest_status == "failed"


@pytest.mark.parametrize(
    "transient_token",
    ["OperationalError", "SerializationFailure", "DeadlockDetected"],
)
def test_backfill_skips_post_1131_transient_class_tokens(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    transient_token: str,
) -> None:
    """Post-#1131 ``error`` strings carry the exception class name; the
    sweep must skip rows where the class is a transient psycopg shape
    so a genuine retry isn't masked as a tombstone."""
    iid = 8810010 + len(transient_token)  # stable distinct iid per token
    _seed_instrument(ebull_test_conn, iid)
    accession = f"0000111100-26-{len(transient_token):06d}"
    long_ago = datetime.now(tz=UTC) - timedelta(hours=48)
    _seed_failed_manifest(
        ebull_test_conn,
        accession=accession,
        instrument_id=iid,
        error=f"upsert error: {transient_token}: lock timeout",
        last_attempted_at=long_ago,
    )
    ebull_test_conn.commit()

    result = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24))
    ebull_test_conn.commit()

    assert result.rows_scanned == 1
    assert result.rows_tombstoned == 0
    assert result.rows_skipped_transient == 1

    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "failed"


def test_backfill_ignores_non_upsert_errors(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Rows whose ``error`` is not an upsert-shape (fetch error /
    parse error / log error) stay in ``failed`` even when old —
    those may recover on retry (network blip, parser-version bump)
    and shouldn't be swept by this heuristic."""
    _seed_instrument(ebull_test_conn, 8810050)
    long_ago = datetime.now(tz=UTC) - timedelta(hours=48)
    _seed_failed_manifest(
        ebull_test_conn,
        accession="0000111200-26-000001",
        instrument_id=8810050,
        error="fetch error: timeout",
        last_attempted_at=long_ago,
    )
    ebull_test_conn.commit()

    result = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24))
    ebull_test_conn.commit()

    assert result.rows_scanned == 0
    row = get_manifest_row(ebull_test_conn, "0000111200-26-000001")
    assert row is not None
    assert row.ingest_status == "failed"


def test_backfill_filters_failed_rows_only(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The sweep query filters ``WHERE ingest_status = 'failed'`` so
    rows already at terminal states (parsed / tombstoned) are
    invisible. No exception leaks; the conditional UPDATE simply
    matches zero rows."""
    _seed_instrument(ebull_test_conn, 8810060)
    long_ago = datetime.now(tz=UTC) - timedelta(hours=48)
    _seed_failed_manifest(
        ebull_test_conn,
        accession="0000111300-26-000001",
        instrument_id=8810060,
        error="upsert error: stale",
        last_attempted_at=long_ago,
    )
    # Now move to tombstoned so the row is already at the target state.
    transition_status(
        ebull_test_conn,
        "0000111300-26-000001",
        ingest_status="tombstoned",
        error="pre-existing tombstone",
    )
    ebull_test_conn.commit()

    result = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24))
    ebull_test_conn.commit()

    assert result.rows_scanned == 0
    assert result.rows_tombstoned == 0
    assert result.rows_skipped_race == 0


def test_backfill_conditional_update_detects_worker_race(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex pre-push round 1 (Finding 2): the sweep's per-row UPDATE
    carries ``WHERE ingest_status = 'failed' AND last_attempted_at
    IS NOT DISTINCT FROM <sampled value>`` so a concurrent worker
    advance between sample and update produces zero rows updated and
    a ``rows_skipped_race`` increment — never a stale tombstone over
    the worker's progress.

    Direct test of the UPDATE WHERE clause: drive the SQL the sweep
    would emit with a deliberately-wrong ``expected_attempted_at``
    and assert the row is untouched. The sweep loop itself is
    exercised by the other tests in this file."""
    _seed_instrument(ebull_test_conn, 8810061)
    long_ago = datetime.now(tz=UTC) - timedelta(hours=48)
    _seed_failed_manifest(
        ebull_test_conn,
        accession="0000111301-26-000001",
        instrument_id=8810061,
        error="upsert error: stale",
        last_attempted_at=long_ago,
    )
    ebull_test_conn.commit()

    # Emulate the worker winning the race: ``last_attempted_at`` is
    # now T1, but the sweep "sampled" the row at T0 (long_ago) and
    # tries to UPDATE with expected_attempted_at=T0.
    t1 = datetime.now(tz=UTC) - timedelta(hours=2)
    ebull_test_conn.execute(
        "UPDATE sec_filing_manifest SET last_attempted_at = %s WHERE accession_number = %s",
        (t1, "0000111301-26-000001"),
    )
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sec_filing_manifest
            SET ingest_status = 'tombstoned',
                error = 'sweep should not win this',
                next_retry_at = NULL,
                last_attempted_at = NOW()
            WHERE accession_number = %(accession)s
              AND ingest_status = 'failed'
              AND last_attempted_at IS NOT DISTINCT FROM %(expected_attempted_at)s
            RETURNING accession_number
            """,
            {
                "accession": "0000111301-26-000001",
                "expected_attempted_at": long_ago,  # stale — does not match T1
            },
        )
        won = cur.fetchone()
    ebull_test_conn.commit()

    # The WHERE guard rejected the stale-expected UPDATE.
    assert won is None
    row = get_manifest_row(ebull_test_conn, "0000111301-26-000001")
    assert row is not None
    assert row.ingest_status == "failed"
    # Original error preserved — sweep did NOT overwrite.
    assert row.error == "upsert error: stale"


def test_backfill_skipped_race_counter_uses_anchored_token_match(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex pre-push round 1 (Finding 1): a deterministic error whose
    message happens to mention the transient token MUST be tombstoned,
    not skipped. Anchored match requires the token to follow the
    upsert prefix directly."""
    _seed_instrument(ebull_test_conn, 8810070)
    long_ago = datetime.now(tz=UTC) - timedelta(hours=48)
    # Deterministic error whose body mentions the token as content.
    _seed_failed_manifest(
        ebull_test_conn,
        accession="0000111400-26-009999",
        instrument_id=8810070,
        error='upsert error: CheckViolation: column value "OperationalError" violates constraint',
        last_attempted_at=long_ago,
    )
    ebull_test_conn.commit()

    result = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24))
    ebull_test_conn.commit()

    # Anchored match: the token comes AFTER the prefix in the error
    # text but is not the class slot, so the row is treated as
    # deterministic and gets tombstoned. A substring-anywhere match
    # would have skipped it.
    assert result.rows_scanned == 1
    assert result.rows_tombstoned == 1
    assert result.rows_skipped_transient == 0

    row = get_manifest_row(ebull_test_conn, "0000111400-26-009999")
    assert row is not None and row.ingest_status == "tombstoned"


def test_backfill_limit_bounds_per_run(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``limit`` caps the candidate batch so a million-row backlog can't
    OOM the worker. Oldest-first order keeps progress monotonic across
    successive runs."""
    long_ago_base = datetime.now(tz=UTC) - timedelta(hours=48)
    for i in range(3):
        _seed_instrument(ebull_test_conn, 8810100 + i)
        _seed_failed_manifest(
            ebull_test_conn,
            accession=f"0000111400-26-{i:06d}",
            instrument_id=8810100 + i,
            error=f"upsert error: row {i}",
            # Stagger so ORDER BY last_attempted_at ASC is observable.
            last_attempted_at=long_ago_base - timedelta(minutes=i),
        )
    ebull_test_conn.commit()

    # First sweep: limit=1 picks the oldest (row 2 — most-negative offset).
    first = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24), limit=1)
    ebull_test_conn.commit()
    assert first.rows_scanned == 1
    assert first.rows_tombstoned == 1

    oldest_row = get_manifest_row(ebull_test_conn, "0000111400-26-000002")
    assert oldest_row is not None
    assert oldest_row.ingest_status == "tombstoned"

    # The other two are still failed.
    for acc in ("0000111400-26-000001", "0000111400-26-000000"):
        row = get_manifest_row(ebull_test_conn, acc)
        assert row is not None
        assert row.ingest_status == "failed"

    # Second sweep with no limit drains the rest.
    second = tombstone_stale_failed_upserts(ebull_test_conn, age=timedelta(hours=24))
    ebull_test_conn.commit()
    assert second.rows_tombstoned == 2
