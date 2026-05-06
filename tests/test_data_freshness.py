"""Tests for ``data_freshness_index`` + the scheduler service (#865).

Covers:

- Schema integrity: PK, CHECK constraints, indexes
- ``seed_scheduler_from_manifest`` round-trip + idempotent re-seed
- ``record_poll_outcome`` UPSERT contract
- ``subjects_due_for_poll`` filter (state IN unknown/current/overdue)
- ``subjects_due_for_recheck`` filter (state IN never_filed/error)
- Per-source cadence calculator
- Polymorphic-subject seeding (issuer + institutional_filer)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import psycopg.rows
import pytest

from app.services.data_freshness import (
    cadence_for,
    get_freshness_row,
    predict_next_at,
    record_poll_outcome,
    seed_freshness_for_manifest_row,
    seed_scheduler_from_manifest,
    subjects_due_for_poll,
    subjects_due_for_recheck,
)
from app.services.sec_manifest import record_manifest_entry, transition_status
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str, cik: str | None = None) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )
    if cik is not None:
        conn.execute(
            """
            INSERT INTO instrument_sec_profile (instrument_id, cik)
            VALUES (%s, %s)
            ON CONFLICT (instrument_id) DO UPDATE SET cik = EXCLUDED.cik
            """,
            (iid, cik),
        )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestSchema:
    def test_table_exists_with_required_columns(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT column_name, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'data_freshness_index'
                """
            )
            cols = {row["column_name"]: row["is_nullable"] for row in cur.fetchall()}

        for required_nn in (
            "subject_type",
            "subject_id",
            "source",
            "last_polled_outcome",
            "new_filings_since",
            "state",
            "created_at",
            "updated_at",
        ):
            assert cols.get(required_nn) == "NO", f"{required_nn} must be NOT NULL"

        for nullable in (
            "cik",
            "instrument_id",
            "last_known_filing_id",
            "last_known_filed_at",
            "last_polled_at",
            "expected_next_at",
            "next_recheck_at",
            "state_reason",
        ):
            assert cols.get(nullable) == "YES", f"{nullable} must be NULLABLE"

    def test_issuer_constraint_rejects_null_instrument_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(psycopg.errors.CheckViolation):
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO data_freshness_index (
                        subject_type, subject_id, source, instrument_id
                    ) VALUES ('issuer', '999', 'sec_form4', NULL)
                    """
                )
        ebull_test_conn.rollback()


# ---------------------------------------------------------------------------
# Cadence calculator
# ---------------------------------------------------------------------------


class TestCadence:
    def test_known_sources_have_cadence(self) -> None:
        # Spot-check the cadences match the spec ceilings.
        assert cadence_for("sec_form4") == timedelta(days=30)
        assert cadence_for("sec_13f_hr") == timedelta(days=120)
        assert cadence_for("sec_def14a") == timedelta(days=365)
        assert cadence_for("sec_8k") == timedelta(days=14)

    def test_unknown_source_raises_keyerror(self) -> None:
        with pytest.raises(KeyError):
            cadence_for("not_a_source")  # type: ignore[arg-type]

    def test_predict_next_at_advances_by_cadence(self) -> None:
        last_filed = datetime(2026, 1, 1, tzinfo=UTC)
        assert predict_next_at("sec_form4", last_filed) == last_filed + timedelta(days=30)

    def test_predict_next_at_returns_none_when_never_filed(self) -> None:
        assert predict_next_at("sec_form4", None) is None


# ---------------------------------------------------------------------------
# Seeder
# ---------------------------------------------------------------------------


class TestSeedFromManifest:
    def _seed_manifest_row(
        self,
        conn: psycopg.Connection[tuple],
        *,
        accession: str,
        subject_type,
        subject_id: str,
        source,
        cik: str,
        instrument_id: int | None,
        filed_at: datetime,
    ) -> None:
        record_manifest_entry(
            conn,
            accession,
            cik=cik,
            form="X",
            source=source,
            subject_type=subject_type,
            subject_id=subject_id,
            instrument_id=instrument_id,
            filed_at=filed_at,
        )
        transition_status(conn, accession, ingest_status="parsed")

    def test_seeds_one_row_per_subject_source(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1701, symbol="AAPL", cik="0000320193")
        # Two Form 4 filings for AAPL — should yield ONE scheduler row
        # for (issuer, 1701, sec_form4) with newest filed_at.
        self._seed_manifest_row(
            ebull_test_conn,
            accession="ACC-1",
            subject_type="issuer",
            subject_id="1701",
            source="sec_form4",
            cik="0000320193",
            instrument_id=1701,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        self._seed_manifest_row(
            ebull_test_conn,
            accession="ACC-2",
            subject_type="issuer",
            subject_id="1701",
            source="sec_form4",
            cik="0000320193",
            instrument_id=1701,
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        n = seed_scheduler_from_manifest(ebull_test_conn)
        ebull_test_conn.commit()
        assert n == 1

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_form4")
        assert row is not None
        assert row.last_known_filing_id == "ACC-2"
        assert row.last_known_filed_at == datetime(2026, 2, 1, tzinfo=UTC)
        # Cadence = 30d for form4
        assert row.expected_next_at == datetime(2026, 3, 3, tzinfo=UTC)
        assert row.state == "current"

    def test_seeds_polymorphic_subjects(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        # issuer scope
        self._seed_manifest_row(
            ebull_test_conn,
            accession="ACC-ISSUER",
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        # institutional_filer scope (no instrument_id)
        self._seed_manifest_row(
            ebull_test_conn,
            accession="ACC-INST",
            subject_type="institutional_filer",
            subject_id="0001364742",
            source="sec_13f_hr",
            cik="0001364742",
            instrument_id=None,
            filed_at=datetime(2026, 2, 14, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        n = seed_scheduler_from_manifest(ebull_test_conn)
        ebull_test_conn.commit()
        assert n == 2

        issuer_row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert issuer_row is not None
        assert issuer_row.instrument_id == 1

        inst_row = get_freshness_row(
            ebull_test_conn,
            subject_type="institutional_filer",
            subject_id="0001364742",
            source="sec_13f_hr",
        )
        assert inst_row is not None
        assert inst_row.instrument_id is None

    def test_re_seed_is_idempotent(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        self._seed_manifest_row(
            ebull_test_conn,
            accession="ACC-1",
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        for _ in range(3):
            seed_scheduler_from_manifest(ebull_test_conn)
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM data_freshness_index")
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 1


# ---------------------------------------------------------------------------
# record_poll_outcome
# ---------------------------------------------------------------------------


class TestRecordPollOutcome:
    def test_new_data_outcome_advances_watermark(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            outcome="new_data",
            last_known_filing_id="ACC-NEW",
            last_known_filed_at=datetime(2026, 3, 1, tzinfo=UTC),
            new_filings_since=2,
            cik="0000000001",
            instrument_id=1,
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        assert row.last_known_filing_id == "ACC-NEW"
        assert row.last_polled_outcome == "new_data"
        assert row.state == "current"
        assert row.new_filings_since == 2
        # cadence = 30d for form4
        assert row.expected_next_at == datetime(2026, 3, 31, tzinfo=UTC)

    def test_error_outcome_sets_error_state(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            outcome="error",
            error="HTTP 503",
            next_recheck_at=datetime(2026, 1, 2, tzinfo=UTC),
            cik="0000000001",
            instrument_id=1,
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        assert row.state == "error"
        assert row.last_polled_outcome == "error"
        assert row.next_recheck_at == datetime(2026, 1, 2, tzinfo=UTC)

    def test_issuer_without_instrument_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(ValueError, match="issuer subject requires instrument_id"):
            record_poll_outcome(
                ebull_test_conn,
                subject_type="issuer",
                subject_id="1",
                source="sec_form4",
                outcome="new_data",
                cik="0000000001",
                instrument_id=None,
            )

    def test_non_issuer_with_instrument_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X")
        with pytest.raises(ValueError, match="non-issuer subject must have instrument_id=None"):
            record_poll_outcome(
                ebull_test_conn,
                subject_type="institutional_filer",
                subject_id="0001364742",
                source="sec_13f_hr",
                outcome="new_data",
                cik="0001364742",
                instrument_id=1,
            )


# ---------------------------------------------------------------------------
# Iterators
# ---------------------------------------------------------------------------


class TestIterators:
    def test_due_for_poll_includes_unknown_state(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Codex review v3 finding 4: ``unknown`` MUST be in the due
        # set so reset-by-rebuild rows drain immediately.
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_freshness_index (
                    subject_type, subject_id, source, instrument_id, cik,
                    state, expected_next_at
                ) VALUES (
                    'issuer', '1', 'sec_form4', 1, '0000000001',
                    'unknown', NULL
                )
                """
            )
        ebull_test_conn.commit()

        rows = list(subjects_due_for_poll(ebull_test_conn, source="sec_form4", limit=10))
        assert len(rows) == 1
        assert rows[0].state == "unknown"

    def test_due_for_poll_excludes_future_due(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_freshness_index (
                    subject_type, subject_id, source, instrument_id, cik,
                    state, expected_next_at
                ) VALUES (
                    'issuer', '1', 'sec_form4', 1, '0000000001',
                    'current', %s
                )
                """,
                (datetime(2099, 1, 1, tzinfo=UTC),),
            )
        ebull_test_conn.commit()

        rows = list(subjects_due_for_poll(ebull_test_conn, source="sec_form4", limit=10))
        assert rows == []

    def test_due_for_poll_excludes_never_filed_state(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_freshness_index (
                    subject_type, subject_id, source, instrument_id, cik,
                    state, expected_next_at, next_recheck_at
                ) VALUES (
                    'issuer', '1', 'sec_form4', 1, '0000000001',
                    'never_filed', NULL, %s
                )
                """,
                (datetime(2024, 1, 1, tzinfo=UTC),),
            )
        ebull_test_conn.commit()

        # never_filed shows up on recheck iterator, NOT poll iterator
        poll_rows = list(subjects_due_for_poll(ebull_test_conn, source="sec_form4", limit=10))
        recheck_rows = list(subjects_due_for_recheck(ebull_test_conn, source="sec_form4", limit=10))
        assert poll_rows == []
        assert len(recheck_rows) == 1
        assert recheck_rows[0].state == "never_filed"

    def test_due_for_recheck_excludes_active_states(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_freshness_index (
                    subject_type, subject_id, source, instrument_id, cik,
                    state, expected_next_at
                ) VALUES (
                    'issuer', '1', 'sec_form4', 1, '0000000001',
                    'current', %s
                )
                """,
                (datetime(2024, 1, 1, tzinfo=UTC),),
            )
        ebull_test_conn.commit()

        recheck_rows = list(subjects_due_for_recheck(ebull_test_conn, source="sec_form4", limit=10))
        assert recheck_rows == []


class TestSeedFreshnessForManifestRow:
    """#956: single-row seed wired into ``record_manifest_entry``."""

    def test_seeds_new_subject(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-1",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        assert row.last_known_filing_id == "ACC-1"
        assert row.last_known_filed_at == datetime(2026, 2, 1, tzinfo=UTC)
        assert row.state == "current"
        # Cadence = 30d for sec_form4
        assert row.expected_next_at == datetime(2026, 3, 3, tzinfo=UTC)

    def test_newer_row_advances_watermark(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-OLD",
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-NEW",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        assert row.last_known_filing_id == "ACC-NEW"
        assert row.last_known_filed_at == datetime(2026, 2, 1, tzinfo=UTC)

    def test_older_row_does_not_clobber_watermark(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Discovery writers (rebuild secondary-page walk, daily-index
        # reconcile catching up) can call this with an OLDER accession
        # than what's already tracked. The watermark must not regress.
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-NEW",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-OLD",
            filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        # Watermark stayed on the newer accession.
        assert row.last_known_filing_id == "ACC-NEW"
        assert row.last_known_filed_at == datetime(2026, 2, 1, tzinfo=UTC)

    def test_re_discovery_does_not_clobber_poll_error_state(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Codex pre-push: the inline UPSERT must NOT clobber legitimate
        # poll-outcome states (``error`` / ``expected_filing_overdue``)
        # on a duplicate / older re-discovery write. A noisy Atom
        # replay of an already-known accession should leave the
        # per-CIK poll's error state intact.
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-1",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        # Simulate a poll error overwriting state to 'error'.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE data_freshness_index
                SET state = 'error',
                    state_reason = 'HTTP 503',
                    next_recheck_at = %s
                WHERE subject_type = 'issuer' AND subject_id = '1' AND source = 'sec_form4'
                """,
                (datetime(2026, 3, 1, tzinfo=UTC),),
            )
        ebull_test_conn.commit()

        # Re-discovery via Atom replay (same accession or older).
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-1",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        # State stayed 'error' — the manifest write did not overwrite
        # the poll-outcome state.
        assert row.state == "error"
        assert row.next_recheck_at == datetime(2026, 3, 1, tzinfo=UTC)
        # state_reason isn't on FreshnessRow; query directly.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT state_reason FROM data_freshness_index
                WHERE subject_type = 'issuer' AND subject_id = '1' AND source = 'sec_form4'
                """
            )
            (state_reason,) = cur.fetchone() or (None,)
        assert state_reason == "HTTP 503"

    def test_re_discovery_escalates_never_filed_to_current(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # The legitimate state change: ``never_filed`` → ``current``
        # MUST happen when manifest evidence appears. This is the
        # rescue path the bulk seed comment documents.
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        # Seed a freshness row in 'never_filed' state (subject was
        # tracked but no filings yet).
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_freshness_index (
                    subject_type, subject_id, source, cik, instrument_id,
                    state, expected_next_at
                ) VALUES (
                    'issuer', '1', 'sec_form4', '0000000001', 1,
                    'never_filed', %s
                )
                """,
                (datetime(2026, 1, 1, tzinfo=UTC),),
            )
        ebull_test_conn.commit()

        # Manifest write rescues the row.
        seed_freshness_for_manifest_row(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1",
            source="sec_form4",
            cik="0000000001",
            instrument_id=1,
            accession_number="ACC-1",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        assert row.state == "current"
        assert row.last_known_filing_id == "ACC-1"

    def test_record_manifest_entry_seeds_inline(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # The actual contract: every ``record_manifest_entry`` call
        # leaves the freshness index queryable for that triple. No
        # separate ``seed_scheduler_from_manifest`` invocation needed.
        _seed_instrument(ebull_test_conn, iid=1, symbol="X", cik="0000000001")
        record_manifest_entry(
            ebull_test_conn,
            "ACC-1",
            cik="0000000001",
            form="4",
            source="sec_form4",
            subject_type="issuer",
            subject_id="1",
            instrument_id=1,
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1", source="sec_form4")
        assert row is not None
        assert row.last_known_filing_id == "ACC-1"
        assert row.state == "current"
