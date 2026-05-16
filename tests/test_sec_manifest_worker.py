"""Tests for the manifest-driven SEC worker (#869).

Covers:

- Pluggable parser registry: register / dispatch by source
- Worker iterates pending + retryable rows
- Outcome → state transition contract
- Parser exception → failed transition with backoff
- Skip rows whose source has no registered parser
- Per-source filter narrows the iteration
- WorkerStats summary
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    ParseOutcome,
    clear_registered_parsers,
    register_parser,
    run_manifest_worker,
)
from app.services.sec_manifest import (
    ManifestRow,
    get_manifest_row,
    record_manifest_entry,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _clear_parsers() -> None:
    clear_registered_parsers()


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_pending(conn: psycopg.Connection[tuple], *, accession: str, source: str = "sec_form4") -> None:
    _seed_instrument(conn, iid=1, symbol="X")
    record_manifest_entry(
        conn,
        accession,
        cik="0000000001",
        form="4",
        source=source,  # type: ignore[arg-type]
        subject_type="issuer",
        subject_id="1",
        instrument_id=1,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


class TestParserRegistry:
    def test_unregistered_source_skips_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()
        # No parser registered
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.rows_processed == 1
        assert stats.parsed == 0
        assert stats.skipped_no_parser == 1
        # #940: per-source breakdown exposes which sources lack parsers.
        assert stats.skipped_no_parser_by_source == {"sec_form4": 1}

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "pending"  # untouched

    def test_unregistered_source_unscoped_tick_returns_early(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #1179 contract change: under the per-source fairness path,
        # the unscoped tick ONLY queries rows from
        # ``registered_parser_sources()``. With NO parsers registered,
        # ``run_manifest_worker(source=None)`` early-returns without
        # firing SQL — operators see the unregistered-source backlog
        # via ``/coverage/manifest-parsers`` (#935 §5), not via the
        # per-tick skip surface. The skipped-no-parser WARNING still
        # fires from the per-source rebuild path (#940 unchanged) —
        # see ``test_unregistered_source_skips_row`` above.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-2", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-3", source="sec_def14a")
        ebull_test_conn.commit()

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=10)
        ebull_test_conn.commit()
        # No parsers registered → empty registered_parser_sources() →
        # early return; no rows are processed or skipped.
        assert stats.rows_processed == 0
        assert stats.skipped_no_parser == 0
        assert stats.skipped_no_parser_by_source == {}
        assert stats.processed_by_source == {}

        # Rows remain pending — operator must register parsers or use
        # the per-source rebuild path to drain them.
        for accession in ("ACC-1", "ACC-2", "ACC-3"):
            row = get_manifest_row(ebull_test_conn, accession)
            assert row is not None
            assert row.ingest_status == "pending"

    def test_no_skip_no_warning(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # When every source has a parser, the warning is not emitted —
        # operators should only see the message when something is
        # actually dropped.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)

        with caplog.at_level("WARNING", logger="app.jobs.sec_manifest_worker"):
            stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.skipped_no_parser == 0
        assert stats.skipped_no_parser_by_source == {}
        warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
        assert not warnings, f"unexpected WARNING(s): {[w.getMessage() for w in warnings]}"

    def test_registered_parser_drives_parsed_transition(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def fake_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="stored")

        register_parser("sec_form4", fake_parser)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.skipped_no_parser == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.parser_version == "v1"
        assert row.raw_status == "stored"

    def test_parser_exception_marks_failed_with_backoff(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def crashing_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            raise RuntimeError("HTTP 503 from SEC")

        register_parser("sec_form4", crashing_parser)

        now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10, now=now)
        ebull_test_conn.commit()
        assert stats.failed == 1

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "failed"
        assert row.error is not None
        assert "RuntimeError" in row.error
        assert "HTTP 503" in row.error
        # 1h default backoff
        assert row.next_retry_at == datetime(2026, 1, 1, 13, 0, tzinfo=UTC)

    def test_parser_outcome_failed_uses_provided_retry(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        custom_retry = datetime(2026, 6, 1, tzinfo=UTC)

        def soft_failing_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="failed", error="parse miss", next_retry_at=custom_retry)

        register_parser("sec_form4", soft_failing_parser)
        run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "failed"
        assert row.next_retry_at == custom_retry

    def test_payload_backed_parser_rejects_parsed_with_absent_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #938 audit invariant: a parser registered with
        # ``requires_raw_payload=True`` must not transition a row to
        # ``parsed`` while ``raw_status='absent'``. The worker
        # converts the outcome to a ``failed`` transition with a
        # descriptive error so the row remains auditable + retryable.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser_drops_raw(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="absent")

        register_parser("sec_form4", parser_drops_raw, requires_raw_payload=True)

        now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10, now=now)
        ebull_test_conn.commit()
        assert stats.parsed == 0
        assert stats.failed == 1
        assert stats.raw_payload_violations == 1

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "failed"
        assert row.error is not None
        assert "raw payload missing" in row.error
        # 1h backoff so the retry path eventually re-fires the parser.
        assert row.next_retry_at == datetime(2026, 1, 1, 13, 0, tzinfo=UTC)

    def test_payload_backed_parser_accepts_parsed_with_stored_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Same flag, valid raw_status -> normal parsed transition.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser_persists_raw(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="stored")

        register_parser("sec_form4", parser_persists_raw, requires_raw_payload=True)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.raw_payload_violations == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.raw_status == "stored"

    def test_payload_backed_parser_accepts_parsed_when_row_already_has_stored_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Codex pre-push regression: ``transition_status(..., parsed,
        # raw_status=None)`` preserves the row's existing
        # ``raw_status`` column. The retry / rebuild flow may re-run a
        # parser whose raw body is already on disk — the parser
        # returns ``ParseOutcome(status='parsed', raw_status=None)``
        # because it has nothing new to write. The worker must check
        # the row's effective raw_status (``outcome.raw_status or
        # row.raw_status``), not just the outcome.
        from app.services.sec_manifest import transition_status

        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        # Pre-stamp ``raw_status='stored'`` while keeping
        # ``ingest_status='pending'`` so the worker picks the row up
        # via ``iter_pending`` AND finds existing raw evidence. Models
        # a rebuild flow: body stored on a prior pass, parsed reset to
        # pending for re-parse, parser doesn't restamp raw.
        transition_status(
            ebull_test_conn,
            "ACC-1",
            ingest_status="pending",
            raw_status="stored",
        )
        ebull_test_conn.commit()

        def parser_no_restamp(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v2", raw_status=None)

        register_parser("sec_form4", parser_no_restamp, requires_raw_payload=True)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.failed == 0
        assert stats.raw_payload_violations == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.raw_status == "stored"  # preserved across the parsed transition

    def test_payload_backed_parser_accepts_parsed_with_compacted_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # ``compacted`` is a valid post-storage state (raw bytes
        # written, then compacted into the per-quarter archive). The
        # invariant is "evidence on disk somewhere", not "literally
        # ``stored``".
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser_compacts_raw(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="compacted")

        register_parser("sec_form4", parser_compacts_raw, requires_raw_payload=True)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.raw_payload_violations == 0

    def test_non_payload_parser_allows_parsed_with_absent_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Default ``requires_raw_payload=False`` preserves backward
        # compatibility: synthesised / non-payload parsers can mark
        # rows ``parsed`` without a raw body. Used for sources where
        # the manifest row IS the truth (e.g. heartbeat-style entries).
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def synthesised_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="absent")

        register_parser("sec_form4", synthesised_parser)  # default flag = False

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.failed == 0
        assert stats.raw_payload_violations == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.raw_status == "absent"

    def test_tombstoned_outcome_clears_retry(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def tombstoning_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="tombstoned", error="not on file")

        register_parser("sec_form4", tombstoning_parser)
        run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "tombstoned"
        assert row.error == "not on file"
        assert row.next_retry_at is None


class TestSourceFilter:
    def test_source_filter_narrows_dispatch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-FORM4", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-DEF14A", source="sec_def14a")
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        register_parser("sec_def14a", parser)

        # Only drain form4 source
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1

        form4_row = get_manifest_row(ebull_test_conn, "ACC-FORM4")
        def14a_row = get_manifest_row(ebull_test_conn, "ACC-DEF14A")
        assert form4_row is not None
        assert def14a_row is not None
        assert form4_row.ingest_status == "parsed"
        assert def14a_row.ingest_status == "pending"

    def test_no_source_filter_drains_all(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-FORM4", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-DEF14A", source="sec_def14a")
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        register_parser("sec_def14a", parser)

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 2


class TestRetryablePath:
    def test_failed_rows_past_retry_eligible(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.sec_manifest import transition_status

        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        # Manually mark failed with retry in past
        transition_status(
            ebull_test_conn,
            "ACC-1",
            ingest_status="failed",
            error="x",
            next_retry_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1

    def test_failed_rows_with_future_retry_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.sec_manifest import transition_status

        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        transition_status(
            ebull_test_conn,
            "ACC-1",
            ingest_status="failed",
            error="x",
            next_retry_at=datetime(2099, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 0
        assert stats.rows_processed == 0


# --------------------------------------------------------------------------- #
# #1179 — fairness: per-source quota + Phase B residual top-up.
# --------------------------------------------------------------------------- #

from app.jobs.sec_manifest_worker import (  # noqa: E402
    compute_quotas,
    registered_parser_sources,
)
from app.services.sec_manifest import (  # noqa: E402
    ManifestSource,
    iter_pending_topup,
    iter_retryable_topup,
)


class TestComputeQuotas:
    """Direct unit tests for the quota helper (plan §T1)."""

    def test_max_rows_greater_than_n(self) -> None:
        # n=3, max_rows=10, tick_id=0. base=3, remainder=1.
        # Rotated lead=0 → first source at rotated index 0 gets +1.
        assert compute_quotas(
            sources=("sec_form4", "sec_n_csr", "sec_def14a"),
            max_rows=10,
            tick_id=0,
        ) == {"sec_form4": 4, "sec_n_csr": 3, "sec_def14a": 3}

    def test_max_rows_less_than_n(self) -> None:
        # n=4, max_rows=2, tick_id=0. base=0, remainder=2.
        # First 2 sources at rotated index get 1; rest get 0.
        assert compute_quotas(
            sources=("sec_form3", "sec_form4", "sec_form5", "sec_8k"),
            max_rows=2,
            tick_id=0,
        ) == {"sec_form3": 1, "sec_form4": 1, "sec_form5": 0, "sec_8k": 0}

    def test_rotated_tick_id_shifts_lead(self) -> None:
        # Same n=4, max_rows=2, tick_id=1. lead=1.
        # Rotated indices: idx 0→3, idx 1→0, idx 2→1, idx 3→2.
        # remainder=2 → rot in {0, 1} gets +1 → indices 1 + 2.
        assert compute_quotas(
            sources=("sec_form3", "sec_form4", "sec_form5", "sec_8k"),
            max_rows=2,
            tick_id=1,
        ) == {"sec_form3": 0, "sec_form4": 1, "sec_form5": 1, "sec_8k": 0}

    def test_empty_sources(self) -> None:
        assert compute_quotas(sources=(), max_rows=100, tick_id=0) == {}

    def test_sum_invariant_across_tick_ids(self) -> None:
        sources_ms: tuple[ManifestSource, ...] = (
            "sec_form3",
            "sec_form4",
            "sec_form5",
            "sec_13d",
            "sec_13g",
            "sec_8k",
            "sec_def14a",
        )
        for tick_id in range(20):
            quotas = compute_quotas(sources_ms, max_rows=23, tick_id=tick_id)
            assert sum(quotas.values()) == 23, f"tick_id={tick_id}"

    def test_rotation_covers_every_source_under_base_zero_regime(self) -> None:
        # Under base=0 regime, ticks 0..(n-remainder) must visit every
        # source at least once. n=12, max_rows=8 → 5 ticks.
        sources_ms: tuple[ManifestSource, ...] = (
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
        )
        n, max_rows = len(sources_ms), 8
        touched: set[ManifestSource] = set()
        for tick_id in range(n - max_rows + 1):
            quotas = compute_quotas(sources_ms, max_rows, tick_id)
            touched.update(s for s, q in quotas.items() if q > 0)
        assert touched == set(sources_ms)


# Helper for fairness integration tests: seed manifest rows with
# distinct accession + filed_at per source so deterministic ordering
# is reproducible across runs.
def _seed_pending_n(
    conn: psycopg.Connection[tuple],
    *,
    source: ManifestSource,
    n: int,
    base_filed_at: datetime,
    iid: int,
    cik: str,
) -> list[str]:
    """Seed N pending rows for `source`. Returns the accession list."""
    _seed_instrument(conn, iid=iid, symbol=f"SYM{iid}")
    accessions: list[str] = []
    for i in range(n):
        accession = f"{source}-{base_filed_at:%Y%m%d}-{i:05d}"
        record_manifest_entry(
            conn,
            accession,
            cik=cik,
            form="4" if source.startswith("sec_form") else source.replace("sec_", ""),
            source=source,
            subject_type="issuer",
            subject_id=str(iid),
            instrument_id=iid,
            filed_at=base_filed_at,
        )
        accessions.append(accession)
    return accessions


def _make_capturing_parser(
    captures: list[tuple[str, ManifestSource]],
) -> Callable[[psycopg.Connection[Any], ManifestRow], ParseOutcome]:
    def _parse(conn: psycopg.Connection[Any], row: ManifestRow) -> ParseOutcome:
        captures.append((row.accession_number, row.source))
        return ParseOutcome(status="parsed", parser_version="fake-v1")

    return _parse


# All 14 registered ManifestSource values (we register fakes for all
# of them in the fairness cases so the registered_parser_sources()
# set matches production-shape).
_ALL_SOURCES: tuple[ManifestSource, ...] = (
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
    "finra_short_interest",
)


class TestFairness:
    """Spec §6 integration cases for #1179 fairness."""

    def _register_all_fakes(self, captures: list[tuple[str, ManifestSource]]) -> None:
        for source in _ALL_SOURCES:
            register_parser(source, _make_capturing_parser(captures))

    def test_case1_every_source_progresses(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Seed 1000 sec_form4 + 50 sec_n_csr with distinct filed_at
        # floors. Register fakes for ALL 14 sources so the
        # production-shape registry is in place. max_rows=100,
        # tick_id=0. Assert per-source counts respect quotas + sum to
        # max_rows.
        _seed_pending_n(
            ebull_test_conn,
            source="sec_form4",
            n=20,  # smaller than spec example to keep test fast
            base_filed_at=datetime(2010, 1, 1, tzinfo=UTC),
            iid=1,
            cik="0000000001",
        )
        _seed_pending_n(
            ebull_test_conn,
            source="sec_n_csr",
            n=20,
            base_filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            iid=2,
            cik="0000000002",
        )
        ebull_test_conn.commit()

        captures: list[tuple[str, ManifestSource]] = []
        self._register_all_fakes(captures)

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        # Both sources appear in dispatch list.
        sources_dispatched = {s for _, s in captures}
        assert "sec_form4" in sources_dispatched
        assert "sec_n_csr" in sources_dispatched

        # Per-source counts match the quota for tick_id=0. Derive
        # expected quotas from the SAME sort the worker uses
        # (sorted(registered_parser_sources())) — using the unsorted
        # _ALL_SOURCES tuple would produce different per-source `i`
        # indices and therefore different quota values (review bot
        # PREVENTION on #1180).
        quotas = compute_quotas(sorted(registered_parser_sources()), max_rows=100, tick_id=0)
        for source in ("sec_form4", "sec_n_csr"):
            assert stats.processed_by_source[source] >= quotas[source]

        # Total dispatched ≤ max_rows; equals seeded rows (40 < 100).
        assert sum(stats.processed_by_source.values()) == 40
        assert stats.parsed == 40

    def test_case2_single_source_bursty_catchup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Seed 500 sec_n_csr rows, no other source has rows. Phase A
        # allocates per_source_quota; Phase B fills residual from
        # same source's globally-oldest pending rows. Expect
        # max_rows=100 sec_n_csr dispatched.
        _seed_pending_n(
            ebull_test_conn,
            source="sec_n_csr",
            n=500,
            base_filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            iid=3,
            cik="0000000003",
        )
        ebull_test_conn.commit()

        captures: list[tuple[str, ManifestSource]] = []
        self._register_all_fakes(captures)

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        # Single source consumed the full budget via Phase A quota +
        # Phase B top-up. processed_by_source has ONLY sec_n_csr.
        assert stats.processed_by_source.get("sec_n_csr") == 100
        # No other source dispatched.
        for other in _ALL_SOURCES:
            if other != "sec_n_csr":
                assert stats.processed_by_source.get(other, 0) == 0

    def test_case3_per_source_path_unchanged(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # source='sec_n_csr' — regression on per-source rebuild path.
        _seed_pending_n(
            ebull_test_conn,
            source="sec_n_csr",
            n=10,
            base_filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            iid=4,
            cik="0000000004",
        )
        _seed_pending_n(
            ebull_test_conn,
            source="sec_form4",
            n=10,
            base_filed_at=datetime(2010, 1, 1, tzinfo=UTC),
            iid=5,
            cik="0000000005",
        )
        ebull_test_conn.commit()

        captures: list[tuple[str, ManifestSource]] = []
        # Both registered, but per-source path only drains the
        # requested one.
        register_parser("sec_n_csr", _make_capturing_parser(captures))
        register_parser("sec_form4", _make_capturing_parser(captures))

        stats = run_manifest_worker(ebull_test_conn, source="sec_n_csr", max_rows=100)
        ebull_test_conn.commit()

        assert stats.processed_by_source == {"sec_n_csr": 10}
        assert all(s == "sec_n_csr" for _, s in captures)

    def test_case4_determinism_same_input_same_output(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Seed; run twice with tick_id=0. Reset between runs by
        # rolling back state via transition_status(pending). Capture
        # lists must be identical.
        from app.services.sec_manifest import transition_status

        _seed_pending_n(
            ebull_test_conn,
            source="sec_form4",
            n=5,
            base_filed_at=datetime(2010, 1, 1, tzinfo=UTC),
            iid=6,
            cik="0000000006",
        )
        _seed_pending_n(
            ebull_test_conn,
            source="sec_n_csr",
            n=5,
            base_filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            iid=7,
            cik="0000000007",
        )
        ebull_test_conn.commit()

        captures_a: list[tuple[str, ManifestSource]] = []
        self._register_all_fakes(captures_a)
        run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        # Reset all 10 rows back to pending for round 2.
        for accession_source in captures_a:
            transition_status(ebull_test_conn, accession_source[0], ingest_status="pending")
        ebull_test_conn.commit()

        captures_b: list[tuple[str, ManifestSource]] = []
        clear_registered_parsers()
        for source in _ALL_SOURCES:
            register_parser(source, _make_capturing_parser(captures_b))

        run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        assert captures_a == captures_b

    def test_case5_zero_registered_sources_no_dispatch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # clear_registered_parsers() is autouse'd; seed rows; assert
        # no transition_status fires.
        import app.jobs.sec_manifest_worker as worker_mod

        _seed_pending_n(
            ebull_test_conn,
            source="sec_form4",
            n=10,
            base_filed_at=datetime(2010, 1, 1, tzinfo=UTC),
            iid=8,
            cik="0000000008",
        )
        ebull_test_conn.commit()

        call_count = {"n": 0}
        real_transition = worker_mod.transition_status

        def _counting_transition(*args: object, **kwargs: object) -> object:
            call_count["n"] += 1
            return real_transition(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(worker_mod, "transition_status", _counting_transition)

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        assert stats.rows_processed == 0
        assert stats.processed_by_source == {}
        assert call_count["n"] == 0  # NO UPDATE fired

    def test_case6_retryable_tail_fairness(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Seed retryable-only rows across two sources; assert both
        # dispatched via Phase A retryable allocation + Phase B
        # retryable top-up.
        from app.services.sec_manifest import transition_status

        retry_in_past = datetime(2024, 1, 1, tzinfo=UTC)

        form4_accessions = _seed_pending_n(
            ebull_test_conn,
            source="sec_form4",
            n=20,
            base_filed_at=datetime(2010, 1, 1, tzinfo=UTC),
            iid=9,
            cik="0000000009",
        )
        ncsr_accessions = _seed_pending_n(
            ebull_test_conn,
            source="sec_n_csr",
            n=5,
            base_filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            iid=10,
            cik="0000000010",
        )
        for acc in form4_accessions + ncsr_accessions:
            transition_status(
                ebull_test_conn,
                acc,
                ingest_status="failed",
                error="prior",
                next_retry_at=retry_in_past,
            )
        ebull_test_conn.commit()

        captures: list[tuple[str, ManifestSource]] = []
        self._register_all_fakes(captures)

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        # Both sources dispatched. retryable rows go through the
        # parser → parsed transition for the fake.
        assert stats.processed_by_source.get("sec_form4", 0) > 0
        assert stats.processed_by_source.get("sec_n_csr", 0) > 0
        assert sum(stats.processed_by_source.values()) == 25

    def test_case7_topup_no_double_dispatch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Seed N rows in one source where N < per_source_quota.
        # Phase A picks all N; Phase B SQL's `!= ALL` excludes them.
        # Assert no accession appears twice.
        captures: list[tuple[str, ManifestSource]] = []
        self._register_all_fakes(captures)
        # Derive expected quota from the SAME sort the worker uses
        # (sorted(registered_parser_sources())) — review bot PREVENTION
        # on #1180. Must register fakes first so the registry matches
        # what the worker will see.
        quotas = compute_quotas(sorted(registered_parser_sources()), max_rows=100, tick_id=0)
        ncsr_quota = quotas["sec_n_csr"]
        _seed_pending_n(
            ebull_test_conn,
            source="sec_n_csr",
            n=ncsr_quota // 2,
            base_filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            iid=11,
            cik="0000000011",
        )
        ebull_test_conn.commit()

        run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        accessions = [a for a, _ in captures]
        assert len(accessions) == len(set(accessions)), "double dispatch"

    def test_case8_unregistered_source_excluded_from_topup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Seed sec_xbrl_facts rows; register fakes for OTHER sources
        # only. Assert sec_xbrl_facts never dispatched (Phase A
        # skipped because not in registered set; Phase B SQL excludes
        # via `source = ANY(...)`).
        _seed_pending_n(
            ebull_test_conn,
            source="sec_xbrl_facts",
            n=20,
            base_filed_at=datetime(2024, 1, 1, tzinfo=UTC),
            iid=12,
            cik="0000000012",
        )
        ebull_test_conn.commit()

        captures: list[tuple[str, ManifestSource]] = []
        # Register all EXCEPT sec_xbrl_facts.
        for source in _ALL_SOURCES:
            if source == "sec_xbrl_facts":
                continue
            register_parser(source, _make_capturing_parser(captures))

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=100, tick_id=0)
        ebull_test_conn.commit()

        assert "sec_xbrl_facts" not in stats.processed_by_source
        assert not any(s == "sec_xbrl_facts" for _, s in captures)

    def test_case9_rotation_covers_every_source_under_n_gt_max_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # n=12 registered sources, max_rows=8. Seed 100 rows per
        # source; loop tick_id=0..4; assert union of dispatched
        # sources covers all 12.
        registered: tuple[ManifestSource, ...] = tuple(
            s for s in _ALL_SOURCES if s not in ("sec_xbrl_facts", "finra_short_interest")
        )  # 12 sources
        assert len(registered) == 12

        for i, source in enumerate(registered):
            _seed_pending_n(
                ebull_test_conn,
                source=source,
                n=10,
                base_filed_at=datetime(2024, 1, 1 + i, tzinfo=UTC),
                iid=100 + i,
                cik=f"{100 + i:010d}",
            )
        ebull_test_conn.commit()

        captures: list[tuple[str, ManifestSource]] = []
        for source in registered:
            register_parser(source, _make_capturing_parser(captures))

        # Run 5 ticks (n - remainder + 1 = 12 - 8 + 1).
        all_dispatched: set[ManifestSource] = set()
        for tick_id in range(5):
            captures_this_tick: list[tuple[str, ManifestSource]] = []
            clear_registered_parsers()
            for source in registered:
                register_parser(source, _make_capturing_parser(captures_this_tick))
            run_manifest_worker(ebull_test_conn, source=None, max_rows=8, tick_id=tick_id)
            ebull_test_conn.commit()
            all_dispatched.update(s for _, s in captures_this_tick)
            # Reset dispatched rows to pending so next tick has rows
            # to pick from (avoids running out as ticks accumulate).
            from app.services.sec_manifest import transition_status

            for accession, _ in captures_this_tick:
                transition_status(ebull_test_conn, accession, ingest_status="pending")
            ebull_test_conn.commit()

        registered_set: set[ManifestSource] = set(registered)
        assert all_dispatched == registered_set, f"missing sources: {registered_set - all_dispatched}"

    def test_case10_topup_handles_empty_exclude_array(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Direct invocation of iter_pending_topup +
        # iter_retryable_topup with exclude_accessions=[]. Confirms
        # %s::text[] cast handles empty arrays without psycopg
        # type-inference error.
        from app.services.sec_manifest import transition_status

        accessions = _seed_pending_n(
            ebull_test_conn,
            source="sec_n_csr",
            n=5,
            base_filed_at=datetime(2024, 5, 1, tzinfo=UTC),
            iid=200,
            cik="0000000200",
        )
        ebull_test_conn.commit()

        rows = list(
            iter_pending_topup(
                ebull_test_conn,
                sources=("sec_n_csr",),
                exclude_accessions=[],
                limit=5,
            )
        )
        assert len(rows) == 5
        assert {r.accession_number for r in rows} == set(accessions)

        # Flip one to failed, test retryable variant with empty
        # exclude.
        transition_status(
            ebull_test_conn,
            accessions[0],
            ingest_status="failed",
            error="x",
            next_retry_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()
        retryable_rows = list(
            iter_retryable_topup(
                ebull_test_conn,
                sources=("sec_n_csr",),
                exclude_accessions=[],
                limit=5,
            )
        )
        assert len(retryable_rows) == 1
        assert retryable_rows[0].accession_number == accessions[0]
