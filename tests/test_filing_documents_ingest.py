"""Integration tests for ``ingest_filing_documents`` (#452 / #723)."""

from __future__ import annotations

from typing import cast

import psycopg
import pytest

from app.services import filing_documents
from app.services.filing_documents import (
    ingest_filing_documents,
    list_filing_documents,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable"),
]


class _StubIndexFetcher:
    def __init__(self, by_accession: dict[str, dict[str, object] | None]) -> None:
        self._by = by_accession
        self.calls: list[tuple[str, str | None]] = []

    def fetch_filing_index(
        self,
        accession: str,
        *,
        issuer_cik: str | None = None,
    ) -> dict[str, object] | None:
        self.calls.append((accession, issuer_cik))
        return self._by.get(accession)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int = 501, *, cik: str = "0000320193") -> int:
    """Seed a tradable instrument with a primary SEC CIK identifier.

    The ingester's candidate selector requires a primary
    ``external_identifiers`` row of provider='sec',
    identifier_type='cik', so the parser can build SEC archive URLs.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
            "VALUES (%s, %s, %s, TRUE) RETURNING instrument_id",
            (iid, f"APEX{iid}", f"Apex Inc. {iid}"),
        )
        row = cur.fetchone()
        assert row is not None
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, cik),
        )
    conn.commit()
    return int(row[0])


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str | None = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000001/apex-10k.htm",
    filing_date: str = "2026-04-01",
    filing_type: str = "10-K",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url)
            VALUES (%s, %s, %s, 'sec', %s, %s)
            RETURNING filing_event_id
            """,
            (instrument_id, filing_date, filing_type, accession, url),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


# Real SEC index.json shape — see test_filing_documents.py for the
# verified-against-live-SEC fixture rationale (#723).
_INDEX_JSON: dict[str, object] = {
    "directory": {
        "name": "/Archives/edgar/data/320193/000032019324000001",
        "item": [
            {"name": "apex-10k.htm", "type": "text.gif", "size": "999000", "last-modified": "2026-04-01 09:00:00"},
            {"name": "ex-21.htm", "type": "text.gif", "size": "2000", "last-modified": "2026-04-01 09:00:00"},
            {"name": "ex-99-1.htm", "type": "text.gif", "size": "5000", "last-modified": "2026-04-01 09:00:00"},
        ],
    }
}


class TestIngestFilingDocuments:
    def test_documents_land_per_filing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        fid = _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000320193-24-000001",
        )
        fetcher = _StubIndexFetcher({"0000320193-24-000001": _INDEX_JSON})

        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_parsed == 1
        assert result.documents_inserted == 3

        docs = list_filing_documents(ebull_test_conn, filing_event_id=fid)
        assert len(docs) == 3
        # Primary document always sorts first; primary derived from
        # filing_events.primary_document_url filename.
        assert docs[0].is_primary is True
        assert docs[0].document_name == "apex-10k.htm"
        # Type/description NULL on this code path — see module docstring.
        assert docs[0].document_type is None
        assert docs[0].description is None

    def test_rerun_skips_filings_with_existing_children(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=502)
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000320193-24-000002",
        )
        fetcher = _StubIndexFetcher({"0000320193-24-000002": _INDEX_JSON})
        ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        # Second pass: existing child rows mean the filing is no
        # longer a candidate.
        second = _StubIndexFetcher({"0000320193-24-000002": _INDEX_JSON})
        ingest_filing_documents(ebull_test_conn, cast("object", second))  # type: ignore[arg-type]
        assert second.calls == []

    def test_fetch_404_increments_fetch_errors_without_raising(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=503)
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000320193-24-000003",
        )
        fetcher = _StubIndexFetcher({"0000320193-24-000003": None})
        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.fetch_errors == 1
        assert result.filings_parsed == 0

    def test_non_sec_filings_ignored(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, iid=504)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO filing_events
                    (instrument_id, filing_date, filing_type, provider,
                     provider_filing_id, primary_document_url)
                VALUES (%s, CURRENT_DATE, '10-K', 'companies_house',
                        'CH-1', 'https://example.com/x.pdf')
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        fetcher = _StubIndexFetcher({})
        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []

    def test_filing_without_primary_url_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Ingest selector requires ``primary_document_url`` — without
        it we cannot flag the submission's primary document, and an
        all-rows-non-primary listing is misleading. Such filings skip
        silently and re-qualify automatically once the URL is
        populated upstream."""
        iid = _seed_instrument(ebull_test_conn, iid=505)
        _seed_filing(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000320193-24-000005",
            url=None,
        )
        fetcher = _StubIndexFetcher({"0000320193-24-000005": _INDEX_JSON})
        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []

    def test_filing_without_cik_mapping_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Ingest selector requires a primary SEC CIK identifier on
        the parent instrument so the URL builder has a CIK to
        substitute. Without one, the JOIN produces zero rows and the
        filing is skipped silently."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
                "VALUES (506, 'NOID', 'No CIK Inc.', TRUE)"
            )
            cur.execute(
                """
                INSERT INTO filing_events
                    (instrument_id, filing_date, filing_type, provider,
                     provider_filing_id, primary_document_url)
                VALUES (506, CURRENT_DATE, '10-K', 'sec',
                        '0000000506-24-000001', 'https://example.com/x.htm')
                """,
            )
        ebull_test_conn.commit()
        fetcher = _StubIndexFetcher({"0000000506-24-000001": _INDEX_JSON})
        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0


class _TickRecorder:
    """Records ``report_progress`` CALL SITES, not callback emissions.

    #2274. The three layers are distinct and this fixture pins only the
    first: the call site here, the 5-item/10s throttle in
    ``sync_orchestrator.progress.report_progress``, and the 5s write floor in
    ``job_heartbeat.JobRunHeartbeat``. Asserting "N rows reached job_runs"
    from this test would be asserting two other modules' throttles; each
    owns its own tests. What can only be pinned here is that the ingester
    ticks once per attempted item on EVERY branch — the #3080 lesson that a
    probe at the helper's layer cannot see a caller that drops the call.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[int, int | None, bool]] = []

    def __call__(
        self,
        items_done: int,
        items_total: int | None,
        *,
        force: bool = False,
        **_: object,
    ) -> None:
        self.calls.append((items_done, items_total, force))


# Deliberately NOT a multiple of ``report_progress``'s 5-item throttle: a
# batch size that aligns with it would let the last iteration emit the final
# count incidentally, so a missing ``force=True`` tail tick would still pass.
_OFF_THROTTLE_BATCH = 7


def _seed_batch(conn: psycopg.Connection[tuple], n: int, *, iid: int) -> list[str]:
    _seed_instrument(conn, iid=iid)
    accessions = [f"{iid:010d}-24-{i:06d}" for i in range(1, n + 1)]
    for accession in accessions:
        _seed_filing(conn, instrument_id=iid, accession=accession)
    return accessions


class TestIngestFilingDocumentsHeartbeat:
    """#2274 — the job's ``job_runs`` heartbeat comes from this loop."""

    def test_ticks_once_per_item_plus_a_forced_final(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        accessions = _seed_batch(ebull_test_conn, _OFF_THROTTLE_BATCH, iid=520)
        fetcher = _StubIndexFetcher(dict.fromkeys(accessions, _INDEX_JSON))
        recorder = _TickRecorder()
        monkeypatch.setattr(filing_documents, "report_progress", recorder)

        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_parsed == _OFF_THROTTLE_BATCH
        n = _OFF_THROTTLE_BATCH
        assert recorder.calls == [(i, n, False) for i in range(1, n + 1)] + [(n, n, True)]

    def test_ticks_on_every_item_when_every_fetch_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A batch whose every fetch fails is still one SEC round trip per
        item — a live worker, not a wedged one — so it must keep ticking.
        This is the branch a tick placed on the success path would lose."""
        accessions = _seed_batch(ebull_test_conn, _OFF_THROTTLE_BATCH, iid=521)

        class _Exploding:
            def fetch_filing_index(self, accession: str, *, issuer_cik: str | None = None) -> None:
                raise RuntimeError(f"boom {accession} {issuer_cik}")

        recorder = _TickRecorder()
        monkeypatch.setattr(filing_documents, "report_progress", recorder)

        result = ingest_filing_documents(ebull_test_conn, cast("object", _Exploding()))  # type: ignore[arg-type]

        assert result.fetch_errors == _OFF_THROTTLE_BATCH
        assert result.filings_parsed == 0
        assert len(accessions) == _OFF_THROTTLE_BATCH
        assert [done for done, _total, _force in recorder.calls] == [
            *range(1, _OFF_THROTTLE_BATCH + 1),
            _OFF_THROTTLE_BATCH,
        ]

    def test_ticks_on_every_item_when_every_upsert_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        accessions = _seed_batch(ebull_test_conn, _OFF_THROTTLE_BATCH, iid=522)
        fetcher = _StubIndexFetcher(dict.fromkeys(accessions, _INDEX_JSON))
        recorder = _TickRecorder()

        def _explode(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("upsert boom")

        monkeypatch.setattr(filing_documents, "upsert_filing_documents", _explode)
        monkeypatch.setattr(filing_documents, "report_progress", recorder)

        result = ingest_filing_documents(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_parsed == 0
        assert [done for done, _total, _force in recorder.calls] == [
            *range(1, _OFF_THROTTLE_BATCH + 1),
            _OFF_THROTTLE_BATCH,
        ]

    def test_empty_batch_ticks_nothing_at_all(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A run that attempted nothing must not stamp ``last_progress_at``.

        That is the fabricated-progress claim ``set_active_progress``'s
        ``initial_tick=False`` exists to forbid — a heartbeat on a no-op run
        would defer a ``dev_reload`` reload and mute ``mid_flight_stuck``
        while nothing was happening.
        """
        recorder = _TickRecorder()
        monkeypatch.setattr(filing_documents, "report_progress", recorder)

        result = ingest_filing_documents(ebull_test_conn, cast("object", _StubIndexFetcher({})))  # type: ignore[arg-type]

        assert result.filings_scanned == 0
        assert recorder.calls == []
