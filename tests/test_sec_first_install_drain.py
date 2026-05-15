"""Tests for first-install drain (#871) + N-CSR bootstrap drain (#1174)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, timedelta

import psycopg
import pytest

from app.jobs.sec_first_install_drain import (
    bootstrap_n_csr_drain,
    run_first_install_drain,
    seed_manifest_from_filing_events,
)
from app.services.bootstrap_preconditions import BootstrapPhaseSkipped
from app.services.bootstrap_state import BootstrapStageCancelled
from app.services.sec_manifest import get_manifest_row
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


_AAPL_RECENT = {
    "cik": "320193",
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-26-000001", "0000320193-26-000002"],
            "filingDate": ["2026-01-15", "2026-02-14"],
            "form": ["8-K", "DEF 14A"],
            "acceptanceDateTime": [
                "2026-01-15T16:30:00.000Z",
                "2026-02-14T08:00:00.000Z",
            ],
            "primaryDocument": ["item502.htm", "proxy.htm"],
        },
        "files": [],
    },
}


def _seed_aapl(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
        """
    )
    conn.execute("INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (1701, '0000320193')")
    conn.commit()


def _fake_get(payload: dict):
    body = json.dumps(payload).encode("utf-8")

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return 200, body

    return _impl


class TestDrain:
    def test_drains_universe_in_order(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        assert stats.ciks_processed == 1
        assert stats.manifest_rows_upserted == 2

        for accession in ("0000320193-26-000001", "0000320193-26-000002"):
            row = get_manifest_row(ebull_test_conn, accession)
            assert row is not None
            assert row.subject_type == "issuer"
            assert row.instrument_id == 1701

    def test_idempotent_on_rerun(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()
        run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sec_filing_manifest")
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 2

    def test_bulk_zip_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(NotImplementedError, match="bulk-zip drain not yet implemented"):
            run_first_install_drain(
                ebull_test_conn,
                http_get=_fake_get(_AAPL_RECENT),
                use_bulk_zip=True,
            )

    def test_drain_seeds_data_freshness_index(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #937: drain MUST leave both manifest AND scheduler queryable
        # so the per-CIK poll (#870) finds work post-drain. Pre-fix
        # the scheduler stayed empty for the drained scope.
        _seed_aapl(ebull_test_conn)
        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        assert stats.manifest_rows_upserted == 2
        # AAPL recent has two distinct sources: sec_8k + sec_def14a.
        # Each (issuer, instrument_id, source) triple gets one
        # data_freshness_index row. #959 round 1: pin EXACT count
        # so a future regression that under- or over-counts the
        # inline-seeded triples is caught.
        assert stats.scheduler_rows_seeded == 2

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT source FROM data_freshness_index
                WHERE subject_type = 'issuer' AND subject_id = '1701'
                ORDER BY source
                """
            )
            sources = [row[0] for row in cur.fetchall()]
        assert sources == ["sec_8k", "sec_def14a"]

    def test_max_subjects_caps(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        # Add a second issuer
        ebull_test_conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (1702, 'X', 'X Inc', '4', 'USD', TRUE)
            """
        )
        ebull_test_conn.execute("INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (1702, '0000999999')")
        ebull_test_conn.commit()

        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
            max_subjects=1,
        )
        ebull_test_conn.commit()
        assert stats.ciks_processed == 1


class TestSeedFromFilingEvents:
    def test_seeds_manifest_from_filing_events_no_http(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #1044: when filing_events has rows for the SEC provider,
        # the drain seeds sec_filing_manifest from that table without
        # any HTTP. The fake_get below would raise if called — proves
        # the fast path was taken.
        _seed_aapl(ebull_test_conn)
        ebull_test_conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES
                (1701, 'sec', 'cik', '0000320193', TRUE)
            ON CONFLICT DO NOTHING
            """
        )
        ebull_test_conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type, provider,
                provider_filing_id, source_url, primary_document_url, raw_payload_json
            )
            VALUES
                (1701, %s, '8-K', 'sec', '0000320193-26-000001',
                 'https://www.sec.gov/...', 'https://www.sec.gov/.../item502.htm',
                 %s::jsonb),
                (1701, %s, 'DEF 14A', 'sec', '0000320193-26-000002',
                 'https://www.sec.gov/...', 'https://www.sec.gov/.../proxy.htm',
                 %s::jsonb)
            """,
            (
                date(2026, 1, 15),
                json.dumps({"provider_filing_id": "0000320193-26-000001"}),
                date(2026, 2, 14),
                json.dumps({"provider_filing_id": "0000320193-26-000002"}),
            ),
        )
        ebull_test_conn.commit()

        n = seed_manifest_from_filing_events(ebull_test_conn)
        ebull_test_conn.commit()

        assert n == 2
        for accession in ("0000320193-26-000001", "0000320193-26-000002"):
            row = get_manifest_row(ebull_test_conn, accession)
            assert row is not None
            assert row.subject_type == "issuer"
            assert row.instrument_id == 1701
            assert row.cik == "0000320193"

    def test_skips_non_issuer_sources(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # PR #1051 review WARNING: 13F-HR / N-PORT / N-CSR rows in
        # filing_events must NOT be seeded as subject_type='issuer'
        # — they're filer-scoped manifest rows (instrument_id=NULL,
        # subject_id=filer CIK). The seed must skip them so the
        # legacy/per-CIK path can correctly classify them.
        _seed_aapl(ebull_test_conn)
        ebull_test_conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (1701, 'sec', 'cik', '0000320193', TRUE)
            ON CONFLICT DO NOTHING
            """
        )
        # Insert one issuer-scoped (10-K) and one filer-scoped (13F-HR).
        ebull_test_conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type, provider,
                provider_filing_id, source_url, primary_document_url, raw_payload_json
            )
            VALUES
                (1701, %s, '10-K', 'sec', '0000320193-26-000010',
                 'https://www.sec.gov/...', NULL, %s::jsonb),
                (1701, %s, '13F-HR', 'sec', '0000320193-26-000011',
                 'https://www.sec.gov/...', NULL, %s::jsonb)
            """,
            (
                date(2026, 1, 15),
                json.dumps({"provider_filing_id": "0000320193-26-000010"}),
                date(2026, 2, 14),
                json.dumps({"provider_filing_id": "0000320193-26-000011"}),
            ),
        )
        ebull_test_conn.commit()

        n = seed_manifest_from_filing_events(ebull_test_conn)
        ebull_test_conn.commit()

        # Only the 10-K should land — 13F-HR is filer-scoped.
        assert n == 1
        assert get_manifest_row(ebull_test_conn, "0000320193-26-000010") is not None
        assert get_manifest_row(ebull_test_conn, "0000320193-26-000011") is None

    def test_run_first_install_drain_uses_filing_events_fast_path(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #1044: when filing_events seeds the issuer rows, the per-CIK
        # HTTP path is skipped for issuer subjects. Run with a fake
        # get that raises — proves no HTTP was issued for the issuer.
        _seed_aapl(ebull_test_conn)
        ebull_test_conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES
                (1701, 'sec', 'cik', '0000320193', TRUE)
            ON CONFLICT DO NOTHING
            """
        )
        ebull_test_conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type, provider,
                provider_filing_id, source_url, primary_document_url, raw_payload_json
            )
            VALUES (1701, %s, '8-K', 'sec', '0000320193-26-000001',
                    'https://www.sec.gov/...', NULL, %s::jsonb)
            """,
            (
                date(2026, 1, 15),
                json.dumps({"provider_filing_id": "0000320193-26-000001"}),
            ),
        )
        ebull_test_conn.commit()

        def _raising_get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            raise AssertionError(f"HTTP fast-path bypass failed: {url}")

        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_raising_get,
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        assert stats.rows_seeded_from_filing_events == 1
        # ciks_skipped picks up the issuer subject the loop short-circuited.
        assert stats.ciks_skipped >= 1
        assert stats.manifest_rows_upserted >= 1
        # #959 round 1: the fast path's record_manifest_entry calls
        # inline-seed data_freshness_index too, and the fast-path
        # seeder now records into the inline_seeded_triples accumulator.
        # Pin BOTH the scheduler_rows_seeded counter AND a direct
        # query against data_freshness_index so a regression in either
        # the counter wiring or the inline-seed plumbing is caught.
        assert stats.scheduler_rows_seeded == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT source FROM data_freshness_index
                WHERE subject_type = 'issuer' AND subject_id = '1701'
                ORDER BY source
                """
            )
            sources = [row[0] for row in cur.fetchall()]
        assert sources == ["sec_8k"]


# ---------------------------------------------------------------------------
# #1174 — N-CSR / N-CSRS fund-scoped bootstrap drain (T8 deferred from #1171).
# ---------------------------------------------------------------------------


_VANGUARD_CIK = "0000036405"


def _make_submissions_payload(
    *,
    cik: str,
    rows: Sequence[tuple[str, str, str, str | None]],
    files: list[str] | None = None,
) -> dict:
    """Build a primary-page submissions.json payload.

    ``rows`` is a list of ``(accession, filingDate, form, primaryDocument_or_None)``
    tuples in the same order as SEC emits them.
    """
    return {
        "cik": cik.lstrip("0") or "0",
        "filings": {
            "recent": {
                "accessionNumber": [r[0] for r in rows],
                "filingDate": [r[1] for r in rows],
                "form": [r[2] for r in rows],
                "acceptanceDateTime": [f"{r[1]}T16:30:00.000Z" for r in rows],
                "primaryDocument": [r[3] or "" for r in rows],
            },
            "files": [{"name": name} for name in (files or [])],
        },
    }


def _make_secondary_payload(
    *,
    cik: str,
    rows: Sequence[tuple[str, str, str, str | None]],
) -> dict:
    """Build a secondary-page payload. SEC's secondary pages either
    carry the top-level parallel-array shape OR are wrapped under
    ``filings.recent`` — parse_submissions_page handles both. We use the
    wrapped form so the same builder applies."""
    return _make_submissions_payload(cik=cik, rows=rows, files=None)


def _by_url(mapping: dict[str, dict | bytes | Exception | int]):
    """Build an http_get callable keyed by URL.

    Values can be:
    - dict: returns (200, json.dumps(body))
    - bytes: returns (200, body)
    - int: returns (status, b"") — for 404 / 500 paths
    - Exception: raised
    """

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        try:
            value = mapping[url]
        except KeyError:
            raise AssertionError(f"unexpected URL in test fake_get: {url}")
        if isinstance(value, Exception):
            raise value
        if isinstance(value, int):
            return value, b""
        if isinstance(value, bytes):
            return 200, value
        if isinstance(value, dict):
            return 200, json.dumps(value).encode("utf-8")
        raise AssertionError(f"unsupported _by_url value type {type(value)!r}")

    return _impl


def _seed_trust(conn: psycopg.Connection[tuple], trust_cik: str) -> None:
    """Seed one row in ``cik_refresh_mf_directory`` for a trust."""
    conn.execute(
        """
        INSERT INTO cik_refresh_mf_directory (class_id, series_id, symbol, trust_cik)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (class_id) DO NOTHING
        """,
        (f"C{trust_cik[-9:]}", f"S{trust_cik[-9:]}", "VFIAX", trust_cik),
    )
    conn.commit()


def _seed_n_trusts(conn: psycopg.Connection[tuple], n: int) -> list[str]:
    """Seed N distinct trusts with synthetic CIKs."""
    ciks: list[str] = []
    with conn.cursor() as cur:
        for i in range(n):
            cik = str(100000 + i).zfill(10)
            ciks.append(cik)
            cur.execute(
                """
                INSERT INTO cik_refresh_mf_directory (class_id, series_id, symbol, trust_cik)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (class_id) DO NOTHING
                """,
                (f"C{cik[-9:]}", f"S{cik[-9:]}", f"FUND{i:04d}", cik),
            )
    conn.commit()
    return ciks


def _submissions_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"


class TestNCsrBootstrapDrain:
    """Spec §6 cases 1-11."""

    # ------------------------------------------------------------------
    # Case 1 — first-run writes manifest rows
    # ------------------------------------------------------------------
    def test_first_run_writes_manifest_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        today = date.today()
        rows = [
            ("0001104659-26-000001", today.isoformat(), "N-CSR", "ncsr1.htm"),
            ("0001104659-26-000002", today.isoformat(), "N-CSR/A", "ncsr2.htm"),
            ("0001104659-26-000003", today.isoformat(), "N-CSRS", "ncsrs1.htm"),
            ("0001104659-26-000004", today.isoformat(), "N-CSRS/A", "ncsrs2.htm"),
            ("0001104659-26-000005", today.isoformat(), "N-CSR", "ncsr3.htm"),
            # Non-N-CSR rows — must be filtered out by source.
            ("0001104659-26-000010", today.isoformat(), "10-K", "10k.htm"),
            ("0001104659-26-000011", today.isoformat(), "8-K", "8k.htm"),
            ("0001104659-26-000012", today.isoformat(), "NPORT-P", "nport.htm"),
            ("0001104659-26-000013", today.isoformat(), "DEF 14A", "proxy.htm"),
            ("0001104659-26-000014", today.isoformat(), "13F-HR", "13f.htm"),
        ]
        payload = _make_submissions_payload(cik=_VANGUARD_CIK, rows=rows)
        http_get = _by_url({_submissions_url(_VANGUARD_CIK): payload})

        stats = bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        assert stats.manifest_rows_upserted == 5
        assert stats.trusts_processed == 1
        assert stats.trusts_skipped == 0
        assert stats.errors == 0
        assert stats.accessions_outside_horizon == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, subject_type, subject_id, instrument_id
                FROM sec_filing_manifest
                WHERE accession_number LIKE '0001104659-26-%'
                ORDER BY accession_number
                """
            )
            manifest_rows = cur.fetchall()
        assert len(manifest_rows) == 5
        for row in manifest_rows:
            source, subject_type, subject_id, instrument_id = row
            assert source == "sec_n_csr"
            assert subject_type == "institutional_filer"
            assert subject_id == _VANGUARD_CIK
            assert instrument_id is None

    # ------------------------------------------------------------------
    # Case 2 — idempotent re-run
    # ------------------------------------------------------------------
    def test_idempotent_rerun(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        today = date.today()
        rows = [
            ("0001104659-26-000001", today.isoformat(), "N-CSR", "ncsr1.htm"),
            ("0001104659-26-000003", today.isoformat(), "N-CSRS", "ncsrs1.htm"),
        ]
        payload = _make_submissions_payload(cik=_VANGUARD_CIK, rows=rows)
        http_get = _by_url({_submissions_url(_VANGUARD_CIK): payload})

        bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        # Move one manifest row to a non-pending state to verify the UPSERT
        # does not flip it back to ``pending``.
        ebull_test_conn.execute(
            "UPDATE sec_filing_manifest SET ingest_status = 'parsed' WHERE accession_number = '0001104659-26-000001'"
        )
        ebull_test_conn.commit()

        stats2 = bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        assert stats2.manifest_rows_upserted == 2  # UPSERT touches both
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT ingest_status FROM sec_filing_manifest WHERE accession_number = '0001104659-26-000001'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "parsed"  # NOT flipped back to pending
            cur.execute("SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number LIKE '0001104659-26-%'")
            count_row = cur.fetchone()
            assert count_row is not None
            assert int(count_row[0]) == 2

    # ------------------------------------------------------------------
    # Case 3 — 2-year horizon truncation (primary page)
    # ------------------------------------------------------------------
    def test_horizon_truncates_old_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        today = date.today()
        old = today - timedelta(days=800)
        rows = [
            ("0001104659-26-000001", today.isoformat(), "N-CSR", "ncsr1.htm"),
            ("0001104659-24-000001", old.isoformat(), "N-CSR", "ncsr_old.htm"),
        ]
        payload = _make_submissions_payload(cik=_VANGUARD_CIK, rows=rows)
        http_get = _by_url({_submissions_url(_VANGUARD_CIK): payload})

        stats = bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        assert stats.manifest_rows_upserted == 1
        assert stats.accessions_outside_horizon == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT accession_number FROM sec_filing_manifest WHERE accession_number LIKE '0001104659-2_-000001'"
            )
            accs = sorted(row[0] for row in cur.fetchall())
        assert accs == ["0001104659-26-000001"]

    # ------------------------------------------------------------------
    # Case 4 — CHECK constraint honored
    # ------------------------------------------------------------------
    def test_check_constraint_honored(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        today = date.today()
        rows = [("0001104659-26-000001", today.isoformat(), "N-CSR", "ncsr1.htm")]
        payload = _make_submissions_payload(cik=_VANGUARD_CIK, rows=rows)
        http_get = _by_url({_submissions_url(_VANGUARD_CIK): payload})

        bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        # The DB CHECK constraint ``chk_manifest_issuer_has_instrument`` would
        # have aborted the INSERT if the wiring were wrong; reaching this line
        # already proves the contract. Re-assert defensively.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT subject_type, instrument_id FROM sec_filing_manifest "
                "WHERE accession_number = '0001104659-26-000001'"
            )
            row = cur.fetchone()
        assert row is not None
        subject_type, instrument_id = row
        assert subject_type == "institutional_filer"
        assert instrument_id is None

    # ------------------------------------------------------------------
    # Case 5 — Cancel signal observed mid-run (bounded poll cadence)
    # ------------------------------------------------------------------
    def test_cancel_signal_mid_run(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Seed 75 trusts so we exercise the 50-trust poll cadence on the
        # second poll cycle. Cancel flips True after the drain has
        # processed at least one trust — so we observe mid-run, not
        # before-first-fetch.
        ciks = _seed_n_trusts(ebull_test_conn, 75)
        today = date.today()
        empty_payload = _make_submissions_payload(cik="0", rows=[])
        url_map: dict[str, dict | bytes | Exception | int] = {_submissions_url(cik): empty_payload for cik in ciks}
        # Return one row to keep the drain advancing through trusts.
        rows = [("0001104659-26-000099", today.isoformat(), "N-CSR", "n.htm")]
        for cik in ciks:
            url_map[_submissions_url(cik)] = _make_submissions_payload(cik=cik, rows=rows)

        # Cancel after first poll has cleared (n=0). The drain polls at
        # n % 50 == 0, so the second poll fires at n=50; we want cancel
        # observed there.
        call_count = {"http": 0}
        cancel_state = {"flag": False}

        original_http = _by_url(url_map)

        def counting_http(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            call_count["http"] += 1
            if call_count["http"] >= 5:
                # After 5 HTTP calls (well into the run, past n=0 poll),
                # arm cancel for the next poll cycle.
                cancel_state["flag"] = True
            return original_http(url, headers)

        def fake_cancel_requested(*args, **kwargs) -> bool:
            del args, kwargs
            return cancel_state["flag"]

        monkeypatch.setattr(
            "app.jobs.sec_first_install_drain.bootstrap_cancel_requested",
            fake_cancel_requested,
        )

        with pytest.raises(BootstrapStageCancelled):
            bootstrap_n_csr_drain(ebull_test_conn, http_get=counting_http)
        ebull_test_conn.rollback()

        from app.jobs.sec_first_install_drain import (
            _N_CSR_DRAIN_CANCEL_POLL_EVERY_N,
        )

        # Cancel observed mid-run, NOT before first fetch.
        assert call_count["http"] > 1
        # Bounded poll cadence — drain must observe cancel on the next
        # poll cycle after the cancel flag fires (n=50 in default config),
        # NOT after walking all 75 seeded trusts. Tight bound per spec
        # §6 case 5 + Codex 2 WARNING.
        assert call_count["http"] <= _N_CSR_DRAIN_CANCEL_POLL_EVERY_N + 1

    # ------------------------------------------------------------------
    # Case 6 — Secondary-page pagination + horizon
    # ------------------------------------------------------------------
    def test_pagination_with_horizon_filter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        today = date.today()
        old = today - timedelta(days=900)
        primary = _make_submissions_payload(
            cik=_VANGUARD_CIK,
            rows=[("0001104659-26-000001", today.isoformat(), "N-CSR", "n1.htm")],
            files=["CIK0000036405-submissions-001.json", "CIK0000036405-submissions-002.json"],
        )
        page1 = _make_secondary_payload(
            cik=_VANGUARD_CIK,
            rows=[(f"0001104659-26-00010{i}", today.isoformat(), "N-CSR", f"p1_{i}.htm") for i in range(5)],
        )
        page2 = _make_secondary_payload(
            cik=_VANGUARD_CIK,
            rows=[
                ("0001104659-24-000020", old.isoformat(), "N-CSR", "old1.htm"),
                ("0001104659-24-000021", old.isoformat(), "N-CSR", "old2.htm"),
                ("0001104659-24-000022", old.isoformat(), "N-CSR", "old3.htm"),
                # NPORT-P rows must NOT be enqueued.
                ("0001104659-24-000030", old.isoformat(), "NPORT-P", "np1.htm"),
                ("0001104659-24-000031", old.isoformat(), "NPORT-P", "np2.htm"),
            ],
        )
        url_map: dict[str, dict | bytes | Exception | int] = {
            _submissions_url(_VANGUARD_CIK): primary,
            "https://data.sec.gov/submissions/CIK0000036405-submissions-001.json": page1,
            "https://data.sec.gov/submissions/CIK0000036405-submissions-002.json": page2,
        }
        http_get = _by_url(url_map)

        stats = bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        # 1 primary in-horizon + 5 page1 in-horizon = 6 manifest rows.
        assert stats.manifest_rows_upserted == 6
        assert stats.secondary_pages_fetched == 2
        # 3 old N-CSR rows on page2 are filtered by horizon (NPORT-P rows
        # are filtered earlier by source, NOT counted as outside-horizon).
        assert stats.accessions_outside_horizon == 3
        # No NPORT-P rows enqueued.
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number LIKE '0001104659-24-00003%'")
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 0

    # ------------------------------------------------------------------
    # Case 7 — Secondary-page source filter
    # ------------------------------------------------------------------
    def test_secondary_source_filter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        today = date.today()
        primary = _make_submissions_payload(
            cik=_VANGUARD_CIK,
            rows=[],
            files=["CIK0000036405-submissions-001.json"],
        )
        page1 = _make_secondary_payload(
            cik=_VANGUARD_CIK,
            rows=[
                ("0001104659-26-000040", today.isoformat(), "N-CSR", "ncsr.htm"),
                ("0001104659-26-000041", today.isoformat(), "10-K", "10k.htm"),
                ("0001104659-26-000042", today.isoformat(), "13F-HR", "13f.htm"),
                ("0001104659-26-000043", today.isoformat(), "N-CSRS", "ncsrs.htm"),
            ],
        )
        url_map: dict[str, dict | bytes | Exception | int] = {
            _submissions_url(_VANGUARD_CIK): primary,
            "https://data.sec.gov/submissions/CIK0000036405-submissions-001.json": page1,
        }
        http_get = _by_url(url_map)

        stats = bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        assert stats.manifest_rows_upserted == 2  # only N-CSR + N-CSRS
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT accession_number, source FROM sec_filing_manifest "
                "WHERE accession_number LIKE '0001104659-26-00004%' ORDER BY accession_number"
            )
            rows = cur.fetchall()
        assert [(r[0], r[1]) for r in rows] == [
            ("0001104659-26-000040", "sec_n_csr"),
            ("0001104659-26-000043", "sec_n_csr"),
        ]

    # ------------------------------------------------------------------
    # Case 8 — 404 submissions.json
    # ------------------------------------------------------------------
    def test_404_submissions(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        http_get = _by_url({_submissions_url(_VANGUARD_CIK): 404})

        stats = bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        assert stats.trusts_processed == 1
        assert stats.trusts_skipped == 1
        assert stats.errors == 0
        assert stats.manifest_rows_upserted == 0

    # ------------------------------------------------------------------
    # Case 9 — Fetch exception
    # ------------------------------------------------------------------
    def test_fetch_exception_counted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        ciks = _seed_n_trusts(ebull_test_conn, 2)
        today = date.today()
        good_payload = _make_submissions_payload(
            cik=ciks[1],
            rows=[("0001104659-26-000050", today.isoformat(), "N-CSR", "n.htm")],
        )
        http_get = _by_url(
            {
                _submissions_url(ciks[0]): RuntimeError("simulated transient"),
                _submissions_url(ciks[1]): good_payload,
            }
        )

        stats = bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        assert stats.trusts_processed == 2
        assert stats.errors == 1
        assert stats.manifest_rows_upserted == 1  # second trust succeeded

    # ------------------------------------------------------------------
    # Case 10 — Empty trust cohort raises BootstrapPhaseSkipped
    # ------------------------------------------------------------------
    def test_empty_cohort_raises_phase_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # No seed — directory empty.
        called = {"count": 0}

        def fail_http(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            called["count"] += 1
            return 200, b""

        with pytest.raises(BootstrapPhaseSkipped, match="cik_refresh_mf_directory empty"):
            bootstrap_n_csr_drain(ebull_test_conn, http_get=fail_http)
        ebull_test_conn.rollback()
        assert called["count"] == 0  # No HTTP — guard fires first.

    # ------------------------------------------------------------------
    # Case 11 — Scheduler / freshness side-effect (#956 contract)
    # ------------------------------------------------------------------
    def test_freshness_side_effect(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_trust(ebull_test_conn, _VANGUARD_CIK)
        today = date.today()
        rows = [
            ("0001104659-26-000060", today.isoformat(), "N-CSR", "n.htm"),
            ("0001104659-26-000061", today.isoformat(), "N-CSRS", "ns.htm"),
        ]
        payload = _make_submissions_payload(cik=_VANGUARD_CIK, rows=rows)
        http_get = _by_url({_submissions_url(_VANGUARD_CIK): payload})

        bootstrap_n_csr_drain(ebull_test_conn, http_get=http_get)
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM data_freshness_index
                WHERE subject_type = 'institutional_filer'
                  AND subject_id = %s
                  AND source = 'sec_n_csr'
                """,
                (_VANGUARD_CIK,),
            )
            row = cur.fetchone()
        assert row is not None
        # #956 contract — one row per (subject_type, subject_id, source).
        assert int(row[0]) == 1
