"""Tests for ``app.services.sec_submissions_files_walk.walk_files_pages``
(Stream A PR-B T1.3, #1233) — the S14 sidecar consumer that REPLACED the
pre-PR-B "re-fetch primary submissions.json per CIK" behaviour.

Covers:
  * Sidecar-empty → ``ciks_with_empty_sidecar`` + ``parse_errors`` both
    increment for an in-universe CIK that is NOT an agent.
  * Sidecar with only sentinel → ``ciks_with_no_overflow`` increments;
    no HTTP issued; ``parse_errors`` stays 0.
  * Sidecar with real pages → secondary pages fetched (HTTP); fixture
    accessions appended to filing_events.
  * Agent CIK with empty sidecar (expected) → silently skipped; NEITHER
    ``parse_errors`` NOR ``ciks_with_empty_sidecar`` increments.
  * Contract: ZERO PRIMARY ``data.sec.gov/submissions/CIK<10>.json``
    HTTP calls when the sidecar is populated. Pinned via ``respx`` at
    the httpx transport layer so a future caller bypassing
    ``SecFilingsProvider`` would still trip the assertion.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import psycopg.rows
import pytest
import respx
from httpx import Response

from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS
from app.services.sec_submissions_files_walk import walk_files_pages

_REAL_CIK = "0009999998"
_REAL_SYMBOL = "STREAMA"
_OVERFLOW_PAGE = f"CIK{_REAL_CIK}-submissions-001.json"


def _wipe_test_instrument(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (_REAL_CIK,))
        cur.execute(
            "DELETE FROM external_identifiers "
            "WHERE provider = 'sec' AND identifier_type = 'cik' AND identifier_value = %s",
            (_REAL_CIK,),
        )
        cur.execute("DELETE FROM instruments WHERE symbol = %s", (_REAL_SYMBOL,))
    conn.commit()


def _seed_test_instrument(conn: psycopg.Connection[tuple]) -> int:
    """``instruments.instrument_id`` is BIGINT PRIMARY KEY (no DEFAULT,
    no sequence — manually assigned per sql/001:2). Allocate one
    above the current MAX so we don't clash with prod-like data the
    test DB may already carry."""
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(instrument_id), 0) FROM instruments")
        row = cur.fetchone()
        assert row is not None
        iid = int(row[0]) + 1
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable) "
            "VALUES (%s, %s, %s, %s, TRUE)",
            (iid, _REAL_SYMBOL, "Stream A Test Co.", "NASDAQ"),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, _REAL_CIK),
        )
    conn.commit()
    return iid


def _insert_sidecar(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    pages: list[tuple[str, str | None, str | None]],
) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (cik,))
        for name, ff, ft in pages:
            cur.execute(
                "INSERT INTO sec_cik_submissions_files_index "
                "(cik, page_name, filing_from, filing_to, bootstrap_run_id, populate_origin) "
                "VALUES (%s, %s, %s, %s, NULL, 'steady_state')",
                (cik, name, ff, ft),
            )
    conn.commit()


@pytest.fixture
def s14_test_instrument(
    ebull_test_conn: psycopg.Connection[tuple],
) -> Iterator[int]:
    _wipe_test_instrument(ebull_test_conn)
    iid = _seed_test_instrument(ebull_test_conn)
    yield iid
    _wipe_test_instrument(ebull_test_conn)


class TestS14SidecarConsume:
    """All assertions use DELTA against a BASELINE call so the tests are
    robust to other rows in the shared per-worker test DB (per PR #1308
    review bot BLOCKING — exact equality on DB-global counters is a
    flake vector on any non-empty test DB)."""

    @pytest.mark.integration
    def test_empty_sidecar_for_in_universe_cik_is_parse_error(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        # WIPE FIRST so a dirty _REAL_CIK row left by a killed prior
        # run doesn't get counted in baseline; then capture baseline;
        # then seed + measure delta (bot review iter 2 WARNING — baseline-
        # before-wipe was a flake vector).
        _wipe_test_instrument(ebull_test_conn)
        baseline = walk_files_pages(conn=ebull_test_conn)
        _seed_test_instrument(ebull_test_conn)
        try:
            # No sidecar row inserted for _REAL_CIK — empty sidecar branch.
            after = walk_files_pages(conn=ebull_test_conn)

            assert after.ciks_with_empty_sidecar - baseline.ciks_with_empty_sidecar == 1
            assert after.parse_errors - baseline.parse_errors == 1
            assert after.secondary_pages_fetched == baseline.secondary_pages_fetched
        finally:
            _wipe_test_instrument(ebull_test_conn)

    @pytest.mark.integration
    def test_sentinel_row_skips_secondary_walk_silently(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        # Wipe BEFORE baseline (bot review iter 2 WARNING).
        _wipe_test_instrument(ebull_test_conn)
        baseline = walk_files_pages(conn=ebull_test_conn)
        _seed_test_instrument(ebull_test_conn)
        try:
            _insert_sidecar(
                ebull_test_conn,
                cik=_REAL_CIK,
                pages=[("__no_overflow_pages__", None, None)],
            )
            after = walk_files_pages(conn=ebull_test_conn)

            assert after.ciks_with_no_overflow - baseline.ciks_with_no_overflow == 1
            # Sentinel branch never enters the fetch loop.
            assert after.secondary_pages_fetched == baseline.secondary_pages_fetched
            # Sentinel-only is NOT an error.
            assert after.ciks_with_empty_sidecar == baseline.ciks_with_empty_sidecar
            assert after.parse_errors == baseline.parse_errors
        finally:
            _wipe_test_instrument(ebull_test_conn)

    @pytest.mark.integration
    def test_agent_cik_with_empty_sidecar_is_not_an_error(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """An agent CIK in the universe with no sidecar row is EXPECTED
        (populate filters them out). Must NOT increment parse_errors
        or ciks_with_empty_sidecar. Delta-based assertions (per PR #1308
        review bot BLOCKING)."""
        agent_cik = next(iter(KNOWN_FILING_AGENT_CIKS))
        _wipe_test_instrument(ebull_test_conn)

        baseline = walk_files_pages(conn=ebull_test_conn)

        # Seed instrument with the agent CIK.
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(instrument_id), 0) FROM instruments")
            row = cur.fetchone()
            assert row is not None
            iid = int(row[0]) + 1
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable) "
                "VALUES (%s, %s, %s, %s, TRUE)",
                (iid, _REAL_SYMBOL, "Agent CIK Test Co.", "NASDAQ"),
            )
            cur.execute(
                "INSERT INTO external_identifiers "
                "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
                "VALUES (%s, 'sec', 'cik', %s, TRUE)",
                (iid, agent_cik),
            )
        ebull_test_conn.commit()

        try:
            after = walk_files_pages(conn=ebull_test_conn)
            # Delta on the three counters that the agent-CIK branch
            # must NOT touch — robust to any other rows in the test DB.
            assert after.ciks_with_empty_sidecar == baseline.ciks_with_empty_sidecar, (
                "agent CIK empty sidecar must NOT count as error"
            )
            assert after.parse_errors == baseline.parse_errors
            assert after.secondary_pages_fetched == baseline.secondary_pages_fetched
            # And ciks_visited must NOT increment for the agent CIK
            # (Architect IMPORTANT — guard fires before counter).
            assert after.ciks_visited == baseline.ciks_visited
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM external_identifiers "
                    "WHERE provider = 'sec' AND identifier_type = 'cik' AND identifier_value = %s",
                    (agent_cik,),
                )
                cur.execute("DELETE FROM instruments WHERE symbol = %s", (_REAL_SYMBOL,))
            ebull_test_conn.commit()


class TestS14ZeroPrimaryHttpContract:
    """Pinned contract: ``walk_files_pages`` MUST issue ZERO HTTP calls
    to the primary ``data.sec.gov/submissions/CIK<10>.json`` URL when
    the sidecar is populated. Asserted at the httpx transport layer
    via respx so a future code path that bypasses ``SecFilingsProvider``
    would still trip the test."""

    @pytest.mark.integration
    def test_no_primary_fetch_when_sidecar_has_real_pages(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        s14_test_instrument: int,
    ) -> None:
        _insert_sidecar(
            ebull_test_conn,
            cik=_REAL_CIK,
            pages=[(_OVERFLOW_PAGE, "2010-01-15", "2012-06-30")],
        )

        # respx registers wire-level mocks. We register:
        #   * the secondary page URL → 200 with a minimal-shape body
        #     so _normalise_submissions_block returns 0 filings (no
        #     filing_events writes needed for the contract test).
        #   * an explicit assert-not-called on the primary URL.
        primary_url = f"https://data.sec.gov/submissions/CIK{_REAL_CIK}.json"
        secondary_url = f"https://data.sec.gov/submissions/{_OVERFLOW_PAGE}"

        empty_secondary_body: dict[str, Any] = {
            "filings": {
                "accessionNumber": [],
                "filingDate": [],
                "form": [],
                "acceptanceDateTime": [],
                "primaryDocument": [],
            },
        }

        with respx.mock(assert_all_called=False) as mock:
            primary_route = mock.get(primary_url).mock(return_value=Response(200, json={}))
            mock.get(secondary_url).mock(return_value=Response(200, json=empty_secondary_body))

            result = walk_files_pages(conn=ebull_test_conn)

        # The load-bearing assertion — primary URL was NEVER hit.
        assert primary_route.call_count == 0, (
            f"S14 issued {primary_route.call_count} PRIMARY {primary_url} fetch(es); "
            "sidecar exists so the primary refetch contract is violated"
        )
        # secondary_pages_fetched should reflect the real fetch via respx.
        assert result.secondary_pages_fetched >= 1


# ---------------------------------------------------------------------------
# #1341 — chunk-and-drain prefetch shape (walker control-flow tests,
# pure-unit via monkeypatch — no DB).
# ---------------------------------------------------------------------------


class TestS14ChunkedHelpers:
    def test_chunked_yields_correct_slices(self) -> None:
        from app.services.sec_submissions_files_walk import _chunked

        items: list[tuple[int, str, str, str]] = [(i, str(i), str(i), f"page-{i}") for i in range(7)]
        chunks = list(_chunked(items, 3))
        assert [len(c) for c in chunks] == [3, 3, 1]
        # Slices must reassemble to the input in order (no overlap, no drops).
        flattened: list[tuple[int, str, str, str]] = []
        for c in chunks:
            flattened.extend(c)
        assert flattened == items

    def test_chunked_empty_yields_nothing(self) -> None:
        from app.services.sec_submissions_files_walk import _chunked

        assert list(_chunked([], 10)) == []

    def test_chunked_size_zero_raises(self) -> None:
        from app.services.sec_submissions_files_walk import _chunked

        with pytest.raises(ValueError):
            list(_chunked([(1, "a", "a", "p")], 0))


class TestS14LoadAllWatermarks:
    def test_returns_dict_keyed_by_cik_page_tuple(self) -> None:
        from app.services.sec_submissions_files_walk import (
            _SIDECAR_SENTINEL_PAGE_NAME,
            _load_all_watermarks_for_pages,
        )

        # Mock a psycopg connection with a cursor that returns rows.
        class _Cur:
            def __init__(self) -> None:
                self.last_sql: str = ""
                self.last_args: tuple[Any, ...] | None = None

            def __enter__(self) -> _Cur:
                return self

            def __exit__(self, *a: object) -> None:
                return None

            def execute(self, sql: str, args: tuple[Any, ...]) -> None:
                self.last_sql = sql
                self.last_args = args

            def fetchall(self) -> list[tuple[str, str]]:
                return [
                    ("0000320193:CIK0000320193-submissions-001.json", "Mon, 25 May 2026 00:00:00 GMT"),
                    ("0000789019:CIK0000789019-submissions-002.json", "Tue, 26 May 2026 00:00:00 GMT"),
                ]

        class _Conn:
            def __init__(self, cur: _Cur) -> None:
                self._cur = cur

            def cursor(self) -> _Cur:
                return self._cur

        cur = _Cur()
        conn = _Conn(cur)
        targets = [
            (1, "0000320193", "AAPL", ["CIK0000320193-submissions-001.json"]),
            (2, "0000789019", "MSFT", ["CIK0000789019-submissions-002.json"]),
            # Sentinel-only sidecar — must NOT contribute a key.
            (3, "0001326380", "GME", [_SIDECAR_SENTINEL_PAGE_NAME]),
        ]
        out = _load_all_watermarks_for_pages(conn, targets)  # type: ignore[arg-type]
        assert out == {
            ("0000320193", "CIK0000320193-submissions-001.json"): "Mon, 25 May 2026 00:00:00 GMT",
            ("0000789019", "CIK0000789019-submissions-002.json"): "Tue, 26 May 2026 00:00:00 GMT",
        }
        assert cur.last_args is not None
        # Sentinel page NOT in keys argument (defensive: schema CHECK
        # also bars it, but the helper itself must skip it client-side).
        assert _SIDECAR_SENTINEL_PAGE_NAME not in (cur.last_args[1] or [])

    def test_empty_targets_returns_empty_dict(self) -> None:
        from app.services.sec_submissions_files_walk import _load_all_watermarks_for_pages

        class _Conn:
            def cursor(self) -> Any:
                raise AssertionError("must not query when no keys")

        assert _load_all_watermarks_for_pages(_Conn(), []) == {}  # type: ignore[arg-type]


class TestS14ChunkAndDrainShape:
    """Walker control-flow assertions WITHOUT DB. Monkeypatches
    `_list_cik_secondary_pages` (cohort source) +
    `_load_all_watermarks_for_pages` (watermark source) +
    `prefetch_submissions_pages_conditional` (the function under
    bootstrap mode) + `SecFilingsProvider` (so the underlying
    ResilientClient never wakes up). DB is never touched."""

    def _install_walker_stubs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        targets: list[tuple[int, str, str, list[str]]],
        prefetch_seam: list[list[Any]] | None = None,
        bootstrap: bool,
    ) -> tuple[list[list[Any]], list[Any]]:
        """Returns (prefetch_calls, page_calls). page_calls captures
        per-(name, ims) tuples from the sync fallback provider, which
        in bootstrap mode should be EMPTY (everything served from
        prefetch cache)."""
        from app.providers.implementations.sec_edgar import SubmissionsPageResult
        from app.services import sec_submissions_files_walk as mod
        from app.services.bootstrap_state import BootstrapProgressContext

        prefetch_calls: list[list[Any]] = prefetch_seam if prefetch_seam is not None else []
        page_calls: list[tuple[str, str | None]] = []

        def _fake_targets(_conn: Any) -> list[tuple[int, str, str, list[str]]]:
            return targets

        def _fake_watermarks(
            _conn: Any, _targets: list[tuple[int, str, str, list[str]]]
        ) -> dict[tuple[str, str], str | None]:
            return {}

        def _fake_prefetch(tasks: list[Any], **_kwargs: Any) -> dict[str, SubmissionsPageResult | None]:
            prefetch_calls.append(list(tasks))
            # Cache every task as 404 (None) so walker hits the
            # 404 short-circuit in `_process_one_page` WITHOUT
            # touching `conn` (which is a bare object() in these
            # tests). Wrapper still bumps `cache_hits` for None
            # values, so chunk-shape + telemetry assertions still
            # exercise the cache-hit path end-to-end.
            return dict.fromkeys((task.page_name for task in tasks), None)

        class _StubProvider:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            def __enter__(self) -> _StubProvider:
                return self

            def __exit__(self, *a: object) -> None:
                return None

            def fetch_submissions_page_conditional(
                self, name: str, *, if_modified_since: str | None = None
            ) -> SubmissionsPageResult | None:
                page_calls.append((name, if_modified_since))
                return None

        monkeypatch.setattr(mod, "_list_cik_secondary_pages", _fake_targets)
        monkeypatch.setattr(mod, "_load_all_watermarks_for_pages", _fake_watermarks)
        monkeypatch.setattr(mod, "prefetch_submissions_pages_conditional", _fake_prefetch)
        monkeypatch.setattr(mod, "SecFilingsProvider", _StubProvider)
        if bootstrap:
            monkeypatch.setattr(
                mod, "resolve_progress_context", lambda: BootstrapProgressContext(run_id=1, stage_key="x")
            )
            # set_stage_target + set_stage_processed are no-ops in test
            # — they take run_id which isn't a real bootstrap_runs row.
            monkeypatch.setattr(mod, "set_stage_target", lambda **_kwargs: None)
            monkeypatch.setattr(mod, "set_stage_processed", lambda **_kwargs: None)
        else:
            monkeypatch.setattr(mod, "resolve_progress_context", lambda: None)
        return prefetch_calls, page_calls

    def test_bootstrap_mode_chunks_2500_into_three_slices(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # 2500 unique CIKs × 1 page each = 2500 tasks. With
        # DEFAULT_PREFETCH_CHUNK_SIZE=1000 → 3 chunks (1000+1000+500).
        from app.services.sec_submissions_files_walk import walk_files_pages

        targets: list[tuple[int, str, str, list[str]]] = [
            (i, f"{i:010d}", f"SYM{i}", [f"CIK{i:010d}-submissions-001.json"]) for i in range(1, 2501)
        ]
        prefetch_calls, page_calls = self._install_walker_stubs(monkeypatch, targets=targets, bootstrap=True)

        result = walk_files_pages(conn=object())  # type: ignore[arg-type]
        assert [len(c) for c in prefetch_calls] == [1000, 1000, 500]
        # No overlap between chunks — slices are disjoint.
        seen_names: set[str] = set()
        for chunk in prefetch_calls:
            chunk_names = {t.page_name for t in chunk}
            assert seen_names.isdisjoint(chunk_names)
            seen_names |= chunk_names
        assert len(seen_names) == 2500
        # Cache covered every task → sync fallback NOT exercised.
        assert page_calls == []
        # Telemetry summed across chunks.
        assert result.prefetch_pages_seeded == 2500
        assert result.loop_pages_from_prefetch == 2500
        assert result.loop_pages_from_sync_fallback == 0
        assert result.prefetch_window_seconds_total is not None
        assert result.prefetch_window_seconds_total >= 0

    def test_steady_state_skips_prefetch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # No bootstrap context → walker uses sync provider path; the
        # prefetch function is NEVER called.
        from app.services.sec_submissions_files_walk import walk_files_pages

        targets: list[tuple[int, str, str, list[str]]] = [
            (1, "0000000001", "S1", ["CIK0000000001-submissions-001.json"]),
            (2, "0000000002", "S2", ["CIK0000000002-submissions-001.json"]),
        ]
        prefetch_calls, page_calls = self._install_walker_stubs(monkeypatch, targets=targets, bootstrap=False)

        result = walk_files_pages(conn=object())  # type: ignore[arg-type]
        assert prefetch_calls == []
        # Sync provider invoked once per page (cache miss is the only
        # path in steady-state mode).
        assert [name for (name, _) in page_calls] == [
            "CIK0000000001-submissions-001.json",
            "CIK0000000002-submissions-001.json",
        ]
        # Steady-state telemetry: prefetch_window_seconds_total is None.
        assert result.prefetch_window_seconds_total is None
        assert result.prefetch_pages_seeded == 0
        assert result.loop_pages_from_prefetch == 0
        assert result.loop_pages_from_sync_fallback == 0  # wrapper not used

    def test_short_circuit_targets_do_not_appear_in_fetch_tasks(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Agent CIK / empty sidecar / sentinel-only — none should be
        # in the prefetch task list.
        from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS
        from app.services.sec_submissions_files_walk import (
            _SIDECAR_SENTINEL_PAGE_NAME,
            walk_files_pages,
        )

        agent_cik = next(iter(KNOWN_FILING_AGENT_CIKS))
        targets: list[tuple[int, str, str, list[str]]] = [
            (1, agent_cik, "AGENT", []),  # agent CIK
            (2, "0000000099", "EMPTY", []),  # empty sidecar
            (3, "0000000100", "SENT", [_SIDECAR_SENTINEL_PAGE_NAME]),  # sentinel-only
            (4, "0000000200", "REAL", ["CIK0000000200-submissions-001.json"]),
        ]
        prefetch_calls, _ = self._install_walker_stubs(monkeypatch, targets=targets, bootstrap=True)

        result = walk_files_pages(conn=object())  # type: ignore[arg-type]
        # Only the REAL CIK's page is in the prefetch.
        assert len(prefetch_calls) == 1
        assert [t.page_name for t in prefetch_calls[0]] == ["CIK0000000200-submissions-001.json"]
        # Counters: agent CIK not visited; empty + sentinel + real all visited.
        assert result.ciks_visited == 3
        assert result.ciks_with_empty_sidecar == 1
        assert result.ciks_with_no_overflow == 1
        # Empty sidecar bumps parse_errors per existing semantics.
        assert result.parse_errors == 1
