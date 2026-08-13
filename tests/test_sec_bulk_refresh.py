"""Tests for the SEC bulk-archive daily refresh adapter (#1233 PR-8)."""

from __future__ import annotations

import io
import zipfile
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

import app.services.sec_bulk_refresh as refresh_mod
from app.services.sec_bulk_download import BulkArchive
from app.services.sec_bulk_refresh import (
    _COMPANYFACTS_NAME,
    _SUBMISSIONS_NAME,
    RefreshResult,
    _archive_for_name,
    _atomic_write_text,
    _bootstrap_running,
    _companyfacts_archive_names,
    _compute_sha256,
    _etag_sidecar_path,
    _quarterly_dataset_archive_names,
    _read_local_etag,
    _refresh_one_async,
    _sha256_sidecar_path,
    _submissions_archive_names,
    refresh_bulk_archive_if_stale,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _zip_bytes(payload: bytes = b"{}") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("CIK0000320193.json", payload)
    return buf.getvalue()


def _make_handler(
    *,
    archive_url: str,
    archive_body: bytes,
    etag: str,
    head_status: int = 200,
    get_status: int = 200,
    include_etag: bool = True,
    include_content_length: bool = True,
    content_type: str = "application/zip",
    get_etag: str | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    """Return a MockTransport handler with controllable HEAD + GET behaviour.

    ``get_etag`` (when set) overrides the GET response's ETag header
    independent of the HEAD ETag — exercises the CDN-race detection
    path. Defaults to ``etag`` so HEAD/GET agree by default.
    """
    effective_get_etag = etag if get_etag is None else get_etag

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) != archive_url:
            return httpx.Response(404)
        if request.method == "HEAD":
            if head_status != 200:
                return httpx.Response(head_status)
            headers: dict[str, str] = {"Content-Type": content_type}
            if include_content_length:
                headers["Content-Length"] = str(len(archive_body))
            if include_etag:
                headers["ETag"] = etag
            return httpx.Response(200, headers=headers)
        if request.method == "GET":
            if get_status != 200:
                return httpx.Response(get_status)
            return httpx.Response(
                200,
                content=archive_body,
                headers={
                    "Content-Length": str(len(archive_body)),
                    "ETag": effective_get_etag,
                },
            )
        return httpx.Response(405)

    return handler


@asynccontextmanager
async def _patched_make_client(
    transport: httpx.MockTransport,
) -> AsyncIterator[None]:
    """Yield an httpx.AsyncClient over ``transport`` and patch the module-
    level ``_make_client`` factory in ``sec_bulk_refresh`` to use it."""

    @asynccontextmanager
    async def _factory(user_agent: str) -> AsyncIterator[httpx.AsyncClient]:
        async with httpx.AsyncClient(
            transport=transport,
            headers={"User-Agent": user_agent, "Accept": "application/zip,*/*"},
        ) as client:
            yield client

    orig = refresh_mod._make_client
    refresh_mod._make_client = _factory  # type: ignore[assignment]
    try:
        yield
    finally:
        refresh_mod._make_client = orig  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Archive lookup
# ---------------------------------------------------------------------------


class TestArchiveLookup:
    def test_submissions_lookup_returns_known_url(self) -> None:
        archive = _archive_for_name(_SUBMISSIONS_NAME)
        assert archive is not None
        assert archive.name == _SUBMISSIONS_NAME
        assert archive.url.endswith("/Archives/edgar/daily-index/bulkdata/submissions.zip")

    def test_companyfacts_lookup_returns_known_url(self) -> None:
        archive = _archive_for_name(_COMPANYFACTS_NAME)
        assert archive is not None
        assert archive.name == _COMPANYFACTS_NAME
        assert archive.url.endswith("/Archives/edgar/daily-index/xbrl/companyfacts.zip")

    def test_quarterly_archive_lookup_uses_inventory(self) -> None:
        # Pick a known-shape quarterly name from the inventory builder.
        names = _quarterly_dataset_archive_names()
        assert names, "quarterly inventory must be non-empty"
        first = names[0]
        archive = _archive_for_name(first)
        assert archive is not None
        assert archive.name == first

    def test_unknown_archive_returns_none(self) -> None:
        assert _archive_for_name("not_a_real_archive.zip") is None


# ---------------------------------------------------------------------------
# Sidecar helpers
# ---------------------------------------------------------------------------


class TestSidecars:
    def test_atomic_write_replaces_existing(self, tmp_path: Path) -> None:
        target = tmp_path / "x.etag"
        _atomic_write_text(target, "first")
        assert target.read_text(encoding="utf-8") == "first"
        _atomic_write_text(target, "second")
        assert target.read_text(encoding="utf-8") == "second"
        # No leftover tmpfile.
        assert not (tmp_path / "x.etag.tmp").exists()

    def test_read_local_etag_returns_none_when_missing(self, tmp_path: Path) -> None:
        assert _read_local_etag(tmp_path / "submissions.zip") is None

    def test_read_local_etag_strips_whitespace(self, tmp_path: Path) -> None:
        archive_path = tmp_path / "submissions.zip"
        _atomic_write_text(_etag_sidecar_path(archive_path), '  "abc-123"  \n')
        assert _read_local_etag(archive_path) == '"abc-123"'

    def test_compute_sha256_streams_large_file(self, tmp_path: Path) -> None:
        # 5 MB file exercises the chunk loop (chunk_size=1MB).
        path = tmp_path / "big.bin"
        payload = b"x" * (5 * 1024 * 1024)
        path.write_bytes(payload)
        import hashlib

        expected = hashlib.sha256(payload).hexdigest()
        assert _compute_sha256(path) == expected


# ---------------------------------------------------------------------------
# Async refresh — unchanged-SEC path
# ---------------------------------------------------------------------------


class TestRefreshOneAsyncUnchanged:
    @pytest.mark.asyncio
    async def test_etag_match_returns_no_op(self, tmp_path: Path) -> None:
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        body = _zip_bytes()
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        etag = '"504b124e9474334e889e9e525db95c14-184"'

        # Pre-seed a matching ETag sidecar + a real (round-trippable)
        # zip body at the canonical path.
        archive_path = tmp_path / _SUBMISSIONS_NAME
        archive_path.write_bytes(body)
        _atomic_write_text(_etag_sidecar_path(archive_path), etag)

        handler = _make_handler(archive_url=url, archive_body=body, etag=etag)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result == RefreshResult(
            archive_name=_SUBMISSIONS_NAME,
            etag_changed=False,
            bytes_downloaded=0,
            skipped_reason=None,
        )
        # Sidecar untouched, archive untouched.
        assert _read_local_etag(archive_path) == etag
        assert archive_path.read_bytes() == body
        # No SHA-256 sidecar was created (we only create on download).
        assert not _sha256_sidecar_path(archive_path).exists()


# ---------------------------------------------------------------------------
# Async refresh — changed-SEC path
# ---------------------------------------------------------------------------


class TestRefreshOneAsyncChanged:
    @pytest.mark.asyncio
    async def test_etag_change_triggers_download_and_writes_sidecars(self, tmp_path: Path) -> None:
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        new_body = _zip_bytes(payload=b'{"updated": true}')
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        old_etag = '"old-etag-aaaa"'
        new_etag = '"new-etag-bbbb"'

        # Pre-seed an OLD archive + sidecar.
        archive_path = tmp_path / _SUBMISSIONS_NAME
        old_body = _zip_bytes(payload=b'{"stale": true}')
        archive_path.write_bytes(old_body)
        _atomic_write_text(_etag_sidecar_path(archive_path), old_etag)

        handler = _make_handler(archive_url=url, archive_body=new_body, etag=new_etag)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.archive_name == _SUBMISSIONS_NAME
        assert result.etag_changed is True
        assert result.bytes_downloaded == len(new_body)
        assert result.skipped_reason is None
        # Archive now holds the new body.
        assert archive_path.read_bytes() == new_body
        # ETag sidecar updated to the new value (verbatim, with quotes).
        assert _read_local_etag(archive_path) == new_etag
        # SHA-256 sidecar landed.
        import hashlib

        expected_sha = hashlib.sha256(new_body).hexdigest()
        assert _sha256_sidecar_path(archive_path).read_text().strip() == expected_sha
        # No leftover partial.
        assert not (tmp_path / f"{_SUBMISSIONS_NAME}.refresh.partial").exists()

    @pytest.mark.asyncio
    async def test_missing_local_archive_triggers_initial_download(self, tmp_path: Path) -> None:
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        body = _zip_bytes()
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        etag = '"first-pull"'

        # No pre-existing file or sidecar.
        archive_path = tmp_path / _SUBMISSIONS_NAME
        assert not archive_path.exists()
        assert not _etag_sidecar_path(archive_path).exists()

        handler = _make_handler(archive_url=url, archive_body=body, etag=etag)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.etag_changed is True
        assert result.bytes_downloaded == len(body)
        assert archive_path.read_bytes() == body
        assert _read_local_etag(archive_path) == etag


# ---------------------------------------------------------------------------
# Async refresh — fail-closed paths
# ---------------------------------------------------------------------------


class TestRefreshOneAsyncFailClosed:
    @pytest.mark.asyncio
    async def test_head_500_leaves_local_archive_alone(self, tmp_path: Path) -> None:
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)

        # Pre-existing archive + ETag should be untouched by a SEC 5xx.
        archive_path = tmp_path / _SUBMISSIONS_NAME
        body = _zip_bytes()
        archive_path.write_bytes(body)
        _atomic_write_text(_etag_sidecar_path(archive_path), '"old-etag"')

        handler = _make_handler(archive_url=url, archive_body=body, etag='"unused"', head_status=503)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.etag_changed is False
        assert result.bytes_downloaded == 0
        assert result.skipped_reason is not None and "503" in result.skipped_reason
        # File + sidecar preserved.
        assert archive_path.read_bytes() == body
        assert _read_local_etag(archive_path) == '"old-etag"'

    @pytest.mark.asyncio
    async def test_head_missing_etag_skips(self, tmp_path: Path) -> None:
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        body = _zip_bytes()
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)

        # No ETag header → skip with structured reason.
        handler = _make_handler(archive_url=url, archive_body=body, etag="unused", include_etag=False)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.etag_changed is False
        assert result.skipped_reason == "head_missing_etag"

    @pytest.mark.asyncio
    async def test_get_500_after_head_ok_preserves_old_archive(self, tmp_path: Path) -> None:
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        new_body = _zip_bytes(payload=b'{"new": true}')
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)

        # Old archive present; new ETag advertised but GET fails.
        archive_path = tmp_path / _SUBMISSIONS_NAME
        old_body = _zip_bytes(payload=b'{"old": true}')
        archive_path.write_bytes(old_body)
        _atomic_write_text(_etag_sidecar_path(archive_path), '"old-etag"')

        handler = _make_handler(archive_url=url, archive_body=new_body, etag='"new-etag"', get_status=502)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.etag_changed is False
        assert result.skipped_reason == "get_status_502"
        # Critically: the old archive + old sidecar survive.
        assert archive_path.read_bytes() == old_body
        assert _read_local_etag(archive_path) == '"old-etag"'
        # No partial leftover.
        assert not (tmp_path / f"{_SUBMISSIONS_NAME}.refresh.partial").exists()

    @pytest.mark.asyncio
    async def test_head_bad_content_type_skips(self, tmp_path: Path) -> None:
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)

        # SEC served an HTML error page — pre-flight rejects.
        handler = _make_handler(
            archive_url=url,
            archive_body=b"<html>error</html>",
            etag='"x"',
            content_type="text/html",
        )
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.etag_changed is False
        assert result.skipped_reason is not None
        assert result.skipped_reason.startswith("head_bad_content_type")


# ---------------------------------------------------------------------------
# Seed-on-first-encounter (no sidecar yet, archive matches HEAD)
# ---------------------------------------------------------------------------


class TestSeedOnFirstEncounter:
    @pytest.mark.asyncio
    async def test_existing_archive_without_sidecar_is_seeded_without_download(self, tmp_path: Path) -> None:
        """Bootstrap downloader writes the `.zip` but (today) NOT the
        `.zip.etag` sidecar. The FIRST refresh fire must recognise the
        existing valid archive matches HEAD by size+ZIP integrity and
        adopt the live ETag as the sidecar — transferring ZERO bytes.
        Without this, every install would re-download the multi-GB
        archive on its first refresh fire.
        """
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        body = _zip_bytes()
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        etag = '"seed-test-etag"'

        # Archive present, NO sidecar. Bootstrap-downloader state.
        archive_path = tmp_path / _SUBMISSIONS_NAME
        archive_path.write_bytes(body)
        assert not _etag_sidecar_path(archive_path).exists()
        assert not _sha256_sidecar_path(archive_path).exists()

        handler = _make_handler(archive_url=url, archive_body=body, etag=etag)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        # ZERO transfer — we adopted the HEAD ETag.
        assert result.etag_changed is False
        assert result.bytes_downloaded == 0
        assert result.skipped_reason is None
        # Sidecars now exist.
        assert _read_local_etag(archive_path) == etag
        import hashlib

        assert _sha256_sidecar_path(archive_path).read_text().strip() == hashlib.sha256(body).hexdigest()
        # Archive untouched.
        assert archive_path.read_bytes() == body

    @pytest.mark.asyncio
    async def test_existing_archive_with_size_mismatch_falls_through_to_download(self, tmp_path: Path) -> None:
        """If the local file exists but its size doesn't match HEAD's
        Content-Length, the seed-on-first-encounter path must NOT
        adopt — fall through to the genuine re-download. Otherwise
        a stale/corrupted local archive would be canonicalised under
        the live ETag.
        """
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        new_body = _zip_bytes(payload=b'{"new": true}')
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        etag = '"seed-skip-test"'

        # Local archive smaller than HEAD says (simulating partial / wrong version).
        archive_path = tmp_path / _SUBMISSIONS_NAME
        archive_path.write_bytes(b"truncated")
        assert archive_path.stat().st_size != len(new_body)

        handler = _make_handler(archive_url=url, archive_body=new_body, etag=etag)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        # Re-download happened — new body landed.
        assert result.etag_changed is True
        assert result.bytes_downloaded == len(new_body)
        assert archive_path.read_bytes() == new_body


# ---------------------------------------------------------------------------
# CDN race: GET ETag mismatch with HEAD
# ---------------------------------------------------------------------------


class TestCdnRaceGetEtagMismatch:
    @pytest.mark.asyncio
    async def test_get_etag_differs_from_head_keeps_old_archive(self, tmp_path: Path) -> None:
        """A CDN race can serve HEAD against version A and GET against
        version B. If we wrote version B bytes under version A's ETag,
        future HEAD-match fast-paths would wrongly skip real updates.
        Detect the mismatch, discard the partial, retain the old
        archive + sidecar.
        """
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        body = _zip_bytes(payload=b'{"served": true}')
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        head_etag = '"head-version-A"'
        get_response_etag = '"get-version-B"'

        archive_path = tmp_path / _SUBMISSIONS_NAME
        old_body = _zip_bytes(payload=b'{"old": true}')
        archive_path.write_bytes(old_body)
        _atomic_write_text(_etag_sidecar_path(archive_path), '"old-sidecar"')

        handler = _make_handler(
            archive_url=url,
            archive_body=body,
            etag=head_etag,
            get_etag=get_response_etag,
        )
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.etag_changed is False
        assert result.bytes_downloaded == 0
        assert result.skipped_reason == "get_etag_mismatch_with_head"
        # OLD archive + sidecar survive — critical invariant.
        assert archive_path.read_bytes() == old_body
        assert _read_local_etag(archive_path) == '"old-sidecar"'
        # No partial leftover.
        assert not (tmp_path / f"{_SUBMISSIONS_NAME}.refresh.partial").exists()

    @pytest.mark.asyncio
    async def test_get_etag_matches_head_records_get_etag(self, tmp_path: Path) -> None:
        """When HEAD + GET ETag agree, the sidecar records the GET
        value (preferred — describes exactly the bytes we kept).
        """
        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        body = _zip_bytes()
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        etag = '"agreement"'

        handler = _make_handler(archive_url=url, archive_body=body, etag=etag)
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )

        assert result.etag_changed is True
        archive_path = tmp_path / _SUBMISSIONS_NAME
        assert _read_local_etag(archive_path) == etag


# ---------------------------------------------------------------------------
# Bootstrap fence
# ---------------------------------------------------------------------------


class TestBootstrapFence:
    def test_refresh_skips_when_bootstrap_running(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``refresh_bulk_archive_if_stale`` must NOT contact SEC while
        ``bootstrap_state.status='running'`` — PR-5b's reuse path would
        otherwise race the daily refresh.
        """
        # Patch the data dir to the test tmp_path.
        monkeypatch.setattr(refresh_mod, "_resolve_target_dir", lambda: tmp_path)

        # Force the bootstrap fence to report "running".
        class _FakeConn:
            def execute(self, *_args: Any, **_kwargs: Any) -> Any:
                class _Row:
                    @staticmethod
                    def fetchone() -> tuple[str]:
                        return ("running",)

                return _Row()

            def __enter__(self) -> _FakeConn:
                return self

            def __exit__(self, *_exc: Any) -> bool:
                return False

        # Replace psycopg.connect (only in the module under test) with a
        # context manager yielding the fake conn. Using monkeypatch.setattr
        # against the imported ``psycopg`` symbol limits scope.
        import psycopg

        class _FakeContext:
            def __enter__(self) -> _FakeConn:
                return _FakeConn()

            def __exit__(self, *_exc: Any) -> bool:
                return False

        monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: _FakeContext())

        # Sentinel: if the fence is honored, _refresh_one_async must NOT
        # be invoked. Replace it with one that throws.
        def _should_not_be_called(**_kwargs: Any) -> RefreshResult:
            raise AssertionError("_refresh_one_async called despite bootstrap_running fence")

        monkeypatch.setattr(refresh_mod, "_refresh_one_async", _should_not_be_called)

        result = refresh_bulk_archive_if_stale(_SUBMISSIONS_NAME)

        assert result.archive_name == _SUBMISSIONS_NAME
        assert result.etag_changed is False
        assert result.bytes_downloaded == 0
        assert result.skipped_reason == "bootstrap_running"

    def test_bootstrap_running_returns_true_when_status_is_running(self) -> None:
        """Direct test of the predicate against a fake conn — ensures
        the SELECT result is parsed correctly."""

        class _Row:
            @staticmethod
            def fetchone() -> tuple[str]:
                return ("running",)

        class _Conn:
            def execute(self, *_args: Any, **_kwargs: Any) -> _Row:
                return _Row()

        assert _bootstrap_running(_Conn()) is True  # type: ignore[arg-type]

    def test_bootstrap_running_returns_false_when_status_is_complete(self) -> None:
        class _Row:
            @staticmethod
            def fetchone() -> tuple[str]:
                return ("complete",)

        class _Conn:
            def execute(self, *_args: Any, **_kwargs: Any) -> _Row:
                return _Row()

        assert _bootstrap_running(_Conn()) is False  # type: ignore[arg-type]

    def test_bootstrap_running_fails_closed_on_missing_row(self) -> None:
        """If the singleton row is missing, the fence must default to
        ``True`` (refuse to race) — defense in depth against migration
        corruption.
        """

        class _Row:
            @staticmethod
            def fetchone() -> None:
                return None

        class _Conn:
            def execute(self, *_args: Any, **_kwargs: Any) -> _Row:
                return _Row()

        assert _bootstrap_running(_Conn()) is True  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Rate-limit acquisition
# ---------------------------------------------------------------------------


class TestRateLimitAcquisition:
    @pytest.mark.asyncio
    async def test_refresh_acquires_from_gate(self, tmp_path: Path) -> None:
        """The refresh must acquire from the shared SEC rate gate (#1484).

        After the async/bulk paths were migrated to route through the gate
        (rather than the old shared process clock), we verify by installing a
        SpyGate via the holder and asserting acquire_async was called at least
        once during the refresh fire.
        """
        from app.providers import sec_rate_gate_holder as holder
        from app.providers.rate_gate import InProcessFloorGate

        class SpyGate(InProcessFloorGate):
            def __init__(self) -> None:
                super().__init__(floor=0.0)
                self.async_calls = 0

            async def acquire_async(self) -> None:
                self.async_calls += 1
                await super().acquire_async()

        holder._reset_sec_rate_gate_for_tests()
        spy = SpyGate()
        holder.set_sec_rate_gate(spy)
        try:
            url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
            body = _zip_bytes()
            archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
            etag = '"gate-spy-test"'

            # Seed local sidecar so the HEAD path is a no-op (single acquire).
            archive_path = tmp_path / _SUBMISSIONS_NAME
            archive_path.write_bytes(body)
            _atomic_write_text(_etag_sidecar_path(archive_path), etag)

            handler = _make_handler(archive_url=url, archive_body=body, etag=etag)
            async with _patched_make_client(httpx.MockTransport(handler)):
                result = await _refresh_one_async(
                    archive=archive,
                    target_dir=tmp_path,
                    user_agent="ebull/test (admin@example.com)",
                )

            assert result.skipped_reason is None
            assert spy.async_calls > 0, "bulk refresh did not acquire from the SEC gate"
        finally:
            holder._reset_sec_rate_gate_for_tests()


# ---------------------------------------------------------------------------
# Operator force-override is bootstrap-only
# ---------------------------------------------------------------------------


class TestOperatorForceOverrideIsBootstrapOnly:
    def test_force_redownload_env_does_not_change_refresh_behaviour(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``BOOTSTRAP_FORCE_REDOWNLOAD=1`` is a bootstrap-time operator
        override (PR-5b's territory). The daily refresh must ignore it —
        operator forces the bootstrap downloader, not the refresh adapter.
        """
        monkeypatch.setenv("BOOTSTRAP_FORCE_REDOWNLOAD", "1")

        # Walk through the source: there must be NO read of the env var
        # in the refresh module. We assert by import-time string presence.
        import inspect

        src = inspect.getsource(refresh_mod)
        assert "BOOTSTRAP_FORCE_REDOWNLOAD" not in src, (
            "sec_bulk_refresh must not honour the BOOTSTRAP_FORCE_REDOWNLOAD "
            "operator override; that flag belongs to the bootstrap downloader "
            "(PR-5b). Daily refresh decisions are ETag-driven only."
        )


# ---------------------------------------------------------------------------
# Scheduler + invoker registration
# ---------------------------------------------------------------------------


class TestSchedulerRegistration:
    def test_all_three_jobs_are_in_scheduled_jobs(self) -> None:
        from app.workers.scheduler import (
            JOB_SEC_COMPANYFACTS_BULK_REFRESH,
            JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH,
            JOB_SEC_SUBMISSIONS_BULK_REFRESH,
            SCHEDULED_JOBS,
        )

        names = {job.name for job in SCHEDULED_JOBS}
        assert JOB_SEC_SUBMISSIONS_BULK_REFRESH in names
        assert JOB_SEC_COMPANYFACTS_BULK_REFRESH in names
        assert JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH in names

    def test_all_three_jobs_are_in_invokers(self) -> None:
        from app.jobs.runtime import _INVOKERS
        from app.workers.scheduler import (
            JOB_SEC_COMPANYFACTS_BULK_REFRESH,
            JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH,
            JOB_SEC_SUBMISSIONS_BULK_REFRESH,
        )

        assert JOB_SEC_SUBMISSIONS_BULK_REFRESH in _INVOKERS
        assert JOB_SEC_COMPANYFACTS_BULK_REFRESH in _INVOKERS
        assert JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH in _INVOKERS

    def test_all_three_jobs_resolve_to_sec_bulk_download_source(self) -> None:
        """The lane MUST be ``sec_bulk_download`` — disjoint from
        ``sec_rate`` by design so daily refresh transfers don't steal
        budget from per-CIK SEC fetches.
        """
        from app.jobs.sources import source_for
        from app.workers.scheduler import (
            JOB_SEC_COMPANYFACTS_BULK_REFRESH,
            JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH,
            JOB_SEC_SUBMISSIONS_BULK_REFRESH,
        )

        for name in (
            JOB_SEC_SUBMISSIONS_BULK_REFRESH,
            JOB_SEC_COMPANYFACTS_BULK_REFRESH,
            JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH,
        ):
            assert source_for(name) == "sec_bulk_download"

    def test_cadences_match_spec(self) -> None:
        """The spec calls for cron schedules:

        * ``submissions``    — ``0 8 * * *``    (daily 08:00 UTC)
        * ``companyfacts``   — ``30 8 * * *``   (daily 08:30 UTC)
        * ``quarterly``      — ``0 6 5 * *``    (monthly day-5 06:00 UTC)
        """
        from app.workers.scheduler import (
            JOB_SEC_COMPANYFACTS_BULK_REFRESH,
            JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH,
            JOB_SEC_SUBMISSIONS_BULK_REFRESH,
            SCHEDULED_JOBS,
        )

        by_name = {job.name: job for job in SCHEDULED_JOBS}

        sub = by_name[JOB_SEC_SUBMISSIONS_BULK_REFRESH].cadence
        assert sub.kind == "daily"
        assert sub.hour == 8 and sub.minute == 0

        cf = by_name[JOB_SEC_COMPANYFACTS_BULK_REFRESH].cadence
        assert cf.kind == "daily"
        assert cf.hour == 8 and cf.minute == 30

        qtr = by_name[JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH].cadence
        assert qtr.kind == "monthly"
        assert qtr.day == 5
        assert qtr.hour == 6 and qtr.minute == 0

    def test_bulk_refresh_outcome_populates_tracker_note_on_skip(self) -> None:
        """``_record_bulk_refresh_outcome`` must forward the skip digest
        to ``tracker.note`` so ``_tracked_job`` writes it into
        ``job_runs.error_msg`` on the SUCCESS path. Without this, a
        bootstrap-fence / SEC-5xx / missing-ETag skip records a plain
        success row and the operator has to read process logs to learn
        why nothing happened.
        """
        from app.workers.scheduler import _JobTracker, _record_bulk_refresh_outcome

        tracker = _JobTracker("test_job")
        results = [
            RefreshResult(
                archive_name="submissions.zip",
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="bootstrap_running",
            ),
        ]
        _record_bulk_refresh_outcome(tracker, results, job_name="test_job")
        assert tracker.note == "submissions.zip:bootstrap_running"
        assert tracker.row_count == 0

    def test_bulk_refresh_outcome_no_note_on_clean_no_op(self) -> None:
        """When every archive's outcome is fresh (no transfer, no skip)
        the tracker.note stays ``None`` — a clean SUCCESS row with NULL
        error_msg.
        """
        from app.workers.scheduler import _JobTracker, _record_bulk_refresh_outcome

        tracker = _JobTracker("test_job")
        results = [
            RefreshResult(
                archive_name="submissions.zip",
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=None,
            ),
        ]
        _record_bulk_refresh_outcome(tracker, results, job_name="test_job")
        assert tracker.note is None
        assert tracker.row_count == 0

    def test_archive_set_helpers(self) -> None:
        """Confirm the archive-set helpers return what the invokers
        will iterate over."""
        assert _submissions_archive_names() == (_SUBMISSIONS_NAME,)
        assert _companyfacts_archive_names() == (_COMPANYFACTS_NAME,)
        qtr = _quarterly_dataset_archive_names()
        assert qtr  # non-empty
        assert _SUBMISSIONS_NAME not in qtr
        assert _COMPANYFACTS_NAME not in qtr
        # All entries should be real archives.
        for name in qtr:
            assert _archive_for_name(name) is not None


# ---------------------------------------------------------------------------
# SEC 429 counter (#1545) — async refresh paths increment sec_throttle_429_total
# ---------------------------------------------------------------------------


class TestSec429Counter:
    """Delta-based assertions per the prevention log — the counter is
    process-global, never assert exact totals."""

    @pytest.mark.asyncio
    async def test_head_429_increments_counter(self, tmp_path: Path) -> None:
        from app.providers.sec_throttle_metrics import sec_throttle_429_total

        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        handler = _make_handler(archive_url=url, archive_body=_zip_bytes(), etag='"unused"', head_status=429)

        before = sec_throttle_429_total()
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )
        assert result.skipped_reason == "head_status_429"
        assert sec_throttle_429_total() - before == 1

    @pytest.mark.asyncio
    async def test_get_429_increments_counter(self, tmp_path: Path) -> None:
        from app.providers.sec_throttle_metrics import sec_throttle_429_total

        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)

        # Old archive present; new ETag advertised so the GET fires; GET 429s.
        archive_path = tmp_path / _SUBMISSIONS_NAME
        archive_path.write_bytes(_zip_bytes(payload=b'{"old": true}'))
        _atomic_write_text(_etag_sidecar_path(archive_path), '"old-etag"')

        handler = _make_handler(archive_url=url, archive_body=_zip_bytes(), etag='"new-etag"', get_status=429)

        before = sec_throttle_429_total()
        async with _patched_make_client(httpx.MockTransport(handler)):
            result = await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )
        assert result.skipped_reason == "get_status_429"
        assert sec_throttle_429_total() - before == 1

    @pytest.mark.asyncio
    async def test_head_503_does_not_increment(self, tmp_path: Path) -> None:
        from app.providers.sec_throttle_metrics import sec_throttle_429_total

        url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        archive = BulkArchive(name=_SUBMISSIONS_NAME, url=url)
        handler = _make_handler(archive_url=url, archive_body=_zip_bytes(), etag='"unused"', head_status=503)

        before = sec_throttle_429_total()
        async with _patched_make_client(httpx.MockTransport(handler)):
            await _refresh_one_async(
                archive=archive,
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
            )
        assert sec_throttle_429_total() - before == 0
