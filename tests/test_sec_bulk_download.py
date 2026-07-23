"""Tests for the SEC bulk-archive download service (#1021)."""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

import httpx
import pytest

from app.services.sec_bulk_download import (
    PROBE_BYTES,
    ArchiveDownloadResult,
    BulkArchive,
    BulkDownloadResult,
    _download_one,
    _fatal_download_failures,
    _purge_archive_artifacts,
    _zip_round_trip,
    build_bulk_archive_inventory,
    check_disk_space,
    download_bulk_archives,
    last_n_13f_periods,
    last_n_quarters,
    measure_bandwidth_mbps,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_zip_bytes(*, filenames: tuple[str, ...] = ("CIK0000320193.json",)) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name in filenames:
            zf.writestr(name, b"{}")
    return buf.getvalue()


def _make_handler(
    archive_url: str,
    archive_body: bytes,
) -> Callable[[httpx.Request], httpx.Response]:
    """Return a MockTransport handler that serves ``archive_body`` at ``archive_url``."""

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) != archive_url:
            return httpx.Response(404)
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={
                    "Content-Length": str(len(archive_body)),
                    # Mock SEC's real response so the new
                    # Content-Type integrity check passes (#1059).
                    "Content-Type": "application/zip",
                },
            )
        if request.method == "GET":
            range_header = request.headers.get("Range")
            if range_header:
                # ``bytes=N-`` or ``bytes=N-M``
                spec = range_header.removeprefix("bytes=")
                if "-" in spec:
                    start_str, end_str = spec.split("-", 1)
                    start = int(start_str) if start_str else 0
                    end = int(end_str) if end_str else len(archive_body) - 1
                    chunk = archive_body[start : end + 1]
                    return httpx.Response(
                        206,
                        content=chunk,
                        headers={
                            "Content-Length": str(len(chunk)),
                            "Content-Range": f"bytes {start}-{start + len(chunk) - 1}/{len(archive_body)}",
                        },
                    )
            return httpx.Response(
                200,
                content=archive_body,
                headers={"Content-Length": str(len(archive_body))},
            )
        return httpx.Response(405)

    return handler


# ---------------------------------------------------------------------------
# URL builder tests
# ---------------------------------------------------------------------------


class TestQuarterLabels:
    def test_last_n_quarters_excludes_in_progress(self) -> None:
        # 2026-05-08 is in 2026Q2; the most-recent COMPLETED quarter is
        # 2026Q1, then 2025Q4, etc.
        labels = last_n_quarters(4, today=date(2026, 5, 8))
        assert labels == ["2026q1", "2025q4", "2025q3", "2025q2"]

    def test_last_n_13f_periods_uses_sec_rolling_3_month_format(self) -> None:
        # Verified URLs HEAD'd against SEC.gov on 2026-05-08:
        # Latest published 13F dataset is 01dec2025-28feb2026, NOT
        # 01jan-31mar (calendar Q1). SEC's rolling windows start on
        # the 1st of Mar, Jun, Sep, Dec.
        labels = last_n_13f_periods(4, today=date(2026, 5, 8))
        assert labels == [
            "01dec2025-28feb2026",
            "01sep2025-30nov2025",
            "01jun2025-31aug2025",
            "01mar2025-31may2025",
        ]


class TestInventory:
    def test_inventory_includes_all_archive_families(self) -> None:
        inventory = build_bulk_archive_inventory(today=date(2026, 5, 8))
        names = {a.name for a in inventory}
        assert "submissions.zip" in names
        assert "companyfacts.zip" in names
        assert any(n.startswith("form13f_") for n in names)
        assert any(n.startswith("insider_") for n in names)
        assert any(n.startswith("nport_") for n in names)

    def test_newest_13f_window_is_optional_rest_required(self) -> None:
        # #1423 — the newest 13F rolling window is the just-closed quarter;
        # SEC publishes it weeks after close, so the day after a boundary it
        # 404s. Mark ONLY that one optional so its absence doesn't fatal the
        # stage. today=2026-06-01: newest completed window is Mar-May 2026.
        inventory = build_bulk_archive_inventory(today=date(2026, 6, 1))
        form13f = [a for a in inventory if a.name.startswith("form13f_")]
        # Newest-first ordering: index 0 is the most recent window.
        assert form13f[0].name == "form13f_01mar2026-31may2026.zip"
        assert form13f[0].optional is True, "newest 13F window must be optional"
        # Every other 13F window and every non-13F archive stays required.
        for a in form13f[1:]:
            assert a.optional is False, f"{a.name} must stay required"
        # FSDS mirrors the 13F posture (#788): newest quarter published weeks late
        # → optional; the rest required.
        fsds = [a for a in inventory if a.name.startswith("fsds_")]
        assert fsds[0].optional is True, "newest FSDS quarter must be optional"
        for a in fsds[1:]:
            assert a.optional is False, f"{a.name} must stay required"
        # FSNDS (#844): monthlies mirror the same posture (newest month
        # published weeks late → optional; the rest required); the
        # quarterly consolidations cover months 13-24, with the newest
        # quarterly on the consolidation boundary → optional.
        fsnds_m = [a for a in inventory if a.name.startswith("fsnds_") and "q" not in a.name.split("_notes")[0][-7:]]
        fsnds_q = [a for a in inventory if a.name.startswith("fsnds_") and "q" in a.name.split("_notes")[0][-7:]]
        assert len(fsnds_m) == 12, "FSNDS monthlies missing from inventory"
        assert len(fsnds_q) == 4, "FSNDS quarterly consolidations missing from inventory"
        assert fsnds_m[0].optional is True, "newest FSNDS month must be optional"
        for a in fsnds_m[1:]:
            assert a.optional is False, f"{a.name} must stay required"
        assert fsnds_q[0].optional is True, "boundary FSNDS quarter must be optional"
        for a in fsnds_q[1:]:
            assert a.optional is False, f"{a.name} must stay required"
        # Every non-13F, non-FSDS, non-FSNDS archive stays required.
        for a in inventory:
            if not a.name.startswith(("form13f_", "fsds_", "fsnds_")):
                assert a.optional is False, f"{a.name} must stay required"

    def test_archive_urls_use_correct_path_prefixes(self) -> None:
        inventory = build_bulk_archive_inventory(today=date(2026, 5, 8))
        by_name = {a.name: a.url for a in inventory}
        # Verified URL paths (HEAD-checked 2026-05-08).
        assert by_name["submissions.zip"].endswith("/Archives/edgar/daily-index/bulkdata/submissions.zip")
        assert by_name["companyfacts.zip"].endswith("/Archives/edgar/daily-index/xbrl/companyfacts.zip")
        # 13F under /files/structureddata/
        assert any("/files/structureddata/data/form-13f-data-sets/" in u for u in by_name.values())
        # Insider under /files/structureddata/ ending _form345.zip
        assert any(u.endswith("_form345.zip") for u in by_name.values())
        # N-PORT under /files/dera/data/ ending _nport.zip
        assert any(
            "/files/dera/data/form-n-port-data-sets/" in u and u.endswith("_nport.zip") for u in by_name.values()
        )


# ---------------------------------------------------------------------------
# Fatal-failure filter (#1423)
# ---------------------------------------------------------------------------


class TestFatalDownloadFailures:
    """An optional archive's error must NOT count as a fatal failure.

    This is the gate that decides whether ``sec_bulk_download`` raises
    ``BootstrapPartialDownloadError`` (→ stage error → db-lane cascade
    blocked) or marks success. Only the newest 13F window is optional.
    """

    @staticmethod
    def _result(name: str, *, error: str | None, optional: bool) -> ArchiveDownloadResult:
        return ArchiveDownloadResult(
            name=name,
            path=None,
            bytes_downloaded=0,
            error=error,
            optional=optional,
        )

    def test_optional_404_is_not_fatal(self) -> None:
        # A 404 on the optional newest window = not yet published → tolerated.
        results = [
            self._result("submissions.zip", error=None, optional=False),
            self._result(
                "form13f_01mar2026-31may2026.zip",
                error="HEAD failed: status=404 url=https://sec.gov/...",
                optional=True,
            ),
        ]
        assert _fatal_download_failures(results) == []

    def test_optional_non_404_error_is_fatal(self) -> None:
        # A 500/timeout/corrupt on the optional window is a real problem and
        # must stay fatal — only not-published 404 is tolerated (#1423).
        bad = self._result(
            "form13f_01mar2026-31may2026.zip",
            error="GET failed: status=500",
            optional=True,
        )
        results = [self._result("submissions.zip", error=None, optional=False), bad]
        assert _fatal_download_failures(results) == [bad]

    def test_required_archive_error_is_fatal(self) -> None:
        bad = self._result("companyfacts.zip", error="HEAD failed: status=500", optional=False)
        results = [self._result("submissions.zip", error=None, optional=False), bad]
        assert _fatal_download_failures(results) == [bad]

    def test_optional_404_does_not_mask_required_error(self) -> None:
        bad = self._result("insider_2026q1.zip", error="transfer failed: timeout", optional=False)
        results = [
            self._result(
                "form13f_01mar2026-31may2026.zip",
                error="HEAD failed: status=404",
                optional=True,
            ),
            bad,
        ]
        assert _fatal_download_failures(results) == [bad]


# ---------------------------------------------------------------------------
# Disk pre-flight
# ---------------------------------------------------------------------------


class TestPreflightPurge:
    def test_purge_archive_artifacts_removes_zip_partial_and_sidecars(self, tmp_path: Path) -> None:
        # _purge_archive_artifacts is the per-archive cleanup the
        # ETag-keyed pre-flight calls when reuse is rejected. It must
        # delete the .zip, .partial, .sha256, and .etag in one call.
        (tmp_path / "submissions.zip").write_bytes(b"complete payload")
        (tmp_path / "submissions.zip.partial").write_bytes(b"resume tail")
        (tmp_path / "submissions.zip.sha256").write_text("deadbeef")
        (tmp_path / "submissions.zip.etag").write_text('"abc-123"')
        _purge_archive_artifacts(tmp_path, "submissions.zip")
        for name in ("submissions.zip", "submissions.zip.partial", "submissions.zip.sha256", "submissions.zip.etag"):
            assert not (tmp_path / name).exists(), name

    def test_purge_archive_artifacts_no_op_when_missing(self, tmp_path: Path) -> None:
        _purge_archive_artifacts(tmp_path, "absent.zip")


class TestDiskPreflight:
    def test_check_disk_space_passes_when_free_above_floor(self, tmp_path: Path) -> None:
        ok, free = check_disk_space(tmp_path, min_free_bytes=1)
        assert ok is True
        assert free > 0

    def test_check_disk_space_rejects_when_floor_unrealistic(self, tmp_path: Path) -> None:
        ok, _ = check_disk_space(tmp_path, min_free_bytes=10**18)  # 1 EB
        assert ok is False


# ---------------------------------------------------------------------------
# Bandwidth probe
# ---------------------------------------------------------------------------


class TestBandwidthProbe:
    @pytest.mark.asyncio
    async def test_probe_returns_positive_mbps(self) -> None:
        body = b"x" * (PROBE_BYTES * 2)
        handler = _make_handler("https://example.test/probe.zip", body)
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            mbps = await measure_bandwidth_mbps(client, probe_url="https://example.test/probe.zip")
        assert mbps > 0

    @pytest.mark.asyncio
    async def test_probe_raises_on_404(self) -> None:
        def not_found(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        transport = httpx.MockTransport(not_found)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(RuntimeError):
                await measure_bandwidth_mbps(client, probe_url="https://example.test/missing.zip")

    @pytest.mark.asyncio
    async def test_probe_acquires_shared_rate_clock(self) -> None:
        # When a rate_limiter is supplied, the probe must call .acquire()
        # before issuing the GET so the request counts against the
        # shared SEC budget (#1042).
        body = b"x" * (PROBE_BYTES * 2)
        handler = _make_handler("https://example.test/probe.zip", body)
        transport = httpx.MockTransport(handler)

        acquire_calls: list[None] = []

        class _SpyLimiter:
            async def acquire(self) -> None:
                acquire_calls.append(None)

        async with httpx.AsyncClient(transport=transport) as client:
            await measure_bandwidth_mbps(
                client,
                probe_url="https://example.test/probe.zip",
                rate_limiter=_SpyLimiter(),
            )
        assert len(acquire_calls) == 1


class TestRateClockOrdering:
    """Spy-transport tests pinning the #1042 contract: every SEC HTTP
    request issued by the bulk downloader is preceded by an acquire()
    on the shared rate clock."""

    @pytest.mark.asyncio
    async def test_download_one_acquires_before_head_and_get(self, tmp_path: Path) -> None:
        from app.services.sec_bulk_download import _download_one

        events: list[str] = []
        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"

        def handler(request: httpx.Request) -> httpx.Response:
            events.append(request.method)
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "content-length": str(len(body)),
                        "content-type": "application/zip",
                    },
                )
            return httpx.Response(200, content=body)

        class _SpyLimiter:
            async def acquire(self) -> None:
                events.append("ACQUIRE")

        archive = BulkArchive(name="archive.zip", url=url)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await _download_one(client, archive, tmp_path, rate_limiter=_SpyLimiter())
        assert result.error is None
        # ACQUIRE must precede HEAD; ACQUIRE must precede GET.
        assert events.index("ACQUIRE") < events.index("HEAD")
        # The HEAD acquire is at index 0; the GET acquire happens before
        # the GET request, so two ACQUIREs total.
        assert events.count("ACQUIRE") == 2
        # Order is ACQUIRE, HEAD, ACQUIRE, GET.
        assert events == ["ACQUIRE", "HEAD", "ACQUIRE", "GET"]


# ---------------------------------------------------------------------------
# Per-archive download
# ---------------------------------------------------------------------------


class TestDownloadOne:
    @pytest.mark.asyncio
    async def test_atomic_rename_on_success(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)
        transport = httpx.MockTransport(_make_handler(url, body))
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is None
        assert result.path is not None
        assert result.path.exists()
        # Partial must not survive on success.
        assert not (tmp_path / "archive.zip.partial").exists()
        # Final ZIP must round-trip.
        assert _zip_round_trip(result.path)

    @pytest.mark.asyncio
    async def test_resume_from_partial(self, tmp_path: Path) -> None:
        body = _build_zip_bytes(filenames=("CIK1.json", "CIK2.json", "CIK3.json"))
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)

        # Pre-seed half the file as ``.partial``.
        partial_path = tmp_path / "archive.zip.partial"
        partial_path.write_bytes(body[: len(body) // 2])

        transport = httpx.MockTransport(_make_handler(url, body))
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)

        assert result.error is None
        assert result.path is not None
        assert result.path.read_bytes() == body
        # ``bytes_downloaded`` reflects the resumed-only suffix, not full body.
        assert result.bytes_downloaded == len(body) - len(body) // 2

    @pytest.mark.asyncio
    async def test_skip_when_final_file_already_present_and_valid(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)
        # Final file exists already and is a valid ZIP.
        final = tmp_path / "archive.zip"
        final.write_bytes(body)

        transport = httpx.MockTransport(_make_handler(url, body))
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.skipped is True
        assert result.bytes_downloaded == 0

    @pytest.mark.asyncio
    async def test_rejects_non_zip_content_type(self, tmp_path: Path) -> None:
        # SEC redirected to an HTML error page — Content-Type='text/html'
        # must reject before downloading any bytes (#1059).
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)

        def html_handler(request: httpx.Request) -> httpx.Response:
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={"Content-Length": "1024", "Content-Type": "text/html"},
                )
            return httpx.Response(200, content=b"<html>error</html>")

        transport = httpx.MockTransport(html_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is not None
        assert "Content-Type" in result.error
        assert not (tmp_path / "archive.zip").exists()

    @pytest.mark.asyncio
    async def test_rejects_missing_zip_magic_bytes(self, tmp_path: Path) -> None:
        # SEC served Content-Type='application/zip' but the body is
        # not a real ZIP — magic-byte check rejects (#1059).
        body = b"NOTAZIP" * 100_000  # >>>700KB of non-zip bytes
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)

        def lying_handler(request: httpx.Request) -> httpx.Response:
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "Content-Length": str(len(body)),
                        "Content-Type": "application/zip",
                    },
                )
            return httpx.Response(200, content=body)

        transport = httpx.MockTransport(lying_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is not None
        assert "magic bytes" in result.error
        assert not (tmp_path / "archive.zip").exists()

    @pytest.mark.asyncio
    async def test_corrupted_zip_keeps_partial_clean(self, tmp_path: Path) -> None:
        # ZIP magic present but the rest of the bytes don't form a
        # valid ZIP — round-trip catches it.
        body = b"PK\x03\x04" + b"BROKEN" * 100_000  # has magic, not valid zip
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "Content-Length": str(len(body)),
                        "Content-Type": "application/zip",
                    },
                )
            return httpx.Response(200, content=body)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is not None
        assert "ZIP round-trip failed" in result.error
        assert not (tmp_path / "archive.zip").exists()


# ---------------------------------------------------------------------------
# Top-level entrypoint
# ---------------------------------------------------------------------------


class TestDownloadBulkArchives:
    @pytest.mark.asyncio
    async def test_disk_preflight_blocks_when_floor_unrealistic(self, tmp_path: Path) -> None:
        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            min_free_bytes=10**18,  # 1 EB
            archives=[],
        )
        assert isinstance(result, BulkDownloadResult)
        assert result.mode == "skipped_disk"

    @pytest.mark.asyncio
    async def test_empty_archive_list_with_disk_pass_does_not_indexerror(self, tmp_path: Path) -> None:
        # Regression: if a caller passes archives=[] AND disk preflight
        # passes, the bandwidth-probe path must NOT IndexError on
        # archives[0]. Bot review BLOCKING.
        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            min_free_bytes=1,  # disk preflight will pass
            archives=[],
        )
        assert result.mode == "bulk"
        assert result.archives == []
        assert result.measured_mbps is None

    @pytest.mark.asyncio
    async def test_slow_connection_routes_to_fallback(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"

        # Threshold of 10000 Mbps is impossible to clear over a
        # MockTransport (no network), so probe always returns "below".
        archives = [BulkArchive(name="archive.zip", url=url)]
        transport = httpx.MockTransport(_make_handler(url, body))

        # Patch the client factory to inject the mock transport.
        import app.services.sec_bulk_download as mod

        orig = mod._make_client

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _patched(user_agent: str):
            async with httpx.AsyncClient(
                transport=transport,
                headers={"User-Agent": user_agent, "Accept": "application/zip,*/*"},
            ) as client:
                yield client

        mod._make_client = _patched
        try:
            result = await download_bulk_archives(
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
                bandwidth_threshold_mbps=10_000.0,
                min_free_bytes=1,
                archives=archives,
            )
        finally:
            mod._make_client = orig

        assert result.mode == "fallback"

    @pytest.mark.asyncio
    async def test_bulk_path_downloads_all_archives(self, tmp_path: Path) -> None:
        body_a = _build_zip_bytes(filenames=("CIK0001.json",))
        body_b = _build_zip_bytes(filenames=("CIK0002.json",))
        url_a = "https://example.test/archive_a.zip"
        url_b = "https://example.test/archive_b.zip"
        archives = [
            BulkArchive(name="archive_a.zip", url=url_a),
            BulkArchive(name="archive_b.zip", url=url_b),
        ]

        bodies = {url_a: body_a, url_b: body_b}

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            body = bodies.get(url)
            if body is None:
                return httpx.Response(404)
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "Content-Length": str(len(body)),
                        "Content-Type": "application/zip",
                    },
                )
            return httpx.Response(200, content=body, headers={"Content-Length": str(len(body))})

        transport = httpx.MockTransport(handler)
        import app.services.sec_bulk_download as mod

        orig = mod._make_client

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _patched(user_agent: str):
            async with httpx.AsyncClient(
                transport=transport,
                headers={"User-Agent": user_agent},
            ) as client:
                yield client

        mod._make_client = _patched
        try:
            result = await download_bulk_archives(
                target_dir=tmp_path,
                user_agent="ebull/test (admin@example.com)",
                bandwidth_threshold_mbps=0.0,
                min_free_bytes=1,
                archives=archives,
                concurrency=2,
            )
        finally:
            mod._make_client = orig

        assert result.mode == "bulk"
        assert len(result.archives) == 2
        assert all(r.error is None for r in result.archives)
        assert (tmp_path / "archive_a.zip").exists()
        assert (tmp_path / "archive_b.zip").exists()


# ---------------------------------------------------------------------------
# SEC 429 counter (#1545) — async download paths increment sec_throttle_429_total
# ---------------------------------------------------------------------------


class TestSec429Counter:
    """One test per 429-detection site. Delta-based assertions per the
    prevention log — the counter is process-global, never assert
    exact totals."""

    @pytest.mark.asyncio
    async def test_preflight_head_etag_429_increments(self) -> None:
        from app.providers.sec_throttle_metrics import sec_throttle_429_total
        from app.services.sec_bulk_download import _head_etag

        transport = httpx.MockTransport(lambda r: httpx.Response(429))
        before = sec_throttle_429_total()
        async with httpx.AsyncClient(transport=transport) as client:
            etag = await _head_etag(client, "https://example.test/archive.zip")
        assert etag is None
        assert sec_throttle_429_total() - before == 1

    @pytest.mark.asyncio
    async def test_bandwidth_probe_429_increments(self) -> None:
        from app.providers.sec_throttle_metrics import sec_throttle_429_total

        transport = httpx.MockTransport(lambda r: httpx.Response(429))
        before = sec_throttle_429_total()
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(RuntimeError):
                await measure_bandwidth_mbps(client, probe_url="https://example.test/probe.zip")
        assert sec_throttle_429_total() - before == 1

    @pytest.mark.asyncio
    async def test_head_size_and_type_429_increments(self) -> None:
        from app.providers.sec_throttle_metrics import sec_throttle_429_total
        from app.services.sec_bulk_download import _head_size_and_type

        transport = httpx.MockTransport(lambda r: httpx.Response(429))
        before = sec_throttle_429_total()
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(RuntimeError):
                await _head_size_and_type(client, "https://example.test/archive.zip")
        assert sec_throttle_429_total() - before == 1

    @pytest.mark.asyncio
    async def test_download_stream_429_increments(self, tmp_path: Path) -> None:
        from app.providers.sec_throttle_metrics import sec_throttle_429_total

        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "content-length": str(len(body)),
                        "content-type": "application/zip",
                    },
                )
            return httpx.Response(429)

        before = sec_throttle_429_total()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is not None and "429" in result.error
        assert sec_throttle_429_total() - before == 1

    @pytest.mark.asyncio
    async def test_download_resume_fresh_stream_429_increments(self, tmp_path: Path) -> None:
        """Resume path: Range GET answered with 200-but-then-429 on the
        fresh retry — the fresh stream's 429 must count too."""
        from app.providers.sec_throttle_metrics import sec_throttle_429_total

        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url)

        # Pre-seed a partial so the resume branch fires.
        (tmp_path / "archive.zip.partial").write_bytes(body[: len(body) // 2])

        get_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal get_count
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "content-length": str(len(body)),
                        "content-type": "application/zip",
                    },
                )
            get_count += 1
            # First GET (Range) ignored with a 200-shaped non-206 → fresh
            # retry; fresh GET answers 429.
            if get_count == 1:
                return httpx.Response(200, content=b"")
            return httpx.Response(429)

        before = sec_throttle_429_total()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is not None
        assert sec_throttle_429_total() - before == 1
