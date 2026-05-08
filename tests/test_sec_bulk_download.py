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
    BulkArchive,
    BulkDownloadResult,
    _download_one,
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
                headers={"Content-Length": str(len(archive_body))},
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
# Disk pre-flight
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Per-archive download
# ---------------------------------------------------------------------------


class TestDownloadOne:
    @pytest.mark.asyncio
    async def test_atomic_rename_on_success(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url, expected_min_bytes=1)
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
        archive = BulkArchive(name="archive.zip", url=url, expected_min_bytes=1)

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
        archive = BulkArchive(name="archive.zip", url=url, expected_min_bytes=1)
        # Final file exists already and is a valid ZIP.
        final = tmp_path / "archive.zip"
        final.write_bytes(body)

        transport = httpx.MockTransport(_make_handler(url, body))
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.skipped is True
        assert result.bytes_downloaded == 0

    @pytest.mark.asyncio
    async def test_rejects_truncated_archive(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        url = "https://example.test/archive.zip"
        archive = BulkArchive(
            name="archive.zip",
            url=url,
            expected_min_bytes=10 * len(body),  # impossibly large floor
        )
        transport = httpx.MockTransport(_make_handler(url, body))
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is not None
        assert "below floor" in result.error
        # No final file written.
        assert not (tmp_path / "archive.zip").exists()

    @pytest.mark.asyncio
    async def test_corrupted_zip_keeps_partial_clean(self, tmp_path: Path) -> None:
        body = b"NOTAZIP" * 100_000  # >>> 700KB of non-zip bytes
        url = "https://example.test/archive.zip"
        archive = BulkArchive(name="archive.zip", url=url, expected_min_bytes=1)
        transport = httpx.MockTransport(_make_handler(url, body))
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
        archives = [BulkArchive(name="archive.zip", url=url, expected_min_bytes=1)]
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
            BulkArchive(name="archive_a.zip", url=url_a, expected_min_bytes=1),
            BulkArchive(name="archive_b.zip", url=url_b, expected_min_bytes=1),
        ]

        bodies = {url_a: body_a, url_b: body_b}

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            body = bodies.get(url)
            if body is None:
                return httpx.Response(404)
            if request.method == "HEAD":
                return httpx.Response(200, headers={"Content-Length": str(len(body))})
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
