"""Tests for the ETag-keyed bulk-archive reuse pre-flight (PR-5b).

Settled decision: docs/settled-decisions.md, "Bulk archive reuse keyed
on SEC ETag + SHA-256 (2026-05-22)". Spec:
docs/superpowers/specs/2026-05-22-bootstrap-etl-optimisation-v2.md §10.

These tests exercise ``_preflight_etag_keyed_reuse`` +
``download_bulk_archives`` end-to-end against an in-process
``httpx.MockTransport``. The cold-install / unchanged-SEC / changed-SEC
matrix is the contract:

  cold install                 -> downloads, writes sidecars
  unchanged ETag + good SHA    -> 0-byte reuse, manifest reuse_reason
  changed ETag                 -> redownload (purge + GET)
  SHA mismatch on disk         -> redownload
  missing sidecar              -> redownload (defensive)
  BOOTSTRAP_FORCE_REDOWNLOAD=1 -> redownload regardless
  .partial alongside .zip      -> redownload (interrupted prior run)
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import zipfile
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

import app.services.sec_bulk_download as mod
from app.services.sec_bulk_download import (
    RUN_MANIFEST_NAME,
    BulkArchive,
    _ArchiveReuseDecision,
    _atomic_write_sidecar,
    _preflight_etag_keyed_reuse,
    _sha256_file,
    assert_archive_belongs_to_run,
    download_bulk_archives,
    write_run_manifest,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_zip_bytes(filenames: tuple[str, ...] = ("CIK0000320193.json",)) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name in filenames:
            zf.writestr(name, b"{}")
    return buf.getvalue()


def _make_handler_with_etag(
    archive_url: str,
    archive_body: bytes,
    *,
    etag: str | None,
    request_log: list[str] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    """MockTransport handler that serves ``archive_body`` at ``archive_url``
    with optional ``ETag`` header on both HEAD and GET.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request_log is not None:
            range_header = request.headers.get("Range")
            label = f"{request.method}"
            if range_header:
                label = f"{request.method}[Range={range_header}]"
            request_log.append(f"{label} {request.url}")
        if str(request.url) != archive_url:
            return httpx.Response(404)
        common_headers: dict[str, str] = {
            "Content-Length": str(len(archive_body)),
            "Content-Type": "application/zip",
        }
        if etag is not None:
            common_headers["ETag"] = etag
        if request.method == "HEAD":
            return httpx.Response(200, headers=common_headers)
        if request.method == "GET":
            range_header = request.headers.get("Range")
            if range_header:
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
                            **({"ETag": etag} if etag else {}),
                        },
                    )
            return httpx.Response(200, content=archive_body, headers=common_headers)
        return httpx.Response(405)

    return handler


def _patch_client(monkeypatch: pytest.MonkeyPatch, transport: httpx.MockTransport) -> None:
    """Replace ``_make_client`` with a fixture-friendly factory."""

    @asynccontextmanager
    async def _patched(user_agent: str):
        async with httpx.AsyncClient(
            transport=transport,
            headers={"User-Agent": user_agent, "Accept": "application/zip,*/*"},
        ) as client:
            yield client

    monkeypatch.setattr(mod, "_make_client", _patched)


# ---------------------------------------------------------------------------
# Sidecar primitives
# ---------------------------------------------------------------------------


class TestSidecarPrimitives:
    def test_sha256_file_streams_multi_chunk(self, tmp_path: Path) -> None:
        # 3 MB body across multiple 1 MB chunks: hash must match the
        # one-shot hashlib computation.
        body = (b"abc123" * (1024 * 1024 // 6 + 1))[: 3 * 1024 * 1024]
        path = tmp_path / "x.zip"
        path.write_bytes(body)
        expected = hashlib.sha256(body).hexdigest()
        assert _sha256_file(path) == expected

    def test_atomic_sidecar_write_replaces_existing(self, tmp_path: Path) -> None:
        target = tmp_path / "x.zip.etag"
        _atomic_write_sidecar(target, '"v1"')
        assert target.read_text(encoding="utf-8") == '"v1"'
        _atomic_write_sidecar(target, '"v2"')
        assert target.read_text(encoding="utf-8") == '"v2"'
        # No leftover .tmp.
        assert not target.with_suffix(target.suffix + ".tmp").exists()


# ---------------------------------------------------------------------------
# _preflight_etag_keyed_reuse — unit tests against MockTransport
# ---------------------------------------------------------------------------


class TestPreflightDecision:
    @pytest.mark.asyncio
    async def test_cold_install_decides_redownload(self, tmp_path: Path) -> None:
        # No local files at all → reuse rejected.
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, _build_zip_bytes(), etag='"e1"'))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "local_missing"

    @pytest.mark.asyncio
    async def test_etag_match_with_sha_match_decides_reuse(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        (Path(str(zip_path) + ".sha256")).write_text(hashlib.sha256(body).hexdigest())
        etag = '"504b124e9474334e889e9e525db95c14-184"'
        (Path(str(zip_path) + ".etag")).write_text(etag)

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is True
        assert d.reason == "etag_match_sha256_verified"
        assert d.sec_etag == etag
        # Local files preserved.
        assert zip_path.exists()

    @pytest.mark.asyncio
    async def test_etag_mismatch_purges_local(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        (Path(str(zip_path) + ".sha256")).write_text(hashlib.sha256(body).hexdigest())
        (Path(str(zip_path) + ".etag")).write_text('"old"')

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag='"new"'))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "etag_mismatch"
        # Purged: zip + sidecars removed.
        assert not zip_path.exists()
        assert not Path(str(zip_path) + ".sha256").exists()
        assert not Path(str(zip_path) + ".etag").exists()

    @pytest.mark.asyncio
    async def test_sha_mismatch_purges_local_even_if_etag_matches(self, tmp_path: Path) -> None:
        # Tampered .zip but sidecars look "correct" → SHA recomputation
        # catches it.
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        # SHA sidecar points to a different content (corruption on disk).
        (Path(str(zip_path) + ".sha256")).write_text(hashlib.sha256(b"different").hexdigest())
        etag = '"e1"'
        (Path(str(zip_path) + ".etag")).write_text(etag)

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "sha256_mismatch"
        assert not zip_path.exists()

    @pytest.mark.asyncio
    async def test_missing_sha_sidecar_purges_local(self, tmp_path: Path) -> None:
        # .zip exists but no .sha256 sidecar → defensive re-download.
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        (Path(str(zip_path) + ".etag")).write_text('"e1"')

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag='"e1"'))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "sha256_sidecar_missing"
        assert not zip_path.exists()

    @pytest.mark.asyncio
    async def test_missing_etag_sidecar_purges_local(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        (Path(str(zip_path) + ".sha256")).write_text(hashlib.sha256(body).hexdigest())

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag='"e1"'))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "etag_sidecar_missing"
        assert not zip_path.exists()

    @pytest.mark.asyncio
    async def test_partial_alongside_zip_forces_redownload(self, tmp_path: Path) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        partial_path = zip_path.with_suffix(zip_path.suffix + ".partial")
        partial_path.write_bytes(b"interrupted tail")
        (Path(str(zip_path) + ".sha256")).write_text(hashlib.sha256(body).hexdigest())
        (Path(str(zip_path) + ".etag")).write_text('"e1"')

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag='"e1"'))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "partial_present"
        # Purge removes BOTH .zip and .partial.
        assert not zip_path.exists()
        assert not partial_path.exists()

    @pytest.mark.asyncio
    async def test_head_without_etag_header_forces_redownload(self, tmp_path: Path) -> None:
        # SEC sometimes serves static files without an ETag — we cannot
        # prove freshness, safe default is re-download.
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        (Path(str(zip_path) + ".sha256")).write_text(hashlib.sha256(body).hexdigest())
        (Path(str(zip_path) + ".etag")).write_text('"e1"')

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=None))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "sec_etag_missing"

    @pytest.mark.asyncio
    async def test_force_redownload_env_bypasses_reuse(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        (Path(str(zip_path) + ".sha256")).write_text(hashlib.sha256(body).hexdigest())
        etag = '"e1"'
        (Path(str(zip_path) + ".etag")).write_text(etag)

        # Track HEAD calls — force-mode must NOT need them.
        request_log: list[str] = []
        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag, request_log=request_log))
        monkeypatch.setenv("BOOTSTRAP_FORCE_REDOWNLOAD", "1")
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is False
        assert d.reason == "force_redownload_env"
        assert not zip_path.exists()
        assert all(not entry.startswith("HEAD ") for entry in request_log), request_log

    @pytest.mark.asyncio
    async def test_preflight_wipes_stale_run_manifest(self, tmp_path: Path) -> None:
        # Pre-existing run-manifest from a prior bootstrap must be
        # removed up front; write_run_manifest re-stamps later.
        manifest = tmp_path / RUN_MANIFEST_NAME
        manifest.write_text('{"bootstrap_run_id": 99, "mode": "bulk", "archives": []}')
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, _build_zip_bytes(), etag='"e1"'))
        async with httpx.AsyncClient(transport=transport) as client:
            await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        assert not manifest.exists()

    @pytest.mark.asyncio
    async def test_stale_archive_not_in_inventory_is_cleaned(self, tmp_path: Path) -> None:
        # A 13F window that rolled off the list must not stay on disk.
        # Current inventory contains only ``archive.zip``; stray
        # ``form13f_old.zip`` + sidecars should be purged.
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        stray = tmp_path / "form13f_old.zip"
        stray.write_bytes(b"stale")
        (Path(str(stray) + ".sha256")).write_text("dead")
        (Path(str(stray) + ".etag")).write_text('"old"')
        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, _build_zip_bytes(), etag='"e1"'))
        async with httpx.AsyncClient(transport=transport) as client:
            await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        assert not stray.exists()
        assert not Path(str(stray) + ".sha256").exists()
        assert not Path(str(stray) + ".etag").exists()


# ---------------------------------------------------------------------------
# End-to-end via download_bulk_archives
# ---------------------------------------------------------------------------


class TestEndToEndReuse:
    @pytest.mark.asyncio
    async def test_cold_install_writes_sidecars_and_downloaded_in_run_provenance(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        etag = '"504b124e9474334e889e9e525db95c14-184"'
        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag))
        _patch_client(monkeypatch, transport)

        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            bandwidth_threshold_mbps=0.0,
            min_free_bytes=1,
            archives=[archive],
            concurrency=1,
        )

        assert result.mode == "bulk"
        assert len(result.archives) == 1
        r = result.archives[0]
        assert r.error is None
        assert r.reuse_reason == "downloaded_in_run"
        assert r.bytes_downloaded == len(body)
        # Sidecars exist with expected content.
        assert (tmp_path / "archive.zip").read_bytes() == body
        sha_sidecar = (tmp_path / "archive.zip.sha256").read_text(encoding="utf-8").strip()
        assert sha_sidecar == hashlib.sha256(body).hexdigest()
        etag_sidecar = (tmp_path / "archive.zip.etag").read_text(encoding="utf-8").strip()
        assert etag_sidecar == etag

        # write_run_manifest path stamps reuse_reason.
        write_run_manifest(tmp_path, bootstrap_run_id=42, archives=result.archives)
        manifest = json.loads((tmp_path / RUN_MANIFEST_NAME).read_text())
        assert manifest["archives"][0]["reuse_reason"] == "downloaded_in_run"
        assert_archive_belongs_to_run(tmp_path, "archive.zip", bootstrap_run_id=42)

    @pytest.mark.asyncio
    async def test_rerun_with_unchanged_sec_reuses_zero_bytes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        etag = '"e1"'

        # Cold install seeds the on-disk state.
        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        _atomic_write_sidecar(Path(str(zip_path) + ".sha256"), hashlib.sha256(body).hexdigest())
        _atomic_write_sidecar(Path(str(zip_path) + ".etag"), etag)

        request_log: list[str] = []
        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag, request_log=request_log))
        _patch_client(monkeypatch, transport)

        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            bandwidth_threshold_mbps=0.0,
            min_free_bytes=1,
            archives=[archive],
            concurrency=1,
        )

        assert result.mode == "bulk"
        r = result.archives[0]
        assert r.error is None
        assert r.reuse_reason == "etag_match_sha256_verified"
        assert r.bytes_downloaded == 0
        # No FULL GET fired against the archive URL — only HEAD(s) +
        # the bandwidth-probe Range-GET. (Range-GET of the first 4 MB
        # is still issued; it does not download the archive body.)
        full_gets = [e for e in request_log if e.startswith("GET ") and archive.url in e]
        assert full_gets == [], f"unexpected full GETs during reuse: {full_gets!r}"

        # Manifest provenance accepts the reuse reason.
        write_run_manifest(tmp_path, bootstrap_run_id=43, archives=result.archives)
        manifest = json.loads((tmp_path / RUN_MANIFEST_NAME).read_text())
        assert manifest["archives"][0]["reuse_reason"] == "etag_match_sha256_verified"
        assert_archive_belongs_to_run(tmp_path, "archive.zip", bootstrap_run_id=43)

    @pytest.mark.asyncio
    async def test_rerun_with_changed_sec_redownloads(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        old_body = _build_zip_bytes(filenames=("OLD.json",))
        new_body = _build_zip_bytes(filenames=("NEW1.json", "NEW2.json"))
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        old_etag = '"v1"'
        new_etag = '"v2"'

        zip_path = tmp_path / archive.name
        zip_path.write_bytes(old_body)
        _atomic_write_sidecar(Path(str(zip_path) + ".sha256"), hashlib.sha256(old_body).hexdigest())
        _atomic_write_sidecar(Path(str(zip_path) + ".etag"), old_etag)

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, new_body, etag=new_etag))
        _patch_client(monkeypatch, transport)

        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            bandwidth_threshold_mbps=0.0,
            min_free_bytes=1,
            archives=[archive],
            concurrency=1,
        )

        assert result.mode == "bulk"
        r = result.archives[0]
        assert r.error is None
        assert r.reuse_reason == "downloaded_in_run"
        assert r.bytes_downloaded == len(new_body)
        assert zip_path.read_bytes() == new_body
        # Sidecars refreshed.
        assert (tmp_path / "archive.zip.sha256").read_text(encoding="utf-8").strip() == hashlib.sha256(
            new_body
        ).hexdigest()
        assert (tmp_path / "archive.zip.etag").read_text(encoding="utf-8").strip() == new_etag

    @pytest.mark.asyncio
    async def test_corrupt_local_sha_triggers_redownload(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        etag = '"e1"'

        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        # Corrupt SHA sidecar.
        _atomic_write_sidecar(Path(str(zip_path) + ".sha256"), "deadbeef" * 8)
        _atomic_write_sidecar(Path(str(zip_path) + ".etag"), etag)

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag))
        _patch_client(monkeypatch, transport)

        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            bandwidth_threshold_mbps=0.0,
            min_free_bytes=1,
            archives=[archive],
            concurrency=1,
        )

        r = result.archives[0]
        assert r.error is None
        assert r.reuse_reason == "downloaded_in_run"
        # SHA sidecar is now the correct value (rewritten by download).
        assert (tmp_path / "archive.zip.sha256").read_text(encoding="utf-8").strip() == hashlib.sha256(body).hexdigest()

    @pytest.mark.asyncio
    async def test_force_env_redownloads_even_when_etag_matches(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        etag = '"e1"'

        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        _atomic_write_sidecar(Path(str(zip_path) + ".sha256"), hashlib.sha256(body).hexdigest())
        _atomic_write_sidecar(Path(str(zip_path) + ".etag"), etag)

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag))
        _patch_client(monkeypatch, transport)
        monkeypatch.setenv("BOOTSTRAP_FORCE_REDOWNLOAD", "1")

        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            bandwidth_threshold_mbps=0.0,
            min_free_bytes=1,
            archives=[archive],
            concurrency=1,
        )

        r = result.archives[0]
        assert r.error is None
        assert r.reuse_reason == "downloaded_in_run"
        assert r.bytes_downloaded == len(body)

    @pytest.mark.asyncio
    async def test_partial_alongside_zip_triggers_full_redownload(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = _build_zip_bytes()
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        etag = '"e1"'

        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        partial_path = zip_path.with_suffix(zip_path.suffix + ".partial")
        partial_path.write_bytes(b"interrupted")
        _atomic_write_sidecar(Path(str(zip_path) + ".sha256"), hashlib.sha256(body).hexdigest())
        _atomic_write_sidecar(Path(str(zip_path) + ".etag"), etag)

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=etag))
        _patch_client(monkeypatch, transport)

        result = await download_bulk_archives(
            target_dir=tmp_path,
            user_agent="ebull/test (admin@example.com)",
            bandwidth_threshold_mbps=0.0,
            min_free_bytes=1,
            archives=[archive],
            concurrency=1,
        )

        r = result.archives[0]
        assert r.error is None
        assert r.reuse_reason == "downloaded_in_run"
        assert not partial_path.exists()


# ---------------------------------------------------------------------------
# assert_archive_belongs_to_run accepts both reuse reasons
# ---------------------------------------------------------------------------


class TestAssertArchiveBelongsToRun:
    def test_accepts_downloaded_in_run(self, tmp_path: Path) -> None:
        path = tmp_path / RUN_MANIFEST_NAME
        path.write_text(
            json.dumps(
                {
                    "bootstrap_run_id": 1,
                    "mode": "bulk",
                    "archives": [
                        {"name": "x.zip", "bytes_downloaded": 100, "error": None, "reuse_reason": "downloaded_in_run"}
                    ],
                }
            )
        )
        assert_archive_belongs_to_run(tmp_path, "x.zip", bootstrap_run_id=1)

    def test_accepts_etag_match_sha256_verified(self, tmp_path: Path) -> None:
        path = tmp_path / RUN_MANIFEST_NAME
        path.write_text(
            json.dumps(
                {
                    "bootstrap_run_id": 1,
                    "mode": "bulk",
                    "archives": [
                        {
                            "name": "x.zip",
                            "bytes_downloaded": 0,
                            "error": None,
                            "reuse_reason": "etag_match_sha256_verified",
                        }
                    ],
                }
            )
        )
        assert_archive_belongs_to_run(tmp_path, "x.zip", bootstrap_run_id=1)

    def test_rejects_unknown_reuse_reason(self, tmp_path: Path) -> None:
        path = tmp_path / RUN_MANIFEST_NAME
        path.write_text(
            json.dumps(
                {
                    "bootstrap_run_id": 1,
                    "mode": "bulk",
                    "archives": [{"name": "x.zip", "bytes_downloaded": 0, "error": None, "reuse_reason": "made_up"}],
                }
            )
        )
        with pytest.raises(RuntimeError, match="reuse_reason"):
            assert_archive_belongs_to_run(tmp_path, "x.zip", bootstrap_run_id=1)

    def test_missing_reuse_reason_defaults_to_downloaded_in_run(self, tmp_path: Path) -> None:
        # Backward compatibility: manifests written by pre-PR-5b code
        # paths may omit reuse_reason; the assertion must still pass.
        path = tmp_path / RUN_MANIFEST_NAME
        path.write_text(
            json.dumps(
                {
                    "bootstrap_run_id": 1,
                    "mode": "bulk",
                    "archives": [{"name": "x.zip", "bytes_downloaded": 100, "error": None}],
                }
            )
        )
        assert_archive_belongs_to_run(tmp_path, "x.zip", bootstrap_run_id=1)


# ---------------------------------------------------------------------------
# Fixture round-trip — recorded SEC HEAD response
# ---------------------------------------------------------------------------


_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sec_bulk_head"


def _load_fixture_etag(name: str) -> str:
    """Read the recorded ETag for ``name`` from the fixture dir."""
    path = _FIXTURE_DIR / f"{name}.headers.json"
    headers = json.loads(path.read_text(encoding="utf-8"))
    return headers["etag"]


class TestRecordedHeadFixture:
    @pytest.mark.asyncio
    async def test_unchanged_etag_scenario_against_recorded_fixture(self, tmp_path: Path) -> None:
        """Use the recorded SEC HEAD ETag for submissions.zip; verify
        the reuse decision matches against the same ETag."""
        recorded_etag = _load_fixture_etag("submissions.zip")
        body = _build_zip_bytes()
        archive = BulkArchive(name="submissions.zip", url="https://example.test/submissions.zip")

        zip_path = tmp_path / archive.name
        zip_path.write_bytes(body)
        _atomic_write_sidecar(Path(str(zip_path) + ".sha256"), hashlib.sha256(body).hexdigest())
        _atomic_write_sidecar(Path(str(zip_path) + ".etag"), recorded_etag)

        transport = httpx.MockTransport(_make_handler_with_etag(archive.url, body, etag=recorded_etag))
        async with httpx.AsyncClient(transport=transport) as client:
            decisions = await _preflight_etag_keyed_reuse(client, [archive], tmp_path)
        d = decisions[archive.name]
        assert d.reused is True
        assert d.sec_etag == recorded_etag


# ---------------------------------------------------------------------------
# Dataclass + module sanity
# ---------------------------------------------------------------------------


def test_archive_reuse_decision_is_immutable() -> None:
    d = _ArchiveReuseDecision(name="x", reused=True, sec_etag='"e"', reason="ok")
    with pytest.raises(Exception):
        d.reused = False  # type: ignore[misc]


class TestArchiveNameValidation:
    """Defence-in-depth path-traversal guard added by Codex 2 BLOCKING."""

    def test_validator_accepts_legit_inventory_names(self) -> None:
        from app.services.sec_bulk_download import _validate_archive_name

        for name in (
            "submissions.zip",
            "companyfacts.zip",
            "form13f_01dec2025-28feb2026.zip",
            "insider_2025q4.zip",
            "nport_2025q4.zip",
        ):
            assert _validate_archive_name(name) == name

    @pytest.mark.parametrize(
        "name",
        [
            "",
            ".",
            "..",
            "../escape.zip",
            "../../etc/passwd",
            "a/b.zip",
            "a\\b.zip",
            "/abs/path.zip",
            "with\x00nul.zip",
        ],
    )
    def test_validator_rejects_path_traversal(self, name: str) -> None:
        from app.services.sec_bulk_download import _validate_archive_name

        with pytest.raises(ValueError):
            _validate_archive_name(name)

    def test_purge_rejects_traversal_name_without_touching_outside_dir(self, tmp_path: Path) -> None:
        from app.services.sec_bulk_download import _purge_archive_artifacts

        outside = tmp_path.parent / "must_survive.zip"
        outside.write_bytes(b"sentinel")
        # Construct a target_dir under tmp_path and try a ../ name —
        # validator must raise before any unlink fires.
        target = tmp_path / "bulk"
        target.mkdir()
        with pytest.raises(ValueError):
            _purge_archive_artifacts(target, "../must_survive.zip")
        assert outside.exists()
        outside.unlink()


class TestIfRangeOnResume:
    """If SEC rebuilds between HEAD and resume-GET, ``If-Range`` makes
    the server return 200 + full body — the existing 200-after-Range
    fallback discards the partial and restarts. Codex 2 LOW/MEDIUM."""

    @pytest.mark.asyncio
    async def test_if_range_header_sent_when_partial_exists(self, tmp_path: Path) -> None:
        from app.services.sec_bulk_download import _download_one

        body = _build_zip_bytes(filenames=("a.json", "b.json", "c.json", "d.json"))
        archive = BulkArchive(name="archive.zip", url="https://example.test/archive.zip")
        partial_path = tmp_path / "archive.zip.partial"
        partial_path.write_bytes(body[: len(body) // 2])

        seen_if_range: list[str] = []
        etag = '"resume-binding"'

        def handler(request: httpx.Request) -> httpx.Response:
            if str(request.url) != archive.url:
                return httpx.Response(404)
            if request.method == "HEAD":
                return httpx.Response(
                    200,
                    headers={
                        "Content-Length": str(len(body)),
                        "Content-Type": "application/zip",
                        "ETag": etag,
                    },
                )
            # GET — record the If-Range header.
            if_range = request.headers.get("If-Range")
            if if_range:
                seen_if_range.append(if_range)
            range_header = request.headers.get("Range")
            if range_header:
                start = int(range_header.removeprefix("bytes=").split("-", 1)[0])
                chunk = body[start:]
                return httpx.Response(
                    206,
                    content=chunk,
                    headers={
                        "Content-Length": str(len(chunk)),
                        "Content-Range": f"bytes {start}-{len(body) - 1}/{len(body)}",
                        "ETag": etag,
                    },
                )
            return httpx.Response(200, content=body, headers={"Content-Length": str(len(body))})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _download_one(client, archive, tmp_path)
        assert result.error is None
        assert seen_if_range == [etag], f"expected If-Range to bind to HEAD etag, got {seen_if_range!r}"


def test_force_redownload_env_var_truthy_values(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services.sec_bulk_download import _force_redownload_active

    for v in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv("BOOTSTRAP_FORCE_REDOWNLOAD", v)
        assert _force_redownload_active() is True
    for v in ("", "0", "false", "no", "off", "anythingelse"):
        monkeypatch.setenv("BOOTSTRAP_FORCE_REDOWNLOAD", v)
        assert _force_redownload_active() is False
    # Cleanup
    monkeypatch.delenv("BOOTSTRAP_FORCE_REDOWNLOAD", raising=False)
    assert os.environ.get("BOOTSTRAP_FORCE_REDOWNLOAD") is None
