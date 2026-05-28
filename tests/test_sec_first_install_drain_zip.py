"""Hybrid local-zip path unit tests (#1277).

Pure-Python — no DB. The integration-mark module
``tests/test_sec_first_install_drain.py`` covers the drain-with-zip
integration cases (T2/T3) where the DB is required.

Spec: ``docs/proposals/etl/1277-s16-local-zip.md`` §3.1 + §4 T1a-T1d.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from app.jobs.sec_first_install_drain import _make_zip_http_get

# Minimal AAPL primary-page payload — shape only, no real SEC bytes.
_AAPL_PAYLOAD = {
    "cik": "320193",
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-26-000001"],
            "filingDate": ["2026-01-15"],
            "form": ["8-K"],
            "acceptanceDateTime": ["2026-01-15T16:30:00.000Z"],
            "primaryDocument": ["item502.htm"],
        },
        "files": [],
    },
}


def _build_submissions_zip(tmp_path: Path, ciks_payload: dict[str, dict]) -> Path:
    """Build a synthetic ``submissions.zip`` on disk.

    Mirrors the real archive shape: ONLY primary ``CIK<10>.json``
    entries. No synthetic secondary pages — per #1277 IMPORTANT-3 fold
    that would encode a false invariant against the real SEC archive
    (reference: ``app/services/sec_submissions_files_walk.py:16-23``).
    """
    archive_path = tmp_path / "submissions.zip"
    with zipfile.ZipFile(archive_path, "w") as zf:
        for cik_padded, payload in ciks_payload.items():
            assert cik_padded.isdigit() and len(cik_padded) == 10
            zf.writestr(f"CIK{cik_padded}.json", json.dumps(payload).encode("utf-8"))
    return archive_path


class TestMakeZipHttpGet:
    """#1277 T1a-T1d — hybrid HttpGet routing."""

    def test_primary_hit_returns_zip_bytes(self, tmp_path: Path) -> None:
        archive = _build_submissions_zip(tmp_path, {"0000320193": _AAPL_PAYLOAD})
        fallback_called: list[str] = []

        def _fallback(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            fallback_called.append(url)
            return (500, b"")

        http_get, zf = _make_zip_http_get(archive, fallback_http_get=_fallback)
        try:
            status, body = http_get("https://data.sec.gov/submissions/CIK0000320193.json", {})
        finally:
            zf.close()
        assert status == 200
        assert json.loads(body)["cik"] == "320193"
        assert fallback_called == []

    def test_primary_miss_returns_404(self, tmp_path: Path) -> None:
        archive = _build_submissions_zip(tmp_path, {"0000320193": _AAPL_PAYLOAD})

        def _fallback(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            raise AssertionError("fallback must not fire for primary miss")

        http_get, zf = _make_zip_http_get(archive, fallback_http_get=_fallback)
        try:
            status, body = http_get("https://data.sec.gov/submissions/CIK0000999999.json", {})
        finally:
            zf.close()
        assert status == 404
        assert body == b""

    def test_secondary_url_delegates_to_fallback(self, tmp_path: Path) -> None:
        # The bulk archive does NOT contain secondary pages — those must
        # route through the real HTTP transport. Spec: §3.1 + #1277
        # BLOCKING-2 fold. Reference:
        # app/services/sec_submissions_files_walk.py:16-23.
        archive = _build_submissions_zip(tmp_path, {"0000320193": _AAPL_PAYLOAD})
        fallback_calls: list[str] = []

        def _fallback(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            fallback_calls.append(url)
            return (200, b'{"filings": {"recent": {}}}')

        http_get, zf = _make_zip_http_get(archive, fallback_http_get=_fallback)
        try:
            secondary_url = "https://data.sec.gov/submissions/CIK0000320193-submissions-001.json"
            status, _ = http_get(secondary_url, {})
        finally:
            zf.close()
        assert status == 200
        assert fallback_calls == [secondary_url]

    def test_non_submissions_url_delegates_to_fallback(self, tmp_path: Path) -> None:
        archive = _build_submissions_zip(tmp_path, {"0000320193": _AAPL_PAYLOAD})
        fallback_calls: list[str] = []

        def _fallback(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            fallback_calls.append(url)
            return (418, b"")

        http_get, zf = _make_zip_http_get(archive, fallback_http_get=_fallback)
        try:
            unrelated = "https://example.com/foo.json"
            status, _ = http_get(unrelated, {})
        finally:
            zf.close()
        assert status == 418
        assert fallback_calls == [unrelated]

    def test_corrupt_member_read_falls_back_to_http(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        # Codex 2 IMPORTANT fold — _get must catch read-time zip errors
        # (BadZipFile / OSError on zf.open() or fh.read()), not just
        # open-time errors during _make_zip_http_get construction. A
        # corrupt member should delegate to fallback_http_get rather
        # than aborting S16.
        archive = _build_submissions_zip(tmp_path, {"0000320193": _AAPL_PAYLOAD})
        fallback_calls: list[str] = []

        def _fallback(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            fallback_calls.append(url)
            return (200, b'{"served": "from-http"}')

        http_get, zf = _make_zip_http_get(archive, fallback_http_get=_fallback)

        # Monkeypatch the open ZipFile so that opening the AAPL entry
        # raises BadZipFile at read-time (simulates a corrupt CRC /
        # truncated member surfaced after construction).
        original_open = zf.open

        def _bad_open(name, *args, **kwargs):
            if name == "CIK0000320193.json":
                raise zipfile.BadZipFile(
                    "Bad CRC for entry CIK0000320193.json"
                )
            return original_open(name, *args, **kwargs)

        monkeypatch.setattr(zf, "open", _bad_open)

        try:
            url = "https://data.sec.gov/submissions/CIK0000320193.json"
            status, body = http_get(url, {})
        finally:
            zf.close()
        assert status == 200
        assert body == b'{"served": "from-http"}'
        assert fallback_calls == [url]
