"""SEC bulk-archive download service (#1021).

Phase A3 of the bulk-datasets-first first-install bootstrap (#1020).
Downloads SEC's nightly + quarterly bulk archives in parallel so
Phase C can ingest them locally without per-CIK HTTP fetches.

Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md

Verified URLs (HEAD against www.sec.gov on 2026-05-08):

- ``submissions.zip``  1.54 GB at /Archives/edgar/daily-index/bulkdata/
- ``companyfacts.zip`` 1.38 GB at /Archives/edgar/daily-index/xbrl/
- Form 13F rolling 3-month windows ~90 MB at /files/structureddata/data/form-13f-data-sets/
- Insider Transactions ~14 MB quarterly at /files/structureddata/data/insider-transactions-data-sets/
  ``<YYYY>q<N>_form345.zip``
- Form N-PORT ~463 MB quarterly at /files/dera/data/form-n-port-data-sets/<YYYY>q<N>_nport.zip

Behaviours:

- Atomic write: each archive downloads to ``<name>.partial`` and
  renames to ``<name>`` only after Content-Length matches the HEAD
  response and ``zipfile.ZipFile.namelist()`` round-trips clean.
- Resume: a pre-existing ``.partial`` triggers an HTTP Range request
  for the un-downloaded suffix.
- Slow-connection probe: a range-GET of the first 4 MB of
  ``submissions.zip`` measures effective Mbps. If below the
  configured threshold (default 13 Mbps), ``download_bulk_archives``
  returns a sentinel telling the caller to skip A3 and fall back to
  the legacy per-CIK path.
- Disk pre-flight: ``shutil.disk_usage`` rejects bootstrap with a
  clear error if free space is below the configured floor (default
  25 GB) — covers ~5.7 GB downloads + ~10 GB unzipped + parsed peak.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import time
import zipfile
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import httpx

logger = logging.getLogger(__name__)


SEC_BASE_URL: Final[str] = "https://www.sec.gov"


# Disk + bandwidth budgets. Configurable by env in callers.
DEFAULT_MIN_FREE_BYTES: Final[int] = 25 * 1024**3  # 25 GB
DEFAULT_BANDWIDTH_THRESHOLD_MBPS: Final[float] = 13.0
PROBE_BYTES: Final[int] = 4 * 1024 * 1024  # 4 MB range-GET probe
DEFAULT_CONCURRENCY: Final[int] = 4  # one TCP connection per archive family
DEFAULT_TIMEOUT_S: Final[float] = 600.0  # multi-GB transfers can take many minutes


@dataclass(frozen=True)
class BulkArchive:
    """One archive to download.

    ``url`` is fully qualified. ``expected_min_bytes`` is the floor
    used to reject a corrupted / truncated transfer — set ~20% below
    the observed Content-Length so SEC's normal week-on-week archive
    growth does not trip it.
    """

    name: str
    url: str
    expected_min_bytes: int


@dataclass
class ArchiveDownloadResult:
    """Per-archive outcome reported back to the orchestrator."""

    name: str
    path: Path | None
    bytes_downloaded: int
    skipped: bool = False
    error: str | None = None


@dataclass
class BulkDownloadResult:
    """Outcome of an entire bulk-download phase.

    ``mode`` is ``"bulk"`` if archives were downloaded, ``"fallback"``
    if the slow-connection probe routed to the legacy per-CIK path,
    ``"skipped_disk"`` if disk pre-flight refused.
    """

    mode: str
    measured_mbps: float | None
    archives: list[ArchiveDownloadResult] = field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------


def _quarter_label(d: date) -> str:
    """Return ``YYYYqN`` for the quarter containing ``d``."""
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}q{quarter}"


def _quarter_start(d: date) -> date:
    """Return the first day of the quarter containing ``d``."""
    quarter = (d.month - 1) // 3 + 1
    return date(d.year, (quarter - 1) * 3 + 1, 1)


def _previous_quarter_start(d: date) -> date:
    """Return the first day of the quarter immediately preceding ``d``'s quarter."""
    qs = _quarter_start(d)
    if qs.month == 1:
        return date(qs.year - 1, 10, 1)
    return date(qs.year, qs.month - 3, 1)


def last_n_quarters(n: int, *, today: date | None = None) -> list[str]:
    """Return labels of the last ``n`` completed quarters in newest-first order.

    Excludes the in-progress quarter — SEC publishes datasets after
    quarter end, so the current quarter is never available.
    """
    today = today or date.today()
    # Most-recent COMPLETED quarter is the one before today's quarter.
    cursor = _previous_quarter_start(today)
    out: list[str] = []
    for _ in range(n):
        out.append(_quarter_label(cursor))
        cursor = _previous_quarter_start(cursor)
    return out


_FORM13F_START_MONTHS: Final[tuple[int, ...]] = (3, 6, 9, 12)
"""Form 13F rolling-3-month windows start on the 1st of these months.

Verified against SEC's Form 13F Data Sets index page on 2026-05-08:
Mar–May, Jun–Aug, Sep–Nov, Dec–Feb. The Dec window straddles a
year boundary (Dec YYYY → Feb YYYY+1).
"""


def _form13f_window_for(start_year: int, start_month: int) -> tuple[date, date]:
    """Return (start_date, end_date) for a Form 13F window starting on
    the 1st of ``start_month`` of ``start_year``."""
    start = date(start_year, start_month, 1)
    end_month = start_month + 2
    end_year = start_year
    if end_month > 12:
        end_month -= 12
        end_year += 1
    next_first = date(end_year + 1, 1, 1) if end_month == 12 else date(end_year, end_month + 1, 1)
    end = next_first - timedelta(days=1)
    return start, end


def last_n_13f_periods(n: int, *, today: date | None = None) -> list[str]:
    """Return rolling-3-month period filenames for Form 13F datasets.

    SEC switched to rolling-3-month windows in 2024Q1; the windows
    start on the 1st of March, June, September, and December (NOT
    calendar quarter starts). Most-recent published file as of
    2026-05-08: ``01dec2025-28feb2026_form13f.zip``.

    Returns the most-recent ``n`` COMPLETED periods in newest-first
    order. A period is "completed" once its end-date is strictly
    before ``today``.
    """
    today = today or date.today()
    months = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")

    # Walk back through candidate windows newest-first until we have ``n``.
    out: list[str] = []
    year = today.year
    # Order: 12 (Dec), 9, 6, 3 — newest start within a year is December.
    candidate_starts_descending = [12, 9, 6, 3]
    while len(out) < n:
        for start_month in candidate_starts_descending:
            start_d, end_d = _form13f_window_for(year, start_month)
            if end_d >= today:
                continue  # Window not completed yet.
            label = (
                f"{start_d.day:02d}{months[start_d.month - 1]}{start_d.year}"
                f"-{end_d.day:02d}{months[end_d.month - 1]}{end_d.year}"
            )
            out.append(label)
            if len(out) >= n:
                break
        year -= 1
    return out


def build_bulk_archive_inventory(
    *,
    n_quarters_13f: int = 4,
    n_quarters_insider: int = 8,
    n_quarters_nport: int = 4,
    today: date | None = None,
) -> list[BulkArchive]:
    """Return the full inventory of archives Phase A3 downloads."""
    archives: list[BulkArchive] = [
        BulkArchive(
            name="submissions.zip",
            url=f"{SEC_BASE_URL}/Archives/edgar/daily-index/bulkdata/submissions.zip",
            # 1.2 GB floor; observed 1.54 GB on 2026-05-08.
            expected_min_bytes=int(1.2 * 1024**3),
        ),
        BulkArchive(
            name="companyfacts.zip",
            url=f"{SEC_BASE_URL}/Archives/edgar/daily-index/xbrl/companyfacts.zip",
            # 1.0 GB floor; observed 1.38 GB on 2026-05-08.
            expected_min_bytes=int(1.0 * 1024**3),
        ),
    ]
    for label in last_n_13f_periods(n_quarters_13f, today=today):
        archives.append(
            BulkArchive(
                name=f"form13f_{label}.zip",
                url=f"{SEC_BASE_URL}/files/structureddata/data/form-13f-data-sets/{label}_form13f.zip",
                expected_min_bytes=50 * 1024**2,
            )
        )
    for q in last_n_quarters(n_quarters_insider, today=today):
        archives.append(
            BulkArchive(
                name=f"insider_{q}.zip",
                url=f"{SEC_BASE_URL}/files/structureddata/data/insider-transactions-data-sets/{q}_form345.zip",
                expected_min_bytes=8 * 1024**2,
            )
        )
    for q in last_n_quarters(n_quarters_nport, today=today):
        archives.append(
            BulkArchive(
                name=f"nport_{q}.zip",
                url=f"{SEC_BASE_URL}/files/dera/data/form-n-port-data-sets/{q}_nport.zip",
                expected_min_bytes=300 * 1024**2,
            )
        )
    return archives


# ---------------------------------------------------------------------------
# Disk pre-flight + bandwidth probe
# ---------------------------------------------------------------------------


def _preflight_cleanup_stale_partials(target_dir: Path) -> None:
    """Delete any leftover ``*.partial`` files from a previous interrupted run.

    Disk-hygiene policy (#1020 follow-up): completed ``.zip`` files
    are preserved so an interrupted bootstrap can resume Phase C
    without re-downloading. Stale ``.partial`` files however are
    never useful — the resume path on a complete-download retry
    issues a fresh HTTP Range request and would overwrite any
    partial. Deleting them up-front frees disk + removes ambiguity
    if a previous transfer aborted at an unknown byte count.
    """
    if not target_dir.exists():
        return
    for path in target_dir.iterdir():
        if path.is_file() and path.name.endswith(".partial"):
            try:
                path.unlink()
                logger.info("preflight cleanup: removed stale partial %s", path)
            except OSError as exc:
                logger.warning("preflight cleanup: failed to remove %s: %s", path, exc)


def check_disk_space(target_dir: Path, *, min_free_bytes: int = DEFAULT_MIN_FREE_BYTES) -> tuple[bool, int]:
    """Return ``(has_enough, free_bytes)`` for ``target_dir``.

    ``target_dir`` is created if missing. The check is against the
    PARENT mount when possible, but ``shutil.disk_usage`` resolves
    via the path itself, which is what we want.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(target_dir)
    return usage.free >= min_free_bytes, usage.free


async def measure_bandwidth_mbps(
    client: httpx.AsyncClient,
    *,
    probe_url: str,
    probe_bytes: int = PROBE_BYTES,
) -> float:
    """Range-GET the first ``probe_bytes`` of ``probe_url`` and return
    measured Mbps.

    A 4 MB probe amortises TCP slow-start enough to give a stable
    bandwidth estimate on typical broadband links. Smaller windows
    (e.g. 1 MB) read significantly slower than steady-state; larger
    windows are more accurate but slow down the probe itself.
    """
    headers = {"Range": f"bytes=0-{probe_bytes - 1}"}
    started = time.monotonic()
    response = await client.get(probe_url, headers=headers)
    elapsed = time.monotonic() - started
    if response.status_code not in (200, 206):
        raise RuntimeError(f"bandwidth probe failed: status={response.status_code} url={probe_url}")
    bytes_read = len(response.content)
    if elapsed <= 0:
        return float("inf")
    bits_per_second = (bytes_read * 8) / elapsed
    return bits_per_second / 1_000_000


# ---------------------------------------------------------------------------
# Per-archive download
# ---------------------------------------------------------------------------


def _zip_round_trip(path: Path) -> bool:
    """Return True if ``path`` is a readable ZIP file.

    Two distinct failures matter:
      - ``zipfile.BadZipFile`` if the bytes are not a valid ZIP.
      - ``OSError`` if the file disappears or is unreadable mid-check.
    Both → corrupted/incomplete archive → return False so the caller
    discards and re-downloads.

    Note: the ``as exc`` clause is deliberate — without it ``ruff format``
    on Python 3.14 strips the tuple parens (PEP 758 except-without-parens
    is the new default), and Codex / older Python parsers reject the bare
    form. Binding the exception keeps the syntax stable across tools.
    """
    try:
        with zipfile.ZipFile(path) as zf:
            zf.namelist()
        return True
    except (zipfile.BadZipFile, OSError) as exc:
        logger.debug("zip round-trip failed for %s: %s", path, exc)
        return False


async def _head_size(client: httpx.AsyncClient, url: str) -> int:
    """Return Content-Length of ``url`` via HEAD."""
    response = await client.head(url)
    if response.status_code != 200:
        raise RuntimeError(f"HEAD failed: status={response.status_code} url={url}")
    length = response.headers.get("content-length")
    if length is None:
        raise RuntimeError(f"HEAD missing Content-Length: url={url}")
    return int(length)


async def _download_one(
    client: httpx.AsyncClient,
    archive: BulkArchive,
    target_dir: Path,
    *,
    chunk_size: int = 1024 * 1024,
) -> ArchiveDownloadResult:
    """Download one archive with atomic write + resume-from-partial."""
    final_path = target_dir / archive.name
    partial_path = final_path.with_suffix(final_path.suffix + ".partial")

    if final_path.exists() and _zip_round_trip(final_path):
        # Already-good archive on disk; treat as skip.
        return ArchiveDownloadResult(
            name=archive.name,
            path=final_path,
            bytes_downloaded=0,
            skipped=True,
        )

    try:
        expected_total = await _head_size(client, archive.url)
    except Exception as exc:  # noqa: BLE001 — operator-visible message
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=0,
            error=f"HEAD failed: {exc}",
        )

    if expected_total < archive.expected_min_bytes:
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=0,
            error=(
                f"Content-Length {expected_total} below floor {archive.expected_min_bytes} — archive likely truncated"
            ),
        )

    # Resume from partial if present and shorter than expected.
    headers: dict[str, str] = {}
    resume_from = 0
    if partial_path.exists():
        existing = partial_path.stat().st_size
        if existing >= expected_total:
            partial_path.unlink()  # Bigger than expected — discard.
        else:
            resume_from = existing
            headers["Range"] = f"bytes={existing}-"

    try:
        async with client.stream("GET", archive.url, headers=headers) as response:
            if resume_from and response.status_code != 206:
                # Server ignored Range; restart from zero.
                await response.aclose()
                resume_from = 0
                if partial_path.exists():
                    partial_path.unlink()
                async with client.stream("GET", archive.url) as fresh:
                    if fresh.status_code != 200:
                        return ArchiveDownloadResult(
                            name=archive.name,
                            path=None,
                            bytes_downloaded=0,
                            error=f"GET failed: status={fresh.status_code}",
                        )
                    await _stream_to_partial(fresh, partial_path, mode="wb", chunk_size=chunk_size)
            elif response.status_code in (200, 206):
                mode = "ab" if resume_from else "wb"
                await _stream_to_partial(response, partial_path, mode=mode, chunk_size=chunk_size)
            else:
                return ArchiveDownloadResult(
                    name=archive.name,
                    path=None,
                    bytes_downloaded=0,
                    error=f"GET failed: status={response.status_code}",
                )
    except (httpx.HTTPError, OSError) as exc:
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=0,
            error=f"transfer failed: {exc}",
        )

    final_size = partial_path.stat().st_size
    if final_size != expected_total:
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=final_size,
            error=f"size mismatch: got {final_size} bytes, expected {expected_total}",
        )

    if not _zip_round_trip(partial_path):
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=final_size,
            error="ZIP round-trip failed — archive corrupted",
        )

    partial_path.replace(final_path)
    return ArchiveDownloadResult(
        name=archive.name,
        path=final_path,
        bytes_downloaded=final_size - resume_from,
    )


async def _stream_to_partial(
    response: httpx.Response,
    partial_path: Path,
    *,
    mode: str,
    chunk_size: int,
) -> int:
    """Stream ``response`` body into ``partial_path``. Returns bytes written."""
    bytes_written = 0
    # ``open`` is sync; for our chunk sizes (1 MB) the GIL release on
    # ``write`` is sufficient. Avoiding aiofiles keeps the dep budget.
    with partial_path.open(mode) as fh:
        async for chunk in response.aiter_bytes(chunk_size=chunk_size):
            fh.write(chunk)
            bytes_written += len(chunk)
    return bytes_written


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _make_client(user_agent: str) -> AsyncIterator[httpx.AsyncClient]:
    """Yield an ``httpx.AsyncClient`` configured for SEC bulk archives."""
    limits = httpx.Limits(max_connections=DEFAULT_CONCURRENCY, max_keepalive_connections=DEFAULT_CONCURRENCY)
    async with httpx.AsyncClient(
        headers={"User-Agent": user_agent, "Accept": "application/zip,*/*"},
        timeout=DEFAULT_TIMEOUT_S,
        limits=limits,
        follow_redirects=True,
    ) as client:
        yield client


async def download_bulk_archives(
    *,
    target_dir: Path,
    user_agent: str,
    bandwidth_threshold_mbps: float = DEFAULT_BANDWIDTH_THRESHOLD_MBPS,
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    archives: Sequence[BulkArchive] | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> BulkDownloadResult:
    """Download every archive in the inventory.

    Sequence:
      1. Disk pre-flight.
      2. Bandwidth probe — if below threshold, return ``mode="fallback"``.
      3. Parallel per-archive download with ``concurrency`` bound.

    Returns a ``BulkDownloadResult`` the orchestrator inspects to
    decide whether to run Phase C (bulk) or fall back to legacy
    per-CIK ingest. Per-archive errors do NOT raise — they are
    recorded on the result and surfaced in the admin UI.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    _preflight_cleanup_stale_partials(target_dir)
    has_space, free_bytes = check_disk_space(target_dir, min_free_bytes=min_free_bytes)
    if not has_space:
        return BulkDownloadResult(
            mode="skipped_disk",
            measured_mbps=None,
            error=(f"Insufficient free space at {target_dir}: {free_bytes} bytes free, {min_free_bytes} required"),
        )

    archives = archives if archives is not None else build_bulk_archive_inventory()
    if not archives:
        # Defensive guard for callers that pass an empty list (e.g. a
        # test that wants to exercise only the disk-preflight branch).
        # Without this the bandwidth probe below would IndexError —
        # PR review BLOCKING.
        return BulkDownloadResult(
            mode="bulk",
            measured_mbps=None,
            archives=[],
        )

    async with _make_client(user_agent) as client:
        # Bandwidth probe against the first archive (submissions.zip).
        probe_url = archives[0].url
        try:
            measured = await measure_bandwidth_mbps(client, probe_url=probe_url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("bandwidth probe failed: %s", exc)
            return BulkDownloadResult(
                mode="fallback",
                measured_mbps=None,
                error=f"bandwidth probe failed: {exc}",
            )

        logger.info("bulk-download bandwidth probe: %.1f Mbps", measured)
        if measured < bandwidth_threshold_mbps:
            return BulkDownloadResult(
                mode="fallback",
                measured_mbps=measured,
                error=(f"measured {measured:.1f} Mbps below threshold {bandwidth_threshold_mbps:.1f}"),
            )

        sem = asyncio.Semaphore(concurrency)

        async def _bounded(archive: BulkArchive) -> ArchiveDownloadResult:
            async with sem:
                return await _download_one(client, archive, target_dir)

        results = await asyncio.gather(*(_bounded(a) for a in archives))

    return BulkDownloadResult(
        mode="bulk",
        measured_mbps=measured,
        archives=list(results),
    )


# ---------------------------------------------------------------------------
# Job invoker — dispatched via ``_INVOKERS["sec_bulk_download"]``
# ---------------------------------------------------------------------------


JOB_SEC_BULK_DOWNLOAD: Final[str] = "sec_bulk_download"


def sec_bulk_download_job() -> None:
    """Zero-arg job invoker for the runtime registry."""
    from app.config import settings
    from app.security.master_key import resolve_data_dir

    target_dir = resolve_data_dir() / "sec" / "bulk"
    result = asyncio.run(
        download_bulk_archives(
            target_dir=target_dir,
            user_agent=settings.sec_user_agent,
        )
    )
    if result.mode == "bulk":
        ok = sum(1 for r in result.archives if r.error is None)
        failed = sum(1 for r in result.archives if r.error is not None)
        logger.info(
            "sec_bulk_download: mode=bulk mbps=%.1f archives_ok=%d archives_failed=%d",
            result.measured_mbps or 0.0,
            ok,
            failed,
        )
    elif result.mode == "fallback":
        logger.warning(
            "sec_bulk_download: mode=fallback mbps=%s reason=%s",
            result.measured_mbps,
            result.error,
        )
    else:
        logger.error("sec_bulk_download: mode=%s error=%s", result.mode, result.error)
