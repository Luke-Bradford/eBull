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
import hashlib
import logging
import os
import shutil
import time
import zipfile
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Final, Literal

import httpx

from app.providers.sec_throttle_metrics import incr_sec_429
from app.services.bootstrap_preconditions import BootstrapPhaseSkipped

logger = logging.getLogger(__name__)


SEC_BASE_URL: Final[str] = "https://www.sec.gov"


# Disk + bandwidth budgets. Configurable by env in callers.
DEFAULT_MIN_FREE_BYTES: Final[int] = 25 * 1024**3  # 25 GB
# Bandwidth threshold tuned for the SEC archive single-stream limit
# (typically 10-30 Mbps per TCP connection from sec.gov). Operator
# 2026-05-22: prior 13 Mbps default forced fallback on a healthy
# 100+ Mbps line because the 4 MB range-GET probe measured 9.5 Mbps —
# normal for a single SEC stream. 5 Mbps admits modest broadband while
# still excluding genuinely slow networks (sub-broadband, congested
# tether). The fallback path's cascade-skip of bulk-dependent stages
# (companyfacts, insider/13F/NPORT from dataset) loses half the data
# pipeline silently — overshooting the threshold has high cost.
DEFAULT_BANDWIDTH_THRESHOLD_MBPS: Final[float] = 5.0
PROBE_BYTES: Final[int] = 4 * 1024 * 1024  # 4 MB range-GET probe
DEFAULT_CONCURRENCY: Final[int] = 4  # one TCP connection per archive family
DEFAULT_TIMEOUT_S: Final[float] = 600.0  # multi-GB transfers can take many minutes


@dataclass(frozen=True)
class BulkArchive:
    """One archive to download. ``url`` is fully qualified.

    Content validation is performed at three points downstream — none
    of which depend on a hard-coded size floor:

      1. ``Content-Type`` HEAD check (must be application/zip-ish);
         catches SEC redirects to HTML error pages without any
         download.
      2. HEAD-Content-Length-vs-streamed-bytes match; catches
         network truncation mid-stream. This is the streaming
         protocol's correctness check, NOT content validation.
      3. Magic-byte check (PK\\x03\\x04 prefix) + ``zipfile.ZipFile``
         round-trip after the bytes land; catches non-ZIP content
         and corruption that survived 1+2.
    """

    name: str
    url: str
    optional: bool = False
    """Best-effort archive: a download/HEAD error does NOT fatal the stage.

    #1423 — set only on the newest Form 13F rolling window. That window
    is the quarter that just closed; SEC publishes the dataset weeks
    after the window ends, so the day after a boundary it 404s. Treating
    its absence as fatal blocks every db-lane stage (the bulk_archives_ready
    cascade). It is still HEAD-probed each run, so it self-heals the day
    SEC posts it. Every other archive stays required — a real outage on
    them still fails loudly.
    """


@dataclass
class ArchiveDownloadResult:
    """Per-archive outcome reported back to the orchestrator.

    ``reuse_reason`` is the value stamped into the per-run manifest:
    - ``"downloaded_in_run"``: the bytes were fetched in THIS run.
    - ``"etag_match_sha256_verified"``: a prior-run .zip was reused
      because SEC's HEAD ETag matched the local ``.zip.etag`` sidecar
      AND the local file's SHA-256 matched ``.zip.sha256``.
    Both reasons satisfy ``assert_archive_belongs_to_run`` provenance
    because the manifest is stamped fresh with the current
    ``bootstrap_run_id`` regardless of reuse path
    (settled-decisions.md "Bulk archive reuse keyed on SEC ETag +
    SHA-256").
    """

    name: str
    path: Path | None
    bytes_downloaded: int
    skipped: bool = False
    error: str | None = None
    reuse_reason: Literal["downloaded_in_run", "etag_match_sha256_verified"] | None = None
    optional: bool = False
    """Mirrors ``BulkArchive.optional`` — stamped after the download gather
    so the job's fatal-failure filter can tell an expected-missing newest
    13F window from a genuine archive failure (#1423)."""


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


def last_n_months(n: int, *, today: date | None = None) -> list[str]:
    """Return ``YYYY_MM`` labels of the last ``n`` completed months,
    newest-first (excludes the in-progress month — DERA publishes the
    FSNDS monthly weeks after month end)."""
    today = today or date.today()
    year, month = today.year, today.month
    out: list[str] = []
    for _ in range(n):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        out.append(f"{year}_{month:02d}")
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
    n_quarters_fsds: int = 4,
    n_months_fsnds: int = 12,
    today: date | None = None,
) -> list[BulkArchive]:
    """Return the full inventory of archives Phase A3 downloads."""
    archives: list[BulkArchive] = [
        BulkArchive(
            name="submissions.zip",
            url=f"{SEC_BASE_URL}/Archives/edgar/daily-index/bulkdata/submissions.zip",
        ),
        BulkArchive(
            name="companyfacts.zip",
            url=f"{SEC_BASE_URL}/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        ),
    ]
    # ``last_n_13f_periods`` returns newest-first; index 0 is the just-closed
    # window. SEC publishes 13F rolling-window datasets weeks after the window
    # ends, so the newest one is expected-404 immediately after a boundary —
    # mark it optional (#1423) so its absence doesn't fatal the stage.
    for idx, label in enumerate(last_n_13f_periods(n_quarters_13f, today=today)):
        archives.append(
            BulkArchive(
                name=f"form13f_{label}.zip",
                url=f"{SEC_BASE_URL}/files/structureddata/data/form-13f-data-sets/{label}_form13f.zip",
                optional=(idx == 0),
            )
        )
    for q in last_n_quarters(n_quarters_insider, today=today):
        archives.append(
            BulkArchive(
                name=f"insider_{q}.zip",
                url=f"{SEC_BASE_URL}/files/structureddata/data/insider-transactions-data-sets/{q}_form345.zip",
            )
        )
    for q in last_n_quarters(n_quarters_nport, today=today):
        archives.append(
            BulkArchive(
                name=f"nport_{q}.zip",
                url=f"{SEC_BASE_URL}/files/dera/data/form-n-port-data-sets/{q}_nport.zip",
            )
        )
    # DERA Financial Statement Data Sets — the per-class shares-outstanding
    # denominator source (#788). num.txt carries the dimensional
    # ``ClassOfStock=<member>`` segments the companyfacts JSON API strips. The
    # newest quarter is published weeks after the quarter closes, so mark it
    # optional (idx 0) — its absence the day after a boundary must not fatal the
    # db-lane stage (mirrors the 13F rolling-window posture, #1423).
    for idx, q in enumerate(last_n_quarters(n_quarters_fsds, today=today)):
        archives.append(
            BulkArchive(
                name=f"fsds_{q}.zip",
                url=f"{SEC_BASE_URL}/files/dera/data/financial-statement-data-sets/{q}.zip",
                optional=(idx == 0),
            )
        )
    # DERA Financial Statement AND NOTES Data Sets (#844) — the ONLY
    # pipeline-reachable source for note-level facts (unvested RSU/PSU
    # counts): companyfacts strips dimensional facts, plain FSDS is
    # face-statements-only. Monthly; published weeks after month close, so
    # the newest is optional (mirrors the FSDS #1423 posture). NOTE the
    # path segment differs from FSDS: financial-statement-notes-data-sets.
    for idx, m in enumerate(last_n_months(n_months_fsnds, today=today)):
        archives.append(
            BulkArchive(
                name=f"fsnds_{m}_notes.zip",
                url=f"{SEC_BASE_URL}/files/dera/data/financial-statement-notes-data-sets/{m}_notes.zip",
                optional=(idx == 0),
            )
        )
    # DERA serves only ~12 months as monthlies, then consolidates into
    # QUARTERLY {y}q{n}_notes.zip (page-verified 2026-07-23: monthlies
    # 2025_07→2026_06, quarterlies 2025q2 and older). Quarters 5-8 back
    # cover the note-freshness window (548d on period_end + filing lag)
    # for a fresh install — without them a 13-18-month-old 10-K note is
    # policy-fresh but never ingested (codex ckpt-2 finding). The newest
    # of the four sits on the consolidation boundary (may still be
    # monthly-only) → optional; its months are then covered by the
    # monthly set above.
    for idx, q in enumerate(last_n_quarters(8, today=today)[4:8]):
        archives.append(
            BulkArchive(
                name=f"fsnds_{q}_notes.zip",
                url=f"{SEC_BASE_URL}/files/dera/data/financial-statement-notes-data-sets/{q}_notes.zip",
                optional=(idx == 0),
            )
        )
    return archives


# ---------------------------------------------------------------------------
# Disk pre-flight + bandwidth probe
# ---------------------------------------------------------------------------


def _validate_archive_name(name: str) -> str:
    """Return ``name`` unchanged after defending against path-traversal.

    ``archive.name`` is joined into ``target_dir`` in three layers
    (purge / preflight / download) to derive concrete paths for the
    archive, its sidecars, and the resume-partial. A crafted name
    containing path separators or ``..`` would escape ``target_dir``
    and could delete or overwrite arbitrary filesystem state. Today
    every name comes from ``build_bulk_archive_inventory`` which we
    control, but defense-in-depth: validate at the boundary so a
    future caller-supplied inventory cannot bypass the check.

    Rules:
      - Must equal its basename (no separators).
      - Must not be empty, ``.``, ``..``, or absolute.
      - Must not contain backslashes or NUL.
      - Codex 2 BLOCKING for PR-5b.
    """
    if not name:
        raise ValueError("archive name must be non-empty")
    if name in (".", ".."):
        raise ValueError(f"archive name must not be {name!r}")
    if "/" in name or "\\" in name or "\x00" in name:
        raise ValueError(f"archive name must not contain path separators or NUL: {name!r}")
    if Path(name).is_absolute():
        raise ValueError(f"archive name must be relative: {name!r}")
    # Posix-form basename equality also catches drive letters on Win
    # (``C:foo``) which would otherwise sneak past the separator check.
    if Path(name).name != name:
        raise ValueError(f"archive name must equal its basename: {name!r}")
    return name


def _resolve_archive_path(target_dir: Path, archive_name: str) -> Path:
    """Return ``target_dir / archive_name`` after validation."""
    return target_dir / _validate_archive_name(archive_name)


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return hex SHA-256 of ``path``. Streams in 1 MB chunks (multi-GB safe)."""
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


_SIDECAR_TMP_TAG: Final[str] = f".tmp.{os.getpid()}"


def _atomic_write_sidecar(target_path: Path, value: str) -> None:
    """Write ``value`` to ``target_path`` atomically (tmp + rename).

    Crash-mid-write must not leave a half-formed sidecar that the next
    run accepts as authoritative. Sidecar contents are pure text (hex
    digest or ETag string) so encoding is fixed UTF-8.

    The tmp suffix incorporates the writer's PID so two concurrent
    processes targeting the same archive (defensive — bootstrap is
    singleton-gated, but a future operator override could fire two
    sec_bulk_download runs side-by-side) don't clobber each other's
    half-written sidecar before the final ``rename``. Codex 2 MEDIUM
    for PR-5b.
    """
    tmp_path = target_path.with_suffix(target_path.suffix + _SIDECAR_TMP_TAG)
    tmp_path.write_text(value, encoding="utf-8")
    tmp_path.replace(target_path)


def _read_sidecar(path: Path) -> str | None:
    """Return stripped sidecar contents, or None if missing/unreadable."""
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.warning("sidecar read failed for %s: %s", path, exc)
        return None


def _purge_archive_artifacts(target_dir: Path, archive_name: str) -> None:
    """Delete the .zip, .partial, .sha256 + .etag sidecars for ``archive_name``.

    Used by the ETag-keyed pre-flight when a re-download is required so
    no stale local state can leak into the next attempt. Also used as
    the broad reset path when ``BOOTSTRAP_FORCE_REDOWNLOAD=1``.
    """
    base = _resolve_archive_path(target_dir, archive_name)
    candidates = (
        base,
        base.with_suffix(base.suffix + ".partial"),
        Path(str(base) + ".sha256"),
        Path(str(base) + ".etag"),
    )
    for path in candidates:
        if not path.exists():
            continue
        try:
            path.unlink()
            logger.info("preflight purge: removed %s", path)
        except OSError as exc:
            logger.warning("preflight purge: failed to remove %s: %s", path, exc)


_FORCE_REDOWNLOAD_ENV: Final[str] = "BOOTSTRAP_FORCE_REDOWNLOAD"


def _force_redownload_active() -> bool:
    """Return True when the operator override env var is truthy."""
    raw = os.environ.get(_FORCE_REDOWNLOAD_ENV, "").strip().lower()
    return raw in ("1", "true", "yes", "on")


async def _head_etag(
    client: httpx.AsyncClient,
    url: str,
    *,
    rate_limiter: Any | None = None,
) -> str | None:
    """Return the ``ETag`` header from a HEAD against ``url``, or None.

    Used by the pre-flight reuse path. Failure modes are tolerated by
    the caller (no-reuse → re-download) — this helper never raises for
    a missing or non-200 ETag, only for outright transport faults that
    the caller must escalate.
    """
    if rate_limiter is not None:
        await rate_limiter.acquire()
    response = await client.head(url)
    if response.status_code == 429:
        incr_sec_429()
    if response.status_code != 200:
        logger.info(
            "preflight HEAD non-200 for %s: status=%d — will re-download",
            url,
            response.status_code,
        )
        return None
    etag = response.headers.get("etag") or response.headers.get("ETag")
    if etag is None:
        # Per spec: SEC sometimes serves static files without an ETag.
        # No sidecar comparison possible → caller must re-download.
        return None
    return etag


@dataclass(frozen=True)
class _ArchiveReuseDecision:
    """Outcome of the pre-flight reuse check for one archive."""

    name: str
    reused: bool
    sec_etag: str | None  # remote ETag at preflight time, if HEAD succeeded
    reason: str  # human-readable reason logged to operator


async def _preflight_archive_reuse_decision(
    client: httpx.AsyncClient,
    archive: BulkArchive,
    target_dir: Path,
    *,
    rate_limiter: Any | None = None,
) -> _ArchiveReuseDecision:
    """Decide whether ``archive`` can be reused from local disk.

    Reuse criteria (ALL must hold):
      1. Local ``.zip`` exists.
      2. ``.zip.sha256`` sidecar exists AND matches a fresh SHA-256
         of the local ``.zip``.
      3. ``.zip.etag`` sidecar exists AND matches SEC's HEAD ``ETag``.

    Operator override ``BOOTSTRAP_FORCE_REDOWNLOAD=1`` is handled at a
    higher layer (the caller short-circuits before invoking this
    helper) so the per-archive decision flow stays focused on
    sidecar/HEAD comparison.

    On any negative decision the caller is expected to purge the
    archive's artefacts (.zip + .partial + sidecars) before retry.
    """
    zip_path = _resolve_archive_path(target_dir, archive.name)
    sha_path = Path(str(zip_path) + ".sha256")
    etag_path = Path(str(zip_path) + ".etag")
    partial_path = zip_path.with_suffix(zip_path.suffix + ".partial")

    # A leftover .partial alongside a complete .zip indicates a prior
    # interrupted resume; safe path is full re-download.
    if partial_path.exists():
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=None,
            reason="partial_present",
        )

    if not zip_path.exists():
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=None,
            reason="local_missing",
        )

    stored_sha = _read_sidecar(sha_path)
    if stored_sha is None:
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=None,
            reason="sha256_sidecar_missing",
        )

    stored_etag = _read_sidecar(etag_path)
    if stored_etag is None:
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=None,
            reason="etag_sidecar_missing",
        )

    # HEAD SEC for the live ETag. A non-200 / missing ETag means we
    # cannot prove freshness; safe default is re-download.
    try:
        sec_etag = await _head_etag(client, archive.url, rate_limiter=rate_limiter)
    except (httpx.HTTPError, OSError) as exc:
        logger.warning("preflight HEAD failed for %s: %s — will re-download", archive.url, exc)
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=None,
            reason=f"head_failed: {type(exc).__name__}",
        )

    if sec_etag is None:
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=None,
            reason="sec_etag_missing",
        )

    if sec_etag != stored_etag:
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=sec_etag,
            reason="etag_mismatch",
        )

    # ETag matches → verify the local bytes actually still match the
    # SHA-256 we stamped at download time (defends against on-disk
    # corruption or a tampered sidecar).
    try:
        actual_sha = _sha256_file(zip_path)
    except OSError as exc:
        logger.warning("preflight SHA-256 failed for %s: %s — will re-download", zip_path, exc)
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=sec_etag,
            reason=f"sha256_read_failed: {type(exc).__name__}",
        )

    if actual_sha != stored_sha:
        return _ArchiveReuseDecision(
            name=archive.name,
            reused=False,
            sec_etag=sec_etag,
            reason="sha256_mismatch",
        )

    return _ArchiveReuseDecision(
        name=archive.name,
        reused=True,
        sec_etag=sec_etag,
        reason="etag_match_sha256_verified",
    )


async def _preflight_etag_keyed_reuse(
    client: httpx.AsyncClient,
    archives: Sequence[BulkArchive],
    target_dir: Path,
    *,
    rate_limiter: Any | None = None,
) -> dict[str, _ArchiveReuseDecision]:
    """For each archive, decide reuse-or-redownload and clean up
    non-reusable local state.

    Replaces the older blanket ``_preflight_cleanup_stale_partials``
    that nuked every prior-run ``.zip`` regardless of freshness. With
    sidecar evidence + SEC's stable S3-backed ETag, an unchanged
    archive can be reused safely (settled-decisions.md, "Bulk archive
    reuse keyed on SEC ETag + SHA-256").

    The stale ``.run_manifest.json`` is always deleted up front — the
    next run will stamp a fresh manifest with the current
    ``bootstrap_run_id`` regardless of which archives were reused vs
    downloaded. This preserves the provenance contract that
    ``assert_archive_belongs_to_run`` enforces (#1020).

    Operator override: ``BOOTSTRAP_FORCE_REDOWNLOAD=1`` bypasses reuse
    for every archive.
    """
    decisions: dict[str, _ArchiveReuseDecision] = {}
    if not target_dir.exists():
        return decisions

    # Always wipe the prior manifest so we never leak a stale run_id
    # forward; the new manifest is written by write_run_manifest().
    manifest_path = target_dir / RUN_MANIFEST_NAME
    if manifest_path.exists():
        try:
            manifest_path.unlink()
            logger.info("preflight: removed stale run manifest %s", manifest_path)
        except OSError as exc:
            logger.warning("preflight: failed to remove stale manifest %s: %s", manifest_path, exc)

    force = _force_redownload_active()
    if force:
        logger.warning(
            "preflight: %s=1 — purging all archives + sidecars (forced redownload)",
            _FORCE_REDOWNLOAD_ENV,
        )

    expected_names: set[str] = set()
    for archive in archives:
        expected_names.add(archive.name)
        if force:
            decision = _ArchiveReuseDecision(
                name=archive.name,
                reused=False,
                sec_etag=None,
                reason="force_redownload_env",
            )
        else:
            decision = await _preflight_archive_reuse_decision(client, archive, target_dir, rate_limiter=rate_limiter)
        decisions[archive.name] = decision
        if not decision.reused:
            _purge_archive_artifacts(target_dir, archive.name)
            logger.info(
                "preflight: %s will re-download (reason=%s)",
                archive.name,
                decision.reason,
            )
        else:
            logger.info(
                "preflight: %s reused from disk (etag=%s)",
                archive.name,
                decision.sec_etag,
            )

    # Stray archives not in the current inventory should still be
    # cleaned (e.g. an old 13F window dropped off the rolling list).
    for path in target_dir.iterdir():
        if not path.is_file():
            continue
        name = path.name
        if not (name.endswith(".zip") or name.endswith(".partial")):
            continue
        # Strip ``.partial`` to get the canonical archive name.
        base_name = name[: -len(".partial")] if name.endswith(".partial") else name
        if base_name in expected_names:
            continue
        # Also clean the matching sidecars if any.
        _purge_archive_artifacts(target_dir, base_name)

    return decisions


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
    rate_limiter: Any | None = None,
) -> float:
    """Range-GET the first ``probe_bytes`` of ``probe_url`` and return
    measured Mbps.

    A 4 MB probe amortises TCP slow-start enough to give a stable
    bandwidth estimate on typical broadband links. Smaller windows
    (e.g. 1 MB) read significantly slower than steady-state; larger
    windows are more accurate but slow down the probe itself.

    ``rate_limiter`` (optional) acquires the shared SEC rate clock
    before the probe so the GET counts against the per-IP budget
    shared with sec_edgar / pipelined fetcher (#1042).
    """
    if rate_limiter is not None:
        await rate_limiter.acquire()
    headers = {"Range": f"bytes=0-{probe_bytes - 1}"}
    started = time.monotonic()
    response = await client.get(probe_url, headers=headers)
    elapsed = time.monotonic() - started
    if response.status_code == 429:
        incr_sec_429()
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


async def _head_size_and_type(
    client: httpx.AsyncClient,
    url: str,
    *,
    rate_limiter: Any | None = None,
) -> tuple[int, str, str | None]:
    """Return ``(Content-Length, Content-Type, ETag-or-None)`` for ``url``.

    ``rate_limiter`` acquires the shared SEC rate clock before the
    HEAD so it counts against the per-IP budget (#1042).

    Content-Type is the real integrity signal — SEC serves bulk
    archives as ``application/zip`` (or ``application/x-zip-compressed``);
    an HTML error page would be ``text/html``. Returning the type
    lets the caller reject non-archive responses BEFORE downloading
    bytes, replacing the brittle ``expected_min_bytes`` floor that
    conflated "small file" with "corrupted file" (#1059).

    ETag is returned so the caller can bind a resume Range request to
    the specific resource version it HEAD'd — if SEC rebuilds the
    archive between HEAD and the resume GET, ``If-Range`` makes the
    server return a 200 (full body) instead of appending fresh bytes
    onto a stale ``.partial``. Codex 2 LOW/MEDIUM for PR-5b.
    """
    if rate_limiter is not None:
        await rate_limiter.acquire()
    response = await client.head(url)
    if response.status_code == 429:
        incr_sec_429()
    if response.status_code != 200:
        raise RuntimeError(f"HEAD failed: status={response.status_code} url={url}")
    length = response.headers.get("content-length")
    if length is None:
        raise RuntimeError(f"HEAD missing Content-Length: url={url}")
    content_type = (response.headers.get("content-type") or "").lower()
    etag = response.headers.get("etag") or response.headers.get("ETag")
    return int(length), content_type, etag


# Acceptable Content-Type values for SEC bulk archives. Real-world
# observation 2026-05-08 against www.sec.gov: archives served as
# ``application/zip``. SEC's API docs don't contractually pin the
# MIME header so we also accept common ZIP variants. An obvious-bad
# CT (e.g. ``text/html`` for an error page) is rejected pre-flight;
# anything else falls through to the post-download magic-byte +
# ZipFile round-trip checks. Codex pre-push for #1059.
_KNOWN_ARCHIVE_CONTENT_TYPES: tuple[str, ...] = (
    "application/zip",
    "application/x-zip",
    "application/x-zip-compressed",
    "application/octet-stream",
)
_OBVIOUS_BAD_CONTENT_TYPES: tuple[str, ...] = (
    "text/html",
    "text/plain",
    "application/json",
    "application/xml",
)


def _classify_content_type(content_type: str) -> Literal["known", "unknown", "bad"]:
    """Classify HEAD Content-Type for archive pre-flight.

    - 'known': matches a documented ZIP MIME → proceed.
    - 'bad':   matches a known error-page MIME → reject pre-flight.
    - 'unknown': anything else → proceed and let magic-byte +
      ZipFile post-checks decide.
    """
    if not content_type:
        return "unknown"
    head = content_type.split(";", 1)[0].strip().lower()
    if head in _KNOWN_ARCHIVE_CONTENT_TYPES:
        return "known"
    if head in _OBVIOUS_BAD_CONTENT_TYPES:
        return "bad"
    return "unknown"


# ZIP magic bytes for single-volume archives — the only kind SEC
# publishes today.
#   PK\\x03\\x04 = local-file-header signature (start of any
#                 non-empty single-volume archive).
#   PK\\x05\\x06 = end-of-central-directory signature (only header
#                 in an empty archive).
# Multi-disk / spanned archives (PK\\x06\\x06 / PK\\x06\\x07) are
# intentionally rejected — Python's ``zipfile`` would reject them
# downstream anyway, so accepting their magic at pre-check would
# mislead the operator. ``PK\\x07\\x08`` is the data-descriptor
# signature inside a stream-zip body, not a file header — also not
# a valid first-bytes signature.
_ZIP_MAGIC_BYTES: tuple[bytes, ...] = (b"PK\x03\x04", b"PK\x05\x06")


def _has_zip_magic(path: Path) -> bool:
    """Return True when the first 4 bytes of ``path`` match a ZIP signature."""
    try:
        with open(path, "rb") as fh:
            header = fh.read(4)
    except OSError:
        return False
    return any(header.startswith(magic) for magic in _ZIP_MAGIC_BYTES)


_TRANSIENT_HTTPX_ERRORS = (
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.NetworkError,
)


async def _download_one_with_retry(
    client: httpx.AsyncClient,
    archive: BulkArchive,
    target_dir: Path,
    *,
    chunk_size: int = 1024 * 1024,
    max_attempts: int = 3,
    backoff_base_s: float = 2.0,
    rate_limiter: Any | None = None,
) -> ArchiveDownloadResult:
    """Wrap ``_download_one`` with retry-on-transient-error.

    First live test: 3 of 18 archives failed mid-transfer (network
    flakes — N-PORT 463 MB × 4). PR1 marked the stage error and
    blocked Phase C, but a single TCP hiccup shouldn't condemn
    the whole bulk pipeline. Retry transient ``httpx`` errors with
    exponential backoff before declaring the archive failed.

    Resume-from-partial in ``_download_one`` means each retry picks
    up where the prior left off — no wasted bandwidth on the bytes
    already received.

    Codex sweep BLOCKING for #1020.
    """
    last_error: str | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = await _download_one(client, archive, target_dir, chunk_size=chunk_size, rate_limiter=rate_limiter)
        except _TRANSIENT_HTTPX_ERRORS as exc:
            last_error = f"transient error attempt {attempt}: {type(exc).__name__}: {exc}"
            logger.warning("download retry: %s — %s", archive.name, last_error)
        else:
            # On non-transient error in `result.error`, retry once
            # in case the server briefly returned 5xx; otherwise
            # the result is final.
            if result.error is None:
                return result
            # Detect transient-shaped error strings inside the result
            # (the underlying try/except in _download_one converted them
            # to ArchiveDownloadResult.error).
            err = result.error
            if any(
                tok in err
                for tok in (
                    "ConnectError",
                    "ReadTimeout",
                    "WriteTimeout",
                    "PoolTimeout",
                    "RemoteProtocolError",
                    "NetworkError",
                )
            ):
                last_error = f"transient (in result) attempt {attempt}: {err}"
                logger.warning("download retry: %s — %s", archive.name, last_error)
            else:
                # Non-transient (e.g. floor mismatch, ZIP corrupt).
                # Don't retry — the cause won't change.
                return result
        if attempt < max_attempts:
            wait_s = backoff_base_s * (2 ** (attempt - 1))
            logger.info("download retry: sleeping %.1fs before attempt %d", wait_s, attempt + 1)
            await asyncio.sleep(wait_s)
    return ArchiveDownloadResult(
        name=archive.name,
        path=None,
        bytes_downloaded=0,
        error=last_error or f"download failed after {max_attempts} attempts",
    )


async def _download_one(
    client: httpx.AsyncClient,
    archive: BulkArchive,
    target_dir: Path,
    *,
    chunk_size: int = 1024 * 1024,
    rate_limiter: Any | None = None,
) -> ArchiveDownloadResult:
    """Download one archive with atomic write + resume-from-partial.

    ``rate_limiter`` acquires the shared SEC rate clock before each
    HEAD/GET so the bulk downloader counts against the per-IP budget
    shared with sec_edgar / pipelined fetcher (#1042).
    """
    final_path = _resolve_archive_path(target_dir, archive.name)
    partial_path = final_path.with_suffix(final_path.suffix + ".partial")

    if final_path.exists() and _zip_round_trip(final_path):
        # Already-good archive on disk; treat as skip. Under the
        # ETag-keyed reuse model the pre-flight is expected to either
        # remove a stale .zip or short-circuit reuse before reaching
        # this branch, so this is a defensive fallback. Stamp the
        # in-run reuse_reason so manifest provenance still ties this
        # path to the current bootstrap_run_id.
        return ArchiveDownloadResult(
            name=archive.name,
            path=final_path,
            bytes_downloaded=0,
            skipped=True,
            reuse_reason="downloaded_in_run",
        )

    try:
        expected_total, content_type, head_etag = await _head_size_and_type(
            client, archive.url, rate_limiter=rate_limiter
        )
    except Exception as exc:  # noqa: BLE001 — operator-visible message
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=0,
            error=f"HEAD failed: {exc}",
        )

    # Pre-flight integrity check on the HEAD response. Three cases:
    # known-archive MIME → proceed; obvious-bad MIME (text/html etc)
    # → reject before downloading; anything else → proceed and let
    # the post-download magic-byte + ZipFile round-trip catch it.
    # #1059.
    ct_class = _classify_content_type(content_type)
    if ct_class == "bad":
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=0,
            error=(f"Content-Type {content_type!r} is not an archive — SEC may have served an error page"),
        )

    # Resume from partial if present and shorter than expected.
    # ``If-Range`` binds the resume to the exact resource version we
    # HEAD'd a few ms earlier; if SEC rebuilds the archive between HEAD
    # and the resume GET, the server returns 200 (full body) instead of
    # 206 — the existing 200-fallback path below already discards the
    # partial and restarts from zero. Codex 2 LOW/MEDIUM for PR-5b.
    headers: dict[str, str] = {}
    resume_from = 0
    if partial_path.exists():
        existing = partial_path.stat().st_size
        if existing >= expected_total:
            partial_path.unlink()  # Bigger than expected — discard.
        else:
            resume_from = existing
            headers["Range"] = f"bytes={existing}-"
            if head_etag is not None:
                headers["If-Range"] = head_etag

    response_etag: str | None = None
    try:
        if rate_limiter is not None:
            await rate_limiter.acquire()
        async with client.stream("GET", archive.url, headers=headers) as response:
            if response.status_code == 429:
                incr_sec_429()
            if resume_from and response.status_code != 206:
                # Server ignored Range; restart from zero.
                await response.aclose()
                resume_from = 0
                if partial_path.exists():
                    partial_path.unlink()
                if rate_limiter is not None:
                    await rate_limiter.acquire()
                async with client.stream("GET", archive.url) as fresh:
                    if fresh.status_code == 429:
                        incr_sec_429()
                    if fresh.status_code != 200:
                        return ArchiveDownloadResult(
                            name=archive.name,
                            path=None,
                            bytes_downloaded=0,
                            error=f"GET failed: status={fresh.status_code}",
                        )
                    response_etag = fresh.headers.get("etag") or fresh.headers.get("ETag")
                    await _stream_to_partial(fresh, partial_path, mode="wb", chunk_size=chunk_size)
            elif response.status_code in (200, 206):
                # Only capture ETag on a full GET (200). A 206 partial
                # response's ETag refers to the resumed slice's parent
                # which is fine to record on completion, but if the
                # server already produced an ETag on the initial GET
                # earlier we'd be stamping the same value. Either way,
                # ETag from the response covers the full resource for
                # SEC's S3-backed bulk archives.
                response_etag = response.headers.get("etag") or response.headers.get("ETag")
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

    # Magic-byte check before the more expensive ZIP round-trip.
    # ZIP files start with PK\\x03\\x04 (or one of the central-
    # directory variants); an HTML error page or random text would
    # fail this faster than zipfile.ZipFile(). #1059.
    if not _has_zip_magic(partial_path):
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=final_size,
            error="missing ZIP magic bytes — content is not a ZIP archive",
        )

    if not _zip_round_trip(partial_path):
        return ArchiveDownloadResult(
            name=archive.name,
            path=None,
            bytes_downloaded=final_size,
            error="ZIP round-trip failed — archive corrupted",
        )

    partial_path.replace(final_path)

    # ETag-keyed reuse depends on these sidecars being present after
    # every successful download. SHA-256 is computed from the
    # on-disk file (not streamed during download) so corruption that
    # somehow survives the size + magic + zip-round-trip checks is
    # still caught on the next pre-flight. Both writes are atomic
    # (tmp + rename); failures here are logged but do not fail the
    # download itself — the manifest still records success and the
    # next run will simply re-download for "sidecar_missing".
    try:
        sha256_hex = _sha256_file(final_path)
        _atomic_write_sidecar(Path(str(final_path) + ".sha256"), sha256_hex)
    except OSError as exc:
        logger.warning("sidecar write (sha256) failed for %s: %s", final_path, exc)
    if response_etag is not None:
        try:
            _atomic_write_sidecar(Path(str(final_path) + ".etag"), response_etag)
        except OSError as exc:
            logger.warning("sidecar write (etag) failed for %s: %s", final_path, exc)
    else:
        logger.info(
            "no ETag header on GET for %s — reuse pre-flight will force re-download next run",
            archive.name,
        )

    return ArchiveDownloadResult(
        name=archive.name,
        path=final_path,
        bytes_downloaded=final_size - resume_from,
        reuse_reason="downloaded_in_run",
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


RUN_MANIFEST_NAME: Final[str] = ".run_manifest.json"


def write_run_manifest(
    target_dir: Path,
    *,
    bootstrap_run_id: int,
    archives: Sequence[ArchiveDownloadResult],
    mode: Literal["bulk", "fallback"] = "bulk",
) -> None:
    """Persist a per-run archive manifest at ``<bulk>/.run_manifest.json``.

    Phase C preconditions read this to verify each archive landed in
    the CURRENT bootstrap run, not a previous one. Stale archives left
    on disk from a prior run will have a different ``bootstrap_run_id``
    and fail the provenance check (Codex review BLOCKING for #1020).

    ``mode='fallback'`` writes a stub manifest with no archives so
    Phase C preconditions can detect intentional bypass and mark the
    stage ``skipped`` instead of ``error``. See assert_archives_in_manifest
    + BootstrapPhaseSkipped in app/services/bootstrap_preconditions.py.
    """
    import json

    manifest = {
        "bootstrap_run_id": bootstrap_run_id,
        "mode": mode,
        "archives": [
            {
                "name": r.name,
                "bytes_downloaded": r.bytes_downloaded,
                "error": r.error,
                # Provenance: which path produced this archive in THIS
                # run. Default to "downloaded_in_run" if the result
                # predates the field (defensive — should not occur
                # after PR-5b but keeps the manifest schema honest).
                "reuse_reason": r.reuse_reason or "downloaded_in_run",
            }
            for r in archives
            if r.error is None and r.path is not None
        ],
    }
    # Atomic write: write to a sibling tempfile then rename. Without
    # this, a crash mid-write leaves a partial JSON that read_run_manifest
    # silently treats as "no manifest" → Phase C errors as "manifest
    # missing" instead of detecting fallback. Codex pre-push LOW for
    # #1041.
    final_path = target_dir / RUN_MANIFEST_NAME
    tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(manifest))
    tmp_path.replace(final_path)


def read_run_manifest(target_dir: Path) -> dict | None:
    """Return the manifest dict at ``<bulk>/.run_manifest.json`` or None."""
    import json

    path = target_dir / RUN_MANIFEST_NAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.debug("read_run_manifest failed: %s", exc)
        return None


_ACCEPTED_REUSE_REASONS: Final[frozenset[str]] = frozenset({"downloaded_in_run", "etag_match_sha256_verified"})


def assert_archive_belongs_to_run(
    target_dir: Path,
    archive_name: str,
    *,
    bootstrap_run_id: int,
) -> None:
    """Raise if the archive at ``<bulk>/<archive_name>`` is not in the
    current run's manifest.

    Provenance contract (#1020): every archive read by a Phase C
    stage must trace to a manifest stamped with the current
    ``bootstrap_run_id``. PR-5b widens the contract to accept BOTH
    ``reuse_reason='downloaded_in_run'`` and
    ``reuse_reason='etag_match_sha256_verified'`` — see
    settled-decisions.md "Bulk archive reuse keyed on SEC ETag +
    SHA-256". The manifest is rewritten every run regardless of
    reuse path, so a stale prior-run manifest still fails the
    ``bootstrap_run_id`` check below.
    """
    manifest = read_run_manifest(target_dir)
    if manifest is None:
        raise RuntimeError(
            f"PRECONDITION: bulk run manifest missing at {target_dir / RUN_MANIFEST_NAME}; "
            f"sec_bulk_download did not complete in the current run."
        )
    if int(manifest.get("bootstrap_run_id", -1)) != bootstrap_run_id:
        raise RuntimeError(
            f"PRECONDITION: bulk manifest run_id={manifest.get('bootstrap_run_id')!r} "
            f"!= current bootstrap_run_id={bootstrap_run_id}; archive is stale."
        )
    archive_entry: dict[str, Any] | None = None
    for entry in manifest.get("archives", []):
        if entry.get("name") == archive_name:
            archive_entry = entry
            break
    if archive_entry is None:
        raise RuntimeError(
            f"PRECONDITION: archive {archive_name!r} not in current-run manifest; sec_bulk_download did not land it."
        )
    # ``reuse_reason`` may be absent on manifests written by pre-PR-5b
    # code paths; treat absent as the legacy default.
    reuse_reason = archive_entry.get("reuse_reason") or "downloaded_in_run"
    if reuse_reason not in _ACCEPTED_REUSE_REASONS:
        raise RuntimeError(
            f"PRECONDITION: archive {archive_name!r} has unexpected reuse_reason={reuse_reason!r}; "
            f"accepted values are {sorted(_ACCEPTED_REUSE_REASONS)}."
        )


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

    # Acquire the cross-process gate so A3's HEAD/GET requests count
    # against the per-IP budget shared with sec_edgar and the pipelined
    # fetcher (#1042, #1484). no shared_clock -> _AsyncRateLimiter defaults
    # to the process-global gate.
    from app.services.sec_pipelined_fetcher import _AsyncRateLimiter

    rate_limiter = _AsyncRateLimiter(target_rps=7.0)

    async with _make_client(user_agent) as client:
        # ETag-keyed reuse pre-flight (PR-5b): for each archive,
        # decide whether the existing local .zip + sidecars prove
        # freshness against SEC's HEAD ETag. Non-reusable artefacts
        # are purged before bandwidth probe + download. The stale
        # run-manifest is always wiped (a fresh one is written
        # post-download).
        reuse_decisions = await _preflight_etag_keyed_reuse(client, archives, target_dir, rate_limiter=rate_limiter)

        # Bandwidth probe against the first archive (submissions.zip).
        # If every archive is reused, the probe is unnecessary (0 bytes
        # to fetch) but still cheap (4 MB range-GET) and confirms SEC
        # is reachable before we declare the run a no-op.
        probe_url = archives[0].url
        try:
            measured = await measure_bandwidth_mbps(client, probe_url=probe_url, rate_limiter=rate_limiter)
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
                # Use the retry wrapper so transient network blips
                # (Codex sweep BLOCKING) don't condemn an archive.
                return await _download_one_with_retry(client, archive, target_dir, rate_limiter=rate_limiter)

        async def _resolve(archive: BulkArchive) -> ArchiveDownloadResult:
            decision = reuse_decisions.get(archive.name)
            if decision is not None and decision.reused:
                # 0-byte reuse: surface as a successful skip so the
                # manifest writer records it with provenance
                # ``etag_match_sha256_verified``. No network IO.
                return ArchiveDownloadResult(
                    name=archive.name,
                    path=_resolve_archive_path(target_dir, archive.name),
                    bytes_downloaded=0,
                    skipped=True,
                    reuse_reason="etag_match_sha256_verified",
                )
            return await _bounded(archive)

        results = await asyncio.gather(*(_resolve(a) for a in archives))

    # Propagate the archive's optional flag onto its result so the job's
    # fatal-failure filter (#1423) can distinguish an expected-missing newest
    # 13F window from a genuine failure. ``_resolve`` preserves input order.
    for archive, result in zip(archives, results, strict=True):
        result.optional = archive.optional

    return BulkDownloadResult(
        mode="bulk",
        measured_mbps=measured,
        archives=list(results),
    )


# ---------------------------------------------------------------------------
# Job invoker — dispatched via ``_INVOKERS["sec_bulk_download"]``
# ---------------------------------------------------------------------------


JOB_SEC_BULK_DOWNLOAD: Final[str] = "sec_bulk_download"


class BootstrapPartialDownloadError(RuntimeError):
    """Raised when ``sec_bulk_download`` lands fewer than the full
    archive inventory.

    The orchestrator catches this and marks the A3 stage ``error``
    with the failed-archive list, so downstream Phase C stages see
    the failure (= status `blocked`) rather than no-op'ing on
    missing files. Closes the silent-success bug observed in the
    first live attempt: 2 of 14 archives errored mid-transfer,
    stage marked `success`, C1.a + C2 skipped silently.

    Spec: docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md
    """


def _is_not_published_error(error: str) -> bool:
    """True iff ``error`` indicates the archive is not yet published (HTTP 404).

    SEC returns 404 for a rolling-window / quarterly dataset that has not been
    posted yet. Both the HEAD pre-flight (``HEAD failed: status=404 …``) and
    the GET path (``GET failed: status=404``) embed ``status=404``. Any other
    failure (500, timeout, size mismatch, corrupt zip) is a genuine problem
    even for an optional archive and must stay fatal (#1423, Codex ckpt-2).
    """
    return "status=404" in error


def _fatal_download_failures(
    archives: list[ArchiveDownloadResult],
) -> list[ArchiveDownloadResult]:
    """Return the archives whose error must fail the stage.

    An optional archive (the newest 13F rolling window, #1423) is excluded
    ONLY when its error is a not-yet-published 404 — SEC has not posted the
    just-closed quarter. A non-404 failure on the same optional archive
    (500/timeout/corrupt) stays fatal. Every required archive's error is
    fatal: it raises ``BootstrapPartialDownloadError``, which the orchestrator
    surfaces as a stage error rather than letting downstream Phase C stages
    no-op on missing files.
    """
    return [r for r in archives if r.error is not None and not (r.optional and _is_not_published_error(r.error))]


def sec_bulk_download_job() -> None:
    """Zero-arg job invoker for the runtime registry.

    Raises ``BootstrapPartialDownloadError`` if the bulk-mode run
    finished with any archive in error state. Slow-connection
    fallback (mode=fallback) is NOT treated as an error — it
    intentionally bypasses Phase C and falls through to the legacy
    per-CIK chain.

    Writes ``<bulk>/.run_manifest.json`` after a complete success
    so Phase C preconditions can verify each archive belongs to
    the current bootstrap run (not a stale prior run's leftover).
    """
    from app.config import settings
    from app.security.master_key import resolve_data_dir

    target_dir = resolve_data_dir() / "sec" / "bulk"
    result = asyncio.run(
        download_bulk_archives(
            target_dir=target_dir,
            user_agent=settings.sec_user_agent,
        )
    )

    # Read the current bootstrap_run_id for the manifest stamp.
    import psycopg

    run_id: int | None = None
    try:
        with psycopg.connect(settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM bootstrap_runs WHERE status='running' ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
                run_id = int(row[0]) if row else None
    except Exception:  # noqa: BLE001
        pass

    if result.mode == "bulk":
        ok = sum(1 for r in result.archives if r.error is None)
        failed_archives = _fatal_download_failures(result.archives)
        # Optional archives that errored are expected-missing (newest 13F
        # window not yet published by SEC, #1423). Log them so the operator
        # sees coverage is best-effort, but they do NOT fail the stage.
        skipped_optional = [
            r for r in result.archives if r.error is not None and r.optional and _is_not_published_error(r.error)
        ]
        for r in skipped_optional:
            logger.info(
                "sec_bulk_download: optional archive not yet published (best-effort, #1423): %s: %s",
                r.name,
                r.error,
            )
        logger.info(
            "sec_bulk_download: mode=bulk mbps=%.1f archives_ok=%d archives_failed=%d optional_skipped=%d",
            result.measured_mbps or 0.0,
            ok,
            len(failed_archives),
            len(skipped_optional),
        )
        if failed_archives:
            # Surface partial-failure as a stage error so downstream
            # Phase C stages don't silently no-op on missing archives.
            details = "; ".join(f"{r.name}: {r.error}" for r in failed_archives)
            raise BootstrapPartialDownloadError(
                f"sec_bulk_download landed {ok}/{len(result.archives)} archives; failed: {details}"
            )
        # All archives landed; write the run manifest so Phase C
        # preconditions can verify provenance. The manifest is part
        # of the A3 success contract — without it Phase C cannot
        # validate archive run-id, so refusing to mark success when
        # we couldn't determine the run id keeps the contract honest.
        if run_id is None:
            raise BootstrapPartialDownloadError(
                "sec_bulk_download: could not determine current bootstrap_run_id; "
                "manifest cannot be written. Refuse to mark stage success without "
                "manifest provenance."
            )
        write_run_manifest(target_dir, bootstrap_run_id=run_id, archives=result.archives)
    elif result.mode == "fallback":
        logger.warning(
            "sec_bulk_download: mode=fallback mbps=%s reason=%s",
            result.measured_mbps,
            result.error,
        )
        # Fallback manifest is written for ops-monitor / audit
        # provenance — preserves the historical contract even though
        # the capability layer (#1138 Task A) no longer reads it for
        # downstream dispatch. Refuse to proceed without a writable
        # run_id, matching the bulk-mode contract above (Codex
        # pre-push MEDIUM for #1041).
        if run_id is None:
            raise BootstrapPartialDownloadError(
                "sec_bulk_download: could not determine current bootstrap_run_id; fallback manifest cannot be written."
            )
        write_run_manifest(target_dir, bootstrap_run_id=run_id, archives=[], mode="fallback")
        # #1138 Task A — raise BootstrapPhaseSkipped so the bootstrap
        # orchestrator transitions S7 to `skipped` instead of
        # `success`. Without this, `bulk_archives_ready` would be
        # falsely advertised even though no archives were downloaded;
        # the cascade-skip rule in the dispatcher relies on the
        # `skipped` status to correctly cascade Phase C C-stages.
        raise BootstrapPhaseSkipped(
            f"slow-connection fallback (mbps={result.measured_mbps}); fallback manifest written, bulk archives bypassed"
        )
    elif result.mode == "skipped_disk":
        # Disk pre-flight refused — surface as error so operator
        # knows to free space; downstream Phase C will be `blocked`.
        raise BootstrapPartialDownloadError(f"sec_bulk_download refused: {result.error}")
    else:
        logger.error("sec_bulk_download: mode=%s error=%s", result.mode, result.error)
        raise BootstrapPartialDownloadError(f"sec_bulk_download unexpected mode={result.mode!r}: {result.error}")
