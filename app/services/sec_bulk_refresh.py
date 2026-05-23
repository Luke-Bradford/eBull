"""Daily refresh adapter for SEC bulk archives (#1233 PR-8).

Architectural problem
---------------------
SEC's bulk archives (``submissions.zip``, ``companyfacts.zip``,
quarterly 13F / NPORT / insider datasets) are downloaded ONLY at
bootstrap time by ``app.services.sec_bulk_download``. After initial
install they stale forever: operator daily updates against fresh
discovery (Atom / daily-index) miss newly-published rows that landed
inside the bulk archives between bootstraps. Bulk-ingest stages
(Phase C in the bootstrap orchestrator) consume those archives as if
they were canonical — silent freshness drift across the whole bulk
plane.

PR-8 closes this loop with three SCHEDULED_JOBS — one per archive
family — that HEAD the live SEC URL each day and re-download only
when the server's ETag has changed since the last successful local
copy. The HEAD is the cheap probe; the download is the rare event.

Contract
--------
``refresh_bulk_archive_if_stale(archive_name)`` returns a
``RefreshResult`` whose ``etag_changed`` + ``bytes_downloaded`` +
``skipped_reason`` fields fully describe the outcome:

* ``etag_changed=False`` + ``bytes_downloaded=0`` + ``skipped_reason
  =None``: HEAD ETag matched the local sidecar; no work.
* ``etag_changed=True`` + ``bytes_downloaded>0`` + ``skipped_reason
  =None``: SEC published an update; new file landed atomically,
  sidecars rewritten.
* ``etag_changed=False`` + ``bytes_downloaded=0`` + ``skipped_reason
  =<str>``: skipped without contacting SEC. Reasons: bootstrap in
  flight, SEC 5xx, HEAD missing ETag, archive name unknown.

The job invokers (``sec_submissions_bulk_refresh_job`` /
``sec_companyfacts_bulk_refresh_job`` /
``sec_quarterly_datasets_bulk_refresh_job``) sum ``bytes_downloaded``
across the archives they cover into ``tracker.row_count`` so the
operator can see at a glance how much was actually transferred.

Sidecars
--------
Two sibling files per archive at ``<bulk>/``:

* ``<archive>.etag``    — SEC's HEAD ETag (verbatim, including quotes).
* ``<archive>.sha256``  — SHA-256 hex digest of the local archive bytes.

Both are written atomically (tmp + ``Path.replace``) AFTER the
``.zip`` rename so a crash mid-write never leaves a sidecar
referencing a partial download. On read, a missing or unreadable
sidecar means "treat as stale" — the next refresh re-downloads
and rebuilds the sidecar pair.

Bootstrap fence
---------------
While ``bootstrap_state.status='running'`` the orchestrator's
own bulk-download stage may be re-writing the same files. PR-5b's
reuse path expects the sidecars to be stable mid-bootstrap, so
this refresh adapter SKIPS while bootstrap is in flight rather
than racing it. The fence is a single SELECT against the
singleton row; no advisory lock is held.

Rate limit
----------
HEAD + GET acquire from the shared
``_PROCESS_RATE_LIMIT_CLOCK`` / ``_PROCESS_RATE_LIMIT_LOCK`` budget
(7 req/s ceiling — same as ``sec_bulk_download``). The daily
cadence + small HEAD payload means the typical fire spends ~1 budget
slot; the rare changed-archive fire spends a stream's worth of
GETs that count fully against SEC's per-IP budget.

Fail-closed
-----------
* SEC 5xx on HEAD or GET → return ``skipped_reason`` (do NOT raise).
  The local file is left untouched; the next fire retries.
* HEAD has no ``ETag`` header → return ``skipped_reason="head_missing_etag"``.
  Falling back to size-only compare is intentionally NOT done — SEC's
  ETag is the contractual freshness signal; without it we have no
  cheap way to tell stale from current.
* GET returns non-200 / size mismatch / ZIP corrupt → keep the OLD
  archive on disk and return ``skipped_reason``.
* Bootstrap is running → ``skipped_reason="bootstrap_running"``.

The operator-visible outcome of a skip is a ``job_runs`` row with
``status='success'`` (the refresh itself ran fine), ``row_count=0``,
and ``error_msg=<skipped_reason>`` so the admin UI surfaces the
skip cleanly.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

import httpx
import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.sec_bulk_download import (
    BulkArchive,
    _classify_content_type,
    _has_zip_magic,
    _make_client,
    _zip_round_trip,
    build_bulk_archive_inventory,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of refreshing one archive.

    Exactly one of these three combinations holds:

    * ``etag_changed=True``  → ``bytes_downloaded > 0``, ``skipped_reason is None``.
      A new copy landed; sidecars were rewritten.
    * ``etag_changed=False`` + ``skipped_reason is None``:
      HEAD ETag matched local sidecar; no transfer.
    * ``skipped_reason is not None``:
      skipped — bootstrap running, SEC error, HEAD missing ETag,
      unknown archive_name, or archive currently absent from disk.
      ``etag_changed=False`` and ``bytes_downloaded=0`` in this case.
    """

    archive_name: str
    etag_changed: bool
    bytes_downloaded: int
    skipped_reason: str | None


# ---------------------------------------------------------------------------
# Sidecar helpers
# ---------------------------------------------------------------------------


SIDECAR_ETAG_SUFFIX: Final[str] = ".etag"
SIDECAR_SHA256_SUFFIX: Final[str] = ".sha256"

# SEC 5xx is the canonical "back off" signal; treat 4xx the same way
# (4xx on a URL we control is operator-visible misconfiguration —
# log loudly and skip, do not delete the local file).
_TRANSIENT_HTTP_STATUSES: Final[tuple[int, ...]] = (
    429,
    500,
    502,
    503,
    504,
)


def _etag_sidecar_path(archive_path: Path) -> Path:
    return archive_path.with_name(archive_path.name + SIDECAR_ETAG_SUFFIX)


def _sha256_sidecar_path(archive_path: Path) -> Path:
    return archive_path.with_name(archive_path.name + SIDECAR_SHA256_SUFFIX)


def _read_local_etag(archive_path: Path) -> str | None:
    """Return the recorded ETag for ``archive_path`` or ``None``.

    Returns ``None`` if the sidecar is missing, unreadable, or empty.
    A missing sidecar means "treat as stale" — the refresh will
    re-download and recreate it.
    """
    sidecar = _etag_sidecar_path(archive_path)
    if not sidecar.exists():
        return None
    try:
        content = sidecar.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.warning("sec_bulk_refresh: unreadable etag sidecar at %s: %s", sidecar, exc)
        return None
    return content or None


def _atomic_write_text(path: Path, content: str) -> None:
    """Write ``content`` to ``path`` via a sibling tempfile + rename.

    Sidecars MUST be atomic: a crash mid-write that leaves a partial
    ``.etag`` would make the next refresh skip a real update because
    the truncated text won't equal SEC's full ETag (and we'd hit the
    download path anyway — atomicity is belt-and-braces against
    pathological half-states where the partial happens to be a prefix
    of the live value).
    """
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _compute_sha256(archive_path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return the SHA-256 hex digest of the file at ``archive_path``.

    Streamed in 1 MB chunks so a 1.5 GB ``submissions.zip`` does not
    blow the process RSS. Caller is responsible for ensuring the
    file exists.
    """
    hasher = hashlib.sha256()
    with archive_path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# Bootstrap fence
# ---------------------------------------------------------------------------


def _bootstrap_running(conn: psycopg.Connection) -> bool:
    """Return True if ``bootstrap_state.status='running'``.

    A True result means PR-5b's bulk-download reuse path is or may
    be writing to the same archives; the daily refresh must defer.
    Defense-in-depth: also returns True if the row is missing (the
    fail-closed direction).
    """
    row = conn.execute("SELECT status FROM bootstrap_state WHERE id = 1").fetchone()
    if row is None:
        # Fail closed — missing singleton is operator-visible elsewhere
        # (ensure_bootstrap_state_singleton on boot); we refuse to race
        # rather than guess.
        return True
    return str(row[0]) == "running"


# ---------------------------------------------------------------------------
# Archive name registry
# ---------------------------------------------------------------------------


_SUBMISSIONS_NAME: Final[str] = "submissions.zip"
_COMPANYFACTS_NAME: Final[str] = "companyfacts.zip"


def _archive_for_name(archive_name: str, *, today: date | None = None) -> BulkArchive | None:
    """Return the ``BulkArchive`` for ``archive_name`` or ``None``.

    The quarterly archives (form13f_*, insider_*, nport_*) are looked
    up against the live inventory builder so the daily refresh always
    targets the SAME files the bootstrap downloader would write. Any
    drift between the two URL maps would let one job re-download
    something the other treats as canonical.

    ``today`` is forwarded so tests can pin the quarterly window
    deterministically.
    """
    if archive_name == _SUBMISSIONS_NAME:
        return BulkArchive(
            name=_SUBMISSIONS_NAME,
            url=f"https://www.sec.gov/Archives/edgar/daily-index/bulkdata/{_SUBMISSIONS_NAME}",
        )
    if archive_name == _COMPANYFACTS_NAME:
        return BulkArchive(
            name=_COMPANYFACTS_NAME,
            url=f"https://www.sec.gov/Archives/edgar/daily-index/xbrl/{_COMPANYFACTS_NAME}",
        )
    inventory = build_bulk_archive_inventory(today=today)
    for archive in inventory:
        if archive.name == archive_name:
            return archive
    return None


# ---------------------------------------------------------------------------
# Async core
# ---------------------------------------------------------------------------


def _resolve_target_dir() -> Path:
    """Return the bulk-archives directory used by ``sec_bulk_download``."""
    return resolve_data_dir() / "sec" / "bulk"


async def _refresh_one_async(
    *,
    archive: BulkArchive,
    target_dir: Path,
    user_agent: str,
) -> RefreshResult:
    """HEAD + conditional download for one archive.

    Returns a ``RefreshResult``; never raises on SEC HTTP errors —
    they are captured into ``skipped_reason`` so a single transient
    blip doesn't poison the whole job_run.
    """
    archive_path = target_dir / archive.name
    local_etag = _read_local_etag(archive_path)

    # Lazy import to avoid pulling sec_edgar (heavy) at module load.
    from app.providers.implementations.sec_edgar import (
        _PROCESS_RATE_LIMIT_CLOCK,
        _PROCESS_RATE_LIMIT_LOCK,
    )
    from app.services.sec_pipelined_fetcher import _AsyncRateLimiter

    rate_limiter = _AsyncRateLimiter(
        target_rps=7.0,
        shared_clock=_PROCESS_RATE_LIMIT_CLOCK,
        shared_lock=_PROCESS_RATE_LIMIT_LOCK,
    )

    async with _make_client(user_agent) as client:
        # HEAD probe ------------------------------------------------------
        await rate_limiter.acquire()
        try:
            head = await client.head(archive.url)
        except httpx.HTTPError as exc:
            logger.warning("sec_bulk_refresh HEAD failed for %s: %s", archive.name, exc)
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=f"head_transport_error: {type(exc).__name__}",
            )

        if head.status_code in _TRANSIENT_HTTP_STATUSES:
            logger.warning(
                "sec_bulk_refresh HEAD got %d for %s — skipping (will retry next fire)",
                head.status_code,
                archive.name,
            )
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=f"head_status_{head.status_code}",
            )
        if head.status_code != 200:
            logger.error(
                "sec_bulk_refresh HEAD got unexpected %d for %s",
                head.status_code,
                archive.name,
            )
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=f"head_status_{head.status_code}",
            )

        remote_etag = head.headers.get("etag")
        if not remote_etag:
            logger.warning("sec_bulk_refresh: %s HEAD response has no ETag header", archive.name)
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="head_missing_etag",
            )

        content_type = (head.headers.get("content-type") or "").lower()
        if _classify_content_type(content_type) == "bad":
            logger.warning(
                "sec_bulk_refresh: %s HEAD Content-Type=%r is not an archive — skipping",
                archive.name,
                content_type,
            )
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=f"head_bad_content_type: {content_type}",
            )

        content_length_raw = head.headers.get("content-length")
        if content_length_raw is None:
            logger.warning("sec_bulk_refresh: %s HEAD response missing Content-Length", archive.name)
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="head_missing_content_length",
            )
        try:
            expected_total = int(content_length_raw)
        except ValueError:
            logger.warning(
                "sec_bulk_refresh: %s HEAD Content-Length=%r is not an integer",
                archive.name,
                content_length_raw,
            )
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="head_bad_content_length",
            )

        # Fast-path: ETag match AND we have a local file that
        # round-trips. The round-trip is cheap (zipfile.ZipFile reads
        # the central directory only) and catches the case where the
        # ETag sidecar survived a crash that corrupted the archive.
        if (
            local_etag is not None
            and local_etag == remote_etag
            and archive_path.exists()
            and _zip_round_trip(archive_path)
        ):
            logger.info(
                "sec_bulk_refresh: %s fresh (etag=%s) — no transfer",
                archive.name,
                remote_etag,
            )
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=None,
            )

        # Seed-on-first-encounter: bootstrap downloader (sec_bulk_download)
        # currently writes only the ``.zip`` itself — no ETag sidecar.
        # PR-5b is in flight and will eventually do the sidecar write,
        # but until then the FIRST refresh fire after install would see
        # no sidecar and re-download the multi-GB archive even though
        # the disk copy is fine. Detect this case and adopt the live
        # HEAD ETag as the sidecar value, transferring zero bytes.
        # Verified by: HEAD Content-Length must match local size AND the
        # local ZIP must round-trip. If either fails we fall through to
        # the slow path (a genuine re-download).
        if (
            local_etag is None
            and archive_path.exists()
            and archive_path.stat().st_size == expected_total
            and _zip_round_trip(archive_path)
        ):
            sha256_hex = _compute_sha256(archive_path)
            _atomic_write_text(_etag_sidecar_path(archive_path), remote_etag)
            _atomic_write_text(_sha256_sidecar_path(archive_path), sha256_hex)
            logger.info(
                "sec_bulk_refresh: %s seeded sidecars from live HEAD (etag=%s sha256=%s size=%d) — no transfer",
                archive.name,
                remote_etag,
                sha256_hex,
                expected_total,
            )
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=None,
            )

        # Slow-path: stream the new copy to a sibling tempfile,
        # validate ZIP integrity, then atomic-rename + write sidecars.
        await rate_limiter.acquire()
        partial_path = archive_path.with_name(archive_path.name + ".refresh.partial")
        # Clear any partial from a prior failed refresh — we always
        # restart fresh on the refresh path (the bootstrap downloader's
        # resume logic is for the bootstrap budget; the daily refresh
        # is a single-shot whole-archive re-pull).
        if partial_path.exists():
            try:
                partial_path.unlink()
            except OSError as exc:
                logger.warning("sec_bulk_refresh: could not clear stale partial %s: %s", partial_path, exc)

        bytes_written = 0
        get_etag: str | None = None
        try:
            async with client.stream("GET", archive.url) as response:
                if response.status_code in _TRANSIENT_HTTP_STATUSES:
                    logger.warning(
                        "sec_bulk_refresh GET got %d for %s — skipping (local file untouched)",
                        response.status_code,
                        archive.name,
                    )
                    return RefreshResult(
                        archive_name=archive.name,
                        etag_changed=False,
                        bytes_downloaded=0,
                        skipped_reason=f"get_status_{response.status_code}",
                    )
                if response.status_code != 200:
                    logger.error(
                        "sec_bulk_refresh GET got unexpected %d for %s",
                        response.status_code,
                        archive.name,
                    )
                    return RefreshResult(
                        archive_name=archive.name,
                        etag_changed=False,
                        bytes_downloaded=0,
                        skipped_reason=f"get_status_{response.status_code}",
                    )

                # Capture the GET response's ETag header BEFORE streaming
                # so we can detect a CDN race where HEAD returns version
                # A's ETag but GET serves version B's bytes (different
                # ETag). Without this check, the post-rename sidecar would
                # falsely advertise A's ETag against B's bytes and the
                # next HEAD-match fast-path would wrongly skip future
                # real updates.
                get_etag = response.headers.get("etag")

                with partial_path.open("wb") as fh:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                        fh.write(chunk)
                        bytes_written += len(chunk)
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("sec_bulk_refresh GET transport failure for %s: %s", archive.name, exc)
            # Discard partial; local file unaffected.
            if partial_path.exists():
                try:
                    partial_path.unlink()
                except OSError:
                    pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason=f"get_transport_error: {type(exc).__name__}",
            )

        # Post-transfer integrity checks. ANY failure leaves the
        # local archive on disk untouched — we discard the partial
        # and skip.
        if bytes_written != expected_total:
            logger.error(
                "sec_bulk_refresh: %s size mismatch — got %d, expected %d",
                archive.name,
                bytes_written,
                expected_total,
            )
            try:
                partial_path.unlink()
            except OSError:
                pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="get_size_mismatch",
            )

        if not _has_zip_magic(partial_path):
            logger.error("sec_bulk_refresh: %s downloaded content lacks ZIP magic", archive.name)
            try:
                partial_path.unlink()
            except OSError:
                pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="get_no_zip_magic",
            )

        if not _zip_round_trip(partial_path):
            logger.error("sec_bulk_refresh: %s ZIP round-trip failed", archive.name)
            try:
                partial_path.unlink()
            except OSError:
                pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="get_zip_corrupt",
            )

        # CDN-race check: HEAD said version A, GET served version B.
        # If the GET response carries an ETag and it disagrees with the
        # HEAD ETag, we got bytes from a different version than what
        # we'd record in the sidecar. Skip; the next fire will HEAD
        # again and either see the new version stably or pick up the
        # original. We do NOT proceed to overwrite the local file
        # because doing so would let a stale sidecar's ETag (the HEAD
        # ETag we'd write) misclassify the new bytes as "old version"
        # forever. Missing GET ETag is tolerated — SEC's CDN does
        # return one in practice (verified 2026-05-22) but absence
        # alone shouldn't poison the path.
        if get_etag is not None and get_etag != remote_etag:
            logger.warning(
                "sec_bulk_refresh: %s CDN race detected — HEAD etag=%s "
                "GET etag=%s — discarding partial, retrying next fire",
                archive.name,
                remote_etag,
                get_etag,
            )
            try:
                partial_path.unlink()
            except OSError:
                pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="get_etag_mismatch_with_head",
            )

        # Compute SHA-256 BEFORE the rename so the sidecar describes
        # exactly the bytes that landed at archive_path.
        sha256_hex = _compute_sha256(partial_path)

        # ETag to record: prefer the GET response ETag (describes
        # exactly the bytes we kept); fall back to HEAD when GET
        # didn't supply one. Both agree at this point (mismatch was
        # already returned above).
        recorded_etag = get_etag or remote_etag

        # Atomic rename — moves the validated copy into the canonical
        # path, replacing any existing archive. Sidecars follow.
        partial_path.replace(archive_path)
        _atomic_write_text(_etag_sidecar_path(archive_path), recorded_etag)
        _atomic_write_text(_sha256_sidecar_path(archive_path), sha256_hex)

        logger.info(
            "sec_bulk_refresh: %s updated — old_etag=%s new_etag=%s bytes=%d sha256=%s",
            archive.name,
            local_etag,
            recorded_etag,
            bytes_written,
            sha256_hex,
        )
        return RefreshResult(
            archive_name=archive.name,
            etag_changed=True,
            bytes_downloaded=bytes_written,
            skipped_reason=None,
        )


# ---------------------------------------------------------------------------
# Public entrypoint (sync)
# ---------------------------------------------------------------------------


def refresh_bulk_archive_if_stale(archive_name: str) -> RefreshResult:
    """HEAD the SEC URL for ``archive_name`` and re-download if changed.

    Side effects, on a successful change:

    * Writes ``<bulk>/<archive_name>`` (replacing the old copy via
      atomic rename).
    * Writes ``<bulk>/<archive_name>.etag`` (verbatim SEC ETag).
    * Writes ``<bulk>/<archive_name>.sha256`` (hex digest of the
      bytes that landed).

    On any skip path the local archive + sidecars are untouched.

    The function reads ``settings.sec_user_agent`` and
    ``app.security.master_key.resolve_data_dir()`` directly so the
    scheduled-job invokers can dispatch with no parameters.
    """
    target_dir = _resolve_target_dir()
    target_dir.mkdir(parents=True, exist_ok=True)

    archive = _archive_for_name(archive_name)
    if archive is None:
        logger.error("sec_bulk_refresh: unknown archive_name=%r — registry drift", archive_name)
        return RefreshResult(
            archive_name=archive_name,
            etag_changed=False,
            bytes_downloaded=0,
            skipped_reason="unknown_archive_name",
        )

    # Bootstrap fence — open a fresh autocommit conn so the SELECT is
    # not nested in any outer transaction held by the job runtime.
    try:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            if _bootstrap_running(conn):
                logger.info(
                    "sec_bulk_refresh: %s skipped — bootstrap_state.status='running'",
                    archive_name,
                )
                return RefreshResult(
                    archive_name=archive_name,
                    etag_changed=False,
                    bytes_downloaded=0,
                    skipped_reason="bootstrap_running",
                )
    except psycopg.Error as exc:
        # DB unreachable is a deployment-level failure; we don't
        # want to hammer SEC while the DB is sick. Skip the fire and
        # surface the reason.
        logger.warning("sec_bulk_refresh: bootstrap_state probe failed: %s", exc)
        return RefreshResult(
            archive_name=archive_name,
            etag_changed=False,
            bytes_downloaded=0,
            skipped_reason="bootstrap_state_probe_failed",
        )

    return asyncio.run(
        _refresh_one_async(
            archive=archive,
            target_dir=target_dir,
            user_agent=settings.sec_user_agent,
        )
    )


# ---------------------------------------------------------------------------
# Archive-set helpers — declared here so the scheduler invokers
# stay one-liners and the set membership is testable.
# ---------------------------------------------------------------------------


def _submissions_archive_names() -> tuple[str, ...]:
    return (_SUBMISSIONS_NAME,)


def _companyfacts_archive_names() -> tuple[str, ...]:
    return (_COMPANYFACTS_NAME,)


def _quarterly_dataset_archive_names(*, today: date | None = None) -> tuple[str, ...]:
    """Return the quarterly-dataset archive names (13F + insider + NPORT).

    Pulled from ``build_bulk_archive_inventory`` so the quarterly
    refresh job targets the same files the bootstrap downloader
    would.  ``today`` is honored for deterministic tests.
    """
    return tuple(
        archive.name
        for archive in build_bulk_archive_inventory(today=today)
        if archive.name not in (_SUBMISSIONS_NAME, _COMPANYFACTS_NAME)
    )


def refresh_archive_set(archive_names: Sequence[str]) -> list[RefreshResult]:
    """Refresh every archive in ``archive_names`` sequentially.

    Sequential — not concurrent — for two reasons:

    1. The shared rate limiter would queue them anyway; ``asyncio.run``
       per archive simplifies error containment.
    2. A bootstrap-in-flight skip on the first archive is the same
       answer for every subsequent archive — we still pay one HEAD
       per archive on a non-fenced fire, but the operator-visible
       row_count and the job runtime are simpler.

    The caller wraps this in ``_tracked_job`` and sums
    ``bytes_downloaded`` into ``row_count``.
    """
    results: list[RefreshResult] = []
    for name in archive_names:
        results.append(refresh_bulk_archive_if_stale(name))
    return results


__all__ = [
    "RefreshResult",
    "refresh_archive_set",
    "refresh_bulk_archive_if_stale",
]
