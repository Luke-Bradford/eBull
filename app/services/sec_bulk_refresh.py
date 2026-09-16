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
  =<str>``: no new archive was published. Reasons: bootstrap in
  flight, SEC 5xx, HEAD missing ETag, archive name unknown, or a
  local-filesystem failure during publication
  (``post_download_hash_failed`` / ``sidecar_invalidate_failed`` /
  ``publish_rename_failed``) — in all of which the previous archive
  and its sidecars are left exactly as they were.

The job invokers (``sec_submissions_bulk_refresh_job`` /
``sec_companyfacts_bulk_refresh_job`` /
``sec_quarterly_datasets_bulk_refresh_job``) sum ``bytes_downloaded``
across the archives they cover into ``tracker.row_count`` so the
operator can see at a glance how much was actually transferred.

Sidecars
--------
Two sibling files per archive at ``<bulk>/``:

* ``<archive>.etag``    — the GET response's strong ETag (verbatim,
  including quotes) for exactly the bytes on disk.
* ``<archive>.sha256``  — SHA-256 hex digest of the local archive bytes.

Both are written atomically (tmp + ``Path.replace``). Skipping a
transfer requires BOTH — ETag equality with SEC's live HEAD *and* a
`.sha256` that still matches the local bytes — which is the reuse rule
settled 2026-05-22. On read, a missing, unreadable or mismatched
sidecar means "treat as stale": the next refresh re-downloads and
rebuilds the pair. Length equality and ZIP readability are NOT
evidence of identity and never certify an archive (#3112).

Commit order (#3112): both sidecars are removed BEFORE the validated
``.zip`` is renamed into place, then ``.sha256`` is written, then
``.etag`` last. The ETag sidecar is the commit marker for both
consumers, so any interruption leaves the archive uncertified rather
than certified-wrong; the cost is a re-download, never a bad read.

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
from app.jobs.job_connection import connect_job
from app.providers.sec_throttle_metrics import incr_sec_429
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


def _read_sidecar_text(sidecar: Path) -> str | None:
    """Return the stripped contents of ``sidecar``, or ``None``.

    ``None`` for missing, unreadable, non-UTF-8 or empty — every one of
    which means "we have no trustworthy provenance for these bytes", so
    the caller must treat the archive as stale rather than guess.
    """
    if not sidecar.exists():
        return None
    try:
        content = sidecar.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError) as exc:
        logger.warning("sec_bulk_refresh: unreadable sidecar at %s: %s", sidecar, exc)
        return None
    return content or None


def _read_local_etag(archive_path: Path) -> str | None:
    """Return the recorded ETag for ``archive_path`` or ``None``.

    Returns ``None`` if the sidecar is missing, unreadable, or empty.
    A missing sidecar means "treat as stale" — the refresh will
    re-download and recreate it.
    """
    return _read_sidecar_text(_etag_sidecar_path(archive_path))


def _is_weak_etag(etag: str) -> bool:
    """Return True for an RFC 9110 §8.8.1 weak validator (``W/"..."``).

    A weak ETag only promises SEMANTIC equivalence, so two archives
    sharing one may still differ byte-for-byte. Our whole reuse model
    (settled 2026-05-22) is byte identity, so a weak validator is not
    usable — neither for the skip-without-transfer decision nor as a
    stored sidecar value, because storing one would let a later fire
    compare weak-to-weak and skip a real update. SEC serves STRONG
    multipart ETags today (``"36cd63…-168"``); this guard exists so a
    server-side change degrades into extra transfers rather than into
    silently certifying the wrong bytes.
    """
    return etag.lstrip().startswith(("W/", "w/"))


def _local_sha256_matches(archive_path: Path) -> bool:
    """Return True iff the ``.sha256`` sidecar matches the archive bytes.

    This is condition (2) of the 2026-05-22 settled decision on bulk
    archive reuse. A missing/unreadable sidecar, an unreadable archive,
    or a digest mismatch all return False — "no proof, so re-download".
    """
    stored = _read_sidecar_text(_sha256_sidecar_path(archive_path))
    if stored is None:
        logger.info(
            "sec_bulk_refresh: %s has no readable sha256 sidecar — cannot skip transfer",
            archive_path.name,
        )
        return False
    try:
        actual = _compute_sha256(archive_path)
    except OSError as exc:
        logger.warning("sec_bulk_refresh: could not hash %s: %s", archive_path, exc)
        return False
    if actual != stored:
        logger.warning(
            "sec_bulk_refresh: %s sha256 sidecar does not describe the local bytes "
            "(sidecar=%s actual=%s) — re-downloading",
            archive_path.name,
            stored,
            actual,
        )
        return False
    return True


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

    from app.services.sec_pipelined_fetcher import _AsyncRateLimiter

    # #1484: no shared_clock -> _AsyncRateLimiter defaults to the cross-process
    # gate. The old per-process 7 rps self-limit is subsumed by the global gate.
    rate_limiter = _AsyncRateLimiter(target_rps=7.0)

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

        if head.status_code == 429:
            incr_sec_429()
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

        # Fast-path: skip the transfer only when BOTH conditions of the
        # 2026-05-22 settled decision hold — (1) the local ``.etag``
        # sidecar equals SEC's strong HEAD ETag, and (2) the ``.sha256``
        # sidecar matches a fresh digest of the local bytes. (1) alone
        # only says "the value we last recorded still matches the
        # server"; it says nothing about whether the bytes on disk are
        # still the ones that value was recorded for. The ZIP round-trip
        # stays as a cheap pre-filter (it reads the central directory
        # only) and is ordered BEFORE the hash so a corrupt archive
        # costs no full read. Measured: hashing the real 1.5 GB
        # submissions.zip takes 0.62 s, against ~1 in 3 fires reaching
        # this path.
        if (
            local_etag is not None
            and local_etag == remote_etag
            and not _is_weak_etag(remote_etag)
            and archive_path.exists()
            and _zip_round_trip(archive_path)
            and _local_sha256_matches(archive_path)
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

        # NOTE (#3112): there used to be a "seed-on-first-encounter"
        # branch here. With no local ETag sidecar it accepted the local
        # archive as identical to the remote one on HEAD Content-Length
        # equality plus a ZIP round-trip, then wrote the REMOTE ETag
        # beside a SHA-256 of the OLD LOCAL bytes — a fabricated
        # provenance pair that both this module's fast-path and
        # ``sec_bulk_download``'s reuse pre-flight would then trust.
        # Equal length is not equal content. Its stated justification
        # ("the bootstrap downloader writes no sidecar, PR-5b is in
        # flight") is obsolete: ``sec_bulk_download`` writes both
        # sidecars after every successful download. A missing ETag
        # sidecar now means exactly what it says — no trustworthy
        # provenance — and falls through to a validated re-download.

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
                if response.status_code == 429:
                    incr_sec_429()
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
        try:
            sha256_hex = _compute_sha256(partial_path)
        except OSError as exc:
            logger.error("sec_bulk_refresh: could not hash downloaded %s: %s", archive.name, exc)
            try:
                partial_path.unlink()
            except OSError:
                pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="post_download_hash_failed",
            )

        # ETag to record: ONLY the GET response's own strong ETag, which
        # describes exactly the bytes we kept. #3112: this used to fall
        # back to the HEAD ETag (``get_etag or remote_etag``), which is
        # the same defect as the deleted seed branch one layer down — a
        # CDN serving HEAD=A then GET=B *without* an ETag would have had
        # B's bytes stamped with A's validator, and the next fire's
        # fast-path would then skip a real update forever. With no
        # usable GET validator we keep the fresh bytes but record no
        # ETag, so the next fire re-downloads (same posture as
        # ``sec_bulk_download``'s "no ETag header on GET" path).
        recorded_etag = get_etag if get_etag is not None and not _is_weak_etag(get_etag) else None

        etag_path = _etag_sidecar_path(archive_path)
        sha_path = _sha256_sidecar_path(archive_path)

        # Publish. The ETag sidecar is the commit marker for BOTH
        # consumers (this fast-path and sec_bulk_download's pre-flight),
        # so it is removed BEFORE the new bytes land and written LAST.
        # Any interruption therefore leaves either no ETag sidecar or
        # the pre-existing one — neither of which can certify the new
        # bytes — and the next fire re-downloads.
        try:
            etag_path.unlink(missing_ok=True)
            sha_path.unlink(missing_ok=True)
        except OSError as exc:
            logger.error(
                "sec_bulk_refresh: could not invalidate sidecars for %s: %s — keeping old archive",
                archive.name,
                exc,
            )
            try:
                partial_path.unlink()
            except OSError:
                pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="sidecar_invalidate_failed",
            )

        try:
            partial_path.replace(archive_path)
        except OSError as exc:
            logger.error("sec_bulk_refresh: could not publish %s: %s", archive.name, exc)
            try:
                partial_path.unlink()
            except OSError:
                pass
            return RefreshResult(
                archive_name=archive.name,
                etag_changed=False,
                bytes_downloaded=0,
                skipped_reason="publish_rename_failed",
            )

        # From here the new bytes ARE the archive; a sidecar failure
        # costs a re-download next fire but never a wrong certification,
        # so it is logged rather than raised (raising would abort the
        # remaining archives in refresh_archive_set).
        try:
            _atomic_write_text(sha_path, sha256_hex)
        except OSError as exc:
            logger.error(
                "sec_bulk_refresh: sha256 sidecar write failed for %s: %s — "
                "leaving the archive uncertified (next fire re-downloads)",
                archive.name,
                exc,
            )
        else:
            if recorded_etag is None:
                logger.warning(
                    "sec_bulk_refresh: %s GET carried no usable strong ETag — "
                    "archive updated but left uncertified; next fire re-downloads",
                    archive.name,
                )
            else:
                try:
                    _atomic_write_text(etag_path, recorded_etag)
                except OSError as exc:
                    logger.error(
                        "sec_bulk_refresh: etag sidecar write failed for %s: %s — next fire re-downloads",
                        archive.name,
                        exc,
                    )

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
    # #1693 — connect_job binds the active job's statement_timeout (ContextVar
    # set by _tracked_job for the three steady-state bulk-refresh jobs). A wedged
    # bootstrap_state probe self-aborts at the cap → raised as QueryCanceled
    # (a psycopg.Error) → caught below → clean skip + re-fire next cadence,
    # instead of stranding the job_runs row 'running' (#1689 mode).
    try:
        with connect_job(autocommit=True) as conn:
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
