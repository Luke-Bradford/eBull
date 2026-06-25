"""Local ``submissions.zip`` accelerator for bootstrap SEC stages (#1340).

The bulk ``submissions.zip`` archive published by SEC contains every CIK's
primary ``CIK<10>.json`` submissions index. Bootstrap stages that would
otherwise issue one per-CIK HTTP fetch of
``https://data.sec.gov/submissions/CIK<10>.json`` to enumerate a filer's
accessions can instead read that entry from the already-landed local archive,
eliminating thousands of rate-limited HTTP round-trips.

Two consumers share this module so the primary-URL contract cannot drift:

* **S16 first-install drain** (#1277) — ``HttpGet``-shaped; imports
  :data:`PRIMARY_SUBMISSIONS_URL_RE` only (its ``_make_zip_http_get`` keeps its
  own ``(int, bytes)`` routing).
* **S23 N-PORT trust ingest** (#1340) — ``SecArchiveFetcher``-shaped; wraps a
  real fetcher in :class:`ZipBackedArchiveFetcher`.

Secondary pages ``CIK<10>-submissions-<NNN>.json`` are NOT in the bulk archive
(canonical reference: ``app/services/sec_submissions_files_walk.py:16-23``), so
:data:`PRIMARY_SUBMISSIONS_URL_RE` deliberately matches the primary index only.
"""

from __future__ import annotations

import logging
import re
import threading
import zipfile
from typing import Protocol

logger = logging.getLogger(__name__)

# Primary submissions URL pattern. Routes ONLY the primary ``CIK<10>.json``
# index to the local archive; secondary pages + every other URL fall through to
# the real HTTP transport.
PRIMARY_SUBMISSIONS_URL_RE = re.compile(r"^https://data\.sec\.gov/submissions/CIK(\d{10})\.json$")


class _DocFetcher(Protocol):
    """The single method :class:`ZipBackedArchiveFetcher` delegates to.

    Structurally identical to ``app.services.n_port_ingest.SecArchiveFetcher``;
    re-declared here to keep this module import-light (no dependency on the
    N-PORT service)."""

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


def match_primary_submissions_cik(url: str) -> str | None:
    """Return the zero-padded 10-digit CIK if ``url`` is a primary submissions
    URL, else ``None``."""
    match = PRIMARY_SUBMISSIONS_URL_RE.match(url)
    return match.group(1) if match is not None else None


def read_zip_entry(zf: zipfile.ZipFile, entry_name: str) -> bytes | None:
    """Return the bytes of ``entry_name`` from ``zf``.

    Returns ``None`` when the member is absent (``KeyError``). Re-raises
    ``zipfile.BadZipFile`` / ``OSError`` for the caller to handle (corrupt or
    truncated member surfaced mid-read) — callers route those to an HTTP
    fallback rather than dropping the resource.
    """
    try:
        with zf.open(entry_name) as fh:
            return fh.read()
    except KeyError:
        return None


class ZipBackedArchiveFetcher:
    """Wrap a :class:`_DocFetcher`; serve primary submissions URLs from a local
    ``submissions.zip`` and delegate everything else to the wrapped fetcher.

    The zip is a **pure accelerator, never a coverage reducer** (#1340 Codex 1
    BLOCKING-1): only a clean zip HIT short-circuits the HTTP fetch. Every
    non-hit — member absent, bad bytes, corrupt member, or a non-primary URL —
    delegates to ``fallback.fetch_document_text`` so a trust newly present in
    the directory but absent/stale in the local archive still gets its real
    submissions fetch. (Contrast S16's ``_make_zip_http_get``, which returns a
    ``404`` on miss because the drain records ``not_found`` via the manifest;
    S23's ``fetch_document_text`` contract maps ``None`` to "no work for this
    filer", so a miss MUST fall through to HTTP.)

    Caller owns the open :class:`zipfile.ZipFile` lifecycle (``try/finally``),
    mirroring ``_make_zip_http_get``.
    """

    def __init__(self, zf: zipfile.ZipFile, *, fallback: _DocFetcher) -> None:
        self._zf = zf
        self._fallback = fallback
        # The N-PORT bootstrap sweep (#1274) fans per-filer pipelines across a
        # thread pool that shares this one fetcher. ``zipfile.ZipFile.open``
        # shares the underlying file object's seek position, so concurrent
        # reads on one handle corrupt each other — serialise the local zip
        # read. The lock guards ONLY ``read_zip_entry``; the decode and HTTP
        # fallback delegation stay outside it (the SEC rate gate already
        # coordinates HTTP concurrency), and the zip read is a tiny fraction of
        # per-filer wall-clock, so this barely serialises the pipeline.
        self._read_lock = threading.Lock()

    def fetch_document_text(self, absolute_url: str) -> str | None:
        cik = match_primary_submissions_cik(absolute_url)
        if cik is None:
            # Non-primary URL (NPORT-P document body, secondary page, other).
            return self._fallback.fetch_document_text(absolute_url)
        entry_name = f"CIK{cik}.json"
        try:
            with self._read_lock:
                data = read_zip_entry(self._zf, entry_name)
        except (zipfile.BadZipFile, OSError) as exc:
            logger.warning(
                "submissions-zip: entry %s unreadable (%s) — delegating to HTTP",
                entry_name,
                exc,
            )
            return self._fallback.fetch_document_text(absolute_url)
        if data is None:
            # Member absent: trust newer than the archive (or stale archive).
            # Fall through to the real fetcher to preserve coverage.
            return self._fallback.fetch_document_text(absolute_url)
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            logger.warning(
                "submissions-zip: entry %s not valid utf-8 (%s) — delegating to HTTP",
                entry_name,
                exc,
            )
            return self._fallback.fetch_document_text(absolute_url)
