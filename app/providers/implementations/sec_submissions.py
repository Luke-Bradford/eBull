"""Pure SEC submissions.json reader for the freshness scheduler (#866).

Issue #866 / spec §"#865 — submissions.json + daily-index readers"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

Distinct from the ``sec_edgar.SecFilingsProvider.fetch_submissions``
helper: this module is the small purely-functional surface the
freshness scheduler (#869 worker, #870 per-CIK polling, #871 first-
install drain) calls. The legacy provider still owns the FilingsProvider
contract for the operator filings UI; this layer is the new ETL plumbing.

The function the worker calls is ``check_freshness``:

    delta = check_freshness(
        http_get,
        cik="0000320193",
        last_known_filing_id="0000320193-25-000142",
        sources={"sec_form4", "sec_def14a"},
    )

``http_get`` is any callable ``(url, headers) -> tuple[int, bytes]`` so
the module is HTTP-client-agnostic — tests pass a fake; production
passes a wrapper around ``ResilientClient``.

Codex review v2 finding 7: ``check_freshness`` takes
``last_known_filing_id`` as an explicit arg. The provider stays pure;
the scheduler handles the DB lookup of the watermark.

submissions.json shape (truncated):

    {
      "cik": "320193",
      "filings": {
        "recent": {
          "accessionNumber": ["0000320193-26-000001", ...],
          "filingDate":      ["2026-01-15", ...],
          "form":            ["8-K", ...],
          "acceptanceDateTime": ["2026-01-15T16:30:00.000Z", ...],
          "primaryDocument": ["0000320193-26-000001-index.htm", ...]
        },
        "files": [
          { "name": "CIK0000320193-submissions-001.json",
            "filingFrom": "...", "filingTo": "..." }
        ]
      }
    }

The ``recent`` block carries the most-recent ~1000 accessions inline.
Older filings live in the secondary pages named in ``files[]``;
first-install / targeted-rebuild paths follow those pages.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.services.sec_manifest import (
    ManifestSource,
    is_amendment_form,
    map_form_to_source,
)

logger = logging.getLogger(__name__)


HttpGet = Callable[[str, dict[str, str]], tuple[int, bytes]]
"""HTTP getter: ``(url, headers) -> (status, body)``. Implementations
must respect the SEC 10 req/s fair-use cap externally — this module
makes no rate-limiting assumptions."""


@dataclass(frozen=True)
class FilingIndexRow:
    """One discovered filing — minimal shape carried across the
    discovery layer (submissions.json, daily-index, getcurrent feed).
    Maps cleanly into ``record_manifest_entry`` kwargs."""

    accession_number: str
    cik: str
    form: str
    source: ManifestSource | None
    # ``None`` when the form is not in the manifest source set
    # (e.g. ``S-1``, ``CORRESP``); discovery layer can skip these.
    filed_at: datetime
    accepted_at: datetime | None
    primary_document_url: str | None
    is_amendment: bool


@dataclass(frozen=True)
class FreshnessDelta:
    """Result of a per-CIK freshness check.

    ``new_filings`` are accessions strictly newer than
    ``last_known_filing_id`` (or all when watermark is None / not in
    response). ``has_more_in_files`` is true when SEC's submissions.json
    paginates older filings into ``files[]`` — meaningful for
    first-install / rebuild paths only.

    ``files_pages`` carries the names of those secondary pages
    extracted from the same primary fetch. Callers (#936 rebuild)
    walk these without re-issuing the primary request — pre-#959
    rewrite, ``_walk_secondary_pages`` re-fetched the primary CIK
    JSON purely to read ``filings.files[]`` again, doubling the
    request count + creating a new failure mode."""

    cik: str
    new_filings: list[FilingIndexRow]
    last_filed_at: datetime | None
    has_more_in_files: bool
    files_pages: list[str] = field(default_factory=list)


def _zero_pad_cik(cik: str) -> str:
    """SEC submissions.json paths use 10-digit zero-padded CIK."""
    return cik.zfill(10)


def _parse_filed_at(filing_date: str, accepted: str | None) -> datetime:
    """Pick the most precise filing timestamp available.

    Prefer ``acceptanceDateTime`` (carries time-of-day in UTC) over
    ``filingDate`` (date-only). When only the date is available we
    anchor to 00:00 UTC — the freshness scheduler's cadence ceilings
    swallow the ~24h imprecision."""
    if accepted:
        # SEC emits ISO-ish strings; tolerate a trailing 'Z'.
        s = accepted.rstrip("Z")
        return datetime.fromisoformat(s).replace(tzinfo=UTC)
    return datetime.fromisoformat(filing_date).replace(tzinfo=UTC)


def parse_submissions_page(
    body: dict[str, Any] | bytes | str,
    *,
    cik: str,
) -> tuple[list[FilingIndexRow], bool]:
    """Parse one submissions.json page (either the primary ``recent``
    block or a ``files[]`` secondary page) into rows.

    Both shapes carry the same parallel arrays — accessionNumber +
    filingDate + form + acceptanceDateTime + primaryDocument — so the
    pagination layer can call this per page without re-fetching.

    Returns ``(rows, has_more_in_files)``. ``has_more_in_files`` is
    only ever true on the primary page; secondary pages always return
    False.
    """
    if isinstance(body, (bytes, str)):
        payload: dict[str, Any] = json.loads(body)
    else:
        payload = body

    # Two valid shapes (Codex pre-push review #866):
    # 1. Primary page (``data.sec.gov/submissions/CIKNNN.json``):
    #    top-level dict with ``filings.recent`` carrying the parallel
    #    arrays + optional ``filings.files[]`` for older pages.
    # 2. Secondary page (``CIKNNN-submissions-NNN.json``): parallel
    #    arrays at the TOP level — no wrapping ``filings.recent``.
    # Detect whichever shape is present so first-install / rebuild
    # pagination doesn't silently drop older filings.
    if "accessionNumber" in payload:
        recent: dict[str, Any] = payload
        filings: dict[str, Any] = {}
    else:
        filings = payload.get("filings", {}) or {}
        recent = filings.get("recent", {}) or {}

    accessions: list[str] = recent.get("accessionNumber") or []
    filing_dates: list[str] = recent.get("filingDate") or []
    forms: list[str] = recent.get("form") or []
    accepted: list[str] = recent.get("acceptanceDateTime") or []
    primary_docs: list[str] = recent.get("primaryDocument") or []

    cik_padded = _zero_pad_cik(cik)
    rows: list[FilingIndexRow] = []
    for i, accession in enumerate(accessions):
        if not accession:
            continue
        form = (forms[i] if i < len(forms) else "").strip()
        if not form:
            continue
        filing_date = (filing_dates[i] if i < len(filing_dates) else "").strip()
        if not filing_date:
            continue
        accepted_str = (accepted[i] if i < len(accepted) else None) or None
        primary_doc = (primary_docs[i] if i < len(primary_docs) else None) or None
        primary_url: str | None = None
        if primary_doc:
            stripped_acc = accession.replace("-", "")
            primary_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik_padded)}/{stripped_acc}/{primary_doc}"

        rows.append(
            FilingIndexRow(
                accession_number=accession,
                cik=cik_padded,
                form=form,
                source=map_form_to_source(form),
                filed_at=_parse_filed_at(filing_date, accepted_str),
                accepted_at=(
                    datetime.fromisoformat(accepted_str.rstrip("Z")).replace(tzinfo=UTC) if accepted_str else None
                ),
                primary_document_url=primary_url,
                is_amendment=is_amendment_form(form),
            )
        )

    has_more = bool(filings.get("files") or [])
    return rows, has_more


def check_freshness(
    http_get: HttpGet,
    *,
    cik: str,
    last_known_filing_id: str | None = None,
    sources: Iterable[ManifestSource] | None = None,
    user_agent: str = "eBull research/1.0 contact@example.com",
) -> FreshnessDelta:
    """Pure freshness probe for one CIK against SEC's submissions.json.

    Caller (the freshness scheduler) supplies the last-known accession
    so we can short-circuit returning an empty delta when nothing newer
    has been filed. ``sources`` filters by manifest source enum — pass
    ``{'sec_form4', 'sec_def14a'}`` to trim the result for a per-source
    poll.

    Pagination (``filings.files[]``): NOT followed here. The recent
    array carries enough for steady-state polling; first-install drain
    + targeted rebuild call ``parse_submissions_page`` on each
    secondary page directly.
    """
    cik_padded = _zero_pad_cik(cik)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    }

    status, body = http_get(url, headers)
    if status == 404:
        return FreshnessDelta(cik=cik_padded, new_filings=[], last_filed_at=None, has_more_in_files=False)
    if status != 200:
        raise RuntimeError(f"submissions.json fetch failed: status={status} cik={cik_padded}")

    rows, has_more = parse_submissions_page(body, cik=cik_padded)
    if sources is not None:
        wanted = set(sources)
        rows = [r for r in rows if r.source is not None and r.source in wanted]

    # Extract ``filings.files[*].name`` from the same primary body so
    # ``_walk_secondary_pages`` (#936) does not re-fetch the primary
    # JSON. Skip silently on JSON-decode error — the row parse above
    # already succeeded, so the body is well-formed for the rows path;
    # the files block is best-effort secondary metadata.
    files_pages: list[str] = []
    try:
        payload = json.loads(body) if isinstance(body, (bytes, str)) else body
        if isinstance(payload, dict):
            files_meta = (payload.get("filings", {}) or {}).get("files", []) or []
            for meta in files_meta:
                if isinstance(meta, dict):
                    name = meta.get("name")
                    if isinstance(name, str) and name:
                        files_pages.append(name)
    except json.JSONDecodeError, TypeError:
        pass

    # Filter to strictly newer than the watermark. SEC's recent array
    # is ordered newest-first; we walk until we hit the watermark and
    # stop — preserves chronological order in the result and avoids
    # double-recording on amendments that share an accession family.
    new_filings: list[FilingIndexRow] = []
    if last_known_filing_id is None:
        new_filings = rows
    else:
        for row in rows:
            if row.accession_number == last_known_filing_id:
                break
            new_filings.append(row)
        else:
            # Watermark not in response: either it's old enough to
            # have rolled into the secondary pages, or the caller's
            # watermark is wrong. Return everything; the scheduler
            # will UPSERT the manifest (idempotent on accession PK)
            # and the next poll will see the new newest as the
            # watermark.
            new_filings = rows

    last_filed_at = max((r.filed_at for r in rows), default=None)
    return FreshnessDelta(
        cik=cik_padded,
        new_filings=new_filings,
        last_filed_at=last_filed_at,
        has_more_in_files=has_more,
        files_pages=files_pages,
    )
