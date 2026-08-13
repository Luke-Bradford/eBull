"""Top 13F-HR filer discovery from SEC's quarterly form indexes.

Operator audit 2026-05-03 (issue #807) flagged the
``institutional_filer_seeds`` table at 14 rows vs the ~5,400-row
13F-HR universe. The pie chart on the ownership card silently
omits institutional ownership for any issuer not held by one of
those 14 filers.

Approach: SEC publishes quarterly ``form.idx`` files at
``https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{n}/form.idx``.
Each file is a fixed-width text index of every form filed in that
quarter. Aggregating the 13F-HR rows by filer CIK over the last N
quarters surfaces ACTIVE 13F filers without an inventive crawler.

Limitations of the count-based ranking:

  * **Filing count is not AUM.** A small filer who files many
    13F-HR/A amendments per quarter outranks a large filer who
    files one clean 13F-HR per quarter. The household-name top
    managers (Vanguard, BlackRock, Fidelity, State Street) file
    once per quarter and rank low on count.
  * The truly AUM-based ranking requires fetching each filer's
    primary_doc.xml (carries ``reportSummary/tableValueTotal``)
    and ranking by that value. Tracked as the AUM-ranking
    follow-up; this discovery primitive feeds into it.

For now this module is useful as:

  * A "is filer X actively filing right now?" check.
  * A long-tail discovery surface — the operator can review the
    top-200 by count, hand-pick names not yet in the curated 14
    seeds, and apply through the verification gate.

The expansion path:

  1. ``aggregate_top_filers(quarters=N, top_n=200)`` walks the
     last N quarterly form.idx files (default 4 = one full year).
  2. Each entry yields ``(cik, latest_name, filing_count)``
     sorted by ``filing_count`` desc.
  3. The CLI in ``scripts/seed_top_13f_filers.py`` runs the
     filer_seed_verification gate against each candidate before
     persisting via ``seed_filer`` — same gate from PR #821.

This module is the data-fetch + aggregation half. Persistence
lives in the CLI so the operator can review candidates before
applying.
"""

from __future__ import annotations

import logging
import re
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date

from app.config import settings

logger = logging.getLogger(__name__)


_FORM_IDX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/form.idx"

# Form types we treat as 13F-HR for the count. ``13F-HR/A`` is an
# amendment of a previously-filed 13F-HR; both signal active filer
# status. ``13F-NT`` is a notice-only "another manager has all my
# holdings" filing — counted because it's still operator-relevant
# (a no-positions notice still indicates an active manager).
_THIRTEEN_F_FORM_TYPES: frozenset[str] = frozenset({"13F-HR", "13F-HR/A", "13F-NT", "13F-NT/A"})


@dataclass(frozen=True)
class FormIndexEntry:
    """One row from form.idx."""

    form_type: str
    company_name: str
    cik: str  # 10-digit zero-padded
    date_filed: date
    file_name: str


@dataclass(frozen=True)
class TopFilerCandidate:
    """One aggregated candidate. ``filing_count`` is the count of
    13F-HR / 13F-HR/A / 13F-NT filings observed across the
    aggregated quarters."""

    cik: str  # 10-digit zero-padded
    latest_name: str
    filing_count: int


def fetch_form_index(year: int, quarter: int) -> str:
    """Fetch one quarterly form.idx as latin-1 text. Raises on
    network / decode failure — caller decides whether to retry or
    skip the quarter.

    The file is ~50MB; SEC serves it cacheable, so a periodic
    weekly refresh is the operator-intended cadence.
    """
    url = _FORM_IDX_URL.format(year=year, q=quarter)
    req = urllib.request.Request(url, headers={"User-Agent": settings.sec_user_agent})
    with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310 — fixed SEC URL
        # form.idx historically uses latin-1 encoding for company
        # names with non-ASCII characters; using utf-8 raises on
        # certain rows. SEC's archive convention.
        return resp.read().decode("latin-1")


# Header marker that identifies the start of data rows. Everything
# above it is preamble / column headers / dashes.
_DATA_MARKER = re.compile(r"^-+\s*$")


def parse_form_index(payload: str) -> Iterable[FormIndexEntry]:
    """Yield :class:`FormIndexEntry` per data row.

    Strategy: anchor on the predictable trailing fields (CIK =
    digits, Date Filed = ``YYYY-MM-DD``, File Name =
    ``edgar/...``). Everything before is "form_type + spaces +
    company_name"; split on the LAST 2+-space gap to recover the
    two parts. This is robust to:

      * Form types that contain spaces (``1-A POS``, ``25-NSE``).
      * Company names with internal spaces.
      * Column-width drift across years (SEC widened the Company
        Name column when issuers started exceeding 60 chars).
    """
    line_pattern = re.compile(
        r"^(?P<head>.+?)\s+"
        r"(?P<cik>\d+)\s+"
        r"(?P<date>\d{4}-\d{2}-\d{2})\s+"
        r"(?P<file>edgar/\S+)\s*$"
    )
    # Form type + Company name are separated by at least 2 spaces.
    # rsplit guards against the possibility that a form type
    # contains a single space ("1-A POS"); SEC keeps a 2+-space
    # gap before the company name as the canonical boundary.
    head_split = re.compile(r"^(?P<form>.+?)\s{2,}(?P<name>.+?)\s*$")

    in_data = False
    for line in payload.splitlines():
        if not in_data:
            if _DATA_MARKER.match(line):
                in_data = True
            continue
        m = line_pattern.match(line)
        if m is None:
            continue
        head_m = head_split.match(m.group("head"))
        if head_m is None:
            continue
        try:
            cik_padded = f"{int(m.group('cik')):010d}"
            filed = date.fromisoformat(m.group("date"))
        except ValueError:
            continue
        yield FormIndexEntry(
            form_type=head_m.group("form").strip(),
            company_name=head_m.group("name").strip(),
            cik=cik_padded,
            date_filed=filed,
            file_name=m.group("file").strip(),
        )


def aggregate_top_filers(
    quarters: list[tuple[int, int]],
    *,
    top_n: int = 150,
    fetch: Callable[[int, int], str] = fetch_form_index,
) -> list[TopFilerCandidate]:
    """Fetch each quarter's form.idx, sum 13F-HR filing counts per
    CIK, return the top ``top_n`` candidates sorted by count desc.

    ``fetch`` is injectable for tests so the aggregator can be
    exercised against a fake.

    A per-quarter fetch failure is logged and skipped — partial
    coverage is preferable to aborting the whole sweep.
    """
    counts: dict[str, int] = defaultdict(int)
    # Track the date_filed of the name we've recorded so far, so
    # we keep the name from the LATEST observed filing — robust
    # against caller passing quarters in any order. Codex pre-push
    # review caught a previous bug where iteration order determined
    # the outcome.
    latest_name: dict[str, str] = {}
    latest_filed: dict[str, date] = {}

    for year, q in quarters:
        try:
            payload = fetch(year, q)
        except Exception:  # noqa: BLE001 — per-quarter failure isolation
            logger.exception("top_filer_discovery: form.idx fetch failed for %sQ%s", year, q)
            continue
        for entry in parse_form_index(payload):
            if entry.form_type not in _THIRTEEN_F_FORM_TYPES:
                continue
            counts[entry.cik] += 1
            prior = latest_filed.get(entry.cik)
            if prior is None or entry.date_filed > prior:
                latest_name[entry.cik] = entry.company_name
                latest_filed[entry.cik] = entry.date_filed

    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [
        TopFilerCandidate(cik=cik, latest_name=latest_name[cik], filing_count=count) for cik, count in ranked[:top_n]
    ]
