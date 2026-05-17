"""Pure SEC full-index quarterly reader (G12).

Quarterly full-index files at
``https://www.sec.gov/Archives/edgar/full-index/{YYYY}/QTR{n}/master.idx``
list every filing accepted across the entire SEC universe for one
calendar quarter (~250-300k rows / ~50 MB).

Used by ``sec_master_idx_quarterly_sweep`` as a CROSS-QUARTER
SAFETY NET — catches accessions that Layer 1 (Atom fast lane) and
Layer 2 (daily-index reconcile) missed AND that Layer 3 (per-CIK
poll) cannot discover because the CIK is tombstoned / deactivated
/ no longer emitting submissions.json updates.

Format: pipe-delimited, identical schema to daily-index. Reuses
``parse_daily_index`` byte-for-byte.

404 contract: strict by default — only the JOB's current-quarter walk
should pass ``allow_404=True`` (newborn quarter / PAC-build pending).
Previous-quarter 404s indicate a network/SEC failure and MUST surface
so the sweep can record ``QuarterStats(failed=True)`` instead of
silently committing a zero-row walk.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from app.providers.implementations.sec_daily_index import HttpGet, parse_daily_index
from app.providers.implementations.sec_submissions import FilingIndexRow

logger = logging.getLogger(__name__)


def _quarter_start_date(year: int, quarter: int) -> date:
    """First day of the given quarter — used as the default
    ``filed_at`` anchor for any row whose date column is malformed."""
    if not 1 <= quarter <= 4:
        raise ValueError(f"quarter must be 1..4, got {quarter}")
    return date(year, (quarter - 1) * 3 + 1, 1)


def _build_url(year: int, quarter: int) -> str:
    if not 1 <= quarter <= 4:
        raise ValueError(f"quarter must be 1..4, got {quarter}")
    return f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"


def read_master_idx(
    http_get: HttpGet,
    year: int,
    quarter: int,
    *,
    user_agent: str = "eBull research/1.0 contact@example.com",
    allow_404: bool = False,
) -> Iterator[FilingIndexRow]:
    """Fetch + parse one quarter's master.idx.

    ``allow_404=False`` (default): 404 raises ``RuntimeError`` so the
    caller's per-quarter ``try/except`` records the failure. Only
    pass ``allow_404=True`` for the current calendar quarter (the only
    case where 404 = "not yet published"). Previous-quarter 404s
    indicate an SEC outage / typo / CDN failure and must NOT be
    silenced.

    Raises ``RuntimeError`` on any other non-200 status.
    """
    url = _build_url(year, quarter)
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    }
    status, body = http_get(url, headers)
    if status == 404:
        if allow_404:
            logger.info(
                "master.idx not yet published for %sQ%s (404; allowed)",
                year,
                quarter,
            )
            return
            yield  # pragma: no cover — keeps signature as Iterator
        raise RuntimeError(
            f"master.idx fetch failed: status=404 year={year} quarter={quarter} "
            f"(allow_404=False; previous-quarter 404 indicates SEC/network failure)"
        )
    if status != 200:
        raise RuntimeError(f"master.idx fetch failed: status={status} year={year} quarter={quarter}")
    yield from parse_daily_index(body, default_filed_at=_quarter_start_date(year, quarter))
