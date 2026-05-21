"""Tests for the 13D/G blockholders submissions-index parser (#766 PR 2).

The legacy seed-walking ingester (``ingest_all_active_filers``,
``ingest_filer_blockholders``, ``seed_filer``) was retired in #1233
PR11 Task 8.2 — the manifest-worker path
(:mod:`app.services.manifest_parsers.sec_13dg`) is now the sole
production write path. Coverage for that surface lives in
``tests/test_manifest_parser_sec_13dg.py``; this file retains only
the pure-function coverage for :func:`parse_submissions_index`,
which is still consumed by tests + downstream tooling.

Aggregator coverage for :func:`latest_blockholder_positions` is
exercised end-to-end via ``tests/test_api_blockholders.py``, which
drives data directly into ``blockholder_filings`` and hits the
``/blockholders`` HTTP path.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from app.services.blockholders import (
    parse_submissions_index,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixture builders — minimal SEC payloads
# ---------------------------------------------------------------------------


def _submissions_json(*, accessions: list[tuple[str, str, str]]) -> str:
    """Build a fake submissions JSON. Each tuple is
    ``(accession, form, filing_date)``."""
    return json.dumps(
        {
            "filings": {
                "recent": {
                    "accessionNumber": [a[0] for a in accessions],
                    "form": [a[1] for a in accessions],
                    "filingDate": [a[2] for a in accessions],
                },
                "files": [],
            }
        }
    )

# ---------------------------------------------------------------------------
# Pure-parser tests (no DB)
# ---------------------------------------------------------------------------


class TestParseSubmissionsIndex:
    def test_filters_to_13dg_forms_only(self) -> None:
        payload = _submissions_json(
            accessions=[
                ("0001234567-25-000001", "SC 13D", "2025-11-06"),
                ("0001234567-25-000002", "10-K", "2025-09-15"),
                ("0001234567-25-000003", "SC 13D/A", "2025-11-15"),
                ("0001234567-25-000004", "SC 13G", "2025-10-01"),
                ("0001234567-25-000005", "SC 13G/A", "2025-10-15"),
                ("0001234567-25-000006", "13F-HR", "2025-11-14"),
            ]
        )
        refs = parse_submissions_index(payload)
        assert refs is not None
        assert len(refs) == 4
        assert {r.filing_type for r in refs} == {"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}

    def test_accepts_long_form_schedule_labels(self) -> None:
        """Real-world SEC submissions JSON uses the long ``SCHEDULE
        13D`` form for post-BOM-rule filings (post-2024-12-19) —
        verified against Carl Icahn submissions. The filter must
        accept both short and long forms or every modern filing
        is silently skipped."""
        payload = _submissions_json(
            accessions=[
                ("0000921669-25-000001", "SCHEDULE 13D", "2025-03-01"),
                ("0000921669-25-000002", "SCHEDULE 13D/A", "2025-04-01"),
                ("0000921669-25-000003", "SCHEDULE 13G", "2025-05-01"),
                ("0000921669-25-000004", "SCHEDULE 13G/A", "2025-06-01"),
            ]
        )
        refs = parse_submissions_index(payload)
        assert refs is not None
        assert len(refs) == 4
        assert {r.filing_type for r in refs} == {
            "SCHEDULE 13D",
            "SCHEDULE 13D/A",
            "SCHEDULE 13G",
            "SCHEDULE 13G/A",
        }

    def test_filed_at_is_utc_tz_aware(self) -> None:
        payload = _submissions_json(accessions=[("0001234567-25-000001", "SC 13D", "2025-11-06")])
        refs = parse_submissions_index(payload)
        assert refs is not None
        ref = refs[0]
        assert ref.filed_at == datetime(2025, 11, 6, tzinfo=UTC)
        assert ref.filed_at is not None and ref.filed_at.tzinfo is UTC

    def test_malformed_json_returns_none(self) -> None:
        """Malformed payload returns None (not []) so the ingester can
        distinguish ``no 13D/G filings on file`` (legitimate empty
        list) from ``cannot parse the index`` (treat as a failure).
        """
        assert parse_submissions_index("not json") is None

    def test_missing_recent_returns_empty_list(self) -> None:
        """Valid JSON with no 'recent' array is a legitimate empty
        result — distinct from malformed JSON above."""
        assert parse_submissions_index('{"filings": {}}') == []
