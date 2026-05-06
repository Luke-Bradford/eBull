"""Tests for the pure submissions.json + daily-index providers (#866).

Covers:

- ``parse_submissions_page`` against the AAPL recent block shape
- ``check_freshness`` watermark short-circuit (returns only rows newer
  than ``last_known_filing_id``)
- ``check_freshness`` source filter
- ``check_freshness`` 404 returns empty delta
- Pagination signal (``has_more_in_files``)
- ``parse_daily_index`` against an SEC daily-index sample
- ``read_daily_index`` 404 returns empty iterator
- ``_accession_from_filename`` reconstructs the dashed form
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from app.providers.implementations.sec_daily_index import (
    _accession_from_filename,
    parse_daily_index,
    read_daily_index,
)
from app.providers.implementations.sec_submissions import (
    check_freshness,
    parse_submissions_page,
)

# ---------------------------------------------------------------------------
# Submissions parser
# ---------------------------------------------------------------------------


def _aapl_recent_block() -> dict[str, object]:
    """Minimal AAPL submissions.json shape for testing the parser."""
    return {
        "cik": "320193",
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0000320193-26-000003",
                    "0000320193-26-000002",
                    "0000320193-26-000001",
                ],
                "filingDate": ["2026-03-15", "2026-02-14", "2026-01-15"],
                "form": ["8-K", "DEF 14A", "4"],
                "acceptanceDateTime": [
                    "2026-03-15T16:30:00.000Z",
                    "2026-02-14T08:00:00.000Z",
                    "2026-01-15T17:45:00.000Z",
                ],
                "primaryDocument": ["item502.htm", "proxy.htm", "form4.xml"],
            },
            "files": [],
        },
    }


def _aapl_paginated_block() -> dict[str, object]:
    """Submissions.json with files[] pagination — older filings."""
    block = _aapl_recent_block()
    block["filings"] = dict(block["filings"])  # type: ignore[arg-type]
    block["filings"]["files"] = [{"name": "CIK0000320193-submissions-001.json"}]  # type: ignore[index]
    return block


class TestParseSubmissionsPage:
    def test_parses_recent_block_into_rows(self) -> None:
        rows, has_more = parse_submissions_page(_aapl_recent_block(), cik="320193")
        assert len(rows) == 3
        assert has_more is False

        # Rows in declared order (newest first per SEC convention)
        assert rows[0].accession_number == "0000320193-26-000003"
        assert rows[0].form == "8-K"
        assert rows[0].source == "sec_8k"
        assert rows[0].filed_at == datetime(2026, 3, 15, 16, 30, tzinfo=UTC)
        assert rows[0].cik == "0000320193"
        assert rows[0].is_amendment is False

        assert rows[1].source == "sec_def14a"
        assert rows[2].source == "sec_form4"

    def test_pagination_flag(self) -> None:
        _, has_more = parse_submissions_page(_aapl_paginated_block(), cik="320193")
        assert has_more is True

    def test_amendment_flag(self) -> None:
        block = _aapl_recent_block()
        block["filings"]["recent"]["form"] = ["4/A", "DEF 14A", "4"]  # type: ignore[index]
        rows, _ = parse_submissions_page(block, cik="320193")
        assert rows[0].is_amendment is True
        assert rows[1].is_amendment is False
        assert rows[0].source == "sec_form4"  # /A still maps to base source

    def test_amendment_flag_recognises_non_suffix_forms(self) -> None:
        # Regression for #939. ``DEFA14A`` / ``DEFR14A`` are amendments
        # of ``DEF 14A`` that encode the amendment in the form code
        # rather than via a ``/A`` suffix. The submissions parser used
        # ``form.endswith("/A")`` which silently mis-classified these
        # as initial filings, breaking supersession in
        # ``ownership_*_current``. Route every amendment check through
        # ``app.services.sec_manifest.is_amendment_form``.
        # Also asserts ``source == "sec_def14a"`` for both: a missing
        # ``DEFR14A`` mapping in ``_FORM_TO_SOURCE`` would let
        # downstream ``record_manifest_entry`` callers silently drop
        # the row even with the amendment flag corrected.
        block = _aapl_recent_block()
        block["filings"]["recent"]["form"] = ["DEFA14A", "DEFR14A", "4"]  # type: ignore[index]
        rows, _ = parse_submissions_page(block, cik="320193")
        assert rows[0].is_amendment is True
        assert rows[0].source == "sec_def14a"
        assert rows[1].is_amendment is True
        assert rows[1].source == "sec_def14a"
        assert rows[2].is_amendment is False
        assert rows[2].source == "sec_form4"

    def test_skips_unmapped_forms(self) -> None:
        # ``S-1``, ``CORRESP`` etc are not in the manifest source set.
        # They DO produce rows (we still see the form), but ``source`` is None.
        block = _aapl_recent_block()
        block["filings"]["recent"]["form"] = ["S-1", "CORRESP", "4"]  # type: ignore[index]
        rows, _ = parse_submissions_page(block, cik="320193")
        assert rows[0].source is None
        assert rows[1].source is None
        assert rows[2].source == "sec_form4"

    def test_accepts_bytes_payload(self) -> None:
        body = json.dumps(_aapl_recent_block()).encode("utf-8")
        rows, _ = parse_submissions_page(body, cik="320193")
        assert len(rows) == 3

    def test_primary_document_url_built(self) -> None:
        rows, _ = parse_submissions_page(_aapl_recent_block(), cik="320193")
        assert rows[0].primary_document_url == (
            "https://www.sec.gov/Archives/edgar/data/320193/000032019326000003/item502.htm"
        )

    def test_parses_secondary_page_with_arrays_at_top_level(self) -> None:
        # SEC's ``CIKNNN-submissions-NNN.json`` secondary pages carry
        # the parallel arrays at the top of the doc (no wrapping
        # ``filings.recent``). Codex pre-push review #866 — without
        # this branch, first-install / rebuild pagination silently
        # dropped all older filings.
        secondary_page = {
            "accessionNumber": ["0000320193-20-000001"],
            "filingDate": ["2020-01-15"],
            "form": ["10-K"],
            "acceptanceDateTime": ["2020-01-15T16:30:00.000Z"],
            "primaryDocument": ["aapl-20200115.htm"],
        }
        rows, has_more = parse_submissions_page(secondary_page, cik="320193")
        assert len(rows) == 1
        assert rows[0].accession_number == "0000320193-20-000001"
        assert rows[0].source == "sec_10k"
        assert has_more is False

    def test_defm14a_maps_to_def14a(self) -> None:
        # Codex pre-push review #866 — the existing DEF 14A ingester
        # (app/services/def14a_ingest.py) treats ``DEFM14A`` (merger
        # proxy) as ingestible; the manifest mapping must agree.
        block = _aapl_recent_block()
        block["filings"]["recent"]["form"] = ["DEFM14A", "DEF 14A", "4"]  # type: ignore[index]
        rows, _ = parse_submissions_page(block, cik="320193")
        assert rows[0].source == "sec_def14a"
        assert rows[1].source == "sec_def14a"


# ---------------------------------------------------------------------------
# check_freshness
# ---------------------------------------------------------------------------


def _fake_get(status: int, body: dict[str, object] | bytes):
    """Build a fake HttpGet that always returns the given response."""

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        if isinstance(body, dict):
            return status, json.dumps(body).encode("utf-8")
        return status, body

    return _impl


class TestCheckFreshness:
    def test_no_watermark_returns_all_rows(self) -> None:
        delta = check_freshness(_fake_get(200, _aapl_recent_block()), cik="320193")
        assert len(delta.new_filings) == 3
        assert delta.cik == "0000320193"
        assert delta.has_more_in_files is False

    def test_watermark_filters_to_strictly_newer(self) -> None:
        delta = check_freshness(
            _fake_get(200, _aapl_recent_block()),
            cik="320193",
            last_known_filing_id="0000320193-26-000002",
        )
        # Only the 2026-03-15 filing is newer than the watermark
        assert [r.accession_number for r in delta.new_filings] == ["0000320193-26-000003"]

    def test_watermark_not_in_response_returns_all(self) -> None:
        # Caller's watermark predates the recent array — recover by
        # returning everything; manifest UPSERT is idempotent.
        delta = check_freshness(
            _fake_get(200, _aapl_recent_block()),
            cik="320193",
            last_known_filing_id="0000320193-99-999999",
        )
        assert len(delta.new_filings) == 3

    def test_source_filter(self) -> None:
        delta = check_freshness(
            _fake_get(200, _aapl_recent_block()),
            cik="320193",
            sources={"sec_form4"},
        )
        assert [r.accession_number for r in delta.new_filings] == ["0000320193-26-000001"]

    def test_404_returns_empty_delta(self) -> None:
        delta = check_freshness(_fake_get(404, b""), cik="999999999")
        assert delta.new_filings == []
        assert delta.last_filed_at is None

    def test_non_404_non_200_raises(self) -> None:
        with pytest.raises(RuntimeError, match="status=503"):
            check_freshness(_fake_get(503, b""), cik="320193")


# ---------------------------------------------------------------------------
# Daily index parser
# ---------------------------------------------------------------------------


_DAILY_INDEX_SAMPLE = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    April 30, 2026
Comments:              webmaster@sec.gov

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2026-04-30|edgar/data/320193/0000320193-26-000042.txt
320193|Apple Inc.|4|2026-04-30|edgar/data/320193/0000320193-26-000043.txt
1364742|BLACKROCK INC.|13F-HR|2026-04-30|edgar/data/1364742/0001364742-26-000099.txt
0|Bad Row|||
99999|Bad Row 2|FORM|||
"""


class TestDailyIndex:
    def test_accession_extraction(self) -> None:
        assert _accession_from_filename("edgar/data/320193/0000320193-26-000042.txt") == "0000320193-26-000042"
        # Already-dashed accession in path
        assert _accession_from_filename("edgar/data/320193/0000320193-26-000042.txt") == "0000320193-26-000042"

    def test_accession_extraction_rejects_short(self) -> None:
        assert _accession_from_filename("edgar/data/x/short.txt") is None
        assert _accession_from_filename("") is None

    def test_parses_data_rows_skips_bad(self) -> None:
        rows = list(parse_daily_index(_DAILY_INDEX_SAMPLE, default_filed_at=date(2026, 4, 30)))
        assert len(rows) == 3
        forms = {r.form for r in rows}
        assert forms == {"8-K", "4", "13F-HR"}

    def test_zero_pads_cik(self) -> None:
        rows = list(parse_daily_index(_DAILY_INDEX_SAMPLE, default_filed_at=date(2026, 4, 30)))
        for row in rows:
            assert len(row.cik) == 10
            assert row.cik.isdigit()

    def test_maps_source(self) -> None:
        rows = list(parse_daily_index(_DAILY_INDEX_SAMPLE, default_filed_at=date(2026, 4, 30)))
        sources = {r.source for r in rows}
        assert sources == {"sec_8k", "sec_form4", "sec_13f_hr"}

    def test_amendment_flag_recognises_non_suffix_forms(self) -> None:
        # Regression for #939. The daily-index parser used
        # ``form.endswith("/A")`` which silently missed non-suffix
        # amendments like ``DEFA14A`` / ``DEFR14A``. Route every
        # amendment check through
        # ``app.services.sec_manifest.is_amendment_form``.
        sample = (
            b"Description: Master Index of EDGAR Dissemination Feed\n"
            b"\n"
            b"CIK|Company Name|Form Type|Date Filed|Filename\n"
            b"--------------------------------------------------------------------------------\n"
            b"320193|Apple Inc.|DEFA14A|2026-04-30|edgar/data/320193/0000320193-26-000044.txt\n"
            b"320193|Apple Inc.|DEFR14A|2026-04-30|edgar/data/320193/0000320193-26-000045.txt\n"
            b"320193|Apple Inc.|4/A|2026-04-30|edgar/data/320193/0000320193-26-000046.txt\n"
            b"320193|Apple Inc.|4|2026-04-30|edgar/data/320193/0000320193-26-000047.txt\n"
        )
        rows = list(parse_daily_index(sample, default_filed_at=date(2026, 4, 30)))
        assert len(rows) == 4
        amendment_by_form = {r.form: r.is_amendment for r in rows}
        assert amendment_by_form == {
            "DEFA14A": True,
            "DEFR14A": True,
            "4/A": True,
            "4": False,
        }
        # Also assert source mapping so a missing ``DEFR14A`` entry in
        # ``_FORM_TO_SOURCE`` would fail this test. Without the
        # mapping, daily-index reconciliation would silently drop the
        # row even with the amendment flag corrected.
        source_by_form = {r.form: r.source for r in rows}
        assert source_by_form == {
            "DEFA14A": "sec_def14a",
            "DEFR14A": "sec_def14a",
            "4/A": "sec_form4",
            "4": "sec_form4",
        }

    def test_filed_at_from_row_date(self) -> None:
        rows = list(parse_daily_index(_DAILY_INDEX_SAMPLE, default_filed_at=date(2026, 4, 30)))
        for row in rows:
            assert row.filed_at == datetime(2026, 4, 30, tzinfo=UTC)


class TestReadDailyIndex:
    def test_404_returns_empty_iterator(self) -> None:
        rows = list(read_daily_index(_fake_get(404, b""), date(2026, 4, 30)))
        assert rows == []

    def test_200_parses_rows(self) -> None:
        rows = list(read_daily_index(_fake_get(200, _DAILY_INDEX_SAMPLE), date(2026, 4, 30)))
        assert len(rows) == 3

    def test_non_404_non_200_raises(self) -> None:
        with pytest.raises(RuntimeError, match="status=500"):
            list(read_daily_index(_fake_get(500, b""), date(2026, 4, 30)))
