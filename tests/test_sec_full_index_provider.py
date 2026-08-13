"""Tests for `app/providers/implementations/sec_full_index.py` (G12).

Pure unit tests against a fake ``http_get`` callable. No DB fixture.

Pins the strict-by-default 404 contract: `read_master_idx` raises on 404
unless the caller passes `allow_404=True` (current-quarter-only case).
Previous-quarter 404s indicate SEC/network failure and MUST surface so
the sweep can record `QuarterStats(failed=True)` instead of silently
committing a zero-row walk.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.providers.implementations.sec_full_index import (
    _build_url,
    _quarter_start_date,
    read_master_idx,
)

_MASTER_IDX_SAMPLE = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    March 31, 2024
Comments:              webmaster@sec.gov

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|2024-01-15|edgar/data/320193/0000320193-24-000010.txt
1067983|Berkshire Hathaway Inc|13F-HR|2024-02-14|edgar/data/1067983/0001067983-24-000003.txt
789019|Microsoft Corp|10-K|2024-03-15|edgar/data/789019/0000789019-24-000022.txt
"""


def _fake_get(status: int, body: bytes):
    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return status, body

    return _impl


class TestBuildUrl:
    def test_canonical_shape(self) -> None:
        assert _build_url(2025, 3) == "https://www.sec.gov/Archives/edgar/full-index/2025/QTR3/master.idx"

    def test_rejects_quarter_below_one(self) -> None:
        with pytest.raises(ValueError, match="quarter must be 1..4, got 0"):
            _build_url(2025, 0)

    def test_rejects_quarter_above_four(self) -> None:
        with pytest.raises(ValueError, match="quarter must be 1..4, got 5"):
            _build_url(2025, 5)


class TestQuarterStartDate:
    def test_q1(self) -> None:
        assert _quarter_start_date(2024, 1) == date(2024, 1, 1)

    def test_q2(self) -> None:
        assert _quarter_start_date(2024, 2) == date(2024, 4, 1)

    def test_q3(self) -> None:
        assert _quarter_start_date(2024, 3) == date(2024, 7, 1)

    def test_q4(self) -> None:
        assert _quarter_start_date(2024, 4) == date(2024, 10, 1)


class TestReadMasterIdx:
    def test_happy_path_yields_three_rows(self) -> None:
        rows = list(read_master_idx(_fake_get(200, _MASTER_IDX_SAMPLE), 2024, 1))
        assert len(rows) == 3
        assert rows[0].cik == "0000320193"
        assert rows[0].form == "8-K"
        assert rows[0].source == "sec_8k"
        assert rows[1].cik == "0001067983"
        assert rows[1].form == "13F-HR"
        assert rows[1].source == "sec_13f_hr"
        assert rows[2].cik == "0000789019"
        assert rows[2].form == "10-K"
        assert rows[2].source == "sec_10k"

    def test_404_strict_default_raises(self) -> None:
        """Pin: strict-by-default 404 contract (Codex 1a r1 HIGH-2)."""
        with pytest.raises(RuntimeError, match=r"status=404 year=2024 quarter=1"):
            list(read_master_idx(_fake_get(404, b""), 2024, 1))

    def test_404_allow_404_true_yields_empty(self) -> None:
        rows = list(read_master_idx(_fake_get(404, b""), 2024, 1, allow_404=True))
        assert rows == []

    def test_non_200_non_404_raises(self) -> None:
        with pytest.raises(RuntimeError, match=r"status=503 year=2024 quarter=1"):
            list(read_master_idx(_fake_get(503, b""), 2024, 1))

    def test_default_filed_at_anchors_to_quarter_start(self) -> None:
        """Malformed date column → parser falls back to quarter-start, not today."""
        malformed_body = b"""\
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    March 31, 2024

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|8-K|NOT-A-DATE|edgar/data/320193/0000320193-24-000010.txt
"""
        rows = list(read_master_idx(_fake_get(200, malformed_body), 2024, 2))
        assert len(rows) == 1
        # Q2 2024 starts April 1; fallback anchors to that, NOT today/epoch.
        assert rows[0].filed_at == datetime(2024, 4, 1, tzinfo=UTC)
