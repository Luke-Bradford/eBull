"""Pure-logic tests for the ownership-history coverage-coherence envelope
and the reject-NT decision (#1648).

Fast tier (no DB): the summariser is pure over the points, and the
reject-NT guard introspects the reader source. The behavioural reject-NT
pin (a filer with a later NT still appears in the aggregate history) is the
dev-verify step in the PR — synthetic seeds can't beat the AAPL Vanguard
cross-check (2025-06-30 ≈ 4,672M unchanged, NOT 3,319M).
"""

from __future__ import annotations

import inspect
from datetime import UTC, date, datetime
from decimal import Decimal

from app.services import ownership_history
from app.services.ownership_history import (
    AggregateCoverage,
    OwnershipHistoryPoint,
    summarise_aggregate_coverage,
)

_FILED = datetime(2026, 5, 1, tzinfo=UTC)


def _pt(period_end: date, holder_count: int | None, *, shares: str = "1000") -> OwnershipHistoryPoint:
    return OwnershipHistoryPoint(
        period_end=period_end,
        ownership_nature="economic",
        shares=Decimal(shares),
        source="13f",
        source_accession=None,
        filed_at=_FILED,
        holder_count=holder_count,
    )


class TestSummariseAggregateCoverage:
    def test_empty_series_is_empty(self) -> None:
        cov = summarise_aggregate_coverage([])
        assert cov == AggregateCoverage.empty()
        assert cov.bucket_count == 0
        assert cov.as_of_min is None and cov.as_of_max is None
        assert cov.holder_count_min is None
        assert cov.holder_count_max is None
        assert cov.holder_count_latest is None

    def test_varying_coverage_min_max_latest(self) -> None:
        # 209 → 5577 → 6011 filers; latest bucket is the max period_end.
        pts = [
            _pt(date(2025, 3, 31), 209),
            _pt(date(2025, 6, 30), 5577),
            _pt(date(2026, 3, 31), 6011),
        ]
        cov = summarise_aggregate_coverage(pts)
        assert cov.bucket_count == 3
        assert cov.as_of_min == date(2025, 3, 31)
        assert cov.as_of_max == date(2026, 3, 31)
        assert cov.holder_count_min == 209
        assert cov.holder_count_max == 6011
        assert cov.holder_count_latest == 6011  # the latest bucket, not the max-by-value

    def test_latest_is_the_latest_bucket_not_the_largest(self) -> None:
        # Coverage peaks mid-series then dips in the latest quarter — latest
        # must report the latest bucket's count, not the historical peak.
        pts = [
            _pt(date(2025, 6, 30), 6069),
            _pt(date(2026, 3, 31), 6011),
        ]
        cov = summarise_aggregate_coverage(pts)
        assert cov.holder_count_max == 6069
        assert cov.holder_count_latest == 6011

    def test_latest_bucket_issuer_level_reports_none(self) -> None:
        # Codex ckpt-1 HIGH: a (hypothetical) mixed series whose LATEST bucket
        # is issuer-level (None) must NOT report a stale earlier int as latest.
        pts = [
            _pt(date(2025, 6, 30), 10),
            _pt(date(2026, 3, 31), None),
        ]
        cov = summarise_aggregate_coverage(pts)
        assert cov.holder_count_min == 10
        assert cov.holder_count_max == 10
        assert cov.holder_count_latest is None

    def test_treasury_all_none_multi_point(self) -> None:
        # Issuer-level series (treasury): every holder_count is None, but the
        # as-of span + bucket_count are still meaningful.
        pts = [
            _pt(date(2025, 6, 30), None),
            _pt(date(2025, 9, 30), None),
            _pt(date(2025, 12, 31), None),
        ]
        cov = summarise_aggregate_coverage(pts)
        assert cov.bucket_count == 3
        assert cov.as_of_min == date(2025, 6, 30)
        assert cov.as_of_max == date(2025, 12, 31)
        assert cov.holder_count_min is None
        assert cov.holder_count_max is None
        assert cov.holder_count_latest is None

    def test_single_bucket(self) -> None:
        cov = summarise_aggregate_coverage([_pt(date(2026, 3, 31), 42)])
        assert cov.bucket_count == 1
        assert cov.as_of_min == cov.as_of_max == date(2026, 3, 31)
        assert cov.holder_count_min == cov.holder_count_max == cov.holder_count_latest == 42

    def test_bucket_count_is_distinct_periods_not_len(self) -> None:
        # Codex ckpt-1 MEDIUM: contract counts DISTINCT period_end, never
        # len(points). Readers emit one point per period today, but the helper
        # must not depend on it. Duplicate-period latest → max non-None count.
        pts = [
            _pt(date(2025, 6, 30), 100),
            _pt(date(2026, 3, 31), 200),
            _pt(date(2026, 3, 31), 250),  # duplicate period bucket
        ]
        cov = summarise_aggregate_coverage(pts)
        assert cov.bucket_count == 2  # two DISTINCT periods, not 3 points
        assert cov.holder_count_latest == 250  # latest bucket, deterministic max


class TestRejectNtGuard:
    """The institution history readers must NOT apply 13F-NT supersession
    (#1648). A verbatim port of the rollup's ``NT.period_end > HR.period_end``
    filter erases ~1.4B sh/qtr of valid AAPL history. This tripwire fails the
    moment a notices join is reintroduced — broadened beyond the exact table
    name so an alias/helper rename can't slip past (Codex ckpt-1 LOW).

    The reasoning lives in the readers' DOCSTRINGS (which legitimately say
    "supersession"/"NT"), so the guard strips the docstring and checks only the
    executable body — the forbidden tokens can appear there only via an actual
    NT join."""

    _FORBIDDEN = ("institutional_filer_13f_notices", "notice", "supersed")

    @staticmethod
    def _body_without_docstring(fn: object) -> str:
        src = inspect.getsource(fn)  # type: ignore[arg-type]
        doc = getattr(fn, "__doc__", None)
        if doc:
            src = src.replace(doc, "", 1)  # drop the explanatory docstring
        return src.lower()

    def _assert_clean(self, fn: object) -> None:
        body = self._body_without_docstring(fn)
        for token in self._FORBIDDEN:
            assert token not in body, f"{getattr(fn, '__name__', fn)} body must not reference {token!r}"

    def test_aggregate_reader_has_no_nt_filter(self) -> None:
        self._assert_clean(ownership_history._institutions_aggregate_history)

    def test_per_filer_reader_has_no_nt_filter(self) -> None:
        self._assert_clean(ownership_history._institutions_history)


def test_pydantic_response_mirrors_dataclass_fields() -> None:
    """Codex ckpt-1 MEDIUM: pin BE Pydantic <-> dataclass parity so the
    response shape can't silently drift from :class:`AggregateCoverage`."""
    import dataclasses

    from app.api.instruments import AggregateCoverageResponse

    dataclass_fields = {f.name for f in dataclasses.fields(AggregateCoverage)}
    pydantic_fields = set(AggregateCoverageResponse.model_fields)
    assert dataclass_fields == pydantic_fields
