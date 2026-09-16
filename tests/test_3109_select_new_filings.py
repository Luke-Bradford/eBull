"""#3109 — the per-triple half of a freshness check, as a pure function.

``select_new_filings`` is the source filter + accession-watermark truncation
extracted out of ``check_freshness`` / ``check_freshness_conditional`` so the
batching job can apply it per subject to ONE CIK-wide response instead of
re-fetching that response per subject.

Pure tests, no DB: the rule is a function of a row list and two arguments, and
the repo standard prefers a table-tested pure function over an integration test
for exactly this shape.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.providers.implementations.sec_submissions import (
    FilingIndexRow,
    select_new_filings,
)
from app.services.sec_manifest import ManifestSource


def _row(accession: str, form: str, source: str | None, day: int) -> FilingIndexRow:
    return FilingIndexRow(
        accession_number=accession,
        cik="0000320193",
        form=form,
        source=source,  # type: ignore[arg-type]
        filed_at=datetime(2026, 4, day, tzinfo=UTC),
        accepted_at=None,
        primary_document_url=None,
        is_amendment=False,
    )


# Newest-first, as SEC serves ``filings.recent``. Deliberately interleaved
# across sources so a per-subject filter cannot be satisfied by a prefix.
_PAGE = [
    _row("ACC-05", "8-K", "sec_8k", 5),
    _row("ACC-04", "4", "sec_form4", 4),
    _row("ACC-03", "8-K", "sec_8k", 3),
    _row("ACC-02", "S-1", None, 2),
    _row("ACC-01", "4", "sec_form4", 1),
]


class TestSourceFilter:
    def test_filters_to_the_requested_source(self) -> None:
        new, _ = select_new_filings(_PAGE, sources={"sec_8k"}, last_known_filing_id=None)
        assert [r.accession_number for r in new] == ["ACC-05", "ACC-03"]

    def test_none_means_no_filter(self) -> None:
        new, _ = select_new_filings(_PAGE, sources=None, last_known_filing_id=None)
        assert len(new) == len(_PAGE)

    def test_unmapped_forms_are_dropped_by_any_filter(self) -> None:
        """``source=None`` rows (S-1, CORRESP...) are never in a filtered set."""
        new, _ = select_new_filings(_PAGE, sources={"sec_8k", "sec_form4"}, last_known_filing_id=None)
        assert all(r.source is not None for r in new)
        assert "ACC-02" not in [r.accession_number for r in new]


class TestWatermarkTruncation:
    def test_stops_at_the_watermark(self) -> None:
        new, _ = select_new_filings(_PAGE, sources=None, last_known_filing_id="ACC-03")
        assert [r.accession_number for r in new] == ["ACC-05", "ACC-04"]

    def test_watermark_absent_returns_everything(self) -> None:
        """Rolled into a secondary page, or the caller's watermark is wrong.

        Returning everything is safe because the manifest upsert is idempotent
        on the accession PK.
        """
        new, _ = select_new_filings(_PAGE, sources=None, last_known_filing_id="ACC-NOT-HERE")
        assert len(new) == len(_PAGE)

    def test_watermark_at_the_head_returns_nothing(self) -> None:
        new, _ = select_new_filings(_PAGE, sources=None, last_known_filing_id="ACC-05")
        assert new == []

    def test_truncation_is_applied_AFTER_the_source_filter(self) -> None:
        """⚠ Order matters and the wrong order is silently plausible.

        Filtering to ``sec_8k`` leaves ``[ACC-05, ACC-03]``; a watermark of
        ``ACC-04`` (a Form 4) is not in that list, so the correct answer is
        "watermark not present → return everything". Truncating FIRST would
        return only ``ACC-05`` — dropping ACC-03, a filing this subject has
        never seen.
        """
        new, _ = select_new_filings(_PAGE, sources={"sec_8k"}, last_known_filing_id="ACC-04")
        assert [r.accession_number for r in new] == ["ACC-05", "ACC-03"]


class TestLastFiledAt:
    def test_is_the_max_over_FILTERED_rows_not_new_ones(self) -> None:
        """It describes the newest filing this subject can SEE, not the newest
        that is new to it — so a fully-caught-up subject still reports one."""
        _, last_filed = select_new_filings(_PAGE, sources={"sec_8k"}, last_known_filing_id="ACC-05")
        assert last_filed == datetime(2026, 4, 5, tzinfo=UTC)

    def test_is_none_when_the_filter_matches_nothing(self) -> None:
        new, last_filed = select_new_filings(_PAGE, sources={"sec_13f_hr"}, last_known_filing_id=None)
        assert new == []
        assert last_filed is None


class TestBatchingEquivalence:
    @pytest.mark.parametrize("source", ["sec_8k", "sec_form4", "sec_13f_hr"])
    @pytest.mark.parametrize("watermark", [None, "ACC-03", "ACC-05", "ACC-NOT-HERE"])
    def test_one_shared_page_equals_a_per_subject_page(
        self, source: ManifestSource, watermark: str | None
    ) -> None:
        """The whole premise of #3109 in one assertion.

        Applying the filter to a CIK-wide page must equal what the old path
        produced, where the provider filtered during its own dedicated fetch.
        Same bytes in, same rows out — which is why the test uses a frozen
        page rather than a live response.
        """
        batched, batched_ts = select_new_filings(_PAGE, sources={source}, last_known_filing_id=watermark)
        pre_filtered = [r for r in _PAGE if r.source == source]
        unbatched, unbatched_ts = select_new_filings(pre_filtered, sources={source}, last_known_filing_id=watermark)
        assert [r.accession_number for r in batched] == [r.accession_number for r in unbatched]
        assert batched_ts == unbatched_ts
