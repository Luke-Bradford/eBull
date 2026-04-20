"""Unit tests for app.services.coverage_audit.

Pure-function tests for the classifier use no DB. Integration tests
against ``ebull_test`` live in a separate file.
"""

from __future__ import annotations

from app.services.coverage import AuditCounts, _classify


def _counts(
    *,
    ten_k_in_3y: int = 0,
    ten_q_in_18m: int = 0,
    us_base_or_amend_total: int = 0,
    fpi_total: int = 0,
) -> AuditCounts:
    return AuditCounts(
        instrument_id=1,
        ten_k_in_3y=ten_k_in_3y,
        ten_q_in_18m=ten_q_in_18m,
        us_base_or_amend_total=us_base_or_amend_total,
        fpi_total=fpi_total,
    )


class TestClassify:
    """Classifier branch coverage."""

    def test_no_sec_cik_returns_no_primary_sec_cik(self) -> None:
        assert _classify(None, has_sec_cik=False) == "no_primary_sec_cik"

    def test_no_sec_cik_ignores_agg(self) -> None:
        # Shouldn't happen in practice (agg is None when no SEC filings)
        # but the classifier must be robust to being handed counts anyway.
        agg = _counts(ten_k_in_3y=5, ten_q_in_18m=10, us_base_or_amend_total=15)
        assert _classify(agg, has_sec_cik=False) == "no_primary_sec_cik"

    def test_sec_cik_no_filings_returns_insufficient(self) -> None:
        # SEC CIK present but filing_events empty — pre-backfill state.
        assert _classify(None, has_sec_cik=True) == "insufficient"

    def test_us_issuer_with_full_history_is_analysable(self) -> None:
        agg = _counts(
            ten_k_in_3y=2,
            ten_q_in_18m=4,
            us_base_or_amend_total=6,
        )
        assert _classify(agg, has_sec_cik=True) == "analysable"

    def test_us_issuer_extra_history_is_analysable(self) -> None:
        # More than minimum is still analysable.
        agg = _counts(
            ten_k_in_3y=3,
            ten_q_in_18m=12,
            us_base_or_amend_total=15,
        )
        assert _classify(agg, has_sec_cik=True) == "analysable"

    def test_exactly_at_threshold_is_analysable(self) -> None:
        # 2 and 4 are the minimums — boundary inclusive.
        agg = _counts(
            ten_k_in_3y=2,
            ten_q_in_18m=4,
            us_base_or_amend_total=6,
        )
        assert _classify(agg, has_sec_cik=True) == "analysable"

    def test_one_below_10k_threshold_is_insufficient(self) -> None:
        agg = _counts(
            ten_k_in_3y=1,
            ten_q_in_18m=4,
            us_base_or_amend_total=5,
        )
        assert _classify(agg, has_sec_cik=True) == "insufficient"

    def test_one_below_10q_threshold_is_insufficient(self) -> None:
        agg = _counts(
            ten_k_in_3y=2,
            ten_q_in_18m=3,
            us_base_or_amend_total=5,
        )
        assert _classify(agg, has_sec_cik=True) == "insufficient"

    def test_fpi_has_20f_zero_us_is_fpi(self) -> None:
        # Has SEC CIK, zero US base-or-amend, at least one 20-F.
        agg = _counts(
            ten_k_in_3y=0,
            ten_q_in_18m=0,
            us_base_or_amend_total=0,
            fpi_total=2,  # 2 × 20-F
        )
        assert _classify(agg, has_sec_cik=True) == "fpi"

    def test_fpi_with_6ka_amendment_only_is_fpi(self) -> None:
        agg = _counts(us_base_or_amend_total=0, fpi_total=1)  # single 6-K/A
        assert _classify(agg, has_sec_cik=True) == "fpi"

    def test_mixed_us_and_fpi_forms_is_insufficient_not_fpi(self) -> None:
        # One 10-K + one 20-F → not an FPI (has US base form) and
        # doesn't meet the US bar either.
        agg = _counts(
            ten_k_in_3y=1,
            us_base_or_amend_total=1,
            fpi_total=1,
        )
        assert _classify(agg, has_sec_cik=True) == "insufficient"

    def test_sec_cik_with_only_8k_family_is_insufficient(self) -> None:
        # No US base forms, no FPI forms, only 8-K-ish — insufficient.
        agg = _counts(
            ten_k_in_3y=0,
            ten_q_in_18m=0,
            us_base_or_amend_total=0,
            fpi_total=0,
        )
        assert _classify(agg, has_sec_cik=True) == "insufficient"


class TestAmendmentsDoNotCountTowardBar:
    """Regression guard for the deliberate design decision that
    amendments re-state an existing period and therefore do NOT
    satisfy the history-depth bar.

    Amendments still populate filing_events, still trigger
    event-driven thesis refresh (Chunk I), and still register in
    ``us_base_or_amend_total`` for FPI detection — they just don't
    satisfy ``ten_k_in_3y >= 2`` or ``ten_q_in_18m >= 4``.

    The SQL aggregate filters on exact ``filing_type = '10-K'`` and
    ``'10-Q'`` (no amendments) for the window counters, so the
    AuditCounts values landing in the classifier already exclude
    amendments from those counts. The tests below exercise the
    AGGREGATE'S contract (what the SQL produces), not the classifier,
    because the classifier can't tell base from amendment from its
    inputs alone.
    """

    def test_1_10k_plus_1_10ka_does_not_satisfy_2_10k(self) -> None:
        # SQL aggregate would produce ten_k_in_3y=1 (base only).
        # us_base_or_amend_total=2 (base + amend). Classifier sees
        # ten_k_in_3y=1 → insufficient.
        agg = _counts(
            ten_k_in_3y=1,  # only the base 10-K, amendment excluded
            ten_q_in_18m=4,
            us_base_or_amend_total=6,  # 1 × 10-K + 1 × 10-K/A + 4 × 10-Q
        )
        assert _classify(agg, has_sec_cik=True) == "insufficient"

    def test_1_10q_plus_3_10qa_does_not_satisfy_4_10q(self) -> None:
        # Only one actual 10-Q period filed — amendments re-state it.
        agg = _counts(
            ten_k_in_3y=2,
            ten_q_in_18m=1,  # base only
            us_base_or_amend_total=6,  # 2 × 10-K + 1 × 10-Q + 3 × 10-Q/A
        )
        assert _classify(agg, has_sec_cik=True) == "insufficient"
