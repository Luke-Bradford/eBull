"""Pure-logic tests for the Rankings coverage breakdown (#1918).

``build_coverage`` assembles the ranked-vs-universe denominator + why-not-ranked
buckets from raw full-population counts. No DB — table-tests the MECE invariant,
the analysable split, the residual catch-all, and the no-run case.
"""

from __future__ import annotations

from datetime import UTC, datetime

from app.api.scores import build_coverage

_TS = datetime(2026, 7, 4, 6, 0, 0, tzinfo=UTC)

# The full-population dev snapshot (2026-07-04, model v1.3-balanced).
_REAL_STATUS_COUNTS = {
    "analysable": 3914,
    "no_primary_sec_cik": 7254,
    "fpi": 1088,
    "insufficient": 185,
    "structurally_young": 156,
}


def _bucket(cov, reason):
    return next((b for b in cov.not_ranked if b.reason == reason), None)


class TestBuildCoverage:
    def test_real_snapshot_reconciles_to_universe(self) -> None:
        cov = build_coverage(
            model_version="v1.3-balanced",
            scored_at=_TS,
            universe=12597,
            ranked=3904,
            status_counts=_REAL_STATUS_COUNTS,
        )
        assert cov.ranked == 3904
        assert cov.universe == 12597
        # MECE: ranked + every not_ranked bucket sums back to the universe.
        assert cov.ranked + sum(b.count for b in cov.not_ranked) == cov.universe

    def test_analysable_splits_into_ranked_and_unranked(self) -> None:
        cov = build_coverage(
            model_version="m",
            scored_at=_TS,
            universe=12597,
            ranked=3904,
            status_counts=_REAL_STATUS_COUNTS,
        )
        unranked = _bucket(cov, "analysable_unranked")
        assert unranked is not None
        # 3914 analysable − 3904 ranked = 10 analysable-but-not-in-run.
        assert unranked.count == 10
        assert unranked.label == "Analysable — not in latest ranking run"

    def test_status_buckets_pass_through_with_labels(self) -> None:
        cov = build_coverage(
            model_version="m",
            scored_at=_TS,
            universe=12597,
            ranked=3904,
            status_counts=_REAL_STATUS_COUNTS,
        )
        assert _bucket(cov, "no_primary_sec_cik").count == 7254  # type: ignore[union-attr]
        assert _bucket(cov, "fpi").count == 1088  # type: ignore[union-attr]
        assert _bucket(cov, "fpi").label == "Foreign private issuer (20-F/6-K filer)"  # type: ignore[union-attr]

    def test_zero_count_buckets_omitted(self) -> None:
        cov = build_coverage(
            model_version="m",
            scored_at=_TS,
            universe=4000,
            ranked=3904,
            status_counts={"analysable": 3904, "no_primary_sec_cik": 96},
        )
        # analysable == ranked → no analysable_unranked bucket.
        assert _bucket(cov, "analysable_unranked") is None
        # fpi/insufficient/young absent from counts → no buckets.
        assert {b.reason for b in cov.not_ranked} == {"no_primary_sec_cik"}
        assert cov.ranked + sum(b.count for b in cov.not_ranked) == cov.universe

    def test_no_run_ranks_zero_all_analysable_unranked(self) -> None:
        cov = build_coverage(
            model_version="m",
            scored_at=None,
            universe=12597,
            ranked=0,
            status_counts=_REAL_STATUS_COUNTS,
        )
        assert cov.scored_at is None
        assert cov.ranked == 0
        # Nothing ranked → all 3914 analysable land in analysable_unranked.
        assert _bucket(cov, "analysable_unranked").count == 3914  # type: ignore[union-attr]
        assert cov.ranked + sum(b.count for b in cov.not_ranked) == cov.universe

    def test_unknown_status_folds_into_other(self) -> None:
        # A tradable row with NULL/unknown coverage status is not in the known
        # map; the residual `other` bucket absorbs it so the total reconciles.
        cov = build_coverage(
            model_version="m",
            scored_at=_TS,
            universe=100,
            ranked=40,
            status_counts={"analysable": 40, "no_primary_sec_cik": 50},
        )
        other = _bucket(cov, "other")
        assert other is not None
        assert other.count == 10  # 100 − 40 analysable − 50 no_sec
        assert other.label == "Unclassified coverage"
        assert cov.ranked + sum(b.count for b in cov.not_ranked) == cov.universe

    def test_negative_bucket_clamped_not_shown_negative(self) -> None:
        # Data anomaly: ranked (45) > analysable (40) — e.g. duplicate score
        # rows. analysable_unranked would be −5; it must clamp to 0 (omitted),
        # never surface a negative.
        cov = build_coverage(
            model_version="m",
            scored_at=_TS,
            universe=100,
            ranked=45,
            status_counts={"analysable": 40, "no_primary_sec_cik": 60},
        )
        assert _bucket(cov, "analysable_unranked") is None
        assert all(b.count > 0 for b in cov.not_ranked)
