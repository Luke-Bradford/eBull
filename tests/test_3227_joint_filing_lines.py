"""#3227 item 4 — the joint-filing line refusal survives the rollup's holder pipeline.

The SQL column (``_INSIDER_JOINT_FILING_LINES_SQL``) is pinned against a real DB in
``tests/test_3227_holding_line_sum.py``. These pin the pure passes between it and the API:
``_dedup_by_priority`` → ``_collapse_owner_lots`` → ``_build_slice``, the last of which
rebuilds every ``Holder`` field by field, so a new field is silently dropped unless named.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    _build_slice,
    _Candidate,
    _collapse_owner_lots,
    _dedup_by_priority,
)


def _cand(
    *, nature: str, shares: str, accession: str, joint: int, row_id: int, as_of: date = date(2026, 3, 18)
) -> _Candidate:
    return _Candidate(
        source="form3",
        priority_rank=2,
        filer_cik="0001842281",
        filer_name="Holder",
        filer_type=None,
        shares=Decimal(shares),
        as_of_date=as_of,
        accession_number=accession,
        source_row_id=row_id,
        ownership_nature=nature,
        joint_filing_lines=joint,
    )


def test_dedup_takes_the_winners_count_not_a_losers() -> None:
    """A superseded joint filing's figure is not counted, so its count must not ride along."""
    newer = _cand(nature="direct", shares="100", accession="0001-26-000002", joint=0, row_id=1)
    older = _cand(nature="direct", shares="50", accession="0001-25-000001", joint=3, row_id=2, as_of=date(2025, 1, 1))
    [holder] = _dedup_by_priority([older, newer])
    assert holder.winning_accession == "0001-26-000002"
    assert holder.joint_filing_lines == 0

    [holder] = _dedup_by_priority([_cand(nature="direct", shares="50", accession="a", joint=3, row_id=1)])
    assert holder.joint_filing_lines == 3


def test_lot_collapse_keeps_a_non_primary_lots_count() -> None:
    """Every lot is counted in the collapsed total, so a joint lot that is not the largest still flags it."""
    holders = _dedup_by_priority(
        [
            _cand(nature="direct", shares="900", accession="a", joint=0, row_id=1),
            _cand(nature="indirect", shares="100", accession="b", joint=4, row_id=2),
        ]
    )
    [collapsed] = _collapse_owner_lots(holders)
    assert collapsed.shares == Decimal("1000")
    assert collapsed.joint_filing_lines == 4


def test_build_slice_carries_the_count_to_the_api_holder() -> None:
    holders = _dedup_by_priority([_cand(nature="beneficial", shares="50", accession="a", joint=2, row_id=1)])
    slice_ = _build_slice("insiders", holders, Decimal(1000))
    assert [h.joint_filing_lines for h in slice_.holders] == [2]
