"""#3349 — a joint filing's ``indirect`` restatement of the same owner's ``direct`` block is
counted once, not as a second Section-16 lot.

The SQL flag (``_INSIDER_JOINT_DI_RESTATEMENT_SQL``) is pinned against a real DB in
``tests/test_3349_joint_di_restatement_sql.py``. These pin what the rollup does with it.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    _Candidate,
    _dedup_by_priority,
    _reconcile_owner_once,
    _source_rows_and_total,
)

_ACCN = "0000919574-26-001617"


def _cand(*, nature: str, shares: str, restatement: bool, row_id: int, accession: str = _ACCN) -> _Candidate:
    return _Candidate(
        source="form4",
        priority_rank=1,
        filer_cik="0001278386",
        filer_name="BROADWOOD PARTNERS, L.P.",
        filer_type=None,
        shares=Decimal(shares),
        as_of_date=date(2026, 3, 13),
        accession_number=accession,
        source_row_id=row_id,
        ownership_nature=nature,
        nature_from_table_i=True,
        joint_di_restatement=restatement,
    )


def test_dedup_carries_the_flag_from_the_winner() -> None:
    holders = _dedup_by_priority(
        [
            _cand(nature="direct", shares="16123842", restatement=False, row_id=1),
            _cand(nature="indirect", shares="16123842", restatement=True, row_id=2),
        ]
    )
    assert {h.ownership_nature: h.joint_di_restatement for h in holders} == {"direct": False, "indirect": True}


def test_restatement_is_counted_once_and_kept_as_provenance() -> None:
    """STAA-shaped: the ``I`` line restates the record holder's ``D`` block for the co-filers."""
    holders = _dedup_by_priority(
        [
            _cand(nature="direct", shares="16123842", restatement=False, row_id=1),
            _cand(nature="indirect", shares="16123842", restatement=True, row_id=2),
        ]
    )
    rows, total = _source_rows_and_total("form4", holders)
    assert total == Decimal("16123842")
    [kept] = rows
    assert kept.ownership_nature == "direct"
    assert [(d.accession_number, d.shares) for d in kept.dropped_sources] == [(_ACCN, Decimal("16123842"))]


def test_unflagged_equal_lots_still_sum() -> None:
    """A single-owner filing's equal D and I lots are distinct holdings (ASST: the filer's own
    150,000 beside the spouse's) and keep the #905 SUM."""
    holders = _dedup_by_priority(
        [
            _cand(nature="direct", shares="150000", restatement=False, row_id=1),
            _cand(nature="indirect", shares="150000", restatement=False, row_id=2),
        ]
    )
    _rows, total = _source_rows_and_total("form4", holders)
    assert total == Decimal("300000")


def test_stale_restatement_is_not_maxed_against_a_newer_direct() -> None:
    """A later filing sold the block down to 500; the old 1,000 restatement must not win a MAX
    (Codex ckpt-2: the ``D`` partner left ``_current`` but the ``I`` row stayed)."""
    holders = _dedup_by_priority(
        [
            _cand(nature="direct", shares="500", restatement=False, row_id=1, accession="0000919574-26-009999"),
            _cand(nature="indirect", shares="1000", restatement=True, row_id=2),
        ]
    )
    rows, total = _source_rows_and_total("form4", holders)
    assert total == Decimal("500")
    [kept] = rows
    assert [(d.accession_number, d.shares) for d in kept.dropped_sources] == [(_ACCN, Decimal("1000"))]


def test_a_sole_restatement_row_is_still_the_figure() -> None:
    """If the owner's other rows were excluded upstream, the restatement is all that is left."""
    holders = _dedup_by_priority([_cand(nature="indirect", shares="1000", restatement=True, row_id=2)])
    _rows, total = _source_rows_and_total("form4", holders)
    assert total == Decimal("1000")


def test_owner_once_figure_is_the_block_not_twice_it() -> None:
    """End to end through owner-once, including the Form 3 + Form 4 pooling pass, which
    classifies additive lots on its own (``_merge_section16_forms``)."""
    form3_direct = _Candidate(
        source="form3",
        priority_rank=2,
        filer_cik="0001278386",
        filer_name="BROADWOOD PARTNERS, L.P.",
        filer_type=None,
        shares=Decimal("1000"),
        as_of_date=date(2025, 1, 1),
        accession_number="0000919574-25-000001",
        source_row_id=3,
        ownership_nature="direct",
        nature_from_table_i=True,
    )
    holders = _dedup_by_priority(
        [
            # The Form 4 indirect restatement is the only Form 4 row; the latest direct is on
            # an older Form 3, so the pooling pass decides what is additive.
            _cand(nature="indirect", shares="1000", restatement=True, row_id=2),
            form3_direct,
        ]
    )
    by_category = _reconcile_owner_once(holders)
    [owner] = by_category["insiders"]
    assert owner.shares == Decimal("1000")
