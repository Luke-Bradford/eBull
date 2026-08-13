"""#2121 — DEF 14A ``holder_role`` is a DISPLAY LABEL on the non-additive
``def14a_unmatched`` memo overlay ONLY, and must never leak into the additive
insiders / blockholders slices (I21 / #1659).

Pure tests over ``_bucket_into_slices`` (unmatched carry) and
``_dedup_by_priority`` (matched-path drop) — no DB, so they run in the fast
tier. The invariant is enforced by construction: only the unmatched slice-build
sets ``Holder.holder_role``; the dedup path that feeds insiders/blockholders
never passes it, so it stays ``None``.
"""

from __future__ import annotations

from decimal import Decimal

from app.services.ownership_rollup import (
    Holder,
    _bucket_into_slices,
    _Candidate,
    _dedup_by_priority,
)


def _def14a_candidate(name: str, role: str | None, *, source_row_id: int = 1) -> _Candidate:
    return _Candidate(
        source="def14a",
        priority_rank=99,
        filer_cik=None,
        filer_name=name,
        filer_type=None,
        shares=Decimal("5000"),
        as_of_date=None,
        accession_number="0000000000-24-000001",
        source_row_id=source_row_id,
        ownership_nature="beneficial",
        holder_role=role,
    )


def test_unmatched_slice_carries_holder_role() -> None:
    """Each unmatched DEF 14A candidate's role reaches the overlay holder."""
    cands = [
        _def14a_candidate("Jane Officer", "officer", source_row_id=1),
        _def14a_candidate("All directors and officers as a group", "group", source_row_id=2),
        _def14a_candidate("Unlabelled Holder", None, source_row_id=3),
    ]
    slices = _bucket_into_slices({}, cands, Decimal("1000000"))
    proxy = next(s for s in slices if s.category == "def14a_unmatched")

    roles = {h.filer_name: h.holder_role for h in proxy.holders}
    assert roles["Jane Officer"] == "officer"
    assert roles["All directors and officers as a group"] == "group"
    assert roles["Unlabelled Holder"] is None
    # The overlay stays non-additive regardless of the label.
    assert proxy.denominator_basis == "proxy_disclosure"


def test_matched_def14a_holder_never_carries_role() -> None:
    """A DEF 14A candidate that resolves to a filer flows through
    ``_dedup_by_priority`` into insiders/blockholders — the resulting Holder
    must leave ``holder_role`` at ``None`` even though the candidate carried one
    (the I21/#1659 no-leak guarantee)."""
    matched = _def14a_candidate("Matched Insider", "officer")
    survivors = _dedup_by_priority([matched])
    assert len(survivors) == 1
    assert survivors[0].holder_role is None


def test_holder_role_defaults_none() -> None:
    """Non-DEF-14A holders never set the field."""
    h = Holder(
        filer_cik="0000000000",
        filer_name="Vanguard",
        shares=Decimal("100"),
        pct_outstanding=Decimal("0.1"),
        winning_source="13f",
        winning_accession="0000000000-24-000009",
        winning_edgar_url=None,
        as_of_date=None,
        filer_type="institution",
        dropped_sources=(),
    )
    assert h.holder_role is None
