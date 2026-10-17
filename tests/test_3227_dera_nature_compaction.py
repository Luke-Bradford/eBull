"""#3227 — the audit's model of DERA's compacted ``NATURE_OF_OWNERSHIP`` column."""

from __future__ import annotations

from scripts.audit_3227_dera_nature_compaction import compacted


def test_msbi_shape() -> None:
    # MSBI 0001466026-26-000008: EDGAR order, then DERA's.
    edgar = ["", "red bird investors, llc", "summit investors, llp", "j.m. schultz investment, llc"]
    assert compacted(edgar) == ["red bird investors, llc", "summit investors, llp", "j.m. schultz investment, llc", ""]


def test_inner_blank_moves_to_the_end_and_order_is_kept() -> None:
    assert compacted(["a", "", "b", ""]) == ["a", "b", "", ""]


def test_no_blank_or_only_trailing_blanks_is_unchanged() -> None:
    assert compacted(["a", "b"]) == ["a", "b"]
    assert compacted(["a", ""]) == ["a", ""]
