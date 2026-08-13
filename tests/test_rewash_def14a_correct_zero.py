"""#2173 — when is a zero-holder DEF 14A re-parse the RIGHT answer?

``_apply_def14a``'s guard raises ``RewashParseError`` whenever a re-parse
produces no holders for an accession that previously had rows. That is correct
as a regression brake, but it cannot tell "the parser broke" from "zero is the
right answer" — so #2163's two Schedule 13D/G cover-page accessions were pinned
at their old parser version with the junk still live.

The release rule is keyed on what is STORED, not on a reason threaded out of the
parser: it can then only ever release rows that are provably not Item 403 data.
"""

from __future__ import annotations

from app.services.rewash_filings import all_names_are_13d_cover_labels

# The rows actually stored for the two accessions named on #2173, verbatim.
_ACC_0001104659_17_023458 = [
    "SHARED DISPOSITIVE POWER -0",
    "SHARED VOTING POWER -0",
    "SOLE DISPOSITIVE POWER 32,005,260 shares of Common Stock (See Items 5 and 5)",
]
_ACC_0001308179_25_000114 = [
    "Sole voting power",
    "Shared voting power",
    "Sole investment power",
    "Shared investment power",
]


class TestCorrectZeroRelease:
    def test_the_two_cover_page_accessions_release(self) -> None:
        """17 CFR 240.13d-101/-102 cover-page item labels are form fields, not
        beneficial owners under Rule 13d-3, so zero rows is reg-correct."""
        assert all_names_are_13d_cover_labels(_ACC_0001104659_17_023458)
        assert all_names_are_13d_cover_labels(_ACC_0001308179_25_000114)

    def test_a_genuine_holder_anywhere_keeps_the_guard(self) -> None:
        """ALL, not ANY. A mixed accession has at least one row that may be a
        real holder, and superseding those is the loss the guard prevents."""
        assert not all_names_are_13d_cover_labels([*_ACC_0001308179_25_000114, "BlackRock, Inc."])
        assert not all_names_are_13d_cover_labels(["The Vanguard Group", "SHARED VOTING POWER -0"])
        assert not all_names_are_13d_cover_labels(["BlackRock, Inc.", "The Vanguard Group"])

    def test_an_empty_or_blank_set_is_never_proof(self) -> None:
        """``all()`` over an empty sequence is vacuously TRUE — the same shape
        as the ``<> ALL('{}')`` trap that made #2140's supersede delete a whole
        accession. An accession with no stored rows must not read as a correct
        zero."""
        assert not all_names_are_13d_cover_labels([])
        assert not all_names_are_13d_cover_labels(["", "   "])
        assert not all_names_are_13d_cover_labels([None])
        assert not all_names_are_13d_cover_labels(["SHARED VOTING POWER -0", ""])
