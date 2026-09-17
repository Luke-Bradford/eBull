"""#2173 / #2371 — when is a zero-holder DEF 14A re-parse the RIGHT answer?

``_apply_def14a``'s guard raises ``RewashParseError`` whenever a re-parse
produces no holders for an accession that previously had rows. That is correct
as a regression brake, but it cannot tell "the parser broke" from "zero is the
right answer" — so #2163's two Schedule 13D/G cover-page accessions were pinned
at their old parser version with the junk still live.

The release rule is keyed on what is STORED, not on a reason threaded out of the
parser: it can then only ever release rows that are provably not Item 403 data.

**#2371 widened the rule from a vocabulary to the storage path's own row test.**
The 13D-cover-label form was 13D-only, so an accession whose every stored row is
a 229.403 column-1 *Title of class* value kept its junk forever. The release rule
is now "no stored row names a beneficial owner", composed of the SAME two
refusals ``_extract_holder_rows`` applies before storing a row — imported, not
restated, so the parse half and the release half cannot drift.
"""

from __future__ import annotations

from app.services.rewash_filings import (
    name_is_not_a_beneficial_owner,
    no_stored_name_is_a_beneficial_owner,
)

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


# The three rows stored for #2371's worked case, verbatim — every one a 229.403
# column-1 "Title of class" value that leaked into the name column.
_ACC_0001731122_26_000671 = [
    "Common Stock",
    "Class C Common Stock",
    "Class B Common Stock",
]


class TestCorrectZeroRelease:
    def test_the_two_cover_page_accessions_release(self) -> None:
        """17 CFR 240.13d-101/-102 cover-page item labels are form fields, not
        beneficial owners under Rule 13d-3, so zero rows is reg-correct.

        Retained verbatim from the #2173 rule: the widened predicate must SUBSUME
        the vocabulary it replaces, not merely differ from it."""
        assert no_stored_name_is_a_beneficial_owner(_ACC_0001104659_17_023458)
        assert no_stored_name_is_a_beneficial_owner(_ACC_0001308179_25_000114)

    def test_an_all_titles_of_class_accession_releases(self) -> None:
        """#2371's worked case. 229.403 column 1 is *Title of class*; its values
        are instruments, and Rule 13d-3 makes a beneficial owner a person or
        entity holding voting or investment power.

        ⚠ This is the case the ticket's own follow-up prescription would MISS.
        It proposed ``_is_beneficial_owner_identity``, and
        ``_is_beneficial_owner_identity("Class C Common Stock")`` is ``True`` by
        design — that predicate must not enable ``strip_class_designator``,
        because doing so narrows owner identity, which feeds
        ``_ROW_IDENTITY_FLOOR`` and de-admits whole tables. Only the storage
        path's own call, with the strip ON, sees a class title as an instrument.
        """
        assert no_stored_name_is_a_beneficial_owner(_ACC_0001731122_26_000671)
        assert name_is_not_a_beneficial_owner("Class C Common Stock")
        assert name_is_not_a_beneficial_owner("Common Stock")

    def test_the_two_refusals_are_independent(self) -> None:
        """Each disjunct must carry its own cases, or one could rot unnoticed
        behind the other. A cover label is not instrument vocabulary and a class
        title is not a cover label."""
        from app.providers.implementations.sec_def14a import (
            _is_instrument_not_owner,
            _SCHEDULE_13D_COVER_LABEL_RE,
        )

        assert not _is_instrument_not_owner("SHARED VOTING POWER -0", strip_class_designator=True)
        assert not _SCHEDULE_13D_COVER_LABEL_RE.match("Class C Common Stock")

    def test_a_genuine_holder_anywhere_keeps_the_guard(self) -> None:
        """ALL, not ANY. A mixed accession has at least one row that may be a
        real holder, and superseding those is the loss the guard prevents."""
        assert not no_stored_name_is_a_beneficial_owner([*_ACC_0001308179_25_000114, "BlackRock, Inc."])
        assert not no_stored_name_is_a_beneficial_owner(["The Vanguard Group", "SHARED VOTING POWER -0"])
        assert not no_stored_name_is_a_beneficial_owner(["BlackRock, Inc.", "The Vanguard Group"])
        # Mixed across the NEW disjunct too, not only the inherited one.
        assert not no_stored_name_is_a_beneficial_owner([*_ACC_0001731122_26_000671, "BlackRock, Inc."])

    def test_a_real_holder_is_never_an_instrument(self) -> None:
        """The widened rule's risk is over-removal, so pin the other direction.

        ``strip_class_designator`` drops every token of length <= 2 from a name
        containing 'class' or 'series', which is blunt — these names survive it
        because a proper noun is not equity vocabulary."""
        for holder in (
            "BlackRock, Inc.",
            "The Vanguard Group",
            "Class Action Capital Partners LLC",
            "Series Fund Advisors, L.P.",
            "All directors and executive officers as a group (10 persons)",
        ):
            assert not name_is_not_a_beneficial_owner(holder), holder

    def test_an_empty_or_blank_set_is_never_proof(self) -> None:
        """``all()`` over an empty sequence is vacuously TRUE — the same shape
        as the ``<> ALL('{}')`` trap that made #2140's supersede delete a whole
        accession. An accession with no stored rows must not read as a correct
        zero."""
        assert not no_stored_name_is_a_beneficial_owner([])
        assert not no_stored_name_is_a_beneficial_owner(["", "   "])
        assert not no_stored_name_is_a_beneficial_owner([None])
        assert not no_stored_name_is_a_beneficial_owner(["SHARED VOTING POWER -0", ""])
        assert not no_stored_name_is_a_beneficial_owner(["Class B Common Stock", ""])
