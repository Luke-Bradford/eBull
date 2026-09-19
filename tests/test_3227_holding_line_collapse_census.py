"""#3227 — the census classifier, table-tested without a database.

``classify_group`` is the whole judgement in the census; the SQL around it only supplies
rows.  These cases pin the four distinctions it is allowed to make and, more importantly,
the one it is NOT (deciding what the correct aggregate is).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from scripts.census_3227_insider_holding_line_collapse import (
    HoldingLineGroup,
    classify_group,
)

_ACCESSION = "0000000000-26-000001"


def _group(
    *,
    owners: int = 1,
    kept: str | None = "100",
    siblings: tuple[str | None, ...] = ("100",),
    kept_index: int = 0,
    kept_document_id: str | None = None,
) -> HoldingLineGroup:
    """Build a group whose survivor really is one of its own lines, unless asked otherwise.

    ``kept_document_id`` overrides that to exercise the unclassifiable path — the state where
    ``_current`` and the observations disagree and ``line_count - 1`` counts nothing.
    """
    document_ids = tuple(f"{_ACCESSION}:NDH:{index}" for index in range(len(siblings)))
    return HoldingLineGroup(
        instrument_id=1,
        symbol="TEST",
        holder_name="DOE JANE",
        ownership_nature="direct",
        accession=_ACCESSION,
        source_form="form3",
        period_end=date(2026, 3, 18),
        reporting_owner_count=owners,
        kept_document_id=(
            kept_document_id if kept_document_id is not None else (document_ids[kept_index] if document_ids else "")
        ),
        kept_shares=None if kept is None else Decimal(kept),
        sibling_document_ids=document_ids,
        sibling_shares=tuple(None if s is None else Decimal(s) for s in siblings),
    )


def test_a_single_line_group_is_not_affected() -> None:
    """One Table I line on the accession is the state the key was designed for."""
    verdict = classify_group(_group(siblings=("100",)))
    assert verdict.affected is False
    assert verdict.lines_dropped == 0


def test_a_multi_line_single_filer_group_is_where_attribution_is_cheapest() -> None:
    """Form 3 Instr. 5(b)(iii) / Form 4 Instr. 4(b)(iii) require these to be separate lines.

    One reporting owner on the accession means no line-to-holder question has to be answered
    first — NOT that summing them is then safe (the class axis and 5(b)(iv) both survive).
    """
    verdict = classify_group(_group(owners=1, kept="100", siblings=("100", "250", "70")))
    assert verdict.affected is True
    assert verdict.attribution == "single_filer"
    assert verdict.lines_dropped == 2


def test_a_joint_filing_is_attributed_by_no_structural_element() -> None:
    """``<nonDerivativeTable>`` is a sibling of ``<reportingOwner>``, so no line names its holder.

    ⚠ "Structurally" is the whole claim. Attribution can still exist in the footnotes — CNH's
    own name Exor directly and Agnelli indirectly — so this bucket is "costs more to attribute",
    never "cannot be attributed".
    """
    verdict = classify_group(_group(owners=2, kept="100", siblings=("100", "250")))
    assert verdict.attribution == "joint_filing"


def test_the_survivor_losing_to_a_sibling_is_reported_separately_from_winning() -> None:
    assert classify_group(_group(kept="100", siblings=("100", "250"))).kept_is_max is False
    assert classify_group(_group(kept="250", siblings=("100", "250"))).kept_is_max is True


def test_a_group_with_no_amount_anywhere_is_unknown_not_a_loss() -> None:
    """``None`` is the third state: the survivor did not lose, nothing was measurable."""
    verdict = classify_group(_group(kept=None, siblings=(None, None)))
    assert verdict.kept_is_max is None
    assert verdict.naive_sum_delta is None
    assert verdict.affected is True


def test_identical_sibling_amounts_are_flagged_and_not_resolved() -> None:
    """Equal siblings are two distinct source lines of equal size, not a duplicate.

    ⚠ The owner fan-out cannot explain them: the group key already fixes the holder, so writing
    one line against every reporting owner duplicates it BETWEEN groups, never within one. The
    live case is class mixing — CNH reports 366,927,900 common AND 366,927,900 special voting.
    The discriminator that would separate them (``SECURITY_TITLE`` /
    ``DIRECT_INDIRECT_OWNERSHIP`` / ``NATURE_OF_OWNERSHIP``) is dropped at ingest, so the census
    reports the bucket rather than picking.
    """
    assert classify_group(_group(kept="100", siblings=("100", "100"))).identical_value is True
    assert classify_group(_group(kept="100", siblings=("100", "250"))).identical_value is False


def test_a_partially_valued_group_is_not_called_identical() -> None:
    """A NULL line is not evidence of equality — ``{100, None}`` must not read as one value."""
    verdict = classify_group(_group(kept="100", siblings=("100", None)))
    assert verdict.identical_value is False


def test_the_naive_sum_delta_is_the_wrong_fix_made_visible() -> None:
    """Reported so the wrong fix is visibly wrong, never quoted as the size of the error."""
    verdict = classify_group(_group(kept="100", siblings=("100", "250", "70")))
    assert verdict.naive_sum_delta == Decimal(320)


def test_an_unvalued_survivor_beside_valued_siblings_is_a_loss_not_unknown() -> None:
    """Expressible even though the dev population is currently zero — the census must not crash."""
    verdict = classify_group(_group(kept=None, siblings=(None, "250")))
    assert verdict.kept_is_max is False
    assert verdict.naive_sum_delta == Decimal(250)


@pytest.mark.parametrize("owners", [1, 2, 9])
def test_attribution_is_decided_by_owner_count_alone(owners: int) -> None:
    verdict = classify_group(_group(owners=owners, siblings=("100", "250")))
    assert verdict.attribution == ("single_filer" if owners == 1 else "joint_filing")


def test_a_survivor_that_is_not_one_of_the_groups_lines_is_unclassifiable() -> None:
    """``_current`` and the observations disagreeing is a census defect, not a Table I finding.

    Without this the group would report ``lines_dropped = 2`` off a survivor that belongs to
    some other filing entirely — a number with no referent, printed as if it were evidence.
    """
    verdict = classify_group(_group(siblings=("100", "250", "70"), kept_document_id="9999999999-26-000009:NDH:1"))
    assert verdict.invalid_reason == "survivor is not one of the group's lines"
    assert verdict.affected is False
    assert verdict.attribution is None
    assert verdict.lines_dropped == 0


def test_an_accession_with_no_reporting_owner_is_unclassifiable_not_joint() -> None:
    """``reporting_owner_count`` of 0 must not fall through to ``joint_filing``.

    ``_stage_owners`` skips an owner with a blank name, so a zero is reachable and means the
    census cannot see the filing's owners — the opposite of evidence that there were several.
    """
    verdict = classify_group(_group(owners=0, siblings=("100", "250")))
    assert verdict.invalid_reason == "accession wrote no reporting owner"
    assert verdict.attribution is None


def test_a_group_with_no_lines_is_unclassifiable_not_unaffected() -> None:
    verdict = classify_group(_group(siblings=(), kept_document_id="x"))
    assert verdict.invalid_reason == "group has no observation lines"
    assert verdict.affected is False
