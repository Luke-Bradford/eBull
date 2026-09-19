"""#3227 — Form 3 Table I lines discarded by the ``ownership_insiders_current`` key.

    PYTHONPATH=. uv run python -m scripts.census_3227_insider_holding_line_collapse
    PYTHONPATH=. uv run python -m scripts.census_3227_insider_holding_line_collapse --examples 15

Read-only.  Nothing is written, nothing is refreshed.

## The rule being measured against

Form 3, Table I — "Non-Derivative Securities Beneficially Owned" (SEC 1473, 03-26) —
carries the printed reminder *"Report on a separate line for each class of securities
beneficially owned directly or indirectly."*  General Instruction 5(b)(iii) -- and Form 4's
identically worded 4(b)(iii) -- is explicit about the second axis: *"Report securities
beneficially owned directly on a separate line from those beneficially owned indirectly.
Report different forms of indirect ownership on separate lines."*  (Form 4 says "Report
transactions in securities..."; the separation requirement is word-for-word the same.)

So the source's line grain is **(class of security) x (direct | each distinct form of
indirect ownership)**.  ``ownership_insiders_current`` is keyed
``(instrument_id, holder_identity_key, ownership_nature)``, which carries neither axis on a
dataset row: ``sec_insider_dataset_ingest._map_relationship`` writes the DERA *relationship*
flag into ``ownership_nature`` (officer/director -> ``direct``, ten-percent-owner ->
``beneficial``) and never reads the D/I field (see ``.claude/skills/data-sources/sec-edgar.md``
section 2.3, #2385/#2386).  Lines the regulation requires to be separate therefore collide on
one key and the ``DISTINCT ON`` that builds ``_current`` keeps exactly one.

This is the AGGREGATION question #3146 left open when it fixed line ORDER for ``:NDT:`` and
excluded ``:NDH:`` as *"a simultaneous snapshot per class and ownership form, not a
sequence"* — which is precisely why choosing one line is wrong.

## WHY IT EXISTS

The issue quotes figures.  A figure written into prose goes stale silently the moment the
corpus moves, and this population moves on every insider dataset ingest.  This script is the
derivation, so #3227's numbers can be re-read rather than re-derived, and so the single-filer
half — the part that is actually fixable — stays separated from the joint-filing half that is
not.

It reads ``ownership_insiders_current`` DIRECTLY rather than re-deriving the winner from
``ownership_insiders_observations``.  ``_current`` IS the writer's output, so a census that
re-implemented the ``DISTINCT ON`` could disagree with the table it claims to describe.

## WHAT IT CANNOT ESTABLISH

* **Not the size of the error.**  ``naive_sum_delta`` is reported and is an UPPER BOUND that
  must not be quoted as the understatement.  It sums across share classes (the GOOGL/GOOG
  trap) and, on a joint filing, across lines that belong to no named co-filer — every
  reporting owner on an accession receives every Table I line from ``_stage_owners``, so the
  same holding is counted once per filer.  Form 3 Instruction 5(b)(iv) adds a third reason:
  an indirect amount may be the person's proportionate interest *or*, at their option, the
  entity's entire holding, so even correctly attributed lines are not always additive.
* **Not which lines are duplicates.**  ``identical_value`` groups are reported in their own
  bucket rather than folded in either direction: equal sibling amounts are consistent BOTH
  with one holding fanned out across co-filers AND with two genuinely equal holdings, and the
  discriminator that would separate them (``SECURITY_TITLE`` / ``DIRECT_INDIRECT_OWNERSHIP`` /
  ``NATURE_OF_OWNERSHIP``) is dropped at ingest — ``sec_insider_dataset_ingest`` reads
  ``SHRS_OWND_FOLWNG_TRANS`` and ``NONDERIV_HOLDING_SK`` and nothing else from
  ``NONDERIV_HOLDING.tsv``.
* **Not the observations-layer population.**  Most ``:NDH:`` multi-line groups on ``form4``
  accessions never reach ``_current`` at all — ``_INSIDER_DUAL_PIPELINE_DECOLLISION`` removes
  a dataset row whose plain-accession XML sibling survives.  This census measures what an
  operator can actually read.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.config import settings

#: Marker that identifies a row written by the bulk DERA insider dataset rather than by one
#: of the XML manifest parsers -- ``<accession>:NDT:<sk>`` for a Form 4 transaction,
#: ``<accession>:NDH:<sk>`` for a Form 3 / holdings line.
#:
#: ⚠ This pattern is restated verbatim in five production sites already
#: (``ownership_observations.py`` x2, ``ownership_rollup.py`` x3) with no shared constant.
#: Unifying them would edit the operator-visible rollup read path, so this census carries its
#: own copy rather than widening a measurement change into a behavioural one.  Noted on #3227.
_HOLDING_ROW_MARKER: Final[str] = ":NDH:"

Attribution = Literal["single_filer", "joint_filing"]


@dataclass(frozen=True)
class HoldingLineGroup:
    """One ``(instrument, holder, nature, accession, period_end)`` group as stored.

    ``sibling_shares`` is every ``:NDH:`` observation line in the group, INCLUDING the one
    that won and is now in ``_current``.  ``reporting_owner_count`` is the number of distinct
    holders the accession wrote against this instrument, which is how a joint filing is
    recognised without re-parsing the filing.
    """

    instrument_id: int
    symbol: str
    holder_name: str
    ownership_nature: str
    accession: str
    source_form: str
    period_end: date
    reporting_owner_count: int
    kept_document_id: str
    kept_shares: Decimal | None
    sibling_document_ids: tuple[str, ...]
    sibling_shares: tuple[Decimal | None, ...]


@dataclass(frozen=True)
class GroupVerdict:
    affected: bool
    attribution: Attribution | None
    line_count: int
    lines_dropped: int
    identical_value: bool
    kept_is_max: bool | None
    naive_sum_delta: Decimal | None
    #: Set when the group cannot be classified at all.  These states are impossible if the
    #: writer and this query agree, which is exactly why they are surfaced rather than
    #: absorbed into a bucket that reads like a finding.
    invalid_reason: str | None = None


def classify_group(group: HoldingLineGroup) -> GroupVerdict:
    """Classify one group without deciding what the correct aggregate is.

    The census deliberately stops short of a repair rule.  It answers three questions, refuses
    the fourth, and refuses to classify at all when the group is self-inconsistent:

    1. Did the key discard a line?  ``lines_dropped``.
    2. How cheaply is the discard attributable?  ``single_filer`` means the accession wrote one
       reporting owner against this instrument, so every Table I line on it belongs to that
       person.  ``joint_filing`` means several, and ``<nonDerivativeTable>`` is a sibling of
       ``<reportingOwner>`` (#2385/#2408), so no line names its holder in the XML structure.
       ⚠ This is a COST axis, not an additivity boundary — see the caveat below.
    3. Is the survivor even the largest line?  ``kept_is_max`` is ``None`` when no line in the
       group carries an amount, which is a different state from "the survivor lost".
    4. How wrong is the stored figure?  NOT ANSWERED.  ``naive_sum_delta`` is the arithmetic a
       naive fix would produce and is reported so the wrong fix is visibly wrong, not so it can
       be quoted.

    ⚠⚠ **``single_filer`` does NOT mean "safe to sum" and ``joint_filing`` does NOT mean
    "insoluble".**  Instruction 5(b)(v) lets a joint filing carry positions owned separately by
    any co-filer, and lets co-owners file individually instead — so the packaging of a filing
    and the additivity of its lines are different questions.  Attribution also survives outside
    the XML parent/child structure: CNH's own footnotes name Exor directly and Agnelli
    indirectly.  What the split measures is where attribution is CHEAPEST, which is why it is
    the recommended repair ORDER on #3227 and not a claim about solubility.

    ⚠ ``identical_value`` is carried, not resolved — but note what it is NOT evidence of.  The
    group key already fixes the holder, so the owner fan-out (one line written against every
    reporting owner) duplicates a line BETWEEN groups, never within one.  Equal siblings inside
    a group are therefore two distinct source lines that happen to carry the same amount, and
    the live example is class mixing: CNH accession 0001193125-25-198794 reports 366,927,900
    common AND 366,927,900 special voting shares.  Summing those is wrong for one reporting
    person, before attribution is considered at all.
    """
    line_count = len(group.sibling_shares)

    invalid_reason: str | None = None
    if line_count == 0:
        invalid_reason = "group has no observation lines"
    elif group.reporting_owner_count < 1:
        invalid_reason = "accession wrote no reporting owner"
    elif group.kept_document_id not in group.sibling_document_ids:
        # The ``_current`` survivor must BE one of the lines the group is built from.  If it is
        # not, the projection and the observations disagree and ``line_count - 1`` is not a
        # count of anything.
        invalid_reason = "survivor is not one of the group's lines"

    if invalid_reason is not None:
        return GroupVerdict(
            affected=False,
            attribution=None,
            line_count=line_count,
            lines_dropped=0,
            identical_value=False,
            kept_is_max=None,
            naive_sum_delta=None,
            invalid_reason=invalid_reason,
        )

    attribution: Attribution = "single_filer" if group.reporting_owner_count == 1 else "joint_filing"

    if line_count == 1:
        return GroupVerdict(
            affected=False,
            attribution=attribution,
            line_count=line_count,
            lines_dropped=0,
            identical_value=False,
            kept_is_max=None,
            naive_sum_delta=None,
        )

    valued = [share for share in group.sibling_shares if share is not None]
    kept_is_max: bool | None = None
    naive_sum_delta: Decimal | None = None
    if valued:
        kept_is_max = group.kept_shares is not None and group.kept_shares >= max(valued)
        naive_sum_delta = sum(valued, Decimal(0)) - (group.kept_shares or Decimal(0))

    return GroupVerdict(
        affected=True,
        attribution=attribution,
        line_count=line_count,
        lines_dropped=line_count - 1,
        identical_value=len(valued) == line_count and len(set(valued)) == 1,
        kept_is_max=kept_is_max,
        naive_sum_delta=naive_sum_delta,
    )


# ``_current`` joined to every same-accession sibling line in observations.  The join keys are
# the full natural key plus the accession and period, so a holder whose ``_current`` row came
# from a DIFFERENT filing than the group under test cannot be matched into it.
_CENSUS_SQL: Final[str] = """
WITH cur AS (
    SELECT c.instrument_id,
           c.holder_identity_key,
           c.holder_name,
           c.ownership_nature,
           c.shares AS kept_shares,
           c.period_end,
           c.source AS source_form,
           c.source_document_id AS kept_document_id,
           split_part(c.source_document_id, ':', 1) AS accession
      FROM ownership_insiders_current c
     WHERE strpos(c.source_document_id, %(marker)s) > 0
),
-- The (instrument, accession) pairs a ``_current`` row actually came from.  Seeding ``lines``
-- from this rather than from the whole holdings corpus is not just speed: ``lines`` is
-- referenced twice, so Postgres materialises it, and an unrestricted version scans all 125
-- observation partitions and JIT-compiles 537 functions to group 312,848 rows of which only
-- a few thousand can ever join.  Every sibling of a ``cur`` group shares its accession AND
-- its instrument, so the restriction cannot drop a sibling or under-count an owner.
scope AS (
    SELECT DISTINCT instrument_id, accession FROM cur
),
lines AS (
    SELECT o.instrument_id,
           o.holder_identity_key,
           o.ownership_nature,
           o.period_end,
           split_part(o.source_document_id, ':', 1) AS accession,
           o.source_document_id,
           o.shares
      FROM ownership_insiders_observations o
      JOIN scope s
        ON s.instrument_id = o.instrument_id
       AND s.accession = split_part(o.source_document_id, ':', 1)
     WHERE strpos(o.source_document_id, %(marker)s) > 0
       AND o.known_to IS NULL
),
owners AS (
    SELECT instrument_id, accession, count(DISTINCT holder_identity_key) AS reporting_owner_count
      FROM lines
     GROUP BY 1, 2
),
grouped AS (
    SELECT instrument_id, holder_identity_key, ownership_nature, period_end, accession,
           array_agg(source_document_id ORDER BY source_document_id) AS sibling_document_ids,
           array_agg(shares ORDER BY source_document_id) AS sibling_shares
      FROM lines
     GROUP BY 1, 2, 3, 4, 5
)
SELECT cur.instrument_id,
       COALESCE(i.symbol, '?') AS symbol,
       cur.holder_name,
       cur.ownership_nature,
       cur.accession,
       cur.source_form,
       cur.period_end,
       owners.reporting_owner_count,
       cur.kept_document_id,
       cur.kept_shares,
       grouped.sibling_document_ids,
       grouped.sibling_shares
  FROM cur
  JOIN grouped USING (instrument_id, holder_identity_key, ownership_nature, period_end, accession)
  JOIN owners ON owners.instrument_id = cur.instrument_id AND owners.accession = cur.accession
  LEFT JOIN instruments i ON i.instrument_id = cur.instrument_id
"""


#: Counted independently of ``_CENSUS_SQL``.  The reconciliation below is worthless unless one
#: side of it is measured without the join the other side depends on -- an inner join that lost
#: every row would otherwise still satisfy ``unaffected + affected == total``.
_CURRENT_POPULATION_SQL: Final[str] = """
SELECT count(*) FROM ownership_insiders_current WHERE strpos(source_document_id, %(marker)s) > 0
"""


def count_current_population(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(_CURRENT_POPULATION_SQL, {"marker": _HOLDING_ROW_MARKER}).fetchone()
    return int(row[0]) if row else 0


def load_groups(conn: psycopg.Connection[Any]) -> list[HoldingLineGroup]:
    rows = conn.execute(_CENSUS_SQL, {"marker": _HOLDING_ROW_MARKER}).fetchall()
    return [
        HoldingLineGroup(
            instrument_id=int(row[0]),
            symbol=str(row[1]),
            holder_name=str(row[2]),
            ownership_nature=str(row[3]),
            accession=str(row[4]),
            source_form=str(row[5]),
            period_end=row[6],
            reporting_owner_count=int(row[7]),
            kept_document_id=str(row[8]),
            kept_shares=row[9],
            sibling_document_ids=tuple(str(item) for item in row[10]),
            sibling_shares=tuple(row[11]),
        )
        for row in rows
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--examples", type=int, default=10, help="worked rows to print, worst naive delta first")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        population = count_current_population(conn)
        groups = load_groups(conn)

    verdicts = [(group, classify_group(group)) for group in groups]
    affected = [(group, verdict) for group, verdict in verdicts if verdict.affected]
    invalid = [(group, verdict) for group, verdict in verdicts if verdict.invalid_reason is not None]
    single_line = len(verdicts) - len(affected) - len(invalid)

    # The reconciliation is against ``population``, counted by a SEPARATE query that does not
    # go through the join.  ``unjoined`` is the number of ``_current`` rows the census could not
    # match to an observation group at all -- an inner join drops those silently, and a census
    # that reported only its own joined rows would call that a clean population.
    unjoined = population - len(verdicts)
    print("=== population ===")
    print(f"_current rows from a :NDH: line   {population:>10,}   (counted independently)")
    print(f"  matched to an observation group {len(verdicts):>10,}")
    print(f"    single-line accession (ok)    {single_line:>10,}")
    print(f"    multi-line, a line discarded  {len(affected):>10,}")
    print(f"    UNCLASSIFIABLE                {len(invalid):>10,}")
    print(f"  NOT matched (join lost them)    {unjoined:>10,}")
    if unjoined != 0 or invalid:
        print("  ⚠ a non-zero row on either of those two lines is a defect in this census or in")
        print("    the projection, NOT a finding about Table I. Investigate before quoting anything.")
    for _, verdict in invalid[:5]:
        print(f"      unclassifiable: {verdict.invalid_reason}")

    by_attribution: Counter[str] = Counter()
    instruments: dict[str, set[int]] = {"single_filer": set(), "joint_filing": set()}
    lines_dropped: Counter[str] = Counter()
    by_form: Counter[str] = Counter()
    for group, verdict in affected:
        assert verdict.attribution is not None  # noqa: S101 - narrowed by verdict.affected
        by_attribution[verdict.attribution] += 1
        instruments[verdict.attribution].add(group.instrument_id)
        lines_dropped[verdict.attribution] += verdict.lines_dropped
        by_form[group.source_form] += 1

    all_instruments = instruments["single_filer"] | instruments["joint_filing"]
    both = instruments["single_filer"] & instruments["joint_filing"]
    print("\n=== attribution: where attribution is CHEAPEST, not where it is possible ===")
    for attribution in ("single_filer", "joint_filing"):
        print(
            f"{attribution:<14} rows {by_attribution[attribution]:>7,}"
            f"   instruments {len(instruments[attribution]):>6,}"
            f"   lines dropped {lines_dropped[attribution]:>7,}"
        )
    # NOT a partition of instruments: an instrument can hold rows from both a single-filer and
    # a joint accession, so the two instrument counts overlap and must never be added.
    print(f"{'union':<14} rows {len(affected):>7,}   instruments {len(all_instruments):>6,}   in both {len(both):>6,}")

    print("\n=== form (the marker is holdings, NOT Form 3 -- a 4 or 5 can carry holdings too) ===")
    for form, count in sorted(by_form.items(), key=lambda pair: -pair[1]):
        print(f"{form:<14} rows {count:>7,}")

    # Distinct source lines, which is what a Table I claim is ABOUT.  A dropped observation row
    # is replicated by the owner fan-out (one line per reporting owner) and by the share-class
    # instrument fan-out (#1117), so the row count overstates the filings' own line loss.
    touched = {doc for group, _ in affected for doc in group.sibling_document_ids}
    survived = {group.kept_document_id for group, _ in affected}
    print("\n=== lines, deduplicated to the filing ===")
    rows_dropped = sum(verdict.lines_dropped for _, verdict in affected)
    print(f"observation rows dropped          {rows_dropped:>10,}   (replicated by owner + class fan-out)")
    print(f"distinct Table I lines touched    {len(touched):>10,}")
    print(f"  of which reach _current         {len(survived):>10,}")
    print(f"  of which do not                 {len(touched - survived):>10,}   ← the filings' own line loss")
    print(f"distinct accessions               {len({group.accession for group, _ in affected}):>10,}")

    identical = sum(1 for _, verdict in affected if verdict.identical_value)
    kept_not_max = sum(1 for _, verdict in affected if verdict.kept_is_max is False)
    unvalued = sum(1 for _, verdict in affected if verdict.kept_is_max is None)
    print("\n=== shape of the discarded lines ===")
    print(f"survivor is not the largest line  {kept_not_max:>10,}   (⚠ not a correctness test: the largest")
    print("                                                line can be another share class)")
    print(f"every line carries one amount     {identical:>10,}   (two distinct lines of equal size, e.g.")
    print("                                                CNH's common + special voting)")
    print(f"no line carries an amount         {unvalued:>10,}")

    delta = sum((verdict.naive_sum_delta or Decimal(0) for _, verdict in affected), Decimal(0))
    print(f"\nnaive sum delta  {delta:,.0f} shares")
    print("  ⚠ UPPER BOUND, and not even signed reliably. Mixes share classes, double-counts the")
    print("    owner fan-out, and ignores that a line may already be the entity's whole holding.")

    worked = sorted(affected, key=lambda pair: pair[1].naive_sum_delta or Decimal(0), reverse=True)
    print(f"\n=== worked rows (worst naive delta first, {args.examples}) ===")
    for group, verdict in worked[: args.examples]:
        kept = f"{group.kept_shares:,.0f}" if group.kept_shares is not None else "-"
        print(
            f"{group.symbol:<10} {group.holder_name[:26]:<26} {group.ownership_nature:<10} {group.source_form:<7}"
            f" {group.period_end} lines={verdict.line_count:<3} owners={group.reporting_owner_count:<3}"
            f" kept={kept:>16}"
        )


if __name__ == "__main__":
    main()
