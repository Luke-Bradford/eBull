"""#3714: each reader's decisions before and after the header acceptance refill, for every changed input. Read-only.

Spec ``docs/proposals/etl/2026-10-17-3714-sec-acceptance-authority.md`` (Readers of ``accepted_at``; Acceptance,
"per reader: before and after decisions ... for every event whose input changed").

"Before" is the frozen baseline ``sec_acceptance_legacy.legacy_accepted_at``; "after" is the current
``sec_filing_manifest.accepted_at``. An input changed when the two are distinct, NULL-safe. Rows created after the epoch
have no "before" and are out of scope. Each decision is made by the reader's own function:

* **shock** — ``strategy_shock_mechanism.sec_knowledge_at`` gives the knowledge time. The forms, the 8-K
  material-item filter and the instrument join are the reader's ``_MATERIAL_SEC_SQL``.
* **pead** — ``pead_outcomes.earliest_entry_date`` gives the entry date, for the reader's forms ``10-K`` and ``10-Q``.
  Both reader paths join the manifest on ``instrument_id``, so a NULL one is unread. ``filed_date`` is the XBRL
  ``financial_facts_raw.filed_date``, the reader's DB source. Where no fact row carries the accession, the manifest
  ``filed_at`` UTC date stands in and the row says so; it only matters on the NULL side. Every changed 10-K / 10-Q
  with an instrument is decided, fact or not, so this is a superset of the reader's events.
* **insider** — the guard in ``insider_purchase_outcomes._eligible_window``, ``acceptance.date() >= entry_date``,
  decides refused or not, for every ``4`` / ``4/A``. The reader's signal month is the filed month and its entry must
  fall in the next calendar month, so ``entry_date`` here is that month's first regular session by
  ``market_calendar.us_market_status``. ``filed_date`` is the manifest ``filed_at`` UTC date, because the reader's own
  source is the DERA archive. The reader guards a firm-month on its latest acceptance, which can cross the entry date
  only if some changed accession crosses it, so these accession-level flips are a superset of the reader's.

The gate also requires that every changed non-NULL "after" equals its current-policy ``found`` check; violations are
counted and listed.

    PYTHONPATH=. uv run python -m scripts.census_3714_reader_decisions --cohort T2 --out /tmp/reader_3714.jsonl
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final, LiteralString

import psycopg

from app.config import Settings
from app.providers.implementations.sec_header import POLICY_VERSION
from app.services.market_calendar import us_market_status
from app.services.pead_outcomes import earliest_entry_date
from app.services.sec_acceptance_refill import PRIORITY_SOURCES
from app.services.strategy_shock_mechanism import sec_knowledge_at

SHOCK_FORMS: Final = frozenset({"10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A"})
PEAD_FORMS: Final = frozenset({"10-K", "10-Q"})
INSIDER_FORMS: Final = frozenset({"4", "4/A"})

_CHANGED: Final[LiteralString] = """
    SELECT l.accession_number, l.source, m.form, m.filed_at, m.instrument_id,
           l.legacy_accepted_at, m.accepted_at, c.outcome, c.acceptance_utc, c.policy_version,
           EXISTS (
               SELECT 1 FROM eight_k_items i JOIN sec_8k_item_codes k ON k.code = i.item_code
               WHERE i.accession_number = l.accession_number AND k.severity IN ('material', 'critical')
           ) AS has_material_item
    FROM sec_acceptance_legacy l
    JOIN sec_filing_manifest m ON m.accession_number = l.accession_number
    LEFT JOIN sec_acceptance_checks c ON c.accession_number = l.accession_number
    WHERE l.legacy_accepted_at IS DISTINCT FROM m.accepted_at
      AND (%(all_cohorts)s OR l.source = ANY(%(priority)s))
    ORDER BY l.accession_number
"""

_FACT_FILED: Final[LiteralString] = """
    SELECT accession_number, min(filed_date), max(filed_date)
    FROM financial_facts_raw
    WHERE accession_number = ANY(%(accessions)s) AND form_type = ANY(%(forms)s)
    GROUP BY accession_number
"""

_MISSING_FROM_MANIFEST: Final[LiteralString] = """
    SELECT count(*) FROM sec_acceptance_legacy l
    WHERE NOT EXISTS (SELECT 1 FROM sec_filing_manifest m WHERE m.accession_number = l.accession_number)
      AND (%(all_cohorts)s OR l.source = ANY(%(priority)s))
"""


@dataclass(frozen=True)
class ChangedInput:
    accession_number: str
    form: str
    filed_at: datetime
    instrument_id: int | None
    before: datetime | None
    after: datetime | None
    has_material_item: bool


def utc_date(value: datetime) -> date:
    """The provider's filed-date identity of a date-only ``filed_at`` (``sec_knowledge_at``'s convention)."""
    return value.astimezone(UTC).date()


def first_session_of_next_month(filed_date: date) -> date:
    candidate = (filed_date.replace(day=1) + timedelta(days=32)).replace(day=1)
    while us_market_status(candidate) == "closed":
        candidate += timedelta(days=1)
    return candidate


def insider_refused(acceptance: datetime | None, entry_date: date) -> bool:
    """``_eligible_window``'s guard, byte for byte: a NULL acceptance skips it."""
    return acceptance is not None and acceptance.date() >= entry_date


def shock_decision(row: ChangedInput) -> dict[str, object] | None:
    if row.instrument_id is None or row.form not in SHOCK_FORMS:
        return None
    if row.form.startswith("8-K") and not row.has_material_item:
        return None
    before = sec_knowledge_at(filed_at=row.filed_at, accepted_at=row.before)
    after = sec_knowledge_at(filed_at=row.filed_at, accepted_at=row.after)
    return {"before": before, "after": after, "changed": before != after, "delta_s": (after - before).total_seconds()}


def pead_decision(
    row: ChangedInput, fact_filed: date | None, *, fact_ambiguous: bool = False
) -> dict[str, object] | None:
    """``fact_ambiguous``: fact rows disagree on ``filed_date``, so the manifest date stands in and the row says why."""
    if row.instrument_id is None or row.form not in PEAD_FORMS:
        return None
    filed_date = fact_filed if fact_filed is not None else utc_date(row.filed_at)
    before = earliest_entry_date(filed_date, row.before)
    after = earliest_entry_date(filed_date, row.after)
    return {
        "before": before,
        "after": after,
        "changed": before != after,
        "delta_days": (after - before).days,
        "filed_date_source": (
            "financial_facts_raw"
            if fact_filed is not None
            else "manifest_filed_at_fact_ambiguous"
            if fact_ambiguous
            else "manifest_filed_at"
        ),
    }


def insider_decision(row: ChangedInput) -> dict[str, object] | None:
    if row.form not in INSIDER_FORMS:
        return None
    entry_date = first_session_of_next_month(utc_date(row.filed_at))
    before = insider_refused(row.before, entry_date)
    after = insider_refused(row.after, entry_date)
    return {"entry_date": entry_date, "before_refused": before, "after_refused": after, "changed": before != after}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cohort", choices=("T2", "all"), default="T2", help="T2 = priority baseline rows only")
    parser.add_argument("--out", type=Path, help="JSON lines: every changed decision and every gate violation")
    args = parser.parse_args()
    params = {"priority": list(PRIORITY_SOURCES), "all_cohorts": args.cohort == "all"}
    lines: list[dict[str, object]] = []
    with psycopg.connect(Settings().database_url) as conn:
        missing = (conn.execute(_MISSING_FROM_MANIFEST, params).fetchone() or (0,))[0]
        raw = conn.execute(_CHANGED, params).fetchall()
        pead_accessions = [str(r[0]) for r in raw if r[2] in PEAD_FORMS and r[4] is not None]
        fact_rows = conn.execute(_FACT_FILED, {"accessions": pead_accessions, "forms": sorted(PEAD_FORMS)}).fetchall()
    fact_filed = {str(acc): lo for acc, lo, hi in fact_rows if lo == hi}
    ambiguous = sorted(str(acc) for acc, lo, hi in fact_rows if lo != hi)
    ambiguous_set = frozenset(ambiguous)
    print(f"cohort={args.cohort} changed_inputs={len(raw)} baseline_rows_missing_from_manifest={missing}")
    print(f"pead_fact_filed_date_ambiguous={len(ambiguous)}")
    lines.extend({"kind": "pead_filed_date_ambiguous", "accession_number": acc} for acc in ambiguous)

    header_mismatch = 0
    transitions: Counter[str] = Counter()
    counts: dict[str, Counter[str]] = {"shock": Counter(), "pead": Counter(), "insider": Counter()}
    for acc, _source, form, filed_at, instrument_id, before, after, outcome, acceptance_utc, policy, material in raw:
        transitions[f"{'NULL' if before is None else 'value'}->{'NULL' if after is None else 'value'}"] += 1
        if after is not None and not (outcome == "found" and policy == POLICY_VERSION and acceptance_utc == after):
            header_mismatch += 1
            lines.append({"kind": "after_not_header_value", "accession_number": acc, "after": after})
        row = ChangedInput(str(acc), str(form), filed_at, instrument_id, before, after, bool(material))
        decisions = {
            "shock": shock_decision(row),
            "pead": pead_decision(
                row, fact_filed.get(row.accession_number), fact_ambiguous=row.accession_number in ambiguous_set
            ),
            "insider": insider_decision(row),
        }
        for reader, decision in decisions.items():
            if decision is None:
                continue
            tally = counts[reader]
            tally["inputs_changed"] += 1
            if not decision["changed"]:
                continue
            tally["decisions_changed"] += 1
            if reader == "shock":
                tally[f"delta_s={decision['delta_s']:+.0f}"] += 1
            elif reader == "pead":
                tally[f"delta_days={decision['delta_days']:+d}"] += 1
            else:
                tally[f"refused {decision['before_refused']}->{decision['after_refused']}"] += 1
            lines.append({"kind": f"{reader}_decision_changed", "accession_number": acc, "form": form, **decision})

    for transition, count in sorted(transitions.items()):
        print(f"input transition={transition} rows={count}")
    print(f"gate changed_after_not_header_value={header_mismatch}")
    for reader, tally in counts.items():
        print(
            f"reader={reader} inputs_changed={tally.pop('inputs_changed', 0)} "
            f"decisions_changed={tally.pop('decisions_changed', 0)}"
        )
        for key, count in sorted(tally.items(), key=lambda item: -item[1]):
            print(f"  reader={reader} {key} rows={count}")
    if args.out is not None:
        args.out.write_text("".join(json.dumps(line, default=str) + "\n" for line in lines))
        print(f"wrote {len(lines)} rows to {args.out}")


if __name__ == "__main__":
    main()
