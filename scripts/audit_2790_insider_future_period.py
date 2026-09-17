"""#2790 — measure stored insider observations whose date postdates their filing.

17 CFR 240.16a-3(g) requires a Form 4 "before the end of the second business day
following the day on which the subject transaction has been executed", and
16a-3(f) puts Form 5 within 45 days *after* the fiscal year it reports. In both
the filing follows the reported date, so ``period_end > filed_at`` is impossible.

⚠⚠ **A Form 3 is the opposite and must NOT be corrected.** Exchange Act
§16(a)(2) sets only *latest* bounds — "by the effective date of a registration
statement", "within 10 days after" — never an earliest one, so a Form 3 filed
ahead of a known future event date is correct.

That distinction is why this script does **not** select on ``source``.
``sec_insider_dataset_ingest._map_form_to_source`` maps every non-``3`` form
(including blank/unknown) to ``form4``, so ``source = 'form4'`` contains real
Form 3 holdings. Measured 2026-09-17: of the 1,147 breaching rows, **101 carry a
form4-mapped source but sit on accessions that produced only ``:NDH:`` holdings
rows and never an ``:NDT:`` transaction row** — the same signature as the 165
rows already typed ``form3`` (control: 165/165 also ``NDH``-only). A Form 3 has
no transaction table, so ``:NDT:`` implies Form 4/5 and ``:NDH:``-only does not.
Selecting on ``source`` would soft-delete those 101 correct rows.

The predicate therefore attributes the form instead:

* exclude anything carrying ``:NDH:`` — holdings rows are form-ambiguous here;
* include a row only when ``:NDT:`` marks it a transaction (⇒ Form 4/5) **or**
  ``sec_filing_manifest.form`` explicitly says ``4``/``4/A``/``5``/``5/A``.

⚠⚠ **This instrument does NOT correct anything, and the correction is
deliberately NOT shipped.** It scopes and reconciles the population so a
follow-up can act on a measured denominator, and stops there.

The correction is blocked on a source-rule conflict this ticket surfaced.
``insider_transactions.evaluate_insider_date_validity`` exempts
``transaction_timeliness == 'E'`` on the premise (``sql/057:264``) that ``E``
means "filed early (before the event)". The SEC EDGAR Ownership XML Technical
Specification §4.3.8.2 says otherwise: on a "4" submission, *"By definition, a
'4' transaction is on time… By definition, a '5' transaction is early"* — ``E``
marks a **Form-5-eligible transaction voluntarily reported early on a Form 4**,
which says nothing about the transaction postdating the filing. ``sql/057``
contradicts itself on the point (``:264`` "before the event" vs ``:338`` "before
the deadline").

Until that is resolved, the correctable set is ambiguous exactly where it
matters: **24 of the 778 Form 4/5-attributable rows resolve to an ``E``
transaction**, and a further 584 come from the bulk DERA drain, which stores no
timeliness at all and therefore cannot be resolved either way from stored data.
Correcting them would contradict the live ingest gate; exempting them all would
correct almost nothing. The ingest gate shipped in this PR stops the bleed
without depending on the answer.

⚠ Deliberately NOT done: inferring the intended year for the 300-399 day band
(the wrong-year typo shape). That is a *repair*, not a correction, and a 2047
date does not identify what was meant at all.

⚠ Read-path consequence, recorded because it is not obvious from the storage
change: ``app/services/ownership_history.py:189`` filters closed rows, so
``as_of_min``/``as_of_max`` move retroactively for affected instruments.

Read-only: the connection is opened ``read_only``. Exit status is 1 when the
scope and the exempt set fail to reconcile against the whole breaching
population, because a predicate that silently covers neither is the failure mode
that makes a correction look safe.

Run::

    PYTHONPATH=. uv run python scripts/audit_2790_insider_future_period.py
"""

from __future__ import annotations

import argparse
import sys
from typing import LiteralString, cast

import psycopg

from app.config import settings
from scripts._dev_guard import assert_dev_environment

# The attribution predicate. Shared verbatim between --plan and --apply so the
# scope that is reported cannot differ from the scope that is changed.
_SCOPE = """
    period_end > (filed_at AT TIME ZONE 'UTC')::date
AND known_to IS NULL
AND source_document_id NOT LIKE '%:NDH:%'
AND (
      source_document_id LIKE '%:NDT:%'
   OR EXISTS (SELECT 1 FROM sec_filing_manifest m
               WHERE m.accession_number = o.source_accession
                 AND m.form IN ('4', '4/A', '5', '5/A'))
)
"""


def _sql(text: str) -> LiteralString:
    """Narrow a build-time-constant SQL string for the type checker."""
    return cast(LiteralString, text)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()

    assert_dev_environment()
    failures: list[str] = []

    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute(
                _sql(f"""
                SELECT count(*), count(DISTINCT instrument_id), count(DISTINCT source_accession),
                       min(period_end), max(period_end)
                FROM ownership_insiders_observations o
                WHERE {_SCOPE}
                """)
            )
            n, instruments, accessions, lo, hi = cur.fetchone()  # type: ignore[misc]
            print(f"Form 4/5-attributable : {n:,} live rows / {instruments:,} instruments / {accessions:,} accessions")
            print(f"                        period_end range {lo} .. {hi}")

            # The exempt side is printed every run, not assumed. An over-broad
            # predicate shows up here as this number falling.
            cur.execute(
                _sql("""
                SELECT count(*) FROM ownership_insiders_observations o
                WHERE period_end > (filed_at AT TIME ZONE 'UTC')::date
                  AND known_to IS NULL
                  AND source_document_id LIKE '%:NDH:%'
                """)
            )
            exempt = cur.fetchone()[0]  # type: ignore[index]
            print(f"Form 3 / holdings     : {exempt:,} live rows that are CORRECT and must never be touched")

            cur.execute(
                _sql("""
                SELECT count(*) FROM ownership_insiders_observations
                WHERE period_end > (filed_at AT TIME ZONE 'UTC')::date AND known_to IS NULL
                """)
            )
            total_live = cur.fetchone()[0]  # type: ignore[index]
            print(f"total live breaching  : {total_live:,}")
            if n + exempt != total_live:
                failures.append(
                    f"scope {n:,} + exempt {exempt:,} = {n + exempt:,} does not reconcile against "
                    f"{total_live:,} live breaching rows — the predicate neither covers nor exempts "
                    "every row, so something is being silently dropped"
                )

            # The blocker, measured rather than described. These two counts are
            # why no correction ships with this PR.
            cur.execute(
                _sql(f"""
                SELECT
                  count(*) FILTER (WHERE EXISTS (
                      SELECT 1 FROM insider_transactions t
                       WHERE t.accession_number = o.source_accession
                         AND t.txn_date = o.period_end
                         AND t.transaction_timeliness = 'E')),
                  count(*) FILTER (WHERE NOT EXISTS (
                      SELECT 1 FROM insider_transactions t
                       WHERE t.accession_number = o.source_accession
                         AND t.txn_date = o.period_end))
                FROM ownership_insiders_observations o
                WHERE {_SCOPE}
                """)
            )
            early, unresolvable = cur.fetchone()  # type: ignore[misc]
            # ⚠ The two FILTERs must match on (accession, period_end), not on
            # accession alone. An accession can carry typed transactions while
            # carrying none at THIS observation's date — the XML parser skips a
            # transaction the bulk drain keeps — and matching on accession only
            # would call that row's timeliness "resolved" and count it as safely
            # correctable. It is not: nothing establishes whether it qualifies
            # for the E exemption. (Codex checkpoint 2.)
            print(f"  of which timeliness='E' (the live gate KEEPS these) : {early:,}")
            print(f"  of which unresolvable (bulk DERA, no timeliness)    : {unresolvable:,}")
            print(f"  correctable under BOTH readings of 'E'             : {n - early - unresolvable:,}")

    for f in failures:
        print(f"  ⚠ INVARIANT FAILED: {f}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
