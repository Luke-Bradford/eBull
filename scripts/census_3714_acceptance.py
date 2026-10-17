"""#3714: census of the header-acceptance refill against the frozen pre-#3714 baseline. Read-only.

Spec ``docs/proposals/etl/2026-10-17-3714-sec-acceptance-authority.md`` (Design invariant, Acceptance gates).

Per cohort (T2 = baseline rows of priority sources, T3 = the other baseline rows, T1 = priority rows created after
the epoch), every state is counted against the cohort's frozen size: ``found``, ``absent``, ``invalid_value``,
retry-waiting, exhausted (retryable with >= 6 attempts), old-policy final, and never-attempted.

Invariants, NULL-safe, a row with no check counted as unchecked:

* **transitional** — a non-NULL ``accepted_at`` equals its ``found`` check's conversion, or it is the row's unchanged
  baseline value and the row has no final check yet;
* **converse** — every ``found`` check under the current policy has its conversion in ``accepted_at``.

``--out`` writes the accession-level rows (every invariant violation and every non-final baseline state) as JSON lines.
The header-vs-baseline offset histogram is the A/B: one line per (cohort, offset in seconds) over ``found`` rows.

    PYTHONPATH=. uv run python -m scripts.census_3714_acceptance --out /tmp/census_3714.jsonl
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Final, LiteralString

import psycopg

from app.config import Settings
from app.providers.implementations.sec_header import POLICY_VERSION
from app.services.sec_acceptance_refill import BACKOFF_ATTEMPTS, PRIORITY_SOURCES

_COHORTS: Final[LiteralString] = """
    WITH epoch AS (SELECT epoch FROM sec_acceptance_epoch),
    cohort AS (
        SELECT l.accession_number, CASE WHEN l.source = ANY(%(priority)s) THEN 'T2' ELSE 'T3' END AS cohort
        FROM sec_acceptance_legacy l
        UNION ALL
        SELECT m.accession_number, 'T1'
        FROM sec_filing_manifest m, epoch
        WHERE m.created_at >= epoch.epoch AND m.source = ANY(%(priority)s)
          AND NOT EXISTS (SELECT 1 FROM sec_acceptance_legacy l WHERE l.accession_number = m.accession_number)
    ),
    state AS (
        SELECT k.cohort, k.accession_number,
               CASE
                   WHEN c.accession_number IS NULL THEN 'never_attempted'
                   WHEN c.next_retry_at IS NULL AND c.policy_version < %(policy)s THEN 'old_policy_final'
                   WHEN c.next_retry_at IS NULL THEN c.outcome
                   WHEN c.attempts >= %(exhausted)s THEN 'exhausted'
                   ELSE 'retry_waiting'
               END AS state
        FROM cohort k LEFT JOIN sec_acceptance_checks c ON c.accession_number = k.accession_number
    )
"""

_STATES: Final[LiteralString] = _COHORTS + "SELECT cohort, state, count(*) FROM state GROUP BY 1, 2 ORDER BY 1, 2"

_NON_FINAL: Final[LiteralString] = (
    _COHORTS
    + """
    SELECT s.cohort, s.accession_number, s.state, c.outcome, c.http_status, c.attempts, c.next_retry_at
    FROM state s LEFT JOIN sec_acceptance_checks c ON c.accession_number = s.accession_number
    WHERE s.state NOT IN ('found', 'absent', 'invalid_value') AND s.cohort <> 'T1'
    ORDER BY s.cohort, s.accession_number
    """
)

_TRANSITIONAL: Final[LiteralString] = """
    SELECT m.accession_number, m.accepted_at, c.outcome, c.acceptance_utc, l.legacy_accepted_at
    FROM sec_filing_manifest m
    LEFT JOIN sec_acceptance_checks c ON c.accession_number = m.accession_number
    LEFT JOIN sec_acceptance_legacy l ON l.accession_number = m.accession_number
    WHERE m.accepted_at IS NOT NULL
      AND NOT COALESCE(c.outcome = 'found' AND c.acceptance_utc = m.accepted_at, false)
      AND NOT COALESCE(
          l.legacy_accepted_at = m.accepted_at AND (c.accession_number IS NULL OR c.next_retry_at IS NOT NULL), false
      )
    ORDER BY m.accession_number
"""

_CONVERSE: Final[LiteralString] = """
    SELECT c.accession_number, c.acceptance_utc, m.accepted_at
    FROM sec_acceptance_checks c
    LEFT JOIN sec_filing_manifest m ON m.accession_number = c.accession_number
    WHERE c.outcome = 'found' AND c.policy_version = %(policy)s
      AND c.acceptance_utc IS DISTINCT FROM m.accepted_at
    ORDER BY c.accession_number
"""

_OFFSETS: Final[LiteralString] = """
    SELECT CASE WHEN l.source = ANY(%(priority)s) THEN 'T2' ELSE 'T3' END AS cohort,
           extract(epoch FROM c.acceptance_utc - l.legacy_accepted_at)::bigint AS header_minus_legacy_s,
           count(*)
    FROM sec_acceptance_legacy l JOIN sec_acceptance_checks c ON c.accession_number = l.accession_number
    WHERE c.outcome = 'found'
    GROUP BY 1, 2 ORDER BY 1, 3 DESC
"""

_T1_HEALTH: Final[LiteralString] = """
    SELECT (SELECT count(*) FROM sec_filing_manifest
            WHERE source = ANY(%(priority)s) AND created_at > now() - interval '7 days') AS priority_created_7d,
           (SELECT now() - min(m.created_at) FROM sec_filing_manifest m
            LEFT JOIN sec_acceptance_checks c ON c.accession_number = m.accession_number
            WHERE m.created_at >= (SELECT epoch FROM sec_acceptance_epoch) AND m.source = ANY(%(priority)s)
              AND c.accession_number IS NULL) AS oldest_unchecked_t1_age,
           (SELECT epoch FROM sec_acceptance_epoch) AS epoch
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, help="accession-level JSON lines: violations and non-final states")
    args = parser.parse_args()
    params = {"priority": list(PRIORITY_SOURCES), "policy": POLICY_VERSION, "exhausted": BACKOFF_ATTEMPTS}
    lines: list[dict[str, object]] = []
    with psycopg.connect(Settings().database_url) as conn:
        priority_7d, oldest_age, epoch = conn.execute(_T1_HEALTH, params).fetchone() or (None, None, None)
        print(f"epoch={epoch} priority_created_7d={priority_7d} oldest_unchecked_t1_age={oldest_age}")
        for cohort, state, count in conn.execute(_STATES, params):
            print(f"state cohort={cohort} state={state} rows={count}")
        for cohort, offset, count in conn.execute(_OFFSETS, params):
            print(f"offset cohort={cohort} header_minus_legacy_s={offset} rows={count}")
        invariants: tuple[tuple[str, LiteralString], ...] = (("transitional", _TRANSITIONAL), ("converse", _CONVERSE))
        for name, sql in invariants:
            violations = conn.execute(sql, params).fetchall()
            print(f"invariant {name} violations={len(violations)}")
            lines += [{"kind": f"violation_{name}", "row": list(row)} for row in violations]
        if args.out is not None:
            lines += [{"kind": "non_final", "row": list(row)} for row in conn.execute(_NON_FINAL, params)]
            args.out.write_text("".join(json.dumps(line, default=str) + "\n" for line in lines))
            print(f"wrote {len(lines)} accession rows to {args.out}")


if __name__ == "__main__":
    main()
