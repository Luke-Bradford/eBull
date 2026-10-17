"""#3714: the sole writer of ``sec_filing_manifest.accepted_at`` — the EDGAR header, with stored evidence.

Spec: ``docs/proposals/etl/2026-10-17-3714-sec-acceptance-authority.md`` (Design 3-5). The table trigger from
``sql/502_sec_acceptance_header_authority.sql`` holds the column unless the transaction sets
``ebull.acceptance_writer = 'header'``; this module is the only place that sets it, and only in the same transaction
as the ``sec_acceptance_checks`` row that backs the value.

Ready work, per accession: no check row; or a retryable check whose ``next_retry_at`` has passed; or a final check
written under an older ``POLICY_VERSION``. Cohorts, in accession order within each:

* **T1** — priority sources created at or after the baseline epoch and not in the baseline, at most ``T1_SHARE``
  of the budget;
* **T2** — frozen-baseline rows of priority sources;
* **T3** — every other frozen-baseline row, which receives at least ``T3_FLOOR`` of the budget whenever it has ready
  work, so T2 cannot starve it. Unused T1 capacity passes to T2 and then T3.

Per row, after the fetch, one transaction: lock the manifest row, lock and re-read the check, upsert the check (a
retryable outcome never overwrites a final one), then on a final outcome write ``accepted_at`` (the conversion on
``found``, NULL on ``absent`` / ``invalid_value``). A retryable outcome leaves ``accepted_at`` untouched, so a legacy
value stays until a final answer replaces it.
"""

from __future__ import annotations

import collections
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, LiteralString

import psycopg

from app.providers.implementations.sec_header import FINAL_OUTCOMES, POLICY_VERSION, RETRYABLE_OUTCOMES, HeaderCheck

#: Sources whose ``accepted_at`` a reader uses (strategy_shock_mechanism, pead, insider_purchase) or will.
PRIORITY_SOURCES: Final[tuple[str, ...]] = ("sec_8k", "sec_10k", "sec_10q", "sec_form3", "sec_form4", "sec_form5")
#: Requests per fire; at a 10-minute cadence that is <= 1 req/s averaged against SEC's shared 10 req/s.
BUDGET: Final = 500
T1_SHARE: Final = 0.40
T3_FLOOR: Final = 0.20
#: Backoff is 1 h x 2^(attempts-1) through this many attempts, then a 7-day reconsideration, never abandonment.
BACKOFF_ATTEMPTS: Final = 6
_RECONSIDER: Final = timedelta(days=7)

Cohort = Literal["T1", "T2", "T3"]
RowResult = Literal[
    "found",
    "absent",
    "invalid_value",
    "transport_error",
    "http_error",
    "bad_response",
    "kept_final",
    "already_final",
    "vanished",
]

# ``m.cik`` must build a header URL (``header_url``); a row that cannot is never fetched, so it cannot poison a fire,
# and the census still counts it as never-attempted.
_READY: Final[LiteralString] = """
    m.cik ~ '^[0-9]{10}$'
    AND (c.accession_number IS NULL
     OR (c.next_retry_at IS NOT NULL AND c.next_retry_at <= %(now)s)
     OR (c.next_retry_at IS NULL AND c.policy_version < %(policy)s))
"""

_T1_SQL: Final[LiteralString] = (
    """
    SELECT m.accession_number, m.cik
    FROM sec_filing_manifest m
    LEFT JOIN sec_acceptance_checks c ON c.accession_number = m.accession_number
    WHERE m.created_at >= (SELECT epoch FROM sec_acceptance_epoch)
      AND m.source = ANY(%(priority)s)
      AND NOT EXISTS (SELECT 1 FROM sec_acceptance_legacy l WHERE l.accession_number = m.accession_number)
      AND """
    + _READY
    + """
    ORDER BY m.accession_number
    LIMIT %(limit)s
    """
)

_BASELINE_SQL: Final[LiteralString] = (
    """
    SELECT l.accession_number, m.cik
    FROM sec_acceptance_legacy l
    JOIN sec_filing_manifest m ON m.accession_number = l.accession_number
    LEFT JOIN sec_acceptance_checks c ON c.accession_number = l.accession_number
    WHERE (l.source = ANY(%(priority)s)) = %(priority_cohort)s
      AND """
    + _READY
    + """
    ORDER BY l.accession_number
    LIMIT %(limit)s
    """
)


@dataclass(frozen=True)
class ReadyRow:
    accession_number: str
    cik: str
    cohort: Cohort


@dataclass
class RefillReport:
    selected: dict[Cohort, int] = field(default_factory=dict)
    results: collections.Counter[RowResult] = field(default_factory=collections.Counter)

    @property
    def attempted(self) -> int:
        return sum(self.selected.values())

    @property
    def final(self) -> int:
        return sum(self.results[outcome] for outcome in FINAL_OUTCOMES)


def next_retry_at(attempts: int, now: datetime) -> datetime:
    """When a retryable check becomes ready again, given the attempts made so far (>= 1)."""
    if attempts > BACKOFF_ATTEMPTS:
        return now + _RECONSIDER
    return now + timedelta(hours=2 ** (attempts - 1))


def _ready(
    conn: psycopg.Connection[Any], sql: LiteralString, limit: int, now: datetime, **params: Any
) -> list[tuple[str, str]]:
    if limit <= 0:
        return []
    rows = conn.execute(
        sql, {"now": now, "policy": POLICY_VERSION, "priority": list(PRIORITY_SOURCES), "limit": limit, **params}
    ).fetchall()
    return [(str(accession), str(cik)) for accession, cik in rows]


def select_ready(conn: psycopg.Connection[Any], *, budget: int, now: datetime) -> list[ReadyRow]:
    """The fire's work list under the cohort capacities (module docstring)."""
    t1 = _ready(conn, _T1_SQL, math.floor(budget * T1_SHARE), now)
    rest = budget - len(t1)
    # T3's floor is claimed by its actual demand first, so a sparse T3 hands the unclaimed part to T2.
    t3_floor = min(math.ceil(budget * T3_FLOOR), rest)
    t3 = _ready(conn, _BASELINE_SQL, t3_floor, now, priority_cohort=False)
    t2 = _ready(conn, _BASELINE_SQL, rest - len(t3), now, priority_cohort=True)
    if len(t3) == t3_floor:
        # Top T3 up with whatever T2 left; same order and no writes since, so this re-reads a superset.
        t3 = _ready(conn, _BASELINE_SQL, rest - len(t2), now, priority_cohort=False)
    seen: set[str] = set()
    picked: list[ReadyRow] = []
    for cohort, rows in (("T1", t1), ("T2", t2), ("T3", t3)):
        for accession, cik in rows:
            if accession not in seen:
                seen.add(accession)
                picked.append(ReadyRow(accession, cik, cohort))  # type: ignore[arg-type]
    return picked


_UPSERT_CHECK = """
    INSERT INTO sec_acceptance_checks (
        accession_number, outcome, raw_value, acceptance_utc, url, http_status, diagnostic,
        policy_version, attempts, checked_at, next_retry_at
    ) VALUES (
        %(accession)s, %(outcome)s, %(raw_value)s, %(acceptance_utc)s, %(url)s, %(http_status)s, %(diagnostic)s,
        %(policy)s, %(attempts)s, %(now)s, %(next_retry_at)s
    )
    ON CONFLICT (accession_number) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        raw_value = EXCLUDED.raw_value,
        acceptance_utc = EXCLUDED.acceptance_utc,
        url = EXCLUDED.url,
        http_status = EXCLUDED.http_status,
        diagnostic = EXCLUDED.diagnostic,
        policy_version = EXCLUDED.policy_version,
        attempts = EXCLUDED.attempts,
        checked_at = EXCLUDED.checked_at,
        next_retry_at = EXCLUDED.next_retry_at
    -- A retryable outcome never overwrites a final one.
    WHERE sec_acceptance_checks.outcome NOT IN ('found', 'absent', 'invalid_value')
       OR EXCLUDED.outcome IN ('found', 'absent', 'invalid_value')
    RETURNING attempts
"""


def record_check(conn: psycopg.Connection[Any], accession: str, check: HeaderCheck, *, now: datetime) -> RowResult:
    """Store one fetch's outcome and, on a final one, the ``accepted_at`` it implies. One transaction."""
    with conn.transaction():
        manifest = conn.execute(
            "SELECT 1 FROM sec_filing_manifest WHERE accession_number = %s FOR UPDATE", (accession,)
        ).fetchone()
        if manifest is None:
            return "vanished"
        prior = conn.execute(
            """
            SELECT outcome, policy_version, attempts FROM sec_acceptance_checks
            WHERE accession_number = %s FOR UPDATE
            """,
            (accession,),
        ).fetchone()
        if prior is not None and prior[0] in FINAL_OUTCOMES:
            if prior[1] >= POLICY_VERSION:
                return "already_final"
            if check.outcome in RETRYABLE_OUTCOMES:
                # An older-policy final answer stays the best evidence; the row stays ready for the next fire.
                return "kept_final"
        attempts = 1 if prior is None else int(prior[2]) + 1
        is_final = check.outcome in FINAL_OUTCOMES
        written = conn.execute(
            _UPSERT_CHECK,
            {
                "accession": accession,
                "outcome": check.outcome,
                "raw_value": check.raw_value,
                "acceptance_utc": check.acceptance_utc,
                "url": check.url,
                "http_status": check.http_status,
                "diagnostic": check.diagnostic,
                "policy": POLICY_VERSION,
                "attempts": attempts,
                "now": now,
                "next_retry_at": None if is_final else next_retry_at(attempts, now),
            },
        ).fetchone()
        if written is None:
            return "kept_final"
        if is_final:
            conn.execute("SELECT set_config('ebull.acceptance_writer', 'header', true)")
            conn.execute(
                "UPDATE sec_filing_manifest SET accepted_at = %s WHERE accession_number = %s",
                (check.acceptance_utc, accession),
            )
        return check.outcome


def run_sec_acceptance_refill(
    conn: psycopg.Connection[Any],
    fetch: Callable[[str, str], HeaderCheck],
    *,
    budget: int = BUDGET,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RefillReport:
    """One fire: select the ready work, then fetch and record each row in its own transaction.

    ``fetch(cik, accession)`` is ``sec_header.fetch_header_acceptance`` bound to a provider. ``conn`` must be
    autocommit, so each ``record_check`` transaction is a real commit rather than a savepoint.
    """
    if not conn.autocommit:
        raise ValueError("run_sec_acceptance_refill needs an autocommit connection (one transaction per row)")
    rows = select_ready(conn, budget=budget, now=clock())
    report = RefillReport(selected=dict(collections.Counter(row.cohort for row in rows)))
    for row in rows:
        check = fetch(row.cik, row.accession_number)
        report.results[record_check(conn, row.accession_number, check, now=clock())] += 1
    return report
