"""Migration 258 — ``idx_def14a_holdings_accession`` must be USABLE, not merely present.

#2171. Four statements in the DEF 14A rewash and ingest paths key on
``accession_number`` alone (``rewash_filings.py`` :518, :587, :720, :851). An
existence assertion cannot tell a usable index from an unusable one, and the
unusable variant is the one the issue proposed: a partial index
``WHERE instrument_id IS NOT NULL`` is invisible to the three statements that do
not carry that predicate, because ``instrument_id`` is nullable and Postgres
cannot prove the implication. Such an index passes "does it exist?" and leaves
three of four seq scans in place.

So the test drives the planner instead. ``SET enable_seqscan = off`` makes a seq
scan expensive but still legal, so the planner falls back to one whenever no
index is *usable* for the shape — which is exactly the discriminator wanted. At
test-fixture row counts a seq scan would otherwise always win on cost, so this
is also the only way to assert the plan at all.

The query strings are the call-site shapes, parameterless so ``EXPLAIN`` can run
them without executing the DELETE.

Division of labour between the two tests here, established by revert-probe and
worth stating because it is not obvious:

* The PLAN test catches the partial-index defect. Probed: rebuilding migration
  258 as ``(accession_number) WHERE instrument_id IS NOT NULL`` fails it for
  :518, :587 and :720, and correctly still passes for :851 — the one shape a
  partial index does serve.
* The STRUCTURAL test catches wrong column ORDER. The plan test cannot: given
  ``(instrument_id, accession_number)`` Postgres will happily scan the whole
  index and report ``Index Cond: (accession_number = …)`` on it, so the plan
  names our index and looks healthy. Probed: that variant passes all four plan
  assertions and fails the structural one. At fixture row counts cost is the
  only thing separating "leads with the column" from "contains the column", and
  cost at that scale is a fixture artefact, not an invariant.
"""

from __future__ import annotations

from typing import LiteralString

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401

_INDEX = "idx_def14a_holdings_accession"

# Verbatim predicate shapes from the call sites. A literal accession stands in
# for the bound parameter — the plan shape is what is under test, not the value.
_ACC: LiteralString = "'0001193125-26-000001'"

_SHAPES: list[tuple[str, LiteralString]] = [
    (
        "rewash_filings.py:518 cover-label guard",
        f"SELECT holder_name FROM def14a_beneficial_holdings WHERE accession_number = {_ACC}",
    ),
    (
        "rewash_filings.py:587 existing-rows probe",
        f"SELECT issuer_cik, instrument_id FROM def14a_beneficial_holdings WHERE accession_number = {_ACC} LIMIT 1",
    ),
    (
        "rewash_filings.py:720 replace-then-insert DELETE",
        f"DELETE FROM def14a_beneficial_holdings WHERE accession_number = {_ACC}",
    ),
    (
        "rewash_filings.py:851 sibling-instrument set",
        "SELECT DISTINCT instrument_id FROM def14a_beneficial_holdings "
        f"WHERE accession_number = {_ACC} AND instrument_id IS NOT NULL",
    ),
]


def _plan(conn: psycopg.Connection[tuple], sql: LiteralString) -> str:
    with conn.cursor() as cur:
        # EXPLAIN without ANALYZE — one of the shapes is a DELETE.
        cur.execute(f"EXPLAIN {sql}")
        return "\n".join(row[0] for row in cur.fetchall())


def test_index_exists_and_leads_with_accession_number(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname = %s", (_INDEX,))
        row = cur.fetchone()
    ebull_test_conn.commit()
    assert row is not None, "migration 258 index missing"
    indexdef = row[0]
    # Leading column is the whole point: uq_def14a_holdings_instrument_accession_holder
    # already CONTAINS accession_number and is useless here because it leads with
    # instrument_id.
    assert "(accession_number" in indexdef, f"{_INDEX} does not lead with accession_number: {indexdef}"
    # A partial index would silently serve only the one shape that carries the
    # predicate; migration 258 must be unconditional.
    assert " WHERE " not in indexdef, f"{_INDEX} must not be partial: {indexdef}"


@pytest.mark.parametrize(("label", "sql"), _SHAPES, ids=[s[0] for s in _SHAPES])
def test_accession_keyed_shape_can_use_the_index(
    ebull_test_conn: psycopg.Connection[tuple], label: str, sql: LiteralString
) -> None:
    with ebull_test_conn.cursor() as cur:
        cur.execute("SET LOCAL enable_seqscan = off")
        plan = _plan(ebull_test_conn, sql)
    ebull_test_conn.rollback()
    assert _INDEX in plan, f"{label} cannot use {_INDEX}; planner chose:\n{plan}"
    # Named AND driving the scan. Without this, a plan that merely mentions the
    # index somewhere would satisfy the assertion. Matched per line rather than
    # as a prefix: shape :851 conjoins the IS NOT NULL check into the same
    # condition, so the text is ``Index Cond: ((accession_number = …) AND …)``.
    conds = [line.strip() for line in plan.splitlines() if line.strip().startswith("Index Cond:")]
    assert any("accession_number" in cond for cond in conds), (
        f"{label} reaches {_INDEX} without an accession_number index condition:\n{plan}"
    )
