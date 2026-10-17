"""#3349 — ``_INSIDER_JOINT_DIRECT_LINES_SQL`` and the flag it drives, against a real DB.

Each case changes ONE clause of the predicate from the flagged STAA shape, so each clause is
pinned by a fixture it alone rejects (prevention-log, #2240).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo
from app.services.ownership_rollup import (
    _collect_canonical_holders_from_current,
    _dedup_by_priority,
    _reconcile_owner_once,
)

_ACCN = "0000919574-26-001617"
_OWNER = "0001278386"
_CO_FILER = "0001278387"
_LATER_ACCN = "0000919574-26-009999"


@dataclass(frozen=True)
class _Case:
    co_filer: bool = True
    direct_shares: str = "16123842"
    indirect_shares: str = "16123842"
    direct_accession: str = _ACCN
    direct_document_id: str | None = None  # defaults to ``direct_accession``, an XML row
    # A later filing's ``direct`` balance, which replaces the joint ``D`` row in ``_current``.
    later_direct: str | None = None


_STAA = _Case()

_CASES = [
    pytest.param(_STAA, True, "16123842", id="joint_equal_xml_pair_is_a_restatement"),
    pytest.param(replace(_STAA, co_filer=False), False, "32247684", id="single_owner_sums"),
    pytest.param(replace(_STAA, indirect_shares="500"), False, "16124342", id="unequal_lots_sum"),
    pytest.param(
        replace(_STAA, direct_accession="0000919574-25-000001"), False, "32247684", id="direct_on_another_filing_sums"
    ),
    # A dataset ``direct`` is a DERA relationship flag, not a ``D`` line (#2386). Observations
    # keep it beside the XML ``I`` row (``_current`` de-collides it away), so only the
    # ``indirect`` lot is counted either way; the flag is what this pins.
    pytest.param(
        replace(_STAA, direct_document_id=f"{_ACCN}:NDT:8924078"),
        False,
        "16123842",
        id="dataset_direct_is_not_a_d_line",
    ),
    # Codex ckpt-2: the partner is looked up in live observations, not ``_current``.
    pytest.param(replace(_STAA, later_direct="9000000"), True, "9000000", id="later_direct_keeps_the_restatement"),
]


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _record(
    conn: psycopg.Connection[Any],
    iid: int,
    *,
    nature: str,
    shares: str,
    accession: str,
    doc_id: str,
    period_end: date = date(2026, 3, 13),
) -> None:
    oo.record_insider_observation(
        conn,
        instrument_id=iid,
        holder_cik=_OWNER,
        holder_name="BROADWOOD PARTNERS, L.P.",
        ownership_nature=nature,  # type: ignore[arg-type]
        source="form3",
        source_document_id=doc_id,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=datetime(2026, 3, 16, tzinfo=UTC),
        period_start=None,
        period_end=period_end,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
    )


def _seed(conn: psycopg.Connection[Any], iid: int, case: _Case) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, f"T3349-{iid}", f"Test 3349 Co {iid}"),
        )
        for accession in {_ACCN, case.direct_accession, _LATER_ACCN}:
            cur.execute(
                "INSERT INTO insider_filings (accession_number, instrument_id, document_type) VALUES (%s, %s, '3')",
                (accession, iid),
            )
            filers = [_OWNER, _CO_FILER] if case.co_filer else [_OWNER]
            for cik in filers:
                cur.execute(
                    "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, %s)",
                    (accession, cik, f"Filer {cik}"),
                )
    _record(
        conn,
        iid,
        nature="direct",
        shares=case.direct_shares,
        accession=case.direct_accession,
        doc_id=case.direct_document_id or case.direct_accession,
    )
    _record(
        conn,
        iid,
        nature="indirect",
        shares=case.indirect_shares,
        accession=_ACCN,
        doc_id=_ACCN,
    )
    if case.later_direct is not None:
        _record(
            conn,
            iid,
            nature="direct",
            shares=case.later_direct,
            accession=_LATER_ACCN,
            doc_id=_LATER_ACCN,
            period_end=date(2026, 6, 1),
        )
    oo.refresh_insiders_current(conn, instrument_id=iid)
    conn.commit()


@pytest.mark.parametrize(("case", "flagged", "owner_figure"), _CASES)
def test_flag_and_owner_figure(conn: psycopg.Connection[Any], case: _Case, flagged: bool, owner_figure: str) -> None:
    iid = 933_490
    _seed(conn, iid, case)
    cands = _collect_canonical_holders_from_current(conn, iid)
    assert [c.joint_di_restatement for c in cands if c.ownership_nature == "indirect"] == [flagged]
    # Owner-once keeps each counted lot as its own row; the owner's figure is their sum.
    rows = _reconcile_owner_once(_dedup_by_priority(cands))["insiders"]
    assert sum((h.shares for h in rows), Decimal(0)) == Decimal(owner_figure)
