"""#3227 item 3a — a winning ``:NDH:`` line becomes its accession's single-class line SUM.

``INSIDER_HOLDING_LINE_SUM_LATERAL`` is shared by both ``_current`` refreshers and the insider
history chart. Each case below states what the projection must render; the refusal cases all
build a fixture the SUM would otherwise accept, so each guard is pinned by a fixture it alone
rejects (a fixture the defect also satisfies pins nothing — prevention-log, #2240).

The DISTINCT ON winner among ``:NDH:`` siblings is the lexically smallest document id, so every
fixture gives that line the SMALLEST amount: "kept" is then distinguishable from both the sum and
the largest line.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo
from app.services.ownership_history import _insiders_history

_PERIOD_END = date(2026, 3, 18)
_FILED_AT = datetime(2026, 3, 19, tzinfo=UTC)
_ACCN = "0001104659-26-030863"
_CIK = "0001842281"


@dataclass(frozen=True)
class _Line:
    sk: int
    shares: str
    title: str | None = "Ordinary shares"
    di: str | None = "I"
    nature: str | None = None
    holder_cik: str = _CIK


_MNSO = (
    _Line(1, "100", nature="by Mini Investment Limited"),
    _Line(2, "300", nature="by YGF MN LIMITED"),
    _Line(3, "600", nature="by YYY MC LIMITED"),
)


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _seed(conn: psycopg.Connection[Any], iid: int, lines: tuple[_Line, ...], *, nature: str = "direct") -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, f"T3227-{iid}", f"Test 3227 Co {iid}"),
        )
    for ln in lines:
        oo.record_insider_observation(
            conn,
            instrument_id=iid,
            holder_cik=ln.holder_cik,
            holder_name=f"Holder {ln.holder_cik}",
            ownership_nature=nature,  # type: ignore[arg-type]
            source="form3",
            source_document_id=f"{_ACCN}:NDH:{1000 + ln.sk}",
            source_accession=_ACCN,
            source_field=None,
            source_url=None,
            filed_at=_FILED_AT,
            period_start=None,
            period_end=_PERIOD_END,
            ingest_run_id=uuid4(),
            shares=Decimal(ln.shares),
            security_title=ln.title,
            direct_indirect=ln.di,
            nature_of_ownership=ln.nature,
        )
    conn.commit()


def _current_shares(conn: psycopg.Connection[Any], iid: int) -> Decimal:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT shares FROM ownership_insiders_current WHERE instrument_id = %s AND holder_identity_key = %s",
            (iid, f"CIK:{_CIK}"),
        )
        row = cur.fetchone()
    assert row is not None
    return Decimal(row[0])


_CASES = [
    pytest.param(_MNSO, "1000", id="distinct_indirect_forms_of_one_class_sum"),
    pytest.param(
        (_Line(1, "100", di="D"), _Line(2, "300", nature="By Trust"), _Line(3, "600", nature="By LLC")),
        "1000",
        id="direct_plus_indirect_lines_sum",
    ),
    pytest.param(
        (_Line(1, "100", title="Common Shares", di="D"), _Line(2, "900", title="Special Voting Shares", di="D")),
        "100",
        id="two_classes_refuse",
    ),
    pytest.param(
        (_Line(1, "100", nature="By Trust"), _Line(2, "900", nature="By Trust")),
        "100",
        id="repeated_form_refuses",
    ),
    pytest.param(
        (_Line(1, "100"), _Line(2, "900", nature="By Trust")),
        "1000",
        id="null_nature_is_its_own_form",
    ),
    pytest.param(
        # YPF-shaped: one block reported once D and once I (Form 3 Instr. 5(b)(iv)).
        (_Line(1, "500", di="D", nature="Indirectly owned by shareholders"), _Line(2, "500")),
        "500",
        id="equal_amounts_refuse",
    ),
    pytest.param(
        (_Line(1, "100", title=None, nature="A"), _Line(2, "900", nature="B")),
        "100",
        id="unknown_title_refuses",
    ),
    pytest.param(
        (_Line(1, "100", title="  ", nature="A"), _Line(2, "900", title="  ", nature="B")),
        "100",
        id="blank_title_refuses",
    ),
    pytest.param(
        (
            _Line(1, "100", nature="A"),
            _Line(2, "900", nature="B"),
            _Line(1, "100", nature="A", holder_cik="0002117646"),
        ),
        "100",
        id="joint_filing_refuses",
    ),
]


@pytest.mark.parametrize(("lines", "expected"), _CASES)
@pytest.mark.parametrize("batch", [False, True], ids=["single", "batch"])
def test_current_projection(
    conn: psycopg.Connection[Any], lines: tuple[_Line, ...], expected: str, batch: bool
) -> None:
    iid = 932_270
    _seed(conn, iid, lines)
    if batch:
        oo.refresh_insiders_current_batch(conn, instrument_ids=[iid])
    else:
        oo.refresh_insiders_current(conn, instrument_id=iid)
    assert _current_shares(conn, iid) == Decimal(expected)


@pytest.mark.parametrize(("lines", "expected"), _CASES)
def test_history_chart_agrees_with_projection(
    conn: psycopg.Connection[Any], lines: tuple[_Line, ...], expected: str
) -> None:
    """#3232's lesson: the chart and the pie must not name different values for one filing."""
    iid = 932_271
    _seed(conn, iid, lines)
    points = _insiders_history(conn, instrument_id=iid, holder_cik=_CIK)
    assert [p.shares for p in points] == [Decimal(expected)]


def _indirect_row(conn: psycopg.Connection[Any], iid: int) -> None:
    """An XML ``indirect`` row for the same holder from an older, different filing."""
    oo.record_insider_observation(
        conn,
        instrument_id=iid,
        holder_cik=_CIK,
        holder_name=f"Holder {_CIK}",
        ownership_nature="indirect",
        source="form4",
        source_document_id="0001104659-25-000001",
        source_accession="0001104659-25-000001",
        source_field=None,
        source_url=None,
        filed_at=datetime(2025, 1, 2, tzinfo=UTC),
        period_start=None,
        period_end=date(2025, 1, 1),
        ingest_run_id=uuid4(),
        shares=Decimal("50"),
    )
    conn.commit()


def _shares_for(conn: psycopg.Connection[Any], iid: int, nature: str) -> Decimal:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT shares FROM ownership_insiders_current WHERE instrument_id = %s AND ownership_nature = %s",
            (iid, nature),
        )
        row = cur.fetchone()
    assert row is not None
    return Decimal(row[0])


def test_direct_winner_refuses_when_holder_has_an_indirect_row(conn: psycopg.Connection[Any]) -> None:
    """The rollup SUMs ``direct`` + ``indirect`` (#905); a ``direct``-labelled total that already
    contains the owner's I lines would count them twice against an XML ``indirect`` row."""
    iid = 932_272
    _seed(conn, iid, _MNSO)
    _indirect_row(conn, iid)
    oo.refresh_insiders_current(conn, instrument_id=iid)
    assert _shares_for(conn, iid, "direct") == Decimal("100")


def test_beneficial_winner_sums_despite_an_indirect_row(conn: psycopg.Connection[Any]) -> None:
    """A ``beneficial`` total is MAXed against the additive sum, never added, so no refusal."""
    iid = 932_273
    _seed(conn, iid, _MNSO, nature="beneficial")
    _indirect_row(conn, iid)
    oo.refresh_insiders_current(conn, instrument_id=iid)
    assert _shares_for(conn, iid, "beneficial") == Decimal("1000")
