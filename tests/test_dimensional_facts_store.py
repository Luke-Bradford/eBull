"""DB-tier test for the dimensional-facts store (#554).

ONE integration test per genuinely-new SQL mechanism (test-quality
skill): the winner-per-(axis, METRIC) reader with annual-duration
filtering + subtotal exclusion, plus the delete-then-insert writer
contract it depends on.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.dimensional_facts import DimensionalFact
from app.services.dimensional_facts_store import read_segments, replace_accession_rows
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_IID = 554001


def _seed_instrument(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, 'SEGT', 'Segments co', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (_IID,),
    )


def _fact(
    metric: str,
    member: str,
    val: str,
    *,
    axis: str = "business_segment",
    start: date | None = date(2025, 1, 1),
    end: date = date(2025, 12, 31),
    is_subtotal: bool = False,
) -> DimensionalFact:
    return DimensionalFact(
        axis=axis,  # type: ignore[arg-type]
        member_qname=member,
        member_label=member.split(":", 1)[-1],
        metric=metric,  # type: ignore[arg-type]
        unit="USD",
        is_subtotal=is_subtotal,
        period_start=start,
        period_end=end,
        val=Decimal(val),
        decimals="-6",
    )


@pytest.mark.integration
def test_reader_winner_per_metric_annual_filter_and_writer_replace(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn)

    original = [
        # FY2025 annual rows.
        _fact("revenue", "t:NorthMember", "600"),
        _fact("revenue", "t:SouthMember", "400"),
        _fact("revenue", "t:TotalMember", "1000", is_subtotal=True),
        _fact("operating_income", "t:NorthMember", "60"),
        _fact("operating_income", "t:SouthMember", "40"),
        _fact("assets", "t:NorthMember", "5000", start=None),
        # Prior-FY comparative — must lose to FY2025 on period selection.
        _fact("revenue", "t:NorthMember", "500", start=date(2024, 1, 1), end=date(2024, 12, 31)),
        # Quarterly/YTD context a 10-K also carries — annual filter drops it.
        _fact("revenue", "t:NorthMember", "150", start=date(2025, 10, 1), end=date(2025, 12, 31)),
    ]
    n = replace_accession_rows(
        conn,
        instrument_id=_IID,
        source_accession="ACC-ORIG",
        form_type="10-K",
        filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        parser_version="10k-v2",
        facts=original,
    )
    assert n == len(original)

    # Amendment: restates REVENUE only (omits op income + assets) — the
    # per-METRIC winner must take revenue from here without regressing
    # the other metrics to empty (spec §D4, Codex ckpt-1 HIGH).
    amendment = [
        _fact("revenue", "t:NorthMember", "610"),
        _fact("revenue", "t:SouthMember", "390"),
    ]
    replace_accession_rows(
        conn,
        instrument_id=_IID,
        source_accession="ACC-AMEND",
        form_type="10-K/A",
        filed_at=datetime(2026, 3, 1, tzinfo=UTC),
        parser_version="10k-v2",
        facts=amendment,
    )
    conn.commit()

    result = read_segments(conn, instrument_id=_IID, axis="business_segment")
    assert result.sources == {
        "revenue": "ACC-AMEND",
        "operating_income": "ACC-ORIG",
        "assets": "ACC-ORIG",
    }
    assert result.period_end == date(2025, 12, 31)
    by_member = {r["member_qname"]: r for r in result.rows}
    # Subtotal excluded; restated revenue paired with original op income.
    assert set(by_member) == {"t:NorthMember", "t:SouthMember"}
    assert by_member["t:NorthMember"]["revenue"] == Decimal("610")
    assert by_member["t:NorthMember"]["operating_income"] == Decimal("60")
    assert by_member["t:NorthMember"]["assets"] == Decimal("5000")
    assert by_member["t:SouthMember"]["revenue"] == Decimal("390")

    # Writer replace contract: re-ingesting ACC-AMEND with fewer rows
    # clears its previous rows (rewash correction path, spec §D4).
    replace_accession_rows(
        conn,
        instrument_id=_IID,
        source_accession="ACC-AMEND",
        form_type="10-K/A",
        filed_at=datetime(2026, 3, 1, tzinfo=UTC),
        parser_version="10k-v3",
        facts=[_fact("revenue", "t:NorthMember", "615")],
    )
    conn.commit()
    result2 = read_segments(conn, instrument_id=_IID, axis="business_segment")
    by_member2 = {r["member_qname"]: r for r in result2.rows}
    assert by_member2["t:NorthMember"]["revenue"] == Decimal("615")
    # SouthMember revenue no longer in the amendment; reader still
    # surfaces the member because op income (winner ACC-ORIG) has it,
    # with revenue empty — per-metric winners are independent.
    assert by_member2["t:SouthMember"]["revenue"] is None
    assert by_member2["t:SouthMember"]["operating_income"] == Decimal("40")

    # A still-newer accession with ONLY ineligible rows (quarterly
    # duration + a subtotal) must NOT win the revenue metric — winner
    # selection filters eligibility first (Codex pre-push finding).
    replace_accession_rows(
        conn,
        instrument_id=_IID,
        source_accession="ACC-Q-ONLY",
        form_type="10-K/A",
        filed_at=datetime(2026, 4, 1, tzinfo=UTC),
        parser_version="10k-v3",
        facts=[
            _fact("revenue", "t:NorthMember", "160", start=date(2025, 10, 1), end=date(2025, 12, 31)),
            _fact("revenue", "t:TotalMember", "1010", is_subtotal=True),
        ],
    )
    conn.commit()
    result3 = read_segments(conn, instrument_id=_IID, axis="business_segment")
    assert result3.sources["revenue"] == "ACC-AMEND"
    by_member3 = {r["member_qname"]: r for r in result3.rows}
    assert by_member3["t:NorthMember"]["revenue"] == Decimal("615")

    # Review-bot WARNING (PR #1588): a subtotal row in the WINNING
    # accession with a LATER period_end must not anchor the target
    # period — every CTE stage carries the same eligibility filters.
    replace_accession_rows(
        conn,
        instrument_id=_IID,
        source_accession="ACC-AMEND",
        form_type="10-K/A",
        filed_at=datetime(2026, 3, 1, tzinfo=UTC),
        parser_version="10k-v3",
        facts=[
            _fact("revenue", "t:NorthMember", "615"),
            _fact(
                "revenue",
                "t:TotalMember",
                "1300",
                start=date(2026, 1, 1),
                end=date(2026, 12, 31),
                is_subtotal=True,
            ),
        ],
    )
    conn.commit()
    result4 = read_segments(conn, instrument_id=_IID, axis="business_segment")
    assert result4.period_end == date(2025, 12, 31)
    by_member4 = {r["member_qname"]: r for r in result4.rows}
    assert by_member4["t:NorthMember"]["revenue"] == Decimal("615")
