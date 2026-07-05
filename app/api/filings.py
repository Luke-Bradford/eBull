"""Filings feed API endpoint.

Reads from:
  - filing_events  (per-instrument filings with summary, risk score, document link)
  - instruments     (symbol, company_name for display context)

No writes. No schema changes.

Filing identity is provider-scoped (settled decision). The API exposes
``provider``, ``filing_type``, and ``accession_number`` (the
provider's primary identifier — see #565). It does NOT expose
``raw_payload_json``.
"""

from __future__ import annotations

from datetime import date, datetime

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api._helpers import parse_optional_float
from app.db import get_conn

router = APIRouter(prefix="/filings", tags=["filings"])

MAX_PAGE_LIMIT = 200


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class NtNoticeSummary(BaseModel):
    """Parsed Form 12b-25 fields for an NT 10-K / NT 10-Q filing (#1015).

    Attached to ``FilingItem.nt_notice`` for NT rows; ``None`` for every
    other form. The red-flag badge already renders via ``red_flag_score``;
    this exposes the body content (why late + whether a significant change in
    results of operations vs the prior-year period is anticipated).
    """

    late_form: str  # '10-K' | '10-Q'
    period_of_report: date | None
    grace_period_days: int  # 15 for 10-K, 5 for 10-Q (Rule 12b-25(b))
    reason_excerpt: str | None  # first ~280 chars of the Part III narrative
    results_change_anticipated: bool | None


class Pre14aSignalSummary(BaseModel):
    """Parsed PRE 14A / PRER14A meeting-agenda proposal signal (#1892).

    Attached to ``FilingItem.pre14a_signal`` for PRE 14A / PRER14A rows;
    ``None`` for every other form.
    """

    proposal_count: int
    reverse_stock_split_proposal: bool
    authorized_share_increase_proposal: bool
    say_on_pay_advisory_vote: bool
    agenda_items: list[str]


class OfferingSummary(BaseModel):
    """Parsed 424B cover offering (Reg S-K Item 501(b)(3)) — #1816.

    Attached to ``FilingItem.offering`` for parsed 424B rows (B1/B3/B4/B5/B7 +
    volume-gated B2, #1975);
    ``None`` for every other form. Every money field is nullable: NULL means
    the cover presentation was not resolvable (percent-of-principal notes,
    resale shelves, non-tabular covers) — never a guessed value.
    """

    subtype: str
    is_issuer_offering: bool | None
    price_per_unit: float | None
    unit_label: str | None
    aggregate_offering_amount: float | None
    underwriting_discount: float | None
    net_proceeds_to_issuer: float | None
    proceeds_to_selling_holders: float | None
    currency: str
    security_type: str | None


class FilingItem(BaseModel):
    """Single filing event for an instrument.

    ``accession_number`` is the provider's primary filing identifier
    (#565). FilingsPane drilldown links route to
    ``/instrument/{symbol}/filings/10-k`` — without an accession the
    drilldown always lands on the latest filing, so clicking a
    historical 10-K or a 10-K/A row landed on the wrong document.
    Populated from ``filing_events.provider_filing_id``; nullable
    only as a defensive guard for rows missing the column (none in
    the current schema, but the column is nullable).
    """

    filing_event_id: int
    instrument_id: int
    filing_date: date
    filing_type: str | None
    provider: str
    accession_number: str | None
    source_url: str | None
    primary_document_url: str | None
    extracted_summary: str | None
    red_flag_score: float | None
    created_at: datetime
    # Form 12b-25 detail for NT 10-K / NT 10-Q rows (#1015); None otherwise.
    nt_notice: NtNoticeSummary | None = None
    # Meeting-agenda proposal signal for PRE 14A / PRER14A rows (#1892);
    # None otherwise.
    pre14a_signal: Pre14aSignalSummary | None = None
    # Parsed 424B cover offering for parsed 424B rows (#1816/#1975); None otherwise.
    offering: OfferingSummary | None = None


class FilingsListResponse(BaseModel):
    instrument_id: int
    symbol: str | None
    items: list[FilingItem]
    total: int
    offset: int
    limit: int


class FilingQuarterCount(BaseModel):
    quarter: str  # "YYYY-Qn"
    filing_type: str
    count: int


class FilingQuarterlyCounts(BaseModel):
    instrument_id: int
    symbol: str | None
    counts: list[FilingQuarterCount]


class RedFlagTrendPoint(BaseModel):
    quarter: str  # "YYYY-Qn"
    avg_score: float  # mean red_flag_score over scored filings that quarter
    n: int  # number of scored (non-NULL) filings in the quarter


class RedFlagTrend(BaseModel):
    instrument_id: int
    symbol: str | None
    points: list[RedFlagTrendPoint]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_NT_REASON_EXCERPT_CHARS = 280


def _parse_nt_notice(row: dict[str, object]) -> NtNoticeSummary | None:
    """Build the NT detail sub-object from a LEFT-JOINed nt_filing_notices row.

    Returns ``None`` when the join produced no NT row (non-NT filings).
    Keyed on ``nt_late_form`` being present.
    """
    late_form = row.get("nt_late_form")
    if late_form is None:
        return None
    reason = row.get("nt_reason_text")
    excerpt: str | None = None
    if isinstance(reason, str) and reason:
        excerpt = reason[:_NT_REASON_EXCERPT_CHARS]
    return NtNoticeSummary(
        late_form=late_form,  # type: ignore[arg-type]
        period_of_report=row.get("nt_period_of_report"),  # type: ignore[arg-type]
        grace_period_days=row["nt_grace_period_days"],  # type: ignore[arg-type]
        reason_excerpt=excerpt,
        results_change_anticipated=row.get("nt_results_change_anticipated"),  # type: ignore[arg-type]
    )


def _parse_pre14a_signal(row: dict[str, object]) -> Pre14aSignalSummary | None:
    """Build the PRE 14A detail sub-object from a LEFT-JOINed
    ``pre14a_proposal_signals`` row. Returns ``None`` when the join produced
    no row (non-PRE-14A filings, or a tombstoned accession).
    """
    proposal_count = row.get("pre14a_proposal_count")
    if proposal_count is None:
        return None
    return Pre14aSignalSummary(
        proposal_count=proposal_count,  # type: ignore[arg-type]
        reverse_stock_split_proposal=row["pre14a_reverse_stock_split_proposal"],  # type: ignore[arg-type]
        authorized_share_increase_proposal=row["pre14a_authorized_share_increase_proposal"],  # type: ignore[arg-type]
        say_on_pay_advisory_vote=row["pre14a_say_on_pay_advisory_vote"],  # type: ignore[arg-type]
        agenda_items=row.get("pre14a_agenda_items") or [],  # type: ignore[arg-type]
    )


def _parse_offering(row: dict[str, object]) -> OfferingSummary | None:
    """Build the 424B offering sub-object from a LEFT-JOINed
    ``prospectus_offerings`` row. Returns ``None`` when the join produced no
    row (non-424B filings, or a tombstoned accession). Keyed on ``po_subtype``
    being present (NOT NULL in the table).
    """
    subtype = row.get("po_subtype")
    if subtype is None:
        return None
    return OfferingSummary(
        subtype=subtype,  # type: ignore[arg-type]
        is_issuer_offering=row.get("po_is_issuer_offering"),  # type: ignore[arg-type]
        price_per_unit=parse_optional_float(row, "po_price_per_unit"),
        unit_label=row.get("po_unit_label"),  # type: ignore[arg-type]
        aggregate_offering_amount=parse_optional_float(row, "po_aggregate_offering_amount"),
        underwriting_discount=parse_optional_float(row, "po_underwriting_discount"),
        net_proceeds_to_issuer=parse_optional_float(row, "po_net_proceeds_to_issuer"),
        proceeds_to_selling_holders=parse_optional_float(row, "po_proceeds_to_selling_holders"),
        currency=row["po_currency"],  # type: ignore[arg-type]
        security_type=row.get("po_security_type"),  # type: ignore[arg-type]
    )


def _parse_filing_item(row: dict[str, object]) -> FilingItem:
    return FilingItem(
        filing_event_id=row["filing_event_id"],  # type: ignore[arg-type]
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        filing_date=row["filing_date"],  # type: ignore[arg-type]
        filing_type=row["filing_type"],  # type: ignore[arg-type]
        provider=row["provider"],  # type: ignore[arg-type]
        accession_number=row.get("provider_filing_id"),  # type: ignore[arg-type]
        source_url=row["source_url"],  # type: ignore[arg-type]
        primary_document_url=row["primary_document_url"],  # type: ignore[arg-type]
        extracted_summary=row["extracted_summary"],  # type: ignore[arg-type]
        red_flag_score=parse_optional_float(row, "red_flag_score"),
        created_at=row["created_at"],  # type: ignore[arg-type]
        nt_notice=_parse_nt_notice(row),
        pre14a_signal=_parse_pre14a_signal(row),
        offering=_parse_offering(row),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{instrument_id}", response_model=FilingsListResponse)
def list_filings(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    filing_type: str | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> FilingsListResponse:
    """Filing events for an instrument, ordered by filing_date DESC.

    Optional ``filing_type`` filter for narrowing to e.g. ``10-K``, ``10-Q``.

    Returns 404 if the instrument does not exist.
    """
    # Resolve instrument symbol for the response envelope.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT symbol FROM instruments WHERE instrument_id = %(id)s",
            {"id": instrument_id},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    symbol: str = inst_row["symbol"]  # type: ignore[assignment]

    # Build dynamic WHERE. Columns are qualified with the ``fe`` alias because
    # the items query LEFT JOINs ``nt_filing_notices`` (which also has an
    # instrument_id column) — unqualified names would be ambiguous there.
    where_clauses: list[str] = ["fe.instrument_id = %(instrument_id)s"]
    filter_params: dict[str, object] = {"instrument_id": instrument_id}

    if filing_type is not None:
        types = [t.strip() for t in filing_type.split(",") if t.strip()]
        if types:
            where_clauses.append("fe.filing_type = ANY(%(filing_types)s)")
            filter_params["filing_types"] = types

    where_sql = " AND ".join(where_clauses)

    # COUNT query — separate cursor, separate params dict.
    # where_sql is built from hardcoded clause strings only — not user input.
    count_sql = f"SELECT COUNT(*) AS cnt FROM filing_events fe WHERE {where_sql}"  # noqa: S608
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]
        # COUNT always returns exactly one row; the column value is 0 when empty.
        count_row = cur.fetchone()
        total: int = count_row["cnt"]  # type: ignore[index]

    # Items query — separate cursor, separate params dict.
    items_params: dict[str, object] = {
        **filter_params,
        "limit": limit,
        "offset": offset,
    }
    items_sql = f"""SELECT fe.filing_event_id, fe.instrument_id, fe.filing_date,
                       fe.filing_type, fe.provider, fe.provider_filing_id,
                       fe.source_url, fe.primary_document_url,
                       fe.extracted_summary, fe.red_flag_score,
                       fe.created_at,
                       nn.late_form               AS nt_late_form,
                       nn.period_of_report        AS nt_period_of_report,
                       nn.grace_period_days       AS nt_grace_period_days,
                       nn.reason_text             AS nt_reason_text,
                       nn.results_change_anticipated AS nt_results_change_anticipated,
                       ps.proposal_count                     AS pre14a_proposal_count,
                       ps.reverse_stock_split_proposal       AS pre14a_reverse_stock_split_proposal,
                       ps.authorized_share_increase_proposal AS pre14a_authorized_share_increase_proposal,
                       ps.say_on_pay_advisory_vote            AS pre14a_say_on_pay_advisory_vote,
                       ps.agenda_items                        AS pre14a_agenda_items,
                       po.subtype                    AS po_subtype,
                       po.is_issuer_offering         AS po_is_issuer_offering,
                       po.price_per_unit             AS po_price_per_unit,
                       po.unit_label                 AS po_unit_label,
                       po.aggregate_offering_amount  AS po_aggregate_offering_amount,
                       po.underwriting_discount      AS po_underwriting_discount,
                       po.net_proceeds_to_issuer     AS po_net_proceeds_to_issuer,
                       po.proceeds_to_selling_holders AS po_proceeds_to_selling_holders,
                       po.currency                   AS po_currency,
                       po.security_type              AS po_security_type
                FROM filing_events fe
                LEFT JOIN nt_filing_notices nn
                       ON nn.accession_number = fe.provider_filing_id
                      AND fe.provider = 'sec'
                LEFT JOIN pre14a_proposal_signals ps
                       ON ps.accession_number = fe.provider_filing_id
                      AND fe.provider = 'sec'
                LEFT JOIN prospectus_offerings po
                       ON po.accession_number = fe.provider_filing_id
                      AND fe.provider = 'sec'
                WHERE {where_sql}
                ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
                LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608  — where_sql is hardcoded clauses only
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(items_sql, items_params)  # type: ignore[arg-type]
        rows = cur.fetchall()

    items = [_parse_filing_item(r) for r in rows]
    return FilingsListResponse(
        instrument_id=instrument_id,
        symbol=symbol,
        items=items,
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{instrument_id}/quarterly-counts", response_model=FilingQuarterlyCounts)
def filing_quarterly_counts(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    years: int = Query(default=5, ge=1, le=20),
) -> FilingQuarterlyCounts:
    """Per-(quarter, filing_type) filing counts over the last ``years`` years.

    Feeds the filings-analytics drill (#592) — the density timeline + the
    form-type heatmap. Aggregated server-side so the client never pulls the raw
    filing rows (an active filer has hundreds of Form 4s alone, past the
    /filings page cap); the small {quarter, filing_type, count} payload is
    categorised + bucketed on the FE. The red-flag-score trend is served
    separately by ``/{instrument_id}/red-flag-trend`` (#1748).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT symbol FROM instruments WHERE instrument_id = %(id)s",
            {"id": instrument_id},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT to_char(date_trunc('quarter', filing_date), 'YYYY-"Q"Q') AS quarter,
                   filing_type,
                   COUNT(*) AS count
            FROM filing_events
            WHERE instrument_id = %(id)s
              AND filing_type IS NOT NULL
              AND filing_date >= (CURRENT_DATE - make_interval(years => %(years)s))
            GROUP BY 1, filing_type
            ORDER BY 1, filing_type
            """,
            {"id": instrument_id, "years": years},
        )
        rows = cur.fetchall()

    return FilingQuarterlyCounts(
        instrument_id=instrument_id,
        symbol=inst_row["symbol"],  # type: ignore[index]
        counts=[
            FilingQuarterCount(
                quarter=str(r["quarter"]),
                filing_type=str(r["filing_type"]),
                count=int(r["count"]),
            )
            for r in rows
        ],
    )


@router.get("/{instrument_id}/red-flag-trend", response_model=RedFlagTrend)
def filing_red_flag_trend(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    years: int = Query(default=5, ge=1, le=20),
) -> RedFlagTrend:
    """Per-quarter mean ``red_flag_score`` over the last ``years`` years.

    The #592-deferred third filings-analytics chart (#1748). Only scored
    filings (``red_flag_score IS NOT NULL`` — critical 8-Ks and Form NT
    late filings) contribute, so a quarter with no risk-bearing filing is
    simply absent (the FE renders an empty state / gaps).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT symbol FROM instruments WHERE instrument_id = %(id)s",
            {"id": instrument_id},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail="Instrument not found")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT to_char(date_trunc('quarter', filing_date), 'YYYY-"Q"Q') AS quarter,
                   AVG(red_flag_score) AS avg_score,
                   COUNT(*) AS n
            FROM filing_events
            WHERE instrument_id = %(id)s
              AND red_flag_score IS NOT NULL
              AND filing_date >= (CURRENT_DATE - make_interval(years => %(years)s))
            GROUP BY 1
            ORDER BY 1
            """,
            {"id": instrument_id, "years": years},
        )
        rows = cur.fetchall()

    return RedFlagTrend(
        instrument_id=instrument_id,
        symbol=inst_row["symbol"],  # type: ignore[index]
        points=[
            RedFlagTrendPoint(
                quarter=str(r["quarter"]),
                avg_score=float(r["avg_score"]),
                n=int(r["n"]),
            )
            for r in rows
        ],
    )
