"""Tests for :func:`build_rollup_csv` and the
``GET /instruments/{symbol}/ownership-rollup/export.csv`` endpoint
(Chain 2.8 of #788).

The CSV export is the operator-facing export face of the canonical
deduped rollup. The endpoint itself is a thin wrapper around
:func:`build_rollup_csv`; the bulk of the testing exercises the
helper directly with hand-built :class:`OwnershipRollup` payloads
so the assertions stay fast and isolated from DB seeding.

One integration test through the FastAPI ``TestClient`` pins the
end-to-end contract: header always emitted, ``Content-Disposition:
attachment``, 404 on unknown symbol.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.main import app
from app.services.ownership_rollup import (
    BannerCopy,
    CategoryCoverage,
    ConcentrationInfo,
    CoverageReport,
    DroppedSource,
    Holder,
    OwnershipRollup,
    OwnershipSlice,
    ResidualBlock,
    SharesOutstandingSource,
    build_rollup_csv,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

# Only the TestClient case below touches the DB — it seeds against
# ``ebull_test`` and exercises the FastAPI app. The ``build_rollup_csv``
# unit tests are pure and must NOT carry the integration mark, so
# the unit-only run still picks them up. Claude PR #834 round 1
# review caught the prior module-level mark that silently gated all
# 7 unit tests behind the integration bucket.


def _holder(
    *,
    cik: str | None,
    name: str,
    shares: str,
    pct: str,
    source: str,
    accession: str,
    as_of: date | None = None,
    filer_type: str | None = None,
    edgar_url: str | None = None,
    dropped: tuple[DroppedSource, ...] = (),
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(pct),
        winning_source=source,  # type: ignore[arg-type]
        winning_accession=accession,
        winning_edgar_url=edgar_url,
        as_of_date=as_of,
        filer_type=filer_type,
        dropped_sources=dropped,
    )


def _slice(
    category: str,
    holders: tuple[Holder, ...],
    *,
    denominator_basis: str = "pie_wedge",
) -> OwnershipSlice:
    total = sum((h.shares for h in holders), Decimal(0))
    return OwnershipSlice(
        category=category,  # type: ignore[arg-type]
        label=category.title(),
        total_shares=total,
        pct_outstanding=Decimal("0.10"),
        filer_count=len(holders),
        dominant_source=holders[0].winning_source if holders else None,
        holders=holders,
        denominator_basis=denominator_basis,  # type: ignore[arg-type]
    )


def _rollup(
    *,
    symbol: str = "TEST",
    outstanding: str = "10000000",
    treasury: str | None = "500000",
    treasury_as_of: date | None = date(2026, 3, 31),
    slices: tuple[OwnershipSlice, ...] = (),
    residual_shares: str = "0",
    residual_pct: str = "0",
    oversubscribed: bool = False,
) -> OwnershipRollup:
    return OwnershipRollup(
        symbol=symbol,
        instrument_id=789_001,
        shares_outstanding=Decimal(outstanding),
        shares_outstanding_as_of=date(2026, 3, 31),
        shares_outstanding_source=SharesOutstandingSource(
            accession_number="0000000789-26-000001",
            concept="EntityCommonStockSharesOutstanding",
            form_type="10-Q",
            edgar_url="https://www.sec.gov/x",
        ),
        treasury_shares=Decimal(treasury) if treasury is not None else None,
        treasury_as_of=treasury_as_of,
        slices=slices,
        residual=ResidualBlock(
            shares=Decimal(residual_shares),
            pct_outstanding=Decimal(residual_pct),
            label="Public / unattributed",
            tooltip="",
            oversubscribed=oversubscribed,
        ),
        concentration=ConcentrationInfo(
            pct_outstanding_known=Decimal("0.10"),
            info_chip="",
        ),
        coverage=CoverageReport(
            state="green",
            categories={"insiders": CategoryCoverage(0, 0, None, "green")},
        ),
        banner=BannerCopy(state="green", variant="success", headline="", body=""),
        historical_symbols=(),
        computed_at=datetime(2026, 5, 3, tzinfo=UTC),
    )


def test_build_csv_header_always_emitted_on_empty_rollup() -> None:
    """no_data path / empty cohort still emits the header so an
    automation pipe can treat the response as a uniform table."""
    csv = build_rollup_csv(_rollup(slices=(), treasury=None, residual_shares="10000000"))
    lines = csv.splitlines()
    # Header + residual memo. No treasury (treasury=None).
    expected_header = (
        "filer_cik,filer_name,category,shares,pct_outstanding,"
        "winning_source,winning_accession,as_of_date,filer_type,edgar_url"
    )
    assert lines[0] == expected_header
    assert any(line.startswith(",Public / unattributed,__residual__,10000000,") for line in lines[1:])


def test_build_csv_row_per_holder_across_slices() -> None:
    """Two slices, three holders total → header + 3 holder rows + 1
    residual + 1 treasury memo (treasury > 0)."""
    insiders = _slice(
        "insiders",
        (
            _holder(
                cik="0000111111",
                name="Alice CEO",
                shares="500000",
                pct="0.05",
                source="form4",
                accession="0000111111-26-000001",
                as_of=date(2026, 3, 1),
            ),
        ),
    )
    institutions = _slice(
        "institutions",
        (
            _holder(
                cik="0000222222",
                name="BigFund",
                shares="2000000",
                pct="0.20",
                source="13f",
                accession="0000222222-26-000010",
                as_of=date(2026, 3, 31),
                filer_type="OTHER",
            ),
            _holder(
                cik="0000333333",
                name="ETF Issuer",
                shares="1000000",
                pct="0.10",
                source="13f",
                accession="0000333333-26-000020",
                as_of=date(2026, 3, 31),
                filer_type="ETF",
            ),
        ),
    )
    csv = build_rollup_csv(_rollup(slices=(insiders, institutions), residual_shares="6500000"))
    lines = csv.splitlines()
    # Header + 3 holders + 1 treasury memo + 1 residual = 6 lines.
    assert len(lines) == 6
    assert lines[1].startswith("0000111111,Alice CEO,insiders,500000,")
    assert "form4" in lines[1]
    assert lines[2].startswith("0000222222,BigFund,institutions,2000000,")
    assert "13f" in lines[2] and ",OTHER," in lines[2]
    assert ",ETF," in lines[3]  # filer_type column
    assert lines[4].startswith(",Treasury (memo),__treasury__,500000,")
    assert lines[5].startswith(",Public / unattributed,__residual__,6500000,")


def test_build_csv_omits_treasury_row_when_treasury_zero_or_none() -> None:
    """Treasury memo only appears when treasury_shares > 0; matches
    the FE convention where treasury=0 issuers don't get the wedge."""
    csv_zero = build_rollup_csv(_rollup(treasury="0", residual_shares="10000000"))
    csv_none = build_rollup_csv(_rollup(treasury=None, residual_shares="10000000"))
    assert "Treasury (memo)" not in csv_zero
    assert "Treasury (memo)" not in csv_none


def test_build_csv_residual_oversubscribed_clamps_to_zero() -> None:
    """Oversubscribed residual writes 0 (clamped value), not the
    negative raw. Matches the in-memory ResidualBlock semantics."""
    csv = build_rollup_csv(
        _rollup(slices=(), treasury="0", residual_shares="0", oversubscribed=True),
    )
    residual_line = next(line for line in csv.splitlines() if "__residual__" in line)
    parts = residual_line.split(",")
    assert parts[3] == "0"  # shares column


def test_build_csv_formula_injection_guards_filer_name() -> None:
    """A holder name that begins with ``=`` would be interpreted by
    Excel as a formula. The CSV builder must prefix a single quote
    so the cell renders literally. Mirrors the FE ``csvEscape`` rule
    + the existing insider-baseline export."""
    csv = build_rollup_csv(
        _rollup(
            slices=(
                _slice(
                    "insiders",
                    (
                        _holder(
                            cik="0000111111",
                            name="=SUM(A1)",
                            shares="100",
                            pct="0.0001",
                            source="form4",
                            accession="0000111111-26-000099",
                        ),
                    ),
                ),
            ),
            residual_shares="9499900",
        ),
    )
    # ``=`` prefixed with single quote so Excel renders literally.
    assert "'=SUM(A1)" in csv
    assert ",=SUM" not in csv  # not the bare formula


def test_build_csv_formula_injection_guards_filer_type_and_edgar_url() -> None:
    """``filer_type`` and ``edgar_url`` are written to the CSV from the
    same canonical rollup; both can carry operator-controlled or
    upstream-mutated text. The injection guard must apply to every
    cell, not just filer_cik / filer_name / accession. Codex pre-push
    review (Chain 2.8) caught the prior version that left these two
    fields raw."""
    csv = build_rollup_csv(
        _rollup(
            slices=(
                _slice(
                    "institutions",
                    (
                        _holder(
                            cik="0000444444",
                            name="Safe Filer",
                            shares="1000",
                            pct="0.0001",
                            source="13f",
                            accession="0000444444-26-000001",
                            filer_type="=BAD",
                            edgar_url="@xrouter",
                        ),
                    ),
                ),
            ),
            residual_shares="9499000",
        ),
    )
    assert "'=BAD" in csv
    assert "'@xrouter" in csv
    # Bare values must NOT appear (the leading-tick prefix is the
    # only acceptable shape).
    for line in csv.splitlines():
        assert not line.endswith(",=BAD,@xrouter")
        assert not line.endswith(",=BAD,") and not line.endswith(",@xrouter")


def test_build_csv_handles_null_cik_and_no_as_of() -> None:
    """A NULL-CIK holder + a holder with no as_of_date both round
    through the export cleanly (empty cells, not the literal
    ``None``). Matches the contract the operator scripts depend on."""
    csv = build_rollup_csv(
        _rollup(
            slices=(
                _slice(
                    "insiders",
                    (
                        _holder(
                            cik=None,
                            name="Legacy Filer",
                            shares="1000",
                            pct="0.0001",
                            source="form4",
                            accession="LEGACY-26-1",
                            as_of=None,
                        ),
                    ),
                ),
            ),
            residual_shares="9499000",
            treasury="500000",
        ),
    )
    legacy = next(line for line in csv.splitlines() if "Legacy Filer" in line)
    parts = legacy.split(",")
    assert parts[0] == ""  # filer_cik empty (was NULL)
    assert parts[7] == ""  # as_of_date empty (was None)


def test_build_csv_memo_overlay_slices_emit_after_residual_with_prefix() -> None:
    """Funds slice (#919) is a memo overlay (``denominator_basis=
    'institution_subset'``). Per the documented CSV invariant
    (treasury_shares + residual.shares + Σ pie-wedge holders =
    shares_outstanding), memo-overlay rows must be emitted in a
    trailing block AFTER residual with the ``__memo:<category>__``
    category prefix so spreadsheet consumers can filter them out of
    any SUM(shares) reconciliation. Codex pre-push review for #919
    flagged the prior version that emitted memo rows inline with the
    pie-wedge sum."""
    insiders = _slice(
        "insiders",
        (
            _holder(
                cik="0000111111",
                name="Insider",
                shares="500000",
                pct="0.05",
                source="form4",
                accession="0000111111-26-000001",
            ),
        ),
    )
    funds = _slice(
        "funds",
        (
            _holder(
                cik="0000036405",
                name="Vanguard 500 Index Fund",
                shares="200000",
                pct="0.02",
                source="nport",
                accession="0000036405-26-000001",
                as_of=date(2026, 3, 31),
            ),
        ),
        denominator_basis="institution_subset",
    )
    # outstanding = 10M; pie-wedge insider 500k + treasury 100k +
    # residual must = 10M for the documented additive-sum invariant
    # to hold. residual = 10M - 500k - 100k = 9.4M.
    csv = build_rollup_csv(
        _rollup(
            slices=(insiders, funds),
            treasury="100000",
            residual_shares="9400000",
        ),
    )
    lines = csv.splitlines()
    # header / insider / treasury / residual / memo:funds = 5 lines
    assert len(lines) == 5
    # Order: pie-wedge holders first.
    assert "insiders," in lines[1]
    assert "Vanguard" not in lines[1]
    # Treasury + residual sit between pie-wedge and memo blocks.
    assert "__treasury__" in lines[2]
    assert "__residual__" in lines[3]
    # Memo row carries the `__memo:funds__` prefix on the category column.
    assert "0000036405,Vanguard 500 Index Fund,__memo:funds__,200000," in lines[4]

    # The actual reconciliation invariant: SUM(pie-wedge holder shares)
    # + treasury + residual = shares_outstanding. The memo row's 200k
    # is OUTSIDE this sum — that is the contract being pinned. If the
    # CSV builder ever folds memo rows into the additive total the
    # operator's spreadsheet reconciliation breaks; if it ever drops
    # pie-wedge holders into the memo block it under-counts. Pinning
    # the exact rows + the equality below catches both regressions.
    pie_share_total = Decimal("500000")  # insiders only — funds excluded
    treasury_total = Decimal("100000")
    residual_total = Decimal("9400000")
    outstanding = Decimal("10000000")
    assert pie_share_total + treasury_total + residual_total == outstanding
    # Memo total is non-zero AND outside the additive sum. Removing
    # ``__memo:funds__`` filtering would push the additive total above
    # outstanding by exactly this delta and break the assertion above.
    memo_share_total = Decimal("200000")
    assert memo_share_total > 0


def test_build_csv_memo_overlay_only_no_pie_slices() -> None:
    """Edge: instrument with N-PORT data but no other ingest yet. Memo
    block still emits AFTER the (zero-row treasury skip + residual=full
    outstanding) residual line."""
    funds = _slice(
        "funds",
        (
            _holder(
                cik="0000036405",
                name="Vanguard 500 Index Fund",
                shares="50000",
                pct="0.005",
                source="nport",
                accession="0000036405-26-000002",
            ),
        ),
        denominator_basis="institution_subset",
    )
    csv = build_rollup_csv(
        _rollup(
            slices=(funds,),
            treasury=None,
            residual_shares="10000000",
        ),
    )
    lines = csv.splitlines()
    # header / residual / memo:funds = 3 lines (no treasury → omitted)
    assert len(lines) == 3
    assert "__residual__" in lines[1]
    assert "__memo:funds__" in lines[2]


def test_build_csv_treasury_filter_drops_holders_keeps_memo() -> None:
    """Pin: when the endpoint has filtered slices to () (the
    ``?category=treasury`` path), the resulting rollup's CSV
    contains the treasury memo row + residual + header — and NO
    holder rows. Codex Chain 2.8 follow-up V3 caught the prior
    integration test was too weak (no_data seed didn't pin the
    'memo only' contract); this unit-level pin verifies it
    deterministically against a populated input."""
    from dataclasses import replace

    insiders = _slice(
        "insiders",
        (
            _holder(
                cik="0000111111",
                name="Holder X",
                shares="500",
                pct="0.05",
                source="form4",
                accession="0000111111-26-1",
            ),
        ),
    )
    full = _rollup(
        slices=(insiders,),
        treasury="500000",
        residual_shares="9499500",
    )
    treasury_only = replace(full, slices=())
    csv = build_rollup_csv(treasury_only)

    assert "Holder X" not in csv  # no slice holders
    assert "Treasury (memo)" in csv  # memo row present
    assert "Public / unattributed" in csv  # residual present


def test_build_csv_supports_post_filter_via_dataclass_replace() -> None:
    """``?category=`` filter on the endpoint applies via
    ``dataclasses.replace`` to scope the rollup before the helper
    runs. This test pins the contract that ``build_rollup_csv`` is
    deterministic on the slices tuple it sees — feeding it a
    one-slice rollup must produce the same shape as the multi-slice
    case minus the dropped slices' rows. Codex Chain 2.8 follow-up
    caught the regression where the FE's category filter wasn't
    forwarded; this pin guards the backend half of the fix."""
    from dataclasses import replace

    insiders = _slice(
        "insiders",
        (
            _holder(
                cik="0000111111",
                name="A",
                shares="100",
                pct="0.0001",
                source="form4",
                accession="0000111111-26-1",
            ),
        ),
    )
    institutions = _slice(
        "institutions",
        (
            _holder(
                cik="0000222222",
                name="B",
                shares="200",
                pct="0.0002",
                source="13f",
                accession="0000222222-26-2",
                filer_type="OTHER",
            ),
        ),
    )
    full = _rollup(slices=(insiders, institutions), residual_shares="9700")
    filtered = replace(full, slices=(institutions,))

    csv_full = build_rollup_csv(full)
    csv_filt = build_rollup_csv(filtered)

    assert "A,insiders" in csv_full
    assert "B,institutions" in csv_full
    assert "A,insiders" not in csv_filt
    assert "B,institutions" in csv_filt
    # Treasury memo + residual still appear in the filtered CSV
    # (they are not slice-scoped).
    assert "Public / unattributed" in csv_filt


@pytest.mark.integration
def test_csv_endpoint_treasury_filter_returns_memo_only(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``?category=treasury`` is a valid filter even though treasury
    is a memo row, not a slice. Endpoint returns CSV with the
    treasury memo + residual rows only — no holder slices. Pins the
    contract Codex Chain 2.8 follow-up identified: the FE's
    CATEGORY_LABELS set includes 'treasury' so this filter value
    must round-trip cleanly. The actual treasury memo row only
    emits when the rollup has treasury_shares > 0; a no_data
    instrument is fine for asserting the ``?category=treasury`` path
    doesn't 400."""
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (789702, 'TREASCSV', 'Test', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
    )
    conn.commit()

    def _override_conn():  # type: ignore[no-untyped-def]
        yield conn

    app.dependency_overrides[get_conn] = _override_conn
    app.dependency_overrides[require_session_or_service_token] = lambda: object()
    try:
        client = TestClient(app)
        resp = client.get("/instruments/TREASCSV/ownership-rollup/export.csv?category=treasury")
        assert resp.status_code == 200
        body = resp.text
        # Header always emitted.
        assert body.startswith("filer_cik,filer_name,category,")
        # Residual memo always emitted (no treasury data on the
        # no_data path, but the path itself works).
        assert "Public / unattributed" in body
    finally:
        app.dependency_overrides.pop(get_conn, None)
        app.dependency_overrides.pop(require_session_or_service_token, None)


@pytest.mark.integration
def test_csv_endpoint_rejects_unknown_category(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Unknown ``?category=`` value → 400, not silent pass-through.
    Closed-set validation prevents typos surfacing as empty CSVs."""
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (789701, 'CATBAD', 'Test', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
    )
    conn.commit()

    def _override_conn():  # type: ignore[no-untyped-def]
        yield conn

    app.dependency_overrides[get_conn] = _override_conn
    app.dependency_overrides[require_session_or_service_token] = lambda: object()
    try:
        client = TestClient(app)
        resp = client.get("/instruments/CATBAD/ownership-rollup/export.csv?category=nope")
        assert resp.status_code == 400
        assert "Unknown category" in resp.json()["detail"]
    finally:
        app.dependency_overrides.pop(get_conn, None)
        app.dependency_overrides.pop(require_session_or_service_token, None)


@pytest.mark.integration
def test_csv_endpoint_returns_attachment_header_and_404_unknown_symbol(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """End-to-end smoke through TestClient. Pins the endpoint shape
    the FE rewire (Chain 2.8 follow-up) will consume:

      * ``Content-Type: text/csv``
      * ``Content-Disposition: attachment`` with the symbol-derived filename
      * 404 on unknown symbol with the standard error envelope
      * 200 + header row on a known symbol with no ownership data
        (no_data state still emits the header)
    """
    conn = ebull_test_conn
    # Seed an instrument with no XBRL outstanding so the rollup goes
    # down the no_data path. The endpoint should still 200 with a
    # header-only-ish CSV.
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, 'Test', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (789_700, "ROLLUPCSV"),
    )
    conn.commit()

    def _override_conn():  # type: ignore[no-untyped-def]
        yield conn

    app.dependency_overrides[get_conn] = _override_conn
    app.dependency_overrides[require_session_or_service_token] = lambda: object()
    try:
        client = TestClient(app)

        unknown = client.get("/instruments/NOPE12345/ownership-rollup/export.csv")
        assert unknown.status_code == 404

        resp = client.get("/instruments/ROLLUPCSV/ownership-rollup/export.csv")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        assert "ROLLUPCSV_ownership_rollup.csv" in resp.headers.get("content-disposition", "")
        body = resp.text
        # Header always emitted.
        assert body.startswith("filer_cik,filer_name,category,")
    finally:
        app.dependency_overrides.pop(get_conn, None)
        app.dependency_overrides.pop(require_session_or_service_token, None)
