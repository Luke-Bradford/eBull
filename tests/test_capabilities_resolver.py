"""Tests for the per-instrument capability resolver (#515 PR 3).

Pins the contract:

1. Exchange-row defaults flow through to the resolved cell.
2. ``external_identifiers`` SEC CIK augments the cell with the
   SEC family providers (filings/fundamentals/dividends/insider/
   ownership/corporate_events/business_summary/analyst).
3. Multi-source augmentation (LSE row + SEC CIK) yields BOTH
   provider sets in ``providers`` without duplicates.
4. ``data_present`` reports True iff the wired SQL EXISTS query
   returns true for the instrument.
5. Unknown / drifted provider tags are silently skipped (operator-
   override safety net).
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.capabilities import (
    V1_CAPABILITIES,
    resolve_capabilities,
)

pytestmark = pytest.mark.integration


def _seed_exchange_with_capabilities(
    conn: psycopg.Connection[tuple],
    *,
    exchange_id: str,
    asset_class: str,
    capabilities: dict[str, list[str]],
) -> None:
    """Insert (or reset) one exchange row with explicit capabilities."""
    import json as _json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, asset_class, capabilities)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (exchange_id) DO UPDATE SET
                asset_class  = EXCLUDED.asset_class,
                capabilities = EXCLUDED.capabilities
            """,
            (exchange_id, asset_class, _json.dumps(capabilities)),
        )


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    exchange: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable)
            VALUES (%s, %s, %s, %s, TRUE)
            """,
            (instrument_id, symbol, f"Test {symbol}", exchange),
        )


def _seed_sec_cik(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    cik: str,
    is_primary: bool = True,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cik', %s, %s)
            ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
            """,
            (instrument_id, cik, is_primary),
        )


def _cleanup(
    conn: psycopg.Connection[tuple],
    *,
    instrument_ids: list[int],
    exchange_ids: list[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM external_identifiers WHERE instrument_id = ANY(%s)",
            (instrument_ids,),
        )
        cur.execute("DELETE FROM instruments WHERE instrument_id = ANY(%s)", (instrument_ids,))
        cur.execute("DELETE FROM exchanges WHERE exchange_id = ANY(%s)", (exchange_ids,))
    conn.commit()


def test_returns_one_cell_per_v1_capability(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The resolver always returns all 11 v1 capabilities, even on
    an instrument whose exchange row carries empty providers."""
    _seed_exchange_with_capabilities(
        ebull_test_conn,
        exchange_id="test_cap_001",
        asset_class="unknown",
        capabilities={cap: [] for cap in V1_CAPABILITIES},
    )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960001,
        symbol="CAP1",
        exchange="test_cap_001",
    )
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960001,
            exchange_id="test_cap_001",
        )
        assert set(resolved.cells.keys()) == set(V1_CAPABILITIES)
        for cap in V1_CAPABILITIES:
            cell = resolved.cells[cap]
            assert cell.providers == ()
            assert cell.data_present == {}
    finally:
        _cleanup(ebull_test_conn, instrument_ids=[960001], exchange_ids=["test_cap_001"])


def test_exchange_row_defaults_flow_through(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Whatever providers the exchange row lists for a capability
    appear in the resolved cell — no SEC augmentation when there's
    no external_identifiers row."""
    _seed_exchange_with_capabilities(
        ebull_test_conn,
        exchange_id="test_cap_002",
        asset_class="uk_equity",
        capabilities={
            **{cap: [] for cap in V1_CAPABILITIES},
            "filings": ["companies_house"],
            "dividends": ["lse_rns"],
        },
    )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960002,
        symbol="CAP2",
        exchange="test_cap_002",
    )
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960002,
            exchange_id="test_cap_002",
        )
        assert resolved.cells["filings"].providers == ("companies_house",)
        assert resolved.cells["dividends"].providers == ("lse_rns",)
        assert resolved.cells["insider"].providers == ()
        # Neither companies_house nor lse_rns has a wired EXISTS
        # query (no UK ingest yet), so data_present is False.
        assert resolved.cells["filings"].data_present == {"companies_house": False}
        assert resolved.cells["dividends"].data_present == {"lse_rns": False}
    finally:
        _cleanup(ebull_test_conn, instrument_ids=[960002], exchange_ids=["test_cap_002"])


def test_sec_cik_augments_capabilities(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An instrument with a SEC CIK in external_identifiers gets
    the SEC family providers added to ``filings`` /
    ``fundamentals`` / ``dividends`` / ``insider`` /
    ``ownership`` / ``corporate_events`` / ``business_summary`` /
    ``analyst`` regardless of its exchange row's defaults."""
    _seed_exchange_with_capabilities(
        ebull_test_conn,
        exchange_id="test_cap_003",
        asset_class="uk_equity",
        capabilities={
            **{cap: [] for cap in V1_CAPABILITIES},
            "filings": ["companies_house"],
        },
    )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960003,
        symbol="CAP3",
        exchange="test_cap_003",
    )
    _seed_sec_cik(ebull_test_conn, instrument_id=960003, cik="0001000003")
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960003,
            exchange_id="test_cap_003",
        )
        # Multi-source: companies_house FROM exchange row + sec_edgar
        # FROM the SEC CIK augmentation, in that order, no duplicates.
        # Note ``filings`` augment is sec_edgar (filing_events),
        # NOT sec_xbrl — Codex review caught the conflation: filings
        # capability points at the filings index, fundamentals points
        # at XBRL.
        assert resolved.cells["filings"].providers == ("companies_house", "sec_edgar")
        # SEC-only capabilities surface entirely from the augment.
        assert resolved.cells["insider"].providers == ("sec_form4",)
        assert resolved.cells["ownership"].providers == ("sec_13f", "sec_13d_13g")
        # Capabilities not augmented by SEC stay empty.
        assert resolved.cells["esg"].providers == ()
    finally:
        _cleanup(
            ebull_test_conn,
            instrument_ids=[960003],
            exchange_ids=["test_cap_003"],
        )


def test_unknown_provider_tag_skipped(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Operator hand-edits the JSONB to add a typo (e.g.
    ``companeis_house``). The resolver skips silently rather
    than raising — admin UI surfaces the override so the
    operator can fix the typo without breaking the instrument
    page."""
    _seed_exchange_with_capabilities(
        ebull_test_conn,
        exchange_id="test_cap_004",
        asset_class="uk_equity",
        capabilities={
            **{cap: [] for cap in V1_CAPABILITIES},
            "filings": ["companeis_house", "companies_house"],
        },
    )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960004,
        symbol="CAP4",
        exchange="test_cap_004",
    )
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960004,
            exchange_id="test_cap_004",
        )
        # Typo dropped; valid value retained.
        assert resolved.cells["filings"].providers == ("companies_house",)
    finally:
        _cleanup(
            ebull_test_conn,
            instrument_ids=[960004],
            exchange_ids=["test_cap_004"],
        )


def test_unknown_exchange_returns_empty_cells(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An instrument whose exchange isn't in ``exchanges`` (e.g.
    a NULL exchange column) gets every cell empty — no crash."""
    resolved = resolve_capabilities(
        ebull_test_conn,
        instrument_id=999_999_999,
        exchange_id="this_exchange_does_not_exist",
    )
    assert set(resolved.cells.keys()) == set(V1_CAPABILITIES)
    for cap in V1_CAPABILITIES:
        assert resolved.cells[cap].providers == ()


def test_unknown_exchange_with_sec_cik_still_augments(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An instrument with a NULL exchange BUT a SEC CIK still
    surfaces SEC capability coverage. Codex round-1 finding:
    returning empty cells on missing exchange contradicted
    has_sec_cik on partially-synced instruments."""
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960005,
        symbol="CAP5",
        exchange="this_exchange_does_not_exist_either",
    )
    _seed_sec_cik(ebull_test_conn, instrument_id=960005, cik="0001000005")
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960005,
            exchange_id="this_exchange_does_not_exist_either",
        )
        # Exchange row missing → no defaults. SEC CIK augment
        # still adds the SEC family.
        assert resolved.cells["filings"].providers == ("sec_edgar",)
        assert resolved.cells["insider"].providers == ("sec_form4",)
    finally:
        _cleanup(
            ebull_test_conn,
            instrument_ids=[960005],
            exchange_ids=[],
        )


def test_non_cik_sec_identifier_does_not_augment(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """external_identifiers with provider='sec' but
    identifier_type != 'cik' must NOT trigger the SEC capability
    augment — every other SEC gate in the codebase filters on
    identifier_type='cik' and the resolver must match. A future
    SEC accession-number row would otherwise overstate coverage."""
    _seed_exchange_with_capabilities(
        ebull_test_conn,
        exchange_id="test_cap_006",
        asset_class="uk_equity",
        capabilities={cap: [] for cap in V1_CAPABILITIES},
    )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960006,
        symbol="CAP6",
        exchange="test_cap_006",
    )
    with ebull_test_conn.cursor() as cur:
        # Non-CIK SEC identifier — e.g. a future accession number type.
        cur.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'accession_no', '0001234567-26-000001', TRUE)
            ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
            """,
            (960006,),
        )
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960006,
            exchange_id="test_cap_006",
        )
        # No SEC CIK → no augment. All cells empty.
        for cap in V1_CAPABILITIES:
            assert resolved.cells[cap].providers == (), f"id={cap} unexpectedly augmented from non-CIK SEC row"
    finally:
        _cleanup(
            ebull_test_conn,
            instrument_ids=[960006],
            exchange_ids=["test_cap_006"],
        )


def test_malformed_jsonb_value_does_not_crash(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """An operator hand-edits the JSONB to put a scalar / null
    where a list belongs (e.g. ``"filings": "sec_xbrl"`` instead
    of ``["sec_xbrl"]``). The resolver skips silently rather than
    500ing — the admin UI surfaces overrides for the operator to
    fix without breaking the instrument page."""
    import json as _json

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, asset_class, capabilities)
            VALUES (%s, 'unknown', %s::jsonb)
            ON CONFLICT (exchange_id) DO UPDATE SET
                asset_class  = 'unknown',
                capabilities = EXCLUDED.capabilities
            """,
            (
                "test_cap_007",
                # Mix of valid + scalar + null — every malformed
                # cell is skipped, valid ones survive.
                _json.dumps(
                    {
                        "filings": "sec_edgar",  # scalar instead of list
                        "fundamentals": None,  # null instead of list
                        "dividends": ["sec_dividend_summary"],  # valid
                    }
                ),
            ),
        )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960007,
        symbol="CAP7",
        exchange="test_cap_007",
    )
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960007,
            exchange_id="test_cap_007",
        )
        assert resolved.cells["filings"].providers == ()
        assert resolved.cells["fundamentals"].providers == ()
        assert resolved.cells["dividends"].providers == ("sec_dividend_summary",)
    finally:
        _cleanup(
            ebull_test_conn,
            instrument_ids=[960007],
            exchange_ids=["test_cap_007"],
        )


def test_non_primary_sec_cik_does_not_augment(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """external_identifiers preserves historical non-primary
    SEC CIKs (per sql/003). A non-primary CIK alone must NOT
    trigger the SEC capability augment — _has_sec_cik() filters
    on is_primary=TRUE and the resolver matches that gate.
    Codex round-2 finding on PR 3a."""
    _seed_exchange_with_capabilities(
        ebull_test_conn,
        exchange_id="test_cap_008",
        asset_class="us_equity",
        capabilities={cap: [] for cap in V1_CAPABILITIES},
    )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960008,
        symbol="CAP8",
        exchange="test_cap_008",
    )
    _seed_sec_cik(
        ebull_test_conn,
        instrument_id=960008,
        cik="0001000008",
        is_primary=False,
    )
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960008,
            exchange_id="test_cap_008",
        )
        # Non-primary CIK → no augment. All cells empty.
        for cap in V1_CAPABILITIES:
            assert resolved.cells[cap].providers == (), f"id={cap} unexpectedly augmented from non-primary SEC CIK"
    finally:
        _cleanup(
            ebull_test_conn,
            instrument_ids=[960008],
            exchange_ids=["test_cap_008"],
        )


def test_fmp_serves_fundamentals_and_analyst_via_distinct_tables(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ``fmp`` provider tag backs both ``fundamentals`` (via
    fundamentals_snapshot) and ``analyst`` (via analyst_estimates).
    A row in fundamentals_snapshot must NOT make
    analyst.data_present[fmp] = True. Codex round-2 finding on
    PR 3a: pre-fix, the resolver shared a single SQL EXISTS per
    provider, mis-reporting analyst coverage off fundamentals
    data alone."""
    _seed_exchange_with_capabilities(
        ebull_test_conn,
        exchange_id="test_cap_009",
        asset_class="us_equity",
        capabilities={
            **{cap: [] for cap in V1_CAPABILITIES},
            "fundamentals": ["fmp"],
            "analyst": ["fmp"],
        },
    )
    _seed_instrument(
        ebull_test_conn,
        instrument_id=960009,
        symbol="CAP9",
        exchange="test_cap_009",
    )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO fundamentals_snapshot
                (instrument_id, as_of_date)
            VALUES (%s, '2026-04-25')
            """,
            (960009,),
        )
    ebull_test_conn.commit()

    try:
        resolved = resolve_capabilities(
            ebull_test_conn,
            instrument_id=960009,
            exchange_id="test_cap_009",
        )
        assert resolved.cells["fundamentals"].data_present == {"fmp": True}
        # No row in analyst_estimates → analyst.data_present must
        # be False even though fmp also serves fundamentals.
        assert resolved.cells["analyst"].data_present == {"fmp": False}
    finally:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM fundamentals_snapshot WHERE instrument_id = %s", (960009,))
        _cleanup(
            ebull_test_conn,
            instrument_ids=[960009],
            exchange_ids=["test_cap_009"],
        )
