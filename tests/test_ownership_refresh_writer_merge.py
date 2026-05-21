"""PR12 writer-rewrite contract tests (#1233).

Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md
§6 (52 parametrised cases across 7 helpers + helper-specific overlays).

Pinned invariants per case:

* insert (1):           one obs → one row, fresh xmin
* no-op churn (2):      LOAD-BEARING. xmin stable + pgstattuple
                        table_len unchanged + dead_tuple delta 0;
                        state-table tuple_count delta 0, dead_tuple
                        delta <= 1. refreshed_at unchanged.
* update / amendment (3): xmin changes; refreshed_at advances.
* delete / known_to (4): MERGE NOT MATCHED BY SOURCE → DELETE.
* scope clamp (5):      A's refresh leaves B's xmin stable. Pins
                        the literal `tgt.instrument_id = %(iid)s`
                        clamp in ON + DELETE clauses.
* priority chain (6):   INSIDERS only — Form 4 wins over 13d.
* per-helper filter (7): TREASURY null guard + DEF14A 3-clause ESOP
                        exclusion (regex + holder_role + shares NOT
                        NULL).
* repair-sweep no-loop (8): same-obs UPSERT bumps ingested_at; refresh
                        no-op; _drifted_instruments returns empty.
* known_to expiry watermark (9): expire active obs (SET known_to +
                        ingested_at = clock_timestamp() explicitly,
                        Codex 1d MED-2); refresh deletes _current
                        row + advances state watermark; sweep empty.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo


# Per-helper test contract.
@dataclass(frozen=True)
class HelperCase:
    name: str  # 'funds', 'institutions', ...
    refresh_fn: Callable[[psycopg.Connection[Any], int], int]
    current_table: str
    observations_table: str
    category_literal: str
    has_priority_chain: bool  # only insiders
    has_per_helper_filter: bool  # treasury + def14a


ALL_HELPERS: list[HelperCase] = [
    HelperCase(
        "insiders",
        lambda c, i: oo.refresh_insiders_current(c, instrument_id=i),
        "ownership_insiders_current",
        "ownership_insiders_observations",
        "insiders",
        True,
        False,
    ),
    HelperCase(
        "institutions",
        lambda c, i: oo.refresh_institutions_current(c, instrument_id=i),
        "ownership_institutions_current",
        "ownership_institutions_observations",
        "institutions",
        False,
        False,
    ),
    HelperCase(
        "blockholders",
        lambda c, i: oo.refresh_blockholders_current(c, instrument_id=i),
        "ownership_blockholders_current",
        "ownership_blockholders_observations",
        "blockholders",
        False,
        False,
    ),
    HelperCase(
        "treasury",
        lambda c, i: oo.refresh_treasury_current(c, instrument_id=i),
        "ownership_treasury_current",
        "ownership_treasury_observations",
        "treasury",
        False,
        True,
    ),
    HelperCase(
        "def14a",
        lambda c, i: oo.refresh_def14a_current(c, instrument_id=i),
        "ownership_def14a_current",
        "ownership_def14a_observations",
        "def14a",
        False,
        True,
    ),
    HelperCase(
        "funds",
        lambda c, i: oo.refresh_funds_current(c, instrument_id=i),
        "ownership_funds_current",
        "ownership_funds_observations",
        "funds",
        False,
        False,
    ),
    HelperCase(
        "esop",
        lambda c, i: oo.refresh_esop_current(c, instrument_id=i),
        "ownership_esop_current",
        "ownership_esop_observations",
        "esop",
        False,
        False,
    ),
]


@pytest.fixture
def conn(ebull_test_conn):
    """Reuse the existing per-worker test DB connection fixture
    (`ebull_test_conn` from `tests/fixtures/ebull_test_db.py`)."""
    return ebull_test_conn


def _pgstattuple(conn, table: str) -> dict[str, int]:
    """Return pgstattuple measurements; fail loud on missing extension.

    Uses `%s::regclass` cast so the table-name parameter resolves to a
    regclass OID exactly as pgstattuple expects (text-parameter form
    can fail function resolution under some psycopg modes)."""
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT * FROM pgstattuple(%s::regclass)", (table,))
        except psycopg.errors.UndefinedFunction:
            pytest.fail(
                "pgstattuple extension missing in test DB — provisioning "
                "bug, do NOT skip (spec §6 CI-fail-loud contract)."
            )
        row = cur.fetchone()
        cols = [d.name for d in cur.description]
        return dict(zip(cols, row))


def _xmin_text_for_instrument(conn, current_table: str, instrument_id: int) -> list[str]:
    """Return per-row xmin::text for an instrument (deterministic order)."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT xmin::text FROM {current_table} WHERE instrument_id = %s ORDER BY 1",
            (instrument_id,),
        )
        return [r[0] for r in cur.fetchall()]


def _seed_one_observation(conn, helper: HelperCase, instrument_id: int, *, fixture_idx: int = 0) -> str:
    """Insert one observation appropriate to the helper's natural key.
    Returns the source_document_id used (for test setup chaining)."""
    # Map per-helper to the matching record_*_observation call signature.
    # This routes through the production writer so DO UPDATE / ingested_at
    # semantics are exercised exactly as production.
    run_id = uuid4()
    doc_id = f"PR12-{helper.name}-{instrument_id}-{fixture_idx}"
    filed = datetime(2025, 1, 1 + fixture_idx, tzinfo=UTC)
    period_end = date(2024, 12, 31)
    if helper.name == "insiders":
        # holder_identity_key is a schema-generated column (not a param).
        # Verified against app/services/ownership_observations.py:110-127.
        oo.record_insider_observation(
            conn,
            instrument_id=instrument_id,
            holder_cik="0000000001",
            holder_name="Test Holder",
            ownership_nature="direct",
            source="form4",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("100"),
        )
    elif helper.name == "institutions":
        oo.record_institution_observation(
            conn,
            instrument_id=instrument_id,
            filer_cik="0000000002",
            filer_name="Test Filer",
            filer_type="ETF",
            ownership_nature="economic",
            source="13f",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("1000"),
            market_value_usd=Decimal("50000"),
            voting_authority="SOLE",
            exposure_kind="EQUITY",
        )
    elif helper.name == "blockholders":
        oo.record_blockholder_observation(
            conn,
            instrument_id=instrument_id,
            reporter_cik="0000000003",
            reporter_name="Test Reporter",
            ownership_nature="beneficial",
            submission_type="SC 13G",
            status_flag=None,
            source="13g",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            aggregate_amount_owned=Decimal("200"),
            percent_of_class=Decimal("5.25"),
        )
    elif helper.name == "treasury":
        oo.record_treasury_observation(
            conn,
            instrument_id=instrument_id,
            source="xbrl_dei",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            treasury_shares=Decimal("300"),
        )
    elif helper.name == "def14a":
        oo.record_def14a_observation(
            conn,
            instrument_id=instrument_id,
            holder_name="Vanguard Group",
            holder_role="principal",
            ownership_nature="beneficial",
            source="def14a",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("400"),
            percent_of_class=Decimal("3.5"),
        )
    elif helper.name == "funds":
        # ownership_nature + source are fixed by schema CHECK constraints
        # and NOT accepted as params (app/services/ownership_observations.py:913-933).
        oo.record_fund_observation(
            conn,
            instrument_id=instrument_id,
            fund_series_id="S000000001",
            fund_series_name="Test Fund",
            fund_filer_cik="0000000004",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("500"),
            market_value_usd=Decimal("25000"),
            payoff_profile="Long",
            asset_category="EC",
        )
    elif helper.name == "esop":
        # No ownership_nature / source params — same pattern as funds.
        oo.record_esop_observation(
            conn,
            instrument_id=instrument_id,
            plan_name="Test ESOP",
            plan_trustee_name="Fidelity",
            plan_trustee_cik="0000000005",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("600"),
            percent_of_class=Decimal("2.1"),
        )
    else:
        pytest.fail(f"unknown helper: {helper.name}")
    conn.commit()
    return doc_id


# ----------------------------------------------------------------------
# Case 1: insert
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_insert(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id)
    pre_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    post_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    assert len(post_xmin) >= 1
    assert post_xmin != pre_xmin  # row was created


# ----------------------------------------------------------------------
# Case 2: no-op churn (load-bearing)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_no_op_churn(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    pre_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    pre_current_stat = _pgstattuple(conn, helper.current_table)
    pre_state_stat = _pgstattuple(conn, "ownership_refresh_state")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        pre_refreshed = cur.fetchall()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    post_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    post_current_stat = _pgstattuple(conn, helper.current_table)
    post_state_stat = _pgstattuple(conn, "ownership_refresh_state")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        post_refreshed = cur.fetchall()
    assert post_xmin == pre_xmin, "no-op refresh rewrote rows"
    assert post_current_stat["table_len"] == pre_current_stat["table_len"]
    assert post_current_stat["dead_tuple_count"] - pre_current_stat["dead_tuple_count"] == 0
    assert post_refreshed == pre_refreshed, "refreshed_at advanced on no-op"
    state_dead_delta = post_state_stat["dead_tuple_count"] - pre_state_stat["dead_tuple_count"]
    assert state_dead_delta <= 1, f"state-table churn > 1 dead tuple: {state_dead_delta}"
    state_live_delta = post_state_stat["tuple_count"] - pre_state_stat["tuple_count"]
    assert state_live_delta == 0, "state-table row count grew on no-op refresh"


# ----------------------------------------------------------------------
# Case 3: update (amendment)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_update_amendment(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id, fixture_idx=0)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    pre_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        pre_refreshed = cur.fetchall()
    # Second obs with later filed_at + different shares — same natural key.
    _seed_one_observation(conn, helper, seeded_instrument_id, fixture_idx=1)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    post_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        post_refreshed = cur.fetchall()
    assert post_xmin != pre_xmin
    assert post_refreshed > pre_refreshed


# ----------------------------------------------------------------------
# Case 4: delete (known_to expiry → MERGE NOT MATCHED BY SOURCE → DELETE)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_delete_via_known_to(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {helper.observations_table} "
            f"SET known_to = now(), ingested_at = clock_timestamp() "
            f"WHERE instrument_id = %s AND known_to IS NULL",
            (seeded_instrument_id,),
        )
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        assert cur.fetchone()[0] == 0, "MERGE NOT MATCHED BY SOURCE did not DELETE"


# ----------------------------------------------------------------------
# Case 5: scope clamp (other-instrument xmin stable)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_scope_clamp_other_instrument_untouched(conn, two_seeded_instrument_ids, helper):
    a, b = two_seeded_instrument_ids
    _seed_one_observation(conn, helper, a)
    _seed_one_observation(conn, helper, b)
    helper.refresh_fn(conn, a)
    helper.refresh_fn(conn, b)
    conn.commit()
    pre_b_xmin = _xmin_text_for_instrument(conn, helper.current_table, b)
    helper.refresh_fn(conn, a)
    conn.commit()
    post_b_xmin = _xmin_text_for_instrument(conn, helper.current_table, b)
    assert post_b_xmin == pre_b_xmin, "scope clamp leaked — other instrument rewritten"


# ----------------------------------------------------------------------
# Case 6: insiders priority chain (Form 4 wins over 13d)
# ----------------------------------------------------------------------
def test_insiders_priority_chain(conn, seeded_instrument_id):
    helper = next(h for h in ALL_HELPERS if h.name == "insiders")
    run_id = uuid4()
    period = date(2024, 12, 31)
    # Two observations same (holder_cik, ownership_nature) — the schema
    # generates holder_identity_key from holder_cik (NULL-safe) — but
    # different source priority. Form 4 (priority 1) must win.
    oo.record_insider_observation(
        conn,
        instrument_id=seeded_instrument_id,
        holder_cik="0000000007",
        holder_name="Insider X",
        ownership_nature="direct",
        source="13d",  # priority 3
        source_document_id="13d-doc",
        source_accession=None,
        source_field=None,
        source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=UTC),
        period_start=None,
        period_end=period,
        ingest_run_id=run_id,
        shares=Decimal("100"),
    )
    oo.record_insider_observation(
        conn,
        instrument_id=seeded_instrument_id,
        holder_cik="0000000007",
        holder_name="Insider X",
        ownership_nature="direct",
        source="form4",  # priority 1
        source_document_id="form4-doc",
        source_accession=None,
        source_field=None,
        source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=UTC),
        period_start=None,
        period_end=period,
        ingest_run_id=run_id,
        shares=Decimal("200"),
    )
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT source, shares FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        row = cur.fetchone()
    assert row[0] == "form4", f"priority chain broken: got source={row[0]!r}"
    assert row[1] == Decimal("200"), f"wrong row picked: shares={row[1]}"


# ----------------------------------------------------------------------
# Case 7a: treasury null-displacement guard
# ----------------------------------------------------------------------
def test_treasury_null_guard(conn, seeded_instrument_id):
    helper = next(h for h in ALL_HELPERS if h.name == "treasury")
    run_id = uuid4()
    period = date(2024, 12, 31)
    # Null observation arrives first, non-null second. Null must not displace.
    oo.record_treasury_observation(
        conn,
        instrument_id=seeded_instrument_id,
        source="xbrl_dei",
        source_document_id="null-doc",
        source_accession=None,
        source_field=None,
        source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=UTC),
        period_start=None,
        period_end=period,
        ingest_run_id=run_id,
        treasury_shares=None,
    )
    oo.record_treasury_observation(
        conn,
        instrument_id=seeded_instrument_id,
        source="xbrl_dei",
        source_document_id="good-doc",
        source_accession=None,
        source_field=None,
        source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=UTC),
        period_start=None,
        period_end=period,
        ingest_run_id=run_id,
        treasury_shares=Decimal("12345"),
    )
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT treasury_shares FROM ownership_treasury_current WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        assert cur.fetchone()[0] == Decimal("12345")


# ----------------------------------------------------------------------
# Case 7b: def14a ESOP 3-clause filter (holder_role + name regex + shares)
# ----------------------------------------------------------------------
def test_def14a_esop_exclusion(conn, seeded_instrument_id):
    helper = next(h for h in ALL_HELPERS if h.name == "def14a")
    run_id = uuid4()
    period = date(2024, 12, 31)
    filed = datetime(2025, 1, 1, tzinfo=UTC)
    # (a) holder_role='esop' — excluded.
    oo.record_def14a_observation(
        conn,
        instrument_id=seeded_instrument_id,
        holder_name="Acme ESOP Trust",
        holder_role="esop",
        ownership_nature="beneficial",
        source="def14a",
        source_document_id="esop-role-doc",
        source_accession=None,
        source_field=None,
        source_url=None,
        filed_at=filed,
        period_start=None,
        period_end=period,
        ingest_run_id=run_id,
        shares=Decimal("100"),
        percent_of_class=Decimal("1.0"),
    )
    # (b) holder_role='principal' but name matches ESOP regex — excluded.
    oo.record_def14a_observation(
        conn,
        instrument_id=seeded_instrument_id,
        holder_name="Acme Employee Stock Ownership Plan",
        holder_role="principal",
        ownership_nature="beneficial",
        source="def14a",
        source_document_id="esop-name-doc",
        source_accession=None,
        source_field=None,
        source_url=None,
        filed_at=filed,
        period_start=None,
        period_end=period,
        ingest_run_id=run_id,
        shares=Decimal("100"),
        percent_of_class=Decimal("1.0"),
    )
    # (c) holder_role='principal', name benign — included.
    oo.record_def14a_observation(
        conn,
        instrument_id=seeded_instrument_id,
        holder_name="Vanguard Group",
        holder_role="principal",
        ownership_nature="beneficial",
        source="def14a",
        source_document_id="vanguard-doc",
        source_accession=None,
        source_field=None,
        source_url=None,
        filed_at=filed,
        period_start=None,
        period_end=period,
        ingest_run_id=run_id,
        shares=Decimal("500"),
        percent_of_class=Decimal("3.5"),
    )
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT holder_name FROM ownership_def14a_current WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Vanguard Group"


# ----------------------------------------------------------------------
# Case 8: repair-sweep no-loop
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_repair_sweep_no_loop(conn, seeded_instrument_id, helper):
    from app.jobs.ownership_observations_repair import _drifted_instruments

    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    # Re-UPSERT the same obs — DO UPDATE bumps ingested_at via clock_timestamp().
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    drifted = _drifted_instruments(conn, helper.current_table, helper.observations_table, helper.category_literal)
    assert seeded_instrument_id not in drifted, (
        f"repair sweep would re-select {helper.name} instrument forever despite no-op MERGE"
    )


# ----------------------------------------------------------------------
# Case 9: known_to expiry watermark alignment
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_known_to_expiry_watermark_alignment(conn, seeded_instrument_id, helper):
    from app.jobs.ownership_observations_repair import _drifted_instruments

    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    # Explicit `ingested_at = clock_timestamp()` bump alongside known_to
    # mirrors the production ingest path's DO UPDATE clause (Codex 1d MED-2).
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {helper.observations_table} "
            f"SET known_to = now(), ingested_at = clock_timestamp() "
            f"WHERE instrument_id = %s AND known_to IS NULL",
            (seeded_instrument_id,),
        )
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    drifted = _drifted_instruments(conn, helper.current_table, helper.observations_table, helper.category_literal)
    assert seeded_instrument_id not in drifted
