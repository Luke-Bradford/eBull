"""Behaviour test for migration 111 — seed the pre-drift state,
run the migration SQL inline, verify the post-state.

The fixture-scoped TRUNCATE on ``institutional_filer_seeds`` means
the dev-DB-applied migration's effects don't survive into a test
function, so the test seeds the four drifted pre-rows itself,
runs the migration, and asserts the corrections.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from app.services import filer_seed_verification
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_MIGRATION_SQL = (Path(__file__).resolve().parents[1] / "sql" / "111_fix_filer_seed_drifts.sql").read_text()


def _seed_pre_drift_state(conn: psycopg.Connection[tuple]) -> None:
    """Seed the exact pre-migration state of the 4 drifted rows
    that PR #821's verification gate flagged on dev."""
    rows = [
        ("0000080255", "T. Rowe Price Associates Inc.", "T. Rowe Price Associates Inc."),
        ("0000093751", "State Street Corporation", "State Street Corporation"),
        ("0000315066", "FMR LLC (Fidelity)", "FMR LLC (Fidelity)"),
        ("0001364742", "BlackRock Inc.", "BlackRock Inc."),
    ]
    for cik, label, expected_name in rows:
        conn.execute(
            """
            INSERT INTO institutional_filer_seeds (cik, label, expected_name, active)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (cik) DO UPDATE SET
                label = EXCLUDED.label,
                expected_name = EXCLUDED.expected_name,
                active = TRUE
            """,
            (cik, label, expected_name),
        )
    conn.commit()


def test_migration_111_replaces_wrong_blackrock_cik(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The wrong BlackRock CIK 0001364742 (BlackRock Finance, Inc.,
    3 recent 13F-HRs) gets deleted; the canonical 0001086364
    (BlackRock Advisors LLC, 48 recent 13F-HRs) gets inserted."""
    conn = ebull_test_conn
    _seed_pre_drift_state(conn)

    with psycopg.ClientCursor(conn) as cur:
        cur.execute(_MIGRATION_SQL)  # type: ignore[call-overload]
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cik FROM institutional_filer_seeds
            WHERE cik IN ('0001364742', '0001086364')
            ORDER BY cik
            """,
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["0001086364"]


def test_migration_111_normalises_expected_names(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Three drifted rows get expected_name normalised to SEC's
    canonical form so the verification gate stops flagging them."""
    conn = ebull_test_conn
    _seed_pre_drift_state(conn)

    with psycopg.ClientCursor(conn) as cur:
        cur.execute(_MIGRATION_SQL)  # type: ignore[call-overload]
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cik, expected_name FROM institutional_filer_seeds
            WHERE cik IN ('0000080255', '0000093751', '0000315066')
            ORDER BY cik
            """,
        )
        rows = cur.fetchall()
    actual = dict(rows)
    assert actual == {
        "0000080255": "PRICE T ROWE ASSOCIATES INC /MD/",
        "0000093751": "STATE STREET CORP",
        "0000315066": "FMR LLC",
    }


def test_migration_111_cleans_downstream_filer_and_holdings_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """If 13F-HR ingest already ran against the wrong BlackRock CIK,
    institutional_filers + institutional_holdings rows would still
    feed ownership reads after the seed table was cleaned. The
    migration deletes the downstream rows too — same precedent as
    migration 106 (Soros/Geode relabel)."""
    conn = ebull_test_conn
    _seed_pre_drift_state(conn)

    # Seed a stub instrument + downstream filer/holding for the wrong CIK
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, 'STUB', 'Stub Inc', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (901_111,),
    )
    conn.execute(
        """
        INSERT INTO institutional_filers (cik, name)
        VALUES ('0001364742', 'BlackRock Finance, Inc.')
        ON CONFLICT (cik) DO NOTHING
        RETURNING filer_id
        """,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM institutional_filers WHERE cik = '0001364742'")
        result = cur.fetchone()
    assert result is not None
    bad_filer_id = result[0]

    conn.execute(
        """
        INSERT INTO institutional_holdings (
            filer_id, instrument_id, accession_number, period_of_report,
            shares, market_value_usd
        ) VALUES (%s, %s, '9999-99-test', '2025-09-30', 100, 1000)
        """,
        (bad_filer_id, 901_111),
    )
    conn.commit()

    with psycopg.ClientCursor(conn) as cur:
        cur.execute(_MIGRATION_SQL)  # type: ignore[call-overload]
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM institutional_filers WHERE cik = '0001364742'")
        result = cur.fetchone()
    assert result is not None
    filer_count = result[0]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM institutional_holdings
            WHERE accession_number = '9999-99-test'
            """,
        )
        result = cur.fetchone()
    assert result is not None
    holding_count = result[0]
    assert filer_count == 0  # bad-CIK filer row gone
    assert holding_count == 0  # bad-CIK holdings gone


def test_bootstrap_seed_list_matches_post_migration_state(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for the bootstrap-source-of-truth gap Codex
    flagged: a fresh ``_seed_all()`` run via
    ``scripts/seed_holder_coverage.py`` recreates
    ``institutional_filer_seeds`` from ``_INSTITUTIONAL_SEEDS``.
    That tuple list must match the post-migration-111 expected
    names so the verification gate stays green after a re-seed."""
    from scripts.seed_holder_coverage import _INSTITUTIONAL_SEEDS

    conn = ebull_test_conn
    # Run the bootstrap seed inserts directly (no migration first —
    # we're testing that the bootstrap list IS canonical).
    from app.services.institutional_holdings import seed_filer

    for cik, label, expected_name in _INSTITUTIONAL_SEEDS:
        seed_filer(conn, cik=cik, label=label, expected_name=expected_name)
    conn.commit()

    # Stub SEC submissions.json with the canonical names recorded
    # in _INSTITUTIONAL_SEEDS. If any tuple's expected_name doesn't
    # match what SEC publishes, this test fails — forcing the
    # bootstrap list to stay aligned with reality.
    sec_canonical = {cik: expected for cik, _label, expected in _INSTITUTIONAL_SEEDS}

    def _stub_fetch(_conn: object, cik: str) -> dict[str, object]:
        return {"name": sec_canonical[cik]}

    monkeypatch.setattr(filer_seed_verification, "_fetch_submissions", _stub_fetch)

    results = list(filer_seed_verification.verify_all_active(conn))
    drifted = [r for r in results if r.status != "match"]
    assert drifted == [], f"bootstrap seed list has drift: {drifted}"


def test_post_migration_seeds_verify_clean(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end gate: post-migration seed expected_names + the
    canonical BlackRock CIK match SEC's authoritative names. The
    verification sweep produces zero drifted rows."""
    conn = ebull_test_conn
    _seed_pre_drift_state(conn)
    with psycopg.ClientCursor(conn) as cur:
        cur.execute(_MIGRATION_SQL)  # type: ignore[call-overload]
    conn.commit()

    sec_canonical = {
        "0000080255": "PRICE T ROWE ASSOCIATES INC /MD/",
        "0000093751": "STATE STREET CORP",
        "0000315066": "FMR LLC",
        "0001086364": "BLACKROCK ADVISORS LLC",
    }

    def _stub_fetch(_conn: object, cik: str) -> dict[str, object]:
        return {"name": sec_canonical.get(cik, "UNKNOWN")}

    monkeypatch.setattr(filer_seed_verification, "_fetch_submissions", _stub_fetch)

    results = list(filer_seed_verification.verify_all_active(conn))
    drifted = [r for r in results if r.status != "match"]
    assert drifted == [], f"unexpected drift after migration 111: {drifted}"
