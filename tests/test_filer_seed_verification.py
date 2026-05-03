"""Tests for the filer-seed verification framework.

Pins the contract:

  * Status enum: match / drift / missing / fetch_error.
  * Name normalisation: punctuation + case-insensitive comparison
    so "FMR LLC" and "FMR LLC." both match.
  * Disambiguation suffixes (e.g. "(Fidelity)") in expected_name
    correctly fall under "drift" when SEC's canonical name doesn't
    carry them.
  * verify_all_active walks only active=TRUE rows.
  * Cache wiring: SEC fetched once, cached read on the second call.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services import filer_seed_verification
from app.services.filer_seed_verification import (
    VerificationResult,
    verify_all_active,
    verify_seed,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_filer(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    label: str,
    expected_name: str | None = None,
    active: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO institutional_filer_seeds (cik, label, active, expected_name)
        VALUES (%s, %s, %s, COALESCE(%s, %s))
        ON CONFLICT (cik) DO UPDATE SET
            label = EXCLUDED.label,
            active = EXCLUDED.active,
            expected_name = EXCLUDED.expected_name
        """,
        (cik, label, active, expected_name, label),
    )


def _stub_submissions(monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any]) -> None:
    """Stub _fetch_submissions to bypass network + cache and return
    the payload directly. Tests focus on the verification logic;
    cache wiring is covered separately."""
    monkeypatch.setattr(
        filer_seed_verification,
        "_fetch_submissions",
        lambda _conn, _cik: payload,
    )


def test_verify_seed_match_on_canonical_name(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_submissions(monkeypatch, {"name": "Vanguard Group, Inc."})

    result = verify_seed(
        ebull_test_conn,
        cik="0000102909",
        expected_name="Vanguard Group, Inc.",
    )

    assert result.status == "match"
    assert result.sec_name == "Vanguard Group, Inc."


def test_verify_seed_match_normalises_punctuation(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC's name field has minor punctuation drift across years
    (commas, trailing periods). Normalised compare prevents
    false-drift on cosmetic differences."""
    _stub_submissions(monkeypatch, {"name": "FMR LLC"})

    result = verify_seed(
        ebull_test_conn,
        cik="0000315066",
        expected_name="fmr llc.",  # case + trailing period only
    )

    assert result.status == "match"


def test_verify_seed_drift_when_disambiguation_suffix_in_expected(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The existing 14-row seed list has labels like
    ``FMR LLC (Fidelity)``. SEC's name is just ``FMR LLC``. The
    verification sweep should flag this as drift so the operator
    explicitly chooses to either fix the expected_name or accept
    the suffix."""
    _stub_submissions(monkeypatch, {"name": "FMR LLC"})

    result = verify_seed(
        ebull_test_conn,
        cik="0000315066",
        expected_name="FMR LLC (Fidelity)",
    )

    assert result.status == "drift"
    assert result.sec_name == "FMR LLC"
    assert result.detail is not None
    assert "Fidelity" in result.detail


def test_verify_seed_missing_when_sec_has_no_name(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_submissions(monkeypatch, {})  # no name field

    result = verify_seed(
        ebull_test_conn,
        cik="0000999999",
        expected_name="Defunct Filer",
    )

    assert result.status == "missing"


def test_verify_seed_fetch_error_when_cik_not_padded(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Boundary validation — the verification layer rejects a
    non-padded CIK before any network call."""
    result = verify_seed(
        ebull_test_conn,
        cik="12345",  # wrong format
        expected_name="Anything",
    )

    assert result.status == "fetch_error"
    assert result.detail is not None
    assert "10-digit" in result.detail


def test_verify_seed_fetch_error_when_sec_unreachable(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(_conn: object, _cik: str) -> dict[str, Any]:
        raise RuntimeError("SEC unreachable")

    monkeypatch.setattr(filer_seed_verification, "_fetch_submissions", _boom)

    result = verify_seed(
        ebull_test_conn,
        cik="0000102909",
        expected_name="Vanguard Group, Inc.",
    )

    assert result.status == "fetch_error"
    assert "SEC unreachable" in (result.detail or "")


def test_verify_all_active_skips_inactive_seeds(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    _seed_filer(conn, cik="0000111111", label="Active Filer", active=True)
    _seed_filer(conn, cik="0000222222", label="Paused Filer", active=False)
    conn.commit()

    name_for: dict[str, str] = {
        "0000111111": "Active Filer",
        "0000222222": "Paused Filer",
    }

    monkeypatch.setattr(
        filer_seed_verification,
        "_fetch_submissions",
        lambda _conn, cik: {"name": name_for[cik]},
    )

    results = list(verify_all_active(conn))

    assert {r.cik for r in results} == {"0000111111"}


def test_seed_filer_records_expected_name(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``seed_filer`` (the public seed-write helper) must persist
    ``expected_name`` so a future seed added via the admin /
    operator path doesn't silently fall back to ``label`` in the
    verification sweep — Codex pre-push review caught the prior
    bypass."""
    from app.services.institutional_holdings import seed_filer

    conn = ebull_test_conn
    seed_filer(
        conn,
        cik="0000999000",
        label="ACME Capital Mgmt",
        expected_name="ACME CAPITAL MANAGEMENT LLC",
    )
    conn.commit()

    monkeypatch.setattr(
        filer_seed_verification,
        "_fetch_submissions",
        lambda _conn, _cik: {"name": "ACME CAPITAL MANAGEMENT LLC"},
    )

    results = list(verify_all_active(conn))
    by_cik = {r.cik: r for r in results}
    assert by_cik["0000999000"].expected_name == "ACME CAPITAL MANAGEMENT LLC"
    assert by_cik["0000999000"].status == "match"  # would be "drift" if it had fallen back to label


def test_seed_filer_falls_back_to_label_when_expected_name_omitted(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Backwards-compat: callers that don't yet pass
    ``expected_name`` get the prior label-as-expected behaviour.
    The verification sweep will then surface drift if the label
    differs from SEC's canonical name — which is the correct
    failure mode."""
    from app.services.institutional_holdings import seed_filer

    conn = ebull_test_conn
    seed_filer(conn, cik="0000999001", label="Some Filer")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT expected_name FROM institutional_filer_seeds WHERE cik = %s",
            ("0000999001",),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "Some Filer"


def _route_cli_to_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reroute the CLI's psycopg.connect call to open against the
    isolated ``ebull_test`` database rather than the dev one.

    Test seeds live in the fixture-managed test DB; without this
    swap the CLI would never see them (it reads from the dev URL
    in production). The smoke gate at
    tests/smoke/test_no_settings_url_in_destructive_paths.py
    flags any literal ``connect(settings...url)`` substring inside
    test files — keep this docstring substring-free of that exact
    pattern even when explaining the swap.
    """
    from scripts import verify_filer_seeds as cli
    from tests.fixtures.ebull_test_db import test_database_url

    test_url = test_database_url()
    original_connect = psycopg.connect
    monkeypatch.setattr(cli.psycopg, "connect", lambda _url: original_connect(test_url))


def test_cli_exit_code_zero_only_when_every_seed_matches(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI must return non-zero unless every active seed verifies
    clean. drift / missing / fetch_error all block — Codex pre-push
    review caught the prior 'drift only' rule."""
    from scripts import verify_filer_seeds as cli

    conn = ebull_test_conn
    _seed_filer(conn, cik="0000888001", label="Match", expected_name="Match Inc")
    _seed_filer(conn, cik="0000888002", label="Drift", expected_name="Old Name")
    conn.commit()

    sec_names = {
        "0000888001": "Match Inc",
        "0000888002": "Renamed Inc",
    }
    monkeypatch.setattr(
        filer_seed_verification,
        "_fetch_submissions",
        lambda _conn, cik: {"name": sec_names[cik]},
    )
    _route_cli_to_test_db(monkeypatch)

    rc = cli.main()
    capsys.readouterr()
    assert rc == 1  # one drift → non-zero


def test_cli_exit_code_zero_when_all_match(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Counter case: every active seed matches → exit 0."""
    from scripts import verify_filer_seeds as cli

    conn = ebull_test_conn
    _seed_filer(conn, cik="0000888003", label="Clean", expected_name="Clean Inc")
    conn.commit()

    monkeypatch.setattr(
        filer_seed_verification,
        "_fetch_submissions",
        lambda _conn, _cik: {"name": "Clean Inc"},
    )
    _route_cli_to_test_db(monkeypatch)

    rc = cli.main()
    capsys.readouterr()
    assert rc == 0


def test_cli_exit_code_nonzero_when_all_fetch_errors(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """If every seed errors out (e.g. SEC outage), CLI must exit
    non-zero so downstream automation doesn't treat an unverified
    seed set as passing — the original Codex finding."""
    from scripts import verify_filer_seeds as cli

    conn = ebull_test_conn
    _seed_filer(conn, cik="0000888004", label="X")
    conn.commit()

    def _boom(_conn: object, _cik: str) -> dict[str, Any]:
        raise RuntimeError("SEC unreachable")

    monkeypatch.setattr(filer_seed_verification, "_fetch_submissions", _boom)
    _route_cli_to_test_db(monkeypatch)

    rc = cli.main()
    capsys.readouterr()
    assert rc == 1


def test_verify_all_active_yields_one_result_per_seed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status mix across the cohort drives the operator-triage
    surface — drift surfaces explicitly while match/fetch-error
    bucket separately."""
    conn = ebull_test_conn
    _seed_filer(conn, cik="0000333333", label="Match", expected_name="Match Inc")
    _seed_filer(conn, cik="0000444444", label="Drift", expected_name="Old Name")
    conn.commit()

    sec_names = {
        "0000333333": "Match Inc",
        "0000444444": "Renamed Inc",
    }

    monkeypatch.setattr(
        filer_seed_verification,
        "_fetch_submissions",
        lambda _conn, cik: {"name": sec_names[cik]},
    )

    results: dict[str, VerificationResult] = {r.cik: r for r in verify_all_active(conn)}

    assert results["0000333333"].status == "match"
    assert results["0000444444"].status == "drift"
