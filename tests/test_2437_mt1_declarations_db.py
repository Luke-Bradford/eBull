"""Database boundary for the atomic MT-1 declaration freeze (#2437)."""

from __future__ import annotations

from dataclasses import replace

import psycopg
import pytest

from app.services.result_ledger import freeze_preregistration, load_preregistration
from scripts.freeze_2437_mt1_declarations import _freeze_batch, build_declarations

# #2829 — freezes synthetic or pre-mapped identities while testing a different
# gate; see `assume_trial_registered` in tests/conftest.py.
pytestmark = pytest.mark.usefixtures("assume_trial_registered")


def test_both_declarations_freeze_atomically_and_identical_retry_is_safe(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    declarations = build_declarations()
    reports, ok = _freeze_batch(ebull_test_conn, declarations)
    assert ok is True
    assert [report["outcome"] for report in reports] == ["frozen", "frozen"]

    for declaration in declarations:
        stored = load_preregistration(ebull_test_conn, declaration.strategy_id, declaration.strategy_version)
        assert stored is not None
        assert stored.declaration_sha256 == declaration.sha256

    retry_reports, retry_ok = _freeze_batch(ebull_test_conn, declarations)
    assert retry_ok is True
    assert [report["outcome"] for report in retry_reports] == [
        "already_frozen_identical",
        "already_frozen_identical",
    ]


def test_a_conflicting_first_row_prevents_the_second_row_from_being_written(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    mt1, control = build_declarations()
    conflicting = replace(mt1, declared_by="a different pre-outcome declaration")
    freeze_preregistration(ebull_test_conn, conflicting)
    ebull_test_conn.commit()

    reports, ok = _freeze_batch(ebull_test_conn, (mt1, control))
    assert ok is False
    assert reports[0]["outcome"] == "conflicting_declaration_already_frozen"
    assert load_preregistration(ebull_test_conn, control.strategy_id, control.strategy_version) is None
