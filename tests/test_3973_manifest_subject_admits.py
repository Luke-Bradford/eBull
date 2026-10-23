"""#3973 — a Form 3/4/5 is never written under a filer subject (pure, no DB)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from app.services.sec_manifest import manifest_subject_admits, record_manifest_entry


@pytest.mark.parametrize("source", ["sec_form3", "sec_form4", "sec_form5"])
@pytest.mark.parametrize("subject_type", ["institutional_filer", "blockholder_filer", "fund_series"])
def test_filer_subject_refused_for_insider_sources(subject_type: str, source: str) -> None:
    assert manifest_subject_admits(subject_type, source) is False  # type: ignore[arg-type]


@pytest.mark.parametrize("source", ["sec_form3", "sec_form4", "sec_form5", "sec_13f_hr", "sec_13g", "sec_8k"])
def test_issuer_subject_admits_every_source(source: str) -> None:
    assert manifest_subject_admits("issuer", source) is True  # type: ignore[arg-type]


@pytest.mark.parametrize("source", ["sec_13f_hr", "sec_13d", "sec_13g", "sec_n_port"])
def test_filer_subject_keeps_its_own_sources(source: str) -> None:
    # 13D/G filed by an institutional filer parse today under that subject;
    # the refusal is scoped to the issuer-keyed insider forms only.
    assert manifest_subject_admits("institutional_filer", source) is True  # type: ignore[arg-type]


def test_record_manifest_entry_refusal_writes_nothing() -> None:
    conn = MagicMock()
    written = record_manifest_entry(
        conn,
        "0001104659-23-075930",
        cik="0001176948",
        form="4",
        source="sec_form4",
        subject_type="institutional_filer",
        subject_id="0001176948",
        instrument_id=None,
        filed_at=datetime(2023, 6, 30, tzinfo=UTC),
    )
    assert written is False
    conn.cursor.assert_not_called()
    conn.execute.assert_not_called()
    conn.transaction.assert_not_called()
