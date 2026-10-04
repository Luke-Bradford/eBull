"""#3619 slice 2b — the N-PORT loader against a real schema: staged insert, idempotent rerun, changed-row refusal."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
import pytest

from scripts.load_3619_nport_returns import load_quarter

_SUBMISSION = (
    "ACCESSION_NUMBER\tFILING_DATE\tFILE_NUM\tSUB_TYPE\tREPORT_ENDING_PERIOD\tREPORT_DATE\tIS_LAST_FILING\n"
    "0001752724-24-232931\t30-OCT-2024\t\tNPORT-P\t31-DEC-2024\t30-SEP-2024\tN\n"
)
_HEADER = (
    "ACCESSION_NUMBER\tMONTHLY_TOTAL_RETURN_ID\tCLASS_ID\tMONTHLY_TOTAL_RETURN1\tMONTHLY_TOTAL_RETURN2"
    "\tMONTHLY_TOTAL_RETURN3\n"
)


def _write(folder: Path, third_month: str) -> Path:
    folder.mkdir(exist_ok=True)
    (folder / "SUBMISSION.tsv").write_text(_SUBMISSION)
    (folder / "MONTHLY_TOTAL_RETURN.tsv").write_text(
        _HEADER
        + f"0001752724-24-232931\t761014\tC000012090\t-.77\t4.39\t{third_month}\n"
        + "0001752724-24-232931\t761015\t\t1.1\t\t\n"
    )
    return folder


def test_load_is_idempotent_and_refuses_a_changed_row(ebull_test_conn: psycopg.Connection[Any], tmp_path: Path) -> None:
    folder = _write(tmp_path / "q", "4.29")
    assert load_quarter(ebull_test_conn, "2024q4", folder) == (4, 4)
    rows = ebull_test_conn.execute(
        "SELECT class_id, month, return_pct::text FROM sec_nport_monthly_returns"
        " ORDER BY monthly_total_return_id, month"
    ).fetchall()
    assert [(c, m.isoformat(), v) for c, m, v in rows] == [
        ("C000012090", "2024-07-01", "-0.77"),
        ("C000012090", "2024-08-01", "4.39"),
        ("C000012090", "2024-09-01", "4.29"),
        (None, "2024-07-01", "1.1"),
    ]
    assert load_quarter(ebull_test_conn, "2024q4", folder) == (4, 0)

    _write(tmp_path / "q", "4.30")
    with pytest.raises(RuntimeError, match="1 stored rows differ"):
        load_quarter(ebull_test_conn, "2024q4", folder)
    stored = ebull_test_conn.execute(
        "SELECT return_pct::text FROM sec_nport_monthly_returns WHERE month = '2024-09-01'"
    ).fetchone()
    assert stored == ("4.29",)

    (folder / "MONTHLY_TOTAL_RETURN.tsv").write_text(
        _HEADER + "0001752724-24-232931\t761014\tC000012090\t-.77\t4.39\t4.29\n"
    )
    with pytest.raises(RuntimeError, match="1 stored rows are missing"):
        load_quarter(ebull_test_conn, "2024q4", folder)
