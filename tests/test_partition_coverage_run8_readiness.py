"""Partition extension coverage tests for sql/174 + sql/175.

Static-analysis tests on the migration files themselves — no DB
required. Asserts the loop bounds match the Run-#8-readiness Item 10
spec target years so an accidental edit (e.g. ``'2030-04-01'`` ↔
``'2035-04-01'``) fails loudly.

See `docs/proposals/etl/run-8-readiness-fixes.md` §Item 10. Boundary
syntax was caught by Codex 1 (v1.2 → v1.3 fold).

Why static-only: pre-push pytest applies migrations via the test DB
template — a partition-coverage assertion at runtime would either
(a) duplicate what the template build implicitly tests by booting the
schema (boot-smoke at ``tests/smoke/test_app_boots.py``), or (b)
require a fresh psycopg connection in a unit test (~1.5s setup). The
static analysis here is fast (~ms) + catches the actual regression
class (typo'd loop bound) at the point of the change.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SQL_DIR = _REPO_ROOT / "sql"


def test_sql_174_finra_regsho_extends_to_2035_q1() -> None:
    """sql/174 loop bound = ``'2035-04-01'`` exclusive."""
    src = (_SQL_DIR / "174_finra_regsho_daily_partitions_2035.sql").read_text()
    matches = re.findall(r"WHILE q_start < '(\d{4}-\d{2}-\d{2})'", src)
    assert matches == ["2035-04-01"], (
        f"sql/174 loop bound drifted. Expected ['2035-04-01'], got {matches}. "
        f"Per Run-#8-readiness Item 10: 5y headroom from 2030-Q1 = 2035-Q1 "
        f"(exclusive boundary 2035-04-01)."
    )
    start_matches = re.findall(r"q_start DATE := '(\d{4}-\d{2}-\d{2})'", src)
    assert start_matches == ["2030-04-01"], (
        f"sql/174 start date drifted. Expected ['2030-04-01'] (where sql/154 ended), got {start_matches}."
    )


def test_sql_174_idempotent() -> None:
    """sql/174 must use ``IF NOT EXISTS`` so re-run is safe."""
    src = (_SQL_DIR / "174_finra_regsho_daily_partitions_2035.sql").read_text()
    assert "IF NOT EXISTS" in src, (
        "sql/174 must use CREATE TABLE IF NOT EXISTS for idempotent re-run. "
        "Without it, second run fails with relation-already-exists."
    )


def test_sql_175_financial_facts_extends_to_2040() -> None:
    """sql/175 loop bound = ``2031..2040`` (inclusive PG FOR loop)."""
    src = (_SQL_DIR / "175_financial_facts_raw_partitions_2040.sql").read_text()
    matches = re.findall(r"FOR y IN (\d+)\.\.(\d+)", src)
    assert matches == [("2031", "2040")], (
        f"sql/175 year loop drifted. Expected [('2031', '2040')], got {matches}. "
        f"Per Run-#8-readiness Item 10: 10y headroom from 2030 = 2040. Range "
        f"starts at 2031 because sql/156 covers 2010..2030."
    )


def test_sql_175_targets_canonical_parent_table() -> None:
    """sql/175 must reference ``financial_facts_raw`` (the post-rename
    canonical name), NOT ``financial_facts_raw_new`` (transient during
    sql/156 migration). Codex 1 diff re-pass focus area 9.
    """
    src = (_SQL_DIR / "175_financial_facts_raw_partitions_2040.sql").read_text()
    assert "financial_facts_raw_new" not in src, (
        "sql/175 references financial_facts_raw_new — that was the transient "
        "table name during sql/156 migration. ALTER TABLE renamed it to "
        "financial_facts_raw at sql/156:153. New partitions must attach to "
        "the post-rename canonical name."
    )
    assert "PARTITION OF financial_facts_raw" in src


def test_sql_175_idempotent() -> None:
    """sql/175 must use ``IF NOT EXISTS`` so re-run is safe."""
    src = (_SQL_DIR / "175_financial_facts_raw_partitions_2040.sql").read_text()
    assert "IF NOT EXISTS" in src
