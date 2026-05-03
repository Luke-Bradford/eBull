"""Schema-drift smoke gate (B5 of #797 pulled forward into Batch 1
of #788).

For every ``sql/NNN_*.sql`` migration file, parse the ``CREATE TABLE``
statements and assert that every declared column exists in the live
DB's ``information_schema.columns``. Catches the migration-093 class
of bug:

  * A migration declares ``CREATE TABLE IF NOT EXISTS foo (a, b, c, d)``
  * On a DB that already has ``foo (a, b, c)`` from a parallel
    experiment, ``CREATE TABLE IF NOT EXISTS`` no-ops and ``d`` is
    silently missing.
  * The migration is recorded as applied — the next reader of ``d``
    blows up with "column does not exist" weeks later.

This gate runs on every smoke pass and would have caught migration
093 the day it landed.

Tolerance:

  * **Extra columns in live DB are OK.** A column added by a later
    ``ALTER TABLE ... ADD COLUMN`` migration is a legitimate addition;
    we only fail on columns the CREATE statement declared but the live
    DB lacks.
  * **VIEWs are skipped.** Only physical tables (``CREATE TABLE``) are
    validated. Views are derived from underlying tables, so a column
    drift on a view IS a column drift on its source table — caught
    transitively.
  * **Comments inside column definitions are tolerated.** The parser
    strips ``-- ...`` and ``/* ... */`` runs before splitting on
    commas.
"""

from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pytest

# Same DB-reachable probe used by test_app_boots.py — keeps the smoke
# gate skip cleanly when there's no Postgres at all.


def _db_reachable() -> bool:
    try:
        from app.config import settings

        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_reachable(),
    reason="dev Postgres not reachable; schema drift gate requires the real DB",
)


_SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"

# Match ``CREATE TABLE [IF NOT EXISTS] name (...)`` capturing name +
# parenthesised body. Body may contain nested parens (``CHECK (x > 0)``)
# so the regex is permissive — we re-scan with bracket counting.
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([\w.]+)\s*\(",
    re.IGNORECASE,
)

# Recognise constraint / table-level clauses to skip when extracting
# column names. Both bare keywords (``UNIQUE (...)``) and run-on
# (``UNIQUE(``) need to match — strip the trailing paren so the
# tokeniser sees the keyword regardless of whitespace.
_NON_COLUMN_PREFIXES = (
    "CONSTRAINT",
    "PRIMARY",
    "FOREIGN",
    "UNIQUE",
    "CHECK",
    "EXCLUDE",
    "LIKE",
)


def _first_word(part: str) -> str:
    """Return the leading identifier-ish token, uppercased, with any
    trailing ``(`` stripped so ``UNIQUE(...)`` and ``UNIQUE (...)``
    both reduce to ``UNIQUE``."""
    raw = part.split(maxsplit=1)[0]
    # Strip any trailing punctuation (`(`, `,`, etc.) the next char is
    # the keyword's argument list — we only want the keyword itself.
    return re.split(r"[(,]", raw, maxsplit=1)[0].upper().strip()


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def _extract_create_table_blocks(sql: str) -> list[tuple[str, str]]:
    """Return list of ``(table_name, body)`` for every CREATE TABLE in
    the SQL text. Body is everything between the opening ``(`` and
    the matching ``)``."""
    out: list[tuple[str, str]] = []
    for match in _CREATE_TABLE_RE.finditer(sql):
        table_name = match.group(1).split(".")[-1].lower()
        # Walk forward from the opening paren to find the matching close.
        depth = 0
        start = match.end() - 1  # index of '('
        body_start = start + 1
        for i in range(start, len(sql)):
            c = sql[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    out.append((table_name, sql[body_start:i]))
                    break
    return out


def _extract_columns(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas at depth 0, then keep only
    the entries that look like column definitions (skip CONSTRAINT /
    PRIMARY / etc. clauses)."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for c in body:
        if c == "(":
            depth += 1
            current.append(c)
        elif c == ")":
            depth -= 1
            current.append(c)
        elif c == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(c)
    if current:
        parts.append("".join(current).strip())

    columns: list[str] = []
    for part in parts:
        if not part:
            continue
        if _first_word(part) in _NON_COLUMN_PREFIXES:
            continue
        # Column definition: ``name TYPE ...``. First token is the
        # column name, possibly quoted.
        col_name = part.split(maxsplit=1)[0].strip().strip('"').lower()
        if col_name:
            columns.append(col_name)
    return columns


def _live_columns_for(conn: psycopg.Connection[tuple], table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            """,
            (table,),
        )
        return {str(r[0]) for r in cur.fetchall()}


def test_no_create_table_column_drift() -> None:
    """For every CREATE TABLE in ``sql/*.sql``, every declared column
    must exist in the live DB.

    Extra columns in live (added by later ALTER TABLE) are OK.
    Missing columns are NOT OK and fail the gate.
    """
    files = sorted(_SQL_DIR.glob("*.sql"))
    assert files, "no migration files found under sql/"

    from app.config import settings

    drift_findings: list[str] = []
    with psycopg.connect(settings.database_url) as conn:
        for path in files:
            sql = _strip_comments(path.read_text(encoding="utf-8"))
            for table, body in _extract_create_table_blocks(sql):
                declared = set(_extract_columns(body))
                if not declared:
                    continue
                live = _live_columns_for(conn, table)
                if not live:
                    # Table was renamed or dropped by a later migration —
                    # not drift; skip. (A typo'd CREATE that never landed
                    # would also surface here, but the migration runner
                    # would have failed loudly long before this gate.)
                    continue
                missing = declared - live
                if missing:
                    drift_findings.append(
                        f"{path.name}: table {table!r} declares columns "
                        f"{sorted(missing)} but the live DB lacks them. "
                        f"This is the migration-093 class of bug — "
                        f"a CREATE TABLE IF NOT EXISTS no-op'd onto a "
                        f"pre-existing table with a different shape."
                    )

    assert not drift_findings, "schema drift detected:\n" + "\n".join(drift_findings)
