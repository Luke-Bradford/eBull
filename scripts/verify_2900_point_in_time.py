"""Reproduce the frozen #2900 point-in-time admissibility verdict.

Run only after the implementation commit is clean:

    PYTHONPATH=. uv run python -m scripts.verify_2900_point_in_time --format json
    PYTHONPATH=. uv run python -m scripts.verify_2900_point_in_time --format markdown
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import io
import json
import re
import subprocess
import tokenize
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from app.config import settings
from app.services.ownership_observations import record_institution_observation
from app.services.research_point_in_time import FIELD_REGISTRY, PROBE_MATRIX, REGISTRY_VERSION, RankingFamily
from scripts._dev_guard import assert_dev_environment

DECLARATION_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-point-in-time-spine-declaration.md")
DECLARATION_SHA256: Final = "369b397e17694f2a54b07897b4d68a8728bf0624e35d00ced8a6ce833bb4da20"
DECLARATION_COMMIT: Final = "51e55d58823a5ebe98e5eea6473895b9d05abc1d"
CORRECTION_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-point-in-time-spine-correction-1.md")
CORRECTION_SHA256: Final = "b101123c59183b8204a70b98a9c40b25e350a4fc58a9c114d8ea76735157cff6"
CORRECTION_COMMIT: Final = "fe67f100e565fd52db8201ac6ad8f1758c2b163f"
CORRECTION_2_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-point-in-time-spine-correction-2.md")
CORRECTION_2_SHA256: Final = "a330ed170d39da3c201e2bd8e1ce5f80566209a356faa75bb899090e5e2b4f32"
CORRECTION_2_COMMIT: Final = "055056a5c3b1aa3bc5971de4a6085b0f7bd72206"
CORRECTION_3_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-point-in-time-spine-correction-3.md")
CORRECTION_3_SHA256: Final = "3186b8573a2bd66dbe04eb0cf378e520dad8e9deaccae4dc92800c696708445d"
CORRECTION_3_COMMIT: Final = "4fc9c397dd5c31835856d948137fc0cab3318841"
DECISION_DATE: Final = date(2020, 1, 15)
SENTINEL_CIK: Final = "0000002900"
SENTINEL_DOCUMENT: Final = "0000002900-20-002900"
SENTINEL_PERIOD_END: Final = date(2019, 12, 31)
SENTINEL_RUN: Final = UUID("00000000-0000-0000-0000-000000002900")
POST_DOCUMENT: Final = "0000002900-20-002901"
SCHEMA_VERSION: Final = "r6-2900-evidence-v1"

_TUPLE_COLUMNS: Final = (
    "instrument_id",
    "filer_cik",
    "filer_name",
    "filer_type",
    "ownership_nature",
    "source",
    "source_document_id",
    "source_accession",
    "source_field",
    "source_url",
    "filed_at",
    "period_start",
    "period_end",
    "known_from",
    "known_to",
    "ingest_run_id",
    "shares",
    "market_value_usd",
    "voting_authority",
    "exposure_kind",
    "ingested_at",
)


@dataclass(frozen=True)
class SourceAnchor:
    path: str
    needle: str
    minimum: int = 1
    maximum: int | None = None


@dataclass(frozen=True)
class ProbeResult:
    probe_id: str
    passed: bool
    anchor_counts: Mapping[str, int]
    source_sha256: Mapping[str, str]
    detail: str


@dataclass(frozen=True)
class MutationEvidence:
    instrument_id: int
    before_sha256: str
    postdated_control_sha256: str
    overwritten_sha256: str
    old_vintage_rows_after_overwrite: int
    first_unequal_column: str
    before_value: object
    overwritten_value: object
    rollback_proved: bool


@dataclass(frozen=True)
class Evidence:
    schema_version: str
    execution_commit: str
    declaration_commit: str
    declaration_sha256: str
    correction_commit: str
    correction_sha256: str
    correction_2_commit: str
    correction_2_sha256: str
    correction_3_commit: str
    correction_3_sha256: str
    decision_date: str
    registry_version: str
    registry: Mapping[str, object]
    probes: Sequence[ProbeResult]
    censuses: Mapping[str, object]
    mutation: MutationEvidence
    verdict: str


_PROBE_ANCHORS: Final[Mapping[str, tuple[SourceAnchor, ...]]] = {
    "D0": (SourceAnchor("sql/032_financial_data_enrichment_p1.sql", "filed_date           DATE,", minimum=2),),
    "F0": (
        SourceAnchor("app/services/fundamentals/__init__.py", '"filed_date",', minimum=1),
        SourceAnchor("sql/032_financial_data_enrichment_p1.sql", "filed_date           DATE NOT NULL", maximum=1),
    ),
    "F1": (SourceAnchor("app/services/fundamentals/__init__.py", "DO UPDATE SET\n    val = EXCLUDED.val", maximum=1),),
    "F2": (SourceAnchor("app/services/financial_facts_retention.py", "DELETE FROM financial_facts_raw", maximum=1),),
    "D1": (
        SourceAnchor(
            "app/services/fundamentals/__init__.py",
            "DELETE FROM financial_periods_raw WHERE instrument_id = %(iid)s AND source = 'sec_edgar'",
            maximum=1,
        ),
        SourceAnchor(
            "app/services/fundamentals/__init__.py",
            "SELECT DISTINCT ON (fiscal_year, fiscal_quarter, period_type)",
            minimum=2,
        ),
    ),
    "X1": (
        SourceAnchor("app/providers/implementations/sec_fundamentals.py", "companyfacts", minimum=1),
        SourceAnchor(
            "sql/032_financial_data_enrichment_p1.sql", "CREATE TABLE IF NOT EXISTS financial_facts_raw", maximum=1
        ),
    ),
    "O1": (
        SourceAnchor("app/services/ownership_observations.py", "def record_", minimum=7),
        SourceAnchor("app/services/ownership_observations.py", "ON CONFLICT (", minimum=22, maximum=22),
        SourceAnchor("app/services/ownership_observations.py", "ingested_at = clock_timestamp()", minimum=7),
    ),
    "O0": (
        SourceAnchor(
            "sql/114_ownership_institutions_observations.sql", "filed_at                TIMESTAMPTZ NOT NULL", minimum=2
        ),
    ),
    "O2": (
        SourceAnchor("app/services/def14a_ingest.py", "fetched_at = datetime.now(tz=UTC)", minimum=2),
        SourceAnchor("app/services/def14a_ingest.py", "filed_at = fetched_at", minimum=2),
    ),
    "O3": (
        SourceAnchor("app/services/ownership_observations_sync.py", "retention_cutoff", minimum=3),
        SourceAnchor("app/services/rewash_filings.py", "DELETE FROM", minimum=1),
        SourceAnchor("app/services/ownership_observations.py", "SET known_to = NOW()", minimum=1),
    ),
    "R0": (SourceAnchor("sql/001_init.sql", "filing_date DATE NOT NULL", maximum=1),),
    "R1": (
        SourceAnchor("app/services/sec_filing_items.py", "SET items = %s, red_flag_score = %s", maximum=1),
        SourceAnchor("app/services/filings_risk.py", "SELECT code, severity FROM sec_8k_item_codes", maximum=1),
    ),
    "N0": (
        SourceAnchor(
            "sql/152_finra_short_interest.sql", "settlement_date         DATE   NOT NULL", minimum=2, maximum=2
        ),
    ),
    "N1": (
        SourceAnchor(
            "app/services/finra_short_interest_ingest.py",
            "filed_at = datetime.combine(settlement_date, datetime.min.time(), tzinfo=UTC)",
            maximum=1,
        ),
        SourceAnchor(
            "app/services/finra_short_interest_ingest.py",
            "current_short_interest = EXCLUDED.current_short_interest",
            minimum=2,
        ),
    ),
    "N2": (
        SourceAnchor(
            "app/services/finra_short_interest_ingest.py",
            "SELECT instrument_id, symbol FROM instruments WHERE is_tradable = TRUE",
            maximum=1,
        ),
    ),
    "L1": (SourceAnchor("app/services/strategies/validated_universe.py", "WHERE i.is_tradable", maximum=1),),
    "H1": (
        SourceAnchor("app/services/strategies/validated_universe.py", "WHERE i.is_tradable", maximum=1),
        SourceAnchor("app/services/universe_selection.py", "if alive:", maximum=1),
    ),
    "H2": (
        SourceAnchor(
            "sql/271_instrument_universe_membership.sql",
            "source_event IN ('imported', 'listing', 'relisting')",
            maximum=1,
        ),
        SourceAnchor("sql/271_instrument_universe_membership.sql", "true start unknown and truncated here", maximum=1),
    ),
    "H3": (
        SourceAnchor("sql/103_instrument_symbol_history.sql", "Synthetic backfill from former_names", maximum=1),
        SourceAnchor("sql/003_external_identifiers.sql", "last_verified_at", maximum=1),
    ),
    "P0": (SourceAnchor("sql/249_research_price_corpus.sql", "bar_date    DATE NOT NULL", maximum=1),),
    "P1": (
        SourceAnchor("sql/249_research_price_corpus.sql", "CREATE TABLE IF NOT EXISTS research_price_daily", maximum=1),
    ),
    "P2": (
        SourceAnchor("app/services/universe_selection.py", "validated_ids", minimum=4),
        SourceAnchor("app/services/universe_selection.py", "if alive:", maximum=1),
    ),
    "P3": (
        SourceAnchor("app/services/price_quarantine.py", "def rule_b4(prev: Bar, bar: Bar, nxt: Bar", maximum=1),
        SourceAnchor("app/services/price_quarantine.py", "next_close / close", maximum=1),
    ),
    "P4": (
        SourceAnchor("app/services/research_corpus_ingest.py", 'ADJUSTMENT_BASIS = "split_adjusted"', maximum=1),
        SourceAnchor("app/services/backtest_run.py", 'price_basis="split_adjusted"', minimum=2, maximum=2),
    ),
    "P5": (
        SourceAnchor(
            "app/services/universe_selection.py",
            "INTRADER_CAPTURE_DATE: Final = date(2024, 9, 27)",
            maximum=1,
        ),
        SourceAnchor("app/services/universe_selection.py", "EXCHANGE_TEST_ISSUE_SYMBOLS", minimum=3, maximum=3),
    ),
}


def _file_sha(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _python_semantic_text(path: str) -> str:
    source = Path(path).read_text()
    tree = ast.parse(source, filename=path)
    path_obj = Path(path)
    if not path_obj.is_absolute():
        module_name = path.removesuffix(".py").replace("/", ".")
        if importlib.util.find_spec(module_name) is None:
            raise RuntimeError(f"Python probe module does not resolve: {module_name}")

    excluded: set[int] = set()
    owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if isinstance(node, owners) and node.body:
            first = node.body[0]
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                excluded.update(range(first.lineno, (first.end_lineno or first.lineno) + 1))
        if isinstance(node, ast.If) and isinstance(node.test, ast.Constant) and not bool(node.test.value):
            for child in node.body:
                excluded.update(range(child.lineno, (child.end_lineno or child.lineno) + 1))

    masked = "".join("\n" if number in excluded else line for number, line in enumerate(source.splitlines(True), 1))
    tokens = tokenize.generate_tokens(io.StringIO(masked).readline)
    return tokenize.untokenize(token for token in tokens if token.type != tokenize.COMMENT)


def _semantic_text(path: str) -> str:
    if path.endswith(".py"):
        return _python_semantic_text(path)
    source = Path(path).read_text()
    without_blocks = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", "", without_blocks)


def _semantic_anchor_count(text: str, needle: str) -> int:
    """Count an executable anchor without coupling it to source formatting."""

    def normalize(value: str) -> str:
        lines = (re.sub(r"[^\S\n]+", " ", line).strip() for line in value.splitlines())
        return "\n".join(line for line in lines if line)

    normalized_text = normalize(text)
    normalized_needle = normalize(needle)
    return normalized_text.count(normalized_needle)


def run_source_probes(conn: psycopg.Connection[Any]) -> tuple[ProbeResult, ...]:
    """Execute every declared source/schema probe; fail on vacuous anchors."""
    declared = {probe for cells in PROBE_MATRIX.values() for cell in cells.values() for probe in cell.probes}
    if declared != set(_PROBE_ANCHORS):
        raise RuntimeError(f"probe definition mismatch: declared={sorted(declared)} defined={sorted(_PROBE_ANCHORS)}")

    results: list[ProbeResult] = []
    for probe_id in sorted(_PROBE_ANCHORS):
        counts: dict[str, int] = {}
        hashes: dict[str, str] = {}
        for index, anchor in enumerate(_PROBE_ANCHORS[probe_id]):
            text = _semantic_text(anchor.path)
            count = _semantic_anchor_count(text, anchor.needle)
            key = f"{anchor.path}:{index}"
            counts[key] = count
            hashes[anchor.path] = _file_sha(anchor.path)
            maximum = anchor.maximum
            if count < anchor.minimum or (maximum is not None and count > maximum):
                expected = f"{anchor.minimum}..{maximum}" if maximum is not None else f">={anchor.minimum}"
                raise RuntimeError(
                    f"probe {probe_id} anchor {anchor.needle!r} in {anchor.path} matched {count}; expected {expected}"
                )
        results.append(ProbeResult(probe_id, True, counts, hashes, "source anchors non-vacuous"))

    # Bind positive and negative DDL claims to the live schema.
    for table, required, forbidden, probe_id in (
        ("financial_periods_raw", {"filed_date"}, set(), "D0"),
        ("financial_facts_raw", {"filed_date"}, {"dimension", "member", "dimensions"}, "F0/X1"),
        (
            "ownership_institutions_observations",
            {"filed_at", "ingested_at", "known_from", "known_to"},
            set(),
            "O0/O1",
        ),
        (
            "research_price_daily",
            {"bar_date"},
            {"observed_at", "published_at", "source_public_at", "source_version"},
            "P0/P1",
        ),
        ("finra_short_interest_observations", {"settlement_date", "filed_at", "known_from"}, set(), "N0/N1"),
        ("instrument_universe_membership", {"effective_from", "effective_to"}, set(), "H2"),
        ("instrument_symbol_history", {"effective_from", "effective_to"}, set(), "H3"),
        ("external_identifiers", {"last_verified_at"}, set(), "H3"),
    ):
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",
            (table,),
        ).fetchall()
        columns = {str(row[0]) for row in rows}
        if not required <= columns or columns & forbidden:
            raise RuntimeError(f"probe {probe_id} schema failure for {table}: columns={sorted(columns)}")

    return tuple(results)


def derive_verdict(probes: Sequence[ProbeResult]) -> str:
    passed = {probe.probe_id for probe in probes if probe.passed}
    for family, cells in PROBE_MATRIX.items():
        if not any(cell.outcome == "fail" for cell in cells.values()):
            raise RuntimeError(f"family {family.value} has no failed admissibility condition")
        for condition, cell in cells.items():
            missing = set(cell.probes) - passed
            if missing:
                raise RuntimeError(f"family {family.value}/{condition} lacks probes {sorted(missing)}")
    return "FAIL — NO ADMISSIBLE HISTORICAL FIELD"


def _canonical(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    return value


def _load_sentinel_rows(
    conn: psycopg.Connection[Any], *, iid: int, ingested_through: datetime | None = None
) -> list[dict[str, object]]:
    through = "AND ingested_at <= %(through)s" if ingested_through is not None else ""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT {", ".join(_TUPLE_COLUMNS)}
            FROM ownership_institutions_observations
            WHERE instrument_id = %(iid)s
              AND filer_cik = %(cik)s
              AND filed_at < %(decision)s
              {through}
            ORDER BY instrument_id, filer_cik, ownership_nature, period_end,
                     source_document_id, exposure_kind
            """,
            {"iid": iid, "cik": SENTINEL_CIK, "decision": DECISION_DATE, "through": ingested_through},
        )
        return [{key: _canonical(value) for key, value in row.items()} for row in cur.fetchall()]


def _rows_sha(rows: Sequence[Mapping[str, object]]) -> str:
    payload = "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows)
    return hashlib.sha256(payload.encode()).hexdigest()


def _record_specimen(
    conn: psycopg.Connection[Any],
    *,
    iid: int,
    document: str,
    filed_at: datetime,
    period_end: date,
    shares: Decimal,
    run_id: UUID,
) -> None:
    record_institution_observation(
        conn,
        instrument_id=iid,
        filer_cik=SENTINEL_CIK,
        filer_name="R6 PIT Sentinel",
        filer_type="OTHER",
        ownership_nature="economic",
        source="13f",
        source_document_id=document,
        source_accession=document,
        source_field="shares",
        source_url="https://example.invalid/r6-2900",
        filed_at=filed_at,
        period_start=None,
        period_end=period_end,
        ingest_run_id=run_id,
        shares=shares,
        market_value_usd=Decimal("1000.00"),
        voting_authority="SOLE",
        exposure_kind="EQUITY",
    )


def run_mutation_test(conn: psycopg.Connection[Any]) -> MutationEvidence:
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('r6-2900-pit-verifier', 0))")
    row = conn.execute("SELECT min(instrument_id) FROM instruments").fetchone()
    iid = int(row[0]) if row and row[0] is not None else None
    if iid is None:
        raise RuntimeError("no instrument exists for the rollback-only sentinel")
    collision = conn.execute(
        """
        SELECT 1 FROM ownership_institutions_observations
        WHERE instrument_id=%s AND filer_cik=%s AND ownership_nature='economic'
          AND period_end=%s AND source_document_id=%s AND exposure_kind='EQUITY'
        """,
        (iid, SENTINEL_CIK, SENTINEL_PERIOD_END, SENTINEL_DOCUMENT),
    ).fetchone()
    if collision:
        raise RuntimeError("fixed #2900 sentinel already exists; refusing to overwrite it")

    _record_specimen(
        conn,
        iid=iid,
        document=SENTINEL_DOCUMENT,
        filed_at=datetime(2020, 1, 14, tzinfo=UTC),
        period_end=SENTINEL_PERIOD_END,
        shares=Decimal("100"),
        run_id=SENTINEL_RUN,
    )
    before = _load_sentinel_rows(conn, iid=iid)
    if len(before) != 1:
        raise RuntimeError(f"sentinel baseline expected one row, got {len(before)}")
    baseline_ingested_at = datetime.fromisoformat(str(before[0]["ingested_at"]))
    before_sha = _rows_sha(before)

    _record_specimen(
        conn,
        iid=iid,
        document=POST_DOCUMENT,
        filed_at=datetime(2020, 1, 16, tzinfo=UTC),
        period_end=date(2020, 1, 31),
        shares=Decimal("999"),
        run_id=UUID("00000000-0000-0000-0000-000000002901"),
    )
    control_sha = _rows_sha(_load_sentinel_rows(conn, iid=iid))
    if control_sha != before_sha:
        raise RuntimeError("post-decision observation changed the public-clock control")

    _record_specimen(
        conn,
        iid=iid,
        document=SENTINEL_DOCUMENT,
        filed_at=datetime(2020, 1, 14, tzinfo=UTC),
        period_end=SENTINEL_PERIOD_END,
        shares=Decimal("101"),
        run_id=UUID("00000000-0000-0000-0000-000000002902"),
    )
    overwritten = _load_sentinel_rows(conn, iid=iid)
    overwritten_sha = _rows_sha(overwritten)
    if overwritten_sha == before_sha:
        raise RuntimeError("same-key historical overwrite was not detected")
    old_rows = _load_sentinel_rows(conn, iid=iid, ingested_through=baseline_ingested_at)
    if old_rows:
        raise RuntimeError("system-time predicate unexpectedly recovered an overwritten predecessor")

    differing = next(column for column in _TUPLE_COLUMNS if before[0][column] != overwritten[0][column])
    return MutationEvidence(
        instrument_id=iid,
        before_sha256=before_sha,
        postdated_control_sha256=control_sha,
        overwritten_sha256=overwritten_sha,
        old_vintage_rows_after_overwrite=len(old_rows),
        first_unequal_column=differing,
        before_value=before[0][differing],
        overwritten_value=overwritten[0][differing],
        rollback_proved=False,
    )


_CENSUS_TABLES: Final = (
    "financial_facts_raw",
    "ownership_insiders_observations",
    "ownership_institutions_observations",
    "ownership_blockholders_observations",
    "ownership_treasury_observations",
    "ownership_def14a_observations",
    "ownership_funds_observations",
    "ownership_esop_observations",
    "finra_short_interest_observations",
    "research_price_series",
    "research_price_daily",
    "research_price_quarantine_coverage",
    "instrument_universe_membership",
    "instrument_symbol_history",
    "external_identifiers",
)
_DATE_COLUMNS: Final = (
    "filed_date",
    "filed_at",
    "period_end",
    "period_end_date",
    "ingested_at",
    "known_from",
    "known_to",
    "settlement_date",
    "bar_date",
    "first_bar",
    "last_bar",
    "evaluated_at",
    "effective_from",
    "effective_to",
    "created_at",
    "last_verified_at",
)


def collect_censuses(conn: psycopg.Connection[Any]) -> dict[str, object]:
    censuses: dict[str, object] = {}
    for table in _CENSUS_TABLES:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",
            (table,),
        ).fetchall()
        columns = {str(row[0]) for row in rows}
        if not columns:
            raise RuntimeError(f"census table {table} is absent")
        select: list[sql.Composable] = [sql.SQL("count(*) AS row_count")]
        if "instrument_id" in columns:
            select.append(sql.SQL("count(DISTINCT instrument_id) AS distinct_instruments"))
        for column in _DATE_COLUMNS:
            if column in columns:
                select.extend(
                    (
                        sql.SQL("min({column}) AS {alias}").format(
                            column=sql.Identifier(column), alias=sql.Identifier(f"min_{column}")
                        ),
                        sql.SQL("max({column}) AS {alias}").format(
                            column=sql.Identifier(column), alias=sql.Identifier(f"max_{column}")
                        ),
                    )
                )
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                sql.SQL("SELECT {columns} FROM {table}").format(
                    columns=sql.SQL(", ").join(select), table=sql.Identifier(table)
                )
            )
            result = cur.fetchone()
        if result is None:
            raise RuntimeError(f"census for {table} returned no aggregate row")
        censuses[table] = {key: _canonical(value) for key, value in result.items()}
    return censuses


def _registry_json() -> dict[str, object]:
    return {
        family.value: {
            "status": FIELD_REGISTRY[family].status,
            "reason": FIELD_REGISTRY[family].reason,
            "conditions": {condition: asdict(cell) for condition, cell in sorted(PROBE_MATRIX[family].items())},
        }
        for family in sorted(RankingFamily, key=lambda item: item.value)
    }


def _git(*args: str) -> str:
    return subprocess.run(("git", *args), check=True, text=True, capture_output=True).stdout.strip()


def collect_evidence() -> Evidence:
    assert_dev_environment()
    measured = hashlib.sha256(DECLARATION_PATH.read_bytes()).hexdigest()
    if measured != DECLARATION_SHA256:
        raise RuntimeError(f"declaration hash moved: expected {DECLARATION_SHA256}, measured {measured}")
    measured_correction = hashlib.sha256(CORRECTION_PATH.read_bytes()).hexdigest()
    if measured_correction != CORRECTION_SHA256:
        raise RuntimeError(f"correction hash moved: expected {CORRECTION_SHA256}, measured {measured_correction}")
    measured_correction_2 = hashlib.sha256(CORRECTION_2_PATH.read_bytes()).hexdigest()
    if measured_correction_2 != CORRECTION_2_SHA256:
        raise RuntimeError(f"correction-2 hash moved: expected {CORRECTION_2_SHA256}, measured {measured_correction_2}")
    measured_correction_3 = hashlib.sha256(CORRECTION_3_PATH.read_bytes()).hexdigest()
    if measured_correction_3 != CORRECTION_3_SHA256:
        raise RuntimeError(f"correction-3 hash moved: expected {CORRECTION_3_SHA256}, measured {measured_correction_3}")
    if _git("status", "--porcelain"):
        raise RuntimeError("verifier requires a clean worktree")
    execution_commit = _git("rev-parse", "HEAD")

    mutation: MutationEvidence | None = None
    try:
        with psycopg.connect(settings.database_url) as conn:
            conn.autocommit = False
            try:
                probes = run_source_probes(conn)
                censuses = collect_censuses(conn)
                mutation = run_mutation_test(conn)
            finally:
                conn.rollback()
    finally:
        if mutation is not None:
            with psycopg.connect(settings.database_url) as check_conn:
                remaining = check_conn.execute(
                    "SELECT count(*) FROM ownership_institutions_observations WHERE filer_cik=%s "
                    "AND source_document_id IN (%s, %s)",
                    (SENTINEL_CIK, SENTINEL_DOCUMENT, POST_DOCUMENT),
                ).fetchone()
            if remaining is None or int(remaining[0]) != 0:
                raise RuntimeError("rollback proof failed: #2900 sentinel remains")
            mutation = MutationEvidence(**{**asdict(mutation), "rollback_proved": True})

    if mutation is None:
        raise RuntimeError("mutation test did not produce evidence")
    verdict = derive_verdict(probes)
    return Evidence(
        schema_version=SCHEMA_VERSION,
        execution_commit=execution_commit,
        declaration_commit=DECLARATION_COMMIT,
        declaration_sha256=DECLARATION_SHA256,
        correction_commit=CORRECTION_COMMIT,
        correction_sha256=CORRECTION_SHA256,
        correction_2_commit=CORRECTION_2_COMMIT,
        correction_2_sha256=CORRECTION_2_SHA256,
        correction_3_commit=CORRECTION_3_COMMIT,
        correction_3_sha256=CORRECTION_3_SHA256,
        decision_date=DECISION_DATE.isoformat(),
        registry_version=REGISTRY_VERSION,
        registry=_registry_json(),
        probes=probes,
        censuses=censuses,
        mutation=mutation,
        verdict=verdict,
    )


def render_json(evidence: Evidence) -> str:
    return json.dumps(asdict(evidence), sort_keys=True, separators=(",", ":"), default=_canonical)


def _lines(items: Iterable[str]) -> str:
    return "\n".join(items)


def render_markdown(evidence: Evidence) -> str:
    mutation = evidence.mutation
    census_lines: list[str] = []
    for table, raw_values in evidence.censuses.items():
        if not isinstance(raw_values, Mapping):
            raise TypeError(f"census {table} is not a mapping")
        details = ", ".join(f"`{key}`={value}" for key, value in raw_values.items())
        census_lines.append(f"- `{table}`: {details}")
    return _lines(
        (
            "# R6 point-in-time spine result (#2900)",
            "",
            f"Verdict: **{evidence.verdict}**",
            "",
            f"Declaration SHA-256: `{evidence.declaration_sha256}` at commit `{evidence.declaration_commit}`.",
            f"Correction-1 SHA-256: `{evidence.correction_sha256}` at commit `{evidence.correction_commit}`.",
            f"Correction-2 SHA-256: `{evidence.correction_2_sha256}` at commit `{evidence.correction_2_commit}`.",
            f"Correction-3 SHA-256: `{evidence.correction_3_sha256}` at commit `{evidence.correction_3_commit}`.",
            f"Execution commit: `{evidence.execution_commit}`. Registry: `{evidence.registry_version}`.",
            "Reproduce: `PYTHONPATH=. uv run python -m scripts.verify_2900_point_in_time --format markdown`",
            "",
            "## Adversarial leak test",
            "",
            f"- Decision date: `{evidence.decision_date}` (NYSE session).",
            f"- Baseline hash: `{mutation.before_sha256}`.",
            f"- Post-decision insert hash: `{mutation.postdated_control_sha256}` (unchanged).",
            f"- Same-key pre-decision overwrite hash: `{mutation.overwritten_sha256}` (changed).",
            f"- First unequal field: `{mutation.first_unequal_column}`: "
            f"`{mutation.before_value}` → `{mutation.overwritten_value}`.",
            "- Rows recoverable at the old `ingested_at` after overwrite: "
            f"{mutation.old_vintage_rows_after_overwrite}.",
            f"- Rollback proved on a new connection: `{str(mutation.rollback_proved).lower()}`.",
            "",
            "## Contract evidence",
            "",
            f"- Successful non-vacuous probes: {', '.join(probe.probe_id for probe in evidence.probes)}.",
            f"- Refused registry families: {', '.join(sorted(evidence.registry))}.",
            f"- Exact census tables: {', '.join(evidence.censuses)}.",
            "- Full probe anchors, source hashes, registry matrix and census values are emitted by `--format json`.",
            "",
            "## Population census",
            "",
            *census_lines,
            "",
            "## Consequence",
            "",
            "The public-date filter correctly ignores a genuinely later filing, but the production writer overwrites a "
            "pre-decision natural key and the prior bytes cannot be recovered. The other source families fail at least "
            "one independently probed public-clock, system-version, population/identity or causal-transform condition.",
            "",
            "There is no admissible historical ranking field under the current contracts. Tier 2 arms are blocked and "
            "remain unmeasured; no return, haircut, cost or benchmark claim was produced.",
            "",
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=("json", "markdown"), required=True)
    args = parser.parse_args()
    evidence = collect_evidence()
    print(render_json(evidence) if args.format == "json" else render_markdown(evidence))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
