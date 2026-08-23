from __future__ import annotations

import json

import psycopg
import pytest

from scripts.verify_2900_point_in_time import (
    _DATE_COLUMNS,
    POST_DOCUMENT,
    SENTINEL_CIK,
    SENTINEL_DOCUMENT,
    _semantic_anchor_count,
    _semantic_text,
    derive_verdict,
    render_json,
    render_markdown,
    run_mutation_test,
    run_source_probes,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def test_census_includes_both_system_version_bounds() -> None:
    assert "known_from" in _DATE_COLUMNS
    assert "known_to" in _DATE_COLUMNS


def test_source_probe_text_excludes_comments_docstrings_and_dead_blocks(tmp_path) -> None:
    specimen = tmp_path / "probe_specimen.py"
    specimen.write_text(
        '"""DOCSTRING_NEEDLE"""\n# COMMENT_NEEDLE\nif False:\n    DEAD_NEEDLE = True\nLIVE_NEEDLE = "reachable"\n'
    )
    text = _semantic_text(str(specimen))
    assert "DOCSTRING_NEEDLE" not in text
    assert "COMMENT_NEEDLE" not in text
    assert "DEAD_NEEDLE" not in text
    assert "LIVE_NEEDLE" in text
    assert _semantic_anchor_count(text, 'LIVE_NEEDLE   =   "reachable"') == 1
    assert _semantic_anchor_count("LEFT_HALF = 1\nRIGHT_HALF = 2\n", "LEFT_HALF = 1 RIGHT_HALF") == 0


def _seed_instrument(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (2900001, 'R6PIT', 'R6 PIT Test', '4', 'USD', TRUE)
        """
    )


def test_declared_probes_are_non_vacuous_and_derive_fail_verdict(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    probes = run_source_probes(ebull_test_conn)
    assert len(probes) == 25
    assert all(probe.passed and probe.anchor_counts and probe.source_sha256 for probe in probes)
    assert derive_verdict(probes) == "FAIL — NO ADMISSIBLE HISTORICAL FIELD"


def test_rollback_mutation_proves_public_clock_and_lost_system_version(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn)
    evidence = run_mutation_test(ebull_test_conn)
    assert evidence.instrument_id == 2900001
    assert evidence.before_sha256 == evidence.postdated_control_sha256
    assert evidence.overwritten_sha256 != evidence.before_sha256
    assert evidence.old_vintage_rows_after_overwrite == 0
    assert evidence.first_unequal_column in {"ingest_run_id", "shares", "ingested_at"}

    ebull_test_conn.rollback()
    remaining = ebull_test_conn.execute(
        "SELECT count(*) FROM ownership_institutions_observations WHERE filer_cik=%s "
        "AND source_document_id IN (%s, %s)",
        (SENTINEL_CIK, SENTINEL_DOCUMENT, POST_DOCUMENT),
    ).fetchone()
    assert remaining is not None and int(remaining[0]) == 0


def test_renderers_share_one_typed_evidence_schema() -> None:
    from scripts.verify_2900_point_in_time import Evidence, MutationEvidence, ProbeResult

    evidence = Evidence(
        schema_version="test",
        execution_commit="a" * 40,
        declaration_commit="b" * 40,
        declaration_sha256="c" * 64,
        correction_commit="d" * 40,
        correction_sha256="e" * 64,
        correction_2_commit="1" * 40,
        correction_2_sha256="2" * 64,
        correction_3_commit="3" * 40,
        correction_3_sha256="4" * 64,
        decision_date="2020-01-15",
        registry_version="test-registry",
        registry={"family": {"status": "refused"}},
        probes=(ProbeResult("X", True, {"a": 1}, {"a": "d" * 64}, "ok"),),
        censuses={"table": {"row_count": 0, "min_known_to": None, "max_known_to": None}},
        mutation=MutationEvidence(1, "f" * 64, "f" * 64, "0" * 64, 0, "shares", "100", "101", True),
        verdict="FAIL — NO ADMISSIBLE HISTORICAL FIELD",
    )
    payload = json.loads(render_json(evidence))
    assert payload["schema_version"] == "test"
    markdown = render_markdown(evidence)
    assert "FAIL — NO ADMISSIBLE HISTORICAL FIELD" in markdown
    assert "`100` → `101`" in markdown
    assert "`min_known_to`=None" in markdown
