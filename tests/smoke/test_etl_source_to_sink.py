"""Per-source end-to-end smoke test.

For EVERY data source eBull consumes, this test asserts the wiring
chain from provider → manifest → parser → table → endpoint exists at
import-time + has a matching per-source spec file at
``docs/etl/sources/<source>.md``.

This is the integrity floor. A source missing wiring (e.g. dropped
from ``ManifestSource`` Literal but still referenced in code; or has
spec file but no parser registered) fails the test loudly.

What this test does NOT cover:
* Live HTTP fetches against SEC / FINRA / eToro (use the live-smoke
  runbooks under ``app/runbooks/`` for that).
* Operator-visible figures against a known instrument (cross-source
  validation lives in the verification queries section of each per-
  source spec file; operator runs them).

See ``docs/etl/sources/README.md`` for the full template + invariants.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, get_args

import psycopg
import pytest

from app.services.sec_manifest import FORM_MAPPING_EXEMPT
from app.services.sec_manifest import ManifestSource as _ManifestSource
from scripts._etl_source_inventory import (
    AD_HOC_SOURCES as _AD_HOC_SOURCES,
)
from scripts._etl_source_inventory import (
    ALL_SOURCES as _ALL_SOURCES,
)
from scripts._etl_source_inventory import (
    MANIFEST_SOURCE_SINKS,
)
from scripts._etl_source_inventory import (
    MANIFEST_SOURCES as _MANIFEST_SOURCES,
)
from scripts._etl_source_inventory import (
    REQUIRED_SECTIONS as _REQUIRED_SECTIONS,
)

_DOCS_DIR: Path = Path(__file__).resolve().parents[2] / "docs" / "etl" / "sources"


@pytest.mark.parametrize("source", _ALL_SOURCES)
def test_source_has_spec_file(source: str) -> None:
    """Every source MUST have a per-source spec file at
    ``docs/etl/sources/<source>.md``.

    Drift symptom: a maintainer adds a source to ``ManifestSource``
    but forgets the spec file → operator can't find the wiring
    contract → repeats the Stage A→F sweep work.
    """
    spec_path = _DOCS_DIR / f"{source}.md"
    assert spec_path.is_file(), (
        f"Missing per-source spec at {spec_path}. "
        f"Required by docs/etl/sources/README.md § Template — "
        f"every source has 13 sections covering origin → endpoint."
    )


@pytest.mark.parametrize("source", _ALL_SOURCES)
def test_source_spec_has_required_sections(source: str) -> None:
    """Each per-source spec file MUST contain the 13 required
    section headers from the README template.
    """
    spec_path = _DOCS_DIR / f"{source}.md"
    if not spec_path.is_file():
        pytest.skip("spec file missing — covered by test_source_has_spec_file")
    body = spec_path.read_text()
    missing = [h for h in _REQUIRED_SECTIONS if h not in body]
    assert not missing, (
        f"docs/etl/sources/{source}.md missing required section(s): {missing}. "
        f"Template at docs/etl/sources/README.md § Template."
    )


@pytest.mark.parametrize("source", _AD_HOC_SOURCES)
def test_ad_hoc_source_has_architectural_exception_section(source: str) -> None:
    """Ad-hoc sources (currently only sec_n_cen) MUST document the
    bypass in a ``## 0. Architectural exception`` section so a future
    agent reading the file sees the deliberate-vs-oversight signal.
    """
    spec_path = _DOCS_DIR / f"{source}.md"
    if not spec_path.is_file():
        pytest.skip("spec file missing — covered by test_source_has_spec_file")
    body = spec_path.read_text()
    # Prefix-match so authors can append a qualifier like "— READ FIRST".
    assert any(line.startswith("## 0. Architectural exception") for line in body.splitlines()), (
        f"docs/etl/sources/{source}.md must include a section "
        f"starting '## 0. Architectural exception' explaining the ManifestSource bypass. "
        f"Required by README § cross-cutting invariants #2."
    )


@pytest.mark.parametrize("source", _MANIFEST_SOURCES)
def test_manifest_source_has_registered_parser(source: str) -> None:
    """Every ManifestSource entry MUST have a registered parser in
    the manifest worker's parser registry.

    The two synth no-op parsers (sec_10q + sec_xbrl_facts) still
    count — they register a callable that returns
    ParseOutcome(status='parsed', parser_version='*-noop-v1').
    """
    from app.jobs.sec_manifest_worker import registered_parser_sources

    registered = set(registered_parser_sources())
    assert source in registered, (
        f"ManifestSource '{source}' has no registered parser. "
        f"Either register one in app/services/manifest_parsers/ "
        f"(see register_all_parsers in app/services/manifest_parsers/__init__.py) "
        f"or remove '{source}' from the ManifestSource Literal."
    )


# `FORM_MAPPING_EXEMPT` lives at app/services/sec_manifest.py
# (production is the authoritative source post Architect IMP-2 fold).
# Imported at top of file.


@pytest.mark.parametrize("source", _MANIFEST_SOURCES)
def test_manifest_source_form_mapping_present(source: str) -> None:
    """Every ManifestSource (except the exempt list) MUST appear in
    ``_FORM_TO_SOURCE`` so the fast-lane Atom feed + daily-index
    reconcile can route filings to the right manifest source.
    Layer 1/2/3 + Layer 4 all consult this dispatch table.
    Exempt list lives at ``app.services.sec_manifest.FORM_MAPPING_EXEMPT``.
    """
    if source in FORM_MAPPING_EXEMPT:
        pytest.skip(f"'{source}' is exempt — not discovered via SEC form type")
    from app.services.sec_manifest import _FORM_TO_SOURCE

    mapped_sources = set(_FORM_TO_SOURCE.values())
    assert source in mapped_sources, (
        f"ManifestSource '{source}' has no entry in _FORM_TO_SOURCE. "
        f"Either add at least one form_type → '{source}' mapping at "
        f"app/services/sec_manifest.py:860-918, or remove '{source}' from "
        f"the ManifestSource Literal, or add to _FORM_MAPPING_EXEMPT if it "
        f"genuinely doesn't go through form-type discovery (and document "
        f"why in docs/etl/sources/<source>.md §6)."
    )


def test_readme_section_count_matches_required_sections() -> None:
    """README §Maintenance bullet 2 says "the N required sections".
    N MUST equal ``len(REQUIRED_SECTIONS)`` from the inventory. Bot
    iter 1 PREVENTION fold — prevents the 11-vs-13 doc drift that
    landed in v1 of this PR.
    """
    readme = (_DOCS_DIR / "README.md").read_text()
    expected = f"the {len(_REQUIRED_SECTIONS)} required sections"
    assert expected in readme, (
        f"README.md §Maintenance bullet 2 must mention '{expected}' to "
        f"stay in sync with REQUIRED_SECTIONS ({len(_REQUIRED_SECTIONS)} "
        f"entries in scripts/_etl_source_inventory.py)."
    )


@pytest.mark.parametrize("source", _MANIFEST_SOURCES)
def test_manifest_source_has_freshness_cadence(source: str) -> None:
    """Every ManifestSource MUST have a ``_CADENCE`` entry at
    ``app/services/data_freshness.py:69-100`` so the per-CIK poll
    can compute ``expected_next_at``. Missing entry → freshness
    index stops driving polls for that source.
    """
    from app.services.data_freshness import _CADENCE

    assert source in _CADENCE, (
        f"ManifestSource '{source}' missing from data_freshness._CADENCE. "
        f"Add a row at app/services/data_freshness.py:69-100. Without it, "
        f"seed_freshness_for_manifest_row cannot populate expected_next_at "
        f"and subjects_due_for_poll will never surface this source."
    )


# ---------------------------------------------------------------------------
# #1322 — manifest source → sink table smoke (per Phase 0 §2.3)
# ---------------------------------------------------------------------------


def _table_exists(conn: psycopg.Connection[Any], table_name: str) -> bool:
    """Existence check via pg_tables — avoids the UndefinedTable
    aborted-tx hazard of ``SELECT 1 FROM <table>`` (Codex iter-2
    BLOCKING fold)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s",
            (table_name,),
        )
        return cur.fetchone() is not None


def test_manifest_source_sinks_complete() -> None:
    """#1322 — closure test: every ManifestSource entry MUST have a sink declaration.

    A future Literal addition (e.g. sec_form6 lands) without a corresponding
    MANIFEST_SOURCE_SINKS entry fails this test loudly. Catches drift between
    the type contract (ManifestSource) and the sink mapping (used by all
    downstream coverage tests). Codex iter-2 IMPORTANT-1 fold.
    """
    declared = set(MANIFEST_SOURCE_SINKS.keys())
    actual = set(get_args(_ManifestSource))
    only_declared = declared - actual
    only_actual = actual - declared
    assert declared == actual, (
        f"MANIFEST_SOURCE_SINKS drift vs ManifestSource Literal:\n"
        f"  only in MANIFEST_SOURCE_SINKS (remove): {sorted(only_declared)}\n"
        f"  only in ManifestSource (add a sink): {sorted(only_actual)}\n"
        f"Update scripts/_etl_source_inventory.py::MANIFEST_SOURCE_SINKS."
    )


_PARSER_MODULE_BY_SOURCE: dict[str, str] = {
    "sec_form3": "app.services.manifest_parsers.insider_345",
    "sec_form4": "app.services.manifest_parsers.insider_345",
    "sec_form5": "app.services.manifest_parsers.insider_345",
    "sec_13d": "app.services.manifest_parsers.sec_13dg",
    "sec_13g": "app.services.manifest_parsers.sec_13dg",
    "sec_13f_hr": "app.services.manifest_parsers.sec_13f_hr",
    "sec_def14a": "app.services.manifest_parsers.def14a",
    "sec_n_port": "app.services.manifest_parsers.sec_n_port",
    "sec_n_csr": "app.services.manifest_parsers.sec_n_csr",
    "sec_10k": "app.services.manifest_parsers.sec_10k",
    "sec_10q": "app.services.manifest_parsers.sec_10q",
    "sec_8k": "app.services.manifest_parsers.eight_k",
    "sec_xbrl_facts": "app.services.manifest_parsers.sec_xbrl_facts",
    "finra_short_interest": "app.services.manifest_parsers.finra_short_interest",
    "finra_regsho_daily": "app.services.manifest_parsers.finra_regsho_daily",
    "sec_nt": "app.services.manifest_parsers.sec_nt",
    "sec_pre14a": "app.services.manifest_parsers.sec_pre14a",
}

# #1322 PR #1354 bot iter-1 WARNING fold — parallel-dict drift guard.
# Module-level closure assertion catches the "new source landed in
# MANIFEST_SOURCE_SINKS but forgot to add a parser module entry here"
# regression at COLLECTION time, not at parametrize-iteration time.
# Mirrors the import-time check in scripts/_etl_source_inventory.py.
assert set(_PARSER_MODULE_BY_SOURCE) == set(MANIFEST_SOURCE_SINKS), (
    f"_PARSER_MODULE_BY_SOURCE drift vs MANIFEST_SOURCE_SINKS:\n"
    f"  only in _PARSER_MODULE_BY_SOURCE: "
    f"{sorted(set(_PARSER_MODULE_BY_SOURCE) - set(MANIFEST_SOURCE_SINKS))}\n"
    f"  only in MANIFEST_SOURCE_SINKS: "
    f"{sorted(set(MANIFEST_SOURCE_SINKS) - set(_PARSER_MODULE_BY_SOURCE))}\n"
    f"Update _PARSER_MODULE_BY_SOURCE in tests/smoke/test_etl_source_to_sink.py."
)


@pytest.mark.parametrize("source,spec", sorted(MANIFEST_SOURCE_SINKS.items()))
def test_manifest_source_has_sink_tables(
    source: str,
    spec: tuple[tuple[str, ...], str],
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#1322 — every ManifestSource entry's declared sink tables MUST exist
    in the DB; synth-noop sources MUST declare _SYNTH_NOOP=True in their
    parser module (bidirectional parity per Codex iter-3 IMPORTANT fold).

    Uses ``ebull_test_conn`` (worker-private DB per CLAUDE.md test isolation)
    NOT ``settings.database_url`` — pg_tables read is harmless but the
    convention is to never reach for dev DB from a test.
    """
    target_tables, kind = spec

    # Sink-table existence check (read-only via pg_tables)
    missing = [t for t in target_tables if not _table_exists(ebull_test_conn, t)]
    assert not missing, (
        f"Source {source!r} (kind={kind!r}) declares sink tables in "
        f"MANIFEST_SOURCE_SINKS that don't exist in DB: {missing}. "
        f"Either add the migration, or update MANIFEST_SOURCE_SINKS to "
        f"reflect the real shape."
    )

    # Synth-noop parity: bidirectional flag check
    module_path = _PARSER_MODULE_BY_SOURCE.get(source)
    if module_path is None:
        pytest.fail(
            f"Source {source!r} missing _PARSER_MODULE_BY_SOURCE entry — "
            f"update the dict in this test file when adding a new source."
        )
    parser_module = importlib.import_module(module_path)
    flag = getattr(parser_module, "_SYNTH_NOOP", False)
    expected = kind == "synth_noop"
    assert flag is expected, (
        f"Source {source!r} parity mismatch: kind={kind!r} implies "
        f"_SYNTH_NOOP={expected}, but {module_path}._SYNTH_NOOP={flag}. "
        f"Either flip the module flag OR update MANIFEST_SOURCE_SINKS kind."
    )


def test_categories_match_ownership_writers(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#1322 — every _CATEGORIES entry must have:
    (a) a callable refresh_fn (the lambda in the tuple)
    (b) current_table exists in DB
    (c) observations_table exists in DB
    (d) corresponding refresh_<category>_current importable from
        app.services.ownership_observations namespace

    Direct iteration over _CATEGORIES (multi-agent B2 + Codex iter-1
    BLOCKING-2 fold — no string templating). Uses ``ebull_test_conn``
    per CLAUDE.md test isolation.
    """
    from app.jobs.ownership_observations_repair import _CATEGORIES
    from app.services import ownership_observations

    for current_table, observations_table, category_literal, refresh_batch_fn, refresh_one_fn in _CATEGORIES:
        assert callable(refresh_batch_fn), (
            f"_CATEGORIES entry for {category_literal!r}: refresh_batch_fn is not callable"
        )
        assert callable(refresh_one_fn), f"_CATEGORIES entry for {category_literal!r}: refresh_one_fn is not callable"
        assert _table_exists(ebull_test_conn, current_table), (
            f"_CATEGORIES entry for {category_literal!r}: current_table {current_table!r} does not exist in DB"
        )
        assert _table_exists(ebull_test_conn, observations_table), (
            f"_CATEGORIES entry for {category_literal!r}: observations_table "
            f"{observations_table!r} does not exist in DB"
        )
        # #1345 PR-B: each category carries both the per-instrument writer
        # (fallback) and its whole-set batch writer (happy path).
        for expected_fn_name in (f"refresh_{category_literal}_current", f"refresh_{category_literal}_current_batch"):
            assert hasattr(ownership_observations, expected_fn_name), (
                f"_CATEGORIES entry for {category_literal!r}: "
                f"app.services.ownership_observations is missing "
                f"function {expected_fn_name!r}. Either rename the function "
                f"or update _CATEGORIES."
            )
