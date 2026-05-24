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

from pathlib import Path

import pytest

from app.services.sec_manifest import FORM_MAPPING_EXEMPT
from scripts._etl_source_inventory import (
    AD_HOC_SOURCES as _AD_HOC_SOURCES,
)
from scripts._etl_source_inventory import (
    ALL_SOURCES as _ALL_SOURCES,
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
