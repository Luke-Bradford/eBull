"""Regression test for the Soros/Geode CIK disambiguation (#790 P2,
migration 104, Batch 2 of #788).

Codex audit + SEC submissions.json verified that CIK 0001029160 is
SOROS FUND MANAGEMENT LLC, not Geode Capital Management. Migration
091's curated seed list mis-labelled the row as Geode and tagged it
ETF, routing every Soros position into the etfs slice on the
ownership card. Real Geode Capital Management LLC is CIK
0001214717 (verified via SEC EDGAR full-text search for 13F-HR).

Tests target the canonical seed sources (the migration SQL + the
``scripts/seed_holder_coverage`` constants) rather than the live DB
state — the per-test TRUNCATE in ``ebull_test_conn`` wipes the seed
rows between tests and would force every test to re-apply the
migration first. This file's job is "did the source-of-truth lists
get updated", which is decidable without round-tripping through the
DB.
"""

from __future__ import annotations

from pathlib import Path

from scripts.seed_holder_coverage import _ETF_OVERRIDES, _INSTITUTIONAL_SEEDS

_REAL_SOROS_CIK = "0001029160"
_REAL_GEODE_CIK = "0001214717"


def test_soros_cik_in_seed_list_labelled_soros() -> None:
    """The CIK that SEC says is Soros must be labelled Soros, not Geode."""
    by_cik = dict(_INSTITUTIONAL_SEEDS)
    label = by_cik.get(_REAL_SOROS_CIK)
    assert label is not None, (
        f"CIK {_REAL_SOROS_CIK} missing from _INSTITUTIONAL_SEEDS — "
        f"the curated list dropped Soros entirely (likely a bad merge)."
    )
    assert "Soros" in label, (
        f"CIK {_REAL_SOROS_CIK} mislabelled as {label!r}. "
        f"SEC submissions.json confirms this CIK is SOROS FUND "
        f"MANAGEMENT LLC, not Geode Capital Management."
    )
    assert "Geode" not in label


def test_soros_cik_not_in_etf_override_list() -> None:
    """Soros is a hedge fund, not an ETF issuer."""
    etf_ciks = {cik for cik, _label in _ETF_OVERRIDES}
    assert _REAL_SOROS_CIK not in etf_ciks, (
        f"CIK {_REAL_SOROS_CIK} (Soros) appears in _ETF_OVERRIDES; "
        f"every Soros position would route to the etfs slice on the "
        f"ownership card. Soros is a hedge fund — drop the override."
    )


def test_real_geode_cik_in_seed_list() -> None:
    by_cik = dict(_INSTITUTIONAL_SEEDS)
    label = by_cik.get(_REAL_GEODE_CIK)
    assert label is not None, f"Real Geode CIK {_REAL_GEODE_CIK} missing from _INSTITUTIONAL_SEEDS."
    assert "Geode" in label


def test_real_geode_cik_in_etf_override_list() -> None:
    etf_ciks = {cik for cik, _label in _ETF_OVERRIDES}
    assert _REAL_GEODE_CIK in etf_ciks, (
        f"Real Geode CIK {_REAL_GEODE_CIK} missing from _ETF_OVERRIDES — "
        f"Geode operates Fidelity's passive-index franchise and IS an "
        f"ETF issuer for the chart's filer_type split."
    )


def test_migration_104_present_and_addresses_both_ciks() -> None:
    """Pin the migration file's existence so a future cleanup that
    drops it (or moves the fix into an earlier migration) can't
    silently re-introduce the bug.

    Verifies the migration text references both CIKs and the
    canonical actions (Soros UPDATE label, Geode INSERT seed,
    Geode INSERT etf override, Soros DELETE etf override)."""
    migration = Path(__file__).resolve().parent.parent / "sql" / "104_fix_soros_geode_disambig.sql"
    assert migration.exists(), "migration 104 missing"
    text = migration.read_text(encoding="utf-8")
    assert _REAL_SOROS_CIK in text
    assert _REAL_GEODE_CIK in text
    assert "Soros Fund Management" in text
    assert "Geode Capital Management" in text
    assert "DELETE FROM etf_filer_cik_seeds" in text
    assert "INSERT INTO etf_filer_cik_seeds" in text
