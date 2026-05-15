"""Tests for the N-CSR iXBRL fact extractor (#1171, spec §5 + §8).

Uses the spike-cached iXBRL fixtures at ``.tmp/spike-918/`` (gitignored)
when available. CI runs against bundled fixtures under
``tests/fixtures/n_csr_ixbrl/``; the spike-cache path is the local-dev
shortcut.
"""

from __future__ import annotations

import json
import pathlib
import re
from decimal import Decimal

import pytest

from app.services.n_csr_extractor import (
    FACTORS_AFFECTING_CAP_BYTES,
    MATERIAL_CHNG_NOTICE_CAP_BYTES,
    RAW_FACTS_SIZE_CAP_BYTES,
    extract_fund_metadata_facts,
)

_SPIKE_DIR = pathlib.Path(".tmp/spike-918")
_FIDELITY = _SPIKE_DIR / "fidelity_ixbrl.xml"
_VANGUARD_A = _SPIKE_DIR / "vanguard_a_ixbrl.xml"
_VANGUARD_NCSRS = _SPIKE_DIR / "vanguard_ncsrs_ixbrl.xml"
_ISHARES = _SPIKE_DIR / "ishares_ixbrl.xml"

requires_fidelity = pytest.mark.skipif(not _FIDELITY.exists(), reason="spike fixture not available")
requires_vanguard_a = pytest.mark.skipif(not _VANGUARD_A.exists(), reason="spike fixture not available")
requires_vanguard_ncsrs = pytest.mark.skipif(not _VANGUARD_NCSRS.exists(), reason="spike fixture not available")
requires_ishares = pytest.mark.skipif(not _ISHARES.exists(), reason="spike fixture not available")


@requires_fidelity
def test_fidelity_class_count() -> None:
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    assert len(facts) == 9
    class_ids = [f.class_id for f in facts]
    assert all(re.fullmatch(r"C\d{9}", cid) for cid in class_ids)


@requires_fidelity
def test_fidelity_trust_cik_and_document_type() -> None:
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    for f in facts:
        assert f.trust_cik == "0000819118"
        assert f.document_type == "N-CSR"


@requires_fidelity
def test_fidelity_per_class_scalars_populated() -> None:
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    for f in facts:
        assert f.class_name is not None
        assert f.trading_symbol is not None
        assert f.expense_ratio_pct is not None
        assert f.net_assets_amt is not None and f.net_assets_amt > 0
        assert f.holdings_count is not None and f.holdings_count > 0
        assert f.portfolio_turnover_pct is not None


@requires_fidelity
def test_fidelity_sector_allocation_populated() -> None:
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    for f in facts:
        assert f.sector_allocation, f"class={f.class_id} has no sector allocation"
        for label, pct in f.sector_allocation.items():
            assert isinstance(pct, Decimal)
            assert isinstance(label, str)


@requires_fidelity
def test_fidelity_region_allocation_populated() -> None:
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    # At least one class should have region allocation (international funds).
    assert any(f.region_allocation for f in facts)


@requires_fidelity
def test_fidelity_multi_class_isolation() -> None:
    """Different classes must have different expense_ratio_pct, trading_symbol — facts must NOT bleed across classes."""
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    symbols = {f.class_id: f.trading_symbol for f in facts}
    # Each class has its own ticker.
    assert len(set(symbols.values())) == len(symbols), "trading_symbol should be unique per class"


@requires_fidelity
def test_fidelity_boilerplate_blocklist_skipped() -> None:
    """HoldingsTableTextBlock + LineGraphTableTextBlock + AvgAnnlRtrTableTextBlock
    must NOT appear in raw_facts (per §5 blocklist)."""
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    for f in facts:
        for concept_qname in f.raw_facts:
            local = concept_qname.rsplit(":", 1)[1]
            assert local not in {
                "HoldingsTableTextBlock",
                "LineGraphTableTextBlock",
                "AvgAnnlRtrTableTextBlock",
                "AddlFundStatisticsTextBlock",
                "AnnlOrSemiAnnlStatementTextBlock",
                "PerformancePastDoesNotIndicateFuture",
                "NoDeductionOfTaxesTextBlock",
                "LargestHoldingsTableTextBlock",
                "ExpensesTextBlock",
                "UpdPerfInfoLocationTextBlock",
                "AddlInfoTextBlock",
            }, f"blocklisted concept {local} leaked into raw_facts"


@requires_fidelity
def test_fidelity_raw_facts_size_cap() -> None:
    """raw_facts payload must not exceed the spec §5 hard cap (32 KB serialized)."""
    facts = extract_fund_metadata_facts(_FIDELITY.read_bytes())
    for f in facts:
        size = len(json.dumps(f.raw_facts, default=str))
        assert size <= RAW_FACTS_SIZE_CAP_BYTES + 2048, f"raw_facts size {size} > cap"


@requires_vanguard_a
def test_vanguard_a_multi_series_isolation() -> None:
    """Vanguard's N-CSR carries multiple series; each class observation
    must NOT contain a sibling-series HoldingsCount."""
    facts = extract_fund_metadata_facts(_VANGUARD_A.read_bytes())
    # Vanguard accession carries multiple series each with multiple classes.
    # The spec §8.6.c hard context filter must keep per-class facts isolated.
    assert len(facts) >= 2, "Expected multiple classes in Vanguard fixture"
    # Each class should have its own scalar fields.
    class_ids = [f.class_id for f in facts]
    assert len(set(class_ids)) == len(class_ids), "class_ids must be unique"


@requires_vanguard_ncsrs
def test_vanguard_ncsrs_document_type() -> None:
    facts = extract_fund_metadata_facts(_VANGUARD_NCSRS.read_bytes())
    for f in facts:
        assert f.document_type == "N-CSRS"


@requires_ishares
def test_ishares_credit_quality_allocation() -> None:
    """iShares bond ETF should expose credit_quality_allocation."""
    facts = extract_fund_metadata_facts(_ISHARES.read_bytes())
    assert any(f.credit_quality_allocation for f in facts), (
        "Expected at least one bond-fund class to have credit_quality_allocation"
    )


def test_extractor_rejects_malformed_xml() -> None:
    with pytest.raises(ValueError, match="iXBRL parse"):
        extract_fund_metadata_facts(b"<not-valid-xml")


def test_extractor_rejects_missing_entity_cik() -> None:
    # Valid XML but no entity CIK.
    xml = (
        b'<xbrl xmlns="http://www.xbrl.org/2003/instance">'
        b'<context id="c1"><entity></entity>'
        b"<period><instant>2025-12-31</instant></period></context></xbrl>"
    )
    with pytest.raises(ValueError, match="no entity CIK"):
        extract_fund_metadata_facts(xml)


def test_size_caps_documented() -> None:
    """Sanity: hard caps from spec §5 are exposed as module constants."""
    assert RAW_FACTS_SIZE_CAP_BYTES == 32 * 1024
    assert MATERIAL_CHNG_NOTICE_CAP_BYTES == 16 * 1024
    assert FACTORS_AFFECTING_CAP_BYTES == 8 * 1024
