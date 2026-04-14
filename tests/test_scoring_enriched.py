"""Tests for enhanced _value_score with fundamentals fallback."""

from __future__ import annotations

import pytest

from app.services.scoring import _value_score


class TestValueScoreEnriched:
    def test_value_score_thesis_based_unchanged(self) -> None:
        """Thesis path dominates when base_value is present.

        base_value=150, bear_value=80, current_price=100
        upside=(150-100)/100=0.5, upside_score=clip(0.5/0.5)=1.0
        downside=(100-80)/100=0.2, downside_penalty=clip(0.2/0.5)=0.4
        score=0.75*1.0 + 0.25*(1-0.4) = 0.75+0.15 = 0.90
        """
        score, notes = _value_score(
            base_value=150.0,
            bear_value=80.0,
            current_price=100.0,
        )
        assert score == pytest.approx(0.90)
        assert "fundamentals fallback" not in " ".join(notes)

    def test_value_score_thesis_overrides_fundamentals(self) -> None:
        """When both thesis AND fundamentals data exist, thesis path is used."""
        score, notes = _value_score(
            base_value=150.0,
            bear_value=80.0,
            current_price=100.0,
            pe_ratio=10.0,
            fcf_yield=0.08,
            price_target_mean=200.0,
        )
        # Thesis path: score = 0.90 (same as above, enrichment params ignored)
        assert score == pytest.approx(0.90)
        assert "fundamentals fallback" not in " ".join(notes)

    def test_value_score_fundamentals_all_three_signals(self) -> None:
        """Fallback with all three signals available.

        pe_ratio=10 → pe_score=clip(1-(10-10)/40)=1.0 (w=0.35)
        fcf_yield=0.08 → fy_score=clip(0.08/0.08)=1.0 (w=0.35)
        price_target_mean=150, current_price=100 → pt_upside=0.5, pt_score=clip(0.5/0.5)=1.0 (w=0.30)
        total_weight=1.0, score=1.0
        """
        score, notes = _value_score(
            base_value=None,
            bear_value=None,
            current_price=100.0,
            pe_ratio=10.0,
            fcf_yield=0.08,
            price_target_mean=150.0,
        )
        assert score == pytest.approx(1.0)
        assert any("fundamentals fallback" in n for n in notes)

    def test_value_score_fundamentals_expensive_stock(self) -> None:
        """Fallback with all three signals pointing to expensive / weak stock.

        pe_ratio=50 → pe_score=clip(1-(50-10)/40)=clip(0.0)=0.0 (w=0.35)
        fcf_yield=0.01 → fy_score=clip(0.01/0.08)=0.125 (w=0.35)
        price_target_mean=90, current_price=100 → pt_upside=-0.1, pt_score=clip(-0.1/0.5)=0.0 (w=0.30)
        total_weight=1.0, score=(0*0.35 + 0.125*0.35 + 0*0.30)/1.0=0.04375
        """
        score, notes = _value_score(
            base_value=None,
            bear_value=None,
            current_price=100.0,
            pe_ratio=50.0,
            fcf_yield=0.01,
            price_target_mean=90.0,
        )
        assert score == pytest.approx(0.04375)
        assert any("fundamentals fallback" in n for n in notes)

    def test_value_score_pe_only(self) -> None:
        """Fallback with only P/E available; re-normalised to weight 1.0.

        pe_ratio=15 → pe_score=clip(1-(15-10)/40)=clip(0.875)=0.875
        only one component, renormalized weight=1.0; score=0.875
        """
        score, notes = _value_score(
            base_value=None,
            bear_value=None,
            current_price=100.0,
            pe_ratio=15.0,
        )
        assert score == pytest.approx(0.875)
        assert any("fundamentals fallback" in n for n in notes)

    def test_value_score_no_thesis_no_fundamentals(self) -> None:
        """All None → neutral 0.5."""
        score, notes = _value_score(
            base_value=None,
            bear_value=None,
            current_price=100.0,
        )
        assert score == pytest.approx(0.5)
        assert any("fundamentals fallback" in n for n in notes)

    def test_value_score_no_price(self) -> None:
        """current_price=None → neutral 0.5 regardless of other params."""
        score, notes = _value_score(
            base_value=None,
            bear_value=None,
            current_price=None,
            pe_ratio=15.0,
            fcf_yield=0.06,
        )
        assert score == pytest.approx(0.5)
        assert any("current_price missing" in n for n in notes)

    def test_value_score_negative_fcf_yield(self) -> None:
        """Cash-burning company: fcf_yield=-0.05 → fy_score=clip(-0.05/0.08)=0.0."""
        score, notes = _value_score(
            base_value=None,
            bear_value=None,
            current_price=100.0,
            fcf_yield=-0.05,
        )
        # Only fcf_yield component, fy_score=0.0, renormalized weight=1.0 → score=0.0
        assert score == pytest.approx(0.0)
        assert any("fundamentals fallback" in n for n in notes)
