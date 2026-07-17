"""Pure predicate tests for the standing thesis DQ audit (#2014).

Spec: docs/proposals/thesis/2026-07-15-thesis-dq-audit.md. The predicates
mirror #2007's `_validate_writer_output` semantics through the same
`_to_float` coercion chokepoint (NaN/±inf → None).
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.services.thesis_dq_audit import (
    _BLOCK_KEYWORDS,
    check_base_vs_anchor_close,
    check_ordering,
    check_stale_anchor,
    check_zone,
    claim_lint,
    classify_zoneless_buy,
    is_target_abstention,
)


class TestOrdering:
    @pytest.mark.parametrize(
        ("bear", "base", "bull", "violates"),
        [
            (10, 20, 30, False),
            (30, 20, 10, True),  # fully inverted
            (25, 20, 30, True),  # bear > base
            (10, 35, 30, True),  # base > bull
            (None, 20, 10, True),  # base > bull with bear absent
            (None, None, None, False),  # abstention is not an ordering violation
            ("nan", 20, 10, True),  # NaN coerces to None; base>bull still caught
            (float("inf"), 20, 30, False),  # inf coerces to None, drops out
            ("10", "20", "30", False),  # numeric strings coerce
        ],
    )
    def test_table(self, bear: object, base: object, bull: object, violates: bool) -> None:
        assert (check_ordering(bear, base, bull) is not None) is violates


class TestZone:
    def test_inverted(self) -> None:
        assert check_zone(50, 40) is not None

    def test_ordered_and_nulls(self) -> None:
        assert check_zone(40, 50) is None
        assert check_zone(None, 50) is None
        assert check_zone("nan", 40) is None


class TestZonelessBuy:
    def test_violation_when_anchor_available(self) -> None:
        assert classify_zoneless_buy("buy", None, None, True) == "zoneless_buy"

    def test_info_when_anchor_unavailable(self) -> None:
        """Writer prompt documents null zones when price_anchor is null —
        anchor-less buys are exempt (Codex ckpt-1 HIGH)."""
        assert classify_zoneless_buy("buy", None, None, False) == "zoneless_buy_no_anchor"

    def test_info_when_anchor_state_unknown(self) -> None:
        assert classify_zoneless_buy("buy", None, None, None) == "zoneless_buy_no_anchor"

    def test_not_buy_or_zoned(self) -> None:
        assert classify_zoneless_buy("hold", None, None, True) is None
        assert classify_zoneless_buy("buy", 10, 12, True) is None
        assert classify_zoneless_buy("buy", 10, None, True) is None  # partial zone != zoneless


class TestBaseVsAnchorClose:
    def test_far(self) -> None:
        assert check_base_vs_anchor_close(200, 100) is not None  # +100%

    def test_within(self) -> None:
        assert check_base_vs_anchor_close(150, 100) is None  # +50% <= 60%

    def test_missing_or_nonpositive_close(self) -> None:
        assert check_base_vs_anchor_close(200, None) is None
        assert check_base_vs_anchor_close(200, 0) is None
        assert check_base_vs_anchor_close(None, 100) is None


class TestStaleAnchor:
    _created = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)

    def test_stale(self) -> None:
        assert check_stale_anchor(date(2026, 7, 1), self._created) is not None

    def test_fresh_and_absent(self) -> None:
        assert check_stale_anchor(date(2026, 7, 10), self._created) is None
        assert check_stale_anchor(None, self._created) is None


class TestAbstention:
    def test_all_null(self) -> None:
        assert is_target_abstention(None, "nan", None)

    def test_any_present(self) -> None:
        assert not is_target_abstention(None, 10, None)


class TestClaimLint:
    _blocks = {
        "news": {"available": True, "count": 4},
        "filings": {"available": False, "count": 0},
        "fair_value_band": {"available": True, "status": "high"},
    }

    def test_fabricated_claim_flagged(self) -> None:
        memo = "There is no recent news coverage for this name."
        assert claim_lint(memo, self._blocks) == ["news"]

    def test_honest_claim_not_flagged(self) -> None:
        """Filings genuinely unavailable — claiming so is correct."""
        memo = "Filings data is unavailable for this issuer."
        assert claim_lint(memo, self._blocks) == []

    def test_multiword_keyword(self) -> None:
        memo = "A fair value band is not available, so anchoring uses fundamentals."
        assert claim_lint(memo, self._blocks) == ["fair_value_band"]

    def test_window_bounded(self) -> None:
        """Negation and keyword in different sentences do not match."""
        memo = "There is no debt on the balance sheet. The news flow is strong."
        assert claim_lint(memo, self._blocks) == []

    def test_no_claims(self) -> None:
        assert claim_lint("Solid quarter; strong news momentum.", self._blocks) == []

    def test_keyword_map_covers_all_summarized_blocks(self) -> None:
        """Writer prompt's availability rule applies to every block —
        keep the keyword map aligned with summarize_context's block set."""
        summarized = {
            "news",
            "filings",
            "ta_state",
            "valuation",
            "fundamentals",
            "price_anchor",
            "prior_thesis",
            "risk_metrics",
            "fair_value_band",
            "earnings_history",
            "analyst_estimates",
            "analytics_evidence",
            "instrument",
        }
        # prior_thesis / instrument carry no "data availability" language a
        # writer would fabricate about; every other block must be covered.
        assert summarized - set(_BLOCK_KEYWORDS) == {"prior_thesis", "instrument"}


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


class TestComputeReportRealQuery:
    """One db-tier drive of the REAL query into the real reader — a
    projection gap fails here, not in production (prevention-log #2021
    dict-row lesson)."""

    def _seed(self, conn) -> int:
        iid = 9014
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
            " VALUES (%s, 'DQT', 'DQ Test Co', TRUE)",
            (iid,),
        )
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume)"
            " VALUES (%s, '2026-07-01', 10, 10, 10, 10, 0)",
            (iid,),
        )
        # Inverted targets + zoneless buy; older superseded row proves
        # latest-per-instrument selection.
        conn.execute(
            """
            INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown,
                                bear_value, base_value, bull_value, model, provider, prompt_version, created_at)
            VALUES (%s, 1, 'value', 'hold', 'old clean memo', 1, 2, 3, 'm', 'p', 'v4', now() - interval '2 days')
            """,
            (iid,),
        )
        thesis_id = conn.execute(
            """
            INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown,
                                bear_value, base_value, bull_value, model, provider, prompt_version)
            VALUES (%s, 2, 'value', 'buy', 'there is no recent news coverage.', 30, 20, 10, 'm', 'p', 'v4')
            RETURNING thesis_id
            """,
            (iid,),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO thesis_runs (instrument_id, trigger, status, thesis_id, context_summary)
            VALUES (%s, 'manual', 'ok', %s,
                    '{"blocks": {"news": {"available": true, "count": 3},
                                 "price_anchor": {"available": true, "as_of": "2026-07-01"}},
                      "prompt_version": "v4"}'::jsonb)
            """,
            (iid, thesis_id),
        )
        conn.commit()
        return thesis_id

    def test_report_classes(self, conn) -> None:
        from app.services.thesis_dq_audit import compute_thesis_dq_report

        thesis_id = self._seed(conn)
        report = compute_thesis_dq_report(conn)
        assert report.scanned >= 1
        by_class: dict[str, list] = {}
        for f in report.findings:
            if f.thesis_id == thesis_id:
                by_class.setdefault(f.dq_class, []).append(f)
        assert "ordering" in by_class
        assert "zoneless_buy" in by_class
        assert "claim_lint" in by_class
        # base 20 vs anchor-date close 10 = 100% off -> flagged
        assert "base_far_from_close" in by_class
        # v1 (clean, ordered) row was NOT scanned — latest-per-instrument only.
        assert report.class_counts["ordering"] == 1


class TestCheckFireRate:
    """#2072 — standing fire-rate band check (2-8%/month proxy)."""

    def test_in_band_none(self) -> None:
        from app.services.thesis_dq_audit import check_fire_rate

        assert check_fire_rate("price_move", 5, 100) is None  # 5%

    def test_above_band_flags(self) -> None:
        from app.services.thesis_dq_audit import check_fire_rate

        detail = check_fire_rate("price_move", 12, 100)
        assert detail is not None and "above" in detail and "12.0%" in detail

    def test_below_band_is_silent(self) -> None:
        """A small standing count is the healthy drained steady state —
        it cannot distinguish 'calibrated' from 'too tight'; #2063's
        dated trailing-30d re-check owns the under-firing judgement."""
        from app.services.thesis_dq_audit import check_fire_rate

        assert check_fire_rate("band_exit", 1, 100) is None
        assert check_fire_rate("news_spike", 0, 100) is None

    def test_empty_population_none(self) -> None:
        from app.services.thesis_dq_audit import check_fire_rate

        assert check_fire_rate("price_move", 3, 0) is None

    def test_band_edges_inclusive(self) -> None:
        from app.services.thesis_dq_audit import check_fire_rate

        assert check_fire_rate("price_move", 2, 100) is None  # 2% lower edge
        assert check_fire_rate("price_move", 8, 100) is None  # 8% upper edge
