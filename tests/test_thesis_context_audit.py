"""Fast-tier tests for the pure thesis context-audit helpers (#2017).

No DB: hash determinism + per-block availability/status/as-of summary.
"""

from __future__ import annotations

from collections.abc import Mapping

from app.services.thesis_context_audit import hash_context, summarize_context

_PV = "v4"


def _summary_blocks(context: Mapping[str, object]) -> dict[str, object]:
    """Type-narrowing helper: ``summarize_context``'s "blocks" value is always
    a dict by construction, but that's erased by the ``dict[str, object]``
    return annotation after one subscript — narrow once here instead of an
    ``isinstance`` assert scattered across every test.
    """
    blocks = summarize_context(context, _PV)["blocks"]
    assert isinstance(blocks, dict)
    return blocks


def _full_context() -> dict[str, object]:
    """A representative _assemble_context-shaped dict (subset of real fields)."""
    return {
        "instrument": {"symbol": "AAPL", "company_name": "Apple", "currency": "USD"},
        "fundamentals": [
            {"as_of_date": "2025-03-31", "revenue_ttm": 4.0e11},
            {"as_of_date": "2024-12-31", "revenue_ttm": 3.9e11},
        ],
        "filings": [{"filing_date": "2025-05-01", "filing_type": "10-Q", "summary": "…"}],
        # news is ordered importance DESC, NOT recency — [0] is NOT the newest event.
        "news": [
            {"event_time": "2025-07-01T00:00:00+00:00", "headline": "high-importance older"},
            {"event_time": "2025-07-10T00:00:00+00:00", "headline": "low-importance newer"},
        ],
        "prior_thesis": {"version": 3, "stance": "buy", "created_at": "2025-07-01T12:00:00+00:00"},
        "risk_metrics": {
            "metric_version": "risk_v1",
            "windows": [
                {"window_key": "1y", "as_of_date": "2025-07-11", "cagr_status": "ok"},
                {"window_key": "3y", "as_of_date": "2025-07-11", "cagr_status": "thin_history"},
            ],
        },
        "price_anchor": {"close": 210.0, "price_date": "2025-07-11", "currency": "USD"},
        "valuation": {"available": True, "current_price": 210.0, "price_as_of": "2025-07-11"},
        # representative _shape_fair_value_band output — as_of_date (band vintage)
        # deliberately DISTINCT from price_as_of so the test proves we pick as_of_date.
        "fair_value_band": {
            "available": True,
            "reason": None,
            "quality_status": "ok",
            "bear": 150.0,
            "base": 200.0,
            "bull": 250.0,
            "as_of_date": "2025-06-30",
            "ttm_end": "2025-03-31",
            "price_as_of": "2025-07-11",
            "basis": {},
        },
        "analytics_evidence": {"schema": "iar_v1", "as_of": "2025-07-09T00:00:00+00:00", "piotroski": {"score": 7}},
        "ta_state": {"sma_50": 205.0, "sma_200": 190.0, "price_vs_sma200": "above"},
        "earnings_history": [],
        "analyst_estimates": None,
    }


# --- hash_context ---------------------------------------------------------


def test_hash_is_deterministic_and_key_order_independent() -> None:
    a = {"x": 1, "y": {"b": 2, "a": 3}, "z": [1, 2]}
    b = {"z": [1, 2], "y": {"a": 3, "b": 2}, "x": 1}  # same content, keys reordered
    assert hash_context(a) == hash_context(b)


def test_hash_changes_on_nested_value_change() -> None:
    a = _full_context()
    b = _full_context()
    b["price_anchor"] = {**a["price_anchor"], "close": 999.0}  # type: ignore[dict-item]
    assert hash_context(a) != hash_context(b)


def test_hash_is_64_hex_chars() -> None:
    h = hash_context(_full_context())
    assert len(h) == 64 and all(c in "0123456789abcdef" for c in h)


# --- summarize_context ----------------------------------------------------


def test_prompt_version_echoed() -> None:
    s = summarize_context(_full_context(), _PV)
    assert s["prompt_version"] == _PV


def test_absent_blocks_marked_unavailable_never_fabricated() -> None:
    ctx = {"analyst_estimates": None, "earnings_history": [], "empty_dict": {}}
    blocks = _summary_blocks(ctx)
    assert blocks["analyst_estimates"] == {"available": False}
    assert blocks["earnings_history"] == {"available": False, "count": 0}
    assert blocks["empty_dict"] == {"available": False}


def test_list_as_of_is_max_not_first_element() -> None:
    # Guards Codex HIGH-2: news[0] is the high-importance OLDER event; the
    # summary as_of must be the newest event_time (max), 2025-07-10.
    blocks = _summary_blocks(_full_context())
    assert blocks["news"] == {"available": True, "count": 2, "as_of": "2025-07-10T00:00:00+00:00"}
    fundamentals = blocks["fundamentals"]
    assert isinstance(fundamentals, dict)
    assert fundamentals["as_of"] == "2025-03-31"  # DESC-ordered, max = latest


def test_explicit_available_blocks_mirror_flag_and_carry_status_asof() -> None:
    blocks = _summary_blocks(_full_context())
    # fair_value_band as_of = the band's own as_of_date (2025-06-30), NOT price_as_of.
    assert blocks["fair_value_band"] == {"available": True, "status": "ok", "as_of": "2025-06-30"}
    assert blocks["valuation"] == {"available": True, "as_of": "2025-07-11"}  # present → no status field


def test_valuation_absent_carries_reason() -> None:
    ctx = {"valuation": {"available": False, "reason": "no_live_quote"}}
    assert _summary_blocks(ctx)["valuation"] == {
        "available": False,
        "status": "no_live_quote",
    }


def test_malformed_analytics_is_unavailable() -> None:
    # Codex MED-2: a status-only dict is absent usable evidence, not present.
    ctx = {"analytics_evidence": {"reason": "malformed"}}
    assert _summary_blocks(ctx)["analytics_evidence"] == {
        "available": False,
        "status": "malformed",
    }


def test_unsupported_schema_analytics_is_unavailable() -> None:
    ctx = {"analytics_evidence": {"reason": "unsupported_schema", "schema": "iar_v2"}}
    assert _summary_blocks(ctx)["analytics_evidence"] == {
        "available": False,
        "status": "unsupported_schema",
    }


def test_risk_metrics_carries_version_and_max_window_asof() -> None:
    blocks = _summary_blocks(_full_context())
    assert blocks["risk_metrics"] == {
        "available": True,
        "metric_version": "risk_v1",
        "as_of": "2025-07-11",
    }


def test_ta_state_and_instrument_available_only() -> None:
    blocks = _summary_blocks(_full_context())
    assert blocks["ta_state"] == {"available": True}
    assert blocks["instrument"] == {"available": True}
    assert blocks["price_anchor"] == {"available": True, "as_of": "2025-07-11"}
    assert blocks["prior_thesis"] == {"available": True, "as_of": "2025-07-01T12:00:00+00:00"}


def test_unknown_block_gets_drift_safe_available_entry() -> None:
    ctx = {"a_future_block": {"some": "payload"}}
    assert _summary_blocks(ctx)["a_future_block"] == {"available": True}
