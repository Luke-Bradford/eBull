"""#2842 slice 4b-i — ranking-pot rebalance steps 0–3, pure half (spec §4). No DB."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from app.services import ranking_pot as rp
from app.services import ranking_pot_rebalance as rb
from app.services.ai_trial_pack import canonical_json, canonical_sha256
from app.services.market_calendar import us_market_status

LAST = date(2026, 9, 30)
AS_OF = datetime(2026, 9, 30, 23, 30, tzinfo=UTC)
OCT, NOV, DEC = date(2026, 10, 1), date(2026, 11, 1), date(2026, 12, 1)


def _fire(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 30, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Step 0
# ---------------------------------------------------------------------------
def test_session_ordinal_counts_nyse_sessions_of_the_month() -> None:
    # October 2026: Thu 1, Fri 2, Mon 5, Tue 6, Wed 7, Thu 8.
    assert [rb.session_ordinal(date(2026, 10, d)) for d in (1, 2, 5, 6, 7, 8)] == [1, 2, 3, 4, 5, 6]
    with pytest.raises(ValueError):
        rb.session_ordinal(date(2026, 10, 3))  # Saturday


def test_first_month_is_the_freeze_month_only_inside_its_first_five_sessions() -> None:
    assert rb.first_month(_fire(date(2026, 10, 1))) == OCT  # target Oct 2 = session 2
    assert rb.first_month(_fire(date(2026, 10, 6))) == OCT  # target Oct 7 = session 5
    assert rb.first_month(_fire(date(2026, 10, 7))) == NOV  # target Oct 8 = session 6
    assert rb.first_month(_fire(date(2026, 10, 20))) == NOV


def test_plan_due_inside_the_first_five_sessions() -> None:
    p = rb.plan(_fire(date(2026, 10, 1)), first=OCT, resolved=frozenset())
    assert (p.target_session, p.month, p.due, p.skips) == (date(2026, 10, 2), OCT, True, ())
    assert not rb.plan(_fire(date(2026, 10, 1)), first=OCT, resolved=frozenset({OCT})).due


def test_plan_skips_the_month_at_session_six_with_nothing_decided() -> None:
    p = rb.plan(_fire(date(2026, 10, 7)), first=OCT, resolved=frozenset())
    assert (p.due, p.skips) == (False, (OCT,))
    assert rb.plan(_fire(date(2026, 10, 7)), first=OCT, resolved=frozenset({OCT})).skips == ()


def test_plan_closes_every_month_missed_by_downtime_r3_71() -> None:
    # Down from Oct 7 to Dec 3: October was decided, November never fired.
    p = rb.plan(_fire(date(2026, 12, 3)), first=OCT, resolved=frozenset({OCT}))
    assert (p.target_session, p.due, p.skips) == (date(2026, 12, 4), True, (NOV,))
    # And with October also open, past its sixth session, both close.
    late = rb.plan(_fire(date(2026, 12, 10)), first=OCT, resolved=frozenset())
    assert (late.due, late.skips) == (False, (OCT, NOV, DEC))


def test_plan_before_the_first_month_is_neither_due_nor_skipping() -> None:
    p = rb.plan(_fire(date(2026, 10, 1)), first=NOV, resolved=frozenset())
    assert (p.due, p.skips) == (False, ())
    with pytest.raises(ValueError):
        rb.plan(_fire(date(2026, 10, 1)), first=date(2026, 11, 2), resolved=frozenset())


# ---------------------------------------------------------------------------
# Step 2
# ---------------------------------------------------------------------------
def _cov(s0: int = 100, scored: int = 100, barred: int = 100) -> rb.Coverage:
    return rb.Coverage(s0_tradable=s0, scored=scored, barred=barred)


@pytest.mark.parametrize(
    ("cov", "policy_ok", "spy_ok", "expected"),
    [
        (_cov(), True, True, None),
        (_cov(scored=95, barred=95), True, True, None),  # inclusive 95%
        (_cov(scored=94), True, True, "scores_run_incomplete"),
        (_cov(s0=0, scored=0, barred=0), True, True, "scores_run_incomplete"),
        (_cov(barred=94), True, True, "price_daily_stale"),
        (_cov(), True, False, "spy_unavailable"),
        # Frozen order: drift first, then coverage, then SPY.
        (_cov(scored=1, barred=0), False, False, "ranking_drift"),
        (_cov(barred=0), True, False, "price_daily_stale"),
    ],
)
def test_gate_refusal_thresholds_and_order(
    cov: rb.Coverage, policy_ok: bool, spy_ok: bool, expected: str | None
) -> None:
    assert rb.gate_refusal(cov, policy_ok=policy_ok, spy_ok=spy_ok) == expected


def test_universe_collapse_guard_r3_74_r3_75() -> None:
    assert not rb.universe_collapsed(1, previous_r_count=None, consecutive_collapses=0)  # first rebalance
    assert not rb.universe_collapsed(800, previous_r_count=1000, consecutive_collapses=0)  # exactly 80%
    assert rb.universe_collapsed(799, previous_r_count=1000, consecutive_collapses=0)
    assert rb.universe_collapsed(799, previous_r_count=1000, consecutive_collapses=1)
    assert not rb.universe_collapsed(1, previous_r_count=1000, consecutive_collapses=2)  # override matured


_GOOD_BAR: dict[str, Any] = {"open": Decimal(1), "high": Decimal(2), "low": Decimal("0.5"), "close": Decimal(1)}


def _spy(**over: Any) -> rb.SpyInputs:
    base: dict[str, Any] = {
        "bid": Decimal("764.46"),
        "ask": Decimal("764.48"),
        "quoted_at": AS_OF - timedelta(hours=1),
        "bar": (LAST, _GOOD_BAR),
    }
    base.update(over)
    return rb.SpyInputs(**base)


@pytest.mark.parametrize(
    ("spy", "ok"),
    [
        (_spy(), True),
        (_spy(quoted_at=AS_OF - timedelta(hours=25)), False),
        (_spy(quoted_at=None), False),
        (_spy(bid=Decimal("700")), False),  # spread > 1% of mid
        (_spy(bar=None), False),
        (_spy(bar=(LAST - timedelta(days=1), _GOOD_BAR)), False),  # not the last completed session
        (_spy(bar=(LAST, {"open": Decimal(0), "high": Decimal(2), "low": Decimal(0), "close": Decimal(1)})), False),
        (_spy(bar=(LAST, {"open": Decimal(1), "high": Decimal(2), "low": Decimal(0.5), "close": None})), False),
        (_spy(bar=(LAST, {"open": Decimal(3), "high": Decimal(2), "low": Decimal(1), "close": Decimal(1)})), False),
        (_spy(bar=(LAST, {"open": Decimal("NaN"), "high": 2, "low": 1, "close": 1})), False),
    ],
)
def test_spy_validity_r3_24(spy: rb.SpyInputs, ok: bool) -> None:
    assert rb.spy_ok(spy, as_of=AS_OF, last_session=LAST) is ok


# ---------------------------------------------------------------------------
# Step 3 — the snapshot round trip
# ---------------------------------------------------------------------------
def _sessions(count: int) -> tuple[date, ...]:
    out: list[date] = []
    d = LAST
    while len(out) < count:
        if us_market_status(d) != "closed":
            out.append(d)
        d -= timedelta(days=1)
    return tuple(reversed(out))


def _name(iid: int, score: Decimal | None, *, jump: str = "0.01", bars: bool = True, **over: Any) -> rp.NameFacts:
    closes = [Decimal(100)] * 59 + [Decimal(100) * (1 + Decimal(jump))]
    rows = tuple(
        {"open": c, "high": c * Decimal("1.01"), "low": c * Decimal("0.99"), "close": c, "volume": 1000} for c in closes
    )
    base: dict[str, Any] = {
        "instrument_id": iid,
        "symbol": f"S{iid}",
        "is_tradable": True,
        "asset_class": "us_equity",
        "total_score": score,
        "completeness_tier": "full",
        "filings_status": "analysable",
        "market_cap_usd": Decimal(10**11),
        "bid": Decimal("100.95"),
        "ask": Decimal("101.05"),
        "quoted_at": AS_OF - timedelta(hours=1),
        "bar_dates": _sessions(60) if bars else (),
        "bar_rows": rows if bars else (),
    }
    base.update(over)
    return rp.NameFacts(**base)


def _score(total: Decimal) -> rp.ScoreBreakdown:
    return rp.ScoreBreakdown(
        model_version="v1.5-balanced",
        total_score=total,
        raw_total=total,
        families={f: Decimal("0.5") for f in rb.SCORE_FAMILIES} | {"sentiment": None},
        penalties_json=({"kind": "reward", "name": "strong_calmar", "reason": "r", "addition": 0.03},),
    )


def _inputs(monkeypatch: pytest.MonkeyPatch) -> rb.SnapshotInputs:
    monkeypatch.setattr(rp, "MIN_NYSE_CAPS", 1)
    monkeypatch.setattr(rp, "MIN_VALID_MAX", 1)
    facts = (
        _name(1, Decimal("0.9"), sic="7372"),
        _name(2, Decimal("0.8"), quoted_at=AS_OF - timedelta(days=2)),  # in R, fails an entry rule
        _name(3, Decimal("NaN"), bars=False),  # stored NaN: fails `score_not_positive`, decodes exactly
        _name(4, None, bars=False, is_tradable=False, bid=None, ask=None, quoted_at=None, market_cap_usd=None),
        _name(5, Decimal("0.7"), bars=False, filings_status=None, completeness_tier=None),
    )
    scores = {f.instrument_id: _score(f.total_score) for f in facts if f.total_score is not None}
    return rb.SnapshotInputs(
        declaration_id=7,
        declaration_sha256="d" * 64,
        policy_hash="e" * 64,
        as_of=AS_OF,
        last_session=LAST,
        target_session=date(2026, 10, 1),
        scored_at=AS_OF - timedelta(hours=6),
        facts=facts,
        scores=scores,
        nyse_caps=((1, Decimal(10**11)), (99, Decimal(10**9)), (100, None)),
        spy=_spy(bars=((LAST - timedelta(days=1), _GOOD_BAR), (LAST, _GOOD_BAR))),
        theses={1: rb.ThesisUsed(41, AS_OF - timedelta(days=40), "claude-x", "p7")},
    )


def test_snapshot_round_trips_through_json_and_re_derives_the_universes(monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = _inputs(monkeypatch)
    doc = rb.encode_snapshot(inputs)
    stored = json.loads(canonical_json(doc))  # what JSONB hands back
    assert canonical_sha256(stored) == canonical_sha256(doc)
    back = rb.decode_snapshot(stored)
    assert canonical_sha256(rb.encode_snapshot(back)) == canonical_sha256(doc)

    u, u2 = rb.universes_of(inputs), rb.universes_of(back)
    assert isinstance(u, rp.Universes) and isinstance(u2, rp.Universes)
    assert u == u2
    assert u.r_ids == {1, 2} and u.f_ids == {1}
    assert dict(u.entry_failure) == {2: "quote_ineligible"}
    assert dict(u.hold_failure) == {3: "score_not_positive", 4: "not_tradable", 5: "completeness_insufficient"}
    assert back.scores[3].total_score.is_nan()
    assert back.scores[1].families["sentiment"] is None
    assert back.theses == inputs.theses  # r3-95: captured provenance survives the round trip
    # Slice 6c-ii-c-1's exposure inputs: the SIC and SPY's whole segment survive too.
    assert [f.sic for f in back.facts] == ["7372", None, None, None, None]
    assert [(d, r["close"]) for d, r in back.spy.bars] == [(d, r["close"]) for d, r in inputs.spy.bars]


def test_snapshot_holds_inputs_only_r3_9(monkeypatch: pytest.MonkeyPatch) -> None:
    doc = rb.encode_snapshot(_inputs(monkeypatch))
    text = canonical_json(doc)
    for derived in ("r_rank", "f_rank", "breakpoint", "max_cut", "donor", "control", "r_ids", "f_ids"):
        assert f'"{derived}"' not in text, derived
    assert doc["nyse_caps"] == [[1, str(10**11)], [99, str(10**9)], [100, None]]  # r3-85: non-S₀ names kept


def test_snapshot_refuses_facts_that_disagree_with_their_score_row(monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = _inputs(monkeypatch)
    with pytest.raises(ValueError):
        rb.encode_snapshot(rb.SnapshotInputs(**{**inputs.__dict__, "scores": {}}))
    with pytest.raises(ValueError):
        rb.encode_snapshot(
            rb.SnapshotInputs(**{**inputs.__dict__, "theses": {999: rb.ThesisUsed(1, AS_OF, None, None)}})
        )
    with pytest.raises(ValueError):
        rb.decode_snapshot({"kind": "something-else"})


def test_thresholds_are_the_spec_figures() -> None:
    assert (rb.FIRST_SESSIONS, rb.SCORE_COVERAGE_MIN, rb.BAR_COVERAGE_MIN, rb.UNIVERSE_FLOOR) == (
        5,
        Fraction(95, 100),
        Fraction(95, 100),
        Fraction(80, 100),
    )
    assert rb.INPUT_REFUSALS[0] == "ranking_drift"
