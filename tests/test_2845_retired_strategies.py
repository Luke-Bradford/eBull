"""#2845 — retiring eight of the ten, and the things retirement must NOT change.

The interesting assertions here are the negative ones. Retiring a strategy is easy;
retiring it without silently breaking its stored evidence, without concealing a
malformed declaration, and without leaving a permanent false alarm on
`/system/status` is the actual work, and each of those is a test below.

Pure only — no `ebull_test_conn`, so the `db` marker does not evict this module from
the fast pre-push gate. The scan's behaviour is exercised in
`tests/test_2845_retired_strategies_db.py`.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from app.services import backtest_run
from app.services.backtest_run import runnable_strategies
from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_control_plane import StrategyControlError, promote_strategy
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyEntry
from app.services.strategy_result_identity import current_result_versions
from app.services.strategy_scan_freshness import assess_scan_freshness
from app.services.strategy_signal_scan import SCAN_UNIVERSE

RETIRED = frozenset(
    {
        "s1-time-series-momentum",
        "s2-cross-sectional-momentum",
        "s3-mean-reversion-in-trend",
        "s5-support-bounce",
        "s6-resistance-breakout",
        "s7-trend-pullback",
        "s9-squeeze-expansion",
        "s10-relative-strength-leader",
    }
)
#: ⚠ THREE, not two. s4 and s8 are the survivors of the measured ten (#2827);
#: s11 is #2840's research seat — S-4's rule gated to the two volatile regimes —
#: which landed AFTER this retirement and is not one of the ten. It is listed
#: here rather than excused by loosening the assertion below, so "the manifest
#: minus the retired eight" stays an exact statement.
KEPT = frozenset(
    {
        "s4-volatility-compression-breakout",
        "s8-range-mean-reversion",
        "s11-volatile-regime-gated-breakout",
    }
)


def _retired_ids() -> frozenset[str]:
    return frozenset(sid for sid, entry in STRATEGY_MANIFEST.items() if entry.retired_reason is not None)


# --------------------------------------------------------------- the declaration


def test_the_retirement_set_is_exactly_the_measured_dead_eight() -> None:
    assert _retired_ids() == RETIRED
    assert frozenset(STRATEGY_MANIFEST) - _retired_ids() == KEPT


def test_every_retirement_names_a_reason_and_its_evidence() -> None:
    for strategy_id in sorted(RETIRED):
        reason = STRATEGY_MANIFEST[strategy_id].retired_reason
        assert reason is not None and reason.strip()
        # The ticket, not the numbers: a derived statistic written by hand goes
        # stale in the place a reader trusts most.
        assert "#2827" in reason
        assert "2026-08-22" in reason


@pytest.mark.parametrize("blank", ["", " ", "\t\n"])
def test_a_blank_retirement_reason_is_refused(blank: str) -> None:
    """A blank reason renders an empty exclusion in the UI and in a job note — the
    silent skip this field exists to prevent, wearing the field's name."""
    live = STRATEGY_MANIFEST["s4-volatility-compression-breakout"]
    with pytest.raises(ValueError, match="blank retired_reason"):
        replace(live, retired_reason=blank)


# ------------------------------------------- what retirement must NOT change


def test_all_ten_stay_in_the_manifest_and_keep_resolving() -> None:
    """⚠ The assertion that catches a future "tidy-up" deleting the entries.

    Membership is what keeps 568 stored result rows readable: drop an entry and
    `registered_strategy_purpose` goes None, `current_result_versions` stops
    resolving it, and `/strategies` loses it entirely — the "vanished" outcome the
    acceptance criteria forbid.
    """
    # ⚠ ELEVEN. The ten stay; #2840's S-11 research seat was added afterwards.
    # Derived from RETIRED | KEPT rather than restated, so the next addition
    # updates one place.
    assert len(STRATEGY_MANIFEST) == len(RETIRED | KEPT) == 11
    assert RETIRED | KEPT == set(STRATEGY_MANIFEST)
    assert set(current_result_versions()) == set(STRATEGY_MANIFEST)


def test_retirement_does_not_move_any_identity_version() -> None:
    """⚠ The load-bearing claim of "stored rows stay resolvable", pinned.

    `retired_reason` must never reach the identity hash — it comes from the
    `identity` factory rather than from the dataclass. If it ever entered, retiring
    a strategy would rotate its version and strand every row written under the old
    one. Asserted by construction: an entry with and without the field must produce
    the same version.
    """
    for strategy_id, entry in sorted(STRATEGY_MANIFEST.items()):
        with_field = entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        without_field = (
            replace(entry, retired_reason=None).identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        )
        assert with_field == without_field, strategy_id


def test_retirement_cannot_conceal_a_malformed_declaration(monkeypatch: pytest.MonkeyPatch) -> None:
    """⚠⚠ Codex ckpt-1's finding, as a test.

    A first draft checked `retired_reason` BEFORE the capability probe, which would
    have made retirement a way to hide a broken entry — and eight of ten would have
    stopped being schema-checked in one change. A retired, level-based entry whose
    builder has stopped refusing must still raise.
    """
    s5 = STRATEGY_MANIFEST["s5-support-bounce"]
    assert s5.retired_reason is not None
    manifest = {**STRATEGY_MANIFEST, s5.strategy_id: replace(s5, exit_levels=None, exit_levels_batch=None)}
    monkeypatch.setattr(backtest_run, "_demonstrate_level_refusal", lambda entry, regime: None)
    with pytest.raises(RuntimeError, match="did NOT refuse"):
        runnable_strategies(manifest)


def test_a_capability_refusal_outranks_a_retirement(monkeypatch: pytest.MonkeyPatch) -> None:
    """ "The builder cannot produce a result for this at all" is the stronger
    statement, so a retired-and-broken entry reports the refusal, not the policy."""
    s5 = STRATEGY_MANIFEST["s5-support-bounce"]
    manifest = {**STRATEGY_MANIFEST, s5.strategy_id: replace(s5, exit_levels=None, exit_levels_batch=None)}
    monkeypatch.setattr(backtest_run, "_demonstrate_level_refusal", lambda entry, regime: "builder refused")
    _runnable, excluded = runnable_strategies(manifest)
    by_id = {item.strategy_id: item for item in excluded}
    assert by_id["s5-support-bounce"].kind == "capability"
    assert by_id["s5-support-bounce"].reason == "builder refused"
    assert by_id["s1-time-series-momentum"].kind == "retired"


def test_the_outcome_drain_still_covers_retired_strategies() -> None:
    """⚠ Retirement stops NEW evidence, never the drain of old.

    `run_outcome_resolution` selects every entry with `exit_levels`, which includes
    retired s5/s6/s7/s9. Filtering them would strand their already-fired signals
    permanently — and silently, because an unresolved fill has no alarm. This
    asserts the omission is the deliberate one.
    """
    level_ids = {sid for sid, entry in STRATEGY_MANIFEST.items() if entry.exit_levels}
    assert level_ids & RETIRED == {
        "s5-support-bounce",
        "s6-resistance-breakout",
        "s7-trend-pullback",
        "s9-squeeze-expansion",
    }


# ------------------------------------------------------- the false alarm, averted


def _freshness(strategy_id: str, *, retired: bool) -> object:
    """One verdict for a strategy whose watermark is old enough to be `stale`."""
    trading_dates = [date(2026, 8, d) for d in range(1, 21)]
    results = assess_scan_freshness(
        current_versions={strategy_id: "v1"},
        watermarks={(strategy_id, "v1"): date(2026, 8, 1)},
        trading_dates=trading_dates,
        retired_ids=frozenset({strategy_id}) if retired else frozenset(),
    )
    assert len(results) == 1
    return results[0]


def test_a_retired_strategy_reads_retired_and_does_not_alert() -> None:
    """⚠⚠ Without this the change ships a PERMANENT false alarm.

    A retired strategy stops scanning, so its watermark freezes and every poll of
    `/system/status` would report it `stale` for ever. The control arm is the same
    inputs with `retired_ids` empty — which must still be `stale`, or this test
    would pass for the wrong reason.
    """
    control = _freshness("s1-time-series-momentum", retired=True)
    assert control.status == "retired"  # type: ignore[attr-defined]
    assert control.is_alerting is False  # type: ignore[attr-defined]
    # ⚠ The watermark it froze at is still reported, so "when did it stop" stays
    # answerable from the verdict rather than needing a second query.
    assert control.frontier_date == date(2026, 8, 1)  # type: ignore[attr-defined]

    not_retired = _freshness("s1-time-series-momentum", retired=False)
    assert not_retired.status == "stale"  # type: ignore[attr-defined]
    assert not_retired.is_alerting is True  # type: ignore[attr-defined]


def test_every_manifest_strategy_still_gets_a_freshness_verdict() -> None:
    """A strategy with no verdict is a strategy nothing reports on — retirement
    changes the verdict, never whether there is one."""
    results = assess_scan_freshness(
        current_versions=dict.fromkeys(STRATEGY_MANIFEST, "v1"),
        watermarks={},
        trading_dates=[date(2026, 8, 20)],
        retired_ids=_retired_ids(),
    )
    assert {item.strategy_id for item in results} == set(STRATEGY_MANIFEST)


# --------------------------------------------------- a retired stage cannot advance


def test_promote_strategy_refuses_a_retired_strategy_into_an_evidence_stage() -> None:
    """Bound at the chokepoint every promotion path passes: the API's `/advance`,
    #2770's `advance_strategy`, and #2843's autonomous approver.

    ⚠ No connection is needed — the refusal is raised from the argument checks,
    before `_lock_strategy` touches the database. `None` reaching a query would be
    an AttributeError, so this passing IS the proof it refuses early.
    """
    with pytest.raises(StrategyControlError, match="is retired and cannot advance"):
        promote_strategy(
            None,  # type: ignore[arg-type]
            strategy_id="s1-time-series-momentum",
            strategy_version="v1",
            to_stage="historical_validated",
            promoted_by="operator",
            reason="should refuse",
            evidence_ref="ref",
        )


def test_the_retirement_guard_fires_ahead_of_the_purpose_guard() -> None:
    """⚠ Ordering is the point, not an accident.

    Every manifest entry is `harness_validation` today, so placing the retirement
    check after the purpose checks would make it UNREACHABLE — and an unreachable
    guard cannot be proven to work. The test above only passes because retirement
    is checked first; this records why that ordering is load-bearing.
    """
    assert all(entry.purpose == "harness_validation" for entry in STRATEGY_MANIFEST.values())
    with pytest.raises(StrategyControlError) as caught:
        promote_strategy(
            None,  # type: ignore[arg-type]
            strategy_id="s4-volatility-compression-breakout",
            strategy_version="v1",
            to_stage="historical_validated",
            promoted_by="operator",
            reason="a KEPT strategy still hits the purpose guard",
            evidence_ref="ref",
        )
    assert "permanent controls" in str(caught.value)


def test_a_retired_strategy_may_still_be_paused() -> None:
    """⚠ The guard is scoped to the evidence stages deliberately. A lifecycle that
    cannot be wound down is worse than one that can still move forward, so `paused`
    must stay reachable — asserted by the refusal NOT being the retirement one."""
    with pytest.raises(Exception) as caught:  # noqa: PT011 - the type is not the point
        promote_strategy(
            None,  # type: ignore[arg-type]
            strategy_id="s1-time-series-momentum",
            strategy_version="v1",
            to_stage="paused",
            promoted_by="operator",
            reason="wind it down",
        )
    assert "is retired and cannot advance" not in str(caught.value)


def test_a_manifest_entry_is_constructible_without_the_field() -> None:
    """`retired_reason` defaults to None, so every existing construction site — and
    every future strategy — is live unless it says otherwise."""
    live = STRATEGY_MANIFEST["s4-volatility-compression-breakout"]
    assert isinstance(live, StrategyEntry)
    assert live.retired_reason is None


# ------------------------------------------------------- the scan stops paying for them


def test_the_scan_excludes_exactly_the_retired_eight() -> None:
    from app.services.strategy_signal_scan import retired_scan_ids

    assert retired_scan_ids(STRATEGY_MANIFEST) == RETIRED


def test_both_scan_call_sites_share_one_retirement_predicate() -> None:
    """⚠ Written twice, the per-strategy skip and the decision-calendar filter would
    drift — and silently in the worse direction: a strategy skipped by the scan but
    still having its calendar published looks healthy on the card while producing
    nothing. Asserted from the source because the two sites cannot be reached
    together without a universe/bar fixture.
    """
    import inspect

    from app.services import strategy_signal_scan

    source = inspect.getsource(strategy_signal_scan)
    # The predicate is used, and the raw field test appears ONLY inside its own body.
    assert source.count("retired_scan_ids(manifest)") == 2
    assert source.count("entry.retired_reason is not None") == 1


def test_no_retired_cross_sectional_strategy_can_reach_calendar_publication() -> None:
    """Both cross-sectional strategies are retired, so the publication list is empty
    and `_publish_decision_calendars` returns before loading the union calendar."""
    from app.services.strategy_signal_scan import retired_scan_ids

    cross_sectional = {sid for sid, entry in STRATEGY_MANIFEST.items() if entry.strategy_class == "cross_sectional"}
    assert cross_sectional == {"s2-cross-sectional-momentum", "s10-relative-strength-leader"}
    assert cross_sectional <= retired_scan_ids(STRATEGY_MANIFEST)


def test_a_retired_strategy_with_no_watermark_at_all_still_reads_retired() -> None:
    """⚠ Codex ckpt-2 raised this as a P2 defect — "the `basis_date is None` branch
    fires first, so a never-scanned retired strategy reports `never_scanned`".

    It does not: the retirement check sits AHEAD of that branch. But nothing
    asserted the case, so the rebuttal was unevidenced — a wrong finding pointing at
    a real coverage hole. This is the evidence, and it also pins the ordering
    against a future edit that moves the check down.

    The state is reachable: a fresh deployment, or a purged watermark history.
    """
    results = assess_scan_freshness(
        current_versions={"s1-time-series-momentum": "v1"},
        watermarks={},
        trading_dates=[date(2026, 8, 20)],
        retired_ids=frozenset({"s1-time-series-momentum"}),
    )
    assert [item.status for item in results] == ["retired"]
    assert results[0].frontier_date is None
    assert results[0].is_alerting is False

    # Control: the same inputs without the retirement say `never_scanned`, so the
    # assertion above is about retirement and not about the empty watermark map.
    control = assess_scan_freshness(
        current_versions={"s1-time-series-momentum": "v1"},
        watermarks={},
        trading_dates=[date(2026, 8, 20)],
    )
    assert [item.status for item in control] == ["never_scanned"]
