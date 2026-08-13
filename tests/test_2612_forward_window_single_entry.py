"""#2612 — the live gate's two evidence windows are SINGLE-ENTRY by construction.

`assess_live_gate` anchors the forward window on
``max(promoted_at) FILTER (WHERE to_stage='forward_observation')`` and the paper
window on the same aggregate over ``paper_enabled``.  #2612 read that as a
splice bug: *"a strategy promoted to forward_observation twice measures its
forward evidence from the SECOND promotion only"*.

**That premise is false, and these tests are why.**  A version cannot arrive at
either anchor twice, so `max` aggregates over at most one row and there is no
splice-versus-accumulate policy to choose.  Two independent barriers enforce it:

1. `strategy_control_plane._NEXT_STAGE` is a DAG with no back-edge — the only
   edge into `forward_observation` leaves `historical_validated`, and the exits
   (`paused` → `retired` → nothing) never return.  `promote_strategy` checks it
   under the per-version advisory lock.
2. The partial UNIQUE index `idx_strategy_promotions_one_successor` on
   ``(strategy_id, strategy_version, from_stage)`` — so `historical_validated`
   can be departed exactly once even if the service check were bypassed.

Both barriers are pinned against a real database in the `_db` sibling, because
either alone would make the other's removal silent.  THIS module holds the half
that needs no Postgres — the stage-machine coupling guard and the refusal table
— so that it runs on every push.

⚠ KEEP THIS MODULE FREE OF EVERY DB TOKEN, INCLUDING IN PROSE.
`tests/conftest.py::_module_source_touches_db` auto-applies the `db` marker per
MODULE by substring-matching the connection-fixture name against the raw source
text.  It does not parse: a mention inside a docstring marks the module just as
a real fixture would.  Naming the fixture here — to document this very rule —
is what silently pulled every test below out of the fast tier while this file
was being written.  Refer to it by description, never by spelling it.

Verify with ``pytest <this file> -m "not db" --collect-only -q``, which prints
``path: N``; exit code 5 means nothing was collected.  Do not verify by reading
the file — the whole point is that the marking is invisible in the source.

The runtime refusal covered here
(`forward_window_ambiguous` / `paper_window_ambiguous`) is the fail-closed
backstop for the day someone adds a legitimate re-entry edge: without it, `max`
re-anchors quietly and discards the entire first observation period, restarting
exactly the `forward_days` and `forward_decision_dates` that #2599's
contract-frozen floor reads.
"""

from __future__ import annotations

import pytest

from app.services.strategy_control_plane import _NEXT_STAGE

# Reuse > reinvent: `_facts` is a 27-field dataclass and `_gate` already wires
# the pure refusal table.  Duplicating either here would let the two copies
# drift, which is the defect this module exists to prevent.
from tests.test_prereg_declaration_gate import _facts, _gate

# ---------------------------------------------------------------------------
# The stage machine — the first of the two barriers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("anchor", ["forward_observation", "paper_enabled"])
def test_exactly_one_stage_leads_to_each_live_gate_window_anchor(anchor: str) -> None:
    """⚠ THIS TEST IS A COUPLING GUARD, NOT A TAUTOLOGY.

    `assess_live_gate` derives both window starts with `max(promoted_at)`, which
    is the true window start ONLY while each anchor is reachable from exactly one
    predecessor.  Nothing in `strategy_live_gate` can see `_NEXT_STAGE`, so a
    future re-entry edge (`paused` → `forward_observation` is the plausible one)
    would silently re-anchor the window rather than fail.

    If this test fails you are adding that edge.  That is allowed — but
    `assess_live_gate`'s window derivation must be revisited in the same change,
    and #2612's splice-versus-accumulate question becomes live for the first
    time.
    """
    predecessors = sorted(stage for stage, nexts in _NEXT_STAGE.items() if stage is not None and anchor in nexts)
    assert len(predecessors) == 1, f"{anchor} is now reachable from {predecessors}; see this test's docstring"


def test_no_stage_returns_to_an_earlier_one() -> None:
    """The DAG property the single-entry claim rests on, stated directly.

    ⚠ Named explicitly rather than inferred from the parametrised test above:
    "exactly one predecessor" would still permit a cycle, and a cycle is what
    would let a version re-arrive.  Ordering the stages by their lifecycle rank
    and asserting every edge increases it is the single-row predicate that
    implies the reachability property.
    """
    rank = {
        "research_candidate": 0,
        "historical_validated": 1,
        "forward_observation": 2,
        "paper_enabled": 3,
        "live_enabled": 4,
        "paused": 5,
        "retired": 6,
    }
    assert set(rank) == {stage for stage in _NEXT_STAGE if stage is not None}
    for stage, nexts in _NEXT_STAGE.items():
        if stage is None:
            continue
        for successor in nexts:
            assert rank[successor] > rank[stage], f"back-edge {stage} -> {successor}"


# ---------------------------------------------------------------------------
# The fail-closed backstop in the gate itself
# ---------------------------------------------------------------------------


def test_a_single_entry_window_adds_no_ambiguity_refusal() -> None:
    codes = _gate(facts=_facts(forward_observation_entries=1, paper_enabled_entries=1))
    assert "forward_window_ambiguous" not in codes
    assert "paper_window_ambiguous" not in codes


def test_an_unstarted_window_is_not_reported_as_ambiguous() -> None:
    """Zero arrivals is "not there yet", refused elsewhere by `paper_stage_required`."""
    codes = _gate(facts=_facts(forward_observation_entries=0, paper_enabled_entries=0))
    assert "forward_window_ambiguous" not in codes
    assert "paper_window_ambiguous" not in codes


def test_a_second_forward_arrival_refuses_the_live_gate() -> None:
    codes = _gate(facts=_facts(forward_observation_entries=2))
    assert "forward_window_ambiguous" in codes
    assert "paper_window_ambiguous" not in codes


def test_a_second_paper_arrival_refuses_the_live_gate() -> None:
    codes = _gate(facts=_facts(paper_enabled_entries=2))
    assert "paper_window_ambiguous" in codes
    assert "forward_window_ambiguous" not in codes


def test_the_two_windows_are_named_by_separate_codes() -> None:
    """⚠ TWO CODES, NOT ONE — the convention `live_gate_refusals` already states.

    A corrupted forward window and a corrupted paper window invalidate different
    measurements, so collapsing them to one code would hide whichever fired
    second.
    """
    codes = _gate(facts=_facts(forward_observation_entries=2, paper_enabled_entries=3))
    assert "forward_window_ambiguous" in codes
    assert "paper_window_ambiguous" in codes


def test_an_ambiguous_window_is_refused_even_when_every_floor_reads_as_met() -> None:
    """The case that makes this a refusal rather than a diagnostic.

    A spliced window UNDER-counts, so a strategy whose `forward_days` and
    `forward_decision_dates` clear #2599's floor may be clearing it on a window
    that silently discarded its first observation period.  The floors passing is
    exactly when the ambiguity is most dangerous, so it must still refuse.
    """
    codes = _gate(facts=_facts(forward_observation_entries=2))
    assert "forward_decision_dates_insufficient" not in codes
    assert "forward_calendar_weeks_insufficient" not in codes
    assert "forward_window_ambiguous" in codes


# ---------------------------------------------------------------------------
# The operator-visible mirror
# ---------------------------------------------------------------------------


def test_every_live_gate_fact_survives_into_the_operator_view() -> None:
    """⚠ NOTHING ELSE COVERS THIS SPLAT, AND IT FAILS SILENTLY.

    `_live_gate_view` builds the response with
    ``LiveGateFactsView(**report.facts.__dict__)``.  The view does not set
    ``extra='forbid'``, so pydantic's default is to IGNORE unknown keys: a fact
    added to the dataclass and not to the view is dropped from the API with no
    error, no type failure and no test failure.  #2612 added two fields through
    exactly that splat, and no test constructs `LiveGateResponse` at all.

    Asserted as a set equality over field NAMES rather than by building a
    response, so it stays a pure test and catches drift in both directions —
    a view field with no backing fact would raise at construction instead.
    """
    from dataclasses import fields

    from app.api.strategies import LiveGateFactsView
    from app.services.strategy_live_gate import LiveGateFacts

    assert {f.name for f in fields(LiveGateFacts)} == set(LiveGateFactsView.model_fields)
