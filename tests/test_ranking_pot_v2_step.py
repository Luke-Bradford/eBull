"""#3592 slice 4a — ranking-pot-v2's step job, the pure parts (spec §4 "Step job", §5, §6; Appendix A 60, 61, 76, 80).

- The ``Rebalance`` adapter's orders: §5 for the shadow and the variant, §6 for a control, v1's for the reference.
- Appendix A 76/80: the identity permutation reproduces the shadow; BOTH new components constant reproduce the
  reference (one does not); recipients keep their own score and take the donor's pair, donors may lie outside R; the
  variant takes the shadow's order; π_k is the same at every rebalance.
- Snapshot-only replay: the order is re-derived from the stored document and the declaration alone.
- The per-control missing-donor-DTC count, the shadow's per-entry reasons, the K + 3 layout, and import isolation.
"""

from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import pytest

from app.services import ranking_pot as pot
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_job as job
from app.services import ranking_pot_v2_step as st
from app.services.ai_trial_pack import canonical_json
from app.services.ranking_pot_v2_policy import RANKING_POT_V2_POLICY_HASH
from tests.test_ranking_pot_rebalance import _inputs as v1_inputs
from tests.test_ranking_pot_step import FLAT, _universes, table_of
from tests.test_ranking_pot_v2 import FULL_HISTORY, _dtc, _history, _row

ROOT = Path(__file__).resolve().parents[1]
D = Decimal
THU, FRI = date(2026, 10, 1), date(2026, 10, 2)
SETTLE, KNOWN = date(2026, 9, 15), datetime(2026, 9, 25, 12, tzinfo=UTC)


def _rebalance(r: dict[int, str], f: set[int], dtc: dict[int, str], buyers: set[int]) -> st.Rebalance:
    """A decided rebalance with synthetic universes and §5 inputs (``dtc`` and ``buyers`` cover S₀)."""
    return st.Rebalance(
        attempt_id=7,
        target_session=FRI,
        last_session=THU,
        universes=_universes(r, f),
        half_spread={iid: D("0.001") for iid in r},
        snapshot_close={iid: D(100) for iid in f},
        dtc={iid: Fraction(D(v)) for iid, v in dtc.items()},
        buyers=frozenset(buyers),
    )


def _bars(session: date, bars: dict[int, sim.Bar | None], closes: dict[int, dict[date, Decimal]]) -> st.SessionBars:
    return st.SessionBars(session, bars, closes, sim.Bar(D(500), D(501), D(499), D(500)))


def _book(n: int = 2) -> st.Book:
    return st.Book(replace(sim.new_book(n), last_session=THU))


R5 = {1: "0.9", 2: "0.8", 3: "0.7", 4: "0.6", 5: "0.5"}


def test_each_book_takes_its_own_order() -> None:
    # Name 5 has the lowest score but the lowest DTC and an opportunistic buyer: v2 lifts it, v1 does not.
    reb = _rebalance(R5, set(R5), {1: "9", 2: "8", 3: "7", 4: "6", 5: "1"}, {5})
    shadow = reb.order_for(None)
    assert shadow == v2.real_order(reb.universes, reb.dtc, reb.buyers)
    assert reb.order_for(None, order="reference") == pot.real_order(reb.universes) == (1, 2, 3, 4, 5)
    assert shadow[0] == 5 and shadow != pot.real_order(reb.universes)
    # The identity permutation reproduces the shadow (Appendix A 76).
    assert reb.order_for({i: i for i in R5}) == shadow
    with pytest.raises(ValueError, match="no donor"):
        reb.order_for({i: i for i in R5}, order="reference")


def test_both_new_components_constant_reproduce_the_reference_and_one_does_not() -> None:
    reference = pot.real_order(_universes(R5, set(R5)))
    assert _rebalance(R5, set(R5), {}, set()).order_for(None) == reference  # no DTC, no buyer
    assert _rebalance(R5, set(R5), dict.fromkeys(R5, "3"), set(R5)).order_for(None) == reference  # all equal
    # One constant (no buyer) does not: DTC alone reorders (R2-15).
    assert _rebalance(R5, set(R5), {1: "9", 2: "1", 3: "5", 4: "5", 5: "5"}, set()).order_for(None) != reference


def test_a_control_member_keeps_its_score_and_takes_its_donors_pair_from_outside_r() -> None:
    # S₀ = 1..7; 6 and 7 are outside R. 1 takes 6's (low DTC, buyer); 2 takes 7's (no DTC).
    reb = _rebalance(R5, set(R5), {1: "9", 2: "8", 3: "7", 4: "6", 5: "5", 6: "1"}, {6})
    donor_of = {1: 6, 2: 7, 3: 3, 4: 4, 5: 5, 6: 1, 7: 2}
    dtc_k, buyers_k = v2.donated(reb.universes, donor_of, reb.dtc, reb.buyers)
    assert dtc_k == {1: Fraction(1), 3: Fraction(7), 4: Fraction(6), 5: Fraction(5)} and buyers_k == {1}
    assert reb.order_for(donor_of) == v2.order_of(
        v2.composite_scores(reb.universes.r_ids, reb.universes.own_score, dtc_k, buyers_k)
    )
    assert reb.order_for(donor_of)[0] == 1
    assert reb.missing_donors(donor_of) == 1  # only 2's donor (7) has no DTC


def test_the_variant_takes_the_shadows_order_and_the_reference_v1s() -> None:
    reb = _rebalance(R5, set(R5), {1: "9", 2: "8", 3: "7", 4: "6", 5: "1"}, {5})
    bars = _bars(FRI, dict.fromkeys(R5, FLAT), {i: {THU: D(100)} for i in R5})
    decisions = {}
    for name, kw in {
        "shadow": {},
        "variant": {"protective": False},
        "reference": {"order": "reference"},
    }.items():
        _, _, decision = st.advance(
            _book(), session=FRI, bars=bars, rebalance=reb, donor_of=None, wind=False, used=set(), **kw
        )
        assert decision is not None
        decisions[name] = decision.entries
    assert decisions["shadow"] == decisions["variant"] == (5, 1)
    assert decisions["reference"] == (1, 2)


def test_the_drawer_is_the_stratified_draw_and_persists_across_rebalances() -> None:
    strata = {i: i % 2 for i in range(1, 13)}
    donor = st.control_drawer(strata, declaration_sha256="a" * 64, first_snapshot_sha256="b" * 64)
    base = sim.control_seed("a" * 64, "b" * 64)
    assert donor(5) == v2.draw_stratified(strata, base_seed=base, k=5) == donor(5)
    assert all(strata[d] == strata[r] for r, d in donor(5).items())


def test_explain_reproduces_the_composite_and_names_the_purchases() -> None:
    buy = _row(txn_date=date(2026, 5, 4), iid=5)
    history = _history({2023: [3], 2024: [4], 2025: [5]})
    s0 = set(R5)
    as_of = datetime(2026, 10, 1, 23, 50, tzinfo=UTC)
    ins = v2.insider_read([buy], history, s0_ids=s0, target_session=FRI, as_of=as_of, history_floor=FULL_HISTORY)
    dtc = v2.dtc_read([_dtc(i, SETTLE, str(10 - i), KNOWN) for i in (1, 2, 3, 5)], s0_ids=s0, as_of=as_of)
    reb = replace(
        _rebalance(R5, set(R5), {}, set()),
        dtc=dtc.values,
        buyers=ins.buyers,
        dtc_read=dtc,
        insider=ins,
        purchases=(buy,),
    )
    composite = v2.composite_scores(reb.universes.r_ids, reb.universes.own_score, reb.dtc, reb.buyers)
    [five, four] = reb.explain([5, 4])
    assert five["composite"] == str(composite[5]) and four["composite"] == str(composite[4])
    assert Fraction(five["u_score"]) + Fraction(five["u_dtc"]) + Fraction(five["u_ins"]) == 3 * composite[5]
    assert (five["dtc"], five["dtc_missing"], five["dtc_settlement_date"]) == ("5", None, "2026-09-15")
    assert five["accessions"] == [buy.accession]
    assert five["pairs"] == [[buy.filer_cik, buy.issuer_cik, 2026, "opportunistic", [[2023, 3], [2024, 4], [2025, 5]]]]
    assert (four["dtc"], four["dtc_missing"], four["accessions"], four["pairs"]) == (None, "no_row", [], [])
    with pytest.raises(rb.SnapshotIntegrityError, match="v2 reads"):
        _rebalance(R5, set(R5), {}, set()).explain([1])
    with pytest.raises(rb.SnapshotIntegrityError, match="outside R_t"):
        reb.explain([5, 99])


def _v2_doc(monkeypatch: pytest.MonkeyPatch, *, policy_hash: str = RANKING_POT_V2_POLICY_HASH) -> dict[str, Any]:
    """A real stored v2 snapshot: v1's document (``tests.test_ranking_pot_rebalance``: S₀ 1..5, R {1, 2}, F {1},
    target 2026-10-01) under v2's identity, with a ``v2`` block in which 2 is an opportunistic buyer with the lower
    DTC (its pair traded 2023-03, 2024-04, 2025-04: routine only if a hidden 2023-04 is filled), read back through
    JSON as JSONB returns it."""
    inputs = replace(v1_inputs(monkeypatch), policy_hash=policy_hash)
    block = v2.V2Inputs(
        purchases=(_row(txn_date=date(2026, 5, 4), iid=2),),
        history=tuple(_history({2023: [3], 2024: [4], 2025: [4]})),
        dtc=(_dtc(1, SETTLE, "9", KNOWN), _dtc(2, SETTLE, "1", KNOWN)),
        reader_counts={},
    )
    doc = v2.with_v2_block({**rb.encode_snapshot(inputs), "strategy_id": v2.STRATEGY_ID}, block)
    return json.loads(canonical_json(doc))


def test_snapshot_only_replay_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    doc = _v2_doc(monkeypatch)
    a = st.Rebalance.of(11, doc, s0_ids=(1, 2, 3, 4, 5), history_floor=FULL_HISTORY)
    b = st.Rebalance.of(11, json.loads(json.dumps(doc)), s0_ids=(1, 2, 3, 4, 5), history_floor=FULL_HISTORY)
    assert a.buyers == b.buyers == {2} and a.dtc == b.dtc == {1: Fraction(9), 2: Fraction(1)}
    assert a.order_for(None) == b.order_for(None) == (2, 1)  # restart determinism: the same bytes, the same order
    assert a.order_for(None, order="reference") == (1, 2)
    # The frozen floor is the declaration's: hiding 2023 makes the buyer's pair indeterminate.
    hidden = st.Rebalance.of(11, doc, s0_ids=(1, 2, 3, 4, 5), history_floor=date(2023, 6, 1))
    assert hidden.buyers == frozenset() and hidden.order_for(None) == (1, 2)


def test_only_a_v2_snapshot_under_v2s_hash_decodes(monkeypatch: pytest.MonkeyPatch) -> None:
    doc = _v2_doc(monkeypatch)
    with pytest.raises(rb.SnapshotIntegrityError, match="not a ranking-pot-v2 snapshot"):
        st.Rebalance.of(11, {**doc, "strategy_id": "ranking-pot-v1"}, s0_ids=(1, 2), history_floor=FULL_HISTORY)
    with pytest.raises(rb.SnapshotIntegrityError, match="v2's policy hash"):
        st.Rebalance.of(11, _v2_doc(monkeypatch, policy_hash="e" * 64), s0_ids=(1, 2), history_floor=FULL_HISTORY)
    with pytest.raises(ValueError, match="no ranking-pot-v2"):
        st.Rebalance.of(11, {k: v for k, v in doc.items() if k != "v2"}, s0_ids=(1, 2), history_floor=FULL_HISTORY)
    # The rebalance job's own snapshot builder produces a document this decoder accepts.
    assert job.snapshot_of(replace(v1_inputs(monkeypatch)), v2.v2_block_of(doc))["policy_hash"] == doc["policy_hash"]


def test_the_layout_is_k_plus_3_and_the_declaration_must_carry_it() -> None:
    assert (st.variant_book(9999), st.reference_book(9999), st.book_count(9999)) == (10000, 10001, 10002)

    def decl(terms: dict[str, Any]) -> rb.PotDeclaration:
        return rb.PotDeclaration(1, {"terms": terms}, "a" * 64, datetime(2026, 10, 3, tzinfo=UTC), "shadow_only")

    assert st.book_terms(decl({"n": 25, "k_controls": 2, "book_count": 5})) == (25, 2)
    for bad in ({"n": 25, "k_controls": 2, "book_count": 4}, {"n": 25, "k_controls": 2}):
        with pytest.raises(rb.SnapshotIntegrityError, match="K \\+ 3"):
            st.book_terms(decl(bad))


def test_documents_carry_the_reasons_and_the_dtc_donor_count() -> None:
    reb = _rebalance(R5, set(R5), {1: "9", 2: "8", 3: "7", 4: "6", 5: "1"}, {5})
    bars = _bars(FRI, dict.fromkeys(R5, FLAT), {i: {THU: D(100)} for i in R5})
    _, result, decision = st.advance(
        _book(), session=FRI, bars=bars, rebalance=reb, donor_of=None, wind=False, used=set()
    )
    assert decision is not None
    reasons = [{"instrument_id": iid} for iid in decision.entries]
    doc = st.shadow_doc(result, decision, FRI, table_of(set(R5)), reasons)
    assert doc["decision"]["v2_entries"] == reasons
    assert "v2_entries" not in st.shadow_doc(result, decision, FRI, table_of(set(R5)))["decision"]
    cols = st.ControlColumns()
    cols.add_decision(decision, 3)
    assert cols.doc()["decision"]["missing_donor_dtc"] == [3] and "missing_donors" not in cols.doc()["decision"]


def _imports(path: Path) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module.rsplit(".", 1)[-1])
            if node.module == "app.services":
                out.update(a.name for a in node.names)
    return out


def test_neither_step_imports_the_other() -> None:
    services = ROOT / "app" / "services"
    v2_imports = _imports(services / "ranking_pot_v2_step.py")
    assert not {"ranking_pot_step", "ranking_pot_exits", "ranking_pot_job", "ranking_pot_exec"} & v2_imports
    assert not {m for m in _imports(services / "ranking_pot_step.py") if m.startswith("ranking_pot_v2")}


def test_the_step_minute_is_registered_on_the_rebalances_lane() -> None:
    from app.jobs.sources import source_for
    from app.workers import scheduler

    registered = next(j for j in scheduler.SCHEDULED_JOBS if j.name == scheduler.JOB_RANKING_POT_V2_STEP)
    assert (registered.source, registered.cadence.kind, registered.cadence.minute) == ("db", "hourly", 25)
    assert source_for(scheduler.JOB_RANKING_POT_V2_STEP) == source_for(scheduler.JOB_RANKING_POT_V2_REBALANCE)
    assert scheduler.RANKING_POT_V2_STEP_FIRE_MINUTE not in (
        scheduler.RANKING_POT_FIRE_MINUTE,
        scheduler.RANKING_POT_STEP_FIRE_MINUTE,
        scheduler.RANKING_POT_EXECUTE_FIRE_MINUTE,
        scheduler.RANKING_POT_V2_FIRE_MINUTE,
    )
