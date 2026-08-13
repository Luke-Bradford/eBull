"""#2625 — the promotion transition's replay policy, and the guard that keeps it honest.

The policy itself is prose; these tests are what make it binding. Pure — the
policy is a literal mapping and ``PromotionCandidate`` is a dataclass, so
nothing here needs Postgres.
"""

from __future__ import annotations

from app.services.strategy_promotion_replay import (
    REPLAY_TEMPORAL_POLICY,
    unclassified_candidate_fields,
    unenforced_candidate_fields,
)


class TestEveryGateInputIsClassified:
    def test_no_promotion_candidate_field_is_unclassified(self) -> None:
        """⚠⚠ THE COUPLING GUARD. Adding an input to ``PromotionCandidate``
        without declaring its temporal rule fails HERE.

        The transition cannot call ``check_promotable`` — it has no
        ``StrategyResult`` to hand it and two of the gate's inputs cost an
        audited read — so every input is replayed individually or not at all.
        That design only stays coherent while each input has a written rule;
        #2625 exists because five of them did not.

        ⚠ KEYED ON FIELDS, NOT ON REFUSAL CODES, and the distinction is the
        whole strength of the guard. Codes are a many-to-many projection of the
        inputs (one input emits several; several emit one), so a code-keyed
        assertion is fully satisfied by a new input that reuses an existing
        code. Fields are what a new gate input actually arrives as.
        """
        assert unclassified_candidate_fields() == frozenset(), (
            "a PromotionCandidate field has no entry in REPLAY_TEMPORAL_POLICY — decide whether the "
            "transition replays it FROZEN, replays it against TODAY, or does not re-read it, and record the "
            "reason in app/services/strategy_promotion_replay.py"
        )

    def test_the_policy_classifies_nothing_that_is_not_a_gate_input(self) -> None:
        # The other direction: a stale entry for a field that no longer exists
        # would make the guard above pass while describing a gate that is gone.
        import dataclasses

        from app.services.strategy_result import PromotionCandidate

        assert set(REPLAY_TEMPORAL_POLICY) == {f.name for f in dataclasses.fields(PromotionCandidate)}

    def test_every_entry_carries_a_reason(self) -> None:
        for name, entry in REPLAY_TEMPORAL_POLICY.items():
            assert entry.reason.strip(), f"{name} is classified without a reason"
            assert entry.source.strip(), f"{name} is classified without a source"


class TestTodayIsRestricted:
    def test_only_declared_validity_windows_replay_against_today(self) -> None:
        """⚠ A today-rule is legitimate ONLY where the record declares its own
        window or is explicitly supersedable.

        Everywhere else it is an undeclared freshness rule, which is how a
        result that passed once silently stops passing — #2621's reason for
        freezing the universe rather than re-loading it. Pinning the set means
        a future author cannot quietly add a second today-check.
        """
        today = {name for name, entry in REPLAY_TEMPORAL_POLICY.items() if entry.rule == "today"}
        assert today == {"promotion_evidence"}, (
            "a new input replays against TODAY — justify it by naming the validity window the stored record "
            "declares (promotion_evidence declares cost_observed_on / cost_valid_through), or classify it frozen"
        )


class TestTheGapIsCounted:
    def test_the_unenforced_set_is_exactly_the_known_gap(self) -> None:
        """⚠⚠ NOT_RE_READ IS A GAP, NOT COVERAGE — and this pins its size.

        These three inputs are neither persisted nor re-derived, so
        ``promote_strategy`` still trusts a write-time verdict that died with
        ``WrittenRow`` — the exact defect #2621 was filed about, surviving where
        re-reading has a governance cost. The set is asserted rather than
        described so that it cannot GROW unnoticed: a new unenforced input is a
        test failure, and shrinking it is the follow-up work.
        """
        assert unenforced_candidate_fields() == frozenset(
            {"holdout_evaluations", "recorded_accesses", "quarantine_arms_compared"}
        )

    def test_every_unenforced_field_is_classified_not_re_read(self) -> None:
        # The two axes must agree: an input the transition does not replay is
        # not "frozen", it is unenforced, and calling it frozen would read as
        # coverage.
        for name in unenforced_candidate_fields():
            assert REPLAY_TEMPORAL_POLICY[name].rule == "not_re_read", (
                f"{name} is not replayed but is classified {REPLAY_TEMPORAL_POLICY[name].rule!r} — "
                "a rule the transition does not apply is a claim it does not honour"
            )

    def test_every_replayed_field_has_a_real_source(self) -> None:
        for name, entry in REPLAY_TEMPORAL_POLICY.items():
            if entry.replayed_at_transition:
                assert not entry.source.startswith("none"), f"{name} claims a replay with no source"
