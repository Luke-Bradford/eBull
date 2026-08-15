"""#2721 — the termination rule's class table and realisation fractions.

The module ships UNWIRED (no execution path calls it until step 3 wires the
``BACKTEST_UNIVERSE`` parameterisation), so these tests ARE its whole
behavioural surface for now.
"""

from __future__ import annotations

import pytest

from app.services.series_termination import (
    SHUMWAY_HAIRCUT,
    TERMINATION_RULE_VERSION,
    TWO_ARMED_CLASSES,
    TerminationClass,
    TerminationEvidence,
    classify_termination,
    terminal_value_fraction,
)


def _evidence(**overrides: object) -> TerminationEvidence:
    base: dict[str, object] = {"linked": True, "provision": "(b)", "q_suffix": False}
    base.update(overrides)
    return TerminationEvidence(**base)  # type: ignore[arg-type]


# --- the class table -------------------------------------------------------


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (_evidence(provision="(b)"), TerminationClass.EXCHANGE_FAILURE),
        (_evidence(provision="(a)(4)"), TerminationClass.EXCHANGE_FAILURE_A4),
        (_evidence(provision="(a)(3)"), TerminationClass.OPERATION_OF_LAW),
        (_evidence(provision=None), TerminationClass.LINKED_UNPARSED),
        (
            _evidence(linked=False, provision=None, q_suffix=True),
            TerminationClass.Q_SUFFIX_OTC,
        ),
        (_evidence(linked=False, provision=None), TerminationClass.UNKNOWN),
    ],
)
def test_class_table(evidence: TerminationEvidence, expected: TerminationClass) -> None:
    assert classify_termination(evidence) == expected


def test_a4_is_a_distinct_label_from_b() -> None:
    # "(a)(4) ≈ (b)" is asserted, not demonstrated (ckpt-1). The two realise
    # identically TODAY, but collapsing the labels would make the
    # expanded-register verification unmeasurable.
    a4 = classify_termination(_evidence(provision="(a)(4)"))
    b = classify_termination(_evidence(provision="(b)"))
    assert a4 != b
    assert terminal_value_fraction(a4, None) == terminal_value_fraction(b, None)


def test_form25_link_outranks_the_q_suffix() -> None:
    # The filing is a regulator's statement about this security; the suffix
    # is a naming convention. A linked (a)(3) that happens to carry a Q must
    # realise as the conversion, not as the OTC heuristic.
    evidence = _evidence(provision="(a)(3)", q_suffix=True)
    assert classify_termination(evidence) == TerminationClass.OPERATION_OF_LAW


# --- realisation fractions -------------------------------------------------


def test_failure_classes_take_the_adverse_shumway_anchor() -> None:
    # Venue is unknown for dead names, so the Nasdaq −55% (Shumway & Warther
    # 1999) binds, not the NYSE/AMEX −30% (Shumway 1997 Table V) — the same
    # pessimistic-end construction as UNKNOWN_NOMINAL_PRICE_BAND.
    assert terminal_value_fraction(TerminationClass.EXCHANGE_FAILURE, None) == pytest.approx(0.45)
    assert SHUMWAY_HAIRCUT == pytest.approx(0.55)


def test_conversion_and_qsuffix_realise_at_last_close() -> None:
    assert terminal_value_fraction(TerminationClass.OPERATION_OF_LAW, None) == 1.0
    assert terminal_value_fraction(TerminationClass.Q_SUFFIX_OTC, None) == 1.0


@pytest.mark.parametrize("termination_class", sorted(TWO_ARMED_CLASSES))
def test_two_armed_classes_span_the_bounds(termination_class: TerminationClass) -> None:
    best = terminal_value_fraction(termination_class, "best_case")
    worst = terminal_value_fraction(termination_class, "worst_case")
    assert best == 1.0
    assert worst == pytest.approx(1.0 - SHUMWAY_HAIRCUT)


@pytest.mark.parametrize("termination_class", sorted(TWO_ARMED_CLASSES))
def test_two_armed_classes_refuse_a_missing_arm(termination_class: TerminationClass) -> None:
    # Silently picking a side is exactly the unstated survivorship treatment
    # this module exists to abolish.
    with pytest.raises(ValueError):
        terminal_value_fraction(termination_class, None)


def test_single_valued_classes_ignore_the_arm() -> None:
    # A caller may pass its running arm unconditionally.
    assert terminal_value_fraction(TerminationClass.OPERATION_OF_LAW, "worst_case") == 1.0
    assert terminal_value_fraction(TerminationClass.EXCHANGE_FAILURE, "best_case") == pytest.approx(0.45)


def test_version_carries_rule_set_id_and_code_hash() -> None:
    rule_set, _, code_hash = TERMINATION_RULE_VERSION.partition("+")
    assert rule_set == "series-termination-v1"
    assert len(code_hash) == 12
