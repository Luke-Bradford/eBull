"""What "the current result identity" means, in one place (#2770).

Extracted from ``app.api.strategies``, which owned both of these and was the only
reader. It is no longer the only reader: ``strategy_operator_promotion`` binds the
same pins when it assembles the promotion denominator, and a service may not import
an API module.

The extraction is the point rather than a side effect. ``current_identity_pins``'s
own reason for existing is that two readers must not drift; a third reader that
copied the dict would defeat it exactly as a second one would have.
"""

from __future__ import annotations

from typing import Final

from app.services.backtest_run import BACKTEST_UNIVERSE, corpus_version_for
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_ambiguity_policy import AMBIGUITY_RULE_VERSION
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import TOTAL_RETURN_BASIS

#: The stored-result namespace these pins describe. Named because three readers
#: assert on it and a string literal in three files is how the drift starts.
HOLD_OUT_NAMESPACE: Final = "hold_out"


def current_result_versions() -> dict[str, str]:
    """The identities STORED RESULTS carry — the backtest measurement basis.

    ⚠ NOT the identities the live scan writes. See
    ``app.api.strategies._current_scan_versions``, which stamps ``SCAN_UNIVERSE``
    and is therefore a DISJOINT set — the intersection over the full manifest is
    0. Route ``strategy_results*`` through this function and scan relations
    through that one.
    """
    return {
        strategy_id: entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        for strategy_id, entry in STRATEGY_MANIFEST.items()
    }


def current_identity_pins() -> dict[str, str]:
    """The pins a stored row must match to sit on today's measurement basis.

    Exactly the equality predicates ``_RESULTS_SQL`` applies, named once so the
    prior-version reader cannot drift from the current-version one. These ARE the
    result identity — differing on any of them means the numbers are not
    comparable, which is why the reader reports the difference instead of either
    hiding the version or splicing its figures in.

    ⚠ THIS IS ALSO THE CROSS-ROW COHERENCE RULE (#2770). A promotion denominator
    assembled from rows that differ on any pin would be label-complete — six
    windows, four arms, no gaps — while mixing results measured against different
    corpora, cost models or rule sets. Binding the whole dict is what makes
    "complete matrix" mean "one comparable matrix"; there is no separate coherence
    check, and there must not be a second copy of this list.
    """
    return {
        "namespace": HOLD_OUT_NAMESPACE,
        "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
        "cost_model_id": COST_MODEL_ID,
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
        "return_basis": TOTAL_RETURN_BASIS,
        "ambiguity_rule_version": AMBIGUITY_RULE_VERSION,
        "position_rule_set_version": POSITION_RULE_SET_VERSION,
        "outcome_rule_set_version": OUTCOME_RULE_SET_VERSION,
        "input_rule_set_version": QUARANTINE_RULE_SET_VERSION,
    }


__all__ = ["HOLD_OUT_NAMESPACE", "current_identity_pins", "current_result_versions"]
