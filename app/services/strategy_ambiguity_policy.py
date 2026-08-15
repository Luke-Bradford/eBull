"""Pure §3.4 ambiguity policy shared by result identity, writer and replay."""

from __future__ import annotations

import math
from typing import Final

from app.services.random_entry_cohort import SyntheticControl

#: The pre-#2747 writer had no way to attach §3.4's right-hand side. It
#: remains named so historical result hashes can be reconstructed, but replay
#: deliberately does not recognise it as today's rule.
LEGACY_AMBIGUITY_RULE_VERSION: Final = "ambiguity-verdict-2026-08-13-v1-no-cohort-threshold"

#: #2747 supplies the matched-control margin before run 98349's outcomes were
#: visible. A real arm pair has two matched controls, so the shared ceiling is
#: the SMALLER positive margin above each arm's own cohort p95. Borrowing the
#: better arm's larger margin could admit a pair the weaker arm cannot support.
AMBIGUITY_RULE_VERSION: Final = "ambiguity-verdict-2026-08-15-v2-matched-control-margin"


def matched_control_margin(
    best: SyntheticControl | None,
    worst: SyntheticControl | None,
    *,
    best_case_sharpe: float,
    worst_case_sharpe: float,
) -> float | None:
    """§3.4's shared ceiling from the exact two arm-matched controls.

    Missing controls or an arm that does not strictly clear its own cohort are
    an honestly unmade comparison and return ``None``. Metadata disagreement is
    different: it means two unlike nulls were presented as one pair, which is
    an integrity failure and raises rather than becoming a polite gate refusal.
    """
    if best is None or worst is None:
        return None
    metadata = (
        (best.model_id, worst.model_id, "model id"),
        (best.cohort_size, worst.cohort_size, "cohort size"),
        (best.root_seed, worst.root_seed, "root seed"),
        (best.sharpe_percentile, worst.sharpe_percentile, "Sharpe percentile"),
    )
    for left, right, label in metadata:
        if left != right:
            raise ValueError(f"ambiguity controls disagree on {label}: {left!r} != {right!r}")
    if best.strategy_sharpe != best_case_sharpe or worst.strategy_sharpe != worst_case_sharpe:
        raise ValueError("ambiguity controls do not describe the Sharpes carried by their arm measurements")
    values = (
        best_case_sharpe,
        worst_case_sharpe,
        best.cohort_sharpe_threshold,
        worst.cohort_sharpe_threshold,
    )
    if not all(math.isfinite(value) for value in values):
        return None
    margins = (
        best_case_sharpe - best.cohort_sharpe_threshold,
        worst_case_sharpe - worst.cohort_sharpe_threshold,
    )
    if any(margin <= 0 for margin in margins):
        return None
    return min(margins)


__all__ = ["AMBIGUITY_RULE_VERSION", "LEGACY_AMBIGUITY_RULE_VERSION", "matched_control_margin"]
