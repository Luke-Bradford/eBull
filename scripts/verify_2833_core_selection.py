"""Open #2833's prospective core-sleeve cost verdict without tuning.

The declaration starts after the already-seen 2026-08-24 observations.  Before
five complete common UTC dates exist this script reports readiness only: it does
not compute or reveal a candidate spread statistic.

⚠ The RULE now lives in ``scripts/_core_selection_rule.py`` (#2834 ARM A,
2026-09-16), which needed the same percentile/population/missingness/FX/
eligibility logic against a different candidate set and bar.  It was EXTRACTED,
not rewritten, and the extraction is provably non-behaviour-changing for this
declaration: ``pass_bar_bps = "60"`` renders the same ``cost_above_60_bps``
label the literal produced, ``binding_percentile``/``descriptive_percentile``
carry the ``"0.75"``/``"0.50"`` the literals used, and ``verdict_mode`` is
emitted only in ``per_candidate`` mode so this payload is unchanged.
``tests/test_2833_core_selection_verdict.py`` is deliberately untouched by that
change -- it imports from this module and its continuing to pass is part of the
evidence.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from scripts._core_selection_rule import (
    CandidateVerdict,
    Eligibility,
    Observation,
    Verdict,
    assert_verifier_sources_clean,
    percentile_cont,
    run_verifier,
    sha256_of,
)
from scripts._core_selection_rule import evaluate as _evaluate
from scripts._core_selection_rule import load_declaration as _load_declaration

DECLARATION_PATH: Final = Path("docs/proposals/ta/2026-08-24-core-selection-declaration.json")
# Filled from the exact declaration bytes before the prospective boundary.
DECLARATION_SHA256: Final = "5f3929e035c35d25254747512a3327adcec1863b9a5a5eb028706fcb95a66804"
VERIFIER_PATH: Final = Path("scripts/verify_2833_core_selection.py")
RULE_PATH: Final = Path("scripts/_core_selection_rule.py")


def load_declaration(path: Path = DECLARATION_PATH) -> Mapping[str, Any]:
    return _load_declaration(path, DECLARATION_SHA256)


def evaluate(
    observations: Sequence[Observation],
    eligibilities: Mapping[int, Eligibility],
    declaration: Mapping[str, Any],
    *,
    now: Any,
) -> Mapping[str, Any]:
    return _evaluate(
        observations,
        eligibilities,
        declaration,
        now=now,
        declaration_sha256=DECLARATION_SHA256,
    )


def main(argv: Sequence[str] | None = None) -> int:
    return run_verifier(
        description=__doc__,
        declaration_path=DECLARATION_PATH,
        declaration_sha256=DECLARATION_SHA256,
        verifier_path=VERIFIER_PATH,
        source_paths=(DECLARATION_PATH, VERIFIER_PATH, RULE_PATH),
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DECLARATION_PATH",
    "DECLARATION_SHA256",
    "RULE_PATH",
    "CandidateVerdict",
    "Eligibility",
    "Observation",
    "VERIFIER_PATH",
    "Verdict",
    "assert_verifier_sources_clean",
    "evaluate",
    "load_declaration",
    "main",
    "percentile_cont",
    "sha256_of",
]
