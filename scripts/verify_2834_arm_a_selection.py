"""Open #2834 ARM A's tilt-ETF COST-BAR verdict without tuning.

ARM A asks one question: is the p75 full round-trip spread on IUMO.L (momentum),
IUQA.L (quality) and R1VL.L (value) under 50 bps, on a product proved to be a
real long at x1?  Before five complete common UTC dates exist this script
reports readiness only -- it does not compute or reveal a candidate spread
statistic.

⚠ A pass is a COST-BAR verdict, not an executability proof.  It does not show
the order path accepts an LSE listing; session handling, minimum size and venue
hours are unchecked here.

⚠⚠ The declaration is POST-HOC and says so in its own ``declaration_basis``
field.  ARM A's observations began 2026-08-24, so no #2829 preregistration claim
is available retroactively and none is made.  What holds is narrower and is
written out in ``docs/proposals/ta/2026-09-16-arm-a-tilt-selection.md`` section
3: no spread statistic has been read, every threshold and method field is copied
from an artefact predating the first observation, and where two boundaries
existed the stricter already-frozen one was taken -- the one that does NOT
permit a readout on the declaration date.

The rule itself is #2833's, shared rather than copied
(``scripts/_core_selection_rule.py``).  ARM A differs only in its declaration:
candidate set, a 50 bps bar instead of 60, and ``verdict_mode =
"per_candidate"`` -- a tilt sleeve holds momentum AND quality AND value, so
naming a cheapest "winner" would be a misleading output rather than a verdict.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Final

from scripts._core_selection_rule import run_verifier

DECLARATION_PATH: Final = Path("docs/proposals/ta/2026-09-16-arm-a-tilt-selection-declaration.json")
# Filled from the exact declaration bytes at freeze time.
DECLARATION_SHA256: Final = "ccd8adf5d113697ab83d688c869a181932a5cfe138a89c45b81d9b4f30f8ff2b"
VERIFIER_PATH: Final = Path("scripts/verify_2834_arm_a_selection.py")
RULE_PATH: Final = Path("scripts/_core_selection_rule.py")


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
    "VERIFIER_PATH",
    "main",
]
