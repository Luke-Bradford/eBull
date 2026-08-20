"""``METRIC_SET_ID`` must agree between the producer and the page that reads it.

Pure-logic (no DB, no app boot). Same shape as
``test_correction_kind_vocab_contract`` and the same recurring class the prevention
log calls "contract-field wired into one model not its sibling".

``StrategiesPage.tsx`` decides what an EMPTY holding-period cell means by comparing
``metric_set_id`` against a literal it declares itself:

  * ``criterion7-v2`` with no median  -> "No completed trades"
  * anything else                     -> "Result version <id>", i.e. written before
                                         the measurement existed

Those two sentences are not interchangeable — one says the strategy closed nothing,
the other says we never measured it. A backend rename of ``METRIC_SET_ID`` would send
every freshly-written row down the second branch, telling the operator the current
result version is unmeasured. Nothing else would fail: TypeScript cannot see a Python
constant, and the branch is only reachable on data that does not exist yet (#2623
populates the columns forward only), so no fixture would catch it either.

Read as text rather than by importing anything from the frontend, for the same reason
the sibling test reads its producer by AST: what matters is that the literal in the
shipped file agrees, not that some render path happened to exercise it.
"""

from __future__ import annotations

import re
from pathlib import Path

from app.services.strategy_statistics import METRIC_SET_ID

_REPO = Path(__file__).resolve().parents[1]
_PAGE = _REPO / "frontend" / "src" / "pages" / "StrategiesPage.tsx"

_DECLARATION = re.compile(r'const HOLD_PERIOD_METRIC_SET = "([^"]+)";')


def test_frontend_hold_period_metric_set_matches_the_producer() -> None:
    source = _PAGE.read_text()
    match = _DECLARATION.search(source)
    assert match is not None, (
        f"{_PAGE.relative_to(_REPO)} no longer declares HOLD_PERIOD_METRIC_SET. If the constant moved, "
        "move this assertion with it — do not delete it: it is the only thing tying the page's "
        "'not measured for this result version' branch to the producer."
    )
    assert match.group(1) == METRIC_SET_ID, (
        f"{_PAGE.relative_to(_REPO)} pins HOLD_PERIOD_METRIC_SET to {match.group(1)!r} but "
        f"strategy_statistics.METRIC_SET_ID is {METRIC_SET_ID!r}. While these disagree the page reports "
        "every row written under the CURRENT version as 'not measured for this result version', which is "
        "a different claim from 'no completed trades' and is false."
    )
