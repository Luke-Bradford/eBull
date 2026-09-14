"""Which venue classes this repo can time a submission against (#2312, #2603).

A LEAF module on purpose. The statement "we have a trading-session calendar for
this venue" is needed by the core SUBMISSION path
(``strategy_core_preflight.decide_core_preflight``) and by the core SELECTION
path (``strategy_core_selection.load_core_selection``), and those two cannot
import each other: ``strategy_core_selection`` -> ``strategy_core_preflight`` ->
``strategy_core_mandate`` -> ``strategy_core_selection`` is a real cycle,
measured 2026-09-14. Two copies of the allow-list would drift silently, and the
direction they would drift in is "selection says ready, submission refuses" --
which is only discovered in an operator-attended session.

Source rule: the one trading-session calendar this repo has is
``app/services/market_calendar.py``, whose own docstring scopes it to the
**NYSE** published holiday + early-close calendar
(https://www.nyse.com/markets/hours-calendars). There is no second calendar, so
there is no second supported venue class. #2312 tracks adding one.
"""

from __future__ import annotations

from typing import Final

#: ⚠ An ALLOW-list, and that direction is the whole point: ``exchanges.asset_class``
#: is a CHECK vocabulary that has already grown once (``mena_equity``, added by
#: ``sql/068`` over ``sql/067``'s original nine). A value added later lands on the
#: REFUSE side with no code change here. An exclusion list would have admitted it.
#:
#: ⚠ This does NOT contradict ``docs/settled-decisions.md`` ("core allocation
#: (#2603) -- a non-US-listed core instrument is permitted if its eligibility proof
#: passes"). That governs what a mandate may DECLARE; this governs what we can
#: session-check. A non-US core instrument is a legal mandate whose submissions
#: refuse until a calendar for its venue exists.
SESSION_SUPPORTED_ASSET_CLASSES: Final = frozenset({"us_equity"})


def session_support_reason(asset_class: str | None) -> str | None:
    """``None`` when this venue class can be session-checked, else why it cannot.

    ⚠ ``None`` as the ARGUMENT (no ``exchanges`` row for the instrument) is a
    refusal, not a pass -- an unknown venue is exactly the case an allow-list
    exists to catch.

    ⚠ The returned string opens with ``asset_class=<repr>`` so the ``detail`` that
    ``core_unsupported_market_session`` records keeps the shape it already had;
    the explanation after it is additive.
    """
    if asset_class in SESSION_SUPPORTED_ASSET_CLASSES:
        return None
    return (
        f"asset_class={asset_class!r} has no trading-session calendar in this repo "
        f"(supported: {', '.join(sorted(SESSION_SUPPORTED_ASSET_CLASSES))}), so no "
        "submission on that venue can be timed against its own market hours"
    )


__all__ = ["SESSION_SUPPORTED_ASSET_CLASSES", "session_support_reason"]
