"""#3046 residual 5 — pure-logic pins for the two arm-D definitions that were wrong.

Both were caught by Codex, one per checkpoint, and both are silent: they change a
count the settled contract cites without failing anything. Table-tested here rather
than left to the corpus, because the corpus answer moves with every ingest.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.price_quarantine import params_for, rule_w2
from scripts.verify_3046_consumer_exposure import Anchors
from scripts.verify_3046_contract_decision import _w2_windows, _weekend_explains

_EQUITY = params_for("us_equity")  # calendar_days_per_bar = 1.4 (7/5)
_CRYPTO = params_for("crypto")  # calendar_days_per_bar = 1


class TestW2WindowBarCount:
    """``by_rank[r]`` is the r-th MOST RECENT bar, so the window holds exactly r bars.

    Draft 1 used ``r + 1``. Because ``nominal = (bar_count - 1) * days_per_bar``, that
    inflates the gate and makes W2 fire LESS — the first full run reported 1,394
    firings where the corrected one reports 8,301.
    """

    def test_a_rank_window_holds_exactly_rank_bars(self) -> None:
        anchors = Anchors(
            win_end=date(2026, 1, 30),
            oldest=date(2026, 1, 2),
            depth=21,
            by_rank={1: date(2026, 1, 30), 2: date(2026, 1, 29), 20: date(2026, 1, 5)},
        )
        by_label = {label: bars for label, _, _, bars in _w2_windows(anchors)}
        assert by_label["rank_2"] == 2
        assert by_label["rank_20"] == 20
        assert by_label["whole_slice"] == 21

    def test_rank_1_produces_no_window(self) -> None:
        # rank 1 IS win_end, so the window is degenerate and rule_w2 refuses it anyway.
        anchors = Anchors(
            win_end=date(2026, 1, 30),
            oldest=date(2026, 1, 30),
            depth=1,
            by_rank={1: date(2026, 1, 30)},
        )
        assert _w2_windows(anchors) == []


class TestWeekendExplains:
    """Both sides must be in the same units, or the weekend is deducted twice.

    ``calendar_days_per_bar = 1.4`` IS ``7/5`` — it already carries the weekend
    allowance. Shortening the observed span by its weekend days while keeping 1.4 as
    the nominal excuses genuinely stretched windows.
    """

    def test_an_ordinary_friday_to_monday_pair_is_explained(self) -> None:
        # 2026-01-02 is a Friday, 2026-01-05 the following Monday. The 2-bar gate is
        # 2 * 1.4 = 2.8 days, below the ordinary 3-day gap, so W2 fires on a normal
        # weekend. ~6,900 load_day_changes windows are exactly this.
        friday, monday = date(2026, 1, 2), date(2026, 1, 5)
        assert rule_w2(friday, monday, 2, _EQUITY) is True
        assert _weekend_explains(friday, monday, 2, _EQUITY.calendar_days_per_bar) is True

    def test_a_genuinely_stretched_window_is_NOT_explained(self) -> None:
        # Codex checkpoint 2's counterexample. 20 bars over 60 calendar days is 44
        # weekdays for 19 intervals — stretched by any reading. The first version of
        # the helper returned True here and understated arm D's residue.
        start, end = date(2026, 1, 5), date(2026, 3, 6)
        assert rule_w2(start, end, 20, _EQUITY) is True
        assert _weekend_explains(start, end, 20, _EQUITY.calendar_days_per_bar) is False

    def test_a_seven_day_class_never_gets_the_weekend_exception(self) -> None:
        # On a 7-day market a Saturday IS a session; removing it would invent a
        # holiday the venue does not take. Read from ClassParams, not from the symbol.
        start, end = date(2026, 1, 5), date(2026, 3, 6)
        assert _CRYPTO.calendar_days_per_bar == 1
        assert _weekend_explains(start, end, 20, _CRYPTO.calendar_days_per_bar) is False

    @pytest.mark.parametrize("bar_count", [2, 5, 20, 50])
    def test_the_helper_never_claims_to_explain_a_window_w2_did_not_fire_on(self, bar_count: int) -> None:
        # A dense weekday window: W2 does not fire, so there is nothing to explain and
        # the helper's answer is unused. Pinned so a future change cannot make the
        # explanation arm the thing that DECIDES whether a window is exposed.
        start, end = date(2026, 1, 5), date(2026, 1, 5 + 6)
        if not rule_w2(start, end, bar_count, _EQUITY):
            assert _weekend_explains(start, end, bar_count, _EQUITY.calendar_days_per_bar) is True
