"""``LevelScan`` — whole-series pivot detection hoisted out of the per-bar call.

Module under test: ``app/services/price_levels.py``.

⚠ THE HOIST IS A PERFORMANCE CHANGE AND MUST NOT BE A BEHAVIOURAL ONE. S-5 and
S-6 both ask for levels at every bar of a series, so the cost is real; the
verdicts must be identical to the form this replaced, and that is asserted here
rather than argued.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

import numpy as np
import pytest

from app.services.price_levels import LevelScan, levels_at, swing_pivots


class TestLevelScanReproducesTheScalarForm:
    """⚠ The hoist is a PERFORMANCE change and must not be a behavioural one.

    ``levels_at`` now builds a ``LevelScan`` and calls ``at``, so there is one
    code path — but the equivalence that made the hoist legal (a pivot's verdict
    depends only on its own +/- 5 bars, never on where the observer stands) is
    the claim, and it is asserted rather than argued.
    """

    @staticmethod
    def _wavy(n: int = 120) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        i = np.arange(n, dtype=float)
        highs = 100.0 + 5.0 * np.sin(i / 3.0) + 0.4 * np.sin(i / 11.0)
        lows = highs - 2.0
        volumes = np.full(n, 1_000.0)
        return highs, lows, volumes

    def test_the_hoisted_and_scalar_forms_agree_at_every_index(self) -> None:
        highs, lows, volumes = self._wavy()
        scan = LevelScan.build(highs=highs, lows=lows, volumes=volumes)
        seen = 0
        for index in range(highs.size):
            hoisted = scan.at(atr=1.5, index=index)
            scalar = levels_at(highs=highs, lows=lows, volumes=volumes, atr=1.5, index=index)
            assert hoisted == scalar
            seen += len(hoisted)
        assert seen > 0, "a fixture with no levels would make this vacuous"

    def test_a_pivot_is_never_reported_before_it_is_confirmed(self) -> None:
        """⚠⚠ The lookahead this whole construction exists to prevent. The last
        candidate is ``index - 5``, so a pivot at ``index`` is unknowable."""
        highs, lows, _ = self._wavy()
        pivots = swing_pivots(highs, lows)
        assert pivots.high_indices, "no pivots detected — the fixture proves nothing"
        for index in range(20, highs.size):
            live = LevelScan.build(highs=highs, lows=lows, volumes=None).at(atr=1.5, index=index)
            for level in live:
                assert level.last_touch_index <= index - pivots.half_window

    def test_ragged_inputs_raise_rather_than_returning_nothing(self) -> None:
        with pytest.raises(ValueError, match="must align"):
            LevelScan.build(highs=np.zeros(5), lows=np.zeros(4), volumes=None)
