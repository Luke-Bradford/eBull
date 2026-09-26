"""#3385 slice 3c-iii — the computation over a loaded panel (pure; no database).

Hand-computed where the number matters: which names enter each cohort, which cost band
each position pays, the stress charge on the arm only, and A ≡ C giving a(d) ≡ 0.
"""

from __future__ import annotations

import hashlib
import inspect
import math
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from app.services import hunt_compute as hc
from app.services import hunt_harness as hh
from app.services import hunt_panel
from app.services.hunt_harness import canonical_form
from app.services.hunt_panel import nyse_sessions
from app.services.hunt_view import Bars, SignalView
from app.services.series_termination import TerminationClass

_START = date(2000, 1, 3)
_SESSIONS = 130
_SESSION_DATES = tuple(_START + timedelta(days=i) for i in range(_SESSIONS))


def _series(
    series_id: int,
    closes: Sequence[float],
    *,
    opens: Sequence[float] | None = None,
    ordinals: Sequence[int] | None = None,
    exclusion: bytes | None = None,
    terminal: tuple[int, TerminationClass] | None = None,
    dividends: Mapping[int, float] | None = None,
) -> hc.PanelSeries:
    opens = list(opens if opens is not None else closes)
    ordinals = list(ordinals if ordinals is not None else range(len(closes)))
    highs = [max(o, c) * 1.01 for o, c in zip(opens, closes, strict=True)]
    lows = [min(o, c) * 0.99 for o, c in zip(opens, closes, strict=True)]
    return hc.PanelSeries(
        series_id=series_id,
        ratio=Bars(tuple(ordinals), tuple(opens), tuple(highs), tuple(lows), tuple(closes), (1000.0,) * len(closes)),
        traded_open=tuple(opens),
        traded_close=tuple(closes),
        exclusion=exclusion if exclusion is not None else bytes(len(closes)),
        dividends=dividends or {},
        terminal_ordinal=None if terminal is None else terminal[0],
        termination_class=None if terminal is None else terminal[1],
    )


def _panel(*series: hc.PanelSeries) -> hc.HuntPanel:
    return hc.HuntPanel(
        sessions=_SESSION_DATES,
        series={s.series_id: s for s in series},
        regime_labels=("bull_quiet",) * _SESSIONS,
        load_counts={"series_loaded": len(series)},
    )


def _params(**overrides: Any) -> hc.ComputeParams:
    fields: dict[str, Any] = {
        "split_start": _SESSION_DATES[0],
        "split_end": _SESSION_DATES[-1],
        "embargo": 0,
        "lag": 1,
        "h": 1,
        "entry_point": "open",
        "exit_point": "close",
        "sign": 1,
        "selection": 0.5,
        "constants": {},
        "commission": 0.0,
    }
    fields.update(overrides)
    return hc.ComputeParams(**fields)


def _constant(_t: int, view: SignalView, _constants: Mapping[str, Any]) -> Mapping[int, float]:
    return dict.fromkeys(view.series, 1.0)


def _by_close(_t: int, view: SignalView, _constants: Mapping[str, Any]) -> Mapping[int, float]:
    return {series_id: bars.close[-1] for series_id, bars in view.series.items()}


# --- eligibility --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("bar", "code"),
    [
        ((10.0, 11.0, 9.0, 10.5, 100.0), 0),
        ((10.0, 11.0, 9.0, math.nan, 100.0), 1),
        ((0.0, 11.0, 9.0, 10.5, 100.0), 1),
        ((10.0, 11.0, 10.2, 10.5, 100.0), 2),  # low above the open
        ((10.0, 10.4, 9.0, 10.5, 100.0), 3),  # high below the close
        ((10.0, 11.0, 9.0, 10.5, 0.0), 4),
        ((10.0, 11.0, 9.0, 10.5, math.nan), 4),
        ((10.0, 10.0, 10.0, 10.0, 100.0), 5),  # zero range
    ],
)
def test_bar_exclusion_reasons_in_order(bar: tuple[float, ...], code: int) -> None:
    assert hc.bar_exclusion(*bar) == code


def test_exclusions_unscored_and_gaps_are_counted_per_formation() -> None:
    closes = [50.0 + (i % 3) for i in range(_SESSIONS)]
    bad = bytearray(_SESSIONS)
    bad[10] = 2
    gap_ordinals = [i for i in range(_SESSIONS) if i != 10]
    panel = _panel(
        _series(1, closes),
        _series(2, closes, exclusion=bytes(bad)),
        _series(3, closes),
        _series(4, [closes[i] for i in gap_ordinals], ordinals=gap_ordinals),
    )

    def score(t: int, view: SignalView, constants: Mapping[str, Any]) -> Mapping[int, float]:
        return {sid: math.nan if sid == 3 else 1.0 for sid in view.series}

    outcome = hc.compute_panel(panel, _params(), score)
    per = outcome.statistics["per_formation"]
    at = per["formation_dates"].index(_SESSION_DATES[10].isoformat())
    assert per["low_above_body"][at] == 1
    assert per["no_bar"][at] == 1
    assert per["eligible"][at] == 2  # series 1 and 3
    assert per["unscored"][at] == 1  # series 3
    assert per["scored"][at] == 1
    assert per["arm"][at] == 1
    canonical_form(outcome.statistics)  # storable as-is


@pytest.mark.parametrize(
    ("scores", "error"),
    [({99: 1.0}, ValueError), ({1: "high"}, TypeError), ({1: True}, TypeError), ([1.0], TypeError)],
)
def test_a_malformed_signal_is_an_infrastructure_error(scores: Any, error: type[Exception]) -> None:
    panel = _panel(_series(1, [50.0] * _SESSIONS))
    with pytest.raises(error):
        hc.compute_panel(panel, _params(), lambda _t, _v, _c: scores)


# --- books through the panel --------------------------------------------------------------------


def test_a_no_signal_spec_gives_zero_active_return_at_base_cost() -> None:
    closes = [50.0 * (1.0 + 0.01 * math.sin(i)) for i in range(_SESSIONS)]
    panel = _panel(
        _series(1, closes),
        _series(2, [2 * c for c in closes], opens=[2 * c * 0.995 for c in closes]),
        _series(3, [3 * c for c in reversed(closes)]),
    )
    outcome = hc.compute_panel(panel, _params(h=3, entry_point="close", exit_point="close"), _constant)
    assert outcome.active_series is not None
    assert set(outcome.active_series) == {0.0}
    cells = outcome.statistics["cells"]
    for key, cell in cells.items():
        if key.endswith("|base"):
            assert cell == {"refused": "degenerate_variance", "detail": cell["detail"]}, key
        else:
            # The arm alone pays the top band under stress, so A ≡ C is no longer free.
            assert cell["mean"] < 0.0, key
    assert outcome.status == "refused"


def test_cohort_selection_and_the_band_each_position_pays() -> None:
    low_closes = [51.0 + (i % 2) for i in range(_SESSIONS)]
    panel = _panel(_series(1, low_closes, opens=[50.0] * _SESSIONS), _series(2, [150.0] * _SESSIONS))
    outcome = hc.compute_panel(panel, _params(), _by_close)

    band_50, band_150, top = 0.00509 / 2, 0.00322 / 2, 0.01450 / 2
    r2 = 150.0 * (1 - band_150) / (150.0 * (1 + band_150)) - 1
    r2_stress = 150.0 * (1 - top) / (150.0 * (1 + top)) - 1
    expected, expected_stress = [], []
    for d in range(1, _SESSIONS):  # formation d − 1, entry at the open of d
        r1 = low_closes[d] * (1 - band_50) / (50.0 * (1 + band_50)) - 1
        control = (r1 + r2) / 2
        expected.append(r2 - control)
        expected_stress.append(r2_stress - control)
    assert outcome.active_series == pytest.approx(expected, rel=1e-12, abs=1e-15)
    assert outcome.status == "computed"
    stress = outcome.statistics["cells"][hc.cell_key("zero_recovery", True, "stress")]
    assert stress["mean"] == pytest.approx(math.fsum(expected_stress) / len(expected_stress), rel=1e-12)
    assert outcome.statistics["per_formation"]["arm"][0] == 1


def test_the_terminal_fraction_follows_each_cells_policy() -> None:
    last = 60
    dying = _series(2, [150.0 + (i % 2) for i in range(last + 1)], terminal=(last, TerminationClass.OPERATION_OF_LAW))
    panel = _panel(_series(1, [50.0 + (i % 3) for i in range(_SESSIONS)]), dying)
    outcome = hc.compute_panel(panel, _params(h=2, entry_point="close", exit_point="close"), _by_close)
    cells = outcome.statistics["cells"]
    zero, best = (cells[hc.cell_key(policy, True, "base")] for policy in ("zero_recovery", "classified_best"))
    # Zero recovery writes the arm's held position to 0 on the terminal bar; best keeps its value.
    assert zero["mean"] < best["mean"]


def test_an_empty_grid_refuses_without_a_series() -> None:
    panel = _panel(_series(1, [50.0] * _SESSIONS))
    outcome = hc.compute_panel(panel, _params(split_start=date(2030, 1, 1), split_end=date(2030, 12, 31)), _constant)
    assert outcome.status == "refused"
    assert outcome.active_series is None
    assert outcome.statistics["grid"]["refused"] == "empty_grid"


def test_panel_series_refuses_a_bar_after_its_terminal_bar() -> None:
    with pytest.raises(ValueError, match="after its terminal bar"):
        _series(1, [50.0] * 5, terminal=(2, TerminationClass.UNKNOWN))
    with pytest.raises(ValueError, match="needs a class"):
        hc.PanelSeries(
            series_id=1,
            ratio=Bars((0,), (1.0,), (1.0,), (1.0,), (1.0,), (1.0,)),
            traded_open=(1.0,),
            traded_close=(1.0,),
            exclusion=b"\x00",
            dividends={},
            terminal_ordinal=0,
            termination_class=None,
        )


def test_nyse_sessions_include_pre_1998_mlk_day() -> None:
    sessions = nyse_sessions(date(1997, 1, 17), date(1998, 1, 20))
    assert date(1997, 1, 20) in sessions
    assert date(1998, 1, 19) not in sessions
    assert date(1997, 1, 18) not in sessions  # a Saturday


def test_the_computation_and_its_loader_are_part_of_the_harness_model_id() -> None:
    """Codex ckpt-2: every line that decides a stored number must move the model id."""
    expected = {
        module.__name__: hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest()
        for module in (hc, hunt_panel)
    }
    assert expected.items() <= hh._model_constants()["model_code_sha256"].items()
    assert set(hh.MODEL_INPUT_RULE_SETS.values()) <= set(hh._model_constants()["input_rule_sets"].values())
    # And ``evaluate`` takes no computation argument: a stub cannot store an outcome under this identity.
    assert set(inspect.signature(hh.evaluate).parameters) == {"conn", "spec", "registered_by"}


# --- #3386 slice 2: the signal receives dates and ex-dates ≤ t ------------------------------------


def test_the_signal_sees_dates_and_ex_dates_up_to_t_only() -> None:
    seen: dict[int, tuple[Any, ...]] = {}

    def recording(t: int, view: SignalView, _constants: Mapping[str, Any]) -> Mapping[int, float]:
        bars = view.series[1]
        seen[t] = (view.dates[-1], len(view.dates), tuple(bars.dividend_ordinals), tuple(bars.dividend_amounts))
        return dict.fromkeys(view.series, 1.0)

    closes = [10.0] * _SESSIONS
    hc.compute_panel(_panel(_series(1, closes, dividends={5: 0.2, 40: 0.3})), _params(), recording)
    day = _SESSION_DATES[10]
    assert seen[10] == ((day.year, day.month, day.day, day.weekday()), 11, (5,), pytest.approx((0.2,)))
    assert seen[40][2] == (5, 40)  # an ex-date on t is known on t
    assert seen[39][2] == (5,)
