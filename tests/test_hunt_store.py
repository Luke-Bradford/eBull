"""#3386 slice 1 — the persisted discovery panel (spec ``2026-09-26-3386-hunt-feature-store.md`` v3, "Part A")."""

from __future__ import annotations

import json
import math
from array import array
from collections.abc import Callable, Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

import numpy as np
import pytest

from app.services import hunt_store
from app.services.hunt_compute import PanelSeries
from app.services.hunt_store import PanelParts, StoredParts
from app.services.hunt_view import Bars
from app.services.series_termination import TerminationClass

_THROUGH = date(1990, 1, 31)
_SESSIONS = tuple(date(1990, 1, 2) + timedelta(days=offset) for offset in range(20))
_KEY = "k" * 64


def _series(
    series_id: int,
    ordinals: Sequence[int],
    *,
    dividends: Mapping[int, float] | None = None,
    terminal: tuple[int, TerminationClass] | None = None,
    nan_at: int | None = None,
) -> PanelSeries:
    count = len(ordinals)
    closes = [10.0 + series_id + 0.25 * i for i in range(count)]
    if nan_at is not None:
        closes[nan_at] = math.nan
    return PanelSeries(
        series_id=series_id,
        ratio=Bars(
            array("l", ordinals),
            array("d", [c * 0.99 for c in closes]),
            array("d", [c * 1.02 for c in closes]),
            array("d", [c * 0.97 for c in closes]),
            array("d", closes),
            array("d", [1000.0 + i for i in range(count)]),
        ),
        traded_open=array("d", [c * 1.98 for c in closes]),
        traded_close=array("d", [c * 2.0 for c in closes]),
        exclusion=bytes(i % 6 for i in range(count)),
        dividends=MappingProxyType(dict(dividends or {})),
        terminal_ordinal=None if terminal is None else terminal[0],
        termination_class=None if terminal is None else terminal[1],
    )


def _parts() -> PanelParts:
    series = {
        3: _series(3, [0, 1, 2, 5, 6], dividends={1: 0.2, 4: math.nan}, nan_at=2),  # ex-date 4: a no-bar session
        7: _series(7, [2, 3, 4], terminal=(4, TerminationClass.OPERATION_OF_LAW)),
        11: _series(11, list(range(20)), dividends={19: 0.5}),  # the last series, dividend on the last session
    }
    return PanelParts(
        sessions=_SESSIONS,
        series=MappingProxyType(series),
        load_counts=MappingProxyType({"series_loaded": 3, "bars_loaded": 28}),
    )


def _same_floats(left: Sequence[float], right: Sequence[float]) -> bool:
    return len(left) == len(right) and all(
        (math.isnan(a) and math.isnan(b)) or a == b for a, b in zip(left, right, strict=True)
    )


def _assert_equal(stored: PanelParts, source: PanelParts) -> None:
    assert stored.sessions == source.sessions
    assert list(stored.series) == list(source.series)  # iteration order too
    assert dict(stored.load_counts) == dict(source.load_counts)
    for series_id, want in source.series.items():
        got = stored.series[series_id]
        assert type(got.ratio.ordinals) is array and got.ratio.ordinals.typecode == "l"
        assert list(got.ratio.ordinals) == list(want.ratio.ordinals)
        for name in ("open", "high", "low", "close", "volume"):
            column = getattr(got.ratio, name)
            assert type(column) is array and column.typecode == "d"
            assert _same_floats(column, getattr(want.ratio, name)), name
        assert _same_floats(got.traded_open, want.traded_open)
        assert _same_floats(got.traded_close, want.traded_close)
        assert got.exclusion == want.exclusion
        assert list(got.dividends) == list(want.dividends)
        assert _same_floats(list(got.dividends.values()), list(want.dividends.values()))
        assert got.terminal_ordinal == want.terminal_ordinal
        assert got.termination_class is want.termination_class


def _write(tmp_path: Path) -> tuple[Path, str]:
    directory = tmp_path / _KEY
    return directory, hunt_store.save_parts(_parts(), directory, key=_KEY, through=_THROUGH)


def test_a_round_trip_returns_the_parts_field_by_field(tmp_path: Path) -> None:
    directory, content = _write(tmp_path)
    stored = hunt_store.load_parts(directory, key=_KEY, through=_THROUGH)
    assert stored is not None and stored.content_sha256 == content
    _assert_equal(stored.parts, _parts())


def test_the_content_digest_is_deterministic(tmp_path: Path) -> None:
    _, first = _write(tmp_path / "a")
    _, second = _write(tmp_path / "b")
    assert first == second


def test_an_empty_panel_round_trips(tmp_path: Path) -> None:
    empty = PanelParts(sessions=(), series=MappingProxyType({}), load_counts=MappingProxyType({}))
    hunt_store.save_parts(empty, tmp_path / "e", key=_KEY, through=_THROUGH)
    stored = hunt_store.load_parts(tmp_path / "e", key=_KEY, through=_THROUGH)
    assert stored is not None and stored.parts.sessions == () and dict(stored.parts.series) == {}


def _flip_byte(directory: Path) -> None:
    path = directory / "close.npy"
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(bytes(data))


def _truncate(directory: Path) -> None:
    path = directory / "ordinal.npy"
    path.write_bytes(path.read_bytes()[:-8])


def _rewrite(directory: Path, name: str, values: np.ndarray[Any, Any], *, pickle: bool = False) -> None:
    """Replace one array AND its manifest hash, so only the structural checks can catch it."""
    path = directory / f"{name}.npy"
    np.save(path, values, allow_pickle=pickle)
    manifest = json.loads((directory / "manifest.json").read_bytes())
    manifest["files"][name] = hunt_store._sha256(path.read_bytes())
    (directory / "manifest.json").write_bytes(hunt_store.canonical_json(manifest))


def _series_table(directory: Path) -> np.ndarray[Any, Any]:
    return np.load(directory / "series.npy")


def _bad_offset(directory: Path) -> None:
    table = _series_table(directory)
    table[1, 1] = table[2, 1] + 1
    _rewrite(directory, "series", table)


def _bad_class(directory: Path) -> None:
    table = _series_table(directory)
    table[0, 3], table[0, 4] = 6, 99
    _rewrite(directory, "series", table)


@pytest.mark.parametrize(
    "corrupt",
    [
        pytest.param(lambda d: (d / "manifest.json").unlink(), id="missing-manifest"),
        pytest.param(lambda d: (d / "manifest.json").write_text("{not json"), id="malformed-manifest"),
        pytest.param(lambda d: (d / "volume.npy").unlink(), id="missing-array"),
        pytest.param(_flip_byte, id="flipped-byte"),
        pytest.param(_truncate, id="truncated-array"),
        pytest.param(lambda d: _rewrite(d, "close", np.zeros(28, dtype=np.float32)), id="wrong-dtype"),
        pytest.param(lambda d: _rewrite(d, "close", np.zeros(27)), id="wrong-length"),
        pytest.param(
            lambda d: _rewrite(d, "dividend_amount", np.array([0.2, None, 0.5], dtype=object), pickle=True),
            id="pickled-array",
        ),
        pytest.param(_bad_offset, id="bad-offset"),
        pytest.param(_bad_class, id="bad-class-code"),
        pytest.param(lambda d: _rewrite(d, "exclusion", np.full(28, 9, dtype=np.uint8)), id="bad-exclusion"),
        pytest.param(
            lambda d: _rewrite(d, "sessions", np.array([day.toordinal() for day in reversed(_SESSIONS)])),
            id="sessions-out-of-order",
        ),
    ],
)
def test_any_damage_is_a_miss(tmp_path: Path, corrupt: Callable[[Path], None]) -> None:
    directory, _ = _write(tmp_path)
    corrupt(directory)
    assert hunt_store.load_parts(directory, key=_KEY, through=_THROUGH) is None


def test_another_key_or_through_date_is_a_miss(tmp_path: Path) -> None:
    directory, _ = _write(tmp_path)
    assert hunt_store.load_parts(directory, key="x" * 64, through=_THROUGH) is None
    assert hunt_store.load_parts(directory, key=_KEY, through=_THROUGH - timedelta(days=1)) is None


def test_parts_after_the_through_date_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="after the store's through date"):
        hunt_store.save_parts(_parts(), tmp_path / "x", key=_KEY, through=_SESSIONS[-2])


def test_parts_out_of_series_order_are_refused(tmp_path: Path) -> None:
    source = _parts()
    shuffled = PanelParts(
        sessions=source.sessions,
        series=MappingProxyType({sid: source.series[sid] for sid in (7, 3, 11)}),
        load_counts=source.load_counts,
    )
    with pytest.raises(ValueError, match="series_id order"):
        hunt_store.save_parts(shuffled, tmp_path / "x", key=_KEY, through=_THROUGH)


def test_the_key_moves_with_every_input() -> None:
    base: dict[str, Any] = {
        "through": _THROUGH,
        "universe_identity": {"universe": "survivorship_free", "archive_sha256": "a" * 64},
        "quarantine_identity": "q" * 64,
        "model_id": "hunt-harness-v1+abc",
    }
    changes: dict[str, Any] = {
        "through": _THROUGH - timedelta(days=1),
        "universe_identity": {"universe": "survivorship_free", "archive_sha256": "b" * 64},
        "quarantine_identity": "r" * 64,
        "model_id": "hunt-harness-v1+abd",
    }
    key = hunt_store.store_key(**base)
    for name, value in changes.items():
        assert hunt_store.store_key(**{**base, name: value}) != key, name


def test_canonical_json_refuses_other_leaves() -> None:
    for value in (1.5, True, None, {1: "x"}):
        with pytest.raises(TypeError):
            hunt_store.canonical_json({"v": value})


def _build_counter(parts: PanelParts) -> tuple[list[int], Callable[[], PanelParts]]:
    calls: list[int] = []

    def build() -> PanelParts:
        calls.append(1)
        return parts

    return calls, build


def test_a_miss_builds_publishes_and_the_next_call_hits(tmp_path: Path) -> None:
    calls, build = _build_counter(_parts())
    first = hunt_store.load_or_build(tmp_path, key=_KEY, through=_THROUGH, build=build, verify=lambda: None)
    second = hunt_store.load_or_build(tmp_path, key=_KEY, through=_THROUGH, build=build, verify=lambda: None)
    assert calls == [1]
    assert isinstance(first, StoredParts) and first.content_sha256 == second.content_sha256
    _assert_equal(first.parts, _parts())
    assert sorted(path.name for path in tmp_path.iterdir()) == [_KEY]


def test_a_crashed_temp_directory_is_removed_and_other_keys_are_kept(tmp_path: Path) -> None:
    (tmp_path / ".tmp-old-123").mkdir()
    (tmp_path / ".tmp-old-123" / "close.npy").write_bytes(b"partial")
    other = tmp_path / ("o" * 64)
    other.mkdir()
    _, build = _build_counter(_parts())
    hunt_store.load_or_build(tmp_path, key=_KEY, through=_THROUGH, build=build, verify=lambda: None)
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted([_KEY, "o" * 64])


def test_a_corrupt_entry_is_replaced(tmp_path: Path) -> None:
    directory, _ = _write(tmp_path)
    _flip_byte(directory)
    calls, build = _build_counter(_parts())
    stored = hunt_store.load_or_build(tmp_path, key=_KEY, through=_THROUGH, build=build, verify=lambda: None)
    assert calls == [1]
    _assert_equal(stored.parts, _parts())
    assert hunt_store.load_parts(directory, key=_KEY, through=_THROUGH) is not None


class _Moved(RuntimeError):
    pass


def test_an_identity_change_during_the_build_publishes_nothing(tmp_path: Path) -> None:
    _, build = _build_counter(_parts())

    def verify() -> None:
        raise _Moved

    with pytest.raises(_Moved):
        hunt_store.load_or_build(tmp_path, key=_KEY, through=_THROUGH, build=build, verify=verify)
    assert list(tmp_path.iterdir()) == []


def test_verify_runs_after_the_read_back(tmp_path: Path) -> None:
    seen: list[bool] = []
    _, build = _build_counter(_parts())

    def verify() -> None:
        # The temp directory is already written and complete when the identities are re-read.
        temporaries = list(tmp_path.glob(".tmp-*"))
        seen.append(len(temporaries) == 1 and (temporaries[0] / "manifest.json").exists())

    hunt_store.load_or_build(tmp_path, key=_KEY, through=_THROUGH, build=build, verify=verify)
    assert seen == [True]


# ---------------------------------------------------------------------------
# hunt_panel.compute_trial: which splits use the store
# ---------------------------------------------------------------------------


def _route(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, split: str, *, quarantine: list[str] | None = None
) -> tuple[Any, list[str]]:
    from app.services import hunt_compute, hunt_panel

    events: list[str] = []
    identities = iter(quarantine or ["q" * 64] * 3)
    monkeypatch.setattr(hunt_panel, "STORE_ROOT", tmp_path)
    monkeypatch.setattr(hunt_panel, "quarantine_identity", lambda _conn, **_kw: next(identities))
    monkeypatch.setattr(hunt_panel, "load_panel_parts", lambda _conn, **_kw: events.append("live-parts") or _parts())
    monkeypatch.setattr(
        hunt_panel,
        "load_hunt_panel",
        lambda _conn, **_kw: events.append("live-panel") or hunt_panel._with_regimes(None, _parts()),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(hunt_panel, "_regime_labels", lambda _conn, sessions: ["unclassified"] * len(sessions))

    def compute(panel: Any, _params: Any, _signal: Any) -> hunt_compute.PanelOutcome:
        _assert_equal(PanelParts(panel.sessions, panel.series, panel.load_counts), _parts())
        return hunt_compute.PanelOutcome("computed", {"load": dict(panel.load_counts)}, (0.0,))

    monkeypatch.setattr(hunt_panel.hunt_compute, "compute_panel", compute)
    outcome = hunt_panel.compute_trial(
        None,  # type: ignore[arg-type]
        split=split,
        split_start=_SESSIONS[0],
        split_end=_THROUGH,
        embargo_sessions=63,
        universe="survivorship_free",
        lag=1,
        h=1,
        entry_point="open",
        exit_point="close",
        sign=1,
        selection=0.5,
        constants={},
        commission=0.0,
        signal=lambda _t, _view, _constants: {},
        universe_identity={"universe": "survivorship_free"},
        model_id="hunt-harness-v1+test",
        read_universe_identity=lambda: {"universe": "survivorship_free"},
    )
    return outcome, events


def test_discovery_computes_on_the_store_and_records_it(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    first, events = _route(monkeypatch, tmp_path, "discovery")
    assert events == ["live-parts"]
    load = first.statistics["load"]
    assert load["store_key"] in {path.name for path in tmp_path.iterdir()}
    second, events = _route(monkeypatch, tmp_path, "discovery")
    assert events == []  # a hit reads no live parts
    assert second.statistics == first.statistics  # hit and miss record the same identifiers


@pytest.mark.parametrize("split", ["validation", "holdout"])
def test_validation_and_holdout_never_touch_the_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, split: str
) -> None:
    outcome, events = _route(monkeypatch, tmp_path, split)
    assert events == ["live-panel"]
    assert list(tmp_path.iterdir()) == []
    assert outcome.statistics["load"]["store_key"] is None
    assert outcome.statistics["load"]["store_content_sha256"] is None


def test_a_quarantine_change_during_the_computation_refuses(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from app.services import hunt_panel

    with pytest.raises(hunt_panel.HuntPanelError, match="during the computation"):
        _route(monkeypatch, tmp_path, "discovery", quarantine=["q" * 64, "q" * 64, "r" * 64])


def test_a_quarantine_change_during_the_build_publishes_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from app.services import hunt_panel

    with pytest.raises(hunt_panel.HuntPanelError, match="while the store was built"):
        _route(monkeypatch, tmp_path, "discovery", quarantine=["q" * 64, "r" * 64])
    assert list(tmp_path.iterdir()) == []
