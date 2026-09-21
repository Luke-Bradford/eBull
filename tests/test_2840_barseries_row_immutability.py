"""#2840 — ``BarSeries`` rows and caches are read-only. Pure, no DB.

Spec: ``docs/proposals/ta/2026-09-21-2840-barseries-row-immutability.md``.

⚠⚠ WHY THESE ASSERT CONSISTENCY AND NOT JUST "IT RAISED" (ckpt-1 finding 23).
A test that only checks an assignment raises passes under every bypass the spec
names in §3 — the hostile reflected ``__eq__``, ``setflags(write=True)``,
``vars(series)[…]``. It is the wrong question. What the class actually promises
is that ``rows`` and the six caches keep agreeing, so every attempt below is
followed by a consistency check, cold AND warm.

⚠ And none of these assert impossibility. This is an accident control (the
``unattended_guard`` class), so the claim under test is "the ordinary
alias-write is refused by two independent gates" — pyright at push time, the
proxy at run time. The surviving bypasses are named in ``BarSeries``' docstring
on purpose; a test asserting they are closed would be false.
"""

from __future__ import annotations

import copy
import dataclasses
import pickle
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, cast

import numpy as np
import pytest

from app.services.indicator_series import BarSeries, OHLCVRow

U = "survivor_only"


def _rows(n: int = 8) -> list[OHLCVRow]:
    """Mutable rows, as a caller builds them. Values differ per bar so a stale
    cache cannot coincidentally equal a fresh one."""
    return [
        cast(
            "OHLCVRow",
            {
                "open": Decimal(f"{10 + i}.25"),
                "high": Decimal(f"{11 + i}.50"),
                "low": Decimal(f"{9 + i}.00"),
                "close": Decimal(f"{10 + i}.75"),
                "volume": 1_000 + i,
            },
        )
        for i in range(n)
    ]


def _dates(n: int = 8) -> tuple[date, ...]:
    return tuple(date(2024, 1, 2) + timedelta(days=i) for i in range(n))


def _series(n: int = 8) -> BarSeries:
    return BarSeries(dates=_dates(n), rows=tuple(_rows(n)))


def _warm(series: BarSeries) -> None:
    """Populate all six caches, so a later divergence is observable."""
    for name in ("float_closes", "float_highs", "float_lows", "array_closes", "array_highs", "array_lows"):
        getattr(series, name)


def _assert_consistent(series: BarSeries) -> None:
    """Every cache still agrees with the rows it was derived from.

    This is the real invariant. `rows` is what `binding_mismatch` validates and
    the caches are what the strategies compute from; the defect was that those
    two could disagree in silence.
    """
    for field, floats, array in (
        ("close", series.float_closes, series.array_closes),
        ("high", series.float_highs, series.array_highs),
        ("low", series.float_lows, series.array_lows),
    ):
        live = [None if (v := row.get(field)) is None else float(v) for row in series.rows]
        assert list(floats) == live, f"{field}: float cache diverged from rows"
        np.testing.assert_array_equal(array, np.array(live, dtype=float))


class TestTheRowsThemselves:
    def test_writing_through_the_series_rows_is_refused_cold_and_warm(self) -> None:
        for warm in (False, True):
            series = _series()
            if warm:
                _warm(series)
            with pytest.raises(TypeError):
                series.rows[3]["close"] = Decimal("999")  # type: ignore[index]  # the runtime half of the guard
            _assert_consistent(series)

    def test_the_callers_retained_row_DICTS_cannot_reach_the_series(self) -> None:
        """⚠ The retained *list* could never reach the series — a tuple() copy
        of the list already decoupled that, so asserting on it would be vacuous
        (ckpt-1 finding 24). The rows are the dicts INSIDE it, which aliased
        straight through until the constructor started copying them."""
        for warm in (False, True):
            rows = _rows()
            series = BarSeries(dates=_dates(), rows=tuple(rows))
            if warm:
                _warm(series)
            before = series.rows[3]["close"]
            rows[3]["close"] = Decimal("999")
            assert series.rows[3]["close"] == before
            assert series.closes[3] == before
            _assert_consistent(series)

    def test_the_exploit_that_defeated_the_carrier_binding_now_raises(self) -> None:
        """The four-step sequence from spec §2: warm a cache at V_fake, mutate
        the rows to V_real, and the carrier binds V_real while the strategy
        computes V_fake. It now fails at step 3."""
        rows = _rows()
        series = BarSeries(dates=_dates(), rows=tuple(rows))
        _ = series.float_closes  # step 2 — S-12 reads THIS, not array_closes
        with pytest.raises(TypeError):  # step 3
            series.rows[3]["close"] = Decimal("999")  # type: ignore[index]
        _assert_consistent(series)

    def test_a_dict_subclass_whose_copy_returns_itself_cannot_smuggle_a_live_row(self) -> None:
        """Why the constructor uses ``dict(row)`` and not the ~3x faster
        ``row.copy()``: ``mappingproxy.copy()`` DELEGATES to the backing
        object's ``copy``, so this subclass would pass straight through."""

        class SelfCopy(dict[str, Any]):
            def copy(self) -> SelfCopy:
                return self

        live = SelfCopy(_rows(1)[0])
        series = BarSeries(dates=_dates(1), rows=(cast("OHLCVRow", live),))
        _warm(series)
        before = series.rows[0]["close"]
        live["close"] = Decimal("999")
        assert series.rows[0]["close"] == before
        _assert_consistent(series)


class TestTheCaches:
    """⚠ In scope precisely BECAUSE the strategies do not read ``rows`` — they
    read these (`closes = series.float_closes`, `LevelScan.build(highs=
    series.array_highs, …)`). Freezing the rows alone would guard the half
    nothing computes from."""

    @pytest.mark.parametrize("name", ["float_closes", "float_highs", "float_lows"])
    def test_a_float_cache_is_a_tuple_and_refuses_assignment(self, name: str) -> None:
        series = _series()
        cache = getattr(series, name)
        assert isinstance(cache, tuple)
        with pytest.raises(TypeError):
            cache[2] = 999.0  # type: ignore[index]
        _assert_consistent(series)

    @pytest.mark.parametrize("name", ["array_closes", "array_highs", "array_lows"])
    def test_an_array_cache_refuses_assignment(self, name: str) -> None:
        series = _series()
        array = getattr(series, name)
        assert not array.flags.writeable
        with pytest.raises(ValueError):
            array[2] = 999.0
        _assert_consistent(series)

    def test_the_decimal_closes_property_stays_live_and_is_not_frozen(self) -> None:
        """⚠ Deliberately NOT changed. It is rebuilt from ``rows`` on every
        access, so it is never stale and the list it hands back is a fresh
        object no other reader holds — mutating it harms nobody."""
        series = _series()
        first, second = series.closes, series.closes
        assert first == second
        assert first is not second
        first[0] = None
        assert series.closes[0] is not None


class TestTheShapesProductionActuallySupplies:
    """⚠ The happy fixture above has five non-null keys on every bar, which is
    NOT what ``load_masked_bars`` emits. Measured over 60 instruments / 60,626
    bars: ``volume`` ``None`` 16,661 times, ``high`` 12, ``low`` 12, ``close``
    11, ``open`` 2 — a quarantined field is masked to ``None``. A freeze tested
    only on complete bars would not have exercised the ``None`` branch in
    ``_floats``, which is the branch every masked bar takes."""

    def test_a_masked_bar_stays_consistent_through_the_freeze(self) -> None:
        rows = _rows(4)
        rows[1]["close"] = cast("Any", None)  # the mask, as price_masked_bars writes it
        rows[2]["volume"] = None
        rows[3]["high"] = cast("Any", None)
        series = BarSeries(dates=_dates(4), rows=tuple(rows))
        _warm(series)

        assert series.float_closes[1] is None
        assert np.isnan(series.array_closes[1])
        _assert_consistent(series)
        with pytest.raises(TypeError):
            series.rows[1]["close"] = Decimal("1")  # type: ignore[index]
        _assert_consistent(series)

    def test_a_row_missing_a_key_entirely_still_freezes_and_stays_consistent(self) -> None:
        """S-12's own fixtures omit keys rather than nulling them, and
        ``_floats`` reads through ``.get``. Both shapes have to survive."""
        partial = cast("OHLCVRow", {"open": Decimal("1"), "close": Decimal("2")})
        series = BarSeries(dates=_dates(1), rows=(partial,))
        _warm(series)
        assert series.float_highs[0] is None
        _assert_consistent(series)
        with pytest.raises(TypeError):
            series.rows[0]["open"] = Decimal("9")  # type: ignore[index]


class TestDatesAreCoercedNotJustAnnotated:
    def test_a_retained_list_of_dates_cannot_be_reordered_after_validation(self) -> None:
        dates = list(_dates())
        series = BarSeries(dates=cast("tuple[date, ...]", dates), rows=tuple(_rows()))
        dates.reverse()
        assert series.dates == _dates()
        assert isinstance(series.dates, tuple)

    def test_the_ordering_and_duplicate_guards_still_fire(self) -> None:
        rows = tuple(_rows(3))
        with pytest.raises(ValueError, match="not ascending"):
            BarSeries(dates=(date(2024, 1, 3), date(2024, 1, 2), date(2024, 1, 4)), rows=rows)
        with pytest.raises(ValueError, match="duplicate date"):
            BarSeries(dates=(date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 4)), rows=rows)
        with pytest.raises(ValueError, match="length mismatch"):
            BarSeries(dates=_dates(2), rows=rows)


class TestReconstruction:
    """Every path that builds a BarSeries without going through ``__init__``
    normally. Each asserts the result is itself frozen and self-consistent —
    not merely that the values match — because a reconstruction that skipped
    the freeze would pass a value-equality check (ckpt-1 finding 25)."""

    def test_replace_produces_a_frozen_consistent_series(self) -> None:
        series = dataclasses.replace(_series(), dates=_dates())
        with pytest.raises(TypeError):
            series.rows[0]["close"] = Decimal("999")  # type: ignore[index]
        _assert_consistent(series)

    def test_shallow_copy_produces_a_frozen_consistent_series(self) -> None:
        original = _series()
        _warm(original)
        clone = copy.copy(original)
        with pytest.raises(TypeError):
            clone.rows[0]["close"] = Decimal("999")  # type: ignore[index]
        _assert_consistent(clone)

    @pytest.mark.parametrize("roundtrip", [copy.deepcopy, lambda s: pickle.loads(pickle.dumps(s))])
    def test_deepcopy_and_pickle_survive_and_re_freeze(self, roundtrip: Any) -> None:
        """⚠ A ``MappingProxyType`` is not picklable and ``BarSeries`` WAS, so
        both of these would have broken silently — nothing crosses a process
        boundary today, which is exactly why nobody would have noticed.

        ⚠ The ``pickle`` here round-trips an object built in this process. It
        neither reads nor introduces a deserialization surface: no ``BarSeries``
        is persisted or received from anywhere, and this diff preserves an
        existing capability rather than adding one."""
        original = _series()
        _warm(original)
        clone = roundtrip(original)
        assert clone == original
        with pytest.raises(TypeError):
            clone.rows[0]["close"] = Decimal("999")  # type: ignore[index]
        _assert_consistent(clone)

    def test_the_pickle_payload_drops_the_warm_caches(self) -> None:
        series = _series()
        _warm(series)
        state = series.__getstate__()
        assert set(state) == {"dates", "rows"}
        assert all(type(row) is dict for row in state["rows"])

    def test_setstate_onto_an_already_warm_instance_leaves_no_stale_cache(self) -> None:
        """``__setstate__`` clears ``__dict__`` before re-running ``__init__``.
        Without the clear, the caches below would still describe the OLD rows
        while ``rows`` described the new ones — the original defect, reached by
        a different door (ckpt-1 finding 11)."""
        series = _series(8)
        _warm(series)
        series.__setstate__({"dates": _dates(3), "rows": tuple(_rows(3))})
        assert len(series) == 3
        _assert_consistent(series)

    @pytest.mark.parametrize(
        ("state", "match"),
        [
            ({"dates": _dates(2), "rows": tuple(_rows(3))}, "length mismatch"),
            ({"dates": (date(2024, 1, 3), date(2024, 1, 2)), "rows": tuple(_rows(2))}, "not ascending"),
            ({"dates": (date(2024, 1, 2), date(2024, 1, 2)), "rows": tuple(_rows(2))}, "duplicate date"),
        ],
    )
    def test_a_malformed_payload_is_refused_AND_leaves_the_instance_intact(
        self, state: dict[str, Any], match: str
    ) -> None:
        """⚠⚠ THE SECOND HALF IS THE POINT, and an earlier design got it wrong.

        That design cleared ``__dict__`` and then called ``__init__``. On a bad
        payload it raised — correctly — but left an EXISTING, previously valid
        instance empty or length-inconsistent. ``__setstate__`` is reachable on
        a live object, so a failed restore that destroys its target is a worse
        defect than the pickle breakage the hook exists to fix. Validation now
        happens on a replacement before anything is committed.
        """
        series = _series(3)
        _warm(series)
        with pytest.raises(ValueError, match=match):
            series.__setstate__(state)
        assert len(series) == 3
        assert series.dates == _dates(3)
        _assert_consistent(series)


class TestTheStatedLosses:
    """Pinned so they cannot silently return, and so the next reader finds the
    reason rather than rediscovering the breakage."""

    def test_subclassing_is_refused(self) -> None:
        with pytest.raises(TypeError, match="may not be subclassed"):
            type("Sneaky", (BarSeries,), {})

    @pytest.mark.parametrize("fn", [dataclasses.asdict, dataclasses.astuple])
    def test_asdict_and_astuple_fail_on_a_real_series_and_succeed_on_the_empty_one(self, fn: Any) -> None:
        """⚠ A real capability loss, accepted rather than hidden: these walk the
        fields directly and never reach ``__getstate__``. Nothing in ``app/`` or
        ``scripts/`` calls either on a ``BarSeries``.

        ⚠ Both halves are pinned because the loss is CONDITIONAL, exactly like
        ``hash`` below. An earlier draft claimed they raise unconditionally;
        they do not, because an empty series has no proxy to choke on, and a
        test asserting the unconditional claim would have been wrong in the
        direction that reads as thorough.
        """
        with pytest.raises(TypeError):
            fn(_series())
        assert fn(BarSeries(dates=(), rows=())) in ({"dates": (), "rows": ()}, ((), ()))

    def test_the_subclass_refusal_is_itself_suppressible_and_that_is_recorded(self) -> None:
        """⚠⚠ A NAMED GAP, pinned so nobody later reads the refusal as absolute.

        ``__init_subclass__`` only fires if every preceding class in the MRO
        cooperates. A mixin whose own hook omits ``super()`` swallows it, and
        the resulting subclass can override ``__post_init__`` and keep mutable
        rows. This is why ``__getstate__`` enumerates ``dataclasses.fields``
        instead of trusting the refusal to hold the field list at two.
        """

        class Swallow:
            def __init_subclass__(cls, **kwargs: Any) -> None:
                pass  # deliberately does NOT call super()

        class Child(Swallow, BarSeries):  # the refusal never runs
            def __post_init__(self) -> None:
                pass  # ... and the freeze is skipped with it

        live = _rows(1)[0]
        child = Child(dates=_dates(1), rows=(live,))
        live["close"] = Decimal("999")
        assert child.rows[0]["close"] == Decimal("999"), "the gap is real; if this fails the docs are stale"

    def test_hash_succeeds_only_for_the_empty_series(self) -> None:
        """⚠ The module comment used to claim ``__hash__`` compares
        ``(dates, rows)`` and works. It is generated over that pair, but a
        mapping is unhashable either way — so it raises for every real series
        and succeeds only for the degenerate one. Nothing hashes a BarSeries;
        this pins the corrected comment."""
        assert isinstance(hash(BarSeries(dates=(), rows=())), int)
        with pytest.raises(TypeError, match="unhashable"):
            hash(_series())


class TestTheSliceIdiomStillWorks:
    """All five production sub-series sites build ``BarSeries(dates=s.dates[a:b],
    rows=s.rows[a:b])``: ``price_segments.py:70``,
    ``strategy_signal_scan.py:1035``, ``backtest_run.py:1044`` and both
    dispatchers in ``strategy_segmented_evaluation.py``. They now hand the
    constructor rows that are ALREADY frozen, which is the path that has to keep
    working — and has to stay independent, not just equal."""

    def test_a_sub_series_is_correct_frozen_and_independent_of_its_parent(self) -> None:
        parent = _series(8)
        _warm(parent)
        child = BarSeries(dates=parent.dates[2:6], rows=parent.rows[2:6])

        assert len(child) == 4
        assert child.dates == parent.dates[2:6]
        assert child.float_closes == parent.float_closes[2:6]
        _assert_consistent(child)

        # Independent: the child re-copied, so the two do not share row objects.
        assert child.rows[0] is not parent.rows[2]
        with pytest.raises(TypeError):
            child.rows[0]["close"] = Decimal("999")  # type: ignore[index]
        _assert_consistent(parent)
