"""The exit-level adapters must REFUSE a bad bar, never abort the batch.

Spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md``. Refs #2437.

⚠⚠ THIS FILE EXISTS BECAUSE THE SOURCE READS AMBIGUOUSLY, AND A REVIEWER READ IT
WRONG. The adapters are written ``except ValueError, IndexError:`` — no
parentheses. On Python 3.14 that is **PEP 758 multi-catch** and it catches BOTH.
On Python 2 the identical text caught only ``ValueError`` and bound it to the
name ``IndexError``, which is why it looks like a classic blunder.

The parentheses were written and ``ruff format`` removed them as redundant under
the project's Python version. That is correct and it is also a trap: the reviewer
on PR #2715 raised it as BLOCKING, and the next reader has the same doubt with no
way to settle it from the source.

So the BEHAVIOUR is pinned here rather than argued in a comment. If the project's
Python version, ruff's rewrite, or the except clause ever changes such that
``IndexError`` stops being caught, these fail and say so — which a comment
asserting "it's fine, PEP 758" could never do.

⚠ The reviewer's second claim — that ``IndexError`` is dead because the brackets
only raise ``ValueError`` — is FALSE, and ``test_index_error_is_reachable`` is
the proof: an out-of-range ``signal_index`` raises it from the ``atr.values[...]``
lookup before any of the bracket's own validation runs.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services import strategy_manifest
from app.services.indicator_series import BarSeries
from app.services.outcome_resolver import exit_levels_are_orderable
from app.services.strategies.s5_support_bounce import s5_exit_bracket
from app.services.strategies.s6_resistance_breakout import s6_exit_bracket
from app.services.strategies.s9_squeeze_expansion import s9_exit_bracket
from app.services.strategy_manifest import STRATEGY_MANIFEST

#: Every strategy with a level-based `exit_levels` factory, batched or not.
#:
#: ⚠⚠ S-4 was EXCLUDED here until #2781, on the grounds that "its adapter goes
#: through `s4_exit_levels_batch` and has its own equivalence check". That check
#: compares an ORDERABLE bracket against the scalar; it never covered a bad index
#: or a missing ATR, and under it `s4_exit_levels_batch` raised where its five
#: siblings refused. **An exclusion justified by another test's existence is only
#: as good as that test's coverage** — which is the reason this list must now name
#: every level-based strategy rather than a curated three.
LEVEL_BASED = (
    "s4-volatility-compression-breakout",
    "s5-support-bounce",
    "s6-resistance-breakout",
    "s9-squeeze-expansion",
)

#: The subset whose SCALAR bracket reaches `atr.values[signal_index]` before any
#: range validation, and so raises `IndexError` rather than `ValueError`.
#:
#: ⚠ S-4 is absent by MEASUREMENT, not by assumption — it carries its own explicit
#: `0 <= signal_index < len(series)` guard, so it raises `ValueError` first. That
#: is asserted positively in `test_s4_raises_value_error_where_the_others_raise_index_error`
#: rather than left as a silent omission, because a silent omission is what #2781
#: was.
_BRACKETS = {
    "s5-support-bounce": s5_exit_bracket,
    "s6-resistance-breakout": s6_exit_bracket,
    "s9-squeeze-expansion": s9_exit_bracket,
}


def _series(n: int = 40) -> BarSeries:
    rows = tuple(
        {
            "open": Decimal(str(100 + i)),
            "high": Decimal(str(101 + i)),
            "low": Decimal(str(99 + i)),
            "close": Decimal(str(100 + i)),
            "volume": 1_000,
        }
        for i in range(n)
    )
    return BarSeries(dates=tuple(date(2024, 1, 1) + timedelta(days=i) for i in range(n)), rows=rows)  # type: ignore[arg-type]


@pytest.mark.parametrize("strategy_id", LEVEL_BASED)
def test_an_out_of_range_bar_is_refused_not_raised(strategy_id: str) -> None:
    """⚠⚠ THE ONE THAT MATTERS. An uncaught exception here aborts the WHOLE
    outcome batch for one bad bar, instead of recording one unresolved outcome.

    This is also the empirical answer to PR #2715's BLOCKING question: if the
    comma form were Python-2-shaped, ``IndexError`` would escape and this test
    would fail with an error rather than an assertion.
    """
    entry = STRATEGY_MANIFEST[strategy_id]
    assert entry.exit_levels is not None
    result = entry.exit_levels(
        _series(),
        signal_index=999,
        entry_price=Decimal("100"),
        universe="survivor_only",
    )
    assert result == "unorderable_exit_levels"


@pytest.mark.parametrize("strategy_id", LEVEL_BASED)
def test_a_bar_with_no_atr_is_refused_not_raised(strategy_id: str) -> None:
    """#2781's measured case. Bar 0 has no prior close, so it has no ATR.

    Five strategies refused this bar and S-4 raised. It is one masked bar from
    being live: `_exit_levels_for_entries` only asks for indices where an entry
    fired and the registry will not fire on an unevaluable bar, but the
    quarantine `masked` arm exists precisely to remove bars.
    """
    entry = STRATEGY_MANIFEST[strategy_id]
    assert entry.exit_levels is not None
    result = entry.exit_levels(
        _series(),
        signal_index=0,
        entry_price=Decimal("100"),
        universe="survivor_only",
    )
    assert result == "unorderable_exit_levels"


def test_s4_raises_value_error_where_the_others_raise_index_error() -> None:
    """Evidences S-4's absence from `_BRACKETS` instead of leaving it implicit.

    S-4's scalar validates the range itself, so an out-of-range index never
    reaches the `atr.values[...]` lookup. The distinction matters to the adapter
    contract: naming only one of the two exception classes would let the other
    escape, which is why both are caught and why this asymmetry is pinned rather
    than assumed.
    """
    from app.services.strategies.s4_volatility_compression_breakout import s4_exit_bracket

    with pytest.raises(ValueError):
        s4_exit_bracket(
            _series(),
            signal_index=999,
            entry_price=Decimal("100"),
            universe="survivor_only",
        )


@pytest.mark.parametrize("strategy_id", tuple(_BRACKETS))
def test_index_error_is_reachable(strategy_id: str) -> None:
    """The refuted half of the review: ``IndexError`` is NOT dead code.

    The brackets raise ``ValueError`` for their own refusals (no ATR, no level),
    but an out-of-range ``signal_index`` trips the ``atr.values[signal_index]``
    lookup first and raises ``IndexError`` — before any bracket-owned validation
    can run. Naming only ``ValueError`` in the adapter would let that escape.
    """
    with pytest.raises(IndexError):
        _BRACKETS[strategy_id](
            _series(),
            signal_index=999,
            entry_price=Decimal("100"),
            universe="survivor_only",
        )


@pytest.mark.parametrize(
    ("strategy_id", "factory_name", "target", "stop", "entry"),
    [
        # Measured 2026-08-15: S-5 signal 306648 filled at 48.01 after
        # gapping through its signal-time 50.2965 stop.
        ("s5-support-bounce", "s5_exit_bracket", Decimal("55"), Decimal("50.2965"), Decimal("48.01")),
        ("s6-resistance-breakout", "s6_exit_bracket", Decimal("55"), Decimal("50"), Decimal("48")),
        # The target can be above the stop yet already below the gap-up entry.
        ("s8-range-mean-reversion", "s8_exit_bracket", Decimal("99"), Decimal("98"), Decimal("100")),
        # Stop-only rules owe the same boundary.
        ("s7-trend-pullback", "s7_exit_bracket", None, Decimal("100"), Decimal("100")),
        ("s9-squeeze-expansion", "s9_exit_bracket", Decimal("101"), Decimal("100"), Decimal("100")),
    ],
)
def test_a_gap_through_an_exit_is_a_typed_refusal_not_a_batch_abort(
    monkeypatch: pytest.MonkeyPatch,
    strategy_id: str,
    factory_name: str,
    target: Decimal | None,
    stop: Decimal,
    entry: Decimal,
) -> None:
    monkeypatch.setattr(strategy_manifest, factory_name, lambda *args, **kwargs: (target, stop, 20))
    adapter = STRATEGY_MANIFEST[strategy_id].exit_levels
    assert adapter is not None
    assert adapter(_series(), signal_index=20, entry_price=entry, universe="survivor_only") == (
        "unorderable_exit_levels"
    )


@pytest.mark.parametrize(
    ("entry", "target", "stop", "expected"),
    [
        ("100", "110", "90", True),
        ("100", None, "90", True),
        ("100", "100", "90", False),
        ("100", "110", "100", False),
        ("100", None, "100", False),
        ("0", "110", "90", False),
        ("NaN", "110", "90", False),
        ("100", "Infinity", "90", False),
        ("100", "110", "NaN", False),
    ],
)
def test_shared_orderability_boundary(
    entry: str,
    target: str | None,
    stop: str,
    expected: bool,
) -> None:
    assert (
        exit_levels_are_orderable(
            entry_price=Decimal(entry),
            take_profit=None if target is None else Decimal(target),
            stop_loss=Decimal(stop),
        )
        is expected
    )
