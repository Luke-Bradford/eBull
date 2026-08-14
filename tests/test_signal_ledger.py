"""Phase 3c — the writer's fill arithmetic, as pure logic.

Spec: ``docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md``
§4 and acceptance 2, 5, 7, 8.

⚠ These are DB-free by design. The thing under test is index arithmetic over a
series — extracting it into a pure function and table-testing it is the repo's
stated default, and the constraint set it mirrors is already exercised against
a real database in ``test_strategy_signals_ledger.py``. The one genuinely-new
SQL behaviour (the writer refusing to upsert) lives in
``test_signal_ledger_writer_db.py``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.indicator_series import RULE_SET_VERSION as INDICATOR_SERIES_RULE_SET_VERSION
from app.services.indicator_series import BarSeries
from app.services.market_regime_provider import RULE_SET_VERSION as BENCHMARK_SOURCE_RULE_SET_VERSION
from app.services.signal_ledger import LedgerRow, resolve_fills
from app.services.strategy_registry import StrategyIdentity, StrategySignal
from app.services.technical_analysis import OHLCVRow

_IDENTITY = StrategyIdentity(
    strategy_id="S-TEST",
    params={"period": 14},
    universe="survivor_only",
    cost_model_id="static-v1",
    source_hash="deadbeef",
)


def _bar(open_: Decimal | None) -> OHLCVRow:
    """One bar whose OPEN is the only field under test.

    ⚠ ``open_`` accepts None even though ``OHLCVRow`` declares
    ``open: Decimal``. That is not a test convenience — both
    ``price_daily.open`` and ``research_price_daily.open`` are NULLABLE columns,
    and ``price_structure`` builds ``OHLCVRow`` by passing ``bar.open`` through
    with no None check. The runtime type is wider than the annotation, so the
    writer has to cope with it rather than trust it.
    """
    return {
        "open": open_,  # type: ignore[typeddict-item]
        "high": Decimal("11"),
        "low": Decimal("9"),
        "close": Decimal("10.5"),
        "volume": 1000,
    }


def _series(dates: list[date], opens: list[Decimal | None] | None = None) -> BarSeries:
    values = opens if opens is not None else [Decimal(100 + i) for i in range(len(dates))]
    return BarSeries(dates=tuple(dates), rows=tuple(_bar(v) for v in values))


_CONSECUTIVE = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]

# ⚠ A REAL calendar shape, not an off-by-one fixture: 2024-01-05 is a Friday
# and 2024-01-08 the following Monday, so the "next bar" is three calendar days
# later. `signal_bar_date + 1 day` would name 2024-01-06, a Saturday, on which
# no instrument traded.
_WEEKEND_GAP = [date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8), date(2024, 1, 9)]


def _fired_at(index: int) -> StrategySignal:
    return StrategySignal(verdict="fired", signal_index=index)


class TestFillResolution:
    def test_fill_is_the_next_bar_open(self) -> None:
        rows = resolve_fills([_fired_at(0)], series=_series(_CONSECUTIVE), identity=_IDENTITY, instrument_id=1)
        assert rows[0].signal_bar_date == date(2024, 1, 2)
        assert rows[0].fill_bar_date == date(2024, 1, 3)
        assert rows[0].fill_price == Decimal("101")

    def test_fill_is_the_next_bar_in_the_series_not_the_next_calendar_day(self) -> None:
        """Acceptance 2. A signal on Friday fills on MONDAY, not Saturday."""
        rows = resolve_fills([_fired_at(1)], series=_series(_WEEKEND_GAP), identity=_IDENTITY, instrument_id=1)
        assert rows[0].signal_bar_date == date(2024, 1, 5)
        assert rows[0].fill_bar_date == date(2024, 1, 8)
        assert rows[0].fill_bar_date != date(2024, 1, 6), "date arithmetic invented a fill on a Saturday"

    def test_last_bar_is_no_fill_bar_not_a_fill(self) -> None:
        """Acceptance 8. There is no t+1, so the signal can never be entered."""
        rows = resolve_fills([_fired_at(3)], series=_series(_CONSECUTIVE), identity=_IDENTITY, instrument_id=1)
        assert rows[0].verdict == "not_evaluable"
        assert rows[0].not_evaluable_reason == "no_fill_bar"
        assert rows[0].fill_bar_date is None
        assert rows[0].fill_price is None

    def test_last_bar_is_no_fill_bar_whatever_verdict_it_arrived_with(self) -> None:
        """Mirrors ``strategy_registry.evaluate``, which stamps the final bar
        before it looks at anything else. A decision on the last bar cannot be
        acted on regardless of which way it went, and the two layers must not
        disagree depending on which one produced the signal."""
        rows = resolve_fills(
            [StrategySignal(verdict="not_fired", signal_index=3)],
            series=_series(_CONSECUTIVE),
            identity=_IDENTITY,
            instrument_id=1,
        )
        assert rows[0].verdict == "not_evaluable"
        assert rows[0].not_evaluable_reason == "no_fill_bar"

    def test_a_fill_bar_with_no_open_price_is_unusable_fill_price(self) -> None:
        """Both ``open`` columns are nullable, and the alternative to failing
        closed is storing a fill price of None on a fired row.

        ⚠ This asserted ``no_fill_bar`` until #2354. The code is now the tenth,
        because the bar EXISTS — see ``test_the_two_refusals_are_different_facts``
        for why the distinction is not cosmetic."""
        series = _series(_CONSECUTIVE, opens=[Decimal("100"), None, Decimal("102"), Decimal("103")])
        rows = resolve_fills([_fired_at(0)], series=series, identity=_IDENTITY, instrument_id=1)
        assert rows[0].verdict == "not_evaluable"
        assert rows[0].not_evaluable_reason == "unusable_fill_price"

    def test_a_zero_open_is_refused_and_never_becomes_a_fill_price(self) -> None:
        """⚠⚠ #2354, and the reason the ticket exists. ``open = 0`` is not NULL,
        so the old ``fill_open is None`` branch passed it straight through and
        stored ``fill_price = 0`` on a ``fired`` row — which every reader then
        refuses (``outcome_resolver``: *"entry_price must be > 0 …
        gross_return_pct divides by it"*). Measured on the dev corpus
        2026-08-08: 16 such bars in ``research_price_daily``, 154 in
        ``price_daily``, all `B1`-quarantined on both axes."""
        series = _series(_CONSECUTIVE, opens=[Decimal("100"), Decimal("0"), Decimal("102"), Decimal("103")])
        rows = resolve_fills([_fired_at(0)], series=series, identity=_IDENTITY, instrument_id=1)
        assert rows[0].verdict == "not_evaluable"
        assert rows[0].not_evaluable_reason == "unusable_fill_price"
        assert rows[0].fill_price is None
        assert rows[0].fill_bar_date is None

    def test_a_negative_open_is_refused(self) -> None:
        """⚠ Neither corpus stores a negative open today. That is a fact about
        an ingest run rather than a property of the column, so the bound is
        ``<= 0`` and this pins the half no corpus row currently reaches."""
        series = _series(_CONSECUTIVE, opens=[Decimal("100"), Decimal("-5"), Decimal("102"), Decimal("103")])
        rows = resolve_fills([_fired_at(0)], series=series, identity=_IDENTITY, instrument_id=1)
        assert rows[0].not_evaluable_reason == "unusable_fill_price"

    def test_the_two_refusals_are_different_facts(self) -> None:
        """⚠⚠ Criterion 8: *"collapsing them loses the ability to tell a data
        gap from a real absence."* The SAME series produces both codes — the
        last bar has no ``t+1`` at all, while bar 0's fill bar exists and is
        unpriceable. One `not_evaluable` count covering both would report a
        corpus growing zero-open bars as a corpus of series endings."""
        series = _series(_CONSECUTIVE, opens=[Decimal("100"), Decimal("0"), Decimal("102"), Decimal("103")])
        rows = resolve_fills([_fired_at(0), _fired_at(3)], series=series, identity=_IDENTITY, instrument_id=1)
        assert [r.not_evaluable_reason for r in rows] == ["unusable_fill_price", "no_fill_bar"]

    def test_non_fired_verdicts_carry_no_fill(self) -> None:
        rows = resolve_fills(
            [
                StrategySignal(verdict="not_fired", signal_index=0),
                StrategySignal(verdict="not_evaluable", signal_index=1, reason="quarantined_bar"),
            ],
            series=_series(_CONSECUTIVE),
            identity=_IDENTITY,
            instrument_id=1,
        )
        assert [r.verdict for r in rows] == ["not_fired", "not_evaluable"]
        assert [r.not_evaluable_reason for r in rows] == [None, "quarantined_bar"]
        assert all(r.fill_bar_date is None and r.fill_price is None for r in rows)

    def test_every_fired_row_fills_strictly_after_its_signal(self) -> None:
        """The invariant the CHECK backstops, asserted over a whole series."""
        series = _series(_WEEKEND_GAP)
        rows = resolve_fills(
            [_fired_at(i) for i in range(len(series))], series=series, identity=_IDENTITY, instrument_id=1
        )
        filled = [r for r in rows if r.verdict == "fired"]
        assert len(filled) == 3, "only the final bar should lack a fill"
        assert all(r.fill_bar_date is not None and r.fill_bar_date > r.signal_bar_date for r in filled)


class TestBatchIntegrity:
    def test_signal_index_outside_the_series_raises(self) -> None:
        with pytest.raises(ValueError, match="outside the 4-bar series"):
            resolve_fills([_fired_at(9)], series=_series(_CONSECUTIVE), identity=_IDENTITY, instrument_id=1)

    # ⚠ -1 and -2 are BOTH here because they fail differently, and a -1-only
    # test would have passed before the bound was two-sided (#2317). Measured on
    # this fixture against `5078c173`:
    #
    #   idx=-1: RAISED "fill_bar_date 2024-01-02 is not after signal_bar_date
    #           2024-01-05" — caught, but by the fill-after-signal mirror and
    #           with a misleading diagnosis. `fill_index = -1 + 1 = 0`, so the
    #           fill resolves EARLIER than the signal and trips that CHECK.
    #   idx=-2: STORED verdict=fired signal_bar=2024-01-04 fill_bar=2024-01-05
    #           price=103 — a well-formed row, dates correctly ordered, wrong
    #           bars. At -2 or below `fill_index` is still negative, so both
    #           dates wrap TOGETHER and every mirrored constraint passes.
    #
    # So the backstop is blind below -1 by construction, and only the writer's
    # own bound closes it.
    @pytest.mark.parametrize("signal_index", [-1, -2])
    def test_a_negative_signal_index_raises_rather_than_wrapping(self, signal_index: int) -> None:
        """#2317. Python list indexing makes `series.dates[-2]` legal, so a
        one-sided bound does not fail — it silently resolves a bar near the END
        of the series.

        ⚠ The state is built by mutating a frozen instance because
        `StrategySignal.__post_init__` refuses a negative index at construction,
        and the subject here is the WRITER's bound rather than the contract's.
        `resolve_fills` takes `Sequence[StrategySignal]`, which is an annotation
        and not a runtime gate — so this is a state the parameter type admits,
        the same shape as the half-fill defect on this module.
        """
        signal = _fired_at(0)
        object.__setattr__(signal, "signal_index", signal_index)
        with pytest.raises(ValueError, match="outside the 4-bar series"):
            resolve_fills([signal], series=_series(_CONSECUTIVE), identity=_IDENTITY, instrument_id=1)

    def test_duplicate_bar_and_kind_in_one_batch_raises(self) -> None:
        with pytest.raises(ValueError, match="duplicate signal"):
            resolve_fills(
                [_fired_at(0), _fired_at(0)],
                series=_series(_CONSECUTIVE),
                identity=_IDENTITY,
                instrument_id=1,
            )

    def test_the_same_bar_as_entry_and_exit_is_not_a_duplicate(self) -> None:
        """Parent §3.5 applies the fill rule to "entries and exits alike", so a
        strategy exiting one position and entering another on the same bar is
        legitimate — which is why ``signal_kind`` is in the uniqueness key."""
        rows = resolve_fills(
            [
                StrategySignal(verdict="fired", signal_index=0, kind="entry"),
                StrategySignal(verdict="fired", signal_index=0, kind="exit"),
            ],
            series=_series(_CONSECUTIVE),
            identity=_IDENTITY,
            instrument_id=1,
        )
        assert [r.signal_kind for r in rows] == ["entry", "exit"]

    def test_universe_and_version_come_from_the_identity(self) -> None:
        """Criterion 11 puts the universe INSIDE the version hash. Taking the
        stored label from the same object makes the two unable to disagree."""
        rows = resolve_fills([_fired_at(0)], series=_series(_CONSECUTIVE), identity=_IDENTITY, instrument_id=7)
        assert rows[0].universe == _IDENTITY.universe
        assert rows[0].strategy_version == _IDENTITY.version
        assert rows[0].instrument_id == 7

    def test_input_rule_set_versions_come_from_the_identity(self) -> None:
        """#2333, and the same argument one step further: the indicator rule
        set is hashed INTO ``version``, so reading the stored copy off the same
        object is what stops the column disagreeing with the hash beside it."""
        rows = resolve_fills([_fired_at(0)], series=_series(_CONSECUTIVE), identity=_IDENTITY, instrument_id=7)
        assert rows[0].input_rule_set_versions == _IDENTITY.input_rule_set_versions
        assert dict(rows[0].input_rule_set_versions) == {
            "indicator_series": INDICATOR_SERIES_RULE_SET_VERSION,
            "market_regime_provider": BENCHMARK_SOURCE_RULE_SET_VERSION,
        }


class TestLedgerRowRejects:
    """The row mirrors ``sql/255``'s CHECKs so a bad row fails before SQL."""

    _VALID: dict[str, object] = {
        "strategy_id": "S-TEST",
        "strategy_version": "strategy-registry-v1+abc123",
        "instrument_id": 1,
        "signal_bar_date": date(2024, 1, 2),
        "signal_kind": "entry",
        "verdict": "not_fired",
        "universe": "survivor_only",
        "input_rule_set_versions": {"indicator_series": "indicator-series-v1+abc123"},
    }

    def test_universe_has_no_default(self) -> None:
        """Acceptance 5. #2288: a field with a default is a field a writer can
        forget, and an unlabelled survivor-only win rate is the thing that
        cannot be un-published."""
        without_universe = {k: v for k, v in self._VALID.items() if k != "universe"}
        with pytest.raises(TypeError, match="universe"):
            LedgerRow(**without_universe)  # type: ignore[arg-type]

    def test_input_rule_set_versions_has_no_default(self) -> None:
        """#2333, same argument as ``universe``: an unrecorded indicator rule
        set makes two signals computed under different indicator code
        indistinguishable on the ledger."""
        without = {k: v for k, v in self._VALID.items() if k != "input_rule_set_versions"}
        with pytest.raises(TypeError, match="input_rule_set_versions"):
            LedgerRow(**without)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("label", "overrides", "match"),
        [
            (
                "same-bar fill",
                {"verdict": "fired", "fill_bar_date": date(2024, 1, 2), "fill_price": Decimal(10)},
                "not after",
            ),
            (
                "backwards fill",
                {"verdict": "fired", "fill_bar_date": date(2024, 1, 1), "fill_price": Decimal(10)},
                "not after",
            ),
            ("fired with no fill", {"verdict": "fired"}, "fill exists exactly"),
            (
                "not_fired carrying a fill",
                {"fill_bar_date": date(2024, 1, 3), "fill_price": Decimal(10)},
                "fill exists exactly",
            ),
            # ⚠ HALF a fill on a non-fired row. Codex found this passing at
            # checkpoint 2: an `a is not None and b is not None` test scores
            # False here and matched `verdict != "fired"`, while the SQL CHECK
            # requires both columns NULL. A mirror that disagrees with the
            # constraint it mirrors is worse than no mirror.
            ("not_fired with only a fill date", {"fill_bar_date": date(2024, 1, 3)}, "both fields move together"),
            ("not_fired with only a fill price", {"fill_price": Decimal(10)}, "both fields move together"),
            (
                "fired with only a fill date",
                {"verdict": "fired", "fill_bar_date": date(2024, 1, 3)},
                "both fields move together",
            ),
            ("not_evaluable with no reason", {"verdict": "not_evaluable"}, "disagree"),
            ("reason on a not_fired row", {"not_evaluable_reason": "series_break"}, "disagree"),
            (
                "free-text reason",
                {"verdict": "not_evaluable", "not_evaluable_reason": "because reasons"},
                "unknown reason",
            ),
            ("unknown verdict", {"verdict": "maybe"}, "unknown verdict"),
            ("unknown kind", {"signal_kind": "hedge"}, "unknown signal kind"),
            # #2333 — an EXACT mirror of sql/257's shape CHECK. Each of these
            # is a state `NOT NULL` alone lets through.
            ("empty rule-set mapping", {"input_rule_set_versions": {}}, "non-empty mapping"),
            ("rule sets not a mapping", {"input_rule_set_versions": ["indicator_series"]}, "non-empty mapping"),
            (
                "blank rule-set version",
                {"input_rule_set_versions": {"indicator_series": ""}},
                "non-empty version string",
            ),
            (
                "whitespace rule-set version",
                {"input_rule_set_versions": {"indicator_series": "   "}},
                "non-empty version string",
            ),
            (
                "non-string rule-set version",
                {"input_rule_set_versions": {"indicator_series": 5}},
                "non-empty version string",
            ),
        ],
    )
    def test_rejects(self, label: str, overrides: dict[str, object], match: str) -> None:
        with pytest.raises(ValueError, match=match):
            LedgerRow(**{**self._VALID, **overrides})  # type: ignore[arg-type]
