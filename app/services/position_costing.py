"""Phase 5b — applying the cost model to a built position.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.1 (the
arithmetic), §3.2 rule 5 (the open-position mark) and §3.4 (what an unpriced
close does to a statistic). Parent criterion 2. Refs #2240.

⚠⚠ NET RETURN IS COMPUTED FROM ADJUSTED PRICES, NOT BY SUBTRACTING A COST.

§5.1: *"a buy fills at ``fill_price × (1 + h)`` and a sell at
``fill_price × (1 − h)`` … Net return is computed from those adjusted prices,
never by subtracting a cost from ``gross_return_pct``"*. The two differ — a
round-trip half-spread is multiplicative in both the numerator and the
denominator — and ``sql/256`` names its column GROSS precisely so nothing
averages it as performance.

⚠ WHY THIS IS A SECOND MODULE. ``cost_model`` is a LEAF: it imports nothing from
the app, so ``strategy_registry`` can depend on it without dragging phase 5's
position builder into phase 3a's registry. This module is the consumer that
knows about both.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, get_args

from app.services.cost_model import (
    COST_MODEL_ID,
    PRICE_BASES,
    PriceBasis,
    band_for,
    buy_price,
    cost_band_for,
    sell_price,
)
from app.services.position_builder import Position

#: Why a position carries no return even though it carries a cost band.
#:
#: ⚠ ``ambiguous_close`` is §3.4's row: ``sql/256`` gives an ambiguous outcome a
#: date and withholds the price BY CONSTRAINT, so the bar is known and the
#: return is not. It is *excluded and counted*, never recorded as zero —
#: recording zero silently asserts break-even, which is favourable for a
#: strategy whose ambiguous bars span a stop.
#:
#: ⚠ ``no_mark`` is the open position whose series offered no usable close at or
#: after its fill (``position_builder``'s ``marks_unavailable``). Also excluded
#: and counted; §3.2 rule 5 forbids dropping it.
UncostedReason = Literal["ambiguous_close", "no_mark"]

UNCOSTED_REASONS: frozenset[str] = frozenset(get_args(UncostedReason))

#: Where the exit price came from. ⚠ ``mark`` is UNREALISED and 5d must be able
#: to tell the two apart: §3.4 keeps an open position out of the win rate and
#: out of expectancy while keeping it in exposure and on the equity curve.
ExitBasis = Literal["close", "mark"]

EXIT_BASES: frozenset[str] = frozenset(get_args(ExitBasis))


@dataclass(frozen=True)
class CostedPosition:
    """One position with the half-spread charged on both sides.

    ⚠ THE COST MODEL ID IS CARRIED PER ROW, not assumed from the module. It is
    hashed into ``strategy_version`` anyway, but a costed row that travels to a
    result table without it is a number whose cost basis has to be inferred —
    and criterion 2 makes the model a declared input, not an inferrable one.
    """

    position: Position
    cost_model_id: str
    price_basis: PriceBasis
    #: A nominal band for ``as_traded`` or ``max:<band>`` when the nominal
    #: price is unavailable. The latter must not read like observed history.
    band_label: str
    #: ``h`` — one side, as a fraction. Keyed on the ENTRY fill price (§5.1) and
    #: applied to BOTH sides, so a position crossing a band boundary mid-hold
    #: does not re-key.
    half_spread: Decimal
    entry_price_net: Decimal
    exit_price_gross: Decimal | None
    exit_price_net: Decimal | None
    exit_basis: ExitBasis | None
    gross_return_pct: Decimal | None
    net_return_pct: Decimal | None
    uncosted_reason: UncostedReason | None

    def __post_init__(self) -> None:
        if self.price_basis not in PRICE_BASES:
            raise ValueError(f"unknown price basis {self.price_basis!r}; must be one of {sorted(PRICE_BASES)}")
        expected_band = cost_band_for(self.position.entry_fill_price, price_basis=self.price_basis)
        expected_label = expected_band.label if self.price_basis == "as_traded" else f"max:{expected_band.label}"
        if self.band_label != expected_label or self.half_spread != expected_band.half_spread:
            raise ValueError(
                f"position on signal {self.position.entry_signal_id}: "
                f"cost basis {(self.band_label, self.half_spread)!r} "
                f"does not match {self.price_basis} entry basis {(expected_label, expected_band.half_spread)!r}"
            )
        if self.exit_basis is not None and self.exit_basis not in EXIT_BASES:
            raise ValueError(f"unknown exit basis {self.exit_basis!r}; must be one of {sorted(EXIT_BASES)}")
        if self.uncosted_reason is not None and self.uncosted_reason not in UNCOSTED_REASONS:
            raise ValueError(
                f"unknown uncosted reason {self.uncosted_reason!r}; must be one of {sorted(UNCOSTED_REASONS)}"
            )
        if (self.exit_basis is None) == (self.uncosted_reason is None):
            raise ValueError(
                f"position on signal {self.position.entry_signal_id} is "
                f"{'both' if self.exit_basis else 'neither'} priced and unpriced: exactly one of exit_basis / "
                "uncosted_reason is set"
            )
        # ⚠ COUNTED, not ANDed — ``position_builder``'s own guard, for the same
        # reason. Four fields move together and a subset of them present is a
        # half-costed row, which reads as "no exit" under a chain of ANDs.
        priced = (
            (self.exit_price_gross is not None)
            + (self.exit_price_net is not None)
            + (self.gross_return_pct is not None)
            + (self.net_return_pct is not None)
        )
        if priced != (4 if self.exit_basis is not None else 0):
            raise ValueError(
                f"position on signal {self.position.entry_signal_id} carries a partial costing "
                f"{(self.exit_price_gross, self.exit_price_net, self.gross_return_pct, self.net_return_pct)!r}: "
                "a priced position has all four, an unpriced one none"
            )
        if self.entry_price_net <= self.position.entry_fill_price:
            raise ValueError(
                f"position on signal {self.position.entry_signal_id}: net entry {self.entry_price_net} does not "
                f"exceed the gross fill {self.position.entry_fill_price} — a buy pays the spread (criterion 2 "
                "forbids a zero-cost trade)"
            )
        if self.exit_price_gross is not None:
            assert self.exit_price_net is not None  # narrowed by the count above
            if self.exit_price_net >= self.exit_price_gross:
                raise ValueError(
                    f"position on signal {self.position.entry_signal_id}: net exit {self.exit_price_net} is not "
                    f"below the gross exit {self.exit_price_gross} — a sell pays the spread"
                )
            assert self.gross_return_pct is not None and self.net_return_pct is not None
            if self.net_return_pct >= self.gross_return_pct:
                raise ValueError(
                    f"position on signal {self.position.entry_signal_id}: net return {self.net_return_pct} is not "
                    f"below gross {self.gross_return_pct} — costs cannot improve a trade"
                )


def _exit_leg(position: Position) -> tuple[Decimal | None, ExitBasis | None, UncostedReason | None]:
    """The gross exit price, where it came from, and why there is none.

    ⚠⚠ AN OPEN POSITION'S MARK IS CHARGED THE EXIT SIDE, AND THAT IS §3.2 RULE 5
    VERBATIM: the mark is *"taken at the last usable close of the evaluation
    window for that instrument, **minus one side of the cost model** (the exit
    that has not happened)"*. Acceptance C2(b) — *"both sides are charged on a
    closed position and one side on an open-at-window-end mark"* — is the same
    statement about the same trade: the entry side was paid when the position
    opened, and the one side the criterion names is the one applied to the MARK.
    Leaving the mark gross would report an unrealised gain nobody could realise.
    """
    if position.close_source is not None:
        if position.close_price is None:
            # `ambiguous` — sql/256 withholds the price by constraint (§3.4).
            return None, None, "ambiguous_close"
        return position.close_price, "close", None
    if position.mark_price is None:
        return None, None, "no_mark"
    return position.mark_price, "mark", None


def cost_position(position: Position, *, price_basis: PriceBasis) -> CostedPosition:
    """Charge the half-spread on one position. Pure; reads no database.

    ``price_basis`` is mandatory: nominal prices select their calibrated band;
    split-adjusted research prices cannot and receive the maximum band. The
    selected ``h`` goes to both sides and never re-keys during the position.

    ⚠ LONG-ONLY BY ITS OWN ARITHMETIC, and that is the backtest half of the
    cost model's declared lane (#2720): the entry is charged as ``buy_price``
    and the exit as ``sell_price``, so a short — whose entry is a sale — cannot
    pass through here meaningfully. The model's carry/FX structural-zero
    closure holds for exactly this lane (``cost_model.STRUCTURAL_ZERO_LANE``);
    a short is a CFD, accrues financing by construction, and needs a NEW cost
    model plus its own costing arithmetic, never this function reused.
    """
    band = cost_band_for(position.entry_fill_price, price_basis=price_basis)
    half_spread = band.half_spread
    entry_net = buy_price(position.entry_fill_price, half_spread=half_spread)

    exit_gross, exit_basis, uncosted = _exit_leg(position)
    exit_net: Decimal | None = None
    gross_return: Decimal | None = None
    net_return: Decimal | None = None
    if exit_gross is not None:
        exit_net = sell_price(exit_gross, half_spread=half_spread)
        hundred = Decimal(100)
        gross_return = (exit_gross - position.entry_fill_price) / position.entry_fill_price * hundred
        net_return = (exit_net - entry_net) / entry_net * hundred

    return CostedPosition(
        position=position,
        cost_model_id=COST_MODEL_ID,
        price_basis=price_basis,
        band_label=band.label if price_basis == "as_traded" else f"max:{band.label}",
        half_spread=half_spread,
        entry_price_net=entry_net,
        exit_price_gross=exit_gross,
        exit_price_net=exit_net,
        exit_basis=exit_basis,
        gross_return_pct=gross_return,
        net_return_pct=net_return,
        uncosted_reason=uncosted,
    )


def cost_positions(positions: Iterable[Position], *, price_basis: PriceBasis) -> tuple[CostedPosition, ...]:
    """``cost_position`` over a set, preserving order."""
    return tuple(cost_position(position, price_basis=price_basis) for position in positions)


def band_crossings(costed: Sequence[CostedPosition]) -> int:
    """Positions whose EXIT price sits in a different band from the entry.

    ⚠ REPORTED, NOT PREVENTED. §5.1 fixes the band on the entry price on
    purpose — *"re-keying mid-hold would make the cost depend on the outcome"* —
    so a crossing is correct behaviour, not a defect. It is counted because the
    count is how big the deliberate approximation is, and a narrowing that is
    not counted is a narrowing asserted harmless.

    Split-adjusted prices are skipped: their numeric thresholds are not nominal
    bands, so calling a movement between them a crossing would repeat #2400.
    """
    crossings = 0
    for row in costed:
        if row.price_basis != "as_traded":
            continue
        if row.exit_price_gross is None:
            continue
        if band_for(row.exit_price_gross).label != row.band_label:
            crossings += 1
    return crossings


__all__ = [
    "EXIT_BASES",
    "UNCOSTED_REASONS",
    "CostedPosition",
    "ExitBasis",
    "UncostedReason",
    "band_crossings",
    "cost_position",
    "cost_positions",
]
