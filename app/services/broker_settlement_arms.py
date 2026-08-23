"""What counts as the UNDERLYING product, in one place.

eToro's eligibility response documents a closed four-value ``settlementType``
vocabulary, each with the provider's own definition (live portal 2026-08-13,
``trading--demo/check-instrument-trading-eligibility``):

===============  ==========================================================
``real``         "the real instrument held in full value"
``realFutures``  "the real future contract, which is a derivative of an
                 underlying instrument"
``marginTrade``  "the real instrument held with only a portion of its value
                 called margin (leveraged asset)"
``cfd``          "contract for difference, which is a derivative following
                 the underlying instrument"
===============  ==========================================================

Exactly one of those is ownership at full value.  ``marginTrade`` IS the real
instrument, but leveraged, which the standing no-leverage posture bars; the
other two are derivatives by the provider's own wording.

Two callers share this definition: ``strategy_core_eligibility`` (#2603 item 2's
proof) and ``strategy_paper_executor._eligibility_reason`` (strategy entry arm
selection).  Two copies of "what counts as the underlying" is exactly the drift
#2437 keeps recording, so it lives here rather than in either.

⚠ This is arm SELECTION only, and is deliberately narrower than any caller's
full eligibility rule.  The executor additionally requires exactly one matching
row, an ``allowStopLossTakeProfit`` arm and a satisfied minimum; a core sleeve is
a stop-less indefinite holding and must not inherit an entry rule.  Calling this
does not make a caller eligible to trade.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Final

from app.providers.broker import BrokerInstrumentEligibility, BrokerLeverageConfig

# The one settlement type that means ownership at full value.
UNDERLYING_SETTLEMENT_TYPE = "real"
UNDERLYING_DIRECTION = "long"
UNLEVERAGED_LEVERAGE = 1

# --- The OPEN-POSITION vocabulary (#2602 item 3) ---------------------------
#
# A second, separate enumeration the provider publishes on a different endpoint.
# See ``position_investment_type_label`` for the citation and for why it is NOT
# mapped onto the four-value eligibility vocabulary above.
_POSITION_INVESTMENT_TYPES: Final[Mapping[int, str]] = MappingProxyType(
    {
        0: "CFD",
        1: "Real Asset",
        2: "SWAP",
        3: "Crypto MarginTrade",
        4: "Future Contract",
    }
)

#: The one investment type that means ownership at full value, unleveraged.
UNDERLYING_POSITION_INVESTMENT_TYPE_ID: Final[int] = 1


def offers_unleveraged(leverage_values: Iterable[object]) -> bool:
    """True when the arm offers x1.

    ⚠ ``bool`` is a subclass of ``int`` in Python, so a naive ``1 in values`` is
    true for ``leverageValues: [true]`` -- and the provider parser admits that,
    because ``isinstance(True, int)`` passes its integer check.  Malformed broker
    data must not be able to prove x1 eligibility.

    ⚠⚠ A boolean anywhere in the array disqualifies the WHOLE arm, not just that
    entry.  Rejecting only the entry would let ``[1, true]`` qualify on its
    genuine ``1``, and the qualifying arm's ``leverage_values`` is then PROJECTED
    into storage -- where ``int(True)`` is ``1``, so the stored evidence would
    claim the broker sent ``[1, 1]``.  Refusing the arm is what keeps a
    projection from asserting something the response never contained; it also
    fails closed, which is the right bias for an array we cannot read.
    """
    values = list(leverage_values)
    if any(isinstance(value, bool) for value in values):
        return False
    return any(value == UNLEVERAGED_LEVERAGE for value in values)


def is_underlying_long_arm(arm: BrokerLeverageConfig) -> bool:
    """True when this arm is the underlying product, held long, unleveraged.

    Compared case-insensitively: the documented vocabulary is the provider's
    promise, not ours to depend on, and the demo response already answers its
    response-level ``currency`` in lower case where the request sent upper.
    Anything that is not ``real`` is not the underlying, including a value that
    is not in the documented vocabulary at all -- unknown fails closed.
    """
    return (
        arm.settlement_type.strip().lower() == UNDERLYING_SETTLEMENT_TYPE
        and arm.direction.strip().lower() == UNDERLYING_DIRECTION
        and offers_unleveraged(arm.leverage_values)
    )


def position_investment_type_label(settlement_type_id: int | None) -> str | None:
    """The provider's own label for an OPEN POSITION's ``settlementTypeID``.

    Source rule (live portal 2026-08-23,
    ``trading--demo/get-account-pnl-and-portfolio-details``): the open-positions
    response documents the field as *"Position investment type. 0 - CFD,
    1 - Real Asset, 2 - SWAP, 3 - Crypto MarginTrade, 4 - Future Contract"*.

    ⚠⚠ **This is a DIFFERENT vocabulary from the eligibility one above** — five
    numeric values against four strings, and ``SWAP`` has no counterpart there.
    The two are deliberately not mapped onto each other: eToro publishes them as
    separate enumerations on separate endpoints, so an equivalence table would be
    ours, not theirs. The labels are returned verbatim as documented rather than
    normalised into ``real``/``cfd``/… for the same reason.

    They also answer different questions. The eligibility vocabulary answers
    *"can this account open the underlying today"*; this one answers *"what is
    the position that already exists"*. A position opened as a CFD stays a CFD
    after the underlying becomes available, so #2602 item 3's identity cannot be
    read off ``strategy_core_eligibility_proofs``.

    Returns ``None`` for an unrecognised or absent id — an unknown investment
    type must read as "not observed", never as one of the known ones.
    """
    return _POSITION_INVESTMENT_TYPES.get(settlement_type_id) if settlement_type_id is not None else None


def position_is_underlying(settlement_type_id: int | None) -> bool | None:
    """True when the position is the real asset held outright.

    Tri-state on purpose: ``None`` means the broker did not report an id we
    recognise, which is a different fact from "this is a derivative" and must not
    collapse into ``False`` on an operator-facing panel.

    ⚠ ``2 - SWAP`` and ``4 - Future Contract`` are derivatives by the provider's
    own wording, and ``3 - Crypto MarginTrade`` is the real asset but leveraged,
    which the standing no-leverage posture bars — so exactly one id qualifies,
    mirroring ``UNDERLYING_SETTLEMENT_TYPE`` on the eligibility side.
    """
    if settlement_type_id is None or settlement_type_id not in _POSITION_INVESTMENT_TYPES:
        return None
    return settlement_type_id == UNDERLYING_POSITION_INVESTMENT_TYPE_ID


def select_underlying_long_arms(
    row: BrokerInstrumentEligibility,
) -> tuple[BrokerLeverageConfig, ...]:
    """Every arm on ``row`` that is the underlying product, held long, unleveraged.

    Returns all matches rather than one: zero and many are different answers, and
    a caller that cannot tell them apart reports "ambiguous" for an instrument
    that is simply not offered as the underlying.
    """
    return tuple(arm for arm in row.leverage_configs if is_underlying_long_arm(arm))


__all__ = [
    "UNDERLYING_DIRECTION",
    "UNDERLYING_POSITION_INVESTMENT_TYPE_ID",
    "UNDERLYING_SETTLEMENT_TYPE",
    "UNLEVERAGED_LEVERAGE",
    "is_underlying_long_arm",
    "offers_unleveraged",
    "position_investment_type_label",
    "position_is_underlying",
    "select_underlying_long_arms",
]
