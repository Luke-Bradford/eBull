"""The capital sandbox bound — the engine's allocation boundary (#2844).

The operator's 2026-08-22 decision, verbatim: *"This won't be on the total pot, I
would still only be allocating a portion … this system would either work with an
expanding pot or always limited to the amount assigned. That is the only safety
net I'm interested in."*

⚠ **This module is arithmetic, not an authority.** It computes a bound and says
whether an exposure sits inside it. It reads nothing, writes nothing and refuses
nothing. The refusals live where the capital is committed --
``strategy_paper_executor`` -- and the withdrawal rule lives in
``strategy_control_plane``. Do not cite this module as the safety net; it is the
one line of arithmetic the safety net is built from.

⚠⚠ **The operator's words and the stored values are different vocabularies for
the same two modes**, and nothing in the schema says so:

    ``capital_mode = 'fixed'``    IS the operator's **capped** pot
    ``capital_mode = 'compound'`` IS the operator's **expanding** pot

``strategy_paper_pool_events.capital_mode`` predates the decision and its CHECK
enumerates ``('fixed', 'compound')``. #2844's issue text asks for an
``assigned_capital`` column and a ``capped``/``expanding`` mode -- both already
exist under these names, so adding them would have minted a FOURTH capital-limit
surface (alongside this pool, ``strategy_deployments.capital_limit`` and the core
mandate's percentages) and a second vocabulary for one concept. The mapping is
recorded here rather than migrated, because renaming a CHECK-pinned column buys a
migration and loses the history the append-only events table exists to keep.

WHY THIS IS ONE FUNCTION AND NOT THREE
---------------------------------------------------------------------------
The same arithmetic was written by hand in three places before this module:

* ``strategy_control_plane.py`` -- can the operator withdraw principal?
* ``app/api/strategies.py``     -- what headroom does the /strategies card show?
* ``strategy_paper_executor.py`` -- how much may this entry commit?

The third is the control, the second is the panel that tells the operator what
the control will permit, and the first is the operator's own edit path. Three
copies of one rule means the panel can promise headroom the executor refuses, or
the withdrawal check can allow a withdrawal the executor has already spent
against -- with every copy internally consistent and nothing to fail on.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

# ⚠ NO `SANDBOX_POLICY_VERSION` HERE, deliberately. The first draft defined one and
# documented it as what a stored refusal is interpreted against — but nothing in this
# module's blast radius persists it, so the claim was true of no row that exists. The
# repo's other policy versions (`CORE_MANDATE_POLICY_VERSION`, `GOVERNANCE_GATE_VERSION`)
# earn theirs by being written onto the event they stamp.
#
# When a `sandbox_exceeded` refusal is persisted with its bound — the point at which the
# arithmetic that produced it becomes un-recoverable from the row — add the stamp AND the
# column in that change, so the version and the thing it versions arrive together.

CapitalMode = Literal["fixed", "compound"]

#: The operator's vocabulary for each stored mode. Presentation only -- nothing
#: branches on it, because a second spelling that CAN be branched on is a second
#: mode waiting to disagree with the first.
OPERATOR_WORD_FOR_CAPITAL_MODE: dict[CapitalMode, str] = {
    "fixed": "capped",
    "compound": "expanding",
}

#: The refusal code the settled decision names for hitting the boundary.
SANDBOX_EXCEEDED = "sandbox_exceeded"


def effective_realised_delta(realised_delta: Decimal, capital_mode: CapitalMode) -> Decimal:
    """How much realised P&L moves the bound, under each mode.

    ⚠ Asymmetric on purpose, and the asymmetry is the whole of ``capped``: a
    ``fixed`` pot takes realised LOSSES (they shrink what remains of the
    assignment) but not realised PROFITS (those skim to unmanaged cash and must
    not quietly raise the ceiling the operator set). ``compound`` takes both.

    ⚠ Realised only. Open marks never move the bound -- an unrealised gain that
    widened the boundary would let a position fund its own enlargement, and give
    the widest boundary at exactly the moment the position is most extended.
    """
    if capital_mode == "fixed":
        return min(realised_delta, Decimal("0"))
    return realised_delta


def sandbox_bound(
    *,
    capital_limit: Decimal,
    capital_mode: CapitalMode,
    realised_delta: Decimal,
) -> Decimal:
    """The maximum engine exposure permitted right now.

    ⚠ Floored at zero. A pot whose realised losses exceed its assignment is
    exhausted, not negative: a negative bound would make every ``exposure <=
    bound`` comparison false in a way that reads as arithmetic rather than as the
    exhaustion it is, and ``max(0, ...)`` keeps the exhausted case saying exactly
    what an empty one says.

    ⚠ ``realised_delta`` must come from OUR ledger. Inferring it from broker
    balance is barred by the settled decision -- the account is shared with the
    operator's non-engine holdings, so the broker's number answers a different
    question (see #2602, where mirrors and pending orders were 39.8% of it).
    """
    return max(Decimal("0"), capital_limit + effective_realised_delta(realised_delta, capital_mode))


@dataclass(frozen=True)
class SandboxHeadroom:
    """What the bound permits, and how much of it is already spent."""

    bound: Decimal
    committed: Decimal
    #: Never negative: an over-committed pot has no headroom, not backwards headroom.
    remaining: Decimal

    @property
    def within_bound(self) -> bool:
        """Is current exposure inside the boundary at all?

        ⚠ ``committed <= bound``, NOT ``remaining > 0``. A pot spent to exactly
        its bound is within it and merely full; only a pot spent PAST its bound
        has breached. Collapsing the two would report a breach on the ordinary
        fully-invested state.
        """
        return self.committed <= self.bound


def headroom_from_bound(*, bound: Decimal, committed: Decimal) -> SandboxHeadroom:
    """Headroom against an ALREADY-COMPUTED bound.

    For the caller that resolved the bound earlier for its own reasons --
    ``strategy_paper_executor`` needs the deployment bound in the same pass and
    resolves both together. Re-deriving the bound from a recovered delta would
    reintroduce exactly the round-trip #2844 removed.
    """
    return SandboxHeadroom(
        bound=bound,
        committed=committed,
        remaining=max(Decimal("0"), bound - committed),
    )


def sandbox_headroom(
    *,
    capital_limit: Decimal,
    capital_mode: CapitalMode,
    realised_delta: Decimal,
    committed: Decimal,
) -> SandboxHeadroom:
    """Bound, exposure and remaining capacity in one consistent triple.

    Callers must not recompute any member from the others -- that is how the
    three hand-written copies this module replaces drifted apart.
    """
    return headroom_from_bound(
        bound=sandbox_bound(
            capital_limit=capital_limit,
            capital_mode=capital_mode,
            realised_delta=realised_delta,
        ),
        committed=committed,
    )


__all__ = [
    "OPERATOR_WORD_FOR_CAPITAL_MODE",
    "SANDBOX_EXCEEDED",
    "CapitalMode",
    "SandboxHeadroom",
    "effective_realised_delta",
    "headroom_from_bound",
    "sandbox_bound",
    "sandbox_headroom",
]
