"""Shared SQL fragments for the core/cash arm of ``strategy_trades`` (#2603 item 3).

``sql/349`` made ``strategy_trades`` an exclusive arc: exactly one of
``funding_decision_id`` (signal arm) and ``core_rebalance_intent_id`` (core arm)
is non-null.  Seven queries previously reached a trade and then INNER JOINed
``strategy_funding_decisions``, which silently drops every core trade.

The fragments live here rather than being retyped at each site because the
requirements they encode -- an actionable verdict, a paper-declared mandate --
are exactly the kind that drift when duplicated, and a site that drops one
half fails OPEN.  ``sql/348`` stores holds and refusals as evidence, so the FK
alone would let a ``refused`` intent back a trade.

⚠⚠ THERE ARE TWO PREDICATES AND THEY ARE NOT INTERCHANGEABLE.

``core_arm_authorised`` is for paths that LOAD A POSITION IN ORDER TO ACT ON IT
-- the paper cycle's owned batch, the position manager's loader.  It fails
CLOSED: an intent that is not actionable, or a mandate that is not paper, does
not load, exactly as a non-paper deployment does not load on the signal arm.

``CORE_ARM_PRESENT`` is for paths that REPORT.  It asks only "is this a core
trade".  Applying the authorised predicate to a read endpoint would HIDE a
core position whose mandate or intent went unexpected -- and an invisible
position is the precise failure this whole slice exists to remove.  On a read
path, fail closed means show it, not hide it.

Alias discipline: the trade alias is supplied by the caller because the existing
queries disagree (``t`` and ``trade``).  It is typed ``LiteralString``, and that
is load-bearing rather than decorative -- ``str.format`` preserves
``LiteralString`` only when every argument is itself a literal, so a runtime
string CANNOT reach these fragments and psycopg's own ``LiteralString`` guard on
``execute`` stays intact all the way to the call site.  Verified against pyright
1.1.408, not assumed.  The two core aliases are FIXED so a reader can grep
``core_intent`` / ``core_mandate`` and find every site.
"""

from __future__ import annotations

from typing import LiteralString

# The join pair every core-aware query needs.  ``mode='paper'`` sits in the ON
# clause rather than the WHERE so that a LEFT JOIN keeps it a filter on this arm
# only; ``core_mandate.core_mandate_event_id IS NOT NULL`` then witnesses it.
#
# ⚠ A join predicate that also FILTERS is a safety control.  Converting an INNER
# JOIN to a LEFT JOIN moves that control into the WHERE clause or deletes it --
# there is no third outcome.  That is why each witness below is explicit rather
# than inherited from the join shape.
_CORE_ARM_JOINS: LiteralString = """
        LEFT JOIN strategy_core_rebalance_intents core_intent
          ON core_intent.core_rebalance_intent_id={t}.core_rebalance_intent_id
        LEFT JOIN strategy_core_mandate_events core_mandate
          ON core_mandate.core_mandate_event_id=core_intent.core_mandate_event_id
         AND core_mandate.mode='paper'
"""

# Every link witnessed individually.  Witnessing the intent alone would not
# prove the mandate resolved, and ``core_intent.action`` alone would not prove
# the mandate is paper.
_CORE_ARM_AUTHORISED: LiteralString = """(
              {t}.core_rebalance_intent_id IS NOT NULL
              AND core_intent.action IN ('buy_core','sell_core')
              AND core_mandate.core_mandate_event_id IS NOT NULL
          )"""

#: Report-path predicate: is this a core trade at all.  No join required, so a
#: read query needs only to stop INNER JOINing funding.
_CORE_ARM_PRESENT: LiteralString = "{t}.core_rebalance_intent_id IS NOT NULL"


def core_arm_joins(trade: LiteralString) -> LiteralString:
    """LEFT JOINs binding a trade's core intent and its paper-declared mandate."""
    return _CORE_ARM_JOINS.format(t=trade)


def core_arm_authorised(trade: LiteralString) -> LiteralString:
    """Predicate admitting a core trade that may be ACTED on. Fails closed.

    Requires :func:`core_arm_joins` on the same query.
    """
    return _CORE_ARM_AUTHORISED.format(t=trade)


def core_arm_present(trade: LiteralString) -> LiteralString:
    """Predicate admitting any core trade, for REPORT paths. Fails visible."""
    return _CORE_ARM_PRESENT.format(t=trade)
