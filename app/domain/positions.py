"""Domain types for the positions table.

Shared between the write paths (``order_client`` for eBull-placed
orders, ``portfolio_sync`` for externally-discovered positions) and
the read paths (``services.portfolio``, ``api.portfolio``).  Keeping
the type here avoids a service-to-service import that would otherwise
have write-side modules depending on each other just to share a
string literal.
"""

from __future__ import annotations

from typing import Literal

#: Provenance of the *currently-open* position on a ``positions`` row.
#:
#: - ``"ebull"``       — the currently-open units were opened (or
#:                       reopened) by eBull's execution layer.
#: - ``"broker_sync"`` — the currently-open units were opened
#:                       externally (eToro UI, copy trading, etc.) and
#:                       discovered via the broker portfolio sync.
#:
#: "Currently-open" is the key qualifier.  On a close/reopen cycle the
#: source is reset to reflect the new opener — see the CASE WHEN
#: logic in ``order_client._update_position_buy`` and
#: ``portfolio_sync.sync_portfolio``.  Preserving source across a
#: close/reopen cycle would mislead the execution guard about who is
#: currently managing the position.
PositionSource = Literal["ebull", "broker_sync"]
