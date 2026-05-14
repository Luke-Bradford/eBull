"""Shared share-class sibling fan-out helper for manifest parsers.

Per #1102, multiple instruments may share an SEC issuer CIK —
share-class siblings (GOOG/GOOGL, BRK.A/BRK.B). Per-filing parsers
that write per-instrument rows must fan out across all siblings,
not collapse to one. This helper resolves the sibling set for a
given manifest row, with the fail-closed union pattern from PR #1152.

Used by ``sec_10k.py`` (PR #1152) and ``eight_k.py`` (PR #1158 —
dividend_events extraction). Other manifest parsers
(``def14a.py``, ``insider_345.py``, ``sec_13f_hr.py``,
``sec_n_port.py``, ``sec_13dg.py``) follow per-source fan-out
patterns and don't currently share this helper; future unification
is its own ticket.
"""

from __future__ import annotations

from typing import Any

import psycopg

from app.services.sec_identity import siblings_for_issuer_cik

# Sentinel used by adapters when the manifest row has no CIK. The
# helper short-circuits to the canonical sibling rather than calling
# ``siblings_for_issuer_cik`` with a nonsense value (which would
# raise ValueError on the non-numeric check).
CIK_MISSING_SENTINEL = "__missing__"


def resolve_siblings(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    issuer_cik: str,
) -> list[int]:
    """Resolve share-class siblings for fan-out.

    Always includes ``instrument_id`` in the returned set (PR #1152
    Codex checkpoint 2 HIGH): if ``siblings_for_issuer_cik`` returns
    a non-empty but incomplete set because the canonical sibling's
    ``external_identifiers`` row is missing or stale, dropping it
    from fan-out leaves the operator-visible page empty for the
    canonical listing. Failing closed by union-ing the manifest's
    ``instrument_id`` in keeps the canonical sibling's write safe
    even on a data-quality gap elsewhere.

    Sentinel branch returns just the canonical sibling because we
    have no CIK to query siblings against.
    """
    if issuer_cik == CIK_MISSING_SENTINEL:
        return [instrument_id]
    siblings = siblings_for_issuer_cik(conn, issuer_cik)
    return sorted(set(siblings) | {instrument_id})
