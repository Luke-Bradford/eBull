"""Operational-duplicate canonical-instrument redirects (#819).

Some ticker conventions create a separate ``instruments`` row for the
same underlying security. The canonical example is eToro's ``.RTH``
suffix (regular trading hours) — ``AAPL.RTH`` and ``AAPL`` are the
same security; SEC filings only land under ``AAPL``'s CIK. The
``.RTH`` row carries no CIK (cik_discovery correctly resolves to the
underlying and the partial-unique CIK index in sql/143 blocks a
second instrument from claiming the same CIK).

Pre-#819 the operator hit an empty ``.RTH`` page — chart blank,
ownership pie blank, fundamentals blank. The fix:

  * Schema (sql/145): ``instruments.canonical_instrument_id`` FK to
    self. NULL = this row IS canonical (the default for every row).
  * Population (this module): for every instrument whose symbol ends
    in ``.RTH`` (suffix lowercase + UPPER), find the matching base
    instrument by stripping the suffix; set ``canonical_instrument_id``
    on the variant.
  * API (``app/api/instruments.py``): instrument summary exposes the
    canonical symbol so the frontend can redirect.
  * Frontend (``InstrumentPage``): when the loaded summary advertises a
    canonical symbol differing from the URL slug, ``<Navigate replace>``
    to the canonical page.

Scope: this mechanism is for OPERATIONAL DUPLICATES (.RTH and any
future similar suffix variants). Share-class siblings (GOOG/GOOGL,
BRK.A/BRK.B) MUST NOT use this — those are distinct securities
(distinct CUSIPs) that legitimately share an issuer CIK. See
``docs/settled-decisions.md`` "CIK = entity, CUSIP = security".

Match rule (kept narrow on purpose):

  * Variant symbol ends in the configured suffix (case-insensitive).
  * Base symbol == variant symbol with suffix stripped.
  * Base lives on a DIFFERENT exchange (RTH variants live on eToro's
    operational-duplicate exchange; the real listing is elsewhere).
  * Base exchange has ``asset_class='us_equity'`` (Codex pre-push
    round 1 — prevents a lone crypto/non-equity ``AAPL`` from being
    bound to ``AAPL.RTH``).
  * Currency match when both sides are non-NULL (.RTH variants
    sometimes carry NULL currency on dev DB).
  * Single base candidate, OR exactly one with
    ``is_primary_listing=TRUE``. Multiple ambiguous bases are skipped
    with a warning — the operator runbook covers manual binding.

Idempotency: re-running is a no-op for rows that already point at the
correct base.

Operator hand-bindings: the script NEVER overwrites a non-NULL
``canonical_instrument_id``. If a variant points at a base that
differs from what the rule would compute (because the operator
hand-bound it, or an earlier rule version computed differently), the
row is preserved + counted under
``redirects_skipped_already_set_differently``. To force a rebind, an
operator first ``UPDATE instruments SET canonical_instrument_id=NULL``
then re-runs the populate.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


# Suffix conventions handled by the script. ``.RTH`` is the only one
# in eBull's universe today; the structure leaves room for future
# operational-duplicate suffixes without rewriting the loop.
_SUFFIX_RULES: tuple[str, ...] = (".RTH",)


@dataclass(frozen=True)
class RedirectPopulationStats:
    """Outcome counters from a single populate run."""

    variants_scanned: int
    redirects_set: int
    redirects_already_correct: int
    redirects_skipped_no_base: int
    redirects_skipped_ambiguous: int
    # Codex pre-push round 1: variants whose canonical_instrument_id is
    # already set to a value the rule would NOT compute. Treated as
    # operator hand-bindings and preserved.
    redirects_skipped_already_set_differently: int = 0


def populate_canonical_redirects(
    conn: psycopg.Connection[Any],
) -> RedirectPopulationStats:
    """Set ``canonical_instrument_id`` for every ``.RTH``-style variant.

    Idempotent: a second run touches no rows when the universe is
    stable. Operator-driven manual mappings (a variant pointing at a
    base set by hand, not via this script's rule) are not disturbed
    because the script only updates rows whose current value is NULL
    or already points at the rule's computed base.

    Returns counters so the operator runbook can audit each invocation.

    The caller owns ``conn``'s commit. PR #1121 round 1 (Claude bot
    WARNING): a service that accepts an external connection MUST NOT
    commit on its own — doing so silently flushes any earlier
    mutations the caller staged on the same connection. The job
    wrapper below opens its own connection and commits explicitly.
    """
    variants_scanned = 0
    redirects_set = 0
    redirects_already_correct = 0
    redirects_skipped_no_base = 0
    redirects_skipped_ambiguous = 0
    redirects_skipped_already_set_differently = 0

    for suffix in _SUFFIX_RULES:
        # Pull every variant + already-resolved canonical_instrument_id
        # in one pass so we can decide "set", "already correct", or
        # "skip" without a per-row roundtrip.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT v.instrument_id, v.symbol, v.exchange, v.currency,
                       v.canonical_instrument_id
                  FROM instruments v
                 WHERE UPPER(v.symbol) LIKE %s ESCAPE '\\'
                """,
                (f"%{suffix.upper()}",),
            )
            variants = cur.fetchall()

        for instrument_id, symbol, exchange, currency, current_canonical in variants:
            variants_scanned += 1
            base_symbol = symbol[: -len(suffix)] if symbol.lower().endswith(suffix.lower()) else None
            if base_symbol is None:
                # Defensive: LIKE matched on UPPER but symbol may have
                # mixed-case ".rth". Skip rather than guess.
                continue

            with conn.cursor() as cur:
                # Exclude same-exchange rows: .RTH lives on eToro's
                # operational-duplicate exchange; the actual listing
                # is on a different exchange. Match by symbol, JOIN
                # exchanges to keep base universe-aligned (us_equity
                # only — Codex pre-push round 1 prevents a lone
                # crypto ``AAPL`` from being bound to ``AAPL.RTH``).
                # Currency match relaxed: variants carry NULL currency
                # on dev DB; require equality only when BOTH sides are
                # non-NULL.
                # Cast currency param to TEXT so a NULL bind doesn't
                # trip the planner's "could not determine data type"
                # error — psycopg can't infer NULL parameter types.
                cur.execute(
                    """
                    SELECT i.instrument_id, i.is_primary_listing
                      FROM instruments i
                      JOIN exchanges e ON e.exchange_id = i.exchange
                     WHERE UPPER(i.symbol) = %s
                       AND i.exchange IS DISTINCT FROM %s
                       AND i.instrument_id <> %s
                       AND e.asset_class = 'us_equity'
                       AND (
                            %s::TEXT IS NULL
                         OR i.currency IS NULL
                         OR i.currency = %s::TEXT
                       )
                    """,
                    (
                        base_symbol.upper(),
                        exchange,
                        instrument_id,
                        currency,
                        currency,
                    ),
                )
                bases = cur.fetchall()

            if not bases:
                redirects_skipped_no_base += 1
                logger.debug(
                    "canonical_redirects: variant=%s instrument_id=%d has no base; skipping",
                    symbol,
                    instrument_id,
                )
                continue

            # Single candidate → use it. Multiple → require exactly
            # one is_primary_listing=TRUE. Else skip ambiguous.
            target_id: int | None
            if len(bases) == 1:
                target_id = bases[0][0]
            else:
                primaries = [bid for (bid, is_primary) in bases if is_primary]
                if len(primaries) == 1:
                    target_id = primaries[0]
                else:
                    redirects_skipped_ambiguous += 1
                    logger.warning(
                        "canonical_redirects: variant=%s instrument_id=%d "
                        "has %d base candidates (primary=%d); "
                        "skipping (manual UPDATE needed)",
                        symbol,
                        instrument_id,
                        len(bases),
                        len(primaries),
                    )
                    continue
            if current_canonical == target_id:
                redirects_already_correct += 1
                continue

            # Codex pre-push round 1: never clobber a non-NULL value.
            # If the operator hand-bound a variant to a different base
            # (or an earlier rule version computed a different one),
            # the script leaves it alone. The escape hatch is a manual
            # ``UPDATE instruments SET canonical_instrument_id=NULL``
            # to reset before re-running.
            if current_canonical is not None:
                redirects_skipped_already_set_differently += 1
                logger.warning(
                    "canonical_redirects: variant=%s instrument_id=%d "
                    "already points at canonical=%d, rule would set %d; "
                    "preserving manual binding",
                    symbol,
                    instrument_id,
                    current_canonical,
                    target_id,
                )
                continue

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE instruments
                       SET canonical_instrument_id = %s
                     WHERE instrument_id = %s
                    """,
                    (target_id, instrument_id),
                )
            redirects_set += 1
            logger.info(
                "canonical_redirects: variant=%s -> base instrument_id=%d (%s)",
                symbol,
                target_id,
                base_symbol,
            )

    # Caller commits. PR #1121 WARNING fix — see docstring.
    logger.info(
        "canonical_redirects: scanned=%d set=%d already_correct=%d "
        "skipped_no_base=%d skipped_ambiguous=%d "
        "skipped_already_set_differently=%d",
        variants_scanned,
        redirects_set,
        redirects_already_correct,
        redirects_skipped_no_base,
        redirects_skipped_ambiguous,
        redirects_skipped_already_set_differently,
    )
    return RedirectPopulationStats(
        variants_scanned=variants_scanned,
        redirects_set=redirects_set,
        redirects_already_correct=redirects_already_correct,
        redirects_skipped_no_base=redirects_skipped_no_base,
        redirects_skipped_ambiguous=redirects_skipped_ambiguous,
        redirects_skipped_already_set_differently=redirects_skipped_already_set_differently,
    )


JOB_POPULATE_CANONICAL_REDIRECTS = "populate_canonical_redirects"


def populate_canonical_redirects_job() -> None:
    """Zero-arg job invoker — opens a connection and runs the populate.

    Registered in ``app/jobs/runtime.py``. The operator triggers it
    after a universe sync that may have introduced new ``.RTH``-style
    variants. Idempotent — safe to re-run any time.
    """
    from app.config import settings

    with psycopg.connect(settings.database_url) as conn:
        stats = populate_canonical_redirects(conn)
        # PR #1121 WARNING fix — the service is caller-owns-conn, so
        # the job wrapper that owns the connection commits explicitly.
        conn.commit()
        logger.info("populate_canonical_redirects_job: %s", stats)
