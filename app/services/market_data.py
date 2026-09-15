"""
Market data service.

Ingests daily OHLCV candles and current quotes for covered instruments,
computes rolling return and volatility features, and flags wide spreads.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal

import psycopg
from psycopg.rows import dict_row

from app.providers.market_data import MarketDataProvider, OHLCVBar, Quote
from app.services.price_window_verdict import assess_window, load_window_inputs
from app.services.strategy_core_quote_observation import (
    CoreQuoteObservation,
    record_core_quote_observations,
)
from app.services.strategy_core_quote_observation import (
    normalise_quote as normalise_core_quote,
)
from app.services.sync_orchestrator.exception_classifier import classify_exception
from app.services.sync_orchestrator.layer_types import FailureCategory, UpstreamUnreachableError
from app.services.sync_orchestrator.progress import report_progress
from app.services.technical_analysis import OHLCVRow, compute_indicators

logger = logging.getLogger(__name__)

# Batch circuit-breaker (#1833). When the eToro market-data API is
# unreachable, every per-instrument candle fetch hits the provider's 30s
# timeout and raises — walking all ~775 scoped instruments would burn
# ≈6.5h grinding through dead requests for a job that normally finishes in
# ~1 minute. After this many CONSECUTIVE *systemic* failures the candle
# loop aborts the whole batch with a clear terminal status instead.
# "Consecutive" counts ATTEMPTED fetches: a freshness-skipped instrument
# is neutral (no fetch, no reachability evidence) and neither increments
# nor resets the counter. The counter resets on any fetch that proves the
# server is still reachable — a clean fetch OR a per-instrument fault that
# still got a response (e.g. a 404 for a delisted symbol) — so a few
# genuinely-delisted instruments never trip it.
_CANDLE_BATCH_ABORT_LIMIT = 10

# Failure categories that indicate a WHOLE-BATCH outage (the next
# instrument will fail the same way), as opposed to a per-instrument fault
# (404 delisted → INTERNAL_ERROR, a unique-constraint clash → DB_CONSTRAINT,
# a feature-compute bug → INTERNAL_ERROR). Sourced from the single failure
# taxonomy in ``classify_exception`` so this never drifts from it:
#   * SOURCE_DOWN   — httpx.TransportError (DNS / connect / read timeout),
#                     5xx after retries, or a psycopg.OperationalError raised
#                     mid-fetch (transient DB blip inside the transaction).
#                     A HARD DB outage trips the freshness probe on the first
#                     instrument (outside the per-item try) and fails the run
#                     fast on its own — no grind to break.
#   * AUTH_EXPIRED  — 401/403: the broker session is dead for every call
#   * RATE_LIMITED  — 429 after the retry budget is exhausted
_SYSTEMIC_FAILURE_CATEGORIES = frozenset(
    {
        FailureCategory.SOURCE_DOWN,
        FailureCategory.AUTH_EXPIRED,
        FailureCategory.RATE_LIMITED,
    }
)

# #2262 — consecutive attempted-but-unmoved fetches after which an instrument
# reads as SUPPLY-LESS. The refresh is nightly, so 5 is roughly a week: long
# enough to absorb a market holiday, a mid-week halt, or a run that fired before
# the venue's close, and short enough that a delisted name stops consuming a
# capped T3 slot within days rather than forever.
_SUPPLY_LESS_CONSECUTIVE_MISSES = 5


def series_advanced(last_bar_before: date | None, last_bar_after: date | None) -> bool:
    """Did an attempted fetch actually move the series forward? (#2262)

    PUBLIC + pure so it is table-testable: this single predicate is the whole
    supply signal, because eToro answers HTTP 200 with nothing new for a
    supply-less instrument and there is no other observable.

    A series that went from "no bars" to "some bars" ADVANCED. A series whose
    last bar is unchanged did not — and neither did one that somehow went
    backwards, which is why this is ``>`` and not ``!=``.
    """
    if last_bar_after is None:
        return False
    return last_bar_before is None or last_bar_after > last_bar_before


def _record_supply_outcome(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    *,
    last_bar_before: date | None,
    last_bar_after: date | None,
) -> None:
    """Record whether an ATTEMPTED candle fetch actually moved the series (#2262).

    ⚠⚠ eToro returns HTTP 200 with nothing new for a supply-less instrument — no
    error, no 404, no exception. So this is keyed on the only observable there
    is: did ``MAX(price_date)`` move. A marker keyed on status or on an
    exception would never fire for any of the ~108 affected instruments.

    Called ONLY on an attempted fetch. A freshness skip is not an attempt (we
    never asked), and a FAILED fetch is neutral (the provider was unreachable,
    which is a different signal from the provider having nothing) — neither
    touches the counter, so neither can manufacture a supply-less verdict.
    """
    advanced = series_advanced(last_bar_before, last_bar_after)
    conn.execute(
        """
        INSERT INTO instrument_price_supply
            (instrument_id, consecutive_no_advance, last_attempt_at, last_advance_at, last_known_bar, updated_at)
        VALUES (%(iid)s, %(miss)s, now(), CASE WHEN %(advanced)s THEN now() END, %(last_bar)s, now())
        ON CONFLICT (instrument_id) DO UPDATE
           SET consecutive_no_advance = CASE
                   WHEN %(advanced)s THEN 0
                   ELSE instrument_price_supply.consecutive_no_advance + 1
               END,
               last_attempt_at = now(),
               last_advance_at = CASE
                   WHEN %(advanced)s THEN now()
                   ELSE instrument_price_supply.last_advance_at
               END,
               last_known_bar = %(last_bar)s,
               updated_at = now()
        """,
        {
            "iid": instrument_id,
            "miss": 0 if advanced else 1,
            "advanced": advanced,
            "last_bar": last_bar_after,
        },
    )


def _last_bar(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
) -> date | None:
    row = conn.execute(
        "SELECT MAX(price_date) FROM price_daily WHERE instrument_id = %(iid)s",
        {"iid": instrument_id},
    ).fetchone()
    return None if row is None else row[0]


# Default spread threshold from trading-policy.md.
# An instrument is flagged if (ask - bid) / mid > this value.
DEFAULT_MAX_SPREAD_PCT = Decimal("1.0")  # 1%

# Lookback windows in calendar days for rolling return computation.
# These are approximate (trading days vary); exact day counts are resolved
# from the available price history.
_RETURN_WINDOWS: dict[str, int] = {
    "return_1w": 7,
    "return_1m": 30,
    "return_3m": 91,
    "return_6m": 182,
    "return_1y": 365,
}
_VOLATILITY_WINDOW_DAYS = 30


@dataclass(frozen=True)
class DayChange:
    """Close-to-close day-change for one instrument, from ``price_daily``.

    Built from an instrument's two most recent **strictly-positive** closes.
    ``as_of`` is the latest close's ``price_date`` — the metric is stamped with
    it (settled-decisions.md:767 "latest closed session" as-of convention) so a
    stale close reads honestly rather than as "today". ``change_pct`` is a
    FRACTION (``-0.015`` = −1.5%), matching the ``formatPct`` frontend contract.

    ⚠ ``change_abs``/``change_pct`` are ``None`` when ``verdict`` is
    ``quarantined`` (#3046): the ratio between the two closes is not a return, so
    there is no day change to render. ``as_of``, ``last_close`` and ``prior_close``
    are retained in EVERY state — the closes are prices, and what the quarantine
    rules condemn is the ratio, never the level (``sql/247:83-88``). ⚠ That
    level-validity statement is scoped to a level break and is NOT asserted when
    ``bar_return_unusable`` is among ``reasons``; there the API carries the reason
    and this module makes no claim about the close.
    """

    as_of: date
    prior_date: date
    """The earlier operand's ``price_date``. Carried so the window is explicit:
    "as of" alone reads as a one-day change, and 208 of these windows are not one."""
    last_close: Decimal
    prior_close: Decimal
    change_abs: Decimal | None
    change_pct: Decimal | None
    verdict: str
    reasons: tuple[str, ...]


def compute_day_change(last_close: Decimal, prior_close: Decimal) -> Decimal | None:
    """Fractional close-to-close change, or ``None`` when ``prior_close <= 0``.

    A non-positive prior close is a non-price sentinel (``price_daily`` holds
    real ``close = 0`` rows — the same cross-surface invariant prevention-log
    #1428 documents for ``quotes.last``), so no meaningful change exists.
    """
    if prior_close <= 0:
        return None
    return (last_close - prior_close) / prior_close


def load_day_changes(
    conn: psycopg.Connection[object],
    instrument_ids: Sequence[int],
) -> dict[int, DayChange]:
    """Batch day-change over an instrument's two most-recent positive closes.

    One window query ranks ``close > 0`` rows per instrument (strictly-positive
    skips ``price_daily``'s real zero-close sentinels) and keeps the top two.
    Instruments with fewer than two positive closes are omitted (caller renders
    "—"). Fan-out-safe: PK ``(instrument_id, price_date)`` guarantees one row
    per date.

    ⚠⚠ #3046 — THE WINDOW IS ASSESSED, NOT ASSUMED. The two closes are raw vendor
    prices and their ratio is not automatically a return. Measured on the full live
    corpus before this landed: 208 of 12,262 instruments rendered a "day change" that
    is not one — ALNEV.PA at **+999,900%** across a 12-day hole, MOND at −99.18%
    across an unresolved level break. ``price_window_verdict`` composes the four
    contract clauses over ``(prior_date, as_of]``; a ``quarantined`` verdict nulls
    the change and keeps the closes.

    ⚠ Omitting an instrument for <2 positive closes is NOT a verdict. Absent data
    and an unverified window are different states and are not conflated: a missing
    key carries no ``reasons`` at all.

    ⚠ The original operand pair is preserved. Nothing substitutes an older bar for a
    condemned one — a day change computed from a replaced operand would be a
    different quantity wearing the same label.
    """
    ids = list({int(i) for i in instrument_ids})
    if not ids:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT instrument_id, price_date, close,
                       row_number() OVER (
                           PARTITION BY instrument_id ORDER BY price_date DESC
                       ) AS rn
                FROM price_daily
                WHERE instrument_id = ANY(%(ids)s) AND close > 0
            ), pair AS (
                SELECT instrument_id,
                       max(close)      FILTER (WHERE rn = 1) AS last_close,
                       max(price_date) FILTER (WHERE rn = 1) AS as_of,
                       max(close)      FILTER (WHERE rn = 2) AS prior_close,
                       max(price_date) FILTER (WHERE rn = 2) AS prior_date
                FROM ranked
                WHERE rn <= 2
                GROUP BY instrument_id
                HAVING count(*) = 2
            )
            SELECT pair.*,
                   -- ⚠ ``rule_w2`` needs the STORED bar count, never a rank and
                   -- never a calendar estimate. The two most recent POSITIVE closes
                   -- need not be adjacent stored bars: a zero-close sentinel row
                   -- between them is a third bar, and counting it is the difference
                   -- between a stretched horizon and an ordinary one.
                   (SELECT count(*) FROM price_daily d
                     WHERE d.instrument_id = pair.instrument_id
                       AND d.price_date BETWEEN pair.prior_date AND pair.as_of
                   ) AS bar_count
            FROM pair
            """,
            {"ids": ids},
        )
        rows = cur.fetchall()

    # ⚠ Guard on what was actually CONSUMED, not on ``rows``'s truthiness. A cursor
    # double that is truthy while iterating empty — which is what ``MagicMock`` is,
    # and what 11 of the summary-endpoint tests hand this function — passes
    # ``if not rows`` and then empties ``min()``. Materialising the operands once
    # settles it for any container, and is one pass instead of two.
    windows = {int(r["instrument_id"]): r["prior_date"] for r in rows}  # type: ignore[arg-type,misc]
    if not windows:
        return {}
    # ⚠ Each instrument is bounded at ITS OWN window start, not at the batch minimum:
    # one instrument with a 2020 window must not drag every other instrument's scan
    # back with it (measured 4x on the worst page — see ``load_window_inputs``).
    inputs = load_window_inputs(conn, windows)

    out: dict[int, DayChange] = {}
    for r in rows:
        instrument_id = int(r["instrument_id"])  # type: ignore[arg-type]
        last_close = r["last_close"]  # type: ignore[assignment]
        prior_close = r["prior_close"]  # type: ignore[assignment]
        # ``prior_close > 0`` is guaranteed by the ``WHERE close > 0`` filter, so
        # this is never None in practice — the guard narrows the type for the
        # checker and routes the formula through the single tested source
        # (``compute_day_change``) rather than duplicating it inline.
        pct = compute_day_change(last_close, prior_close)
        if pct is None:  # pragma: no cover — defensive; filter guarantees prior_close > 0
            continue
        assessment = assess_window(
            inputs.get(instrument_id),
            window_start=r["prior_date"],  # type: ignore[arg-type]
            window_end=r["as_of"],  # type: ignore[arg-type]
            bar_count=int(r["bar_count"]),  # type: ignore[arg-type]
            # Clause 5's operand. ⚠ These are the two STORED BARS this day change
            # divides, which for this consumer happen to equal the window bounds —
            # they are passed explicitly because that coincidence is this caller's
            # property, not a general one (``assess_window``).
            endpoint_bar_dates=(r["prior_date"], r["as_of"]),  # type: ignore[arg-type]
        )
        suppressed = assessment.is_quarantined
        out[instrument_id] = DayChange(
            as_of=r["as_of"],  # type: ignore[arg-type]
            prior_date=r["prior_date"],  # type: ignore[arg-type]
            last_close=last_close,
            prior_close=prior_close,
            change_abs=None if suppressed else last_close - prior_close,
            change_pct=None if suppressed else pct,
            verdict=assessment.verdict,
            reasons=assessment.reasons,
        )
    return out


@dataclass(frozen=True)
class MarketRefreshSummary:
    instruments_refreshed: int
    candle_rows_upserted: int
    features_computed: int
    quotes_updated: int
    quotes_skipped: int
    spread_flags_set: int
    # #1293 — disambiguate a candle_rows_upserted=0 outcome. ``candles_skipped``
    # counts instruments skipped because their candles were already fresh
    # (legitimate no-op). ``candles_failed`` counts instruments whose refresh
    # raised — that wraps the WHOLE per-instrument transaction (provider
    # fetch + ``_upsert_candles`` + feature compute), so a DB/write error
    # counts too, not only an eToro fetch/session failure; callers must phrase
    # the cause accordingly. Without these the caller cannot tell a healthy
    # "everything already fresh" run from a broken "every fetch failed" run;
    # both report 0 candles written.
    candles_skipped: int = 0
    candles_failed: int = 0
    # #2066 — instruments whose incremental overlap showed a ratio-scale
    # close mismatch (split/adjustment event) and were healed with an
    # in-run full-history re-fetch.
    adjustment_refetches: int = 0
    # #2414 — the subset of ``candle_rows_upserted`` that OVERWROTE an
    # existing bar's OHLCV with a different value, as opposed to appending a
    # new one. Not a second count of the same thing: an insert extends the
    # series, a revision destroys a value some ``strategy_signals`` row may
    # already have decided on, and ``price_daily`` keeps no prior value or
    # audit column, so this counter is the ONLY place a revision is ever
    # visible. #2414 asked "how often is a bar revised, and by how much"; the
    # honest answer before this field was that stored state cannot say,
    # because the overwrite and the evidence of it happen in one statement.
    # This makes the rate measurable going forward — it does NOT recover the
    # history, and it does not fix the ledger collision #2414 is really about.
    candle_rows_revised: int = 0
    # #2414 — the same revisions, split by HOW FAR BACK the overwritten bar was.
    # The rate alone cannot choose between the ticket's two candidate fixes: an
    # embargo ("do not decide on a bar younger than the correction buffer") is
    # sufficient if and only if revisions never reach past it, and a corpus stamp
    # in the signal key is required if they do. Measured 2026-09-13 on the 21
    # stored runs that carry `bars_revised`: 4,076 of 36,636 bar-writes (11.1%)
    # were revisions, so the "rate might be zero" branch is already refuted — but
    # nothing stored says whether those were the in-progress bar or four-year-old
    # history, and the two have opposite consequences.
    # ⚠ `default_factory`, not `{}` — a mutable default is shared across every
    # instance, and this dataclass is constructed per run.
    candle_revision_age_days: dict[str, int] = field(default_factory=dict)
    # The oldest single revision in the run, in calendar days. Kept beside the
    # histogram because one deep revision IS the finding and a bucket count of 1
    # does not say how deep. `None` when nothing was revised.
    candle_revision_max_age_days: int | None = None
    # #2414 — the same revisions again, split by WHICH WRITE BRANCH produced
    # them. Age turned out not to identify the branch: an incremental fetch is
    # bounded in BARS not in days (a sparse name's third-newest bar can be
    # months old), a heal can revise a handful of rows, and a stale
    # re-observation can rewrite four years. So depth cannot be inverted to a
    # cause, and the run aggregate cannot even say which instrument a maximum
    # belongs to.
    #
    # ⚠⚠ THE BRANCH IS NOT THE ECONOMIC CAUSE. One fetch can rewrite bars for
    # more than one underlying reason at once, including inside a heal, so this
    # is an upper bound on attribution. It narrows the population #2414's
    # supersession question has to examine; it does not classify it.
    candle_revisions_by_cause: dict[str, int] = field(default_factory=dict)
    # The deepest revision within each cause, in calendar days. Present for
    # exactly the causes with a positive count.
    candle_revision_max_age_by_cause: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class QuoteRefreshSummary:
    """Outcome of a quotes-only refresh (#2271)."""

    instruments_requested: int
    quotes_updated: int
    quotes_skipped: int
    # Count of FETCHED quotes whose spread exceeded the threshold — NOT a
    # count of rows now flagged in the table. The two diverge when
    # ``_upsert_quote``'s monotonicity guard rejects a stale snapshot: the
    # fetched quote is still counted here, while the stored row keeps the
    # fresher tick's flag. Named for the fetch because that is what this
    # function observes; the table is the authority on stored state.
    spread_flags_set: int
    # True when the provider's batch fetch itself failed, so every
    # instrument counts as skipped for a reason that is NOT "the provider
    # had no quote for it". Without this the caller cannot tell a total
    # upstream outage from a universe of untraded instruments — both
    # report quotes_updated=0 (#1293 / #2218 shape).
    batch_failed: bool = False
    # The exception that caused ``batch_failed``, retained so a caller that
    # owns a job_runs row can re-raise it and have ``classify_exception`` key
    # off the original httpx type (AUTH_EXPIRED / RATE_LIMITED / SOURCE_DOWN).
    # Swallowing it into a bare bool would force the job to either report
    # success or invent a category (#2271, Codex round 2).
    batch_error: Exception | None = None
    # Rows written to ``strategy_core_quote_observations`` this tick (#2833
    # step 2). Reported rather than left implicit so the lane cannot become a
    # writer with no reader: a candidate cohort that silently stops accruing
    # would otherwise look identical to one whose bar is still forming.
    core_observations_written: int = 0
    # Observation writes that raised. Isolated from the quote refresh (the
    # lane must never break eight headless readers) but NOT swallowed: a
    # persistent non-zero count is how a broken lane — bad SQL, missing
    # column — surfaces instead of logging once an hour forever.
    core_observation_failures: int = 0


def refresh_quotes(
    provider: MarketDataProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instruments: list[tuple[int, str]],
    *,
    max_spread_pct: Decimal = DEFAULT_MAX_SPREAD_PCT,
    observe_instrument_ids: frozenset[int] | None = None,
) -> QuoteRefreshSummary:
    """Batch-fetch quotes for *instruments* and upsert each one.

    Extracted from ``refresh_market_data``'s quote phase (#2271) so the
    scheduled ``quotes_refresh`` job can reach it without also pulling
    candles. ``refresh_market_data`` still calls it, so there is one
    implementation, not two.

    Why this needed extracting: both of the callers that pass through
    ``refresh_market_data`` set ``skip_quotes=True``, so the quote phase was
    unreachable in production. Combined with the WS subscriber only writing
    for instruments an SSE stream has on screen, nothing wrote the ``quotes``
    table unless an operator had the page open — while scoring, the portfolio
    manager and the execution guard all read it headless.

    Per-instrument upsert failures are logged and skipped; a failure of the
    batch fetch itself aborts the whole set and is reported as
    ``batch_failed`` rather than silently reading as "no quotes available".

    ``observe_instrument_ids`` additionally records each of those instruments
    as an immutable hourly row in ``strategy_core_quote_observations`` (#2833
    step 2).  The ``quotes`` table is one MUTABLE row per instrument, so it
    can never accumulate the multi-day spread sample the core sleeve's pass
    bar reads -- see sql/366.  The quotes are already in hand here, so the
    lane costs no extra provider calls.  A missing quote is recorded too:
    thinning the sample silently would bias the very percentile being
    measured.
    """
    if not instruments:
        return QuoteRefreshSummary(0, 0, 0, 0)

    quotes_updated = 0
    quotes_skipped = 0
    spread_flags_set = 0

    all_ids = [iid for iid, _ in instruments]
    try:
        quotes = provider.get_quotes(all_ids)
    except Exception as exc:
        logger.warning("Failed to batch-fetch quotes, skipping all quote updates", exc_info=True)
        return QuoteRefreshSummary(
            instruments_requested=len(instruments),
            quotes_updated=0,
            quotes_skipped=len(instruments),
            spread_flags_set=0,
            batch_failed=True,
            batch_error=exc,
        )

    quote_map: dict[int, Quote] = {q.instrument_id: q for q in quotes}
    observed_ids = observe_instrument_ids or frozenset()
    observed_at = datetime.now(tz=UTC)
    core_observations_written = 0
    core_observation_failures = 0
    # Normalised in the same pass as the quote upserts but WRITTEN ONCE
    # below.  ``quotes_refresh`` opens its connection with autocommit=True,
    # so a ``conn.transaction()`` per candidate is a real BEGIN/COMMIT round
    # trip each -- not a savepoint, which is what an earlier version of this
    # comment claimed (Codex ckpt-3).  Batching removes that per-candidate
    # cost without giving up per-instrument coverage: ``refusal_reason`` is
    # ordinary row data and survives batching, and every row normalise_quote
    # returns is shape-valid by construction, so a failure here is systemic
    # (missing table, bad column) and would have failed each isolated insert
    # anyway.
    #
    # ⚠ The candidate cohort is fetched SEPARATELY, and that is not
    # redundancy.  ``get_quotes`` chunks internally and, on a partial chunk
    # failure, logs it and returns only the chunks that SUCCEEDED -- so a
    # candidate sitting in a failed chunk comes back as ``None``, exactly
    # like an instrument eToro genuinely has no quote for.  Writing that as
    # ``provider_omitted_quote`` would record a transport failure as broker
    # evidence, which is the error `prove_2603_core_eligibility` names in its
    # own docstring: "a transport failure records NOTHING" (Codex ckpt-3).
    # The cohort is small (one chunk today), so its fetch either succeeds
    # wholly -- making every remaining ``None`` a genuine omission -- or
    # raises, and we record nothing at all for the tick.
    candidate_ids = sorted(observed_ids)
    pending_observations: list[CoreQuoteObservation] = []
    if candidate_ids:
        try:
            # Split by the provider's own batch size so each call is exactly
            # ONE upstream request: it then either returns or raises, and no
            # id can go missing to a swallowed chunk. Fetching the cohort in
            # one call would reintroduce the ambiguity as soon as it outgrew
            # a single chunk (Codex ckpt-3, second round).
            stride = max(provider.quote_batch_size, 1)
            candidate_quotes: dict[int, Quote] = {}
            for start in range(0, len(candidate_ids), stride):
                chunk = candidate_ids[start : start + stride]
                candidate_quotes.update({q.instrument_id: q for q in provider.get_quotes(chunk)})
        except Exception:
            core_observation_failures = len(candidate_ids)
            logger.warning(
                "Core candidate quote fetch FAILED for %d instrument(s); recording no observations "
                "rather than storing a transport failure as absence of a quote",
                len(candidate_ids),
                exc_info=True,
            )
        else:
            pending_observations = [
                normalise_core_quote(
                    instrument_id=instrument_id,
                    quote=candidate_quotes.get(instrument_id),
                    observed_at=observed_at,
                )
                # Includes candidates with NO quote: against a determinate
                # fetch that absence is real coverage evidence, and dropping
                # it would let the sample quietly shrink without the
                # percentile moving.
                for instrument_id in candidate_ids
            ]
    if pending_observations:
        try:
            with conn.transaction():
                core_observations_written = record_core_quote_observations(conn, pending_observations)
        except Exception:
            # Never let the evidence lane break the quote refresh that eight
            # headless services depend on (#2271) -- but do not let it fail
            # SILENTLY either.  A bad column or bad SQL here would otherwise
            # log once an hour forever while the candidate's sample never
            # grows, which is this repo's "job that no-ops and reports
            # success" class.  The counter is the detector: `quotes_refresh`
            # logs it every tick, so a broken lane reads as written=0
            # failures=N rather than being inferred from absence.
            core_observation_failures = len(pending_observations)
            logger.warning(
                "Failed to record %d core quote observation(s); the candidate spread sample is not accruing this tick",
                len(pending_observations),
                exc_info=True,
            )

    for instrument_id, symbol in instruments:
        quote = quote_map.get(instrument_id)
        if quote is None:
            logger.debug("No quote returned for %s (id=%d), skipping quote upsert", symbol, instrument_id)
            quotes_skipped += 1
            continue
        try:
            with conn.transaction():
                flagged = _upsert_quote(conn, instrument_id, quote, max_spread_pct)
                quotes_updated += 1
                if flagged:
                    spread_flags_set += 1
        except Exception:
            logger.warning(
                "Failed to upsert quote for %s (id=%d), skipping",
                symbol,
                instrument_id,
                exc_info=True,
            )

    return QuoteRefreshSummary(
        instruments_requested=len(instruments),
        quotes_updated=quotes_updated,
        quotes_skipped=quotes_skipped,
        spread_flags_set=spread_flags_set,
        core_observations_written=core_observations_written,
        core_observation_failures=core_observation_failures,
    )


def refresh_market_data(
    provider: MarketDataProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instruments: list[tuple[int, str]],  # [(instrument_id, symbol), ...]
    lookback_days: int = 1000,
    max_spread_pct: Decimal = DEFAULT_MAX_SPREAD_PCT,
    *,
    skip_quotes: bool = False,
    force_backfill: bool = False,
    consecutive_failure_limit: int = _CANDLE_BATCH_ABORT_LIMIT,
    fresh_through: date | None = None,
) -> MarketRefreshSummary:
    """
    For each instrument: fetch candles, upsert to price_daily, compute
    features, then (unless skip_quotes=True) batch-fetch quotes and
    upsert with spread flag.

    When skip_quotes is True, quote fetching and upserting are skipped
    entirely. Use this when a separate hourly job owns quote freshness
    (e.g. fx_rates_refresh).

    When force_backfill is True, every instrument fetches the full
    ``lookback_days`` window regardless of whether incremental mode
    would otherwise apply. Used for the one-shot deepening invocation
    (#603) — the daily scheduled refresh leaves it False so steady-state
    eToro call weight stays at the incremental cadence.

    instruments is a list of (instrument_id, symbol) tuples — instrument_id
    must already exist in the instruments table. symbol is used for logging.

    ``consecutive_failure_limit`` (#1833) is the batch circuit-breaker
    threshold: after this many CONSECUTIVE systemic candle-fetch failures
    (provider unreachable, session dead, rate-limited) the loop raises
    ``UpstreamUnreachableError`` and aborts the rest of the batch instead
    of grinding through hundreds of per-instrument 30s timeouts. "Consecutive"
    counts attempted fetches — a freshness-skipped instrument is neutral; a
    reachable response (clean fetch or a 404) resets the counter. Pass
    ``<= 0`` to disable the breaker (walk every instrument regardless).

    ``fresh_through`` is the caller's completed provider-session boundary.
    When omitted, the legacy weekday/date boundary remains in force. Long
    scheduled sweeps must pass it explicitly so a pre-open retry does not
    fetch and retain a forming same-day candle merely because UTC advanced.

    Raw provider responses are persisted by the provider before being returned.

    CONNECTION CONTRACT (#2269) — ``conn`` MUST be autocommit, or the
    per-instrument atomicity this function advertises does not exist. Each
    instrument's work is scoped by ``with conn.transaction()``, which psycopg3
    turns into a real ``BEGIN``/``COMMIT`` only when no transaction is already
    open. On a non-autocommit connection the caller's first ``execute`` has
    already opened one, so every block degrades to ``SAVEPOINT``/``RELEASE``
    and nothing is durable until the CALLER commits — for a 12k-instrument
    sweep that is hours of work riding on one transaction, lost whole on any
    restart. This function must not commit it away itself: it does not own the
    connection (prevention log, "Mid-transaction ``conn.commit()`` in service
    functions"). The caller supplies the boundary; we only check and warn.
    """
    if not conn.autocommit:
        logger.warning(
            "refresh_market_data called on a NON-AUTOCOMMIT connection (%d instruments) — "
            "per-instrument transactions degrade to savepoints and NOTHING is durable until "
            "the caller commits. A restart mid-sweep loses the entire run (#2269).",
            len(instruments),
        )

    candle_rows_upserted = 0
    candle_rows_revised = 0
    candle_revision_age_days: dict[str, int] = {}
    candle_revision_max_age_days: int | None = None
    candle_revisions_by_cause: dict[str, int] = {}
    candle_revision_max_age_by_cause: dict[str, int] = {}
    features_computed = 0
    quotes_updated = 0
    quotes_skipped = 0
    spread_flags_set = 0

    today = date.today()
    freshness_target = fresh_through or most_recent_trading_day(today)
    candles_skipped = 0
    candles_failed = 0

    # --- Candles: per-instrument (with freshness skip + two-mode fetch) ---
    # Two-mode fetch (#271):
    #   * Backfill mode — instrument has NO prior candles (new to the
    #     universe, or gap detected). Pull full `lookback_days` history.
    #     Default 1000 — eToro's hard ceiling per request (#603 raised
    #     from 400 → 1000). 1000 trading days ≈ 4 calendar years of
    #     price points, which is the most we can fit in a single fetch.
    #     The endpoint is count-based with no from_date pagination, so
    #     we cannot deepen further without re-fetching everything.
    #   * Incremental mode — instrument already has candle history.
    #     Pull only INCREMENTAL_FETCH_BARS bars (yesterday + today +
    #     correction buffer). The upsert dedupes on (instrument_id,
    #     price_date) so overlap with existing rows is harmless.
    # On a typical day, ~100% of Tier 1/2 instruments are in incremental
    # mode — eToro call weight stays at 3 × ~500 instruments (~1500
    # rows). The 1000-bar default only fires on initial seed,
    # gap-detect, or the one-shot ``force_backfill=True`` deepening.
    total = len(instruments)
    # #1833 batch circuit-breaker — consecutive systemic failures.
    consecutive_systemic_failures = 0
    adjustment_refetches = 0
    for idx, (instrument_id, symbol) in enumerate(instruments, start=1):
        if not force_backfill and _candles_are_fresh(conn, instrument_id, today, fresh_through=freshness_target):
            candles_skipped += 1
            report_progress(idx, total)
            continue
        if force_backfill:
            # ⚠ No fetch reason: this path never consults `_candles_fetch_count`,
            # and `revision_cause` reports `force_backfill` for it rather than
            # inferring one from the size. `None` and not `""` — an empty string
            # is a value that can reach a counter key, `None` is one the type
            # checker forces every consumer to handle.
            fetch_count, fetch_reason = lookback_days, None
        else:
            fetch_count, fetch_reason = _candles_fetch_count(
                conn, instrument_id, default=lookback_days, today=freshness_target
            )
        upserted = 0
        revised = 0
        revision_ages: dict[str, int] = {}
        revision_max_age: int | None = None
        computed = 0
        adjustment_detected = False
        try:
            # #2262 — snapshot BEFORE the fetch so the supply marker can tell
            # "the provider had nothing" from "we never asked". INSIDE the
            # per-instrument try (and outside the transaction below, so a
            # rolled-back write cannot corrupt the baseline): a transient DB
            # error reading it is a per-instrument fault like any other, and
            # raising it here would abort the whole batch loop, which is the
            # one thing this loop's error handling exists to prevent.
            last_bar_before = _last_bar(conn, instrument_id)
            with conn.transaction():
                bars = provider.get_daily_candles(instrument_id, fetch_count)
                if fresh_through is not None:
                    # A pre-open/manual retry can receive a forming candle for
                    # a civil date beyond the declared completed-session
                    # boundary. Keep the provider response in its bounded raw
                    # audit store, but never publish that forming value as a
                    # daily close in price_daily (#2572).
                    bars = [bar for bar in bars if bar.price_date <= fresh_through]
                # #2066 split-cliff guard: provider history is back-adjusted
                # at fetch time, so a future split re-bases every bar — but an
                # incremental fetch only rewrites the overlap window, leaving
                # all older rows on the old basis (a permanent cliff at the
                # buffer edge). The overlap re-fetch is the one place the two
                # bases meet: a ratio-scale close mismatch on an already-stored
                # date = adjustment event → heal same-day with an in-run
                # full-history re-fetch (idempotent upsert rewrites the series).
                if bars and not force_backfill and fetch_count == _INCREMENTAL_FETCH_BARS:
                    stored = _stored_overlap_closes(conn, instrument_id, [b.price_date for b in bars])
                    ratio = detect_adjustment_event(stored, bars)
                    if ratio is not None:
                        logger.warning(
                            "Adjustment event detected for %s (id=%d): overlap close ratio %s — "
                            "re-fetching full %d-bar history to heal the series",
                            symbol,
                            instrument_id,
                            ratio,
                            lookback_days,
                        )
                        bars = provider.get_daily_candles(instrument_id, lookback_days)
                        if fresh_through is not None:
                            bars = [bar for bar in bars if bar.price_date <= fresh_through]
                        # An empty heal re-fetch wrote nothing — a heal is
                        # only a heal if the series was actually rewritten.
                        adjustment_detected = bool(bars)
                if bars:
                    # #2414 item 2 — the frontier for backdated-insert
                    # classification, read INSIDE this transaction rather than
                    # reusing `last_bar_before` above (which is read before
                    # `BEGIN` on purpose, for #2262). See `_observed_frontier`.
                    frontier_before = _observed_frontier(conn, instrument_id)
                    outcome = _upsert_candles(
                        conn,
                        instrument_id,
                        bars,
                        reference_date=freshness_target,
                        frontier_before=frontier_before,
                    )
                    revised = outcome.revised
                    upserted = outcome.inserted + revised
                    revision_ages = outcome.revision_age_days
                    revision_max_age = outcome.revision_max_age_days
                    # #2414 — the IDENTITY of each overwritten bar, written
                    # INSIDE this transaction so it rolls back with the bar
                    # write it describes. The counters below are merged AFTER
                    # commit for the opposite reason (#1293): a counter cannot
                    # roll back, a row can, and a revision row that outlived a
                    # rolled-back write would assert a change `price_daily` has
                    # no record of and nothing left to contradict.
                    #
                    # ⚠ `revision_cause` is a pure function of three values that
                    # do not change between here and the post-commit block
                    # below, and is STILL called separately there rather than
                    # hoisted out of the transaction — the counter block's "same
                    # keys in both maps" invariant is pinned by tests and is left
                    # untouched. The local below is shared only by the two AUDIT
                    # writers in this block, which must agree with each other by
                    # construction: one fetch has one write branch, and two rows
                    # describing the same fetch disagreeing about it would be a
                    # defect no consumer could detect.
                    write_branch = revision_cause(
                        adjustment_detected=adjustment_detected,
                        force_backfill=force_backfill,
                        fetch_reason=fetch_reason,
                    )
                    _record_bar_revisions(
                        conn,
                        instrument_id,
                        outcome.revised_bar_dates,
                        cause=write_branch,
                    )
                    # #2414 item 2 — the other mutation class, same transaction,
                    # same rollback property. `frontier_before` is not None
                    # whenever this list is non-empty: `_upsert_candles` can only
                    # classify a bar as backdated by comparing against it.
                    if outcome.backdated_insert_dates:
                        assert frontier_before is not None
                        _record_backdated_inserts(
                            conn,
                            instrument_id,
                            outcome.backdated_insert_dates,
                            frontier_before=frontier_before,
                            cause=write_branch,
                        )
                    computed = _compute_and_store_features(conn, instrument_id)
            # Accumulate the running totals ONLY after the transaction has
            # committed cleanly (#1293 / Codex): incrementing inside the
            # ``with`` block would over-report rows for an instrument whose
            # feature-compute or commit later raised and rolled the write back
            # — and that same instrument is also counted in ``candles_failed``.
            candle_rows_upserted += upserted
            candle_rows_revised += revised
            # Merged here rather than inside the `with` for the same #1293
            # reason as the counters above: an instrument whose commit later
            # raised did not revise anything.
            for bucket, count in revision_ages.items():
                candle_revision_age_days[bucket] = candle_revision_age_days.get(bucket, 0) + count
            if revision_max_age is not None:
                candle_revision_max_age_days = (
                    revision_max_age
                    if candle_revision_max_age_days is None
                    else max(candle_revision_max_age_days, revision_max_age)
                )
            # #2414 — the same revisions, attributed to the branch that wrote
            # them. Gated on `revised` so a cause never appears with a zero
            # count: the by-cause map and its max-age map must have exactly the
            # same keys, which is the invariant the tests pin.
            if revised:
                cause = revision_cause(
                    adjustment_detected=adjustment_detected,
                    force_backfill=force_backfill,
                    fetch_reason=fetch_reason,
                )
                candle_revisions_by_cause[cause] = candle_revisions_by_cause.get(cause, 0) + revised
                # ⚠ `revised` and `revision_max_age` are set in the SAME branch of
                # `_upsert_candles`, so this is never skipped for a positive count —
                # which is what makes "same keys in both maps" an invariant rather
                # than a hope. The narrowing is for the type checker, and it keeps a
                # producer change from silently inventing a cause with no depth.
                if revision_max_age is not None:
                    prior = candle_revision_max_age_by_cause.get(cause)
                    # `max` over the RAW age, so a future-dated bar's negative age
                    # survives here exactly as it does in the global maximum.
                    candle_revision_max_age_by_cause[cause] = (
                        revision_max_age if prior is None else max(prior, revision_max_age)
                    )
            features_computed += computed
            # Counted only after a clean commit (same #1293 rule as the row
            # totals) — a heal whose re-fetch or write failed did NOT happen.
            if adjustment_detected:
                adjustment_refetches += 1
            # A clean fetch proves the provider + DB are reachable → reset.
            consecutive_systemic_failures = 0
            # #2262 — the fetch was attempted and returned cleanly. Whether it
            # returned anything NEW is the supply signal, and it is the only
            # one there is: a 200-with-nothing looks identical to a 200-with-a-
            # bar at every layer above this one.
            try:
                with conn.transaction():
                    _record_supply_outcome(
                        conn,
                        instrument_id,
                        last_bar_before=last_bar_before,
                        last_bar_after=_last_bar(conn, instrument_id),
                    )
            except Exception:
                # DELIBERATELY BROAD, against the usual rule. The candle write
                # has already COMMITTED at this point. Any exception escaping
                # here reaches the outer handler, which increments
                # ``candles_failed`` and logs the instrument as a failed refresh
                # — mislabelling a successful fetch, and corrupting the job's own
                # health signal in the direction #2218 documents (a run whose
                # reported outcome does not match what it actually did).
                #
                # The usual objection to a broad except is that it hides a
                # genuine TypeError/AttributeError bug. That is answered by
                # scope and by ``exc_info``: this guards two bookkeeping calls,
                # not a work path, and the full traceback is logged at WARNING
                # every time. A bug here is loud; it just is not fatal.
                logger.warning(
                    "Failed to record price-supply outcome for %s (id=%d)", symbol, instrument_id, exc_info=True
                )
        except Exception as exc:
            candles_failed += 1
            logger.warning("Failed to refresh candles for %s (id=%d), skipping", symbol, instrument_id, exc_info=True)
            category = classify_exception(exc)
            if category in _SYSTEMIC_FAILURE_CATEGORIES:
                consecutive_systemic_failures += 1
                if 0 < consecutive_failure_limit <= consecutive_systemic_failures:
                    # Whole-batch outage — fail FAST with the triggering
                    # category instead of walking the remaining instruments
                    # through the same 30s-timeout grind (#1833). report_progress
                    # the partial position first so the run's last heartbeat
                    # reflects where it stopped.
                    report_progress(idx, total, force=True)
                    raise UpstreamUnreachableError(
                        category,
                        f"{consecutive_systemic_failures} consecutive systemic candle-fetch "
                        f"failures (aborted batch of {total} after {idx} instruments; "
                        f"last failure on {symbol} id={instrument_id})",
                    ) from exc
            else:
                # A per-instrument fault (404 delisted, DB-constraint clash,
                # feature-compute bug) proves the server still responds →
                # reset so a sprinkling of dead symbols never trips the breaker.
                consecutive_systemic_failures = 0
        report_progress(idx, total)

    # Final force-tick so items_done lands at the loop boundary even
    # if the last increment was below the throttle threshold.
    report_progress(total, total, force=True)

    if candles_skipped:
        logger.info("Candle freshness skip: %d/%d instruments already fresh", candles_skipped, len(instruments))

    # --- Quotes: batch fetch, then per-instrument upsert ---
    # When skip_quotes is True, quote freshness is owned by the hourly
    # quotes_refresh job — the daily candle job must not shadow those
    # fresher values with stale end-of-day data.
    if not skip_quotes:
        quote_result = refresh_quotes(provider, conn, instruments, max_spread_pct=max_spread_pct)
        quotes_updated = quote_result.quotes_updated
        quotes_skipped = quote_result.quotes_skipped
        spread_flags_set = quote_result.spread_flags_set

    # #2414 — logged HERE and not only in the scheduler, because the scheduler is
    # the only caller that persists `progress_json`. `scripts/rebackfill_candles_5y.py`
    # is the sole `force_backfill=True` caller and it reports neither revisions nor
    # causes, so without this line the `force_backfill` cause could never be observed
    # anywhere. Gated on a non-zero count: a quiet run must stay quiet.
    if candle_rows_revised:
        logger.info(
            "Candle revisions: %d bars, by cause %s, deepest by cause %s (max %s days)",
            candle_rows_revised,
            dict(sorted(candle_revisions_by_cause.items())),
            dict(sorted(candle_revision_max_age_by_cause.items())),
            candle_revision_max_age_days,
        )

    return MarketRefreshSummary(
        instruments_refreshed=len(instruments),
        candle_rows_upserted=candle_rows_upserted,
        candle_rows_revised=candle_rows_revised,
        candle_revision_age_days=candle_revision_age_days,
        candle_revision_max_age_days=candle_revision_max_age_days,
        candle_revisions_by_cause=candle_revisions_by_cause,
        candle_revision_max_age_by_cause=candle_revision_max_age_by_cause,
        features_computed=features_computed,
        quotes_updated=quotes_updated,
        quotes_skipped=quotes_skipped,
        spread_flags_set=spread_flags_set,
        candles_skipped=candles_skipped,
        candles_failed=candles_failed,
        adjustment_refetches=adjustment_refetches,
    )


def most_recent_trading_day(today: date) -> date:
    """Return the most recent weekday (Mon-Fri) on or before today.

    PUBLIC because it is the single definition of "a price series is
    current" and four modules depend on it agreeing: the per-instrument
    fetch skip (``_candles_are_fresh`` below), the T3 refresh scope
    (``scheduler._T3_CANDLE_SELECT``, #2254 — a scope boundary that
    drifts from the skip boundary either burns requests or strands
    series), and the orchestrator's candle freshness/content predicates.

    On weekdays (Mon-Fri) the freshness target is today itself.

    ⚠ The original rationale for that ("the job runs at 22:00 UTC, well
    after the ~21:00 UTC US close") is STALE: ``daily_candle_refresh``
    has no ``ScheduledJob`` entry — it is the ``candles`` DataLayer in
    the orchestrator registry, fired by the full sync at **03:00 UTC**,
    which is before the US session it names. Two consequences, both
    benign and neither worth "fixing" without a reason:
      * no US equity is ever skipped as fresh on a weekday run, so the
        T1/T2 set is re-fetched nightly (~1.1 s each);
      * a partially-formed bar dated today can be stored for an
        instrument quoting outside US hours. It self-heals — the next
        run's ``_INCREMENTAL_FETCH_BARS`` window (yesterday + today +
        one correction day) rewrites it, which is what that buffer is
        for. Do NOT treat a same-day bar from an off-close run as a
        final close.

    Weekends roll back to Friday (no candles for Sat/Sun).

    No holiday calendar — if a holiday causes a gap, the next fetch
    fills it. Holidays don't cause false staleness because the candle
    endpoint simply returns nothing new.
    """
    weekday = today.weekday()  # 0=Mon, 6=Sun
    if weekday == 5:  # Saturday → Friday
        return today - timedelta(days=1)
    if weekday == 6:  # Sunday → Friday
        return today - timedelta(days=2)
    # Mon-Fri: today's candle is the freshness target
    return today


def _candles_are_fresh(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    today: date,
    *,
    fresh_through: date | None = None,
) -> bool:
    """Return True if price_daily already has the most recent trading day's candle."""
    row = conn.execute(
        """
        SELECT MAX(price_date)
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
        """,
        {"instrument_id": instrument_id},
    ).fetchone()
    if row is None or row[0] is None:
        return False
    latest_date: date = row[0]
    return latest_date >= (fresh_through or most_recent_trading_day(today))


# Incremental fetch window in bars — yesterday + today + one
# correction-day buffer. eToro's /candles endpoint has no date-range
# filter, only `candlesCount`; this is the smallest count that still
# catches the latest bar plus a one-day retrospective correction.
_INCREMENTAL_FETCH_BARS = 3


#: Why ``_candles_fetch_count`` chose the size it chose (#2414). Returned rather
#: than left for the caller to infer from the count, because the inference is
#: wrong twice: it collapses whenever ``lookback_days == _INCREMENTAL_FETCH_BARS``
#: (a stale-gap fallback also returns 3, and would read as incremental), and
#: re-deriving "did this instrument have prior bars" from a SECOND read can
#: disagree with the read this function already did.
#:
#: ⚠ A `Literal`, not a bare `str`. These strings become KEYS in
#: `bars_revised_by_cause`, and an unrecognised key is indistinguishable on the
#: admin surface from a cause that genuinely did not fire — so a typo would be
#: invisible rather than loud. Typing them is what makes pyright the detector.
FetchReason = Literal["initial_backfill", "stale_reobservation", "incremental"]

FETCH_REASON_INITIAL_BACKFILL: FetchReason = "initial_backfill"
FETCH_REASON_STALE_REOBSERVATION: FetchReason = "stale_reobservation"
FETCH_REASON_INCREMENTAL: FetchReason = "incremental"


def _candles_fetch_count(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    *,
    default: int,
    today: date | None = None,
) -> tuple[int, FetchReason]:
    """Decide the candlesCount for an instrument's fetch, and say why (#271, #2414).

    Returns ``default`` (typically 1000 per #603) in two cases, which the second
    element separates:
      * ``initial_backfill`` — no prior candles at all.
      * ``stale_reobservation`` — prior candles exist but the most recent is
        older than the incremental window (e.g. instrument was halted, re-added
        to the universe after a gap, or a multi-day market closure). A 3-bar
        incremental fetch here would silently leave a history gap; falling back
        to ``default`` closes the gap.

    Returns ``_INCREMENTAL_FETCH_BARS`` / ``incremental`` when the most recent
    candle is within the incremental window — normal daily maintenance mode.
    The upsert dedupes on (instrument_id, price_date) so overlap is safe.

    ⚠ The incremental window is a CALENDAR-gap test on the NEWEST stored bar
    only. The three bars the provider then returns are spaced by the
    instrument's own trading cadence, so an incremental fetch is bounded in
    BARS and not in days — a sparse name two days behind can have a
    third-newest bar hundreds of days old. This is why #2414's revision-age
    axis cannot be inverted to a fetch reason, and why the reason is returned.

    Note: this function does NOT extend an instrument's lookback when
    ``default`` is bumped. An instrument that has 400 bars stays at
    400 in incremental mode; deepening to 5y requires the one-shot
    ``force_backfill=True`` invocation in ``refresh_market_data``.
    """
    row = conn.execute(
        """
        SELECT MAX(price_date) FROM price_daily
        WHERE instrument_id = %(instrument_id)s
        """,
        {"instrument_id": instrument_id},
    ).fetchone()
    if row is None or row[0] is None:
        return default, FETCH_REASON_INITIAL_BACKFILL
    latest: date = row[0]
    reference = today if today is not None else date.today()
    gap_days = (reference - latest).days
    if gap_days > _INCREMENTAL_FETCH_BARS:
        # Gap wider than the incremental window — backfill to close it.
        return default, FETCH_REASON_STALE_REOBSERVATION
    return _INCREMENTAL_FETCH_BARS, FETCH_REASON_INCREMENTAL


#: The three causes that do not come from ``_candles_fetch_count``.
RevisionCause = Literal[
    "initial_backfill",
    "stale_reobservation",
    "incremental",
    "adjustment_heal",
    "force_backfill",
    "unknown",
]

REVISION_CAUSE_ADJUSTMENT_HEAL: RevisionCause = "adjustment_heal"
REVISION_CAUSE_FORCE_BACKFILL: RevisionCause = "force_backfill"
#: A non-forced fetch that arrived without the reason its sizing decision
#: produced. Unreachable from the one call site today, and NAMED rather than
#: guessed: a future caller that forgets the reason must show up in the census
#: as an unattributed revision, not be silently folded into a real cause.
REVISION_CAUSE_UNKNOWN: RevisionCause = "unknown"


def revision_cause(
    *, adjustment_detected: bool, force_backfill: bool, fetch_reason: FetchReason | None
) -> RevisionCause:
    """Which write branch produced this instrument's revisions (#2414).

    ⚠⚠ A BRANCH, NOT AN ECONOMIC CAUSE. One fetch can rewrite bars for more than
    one underlying reason at once — a provider correction landing in the same
    response as a re-basing, including inside a heal — so this is an UPPER BOUND
    on attribution. It narrows the population #2414's supersession question has
    to examine; it does not classify it. Do not read ``adjustment_heal`` as
    "this revision was a split" or ``incremental`` as "this revision was a bad
    print".

    ⚠ The order is load-bearing and is neither alphabetical nor historical:

    * A heal is reachable ONLY from the incremental branch — its precondition is
      ``not force_backfill and fetch_count == _INCREMENTAL_FETCH_BARS`` — so
      returning ``fetch_reason`` first would claim every heal as incremental.
    * ``force_backfill`` bypasses ``_candles_fetch_count`` entirely, so there is
      no reason to report for it.
    * ``adjustment_detected and force_backfill`` is unreachable today by that
      same precondition. It resolves to the heal rather than raising: a
      telemetry helper that can abort a refresh is a worse failure than a
      mislabelled counter.
    """
    if adjustment_detected:
        return REVISION_CAUSE_ADJUSTMENT_HEAL
    if force_backfill:
        return REVISION_CAUSE_FORCE_BACKFILL
    if fetch_reason is None:
        return REVISION_CAUSE_UNKNOWN
    return fetch_reason


# #2066 — smallest overlap close ratio that reads as an adjustment event
# rather than a late correction. Splits re-base by the split ratio (2x,
# 3x, 10x; smallest common uneven split 5:4 = 1.25x); exchange corrections
# to a finalized close are single-digit percent. 1.2 sits between the two
# with margin, and a false positive only costs one idempotent full-history
# re-fetch, so the threshold errs low.
#
# ⚠ It is a TRIGGER for the heal, never a verdict on whether a stored
# strategy decision survives the revision. #3046 killed that reading twice:
# "magnitude is a trigger, not a verdict".
_ADJUSTMENT_RATIO_THRESHOLD = Decimal("1.2")


def detect_adjustment_event(
    stored_closes: dict[date, Decimal],
    bars: list[OHLCVBar],
) -> Decimal | None:
    """Ratio-scale mismatch between stored and re-fetched overlap closes (#2066).

    Provider candles are back-adjusted at fetch time, so after a split every
    re-fetched bar is on the new basis while stored rows outside the fetch
    window keep the old one. Comparing the re-fetched bars against what is
    already stored for the SAME dates exposes the re-basing: returns the
    largest direction-normalised close ratio (max(r, 1/r)) at or above
    ``_ADJUSTMENT_RATIO_THRESHOLD``, or None when the overlap is consistent.

    Non-positive closes on either side are skipped — ``price_daily`` holds
    zero-close sentinels and a garbage quote must not fake a split. Dates
    absent from ``stored_closes`` (new rows) carry no signal.
    """
    worst: Decimal | None = None
    for bar in bars:
        stored = stored_closes.get(bar.price_date)
        if stored is None or stored <= 0 or bar.close <= 0:
            continue
        ratio = bar.close / stored
        normalised = max(ratio, Decimal(1) / ratio)
        if normalised >= _ADJUSTMENT_RATIO_THRESHOLD and (worst is None or normalised > worst):
            worst = normalised
    return worst


def _stored_overlap_closes(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    dates: list[date],
) -> dict[date, Decimal]:
    """Stored ``price_daily`` closes for the given dates (#2066 overlap read)."""
    if not dates:
        return {}
    rows = conn.execute(
        """
        SELECT price_date, close FROM price_daily
        WHERE instrument_id = %(instrument_id)s AND price_date = ANY(%(dates)s)
        """,
        {"instrument_id": instrument_id, "dates": dates},
    ).fetchall()
    return {row[0]: row[1] for row in rows}


#: Calendar-day upper edges for the revised-bar age histogram (#2414), in the
#: order they are tested. NOT invented: every edge is one of this module's own
#: constants, so the buckets answer the question the code already poses.
#:
#: * ``0`` — the run's own reference date: the in-progress bar being re-observed
#:   as the session moves. This is NOT a historical correction and must never be
#:   summed with the others.
#: * ``3`` — ``_INCREMENTAL_FETCH_BARS``, the documented correction buffer.
#: * ``30`` / ``365`` — the two spans between the buffer and the ``lookback_days``
#:   (1000 bars ≈ 4 calendar years) deep re-fetch.
#: * beyond — reachable ONLY through that deep re-fetch path.
#:
#: ⚠⚠ The edges are CALENDAR days; ``_INCREMENTAL_FETCH_BARS`` counts BARS. A
#: 3-bar buffer spans up to 5 calendar days across a weekend, and more across a
#: holiday, so ``1_3`` is a strict SUBSET of "inside the buffer" and ``4_30``
#: contains an unknown handful of inside-buffer revisions. The decisive reading is
#: therefore ``31_365`` and ``over_365``, which no buffer explanation covers.
_REVISION_AGE_EDGES: tuple[tuple[str, int], ...] = (("same_day", 0), ("1_3", 3), ("4_30", 30), ("31_365", 365))
_REVISION_AGE_BEYOND = "over_365"
#: A bar dated after the run's reference date. Its own defect if it ever appears,
#: and given its own bucket so it can never be absorbed into ``same_day``.
_REVISION_AGE_FUTURE = "future"


@dataclass(frozen=True)
class CandleUpsertOutcome:
    """What one instrument's upsert did, split by KIND rather than counted once.

    A dataclass rather than a widening tuple: the caller merges these into
    running totals only after a clean commit (the #1293 rule), and a four-slot
    positional return is where that merge starts going wrong silently.
    """

    inserted: int
    revised: int
    #: bucket name -> count, over the REVISED bars only. Empty when none.
    revision_age_days: dict[str, int]
    #: The ``price_date`` of every bar this call OVERWROTE, in the order seen
    #: (#2414). Empty when none, and ``len(...) == revised`` always.
    #:
    #: ⚠ MAY CONTAIN THE SAME DATE TWICE. ``_normalise_candles``
    #: (``app/providers/implementations/etoro.py``) flattens every group's inner
    #: array with no date dedup, so one payload can carry a date twice and the
    #: second occurrence is a genuine second overwrite. The caller's table is
    #: append-only for exactly this reason.
    #:
    #: Carried here rather than written here because ``_upsert_candles`` does not
    #: know the write BRANCH — ``revision_cause`` needs ``adjustment_detected``,
    #: which only the caller has.
    revised_bar_dates: tuple[date, ...]
    #: The oldest revised bar's age in calendar days, or ``None`` if none were
    #: revised. Kept beside the histogram because a single deep revision is the
    #: finding, and a bucket count of 1 does not say how deep.
    revision_max_age_days: int | None
    #: The ``price_date`` of every bar this call INSERTED strictly below the
    #: frontier the caller observed (#2414 item 2). Empty when none, and always a
    #: subset of the ``inserted`` count — a bar above the frontier extends the
    #: series and is not here.
    #:
    #: ⚠ Cannot repeat a date within one call, unlike ``revised_bar_dates``: the
    #: second occurrence of a duplicated date conflicts, so it is a revision or an
    #: ``IS DISTINCT FROM`` no-op, never a second insert.
    backdated_insert_dates: tuple[date, ...]


def _revision_age_bucket(age_days: int) -> str:
    """Bucket one revised bar's age. ``age_days`` is CALENDAR days; see the edges above.

    ⚠ The caller computes ``age_days`` as ``reference_date - bar.price_date``,
    where ``reference_date`` is the run's ``freshness_target`` — ``fresh_through``
    if the caller pinned one, otherwise ``most_recent_trading_day(today)``. A
    NEGATIVE age therefore means a bar dated after the session the run is
    reconciling to, which is a provider or pinning fault rather than an age
    (review nitpick, round 1: worth stating for anyone touching
    ``freshness_target``, since nothing in this function's signature implies it).
    It is bucketed separately for that reason and never clamped to zero.
    """
    if age_days < 0:
        return _REVISION_AGE_FUTURE
    for name, edge in _REVISION_AGE_EDGES:
        if age_days <= edge:
            return name
    return _REVISION_AGE_BEYOND


def _upsert_candles(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    bars: list[OHLCVBar],
    *,
    reference_date: date,
    frontier_before: date | None,
) -> CandleUpsertOutcome:
    """
    Upsert OHLCV bars into price_daily. Idempotent — re-running with the same
    data produces no changes (ON CONFLICT DO UPDATE with WHERE clause).
    Returns inserted and revised counts — NEW bars and bars whose stored OHLCV
    was OVERWRITTEN by a different value. Their sum is the old scalar return.

    ⚠ The age histogram is FREE: the revised bar's ``price_date`` is already in
    hand, so it costs no query and no column. It is here rather than in a
    separate probe because ``price_daily`` keeps no prior value — once this
    statement returns, the age of what was overwritten is unrecoverable, exactly
    as the magnitude already is.

    ⚠ MAGNITUDE is deliberately NOT captured. ``RETURNING OLD.close`` would give
    it for nothing, and that is PostgreSQL 18; this cluster is 17.9 (checked, not
    assumed). The alternatives — a per-bar SELECT or a prior-value CTE — double
    the index probes on a path that writes up to 500k rows in one stale sweep, to
    answer a question that does not discriminate between #2414's two candidate
    fixes. Age does: an embargo works only if revisions never reach past the
    correction buffer.

    ⚠⚠ The split exists because these two are not the same event and the
    caller could not tell them apart (#2414). A new bar extends the series; a
    revision **destroys the value a strategy already made a decision on**, in
    place, with no prior value retained anywhere — ``price_daily`` has no
    audit column, so after this statement nothing can establish that the bar
    ever held a different number. ``strategy_signals`` rows key on
    ``(strategy_id, strategy_version, instrument_id, signal_bar_date,
    signal_kind)`` and carry no corpus stamp, so a revised bar silently
    invalidates any signal written against it and the corrected verdict cannot
    even be re-recorded (it collides on that key). Same reasoning as #1293's
    ``candles_skipped`` / ``candles_failed``: a caller that cannot distinguish
    two very different outcomes reports both as one number.

    ⚠ The revision surface is NOT the 3-bar correction buffer. ``_candles_fetch_count``
    falls back to ``lookback_days`` (1000) whenever ``gap_days >
    _INCREMENTAL_FETCH_BARS``, so any instrument stale by more than 3 days
    re-observes — and may rewrite — roughly four years of its history in one
    pass. Measured 2026-08-23: 1,183 of 12,230 instruments (9.7%) were in that
    state, holding 544,799 bars. A multi-day jobs outage puts the whole
    universe over that threshold at once.

    ⚠ ``xmax = 0`` distinguishes the INSERT path from the DO UPDATE path;
    verified empirically against this cluster rather than assumed (insert →
    ``xmax = 0``; update → non-zero). A row blocked by the ``IS DISTINCT FROM``
    guard returns NO row at all, so a genuine no-op counts as neither — which
    is the same thing the previous ``rowcount`` accounting did.

    ⚠⚠ ``frontier_before`` splits the INSERTS in two (#2414 item 2), and it must be
    the caller's OBSERVED ``MAX(price_date)`` — read in-transaction, immediately
    before this call. It is held FIXED for the whole call and deliberately not
    advanced as bars land: within one transaction nothing is visible to anyone
    else, so a bar that arrives after a higher-dated bar in the same payload was
    never behind committed history. Advancing it would classify provider payload
    ORDER, which is not a property of the corpus.

    ``None`` means the instrument had no prior bars, so **no** bar can be
    backdated — the initial-backfill case, where nothing had been decided against.
    """
    inserted = 0
    revised = 0
    age_days: dict[str, int] = {}
    max_age: int | None = None
    revised_dates: list[date] = []
    backdated_dates: list[date] = []
    for bar in bars:
        row = conn.execute(
            """
            INSERT INTO price_daily (
                instrument_id, price_date, open, high, low, close, volume
            )
            VALUES (
                %(instrument_id)s, %(price_date)s,
                %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s
            )
            ON CONFLICT (instrument_id, price_date) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume
            WHERE (
                price_daily.open   IS DISTINCT FROM EXCLUDED.open   OR
                price_daily.high   IS DISTINCT FROM EXCLUDED.high   OR
                price_daily.low    IS DISTINCT FROM EXCLUDED.low    OR
                price_daily.close  IS DISTINCT FROM EXCLUDED.close  OR
                price_daily.volume IS DISTINCT FROM EXCLUDED.volume
            )
            RETURNING (xmax = 0) AS was_insert
            """,
            {
                "instrument_id": instrument_id,
                "price_date": bar.price_date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            },
        ).fetchone()
        if row is None:
            continue
        if row[0]:
            inserted += 1
            if frontier_before is not None and bar.price_date < frontier_before:
                backdated_dates.append(bar.price_date)
        else:
            revised += 1
            revised_dates.append(bar.price_date)
            age = (reference_date - bar.price_date).days
            bucket = _revision_age_bucket(age)
            age_days[bucket] = age_days.get(bucket, 0) + 1
            # ⚠ `max` over the RAW age, including a negative one: a future-dated
            # bar must not be silently floored at 0 here after being given its
            # own bucket above.
            max_age = age if max_age is None else max(max_age, age)
    return CandleUpsertOutcome(
        inserted=inserted,
        revised=revised,
        revision_age_days=age_days,
        revision_max_age_days=max_age,
        revised_bar_dates=tuple(revised_dates),
        backdated_insert_dates=tuple(backdated_dates),
    )


def _record_bar_revisions(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    revised_bar_dates: Sequence[date],
    *,
    cause: RevisionCause,
) -> None:
    """Append one ``price_daily_revision`` row per overwritten bar (#2414).

    ⚠⚠ CALL THIS INSIDE THE SAME TRANSACTION AS THE BAR WRITE. That is the whole
    point of the table and the one property it rests on: `price_daily` keeps no
    prior value, so a revision row that survived a rolled-back bar write would
    assert a change that never happened, with nothing left to contradict it.
    The running counters in ``refresh_market_data`` are merged AFTER commit for
    the opposite reason (#1293) — a counter cannot roll back and a row can.

    ⚠ ``revised_bar_dates`` may repeat a date; see ``CandleUpsertOutcome``. Every
    element becomes a row, because each one IS a separate overwrite.

    The insert is ``executemany`` for the ROUND TRIPS, not for statement count —
    psycopg runs the command once per row either way. A heal re-fetches
    ``lookback_days`` (1000) bars and can revise all of them inside a
    per-instrument transaction on a sweep that already runs 17-34 minutes.
    """
    if not revised_bar_dates:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO price_daily_revision (instrument_id, price_date, cause)
            VALUES (%(instrument_id)s, %(price_date)s, %(cause)s)
            """,
            [
                {"instrument_id": instrument_id, "price_date": price_date, "cause": cause}
                for price_date in revised_bar_dates
            ],
        )


def _observed_frontier(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
) -> date | None:
    """``MAX(price_date)`` for one instrument, for backdated-insert classification.

    ⚠⚠ Deliberately NOT ``last_bar_before`` from the caller's #2262 supply-marker
    read, even though that value is already in hand and this costs a second
    indexed aggregate. That one is read BEFORE ``BEGIN``; this one is read inside
    the bar write's transaction, immediately before the upsert, which is the
    tightest window available. Codex checkpoint 1: writer A reads frontier 10,
    writer B commits 15, a decision consumes B's history, A then inserts 12 — with
    the pre-transaction value A calls 12 an extension, and it is not.

    ⚠ Read Committed does not make this exact, it makes it TIGHT. A concurrent
    commit inside the remaining window still misclassifies, which is why the
    observed frontier is STORED on the row rather than implied: two honest writers
    can disagree about the same date, and the row says which frontier it used.
    """
    row = conn.execute(
        "SELECT MAX(price_date) FROM price_daily WHERE instrument_id = %(iid)s",
        {"iid": instrument_id},
    ).fetchone()
    return None if row is None else row[0]


def _record_backdated_inserts(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    backdated_insert_dates: Sequence[date],
    *,
    frontier_before: date,
    cause: RevisionCause,
) -> None:
    """Append one ``price_daily_backdated_insert`` row per bar inserted behind the frontier.

    ⚠⚠ CALL THIS INSIDE THE SAME TRANSACTION AS THE BAR WRITE, for the same reason
    as ``_record_bar_revisions``: an audit row that outlived a rolled-back write
    would assert a mutation ``price_daily`` has no record of.

    ⚠ ``cause`` is shared with the revision log deliberately — it is the WRITE
    BRANCH (``revision_cause``), which is identical for both classes. The
    function's name says "revision" only because that is where the branch
    resolution was first needed; it makes no claim about the row's kind.
    """
    if not backdated_insert_dates:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO price_daily_backdated_insert
                (instrument_id, price_date, frontier_before, cause)
            VALUES (%(instrument_id)s, %(price_date)s, %(frontier_before)s, %(cause)s)
            """,
            [
                {
                    "instrument_id": instrument_id,
                    "price_date": price_date,
                    "frontier_before": frontier_before,
                    "cause": cause,
                }
                for price_date in backdated_insert_dates
            ],
        )


def _compute_and_store_features(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
) -> int:
    """
    Compute rolling returns and 30-day realised volatility for the most recent
    price_daily row of this instrument, then write back to the same row.

    Returns 1 if the most recent row was updated, 0 if no price data exists.

    Rolling return formula: (close_today / close_n_days_ago) - 1
    Volatility: annualised standard deviation of daily log returns over 30 days.
    """
    # Fetch enough history to compute all windows (up to 1y + buffer)
    rows = conn.execute(
        """
        SELECT price_date, close
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 400
        """,
        {"instrument_id": instrument_id},
    ).fetchall()

    if not rows:
        return 0

    # rows are newest-first; reverse to oldest-first for computation
    prices: list[tuple[date, Decimal]] = [(r[0], r[1]) for r in reversed(rows)]
    latest_date, _ = prices[-1]

    returns = _compute_rolling_returns(prices)
    volatility = _compute_volatility_30d(prices)

    # --- TA indicators (full OHLCV needed, not just close) ---
    # Require all four price columns non-null; the schema permits partial
    # rows (close-only) which would crash float() in stochastic/ATR.
    # Include price_date so we can verify the latest complete OHLCV bar
    # matches the row we're updating — avoids writing stale TA values
    # when the newest candle has close but incomplete OHLC.
    ohlcv_rows = conn.execute(
        """
        SELECT price_date, open, high, low, close, volume
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
          AND open IS NOT NULL
          AND high IS NOT NULL
          AND low IS NOT NULL
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 400
        """,
        {"instrument_id": instrument_id},
    ).fetchall()

    _TA_COLUMNS = [
        "sma_20",
        "sma_50",
        "sma_200",
        "ema_12",
        "ema_26",
        "macd_line",
        "macd_signal",
        "macd_histogram",
        "rsi_14",
        "stoch_k",
        "stoch_d",
        "bb_upper",
        "bb_lower",
        "atr_14",
    ]
    ta_params: dict[str, Decimal | None] = {k: None for k in _TA_COLUMNS}

    if ohlcv_rows:
        # Only compute TA if the latest complete OHLCV bar matches the row
        # we're updating; otherwise the indicators would be stale-by-one-day.
        ohlcv_latest_date: date = ohlcv_rows[0][0]  # newest first
        if ohlcv_latest_date == latest_date:
            bars: list[OHLCVRow] = [
                OHLCVRow(open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5]) for r in reversed(ohlcv_rows)
            ]
            ta_result = compute_indicators(bars)
            if ta_result is not None:
                for k, v in ta_result.items():
                    if k in ta_params and isinstance(v, float) and math.isfinite(v):
                        ta_params[k] = Decimal(str(round(v, 6)))

    conn.execute(
        """
        UPDATE price_daily SET
            return_1w      = %(return_1w)s,
            return_1m      = %(return_1m)s,
            return_3m      = %(return_3m)s,
            return_6m      = %(return_6m)s,
            return_1y      = %(return_1y)s,
            volatility_30d = %(volatility_30d)s,
            sma_20         = %(sma_20)s,
            sma_50         = %(sma_50)s,
            sma_200        = %(sma_200)s,
            ema_12         = %(ema_12)s,
            ema_26         = %(ema_26)s,
            macd_line      = %(macd_line)s,
            macd_signal    = %(macd_signal)s,
            macd_histogram = %(macd_histogram)s,
            rsi_14         = %(rsi_14)s,
            stoch_k        = %(stoch_k)s,
            stoch_d        = %(stoch_d)s,
            bb_upper       = %(bb_upper)s,
            bb_lower       = %(bb_lower)s,
            atr_14         = %(atr_14)s
        WHERE instrument_id = %(instrument_id)s
          AND price_date = %(price_date)s
        """,
        {
            "instrument_id": instrument_id,
            "price_date": latest_date,
            "return_1w": returns.get("return_1w"),
            "return_1m": returns.get("return_1m"),
            "return_3m": returns.get("return_3m"),
            "return_6m": returns.get("return_6m"),
            "return_1y": returns.get("return_1y"),
            "volatility_30d": volatility,
            **ta_params,
        },
    )
    return 1


def _compute_rolling_returns(
    prices: list[tuple[date, Decimal]],
) -> dict[str, Decimal | None]:
    """
    Compute rolling returns for each window against the most recent close.

    prices must be sorted oldest-first. Returns a dict of column_name → return
    value (or None if insufficient history for that window).

    Return = (close_latest / close_at_window_start) - 1
    """
    if not prices:
        return {col: None for col in _RETURN_WINDOWS}

    latest_date, latest_close = prices[-1]
    results: dict[str, Decimal | None] = {}

    for col, days in _RETURN_WINDOWS.items():
        target_date = date.fromordinal(latest_date.toordinal() - days)
        # Find the closest available price on or before target_date.
        # The break on the first date after target_date is correct only because
        # prices is strictly sorted oldest-first. That ordering is guaranteed by
        # the DB query (ORDER BY price_date DESC, reversed in Python) and by the
        # UNIQUE (instrument_id, price_date) constraint preventing duplicate dates.
        anchor: Decimal | None = None
        for price_date, close in prices[:-1]:  # exclude the latest bar itself
            if price_date <= target_date:
                anchor = close  # keep iterating to find closest to target
            else:
                break
        if anchor is not None and anchor != 0:
            results[col] = (latest_close / anchor) - Decimal("1")
        else:
            results[col] = None

    return results


def _compute_volatility_30d(prices: list[tuple[date, Decimal]]) -> Decimal | None:
    """
    Compute 30-day annualised realised volatility from daily log returns.

    Uses the most recent 31 prices (30 daily returns).
    Returns None if fewer than 5 returns are available (too few to be meaningful).
    """
    if len(prices) < 2:
        return None

    recent = prices[-31:]  # up to 31 prices → up to 30 returns
    log_returns = []
    for i in range(1, len(recent)):
        prev = recent[i - 1][1]
        curr = recent[i][1]
        if prev > 0 and curr > 0:
            log_returns.append(math.log(float(curr) / float(prev)))

    if len(log_returns) < 5:
        return None

    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
    daily_std = math.sqrt(variance)
    annualised = daily_std * math.sqrt(252)  # trading days per year

    return Decimal(str(round(annualised, 6)))


def _upsert_quote(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    quote: Quote,
    max_spread_pct: Decimal,
) -> bool:
    """
    Upsert the current quote into the quotes table.
    Computes spread_pct and sets spread_flag if spread exceeds the threshold.
    Returns True if spread_flag was set (i.e. spread is wide).

    The ``WHERE`` clause makes the write MONOTONIC in ``quoted_at`` (#2271),
    matching ``etoro_websocket.upsert_quote``'s guard. The two writers share
    one row per instrument and race: the WS streams live ticks for whatever is
    on the operator's screen while this REST path runs on a schedule, so
    without the guard a periodic snapshot would clobber a fresher live tick.
    That is the hazard the old ``skip_quotes=True`` call sites were working
    around by never writing quotes at all — which is what left the table with
    no scheduled writer in the first place.

    Note this makes the return value mean "the fetched quote is wide", not
    "the stored row is now flagged" — on a rejected (stale) write the stored
    row keeps the fresher tick's flag. The caller counts spread flags for
    reporting only.
    """
    spread_pct = compute_spread_pct(quote.bid, quote.ask)
    spread_flag = spread_pct is not None and spread_pct > max_spread_pct

    conn.execute(
        """
        INSERT INTO quotes (
            instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag
        )
        VALUES (
            %(instrument_id)s, %(quoted_at)s, %(bid)s, %(ask)s,
            %(last)s, %(spread_pct)s, %(spread_flag)s
        )
        ON CONFLICT (instrument_id) DO UPDATE SET
            quoted_at   = EXCLUDED.quoted_at,
            bid         = EXCLUDED.bid,
            ask         = EXCLUDED.ask,
            last        = EXCLUDED.last,
            spread_pct  = EXCLUDED.spread_pct,
            spread_flag = EXCLUDED.spread_flag
        WHERE quotes.quoted_at IS NULL OR EXCLUDED.quoted_at >= quotes.quoted_at
        """,
        {
            "instrument_id": instrument_id,
            "quoted_at": quote.timestamp,
            "bid": quote.bid,
            "ask": quote.ask,
            "last": quote.last,
            "spread_pct": spread_pct,
            "spread_flag": spread_flag,
        },
    )
    return spread_flag


def compute_spread_pct(bid: Decimal, ask: Decimal) -> Decimal | None:
    """Public helper for testing: compute spread % from bid/ask."""
    mid = (bid + ask) / 2
    if mid <= 0:
        return None
    return (ask - bid) / mid * 100
