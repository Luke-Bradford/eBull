"""Compute-from-XBRL helpers (#432) — replaces yfinance key-stats
where SEC data + quotes can answer the same question.

Each helper takes a ``psycopg.Connection`` + ``instrument_id`` and
returns a typed ``Decimal | None``. None = insufficient data (operator
UI falls back to yfinance cleanly).

Helpers land as they retire a specific yfinance call site — see the
ticket ladder in #432 for the per-call-site status.

Shipped in this PR:
  - compute_market_cap           (retires profile.market_cap)

Queued for follow-ups:
  - compute_pe_ttm, compute_pb, compute_roe, compute_roa,
    compute_debt_to_equity, compute_revenue_growth_yoy,
    compute_earnings_growth_yoy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg
import psycopg.rows

MarketCapSource = Literal["dei", "us-gaap", "unavailable"]


@dataclass(frozen=True)
class MarketCap:
    value: Decimal
    shares: Decimal
    price: Decimal
    price_as_of: date | None
    shares_as_of: date
    shares_source: MarketCapSource


def compute_market_cap(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> MarketCap | None:
    """Compute live market cap from the newest SEC share count ×
    latest quote (bid/ask midpoint, or ``last`` when available).

    Returns ``None`` if either input is missing. Wires to
    ``instrument_share_count_latest`` (sql/052, #435) for the share
    count — that view prefers DEI over us-gaap, picks the newest
    restated value, and exposes the source taxonomy used. Price
    mirrors the pattern in ``instrument_dividend_summary.priced``:
    NULLIF(GREATEST(last, 0), 0) first, then (bid+ask)/2.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH shares AS (
                SELECT latest_shares, as_of_date, source_taxonomy
                FROM instrument_share_count_latest
                WHERE instrument_id = %(iid)s
            ),
            priced AS (
                -- ``quotes`` is 1:1 current-snapshot by contract, but
                -- pin ORDER BY + LIMIT 1 as a defensive belt so any
                -- future migration that holds historical rows cannot
                -- fan-out this LEFT JOIN into a non-deterministic
                -- fetchone (review #444 BLOCKING).
                SELECT
                    COALESCE(
                        NULLIF(GREATEST(last, 0), 0),
                        CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END
                    ) AS price,
                    quoted_at::date AS price_as_of
                FROM quotes
                WHERE instrument_id = %(iid)s
                ORDER BY quoted_at DESC
                LIMIT 1
            )
            SELECT s.latest_shares, s.as_of_date, s.source_taxonomy,
                   p.price, p.price_as_of
            FROM shares s
            LEFT JOIN priced p ON TRUE
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()

    if row is None:
        return None
    shares = row["latest_shares"]
    price = row["price"]
    if shares is None or price is None or shares <= 0 or price <= 0:
        return None

    source_raw = str(row["source_taxonomy"])
    if source_raw not in ("dei", "us-gaap", "unavailable"):
        source: MarketCapSource = "unavailable"
    else:
        source = source_raw  # type: ignore[assignment]

    return MarketCap(
        value=Decimal(shares) * Decimal(price),
        shares=Decimal(shares),
        price=Decimal(price),
        price_as_of=row["price_as_of"],
        shares_as_of=row["as_of_date"],
        shares_source=source,
    )


# ---------------------------------------------------------------------------
# Total-company market cap for multi-class issuers (#1662, #1623 item 2)
# ---------------------------------------------------------------------------
#
# A multi-class issuer's market cap is the TOTAL company capitalization
# (Σ over classes of class_shares × class_price), identical across share-class
# siblings — both GOOG and GOOGL price the one company. The legacy
# ``compute_market_cap`` (combined all-class count × THIS class's price) is
# structurally wrong for these: it prices every class at one class's price and
# yields a different "cap" per sibling. This module computes the proper total from
# the #1623 per-class FSDS table and, for a CURATED dual-class issuer where a clean
# total cannot be built, FAILS CLOSED (market cap suppressed) rather than publish
# the defective product. Genuine single-class issuers are untouched.
#
# PURE READ-PATH. See docs/specs/etl/2026-06-17-per-class-market-cap.md.

# Σ of the mapped classes may exceed the combined all-class count by at most this
# fraction (rounding / period jitter) before we treat it as a source/period
# mismatch and fail closed (Codex ckpt-1 HIGH: don't clamp-and-publish a broken
# invariant).
_CLASS_SUM_OVERAGE_TOL: Final = Decimal("0.005")
# The untraded/unmapped residual class (e.g. Alphabet Class B) is valued at the
# largest traded class's price. That imputation is only trustworthy while the
# residual is a minority of the company; a larger residual means a major class is
# unmapped → the imputed leg would drive too much of the value → fail closed
# (Codex ckpt-1 HIGH/MED).
_RESIDUAL_MAX_FRACTION: Final = Decimal("0.25")
# The combined all-class count is read from companyfacts NEAR the FSDS class
# period_end (companyfacts leads DERA FSDS by ~a quarter). Bound how far apart the
# two instants may be, so a stale/orphan combined row can't drive the residual math
# (Codex ckpt-1 MED: "nearest" needs a max delta). ~4 quarters.
_MAX_COMBINED_FSDS_DELTA_DAYS: Final = 400

# #1745 per-period cap: max gap between the FSDS class-shares period actually on
# file and the TARGET financial period_end being priced. Tight (~one quarter) so a
# period borrows class shares only from its own (or an immediately adjacent) quarter
# — not a year-away row the 548-day staleness window would otherwise admit.
_MAX_CLASS_PERIOD_TARGET_DELTA_DAYS: Final = 100


@dataclass(frozen=True)
class _ClassLeg:
    """One traded share-class leg of a total-company market-cap sum."""

    instrument_id: int
    shares: Decimal
    price: Decimal


@dataclass(frozen=True)
class TotalCompanyMarketCap:
    """Total company market capitalization for a multi-class issuer (#1662).

    ``value`` = Σ over share-class sibling instruments (per-class FSDS shares × that
    sibling's quote) + the untraded residual class valued at the largest traded
    class's price. Identical across share-class siblings. ``imputed_residual`` flags
    that part of ``value`` is an imputed (non-quoted) leg — the standard treatment
    for a non-traded, economically-equivalent class (e.g. Alphabet Class B, 1:1
    convertible to Class A), not an independent figure."""

    value: Decimal
    period_end: date
    combined_shares: Decimal
    sum_mapped_shares: Decimal
    residual_shares: Decimal
    imputed_residual: bool
    leg_count: int
    # The priced traded legs that summed into ``value`` — one per share-class
    # sibling instrument (the imputed untraded residual class is NOT a leg: it has
    # no instrument). Carried so a caller can surface a single sibling's per-class
    # FLOAT value (its ``shares × price``) — the tradable-class market value, a
    # SEPARATE stat from this total-company ``value`` (#1665). Σ over ``legs`` =
    # ``value - residual_shares × <largest-leg price>``.
    legs: tuple[_ClassLeg, ...]


MarketCapBasis = Literal["total_company", "multiclass_unavailable", "not_multiclass"]


@dataclass(frozen=True)
class MarketCapResolution:
    """How to source an instrument's market cap.

    - ``total_company`` — use ``total.value`` (Σ class×price across siblings).
    - ``multiclass_unavailable`` — a CURATED dual-class issuer for which a clean
      total cannot be built (stale/missing per-class row, unpriced class, invariant
      breach). FAIL CLOSED: the caller must suppress market cap (null), never fall
      back to the structurally-wrong combined×price for a known dual-class issuer.
    - ``not_multiclass`` — single-class (or not a curated dual-class issuer): the
      caller uses the legacy ``compute_market_cap`` (combined × price, exact)."""

    basis: MarketCapBasis
    total: TotalCompanyMarketCap | None = None
    # Per-class FLOAT value of the VIEWED instrument's own share class — its leg's
    # ``shares × price`` (#1665). A SEPARATE stat from ``total.value`` (the whole
    # company): GOOGL Class A ≈ $2.15T vs Alphabet total ≈ $4.45T. Set only for
    # ``total_company`` basis, and only when the viewed instrument is itself a
    # priced leg (``None`` for a same-CIK sibling with no FSDS class row, and for
    # ``not_multiclass`` / ``multiclass_unavailable`` — which scopes the stat to
    # curated dual-class issuers).
    class_market_value: Decimal | None = None


def _sum_class_caps(legs: list[_ClassLeg], residual_shares: Decimal, impute_price: Decimal) -> Decimal:
    """Pure: Σ(shares × price) over the traded class legs, plus the untraded residual
    valued at ``impute_price`` (the representative traded class). ``residual_shares``
    is clamped ≥0 by the caller; a 0 residual contributes nothing. Table-tested
    without a DB."""
    total = sum((leg.shares * leg.price for leg in legs), Decimal(0))
    if residual_shares > 0:
        total += residual_shares * impute_price
    return total


def _leg_market_value(total: TotalCompanyMarketCap, instrument_id: int) -> Decimal | None:
    """Per-class FLOAT value of ``instrument_id``'s own leg within ``total`` — its
    ``shares × price`` (#1665). The FSDS table PK is ``(instrument_id, period_end)``
    and ``_build_total_company_cap`` reads a single ``period_end``, so AT MOST ONE
    leg can match a given instrument — this returns the first match (defensive: a
    hypothetical duplicate-ID leg list must not double-count). ``None`` when the
    viewed instrument is not itself a priced leg (e.g. a same-CIK ``.US`` listing
    with no FSDS class row). Pure — table-tested without a DB."""
    for leg in total.legs:
        if leg.instrument_id == instrument_id:
            return leg.shares * leg.price
    return None


def _latest_price(conn: psycopg.Connection[Any], instrument_id: int) -> Decimal | None:
    """Latest quote price for an instrument — the same mark hierarchy
    ``compute_market_cap`` uses (live ``last`` > 0 → bid/ask mid). ``None`` when no
    positive price is on file."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(
                       NULLIF(GREATEST(last, 0), 0),
                       CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END
                   ) AS price
            FROM quotes
            WHERE instrument_id = %s
            ORDER BY quoted_at DESC
            LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    price = Decimal(row[0])
    return price if price > 0 else None


def _price_at(conn: psycopg.Connection[Any], instrument_id: int, on_date: date) -> Decimal | None:
    """Daily close at/just-before ``on_date`` — the period-end price for the
    historical per-period cap (#1745). ``price_daily`` (EOD), NOT live ``quotes``,
    because a 2023 cap must use the 2023 price, not today's. ``None`` when no
    positive close is on file on/before the date."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT close FROM price_daily
            WHERE instrument_id = %(iid)s AND price_date <= %(d)s::date AND close > 0
            ORDER BY price_date DESC
            LIMIT 1
            """,
            {"iid": instrument_id, "d": on_date},
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    price = Decimal(row[0])
    return price if price > 0 else None


def _read_combined_shares_near(
    conn: psycopg.Connection[Any], instrument_id: int, near_period: date
) -> tuple[Decimal, date] | None:
    """Combined all-class us-gaap ``CommonStockSharesOutstanding`` for this instrument
    whose ``period_end`` is NEAREST ``near_period`` (an exact same-period row sorts
    first at delta 0). ``financial_facts_raw`` holds only the combined count —
    companyfacts strips the dimensional per-class facts (#1646). ``None`` when the
    concept is not on file. Local to the market-cap path (a tiny read, not policy) so
    this leaf module does not import ownership_rollup internals."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT val, period_end
            FROM financial_facts_raw
            WHERE instrument_id = %(iid)s
              AND taxonomy = 'us-gaap'
              AND concept = 'CommonStockSharesOutstanding'
              AND unit = 'shares'
              AND val IS NOT NULL
            ORDER BY ABS(period_end - %(near)s::date) ASC, filed_date DESC, accession_number DESC
            LIMIT 1
            """,
            {"iid": instrument_id, "near": near_period},
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return Decimal(row[0]), row[1]


def _assemble_total_company_cap(
    *,
    period_end: date,
    today: date,
    legs_raw: list[tuple[int, Decimal, Decimal | None]],
    combined_shares: Decimal,
    combined_as_of: date,
    as_of: date | None = None,
) -> TotalCompanyMarketCap | None:
    """Pure policy: given the per-class rows (``(instrument_id, shares, price | None)``
    — ``price=None`` = unpriced sibling), the combined all-class count, and the two
    instants, return the total-company cap or ``None`` if any guard fails. No DB —
    every fail-closed branch is table-tested. The freshness + structural per-class
    policy defers to the shared ``ownership_rollup.class_shares_usable`` (single
    source with the ownership denominator).

    ``as_of`` is the freshness reference for the per-class staleness gate. The LATEST
    live-cap path leaves it ``None`` → it defaults to ``today`` (is the newest FSDS
    instant fresh vs now?). The per-period historical path (#1745) passes the TARGET
    financial period_end → the class row is judged fresh *relative to the period it
    prices*, not vs 2026-now (which would fail every old point). The future-date
    guard still uses the real ``today`` (a future instant is corrupt regardless)."""
    from app.services.ownership_rollup import class_shares_usable

    freshness_ref = as_of if as_of is not None else today

    # A future period_end is corrupt data, not a fresh figure (the staleness policy
    # deliberately treats future as not-stale; the cap path must not inherit that
    # loophole — Codex ckpt-1 MED).
    if period_end > today:
        return None
    # Need ≥2 distinct covered siblings at this instant to sum a total-company cap.
    if len({iid for iid, _, _ in legs_raw}) < 2:
        return None
    # Combined must be ~the same instant (companyfacts leads FSDS by ~a quarter); a
    # far/orphan combined row must not drive the residual math (Codex ckpt-1 MED).
    if abs((combined_as_of - period_end).days) > _MAX_COMBINED_FSDS_DELTA_DAYS:
        return None

    legs: list[_ClassLeg] = []
    for iid, shares, price in legs_raw:
        if not class_shares_usable(
            class_shares=shares,
            class_period_end=period_end,
            combined_shares=combined_shares,
            today=freshness_ref,
        ):
            return None  # stale or structurally implausible per-class row
        if price is None or price <= 0:
            return None  # cannot price a covered class honestly → fail closed
        legs.append(_ClassLeg(instrument_id=iid, shares=shares, price=price))

    sum_mapped_shares = sum((leg.shares for leg in legs), Decimal(0))
    # Σ classes must not materially exceed the combined all-class total — a real
    # excess is a source/period mismatch or bad map, not a residual; fail closed
    # rather than clamp-and-publish (Codex ckpt-1 HIGH).
    if sum_mapped_shares > combined_shares * (Decimal(1) + _CLASS_SUM_OVERAGE_TOL):
        return None
    residual_shares = combined_shares - sum_mapped_shares
    if residual_shares < 0:
        residual_shares = Decimal(0)  # within tolerance — treat as rounding noise
    # The imputed residual leg may be at most a minority of the company; a larger
    # residual means a major class is unmapped and the imputation would carry too
    # much of the value (Codex ckpt-1 HIGH/MED).
    if residual_shares > combined_shares * _RESIDUAL_MAX_FRACTION:
        return None
    # Representative traded class for the untraded residual = the largest-share leg
    # (Alphabet Class A prices the Class B residual — identical economic rights).
    impute_price = max(legs, key=lambda leg: leg.shares).price
    value = _sum_class_caps(legs, residual_shares, impute_price)

    return TotalCompanyMarketCap(
        value=value,
        period_end=period_end,
        combined_shares=combined_shares,
        sum_mapped_shares=sum_mapped_shares,
        residual_shares=residual_shares,
        imputed_residual=residual_shares > 0,
        leg_count=len(legs),
        legs=tuple(legs),
    )


def _build_total_company_cap(
    conn: psycopg.Connection[Any], instrument_id: int, cik: str
) -> TotalCompanyMarketCap | None:
    """IO wrapper: read the per-class rows + combined count for a known curated
    dual-class issuer, then defer to the pure :func:`_assemble_total_company_cap`."""
    with conn.cursor() as cur:
        # Single FSDS instant = the newest per-class period_end covered for this CIK.
        cur.execute(
            "SELECT MAX(period_end) FROM instrument_class_shares_outstanding WHERE source_cik = %s",
            (cik,),
        )
        pe_row = cur.fetchone()
        if pe_row is None or pe_row[0] is None:
            return None
        period_end: date = pe_row[0]

        # Per-class shares per sibling instrument AT that instant. Driving off
        # ``source_cik`` keeps the set to FSDS-covered classes only — no .US
        # dual-listing / ETF double-count.
        cur.execute(
            """
            SELECT instrument_id, shares
            FROM instrument_class_shares_outstanding
            WHERE source_cik = %s AND period_end = %s
            """,
            (cik, period_end),
        )
        class_rows = [(int(r[0]), Decimal(r[1])) for r in cur.fetchall()]

        # UTC calendar "today" for the staleness clock + future-date guard.
        cur.execute("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        today_row = cur.fetchone()
        if today_row is None:  # CURRENT_TIMESTAMP always yields a row; narrows the type
            return None
        today: date = today_row[0]

    # Combined all-class count: the same issuer-level figure is fanned out to every
    # share-class sibling (#1102), but if it has not reached this particular sibling
    # yet, read it from any sibling that has it — so the total is IDENTICAL on every
    # sibling's page rather than computed on one and suppressed on another.
    combined: tuple[Decimal, date] | None = None
    for candidate in [instrument_id, *(iid for iid, _ in class_rows)]:
        combined = _read_combined_shares_near(conn, candidate, period_end)
        if combined is not None:
            break
    if combined is None:
        return None
    combined_shares, combined_as_of = combined

    legs_raw: list[tuple[int, Decimal, Decimal | None]] = [
        (iid, shares, _latest_price(conn, iid)) for iid, shares in class_rows
    ]
    return _assemble_total_company_cap(
        period_end=period_end,
        today=today,
        legs_raw=legs_raw,
        combined_shares=combined_shares,
        combined_as_of=combined_as_of,
    )


def total_company_cap_at_period(
    conn: psycopg.Connection[Any],
    *,
    cik: str,
    target_period_end: date,
    today: date,
) -> TotalCompanyMarketCap | None:
    """Total-company cap AS OF a historical ``target_period_end`` (#1745).

    Like :func:`_build_total_company_cap` but period-anchored, not latest-pinned:
      * per-class shares = the FSDS row NEAREST ``target_period_end`` for this CIK,
        rejected if more than ``_MAX_CLASS_PERIOD_TARGET_DELTA_DAYS`` away (Codex
        ckpt-1 HIGH — "nearest" alone could borrow a year-away quarter);
      * per-class price = ``price_daily`` close at/just-before the target (NOT the
        live quote);
      * freshness judged vs the target (``as_of=target_period_end``), so an old
        point is not failed merely for being old relative to today.
    Returns ``None`` (fail-closed) when no clean cap exists for that period — the
    caller renders absolute FCF with a NULL yield for that point.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT period_end
            FROM instrument_class_shares_outstanding
            WHERE source_cik = %(cik)s
            ORDER BY ABS(period_end - %(t)s::date) ASC, period_end DESC
            LIMIT 1
            """,
            {"cik": cik, "t": target_period_end},
        )
        pe_row = cur.fetchone()
        if pe_row is None or pe_row[0] is None:
            return None
        fsds_period: date = pe_row[0]
        if abs((fsds_period - target_period_end).days) > _MAX_CLASS_PERIOD_TARGET_DELTA_DAYS:
            return None

        cur.execute(
            """
            SELECT instrument_id, shares
            FROM instrument_class_shares_outstanding
            WHERE source_cik = %s AND period_end = %s
            """,
            (cik, fsds_period),
        )
        class_rows = [(int(r[0]), Decimal(r[1])) for r in cur.fetchall()]
    if not class_rows:
        return None

    # Combined all-class count nearest the TARGET period, read from any sibling that
    # carries it (the issuer-level figure is fanned to all siblings, #1102).
    combined: tuple[Decimal, date] | None = None
    for iid, _ in class_rows:
        combined = _read_combined_shares_near(conn, iid, target_period_end)
        if combined is not None:
            break
    if combined is None:
        return None
    combined_shares, combined_as_of = combined

    legs_raw: list[tuple[int, Decimal, Decimal | None]] = [
        (iid, shares, _price_at(conn, iid, target_period_end)) for iid, shares in class_rows
    ]
    return _assemble_total_company_cap(
        period_end=fsds_period,
        today=today,
        as_of=target_period_end,
        legs_raw=legs_raw,
        combined_shares=combined_shares,
        combined_as_of=combined_as_of,
    )


def resolve_market_cap_basis(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> MarketCapResolution:
    """Decide how to source ``instrument_id``'s market cap (#1662).

    A "multi-class issuer" is detected by presence in the #1623 CURATED per-class
    FSDS table (``instrument_class_shares_outstanding`` keyed by issuer CIK) — NOT by
    a raw shared-CIK sibling count, which is dominated by ``.US`` dual-listings, ETF
    trust families, warrants and preferreds (the same noise the #1646 dual-class
    detector filters with CUSIP gates). For a detected multi-class issuer we either
    return the total-company cap or fail closed; everything else uses the legacy
    single-class product. PURE READ-PATH; see
    docs/specs/etl/2026-06-17-per-class-market-cap.md."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_value
            FROM external_identifiers
            WHERE instrument_id = %s
              AND provider = 'sec' AND identifier_type = 'cik'
              AND is_primary = TRUE
            LIMIT 1
            """,
            (instrument_id,),
        )
        cik_row = cur.fetchone()
        if cik_row is None:
            return MarketCapResolution(basis="not_multiclass")
        # Normalize to the 10-digit zero-padded form the rest of the SEC pipeline
        # (and instrument_class_shares_outstanding.source_cik, #1623) stores, so an
        # unpadded external_identifiers row can't silently miss the curated oracle and
        # route a known dual-class issuer back to the broken legacy product.
        cik = str(cik_row[0]).zfill(10)

        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM instrument_class_shares_outstanding WHERE source_cik = %s)",
            (cik,),
        )
        exists_row = cur.fetchone()
        if exists_row is None or not exists_row[0]:
            return MarketCapResolution(basis="not_multiclass")

    total = _build_total_company_cap(conn, instrument_id, cik)
    if total is not None:
        # Also surface the viewed instrument's OWN per-class float value (#1665) —
        # a SEPARATE stat from the total-company ``value``. None when this sibling
        # is not itself a priced leg.
        return MarketCapResolution(
            basis="total_company",
            total=total,
            class_market_value=_leg_market_value(total, instrument_id),
        )
    # Curated dual-class issuer but no clean total → suppress, do not publish the
    # structurally-wrong combined×price for a known dual-class issuer.
    return MarketCapResolution(basis="multiclass_unavailable")
