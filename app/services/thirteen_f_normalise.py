"""Shared 13F-HR holdings normalisation (#1567 / #1566).

A 13F-HR legitimately splits ONE ``(cusip, putCall)`` position across
multiple ``<infoTable>`` rows by ``otherManager`` / investment
discretion (Vanguard Group Q4-2025 carries 7 AAPL rows summing
1,426,283,914 shares). Every ingest path must SUM those rows, not keep
one. Two of the per-filing paths additionally lacked the PRN
(bond-principal) filter and the pre-2023 VALUE x1000 scaling that the
manifest parser and bulk dataset path already apply.

This module is the single source of truth for all three corrections on
the ``ThirteenFHolding`` (per-filing) paths:

  * ``institutional_holdings._ingest_single_accession`` (legacy first-ingest)
  * ``manifest_parsers.sec_13f_hr._parse_13f_hr`` (manifest worker)
  * ``rewash_filings._apply_13f_infotable`` (CUSIP rewash)

The bulk COPY/SQL path (``sec_13f_dataset_ingest``) cannot route per-row
through Python at its scale; it applies the identical corrections in SQL
(``GROUP BY`` + ``SUM`` + a voting CASE that mirrors
:func:`dominant_voting_authority`) and imports :data:`VALUE_DOLLARS_CUTOVER`
from here so the cutover date stays single-sourced.

Pure / DB-free → exhaustively table-testable. Lives in the service layer
(settled decision: providers stay thin; the ``ThirteenFHolding`` docstring
already states "service layer applies any conversion").
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal

from app.providers.implementations.sec_13f import ThirteenFHolding

# 13F-HR Column 4 (VALUE) unit cutover. SEC EDGAR Release 22.4.1 switched
# VALUE from $thousands to whole $dollars effective 2023-01-03. Single
# source of truth — the manifest parser and bulk dataset path import this
# rather than redeclaring the date.
VALUE_DOLLARS_CUTOVER = date(2023, 1, 3)


def exposure_key(holding: ThirteenFHolding) -> str:
    """Aggregation/exposure key: ``'PUT'`` / ``'CALL'`` / ``'EQUITY'``.

    Mirrors the DB partial unique index expression
    ``COALESCE(is_put_call, 'EQUITY')`` (migration 090) and the
    observation identity's ``exposure_kind``.
    """
    return holding.put_call if holding.put_call in ("PUT", "CALL") else "EQUITY"


def _sum_group(rows: list[ThirteenFHolding]) -> ThirteenFHolding:
    """Collapse ≥1 holdings sharing an aggregation key into one summed row.

    SUMs shares, value, and the three voting sub-amounts (the canonical
    ``voting_authority`` label is derived downstream by
    :func:`dominant_voting_authority` over these summed components, so
    summing-then-deriving is correct). Identity fields (cusip / issuer /
    class / put_call) come from the first row. ``investment_discretion``
    is audit-only free text — kept when the group is unanimous, else
    ``None`` (a summed multi-discretion position has no single label).
    """
    first = rows[0]
    if len(rows) == 1:
        return first
    discretions = {r.investment_discretion for r in rows}
    return replace(
        first,
        value_usd=sum((r.value_usd for r in rows), Decimal(0)),
        shares_or_principal=sum((r.shares_or_principal for r in rows), Decimal(0)),
        voting_sole=sum((r.voting_sole for r in rows), Decimal(0)),
        voting_shared=sum((r.voting_shared for r in rows), Decimal(0)),
        voting_none=sum((r.voting_none for r in rows), Decimal(0)),
        investment_discretion=(first.investment_discretion if len(discretions) == 1 else None),
    )


def normalise_13f_holdings(holdings: list[ThirteenFHolding], *, filed_at: datetime | None) -> list[ThirteenFHolding]:
    """Drop PRN + bad-quantity rows, scale pre-cutover VALUE, SUM-aggregate.

    1. Drop rows whose type is not ``SH`` (bond principal, dollars not
       shares). ``parse_infotable`` already defaults blank/unknown Type to
       ``SH``, so a blank never arrives here as PRN.
    2. Drop ``SH`` rows with a non-positive share count (malformed; mirrors
       the bulk ``#1433`` guard). ``parse_infotable`` already drops
       both-value-and-shares-zero rows.
    3. If ``filed_at`` is before :data:`VALUE_DOLLARS_CUTOVER`, scale
       ``value_usd`` x1000 (pre-2023 VALUE is in $thousands). ``filed_at``
       is None → no scale (fail-safe; mirrors the manifest parser).
    4. Aggregate by ``(cusip, exposure)`` and SUM (see :func:`_sum_group`).

    Returns one holding per ``(cusip, exposure)`` in first-seen order
    (deterministic).
    """
    scale = filed_at is not None and filed_at.date() < VALUE_DOLLARS_CUTOVER
    groups: OrderedDict[tuple[str, str], list[ThirteenFHolding]] = OrderedDict()
    for holding in holdings:
        if (holding.shares_or_principal_type or "").strip().upper() != "SH":
            continue
        if holding.shares_or_principal is None or holding.shares_or_principal <= 0:
            continue
        if scale:
            holding = replace(holding, value_usd=holding.value_usd * Decimal("1000"))
        groups.setdefault((holding.cusip, exposure_key(holding)), []).append(holding)
    return [_sum_group(rows) for rows in groups.values()]


def merge_resolved_by_instrument(
    resolved: list[tuple[int, ThirteenFHolding]],
) -> list[tuple[int, ThirteenFHolding]]:
    """Second-pass SUM-merge of already-resolved holdings by ``(instrument_id,
    exposure)``.

    :func:`normalise_13f_holdings` aggregates by ``(cusip, exposure)``, but
    the typed table's unique key and the observation identity are on
    ``(…, instrument_id, exposure)``. Two distinct CUSIPs in one accession
    that resolve to the same eBull instrument (e.g. aliased share classes)
    would otherwise collide at the upsert and keep-last one summed group
    instead of summing both. Vanishingly rare for 13F, but this makes the
    per-filing paths exactly match the bulk path's ``instrument_id``-level
    ``GROUP BY``. Preserves first-seen order.
    """
    groups: OrderedDict[tuple[int, str], list[ThirteenFHolding]] = OrderedDict()
    for instrument_id, holding in resolved:
        groups.setdefault((instrument_id, exposure_key(holding)), []).append(holding)
    return [(instrument_id, _sum_group(rows)) for (instrument_id, _), rows in groups.items()]
