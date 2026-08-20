"""DERA FSNDS (Financial Statement and Notes) monthly loader — unvested
RSU/PSU counts (#844).

Note-level XBRL facts are reachable through NO other pipeline: companyfacts
strips dimensional facts (sec-edgar §7.17) and plain FSDS is
face-statements-only. FSNDS monthlies (``{YYYY}_{MM}_notes.zip``) carry the
full note tagging: ``num.tsv`` (facts, ``dimh``-keyed) + ``dim.tsv``
(dimension-hash → segments) + ``sub.tsv`` (filings index, same columns as
FSDS ``sub.txt``).

Scope (spec docs/specs/etl/2026-07-23-drs-rsu-issuer-disclosures.md):
ONE tag — the ASC 718-10-50-2(c)(2) nonvested-award count — routed to
``instrument_dimensional_facts`` axis ``award_type``. The award axis is
NON-ADDITIVE by construction (it mixes award types with plan names; full-pop
2026_03: 58/91 default-vs-member-sum disagreements) so this loader stores
rows verbatim and NEVER sums; the read rule picks default-total /
single-standard-member / RSU-member, else abstains.

Convergence: the segment-family writers (#554 per-filing, #1590 FSDS bulk)
and this loader share the per-(instrument, accession) advisory lock but are
axis-scoped on delete/existence (``PER_FILING_AXES`` vs ``award_type``) so
neither wipes nor blocks the other's rows for the same 10-K accession.
"""

from __future__ import annotations

import logging
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg

from app.services.dimensional_facts import DimensionalFact, prettify_localname
from app.services.fsds_class_shares import iter_fsds_num, read_fsds_sub
from app.services.sec_identity import siblings_for_issuer_cik

logger = logging.getLogger(__name__)

PARSER_VERSION = "fsnds_notes_v1"
_TENK_FORMS = frozenset({"10-K", "10-K/A"})

NONVESTED_TAG = (
    "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber"
)

# FSNDS null-dimension hash (dim.tsv row ``0x00000000`` / segments='') —
# empirically pinned 2026_03. Default rows are ALSO detectable via
# ``dimn=0``; both are checked (belt + braces: an unresolved dimh must
# NEVER be mistaken for the default).
_NULL_DIMH = "0x00000000"

# member_qname stored for the dimensionless (all-award-types) total. The
# axis domain localname, per the us-gaap taxonomy's AwardTypeAndPlanName
# domain; the read rule prefers this row outright.
DEFAULT_MEMBER = "AwardTypeDomain"
DEFAULT_LABEL = "All award types"


# Standard us-gaap award-type members whose scope is company-wide-for-that-
# type by construction, mapped to fixed server-owned memo copy (a generic
# ``label.lower()`` mangles acronym members — "…rights sars"; review
# NITPICK on PR #2122). A single NON-standard member (usually a plan name)
# has unknowable scope → abstain. Spec "Read rule".
_STANDARD_MEMBER_COPY: dict[str, str] = {
    "RestrictedStockUnitsRSU": "unvested RSUs",
    "RestrictedStock": "unvested restricted stock",
    "PerformanceShares": "unvested performance shares",
    "PhantomShareUnitsPhantomStockUnits": "unvested phantom share units",
    "StockAppreciationRightsSARS": "unvested stock appreciation rights",
    "DeferredStockUnits": "unvested deferred stock units",
}
STANDARD_AWARD_MEMBERS: frozenset[str] = frozenset(_STANDARD_MEMBER_COPY)

_RSU_MEMBER = "RestrictedStockUnitsRSU"


@dataclass(frozen=True)
class NonvestedMemo:
    """The one figure the ownership memo line renders (absolute count —
    never a pie wedge, never a Σ over members: the award axis mixes award
    types with plan names and is non-additive by design, full-pop
    2026_03 58/91 default-vs-member-sum disagreements)."""

    shares: Decimal
    label: str  # server-owned memo copy fragment, e.g. "unvested RSUs"
    member_qname: str


def select_nonvested_memo(rows: list[tuple[str, str, Decimal]]) -> NonvestedMemo | None:
    """Pick the honest figure from one accession's latest-period award rows
    (``(member_qname, member_label, val)``), or ``None`` to abstain.

    Priority: default total → the single standard member → the RSU member
    among many (definitionally the RSU-typed count; scope named in the
    label) → abstain. NEVER sums members."""
    by_member = {qname: (label, val) for qname, label, val in rows}
    default = by_member.get(DEFAULT_MEMBER)
    if default is not None:
        return NonvestedMemo(shares=default[1], label="unvested awards", member_qname=DEFAULT_MEMBER)
    members = {q: lv for q, lv in by_member.items() if q != DEFAULT_MEMBER}
    if len(members) == 1:
        (qname, (_label, val)) = next(iter(members.items()))
        copy = _STANDARD_MEMBER_COPY.get(qname)
        if copy is not None:
            return NonvestedMemo(shares=val, label=copy, member_qname=qname)
        return None
    rsu = members.get(_RSU_MEMBER)
    if rsu is not None:
        return NonvestedMemo(shares=rsu[1], label="unvested RSUs", member_qname=_RSU_MEMBER)
    return None


# CamelCase → spaced-words quick-tier label, shared with the FSDS loader
# (review NITPICK on PR #2122 — was duplicated verbatim).
_prettify_member = prettify_localname


def _parse_value(raw: str) -> Decimal | None:
    try:
        return Decimal(raw)
    except InvalidOperation:
        # raw is always a str here (TSV field) — Decimal(str) raises only
        # InvalidOperation (review NITPICK round 4: ValueError/TypeError
        # were dead branches).
        return None


def read_award_dim_map(zf: zipfile.ZipFile) -> dict[str, str]:
    """``dim.tsv`` → {dimhash: member_localname} for SINGLE-axis
    ``AwardType=<member>`` cells only. Cross-dimensional cells (e.g.
    ``AwardType=…;Range=…``) are simply absent from the map, so num rows
    carrying them fall through unrouted — the exact-set rejection comes
    for free."""
    out: dict[str, str] = {}
    for row in iter_fsds_num(zf, name="dim.tsv"):
        seg = (row.get("segments") or "").strip()
        if not seg:
            continue
        axis, eq, member = seg.rstrip(";").partition("=")
        if not eq or axis != "AwardType" or not member or ";" in member:
            continue
        dimhash = row.get("dimhash", "")
        if dimhash:
            out[dimhash] = member
    return out


# (ddate, dimh) → chosen row within one accession. iprx arbitration: DERA
# defines iprx as the disambiguator for otherwise-identical facts; the
# primary presentation is iprx=0, so the LOWEST iprx wins. A same-iprx
# value conflict drops the key (conservative; mirrors the FSDS loader).
_FactKey = tuple[date, str]


@dataclass
class _AccBuf:
    facts: dict[_FactKey, tuple[int, Decimal, str | None]] = field(default_factory=dict)
    conflicted: set[_FactKey] = field(default_factory=set)


@dataclass
class FsndsNotesResult:
    rows_written: int = 0
    accessions_written: int = 0
    accessions_skipped_existing: int = 0
    accessions_no_instrument: int = 0
    instruments_written: int = 0


def _accumulate(per_adsh: dict[str, _AccBuf], row: dict[str, str], award_map: dict[str, str]) -> None:
    """Route one gated num.tsv row into its accession buffer."""
    if (row.get("coreg") or "").strip():
        return  # co-registrant facts belong to a different entity
    if row.get("uom") != "shares" or row.get("qtrs") != "0":
        return  # the count is an instant share figure — anything else is noise
    if not (row.get("version") or "").startswith("us-gaap/"):
        return  # reject filer-extension homonyms (DERA versions per row, e.g. us-gaap/2025)
    dimh = row.get("dimh", "")
    dimn = (row.get("dimn") or "").strip()
    is_default = dimn == "0" or dimh == _NULL_DIMH
    if not is_default and dimh not in award_map:
        return  # cross-dimensional / non-award cell — unrouted by design
    raw_ddate = row.get("ddate", "")
    if len(raw_ddate) != 8:
        return  # strptime("%Y%m%d") greedily accepts 7-char inputs — gate length first
    try:
        ddate = datetime.strptime(raw_ddate, "%Y%m%d").date()
    except ValueError:
        return
    value = _parse_value(row.get("value", ""))
    if value is None:
        return
    try:
        iprx = int(row.get("iprx") or 0)
    except ValueError:
        return
    dcml = (row.get("dcml") or "").strip() or None

    buf = per_adsh.setdefault(row["adsh"], _AccBuf())
    # Default rows normalise to ONE key regardless of how the source
    # encoded "no dimension" (dimn=0 vs the null hash) — two default rows
    # for one ddate must arbitrate here, or they collide on the identity
    # index (same member_qname + period_end) and fail the insert batch.
    key: _FactKey = (ddate, _NULL_DIMH if is_default else dimh)
    if key in buf.conflicted:
        return
    incumbent = buf.facts.get(key)
    if incumbent is None:
        buf.facts[key] = (iprx, value, dcml)
        return
    inc_iprx, inc_value, _inc_dcml = incumbent
    if iprx < inc_iprx:
        buf.facts[key] = (iprx, value, dcml)
    elif iprx == inc_iprx and value != inc_value:
        del buf.facts[key]
        buf.conflicted.add(key)


def _write_award_accession(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession: str,
    form_type: str,
    filed_at: datetime,
    facts: list[DimensionalFact],
) -> int:
    """Insert one accession's award facts for one instrument — ONLY if no
    ``award_type`` row exists for ``(instrument, accession)`` yet. Takes the
    SAME advisory lock key as the segment-family writers so all
    check-then-insert paths on one accession serialize; the existence check
    is axis-scoped so their segment rows never block this write."""
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s::text || ':' || %s::text)::bigint)",
        (instrument_id, accession),
    )
    existing = conn.execute(
        "SELECT 1 FROM instrument_dimensional_facts"
        " WHERE instrument_id = %s AND source_accession = %s AND axis = 'award_type' LIMIT 1",
        (instrument_id, accession),
    ).fetchone()
    if existing is not None or not facts:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO instrument_dimensional_facts (
                instrument_id, axis, member_qname, member_label, metric,
                unit, is_subtotal, period_start, period_end, val, decimals,
                source_accession, form_type, filed_at, parser_version
            ) VALUES (
                %(instrument_id)s, 'award_type', %(member_qname)s, %(member_label)s,
                'nonvested_awards', 'shares', FALSE, NULL,
                %(period_end)s, %(val)s, %(decimals)s, %(source_accession)s,
                %(form_type)s, %(filed_at)s, %(parser_version)s
            )
            """,
            [
                {
                    "instrument_id": instrument_id,
                    "member_qname": f.member_qname,
                    "member_label": f.member_label,
                    "period_end": f.period_end,
                    "val": f.val,
                    "decimals": f.decimals,
                    "source_accession": accession,
                    "form_type": form_type,
                    "filed_at": filed_at,
                    "parser_version": PARSER_VERSION,
                }
                for f in facts
            ],
        )
        # psycopg3 executemany rowcount is cumulative across the batch.
        return cur.rowcount


def ingest_fsnds_notes_archive(
    conn: psycopg.Connection[Any],
    *,
    archive_path: Path,
    fsnds_month: str,
) -> FsndsNotesResult:
    """Stream one ``{YYYY}_{MM}_notes.zip`` and load nonvested-award facts.
    Commits per accession (advisory locks release promptly); the caller need
    not wrap this in a transaction."""
    result = FsndsNotesResult()
    with zipfile.ZipFile(archive_path) as zf:
        award_map = read_award_dim_map(zf)
        subs = read_fsds_sub(zf, name="sub.tsv")
        tenk = {adsh: sub for adsh, sub in subs.items() if sub.form in _TENK_FORMS and sub.filed is not None}
        per_adsh: dict[str, _AccBuf] = {}
        for row in iter_fsds_num(zf, name="num.tsv"):
            if row.get("tag") == NONVESTED_TAG and row.get("adsh", "") in tenk:
                _accumulate(per_adsh, row, award_map)

    for adsh, buf in per_adsh.items():
        if not buf.facts:
            continue
        sub = tenk[adsh]
        siblings = siblings_for_issuer_cik(conn, sub.cik)
        if not siblings:
            result.accessions_no_instrument += 1
            conn.commit()  # close the read txn opened by the lookup above
            continue
        facts: list[DimensionalFact] = []
        for (ddate, dimh), (_iprx, value, dcml) in buf.facts.items():
            member = award_map.get(dimh)
            facts.append(
                DimensionalFact(
                    axis="award_type",
                    member_qname=member if member is not None else DEFAULT_MEMBER,
                    member_label=_prettify_member(member) if member is not None else DEFAULT_LABEL,
                    metric="nonvested_awards",
                    unit="shares",
                    is_subtotal=False,
                    period_start=None,
                    period_end=ddate,
                    val=value,
                    decimals=dcml,
                )
            )
        assert sub.filed is not None  # tenk filter guarantees this
        filed_at = datetime.combine(sub.filed, time.min, tzinfo=UTC)
        wrote_any = False
        with conn.transaction():
            for instrument_id in siblings:
                n = _write_award_accession(
                    conn,
                    instrument_id=instrument_id,
                    accession=adsh,
                    form_type=sub.form,
                    filed_at=filed_at,
                    facts=facts,
                )
                if n > 0:
                    result.rows_written += n
                    result.instruments_written += 1
                    wrote_any = True
        # Explicit top-level commit per accession: persists rows AND releases
        # the advisory xact lock (see the FSDS loader's identical note).
        conn.commit()
        if wrote_any:
            result.accessions_written += 1
        else:
            result.accessions_skipped_existing += 1

    logger.info(
        "sec_fsnds_notes_ingest: month=%s rows=%d accessions_written=%d skipped_existing=%d no_instrument=%d",
        fsnds_month,
        result.rows_written,
        result.accessions_written,
        result.accessions_skipped_existing,
        result.accessions_no_instrument,
    )
    return result
