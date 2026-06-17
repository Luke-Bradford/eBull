"""Per-class shares-outstanding ingest from the SEC DERA Financial Statement Data
Sets (FSDS). #788 ownership DQ audit — spec
``docs/specs/etl/2026-06-17-per-class-shares-denominator.md``.

A multi-class issuer whose classes share one SEC CIK (GOOG/GOOGL, HEI/HEI.A,
METC/METCB) has per-class holdings resolved by CUSIP, but the only
shares-outstanding figure in ``financial_facts_raw`` is the issuer's COMBINED
all-class count — the companyfacts JSON API strips every dimensional (per-class)
fact. The per-class values survive only in the FSDS ``num.txt`` ``segments``
column, tagged ``us-gaap:CommonStockSharesOutstanding`` with a single
``ClassOfStock=<member>`` axis. This module streams those rows, maps
``(cik, class_member) -> CUSIP -> instrument`` via a curated, hand-verified map,
and upserts current-period rows into ``instrument_class_shares_outstanding``
(sql/200), which ``ownership_rollup`` divides by when the read-path fail-closed
guards pass.

Fail-closed everywhere: a ``(cik, member)`` absent from the curated map, a CUSIP
that resolves to 0 or >1 tradable instruments, a non-current-period row, or a
malformed value is skipped (counted, never written). A wrong per-class
denominator is worse than the honest #1646 caveat, so we never guess.

Empirical FSDS findings (verified 2025q1 num.txt, 2026-06-17):
  * Tag is us-gaap ``CommonStockSharesOutstanding`` (version ``us-gaap/<year>``),
    NOT ``dei:EntityCommonStockSharesOutstanding`` (which is absent — dimensional
    strip). ``uom=shares``, ``qtrs=0`` (instant).
  * ``ddate`` is the balance-sheet instant date, NOT a cover-date. Each filing
    reports the tag at TWO ddates: the current fiscal period end + the prior-year
    comparative. The current value is the one whose ``ddate == sub.period``.
  * Member localnames are issuer-specific (``CommonClassA``, ``CapitalClassC``,
    ``HeicoCommonStock``, …) → a curated map is mandatory, not algorithmic.
  * Multi-axis ``ClassOfStock`` rows exist (``ClassOfStock=X;EquityComponents=Y;``)
    → require EXACTLY one ``ClassOfStock=<member>;`` segment.
"""

from __future__ import annotations

import io
import logging
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

PARSER_VERSION = "fsds_class_shares_v1"

# Curated, hand-verified (cik_10digit, FSDS ClassOfStock member) -> CUSIP. Keyed
# on CUSIP (the security identity, settled #1102) so it is ENVIRONMENT-INDEPENDENT
# (dev/prod instrument ids differ). Every entry verified against the issuer's SEC
# 10-K/10-Q per-class cover at add time — the curated map IS the correctness
# guarantee; the runtime guards in ownership_rollup are drift tripwires. A
# ``(cik, member)`` absent here is skipped (fail-closed). Untraded classes
# (Alphabet Class B) are intentionally absent — they have no ``instruments`` row.
_CLASS_MEMBER_TO_CUSIP: dict[tuple[str, str], str] = {
    # Alphabet — CIK 0001652044. Class A = GOOGL, Class C = GOOG (verified vs
    # FSDS 2025q1: ClassA 5,835M, CapitalClassC 5,515M; combined 12,211M).
    ("0001652044", "CommonClassA"): "02079K305",  # GOOGL
    ("0001652044", "CapitalClassC"): "02079K107",  # GOOG
    # HEICO — CIK 0000046619. Class A = HEI.A (83.9M), HeicoCommonStock = HEI
    # (the voting common, 55.0M) — issuer-specific member localname.
    ("0000046619", "CommonClassA"): "422806208",  # HEI.A
    ("0000046619", "HeicoCommonStock"): "422806109",  # HEI
    # Ramaco Resources — CIK 0001687187. Class A = METC (43.8M), Class B = METCB
    # (9.5M).
    ("0001687187", "CommonClassA"): "75134P600",  # METC
    ("0001687187", "CommonClassB"): "75134P501",  # METCB
}


@dataclass(frozen=True)
class FsdsSub:
    """One ``sub.txt`` row, the fields the per-class consumer needs."""

    cik: str  # 10-digit, zero-padded
    period: date | None  # filing reporting-period end (balance-sheet date)
    form: str  # 10-K / 10-Q (-> SharesOutstandingSource.form_type)
    filed: date | None  # filing acceptance date (-> source_filed_at no-demotion key)


@dataclass(frozen=True)
class _ClassRow:
    """A resolved, current-period per-class candidate ready to upsert."""

    instrument_id: int
    period_end: date
    shares: Decimal
    class_member: str
    source_cik: str
    source_adsh: str
    source_form_type: str
    source_filed_at: date


@dataclass
class FsdsClassSharesResult:
    """Telemetry for stage reporting. Per-row drops are counted, not raised."""

    num_rows_seen: int = 0
    class_rows_matched: int = 0  # passed all row filters + single ClassOfStock
    rows_written: int = 0
    skipped_not_current_period: int = 0
    skipped_cusip_unresolved: int = 0
    skipped_cusip_ambiguous: int = 0
    skipped_bad_value: int = 0
    # Curated (cik, member) pairs that produced NO current-period row in this
    # archive — surfaced so a download-window gap (a dual-class issuer's filing
    # quarter absent) fails VISIBLY rather than silently understating.
    curated_pairs_without_row: list[str] = field(default_factory=list)


def _parse_fsds_date(value: str | None) -> date | None:
    """Parse an FSDS ``YYYYMMDD`` date. Returns None on any malformation."""
    if not value or len(value) != 8 or not value.isdigit():
        return None
    try:
        return date(int(value[0:4]), int(value[4:6]), int(value[6:8]))
    except ValueError:
        return None


def parse_class_member(segments: str) -> str | None:
    """Extract the ClassOfStock member from an FSDS ``segments`` cell, or None.

    Accepts EXACTLY one ``ClassOfStock=<member>;`` segment. A multi-axis cell
    (``ClassOfStock=X;EquityComponents=Y;``) is rejected — those are
    restatement / scenario / consolidated sub-slices, not the issuer-level
    per-class count. A non-ClassOfStock or empty cell is rejected.
    """
    parts = [p for p in segments.split(";") if p]
    if len(parts) != 1:
        return None
    axis, _, member = parts[0].partition("=")
    if axis != "ClassOfStock" or not member:
        return None
    return member


def read_fsds_sub(zf: zipfile.ZipFile) -> dict[str, FsdsSub]:
    """``sub.txt`` -> ``{adsh: FsdsSub}``. sub.txt is small (~2 MB)."""
    name = "sub.txt"
    if name not in zf.namelist():
        candidates = [n for n in zf.namelist() if n.endswith("/" + name) or n == name]
        if not candidates:
            return {}
        name = candidates[0]
    out: dict[str, FsdsSub] = {}
    with zf.open(name) as fh:
        text = io.TextIOWrapper(fh, encoding="latin-1", newline="")
        header = text.readline().rstrip("\n").split("\t")
        idx = {col: i for i, col in enumerate(header)}
        for line in text:
            row = line.rstrip("\n").split("\t")
            try:
                adsh = row[idx["adsh"]]
                cik_raw = row[idx["cik"]]
            except LookupError:
                continue
            if not adsh or not cik_raw.isdigit():
                continue
            out[adsh] = FsdsSub(
                cik=cik_raw.zfill(10),
                period=_parse_fsds_date(row[idx["period"]]) if "period" in idx else None,
                form=row[idx["form"]] if "form" in idx else "",
                filed=_parse_fsds_date(row[idx["filed"]]) if "filed" in idx else None,
            )
    return out


def iter_fsds_num(zf: zipfile.ZipFile) -> Iterator[dict[str, str]]:
    """Stream ``num.txt`` rows as dicts (header-keyed). 530 MB/quarter — never
    materialised. Manual tab-split (DERA num.txt is a flat TSV with NO quoting;
    csv.reader's quote handling could misfire on a stray ``"`` in a footnote)."""
    name = "num.txt"
    if name not in zf.namelist():
        candidates = [n for n in zf.namelist() if n.endswith("/" + name) or n == name]
        if not candidates:
            return
        name = candidates[0]
    with zf.open(name) as fh:
        text = io.TextIOWrapper(fh, encoding="latin-1", newline="")
        header = text.readline().rstrip("\n").split("\t")
        n = len(header)
        for line in text:
            row = line.rstrip("\n").split("\t")
            if len(row) < n:
                continue
            yield dict(zip(header, row, strict=False))


def _resolve_cusip_to_instrument(conn: psycopg.Connection[Any], cusip: str) -> int | None:
    """Resolve a CUSIP to EXACTLY ONE tradable instrument, else None (fail-closed
    on both 0 and >1 — an operational duplicate / historical row must not silently
    pick a winner)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT i.instrument_id
            FROM external_identifiers ei
            JOIN instruments i ON i.instrument_id = ei.instrument_id
            WHERE ei.provider IN ('sec', 'openfigi')
              AND ei.identifier_type = 'cusip'
              AND ei.identifier_value = %s
              AND i.is_tradable = TRUE
            """,
            (cusip,),
        )
        rows = cur.fetchall()
    if len(rows) != 1:
        return None
    return int(rows[0][0])


def ingest_fsds_class_shares_archive(
    *,
    conn: psycopg.Connection[Any],
    archive_path: Path,
    fsds_qtr: str,
) -> FsdsClassSharesResult:
    """Stream one FSDS quarter ZIP and upsert current-period per-class rows.

    The caller owns the transaction (commits after this returns). The upsert is
    no-demotion: a later ``source_filed_at`` (or, tied, a larger ``source_adsh``)
    wins, so a same-quarter amendment or a newer FSDS quarter restating an old
    period replaces in place deterministically.
    """
    result = FsdsClassSharesResult()

    with zipfile.ZipFile(archive_path) as zf:
        sub = read_fsds_sub(zf)
        # Resolve the curated CUSIP map to instruments ONCE for this archive.
        cusip_to_instrument: dict[str, int | None] = {}
        for cusip in set(_CLASS_MEMBER_TO_CUSIP.values()):
            cusip_to_instrument[cusip] = _resolve_cusip_to_instrument(conn, cusip)

        # Best per-(instrument, period_end) candidate, no-demotion within the
        # archive: later filed wins, tie -> larger adsh.
        best: dict[tuple[int, date], _ClassRow] = {}
        seen_pairs: set[tuple[str, str]] = set()

        for row in iter_fsds_num(zf):
            result.num_rows_seen += 1
            if row.get("tag") != "CommonStockSharesOutstanding":
                continue
            if not row.get("version", "").startswith("us-gaap/"):
                continue
            if row.get("uom") != "shares" or row.get("qtrs") != "0":
                continue
            member = parse_class_member(row.get("segments", ""))
            if member is None:
                continue
            adsh = row.get("adsh", "")
            sub_row = sub.get(adsh)
            if sub_row is None or sub_row.period is None:
                continue
            key = (sub_row.cik, member)
            if key not in _CLASS_MEMBER_TO_CUSIP:
                # Not a curated dual-class member we track. Don't count as a
                # match (only curated members are in scope).
                continue
            result.class_rows_matched += 1
            seen_pairs.add(key)
            ddate = _parse_fsds_date(row.get("ddate"))
            # Current period only (ddate == sub.period); the comparative-year row
            # carries the same member at the prior ddate.
            if ddate is None or ddate != sub_row.period:
                result.skipped_not_current_period += 1
                continue
            cusip = _CLASS_MEMBER_TO_CUSIP[key]
            instrument_id = cusip_to_instrument.get(cusip)
            if instrument_id is None:
                # 0 or >1 tradable instruments for this CUSIP (fail-closed).
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*) FROM external_identifiers ei
                        JOIN instruments i ON i.instrument_id = ei.instrument_id
                        WHERE ei.provider IN ('sec','openfigi') AND ei.identifier_type='cusip'
                          AND ei.identifier_value=%s AND i.is_tradable=TRUE
                        """,
                        (cusip,),
                    )
                    count_row = cur.fetchone()
                    n = int(count_row[0]) if count_row else 0
                if n == 0:
                    result.skipped_cusip_unresolved += 1
                else:
                    result.skipped_cusip_ambiguous += 1
                continue
            try:
                shares = Decimal(row.get("value", ""))
            except InvalidOperation, ValueError:
                result.skipped_bad_value += 1
                continue
            if shares <= 0:
                result.skipped_bad_value += 1
                continue
            candidate = _ClassRow(
                instrument_id=instrument_id,
                period_end=ddate,
                shares=shares,
                class_member=member,
                source_cik=sub_row.cik,
                source_adsh=adsh,
                source_form_type=sub_row.form,
                source_filed_at=sub_row.filed or sub_row.period,
            )
            pk = (instrument_id, ddate)
            incumbent = best.get(pk)
            if incumbent is None or _supersedes(candidate, incumbent):
                best[pk] = candidate

    # Surface curated pairs that appeared in this archive but produced NO
    # current-period written row (e.g. only the comparative ddate present, or a
    # resolution failure) — so a download-window gap fails visibly rather than
    # silently understating. A pair absent from the archive entirely is expected
    # (an issuer files in one quarter only) and is NOT flagged.
    written_pairs = {(r.source_cik, r.class_member) for r in best.values()}
    result.curated_pairs_without_row = [
        f"{cik}/{member}" for (cik, member) in sorted(seen_pairs) if (cik, member) not in written_pairs
    ]

    for crow in best.values():
        _upsert_class_row(conn, crow, fsds_qtr=fsds_qtr)
        result.rows_written += 1

    logger.info(
        "fsds_class_shares %s: num_seen=%d matched=%d written=%d "
        "skip[period=%d cusip0=%d cuspN=%d badval=%d] no_row=%s",
        fsds_qtr,
        result.num_rows_seen,
        result.class_rows_matched,
        result.rows_written,
        result.skipped_not_current_period,
        result.skipped_cusip_unresolved,
        result.skipped_cusip_ambiguous,
        result.skipped_bad_value,
        result.curated_pairs_without_row,
    )
    return result


def _supersedes(candidate: _ClassRow, incumbent: _ClassRow) -> bool:
    """No-demotion: later filed wins; equal filed -> larger adsh."""
    if candidate.source_filed_at != incumbent.source_filed_at:
        return candidate.source_filed_at > incumbent.source_filed_at
    return candidate.source_adsh > incumbent.source_adsh


def _upsert_class_row(conn: psycopg.Connection[Any], row: _ClassRow, *, fsds_qtr: str) -> None:
    """Upsert one row, no-demotion across runs: DO UPDATE only when the incoming
    ``(source_filed_at, source_adsh)`` is strictly newer than the stored one."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_class_shares_outstanding (
                instrument_id, period_end, shares, class_member, source_cik,
                source_adsh, source_form_type, source_fsds_qtr, source_filed_at,
                resolution_method, parser_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'curated', %s)
            ON CONFLICT (instrument_id, period_end) DO UPDATE SET
                shares = EXCLUDED.shares,
                class_member = EXCLUDED.class_member,
                source_cik = EXCLUDED.source_cik,
                source_adsh = EXCLUDED.source_adsh,
                source_form_type = EXCLUDED.source_form_type,
                source_fsds_qtr = EXCLUDED.source_fsds_qtr,
                source_filed_at = EXCLUDED.source_filed_at,
                parser_version = EXCLUDED.parser_version,
                ingested_at = now()
            WHERE EXCLUDED.source_filed_at > instrument_class_shares_outstanding.source_filed_at
               OR (EXCLUDED.source_filed_at = instrument_class_shares_outstanding.source_filed_at
                   AND EXCLUDED.source_adsh > instrument_class_shares_outstanding.source_adsh)
            """,
            (
                row.instrument_id,
                row.period_end,
                row.shares,
                row.class_member,
                row.source_cik,
                row.source_adsh,
                row.source_form_type,
                fsds_qtr,
                row.source_filed_at,
                PARSER_VERSION,
            ),
        )
