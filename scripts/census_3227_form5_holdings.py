"""#3227 — a Form 5's Table I holding lines (``nonDerivativeHolding``) never reach the XML writers.

    PYTHONPATH=. uv run python -m scripts.census_3227_form5_holdings --as-of 2026-10-18T23:00:00Z
    PYTHONPATH=. uv run python -m scripts.census_3227_form5_holdings --examples 5 --show <accession>

Read-only.  Nothing is written, nothing is refreshed.  Spec:
docs/specs/ownership/2026-10-18-3227-form5-holdings.md, whose rule this script applies; every count
the spec's "Full-population verification" section quotes comes from this script's output.

``parse_form_5_xml`` reads only transactions, so a Form 5's holding lines reach no XML observation
and a holdings-only Form 5 is tombstoned.  The DERA bulk path writes ``NONDERIV_HOLDING`` rows
only for accessions with no ``NONDERIV_TRANS`` row, and lags the quarter.

## The rule applied (the spec's Change section)

The Form 4 holding-only rule (``form4_holding_observations``), with Form 5's own date, retention
and two gates:

* only an original Form 5 (``5``) with ``notSubjectToSection16`` unset writes; a 5/A (Instruction
  9: partial) and an exit-box filing (Instruction 4(a)(i): an undated earlier balance) write
  nothing, and neither does a group holding a delinquent-Form-3 line (``transactionFormType`` 3,
  Instruction 8), whose as-of date the form leaves ambiguous;
* a group is the first owner's ``D`` (or ``I``) non-derivative holding lines, when the payload has
  no non-derivative transaction line of that D/I (a transaction with no D/I counts as ``D``);
* its verdict is the production ``_form4_holding_verdict``, called here unchanged;
* its ``period_end`` is ``period_of_report`` (Item 3, the fiscal year end) when that is on or
  before the filed date, else nothing;
* it needs a manifest ``filed_at`` inside the Form 5 retention window, on the UTC date, at the
  pinned ``--as-of`` instant.

The cohort is the backfill's: a stored payload with an ``insider_filings`` row (the rewash path
re-applies onto it), counted per its canonical ``insider_filings.instrument_id``.  The live
writer also fans out to share-class siblings; the build's A/B covers them.

## What it cannot establish

The ``_current`` move exactly: the comparison re-implements the winner order (source rank,
``period_end`` DESC, ``filed_at`` DESC) against the stored winner only, without the amendment
filter or de-collision.  The build's A/B diffs the real refreshers.
"""

from __future__ import annotations

import argparse
import calendar
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from app.services.insider_transactions import (
    _XMLNS_RE,
    Form4HoldingLine,
    _date,
    _extract_filers,
    _extract_holdings,
    _extract_transactions,
    _flag,
    _form4_holding_verdict,
    _localname,
    _text,
    _unwrap_sgml_submission,
    evaluate_insider_date_validity,
    form5_within_retention,
    predates_section_16,
)
from scripts.census_3227_form4_holding_rows import _CURRENT, _DERA_ID, _NATURE, _SOURCE_RANK

_PAYLOADS: Final[LiteralString] = """
SELECT r.accession_number, f.instrument_id, f.is_tombstone, m.filed_at, r.payload
  FROM filing_raw_documents r
  LEFT JOIN insider_filings f ON f.accession_number = r.accession_number
  LEFT JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
 WHERE r.document_kind = 'form5_xml'
   AND r.payload LIKE '%%erivativeHolding%%'
"""

# Item 3 check: every stored Form 5 payload's periodOfReport against the issuer's fiscal year end.
_FYE: Final[LiteralString] = """
SELECT substring(r.payload from '<periodOfReport>\\s*([0-9-]+)'), p.fiscal_year_end, f.accession_number IS NOT NULL
  FROM filing_raw_documents r
  LEFT JOIN insider_filings f ON f.accession_number = r.accession_number
  LEFT JOIN instrument_sec_profile p ON p.instrument_id = f.instrument_id
 WHERE r.document_kind = 'form5_xml'
"""

_WRITES: Final = frozenset({"one_line", "summed"})
_MMDD = re.compile(r"^(\d{2})(\d{2})$")


@dataclass(frozen=True)
class Group:
    instrument_id: int
    accession: str
    is_tombstone: bool
    filed_at: datetime
    filer_cik: str
    di: str
    titles: tuple[str, ...]
    shares: tuple[Decimal | None, ...]
    verdict: str
    value: Decimal | None
    period_end: date | None
    code3: bool
    joint: bool
    singleton_flags: tuple[str, ...]  # the multi-line conditions a one-line group skips, where it fails them


def _gap_band(period: date, filed: date) -> str:
    days = (filed - period).days
    if days <= 45:
        return "0-45d (Rule 16a-3(f) due date)"
    if days <= 365:
        return "46-365d"
    return ">365d"


def _fye_distance(period: date, fye: str) -> int | None:
    """Days from ``period`` to the nearest occurrence of the ``MMDD`` fiscal year end, 0 only on a
    literal month-day match.  ``None`` for a malformed ``MMDD`` (a month outside 1-12, or a day the
    month never has).  A ``0229`` year end falls on 02-28 in a non-leap year, at distance >= 1."""
    m = _MMDD.fullmatch(fye.strip())
    if m is None:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    if not 1 <= month <= 12 or not 1 <= day <= (29 if month == 2 else calendar.monthrange(2001, month)[1]):
        return None
    if (period.month, period.day) == (month, day):
        return 0
    best = None
    for year in (y for y in (period.year - 1, period.year, period.year + 1) if 1 <= y <= 9999):
        d = date(year, month, min(day, calendar.monthrange(year, month)[1]))
        gap = max(1, abs((period - d).days))
        best = gap if best is None else min(best, gap)
    return best


def _singleton_flags(
    line: Form4HoldingLine,
    holding_amounts: list[tuple[str, Decimal]],
    txn_balances: list[tuple[str, Decimal]],
) -> tuple[str, ...]:
    """Which of ``_form4_holding_verdict``'s multi-line conditions a one-line group would fail; the
    verdict returns ``one_line`` before checking them (the inherited singleton residual)."""
    title = (line.security_title or "").strip()
    flags = []
    if line.shares is not None and line.shares < 0:
        flags.append("negative")
    if not title:
        flags.append("blank_title")
    if line.shares is not None and [a for t, a in holding_amounts if t == title].count(line.shares) > 1:
        flags.append("equal_holding_amount")
    if line.shares in {b for t, b in txn_balances if t == title}:
        flags.append("equal_to_transaction_balance")
    return tuple(flags)


def collect(
    conn: psycopg.Connection[Any], as_of: datetime, show: set[str]
) -> tuple[list[Group], Counter[str], dict[str, list[str]]]:
    groups: list[Group] = []
    counts: Counter[str] = Counter()
    inventory: dict[str, list[str]] = defaultdict(list)
    with conn.cursor(name="payloads") as cur:
        cur.itersize = 500
        cur.execute(_PAYLOADS)
        for accession, instrument_id, is_tombstone, filed_at, payload in cur:
            counts["payload_rows"] += 1
            if instrument_id is None:
                counts["excluded_no_filing_row"] += 1
                continue
            try:
                root = ET.fromstring(_XMLNS_RE.sub("", _unwrap_sgml_submission(payload)))  # noqa: S314
            except ET.ParseError:
                counts["excluded_unparseable"] += 1
                continue
            if _localname(root.tag) != "ownershipDocument":
                counts["excluded_not_ownership_document"] += 1
                continue
            filers = _extract_filers(root)
            if not filers:
                counts["excluded_no_filers"] += 1
                continue
            cik0 = filers[0].filer_cik
            all_holdings = _extract_holdings(root, default_filer_cik=cik0)
            holdings = [h for h in all_holdings if not h.is_derivative]
            if accession in show:
                for h in all_holdings:
                    kind = "derivative holding" if h.is_derivative else "holding"
                    inventory[accession].append(f"{kind} {h.direct_indirect} {h.security_title!r} {h.shares}")
                for t in _extract_transactions(root, default_filer_cik=cik0):
                    kind = "derivative txn" if t.is_derivative else "txn"
                    inventory[accession].append(
                        f"{kind} {t.direct_indirect} {t.security_title!r} {t.txn_date} bal={t.post_transaction_shares}"
                    )
            payload_type = _text(root.find("./documentType")) or "5"
            filed = filed_at.astimezone(UTC).date() if filed_at is not None else None
            gated_in = payload_type in ("5", "5/A") and filed is not None and form5_within_retention(filed, now=as_of)
            if not holdings:
                kind = "derivative_holdings_only" if all_holdings else "no_holding_parsed"
                counts[kind] += 1
                if gated_in:
                    counts[f"{kind}_replay_eligible"] += 1
                    nd_txns = [t for t in _extract_transactions(root, default_filer_cik=cik0) if not t.is_derivative]
                    any_txns = _extract_transactions(root, default_filer_cik=cik0)
                    shape = (
                        "with_nonderivative_txn" if nd_txns else "with_derivative_txn_only" if any_txns else "no_txn"
                    )
                    counts[f"{kind}_replay_eligible_{shape}"] += 1
                    if is_tombstone:
                        counts[f"{kind}_replay_eligible_tombstoned_{shape}"] += 1
                continue
            counts["with_nonderivative_holding"] += 1
            counts["holding_lines"] += len(holdings)
            # ``_extract_holdings`` walks nonDerivativeHolding in document order, so the elements zip.
            codes = [
                _text(el.find("./transactionCoding/transactionFormType"))
                for el in root.findall("./nonDerivativeTable/nonDerivativeHolding")
            ]
            assert len(codes) == len(holdings), accession
            for code in codes:
                counts[f"holding_lines_form_type_{code or 'none'}"] += 1
            if payload_type not in ("5", "5/A"):
                counts["excluded_document_type"] += 1
                continue
            if filed_at is None or filed is None:
                counts["excluded_no_manifest_filed_at"] += 1
                continue
            if not form5_within_retention(filed, now=as_of):
                counts["excluded_retention"] += 1
                continue
            counts["in_cohort"] += 1
            if is_tombstone:
                counts["in_cohort_tombstoned"] += 1
            period = _date(_text(root.find("./periodOfReport")))
            if period is None:
                counts["period_of_report_missing"] += 1
            elif predates_section_16(period):
                counts["period_of_report_before_section_16"] += 1
            elif period > filed:
                counts["period_of_report_after_filed"] += 1
            else:
                counts[f"period_to_filed {_gap_band(period, filed)}"] += 1
            period_end = period if period is not None and not predates_section_16(period) and period <= filed else None
            exit_box = bool(_flag(_text(root.find("./notSubjectToSection16"))))
            gate = "refused:amendment" if payload_type == "5/A" else "refused:exit_box" if exit_box else None
            txns = [t for t in _extract_transactions(root, default_filer_cik=cik0) if not t.is_derivative]
            covered = {"I" if t.direct_indirect == "I" else "D" for t in txns}
            # A transaction line the writers keep: a balance, and a date valid under #1687 / #2790.
            eligible = {
                "I" if t.direct_indirect == "I" else "D"
                for t in txns
                if t.post_transaction_shares is not None
                and not evaluate_insider_date_validity(
                    t.txn_date,
                    t.deemed_execution_date,
                    filed_at,
                    t.transaction_timeliness,
                    submission_form_type=payload_type,
                    txn_form_type=t.txn_form_type,
                )[0]
            }
            holding_amounts = [((h.security_title or "").strip(), h.shares) for h in holdings if h.shares is not None]
            txn_balances = [
                ((t.security_title or "").strip(), t.post_transaction_shares)
                for t in txns
                if t.post_transaction_shares is not None
            ]
            owner_count = len({(f.filer_cik or "").strip() for f in filers})
            for di in ("D", "I"):
                picked = [(h, c) for h, c in zip(holdings, codes, strict=True) if h.direct_indirect == di]
                group: list[Form4HoldingLine] = [h for h, _ in picked]
                if not group:
                    continue
                if di in covered:
                    blocked = "" if di in eligible else "_no_eligible_transaction"
                    counts[f"mixed_groups_{di}{blocked}"] += 1
                    counts[f"mixed_lines_{di}"] += len(group)
                    continue
                code3 = any(c == "3" for _, c in picked)
                verdict = gate or ("refused:delinquent_form3" if code3 else None)
                verdict = verdict or _form4_holding_verdict(group, holding_amounts, txn_balances, owner_count)
                value = None
                if verdict in _WRITES and period_end is not None:
                    value = sum((h.shares for h in group if h.shares is not None), Decimal(0))
                groups.append(
                    Group(
                        instrument_id=int(instrument_id),
                        accession=accession,
                        is_tombstone=bool(is_tombstone),
                        filed_at=filed_at,
                        filer_cik=(cik0 or "").strip(),
                        di=di,
                        titles=tuple((h.security_title or "").strip() for h in group),
                        shares=tuple(h.shares for h in group),
                        verdict=verdict,
                        value=value,
                        period_end=period_end,
                        code3=code3,
                        joint=owner_count > 1,
                        singleton_flags=_singleton_flags(group[0], holding_amounts, txn_balances)
                        if verdict == "one_line"
                        else (),
                    )
                )
            counts["holding_lines_no_di"] += sum(1 for h in holdings if h.direct_indirect not in _NATURE)
    return groups, counts, inventory


def _compare(g: Group, cur_row: tuple[Any, ...] | None) -> str:
    """The latest written group against the stored winner, by the winner order's keys."""
    if cur_row is None:
        return "new_key"
    assert g.period_end is not None
    source, _sdoc, cur_pend, cur_filed, cur_shares = cur_row
    new_rank, cur_rank = _SOURCE_RANK["form4"], _SOURCE_RANK.get(source, 10)
    new_order = (g.period_end, g.filed_at)
    cur_order = (cur_pend or date.min, cur_filed or datetime.min.replace(tzinfo=g.filed_at.tzinfo))
    # PostgreSQL orders ``period_end DESC, filed_at DESC`` NULLS FIRST.
    if new_rank == cur_rank and (cur_pend is None or (cur_pend == g.period_end and cur_filed is None)):
        return "loses_to_current_null_date"
    if new_rank > cur_rank or (new_rank == cur_rank and new_order < cur_order):
        return "loses_to_current"
    if new_rank == cur_rank and new_order == cur_order:
        return "ties_current"
    if new_rank < cur_rank and cur_order > new_order:
        return "wins_by_source_rank_over_later"
    return "wins_same_value" if cur_shares == g.value else "wins_value_differs"


def _fye_report(rows: list[tuple[Any, ...]]) -> None:
    print("\n== Item 3: periodOfReport vs instrument_sec_profile.fiscal_year_end (every form5_xml) ==")
    bands: Counter[str] = Counter()
    for por, fye, has_filing in rows:
        if not has_filing:
            bands["no insider_filings row"] += 1
        period = _date(por) if por else None
        if period is None:
            bands["no periodOfReport"] += 1
            continue
        if not fye:
            bands["no fiscal_year_end on file"] += 1
            continue
        gap = _fye_distance(period, str(fye))
        if gap is None:
            bands["unreadable fiscal_year_end"] += 1
        elif gap == 0:
            bands["exact month-day"] += 1
        elif gap <= 7:
            bands["1-7 days (52/53-week years)"] += 1
        else:
            bands[">7 days"] += 1
    print(f"  payloads: {len(rows):,}")
    for k, n in sorted(bands.items()):
        print(f"  {k}: {n:,}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", type=datetime.fromisoformat, default=None, help="retention instant (UTC ISO)")
    ap.add_argument("--examples", type=int, default=3)
    ap.add_argument("--show", action="append", default=[], help="print every group of this accession")
    args = ap.parse_args()
    if args.as_of is not None and args.as_of.tzinfo is None:
        ap.error("--as-of needs an explicit offset, e.g. 2026-10-18T23:00:00Z")
    as_of: datetime = (args.as_of or datetime.now(UTC)).astimezone(UTC)
    print(f"as_of {as_of.isoformat()}")

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        groups, counts, inventory = collect(conn, as_of, set(args.show))
        current_rows = conn.execute(_CURRENT).fetchall()
        fye_rows = conn.execute(_FYE).fetchall()

    current: dict[tuple[int, str, str], tuple[Any, ...]] = {}
    current_accession: dict[tuple[int, str, str], str | None] = {}
    dera_by_accession: dict[tuple[int, str, str], list[tuple[Any, ...]]] = defaultdict(list)
    for r in current_rows:
        iid, hik, nature, source, sdoc, sacc, pend, filed, shares = r
        current[(iid, hik, nature)] = (source, sdoc, pend, filed, shares)
        current_accession[(iid, hik, nature)] = sacc
        if sdoc and _DERA_ID.search(sdoc):
            dera_by_accession[(iid, hik, sacc)].append(r)

    print("== cohort ==")
    for k in sorted(counts):
        print(f"  {k}: {counts[k]:,}")

    print("\n== holding-only groups (instrument, accession, filer, D/I) ==")
    shape: Counter[tuple[str, str, str]] = Counter()
    for g in groups:
        shape[(g.di, "tombstone" if g.is_tombstone else "live", g.verdict)] += 1
    for (di, tomb, verdict), n in sorted(shape.items()):
        print(f"  {di} {tomb:9s} {verdict:34s} {n:,}")
    written = [g for g in groups if g.value is not None]
    undated = sum(1 for g in groups if g.verdict in _WRITES and g.period_end is None)
    print(f"  writable verdict but no period_end: {undated:,}")
    print(f"  written groups: {len(written):,}")
    print(f"  ... containing a delinquent-Form-3 (code 3) line: {sum(1 for g in written if g.code3):,}")
    print(f"  ... of them summed with other lines: {sum(1 for g in written if g.code3 and len(g.shares) > 1):,}")
    print(f"  ... one line on a joint filing (written to filers[0]): {sum(1 for g in written if g.joint):,}")
    print(f"  ... with no numeric filer CIK: {sum(1 for g in written if not g.filer_cik.isdigit()):,}")
    print(f"  refused groups containing a code-3 line: {sum(1 for g in groups if g.code3 and g.value is None):,}")
    singles: Counter[str] = Counter(f for g in written for f in g.singleton_flags)
    print(f"  written one-line groups failing a multi-line condition: {sum(1 for g in written if g.singleton_flags):,}")
    for flag, n in sorted(singles.items()):
        print(f"    {flag}: {n:,}")

    best: dict[tuple[int, str, str], Group] = {}
    for g in written:
        if not g.filer_cik.isdigit():
            continue
        key = (g.instrument_id, f"CIK:{g.filer_cik.zfill(10)}", _NATURE[g.di])
        prev = best.get(key)
        assert g.period_end is not None
        prev_order = (prev.period_end or date.min, prev.filed_at, prev.accession) if prev else None
        if prev_order is None or (g.period_end, g.filed_at, g.accession) > prev_order:
            best[key] = g

    print("\n== the latest written group per key vs the stored _current winner ==")
    verdicts: Counter[tuple[str, str]] = Counter()
    instruments: dict[str, set[int]] = defaultdict(set)
    examples: dict[str, list[str]] = defaultdict(list)
    winning: dict[tuple[int, str, str], set[str]] = defaultdict(set)
    tie_same_accession: Counter[bool] = Counter()
    code3_moves = 0
    for key, g in best.items():
        cur_row = current.get(key)
        verdict = _compare(g, cur_row)
        prov = "-"
        if cur_row is not None:
            prov = f"{cur_row[0]}:{'dera' if cur_row[1] and _DERA_ID.search(cur_row[1]) else 'xml'}"
        verdicts[(verdict, prov)] += 1
        instruments[verdict].add(key[0])
        same_accession = current_accession.get(key) == g.accession
        dera_winner = prov == "form4:dera"
        if verdict == "ties_current":
            tie_same_accession[same_accession and dera_winner] += 1
        # A tie with a same-accession DERA row goes to the XML row (de-collision clause 1).
        dera_tie = verdict == "ties_current" and same_accession and dera_winner
        moves = verdict == "new_key" or verdict.startswith("wins") or dera_tie
        if moves:
            winning[(key[0], key[1], g.accession)].add(key[2])
            code3_moves += g.code3
        wanted = verdict in ("wins_value_differs", "new_key", "wins_by_source_rank_over_later")
        if wanted and len(examples[verdict]) < args.examples:
            examples[verdict].append(
                f"iid={key[0]} {key[1]} {g.di} current={cur_row} -> {g.accession} pe={g.period_end} "
                f"value={g.value} lines={list(g.shares)}"
            )
    for verdict in ("wins_by_source_rank_over_later",):
        verdicts.setdefault((verdict, "-"), 0)
    for (verdict, prov), n in sorted(verdicts.items()):
        print(f"  {verdict:32s} {prov:12s} {n:,}")
    ties = sum(tie_same_accession.values())
    print(f"  ties whose winner is a same-accession DERA row: {tie_same_accession[True]:,} of {ties:,}")
    for verdict, iids in sorted(instruments.items()):
        n = sum(v for (vv, _p), v in verdicts.items() if vv == verdict)
        print(f"  {verdict}: keys {n:,} over instruments {len(iids):,}")
    print(f"  keys that move and hold a code-3 line: {code3_moves:,}")

    print("\n== DERA _current winners on an accession whose holding group takes a key (de-collision clause 1) ==")
    won_keys = {(k[0], k[1], nature) for k, natures in winning.items() for nature in natures}
    affected = {id(r): r for k in winning for r in dera_by_accession.get(k, [])}.values()
    replaced = [r for r in affected if (r[0], r[1], r[2]) in won_keys]
    dropped = [r for r in affected if (r[0], r[1], r[2]) not in won_keys]
    print(f"  DERA winners replaced (their key is taken by a holding group): {len(replaced):,}")
    dropped_iids = len({r[0] for r in dropped})
    print(f"  DERA winners dropped (their key is not taken): {len(dropped):,} over instruments {dropped_iids:,}")
    for r in dropped:
        print(f"    {r[0]} {r[1]} {r[2]} {r[4]} pe={r[6]} shares={r[8]}")

    _fye_report(fye_rows)

    print("\n== examples ==")
    for k, rows in sorted(examples.items()):
        print(f"  [{k}]")
        for row in rows:
            print(f"    {row}")
    for accession in args.show:
        print(f"\n== --show {accession} ==")
        for line in inventory.get(accession, []):
            print(f"  line: {line}")
        for g in groups:
            if g.accession == accession:
                print(
                    f"  iid={g.instrument_id} CIK:{g.filer_cik} {g.di} {g.verdict} value={g.value} "
                    f"pe={g.period_end} code3={g.code3} lines={list(zip(g.titles, g.shares, strict=True))}"
                )
                key = (g.instrument_id, f"CIK:{g.filer_cik.zfill(10)}", _NATURE[g.di])
                print(f"    current winner: {current.get(key)}")


if __name__ == "__main__":
    main()
