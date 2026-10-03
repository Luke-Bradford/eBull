"""Ranking-pot-v2 pure core (#3592 slice 2).

Spec ``docs/proposals/execution/2026-10-03-3592-ranking-pot-v2.md``: §2's CMP (2012) opportunistic-purchase
indicator and FINRA days-to-cover, §5's midrank composite and order, §6's stratified permutation controls, and the
snapshot's versioned ``v2`` block.

Everything here is pure. Slice 3's reader hands in the rows it read inside the rebalance transaction; the ``v2`` block
stores those INPUTS, never this module's outputs, so a replay re-derives every order from the same bytes. v1's
selection (R_t, F_t, ``ranking_pot.decide``) is reused unchanged; this module only supplies the order.

Fixed here by construction (each closes a spec Appendix A item; the PR lists them and the audits behind them):

- Insider rows are taken AS FILED (spec §2 (f)). Form 4 General Instruction 9: an amendment provides each line it ADDS
  (9(a)) or *"the complete line or lines being amended, as amended"* (9(b)), never the unchanged lines. A 9(b) line
  replaces an original line identified only by footnote, and ``insider_filings`` stores no link from an amendment to
  its original, so supersession would need a heuristic match: the original line stays as evidence beside its
  correction, a stated departure. Heuristic exposure: ``scripts.audit_3592_v2_slice2``.
- A qualifying purchase needs ``txn_code = 'P'`` AND ``acquired_disposed_code = 'A'``; a NULL code or direction
  flag never qualifies, in the purchase window or in the classification history (history: ``P``/``A`` or ``S``/``D``).
- Classification is per (reporting-owner CIK, issuer CIK, purchase calendar year), from that pair's open-market rows
  dated in the three preceding calendar years and FILED before 1 January (UTC) of the purchase year. A row with a
  NULL owner or issuer CIK cannot be keyed: a purchase row so affected is excluded and counted.
- The Form 4 corpus is truncated (ingest is capped at a rolling three years, retention rubric §4.3, and the dev
  corpus is dense only from 2023-06). A month before the declaration's frozen ``history_floor`` is UNOBSERVABLE, not
  empty: a pair takes the class CMP gives under every filling of those months, else ``indeterminate`` (excluded and
  counted, as unclassifiable pairs are). Measured before this rule: of the opportunistic pairs with a 2026 purchase,
  a share rested on a month hidden in 2023 and could be routine on full history (``scripts.audit_3592_v2_slice2``).
- DTC is FINRA's value as stored, read by FINRA's data dictionary (*Equity Short Interest Data File Download API*,
  ``daysToCoverNumber``): *"Short Interest / Average Daily Share Volume, Rounded to Hundredths. 1.00 will be displayed
  for any values equal or less than 1 … N/A will be displayed If the days to cover is Zero (i.e., Average Daily Share
  Volume is Zero)"*, with ADV *"NULL values are translated as zero"*, and the N/A encoded as 999.99 in its own
  examples. So a zero (or NULL) ADV row is N/A → missing (``not_available``), never the worst DTC; and a 0 is not a
  value the formula can produce (``api.finra.org`` metadata: *"Days to Cover Quantity. Default value is 0"*) →
  missing (``default_zero``). The 1.00 floor ties many names at the best DTC; midranks share it.
- Percentiles, the composite and the sort are exact ``Fraction`` arithmetic; ties break by ``instrument_id``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from statistics import median_low
from typing import Any, Final, Literal

from app.services import ranking_pot as pot
from app.services.ranking_pot_sim import draw_bijection

STRATEGY_ID: Final = "ranking-pot-v2"
#: §2 (c), by construction from CMP's event-time result: 6 equally weighted calendar months before the target month.
INSIDER_LOOKBACK_MONTHS: Final = 6
#: §2 — CMP: "at least one trade in each of the three preceding years"; routine = the same calendar month in all three.
CMP_HISTORY_YEARS: Final = 3
#: §2 strata — deciles by v1.5 score; unscored names take the index after the last decile.
STRATA_COUNT: Final = 10
UNSCORED_STRATUM: Final = STRATA_COUNT
MIN_STRATUM_SIZE: Final = 2
#: §4 gates, by construction.
DTC_MAX_AGE_DAYS: Final = 31
DTC_COVERAGE_FLOOR: Final = Fraction(95, 100)
V2_BLOCK_SCHEMA: Final = "ranking-pot-v2-snapshot-1"
#: §8 history floor, by construction: reference window and threshold share.
FLOOR_REFERENCE_MONTHS: Final = 24

Classification = Literal["unclassifiable", "indeterminate", "routine", "opportunistic"]
DtcMissing = Literal["no_row", "null", "non_finite", "negative", "not_available", "default_zero"]
DtcRefusal = Literal["dtc_unavailable", "dtc_incomplete"]
StrataRefusal = Literal["stratum_too_small"]
PairKey = tuple[str, str, int]  # (filer CIK, issuer CIK, purchase year)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class InsiderRow:
    """One ``insider_transactions`` row joined to its filing (issuer CIK) and manifest (``filed_at``)."""

    txn_id: int
    accession: str
    instrument_id: int
    filer_cik: str | None
    issuer_cik: str | None
    txn_date: date
    txn_code: str | None
    acquired_disposed_code: str | None
    is_derivative: bool
    txn_date_invalid: bool
    filed_at: datetime


@dataclass(frozen=True)
class DtcRow:
    """One ``finra_short_interest_observations`` row."""

    instrument_id: int
    settlement_date: date
    days_to_cover: Decimal | None
    known_from: datetime
    source_document_id: str
    #: ``average_daily_volume`` as stored; NULL reads as zero (FINRA's dictionary), i.e. DTC not available.
    average_daily_volume: int | None


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts.astimezone(UTC)


# ---------------------------------------------------------------------------
# §2 insider component (CMP 2012)
# ---------------------------------------------------------------------------
def lookback_window(target_session: date, months: int = INSIDER_LOOKBACK_MONTHS) -> tuple[date, date]:
    """``[first, end)``: the ``months`` whole calendar months before ``target_session``'s month."""
    if months <= 0:
        raise ValueError("months must be positive")
    k = target_session.year * 12 + target_session.month - 1 - months
    return date(k // 12, k % 12 + 1, 1), target_session.replace(day=1)


def is_qualifying_purchase(row: InsiderRow) -> bool:
    return (
        row.txn_code == "P" and row.acquired_disposed_code == "A" and not row.is_derivative and not row.txn_date_invalid
    )


def is_open_market(row: InsiderRow) -> bool:
    """A classification-history trade: a purchase (``P``/``A``) or sale (``S``/``D``), non-derivative. Form 4's ``P``
    and ``S`` are "open market or private" (``sec-edgar.md`` §2.3); "open-market" here means those two codes."""
    if row.is_derivative or row.txn_date_invalid:
        return False
    return (row.txn_code, row.acquired_disposed_code) in (("P", "A"), ("S", "D"))


def year_start(year: int) -> datetime:
    return datetime(year, 1, 1, tzinfo=UTC)


def pair_cells(history: Iterable[InsiderRow], key: PairKey) -> tuple[tuple[int, int], ...]:
    """The (year, month) cells behind ``key``'s classification: its open-market rows dated in the three calendar years
    before the purchase year and filed before that year's 1 January (UTC)."""
    filer, issuer, year = key
    cut = year_start(year)
    cells = {
        (r.txn_date.year, r.txn_date.month)
        for r in history
        if r.filer_cik == filer
        and r.issuer_cik == issuer
        and is_open_market(r)
        and year - CMP_HISTORY_YEARS <= r.txn_date.year <= year - 1
        and _utc(r.filed_at) < cut
    }
    return tuple(sorted(cells))


def history_floor(
    month_counts: Mapping[date, int], *, freeze_month: date
) -> date | Literal["history_floor_unavailable"]:
    """§8 ``history_floor``, by construction. ``month_counts``: usable history rows (§2's history trades with both CIKs
    and a manifest ``filed_at``) per calendar month (month starts). Threshold = 10% of the lower median of the
    ``FLOOR_REFERENCE_MONTHS`` complete months before ``freeze_month`` (absent months count 0). The floor is the month
    AFTER the first month of the trailing run of months at or above the threshold that ends with the last complete
    month — the run's first month may be partly ingested, so it is treated as hidden. Refused when the reference window
    has no rows or the last complete month is itself below the threshold."""
    if freeze_month.day != 1 or any(m.day != 1 for m in month_counts):
        raise ValueError("months are month starts")

    def back(m: date, k: int) -> date:
        n = m.year * 12 + m.month - 1 - k
        return date(n // 12, n % 12 + 1, 1)

    reference = [month_counts.get(back(freeze_month, k), 0) for k in range(1, FLOOR_REFERENCE_MONTHS + 1)]
    threshold = Fraction(median_low(reference), 10)
    if threshold == 0 or month_counts.get(back(freeze_month, 1), 0) < threshold:
        return "history_floor_unavailable"
    k = 1
    while month_counts.get(back(freeze_month, k + 1), 0) >= threshold:
        k += 1
    return back(freeze_month, k - 1)


def hidden_months(year: int, history_floor: date) -> frozenset[int]:
    """The months of ``year`` before the corpus floor: nothing there is observable, so a missing cell proves nothing."""
    if history_floor.day != 1:
        raise ValueError("the history floor is a month start")
    return frozenset(m for m in range(1, 13) if date(year, m, 1) < history_floor)


def classify(cells: Iterable[tuple[int, int]], year: int, *, history_floor: date) -> Classification:
    """CMP: classified iff a trade in each of the three preceding years; routine iff one calendar month carries a trade
    in all three; opportunistic otherwise.

    Adapted for a truncated corpus (spec §2 (e)): the class is the one CMP gives under EVERY filling of the months
    before ``history_floor``; when fillings disagree the pair is ``indeterminate``. With the floor at or before the
    first history year's 1 January no month is hidden and this is CMP's rule exactly. Observed cells are fixed (a trade
    seen before the floor still counts); only a MISSING cell before the floor is unknown; a missing cell from the floor
    on is a month with no trade."""
    years = range(year - CMP_HISTORY_YEARS, year)
    seen: dict[int, set[int]] = {y: set() for y in years}
    for y, m in cells:
        if not 1 <= m <= 12:
            raise ValueError(f"month out of range: {m}")
        if y in seen:
            seen[y].add(m)
    hidden = {y: hidden_months(y, history_floor) for y in years}
    if any(not seen[y] and not hidden[y] for y in years):
        return "unclassifiable"  # under every filling some year has no trade
    if set.intersection(*seen.values()):
        return "routine"  # a month observed in all three years
    possibly_routine = any(all(m in seen[y] or m in hidden[y] for y in years) for m in range(1, 13))
    if all(seen.values()) and not possibly_routine:
        return "opportunistic"
    return "indeterminate"


@dataclass(frozen=True)
class InsiderRead:
    #: S₀ names with ``ins = 1``.
    buyers: frozenset[int]
    #: Per buyer, the qualifying purchases' accessions, ascending.
    qualifying: Mapping[int, tuple[str, ...]]
    #: Every (filer, issuer, year) pair behind an in-window purchase of an S₀ name: its class and cells.
    pair_class: Mapping[PairKey, Classification]
    pair_cells: Mapping[PairKey, tuple[tuple[int, int], ...]]
    #: In-window, known qualifying purchases of S₀ names (before the CIK exclusion).
    purchases_in_window: int
    excluded_null_cik: int
    unclassifiable_pairs: int
    #: S₀ names with an in-window purchase from an unclassifiable pair.
    unclassifiable_names: int
    #: Pairs (keyed per purchase year) whose class depends on months before the history floor (§2 (e)), and the S₀
    #: names they touch. A name can be touched AND a buyer through another pair: these count touches, not exclusions.
    indeterminate_pairs: int
    indeterminate_names: int


def insider_read(
    purchases: Iterable[InsiderRow],
    history: Iterable[InsiderRow],
    *,
    s0_ids: Iterable[int],
    target_session: date,
    as_of: datetime,
    history_floor: date,
) -> InsiderRead:
    """§2 ``ins_i`` for every S₀ name. ``purchases`` and ``history`` may be supersets: every rule is re-applied here.
    ``history_floor`` is the declaration's frozen corpus floor (§2 (e))."""
    s0 = frozenset(s0_ids)
    first, end = lookback_window(target_session)
    as_of = _utc(as_of)
    hist = tuple(history)
    in_window = [
        r
        for r in purchases
        if r.instrument_id in s0
        and is_qualifying_purchase(r)
        and first <= r.txn_date < end
        and _utc(r.filed_at) <= as_of
    ]
    keyed: list[tuple[PairKey, InsiderRow]] = []
    excluded = 0
    for r in in_window:
        if r.filer_cik is None or r.issuer_cik is None:
            excluded += 1
        else:
            keyed.append(((r.filer_cik, r.issuer_cik, r.txn_date.year), r))

    known_hist = tuple(h for h in hist if _utc(h.filed_at) <= as_of)
    pair_class: dict[PairKey, Classification] = {}
    cells_of: dict[PairKey, tuple[tuple[int, int], ...]] = {}
    for key, _ in keyed:
        if key not in pair_class:
            cells_of[key] = pair_cells(known_hist, key)
            pair_class[key] = classify(cells_of[key], key[2], history_floor=history_floor)

    qualifying: dict[int, set[str]] = {}
    touched: dict[Classification, set[int]] = {"unclassifiable": set(), "indeterminate": set()}
    for key, r in keyed:
        cls = pair_class[key]
        if cls == "opportunistic":
            qualifying.setdefault(r.instrument_id, set()).add(r.accession)
        elif cls in touched:
            touched[cls].add(r.instrument_id)
    return InsiderRead(
        buyers=frozenset(qualifying),
        qualifying={iid: tuple(sorted(accs)) for iid, accs in sorted(qualifying.items())},
        pair_class=pair_class,
        pair_cells=cells_of,
        purchases_in_window=len(in_window),
        excluded_null_cik=excluded,
        unclassifiable_pairs=sum(1 for c in pair_class.values() if c == "unclassifiable"),
        unclassifiable_names=len(touched["unclassifiable"]),
        indeterminate_pairs=sum(1 for c in pair_class.values() if c == "indeterminate"),
        indeterminate_names=len(touched["indeterminate"]),
    )


# ---------------------------------------------------------------------------
# §2 / §5 days-to-cover component
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DtcRead:
    #: S*, or ``None`` when no S₀ row is observed by ``as_of``.
    settlement_date: date | None
    #: S₀ names with a usable DTC at S*.
    values: Mapping[int, Fraction]
    #: Per S₀ name with a row at S*, its latest observed revision.
    chosen: Mapping[int, DtcRow]
    missing: Mapping[int, DtcMissing]


def dtc_read(rows: Iterable[DtcRow], *, s0_ids: Iterable[int], as_of: datetime) -> DtcRead:
    """§5: S* = the latest settlement date ≤ ``as_of``'s UTC date with a row for an S₀ name observed by ``as_of``; per
    name its latest observed revision there (``known_from``, then ``source_document_id``). An older revision is never
    resurrected when the latest is unusable."""
    s0 = frozenset(s0_ids)
    as_of = _utc(as_of)
    seen = [
        r for r in rows if r.instrument_id in s0 and _utc(r.known_from) <= as_of and r.settlement_date <= as_of.date()
    ]
    if not seen:
        return DtcRead(None, {}, {}, dict.fromkeys(sorted(s0), "no_row"))
    settle = max(r.settlement_date for r in seen)
    chosen: dict[int, DtcRow] = {}
    for r in sorted(
        (r for r in seen if r.settlement_date == settle),
        key=lambda r: (r.instrument_id, _utc(r.known_from), r.source_document_id),
    ):
        chosen[r.instrument_id] = r  # ascending: the last one written per name is its latest revision
    values: dict[int, Fraction] = {}
    missing: dict[int, DtcMissing] = {}
    for iid in sorted(s0):
        row = chosen.get(iid)
        if row is None:
            missing[iid] = "no_row"
        elif row.days_to_cover is None:
            missing[iid] = "null"
        elif not row.days_to_cover.is_finite():
            missing[iid] = "non_finite"
        elif row.days_to_cover < 0:
            missing[iid] = "negative"
        elif not row.average_daily_volume:
            missing[iid] = "not_available"  # FINRA: N/A when ADV is zero (NULL reads as zero); stored as 999.99
        elif row.days_to_cover == 0:
            missing[iid] = "default_zero"  # the formula floors at 1.00; 0 is the field's default
        else:
            values[iid] = Fraction(row.days_to_cover)
    return DtcRead(settle, values, chosen, missing)


def dtc_gate(read: DtcRead, *, as_of: datetime, baseline: int) -> DtcRefusal | None:
    """§4 pre-score gates: no S*, or S* older than ``DTC_MAX_AGE_DAYS`` → ``dtc_unavailable``; usable DTCs fewer than
    95% of the frozen ``baseline`` → ``dtc_incomplete``."""
    if baseline <= 0:
        raise ValueError("baseline must be positive")
    if read.settlement_date is None or read.settlement_date < _utc(as_of).date() - timedelta(days=DTC_MAX_AGE_DAYS):
        return "dtc_unavailable"
    if Fraction(len(read.values)) < DTC_COVERAGE_FLOOR * baseline:
        return "dtc_incomplete"
    return None


# ---------------------------------------------------------------------------
# §5 order
# ---------------------------------------------------------------------------
def midrank_percentiles(values: Mapping[int, Fraction], members: Iterable[int]) -> dict[int, Fraction]:
    """(midrank − ½) / n over the members holding a value, ascending (higher value → higher percentile); a member with
    no value takes ½."""
    member_set = frozenset(members)
    have = sorted((v, iid) for iid, v in values.items() if iid in member_set)
    n = len(have)
    out: dict[int, Fraction] = dict.fromkeys(member_set, Fraction(1, 2))
    i = 0
    while i < n:
        j = i
        while j + 1 < n and have[j + 1][0] == have[i][0]:
            j += 1
        midrank = Fraction(i + j + 2, 2)  # 1-based ranks i+1 .. j+1
        for k in range(i, j + 1):
            out[have[k][1]] = (midrank - Fraction(1, 2)) / n
        i = j + 1
    return out


def composite_scores(
    r_ids: Iterable[int], score: Mapping[int, Decimal], dtc: Mapping[int, Fraction], buyers: frozenset[int]
) -> dict[int, Fraction]:
    """§2 composite = (u_score + u_dtc + u_ins) / 3 within R_t: higher score, lower DTC and ``ins = 1`` rank higher."""
    members = frozenset(r_ids)
    for iid in members:
        s = score.get(iid)
        if s is None or not s.is_finite():
            raise ValueError(f"{iid}: every R member needs a finite score")
    u_score = midrank_percentiles({iid: Fraction(score[iid]) for iid in members}, members)
    u_dtc = midrank_percentiles({iid: -v for iid, v in dtc.items() if iid in members}, members)
    u_ins = midrank_percentiles({iid: Fraction(int(iid in buyers)) for iid in members}, members)
    return {iid: (u_score[iid] + u_dtc[iid] + u_ins[iid]) / 3 for iid in members}


def order_of(composite: Mapping[int, Fraction]) -> tuple[int, ...]:
    """Descending exact composite; ties by ``instrument_id`` (v1 r3-5)."""
    return tuple(sorted(composite, key=lambda iid: (-composite[iid], iid)))


def real_order(universes: pot.Universes, dtc: Mapping[int, Fraction], buyers: frozenset[int]) -> tuple[int, ...]:
    """The shadow's (and the no-SL/TP variant's) order: every R member with its own score, DTC and indicator."""
    return order_of(composite_scores(universes.r_ids, universes.own_score, dtc, buyers))


def donated(
    universes: pot.Universes, donor_of: Mapping[int, int], dtc: Mapping[int, Fraction], buyers: frozenset[int]
) -> tuple[dict[int, Fraction], frozenset[int]]:
    """A control's components on R_t: member i takes donor π(i)'s DTC (absent when the donor has none) and indicator."""
    dtc_k = {iid: dtc[d] for iid in universes.r_ids if (d := donor_of[iid]) in dtc}
    buyers_k = frozenset(iid for iid in universes.r_ids if donor_of[iid] in buyers)
    return dtc_k, buyers_k


def control_order(
    universes: pot.Universes, donor_of: Mapping[int, int], dtc: Mapping[int, Fraction], buyers: frozenset[int]
) -> tuple[int, ...]:
    """§6 control order: donated (dtc, ins) pairs, each member keeping its own v1.5 score. ``dtc`` and ``buyers`` cover
    S₀ (donors can lie outside R_t)."""
    dtc_k, buyers_k = donated(universes, donor_of, dtc, buyers)
    return order_of(composite_scores(universes.r_ids, universes.own_score, dtc_k, buyers_k))


def missing_donor_dtc(universes: pot.Universes, donor_of: Mapping[int, int], dtc: Mapping[int, Fraction]) -> int:
    """R members whose donor has no DTC (§6 diagnostic)."""
    return sum(1 for iid in universes.r_ids if donor_of[iid] not in dtc)


@dataclass(frozen=True)
class OrderDiagnostics:
    """§5 per-rebalance record."""

    r_buyers: int
    r_dtc_coverage: int
    dtc_constant: bool
    ins_constant: bool


def order_diagnostics(r_ids: Iterable[int], dtc: Mapping[int, Fraction], buyers: frozenset[int]) -> OrderDiagnostics:
    members = frozenset(r_ids)
    present = {dtc[iid] for iid in members if iid in dtc}
    flags = {iid in buyers for iid in members}
    return OrderDiagnostics(
        r_buyers=len(members & buyers),
        r_dtc_coverage=sum(1 for iid in members if iid in dtc),
        dtc_constant=len(present) <= 1,
        ins_constant=len(flags) <= 1,
    )


# ---------------------------------------------------------------------------
# §6 strata and the stratified draw
# ---------------------------------------------------------------------------
def score_strata(s0_ids: Iterable[int], scores: Mapping[int, Decimal | None]) -> dict[int, int]:
    """§2 strata: deciles of S₀ by score with nearest-rank cut points at 10..90; a score equal to a cut point takes the
    lower decile (index = number of cut points strictly below it); unscored names → ``UNSCORED_STRATUM``."""
    ids = sorted(set(s0_ids))
    scored = {iid: s for iid in ids if (s := scores.get(iid)) is not None and s.is_finite()}
    cuts = [pot.nearest_rank(scored.values(), 100 * d // STRATA_COUNT) for d in range(1, STRATA_COUNT)]
    out: dict[int, int] = {}
    for iid in ids:
        s = scored.get(iid)
        out[iid] = UNSCORED_STRATUM if s is None else sum(1 for c in cuts if c is not None and c < s)
    return out


def strata_refusal(strata: Mapping[int, int]) -> StrataRefusal | None:
    """The freeze refuses a non-empty stratum of fewer than ``MIN_STRATUM_SIZE`` names."""
    sizes: dict[int, int] = {}
    for s in strata.values():
        sizes[s] = sizes.get(s, 0) + 1
    return "stratum_too_small" if any(n < MIN_STRATUM_SIZE for n in sizes.values()) else None


def stratum_seed(base_seed: bytes, stratum: int) -> bytes:
    """seed_s = sha256(base seed ‖ the stratum index as 4-byte big-endian); base = ``ranking_pot_sim.control_seed``."""
    if len(base_seed) != 32:
        raise ValueError("base seed must be a sha256 digest")
    if not 0 <= stratum <= UNSCORED_STRATUM:
        raise ValueError("stratum out of range")
    return hashlib.sha256(base_seed + stratum.to_bytes(4, "big")).digest()


def draw_stratified(strata: Mapping[int, int], *, base_seed: bytes, k: int) -> dict[int, int]:
    """Control ``k``'s bijection of S₀: v1's ``draw_bijection`` within each stratum, in ascending stratum order."""
    members: dict[int, list[int]] = {}
    for iid, s in strata.items():
        members.setdefault(s, []).append(iid)
    out: dict[int, int] = {}
    for s in sorted(members):
        out.update(draw_bijection(sorted(members[s]), seed=stratum_seed(base_seed, s), k=k))
    return out


# ---------------------------------------------------------------------------
# The snapshot's versioned v2 block (pure; inputs only)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class V2Inputs:
    purchases: tuple[InsiderRow, ...]
    history: tuple[InsiderRow, ...]
    dtc: tuple[DtcRow, ...]
    #: The reader's never-gated counts (§5), e.g. unparsed or tombstoned Form 4 manifest rows in the window.
    reader_counts: Mapping[str, int]


_BLOCK_KEYS: Final = frozenset({"schema", "purchases", "history", "dtc", "reader_counts"})
_INSIDER_KEYS: Final = frozenset(
    {
        "txn_id",
        "accession",
        "instrument_id",
        "filer_cik",
        "issuer_cik",
        "txn_date",
        "txn_code",
        "acquired_disposed_code",
        "is_derivative",
        "txn_date_invalid",
        "filed_at",
    }
)
_DTC_KEYS: Final = frozenset(
    {"instrument_id", "settlement_date", "days_to_cover", "known_from", "source_document_id", "average_daily_volume"}
)


def _insider_json(r: InsiderRow) -> dict[str, Any]:
    return {
        "txn_id": r.txn_id,
        "accession": r.accession,
        "instrument_id": r.instrument_id,
        "filer_cik": r.filer_cik,
        "issuer_cik": r.issuer_cik,
        "txn_date": r.txn_date.isoformat(),
        "txn_code": r.txn_code,
        "acquired_disposed_code": r.acquired_disposed_code,
        "is_derivative": r.is_derivative,
        "txn_date_invalid": r.txn_date_invalid,
        "filed_at": _utc(r.filed_at).isoformat(),
    }


def _dtc_json(r: DtcRow) -> dict[str, Any]:
    return {
        "instrument_id": r.instrument_id,
        "settlement_date": r.settlement_date.isoformat(),
        "days_to_cover": None if r.days_to_cover is None else str(r.days_to_cover),
        "known_from": _utc(r.known_from).isoformat(),
        "source_document_id": r.source_document_id,
        "average_daily_volume": r.average_daily_volume,
    }


def encode_v2_block(inputs: V2Inputs) -> dict[str, Any]:
    """Canonical: str/int/bool/None/list/dict only, rows in a fixed order, so the JSONB round trip keeps the sha."""
    for rows in (inputs.purchases, inputs.history):
        ids = [r.txn_id for r in rows]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate txn_id in a v2 evidence list")
    dtc_keys = [(r.instrument_id, r.settlement_date, r.source_document_id) for r in inputs.dtc]
    if len(set(dtc_keys)) != len(dtc_keys):
        raise ValueError("duplicate DTC row")
    return {
        "schema": V2_BLOCK_SCHEMA,
        "purchases": [_insider_json(r) for r in sorted(inputs.purchases, key=lambda r: r.txn_id)],
        "history": [_insider_json(r) for r in sorted(inputs.history, key=lambda r: r.txn_id)],
        "dtc": [
            _dtc_json(r)
            for r in sorted(
                inputs.dtc,
                key=lambda r: (r.instrument_id, r.settlement_date, _utc(r.known_from), r.source_document_id),
            )
        ],
        "reader_counts": {k: int(v) for k, v in sorted(inputs.reader_counts.items())},
    }


def _malformed(what: str) -> ValueError:
    return ValueError(f"malformed {V2_BLOCK_SCHEMA} block: {what}")


def _int(v: object, what: str) -> int:
    if type(v) is not int:
        raise _malformed(what)
    return v


def _str(v: object, what: str, *, nullable: bool = False) -> str | None:
    if v is None and nullable:
        return None
    if not isinstance(v, str):
        raise _malformed(what)
    return v


def _bool(v: object, what: str) -> bool:
    if type(v) is not bool:
        raise _malformed(what)
    return v


def _aware(v: object, what: str) -> datetime:
    ts = datetime.fromisoformat(_str(v, what) or "")
    if ts.tzinfo is None:
        raise _malformed(what)
    return ts


def _insider_from(doc: object) -> InsiderRow:
    if not isinstance(doc, dict) or set(doc) != _INSIDER_KEYS:
        raise _malformed("insider row keys")
    return InsiderRow(
        txn_id=_int(doc["txn_id"], "txn_id"),
        accession=_str(doc["accession"], "accession") or "",
        instrument_id=_int(doc["instrument_id"], "instrument_id"),
        filer_cik=_str(doc["filer_cik"], "filer_cik", nullable=True),
        issuer_cik=_str(doc["issuer_cik"], "issuer_cik", nullable=True),
        txn_date=date.fromisoformat(_str(doc["txn_date"], "txn_date") or ""),
        txn_code=_str(doc["txn_code"], "txn_code", nullable=True),
        acquired_disposed_code=_str(doc["acquired_disposed_code"], "acquired_disposed_code", nullable=True),
        is_derivative=_bool(doc["is_derivative"], "is_derivative"),
        txn_date_invalid=_bool(doc["txn_date_invalid"], "txn_date_invalid"),
        filed_at=_aware(doc["filed_at"], "filed_at"),
    )


def _dtc_from(doc: object) -> DtcRow:
    if not isinstance(doc, dict) or set(doc) != _DTC_KEYS:
        raise _malformed("dtc row keys")
    raw = _str(doc["days_to_cover"], "days_to_cover", nullable=True)
    return DtcRow(
        instrument_id=_int(doc["instrument_id"], "instrument_id"),
        settlement_date=date.fromisoformat(_str(doc["settlement_date"], "settlement_date") or ""),
        days_to_cover=None if raw is None else Decimal(raw),
        known_from=_aware(doc["known_from"], "known_from"),
        source_document_id=_str(doc["source_document_id"], "source_document_id") or "",
        average_daily_volume=None if doc["average_daily_volume"] is None else _int(doc["average_daily_volume"], "adv"),
    )


def decode_v2_block(doc: object) -> V2Inputs:
    """Strict inverse of ``encode_v2_block``. Raises on an absent, foreign-version or malformed block — never a
    fallback to v1's order (§4)."""
    if not isinstance(doc, dict):
        raise _malformed("not an object")
    if doc.get("schema") != V2_BLOCK_SCHEMA:
        raise ValueError(f"not a {V2_BLOCK_SCHEMA} block")
    if set(doc) != _BLOCK_KEYS:
        raise _malformed("block keys")
    lists = {k: doc[k] for k in ("purchases", "history", "dtc")}
    if not all(isinstance(v, list) for v in lists.values()):
        raise _malformed("row lists")
    counts = doc["reader_counts"]
    if not isinstance(counts, dict) or not all(isinstance(k, str) for k in counts):
        raise _malformed("reader_counts")
    try:
        inputs = V2Inputs(
            purchases=tuple(_insider_from(r) for r in lists["purchases"]),
            history=tuple(_insider_from(r) for r in lists["history"]),
            dtc=tuple(_dtc_from(r) for r in lists["dtc"]),
            reader_counts={k: _int(v, f"reader_counts.{k}") for k, v in counts.items()},
        )
    except (TypeError, ValueError, ArithmeticError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith("malformed"):
            raise
        raise _malformed(str(exc)) from exc
    if encode_v2_block(inputs) != doc:
        raise _malformed("not canonical (row order or value form)")
    return inputs


def v2_block_of(snapshot: Mapping[str, Any]) -> V2Inputs:
    """The ``v2`` block of a v2 rebalance snapshot; raises when it is absent."""
    if "v2" not in snapshot:
        raise ValueError(f"snapshot carries no {V2_BLOCK_SCHEMA} block")
    return decode_v2_block(snapshot["v2"])


def with_v2_block(snapshot: Mapping[str, Any], inputs: V2Inputs) -> dict[str, Any]:
    """v1's snapshot document plus the ``v2`` block, inside the one canonical hash."""
    if "v2" in snapshot:
        raise ValueError("snapshot already carries a v2 block")
    return {**snapshot, "v2": encode_v2_block(inputs)}
