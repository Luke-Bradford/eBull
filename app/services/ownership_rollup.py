"""Tier 0 ownership rollup (#789, parent #788).

Cross-channel deduped ownership snapshot for one instrument, derived
from Form 4 + Form 3 + 13D/G + DEF 14A + 13F under the SEC Rule
13d-3 beneficial-ownership semantic. Single denominator
(``shares_outstanding`` from XBRL DEI), explicit
``Public / unattributed`` residual wedge, and a coverage banner that
distinguishes *float concentration* from *universe coverage* — see
``docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md``
for the full design.

Two ship-blockers from the codex audit (2026-05-03) that this module
closes:

  * **Wrong denominator** — the prior frontend math used
    ``shares_outstanding + treasury_shares``; the canonical
    denominator is ``shares_outstanding`` only, with treasury
    rendered as an additive top wedge.
  * **No cross-channel dedup** — the prior pipeline summed Form 4 +
    13D/A + 13F + DEF 14A as a partition (e.g. Cohen on GME read as
    ~75M shares because his Form 4 cumulative AND his 13D/A row
    both contributed). Dedup priority is
    ``form4 > form3 > 13d/g > def14a > 13f`` per CIK match, with
    DEF 14A enriched via :mod:`app.services.holder_name_resolver`
    before dedup runs (DEF 14A has no filer_cik in the schema).

The reader uses :func:`app.db.snapshot.snapshot_read` at the FastAPI
handler layer to keep every read inside one REPEATABLE READ
transaction so the per-slice numbers, residual, and coverage all
reconcile.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg
import psycopg.rows

from app.services.holder_name_resolver import resolve_holder_to_filer
from app.services.institutional_families import (
    InstitutionalFamily,
    resolve_family,
)
from app.services.instrument_history import (
    SymbolHistoryEntry,
    historical_symbols_for,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public dataclasses (mirrored to Pydantic models in the API layer)
# ---------------------------------------------------------------------------


SliceCategory = Literal["insiders", "blockholders", "institutions", "etfs", "def14a_unmatched", "funds", "esop"]
SourceTag = Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"]
CoverageState = Literal["no_data", "red", "unknown_universe", "amber", "green"]

# Denominator-basis tag per spec
# `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md`
# §"Target chart decomposition". `pie_wedge` slices contribute to the
# residual / concentration math (sum to ≤ shares_outstanding). Memo
# overlays render as additional surface area without affecting the
# pie — used by the funds slice (N-PORT rows are fund-level detail
# INSIDE the 13F-HR institutional aggregate; counting them additively
# would double-count) and the esop slice (#961; DEF 14A plan-trustee
# rows are a Rule 13d-3 deemed-ownership disclosure, same basis as
# def14a_unmatched — not a distinct institutional holding). Future
# DRS / short-interest overlays land here too.
#   * ``institution_subset`` — fund-level detail inside the 13F-HR
#     institutional aggregate (funds / N-PORT, #919).
#   * ``proxy_disclosure`` — DEF 14A "Security Ownership of Certain
#     Beneficial Owners" rows (#1659), including ESOP/employee-benefit-
#     plan rows (#961). A Rule 13d-3 deemed-ownership disclosure (SEC
#     Item 403) where the same securities are listed under multiple
#     owners (control groups, parent/sub, spouse attribution, "all
#     officers as a group" aggregates, plan trustees) — overlapping,
#     NOT additive. The real holders are already counted + de-duplicated
#     via 13D/G, 13F, Form 4; the proxy is a cross-check, not a wedge
#     (reverses #1627's additive treatment — see data-engineer I14/I16).
DenominatorBasis = Literal["pie_wedge", "institution_subset", "proxy_disclosure"]

# Why an ``OwnershipRollup.no_data`` payload carries no usable denominator.
# ``absent`` — no shares-outstanding row on file at all.
# ``stale_denominator`` — a row exists but its ``as_of`` is too old to use
#   (the #1581 dual-class dimension-only trap, or an ingest-coverage gap);
#   percentages would be nonsense against it, so we suppress them.
NoDataReason = Literal["absent", "stale_denominator"]


@dataclass(frozen=True)
class DroppedSource:
    """Provenance for a losing source in the dedup race. Surfaces in
    the provenance footer so the operator can see "Form 4 won; the
    13D/A you'd expect to see also reports 36.85M for the same
    filer". One row per losing source per holder."""

    source: SourceTag
    accession_number: str
    shares: Decimal
    as_of_date: date | None
    edgar_url: str | None  # Provenance link to the SEC archive index


@dataclass(frozen=True)
class FamilyMember:
    """One constituent row inside a collapsed institutional family
    (#1644 + #1649). Display-only breakdown for the L2 filer table — a
    family's individual 13F sub-CIK holdings, carried on the family
    :class:`Holder` so the operator still sees which entities filed.

    NOT re-summed into residual / concentration: the family Holder's
    ``shares`` already IS the family figure (the within-channel
    aggregation), so these members are a breakdown, not additive shares."""

    filer_cik: str | None
    filer_name: str
    shares: Decimal
    source: SourceTag
    accession_number: str
    edgar_url: str | None
    as_of_date: date | None


@dataclass(frozen=True)
class CorrectionApplied:
    """A machine-readable record of a figure-changing correction applied at
    rollup-read time. The first kind (#1639) is ``suppressed_by_13f_nt``: a
    filer's stale 13F-HR was excluded because the filer filed a 13F-NT for a
    later quarter.

    This is the structured down-payment on the ownership machine-trust contract
    (#1647): a decision agent (or operator) sees not just the corrected figure
    but WHY it changed — which filer, how many shares left the institutions
    slice, and which channel folded. Distinct from :class:`DroppedSource` (which
    records same-owner dedup losers that are still counted once); a correction
    here REMOVES shares from the total.

    Closed ``kind`` vocab:
      * ``suppressed_by_13f_nt`` (#1639) — a filer's stale 13F-HR removed because
        the filer filed a 13F-NT for a later quarter. NT-specific fields set.
      * ``def14a_restates_institution`` (#1644) — a proxy 5%-holder figure folded
        under a larger 13F family sum (the channel restated, did not add).
      * ``institutional_family_collapse`` (#1649) — a family's 13F shell figure
        folded under a larger proxy/13G consolidated figure (gap-fill).
      * ``blockholder_group_collapse`` (#1645) — a Rule 13d-5 group's members each
        reported the same aggregate stake on separate 13D/G accessions; the group is
        counted once at MAX and the other members are folded. ``source_channel`` ==
        ``winning_source`` (intra-blockholder-channel collapse); the per-member fold
        detail is in ``detail`` + the surviving holder's ``dropped_sources``.
      * ``insider_control_group_collapse`` (#1652) — a sponsor's GP/LP chain reported the
        same deemed block under many related CIKs across Form 4 / Form 3 / 13D / 13G; the
        cross-channel group is counted once (insiders slice) and the other members folded.
        Folded member CIK+name+shares live in ``detail``.

    The NT-specific fields are ``Optional`` for the #1644/#1649/#1645 kinds (no NT
    quarters). The generic ``family_id`` / ``source_channel`` (the folded channel)
    / ``winning_source`` / ``winning_accession`` / ``detail`` make every kind's
    provenance non-lossy (Codex spec review F8). ``filer_cik`` is nullable: a
    proxy-name-only fold has no CIK."""

    kind: str
    filer_name: str
    shares_removed: Decimal
    filer_cik: str | None = None
    # NT-specific (suppressed_by_13f_nt only)
    superseded_period: date | None = None  # the excluded HR's quarter
    winning_nt_period: date | None = None  # the Notice quarter that superseded it
    winning_nt_accession: str | None = None
    # Generic provenance (all kinds)
    family_id: str | None = None
    source_channel: SourceTag | None = None  # the losing/folded channel
    winning_source: SourceTag | None = None
    winning_accession: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class HolderLot:
    """One additive Section-16 lot (``direct`` / ``indirect``) of an owner
    collapsed to a single display line (#1942). Display-only drilldown breakdown:
    the lots SUM to the parent :class:`Holder`'s ``shares`` (Form 4 General
    Instruction 4(b) reports direct/indirect on separate lines; #905 keeps them
    additive), so they are NOT counted again. Distinct from ``family_members``
    (13F sub-CIKs of one manager family) and ``dropped_sources`` (channels NOT
    counted in the figure)."""

    ownership_nature: str | None
    shares: Decimal
    source: SourceTag
    accession_number: str
    edgar_url: str | None
    as_of_date: date | None


@dataclass(frozen=True)
class Holder:
    """A canonical holder after cross-channel dedup. ``winning_source``
    decides which slice this holder lands in (insiders /
    blockholders / institutions / etfs)."""

    filer_cik: str | None
    filer_name: str
    shares: Decimal
    pct_outstanding: Decimal
    winning_source: SourceTag
    winning_accession: str
    winning_edgar_url: str | None  # Direct link to the SEC archive index
    as_of_date: date | None
    filer_type: str | None  # 13F filer-type tag; None for non-13F survivors
    dropped_sources: tuple[DroppedSource, ...]
    # Constituent rows of a collapsed institutional family (#1644 + #1649);
    # display-only breakdown, NOT additive. Empty for ordinary holders.
    family_members: tuple[FamilyMember, ...] = ()
    # Form 4/3 ownership nature (``direct`` / ``indirect`` / ``beneficial``);
    # carried so :func:`_source_rows_and_total` can apply the within-source
    # additive-vs-overlapping regime (#905 / prevention-log 1835). ``None`` for
    # holders where nature is not meaningful (13F / family reps / treasury).
    ownership_nature: str | None = None
    # Per-lot breakdown when this owner's additive Section-16 lots (direct +
    # indirect) were collapsed to one display line (#1942). Display-only; the
    # lots SUM to ``shares`` (already counted once). Empty for single-lot owners.
    lots: tuple[HolderLot, ...] = ()


@dataclass(frozen=True)
class OwnershipSlice:
    """One slice on the ownership card (insiders, blockholders, etc.).
    ``filer_count`` is the number of *deduped* holders contributing
    to ``total_shares`` — a category that resolved 7 holders shows
    7 here even when the underlying 13F filings had option exposure
    rows on top of the equity rows.

    ``denominator_basis`` (added with the funds slice in #919) tags
    whether this slice is part of the pie wedges that sum to
    shares_outstanding (``pie_wedge``) or a memo overlay rendered
    alongside the pie without contributing to its math
    (``institution_subset``). The residual + concentration computations
    only sum slices marked ``pie_wedge``."""

    category: SliceCategory
    label: str
    total_shares: Decimal
    pct_outstanding: Decimal
    filer_count: int
    dominant_source: SourceTag | None
    holders: tuple[Holder, ...]
    denominator_basis: DenominatorBasis = "pie_wedge"
    # As-of coherence envelope (#1647 part 1). The as-of span of this slice's
    # deduped holders, so a machine consumer can see the figure sums across
    # quarters (98.2% of dev instruments mix ≥2 13F quarters in the
    # institutions slice). Computed in :func:`_build_slice` from each holder's
    # ``as_of_date`` PLUS its ``family_members`` as-of dates — a collapsed
    # family is one Holder whose own ``as_of_date`` is ``min(member dates)``,
    # so looking only at the holder would hide an intra-family quarter spread
    # (Codex ckpt-1). NULL-as_of holders are ignored; an all-NULL slice keeps
    # the defaults (None / 0 / False).
    as_of_min: date | None = None
    as_of_max: date | None = None
    distinct_quarters: int = 0
    mixed_period: bool = False


@dataclass(frozen=True)
class ResidualBlock:
    """``Public / unattributed`` wedge. ``oversubscribed=True`` when
    deduped slices + treasury exceed shares_outstanding (stale
    13F + fresh Form 4 / 13D mix); residual clamps to 0 in that
    case and the frontend renders the warning bar."""

    shares: Decimal
    pct_outstanding: Decimal
    label: str
    tooltip: str
    oversubscribed: bool


@dataclass(frozen=True)
class CategoryCoverage:
    known_filers: int
    estimated_universe: int | None
    pct_universe: Decimal | None
    state: CoverageState
    # Honest machine completeness flag (#1647 part 2, shippable half).
    # ``True`` ⇔ no real filer-universe estimate exists for this category
    # (``estimated_universe is None``) → the figure is a floor, not a measured
    # share of a known universe. A real seeded ``estimate == 0`` (vacuously-
    # green, "we know the SEC universe here is empty") is NOT an estimate.
    # The real ``coverage_ratio`` gate is blocked on the per-instrument 13F
    # universe-count ingest → DEFERRED #790. Derived in :func:`_compute_coverage`.
    is_estimate: bool = True


@dataclass(frozen=True)
class CoverageReport:
    state: CoverageState
    categories: dict[str, CategoryCoverage]


@dataclass(frozen=True)
class ConcentrationInfo:
    """Float concentration shown as an info chip — NOT the banner
    driver. ``pct_outstanding_known`` is sum of deduped slices
    over outstanding; treasury is excluded from the numerator
    because the issuer doesn't 'invest' in itself."""

    pct_outstanding_known: Decimal
    info_chip: str


@dataclass(frozen=True)
class Def14ADriftInfo:
    """DEF 14A vs Form 4 cumulative drift summary for the chip (#966).

    Built from ``def14a_drift_alerts`` (written by
    ``def14a_drift.detect_drift`` at the ingest/rewash/weekly loci) —
    a read-only summary, never a detector invocation. ``info``-severity
    alerts (unmatched proxy names) are EXCLUDED: that population is
    already visible as the ``def14a_unmatched`` slice, and a chip
    restating it would double-signal. Drift is a COVERAGE-INTEGRITY
    signal (missed/late Form 4s, ingest gap) — never a holdings
    correction (Item 403 vs Section 16(a) restate the same shares;
    prevention-log #1851/#1852)."""

    worst_severity: Literal["warning", "critical"]
    alert_count: int
    chip: str
    holders: tuple[str, ...]  # top 3 holder names by |drift_pct| DESC


@dataclass(frozen=True)
class SanityChecks:
    """Raw plausibility facts over the pie-wedge slices (#1647 part 4).

    NOT pass/fail thresholds — measurable facts a decision agent or operator
    can reason over to catch the NEXT silent inflation bug. The only guard
    today (``residual.oversubscribed``) cannot express sub-residual
    granularity, so it never trips on a sub-100% inflation like #1639 (AAPL
    44.4%). Memo-overlay slices (``denominator_basis != "pie_wedge"``) are
    excluded everywhere — they are already-counted detail.

      * ``max_distinct_quarters`` — worst per-slice as-of spread; >1 means at
        least one slice sums filings from different quarters.
      * ``institutions_pct`` — Σ pie-wedge institutions+etfs / outstanding.
      * ``institutions_over_100pct`` — institutions own >100% (impossible).
      * ``largest_single_holder_pct`` — biggest single deduped pie-wedge
        holder / outstanding (a collapsed family is one holder, so this is
        the committee's "single-family plausibility").
      * ``any_pie_slice_over_100pct`` — any single slice exceeds 100%.

    ``outstanding <= 0`` → all pct ``Decimal(0)``, both booleans ``False``."""

    max_distinct_quarters: int
    institutions_pct: Decimal
    institutions_over_100pct: bool
    largest_single_holder_pct: Decimal
    any_pie_slice_over_100pct: bool

    @classmethod
    def empty(cls) -> SanityChecks:
        """Zeroed checks for the ``no_data`` path (no slices to measure)."""
        return cls(
            max_distinct_quarters=0,
            institutions_pct=Decimal(0),
            institutions_over_100pct=False,
            largest_single_holder_pct=Decimal(0),
            any_pie_slice_over_100pct=False,
        )


@dataclass(frozen=True)
class BannerCopy:
    state: CoverageState
    variant: Literal["error", "warning", "info", "success"]
    headline: str
    body: str


@dataclass(frozen=True)
class SharesOutstandingSource:
    accession_number: str | None
    concept: str | None
    form_type: str | None
    edgar_url: str | None  # Pre-computed archive index URL, NULL when accession is null


@dataclass(frozen=True)
class DualClassDenominator:
    """Honest-degradation marker (#1646): this instrument is one share class of a
    multi-class issuer whose classes share a single SEC CIK (GOOG/GOOGL, BRK.A/
    BRK.B), but the only shares-outstanding figure on file is the issuer's
    **combined all-class** count.

    The per-class ``dei:EntityCommonStockSharesOutstanding`` exists only in the
    per-filing XBRL instance, tagged with a ``us-gaap:StatementClassOfStockAxis``
    member — the SEC companyfacts/companyconcept API strips those dimensional
    facts, so for a multi-class issuer the per-class count is absent from our
    pipeline entirely (only the combined us-gaap ``CommonStockSharesOutstanding``
    survives). The precise per-class denominator rides on #1590 (DERA FSDS, whose
    ``num.tsv`` carries the dimensional ``segments`` column).

    Consequence: every percentage in this rollup is a combined-basis **lower
    bound** — the true per-class concentration is higher. The numerators are
    per-class-correct for the CUSIP-resolved channels (institutions / insiders /
    blockholders); only the denominator is coarse. This field flags the caveat so
    the chart's percentages are not silently misread as per-class figures. It
    removes no shares and is therefore NOT a :class:`CorrectionApplied`."""

    cik: str
    # All traded classes sharing the CIK (incl. self), symbol-sorted. Class B and
    # other untraded classes are absent — they have no ``instruments`` row.
    sibling_symbols: tuple[str, ...]
    note: str  # Server-owned copy for the FE caveat callout (single source).


@dataclass(frozen=True)
class PerClassDenominator:
    """The rollup was divided by a VERIFIED per-class share count from the SEC
    DERA FSDS (``instrument_class_shares_outstanding``, sql/200), not the issuer's
    combined all-class count — so every percentage is per-class-true and the #1646
    :class:`DualClassDenominator` caveat is SUPERSEDED (the two are mutually
    exclusive: when this is set, ``dual_class_denominator`` is None).

    Provenance only — it changes the operative denominator but removes no shares,
    so it is NOT a :class:`CorrectionApplied` (whose contract is share removal).
    The read-path applies it only when the fail-closed guards pass (period
    coherence with the combined ``shares_outstanding_as_of``; ``0 < per_class <
    combined``; no pie-wedge holder exceeds the per-class count); otherwise the
    combined denominator + the #1646 caveat are preserved (#788 per-class spec)."""

    cik: str
    class_member: str  # FSDS ClassOfStock localname (CommonClassA / CapitalClassC / …)
    period_end: date  # FSDS class period; == the combined as_of by the read-path guard
    per_class_shares: Decimal  # the denominator actually used
    combined_shares: Decimal  # what #1646 would have divided by (transparency)
    source_adsh: str
    source_fsds_qtr: str
    note: str  # Server-owned copy for the FE info callout (single source).


@dataclass(frozen=True)
class DenominatorCrossCheck:
    """Independent tie-out of the operative shares-outstanding denominator to a
    SECOND SEC-native figure (#1647 part 5). The denominator is the highest-leverage
    number in the rollup (every wedge % divides by it) and the one figure the as-of
    coherence (pt1) + sanity (pt4) layers structurally CANNOT validate: a
    wrong-but-self-consistent denominator (#1646's 2x dual-class error) passes every
    internal check. This reconciles it against an independent SEC disclosure of the
    same quantity, as facts (NOT a gate — it never changes a share count).

    ``primary_value`` / ``comparison_value`` are THE TWO FIGURES THIS CHECK COMPARES —
    NOT necessarily the rollup's operative denominator, so ``pct_diff`` is one uniform
    formula across methods. ``primary_concept`` names which is which:

      * ``independent_concept`` — single-class: ``primary`` = the denominator used
        (us-gaap ``CommonStockSharesOutstanding`` balance-sheet OR dei
        ``EntityCommonStockSharesOutstanding`` cover-page); ``comparison`` = the OTHER
        SEC concept, the row nearest ``primary``'s period (they are inherently
        different instants — cover-page is dated weeks after quarter-end — so
        ``as_of_delta_days`` surfaces the skew). A genuine independent cross-source.
      * ``per_class_subset_bound`` — dual-class: ``primary`` = Σ resolved per-class
        FSDS counts (sibling instruments); ``comparison`` = the combined all-class
        us-gaap count at the same FSDS instant. NOT independent (same FSDS family;
        omits untraded Class B) — only flags the IMPOSSIBLE (sum > combined). The
        operative per-class denominator was already cross-source-verified at #1623
        ingest + is structurally guarded (``_should_use_class_denominator``); this is
        a thin backstop. The rollup's actual per-class denominator is in
        ``OwnershipRollup.per_class_denominator``, not duplicated here.
      * ``unavailable`` — no_data / ``outstanding <= 0`` / no comparison figure on file
        / ``comparison_value <= 0``.

    The aggregate ownership PERCENTAGES (institutions / insiders / blockholders) have
    NO independent source — every vendor sums the same SEC 13F/Form 4/13D filings and
    disagrees only by method (GOOGL: Fintel 84.99% vs ours 79.78%). That single-source
    fact is a global contract truth documented in the metrics-analyst skill, NOT a
    per-response field."""

    method: Literal["independent_concept", "per_class_subset_bound", "unavailable"]
    primary_value: Decimal | None
    primary_concept: str | None
    comparison_value: Decimal | None
    comparison_concept: str | None
    primary_as_of: date | None
    comparison_as_of: date | None
    as_of_delta_days: int | None
    pct_diff: Decimal | None  # (primary_value - comparison_value) / comparison_value — uniform
    status: Literal["agrees", "minor_skew", "diverges", "plausible", "unavailable"]
    note: str  # Server-owned copy for the FE provenance caption (single source).

    @classmethod
    def unavailable(cls) -> DenominatorCrossCheck:
        """No comparison figure on file (no_data path, dimension-stripped dual-class
        with no combined sibling sum, thin single-concept issuer, or outstanding<=0)."""
        return cls(
            method="unavailable",
            primary_value=None,
            primary_concept=None,
            comparison_value=None,
            comparison_concept=None,
            primary_as_of=None,
            comparison_as_of=None,
            as_of_delta_days=None,
            pct_diff=None,
            status="unavailable",
            note="No independent SEC figure on file to cross-check the share-count denominator.",
        )


@dataclass(frozen=True)
class OwnershipRollup:
    symbol: str
    instrument_id: int
    shares_outstanding: Decimal | None
    shares_outstanding_as_of: date | None
    shares_outstanding_source: SharesOutstandingSource
    treasury_shares: Decimal | None
    treasury_as_of: date | None
    slices: tuple[OwnershipSlice, ...]
    residual: ResidualBlock
    concentration: ConcentrationInfo
    coverage: CoverageReport
    banner: BannerCopy
    # Historical symbols from ``instrument_symbol_history`` (#794
    # frontend finish, Batch 7 of #788). Empty for instruments
    # without a backfilled chain. Frontend renders a "Filed as X"
    # callout when the chain includes any symbol other than the
    # current one — useful for BBBY → BBBYQ ticker-change cases
    # where filings under the prior symbol still belong to this
    # instrument.
    historical_symbols: tuple[SymbolHistoryEntry, ...]
    computed_at: datetime
    # Figure-changing corrections applied at read time (#1639 / #1647). Today:
    # 13F-NT supersessions. Empty when no correction fired. First-class
    # structured JSON (NOT only the CSV ``__suppressed_by_13f_nt:`` memo) so a
    # machine consumer can see why the institutions total changed. Last field
    # with a default so existing constructors (CSV-test fixtures, ``no_data``)
    # need no change.
    corrections_applied: tuple[CorrectionApplied, ...] = ()
    # Multi-class denominator caveat (#1646). Non-None only when this instrument
    # shares its SEC CIK with another traded share class, so the rollup's
    # denominator is the combined all-class count and every percentage is a
    # combined-basis lower bound. None for single-class issuers and the no_data
    # path. Last field with a default so existing constructors need no change.
    dual_class_denominator: DualClassDenominator | None = None
    # Per-class denominator applied (#788). Non-None only when a verified FSDS
    # per-class share count replaced the combined denominator (so every pct is
    # per-class-true and ``dual_class_denominator`` is None). Mutually exclusive
    # with ``dual_class_denominator``. None on the no_data path + single-class
    # issuers. Last field with a default so existing constructors need no change.
    per_class_denominator: PerClassDenominator | None = None
    # Sanity-invariant facts over the pie-wedge slices (#1647 part 4). Raw
    # plausibility measurements (not pass/fail) so a machine consumer can
    # catch the next silent inflation. Zeroed on the no_data path. Last field
    # with a default so existing constructors need no change.
    sanity: SanityChecks = field(default_factory=SanityChecks.empty)
    # Independent denominator tie-out (#1647 part 5). Facts, not a gate. The two
    # real constructors (the main assembly + ``no_data``) set it explicitly; the
    # default_factory is only the safety net for CSV-test fixtures, mirroring how
    # ``sanity`` was added (Codex ckpt-1 LOW — no silent default masking a wiring miss).
    denominator_cross_check: DenominatorCrossCheck = field(default_factory=DenominatorCrossCheck.unavailable)
    # DEF 14A vs Form 4 drift chip (#966). None when the instrument has no
    # warning/critical drift alerts. Carried through BOTH the main assembly
    # and ``no_data`` — a coverage-integrity signal must not vanish exactly
    # when the rollup is otherwise degraded. Last field with a default so
    # existing constructors need no change.
    def14a_drift: Def14ADriftInfo | None = None

    @classmethod
    def no_data(
        cls,
        symbol: str,
        instrument_id: int,
        historical_symbols: tuple[SymbolHistoryEntry, ...] = (),
        *,
        reason: NoDataReason = "absent",
        stale_as_of: date | None = None,
        def14a_drift: Def14ADriftInfo | None = None,
    ) -> OwnershipRollup:
        """Empty payload for the ``no_data`` state. 200 OK with the error
        banner, not 503 — the frontend renders a uniform empty state
        rather than crashing on a non-2xx response.

        Two reasons share this path, distinguished only by server-owned
        copy (the coverage state stays ``no_data`` either way, so the
        #840/#923 5-state machine is unchanged):

        * ``absent`` — no shares-outstanding row on file. Generic banner
          tells the operator to trigger a fundamentals sync.
        * ``stale_denominator`` — a row exists but its ``as_of`` is too
          old to use (#1581). Honest banner names the date; the stale
          ``as_of`` is RETAINED in ``shares_outstanding_as_of`` as the FE
          discriminator (``absent`` keeps it null). ``stale_as_of`` is
          required for this reason — it is the only provenance that
          survives once the denominator is nulled.

        ``historical_symbols`` is threaded through so the BBBY-style
        callout still renders on instruments missing
        ``shares_outstanding`` — that case is exactly when the operator
        wants the "filings before symbol change still belong here" hint.
        Codex pre-push review (Batch 7 of #788) caught the prior version
        dropping the chain on this path."""
        residual = ResidualBlock(
            shares=Decimal(0),
            pct_outstanding=Decimal(0),
            label="Public / unattributed",
            tooltip=_RESIDUAL_TOOLTIP,
            oversubscribed=False,
        )
        coverage = CoverageReport(state="no_data", categories={})
        if reason == "stale_denominator":
            if stale_as_of is None:
                raise ValueError("no_data(reason='stale_denominator') requires stale_as_of")
            banner = _stale_denominator_banner(stale_as_of)
            info_chip = "Shares-outstanding figure on file is too stale to use as a denominator."
            as_of_out: date | None = stale_as_of
        else:
            banner = _banner_for_state("no_data", coverage, Decimal(0))
            info_chip = "No shares-outstanding figure on file."
            as_of_out = None
        return cls(
            symbol=symbol,
            instrument_id=instrument_id,
            shares_outstanding=None,
            shares_outstanding_as_of=as_of_out,
            shares_outstanding_source=SharesOutstandingSource(None, None, None, None),
            treasury_shares=None,
            treasury_as_of=None,
            slices=(),
            residual=residual,
            concentration=ConcentrationInfo(
                pct_outstanding_known=Decimal(0),
                info_chip=info_chip,
            ),
            coverage=coverage,
            banner=banner,
            historical_symbols=historical_symbols,
            corrections_applied=(),
            denominator_cross_check=DenominatorCrossCheck.unavailable(),
            computed_at=datetime.now(tz=UTC),
            def14a_drift=def14a_drift,
        )


# ---------------------------------------------------------------------------
# Internal helpers: candidate collection + dedup
# ---------------------------------------------------------------------------


@dataclass
class _Candidate:
    """One row in the canonical-holder union before dedup. Mutable so
    the DEF 14A enrichment step can append rows in Python after the
    SQL-side union has run.

    ``ownership_nature`` (#840.E): when reading from the new
    ``ownership_*_current`` tables, candidates carry the
    direct/indirect/beneficial/voting/economic axis explicitly. The
    legacy SQL path leaves this ``None`` (it implicitly only reads
    ``direct`` Form 4s + ``beneficial`` 13D/Gs). Codex pre-push
    review for #840.E caught a cross-nature collapse bug under the
    flag-ON path: identity-key-only dedup folded a holder's
    direct + indirect rows into one and lost the indirect side. The
    dedup identity key now includes ``ownership_nature`` whenever
    it's set."""

    source: SourceTag
    priority_rank: int
    filer_cik: str | None
    filer_name: str
    filer_type: str | None
    shares: Decimal
    as_of_date: date | None
    accession_number: str
    source_row_id: int
    ownership_nature: str | None = None


def edgar_archive_url(accession_number: str | None) -> str | None:
    """Return the SEC EDGAR archive index URL for an accession.

    Format: ``https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dashes}/{accession}-index.htm``

    The filer CIK is derived from the accession's first segment (SEC
    accession format is ``{cik_padded}-{yy}-{seq}``). Returns
    ``None`` for malformed or missing accessions so callers can ship
    the field as a nullable provenance link.
    """
    if not accession_number:
        return None
    parts = accession_number.split("-", 1)
    if len(parts) != 2 or not parts[0]:
        return None
    try:
        cik_int = int(parts[0].lstrip("0") or "0")
    except ValueError:
        return None
    acc_no_dashes = accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_dashes}/{accession_number}-index.htm"


def _identity_key(filer_cik: str | None, filer_name: str) -> str:
    """Cross-source dedup key.

    CIK when present (every modern Form 4 / 13F / 13D / Form 3 row);
    falls back to ``LOWER(TRIM(filer_name))`` for legacy NULL-CIK
    rows so two distinct NULL-CIK filers do not collapse into one
    bucket. Codex review (v3 spec pass) caught the prior over-collapse
    bug.
    """
    if filer_cik is not None and filer_cik.strip():
        return f"CIK:{filer_cik.strip()}"
    return f"NAME:{filer_name.strip().lower()}"


_PRIORITY_RANK: dict[SourceTag, int] = {
    "form4": 1,
    "form3": 2,
    "13d": 3,
    "13g": 3,
    "def14a": 4,
    "13f": 5,
    # N-PORT rows live in the funds memo-overlay slice (#919); they
    # never compete with the pie-wedge sources in cross-source dedup.
    # Lowest priority is defensive — if a row ever leaks into the
    # priority pool, the wedge sources still win.
    "nport": 6,
}

_RESIDUAL_TOOLTIP = (
    "Shares outstanding minus all known regulated filings and "
    "treasury. Includes retail, undeclared institutional, and any "
    "filer outside our coverage cohort."
)


def _collect_canonical_holders_from_current(conn: psycopg.Connection[Any], instrument_id: int) -> list[_Candidate]:
    """Build the canonical-holder candidate set from the per-source
    ``ownership_*_current`` snapshots populated by Phase 1 write-through
    (#888-#891) and primed by ``ownership_observations_backfill`` (#909).

    Maps:
      - ``ownership_insiders_current`` (form4, form3 + nature axis)
        → insiders candidates.
      - ``ownership_blockholders_current`` (13d, 13g, beneficial)
        → blockholders candidates.
      - ``ownership_institutions_current`` (13f, economic, EQUITY only;
        PUT / CALL exposures are option overlays, NOT pie wedges)
        → institutions / etfs candidates (filer_type drives bucket).

    Treasury is read separately via :func:`_read_treasury_from_current`.
    DEF 14A rows are fetched separately via
    :func:`_read_def14a_unmatched_from_current` and injected into
    :func:`_enrich_and_union_def14a` through its ``def14a_rows`` kwarg
    so the matched/unmatched routing stays one code path."""
    rows: list[_Candidate] = []
    next_row_id = iter(range(1, 1_000_000))

    # Insiders.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Dual-pipeline de-collision (#788): the same Form 4/3 accession is
        # written by BOTH the XML manifest parser (plain ``source_document_id`` =
        # the bare accession, nature from Table II Direct/Indirect) AND the bulk
        # SEC insider dataset (``<accn>:NDT:<sk>`` for Form-4 transactions /
        # ``<accn>:NDH:<sk>`` for Form-3 holdings, nature relabelled from the
        # reporting person's relationship — ``_map_relationship``). Since
        # ``ownership_nature`` is in the MERGE key the two coexist, so the same
        # stake is counted under two natures (direct+beneficial, direct+indirect).
        # Drop the dataset row (its id carries a ``:NDT:`` / ``:NDH:`` marker)
        # whenever an XML-manifest row exists for the same ``(holder_cik,
        # source_accession)`` — the manifest parse is the authoritative
        # full-Table-II view of that filing; the dataset only fills accessions the
        # manifest never parsed.
        cur.execute(
            """
            SELECT holder_cik, holder_name, ownership_nature,
                   source, source_accession, shares, period_end
            FROM ownership_insiders_current oc
            WHERE instrument_id = %s
              AND shares IS NOT NULL
              AND NOT (
                oc.source_document_id ~ ':(NDT|NDH):'
                AND EXISTS (
                    SELECT 1 FROM ownership_insiders_current x
                    WHERE x.instrument_id = oc.instrument_id
                      AND x.holder_cik IS NOT DISTINCT FROM oc.holder_cik
                      AND x.source_accession = oc.source_accession
                      AND x.source_document_id !~ ':(NDT|NDH):'
                )
              )
            """,
            (instrument_id,),
        )
        for row in cur.fetchall():
            source = str(row["source"])
            if source not in ("form4", "form3"):
                continue
            rows.append(
                _Candidate(
                    source=source,  # type: ignore[arg-type]
                    priority_rank=_PRIORITY_RANK[source],  # type: ignore[index]
                    filer_cik=str(row["holder_cik"]) if row["holder_cik"] else None,
                    filer_name=str(row["holder_name"]),
                    filer_type=None,
                    shares=Decimal(row["shares"]),
                    as_of_date=row.get("period_end"),  # type: ignore[arg-type]
                    accession_number=str(row.get("source_accession") or ""),
                    source_row_id=next(next_row_id),
                    ownership_nature=str(row["ownership_nature"]),
                )
            )

    # Blockholders (13D/G).
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT reporter_cik, reporter_name, ownership_nature,
                   source, source_accession, aggregate_amount_owned, period_end
            FROM ownership_blockholders_current
            WHERE instrument_id = %s
              AND aggregate_amount_owned IS NOT NULL
            """,
            (instrument_id,),
        )
        for row in cur.fetchall():
            source = str(row["source"])
            if source not in ("13d", "13g"):
                continue
            rows.append(
                _Candidate(
                    source=source,  # type: ignore[arg-type]
                    priority_rank=_PRIORITY_RANK[source],  # type: ignore[index]
                    filer_cik=str(row["reporter_cik"]) if row["reporter_cik"] else None,
                    filer_name=str(row["reporter_name"]),
                    filer_type=None,
                    shares=Decimal(row["aggregate_amount_owned"]),
                    as_of_date=row.get("period_end"),  # type: ignore[arg-type]
                    accession_number=str(row.get("source_accession") or ""),
                    source_row_id=next(next_row_id),
                    ownership_nature=str(row["ownership_nature"]),
                )
            )

    # Institutions (13F-HR equity only — PUT / CALL exposures are
    # option overlays, not pie wedges; matches the legacy SQL's
    # ``is_put_call IS NULL`` filter).
    #
    # 13F-NT supersession (#1639): exclude a filer's HR when that filer filed a
    # 13F-NT for a LATER quarter. A Notice declares the filer holds nothing
    # reportable — its book is reported by other managers (post-reorg sub-entity
    # CIKs) — so its stale HR is dead. The predicate is on ``period_end`` (NOT
    # filed_at): an NT/A amending an old quarter can be filed after a resumed HR,
    # so file-time is the wrong axis. ``period_end`` is NOT NULL on
    # ``_current``, so the strict ``>`` is always well-defined. The companion
    # :func:`_read_notice_suppressions` lists exactly the rows this excludes for
    # the ``corrections_applied`` telemetry.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT c.filer_cik, c.filer_name, c.filer_type, c.ownership_nature,
                   c.source, c.source_accession, c.shares, c.period_end
            FROM ownership_institutions_current c
            WHERE c.instrument_id = %s
              AND c.shares IS NOT NULL
              AND c.exposure_kind = 'EQUITY'
              AND NOT EXISTS (
                    SELECT 1 FROM institutional_filer_13f_notices n
                    WHERE n.filer_cik  = c.filer_cik
                      AND n.period_end > c.period_end
              )
            """,
            (instrument_id,),
        )
        for row in cur.fetchall():
            rows.append(
                _Candidate(
                    source="13f",
                    priority_rank=_PRIORITY_RANK["13f"],
                    filer_cik=str(row["filer_cik"]) if row["filer_cik"] else None,
                    filer_name=str(row["filer_name"]),
                    filer_type=(str(row["filer_type"]) if row["filer_type"] else None),
                    shares=Decimal(row["shares"]),
                    as_of_date=row.get("period_end"),  # type: ignore[arg-type]
                    accession_number=str(row.get("source_accession") or ""),
                    source_row_id=next(next_row_id),
                    ownership_nature=str(row["ownership_nature"]),
                )
            )

    return rows


def _read_notice_suppressions(conn: psycopg.Connection[Any], instrument_id: int) -> tuple[CorrectionApplied, ...]:
    """List the institution rows EXCLUDED by 13F-NT supersession (#1639), with
    the winning Notice, for the ``corrections_applied`` telemetry.

    The selection is the exact complement of the rollup institutions query's
    ``NOT EXISTS`` clause — these are the rows that filter removed. The lateral
    join picks the LATEST superseding Notice (``ORDER BY period_end DESC`` —
    never a bare ``LIMIT 1``, per the deterministic-pick rule). Without this an
    operator would see the institutions wedge shrink and the residual grow with
    no visible cause."""
    rows: list[CorrectionApplied] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT c.filer_cik, c.filer_name, c.shares,
                   c.period_end AS superseded_period,
                   n.period_end AS winning_nt_period,
                   n.accession_number AS winning_nt_accession
            FROM ownership_institutions_current c
            JOIN LATERAL (
                SELECT period_end, accession_number
                FROM institutional_filer_13f_notices nt
                WHERE nt.filer_cik = c.filer_cik
                  AND nt.period_end > c.period_end
                ORDER BY nt.period_end DESC, nt.accession_number DESC
                LIMIT 1
            ) n ON TRUE
            WHERE c.instrument_id = %s
              AND c.shares IS NOT NULL
              AND c.exposure_kind = 'EQUITY'
            ORDER BY c.shares DESC, c.filer_cik
            """,
            (instrument_id,),
        )
        for row in cur.fetchall():
            rows.append(
                CorrectionApplied(
                    kind="suppressed_by_13f_nt",
                    filer_cik=str(row["filer_cik"]),
                    filer_name=str(row["filer_name"]),
                    shares_removed=Decimal(row["shares"]),
                    superseded_period=row["superseded_period"],
                    winning_nt_period=row["winning_nt_period"],
                    winning_nt_accession=str(row["winning_nt_accession"]),
                    source_channel="13f",  # the folded channel (#1647 generic provenance)
                )
            )
    return tuple(rows)


def _collect_funds_from_current(conn: psycopg.Connection[Any], instrument_id: int) -> list[Holder]:
    """Build the funds-slice holder set from ``ownership_funds_current``.

    Each row in ``ownership_funds_current`` is one (fund_series, instrument)
    position — already deduped by the table's ``(instrument_id, fund_series_id)``
    PRIMARY KEY (the refresh function picks the latest filing per series).
    No cross-source dedup needed: N-PORT is the only source and the funds
    slice is a memo overlay (``denominator_basis="institution_subset"``)
    that doesn't compete with the pie-wedge slices.

    Holders surface ``filer_name = fund_series_name`` (the operator-visible
    fund identity, e.g. "Vanguard 500 Index Fund") rather than
    ``fund_filer_cik`` / ``fund_filer_name`` (the trust/manager) — per
    the #919 acceptance "Fidelity Contrafund's AAPL position renders
    separately from Fidelity's 13F-HR aggregate".

    Funds always carry ``winning_source='nport'`` and ``filer_type=None``
    (filer-type is a 13F-HR notion; N-PORT's series-level identity has
    no analogous discriminator). ``pct_outstanding`` is filled in by
    :func:`_build_slice` once the denominator is known.
    """
    holders: list[Holder] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT fund_series_id, fund_series_name, fund_filer_cik,
                   shares, source_accession, period_end
            FROM ownership_funds_current
            WHERE instrument_id = %s
              AND shares IS NOT NULL
              AND shares > 0
            """,
            (instrument_id,),
        )
        for row in cur.fetchall():
            accession = str(row.get("source_accession") or "")
            holders.append(
                Holder(
                    filer_cik=str(row["fund_filer_cik"]) if row.get("fund_filer_cik") else None,
                    filer_name=str(row["fund_series_name"]),
                    shares=Decimal(row["shares"]),
                    pct_outstanding=Decimal(0),  # filled by _build_slice
                    winning_source="nport",
                    winning_accession=accession,
                    winning_edgar_url=edgar_archive_url(accession),
                    as_of_date=row.get("period_end"),  # type: ignore[arg-type]
                    filer_type=None,
                    dropped_sources=(),
                )
            )
    return holders


def _collect_esop_from_current(conn: psycopg.Connection[Any], instrument_id: int) -> list[Holder]:
    """Build the esop-slice holder set from ``ownership_esop_current`` (#961).

    Each row is one (instrument, plan_name) DEF 14A-disclosed ESOP /
    employee-benefit-plan holding — PK-deduped at the table level
    (``ownership_esop_current`` PK is ``(instrument_id, plan_name)``),
    same shape as :func:`_collect_funds_from_current`.

    #843's spec (`docs/proposals/etl/def14a-bene-table-extension.md`)
    originally called for tagging matching ``ownership_funds_current``
    rows via ``plan_trustee_cik = fund_filer_cik``. Full-population
    check (2026-07-03, all 15 populated rows) found ``plan_trustee_cik``
    is NULL on every row — the DEF 14A beneficial-ownership table gives
    free-text trustee names ("Kearny Bank ESOP Trust c/o Pentegra
    Services, Inc."), never a resolvable CIK, so that join can never
    match. Rendering ESOP as its own memo-overlay slice (mirroring
    funds/def14a_unmatched) surfaces the same already-validated data
    without depending on a join key that doesn't exist.

    Holders surface ``filer_name = plan_name`` (the operator-visible
    plan identity) with ``filer_cik=None`` (no resolvable trustee CIK)
    and ``winning_source='def14a'`` (the schema's fixed source value).
    ``pct_outstanding`` is filled in by :func:`_build_slice`.
    """
    holders: list[Holder] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT plan_name, shares, source_accession, period_end
            FROM ownership_esop_current
            WHERE instrument_id = %s
              AND shares IS NOT NULL
              AND shares > 0
            """,
            (instrument_id,),
        )
        for row in cur.fetchall():
            accession = str(row.get("source_accession") or "")
            holders.append(
                Holder(
                    filer_cik=None,
                    filer_name=str(row["plan_name"]),
                    shares=Decimal(row["shares"]),
                    pct_outstanding=Decimal(0),  # filled by _build_slice
                    winning_source="def14a",
                    winning_accession=accession,
                    winning_edgar_url=edgar_archive_url(accession),
                    as_of_date=row.get("period_end"),  # type: ignore[arg-type]
                    filer_type=None,
                    dropped_sources=(),
                )
            )
    return holders


def _read_treasury_from_current(
    conn: psycopg.Connection[Any], instrument_id: int
) -> tuple[Decimal | None, date | None]:
    """Read latest treasury from ``ownership_treasury_current`` instead
    of walking ``financial_periods``. Used when the rollup
    feature-flag selects the new read path (#840.E).

    ``ownership_treasury_current`` PK is ``(instrument_id)`` so there
    is at most one row per instrument by construction. The explicit
    ``ORDER BY period_end DESC LIMIT 1`` is defence in depth — bot
    review for #840.E PR #861 caught the prior version trusting
    ``fetchone()`` without an ORDER BY clause; that path would have
    returned an arbitrary row if the PK was ever weakened in a future
    migration."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT treasury_shares, period_end
            FROM ownership_treasury_current
            WHERE instrument_id = %s
              AND treasury_shares IS NOT NULL
            ORDER BY period_end DESC
            LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None, None
    return Decimal(row["treasury_shares"]), row.get("period_end")  # type: ignore[arg-type]


def _read_def14a_unmatched_from_current(conn: psycopg.Connection[Any], instrument_id: int) -> list[dict[str, Any]]:
    """Return DEF 14A holdings from ``ownership_def14a_current`` in
    the same dict shape as the legacy ``def14a_beneficial_holdings``
    SELECT — so the existing ``_enrich_and_union_def14a`` enrichment
    can run against either source unchanged."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Bot review for #840.E PR #861: ``row_number() OVER ()`` with
        # no ORDER BY inside the window frame is non-deterministic
        # across executions. ``holder_id`` here is a synthetic id that
        # ``_enrich_and_union_def14a`` carries through to the
        # ``_Candidate.source_row_id`` field and the dedup tie-breaker
        # touches it on equal-priority/equal-date pairs. Pin the
        # ordering to ``holder_name_key`` (deterministic identity) so
        # the synthetic id is stable across runs.
        cur.execute(
            """
            SELECT
                row_number() OVER (ORDER BY holder_name_key, ownership_nature) AS holding_id,
                holder_name,
                ownership_nature,
                shares,
                period_end AS as_of_date,
                source_accession AS accession_number
            FROM ownership_def14a_current
            WHERE instrument_id = %s
              AND shares IS NOT NULL
            """,
            (instrument_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def _read_def14a_drift(conn: psycopg.Connection[Any], instrument_id: int) -> Def14ADriftInfo | None:
    """Summarise this instrument's warning/critical drift alerts for the
    chip (#966). Read-only over ``def14a_drift_alerts`` (the detector
    runs at the write loci, never here). ``info`` severity excluded —
    unmatched-name noise already renders as the ``def14a_unmatched``
    slice. Returns None when no warning/critical alerts exist.

    Uses the existing ``(instrument_id, detected_at)`` index for the
    instrument scope; the residual severity filter walks that
    instrument's handful of alert rows.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT holder_name, severity, drift_pct
            FROM def14a_drift_alerts
            WHERE instrument_id = %(iid)s
              AND severity IN ('warning', 'critical')
            ORDER BY drift_pct DESC NULLS LAST, holder_name
            """,
            {"iid": instrument_id},
        )
        rows = cur.fetchall()
    if not rows:
        return None
    worst: Literal["warning", "critical"] = "critical" if any(r["severity"] == "critical" for r in rows) else "warning"
    n = len(rows)
    holders_phrase = f"{n} holder diverges" if n == 1 else f"{n} holders diverge"
    chip = (
        f"Proxy vs insider-stream drift: {holders_phrase} ≥5% between the "
        f"DEF 14A table and Form 4 cumulative positions — possible missed "
        f"or unreported insider filings."
    )
    return Def14ADriftInfo(
        worst_severity=worst,
        alert_count=n,
        chip=chip,
        holders=tuple(str(r["holder_name"]) for r in rows[:3]),
    )


def _enrich_and_union_def14a(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    sql_candidates: list[_Candidate],
    *,
    def14a_rows: list[dict[str, Any]],
) -> tuple[list[_Candidate], list[_Candidate]]:
    """Resolve each DEF 14A holder to a filer_cik and union into the
    candidate set. Returns ``(matched_candidates, unmatched_candidates)``.

    Under the two-axis model (#840 P1 + #905 cutover) DEF 14A
    candidates carry ``ownership_nature='beneficial'`` and live in
    their own dedup group separate from same-CIK Form 4 (direct) /
    Form 3 (direct) rows. Matched DEF 14A rows therefore surface as
    independent holders in the insiders slice rather than collapsing
    into a Form 4 ``dropped_source``.

    Unmatched DEF 14A rows go directly to the ``def14a_unmatched``
    slice keyed on the holder name — no CIK is available to dedup
    against. These are mostly named officers in the proxy who never
    filed a Form 4 / Form 3.

    ``def14a_rows`` shape: ``{holding_id, holder_name, ownership_nature,
    shares, as_of_date, accession_number}`` — supplied by
    :func:`_read_def14a_unmatched_from_current`. Codex pre-push review
    for #905 caught the prior shape that omitted ``ownership_nature``:
    ``ownership_def14a_current`` PK is
    ``(instrument_id, holder_name_key, ownership_nature)`` so a single
    holder can carry both a beneficial row and a voting row. Without
    ``ownership_nature`` on the candidate, those rows would collapse
    into one in :func:`_dedup_by_priority`.
    """
    matched: list[_Candidate] = list(sql_candidates)
    unmatched: list[_Candidate] = []
    for row in def14a_rows:
        is_matched, cik, _known_shares = resolve_holder_to_filer(
            conn,
            instrument_id=instrument_id,
            holder_name=str(row["holder_name"]),  # type: ignore[arg-type]
        )
        candidate = _Candidate(
            source="def14a",
            priority_rank=_PRIORITY_RANK["def14a"],
            filer_cik=cik,
            filer_name=str(row["holder_name"]),  # type: ignore[arg-type]
            filer_type=None,
            shares=Decimal(row["shares"]),  # type: ignore[arg-type]
            as_of_date=row.get("as_of_date"),  # type: ignore[arg-type]
            accession_number=str(row["accession_number"]),  # type: ignore[arg-type]
            source_row_id=int(row["holding_id"]),  # type: ignore[arg-type]
            ownership_nature=str(row["ownership_nature"]) if row.get("ownership_nature") else None,
        )
        # Use the resolver's ``matched`` flag, not just ``cik is not
        # None``. The resolver returns ``matched=True, cik=None`` for a
        # legacy NULL-CIK Form 4 row that name-matches the holder; that
        # is a clean reconciliation, not a coverage gap. Routing it to
        # ``unmatched`` would have lost the holder twice — once from
        # the ``insiders`` slice (DEF 14A loses priority to the legacy
        # Form 4 it matched) and once again on the chart because the
        # resolver couldn't pin a CIK. Codex pre-push review (Batch 1
        # of #788) caught this.
        if is_matched:
            matched.append(candidate)
        else:
            unmatched.append(candidate)
    return matched, unmatched


def _dedup_by_priority(candidates: Iterable[_Candidate]) -> list[Holder]:
    """Group candidates by identity key, pick the highest-priority
    survivor per group, ship the losers as ``dropped_sources``.

    Tie-break sequence — applied via a chain of stable sorts (last
    sort = most significant):

      1. ``priority_rank`` ascending (lower wins: form4=1 beats 13f=5)
      2. ``as_of_date`` descending (newest wins; NULL last)
      3. ``accession_number`` descending (lex-larger = later in the
         filer-year sequence wins)
      4. ``source_row_id`` descending (final pin against ties)

    Matches the pinned spec sequence Codex reviewed.
    """
    # Codex pre-push review for #840.E: include ``ownership_nature``
    # in the dedup identity key. Without this, a holder's direct +
    # indirect rows from ``ownership_*_current`` collapse into one
    # and the second nature is silently dropped.
    groups: dict[str, list[_Candidate]] = {}
    for c in candidates:
        base_key = _identity_key(c.filer_cik, c.filer_name)
        key = f"{base_key}|{c.ownership_nature}" if c.ownership_nature else base_key
        groups.setdefault(key, []).append(c)

    survivors: list[Holder] = []
    for cands in groups.values():
        # Stable sorts applied bottom-up so the final ordering is
        # priority_rank → as_of_date desc → accession desc → row_id desc.
        cands.sort(key=lambda c: c.source_row_id, reverse=True)
        cands.sort(key=lambda c: c.accession_number, reverse=True)
        cands.sort(key=lambda c: c.as_of_date or date.min, reverse=True)
        cands.sort(key=lambda c: c.priority_rank)
        winner = cands[0]
        losers = cands[1:]
        survivors.append(
            Holder(
                filer_cik=winner.filer_cik,
                filer_name=winner.filer_name,
                shares=winner.shares,
                pct_outstanding=Decimal(0),  # filled by _build_slice once denom known
                winning_source=winner.source,
                winning_accession=winner.accession_number,
                winning_edgar_url=edgar_archive_url(winner.accession_number),
                as_of_date=winner.as_of_date,
                filer_type=winner.filer_type,
                ownership_nature=winner.ownership_nature,
                dropped_sources=tuple(
                    DroppedSource(
                        source=loser.source,
                        accession_number=loser.accession_number,
                        shares=loser.shares,
                        as_of_date=loser.as_of_date,
                        edgar_url=edgar_archive_url(loser.accession_number),
                    )
                    for loser in losers
                ),
            )
        )
    return survivors


# ---------------------------------------------------------------------------
# Slice + residual + coverage assembly
# ---------------------------------------------------------------------------


_SLICE_LABELS: dict[SliceCategory, str] = {
    "insiders": "Insiders",
    "blockholders": "Blockholders",
    "institutions": "Institutions",
    "etfs": "ETFs",
    "def14a_unmatched": "Proxy-only (DEF 14A)",
    "funds": "Mutual funds (N-PORT)",
    "esop": "Employee benefit plans (ESOP)",
}


def _dedup_within_source(candidates: Iterable[_Candidate]) -> list[Holder]:
    """Per-CIK (or per-name fallback) dedup *within* a single source —
    used by the blockholders pipeline post-#837 so 13D/G filings are
    not eliminated by the cross-source ``form4 > 13d/g`` priority chain.

    Same identity-key + tie-break sequence as
    :func:`_dedup_by_priority` but no cross-source priority race —
    every input row is the same source, so the winner is just the
    latest amendment for that CIK / name. Cohen's 13D/A latest
    amendment wins over his 13D original; both Cohen Form 4 (direct)
    and Cohen 13D/A (beneficial) survive elsewhere because they no
    longer compete in the same dedup pool."""
    # Codex pre-push review for #840.E: include ``ownership_nature``
    # in the dedup identity key whenever it's set (always under the
    # flag-ON path, never under legacy). Without this, a holder's
    # direct + indirect rows from ``ownership_*_current`` collapse
    # into one and the second nature is silently dropped.
    groups: dict[str, list[_Candidate]] = {}
    for c in candidates:
        base_key = _identity_key(c.filer_cik, c.filer_name)
        key = f"{base_key}|{c.ownership_nature}" if c.ownership_nature else base_key
        groups.setdefault(key, []).append(c)

    survivors: list[Holder] = []
    for cands in groups.values():
        cands.sort(key=lambda c: c.source_row_id, reverse=True)
        cands.sort(key=lambda c: c.accession_number, reverse=True)
        cands.sort(key=lambda c: c.as_of_date or date.min, reverse=True)
        # Same-source: priority_rank ties (13d == 13g == 3); the prior
        # sorts decide.
        winner = cands[0]
        losers = cands[1:]
        survivors.append(
            Holder(
                filer_cik=winner.filer_cik,
                filer_name=winner.filer_name,
                shares=winner.shares,
                pct_outstanding=Decimal(0),
                winning_source=winner.source,  # type: ignore[arg-type]
                winning_accession=winner.accession_number,
                winning_edgar_url=edgar_archive_url(winner.accession_number),
                as_of_date=winner.as_of_date,
                filer_type=winner.filer_type,
                ownership_nature=winner.ownership_nature,
                dropped_sources=tuple(
                    DroppedSource(
                        source=loser.source,  # type: ignore[arg-type]
                        accession_number=loser.accession_number,
                        shares=loser.shares,
                        as_of_date=loser.as_of_date,
                        edgar_url=edgar_archive_url(loser.accession_number),
                    )
                    for loser in losers
                ),
            )
        )
    return survivors


# ---------------------------------------------------------------------------
# Owner-identity reconciliation — one beneficial owner, counted once (#1640)
# See docs/specs/etl/2026-06-15-ownership-owner-once-dedup.md
# ---------------------------------------------------------------------------

# Beneficial-restatement sources: each is an estimate of the SAME total
# beneficial stake (Rule 13d-3). DEF 14A beneficial ≈ Form-4 Section-16 total
# ≈ 13D/G beneficial — overlapping, so a single owner's contribution is the
# MAX across them (not the sum).
_BENEFICIAL_SOURCES: Final[frozenset[SourceTag]] = frozenset({"form4", "form3", "def14a", "13d", "13g"})
# Section-16 / management sources: presence of any one makes the owner an
# insider regardless of any other channel.
_INSIDER_SOURCES: Final[frozenset[SourceTag]] = frozenset({"form4", "form3", "def14a"})
# Sources whose per-owner nature rows ADD (Form 4 / Form 3 ``direct`` +
# ``indirect`` are distinct Section-16 holdings — the #905 JPM rule). Every
# other source's nature rows OVERLAP (DEF 14A ``beneficial`` vs ``voting``, a
# single 13D/G beneficial figure, a single 13F economic position) and so are
# collapsed to their largest representative, never summed (Codex #1640 ckpt-2).
_ADDITIVE_SOURCES: Final[frozenset[SourceTag]] = frozenset({"form4", "form3"})
# Within an additive source, only these natures are the distinct Section-16
# holdings that SUM (#905). A ``beneficial`` / ``voting`` / ``economic`` row is an
# OVERLAPPING restatement of the owner's total (Rule 13d-3, prevention-log 1835)
# and must MAX against the additive sum, never add to it. The bulk SEC insider
# dataset (`sec_insider_dataset_ingest._map_relationship`) relabels a 10%-owner's
# directly-held lot ``beneficial`` purely from the relationship flag, so the same
# Form-4 position lands under BOTH a ``direct``/``indirect`` (XML) and a
# ``beneficial`` (dataset) row; summing them double-counts one stake.
_ADDITIVE_NATURES: Final[frozenset[str]] = frozenset({"direct", "indirect"})


def _argmax_source(sources: Iterable[SourceTag], src_total: dict[SourceTag, Decimal]) -> SourceTag:
    """Source carrying the owner's largest total; tie-break to the
    higher-priority (lower ``_PRIORITY_RANK``) source so form4 beats 13d
    on equal shares."""
    return max(sources, key=lambda s: (src_total[s], -_PRIORITY_RANK[s]))


def _source_rows_and_total(source: SourceTag, rows: list[Holder]) -> tuple[list[Holder], Decimal]:
    """The display rows + owner subtotal for one ``(owner, source)``.

    Additive sources (Form 4 / Form 3): the ``direct`` + ``indirect`` nature rows
    are distinct Section-16 holdings and SUM (#905); a ``beneficial`` (or
    ``voting`` / ``economic``) row is an OVERLAPPING restatement of the owner's
    total (Rule 13d-3, prevention-log 1835) and MAXes against that sum rather than
    adding to it — the within-source half of the regime the earlier source-level
    code missed (the dual-pipeline ``direct`` vs dataset-``beneficial`` collision,
    #788). Owner subtotal = ``MAX(additive_sum, overlap_max)``; this is monotone —
    it can only remove or hold counted shares vs the old blanket sum, never add
    (verified on the full dev population, all 1,206 same-accession both-nature
    groups). Overlapping sources (DEF 14A beneficial/voting, 13D/G, 13F): keep
    only the largest row — the others restate the same shares through a different
    lens (Codex #1640 ckpt-2 F3)."""
    if source not in _ADDITIVE_SOURCES:
        rep = max(rows, key=lambda h: h.shares)
        return [rep], rep.shares

    additive = [h for h in rows if (h.ownership_nature or "direct") in _ADDITIVE_NATURES]
    overlap = [h for h in rows if (h.ownership_nature or "direct") not in _ADDITIVE_NATURES]
    additive_sum = sum((h.shares for h in additive), Decimal(0))
    overlap_max = max((h.shares for h in overlap), default=Decimal(0))

    if additive and additive_sum >= overlap_max:
        # Additive lots dominate: keep them, fold the subsumed overlap
        # restatement(s) into the largest additive row's provenance.
        keep = list(additive)
        if overlap:
            primary_idx = max(range(len(keep)), key=lambda i: keep[i].shares)
            keep[primary_idx] = _fold_overlap_into(keep[primary_idx], overlap)
        return keep, additive_sum

    # Overlap exceeds the additive sum (XML under-captured the position) or no
    # additive row exists (dataset-only owner): the beneficial restatement is the
    # better total. Keep its largest row, fold the additive lots as provenance.
    rep = max(overlap, key=lambda h: h.shares)
    if additive:
        rep = _fold_overlap_into(rep, additive)
    return [rep], rep.shares


def _fold_overlap_into(rep: Holder, folded: list[Holder]) -> Holder:
    """Append ``folded`` rows to ``rep``'s ``dropped_sources`` (provenance only),
    de-duped on ``(source, accession, shares)`` against what is already recorded —
    so the subsumed same-source restatement (e.g. the dataset ``:NDT:`` beneficial
    row) stays auditable instead of vanishing. ``shares`` is in the key so two
    distinct lots on ONE accession (a folded ``direct`` + ``indirect`` when the
    overlap row wins) are BOTH preserved, not collapsed to one (Codex ckpt-2)."""
    dropped = list(rep.dropped_sources)
    seen = {(d.source, d.accession_number, d.shares) for d in dropped}
    for h in folded:
        key = (h.winning_source, h.winning_accession, h.shares)
        if key in seen:
            continue
        seen.add(key)
        dropped.append(
            DroppedSource(
                source=h.winning_source,
                accession_number=h.winning_accession,
                shares=h.shares,
                as_of_date=h.as_of_date,
                edgar_url=h.winning_edgar_url,
            )
        )
    return replace(rep, dropped_sources=tuple(dropped))


def _section16_row_supersedes(new: Holder, cur: Holder) -> bool:
    """True when ``new`` should replace ``cur`` as the latest row for a *repeated*
    Section-16 additive nature: newer ``as_of_date`` wins (NULL last), tie-broken
    to the higher-priority form (Form 4 transaction over Form 3 snapshot). Only
    exercised in a degenerate identity merge — the ``ownership_insiders_current``
    PK ``(instrument, holder, nature)`` gives one form per nature on the real
    population — so this is a defensive dedup, never the common path."""
    new_key = (new.as_of_date or date.min, -_PRIORITY_RANK[new.winning_source])
    cur_key = (cur.as_of_date or date.min, -_PRIORITY_RANK[cur.winning_source])
    return new_key > cur_key


def _merge_section16_forms(by_source: dict[SourceTag, list[Holder]]) -> None:
    """Pool an owner's Form 3 + Form 4 rows into ONE Section-16 additive channel.

    Form 3 (initial-holdings snapshot) and Form 4 (subsequent transactions) carry
    DISTINCT additive natures for one owner — the latest ``direct`` may sit on a
    Form 4 while the latest ``indirect`` sits on a Form 3, because each nature
    keeps its own latest observation in ``ownership_insiders_current`` (PK
    ``(instrument, holder, nature)``, so ``source`` is a per-nature provenance
    stamp). Those are distinct Section-16 holdings that SUM (#905, prevention-log
    "Same owner across reporting channels"), NOT overlapping restatements. The
    prior read path kept form4/form3 as separate ``by_source`` buckets, so
    :func:`_reconcile_owner_once` MAXed the two form subtotals and dropped the
    smaller-form lot (#1941: AAPL / Khan Sabih — ``direct`` 1,073,895 on Form 4 +
    ``indirect`` 31,632 on Form 3, the indirect silently folded to
    ``dropped_sources`` instead of summed).

    Collapse both forms' rows under one representative key (``form4`` if present,
    else ``form3``) so the shared :func:`_source_rows_and_total` additive path SUMs
    them. Keep only the latest row per additive nature across the two forms (Form 4
    supersedes the Form 3 snapshot for a repeated nature — a defensive dedup; the
    real population has exactly one form per nature). Overlap ``beneficial`` rows
    from either form are retained for the within-channel MAX. Rows keep their own
    ``winning_source`` so provenance / EDGAR links stay per-filing accurate; the
    dict key is only a grouping label."""
    forms: list[SourceTag] = [s for s in ("form4", "form3") if s in by_source]
    if len(forms) < 2:
        return
    pooled = [h for s in forms for h in by_source[s]]
    latest_additive: dict[str, Holder] = {}
    overlap: list[Holder] = []
    for h in pooled:
        nature = h.ownership_nature or "direct"
        if nature not in _ADDITIVE_NATURES:
            overlap.append(h)
            continue
        cur = latest_additive.get(nature)
        if cur is None or _section16_row_supersedes(h, cur):
            latest_additive[nature] = h
    rep = forms[0]
    for s in forms:
        del by_source[s]
    by_source[rep] = list(latest_additive.values()) + overlap


def _reconcile_owner_once(holders: list[Holder]) -> dict[SliceCategory, list[Holder]]:
    """Collapse each beneficial owner to a single pie-wedge contribution.

    Input = the combined pie-wedge ``Holder`` set (insiders / institutions /
    etfs survivors + blockholders), already deduped per-(cik, nature) and
    per-source by the upstream passes. Output = per-category holder lists,
    each owner appearing in exactly one category.

    Rule (#1640, SEC proxy "beneficial owners and management" semantics):
    one owner (identity = CIK, name fallback), counted once, at their total
    beneficial ownership, classified by most-specific role. Beneficial-
    restatement sources (Form 4 / 3 / DEF 14A / 13D / 13G) are overlapping
    estimates of one stake → MAX; 13F is managed/economic exposure (often
    clients' assets), a different concept that never inflates an insider's
    figure. The losing sources become one ``dropped_sources`` entry each (at
    that source's owner subtotal) on the owner's largest surviving row.

    Note: a dropped source's own upstream ``dropped_sources`` (superseded
    amendments collapsed by :func:`_dedup_by_priority` /
    :func:`_dedup_within_source`) are not re-surfaced — they were never a
    top-level holder either, and the per-source subtotal is the channel's
    authoritative figure."""
    groups: dict[str, list[Holder]] = {}
    for h in holders:
        groups.setdefault(_identity_key(h.filer_cik, h.filer_name), []).append(h)

    by_category: dict[SliceCategory, list[Holder]] = {
        "insiders": [],
        "blockholders": [],
        "institutions": [],
        "etfs": [],
    }
    for group in groups.values():
        by_source: dict[SourceTag, list[Holder]] = {}
        for h in group:
            by_source.setdefault(h.winning_source, []).append(h)
        # Form 3 + Form 4 are ONE Section-16 additive channel, not two competing
        # restatements — pool them before the per-source subtotal so a ``direct``
        # on Form 4 and an ``indirect`` on Form 3 SUM instead of the smaller form
        # losing the cross-source MAX (#1941).
        _merge_section16_forms(by_source)
        # Per-source display rows + subtotal (additive vs overlapping natures).
        src_rows: dict[SourceTag, list[Holder]] = {}
        src_total: dict[SourceTag, Decimal] = {}
        for s, rows in by_source.items():
            src_rows[s], src_total[s] = _source_rows_and_total(s, rows)

        present = set(by_source)
        bene_sources: list[SourceTag] = [s for s in present if s in _BENEFICIAL_SOURCES]
        bene_max_source = _argmax_source(bene_sources, src_total) if bene_sources else None

        if present & _INSIDER_SOURCES:
            # Section-16 person: total beneficial = MAX of the beneficial
            # restatements. Any 13F for this CIK is managed assets → it stays
            # a dropped_source, never added to the insider's stake.
            category: SliceCategory = "insiders"
            figure_src = bene_max_source
        elif "13f" in present:
            biggest_13f = max(by_source["13f"], key=lambda h: h.shares)
            category = "etfs" if (biggest_13f.filer_type or "").upper() == "ETF" else "institutions"
            # A 13F manager that also filed a passive 13G/D reports the same
            # book through two lenses → count once at the larger.
            if bene_max_source is None or src_total["13f"] >= src_total[bene_max_source]:
                figure_src = "13f"
            else:
                figure_src = bene_max_source
        else:  # only 13d / 13g
            category = "blockholders"
            figure_src = bene_max_source

        if figure_src is None:
            # Unreachable for known sources (every pie-wedge holder is in
            # BENEFICIAL ∪ {13f}). Explicit raise, not assert, so a future
            # SourceTag can't silently None-deref src_rows under ``python -O``
            # (python-hygiene.md: never assert a production invariant).
            raise ValueError(f"no figure source for owner group: sources={present!r}")
        keep = list(src_rows[figure_src])
        losing_sources: list[SourceTag] = [s for s in present if s != figure_src]
        if losing_sources:
            primary_idx = max(range(len(keep)), key=lambda i: keep[i].shares)
            dropped = list(keep[primary_idx].dropped_sources)
            seen = {(d.source, d.accession_number) for d in dropped}
            for s in losing_sources:
                rep = max(by_source[s], key=lambda h: h.shares)  # link target for the channel
                # Stamp the dropped entry with the rep row's OWN source, not the
                # bucket key: after ``_merge_section16_forms`` folds Form 3 rows
                # under the ``form4`` key, ``s`` no longer names the rep's filing —
                # using it would emit a form4 label on a form3 accession/URL. For
                # every un-merged bucket ``rep.winning_source == s`` so this is a
                # no-op there.
                rep_source = rep.winning_source
                key = (rep_source, rep.winning_accession)
                if key in seen:
                    continue
                seen.add(key)
                dropped.append(
                    DroppedSource(
                        source=rep_source,
                        accession_number=rep.winning_accession,
                        shares=src_total[s],  # the channel's owner subtotal, not one row
                        as_of_date=rep.as_of_date,
                        edgar_url=rep.winning_edgar_url,
                    )
                )
            keep[primary_idx] = replace(keep[primary_idx], dropped_sources=tuple(dropped))
        by_category[category].extend(keep)

    return by_category


# ---------------------------------------------------------------------------
# Institutional family identity — count each manager family once (#1644 + #1649)
# See docs/specs/etl/2026-06-15-institutional-family-identity.md
# ---------------------------------------------------------------------------

# Channel tie-break when two channels report the same family figure: prefer the
# current 13F-HR, then the 13G/D consolidated, then the proxy (lower = preferred).
_CHANNEL_TIEBREAK: Final[dict[str, int]] = {"13f": 0, "13g": 1, "def14a": 2}


def _value_is_sane(shares: Decimal, outstanding: Decimal) -> bool:
    """A single beneficial owner cannot hold more than 100% of shares
    outstanding. Rejects DEF 14A parser-garbage values (#1644: LAMR's
    48-trillion-share row, GEF director-group rows) before they can win a
    family MAX or inflate the additive proxy wedge and detonate the rollup.

    Denominator-aware: compares against the same ``shares_outstanding`` the rest
    of the rollup trusts (post the ``_denominator_too_stale`` gate). The known
    soft spot is dual-class issuers (a holder's one-class shares can exceed
    another class's count) — precise per-class denominators are #1646's scope;
    the 300,000× garbage here is caught by any sane threshold."""
    return shares <= outstanding


def _reconcile_institutional_families(
    survivors: list[Holder],
    blockholders: list[Holder],
    unmatched_def14a: list[_Candidate],
    outstanding: Decimal,
) -> tuple[
    dict[SliceCategory, list[Holder]],
    list[Holder],
    list[Holder],
    list[_Candidate],
    list[CorrectionApplied],
]:
    """Collapse each curated institutional manager family to ONE holder at
    ``MAX(Σ 13F holdings, max proxy, max 13G/D)``, counted once (#1644 + #1649).

    Runs BEFORE :func:`_reconcile_owner_once` on its raw inputs — the per-(CIK,
    source) 13F survivors, the 13D/G blockholders, and the unmatched DEF 14A proxy
    candidates — because owner-once has already MAX-folded each CIK and buried the
    raw channel figures in ``dropped_sources`` (Codex plan review G1).

    Returns ``(family_by_category, rest_survivors, rest_blockholders,
    rest_unmatched, corrections)``. The ``rest_*`` flow into the normal pipeline
    unchanged (zero regression for non-curated holders); ``family_by_category``
    holders are injected into the corresponding slice category; ``corrections``
    record each cross-channel fold (the #1647 contract)."""
    fams: dict[str, InstitutionalFamily] = {}
    ch_13f: dict[str, list[Holder]] = {}
    ch_13g: dict[str, list[Holder]] = {}
    ch_proxy: dict[str, list[_Candidate]] = {}

    # Partition. Only 13F survivors are family-eligible on the survivor side;
    # Form 4 / Form 3 / matched-DEF14A survivors are persons, never families.
    rest_survivors: list[Holder] = []
    for h in survivors:
        fam = resolve_family(h.filer_cik, h.filer_name) if h.winning_source == "13f" else None
        if fam is None:
            rest_survivors.append(h)
        else:
            fams[fam.family_id] = fam
            ch_13f.setdefault(fam.family_id, []).append(h)

    rest_blockholders: list[Holder] = []
    for h in blockholders:
        fam = resolve_family(h.filer_cik, h.filer_name)
        if fam is None:
            rest_blockholders.append(h)
        else:
            fams[fam.family_id] = fam
            ch_13g.setdefault(fam.family_id, []).append(h)

    rest_unmatched: list[_Candidate] = []
    for c in unmatched_def14a:
        fam = resolve_family(c.filer_cik, c.filer_name)
        if fam is None:
            # Non-family proxy row stays in the additive wedge — but sanity-reject
            # parser garbage first (#1644: LAMR Kevin Reilly 48T, GEF groups).
            if _value_is_sane(c.shares, outstanding):
                rest_unmatched.append(c)
            else:
                logger.warning(
                    "ownership_rollup: rejected garbage proxy value %s for %r (outstanding=%s)",
                    c.shares,
                    c.filer_name,
                    outstanding,
                )
            continue
        fams[fam.family_id] = fam
        ch_proxy.setdefault(fam.family_id, []).append(c)

    family_by_category: dict[SliceCategory, list[Holder]] = {}
    corrections: list[CorrectionApplied] = []

    for fid, fam in fams.items():
        v13f = [h for h in ch_13f.get(fid, []) if _value_is_sane(h.shares, outstanding)]
        v13g = [h for h in ch_13g.get(fid, []) if _value_is_sane(h.shares, outstanding)]
        vproxy = [c for c in ch_proxy.get(fid, []) if _value_is_sane(c.shares, outstanding)]
        n_rejected = (
            len(ch_13f.get(fid, []))
            - len(v13f)
            + len(ch_13g.get(fid, []))
            - len(v13g)
            + len(ch_proxy.get(fid, []))
            - len(vproxy)
        )
        if n_rejected:
            logger.warning("ownership_rollup: family %s dropped %d garbage-value channel row(s)", fid, n_rejected)

        total_rows = len(v13f) + len(v13g) + len(vproxy)
        if total_rows == 0:
            continue
        # Even a single curated-family row is emitted as a family holder so it
        # lands in the family BUCKET (institutions) — NOT left to owner-once,
        # which would classify a lone 13G as blockholders or a lone proxy as the
        # def14a_unmatched wedge, contradicting the registry bucket (Codex ckpt-2
        # P1). A single 13F row collapses to itself (one member), same bucket and
        # shares as before — no math change, just a consistent family label.

        f_13f = sum((h.shares for h in v13f), Decimal(0))
        f_proxy = max((c.shares for c in vproxy), default=Decimal(0))
        f_13g = max((h.shares for h in v13g), default=Decimal(0))

        channel_figs: list[tuple[str, Decimal]] = []
        if v13f:
            channel_figs.append(("13f", f_13f))
        if v13g:
            channel_figs.append(("13g", f_13g))
        if vproxy:
            channel_figs.append(("def14a", f_proxy))
        # MAX figure; deterministic tie-break to the preferred (current) channel.
        winner_key, family_figure = max(channel_figs, key=lambda kv: (kv[1], -_CHANNEL_TIEBREAK[kv[0]]))

        # The family's 13F sub-CIK rows are the display breakdown regardless of
        # which channel wins the figure (review bot: the BlackRock/proxy-wins shape
        # must still surface its sub-books). When 13F wins they sum to the figure;
        # when a consolidated proxy/13G wins, they are the (folded, smaller) 13F
        # detail — non-additive either way.
        members: tuple[FamilyMember, ...] = tuple(
            FamilyMember(
                filer_cik=h.filer_cik,
                filer_name=h.filer_name,
                shares=h.shares,
                source=h.winning_source,
                accession_number=h.winning_accession,
                edgar_url=h.winning_edgar_url,
                as_of_date=h.as_of_date,
            )
            for h in sorted(v13f, key=lambda h: h.shares, reverse=True)
        )

        # Winning representative row → accession / edgar / as-of for the family holder.
        if winner_key == "13f":
            rep = max(v13f, key=lambda h: h.shares)
            win_source: SourceTag = "13f"
            win_acc, win_url, win_cik, filer_type = (
                rep.winning_accession,
                rep.winning_edgar_url,
                rep.filer_cik,
                rep.filer_type,
            )
            # Conservative as-of for an aggregated sum: the OLDEST member quarter
            # (never max — that overstates freshness). Codex plan review Q5.
            member_dates = [h.as_of_date for h in v13f if h.as_of_date is not None]
            as_of = min(member_dates) if member_dates else None
        elif winner_key == "13g":
            rep = max(v13g, key=lambda h: h.shares)
            win_source = rep.winning_source  # "13d" or "13g"
            win_acc, win_url, win_cik, filer_type, as_of = (
                rep.winning_accession,
                rep.winning_edgar_url,
                rep.filer_cik,
                None,
                rep.as_of_date,
            )
        else:  # def14a proxy
            repc = max(vproxy, key=lambda c: c.shares)
            win_source = "def14a"
            win_acc, win_url, win_cik, filer_type, as_of = (
                repc.accession_number,
                edgar_archive_url(repc.accession_number),
                None,
                None,
                repc.as_of_date,
            )

        # Folded (losing) channels → one correction each. The 13F channel's detail
        # lives in ``family_members`` (so a folded 13F is a correction only — no
        # duplicate dropped_source); the single-figure proxy / 13G channels fold to
        # a dropped_source as well.
        dropped: list[DroppedSource] = []
        # (source, figure, accession, edgar_url, as_of, emit_dropped_source)
        losers: list[tuple[SourceTag, Decimal, str, str | None, date | None, bool]] = []
        if winner_key != "13f" and v13f:
            r = max(v13f, key=lambda h: h.shares)
            losers.append(("13f", f_13f, r.winning_accession, r.winning_edgar_url, r.as_of_date, False))
        if winner_key != "13g" and v13g:
            r = max(v13g, key=lambda h: h.shares)
            losers.append((r.winning_source, f_13g, r.winning_accession, r.winning_edgar_url, r.as_of_date, True))
        if winner_key != "def14a" and vproxy:
            rc = max(vproxy, key=lambda c: c.shares)
            losers.append(
                ("def14a", f_proxy, rc.accession_number, edgar_archive_url(rc.accession_number), rc.as_of_date, True)
            )
        for src, fig, acc, url, loser_as_of, emit_dropped in losers:
            if emit_dropped:
                dropped.append(
                    DroppedSource(source=src, accession_number=acc, shares=fig, as_of_date=loser_as_of, edgar_url=url)
                )
            corrections.append(
                CorrectionApplied(
                    kind=("def14a_restates_institution" if src == "def14a" else "institutional_family_collapse"),
                    filer_name=fam.display_name,
                    shares_removed=fig,
                    filer_cik=win_cik,
                    family_id=fam.family_id,
                    source_channel=src,
                    winning_source=win_source,
                    winning_accession=win_acc,
                    detail=(
                        f"{fam.display_name}: {src} {fig} folded under {win_source} {family_figure}"
                        f" (as_of {loser_as_of} vs {as_of})"
                    ),
                )
            )

        family_holder = Holder(
            filer_cik=win_cik,
            filer_name=fam.display_name,
            shares=family_figure,
            pct_outstanding=Decimal(0),  # filled by _build_slice once denom known
            winning_source=win_source,
            winning_accession=win_acc,
            winning_edgar_url=win_url,
            as_of_date=as_of,
            filer_type=filer_type,
            dropped_sources=tuple(dropped),
            family_members=members,
        )
        family_by_category.setdefault(fam.bucket, []).append(family_holder)

    return family_by_category, rest_survivors, rest_blockholders, rest_unmatched, corrections


# ---------------------------------------------------------------------------
# 13D/G group collapse — count a Rule 13d-5 group once (#1645)
# See docs/specs/etl/2026-06-16-blockholder-13d-group-collapse.md
# ---------------------------------------------------------------------------

# A Rule 13d-5 group's members each report the IDENTICAL aggregate group stake on
# separate accessions/CIKs, so they sum N× in the blockholders wedge. The group is
# inferred from the only signal actually present in modern 13D/G XML: same issuer
# (the rollup scopes to one) + same period_end + a near-equal, non-round aggregate
# across distinct non-null CIKs. (The <memberOfGroup> Item-2 checkbox is empirically
# absent/blank in the source — see spec.) Three guards keep it conservative (it fails
# toward keeping holders separate):
#  - roundness: a round-lot shared figure (multiple of _ROUNDNESS_UNIT) is plausible
#    independent coincidence → never collapses;
#  - member-count tiered tolerance: a 3+-member cluster uses the loose _GROUP_REL_TOL
#    band (a 3-way numeric coincidence is negligible; this catches a control group
#    whose members carry small personal slivers, e.g. TKO 0.064%), but a 2-member
#    cluster — the coincidence-prone case — demands the tight _GROUP_PAIR_REL_TOL
#    (every genuine 2-member group on dev is exact or within ~1 share);
#  - a round seed never anchors a cluster, so it cannot swallow a genuine non-round
#    sub-group and then dissolve it (Codex ckpt-2 HIGH).
_GROUP_REL_TOL: Final[Decimal] = Decimal("0.001")  # 0.1% — cluster band + 3+-member collapse gate
_GROUP_PAIR_REL_TOL: Final[Decimal] = Decimal("0.00001")  # 0.001% — tighter gate for a 2-member cluster
_ROUNDNESS_UNIT: Final[int] = 100_000  # a shared figure that is a whole multiple is round-lot-coincidence-prone


def _is_group_block(shares: Decimal) -> bool:
    """A shared blockholder figure is 'improbably precise' — group evidence, not
    round-lot coincidence — when it is NOT a whole multiple of ``_ROUNDNESS_UNIT``.

    700,000 / 1,000,000 / 100,000 are plausible independent round lots (round → keep
    separate); 7,720,340 / 6,016,847 / 2,724,075 are not (collapse)."""
    return shares % _ROUNDNESS_UNIT != 0


def _is_collapsible_cluster(cluster: list[Holder], seed_shares: Decimal) -> bool:
    """Distinct-CIK, member-count tiered tolerance (Codex ckpt-2). The tier is keyed
    on the count of DISTINCT reporter CIKs (a group inference needs distinct-CIK
    evidence — duplicate-CIK rows are not a Rule 13d-5 group; ``_dedup_within_source``
    already collapses them upstream, this is the standalone guard). A cluster with ≥3
    distinct CIKs collapses on the loose band already applied (a 3-way same-period
    non-round coincidence is negligible; this preserves a control group whose members
    carry small personal slivers, e.g. TKO). Exactly 2 distinct CIKs — the
    coincidence-prone case — collapses only when the smallest member is within the
    tighter ``_GROUP_PAIR_REL_TOL`` of the seed (cluster max); every genuine 2-member
    group on dev is exact or within ~1 share."""
    n_distinct = len({h.filer_cik for h in cluster})
    if n_distinct < 2:
        return False
    if n_distinct >= 3:
        return True
    return min(h.shares for h in cluster) >= seed_shares - _GROUP_PAIR_REL_TOL * seed_shares


def _collapse_blockholder_group(cluster: list[Holder]) -> tuple[Holder, CorrectionApplied]:
    """Collapse a confirmed group ``cluster`` (≥2 members, sorted shares-descending
    by the caller) to ONE holder at the rep's shares (the max-share seed, Rule 13d-3
    total beneficial ownership). Non-rep members are appended to the rep's existing
    ``dropped_sources`` (the amendment-chain provenance is preserved, never replaced)
    and folded into one ``blockholder_group_collapse`` correction."""
    rep = cluster[0]
    losers = cluster[1:]
    dropped = list(rep.dropped_sources)
    for loser in losers:
        dropped.append(
            DroppedSource(
                source=loser.winning_source,
                accession_number=loser.winning_accession,
                shares=loser.shares,
                as_of_date=loser.as_of_date,
                edgar_url=loser.winning_edgar_url,
            )
        )
    collapsed = replace(rep, dropped_sources=tuple(dropped))
    shares_removed = sum((loser.shares for loser in losers), Decimal(0))
    folded = "; ".join(f"{loser.filer_name} {loser.shares}" for loser in losers)
    correction = CorrectionApplied(
        kind="blockholder_group_collapse",
        filer_name=rep.filer_name,
        shares_removed=shares_removed,
        filer_cik=rep.filer_cik,
        source_channel=rep.winning_source,  # intra-channel collapse: winning == folded channel
        winning_source=rep.winning_source,
        winning_accession=rep.winning_accession,
        detail=(
            f"Rule 13d-5 group: {len(cluster)} members reporting ~{rep.shares} as of "
            f"{rep.as_of_date}; counted once at MAX under {rep.filer_name}. Folded: {folded}"
        ),
    )
    return collapsed, correction


def _reconcile_13d_groups(
    blockholders: list[Holder],
    survivor_keys: frozenset[str],
) -> tuple[list[Holder], list[CorrectionApplied]]:
    """Collapse each inferred Rule 13d-5 blockholder group to ONE holder at MAX,
    counted once (#1645). Pure read-path; scoped to the blockholders pie wedge.

    Predicate (see spec): distinct non-null reporter CIKs; same non-null
    ``period_end``; ``shares > 0``; ``aggregate_amount_owned`` within the
    member-count tiered tolerance of the cluster max (descending-greedy, max-anchored
    — never transitively chained; :func:`_is_collapsible_cluster`); the max non-round
    and never a round seed anchoring the cluster (:func:`_is_group_block`).

    A blockholder whose identity key is in ``survivor_keys`` (a CIK that also files
    Form 4 / Form 3 / 13F / matched DEF 14A — the non-blockholder candidate set
    ``_reconcile_owner_once`` reconciles by CIK) is EXCLUDED from clustering and
    passed through untouched. owner-once already merges that CIK's 13D with its other
    channels; folding it into a different rep would orphan those rows and re-count the
    block (Codex ckpt-1 HIGH-3). The cross-channel control-group case (a sponsor's GP/LP
    chain that Form-4s the same deemed block) is handled UPSTREAM by
    :func:`_reconcile_insider_control_groups` (#1652), which consumes those members'
    13D/G rows before this pass sees them; this pass owns the purely-blockholder
    co-investor group (no Form-4 footprint).

    Returns ``(survivors, corrections)``: the post-collapse blockholder list (each
    group folded to one MAX holder; non-group + excluded rows unchanged) and one
    ``blockholder_group_collapse`` correction per collapsed group."""
    survivors: list[Holder] = []
    by_period: dict[date, list[Holder]] = {}
    for h in blockholders:
        # Exclude null-period, non-positive-share, null/empty-CIK, and survivor-
        # overlapping rows from clustering; they pass through to owner-once exactly as
        # before. A group inference needs distinct CIK evidence — a null-CIK row gives
        # none (and reporter_cik is NOT NULL in _current, so this is defence-in-depth).
        cik = h.filer_cik
        if (
            h.as_of_date is None
            or h.shares <= 0
            or cik is None
            or not cik.strip()
            or _identity_key(cik, h.filer_name) in survivor_keys
        ):
            survivors.append(h)
        else:
            by_period.setdefault(h.as_of_date, []).append(h)

    corrections: list[CorrectionApplied] = []
    for rows in by_period.values():
        if len(rows) < 2:
            survivors.extend(rows)
            continue
        # Descending by shares, deterministic tie-break. Max-anchored greedy: each
        # unused NON-ROUND row (descending) seeds a cluster and pulls every later row
        # within the loose band OF THE SEED (the cluster max). A row below the band
        # seeds its own later cluster — no transitive chaining.
        rows_desc = sorted(
            rows,
            key=lambda h: (h.shares, h.filer_cik or "", h.winning_accession),
            reverse=True,
        )
        used = [False] * len(rows_desc)
        for i, seed in enumerate(rows_desc):
            if used[i]:
                continue
            used[i] = True
            # A round seed never anchors: pass it through as a standalone so it cannot
            # swallow a genuine non-round sub-group and then dissolve it (Codex ckpt-2
            # HIGH). The next unused non-round row seeds the next cluster.
            if not _is_group_block(seed.shares):
                survivors.append(seed)
                continue
            floor = seed.shares - _GROUP_REL_TOL * seed.shares
            cluster = [seed]
            for j in range(i + 1, len(rows_desc)):
                if used[j]:
                    continue
                if rows_desc[j].shares < floor:
                    break  # descending — nothing further is within the seed's band
                # A round member is coincidence-prone too — never group evidence, so it
                # can't inflate the cluster count past the tight 2-member gate (Codex
                # ckpt-2). Leave it unused so it is handled as its own standalone.
                if not _is_group_block(rows_desc[j].shares):
                    continue
                used[j] = True
                cluster.append(rows_desc[j])
            if len(cluster) >= 2 and _is_collapsible_cluster(cluster, seed.shares):
                collapsed, correction = _collapse_blockholder_group(cluster)
                survivors.append(collapsed)
                corrections.append(correction)
            else:
                survivors.extend(cluster)
    return survivors, corrections


# ---------------------------------------------------------------------------
# Insider control-group collapse — count a deemed-ownership block once (#1652)
# See docs/specs/etl/2026-06-16-insider-control-group-collapse.md
# ---------------------------------------------------------------------------
#
# In a sponsor-controlled issuer every entity in a PE fund's GP/LP chain files its
# own Form 4 for the SAME indirect-beneficial block (Rule 13d-3 deemed ownership),
# and the same related CIKs restate it on Schedule 13D/G (a >10% owner is both a
# Section-16 insider AND a 5% blockholder). Per-CIK dedup keeps every CIK, so the
# block sums N× and the insiders wedge explodes past 100% of float (AMTM 423%,
# VSAT 116%, LCID 1719%). Deemed ownership flows the BYTE-IDENTICAL number up the
# chain and across channels, so the signal is exact (not #1645's near-equal band):
# the same exact non-round value across ≥2 distinct CIKs on one instrument, spanning
# Form 4 / Form 3 / 13D / 13G. Validated zero false positives over 1,150 dev buckets
# (every one is a single genuine control group — Berkshire/Buffett, JAB/Reimann,
# KKR, Blackstone, Carlos Slim — even those reporting a static block across years, so
# NO period constraint: a window would wrongly split a long-held block).
_INSIDER_GROUP_SOURCES: Final[frozenset[SourceTag]] = frozenset({"form4", "form3"})
_BLOCK_GROUP_SOURCES: Final[frozenset[SourceTag]] = frozenset({"13d", "13g"})
# Eligibility scope = the CIK-keyed beneficial-restatement channels (precomputed union
# so _is_eligible doesn't rebuild it per holder).
_GROUP_ELIGIBLE_SOURCES: Final[frozenset[SourceTag]] = _INSIDER_GROUP_SOURCES | _BLOCK_GROUP_SOURCES
# Magnitude floor for the deemed-block signal. The non-round guard (_is_group_block,
# divisibility by _ROUNDNESS_UNIT) is magnitude-BLIND — it flags a 19,532-share director
# grant as "precise" exactly as it flags 45,026,743. Below ~1M, an exact non-round match
# across distinct CIKs is dominated by coincidental equal small grants (dev: ~1,800 such
# clusters, e.g. three EVEX directors each at 101,107 — independent, not a deemed block),
# whereas every ≥1M exact-match cluster on dev (1,144 of them) is a genuine control group
# (fund+principal, trust+person, parent/sub). A sub-1M deemed block also cannot
# meaningfully explode a normal float, so the floor sheds the coincidence-prone tail while
# keeping every explosion-causing block; the residual (a rare small real control group)
# stays un-collapsed, the conservative direction (matches the round-lot residual).
_INSIDER_GROUP_MIN_SHARES: Final[Decimal] = Decimal(1_000_000)


def _collapse_insider_control_group(cluster: list[Holder]) -> tuple[Holder, CorrectionApplied]:
    """Collapse a confirmed control-group ``cluster`` (≥2 distinct CIKs, ≥1 insider
    member, all at the same exact non-round block value) to ONE holder at that value
    (Rule 13d-3 total beneficial ownership, counted once).

    Representative = a member, preferring an insider source (``form4``/``form3``) so the
    rep routes to the insiders slice via owner-once, then deterministic tie-break
    ``(insider-source, shares, filer_cik, winning_accession)`` descending. Non-rep members
    (from BOTH channels) are appended to the rep's existing ``dropped_sources`` (amendment
    provenance preserved) and folded into one ``insider_control_group_collapse`` correction
    whose ``detail`` carries each folded member's CIK + name + shares (``DroppedSource`` has
    no CIK/name field — same limitation as #1645)."""
    rep = sorted(
        cluster,
        key=lambda h: (
            h.winning_source in _INSIDER_GROUP_SOURCES,
            h.shares,
            h.filer_cik or "",
            h.winning_accession,
        ),
        reverse=True,
    )[0]
    losers = [h for h in cluster if h is not rep]
    dropped = list(rep.dropped_sources)
    for loser in losers:
        dropped.append(
            DroppedSource(
                source=loser.winning_source,
                accession_number=loser.winning_accession,
                shares=loser.shares,
                as_of_date=loser.as_of_date,
                edgar_url=loser.winning_edgar_url,
            )
        )
    collapsed = replace(rep, dropped_sources=tuple(dropped))
    shares_removed = sum((loser.shares for loser in losers), Decimal(0))
    folded = "; ".join(f"{loser.filer_name} ({loser.filer_cik}) {loser.shares}" for loser in losers)
    correction = CorrectionApplied(
        kind="insider_control_group_collapse",
        filer_name=rep.filer_name,
        shares_removed=shares_removed,
        filer_cik=rep.filer_cik,
        source_channel=rep.winning_source,  # intra-insider-channel rep (cross-channel fold in detail)
        winning_source=rep.winning_source,
        winning_accession=rep.winning_accession,
        detail=(
            f"Control group: {len(cluster)} related CIKs reporting {rep.shares} (deemed "
            f"ownership) counted once under {rep.filer_name}. Folded: {folded}"
        ),
    )
    return collapsed, correction


def _collapse_same_accession_channel(
    holders: list[Holder],
    *,
    eligible: Callable[[Holder], bool],
    collapse: Callable[[list[Holder]], tuple[Holder, CorrectionApplied]],
    corrections: list[CorrectionApplied],
    sort_cluster_shares_desc: bool = False,
) -> list[Holder]:
    """Collapse same-(``winning_accession``, ``shares``) groups within one channel.

    Eligible holders are bucketed by ``(winning_accession, shares)``; a bucket with **≥2
    distinct** :func:`_identity_key` collapses via ``collapse`` (one rep + the folded
    members in ``dropped_sources`` + one ``corrections`` entry). Single-member buckets and
    ineligible holders pass through unchanged. ``sort_cluster_shares_desc`` pre-sorts the
    cluster shares-descending (with a deterministic ``(shares, filer_cik, accession)``
    tie-break) for collapsers that take the rep as ``cluster[0]``
    (:func:`_collapse_blockholder_group`); the insider collapser sorts internally."""
    groups: dict[tuple[str, Decimal], list[Holder]] = {}
    out: list[Holder] = []
    for h in holders:
        if eligible(h):
            groups.setdefault((h.winning_accession, h.shares), []).append(h)
        else:
            out.append(h)
    for cluster in groups.values():
        if len({_identity_key(h.filer_cik, h.filer_name) for h in cluster}) >= 2:
            if sort_cluster_shares_desc:
                cluster = sorted(
                    cluster,
                    key=lambda h: (h.shares, h.filer_cik or "", h.winning_accession),
                    reverse=True,
                )
            rep, correction = collapse(cluster)
            out.append(rep)
            corrections.append(correction)
        else:
            out.extend(cluster)
    return out


def _reconcile_same_accession_groups(
    survivors: list[Holder],
    blockholders: list[Holder],
) -> tuple[list[Holder], list[Holder], list[CorrectionApplied]]:
    """Collapse a same-accession control-chain duplicate to ONE holder, counted once (#1764).

    A joint Form 4/3 (or 13D/G) accession reports the SAME deemed block under ≥2 distinct
    reporting owners (the controlling person + the controlled entity, or a fund GP/LP chain)
    — Rule 16a-1(a)(2) / Rule 13d-3 deemed beneficial ownership. Per-CIK dedup keeps all N
    rows and a ``SUM`` counts the one block N× (data-engineer I14: MAX overlapping, SUM
    additive). This is the PRECISE same-accession variant of the fuzzy cross-accession #1652
    pass: the accession itself is direct group-membership evidence (parties co-file ONE
    accession only when they ARE a group under Rule 16a-3(j) / Rule 13d-1(k)), so unlike
    #1652/#1645 it needs **NO magnitude floor or roundness proxy** — those guards exist only
    to substitute for the membership evidence that a shared accession already provides.

    Restricted to insider sources ``{form4, form3}`` (in ``survivors``) and the blockholder
    channel ``{13d, 13g}`` (in ``blockholders``) ONLY. DEF 14A is deliberately excluded: all
    proxy holders share ONE accession, so same-accession does NOT separate independent
    equal-grant executives there (the #1659 false-positive class) — and a def14a-source
    holder CAN reach ``survivors`` (matched proxy rows). 13F is excluded too (one filer per
    accession → no same-accession multi-holder dup by construction).

    Group key = ``(winning_accession, shares)`` with a NON-EMPTY accession (a NULL/'' accession
    coerces to '' and would wrongly bucket unrelated equal-share holders — Codex ckpt-2 #1),
    ``shares > 0``, and ≥2 distinct :func:`_identity_key`. Distinctness is keyed on holder
    identity, NOT rows, so a single person's direct+indirect on one accession (distinct count
    = 1) is untouched and flows to :func:`_reconcile_owner_once`'s additive SUM.

    **Cross-channel consume (Codex ckpt-2 #2, like #1652).** A folded insider member often ALSO
    restates the SAME deemed block on a 13D/G (226 such pairs on dev). If only the insider rows
    collapsed, the loser's matching blockholder row would orphan — it is no longer a survivor so
    ``survivor_keys`` cannot exclude it and owner-once would re-count it (relocating, not removing,
    the double-count). So each insider group ALSO pulls in every blockholder row whose
    ``(_identity_key, shares)`` matches a group member; those fold into the insider rep's
    ``dropped_sources`` and are removed from ``blockholders``. The blockholder-only same-accession
    pass then runs over the remainder (a purely-13D/G co-investor group with no insider member).

    Pure read-path; runs BEFORE the #1652 and #1645 fuzzy passes (so they see the single
    representative, never the N dups). The rep keeps its original ``_identity_key`` so the
    downstream survivor-key exclusion + owner-once see it correctly."""
    corrections: list[CorrectionApplied] = []

    # --- Insider side: bucket form4/form3 survivors by (accession, shares), pull matching
    #     cross-channel 13D/G restatements into the cluster so they fold into the rep. ---
    eligible_insider: dict[tuple[str, Decimal], list[Holder]] = {}
    survivors_out: list[Holder] = []
    for h in survivors:
        if h.winning_source in _INSIDER_GROUP_SOURCES and h.shares > 0 and h.winning_accession.strip():
            eligible_insider.setdefault((h.winning_accession, h.shares), []).append(h)
        else:
            survivors_out.append(h)

    blockholders_by_key: dict[tuple[str, Decimal], list[Holder]] = {}
    for b in blockholders:
        blockholders_by_key.setdefault((_identity_key(b.filer_cik, b.filer_name), b.shares), []).append(b)
    consumed_blockholders: set[int] = set()

    for (_acc, shares), cluster in eligible_insider.items():
        if len({_identity_key(h.filer_cik, h.filer_name) for h in cluster}) < 2:
            survivors_out.extend(cluster)
            continue
        cross_channel: list[Holder] = []
        for member in cluster:
            for b in blockholders_by_key.get((_identity_key(member.filer_cik, member.filer_name), shares), []):
                if id(b) not in consumed_blockholders:
                    cross_channel.append(b)
                    consumed_blockholders.add(id(b))
        rep, correction = _collapse_insider_control_group(cluster + cross_channel)
        survivors_out.append(rep)
        corrections.append(correction)

    blockholders_remaining = [b for b in blockholders if id(b) not in consumed_blockholders]

    # --- Blockholder-only side: a purely-13D/G joint accession (Rule 13d-5 group, each member
    #     deemed to own the whole stake → identical aggregate) with no insider footprint. ---
    blockholders_out = _collapse_same_accession_channel(
        blockholders_remaining,
        eligible=lambda h: (
            h.winning_source in _BLOCK_GROUP_SOURCES and h.shares > 0 and bool(h.winning_accession.strip())
        ),
        collapse=_collapse_blockholder_group,
        corrections=corrections,
        sort_cluster_shares_desc=True,
    )
    return survivors_out, blockholders_out, corrections


def _reconcile_insider_control_groups(
    survivors: list[Holder],
    blockholders: list[Holder],
) -> tuple[list[Holder], list[Holder], list[CorrectionApplied]]:
    """Collapse each inferred cross-channel control group to ONE insiders holder at the
    block value, counted once (#1652). Pure read-path.

    Operates on the union of insider survivors (``form4``/``form3``) and blockholders
    (``13d``/``13g``), bucketed by EXACT ``shares`` value. A bucket collapses when it has
    **≥2 distinct non-null CIKs** AND **≥1 insider-source member** (a Form-4 footprint —
    the #1652 explosion). A purely-13D/G bucket (a co-investor group with no insider) is
    left untouched for :func:`_reconcile_13d_groups` (#1645); the two passes partition the
    work. Only **non-round** (:func:`_is_group_block`), positive, non-null-CIK rows are
    eligible; everything else passes through to its channel unchanged.

    Consumption is **exact-value only**: a consumed CIK's 13D/G row at a *different* value
    (usually the larger full group block) stays in ``blockholders`` for #1645/owner-once —
    folding a member's entire 13D footprint would delete the genuine larger block (see
    spec, Codex ckpt-1 MED).

    Returns ``(survivors, blockholders, corrections)``: the bucket members removed from
    BOTH lists (nothing orphaned — the 13D rows of a collapsed group are consumed here, so
    neither #1645 nor owner-once can re-count the block) and the rep added back to
    ``survivors``; one ``insider_control_group_collapse`` correction per collapsed group."""
    survivors_out: list[Holder] = []
    blockholders_out: list[Holder] = []
    eligible_by_value: dict[Decimal, list[tuple[str, Holder]]] = {}

    def _is_eligible(h: Holder) -> bool:
        cik = h.filer_cik
        return (
            h.shares >= _INSIDER_GROUP_MIN_SHARES
            and cik is not None
            and bool(cik.strip())
            and _is_group_block(h.shares)
            and h.winning_source in _GROUP_ELIGIBLE_SOURCES
        )

    for origin, source_list, out_list in (
        ("s", survivors, survivors_out),
        ("b", blockholders, blockholders_out),
    ):
        for h in source_list:
            if _is_eligible(h):
                eligible_by_value.setdefault(h.shares, []).append((origin, h))
            else:
                out_list.append(h)

    corrections: list[CorrectionApplied] = []
    for members in eligible_by_value.values():
        holders = [h for _, h in members]
        distinct_ciks = {h.filer_cik for h in holders}
        has_insider = any(h.winning_source in _INSIDER_GROUP_SOURCES for h in holders)
        if len(distinct_ciks) >= 2 and has_insider:
            collapsed, correction = _collapse_insider_control_group(holders)
            # The rep is always an insider-source row (≥1 insider member + rep preference),
            # so it routes to the insiders slice via owner-once → goes to survivors.
            survivors_out.append(collapsed)
            corrections.append(correction)
        else:
            for origin, h in members:
                (survivors_out if origin == "s" else blockholders_out).append(h)
    return survivors_out, blockholders_out, corrections


def _bucket_into_slices(
    by_category: dict[SliceCategory, list[Holder]],
    unmatched_def14a: list[_Candidate],
    outstanding: Decimal,
    *,
    funds_holders: list[Holder] | None = None,
    esop_holders: list[Holder] | None = None,
) -> list[OwnershipSlice]:
    """Build slices from the per-category holder lists produced by
    :func:`_reconcile_owner_once` (each owner already in exactly one
    category).

    ``funds_holders`` (#919) arrives pre-deduped from
    :func:`_collect_funds_from_current` (PK-deduped at table level)
    and lands in the funds memo-overlay slice. Renders as a separate
    slice but does NOT contribute to residual / concentration math —
    N-PORT rows are fund-level detail of holdings already aggregated
    in the institutions slice via 13F-HR, so additive accounting would
    double-count.

    ``esop_holders`` (#961) arrives pre-deduped from
    :func:`_collect_esop_from_current` (PK-deduped at table level) and
    lands in its own ``esop`` memo-overlay slice, ``denominator_basis=
    "proxy_disclosure"`` — same basis as ``def14a_unmatched``, since
    ESOP rows are DEF 14A beneficial-ownership disclosure (SEC Item
    403), not a distinct institutional holding."""
    slices: list[OwnershipSlice] = []

    def _add(built: OwnershipSlice) -> None:
        # A slice whose holders are all zero-share becomes empty after
        # ``_build_slice`` drops them (#1916 Finding A) — skip it, mirroring
        # the "no holders → no slice" convention below.
        if built.holders:
            slices.append(built)

    for category in _CATEGORY_ORDER:
        holders = by_category.get(category)
        if not holders:
            continue
        _add(_build_slice(category, holders, outstanding))

    if unmatched_def14a:
        unmatched_holders = [
            Holder(
                filer_cik=None,
                filer_name=c.filer_name,
                shares=c.shares,
                pct_outstanding=Decimal(0),
                winning_source="def14a",
                winning_accession=c.accession_number,
                winning_edgar_url=edgar_archive_url(c.accession_number),
                as_of_date=c.as_of_date,
                filer_type=None,
                dropped_sources=(),
            )
            for c in unmatched_def14a
        ]
        # Non-additive memo overlay (#1659): DEF 14A beneficial ownership is a Rule
        # 13d-3 deemed/overlapping disclosure (SEC Item 403), not additive holdings —
        # so it does NOT contribute to the pie / residual / concentration. Renders as
        # a cross-check overlay (the additive math filters denominator_basis ==
        # "pie_wedge"; the memo paths filter != "pie_wedge").
        _add(
            _build_slice(
                "def14a_unmatched",
                unmatched_holders,
                outstanding,
                denominator_basis="proxy_disclosure",
            )
        )

    if funds_holders:
        _add(
            _build_slice(
                "funds",
                list(funds_holders),
                outstanding,
                denominator_basis="institution_subset",
            )
        )

    if esop_holders:
        _add(
            _build_slice(
                "esop",
                list(esop_holders),
                outstanding,
                denominator_basis="proxy_disclosure",
            )
        )
    return slices


def _calendar_quarter(d: date) -> tuple[int, int]:
    """``(year, quarter)`` with quarter in ``1..4`` (human convention).

    13F quarter-ends (03-31/06-30/09-30/12-31) map cleanly to Q1..Q4."""
    return (d.year, (d.month - 1) // 3 + 1)


def _slice_coherence(holders: Sequence[Holder]) -> tuple[date | None, date | None, int, bool]:
    """As-of span of a slice's holders (#1647 part 1).

    Gathers, per holder, ``holder.as_of_date`` PLUS every
    ``family_member.as_of_date`` — a collapsed family is one synthetic Holder
    whose own ``as_of_date`` is ``min(member dates)``, so looking only at the
    holder would hide an intra-family quarter spread (Codex ckpt-1). Dropped
    sources are NOT gathered — a dedup loser is not part of the counted
    figure's span.

    NULL as-of dates are ignored. Returns
    ``(as_of_min, as_of_max, distinct_quarters, mixed_period)``; an all-NULL
    (or empty) slice → ``(None, None, 0, False)``."""
    dates: list[date] = []
    for h in holders:
        if h.as_of_date is not None:
            dates.append(h.as_of_date)
        for m in h.family_members:
            if m.as_of_date is not None:
                dates.append(m.as_of_date)
        # A collapsed owner's lots (#1942) can span quarters (direct on a Form 4,
        # indirect on a Form 3) — gather them so the coherence envelope reflects
        # the real span rather than only the representative lot's date.
        for lot in h.lots:
            if lot.as_of_date is not None:
                dates.append(lot.as_of_date)
    if not dates:
        return (None, None, 0, False)
    quarters = {_calendar_quarter(d) for d in dates}
    distinct = len(quarters)
    return (min(dates), max(dates), distinct, distinct > 1)


def _collapse_owner_lots(holders: list[Holder]) -> list[Holder]:
    """Collapse each owner's multiple additive lots into ONE display line at the
    summed shares, preserving the per-lot split in ``lots`` for the drilldown
    (#1942). Item 403 (17 CFR 229.403) shows one beneficial owner on one line at
    total beneficial ownership; Form 4 General Instruction 4(b) splits direct /
    indirect onto separate lines, which ``_source_rows_and_total`` keeps additive
    (#905) — this denormalises them back to one row for display, figure-neutral.

    CALLER CONTRACT: only invoke for the ``insiders`` slice. Identity =
    :func:`_identity_key` — the SAME key :func:`_reconcile_owner_once` grouped by,
    so every same-key insider row here came from one reconcile group and is
    genuinely one owner's lot (never two distinct owners; no new merge risk). Only
    the insiders additive path yields multi-row same-identity groups (13F / 13D /
    13G survivors are a single rep, and those rows are pure ``direct`` /
    ``indirect`` — ``beneficial`` overlaps are folded to ``dropped_sources``
    upstream). The ``funds`` / ``def14a_unmatched`` slices must NOT be passed
    here: they bypass reconcile and are intentionally one row per fund_series / per
    proxy nature while sharing a filer CIK, so the CIK-first identity key would
    wrongly merge them (Codex ckpt-2 HIGH). Single-row owners pass through
    untouched with no ``lots``. Representative = the max-shares row, keeping the
    cross-source provenance :func:`_reconcile_owner_once` stamped on the largest
    row; each lot's own ``dropped_sources`` are merged in (de-duped)."""
    groups: dict[str, list[Holder]] = {}
    order: list[str] = []
    for h in holders:
        k = _identity_key(h.filer_cik, h.filer_name)
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(h)
    out: list[Holder] = []
    for k in order:
        rows = groups[k]
        if len(rows) == 1:
            out.append(rows[0])
            continue
        # Deterministic representative on equal shares: shares DESC, then the
        # higher-priority form (Form 4 over Form 3 via ``_PRIORITY_RANK``), then
        # accession — so the winning source/accession/provenance carried onto the
        # collapsed row does NOT depend on the upstream DB row order (Claude review
        # NITPICK, PR #1946). ``-_PRIORITY_RANK`` because ``reverse=True`` wants the
        # preferred (lower-rank) form to sort largest.
        rows_desc = sorted(
            rows,
            key=lambda h: (h.shares, -_PRIORITY_RANK[h.winning_source], h.winning_accession),
            reverse=True,
        )
        primary = rows_desc[0]
        total = sum((h.shares for h in rows_desc), Decimal(0))
        lots = tuple(
            HolderLot(
                ownership_nature=h.ownership_nature,
                shares=h.shares,
                source=h.winning_source,
                accession_number=h.winning_accession,
                edgar_url=h.winning_edgar_url,
                as_of_date=h.as_of_date,
            )
            for h in rows_desc
        )
        # Merge every lot's ``dropped_sources`` onto the representative so a
        # non-primary lot's provenance (a superseded amendment folded within its
        # own nature by the upstream dedup) stays auditable — ``replace`` would
        # otherwise keep only the primary's (Codex ckpt-2 MED). De-dup on
        # (source, accession, shares), preserving the primary's entries first.
        merged_dropped = list(primary.dropped_sources)
        seen = {(d.source, d.accession_number, d.shares) for d in merged_dropped}
        for h in rows_desc[1:]:
            for d in h.dropped_sources:
                dkey = (d.source, d.accession_number, d.shares)
                if dkey in seen:
                    continue
                seen.add(dkey)
                merged_dropped.append(d)
        out.append(replace(primary, shares=total, lots=lots, dropped_sources=tuple(merged_dropped)))
    return out


def _build_slice(
    category: SliceCategory,
    holders: list[Holder],
    outstanding: Decimal,
    *,
    denominator_basis: DenominatorBasis = "pie_wedge",
) -> OwnershipSlice:
    # Drop zero-share holders (#1916 Finding A): a holder whose reconciled
    # current holding is 0 is not a holder of the issuer — rendering it produces
    # a duplicate-looking row (a live ``direct`` lot beside a stale 0-share
    # ``indirect`` lot showed the same person twice, e.g. AAPL / Katherine
    # Adams). Figure-neutral: zero-share rows contribute 0 to ``total`` /
    # ``pct`` / residual, and no ``ownership_*_current`` row is strictly
    # negative (full-population check on the dev DB). Corrects ``filer_count``.
    #
    # ``> 0`` also drops any strictly-negative row. That is impossible today,
    # but a negative row would silently INCREASE the slice total when removed
    # (it was reducing it), so surface it rather than dropping it silently —
    # the invariant is asserted by data, not code, so a regression must be
    # visible, not swallowed (Claude review NITPICK, PR #1940).
    negatives = [h for h in holders if h.shares < Decimal(0)]
    if negatives:
        logger.warning(
            "ownership_rollup %s slice: dropping %d negative-share holder(s) "
            "(unexpected — ownership_*_current should never be negative): %s",
            category,
            len(negatives),
            [(h.filer_cik or h.filer_name, str(h.shares)) for h in negatives],
        )
    holders = [h for h in holders if h.shares > Decimal(0)]
    holders.sort(key=lambda h: h.shares, reverse=True)
    total = sum((h.shares for h in holders), Decimal(0))
    pct_total = total / outstanding if outstanding > 0 else Decimal(0)
    # ``sources`` / ``dominant`` are computed from the PRE-collapse per-lot rows
    # (sorted shares-desc, as before) so the slice source-mix — and the tie order
    # of ``dominant`` — is unchanged by the #1942 display collapse: folding a Form
    # 3 indirect lot's shares under a Form 4 representative would otherwise skew
    # which source dominates (Codex ckpt-1 HIGH; ckpt-2 LOW on tie determinism).
    sources: dict[SourceTag, Decimal] = {}
    for h in holders:
        sources[h.winning_source] = sources.get(h.winning_source, Decimal(0)) + h.shares
    dominant: SourceTag | None = None
    if sources:
        dominant = max(sources.keys(), key=lambda s: sources[s])
    # Collapse an owner's additive direct/indirect lots to one display line at the
    # summed shares (#1942, Item 403). Figure-neutral: ``total`` == the summed
    # per-lot shares. INSIDERS ONLY: only the Section-16 additive path produces
    # multi-row same-identity groups that reached here via _reconcile_owner_once
    # (13F / 13D / 13G survivors are one rep per identity). The ``funds`` and
    # ``def14a_unmatched`` slices bypass reconcile and are intentionally one row
    # per fund_series / per proxy nature while SHARING a filer CIK
    # (``fund_filer_cik`` — 221,540 dev instrument×CIK groups have ≥2 distinct
    # series), so a CIK-keyed collapse there would wrongly merge distinct holders
    # and undercount ``filer_count`` (Codex ckpt-2 HIGH).
    if category == "insiders":
        holders = _collapse_owner_lots(holders)
        holders.sort(key=lambda h: h.shares, reverse=True)
    enriched_holders = tuple(
        Holder(
            filer_cik=h.filer_cik,
            filer_name=h.filer_name,
            shares=h.shares,
            pct_outstanding=(h.shares / outstanding) if outstanding > 0 else Decimal(0),
            winning_source=h.winning_source,
            winning_accession=h.winning_accession,
            winning_edgar_url=h.winning_edgar_url,
            as_of_date=h.as_of_date,
            filer_type=h.filer_type,
            dropped_sources=h.dropped_sources,
            family_members=h.family_members,  # display breakdown of a collapsed family (#1644/#1649)
            lots=h.lots,  # per-lot breakdown of a collapsed direct/indirect owner (#1942)
        )
        for h in holders
    )
    # Count a collapsed family as its constituent filers (the family_members ARE
    # the underlying filers), not as one row, so coverage % is not deflated by the
    # collapse (#1644/#1649). A collapsed owner's direct/indirect lots (#1942) are
    # ONE filer (empty ``family_members`` → counts 1), correcting the prior
    # over-count of one person's two lots as two filers.
    filer_count = sum(len(h.family_members) or 1 for h in holders)
    as_of_min, as_of_max, distinct_quarters, mixed_period = _slice_coherence(enriched_holders)
    return OwnershipSlice(
        category=category,
        label=_SLICE_LABELS[category],
        total_shares=total,
        pct_outstanding=pct_total,
        filer_count=filer_count,
        dominant_source=dominant,
        holders=enriched_holders,
        denominator_basis=denominator_basis,
        as_of_min=as_of_min,
        as_of_max=as_of_max,
        distinct_quarters=distinct_quarters,
        mixed_period=mixed_period,
    )


def _compute_residual(
    outstanding: Decimal,
    slices: Sequence[OwnershipSlice],
    treasury: Decimal | None,
) -> ResidualBlock:
    """Compute the ``Public / unattributed`` residual.

    Stale-mixed-date inputs (fresh Form 4 + old 13F) can leave the
    raw residual negative — we clamp to 0 and surface
    ``oversubscribed=True`` so the frontend renders a warning bar.
    The category-counted slices use deduped totals, so the only path
    to oversubscription is the snapshot-lag class of bug, not a
    dedup mistake."""
    treasury_d = treasury if treasury is not None else Decimal(0)
    # Memo-overlay slices (funds N-PORT; DEF 14A proxy_disclosure #1659; future
    # ESOP/DRS/short-interest) do NOT contribute to ``sum_known`` — they describe
    # positions already counted via a pie-wedge slice (N-PORT funds are fund-level
    # detail inside the 13F-HR institutional aggregate) or, for DEF 14A, a Rule
    # 13d-3 deemed/overlapping disclosure already counted via 13D/G + 13F + Form 4.
    sum_known = sum(
        (s.total_shares for s in slices if s.denominator_basis == "pie_wedge"),
        Decimal(0),
    )
    raw = outstanding - sum_known - treasury_d
    clamped = raw if raw > 0 else Decimal(0)
    pct = clamped / outstanding if outstanding > 0 else Decimal(0)
    return ResidualBlock(
        shares=clamped,
        pct_outstanding=pct,
        label="Public / unattributed",
        tooltip=_RESIDUAL_TOOLTIP,
        oversubscribed=raw < 0,
    )


def _compute_concentration(outstanding: Decimal, slices: Sequence[OwnershipSlice]) -> ConcentrationInfo:
    """Float concentration — sum-of-deduped-pie-wedge-slices / outstanding.
    Treasury excluded (the issuer doesn't invest in itself). Memo-overlay
    slices (funds; DEF 14A proxy_disclosure #1659) excluded so the chip counts
    only additive filings (13F + 13D/G + Form 4) and doesn't double-count a
    position surfaced via both a pie wedge and an overlay."""
    sum_known = sum(
        (s.total_shares for s in slices if s.denominator_basis == "pie_wedge"),
        Decimal(0),
    )
    pct = sum_known / outstanding if outstanding > 0 else Decimal(0)
    return ConcentrationInfo(
        pct_outstanding_known=pct,
        info_chip=f"Known filers hold {pct * 100:.2f}% of float.",
    )


_CATEGORY_ORDER: tuple[SliceCategory, ...] = (
    "insiders",
    "blockholders",
    "institutions",
    "etfs",
)


_INSTITUTIONAL_CATEGORIES: frozenset[SliceCategory] = frozenset({"institutions", "etfs"})


def _compute_sanity(slices: Sequence[OwnershipSlice], outstanding: Decimal) -> SanityChecks:
    """Raw plausibility facts over the pie-wedge slices (#1647 part 4).

    Measurements, not pass/fail. Memo-overlay slices are excluded — they are
    already-counted detail. ``outstanding <= 0`` → zeroed pct / False booleans.
    See :class:`SanityChecks`."""
    pie = [s for s in slices if s.denominator_basis == "pie_wedge"]
    max_distinct_quarters = max((s.distinct_quarters for s in pie), default=0)
    if outstanding <= 0:
        return SanityChecks(
            max_distinct_quarters=max_distinct_quarters,
            institutions_pct=Decimal(0),
            institutions_over_100pct=False,
            largest_single_holder_pct=Decimal(0),
            any_pie_slice_over_100pct=False,
        )
    inst_shares = sum(
        (s.total_shares for s in pie if s.category in _INSTITUTIONAL_CATEGORIES),
        Decimal(0),
    )
    institutions_pct = inst_shares / outstanding
    largest_holder_shares = max(
        (h.shares for s in pie for h in s.holders),
        default=Decimal(0),
    )
    largest_single_holder_pct = largest_holder_shares / outstanding
    # Recompute from this function's ``outstanding`` arg (not the slice's
    # precomputed ``pct_outstanding``) so all four facts share one denominator
    # and cannot disagree if a slice were ever built against a different one
    # (Codex ckpt-2). ``outstanding > 0`` guaranteed by the early return above.
    any_pie_slice_over_100pct = any(s.total_shares / outstanding > 1 for s in pie)
    return SanityChecks(
        max_distinct_quarters=max_distinct_quarters,
        institutions_pct=institutions_pct,
        institutions_over_100pct=institutions_pct > 1,
        largest_single_holder_pct=largest_single_holder_pct,
        any_pie_slice_over_100pct=any_pie_slice_over_100pct,
    )


def _compute_coverage(
    slices: Sequence[OwnershipSlice],
    estimates: dict[str, int | None],
) -> CoverageReport:
    """Build per-category coverage + the fold-up banner state.

    The fold uses ``no_data > red > unknown_universe > amber > green``
    where the worst-of across categories wins. ``unknown_universe``
    ranks worse than ``amber`` because a category without an
    estimate is genuinely unknown — Codex v2 review caught the prior
    rule that let one well-seeded category mask blind spots in the
    others.
    """
    by_category = {s.category: s for s in slices}
    cats: dict[str, CategoryCoverage] = {}
    for category in _CATEGORY_ORDER:
        slc = by_category.get(category)
        known = slc.filer_count if slc is not None else 0
        estimate = estimates.get(category)
        pct_universe: Decimal | None = None
        if estimate is not None and estimate > 0:
            pct_universe = Decimal(known) / Decimal(estimate)
        per_state = _per_category_state(known, estimate, pct_universe)
        cats[category] = CategoryCoverage(
            known_filers=known,
            estimated_universe=estimate,
            pct_universe=pct_universe,
            state=per_state,
            # Honest machine flag (#1647): no real universe estimate ⇒ the
            # figure is a floor. A real seeded estimate==0 is NOT an estimate.
            is_estimate=estimate is None,
        )
    fold_state = _worst_of(cats[c].state for c in _CATEGORY_ORDER)
    return CoverageReport(state=fold_state, categories=cats)


def _per_category_state(known: int, estimate: int | None, pct_universe: Decimal | None) -> CoverageState:
    if estimate is None:
        return "unknown_universe"
    # ``estimate=0`` is a real seeded value distinct from NULL — it
    # means "we know the SEC universe for this category on this
    # instrument is empty" (e.g. an issuer with no expected 13F
    # filers). Treat as vacuously green: 0 known of 0 expected =
    # complete coverage. Claude PR review (PR 798) caught the prior
    # collapse to ``unknown_universe`` that lost this distinction.
    if estimate == 0:
        return "green"
    if pct_universe is None:
        return "unknown_universe"
    if pct_universe < Decimal("0.50"):
        return "red"
    if pct_universe < Decimal("0.80"):
        return "amber"
    return "green"


_STATE_RANK: dict[CoverageState, int] = {
    "no_data": 0,
    "red": 1,
    "unknown_universe": 2,
    "amber": 3,
    "green": 4,
}


def _worst_of(states: Iterable[CoverageState]) -> CoverageState:
    """Lower rank = worse. Default green when there are no
    categories (vacuously satisfied) — but the caller never reaches
    this branch with the no_data short-circuit."""
    worst: CoverageState | None = None
    for s in states:
        if worst is None or _STATE_RANK[s] < _STATE_RANK[worst]:
            worst = s
    return worst if worst is not None else "green"


def _banner_for_state(
    state: CoverageState,
    coverage: CoverageReport,
    pct_concentration: Decimal,
) -> BannerCopy:
    if state == "no_data":
        return BannerCopy(
            state="no_data",
            variant="error",
            headline="Cannot compute ownership",
            body=(
                "XBRL shares outstanding not on file. Trigger fundamentals sync, or wait for the next scheduled run."
            ),
        )
    unknown_cats = sorted(cat for cat, cov in coverage.categories.items() if cov.state == "unknown_universe")
    if state == "red":
        worst = _worst_named_category(coverage)
        return BannerCopy(
            state="red",
            variant="error",
            headline="Coverage incomplete",
            body=(
                f"Coverage incomplete in {worst.category} — do not use for "
                f"investment decisions. {worst.known_filers} of "
                f"{worst.estimated_universe} known filers in "
                f"{worst.category}; {len(unknown_cats)} categories without "
                f"an estimate."
            ),
        )
    if state == "unknown_universe":
        return BannerCopy(
            state="unknown_universe",
            variant="warning",
            headline="Coverage estimate not available",
            body=(
                f"Coverage estimate not available for "
                f"{', '.join(unknown_cats) or 'all categories'}. Known "
                f"filings represent {pct_concentration * 100:.2f}% of float. "
                f"Treat as best-effort — the filer universe for these "
                f"categories has not been estimated yet."
            ),
        )
    if state == "amber":
        worst = _worst_named_category(coverage)
        return BannerCopy(
            state="amber",
            variant="warning",
            headline="Limited coverage",
            body=(f"Limited coverage in {worst.category} — verify against SEC EDGAR for major positions."),
        )
    return BannerCopy(
        state="green",
        variant="success",
        headline="Coverage sufficient",
        body="Universe coverage ≥ 80% across all four categories.",
    )


@dataclass(frozen=True)
class _WorstNamed:
    category: str
    known_filers: int
    estimated_universe: int


def _worst_named_category(coverage: CoverageReport) -> _WorstNamed:
    """Pick the category in worst state with non-null estimate. Used
    only when the fold returned red/amber, so at least one such
    category exists by construction.

    Iterates categories in declaration order so two categories tied
    on state pick the first one (deterministic, matches the
    ``_CATEGORY_ORDER`` tuple)."""
    best_rank = max(_STATE_RANK.values()) + 1
    chosen: _WorstNamed | None = None
    for cat_name in _CATEGORY_ORDER:
        cov = coverage.categories.get(cat_name)
        if cov is None or cov.estimated_universe is None:
            continue
        rank = _STATE_RANK[cov.state]
        if rank < best_rank:
            best_rank = rank
            chosen = _WorstNamed(
                category=cat_name,
                known_filers=cov.known_filers,
                estimated_universe=cov.estimated_universe,
            )
    if chosen is None:
        # Defensive fallback — caller only invokes this on red/amber
        # which by construction means at least one named category
        # has a non-null estimate.
        return _WorstNamed(category="unknown", known_filers=0, estimated_universe=0)
    return chosen


# ---------------------------------------------------------------------------
# Inputs: shares_outstanding + treasury
# ---------------------------------------------------------------------------


_STALE_DENOMINATOR_MAX_AGE_DAYS: Final[int] = 548
"""~18 months. A covered issuer's cover-page share count refreshes every
10-Q / 10-K, so 18 months clears even an annual-only filer's
between-filings gap and never false-positives on a normally-ingested
instrument (panel AAPL/GOOG/GME/MSFT/JPM/HD are all < 80 days). A
denominator older than this means we are missing recent filings
(ingest-coverage gap) OR the issuer reports per-share-class counts only
and the newest un-dimensioned row is ancient (the #1581 dual-class trap —
BRK.B's is 2011) — both render nonsense percentages, so honest no_data
wins. Self-healing: a fresh row (< this) restores rendering. Tunable;
fleet blast radius recorded in the PR."""


def _denominator_too_stale(as_of: date | None, today: date) -> bool:
    """Pure staleness policy for the shares-outstanding denominator (#1581).

    ``as_of is None`` → ``False``: denominator absence is the caller's
    separate ``outstanding is None or <= 0`` short-circuit, not this
    guard. A future ``as_of`` (``today - as_of < 0``) is treated as
    not-stale — corrupt-future-``period_end`` handling is out of scope
    here, but pinned by a test so it cannot silently start bypassing the
    guard."""
    if as_of is None:
        return False
    return (today - as_of).days > _STALE_DENOMINATOR_MAX_AGE_DAYS


def _snapshot_today(conn: psycopg.Connection[Any]) -> date:
    """Transaction-stable "today" for the staleness clock.

    ``get_ownership_rollup`` runs inside a REPEATABLE READ snapshot
    (``snapshot_read``). A wall-clock ``datetime.now()`` would let two reads
    over the same snapshot disagree at the exact staleness boundary across a
    UTC-midnight rollover. ``CURRENT_TIMESTAMP`` is ``transaction_timestamp()``
    — fixed for the life of the transaction — so the verdict is consistent
    within the snapshot. ``AT TIME ZONE 'UTC'`` pins it to the UTC calendar
    date, matching the prior ``datetime.now(tz=UTC)`` semantics and the naive
    ``period_end`` dates we compare against."""
    with conn.cursor() as cur:
        cur.execute("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        row = cur.fetchone()
    assert row is not None  # SELECT of a constant always returns one row
    return row[0]


def _stale_denominator_banner(as_of: date) -> BannerCopy:
    """Honest ``no_data`` banner when a shares-outstanding row exists but
    is too old to use as a denominator (#1581).

    Cause-agnostic by design: the stale row may be the dual-class
    dimension-only trap (BRK.B's newest un-dimensioned count is 2011) OR
    an ingest-coverage gap — both produce nonsense percentages, so the
    copy states only what we know (the figure is stale), never why.
    Unlike the generic ``absent`` copy it does NOT tell the operator to
    trigger a fundamentals sync (which re-fetches the same ancient row for
    the dual-class case). State stays ``no_data``; only the copy differs."""
    as_of_txt = f"{as_of.day} {as_of:%b %Y}"  # en-GB short month, e.g. "29 Apr 2011"
    return BannerCopy(
        state="no_data",
        variant="error",
        headline="Cannot compute ownership",
        body=(
            f"Latest shares-outstanding on file ({as_of_txt}) is too stale to use as a "
            "denominator, so ownership percentages are suppressed rather than computed "
            "against an outdated share count. The breakdown returns once a current figure "
            "is on file."
        ),
    )


def _read_shares_outstanding(
    conn: psycopg.Connection[Any], instrument_id: int
) -> tuple[Decimal | None, date | None, SharesOutstandingSource]:
    """Latest XBRL DEI / us-gaap shares-outstanding figure with full
    provenance.

    The view ``instrument_share_count_latest`` (migration 052) gives
    the canonical latest value + DEI/us-gaap source taxonomy + period
    end. Batch 3 of #788 (#792) extends the payload with the source
    accession + form_type by re-querying ``financial_facts_raw`` for
    the row that produced the value. The view does the
    DEI > us-gaap precedence work; this function just enriches.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT latest_shares, as_of_date, source_taxonomy
            FROM instrument_share_count_latest
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None or row.get("latest_shares") is None:
        return None, None, SharesOutstandingSource(None, None, None, None)
    taxonomy = str(row["source_taxonomy"])
    concept = "EntityCommonStockSharesOutstanding" if taxonomy == "dei" else "CommonStockSharesOutstanding"
    as_of_date = row.get("as_of_date")
    # Pull the producing accession + form_type from
    # ``financial_facts_raw``. The view DISTINCT-ON's by
    # ``filed_date DESC, accession_number DESC``, so we pick the same
    # row by mirroring that ORDER BY here. The concept-name selection
    # is keyed by the view's ``source_taxonomy`` output.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, form_type, filed_date
            FROM financial_facts_raw
            WHERE instrument_id = %s
              AND concept = %s
              AND period_end = %s
            ORDER BY filed_date DESC, accession_number DESC
            LIMIT 1
            """,
            (instrument_id, concept, as_of_date),
        )
        prov_row = cur.fetchone()
    accession = str(prov_row["accession_number"]) if prov_row is not None else None
    return (
        Decimal(row["latest_shares"]),  # type: ignore[arg-type]
        as_of_date,  # type: ignore[arg-type]
        SharesOutstandingSource(
            accession_number=accession,
            concept=concept,
            form_type=(str(prov_row["form_type"]) if prov_row is not None else None),
            # Backend computes the archive URL once so the frontend can't
            # roll its own with the wrong EDGAR endpoint shape. Claude PR
            # 800 review caught the prior frontend ``filenum=`` URL —
            # ``filenum`` expects a SEC file number (e.g. 001-12345),
            # not an accession.
            edgar_url=edgar_archive_url(accession),
        ),
    )


def _dual_class_note(symbols: tuple[str, ...]) -> str:
    """Server-owned copy for the multi-class denominator caveat (#1646). Single
    source — the FE renders this verbatim, never re-derives the copy."""
    joined = ", ".join(symbols)
    return (
        f"Percentages use the issuer's combined all-class share count — "
        f"{joined} are separate share classes filed under one SEC CIK. The "
        f"per-class share count is not yet available, so each figure is a "
        f"combined-basis lower bound; true per-class concentration is higher."
    )


@dataclass(frozen=True)
class _ClassShareRow:
    """One ``instrument_class_shares_outstanding`` row (latest period for the
    instrument). Carries the FSDS provenance the read-path needs to synthesize a
    :class:`SharesOutstandingSource` and a :class:`PerClassDenominator`."""

    shares: Decimal
    period_end: date
    class_member: str
    source_cik: str
    source_adsh: str
    source_form_type: str
    source_fsds_qtr: str


def _read_class_shares_outstanding(conn: psycopg.Connection[Any], instrument_id: int) -> _ClassShareRow | None:
    """Latest verified per-class share count for this instrument (FSDS, sql/200),
    or None when no per-class row exists. The read-path applies it only behind the
    fail-closed guards in :func:`get_ownership_rollup`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shares, period_end, class_member, source_cik, source_adsh,
                   source_form_type, source_fsds_qtr
            FROM instrument_class_shares_outstanding
            WHERE instrument_id = %s
            ORDER BY period_end DESC
            LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return _ClassShareRow(
        shares=Decimal(row[0]),
        period_end=row[1],
        class_member=str(row[2]),
        source_cik=str(row[3]),
        source_adsh=str(row[4]),
        source_form_type=str(row[5]),
        source_fsds_qtr=str(row[6]),
    )


def class_shares_usable(
    *,
    class_shares: Decimal,
    class_period_end: date,
    combined_shares: Decimal,
    today: date,
) -> bool:
    """Freshness + structural gate on a verified FSDS per-class share count, shared
    by the ownership per-class denominator (:func:`_should_use_class_denominator`)
    and the per-class total-company market-cap path
    (``xbrl_derived_stats.compute_total_company_market_cap``, #1662). Single source
    of truth for the policy so the two consumers can never drift. Pure — table-tested
    without a DB.

      1. **Freshness coherence** — the per-class period clears the same staleness
         bound the combined denominator must clear (#1581
         ``_STALE_DENOMINATOR_MAX_AGE_DAYS`` = 548 days). Never multiply/divide a
         fresh figure by a STALE class count, but don't demand exact period-equality
         with the combined as_of — companyfacts (combined) updates a quarter ahead of
         DERA FSDS (per-class), so an equality gate would make the path essentially
         never fire. Bounding the per-class period by the repo's own settled
         denominator-freshness window is the principled middle (shares-outstanding
         drifts < a few % over that window).
      2. **Structural subset** — ``0 < class < combined``. A class is a strict subset
         of the combined all-class total when ≥2 classes exist; rejects a
         stale/garbage row ≥ combined."""
    return not _denominator_too_stale(class_period_end, today) and Decimal(0) < class_shares < combined_shares


def _should_use_class_denominator(
    *,
    class_shares: Decimal,
    class_period_end: date,
    combined_shares: Decimal,
    today: date,
    max_pie_holder_shares: Decimal,
) -> bool:
    """Fail-closed gate for swapping the combined denominator to a verified FSDS
    per-class count (#788). ALL must hold; any miss keeps the combined denominator
    + the #1646 caveat. Pure so the policy is table-tested without a DB.

    KEEP IN SYNC: ``docs/specs/etl/2026-06-17-per-class-shares-denominator.md`` §7
    step 5 describes these guards verbatim. If this signature or any condition
    changes, update that spec section in the SAME change (a v1 strict-equality
    coherence guard was relaxed here but the spec lagged → review BLOCKING; see
    review-prevention-log "Relaxing a guard mid-PR").

    Conditions 1–2 (freshness + structural subset) are :func:`class_shares_usable`;
    this adds:

      3. **Holdings-plausibility** — no resolved pie-wedge holder owns more shares
         than exist in the class (catches a mis-mapped too-small denominator, the
         %-inflating direction). Ownership-specific — the market-cap path has no pie
         holder, which is why this predicate is NOT in the shared
         :func:`class_shares_usable`."""
    return (
        class_shares_usable(
            class_shares=class_shares,
            class_period_end=class_period_end,
            combined_shares=combined_shares,
            today=today,
        )
        and max_pie_holder_shares <= class_shares
    )


def _humanize_class_member(class_member: str) -> str:
    """FSDS ``ClassOfStock`` localname → human label for user-facing copy. The
    localnames are XBRL technical strings (``CommonClassA``, ``CapitalClassC``,
    ``HeicoCommonStock``) — never render them verbatim (review WARNING). Maps the
    standard ``(Common|Capital|Preferred)Class<X>`` shape to ``Class X``; an
    issuer-specific localname (e.g. ``HeicoCommonStock``) falls back to a
    space-separated form (``Heico Common Stock``)."""
    m = re.fullmatch(r"(?:Common|Capital|Preferred)Class([A-Z])", class_member)
    if m is not None:
        return f"Class {m.group(1)}"
    # Fallback: split the CamelCase localname into spaced words.
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", class_member)


def _per_class_note(symbol: str, class_member: str, per_class_shares: Decimal) -> str:
    """Server-owned copy for the per-class denominator info callout (#788). Single
    source — the FE renders this verbatim, never re-derives the copy. Names the
    instrument by its ``symbol`` + a humanized class label, never the raw FSDS
    localname (review WARNING)."""
    millions = per_class_shares / Decimal(1_000_000)
    return (
        f"Percentages use the verified per-class share count for {symbol} "
        f"({_humanize_class_member(class_member)}; {millions:,.0f}M shares), not "
        f"the issuer's combined all-class count — so each figure is per-class-true."
    )


def _detect_dual_class_denominator(
    conn: psycopg.Connection[Any], instrument_id: int, *, denominator_concept: str | None
) -> DualClassDenominator | None:
    """Detect that this instrument is one traded class of a multi-class issuer
    (GOOG/GOOGL, HEI/HEI.A, METC/METCB) whose classes share one SEC CIK and whose
    rollup denominator is therefore the combined all-class count (#1646).

    Three complementary gates, ALL required — empirically zero false positives
    across the dev universe (fires on exactly GOOG/GOOGL, HEI/HEI.A, METC/METCB):

    1. **Denominator is the combined us-gaap count** (``denominator_concept ==
       'CommonStockSharesOutstanding'``). This is the multi-class fingerprint: a
       true multi-class issuer reports every per-class
       ``dei:EntityCommonStockSharesOutstanding`` cover value with a
       ``StatementClassOfStockAxis`` member, which the SEC companyfacts API strips,
       so the ``instrument_share_count_latest`` view falls back to the combined
       us-gaap ``CommonStockSharesOutstanding``. A ``dei`` denominator means one
       non-dimensional cover value was reported — a single-class issuer, an
       ETF/ETN trust, or a same-security ``.US`` dual-listing — none of which are
       understated. Excludes every false positive (ProShares ETFs, iPath ETNs,
       ``.US`` listing dups) by construction.
    2. **≥2 tradable instruments share the CIK as their PRIMARY mapping** — the
       multi-class structure exists in the live universe (data-engineer §Q15 /
       settled fan-out rule: two instruments on one SEC CIK are share classes).
       Restricting to ``is_primary`` CIK rows + ``is_tradable`` instruments stops a
       stale/historical CIK mapping or a delisted instrument from manufacturing a
       spurious sibling (Codex #1646 MED).
    3. **≥2 of those siblings each carry a CUSIP, across ≥2 distinct CUSIP values**
       — the siblings are genuinely different securities, not a same-security
       ``.US`` listing dup (which shares or lacks a CUSIP). Counting distinct
       *instruments-with-a-CUSIP* (not just distinct CUSIP values) stops one
       instrument's two historical CUSIPs from passing the gate against a
       CUSIP-less sibling (Codex #1646 LOW). Self-healing: an issuer with only one
       class's CUSIP ingested (Brown-Forman BF.A/BF-B today) starts firing once the
       second class's CUSIP lands.

    The precise per-class denominator rides on #1590 (DERA FSDS ``num.tsv``
    ``segments`` column); when that lands, this detector is superseded by a
    per-class lookup. Limitation: a multi-class issuer with only ONE class in the
    universe (BRK.B alone — BRK.A not ingested) cannot be caught by the sibling
    test, so its combined-basis figures stay silently understated until #1590."""
    # Gate 1: only the combined us-gaap denominator is class-blind.
    if denominator_concept != "CommonStockSharesOutstanding":
        return None
    with conn.cursor() as cur:
        # Canonical primary CIK for this instrument (§12.A canonical pick).
        cur.execute(
            """
            SELECT identifier_value
            FROM external_identifiers
            WHERE provider = 'sec' AND identifier_type = 'cik'
              AND instrument_id = %s
            ORDER BY is_primary DESC, external_identifier_id ASC
            LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cik = str(row[0])
        # Gate 2: tradable share-class siblings for which this CIK is their PRIMARY
        # mapping (incl. self). is_primary + is_tradable exclude a historical CIK
        # mapping or a delisted instrument from manufacturing a spurious sibling.
        cur.execute(
            """
            SELECT DISTINCT i.symbol
            FROM external_identifiers ei
            JOIN instruments i ON i.instrument_id = ei.instrument_id
            WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik'
              AND ei.identifier_value = %s
              AND ei.is_primary = TRUE
              AND i.is_tradable = TRUE
            ORDER BY i.symbol
            """,
            (cik,),
        )
        symbols = tuple(str(r[0]) for r in cur.fetchall())
        if len(symbols) < 2:
            return None
        # Gate 3: ≥2 of those siblings each carry a CUSIP, across ≥2 distinct CUSIP
        # values — genuinely separate securities, not a same-security listing dup
        # and not one instrument's two historical CUSIPs standing in for a sibling.
        cur.execute(
            """
            SELECT COUNT(DISTINCT cu.instrument_id), COUNT(DISTINCT cu.identifier_value)
            FROM external_identifiers cu
            WHERE cu.provider IN ('sec', 'openfigi')
              AND cu.identifier_type = 'cusip'
              AND cu.instrument_id IN (
                  SELECT ei.instrument_id
                  FROM external_identifiers ei
                  JOIN instruments i ON i.instrument_id = ei.instrument_id
                  WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik'
                    AND ei.identifier_value = %s
                    AND ei.is_primary = TRUE
                    AND i.is_tradable = TRUE
              )
            """,
            (cik,),
        )
        cusip_row = cur.fetchone()
        n_instruments_with_cusip = int(cusip_row[0])  # type: ignore[index]
        n_distinct_cusips = int(cusip_row[1])  # type: ignore[index]
    if n_instruments_with_cusip < 2 or n_distinct_cusips < 2:
        return None
    return DualClassDenominator(cik=cik, sibling_symbols=symbols, note=_dual_class_note(symbols))


# --- Denominator cross-check (#1647 part 5) -------------------------------------
# Single-class band on |pct_diff| between the two independent SEC concepts. 2% clears
# the inherent cover-page-vs-balance-sheet date skew (panel: AAPL 0.13%, JPM 0.62%);
# >5% is a real concern (the #1646 class: a 2x denominator error = 100%). Between =
# informative "minor_skew", not alarming (Codex ckpt-1 MED — 5%-agree was too loose).
_DENOM_CROSS_AGREE_TOL: Final[Decimal] = Decimal("0.02")
_DENOM_CROSS_DIVERGE_TOL: Final[Decimal] = Decimal("0.05")

# The two SEC shares-outstanding concepts the rollup uses as a denominator, each
# mapped to the OTHER (taxonomy, concept) for the independent tie-out.
_OPPOSITE_SHARES_CONCEPT: Final[dict[str, tuple[str, str]]] = {
    "CommonStockSharesOutstanding": ("dei", "EntityCommonStockSharesOutstanding"),
    "EntityCommonStockSharesOutstanding": ("us-gaap", "CommonStockSharesOutstanding"),
}


def _qualified_shares_concept(concept: str) -> str:
    """Prefix the bare concept name (as carried on ``SharesOutstandingSource``) with its
    taxonomy for the cross-check provenance field."""
    taxonomy = "dei" if concept == "EntityCommonStockSharesOutstanding" else "us-gaap"
    return f"{taxonomy}:{concept}"


def _humanize_shares_concept(concept: str) -> str:
    """Friendly label for the FE provenance caption."""
    if "EntityCommonStock" in concept:
        return "the SEC cover-page share count"
    if "CommonStockSharesOutstanding" in concept:
        return "the SEC balance-sheet share count"
    return concept


def _cross_check_note(
    method: str,
    status: str,
    pct_diff: Decimal,
    comparison_concept: str,
) -> str:
    """Server-owned copy for the FE provenance caption (single source)."""
    signed = f"{pct_diff * 100:+.2f}%"
    if method == "independent_concept":
        ref = _humanize_shares_concept(comparison_concept)
        if status == "agrees":
            return (
                f"Share-count denominator independently reconciled to {ref} (agrees within {abs(pct_diff) * 100:.2f}%)."
            )
        if status == "minor_skew":
            return (
                f"Denominator differs {signed} from {ref} — a cover-page vs "
                f"balance-sheet date skew, not a material disagreement."
            )
        return f"Denominator diverges {signed} from {ref} — the share-count figure is suspect."
    # per_class_subset_bound
    if status == "plausible":
        return (
            f"Resolved per-class share counts are a valid subset of the combined all-class total "
            f"({signed}); the untraded-class remainder is not independently verifiable."
        )
    return (
        "Resolved per-class share counts EXCEED the combined all-class total — "
        "an FSDS resolution error; per-class denominator is suspect."
    )


def _classify_cross_check(
    *,
    method: Literal["independent_concept", "per_class_subset_bound"],
    primary_value: Decimal | None,
    primary_concept: str,
    primary_as_of: date | None,
    comparison_value: Decimal | None,
    comparison_concept: str,
    comparison_as_of: date | None,
) -> DenominatorCrossCheck:
    """PURE (no DB) — given the two figures + method, compute the cross-check facts +
    status. Table-tested. Any missing / non-positive figure → ``unavailable`` (never
    divides; Codex ckpt-1 MED)."""
    if primary_value is None or primary_value <= 0 or comparison_value is None or comparison_value <= 0:
        return DenominatorCrossCheck.unavailable()
    pct_diff = (primary_value - comparison_value) / comparison_value
    as_of_delta = (
        abs((primary_as_of - comparison_as_of).days)
        if primary_as_of is not None and comparison_as_of is not None
        else None
    )
    status: Literal["agrees", "minor_skew", "diverges", "plausible"]
    if method == "independent_concept":
        magnitude = abs(pct_diff)
        if magnitude <= _DENOM_CROSS_AGREE_TOL:
            status = "agrees"
        elif magnitude <= _DENOM_CROSS_DIVERGE_TOL:
            status = "minor_skew"
        else:
            status = "diverges"
    else:
        # Dual-class subset bound: only the IMPOSSIBLE (traded classes exceed the
        # all-class total) diverges. The untraded-class remainder (Class B, no
        # instruments row) is unverifiable, so a valid subset is "plausible" — NO
        # arbitrary floor (Codex ckpt-1 BLOCKER: a material founder class / 49-51
        # split would falsely diverge; a wrong map at 55% would falsely agree).
        status = "diverges" if primary_value > comparison_value else "plausible"
    return DenominatorCrossCheck(
        method=method,
        primary_value=primary_value,
        primary_concept=primary_concept,
        comparison_value=comparison_value,
        comparison_concept=comparison_concept,
        primary_as_of=primary_as_of,
        comparison_as_of=comparison_as_of,
        as_of_delta_days=as_of_delta,
        pct_diff=pct_diff,
        status=status,
        note=_cross_check_note(method, status, pct_diff, comparison_concept),
    )


def _read_shares_outstanding_near(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    *,
    taxonomy: str,
    concept: str,
    near_period: date,
) -> tuple[Decimal, date] | None:
    """The ``financial_facts_raw`` shares row for (instrument, taxonomy, concept) whose
    ``period_end`` is NEAREST ``near_period`` — an exact same-period row sorts first at
    delta 0 (Codex ckpt-1 HIGH: nearest, NOT latest, so we never compare different
    instants). ``None`` when the concept is not on file for this instrument."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT val, period_end
            FROM financial_facts_raw
            WHERE instrument_id = %(iid)s
              AND taxonomy = %(tax)s
              AND concept = %(concept)s
              AND unit = 'shares'
              AND val IS NOT NULL
            ORDER BY ABS(period_end - %(near)s::date) ASC, filed_date DESC
            LIMIT 1
            """,
            {"iid": instrument_id, "tax": taxonomy, "concept": concept, "near": near_period},
        )
        row = cur.fetchone()
    if row is None or row.get("val") is None:
        return None
    return Decimal(row["val"]), row["period_end"]  # type: ignore[arg-type]


def _sum_sibling_class_shares(conn: psycopg.Connection[Any], source_cik: str, period_end: date) -> Decimal | None:
    """Σ of the per-class FSDS share count across every traded sibling sharing this
    issuer CIK, AT a single ``period_end`` (the dual-class subset-bound primary).

    Filtering to one instant is load-bearing: the sum is labelled with this
    ``period_end`` and compared to the combined us-gaap count at the same instant, so a
    sibling whose latest FSDS row is an OLDER quarter must not be mixed in at a stale
    figure (Codex ckpt-2 MED). The FSDS table PK is ``(instrument_id, period_end)`` so
    one row per sibling at this period — no DISTINCT ON needed. A sibling absent at this
    period simply drops out (the bound is over the classes we have at this instant).
    ``None`` when no rows / non-positive."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(shares), 0)
            FROM instrument_class_shares_outstanding
            WHERE source_cik = %(cik)s AND period_end = %(pe)s
            """,
            {"cik": source_cik, "pe": period_end},
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    total = Decimal(row[0])
    return total if total > 0 else None


def _cross_validate_denominator(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    *,
    effective_outstanding: Decimal | None,
    effective_as_of: date | None,
    effective_concept: str | None,
    per_class_denominator: PerClassDenominator | None,
) -> DenominatorCrossCheck:
    """Independent tie-out of the operative denominator (#1647 part 5). Reads the
    comparison figure(s) from the snapshot connection, then defers to the pure
    :func:`_classify_cross_check`. See :class:`DenominatorCrossCheck`."""
    if per_class_denominator is not None:
        # Dual-class: Σ resolved sibling per-class FSDS counts vs the combined all-class
        # us-gaap count at the SAME FSDS balance-sheet instant (exact-period match sorts
        # first; nearest fallback exposes the gap via as_of_delta_days).
        sibling_sum = _sum_sibling_class_shares(conn, per_class_denominator.cik, per_class_denominator.period_end)
        combined = _read_shares_outstanding_near(
            conn,
            instrument_id,
            taxonomy="us-gaap",
            concept="CommonStockSharesOutstanding",
            near_period=per_class_denominator.period_end,
        )
        if combined is None:
            return DenominatorCrossCheck.unavailable()
        combined_val, combined_as_of = combined
        return _classify_cross_check(
            method="per_class_subset_bound",
            primary_value=sibling_sum,
            primary_concept="sum of resolved per-class FSDS counts (sibling instruments)",
            primary_as_of=per_class_denominator.period_end,
            comparison_value=combined_val,
            comparison_concept="us-gaap:CommonStockSharesOutstanding (combined all-class)",
            comparison_as_of=combined_as_of,
        )
    # Single-class: the OTHER SEC shares concept, nearest the primary's period.
    if effective_outstanding is None or effective_outstanding <= 0 or effective_concept is None:
        return DenominatorCrossCheck.unavailable()
    opposite = _OPPOSITE_SHARES_CONCEPT.get(effective_concept)
    if opposite is None or effective_as_of is None:
        return DenominatorCrossCheck.unavailable()
    cmp_taxonomy, cmp_concept = opposite
    comparison = _read_shares_outstanding_near(
        conn,
        instrument_id,
        taxonomy=cmp_taxonomy,
        concept=cmp_concept,
        near_period=effective_as_of,
    )
    if comparison is None:
        return DenominatorCrossCheck.unavailable()
    cmp_val, cmp_as_of = comparison
    return _classify_cross_check(
        method="independent_concept",
        primary_value=effective_outstanding,
        primary_concept=_qualified_shares_concept(effective_concept),
        primary_as_of=effective_as_of,
        comparison_value=cmp_val,
        comparison_concept=f"{cmp_taxonomy}:{cmp_concept}",
        comparison_as_of=cmp_as_of,
    )


def _read_universe_estimates(conn: psycopg.Connection[Any], instrument_id: int) -> dict[str, int | None]:
    """Per-category universe estimates — INTENTIONALLY all-NULL.

    #790 (disposed 2026-06-17, committee verdict): do NOT revive a universe
    denominator here. A filer-count ``coverage_ratio`` is the WRONG metric — our
    internal DERA universe is known by construction (ratio ≈ 1), and an external
    SEC full-text-search count is size-correlated (+0.55) so it reintroduces the
    very market-cap bias it claims to measure; both render as misleading-green.
    The honest completeness story is already shipped elsewhere: as-of/quarter
    coherence (#1647 pt1), value-plausibility sanity checks (#1647 pt4), NT
    supersessions (#1639), history coverage (#1648). ``is_estimate=True`` /
    ``unknown_universe`` IS the truthful state — faking a denominator is the lie.
    The one real gap (size-debiasing the ownership LEVEL) is a ranking-engine
    signal tracked in #1660, not a rollup denominator. GitHub #790 is CLOSED."""
    return {
        "insiders": None,
        "blockholders": None,
        "institutions": None,
        "etfs": None,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def get_ownership_rollup(conn: psycopg.Connection[Any], symbol: str, instrument_id: int) -> OwnershipRollup:
    """Build the rollup payload for one instrument.

    The caller MUST already be inside :func:`app.db.snapshot.snapshot_read`
    so every read in this function lands on the same REPEATABLE READ
    snapshot. The function does NOT open its own transaction; doing
    so on a pooled connection that already has an implicit READ
    COMMITTED tx open would produce a SAVEPOINT instead of a fresh
    snapshot, with the isolation change silently ignored. Codex spec
    review caught the v1 spec attempting the inner-transaction
    anti-pattern.

    Reads from the per-source ``ownership_*_current`` snapshots built
    by Phase 1 (#840) write-through and primed by the
    ``ownership_observations_backfill`` job (#909). #905 retires the
    legacy typed-table readers (``_collect_canonical_holders_sql``,
    ``_read_treasury``) — the new ``_current`` tables are the single
    source of truth for the rollup. ``_enrich_and_union_def14a`` still
    runs the holder-name resolver against ``ownership_def14a_current``
    so the matched/unmatched routing is preserved.
    """
    outstanding, outstanding_as_of, outstanding_source = _read_shares_outstanding(conn, instrument_id)
    historical_symbols = tuple(historical_symbols_for(conn, instrument_id))
    # Drift chip (#966) is denominator-independent — read it BEFORE the
    # no_data short-circuits so a coverage-integrity signal doesn't vanish
    # exactly when the rollup is otherwise degraded.
    def14a_drift = _read_def14a_drift(conn, instrument_id)
    if outstanding is None or outstanding <= 0:
        return OwnershipRollup.no_data(
            symbol=symbol,
            instrument_id=instrument_id,
            historical_symbols=historical_symbols,
            def14a_drift=def14a_drift,
        )
    # A denominator many years stale produces nonsense percentages — a
    # single 13F holding renders >100% of "outstanding" (BRK.B: 124% off a
    # 2011 count). Suppress to honest no_data rather than compute against
    # it. See _denominator_too_stale / #1581.
    if _denominator_too_stale(outstanding_as_of, _snapshot_today(conn)):
        return OwnershipRollup.no_data(
            symbol=symbol,
            instrument_id=instrument_id,
            historical_symbols=historical_symbols,
            reason="stale_denominator",
            stale_as_of=outstanding_as_of,
            def14a_drift=def14a_drift,
        )
    treasury, treasury_as_of = _read_treasury_from_current(conn, instrument_id)
    sql_candidates = _collect_canonical_holders_from_current(conn, instrument_id)
    def14a_rows = _read_def14a_unmatched_from_current(conn, instrument_id)
    matched, unmatched_def14a = _enrich_and_union_def14a(conn, instrument_id, sql_candidates, def14a_rows=def14a_rows)
    # N-PORT mutual-fund holdings (#919). PK-deduped at the table level
    # so no cross-source dedup needed; lands in a memo-overlay slice
    # via _bucket_into_slices.
    funds_holders = _collect_funds_from_current(conn, instrument_id)
    # DEF-14A-disclosed ESOP / employee-benefit-plan holdings (#961). PK-deduped
    # at the table level; lands in its own memo-overlay slice via _bucket_into_slices.
    esop_holders = _collect_esop_from_current(conn, instrument_id)

    # Dedup in two stages: (1) per-source winner selection — Form 4 amendment
    # chains and 13D/G amendment chains each collapse to their latest filing;
    # (2) owner-identity reconciliation (#1640) — one beneficial owner is
    # counted ONCE across channels, classified by most-specific role, at their
    # total beneficial ownership (MAX of the Form 4 / DEF 14A / 13D/G
    # restatements; 13F managed assets excluded). This supersedes the #837/#788
    # P0b "show both as additive wedges" posture, whose premise (Cohen 38M
    # direct vs ~75M beneficial) the live data falsified (13D 36.85M ≈ Form 4
    # 38.35M — one stake). #837's intent is honored: the larger beneficial
    # figure is kept (via MAX) and the losing filing is preserved in
    # ``dropped_sources``, not discarded.
    # See docs/specs/etl/2026-06-15-ownership-owner-once-dedup.md.
    block_candidates = [c for c in matched if c.source in ("13d", "13g")]
    other_candidates = [c for c in matched if c.source not in ("13d", "13g")]

    survivors = _dedup_by_priority(other_candidates)
    blockholders = _dedup_within_source(block_candidates)
    # Institutional family identity (#1644 + #1649): collapse each curated manager
    # family (Vanguard, BlackRock, …) to ONE holder at MAX(Σ13F, proxy, 13G) BEFORE
    # owner-once, so the consolidated proxy 5%-holder figure neither double-counts
    # the 13F family sum (Vanguard) nor is dropped when it is the only complete
    # figure (BlackRock). Non-family holders pass through untouched. Also sanity-
    # rejects DEF 14A parser-garbage values. See the institutional-family spec.
    family_by_category, survivors, blockholders, unmatched_def14a, family_corrections = (
        _reconcile_institutional_families(survivors, blockholders, unmatched_def14a, outstanding)
    )
    # Insider control-group collapse (#1652): a sponsor's GP/LP chain Form-4s the SAME
    # deemed block under many related CIKs (and restates it on 13D/G), so the insiders
    # wedge explodes past 100% of float. Collapse the cross-channel union by EXACT
    # non-round value to ONE insiders holder before #1645 + owner-once, removing the
    # consumed rows from BOTH lists (nothing orphaned). Runs BEFORE #1645 so the control
    # group's 13D rows are consumed here; a purely-13D co-investor group (no insider
    # member) is left for #1645.
    # Same-accession control-group collapse (#1764): a joint Form 4/3 (or 13D/G) accession
    # reports the SAME deemed block under ≥2 distinct reporting owners (controlling person +
    # entity, or a fund chain). The fuzzy #1652 pass only fires ≥1M, so every sub-1M same-
    # accession dup leaks. Collapse the PRECISE same-accession signal first (no floor — the
    # shared accession IS the group evidence #1652/#1645 must infer); insiders restricted to
    # {form4,form3}, blockholders to {13d,13g}, never def14a (#1659 FP class).
    survivors, blockholders, same_accession_corrections = _reconcile_same_accession_groups(survivors, blockholders)
    survivors, blockholders, insider_group_corrections = _reconcile_insider_control_groups(survivors, blockholders)
    # 13D/G group collapse (#1645): a Rule 13d-5 group's members each report the
    # identical aggregate stake on separate accessions/CIKs and otherwise sum N× in
    # the blockholders wedge. Collapse a near-equal, same-period, non-round cluster to
    # ONE holder at MAX before owner-once. survivor_keys excludes any blockholder CIK
    # that owner-once will reconcile cross-channel (so its other-channel rows are not
    # orphaned). Computed AFTER the #1652 pass so a consumed control-group CIK (no longer
    # a survivor) is not wrongly excluded from this clustering.
    survivor_keys = frozenset(_identity_key(h.filer_cik, h.filer_name) for h in survivors)
    blockholders, group_corrections = _reconcile_13d_groups(blockholders, survivor_keys)
    by_category = _reconcile_owner_once(survivors + blockholders)
    for _cat, _fam_holders in family_by_category.items():
        by_category.setdefault(_cat, []).extend(_fam_holders)
    # Per-class denominator swap (#788): when a VERIFIED FSDS per-class share count
    # exists for this instrument and passes the fail-closed guards, divide by it so
    # a multi-class issuer's percentages are per-class-true, instead of the combined
    # all-class count + the #1646 caveat. Fail-closed: any guard miss keeps the
    # combined denominator + the caveat. Mutually exclusive with the #1646 caveat.
    effective_outstanding = outstanding
    effective_as_of = outstanding_as_of
    effective_source = outstanding_source
    per_class_denominator: PerClassDenominator | None = None
    dual_class_denominator: DualClassDenominator | None = None
    class_row = _read_class_shares_outstanding(conn, instrument_id)
    if class_row is not None:
        # Largest single pie-wedge holder. Only ADDITIVE pie wedges (the
        # by_category holders) count: funds (institution_subset) and DEF 14A /
        # esop (proxy_disclosure, #1659/#961) are non-additive memo overlays —
        # a deemed / overlapping figure must not veto the per-class denominator.
        # Denominator-independent.
        _pie_shares = [h.shares for _hs in by_category.values() for h in _hs]
        max_pie_holder_shares = max(_pie_shares, default=Decimal(0))
        if _should_use_class_denominator(
            class_shares=class_row.shares,
            class_period_end=class_row.period_end,
            combined_shares=outstanding,
            today=_snapshot_today(conn),
            max_pie_holder_shares=max_pie_holder_shares,
        ):
            effective_outstanding = class_row.shares
            effective_as_of = class_row.period_end
            # Synthesize the FSDS source — the reported source must NOT claim
            # companyfacts when the value came from FSDS (Codex ckpt-1 #1). The FSDS
            # accession yields a valid EDGAR archive URL.
            effective_source = SharesOutstandingSource(
                accession_number=class_row.source_adsh,
                concept="CommonStockSharesOutstanding",
                form_type=class_row.source_form_type,
                edgar_url=edgar_archive_url(class_row.source_adsh),
            )
            per_class_denominator = PerClassDenominator(
                cik=class_row.source_cik,
                class_member=class_row.class_member,
                period_end=class_row.period_end,
                per_class_shares=class_row.shares,
                combined_shares=outstanding,
                source_adsh=class_row.source_adsh,
                source_fsds_qtr=class_row.source_fsds_qtr,
                note=_per_class_note(symbol, class_row.class_member, class_row.shares),
            )
    # Only when we did NOT swap to a verified per-class denominator do we pay for
    # the #1646 detector — the dual-class issuers this feature targets pass the
    # guard, so the common case skips the extra DB round-trip (review WARNING).
    # Self-review for any future pre-swap lookup: is this query's result still
    # needed when the swap succeeds? If not, gate it like this.
    if per_class_denominator is None:
        dual_class_denominator = _detect_dual_class_denominator(
            conn, instrument_id, denominator_concept=outstanding_source.concept
        )
    slices = _bucket_into_slices(
        by_category,
        unmatched_def14a,
        effective_outstanding,
        funds_holders=funds_holders,
        esop_holders=esop_holders,
    )
    residual = _compute_residual(effective_outstanding, slices, treasury)
    concentration = _compute_concentration(effective_outstanding, slices)
    sanity = _compute_sanity(slices, effective_outstanding)
    # Independent denominator tie-out (#1647 part 5) — reads the comparison figure from
    # the same snapshot; facts only, never changes a share count. ``effective_*`` are the
    # post-swap denominator the rollup actually used; per_class set ⟹ dual-class path.
    denominator_cross_check = _cross_validate_denominator(
        conn,
        instrument_id,
        effective_outstanding=effective_outstanding,
        effective_as_of=effective_as_of,
        effective_concept=effective_source.concept,
        per_class_denominator=per_class_denominator,
    )
    estimates = _read_universe_estimates(conn, instrument_id)
    coverage = _compute_coverage(slices, estimates)
    banner = _banner_for_state(coverage.state, coverage, concentration.pct_outstanding_known)
    # 13F-NT supersession telemetry (#1639): the rows the institutions query
    # excluded, so the shrunk wedge + grown residual are explainable. Read from
    # the same snapshot as everything else (caller is inside snapshot_read).
    corrections_applied = (
        *_read_notice_suppressions(conn, instrument_id),
        *family_corrections,
        *same_accession_corrections,
        *insider_group_corrections,
        *group_corrections,
    )
    return OwnershipRollup(
        symbol=symbol,
        instrument_id=instrument_id,
        shares_outstanding=effective_outstanding,
        shares_outstanding_as_of=effective_as_of,
        shares_outstanding_source=effective_source,
        treasury_shares=treasury,
        treasury_as_of=treasury_as_of,
        slices=tuple(slices),
        residual=residual,
        concentration=concentration,
        coverage=coverage,
        banner=banner,
        historical_symbols=historical_symbols,
        corrections_applied=corrections_applied,
        # Per-class denominator (#788) and the #1646 multi-class caveat are
        # mutually exclusive — both computed in the swap block above. When a
        # verified FSDS per-class count was applied, ``per_class_denominator`` is
        # set and ``dual_class_denominator`` is None; otherwise the caveat fires.
        dual_class_denominator=dual_class_denominator,
        per_class_denominator=per_class_denominator,
        sanity=sanity,
        denominator_cross_check=denominator_cross_check,
        computed_at=datetime.now(tz=UTC),
        def14a_drift=def14a_drift,
    )


# ---------------------------------------------------------------------------
# CSV export of the canonical rollup (Chain 2.8 of #788)
# ---------------------------------------------------------------------------


_CSV_HEADER: tuple[str, ...] = (
    "filer_cik",
    "filer_name",
    "category",
    "shares",
    "pct_outstanding",
    "winning_source",
    "winning_accession",
    "as_of_date",
    "filer_type",
    "edgar_url",
)


def build_rollup_csv(rollup: OwnershipRollup) -> str:
    """Flatten a deduped :class:`OwnershipRollup` into a CSV string.

    One row per surviving holder across all slices, plus two memo
    rows at the end:

      * ``__treasury__`` — issuer treasury share count (additive
        wedge on the chart, not a deduped holder).
      * ``__residual__`` — ``Public / unattributed`` block (clamped
        to 0 when oversubscribed).

    The two memo rows let an operator sum the ``shares`` column and
    verify it equals ``shares_outstanding`` without round-tripping
    to a separate endpoint.

    Header always emitted so an automation pipe can be branchless on
    empty rollups (no_data state, pre-ingest instruments).

    Formula-injection guard: any cell value beginning with
    ``=``, ``+``, ``-``, ``@``, ``\\t``, or ``\\r`` is prefixed with a
    single quote. Mirrors the FE ``csvEscape`` rule and the existing
    insider-baseline CSV export.
    """
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(_CSV_HEADER)

    # Emit pie-wedge slices first so the additive-sum invariant holds
    # against (treasury_shares + residual.shares + Σ pie-wedge holders)
    # = shares_outstanding. Memo-overlay slices (funds, esop, future
    # DRS / short-interest) are emitted in a trailing block with the
    # ``__memo:<category>__`` prefix so spreadsheet consumers can
    # filter them OUT of any SUM(shares) reconciliation. Codex
    # pre-push review (#919) flagged the prior build_rollup_csv that
    # blindly iterated ``rollup.slices`` — emitting funds inline would
    # break the documented invariant by adding the memo-overlay total
    # to the additive sum.
    pie_slices = [s for s in rollup.slices if s.denominator_basis == "pie_wedge"]
    memo_slices = [s for s in rollup.slices if s.denominator_basis != "pie_wedge"]

    for slc in pie_slices:
        for holder in slc.holders:
            writer.writerow(
                [
                    _csv_safe(holder.filer_cik or ""),
                    _csv_safe(holder.filer_name),
                    slc.category,
                    str(holder.shares),
                    f"{holder.pct_outstanding}",
                    holder.winning_source,
                    _csv_safe(holder.winning_accession),
                    holder.as_of_date.isoformat() if holder.as_of_date is not None else "",
                    _csv_safe(holder.filer_type or ""),
                    _csv_safe(holder.winning_edgar_url or ""),
                ]
            )

    if rollup.treasury_shares is not None and rollup.treasury_shares > 0:
        treasury_pct = (
            f"{rollup.treasury_shares / rollup.shares_outstanding}"
            if rollup.shares_outstanding is not None and rollup.shares_outstanding > 0
            else ""
        )
        writer.writerow(
            [
                "",
                "Treasury (memo)",
                "__treasury__",
                str(rollup.treasury_shares),
                treasury_pct,
                "",
                "",
                rollup.treasury_as_of.isoformat() if rollup.treasury_as_of is not None else "",
                "",
                "",
            ]
        )

    writer.writerow(
        [
            "",
            "Public / unattributed",
            "__residual__",
            str(rollup.residual.shares),
            f"{rollup.residual.pct_outstanding}",
            "",
            "",
            "",
            "",
            "",
        ]
    )

    # Dropped-source provenance (#1640): when one beneficial owner is counted
    # once across channels, the losing filings (e.g. Cohen's 13D behind his
    # Form 4) become ``dropped_sources`` and would otherwise vanish from the
    # CSV audit. Emit them as ``__dropped:<source>__`` memo rows — excluded
    # from any SUM(shares) reconciliation (the sum invariant holds over the
    # pie-wedge rows + treasury + residual), but visible so an operator can see
    # the full filing trail behind a deduped owner.
    for slc in pie_slices:
        for holder in slc.holders:
            for dropped in holder.dropped_sources:
                writer.writerow(
                    [
                        _csv_safe(holder.filer_cik or ""),
                        _csv_safe(holder.filer_name),
                        f"__dropped:{dropped.source}__",
                        str(dropped.shares),
                        "",
                        dropped.source,
                        _csv_safe(dropped.accession_number),
                        dropped.as_of_date.isoformat() if dropped.as_of_date is not None else "",
                        "",
                        _csv_safe(dropped.edgar_url or ""),
                    ]
                )

    # Institutional family breakdown (#1644/#1649): a collapsed family
    # (e.g. "The Vanguard Group") shows ONE pie-wedge row at the family figure;
    # its constituent 13F sub-CIK holdings are carried as ``family_members``.
    # Emit them as ``__family_member:<family_winning_source>__`` memo rows so the
    # CSV keeps the sub-CIK breakdown the L2 table shows. Excluded from any
    # SUM(shares) reconciliation (the family figure already counts them once).
    for slc in pie_slices:
        for holder in slc.holders:
            for member in holder.family_members:
                writer.writerow(
                    [
                        _csv_safe(member.filer_cik or ""),
                        _csv_safe(member.filer_name),
                        f"__family_member:{holder.filer_name}__",
                        str(member.shares),
                        "",
                        member.source,
                        _csv_safe(member.accession_number),
                        member.as_of_date.isoformat() if member.as_of_date is not None else "",
                        "",
                        _csv_safe(member.edgar_url or ""),
                    ]
                )

    # 13F-NT supersession audit (#1639): a filer's stale 13F-HR was removed from
    # the institutions wedge because the filer filed a 13F-NT for a later
    # quarter — the removed shares flow into the residual. Emit one
    # ``__suppressed_by_13f_nt:<filer_cik>__`` memo row per correction so the
    # wedge shrink + residual growth are traceable. Excluded from any
    # SUM(shares) reconciliation (the sum invariant holds: removed shares land
    # in the residual). ``as_of_date`` = the superseded HR quarter; ``filer_type``
    # carries the winning Notice quarter so both quarters are visible.
    for corr in rollup.corrections_applied:
        if (
            corr.kind != "suppressed_by_13f_nt"
            or corr.superseded_period is None
            or corr.winning_nt_period is None
            or corr.winning_nt_accession is None
        ):
            continue
        writer.writerow(
            [
                _csv_safe(corr.filer_cik or ""),
                _csv_safe(corr.filer_name),
                f"__suppressed_by_13f_nt:{corr.filer_cik}__",
                str(corr.shares_removed),
                "",
                "13F-NT",
                _csv_safe(corr.winning_nt_accession),
                corr.superseded_period.isoformat(),
                f"nt_period={corr.winning_nt_period.isoformat()}",
                _csv_safe(edgar_archive_url(corr.winning_nt_accession) or ""),
            ]
        )

    # Institutional family fold audit (#1644/#1649): a manager family's losing
    # channel (proxy restating the 13F sum, or a 13F shell folded under a larger
    # consolidated proxy/13G) — emit one ``__family_fold:<source_channel>__`` memo
    # row per correction so the institutions figure change is traceable. Excluded
    # from any SUM(shares) reconciliation (the folded shares are restatements; the
    # family figure counts the owner once). The folded channel ALSO appears as a
    # ``__dropped:<source>__`` row; this memo adds the family + winning-channel why.
    for corr in rollup.corrections_applied:
        if corr.kind not in ("def14a_restates_institution", "institutional_family_collapse"):
            continue
        writer.writerow(
            [
                _csv_safe(corr.filer_cik or ""),
                _csv_safe(corr.filer_name),
                f"__family_fold:{corr.source_channel or 'unknown'}__",
                str(corr.shares_removed),
                "",
                _csv_safe(corr.source_channel or ""),
                _csv_safe(corr.winning_accession or ""),
                "",
                _csv_safe(corr.detail),
                "",
            ]
        )

    # 13D/G group collapse audit (#1645): a Rule 13d-5 group's members each reported
    # the same aggregate stake; the group is counted once at MAX and the others
    # folded. Emit one ``__group_collapse:<rep_cik>__`` memo row per correction so the
    # blockholders shrink is traceable. Excluded from any SUM(shares) reconciliation
    # (the folded shares were double-counts that were never real). The folded members
    # ALSO appear as ``__dropped:13d/g__`` rows; this memo adds the group + rep why.
    for corr in rollup.corrections_applied:
        if corr.kind != "blockholder_group_collapse":
            continue
        writer.writerow(
            [
                _csv_safe(corr.filer_cik or ""),
                _csv_safe(corr.filer_name),
                f"__group_collapse:{corr.filer_cik or 'unknown'}__",
                str(corr.shares_removed),
                "",
                _csv_safe(corr.source_channel or ""),
                _csv_safe(corr.winning_accession or ""),
                "",
                _csv_safe(corr.detail),
                "",
            ]
        )

    # Insider control-group collapse audit (#1652): a sponsor's GP/LP chain reported the
    # same deemed block under many related CIKs across Form 4 / 13D / 13G; counted once
    # in the insiders wedge, the others folded. Emit one ``__insider_group_collapse:<rep_cik>__``
    # memo row per correction (folded member CIK+name+shares are in ``detail``). Excluded
    # from any SUM(shares) reconciliation (the folded shares were never real).
    for corr in rollup.corrections_applied:
        if corr.kind != "insider_control_group_collapse":
            continue
        writer.writerow(
            [
                _csv_safe(corr.filer_cik or ""),
                _csv_safe(corr.filer_name),
                f"__insider_group_collapse:{corr.filer_cik or 'unknown'}__",
                str(corr.shares_removed),
                "",
                _csv_safe(corr.source_channel or ""),
                _csv_safe(corr.winning_accession or ""),
                "",
                _csv_safe(corr.detail),
                "",
            ]
        )

    # Multi-class denominator caveat (#1646): one ``__dual_class_denominator__``
    # memo row so the export self-documents that every ``pct_outstanding`` above
    # is a combined-basis lower bound (the denominator is the issuer's combined
    # all-class count; per-class is not yet ingested). Zero shares — it removes
    # nothing — so it is inert under any SUM(shares) reconciliation.
    if rollup.dual_class_denominator is not None:
        dc = rollup.dual_class_denominator
        writer.writerow(
            [
                _csv_safe(dc.cik),
                _csv_safe(", ".join(dc.sibling_symbols)),
                "__dual_class_denominator__",
                "0",
                "",
                "",
                "",
                "",
                _csv_safe(dc.note),
                "",
            ]
        )

    # Per-class denominator (#788): one ``__per_class_denominator__`` memo row when
    # a verified FSDS per-class share count was applied (mutually exclusive with the
    # caveat above), so the export self-documents that ``pct_outstanding`` is
    # per-class-true and records the provenance. Zero shares — inert under any
    # SUM(shares) reconciliation.
    if rollup.per_class_denominator is not None:
        pc = rollup.per_class_denominator
        writer.writerow(
            [
                _csv_safe(pc.cik),
                _csv_safe(pc.class_member),
                "__per_class_denominator__",
                "0",
                "",
                "",
                _csv_safe(pc.source_adsh),
                pc.period_end.isoformat(),
                "",
                _csv_safe(pc.note),
            ]
        )

    # Denominator cross-check (#1647 pt5): one ``__denominator_cross_check__`` memo row
    # so the export self-documents the independent denominator tie-out (status + the two
    # figures' concepts + pct_diff). Zero shares — inert under any SUM(shares) recon.
    # Skipped on the ``unavailable`` path (no comparison figure → nothing to record).
    dcc = rollup.denominator_cross_check
    if dcc.method != "unavailable":
        writer.writerow(
            [
                "",
                _csv_safe(dcc.method),
                "__denominator_cross_check__",
                "0",
                f"{dcc.pct_diff}" if dcc.pct_diff is not None else "",
                _csv_safe(dcc.status),
                _csv_safe(dcc.comparison_concept or ""),
                dcc.comparison_as_of.isoformat() if dcc.comparison_as_of is not None else "",
                _csv_safe(dcc.primary_concept or ""),
                _csv_safe(dcc.note),
            ]
        )

    # Memo-overlay slices land AFTER the residual row. Each row's
    # ``category`` column carries the ``__memo:<original_category>__``
    # prefix so spreadsheet consumers know to filter them OUT of any
    # SUM(shares) reconciliation. Per #919 these are fund-level detail
    # of holdings already counted in pie-wedge slices via 13F-HR;
    # additive accounting would double-count.
    for slc in memo_slices:
        for holder in slc.holders:
            writer.writerow(
                [
                    _csv_safe(holder.filer_cik or ""),
                    _csv_safe(holder.filer_name),
                    f"__memo:{slc.category}__",
                    str(holder.shares),
                    f"{holder.pct_outstanding}",
                    holder.winning_source,
                    _csv_safe(holder.winning_accession),
                    holder.as_of_date.isoformat() if holder.as_of_date is not None else "",
                    _csv_safe(holder.filer_type or ""),
                    _csv_safe(holder.winning_edgar_url or ""),
                ]
            )
    return buf.getvalue()


def _csv_safe(value: str) -> str:
    """Formula-injection guard. ``csv.writer`` already handles RFC
    4180 quoting; this function only guards against spreadsheet
    formula triggers.

    Mirrors the frontend ``csvEscape`` rule + the existing
    insider-baseline CSV. Codex pre-push review caught the prior
    implementation which forgot ``\\t`` / ``\\r`` (Excel treats both
    as formula triggers in addition to ``=+-@``)."""
    if value and value[0] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value
    return value
