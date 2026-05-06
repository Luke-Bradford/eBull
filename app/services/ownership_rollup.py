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

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.services.holder_name_resolver import resolve_holder_to_filer
from app.services.instrument_history import (
    SymbolHistoryEntry,
    historical_symbols_for,
)

# ---------------------------------------------------------------------------
# Public dataclasses (mirrored to Pydantic models in the API layer)
# ---------------------------------------------------------------------------


SliceCategory = Literal["insiders", "blockholders", "institutions", "etfs", "def14a_unmatched", "funds"]
SourceTag = Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"]
CoverageState = Literal["no_data", "red", "unknown_universe", "amber", "green"]

# Denominator-basis tag per spec
# `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md`
# §"Target chart decomposition". `pie_wedge` slices contribute to the
# residual / concentration math (sum to ≤ shares_outstanding). Memo
# overlays render as additional surface area without affecting the
# pie — used by the funds slice today (N-PORT rows are fund-level
# detail INSIDE the 13F-HR institutional aggregate; counting them
# additively would double-count). Future ESOP / DRS / short-interest
# overlays land here too (#961, etc.).
DenominatorBasis = Literal["pie_wedge", "institution_subset"]


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

    @classmethod
    def no_data(
        cls,
        symbol: str,
        instrument_id: int,
        historical_symbols: tuple[SymbolHistoryEntry, ...] = (),
    ) -> OwnershipRollup:
        """Empty payload for the ``no_data`` state (no XBRL outstanding
        on file). 200 OK with the red banner, not 503 — that way the
        frontend renders a uniform empty state and a sync-trigger
        CTA rather than crashing on a non-2xx response.

        ``historical_symbols`` is threaded through so the BBBY-style
        callout still renders on instruments missing
        ``shares_outstanding`` — that case is exactly when the
        operator wants the "filings before symbol change still belong
        here" hint. Codex pre-push review (Batch 7 of #788) caught
        the prior version dropping the chain on this path."""
        residual = ResidualBlock(
            shares=Decimal(0),
            pct_outstanding=Decimal(0),
            label="Public / unattributed",
            tooltip=_RESIDUAL_TOOLTIP,
            oversubscribed=False,
        )
        coverage = CoverageReport(state="no_data", categories={})
        banner = _banner_for_state("no_data", coverage, Decimal(0))
        return cls(
            symbol=symbol,
            instrument_id=instrument_id,
            shares_outstanding=None,
            shares_outstanding_as_of=None,
            shares_outstanding_source=SharesOutstandingSource(None, None, None, None),
            treasury_shares=None,
            treasury_as_of=None,
            slices=(),
            residual=residual,
            concentration=ConcentrationInfo(
                pct_outstanding_known=Decimal(0),
                info_chip="No shares-outstanding figure on file.",
            ),
            coverage=coverage,
            banner=banner,
            historical_symbols=historical_symbols,
            computed_at=datetime.now(tz=UTC),
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
        cur.execute(
            """
            SELECT holder_cik, holder_name, ownership_nature,
                   source, source_accession, shares, period_end
            FROM ownership_insiders_current
            WHERE instrument_id = %s
              AND shares IS NOT NULL
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
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT filer_cik, filer_name, filer_type, ownership_nature,
                   source, source_accession, shares, period_end
            FROM ownership_institutions_current
            WHERE instrument_id = %s
              AND shares IS NOT NULL
              AND exposure_kind = 'EQUITY'
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


def _bucket_into_slices(
    survivors: list[Holder],
    blockholders: list[Holder],
    unmatched_def14a: list[_Candidate],
    outstanding: Decimal,
    *,
    funds_holders: list[Holder] | None = None,
) -> list[OwnershipSlice]:
    """Split deduped survivors into slices by ``winning_source``
    (and ``filer_type`` for 13F).

    ``blockholders`` arrives pre-deduped from the parallel 13D/G
    pipeline (#837 split). Insiders / institutions / ETFs still come
    from cross-source priority dedup.

    ``funds_holders`` (#919) arrives pre-deduped from
    :func:`_collect_funds_from_current` (PK-deduped at table level)
    and lands in the funds memo-overlay slice. Renders as a separate
    slice but does NOT contribute to residual / concentration math —
    N-PORT rows are fund-level detail of holdings already aggregated
    in the institutions slice via 13F-HR, so additive accounting would
    double-count."""
    by_category: dict[SliceCategory, list[Holder]] = {
        "insiders": [],
        "blockholders": list(blockholders),
        "institutions": [],
        "etfs": [],
    }
    for h in survivors:
        if h.winning_source in ("form4", "form3", "def14a"):
            by_category["insiders"].append(h)
        elif h.winning_source in ("13d", "13g"):
            # Defensive: 13d/13g should now be partitioned out before
            # cross-source dedup. If one slips through, it still
            # surfaces in the blockholders slice rather than a
            # ValueError below.
            by_category["blockholders"].append(h)
        elif h.winning_source == "13f":
            if (h.filer_type or "").upper() == "ETF":
                by_category["etfs"].append(h)
            else:
                by_category["institutions"].append(h)
        else:
            raise ValueError(f"unknown winning_source {h.winning_source!r}")

    slices: list[OwnershipSlice] = []
    for category, holders in by_category.items():
        if not holders:
            continue
        slices.append(_build_slice(category, holders, outstanding))

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
        slices.append(_build_slice("def14a_unmatched", unmatched_holders, outstanding))

    if funds_holders:
        slices.append(
            _build_slice(
                "funds",
                list(funds_holders),
                outstanding,
                denominator_basis="institution_subset",
            )
        )
    return slices


def _build_slice(
    category: SliceCategory,
    holders: list[Holder],
    outstanding: Decimal,
    *,
    denominator_basis: DenominatorBasis = "pie_wedge",
) -> OwnershipSlice:
    holders.sort(key=lambda h: h.shares, reverse=True)
    total = sum((h.shares for h in holders), Decimal(0))
    pct_total = total / outstanding if outstanding > 0 else Decimal(0)
    sources: dict[SourceTag, Decimal] = {}
    for h in holders:
        sources[h.winning_source] = sources.get(h.winning_source, Decimal(0)) + h.shares
    dominant: SourceTag | None = None
    if sources:
        dominant = max(sources.keys(), key=lambda s: sources[s])
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
        )
        for h in holders
    )
    return OwnershipSlice(
        category=category,
        label=_SLICE_LABELS[category],
        total_shares=total,
        pct_outstanding=pct_total,
        filer_count=len(holders),
        dominant_source=dominant,
        holders=enriched_holders,
        denominator_basis=denominator_basis,
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
    # Memo-overlay slices (funds, future ESOP/DRS/short-interest) do NOT
    # contribute to ``sum_known`` — they describe positions that are
    # already counted via a pie-wedge slice (e.g. N-PORT funds are
    # fund-level detail inside the 13F-HR institutional aggregate).
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
    slices (funds, etc.) excluded so the chip doesn't double-count
    positions surfaced via both a pie wedge and an overlay."""
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
                f"Treat as best-effort until coverage expansion lands (#790)."
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


def _read_universe_estimates(conn: psycopg.Connection[Any], instrument_id: int) -> dict[str, int | None]:
    """Per-category universe estimates. Tier 0 returns NULL for every
    category — the per-instrument 13F filer-count ingest lands in
    #790 / Batch 2, after which institutions starts returning a
    real number. Other categories never get estimates in the
    visible roadmap; they stay NULL until a curated seed lands."""
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
    if outstanding is None or outstanding <= 0:
        return OwnershipRollup.no_data(
            symbol=symbol,
            instrument_id=instrument_id,
            historical_symbols=historical_symbols,
        )
    treasury, treasury_as_of = _read_treasury_from_current(conn, instrument_id)
    sql_candidates = _collect_canonical_holders_from_current(conn, instrument_id)
    def14a_rows = _read_def14a_unmatched_from_current(conn, instrument_id)
    matched, unmatched_def14a = _enrich_and_union_def14a(conn, instrument_id, sql_candidates, def14a_rows=def14a_rows)
    # N-PORT mutual-fund holdings (#919). PK-deduped at the table level
    # so no cross-source dedup needed; lands in a memo-overlay slice
    # via _bucket_into_slices.
    funds_holders = _collect_funds_from_current(conn, instrument_id)

    # Split 13D/G out of the cross-source priority dedup (#837 / #788
    # P0b). 13D/G reports BENEFICIAL ownership per Rule 13d-3
    # (direct + indirect via family trusts, control entities, funds),
    # while Form 4 reports DIRECT only. They are different facts; the
    # legacy single-chain dedup ``form4 > 13d/g`` discards the
    # beneficial figure entirely whenever the same CIK has any Form 4
    # — Cohen-on-GME is the canonical case (38M direct vs 75M
    # beneficial; only the 38M renders today). The full two-axis
    # ``source × ownership_nature`` dedup model lands in Phase 1
    # (#840); this PR is the immediate rollup-query patch so 13D/G
    # filings always surface in the blockholders slice with their
    # reported beneficial figure, regardless of any same-CIK Form 4.
    block_candidates = [c for c in matched if c.source in ("13d", "13g")]
    other_candidates = [c for c in matched if c.source not in ("13d", "13g")]

    survivors = _dedup_by_priority(other_candidates)
    blockholders = _dedup_within_source(block_candidates)
    slices = _bucket_into_slices(
        survivors,
        blockholders,
        unmatched_def14a,
        outstanding,
        funds_holders=funds_holders,
    )
    residual = _compute_residual(outstanding, slices, treasury)
    concentration = _compute_concentration(outstanding, slices)
    estimates = _read_universe_estimates(conn, instrument_id)
    coverage = _compute_coverage(slices, estimates)
    banner = _banner_for_state(coverage.state, coverage, concentration.pct_outstanding_known)
    return OwnershipRollup(
        symbol=symbol,
        instrument_id=instrument_id,
        shares_outstanding=outstanding,
        shares_outstanding_as_of=outstanding_as_of,
        shares_outstanding_source=outstanding_source,
        treasury_shares=treasury,
        treasury_as_of=treasury_as_of,
        slices=tuple(slices),
        residual=residual,
        concentration=concentration,
        coverage=coverage,
        banner=banner,
        historical_symbols=historical_symbols,
        computed_at=datetime.now(tz=UTC),
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
    # = shares_outstanding. Memo-overlay slices (funds, future ESOP /
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
