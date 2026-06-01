"""Quarterly full-index cross-quarter discovery (G12).

Plan: docs/superpowers/plans/2026-05-17-us-etl-completion.md §2 Phase 3.
Spec: docs/superpowers/specs/2026-05-17-g12-master-idx-quarterly-walker.md.

One scheduled fire walks the current calendar quarter AND the
immediately-previous calendar quarter by default. Callers can pass
an explicit ``quarters=[(year, q), ...]`` kwarg for the >1-quarter
outage backfill runbook (spec §3.1) or for tests.

Per-(year, quarter) failure is isolated: HTTP / parse / DB errors in
one quarter trigger ``conn.rollback()`` + ``QuarterStats(failed=True)``
and the loop continues to the next quarter. Successful quarters
``conn.commit()`` before the next iteration so already-walked work is
durably persisted before the next quarter's risk surface.

Subject resolution uses a preloaded universe map (one-shot SELECTs at
fire time, O(1) per-row lookup) instead of the per-row 3-table
``default_subject_resolver`` from Layer 1 / Layer 2. See spec §3.5.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.jobs.sec_atom_fast_lane import ResolvedSubject, SubjectResolver
from app.providers.implementations.sec_daily_index import HttpGet
from app.providers.implementations.sec_full_index import read_master_idx
from app.services.sec_manifest import record_manifest_entry

logger = logging.getLogger(__name__)


# #1415 — FILING-METADATA sources the bootstrap recent-window gap-close
# (``sec_master_idx_gap_close``) is allowed to seed + advance. EXCLUDES the
# bulk-dataset-sourced ownership families (sec_13f_hr / sec_n_port /
# sec_form3 / sec_form4 / sec_form5): their observations come from the
# quarterly bulk datasets (bootstrap S10/S11/S12), NOT the manifest worker,
# so a discovery-side watermark advance from this pass would push the
# steady-state cursor ahead of loaded data (silent gap, spec §4.3 pillar 3).
# DEF14A / 13D / 13G ARE included — the manifest worker parses their
# observations from the seeded manifest rows post-bootstrap, so seeding +
# advancing the watermark is eventually consistent (the worker fills the
# observation; the watermark only says "we know this filing exists").
# ``sec_n_csr`` is deliberately NOT included: N-CSR filers are fund TRUSTS,
# whose CIKs are not in ``build_preloaded_subject_resolver`` (issuers +
# institutional + blockholder only) until S26 ``mf_directory_sync`` seeds the
# fund directory — which runs AFTER this stage (order 15). N-CSR rows here
# would resolve as unknown-subject and never seed; N-CSR discovery is
# steady-state's job (post-S26 directory + Atom/daily-index), and N-CSR is
# not panel-relevant (funds slice = bulk N-PORT). The unscoped weekly G12
# sweep passes source_allowlist=None (full discovery).
GAP_CLOSE_FILING_METADATA_SOURCES: frozenset[str] = frozenset(
    {"sec_8k", "sec_10k", "sec_10q", "sec_def14a", "sec_13d", "sec_13g"}
)


@dataclass(frozen=True)
class QuarterStats:
    year: int
    quarter: int
    index_rows: int = 0
    matched_in_universe: int = 0
    upserted: int = 0
    skipped_unmapped_form: int = 0
    skipped_unknown_subject: int = 0
    failed: bool = False
    error_detail: str | None = None


@dataclass(frozen=True)
class MasterIdxSweepStats:
    quarters: list[QuarterStats] = field(default_factory=list)

    @property
    def total_upserted(self) -> int:
        return sum(q.upserted for q in self.quarters)

    @property
    def failed_quarters(self) -> int:
        return sum(1 for q in self.quarters if q.failed)


def _current_calendar_quarter(now: datetime) -> tuple[int, int]:
    """Return ``(year, quarter)`` for the UTC moment ``now``."""
    return now.year, (now.month - 1) // 3 + 1


def _previous_calendar_quarter(year: int, quarter: int) -> tuple[int, int]:
    """Return ``(year, quarter)`` for the calendar quarter immediately
    before ``(year, quarter)``."""
    if quarter == 1:
        return year - 1, 4
    return year, quarter - 1


def _quarters_to_walk(now: datetime) -> list[tuple[int, int]]:
    """Return ``[(current_year, current_q), (prev_year, prev_q)]``.

    Pure function — exercised against a fixed clock in tests so the
    Jan-1-rollover branch (CQ1 → CQ4-prev-year) is pinned.
    """
    cq = _current_calendar_quarter(now)
    return [cq, _previous_calendar_quarter(*cq)]


def build_preloaded_subject_resolver(
    conn: psycopg.Connection[Any],
) -> SubjectResolver:
    """Eager universe preload → O(1) closure resolver. See spec §3.5.

    Issues three SELECTs at fire time, materialises the union as
    ``dict[cik, ResolvedSubject]``, returns a ``(conn, cik) ->
    ResolvedSubject | None`` closure that runs in O(1).

    Resolution priority matches default_subject_resolver:
        issuer > institutional_filer > blockholder_filer.

    Memory profile: ~10k issuers + ~5k institutional filers +
    ~1-2k blockholder filers ≈ ~17k entries × ~80 bytes per
    ResolvedSubject ≈ 1.5 MB. Bounded.
    """
    universe: dict[str, ResolvedSubject] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT cik, instrument_id FROM instrument_sec_profile")
        for cik, instrument_id in cur.fetchall():
            universe[cik] = ResolvedSubject(
                subject_type="issuer",
                subject_id=str(int(instrument_id)),
                instrument_id=int(instrument_id),
            )
        cur.execute("SELECT cik FROM institutional_filers")
        for (cik,) in cur.fetchall():
            universe.setdefault(
                cik,
                ResolvedSubject(
                    subject_type="institutional_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
            )
        cur.execute("SELECT cik FROM blockholder_filers")
        for (cik,) in cur.fetchall():
            universe.setdefault(
                cik,
                ResolvedSubject(
                    subject_type="blockholder_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
            )

    logger.info(
        "master_idx sweep: preloaded universe size=%d (issuer / institutional_filer / blockholder_filer)",
        len(universe),
    )

    def _resolve(_conn: psycopg.Connection[Any], cik: str) -> ResolvedSubject | None:
        return universe.get(cik)

    return _resolve


def run_master_idx_quarterly_sweep(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    now: datetime | None = None,
    subject_resolver: SubjectResolver | None = None,
    quarters: Sequence[tuple[int, int]] | None = None,
    source_allowlist: frozenset[str] | None = None,
) -> MasterIdxSweepStats:
    """One quarterly-sweep cycle.

    Per-quarter commit / rollback isolation — see spec §3.2.

    ``quarters=None`` (default) walks ``[CQ, CQ-1]`` from ``now``.
    Pass an explicit ``[(year, q), ...]`` sequence for the >1-quarter
    outage backfill runbook (spec §3.1) — each pair MUST have year >=
    1993 (EDGAR full-index history start) and quarter in 1..4;
    out-of-range pairs propagate as a ``RuntimeError`` from
    ``read_master_idx`` and surface in ``QuarterStats(failed=True)``.

    ``subject_resolver=None`` (default) preloads the universe map via
    ``build_preloaded_subject_resolver``. Tests inject a pre-built
    resolver.

    ``source_allowlist=None`` (default) discovers EVERY mapped form (the
    steady-state G12 safety-net behaviour). The #1413/#1415 bootstrap
    gap-close passes a FILING-METADATA allowlist so the recent-window pass
    advances only filing-metadata watermarks (8-K/10-K/DEF14A/13D-G) and
    NEVER seeds a manifest/freshness row for a bulk-dataset ownership source
    (13F-HR/N-PORT/Form-3/4/5). Those observations come from the quarterly
    bulk datasets, not the manifest worker, so a discovery-side watermark
    advance would push the steady-state cursor ahead of loaded data (silent
    gap, spec §4.3 pillar 3). (N-CSR is also excluded — see
    ``GAP_CLOSE_FILING_METADATA_SOURCES`` for why.)
    """
    if now is None:
        now = datetime.now(tz=UTC)
    walk = list(quarters) if quarters else _quarters_to_walk(now)
    cq_year, cq_q = _current_calendar_quarter(now)
    resolver: SubjectResolver = (
        subject_resolver if subject_resolver is not None else build_preloaded_subject_resolver(conn)
    )

    quarter_stats: list[QuarterStats] = []
    for year, q in walk:
        index_rows = 0
        matched = 0
        upserted = 0
        skipped_unmapped = 0
        skipped_unknown = 0
        is_current = (year, q) == (cq_year, cq_q)

        try:
            for row in read_master_idx(http_get, year, q, allow_404=is_current):
                index_rows += 1
                if row.source is None:
                    skipped_unmapped += 1
                    continue
                # #1415 — bootstrap gap-close guard: skip any source outside
                # the filing-metadata allowlist so the recent-window pass
                # never seeds a manifest/freshness row for a bulk-dataset
                # ownership source (would advance its watermark ahead of the
                # parsed observations). Counted as skipped_unmapped (mapped
                # form, out of this sweep's scope).
                if source_allowlist is not None and row.source not in source_allowlist:
                    skipped_unmapped += 1
                    continue
                subject: ResolvedSubject | None = resolver(conn, row.cik)
                if subject is None:
                    skipped_unknown += 1
                    continue
                matched += 1
                try:
                    record_manifest_entry(
                        conn,
                        row.accession_number,
                        cik=row.cik,
                        form=row.form,
                        source=row.source,
                        subject_type=subject.subject_type,  # type: ignore[arg-type]
                        subject_id=subject.subject_id,
                        instrument_id=subject.instrument_id,
                        filed_at=row.filed_at,
                        accepted_at=row.accepted_at,
                        primary_document_url=row.primary_document_url,
                        is_amendment=row.is_amendment,
                    )
                    upserted += 1
                except ValueError as exc:
                    logger.warning(
                        "master_idx sweep %sQ%s: rejected accession=%s: %s",
                        year,
                        q,
                        row.accession_number,
                        exc,
                    )
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — per-quarter failure isolation
            logger.exception(
                "master_idx sweep %sQ%s: quarter failed; rolling back",
                year,
                q,
            )
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 — loop continues even if rollback chokes
                logger.exception(
                    "master_idx sweep %sQ%s: rollback raised; continuing",
                    year,
                    q,
                )
            quarter_stats.append(
                QuarterStats(
                    year=year,
                    quarter=q,
                    index_rows=index_rows,
                    matched_in_universe=matched,
                    upserted=0,  # rollback discarded any UPSERTs from this quarter
                    skipped_unmapped_form=skipped_unmapped,
                    skipped_unknown_subject=skipped_unknown,
                    failed=True,
                    error_detail=str(exc),
                )
            )
            continue

        logger.info(
            "master_idx sweep %sQ%s: index=%d matched=%d upserted=%d unmapped=%d unknown=%d",
            year,
            q,
            index_rows,
            matched,
            upserted,
            skipped_unmapped,
            skipped_unknown,
        )
        quarter_stats.append(
            QuarterStats(
                year=year,
                quarter=q,
                index_rows=index_rows,
                matched_in_universe=matched,
                upserted=upserted,
                skipped_unmapped_form=skipped_unmapped,
                skipped_unknown_subject=skipped_unknown,
            )
        )

    return MasterIdxSweepStats(quarters=quarter_stats)
