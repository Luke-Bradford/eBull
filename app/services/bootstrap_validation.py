"""Bootstrap load-time validation gate (#1419, P4 of the bootstrap ETL redesign).

The terminal bootstrap stage. It runs after every data stage — gated in
``_STAGE_REQUIRES_CAPS`` on the bulk leaf caps plus ``ownership_current_refreshed``
(S24), since the dispatcher is capability-driven and ``stage_order`` does not
order execution — and performs three checks against the just-loaded data:

  1. **Absolute per-source row-count floors.** ``check_row_count_spike`` no-ops
     on a first install (it compares against the prior successful run, of which
     there is none), so this asserts ABSOLUTE presence instead. The floor
     VALUES here are conservative placeholders (every bulk-backed table must be
     non-empty); the §6 clean-bootstrap drive (P6) calibrates the real floors
     from the first clean run's baselines. Blockholders / DEF 14A current tables
     are deliberately NOT floored — they are manifest-worker / lazy-on-view
     driven and legitimately empty at completion (deferred slices, spec §4.4).
  2. **Per-slice-tolerant panel render.** ``get_ownership_rollup`` for the
     AAPL / GME / MSFT / JPM / HD panel; each must render (``banner.state`` not
     ``no_data`` and ``shares_outstanding`` present). Per-instrument tolerant:
     an instrument missing from the universe is a warning, not a failure, and
     the bulk-backed slices are what matters — blockholder / DEF 14A emptiness
     is expected.
  3. **Cross-source reconciliation (offline).** No live-SEC call at the
     bootstrap tail (rate budget + flakiness). Reconciles two independent
     sources the rollup already joins — 13F / Form 4 holdings vs XBRL DEI
     shares-outstanding — flagging gross oversubscription (a likely double-count
     / CUSIP-misresolution / wrong-shares-outstanding data bug).

**Verdict mapping (honours the rejected 'partial_complete' status).** A HARD
breach raises :class:`BootstrapValidationError`; ``_run_one_stage`` catches it
and calls ``mark_stage_error`` → ``finalize_run`` terminalises the run as
``partial_error`` (the universal bootstrap gate, #1064, stays closed). No new
status enum. Soft warnings → stage success + a verdict written to the
``bootstrap_runs.validation_gate_status`` column (sql/180): ``passed`` /
``warned`` / ``failed_<check_id>``. The column is the operator-facing reason;
the *gate* is the stage-error → ``partial_error`` path.

Spec: docs/proposals/etl/2026-06-01-bootstrap-etl-redesign-design.md §4.4.
Plan: docs/superpowers/plans/2026-06-01-bootstrap-etl-redesign.md Phase 4.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Final

import psycopg
import psycopg.rows
from psycopg import sql

from app.config import settings
from app.db.snapshot import snapshot_read
from app.services.ownership_rollup import OwnershipRollup, get_ownership_rollup
from app.services.processes.bootstrap_cancel_signal import active_bootstrap_context

logger = logging.getLogger(__name__)


class BootstrapValidationError(RuntimeError):
    """A hard validation-floor breach. Carries the failing ``check_id`` so the
    invoker can persist ``failed_<check_id>`` to ``validation_gate_status``
    before re-raising into ``_run_one_stage`` (→ stage error → partial_error).
    """

    def __init__(self, check_id: str, message: str) -> None:
        super().__init__(message)
        self.check_id = check_id


# --- Check 1: absolute row-count floors ------------------------------------
#
# PLACEHOLDER floors: every bulk-backed table must be non-empty (>0) after a
# successful bulk-only bootstrap. P6 (clean-bootstrap drive) replaces these with
# the first clean run's measured baselines (#1419 / plan Phase 6). Keep this a
# plain {table: int} knob — no hardcoded ratios.
#
# Scope rules (which tables are SAFE to hard-floor):
#  * The three observation tables + filing_events + financial_facts_raw are
#    written DIRECTLY by bulk stages whose caps validation requires (S8/S9/S10/
#    S11/S12) — guaranteed non-empty, no cross-stage ordering ambiguity.
#  * insiders/institutions/funds ``_current`` are refreshed by S24 FROM those
#    observation tables (S24's own required caps), so they too are safe.
#  * ``ownership_treasury_current`` is deliberately NOT floored: it is refreshed
#    by S24 FROM ``financial_periods``, which S25 ``fundamentals_sync`` produces —
#    but S24 runs before/parallel to S25 (no cap between them), so treasury
#    _current can be empty/stale at completion even on a healthy run. The panel
#    render check still exercises treasury as a rollup slice; a hard floor here
#    would false-fail. P6 re-scopes once treasury refresh is ordered after S25.
#  * ``ownership_blockholders_current`` / ``ownership_def14a_current`` are
#    deferred (manifest-worker / lazy-on-view) slices, legitimately empty at
#    completion (spec §4.4) — never floored.
_ROW_FLOORS: Final[dict[str, int]] = {
    "filing_events": 1,
    "financial_facts_raw": 1,
    "ownership_insiders_observations": 1,
    "ownership_institutions_observations": 1,
    "ownership_funds_observations": 1,
    "ownership_insiders_current": 1,
    "ownership_institutions_current": 1,
    "ownership_funds_current": 1,
}

# --- Check 2: panel render -------------------------------------------------
_PANEL: Final[tuple[str, ...]] = ("AAPL", "GME", "MSFT", "JPM", "HD")
# Minimum panel instruments that must fully render for the gate to pass.
# PLACEHOLDER 1 (catches total breakage without false-failing on one quirky
# instrument); P6 tightens toward len(_PANEL) once the clean run confirms all
# five render.
_MIN_PANEL_RENDERS: Final[int] = 1

# --- Check 3: cross-source reconciliation ----------------------------------
# ``pct_outstanding_known`` is a FRACTION (sum of deduped pie-wedge slice
# shares / shares_outstanding). Above this bound, known holders exceed the
# float so grossly that the cause is a data bug (double-count, CUSIP
# misresolution, or a wrong shares_outstanding) rather than the routine
# stale-13F-vs-fresh-XBRL skew (which only mildly oversubscribes and is a
# warning, via residual.oversubscribed).
_MAX_PCT_OUTSTANDING_KNOWN: Final[Decimal] = Decimal("1.50")


def run_bootstrap_validation() -> None:
    """Zero-arg bootstrap invoker (registered in ``app/jobs/runtime.py`` via
    ``_adapt_zero_arg``). Resolves the active run from the bootstrap contextvar,
    runs the three checks (floors on an autocommit conn for lock hygiene; the
    panel rollups in one snapshot_read), and persists the verdict. Raises
    :class:`BootstrapValidationError` on a hard breach so the stage terminalises
    as ``error`` (→ ``partial_error``).
    """
    run_id = _active_run_id()
    _persist_verdict(run_id, "pending")

    warnings: list[str] = []
    try:
        # AUTOCOMMIT so each floor count commits + releases its AccessShareLocks
        # immediately. The floor tables (financial_facts_raw +
        # ownership_*_observations) are partitioned ~125 ways each and an
        # unpruned read locks EVERY partition; holding the four together in one
        # transaction would reserve ~500 locks at once, near the
        # max_locks_per_transaction floor (1024, #1187). Releasing per count
        # keeps the peak at one table (~126). Nothing writes these tables
        # concurrently during validation — every data stage has terminalised
        # (cap gating) and the manifest worker + steady-state jobs are gated
        # until 'complete' — so per-statement reads are consistent.
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            _check_row_floors(conn)
            # The panel's five rollups share ONE snapshot_read: they read the
            # same tables (ownership_*_current + financial_facts_raw via the
            # share-count view), so the lock set is per-table not per-instrument
            # (~133 peak), and the slices reconcile against a single snapshot
            # like the ownership-rollup endpoint.
            rollups = _check_panel_render(conn, warnings)
            _check_cross_source(rollups, warnings)
    except BootstrapValidationError as exc:
        _persist_verdict(run_id, f"failed_{exc.check_id}")
        logger.error("bootstrap_validation: HARD FAIL (%s): %s", exc.check_id, exc)
        raise

    verdict = "warned" if warnings else "passed"
    _persist_verdict(run_id, verdict)
    if warnings:
        logger.warning(
            "bootstrap_validation: passed with %d warning(s): %s",
            len(warnings),
            "; ".join(warnings),
        )
    else:
        logger.info("bootstrap_validation: all checks passed (verdict=passed)")


def _check_row_floors(conn: psycopg.Connection[Any]) -> None:
    """Assert every bulk-backed table meets its absolute floor. Fail-fast on the
    first breach (the run is partial_error regardless of how many breach)."""
    for table, floor in _ROW_FLOORS.items():
        met, got = _count_at_least(conn, table, floor)
        if not met:
            raise BootstrapValidationError(
                "row_floor",
                f"row-count floor breach: {table} has {got} row(s), want >= {floor}",
            )
        logger.info("bootstrap_validation: row floor OK — %s >= %d", table, floor)


def _count_at_least(conn: psycopg.Connection[Any], table: str, floor: int) -> tuple[bool, int]:
    """Return ``(met, bounded_count)`` for ``table`` against ``floor``.

    Uses a LIMIT-bounded subquery so the scan stops at ``floor`` rows — a >0
    placeholder scans one row even on a 16M-row partitioned table, and the
    mechanism still answers "are there >= floor rows?" exactly for any calibrated
    floor. ``bounded_count == min(floor, total)``, so on a FAILURE
    (``total < floor``) it equals the true total — the error message reports the
    real shortfall. ``table`` comes only from the trusted ``_ROW_FLOORS`` keys;
    rendered via ``sql.Identifier`` (never string-interpolated) regardless.
    """
    query = sql.SQL("SELECT count(*) FROM (SELECT 1 FROM {} LIMIT %(lim)s) AS _bounded").format(sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(query, {"lim": floor})
        row = cur.fetchone()
    got = int(row[0]) if row is not None and row[0] is not None else 0
    return got >= floor, got


def _check_panel_render(conn: psycopg.Connection[Any], warnings: list[str]) -> list[tuple[str, OwnershipRollup]]:
    """Render the panel rollups. Returns ``[(symbol, rollup)]`` for instruments
    that fully render. Raises if fewer than ``_MIN_PANEL_RENDERS`` render.

    All five rollups read inside one ``snapshot_read`` (REPEATABLE READ) — the
    contract ``get_ownership_rollup`` documents, and the lock set is per-table
    (shared across instruments) so it stays bounded."""
    rendered: list[tuple[str, OwnershipRollup]] = []
    with snapshot_read(conn):
        for symbol in _PANEL:
            resolved = _resolve_instrument(conn, symbol)
            if resolved is None:
                warnings.append(f"panel: {symbol} not in universe (skipped)")
                continue
            instrument_id, canonical_symbol = resolved
            rollup = get_ownership_rollup(conn, symbol=canonical_symbol, instrument_id=instrument_id)
            if rollup.banner.state == "no_data" or rollup.shares_outstanding is None or rollup.shares_outstanding <= 0:
                warnings.append(
                    f"panel: {symbol} did not render "
                    f"(banner={rollup.banner.state}, shares_outstanding={rollup.shares_outstanding})"
                )
                continue
            rendered.append((symbol, rollup))

    if len(rendered) < _MIN_PANEL_RENDERS:
        raise BootstrapValidationError(
            "panel",
            f"panel render floor breach: {len(rendered)}/{len(_PANEL)} instruments rendered, "
            f"want >= {_MIN_PANEL_RENDERS}",
        )
    logger.info(
        "bootstrap_validation: panel render OK — %d/%d instruments rendered",
        len(rendered),
        len(_PANEL),
    )
    return rendered


def _resolve_instrument(conn: psycopg.Connection[Any], symbol: str) -> tuple[int, str] | None:
    """Resolve ``symbol`` → ``(instrument_id, canonical_symbol)`` (primary
    listing first), or ``None`` if not in the universe. Mirrors the
    ownership-rollup endpoint's resolution (``app/api/instruments.py``)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol.strip().upper()},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return int(row["instrument_id"]), str(row["symbol"])


def _check_cross_source(rollups: list[tuple[str, OwnershipRollup]], warnings: list[str]) -> None:
    """Offline cross-source reconciliation on the rendered panel: 13F / Form 4
    holdings vs XBRL DEI shares-outstanding. Gross oversubscription is a hard
    breach; mild oversubscription is a tolerated warning."""
    if not rollups:
        # Unreachable while _MIN_PANEL_RENDERS >= 1 (panel check would have
        # raised), but guard so a future relaxation can't silently skip.
        warnings.append("reconciliation: no rendered panel instruments to reconcile")
        return
    for symbol, rollup in rollups:
        pct_known = rollup.concentration.pct_outstanding_known
        if pct_known > _MAX_PCT_OUTSTANDING_KNOWN:
            raise BootstrapValidationError(
                "reconciliation",
                f"cross-source reconciliation breach: {symbol} known holders = "
                f"{pct_known * 100:.1f}% of shares_outstanding "
                f"(> {_MAX_PCT_OUTSTANDING_KNOWN * 100:.0f}%; likely double-count / "
                f"CUSIP misresolution / wrong shares_outstanding)",
            )
        if rollup.residual.oversubscribed:
            warnings.append(
                f"reconciliation: {symbol} mildly oversubscribed "
                f"(known holders + treasury exceed shares_outstanding — stale 13F vs fresh XBRL)"
            )
    logger.info("bootstrap_validation: cross-source reconciliation OK (%d rendered)", len(rollups))


def _active_run_id() -> int | None:
    """The in-flight bootstrap run_id from the orchestrator contextvar, or None
    (manual / test invocation outside ``active_bootstrap_run``)."""
    ctx = active_bootstrap_context()
    return ctx[0] if ctx is not None else None


def _persist_verdict(run_id: int | None, status: str) -> None:
    """Write the verdict to ``bootstrap_runs.validation_gate_status``. No-op when
    there is no active run (the column is informational; the gate is the
    stage-error path)."""
    if run_id is None:
        logger.warning("bootstrap_validation: no active run_id; verdict %r not persisted", status)
        return
    with psycopg.connect(settings.database_url) as conn:
        conn.execute(
            "UPDATE bootstrap_runs SET validation_gate_status = %(s)s WHERE id = %(id)s",
            {"s": status, "id": run_id},
        )
        conn.commit()
