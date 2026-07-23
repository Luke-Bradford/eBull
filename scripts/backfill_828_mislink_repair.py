"""Backfill: repair owner-stream insider CIK mislinks (#828 PR-2).

Spec: docs/proposals/etl/2026-07-22-828-insider-cik-routing.md §PR-2.

Cohort is RECOMPUTED at run time (never a stored list): ``insider_filings``
rows whose stored ``instrument_id`` is outside the parsed issuer CIK's
non-empty production sibling set (``external_identifiers`` sec/cik — the
same resolver PR-1's writer routing uses). These are filings discovered
via the reporting OWNER's EDGAR stream: BAC Form 4s bound to BRK.B, CHTR
under LBRDA, PRME under GOOG.

Mechanism — snapshot-first, then refan through the LIVE apply chokepoint:

1. Snapshot every row the repair may mutate into ``mislink_828_snapshot``
   (filing_events non-sibling bindings, live non-sibling insider
   observations, mislinked ``insider_filings`` entity rows) —
   reversibility for the destructive fe DELETEs.
2. Per cohort accession: read the stored raw XML
   (``filing_raw_documents`` — 758/758 coverage verified 2026-07-22, no
   SEC fetch) and re-apply via the registered rewash spec
   (``rewash_filings._apply_form4`` / ``_apply_form3``), which invokes
   ``upsert_filing`` / ``upsert_form_3_filing`` with the STORED (wrong)
   instrument. Post-#828-PR-1 the chokepoint self-repairs every surface:
   entity rows move (ON CONFLICT ``instrument_id``), stale non-sibling
   observations tombstone (``known_to`` — I6 soft-delete; deliberate
   deviation from the spec's DELETE, the snapshot preserves audit),
   owner filing_events bindings delete + issuer-sibling rows upsert,
   ``refresh_insiders_current`` runs on both sides.
3. DEF 14A mislinks (1 binding full-pop, 2026-07-22 — DEFA14A
   0001193125-26-182734 bound to instrument 1048024; issuer sibling row
   already present; zero observation pollution): generic non-sibling fe
   DELETE, no parser involvement (proxies have no owner-stream code path
   per spec §PR-1 scope note).
4. Full-pop post-repair invariants (spec §PR-2 step 5) — the script
   FAILS (exit 1) if any is violated:
   - insider mislink count → 0
   - live non-sibling observations for the cohort → 0
   - non-sibling filing_events bindings for the cohort → 0 (insider + def14a)
   - issuer-sibling live observation coverage ≥ pre-repair count (no net loss)

Idempotent: a second run finds an empty cohort and exits after the
invariant checks.

Usage:
    PYTHONPATH=. uv run python scripts/backfill_828_mislink_repair.py --dry-run
    PYTHONPATH=. uv run python scripts/backfill_828_mislink_repair.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from datetime import UTC, datetime

import psycopg

# NOTE (#2110): a load-bearing side-effect pre-import of
# ``app.services.manifest_parsers`` stood here (the insider-module import
# cycle through the package init; 14/758 first-run failures on 2026-07-22).
# Dead since ``_classify`` moved to ``app.services.upsert_classify`` —
# import order is now free. Guarded by tests/test_import_order_regression.py.
from app.config import settings
from app.services import raw_filings, rewash_filings

logger = logging.getLogger("backfill_828")

_SIB_CTE = """
WITH sib AS (
    SELECT identifier_value AS cik,
           array_agg(instrument_id ORDER BY instrument_id) AS ids
    FROM external_identifiers
    WHERE provider = 'sec' AND identifier_type = 'cik'
    GROUP BY identifier_value
)
"""

# Insider cohort: stored entity instrument outside a non-empty sibling set.
_COHORT_SQL = (
    _SIB_CTE
    + """
SELECT f.accession_number, f.document_type, s.ids
FROM insider_filings f
JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
WHERE f.issuer_cik IS NOT NULL
  AND NOT (f.instrument_id = ANY(s.ids))
ORDER BY f.accession_number
"""
)

# DEF 14A mislinked fe bindings (issuer resolved via def14a_ingest_log).
_DEF14A_MISLINK_SQL = (
    _SIB_CTE
    + """
SELECT fe.filing_event_id, fe.provider_filing_id, fe.instrument_id
FROM def14a_ingest_log d
JOIN filing_events fe
  ON fe.provider = 'sec' AND fe.provider_filing_id = d.accession_number
JOIN sib s ON s.cik = lpad(btrim(d.issuer_cik), 10, '0')
WHERE d.issuer_cik IS NOT NULL
  AND NOT (fe.instrument_id = ANY(s.ids))
ORDER BY fe.provider_filing_id
"""
)

_KIND_BY_DOCTYPE = {
    "3": "form3_xml",
    "3/A": "form3_xml",
    "4": "form4_xml",
    "4/A": "form4_xml",
    "5": "form5_xml",
    "5/A": "form5_xml",
}


def _snapshot(conn: psycopg.Connection, run_label: str) -> dict[str, int]:
    """Audit-snapshot every row the repair may mutate. Returns per-kind counts."""
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mislink_828_snapshot (
                run_label      TEXT NOT NULL,
                kind           TEXT NOT NULL,
                snapshotted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                row_json       JSONB NOT NULL
            )
            """
        )
        cohort_cte = """
            , cohort AS (
                SELECT f.accession_number, s.ids
                FROM insider_filings f
                JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
                WHERE f.issuer_cik IS NOT NULL
                  AND NOT (f.instrument_id = ANY(s.ids))
            )
        """
        cur.execute(
            _SIB_CTE
            + cohort_cte
            + """
            INSERT INTO mislink_828_snapshot (run_label, kind, row_json)
            SELECT %(label)s, 'filing_events', to_jsonb(fe.*)
            FROM filing_events fe
            JOIN cohort c ON c.accession_number = fe.provider_filing_id
            WHERE fe.provider = 'sec' AND NOT (fe.instrument_id = ANY(c.ids))
            """,
            {"label": run_label},
        )
        counts["filing_events"] = cur.rowcount
        cur.execute(
            _SIB_CTE
            + cohort_cte
            + """
            INSERT INTO mislink_828_snapshot (run_label, kind, row_json)
            SELECT %(label)s, 'observations', to_jsonb(o.*)
            FROM ownership_insiders_observations o
            JOIN cohort c ON c.accession_number = o.source_accession
            WHERE o.known_to IS NULL AND NOT (o.instrument_id = ANY(c.ids))
            """,
            {"label": run_label},
        )
        counts["observations"] = cur.rowcount
        cur.execute(
            _SIB_CTE
            + """
            INSERT INTO mislink_828_snapshot (run_label, kind, row_json)
            SELECT %(label)s, 'entity', to_jsonb(f.*)
            FROM insider_filings f
            JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
            WHERE f.issuer_cik IS NOT NULL AND NOT (f.instrument_id = ANY(s.ids))
            """,
            {"label": run_label},
        )
        counts["entity"] = cur.rowcount
        cur.execute(
            _SIB_CTE
            + """
            INSERT INTO mislink_828_snapshot (run_label, kind, row_json)
            SELECT %(label)s, 'def14a_filing_events', to_jsonb(fe.*)
            FROM def14a_ingest_log d
            JOIN filing_events fe
              ON fe.provider = 'sec' AND fe.provider_filing_id = d.accession_number
            JOIN sib s ON s.cik = lpad(btrim(d.issuer_cik), 10, '0')
            WHERE d.issuer_cik IS NOT NULL AND NOT (fe.instrument_id = ANY(s.ids))
            """,
            {"label": run_label},
        )
        counts["def14a_filing_events"] = cur.rowcount
        # GLOBAL non-sibling fe strays — a superset of the cohort's: healthy
        # entity rows whose owner-stream discovery still wrote fe bindings
        # (issuer parsed first, owner stream walked later — the ordering
        # PR-1's writer guard now blocks; these are its historical
        # instances, 483 rows / 444 accessions full-pop 2026-07-22, every
        # one with issuer-sibling fe coverage already present).
        cur.execute(
            _SIB_CTE
            + """
            INSERT INTO mislink_828_snapshot (run_label, kind, row_json)
            SELECT %(label)s, 'filing_events_global', to_jsonb(fe.*)
            FROM filing_events fe
            JOIN insider_filings f ON f.accession_number = fe.provider_filing_id
            JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
            WHERE fe.provider = 'sec'
              AND f.issuer_cik IS NOT NULL
              AND NOT (fe.instrument_id = ANY(s.ids))
            """,
            {"label": run_label},
        )
        counts["filing_events_global"] = cur.rowcount
    conn.commit()
    return counts


def _global_fe_stray_repair(conn: psycopg.Connection) -> int:
    """DELETE every non-sibling filing_events binding for a routable-issuer
    insider accession — the owner-stream L2 pollution, whether or not the
    accession's ENTITY row was mislinked (Codex ckpt-2 on this script: the
    cohort-keyed check let healthy-entity strays hide). Snapshot kind
    ``filing_events_global`` captures these rows first.

    Live safety guard (PR #2111 review): a stray is deleted ONLY when an
    issuer-sibling fe row exists for the accession — self-enforcing, not
    doc-only (the 2026-07-22 run verified 444/444 had coverage; the guard
    makes that a standing property). A guard-blocked stray (no sibling
    coverage) is left in place and surfaces via the GLOBAL fe invariant."""
    with conn.cursor() as cur:
        cur.execute(
            _SIB_CTE
            + """
            DELETE FROM filing_events fe
            USING insider_filings f, sib s
            WHERE fe.provider = 'sec'
              AND f.accession_number = fe.provider_filing_id
              AND s.cik = lpad(btrim(f.issuer_cik), 10, '0')
              AND f.issuer_cik IS NOT NULL
              AND NOT (fe.instrument_id = ANY(s.ids))
              AND EXISTS (
                  SELECT 1 FROM filing_events fe2
                  WHERE fe2.provider = 'sec'
                    AND fe2.provider_filing_id = fe.provider_filing_id
                    AND fe2.instrument_id = ANY(s.ids)
              )
            """
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def _sibling_live_obs_count(conn: psycopg.Connection) -> int:
    """Live observation rows on ISSUER-SIBLING instruments for the current
    insider mislink cohort — the no-net-loss baseline (spec invariant 4)."""
    with conn.cursor() as cur:
        cur.execute(
            _SIB_CTE
            + """
            , cohort AS (
                SELECT f.accession_number, s.ids
                FROM insider_filings f
                JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
                WHERE f.issuer_cik IS NOT NULL
                  AND NOT (f.instrument_id = ANY(s.ids))
            )
            SELECT COUNT(*)
            FROM ownership_insiders_observations o
            JOIN cohort c ON c.accession_number = o.source_accession
            WHERE o.known_to IS NULL AND o.instrument_id = ANY(c.ids)
            """
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _invariants(conn: psycopg.Connection, *, pre_sibling_obs: int, cohort_accessions: list[str]) -> list[str]:
    """Full-pop post-repair invariants (spec §PR-2 step 5). Returns violations."""
    violations: list[str] = []
    with conn.cursor() as cur:
        cur.execute(_COHORT_SQL)
        residual = cur.fetchall()
        if residual:
            violations.append(f"insider mislink count != 0: {len(residual)} rows remain")

        # GLOBAL full-pop forms (Codex ckpt-2 on this script): keying these
        # on the captured cohort list would vacuously pass on an idempotent
        # re-run (empty cohort) and let healthy-entity strays hide.
        cur.execute(
            _SIB_CTE
            + """
            SELECT COUNT(*)
            FROM ownership_insiders_observations o
            JOIN insider_filings f ON f.accession_number = o.source_accession
            JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
            WHERE o.known_to IS NULL
              AND NOT (o.instrument_id = ANY(s.ids))
            """
        )
        row = cur.fetchone()
        n = int(row[0]) if row else 0
        if n:
            violations.append(f"GLOBAL live non-sibling observations != 0: {n}")

        cur.execute(
            _SIB_CTE
            + """
            SELECT COUNT(*)
            FROM filing_events fe
            JOIN insider_filings f ON f.accession_number = fe.provider_filing_id
            JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
            WHERE fe.provider = 'sec'
              AND f.issuer_cik IS NOT NULL
              AND NOT (fe.instrument_id = ANY(s.ids))
            """
        )
        row = cur.fetchone()
        n = int(row[0]) if row else 0
        if n:
            violations.append(f"GLOBAL non-sibling filing_events bindings != 0: {n}")

        cur.execute(_DEF14A_MISLINK_SQL)
        d = cur.fetchall()
        if d:
            violations.append(f"def14a non-sibling fe bindings != 0: {len(d)}")

        post_sibling_obs = _cohort_sibling_obs_for_accessions(conn, cohort_accessions)
        if post_sibling_obs < pre_sibling_obs:
            violations.append(
                f"issuer-sibling live obs coverage dropped: pre={pre_sibling_obs} post={post_sibling_obs}"
            )
        else:
            logger.info(
                "sibling obs coverage: pre=%d post=%d (no net loss)",
                pre_sibling_obs,
                post_sibling_obs,
            )
    return violations


def _cohort_sibling_obs_for_accessions(conn: psycopg.Connection, accessions: list[str]) -> int:
    """Post-repair twin of :func:`_sibling_live_obs_count` — the cohort no
    longer matches ``_COHORT_SQL`` after the rebind, so key on the captured
    accession list instead."""
    with conn.cursor() as cur:
        cur.execute(
            _SIB_CTE
            + """
            SELECT COUNT(*)
            FROM ownership_insiders_observations o
            JOIN insider_filings f ON f.accession_number = o.source_accession
            JOIN sib s ON s.cik = lpad(btrim(f.issuer_cik), 10, '0')
            WHERE o.source_accession = ANY(%(accs)s)
              AND o.known_to IS NULL
              AND o.instrument_id = ANY(s.ids)
            """,
            {"accs": accessions},
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report cohort + surfaces, write nothing")
    args = ap.parse_args()

    specs = rewash_filings.registered_specs()
    run_label = f"828-pr2-{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}"

    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(_COHORT_SQL)
            cohort = [(str(r[0]), str(r[1])) for r in cur.fetchall()]
            cur.execute(_DEF14A_MISLINK_SQL)
            def14a_rows = cur.fetchall()
        cohort_accessions = [acc for acc, _ in cohort]
        pre_sibling_obs = _sibling_live_obs_count(conn)
        logger.info(
            "cohort: %d insider accessions, %d def14a fe bindings, pre sibling obs=%d",
            len(cohort),
            len(def14a_rows),
            pre_sibling_obs,
        )
        if args.dry_run:
            return 0

        # Unconditional: the global fe-stray delete below must never run
        # without its rows snapshotted (zero-row snapshot inserts are
        # harmless no-ops on a clean DB).
        snap = _snapshot(conn, run_label)
        logger.info("snapshot %s: %s", run_label, snap)

        reparsed = 0
        failed = 0
        skipped = 0
        for accession, document_type in cohort:
            kind = _KIND_BY_DOCTYPE.get(document_type)
            if kind is None or kind not in specs:
                logger.warning("no rewash spec for accession=%s document_type=%s", accession, document_type)
                skipped += 1
                continue
            raw_doc = raw_filings.read_raw(conn, accession_number=accession, document_kind=kind)  # type: ignore[arg-type]
            if raw_doc is None or raw_doc.payload is None:
                logger.warning("raw body missing for accession=%s kind=%s", accession, kind)
                skipped += 1
                continue
            # 13/758 cohort rows store the FULL SGML submission
            # (``<SEC-DOCUMENT>`` wrapper — a legacy owner-side fetch hit the
            # .txt full-submission URL instead of the primary-doc XML). The
            # ownership XML is embedded verbatim; slice it out so the parsers
            # see the ``<ownershipDocument>`` root they expect. One-shot,
            # cohort-scoped — the live paths fetch the primary doc directly.
            body = raw_doc.payload
            if body.lstrip().startswith("<SEC-DOCUMENT>") and "<ownershipDocument" in body:
                start = body.index("<ownershipDocument")
                # First close AFTER start (not rindex): a submission with two
                # ownership documents must not produce a multi-root slice
                # (Codex ckpt-2).
                end = body.index("</ownershipDocument>", start) + len("</ownershipDocument>")
                raw_doc = replace(raw_doc, payload=body[start:end])
                logger.info("unwrapped SGML submission for accession=%s", accession)
            try:
                specs[kind].apply_fn(conn, raw_doc)  # type: ignore[index]
                conn.commit()
                reparsed += 1
            except psycopg.OperationalError, psycopg.InterfaceError:
                # Connection-level death: every subsequent apply would fail
                # too — abort loudly instead of logging 700 tracebacks
                # (PR #2111 review).
                logger.exception("connection failure at accession=%s — aborting sweep", accession)
                raise
            except Exception:  # noqa: BLE001 — single-accession failure must not abort the sweep
                logger.exception("apply failed for accession=%s kind=%s", accession, kind)
                conn.rollback()
                failed += 1

        # DEF 14A: pure fe-binding repair (no parser path — spec §PR-1 note).
        def14a_deleted = 0
        if def14a_rows:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM filing_events WHERE filing_event_id = ANY(%(ids)s)",
                    {"ids": [int(r[0]) for r in def14a_rows]},
                )
                def14a_deleted = cur.rowcount
            conn.commit()

        # Global fe strays (healthy-entity superset — see helper docstring).
        global_fe_deleted = _global_fe_stray_repair(conn)

        logger.info(
            "repair done: reparsed=%d failed=%d skipped=%d def14a_fe_deleted=%d global_fe_deleted=%d",
            reparsed,
            failed,
            skipped,
            def14a_deleted,
            global_fe_deleted,
        )

        violations = _invariants(conn, pre_sibling_obs=pre_sibling_obs, cohort_accessions=cohort_accessions)
        if violations:
            for v in violations:
                logger.error("INVARIANT VIOLATED: %s", v)
            return 1
        logger.info("all full-pop post-repair invariants hold")
        if failed or skipped:
            logger.warning("residuals: failed=%d skipped=%d — inspect logs before closing #828", failed, skipped)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
