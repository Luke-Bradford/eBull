"""#3899 slice 2 acceptance (spec §6 items 1, 2 and 4): recipient A/B arms + panel dumps.

Spec: docs/specs/etl/2026-10-21-3899-def14a-pre14a-body-sweep.md.

Each arm runs ``def14a_recipients.compute_desired`` inside one writable transaction
that is rolled back (cover-cache and Item 403 cache writes vanish with it; the
production ledgers are never applied) and dumps the complete desired records, the
keep sets, the run report and quiescence markers taken at start and end:

    arm a  — the BASE code: run with PYTHONPATH pointing at a worktree of the base SHA
    arm b  — the build, cold Item 403 cache, live bodies
    arm c  — the build, cold cache, every live body re-fetched and verified against
             its own digest (acceptance-only adapter in place of ``verified_body``)
    arm d  — the build after the sweep, cold cache

    PYTHONPATH=<worktree> uv run python scripts/ab_3899_recipient_arms.py arm a --out a.json
    PYTHONPATH=. uv run python scripts/ab_3899_recipient_arms.py panel --out panel.json
    PYTHONPATH=. uv run python scripts/ab_3899_recipient_arms.py compare a.json b.json

SEC fetches go through the cross-process rate gate. Nothing here writes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, LiteralString

import psycopg
from psycopg import sql

from app.config import settings

PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")
FORCED_FETCHES: dict[str, int] = {"fetched": 0, "mismatch": 0, "not_found": 0}


def _markers(conn: psycopg.Connection[Any], ids: list[int]) -> dict[str, Any]:
    """What a concurrent writer would move: newest stored DEF 14A body, and a
    content hash of the population's holdings / observations / cover cache."""

    def digest(query: LiteralString) -> str:
        row = conn.execute(query, {"ids": ids}).fetchone()
        return hashlib.sha256(str(row).encode()).hexdigest()[:16]

    return {
        "max_def14a_fetched_at": str(
            conn.execute(
                "SELECT max(fetched_at) FROM filing_raw_documents WHERE document_kind = 'def14a_body'"
            ).fetchone()
        ),
        "holdings": digest(
            "SELECT count(*), md5(string_agg(h::text, '|' ORDER BY h::text)) FROM def14a_beneficial_holdings h"
            " WHERE instrument_id = ANY(%(ids)s)"
        ),
        "def14a_obs": digest(
            "SELECT count(*), md5(string_agg(o::text, '|' ORDER BY o::text)) FROM ownership_def14a_observations o"
            " WHERE instrument_id = ANY(%(ids)s) AND known_to IS NULL"
        ),
        "cover_cache": digest("SELECT count(*), max(fetched_at) FROM sec_cover_12b_fetches"),
    }


def _install_gate() -> Any:
    from psycopg_pool import ConnectionPool

    from app.providers.postgres_rate_gate import PostgresFloorGate
    from app.providers.rate_gate import SEC_MIN_REQUEST_INTERVAL_S
    from app.providers.sec_rate_gate_holder import set_sec_rate_gate

    pool = ConnectionPool(settings.database_url, min_size=1, max_size=1, kwargs={"application_name": "ab-3899"})
    set_sec_rate_gate(PostgresFloorGate(pool, budget="sec", floor_s=SEC_MIN_REQUEST_INTERVAL_S))
    return pool


def _force_fetch_adapter() -> None:
    """Arm c: a live body is re-fetched and compared to its OWN digest, so the
    fetch-and-verify path runs before anything is swept (Codex ckpt-1 round 3 #5)."""
    from app.services import def14a_recipients, raw_payload_retention
    from app.services.raw_payload_retention import VerifiedBody
    from app.services.sec_manifest import is_filing_index_url

    original = raw_payload_retention.verified_body

    def forced(conn: Any, *, accession_number: str, document_kind: Any, fetch_text: Any) -> VerifiedBody:
        row = conn.execute(
            "SELECT payload, source_url FROM filing_raw_documents WHERE accession_number = %s AND document_kind = %s",
            (accession_number, document_kind),
        ).fetchone()
        if row is None or row[0] is None or is_filing_index_url(row[1]):
            return original(conn, accession_number=accession_number, document_kind=document_kind, fetch_text=fetch_text)
        text = fetch_text(row[1])
        if not text:
            FORCED_FETCHES["not_found"] += 1
            return VerifiedBody(status="unavailable", reason="not_found")
        sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if sha != hashlib.sha256(str(row[0]).encode("utf-8")).hexdigest():
            FORCED_FETCHES["mismatch"] += 1
            return VerifiedBody(status="unavailable", reason="mismatch")
        FORCED_FETCHES["fetched"] += 1
        return VerifiedBody(status="fetched", text=text, sha256=sha)

    def14a_recipients.verified_body = forced  # type: ignore[assignment]


def run_arm(arm: str, out: Path) -> int:
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services import def14a_recipients as dr

    pool = _install_gate()
    if arm == "c":
        _force_fetch_adapter()
    with psycopg.connect(settings.database_url) as conn, SecFilingsProvider(user_agent=settings.sec_user_agent) as p:
        ids = [s.instrument_id for v in dr.load_population(conn).values() for s in v]
        start = _markers(conn, ids)
        if arm != "a":
            # Cold cache, rolled back below. Before the deploy applies sql/511 the table
            # does not exist yet: create it inside the same rolled-back transaction.
            conn.execute(Path("sql/511_def14a_item403_tables.sql").read_text())  # type: ignore[arg-type]
            conn.execute("DELETE FROM def14a_item403_tables")
        report = dr.RunReport()
        desired = dr.compute_desired(conn, p.fetch_document_text, report)
        end = _markers(conn, ids)
        conn.rollback()
    pool.close()  # an arm that raises never reaches here; the traceback is the evidence
    dump = {
        "arm": arm,
        "markers_start": start,
        "markers_end": end,
        "report": asdict(report),
        "accessions": {f"{k[0]}|{k[1]}": list(v) for k, v in sorted(desired.accessions.items())},
        "rows": {f"{k[0]}|{k[1]}|{k[2]}": list(v) for k, v in sorted(desired.rows.items())},
        "keep": sorted(f"{i}|{a}" for i, a in desired.keep),
        "keep_instruments": sorted(desired.keep_instruments),
        "forced_fetches": dict(FORCED_FETCHES) if arm == "c" else None,
    }
    out.write_text(json.dumps(dump, indent=1, default=str, sort_keys=True))
    print(
        f"arm {arm}: accessions={len(dump['accessions'])} rows={len(dump['rows'])} keep={len(dump['keep'])} "
        f"keep_instruments={len(dump['keep_instruments'])} report={dump['report']} stable={start == end} "
        f"forced_fetches={dump['forced_fetches']}"
    )
    return 0


def run_panel(out: Path) -> int:
    with psycopg.connect(settings.database_url) as conn:
        ids = dict(
            conn.execute(
                "SELECT symbol, instrument_id FROM instruments WHERE symbol = ANY(%s)", (list(PANEL),)
            ).fetchall()
        )
        dump: dict[str, Any] = {"instrument_ids": ids}
        tables: tuple[tuple[str, LiteralString], ...] = (
            ("def14a_beneficial_holdings", ""),
            ("def14a_exec_compensation", ""),
            ("ownership_def14a_observations", " AND known_to IS NULL"),
            ("ownership_esop_observations", " AND known_to IS NULL"),
            ("ownership_def14a_current", ""),
            ("ownership_esop_current", ""),
        )
        for table, extra in tables:
            rows = conn.execute(
                sql.SQL("SELECT to_jsonb(t) FROM {} t WHERE instrument_id = ANY(%s)").format(sql.Identifier(table))
                + sql.SQL(extra),
                (list(ids.values()),),
            ).fetchall()
            volatile = {
                "ingest_run_id",
                "known_from",
                "created_at",
                "updated_at",
                "refreshed_at",
                "observation_id",
                "id",
            }
            dump[table] = sorted(
                json.dumps({k: v for k, v in r[0].items() if k not in volatile}, sort_keys=True) for r in rows
            )
            print(f"{table}: {len(rows)} rows")
        dump["accessions"] = sorted(
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT accession_number FROM def14a_beneficial_holdings WHERE instrument_id = ANY(%s)"
                " UNION SELECT DISTINCT accession_number FROM def14a_exec_compensation WHERE instrument_id = ANY(%s)",
                (list(ids.values()), list(ids.values())),
            ).fetchall()
        )
    out.write_text(json.dumps(dump, indent=1, sort_keys=True))
    return 0


def compare(paths: list[Path]) -> int:
    dumps = [json.loads(p.read_text()) for p in paths]
    base = dumps[0]
    rc = 0
    for other in dumps[1:]:
        for key in ("accessions", "rows", "keep", "keep_instruments"):
            if base.get(key) != other.get(key):
                rc = 1
                a, b = base.get(key), other.get(key)
                if isinstance(a, dict) and isinstance(b, dict):
                    diff = sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
                else:
                    diff = sorted(set(a or []) ^ set(b or []))
                print(
                    f"{base.get('arm', paths[0].name)} vs {other.get('arm', '?')}: "
                    f"{key} differ on {len(diff)}: {diff[:20]}"
                )
        if "markers_start" in base and base["markers_start"] != other.get("markers_start"):
            print(
                f"markers moved between {base['arm']} and {other['arm']}: "
                f"{base['markers_start']} -> {other['markers_start']}"
            )
    print("IDENTICAL" if rc == 0 else "DIFFERENT")
    return rc


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("arm")
    a.add_argument("arm", choices=["a", "b", "c", "d"])
    a.add_argument("--out", type=Path, required=True)
    pnl = sub.add_parser("panel")
    pnl.add_argument("--out", type=Path, required=True)
    c = sub.add_parser("compare")
    c.add_argument("paths", type=Path, nargs="+")
    args = ap.parse_args(argv)
    if args.cmd == "arm":
        return run_arm(args.arm, args.out)
    if args.cmd == "panel":
        return run_panel(args.out)
    return compare(args.paths)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
