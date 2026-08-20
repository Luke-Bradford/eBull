"""#2701 — research ingest of the full insider span, retention cap lifted.

⚠ Uses `retention_cutoff_override`, which changes NO default. #1233's 3-year cap
stays in force for the operator-alerting path; this asks for the research
boundary explicitly. See the comment at the injection point.
"""

from __future__ import annotations

import logging
from datetime import date

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.sec_insider_dataset_ingest import ingest_insider_dataset_archive

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RESEARCH_CUTOFF = date(2006, 1, 1)  # the dataset's own first served quarter
bulk = resolve_data_dir() / "sec" / "bulk"
archives = sorted(p for p in bulk.glob("insider_*.zip"))
print(f"archives: {len(archives)}  cutoff={RESEARCH_CUTOFF}", flush=True)

written = skipped = unresolved = 0
with psycopg.connect(settings.database_url) as conn:
    from app.services.sec_insider_dataset_ingest import _load_cik_to_instrument

    cik_map = _load_cik_to_instrument(conn)
    for i, p in enumerate(archives, 1):
        r = ingest_insider_dataset_archive(
            conn=conn,
            archive_path=p,
            cik_to_instrument=cik_map,
            retention_cutoff_override=RESEARCH_CUTOFF,
        )
        conn.commit()
        written += r.rows_written
        skipped += r.rows_skipped_retention
        unresolved += r.rows_skipped_unresolved_cik
        print(
            f"[{i}/{len(archives)}] {p.name} written={r.rows_written} "
            f"retention_skipped={r.rows_skipped_retention} "
            f"unresolved={r.rows_skipped_unresolved_cik}",
            flush=True,
        )
print(f"TOTAL written={written} retention_skipped={skipped} unresolved_cik={unresolved}", flush=True)
print("RESEARCH INGEST DONE", flush=True)
