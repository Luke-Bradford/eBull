"""#3621 slice 5a: store the FINRA payloads the premise read from the CDN, raw only.

The payload manifest ``docs/research/3621-si-premise-payloads.csv`` pins each settlement's sha256. Every row whose
payload is not stored in ``filing_raw_documents`` (kind ``finra_short_interest_csv``, accession ``FINRA_SI_<YYYYMMDD>``)
is fetched from FINRA's CDN, refused unless its sha256 equals the manifest's, and written through
``raw_filings.store_raw`` exactly as ``finra_short_interest_refresh`` writes it. No observations or manifest row is
written. A stored payload is never overwritten: one whose sha256 differs from the manifest's refuses the run.

Usage: ``PYTHONPATH=. uv run python -m scripts.capture_3621_si_payloads [--apply]`` (default: dry run).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from datetime import date
from pathlib import Path

import psycopg

from app.config import settings
from app.providers.implementations.finra_short_interest import FinraShortInterestProvider
from app.services.raw_filings import store_raw, stored_body
from app.services.short_interest_flag import DOCUMENT_KIND, accession_for

MANIFEST = Path(__file__).resolve().parents[1] / "docs" / "research" / "3621-si-premise-payloads.csv"


class CaptureError(RuntimeError):
    pass


def main(apply: bool) -> None:
    with MANIFEST.open() as f:
        pinned = {date.fromisoformat(r["settlement_date"]): r["sha256"] for r in csv.DictReader(f)}
    provider = FinraShortInterestProvider()
    with psycopg.connect(settings.database_url) as conn:
        missing: list[date] = []
        for day, sha in sorted(pinned.items()):
            body = stored_body(conn, accession_number=accession_for(day), document_kind=DOCUMENT_KIND)
            if body is None:
                missing.append(day)
            elif hashlib.sha256(body.encode("utf-8")).hexdigest() != sha:
                raise CaptureError(f"{day}: stored payload sha256 differs from the manifest's {sha}")
        print(f"manifest {len(pinned)} payloads; stored and matching {len(pinned) - len(missing)}; missing {missing}")
        for day in missing:
            payload = provider.fetch_settlement_file(day)
            digest = hashlib.sha256(payload).hexdigest()
            if digest != pinned[day]:
                raise CaptureError(f"{day}: CDN payload sha256 {digest} differs from the manifest's {pinned[day]}")
            print(f"{day}: CDN payload {len(payload)} bytes, sha256 {digest} matches")
            if apply:
                store_raw(
                    conn,
                    accession_number=accession_for(day),
                    document_kind=DOCUMENT_KIND,
                    payload=payload.decode("utf-8"),
                    source_url=provider.settlement_file_url(day),
                )
                conn.commit()
                body = stored_body(conn, accession_number=accession_for(day), document_kind=DOCUMENT_KIND)
                if body is None or hashlib.sha256(body.encode("utf-8")).hexdigest() != pinned[day]:
                    raise CaptureError(f"{day}: stored body does not read back to the manifest's sha256")
                print(f"{day}: stored as {accession_for(day)}, read back matches")
    if not apply:
        print("dry run: nothing written (pass --apply)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Store the FINRA payloads the #3621 premise read from the CDN.")
    parser.add_argument("--apply", action="store_true")
    main(parser.parse_args().apply)
