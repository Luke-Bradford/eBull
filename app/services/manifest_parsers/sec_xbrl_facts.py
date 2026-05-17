"""sec_xbrl_facts manifest-worker parser — synth no-op adapter (G7).

XBRL company-facts data lands via the Company Facts API bulk path
(Stage 9 ``sec_companyfacts_ingest`` first-install + Stage 24 /
``JOB_FUNDAMENTALS_SYNC`` daily cron). ``sec_xbrl_facts`` manifest
rows exist for accession-level audit / discovery tracking; no
per-filing payload work is needed at the manifest dispatch layer.

This adapter exists to drain ``sec_filing_manifest`` rows for
``source='sec_xbrl_facts'`` cleanly so:

- ``/coverage/manifest-parsers`` reports ``has_registered_parser=True``
- ``WorkerStats.skipped_no_parser_by_source['sec_xbrl_facts']`` stays at 0
- Real lane-stuck conditions surface against a clean baseline

ParseOutcome contract — mirrors ``sec_10q`` synth no-op:

* ``status='parsed'`` — always. The manifest row's existence is the
  audit signal; the underlying XBRL facts have already been ingested
  via the Companyfacts JSON path.
* No ``tombstoned`` branch — there is no failure mode that requires
  permanent discard.
* No ``failed`` branch — there is no DB write that can raise; no
  fetch that can raise.

Raw-payload invariant (#938): registered with
``requires_raw_payload=False`` — synth source per sec-edgar §11.5.1.

Non-caller invariant: this module does NOT call
``SecFilingsProvider.fetch_document_text``. If a future PR introduces
an XBRL-facts manifest-dispatch consumer (e.g. structured-fact
extraction beyond the Companyfacts bulk JSON path), that PR must add
the fetcher + the ``tests/test_fetch_document_text_callers.py``
allow-list update + the SQL normalisation pathway in lockstep, per
the "Every structured field lands in SQL" prevention contract.

Pattern reference: sec-edgar §11.5.1 + ``sec_10q.py`` (#1168). This
module is the second adopter of the synth no-op pattern.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

_PARSER_VERSION_XBRL_FACTS = "xbrl-facts-noop-v1"


def _parse_sec_xbrl_facts(
    conn: psycopg.Connection[Any],  # noqa: ARG001 — synth no-op uses no DB
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Synth no-op: mark the row parsed without touching SEC or DB.

    Company facts already land via the Companyfacts bulk JSON path
    (Stage 9 bootstrap + Stage 24 / fundamentals_sync daily). The
    manifest row's accession-level audit is the only deliverable at
    the manifest dispatch layer.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    logger.debug(
        "sec_xbrl_facts manifest parser: synth no-op for accession=%s "
        "(XBRL facts land via Companyfacts bulk JSON path; no per-filing payload work)",
        row.accession_number,
    )
    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_XBRL_FACTS,
    )


def register() -> None:
    """Register the synth no-op parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. Called once
    from ``app.services.manifest_parsers.register_all_parsers`` at
    package import time, and re-callable from tests after a registry
    wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_xbrl_facts", _parse_sec_xbrl_facts, requires_raw_payload=False)
