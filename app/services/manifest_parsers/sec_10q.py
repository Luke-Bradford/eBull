"""sec_10q manifest-worker parser — synth no-op adapter (#1168).

10-Q financial-statement data already lands via the Companyfacts XBRL
path (``fundamentals_sync`` daily cron + Stage 24 bootstrap). 10-Q
narrative HTML (MD&A, risk-factors, controls) has no operator-visible
consumer in v1. The manifest discovery row IS the audit signal for
this source; no per-filing payload work is needed.

This adapter exists to drain ``sec_filing_manifest`` rows for
``source='sec_10q'`` cleanly so:

- ``/coverage/manifest-parsers`` reports ``has_registered_parser=True``
- ``WorkerStats.skipped_no_parser_by_source['sec_10q']`` stays at 0
- Real lane-stuck conditions surface against a clean baseline

If a future PR introduces an MD&A / risk-factor extraction consumer,
that PR adds the fetcher + the
``tests/test_fetch_document_text_callers.py`` allow-list update + the
SQL normalisation pathway in lockstep, per the
"Every structured field lands in SQL" prevention contract.

ParseOutcome contract:

* ``status='parsed'`` — always. The manifest row's existence proves
  the filing was discovered; no further per-filing work is in scope.
* No ``tombstoned`` branch — there is no failure mode that requires
  permanent discard of the manifest row. The synth no-op does not
  consume URL or instrument_id.
* No ``failed`` branch — there is no DB write that can raise; there
  is no fetch that can raise.

Raw-payload invariant (#938): registered with
``requires_raw_payload=False`` — this is a synth source per
sec-edgar §11.5.1. The worker accepts ``parsed`` with
``raw_status=None``.

Pattern reference: sec-edgar §11.5.1 documents the "synth no-op
parser" as the canonical fix for sources whose SQL coverage is
complete via another path. This module is the canonical exemplar
for the pattern; ``sec_xbrl_facts`` and (if INFEASIBLE) ``sec_n_csr``
are eligible to adopt the same shape.

Codex pre-spec review:

* Round 1 BLOCKING ×3 against an earlier raw-fetch-no-op design
  (fetch_document_text allow-list violation; raw persistence
  redundant per prevention-log #470 since Companyfacts XBRL covers
  the SQL surface; raise_for_status() body-loss timing). All
  resolved by the pivot to true no-op in this module.
* Round 2 BLOCKING (durability test too weak) + 3 WARNING. All
  resolved in test shape — see
  ``tests/test_manifest_parser_sec_10q.py``.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

_PARSER_VERSION_10Q = "10q-noop-v1"


def _parse_sec_10q(
    conn: psycopg.Connection[Any],  # noqa: ARG001 — synth no-op uses no DB
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Synth no-op: mark the row parsed without touching SEC or DB.

    The manifest discovery row IS the audit. Financial data lands via
    Companyfacts XBRL; narrative HTML has no v1 consumer. Returning
    ``parsed`` lets the worker transition the row and drains the lane
    without burning fetch budget or writing redundant raw bytes.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    logger.debug(
        "sec_10q manifest parser: synth no-op for accession=%s "
        "(financial data lands via Companyfacts XBRL; no per-filing payload work in v1)",
        row.accession_number,
    )
    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_10Q,
    )


def register() -> None:
    """Register the synth no-op parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. Called once
    from ``app.services.manifest_parsers.register_all_parsers`` at
    package import time, and re-callable from tests after a registry
    wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_10q", _parse_sec_10q, requires_raw_payload=False)
