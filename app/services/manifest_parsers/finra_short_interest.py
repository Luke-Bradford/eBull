"""finra_short_interest manifest-worker parser — synth no-op (G6/#915).

FINRA Equity Short Interest data lands via the
``finra_short_interest_refresh`` ScheduledJob (daily 12:00 UTC). The
ScheduledJob owns the fetch + parse + UPSERT into
``finra_short_interest_observations`` + ``finra_short_interest_current``,
then UPSERTs the manifest row as ``ingest_status='parsed'`` directly
(inside the same JOB-owned ``with conn.transaction():`` so manifest
parsed-status atomically implies observations durable).

This parser exists only to satisfy the manifest-worker dispatch
invariant on the rare ``sec_rebuild --source=finra_short_interest``
path — if the operator flips a manifest row back to ``pending``, the
manifest worker needs a registered parser to mark it ``parsed`` again
WITHOUT triggering a network fetch or DB write. The actual re-ingest
mechanism is re-firing the ScheduledJob (which re-walks the revision
window + re-fetches the file).

Architectural sibling: ``sec_xbrl_facts.py`` (G7) — Companyfacts data
lands via the bulk JSON ScheduledJob path; XBRL manifest rows exist for
accession-level tracking only. Same shape here.

ParseOutcome contract — mirrors ``sec_xbrl_facts``:

* ``status='parsed'`` — always. The manifest row's existence is the
  audit signal; the underlying observations have already landed via
  the ScheduledJob path.
* ``parser_version='finra-si-bimonthly-v1'`` — unified with the
  ScheduledJob's write-side parser_version per Codex 1b r2 MED 3.
* No ``tombstoned`` branch — there is no failure mode that requires
  permanent discard.
* No ``failed`` branch — no DB write that can raise, no fetch that
  can raise.

Raw-payload invariant (#938): registered with
``requires_raw_payload=False`` — the ScheduledJob is the sole writer
of the raw payload and runs its own ``store_raw`` + ``conn.commit()``
before the per-file txn. Manifest-worker dispatch for this source is
audit-only.

Non-caller invariant: this module does NOT call
``SecFilingsProvider.fetch_document_text``, ``store_raw``,
``conn.execute``, ``conn.cursor`` or ``conn.transaction``. If a future
PR introduces a manifest-dispatch consumer that needs DB writes, that
PR must add the fetcher + the
``tests/test_fetch_document_text_callers.py`` allow-list update + the
SQL normalisation pathway in lockstep, per the "Every structured field
lands in SQL" prevention contract.

Pattern reference: sec-edgar §11.5.1 + ``sec_10q.py`` (#1168) +
``sec_xbrl_facts.py`` (G7). This module is the third adopter of the
synth no-op pattern.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

PARSER_VERSION = "finra-si-bimonthly-v1"


def _parse_finra_short_interest(
    conn: psycopg.Connection[Any],  # noqa: ARG001 — synth no-op uses no DB
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Synth no-op: mark the row parsed without touching FINRA or DB.

    FINRA short interest already lands via the
    ``finra_short_interest_refresh`` ScheduledJob (daily cron + manual
    trigger). The manifest row's accession-level audit is the only
    deliverable at the manifest dispatch layer.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    logger.debug(
        "finra_short_interest manifest parser: synth no-op for accession=%s "
        "(short interest lands via finra_short_interest_refresh ScheduledJob; "
        "no per-filing payload work)",
        row.accession_number,
    )
    return ParseOutcome(
        status="parsed",
        parser_version=PARSER_VERSION,
    )


def register() -> None:
    """Register the synth no-op parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. Called once
    from ``app.services.manifest_parsers.register_all_parsers`` at
    package import time, and re-callable from tests after a registry
    wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser(
        "finra_short_interest",
        _parse_finra_short_interest,
        requires_raw_payload=False,
    )
