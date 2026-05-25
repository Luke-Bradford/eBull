"""finra_regsho_daily manifest-worker parser — synth no-op (G6/#916).

FINRA RegSHO daily short volume data lands via the
``finra_regsho_daily_refresh`` ScheduledJob (daily 23:00 UTC). The
ScheduledJob owns the fetch + parse + UPSERT into
``finra_regsho_daily_observations``, then UPSERTs the manifest row as
``ingest_status='parsed'`` directly (inside the same JOB-owned ``with
conn.transaction():`` so manifest parsed-status atomically implies
observations durable).

This parser exists only to satisfy the manifest-worker dispatch
invariant on the rare ``sec_rebuild --source=finra_regsho_daily`` path
— if the operator flips manifest rows back to ``pending``, the
manifest worker needs a registered parser to mark them ``parsed``
again WITHOUT triggering a network fetch or DB write. The actual
re-ingest mechanism is re-firing the ScheduledJob (which re-walks the
revision window × 6 prefixes + re-fetches the file).

Architectural siblings: ``sec_xbrl_facts.py`` (G7) +
``finra_short_interest.py`` (G6/#915). Same shape — third + fourth
adopters of the synth no-op pattern.

ParseOutcome contract:

* ``status='parsed'`` — always. Manifest row's existence is the audit
  signal; observations have already landed via the ScheduledJob path.
* ``parser_version='finra-regsho-daily-v1'`` — unified with the
  ScheduledJob's write-side parser_version per #915 Codex 1b r2 MED 3.
* No ``tombstoned`` / ``failed`` branches — no failure mode that
  requires permanent discard; no DB write that can raise.

Raw-payload invariant (#938): registered with
``requires_raw_payload=False`` — the ScheduledJob is the sole writer
of the raw payload and runs its own ``store_raw`` + ``conn.commit()``
before the per-file txn. Manifest-worker dispatch for this source is
audit-only.

Non-caller invariant: this module does NOT call
``SecFilingsProvider.fetch_document_text``, ``store_raw``,
``conn.execute``, ``conn.cursor`` or ``conn.transaction``.
"""

from __future__ import annotations

import logging
from typing import Any, Final

import psycopg

logger = logging.getLogger(__name__)

PARSER_VERSION = "finra-regsho-daily-v1"

# #1322 — synth-noop parity flag. Enforced by
# tests/smoke/test_etl_source_to_sink.py against MANIFEST_SOURCE_SINKS kind.
# Flip to False (or remove) if this module ever grows into a real writer.
_SYNTH_NOOP: Final[bool] = True


def _parse_finra_regsho_daily(
    conn: psycopg.Connection[Any],  # noqa: ARG001 — synth no-op uses no DB
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Synth no-op: mark the row parsed without touching FINRA or DB.

    FINRA RegSHO daily already lands via the
    ``finra_regsho_daily_refresh`` ScheduledJob (daily cron + manual
    trigger). The manifest row's accession-level audit is the only
    deliverable at the manifest dispatch layer.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    logger.debug(
        "finra_regsho_daily manifest parser: synth no-op for accession=%s "
        "(RegSHO daily lands via finra_regsho_daily_refresh ScheduledJob; "
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
        "finra_regsho_daily",
        _parse_finra_regsho_daily,
        requires_raw_payload=False,
    )
