"""sec_n_csr manifest-worker parser — synth no-op adapter (#918 verdict).

Spike `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` confirmed
INFEASIBLE for v1: the OEF iXBRL taxonomy publishes only fund-level +
class-level + sector-axis facts (no per-holding CUSIP / ISIN / SEDOL /
ticker / portfolio-issuer CIK). The N-CSR primary HTML's Schedule of
Investments carries no machine-readable security identifier — Vanguard
+ Fidelity equity-fund samples show `Name`/`Shares`/`Value` columns and
the iShares bond-fund sample shows `Issuer`/`Coupon`/`Maturity`/`Value`,
but no CUSIP / ISIN / SEDOL / ticker column appears in any sampled
family. The N-CSR itself directs the reader to N-PORT for structured
per-issuer holdings, which eBull already ingests via the manifest worker.

A cross-source overlay path (4-part match N-CSR name × N-PORT-P CUSIP at
same fund × same period × shares-or-value corroboration) was identified
but not measured; it was ruled out independently on product-visibility
grounds (an audit badge has no operator-discriminating signal vs the
existing N-PORT data).

This adapter exists to drain ``sec_filing_manifest`` rows for
``source='sec_n_csr'`` cleanly so:

- ``/coverage/manifest-parsers`` reports ``has_registered_parser=True``
- ``WorkerStats.skipped_no_parser_by_source['sec_n_csr']`` stays at 0
- Real lane-stuck conditions surface against a clean baseline

If a future operator-visible audit-stamp surface materialises, the
verdict is REOPEN-ELIGIBLE — and that reopen MUST run the unmeasured
4-part match-rate probe before claiming the overlay path is buildable.
This module then becomes the seam for the eventual real parser; the
PR adding the fetcher must update the
``tests/test_fetch_document_text_callers.py`` allow-list (this guard
discovers any new caller of ``fetch_document_text`` via grep + word-
boundary regex) + the SQL normalisation pathway in lockstep, per the
"Every structured field lands in SQL" prevention contract.

ParseOutcome contract:

* ``status='parsed'`` — always. The manifest row's existence proves
  the filing was discovered; no further per-filing work is in scope.
* No ``tombstoned`` branch — there is no failure mode that requires
  permanent discard of the manifest row. The synth no-op does not
  consume URL or instrument_id.
* No ``failed`` branch — no DB write that can raise; no fetch that
  can raise.

Raw-payload invariant (#938): registered with
``requires_raw_payload=False`` — this is a synth source per
sec-edgar §11.5.1. The worker accepts ``parsed`` with
``raw_status=None``.

Pattern reference: sec-edgar §11.5.1 documents the "synth no-op parser"
canonical shape. ``sec_10q`` (#1168 / PR #1169) is the first exemplar;
this adapter is the second.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

_PARSER_VERSION_N_CSR = "n-csr-noop-v1"


def _parse_sec_n_csr(
    conn: psycopg.Connection[Any],  # noqa: ARG001 — synth no-op uses no DB
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Synth no-op: mark the row parsed without touching SEC or DB.

    The manifest discovery row IS the audit. Fund holdings land via
    N-PORT-P (structured CUSIP grain); N-CSR has no machine-readable
    per-issuer identifier and no operator-visible delta in v1.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    logger.debug(
        "sec_n_csr manifest parser: synth no-op for accession=%s "
        "(fund holdings land via N-PORT-P; N-CSR adds no operator-visible figure in v1; "
        "see docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md)",
        row.accession_number,
    )
    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_N_CSR,
    )


def register() -> None:
    """Register the synth no-op parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. Called once
    from ``app.services.manifest_parsers.register_all_parsers`` at
    package import time, and re-callable from tests after a registry
    wipe.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_n_csr", _parse_sec_n_csr, requires_raw_payload=False)
