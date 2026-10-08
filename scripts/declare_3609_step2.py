"""#3609 step 2 slice 4: the declaration's ``evidence``, its ``declared`` ledger row and the integrity-only read.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"Registration" (the ``evidence`` list, "The
declaration payload is pinned separately, once", factor and RF data) and §"Slices" item 4.

Run once, from the declaration branch with the final spec at ``SPEC_PATH``, before the register row exists::

    PYTHONPATH=. uv run python -m scripts.declare_3609_step2 --accessed-by "<operator or loop identity>"

In order, it

1. refuses if the register or the committed ledger already declares ``3609-step2-book-v1``;
2. computes this checkout's code hashes (``CodeHashes.current``) and verifies every file pin before any database
   write: stage A's manifest bytes against ``STAGE_A_MANIFEST_SHA256``, whose Table 9 entry must equal the committed
   CSV's sha256, and step 0's manifest and ``paths.json`` (``read_step0``), which name the two factor snapshots;
3. reads B1's entry close, SPY's raw close at 2014-09-30 (#3666 item 20), by step 0's rule for ETF raw prices
   (``report_3609_baselines``: the Intrader series' last quarantine-usable bar of the month, ``load_month_ends``'s
   query). The query is bounded to that month in SQL, so no price after 2021-05-31 leaves the database before the
   access row (spec §"Dates, samples and the hold-out"). The bar must fall on that session;
4. commits the integrity-only read's ``read`` access row, then takes the two snapshots' digests
   (``snapshot_digests``), which return no value;
5. builds the register row and checks that the run's own gates accept it (``check_declaration`` against its
   ``declared`` row, ``declared_pins``, ``declared_b1_close``), appends the ``declared`` row to the committed ledger
   and prints the access id, the evidence and the payload sha256.

A failure after step 4's access row leaves that row logged and no ``declared`` row; the access must precede the read,
so this cannot be avoided. A rerun logs its own ``read`` access, which is the honest record of a second read.

The declaration PR then adds :func:`declared_trial` of the printed evidence to ``TRIAL_REGISTER`` verbatim, with a
``TRIAL_REGISTER_VERSION`` bump, and cites the access id. A row that differs from the one hashed here refuses every
attempt at ``check_declaration``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final, LiteralString, cast

import psycopg

from app.config import settings
from app.services.factor_book_declaration import (
    DECLARED_EVENT,
    TRIAL_ID,
    CodeHashes,
    DeclarationError,
    check_declaration,
    evidence_value,
    payload_sha256,
)
from app.services.factor_book_ledger import COMMITTED_LEDGER_PATH, STRATEGY_ID, STRATEGY_VERSION
from app.services.factor_book_path import Month
from app.services.factor_book_reference import QMJ_PDF_SHA256, SICCODES12_SHA256
from app.services.factor_panel_artefact import sha256_file
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.factor_panel_reference import TABLE9_SIGNS_PATH
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import HoldoutAccess, record_holdout_access
from app.services.total_return_reader import _MONTH_END_SQL, INTRADER_VENDOR, MonthEnd
from app.services.trial_register import TRIAL_REGISTER, DeclaredTrial, TrialExactness, TrialRegister
from scripts.build_3609_factor_panel import MANIFEST_FILE
from scripts.report_3609_baselines import _INTRADER_SERIES_SQL
from scripts.report_3609_step2 import STAGE_A_ARTEFACT, STAGE_A_MANIFEST_SHA256, TABLE9_SIGNS
from scripts.report_3609_step2_inputs import (
    B1_CLOSE_LABEL,
    B1_CLOSE_SESSION,
    FF12_LABEL,
    QMJ_LABEL,
    STAGE_A_LABEL,
    STEP0_LABEL,
    STEP0_MANIFEST,
    STEP0_RUN,
    TABLE9_LABEL,
    SnapshotDigests,
    declared_b1_close,
    declared_pins,
    observations_label,
    read_step0,
    response_label,
    snapshot_digests,
)

SPEC_REFERENCE: Final = 'docs/research/2026-10-06-3609-step2-factor-book.md §"Registration" and §"Slices" item 4'
DESCRIPTION: Final = (
    "#3609 step 2 value book, version 1: one configuration (the spec's value-family book, its matched random "
    "control, references and diagnostics) on stage A (development, formations 2014-09..2021-04) and stage B (reused "
    "validation, formations 2021-05..2024-07), net of step 0's costs, screened by the 2026-10-08 settled entry's "
    "four conditions. Its claim is condition 3, the HML loading on stage B, powered in spec premise 6. No "
    "TrialDesign: #2599's claiming design is an IR margin, which this screen does not claim. Step 1's 16 searches "
    "and the 3 exposure-planning searches stay counted."
)
#: The hold-out access identity, named in ``evidence`` (spec §"Registration").
HOLDOUT_STRATEGY_LABEL: Final = "holdout_strategy_id"
HOLDOUT_VERSION_LABEL: Final = "holdout_strategy_version"
B1_SYMBOL: Final = "SPY"
B1_MONTH: Final[Month] = (B1_CLOSE_SESSION.year, B1_CLOSE_SESSION.month)


def access_purpose(snapshot_ids: Mapping[str, int]) -> str:
    """The integrity-only read's ``purpose``, in the spec's words, naming step 0's snapshot ids in ascending order."""
    first, second = sorted(snapshot_ids.values())
    return f"#3609 step 2 declaration: integrity digests of factor snapshots {first} and {second}, no values read out"


def declaration_evidence(
    code: CodeHashes,
    *,
    step0_manifest_sha256: str,
    table9_sha256: str,
    snapshots: Mapping[str, SnapshotDigests],
    b1_close: float,
) -> str:
    """The register row's ``evidence``: the spec reference, then every ``label=value`` pin once."""
    labels = {
        **code.by_label(),
        STAGE_A_LABEL: STAGE_A_MANIFEST_SHA256,
        STEP0_LABEL: step0_manifest_sha256,
        FF12_LABEL: SICCODES12_SHA256,
        QMJ_LABEL: QMJ_PDF_SHA256,
        TABLE9_LABEL: table9_sha256,
    }
    for dataset in sorted(snapshots):
        labels[response_label(dataset)] = snapshots[dataset].response_sha256
        labels[observations_label(dataset)] = snapshots[dataset].observations_sha256
    labels[B1_CLOSE_LABEL] = repr(b1_close)
    labels[HOLDOUT_STRATEGY_LABEL] = STRATEGY_ID
    labels[HOLDOUT_VERSION_LABEL] = STRATEGY_VERSION
    return "; ".join([SPEC_REFERENCE, *(f"{label}={value}" for label, value in labels.items())])


def declared_trial(evidence: str) -> DeclaredTrial:
    """The register row: non-claiming, exact, one search (spec §"Registration")."""
    return DeclaredTrial(
        trial_id=TRIAL_ID, description=DESCRIPTION, evidence=evidence, exactness=TrialExactness.EXACT, searches=1
    )


def declared_row(trial: DeclaredTrial) -> dict[str, Any]:
    """The committed ``declared`` ledger row: the trial id and the sha256 of the row's canonical JSON."""
    return {
        "event": DECLARED_EVENT,
        "trial_id": trial.trial_id,
        "payload_sha256": payload_sha256(trial),
        "at": datetime.now(UTC).isoformat(),
    }


def require_undeclared(register: TrialRegister, ledger_rows: Sequence[Mapping[str, Any]]) -> None:
    """A second declaration under the same id would make every attempt refuse; replacing one needs a new trial id."""
    rows = [trial for trial in register.trials if trial.trial_id == TRIAL_ID]
    declared = [row for row in ledger_rows if row.get("event") == DECLARED_EVENT and row.get("trial_id") == TRIAL_ID]
    if rows or declared:
        raise DeclarationError(
            f"{TRIAL_ID} is already declared: {len(rows)} register rows, {len(declared)} ledger rows"
        )


def stage_a_table9(manifest_bytes: bytes, table9_sha256: str) -> None:
    """Stage A's manifest is the pinned one, and its frozen Table 9 CSV is the committed one."""
    if hashlib.sha256(manifest_bytes).hexdigest() != STAGE_A_MANIFEST_SHA256:
        raise DeclarationError(f"stage A's manifest is not the pinned {STAGE_A_MANIFEST_SHA256}")
    found = json.loads(manifest_bytes)["inputs"].get(TABLE9_SIGNS)
    if found != table9_sha256:
        raise DeclarationError(f"stage A froze Table 9 CSV {found!r}, the committed CSV is {table9_sha256}")


def b1_close(series_ids: Sequence[int], month_ends: Mapping[tuple[int, Month], MonthEnd]) -> float:
    """SPY's raw close at :data:`B1_CLOSE_SESSION` from its one Intrader series' last usable bar of that month."""
    if len(series_ids) != 1:
        raise DeclarationError(f"{B1_SYMBOL} resolves to Intrader series {list(series_ids)}; exactly one is required")
    end = month_ends.get((series_ids[0], B1_MONTH))
    if end is None or end.bar_date != B1_CLOSE_SESSION:
        raise DeclarationError(f"{B1_SYMBOL}'s last usable bar of {B1_MONTH} is {end}, not {B1_CLOSE_SESSION}")
    if not (math.isfinite(end.close) and end.close > 0):
        raise DeclarationError(f"{B1_SYMBOL}'s raw close at {B1_CLOSE_SESSION} is {end.close}")
    return end.close


def bounded_month_end_sql(source: str = _MONTH_END_SQL) -> LiteralString:
    """``load_month_ends``'s query with its bars bounded to ``%(first)s .. %(last)s`` before ``DISTINCT ON``. The
    source is a module constant and the bound a literal, so the cast admits no outside text."""
    anchor = "WHERE d.series_id"
    if source.count(anchor) != 1:
        raise DeclarationError(f"the month-end query has {source.count(anchor)} {anchor!r} clauses, expected one")
    return cast(
        LiteralString, source.replace(anchor, "WHERE d.bar_date BETWEEN %(first)s AND %(last)s\n  AND d.series_id")
    )


def read_b1_close(conn: psycopg.Connection[Any]) -> float:
    found = conn.execute(_INTRADER_SERIES_SQL, {"vendor": INTRADER_VENDOR, "symbols": [B1_SYMBOL]}).fetchall()
    ids = [int(sid) for _symbol, sid in found]
    rows = conn.execute(
        bounded_month_end_sql(),
        {
            "series_ids": ids,
            "quarantine_version": QUARANTINE_RULE_SET_VERSION,
            "first": B1_CLOSE_SESSION.replace(day=1),
            "last": B1_CLOSE_SESSION,
        },
    ).fetchall()
    month_ends = {(int(sid), (day.year, day.month)): MonthEnd(day, adj, close) for sid, day, adj, close in rows}
    return b1_close(ids, month_ends)


def declare(
    code: CodeHashes,
    *,
    step0_manifest_sha256: str,
    table9_sha256: str,
    snapshot_ids: Mapping[str, int],
    b1: float,
    accessed_by: str,
    record_access: Callable[[HoldoutAccess], int],
    digests: Callable[[Mapping[str, int]], Mapping[str, SnapshotDigests]],
) -> tuple[int, DeclaredTrial, dict[str, Any]]:
    """Steps 4 and 5: commit the ``read`` access, take the digests, then build the row and check the gates accept it.

    ``record_access`` must commit before it returns, so the access is logged even if the read then fails."""
    access_id = record_access(
        HoldoutAccess(
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            access_kind="read",
            accessed_by=accessed_by,
            purpose=access_purpose(snapshot_ids),
        )
    )
    evidence = declaration_evidence(
        code,
        step0_manifest_sha256=step0_manifest_sha256,
        table9_sha256=table9_sha256,
        snapshots=digests(snapshot_ids),
        b1_close=b1,
    )
    trial = declared_trial(evidence)
    row = declared_row(trial)
    check_declaration(TrialRegister("declaration check", (trial,)), [row], code)
    if declared_pins(evidence).step0_manifest_sha256 != step0_manifest_sha256 or declared_b1_close(evidence) != b1:
        raise DeclarationError("the evidence does not read back as the values declared")
    for label, value in ((HOLDOUT_STRATEGY_LABEL, STRATEGY_ID), (HOLDOUT_VERSION_LABEL, STRATEGY_VERSION)):
        if evidence_value(evidence, label) != value:
            raise DeclarationError(f"the evidence's {label} does not read back as {value}")
    return access_id, trial, row


def _record_access(access: HoldoutAccess) -> int:
    with psycopg.connect(settings.database_url) as conn:
        access_id = record_holdout_access(conn, access)
        conn.commit()
    return access_id


def _digests(snapshot_ids: Mapping[str, int]) -> Mapping[str, SnapshotDigests]:
    with psycopg.connect(settings.database_url) as conn:
        return snapshot_digests(conn, snapshot_ids)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--accessed-by", required=True, help="the operator or loop identity running the declaration")
    args = parser.parse_args(argv)

    require_undeclared(TRIAL_REGISTER, read_ledger(COMMITTED_LEDGER_PATH))
    code = CodeHashes.current()
    table9_sha256 = sha256_file(TABLE9_SIGNS_PATH)
    stage_a_table9((STAGE_A_ARTEFACT / MANIFEST_FILE).read_bytes(), table9_sha256)
    step0_manifest_sha256 = sha256_file(STEP0_RUN / STEP0_MANIFEST)
    step0 = read_step0(step0_manifest_sha256)
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        b1 = read_b1_close(conn)

    access_id, trial, row = declare(
        code,
        step0_manifest_sha256=step0_manifest_sha256,
        table9_sha256=table9_sha256,
        snapshot_ids=step0.factor_snapshots,
        b1=b1,
        accessed_by=args.accessed_by,
        record_access=_record_access,
        digests=_digests,
    )
    append_ledger(COMMITTED_LEDGER_PATH, row)
    print(json.dumps({"access_id": access_id, "evidence": trial.evidence, "payload_sha256": row["payload_sha256"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
