"""#3515 slice 3b-iii — fund-v1's pack builder (fund-v1 spec §2-§6).

    v1's pack (``assemble_pack``, unchanged) → commit → the two blocks in their own snapshot
      (``read_fund_blocks``, after the claim) → ``snapshot_too_late`` → coverage gates
      → each pack-complete name gains the four block keys → re-sha → per-run byte gate

* Every refusal refuses the RUN (§5) by raising ``PackRefusal``; nothing drops a name, so the
  universe is exactly v1's at the same ``as_of``.
* The run record (``ai_trial_runs.fund_blocks``, ``sql/443``) carries the snapshot time, both
  coverage counts and every shown name's audit counts, on every run that reached the blocks,
  refused or not. The audit never enters the pack (§2: ``withheld_after_as_of`` is post-cutoff).
* The byte gate's bound is the budget fixture's rendered length, read from the declaration
  (``doc["budget_fixture"]["rendered_prompt_bytes"]``, written by the freeze from slice 2b's
  fixture). A declaration without it refuses ``budget_fixture_missing``: fail closed.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Final

import psycopg
from psycopg.types.json import Jsonb

from app.services.ai_trial_fund_blocks import (
    FundBlocks,
    coverage,
    prompt_budget_refusal,
    read_fund_blocks,
    snapshot_refusal,
)
from app.services.ai_trial_pack import canonical_json, canonical_sha256
from app.services.ai_trial_pack_build import BuiltPack, PackRefusal
from app.services.ai_trial_pack_reader import AccountContext, IntradayFetch, Pack, Step1, assemble_pack
from app.services.ai_trial_prompt import render_user_prompt
from app.services.mdna_extraction import extractor_id

Conn = psycopg.Connection[Any]

REFUSE_BUDGET_FIXTURE_MISSING: Final = "budget_fixture_missing"


def fixture_bytes(doc: object) -> int | None:
    """The declared budget fixture's rendered UTF-8 byte length, or ``None`` when absent or malformed."""
    fixture = doc.get("budget_fixture") if isinstance(doc, dict) else None
    value = fixture.get("rendered_prompt_bytes") if isinstance(fixture, dict) else None
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def run_record(blocks: FundBlocks, complete: dict[str, int]) -> dict[str, Any]:
    """The ``fund_blocks`` run column: snapshot time, coverage counts, per-name audit counts."""
    shown = [blocks.by_instrument[iid] for iid in complete.values()]
    cov = coverage(shown)
    return {
        "snapshot_at": blocks.snapshot_at,
        "coverage": None if cov is None else dataclasses.asdict(cov),
        "audit": {symbol: dataclasses.asdict(blocks.by_instrument[iid].audit) for symbol, iid in complete.items()},
    }


def merge_blocks(base: Pack, blocks: FundBlocks) -> Pack:
    """v1's pack with each name's four block keys added; the sha is re-derived over the result."""
    names = [
        {**entry, **blocks.by_instrument[int(entry["instrument_id"])].pack_entry()} for entry in base.pack["names"]
    ]
    pack = {**base.pack, "names": names}
    return Pack(pack, canonical_sha256(pack), base.complete, base.incomplete)


def build_fund_pack(
    conn: Conn, *, step1: Step1, account: AccountContext, fetch_intraday: IntradayFetch, declaration: Any
) -> BuiltPack:
    base = assemble_pack(conn, step1=step1, account=account, fetch_intraday=fetch_intraday)
    # The blocks open their OWN snapshot after the claim (§2), so the pack reads must be closed.
    conn.commit()
    blocks = read_fund_blocks(conn, sorted(base.complete.values()), as_of=step1.as_of, extractor=extractor_id())
    record = run_record(blocks, base.complete)
    stored = {"fund_blocks": Jsonb(record, dumps=canonical_json)}
    reason = snapshot_refusal(as_of=step1.as_of, snapshot_at=blocks.snapshot_at)
    if reason is None and record["coverage"] is not None:
        reason = record["coverage"]["refusal"]
    if reason is not None:
        raise PackRefusal(reason, stored)
    pack = merge_blocks(base, blocks)
    bound = fixture_bytes(getattr(declaration, "doc", None))
    if bound is None:
        raise PackRefusal(REFUSE_BUDGET_FIXTURE_MISSING, stored)
    reason = prompt_budget_refusal(
        rendered_bytes=len(render_user_prompt(pack.pack).text.encode("utf-8")), fixture_bytes=bound
    )
    if reason is not None:
        raise PackRefusal(reason, stored)
    return BuiltPack(pack, stored)


__all__ = ["REFUSE_BUDGET_FIXTURE_MISSING", "build_fund_pack", "fixture_bytes", "merge_blocks", "run_record"]
