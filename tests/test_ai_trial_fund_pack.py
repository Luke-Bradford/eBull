"""#3515 slice 3b-iii — fund-v1's pack builder (fund-v1 spec §2-§6). Pure: the two reads are faked."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.services import ai_trial_fund_pack
from app.services.ai_trial_fund_blocks import (
    MAX_CLAIM_TO_SNAPSHOT,
    MDNA_MIN_SUBSTANTIVE_CHARS,
    REFUSE_FUNDAMENTALS_COVERAGE,
    REFUSE_MDNA_COVERAGE,
    REFUSE_PROMPT_OVER_BUDGET,
    REFUSE_SNAPSHOT_TOO_LATE,
    FundBlocks,
    NameAudit,
    NameBlocks,
)
from app.services.ai_trial_fund_pack import REFUSE_BUDGET_FIXTURE_MISSING, build_fund_pack, fixture_bytes
from app.services.ai_trial_pack import canonical_sha256
from app.services.ai_trial_pack_build import PackRefusal
from app.services.ai_trial_pack_reader import AccountContext, Pack, Step1
from app.services.ai_trial_prompt import render_user_prompt

AS_OF = datetime(2026, 10, 1, 23, 30, tzinfo=UTC)
STEP1 = Step1(AS_OF, date(2026, 10, 1), date(2026, 10, 2), None, None, None)
ACCOUNT = AccountContext(open_positions=(), free_slots=12, max_new_entries=2)
TEXT = "x" * MDNA_MIN_SUBSTANTIVE_CHARS
SYMBOLS = {"AAA": 1, "BBB": 2, "CCC": 3, "DDD": 4, "EEE": 5}


class _Conn:
    def __init__(self) -> None:
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1


def _base() -> Pack:
    names = [{"symbol": s, "instrument_id": i, "bars": []} for s, i in SYMBOLS.items()]
    doc = {"names": names, "account": {"free_slots": 12}}
    return Pack(doc, canonical_sha256(doc), dict(SYMBOLS), {"ZZZ": "masked_bars"})


def _blocks(*, fund: int = 5, text: int = 5, lag: timedelta = timedelta(seconds=1)) -> FundBlocks:
    by: dict[int, NameBlocks] = {}
    for n, iid in enumerate(SYMBOLS.values()):
        by[iid] = NameBlocks(
            {"facts": [iid]} if n < fund else None,
            None if n < fund else {"reason": "no_report"},
            {"text": TEXT} if n < text else None,
            None if n < text else {"reason": "no_extract"},
            NameAudit(ambiguous_event=iid, withheld_after_as_of={"acc": 1} if iid == 1 else {}),
        )
    return FundBlocks(AS_OF + lag, by)


def _declaration(bytes_: object = 10**9) -> SimpleNamespace:
    return SimpleNamespace(doc={"budget_fixture": {"rendered_prompt_bytes": bytes_}})


@pytest.fixture
def reads(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {"blocks": _blocks()}

    def blocks(conn: _Conn, ids: list[int], *, as_of: datetime, extractor: str) -> FundBlocks:
        # The pack reads committed first: the blocks open their own snapshot (§2).
        assert conn.commits == 1
        state["read"] = (ids, as_of, extractor)
        return state["blocks"]

    monkeypatch.setattr(ai_trial_fund_pack, "assemble_pack", lambda _c, **_kw: _base())
    monkeypatch.setattr(ai_trial_fund_pack, "read_fund_blocks", blocks)
    monkeypatch.setattr(ai_trial_fund_pack, "extractor_id", lambda: "edgartools==x/mdna-1")
    return state


def _build(declaration: Any = None) -> Any:
    return build_fund_pack(
        cast(Any, _Conn()),
        step1=STEP1,
        account=ACCOUNT,
        fetch_intraday=lambda _i: [],
        declaration=_declaration() if declaration is None else declaration,
    )


def _record(stored: Any) -> dict[str, Any]:
    return json.loads(stored["fund_blocks"].dumps(stored["fund_blocks"].obj))


def test_each_name_gains_the_four_block_keys_and_the_sha_is_re_derived(reads: dict[str, Any]) -> None:
    built = _build()
    assert reads["read"] == ([1, 2, 3, 4, 5], AS_OF, "edgartools==x/mdna-1")
    names = built.pack.pack["names"]
    assert [n["symbol"] for n in names] == list(SYMBOLS)  # nothing dropped, order kept
    assert names[0]["fundamentals"] == {"facts": [1]} and names[0]["mdna"] == {"text": TEXT}
    assert {"fundamentals", "fundamentals_absent", "mdna", "mdna_absent"} <= names[0].keys()
    assert built.pack.sha256 == canonical_sha256(built.pack.pack) != _base().sha256
    assert (built.pack.complete, built.pack.incomplete) == (_base().complete, _base().incomplete)
    # The audit is recorded on the run, never shown in the pack (§2).
    assert "withheld_after_as_of" not in json.dumps(built.pack.pack)
    record = _record(built.run_record)
    assert record["audit"]["AAA"]["withheld_after_as_of"] == {"acc": 1}
    assert record["coverage"] == {
        "names": 5,
        "required": 4,
        "fundamentals": 5,
        "mdna_substantive": 5,
        "refusal": None,
    }


@pytest.mark.parametrize(
    ("fund", "text", "expected"),
    [(3, 5, REFUSE_FUNDAMENTALS_COVERAGE), (5, 3, REFUSE_MDNA_COVERAGE), (3, 3, REFUSE_FUNDAMENTALS_COVERAGE)],
)
def test_coverage_below_the_floor_refuses_the_run_with_both_counts(
    reads: dict[str, Any], fund: int, text: int, expected: str
) -> None:
    reads["blocks"] = _blocks(fund=fund, text=text)
    with pytest.raises(PackRefusal) as refused:
        _build()
    assert refused.value.reason == expected
    coverage = _record(refused.value.run_record)["coverage"]
    assert (coverage["fundamentals"], coverage["mdna_substantive"]) == (fund, text)


def test_at_the_floor_passes(reads: dict[str, Any]) -> None:
    reads["blocks"] = _blocks(fund=4, text=4)  # ⌈0.8 × 5⌉ = 4
    assert _build().pack.pack["names"][4]["fundamentals"] is None


def test_a_late_snapshot_refuses(reads: dict[str, Any]) -> None:
    reads["blocks"] = _blocks(lag=MAX_CLAIM_TO_SNAPSHOT + timedelta(microseconds=1))
    with pytest.raises(PackRefusal) as refused:
        _build()
    assert refused.value.reason == REFUSE_SNAPSHOT_TOO_LATE
    assert "snapshot_at" in _record(refused.value.run_record)


def test_the_byte_gate_refuses_one_byte_over_and_passes_equal(reads: dict[str, Any]) -> None:
    rendered = len(render_user_prompt(_build().pack.pack).text.encode("utf-8"))
    assert _build(_declaration(rendered)).pack.sha256
    with pytest.raises(PackRefusal) as refused:
        _build(_declaration(rendered - 1))
    assert refused.value.reason == REFUSE_PROMPT_OVER_BUDGET


@pytest.mark.parametrize("doc", [None, {}, {"budget_fixture": {}}, {"budget_fixture": {"rendered_prompt_bytes": True}}])
def test_a_declaration_without_a_budget_fixture_fails_closed(reads: dict[str, Any], doc: object) -> None:
    assert fixture_bytes(doc) is None
    with pytest.raises(PackRefusal) as refused:
        _build(SimpleNamespace(doc=doc))
    assert refused.value.reason == REFUSE_BUDGET_FIXTURE_MISSING
