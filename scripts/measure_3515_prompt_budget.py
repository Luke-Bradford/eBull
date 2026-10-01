"""Measure the real fund-v1 prompt by part, for the §6 budget resize (#3515).

Read-only on the DB. Builds v1's pack at ``now`` exactly as the decision job would (the
intraday fetch is the broker's informational candle read), then the two fund-v1 blocks, and
prints byte sizes per name and part in the production encoding (spec §3, grouped since slice 3a; the
old-encoding and matrix figures in spec §9 came from this script as merged in #3535). No model call: tokens
come from the budget fixture.

    PYTHONPATH=. uv run python -m scripts.measure_3515_prompt_budget             # real shortlist at now
    PYTHONPATH=. uv run python -m scripts.measure_3515_prompt_budget --fixture   # synthetic capped names only

⚠ The account context is empty here (the job passes the arm's book; at most 12 held positions).
"""

from __future__ import annotations

import argparse
import statistics
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.services.ai_trial_fund_blocks import read_fund_blocks
from app.services.ai_trial_fund_pack import merge_blocks
from app.services.ai_trial_pack import canonical_json
from app.services.ai_trial_pack_reader import (
    INTRADAY_INTERVAL,
    INTRADAY_REQUEST_COUNT,
    AccountContext,
    assemble_pack,
    read_step1,
)
from app.services.ai_trial_prompt import render_user_prompt
from app.services.mdna_extraction import extractor_id
from app.workers.scheduler import _load_etoro_credentials


def _b(value: object) -> int:
    return len(canonical_json(value).encode("utf-8"))


def _dist(label: str, values: Sequence[int]) -> None:
    if not values:
        print(f"{label}: none")
        return
    q = statistics.quantiles(values, n=20) if len(values) > 1 else [values[0]] * 19
    p50 = statistics.median(values)
    print(f"{label}: n={len(values)} sum={sum(values):,} p50={p50:,.0f} p95={q[18]:,.0f} max={max(values):,}")


def _prompt_bytes(pack: Mapping[str, Any]) -> int:
    return len(render_user_prompt(pack).text.encode("utf-8"))


_PARTS = ("bars", "intraday", "filings", "news", "fundamentals", "mdna")
_BLOCK_KEYS = ("fundamentals", "fundamentals_absent", "mdna", "mdna_absent")


def fixture() -> int:
    """Capped synthetic names: ``budget_fixture_pack(n)``'s rendered bytes at fixed ``n`` and every name's part
    sizes."""
    from scripts.ai_trial_fund_budget_fixture import budget_fixture_pack

    pack = budget_fixture_pack().pack

    sizes = {n: _prompt_bytes(budget_fixture_pack(n).pack) for n in (1, 2, 30, 50)}
    print("rendered bytes at n: " + ", ".join(f"{n}={b:,}" for n, b in sizes.items()))
    print(f"bytes added by name 2: {sizes[2] - sizes[1]:,}")
    for k, name in enumerate(pack["names"], start=1):
        parts = {p: _b(name[p]) for p in _PARTS}
        v1 = _b({key: v for key, v in name.items() if key not in _BLOCK_KEYS})
        print(f"name {k}: v1 entry {v1:,} {parts}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fixture", action="store_true", help="synthetic capped names only; no DB, no broker")
    if parser.parse_args(argv).fixture:
        return fixture()
    as_of = datetime.now(UTC)
    creds = _load_etoro_credentials("measure_3515_prompt_budget")
    if creds is None:
        print("no eToro credentials")
        return 1
    with (
        psycopg.connect(settings.database_url) as conn,
        EtoroMarketDataProvider(api_key=creds[0], user_key=creds[1], env=settings.etoro_env) as market,
    ):
        step1 = read_step1(conn, as_of=as_of)
        if step1.refusal is not None:
            print(f"step 1 refused: {step1.refusal}")
            return 1
        base = assemble_pack(
            conn,
            step1=step1,
            account=AccountContext((), 12, 6),
            fetch_intraday=lambda iid: market.get_intraday_candles(iid, INTRADAY_INTERVAL, INTRADAY_REQUEST_COUNT),
        )
        conn.commit()
        print(f"as_of {as_of.isoformat()} complete {len(base.complete)} incomplete {dict(base.incomplete)}")
        blocks = read_fund_blocks(conn, sorted(base.complete.values()), as_of=as_of, extractor=extractor_id())
        conn.rollback()

    names = base.pack["names"]
    _dist("v1 name entry bytes", [_b(n) for n in names])
    _dist("v1 intraday bytes", [_b(n["intraday"]) for n in names])
    _dist("v1 intraday bar count", [len(n["intraday"]["bars"]) for n in names])
    _dist("v1 daily bars bytes", [_b(n["bars"]) for n in names])
    _dist("v1 filings+news bytes", [_b(n["filings"]) + _b(n["news"]) for n in names])
    v1_prompt = len(render_user_prompt(base.pack).text.encode("utf-8"))
    print(f"v1 rendered user prompt bytes: {v1_prompt:,}")

    nb = [blocks.by_instrument[int(n["instrument_id"])] for n in names]
    fund = [b.fundamentals for b in nb if b.fundamentals is not None]
    _dist("facts per name", [sum(len(g["rows"]) for r in f["reports"] for g in r["facts"]) for f in fund])
    _dist("fundamentals bytes", [_b(f) for f in fund])
    _dist("mdna block bytes", [_b(b.mdna) for b in nb if b.mdna is not None])
    _dist("mdna text chars", [len(b.mdna["text"]) for b in nb if b.mdna is not None])
    print(f"mdna truncated: {sum(1 for b in nb if b.mdna is not None and b.mdna['truncated'])}")
    _dist("absent-entry bytes", [_b(b.fundamentals_absent) + _b(b.mdna_absent) for b in nb])

    fund = merge_blocks(base, blocks).pack
    print(f"fund-v1 rendered user prompt bytes: {_prompt_bytes(fund):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
