"""Ranking-pot-v2 declaration loader and frozen v2 terms (#3592 slice 3c; spec ``2026-10-03-3592-ranking-pot-v2.md`` §8,
Appendix A (59)).

``load_declaration`` is v1's ``ranking_pot_rebalance.load_declaration`` keyed on ``ranking-pot-v2``: v1's loader is
untouched and never sees a v2 declaration, and this one never sees v1's (the strategy id is the only key either reads).

``FrozenTerms`` is the document's ``v2`` block: what the freeze measured and every v2 job reads back — the score strata
(§2, §6), the DTC baseline (§4 ``dtc_incomplete``) and the insider history floor with its threshold and per-month counts
(§8, Appendix A S2-a). The freeze (``ranking_pot_freeze_v2``, outside the hash) writes it through ``encode_frozen``.

**The block is self-verifying (spec §8 v6; Codex ckpt-1 slice 3c).** The freeze is unhashed, so its arithmetic is not
trusted: the block stores the INPUTS behind each derived term, and ``decode_frozen`` re-derives every term with this
module's hashed rules and raises on any disagreement — the strata from S₀'s stored scores (``score_strata``), the floor
and threshold from the stored month counts (``history_floor`` / ``floor_threshold``, over a window that holds every
month either rule reads), and the DTC baseline's bounds (≤ |S₀|, S* no later than the read and no older than
``DTC_MAX_AGE_DAYS``). An absent, other-schema or malformed block raises, never defaults.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.services import ranking_pot_v2 as v2
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_rebalance import PotDeclaration, SnapshotIntegrityError

Conn = psycopg.Connection[Any]

FROZEN_SCHEMA: Final = "ranking-pot-v2-frozen-1"
_KEYS: Final = frozenset({"schema", "strata", "dtc_baseline", "history_floor"})
_DTC_KEYS: Final = frozenset({"settlement_date", "usable", "read_at"})
_FLOOR_KEYS: Final = frozenset({"floor", "threshold", "month_counts"})


@dataclass(frozen=True)
class FrozenTerms:
    #: S₀ instrument id → its score in S₀'s own run (``None`` = unscored), the input the strata are derived from.
    scores: Mapping[int, Decimal | None]
    #: S₀ instrument id → its freeze-time score stratum (``ranking_pot_v2.score_strata`` over ``scores``).
    strata: Mapping[int, int]
    #: S* at the freeze, the usable-DTC count there (the ``dtc_incomplete`` baseline) and the transaction time read at,
    #: whose UTC month is the freeze month.
    dtc_settlement_date: date
    dtc_baseline: int
    dtc_read_at: datetime
    history_floor: date
    floor_threshold: Fraction
    #: Usable history rows per month (0 included) over ``count_window``: every month ``history_floor`` and
    #: ``floor_threshold`` read. The S2-a guard reads the months from the floor on.
    month_counts: Mapping[date, int]

    @property
    def freeze_month(self) -> date:
        return self.dtc_read_at.astimezone(UTC).date().replace(day=1)


def count_window(freeze_month: date, floor: date) -> tuple[date, ...]:
    """The months the floor rules read: the ``FLOOR_REFERENCE_MONTHS`` reference months, and the trailing run back to
    the first month below the threshold (the month before the run's first, i.e. two before the floor)."""
    first = min(v2.months_back(freeze_month, v2.FLOOR_REFERENCE_MONTHS), v2.months_back(floor, 2))
    out = [first]
    while (nxt := v2.months_back(out[-1], -1)) < freeze_month:
        out.append(nxt)
    return tuple(out)


def encode_frozen(t: FrozenTerms) -> dict[str, Any]:
    """str/int/list/dict only, so the declaration's JSONB round trip keeps its sha."""
    if t.dtc_read_at.tzinfo is None:
        raise ValueError("read_at must be timezone-aware")
    return {
        "schema": FROZEN_SCHEMA,
        "strata": [
            [iid, s, None if (score := t.scores[iid]) is None else str(score)] for iid, s in sorted(t.strata.items())
        ],
        "dtc_baseline": {
            "settlement_date": t.dtc_settlement_date.isoformat(),
            "usable": t.dtc_baseline,
            "read_at": t.dtc_read_at.astimezone(UTC).isoformat(),
        },
        "history_floor": {
            "floor": t.history_floor.isoformat(),
            "threshold": f"{t.floor_threshold.numerator}/{t.floor_threshold.denominator}",
            "month_counts": [[m.isoformat(), n] for m, n in sorted(t.month_counts.items())],
        },
    }


def _bad(what: str) -> ValueError:
    return ValueError(f"malformed ranking-pot-v2 frozen terms: {what}")


def _int(v: object, what: str) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise _bad(what)
    return v


def _date(v: object, what: str) -> date:
    if not isinstance(v, str):
        raise _bad(what)
    try:
        return date.fromisoformat(v)
    except ValueError as exc:
        raise _bad(what) from exc


def _object(v: object, keys: frozenset[str], what: str) -> Mapping[str, Any]:
    if not isinstance(v, dict) or set(v) != keys:
        raise _bad(what)
    return v


def _rows(v: object, width: int, what: str) -> list[list[Any]]:
    if not isinstance(v, list) or not all(isinstance(p, list) and len(p) == width for p in v):
        raise _bad(what)
    return v


def _score(v: object) -> Decimal | None:
    if v is None:
        return None
    if not isinstance(v, str):
        raise _bad("score")
    try:
        return Decimal(v)
    except InvalidOperation as exc:
        raise _bad("score") from exc


def decode_frozen(doc: object, *, s0_ids: tuple[int, ...]) -> FrozenTerms:
    """The ``v2`` block, every derived term re-derived from its stored inputs and checked against S₀."""
    block = _object(doc, _KEYS, "block keys")
    if block["schema"] != FROZEN_SCHEMA:
        raise _bad(f"schema {block['schema']!r}")
    rows = _rows(block["strata"], 3, "strata")
    strata = {_int(r[0], "strata id"): _int(r[1], "stratum") for r in rows}
    scores = {_int(r[0], "strata id"): _score(r[2]) for r in rows}
    if sorted(strata) != sorted(s0_ids) or len(strata) != len(rows) or len(s0_ids) != len(set(s0_ids)):
        raise _bad("strata do not cover S₀ exactly once")
    if strata != v2.score_strata(s0_ids, scores):
        raise _bad("strata are not score_strata of the stored scores")
    if v2.strata_refusal(strata) is not None:
        raise _bad("a stratum below the minimum size")

    dtc = _object(block["dtc_baseline"], _DTC_KEYS, "dtc_baseline keys")
    baseline = _int(dtc["usable"], "dtc usable")
    if not 0 < baseline <= len(s0_ids):
        raise _bad("dtc baseline must be in 1..|S₀|")
    if not isinstance(dtc["read_at"], str):
        raise _bad("dtc read_at")
    try:
        read_at = datetime.fromisoformat(dtc["read_at"])
    except ValueError as exc:
        raise _bad("dtc read_at") from exc
    if read_at.tzinfo is None:
        raise _bad("dtc read_at is naive")
    settle = _date(dtc["settlement_date"], "settlement_date")
    read_day = read_at.astimezone(UTC).date()
    if not read_day - timedelta(days=v2.DTC_MAX_AGE_DAYS) <= settle <= read_day:
        raise _bad("S* is after the read or older than the gate allows")

    floor_doc = _object(block["history_floor"], _FLOOR_KEYS, "history_floor keys")
    floor = _date(floor_doc["floor"], "floor")
    if not isinstance(floor_doc["threshold"], str):
        raise _bad("threshold")
    try:
        threshold = Fraction(floor_doc["threshold"])
    except (ValueError, ZeroDivisionError) as exc:
        raise _bad("threshold") from exc
    count_rows = _rows(floor_doc["month_counts"], 2, "month_counts")
    counts = {_date(r[0], "count month"): _int(r[1], "count") for r in count_rows}
    if any(n < 0 for n in counts.values()):
        raise _bad("a negative count")
    freeze_month = read_day.replace(day=1)
    if floor.day != 1 or not floor < freeze_month:
        raise _bad("the floor is not a month start before the freeze month")
    if len(counts) != len(count_rows) or tuple(sorted(counts)) != count_window(freeze_month, floor):
        raise _bad("month counts do not cover the floor rules' window exactly once")
    if v2.history_floor(counts, freeze_month=freeze_month) != floor:
        raise _bad("the floor is not history_floor of the stored counts")
    if v2.floor_threshold(counts, freeze_month=freeze_month) != threshold:
        raise _bad("the threshold is not floor_threshold of the stored counts")
    return FrozenTerms(
        scores=scores,
        strata=strata,
        dtc_settlement_date=settle,
        dtc_baseline=baseline,
        dtc_read_at=read_at,
        history_floor=floor,
        floor_threshold=threshold,
        month_counts=counts,
    )


def frozen_terms(decl: PotDeclaration) -> FrozenTerms:
    if decl.doc.get("strategy_id") != v2.STRATEGY_ID:
        raise _bad(f"declaration {decl.declaration_id} is not {v2.STRATEGY_ID}")
    if "v2" not in decl.doc:
        raise _bad("the v2 block is absent")
    return decode_frozen(decl.doc["v2"], s0_ids=decl.s0_ids)


def load_declaration(conn: Conn) -> PotDeclaration | None:
    """v2's non-terminal declaration (at most one, sql/445), its document integrity checked."""
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            """
            SELECT d.declaration_id, d.doc, d.doc_sha256, d.frozen_at,
                   (SELECT e.to_state FROM ranking_pot_state_events e
                     WHERE e.declaration_id = d.declaration_id ORDER BY e.event_id DESC LIMIT 1) AS state
              FROM ranking_pot_declarations d
             WHERE d.strategy_id = %(id)s
             ORDER BY d.declaration_id
            """,
            {"id": v2.STRATEGY_ID},
        ).fetchall()
    live = [r for r in rows if r["state"] != "completed"]
    if not live:
        return None
    if len(live) > 1:
        raise SnapshotIntegrityError(f"{len(live)} non-terminal {v2.STRATEGY_ID} declarations")
    r = live[0]
    if canonical_sha256(r["doc"]) != r["doc_sha256"]:
        raise SnapshotIntegrityError(f"declaration {r['declaration_id']} does not hash to its doc_sha256")
    return PotDeclaration(int(r["declaration_id"]), r["doc"], str(r["doc_sha256"]), r["frozen_at"], r["state"])


__all__ = [
    "FROZEN_SCHEMA",
    "count_window",
    "FrozenTerms",
    "decode_frozen",
    "encode_frozen",
    "frozen_terms",
    "load_declaration",
]
