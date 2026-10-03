"""Ranking-pot-v2 declaration loader and frozen v2 terms (#3592 slice 3c; spec ``2026-10-03-3592-ranking-pot-v2.md`` §8,
Appendix A (59)).

``load_declaration`` is v1's ``ranking_pot_rebalance.load_declaration`` keyed on ``ranking-pot-v2``: v1's loader is
untouched and never sees a v2 declaration, and this one never sees v1's (the strategy id is the only key either reads).

``FrozenTerms`` is the document's ``v2`` block: what the freeze measured and every v2 job reads back — the score strata
(§2, §6), the DTC baseline (§4 ``dtc_incomplete``) and the insider history floor with its threshold and per-month counts
(§8, Appendix A S2-a). The freeze (``ranking_pot_freeze_v2``, outside the hash) writes it through ``encode_frozen``; a
reader that finds it absent, of another schema or malformed raises, never defaults.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
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
    #: S₀ instrument id → its freeze-time score stratum (``ranking_pot_v2.score_strata``).
    strata: Mapping[int, int]
    #: S* at the freeze, the usable-DTC count there (the ``dtc_incomplete`` baseline) and the transaction time read at.
    dtc_settlement_date: date
    dtc_baseline: int
    dtc_read_at: datetime
    history_floor: date
    floor_threshold: Fraction
    #: Usable history rows per month, from the floor through the last complete month before the freeze.
    floor_counts: Mapping[date, int]


def encode_frozen(t: FrozenTerms) -> dict[str, Any]:
    """str/int/list/dict only, so the declaration's JSONB round trip keeps its sha."""
    return {
        "schema": FROZEN_SCHEMA,
        "strata": [[iid, s] for iid, s in sorted(t.strata.items())],
        "dtc_baseline": {
            "settlement_date": t.dtc_settlement_date.isoformat(),
            "usable": t.dtc_baseline,
            "read_at": t.dtc_read_at.astimezone(UTC).isoformat(),
        },
        "history_floor": {
            "floor": t.history_floor.isoformat(),
            "threshold": f"{t.floor_threshold.numerator}/{t.floor_threshold.denominator}",
            "month_counts": [[m.isoformat(), n] for m, n in sorted(t.floor_counts.items())],
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


def _pairs(v: object, what: str) -> list[tuple[object, object]]:
    if not isinstance(v, list) or not all(isinstance(p, list) and len(p) == 2 for p in v):
        raise _bad(what)
    return [(p[0], p[1]) for p in v]


def decode_frozen(doc: object, *, s0_ids: tuple[int, ...]) -> FrozenTerms:
    """The ``v2`` block, validated against the declaration's own S₀."""
    block = _object(doc, _KEYS, "block keys")
    if block["schema"] != FROZEN_SCHEMA:
        raise _bad(f"schema {block['schema']!r}")
    strata: dict[int, int] = {}
    for iid, s in _pairs(block["strata"], "strata"):
        stratum = _int(s, "stratum")
        if not 0 <= stratum <= v2.UNSCORED_STRATUM:
            raise _bad(f"stratum {stratum}")
        strata[_int(iid, "strata id")] = stratum
    if sorted(strata) != sorted(s0_ids) or len(strata) != len(block["strata"]):
        raise _bad("strata do not cover S₀ exactly once")
    if v2.strata_refusal(strata) is not None:
        raise _bad("a stratum below the minimum size")

    dtc = _object(block["dtc_baseline"], _DTC_KEYS, "dtc_baseline keys")
    baseline = _int(dtc["usable"], "dtc usable")
    if baseline <= 0:
        raise _bad("dtc baseline must be positive")
    if not isinstance(dtc["read_at"], str):
        raise _bad("dtc read_at")
    read_at = datetime.fromisoformat(dtc["read_at"])
    if read_at.tzinfo is None:
        raise _bad("dtc read_at is naive")

    floor_doc = _object(block["history_floor"], _FLOOR_KEYS, "history_floor keys")
    floor = _date(floor_doc["floor"], "floor")
    if floor.day != 1:
        raise _bad("floor is not a month start")
    if not isinstance(floor_doc["threshold"], str):
        raise _bad("threshold")
    try:
        threshold = Fraction(floor_doc["threshold"])
    except (ValueError, ZeroDivisionError) as exc:
        raise _bad("threshold") from exc
    if threshold <= 0:
        raise _bad("threshold must be positive")
    counts = {_date(m, "count month"): _int(n, "count") for m, n in _pairs(floor_doc["month_counts"], "month_counts")}
    if len(counts) != len(floor_doc["month_counts"]) or not counts or min(counts) != floor:
        raise _bad("month counts must start at the floor, once per month")
    months = sorted(counts)
    if any(m.day != 1 for m in months) or any(v2.months_back(b, 1) != a for a, b in zip(months, months[1:])):
        raise _bad("month counts are not consecutive month starts")
    if any(n < threshold for n in counts.values()):
        raise _bad("a counted month below the threshold the floor was set by")
    return FrozenTerms(
        strata=strata,
        dtc_settlement_date=_date(dtc["settlement_date"], "settlement_date"),
        dtc_baseline=baseline,
        dtc_read_at=read_at,
        history_floor=floor,
        floor_threshold=threshold,
        floor_counts=counts,
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
    "FrozenTerms",
    "decode_frozen",
    "encode_frozen",
    "frozen_terms",
    "load_declaration",
]
