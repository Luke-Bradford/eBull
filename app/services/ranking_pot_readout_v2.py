"""Ranking-pot-v2 readout: what §7 reports and never decides on (#3592 slice 4b-ii; spec
``2026-10-03-3592-ranking-pot-v2.md`` §7 "Readout" and "Invalidation").

``readout`` values any stepped session E ≥ T₀ from the append-only rows v2's looks read, composing v1's readout
(``ranking_pot_readout``): its accumulator streams the shadow, the K controls and the variant exactly as for v1, and
this module adds the v1-reference book (K + 2) beside them and the stored looks. Read-only: it writes nothing and
gates nothing.

**Outside v2's hash, deliberately (as the freeze, ``ranking_pot_freeze_v2``).** The name is not a ``ranking_pot_v2*``
root and no root imports it, so §8's union does not reach it. Hashing it would hash what v1's readout imports —
``trial_register`` (bumped with every register entry, so v2 would drift on each) and the executed-book
``ranking_pot_exec_readout`` (barred from v2's C). A readout decides nothing: the verdict is the look row, computed
under v2's hash by ``ranking_pot_v2_look``.

What differs from v1's readout, and nothing else:

- **Missing donors.** v2's controls store ``decision.missing_donor_dtc`` (an R member whose donor has no DTC; each
  member keeps its own score, so v1's missing-score count has no meaning here). The row adapter hands it to v1's
  accumulator in the ``missing_donors`` slot, whose arithmetic (share of R per control, median and max) is the same,
  and the output keys are renamed ``missing_donor_dtc`` / ``missing_donor_dtc_share``.
- **No executed book.** v1's ``executed`` section does not exist (sql/463).
- **The v1-reference book (§7).** Its stored step document streams through its own ``LookFacts`` (no controls), as
  the variant's does. Reported: per session both books' session sums, counts and cumulative T; per applied rebalance
  both books' decided entrants and their overlap, entries per slot and buy turnover (``bought`` / the book's NAV
  before the target session, v1's measure); the window's NAV return, maximum drawdown, T, occupancy, lifecycle
  distribution, entry refusals and exits by reason; and its exposures beside the shadow's.
- **Looks.** Every stored ``result`` with v1's verdict and v2's (``ranking_pot_v2_look.v2_detail_of``, which raises on
  a malformed block), and whether an ``invalidation`` row cites it (an invalidated look is shown as invalidated, §7).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, localcontext
from typing import Any

import psycopg

from app.services import ranking_pot_exposure as ex
from app.services import ranking_pot_look as look
from app.services import ranking_pot_readout as r1
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_v2_look as v2look
from app.services.ranking_pot_v2_job import policy_ok
from app.services.ranking_pot_v2_policy import RANKING_POT_V2_POLICY_HASH

Conn = psycopg.Connection[Any]

_s = r1._s


def _v1_controls(controls: Mapping[str, Any]) -> dict[str, Any]:
    """v2's control columns in v1's shape: ``decision.missing_donor_dtc`` in the ``missing_donors`` slot."""
    out = dict(controls)
    dec = controls.get("decision")
    if dec is not None:
        if "missing_donors" in dec or "missing_donor_dtc" not in dec:
            raise rb.SnapshotIntegrityError("a v2 control decision without missing_donor_dtc (or with v1's field)")
        out["decision"] = {("missing_donors" if k == "missing_donor_dtc" else k): v for k, v in dec.items()}
    return out


@dataclass(frozen=True)
class V2Row:
    row: r1.ReadoutRow
    reference: Mapping[str, Any]


def _book(doc: Mapping[str, Any]) -> tuple[Decimal, int, Mapping[str, Any]]:
    return Decimal(doc["bought"]), int(doc["entered"]), doc.get("decision") or {}


@dataclass
class ReferenceFacts:
    """The reference book (K + 2) beside v1's accumulator, streamed in the same order."""

    acc: r1.ReadoutFacts
    facts: look.LookFacts = field(init=False)
    entry_session: dict[int, date] = field(default_factory=dict)
    exit_reasons: Counter[str] = field(default_factory=Counter)
    refusals: int = 0
    exposure: ex.Sums = field(default_factory=ex.Sums)
    sessions: list[list[Any]] = field(default_factory=list)
    per_rebalance: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.facts = look.LookFacts(t0=self.acc.t0, endpoint=self.acc.endpoint, k=0)

    def add(self, v: V2Row) -> None:
        with localcontext(sim.CTX):
            self._add(v)

    def _add(self, v: V2Row) -> None:
        row, ref, sh, f = v.row, v.reference, self.acc.facts, self.facts
        index = sh.sessions  # the path index before this session
        before = (sh.shadow_sum, sh.shadow_count, f.shadow_sum, f.shadow_count)
        self.acc.add(row)
        f.add(look.StepRow(row.session, row.forced, ref, r1._NO_CONTROLS, row.spy))
        for r in ref["records"]:
            self.entry_session.setdefault(int(r[1]), row.session)
        for c in ref["closed"]:
            self.exit_reasons[str(c[9])] += 1
        self.refusals += len(ref["refusals"])
        self.exposure.add(ex.Sums.of_doc(ref["exposure"]))
        self.sessions.append(
            [
                row.session.isoformat(),
                _s(sh.shadow_sum - before[0]),
                sh.shadow_count - before[1],
                _s(look._t(sh.shadow_sum, sh.shadow_count)),
                _s(f.shadow_sum - before[2]),
                f.shadow_count - before[3],
                _s(look._t(f.shadow_sum, f.shadow_count)),
            ]
        )
        if row.applied_attempt_id is not None:
            self._rebalance(row, ref, index)

    def _rebalance(self, row: r1.ReadoutRow, ref: Mapping[str, Any], index: int) -> None:
        n = Decimal(self.acc.n)
        s_bought, s_entered, s_dec = _book(row.shadow)
        r_bought, r_entered, r_dec = _book(ref)
        if not s_dec or not r_dec:
            raise rb.SnapshotIntegrityError(f"{row.session}: an applied rebalance without both books' decisions")
        s_in, r_in = [int(i) for i in s_dec["entries"]], [int(i) for i in r_dec["entries"]]
        self.per_rebalance.append(
            {
                "attempt_id": row.applied_attempt_id,
                "target_session": row.session.isoformat(),
                "entrants": {"shadow": s_in, "reference": r_in, "overlap": len(set(s_in) & set(r_in))},
                "entered_per_n": {"shadow": _s(s_entered / n), "reference": _s(r_entered / n)},
                "turnover": {
                    "shadow": _s(s_bought / self.acc.facts.shadow_path[index]),
                    "reference": _s(r_bought / self.facts.shadow_path[index]),
                },
            }
        )

    def doc(self) -> dict[str, Any]:
        with localcontext(sim.CTX):
            s, f, n = self.acc.facts, self.facts, self.acc.n
            shadow_return, ref_return = s.shadow_path[-1] - 1, f.shadow_path[-1] - 1
            return {
                "per_session": self.sessions,
                "per_session_columns": [
                    "session",
                    "shadow_sum",
                    "shadow_count",
                    "shadow_t",
                    "reference_sum",
                    "reference_count",
                    "reference_t",
                ],
                "per_rebalance": self.per_rebalance,
                "nav_return": {
                    "shadow": _s(shadow_return),
                    "reference": _s(ref_return),
                    "shadow_minus_reference": _s(shadow_return - ref_return),
                },
                "max_drawdown": {
                    "shadow": _s(look.max_drawdown(s.shadow_path)),
                    "reference": _s(look.max_drawdown(f.shadow_path)),
                },
                "t": {
                    "shadow": _s(look._t(s.shadow_sum, s.shadow_count)),
                    "reference": _s(look._t(f.shadow_sum, f.shadow_count)),
                },
                "occupancy": {
                    "shadow": _s(Decimal(s.held_total) / (s.sessions * n)),
                    "reference": _s(Decimal(f.held_total) / (f.sessions * n)),
                },
                "lifecycles": r1.lifecycle_distribution(self.acc._lifecycles(f, self.entry_session)),
                "entry_refusals": {"shadow": self.acc.shadow_refusals, "reference": self.refusals},
                "exits_by_reason": {
                    "shadow": dict(sorted(self.acc.exit_reasons.items())),
                    "reference": dict(sorted(self.exit_reasons.items())),
                },
                "exposure": self.exposure.window(),
            }


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
def _rows(conn: Conn, declaration_id: int, *, t0: date, end: date) -> Iterator[V2Row]:
    """v1's ``_rows`` plus the reference column; K-wide control columns never sit in memory together."""
    fields = ", ".join(f"'{f}', controls -> '{f}'" for f in r1._STEP_CONTROL_FIELDS)
    with conn.cursor(name="ranking_pot_v2_readout_rows") as cur:
        cur.itersize = 8
        cur.execute(
            "SELECT session, forced, applied_attempt_id, wind_down_event_id IS NOT NULL, shadow, "
            f"inputs -> 'spy', jsonb_build_object({fields}), variant, characteristics, reference "  # noqa: S608
            "FROM ranking_pot_steps WHERE declaration_id = %s AND session BETWEEN %s AND %s ORDER BY session",
            (declaration_id, t0, end),
        )
        for session, forced, applied, wind, shadow, spy, controls, variant, table, reference in cur:
            if reference is None:
                raise rb.SnapshotIntegrityError(f"{session}: a v2 step row without its reference book")
            bar = None if spy is None else sim.Bar(*(Decimal(x) for x in spy))
            row = r1.ReadoutRow(session, forced, applied, wind, shadow, _v1_controls(controls), bar, variant, table)
            yield V2Row(row, reference)


def looks(conn: Conn, declaration_id: int) -> list[dict[str, Any]]:
    """Every stored ``result`` with both verdicts and its invalidation, if any (§7: shown as invalidated)."""
    rows = conn.execute(
        "SELECT r.look_id, r.look_months, r.endpoint_session, r.verdict, r.harm, r.reasons, r.detail, "
        "       i.look_id, i.note, i.recorded_at "
        "FROM ranking_pot_looks r "
        "LEFT JOIN LATERAL (SELECT look_id, note, recorded_at FROM ranking_pot_looks x "
        "                   WHERE x.cites_look_id = r.look_id AND x.kind = 'invalidation' "
        "                   ORDER BY x.look_id LIMIT 1) i ON TRUE "
        "WHERE r.declaration_id = %s AND r.kind = 'result' ORDER BY r.look_months",
        (declaration_id,),
    ).fetchall()
    out = []
    for look_id, months, end, verdict, harm, reasons, detail, inv_id, inv_note, inv_at in rows:
        decoded = v2look.v2_detail_of(detail)
        out.append(
            {
                "look_id": look_id,
                "look_months": months,
                "endpoint": end.isoformat(),
                "v1_verdict": verdict,
                "v2_verdict": decoded.v2_verdict,
                "harm": harm,
                "reasons": list(reasons) + list(decoded.reasons),
                "conditions": {"6_reference": decoded.condition_6, "7_turnover": decoded.condition_7},
                "invalidated": None
                if inv_id is None
                else {"look_id": inv_id, "note": inv_note, "recorded_at": inv_at.isoformat()},
            }
        )
    return out


def readout(conn: Conn, decl: rb.PotDeclaration, endpoint: date) -> dict[str, Any]:
    """The §7 readout at a stepped session E ≥ T₀. Runs its own REPEATABLE READ READ ONLY transaction."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("readout opens its own transaction")
    t = decl.doc["terms"]
    if t.get("book_count") != int(t["k_controls"]) + 3:
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: book_count is not K + 3")
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        t0 = look.first_target_session(conn, decl.declaration_id)
        if t0 is None or endpoint < t0:
            raise ValueError(f"declaration {decl.declaration_id}: no stepped window ends at {endpoint}")
        terms = look.terms_of(decl)
        spy = r1._spy_closes(conn)
        acc = r1.ReadoutFacts(
            t0=t0,
            endpoint=endpoint,
            n=terms.n,
            k=terms.k,
            rebalances=r1._rebalances(conn, decl.declaration_id, endpoint, spy),
        )
        reference = ReferenceFacts(acc)
        for row in _rows(conn, decl.declaration_id, t0=t0, end=endpoint):
            reference.add(row)
        spreads = look._spy_half_spreads(conn, decl.declaration_id, endpoint)
        if not spreads or spreads[0][0] != t0:
            raise rb.SnapshotIntegrityError("the first decided snapshot does not target T0")
        out = acc.finish(h0=spreads[0][1], h_end=spreads[-1][1])
        out["missing_donor_dtc"] = out.pop("missing_donors")
        for entry in out["turnover_occupancy"]["per_rebalance"]:
            entry["missing_donor_dtc_share"] = entry.pop("missing_donor_share")
        out["reference"] = reference.doc()
        out["looks"] = looks(conn, decl.declaration_id)
    return out | {
        "declaration_id": decl.declaration_id,
        "policy_hash": RANKING_POT_V2_POLICY_HASH,
        "declared_policy_hash": decl.doc.get("policy_hash"),
        "policy_drift": not policy_ok(decl),
    }


__all__ = ["ReferenceFacts", "V2Row", "looks", "readout"]
