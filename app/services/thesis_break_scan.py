"""Nightly thesis-break scan — DB-facing side of #2012 (PR-A).

One read assembling the closed-vocabulary observations for the fire
population (LATEST thesis per instrument), then pure calls into
``thesis_break`` (extract → observe → evaluate → state machine), then
writes: predicate upserts, baseline-state transitions, and at most one
``thesis_break_events`` row per predicate per thesis version.

Altman sector gate (spec "Closed vocabulary"): Altman Z″ excludes
financial firms (Altman 2000), and the upstream ``gics_sector ==
"Financials"`` suppression misses Real Estate (GICS split it out of
Financials in 2016 — sector_classification.py maps SIC 65xx/6798 to
XLRE). Gated here at PREDICATE INSERT on SIC division H (60-67: finance,
insurance, real estate) — a superset of the spec's named 60-64/65xx/6798,
aligned to the SIC division the source rule actually excludes. Gated
conditions stay prose; a permanently-unevaluable pending row would lie.

Mirrors thesis_dq_audit's split (#2014 precedent): pure predicates in
``thesis_break.py``, one DB reader/writer here.

Spec: docs/proposals/thesis/2026-07-16-thesis-break-predicates.md.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.services.instrument_analytics import read_latest_fy_altman
from app.services.thesis_break import (
    METRIC_UNITS,
    BaselineState,
    BreakPredicate,
    MetricInput,
    MetricObservation,
    evaluate_predicate,
    extract_predicates,
    next_baseline_state,
    observe,
    sanitize_writer_break_predicates,
)

# SIC division H — finance, insurance, and real estate (Altman gate).
_FINANCIAL_SIC2 = frozenset({"60", "61", "62", "63", "64", "65", "66", "67"})

_ALTMAN_METRICS = frozenset({"altman_z"})
_PRICE_METRICS = frozenset({"rsi_14", "price_vs_sma200", "sma_50_vs_sma_200"})
_SI_METRICS = frozenset({"short_interest_pct_shares_out", "short_interest_days_to_cover", "short_interest_change_pct"})


@dataclass(frozen=True)
class ThesisBreakScanReport:
    scanned_theses: int
    predicates_total: int
    predicates_inserted: int
    sector_gated: int
    fired: int
    state_counts: dict[str, int]
    eval_counts: dict[str, int]


def _latest_theses(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    """Latest thesis per instrument — the #2014 tiebreak (created_at DESC,
    thesis_version DESC, thesis_id DESC), deterministic under same-second
    inserts."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (t.instrument_id)
                t.instrument_id, t.thesis_id, t.created_at, t.break_conditions_json,
                t.break_predicates_json
            FROM theses t
            ORDER BY t.instrument_id, t.created_at DESC, t.thesis_version DESC, t.thesis_id DESC
            """
        )
        return cur.fetchall()


def _financial_instruments(conn: psycopg.Connection[Any], instrument_ids: list[int]) -> set[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id FROM instrument_sec_profile
            WHERE instrument_id = ANY(%(ids)s) AND sic2 = ANY(%(sic2s)s)
            """,
            {"ids": instrument_ids, "sic2s": sorted(_FINANCIAL_SIC2)},
        )
        return {int(r[0]) for r in cur.fetchall()}


def _price_observations(
    conn: psycopg.Connection[Any], instrument_ids: list[int], today: date
) -> dict[int, dict[str, MetricObservation]]:
    """rsi_14 / price_vs_sma200 / sma_50_vs_sma_200 from the latest
    price_daily row. Regime values are signed gaps (op '<' ⇒ gap < 0);
    a structurally-absent sma (needs 200 trading days) is no_input."""
    out: dict[int, dict[str, MetricObservation]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id)
                instrument_id, price_date, close, sma_50, sma_200, rsi_14
            FROM price_daily
            WHERE instrument_id = ANY(%(ids)s)
            ORDER BY instrument_id, price_date DESC
            """,
            {"ids": instrument_ids},
        )
        for iid, price_date, close, sma_50, sma_200, rsi in cur.fetchall():
            obs: dict[str, MetricObservation] = {}
            obs["rsi_14"] = observe(
                "rsi_14",
                {"price_date": None if rsi is None else MetricInput(float(rsi), price_date, "price_daily")},
                None if rsi is None else float(rsi),
                today=today,
            )
            gap_price = None if close is None or sma_200 is None else float(close) - float(sma_200)
            obs["price_vs_sma200"] = observe(
                "price_vs_sma200",
                {"price_date": None if gap_price is None else MetricInput(gap_price, price_date, "price_daily")},
                gap_price,
                today=today,
            )
            gap_sma = None if sma_50 is None or sma_200 is None else float(sma_50) - float(sma_200)
            obs["sma_50_vs_sma_200"] = observe(
                "sma_50_vs_sma_200",
                {"price_date": None if gap_sma is None else MetricInput(gap_sma, price_date, "price_daily")},
                gap_sma,
                today=today,
            )
            out[int(iid)] = obs
    return out


def _short_interest_observations(
    conn: psycopg.Connection[Any], instrument_ids: list[int], today: date
) -> dict[int, dict[str, MetricObservation]]:
    """FINRA bimonthly current snapshot + dei/gaap shares outstanding.
    days_to_cover and change_percent are computed BY FINRA (no denominator
    of ours); the pct metric divides by share_count_history.shares_outstanding
    — the settled short_interest_signal denominator — with BOTH inputs
    independently freshness-bounded."""
    out: dict[int, dict[str, MetricObservation]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT si.instrument_id, si.settlement_date, si.current_short_interest,
                   si.days_to_cover, si.change_percent,
                   sc.shares_outstanding, sc.latest_filed_date
            FROM finra_short_interest_current si
            LEFT JOIN LATERAL (
                SELECT h.shares_outstanding, h.latest_filed_date
                FROM share_count_history h
                WHERE h.instrument_id = si.instrument_id AND h.shares_outstanding IS NOT NULL
                ORDER BY h.period_end DESC
                LIMIT 1
            ) sc ON TRUE
            WHERE si.instrument_id = ANY(%(ids)s)
            """,
            {"ids": instrument_ids},
        )
        for iid, settle, si_shares, dtc, chg, shares_out, filed in cur.fetchall():
            finra = None if settle is None else MetricInput(float(si_shares), settle, "finra_short_interest_current")
            obs: dict[str, MetricObservation] = {}
            obs["short_interest_days_to_cover"] = observe(
                "short_interest_days_to_cover",
                {
                    "finra_settlement": None
                    if dtc is None
                    else MetricInput(float(dtc), settle, "finra_short_interest_current")
                },
                None if dtc is None else float(dtc),
                today=today,
            )
            obs["short_interest_change_pct"] = observe(
                "short_interest_change_pct",
                {
                    "finra_settlement": None
                    if chg is None
                    else MetricInput(float(chg), settle, "finra_short_interest_current")
                },
                None if chg is None else float(chg),
                today=today,
            )
            pct = (
                None
                if si_shares is None or shares_out is None or float(shares_out) <= 0
                else 100.0 * float(si_shares) / float(shares_out)
            )
            obs["short_interest_pct_shares_out"] = observe(
                "short_interest_pct_shares_out",
                {
                    "finra_settlement": finra,
                    "share_count_filed": (
                        None if shares_out is None else MetricInput(float(shares_out), filed, "share_count_history")
                    ),
                },
                pct,
                today=today,
            )
            out[int(iid)] = obs
    return out


def _altman_observation(conn: psycopg.Connection[Any], instrument_id: int, today: date) -> MetricObservation:
    result, period_end = read_latest_fy_altman(conn, instrument_id)
    z = result.z
    return observe(
        "altman_z",
        {"fy_period_end": None if z is None else MetricInput(float(z), period_end, "financial_facts_raw")},
        None if z is None else float(z),
        today=today,
    )


def _inputs_payload(obs: MetricObservation) -> dict[str, dict[str, Any]]:
    return {
        name: {"value": inp.value, "as_of": None if inp.as_of is None else inp.as_of.isoformat(), "source": inp.source}
        for name, inp in obs.inputs.items()
    }


def run_thesis_break_scan(conn: psycopg.Connection[Any], *, now: datetime | None = None) -> ThesisBreakScanReport:
    now = now or datetime.now(tz=UTC)
    today = now.date()

    theses = _latest_theses(conn)
    if not theses:
        return ThesisBreakScanReport(0, 0, 0, 0, 0, {}, {})
    financial = _financial_instruments(conn, [int(t["instrument_id"]) for t in theses])

    # -- extract + upsert predicate rows for the latest theses ------------
    sector_gated = 0
    to_insert: list[tuple[int, int, int, BreakPredicate, str]] = []
    for t in theses:
        conditions = t["break_conditions_json"]
        if not isinstance(conditions, list):
            continue
        iid = int(t["instrument_id"])
        # #2010 writer-native channel — PURELY ADDITIVE recall: a validated
        # writer predicate fills an index ONLY where the extractor returned
        # None. The 100%-precision extractor channel is never overridden, so
        # a hallucinated writer twin can never suppress a correct extraction.
        # Re-sanitize on read (fail-open): rows written before sql/232, or a
        # hand-edited jsonb, must not crash the scan.
        writer_preds, _ = sanitize_writer_break_predicates(t["break_predicates_json"], len(conditions))
        # Sanitizer guarantees condition_index is an int; the isinstance
        # narrows for the type checker without an assert in the prod path.
        writer_by_idx = {idx: p for p in writer_preds if isinstance(idx := p["condition_index"], int)}
        # Pass the RAW array — extract_predicates None-maps non-string
        # elements itself, keeping predicate_index aligned with the
        # original break_conditions_json slots (a pre-filter would shift
        # every index after a malformed element).
        for idx, extracted in enumerate(extract_predicates(list(conditions))):
            origin = "extractor"
            pred = extracted
            if pred is None:
                wp = writer_by_idx.get(idx)
                if wp is None:
                    continue
                threshold = wp["threshold"]
                pred = BreakPredicate(
                    metric=str(wp["metric"]),
                    op="<" if wp["op"] == "<" else ">",
                    threshold=float(threshold) if threshold is not None else None,  # type: ignore[arg-type]
                    unit=METRIC_UNITS[str(wp["metric"])],
                    source_text=str(conditions[idx]),
                )
                origin = "writer"
            if pred.metric in _ALTMAN_METRICS and iid in financial:
                sector_gated += 1
                continue
            to_insert.append((int(t["thesis_id"]), idx, iid, pred, origin))

    inserted_keys: set[tuple[int, int]] = set()
    with conn.transaction():
        for thesis_id, idx, iid, pred, origin in to_insert:
            row = conn.execute(
                """
                INSERT INTO thesis_break_predicates
                    (thesis_id, predicate_index, instrument_id, metric, op, threshold, unit,
                     source_text, origin)
                VALUES (%(tid)s, %(idx)s, %(iid)s, %(metric)s, %(op)s, %(threshold)s, %(unit)s,
                        %(src)s, %(origin)s)
                ON CONFLICT (thesis_id, predicate_index) DO NOTHING
                RETURNING thesis_id, predicate_index
                """,
                {
                    "tid": thesis_id,
                    "idx": idx,
                    "iid": iid,
                    "metric": pred.metric,
                    "op": pred.op,
                    "threshold": pred.threshold,
                    "unit": pred.unit,
                    "src": pred.source_text,
                    "origin": origin,
                },
            ).fetchone()
            if row is not None:
                inserted_keys.add((int(row[0]), int(row[1])))

    # -- load live predicate rows for the fire population -----------------
    latest_ids = [int(t["thesis_id"]) for t in theses]
    created_by_thesis = {int(t["thesis_id"]): t["created_at"] for t in theses}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT thesis_id, predicate_index, instrument_id, metric, op, threshold, unit,
                   source_text, baseline_state
            FROM thesis_break_predicates
            WHERE thesis_id = ANY(%(ids)s)
            """,
            {"ids": latest_ids},
        )
        rows = cur.fetchall()

    # -- observations, per metric family, only where needed ---------------
    price_ids = sorted({int(r["instrument_id"]) for r in rows if r["metric"] in _PRICE_METRICS})
    si_ids = sorted({int(r["instrument_id"]) for r in rows if r["metric"] in _SI_METRICS})
    altman_ids = sorted({int(r["instrument_id"]) for r in rows if r["metric"] in _ALTMAN_METRICS})
    price_obs = _price_observations(conn, price_ids, today) if price_ids else {}
    si_obs = _short_interest_observations(conn, si_ids, today) if si_ids else {}
    altman_obs = {iid: _altman_observation(conn, iid, today) for iid in altman_ids}

    def _observation(iid: int, metric: str) -> MetricObservation:
        if metric in _PRICE_METRICS:
            return price_obs.get(iid, {}).get(metric, MetricObservation(metric=metric, status="no_input"))
        if metric in _SI_METRICS:
            return si_obs.get(iid, {}).get(metric, MetricObservation(metric=metric, status="no_input"))
        return altman_obs.get(iid, MetricObservation(metric=metric, status="no_input"))

    # -- evaluate + state machine + writes ---------------------------------
    fired = 0
    state_counts: Counter[str] = Counter()
    eval_counts: Counter[str] = Counter()
    with conn.transaction():
        for r in rows:
            key = (int(r["thesis_id"]), int(r["predicate_index"]))
            iid = int(r["instrument_id"])
            pred = BreakPredicate(
                metric=str(r["metric"]),
                op=r["op"],  # type: ignore[arg-type]
                threshold=None if r["threshold"] is None else float(r["threshold"]),
                unit=str(r["unit"]),
                source_text=str(r["source_text"]),
            )
            obs = _observation(iid, pred.metric)
            result = evaluate_predicate(pred, obs)
            eval_counts[result] += 1
            state: BaselineState = r["baseline_state"]
            new_state, fire = next_baseline_state(
                state,
                result,
                newly_inserted=key in inserted_keys,
                thesis_created_at=created_by_thesis[key[0]],
                now=now,
            )
            if new_state != state:
                conn.execute(
                    """
                    UPDATE thesis_break_predicates
                    SET baseline_state = %(state)s,
                        baselined_at = COALESCE(baselined_at, %(now)s)
                    WHERE thesis_id = %(tid)s AND predicate_index = %(idx)s
                    """,
                    {"state": new_state, "now": now, "tid": key[0], "idx": key[1]},
                )
            if fire:
                assert obs.value is not None  # fire implies an ok observation
                row = conn.execute(
                    """
                    INSERT INTO thesis_break_events
                        (thesis_id, predicate_index, instrument_id, metric, op, threshold,
                         observed_value, observed_as_of, inputs_json)
                    VALUES (%(tid)s, %(idx)s, %(iid)s, %(metric)s, %(op)s, %(threshold)s,
                            %(value)s, %(as_of)s, %(inputs)s)
                    ON CONFLICT (thesis_id, predicate_index) DO NOTHING
                    RETURNING break_event_id
                    """,
                    {
                        "tid": key[0],
                        "idx": key[1],
                        "iid": iid,
                        "metric": pred.metric,
                        "op": pred.op,
                        "threshold": pred.threshold,
                        "value": obs.value,
                        "as_of": obs.as_of,
                        "inputs": Jsonb(_inputs_payload(obs)),
                    },
                ).fetchone()
                if row is not None:
                    fired += 1
            state_counts[new_state] += 1

    return ThesisBreakScanReport(
        scanned_theses=len(theses),
        predicates_total=len(rows),
        predicates_inserted=len(inserted_keys),
        sector_gated=sector_gated,
        fired=fired,
        state_counts=dict(state_counts),
        eval_counts=dict(eval_counts),
    )
