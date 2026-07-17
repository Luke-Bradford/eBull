"""Calibration-ledger outcome capture — nightly deterministic job (#2002).

Theses are versioned, dated forecasts; nothing ever scored them against
what the market subsequently did. This module captures realized returns
per THESIS VERSION at fixed horizons (30/90/365d) into the append-only
``thesis_outcomes`` ledger. Passive measurement only: no LLM, no scoring
feed, no trade-path contact, zero cloud spend.

Contracts (spec: docs/proposals/thesis/2026-07-16-calibration-ledger-schema.md):

* Anchor semantics = #2014/#2017: the write-time reference is the minting
  run's ``context_summary.blocks.price_anchor`` (persisted PRE-LLM); the
  trusted close is re-read deterministically from ``price_daily`` at or
  before ``as_of`` — never taken from a JSON copy.
* Maturity is DATA-anchored, not wall-clock: a (thesis, horizon) pair is
  mature when ``max(price_date) >= anchor_date + horizon``. A stale
  series defers capture; it can never mint a wrong-horizon row.
* Insert-once: PK (thesis_id, horizon_days) + ON CONFLICT DO NOTHING.
  Re-runs insert nothing. Rows are never updated.
* Honest missingness: anchorless theses (pre-#2017 rows, unavailable or
  unparseable anchors) get NO rows — a queryable gap, never a neutral
  value. Non-positive closes are skipped and counted, never written.

Pure predicates are module-level functions (table-testable, no DB);
``capture_thesis_outcomes`` is the one DB reader+writer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.services.thesis_dq_audit import _anchor_block, _parse_iso_date, close_row_at_or_before

HORIZONS: tuple[int, ...] = (30, 90, 365)

METHOD_VERSION = "oc_v1"

# A never-matured pair whose price series ended more than this many days
# before the horizon due date is a dead series (delisted/acquired/halted),
# not temporarily stale data. No outcome row is ever written for it — a
# return against a halted print is not a market outcome.
_SERIES_DEAD_GRACE_DAYS = 30


@dataclass(frozen=True)
class ThesisOutcomeCaptureReport:
    """Census of one capture run. ``inserted`` is the job row_count
    (0 = healthy steady state on quiet days); everything else is honest
    missingness, first-class by spec."""

    scanned_theses: int = 0
    anchorless: int = 0
    inserted: int = 0
    mature_pairs: int = 0
    immature_data_current: int = 0
    immature_series_stalled: int = 0
    series_dead: int = 0
    skipped_missing_close: int = 0
    skipped_nonpositive_close: int = 0


def anchor_date_from_summary(summary: object) -> date | None:
    """The usable anchor date of a minting run, or None.

    Usable = ``price_anchor.available`` is True AND ``as_of`` parses as an
    ISO date. Anything else (no summary, no block, available false,
    unparseable as_of) is anchorless — the thesis gets no ledger rows.
    """
    anchor = _anchor_block(summary)
    if anchor is None or anchor.get("available") is not True:
        return None
    return _parse_iso_date(anchor.get("as_of"))


def is_mature(anchor_date: date, horizon_days: int, max_price_date: date | None) -> bool:
    """DATA-anchored maturity: the series has printed at or past the due
    date. Wall-clock never participates."""
    if max_price_date is None:
        return False
    return max_price_date >= anchor_date + timedelta(days=horizon_days)


def classify_immature(
    *,
    is_tradable: bool,
    max_price_date: date | None,
    due_date: date,
    today: date,
) -> str:
    """Split never-matured pairs so a dead series is not indistinguishable
    from temporarily stale data (spec, Codex ckpt-1 Medium).

    * ``immature_data_current`` — series is alive (tradable, printed within
      the grace window of today): the pair simply isn't due yet.
    * ``series_dead`` — the series ended more than the grace window before
      the due date: the print this pair needs will never come. A rowless
      series is dead only when the instrument is also untradable — for a
      tradable instrument, no rows is absent data (ingest gap / fresh
      listing), never a terminal verdict (Codex ckpt-2 Medium).
    * ``immature_series_stalled`` — series has stopped (untradable, no
      recent print, or no rows yet while tradable) but not provably dead.

    ``today`` participates only in the alive-vs-stopped liveness split —
    the counters are telemetry; row insertion is purely data-anchored.
    """
    ended = not is_tradable or max_price_date is None or (today - max_price_date).days > _SERIES_DEAD_GRACE_DAYS
    if not ended:
        return "immature_data_current"
    if max_price_date is None:
        return "series_dead" if not is_tradable else "immature_series_stalled"
    if (due_date - max_price_date).days > _SERIES_DEAD_GRACE_DAYS:
        return "series_dead"
    return "immature_series_stalled"


# All thesis versions with at least one horizon uncaptured. The run
# LATERAL takes the latest context_summary-BEARING run per thesis (spec:
# availability truth is the persisted PRE-LLM summary, #2017); theses
# whose runs all lack a summary come back with NULL and count as
# anchorless. cardinality < n_horizons keeps the scan bounded to
# unfinished theses without repeating the horizon list in SQL.
_CANDIDATES_SQL = """
SELECT t.thesis_id, t.instrument_id, i.is_tradable,
       r.context_summary,
       p.max_price_date,
       COALESCE(o.done, '{}') AS horizons_done
FROM theses t
JOIN instruments i ON i.instrument_id = t.instrument_id
LEFT JOIN LATERAL (
    SELECT context_summary
    FROM thesis_runs r
    WHERE r.thesis_id = t.thesis_id AND r.context_summary IS NOT NULL
    ORDER BY r.run_id DESC
    LIMIT 1
) r ON TRUE
LEFT JOIN LATERAL (
    SELECT max(price_date) AS max_price_date
    FROM price_daily p
    WHERE p.instrument_id = t.instrument_id
) p ON TRUE
LEFT JOIN LATERAL (
    SELECT array_agg(o.horizon_days) AS done
    FROM thesis_outcomes o
    WHERE o.thesis_id = t.thesis_id
) o ON TRUE
WHERE COALESCE(cardinality(o.done), 0) < %(n_horizons)s
ORDER BY t.thesis_id
"""

_INSERT_SQL = """
INSERT INTO thesis_outcomes
    (thesis_id, horizon_days, anchor_date, anchor_close,
     realized_date, realized_close, realized_return, method_version)
VALUES
    (%(thesis_id)s, %(horizon_days)s, %(anchor_date)s, %(anchor_close)s,
     %(realized_date)s, %(realized_close)s, %(realized_return)s, %(method_version)s)
ON CONFLICT (thesis_id, horizon_days) DO NOTHING
"""


def capture_thesis_outcomes(conn: psycopg.Connection[Any]) -> ThesisOutcomeCaptureReport:
    """Insert realized outcomes for every mature, uncaptured
    (thesis, horizon) pair. Idempotent; deterministic; append-only."""
    today = date.today()
    scanned = anchorless = inserted = mature_pairs = 0
    immature_current = immature_stalled = dead = 0
    missing_close = nonpositive = 0

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_CANDIDATES_SQL, {"n_horizons": len(HORIZONS)})
        candidates = cur.fetchall()

    with conn.transaction():
        for row in candidates:
            scanned += 1
            anchor_date = anchor_date_from_summary(row["context_summary"])
            if anchor_date is None:
                anchorless += 1
                continue
            done = set(row["horizons_done"] or ())
            max_price_date = row["max_price_date"]
            mature_missing: list[int] = []
            for horizon in HORIZONS:
                if horizon in done:
                    continue
                if is_mature(anchor_date, horizon, max_price_date):
                    mature_missing.append(horizon)
                    continue
                kind = classify_immature(
                    is_tradable=bool(row["is_tradable"]),
                    max_price_date=max_price_date,
                    due_date=anchor_date + timedelta(days=horizon),
                    today=today,
                )
                if kind == "immature_data_current":
                    immature_current += 1
                elif kind == "series_dead":
                    dead += 1
                else:
                    immature_stalled += 1
            if not mature_missing:
                continue
            mature_pairs += len(mature_missing)
            anchor_row = close_row_at_or_before(conn, row["instrument_id"], anchor_date)
            if anchor_row is None:
                missing_close += len(mature_missing)
                continue
            anchor_close = anchor_row[1]
            if anchor_close <= 0:
                nonpositive += len(mature_missing)
                continue
            for horizon in mature_missing:
                realized = close_row_at_or_before(conn, row["instrument_id"], anchor_date + timedelta(days=horizon))
                if realized is None:
                    missing_close += 1
                    continue
                realized_date, realized_close = realized
                if realized_close <= 0:
                    nonpositive += 1
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        _INSERT_SQL,
                        {
                            "thesis_id": row["thesis_id"],
                            "horizon_days": horizon,
                            "anchor_date": anchor_date,
                            "anchor_close": anchor_close,
                            "realized_date": realized_date,
                            "realized_close": realized_close,
                            "realized_return": (realized_close - anchor_close) / anchor_close,
                            "method_version": METHOD_VERSION,
                        },
                    )
                    inserted += cur.rowcount

    return ThesisOutcomeCaptureReport(
        scanned_theses=scanned,
        anchorless=anchorless,
        inserted=inserted,
        mature_pairs=mature_pairs,
        immature_data_current=immature_current,
        immature_series_stalled=immature_stalled,
        series_dead=dead,
        skipped_missing_close=missing_close,
        skipped_nonpositive_close=nonpositive,
    )


# --- Calibration scoreboard (#2068) — read-side over the ledger ----------
#
# Metric definitions are FIXED in the spec ("Metric definitions" section,
# docs/proposals/thesis/2026-07-16-calibration-ledger-schema.md); this is
# an implementation, not a redesign. Cohort = (model, prompt_version,
# horizon_days) over ALL thesis versions (the ledger scores versions, not
# instruments). Coverage counters are first-class output — honest
# missingness, never imputation.

_DIRECTION_STANCES = frozenset({"buy", "avoid"})

_SCOREBOARD_THESES_SQL = """
SELECT t.thesis_id, t.model, t.prompt_version, t.stance,
       t.base_value, t.confidence_score, i.is_tradable,
       r.context_summary, p.max_price_date
FROM theses t
JOIN instruments i ON i.instrument_id = t.instrument_id
LEFT JOIN LATERAL (
    SELECT context_summary
    FROM thesis_runs r
    WHERE r.thesis_id = t.thesis_id AND r.context_summary IS NOT NULL
    ORDER BY r.run_id DESC
    LIMIT 1
) r ON TRUE
LEFT JOIN LATERAL (
    SELECT max(price_date) AS max_price_date
    FROM price_daily p
    WHERE p.instrument_id = t.instrument_id
) p ON TRUE
ORDER BY t.thesis_id
"""

_SCOREBOARD_OUTCOMES_SQL = """
SELECT thesis_id, horizon_days, realized_close, realized_return
FROM thesis_outcomes
"""


@dataclass(frozen=True)
class CohortScore:
    """One (model, prompt_version, horizon) scoreboard row."""

    model: str | None
    prompt_version: str | None
    horizon_days: int
    # Coverage counters (spec item 4 — always reported).
    total_theses: int
    anchorless: int
    immature_data_current: int
    immature_series_stalled: int
    series_dead: int
    outcome_rows: int
    targets_absent: int
    confidence_absent: int
    direction_claims: int
    # Metrics (None when the contributing set is empty — never imputed).
    target_distance_mape: float | None
    stance_hit_rate: float | None
    conviction_brier: float | None


def aggregate_scoreboard(
    thesis_rows: list[dict[str, Any]],
    outcomes: dict[tuple[int, int], tuple[Decimal, Decimal]],
    today: date,
) -> list[CohortScore]:
    """Pure aggregation over plain values (table-testable, no DB).

    ``thesis_rows``: one dict per thesis version (keys: thesis_id, model,
    prompt_version, stance, base_value, confidence_score, is_tradable,
    context_summary, max_price_date). ``outcomes``: (thesis_id, horizon)
    -> (realized_close, realized_return).

    Per spec: MAPE-form target distance over base_value-bearing outcome
    rows; stance hit-rate over direction claims (buy hit <=> return > 0,
    avoid hit <=> return < 0; watch/hold make no claim); conviction Brier
    over direction claims with non-null confidence_score (diagnostic —
    "does conviction behave like a calibrated probability?"). A mature
    pair the nightly capture has not yet minted counts as immature here
    (compute-on-read never classifies ahead of the ledger).
    """
    acc: dict[tuple[str | None, str | None, int], dict[str, Any]] = {}

    def bucket(model: str | None, pv: str | None, horizon: int) -> dict[str, Any]:
        key = (model, pv, horizon)
        if key not in acc:
            acc[key] = {
                "total_theses": 0,
                "anchorless": 0,
                "immature_data_current": 0,
                "immature_series_stalled": 0,
                "series_dead": 0,
                "outcome_rows": 0,
                "targets_absent": 0,
                "confidence_absent": 0,
                "direction_claims": 0,
                "mape_terms": [],
                "hits": 0,
                "brier_terms": [],
            }
        return acc[key]

    for row in thesis_rows:
        anchor_date = anchor_date_from_summary(row["context_summary"])
        for horizon in HORIZONS:
            b = bucket(row["model"], row["prompt_version"], horizon)
            b["total_theses"] += 1
            if anchor_date is None:
                b["anchorless"] += 1
                continue
            outcome = outcomes.get((int(row["thesis_id"]), horizon))
            if outcome is None:
                kind = classify_immature(
                    is_tradable=bool(row["is_tradable"]),
                    max_price_date=row["max_price_date"],
                    due_date=anchor_date + timedelta(days=horizon),
                    today=today,
                )
                b[kind] += 1
                continue
            realized_close, realized_return = outcome
            b["outcome_rows"] += 1
            base_value = row["base_value"]
            if base_value is None:
                b["targets_absent"] += 1
            else:
                b["mape_terms"].append(abs(Decimal(base_value) - realized_close) / realized_close)
            if row["stance"] in _DIRECTION_STANCES:
                b["direction_claims"] += 1
                hit = realized_return > 0 if row["stance"] == "buy" else realized_return < 0
                if hit:
                    b["hits"] += 1
                confidence = row["confidence_score"]
                if confidence is None:
                    b["confidence_absent"] += 1
                else:
                    b["brier_terms"].append((Decimal(confidence) - (1 if hit else 0)) ** 2)

    def _mean(terms: list[Decimal]) -> float | None:
        return float(sum(terms) / len(terms)) if terms else None

    return [
        CohortScore(
            model=model,
            prompt_version=pv,
            horizon_days=horizon,
            total_theses=b["total_theses"],
            anchorless=b["anchorless"],
            immature_data_current=b["immature_data_current"],
            immature_series_stalled=b["immature_series_stalled"],
            series_dead=b["series_dead"],
            outcome_rows=b["outcome_rows"],
            targets_absent=b["targets_absent"],
            confidence_absent=b["confidence_absent"],
            direction_claims=b["direction_claims"],
            target_distance_mape=_mean(b["mape_terms"]),
            stance_hit_rate=(b["hits"] / b["direction_claims"]) if b["direction_claims"] else None,
            conviction_brier=_mean(b["brier_terms"]),
        )
        for (model, pv, horizon), b in sorted(acc.items(), key=lambda kv: (kv[0][0] or "", kv[0][1] or "", kv[0][2]))
    ]


def compute_calibration_scoreboard(conn: psycopg.Connection[Any]) -> list[CohortScore]:
    """The one DB reader for the scoreboard endpoint (#2068)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_SCOREBOARD_THESES_SQL)
        thesis_rows = cur.fetchall()
    outcomes: dict[tuple[int, int], tuple[Decimal, Decimal]] = {}
    with conn.cursor() as cur:
        cur.execute(_SCOREBOARD_OUTCOMES_SQL)
        for thesis_id, horizon_days, realized_close, realized_return in cur.fetchall():
            outcomes[(int(thesis_id), int(horizon_days))] = (realized_close, realized_return)
    return aggregate_scoreboard(thesis_rows, outcomes, date.today())
