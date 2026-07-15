"""Standing thesis DQ audit — nightly full-population scan (#2014).

Stored theses do not re-validate themselves; insert-time guards (#2007)
evolve while rows stay frozen. This module scans the LATEST thesis per
instrument and surfaces violations as operator-triage CANDIDATES (the
``scripts/dq_audit.py`` board-feeder posture: findings are candidates,
not asserted bugs). No auto-regen — the operator / #2010 staleness path
decides.

Predicates are pure functions over plain values (table-testable, no DB);
``compute_thesis_dq_report`` is the one DB reader. Availability truth per
run is ``thesis_runs.context_summary.blocks`` (#2017, persisted PRE-LLM):
claim-lint compares the memo's unavailability claims against the run's
OWN summary — never against freshly rebuilt context (drift-vs-now is
#2010's staleness concern, not DQ).

Spec: docs/proposals/thesis/2026-07-15-thesis-dq-audit.md.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import psycopg
import psycopg.rows

# Same coercion chokepoint as the #2007 insert-time guard — NaN/±inf → None
# so a garbage target drops out of comparisons instead of defeating them.
from app.services.thesis import _to_float

# Findings payload cap — bounds the endpoint response on a large population
# (mirrors compute_cik_gap_report's bounded per-row payload).
_MAX_FINDINGS = 200

# base more than ±60% from the close the writer anchored on (issue #2014).
_BASE_CLOSE_TOLERANCE = 0.60

# price anchor older than this vs created_at = wrote on a stale anchor.
_STALE_ANCHOR_DAYS = 7

# Memo keywords per context block (claim-lint). The writer prompt's
# availability rule applies to EVERY block's status fields
# (app/services/thesis.py — "Data-availability language MUST mirror the
# block status fields verbatim"), so all summarized blocks are covered.
_BLOCK_KEYWORDS: dict[str, tuple[str, ...]] = {
    "news": ("news",),
    "filings": ("filings", "filing data"),
    "earnings_history": ("earnings history", "earnings data"),
    "analyst_estimates": ("analyst estimates", "analyst coverage", "analyst data"),
    "fundamentals": ("fundamentals", "fundamental data", "financial statements"),
    "valuation": ("valuation data", "valuation metrics"),
    "fair_value_band": ("fair value band", "fair-value band"),
    "risk_metrics": ("risk metrics", "risk data"),
    "ta_state": ("technical", "ta state"),
    "price_anchor": ("price anchor", "market price", "price data"),
    "analytics_evidence": ("analytics evidence", "analytics data"),
}

# Clause-bounded windows: no crossing of , ; . or newline, and tight spans.
# The 60-char sentence-wide window was FP-dominant on the dev population
# ("valuation multiples unavailable due to no live quote, we rely on
# FUNDAMENTALS" flagged fundamentals; "neutral fundamentals, while the
# ALTMAN Z-SCORE is unavailable" flagged fundamentals). A fabricated claim
# names the block and its unavailability in ONE clause ("no recent news
# coverage", "fundamentals data is unavailable") — cross-clause proximity
# is coincidence, not a claim.
# Negation-first form: word-bounded negation then AT MOST TWO words before
# the block keyword ("no recent news coverage", "missing filings data").
# Bare {0,25}-char windows matched "no" inside "noted"/"not supported by …
# fundamentals" and causal "unavailable due to a stale price anchor".
_NEG_BEFORE = r"(?i)\b(?:no|missing|unavailable|not\s+available|lack\s+of)\s+(?:[\w'-]+\s+){0,2}"
_NEG_AFTER = r"[^.,;\n]{0,30}\b(?:unavailable|not\s+available|missing|non-existent)"


@dataclass(frozen=True)
class ThesisDqFinding:
    instrument_id: int
    symbol: str
    thesis_id: int
    dq_class: str
    severity: str  # violation | flag | candidate
    detail: str


# Info-only classes: counted, never emitted as findings rows.
_INFO_CLASSES: frozenset[str] = frozenset(
    {"target_abstention", "zoneless_buy_no_anchor", "no_run", "no_context_summary"}
)


@dataclass(frozen=True)
class ThesisDqReport:
    scanned: int
    class_counts: dict[str, int]
    findings: list[ThesisDqFinding] = field(default_factory=list)
    truncated: bool = False

    @property
    def total_violations(self) -> int:
        """Violation + flag + candidate counts (info classes excluded)."""
        return sum(v for k, v in self.class_counts.items() if k not in _INFO_CLASSES)


def check_ordering(bear: object, base: object, bull: object) -> str | None:
    """#2007 `_validate_writer_output` semantics: bear<=base<=bull over
    non-null pairs after `_to_float` coercion."""
    b, m, u = _to_float(bear), _to_float(base), _to_float(bull)
    if b is not None and m is not None and b > m:
        return f"bear {b} > base {m}"
    if m is not None and u is not None and m > u:
        return f"base {m} > bull {u}"
    if b is not None and u is not None and b > u:
        return f"bear {b} > bull {u}"
    return None


def check_zone(zone_low: object, zone_high: object) -> str | None:
    lo, hi = _to_float(zone_low), _to_float(zone_high)
    if lo is not None and hi is not None and lo > hi:
        return f"buy_zone_low {lo} > buy_zone_high {hi}"
    return None


def classify_zoneless_buy(
    stance: object, zone_low: object, zone_high: object, anchor_available: bool | None
) -> str | None:
    """Return 'zoneless_buy' (violation) or 'zoneless_buy_no_anchor' (info).

    The writer prompt DOCUMENTS null zones when price_anchor is null
    ("When `price_anchor` is null: leave buy_zone_low/high null regardless
    of stance") — so a zoneless buy is only a violation when the run's
    summary says the anchor WAS available. Unknown anchor state (no usable
    summary) is exempt too — honest absence, not a violation.
    """
    if stance != "buy":
        return None
    if _to_float(zone_low) is not None or _to_float(zone_high) is not None:
        return None
    if anchor_available is True:
        return "zoneless_buy"
    return "zoneless_buy_no_anchor"


def check_base_vs_anchor_close(base: object, anchor_close: object) -> str | None:
    """base >60% away from the close the writer anchored on (write-time
    sanity — latest-close drift belongs to #2010, not DQ)."""
    b, c = _to_float(base), _to_float(anchor_close)
    if b is None or c is None or c <= 0:
        return None
    rel = abs(b / c - 1.0)
    if rel > _BASE_CLOSE_TOLERANCE:
        return f"base {b} is {rel:.0%} from anchor-date close {c}"
    return None


def check_stale_anchor(anchor_as_of: date | None, created_at: datetime) -> str | None:
    if anchor_as_of is None:
        return None
    gap = created_at.date() - anchor_as_of
    if gap > timedelta(days=_STALE_ANCHOR_DAYS):
        return f"price_anchor as_of {anchor_as_of} is {gap.days}d older than thesis created_at"
    return None


def is_target_abstention(bear: object, base: object, bull: object) -> bool:
    return _to_float(bear) is None and _to_float(base) is None and _to_float(bull) is None


def claim_lint(memo: str, blocks: dict[str, Any]) -> list[str]:
    """Blocks the memo claims unavailable while the run's own summary says
    available=True (#2007 Defect 2 fabrication class). Candidate severity —
    the bounded-window regex tolerates false positives by posture."""
    hits: list[str] = []
    for block, keywords in _BLOCK_KEYWORDS.items():
        info = blocks.get(block)
        if not isinstance(info, dict) or info.get("available") is not True:
            continue
        for kw in keywords:
            kw_re = re.escape(kw).replace(r"\ ", r"\s+")
            if re.search(_NEG_BEFORE + kw_re, memo) or re.search(r"(?i)" + kw_re + _NEG_AFTER, memo):
                hits.append(block)
                break
    return hits


def _parse_iso_date(raw: object) -> date | None:
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _anchor_block(summary: object) -> dict[str, Any] | None:
    if not isinstance(summary, dict):
        return None
    blocks = summary.get("blocks")
    if not isinstance(blocks, dict):
        return None
    anchor = blocks.get("price_anchor")
    return anchor if isinstance(anchor, dict) else None


def _close_at_or_before(conn: psycopg.Connection[Any], instrument_id: int, as_of: date) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT close FROM price_daily
            WHERE instrument_id = %(iid)s AND price_date <= %(as_of)s
            ORDER BY price_date DESC LIMIT 1
            """,
            {"iid": instrument_id, "as_of": as_of},
        )
        row = cur.fetchone()
    return _to_float(row[0]) if row else None


_LATEST_THESES_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (t.instrument_id) t.*
    FROM theses t
    ORDER BY t.instrument_id,
             t.created_at DESC, t.thesis_version DESC, t.thesis_id DESC
)
SELECT l.thesis_id, l.instrument_id, i.symbol, l.stance, l.created_at,
       l.bear_value, l.base_value, l.bull_value,
       l.buy_zone_low, l.buy_zone_high, l.memo_markdown,
       r.run_id, r.context_summary
FROM latest l
JOIN instruments i ON i.instrument_id = l.instrument_id
LEFT JOIN LATERAL (
    SELECT run_id, context_summary
    FROM thesis_runs r
    WHERE r.thesis_id = l.thesis_id
    ORDER BY r.run_id DESC
    LIMIT 1
) r ON TRUE
ORDER BY l.instrument_id
"""


def compute_thesis_dq_report(conn: psycopg.Connection[Any]) -> ThesisDqReport:
    """Full-population scan of the latest thesis per instrument.

    Latest-row selection mirrors the API tiebreak (created_at DESC,
    thesis_version DESC, thesis_id DESC). Run linkage is LATERAL-latest by
    run_id: ``thesis_runs.thesis_id`` is a nullable NON-unique FK, so a
    plain join could duplicate findings on bad historical multi-links.
    """
    counts: dict[str, int] = {}
    findings: list[ThesisDqFinding] = []

    def add(row: dict[str, Any], dq_class: str, severity: str, detail: str) -> None:
        counts[dq_class] = counts.get(dq_class, 0) + 1
        if len(findings) < _MAX_FINDINGS:
            findings.append(
                ThesisDqFinding(
                    instrument_id=row["instrument_id"],
                    symbol=row["symbol"],
                    thesis_id=row["thesis_id"],
                    dq_class=dq_class,
                    severity=severity,
                    detail=detail,
                )
            )

    scanned = 0
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_LATEST_THESES_SQL)
        rows = cur.fetchall()

    for row in rows:
        scanned += 1
        summary = row["context_summary"]
        blocks = summary.get("blocks") if isinstance(summary, dict) else None
        anchor = _anchor_block(summary)
        anchor_available = anchor.get("available") is True if anchor is not None else None

        if detail := check_ordering(row["bear_value"], row["base_value"], row["bull_value"]):
            add(row, "ordering", "violation", detail)
        if detail := check_zone(row["buy_zone_low"], row["buy_zone_high"]):
            add(row, "zone_inverted", "violation", detail)

        zoneless = classify_zoneless_buy(row["stance"], row["buy_zone_low"], row["buy_zone_high"], anchor_available)
        if zoneless == "zoneless_buy":
            add(row, "zoneless_buy", "violation", "stance=buy with no buy zone despite available price anchor")
        elif zoneless == "zoneless_buy_no_anchor":
            counts["zoneless_buy_no_anchor"] = counts.get("zoneless_buy_no_anchor", 0) + 1

        if is_target_abstention(row["bear_value"], row["base_value"], row["bull_value"]):
            counts["target_abstention"] = counts.get("target_abstention", 0) + 1

        if row["run_id"] is None:
            counts["no_run"] = counts.get("no_run", 0) + 1
            continue
        if not isinstance(blocks, dict):
            counts["no_context_summary"] = counts.get("no_context_summary", 0) + 1
            continue

        anchor_as_of = _parse_iso_date(anchor.get("as_of")) if anchor else None
        if detail := check_stale_anchor(anchor_as_of, row["created_at"]):
            add(row, "stale_price_anchor", "flag", detail)

        if anchor_as_of is not None:
            anchor_close = _close_at_or_before(conn, row["instrument_id"], anchor_as_of)
            if detail := check_base_vs_anchor_close(row["base_value"], anchor_close):
                add(row, "base_far_from_close", "flag", detail)

        memo = row["memo_markdown"] or ""
        for block in claim_lint(memo, blocks):
            add(row, "claim_lint", "candidate", f"memo claims '{block}' unavailable; run summary says available")

    return ThesisDqReport(
        scanned=scanned,
        class_counts=counts,
        findings=findings,
        truncated=sum(v for k, v in counts.items() if k not in _INFO_CLASSES) > len(findings),
    )
