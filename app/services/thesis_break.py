"""Machine-checkable thesis break predicates — pure layer (#2012 PR-A).

``break_conditions_json`` is free prose. This module extracts the subset
that maps onto a CLOSED, trust-verified metric vocabulary (whole-string,
precision-gated — recall is explicitly not a goal; anything ambiguous,
composite, duration-qualified or naming a denominator we do not ingest
stays prose, fail-open) and evaluates predicates against supplied
observations with per-input freshness bounds (fail-closed: absent or
stale input can never fire, and can never arm a baseline).

Arm/baseline model (spec Design 5): a predicate's first CONTEMPORANEOUS
evaluation is its baseline. Baseline true → the writer's own premise
(``already_true``) — never fires. Baseline true after any unobserved gap
(the row was ever ``pending``, or the first scan ran > grace after thesis
creation) → ``already_true_after_gap`` — indistinguishable from a break
missed during the gap, so still never fires, but counted separately.
Baseline false → ``armed``; only armed predicates fire, on the first
observed false→true transition. An already-true premise that later
resolves (observed false) RE-ARMS — a subsequent true is then a genuine
transition.

Pure: no DB, no I/O, no import from ``thesis`` (no cycle). The DB-facing
scan lives in ``thesis_break_scan.py``.

Spec: docs/proposals/thesis/2026-07-16-thesis-break-predicates.md.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Literal

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

# Regime metrics compare two live columns; they carry no threshold.
REGIME_METRICS: frozenset[str] = frozenset({"price_vs_sma200", "sma_50_vs_sma_200"})

# Per-input freshness bounds (spec "Closed vocabulary"). A metric evaluates
# only when EVERY listed input is present and within its own bound — a ratio
# is only as fresh as its stalest input.
#   finra_settlement  ≤ 45d  — two missed bimonthly cycles (skill finra.md:11)
#   share_count_filed ≤ 183d — dei shares outstanding is stated on every
#                              10-K/10-Q cover (quarterly cadence; 2 missed)
#   fy_period_end     ≤ 456d — annual cadence + 10-K filing lag (15 months)
#   price_date        ≤ 10d  — calendar days; holidays covered
FRESHNESS_BOUNDS: dict[str, dict[str, timedelta]] = {
    "short_interest_pct_shares_out": {
        "finra_settlement": timedelta(days=45),
        "share_count_filed": timedelta(days=183),
    },
    "short_interest_days_to_cover": {"finra_settlement": timedelta(days=45)},
    "short_interest_change_pct": {"finra_settlement": timedelta(days=45)},
    "altman_z": {"fy_period_end": timedelta(days=456)},
    "rsi_14": {"price_date": timedelta(days=10)},
    "price_vs_sma200": {"price_date": timedelta(days=10)},
    "sma_50_vs_sma_200": {"price_date": timedelta(days=10)},
}

# already_true (premise) requires the baseline to be contemporaneous with
# thesis creation: first evaluation on the scan that created the row AND
# within this grace of created_at (one nightly cadence + slack). Anything
# later — ever-pending or rollout — is already_true_after_gap.
BASELINE_GRACE = timedelta(hours=48)


@dataclass(frozen=True)
class BreakPredicate:
    """One machine-checkable condition. ``threshold`` is None iff regime."""

    metric: str
    op: Literal["<", ">"]
    threshold: float | None
    unit: str
    source_text: str


@dataclass(frozen=True)
class MetricInput:
    """One named input feeding an observation (evidence unit for inputs_json)."""

    value: float
    as_of: date | None
    source: str


@dataclass(frozen=True)
class MetricObservation:
    """A metric's current reading, freshness-classified via ``observe``.

    ``value`` is the scalar the predicate compares (for regime metrics: the
    signed LHS−RHS gap, so op '<' means gap < 0). ``as_of`` is the STALEST
    contributing input's as-of. ``status`` other than 'ok' can never fire
    and can never baseline.
    """

    metric: str
    status: Literal["ok", "no_input", "stale_input"]
    value: float | None = None
    as_of: date | None = None
    inputs: dict[str, MetricInput] = field(default_factory=dict)


def observe(
    metric: str,
    inputs: dict[str, MetricInput | None],
    value: float | None,
    *,
    today: date,
) -> MetricObservation:
    """Freshness-classify raw inputs into a MetricObservation (fail-closed).

    Every input named in FRESHNESS_BOUNDS[metric] must be present (else
    ``no_input``) and within its own bound (else ``stale_input``); each
    input's bound is checked independently — never a collapsed scalar.
    """
    bounds = FRESHNESS_BOUNDS[metric]
    present: dict[str, MetricInput] = {k: v for k, v in inputs.items() if v is not None}
    if value is None or any(name not in present for name in bounds):
        return MetricObservation(metric=metric, status="no_input", inputs=present)
    for name, bound in bounds.items():
        inp = present[name]
        if inp.as_of is None or (today - inp.as_of) > bound:
            return MetricObservation(metric=metric, status="stale_input", inputs=present)
    as_of = min(inp.as_of for name, inp in present.items() if name in bounds and inp.as_of is not None)
    return MetricObservation(metric=metric, status="ok", value=value, as_of=as_of, inputs=present)


EvalResult = Literal["true", "false", "no_input", "stale_input"]


def evaluate_predicate(pred: BreakPredicate, obs: MetricObservation) -> EvalResult:
    """Pure compare. Regime metrics compare the signed gap against zero."""
    if obs.status != "ok":
        return obs.status
    assert obs.value is not None
    threshold = 0.0 if pred.threshold is None else pred.threshold
    hit = obs.value < threshold if pred.op == "<" else obs.value > threshold
    return "true" if hit else "false"


# ---------------------------------------------------------------------------
# Baseline state machine (spec Design 5)
# ---------------------------------------------------------------------------

BaselineState = Literal["pending", "armed", "already_true", "already_true_after_gap"]


def next_baseline_state(
    state: BaselineState,
    result: EvalResult,
    *,
    newly_inserted: bool,
    thesis_created_at: datetime,
    now: datetime,
) -> tuple[BaselineState, bool]:
    """(new_state, fire). Fires ONLY from ``armed`` on an observed true.

    ``newly_inserted`` = the predicate row was created by THIS scan (it was
    never left pending by a prior scan). Non-ok results change nothing —
    a predicate cannot arm, fire, or re-arm on data we do not have.
    """
    if result in ("no_input", "stale_input"):
        return state, False
    if state == "pending":
        if result == "false":
            return "armed", False
        contemporaneous = newly_inserted and (now - thesis_created_at) <= BASELINE_GRACE
        return ("already_true" if contemporaneous else "already_true_after_gap"), False
    if state == "armed":
        return "armed", result == "true"
    # already_true / already_true_after_gap: premise resolved → re-arm.
    if result == "false":
        return "armed", False
    return state, False


# ---------------------------------------------------------------------------
# Extractor — whole-string, precision-gated (spec Designs 3, 4, 7)
# ---------------------------------------------------------------------------

# Fail-open pre-filter. Composites (a substring match would evaluate a WEAKER
# condition than written), illustrative "(e.g., …)" wrappers (the head is the
# real, broader condition), duration/persistence qualifiers (not expressible
# as a single-scan edge trigger), deadline semantics ("within N months",
# "fails to …" = failure-to-achieve), and eToro-context metadata leaks.
_REJECT = re.compile(
    r"\b(?:and|or|with|while|plus|alongside|amid|versus|vs)\b"
    r"|e\.g\."
    r"|\bfor\s+(?:[>≥]?\s*\d|a\b|an\b|two|three|four|several|consecutive|sustained|prolonged|extended)"
    r"|\b(?:sustained|persistent|prolonged|consecutive|consistently)\b"
    r"|\bwithin\s+\d"
    r"|\bfail(?:s|ure|ing)?\s+to\b"
    r"|\{"
)

_NUM = r"(?P<num>-?\d+(?:\.\d+)?)"
# One trailing annotation parenthetical: "(current 1.805)", "(oversold
# territory)", "(death cross confirmation)". Composite/e.g. content inside
# a parenthetical is already killed by _REJECT (it scans the whole string).
_ANNOT = r"(?:\s*\([^()]*\))?"

# Verb families carry direction; the explicit preposition decides, but a
# non-neutral verb that CONTRADICTS the preposition ("improves below -1.5")
# is a writer error → fail open.
_POSITIVE_VERBS = frozenset(
    {
        "improves",
        "improve",
        "improving",
        "improvement",
        "recovers",
        "recovery",
        "rises",
        "rising",
        "rise",
        "climbs",
        "increase",
        "increases",
    }
)
_NEGATIVE_VERBS = frozenset(
    {
        "falls",
        "fall",
        "falling",
        "drops",
        "drop",
        "dropping",
        "declines",
        "decline",
        "declining",
        "deteriorates",
        "deterioration",
        "worsens",
        "worsening",
        "reduction",
        "decrease",
        "decreases",
    }
)

_DIR_UP = frozenset({"above", ">", "exceeds"})
_DIR_DOWN = frozenset({"below", "<"})


def _direction(verb: str | None, prep: str) -> Literal["<", ">"] | None:
    """Resolve op from preposition, vetoed by a contradicting verb family."""
    op: Literal["<", ">"] = ">" if prep in _DIR_UP else "<"
    if verb:
        verb = verb.strip().lower()
        if verb in _POSITIVE_VERBS and op == "<":
            return None
        if verb in _NEGATIVE_VERBS and op == ">":
            return None
    return op


# altman: optional possessive company prefix ("dropbox's altman z-score").
_ALTMAN_HEAD = r"(?:[a-z][\w.&-]*'s\s+)?altman\s+z[\s-]?score"
_QUAL = r"(?:(?:material|significant|major|notable)\s+)?"

# Form A: "<qual> improvement/decline in altman z-score (to) above/below N"
_ALTMAN_A = re.compile(
    rf"^{_QUAL}(?P<verb>improvement|recovery|reduction|decline|drop|deterioration|decrease)\s+in\s+"
    rf"{_ALTMAN_HEAD}\s+(?:to\s+)?(?P<prep>above|below)\s+{_NUM}{_ANNOT}$"
)
# Form B: "altman z-score falls/improves/crosses/moves (to/into) above|below|>|< N"
_ALTMAN_B = re.compile(
    rf"^{_ALTMAN_HEAD}\s+"
    r"(?P<verb>falls|falling|drops|dropping|declines|declining|deteriorates|worsens|worsening|"
    r"crosses|crossing|moves|moving|rises|rising|improves|improving|improvement|recovers)\s+"
    rf"(?:to\s+|into\s+)?(?P<prep>above|below|>|<)\s*{_NUM}{_ANNOT}$"
)
# Form C: "altman z-score crosses into bankruptcy/'non-distress' territory (<N)"
# — threshold lives in the parenthetical.
_ALTMAN_C = re.compile(
    rf"^{_ALTMAN_HEAD}\s+(?:crosses|crossing)\s+into\s+"
    r"(?:'?non-distress'?\s+(?:territory|band|levels?)|bankruptcy\s+territory)\s+"
    rf"\((?P<prep>[<>])\s*{_NUM}\)$"
)

# rsi: "rsi-14 crosses above 70 (overbought territory)"
_RSI = re.compile(
    r"^rsi[\s-]?(?:14\s+)?(?P<verb>crosses|drops|falls|rises|moves)\s+(?P<prep>above|below)\s+"
    rf"{_NUM}{_ANNOT}$"
)

# short interest, EXPLICIT shares-outstanding denominator ONLY (spec Design 4:
# float and bare percentages fail open — shares_out ≥ float, so substituting
# our denominator systematically under-reports the writer's condition).
_SI_SHARES_OUT = re.compile(
    rf"^{_QUAL}short\s+interest\s+"
    r"(?P<verb>rises|increases|climbs|exceeds|falls|drops|declines)\s+"
    rf"(?:above\s+|below\s+)?{_NUM}%\s+of\s+(?:total\s+)?shares\s+outstanding{_ANNOT}\.?$"
)
# Prefix-verb form: "material increase in short interest above 20% of shares
# outstanding" — the event noun leads, the preposition still carries direction.
_SI_SHARES_OUT_PREFIX = re.compile(
    rf"^{_QUAL}(?P<verb>increase|rise|reduction|decline|decrease|drop)\s+in\s+short\s+interest\s+"
    rf"(?:to\s+)?(?P<prep>above|below)\s+{_NUM}%\s+of\s+(?:total\s+)?shares\s+outstanding{_ANNOT}\.?$"
)

# Regime: price below its 200-day SMA. The corpus pins write-time dollar
# levels ("below 116.05 (200-day sma)") — the semantic condition is the
# MOVING sma, so all forms evaluate as the regime close < sma_200; the
# stamped number stays in source_text.
_PRICE_SMA_FORMS = (
    re.compile(rf"^(?:technical\s+)?break(?:down)?\s+below\s+(?:the\s+)?200[\s-]?day\s+sma{_ANNOT}$"),
    re.compile(rf"^technical\s+breakdown\s+below\s+{_NUM}\s+\(200[\s-]?day\s+sma\)$"),
    re.compile(rf"^technical\s+breakdown\s+below\s+sma[\s_]?200\s+\(\$?{_NUM}\)$"),
    re.compile(rf"^price\s+(?:breaks?|drops?|closes?|falls?)\s+below\s+(?:the\s+)?200[\s-]?day\s+sma{_ANNOT}$"),
)

# Regime: sma_50 vs sma_200. "crosses below" extracts as the REGIME (spec
# Design 5: a cross already holding at arm baselines already_true; armed →
# a later regime flip IS the cross event). Bare-parenthetical definition
# forms allowed; "(e.g., …)" illustrative forms are killed by _REJECT.
_SMA_CROSS_FORMS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(rf"^sma[\s_]?50\s+(?:crosses\s+)?(?P<prep>below|above)\s+sma[\s_]?200{_ANNOT}$"), "prep"),
    (
        re.compile(
            rf"^50[\s-]?day\s+sma\s+(?:crosses\s+)?(?P<prep>below|above)\s+(?:the\s+)?200[\s-]?day\s+sma{_ANNOT}$"
        ),
        "prep",
    ),
    (
        re.compile(
            r"^technical\s+indicators\s+(?:confirm|show)\s+(?:a\s+)?bearish\s+(?:regime|crossover|reversal)\s+"
            r"\(sma[\s_]?50\s+(?:crosses\s+below|below|<)\s+sma[\s_]?200\)$"
        ),
        "<",
    ),
)


def _extract_one(condition: str) -> BreakPredicate | None:
    text = " ".join(condition.split()).lower().rstrip()
    if not text or _REJECT.search(text):
        return None

    for pattern in (_ALTMAN_A, _ALTMAN_B, _ALTMAN_C):
        m = pattern.match(text)
        if m:
            op = _direction(m.groupdict().get("verb"), m.group("prep"))
            if op is None:
                return None
            return BreakPredicate("altman_z", op, float(m.group("num")), "zscore", condition)

    m = _RSI.match(text)
    if m:
        op = _direction(m.group("verb"), m.group("prep"))
        if op is None:
            return None
        return BreakPredicate("rsi_14", op, float(m.group("num")), "index", condition)

    m = _SI_SHARES_OUT.match(text)
    if m:
        verb = m.group("verb")
        prep = "above" if ("above" in m.group(0) or verb in ("rises", "increases", "climbs", "exceeds")) else "below"
        op = _direction(verb if verb != "exceeds" else None, prep)
        if op is None:
            return None
        return BreakPredicate("short_interest_pct_shares_out", op, float(m.group("num")), "pct_shares_out", condition)

    m = _SI_SHARES_OUT_PREFIX.match(text)
    if m:
        op = _direction(m.group("verb"), m.group("prep"))
        if op is None:
            return None
        return BreakPredicate("short_interest_pct_shares_out", op, float(m.group("num")), "pct_shares_out", condition)

    for pattern in _PRICE_SMA_FORMS:
        if pattern.match(text):
            return BreakPredicate("price_vs_sma200", "<", None, "regime", condition)

    for pattern, op_spec in _SMA_CROSS_FORMS:
        m = pattern.match(text)
        if m:
            op = (">" if m.group("prep") == "above" else "<") if op_spec == "prep" else op_spec
            return BreakPredicate("sma_50_vs_sma_200", op, None, "regime", condition)  # type: ignore[arg-type]

    return None


def extract_predicates(conditions: Sequence[object]) -> list[BreakPredicate | None]:
    """Index-aligned with break_conditions_json; None = prose (fail-open).

    Accepts the RAW jsonb array — non-string elements map to None in place
    so a malformed element can never shift later predicate indexes."""
    return [_extract_one(c) if isinstance(c, str) else None for c in conditions]
