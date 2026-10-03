"""Ranking-pot-v2 declaration freeze (#3592 slice 3c; spec ``2026-10-03-3592-ranking-pot-v2.md`` §8 "Declaration").

v1's freeze (``ranking_pot_freeze``) with v2's document: the #2599 row, the ``ranking_pot_declarations`` document and
the genesis ``<none> → shadow_only`` event in ONE transaction, through v1's own write path (keyed on v2's id). The
document is ``build_declaration``, a pure function of the code, S₀, the measured frozen terms and the provenance.

On top of v1's reads, the freeze measures what §8 freezes and every v2 rebalance reads back
(``ranking_pot_v2_declaration.FrozenTerms``):

- the score strata of S₀ in S₀'s own scores run (``ranking_pot_v2.score_strata``); a stratum below the minimum refuses
  ``stratum_too_small``;
- the DTC baseline: S* and the usable-DTC count there, read at the freeze transaction's own time (FINRA's ingest
  re-stamps ``known_from`` on every refresh, spec §5); no S*, or one older than 31 days, refuses ``dtc_unavailable``,
  and a zero count ``dtc_baseline_empty``;
- the insider ``history_floor``, its threshold and the per-month usable-row counts over every month the floor rules
  read (``ranking_pot_v2.history_floor`` over ``ranking_pot_v2_inputs.usable_history_counts``); a floor at
  the freeze month (a one-month run, whose month is treated as partly ingested) leaves no observable month and refuses
  ``history_floor_unavailable`` as the core's own two cases do;
- §2 α resolution: the freeze refuses ``alpha_resolution`` unless 1/(K + 1) ≤ the per-look α of the family sequence
  it is assigned.

The block stores each term's inputs (S₀'s scores, the count window), and the hashed ``decode_frozen`` re-derives every
term from them, so a defect here cannot reach a rebalance as a plausible number. One REPEATABLE READ snapshot holds
every read; ``transaction_timestamp()`` is its instant and the DTC read's ``as_of``.

**Outside v2's policy hash, deliberately (spec §8 v6).** §8 hashes every ``ranking_pot_v2*`` module and the
``app.services`` modules they import directly. A freeze must import the trial register, the result ledger and the
AI-trial freeze, which change often (``trial_register.py``: 14 commits in the 30 days to 2026-10-03, ``git log
--since=2026-09-03 --oneline -- app/services/trial_register.py``); hashed, each register bump would refuse every later
v2 rebalance as ``ranking_drift``. The freeze runs once, before any decision, and its output is the document, which
the declaration sha fixes; v1's freeze is unhashed for the same reason. ``tests/test_ranking_pot_v2_policy.py`` pins
that no hashed module imports this one.

A dry run executes the same path and rolls back. ``apply=True`` is the supervisor's step from the main checkout after
slice 4 sets ``BUILD_COMPLETE``, never the loop's.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any, Final, cast

import psycopg
from psycopg import errors
from psycopg.pq import TransactionStatus

from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_inputs as rd
from app.services import ranking_pot_v2_policy as policy
from app.services.ai_trial_freeze import Provenance
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot import N
from app.services.ranking_pot_freeze import (
    _ROOT_CONSTRAINTS,
    DECLARATION_KIND,
    S0,
    FreezeReport,
    PotFreezeError,
    _existing_doc_sha256,
    _next_family_seq,
    _write,
    prereg_terms,
    read_s0,
)
from app.services.ranking_pot_sim import K_CONTROLS
from app.services.ranking_pot_v2_declaration import FrozenTerms, count_window, decode_frozen, encode_frozen
from app.services.result_ledger import PreregDeclarationRefused, _lock_trial
from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.services.trial_register import DeclaredTrial, TrialExactness

SPEC_PATH: Final = "docs/proposals/execution/2026-10-03-3592-ranking-pot-v2.md"
BUILDER: Final = "ranking_pot_freeze_v2.build_declaration"
SEED_RULE: Final = (
    "ranking_pot_v2.draw_stratified(strata, base_seed=ranking_pot_sim.control_seed(declaration doc_sha256, "
    "first decided snapshot sha256), k)"
)
DRAW_RULE: Final = "ranking_pot_v2.draw_stratified (ranking_pot_sim.draw_bijection per stratum)"
COMPOSITE_RULE: Final = (
    "(u_score + u_dtc + u_ins) / 3, midrank percentiles within R_t (ranking_pot_v2.composite_scores)"
)
#: Every stored month: the floor's trailing run may reach past any fixed window.
_ALL_HISTORY: Final = date(1, 1, 1)

#: ``trial_register`` holds this entry verbatim; a test pins the two equal.
EXPECTED_REGISTER_ENTRY: Final = DeclaredTrial(
    trial_id=v2.STRATEGY_ID,
    description=(
        "#3592 ranking-pot-v2: v1's 25-name monthly policy with the order by v1.5 score, low FINRA days-to-cover "
        "and CMP opportunistic insider purchases (shadow only), against K = 9,999 stratified (dtc, ins) "
        "attachment controls and a v1-reference book; one declared hypothesis (spec §7)."
    ),
    evidence="docs/proposals/execution/2026-10-03-3592-ranking-pot-v2.md §8 'Declaration'; frozen by "
    "scripts/ranking_pot_freeze_v2.py (#3592)",
    exactness=TrialExactness.EXACT,
    searches=1,
    declared_for=(v2.STRATEGY_ID, policy.STRATEGY_VERSION),
)


def build_declaration(
    *,
    s0: S0,
    family_seq: int,
    module_sha256: dict[str, str],
    constant_repr: dict[str, str],
    policy_hash: str,
    frozen: FrozenTerms,
    provenance: Provenance,
) -> dict[str, Any]:
    """The frozen document (§8). JSON strings, ints, lists and objects only; fractions as ``"p/q"``."""
    return {
        "kind": DECLARATION_KIND,
        "strategy_id": v2.STRATEGY_ID,
        "strategy_version": policy.STRATEGY_VERSION,
        "family": policy.FAMILY,
        "family_seq": family_seq,
        "policy_hash": policy_hash,
        "policy_modules": dict(module_sha256),
        "frozen_constants": dict(constant_repr),
        "scores_model_version": _DEFAULT_MODEL_VERSION,
        "s0": {
            "scored_at": s0.scored_at.isoformat(),
            "count": len(s0.instrument_ids),
            "sha256": s0.sha256,
            "instrument_ids": list(s0.instrument_ids),
        },
        "terms": {
            "n": N,
            "k_controls": K_CONTROLS,
            "book_count": policy.BOOK_COUNT,
            "execution": policy.EXECUTION,
            "look_months": list(policy.LOOK_MONTHS),
            "declaration_alpha": str(policy.declaration_alpha(family_seq)),
            "per_look_alpha": str(policy.per_look_alpha(family_seq)),
            "harm_alpha": str(policy.HARM_ALPHA),
            "insider_lookback_months": v2.INSIDER_LOOKBACK_MONTHS,
            "cmp_history_years": v2.CMP_HISTORY_YEARS,
            "composite": COMPOSITE_RULE,
            "strata_count": v2.STRATA_COUNT,
            "min_stratum_size": v2.MIN_STRATUM_SIZE,
            "dtc_max_age_days": v2.DTC_MAX_AGE_DAYS,
            "dtc_coverage_floor": str(v2.DTC_COVERAGE_FLOOR),
            "floor_reference_months": v2.FLOOR_REFERENCE_MONTHS,
            "floor_guard_share": str(v2.FLOOR_GUARD_SHARE),
            "turnover_bar": str(policy.TURNOVER_BAR),
        },
        "v2": encode_frozen(frozen),
        "seed_rule": SEED_RULE,
        "draw_rule": DRAW_RULE,
        "prereg": prereg_terms(),
        "spec_path": SPEC_PATH,
        "spec_sha256": provenance.spec_sha256,
        "code_git_sha": provenance.code_git_sha,
        "python_version": provenance.python_version,
    }


# ---------------------------------------------------------------------------
# Reads (inside the freeze transaction)
# ---------------------------------------------------------------------------
def read_s0_scores(conn: psycopg.Connection[Any], s0: S0) -> dict[int, Decimal | None]:
    """Every S₀ name's ``total_score`` in S₀'s own run; ``None`` (a NULL score) takes ``UNSCORED_STRATUM``. S₀ is that
    run's ids, so every name has exactly one row (``read_s0`` refuses duplicates)."""
    rows = conn.execute(
        "SELECT instrument_id, total_score FROM scores WHERE model_version = %s AND scored_at = %s",
        (_DEFAULT_MODEL_VERSION, s0.scored_at),
    ).fetchall()
    scores: dict[int, Decimal | None] = {int(r[0]): r[1] for r in rows}
    if sorted(scores) != list(s0.instrument_ids):
        raise PotFreezeError("S₀'s scores run changed inside the freeze transaction")
    return scores


def measure_frozen(conn: psycopg.Connection[Any], s0: S0, *, read_at: datetime) -> FrozenTerms | list[str]:
    """Every §8 measured term, or the refusals that stop the freeze."""
    refusals: list[str] = []
    scores = read_s0_scores(conn, s0)
    strata = v2.score_strata(s0.instrument_ids, scores)
    if (refused := v2.strata_refusal(strata)) is not None:
        refusals.append(refused)

    dtc = v2.dtc_read(
        rd.read_dtc_rows(conn, s0_ids=s0.instrument_ids, as_of=read_at), s0_ids=s0.instrument_ids, as_of=read_at
    )
    baseline = len(dtc.values)
    # No S*, or a stale one, is `dtc_unavailable` (the rebalance gate's own rule); a fresh S* with no usable value
    # leaves no baseline to hold later rebalances to.
    if (gate := v2.dtc_gate(dtc, as_of=read_at, baseline=max(baseline, 1))) == "dtc_unavailable":
        refusals.append(gate)
    elif baseline == 0:
        refusals.append("dtc_baseline_empty")

    freeze_month = read_at.astimezone(UTC).date().replace(day=1)
    counts = rd.usable_history_counts(conn, first_month=_ALL_HISTORY, end_month=freeze_month)
    floor = v2.history_floor(counts, freeze_month=freeze_month)
    if isinstance(floor, str):
        refusals.append(floor)
    elif floor >= freeze_month:
        # A run of one month: that month is treated as partly ingested, so no complete month is observable.
        refusals.append("history_floor_unavailable")
    if refusals or isinstance(floor, str) or dtc.settlement_date is None:
        return refusals
    return FrozenTerms(
        scores=scores,
        strata=strata,
        dtc_settlement_date=dtc.settlement_date,
        dtc_baseline=baseline,
        dtc_read_at=read_at.astimezone(UTC),
        history_floor=floor,
        floor_threshold=v2.floor_threshold(counts, freeze_month=freeze_month),
        month_counts={m: counts.get(m, 0) for m in count_window(freeze_month, floor)},
    )


def alpha_refusal(family_seq: int) -> str | None:
    """§2: the permutation p-value's resolution 1/(K + 1) must not exceed the per-look α."""
    return None if Fraction(1, K_CONTROLS + 1) <= policy.per_look_alpha(family_seq) else "alpha_resolution"


# ---------------------------------------------------------------------------
# The freeze
# ---------------------------------------------------------------------------
class _Rollback(Exception):
    pass


def freeze_pot_v2(
    conn: psycopg.Connection[Any],
    *,
    provenance: Provenance,
    apply: bool,
    declared_by: str = "",
) -> FreezeReport:
    """Run the freeze. ``apply=False`` executes the same path and rolls back. Refusals are returned, not raised; a
    post-write assertion (``PotFreezeError``) or an unexpected database error propagates after rolling back."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise PotFreezeError("the freeze requires an idle connection")
    manifest = policy.policy_manifest_now()
    refusals = list(provenance.refusals)
    if manifest.digest() != policy.RANKING_POT_V2_POLICY_HASH:
        refusals.append("policy_hash_stale")
    if apply and not declared_by.strip():
        refusals.append("declared_by_missing")
    if apply and not policy.BUILD_COMPLETE:
        refusals.append("build_incomplete")
    doc: dict[str, Any] | None = None
    doc_sha256: str | None = None
    existing_sha: str | None = None
    declaration_id: int | None = None
    try:
        with conn.transaction():
            # One snapshot for every measured term; ``transaction_timestamp()`` is its instant (Codex ckpt-1).
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            _lock_trial(cast(psycopg.Connection[tuple], conn), v2.STRATEGY_ID, policy.STRATEGY_VERSION)
            exists, existing_sha = _existing_doc_sha256(conn, v2.STRATEGY_ID)
            if exists:
                refusals.append("already_frozen")
            read_at_row = conn.execute("SELECT transaction_timestamp()").fetchone()
            if read_at_row is None:
                raise PotFreezeError("transaction_timestamp() returned no row")
            s0 = read_s0(conn)
            family_seq = _next_family_seq(conn)
            if (alpha := alpha_refusal(family_seq)) is not None:
                refusals.append(alpha)
            if isinstance(s0, str):
                refusals.append(s0)
            elif not s0.instrument_ids:
                refusals.append("s0_empty")
            else:
                frozen = measure_frozen(conn, s0, read_at=read_at_row[0])
                if isinstance(frozen, list):
                    refusals += frozen
                else:
                    doc = build_declaration(
                        s0=s0,
                        family_seq=family_seq,
                        module_sha256=manifest.module_sha256,
                        constant_repr=manifest.constant_repr,
                        policy_hash=manifest.digest(),
                        frozen=frozen,
                        provenance=provenance,
                    )
                    # The hashed reader must accept exactly what the freeze writes.
                    if decode_frozen(doc["v2"], s0_ids=s0.instrument_ids) != frozen:
                        raise PotFreezeError("the v2 block does not decode to the measured terms")
                    doc_sha256 = canonical_sha256(doc)
            if refusals or doc is None or doc_sha256 is None:
                raise _Rollback
            try:
                declaration_id = _write(
                    conn,
                    doc=doc,
                    doc_sha256=doc_sha256,
                    # The placeholder exists only for a dry run; an apply always carries its declarer.
                    declared_by=declared_by.strip() if apply else "dry-run",
                    strategy_id=v2.STRATEGY_ID,
                    builder=BUILDER,
                )
            except PreregDeclarationRefused as exc:
                refusals += [f"prereg_refused:{code}" for code in exc.refusals]
                raise _Rollback from exc
            if not apply:
                raise _Rollback
    except _Rollback:
        declaration_id = None
    except errors.UniqueViolation as exc:
        if exc.diag.constraint_name not in _ROOT_CONSTRAINTS:
            raise
        # Another family member froze concurrently and took this sequence (its lock is its own): retry, not a refusal
        # of this identity.
        family = exc.diag.constraint_name == "ranking_pot_declarations_family_seq_unique"
        refusals.append("family_seq_race" if family else "already_frozen")
        declaration_id = None
    applied = apply and not refusals and declaration_id is not None
    return FreezeReport(
        applied=applied,
        refusals=tuple(refusals),
        doc=doc,
        doc_sha256=doc_sha256,
        declaration_id=declaration_id if applied else None,
        existing_doc_sha256=existing_sha,
    )


__all__ = [
    "BUILDER",
    "EXPECTED_REGISTER_ENTRY",
    "SPEC_PATH",
    "alpha_refusal",
    "build_declaration",
    "freeze_pot_v2",
    "measure_frozen",
    "read_s0_scores",
]
