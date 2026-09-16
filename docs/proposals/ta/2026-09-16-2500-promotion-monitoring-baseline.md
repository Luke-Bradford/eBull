# #2500 slice 1 — resolving the monitoring baseline for a deployed strategy version

Status: proposed, 2026-09-16. Refs #2500. Part of #2437 phase 3.

## The finding that re-scoped this slice

The first draft of this spec proposed a new `strategy_monitoring_baselines` table that
would **copy** the approved envelope at promotion time, on the premise that the envelope
is otherwise unreconstructable once forward data arrives. Codex checkpoint 1 falsified
that premise and it is falsified correctly:

- `strategy_promotion_evidence` (#2505, `sql/327`) is already **one immutable aggregate
  record per `result_id`** — `payload_sha256 ~ '^[0-9a-f]{64}$'`, `evidence_payload` JSONB
  capped at 64 KiB, and a `BEFORE UPDATE OR DELETE` trigger raising *"strategy promotion
  evidence is immutable; create a new result identity"*.
- Its payload already carries most of #2500's "promotion baseline" list:
  `expected_shortfall_5_pct`, `max_drawdown_pct`, `worst_gap_pct`, the concentration
  triple, `probability_calibration_passed`, `ev_buckets`, `target_first_count` /
  `stop_first_count` / `timeout_count`, `recent_year_evidence`, `capacity_usd`,
  `max_concurrency`, and the whole cost vector `spread_bps` / `slippage_bps` /
  `financing_bps_per_day` / `fx_bps` with `cost_observed_on` / `cost_valid_through`
  (`app/services/strategy_promotion_evidence_store.py:29-110`).
- `promote_strategy` already pins `result_id`s into `strategy_promotion_results`
  (`app/services/strategy_control_plane.py:758-768`).

So the approved envelope **is** pinned, immutably, and stays reconstructable forever.
Copying it into a second table would duplicate an already-frozen record and add a write
that could abort a promotion — including the risk-reducing `pause` / `retire`
transitions, which go through the same function.

## What is genuinely missing

1. **A baseline SELECTION rule.** `_RESULT_EVIDENCE_STAGES = {historical_validated,
   forward_observation}` (`strategy_control_plane.py:89`), and `enable_paper` pins **no**
   result ids at all — deliberately, with the reason in the code: *"⚠ No result ids:
   `paper_enabled` is not a `_RESULT_EVIDENCE_STAGES` member. Re-pinning the historical
   matrix here would double-count one denominator as two independent pieces of
   evidence."* (`strategy_operator_promotion.py:516-519`). **The promotion that actually
   deploys a strategy therefore carries no envelope**, and nothing in the repo says which
   earlier promotion's evidence is the baseline for the deployed version.
2. **A fail-closed read** of that baseline, so "healthy" is unreportable without one.
3. Envelope components #2505's record genuinely does not carry (see
   `MISSING_ENVELOPE_COMPONENTS` below).
4. The monitor, the health states, and per-strategy suspension. ⚠
   `strategy_execution_blocks` is `source TEXT PRIMARY KEY` (`sql/285:87-98`) — a
   **global** block with no strategy dimension, so "suspend this strategy" is currently
   inexpressible.

**This slice delivers 1, 2 and the named list for 3. It does not touch 4.** Every part of
4 needs either a declared repeated-look method (an alpha-spending or anytime-valid rule —
a source-rule step of its own) or a change to a live gate.

## Source rule

- **Selection.** Settled decision #2612 makes stage arrival single-entry, enforced by
  `_NEXT_STAGE` and `idx_strategy_promotions_one_successor`
  (`strategy_operator_promotion.py:403-410`). So for one `(strategy_id,
  strategy_version)` there is at most **one** `historical_validated` promotion and at
  most **one** `forward_observation` promotion. The rule needs no tie-break.
- **Which of the two.** `forward_observation` is preferred because it is the later
  evidence stage *and* it is pinned under `weakening_refusals(previously_covered=
  load_pinned_identities(..., to_stage="historical_validated"), now_covered=...)`
  (`strategy_operator_promotion.py:523-532`) — the transition refuses if its evidence
  covers **less** than the historical stage did. So the forward pin cannot be a weaker
  statement than the one it supersedes. This is the repo's own rule, not a preference.
- **Immutability of the referenced record.** `sql/327_strategy_promotion_evidence.sql`.
- **No metric is chosen here.** This slice selects and reads an existing record; it picks
  no threshold, window or ratio, so `cost-aware-viability.md`'s decision-metric ban does
  not bind it. Slice 2, which compares, does.

## Full-population verification (dev, 2026-09-16 05:0xZ)

```sql
select count(*) from strategy_promotions;            --     0
select count(*) from strategy_deployments;           --     0
select count(*) from strategy_promotion_evidence;    --     0
select count(*) from strategy_funding_decisions;     --     0
select count(*) from strategy_results_store;         --   580
select count(*) from strategy_results_store
 where purpose = 'harness_validation';               --   580  (ALL of them)
```

⚠ **Nothing in dev is promotable** — `promote_strategy` refuses `harness_validation` for
every `_EXTERNAL_EVIDENCE_STAGES` transition (`strategy_control_plane.py:591`), and all
580 stored results carry that purpose. So **no dev-verify of a real promotion is
possible**, and this spec does not claim one. Acceptance is pure tests plus one DB test
that builds its own rows.

⚠ Scope of that census, stated rather than implied: it is a full census of **this dev
database**, not of promotable candidates that do not yet exist. It establishes that this
change writes nothing and reads nothing today; it does not establish how a future
promotion will look.

## Design

New module `app/services/strategy_monitoring_baseline.py`. **No schema change. No write.
No change to any existing call path.**

```python
BASELINE_RULE_VERSION: Final = "monitoring-baseline-v1"

#: Evidence stages a baseline may be drawn from, most-preferred first.
#: Order is the source rule above, not a preference.
_BASELINE_STAGE_PREFERENCE: Final = ("forward_observation", "historical_validated")

@dataclass(frozen=True)
class MonitoringBaseline:
    strategy_id: str
    strategy_version: str
    baseline_rule_version: str
    baseline_stage: Stage            # which stage supplied it
    promotion_id: int
    promoted_at: datetime
    result_ids: tuple[int, ...]
    evidence_payload_sha256: tuple[str, ...]   # sorted, one per result
    evidence: tuple[PromotionEvidence, ...]    # parsed from the immutable payloads
    absent_components: tuple[str, ...]
```

- `select_baseline_promotion(candidates) -> BaselineChoice` — **pure**. Takes the
  `(to_stage, promotion_id, promoted_at, result_ids)` tuples for one version and returns
  the chosen promotion or the ordered refusal reasons. No DB, no clock.
- `resolve_monitoring_baseline(conn, *, strategy_id, strategy_version)
  -> MonitoringBaseline | None` — reads the candidates, applies the pure rule, loads the
  immutable evidence via the existing `load_promotion_evidences`, returns `None` when any
  clause refuses.
- `baseline_unavailable_reasons(...) -> tuple[str, ...]` — the closed vocabulary:
  `no_promotion_for_version`, `no_evidence_stage_promotion`, `no_pinned_results`,
  `promotion_evidence_missing`.

**Fail-closed, and specifically NOT by swallowing exceptions.** A `None` return means a
refusal clause fired on data that was read successfully. A database error propagates —
catching it would return `absent` while leaving the caller's transaction aborted, and
would collapse "no baseline" and "database down" into one indistinguishable state. The
health contract in slice 2 reads `None` as *cannot report healthy*.

### `MISSING_ENVELOPE_COMPONENTS`

A module-level frozen tuple naming what #2505's record does **not** carry, each with
where it would have to come from. This is the slice-2 build list, written down so the
next session does not rediscover it:

| code | where it would come from |
| --- | --- |
| `firing_interarrival_range` | `strategy_signals` / `strategy_decision_calendar` — forward observation, not backtest |
| `broker_fill_rejection_range` | `strategy_order_reconciliation_state`, `strategy_trade_orders` |
| `resolved_outcome_maturity_range` | `strategy_outcomes` resolution lag; `strategy_results_store.median_hold_days` is the backtest analogue and is **NULL on 324 of 580 rows (55.9%)** |
| `checkpoint_plan_and_error_budget` | not stored anywhere; needs a declared alpha-spending or anytime-valid rule before it can exist |

## Behaviour change today: none, and it is measurable

New module, no caller, no write, no migration. `git diff --stat origin/main...HEAD` should
show one new service module, one new test module and this spec.

## Acceptance

- [ ] Pure tests pin the selection rule: forward preferred over historical; historical
      used when no forward promotion exists; every refusal code, including a promotion
      that exists but pinned zero results.
- [ ] Pure test pins that a `paper_enabled` / `live_enabled` / `paused` / `retired`
      promotion is **never** selected as the baseline even when it is the most recent.
- [ ] One DB test builds a promotion + evidence and proves `resolve_monitoring_baseline`
      returns the evidence, and returns `None` with `promotion_evidence_missing` when the
      evidence row is absent.
- [ ] DB error propagates rather than reading as `absent` — asserted, not assumed.
- [ ] Every test revert-probed.
