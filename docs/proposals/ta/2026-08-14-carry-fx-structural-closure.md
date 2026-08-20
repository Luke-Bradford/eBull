# Carry + FX structural closure — the second cost model (#2720)

Parent: `docs/proposals/ta/2026-08-07-bounded-backtester.md` §5.1 (criterion 2's
carry/FX requirement), #2698 root cause 2, #2277 (standing carry re-check),
#2363 (the marker split this closes both halves of). Refs #2437.

Codex ckpt-1 ran 2026-08-14 (50 findings); the design below is the post-ckpt-1
revision. The decisive finding — "structural zero is stamped globally while lane
membership is per-position" — is resolved by making the model **intrinsically
single-lane** with the assumption declared as a limit on every result row, plus
two mechanical gates (per-run USD assertion, freeze-time declaration
validation).

## What this closes, in one sentence

`carry_unmodelled` and `fx_unmodelled` become `false` on rows produced under a
new `COST_MODEL_ID`, because for the one execution lane this system trades —
**long, x1, `real` settlement, USD order, USD demo account, USD-quoted
universe** — eToro's own product rule says no overnight/weekend fee exists and
no currency conversion event occurs. The closure is **structural zero for a
declared lane**, not a measured bps written as `Decimal("0")`.

## Source rule

Per `.claude/skills/data-sources/etoro-api.md` (live-portal verification,
WebFetch, never memory). All legs re-verified 2026-08-14:

1. **eToro fees page (`etoro.com/trading/fees`, fetched 2026-08-14):** Stocks
   and ETFs — *"Overnight fee: Free"* for non-leveraged BUY positions;
   *"Short-selling orders and leveraged positions on stocks are executed as
   CFDs and incur CFD spreads and overnight fees."* Overnight/weekend financing
   is a CFD property; a non-leveraged BUY holds the underlying.
   ⚠ The page's own trap: *"some non-leveraged BUY positions in stocks are also
   executed as CFDs"* — closed for the ORDER PATH by leg 2, and declared as a
   backtest LIMIT below (the backtest cannot observe historical settlement
   resolution).
2. **API portal `trading--demo/create-an-order` (OpenAPI v1.342.0, fetched
   2026-08-14):** `settlementType` *"when supplied it is an assertion, not a
   selector: it must equal the settlement type the platform resolves, and a
   mismatch is rejected during execution."* Our strategy order writer pins
   `"settlementType": "real"` in the payload
   (`app/providers/implementations/etoro_broker.py:356`), so an order in this
   lane either holds the underlying or is REJECTED — it can never silently
   execute as a CFD.
3. **What-if cost endpoint (measured, skill §band-census, n=28 across the
   2026-08-12/13 censuses):** `overnightFee` reads `0.0` on every
   `real`-settlement buy observation. ⚠ CONSISTENT WITH the rule and NOT the
   evidence for it — an all-zero component has an undecodable unit (the skill
   says so in bold). The rule is leg 1 + leg 2; this leg only fails to falsify.
4. **FX — three measured facts, one per denomination site:**
   - account: `account_currency_id = 1` (USD), measured on
     `account_equity_evidence` for the configured DEMO account (#2698). The
     claim is scoped to that account — see limits.
   - universe: uniformly USD-quoted, asserted by
     `scripts/measure_2605_universe_scope.py` (settled decision #2605) — and,
     new in this change, re-asserted PER RUN at the stamping site (below),
     because a frozen model id cannot police a mutable universe table.
   - order: `"orderCurrency": "usd"` pinned in the same payload as leg 2, and
     `strategy_base_currency.DEPLOYMENT_CURRENCY = "USD"` enforced at the DB
     CHECKs (sql/290, sql/338) and the executor eligibility + cost-currency
     checks (`strategy_paper_executor.py:555/:593`).
   USD in, USD held, USD out: no conversion event exists to charge.

## Design

### 1. `cost_model.py` — closure vocabulary replaces the bps scalars

```python
CostComponentClosure = Literal["unmodelled", "structural_zero"]

CARRY_CLOSURE: CostComponentClosure = "structural_zero"
FX_CLOSURE: CostComponentClosure = "structural_zero"

@dataclass(frozen=True)
class CostLane:
    direction: str        # "long"
    leverage: int         # 1
    settlement: str       # "real"
    order_currency: str   # "USD"
    account_currency: str # "USD"

STRUCTURAL_ZERO_LANE = CostLane("long", 1, "real", "USD", "USD")
```

- **The model is intrinsically single-lane.** There is exactly ONE lane object;
  both closures are closures *of this model*, whose id names the lane. There is
  no per-component lane to diverge (ckpt-1 #22), and no lane-conditional marker
  state (#23/#24): a consumer outside the lane needs a different cost model,
  which the identity hash makes a different strategy version.
- `CARRY_BPS` / `FX_BPS` are DELETED, not set to zero. A future measured
  nonzero fee (a CFD lane, a non-USD account) reintroduces a charged amount
  together with the arithmetic that adds it and a new model id — the #2286
  shape stays impossible because there is no dormant scalar to set.
- `unmodelled_markers(carry_closure, fx_closure)` keeps its #2363 contract:
  each marker from its own argument ONLY (`closure == "unmodelled"`), test
  drives all four combinations.
- Import guard `_check_unmodelled_components_are_not_charged` is REPLACED by
  `_check_closures()`: closure values in vocabulary; any `structural_zero`
  requires THE single frozen lane and a non-empty, dated evidence tuple (each
  entry `"<source> — <fact> (verified YYYY-MM-DD)"`). Ticket step 4 (remove the
  clause naming the charged component) is subsumed: the clause's subject no
  longer exists.
- `CALIBRATION_LIMITS` — items 4/5 rewritten and one added, so every result row
  keeps carrying the honest limits:
  4. carry is structurally zero FOR THE DECLARED LANE ONLY (long x1 real USD);
     any other lane — short, leveraged, CFD, non-USD — is UNPRICED, not free;
  5. FX is structurally zero for the same lane; account currency is measured on
     the configured demo account, not proven for any other account;
  6. the backtest ASSUMES real-settlement fills: eToro resolves some
     non-leveraged buys as CFDs, historical resolution is unobservable, and the
     order path closes this only forward (settlementType is an assertion the
     platform rejects on mismatch).
- New id:
  `COST_MODEL_ID = "static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd"`.
  Band table, session rule, calibration figures: UNCHANGED. Charged remains
  trade costs only — dividends and corporate-action cash are outside this model
  as before.

### 2. The charge (ticket step 2) — lane enforcement, not a zero added to a price

For a structural-zero component the charged amount is identically zero for
in-lane positions, so adding `+ 0` to `buy_price` would be theatre. What is
real:

- **Per-run FX gate at the stamping site.** `backtest_run`'s universe/corpus
  load asserts `instruments.currency = 'USD'` for every evaluated instrument
  and REFUSES THE RUN on violation — the instrument's own quote currency, not
  the `exchanges.currency` proxy, checked at run time rather than trusted from
  a frozen id (ckpt-1 #29/#30).
- **Freeze-time declaration validation.** `freeze_preregistration` validates
  `declared_carry_unmodelled == cost_model.CARRY_UNMODELLED` (and FX) before
  any write: a declaration is a prediction of what its run will stamp, and
  freezing one that cannot match burns an immutable trial (ckpt-1
  #40/#42/#45). ⚠ SCOPED TO `STRATEGY_MANIFEST` ids — a bespoke contract trial
  (#2582's 13D catalyst charges its own flat 50 bps, models no carry) OWNS its
  stamps and honestly declares `True`; and NOT in
  `PreregDeclaration.__post_init__`, which is also the read-back of stored
  rows frozen under the earlier model.
- **Lane pinned to the executor by non-tautological test.** Capture the body
  built by `EtoroBrokerProvider.place_demo_strategy_order` via a fake transport
  and assert `settlementType/leverage/transaction/orderCurrency` equal the lane
  fields — each side states its literals independently (the "#2240 phase 5c"
  tautology entry, `docs/review-prevention-log.md`). A future short/leveraged/
  non-USD executor change fails THIS test, naming the cost model as the thing
  that must move with it. Plus lane `account_currency ==
  strategy_base_currency.DEPLOYMENT_CURRENCY` (literal vs literal).
- `position_costing.cost_position` is long-only BY ITS OWN ARITHMETIC (entry
  charged as `buy_price`, exit as `sell_price`) — stated in its docstring as
  the backtest half of the lane. The research ledgers that do price shorts
  (`pead_outcomes`, `insider_purchase_outcomes`) consume ONLY the spread
  functions and stamp no cost-model identity and no carry/fx flags (verified
  2026-08-14, grep); the insider preregistration script hard-codes its
  unmodelled stamps and a test pins that it keeps them (ckpt-1 #12/#50).

### 3. What flows downstream, without edits

- `backtest_run.py:2340` stamps `CARRY_UNMODELLED`/`FX_UNMODELLED` per row —
  both now `False`. No stamping-code change.
- `structural_promotion_refusals` UNCHANGED; `STRUCTURAL_REFUSAL_POLICY_VERSION`
  NOT bumped: the rule ("refuse if the row says unmodelled") did not change;
  the stamps rows will carry did. (Ckpt-1 #38 agrees; #39's counterpoint is
  answered by the id move: audits distinguish the regimes by `cost_model_id`,
  which is on every row and in every strategy version.)
- New `COST_MODEL_ID` → `StrategyIdentity` hash → **every strategy version
  moves** (third move of 2026-08-14). Pending fleet-ledger rewrite covers it.

### 4. Deletion-break inventory (all updated in this PR)

- `scripts/verify_2240_cost_model.py:80` — imports/prints both bps constants.
- `tests/test_cost_model.py:25` — imports them; mutation/import-guard probes
  tied to their source text.
- `scripts/probe_2240_cost_model.py:246` — literal source substitutions against
  their declarations.
- `sql/262_strategy_results.sql`, `sql/335_strategy_result_fx_unmodelled.sql` —
  column COMMENTs define the flags via `CARRY_BPS is None`; corrected via a
  small idempotent `COMMENT ON` migration (never by editing applied files).
- Any doc/docstring describing the standing model through `CARRY_BPS is None`
  (grep at implementation time).

## What this deliberately does NOT do

1. Does NOT price any short, leveraged, CFD or non-USD lane — UNPRICED (a
   refusal), not free. A short is a CFD and accrues financing by construction
   (risk-posture note, `.claude/CLAUDE.md`); its cost model is new work.
2. Does NOT make any stored row promotable: existing rows keep their immutable
   `carry_unmodelled = true` stamps, and `universe_basis = 'survivor_only'`
   still hard-refuses everything until #2721.
3. Does NOT bump `STRUCTURAL_REFUSAL_POLICY_VERSION`, does NOT touch existing
   declarations. Declarations frozen pre-merge under old strategy_versions stay
   consistent with rows produced under old code; post-merge runs need fresh
   declarations under the moved versions, which the new freeze-time validation
   forces to declare both flags `False` (#2599 gate itself unchanged).
   ⚠ Coordination: the sibling session working #2437 item 6 is declaring
   S-5..S-10 trials — a warning comment goes on #2437 BEFORE this merges.
4. Does NOT re-run any backtest and does NOT write any DB row (the migration
   updates COMMENT metadata only).
5. Does NOT claim settlement-resolution proof for historical fills or for the
   full instrument population — declared as limit 6 and closed forward by the
   order path (ckpt-1 #2/#3/#25/#26/#27: accepted as limits, not solved).

## Full-population verification

- FX universe leg: `measure_2605_universe_scope.py` asserts USD-uniformity over
  the ENTIRE validated universe at run time; additionally the new per-run gate
  re-asserts it on every backtest's own evaluated set. Both recorded in the PR
  evidence table.
- Lane pin: the executor payload is one construction site
  (`etoro_broker.py:352-364`), grep-verified; `BrokerStrategyOrder` refuses
  `settlement_type != "real"` at type level (`app/providers/broker.py:152`).

## Ckpt-1 findings NOT actioned, with reasons

- #46 (prove every registered strategy long-only): no direction field exists to
  introspect; long-only is the §3 fill contract (every entry is a buy), and the
  costing arithmetic enforces it at the only place a position is priced.
- #19 (out-of-tree `__all__` consumers): none exist; single-repo codebase.
- #13 (reconcile live what-if cost basis with the backtest model): the executor
  order-time cost validation is a separate, unchanged contract.
- #35 (`orderCurrency` semantics): supplementary to the primary FX facts
  (instrument quotes USD + account USD); not load-bearing.
- #36 (dividends/corporate-action cash): outside the model's charged scope
  before and after this change — stated in §1.
- #47 (per-instrument real-arm eligibility over the whole universe): subsumed
  by limit 6; unobservable historically, enforced forward per order.
