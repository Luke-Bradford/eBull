# Gap register: what stands between eBull and justified confidence in making money

Status: revised after Codex framing review (53 findings, classified in §4). Supervisor session, 2026-10-01. Every load-bearing figure states how to reproduce it.

## 0. Objective: what "making money" is measured against

This is fixed by an existing settled decision, not chosen here (`docs/settled-decisions.md`, 2026-08-23):

> *"The benchmark is net buy-and-hold on the same window and universe; a tilt that returns less than the market after costs is a failure."*

Operator north star (2026-09-26): *"trade profitably, hands-off"*.

So the bar is a **net-of-all-costs return above the investable passive alternative, inside the mandate's risk limits**, judged on the engine's own capital. Positive nominal return alone is not the bar. **Fixed by the supervisor 2026-10-01: SPY total return, net of costs, same period.** Every gap below is ranked by how directly it blocks measuring or achieving that.

## 1. Where we stand: facts only

**Return-prediction research.** `TRIAL_REGISTER` r20 has 48 entries, with a declared search count of 479, which is a floor and not 479 independent strategies (`app/services/trial_register.py:50,75`). No concluded return hypothesis has passed. Two are still open or untested: the AI trial (#3471) and fund-v1 (#3515). Concluded results:

- **Price-TA strategies:** net-negative per trade (#2827).
- **Insider purchases, sealed:** +1.192%/mo, CI [−1.452, +4.215]; minus placebo −0.721% (`docs/proposals/ta/2026-08-10-insider-purchase-result.md`).
- **≥12% shock short:** all 8 portfolio sizing arms lost money, max DD 40.68–69.05% (`2026-08-11-extreme-shock-portfolio-result.md`).
- **R6 quality:** `GATE_FAIL` on implementation identity, so this is not a market finding (corr 0.1987 / 0.1959 / 0.1928, all below 0.20; `2026-09-25-2901-quality-result.md:21`).
- **Value and ownership:** closed unrun on low *estimated* power. Power is low, not zero. For value it was estimated at 0.018–0.022 at 1.5%/yr (`2026-09-26-2902-power-first.md`). For ownership, net issuance was estimated and periodic insider selection showed no power (`2026-09-26-2903-power-first.md:63,76`).
- **Hunts 1–3:** no demonstrated edge (#3387, #3448, #3454).

**Passive exposure.** Historical market compounding is context, not a forecast (`strategy-evidence.md:109`). R6's positive result is narrower than "beta works": annual 1/N beat the exclusion overlay, and the rebalance explains the gap to literal buy-and-hold. It still deploys **£0** until reachability, minimum notionals, costs and reconciliation pass (`2026-08-24-r6-sleeve-verdict.md:25,35`).

## 2. Ranked gaps, prerequisites first

### P1. We cannot yet state the engine's net P&L with confidence — #3540

- **Facts.**
  - Exact-owned P&L exists (`strategy_monitoring.load_owned_pnl`) and returns `None` when incomplete.
  - The AI-trial readout uses broker `netProfit`, and the treatment of fees in it is **unresolved**. Distributions (dividends) are omitted (`app/services/ai_trial_readout.py:10-20,265`).
  - The trial spec states that demo-to-live execution transfer is unmeasured (`docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md:37`).
  - There is no NAV bridge (flows, dividends, fees, FX, corporate actions) for the engine book.
- **Why first.** No allocation or evidence decision is meaningful until "did the engine beat its benchmark net of everything" is answerable.
- **Plug.** An engine-book NAV/P&L bridge: opening value, flows, realised and unrealised P&L, fees, dividends, FX, closing value. It reconciles to broker `netProfit` per closed position, and every unexplained difference gets a named residual.
- **Acceptance.** For the current core sleeve, the bridge closes to the cent against the broker over a full week, or names each residual.

### P2. The account's holdings are not all explained by provenance — #3541

- **Facts.** Broker portfolio read at 2026-10-01 15:39Z, via `EtoroBrokerProvider(env='demo').get_portfolio()`:
  - **SPY.RTH:** 4 positions, $16,180. These are the only exact-owned engine positions.
  - **2025 operator holdings:** VOO $10,000, QQQ $10,000, NXH $7,948, IEP $2,661.
  - **SVXY $1,000 and SVOL $480:** opened by a supervisor provider-direct script, with no order-table rows and no ownership (#2437 c5873325695 item 4).
  - **Two copy mirrors:** thomaspj (initialInvestment $20,000, started 2025-08-13) and triangulacapital ($17,280, started 2025-08-15).
  - **What this does not show.** The account panel's `residual_not_in_local_book` is *not an attribution* by its own contract (`app/services/account_equity_evidence.py:80`). The mirrors are not shown to cause the `refused` reconciliation state, since the comparable balance already excludes them (`:901`).
- **Plug.**
  - A provenance register: each broker position classified from exact order/execution provenance as engine-strategy, core, experiment, operator-manual or copy-mirror. Classification never grants trading authority. Ownership is never assigned from symbol or narrative (`trade-lifecycle.md:85`).
  - Diagnose the actual `refused` reason codes.
- **Operator decision.** Whether operator-manual and mirror holdings stay in the demo account (positions are never closed implicitly).

### P3. Capital actions are not explainable at the point of action — #3542

- **Facts.**
  - The AI trial stores a thesis and plan.
  - The core buy stores a mandate intent, but no statement that it carries no entry signal.
  - Supervisor actions bypassed the order path (P2).
- **Plug.** A mandatory trade ticket on every **entry** order: rule id; evidence id (register row or declaration); rationale class (`signal` / `rebalance` / `experiment`); exit rule, or `not_applicable` with a reason for passive holds; and expected cost.
  - **Exits and protective actions are exempt.** The settled EXIT rule says an exit is never blocked.
  - Provider-direct scripts are barred for engine actions.

### P4. The book is not measured against its mandate after entry — #3543

- **Facts.** Entry-time portfolio limits *are* enforced: capacity, reserve, concurrency, realised daily loss, and account/mandate drawdown (`strategy_paper_executor.py:963,1068,1197`). Health blocks stop entries on drawdown, stale data and reconciliation (`strategy_paper_runtime.py:446`). What is missing:
  - `target_volatility_pct` is shown but read by no executor or monitor (`app/api/strategies.py:673,712,2918`), and its semantics (target, ceiling or band) were never defined;
  - there is no persisted ongoing view of volatility, beta, concentration, stress loss or stale marks;
  - sector checks exist only on the recommendation path (`execution_guard.py:588`).
- **Plug.** A daily persisted risk snapshot for the engine book: realised and forecast vol, beta, concentration, a stress loss (e.g. the 2020 or 2022 drawdown applied to current weights), and stale-mark count, compared with the mandate. It is **measurement and alerting only** until the operator defines the vol semantics. It adds no new whole-account authority over the engine (#2844 boundary).

### P5. The allocation policy — RESOLVED 2026-10-01: unassigned budget funds strategies (#2842), never a SPY.RTH stand-in; core stays 50%

- **Facts.**
  - The core mandate is 50% `core_target_pct` (`strategy_core_mandate_events` event 27, 2026-09-18). The stored reason is operational. No derivation was found, but absence is not proven.
  - The core percentage applies to observed core plus assigned cash available, bounded by broker cash and headroom (`strategy_core_sleeve.py:47`). So "≈$20k idle" is an approximation, not a measurement.
  - Mechanics that constrain any change: the band validator requires the reserve at the upper band (`strategy_core_mandate.py:244`), and a core sell closes the single owned position whole (`strategy_core_executor.py:895,923`).
- **Question.** How much of the engine pot should sit in the passive sleeve while no active strategy qualifies? And how is capital released when one does, without forced whole-position churn?
- **Researched starting point.** A rule that holds unassigned budget in the sleeve up to a cap below `100 − reserve − band`, plus a partial-close capability. Before adoption it needs the P1 net benchmark, and a comparison against the operator's feasible passive alternative after tax and wrapper.

### P6. No admission route for published, replicated premia (DELEGATED 2026-10-01; the ETF basket becomes #2842's benchmark) — #3544

- **Facts.** The strict bar exists for novel signals, and the search count justifies that. #2834 ARM A passed only the cost bar: p75 spreads of 8.1 / 14.3 / 42.0 bps from 41–43 observations over 5 dates. That is not executability or alpha (`2026-09-16-arm-a-tilt-selection.md:299`).
- **What a route would need** before it is proposed:
  - an eligibility rule fixed before looking;
  - a selection-bias audit of ETF track records (live versus backfilled, and dead funds);
  - a bridge from the academic factor to the ETF's long-only net excess return after fees;
  - a comparison with the tax-wrapped passive alternative (no ISA API route, #2915);
  - venue readiness beyond #2312: session class, minimum size, order acceptance;
  - a total-loss limit as well as a tracking-error limit;
  - an error-controlled monitoring rule.
- **Status.** Research question. No recommendation yet.

### P7. Cost model scope and calibration — #3545

- **Facts.**
  - The long, x1, real, USD lane is priced from 1,159 quotes, 1,149 of them at UTC hour 19, over 9 summer dates (`cost_model.py:104-166`).
  - Every other lane is *unpriced, not free* (`:34-43`), including shorts and borrow.
  - Fill-vs-ask is not total cost: an ask fill still pays the spread.
- **Plug.** Recalibrate across session hours from the perishables recorder as a new model version that re-runs its dependent evidence. Total-cost measurement means effective spread versus mid, on both sides.

### P8. Launch and run readiness — #3546

- **Fact.** #3529: the AI trial's first fire raced its upstream data job.
- **Plug.** A readiness contract for any scheduled capital pipeline, beyond a one-day rehearsal:
  - upstream-data preconditions before any irreversible claim;
  - idempotent retries;
  - partial-failure and recovery paths;
  - durable submission identity.

### P9. Data limits, split by type — #3547

- **Defects, to measure:** `period_end` values up to 2034 in `financial_periods`, `fundamentals_snapshot` and `financial_facts_raw` (`select max(period_end) from financial_periods`).
- **Missing history:**
  - news only since 2026-06-21;
  - no filing bodies;
  - insider purchases joinable before 2023 are mostly single digits per year (`strategy-evidence.md` §3.0).
- **Possibly irrelevant:** `price_adjustments` is empty, but corpora carry their own adjustments.
- Each must be tied to the hypothesis it would unblock before it is built.

## 3. Open check, not yet a gap — #3548

- On 2026-09-26, 96 hold-out evaluate rows were written without a declaration id (s4, s8, s11, s12; purpose *"complete declared recent-regime evidence denominator"*, `strategy_holdout_accesses`). This is consistent with the designed recent-evidence refresh of harness controls (request 578). **To verify:** those results stay terminal, harness-only evidence that cannot inform selection. The register counts eyeballing as search (`trial_register.py:50`).

## 4. Codex review disposition (53 findings)

Accepted and applied: 1–30, 31–37, 38–44, 45–50, 52, 53, 51 (moved to §3 as an open check). None rebutted.

The main changes from the draft:

- an explicit objective;
- prerequisites ranked first (P1, P2);
- G1 and G2 downgraded from recommendations to operator questions with stated constraints;
- risk claims corrected (entry-time portfolio limits exist);
- the mirror attribution withdrawn;
- exits exempt from tickets;
- cost and readiness claims scoped.
