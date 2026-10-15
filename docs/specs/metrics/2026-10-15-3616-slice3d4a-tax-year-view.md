# The tax-year view, its read surface and the budget reserve (#3616 slice 3d-4a)

Source: #3616 slice 3d. 3d-1's split table gives 3d-4 "the tax-year view: current gains less current losses,
brought-forward losses down to the AEA, the AEA (36); account-only hypothetical labelling (37); unknown ≠ zero coverage
flags (38), including the excluded non-share products 3c reports as omitted; each figure's class and reasons beside it;
every aggregate's tax character (40); read-surface wording (46); no raw Bank of England rate beyond the operator; no
figure for a `real` ledger scope" (`2026-10-14-3616-slice3d1-consideration-basis.md`, "3d split"). 3d-2 added "dropping
the legacy tables and re-pointing their readers including the budget reserve", "A5 stated beside the figures", coverage
evidence and change reporting between runs (`…slice3d2-statutory-matching.md`, Design 11 and "Not in this slice");
3d-3b added "dividends as `not_computed`, never as zero"; 3d-3c-3 added F1, F3–F9 stated beside the figures, the
allowability of a stored loss, and the publication of `oig_or_capital_unresolved` gains
(`…slice3d3c3-fund-disposals.md`, Designs 3–4). The text of findings 36–38, 40 and 46 is in
`var/research/3616/ckpt1_slice3_r1_final.txt` (loop worktree).

**3d-4 is split.** **3d-4a** (this spec): the tax-year computation, the read API over the effective matching run, the
page rendering it, the budget's tax reserve, and dropping the legacy tables. **3d-4b**: the coverage evidence that can
discharge `history_coverage_unestablished` (a new matching policy), and reporting a changed figure against the previous
run. 3d-4b changes no figure 3d-4a defines; it adds evidence and a comparison.

**What this slice decides.**
- The tax-year computation over one effective run: which rows enter the capital-gains computation, in what order losses,
  the annual exempt amount and brought-forward losses are deducted, how an unresolved character is shown, and what
  every aggregate's tax character is (Designs 2–3).
- What is shown when a figure is unknown: structured coverage flags and explicitly partial subtotals, never a zero
  presented as a year's total (Design 4).
- The labels, classes and assumptions stated beside every figure (Design 5).
- The budget reserve: which years and amounts it reserves, and when it is *unavailable* (Design 7). Unavailable blocks
  BUY/ADD; it never blocks an exit.

**The outcome on dev today** (matching run 8, evidence run 7; facts below): tax year 2026/27 holds two disposals. A
share disposal with computed gain −£0.00753863550700 (a capital loss), and SPY's fund disposal, basic gain
£1.10443720572584, `oig_or_capital_unresolved`. Capital reading: net £1.09689857021884, `taxable` £0. Income reading:
net £0, current losses carried forward £0.00753863550700, `taxable` £0. The reserve is £1.10443720572584 (SPY's gain,
reserved whole, Design 7; 2026/27 is the only year with a disposal) with status `incomplete`. The legacy summary shows
£0 gains and £0 losses for the same year today, because it reads tables with no writer.

## Facts established first

Each query below ran on the dev DB on 2026-10-15 through `PYTHONPATH=. uv run python` with `psycopg` against
`settings.database_url`, read-only; the output contains no account identifier.

1. **Runs.** `SELECT matching_run_id, evidence_run_id, matching_policy_id, character_policy_id, state, counts FROM
   tax_matching_runs ORDER BY 1` → runs 1–8, all `published`; run 8 is `identification_v2` / `tax_character_v2` over
   evidence run 7, and `effective_tax_matching` selects it (#3616, 2026-10-15 09:27Z). No run is `refused`.
2. **Disposals in run 8.**
   `SELECT tax_year, status, count(*), sum(gain_gbp) FROM tax_statutory_disposals WHERE matching_run_id = 8 GROUP BY 1, 2`
   → `2026/27, computed, 1, −0.00753863550700`;
   `SELECT r, count(*) FROM tax_statutory_disposals, unnest(reasons) r WHERE matching_run_id = 8 GROUP BY 1` →
   `disposal_formula_assumed`, `history_coverage_unestablished`, `issuer_binding_assumed`,
   `offshore_fund_exclusion_assumed`, `spread_inclusion_assumed`, `sync_charges_unobserved`, 1 each;
   `SELECT tax_year, status, gain_character, count(*), sum(computed_gain_gbp) FROM tax_fund_disposals WHERE
   matching_run_id = 8 GROUP BY 1, 2, 3` → `2026/27, computed, oig_or_capital_unresolved, 1, 1.10443720572584`.
3. **Securities in run 8.** `SELECT status, holding_basis, count(*) FROM tax_matching_securities WHERE matching_run_id =
   8 GROUP BY 1, 2` → `computed s104_pool 2`, `computed relevant_securities 1`, `withheld (null) 4`;
   `SELECT reason, count(*) FROM tax_matching_dependencies WHERE matching_run_id = 8 GROUP BY 1` →
   `unresolved_subject 7`, `character_unestablished 2`; `SELECT character, detail, fund_status, count(*) FROM
   tax_matching_characters WHERE matching_run_id = 8 GROUP BY 1, 2, 3` → `share 4`, `non_reporting_fund 1`,
   `unestablished exchange_traded_fund not_candidate 2`.
4. **Evidence subjects of run 7.** `SELECT outcome, count(*) FROM tax_evidence_subjects WHERE run_id = 7 GROUP BY 1` →
   `admitted 9`, `excluded_non_share_product 6`, `unresolved 7`. Each subject has `first_event_at` / `last_event_at`
   (sql/488, both nullable).
5. **Legacy tables.** `SELECT (SELECT count(*) FROM tax_lots), (SELECT count(*) FROM disposal_matches), (SELECT count(*)
   FROM s104_pool)` → `0, 0, 0`. sql/489 refuses to apply unless the last two are empty; nothing writes them since
   3d-2 (`tax_ledger.py` module docstring).
6. **Their readers.** `grep -rln -e disposal_matches -e s104_pool -e tax_lots app frontend/src scripts` (non-test):
   `app/api/tax.py` via `tax_ledger.py` (`tax_year_summary`, `disposal_audit_trail`, `available_tax_years`,
   `disposals_for_tax_year`, `s104_pool_rows`), `app/services/budget.py::_load_tax_estimates`, comments in
   `execution_guard.py` and `tax_fx.py`, `frontend/src/pages/TaxPage.tsx` through `frontend/src/api/tax.ts`, and two
   measurement scripts (`measure_3616_slice3c_order_lookup.py`, `measure_3616_slice3d2_population.py`).
7. **The reserve today.** `compute_budget_state` reserves `_load_tax_estimates(conn, current year)`: the year's net of
   `disposal_matches` less £3,000 at 18% or 24% by `budget_config.cgt_scenario`, converted to USD by `live_fx_rates`;
   a missing rate with a positive estimate raises `FxRateUnavailable`, which the guard turns into a BUY/ADD block and
   the two `reporting.py` snapshots turn into null fields. It has reserved £0 since 3c retired the fills route (3d-2
   Design 11). The guard computes the budget only for BUY/ADD (`evaluate_recommendation`, step 2), and its
   `budget_available` rule passes when `available_for_deployment > 0`; it does not compare an order's size with it.
8. **The read helpers commit.** `effective_tax_matching` wraps its reads in `snapshot_read`, which calls
   `conn.commit()` before opening a REPEATABLE READ transaction (`app/db/snapshot.py`). It is safe in a read handler and
   not on a caller's connection inside its own transaction.
9. **Environment.** `SELECT environment FROM trade_ledger_scope` → `demo`; `settings.etoro_env` is `demo`.

## Source rules

- **The computation order.** HMRC CG21500: from the year's chargeable gains deduct "allowable losses accruing in that
  year of assessment", "even if the net chargeable gains thus fall below the annual exempt amount"; then "unused
  allowable losses brought forward from earlier years", "but not so as to take the net chargeable gains below the
  annual exempt amount"; losses not used are carried forward. TCGA 1992 s1K(4): the annual exempt amount is deducted
  after the year's allowable losses and before losses of previous years. The two give the same taxable figure.
- **The annual exempt amount.** TCGA s1K(1): deducted from an individual's gains, but not beyond what eliminates them;
  s1K(2): £3,000 (Finance Act 2023, from 2024/25); s1K(6): none for a year in which the individual makes a foreign gain
  claim, a foreign income claim or a foreign employment election.
- **Allocation.** TCGA s1F(1)–(3) and s1K(5): allowable losses and the annual exempt amount may be deducted "in the way
  most beneficial", regardless of the rate that would otherwise apply, and no further than eliminates the gains.
- **Rates.** TCGA s1H(3): 18% or 24%; s1H(4) and s1I decide which applies by the unused basic-rate band, which needs the
  person's income. 2024/25 has two periods (10%/20% to 29 October 2024, then 18%/24%); `tax_ledger._CGT_RATE_PERIODS`
  already holds them and stays the single source.
- **Allowable losses.** CG21500 cites TCGA s16(2A): a loss is allowable only if notified within the time limit of TMA
  1970 s43(1). Allowability of a fund's capital loss is left here by 3d-3c-3 Design 3.
- **Offshore income gains** are income, outside the capital-gains computation (Offshore Funds (Tax) Regulations 2009
  regs 17–18, as 3d-3c-3 cites). No income-tax figure is computed in this slice (Design 3).
- **No source rule** fixes how an account-only figure is labelled, what an unknown is shown as, how a reserve treats a
  gain whose income tax is not computed, when a reserve is released, or when a reserve is unavailable; Designs 4–7 fix
  these by construction.

## Design

### 1. Resolving the run

`resolve_tax_run(conn)` returns the effective run id and the ledger scope's environment, or an unavailable reason. It
never commits, never changes the connection's isolation level and opens no transaction of its own (fact 8). It
**requires** the caller to hold an active REPEATABLE READ or SERIALIZABLE transaction, checked on the server
(`current_setting('transaction_isolation')` with an active transaction, not the connection's next-transaction setting),
and raises a programming error otherwise, so the selection reads one snapshot: every input it reads, and every row read afterwards, is as of one
instant. `effective_tax_matching` becomes a wrapper that opens `snapshot_read` and calls it, unchanged for its callers.

The callers establish the snapshot:
- the tax read handler and `/budget`: `snapshot_read`, as read handlers do today;
- the guard: its step-2 loads for BUY/ADD (fact 7) run inside one `snapshot_read`. The guard's only caller
  (`execute_approved_orders`, `app/workers/scheduler.py`) opens a fresh connection per evaluation and holds no
  transaction around it, so the helper's commit there ends only step 1's read; step 4's audit write keeps its own
  transaction. A read-only REPEATABLE READ transaction cannot fail on serialization;
- the report jobs: `generate_weekly_report` and `generate_monthly_report` document that the caller owns the
  transaction and never commit; the scheduler opens their transaction REPEATABLE READ. The build confirms that the
  job's only write after generation is the snapshot insert.

A sealed run's rows cannot change (the sql/489 and sql/490 seal triggers refuse updates and deletes), so the rows read
by run id agree with the selection.

**Unavailable reasons**, in this precedence (the first that applies is reported):
1. `environment_mismatch`: `settings.etoro_env` differs from `trade_ledger_scope.environment` (budget reserve only).
2. `ledger_scope_not_demo`: the ledger scope's environment is not `demo`. Every policy so far was established on demo
   evidence (3c Limits; 3d-1 fact 1); a real scope needs its own policy.
3. `no_effective_run`: no generation, an unmatched or a refused run. There is no fallback to an older run (3d-2
   Design 9).
4. `year_unsupported` (reserve only): a disposal of the run falls in a year outside `_CGT_RATE_PERIODS`; the reason
   names the year(s). A requested year outside the table is a 422 at the API, as today, with body
   `{"detail": "invalid tax_year '<label>'; expected UK tax-year label like '2026/27'"}`; the selector never offers one
   (Design 6).

### 2. The capital-gains computation (findings 36, 40)

**Members.** Per computed disposal of the year (its stored `tax_year`; each disposal keeps its own year):
- a share statutory disposal: `gain_gbp`, positive a gain, negative a loss, zero neither;
- a fund disposal by `gain_character`: `capital_loss` → its `chargeable_gain_gbp` as a loss; `nil` → nothing;
  `offshore_income_gain` → not a member (income, Design 3); `oig_or_capital_unresolved` → a member only in the
  *capital reading* (below).

An `amount_missing` disposal is not a member and raises a flag (Design 4). The statutory disposal is the unit: one row
per security per day for shares (s105(1)(a), as 3d-2 stores it), one per disposal for funds (reg 42, as 3d-3c-3 stores
it). Identification rows never enter an aggregate.

**Arithmetic.** In a `decimal` context with precision 80 and the `Inexact` and `Rounded` signals trapped, so any
rounding raises instead of happening: stored amounts have at most 30 integer and 14 fractional digits (3d-2 Design 6),
and sums and products by a two-place rate stay well inside 80 digits. Rounding happens only at two named boundaries:
display (the page, to pence, `ROUND_HALF_UP`, formatted from the decimal string without a binary float) and the
reserve's USD conversion (Design 7).

**Computation, per reading, with input `brought_forward_opening` (≥ 0):**
1. `gains` = Σ positive members; `losses` = Σ |negative members|.
2. `net_current` = max(`gains` − `losses`, 0); `current_losses_carried_forward` = max(`losses` − `gains`, 0). Current
   losses are deducted in full even below the AEA (CG21500).
3. `aea` = £3,000; `aea_used` = min(`aea`, `net_current`); `aea_remaining` = `aea` − `aea_used`.
4. `brought_forward_used` = min(`brought_forward_opening`, `net_current` − `aea_used`);
   `brought_forward_remaining` = `brought_forward_opening` − `brought_forward_used`;
   `losses_carried_forward` = `current_losses_carried_forward` + `brought_forward_remaining`.
5. `taxable` = `net_current` − `aea_used` − `brought_forward_used`.
6. **Allocation (s1F, s1K(5)).** Current losses, the AEA and brought-forward losses are deducted from the gains bearing
   the highest rate first, within the scenario. Gains are grouped by the rate period of their disposal date; a year
   with one rate period (every year from 2025/26) is unaffected. This is the most beneficial allocation under
   assumption L6 (no restricted losses, no foreign tax credit); it is not claimed for any other case.
7. `tax_basic` and `tax_higher`: Σ over rate periods of the period's remaining taxable gain × that period's basic or
   higher rate. *Basic* assumes every taxable gain falls inside the unused basic-rate band; *higher* assumes none does
   (s1I). For a given taxable gain the gross CGT lies between them (L4). Both are gross, before any relief.

The production input is `brought_forward_opening` = 0 (assumption L1); the pure function takes it as a parameter so a
later evidence source changes one input, and the tests exercise non-zero values.

**Two readings when an unresolved character is present.** *Capital reading*: every `oig_or_capital_unresolved` basic
gain of the year is a member. *Income reading*: none is. For fixed losses, AEA and brought-forward input, adding
positive gains raises `taxable` by between zero and the amount added, so each scenario's CGT rises by between zero and
24% of `unresolved_gains`: the two readings bound the capital-gains figure whatever each fund's transparency turns out
to be. They do not bound the person's total tax (Design 3, Design 4). A year with no unresolved disposal has one
reading; otherwise both are returned and shown in full.

**Every aggregate and its character (finding 40).**

| aggregate | character |
|---|---|
| `gains`, `losses`, `net_current`, `current_losses_carried_forward` | chargeable gains / allowable losses (TCGA) of this account's computed disposals |
| `aea`, `aea_used`, `aea_remaining`, `brought_forward_*`, `losses_carried_forward`, `taxable` | the CGT computation's deductions and result |
| `tax_basic`, `tax_higher` | gross CGT scenarios, not a liability |
| `offshore_income_gains` | income (reg 17), in no CGT figure; tax not computed |
| `unresolved_gains` | chargeable gains in the capital reading, or offshore income gains in the income reading; income tax not computed |
| `dividends`, fund income as it arises | `not_computed` (3d-3b; Design 4), never zero |

No legacy field survives under its old name (`total_gains_gbp`, `net_gain_gbp`, `dividend_total_gbp`,
`exempt_remaining_gbp`); the API fields are the names above.

### 3. Income lines

`offshore_income_gains` = Σ `offshore_income_gain_gbp` of the year's disposals with `gain_character` =
`offshore_income_gain`; `unresolved_gains` = Σ `basic_gain_gbp` of the year's `oig_or_capital_unresolved` disposals, shown with their two characters as conditional
scenarios (3d-3c-3 Design 3), never split. No income-tax figure is computed: an income-tax figure needs the person's
other income and bands. Each line says "income tax not computed".

### 4. Coverage flags and partial subtotals (findings 37, 38)

Three kinds of amount: **subtotals of the computed disposals** (`gains`, `losses`, `offshore_income_gains`,
`unresolved_gains`), **assumed or input deductions** (`aea`, `brought_forward_opening`), and **derived results** (the
rest of Design 2). The view carries `complete`, true only when the year has no omission flag (below); while it is
false, the page labels the computation "from computed disposals only" and presents no subtotal or result as the year's
total. A year whose disposals are all missing or withheld shows subtotals of zero members with `complete` false, and
the page says the year's figures are unknown.

A flag is structured: a code, a count, and the rows it names.

| code | omission? | raised when | names |
|---|---|---|---|
| `securities_withheld` | yes | the run withholds any security; it has no disposal rows, so its effect cannot be dated and the flag is raised on every year | each security's symbols and dependency reasons |
| `amounts_missing` | yes | a disposal of the year is `amount_missing` | the disposal and its missing lot keys |
| `subjects_unresolved` | yes | an `unresolved` evidence subject's span meets the year, or it has no span | subject, product, reasons |
| `products_excluded` | yes | an `excluded_non_share_product` subject (3c: CFDs and other non-share products) has a span meeting the year, or no span | subject, product |
| `dividends_not_computed` | yes | always (3d-3b) | — |
| `fund_income_not_computed` | yes | the run has any `non_reporting_fund` security: a transparent fund's holders are taxed on income as it arises, which is not computed (3d-3c-3 Design 3) | the securities |
| `income_tax_not_computed` | yes | the year has an established or unresolved OIG | the disposals |
| `account_only` | no (a label) | always: the figures are this account's, not the person's (finding 37; F9, L5) | — |

**A subject's year scope**, by UK date: both endpoints → the years from `first_event_at`'s to `last_event_at`'s; first
only → its year and every later supported year; last only → every supported year up to its year; neither → every
supported year. Each flag returns the scope of each row it names (`years` or `all`). Under the current policies
`complete` is never true (`dividends_not_computed` is always raised); the field exists so that it becomes true only by
evidence.

### 5. Labels, classes and assumptions (findings 37, 46)

- **Labels on every figure:** `demo` (the ledger scope) and `account_only_hypothetical`. The page heading says, in
  words: "Hypothetical, this demo account only: not your tax return".
- **Class.** Class and completeness are separate. A figure is `final` only if every member and every row behind it has
  no reasons *and* it depends on no open assumption. Every Design 2 figure depends on L1–L7, so every figure in this
  slice, including one with no members, is `provisional`. Its reasons are the union of its members' reasons; its
  assumptions are listed beside it. A holding is classed the same way: `provisional`, with its stored reasons, under
  the same assumption list (identification rests on A5, F4–F9 and L5).
- **Assumptions stated beside the figures**, each with its id and the defining sentence copied from its spec, held in
  one constant module: 3d-1 A1 and A4; 3d-2 A5; 3d-3c-3 F1, F3–F9; 3d-3b A8 where a member carries
  `distribution_character_assumed`; and this slice's:
  - **L1**: no allowable losses are brought forward from earlier years (none are evidenced);
  - **L2**: each loss in the computation is allowable: notified within the TMA s43(1) limit (TCGA s16(2A)), and not
    restricted by s16A;
  - **L3**: for the year the individual makes no foreign gain claim, foreign income claim or foreign employment
    election, so the AEA applies (s1K(6)); with F1, no remittance-basis year applies;
  - **L4**: the person's unused basic-rate band is unknown, so tax is shown as the two scenarios;
  - **L5**: the person has no other gains, losses or AEA use in the year (finding 37; with F9, the same holds for
    identification);
  - **L6**: no loss is restricted to particular gains (for example a connected-person loss), and no foreign tax credit
    relief applies, so highest-rate-first allocation is the most beneficial and the scenarios are gross;
  - **L7**: the individual is UK resident for each disposal's tax year, not in a split year, not temporarily
    non-resident and not Treaty non-resident. A5 and F5 cover residence at acquisitions; L7 covers the disposal year.
- **Wording (finding 46).** The API docstring and the page say: the scheduled jobs (`tax_broker_evidence_refresh`,
  `daily_tax_reconciliation`) publish the evidence and the matching run; these endpoints only read, and derive the
  annual view on each request without storing it.
- **Exchange rates.** The tax endpoints return GBP amounts only and no exchange rate; acceptance outputs that print a
  Bank of England rate are not committed (the repository is public). The budget's operational USD conversion by
  `live_fx_rates` (Design 7) is unchanged and is not a Bank of England observation.

### 6. Read surface

`app/api/tax.py`, operator-auth as today, read-only. Amounts are decimal strings, never floats.

**`GET /tax/view?tax_year=`**, one response read in one `snapshot_read` from one resolved run:
- `status` (`available` or `unavailable` with its Design 1 reason); `matching_run_id`, `evidence_run_id`, the policy ids;
  labels; `current_year`; `years`: every supported year, from the first year of `_CGT_RATE_PERIODS` to the current
  year, newest first, each marked with whether the run holds a disposal in it. Listing every supported year keeps a
  year affected only by an omission (Design 4 scopes, including `all`) selectable;
- `year`: the readings (Design 2), income lines (Design 3), `complete` and flags (Design 4), class, reasons and
  assumptions (Design 5);
- `disposals`: per disposal, its stable key (shares: `tax_security_id`, `uk_date`;
  funds: `tax_security_id`, `seq`, with `lot_key`), `uk_date` for both kinds (and `executed_at` for funds), symbols,
  kind
  (`share` or `fund`), units, `proceeds_gbp`, `cost_gbp`, status, missing lot keys, reasons, and by kind: share
  `gain_gbp`; fund `computed_gain_gbp`, `basic_gain_gbp`, `gain_character`, `offshore_income_gain_gbp` and
  `chargeable_gain_gbp`, each NULL exactly where 3d-3c-3's table stores NULL (an unresolved split stays NULL), and the
  reading(s) the disposal is a member of;
- `identifications` under each disposal: rule, acquisition date (NULL for the pool), acquisition lot keys, units, cost
  or missing lot keys, reasons. This replaces the removed `disposal_audit_trail` with the run's own identification;
- `holdings`: per computed security, `tax_security_id`, symbols, holding basis (`s104_pool` or
  `relevant_securities`), units, cost or missing lot keys, class, reasons, `window_open_since`, and fund acquisition
  balances; and **separately** every withheld security with `tax_security_id`, symbols and its dependency reasons, so
  an empty computed list never hides a withheld holding. Holdings are run-level, the same whatever year is selected.

An unavailable response carries `status`, the reason, `current_year` and `years` with the disposal marker null (unknown,
rendered as such), and no figure. A 422 (Design 1)
renders the page's existing error state.

**Removed:** `/tax/summary`, `/tax/disposals`, `/tax/pools`, `/tax/tax-years`, and in `tax_ledger.py`
`tax_year_summary`, `disposal_audit_trail`, `available_tax_years`, `disposals_for_tax_year`, `s104_pool_rows` and their
dataclasses. `current_tax_year`, `valid_tax_year`, `_compute_tax_year` and `_CGT_RATE_PERIODS` stay.

**The page** (`TaxPage.tsx`, `frontend/src/api/tax.ts`): the heading label; the year selector from `years`; the
readings (both, in full, when two) with `taxable` and the two scenarios; the income lines with "income tax not
computed"; `complete` and the flags, each with what it names; the class, reasons and assumptions; the disposal table (dated by `uk_date`)
with each disposal's identifications; the holdings table and the withheld list. An unavailable view renders its reason
and no figure. `frontend/design-system.md` applies; no new route.

### 7. The budget reserve

`tax_reserve(conn, scenario)` in the tax module replaces `_load_tax_estimates`; it resolves the run once (Design 1, on
the caller's connection) and computes every reserved year from that run.
- **Years reserved:** every tax year with a disposal in the run. A payment deadline is not evidence that tax was paid,
  and no settlement or release evidence exists, so no year is released. A payment made from this account's cash both
  lowers cash and leaves its provision in place; that double reservation is the accepted conservative error until a
  release is recorded (later).
- **Amount per year:** the configured scenario's tax under the *income reading* + `offshore_income_gains` +
  `unresolved_gains`, the last two reserved **whole**. Income tax on an amount cannot exceed it at any rate of 100% or
  less, and every UK (ITA 2007 s6) and Scottish income-tax rate is below that, so the whole amount is at least the
  income tax on it at any band; and the capital
  reading adds at most 24% of `unresolved_gains` (Design 2), which the whole amount also covers. This is a bound on the
  computed figures only.
- **Result:** `TaxReserve(status, reason, reason_years, matching_run_id, scenario, labels, class, reasons,
  assumption_ids, flags, years: [{tax_year, cgt_gbp, income_buffer_gbp, total_gbp, complete}], total_gbp)`.
  `cgt_gbp` is the scenario's CGT under the income reading; `income_buffer_gbp` is the whole-amount provision for
  income lines, not a computed income tax. `flags` is run-level: every Design 4 omission flag of the run with its year
  scope, so an omission outside a reserved year, or in a run with no disposal, is still carried. `class` is
  `provisional` and `reasons` the union over the reserved disposals (Design 5).
  - `unavailable` (reason from Design 1, with `reason_years` for `year_unsupported`): no figure.
  - `incomplete`: `flags` holds any omission flag. The reserve covers computed figures only and is not a claim that it
    suffices. Under the current policies the status is always `incomplete`. A run with no disposal gives `years: []`,
    `total_gbp` 0, `incomplete`, and its flags.
  - `available`: no omission flag.
- **BudgetState** replaces `tax_year` with `tax_reserve: TaxReserve`; `estimated_tax_gbp` is the reserve total and
  `estimated_tax_usd` its USD conversion, rounded up to the cent (`ROUND_CEILING`) so rounding never understates the
  reserve; both are None when the reserve is unavailable, and so is `available_for_deployment`.
- **FX.** An unavailable reserve has no figure to convert and reports its own reason. An available or incomplete reserve
  with a positive total and no rate raises `FxRateUnavailable` as today, which now carries the computed `TaxReserve`
  (GBP amounts and run id intact). The conversion outcome is separate from the reserve's status: the guard still
  blocks on it; the reports catch it and keep the reserve, with USD amounts and `available_for_deployment` null and
  conversion diagnostic `fx_rate_unavailable`.
- **Consumers.**
  - *Guard.* `budget_available` already fails a BUY/ADD whose `available_for_deployment` is None; its detail names the
    cause (`tax reserve unavailable: <reason>`, or the empty cash ledger as today). The pass detail lists the reserve's
    status and flag codes. EXIT never computes the budget (fact 7) and is unchanged.
  - *`app/api/budget.py`* returns the whole reserve object, its nullable amounts as nullable fields (no unconditional
    `float(...)`), and `assumption_definitions`: the defining sentence for each returned assumption id, from the one
    constant module. Every GBP amount inside `tax_reserve` is a decimal string (exact); the existing
    `estimated_tax_gbp` / `estimated_tax_usd` float fields stay as the operational totals the dashboard and budget
    arithmetic already use, nullable. On `FxRateUnavailable` it catches the exception and returns the carried reserve with
    `estimated_tax_usd` and `available_for_deployment` null and `conversion_diagnostic` `fx_rate_unavailable`; *`reporting.py`*'s two snapshots store the whole `TaxReserve` (every field above, flags with their
    year scopes, `reason_years`) plus the conversion diagnostic, with null amounts when unavailable; *frontend*
    `BudgetStateResponse` and the dashboard cards that read `estimated_tax_gbp` show "tax reserve (hypothetical, demo,
    provisional, incomplete)" with the scenario and the income buffer named as a buffer, and a details control
    beside the reserve listing its reasons, flags and the assumption definitions (not bare ids); "unavailable:
    <reason>" (with `reason_years`) when unavailable; and the known GBP reserve with "USD conversion unavailable" when
    the conversion failed.
- **What this does not change.** The guard's `budget_available` rule checks that availability is positive, not that an
  order fits within it (fact 7). This slice changes the reserve amount and adds the unavailable block; order sizing
  against deployable cash is outside it.
- **Demo policy and the live prerequisite.** `incomplete` does not block BUY/ADD, as an explicit demo policy: demo
  money bears no tax, so the reserve is a rehearsal of the mechanism. Under a real environment the reserve is
  unavailable (reasons 1–2) until a real-scope policy exists. That policy must resolve or conservatively cover both
  every omission flag *and* the material uncertainty in computed amounts (their reasons, such as
  `history_coverage_unestablished` and `window_open`, and the open assumptions) before its reserve can be used for
  live deployment. Sufficiency of a reserve is a separate property from coverage completeness; this slice establishes
  neither for real money. Until then BUY/ADD refuses under a real environment. This is the
  fail-closed posture 3d-1 assigned to 3d-4 ("3d-4 owns the live prerequisite"); exits are unaffected.

### 8. Dropping the legacy tables

Migration `sql/495_drop_legacy_tax_tables.sql`: refuse unless `tax_lots`, `disposal_matches` and `s104_pool` are all
empty (fact 5; no data is lost), then drop them, the sql/485 trigger `tax_lots_refuse_insert` and its function. The two
measurement scripts that name them recorded runs already published; each gets a one-line note that it ran against the
pre-sql/495 schema, and no code change.

## Build order

- **Part A (pure):** the computation, flags, classes and the reserve arithmetic (Designs 2–5, 7) over in-memory rows,
  and their tests.
- **Part B:** `resolve_tax_run` and the snapshot callers (Design 1), the reader, the API, the budget, guard and report
  wiring, sql/495, DB tests.
- **Part C:** the page and the dashboard reserve label.

## Tests

Pure, each figure by hand in the test:
Tests 1–4 are in 2026/27 (18% / 24%) unless stated; brought-forward is 0 unless stated.
1. Gains 5,000, losses 1,000 → `net_current` 4,000, `aea_used` 3,000, `taxable` 1,000; tax 180 / 240.
2. Gains 1,000, losses 1,500 → `net_current` 0, `current_losses_carried_forward` 500, `aea_used` 0, `aea_remaining`
   3,000.
3. Gains 3,500, losses 1,000 → `net_current` 2,500, `aea_used` 2,500, `aea_remaining` 500, `taxable` 0.
4. Brought-forward 2,000, gains 4,000, losses 0 → `aea_used` 3,000, `brought_forward_used` 1,000,
   `brought_forward_remaining` 1,000, `losses_carried_forward` 1,000, `taxable` 0. Brought-forward 500, gains 2,500,
   losses 500 → `net_current` 2,000, none used, `brought_forward_remaining` 500.
5. 2024/25 across both periods: gains 4,000 before 30 October 2024 and 4,000 after, no losses → the AEA comes off the
   later gains first; basic = 4,000 × 10% + 1,000 × 18% = 580, higher = 4,000 × 20% + 1,000 × 24% = 1,040. With a
   loss of 2,000 as well: the loss and 2,000 of the AEA come off the later gains, the other 1,000 of the AEA off the
   earlier → basic 3,000 × 10% = 300, higher 3,000 × 20% = 600.
6. Readings: two unresolved gains in different periods; the capital reading's tax ≥ the income reading's and exceeds
   it by at most 24% of `unresolved_gains`; an established OIG is in neither; a fund `capital_loss` is a member; `nil`
   adds nothing.
7. Flags: `amount_missing`; withheld securities (raised on every year); a subject span inside, across and outside a
   year's boundary, first-only, last-only and with neither endpoint; a `non_reporting_fund` security; an all-withheld
   run (`complete` false, zero members); a run with no disposal.
8. Class: a year with no members and a year with reason-free members are both `provisional`.
9. Arithmetic: an aggregation that would round raises.
10. Reserve: one year; two years, both retained; whole reservation of income lines; the USD conversion rounds up;
    each status and unavailable reason with its precedence, including `year_unsupported` with its years; a run with no
    disposal; an unavailable reserve with no FX rate reports unavailable, not `FxRateUnavailable`.

DB:
- the reader over a fixture run with a share disposal, a fund disposal, a withheld security and excluded and unresolved
  subjects, asserting the whole response;
- sql/495 refuses on a non-empty legacy table;
- holdings: several computed securities and a withheld one, with no disposal in the selected year;
- `resolve_tax_run`: raises with no active transaction and inside an active READ COMMITTED one; accepts REPEATABLE
  READ and SERIALIZABLE; a generation published by a second connection after the snapshot began is not seen;
- `tax_reserve` inside a caller-owned REPEATABLE READ transaction after an uncommitted write on the same connection:
  returns the expected reserve, leaves isolation unchanged, and the write rolls back afterwards;
- both report snapshots with an incomplete reserve, a `year_unsupported` reserve, and a positive GBP reserve with no FX
  rate (reserve kept, USD null, `fx_rate_unavailable`);
- the guard: an unavailable reserve fails `budget_available` on BUY and ADD with its reason; incompleteness alone does
  not fail it, while an incomplete reserve with zero, negative or unknown availability still fails; EXIT is evaluated
  without the budget in each case;
- `/tax/view`, `/budget` and both report snapshots in the unavailable state: status and reason, null amounts, no error;
  `/budget` with a positive GBP reserve and no FX rate: the reserve kept, USD null, `fx_rate_unavailable`.
- frontend unit: the dashboard reserve renders incomplete (definitions, not ids), unavailable, and conversion-failed
  states from fixtures.

## Acceptance

- **Deterministic:** the DB fixture above reproduces run 8's two disposals (amounts of fact 2) and asserts the dev
  outcome stated at the top: both readings, `taxable` 0, `tax_reserve.total_gbp` the exact string
  `"1.10443720572584"`, status `incomplete`. A second fixture places fund disposals on both sides of 30 October 2024
  and asserts their `uk_date`s and rate periods.
- **Live, on dev after deploy:** a read-only script takes the `matching_run_id` each response returns and recomputes
  from that run's rows: for `GET /tax/view?tax_year=2026/27`, the year's readings and income lines; for `GET /budget`,
  every reserved year under the budget's configured scenario and the total. Each must equal its response. The two
  responses are not required to name the same run.
- **Page:** a frontend unit test renders the fixture response (two readings, flags, unavailable state). On dev, `/tax`
  screenshots, top and bottom, light and dark, show: the hypothetical/demo heading; "from computed disposals only"; the
  flags with what each names; the readings the returned run holds; "income tax not computed" where it applies; the
  assumptions list; no £0 shown for dividends.
- `SELECT to_regclass('tax_lots'), to_regclass('disposal_matches'), to_regclass('s104_pool')` → three NULLs.

## Not in this slice

- **3d-4b:** coverage evidence for `history_coverage_unestablished`; changed figures against the previous run.
- **Later:** brought-forward losses as evidence (L1); an income-tax computation for OIGs, fund income and dividends;
  recorded reserve releases and payments; a real-scope policy; personal coverage beyond the account (F9, L5).

## Findings ledger

| finding | where |
|---|---|
| 36 AEA and losses ordering | Design 2 steps 2–6; L1, L3; tests 1–5 |
| 37 account-only labelling | Design 4 `account_only`; Design 5 labels, L5, L7 |
| 38 unknown ≠ zero, product exclusions | Design 4 partial subtotals and flags; Design 2 removed fields |
| 40 every aggregate's character | Design 2 table; Design 6 disposal fields |
| 46 read-surface wording | Design 5 |

## Security

No order or authorisation path is added. The reserve feeds the guard's existing `budget_available` rule for BUY/ADD:
an unavailable reserve now blocks entries (fail closed, as `FxRateUnavailable` does), including every entry under a
real environment until a real-scope policy exists. Tax-reserve availability never blocks EXIT; the other existing guard
rules still apply. The reserve reader never commits or changes isolation; the guard's step-2 loads become one
REPEATABLE READ snapshot. The tax read endpoints stay behind `require_session_or_service_token` and return no exchange
rate and no account identifier.
