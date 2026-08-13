# Fundamentals drill page — refinement spec

Status: **APPROVED by the operator 2026-07-31** for autonomous implementation with no further
human review between approval and PR. Supersedes the loose ticket set #2182/#2183/#2184/#2185 by
giving them an order and a shared constraint.

Approval scope and its limits:

- Covers §1 (provable defects), §3 (per-chart decisions, including the §3.1 and §3.6 JUDGEMENT
  items), §4 (reduced chart infrastructure), §5 (toggles), §6 (order).
- Does **not** extend to anything this spec marks REJECTED (§2), to #2155 (type scale / font
  pairing — a new visual identity, operator-gated), or to any live-trade, capital, or LLM-model
  promotion decision.
- Each JUDGEMENT item retains its stated kill condition. If the validation named in §3.1 / §3.6
  fails at review, the item is **deleted**, not iterated on. Autonomy to build is not autonomy to
  keep something that did not work.

Operator challenge that produced this doc (2026-07-31): *"check that something can be genuinely
improved and we're not just trying to make changes in the hope it's better."* Every item below is
therefore labelled **PROVABLE** (a defect with a test) or **JUDGEMENT** (a design bet, with the
validation named).

---

## 0. Coverage — measured at the INSTRUMENT level

### 0.1 A correction, recorded because it nearly set the whole spec's direction

Draft 1 of this doc asserted coverage was the binding constraint, citing "full P&L stack 8%,
FCF 37%". **Both figures were wrong**, in two ways Codex caught at spec review:

1. **Wrong denominator.** They counted *rows* (n=50,730 FY rows) while the spec's own rule was
   phrased per *instrument*. An instrument needs a handful of usable periods to draw a trend, not
   every row populated. Measuring the population the rule talks about is the repo's standing
   "full-population verification" requirement — draft 1 violated it.
2. **Wrong FCF rule.** It required `capex IS NOT NULL`. The settled treatment is
   `app/services/fcf_yield.py:111,132` — `operating_cf - ABS(COALESCE(capex, 0))`: capex NULL→0,
   OCF strict. Draft 1 inferred a rule from first principles where a documented one existed.

### 0.2 Corrected measurement

`financial_periods`, `period_type='FY' AND superseded_at IS NULL`, per instrument, counting
instruments with **≥3 usable FY periods** (the minimum to read a trend). n = 4,756.

| chart input | rule | instruments with ≥3 periods |
|---|---|---|
| Revenue trend | `revenue` | 3,763 (79%) |
| FCF | `operating_cf` (capex COALESCE 0) | **4,261 (89%)** |
| Earnings quality | `net_income` + `operating_cf` | 4,092 (86%) |
| EPS / per-share | `net_income` + `shares_diluted` | 3,871 (81%) |
| Book value/share | `shareholders_equity` + `shares_outstanding` | 3,288 (69%) |
| P&L residual stack | `revenue` + `cost_of_revenue` + `operating_income` | 2,282 (47%) |

`normalization_status` is `normalized` for **all** 50,730 FY rows, so no additional filter is
required — verified, not assumed.

**Revised conclusion:** coverage is *not* the binding constraint for most charts. 69–89% of
instruments can feed them. The genuine constraints are narrower:

- The **P&L composition stack at 47%** — the only chart materially gated.
- The **row-level interior gaps** in #2182 (46% of instruments, 6,203 missing instrument-years),
  which affect *continuity within* a series rather than whether it can be drawn at all.

**Standing rule retained** (it did its job even with bad inputs):

> No chart is specified before its inputs' computability is stated, measured per instrument at
> ≥3 usable periods, using the repo's documented treatment for any derived value.

---

## 1. PROVABLE defects — fix regardless of any design decision

Each has a test that fails today.

### 1.1 Numeric-id URLs 404 the page — #2184
`/instrument/1001/fundamentals` renders "Failed to load"; all 4 calls return
`{"detail":"Instrument 1001 not found"}`. `app/api/instruments.py:850` resolves
`WHERE UPPER(symbol) = %(s)s`. 20+ sibling endpoints share the pattern.
**Test:** request each drill endpoint by id and by symbol; both must 200.
**Also:** a 404 currently renders as a red "check the browser console" banner. Uncovered
instruments must render an empty state, not an error.

### 1.2 Interpolation draws unreported values — #2185
13+ `type="monotone"` with `dot={false}`. Quarterly financials are discrete; cubic interpolation
invents intermediate values and can overshoot. Hiding dots removes the only data/drawing cue.
**Fix:** `type="linear"`, dots visible when the series has < ~40 points.
**Test:** lint asserting no `type="monotone"` in `components/fundamentals/`.

### 1.3 Light-theme colours hardcoded in a theme-aware component — #2185
`fundamentalsCharts.tsx` binds `useChartTheme()` to `theme`, then uses `lightTheme.accent[...]`
**23 times** vs 14 uses of `theme.`. Violates `.claude/skills/frontend/design-system.md`: *"a new
chart imports the theme; it does not re-pick colors."* Invisible to `dark:check`, which inspects
only bg/border/hover class pairs (prevention log: "a lint gate's file-glob is part of its contract").
**Test:** extend the dark gate to flag `lightTheme.` / `darkTheme.` referenced outside `chartTheme.ts`.

### 1.4 Money axes carry no currency — #2185
`formatBigNumber` emits `380.00B`. `financial_periods.reported_currency` exists and non-USD
reporters are mixed in. Same class as #2129 (a per-item currency field is a contract on every money
field). Codex: *"genuine defect."*

### 1.5 Duplicated non-zero revenue — 66 rows / 48 instruments
EBAY FY2017 == FY2021 == $2.613B; TEVA FY2009 == FY2010 == $16.121B. Investigate before fixing
broadly — the count is small and the cause is probably comparative-column mis-assignment shared
with #2182.

### 1.6 Balance-sheet chart is an identity
Assets vs Liabilities+Equity are equal by construction; the chart cannot vary. Codex: *"the
strongest defect in 6-12 — drop/replace."* Replace with a chart that has variance (§3.5).

---

## 2. Explicitly REJECTED — do not build

| proposal | author | why rejected |
|---|---|---|
| Single stacked-area P&L over time | agent | Stacked areas make non-baseline bands hard to compare and break on negative values (tax/interest swings). Rejected on that ground alone — the coverage argument originally given (an 8% row-level figure) was itself wrong; see §0.1. |
| Expense mix as 100% stacked bars | Codex | Needs a true R&D/SG&A decomposition. Use the §3.2 residual instead, which sums to revenue by construction. |
| Full `charts/primitives` wrapper layer | agent | Codex: *"framework-building before you know the right chart grammar."* Reduced to §4. |
| Waterfall connector lines | agent | Codex: defer; totals/deltas are legible without them. Real but low value against the data work. |
| Second charting library | agent (considered) | Codex: stay on recharts. Bundle weight, theming duplication, two mental models. Specialised engines only for OHLC. |
| Full layout restructure now | agent | Codex: *"mostly taste unless backed by task failure"* — and layout cannot compensate for missing data. Deferred to §5. |

---

## 3. Per-chart decisions

Position = reading order. Each entry states what the chart answers, its inputs' computability, and
its degrade path.

### 3.1 Headline strip — NEW, position 1 — JUDGEMENT
Answers *"how is this business doing, without scrolling."*
Row of `StatTile` (mandated by design-system.md §Density — a near-copy drifts), each with value,
period-over-period delta, and a sparkline. Metrics chosen by coverage: revenue, gross margin,
operating margin, net income, shareholders' equity.
- Sparkline uses linear interpolation with dots — same rule as §1.2.
- A tile whose metric is uncomputable renders "—" with the reason on hover, never a zero.
**Validation:** an operator can state the direction of the business from the strip alone without
scrolling. If that fails in review, drop the strip rather than iterate on it.

### 3.2 P&L trend — position 2 — KEEP, retarget
Currently a stacked bar (COGS / Opex / Operating income) where **`opex` is literally
`research_and_dev + sga_expense`** — i.e. the current chart already sits on the sparse path this
spec claims to avoid. Draft 1 missed that (Codex: *"still wrong/misrepresented"*).

Two further problems with "the stack total is revenue":
- It is **false** whenever other operating income/expense exists outside COGS + R&D + SG&A.
- It is false whenever either component is missing, which silently shortens the bar.

**Change:** define the middle band as a **residual** — `revenue − cost_of_revenue −
operating_income` — so the stack sums to revenue *by construction* rather than by hope. Inputs are
then the 47% set in §0.2 (revenue + cost_of_revenue + operating_income), and the band is honestly
labelled "operating expenses (all)" rather than implying an R&D/SG&A decomposition we cannot
reliably supply.
**Do NOT** split R&D from SG&A here.

### 3.3 Margin trends — position 3 — KEEP, fix
Three lines (gross/operating/net) is within the 1–3 line guidance. Fix interpolation + theme colours
per §1.2/§1.3. Add a faint band showing each margin's own 5-year range so a value has a reference.

### 3.4 Earnings quality — NEW, position 4 — JUDGEMENT, 86% coverage
Net income vs operating cash flow vs FCF over time. Codex named this a gap; it is the classic
accrual/cash divergence check and the single most useful "is this real?" chart for a long-horizon
investor.

**Gating (per §0.2, not re-derived):** NI + OCF together = **86%** of instruments at ≥3 periods.
FCF uses the settled `operating_cf − ABS(COALESCE(capex, 0))` rule and therefore gates on OCF
alone = **89%** — it does NOT require capex. Any implementation that gates the FCF line on
`capex IS NOT NULL` is wrong.
Replaces the current standalone single-period cash-flow waterfall in reading order; the waterfall
moves down as period detail.

### 3.5 Capital structure — position 5 — REPLACE the identity chart
Instead of Assets vs L+E: **net debt trend** with an interest-coverage overlay. Genuine variance;
answers "is the balance sheet getting safer?"

**Source rule — cited, not invented.** Draft 1 proposed degrading to "long-term-debt minus cash"
reasoning from the 12% `short_term_debt` coverage. The repo already has a documented treatment,
used in three places:

```sql
COALESCE(long_term_debt, 0) + COALESCE(short_term_debt, 0)
```

`app/services/fundamentals/__init__.py:153` · `app/services/peer_comparison.py:185` ·
`app/services/fair_value_band.py:1021` (which carries the full net-debt form,
`… + COALESCE(long,0) + COALESCE(short,0) − cash`).

Use that. Total debt is NULL only when **both** components are NULL; a missing `cash` is a genuine
data gap, not a degrade path. Low `short_term_debt` coverage is therefore already handled by the
settled rule and needs no new invention.

### 3.6 Per-share compounding — NEW, position 6 — JUDGEMENT, 34–50% coverage
Codex's strongest addition: *"long-term investors care whether the business compounds per share,
not just whether aggregate revenue rises."*
Revenue/share, FCF/share, book value/share, indexed to 100 at the first available period, plus a
share-count line so buyback/dilution is visible against them.

**Denominators differ by numerator — do not use one share count for all three** (Codex):

| per-share metric | numerator kind | denominator | why |
|---|---|---|---|
| Revenue/share, FCF/share | duration (flow over the period) | `shares_diluted` | weighted-average diluted shares match a flow measured across the same period |
| Book value/share | instant (point-in-time balance) | `shares_outstanding` | a balance-sheet value divides by shares outstanding **at** that instant, not a period average |

Coverage gates accordingly: flow-per-share 81%, book-value-per-share 69% (§0.2). Render each
sub-series only where its own denominator exists for ≥3 periods; a missing one drops that line, not
the section.

### 3.7 DuPont — position 7 — RESTRUCTURE
Currently 4 series / 2 axes / 3 dashed. Codex: *"genuine usability defect."* DuPont is definitionally
a decomposition, so render as **three small multiples** (net margin, asset turnover, equity
multiplier) with the resulting ROE as a fourth. Kills the dual axis and the dashed spaghetti.

### 3.8 ROIC — position 8 — KEEP, fix interpolation, add reference band.

### 3.9 FCF — position 9 — KEEP AS IS apart from interpolation/theme/currency
Draft 1 said "37% computable; show 'FCF unavailable — capex not reported'". **Both halves were
wrong**: the settled rule (`fcf_yield.py:111,132`) is `operating_cf − ABS(COALESCE(capex, 0))`, so
FCF gates on OCF alone → **89%** of instruments, and a missing capex is not an unavailability
message.

Draft 1 also recorded Codex as *deferring* the dual-axis question and then mandated dropping the
axis anyway because it was "free". That misrepresents a defer as an approval. **Do not drop it**:
the right-hand yield axis carries the shipped #671 TTM FCF-yield signal, which has its own
documented market-cap and FX handling. Removing it deletes a working signal to satisfy a style
preference. Revisit only as a deliberate, separately-evidenced change.

---

## 4. Chart infrastructure — REDUCED scope

Codex trimmed the proposed primitives layer. Do only:

1. Shared formatters: `formatMoney(value, currency)`, `formatPct` — currency-aware, one home.
2. Shared `MoneyAxis` / `PercentAxis` used by fundamentals charts.
3. Lint additions: ban `type="monotone"` and `lightTheme.`/`darkTheme.` outside `chartTheme.ts`,
   scoped to `components/fundamentals/` initially.

**Not now:** wrapping every recharts primitive; a repo-wide bare-`<YAxis>` ban. Revisit once 2–3
pages converge on the same pattern.

---

## 5. Toggles and overlays

The house pattern for a view switch is a **segmented control**, not chips
(`information-architecture.md` §"Presets = a segmented control"). The page already has
Quarterly/Annual; extend that same control rather than inventing a second mechanism.

- **Basis: Quarterly | Annual.** Exists. **Change the default** — currently Quarterly, which for
  AAPL is the sparsest view (Q1×3 Q2×3 Q3×2 Q4×2).
  Draft 1 said "default to whichever basis has more periods". Codex: unsafe — *period count is not
  metric computability*, and choosing requires fetching both bases first, which draft 1 did not
  state. **Revised:** the backend returns a `recommended_basis` field computed server-side from
  **usable periods for the page's headline metrics** (not raw row count), in the existing response;
  the client honours it on first load only and never re-overrides an explicit operator choice.
  This costs no extra round-trip and keeps the rule in one place.
- **Scale: Absolute | Indexed to 100.** Indexed lets a non-expert read relative progress without
  understanding absolute magnitude. Cheap — a client-side transform, no new data.
- **Overlay: none | own 5y range band.** Gives a value a reference. Prefer this to peer overlay,
  which needs a peer set we do not have per-instrument.

**Rejected overlay:** peer/sector comparison on these charts. `PeersPage` exists but a per-instrument
peer set is not a solved data problem (it gated #594 originally). Do not spec it here.

---

## 6. Order of work

Codex revised the agent's order; adopted:

1. **#2184** — page is broken via one URL form.
2. **#2185** — interpolation, theme colours, currency. *Contained, local, prevents misleading charts
   today; must not wait behind the backfill.*
3. **#2182** — deep FY history backfill. Deeper, riskier, backend/pipeline.
4. **§1.5** duplicates — investigate, small count, likely same root cause as #2182.
5. **§3 / #2183** — per-chart restructure and new charts, once data supports them.

Rationale (Codex): *"presentation work before data and visual integrity is premature."*

---

## 7. Operator questions — ANSWERED 2026-07-31, none outstanding

1. **§3.1 headline strip and §3.6 per-share — build, or spec further first?**
   → **BUILD.** Operator: *"if you've refined every chart, codex fully agrees with any changes …
   then please crack on."* Both retain their §3 kill conditions: if the stated validation fails at
   review, delete the item rather than iterate.

2. **Does field-level coverage deserve its own investigation before §3 work?**
   → **NO — folded, not filed.** The question was raised on draft 1's row-level figures, which §0.1
   records as wrong. The corrected instrument-level measurement puts every §3 input at 69–89%, so
   there is no separate coverage crisis to investigate. The one genuinely gated chart (P&L
   composition, 47%) is handled by the §3.2 residual definition. Row-level interior gaps remain
   tracked in #2182 and are not duplicated here.
   Low single-field coverage (`short_term_debt` 12%, `capex` 39%) is already absorbed by the repo's
   settled COALESCE treatments (§3.5, §3.9) and needs no new work.

**No question in this spec is awaiting an operator answer.** Anything discovered during
implementation that would need one is, by §0's standing rule, a reason to stop and write a handoff
note — not to guess.
