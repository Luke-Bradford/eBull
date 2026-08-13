# Ownership denominator cross-check (#1647 part 5)

**Status:** spec · **Epic:** #788 · **Issue:** #1647 (machine-trust contract, last open part)
**Author:** 2026-06-17 · pure read-path, no migration / backfill / ingest / restart

## Problem

The ownership machine-trust contract (#1647) shipped four of five parts: per-slice
as-of coherence (pt1), `is_estimate` completeness (pt2), structured
`corrections_applied[]` (pt3 = #1639), and `SanityChecks` plausibility facts (pt4).
Part 5 — "cross-source validation status" — is the last piece.

The committee framed pt5 as "cross-source each operator-visible ownership figure
against an independent reputable source." Two findings (researched 2026-06-17)
reshape that:

1. **The aggregate ownership % has no clean independent source.** Institutional /
   insider / blockholder percentages are *derived* by summing the same SEC
   13F / Form 4 / 13D filings every vendor reads. They differ only by methodology.
   Empirical: GOOGL institutions render **79.78%** for us vs **84.99%** on Fintel —
   identical SEC source data, different handling of 13F-NT supersession (#1639) and
   Vanguard family dedup (#1644). "Cross-sourcing the %" against a vendor is noise,
   and commercial vendors are runtime-banned anyway (settled decision #532; #788
   ban list — gurufocus / marketbeat / yahoo are dev-time-verification-only).

2. **The denominator IS independently verifiable, and it is the one figure pt1–pt4
   structurally cannot validate.** Every wedge % divides by `shares_outstanding`. A
   wrong-but-self-consistent denominator passes as-of coherence, sanity, residual,
   and oversubscription checks — exactly the #1646 dual-class bug: a 2× combined-vs-
   per-class denominator understated every figure for the ownership card's entire
   history until a *human* cross-sourced it. The fix that finally caught it (#1623)
   was a tie-out to the primary SEC source.

So pt5 = the machine version of "what caught #1646": reconcile the operative
denominator against an **independent SEC-native disclosure of the same quantity**
(the standard "tie-out to primary source" reconciliation — DoD clause 9's
"EdgarTools golden file / SEC EDGAR direct"), and **honestly mark the numerators
`single_source_derived`** so a decision agent knows the % is single-source by
construction, not un-validated by oversight. That honest marker is itself part of
the machine-trust contract.

## Settled decisions / prevention-log applicability

- **#532 free-regulated-source-only** — preserved: pt5 reads only SEC concepts
  already in `financial_facts_raw` + `instrument_class_shares_outstanding`. No
  network, no commercial source.
- **#1102 CIK=entity / CUSIP=security** — the dual-class sum tie-out resolves
  siblings via the existing shared-CIK mechanism (the `instrument_class_shares_outstanding`
  rows already keyed per sibling instrument).
- **#788 methodology** — denominator is `us-gaap:CommonStockSharesOutstanding`
  (XBRL-DEI per the epic); pt5 cross-checks it, does not change it.
- **#840/#923 coverage banner** — untouched; pt5 is a separate evidence field.
- **prevention-log "a source fact can exist at SEC yet be unreachable through our
  ingest — verify reachability"** (#1646) — directly relevant: `dei:Entity
  CommonStockSharesOutstanding` is dimensionally stripped for dual-class issuers, so
  the cross-check MUST degrade cleanly to `method="per_class_subset_bound"` (dual-class) or
  `method="unavailable"` (neither independent figure on file), never fabricate.
- **"facts-not-thresholds" (#1647 pt1/pt4, #1648)** — pt5 follows it: expose both
  values + as-of + diff; `status` is a documented band, never a hard gate.

## Data model

New frozen dataclass on `OwnershipRollup`, mirroring `SanityChecks` / `PerClassDenominator`:

```python
@dataclass(frozen=True)
class DenominatorCrossCheck:
    # How the comparison was made — encodes its STRENGTH so a consumer never reads a
    # subset bound as an independent cross-source (Codex ckpt-1 HIGH):
    #   "independent_concept"   single-class: two DISTINCT SEC disclosures of the same
    #                           count (dei cover-page vs us-gaap balance-sheet) — a real
    #                           independent tie-out.
    #   "per_class_subset_bound" dual-class: traded-sibling FSDS per-class counts summed
    #                           vs the combined us-gaap count. NOT independent (same FSDS
    #                           family; omits untraded Class B) — only flags the
    #                           IMPOSSIBLE (sum > combined). Per-class denom was verified
    #                           at #1623 ingest; this is a structural backstop.
    #   "unavailable"           no_data / outstanding <= 0 / no comparison figure on file.
    method: Literal["independent_concept", "per_class_subset_bound", "unavailable"]
    # primary_value / comparison_value are THE TWO FIGURES THIS CHECK COMPARES — NOT
    # necessarily the rollup's operative denominator (Codex ckpt-1b HIGH: keep pct_diff
    # one uniform formula). primary_concept names which is which:
    #   single-class: primary = the denominator used; comparison = the independent SEC concept.
    #   dual-class:   primary = Σ resolved per-class FSDS counts (sibling-sum); comparison =
    #                 the combined all-class count. (The operative per-class denominator the
    #                 rollup actually divided by is in OwnershipRollup.per_class_denominator.)
    primary_value: Decimal | None
    primary_concept: str | None
    comparison_value: Decimal | None
    comparison_concept: str | None
    primary_as_of: date | None
    comparison_as_of: date | None
    as_of_delta_days: int | None          # |primary_as_of - comparison_as_of|; surfaces the
                                          # cover-vs-balance-sheet (single) / FSDS-vs-balance-sheet
                                          # (dual) date skew (Codex HIGH)
    pct_diff: Decimal | None              # (primary_value - comparison_value) / comparison_value
                                          # — UNIFORM across methods; None when unavailable
    status: Literal["agrees", "minor_skew", "diverges", "plausible", "unavailable"]
    note: str                             # server-owned FE copy (single source)

    @classmethod
    def unavailable(cls) -> DenominatorCrossCheck: ...   # all-None facts, status/method "unavailable"
```

Added to `OwnershipRollup` as `denominator_cross_check: DenominatorCrossCheck`
(non-optional; constructors on every path — incl. no_data — set it explicitly via
`unavailable()`, never relying on a default factory that could mask a missing wiring,
Codex LOW). Same pattern as `SanityChecks.empty()`.

**Dropped from the model: `numerator_source_independence`** (Codex MED — a constant-
valued field nested in a denominator check is cargo-cult). The honest fact — the
aggregate institutional/insider/blockholder **percentages** have no independent source
(every vendor sums the same SEC 13F/Form 4/13D filings and disagrees only by method;
GOOGL: Fintel 84.99% vs ours 79.78%) — is a GLOBAL contract truth, not per-rollup data.
It is documented in the API contract + the metrics-analyst skill, not echoed in every
response.

## Algorithm — `_cross_validate_denominator(...)` (pure)

Inputs (all already computed in `get_ownership_rollup` before the `sanity` call):
`effective_outstanding`, `effective_source.concept`, `per_class_denominator`,
the combined `outstanding` (pre-swap), and an independent-figure lookup.

Method is selected by which denominator the rollup used. A `comparison_value <= 0`
on any path degrades to `unavailable` (never divides; Codex MED).

### A. Single-class — `method="independent_concept"` (the genuine independent tie-out)
When `per_class_denominator is None` and `effective_outstanding > 0`:

- primary = `effective_outstanding`; primary_concept = `effective_source.concept`.
- comparison = the OTHER SEC concept for this instrument, picked as the row whose
  `period_end` is NEAREST primary's `period_end` (NOT bare "latest" — Codex HIGH),
  with `as_of_delta_days` exposed:
  - primary `us-gaap:CommonStockSharesOutstanding` (balance sheet) → comparison =
    `dei:EntityCommonStockSharesOutstanding` (cover page), and vice-versa.
  - dei and us-gaap are inherently DIFFERENT instants (cover page is dated ~weeks
    after the balance-sheet quarter-end), so an exact `period_end` match generally
    does not exist — "nearest" + the surfaced `as_of_delta_days` is the honest shape.
- `pct_diff = (primary - comparison) / comparison`.
- Banded `status` (Codex MED — 5% was too generous):
  - `abs(pct_diff) <= 0.02` → `agrees`
  - `0.02 < abs(pct_diff) <= 0.05` → `minor_skew` (date/buyback drift — informative,
    not alarming; raw `pct_diff` + `as_of_delta_days` let the consumer judge)
  - `abs(pct_diff) > 0.05` → `diverges` (the #1646 class: a 2× error = 100% trips loudly)
- comparison concept absent (dual-class strip, or a thin issuer with only one concept
  on file) → `unavailable`.

Empirical panel (2026-06-17, latest rows): AAPL 0.13%, GME ~0%, MSFT -0.01%, JPM
-0.62%, HD 0.01% — all `agrees` at the 2% band.

### B. Dual-class — `method="per_class_subset_bound"` (structural backstop, NOT independent)
When `per_class_denominator is not None`:

- **primary** = Σ of all resolved sibling per-class FSDS counts
  (`instrument_class_shares_outstanding` across the CIK's sibling instruments);
  primary_concept = `"Σ resolved per-class FSDS (sibling instruments)"`,
  primary_as_of = the FSDS `period_end`. (The rollup's operative denominator —
  this instrument's own per-class count — is in `per_class_denominator`, not duplicated here.)
- **comparison** = the combined all-class `us-gaap:CommonStockSharesOutstanding`, read
  at **exactly** `period_end == FSDS period_end` (the same balance-sheet instant the
  FSDS classes were measured at — `financial_facts_raw` carries that historical 10-K row);
  fall back to the NEAREST us-gaap row only if no exact match, with `as_of_delta_days`
  exposed (Codex HIGH — resolves the prior "nearest, same instant" contradiction; uses
  exact-period here, distinct from the per-class denom freshness guard which deliberately
  relaxes equality because companyfacts leads FSDS by a quarter). comparison_concept =
  `"us-gaap:CommonStockSharesOutstanding (combined all-class)"`.
- `pct_diff = (primary - comparison) / comparison = (sibling_sum - combined)/combined`
  (UNIFORM dataclass formula; ≈ -6% for Alphabet = the untraded Class B remainder) — a
  clean "traded classes are X% below the all-class total" fact at one instant.
- `status`:
  - `diverges` if `sibling_sum > combined` (IMPOSSIBLE — traded classes cannot exceed
    the all-class total → an FSDS mis-resolution / curated-map drift), OR a per-class
    guard is violated (`per_class <= 0` or `per_class >= combined`).
  - else `plausible` — the traded classes are a valid subset; the untraded remainder
    (Class B, no `instruments` row) is unverifiable, so we do NOT claim `agrees`. NO
    lower floor (`_CLASS_SUM_FLOOR` DROPPED — Codex BLOCKER: a material founder class
    or a 49/51 split would falsely "diverge", and a wrong map at 55% would falsely
    "agree"; the only sound machine assertion is the impossible-overage).
- The per-class denominator was already cross-source-VERIFIED at #1623 ingest (vs the
  10-Q per-class cover + companyconcept) and is structurally guarded at read time
  (`max_pie_holder <= per_class` in `_should_use_class_denominator`, which already
  rejects a too-small per-class count). pt5's dual-class half is therefore a thin
  backstop on the impossible-overage, stated honestly via `method` + `plausible`.
  `note` says so.

### C. Unavailable — `method="unavailable"`
`no_data` rollups, `effective_outstanding <= 0`, comparison absent, or
`comparison_value <= 0` → `DenominatorCrossCheck.unavailable()`.

## Read-path integration

In `get_ownership_rollup`, immediately after `sanity = _compute_sanity(...)` (line
~2804), add:

```python
denominator_cross_check = _cross_validate_denominator(
    conn, instrument_id,
    effective_outstanding=effective_outstanding,
    effective_as_of=effective_as_of,          # primary period_end — needed for nearest-pick + as_of_delta_days (Codex HIGH)
    effective_concept=effective_source.concept,
    combined_outstanding=outstanding,
    per_class_denominator=per_class_denominator,
)
```

and pass it to the `OwnershipRollup(...)` constructor. The comparison-figure DB read
reuses the same `snapshot_read` connection (consistent snapshot, like every other
reader). One small reader
`_read_shares_outstanding_near(conn, instrument_id, *, taxonomy, concept, near_period: date)`
returns `(value, period_end)` for the `financial_facts_raw` row (`unit='shares'`)
whose `period_end` is NEAREST `near_period` — `ORDER BY ABS(period_end - %(near)s) ASC,
filed_date DESC LIMIT 1` (NOT latest; Codex HIGH) — or `None`. Single-class passes
`near_period = effective_as_of` (nearest independent-concept row to the primary's
instant); dual-class passes `near_period = FSDS period_end` against the combined
us-gaap concept (exact match preferred — an exact same-period row sorts first at
delta 0). The dual-class sibling sum reuses the existing sibling resolution +
`instrument_class_shares_outstanding`.

## API + FE + CSV

- API: `_DenominatorCrossCheckModel(BaseModel)` in `app/api/instruments.py`, mirroring
  `_SanityChecksModel`; mounted as `denominator_cross_check` on the rollup response
  (non-optional, `default_factory` = unavailable). Decimals serialized as strings
  (repo convention).
- FE: `OwnershipDenominatorCrossCheck` type in `frontend/src/api/ownership.ts`
  mirroring the model. **No new chart in this PR** — the field is machine-trust
  evidence (the audience is a decision agent + the existing provenance footer). A
  one-line provenance caption ("Denominator independently reconciled to SEC
  cover-page, agrees within 0.1%") MAY render under the existing
  `shares_outstanding_source` footer; keep it minimal. *(Open: operator visual call —
  caption yes/no. Default: yes, one line, no new component.)*
- CSV: one inert `__denominator_cross_check__` memo row (mirrors
  `__per_class_denominator__`), so the audit export carries the tie-out.
- Contract doc (replaces the dropped `numerator_source_independence` field): the
  "aggregate ownership %s are single-source SEC aggregation, no independent source"
  fact is recorded once in `.claude/skills/metrics-analyst/SKILL.md` (Institutional/
  Insider/Blockholder ownership % "Validation" lines) + the rollup endpoint's response
  docstring — a global contract truth, not echoed per-response.

## Edge cases / fail-closed

- Independent concept absent → `unavailable`, never fabricate (prevention-log #1646).
- `effective_outstanding <= 0` or no_data → `unavailable`.
- dei present but stale (older period_end than us-gaap) → still compared; `pct_diff`
  + `comparison_as_of` + `as_of_delta_days` make the staleness visible (facts-not-thresholds). The band
  tolerates it.
- A divergence is NOT an error and NOT a gate — it is a fact an agent/operator reads.
  pt5 never changes a share count or a denominator (that is #1623's job).

## Tests (pure-logic, table-driven)

`tests/test_denominator_cross_check.py`:
- single-class `agrees` (panel-like 0.1% diff); `minor_skew` (3% — date/buyback drift);
  `diverges` (2× = #1646 sim); comparison-concept-absent → `unavailable`.
- band boundaries: exactly 0.02 → `agrees`; one tick over → `minor_skew`; exactly 0.05
  → `minor_skew`; one tick over → `diverges`.
- nearest-period pick: given two candidate comparison rows, the one closest to primary's
  `period_end` wins; `as_of_delta_days` is the actual gap.
- dual-class `plausible` (Alphabet sibling-sum ≈ 94% of combined); dual-class `diverges`
  (sibling_sum > combined → mis-resolution); dual-class per-class guard violation
  (`per_class >= combined`) → `diverges`. Untraded-class-heavy sweep (Codex LOW): traded
  sibling sum at 40% / 50% / 55% / 95% of combined all → `plausible` (NO floor — only the
  >100% overage diverges); 101% → `diverges`.
- `outstanding <= 0` → `unavailable`; `comparison_value <= 0` → `unavailable`; no_data →
  `unavailable`.

DB-backed: ONE API-contract test that the field renders on a real rollup (panel
instrument) with the expected `method`/`status` — no thick suite. Service tests assert
every no_data / short-circuit path sets `unavailable()` explicitly (Codex LOW — no
silent default-factory).

## Definition of done (ETL clauses 8–12)

8. Panel smoke AAPL/GME/MSFT/JPM/HD + GOOGL/GOOG on dev; record method + status + pct_diff.
9. Cross-source: the tie-out IS the cross-source — record the dei-vs-us-gaap figures
   per panel instrument (already gathered 2026-06-17).
10. No backfill / no sec_rebuild (pure read-path; reads existing tables).
11. Verify `/instruments/{symbol}/ownership-rollup` renders `denominator_cross_check`
    for the panel + GOOGL/GOOG after the API runs the branch.
12. PR records the above + commit SHA.

No jobs-process restart (pure read-path). API must be on the branch to serve the new
field (dev API already runs main; restart onto branch for the live-endpoint verify).
