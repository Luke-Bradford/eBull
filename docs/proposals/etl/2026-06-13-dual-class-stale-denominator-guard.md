# Stale shares-outstanding denominator guard (#1581)

Status: proposal (unshipped). Closes #1581. Closes #1580 (duplicate, already closed).

Motivating case is the dual-class trap, but the fix is a general **stale-denominator guard** —
see the fleet finding below.

## Problem

`/instruments/BRK.B/ownership-rollup` serves `shares_outstanding = 941,481 as_of 2011-04-29`
as the denominator. A single institution renders at **124.7%**, residual oversubscribed. Real
Class-B outstanding ≈ 1.31B.

## Root cause (empirically confirmed, dev DB 2026-06-13, `scripts/_probe_1580.py`)

- Multi-class issuers report `dei:EntityCommonStockSharesOutstanding` **per share-class**
  (member-dimensioned) in modern filings; Berkshire stopped emitting an un-dimensioned total
  after 2011.
- Our **only** XBRL-facts source is the SEC **companyfacts JSON API**, which is
  **un-dimensioned-only**. `financial_facts_raw` has **no dimension/member/axis column**. The
  `sec_xbrl_facts` manifest parser (`app/services/manifest_parsers/sec_xbrl_facts.py`) is a
  **synth no-op** — no per-filing dimensioned extraction. So the per-class current share count is
  **nowhere in our store.** #1581's fix-2 ("use dimensioned facts from `sec_xbrl_facts`") rests on
  a false premise.
- Picker: `instrument_share_count_latest` (sql/052) orders `period_end DESC` (the filed_date /
  accession dedupe happens inside the upstream `share_count_history` view, not the latest view).
  It faithfully returns the newest row that exists — for BRK.B that is 2011.

### Fleet finding (decides the threshold; F3 from Codex ckpt-1)

`instrument_share_count_latest` over **tradable** instruments with a non-null denominator:

| bucket | count |
|---|---|
| total tradable w/ denominator | 4,604 |
| as_of > 548 days (→ guard fires) | **209** |
| as_of > 730 days | 180 |
| as_of in 365–548 day band | 126 |

The 209 are **not** all dual-class — most are dev **ingest-coverage gaps** (Visa @2010, Nike
@2015, Ford @2011, UPS/Comcast/Mastercard). Spot-check confirmed the **view is not buggy**:
NKE's newest `*SharesOutstanding` row in `financial_facts_raw` IS 2015-07-17 (the view returns
exactly that); dev simply hasn't ingested their recent companyfacts. A 2010 denominator produces
wrong percentages for Visa exactly as for BRK.B, so `no_data` is correct for all 209 regardless of
cause. The guard is **self-healing**: when a fresh denominator lands (< 548d) the instrument
renders again — no manual step. On prod (full companyfacts ingest) the count shrinks to genuine
dual-class + delisted/dark issuers.

**Consequence:** the guard, banner, and empty-state copy are **cause-agnostic** — they state what we
know ("the figure is stale") not why (could be the dimension-only trap OR a coverage gap). Do NOT
assert "multi-class" in operator copy.

## Decision (operator-approved 2026-06-13)

Ship the cheap interim guard (#1581 fix 3). The real per-class denominator (cover-page parse or
per-filing dimensioned-XBRL ingest + instrument→share-class mapping) is a substantial build,
partially blocked on the #1577 entities-layer question — deferred to a follow-up issue.

## Design

Read-time staleness guard in the ownership-rollup path only. No schema/parser/stored-data change.

1. **Pure policy function** (table-testable, no DB):

   ```python
   _STALE_DENOMINATOR_MAX_AGE_DAYS: Final[int] = 548  # ~18 months

   def _denominator_too_stale(as_of: date | None, today: date) -> bool:
       if as_of is None:
           return False  # absence handled by the existing None/<=0 short-circuit
       return (today - as_of).days > _STALE_DENOMINATOR_MAX_AGE_DAYS
   ```

   Threshold: a covered issuer's cover-page count refreshes every 10-Q/10-K; 18 months clears even
   an annual-only filer's between-filings gap, so it never false-positives on a normally-ingested
   instrument (panel: AAPL 57d, GOOG 74d, GME 8d, MSFT 51d, JPM 74d, HD 25d — all < 548). A
   future `as_of` (`today - as_of < 0`) is treated as **not stale** — out of scope here, but pinned
   by a test so a corrupt future `period_end` cannot silently start bypassing the guard (F5).

2. **Guard at the single caller** (`get_ownership_rollup`, beside the existing
   `outstanding is None or <= 0` short-circuit). `_read_shares_outstanding` stays a pure reader.
   Caller altitude (not the SQL view) is deliberate — a view-level guard would silently alter market
   cap / dilution / reconciliation, all explicitly out of scope (F4, Codex-confirmed).

3. **Honest, cause-agnostic copy.** Reusing the generic `no_data` banner ("…not on file. Trigger a
   fundamentals sync") would mislead: the figure IS on file, just stale. `OwnershipRollup.no_data`
   gains keyword-only `reason: Literal["absent","stale_denominator"]` + `stale_as_of: date | None`.
   For the stale reason it builds a banner naming the date (en-GB, server-formatted) and stating the
   figure is too stale to use as a denominator. **`reason="stale_denominator"` requires
   `stale_as_of is not None`** (ValueError otherwise) so the only retained provenance is never lost
   (F6). The `info_chip` is NOT special-cased — it is dead UI on the `no_data` path (the FE returns
   before the concentration chip renders) (F2). **State stays `no_data`** — the 5-state coverage
   machine (#840/#923 settled) is unchanged; only server-owned copy differs.

4. **Retain `shares_outstanding_as_of` on the stale payload as the FE discriminator.** The generic
   `absent` path keeps nulling it; the stale path sets `shares_outstanding_as_of = stale_as_of`
   (shares_outstanding stays None). `banner.state == "no_data" && shares_outstanding_as_of != null`
   ⟺ stale — no new API field, no `types.ts` change.

5. **FE empty-state copy (F1 — the "no FE change" claim was wrong).**
   `OwnershipPanel.tsx` renders a **hardcoded** `EmptyState` below the banner for every `no_data`
   payload ("XBRL shares-outstanding not yet on file… Trigger a fundamentals sync"). Branch its
   `description` on the discriminator above: stale → "Shares-outstanding figure on file is too stale
   to use as a denominator — ownership percentages are suppressed until a current figure is ingested
   (next SEC filing / fundamentals refresh)." (date-free; the banner carries the date). Matches the
   existing pattern where `no_data` already shows banner + empty-state together.

## Invariants preserved

- **#840/#923 5-state coverage machine** — no new state; server-owned banner copy override only.
- **#1102 CIK = entity, CUSIP = security** — untouched.
- **No stored-data mutation** — pure read-path guard; idempotent; no migration; no backfill.
- **API shape unchanged** — discriminator reuses the existing `shares_outstanding_as_of` field.

## Scope / out of scope

- In scope: the ownership-rollup denominator only.
- Out of scope (→ **#1623**): `/summary.identity.market_cap` (`xbrl_derived_stats`) and
  `instrument_dilution_summary` also read the stale row; the real per-class denominator; the #1577
  entities-layer mapping. #1623 captures all three.

## Test plan

- Pure `_denominator_too_stale` table test: `None`→False; 2011 date→True; last-quarter date→False;
  boundary (548→False, 549→True); **future date→False (F5)** — all with an injected `today`.
- `no_data(reason="stale_denominator", stale_as_of=…)`: banner body names the date, state stays
  `no_data`, `shares_outstanding_as_of` retained; `reason="stale_denominator"` + `stale_as_of=None`
  raises ValueError (F6).
- Integration (DB): instrument with only a stale dei row → rollup `no_data` + stale banner +
  retained as_of; instrument with a current row → renders normally (healthy-panel regression).
- FE unit: `PanelBody` no_data + non-null as_of → stale empty-state copy; no_data + null as_of →
  generic copy.

## DoD (ETL clauses 8–12)

- 8 smoke panel: AAPL/GME/MSFT/JPM/HD render unchanged; BRK.B → `no_data` stale banner.
- 9 cross-source: BRK.B real Class-B ≈ 1.31B (SEC 10-Q cover page) — the value we deliberately
  decline to show against a stale denominator.
- 10 backfill: **N/A** — read-path guard, no stored-data/parser/schema change. Documented.
- 11 verify endpoint: `/instruments/BRK.B/ownership-rollup` post-deploy shows the stale banner; the
  fleet count of newly-`no_data` instruments recorded.
- 12 PR records verification + commit SHA per clause + the 209-instrument blast radius.
