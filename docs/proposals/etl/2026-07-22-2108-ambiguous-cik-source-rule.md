# #2108 — source-rule verdict: 31 ambiguous CIK→multi-instrument mappings + display-grade insider entity-row policy

Status: PROPOSAL (session 2026-07-22). Resolves the #828 PR-1 sub-ticket
(spec `2026-07-22-828-insider-cik-routing.md` §Sub-ticket). Research-first ticket:
all three questions answered from the SEC's own record + full-population dev-DB
scans, no heuristics.

## Source rule

- **SEC `company_tickers.json` + `company_tickers_exchange.json`** (both fetched
  2026-07-22): a CIK legitimately maps to MULTIPLE tickers — both files are
  ticker-grain TODAY (each: 10,419 rows / 8,014 unique CIKs / 1,463 multi-ticker
  CIKs; BAC=17, JPM=9). The sec-edgar skill's 2026-05-17 note that
  `company_tickers.json` is CIK-grain/one-row-per-CIK is FALSIFIED by this fetch
  (SEC converged the two files); skill corrected in this PR. All 31 ambiguous
  CIKs appear with ≥2 tickers and **all 71 of our mapped instruments appear in
  the SEC's current list for their CIK** — our sets are strict subsets (the
  eToro-tradable subset). Confirms settled #1102 (CIK = legal entity, not
  security) directly from the source.
- **Exchange Act §16(a)**: Forms 3/4/5 attach to issuers with a class of EQUITY
  registered under §12. **Rule 3a12-3(b)** exempts securities of foreign private
  issuers from §16 (de-minimis expected exposure, not a filing prohibition).
  ETNs are debt — no §16 coverage. Predicts ≈zero insider-filing exposure for
  the FPI-ADR and ETN/trust classes; the full-pop scan matches (16 rows total
  across the three FPI-ADR CIKs, zero across the three ETN/trust CIKs — below).
- **No SEC rule designates a "primary" share class** (no primary flag in either
  tickers file; Form 3/4/5 name the issuer entity + per-transaction security
  titles; no Item/Rule found designating a primary listing). File ROW ORDER is
  not a primary signal either — empirically TAP-A precedes TAP while AGM
  precedes AGM-A, so ordering is inconsistent with any liquidity/primacy rule.
  Any display-grade "primary instrument" pick is OUR convention, not a source
  rule — which is exactly why the settled read-side model (per-instrument
  visibility of entity-level rows, sec-edgar skill §3.6) does not need one.
- **Repo-settled**: #1102 (CIK=entity), #1117 PR-B (issuer-sibling fan-out),
  sec-edgar skill §3.6 write-side/read-side rules.

## Full-population verification (dev, 2026-07-22)

1. **The 31 sets vs SEC record**: every CIK confirmed multi-ticker in
   `company_tickers.json`; every one of our 71 mapped instruments appears in the
   SEC's list for that CIK. 0 mismatches.
2. **Resolver-set parity**: 28/31 `instrument_cik_history` sets ==
   `external_identifiers` (sec,cik) sets exactly. 3 sets differ only by EXTRA
   resolver members (15616 STRK.US, 15721 BBDO, 15724 LILAP) — instruments bound
   after the one-shot 2026-06-03 history import (`backfill_current_history` has
   no callers; the 3 also lack `instrument_sec_profile` rows). Production
   resolver is authoritative; history table is best-effort.
3. **Temporal shape**: all 71 of our mapped tickers are LIVE under their CIK in
   the SEC's CURRENT record (verification 1) → all 31 ambiguities are
   CONCURRENT multi-listings per the source, not historical chains.
   Secondary: zero closed ranges in local `instrument_cik_history` (all 71 rows
   `imported`/open — the frozen 06-03 import alone would not prove this, hence
   the SEC-current-record basis). `instrument_id_for_cik_at_date` temporal
   semantics are unnecessary for this cohort.
4. **Insider exposure**: 4,825 `insider_filings` rows under the 31 CIKs.
   **4,825/4,825 have `instrument_id` ∈ the production sibling set — 0 out.**
   The three ETN/trust CIKs (Barclays, BMO, ProShares Trust II) have ZERO
   insider filings; the three FPI-ADR CIKs have 16 total (Bradesco 11, Cemig 5,
   Andina 0 — genuine Forms 3/4; the Rule 3a12-3(b) exemption makes exposure
   de-minimis, it does not prohibit filing). Whether those filers file
   voluntarily or the issuers ceased to qualify as FPIs is immaterial to the
   mapping verdict: all 16 rows sit inside their sibling sets.
5. **`filing_events` sibling coverage**: 4,824/4,825 accessions have fe rows for
   the FULL sibling set. The 1 partial: `0000899243-20-019076` (Strategy,
   2020-07-06) missing fe for 15616 (STRK.US — CIK bound after fan-out ran).
6. **Bridge-vs-direct reach** (falsification of the reader fix below):
   `insider_transactions` 1,050,536 rows, `insider_initial_holdings` 118,268
   rows — **0 rows in either table are reachable by direct
   `instrument_id`-key but NOT via the `filing_events` bridge.** The bridge is a
   strict ≥ of direct-key reach.

## Classification (question 1)

| Class | CIKs | Members | Verdict |
| --- | --- | --- | --- |
| Dual/multi-class common siblings | 21 | TAP, MKC, SENE\*, WSO, WLY, BELF\*, DGIC\*, AGM, UONE\*, UA/UAA, FWON\*, NWS\*, LBTY\*, LBRD\*, Z/ZG, GOOG\*, METC\*, LILA\*, BATR\*, GLIB\*, LLYV\* | Correct per SEC record; fan-out correct per #1102/§3.6 |
| FPI ADR dual-series | 3 | AKO-A/B, CIG/CIG-C, BBD/BBDO | Correct — same entity, two ADR programs on different share series; de-minimis §16 exposure (11/5/0 filings, verification 4) |
| Common + listed warrant/preferred | 4 | MSTR+STR\*, HTZ+HTZWW, XRX+XRXDW, OPEN+3 warrants | Correct — same registrant; entity-level events apply |
| ETN / trust product umbrella | 3 | Barclays→DJP+VXX, BMO→BMO+NRGU, ProShares Trust II→6 funds | Correct per SEC record (SEC maps 9/27/16 tickers to these CIKs); zero §16 filings possible, insider fan-out vacuous |
| Predecessor/successor | **0** | — | Class does not exist in this cohort |

**Decision 1: all 31 mappings are CORRECT per the SEC's own record. No remap, no
per-class temporal semantics, no code change to `siblings_for_issuer_cik`.**

Non-issue confirmed for Class 4: BMO (FPI, 40-F) has 0 `financial_periods` rows,
so no bank-financials fan-out onto ETN pages exists today.

## Display-grade entity-row policy (question 2)

**Decision 2: the PR-1 interim policy is promoted to FINAL.** Entity-row
`instrument_id` on `insider_filings` / `insider_transactions` /
`insider_initial_holdings` = the unambiguous `instrument_cik_history` instrument
when one exists, else `min(sibling set)` — deterministic, bookkeeping-grade.
No metadata UPDATE, no `is_primary_listing` rebind:

- `instruments.is_primary_listing` is per-SYMBOL dedup (index
  `upper(symbol), is_primary_listing DESC`; both siblings carry `true` in all 31
  sets) — it cannot express "primary class of the entity".
- No SEC source rule for a primary class exists (above), and the settled §3.6
  read-side rule already delivers per-instrument display via the
  `filing_events` bridge — a display-grade entity-row pick would be a second,
  redundant display mechanism with no documented rule behind it.

**The real defect found instead** — two readers violate the §3.6 read-side rule
by keying entity-level tables on `instrument_id` directly:

1. `app/services/holder_name_resolver.py:130,161` (`resolve_holder_to_filer`) —
   feeds DEF 14A → Form 4/3 holder cross-matching in `ownership_rollup` +
   `def14a_drift`. Direct key means matches succeed only on the sibling holding
   the entity row: GOOGL's DEF 14A drift misses Form 4 cumulative baselines
   stored under GOOG. Operator-visible on all 31 sets.
   **Fix (this ticket's impl): sibling-union — resolve the instrument's
   sibling set once per call (new `sec_identity` helper) and filter
   `WHERE instrument_id = ANY(sibling_ids)` in both queries.**
   - Reach: union ⊇ direct (verification 6: 0 direct-only rows in either
     table) and union ⊇ bridge (bridge misses the STRK partial accession of
     verification 5; the union covers it). The #828 PR-1 routing invariant
     (entity-row `instrument_id` ∈ sibling set by construction) + verification
     4 (4,825/4,825 in-set today) guarantee the union never reaches OUTSIDE
     the issuer entity. For single-CIK instruments the sibling set is `[X]` →
     byte-identical behaviour to today.
   - The canonical bridge form was REJECTED on measured request-time cost:
     `resolve_holder_to_filer` runs per-DEF14A-holder inside the
     `/ownership-rollup` endpoint. Dev EXPLAIN ANALYZE, worst-case
     instrument (9,114 txn rows): direct 24ms, `filing_events` bridge 114ms,
     sibling-union 30ms (7ms on the GOOG/GOOGL pair). Bridge ≈5× per holder
     at the endpoint; union ≈parity.
2. `app/services/ownership_observations_sync.py` (legacy #840.E bridge sweep) —
   re-derives observations under the entity-row instrument only, while live
   ingest fans out across siblings. Non-destructive (never deletes sibling
   rows), pre-#1117 module. Documented as debt — NOT changed here (its write
   fan-out is a behavioral change to a repair job; separate ticket if the drift
   sweep ever matters for the 31 sets).

## The 1,998 ambiguous-CIK strays (question 3)

**Decision 3: NO rebinding.** The cohort dissolves exactly like the 769
share-class "strays" in the parent spec: verification 4 shows all 4,825 rows
under the 31 CIKs (a superset of the 1,998) sit INSIDE the production sibling
set — healthy under #1102/§3.6. The 06-28 "rebind" plan is falsified for this
cohort too.

## Residuals (note-only)

- 1 accession (`0000899243-20-019076`) missing the 15616 fe row — heals via a
  scoped `sec_rebuild` for Strategy or a future re-parse; single row, no sweep.
- `instrument_cik_history` is a frozen 2026-06-03 import (no live writer); the
  production resolver never reads it, so staleness is bounded to the
  best-effort history helpers.

## Impl scope (one small PR)

1. `sec_identity.py` — new helper `sibling_instruments_for_instrument(conn,
   instrument_id) -> list[int]` (instrument → CURRENT primary sec/cik →
   currently primary-bound instruments; falls back to `[instrument_id]` when
   the instrument has no CIK binding). Both sides pin `is_primary = TRUE`
   (Codex ckpt-2): a demoted historical CIK (#1173 upsert demotion; 8 rows
   today, none co-bound elsewhere — full-pop) must not union across issuer
   boundaries if ever recycled.
   Also correct the `siblings_for_issuer_cik` docstring's
   "`instruments.is_primary_listing`" pick suggestion — falsified here
   (per-symbol dedup flag, both siblings `true` in all 31 sets).
2. `holder_name_resolver.py` — both queries switch to
   `instrument_id = ANY(sibling_ids)`. Pure-logic test not possible (SQL
   change) → one DB-tier test: entity rows under sibling A, resolve from
   sibling B succeeds; single-CIK instrument unchanged.
3. sec-edgar skill — correct the 2026-05-17 `company_tickers.json` CIK-grain
   claim (both files ticker-grain as of 2026-07-22 fetch).
4. Spec stays in `docs/proposals/etl/` (research verdict); parent #828 spec's
   §Sub-ticket resolves to this document.

## Risks

- Sibling-union depends on the entity-row ∈ sibling-set invariant staying true;
  #828 PR-1 enforces it at every Form 3/4/5 write chokepoint and PR-2 repaired
  the stock. A regression would surface as a resolver miss (match quality),
  never a wrong-instrument match — the union only ever WIDENS the candidate
  set within the issuer entity.
- No other consumer keys entity insider tables by `instrument_id` directly
  (grep verified: drillthrough + API readers already bridge via
  `filing_events` with `fe.provider='sec'` pinned).
