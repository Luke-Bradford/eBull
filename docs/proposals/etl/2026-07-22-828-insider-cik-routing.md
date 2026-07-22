# #828 — insider ownership writer CIK routing + owner-stream mislink repair

Status: PROPOSAL v2 (session 2026-07-22; v1 revised per Codex ckpt-1 — cohort
recomputed with the production resolver, owner-bridge-row keep dropped, entity-row
interim policy fixed, full-pop invariants added). Supersedes the 2026-06-28
issue-comment plan — that plan's "rebind 1,491 clean-CIK strays" cohort is FALSIFIED
by decomposition (below).

## Source rule

- **Exchange Act §16(a) + Form 3/4/5**: ownership reports attach to the ISSUER's
  equity. The EDGAR ownership XML schema (`<ownershipDocument>` — SEC EDGAR Ownership
  Technical Specification) carries the subject registrant in `<issuer>/<issuerCik>`;
  the reporting owner is a separate `<reportingOwner>` block. The parsed
  `issuerCik` is therefore the authoritative subject key, independent of which EDGAR
  stream surfaced the document.
- **EDGAR indexes each ownership document under BOTH the issuer's CIK stream and
  every reporting owner's CIK stream** (submissions JSON / browse-edgar list filings
  "filed by" and "filed for" the CIK; repo skill `.claude/skills/data-sources/sec-edgar.md`
  §"Archive path quirk" line 349 + §"issuer-scoped fan-out" line 375). A Form 4 for
  BAC filed by Berkshire appears in Berkshire's submissions feed — discovery keyed on
  the OWNER's feed mislinks the filing to the owner's instrument.
- **Repo-settled write model** (sec-edgar skill §"Write-side rule" line 384, #1117
  PR-B): issuer-scoped per-instrument writes (observations) fan out across every
  instrument sharing the issuer CIK via `siblings_for_issuer_cik`
  (`app/services/sec_identity.py:26` — reads `external_identifiers` sec/cik);
  entity-level tables (`insider_filings`, `insider_transactions`, …) stay
  PK=accession; read paths bridge per-instrument via `filing_events.instrument_id`
  (verified: `app/api/instruments.py:2940`, `app/services/ownership_drillthrough.py:331,409`).

## Full-population verification (dev, 2026-07-22)

Resolver = the PRODUCTION sibling resolver (`external_identifiers` sec/cik sets),
NOT `instrument_id_for_historical_cik` (Codex ckpt-1 MED: v1 mixed the two; the
history-helper numbers 730/1,499 are retained below only as cross-checks).

`insider_filings`: 557,562 rows.

| cohort (production resolver) | n | verdict |
| --- | --- | --- |
| stored ∈ issuer sibling set | 532,065 (95.4%) | healthy (includes share-class siblings: BF.A vs BF-B, BIO.US vs BIO-B, MOG.A/MOG-B, KELYA/KELYB — the 06-28 plan would have wrongly "rebound" 769 of these) |
| no sibling set for issuer_cik | 24,739 | unroutable — no canonical answer exists; out of scope |
| **stored ∉ sibling set (owner-stream mislink)** | **758** | **repair cohort** — BAC under BRK.B, CHTR under LBRDA, PRME under GOOG, SRFM under PLTR, QDEL under CG: all reporting-owner relationships |

Cross-checks: history-helper decomposition gave 730 mislinks / 769 sibling-class /
1,998 ambiguous-CIK strays — the 758 production-resolver cohort is the authoritative
scope. `instrument_cik_history` shape: 5,109 rows, 0 instruments with >1 CIK, 31
CIKs → >1 instrument (sub-ticket).

**Corruption surfaces on the 758:**

1. `ownership_insiders_observations` under the WRONG instrument for ~621 accessions
   (owner's ownership chart shows the issuer's insider rows — BAC observations on
   BRK.B). Many accessions also carry canon rows (post-#1117 fan-out) — wrong rows
   are ADDITIVE pollution, not substitution.
2. `filing_events` rows binding accession → owner instrument (the read-path bridge:
   drillthrough/L2 lists + tombstone counts key `fe.instrument_id`).
3. `insider_filings.instrument_id` entity-row column — bookkeeping only (no
   read-path key found; see verified read paths above).

**Raw-store coverage** (Codex ckpt-1 MED): 758/758 mislink accessions have
`filing_raw_documents` rows (form3/form4/form5 xml) — repair is fully local, no SEC
re-fetch.

## Plan

### PR-1 — writer routing (prevent new mislinks)

In the Form 4 / Form 3 apply paths (`insider_transactions._ingest_single`,
`insider_form3_ingest._ingest_single`) and DEF 14A
(`def14a_ingest._ingest_single_accession`):

- After parse, resolve `parsed.issuer_cik` → sibling set via
  `siblings_for_issuer_cik` (the settled resolver; NOT the single-answer
  `instrument_id_for_historical_cik`).
- If the discovery-time instrument ∉ sibling set AND the set is non-empty: log
  warning; write the entity row + observations under the ISSUER's siblings only.
  **No live `filing_events` bridge row for the owner instrument** (Codex ckpt-1
  HIGH: a live owner row IS the L2/tombstone pollution). Discovery idempotency
  (does re-enqueue happen without a filing_events row for the discovering
  instrument?) is an implementation-time check — if a row is required, it must be
  written in a form the read bridges exclude (e.g. tombstoned/flagged), not a live
  binding.
- **Entity-row `instrument_id` interim policy** (Codex ckpt-1 HIGH): deterministic
  pick — the unambiguous `instrument_cik_history` instrument when one exists, else
  `min(sibling set)`. Safe because the column is bookkeeping (read paths bridge via
  `filing_events`); the *display-grade* policy (`is_primary_listing`?) stays in the
  sub-ticket, and switching later is a metadata-only UPDATE.
- Sibling set empty: keep discovery linkage as today (24.7k-row unroutable cohort).
- Ambiguous CIKs (31): PR-1 is PARITY with live ingest — `siblings_for_issuer_cik`
  already fans ALL new filings across those sets today (#1117); PR-1 only applies
  the same fan-out to mislink-rerouted filings. Whether those 31 mappings are
  themselves correct is the sub-ticket; PR-1 adds no new ambiguity exposure.

### PR-2 — backfill repair of the 758 mislinks

Scope: stored ∉ issuer sibling set AND sibling set non-empty (the production-resolver
cohort, recomputed at run time).

1. Snapshot affected `filing_events` + observation rows (audit table
   `828_mislink_snapshot` or equivalent) — reversibility (destructive DELETEs below).
2. DELETE `ownership_insiders_observations` rows for (accession, non-sibling
   instrument) pairs; re-run observation fan-out from stored raw XML (rewash path);
   `refresh_insiders_current` on every touched instrument (both sides).
3. Rebind `insider_filings.instrument_id` per the interim policy.
4. `filing_events`: add issuer-sibling rows if absent; DELETE the owner-instrument
   bindings for these accessions.
5. **Full-pop post-repair invariants** (Codex ckpt-1 FLAG — not sample-based):
   - `SELECT COUNT(*) FROM insider_filings f JOIN sib s … WHERE NOT (f.instrument_id = ANY(s.ids))` → 0
   - zero `ownership_insiders_observations` rows on non-sibling instruments for the cohort
   - zero `filing_events` owner bindings for the cohort
   - issuer-sibling observation coverage ≥ pre-repair canon coverage (no net loss)
   - global row-count deltas equal exactly the targeted DELETE counts
6. Endpoint verification (operator-visible): ownership-rollup for BRK.B / LBRDA /
   GOOG / PLTR / CG (owner side must DROP foreign rows) + panel AAPL/GME/MSFT/JPM/HD
   unchanged. Cross-source: BAC insider list vs an independent source.

### Sub-ticket (file at PR-1) — ambiguous 31 CIKs + display-grade entity-row policy

Source-rule the 31 cik→multi-instrument mappings (ADR/ordinary, dual-class,
predecessor/successor — each class may need different temporal semantics per
`instrument_id_for_cik_at_date`), decide the display-grade entity-row
`instrument_id` policy, and decide whether the 1,998 ambiguous-CIK stray rows need
rebinding at all under that model.

## Risks / tradeoffs

- `filing_events` owner-row DELETE is destructive → snapshot first (PR-2 step 1).
- Observations DELETE+refan touches `refresh_insiders_current` rollups — per-
  instrument, verified by the full-pop invariants above.
- DEF 14A path included in PR-1 for completeness but its mislink exposure is not yet
  quantified — quantify with the same production-resolver scan against
  `def14a_ingest_log`/`filing_events` before writing that code path.
