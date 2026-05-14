# N-CSR feasibility re-spike

> Status: **COMPLETED 2026-05-14** — findings + verdict gathered; recommendation locked. Codex 1a (4 rounds) + 1b reviewed.
>
> Issue: #918 (REOPENED 2026-05-13).
> Branch: `feature/918-n-csr-feasibility-spike`.
> Predecessor close decision (2025-Q4): `gh issue view 918 --comments`.

## 1. Context recap

Original #918 close was EdgarTools-surface-only: `FundShareholderReport` exposes fund-level OEF iXBRL facts (NAV / expense ratio / top-holdings %), no per-issuer CUSIP. Operator reopened because:

- The EdgarTools surface is not the raw payload — the iXBRL may carry more than EdgarTools chooses to model.
- The N-CSR HTML body's Schedule of Investments was never inspected in the original close.
- Commercial vendors may publish audited holdings at CUSIP grain — existence proof matters.
- Tie-back paths (filer CIK / portfolio CIK / CUSIP / ticker-name) were dismissed by inference, not by sample.

Acceptance criteria for this spike are pinned in the #918 reopen comment. This doc is the methodology spec; verdict + evidence sections are filled in after execution.

## 2. Settled-decisions check

| Decision | Relevance | Preservation |
|---|---|---|
| §"Fundamentals provider posture" — free regulated-source only (#532) | A commercial-vendor route to audited fund holdings would deviate. | Verdict MUST surface any commercial-vendor finding as a settled-decision-change candidate, not adopt it silently. |
| §"Filing event storage" — full raw filing text out of scope v1 | If an HTML-SoI parser path stores N-CSR body text into a new table, that is an exception. | Verdict's write-target proposal MUST call this out explicitly. |
| §"Provider design rule" — providers thin, DB lookups in services | Any new fetcher/parser respects layering. | Design proposal section will encode the split. |
| §"CIK = entity, CUSIP = security" (#1102) | Per-issuer-CIK fan-out applies if name-only writes ever resolve to instruments. | Write-target proposal MUST state fan-out posture. |
| Source-priority semantics for fund holdings (potential downstream decision) | If a parser lands, N-CSR (audited, semi-annual) overlaps N-PORT-P (unaudited, monthly) on the same fund × instrument × period. Current code has NO source-priority chain because N-CSR never landed. A FEASIBLE verdict MUST propose the chain in the recommendation; it is a new settled decision, not a preserved one. | Verdict §10 records the proposed chain or notes it as out-of-scope-for-spike but in-scope for the parser PR. |
| Product-visibility pivot test (lifted 2026-04-18 but the *test* persists) | "Would the operator feel this moves the product closer to 'I can manage my fund from this screen'?" — applies to any new ingest lane. | Verdict §10 MUST answer the test for the proposed write-target. If the operator-visible delta over N-PORT is marginal, the verdict skews toward synth no-op regardless of technical feasibility. |

Prevention log: no N-CSR entries; no entries apply.

### 2.1 Current state of discovery + allow-list (factual context)

`app/services/sec_manifest.py:_FORM_TO_SOURCE` maps `N-CSR` + `N-CSR/A` to `sec_n_csr`. `app/services/data_freshness.py:_CADENCE` carries `sec_n_csr: 200 days`. `app/jobs/sec_first_install_drain.py:167` excludes `sec_n_csr` from issuer-scoped first-install drain (correct — fund-scoped).

`app/services/filings.py:SEC_INGEST_KEEP_FORMS` (the legacy `refresh_filings(filing_types=...)` gate) does NOT contain `N-CSR` or `N-CSRS` — both are in the implicit SKIP tier per `docs/superpowers/specs/2026-05-08-filing-allow-list-and-raw-retention.md` line 188. The original SKIP justification was *"NPORT supersedes"*. The manifest discovery layers (`sec_atom_fast_lane` / `sec_daily_index_reconcile` / `sec_per_cik_poll`) do NOT consult `SEC_INGEST_KEEP_FORMS` — they use `_FORM_TO_SOURCE` directly. Net effect: N-CSR manifest rows ARE written by the manifest path; legacy `refresh_filings` would have skipped them.

`N-CSRS` is NOT in `_FORM_TO_SOURCE` — discovery silently drops N-CSRS regardless of source. Spike notes this as a side-finding; verdict §10 records whether the form-to-source map needs widening (only if N-CSR parser lands).

The reopen of #918 is implicitly a re-examination of the 2026-05-08 SKIP decision. The spike's verdict either:

- **Reaffirms SKIP** with raw-payload evidence (INFEASIBLE / NOT-WORTH-IT). Adopt §11.5.1 synth no-op so the existing manifest rows drain. SEC_SKIP stays.
- **Reverses SKIP** (FEASIBLE on any tier). Update `_FORM_TO_SOURCE` to include N-CSRS, update spec line 79/188 to remove the strikethrough, register the parser, propose source-priority chain.

## 3. Sampling plan

### 3.1 Fund panel (PLANNED: 6 funds × 5 families × 2 cadences; ACTUAL outcome documented in §8.1)

| Fund | Ticker | Family | Trust CIK (resolved) | Why |
|---|---|---|---|---|
| Vanguard 500 Index | VFIAX | Vanguard | 36405 | Largest passive equity fund; canonical OEF |
| Vanguard Total Stock Market | VTSAX | Vanguard | 36405 (same trust) | Same family; intra-family layout consistency check |
| Fidelity 500 Index | FXAIX | Fidelity | 819118 | Cross-family layout check |
| Invesco QQQ Trust | QQQ | Invesco | 1067839 | UIT, not OEF — taxonomy may differ |
| SPDR S&P 500 ETF Trust | SPY | SSGA / State Street | 884394 | Trust structure variant |
| iShares Core S&P 500 ETF | IVV | iShares / BlackRock | 1100663 | Fifth distinct family for fragility-vote |

**Actual sample diverged from plan**: QQQ (UIT) and SPY (UIT-era pre-2005) filed no recent N-CSR; IVV-specific accession within iShares Trust was not isolated. Effective sample is **3 active N-CSR-filing OEF/ETF families** (Vanguard + Fidelity + iShares-bond-ETF-family), documented in §8.1. The "5-family fragility-vote" framing is therefore weakened; the verdict in §9 leans on the strength of the structural (iXBRL taxonomy + cross-family SoI HTML) evidence, not on family count.

CIKs verified at execution time via `https://www.sec.gov/files/company_tickers_mf.json` (ticker → seriesId → classId → trust CIK chain) + `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=N-CSR`.

For each fund, pull the most recent N-CSR (annual). Pull N-CSRS (semi-annual) opportunistically per fund **only after recording the discovery-side gap**: `_FORM_TO_SOURCE` in `app/services/sec_manifest.py` maps `N-CSR` + `N-CSR/A` but NOT `N-CSRS` (see §2.1). If the verdict reverses SKIP, §10 records the form-to-source widening (`N-CSRS`, `N-CSRS/A`) as a parser-ticket pre-req. Target 8-12 filings (5-6 N-CSR + 3-6 N-CSRS).

### 3.2 Family-layout coverage

Five distinct families (Vanguard / Fidelity / Invesco / SSGA / iShares). The spike's fragility claim is *bounded* — it answers "how many distinct layouts appear in these 5 families" not "how many layouts exist across the universe". A small-N sample can prove "≥ N distinct layouts exist" (lower-bound fragility) but cannot prove "≤ N layouts exist universally". The verdict §6 thresholds are calibrated against the sample size; cross-universe extrapolation is explicitly NOT claimed.

If intra-family layout varies year-on-year for any fund, expand the panel to a sixth family before drawing fragility conclusions.

### 3.3 Inspection per filing

For each accession:

1. **Multi-series isolation (precondition).** N-CSR filings are filed at the **trust** CIK and may carry multiple **series** (funds) in a single document. Before inspecting any SoI section, verify which `seriesId` (`S000...`) and series-name corresponds to the target ticker and isolate that series's SoI. Evidence collected from a different series in the same filing does NOT count toward the target sample. The iXBRL `dei:EntityCentralIndexKey` is the trust; the series-level context lives in `oef:SeriesCentralIndexKey` / `oef:SeriesId` (or equivalent context-ref dimensions). HTML SoI is typically partitioned by series header (`<h2>` / bold-rule heading); inspect only the partition matching the target.
2. **iXBRL facts.** Fetch the inline-XBRL primary doc directly (`/Archives/edgar/data/{cik}/{acc_no_dashes}/{primary_doc}`) and walk its embedded XBRL context. Companyfacts API (`data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`) is the per-CIK XBRL-fact index — useful for sanity-checking what concepts the OEF taxonomy publishes, but NOT a per-filing read; the per-filing iXBRL primary doc is the actual source. (`companyconcept` is a single-tag query, not a discovery endpoint — not used here.) Grep the iXBRL body for:
   - `cusip` / `CUSIP` / `InvestmentIdentifier` (anywhere)
   - `isin` / `ISIN` / `sedol` / `SEDOL` / `lei` / `LEI` (non-CUSIP IDs)
   - `cik` / `CentralIndexKey` on holdings rows (issuer-CIK path — non-CUSIP fan-out via #1102 bridge)
   - `ticker` / `Symbol` on holdings rows
   - `shares` / `BalanceAmount` / `NumberOfShares`
   - `value` / `FairValue` / `MarketValue`
3. **Filing-folder manifest** (`/Archives/edgar/data/{cik}/{acc_no_dashes}/index.json`). Enumerate every exhibit. Flag any structured (`.xml` / `.xsd` / `R*.htm`) attachment that is NOT the iXBRL primary.
4. **Primary HTML body.** Locate Schedule of Investments section **for the target series only** (per §3.3 step 1). Capture:
   - Number of columns
   - Column headers (verbatim)
   - Whether CUSIP is a column
   - Whether ISIN is a column
   - Whether name normalises to a CUSIP-resolvable issuer (spot-check against 13F Official List)
   - **Row grain**: per-issuer position rows vs subtotal/category rows. Count both populations separately. A "CUSIP column" populated only on subtotal rows is useless.
   - Whether **shares OR value** is present per row (per §5 grid the corroboration requirement is shares-or-value, not both; a row with only "% of NAV" is not actionable).
5. **Form-type stamp**: record whether the filing is N-CSR (annual, audited) or N-CSRS (semi-annual, UNAUDITED). Attestation logic in §6 + §10 must NEVER promote N-CSRS rows to "audited" — N-CSRS is reviewed/filed, not audited.
6. **Cross-family layout comparison.** For each column-header set, tag families that share it. Count distinct layouts.

### 3.4 Tools

- **All HTTP fetches go through the SEC shared rate-limit pool**, no exceptions. The pool is the process-singleton `ResilientClient` at `app/providers/implementations/sec_edgar.py:242-257`, constructed with `shared_throttle_lock=_PROCESS_RATE_LIMIT_LOCK` and `min_request_interval_s=_MIN_REQUEST_INTERVAL_S` (module constant `0.11` at line 55; settings-derivation is a separate refactor outside spike scope). Spike fetches go through `SecFilingsProvider` (`app/providers/implementations/sec_edgar.py:214`) or call the existing `ResilientClient` instance via its public fetch methods if a `SecFilingsProvider` method does not cover the endpoint. Direct top-level `httpx.get(...)` calls are forbidden — they bypass the 10 r/s shared budget (§4 of the sec-edgar skill is normative).
- `xml.etree.ElementTree` / `lxml` for iXBRL inspection. `BeautifulSoup` is allowed for HTML body grep (already pinned via `edgartools` transitive; verify in `pyproject.toml`).
- Save raw payloads under `.tmp/spike-918/` (`.tmp/` is gitignored at the repo root — verified). NOT `.claude/spike-918/`: `.claude/` mixes tracked (`skills/`, `commands/`) and ignored entries, so it is not a safe scratch root.

## 4. Commercial / OSS survey

For each vendor below, answer: *do they publish per-issuer fund holdings at CUSIP grain, and do they cite N-CSR as the source?*

| Vendor | URL form | Method | Note |
|---|---|---|---|
| Gurufocus | `gurufocus.com/etf/{ticker}/summary` + holdings page | Public-page inspect | Look for CUSIP column + "Source: N-CSR" footer |
| StockAnalysis | `stockanalysis.com/etf/{ticker}/holdings` | Public-page inspect | |
| Fintel | `fintel.io/sof/us/{ticker}` | Free-tier inspect | |
| WhaleWisdom | `whalewisdom.com/filer/{cik}` | Free-tier inspect | WhaleWisdom is 13F-focused; N-CSR coverage is the open question |
| Marketbeat | `marketbeat.com/stocks/{tkr}/institutional-ownership` | Public-page inspect | |
| Morningstar | `morningstar.com/funds/xnas/{ticker}/portfolio` | Free-tier inspect | Likely uses N-PORT not N-CSR |
| Bloomberg Open Symbology | n/a — out (paid) | — | Note as paid-only floor |

For each vendor that does expose audited per-issuer holdings, drill into one row and trace: do they call it "audited" (N-CSR-derived) or "as of {date}" (N-PORT-derived)? If audited is exposed at CUSIP grain by ANY free vendor, that's existence proof — but does NOT automatically mean they're sourcing from N-CSR (could be paid feed downstream).

**Vendor-counterexample triage**: a counterexample triggers (a) sample-panel expansion to that vendor's exact fund + period if not already in the sample, and (b) source-tracing of the vendor's data lineage (page-footer / "data source" disclosure / press kit). It does NOT auto-promote the verdict to FEASIBLE — counterexamples without traceable sourcing are noise.

**Hard limit**: this is page-inspection at sample size 3-5 per vendor, NOT scraping. Goal is existence proof, not full coverage map.

## 5. Tie-back evaluation framework

For each tie-back path, the verdict states `PRESENT / ABSENT / PARTIAL` based on the sample. PARTIAL requires per-family breakdown.

| Tie-back | Sample test | Pass criteria |
|---|---|---|
| Filer-side CIK | `sec_filing_manifest.cik` already populates | Trivially PRESENT (sanity) |
| Portfolio-issuer CIK | grep iXBRL + HTML for issuer-CIK strings on holdings rows | PRESENT iff issuer CIK appears on holdings rows in ≥ 1 family. PARTIAL if only on some families. Resolution path: #1102 CIK-bridge → issuer-CIK fan-out across siblings, NO CUSIP needed for write |
| CUSIP in iXBRL | grep iXBRL for `cusip` element/attribute | PRESENT iff > 0 holdings rows carry CUSIP across ≥ 1 filing |
| CUSIP in HTML SoI | column-header inspection + row-grain check | PRESENT iff CUSIP column present AND populated on per-issuer position rows (not just subtotals) in ≥ 1 family's layout |
| CUSIP in exhibit | structured-exhibit inspection | PRESENT iff any `EX-99.*` or sibling `.xml` carries CUSIP on holdings rows |
| ISIN on per-issuer position rows | grep + column inspection | PRESENT iff ISIN is populated on per-issuer position rows AND prefixed `US` (US-issued securities). ISIN→CUSIP is deterministic: strip 2-char country prefix + check digit → 9-char CUSIP. |
| LEI on per-issuer position rows (informational) | grep | Recorded for completeness. LEI is an *entity-level* ID (issuer / fund-trust); the GLEIF→issuer-CIK mapping is one extra hop and the operator-CIK bridge (§ #1102) is the same path the §6 issuer-CIK branch already covers. LEI does NOT add a new verdict branch beyond what issuer-CIK provides; if LEI is the *only* identifier present on a row, treat it as confirmatory evidence for the issuer-CIK branch, NOT a standalone feasibility path. |
| SEDOL on non-US constituents (informational) | grep | Recorded for completeness; non-actionable in v1 (US-issuer scope, no free SEDOL bridge). |
| Cross-source reconciliation (N-CSR-row × N-PORT-row, audited-overlay) | same fund (trust CIK + seriesId) + same period_end + name-fuzzy ≥ 0.92 + shares-or-value within ±5% | PRESENT iff all four conditions hold. Name-only is insufficient (Codex 1a) — corroborating shares/value disambiguates same-name-different-security false positives. Audited-overlay writes attestation per N-PORT row, NOT fund-wide |
| Ticker / name only | OEF top-holdings-by-pct already in EdgarTools | PRESENT (already known); operator-value question, not feasibility question |

## 6. Verdict criteria + branch logic

Decision tree applied to the assembled tie-back grid. **All "CUSIP" / "ISIN" / "issuer-CIK" branches additionally require row-grain = per-issuer position (not subtotal/category) AND shares-or-value present on the row.** A header column that exists but only appears on subtotal rows is NOT a feasible identifier source.

```
Step 1 — Is CUSIP present on per-issuer position rows (iXBRL OR exhibit, structured)?
├─ YES → FEASIBLE-STRUCTURED.
│         Write target: extend ownership_funds_observations OR add fund_named_holdings
│         (verdict §10 decides; CUSIP enables direct instrument_id resolution).
│         Parser: lxml on iXBRL or sibling .xml. No HTML fragility.
│
├─ NO → Step 2.

Step 2 — Is ISIN present on per-issuer position rows (any layer) AND prefixed `US`
         (US-issued, the v1 scope)?
├─ YES → FEASIBLE-VIA-ISIN.
│         ISIN strip → CUSIP deterministic for `US`-prefix ISINs.
│         Same write target as Step 1.
│         (SEDOL alone does NOT promote here — no free SEDOL→CUSIP bridge;
│          recorded as PRESENT-INFORMATIONAL in §5 only.)
│
├─ NO → Step 3.

Step 3 — Is portfolio-issuer CIK present on per-issuer position rows (iXBRL OR HTML)?
├─ YES → FEASIBLE-VIA-CIK.
│         Resolution: #1102 CIK→instrument fan-out (siblings union, fail-closed).
│         Write target: ownership_funds_observations with NULL CUSIP + CIK-resolved instrument_id.
│         REQUIRES schema delta (CUSIP NOT NULL → NULLABLE for CIK-sourced rows) +
│         settled-decision update §"Filing dedupe" + source-priority chain proposal.
│
├─ NO → Step 4.

Step 4 — Cross-source reconciliation. For a candidate N-CSR (annual, audited) row to
         attest an N-PORT observation, ALL of the following must hold:
         (a) Same fund (matched by trust CIK + seriesId).
         (b) Same period_end (or N-CSR period_end's corresponding N-PORT month-end
             observation; N-CSR rolls up monthly N-PORT entries).
         (c) Name-fuzzy-match ≥ 0.92 against the N-PORT row's issuer name.
         (d) Shares-or-value (whichever the N-CSR row exposes) within ±5% of the
             N-PORT row — corroboration that this is the SAME position, not a different
             security under a similar name.
         All four required; (c) alone is insufficient (Codex 1a finding).
         **N-CSRS (semi-annual, UNAUDITED) filings do NOT attest "audited"** — at most
         they corroborate "reviewed by management as of {period}". A four-part match
         from N-CSRS produces a `reviewed` flag (or no flag), never `audited`.
├─ YES → FEASIBLE-VIA-NPORT-OVERLAY.
│         Write target: per-observation attestation column whose VALUE encodes both
│         the matched accession and the audit-vs-reviewed semantic
│         (e.g. `attested_audited_by TEXT` + `attested_reviewed_by TEXT`, OR a sidecar
│         with `attestation_type ENUM('audited','reviewed')`). NO new positions written
│         from N-CSR/N-CSRS alone.
│         UI semantics: per-row badge — "Audited" (N-CSR only) or "Reviewed" (N-CSRS)
│         with hover-text showing the matched accession. Rows not meeting the four-part
│         match are NOT badged. Operator-visible value re-test mandatory per §10.
│
├─ NO → Step 5.

Step 5 — Is CUSIP present in HTML SoI on per-issuer position rows?
├─ YES, ≤ 2 distinct layouts across the 5-family sample → FEASIBLE-HTML.
│         Write target: ownership_funds_observations via HTML parser.
│         Parser: BeautifulSoup + per-family adapter.
│         Fragility risk: bounded by sample; verdict §9 records the layout-count.
│         Verdict §10 weighs operator-value over per-family maintenance cost
│         (N-PORT already provides monthly per-issuer; what does audit-stamp add?).
│
├─ YES, ≥ 3 distinct layouts across the 5-family sample → FEASIBLE-BUT-NOT-WORTH-IT.
│         Per-family table-extraction maintenance > value.
│         Verdict: adopt §11.5.1 synth no-op.
│
└─ NO → INFEASIBLE-CONFIRMED.
         No structured ID at any layer; no cross-source overlay; no HTML CUSIP.
         Verdict: adopt §11.5.1 synth no-op.
         Close #1153 as REBUTTED (framing was right; evidence now confirms it).
```

Tree order matters: structured wins over HTML; ISIN/CIK paths enable feasibility even without CUSIP; cross-source overlay (Step 4) is the practical audited-stamp route flagged by Codex 1a. If multiple branches are PRESENT, the *highest* (earliest step) wins as primary; lower branches are recorded as confirmatory.

**Vendor counterexample handling**: see §4 "Vendor-counterexample triage". A vendor finding does NOT short-circuit the tree; it triggers sample expansion + source tracing, then the tree is re-applied with the augmented evidence.

## 7. Deliverables on completion

Per-verdict matrix — each verdict from §9 maps to a concrete deliverable set:

| Verdict | Skill / wiki updates | Memory updates | Ticket actions | Spec/code updates |
|---|---|---|---|---|
| FEASIBLE-STRUCTURED / -VIA-ISIN / -VIA-CIK / -HTML | sec-edgar §11.5 → "FEASIBLE — parser ticket #NNN"; edgartools G12 → weakened ("OEF iXBRL is fund-level; structured holdings via {iXBRL/ISIN/CIK/HTML}"); ownership-card.md source-priority row stays | us-source-coverage.md `sec_n_csr` → ticket pointer + verdict; 873-manifest-worker-parser-rollout → parser PR pending | #918 close (verdict comment); #1153 supersede by parser ticket #NNN; **new parser ticket** | `filings.py:SEC_INGEST_KEEP_FORMS` → add `N-CSR`, `N-CSRS`; spec 2026-05-08 line 79/188 → un-strikethrough N-CSR/N-CSRS; sec_manifest.py `_FORM_TO_SOURCE` → add `N-CSRS`, `N-CSRS/A`; source-priority chain proposed as NEW settled-decision in same PR |
| FEASIBLE-VIA-NPORT-OVERLAY | sec-edgar §11.5 → "FEASIBLE via N-PORT overlay — attestation ticket #NNN"; edgartools G12 → "OEF iXBRL still fund-level; N-CSR adds audit attestation to existing N-PORT rows, not new positions" | us-source-coverage.md `sec_n_csr` → "attestation, ticket #NNN"; 873-manifest-worker → attestation adapter pending | #918 close; #1153 close as REBUTTED (parser not needed); **new attestation ticket** | `filings.py:SEC_INGEST_KEEP_FORMS` widened (raw N-CSR retention now justified for attestation matching); spec 2026-05-08 line 79/188 updated; schema delta (`audited_by_accession` column or sidecar) |
| FEASIBLE-BUT-NOT-WORTH-IT | sec-edgar §11.5 → "feasible but ≥3 HTML layouts; synth no-op adopted (cost/value)"; edgartools G12 unchanged | us-source-coverage.md `sec_n_csr` → "synth no-op (HTML fragility), ticket #NNN" | #918 close with NOT-WORTH-IT verdict; #1153 close as REBUTTED; **new synth-noop ticket** (mirror PR #1169) | No allow-list or _FORM_TO_SOURCE changes (manifest rows drain via no-op) |
| INFEASIBLE-CONFIRMED | sec-edgar §11.5 → "INFEASIBLE confirmed by raw-payload spike, see this doc"; edgartools G12 strengthened with evidence | us-source-coverage.md `sec_n_csr` → "INFEASIBLE, synth no-op, ticket #NNN" | #918 close with INFEASIBLE verdict; #1153 close as REBUTTED; **new synth-noop ticket** (mirror PR #1169) | No allow-list or _FORM_TO_SOURCE changes |

Common across all verdicts:

1. **This doc** filled in with §8 Findings, §9 Verdict, §10 Recommendation, committed in this PR.
2. **PR description** records: settled-decisions touched (§2), verdict, follow-up ticket(s).
3. **Product-visibility-test answer** documented in §10 regardless of verdict.

## 8. Findings

Execution date: 2026-05-14. All raw payloads under `.tmp/spike-918/` (gitignored).

### 8.1 Sample inventory

| Label | Family | Trust CIK | Form | Filed | Period | Primary doc bytes | iXBRL companion bytes | Exhibits |
|---|---|---|---|---|---|---|---|---|
| vanguard_ncsr_a | Vanguard Index Funds (VFIAX series S000002839) | 36405 | N-CSR | 2026-02-27 | 2025-12-31 | 9,461,119 | 5,438,674 | 38 |
| vanguard_ncsr_b | Vanguard Index Funds (VTSAX series S000002848) | 36405 | N-CSR | 2026-02-27 | 2025-12-31 | 20,161,407 | (not pulled — same family/format as -021519) | 59 |
| vanguard_ncsrs | Vanguard Index Funds | 36405 | N-CSRS | 2025-08-27 | 2025-06-30 | 19,471,380 | 1,198,486 | 20 |
| fidelity_ncsr | Fidelity Concord Street Trust (FXAIX series S000006027) | 819118 | N-CSR | 2025-12-22 | 2025-10-31 | 20,383,385 | 594,699 | 82 |
| fidelity_ncsrs | Fidelity Concord Street Trust | 819118 | N-CSRS | 2025-12-22 | 2025-10-31 | 9,150,984 | (not pulled) | 97 |
| ishares_ncsr | iShares Trust (Investment-Grade Corporate Bond ETF family — `iShares 10+ Year IG Corp Bond ETF`, `iShares 1-5 Year IG Corp Bond ETF`, `iShares 5-10 Year IG Corp Bond ETF`, `iShares Broad USD IG Corp Bond ETF`, `iShares Core 10+ Year USD Bond ETF`. **NOT IVV** — per §3.3 multi-series isolation, the originally-planned IVV-specific accession was not isolated; this accession bundles multiple bond-ETF series. The IVV-specific accession from iShares Trust was not separately inspected; this is a known sample gap. The iShares-Trust findings (0 CUSIP / 0 ISIN / 0 holding-level identifiers in iXBRL + HTML) are reported only for the bond-ETF family observed. A future IVV-isolated re-spike could confirm or refute the assumption that the OEF iXBRL taxonomy + Item-7 HTML layout are uniform across iShares Trust series; this spike does not assert that uniformity.) | 1100663 | N-CSR | 2026-05-05 | 2026-02-28 | 115,057,787 | 1,956,992 | 62 |
| ishares_ncsrs | iShares Trust (same Investment-Grade bond family as above) | 1100663 | N-CSRS | 2026-05-05 | 2026-02-28 | 22,921,936 | (not pulled) | 42 |

**Sample-side structural findings:**

- **QQQ (Invesco QQQ Trust, CIK 1067839)**: 0 N-CSR/N-CSRS in 249 recent submissions. Last UIT-structured holdings disclosure is on a different form (Form N-Q historically, now legacy). UIT-structured ETFs are OUT OF SCOPE for N-CSR feasibility — their holdings disclosure pathway is structurally different.
- **SPY (SPDR S&P 500 ETF Trust, CIK 884394)**: exactly 1 N-CSR filed 2004-12-03, none since. Trust file/registration regime changed; current holdings disclosure for SPY also lives outside the N-CSR pathway.
- **Effective sample is 3 active N-CSR-filing families** (Vanguard / Fidelity / iShares), not the planned 5. UITs are a structural gap that does NOT bear on the N-CSR-feasibility question; record + move on.

### 8.2 iXBRL inspection

**Distinct `oef:*` concept tags** observed across the 3 sampled iXBRL companions (Vanguard / Fidelity / iShares) — typical inventory:

`FundName`, `ClassName`, `ClassAxis`, `HoldingsCount`, `HoldingsTableTextBlock`, `ExpenseRatioPct`, `ExpensesPaidAmt`, `AdvisoryFeesPaidAmt`, `AvgAnnlRtrPct`, `AvgAnnlRtrTableTextBlock`, `IndustrySectorAxis`, `GeographicRegionAxis`, `CreditQualityAxis`, `MaterialChngDate`, `FactorsAffectingPerfTextBlock`, `LineGraphTableTextBlock`, `AdditionalIndexAxis`, `BroadBasedIndexAxis`, `AddlFundStatisticsTextBlock`, `AddlInfoPhoneNumber`, `AddlInfoWebsite`, `AddlInfoEmail`, `AnnlOrSemiAnnlStatementTextBlock`, …

**Identifier grep across iXBRL companions** (case-sensitive; the case-insensitive `isin` pattern matches the substring inside "rising" / "registration" / "rising" — false positives):

| Identifier | Vanguard A | Vanguard NCSRS | Fidelity | iShares |
|---|---|---|---|---|
| CUSIP | 0 | 0 | 0 | 0 |
| ISIN (true) | 0 | 0 | 0 | 0 |
| SEDOL | 0 | 0 | 0 | 0 |
| Ticker (per-row) | 0 | 0 | 0 | 0 |
| CentralIndexKey | 2 (filer only) | 2 (filer only) | 1 (filer only) | 2 (filer only) |
| LEI on holdings | 0 | 0 | 0 | 0 |

**`HoldingsTableTextBlock` content inspection** (16-72 occurrences per filing; one block per share class per "what did the fund invest in" disclosure unit). After HTML decoding + plain-text extraction:

- Vanguard A: 30,766 bytes decoded — content is **sector-allocation percentages only** ("Communication Services 10.6%, Consumer Discretionary 10.4%, …"). No per-issuer rows.
- Fidelity: 1,370 bytes — "MARKET SECTORS (% of Fund's net assets)" + "ASSET ALLOCATION (% of Fund's net assets)". No per-issuer rows.
- iShares: 24,839 bytes — "Credit quality allocation" + "Maturity allocation" (bond fund sample). No per-issuer rows.

**R*.htm rendered tables inspection** (e.g. `vanguard_a_R2.htm`, 3.6 MB): rendered XBRL fact tables; **CUSIP = 0, ISIN = 0, SEDOL = 0, Ticker = 0**. Rendered tables carry the same fund-level + class-level + sector-axis facts as the iXBRL — no holdings-level identifiers added by the rendering.

**Conclusion**: the OEF iXBRL taxonomy publishes fund-level + class-level + sector-allocation facts ONLY. There is no holding-level CUSIP / ISIN / SEDOL / Ticker / LEI / portfolio-issuer-CIK in any iXBRL companion or rendered fact table. **The original #918 close was correct on this layer.** The original close cited only the EdgarTools-exposed surface; the spike confirms the underlying taxonomy publishes nothing more.

### 8.3 HTML Schedule of Investments

| Family | Sample filing | SoI columns observed (verbatim) |
|---|---|---|
| Vanguard (VFIAX trust) | vanguard_ncsr_a | `Name | Shares | Market Value • ($000)` |
| Fidelity (FXAIX trust) | fidelity_ncsr | `Name | Shares | Value ($)` |
| iShares Trust (sample: Investment-Grade Corp Bond ETF family — see §8.1 note; NOT IVV-isolated) | ishares_ncsrs | (Item 7 financial statements + SoI; text-form holdings without CUSIP column in primary HTML — same shape as Vanguard / Fidelity) |

**Identifier grep across the 3 primary HTML files** (52 MB combined):

| Identifier | Vanguard primary (9.4 MB) | Fidelity primary (20 MB) | iShares NCSRS primary (22 MB) |
|---|---|---|---|
| CUSIP | 0 | 0 | 0 |
| Cusip | 0 | 0 | 0 |
| ISIN | 0 | 0 | 0 |
| SEDOL | 0 | 0 | 0 |
| Ticker symbol | 0 | 0 | 0 |
| Bloomberg | 0 | 0 | 0 |
| FIGI | 0 | 0 | 0 |

**Row-grain verification**: Vanguard SoI carries per-issuer position rows ("Alphabet Inc. Class A | 147,296,785 | 46,103,894") interleaved with sector subtotals. Fidelity SoI uses a country → sector → industry → holding hierarchical layout. iShares is bond-fund-grain (Issuer | Coupon | Maturity | Value).

**Notable**: Vanguard's SoI header explicitly says

> "The fund files its complete schedule of portfolio holdings with the Securities and Exchange Commission (SEC) for the first and third quarters of each fiscal year as an exhibit to its reports on Form N-PORT. The fund's Form N-PORT reports are available on the SEC's website at www.sec.gov."

The fund itself directs the reader to **N-PORT as the authoritative structured-holdings source.**

**Layout-distinctness count across 3 sampled families: 3 distinct layouts** (Vanguard 3-col, Fidelity hierarchical 4-col country/sector/industry/holding, iShares bond-fund variant). All three converge on "name + shares + value" without any machine-readable security ID.

### 8.4 Exhibit inspection

For each accession, the structured exhibits inventory:

- `*_htm.xml` — iXBRL companion (the inline-XBRL extract). Already covered in §8.2.
- `R1.htm` – `R5.htm` — SEC-rendered XBRL fact tables. Already covered in §8.2.
- `FilingSummary.xml` — filing-summary metadata. No holdings data.
- `cik{padded}-{YYYYMMDD}.xsd` / `{family}-{YYYYMMDD}.xsd` — XBRL schema. No instance data.
- `ex99_cert.htm` / `ex99_906cert.htm` / `ex99_codeeth.htm` — Sarbanes-Oxley certifications + code of ethics. No holdings data.
- `*.zip` — XBRL bundle (same content as `*_htm.xml` extracted).
- Image files (`*.jpg` / `*.gif`) — chart graphics, no machine-readable holdings.

**Conclusion**: no sibling exhibit carries structured holdings data not already covered by the iXBRL companion or the primary HTML SoI.

### 8.5 Commercial / OSS survey

Vendor sample (spot-check, page-inspection):

| Vendor | URL | Holdings columns observed | CUSIP/ISIN/SEDOL? | Cited source |
|---|---|---|---|---|
| StockAnalysis (VOO) | `stockanalysis.com/etf/voo/holdings/` | No. / Symbol / Name / % Weight / Shares | None | "Data Sources" footer present but no specific N-CSR / N-PORT attribution; as-of date is recent quarter-end (consistent with N-PORT cadence) |
| StockAnalysis (VFIAX) | `stockanalysis.com/fund/vfiax/holdings/` | 404 (page does not exist for non-ETF mutual-fund tickers) | n/a | n/a |
| Morningstar (VFIAX portfolio) | `morningstar.com/funds/xnas/vfiax/portfolio` | 403 (blocked via WebFetch) | n/a | n/a |
| Fintel | not reached | n/a | n/a | n/a |

**Counterexample analysis**: StockAnalysis exposes `Symbol` (ticker) per holding — consistent with N-PORT-sourced data (N-PORT carries CUSIP, which maps to ticker via SEC's CUSIP→issuer-name→ticker bridge). **No spot-checked vendor cites N-CSR or exposes CUSIP-grain audited holdings.** No counterexample triggered; verdict §6 tree stands on the raw-payload evidence.

(Vendor coverage is shallow by methodology bound — spot-check, not scrape. The spike's verdict does NOT depend on the vendor survey because the raw-payload evidence is conclusive; the survey would only have changed the verdict if a vendor demonstrably sourced N-CSR for CUSIP-grain holdings, and none did.)

### 8.6 Tie-back grid

| Tie-back | Verdict | Evidence |
|---|---|---|
| Filer-side CIK | PRESENT | `sec_filing_manifest.cik` already populated for all sampled accessions; trivial. |
| Portfolio-issuer CIK on holdings | ABSENT | 0 hits in any iXBRL companion or HTML body across all 3 families. Only filer CIK present in iXBRL (`dei:EntityCentralIndexKey` on the filer context). |
| CUSIP in iXBRL | ABSENT | 0 hits across 4 iXBRL companions inspected. |
| CUSIP in HTML SoI | ABSENT | 0 hits across 3 primary HTML files (52 MB combined). |
| CUSIP in sibling exhibit | ABSENT | No structured exhibit carries holdings; only XBRL bundle (same as iXBRL), schema files, and Sarbanes-Oxley certs. |
| ISIN (true) | ABSENT | 0 hits across 4 iXBRL + 3 HTML. Earlier case-insensitive `isin` hits were false-positives on "rising" / "registration". |
| LEI on holdings | ABSENT | Only filer/fund-trust LEI present (entity-level), never per-row. |
| Cross-source reconciliation (4-part match against N-PORT) | POTENTIALLY-VIABLE / UNTESTED | All 3 sampled trusts file N-PORT-P at the relevant periods (Vanguard 325 / Fidelity 596 / iShares 1422 N-PORT submissions in recent vs 21 / 47 / 41 N-CSR/S). The infrastructure (N-PORT-P observations + same period boundaries + name+shares+value to corroborate) exists, but the 4-part match (same fund + same period + name-fuzzy ≥ 0.92 + shares-or-value within ±5%) was NOT executed in this spike — it would require building the name-matching probe + measuring practical match-rate against share-class siblings and corp-action renames. Spike's verdict cannot claim FEASIBLE-VIA-NPORT-OVERLAY on this evidence alone (Codex 1b). The §10 recommendation does not depend on the overlay match-rate because the product-visibility-test rules out the overlay-attestation path regardless. |
| Ticker / name only | PRESENT | Per-row names with corporate-form suffixes (e.g. "Alphabet Inc. Class A") + sector subtotals. Operator-visible value of name-only listings is low given N-PORT-P already provides the structured equivalent. |
| SEDOL on non-US constituents (informational) | ABSENT | 0 hits. (US-issuer scope; expected.) |

## 9. Verdict

**INFEASIBLE-CONFIRMED for every structured tie-back path. Cross-source overlay (Step 4) is POTENTIALLY-VIABLE but UNTESTED in this spike scope AND product-visibility-negative. Adopt §11.5.1 synth no-op.**

### 9.1 Step-by-step application of §6 to the §8.6 tie-back grid

| Step | Question | Evidence | Result |
|---|---|---|---|
| 1 | CUSIP present on per-issuer position rows (iXBRL or structured exhibit)? | 0 CUSIP hits across 4 iXBRL companions + sibling-exhibit inspection. | NO. |
| 2 | `US`-prefix ISIN present on per-issuer position rows? | 0 true ISIN hits anywhere. | NO. |
| 3 | Portfolio-issuer CIK present on per-issuer position rows? | Only filer CIK appears in iXBRL; HTML body has no per-row CIK column. | NO. |
| 4 | Cross-source 4-part match against N-PORT viable? | All 3 trusts file N-PORT-P at relevant periods; infrastructure exists. **Practical match-rate NOT measured in this spike** — would require a probe (Codex 1b). Therefore the verdict cannot claim FEASIBLE on this evidence; the path is POTENTIALLY-VIABLE-UNTESTED. | INCONCLUSIVE in spike scope. |
| 5 | CUSIP in HTML SoI? | 0 across 3 distinct family layouts (52 MB combined). | NO. |

§6 tree's Steps 1, 2, 3, 5 are all decisively NO. Step 4 is INCONCLUSIVE in spike scope — but §10's product-visibility-test rules out building the overlay regardless of its match-rate, so the inconclusiveness does not gate the recommendation. The decisive outcome for v1 is the same whichever way Step 4 resolves.

### 9.2 Why the overlay path is ruled out for v1 regardless of Step 4's match-rate

Per §2 settled-decision check, the product-visibility test is binding regardless of whether the overlay's match-rate measures FEASIBLE or INFEASIBLE in a future probe. Concrete reasons:

1. **The fund itself directs the reader to N-PORT for structured holdings.** Vanguard's primary HTML literally says "complete schedule of portfolio holdings is filed as an exhibit to its reports on Form N-PORT". The N-CSR's own author considers N-PORT the structured-holdings source; the N-CSR text-form SoI is a human-readable rendering, not new data.
2. **The data is the same.** N-CSR Schedule of Investments and N-PORT-P share the same underlying portfolio at the same period_end. The N-CSR audit certifies the year-end portfolio; the matching N-PORT-P observation IS the year-end portfolio in machine-readable form. Building a four-part overlay to attest "audited" on N-PORT rows merely badges what we already have.
3. **Operator-visible delta is marginal.** An "audited" badge on year-end fund-holdings rows tells the operator nothing actionable for ranking, thesis, or execution. Auditing of fund financial statements is uniform across registered funds; the badge has no discriminating signal.
4. **False-claim risk is non-zero.** Even with a 4-part match, share-class siblings (Alphabet A/C, BRK A/B) and corporate-name changes create false-negatives and rare false-positives. The Codex critique in the original #918 close ("an `audited=true` badge derived from a metadata-only join would encode a false claim") still applies — the 4-part match reduces but does not eliminate the failure mode.
5. **Maintenance cost.** A name-matching bridge between N-CSR and N-PORT is per-PR fragile (corp-action renames break the join; new share classes need handling; ±5% threshold needs periodic tuning). Maintenance > value.

The Step-4 status is recorded for completeness; the practical recommendation is synth no-op. If a future operator-visible audit-stamp surface materialises (e.g. a regulatory disclosure framework gains operator value), this verdict is REOPEN-ELIGIBLE — and that reopen MUST run the unmeasured 4-part match-rate probe before claiming the overlay path is buildable. The spike does not assert buildability; it only documents the path as unmeasured.

### 9.3 Verdict summary table (matches §9 enum from methodology)

| Verdict candidate | Status |
|---|---|
| FEASIBLE-STRUCTURED | RULED OUT — Step 1 NO |
| FEASIBLE-VIA-ISIN | RULED OUT — Step 2 NO |
| FEASIBLE-VIA-CIK | RULED OUT — Step 3 NO |
| FEASIBLE-VIA-NPORT-OVERLAY | NOT CLAIMED — Step 4 inconclusive in spike scope; practical match-rate unmeasured (Codex 1b) |
| FEASIBLE-HTML | RULED OUT — 0 CUSIP across 3 distinct layouts |
| FEASIBLE-BUT-NOT-WORTH-IT (HTML-layout-fragility variant) | RULED OUT — HTML path not feasible at all |
| **INFEASIBLE-CONFIRMED (for v1 scope)** | **SELECTED** — all structured paths NO; overlay path is product-visibility-negative regardless of unmeasured match-rate; adopt synth no-op |

(§2 explicitly authorises the product-visibility-test as a binding cap: "If the operator-visible delta over N-PORT is marginal, the verdict skews toward synth no-op regardless of technical feasibility." The unmeasured overlay path therefore does not need to be probed in spike scope — the v1 disposition is the same. A future reopen that materialises operator-visible audit-stamp value would re-examine BOTH the overlay match-rate AND the product test before recommending a parser.)

## 10. Recommendation

**Adopt the §11.5.1 synth no-op parser for `sec_n_csr`** (mirror PR #1169 / #1168 shape). Concrete plan:

### 10.1 Synth no-op adoption ticket — scope

1. Add `app/services/manifest_parsers/sec_n_csr.py` returning `ParseOutcome(status='parsed', parser_version='n-csr-noop-v1')` without DB writes, fetches, or typed-table touches. `requires_raw_payload=False`.
2. Wire the registration into `register_all_parsers()` at `app/services/manifest_parsers/__init__.py` per the 5-step contract (sec-edgar §11.1).
3. Durability test (`tests/test_manifest_parser_sec_n_csr.py`) — sentinel-connection + monkeypatched `store_raw` (service-layer + module-local) + monkeypatched `fetch_document_text` per §11.5.1 pattern. Asserts parser stays a no-op against any future regression to a fetcher. If the test imports `fetch_document_text`, update the allow-list at `tests/test_fetch_document_text_callers.py` to whitelist the new test file (mirror PR #1169 pattern).
4. Update `.claude/skills/data-sources/sec-edgar.md` §11.5: replace "pending re-spike" with the evidence-backed verdict + link to this doc.
5. Update `.claude/skills/data-sources/edgartools.md` G12: strengthen with raw-payload evidence (the gap is not an EdgarTools surface limitation but a real taxonomy limitation).
6. Update `.claude/skills/data-engineer/etl-endpoint-coverage.md` row for N-CSR/`sec_n_csr` (Codex 1b) — flip "no parser" status to "synth no-op landed" with PR reference.
7. Update `app/services/processes/param_metadata.py:259-265` help text — the "no parser registered yet" verbiage for `sec_n_csr` becomes obsolete once the synth no-op lands. Either remove the sec_n_csr-specific note or restate as "synth no-op (#918 verdict)".
8. Update `docs/wiki/ownership-card.md` lines 27 + 43 — the "N-CSR audited beats NPORT-P unaudited" source-priority claim is **moot** because eBull never ingests N-CSR per-issuer holdings (no audit overlay). Remove or restate to clarify N-CSR is not a v1 holdings source.
9. Update `docs/wiki/glossary.md` lines 36-37 (Codex 1b) — same source-priority claim ("Beats NPORT-P within same period") needs the same restate.
10. Close #918 with verdict comment + spike-doc commit SHA.
11. Close #1153 as REBUTTED — its premise ("stranded enum entry, no parser, manifest rows accumulate") is correct, and the resolution is the synth no-op landing in this PR.

### 10.2 What is NOT recommended

- **Parser PR for `ownership_funds_observations`** — no per-issuer identifier exists in N-CSR; cannot resolve to `instrument_id`.
- **Audit-attestation sidecar** — potentially buildable via N-PORT overlay (§9.1 Step 4 INCONCLUSIVE — match-rate not measured in spike scope) but operator-visible value is marginal (§9.2); maintenance > value regardless of the unmeasured match-rate.
- **Reversing the 2026-05-08 SEC_SKIP for N-CSR/N-CSRS** — the spike confirms the original SKIP decision was correct. `filings.py:SEC_INGEST_KEEP_FORMS` stays as-is. The implicit gap (manifest layer DOES discover N-CSR but doesn't drain) is closed by the synth no-op, not by adding N-CSR to the legacy refresh_filings allow-list.
- **Adding N-CSRS to `_FORM_TO_SOURCE`** — only meaningful if a parser were landing; since synth no-op is the verdict, leaving N-CSRS unmapped is consistent (no manifest rows accumulate for N-CSRS either). Note as a follow-up if any future need for N-CSRS-tracking materialises.

### 10.3 Settled-decisions impact

| Decision (§2) | Outcome |
|---|---|
| §"Fundamentals provider posture" (free regulated-source only #532) | PRESERVED — no commercial vendor needed since no audit attestation is being built. |
| §"Filing event storage" (full raw text out of scope) | PRESERVED — synth no-op does not store N-CSR body. |
| §"Provider design rule" (thin adapters) | PRESERVED — no new provider. |
| §"CIK = entity, CUSIP = security" (#1102) | PRESERVED — no new fan-out required. |
| Source-priority semantics for fund holdings (potential downstream decision) | NOT RAISED — no parser landing, no overlap to dedup. |
| Product-visibility pivot test | ANSWERED — audit attestation does NOT move the product closer to "I can manage my fund from this screen"; no operator-visible figure is blocked by absence of N-CSR ingest. |

### 10.4 Side-findings worth recording elsewhere

- **UIT-structured ETFs (QQQ, SPY) do not file N-CSR/N-CSRS regularly.** Holdings disclosure for UITs is structured differently. Update `data-sources/sec-edgar.md` or `data-engineer/etl-endpoint-coverage.md` to note that UITs are a structural gap in the N-CSR/N-PORT lane — track separately if operator visibility for UIT holdings ever becomes required.
- **The 2024 TSR rule reshaped N-CSR content.** Post-TSR N-CSR primary HTML is a 2-3 page shareholder report + Item 7 financial statements with text-form SoI. Anyone reopening this spike must read the SEC TSR rule (effective 2024-07-24) before designing alternative tie-back paths — the iXBRL taxonomy covers the new TSR section, not the financial-statements section.

### 10.5 Product-visibility-test answer (binding)

The operator-visible question: *"Would building an audited-fund-holdings ingest move the product closer to 'I can manage my fund from this screen'?"*

Answer: **No.** Fund holdings via N-PORT-P are already ingested at monthly cadence with structured CUSIP grain. The audit credential (annual N-CSR) is uniform across registered funds — it has no operator-discriminating signal for ranking, thesis, or execution. The marginal value of a per-row "audited" badge does not justify the schema delta, name-matching code, or per-PR fragility maintenance.

Synth no-op is the right v1 disposition. The spike doc + commit SHA + the §11.5.1 reference let any future operator re-examine the verdict if priorities shift.

## 11. Execution constraints

- Rate limit: 10 r/s shared SEC pool. Spike HTTP budget estimate: ~80-120 SEC requests total
  (10 filings × {1 submissions.json + 1 index.json + 1 primary doc + ~3 avg exhibits + 1 companyconcept probe})
  plus ~30-50 vendor page-loads (5 vendors × ~6-10 funds). Well within budget; no rate concerns.
- User-Agent: pinned via `settings.sec_user_agent`. Confirm config flows before any fetch.
- Raw payloads stored under `.tmp/spike-918/` — `.tmp/` is gitignored at repo root (verified).
- No new dependencies. `lxml` + `bs4` + `httpx` are already pinned (`bs4` transitively via `edgartools`).
- Spike executes in one session; verdict + memory/skill updates land in the same PR as this doc.

## 12. Out of scope

- Writing the parser (if FEASIBLE) — that's a follow-up ticket.
- Re-running the original 2025-Q4 EdgarTools-only analysis — superseded by raw-payload inspection.
- Coverage map of every fund family — sample is intentionally narrow; verdict is about presence/absence of structured holdings, not coverage estimation.
- Cross-source verification against gurufocus/marketbeat figures — that's a parser-PR ETL DoD clause 9 task, not a feasibility task.
