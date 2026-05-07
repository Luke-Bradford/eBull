# SEC filing allow-list + raw-payload retention policy

Author: claude (autonomous)
Date: 2026-05-08
Status: Draft (post-Codex round 1)

## Problem

Operator first-install bootstrap audit (2026-05-07) surfaced two
shapes of waste in the SEC ingest path:

1. **`filings_history_seed` (S5) writes 32% non-consumed rows**:
   816,597 ``filing_events`` rows after first-install, of which
   265,956 (32%) are forms no parser consumes today (424B
   prospectus supplements, 144 sale notices, FWP marketing,
   CORRESP, etc.). Plus a long tail of ~100 other form types
   we never look at.
2. **Raw-payload persistence is over-broad**: ``filing_raw_documents``
   holds ~14.7 GB uncompressed (1.5 GB on disk via TOAST). DEF 14A
   bodies alone are 11 GB for 16,588 filings (avg 713 KB each), but
   ``def14a_beneficial_holdings`` (the parsed output we actually
   query) is 11 MB — a **1000:1 raw-vs-extracted ratio**.

The settled-decision principle is "raw API payloads persisted before
any parse / normalise step" (prevention-log entry). That justifies
SOME raw retention; current implementation persists too much.

## Goal

Refine SEC ingest so:

- We pull and store only what offers signal value to ranking, thesis,
  or coverage classification.
- "No-benefit" forms (defined as: no parser, no classifier use, no
  documented LLM thesis-relevance) are skipped entirely.
- Raw-payload retention is justified per-form: kept when re-parse-
  from-SEC is unsafe (parser-bug forensics on a takedown-prone
  source), dropped when SEC archive guarantees re-fetchability.
- The decision is documented per-form so adding a new ingest later
  inherits the framework.

## Form-type allow-list — by signal value

For each SEC form type seen on the dev DB ingest, the table below
records: what the form contains, what an LLM (or ranking signal)
could extract from it, current parser status, and whether to keep
metadata in `filing_events` / persist raw / skip entirely.

| Form | Contents | LLM / ranking signal | Parser today | Raw policy | Decision |
|---|---|---|---|---|---|
| **10-K, 10-K/A** | Annual report + audited financials + risk factors + business description | High — `business_summary`, fundamentals, risk-factor text mining | Yes (`business_summary`, fundamentals via XBRL) | Persist body — 10-K is foundational, re-parse cost is high | **KEEP + parse + raw** |
| **10-Q, 10-Q/A** | Quarterly unaudited financials + MD&A | High — fundamentals, MD&A text | Yes (fundamentals via XBRL) | Persist on parse-fail only — XBRL Company Facts is the canonical re-fetch | **KEEP + parse + raw-on-fail** |
| **8-K, 8-K/A** | Material events (Item 1.01 agreement, 2.01 M&A, 4.02 non-reliance, 5.02 officer change, etc.) | High — event-driven thesis, material change detection | Yes (`eight_k_events`) | Persist on parse-fail only | **KEEP + parse + raw-on-fail** |
| **DEF 14A** | Annual proxy: beneficial-ownership table, executive compensation, board composition | High — beneficial ownership, governance, comp structure | Yes (`def14a_beneficial_holdings`) | **Drop on success** — raw is 713 KB avg × 16k filings = 11 GB; extracted output is 11 MB. Re-fetch from SEC archive is idempotent | **KEEP + parse + raw-on-fail** |
| **DEFA14A** | Activist proxy supplements, board letters during contested elections | High — activist campaign signal | Partial (def14a parser accepts) | Persist on parse-fail | **KEEP + parse + raw-on-fail** |
| **DEFM14A, DEFR14A** | Merger / revised proxy variants | Medium — M&A signal | Partial | Persist on parse-fail | **KEEP + parse + raw-on-fail** |
| **3, 3/A** | Initial insider holdings statement | High — insider baseline cumulative position | Yes (`insider_form3_ingest`) | Persist on parse-fail | **KEEP + parse + raw-on-fail** |
| **4, 4/A** | Insider transactions (Section 16) | High — insider buy/sell signal, the bedrock of the insider ranking | Yes (`insider_transactions`) | Persist on parse-fail | **KEEP + parse + raw-on-fail** |
| **13F-HR, 13F-HR/A** | Institutional holdings (managers $100M+ AUM) | High — institutional ownership mosaic | Yes (`institutional_holdings`) | Drop on success — informationtable.xml re-fetch is idempotent. 14 GB savings | **KEEP + parse + raw-on-fail** |
| **NPORT-P** | Mutual fund / ETF holdings (monthly) | High — fund flows + sector positioning | Yes (`n_port_ingest`) | Persist on parse-fail | **KEEP + parse + raw-on-fail** |
| **SCHEDULE 13G, 13G/A** | Passive 5%+ blockholder | High — institutional concentration signal | Yes (blockholder_filings) | Persist on parse-fail | **KEEP + parse + raw-on-fail** |
| **SCHEDULE 13D, 13D/A** | Activist 5%+ blockholder (with intent) | High — activist target detection | Partial (blockholder_filings) | Persist on parse-fail | **KEEP + parse + raw-on-fail** |
| **NT 10-K, NT 10-Q** | Notification of late filing — reason often signals trouble (auditor change, restatement pending, accounting issue) | **High — late-filing is a red flag** that historically correlates with restatements / SEC actions | None — no parser | Persist on parse-fail (parser TBD) | **ADD — keep metadata + add lightweight parser later** |
| **20-F, 20-F/A** | Foreign annual report (non-US issuers) | Medium — same role as 10-K for foreign issuers; phase 2 | Coverage classifier reads it | Persist on parse-fail | **KEEP metadata + raw-on-fail** (used by coverage classifier today, parser later) |
| **40-F, 40-F/A** | Canadian annual report | Same as 20-F | Coverage classifier | Persist on parse-fail | **KEEP metadata** |
| **6-K, 6-K/A** | Foreign material event / interim (non-US ADRs) | Medium — equivalent to 8-K + 10-Q for foreign issuers | Coverage classifier | Persist on parse-fail | **KEEP metadata** |
| **13F-NT, 13F-NT/A** | "I'm filing combined with parent X" notice | Low — used for filer classification only | Filer-directory classifier | Skip raw | **KEEP metadata** |
| **424B2/B3/B4/B5/B7/B8** | Prospectus supplements (bond/secondary offerings) | Capital-action signal IS thesis-relevant (new debt, dilutive issuance, ATM offerings, refinancing). Final pricing + use-of-proceeds land here weeks before 10-Q. Codex round 1 flagged the original SKIP call as underweighting capital-action signal | None | Skip raw | **METADATA-ONLY** (parser deferred — see #1015) |
| **144** | Insider notice of intent to sell restricted shares | Form 4 captures the actual transaction afterwards, BUT 144 surfaces insider overhang + low-float pressure + founder/VC distribution intent before the sale. Codex round 1 flagged: not fully superseded by Form 4 | None | Skip raw | **METADATA-ONLY** (parser deferred) |
| **CORRESP** | Letters between issuer and SEC review staff | High-signal in rare cases (revenue-recognition Q&A, going-concern queries, restatement-pending). 95% routine but the 5% is exactly LLM thesis material. Codex round 1: SEC says CorpFin reviews focus on disclosures that may conflict with rules / be materially deficient — exactly what an LLM deep-dive would consume | None | Skip raw | **METADATA-ONLY** (deep-dive LLM parser deferred) |
| **5, 5/A** | Annual insider catch-all for transactions missed in Form 4 | Form 4 should capture most, but Form 5 reveals late/exempt Section 16 reporting. Codex round 1 flagged: late/missed reporting is itself an insider-quality / compliance signal | None | Skip raw | **METADATA-ONLY** (parser deferred) |
| **S-1, S-3, S-4** | Registration statements (IPO, secondary, M&A) | S-1 = IPO / new-issuer thesis; S-3 = shelf takedowns + ATM + secondary; S-4 = merger/exchange-offer economics. Codex round 1 flagged the SKIP call as missing capital-action thesis material | None | Skip raw | **METADATA-ONLY** (IPO/M&A parser deferred to v2) |
| **F-1, F-3, F-4** | Foreign-issuer mirrors of S-1/S-3/S-4 | Same signal as S-* for foreign issuers; thesis-relevant for ADRs and foreign-listed names | None | Skip raw | **METADATA-ONLY** |
| **PRE 14A, PRER14A** | Preliminary proxy + revisions | Contested votes, dilution authorisations, reverse splits surface here before DEF 14A is final. Codex round 1 flagged | None | Skip raw | **METADATA-ONLY** |
| **SC TO-T, SC 14D9, DEF 13E-3** | Tender-offer acquirer materials, target recommendation, going-private fairness | M&A / take-out signal. DEF 13E-3 is the conflict-of-interest disclosure for going-private transactions | None | Skip raw | **METADATA-ONLY** |
| **25, 25-NSE, 15-12B, 15-12G, 15-15D, 15F variants** | Delisting / deregistration / foreign termination notices | Terminal-state signal — instrument leaving the universe. Codex round 1 flagged | None | Skip raw | **METADATA-ONLY** |
| **11-K** | Annual report for employee stock-purchase / savings / 401(k) plans | Low priority but reveals employer-plan stock concentration | None | Skip raw | **METADATA-ONLY** |
| ~~**FWP**~~ | Free writing prospectus (marketing material for offerings) | Near-zero — sales-pitch tier sheets, real terms land in 424B at offering close | None | Skip | **SKIP** |
| ~~**N-CSR, N-CSRS**~~ | Mutual-fund annual / semi-annual reports | Low — NPORT covers fund holdings monthly with structured XBRL | None | Skip | **SKIP** (NPORT supersedes) |
| ~~**D, D/A**~~ | Private placement notices (Reg D exempt offerings) | Low — private placement, not directly relevant to public-equity ranking | None | Skip | **SKIP** |
| ~~**S-8, S-8 POS**~~ | Registration of employee stock plans | Marginal — dilution disclosure already in 10-K cover-page DEI tags + risk factors | None | Skip | **SKIP** |

### Form-type categories not in the table

The dev DB shows ~125 distinct form_type values across 800k
``filing_events`` rows. The table above + the Tier 2 / Tier 3 code
blocks below cover ~70 explicitly. Everything else defaults to
**SKIP** — adding a form to PARSE+RAW or METADATA-ONLY requires a
documented "what LLM / ranking pipeline consumes this"
justification.

Default = skip is the right posture: each new form type added to
the allow-list should require a documented "what would the LLM /
ranking pipeline do with this" justification, not the reverse.

## Concrete output of the form-type review

### Three-tier allow-list (post-Codex round 1)

Codex pushed back on the binary "parse OR skip" model. Reality is
three tiers — each costs different storage:

| Tier | Per-form cost | When to use |
|---|---|---|
| **PARSE+RAW** | filing_events row + raw payload retained per retention policy | Active parsers exist; raw needed for re-parse-on-bug |
| **METADATA-ONLY** | filing_events row only (~200 bytes); no raw body fetch | No parser yet but the form has thesis / signal value an LLM or future ranking signal would consume |
| **SKIP** | nothing — never appears in filing_events | Pure noise / regulatory boilerplate; no documented LLM use case |

A metadata-only row costs ~zero (200 bytes) but lets a future parser
backfill from `filing_events` without a fresh submissions.json walk
of every CIK. This is the cheap insurance Codex flagged.

#### Tier 1 — PARSE+RAW (active parsers + retention policy)

```python
SEC_PARSE_AND_RAW: frozenset[str] = frozenset({
    "10-K", "10-K/A",
    "10-Q", "10-Q/A",
    "8-K", "8-K/A",
    "DEF 14A", "DEFA14A", "DEFM14A", "DEFR14A",
    "3", "3/A",
    "4", "4/A",
    "13F-HR", "13F-HR/A",
    "NPORT-P", "NPORT-P/A",
    "SCHEDULE 13G", "SCHEDULE 13G/A",
    "SCHEDULE 13D", "SCHEDULE 13D/A",
})
```

#### Tier 2 — METADATA-ONLY (no parser yet; future signal value)

Codex round-1 explicitly added each of these:

```python
SEC_METADATA_ONLY: frozenset[str] = frozenset({
    # Late-filing red flags — restatement / auditor-change signal.
    "NT 10-K", "NT 10-Q",
    # Foreign-issuer classification + future parsers (used by
    # coverage today; parser deferred to UK/EU phase 2).
    "20-F", "20-F/A",
    "40-F", "40-F/A",
    "6-K", "6-K/A",
    # 13F-NT — used for institutional-filer classification only.
    "13F-NT", "13F-NT/A",
    # Capital actions — IPO / secondary / shelf / debt / M&A.
    # Final pricing + use-of-proceeds in 424B don't land in 10-Q
    # for weeks; signal is fresher in these forms.
    "S-1", "S-1/A",
    "S-3", "S-3/A",
    "S-4", "S-4/A",
    "F-1", "F-1/A",  # foreign IPO
    "F-3", "F-3/A",  # foreign shelf
    "F-4", "F-4/A",  # foreign M&A
    "424B2", "424B3", "424B4", "424B5", "424B7", "424B8",
    # Proxy variants — contested votes / dilution authorisations
    # / reverse splits land here before DEF 14A.
    "PRE 14A", "PRER14A",
    # Tender offers + going-private — M&A / take-out signal.
    "SC TO-T", "SC TO-T/A",
    "SC 14D9", "SC 14D9/A",
    "DEF 13E-3", "PREM14C", "DEFM14C",
    # Delisting / deregistration — terminal-state signal.
    "25", "25-NSE",
    "15-12B", "15-12G", "15-15D",
    "15F", "15F-12B", "15F-12G", "15F-15D",
    # Insider compliance — late/exempt Section 16, proposed
    # restricted-share sales (insider overhang). Codex flagged
    # these are not fully superseded by Form 4.
    "5", "5/A",
    "144",
    # SEC correspondence — rare red-flag signal (rev-rec Q&A,
    # going-concern, restatement-pending). 95% routine but the
    # 5% is exactly LLM thesis material.
    "CORRESP",
    # Employer-plan stock concentration (low priority but cheap).
    "11-K",
})
```

#### Tier 3 — SKIP (zero documented value)

```python
SEC_SKIP: frozenset[str] = frozenset({
    # Marketing material — final terms land in 424B.
    "FWP",
    # Mutual-fund annual/semi reports — NPORT-P covers fund
    # holdings monthly with structured XBRL.
    "N-CSR", "N-CSRS",
    # Private placement notices — exempt offering, not directly
    # ranking-relevant for public-equity universe.
    "D", "D/A",
    # Employee stock plan registrations — dilution disclosure
    # already in 10-K cover-page DEI tags + risk factors.
    "S-8", "S-8 POS",
})
```

#### Default = SKIP

The dev DB observes ~125 distinct form_type values. The three tiers
above cover ~70. Everything else defaults to SKIP. Adding a new
form-type to Tier 1 or Tier 2 requires a documented "what LLM /
ranking pipeline consumes this" justification.

#### `SEC_INGEST_KEEP_FORMS` (the union the ingester actually uses)

```python
SEC_INGEST_KEEP_FORMS: frozenset[str] = (
    SEC_PARSE_AND_RAW | SEC_METADATA_ONLY
)
```

This is the constant `bootstrap_filings_history_seed` and
`daily_research_refresh` pass to `refresh_filings(filing_types=...)`.
The downstream raw-fetch step gates on `SEC_PARSE_AND_RAW` membership
to decide whether to fetch the raw body.

### What `bootstrap_filings_history_seed` (S5) does today

```python
refresh_filings(filing_types=None, ...)  # = "all forms"
```

### What it should do

```python
refresh_filings(filing_types=list(SEC_BOOTSTRAP_FORM_ALLOWLIST), ...)
```

### Apply to `daily_research_refresh` too

Currently passes `filing_types=["10-K", "10-Q", "8-K"]` — narrower
than the bootstrap allow-list. Daily incremental should also use
the full bootstrap allow-list so first-install + nightly converge to
the same coverage shape.

### Coverage / parser implementation gap

Several forms in the allow-list have **no parser** today:

- `NT 10-K`, `NT 10-Q` — late-filing notices (high signal, no parser)
- `20-F`, `40-F`, `6-K` — foreign-issuer reports (coverage classifier
  uses metadata, no parser)
- `DEFM14A`, `DEFR14A` — proxy variants (def14a parser accepts but
  no specific path)

**Decision**: keep the metadata in `filing_events` for these; they
cost ~bytes per row, downstream parsers ignore them, but they're
available when (a) the parser lands, or (b) operator does ad-hoc
LLM analysis via SQL.

## Raw-payload retention policy

### Current state (uncompressed sizes from dev DB)

| document_kind | count | total | avg/doc |
|---|---|---|---|
| `def14a_body` | 16,588 | 11 GB | 713 KB |
| `infotable_13f` | 14,316 | 3.6 GB | 267 KB |
| `primary_doc` | 14,317 | 32 MB | 2 KB |
| `form4_xml` | 2,500 | 21 MB | 9 KB |
| `form3_xml` | 1,000 | 3.6 MB | 4 KB |

### Why we persist raw

The prevention-log rule is "Raw API payload must be persisted before
any parse / normalise step". The justifications historically:

1. **Re-parse on parser bug**: if a parser fix lands later, we can
   re-parse without re-fetching from SEC.
2. **Forensic audit**: operator can verify what we actually got from
   SEC at ingest time.
3. **SEC takedown risk**: in theory, SEC could remove a filing.
   In practice, SEC archive is durable — accepted filings remain
   accessible at their `primary_document_url` indefinitely.

### Proposed policy

Three retention modes, picked per `document_kind`:

| Mode | When | Storage cost |
|---|---|---|
| **drop-on-success** | Persist raw → run parser → if parse succeeds, delete the raw row → if parse fails, keep raw for forensic re-parse | ~zero steady state; one filing's worth of TOASTed text per recent failure |
| **keep-on-fail** | Same as drop-on-success, plus keep raw rows for any parse outcome marked `failed` or `partial` until the issue is resolved | low; bounded by parse-fail tail |
| **keep-always** | Persist raw forever — used only for filings whose URL is unstable or whose parse cost is prohibitive | full current cost |

### Per-form decision

| document_kind | Mode | Rationale |
|---|---|---|
| `def14a_body` | **drop-on-success** | 11 GB recoverable. Re-fetch is idempotent. Forensic value low — extracted table is the operator-relevant data; full proxy text is rarely needed except for legal review |
| `infotable_13f` | **drop-on-success** | 3.6 GB recoverable. XML is short, re-parse cheap |
| `primary_doc` | keep-always | 32 MB, tiny. Index page is sometimes used for cross-referencing. Not worth optimising |
| `form4_xml` | keep-always | 21 MB. Form 4 amendments + insider-transaction forensics are non-trivial; 21 MB is cheap insurance |
| `form3_xml` | keep-always | 3.6 MB. Same reasoning |
| (future) 10-K body | **drop-on-success** | Annual reports are 5-50 MB each; 9k filings = 45 GB-2 TB. Operator should not pay this |
| (future) 8-K body | keep-on-fail | 50 KB - 1 MB each; full-text mining might want eventual access |

**Net savings**: ~14 GB (def14a + infotable). 95% of `filing_raw_documents`
storage. Successful parses re-fetchable from SEC.

### Reproducibility guard — hash-on-write, hash-verify-on-refetch

Codex round-1 flagged: "re-fetch is idempotent" is too strong without
a hash check. **Add reproducibility guard:**

1. On ingest, persist `payload_sha256` (32 bytes) + `accepted_at`
   + `parser_version` + `extracted_at` per `filing_raw_documents`
   row. These rows survive the drop-on-success sweep — only the
   `payload` column itself is nullified.
2. On any future re-fetch (manual operator action, parser-bug
   re-parse), compute hash of fetched bytes and compare to the
   persisted `payload_sha256`. Mismatch = SEC silently changed the
   document → fail loud, surface to operator, do NOT auto-overwrite.

This makes drop-on-success safe: we always have proof of what we
parsed against, even after the bytes are gone. Operator gets a
clean "the document changed" signal if SEC ever does retract /
re-issue.

### Risk: SEC takedown — actual edge cases

Codex enumerated the real edge cases:

- **Public-release timing for CORRESP / UPLOAD**: SEC withholds
  staff-review correspondence until the review is closed. Then
  releases it bulk. We never see in-flight correspondence. No
  retraction risk; just delayed availability.
- **SEC correction / reprocessing**: rare, but happens (typo in
  filing metadata gets corrected on republish). Hash mismatch
  surfaces this.
- **Malformed historical filings**: pre-2001 EDGAR sometimes has
  non-canonical encodings. Parse-fail keeps raw under `keep-on-fail`
  by definition, so this is auto-handled.
- **Withdrawn filings**: visible as filing + RW (request to
  withdraw) accession. Both stay in EDGAR. No bytes lost.
- **CT ORDER (confidential treatment)**: withheld by design, never
  was bytes to begin with. Not a takedown case.
- **Parser-success-but-semantically-wrong**: hash guard doesn't
  catch this; only re-parse with the fixed parser does. This is
  why we keep `parser_version` per row — sweeper can re-fetch only
  rows with `parser_version < latest_for_kind`.

If a takedown were ever observed (no historical case), we'd flip
the affected `document_kind` to `keep-always` — the policy is
per-form, mutable, not a one-way door.

## Apply to all SEC ingest paths

This audit is one-shot but the framework should govern future ingest
work. Per-stage allow-list discipline:

| Stage / Job | Currently | Should be |
|---|---|---|
| `bootstrap_filings_history_seed` (S5) | `filing_types=None` (all forms) | `filing_types=SEC_BOOTSTRAP_FORM_ALLOWLIST` |
| `daily_research_refresh` (orchestrator-driven) | `["10-K", "10-Q", "8-K"]` | `SEC_BOOTSTRAP_FORM_ALLOWLIST` |
| `daily_financial_facts` (orchestrator-driven) | Master-index walks all forms | Filter to allow-list at the master-index parse step |
| `sec_first_install_drain` (S6) | All sources via `manifest_source_for_form` | Filter same allow-list before recording manifest entries |
| Future: 10-K body parser | n/a | `keep-on-fail` raw |
| Future: 8-K Item-1.01 deep-dive | n/a | `keep-on-fail` raw |

## Open questions for review

1. **Should `CORRESP` be in the allow-list as a "deep-dive" mode**?
   Argument for: rare red-flag signal (revenue-recognition Q&A).
   Argument against: 95% routine, requires NLP to surface signal
   from noise, no current parser. **Default skip; revisit when an
   LLM-driven thesis pipeline can consume it as a free-text input.**

2. **Should `424B*` be added back as metadata-only**?
   Capital actions (debt issuance, secondary offerings) ARE
   thesis-relevant. Argument for: cheap to keep metadata.
   Argument against: 100k+ rows of noise, signal extraction needs
   a parser we don't have. **Default skip; revisit when the
   capital-actions parser lands.**

3. **Should foreign forms (20-F, 40-F, 6-K) keep raw bodies**?
   We don't parse them yet. Coverage classifier reads metadata
   only. **Skip raw, keep metadata** matches the "no parser, no
   raw" rule.

## Migration plan

1. **PR1 (small, low-risk)**: Add `SEC_BOOTSTRAP_FORM_ALLOWLIST`
   constant + thread it through `bootstrap_filings_history_seed`
   and `daily_research_refresh`. Cuts S5 writes by ~32% on next
   first-install. Minimal blast radius.
2. **PR2 (data-migration)**: Retroactively delete `filing_events`
   rows for skipped form types from existing dev DB. Idempotent.
3. **PR3 (raw-retention infra)**: Add `retention_mode` column on
   `filing_raw_documents` + per-document_kind policy + a sweeper
   job that drops `drop-on-success` rows where the parse outcome
   was `success`. ~14 GB recovery.
4. **PR4 (sweeper retroactive run)**: Operator triggers
   `POST /jobs/raw_retention_sweep/run` to drop existing
   already-parsed raw bodies.

## Spec deliverables

- [ ] Codex review of the per-form decisions (especially CORRESP /
      424B / S-1 trade-offs).
- [ ] Operator sign-off on the allow-list shape before PR1.
- [ ] PR1-PR4 land in sequence.
- [ ] Runbook update: how to add a new form type to the allow-list
      (the documentation question is "what LLM / ranking pipeline
      will consume this"; the implementation question is
      "what's the raw retention mode").
