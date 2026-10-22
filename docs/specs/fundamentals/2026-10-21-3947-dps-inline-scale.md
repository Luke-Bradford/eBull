# #3947: dividend-per-share facts tagged with an inline `scale`

**Ticket:** #3947 (refs #3943, #3903, #3915). **Status:** spec v4. Codex checkpoint 1 ran three rounds, the cap:
- round 1 raised 45 findings, applied in v2;
- round 2 accepted the declared-policy framing and raised 31 implementation findings, applied in v3;
- round 3 kept the framing accepted and raised 31 findings, applied in v4 without a fourth round, as #3910, #3915,
  #3903 and #3919 did ("Checkpoint record").

The dividends pane's TTM and latest DPS sum `financial_periods.dps_declared`, and some stored members are a
thousand or a million times the amount the filing displays. EFC shows a TTM DPS of 1,170.39 against a latest DPS
of 0.39. The research on #3947 (2026-10-21) found that SEC companyfacts equals our raw store for these facts: the
filer's inline XBRL produced the value, not our parser. This spec declares a policy for the class a published
data-quality rule flags, and states that a second class (FMBH) is left as filed.

## Problem

Figures marked [n] come from `PYTHONPATH=. uv run python -m scripts.measure_3947_dps_inline_scale` (dev DB and
EDGAR, 2026-10-21; the output is pasted on #3947). The dividends-paid, SEC-frames and cross-filing measurements are
from the #3947 research comments, which carry their own commands; this script does not reproduce them.

- **The scale class.** In EFC's `efc-20250930.htm` the displayed `0.39` carries `scale="3"`, so the instance value
  is 390. Companyfacts serves 390 and `financial_facts_raw` stores 390.
- **[1] Population:** 11,873 accessions filed on or after 2019-06-15 hold a `USD/shares`
  `CommonStockDividendsPerShareDeclared` fact. The same query without the unit restriction also returned 11,873
  (an earlier run of the script, 2026-10-21). 7,979 have an `.htm` primary document in `sec_filing_manifest`; the
  rest are `.txt` full submissions or have no URL.
- **[2] Pilot, primary documents only.** The accessions are selected by a `USD/shares` fact; the counts are
  regex-detected inline occurrences of the concept in the primary document, with unit, context and value
  unresolved. They are not counts of correctable stored facts, and a miss means no regex match.
  - All 68 `.htm` accessions holding a fact ≥ 10: 12 have an occurrence with `scale` > 0 (45 occurrences at
    scale 3, 1 at scale 6). They are EFC (5 accessions), SCI, HTB and TPVG (2 each) and WHR (1: `1.75` at
    scale 6, stored as 1,750,000). 2 of the 68 have no regex-detected occurrence in the primary document; one, WSO
    `0001193125-25-041730`, carries its statements in `d898605dex13.htm`.
  - A seeded random 150 of the 7,911 others: no regex-detected occurrence with a positive scale (726 detected).
  - This is a pilot. Nothing here rests on "only large values are scaled": the scan reads every accession in scope.
- **[3] The cents class.** FMBH's FY2025 10-K tags `98` with `decimals="INF"` and no `scale` (#3947 research).
- **Readers of the stored member** (a grep of `app/` and `sql/`, 2026-10-21):
  - `instrument_dividend_summary` and `dividend_history` (sql/050), which feed the dividends pane through
    `app/services/dividends.py`. `ttm_dps` sums the present members of the last four quarters (sql/050:96);
  - `financial_periods_ttm.dps_declared_ttm` (sql/508) and `instrument_valuation.dps_declared_ttm`, read by
    `ranking_pot_rebalance.py:706-709` for the pot, which is halted (#2842) but not removed;
  - `ranking_ablation_reader`'s diagnostic.

  The dividend yield stopped reading these members under #3903. No order or allocation path reads them today.

## Source rule

| decision | rule | source |
|---|---|---|
| which occurrences are flagged | an inline `us-gaap:CommonStockDividendsPerShareDeclared` fact whose `scale` attribute is an integer greater than 0 | XBRL US DQC.US.0179.10141 (rule "Dividend Event Disclosures", released 2024-06-27, effective 2024-08-31, https://xbrl.us/data-rule/dqc_0179/): the component "checks for an inline scale greater than 0" on this concept. Message: "Generally the amount per share are not scaled when reported." Example: "a value of 230 with an inline scale of 3" |
| how the instance value is formed | the transformed display, multiplied by 10^scale, negated when `sign="-"` | Inline XBRL 1.1 Part 1 §10.2.3; {scale} is ten raised to the attribute (§10.1) |
| which inline fact is the stored fact | same accession, entity, unit, period and instance value, on a context with no dimension | companyfacts serves only the non-dimensional member of a concept (`.claude/skills/data-sources/sec-edgar.md` §7.17) |
| which documents to read | every document of the accession's Inline XBRL Document Set | Inline XBRL 1.1 Part 1 (a document set's contexts and units may sit in any of its documents); WSO above shows the primary document alone is not enough |

**What the rule establishes, and what it does not.** DQC_0179.10141 is a warning ("generally"), not a proof that
a given occurrence is wrong. Dividing by 10^scale recovers the *unscaled transformed numeric value*: the number
the statement displays, after the format transformation and sign. That is the amount the filed statement shows a
reader. It is not independent evidence of the economic amount.

### Declared policy

1. **Restore, as a declared policy.** A stored fact whose matching occurrences all carry the same scale s > 0 is
   read by derivation as its value ÷ 10^s. The risk is stated: if a filer genuinely displays a per-share amount
   in thousands of dollars, the restored value understates it by 10^s. The acceptance below lists every
   restoration with its displayed text and the issuer's other evidence for the same period, so that risk is
   visible case by case. That evidence is reported beside the decision; it is not a guarantee.
   - Why not refuse: `ttm_dps` sums the *present* members (sql/050:96). When no other fact fills the period, a
     refused member can turn an overstated TTM into an understated one, and a refused FY member with no surviving
     alternative removes the derived Q4. In the measured snapshot, EFC's Q3 2025 has no other accession
     reporting it.
2. **Positive scale only.** A negative scale (a display in cents, `scale="-2"`) yields a correct instance value
   when the tagging is right. Negative-scale defects, if any, are outside this detector.
3. **Mixed repairs refuse.** One fact may be displayed several times. When the occurrences matching a stored
   fact carry different scales (`0.39` at scale 3 and `390` at scale 0 encode the same instance value, but imply
   different repairs), the repair is ambiguous and the fact is refused, as #3344 refuses.
4. **Doubt keeps.** A fact is read as stored today when its accession has no active scan, is not inline, holds an
   unresolved occurrence of the concept or an occurrence of another entity, or has no occurrence matching it
   (decision table rows 1–7).
5. **One concept.** The rule names `CommonStockDividendsPerShareDeclared` only. The other `dps_declared` aliases
   (`sec_fundamentals.py:281-287`, LP/LLC distributions per unit) are not scanned.
6. **Coverage cutoff.** 2019-06-15 is a deliberate cutoff at the first inline XBRL phase-in for large accelerated
   filers (sec-edgar skill, mandate table). Earlier facts are kept and counted as untested, not assumed non-inline.

## Construction

The correction is read only when `EBULL_DPS_INLINE_SCALE_ENABLED` is true (default false, `app/config.py`). The
scan can run and fill its tables with the flag off; the A/B evaluates decisions off and on without the flag; and
rollback starts by turning it off. The #2327 CUSIP bridge ships the same way.

### Document set

An accession's document set is every `.htm`, `.html` or `.xhtml` file in its `index.json` listing that contains
an `ix:header`. All of them are read as one Inline XBRL Document Set: contexts and units resolve across the set.
`FilingSummary.xml` is not used to find inline documents, because nothing guarantees that its `<InputFiles>` list is
complete. An accession whose listing holds no such file is `not_inline`.

*Build note (PR a).* The listing also holds the EDGAR Renderer's own pages (`R1.htm` …; WHR's
`0000106640-24-000125` lists 74). The scan skips exactly the files `FilingSummary.xml` names as
`<Report><HtmlFileName>`: SEC wrote those pages from the instance, so none can be a filer document. A name pattern was
rejected because filer document names can be upper-case (`SELECT count(*) FROM filing_documents WHERE document_name
~ '[A-Z]' AND document_name !~ '^R[0-9]+\.htm$'` was 1,090,470 on 2026-10-21). A missing or unparseable summary
excludes nothing, which costs requests but never coverage.

### Parser contract

- Elements and the concept QName are resolved by namespace, not by prefix spelling.
- **Defaults.** An absent `scale` is 0 and an absent `sign` is positive. §10.2.3 applies each "if present".
- **Scale lexical.** The attribute is `xs:integer`: after whitespace collapse it must match `[-+]?[0-9]+`. A
  separate implementation bound, −30..30, rejects other values with reason `scale_out_of_bound`.
- **Canonical key.**
  - Entity: the context identifier under scheme `http://www.sec.gov/CIK`, as a 10-digit zero-padded CIK.
  - Unit: units are compared as expanded QNames. Only a `divide` of `{http://www.xbrl.org/2003/iso4217}USD` over
    `{http://www.xbrl.org/2003/instance}shares` maps to companyfacts' `USD/shares`. Any other unit is
    `unsupported_unit`: it is excluded from matching, and it is not an unresolved occurrence.
  - Period: a duration's `startDate` and `endDate` as written; an instant has no start.
- **Two failure classes:**
  - **The attempt fails, and the active generation stays in force:** a fetch error or non-2xx response, a document
    that does not parse, a context or unit id defined twice in the set, or any element with a `target` attribute
    (a second target document is not handled).
  - **Unresolved, published on the occurrence:** a nil fact, a `contextRef` or `unitRef` that does not resolve, an
    invalid `sign`, a scale outside its lexical space or bound, or a format outside the Inline XBRL
    Transformation Registry (never guessed).
- **Reuse first.** edgartools 5.30.2 has an inline extractor (`edgar/documents/strategies/xbrl_extraction.py`,
  `XBRLExtractor`) that reads `scale`, `sign` and `format`; its transformation table holds five formats. The build
  adopts it only if it passes every parser fixture (acceptance 3). Otherwise the hand-rolled lxml reader cites that
  comparison. EFC, WSO and WHR are smoke tests on real documents, not the adoption test.

### Schema (one migration)

```sql
CREATE TABLE sec_inline_scale_scan (
    accession_number TEXT PRIMARY KEY,
    -- the active generation: NULL until a scan succeeds; a failed attempt never changes these
    outcome          TEXT CHECK (outcome IN ('inline', 'not_inline')),
    documents        INTEGER,
    parser_version   TEXT,
    scanned_at       TIMESTAMPTZ,
    -- work at attempt_version: reset once per version, when the first attempt at a new version starts
    attempt_version  TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ,
    last_error       TEXT
);

CREATE TABLE sec_inline_dps_occurrence (
    accession_number TEXT NOT NULL REFERENCES sec_inline_scale_scan ON DELETE CASCADE,
    document         TEXT NOT NULL,
    element_ordinal  INTEGER NOT NULL,   -- document order among the concept's elements
    fact_id          TEXT,
    context_ref_raw  TEXT,
    unit_ref_raw     TEXT,
    scale_raw        TEXT,
    sign_raw         TEXT,
    format_raw       TEXT,
    decimals_raw     TEXT,
    display_text     TEXT NOT NULL,
    entity_cik       TEXT,               -- the resolved fields below are NULL when unresolved
    period_start     DATE,
    period_end       DATE,
    dimensional      BOOLEAN,
    unit             TEXT,               -- 'USD/shares', or 'unsupported_unit'
    scale            INTEGER,
    instance_value   NUMERIC,
    unresolved       TEXT,               -- NULL when every field above resolved; otherwise the reason
    PRIMARY KEY (accession_number, document, element_ordinal)
);

CREATE SEQUENCE sec_inline_scale_renormalise_revision;
CREATE TABLE sec_inline_scale_renormalise (
    instrument_id    BIGINT PRIMARY KEY REFERENCES instruments,
    revision         BIGINT NOT NULL,    -- nextval of the sequence at each enqueue
    failures         INTEGER NOT NULL DEFAULT 0
);
```

Every occurrence is stored, scale 0 included, so each decision can be traced to its elements.

### The scan (`sec_dps_inline_scale_scan`, new job)

- **One scanner at a time.** The job runs under its `JobLock`, so two scans never overlap; a success publishes
  only if `attempt_version` still equals the version it started with.
- **Population:** accessions holding a `CommonStockDividendsPerShareDeclared` fact (any unit), filed on or after
  2019-06-15. An accession is **eligible** when `parser_version IS DISTINCT FROM current`, and either
  `attempt_version IS DISTINCT FROM current` or `attempts < 5 AND next_attempt_at <= now()`.
- **Attempt.** On the first attempt at a version, set `attempt_version = current` and `attempts = 0`. Each failure
  increments `attempts`, sets `next_attempt_at = now() + 2^attempts hours` and records `last_error`.
  - At 5 attempts the accession is **exhausted** at that version and is reported.
  - Re-admitting it means `attempts = 0, next_attempt_at = now()`.
- **Success** replaces the accession's occurrences and the active-generation columns in one transaction, and
  clears `attempts`, `next_attempt_at` and `last_error`.
- **Version bump.** Old generations stay active until their accession is rescanned at the new version.
- **Queue.** In the transaction of a success, the scan enqueues the accession's instruments with
  `revision = nextval(…)` (an upsert that always takes the new revision) when the old or new generation holds a
  positive-scale or an unresolved occurrence. A later scan never clears a queued row.
- **Draining.** The job takes queued rows in `revision` order and starts no new instrument after half its
  wall-clock cap. It normalises the instrument, then runs `DELETE … WHERE instrument_id = %s AND revision = %s`
  with the revision it read, so work queued meanwhile stays queued. A failure increments `failures`, and the row
  stays for the next run. Draining is skipped while the flag is off.
- **Normaliser writers are serialised.** `normalize_financial_periods` takes
  `pg_advisory_xact_lock(<namespace>, instrument_id)` inside its per-instrument transaction. Its callers (this
  job, `daily_financial_facts` at `app/workers/scheduler.py:5282`, `force_refresh.py:164`, bootstrap) can then
  never interleave writes for one instrument.
- **Order and budget:** draining as above, then filings from the last 7 days, then the backlog oldest first. All
  requests go through the SEC provider's shared 10 req/s limiter via `fetch_document_text` (its caller pin,
  `tests/test_fetch_document_text_callers.py`, gains this caller; the SQL normalisation path is these tables).
  - An illustrative size: 11,873 `index.json` requests plus each listed HTML document. At the pilot's mean
    primary document of 3.74–5.25 MB [2], the primary documents alone are roughly 45 GB, parsed and discarded.
  - Each run reports accessions by work state (done, pending, retrying, exhausted), documents fetched, and the
    queue length.

*Build note (PR b).* Draining admits no instrument at or after half of the scan's 45-minute start cap, measured
from the run's start, so the scan keeps the other half. The uncapped enablement run (acceptance 5.2) is
`drain_renormalise_queue(conn, admit_until=None)` in `app/services/dps_inline_scale_scan.py`, on an autocommit
connection. A failed normalisation is read from `NormalizationSummary.instruments_failed`, which the normaliser now
counts, because it logs a per-instrument failure and does not re-raise it. Rollback step 4 (`--verify-stored off`)
compares against a fresh normalisation, so it is meaningful only after step 3: stored rows drift as ingestion and
code move, while two normalisations in one snapshot agree exactly.

### The correction (normaliser)

Any normalisation applies the decisions, whatever triggered it. A raw-data import or an identifier change
therefore picks up existing occurrences through the existing normalisation paths; the queue only covers changes
to occurrences.

With the flag on, `normalize_financial_periods` step 1 (`app/services/fundamentals/__init__.py:2369-2397`) also
reads:
- the instrument's SEC CIKs (`external_identifiers`, provider `sec`, type `cik`, all rows, 10-digit zero-padded);
- the active scan row of each of its accessions;
- those accessions' occurrences.

One pure function, `_dps_fact_inline_scale_decisions(facts, scans, ciks, occurrences)`, returns a `Decision` for
every `CommonStockDividendsPerShareDeclared` fact: `action`, `reason`, `scale`, `value` and the matched
occurrence keys. The first matching row wins:

| # | condition | action | reason |
|---|---|---|---|
| 1 | filed before 2019-06-15 | keep | `out_of_scope` |
| 2 | the fact's unit is not `USD/shares` | keep | `unsupported_unit` |
| 3 | the accession has no active generation | keep | `untested` |
| 4 | the active generation is `not_inline` | keep | `not_inline` |
| 5 | an occurrence's entity CIK is not one of the instrument's SEC CIKs | keep | `entity_mismatch` |
| 6 | the accession holds an unresolved occurrence of the concept | keep | `unresolved` |
| 7 | no non-dimensional `USD/shares` occurrence has the fact's period and `instance_value = val` | keep | `no_match` |
| 8 | the matching occurrences carry more than one scale, one of them > 0 | refuse | `mixed_scale` |
| 9 | they all carry one scale s > 0 | restore, `value = val ÷ 10^s` | `scaled` |
| 10 | otherwise | keep | `unscaled` |

Rules 5 and 6 are accession-wide on purpose: an unresolved occurrence has no reliable key, so it may be any fact.

**Where decisions act: on DPS value selection only.**
- The existing structural filters (`_per_share_value_is_representable`, `_fact_period_ended_by_filing`) and
  period grouping and anchoring run on the raw values, exactly as today.
- After them, a restored fact's value is replaced, and a refused fact is skipped, only when a value is chosen for
  `dps_declared` and in that column's YTD pool.
- So no other column, no period row and no period date can change. A raw value the representability filter drops
  stays dropped and is reported as `dropped_before_decision`.
- `FactRow` is rebuilt from the raw value on every run, so a restore never applies twice, and
  `financial_facts_raw` is never changed.

The acceptance report applies the same function to every stored fact of the concept, including those the
normaliser's input filter (`:2375-2377`, fiscal year and period not NULL) excludes, and reports both populations.

## The cents class is left as filed

FMBH tags `98` for $0.98 with no scale. This investigation found no applicable published rule: it checked every
DQC_0179 component (10139–10144, 10151, 10153), none of which tests the magnitude of an unscaled amount.
Three discriminators were measured and none separates FMBH from genuine values:

1. **Dividends paid** (DPS × weighted-average shares ÷ dividends paid; #3947 research (a)): GBCI, XHR, DPZ and
   MSGS overlap FMBH.
2. **Dividends declared** on SEC frames, CY2010–CY2026 (#3947 research (b)): J, HL and MSGS overlap, and the band
   mixes scaled DPS, scaled share counts and scaled totals.
3. **FY ÷ nine-month YTD of the same issuer** [3]: of 2,217 eligible pairs (USD-per-share duration facts with a
   positive YTD; 0 selection keys hold two values), 11 have a ratio ≥ 10. FMBH sits at 133–134 and genuine special dividends at up to 46 (PGR) and
   39 (DDS). No observed genuine value reaches FMBH, but no rule supplies a threshold, and one placed between 46
   and 133 would be fitted to today's 11 pairs with no out-of-sample test.

A cross-filing ÷100 disagreement finds 5 instruments and cannot see a defect reported once (#3947, 21:13Z). Stock
splits also restate per-share history by exact ratios, so a ratio alone is not a defect.

**Decision:** FMBH's members are kept as filed, with no flag and no per-issuer exception, because an exception
list is a per-case patch with no rule to say where it ends. Effects: FMBH's `ttm_dps` (98.02 today) and its FY
members stay overstated on the pane and in `dps_declared_ttm`; its `latest_dps` (0.25) is a quarter and is
correct. The sec-edgar skill records the class, the three measured discriminators and FMBH. This change adds no
heuristic for it. Reopening needs a published rule or a structured source that marks the unit, tested on the
full population.

## Acceptance (corpus rung)

The flag stays off until 1–4 pass. Items 1, 2 and 4 run in one script (`scripts/ab_3947_dps_inline_scale.py`).
It uses one `REPEATABLE READ` transaction, opened with `SELECT 1` first, because `conn.transaction()` on an idle
psycopg connection commits. It runs the normaliser with decisions off and on through a function argument, not the
flag, reads every figure from that one snapshot, and rolls back.

1. **Scan coverage.** The script runs once every in-scope accession's work at the current version is done or
   exhausted, with no attempt in flight. It reports, for that snapshot:
   - accessions by active generation (`inline`, `not_inline`, none) and by work state (done, exhausted), with
     exhausted ones listed by `last_error`. 0 are pending or retrying;
   - every stored fact of the concept with one `Decision`, counted by reason for all facts and for
     derivation-eligible ones;
   - every `unresolved`, `entity_mismatch` and `no_match` accession, with its reasons. These keep today's value and
     do not block.
   - **Exhausted accessions are unknown coverage, not absence.** They keep today's value, are listed as a rollout
     exception, and no claim is made about their scale.
2. **Fact-level A/B.**
   - Every changed `dps_declared` cell is explained by its lineage: a restored fact, a refused fact (NULL or another
     fact filling it), or a derived Q4 whose inputs changed. Every other column, every period row's existence and
     its dates are unchanged. An unexplained change fails.
   - Every restored fact is listed with instrument, accession, period, display text, scale and restored value, plus
     the evidence from other accessions. That is every `CommonStockDividendsPerShareDeclared` fact of the same
     instrument, unit `USD/shares`, and the same `period_start` and `period_end` whose decision is `unscaled`, with
     all their values. Each restored fact is classified `agrees` (every one equals the restored value), `disagrees`
     (any differs) or `none`. This evidence sits beside the policy and does not gate it.
3. **Fixtures.**
   - **Parser**, one per contract clause: absent and present `scale` and `sign`; `+3` and ` 3 `; scale out of
     bound; supported and unsupported formats; nil; an unresolved `contextRef` and `unitRef`; a duplicate id; a
     `target` attribute; prefix variants for one namespace; an `unsupported_unit`; the CIK scheme; a
     multi-document set.
   - **Decisions**, with expected values written by hand: one per table row, plus scale 3 and 6, and a refused FY
     member with its derived Q4.
   - **DB tests for the job:**
     - a positive → unscaled rescan, which queues a reversal;
     - a failed rescan, which keeps the active generation;
     - a row re-queued during draining, which survives;
     - a version bump, with a once-per-version attempt reset;
     - exhaustion and re-admission;
     - two normalisations of one instrument, which serialise and give the same rows.
4. **Downstream**, decisions off against on in the same snapshot, for every instrument with a restore or a refusal:
   `instrument_dividend_summary.ttm_dps` and `latest_dps`, and `financial_periods_ttm.dps_declared_ttm`.
5. **Enablement.**
   1. Turn the flag on, then confirm that the jobs child PID and the API process have both restarted.
   2. Enqueue every instrument item 2 lists, and drain the queue to empty with an uncapped one-off run of the
      drain step.
   3. Only then run 6–8.
6. **Smoke panel:** AAPL, GME, MSFT, JPM and HD unchanged on `/instruments/<symbol>/dividends`.
7. **Source-display reconciliation:** EFC's Q3 2025 DPS against the figure `efc-20250930.htm` displays on EDGAR
   (0.39). This checks the implementation against the source display, not the policy.
8. **Live re-check:** EFC on `/instruments/EFC/dividends`. The four quarters `ttm_dps` sums are listed with their
   source accessions, and a derived Q4 shows its FY − Q1 − Q2 − Q3 arithmetic. `ttm_dps` must equal that sum.

## Rollback

1. Set `EBULL_DPS_INLINE_SCALE_ENABLED=false`. Confirm that the jobs child PID and the API process have restarted.
   Those are the processes that host every normaliser caller (`scheduler.py:5282`, `force_refresh.py:164`).
2. Pause `sec_dps_inline_scale_scan`. Wait until `job_runs` shows no run in progress of it or of
   `daily_financial_facts`. The per-instrument advisory lock orders any late writer before the rebuild's own write
   for that instrument.
3. Re-normalise every instrument holding a `CommonStockDividendsPerShareDeclared` fact. That set is wider than the
   affected one and needs no ledger, because the normaliser is deterministic over its inputs.
4. Verify in one snapshot: the A/B script in decisions-off mode against the stored rows, `dps_declared` and the
   item-4 downstream fields, gives 0 differences. An instrument that differs because ingestion moved its inputs is
   re-normalised and checked again.
5. Only then, if the change is abandoned, drop the three tables and the sequence with the migration's rollback file.

## Not in scope

- The cents class (above).
- The other `dps_declared` aliases and `dps_cash_paid` (policy 5).
- Inline scale on other per-share concepts. #3344 covers part of EPS: it catches the "in millions" class when
  earnings, shares and cover shares are present, and deliberately not the "in thousands" class.

## Checkpoint record

- **Round 1** (Codex, 45 findings), applied in v2:
  - The source: DQC as a warning; restore made a declared policy with its risk and a case-by-case listing
    (findings 1–4 and 7). The EFC claim is now "in the measured snapshot" (5). Negative-scale defects are out of
    scope (6). EPS coverage is stated as partial (8).
  - Matching: entity and unit added (9, 10); namespace-aware parsing, the document set, and unresolved outcomes
    (11, 12, 15); per-occurrence provenance (13, 14, 16).
  - Discovery and states: an `index.json` fallback, `not_inline` only when confirmed, and retryable states (17,
    18, 30). Per-form handling is uniform, and amendments are scanned under their own accession (19). The
    cutoff is stated as deliberate (20).
  - The measurement script: an integer scale test, `raise_for_status`, aligned `USD/shares` predicates, a
    deterministic FY/YTD tie-break, and occurrence labelling (21–24, 26). The spec states which figures the
    script reproduces (25) and labels the budget illustrative (27).
  - Design: coverage at three levels (28, 39); an executable schema (29); scan generations (31); durable,
    reversal-aware re-normalising (32, 33); fresh filings ahead of the backlog (34); correction before every
    filter (35).
  - Acceptance and rollback: lineage-explained A/B, fixtures and downstream checks (36–38), and an ordered
    rollback (40).
  - The cents class: the wording on rules (41), overlap and threshold separated (42), the retain decision made
    explicit (43), the reopening condition (44), and the reader audit dated (45).
- **Round 2** (Codex): the declared-policy framing for restore is accepted, with no further change needed to
  it. 31 implementation findings, applied in v3:
  - Decisions: an ordered `(action, reason)` table, with unresolved occurrences accession-wide (findings 1, 2).
    The CIK is passed in explicitly (6), and the canonical key is defined (7).
  - Schema: raw attributes kept, unresolved fields nullable, an `INTEGER` scale with a lexical and range check,
    and an element ordinal (3–5, 28).
  - Discovery and states: a fact-level completeness fallback (8); retry backoff and exhaustion (9); failed
    attempts leave the active generation in force (10); a version bump keeps old generations active until
    rescanned (11).
  - The queue: an instrument-level queue that later scans never clear, with a generation-safe delete and a
    half-budget cap on draining (12–15).
  - Acceptance: a frozen population with an accounting identity (16); decisions off against on, on one snapshot,
    behind the flag (17); all facts reported against derivation-eligible ones (18); refusal confined to DPS value
    selection, with every other column held unchanged (19); a defined comparator (20); DB tests for the job (21);
    downstream checks covering refusals (22); the EFC checks relabelled and pinned (23).
  - Rollback: re-normalise every DPS instrument, check against decisions off on current inputs, and quiesce the
    job first (24–26).
  - Measurement: labels match what was measured (27); the WHR example is removed from the ordering rationale
    (29); the refusal rationale says "can" (30); the parser adoption test is the whole contract, not three
    examples (31).
- **Round 3** (Codex): the declared-policy framing stays accepted. 31 findings, applied in v4 without a fourth
  round:
  - Discovery: the document set now comes from `index.json` alone, with every inline file read as one set; a
    `target` attribute fails the attempt (findings 1, 2).
  - State: an `attempt_version` with a once-per-version reset, a persisted `next_attempt_at`, a `JobLock` with
    version-conditional publication, and active generation reported separately from work state (3–7).
  - Snapshot: coverage, A/B and downstream run in one rolled-back `REPEATABLE READ` transaction (8, 24).
  - The queue: a sequence-backed revision, a per-instrument advisory lock on every normaliser writer, ordered
    draining with an admission deadline and a failure count (9–11). Decisions live in the normaliser, so every
    normalisation path applies them (12).
  - Where decisions act: on DPS value selection after the structural filters, so period structure cannot change
    (13). An all-facts report sits beside the derivation subset (14). The function contract takes scans, CIKs and
    occurrences and returns a `Decision` (15). CIK matching is against all of the instrument's SEC CIKs, with
    `entity_mismatch` (16).
  - Parsing: units compared as expanded QNames (17); absent `scale` and `sign` take their defaults (18); the
    `xs:integer` lexical space with a separate bound (19); parser fixtures per contract clause (20); the cutoff as
    decision row 1 (21); the two failure classes (22).
  - Acceptance and rollout: exhausted accessions are unknown coverage (23); an enablement barrier with a full drain
    (25).
  - Rollback: every normaliser host restarted and in-flight writers ordered by the lock (26); one-snapshot
    verification, including downstream (27).
  - Measurement: regex-detected labels (28); a tie report for the FY/YTD selection (29); an empty-arm guard and
    `min(150, n)` (30); the script's request pacing documented, since it runs outside the jobs limiter (31).
