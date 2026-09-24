# #2351 — DEF 14A Item 403 rows go only to the sibling whose class they report

Status: slice 1 (census) shipped (#3352). Slice 2 is the suppression-ledger reframe
(2026-09-24, third pass); the writer-side withhold version failed ckpt-1 three times
(49 → 78 → 88 findings; #2351 handoffs 03:56Z and 04:21Z). Slice 3 is scoped only.

## Problem

Every DEF 14A writer fans an accession's Item 403 rows out to **every** instrument that
shares the issuer CIK (`siblings_for_issuer_cik`). Three writers do this:
`app/services/manifest_parsers/def14a.py::_parse_def14a` (live manifest drain),
`app/services/def14a_ingest.py::_ingest_single_accession` (legacy), and
`app/services/rewash_filings.py::_apply_def14a`. So warrants (`HTZWW`, `OPENW`,
`XRXDW`), preferred (`STRK`/`STRF`/`STRC`/`STRD`, `LILAP`) and unregistered classes
(`ATROB`) render the common-stock holders as their own, and dual-class pairs
(`GOOG`/`GOOGL`, `FOXA`/`FOX`) receive the same column's figures twice.

## Source rule

- **Item 403 is per-class data.** Reg S-K Item 403(a) and (b) (17 CFR 229.403) prescribe
  the table columns *(1) Title of class, (2) Name, (3) Amount and nature of beneficial
  ownership, (4) Percent of class*. The amount and percent are defined against ONE class.
  `.claude/skills/data-sources/sec-edgar.md` §3.6 lists DEF 14A as "no per-security data
  inside → fan out". That is right for the document's issuer-level content (8-K-style
  events, comp) and wrong for the Item 403 table; this PR corrects §3.6.
- **Which class an instrument is:** the 10-K/10-Q cover's Section 12(b) table, tagged
  `dei:Security12bTitle` + `dei:TradingSymbol` + `dei:SecurityExchangeName` in one
  shared context per registered class (Reg S-K Item 601(b)(104), Reg S-T Rule 406).
  Read with `scripts/census_2900_sec_cover_identity.py::parse_cover_contexts`. We do not
  store it today.
- **No published rule maps proxy class text to a registered class.** Where this spec
  classifies a cover title (common vs non-common) the rule is fixed by construction below
  and frozen in `RECIPIENT_RULE_VERSION`.

## Slice 1 — census (shipped #3352, measurement only)

`scripts/census_2351_def14a_class_binding.py`. Population: the 49 CIKs with >1 instrument
and stored DEF 14A holdings, 103 accessions. For each proxy it takes the latest
10-K/10-K/A/10-Q/20-F filed **on or before** the proxy's filing date (never later), fetches
its `<stem>_htm.xml` instance, and reads the 12(b) pairs.
Reproduce: `PYTHONPATH=. uv run python -m scripts.census_2351_def14a_class_binding --out var/census_2351/report.json`.

At `97e20ec6` the run printed (every figure below is from that one run):

- point-in-time cover with ≥1 12(b) pair: **101 of 103** proxies, all at depth 0 (the
  newest eligible cover was usable every time). The 2 without are 2019/2020 proxies whose
  cover predates iXBRL (HTTP 404 on `_htm.xml`).
- per-(proxy, sibling) decision under the slice-2 rule below (224 decisions):
  `legacy_multiclass` 128 (63 proxies whose cover registers ≥2 common classes —
  `GOOG`/`GOOGL`, `FOXA`/`FOX`, `HEI`/`HEI.A`, Liberty entities …), `legacy_duplicate` 44,
  `withhold_not_on_cover` 22 (10 of them with no sibling on the cover at all),
  `receive` 11, `withhold_non_common` 11, `legacy_no_cover` 4.
  Holder-row × recipient writes: 3,507 today → 3,005 (502 withheld).
- **Why dual-class is NOT in slice 2.** A table-level binder (cover title minus its
  par-value clause, maximal whole-phrase match, reject any residual class word) binds
  **2 of those 128** decisions (`multiclass_decisions_table_bound`). H-shape (side-by-side class columns) and V-shape
  (*Title of class* column) tables carry both classes in one table, so a table-level rule
  cannot attribute rows; only the column the parser actually read can. Withholding on
  "unbound" would blank BOTH siblings on ~60 proxies (e.g. `GOOGL` loses every Item 403
  row) — trading one wrong figure for a larger gap on the most-viewed names. Slice 3 binds
  the read column.
- Exact-token/title rules from the handoff (R1 ticker / R2 title / R3 sole-common, as a
  per-sibling "is my class named anywhere in the selected tables") reach 31 / 7 / 5 of 224
  decisions (`sibling_rule`); reported for slice 3's design, not used by slice 2.
- `withhold_not_on_cover` includes three CIKs whose cover belongs to a **different
  company** than the instrument (`OSG` on CIK 874501 → cover `AMBC`; `ABX` on 1814287 →
  `ABL`; `NXH` on 1130713 → `BYON`/`BBBY`). Withholding there is correct: those holders
  are not the instrument's. The identifier defect itself is outside #2351.

## Slice 2 — suppression ledger for warrant / preferred siblings (reframe, 2026-09-24)

Replaces the writer-side withhold spec that failed ckpt-1 three times (49 → 78 → 88
findings). Three changes of model, one per failed finding group:

1. **Readers, not writers.** The three DEF 14A writers, the fan-out and the parser are
   untouched. Suppression is a ledger that readers anti-join. No row is deleted from any
   fact table, so there is no cross-writer cleanup, rewash cohort or orphan handling.
2. **Positive evidence only.** A sibling is suppressed only when its OWN point-in-time
   cover title says it is a warrant or preferred AND another sibling on that cover is
   common. "Not on the cover", an unrecognised title, an ambiguous match or an
   unresolvable cover never CREATE a suppression. The rule never infers from absence.
3. **Dual-class is out.** Siblings that are both common (`GOOG`/`GOOGL`) keep today's
   behaviour; binding the column the parser read is slice 3.

### Source rule for "a warrant / preferred sibling does not own these rows"

- Item 403(a)/(b) report beneficial ownership per **class** (*Title of class*, *Percent
  of class*), with beneficial ownership determined under Rule 13d-3 (Item 403
  Instruction 1 / 17 CFR 240.13d-3).
- **Warrants:** Rule 13d-3(d)(1)(i) deems a holder the beneficial owner of the securities
  it can acquire within 60 days, so warrant holdings enter the Item 403 table through the
  UNDERLYING common class's figure. The parsed figure is a common-class amount and
  percent; it is not the warrant instrument's holding, whatever the table layout.
- **Preferred:** Item 403(b) may report a preferred class as its own column. A preferred
  sibling correctly owns a figure only when the parser read that column. Measured for
  every suppression in the population (below): the stored rows are the common column
  (MSTR `0001193125-26-186895`: Vanguard 24,062,886 = 7.4% of class A common; XRX
  `0001770450-26-000020`: DD Revocable Trust 15,283,672 = 11.7% common). ⚠ A future
  proxy whose parsed column IS the preferred would lose correct rows on the preferred
  sibling — a new loss this slice can cause, accepted because the parser cannot
  identify the column it read (slice 3), and the common sibling is never touched.
- Which class an instrument is: the cover 12(b) table (`## Source rule` above).
- The title classifier and the witness requirement are fixed by construction; no
  published rule classifies a 12(b) title.

### Population effect

Re-running the rule below over the census records (`var/census_2351/report.json`, run at
`97e20ec6`, script in the PR as `scripts/ab_2351_recipient_suppressions.py`): all 106
distinct cover titles classify as 80 `common`, 20 `non_common` (warrants/preferred), 6
`other` (notes). **11 of 220 sibling decisions suppress, 172 parsed holder rows** —
`HTZWW` ×2 proxies, `XRXDW`, `OPENL`/`OPENW`/`OPENZ`, `STRK.US` ×2, `STRC`, `STRD`,
`STRF`. The census is a population snapshot, not a guarantee; the A/B (acceptance) is
the full-population check on the implemented job.

### Schema (one migration, `sql/420_def14a_recipient_suppressions.sql`)

```
def14a_recipient_suppressions(            -- only suppressions exist; no 'receive' rows
  instrument_id         bigint NOT NULL REFERENCES instruments,
  accession_number      text   NOT NULL,     -- the DEF 14A / DEFA14A
  issuer_cik            text   NOT NULL,
  reason                text   NOT NULL CHECK (reason = 'non_common_sibling'),
  rule_version          int    NOT NULL CHECK (rule_version > 0),
  cover_accession       text   NOT NULL,
  cover_title           text   NOT NULL,     -- the sibling's Security12bTitle
  cover_symbol          text   NOT NULL,
  witness_instrument_id bigint NOT NULL REFERENCES instruments,
  witness_title         text   NOT NULL,
  created_at            timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (instrument_id, accession_number))

sec_cover_12b_fetches(                     -- cache of IMMUTABLE filings; terminal outcomes only
  cover_accession text PRIMARY KEY,
  outcome         text NOT NULL CHECK (outcome IN ('pairs','no_pairs','not_found')),
  entity_ciks     text[] NOT NULL DEFAULT '{}',  -- >1 on a co-registrant cover
  fetched_at      timestamptz NOT NULL DEFAULT now())

sec_cover_12b_pairs(
  cover_accession text NOT NULL REFERENCES sec_cover_12b_fetches,
  security_title  text NOT NULL,           -- whitespace-collapsed
  trading_symbol  text NOT NULL,           -- upper-cased, trimmed
  PRIMARY KEY (cover_accession, trading_symbol, security_title))
```

Plus `document_kind = 'xbrl_cover_instance'` in the `filing_raw_documents` CHECK,
`DocumentKind` and `SWEPT_DOCUMENT_KINDS` (born-compacted: hash + mandatory `source_url`,
rehydratable). `store_raw` commits in its own transaction BEFORE parsing
(`docs/review-prevention-log.md:1252`).

Three views, the single shared predicate:

- `def14a_beneficial_holdings_attributed` = `def14a_beneficial_holdings h` WHERE NOT
  EXISTS a suppression on `(h.instrument_id, h.accession_number)`.
- `ownership_def14a_observations_attributed`, `ownership_esop_observations_attributed`
  — same anti-join on `(instrument_id, source_accession)`. Measured: 0 NULL
  `source_accession` in either table (126,237 / 91 rows).

### Readers switched to the views

Located by grep `FROM def14a_beneficial_holdings` / `FROM ownership_(def14a|esop)_observations`.

- `app/services/ownership_observations.py` — the `USING` source of
  `refresh_def14a_current` (:1477), `refresh_esop_current` (:1945),
  `refresh_def14a_current_batch` (:2847), `refresh_esop_current_batch` (:2966). The
  `MAX(ingested_at)` watermark reads stay on base tables (they track observation
  arrival, not attribution).
- `app/services/ownership_history.py:436`.
- `app/api/instruments.py` :3302/:3310 (latest holders), :3531 (CSV export).
- `app/services/def14a_drift.py` :253 (`_reconcile_alert_rows` purge), :292
  (`_upsert_alert` guard), :354 (`_select_latest_def14a_holders`) — all three, so the
  purge, the guard and the detector agree on "latest attributed accession".

NOT switched, deliberately — they are ingest diagnostics that report what was STORED:
`instruments.py` :3358 (typed row count), :3376 (tombstone count),
`ownership_drillthrough.py` :502/:517, and the `filing_events` latest-known-filing query
(:3334; a filed proxy exists whether or not its rows are attributed). Also not switched:
the writers, `rewash_filings.py:951`, `ownership_observations_sync.py:761` (observations
stay the complete fact log), census/A-B scripts.

⚠ Suppressing an instrument's latest accession makes its latest ATTRIBUTED accession an
older one. If that older accession is not itself suppressed (no resolvable cover), its
holders surface. That is today's wrong figure, older — never a new one. The A/B lists
every instrument where this happens.

### Rule (pure, `app/services/def14a_recipients.py`, `RECIPIENT_RULE_VERSION = 1`)

**Title kind** (case-insensitive, word-bounded, checked in this order):
1. contains `units?` or `rights?` → `other` (a bundle or a rights instrument is neither
   a clean witness nor a clean suppression);
2. contains `warrants?` → `non_common`;
3. else the FIRST of `preferred|preference` / `common|ordinary|capital (stock|shares?)`
   decides → `non_common` / `common`;
4. no match → `other`.

So `Warrants to purchase Common Stock` and `Common Stock Purchase Warrants` → non_common;
`Common Stock and associated preferred stock purchase rights` and `Preferred Stock
Purchase Rights` → other; `Units, each consisting of one share … and one warrant` →
other; `Capital Securities` → other. `other` never suppresses and never witnesses.

**Symbol match** — eToro symbol trimmed, upper-cased, trailing `.US` dropped, compared
for EQUALITY with the cover `TradingSymbol` (trimmed, upper-cased). No punctuation
stripping (`ABC.D` ≠ `ABCD`). A sibling's pair set = the distinct titles whose symbol
equals its key.

**Siblings** — distinct `instrument_id`s sharing the (sec, cik) identifier. An
instrument that carries more than one sec CIK (17 in dev) is excluded from both roles.

`decide(siblings, cover) -> list[Suppression]`, for each sibling S:
- S suppresses iff S has exactly one pair, its title is `non_common`, AND some other
  sibling W with a DIFFERENT symbol key has exactly one pair whose title is `common`.
- Otherwise S is not suppressed.

`.US` duplicates share a key, so `STRK`/`STRK.US` are decided identically and never
witness each other.

### Cover resolution (lifted from the census)

For accession A with filing date D = `min(filing_date)` over `filing_events` rows with
`provider = 'sec'` and `provider_filing_id = A` (none → A is skipped and keeps its
stored rows, exactly like unresolved): candidates are `provider = 'sec'` `filing_events`
rows for any sibling with
`filing_type IN ('10-K','10-Q','20-F')` (originals only — no amendment semantics),
`filing_date < D` (strictly before: no same-day ordering question) and
`filing_date >= D - 400 days` (the annual cover cycle plus slack; older is no evidence),
distinct accessions (the row with the lowest `filing_event_id` represents a duplicate),
ordered `filing_date DESC, provider_filing_id DESC`, at most 4 considered (every candidate counts,
including ones without an `.htm` primary).

Per candidate, cached by `sec_cover_12b_fetches` when terminal:
- no `.htm` primary document → `not_found`, next candidate;
- GET `<stem>_htm.xml` via the shared SEC client + rate gate: 404/410 → `not_found`,
  next candidate; any 200 → `store_raw` the decoded body (committed), then parse — a
  root element other than `xbrl`, or an XML parse error, is **unresolved**; anything
  else (transport error, unfollowed 3xx, 401/403/429/5xx) → **unresolved**. Unresolved
  stops the walk, and A keeps its existing suppression rows unchanged this run.
- Parse = `parse_cover_contexts`, lifted into `app/services/sec_cover_identity.py`
  (bytes in; the census-2900 script imports it from there), plus: facts must be in the
  `dei` namespace, `dei:EntityCentralIndexKey` values are cached as `entity_ciks` and
  checked at USE time (a cover whose entity CIKs do not include the issuer CIK is skipped
  as a candidate — co-registrant covers such as Hertz and Xerox carry two, so
  the cache is issuer-independent), singleton (title, symbol) per
  context; a symbol that ALSO appears in a context with several titles/symbols is
  dropped from the pairs (ambiguous); titles whitespace-collapsed, symbols trimmed and
  upper-cased, empty values dropped. No pairs → `no_pairs`, next candidate. The fetch
  row and its pairs are written in ONE transaction.
- First candidate with `pairs` is the cover. None within the budget → A has no cover.

### Job — full recompute, diff, apply

`def14a_recipient_suppressions` ScheduledJob, source `sec_rate` (it fetches from SEC; no
new lane), daily 04:15 UTC, `catch_up_on_boot=True`, bootstrap-gated like its
neighbours. Each run:

1. Population: multi-sibling CIKs (count DISTINCT instrument_id > 1); accessions = the
   union of `def14a_beneficial_holdings.accession_number` and
   `ownership_(def14a|esop)_observations.source_accession` over those instruments.
2. Per accession: resolve the cover (steady state: all cache hits, zero fetches), run
   `decide` → desired suppression set. Unresolved accessions keep their current rows.
3. Diff desired vs stored per instrument, comparing the WHOLE row (evidence columns and
   `rule_version` included). For each instrument with any insert/update/delete, ONE
   transaction: apply its ledger changes, call `refresh_def14a_current`,
   `refresh_esop_current` (they nest as savepoints) and `detect_drift(instrument_id=…)`
   (purges alerts whose accession is no longer the latest attributed one AND re-mints
   the ones that now apply). Ledger and materialisations commit together, so a crash
   leaves both or neither and a later run re-diffs. An instrument whose transaction
   raises is logged and skipped; the rest continue; the run reports failure if any
   instrument failed.
4. A stored row whose accession/instrument dropped out of the population is deleted in
   step 3 (it is not in the desired set). Rows from an older `rule_version` are
   recomputed like any other.
5. Job output: per-run counts (accessions, unresolved, fetches, inserted, deleted,
   instruments refreshed). The job is the only writer of the ledger.

Reversal = disable the job, delete the rows, and run `refresh_def14a_current` /
`refresh_esop_current` / `detect_drift` for the affected instruments (the ledger rows
name them).
A newly ingested DEF 14A shows on a warrant sibling until the next successful run.

### Tests

- Pure table tests: title kind (the examples above + every census title class), `decide`
  over HTZ/HTZWW, MSTR + four preferred incl. `STRK`/`STRK.US`, OPEN + three warrant
  series, GOOG/GOOGL (none), `BALY`/`BALY.US` (none), wrong-issuer CIK (none), notes-only
  second pair (none), `ABC.D` vs `ABCD`, a sibling matching two titles (none).
- Cover resolution with a fake client: 404 → next; 503 → unresolved, stored rows kept;
  CIK mismatch → next; ambiguous-context symbol dropped.
- ONE DB test: a suppression removes the accession from `ownership_def14a_current`, the
  typed latest-holders read and the drift purge for that instrument only; deleting it
  and re-running the diff restores them.

### Acceptance (DoD 8-12, corpus rung)

- Full-population A/B over every multi-sibling CIK: for every instrument, the full row
  set (hash) of `ownership_def14a_current`, `ownership_esop_current`, the typed
  latest-holders read, the CSV export and `def14a_drift_alerts`, before/after the job's
  first run, plus `ownership_history` def14a series. The ONLY instruments that change
  are the suppressed set; every other instrument's hashes are identical. Instruments
  falling back to an older accession are listed.
- Smoke `/instruments/{HTZWW,STRK.US,OPENW}` ownership: no DEF 14A holders from any
  suppressed accession (an unsuppressed older accession may still show and is listed);
  `HTZ`, `MSTR`, `OPEN`, `GOOGL`, `AAPL` unchanged.
- Cross-source: `HTZWW` and `STRK` cover titles vs the 12(b) table on EDGAR.

### Accepted limits (ckpt-1 round 2, classified by the author)

Every item below can only FAIL TO SUPPRESS or leave today's figure; none can suppress a
common instrument, because a suppression needs an exact symbol match to a warrant/
preferred title AND an exact match of a different sibling to a common title on a cover
of the same issuer CIK.
- XBRL context periods/dimensions, exchange, nil/placeholder facts: the census-2900
  same-context reader is the source rule (Reg S-T Rule 406 / Item 601(b)(104)); context
  dimensions beyond it are not modelled.
- Instance discovery assumes `<stem>_htm.xml` (the iXBRL extracted-instance convention);
  a miss is `not_found` → no evidence → no suppression. `not_found` is cached: an
  accession's derived files are immutable once published.
- Current symbols against historical covers: bounded by the same-CIK and 400-day
  requirements; ticker reuse inside one issuer within 400 days is not modelled.
- Amended covers (10-K/A) are ignored; a corrective amendment of the 12(b) table is not
  modelled.
- Decision history: deletions are logged in the job output, not kept in a history table.
- The scheduler never runs two instances of one job concurrently (one lane permit), so
  mixed-version / overlapping runs do not arise.

## Slice 2b — class evidence from another proxy's cover (`RECIPIENT_RULE_VERSION = 2`)

### Problem (measured on dev after slice 2's first run)

7 suppressed instruments now show an OLDER proxy's common holders, because that proxy's
point-in-time cover does not list them at all:

| instrument | older accession (proxy date) | its point-in-time cover lists |
| --- | --- | --- |
| OPENL / OPENW / OPENZ | 0001140361-25-022644 (2025-06-16) | `0001801169-25-000038`: OPEN only |
| STRC / STRD / STRF | 0001193125-25-100720 (2025-04-28) | `0000950170-25-021814`: MSTR, STRK only |
| XRXDW | 0001770450-25-000017 (2025-04-09) | `0001770450-25-000010`: XRX only |

The older cover does not list the security; the writers fan the older proxy out by CIK
to every sibling that exists today. Reproduce: the A/B script's
`fallback_to_older_accession`, and `sec_cover_12b_pairs` joined to
`sec_cover_12b_fetches` for CIKs 0001801169 / 0001050446 / 0001770450.

### Source rule

Unchanged from slice 2 (§ "Source rule for …" above): warrants enter Item 403 through the
underlying common figure (Rule 13d-3(d)(1)(i)); a preferred sibling owns a figure only when
the parser read the preferred column, which it cannot identify (slice 3) — slice 2's
accepted limit. Re-measured for the three new preferred targets: the MSTR 2025 proxy
`0001193125-25-100720` rows fanned to STRC/STRD/STRF are the class A common column
(Vanguard 16,303,720 = 6.5%), identical to MSTR's. An `instrument_id` is one security and
its class does not change over time. So positive class evidence for instrument I from
ANY cover of I's issuer applies to every accession fanned to I. No published rule governs
cross-cover evidence; the veto below is fixed by construction under rule version 2.

### Rule (pass 2 in `compute_desired`, after the unchanged point-in-time pass)

For each instrument I with ≥1 point-in-time suppression in this run:
1. **Evidence** = I's point-in-time suppression from the accession with the latest proxy
   date (tie → greater accession number). Its `cover_*` and `witness_*` are copied.
2. **Veto** — no pass-2 rows for I if any cover resolved in this run for I's CIK maps I's
   symbol key to a title set that is not exactly one `non_common` title (i.e. it lists I as
   `common`, `other`, or ambiguously). The cover resolved for a proxy is the only cover the
   run reads for it; covers outside those are not consulted.
3. **Targets** = accessions I itself holds rows for (typed holdings or either observation
   table — not the sibling group's union) that got no point-in-time suppression for I.
4. **Blocked** — if any accession I holds has no proxy date or an unresolved cover this
   run, I gets no pass-2 rows AND none of I's stored rows are deleted (the missing cover
   may be the evidence or the veto). This also replaces slice 2's accession-wide `keep`
   with a key-level one: an unresolved/undated accession keeps the stored rows of its own
   sibling group only.
5. Each target gets a row with `reason = 'non_common_sibling_other_cover'` and the
   evidence's cover/witness columns (so `cover_accession` may post-date the proxy — that is
   the audit trail of which cover proved the class).

Direction is symmetric (older and newer accessions); only the older direction occurs on
dev today.

**Bound (why this cannot suppress a common instrument):** a pass-2 row needs a pass-1
suppression of the SAME instrument (exact symbol match to a warrant/preferred title plus a
different common witness on a same-CIK cover), and no resolved cover of that CIK calling
that symbol anything else.

### Schema

`sql/421_def14a_recipient_other_cover.sql`: widen the `reason` CHECK to
`('non_common_sibling', 'non_common_sibling_other_cover')`. No other change.
`RECIPIENT_RULE_VERSION` → 2, so every stored row not held by a blocked key/instrument is
rewritten once (whole-row diff) and its instrument refreshed. Evidence switching between
runs (a later proxy's decision appearing) rewrites pass-2 rows without changing keys.

### Accepted limits (ckpt-1, classified by the author)

All can only fail to suppress, or extend an existing pass-1 decision of the SAME instrument:
- The veto sees only covers resolved this run; `cover_pairs` drops ambiguous symbols
  before caching, so an ambiguous listing is absence, not a veto. Dev: 0 vetoes.
- A veto does not undo pass-1 rows (slice 2 semantics); `instruments_vetoed` is reported.
- A pass-1 ticker-reuse mismatch (slice 2's limit) is extended to the instrument's other
  accessions. Distance between evidence and target is unbounded by design: the claim is
  about the instrument, not the date.
- Different non-common titles across covers (warrant series renamed) do not veto.
- Apply counts are incremented before apply; `instruments_failed` is the failure signal
  and the A/B fails on it.

### Tests

Pure: pass 2 over (a) OPEN-shaped — older cover lacks the warrant → older accession
suppressed with the newer cover's evidence; (b) veto — an older cover lists the symbol as
common → no pass-2 row; (c) a blocked instrument → nothing; (d) an instrument with no pass-1 suppression →
nothing; (e) an accession the instrument holds no rows for → not a target. Existing DB test unchanged (the views do not move).

### Acceptance (corpus rung)

The A/B script's pass condition is generalised so it holds on a non-empty ledger: the set
of instruments whose surface hashes changed EQUALS the set of instruments whose ledger
KEYS `(instrument_id, accession_number)` were added or removed by the run (on an empty
ledger this reduces to slice 2's condition). Re-run it after deploy: inserted = 7 (the
table above), updated = 11 (version bump), changed instruments = {OPENL, OPENW, OPENZ,
STRC, STRD, STRF, XRXDW}, `fallback_to_older_accession` empty. Smoke `/instruments/{OPENW,STRF,XRXDW}` ownership:
no DEF 14A holders; `OPEN`, `MSTR`, `XRX` unchanged.

## Slice 3 — scoped only

Bind the column the parser READ (H-shape group header, V-shape class cell, per-table
caption) to a cover title, emit per-class rows keyed by instrument, and withhold the
unbound sibling. Needs its own spec + ckpt-1. The census records give its test set.

## Security

No auth surface. One new outbound fetch class (SEC Archives instance XML) through the
existing rate-gated client with the configured SEC User-Agent.
