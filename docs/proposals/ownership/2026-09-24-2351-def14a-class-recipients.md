# #2351 — DEF 14A Item 403 rows go only to the sibling whose class they report

Status: slice 1 (census) shipped (#3352). Slice 2 is the suppression-ledger reframe
(2026-09-24, third pass); the writer-side withhold version failed ckpt-1 three times
(49 → 78 → 88 findings; #2351 handoffs 03:56Z and 04:21Z). Slice 2b shipped (#3354). Slice 3 (H-shape, dual-class common) shipped (#3355); slice 3b (V-shape, per-row withhold) is below.

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

## Slice 3 — dual-class common siblings (`RECIPIENT_RULE_VERSION = 3`)

### Problem (measured on dev, 2026-09-24)

The census's 63 `legacy_multiclass` proxies (two or more COMMON siblings on one cover)
come in two table shapes (read from each proxy's selected tables on dev, 2026-09-24).

- **H-shape** — one column group per class (`Class A Common Stock | Class B Common Stock`,
  each over `Shares | Percent`). The parser reads ONE shares column (`_resolve_columns`:
  leftmost of the top tier), so every sibling receives that one class's figures.
  `GOOG` (Class C) shows Alphabet's Class A / Class B column figures; `FOX` (Class B)
  shows FOXA's Class A figures.
- **V-shape** — a *Title of Class* / *Title of Series* column, one row per (holder,
  class). The parser de-duplicates holders by name across rows (`seen`, first-wins) and
  the store key is `(instrument_id, accession_number, holder_name)`, so a holder's
  per-class rows reach every sibling as ONE row of one class (`LEN`/`LEN.B`,
  `GEF`/`GEF.B`, the Liberty entities). No stored row carries its class, so no
  reader-side rule can recover it. **Slice 3b (scoped only, below).**

### Source rule

- Item 403(a)/(b) columns are per class (*Title of class*, *Percent of class*) — § Source
  rule at the top. A figure read from the column captioned `Class A Common Stock` is a
  Class A figure; it is not a Class C holding.
- Which caption sits above a cell: the HTML table model (HTML Living Standard §4.9.12
  "Forming a table"), as implemented by `sec_def14a._layout_rows` (verified against
  `pandas.read_html`; see its docstring). DEF 14A has no structured-data mandate
  (`sec-edgar.md` §2.2), so the markup is the source. The standard's header-ASSOCIATION
  algorithm (`headers`/`scope`) is not used: EDGAR proxies rarely carry those attributes,
  so the rule reads the column's header block positionally and adds the vetoes below.
- Which class an instrument is: the point-in-time cover 12(b) title (slice 2).
- **No published rule** maps a caption's class wording to a 12(b) title. Fixed by
  construction below (class-letter equality) and frozen in `RECIPIENT_RULE_VERSION`.

### Rule

**Designators** — `designators(text)`: the letters of `class|series` (optionally
`classes`) followed by a single letter or a list of them (`Class A and B`, `Classes A and
B`, `Class A, B or C`). A letter followed by a word character or `-` (`Class A-1`,
`Class II`) is not a designator.

**Cover side** (`usable_common_classes`) — a letter L is usable iff exactly one cover title
of ANY kind carries it, that title is `common` naming only L, and exactly one sibling key
maps to it (the sibling's key maps to exactly one cover title). Undesignated common titles
(`Common stock`) are never usable: such a sibling is never suppressed and never a witness.

**Table side** (`sec_def14a.share_locations`) — the tables are the ones the parser selects
for A's stored `def14a_body` (`item403_table_htmls`, a behaviour-preserving extraction from
`parse_beneficial_ownership_table`). A *location* of stored row r (S's own
`def14a_beneficial_holdings` rows for A) is a cell of `_layout_rows(table)` whose text has
no `%` and parses to exactly `r.shares`, on a grid row where some cell's
`_layout_name_key` equals r's. Per location:
- *captions* — the column's non-numeric texts in the table's header block (rows above the
  first row carrying ANY share-count cell — independent of which rows are stored), minus
  any text that also sits above the holder's name cell in that header row (a spanning
  table title, not a column caption);
- *interior* — the column's non-numeric texts between the header block and r's row;
- *row texts* — r's row's other non-empty cells.

A location's label is the single letter of the union of `designators` over its captions;
it is unlabelled when that union is empty or has several letters, when any caption names
a non-common security (`preferred|preference|warrants?|rights?|units?|notes?|debentures?`),
or when any interior or row text carries a designator (a mid-table re-header, a V-shape
class cell). r's label = the single label over ALL its locations; r is *unbound* with no
location (including every NULL-share row), an unlabelled location, or two labels.

**Decision** (`decide_class_column`) — S with usable letter L is suppressed for A iff S
has ≥1 stored row for A, every row is bound, no row's label is L, and ≥1 row's label is
the usable letter of a DIFFERENT sibling key (the witness). A letter the cover does not
list (Alphabet's unregistered Class B) still proves "not Class C". Ledger row:
`reason = 'other_common_class_column'`, `cover_*` = A's point-in-time cover and S's title,
`witness_*` = the lowest-`instrument_id` witness.

No cross-accession extension (per-proxy evidence). Unresolved cover → slice-2 `keep`.
Missing body, or a parse exception (logged; parsing is deterministic) → no evidence.
0 of 43,291 `def14a_body` rows have a NULL payload.

**Failure direction.** A suppression needs every row S shows for A to sit, in the
parser's own tables, under header captions naming exactly one class letter that is not
S's, with no class word in the row or the column below the header, and one row naming a
sibling on the cover. Everything else keeps today's rows.

### Measured reach

`PYTHONPATH=. uv run python -m scripts.probe_2351_read_column` (the job's `compute_desired`,
offline, rolled back) on dev at `2cc7f698` + this branch: 103 accessions, 0 unresolved,
**10 suppressions** — `GOOG` ×2, `FOX.US` ×3, `Z` ×2, `DGICB` ×2, `RUSHB.US` ×1. The probe
prints each one's rows per letter and captions; all 10 were read: every row sits under
`Class A …` (GOOG: 9 A + 2 B, 8 A + 1 B; FOX: `Non-Voting Class A Common Stock`, FOX being
voting Class B). Not reached, fail-open: every V-shape proxy; mixed tables (`MOG`,
`BELFA`); undesignated siblings (`UHAL.B`'s column is `Voting Common Stock`, no letter);
`BIO-B` / `WSO-B` (eToro `-B` vs cover `.B` / `B`, slice 2's exact key). The A/B's output
is the acceptance set.

### Schema

`sql/422_def14a_recipient_class_column.sql`: widen the `reason` CHECK. Views and readers
unchanged. (The migration runner applies each file in one transaction.)

### Tests

`tests/test_def14a_class_column.py` (pure): designators incl. lists / `A-1` / `II`;
cover usability (undesignated, a letter shared with a preferred title); decision —
GOOG-shaped suppress, own-letter row keeps, unlocated / unlabelled / conflicting row
keeps, no witness keeps, undesignated never, `.US` duplicates together. Locator: H-shape
group caption, mid-table re-header → unlabelled, V-shape → unlabelled, spanning title
ignored, percent cell never matched.

### Acceptance (corpus rung)

- Parser extraction: `parse_beneficial_ownership_table` output (rows, as-of date, score)
  identical old-vs-new on all 43,291 stored `def14a_body` payloads.
- `scripts/ab_2351_recipient_suppressions.py` after deploy: changed instruments ==
  instruments whose ledger keys changed; inserted = the 10 above; updated = every existing
  row (version bump). Smoke `/instruments/{GOOG,FOX,Z}` DEF 14A holders: none from a
  suppressed accession; `GOOGL`, `FOXA`, `ZG`, `MOG.A`, `LEN` unchanged.
- Cross-source: one GOOGL holder's stored figure against the Class A column of the
  Alphabet 2026 proxy on EDGAR.

### Accepted limits (ckpt-1, 37 findings, classified by the author)

Fixed in the rule above: undesignated captions (1), mid-table re-headers (2, 4), spanning
titles and prose (3), V-shape class cells (6), percent cells (7), class lists and `A-1`
(14, 15), preferred/warrant captions (17), cover-wide letter uniqueness (19), NULL-share
rows (23), per-accession error boundary (36), 3b's parser de-dup (33). Accepted, each
fail-to-suppress or bounded by the all-rows + witness requirement: name-key prefix
collisions needing an equal share count (9); name in a non-name cell (10); lossy count
normalisation (8); locations the grid cannot see (11); selection ≠ historical provenance
(12, unlocated rows block); percent not compared (13); class/series share one letter space
(16); a parent/subsidiary class lettered like the registrant's in the same table (18);
letter renamed between cover and proxy (20, 21, 22); ESOP/observation rows share the
accession's column (24); ledger staleness ≤ one daily run, as slice 2 (25); ledger keeps
cover + witness, the probe reproduces the rest (35); `_layout_rows` HTML-model gaps (28,
29) and its one-case oracle (30). Rebutted: 26 (missing body is permanent, not transient),
27 (non-usable letters are deliberately not suppressed), 31 (all 10 suppressions read —
the harmful side is the full population), 32 (full-corpus parser comparison added), 37
(runner is transactional).

## Slice 3b — V-shape: withhold a row read from another class's line (`RECIPIENT_RULE_VERSION = 4`)

### Problem (measured on dev, 2026-09-24, at `4ecfe483`)

A V-shape table has a *Title of class* cell per row. The parser keeps a holder's FIRST
row (`seen`, first-wins) and every sibling receives it. On Lennar's 2026 proxy
(`0001193125-26-073504`) `LEN` (Class A) shows Stuart Miller 21,851,560 at 70.2% — his
Class B line — and `LEN.B` shows BlackRock 16,936,080 at 7.8% — a Class A line.

Slice 3 treats a row class cell as a veto, so every V-shape proxy keeps all rows. The
stored row DOES sit on one grid row, and that row's class cell names its class; what is
lost is the holder's OTHER lines (the de-dup), not the class of the line that was kept.

**Scope of this slice: withhold the misattributed rows.** Restoring the discarded lines
needs the writer-side change (parser de-dup on (holder, class), class-aware store key,
routing across three writers) and stays out; it is the shape that failed ckpt-1 three
times in slice 2. Failure direction here: a sibling loses a wrong-class figure, it never
gains one.

### Source rule

Item 403(a)/(b) column (1) *Title of class* (§ Source rule at the top): in a V-shape table
it is a row cell, and the amount and percent in that row are figures of that class.
Which class the instrument is: the point-in-time cover 12(b) title (slice 2). Class-letter
equality is fixed by construction (slice 3) and frozen in `RECIPIENT_RULE_VERSION`.

### Rule

**Class column** (locator, `share_locations`): a column whose header-block text matches
`title of (class|series)` — Item 403's column (1) by its own caption. `ShareLocation`
gains `class_captions` (those columns' header texts) and `class_cells` (the distinct
non-empty texts of the holder's row in those columns, excluding any column holding the
matched name cell). Slice 3's fields are unchanged, so slice 3 is unaffected.

Per location, `row_class_label(loc)` is `None` unless ALL hold:
- the share column is not class-captioned: no caption carries a designator or a
  non-common word (H-shape is slice 3's), and no interior text carries a designator;
- no class caption carries a non-common word or `%` (Seneca's class column is headed
  `6% Preferred Stock` in one table);
- exactly one class cell; it carries exactly one designator `(keyword, letter)`
  (keyword ∈ class / series), no non-common word and no `%` (Seneca's `10% Series B`
  preferred line).
Otherwise the label is `(keyword, letter)`. A row's label = the single label over ALL its
locations; unbound otherwise (no location, a `None` location, two labels).

**Decision** (`decide_class_rows`) — for a sibling S with usable letter L (slice 3's
`usable_common_classes`), and a stored row r of S for accession A: withhold r iff r's
label is `(k, M)` with M ≠ L, M the usable letter of a sibling with a different key (the
witness, lowest `instrument_id`), and the witness's cover title carries the designator
with the same keyword k (a `Series B` line is not a `Class B` line). Skipped when (S, A)
is suppressed whole by slice 3 or slice 2. Everything else keeps.

### Measured reach

`PYTHONPATH=. uv run python -m scripts.probe_2351_row_class` (offline, rolled back) on dev:
103 accessions, 0 unresolved, **148 rows** — `LBTYB` 45, `LBTYK` 44, `SENEB` 20, `LBTYA` 11,
`GEF` 7, `GEF.B` 7, `BELFB` 4, `LEN` 4, `LEN.B` 4, `SENEA` 2. `LEN`: Miller + GAMCO
withheld (Class B lines); `LEN.B`: BlackRock + Vanguard withheld (Class A lines). The
first draft's rule (any designator in the row) reached 160; the class-column rule drops
Seneca's `10% Series B` preferred lines among others. Several Liberty rows are parser
artefacts named after a role or an address line (`Director`, `Clarendon House, …`): they
are stored rows fanned to every sibling and are withheld on the same evidence.

### Schema (`sql/423_def14a_recipient_row_suppressions.sql`)

`def14a_recipient_row_suppressions`: `instrument_id`, `accession_number`, `holder_name`,
`issuer_cik`, `reason` (`CHECK = 'other_common_class_row'`), `rule_version`,
`cover_accession`, `cover_title`, `cover_symbol`, `witness_instrument_id`,
`witness_title`, `class_cell` (the *Title of class* cell, evidence), `created_at`; PK
`(instrument_id, accession_number, holder_name)`. The three `*_attributed` views gain a
second `NOT EXISTS` on `(instrument_id, accession, holder_name)` (`plan_name` for ESOP;
88 of 91 ESOP observations match a holdings name exactly). Column lists unchanged, so
`CREATE OR REPLACE VIEW` is valid and no reader changes.

### Job

`compute_desired` also returns the row keys; the diff/apply is per instrument for both
ledgers in the same transaction, with the same `keep` / `keep_instruments` semantics
(an unresolved or undated accession keeps its row suppressions). Version bump → every
existing ledger row updates.

### Tests

Pure (`tests/test_def14a_class_column.py`): `row_class_label` — V-shape class cell labels;
class-captioned column → None; non-common row text → None; two letters in the row → None;
re-header → None. `decide_class_rows` — LEN-shaped withhold both ways; own letter keeps;
unbound keeps; letter with no witness keeps; whole-accession suppression skips.
DB (`tests/test_def14a_recipients_db.py`, one test): all three views and `_current`
exclude a row-suppressed holder (ESOP by `plan_name`) and keep the sibling's row;
deleting the ledger row restores it. The parser's output is untouched (only
`share_locations` gained fields), so no full-corpus parse comparison is needed.

### Acceptance (corpus rung)

- `scripts/ab_2351_recipient_suppressions.py` after deploy: changed instruments == the
  10 above; the row ledger equals the probe's 148 identities; accession ledger rows
  updated (version); every `_current` row change listed and read.
- Smoke `/instruments/LEN` and `/instruments/LEN.B` ownership: Miller absent from `LEN`,
  present on `LEN.B`; BlackRock the reverse. Golden panel unchanged.
- Cross-source: Lennar 2026 proxy on EDGAR — Miller's Class B line 21,851,560.

### Accepted limits (ckpt-1, 49 findings, classified by the author)

Fixed in the rule above: designator from any row cell / the name cell (1, 2, 3, 9) →
class column by caption, name column excluded; non-common or `%` security in the class
caption or cell (4, 5, 8; Seneca's Series A/B preferred, found by this check); class vs
series namespace (14); lowest-id witness (42); loader returns names (26); `keep`
projected to `(instrument, accession)` (34); a row-only diff schedules the refresh (40);
ledger NOT NULL / FK / CHECKs + the class cell as evidence (43). Accepted, each
fail-to-withhold: rows vetoed by a non-common word in a name (11); bare / split / roman
classes (13); NULL shares (24); no witness (25); `Class A and B-1` (12, would need a
class cell that names two classes, and `designators` still returns A for it — accepted,
no such cell in the reach). Accepted as slice 3: name-key collision with an equal count
(18, 19); subsidiary letters (15); letter renames (16); selection ≠ historical
provenance and `_layout_rows` gaps (21, 22, 23 — the probe prints every one of the
withheld rows, and the A/B lists them); ledger staleness ≤ one run (32, 33); keep
semantics protect deletes only (35, 36, inherited); a missing body is permanent (38).
Per-row not per-accession (17): mitigated by requiring the Item 403 *Title of class*
column itself. Observation match by exact `holder_name` (27-31): the observation writer
copies the holdings name; in the reach accessions 384 of 392 def14a observations match,
and the 8 that do not are closed (`known_to` set) pre-normalisation names; ESOP matches
on `plan_name`. `_current` fallback to an older accession's row (41): measured in the
A/B, every `_current` change is listed. Stored percent not compared (20): the location
is the holder's own row, so the percent sits beside it. Parser comment (44): writer-side,
out of scope. Acceptance lists identities, not counts (45, 46); `instruments_failed`
must be empty (47). Tests widened (48, 49) below.

The holder's other-class lines stay missing (writer-side, out of scope). Unbound rows
keep today's fan-out. A parser artefact row named like a class (`SENEA` "Class B Common
Stock") is not a class-column cell and keeps.

## Security

No auth surface. One new outbound fetch class (SEC Archives instance XML) through the
existing rate-gated client with the configured SEC User-Agent.
