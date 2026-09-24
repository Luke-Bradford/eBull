# #3361 — dated series ↔ CIK linkage with an abstention census

Step 2 of `docs/proposals/ta/2026-09-24-selection-programme-v2.md` ("Security linkage"). Refs #2899, #2721,
#3360 (the fundamentals bundle this joins to), #3362 (termination). Codex ckpt-1 round 1 (67 findings,
2026-09-24) is folded in; see "Design history".

## Contract (deliberately narrow)
> For research price series S and decision date D: which SEC entity (CIK) did the evidence **public strictly
> before D** name as S's issuer, or a typed abstention.

- It links a **series** (one vendor file under one vendor symbol) to an **entity** (a CIK). It does not pick a
  primary class, merge series, split a series, classify instrument type (ETF, fund, ADR), or give a
  succession or delisting any economic meaning. Those belong to each arm's declaration (#2901 first) or to
  #3362.
- **Scope: the `icyDenev/Intrader` vendor only.** The symbol grammar in rule 3 is that vendor's. Every other
  vendor returns `vendor_out_of_scope` until it declares its own grammar.
- **What it cannot prove, stated up front:**
  - The vendor symbol is the vendor's capture-time label for the whole file, and a file may stitch two issuers
    together (ticker reuse). The linkage detects such stitches as successions; it cannot certify that a file
    is one security.
  - The legal filing obligation does not certify that the archive is complete.
  - A causal-equality test proves no look-ahead. It does not prove identity is correct (acceptance item 3
    measures that, partially).

## Why causal
A link at D that used evidence filed after D would condition on the issuer existing after D, i.e. on
survival. The programme forbids this ("the universe never depends on eventual survival or future mapping
success"). Every rule below uses only evidence public before D. There are no intervals, end dates or lifetime
summaries in the reader; those are future information at their start. The cost is abstention and stale
links around identity changes, and the census measures both.

## Source rules
- **Evidence: SEC Insider Transactions Data Sets** (`<YYYY>q<N>_form345.zip`, served from `2006q1`;
  `app/services/sec_bulk_download.py`). From `SUBMISSION.tsv`: `ACCESSION_NUMBER`, `DOCUMENT_TYPE`,
  `ISSUERCIK`, `ISSUERTRADINGSYMBOL`.
  - **Who files.** Exchange Act §16(a) requires Forms 3/4/5 from the insiders of every issuer with an equity
    class registered under §12. Investment Company Act §30(h) extends this to closed-end funds. Rule 3a12-3(b)
    (17 CFR 240.3a12-3, read 2026-09-24 via LII) exempts foreign private issuers. So the evidence population
    is §12 registrants and closed-end funds, and it does not depend on our broker list. Open-end ETFs and FPIs
    typically have no evidence and abstain (`no_recent_evidence`). The linkage does not classify them.
    - None of these citations is in sec-edgar skill §2.3 yet. `.claude/**` is not writable from the loop
      worktree, so the skill text is parked on #2403.
  - **What the symbol is.** `issuerTradingSymbol` is filer-entered and issuer-level: the Form 4 XML carries one
    symbol per filing (sec-edgar skill §2.3). It is evidence, never a label. An amendment (`4/A`) is a separate
    observation at its own acceptance; a symbol is not a restated value, so nothing is replaced.
- **Clock: acceptance, as in #3360.** `acceptanceDateTime` of the accession in the **issuer CIK's**
  `submissions` index (sec-edgar skill §7.4 pages, §7.8 UTC → New York) is used; public iff acceptance NY date
  < D. The `submissions.zip` is the #3360 bundle's snapshotted copy, pinned by its digest. `FILING_DATE` is
  not a clock. Measured over all 4,402,307 rows (see Premise measurements), its acceptance is later than
  `FILING_DATE` on 752 accessions, so `FILING_DATE` would call those filings public too early.
- **Delisting flag: `sec_form25_register`** (sec-edgar skill §2.6), `provision_class = 'equity_delisting'`.
  - A Form 25 **ends an exchange listing, not an issuer's identity** (Rule 12d2-2; the security can trade OTC
    afterwards), so it never ends or changes a link.
  - It surfaces as a flag, public iff `filed_date` < D. `filed_date` is the only date the register stores. For
    a 25-NSE the accession sits under the exchange CIK, so the issuer index gives no acceptance.
  - `resolved_symbol` came from a cover filed before the Form 25 (skill §2.6 trap 4). By construction its
    provenance accession is therefore public no later than the Form 25 itself.
- **Not evidence (deliberately):**
  - `submissions.zip` `tickers` and `company_tickers*.json`: today's mapping, which drops delisted names
    (skill trap 4).
  - eToro `instrument_id` and `instrument_cik_history`: today's broker universe. These are a cross-check
    only (acceptance item 3).
  - `sec_cover_12b_pairs`: a def14a-driven subset of about 1,000 covers.
- **Vendor symbol grammar.** No published grammar for the Intrader file names was found (repo layout
  `Data/Day/<symbol>`, headerless: research-price-corpus skill). The grammar is fixed **by construction** in
  rule 3 from the measured suffix census and frozen in the policy hash. It is an interpretation, and rule 3
  abstains on every shape it does not recognise.
- **Prior-art shape** (not a rule for our data): CRSP/Compustat CCM dated links with a link-type code.

## Premise measurements
Command:

```
PYTHONPATH=. uv run python scripts/measure_3361_symbol_evidence.py --submissions <#3360 bundle>/inputs/submissions.zip
```

Run 2026-09-24 over 81 quarters (`2006q1`–`2026q1`), 4,402,307 `SUBMISSION` rows, 27,388 distinct normalised
symbols. It measures raw spans inside each series' whole bar range, **not the causal reader**. The reader's
own counts are the census.

- **Series:** `research_price_series` has 0 rows with a CIK. Intrader has 22,879 series. By suffix: 19,994
  plain; 1,324 `P`, 579 `WS`, 351 `U`, 288 `W`, 113 `CL`, 111 `R`, 17 `WD`, and 104 single-letter or other
  groups.
- **Plain series vs evidence in their bar range:**

  | outcome | series |
  |---|---:|
  | one CIK | 8,170 |
  | several CIKs | 790 |
  | symbol seen only outside the range | 1,553 |
  | symbol never seen | 9,481 |

  Of the 790 with several CIKs, 267 are disjoint in time and 523 overlap. Examples found by hand, not
  validated:
  - `ACET`: CIK 2034 until 2019-02, then CIK 1720580 from 2020-09.
  - `AB`: the Holding and the LP both cite `AB`.
  - `ACM`: one stray 2023 filing under another CIK.
- **Clock:** acceptance NY date = `FILING_DATE` on 4,400,949 accessions, later on 752, earlier on 1. Not
  found: 76 accessions are missing from the issuer's index, and 529 belong to issuers whose submissions fail
  #3360 integrity.

## Construction rules
1. **Inputs (snapshotted, hashed, as in #3360):**
   - every Form 3/4/5 quarter zip, contiguous from `2006q1` to the last quarter present; a gap or a missing or
     malformed `SUBMISSION.tsv` header fails the BUILD;
   - the #3360 bundle's `submissions.zip`, by digest;
   - the sorted `sec_form25_register` equity-delisting rows read;
   - the sorted Intrader `research_price_series` rows read (`series_id, vendor_symbol, first_bar, last_bar`).

   TSV decoding is strict UTF-8; an undecodable file fails the build.

   `supported_through` = the earliest of: the last quarter's end, the #3360 submissions horizon, and the
   series capture end. Reads after it return `after_capture`.
2. **Observation admission.** Every `SUBMISSION` row gets exactly one outcome; the first match wins.

   Outcomes, in precedence order:
   1. `malformed_row`: wrong column count, or an accession not of the form `\d{10}-\d{2}-\d{6}`.
   2. `malformed_cik`: not 1–10 digits, or zero.
   3. `empty_symbol`: after trimming and upper-casing, the symbol is `""`, `NONE`, `N/A` or `NA`, tested
      **before** separator unification.
   4. `multi_symbol`: the symbol contains `,` `;` or whitespace between tokens.
   5. `issuer_integrity_excluded`: the issuer's submissions fail #3360 rule 5.
   6. `no_acceptance`: the accession is not in the issuer's index, or has no acceptance.
   7. `stored`.

   Identical duplicate rows collapse, and their multiplicity is counted. An accession whose rows disagree on
   (CIK, symbol, document type) is excluded whole as `accession_conflict`. That is a **snapshot-integrity**
   exclusion, applied to every D, exactly like #3360 rule 5, and stated as such: an answer is a function of
   (snapshot integrity state, public prefix).

   The ledger reconciles rows in = Σ outcomes.
3. **Series grammar (Intrader).** The vendor symbol is split on `_`.
   - `root` must be 1–5 of `[A-Z0-9]` and `.` is not allowed; otherwise the result is
     `unparsed_symbol_form`. The remaining tokens `T` decide:
     - `T` empty → plain.
     - `T` = one single letter **not** in `{P, W, U, R}` → class `c`.
     - The first token of `T` in `{P, WS, W, U, R, WD, CL}` → abstain whole as
       `non_common_symbol_form:<token>`. This also covers `P_A_CL`, `R_W` and the like.
     - Anything else → `unparsed_symbol_form`.
   - Exchange test issues (`universe_selection.EXCHANGE_TEST_ISSUE_SYMBOLS`, the #2912 rule) →
     `vendor_test_symbol`.
   - **Collision guard:** two in-scope series whose match keys (rule 4) coincide are both abstained whole as
     `vendor_symbol_collision`. The census counts them.
4. **Matching.** Evidence symbol `e` is separator-unified (`. - / _ space` → `.`).
   - A plain series with root `r` matches `e = r`, or `e = r + "Q"` (**Q alias**, research-price-corpus
     skill §bankruptcy suffix). By construction the alias applies only when `len(r) = 4`,
     which is the shape of every Q example in that skill (`BBBYQ`, `YELLQ`, `SRNEQ`). Other root lengths
     get no alias, and the census counts what this rule misses as `never_seen`. The observation is marked `q_alias`, and if another in-scope series has symbol
     `r + "Q"`, the collision guard applies.
   - A class series `r_c` matches only `e = r.c` (from `BF.B`, `BF-B`, `BF/B`, `BF B`, `BF_B`). It never
     matches `BFB` or `r`, and a plain series never matches `r.c`.
5. **Link at D.** Let W be S's matching stored observations with acceptance NY date in `[D − 730 days, D)`.
   The window is fixed **by construction**: 730 days is the #3360 census population window, so "recent" means
   the same thing on both sides of the join. It is frozen in the policy. There is no insider-filing cadence
   rule to derive it from.
   - W empty → `no_recent_evidence`, with sub-reason `never_seen` when no matching observation precedes D.
   - One CIK in W → `linked(cik, single_cik)`.
   - Several CIKs in W that form a **clean succession** (ordered by first observation in W, every observation
     of each precedes every observation of the next) → `linked(last cik, succession)`.
   - Any interleaving → `conflicting_evidence`.
   - There is no majority, count or recency weighting; none has a source rule. The known costs are measured
     by the census, not argued away:
     - one stray later filing flips a link;
     - a stale predecessor can persist for up to 730 days after an unobserved change;
     - a conflict expires silently when its observations age out of W.
6. **Result algebra.** Precedence, first match wins:
   1. `series_not_in_bundle`
   2. `vendor_out_of_scope`
   3. `after_capture`
   4. `outside_series` (D outside `[first_bar, last_bar]`; bar presence ON D is the consumer's read)
   5. `vendor_test_symbol`
   6. `unparsed_symbol_form`
   7. `non_common_symbol_form:<token>`
   8. `vendor_symbol_collision`
   9. `no_recent_evidence[:never_seen]`
   10. `conflicting_evidence`
   11. `linked(cik, basis)`

   Every result from `no_recent_evidence` on carries:
   - the W observations: accession, CIK, acceptance, `q_alias`;
   - the flag `form25` = the register rows with `filed_date < D` whose `issuer_cik` is any CIK in W, each
     marked `symbol_match` / `symbol_other` / `symbol_null`. Informational only.
7. **Reader** `link_as_of(series_id, D)` returns the rule-6 result. Cross-series facts (two series linked to
   one CIK) are not reader output; the census computes them.

## Artefact
Content-addressed like #3360 (`r6_pit_bundle.read_verified_document`, exclusive publish, policy-bound
loader):
- `<bundle>/inputs/` holds the snapshotted zips and row dumps.
- `<bundle>/series/<series_id>.json` holds the series row, its grammar result and its matching stored
  observations in total order (acceptance, accession, CIK).
- `<bundle>/manifest.json` holds:
  - `POLICY`: sha over the constants (grammar tokens, window, placeholders, precedence), the builder, the
    reader, `pit_fundamentals.py`, `universe_selection.py` (test-issue list), `r6_pit_bundle.py`, Python and
    `tzdata`;
  - the input digests, including the #3360 bundle manifest digest;
  - `supported_through`, the ledger, and the per-series digests.

## Census (descriptive; before any strategy look)
- **Formations:** the #3360 grid (the last NYSE session of each June, 2011–2024).
- **Population at D:** in-scope series with `first_bar ≤ D ≤ last_bar`. They split into:
  - a bar ON D (the primary counts);
  - no bar ON D, counted separately: halts and gaps are exactly the distress-linked missingness in question.

  Bars are read from `research_price_daily`, and the rows read are hashed into the evidence manifest.
- **Per formation, cross-tabulated** result reason (rule 6) × traded-value decile × vendor capture status
  (the issue's year × reason × size × outcome):
  - **Traded-value decile.** This measures **liquidity, not company size**. It is the median over the last
    21 bars before D of close × volume, as stored; bars with NULL or zero volume are skipped. Fewer than 21
    usable bars → `traded_value_unavailable`. Rank over (value, series_id), decile ⌊10·rank/n⌋.
  - **Vendor capture status.** `runs_to_capture` means `last_bar` ≥ the vendor capture end minus 7 days;
    otherwise `ends_before_capture`. This is not an economic outcome.
  - **Form 25.** For linked results, whether the linked CIK has a register row in (D, D+730], with #3360's
    observed-span rule (`unobserved_horizon` outside the register span).
- **Counts:**
  - both series and distinct linked CIKs;
  - CIKs linked by more than one series at D (classes or vendor duplicates);
  - `q_alias` links;
  - `vendor_symbol_collision`;
  - `form25` flag counts by match kind.
- **Identity-quality descriptives (full population, no subset):**
  - the age distribution of the latest supporting observation for linked results;
  - successions that **revert** (the predecessor CIK reappears within 730 days after the switch);
  - conflicts that later resolve to a link by expiry alone.
- **Join to #3360:** linked CIKs at D that are, or are not, members of the #3360 census population; and
  #3360 members at D with no linked series. Both directions are reported.

## Acceptance (this ticket)
1. **Pure fixtures:**
   - single CIK;
   - clean succession, and succession later reverted (the reader follows the prefix);
   - an interleaved stray filing → `conflicting_evidence`, which expires after 730 days;
   - acceptance on D is not public at D;
   - acceptance later than `FILING_DATE` → public by acceptance;
   - `no_acceptance` and `accession_conflict` are ledgered and not used;
   - the `N/A` placeholder;
   - a `q_alias` with a 4-letter root, and none with a 3-letter root;
   - class `BF_B` matches `BF.B` and `BF-B`, not `BFB` or `BF`;
   - `P_A_CL` / `WS` / `R_W` abstain whole;
   - an unrecognised shape → `unparsed_symbol_form`;
   - `vendor_symbol_collision`;
   - a Form 25 is flagged only once `filed_date` < D and never changes the link;
   - `outside_series`, `after_capture`, `vendor_out_of_scope`;
   - ledger reconciliation;
   - loader refusals (policy, digest, path), and rebuild determinism.
2. **Causal reference** (full population, as #3360 item 2):
   - **Decision dates.** For every series, test each D in {every matching observation's acceptance NY date
     + 1, the same + 731 (window expiry), every flagged Form 25 `filed_date` + 1, `first_bar`,
     `last_bar` + 1, one day before the first observation}. For a series with no observations, test
     {`first_bar`, `last_bar` + 1}. All are calendar dates; no session calendar applies.
   - **Independent path.** Rebuild from the raw inputs filtered to acceptance < D first, then apply rules 2–6
     independently (without calling the reader).
   - Assert the **entire** rule-6 result is equal, flags and observations included.
3. **Cross-checks** (reported; subsets, so they cannot establish safety on the rest of the population). Each is
   reported as agree / disagree / not comparable, with denominators and every disagreement listed:
   - (a) eToro: series with `instrument_id` whose `instrument_cik_history` interval contains `last_bar` —
     does the link at `last_bar` agree with that CIK?
   - (b) Form 25: series with `delisting_provision` — does a link built **without** the Form 25 input agree
     with `issuer_cik` on `filed_date`? The existing series↔Form 25 association is itself symbol-based
     (`symbol_exact`), so this is an independent **source**, not an independent association.
4. Census evidence manifest written.
5. Registry (`app/services/research_point_in_time.py`): the cells citing #3361 stay **fail**. This ticket
   adds the linkage's clock and causal evidence to the reasons. **Nothing becomes admissible.** How an arm
   consumes abstentions and `succession` is that arm's declaration.

## Design history
Round 1 (67 findings) changed the design as follows:
- **Clock:** `FILING_DATE` was replaced by the issuer-index acceptance clock, after measurement (752 later).
- **Form 25:** no longer terminates a link; delisting ≠ identity. It is a public-dated flag.
- **Removed from the reader:** interval compression (look-ahead at its start) and `shared_cik`
  (cross-series).
- **Scope:** restricted to the Intrader vendor.
- **Grammar:** now a closed rule that abstains on unknown shapes.
- **Q alias:** restricted to 4-letter roots.
- **Collision guard added.**
- **`first_bar` clip on evidence dropped.**
- **Snapshot-integrity exception declared.**
- **Census:** no longer promises a fund/ETF/FPI split it has no source for; traded value is renamed to
  liquidity; capture status is not called an outcome; the no-bar-on-D population is added; the
  identity-quality descriptives are full-population.
- **Causal test:** covers expiries, Form 25 dates and bar bounds.
- **Fixed:** the `N/A` normalisation bug in the measurement script.

Kept and stated as costs, because no source rule exists for an alternative: the 730-day window and
any-interleaving-abstains.
