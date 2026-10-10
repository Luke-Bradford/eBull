# #3739 stage-C event file

Files frozen under `docs/research/2026-10-09-3739-stage-c-panel.md` §3.6, each pinned by sha256 in `manifest.json`.
No file here was built from a stage-C price.

## Slice 2a: K, G and U pass 1 (2026-10-09)

Built by `scripts/build_3739_slice2.py` at the commit that adds this section.

**K = 1,500** (§2). Over the 636 stage-B pairs (M0, M0 + h), h = 1..24, both formations on or before 2024-07-31,
the largest share of the top 1,000's final ME held by issuers ranked above K at M0:

| K | 1,500 | 2,000 | 2,500 | 3,000 |
|---|---:|---:|---:|---:|
| worst pair | 0.221% | 0.076% | 0.034% | 0.011% |

The worst pair for K = 1,500 is 2022-05-31 → 2024-03-31 (h = 22). Every pair is in `calibration-k-g.json`.

**G** (§9): the 99.9th percentile of max(r, 1/r) of a security's final-ME ratio over g formations, on every
admitted security of stages A and B (formations 2014-09-30 .. 2024-07-31). Selected gaps (all 24 in the file):

| g | 1 | 6 | 12 | 24 |
|---|---:|---:|---:|---:|
| G(g) | 7.98 | 21.06 | 35.69 | 76.96 |
| pairs | 364,196 | 333,569 | 300,378 | 241,955 |

Percentiles interpolate linearly between ranks (Postgres `percentile_cont`).

**U pass 1** (§2, base formation F = 2024-07-31): 3,943 issuers in `u-pass1.csv`.

| basis | issuers | source |
|---|---:|---|
| incumbent (issuer rank ≤ 1,500 at F) | 1,500 | stage-B artefact |
| entrant | 1,943 | EDGAR `submissions.zip` |
| terminated (register event filed 2024-08-01 .. 2026-10-08) | 667 (685 events) | `sec_form25_common_equity_delistings` |

167 issuers carry more than one basis. Step 1's step 5 excludes a CIK with more than one priced security, so 1,500
issuers hold the first 1,500 ranks.

Entrants by qualifying filing, accepted 2024-05-01 .. 2026-07-31 (New York date), issuers with no admitted security
at F (filings, issuers): 8-A12B 3,746 / 1,829; 10-12B 35 / 35; 8-K12B 34 / 34; 8-K with Item 5.06 101 / 97. U bounds
adjudication effort, and an issuer with no split candidate costs nothing to adjudicate.

**Inputs.** `submissions.zip` downloaded 2026-10-09 (`Last-Modified` 2026-10-09 04:40 GMT, sha256 in the manifest),
main files and the 5,394 overflow pages. EDGAR rebuilds it nightly, so the zip cannot be re-fetched; the extract in
`inputs/` is the pinned input. The register is read from `inputs/form25-register-through-2026-10-08.csv`, written
from the live view with the slice-1 harvest date as its cutoff, so a later harvest cannot move U. Acceptance dates are `acceptanceDateTime` (UTC) converted to New York dates, as step 1's
`acceptance_ny_date`.

```bash
PYTHONPATH=. uv run python scripts/build_3739_slice2.py calibrate --out calibration-k-g.json
PYTHONPATH=. uv run python scripts/build_3739_slice2.py extract --zip submissions.zip --out extract.jsonl.gz
PYTHONPATH=. uv run python scripts/build_3739_slice2.py register --through 2026-10-08 --out register.csv \
    --view-sha256 2667138ea99a5304b9c74c517aaf1c968bebe7d921eed7338ea2278a0ec90a70
PYTHONPATH=. uv run python scripts/build_3739_slice2.py universe \
    --calibration calibration-k-g.json \
    --calibration-sha256 347f2fa155432c1ae104fc71adeccb4d48353d362ff02ffc8afaa8c65ac8732d \
    --extract inputs/submissions-2026-10-09-extract.jsonl.gz \
    --extract-sha256 1bd3989bb8b59f63d1e3bb137335b74cfa99309c401d2196424c79e10898d74d \
    --register inputs/form25-register-through-2026-10-08.csv \
    --register-sha256 68bc4062b9071c1c6da33d403aa663ec2ca2ea55d4785da0462e628e2111e489 --out u-pass1.csv
```

`extract` and `register` read sources that change: EDGAR rebuilds the zip nightly, and the register gains or corrects
rows. Rerunning them reproduces the committed inputs only while those sources are unchanged. Replaying U means running
`universe` on the committed `inputs/`.

## Slice 2b (2026-10-10): the candidate screens

Built by `scripts/build_3739_slice2b.py` at the commit that adds this section. §3.2's screens run over every issuer
in the source data (§2), event window 2024-07-01 .. 2026-09-30, the last day every input covers. No price is read.

| list | candidates | issuers | of which in U pass 1 |
|---|---:|---:|---:|
| `candidates-split.csv.gz` (gzip, `mtime` 0) | 16,666 | 4,518 | 5,617 on 2,014 issuers |
| `candidates-symbol-change.csv` | 563 issuers | 563 | |
| `candidates-termination.csv` | 685 events | 667 | 667 |

Split candidates by screen: XBRL ratio 5,426; cover-count jump 4,456; Item 5.03 6,784. Adjudication (slice 4) reads
the U rows; the rest stay pinned for pass 2's closure.

**Split screen inputs.** `StockholdersEquityNoteStockSplitConversionRatio1` and the per-class cover counts are notes
and dimensional facts, absent from plain FSDS and companyfacts (`sec-edgar.md` §7.17, §7.18), so they are read from
the Financial Statement and Notes data sets: quarterly 2024q1 .. 2025q3, monthly 2025-10 .. 2026-09 (DERA
consolidated 2025-07 .. 09 into 2025q3 on 2026-10-07; the build refuses archives that share a filing). 92,123
facts from 69,143 filings; 46 facts have no acceptance in `submissions.zip` and are dropped.

- **Dates.** `num.ddate` is rounded to the nearest month end (FSNDS readme). The reported date is `ddate − datp`
  days; the sign was checked against covers that state their date (Apple 10-Q of 2024-08-01, "as of July 19, 2024":
  ddate 20240731, datp 12; NVIDIA 10-Q of 2024-08-28, "as of August 23, 2024": ddate 20240831, datp 8). Intervals
  use reported dates, so a split near a month end is not rounded out of its interval.
- **Class axis.** The data sets spell `us-gaap:StatementClassOfStockAxis` as `ClassOfStock` in `segments` (the
  spec's §3.2 is corrected to say so). A cover fact is read under the default dimension or exactly one
  `ClassOfStock` member, from the registrant only (`coreg` empty), unit `shares`.
- **Cover ledger.** 2,866 covers replaced by a later amendment of the same period; 1,452 facts not read (co-registrant,
  other dimension or unit); 1,564 non-positive; 2,099 class series whose first cover is dated in the window, so a
  jump from before it cannot be seen (new registrants, and classes first reported per member).

**Symbol-change inputs.** #3361 rule 2 (unchanged code) over the insider data sets 2022q3 .. 2026q3: 804,623 stored
observations (917 issuer-integrity exclusions, 68 with no acceptance). The extract keeps every observation accepted
from 2024-07-01 and, per (CIK, symbol), the latest in the 730 days before it, which is the symbol in force at the
window's start. 2026q2 and 2026q3 are served from `/files/datastandardsinnovation/` (the app's bulk download misses
them: #3747).

**Spot checks** (screens recall known actions; adjudication decides): O'Reilly 15:1 (June 2025) is raised by all three
split screens, the cover interval 2025-05-05 .. 2025-08-04, ratio 14.89; Fastenal 2:1, Interactive Brokers 4:1
(both classes), Broadcom 10:1, Super Micro 10:1 and Lam Research 10:1 by the XBRL and cover screens; Block's SQ → XYZ
change is a symbol-change candidate.

```bash
PYTHONPATH=. uv run python scripts/build_3739_slice2b.py notes-extract --submissions submissions.zip \
    --out notes.jsonl.gz fsnds_2024q1_notes.zip ... fsnds_2026_09_notes.zip   # the 19 archives in the manifest
PYTHONPATH=. uv run python scripts/build_3739_slice2b.py insider-extract --submissions submissions.zip \
    --out observations.jsonl.gz insider_2022q3.zip ... insider_2026q3.zip     # the 17 quarters in the manifest
PYTHONPATH=. uv run python scripts/build_3739_slice2b.py screens --through 2026-09-30 \
    --notes inputs/fsnds-notes-2024q1-2026-09-extract.jsonl.gz \
    --notes-sha256 4b29dcd6d22e3e1642148bb74c6bd2113d55635b43cf7142e46f08914ec79b5c \
    --observations inputs/insider-observations-2022q3-2026q3.jsonl.gz \
    --observations-sha256 67a21589066fe75c16e6dc02c60c4c3a9a4d4a9d0462ee1874f216b96737fa5e \
    --extract inputs/submissions-2026-10-09-extract.jsonl.gz \
    --extract-sha256 1bd3989bb8b59f63d1e3bb137335b74cfa99309c401d2196424c79e10898d74d \
    --register inputs/form25-register-through-2026-10-08.csv \
    --register-sha256 68bc4062b9071c1c6da33d403aa663ec2ca2ea55d4785da0462e628e2111e489 --out-dir .
```

Both extracts read the 2026-10-09 `submissions.zip` (the slice-2a pin), so they replay only from the committed
`inputs/`; `screens` replays from those.

## Slice 3a (2026-10-10): the replica's U and split candidates

Built by `scripts/build_3739_slice3a.py` at the commit that adds this section, under `replica-2022/`. §3.4's replica
is §2's pass 1 items 1–2 with F = 2022-07-31 and events effective 2022-08-01 .. 2024-07-31; it measures split screens
only, so no symbol-change or termination list is pinned and no insider data are read. Slice 2a's K = 1,500 is reused
(K is fixed once by construction, §2). The screens are slice 2b's code with the window passed in; with the defaults
they replay stage C's three pinned lists byte for byte. No price is read.

**Replica U pass 1:** 2,791 issuers: 1,500 incumbents (stage-B artefact, formation 2022-07-31) and 1,291 entrants
(qualifying filing accepted 2022-05-01 .. 2024-07-31). The filing extract starts 2022-04-01, the first day of the
month before F − 92 days, as slice 2a's 2024-04-01 does for stage C.

**Replica split candidates:** 15,512 on 4,796 issuers (XBRL ratio 4,678; cover-count jump 3,650; Item 5.03 7,184).
4,214 rows on 1,565 U issuers (Item 5.03 2,396; XBRL 1,168; cover 650) are what replica adjudication reads.

- **Evidence cutoff = 2024-07-31,** the last event date, as the spec states the window. A cover or XBRL fact
  accepted after it is not read (5,936 covers), so a split in the window's last weeks is raised only by an earlier
  filing or its Item 5.03 8-K. This can only lower measured recall: a pass holds under any later cutoff.
- **Notes inputs:** FSNDS quarterly 2022q1 .. 2024q3, 99,433 facts from 75,970 filings, 57 with no acceptance in
  `submissions.zip` (dropped). 2022q1 gives the covers before the window. Each archive was reduced alone
  (`notes-reduce`) and deleted, for disk; the reduced file heads carry each archive's sha256, all in the manifest.
  The 2024q1 .. q3 digests equal slice 2b's.
- **Cover ledger:** 3,353 covers replaced by an amendment; 1,265 not read; 1,190 non-positive; 4 ambiguous filings;
  1,213 class series whose first cover is dated in the window.
- **Spot checks** (screens raise, adjudication decides): Tesla 3:1 (2022-08), Palo Alto 3:1 (2022-09), Walmart 3:1
  (2024-02), Chipotle 50:1 (2024-06), NVIDIA 10:1 (2024-06) and Broadcom 10:1 (2024-07) are all raised. NVIDIA's and
  Broadcom's only non-5.03 candidates are XBRL facts whose interval ends at the 10-Q acceptance, before the effective
  date (the spec's accepted precision limit for XBRL intervals); adjudication takes the date from the filing.

```bash
PYTHONPATH=. uv run python scripts/build_3739_slice3a.py extract --replica 2022 --zip submissions.zip \
    --out replica-2022/inputs/submissions-2026-10-09-extract-from-2022-04-01.jsonl.gz
PYTHONPATH=. uv run python scripts/build_3739_slice3a.py notes-reduce --out 2022q1.reduced.jsonl.gz \
    fsnds_2022q1_notes.zip                                                     # once per archive in the manifest
PYTHONPATH=. uv run python scripts/build_3739_slice3a.py notes-extract --submissions submissions.zip \
    --out replica-2022/inputs/fsnds-notes-2022q1-2024q3-extract.jsonl.gz 2022q1.reduced.jsonl.gz ...
PYTHONPATH=. uv run python scripts/build_3739_slice3a.py universe --replica 2022 \
    --calibration calibration-k-g.json \
    --calibration-sha256 347f2fa155432c1ae104fc71adeccb4d48353d362ff02ffc8afaa8c65ac8732d \
    --extract replica-2022/inputs/submissions-2026-10-09-extract-from-2022-04-01.jsonl.gz \
    --extract-sha256 be0bdda721dcc4efbca7784762a83495d21062fc89e40b3a7bfebcd9e369fe4b --out replica-2022/u-pass1.csv
PYTHONPATH=. uv run python scripts/build_3739_slice3a.py screens --replica 2022 \
    --notes replica-2022/inputs/fsnds-notes-2022q1-2024q3-extract.jsonl.gz \
    --notes-sha256 5b3bbfb608f58edc7a5bdf7c19b85805b643a05afe6e21db3d3f66a7558c6bd4 \
    --extract replica-2022/inputs/submissions-2026-10-09-extract-from-2022-04-01.jsonl.gz \
    --extract-sha256 be0bdda721dcc4efbca7784762a83495d21062fc89e40b3a7bfebcd9e369fe4b --out-dir replica-2022
```

## Slice 3b (2026-10-10): the checker and the replica's comparison set

Built by `scripts/build_3739_slice3b.py` at the commit that adds this section.

**Comparison set** (§3.4), pinned before any replica adjudication in `replica-2022/comparison-intrader-stamps.csv`:
every `icyDenev/Intrader` `split_factor` stamp (≠ 1) dated 2022-08-01 .. 2024-07-31, with the #3361 linkage
(manifest `1738bc39…`) as of the stamp date. Rows with `in_u = 1` are the comparison set.

| | stamps | series or issuers |
|---|---:|---:|
| all stamps in the window | 747 | 654 series |
| linked to a CIK | 420 | |
| **linked to a replica-U CIK (the comparison set)** | **42** | **38 issuers** (16 stamps < 1) |

The 327 unlinked stamps are the linkage's typed abstentions: no evidence in 730 days 280 (235 never seen), symbol
collision 46, unparsed form 1.

**The 2022 replica fails §3.4's count bar whatever adjudication finds.** Adjudication writes records only from
candidates (§3.2–3.3), and two comparison stamps have no split candidate on their CIK anywhere in the window:
Commerce Bancshares (`CBSH`, 2022-12-01, factor 1.05) and Southern Copper (`SCCO`, 2024-05-07, factor 1.0104). Both
factors lie inside the cover screen's [0.8, 1.25] band, and neither issuer has an XBRL ratio fact or an Item 5.03 8-K
in the window. So count recall is at most 40 / 42 = 95.2%, below 97%. Two more stamps have candidates on their CIK
but none whose interval covers the stamp date: `UHAL` 2022-11-10 (factor 10) and `WRB` 2024-07-11 (1.5).
(Corrected in slice 3c: this sentence first also listed `AMC` 2023-08-23, which Item 5.03 candidate `S004986`,
[2023-08-14, 2023-11-14], covers.) The per-stamp coverage table is
reproduced by joining the two pinned files on CIK. What each of these actions was is adjudication's question, not
this measurement's.

**The checker** (§3.3), `check --type <record type>`, over an event-file record CSV:

- **Fetch and mirror.** Each evidence item's document and its filing's `-index-headers.html` are fetched from
  `/Archives/edgar/data/<CIK>/<accession>/`, under the row's CIK or its `counterparty_cik`, and kept gzipped
  (`mtime` 0) in the mirror. The output records the sha256 and length of the bytes as served. A rerun reads the
  mirror and fetches nothing. Only a served body is mirrored: index headers must carry `ACCEPTANCE-DATETIME`, and a
  document must be non-empty and not an SEC refusal page, so a bad 200 is fetched again on the next run.
- **Acceptance.** The item's `acceptance` must equal the headers' `ACCEPTANCE-DATETIME` (14 digits, EDGAR's Eastern
  clock, as served).
- **Quote.** At most 300 characters, and it must occur in the document's text with all whitespace removed from both.
  The text drops comments, scripts and styles, turns every tag into a space, unescapes entities and applies NFKC.
  This is a rule by construction, because no source rule says how an EDGAR HTML document becomes text.
- **Fields.** Read off the row's quotes. A date counts in a role only when it sits within 40 characters of that
  role's words in the same quote:
  - **split:** the ratio, read as "a-for-b" in numerals or number words, or as "a:b" (never a clock time), within
    40 characters of a split's own words: "split", "stock dividend", "share distribution", "share consolidation",
    "reclassification" or "combination of the outstanding shares". A bare "distribution" or "combination" does not
    count, because those words also name cash dividends and business combinations.
  - **split effective date:** quoted beside adjusted-basis or ex-date words (§1: the first session on the adjusted
    basis). Otherwise, the record date (beside "record") and the payable date (beside "payable", "paid",
    "distributed", "distribution date" or "issued") must be quoted, and the effective date must be FINRA 11140's date
    from them: the NYSE session after payable for a distribution of 25% or more, else the record date (superseded by
    Amendment 1: before 2024-05-28, the business day before the record date; slice 3d changes the code). A reverse
    split has no such fallback (§1 source rules).
  - **split class:** named by title or symbol in an action quote (one that also states the ratio or the effective
    date), or the issuer's own evidence document has a cover that tags exactly one 12(b) class (one
    `dei:Security12bTitle` + `dei:TradingSymbol` context, Reg S-K Item 601(b)(104)) and that class is the row's. A
    12(b) row quoted from a multi-class cover, or from a counterparty's cover, does not identify the class.
  - **symbol_change:** both symbols and the date.
  - **first_trade:** the date beside first-session words.
  - **termination_end:** the date beside the words of its basis (`last_trading_day`, `suspended` or
    `merger_closing`), unless the row is `not_stated`.

  The word lists and the 40-character reach are by construction. They show a quote gives the date its role, and the
  adjudication log records why the quotes describe one completed action. A `cancelled` row states no fields.
- **Smoke run:** NVIDIA's 10:1 split row (8-K `0001045810-24-000144`, effective 2024-06-10, its single-class cover
  identifying the class) passed, and a rerun from the mirror fetched nothing.

```bash
PYTHONPATH=. uv run python scripts/build_3739_slice3b.py comparison --replica 2022 \
    --u replica-2022/u-pass1.csv --u-sha256 9794f71f850a4fb70ab32b0ef381ca6dc8dc633cba515929de51010baa54c57b \
    --out replica-2022/comparison-intrader-stamps.csv
PYTHONPATH=. uv run python scripts/build_3739_slice3b.py check --type split --records <records.csv> \
    --mirror <dir> --out <check.jsonl>
```

Next: §3.4's one screen revision, then the replica with F = 2020-07-31 (the decision and its reasons are on #3739).

## Slice 3c (2026-10-10): Amendment 1, the revision after the 2022 replica's fail

The spec's §"Amendment 1" fixes the one revision §3.4 allows, before anything of the 2020 replica is produced: a
stock-dividend fact screen; periodic-report screens read 203 days past the event window (stage C's split window
ends 2026-10-01 and it builds no earlier than 2027-04-22); the cover band's ends raise; the checker reads percentage
and additional-share ratios; the FINRA 11140(b)(1) fallback is versioned by date; unmatched comparison stamps are read
and listed but stay in both denominators; the count bar's power is stated. The record taxonomy and the comparison set are
unchanged; the split event window, unbounded before, now ends 2026-10-01. Its measurements read only the
pinned replica-2022 inputs:

| measurement | figure |
|---|---:|
| days between distinct acceptance dates of consecutive covers of a (CIK, class) series (69,841 gaps, 9,522 series): p50 / p90 / p99 / p99.9 | 91 / 126 / 197 / 431 |
| comparison stamps with a covering candidate on their CIK (CIK-level interval coverage, not a class, ratio or date match), covers and ratio facts read through 2024-07-31 → 2024-09-30 | 38 → 39 of 42 (`WRB` gains a cover-count candidate, ratio 1.48849) |

```bash
R=docs/research/3739-event-file/replica-2022
N=$R/inputs/fsnds-notes-2022q1-2024q3-extract.jsonl.gz
PYTHONPATH=. uv run python scripts/build_3739_slice3c.py gaps --notes $N
for through in 2024-07-31 2024-09-30; do
  PYTHONPATH=. uv run python scripts/build_3739_slice3c.py coverage --evidence-through $through --notes $N \
      --extract $R/inputs/submissions-2026-10-09-extract-from-2022-04-01.jsonl.gz \
      --comparison $R/comparison-intrader-stamps.csv
done
```

Each command prints its inputs' sha256 beside the figures; they equal the pins above. Next (slice 3d): the code
changes (stock-dividend screen, evidence horizon, closed band, checker ratio forms and versioned fallback, the
unmatched-stamp listing), then the 2020 replica's U and comparison size n with the power table,
pinned before its adjudication.
