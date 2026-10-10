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
