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

## Slice 2b (next): the candidate screens

§3.2's split, symbol-change and termination screens, with their candidate lists pinned here. Item 5.03 8-Ks come from
the extract above. The XBRL split-ratio facts and per-class cover counts are dimensional notes facts, so they need
the Financial Statement and Notes data sets: 2024q2 (for the cover before the window), 2025q3 and the 2026 months
after 2026-07 are not on this machine yet.
