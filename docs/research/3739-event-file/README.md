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
`inputs/` is the pinned input. Acceptance dates are `acceptanceDateTime` (UTC) converted to New York dates, as step 1's
`acceptance_ny_date`.

```bash
PYTHONPATH=. uv run python scripts/build_3739_slice2.py calibrate --out calibration-k-g.json
PYTHONPATH=. uv run python scripts/build_3739_slice2.py extract --zip submissions.zip --out extract.jsonl.gz
PYTHONPATH=. uv run python scripts/build_3739_slice2.py universe --calibration calibration-k-g.json \
    --calibration-sha256 347f2fa1… --extract extract.jsonl.gz --extract-sha256 1bd3989b… --out u-pass1.csv
```

## Slice 2b (next): the candidate screens

§3.2's split, symbol-change and termination screens, with their candidate lists pinned here. Item 5.03 8-Ks come from
the extract above. The XBRL split-ratio facts and per-class cover counts are dimensional notes facts, so they need
the Financial Statement and Notes data sets: 2024q2 (for the cover before the window), 2025q3 and the 2026 months
after 2026-07 are not on this machine yet.
