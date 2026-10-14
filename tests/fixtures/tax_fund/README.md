# Fund evidence fixtures (#3616 slice 3d-3c-1)

Captured on 2026-10-14 by `scripts/measure_3616_slice3d3c1_fund_identity.py` (run directory
`var/research/3616/slice3d3c1/20261014T151111Z/`, loop worktree), stored here byte for byte. Used by
`tests/test_tax_fund_evidence.py`.

| file | source | licence | sha256 |
|---|---|---|---|
| `ncen_0001410368-26-023904.xml` | SEC EDGAR, SPDR S&P 500 ETF Trust (CIK 884394) N-CEN for 2025-12-31, raw `primary_doc.xml` | US public record | `8a9f123f374408ac987506d7bf43b02eab4057ed2703438090cd1d6dfb993c0a` |
| `gleif_record_549300NZAMSJ8FXPQQ63.json` | `https://api.gleif.org/api/v1/lei-records/549300NZAMSJ8FXPQQ63` | CC0 | `baa021e4c0b4639e135d7e18f15eb8b0570b39d0bd18c062c587b6ba976ccdc9` |
| `gleif_isins_549300NZAMSJ8FXPQQ63.json` | the same record's `/isins?page[size]=200&page[number]=1` | CC0 | `fbce823415729666974f275afc32fcf51a03d76664eb6eeccc222db4180b0c72` |

Reproduce the hashes with `shasum -a 256 tests/fixtures/tax_fund/*.xml tests/fixtures/tax_fund/*.json`.

OpenFIGI bodies and HMRC list versions are synthetic, built inside the test module in the spec's fact 4 and fact 5
shapes.
