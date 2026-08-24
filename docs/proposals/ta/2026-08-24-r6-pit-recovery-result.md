# R6 #2900 point-in-time recovery result

Verdict: **PASS — NEW IMMUTABLE READ PATH; OLD MUTABLE DB PATH REMAINS REJECTED**

The recovered path is the content-addressed R6 bundle, not a claim that the old `financial_periods` projection
became historical. Manifest SHA-256 is
`0b25af8c37b12437963f4681df7513397b83cd0ba7e37b15e1165b52f90d17d2`; payload SHA-256 is
`c2af906737be9dd14229657404b76bf0b0c1865db20ba33c9300374e25b17c81`.

## Population and provenance

| Formation close | Listed common classes | Same-filing share pairs | Neutral missing pairs | Ranking-input SHA-256 |
|---|---:|---:|---:|---|
| 2022-06-30 16:00 | 5,106 | 3,148 | 1,958 | `a6b609f1cd8cad18b945970507b07a1d7f149af97c9f747de3548e60b24dc021` |
| 2023-06-30 16:00 | 5,001 | 3,237 | 1,764 | `fc4b5e6628cd87ef045808830860f2c1c97e85beb7d6d4df895f1382746eff59` |
| 2024-06-28 16:00 | 4,778 | 3,208 | 1,570 | `5073cf9b1449825ab96adf470a3627fe23f39105527a26fd473b648afa2addff` |

The source is 19 official SEC FSDS quarters, retained SEC accession XML, the official submissions archive, and a
clean pinned Intrader mirror. The final cover census is
`f1cddd8fb3ebe4c95b51fc6de1ec408494d969d602c0f3f1116e1aa9fbed9382`; its raw 3.90 GB accession archive is
retained at `19d3d217b46a501875c7e8eaae8ea878313151157e1dc614f5838f2515e99d87`.

The loader rejects post-formation identity/share facts, a changed payload, a repointed manifest, symlinks,
duplicates, malformed share pairs and unsorted records. On every real formation, adding a later external ingest
left the full rank map unchanged. Rewriting the bundle was refused under the frozen manifest hash. Focused static
checks and 33 source/bundle tests passed before the declaration commit.

Pre-push review then tightened two latent cover-fallback cases. Full-population A/B was exactly 0 removed / 0 added
at every formation; payload records, formation census and all rank hashes remained equal. Correction 3 SHA-256 is
`f6bbac8492967c881473b30cef3d475252976d9bcea9a75eb6936a5f6c34b425`. The canonical hashes above are the
post-review evidence root.

Boundary: this PASS authorizes only reads from the exact bundle hash. Current mutable database projections remain
inadmissible for historical ranks. A new ingest must create a new manifest and cannot alter this result.
