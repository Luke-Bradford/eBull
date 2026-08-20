## When to use

When adding a new ETL source, modifying a source's wiring (manifest / parser / observation / current / endpoint), or auditing whether the existing sources are completely documented. This skill is the contract that pins per-source documentation as a load-bearing artifact rather than a nice-to-have.

## The rule

Every ETL source — SEC manifest, SEC ad-hoc, SEC bulk reference, FINRA caller-owned, broker REST — MUST have a per-source spec file at `docs/etl/sources/<source>.md` containing the 13 required sections from [docs/etl/sources/README.md § Template](../../../docs/etl/sources/README.md).

**Three independent gates enforce this:**

1. **Pre-push lint guard** — `scripts/check_etl_source_docs.sh` (wired into `.githooks/pre-push`). Fails if any source in `ManifestSource` Literal, ad-hoc list, or bulk-reference list is missing its spec file or missing a required section header. Runs in ~0.3s (three `uv run python -m scripts._etl_source_inventory` forks to read the canonical lists).
2. **CI lint** — same script invoked in `.github/workflows/ci.yml` so a push that bypasses the local hook still trips at the cloud gate.
3. **Pytest smoke** — `tests/smoke/test_etl_source_to_sink.py` parametrizes over every source: spec-file-exists / required-sections-present / ad-hoc-architectural-exception-section-present / registered-parser-exists. Runs in seconds; integration-marker keeps it cheap.

A source that lacks a spec file is, by definition, not done. The integrity-framework invariant from `data-engineer/SKILL.md` §write-through depends on this — without the per-source contract, future-agents repeat the Stage A→F sweep work to re-derive what should be looked up.

## How to add a new source

1. **Manifest source**: add the source string to `ManifestSource` Literal at `app/services/sec_manifest.py:107-126`. `MANIFEST_SOURCES` in `scripts/_etl_source_inventory.py` is derived via `get_args(ManifestSource)`, so the smoke test AND lint pick it up automatically; no test/lint edit needed.
2. **Non-manifest (ad-hoc / bulk-reference) source**: add the source string to `AD_HOC_SOURCES` or `BULK_REFERENCE_SOURCES` in `scripts/_etl_source_inventory.py` (single source of truth — the smoke test AND the lint script both read from there; do NOT edit either file directly).
3. Copy the template from `docs/etl/sources/README.md § Template` into `docs/etl/sources/<source>.md`.
4. Fill in all 13 sections. Every concrete claim MUST cite `path:line` from live code. The skill enforces grounding via section grep; the smoke test enforces section presence.
5. Add the manifest parser module under `app/services/manifest_parsers/`, named by form-family: a dedicated source gets its own `<source>.py` (e.g. `sec_424b.py`), while a family module can register several sources (`insider_345.py` registers sec_form3/sec_form4/sec_form5; `sec_13dg.py` registers sec_13d + sec_13g). Expose a `register()` and wire it into `register_all_parsers()` in `app/services/manifest_parsers/__init__.py`.
6. Run `bash scripts/check_etl_source_docs.sh` locally — must exit 0.
7. Run `uv run pytest tests/smoke/test_etl_source_to_sink.py -v` — every parametrize-case for the new source must pass.

Steps (6)–(7) just run the gates locally; the wiring in (1)–(5) is what's enforced. Skip a spec file or a required section and the lint guard blocks both the push (pre-push hook) and the merge (CI). Skip the parser registration in (5) and `tests/smoke` fails the push — note CI does NOT run pytest, so the pre-push hook is the only gate on parser registration.

## How to MODIFY an existing source

Same gates apply. The spec file is the contract; the code is the implementation. Any change to the code path that affects:

- The origin URL pattern (§1)
- The watermarking model (§2 — what column / key drives "what is new")
- The retry posture for any HTTP class (§3)
- The bootstrap or steady-state stage / cadence (§4 / §5)
- The manifest insert shape or `subject_type` (§6)
- The parser version / `requires_raw_payload` flag / output shape (§7)
- The observation table or `*_current` refresh helper (§8 / §9)
- The operator-visible endpoint (§10)

…requires a parallel update to the spec file in the same PR. The reviewer (bot + Codex) looks for spec-vs-code divergence as a top-shelf finding.

## Why this exists

Stream A's first 3 spec rounds (v1 → v2 → v2.1) burnt hours on hallucinated APIs because the per-source contract lived in scattered memos + skill paragraphs rather than in one authoritative doc per source. The 8-lens committee's #1 recommendation: "build a per-source source-to-sink doc that every reviewer can grep". This skill is the operationalisation.

Operator concern (`feedback_dont_stop_to_ask.md` parallel): every "where does X come from?" question the operator asks should resolve to a single doc — not 4 different memos + a grep across `app/services/`.

## Cross-references

- `docs/etl/sources/README.md` — index + template + cross-cutting invariants.
- `tests/smoke/test_etl_source_to_sink.py` — smoke gate (registry-invariant + section-presence + ad-hoc-§0 + parser-registration).
- `scripts/check_etl_source_docs.sh` — pre-push + CI lint guard.
- `data-engineer/SKILL.md` §write-through — the canonical write-through pattern referenced by every per-source spec §9.
- `data-engineer/etl-endpoint-coverage.md` — the 5-layer wiring matrix; per-source spec is the deep-dive that complements the matrix view.
- `data-sources/sec-edgar.md` + `data-sources/edgartools.md` — SEC-side gotchas referenced by per-source §13 sections.
