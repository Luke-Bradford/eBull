# R6 point-in-time spine implementation plan (#2900)

Status: draft. The declaration in
`docs/proposals/ta/2026-08-23-r6-point-in-time-spine-declaration.md` governs;
this plan cannot weaken it.

1. **Freeze the question before implementation.** Change the declaration status
   to `FROZEN BEFORE LEAK TEST`, compute its SHA-256, and commit only the
   declaration and this plan. Record that commit; do not run the verifier.

2. **Implement the red fail-closed contract.** Add
   `tests/test_research_point_in_time.py` first. It imports the not-yet-existing
   `RankingFamily`, `R6RankingIdentity`, `R6RankingRequest`, `FIELD_REGISTRY`,
   `PROBE_MATRIX`, `REGISTRY_VERSION`, `PointInTimeUnavailableError`, and
   `execute_r6_ranking`; pins the exact issue/family and probe sets; asserts
   every current identity refuses; and covers empty/unknown/under-declared,
   weekend (`2020-01-18`), holiday (`2020-01-20`), same-date policy, and
   post-capture price requests. Run and expect collection failure:
   `uv run pytest tests/test_research_point_in_time.py`.

3. **Make the pure contract green.** Add
   `app/services/research_point_in_time.py`. `R6RankingIdentity` owns its
   immutable, non-empty family set and optional price-corpus bound; no callback
   or caller family override exists. Validate NYSE sessions through
   `app.services.market_calendar.us_market_status`; keep the internal reader map
   empty; canonicalize registry/probe content using sorted-key compact JSON and
   SHA-256. Add an AST test that scans `app/services/r6_*.py` and fails on
   governed table literals outside this module. Run:
   `uv run pytest tests/test_research_point_in_time.py`.

4. **Implement non-vacuous probes and the rollback test.** Add
   `tests/test_2900_point_in_time_db.py` with an isolated test-DB behavioral
   version of the fixed sentinel insert/post-D control/same-key overwrite and
   rollback. Add `scripts/verify_2900_point_in_time.py` with the same functions.
   Each declared probe ID has one function that asserts its runtime symbol is
   importable, its schema columns through `information_schema`, and either an
   exact AST construct or behavioral mutation. Anchor counts are declared per
   probe and zero/unexpected multiples fail. `derive_verdict` fails unless every
   matrix cell cites successful probes and at least one condition fails per
   family. Run:
   `uv run pytest tests/test_2900_point_in_time_db.py`.

5. **Pin isolation and output.** The dev verifier calls
   `scripts._dev_guard.assert_dev_environment`, verifies the declaration hash,
   requires `git status --porcelain` empty, acquires the declared advisory lock,
   refuses a pre-existing sentinel, and scopes all tuple hashes to it. It uses
   one outer transaction with `finally: conn.rollback()` and then a new
   connection for absence proof. It emits one canonical JSON document to
   stdout containing schema version, execution commit, declaration commit/hash,
   decision date, registry/probe matrices, per-probe anchor counts and source
   hashes, exact censuses, before/control/overwrite hashes, first unequal tuple,
   rollback proof and derived verdict. `--format markdown` renders the result
   document from the same typed evidence object. Tests pin both schemas.

6. **Commit executable evidence before looking.** Put the frozen declaration
   hash/commit constants into the verifier, run Ruff format on the new Python
   files, run the two focused test commands above, and commit implementation and
   tests. Require a clean worktree. Only now run:
   `PYTHONPATH=. uv run python -m scripts.verify_2900_point_in_time --format json`
   and then the identical command with `--format markdown`. Add the verbatim
   markdown output as
   `docs/proposals/ta/2026-08-23-r6-point-in-time-spine-result.md` without
   changing the declaration, implementation or test commits.

7. **Run repository gates and review.** Run, separately and without pipelines:
   `uv run ruff check .`; `uv run ruff format --check .`; `uv run pyright`;
   `uv run pytest -m "not db"`; `uv run pytest tests/smoke`. Run
   `codex exec review --base origin/main` before the first push. Commit any
   correction and rerun affected focused/full gates.

8. **Land and report.** Push, open the #2900 PR, resolve every review comment as
   FIXED/DEFERRED/REBUTTED plus prevention under `.claude/CLAUDE.md`, and merge
   only on latest-commit approval and green CI. Post the derived FAIL verdict on
   #2900 and update #2899: dependent Tier 2 arms remain unmeasured. Continue
   only independent Tier 1 kill-checks; never substitute current/restated rows.
