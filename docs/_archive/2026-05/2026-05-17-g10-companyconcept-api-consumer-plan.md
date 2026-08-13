# G10 implementation plan — `companyconcept` provider primitive

> **Status:** v2 2026-05-17 (post-Codex 1b r1: regex `fullmatch`
> discipline; test 5xx fixture mechanics; test-fixture realism split
> Revenues integer vs EPS float; rate-limit-clock test isolation
> cleanup).
> **Spec:** `docs/superpowers/specs/2026-05-17-g10-companyconcept-api-consumer.md` (CLEAN v3 through Codex 1a r3).
> **Branch:** `feat/g10-companyconcept-api-consumer`.
> **PR target:** US ETL completion plan §2 Phase 4 PR 7.

## 0. Pre-flight

- Branch: `feat/g10-companyconcept-api-consumer` (per CLAUDE.md naming
  convention; no issue number — G10 is a coverage-matrix gap).
- Settled-decisions touchpoints: §"Provider design rule",
  §"Fundamentals provider posture", §"Provider boundary",
  §"Auditability". All preserved — see spec §9.
- Prevention-log touchpoints: #1168 (raw-payload before parse), #1174
  (`Decimal(str(x))` boundary), #1204 (silent `.get(default)` for
  financial-semantic dicts). All addressed in spec §3.3 / §3.5 / §4.3.

## 1. Tasks (sequential, single-PR scope)

### T1 — Provider methods + validation

**Files:** `app/providers/implementations/sec_fundamentals.py`.

**Adds:**

1. Module-level constants (above the `SecFundamentalsProvider` class).
   Validation MUST use `.fullmatch(...)` not `.match(...)` (Codex 1b
   r1 MED-1 ownership) — `re.match` + `^...$` admits a trailing
   `\n` because `$` matches before a final newline.

   ```python
   _TAXONOMY_RE: Final = re.compile(r"[a-z][a-z0-9-]*")
   _CONCEPT_TAG_RE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
   # Use `_TAXONOMY_RE.fullmatch(value)` / `_CONCEPT_TAG_RE.fullmatch(value)`.
   ```

2. `SecFundamentalsProvider.fetch_concept(self, cik, taxonomy, tag) -> dict[str, Any] | None`:
   - Validate `taxonomy` against `_TAXONOMY_RE` — raise `ValueError`
     with diagnostic on miss.
   - Validate `tag` against `_CONCEPT_TAG_RE` — raise `ValueError`
     with diagnostic on miss.
   - Build path `f"/api/xbrl/companyconcept/CIK{_zero_pad_cik(cik)}/{taxonomy}/{tag}.json"`.
   - Call `self._http.get(path)` (same code path as
     `_fetch_company_facts`, so rate-limit clock auto-shared).
   - 404 → return None.
   - Else `raise_for_status()` then `return resp.json()`.
   - Docstring per spec §4.1 (taxonomy regex + tag regex + general
     SEC primitive intent + cross-reference to spec
     §4.2 + raw-payload invariant § from spec §3.3).

3. `SecFundamentalsProvider.extract_concept_facts(self, symbol, cik, taxonomy, tag) -> list[XbrlFact]`:
   - Calls `self.fetch_concept(cik, taxonomy, tag)`.
   - If `None`, return `[]`.
   - Read `payload.get("taxonomy")`; if non-None and != request
     taxonomy, `logger.warning(...)`.
   - Read `payload.get("units")`; if not `dict`, `logger.warning(...)`
     and use `{}`.
   - Synthesise `section = {tag: {"units": units_or_empty}}`.
   - Return `_extract_facts_from_section(section, taxonomy=taxonomy)`
     (existing extractor; reuses unit-priority + Decimal(str(val))
     guards already enforced there).

**Net new module imports:** `re`, `Final`. (Both already in stdlib;
no third-party dependency change.)

**No edits to `FundamentalsProvider` ABC at
`app/providers/fundamentals.py`** — methods are concrete-class-only
(spec §2 non-goals + §4.1 close).

### T2 — Tests

**File:** `tests/test_sec_fundamentals_companyconcept.py` (new).

**Setup:** module-level `_USER_AGENT = "ebull-test/0.0 test@example.com"`.
Tests use `unittest.mock` for HTTP stubs except for test 9 which uses
`httpx.MockTransport` (per spec §5 test 9 disposition).

Test list. Spec §5 numbers tests 1-9 (with `6a`); after the r1 split
adding `test_extract_concept_facts_decimal_str_boundary` as a separate
test, this plan renumbers linearly 1-10. The acceptance row "test 9
identity + behaviour" in §2 below refers to spec §5 test 9 / plan
test 10 — same test, different sequence number.

1. `test_fetch_concept_url_zero_pads_cik` — stub `provider._http.get`
   with a recording lambda; assert the path it received.
2. `test_fetch_concept_404_returns_none` — stub returns a 404 Response;
   assert `fetch_concept(...) is None`.
3. `test_fetch_concept_5xx_raises` — stub returns
   `httpx.Response(500, request=httpx.Request("GET",
   "https://data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/Revenues.json"))`
   (Codex 1b r1 MED-3 ownership — a bare `Response(500)` has no
   `Request` attached; `raise_for_status` then raises a wrong
   fixture-level error rather than the intended
   `HTTPStatusError`); assert `httpx.HTTPStatusError` propagates
   cleanly.
4. `test_fetch_concept_rejects_malformed_taxonomy` — parametrise over
   `["Us-Gaap", "", "us gaap", "us/gaap", "-bad", "us-gaap\n",
   "us-gaap ", "\nus-gaap"]`; each raises `ValueError`. Happy-path
   parametrise over `["us-gaap", "dei", "srt", "ifrs-full",
   "invest", "country"]` with stubbed 200 asserts no raise.
   `"us-gaap\n"` pins the `fullmatch` discipline (Codex 1b r1 MED-1).
5. `test_fetch_concept_rejects_malformed_tag` — parametrise over
   `["Revenues/Q1", "Revenues ", "", "123Revenues", "Revenu€s",
   "Revenues\n"]`; each raises. Happy path over `["Revenues",
   "EntityCommonStockSharesOutstanding", "my_custom_concept"]`.
6. `test_extract_concept_facts_reuses_section_extractor` — stub
   `fetch_concept` to return a fixture payload (`tag=Revenues`)
   with two `USD` integer entries — `(form="10-K", fp="FY",
   end="2024-09-28", val=391035000000)` + `(form="10-Q",
   fp="Q3", end="2025-06-28", val=85777000000)`. Asserts
   `len(facts) == 2`, each carries `concept=='Revenues'`,
   `taxonomy=='us-gaap'`, `unit=='USD'`, matching period_end + val
   + form_type. Integer USD is realistic for Revenues (Codex 1b r1
   MED-2 ownership — moved the float-boundary exercise to test 6a).
7. `test_extract_concept_facts_decimal_str_boundary` — fixture
   `(taxonomy=us-gaap, tag=EarningsPerShareDiluted, unit=USD/shares,
   val=3.7)`. Asserts `facts[0].val == Decimal("3.7")` exactly.
   Pins prevention-log #1174 (`Decimal(str(<float>))` boundary)
   without muddling the Revenues integer fixture.
8. `test_extract_concept_facts_empty_on_404` — stub returns None;
   assert `[]`.
9. `test_extract_concept_facts_empty_on_missing_units` — stub
   returns payload without `units`; assert `[]` AND `caplog`
   records a `WARNING` referencing the CIK + tag.
10. `test_fetch_concept_shares_rate_limit_clock` — TWO sub-tests:
    - **Identity sub-test:** construct provider, assert
      `provider._http._last_request_at is sec_edgar._PROCESS_RATE_LIMIT_CLOCK`
      + `provider._http._throttle_lock is sec_edgar._PROCESS_RATE_LIMIT_LOCK`.
    - **Behaviour sub-test:** rebuild `provider._http` around a fresh
      `httpx.Client(transport=httpx.MockTransport(handler))` while
      preserving the shared clock + lock; record `time.monotonic()`
      per stub call; back-to-back `fetch_concept` invocations
      observe delta ≥ `_MIN_REQUEST_INTERVAL_S`. **Teardown** (Codex
      1b r1 LOW-4 ownership): close the swapped client + reset
      `_PROCESS_RATE_LIMIT_CLOCK[0] = 0.0` so the shared-clock
      mutation does not bleed into the rest of `uv run pytest`.

Note: previously-numbered test 6 split into 6 (integer fixture) + 7
(float-boundary) per Codex 1b r1 MED-2. Total test count is now 10.

### T3 — Matrix + skill updates

**File:** `.claude/skills/data-engineer/etl-endpoint-coverage.md`.

- §4 row `data.sec.gov/api/xbrl/companyconcept/CIK*/{taxonomy}/{tag}.json`
  — update last column per spec §6 (PROVIDER PRIMITIVE 2026-05-17 (G10)
  with file:line citations).
- §7 G10 row — `OPEN (low)` → `✅ CLOSED 2026-05-17 — PR #<n> —
  provider primitive landed (sec_fundamentals.py::fetch_concept +
  extract_concept_facts); no fundamentals_sync / daily_financial_facts
  wire-up by design (audit in spec §3.1).`

**File:** `.claude/skills/data-sources/sec-edgar.md`.

- §1.6 "JSON APIs" table — Companyconcept row: append "Provider
  primitive: `app/providers/implementations/sec_fundamentals.py::fetch_concept`
  (G10, 2026-05-17). No production consumer in v1." to the existing
  "One XBRL tag (smaller payload)" description.

### T4 — Pre-push gates

Run in order:

1. `uv run ruff check .`
2. `uv run ruff format --check .`
3. `uv run pyright`
4. `uv run pytest -x tests/test_sec_fundamentals_companyconcept.py`
   (fast, hermetic — confirms new tests pass before full sweep).
5. `uv run pytest` (full suite incl. smoke).
6. **Codex 2 pre-push** against the branch diff.

Repo `.githooks/pre-push` automates steps 1-3.

## 2. Acceptance (spec §7 mirrored)

- [ ] `SecFundamentalsProvider.fetch_concept(cik, taxonomy, tag)`
      callable; returns parsed JSON dict or None on 404.
- [ ] `SecFundamentalsProvider.extract_concept_facts(symbol, cik,
      taxonomy, tag)` callable; returns `list[XbrlFact]` reusing
      `_extract_facts_from_section`.
- [ ] Rate-limit clock shared with companyfacts (test 9 identity +
      behaviour).
- [ ] 404→None + 5xx→raise contract pinned (tests 2 + 3).
- [ ] Taxonomy + tag validation pinned (tests 4 + 5).
- [ ] Matrix §4 + §7 + sec-edgar.md §1.6 updated.
- [ ] Lint + format + pyright + pytest + smoke gate all green
      locally.
- [ ] Codex 2 pre-push CLEAN.

## 3. Risks + mitigations

1. **Risk:** Codex 2 catches an unexpected coupling between
   `_extract_facts_from_section` and a wider companyfacts payload
   context (e.g. a sibling-tag lookup), breaking the
   single-tag-section synthesis.
   **Mitigation:** test 6 exercises the extractor end-to-end with a
   synthesised single-tag section; if the extractor depends on a
   broader payload, that test fails BEFORE Codex 2 runs.

2. **Risk:** Rate-limit identity assertion (test 9 sub-test 1)
   becomes brittle if `ResilientClient.__init__` is later refactored
   to wrap the shared list.
   **Mitigation:** assertion is `is`-identity not value-identity;
   refactor would have to consciously change the contract.
   Behaviour sub-test 2 backs up the identity with end-to-end
   throttle proof.

3. **Risk:** Codex 2 flags the prevention-log #1168 raw-payload
   exemption as insufficient even with the invariant codified in
   spec §3.3.
   **Mitigation:** if escalated, the response is "no DB writer in
   this PR — the rule only fires when raw bytes enter a parse-then-
   persist path. Future-consumer invariant in spec §3.3 binds any
   subsequent caller." If Codex 2 disagrees, escalate to operator
   (genuine arch trade-off).

## 4. Out-of-scope (explicit non-extensions)

- No `FundamentalsProvider` ABC change.
- No `fundamentals_sync` / `daily_financial_facts` / `refresh_fundamentals`
  edit — no production consumer wired (spec §3.1).
- No new SQL migration; no DB writer.
- No bulk-archive change.
- No `extract_concept_catalog` companion method (spec §3.2 closed by
  Codex 1a r1 LOW-8).
- No `frames` API consumption (that's G11, Phase 4 PR 8).

## 5. PR body skeleton

Sections (per CLAUDE.md PR-authoring conventions + spec §8 ETL clauses
disposition):

1. **What changed** — provider primitive landed; no production
   consumer.
2. **Why** — closes G10 gap with a callable surface for future
   single-tag refresh paths (e.g. #435 dilution tracker); under the
   current 10 req/s shared SEC budget, wiring as a fundamentals_sync
   replacement is wall-clock net-negative.
3. **Test plan** — new test file + pytest invocation; smoke gate
   covers app boot unaffected.
4. **ETL clauses #8-#12 disposition** — N/A — provider primitive
   only; no schema / parser / observations / rollup change. Spec §8
   linked.
5. **Settled-decisions check** — preserved per spec §9.
6. **Codex audit** — spec CLEAN through 1a r3; plan CLEAN through
   1b (this doc); Codex 2 pre-push CLEAN against final diff.
7. **Future-consumer invariant** — spec §3.3 raw-payload obligation
   linked for the next caller PR.

## 6. Codex 1b r1 disposition

| Finding | Severity | Resolution |
|---|---|---|
| Test 4/5 boundary — `^...$` + `match()` admits trailing `\n`; need `fullmatch` + add bad cases | MED | FIXED T1 explicit `_TAXONOMY_RE.fullmatch(...)` + spec §4.2 + tests 4/5 now include `"us-gaap\n"`, `"\nus-gaap"`, `"us-gaap "`, `"Revenues\n"` |
| Test 6 fixture muddled — real Revenues is integer-like USD; float-boundary belongs to its own test | MED | FIXED test 6 carries integer revenue figures only; new test 7 `test_extract_concept_facts_decimal_str_boundary` uses EPS `USD/shares` `val=3.7` to pin #1174 |
| Test 3 fixture mechanics — `Response(500).raise_for_status()` needs attached `Request` | MED | FIXED test 3 spec + plan now require `httpx.Response(500, request=httpx.Request("GET", "..."))` |
| Test 10 sub-test 2 cleanup — close swapped client + reset `_PROCESS_RATE_LIMIT_CLOCK[0]` to avoid shared-clock bleed | LOW | FIXED spec test 9 + plan test 10 prose state teardown contract explicitly |
| T1 / T2 step ordering — fine as sequential | LOW | NO-CHANGE acknowledged as OK |
| T3 matrix update ordering — fine post-impl | LOW | NO-CHANGE acknowledged as OK |
| Raw-payload mitigation sufficient; do NOT add `sec_companyconcept_raw` now | LOW | CONFIRMED scope holds — spec §3.3 invariant binds future consumers |

Plan CLEAN v2 — proceed to implementation.

## 7. Codex 1b r2 disposition

| Finding | Severity | Resolution |
|---|---|---|
| Test numbering drift — plan claims "matches spec §5 numbering exactly" while renumbering 6a→7 + 9→10 | LOW | FIXED prose now states "Spec §5 numbers 1-9; plan renumbers linearly 1-10; acceptance row points at the same test under both schemes" |
| 5xx fixture explanation factually off — bare `Response(500).raise_for_status()` does not silently skip, it raises wrong-fixture error | LOW | FIXED test 3 description now precise: "raises a wrong fixture-level error rather than the intended HTTPStatusError" |

Plan CLEAN through r2 — proceed to implementation.

## 8. Codex 2 pre-push r1 disposition

| Finding | Severity | Resolution |
|---|---|---|
| `_CONCEPT_TAG_RE` overclaim — comment says "every legal XBRL concept name" but XBRL NCName admits `_foo`, `foo-bar`, `foo.bar`; regex deliberately rejects those | MED | FIXED `sec_fundamentals.py` validator block-comment now states the regex is "a deliberately tightened subset of legal XBRL NCName syntax — every SEC-observed concept name uses `[A-Za-z][A-Za-z0-9_]*`; widen + add regression test if SEC drift surfaces a legitimate NCName outside this subset" |
| Taxonomy-mismatch warning branch untested | LOW | FIXED added `test_extract_concept_facts_logs_warning_on_taxonomy_mismatch` — payload `taxonomy='srt'` against request `'us-gaap'`; asserts emitted facts carry the request taxonomy + warning lands in caplog |
| 5xx test sleeps ~7 s through default `ResilientClient` retry/backoff | LOW | FIXED `_rewire_transport` test helper now accepts `max_retries=0` (default), drops 5xx test from ~7 s to ~0.1 s |

Implementation CLEAN through Codex 2 r1 — push.
