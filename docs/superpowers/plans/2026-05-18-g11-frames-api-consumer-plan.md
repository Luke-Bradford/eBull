# G11 implementation plan — `frames` API provider primitive

> **Status:** v2 2026-05-18 (post-Codex 1b r1: unit grammar pivoted
> to explicit token-(per-token)? form; fixture realism corrected
> for Revenues/`CY2024` annual; G10 test literal counts pinned at
> 10 bad + 6 happy taxonomy; `usd` lowercase moved to happy path).
> **Spec:** `docs/superpowers/specs/2026-05-18-g11-frames-api-consumer.md` (CLEAN v3 through Codex 1a r1+r2+r3).
> **Branch:** `feat/g11-frames-api-consumer`.
> **PR target:** US ETL completion plan §2 Phase 4 PR 8.

## 0. Pre-flight

- Branch already created: `feat/g11-frames-api-consumer`.
- G10 sibling (PR #1198, merge `0ead989`) established the
  PROVIDER PRIMITIVE pattern; G11 mirrors it.
- Settled-decisions touchpoints: §"Provider design rule",
  §"Fundamentals provider posture", §"Provider boundary",
  §"Auditability". All preserved (spec §9).
- Prevention-log touchpoints: #1168 (raw-payload), #1204 (silent
  `.get` defaults — N/A in v1 since no extractor).

## 1. Tasks (sequential, single-PR scope)

### T1 — Provider method + validation

**File:** `app/providers/implementations/sec_fundamentals.py`.

**Adds:**

1. New module-level constants (alongside existing `_TAXONOMY_RE` +
   `_CONCEPT_TAG_RE`):

   ```python
   _UNIT_RE: Final[re.Pattern[str]] = re.compile(
       r"[A-Za-z][A-Za-z0-9]*(?:-per-[A-Za-z][A-Za-z0-9]*)?"
   )
   _PERIOD_RE: Final[re.Pattern[str]] = re.compile(r"CY[0-9]{4}(?:Q[1-4]I?)?")
   ```

   `_UNIT_RE` uses explicit `token(-per-token)?` grammar (Codex 1b
   r1 MED-3) — rejects `USD--per-shares` (double dash; empty
   interior token), `USD-per` (bare `-per`; missing denominator),
   `USD-per-` (trailing dash; empty denominator). Lowercase
   tokens admitted (`usd`, `Y`, etc.) — primitive is general, not
   bound to a known-unit allowlist (Codex 1b r1 HIGH-1). `_PERIOD_RE`
   admits `CY####`, `CY####Q#`, `CY####Q#I`; rejects `CY####I` (no
   annual-instantaneous frame per SEC docs).

2. Generalise the existing G10 comment block before
   `_TAXONOMY_RE` / `_CONCEPT_TAG_RE` to cover G10 + G11 — cite
   both specs (Codex 1a r1 LOW-4 ownership).

3. `SecFundamentalsProvider.fetch_frame(self, taxonomy, tag, unit,
   period) -> dict[str, Any] | None`:
   - Validate `taxonomy` via `_TAXONOMY_RE.fullmatch` — raise
     `ValueError` with diagnostic.
   - Validate `tag` via `_CONCEPT_TAG_RE.fullmatch`.
   - Validate `unit` via `_UNIT_RE.fullmatch`.
   - Validate `period` via `_PERIOD_RE.fullmatch`.
   - Build path
     `f"/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json"`.
   - `self._http.get(path)` — same shared rate-limit clock as
     companyfacts / companyconcept.
   - 404 → return None.
   - Else `raise_for_status()` then `return resp.json()`.
   - Docstring per spec §4.1 (regex citations + general-primitive
     intent + cross-reference to spec §4.2 + raw-payload invariant
     §3.2).

**Net new imports:** none (re + Final already imported in G10).

**No edits to `FundamentalsProvider` ABC** — `fetch_frame` is
concrete-class-only (per spec §2).

**No `extract_frame_facts` companion in v1** — XbrlFact schema
mismatch (no `cik` field); deferred to future consumer PR per spec
§2.

### T2 — Tests

**File:** `tests/test_sec_fundamentals_frames.py` (new). Mirror G10
test file structure; reuse `_BAD_TAXONOMY` / `_GOOD_TAXONOMY` /
`_BAD_TAG` / `_GOOD_TAG` literals to keep parametrisation
consistent across the two test files (duplicate the literals to
avoid coupling; symmetry is the goal, not code-sharing).

Test list (matches spec §5 numbering):

1. `test_fetch_frame_url` — recording-stub asserts path
   `/api/xbrl/frames/us-gaap/Assets/USD/CY2024Q1I.json`
   (balance-sheet concept paired with `Q#I` instantaneous period).
2. `test_fetch_frame_404_returns_none` — stub 404 → None.
3. `test_fetch_frame_5xx_raises` — stub 500 with `request=` attached
   via `_rewire_transport(max_retries=0)`; assert
   `httpx.HTTPStatusError` propagates (fast — ~0.1 s).
4. `test_fetch_frame_rejects_malformed_taxonomy` — duplicate the
   existing G10 `_BAD_TAXONOMY` (10 cases) + `_GOOD_TAXONOMY`
   (6 cases) literals verbatim from
   `tests/test_sec_fundamentals_companyconcept.py`. Codex 1b r1
   LOW-4 ownership — exact-mirror prevents accidental coverage drift.
5. `test_fetch_frame_rejects_malformed_tag` — duplicate G10
   `_BAD_TAG` (6 cases) + `_GOOD_TAG` (3 cases) literals.
6. `test_fetch_frame_rejects_malformed_unit` — parametrise over
   bad cases `["", "USD ", "US D", "USD\n", "123USD", "USD€",
   "USD/shares", "/USD", "USD-per-", "USD-per", "USD--per-shares"]`
   (11 bad) + happy `["USD", "USD-per-shares", "shares", "pure",
   "GBP", "EUR", "Y", "Y-per-shares", "usd"]` (9 happy). `"usd"`
   admitted per Codex 1b r1 HIGH-1 — primitive is general; doesn't
   second-guess SEC unit naming.
7. `test_fetch_frame_rejects_malformed_period` — parametrise over
   bad cases `["cy2024", "CY24", "CY2024Q5", "CY2024Q0",
   "CY2024Q1A", "CY2024Q1I ", "CY2024Q1I\n", "", "FY2024",
   "CY2024I"]` (10 bad) + happy `["CY2024", "CY2024Q1", "CY2024Q4",
   "CY2024Q1I", "CY2024Q4I"]` (5 happy).
8. `test_fetch_frame_returns_parsed_payload` — stub returns
   real-shaped frames payload for `fetch_frame("us-gaap",
   "Revenues", "USD", "CY2024")` (annual flow; Codex 1b r1 MED-2 —
   `Q#I` is balance-sheet only, `Revenues` is a flow); assert
   parsed dict has `data[0]["cik"] == 320193`, `data[0]["val"] ==
   391035000000` (Apple FY2024 Revenues).
9. `test_fetch_frame_shares_rate_limit_clock` — identity + behaviour
   sub-tests (mirror G10 test 10), incl. teardown reset of
   `_PROCESS_RATE_LIMIT_CLOCK[0] = 0.0`.

`_rewire_transport` helper duplicated locally (test-file-scoped)
since the G10 version lives in
`tests/test_sec_fundamentals_companyconcept.py`. Acceptable
duplication — the helpers are short, identical-shaped, and
test-isolated.

### T3 — Matrix + skill updates

**File:** `.claude/skills/data-engineer/etl-endpoint-coverage.md`.

- §4 `frames` row update per spec §6.
- §7 G11 row → `✅ CLOSED 2026-05-18 — G11 PR …`.

**File:** `.claude/skills/data-sources/sec-edgar.md`.

- §1.6 Frames row — append consumer annotation per spec §6.

### T4 — Pre-push gates

Run in order:

1. `uv run ruff check .`
2. `uv run ruff format --check .`
3. `uv run pyright`
4. `uv run pytest -x tests/test_sec_fundamentals_frames.py` (fast,
   hermetic).
5. `uv run pytest` (full sweep) — IF DB is up. PG may still be
   recovering from G10 session's xdist OOM cycle. If unavailable,
   `--no-verify` is justified per
   `feedback_pre_push_xdist_postgres_locks.md` (G10 set precedent —
   provider primitive is import-stable, no lifespan touch).
6. **Codex 2 pre-push** against the branch diff.

Repo `.githooks/pre-push` automates steps 1-3.

## 2. Acceptance (spec §7 mirrored)

- [ ] `SecFundamentalsProvider.fetch_frame(taxonomy, tag, unit,
      period)` callable; returns parsed JSON dict or None on 404.
- [ ] Rate-limit clock shared with companyfacts + companyconcept
      (test 9).
- [ ] 404→None + 5xx→raise contract pinned (tests 2 + 3).
- [ ] All four validation regexes pinned (tests 4-7).
- [ ] Matrix §4 + §7 + sec-edgar.md §1.6 updated.
- [ ] Lint + format + pyright + targeted pytest all green locally.
- [ ] Codex 2 pre-push CLEAN.

## 3. Risks + mitigations

1. **Risk:** Codex 2 flags missing typed extractor (`extract_frame_facts`)
   as a coverage gap.
   **Mitigation:** spec §2 explicitly defers the typed extractor —
   XbrlFact schema mismatch (no `cik` field). If Codex pushes back,
   the response is "primitive-only by design; consumer PR adds the
   FrameFact dataclass + extractor with `cik` + `entityName` fields."
2. **Risk (CLOSED):** Earlier draft admitted `USD--per-shares` /
   `USD-per`; risk addressed pre-implementation by Codex 1b r1 MED-3
   regex pivot to explicit `token(-per-token)?` grammar. No longer
   open.
3. **Risk:** PG recovery still ongoing during push.
   **Mitigation:** `--no-verify` justified for the same reason as
   G10 (impacted files clean + Codex green; no lifespan touch).
   CI runs full pytest with clean DB.

## 4. Out-of-scope (explicit)

- No `FundamentalsProvider` ABC change.
- No `extract_frame_facts` companion (spec §2; future-consumer PR).
- No new SQL migration; no DB writer; no `sec_frames_raw` table.
- No scheduled job, no `ScheduledJob` registration.
- No bulk-archive change.
- No `companyconcept` re-touch (G10 already shipped).

## 5. PR body skeleton

Sections (CLAUDE.md PR-authoring conventions + spec §8 ETL clauses
disposition):

1. **What changed** — provider primitive landed; no production
   consumer.
2. **Why** — closes G11 gap with a callable surface; decision-rule
   output (#594 latent demand) maps to PROVIDER PRIMITIVE per Codex
   G11-scope review; full wire deferred to whichever data ticket
   #594 drives.
3. **Test plan** — new test file + pytest invocation.
4. **ETL clauses #8-#12 disposition** — N/A (spec §8).
5. **Settled-decisions check** — preserved per spec §9.
6. **Codex audit** — spec CLEAN through 1a r1+r2+r3; plan CLEAN
   through 1b (this doc); Codex 2 pre-push against final diff.
7. **Future-consumer invariant** — spec §3.2 raw-payload obligation
   linked for the next caller PR.

## 6. Codex 1b r1 disposition

| Finding | Severity | Resolution |
|---|---|---|
| Plan/spec `_UNIT_RE` lowercase conflict — `"usd"` marked bad while `"pure"` happy; regex cannot enforce | HIGH | FIXED `_UNIT_RE` grammar pivoted to explicit `token(-per-token)?`; `"usd"` moved from `_BAD_UNIT` to `_GOOD_UNIT` (primitive is general, no known-unit allowlist) |
| Fixture realism — `391035000000` is FY2024 annual not Q4; `CY2024Q1I` is balance-sheet only, not for `Revenues` flow | MED | FIXED test 8 stub now uses `fetch_frame("us-gaap", "Revenues", "USD", "CY2024")` annual; test 1 URL example switched to `Assets` (balance-sheet) paired with `CY2024Q1I` |
| `_UNIT_RE` admits `USD--per-shares` + `USD-per`; tighten now to avoid bot churn | MED | FIXED grammar `[A-Za-z][A-Za-z0-9]*(?:-per-[A-Za-z][A-Za-z0-9]*)?` rejects all three malformed forms (`--`, bare `-per`, trailing `-`) |
| Taxonomy test count stale (8 bad / 6 happy vs G10 actual 10 bad / 6 happy) | LOW | FIXED plan now says "duplicate G10 literals verbatim" — 10 bad / 6 happy taxonomy |
| T1/T2 decomposition mirrors G10 cleanly | OK | NO-CHANGE |
| Keep `_rewire_transport` duplicated for G11 | OK | NO-CHANGE |
| General decomposition sound | OK | NO-CHANGE |

Plan CLEAN v2 — proceed to implementation.

## 7. Codex 1b r2 disposition

| Finding | Severity | Resolution |
|---|---|---|
| T2 test 1 line still says `Revenues/USD/CY2024Q1I` (contradicts spec §5 + plan §6) | MED | FIXED test 1 URL fixed to `Assets/USD/CY2024Q1I` (balance-sheet concept paired with instantaneous period) |
| Risk 2 still describes `USD--per-shares` deferred to bot signal; regex already rejects it | LOW | FIXED risk 2 marked CLOSED with cross-reference to r1 MED-3 |

Plan CLEAN through r2 — proceed to implementation.
