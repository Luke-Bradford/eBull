# Runbook — after a parser change

When a PR lands that changes how ownership / fundamentals /
observation data is parsed or stored, the operator follow-up is:

## 1. Identify scope

Which `(subject, source)` triples need re-ingest?

- Parser-version bumped on Form 4 → `{ "source": "sec_form4" }`.
- Single-CIK tombstone-resolution fix → `{ "instrument_id": <id>, "source": "<src>" }`.
- 13F-HR parser change → `{ "source": "sec_13f" }`.
- NPORT-P parser change → `{ "source": "sec_n_port" }`.

## 2. Trigger the rebuild

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{ "source": "sec_form4" }' \
  http://localhost:8000/jobs/sec_rebuild/run
```

The job resets the relevant scheduler rows + manifest rows to
`pending` and lets the manifest worker drain them.

(Operator token may be required — see admin endpoint docs. For
session-local dev work, calling the underlying service function via
psycopg from a scratch script in `scripts/` is acceptable. Drop the
scratch script when done.)

## 3. Wait for the drain

The worker is rate-limited at 10 req/s shared with all other SEC
ingest paths. Monitor pending count via:

```bash
curl http://localhost:8000/jobs/sec_manifest_worker/status
```

Wait until pending count for the scope reaches zero.

## 4. Verify the operator-visible figure

For ownership changes, hit the rollup endpoint for the panel of
known instruments:

```bash
for sym in AAPL GME MSFT JPM HD; do
  curl -s "http://localhost:8000/instruments/$sym/ownership-rollup" \
    | jq '.slices[] | {category, pct_of_outstanding, filer_count}'
done
```

Confirm the figure renders correctly with the new data path.

## 5. Cross-source confirm

Spot-check at least one figure against an independent source:

- Gurufocus — institutional ownership %.
- Marketbeat — insider ownership %.
- SEC EDGAR direct — raw filing.
- Specific golden-file fixtures committed under `tests/fixtures/sec/`.

## If anything fails

Do NOT consider the PR fully landed even after merge. Open a
follow-up ticket and reference the merge SHA.

## Why this runbook

Every PR touching parser / ETL / schema migration must record
clauses 8-12 of the Definition of Done in its body (see
`.claude/CLAUDE.md`). This runbook is the operator-side
counterpart — what the operator does after a PR with those clauses
ships.
