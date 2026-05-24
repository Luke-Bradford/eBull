# Path migration map — 2026-05-23

After the 2026-05-23 doc reorganisation, ~300 code-comment references in `app/`, `sql/`, `scripts/`, `tests/` still point to old `docs/superpowers/{specs,plans,spikes}/` paths. These are comment-only references — they don't break code, but they break click-through navigation.

This file maps old path → new path. Future PRs can clean up code-comment refs incrementally; until then, grep this file to find the new location.

## Active specs (now under `docs/specs/`)

| Old path | New path |
| --- | --- |
| `docs/superpowers/specs/2026-05-04-etl-coverage-model.md` | `docs/specs/etl/coverage-model.md` |
| `docs/superpowers/specs/2026-05-07-first-install-bootstrap.md` | `docs/specs/bootstrap/first-install.md` |
| `docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md` | `docs/specs/bootstrap/orchestration.md` |
| `docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md` | `docs/specs/bootstrap/bulk-datasets.md` |
| `docs/superpowers/specs/2026-05-08-filing-allow-list-and-raw-retention.md` | `docs/specs/etl/retention.md` |
| `docs/superpowers/specs/2026-05-13-atomic-bootstrap-enqueue.md` | `docs/specs/bootstrap/atomic-enqueue.md` |
| `docs/superpowers/specs/2026-05-13-bootstrap-capability-layer.md` | `docs/specs/bootstrap/capability-layer.md` |
| `docs/superpowers/specs/2026-05-13-db-lane-family-split.md` | `docs/specs/orchestrator/db-lane-family-split.md` |
| `docs/superpowers/specs/2026-05-13-layer-123-wiring.md` | `docs/specs/etl/discovery-layers.md` |
| `docs/superpowers/specs/2026-05-13-precondition-final-data-gates.md` | `docs/specs/bootstrap/preconditions.md` |
| `docs/superpowers/specs/2026-05-14-n-csr-fund-metadata.md` | `docs/specs/fund-data/n-csr-metadata.md` |
| `docs/superpowers/specs/2026-05-14-sec-10q-manifest-parser-noop.md` | `docs/specs/etl/sec-10q-parser.md` |
| `docs/superpowers/specs/2026-05-15-n-csr-bootstrap-drain.md` | `docs/specs/fund-data/n-csr-drain.md` |
| `docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md` | `docs/specs/etl/lane-b-discovery.md` |
| `docs/superpowers/specs/2026-05-16-worker-drain-fairness.md` | `docs/specs/orchestrator/worker-drain-fairness.md` |
| `docs/superpowers/specs/2026-05-17-g12-master-idx-quarterly-walker.md` | `docs/specs/etl/master-idx-quarterly-walker.md` |
| `docs/superpowers/specs/2026-05-17-g8-company-tickers-exchange-directory.md` | `docs/specs/etl/company-tickers-exchange.md` |
| `docs/superpowers/specs/2026-05-17-orchestrator-inner-lock-removal.md` | `docs/specs/orchestrator/inner-lock-removal.md` |
| `docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md` | `docs/specs/infra/pg-max-locks-guard.md` |
| `docs/superpowers/specs/2026-05-18-g11-frames-api-consumer.md` | `docs/specs/etl/frames-api-consumer.md` |
| `docs/superpowers/specs/2026-05-18-n-port-edgartools-dropin.md` | `docs/specs/fund-data/n-port-edgartools.md` |
| `docs/superpowers/specs/2026-05-19-data-retention-rubric.md` | `docs/specs/etl/retention-rubric.md` |

## Proposals (now under `docs/proposals/`)

| Old path | New path |
| --- | --- |
| `docs/superpowers/specs/2026-05-06-def14a-bene-table-extension-design.md` | `docs/proposals/etl/def14a-bene-table-extension.md` |
| `docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md` | `docs/proposals/etl/ownership-tier0-cik-history.md` |
| `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md` | `docs/proposals/etl/ownership-full-decomposition.md` |
| `docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md` | `docs/proposals/ui/admin-control-hub-rewrite.md` |
| `docs/superpowers/specs/2026-05-10-share-class-cik-uniqueness.md` | `docs/proposals/etl/share-class-cik-uniqueness.md` |
| `docs/superpowers/specs/2026-05-22-bootstrap-etl-optimisation-v3.md` | `docs/proposals/etl/bootstrap-optimisation.md` |
| `docs/superpowers/specs/2026-04-25-data-source-routing-spec.md` | `docs/proposals/etl/data-source-routing.md` |
| `docs/superpowers/specs/2026-04-25-visibility-driven-live-prices-spec.md` | `docs/proposals/etl/visibility-driven-live-prices.md` |
| `docs/superpowers/specs/2026-04-26-complete-coverage-spec.md` | `docs/proposals/etl/complete-coverage.md` |

(Plus ~40 more `*-design.md` files moved to `docs/proposals/{ui,etl,infra}/`. Pattern: drop `2026-MM-DD-` prefix, drop `-design` suffix, route by topic. If the new path isn't obvious, grep the new path in `docs/proposals/` for the topic name.)

## Archived (now under `docs/_archive/`)

All `docs/superpowers/plans/*.md` → `docs/_archive/<yyyy-mm>/<original-name>.md` (or `docs/_archive/stale/<original-name>.md` if abandoned).

All `docs/superpowers/spikes/*.md` → `docs/_archive/2026-05/spike-<short-name>.md`.

15 `docs/superpowers/specs/*` files that were SHIPPED → `docs/_archive/2026-05/<original-name>.md`.

7 `docs/superpowers/specs/*` files that were SUPERSEDED → `docs/_archive/2026-05/superseded-<original-name>.md`.

## Future cleanup

Code-comment refs in `app/`, `sql/`, `scripts/`, `tests/` still point at old paths. They are no-impact (comment-only) but should be updated incrementally when adjacent code changes. Pre-push lint could enforce going forward; not enforced today.
