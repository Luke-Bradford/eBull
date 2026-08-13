# Operator-runbook JSON schema versions

Pinned schema versions for the JSON envelope emitted by every runbook
under `app/runbooks/`. The envelope shape is documented in spec v2.4
§17. Each envelope carries `schema_version: <int>` so downstream log
parsers can branch on shape.

## Version registry

| Version | Introduced by | Runbooks | Shape |
|---------|---------------|----------|-------|
| 1 | #1233 PR-D | `stream_a_run_8_verify`, `stream_a_t13_sidecar_repair`, `stream_a_stream_c_gate` | `{schema_version, runbook, ...runbook-specific..., exit_code}` |

## Bump policy

When changing any envelope shape:

1. Bump `JSON_SCHEMA_VERSION` in the runbook module.
2. Add a new row above with the new version + a short summary of the
   shape delta.
3. Note any callers that depend on the previous shape (e.g. operator
   dashboards, log aggregators).

The append-only JSONL log files live under `var/runbooks/` (operator-
owned; not checked in; manual purge >30d per the runbook docstrings).
