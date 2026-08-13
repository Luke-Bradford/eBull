"""Operator runbooks for #1233 Stream A.

Three runbooks live here:

* ``stream_a_run_8_verify`` — drop dev DB, re-run migrations, dispatch
  bootstrap, capture timings, hand off to the Stream-C gate.
* ``stream_a_t13_sidecar_repair`` — rebuild
  ``sec_cik_submissions_files_index`` from a local
  ``submissions.zip`` without re-fetching SEC.
* ``stream_a_stream_c_gate`` — 7-check correctness gate (C1-C7) for a
  completed bootstrap run.

Path chosen as ``app/runbooks/`` (NOT ``app/cli/runbooks/``) to avoid
shadowing the existing ``app/cli.py`` break-glass operator credential
CLI. See spec v2.4 §17 + ``docs/review-prevention-log.md``
"Operator-CLI namespace collisions".
"""
