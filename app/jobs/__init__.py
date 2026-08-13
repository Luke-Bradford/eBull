"""Jobs runtime package (issue #13).

This package owns the runtime that actually fires scheduled jobs and the
manual-trigger surface that lets an operator run them on demand. The
*declared* schedule registry and the per-job functions live in
``app/workers/scheduler.py`` and are imported here -- this package is the
glue between that registry and a live executor (APScheduler), not a
re-implementation of either.

PR slicing (see #13):

* PR A (this PR) -- runtime + lock + manual trigger API + one job wired
  end-to-end (``nightly_universe_sync``). No catch-up, no pipeline, no
  listing endpoint. Minimum surface to prove the runtime fires real
  work and serialises double-clicks.
* PR B -- wire the remaining jobs, add ``GET /jobs`` and the admin UI.
* PR C -- catch-up-on-boot semantics and the pipeline runner.
"""
