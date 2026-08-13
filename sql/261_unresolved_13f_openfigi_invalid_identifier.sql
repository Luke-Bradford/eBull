-- 261_unresolved_13f_openfigi_invalid_identifier.sql
--
-- #2304 — split the OpenFIGI sweep's per-item REJECTION out of
-- ``openfigi_unknown``.
--
-- ## The defect this closes
--
-- ``openfigi_unknown`` was written for every entry the parser could not
-- turn into a mapping, and the parser folded four distinct source
-- verdicts into one ``None``: ``{"warning": ...}`` (no such identifier),
-- ``{"error": ...}`` (OpenFIGI REJECTED the identifier we sent),
-- a ``data`` array with no US-primary common-stock row, and any
-- unrecognised entry shape. The status text claims "OpenFIGI has no
-- mapping for this CUSIP" — a coverage fact. For the ``error`` case the
-- truth is "OpenFIGI would not accept this identifier" — an input fact,
-- with a different owner and a different remedy.
--
-- ## Source rule (probed live 2026-08-06, NOT recited)
--
-- ``POST https://api.openfigi.com/v3/mapping`` with ``idType=ID_CUSIP``:
--
--   * ``037833100``  (AAPL; valid CUSIP mod-10 check digit) → ``{"data": [...]}``
--   * ``000000000``  (valid check digit, unassigned)        → ``{"warning": "No identifier found."}``
--   * ``ZZZZZZZZZ``  (9-char upper alnum, INVALID check digit)
--                                                           → ``{"error": "Invalid idValue format."}``
--
-- OpenFIGI validates the CUSIP check digit and answers a check-digit
-- failure with a per-item ``error``, not a ``warning``. The entry body
-- carries no structured error code — the key set is exactly
-- ``{"error"}`` — so the classifier matches the message text; see
-- ``_classify_item_error`` in ``app/services/openfigi_resolver.py`` for
-- the two literals observed and the deliberately narrow match.
--
-- ``.claude/skills/data-sources/openfigi.md`` §4.3 recorded this shape as
-- "theoretical, not in the probe set". It is not theoretical; the skill
-- is corrected in the same PR.
--
-- ## Blast radius (full population, dev DB, 2026-08-06)
--
-- Reproduce with ``uv run python scripts/audit_cusip_check_digit.py``:
--
--   openfigi_unknown, check digit INVALID :  14,477 distinct / 14,511 rows
--   openfigi_unknown, check digit VALID   :  39,772 distinct / 45,500 rows
--
-- i.e. 26.7% of the bucket holds a terminal "no mapping" verdict that
-- today's source would answer with a rejection instead. NOTE the limit
-- of that claim: it establishes the CURRENT source answer for those
-- identifiers, not the provenance of each historical write. Re-asking
-- the source is what fixes the label, which is why the operator step is
-- a reset-to-NULL and not an in-place UPDATE to the new status.
--
-- ## Why this status is TERMINAL
--
-- Consistent with #740 / sql/192 (operator decision 2026-06-11, no
-- auto-retry; ``SET resolution_status = NULL`` is the escape hatch).
-- The rejection is a deterministic function of a fixed stored
-- identifier, so re-asking cannot change the answer — leaving these
-- rows NULL would re-select them on every pass and reproduce exactly
-- the non-draining backlog sql/192 was written to cure.
--
-- ⚠ ONLY recognised identifier-rejection errors land here. An
-- unrecognised per-item ``error`` (provider bug, entitlement, throttle,
-- a future shape) is NOT terminal — the row stays NULL and retries, and
-- the run's ``item_errors`` counter carries the signal. Terminalising
-- every ``error`` would recreate this same ticket one layer up.
--
-- ## Idempotency
--
-- DROP + re-ADD in one transaction (same pattern as sql/112, sql/168,
-- sql/192). Safe against existing rows: the post-shift value set is a
-- strict superset of the current population.

BEGIN;

ALTER TABLE unresolved_13f_cusips
    DROP CONSTRAINT IF EXISTS unresolved_13f_cusips_resolution_status_check;

ALTER TABLE unresolved_13f_cusips
    ADD CONSTRAINT unresolved_13f_cusips_resolution_status_check
    CHECK (resolution_status IS NULL OR resolution_status IN (
        'unresolvable',
        'ambiguous',
        'conflict',
        'manual_review',
        'resolved_via_extid',
        'resolved_via_openfigi',
        'openfigi_unknown',
        'openfigi_no_instrument',
        'openfigi_invalid_identifier'
    ));

COMMIT;
