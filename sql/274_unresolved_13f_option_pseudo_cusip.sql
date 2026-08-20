-- 274_unresolved_13f_option_pseudo_cusip.sql
--
-- #2353 — a 13F row that reports the Official List's CALL/PUT
-- identifier is a FILER DEVIATION, not an unmappable security. Give it
-- its own terminal verdict so it stops being asked of a provider that
-- cannot answer, and stops being offered to a fuzzy matcher that would
-- answer WRONGLY.
--
-- ## Source rule — Form 13F, Special Instruction 10
--
-- ``https://www.sec.gov/files/form13f.pdf`` p.6-7, quoted verbatim:
--
--   "A Manager must report holdings of options only if the options
--    themselves are Section 13(f) securities. ... The Manager must give
--    the entries in Columns 1 through 5 and in Columns 7 and 8 of the
--    Information Table, however, in terms of the securities UNDERLYING
--    the options, not the options themselves. ... coupled with a
--    designation "PUT" or "CALL" following such segregated entries in
--    Column 5"
--
-- Special Instruction 11.b.iii makes Column 3 the CUSIP. Column 3 is
-- inside "Columns 1 through 5", so the identifier an option row is
-- REQUIRED to carry is the UNDERLYING's CUSIP, with PUT/CALL carried in
-- Column 5 (the ``PUTCALL`` field of the structured INFOTABLE, stored
-- here as ``institutional_holdings.is_put_call``).
--
-- SEC separately publishes an identifier for the option CLASS on the
-- Official List of Section 13(f) Securities — AAPL is
-- ``037833100 COM`` / ``037833900 CALL`` / ``037833950 PUT``. Those are
-- real, SEC-published and deliberately not valid CUSIPs; they are simply
-- not what Column 3 asks for. A filer that puts one in Column 3 has
-- deviated from Special Instruction 10, and the row can never resolve:
--
--   * OpenFIGI REJECTS it (mod-10 check digit — see
--     ``sql/261_unresolved_13f_openfigi_invalid_identifier.sql``);
--   * the Official-List backfill in
--     ``app/services/sec_13f_securities_list.py`` maps only
--     ``_is_common_share`` rows, so it never mints a mapping for one;
--   * the legacy fuzzy-name resolver WOULD match it — on the issuer
--     name, which is identical to the underlying's — and write an
--     ``external_identifiers`` row binding an option-class identifier to
--     the underlying instrument. That is the dangerous outcome this
--     status exists to pre-empt, not merely a wasted lookup.
--
-- ## Discriminator — SEC's own list, and an EXACT description match
--
-- A CUSIP is an option pseudo-CUSIP iff it appears on the CURRENT
-- Official List with a description of exactly ``CALL`` or ``PUT``
-- (after the ``*`` added-since-last flag is stripped). Measured on
-- ``13flist2026q2-txt.txt`` (25,333 rows, parsed by the repo's own
-- ``parse_13f_list``, 0 unmatched):
--
--   description EXACTLY CALL/PUT       12,220 rows / 10,164 CUSIPs
--                                        of which 11,825 rows fail mod-10
--   description CONTAINS CALL/PUT           7 further rows
--                                        of which     0 fail mod-10
--   WTS/WARRANT/WT/RIGHT/RIGHTS           121 CUSIPs, 0 fail mod-10
--   every other description            12,985 CUSIPs, 0 fail mod-10
--
-- Three traps that table closes:
--
--   * the CHECK DIGIT IS NOT THE DISCRIMINATOR in either direction —
--     339 exact CALL/PUT rows pass it, and thousands of stored CUSIPs
--     absent from the list fail it for unrelated reasons;
--   * ``_is_option`` in ``sec_13f_securities_list.py`` folds
--     WTS/WARRANT/WT/RIGHT/RIGHTS in with CALL/PUT. Warrants and rights
--     are GENUINE securities with genuine CUSIPs and must keep their
--     shot at resolution;
--   * CONTAINING the word "CALL" is not enough either. Those 7 extra
--     rows are 4 BMO structured notes (``CALL NRGU 45``, ``CALL NRGD
--     45``, ``CALL BNKU 45``, ``CALL LKD 41``) and 3 covered-call ETFs
--     (``ETHE CO CALL ETF``, ``KWEB COVERD CALL``, ``YIEL S& CALL
--     ETF``) — all real securities. OpenFIGI answers ``063679427``
--     (``CALL NRGU 45``) with a populated ``data`` array, probed live
--     2026-08-08. The containment form was written first and this probe
--     is what falsified it; the check digit could not, because all 7
--     pass it.
--
-- ## Why TERMINAL, and why it never overwrites
--
-- Terminal for the same reason as sql/192 and
-- sql/261_unresolved_13f_openfigi_invalid_identifier.sql: the verdict is
-- a deterministic function of a fixed stored identifier against an
-- SEC-published list, so re-asking cannot change it, and leaving the row
-- NULL re-selects it forever. ``SET resolution_status = NULL`` remains
-- the operator escape hatch.
--
-- ⚠ The writer claims ONLY rows whose ``resolution_status IS NULL``. It
-- never overwrites an existing verdict, including the OpenFIGI
-- negatives — those answer a different question and their provenance is
-- not re-derivable (the #2304 reasoning). The interaction with #2304 is
-- purely one of ORDER: the operator reset returns rows to NULL, and
-- ``cusip_universe_backfill`` (Sunday 05:00 UTC) claims the option rows
-- an hour before ``cusip_resolver_post_bulk_sweep`` (Sunday 06:00 UTC)
-- would spend rate-limited budget re-asking them.
--
-- ## Idempotency
--
-- DROP + re-ADD in one transaction (same pattern as sql/112, sql/168,
-- sql/192, sql/261_unresolved_13f_openfigi_invalid_identifier.sql). Safe
-- against existing rows: the post-shift value set is a strict superset
-- of the current population.

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
        'openfigi_invalid_identifier',
        'option_pseudo_cusip'
    ));

COMMIT;
