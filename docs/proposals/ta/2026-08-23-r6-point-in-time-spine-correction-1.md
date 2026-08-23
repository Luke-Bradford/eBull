# R6 point-in-time spine correction 1: sentinel accession syntax (#2900)

Status: **FROZEN BEFORE A VALID LEAK TEST**.

The first focused database test never reached a baseline or leak result. The
schema rejected the declaration's synthetic `source_accession` value
`R6-2900-PIT-SENTINEL` under
`chk_institutions_obs_source_accession`, which requires the SEC accession shape
`^[0-9]{10}-[0-9]{2}-[0-9]{6}$`.

The corrected fixed identity uses `0000002900-20-002900` for both
`source_document_id` and `source_accession`; the post-decision control uses
`0000002900-20-002901`. Filer CIK, decision date, period end, values, writer,
natural-key overwrite, tuple columns, comparison and verdict rules are
unchanged.

The source probe for FINRA `settlement_date DATE NOT NULL` expects two anchors,
because migration 152 declares that column on both observations and current
tables. This changes no evidence condition; it corrects the non-vacuity count.
