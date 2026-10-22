-- 516_13f_included_managers_capture.sql
--
-- #3227 tier (IM), § Change 1 of docs/specs/ownership/2026-10-22-3227-13f-included-managers.md: the 13F capture of
-- Column 6, Column 7 and the List of Other Included Managers (Form 13F Special Instructions 7.b, 11.b.vi, 11.b.vii).
--
-- `institutional_13f_other_managers`: one row per list entry as filed, invalid entries included. `sequence_number` is
-- NULL when `sequence_number_raw` breaks the grammar (ASCII digits, 1-9 of them, >= 1). CRD and SEC file numbers are
-- not stored: the rule uses only the CIK, and the XML parse carries neither.
--
-- `institutional_13f_captures`: one row per captured accession, with its status against the summary page's controls
-- (Special Instructions 6.a list count, 6.b line count, 6.c value total). Only `complete` serves the rollup tier.
--
-- `ownership_institutions_observations.thirteen_f_segments`: NULL is not captured. Otherwise
-- {"capture_id", "cusips", "lines": [{"discretion", "other_managers", "shares"}]}, the position's lines aggregated by
-- (Column 6, sorted Column 7). A capture of an accession replaces its record, its list and the segments it writes, all
-- under one new `capture_id`, in one transaction.

CREATE TABLE IF NOT EXISTS institutional_13f_captures (
    accession_number     TEXT        PRIMARY KEY,
    capture_id           UUID        NOT NULL,
    source               TEXT        NOT NULL CHECK (source IN ('dataset', 'xml')),
    status               TEXT        NOT NULL CHECK (status IN ('complete', 'incomplete', 'invalid')),
    list_count           INTEGER     NOT NULL,
    expected_list_count  INTEGER,
    line_count           INTEGER     NOT NULL,
    expected_line_count  INTEGER,
    value_total          NUMERIC     NOT NULL,
    expected_value_total NUMERIC,
    captured_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS institutional_13f_other_managers (
    accession_number    TEXT        NOT NULL,
    entry_index         INTEGER     NOT NULL,
    sequence_number_raw TEXT,
    sequence_number     INTEGER,
    cik                 TEXT,
    name                TEXT,
    form13f_file_number TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_number, entry_index)
);

ALTER TABLE ownership_institutions_observations ADD COLUMN IF NOT EXISTS thirteen_f_segments JSONB;
