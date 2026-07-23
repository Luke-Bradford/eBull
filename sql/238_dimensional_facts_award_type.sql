-- 238_dimensional_facts_award_type.sql
--
-- #844 (spec docs/specs/etl/2026-07-23-drs-rsu-issuer-disclosures.md)
-- — extend instrument_dimensional_facts with the award-type axis +
-- nonvested-awards metric for the FSNDS notes loader (unvested RSU/PSU
-- counts from the ASC 718-10-50-2(c)(2) rollforward, 10-K note level).
--
-- Constraint names verified against dev pg_constraint 2026-07-23
-- (auto-named by the sql/193 column CHECKs).

BEGIN;

ALTER TABLE instrument_dimensional_facts
    DROP CONSTRAINT instrument_dimensional_facts_axis_check;
ALTER TABLE instrument_dimensional_facts
    ADD CONSTRAINT instrument_dimensional_facts_axis_check CHECK (axis IN
        ('business_segment', 'product_service', 'geographic', 'award_type'));

ALTER TABLE instrument_dimensional_facts
    DROP CONSTRAINT instrument_dimensional_facts_metric_check;
ALTER TABLE instrument_dimensional_facts
    ADD CONSTRAINT instrument_dimensional_facts_metric_check CHECK (metric IN
        ('revenue', 'operating_income', 'assets', 'nonvested_awards'));

COMMIT;
