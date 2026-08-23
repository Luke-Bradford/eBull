-- 370_exchange_test_issue_census.sql
--
-- #2912 correction 1: official exchange test issues are synthetic production-
-- feed instruments, not equities.  Universe selection now excludes and counts
-- them, so extend the result census' closed vocabulary without rewriting any
-- historical result.

ALTER TABLE strategy_result_termination_census
    DROP CONSTRAINT IF EXISTS strategy_result_termination_census_stratum_check;

ALTER TABLE strategy_result_termination_census
    ADD CONSTRAINT strategy_result_termination_census_stratum_check CHECK (stratum IN (
        'terminated_exchange_failure',
        'terminated_exchange_failure_a4',
        'terminated_operation_of_law',
        'terminated_linked_unparsed_provision',
        'terminated_q_suffix_otc_unverified',
        'terminated_unknown_termination',
        'termination_skipped_series_break',
        'termination_skipped_unresolved_outcome',
        'termination_skipped_close_bar_unfillable',
        'termination_price_unlocatable',
        'universe_admitted_total',
        'universe_unlinked_alive_excluded',
        'universe_linked_early_reuse_suspect',
        'universe_exchange_test_issues_excluded',
        'universe_unharvested_excluded',
        'universe_vendor_series_total'
    ));

COMMENT ON TABLE strategy_result_termination_census IS
    'Criterion 9 over the survivorship treatment, including the official-exchange '
    'test-issue exclusion added by #2912 correction 1. One immutable row per '
    '(result, stratum).';
