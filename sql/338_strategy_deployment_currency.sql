-- #2603 item 4: constrain the deployment currency at rest.
--
-- `strategy_deployments.currency` (sql/281:72) is the currency the paper executor
-- actually reads -- `strategy_paper_executor.py:198` selects `d.currency` and `:374`
-- refuses on it.  It shipped as `TEXT NOT NULL DEFAULT 'USD'` whose only constraint is
-- `currency <> ''` (sql/281:82), and no service or API layer narrowed it either: both
-- `configure_deployment` call sites (app/api/strategies.py:1883, :2162) pass a value
-- read straight back out of this column, so the stored code is a self-perpetuating
-- round-trip seeded by the DEFAULT.
--
-- That is the defect docs/review-prevention-log.md:720 already names -- "New TEXT
-- columns in migrations need CHECK constraints or Literal types" (#232), whose symptom
-- was `capital_events.currency` as free-text TEXT.  The pool side has always been
-- correct (sql/290:96); the deployment side never was.
--
-- USD only because FX is unmodelled: #2363 split `fx_unmodelled` out as a standing
-- refusal, so there is no honest cost to charge a non-USD deployment.  Widening this
-- is a coordinated change -- see the site list on
-- `SUPPORTED_DEPLOYMENT_CURRENCIES` in app/services/strategy_base_currency.py.
--
-- Full population at authoring time (dev, the only database): both tables held 0 rows,
--   select currency, count(*) from strategy_deployments group by 1;        -- 0 rows
--   select currency, count(*) from strategy_deployment_events group by 1;  -- 0 rows
-- so ADD CONSTRAINT validates with nothing to repair.  Plain ADD, not NOT VALID: there
-- is no row for a deferred validation to spare.
--
-- The two constraints are named distinctly so a current-state violation and an event
-- violation stay distinguishable in `psycopg.errors.CheckViolation.diag.constraint_name`.

ALTER TABLE strategy_deployments
    ADD CONSTRAINT strategy_deployments_currency_supported
    CHECK (currency = 'USD');

ALTER TABLE strategy_deployment_events
    ADD CONSTRAINT strategy_deployment_events_currency_supported
    CHECK (currency = 'USD');

COMMENT ON COLUMN strategy_deployments.currency IS
    'Deployment base currency, USD-only while FX is unmodelled (#2363). Validated at '
    'the capital authority by configure_deployment and at rest by '
    'strategy_deployments_currency_supported. NOT operator input on any current path: '
    'both API call sites echo the stored value back, so the DEFAULT seeds it.';

COMMENT ON COLUMN strategy_deployment_events.currency IS
    'Audit mirror of strategy_deployments.currency, constrained identically so the '
    'append-only history cannot record a currency the current-state row could not hold.';
