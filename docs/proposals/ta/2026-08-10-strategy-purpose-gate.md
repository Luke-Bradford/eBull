# Strategy purpose gate

Issue: #2443. Parent: #2437.

Every manifest entry declares exactly one purpose:

- `harness_validation`: a permanent control used to exercise the research and outcome pipeline;
- `capital_candidate`: a preregistered hypothesis that may seek promotion after all evidence gates pass.

S-1 through S-4 are `harness_validation`. Their result rows retain that purpose
immutably. A harness result always carries `harness_validation_only`; promotion,
capital allocation and execution independently refuse it. Improving the corpus,
cost model or test harness cannot clear that refusal.

Harness runs continue to consume the declared trial budget. Purpose does not
erase a search already performed against price data, and excluding a poor
control result after observing it would understate the multiple-testing burden.
This follows the existing rule in `trial_register.py`: searches count; untested
designs do not.

The operator surface separates validation controls from capital candidates and
does not render allocation controls for the former.
