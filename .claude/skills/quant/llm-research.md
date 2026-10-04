# quant/llm-research: using language models in research without fooling ourselves

## When to use

Use this whenever an LLM output can influence a score, a selection or a trade: thesis writing, filing extraction, sentiment, or an AI-driven trial. It sets what an LLM should produce, how to validate it, and how to run it cheaply.

## What the evidence supports

**Works:** LLMs are good at turning text into narrow, structured judgements, which are then tested like any other factor.

- Headline sentiment after the training cutoff (Lopez-Lira & Tang). Strongest in small caps; decays as adoption rises.
- News embeddings that predict cross-sectional returns (Chen, Kelly & Xiu).
- Capex intent and risk exposures from earnings calls (Jha, Qian, Weber & Yang).
- Year-on-year 10-K changes (Cohen, Malloy & Nguyen 2020).
- A sector-neutral relative outlook score evaluated after each model's cutoff (Lehner & Lopez-Lira 2026, working paper).

**Does not work:**

- LLMs as end-to-end traders: rarely beat buy-and-hold over long samples (FINSABER; StockBench).
- Numeric valuation and price targets (Levy 2024).
- The sell-side analogue: absolute targets miss by about 45% on average. Only within-industry rankings inform (Bradshaw, Brown & Huang 2013; Da & Schaumburg 2011; Farago, Hjalmarsson & Zeng 2023).

## Look-ahead and memorisation

A commercial model has read the future of every date before its training cutoff. Given a ticker and a date, models recall later returns, and "ignore what happened after X" instructions do not stop it (Lopez-Lira, Tang & Zhu 2025; Sarkar & Vafa 2024).

- **A backtest of LLM output on pre-cutoff dates is not evidence.** Skill and memory cannot be told apart.
- **Valid evaluation is forward-only after the model's cutoff, with the model version pinned.** Moving to a newer model moves the cutoff forward and invalidates more history.
- Anonymising the company reduces both memorisation and the distraction effect (Glasserman & Lin 2024).
- A recall probe (does the model know this firm-date's later outcome?) measures contamination (Gao, Jiang & Yan).

## What our LLM component should produce

1. **Structured features with an evidence quote and a document id each:**
   - guidance raised / held / cut / withdrawn;
   - risk-factor section changed vs the prior filing;
   - accounting red flags (restatement, auditor change, going-concern language);
   - customer, product and contract events;
   - earnings direction next quarter with a reason.
2. **One sector-neutral relative outlook score per stock**, ranked within industry.
3. **No absolute price target enters any score.** Keep bear/base/bull for the operator-facing memo if useful. Convert them to within-industry upside ranks before any ranking use.
4. **Confidence from self-consistency**: agreement across k samples. Never a model-stated confidence number, which is poorly calibrated (Xiong et al. 2024).

## Validation

- Extraction first: precision and recall of each feature against a hand-labelled sample, and against a matched deterministic text baseline (keyword or diff rules) that costs nothing to run.
- Then each feature is a factor: monthly rank IC, decay at 5/20/60/120 sessions, incremental value over the deterministic families, split by size. Rank IC alone is not enough: report net executable returns after trading and inference costs, and the event-to-decision latency where predictability lasts only days.
- (model id, prompt, schema, document set) is hashed into the strategy version. A change is a new version with its own forward record.
- A/B two writer models on the same post-cutoff dates by rank IC, never by which memo reads better.

## Running it efficiently

- **Trigger on events, not the calendar:** a new 10-K/10-Q/8-K, a transcript, or a large price move. Carry the last structured read forward with its timestamp.
- Feed point-in-time documents only: MD&A and Risk Factors diffs, statements in standard form. Our `periodic_report_sections` holds the sections.
- Batch nightly through the Message Batches API (50% cost), with the rubric and schema in a cached prefix and per-stock documents last.
- Persist raw outputs, prompts and model ids for audit.
