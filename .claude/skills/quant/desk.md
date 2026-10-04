# quant/desk: how eBull works as a small systematic fund

## When to use

Read this first for any work that could change what the engine holds: strategy research, data for signals, risk, execution or capital decisions. It maps each role to the skill that carries its knowledge, and sets the path every strategy must travel before it opens a position.

## Objective

Grow the operator's capital, hands-off, by beating SPY total return net of all costs within the mandate's risk limits. Money is committed only where historical evidence on point-in-time data supports it. The operator delegates strategy design and wants outcomes, not menus. Live capital stays operator-funded.

## The roles

| role | owns | reads |
|---|---|---|
| Research (CIO) | which hypotheses are worth testing; the evidence bar | `quant/research-process.md`, `quant/strategy-menu.md`, `quant/strategy-evidence.md` |
| Quant researcher | panels, factor construction, backtests, segmentation | `quant/research-process.md`, `quant/market-segments.md`, `quant/measurement-discipline.md`, `data-sources/research-price-corpus.md` |
| Data | what we hold, what is point-in-time, what to collect | `quant/data-map.md`, `data-engineer`, `data-sources/*` |
| Portfolio and risk | sizing, construction, limits, stress, attribution | `quant/portfolio-construction-and-risk.md`, `quant/trade-lifecycle.md` |
| Execution | orders, costs, fills, broker mechanics | `quant/cost-aware-viability.md`, `data-sources/etoro-api.md`, `execution-guard` |
| LLM research | text-derived features and their validation | `quant/llm-research.md` |

One agent often plays several roles. What matters is that each decision is made with that role's knowledge loaded.

## The path from idea to an open position

1. **Hypothesis with an economic mechanism.** Either a compensated risk (a premium for bearing it) or a behavioural or structural cause that persists. Check `quant/strategy-menu.md` for prior evidence on the family. A plausible story is not evidence; it only makes the test worth running.
2. **Register, choose the track, check power** (`research-process.md`). If the data cannot answer the question, do not declare it.
3. **Point-in-time panel.** Survivorship-free, knowledge-time correct, segmented.
4. **Backtest.** Net of our real costs, against net buy-and-hold and a random-basket control. Validate against published factors where they exist; deflate for everything tried.
5. **Forward demo test.** Only after step 4 passes, with pre-registered operational and performance stop rules.
6. **Capital.** A capped live canary funded by the operator, then scaled on evidence.

Nothing skips a step. Execution plumbing is tested with its own small harness, never by running an unproven strategy. Exits and protective actions are never blocked.

## Standing judgements

- **Turnover, not calendar frequency, decides cost.** The eToro round trip runs 0.1–1.5% by price band. Strategies that turn their book over quickly need per-trade edges most retail evidence says do not survive. A wide holding band with frequent monitoring can trade less than a monthly full rebalance.
- **Prefer premia with independent out-of-sample evidence over clever new signals.** Size expectations on post-publication results.
- **Volatility is far more forecastable than returns.** Use it for sizing and risk.
- **Every position states why it is held:** the selecting factors or rule, the exit and the evidence id (`strategy_entry_tickets`, #3542). "Beta" is not a selection reason.
- **Report outcomes plainly**, including "not demonstrated" and "this is beta".
