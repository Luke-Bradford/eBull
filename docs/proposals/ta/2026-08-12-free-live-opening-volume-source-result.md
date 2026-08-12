# Free live opening-volume source result

Date: 2026-08-12  
Issue: #2521  
Decision: **refused** — no no-subscription source currently proves the complete
09:35 ET consolidated-volume rank required by #2485

## Question

Can eBull identify the published Opening Range Breakout candidate's top 20
Stocks in Play across the whole eligible US-stock cross-section close enough to
09:35 ET to place demo orders, without buying a data subscription?

This is not a generic search for active tickers. The frozen selector ranks each
eligible stock by first-five-minute volume relative to its own previous 14
opening intervals. A partial venue, delayed list, personalized recommendation
or current-universe shortcut changes the strategy.

## Official eToro capability audit

The complete eToro documentation index was inspected on 2026-08-12. Its market
data surface contains exchanges, instrument types/display data, historical
closing prices, current rates, one-instrument candle history, instrument search
and stock industries. It documents no bulk candle, opening-volume, screener or
market-mover endpoint.

The relevant contracts are:

- [candle history](https://api-portal.etoro.com/api-reference/market-data/get-instrument-candle-history):
  one `instrumentId` in the path, up to 1,000 OHLCV candles, under the shared
  market-data quota of 120 requests per 60 seconds;
- [instrument rates](https://api-portal.etoro.com/api-reference/market-data/get-instrument-market-rates):
  up to 100 instrument IDs, returning bid, ask, last execution and
  margin/conversion price fields, but no volume;
- [WebSocket instrument topics](https://api-portal.etoro.com/core/websocket/topics):
  per-instrument bid, ask, last execution and related price fields, but no
  trade size, cumulative volume or candle;
- [market recommendations](https://api-portal.etoro.com/api-reference/watchlists/get-market-recommendations):
  a personalized list of instrument IDs, with no declared selection timestamp,
  population, volume measure or rank calculation.

At the measured 6,083 currently classified NYSE/Nasdaq common stocks, the
single-instrument candle contract needs at least 51 minutes even at the full
documented shared-quota ceiling (`ceil(6,083 / 120)`). That lower bound excludes
network latency, competing rates/search calls, retries, missing-name handling
and the time needed to rank. The repo's deliberately lower operational client
rate makes the live result slower, not faster.

The batched rates and WebSocket paths cannot replace this scan because a price
update count is not consolidated share volume. Recommendations cannot replace
it because their undisclosed personalized selection cannot be reproduced or
shown to match the paper's point-in-time cross-section.

## Other free-source boundary

Alpaca's official market-data FAQ says historical SIP requests ending at least
15 minutes in the past can be available without a paid subscription, while its
current historical-data page describes IEX as the no-subscription feed. That
entitlement conflict is why #2520 requires a bounded credentialed account probe
rather than an assumed integration.

Both official pages agree on the live distinction that matters here: free
real-time equities are IEX, whereas consolidated SIP coverage is delayed or a
subscription product. IEX is one exchange and cannot be asserted to preserve a
top-20 rank defined on consolidated US trading volume. A parity claim would
need complete simultaneous SIP observations and a predeclared displacement
test; none exists.

Two other plausible official provider surfaces also fail the exact boundary:

- [Twelve Data's US coverage statement](https://support.twelvedata.com/en/articles/9935903-us-equities-market-data)
  says its default real-time feed covers listed symbols but represents about 5%
  of US trading activity; full-market coverage is a separately licensed
  product. Its free Basic plan also has eight API credits per minute. Neither a
  partial-volume rank nor that quota can reproduce the consolidated top 20.
- [Alpha Vantage's official documentation](https://www.alphavantage.co/documentation/)
  states that real-time and 15-minute-delayed US data require premium
  membership, its bulk real-time quote endpoint is premium, and its standard
  free quote is end-of-day. It therefore supplies no free 09:35 input.

Other public websites were not accepted merely because they display “most
active” lists. The acceptance contract requires an authoritative, legally
reusable feed with known venue composition, complete prefiltered coverage,
deterministic timing and an operational interface. No such no-subscription
source was identified in the official provider surfaces above.

## Consequence

- Close #2521 as a documented refusal, not an implementation backlog.
- Keep #2520 open solely for delayed historical falsification. A successful
  delayed test does not make #2485 executable at 09:35.
- Keep the eight-name eToro intraday panel as collection and execution-cost
  instrumentation. It is not an ORB return sample.
- Do not ingest a full-market tick or minute-bar heap. If a qualifying live
  source appears later, retain one opening summary per instrument/session and
  selected top-20 paths under the already frozen bounded design.
- Do not expose ORB as an allocation option and do not emit a demo order until
  a new source version clears complete coverage, rank parity, timeliness and
  eToro quote/shortability gates prospectively.

This refusal says the published candidate is not currently automatable under
the no-subscription constraint. It is not evidence that ORB itself loses money;
that remains a separate delayed falsification question.
